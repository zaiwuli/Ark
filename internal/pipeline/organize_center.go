package pipeline

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zaiwuli/Ark/internal/p115"
	"github.com/zaiwuli/Ark/internal/store"
)

// OrganizeTarget represents a user-saved custom 115 CID or local folder for the Organize Center.
type OrganizeTarget struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Kind   string `json:"kind"`   // cloud | local
	Target string `json:"target"` // 115 CID or local path
	Depth  int    `json:"depth"`  // 1..5, default 1
}

type CenterScanRequest struct {
	TargetKeys        []string `json:"target_keys"` // sys:cloud:offline, src:<id>, custom:<id>, ...
	EnableDateArchive bool     `json:"enable_date_archive"`
	EnableDuplicate   bool     `json:"enable_duplicate"`
	EnableJunk        bool     `json:"enable_junk"`
	EnableClassify    bool     `json:"enable_classify"`
	EnableEmptyDir    bool     `json:"enable_empty_dir"`
	EnableRename      bool     `json:"enable_rename"`
	EnableFlatten     bool     `json:"enable_flatten"`

	// Optional overrides (if omitted, persisted WorkbenchConfig is used)
	JunkExts         string   `json:"junk_exts"`
	JunkKeywords     string   `json:"junk_keywords"`
	JunkKeywordMaxMB *float64 `json:"junk_keyword_max_mb,omitempty"`
	JunkMaxMB        *float64 `json:"junk_max_mb,omitempty"`
	JunkSafeExts     string   `json:"junk_safe_exts"`
	DedupKeepPolicy  string   `json:"dedup_keep_policy"`
	ClassifyMode     string   `json:"classify_mode"`
	RenameCleanAds   *bool    `json:"rename_clean_ads,omitempty"`
	RenameFind       string   `json:"rename_find"`
	RenameReplace    string   `json:"rename_replace"`
	RenameRegex      bool     `json:"rename_regex"`
	RenamePrefix     string   `json:"rename_prefix"`
}

type OrganizeItem struct {
	ID         string `json:"id"`
	Tool       string `json:"tool"`        // date_archive | duplicate | junk | classify | empty_dir | rename | flatten
	Scope      string `json:"scope"`       // cloud | local
	TargetName string `json:"target_name"` // display name of source target
	Name       string `json:"name"`        // filename or folder name
	NewName    string `json:"new_name,omitempty"`
	SHA1       string `json:"sha1,omitempty"`
	Size       int64  `json:"size"`
	Detail     string `json:"detail"`     // human-readable reason / match info
	Action     string `json:"action"`     // move | delete | rename | flatten
	DestLabel  string `json:"dest_label"` // display destination

	// Internal execution metadata (omitted from JSON responses)
	SourceCID  string `json:"-"`
	RootCID    string `json:"-"`
	FID        string `json:"-"`
	ChildFID   string `json:"-"`
	ChildName  string `json:"-"`
	SourcePath string `json:"-"`
	RootPath   string `json:"-"`
	ChildPath  string `json:"-"`
	DestSubdir string `json:"-"`
	DateStr    string `json:"-"`
}

type CenterScanPlan struct {
	GeneratedAt int64          `json:"generated_at"`
	Items       []OrganizeItem `json:"items"`
	Counts      map[string]int `json:"counts"` // tool -> count
}

type CenterExecResult struct {
	MovedCount   int      `json:"moved_count"`
	DeletedCount int      `json:"deleted_count"`
	RenamedCount int      `json:"renamed_count"`
	FailedCount  int      `json:"failed_count"`
	SkippedCount int      `json:"skipped_count"`
	Errors       []string `json:"errors,omitempty"`
}

type CenterJobState struct {
	JobID           string            `json:"job_id"`
	Mode            string            `json:"mode"`   // "" | scan | exec
	Status          string            `json:"status"` // idle | running | done | cancelled | failed
	ProgressCur     int               `json:"progress_cur"`
	ProgressTotal   int               `json:"progress_total"`
	StageLabel      string            `json:"stage_label"`
	Error           string            `json:"error,omitempty"`
	Logs            []string          `json:"logs"`
	Plan            *CenterScanPlan   `json:"plan,omitempty"`
	Result          *CenterExecResult `json:"result,omitempty"`
	UndoRenameCount int               `json:"undo_rename_count"`
	StartedAt       int64             `json:"started_at,omitempty"`
	FinishedAt      int64             `json:"finished_at,omitempty"`
}

type organizeCenter struct {
	mu          sync.Mutex
	state       CenterJobState
	cancel      context.CancelFunc
	lastRenames []RenameUndoEntry
}

func newOrganizeCenter() *organizeCenter {
	return &organizeCenter{
		state: CenterJobState{
			Status: "idle",
			Logs:   make([]string, 0, 32),
		},
	}
}

func (c *organizeCenter) addLogLocked(line string) {
	ts := time.Now().Format("15:04:05")
	c.state.Logs = append(c.state.Logs, fmt.Sprintf("[%s] %s", ts, line))
	if len(c.state.Logs) > 120 {
		c.state.Logs = append([]string(nil), c.state.Logs[len(c.state.Logs)-120:]...)
	}
}

func (c *organizeCenter) addLog(line string) {
	c.mu.Lock()
	c.addLogLocked(line)
	c.mu.Unlock()
}

func (c *organizeCenter) setProgress(cur, total int, label string) {
	c.mu.Lock()
	c.state.ProgressCur = cur
	c.state.ProgressTotal = total
	c.state.StageLabel = label
	c.mu.Unlock()
}

func (c *organizeCenter) snapshot() CenterJobState {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := c.state
	cp.Logs = append([]string(nil), c.state.Logs...)
	if c.state.Plan != nil {
		p := *c.state.Plan
		p.Items = append([]OrganizeItem(nil), c.state.Plan.Items...)
		p.Counts = make(map[string]int, len(c.state.Plan.Counts))
		for k, v := range c.state.Plan.Counts {
			p.Counts[k] = v
		}
		cp.Plan = &p
	}
	if c.state.Result != nil {
		r := *c.state.Result
		r.Errors = append([]string(nil), c.state.Result.Errors...)
		cp.Result = &r
	}
	return cp
}

// Custom Targets CRUD (persisted in SQLite settings under "organizer.targets").

func (s *Service) ListOrganizeTargets(ctx context.Context) ([]OrganizeTarget, error) {
	var list []OrganizeTarget
	_, err := s.store.GetSetting(ctx, "organizer.targets", &list)
	if err != nil {
		return nil, err
	}
	if list == nil {
		list = make([]OrganizeTarget, 0)
	}
	return list, nil
}

func (s *Service) UpsertOrganizeTarget(ctx context.Context, t OrganizeTarget) (OrganizeTarget, error) {
	t.Name = strings.TrimSpace(t.Name)
	t.Kind = strings.TrimSpace(strings.ToLower(t.Kind))
	t.Target = strings.TrimSpace(t.Target)
	if t.Name == "" || (t.Kind != "cloud" && t.Kind != "local") || t.Target == "" {
		return OrganizeTarget{}, store.ErrInvalid
	}
	if t.Depth < 1 {
		t.Depth = 1
	}
	if t.Depth > 5 {
		t.Depth = 5
	}
	list, err := s.ListOrganizeTargets(ctx)
	if err != nil {
		return OrganizeTarget{}, err
	}
	if t.ID == "" {
		t.ID = store.NewID("ot")
		list = append(list, t)
	} else {
		found := false
		for i := range list {
			if list[i].ID == t.ID {
				list[i] = t
				found = true
				break
			}
		}
		if !found {
			list = append(list, t)
		}
	}
	if err := s.store.SetSetting(ctx, "organizer.targets", list, false); err != nil {
		return OrganizeTarget{}, err
	}
	return t, nil
}

func (s *Service) DeleteOrganizeTarget(ctx context.Context, id string) error {
	list, err := s.ListOrganizeTargets(ctx)
	if err != nil {
		return err
	}
	out := make([]OrganizeTarget, 0, len(list))
	found := false
	for _, item := range list {
		if item.ID == id {
			found = true
			continue
		}
		out = append(out, item)
	}
	if !found {
		return store.ErrNotFound
	}
	return s.store.SetSetting(ctx, "organizer.targets", out, false)
}

func (s *Service) CenterJobState() CenterJobState {
	if s.center == nil {
		return CenterJobState{Status: "idle", Logs: []string{}}
	}
	return s.center.snapshot()
}

func (s *Service) CancelCenterJob() bool {
	if s.center == nil {
		return false
	}
	s.center.mu.Lock()
	defer s.center.mu.Unlock()
	if s.center.state.Status != "running" || s.center.cancel == nil {
		return false
	}
	s.center.addLogLocked("收到停止指令，正在安全中止当前整理任务…")
	s.center.cancel()
	return true
}

type resolvedCenterTarget struct {
	Key   string
	Name  string
	Kind  string // cloud | local
	CID   string
	Path  string
	Depth int
}

func (s *Service) resolveCenterTargets(ctx context.Context, keys []string) []resolvedCenterTarget {
	keySet := make(map[string]bool, len(keys))
	for _, k := range keys {
		if strings.TrimSpace(k) != "" {
			keySet[strings.TrimSpace(k)] = true
		}
	}
	_, p115Cfg := s.snapshotCloud()
	defaultDepth := p115Cfg.ScanDepth()
	var out []resolvedCenterTarget

	sysCloud := []struct {
		key  string
		name string
		cid  string
	}{
		{"sys:cloud:offline", "离线下载", p115Cfg.OfflineRootCID},
		{"sys:cloud:output", "解压输出", p115Cfg.CloudOutputCID},
		{"sys:cloud:archive", "原包留存", p115Cfg.CloudArchiveCID},
	}
	for _, sc := range sysCloud {
		if keySet[sc.key] && sc.cid != "" {
			out = append(out, resolvedCenterTarget{Key: sc.key, Name: sc.name, Kind: "cloud", CID: sc.cid, Depth: 1})
		}
	}
	sysLocal := []struct {
		key  string
		name string
		dir  string
	}{
		{"sys:local:watch", "本地监控", filepath.Join(s.workDir, "本地监控")},
		{"sys:local:output", "解压输出", s.outputDir()},
		{"sys:local:archive", "原包留存", s.originalsDir()},
	}
	for _, sl := range sysLocal {
		if keySet[sl.key] && sl.dir != "" {
			out = append(out, resolvedCenterTarget{Key: sl.key, Name: sl.name, Kind: "local", Path: sl.dir, Depth: 1})
		}
	}

	if sources, err := s.store.ListSources(ctx, ""); err == nil {
		for _, src := range sources {
			k := "src:" + src.ID
			if !keySet[k] {
				continue
			}
			var cfg struct {
				CID  string `json:"cid"`
				Path string `json:"path"`
			}
			_ = json.Unmarshal([]byte(src.ConfigJSON), &cfg)
			if src.Kind == "cloud" && strings.TrimSpace(cfg.CID) != "" {
				out = append(out, resolvedCenterTarget{Key: k, Name: src.Name, Kind: "cloud", CID: strings.TrimSpace(cfg.CID), Depth: defaultDepth})
			} else if src.Kind == "local" && strings.TrimSpace(cfg.Path) != "" {
				out = append(out, resolvedCenterTarget{Key: k, Name: src.Name, Kind: "local", Path: strings.TrimSpace(cfg.Path), Depth: defaultDepth})
			}
		}
	}

	if customs, err := s.ListOrganizeTargets(ctx); err == nil {
		for _, ct := range customs {
			k := "custom:" + ct.ID
			if !keySet[k] {
				continue
			}
			d := ct.Depth
			if d < 1 {
				d = 1
			}
			if ct.Kind == "cloud" {
				out = append(out, resolvedCenterTarget{Key: k, Name: ct.Name, Kind: "cloud", CID: ct.Target, Depth: d})
			} else {
				out = append(out, resolvedCenterTarget{Key: k, Name: ct.Name, Kind: "local", Path: ct.Target, Depth: d})
			}
		}
	}
	return out
}

// StartCenterScan launches a background single-pass 5-in-1 scan job and returns immediately.
func (s *Service) StartCenterScan(req CenterScanRequest) (string, error) {
	if s.center == nil {
		s.center = newOrganizeCenter()
	}
	s.center.mu.Lock()
	if s.center.state.Status == "running" {
		s.center.mu.Unlock()
		return "", errors.New("ORGANIZE_JOB_BUSY")
	}
	jobID := store.NewID("org")
	jobCtx, cancel := context.WithCancel(context.Background())
	s.center.cancel = cancel
	s.center.state = CenterJobState{
		JobID:         jobID,
		Mode:          "scan",
		Status:        "running",
		ProgressCur:   0,
		ProgressTotal: len(req.TargetKeys),
		StageLabel:    "正在准备扫描目标…",
		Logs:          make([]string, 0, 32),
		StartedAt:     time.Now().Unix(),
	}
	s.center.addLogLocked(fmt.Sprintf("开始异步扫描预览（勾选目标 %d 个）", len(req.TargetKeys)))
	s.center.mu.Unlock()

	go s.runCenterScanJob(jobCtx, jobID, req)
	return jobID, nil
}

func (s *Service) runCenterScanJob(ctx context.Context, jobID string, req CenterScanRequest) {
	defer func() {
		s.center.mu.Lock()
		if s.center.state.JobID == jobID && s.center.state.Status == "running" {
			if ctx.Err() != nil {
				s.center.state.Status = "cancelled"
				s.center.state.StageLabel = "已停止扫描"
				s.center.addLogLocked("扫描已中止。")
			}
		}
		s.center.cancel = nil
		s.center.state.FinishedAt = time.Now().Unix()
		s.center.mu.Unlock()
	}()

	targets := s.resolveCenterTargets(ctx, req.TargetKeys)
	if len(targets) == 0 {
		s.center.mu.Lock()
		s.center.state.Status = "done"
		s.center.state.StageLabel = "未选择任何有效目录"
		s.center.state.Plan = &CenterScanPlan{GeneratedAt: time.Now().Unix(), Items: []OrganizeItem{}, Counts: map[string]int{}}
		s.center.addLogLocked("未勾选有效目录，跳过扫描。")
		s.center.mu.Unlock()
		return
	}

	now := time.Now().In(s.loc)
	todayStr := now.Format("2006-01-02")
	activeDates, activeCIDs, _ := s.store.ActiveDatesAndCIDs(ctx)
	cloud, p115Cfg := s.snapshotCloud()

	wbCfg := s.WorkbenchConfig(ctx)
	if strings.TrimSpace(req.JunkExts) != "" {
		wbCfg.JunkExts = req.JunkExts
	}
	if strings.TrimSpace(req.JunkKeywords) != "" {
		wbCfg.JunkKeywords = req.JunkKeywords
	}
	if req.JunkKeywordMaxMB != nil {
		wbCfg.JunkKeywordMaxMB = *req.JunkKeywordMaxMB
	}
	if req.JunkMaxMB != nil {
		wbCfg.JunkMaxMB = *req.JunkMaxMB
	}
	if strings.TrimSpace(req.JunkSafeExts) != "" {
		wbCfg.JunkSafeExts = req.JunkSafeExts
	}
	if req.DedupKeepPolicy != "" {
		wbCfg.DedupKeepPolicy = req.DedupKeepPolicy
	}
	if req.ClassifyMode != "" {
		wbCfg.ClassifyMode = req.ClassifyMode
	}
	if req.RenameCleanAds != nil {
		wbCfg.RenameCleanAds = *req.RenameCleanAds
	}
	if req.RenameFind != "" || req.RenameReplace != "" {
		wbCfg.RenameFind = req.RenameFind
		wbCfg.RenameReplace = req.RenameReplace
		wbCfg.RenameRegex = req.RenameRegex
	}
	if req.RenamePrefix != "" {
		wbCfg.RenamePrefix = req.RenamePrefix
	}
	wbCfg = wbCfg.Normalize()

	items := make([]OrganizeItem, 0)
	counts := map[string]int{
		"date_archive": 0,
		"duplicate":    0,
		"junk":         0,
		"classify":     0,
		"empty_dir":    0,
		"rename":       0,
		"flatten":      0,
	}
	nextID := 0
	makeID := func() string {
		nextID++
		return fmt.Sprintf("oi_%d", nextID)
	}

	s.center.setProgress(0, len(targets), fmt.Sprintf("开始扫描 0/%d", len(targets)))

	for idx, t := range targets {
		if ctx.Err() != nil {
			return
		}
		scopeLabel := "本地"
		if t.Kind == "cloud" {
			scopeLabel = "115"
		}
		s.center.setProgress(idx, len(targets), fmt.Sprintf("正在扫描 [%s] %s（%d/%d）", scopeLabel, t.Name, idx+1, len(targets)))
		s.center.addLog(fmt.Sprintf("扫描 [%s] %s（深度 %d 层）…", scopeLabel, t.Name, t.Depth))

		if t.Kind == "cloud" {
			if cloud == nil {
				s.center.addLog("跳过 115 目录：尚未配置 115 Cookie。")
				continue
			}
			found := s.scanCloudCenterTarget(ctx, cloud, p115Cfg, t, req, wbCfg, todayStr, activeDates, activeCIDs, makeID)
			for _, it := range found {
				items = append(items, it)
				counts[it.Tool]++
			}
			s.center.addLog(fmt.Sprintf("完成 [%s] %s：发现 %d 条待处理项", scopeLabel, t.Name, len(found)))
		} else {
			found := s.scanLocalCenterTarget(ctx, t, req, wbCfg, todayStr, activeDates, makeID)
			for _, it := range found {
				items = append(items, it)
				counts[it.Tool]++
			}
			s.center.addLog(fmt.Sprintf("完成 [%s] %s：发现 %d 条待处理项", scopeLabel, t.Name, len(found)))
		}
	}

	plan := &CenterScanPlan{
		GeneratedAt: time.Now().Unix(),
		Items:       items,
		Counts:      counts,
	}
	s.center.mu.Lock()
	s.center.state.ProgressCur = len(targets)
	s.center.state.ProgressTotal = len(targets)
	s.center.state.Status = "done"
	s.center.state.StageLabel = fmt.Sprintf("扫描完成，共发现 %d 条待整理项", len(items))
	s.center.state.Plan = plan
	s.center.state.UndoRenameCount = len(s.center.lastRenames)
	s.center.addLogLocked(fmt.Sprintf("扫描完成：日期 %d · 洗名 %d · 查重 %d · 垃圾 %d · 归类 %d · 解包 %d · 空目录 %d",
		counts["date_archive"], counts["rename"], counts["duplicate"], counts["junk"], counts["classify"], counts["flatten"], counts["empty_dir"]))
	s.center.mu.Unlock()
}

func evalJunkFile(name, sha1 string, size int64, junkExts, safeExts, sha1BL map[string]bool, keywords []string, kwMaxBytes, maxBytes int64, maxMB, kwMaxMB float64) (bool, string) {
	if sha1 != "" && sha1BL[strings.ToLower(sha1)] {
		return true, "命中广告 SHA1 黑名单"
	}
	ext := strings.ToLower(filepath.Ext(name))
	if safeExts[ext] {
		return false, ""
	}
	if junkExts[ext] {
		return true, "命中垃圾扩展名 " + ext
	}
	if len(keywords) > 0 {
		if kw := matchKeyword(name, keywords); kw != "" {
			if kwMaxBytes <= 0 || size <= kwMaxBytes {
				if kwMaxMB > 0 {
					return true, fmt.Sprintf("命中关键词「%s」且 < %.0f MB", kw, kwMaxMB)
				}
				return true, "命中关键词「" + kw + "」"
			}
		}
	}
	if maxBytes > 0 && size > 0 && size < maxBytes {
		return true, fmt.Sprintf("小于 %.1f MB", maxMB)
	}
	return false, ""
}

func pickDupKeeperIndex(names []string, mods []int64, policy string) int {
	if len(names) <= 1 {
		return 0
	}
	best := 0
	for i := 1; i < len(names); i++ {
		switch policy {
		case "newest":
			if mods[i] > mods[best] {
				best = i
			}
		case "oldest":
			if mods[i] > 0 && (mods[best] == 0 || mods[i] < mods[best]) {
				best = i
			}
		default: // "cleanest": prefer shorter filename without URL/@ watermark
			scoreI := len([]rune(names[i]))
			if strings.Contains(names[i], "@") || strings.Contains(strings.ToLower(names[i]), "www.") {
				scoreI += 40
			}
			scoreB := len([]rune(names[best]))
			if strings.Contains(names[best], "@") || strings.Contains(strings.ToLower(names[best]), "www.") {
				scoreB += 40
			}
			if scoreI < scoreB {
				best = i
			}
		}
	}
	return best
}

func (s *Service) scanCloudCenterTarget(
	ctx context.Context,
	cloud p115.Port,
	p115Cfg P115Settings,
	t resolvedCenterTarget,
	req CenterScanRequest,
	wbCfg WorkbenchConfig,
	todayStr string,
	activeDates, activeCIDs map[string]bool,
	makeID func() string,
) []OrganizeItem {
	type dirNode struct {
		cid       string
		parentCID string
		name      string
		depth     int
	}
	maxSubDepth := t.Depth - 1
	queue := []dirNode{{cid: t.CID, depth: 0}}
	visited := make(map[string]bool)
	const maxCloudDirCalls = 200
	apiCalls := 0
	budgetWarned := false

	junkExts := parseExtSet(wbCfg.JunkExts)
	safeExts := parseExtSet(wbCfg.JunkSafeExts)
	sha1BL := make(map[string]bool, len(wbCfg.JunkSHA1Blacklist))
	for _, h := range wbCfg.JunkSHA1Blacklist {
		sha1BL[strings.ToLower(h)] = true
	}
	keywords := parseKeywords(wbCfg.JunkKeywords)
	kwMaxBytes := int64(wbCfg.JunkKeywordMaxMB * 1024 * 1024)
	junkMaxBytes := int64(wbCfg.JunkMaxMB * 1024 * 1024)

	type cloudFileRec struct {
		f      p115.CloudFile
		curCID string
		depth  int
	}
	var allFiles []cloudFileRec
	seenFileFID := make(map[string]bool)
	addFileRec := func(f p115.CloudFile, curCID string, depth int) {
		if f.IsDir || f.FID == "" || seenFileFID[f.FID] {
			return
		}
		seenFileFID[f.FID] = true
		allFiles = append(allFiles, cloudFileRec{f: f, curCID: curCID, depth: depth})
	}
	skipCIDs := make(map[string]bool, len(activeCIDs)+4)
	for cid := range activeCIDs {
		skipCIDs[cid] = true
	}
	for _, managed := range []string{p115Cfg.CloudArchiveZoneCID, p115Cfg.CloudArchiveCID, p115Cfg.CloudFailedCID, p115Cfg.CloudHistoryCID} {
		if managed != "" {
			skipCIDs[managed] = true
		}
	}
	deletedDirCIDs := make(map[string]bool)
	flattenedCIDs := make(map[string]bool)

	var out []OrganizeItem
	catName := sanitizeArchiveCatName(t.Name)

	// Phase 0: Fast flat recursive file fetch (show_dir=0, cur=0, limit=1150) — pulls ALL files across ALL depths in seconds!
	recursiveUsed := false
	wantFileTools := req.EnableJunk || req.EnableDuplicate || req.EnableRename
	if t.Depth >= 2 && wantFileTools && t.CID != "" && t.CID != "0" && t.CID != p115Cfg.CloudRootCID {
		const maxRecPages = 15
		offset := 0
		recOK := true
		for pageIdx := 0; pageIdx < maxRecPages; pageIdx++ {
			if ctx.Err() != nil {
				return out
			}
			page, err := cloud.FilesRecursive(ctx, t.CID, offset)
			if err != nil {
				if isCloudScanFatalErr(err) {
					s.center.addLog("检测到 115 熔断/限流/认证异常，已停止扫描。")
					return out
				}
				s.center.addLog(fmt.Sprintf("极速递归拉取回退到逐层扫描（%s）", err.Error()))
				recOK = false
				break
			}
			for _, f := range page.Files {
				d := 1
				if f.CID == t.CID {
					d = 0
				}
				addFileRec(f, f.CID, d)
			}
			s.center.setProgress(pageIdx+1, maxRecPages, fmt.Sprintf("极速扫描 [115] %s（已拉取 %d 个文件）…", t.Name, len(allFiles)))
			if page.NextOffset == nil {
				break
			}
			offset = *page.NextOffset
		}
		if recOK {
			recursiveUsed = true
			s.center.addLog(fmt.Sprintf("极速递归扫描 [115] %s 完成：共拉取 %d 个全层级文件", t.Name, len(allFiles)))
		}
	}

	reportProgress := func() {
		if apiCalls > 0 && apiCalls%5 == 0 {
			s.center.setProgress(apiCalls, maxCloudDirCalls, fmt.Sprintf("正在扫描 [115] %s（已扫 %d 个目录）…", t.Name, apiCalls))
			s.center.addLog(fmt.Sprintf("扫描进度 [115] %s：已扫 %d 个目录，已收集 %d 个文件 / %d 项待处理", t.Name, apiCalls, len(allFiles), len(out)))
		}
	}

	needDirBFS := req.EnableDateArchive || req.EnableEmptyDir || req.EnableFlatten || req.EnableClassify || (req.EnableJunk && len(keywords) > 0) || !recursiveUsed
	for needDirBFS && len(queue) > 0 {
		if ctx.Err() != nil {
			return out
		}
		if apiCalls >= maxCloudDirCalls {
			if !budgetWarned {
				budgetWarned = true
				s.center.addLog(fmt.Sprintf("[115] %s 目录探测已达单目标上限（%d 次请求），文件级扫描已全部完成，执行本批后可再扫剩余目录", t.Name, maxCloudDirCalls))
			}
			break
		}
		cur := queue[0]
		queue = queue[1:]
		if cur.cid == "" || visited[cur.cid] {
			continue
		}
		visited[cur.cid] = true
		apiCalls++

		files, err := listAllCloudFiles(ctx, cloud, cur.cid)
		if err != nil {
			s.center.addLog(fmt.Sprintf("读取 115 目录 %s 失败：%s", cur.cid, err.Error()))
			if isCloudScanFatalErr(err) {
				s.center.addLog("检测到 115 熔断/限流/认证异常，已停止继续扫描当前目标。")
				break
			}
			continue
		}
		reportProgress()

		// Reuse this single BFS directory listing for empty_dir / junk folder / flatten on subfolders (zero extra API calls!).
		if cur.depth > 0 && !activeCIDs[cur.cid] {
			if req.EnableEmptyDir && len(files) == 0 {
				deletedDirCIDs[cur.cid] = true
				out = append(out, OrganizeItem{
					ID:         makeID(),
					Tool:       "empty_dir",
					Scope:      "cloud",
					TargetName: t.Name,
					Name:       cur.name,
					Detail:     "空文件夹",
					Action:     "delete",
					DestLabel:  "移入 115 回收站",
					SourceCID:  cur.parentCID,
					FID:        cur.cid,
				})
				continue
			}
			if req.EnableJunk && len(keywords) > 0 && len(files) > 0 {
				if hit, reason := evalCloudJunkFolder(cur.name, files, keywords, safeExts, kwMaxBytes, wbCfg.JunkKeywordMaxMB); hit {
					deletedDirCIDs[cur.cid] = true
					out = append(out, OrganizeItem{
						ID:         makeID(),
						Tool:       "junk",
						Scope:      "cloud",
						TargetName: t.Name,
						Name:       cur.name,
						Detail:     reason,
						Action:     "delete",
						DestLabel:  "移入 115 回收站",
						SourceCID:  cur.parentCID,
						FID:        cur.cid,
					})
					continue
				}
			}
			if req.EnableFlatten && len(files) == 1 && !strictDateDirRe.MatchString(cur.name) {
				child := files[0]
				flattenedCIDs[cur.cid] = true
				out = append(out, OrganizeItem{
					ID:         makeID(),
					Tool:       "flatten",
					Scope:      "cloud",
					TargetName: t.Name,
					Name:       cur.name + "/" + child.Name,
					Detail:     "单子项套娃文件夹",
					Action:     "flatten",
					DestLabel:  "提至上层并清壳",
					SourceCID:  cur.parentCID,
					FID:        cur.cid,
					ChildFID:   child.FID,
					ChildName:  child.Name,
				})
				continue
			}
		}

		for _, f := range files {
			if ctx.Err() != nil {
				return out
			}
			if f.IsDir {
				// 1. Date folder archive (top-level only, yesterday & earlier)
				if cur.depth == 0 && req.EnableDateArchive && strictDateDirRe.MatchString(f.Name) && f.Name < todayStr {
					if activeDates[f.Name] || activeCIDs[f.FID] {
						skipCIDs[f.FID] = true
						continue
					}
					if req.EnableEmptyDir && apiCalls < maxCloudDirCalls {
						apiCalls++
						sub, subErr := cloud.Files(ctx, f.FID, 0)
						reportProgress()
						if subErr == nil && len(sub.Files) == 0 {
							deletedDirCIDs[f.FID] = true
							out = append(out, OrganizeItem{
								ID:         makeID(),
								Tool:       "empty_dir",
								Scope:      "cloud",
								TargetName: t.Name,
								Name:       f.Name,
								Detail:     "空日期目录（昨日及以前）",
								Action:     "delete",
								DestLabel:  "移入 115 回收站",
								SourceCID:  cur.cid,
								FID:        f.FID,
							})
							continue
						}
					}
					_, _, dispTarget := historyTargetRel(catName, f.Name)
					out = append(out, OrganizeItem{
						ID:         makeID(),
						Tool:       "date_archive",
						Scope:      "cloud",
						TargetName: t.Name,
						Name:       f.Name,
						Detail:     "旧日期目录（昨日及以前）",
						Action:     "move",
						DestLabel:  dispTarget,
						SourceCID:  cur.cid,
						RootCID:    t.CID,
						FID:        f.FID,
						DateStr:    f.Name,
					})
					continue
				}
				if isManagedCloudSkipDir(f, p115Cfg) {
					skipCIDs[f.FID] = true
					continue
				}
				// If within scan depth, enqueue into BFS — empty_dir/flatten/junkFolder will be evaluated for FREE on pop!
				if cur.depth < maxSubDepth && !visited[f.FID] {
					queue = append(queue, dirNode{
						cid:       f.FID,
						parentCID: cur.cid,
						name:      f.Name,
						depth:     cur.depth + 1,
					})
					continue
				}
				// Leaf directory (at maxSubDepth): probe for empty_dir/flatten, or for junk ONLY when folder name matches a keyword.
				kwHitFolder := req.EnableJunk && len(keywords) > 0 && !strictDateDirRe.MatchString(f.Name) && matchKeyword(f.Name, keywords) != ""
				if (req.EnableEmptyDir || req.EnableFlatten || kwHitFolder) && !activeCIDs[f.FID] {
					if apiCalls >= maxCloudDirCalls {
						if !budgetWarned {
							budgetWarned = true
							s.center.addLog(fmt.Sprintf("[115] %s 目录探测已达单目标上限（%d 次请求），文件级扫描已完成", t.Name, maxCloudDirCalls))
						}
						continue
					}
					apiCalls++
					sub, subErr := cloud.Files(ctx, f.FID, 0)
					reportProgress()
					if subErr != nil {
						if isCloudScanFatalErr(subErr) {
							s.center.addLog("检测到 115 熔断/限流/认证异常，已停止继续扫描当前目标。")
							break
						}
						continue
					}
					matchedFolderOp := false
					if req.EnableEmptyDir && len(sub.Files) == 0 {
						matchedFolderOp = true
						deletedDirCIDs[f.FID] = true
						out = append(out, OrganizeItem{
							ID:         makeID(),
							Tool:       "empty_dir",
							Scope:      "cloud",
							TargetName: t.Name,
							Name:       f.Name,
							Detail:     "空文件夹",
							Action:     "delete",
							DestLabel:  "移入 115 回收站",
							SourceCID:  cur.cid,
							FID:        f.FID,
						})
					} else if kwHitFolder && sub.NextOffset == nil {
						if hit, reason := evalCloudJunkFolder(f.Name, sub.Files, keywords, safeExts, kwMaxBytes, wbCfg.JunkKeywordMaxMB); hit {
							matchedFolderOp = true
							deletedDirCIDs[f.FID] = true
							out = append(out, OrganizeItem{
								ID:         makeID(),
								Tool:       "junk",
								Scope:      "cloud",
								TargetName: t.Name,
								Name:       f.Name,
								Detail:     reason,
								Action:     "delete",
								DestLabel:  "移入 115 回收站",
								SourceCID:  cur.cid,
								FID:        f.FID,
							})
						}
					} else if req.EnableFlatten && len(sub.Files) == 1 && sub.NextOffset == nil && !strictDateDirRe.MatchString(f.Name) {
						matchedFolderOp = true
						flattenedCIDs[f.FID] = true
						child := sub.Files[0]
						out = append(out, OrganizeItem{
							ID:         makeID(),
							Tool:       "flatten",
							Scope:      "cloud",
							TargetName: t.Name,
							Name:       f.Name + "/" + child.Name,
							Detail:     "单子项套娃文件夹",
							Action:     "flatten",
							DestLabel:  "提至上层并清壳",
							SourceCID:  cur.cid,
							FID:        f.FID,
							ChildFID:   child.FID,
							ChildName:  child.Name,
						})
					}
					// Never waste already-fetched leaf listing: collect its non-dir files for junk/rename/duplicate!
					if !matchedFolderOp {
						for _, sf := range sub.Files {
							addFileRec(sf, f.FID, cur.depth+1)
						}
					}
				}
				continue
			}

			addFileRec(f, cur.cid, cur.depth)
		}
	}

	// Exclude files whose parent folder is active/managed, already scheduled for deletion, or being flattened.
	if len(skipCIDs) > 0 || len(deletedDirCIDs) > 0 || len(flattenedCIDs) > 0 {
		filtered := allFiles[:0]
		for _, rec := range allFiles {
			if skipCIDs[rec.curCID] || deletedDirCIDs[rec.curCID] || flattenedCIDs[rec.curCID] {
				continue
			}
			filtered = append(filtered, rec)
		}
		allFiles = filtered
	}

	// 2. SHA1 Duplicate grouping with configurable keep policy
	dupFIDs := make(map[string]bool)
	if req.EnableDuplicate {
		groups := make(map[string][]cloudFileRec)
		var order []string
		for _, rec := range allFiles {
			if rec.f.SHA1 == "" {
				continue
			}
			if _, ok := groups[rec.f.SHA1]; !ok {
				order = append(order, rec.f.SHA1)
			}
			groups[rec.f.SHA1] = append(groups[rec.f.SHA1], rec)
		}
		for _, sha := range order {
			grp := groups[sha]
			if len(grp) < 2 {
				continue
			}
			names := make([]string, len(grp))
			mods := make([]int64, len(grp))
			for i, rec := range grp {
				names[i] = rec.f.Name
				mods[i] = rec.f.ModTime
			}
			keepIdx := pickDupKeeperIndex(names, mods, wbCfg.DedupKeepPolicy)
			keepName := grp[keepIdx].f.Name
			for i, rec := range grp {
				if i == keepIdx {
					continue
				}
				dupFIDs[rec.f.FID] = true
				out = append(out, OrganizeItem{
					ID:         makeID(),
					Tool:       "duplicate",
					Scope:      "cloud",
					TargetName: t.Name,
					Name:       rec.f.Name,
					SHA1:       rec.f.SHA1,
					Size:       rec.f.Size,
					Detail:     "保留「" + keepName + "」",
					Action:     "delete",
					DestLabel:  "移入 115 回收站",
					SourceCID:  rec.curCID,
					FID:        rec.f.FID,
				})
			}
		}
	}

	// 3. Junk, Rename, and Classify on non-duplicate files
	for _, rec := range allFiles {
		if dupFIDs[rec.f.FID] {
			continue
		}
		f := rec.f
		isJunk := false
		if req.EnableJunk {
			if hit, reason := evalJunkFile(f.Name, f.SHA1, f.Size, junkExts, safeExts, sha1BL, keywords, kwMaxBytes, junkMaxBytes, wbCfg.JunkMaxMB, wbCfg.JunkKeywordMaxMB); hit {
				isJunk = true
				out = append(out, OrganizeItem{
					ID:         makeID(),
					Tool:       "junk",
					Scope:      "cloud",
					TargetName: t.Name,
					Name:       f.Name,
					SHA1:       f.SHA1,
					Size:       f.Size,
					Detail:     reason,
					Action:     "delete",
					DestLabel:  "移入 115 回收站",
					SourceCID:  rec.curCID,
					FID:        f.FID,
				})
			}
		}
		if isJunk {
			continue
		}
		if req.EnableRename {
			if nn, reason := cleanFilename(f.Name, wbCfg); nn != f.Name {
				out = append(out, OrganizeItem{
					ID:         makeID(),
					Tool:       "rename",
					Scope:      "cloud",
					TargetName: t.Name,
					Name:       f.Name,
					NewName:    nn,
					Size:       f.Size,
					Detail:     reason,
					Action:     "rename",
					DestLabel:  "→ " + nn,
					SourceCID:  rec.curCID,
					FID:        f.FID,
				})
			}
		}
		if req.EnableClassify && rec.depth == 0 {
			subDir := classifyFolderName(f.Name, f.ModTime, wbCfg.ClassifyMode, s.loc)
			out = append(out, OrganizeItem{
				ID:         makeID(),
				Tool:       "classify",
				Scope:      "cloud",
				TargetName: t.Name,
				Name:       f.Name,
				Size:       f.Size,
				Detail:     "归类到「" + subDir + "」",
				Action:     "move",
				DestLabel:  t.Name + "/" + subDir,
				SourceCID:  rec.curCID,
				RootCID:    t.CID,
				FID:        f.FID,
				DestSubdir: subDir,
			})
		}
	}
	return out
}

func (s *Service) scanLocalCenterTarget(
	ctx context.Context,
	t resolvedCenterTarget,
	req CenterScanRequest,
	wbCfg WorkbenchConfig,
	todayStr string,
	activeDates map[string]bool,
	makeID func() string,
) []OrganizeItem {
	type dirNode struct {
		path  string
		depth int
	}
	maxSubDepth := t.Depth - 1
	queue := []dirNode{{path: t.Path, depth: 0}}
	visited := make(map[string]bool)
	const maxDirs = 100

	junkExts := parseExtSet(wbCfg.JunkExts)
	safeExts := parseExtSet(wbCfg.JunkSafeExts)
	sha1BL := make(map[string]bool, len(wbCfg.JunkSHA1Blacklist))
	for _, h := range wbCfg.JunkSHA1Blacklist {
		sha1BL[strings.ToLower(h)] = true
	}
	keywords := parseKeywords(wbCfg.JunkKeywords)
	kwMaxBytes := int64(wbCfg.JunkKeywordMaxMB * 1024 * 1024)
	junkMaxBytes := int64(wbCfg.JunkMaxMB * 1024 * 1024)

	var out []OrganizeItem
	sizeBuckets := make(map[int64][]string)
	type fileEntry struct {
		path  string
		dir   string
		name  string
		size  int64
		mod   int64
		depth int
	}
	var allFiles []fileEntry
	catName := sanitizeArchiveCatName(t.Name)

	for len(queue) > 0 && len(visited) < maxDirs {
		if ctx.Err() != nil {
			return out
		}
		cur := queue[0]
		queue = queue[1:]
		if cur.path == "" || visited[cur.path] {
			continue
		}
		visited[cur.path] = true

		entries, err := os.ReadDir(cur.path)
		if err != nil {
			s.center.addLog(fmt.Sprintf("读取本地目录 %s 失败：%s", cur.path, err.Error()))
			continue
		}

		for _, e := range entries {
			if ctx.Err() != nil {
				return out
			}
			full := filepath.Join(cur.path, e.Name())
			if e.IsDir() {
				if cur.depth == 0 && req.EnableDateArchive && strictDateDirRe.MatchString(e.Name()) && e.Name() < todayStr {
					if activeDates[e.Name()] {
						continue
					}
					if req.EnableEmptyDir {
						if children, rErr := os.ReadDir(full); rErr == nil && len(children) == 0 {
							out = append(out, OrganizeItem{
								ID:         makeID(),
								Tool:       "empty_dir",
								Scope:      "local",
								TargetName: t.Name,
								Name:       e.Name(),
								Detail:     "空日期目录（昨日及以前）",
								Action:     "delete",
								DestLabel:  "删除空目录",
								SourcePath: full,
							})
							continue
						}
					}
					_, _, dispTarget := historyTargetRel(catName, e.Name())
					out = append(out, OrganizeItem{
						ID:         makeID(),
						Tool:       "date_archive",
						Scope:      "local",
						TargetName: t.Name,
						Name:       e.Name(),
						Detail:     "旧日期目录（昨日及以前）",
						Action:     "move",
						DestLabel:  dispTarget,
						SourcePath: full,
						RootPath:   t.Path,
						DateStr:    e.Name(),
					})
					continue
				}
				kwHitLocalDir := req.EnableJunk && len(keywords) > 0 && !strictDateDirRe.MatchString(e.Name()) && matchKeyword(e.Name(), keywords) != ""
				if req.EnableEmptyDir || req.EnableFlatten || kwHitLocalDir {
					if children, rErr := os.ReadDir(full); rErr == nil {
						if req.EnableEmptyDir && len(children) == 0 {
							out = append(out, OrganizeItem{
								ID:         makeID(),
								Tool:       "empty_dir",
								Scope:      "local",
								TargetName: t.Name,
								Name:       e.Name(),
								Detail:     "空文件夹",
								Action:     "delete",
								DestLabel:  "删除空目录",
								SourcePath: full,
							})
							continue
						}
						if kwHitLocalDir && len(children) > 0 {
							if hit, reason := evalLocalJunkFolder(e.Name(), children, keywords, safeExts, kwMaxBytes, wbCfg.JunkKeywordMaxMB); hit {
								out = append(out, OrganizeItem{
									ID:         makeID(),
									Tool:       "junk",
									Scope:      "local",
									TargetName: t.Name,
									Name:       e.Name(),
									Detail:     reason,
									Action:     "delete",
									DestLabel:  "删除广告文件夹",
									SourcePath: full,
								})
								continue
							}
						}
						if req.EnableFlatten && len(children) == 1 && !strictDateDirRe.MatchString(e.Name()) {
							ch := children[0]
							out = append(out, OrganizeItem{
								ID:         makeID(),
								Tool:       "flatten",
								Scope:      "local",
								TargetName: t.Name,
								Name:       e.Name() + "/" + ch.Name(),
								Detail:     "单子项套娃文件夹",
								Action:     "flatten",
								DestLabel:  "提至上层并清壳",
								SourcePath: full,
								RootPath:   cur.path,
								ChildPath:  filepath.Join(full, ch.Name()),
								ChildName:  ch.Name(),
							})
							continue
						}
					}
				}
				if cur.depth < maxSubDepth && !visited[full] {
					queue = append(queue, dirNode{path: full, depth: cur.depth + 1})
				}
				continue
			}

			info, infoErr := e.Info()
			if infoErr != nil {
				continue
			}
			size := info.Size()
			mod := info.ModTime().Unix()
			if size > 0 {
				sizeBuckets[size] = append(sizeBuckets[size], full)
			}
			allFiles = append(allFiles, fileEntry{path: full, dir: cur.path, name: e.Name(), size: size, mod: mod, depth: cur.depth})
		}
	}

	dupPaths := make(map[string]bool)
	if req.EnableDuplicate && len(allFiles) > 1 {
		groups := make(map[string][]fileEntry)
		var order []string
		for _, fc := range allFiles {
			if len(sizeBuckets[fc.size]) < 2 {
				continue
			}
			h, err := hashFileSHA1(fc.path)
			if err != nil {
				continue
			}
			if _, ok := groups[h]; !ok {
				order = append(order, h)
			}
			groups[h] = append(groups[h], fc)
		}
		for _, sha := range order {
			grp := groups[sha]
			if len(grp) < 2 {
				continue
			}
			names := make([]string, len(grp))
			mods := make([]int64, len(grp))
			for i, fc := range grp {
				names[i] = fc.name
				mods[i] = fc.mod
			}
			keepIdx := pickDupKeeperIndex(names, mods, wbCfg.DedupKeepPolicy)
			keepName := grp[keepIdx].name
			for i, fc := range grp {
				if i == keepIdx {
					continue
				}
				dupPaths[fc.path] = true
				out = append(out, OrganizeItem{
					ID:         makeID(),
					Tool:       "duplicate",
					Scope:      "local",
					TargetName: t.Name,
					Name:       fc.name,
					SHA1:       sha,
					Size:       fc.size,
					Detail:     "保留「" + keepName + "」",
					Action:     "delete",
					DestLabel:  "删除重复副本",
					SourcePath: fc.path,
				})
			}
		}
	}

	for _, fc := range allFiles {
		if dupPaths[fc.path] {
			continue
		}
		isJunk := false
		if req.EnableJunk {
			var sha string
			if len(sha1BL) > 0 && fc.size > 0 && fc.size <= 50*1024*1024 {
				sha, _ = hashFileSHA1(fc.path)
			}
			if hit, reason := evalJunkFile(fc.name, sha, fc.size, junkExts, safeExts, sha1BL, keywords, kwMaxBytes, junkMaxBytes, wbCfg.JunkMaxMB, wbCfg.JunkKeywordMaxMB); hit {
				isJunk = true
				if sha == "" && fc.size > 0 && fc.size <= 50*1024*1024 {
					sha, _ = hashFileSHA1(fc.path)
				}
				out = append(out, OrganizeItem{
					ID:         makeID(),
					Tool:       "junk",
					Scope:      "local",
					TargetName: t.Name,
					Name:       fc.name,
					SHA1:       sha,
					Size:       fc.size,
					Detail:     reason,
					Action:     "delete",
					DestLabel:  "删除文件",
					SourcePath: fc.path,
				})
			}
		}
		if isJunk {
			continue
		}
		if req.EnableRename {
			if nn, reason := cleanFilename(fc.name, wbCfg); nn != fc.name {
				out = append(out, OrganizeItem{
					ID:         makeID(),
					Tool:       "rename",
					Scope:      "local",
					TargetName: t.Name,
					Name:       fc.name,
					NewName:    nn,
					Size:       fc.size,
					Detail:     reason,
					Action:     "rename",
					DestLabel:  "→ " + nn,
					SourcePath: fc.path,
					RootPath:   fc.dir,
				})
			}
		}
		if req.EnableClassify && fc.depth == 0 {
			subDir := classifyFolderName(fc.name, fc.mod, wbCfg.ClassifyMode, s.loc)
			out = append(out, OrganizeItem{
				ID:         makeID(),
				Tool:       "classify",
				Scope:      "local",
				TargetName: t.Name,
				Name:       fc.name,
				Size:       fc.size,
				Detail:     "归类到「" + subDir + "」",
				Action:     "move",
				DestLabel:  t.Name + "/" + subDir,
				SourcePath: fc.path,
				RootPath:   t.Path,
				DestSubdir: subDir,
			})
		}
	}
	return out
}

// StartCenterExec asynchronously executes ONLY the user-checked item IDs from the cached scan plan,
// using 115 BatchMove/BatchDelete (up to 50 items per request) for maximum anti-risk-control efficiency.
func (s *Service) StartCenterExec(itemIDs []string) (string, error) {
	if s.center == nil {
		return "", errors.New("NO_SCAN_PLAN")
	}
	s.center.mu.Lock()
	if s.center.state.Status == "running" {
		s.center.mu.Unlock()
		return "", errors.New("ORGANIZE_JOB_BUSY")
	}
	if s.center.state.Plan == nil || len(s.center.state.Plan.Items) == 0 {
		s.center.mu.Unlock()
		return "", errors.New("NO_SCAN_PLAN")
	}
	idSet := make(map[string]bool, len(itemIDs))
	for _, id := range itemIDs {
		idSet[id] = true
	}
	var selected []OrganizeItem
	for _, it := range s.center.state.Plan.Items {
		if idSet[it.ID] {
			selected = append(selected, it)
		}
	}
	if len(selected) == 0 {
		s.center.mu.Unlock()
		return "", errors.New("NO_ITEMS_SELECTED")
	}

	jobID := store.NewID("org")
	jobCtx, cancel := context.WithCancel(context.Background())
	s.center.cancel = cancel
	s.center.state.JobID = jobID
	s.center.state.Mode = "exec"
	s.center.state.Status = "running"
	s.center.state.ProgressCur = 0
	s.center.state.ProgressTotal = len(selected)
	s.center.state.StageLabel = fmt.Sprintf("准备执行 %d 项整理操作…", len(selected))
	s.center.state.Result = nil
	s.center.state.StartedAt = time.Now().Unix()
	s.center.state.FinishedAt = 0
	s.center.addLogLocked(fmt.Sprintf("开始执行已勾选的 %d 项整理操作…", len(selected)))
	s.center.mu.Unlock()

	go s.runCenterExecJob(jobCtx, jobID, selected)
	return jobID, nil
}

func (s *Service) runCenterExecJob(ctx context.Context, jobID string, selected []OrganizeItem) {
	res := &CenterExecResult{}
	completedIDs := make(map[string]bool)
	var undoBatch []RenameUndoEntry
	var deletedJunkSHA1s []string

	defer func() {
		if len(deletedJunkSHA1s) > 0 {
			if added, err := s.AddJunkSHA1s(context.Background(), deletedJunkSHA1s); err == nil && added > 0 {
				s.center.addLog(fmt.Sprintf("已自动将 %d 个已删垃圾文件的 SHA1 加入黑名单", added))
			}
		}
		s.center.mu.Lock()
		if len(undoBatch) > 0 {
			s.center.lastRenames = undoBatch
			s.center.state.UndoRenameCount = len(undoBatch)
		}
		// Remove completed items from cached plan so UI stays accurate.
		if s.center.state.Plan != nil && len(completedIDs) > 0 {
			rem := make([]OrganizeItem, 0, len(s.center.state.Plan.Items))
			counts := map[string]int{"date_archive": 0, "duplicate": 0, "junk": 0, "classify": 0, "empty_dir": 0, "rename": 0, "flatten": 0}
			for _, it := range s.center.state.Plan.Items {
				if completedIDs[it.ID] {
					continue
				}
				rem = append(rem, it)
				counts[it.Tool]++
			}
			s.center.state.Plan.Items = rem
			s.center.state.Plan.Counts = counts
		}
		s.center.state.Result = res
		if s.center.state.JobID == jobID && s.center.state.Status == "running" {
			if ctx.Err() != nil {
				s.center.state.Status = "cancelled"
				s.center.state.StageLabel = "已停止执行"
				s.center.addLogLocked("执行已中止。")
			} else {
				s.center.state.Status = "done"
				s.center.state.StageLabel = fmt.Sprintf("执行完成：移动 %d · 改名 %d · 清理 %d · 失败 %d",
					res.MovedCount, res.RenamedCount, res.DeletedCount, res.FailedCount)
			}
		}
		s.center.cancel = nil
		s.center.state.FinishedAt = time.Now().Unix()
		s.center.mu.Unlock()

		if res.MovedCount > 0 || res.DeletedCount > 0 || res.RenamedCount > 0 {
			msg := fmt.Sprintf("移动 %d 项，改名 %d 项，清理 %d 项（失败 %d 项）",
				res.MovedCount, res.RenamedCount, res.DeletedCount, res.FailedCount)
			_ = s.store.RecordEvent(context.Background(), "system", "DATE_ORGANIZED", "organize", msg, true)
		}
	}()

	cloud, p115Cfg := s.snapshotCloud()
	doneCount := 0

	// 1. Execute local items first (instant, no rate limit) in safe multi-select phase order:
	// delete -> rename -> flatten -> move, propagating renamed paths so rename+classify on the same file both succeed.
	var localOrdered []OrganizeItem
	for _, phase := range []string{"delete", "rename", "flatten", "move"} {
		for _, it := range selected {
			if it.Scope != "local" {
				continue
			}
			act := it.Action
			if act != "delete" && act != "rename" && act != "flatten" {
				act = "move"
			}
			if act == phase {
				localOrdered = append(localOrdered, it)
			}
		}
	}
	renamedPathMap := make(map[string]string)
	for _, it := range localOrdered {
		if ctx.Err() != nil {
			return
		}
		doneCount++
		s.center.setProgress(doneCount, len(selected), fmt.Sprintf("处理本地：%s（%d/%d）", it.Name, doneCount, len(selected)))
		srcPath := it.SourcePath
		if mapped, ok := renamedPathMap[srcPath]; ok {
			srcPath = mapped
		}
		switch it.Action {
		case "delete":
			var delErr error
			if it.Tool == "junk" {
				if fi, statErr := os.Stat(srcPath); statErr == nil && fi.IsDir() {
					delErr = os.RemoveAll(srcPath)
				} else {
					delErr = os.Remove(srcPath)
				}
			} else {
				delErr = os.Remove(srcPath)
			}
			if delErr != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, it.Name+": "+delErr.Error())
				s.center.addLog("本地清理失败：" + it.Name + "（" + delErr.Error() + "）")
			} else {
				res.DeletedCount++
				completedIDs[it.ID] = true
				if it.Tool == "junk" && it.SHA1 != "" && it.Size > 0 {
					deletedJunkSHA1s = append(deletedJunkSHA1s, it.SHA1)
				}
				s.center.addLog("已清理本地：" + it.Name)
			}
		case "rename":
			destPath := filepath.Join(it.RootPath, it.NewName)
			if _, statErr := os.Stat(destPath); statErr == nil {
				res.FailedCount++
				res.Errors = append(res.Errors, it.Name+": 目标文件名已存在")
				s.center.addLog("本地改名跳过（重名）：" + it.Name)
				continue
			}
			if err := os.Rename(srcPath, destPath); err != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, it.Name+": "+err.Error())
				s.center.addLog("本地改名失败：" + it.Name + "（" + err.Error() + "）")
			} else {
				renamedPathMap[it.SourcePath] = destPath
				res.RenamedCount++
				completedIDs[it.ID] = true
				undoBatch = append(undoBatch, RenameUndoEntry{
					Scope: "local", LocalDir: it.RootPath, OldName: it.Name, NewName: it.NewName,
				})
				s.center.addLog("已改名本地：" + it.Name + " → " + it.NewName)
			}
		case "flatten":
			destPath := uniqueDestPath(filepath.Join(it.RootPath, it.ChildName))
			if err := os.Rename(it.ChildPath, destPath); err != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, it.Name+": "+err.Error())
				s.center.addLog("本地解包失败：" + it.Name + "（" + err.Error() + "）")
			} else {
				_ = os.Remove(srcPath)
				res.MovedCount++
				completedIDs[it.ID] = true
				s.center.addLog("已解包本地：" + it.Name)
			}
		default: // move
			var destParent string
			if it.Tool == "date_archive" {
				cat := sanitizeArchiveCatName(it.TargetName)
				yearDir, monthDir, _ := historyTargetRel(cat, it.DateStr)
				destParent = filepath.Join(s.historyDir(), cat, yearDir, monthDir)
			} else {
				destParent = filepath.Join(it.RootPath, it.DestSubdir)
			}
			if err := os.MkdirAll(destParent, 0o755); err != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, it.Name+": "+err.Error())
				continue
			}
			destPath := uniqueDestPath(filepath.Join(destParent, filepath.Base(srcPath)))
			if err := os.Rename(srcPath, destPath); err != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, it.Name+": "+err.Error())
				s.center.addLog("本地移动失败：" + it.Name + "（" + err.Error() + "）")
			} else {
				res.MovedCount++
				completedIDs[it.ID] = true
				s.center.addLog("已移动本地：" + filepath.Base(srcPath) + " → " + it.DestLabel)
			}
		}
	}

	// 2. Partition cloud items into cross-directory global batches (50 per request!).
	var cloudDeletes []OrganizeItem
	var cloudRenames []OrganizeItem
	var cloudFlattens []OrganizeItem
	var cloudMoves []OrganizeItem
	for _, it := range selected {
		if it.Scope != "cloud" {
			continue
		}
		switch it.Action {
		case "delete":
			cloudDeletes = append(cloudDeletes, it)
		case "rename":
			cloudRenames = append(cloudRenames, it)
		case "flatten":
			cloudFlattens = append(cloudFlattens, it)
		default:
			cloudMoves = append(cloudMoves, it)
		}
	}

	if cloud == nil && (len(cloudDeletes) > 0 || len(cloudRenames) > 0 || len(cloudFlattens) > 0 || len(cloudMoves) > 0) {
		s.center.addLog("跳过 115 云端项：尚未配置 115 Cookie。")
		res.SkippedCount += len(cloudMoves) + len(cloudFlattens) + len(cloudDeletes) + len(cloudRenames)
		return
	}

	// 2a. Global cross-directory BatchRename (up to 50 per HTTP request!)
	const batchChunk = 50
	for i := 0; i < len(cloudRenames); i += batchChunk {
		if ctx.Err() != nil {
			return
		}
		end := i + batchChunk
		if end > len(cloudRenames) {
			end = len(cloudRenames)
		}
		grp := cloudRenames[i:end]
		rmap := make(map[string]string, len(grp))
		for _, it := range grp {
			rmap[it.FID] = it.NewName
		}
		s.center.setProgress(doneCount, len(selected), fmt.Sprintf("115 跨目录批量改名（%d/%d）…", end, len(cloudRenames)))
		if err := cloud.BatchRename(ctx, "", rmap); err != nil {
			res.FailedCount += len(grp)
			res.Errors = append(res.Errors, "批量改名失败: "+err.Error())
			s.center.addLog(fmt.Sprintf("115 批量改名 %d 项失败：%s", len(rmap), err.Error()))
		} else {
			res.RenamedCount += len(grp)
			for _, it := range grp {
				completedIDs[it.ID] = true
				undoBatch = append(undoBatch, RenameUndoEntry{
					Scope: "cloud", SourceCID: it.SourceCID, FID: it.FID, OldName: it.Name, NewName: it.NewName,
				})
			}
			s.center.addLog(fmt.Sprintf("115 批量改名成功：%d 项（1 次批处理，可一键撤销）", len(rmap)))
		}
		doneCount += len(grp)
	}

	// 2b. Flatten single-child cloud folders (move child up, then global batch-recycle all empty wrappers 50 at a time!)
	var flattenShells []string
	for _, it := range cloudFlattens {
		if ctx.Err() != nil {
			return
		}
		s.center.setProgress(doneCount, len(selected), "解包套娃："+it.Name)
		if err := cloud.BatchMove(ctx, it.FID, []string{it.ChildFID}, it.SourceCID); err != nil {
			res.FailedCount++
			res.Errors = append(res.Errors, it.Name+": "+err.Error())
			s.center.addLog("115 解包失败：" + it.Name + "（" + err.Error() + "）")
		} else {
			flattenShells = append(flattenShells, it.FID)
			res.MovedCount++
			completedIDs[it.ID] = true
			s.center.addLog("115 解包成功：" + it.Name)
		}
		doneCount++
	}
	if len(flattenShells) > 0 && ctx.Err() == nil {
		if err := cloud.BatchDelete(ctx, "", flattenShells); err != nil {
			s.center.addLog(fmt.Sprintf("115 批量清理 %d 个解包空壳失败：%s", len(flattenShells), err.Error()))
		} else {
			s.center.addLog(fmt.Sprintf("115 批量清理 %d 个解包空壳完成（全局批处理）", len(flattenShells)))
		}
	}

	// 2c. Global cross-directory BatchDelete (up to 50 per HTTP request!)
	for i := 0; i < len(cloudDeletes); i += batchChunk {
		if ctx.Err() != nil {
			return
		}
		end := i + batchChunk
		if end > len(cloudDeletes) {
			end = len(cloudDeletes)
		}
		grp := cloudDeletes[i:end]
		fids := make([]string, 0, len(grp))
		for _, it := range grp {
			fids = append(fids, it.FID)
		}
		s.center.setProgress(doneCount, len(selected), fmt.Sprintf("115 跨目录批量清理（%d/%d）…", end, len(cloudDeletes)))
		if err := cloud.BatchDelete(ctx, "", fids); err != nil {
			res.FailedCount += len(grp)
			res.Errors = append(res.Errors, "批量清理失败: "+err.Error())
			s.center.addLog(fmt.Sprintf("115 批量清理 %d 项失败：%s", len(fids), err.Error()))
		} else {
			res.DeletedCount += len(grp)
			for _, it := range grp {
				completedIDs[it.ID] = true
				if it.Tool == "junk" && it.SHA1 != "" && it.Size > 0 {
					deletedJunkSHA1s = append(deletedJunkSHA1s, it.SHA1)
				}
			}
			s.center.addLog(fmt.Sprintf("115 批量清理成功：%d 项已移入回收站（1 次批处理请求）", len(fids)))
		}
		doneCount += len(grp)
	}

	// 3. Resolve cloud move destinations and group by (SourceCID, TargetCID) for BatchMove (up to 50 per request!).
	cloudHistoryCID := p115Cfg.CloudHistoryCID
	ensureCloudHistory := func() (string, error) {
		if cloudHistoryCID != "" {
			return cloudHistoryCID, nil
		}
		zoneCID := p115Cfg.CloudArchiveZoneCID
		if zoneCID == "" && p115Cfg.CloudRootCID != "" {
			z, err := cloud.EnsureFolder(ctx, p115Cfg.CloudRootCID, "归档区")
			if err != nil {
				return "", err
			}
			zoneCID = z
			p115Cfg.CloudArchiveZoneCID = z
		}
		if zoneCID == "" {
			return "", ErrCloudRootRequired
		}
		h, err := cloud.EnsureFolder(ctx, zoneCID, "历史归档")
		if err != nil {
			return "", err
		}
		cloudHistoryCID = h
		p115Cfg.CloudHistoryCID = h
		_, _ = s.SaveP115Config(ctx, p115Cfg)
		return h, nil
	}

	folderCache := make(map[string]string)
	type moveKey struct {
		srcCID string
		dstCID string
	}
	moveBatches := make(map[moveKey][]OrganizeItem)
	moveLabels := make(map[moveKey]string)

	for _, it := range cloudMoves {
		if ctx.Err() != nil {
			return
		}
		var dstCID string
		var dstGroupLabel string
		var err error
		if it.Tool == "date_archive" {
			histRoot, hErr := ensureCloudHistory()
			if hErr != nil {
				err = hErr
			} else {
				cat := sanitizeArchiveCatName(it.TargetName)
				yearDir, monthDir, _ := historyTargetRel(cat, it.DateStr)
				dstGroupLabel = fmt.Sprintf("归档区/历史归档/%s/%s/%s", cat, yearDir, monthDir)
				ck := "hist:" + cat + "/" + yearDir + "/" + monthDir
				if cached, ok := folderCache[ck]; ok {
					dstCID = cached
				} else {
					var catCID, yearCID string
					catCID, err = cloud.EnsureFolder(ctx, histRoot, cat)
					if err == nil {
						yearCID, err = cloud.EnsureFolder(ctx, catCID, yearDir)
					}
					if err == nil {
						dstCID, err = cloud.EnsureFolder(ctx, yearCID, monthDir)
					}
					if err == nil {
						folderCache[ck] = dstCID
					}
				}
			}
		} else {
			dstGroupLabel = it.DestLabel
			ck := "sub:" + it.RootCID + "/" + it.DestSubdir
			if cached, ok := folderCache[ck]; ok {
				dstCID = cached
			} else {
				dstCID, err = cloud.EnsureFolder(ctx, it.RootCID, it.DestSubdir)
				if err == nil {
					folderCache[ck] = dstCID
				}
			}
		}
		if err != nil {
			res.FailedCount++
			res.Errors = append(res.Errors, it.Name+": "+err.Error())
			s.center.addLog("创建 115 目标目录失败：" + it.Name + "（" + err.Error() + "）")
			doneCount++
			continue
		}
		mk := moveKey{srcCID: it.SourceCID, dstCID: dstCID}
		moveBatches[mk] = append(moveBatches[mk], it)
		if _, ok := moveLabels[mk]; !ok {
			moveLabels[mk] = dstGroupLabel
		}
	}

	// Deterministic batch order
	keys := make([]moveKey, 0, len(moveBatches))
	for k := range moveBatches {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return moveLabels[keys[i]] < moveLabels[keys[j]] })

	for _, mk := range keys {
		if ctx.Err() != nil {
			return
		}
		grp := moveBatches[mk]
		lbl := moveLabels[mk]
		fids := make([]string, 0, len(grp))
		for _, it := range grp {
			fids = append(fids, it.FID)
		}
		s.center.setProgress(doneCount, len(selected), fmt.Sprintf("批量移动 %d 项 → %s…", len(fids), lbl))
		if err := cloud.BatchMove(ctx, mk.srcCID, fids, mk.dstCID); err != nil {
			res.FailedCount += len(grp)
			res.Errors = append(res.Errors, lbl+": "+err.Error())
			s.center.addLog(fmt.Sprintf("115 批量移动 %d 项失败：%s", len(fids), err.Error()))
		} else {
			res.MovedCount += len(grp)
			for _, it := range grp {
				completedIDs[it.ID] = true
				if it.Tool == "date_archive" {
					s.invalidateDateCache(it.SourceCID, it.DateStr)
				}
			}
			s.center.addLog(fmt.Sprintf("115 批量移动成功：%d 项 → %s", len(fids), lbl))
		}
		doneCount += len(grp)
	}
}

func parseExtSet(raw string) map[string]bool {
	if strings.TrimSpace(raw) == "" {
		raw = ".txt,.url,.html,.htm,.apk,.nfo,.lnk"
	}
	out := make(map[string]bool)
	for _, part := range strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == ' ' || r == ';' || r == '，'
	}) {
		p := strings.ToLower(strings.TrimSpace(part))
		if p == "" {
			continue
		}
		if !strings.HasPrefix(p, ".") {
			p = "." + p
		}
		out[p] = true
	}
	return out
}

func classifyFolderName(name string, modUnix int64, mode string, loc *time.Location) string {
	if mode == "month" {
		if modUnix > 0 {
			return time.Unix(modUnix, 0).In(loc).Format("2006-01")
		}
		return time.Now().In(loc).Format("2006-01")
	}
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".ts", ".m4v", ".webm", ".rmvb", ".mpg", ".mpeg":
		return "视频"
	case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".heic", ".avif", ".tiff":
		return "图片"
	case ".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".iso":
		return "压缩包"
	case ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".md", ".epub":
		return "文档"
	case ".mp3", ".flac", ".wav", ".aac", ".m4a", ".ogg":
		return "音频"
	default:
		return "其他"
	}
}

func hashFileSHA1(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha1.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func isCloudScanFatalErr(err error) bool {
	if err == nil {
		return false
	}
	var ae *p115.AdapterError
	if errors.As(err, &ae) {
		switch ae.Code {
		case "CIRCUIT_OPEN", "AUTH_REQUIRED", "RATE_LIMITED", "BUDGET_EXCEEDED":
			return true
		}
	}
	return false
}

// evalCloudJunkFolder applies Option A: folder name hits keyword AND contains no subfolders,
// no safe-extension files, and no file >= kwMaxBytes (default 20 MB).
func evalCloudJunkFolder(folderName string, children []p115.CloudFile, keywords []string, safeExts map[string]bool, kwMaxBytes int64, kwMaxMB float64) (bool, string) {
	if strictDateDirRe.MatchString(folderName) {
		return false, ""
	}
	kw := matchKeyword(folderName, keywords)
	if kw == "" {
		return false, ""
	}
	for _, ch := range children {
		if ch.IsDir {
			return false, ""
		}
		ext := strings.ToLower(filepath.Ext(ch.Name))
		if ext != "" && safeExts[ext] {
			return false, ""
		}
		if kwMaxBytes > 0 && ch.Size >= kwMaxBytes {
			return false, ""
		}
	}
	return true, fmt.Sprintf("广告文件夹（命中「%s」且无≥%.0fMB大文件）", kw, kwMaxMB)
}

func evalLocalJunkFolder(folderName string, children []os.DirEntry, keywords []string, safeExts map[string]bool, kwMaxBytes int64, kwMaxMB float64) (bool, string) {
	if strictDateDirRe.MatchString(folderName) {
		return false, ""
	}
	kw := matchKeyword(folderName, keywords)
	if kw == "" {
		return false, ""
	}
	for _, ch := range children {
		if ch.IsDir() {
			return false, ""
		}
		ext := strings.ToLower(filepath.Ext(ch.Name()))
		if ext != "" && safeExts[ext] {
			return false, ""
		}
		info, err := ch.Info()
		if err != nil {
			return false, ""
		}
		if kwMaxBytes > 0 && info.Size() >= kwMaxBytes {
			return false, ""
		}
	}
	return true, fmt.Sprintf("广告文件夹（命中「%s」且无≥%.0fMB大文件）", kw, kwMaxMB)
}
