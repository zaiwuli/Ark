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
	JunkExts          string   `json:"junk_exts"`   // e.g. ".txt,.url,.html,.htm,.apk,.nfo,.lnk"
	JunkMaxMB         float64  `json:"junk_max_mb"` // 0 = disabled
	EnableClassify    bool     `json:"enable_classify"`
	ClassifyMode      string   `json:"classify_mode"` // type | month
	EnableEmptyDir    bool     `json:"enable_empty_dir"`
}

type OrganizeItem struct {
	ID         string `json:"id"`
	Tool       string `json:"tool"`        // date_archive | duplicate | junk | classify | empty_dir
	Scope      string `json:"scope"`       // cloud | local
	TargetName string `json:"target_name"` // display name of source target
	Name       string `json:"name"`        // filename or folder name
	Size       int64  `json:"size"`
	Detail     string `json:"detail"`     // human-readable reason / match info
	Action     string `json:"action"`     // move | delete
	DestLabel  string `json:"dest_label"` // display destination

	// Internal execution metadata (omitted from JSON responses)
	SourceCID   string `json:"-"`
	RootCID     string `json:"-"`
	FID         string `json:"-"`
	SourcePath  string `json:"-"`
	RootPath    string `json:"-"`
	DestSubdir  string `json:"-"`
	DateStr     string `json:"-"`
}

type CenterScanPlan struct {
	GeneratedAt int64          `json:"generated_at"`
	Items       []OrganizeItem `json:"items"`
	Counts      map[string]int `json:"counts"` // tool -> count
}

type CenterExecResult struct {
	MovedCount   int      `json:"moved_count"`
	DeletedCount int      `json:"deleted_count"`
	FailedCount  int      `json:"failed_count"`
	SkippedCount int      `json:"skipped_count"`
	Errors       []string `json:"errors,omitempty"`
}

type CenterJobState struct {
	JobID         string            `json:"job_id"`
	Mode          string            `json:"mode"`   // "" | scan | exec
	Status        string            `json:"status"` // idle | running | done | cancelled | failed
	ProgressCur   int               `json:"progress_cur"`
	ProgressTotal int               `json:"progress_total"`
	StageLabel    string            `json:"stage_label"`
	Error         string            `json:"error,omitempty"`
	Logs          []string          `json:"logs"`
	Plan          *CenterScanPlan   `json:"plan,omitempty"`
	Result        *CenterExecResult `json:"result,omitempty"`
	StartedAt     int64             `json:"started_at,omitempty"`
	FinishedAt    int64             `json:"finished_at,omitempty"`
}

type organizeCenter struct {
	mu     sync.Mutex
	state  CenterJobState
	cancel context.CancelFunc
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
	junkExts := parseExtSet(req.JunkExts)
	junkMaxBytes := int64(req.JunkMaxMB * 1024 * 1024)
	if req.ClassifyMode != "month" {
		req.ClassifyMode = "type"
	}

	items := make([]OrganizeItem, 0)
	counts := map[string]int{
		"date_archive": 0,
		"duplicate":    0,
		"junk":         0,
		"classify":     0,
		"empty_dir":    0,
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
			found := s.scanCloudCenterTarget(ctx, cloud, p115Cfg, t, req, todayStr, activeDates, activeCIDs, junkExts, junkMaxBytes, makeID)
			for _, it := range found {
				items = append(items, it)
				counts[it.Tool]++
			}
			s.center.addLog(fmt.Sprintf("完成 [%s] %s：发现 %d 条待处理项", scopeLabel, t.Name, len(found)))
		} else {
			found := s.scanLocalCenterTarget(ctx, t, req, todayStr, activeDates, junkExts, junkMaxBytes, makeID)
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
	s.center.addLogLocked(fmt.Sprintf("扫描全部完成：日期归档 %d · 查重 %d · 垃圾 %d · 归类 %d · 空目录 %d",
		counts["date_archive"], counts["duplicate"], counts["junk"], counts["classify"], counts["empty_dir"]))
	s.center.mu.Unlock()
}

func (s *Service) scanCloudCenterTarget(
	ctx context.Context,
	cloud p115.Port,
	p115Cfg P115Settings,
	t resolvedCenterTarget,
	req CenterScanRequest,
	todayStr string,
	activeDates, activeCIDs map[string]bool,
	junkExts map[string]bool,
	junkMaxBytes int64,
	makeID func() string,
) []OrganizeItem {
	type dirNode struct {
		cid   string
		depth int
	}
	maxSubDepth := t.Depth - 1
	queue := []dirNode{{cid: t.CID, depth: 0}}
	visited := make(map[string]bool)
	const maxDirs = 40

	var out []OrganizeItem
	sha1Seen := make(map[string]string) // sha1 -> first kept filename
	catName := sanitizeArchiveCatName(t.Name)

	for len(queue) > 0 && len(visited) < maxDirs {
		if ctx.Err() != nil {
			return out
		}
		cur := queue[0]
		queue = queue[1:]
		if cur.cid == "" || visited[cur.cid] {
			continue
		}
		visited[cur.cid] = true

		files, err := listAllCloudFiles(ctx, cloud, cur.cid)
		if err != nil {
			s.center.addLog(fmt.Sprintf("读取 115 目录 %s 失败：%s", cur.cid, err.Error()))
			continue
		}

		for _, f := range files {
			if ctx.Err() != nil {
				return out
			}
			if f.IsDir {
				// 1. Date folder archive (top-level only, yesterday & earlier)
				if cur.depth == 0 && req.EnableDateArchive && strictDateDirRe.MatchString(f.Name) && f.Name < todayStr {
					if activeDates[f.Name] || activeCIDs[f.FID] {
						continue
					}
					if req.EnableEmptyDir {
						if sub, subErr := cloud.Files(ctx, f.FID, 0); subErr == nil && len(sub.Files) == 0 {
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
				// Optional empty folder check (only when explicitly enabled by user)
				if req.EnableEmptyDir && !isManagedCloudSkipDir(f, p115Cfg) && !activeCIDs[f.FID] {
					sub, subErr := cloud.Files(ctx, f.FID, 0)
					if subErr == nil && len(sub.Files) == 0 {
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
						continue
					}
				}
				if cur.depth < maxSubDepth && !isManagedCloudSkipDir(f, p115Cfg) && !visited[f.FID] {
					queue = append(queue, dirNode{cid: f.FID, depth: cur.depth + 1})
				}
				continue
			}

			// File checks: Duplicate (SHA1), Junk, Classify
			isDup := false
			if req.EnableDuplicate && f.SHA1 != "" {
				if first, exists := sha1Seen[f.SHA1]; exists {
					isDup = true
					out = append(out, OrganizeItem{
						ID:         makeID(),
						Tool:       "duplicate",
						Scope:      "cloud",
						TargetName: t.Name,
						Name:       f.Name,
						Size:       f.Size,
						Detail:     "与「" + first + "」SHA1 完全一致",
						Action:     "delete",
						DestLabel:  "移入 115 回收站",
						SourceCID:  cur.cid,
						FID:        f.FID,
					})
				} else {
					sha1Seen[f.SHA1] = f.Name
				}
			}

			isJunk := false
			if !isDup && req.EnableJunk {
				ext := strings.ToLower(filepath.Ext(f.Name))
				if junkExts[ext] {
					isJunk = true
					out = append(out, OrganizeItem{
						ID:         makeID(),
						Tool:       "junk",
						Scope:      "cloud",
						TargetName: t.Name,
						Name:       f.Name,
						Size:       f.Size,
						Detail:     "命中垃圾扩展名 " + ext,
						Action:     "delete",
						DestLabel:  "移入 115 回收站",
						SourceCID:  cur.cid,
						FID:        f.FID,
					})
				} else if junkMaxBytes > 0 && f.Size > 0 && f.Size < junkMaxBytes {
					isJunk = true
					out = append(out, OrganizeItem{
						ID:         makeID(),
						Tool:       "junk",
						Scope:      "cloud",
						TargetName: t.Name,
						Name:       f.Name,
						Size:       f.Size,
						Detail:     fmt.Sprintf("小于 %.1f MB", req.JunkMaxMB),
						Action:     "delete",
						DestLabel:  "移入 115 回收站",
						SourceCID:  cur.cid,
						FID:        f.FID,
					})
				}
			}

			// Classify scattered top-level files only (avoids breaking nested folder structures)
			if !isDup && !isJunk && req.EnableClassify && cur.depth == 0 {
				subDir := classifyFolderName(f.Name, f.ModTime, req.ClassifyMode, s.loc)
				out = append(out, OrganizeItem{
					ID:         makeID(),
					Tool:       "classify",
					Scope:      "cloud",
					TargetName: t.Name,
					Name:       f.Name,
					Size:       f.Size,
					Detail:     "归类到子目录「" + subDir + "」",
					Action:     "move",
					DestLabel:  t.Name + "/" + subDir,
					SourceCID:  cur.cid,
					RootCID:    t.CID,
					FID:        f.FID,
					DestSubdir: subDir,
				})
			}
		}
	}
	return out
}

func (s *Service) scanLocalCenterTarget(
	ctx context.Context,
	t resolvedCenterTarget,
	req CenterScanRequest,
	todayStr string,
	activeDates map[string]bool,
	junkExts map[string]bool,
	junkMaxBytes int64,
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

	var out []OrganizeItem
	sizeBuckets := make(map[int64][]string) // size -> paths for lazy SHA1 only on size collisions
	type fileEntry struct {
		path string
		name string
		size int64
		mod  int64
	}
	var dupCandidates []fileEntry
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
				if req.EnableEmptyDir {
					if children, rErr := os.ReadDir(full); rErr == nil && len(children) == 0 {
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

			if req.EnableDuplicate && size > 0 {
				sizeBuckets[size] = append(sizeBuckets[size], full)
				dupCandidates = append(dupCandidates, fileEntry{path: full, name: e.Name(), size: size, mod: mod})
			}

			isJunk := false
			if req.EnableJunk {
				ext := strings.ToLower(filepath.Ext(e.Name()))
				if junkExts[ext] {
					isJunk = true
					out = append(out, OrganizeItem{
						ID:         makeID(),
						Tool:       "junk",
						Scope:      "local",
						TargetName: t.Name,
						Name:       e.Name(),
						Size:       size,
						Detail:     "命中垃圾扩展名 " + ext,
						Action:     "delete",
						DestLabel:  "删除文件",
						SourcePath: full,
					})
				} else if junkMaxBytes > 0 && size > 0 && size < junkMaxBytes {
					isJunk = true
					out = append(out, OrganizeItem{
						ID:         makeID(),
						Tool:       "junk",
						Scope:      "local",
						TargetName: t.Name,
						Name:       e.Name(),
						Size:       size,
						Detail:     fmt.Sprintf("小于 %.1f MB", req.JunkMaxMB),
						Action:     "delete",
						DestLabel:  "删除文件",
						SourcePath: full,
					})
				}
			}

			if !isJunk && req.EnableClassify && cur.depth == 0 {
				subDir := classifyFolderName(e.Name(), mod, req.ClassifyMode, s.loc)
				out = append(out, OrganizeItem{
					ID:         makeID(),
					Tool:       "classify",
					Scope:      "local",
					TargetName: t.Name,
					Name:       e.Name(),
					Size:       size,
					Detail:     "归类到子目录「" + subDir + "」",
					Action:     "move",
					DestLabel:  t.Name + "/" + subDir,
					SourcePath: full,
					RootPath:   t.Path,
					DestSubdir: subDir,
				})
			}
		}
	}

	// Only hash local files that actually share an identical size (fast & low I/O).
	if req.EnableDuplicate && len(dupCandidates) > 1 {
		hashSeen := make(map[string]string)
		for _, fc := range dupCandidates {
			if len(sizeBuckets[fc.size]) < 2 {
				continue
			}
			h, err := hashFileSHA1(fc.path)
			if err != nil {
				continue
			}
			if first, exists := hashSeen[h]; exists {
				out = append(out, OrganizeItem{
					ID:         makeID(),
					Tool:       "duplicate",
					Scope:      "local",
					TargetName: t.Name,
					Name:       fc.name,
					Size:       fc.size,
					Detail:     "与「" + first + "」SHA1 完全一致",
					Action:     "delete",
					DestLabel:  "删除重复副本",
					SourcePath: fc.path,
				})
			} else {
				hashSeen[h] = fc.name
			}
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

	defer func() {
		s.center.mu.Lock()
		// Remove completed items from cached plan so UI stays accurate.
		if s.center.state.Plan != nil && len(completedIDs) > 0 {
			rem := make([]OrganizeItem, 0, len(s.center.state.Plan.Items))
			counts := map[string]int{"date_archive": 0, "duplicate": 0, "junk": 0, "classify": 0, "empty_dir": 0}
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
				s.center.state.StageLabel = fmt.Sprintf("执行完成：移动 %d 项 · 清理 %d 项 · 失败 %d 项",
					res.MovedCount, res.DeletedCount, res.FailedCount)
			}
		}
		s.center.cancel = nil
		s.center.state.FinishedAt = time.Now().Unix()
		s.center.mu.Unlock()

		if res.MovedCount > 0 || res.DeletedCount > 0 {
			msg := fmt.Sprintf("目录整理中心执行完成：移动 %d 项，清理 %d 项（失败 %d 项）",
				res.MovedCount, res.DeletedCount, res.FailedCount)
			_ = s.store.RecordEvent(context.Background(), "system", "DATE_ORGANIZED", "organize", msg, true)
		}
	}()

	cloud, p115Cfg := s.snapshotCloud()
	doneCount := 0

	// 1. Execute local items first (instant, no rate limit).
	for _, it := range selected {
		if ctx.Err() != nil {
			return
		}
		if it.Scope != "local" {
			continue
		}
		doneCount++
		s.center.setProgress(doneCount, len(selected), fmt.Sprintf("处理本地：%s（%d/%d）", it.Name, doneCount, len(selected)))
		if it.Action == "delete" {
			if err := os.Remove(it.SourcePath); err != nil {
				res.FailedCount++
				res.Errors = append(res.Errors, it.Name+": "+err.Error())
				s.center.addLog("本地清理失败：" + it.Name + "（" + err.Error() + "）")
			} else {
				res.DeletedCount++
				completedIDs[it.ID] = true
				s.center.addLog("已清理本地：" + it.Name)
			}
			continue
		}
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
		destPath := uniqueDestPath(filepath.Join(destParent, filepath.Base(it.SourcePath)))
		if err := os.Rename(it.SourcePath, destPath); err != nil {
			res.FailedCount++
			res.Errors = append(res.Errors, it.Name+": "+err.Error())
			s.center.addLog("本地移动失败：" + it.Name + "（" + err.Error() + "）")
		} else {
			res.MovedCount++
			completedIDs[it.ID] = true
			s.center.addLog("已移动本地：" + it.Name + " → " + it.DestLabel)
		}
	}

	// 2. Group cloud deletes by SourceCID and execute via BatchDelete (up to 50 per request!).
	cloudDelGroups := make(map[string][]OrganizeItem)
	var cloudMoves []OrganizeItem
	for _, it := range selected {
		if it.Scope != "cloud" {
			continue
		}
		if it.Action == "delete" {
			cloudDelGroups[it.SourceCID] = append(cloudDelGroups[it.SourceCID], it)
		} else {
			cloudMoves = append(cloudMoves, it)
		}
	}

	if cloud == nil && (len(cloudDelGroups) > 0 || len(cloudMoves) > 0) {
		s.center.addLog("跳过 115 云端项：尚未配置 115 Cookie。")
		res.SkippedCount += len(cloudMoves)
		for _, g := range cloudDelGroups {
			res.SkippedCount += len(g)
		}
		return
	}

	for srcCID, grp := range cloudDelGroups {
		if ctx.Err() != nil {
			return
		}
		fids := make([]string, 0, len(grp))
		for _, it := range grp {
			fids = append(fids, it.FID)
		}
		s.center.setProgress(doneCount, len(selected), fmt.Sprintf("批量移入 115 回收站（%d 项）…", len(fids)))
		if err := cloud.BatchDelete(ctx, srcCID, fids); err != nil {
			res.FailedCount += len(grp)
			res.Errors = append(res.Errors, "批量清理失败: "+err.Error())
			s.center.addLog(fmt.Sprintf("115 批量清理 %d 项失败：%s", len(fids), err.Error()))
		} else {
			res.DeletedCount += len(grp)
			for _, it := range grp {
				completedIDs[it.ID] = true
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
