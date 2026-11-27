package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/zaiwuli/Ark/internal/cleanup"
	"github.com/zaiwuli/Ark/internal/extract"
	"github.com/zaiwuli/Ark/internal/links"
	"github.com/zaiwuli/Ark/internal/localwatch"
	"github.com/zaiwuli/Ark/internal/notify"
	"github.com/zaiwuli/Ark/internal/p115"
	"github.com/zaiwuli/Ark/internal/store"
	"github.com/zaiwuli/Ark/internal/torrent"
)

var (
	ErrP115NotConfigured = errors.New("115_CONNECTION_REQUIRED")
	ErrCloudRootRequired = errors.New("CLOUD_ROOT_REQUIRED")
)

type P115Settings struct {
	Enabled             bool   `json:"enabled"`
	Cookie              string `json:"cookie"`
	CloudRootCID        string `json:"cloud_root_cid"`
	OfflineRootCID      string `json:"offline_root_cid"`
	CloudOutputCID      string `json:"cloud_output_cid"`
	CloudArchiveZoneCID string `json:"cloud_archive_zone_cid"`
	CloudArchiveCID     string `json:"cloud_archive_cid"`
	CloudFailedCID      string `json:"cloud_failed_cid"`
	CloudHistoryCID     string `json:"cloud_history_cid"`
	CloudExtractByDate  *bool  `json:"cloud_extract_by_date,omitempty"`
	LifeMonitorEnabled  bool   `json:"life_monitor_enabled"`
	CloudScanDepth      int    `json:"cloud_scan_depth,omitempty"` // 1..5, default 1
}

func (c P115Settings) ScanDepth() int {
	if c.CloudScanDepth < 1 {
		return 1
	}
	if c.CloudScanDepth > 5 {
		return 5
	}
	return c.CloudScanDepth
}

func (c P115Settings) ExtractByDateEnabled() bool {
	return true
}

type PipelinePolicy struct {
	IsTorrent   bool                `json:"is_torrent,omitempty"`
	SeedCleanup *cleanup.SeedPolicy `json:"seed_cleanup,omitempty"`
}

type Service struct {
	store            *store.Store
	gov              *p115.Governor
	loc              *time.Location
	extractor        *extract.Engine
	localWatcher     *localwatch.Watcher
	notifier         *notify.Dispatcher
	dataDir          string
	workDir          string
	extractBin       string
	extractMaxDictMB int

	mu            sync.RWMutex
	cloud         p115.Port
	portPinned    bool
	p115Cfg       P115Settings
	paused        bool
	dateCache     map[string]string // "root:YYYY-MM-DD" -> cid
	lastLifeRun   int64
	lastLifeSeen  int64
	lastCloudScan int64

	center *organizeCenter
}

type ServiceOptions struct {
	Timezone         string
	DataDir          string
	WorkDir          string
	ExtractBin       string
	ExtractMaxDictMB int
	StableWindow     time.Duration
}

func NewService(st *store.Store, tz string) *Service {
	return NewServiceWithOptions(st, ServiceOptions{Timezone: tz})
}

func NewServiceWithOptions(st *store.Store, opt ServiceOptions) *Service {
	loc, err := time.LoadLocation(opt.Timezone)
	if err != nil {
		loc = time.FixedZone("CST", 8*3600)
	}
	dataDir := opt.DataDir
	if dataDir == "" {
		dataDir = "data"
	}
	svc := &Service{
		store:            st,
		gov:              p115.NewGovernor(600),
		loc:              loc,
		extractor:        extract.NewEngine(),
		localWatcher:     localwatch.NewWatcher(st, opt.WorkDir, opt.StableWindow),
		notifier:         notify.NewDispatcher(st),
		dataDir:          dataDir,
		workDir:          opt.WorkDir,
		extractBin:       opt.ExtractBin,
		extractMaxDictMB: opt.ExtractMaxDictMB,
		dateCache:        make(map[string]string),
		center:           newOrganizeCenter(),
	}
	_ = svc.ReloadP115(context.Background())
	var paused bool
	if ok, _ := st.GetSetting(context.Background(), "scheduler.paused", &paused); ok {
		svc.paused = paused
	}
	return svc
}

func (s *Service) IsPaused() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.paused
}

func (s *Service) SetPaused(ctx context.Context, paused bool) error {
	if err := s.store.SetSetting(ctx, "scheduler.paused", paused, false); err != nil {
		return err
	}
	s.mu.Lock()
	s.paused = paused
	s.mu.Unlock()
	msg := "后台任务调度已恢复"
	if paused {
		msg = "后台任务调度已暂停（不会自动启动新任务）"
	}
	_ = s.store.RecordEvent(ctx, "system", "SCHEDULER_TOGGLED", "cloud_monitor", msg, false)
	return nil
}

// SetPort injects a custom p115.Port (primarily for tests).
func (s *Service) SetPort(port p115.Port, cfg P115Settings) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cloud = port
	s.portPinned = port != nil
	s.p115Cfg = cfg
	s.dateCache = make(map[string]string)
}

func (s *Service) GovernorStats() map[string]any {
	return s.gov.Stats()
}

func (s *Service) P115Config() P115Settings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.p115Cfg
}

func (s *Service) ReloadP115(ctx context.Context) error {
	var cfg P115Settings
	found, err := s.store.GetSetting(ctx, "p115.config", &cfg)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.p115Cfg = cfg
	s.dateCache = make(map[string]string)
	if s.portPinned {
		return nil
	}
	if found && cfg.Enabled && strings.TrimSpace(cfg.Cookie) != "" {
		client, err := p115.NewClient(cfg.Cookie, s.gov)
		if err != nil {
			s.cloud = nil
			return err
		}
		s.cloud = client
	} else {
		s.cloud = nil
	}
	return nil
}

func (s *Service) SaveP115Config(ctx context.Context, incoming P115Settings) (P115Settings, error) {
	var existing P115Settings
	_, _ = s.store.GetSetting(ctx, "p115.config", &existing)

	incoming.Cookie = strings.TrimSpace(incoming.Cookie)
	if incoming.Cookie == "" {
		incoming.Cookie = existing.Cookie
		if incoming.Cookie != "" {
			incoming.Enabled = true
		}
	} else {
		// Saving a cookie automatically enables connection.
		incoming.Enabled = true
	}
	if incoming.CloudRootCID == "" {
		incoming.CloudRootCID = existing.CloudRootCID
	}
	if incoming.OfflineRootCID == "" {
		incoming.OfflineRootCID = existing.OfflineRootCID
	}
	if incoming.CloudOutputCID == "" {
		incoming.CloudOutputCID = existing.CloudOutputCID
	}
	if incoming.CloudArchiveZoneCID == "" {
		incoming.CloudArchiveZoneCID = existing.CloudArchiveZoneCID
	}
	if incoming.CloudArchiveCID == "" {
		incoming.CloudArchiveCID = existing.CloudArchiveCID
	}
	if incoming.CloudFailedCID == "" {
		incoming.CloudFailedCID = existing.CloudFailedCID
	}
	if incoming.CloudHistoryCID == "" {
		incoming.CloudHistoryCID = existing.CloudHistoryCID
	}
	if incoming.CloudScanDepth <= 0 {
		if existing.CloudScanDepth >= 1 && existing.CloudScanDepth <= 5 {
			incoming.CloudScanDepth = existing.CloudScanDepth
		} else {
			incoming.CloudScanDepth = 1
		}
	} else if incoming.CloudScanDepth > 5 {
		incoming.CloudScanDepth = 5
	}
	def := true
	incoming.CloudExtractByDate = &def

	if err := s.store.SetSetting(ctx, "p115.config", incoming, true); err != nil {
		return P115Settings{}, err
	}
	if err := s.ReloadP115(ctx); err != nil {
		return P115Settings{}, err
	}
	s.gov.ResetCircuit()
	return incoming, nil
}

// InitCloudRoot creates the clean 3-folder cloud layout:
// <root>/离线下载, <root>/解压输出, <root>/归档区/{原包留存, 异常待查, 历史归档}
func (s *Service) InitCloudRoot(ctx context.Context, rootCID string) (P115Settings, error) {
	rootCID = strings.TrimSpace(rootCID)
	if rootCID == "" {
		return P115Settings{}, store.ErrInvalid
	}
	cloud, cfg := s.snapshotCloud()
	if cloud == nil {
		return P115Settings{}, ErrP115NotConfigured
	}

	// Verify root CID exists before creating children.
	if _, err := cloud.DirectoryPath(ctx, rootCID); err != nil {
		return P115Settings{}, err
	}

	cfg.CloudRootCID = rootCID
	topFolders := []struct {
		target *string
		name   string
	}{
		{&cfg.OfflineRootCID, "离线下载"},
		{&cfg.CloudOutputCID, "解压输出"},
		{&cfg.CloudArchiveZoneCID, "归档区"},
	}
	for _, f := range topFolders {
		cid, err := cloud.EnsureFolder(ctx, rootCID, f.name)
		if err != nil {
			return P115Settings{}, err
		}
		*f.target = cid
	}

	zoneFolders := []struct {
		target *string
		name   string
	}{
		{&cfg.CloudArchiveCID, "原包留存"},
		{&cfg.CloudFailedCID, "异常待查"},
		{&cfg.CloudHistoryCID, "历史归档"},
	}
	for _, f := range zoneFolders {
		cid, err := cloud.EnsureFolder(ctx, cfg.CloudArchiveZoneCID, f.name)
		if err != nil {
			return P115Settings{}, err
		}
		*f.target = cid
	}

	saved, err := s.SaveP115Config(ctx, cfg)
	if err != nil {
		return P115Settings{}, err
	}
	_ = s.store.RecordEvent(ctx, "system", "CLOUD_ROOT_INITIALIZED", "cloud_monitor",
		fmt.Sprintf("云根目录(%s)初始化完成：离线下载=%s 解压输出=%s 归档区=%s（原包留存=%s 异常待查=%s 历史归档=%s）",
			rootCID, saved.OfflineRootCID, saved.CloudOutputCID, saved.CloudArchiveZoneCID,
			saved.CloudArchiveCID, saved.CloudFailedCID, saved.CloudHistoryCID), false)
	return saved, nil
}

func (s *Service) ImportText(ctx context.Context, tokenID, idemKey, text string, autoExtract bool) (store.BatchResult, error) {
	if err := s.checkCloudRootReady(); err != nil {
		return store.BatchResult{}, err
	}
	parsed, errs, err := links.Extract(text)
	if err != nil {
		return store.BatchResult{}, err
	}
	reqHash := hashRequest("text", []byte(text), autoExtract)
	return s.store.RegisterOfflineBatch(ctx, store.RegisterBatchInput{
		TokenID:        tokenID,
		IdempotencyKey: idemKey,
		RequestHash:    reqHash,
		TargetDate:     time.Now().In(s.loc).Format("2006-01-02"),
		AutoExtract:    autoExtract,
		Links:          parsed,
		InvalidCount:   len(errs),
		PolicyJSON:     "{}",
	})
}

func (s *Service) ImportTorrent(ctx context.Context, tokenID, idemKey string, raw []byte, autoExtract bool) (store.BatchResult, error) {
	if err := s.checkCloudRootReady(); err != nil {
		return store.BatchResult{}, err
	}
	link, err := torrent.ParseToMagnet(raw)
	if err != nil {
		return store.BatchResult{}, err
	}

	// Snapshot active seed cleanup rule at upload time so later rule edits don't alter existing tasks.
	pol := PipelinePolicy{IsTorrent: true}
	seedRule, err := s.store.GetSeedRule(ctx)
	if err == nil && seedRule != nil {
		sp := cleanup.DefaultSeedPolicy()
		if jsonErr := json.Unmarshal([]byte(seedRule.ConfigJSON), &sp); jsonErr == nil {
			if valid, vErr := cleanup.ValidatePolicy(sp); vErr == nil {
				pol.SeedCleanup = &valid
			}
		}
	}
	polBytes, _ := json.Marshal(pol)
	reqHash := hashRequest("torrent", raw, autoExtract)

	return s.store.RegisterOfflineBatch(ctx, store.RegisterBatchInput{
		TokenID:        tokenID,
		IdempotencyKey: idemKey,
		RequestHash:    reqHash,
		TargetDate:     time.Now().In(s.loc).Format("2006-01-02"),
		AutoExtract:    autoExtract,
		Links:          []links.Link{link},
		PolicyJSON:     string(polBytes),
	})
}

func (s *Service) checkCloudRootReady() error {
	cloud, cfg := s.snapshotCloud()
	if cloud != nil && cfg.OfflineRootCID == "" {
		return ErrCloudRootRequired
	}
	return nil
}

func (s *Service) snapshotCloud() (p115.Port, P115Settings) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cloud, s.p115Cfg
}

// RunLoop ticks the background pipeline scheduler until ctx is cancelled.
func (s *Service) RunLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("pipeline tick error", "err", err)
			}
		}
	}
}

func (s *Service) Tick(ctx context.Context) error {
	// Always flush pending notifications (including local-only mode without 115 configured).
	defer func() {
		if s.notifier != nil {
			_ = s.notifier.Flush(ctx)
		}
	}()

	if s.IsPaused() {
		return nil
	}

	// 1. Local/NAS directory watch & local extractions run even without 115 configured.
	if s.localWatcher != nil {
		_, _ = s.localWatcher.Scan(ctx, time.Now().In(s.loc))
	}
	if err := s.stepLocalExtractions(ctx); err != nil {
		return err
	}

	// 2. Cloud offline, discovery, seed cleanup, cloud watched sources, cloud extract, and life-event checks.
	cloud, cfg := s.snapshotCloud()
	if cloud != nil {
		if cfg.OfflineRootCID != "" {
			if err := s.submitQueued(ctx, cloud, cfg.OfflineRootCID); err != nil {
				return err
			}
			if err := s.pollActive(ctx, cloud); err != nil {
				return err
			}
			if err := s.runSeedCleanups(ctx, cloud, cfg.OfflineRootCID); err != nil {
				return err
			}
			if err := s.discoverOfflineArchives(ctx, cloud, cfg); err != nil {
				return err
			}
		}
		if err := s.stepCloudArchives(ctx, cloud, cfg); err != nil {
			return err
		}
		if cfg.LifeMonitorEnabled && time.Now().Unix()-s.lastLifeRun >= 1800 {
			_, _ = s.TriggerLifeCheck(ctx)
		}
	}

	// 3. Optional periodic date-folder organization (runs at most once per month/day when enabled).
	s.maybeAutoOrganize(ctx)
	return nil
}

func (s *Service) resolveDateFolder(ctx context.Context, cloud p115.Port, rootCID, targetDate string) (string, error) {
	key := rootCID + ":" + targetDate
	s.mu.RLock()
	cached := s.dateCache[key]
	s.mu.RUnlock()

	if cached != "" {
		if p, err := cloud.DirectoryPath(ctx, cached); err == nil && len(p) >= 2 && p[len(p)-2] == rootCID && p[len(p)-1] == cached {
			return cached, nil
		}
	}
	cid, err := cloud.EnsureFolder(ctx, rootCID, targetDate)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	s.dateCache[key] = cid
	s.mu.Unlock()
	return cid, nil
}

func (s *Service) submitQueued(ctx context.Context, cloud p115.Port, offlineRootCID string) error {
	now := time.Now().Unix()
	due, err := s.store.ListDuePipelines(ctx, "offline", []string{"queued"}, now, 10)
	if err != nil || len(due) == 0 {
		return err
	}

	// Group by target_date so each batch targets a single daily directory.
	targetDate := due[0].TargetDate
	if targetDate == "" {
		targetDate = time.Now().In(s.loc).Format("2006-01-02")
	}
	batch := make([]store.Pipeline, 0, len(due))
	for _, p := range due {
		if p.TargetDate == targetDate || p.TargetDate == "" {
			batch = append(batch, p)
		}
	}

	targetCID, err := s.resolveDateFolder(ctx, cloud, offlineRootCID, targetDate)
	if err != nil {
		return err
	}

	urlList := make([]string, 0, len(batch))
	prepared := make([]store.Pipeline, 0, len(batch))
	for _, p := range batch {
		opKey := fmt.Sprintf("offline-submit:%s:%d", p.ID, p.Attempts+1)
		existing, err := s.store.JournalGet(ctx, opKey)
		if err != nil {
			return err
		}
		if existing != nil && (existing.Status == "prepared" || existing.Status == "uncertain") {
			p.State = "submission_unknown"
			p.Stage = "offline_poll"
			p.NextRunAt = now + 15
			_ = s.store.UpdatePipeline(ctx, p)
			continue
		}
		if err := s.store.JournalPrepare(ctx, opKey, p.ID, "offline_submit", map[string]string{"target_cid": targetCID}); err != nil {
			return err
		}
		p.Attempts++
		p.TargetCID = targetCID
		p.State = "submitting"
		_ = s.store.UpdatePipeline(ctx, p)
		prepared = append(prepared, p)
		urlList = append(urlList, p.OriginalLink)
	}
	if len(prepared) == 0 {
		return nil
	}

	submitErr := cloud.SubmitOffline(ctx, urlList, targetCID)
	for _, p := range prepared {
		opKey := fmt.Sprintf("offline-submit:%s:%d", p.ID, p.Attempts)
		if submitErr != nil {
			var aErr *p115.AdapterError
			uncertain := errors.As(submitErr, &aErr) && aErr.Uncertain
			if uncertain {
				_ = s.store.JournalSetStatus(ctx, opKey, "uncertain")
				p.State = "submission_unknown"
				p.Stage = "offline_poll"
				p.ErrorCode = submitErr.Error()
				p.NextRunAt = now + 15
				_ = s.store.UpdatePipeline(ctx, p)
				_ = s.store.RecordEvent(ctx, p.ID, "SUBMISSION_UNKNOWN", "offline_submit", "提交结果不确定，等待核对："+p.ErrorCode, true)
			} else {
				_ = s.store.JournalSetStatus(ctx, opKey, "failed")
				p.State = "submit_failed"
				p.Stage = "offline_submit"
				p.ErrorCode = submitErr.Error()
				p.NextRunAt = 0
				_ = s.store.UpdatePipeline(ctx, p)
				_ = s.store.RecordEvent(ctx, p.ID, "SUBMIT_FAILED", "offline_submit", "离线提交失败："+p.ErrorCode, true)
			}
			continue
		}
		_ = s.store.JournalSetStatus(ctx, opKey, "completed")
		p.State = "submitted"
		p.Stage = "offline_poll"
		p.ErrorCode = ""
		p.NextRunAt = now + 15
		_ = s.store.UpdatePipeline(ctx, p)
		_ = s.store.RecordEvent(ctx, p.ID, "SUBMITTED", "offline_poll", "已提交至 115，等待离线下载完成", true)
	}
	return nil
}

func (s *Service) pollActive(ctx context.Context, cloud p115.Port) error {
	now := time.Now().Unix()
	active, err := s.store.ListDuePipelines(ctx, "offline", []string{"submitted", "downloading", "submission_unknown"}, now, 50)
	if err != nil || len(active) == 0 {
		return err
	}

	snapshot, err := cloud.OfflineSnapshot(ctx)
	if err != nil {
		return err
	}

	byHash := make(map[string]p115.OfflineTask, len(snapshot))
	byURL := make(map[string]p115.OfflineTask, len(snapshot))
	for _, t := range snapshot {
		if t.InfoHash != "" {
			byHash[strings.ToLower(t.InfoHash)] = t
		}
		if t.URL != "" {
			byURL[t.URL] = t
		}
	}

	for _, p := range active {
		match, found := findOfflineMatch(p, byHash, byURL)
		if !found {
			if p.State == "submission_unknown" {
				// Not present in 115 offline list -> safe to mark submit_failed for retry.
				p.State = "submit_failed"
				p.Stage = "offline_submit"
				p.ErrorCode = "SUBMISSION_NOT_FOUND"
				p.NextRunAt = 0
				_ = s.store.UpdatePipeline(ctx, p)
				_ = s.store.RecordEvent(ctx, p.ID, "SUBMIT_FAILED", "offline_submit", "核对未在 115 离线列表发现，可安全重试", true)
				continue
			}
			// Backoff poll for up to 24h.
			p.NextRunAt = now + nextPollDelay(now-p.CreatedAt)
			_ = s.store.UpdatePipeline(ctx, p)
			continue
		}

		if match.Name != "" {
			p.Name = match.Name
		}
		switch match.Status {
		case "success":
			var pol PipelinePolicy
			_ = json.Unmarshal([]byte(p.PolicyJSON), &pol)
			if pol.IsTorrent && pol.SeedCleanup != nil && pol.SeedCleanup.Enabled {
				p.State = "seed_cleanup"
				p.Stage = "seed_cleanup"
				p.NextRunAt = now
			} else if p.AutoExtract {
				p.State = "discover"
				p.Stage = "discover"
				p.NextRunAt = now
			} else {
				p.State = "completed"
				p.Stage = "complete"
				p.CompletedAt = &now
				p.NextRunAt = 0
			}
			p.ErrorCode = ""
			_ = s.store.UpdatePipeline(ctx, p)
			_ = s.store.RecordEvent(ctx, p.ID, "OFFLINE_SUCCEEDED", p.Stage, "115 离线下载已完成", p.State == "completed")

		case "failed":
			p.State = "failed"
			p.Stage = "offline_poll"
			p.ErrorCode = "REMOTE_OFFLINE_FAILED"
			p.NextRunAt = 0
			_ = s.store.UpdatePipeline(ctx, p)
			_ = s.store.RecordEvent(ctx, p.ID, "FAILED", "offline_poll", "115 离线下载失败", true)

		default:
			p.State = "downloading"
			p.Stage = "offline_poll"
			p.NextRunAt = now + nextPollDelay(now-p.CreatedAt)
			_ = s.store.UpdatePipeline(ctx, p)
		}
	}
	return nil
}

func (s *Service) runSeedCleanups(ctx context.Context, cloud p115.Port, offlineRootCID string) error {
	now := time.Now().Unix()
	due, err := s.store.ListDuePipelines(ctx, "offline", []string{"seed_cleanup"}, now, 5)
	if err != nil || len(due) == 0 {
		return err
	}
	eng := cleanup.NewEngine(s.store, cloud)
	for _, p := range due {
		var pol PipelinePolicy
		_ = json.Unmarshal([]byte(p.PolicyJSON), &pol)
		if pol.SeedCleanup == nil {
			p.State = "completed"
			p.Stage = "complete"
			p.CompletedAt = &now
			_ = s.store.UpdatePipeline(ctx, p)
			continue
		}
		report, runErr := eng.Run(ctx, p, *pol.SeedCleanup, offlineRootCID)
		p.OutputJSON = cleanup.EncodeReport(report)
		if runErr != nil {
			p.State = "seed_cleanup_failed"
			p.Stage = "seed_cleanup"
			p.ErrorCode = "SEED_CLEANUP_FAILED"
			p.ErrorMessage = report.Error
			p.NextRunAt = 0
			_ = s.store.UpdatePipeline(ctx, p)
			_ = s.store.RecordEvent(ctx, p.ID, "SEED_CLEANUP_FAILED", "seed_cleanup", "种子下载成功，清理失败："+report.Error, true)
			continue
		}
		if p.AutoExtract || (pol.SeedCleanup != nil && pol.SeedCleanup.AutoExtract) {
			p.State = "discover"
			p.Stage = "discover"
			p.ErrorCode = ""
			p.ErrorMessage = ""
			p.NextRunAt = now
		} else {
			p.State = "completed"
			p.Stage = "complete"
			p.ErrorCode = ""
			p.ErrorMessage = ""
			p.CompletedAt = &now
			p.NextRunAt = 0
		}
		_ = s.store.UpdatePipeline(ctx, p)
	}
	return nil
}

type BarkTarget struct {
	SourceID     string
	ReportedPath string
	RelativePath string
}

// HandleBarkTrigger resolves Bark-reported relative subpaths under matched 115 cloud sources and scans them for archives.
// When no specific target path matched, falls back to scanning all enabled 115 cloud sources.
func (s *Service) HandleBarkTrigger(ctx context.Context, targets []BarkTarget) (int, error) {
	cloud, cfg := s.snapshotCloud()
	if cloud == nil {
		return 0, ErrP115NotConfigured
	}
	adopted := 0
	if len(targets) > 0 {
		sources, err := s.store.ListSources(ctx, "cloud")
		if err != nil {
			return 0, err
		}
		byID := make(map[string]store.Source, len(sources))
		for _, src := range sources {
			if src.Enabled {
				byID[src.ID] = src
			}
		}
		rule := s.ArchiveRule(ctx)
		today := time.Now().In(s.loc).Format("2006-01-02")
		for _, t := range targets {
			src, ok := byID[t.SourceID]
			if !ok {
				continue
			}
			n, err := s.scanBarkTarget(ctx, cloud, cfg, rule, src, t, today)
			if err != nil {
				_ = s.store.RecordEvent(ctx, "system", "BARK_PATH_FAILED", "bark",
					fmt.Sprintf("来源「%s」路径 %s 解析失败：%s", src.Name, displayRelPath(t.RelativePath), err.Error()), true)
				continue
			}
			adopted += n
			if n > 0 {
				_ = s.store.RecordEvent(ctx, "system", "BARK_TARGET_CHECKED", "bark",
					fmt.Sprintf("来源「%s」路径 %s 扫描完成，新接管 %d 个压缩包进入解压队列", src.Name, displayRelPath(t.RelativePath), n), true)
			} else {
				_ = s.store.RecordEvent(ctx, "system", "BARK_TARGET_CHECKED", "bark",
					fmt.Sprintf("来源「%s」路径 %s 扫描完成，未发现新的主压缩包", src.Name, displayRelPath(t.RelativePath)), true)
			}
		}
	} else {
		// Anti-risk-control: do NOT fall back to full recursive cloud sweep when Bark didn't match any source prefix.
		_ = s.store.RecordEvent(ctx, "system", "BARK_TARGET_CHECKED", "bark",
			"收到 Bark 信号但未命中任何云来源路径前缀，已跳过全盘扫描（防风控）", false)
	}

	if cfg.LifeMonitorEnabled {
		if n, lifeErr := s.TriggerLifeCheck(ctx); lifeErr == nil {
			adopted += n
		}
	}
	return adopted, nil
}

func displayRelPath(rel string) string {
	if strings.TrimSpace(rel) == "" {
		return "/"
	}
	return "/" + strings.Trim(rel, "/")
}

// TriggerLifeCheck runs an incremental 115 life-events scan and adopts newly observed archives in monitored folders.
func (s *Service) TriggerLifeCheck(ctx context.Context) (int, error) {
	cloud, cfg := s.snapshotCloud()
	if cloud == nil {
		return 0, ErrP115NotConfigured
	}
	now := time.Now().Unix()
	since := s.lastLifeSeen
	if since == 0 {
		since = now - 1800
	}
	events, err := cloud.LifeEvents(ctx, since)
	if err != nil {
		return 0, err
	}
	s.lastLifeRun = now

	watchedCIDs := make(map[string]string) // cid -> sourceID
	if sources, sErr := s.store.ListSources(ctx, "cloud"); sErr == nil {
		for _, src := range sources {
			if !src.Enabled {
				continue
			}
			var sc struct {
				CID string `json:"cid"`
			}
			if json.Unmarshal([]byte(src.ConfigJSON), &sc) == nil && strings.TrimSpace(sc.CID) != "" {
				watchedCIDs[strings.TrimSpace(sc.CID)] = src.ID
			}
		}
	}
	rule := s.ArchiveRule(ctx)
	today := time.Now().In(s.loc).Format("2006-01-02")

	maxTime := since
	archivesFound := 0
	adopted := 0
	for _, ev := range events {
		if ev.Time > maxTime {
			maxTime = ev.Time
		}
		if extract.IsPrimaryArchiveName(ev.Name) {
			archivesFound++
			if ev.FID != "" && ev.Pickcode != "" {
				if srcID, ok := watchedCIDs[ev.CID]; ok || (cfg.OfflineRootCID != "" && ev.CID == cfg.OfflineRootCID) {
					cf := p115.CloudFile{
						FID: ev.FID, CID: ev.CID, Name: ev.Name,
						Size: ev.Size, Pickcode: ev.Pickcode, SHA1: ev.SHA1,
					}
					if okCreated := s.adoptCloudArchive(ctx, "", srcID, cf, today, cfg, rule); okCreated {
						adopted++
					}
				}
			}
		}
	}
	s.lastLifeSeen = maxTime
	_ = s.store.RecordEvent(ctx, "system", "LIFE_MONITOR_CHECK", "cloud_monitor",
		fmt.Sprintf("生活事件增量检查完成：读取 %d 条，发现压缩包 %d 个，新入队 %d 个", len(events), archivesFound, adopted), false)
	return archivesFound, nil
}

func findOfflineMatch(p store.Pipeline, byHash, byURL map[string]p115.OfflineTask) (p115.OfflineTask, bool) {
	if p.InfoHash != "" {
		if t, ok := byHash[strings.ToLower(p.InfoHash)]; ok {
			return t, true
		}
	}
	if p.OriginalLink != "" {
		if t, ok := byURL[p.OriginalLink]; ok {
			return t, true
		}
	}
	return p115.OfflineTask{}, false
}

func nextPollDelay(ageSec int64) int64 {
	switch {
	case ageSec < 120:
		return 15
	case ageSec < 600:
		return 60
	case ageSec < 3600:
		return 180
	default:
		return 300
	}
}

func isArchiveName(name string) bool {
	ext := strings.ToLower(path.Ext(name))
	switch ext {
	case ".zip", ".rar", ".7z", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".iso":
		return true
	}
	return false
}

func hashRequest(kind string, payload []byte, autoExtract bool) string {
	h := sha256.New()
	h.Write([]byte(kind))
	h.Write([]byte{0})
	if autoExtract {
		h.Write([]byte("1"))
	} else {
		h.Write([]byte("0"))
	}
	h.Write([]byte{0})
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}
