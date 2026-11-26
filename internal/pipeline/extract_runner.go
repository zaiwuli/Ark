package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/zaiwuli/Ark/internal/config"
	"github.com/zaiwuli/Ark/internal/extract"
	"github.com/zaiwuli/Ark/internal/localwatch"
	"github.com/zaiwuli/Ark/internal/p115"
	"github.com/zaiwuli/Ark/internal/store"
)

type CloudLocator struct {
	FID          string `json:"fid"`
	CID          string `json:"cid"`
	Pickcode     string `json:"pickcode"`
	Name         string `json:"name"`
	Size         int64  `json:"size"`
	SHA1         string `json:"sha1"`
	CachePath    string `json:"cache_path,omitempty"`
	CloudRetries int    `json:"cloud_retries,omitempty"`
}

// ArchiveRuleConfig governs post-extraction original handling and cloud failure policy.
// Date layering (YYYY-MM-DD) and single-root squashing are always enabled by default.
type ArchiveRuleConfig struct {
	OriginalAction     string `json:"original_action"`      // archive (default) | keep | delete
	CloudFailureAction string `json:"cloud_failure_action"` // confirm_local (default) | auto_local | move_failed
	// Legacy/per-pipeline fields retained for backward compatibility:
	AutoFallback     bool   `json:"auto_fallback,omitempty"`
	SquashRoot       bool   `json:"squash_root,omitempty"`
	DisableRecursion bool   `json:"disable_recursion,omitempty"`
	OutputDateDir    bool   `json:"output_date_dir,omitempty"`
	OutputCID        string `json:"output_cid,omitempty"`
	OutputDir        string `json:"output_dir,omitempty"`
	DeleteOriginal   bool   `json:"delete_original,omitempty"`
}

func DefaultArchiveRule() ArchiveRuleConfig {
	return ArchiveRuleConfig{
		OriginalAction:     "archive",
		CloudFailureAction: "confirm_local",
		SquashRoot:         true,
		DisableRecursion:   true,
		OutputDateDir:      true,
	}
}

func (r ArchiveRuleConfig) Normalize() ArchiveRuleConfig {
	out := r
	switch out.OriginalAction {
	case "archive", "keep", "delete":
	default:
		if out.DeleteOriginal {
			out.OriginalAction = "delete"
		} else {
			out.OriginalAction = "archive"
		}
	}
	switch out.CloudFailureAction {
	case "confirm_local", "auto_local", "move_failed":
	default:
		if out.AutoFallback {
			out.CloudFailureAction = "auto_local"
		} else {
			out.CloudFailureAction = "confirm_local"
		}
	}
	out.SquashRoot = true
	out.DisableRecursion = true
	out.OutputDateDir = true
	return out
}

func (s *Service) ArchiveRule(ctx context.Context) ArchiveRuleConfig {
	rule := DefaultArchiveRule()
	_, _ = s.store.GetSetting(ctx, "extract.rule", &rule)
	return rule.Normalize()
}

func (s *Service) SaveArchiveRule(ctx context.Context, incoming ArchiveRuleConfig) (ArchiveRuleConfig, error) {
	norm := incoming.Normalize()
	if err := s.store.SetSetting(ctx, "extract.rule", norm, false); err != nil {
		return ArchiveRuleConfig{}, err
	}
	return norm, nil
}

func (s *Service) ArchivePasswords(ctx context.Context) []string {
	var list []string
	_, _ = s.store.GetSetting(ctx, "extract.passwords", &list)
	return list
}

func (s *Service) SaveArchivePasswords(ctx context.Context, list []string) error {
	cleaned := make([]string, 0, len(list))
	seen := make(map[string]bool)
	for _, p := range list {
		p = strings.TrimSpace(p)
		if p != "" && !seen[p] {
			seen[p] = true
			cleaned = append(cleaned, p)
		}
	}
	return s.store.SetSetting(ctx, "extract.passwords", cleaned, true)
}

// ConfirmLocalFallback lets a user approve local download + 7-Zip fallback for a failed cloud extract.
func (s *Service) ConfirmLocalFallback(ctx context.Context, pipelineID string) (store.Pipeline, error) {
	p, err := s.store.GetPipeline(ctx, pipelineID)
	if err != nil {
		return store.Pipeline{}, err
	}
	if p.State != "wait_local_confirm" {
		return store.Pipeline{}, store.ErrPipelineBusy
	}
	now := time.Now().Unix()
	p.State = "caching"
	p.Stage = "cache"
	p.WaitReason = ""
	p.ErrorCode = ""
	p.ErrorMessage = ""
	p.NextRunAt = now
	if err := s.store.UpdatePipeline(ctx, p); err != nil {
		return store.Pipeline{}, err
	}
	_ = s.store.RecordEvent(ctx, p.ID, "LOCAL_FALLBACK_APPROVED", "cache", "已确认本地兜底，准备通过 115 直链缓存原包", true)
	return p, nil
}

func listAllCloudFiles(ctx context.Context, cloud p115.Port, cid string) ([]p115.CloudFile, error) {
	var all []p115.CloudFile
	offset := 0
	for range 20 {
		page, err := cloud.Files(ctx, cid, offset)
		if err != nil {
			return nil, err
		}
		all = append(all, page.Files...)
		if page.NextOffset == nil {
			break
		}
		offset = *page.NextOffset
	}
	return all, nil
}

func (s *Service) adoptCloudArchive(ctx context.Context, parentID, sourceID string, f p115.CloudFile, targetDate string, cfg P115Settings, rule ArchiveRuleConfig) bool {
	if targetDate == "" {
		targetDate = time.Now().In(s.loc).Format("2006-01-02")
	}
	locBytes, _ := json.Marshal(CloudLocator{
		FID: f.FID, CID: f.CID, Pickcode: f.Pickcode, Name: f.Name, Size: f.Size, SHA1: f.SHA1,
	})
	pol := rule.Normalize()
	if pol.OutputCID == "" {
		pol.OutputCID = cfg.CloudOutputCID
	}
	polBytes, _ := json.Marshal(pol)
	ident := fmt.Sprintf("cloud:%s:%d", f.FID, f.Size)
	_, created, err := s.store.CreateCloudArchivePipeline(ctx, parentID, sourceID, ident, f.Name, targetDate, f.Size, string(locBytes), string(polBytes))
	return err == nil && created
}

func isManagedCloudSkipDir(f p115.CloudFile, cfg P115Settings) bool {
	if !f.IsDir {
		return false
	}
	if f.FID != "" && (f.FID == cfg.CloudOutputCID || f.FID == cfg.CloudArchiveZoneCID ||
		f.FID == cfg.CloudArchiveCID || f.FID == cfg.CloudFailedCID || f.FID == cfg.CloudHistoryCID) {
		return true
	}
	switch f.Name {
	case "归档区", "解压输出", "原包留存", "异常待查", "历史归档":
		return true
	}
	return false
}

// scanCloudDirRecursive scans startCID and up to maxDepth levels of child directories (bounded to 30 dirs)
// adopting all primary archives into cloud_archive pipelines.
func (s *Service) scanCloudDirRecursive(ctx context.Context, cloud p115.Port, cfg P115Settings, rule ArchiveRuleConfig, sourceID, startCID, today string, maxDepth int) (int, error) {
	type dirItem struct {
		cid   string
		depth int
	}
	queue := []dirItem{{cid: startCID, depth: 0}}
	visited := make(map[string]bool)
	const maxDirsPerScan = 30
	adopted := 0

	for len(queue) > 0 && len(visited) < maxDirsPerScan {
		cur := queue[0]
		queue = queue[1:]
		if cur.cid == "" || visited[cur.cid] {
			continue
		}
		visited[cur.cid] = true

		files, err := listAllCloudFiles(ctx, cloud, cur.cid)
		if err != nil {
			if cur.depth == 0 {
				return adopted, err
			}
			continue
		}
		for _, f := range files {
			if f.IsDir {
				if cur.depth < maxDepth && !isManagedCloudSkipDir(f, cfg) && !visited[f.FID] {
					queue = append(queue, dirItem{cid: f.FID, depth: cur.depth + 1})
				}
				continue
			}
			if !extract.IsPrimaryArchiveName(f.Name) {
				continue
			}
			if s.adoptCloudArchive(ctx, "", sourceID, f, today, cfg, rule) {
				adopted++
			}
		}
	}
	return adopted, nil
}

// scanBarkTarget walks the relative path from a matched cloud source's CID down to the exact folder (or archive file)
// reported in the Bark message, then scans it (plus up to 2 subfolder levels) and creates cloud_archive pipelines.
func (s *Service) scanBarkTarget(ctx context.Context, cloud p115.Port, cfg P115Settings, rule ArchiveRuleConfig, src store.Source, target BarkTarget, today string) (int, error) {
	var sc struct {
		CID string `json:"cid"`
	}
	if err := json.Unmarshal([]byte(src.ConfigJSON), &sc); err != nil || strings.TrimSpace(sc.CID) == "" {
		return 0, errors.New("云监控来源未配置有效 CID")
	}
	cid := strings.TrimSpace(sc.CID)
	rel := strings.Trim(target.RelativePath, "/")
	if rel != "" {
		parts := strings.Split(rel, "/")
		for idx, comp := range parts {
			comp = strings.TrimSpace(comp)
			if comp == "" {
				continue
			}
			files, err := listAllCloudFiles(ctx, cloud, cid)
			if err != nil {
				return 0, err
			}
			var exactDirs, foldDirs []p115.CloudFile
			for _, f := range files {
				if !f.IsDir {
					continue
				}
				if f.Name == comp {
					exactDirs = append(exactDirs, f)
				} else if strings.EqualFold(f.Name, comp) {
					foldDirs = append(foldDirs, f)
				}
			}
			chosen := exactDirs
			if len(chosen) == 0 {
				chosen = foldDirs
			}
			if len(chosen) == 1 {
				cid = chosen[0].FID
				continue
			}
			if len(chosen) > 1 {
				return 0, fmt.Errorf("存在多个同名子目录：%s", comp)
			}
			// If the final path component is an archive file itself, adopt it directly!
			if idx == len(parts)-1 {
				for _, f := range files {
					if !f.IsDir && (f.Name == comp || strings.EqualFold(f.Name, comp)) {
						if extract.IsPrimaryArchiveName(f.Name) {
							if s.adoptCloudArchive(ctx, "", src.ID, f, today, cfg, rule) {
								return 1, nil
							}
							return 0, nil
						}
						return 0, nil
					}
				}
			}
			return 0, fmt.Errorf("未找到子目录或文件：%s", comp)
		}
	}
	return s.scanCloudDirRecursive(ctx, cloud, cfg, rule, src.ID, cid, today, 2)
}

// ScanCloudSources scans enabled 115 cloud watch sources recursively (up to 3 levels)
// and enqueues newly discovered primary archive files into cloud_archive pipelines.
func (s *Service) ScanCloudSources(ctx context.Context, onlySourceIDs []string) (int, error) {
	cloud, cfg := s.snapshotCloud()
	if cloud == nil {
		return 0, ErrP115NotConfigured
	}
	sources, err := s.store.ListSources(ctx, "cloud")
	if err != nil {
		return 0, err
	}
	filterSet := make(map[string]bool, len(onlySourceIDs))
	for _, id := range onlySourceIDs {
		if id != "" {
			filterSet[id] = true
		}
	}

	s.lastCloudScan = time.Now().Unix()
	rule := s.ArchiveRule(ctx)
	today := time.Now().In(s.loc).Format("2006-01-02")
	adopted := 0
	scanned := 0
	failedSources := 0

	for _, src := range sources {
		if !src.Enabled {
			continue
		}
		if len(filterSet) > 0 && !filterSet[src.ID] {
			continue
		}
		var sc struct {
			CID string `json:"cid"`
		}
		if err := json.Unmarshal([]byte(src.ConfigJSON), &sc); err != nil || strings.TrimSpace(sc.CID) == "" {
			continue
		}
		srcCID := strings.TrimSpace(sc.CID)
		n, err := s.scanCloudDirRecursive(ctx, cloud, cfg, rule, src.ID, srcCID, today, 3)
		if err != nil {
			failedSources++
			_ = s.store.RecordEvent(ctx, "system", "CLOUD_SOURCE_SCANNED", "cloud_monitor",
				fmt.Sprintf("扫描云监控目录「%s」(CID=%s)失败：%s", src.Name, srcCID, err.Error()), true)
			continue
		}
		scanned++
		adopted += n
	}

	if adopted > 0 || (len(onlySourceIDs) == 0 && scanned > 0 && failedSources == 0) {
		if adopted > 0 {
			_ = s.store.RecordEvent(ctx, "system", "CLOUD_SOURCE_SCANNED", "cloud_monitor",
				fmt.Sprintf("已扫描 %d 个 115 云监控目录，新接管 %d 个压缩包进入云解压", scanned, adopted), true)
		}
	}
	return adopted, nil
}

// discoverOfflineArchives scans TargetCID for completed offline tasks and adopts archive files into cloud_archive pipelines.
func (s *Service) discoverOfflineArchives(ctx context.Context, cloud p115.Port, cfg P115Settings) error {
	now := time.Now().Unix()
	due, err := s.store.ListDuePipelines(ctx, "offline", []string{"discover"}, now, 10)
	if err != nil || len(due) == 0 {
		return err
	}
	rule := s.ArchiveRule(ctx)

	for _, p := range due {
		if p.TargetCID == "" {
			continue
		}
		files, err := listAllCloudFiles(ctx, cloud, p.TargetCID)
		if err != nil {
			p.NextRunAt = now + 60
			_ = s.store.UpdatePipeline(ctx, p)
			continue
		}

		var candidates []p115.CloudFile
		for _, f := range files {
			if !f.IsDir && extract.IsPrimaryArchiveName(f.Name) {
				candidates = append(candidates, f)
			} else if f.IsDir && strings.EqualFold(f.Name, p.Name) {
				if sub, subErr := listAllCloudFiles(ctx, cloud, f.FID); subErr == nil {
					for _, sf := range sub {
						if !sf.IsDir && extract.IsPrimaryArchiveName(sf.Name) {
							candidates = append(candidates, sf)
						}
					}
				}
			}
		}

		adopted := 0
		for _, f := range candidates {
			if s.adoptCloudArchive(ctx, p.ID, "", f, p.TargetDate, cfg, rule) {
				adopted++
			}
		}

		if adopted > 0 {
			p.State = "completed"
			p.Stage = "complete"
			p.CompletedAt = &now
			p.NextRunAt = 0
			_ = s.store.UpdatePipeline(ctx, p)
			_ = s.store.RecordEvent(ctx, p.ID, "ARCHIVE_HANDOFF", "complete", fmt.Sprintf("已发现 %d 个压缩包并转交云解压", adopted), true)
		} else {
			p.Attempts++
			if p.Attempts >= 2 {
				p.State = "completed"
				p.Stage = "complete"
				p.CompletedAt = &now
				p.NextRunAt = 0
				_ = s.store.UpdatePipeline(ctx, p)
				_ = s.store.RecordEvent(ctx, p.ID, "NO_ARCHIVE_FOUND", "complete", "离线完成，未发现需要解压的压缩包", true)
			} else {
				p.NextRunAt = now + 180
				_ = s.store.UpdatePipeline(ctx, p)
			}
		}
	}
	return nil
}

// stepCloudArchives advances cloud_archive pipelines strictly serially for 115 cloud extraction
// (115 server rejects concurrent push_extract / add_extract_file tasks), while allowing local caching independently.
func (s *Service) stepCloudArchives(ctx context.Context, cloud p115.Port, cfg P115Settings) error {
	now := time.Now().Unix()
	var due []store.Pipeline

	// 1. Always allow 1 due local-fallback caching download independently of 115 cloud-extract slot.
	if cacheDue, err := s.store.ListDuePipelines(ctx, "cloud_archive", []string{"caching"}, now, 1); err == nil && len(cacheDue) > 0 {
		due = append(due, cacheDue...)
	}

	// 2. Strictly serialize 115 cloud extract (1 in-flight task at a time: cloud_progress -> cloud_export -> cloud_inspect -> cloud_prepare).
	inFlight, err := s.store.ListDuePipelines(ctx, "cloud_archive", []string{"cloud_progress", "cloud_export", "cloud_inspect"}, now, 1)
	if err != nil {
		return err
	}
	if len(inFlight) > 0 {
		due = append(due, inFlight...)
	} else {
		// Only start a new cloud_prepare if zero other tasks are currently in-flight on 115 cloud extract.
		activeCount, cErr := s.store.CountPipelinesInStates(ctx, "cloud_archive", []string{"cloud_progress", "cloud_export", "cloud_inspect"})
		if cErr == nil && activeCount == 0 {
			if prepDue, pErr := s.store.ListDuePipelines(ctx, "cloud_archive", []string{"cloud_prepare"}, now, 1); pErr == nil && len(prepDue) > 0 {
				due = append(due, prepDue...)
			}
		}
	}
	if len(due) == 0 {
		return nil
	}
	passwords := s.ArchivePasswords(ctx)
	globalRule := s.ArchiveRule(ctx)

	for _, p := range due {
		var loc CloudLocator
		_ = json.Unmarshal([]byte(p.LocatorJSON), &loc)
		rule := globalRule
		if strings.TrimSpace(p.PolicyJSON) != "" && p.PolicyJSON != "{}" {
			var perPipe ArchiveRuleConfig
			if json.Unmarshal([]byte(p.PolicyJSON), &perPipe) == nil {
				if perPipe.OriginalAction != "" {
					rule.OriginalAction = perPipe.OriginalAction
				}
				if perPipe.CloudFailureAction != "" {
					rule.CloudFailureAction = perPipe.CloudFailureAction
				} else if perPipe.AutoFallback {
					rule.CloudFailureAction = "auto_local"
				}
				if perPipe.OutputCID != "" {
					rule.OutputCID = perPipe.OutputCID
				}
				if perPipe.OutputDir != "" {
					rule.OutputDir = perPipe.OutputDir
				}
			}
		}
		rule = rule.Normalize()

		switch p.State {
		case "cloud_prepare":
			pwd := ""
			if p.Attempts > 0 && p.Attempts <= len(passwords) {
				pwd = passwords[p.Attempts-1]
			}
			st, err := cloud.PrepareExtract(ctx, loc.Pickcode, pwd)
			if err != nil {
				s.handleCloudFailure(ctx, cloud, cfg, p, loc, rule, "CLOUD_PREPARE_FAILED", err.Error(), true)
				continue
			}
			switch st {
			case "success":
				p.State = "cloud_export"
				p.Stage = "cloud_export"
				p.NextRunAt = now
			case "password":
				if p.Attempts < len(passwords) {
					p.Attempts++
					p.NextRunAt = now + 2
				} else {
					s.handleCloudFailure(ctx, cloud, cfg, p, loc, rule, "CLOUD_PASSWORD_FAILED", "云解压密码不匹配", false)
					continue
				}
			case "failed":
				s.handleCloudFailure(ctx, cloud, cfg, p, loc, rule, "CLOUD_PREPARE_FAILED", "115 云解压预检失败", true)
				continue
			default:
				p.State = "cloud_inspect"
				p.Stage = "cloud_inspect"
				p.NextRunAt = now + 5
			}
			_ = s.store.UpdatePipeline(ctx, p)

		case "cloud_inspect":
			st, err := cloud.InspectExtract(ctx, loc.Pickcode)
			if err != nil {
				p.NextRunAt = now + 10
				_ = s.store.UpdatePipeline(ctx, p)
				continue
			}
			switch st {
			case "success":
				p.State = "cloud_export"
				p.Stage = "cloud_export"
				p.NextRunAt = now
			case "password":
				if p.Attempts < len(passwords) {
					p.Attempts++
					p.State = "cloud_prepare"
					p.Stage = "cloud_prepare"
					p.NextRunAt = now + 2
				} else {
					s.handleCloudFailure(ctx, cloud, cfg, p, loc, rule, "CLOUD_PASSWORD_FAILED", "云解压需要有效密码", false)
					continue
				}
			case "failed":
				s.handleCloudFailure(ctx, cloud, cfg, p, loc, rule, "CLOUD_INSPECT_FAILED", "115 云解压分析失败", true)
				continue
			default:
				p.NextRunAt = now + 8
			}
			_ = s.store.UpdatePipeline(ctx, p)

		case "cloud_export":
			outRoot := rule.OutputCID
			if outRoot == "" {
				outRoot = cfg.CloudOutputCID
			}
			if outRoot == "" {
				outRoot = loc.CID
			}
			targetDate := p.TargetDate
			if targetDate == "" {
				targetDate = time.Now().In(s.loc).Format("2006-01-02")
				p.TargetDate = targetDate
			}
			dateCID, err := s.resolveDateFolder(ctx, cloud, outRoot, targetDate)
			if err != nil {
				s.handleCloudFailure(ctx, cloud, cfg, p, loc, rule, "CLOUD_OUTPUT_DIR_FAILED", err.Error(), true)
				continue
			}
			folderName := strings.TrimSuffix(loc.Name, filepath.Ext(loc.Name))
			if folderName == "" {
				folderName = p.ID
			}
			targetCID, err := cloud.EnsureFolder(ctx, dateCID, folderName)
			if err != nil {
				s.handleCloudFailure(ctx, cloud, cfg, p, loc, rule, "CLOUD_OUTPUT_DIR_FAILED", err.Error(), true)
				continue
			}
			remoteID, err := cloud.StartExtract(ctx, loc.Pickcode, targetCID)
			if err != nil {
				s.handleCloudFailure(ctx, cloud, cfg, p, loc, rule, "CLOUD_EXPORT_FAILED", err.Error(), true)
				continue
			}
			p.RemoteID = remoteID
			p.TargetCID = targetCID
			p.State = "cloud_progress"
			p.Stage = "cloud_progress"
			p.NextRunAt = now + 5
			_ = s.store.UpdatePipeline(ctx, p)
			_ = s.store.RecordEvent(ctx, p.ID, "CLOUD_EXPORT_STARTED", "cloud_progress", "已提交 115 云解压导出", true)

		case "cloud_progress":
			st, err := cloud.ExtractStatus(ctx, p.RemoteID)
			if err != nil {
				p.NextRunAt = now + 10
				_ = s.store.UpdatePipeline(ctx, p)
				continue
			}
			switch st {
			case "success":
				origNote := s.handleCloudOriginalAfterSuccess(ctx, cloud, cfg, p, loc, rule)
				p.State = "completed"
				p.Stage = "complete"
				p.ErrorCode = ""
				p.ErrorMessage = ""
				p.CompletedAt = &now
				p.NextRunAt = 0
				outBytes, _ := json.Marshal(map[string]any{
					"engine":          "115-cloud",
					"output_cid":      p.TargetCID,
					"original_action": rule.OriginalAction,
					"original_note":   origNote,
				})
				p.OutputJSON = string(outBytes)
				_ = s.store.UpdatePipeline(ctx, p)
				msg := "115 云解压完成：" + p.Name
				if origNote != "" {
					msg += "（" + origNote + "）"
				}
				_ = s.store.RecordEvent(ctx, p.ID, "SUCCEEDED", "complete", msg, true)
			case "failed":
				s.handleCloudFailure(ctx, cloud, cfg, p, loc, rule, "CLOUD_EXTRACT_FAILED", "115 云解压执行失败", true)
			default:
				p.NextRunAt = now + 8
				_ = s.store.UpdatePipeline(ctx, p)
			}

		case "caching":
			if err := s.cacheCloudArchive(ctx, cloud, &p, &loc); err != nil {
				p.State = "failed"
				p.Stage = "cache"
				p.ErrorCode = "CACHE_DOWNLOAD_FAILED"
				p.ErrorMessage = err.Error()
				p.NextRunAt = 0
				_ = s.store.UpdatePipeline(ctx, p)
				_ = s.store.RecordEvent(ctx, p.ID, "FAILED", "cache", "直链缓存失败："+err.Error(), true)
				continue
			}
			locBytes, _ := json.Marshal(loc)
			p.LocatorJSON = string(locBytes)
			p.State = "local_extract"
			p.Stage = "local_extract"
			p.NextRunAt = now
			_ = s.store.UpdatePipeline(ctx, p)
			_ = s.store.RecordEvent(ctx, p.ID, "CACHE_READY", "local_extract", "原包已缓存到本地，准备 7-Zip 解压", false)
		}
	}
	return nil
}

// handleCloudFailure automatically retries 115 cloud extraction once (15s backoff) for non-password errors,
// then applies the configured failure policy (confirm_local / auto_local / move_failed).
func (s *Service) handleCloudFailure(ctx context.Context, cloud p115.Port, cfg P115Settings, p store.Pipeline, loc CloudLocator, rule ArchiveRuleConfig, code, msg string, allowRetry bool) {
	now := time.Now().Unix()
	if allowRetry && loc.CloudRetries < 1 {
		loc.CloudRetries++
		locBytes, _ := json.Marshal(loc)
		p.LocatorJSON = string(locBytes)
		p.State = "cloud_prepare"
		p.Stage = "cloud_prepare"
		p.ErrorCode = code
		p.ErrorMessage = msg
		p.NextRunAt = now + 15
		_ = s.store.UpdatePipeline(ctx, p)
		_ = s.store.RecordEvent(ctx, p.ID, "CLOUD_EXTRACT_RETRIED", "cloud_prepare",
			msg+"，15 秒后自动重试（第 1/1 次）", true)
		return
	}

	switch rule.CloudFailureAction {
	case "auto_local":
		p.State = "caching"
		p.Stage = "cache"
		p.NextRunAt = now
		p.ErrorCode = code
		p.ErrorMessage = msg
		_ = s.store.UpdatePipeline(ctx, p)
		_ = s.store.RecordEvent(ctx, p.ID, "AUTO_LOCAL_FALLBACK", "cache", msg+"，已自动切换本地兜底解压", true)
	case "move_failed":
		note := ""
		if cloud != nil && cfg.CloudFailedCID != "" && loc.FID != "" {
			if err := cloud.MoveVerified(ctx, p115.FileLocator{
				FID: loc.FID, CID: loc.CID, Size: loc.Size, SHA1: loc.SHA1,
			}, cfg.CloudFailedCID); err == nil {
				note = "，原包已移入归档区/异常待查"
			}
		}
		p.State = "failed"
		p.Stage = "cloud_progress"
		p.ErrorCode = code
		p.ErrorMessage = msg + note
		p.NextRunAt = 0
		_ = s.store.UpdatePipeline(ctx, p)
		_ = s.store.RecordEvent(ctx, p.ID, "FAILED", "cloud_progress", msg+note, true)
	default: // confirm_local
		p.State = "wait_local_confirm"
		p.Stage = "local_fallback"
		p.WaitReason = msg
		p.ErrorCode = code
		p.ErrorMessage = msg
		p.NextRunAt = 0
		_ = s.store.UpdatePipeline(ctx, p)
		_ = s.store.RecordEvent(ctx, p.ID, "WAITING_APPROVAL", "local_fallback", msg+"，等待确认是否下载到本地解压", true)
	}
}

func (s *Service) handleCloudOriginalAfterSuccess(ctx context.Context, cloud p115.Port, cfg P115Settings, p store.Pipeline, loc CloudLocator, rule ArchiveRuleConfig) string {
	if cloud == nil || loc.FID == "" {
		return ""
	}
	fLoc := p115.FileLocator{FID: loc.FID, CID: loc.CID, Size: loc.Size, SHA1: loc.SHA1}
	switch rule.OriginalAction {
	case "delete":
		if err := cloud.DeleteVerified(ctx, fLoc); err == nil {
			_ = s.store.RecordEvent(ctx, p.ID, "ORIGINAL_DELETED", "complete", "已按规则删除云端原压缩包", false)
			return "原包已删除"
		}
	case "archive":
		if cfg.CloudArchiveCID == "" {
			return ""
		}
		targetDate := p.TargetDate
		if targetDate == "" {
			targetDate = time.Now().In(s.loc).Format("2006-01-02")
		}
		archDateCID, err := s.resolveDateFolder(ctx, cloud, cfg.CloudArchiveCID, targetDate)
		if err != nil {
			_ = s.store.RecordEvent(ctx, p.ID, "ARCHIVE_MOVE_WARN", "complete", "原包移入归档区失败（解压成品已保留）："+err.Error(), false)
			return ""
		}
		if err := cloud.MoveVerified(ctx, fLoc, archDateCID); err != nil {
			_ = s.store.RecordEvent(ctx, p.ID, "ARCHIVE_MOVE_WARN", "complete", "原包移入归档区失败（解压成品已保留）："+err.Error(), false)
			return ""
		}
		_ = s.store.RecordEvent(ctx, p.ID, "ORIGINAL_ARCHIVED", "complete", "原压缩包已移入归档区/原包留存/"+targetDate, false)
		return "原包已移入归档区/原包留存"
	}
	return ""
}

// cacheCloudArchive downloads a 115 archive using a fixed 32KB streaming buffer (ultra-low memory).
func (s *Service) cacheCloudArchive(ctx context.Context, cloud p115.Port, p *store.Pipeline, loc *CloudLocator) error {
	info, err := cloud.DownloadURL(ctx, loc.Pickcode)
	if err != nil {
		return err
	}
	cacheRoot := s.cacheDir()
	destDir := filepath.Join(cacheRoot, p.ID)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}
	finalPath := filepath.Join(destDir, filepath.Base(loc.Name))
	partPath := finalPath + ".part"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, info.URL, nil)
	if err != nil {
		return err
	}
	for k, v := range info.Headers {
		req.Header.Set(k, v)
	}
	httpClient := &http.Client{Timeout: 0} // streaming large files controlled via ctx
	resp, err := httpClient.Do(req)
	if err != nil {
		return errors.New("DIRECT_DOWNLOAD_REQUEST_FAILED")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("DIRECT_DOWNLOAD_HTTP_%d", resp.StatusCode)
	}

	out, err := os.Create(partPath)
	if err != nil {
		return err
	}
	buf := make([]byte, 32*1024)
	var written int64
	lastReport := time.Now()
	for {
		n, rErr := resp.Body.Read(buf)
		if n > 0 {
			if _, wErr := out.Write(buf[:n]); wErr != nil {
				_ = out.Close()
				_ = os.Remove(partPath)
				return wErr
			}
			written += int64(n)
			if time.Since(lastReport) >= 2*time.Second {
				prog, _ := json.Marshal(map[string]any{"bytes_done": written, "total_bytes": loc.Size})
				p.ProgressJSON = string(prog)
				_ = s.store.UpdatePipeline(ctx, *p)
				lastReport = time.Now()
			}
		}
		if errors.Is(rErr, io.EOF) {
			break
		}
		if rErr != nil {
			_ = out.Close()
			_ = os.Remove(partPath)
			return rErr
		}
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(partPath)
		return err
	}
	if loc.Size > 0 && written != loc.Size {
		_ = os.Remove(partPath)
		return errors.New("CACHE_SIZE_MISMATCH")
	}
	if err := os.Rename(partPath, finalPath); err != nil {
		return err
	}
	loc.CachePath = finalPath
	return nil
}

// stepLocalExtractions executes local_archive and fallback cloud_archive pipelines serially via 7zz.
func (s *Service) stepLocalExtractions(ctx context.Context) error {
	now := time.Now().Unix()
	var due []store.Pipeline
	if localDue, err := s.store.ListDuePipelines(ctx, "local_archive", []string{"local_extract"}, now, 1); err == nil {
		due = append(due, localDue...)
	}
	if len(due) == 0 {
		if cloudDue, err := s.store.ListDuePipelines(ctx, "cloud_archive", []string{"local_extract"}, now, 1); err == nil {
			due = append(due, cloudDue...)
		}
	}
	if len(due) == 0 {
		return nil
	}

	p := due[0]
	rule := s.ArchiveRule(ctx)
	var srcPath, outRoot string
	var members []string
	var cloudLoc CloudLocator

	if p.Kind == "local_archive" {
		var loc struct {
			Path    string   `json:"path"`
			Members []string `json:"members"`
		}
		_ = json.Unmarshal([]byte(p.LocatorJSON), &loc)
		var cfg localwatch.LocalSourceConfig
		_ = json.Unmarshal([]byte(p.PolicyJSON), &cfg)
		srcPath = loc.Path
		members = loc.Members
		outRoot = cfg.OutputDir
		if cfg.DeleteOriginal {
			rule.OriginalAction = "delete"
		}
	} else {
		_ = json.Unmarshal([]byte(p.LocatorJSON), &cloudLoc)
		var perRule ArchiveRuleConfig
		if json.Unmarshal([]byte(p.PolicyJSON), &perRule) == nil {
			if perRule.OriginalAction != "" {
				rule.OriginalAction = perRule.OriginalAction
			}
			if perRule.OutputDir != "" {
				outRoot = perRule.OutputDir
			}
		}
		srcPath = cloudLoc.CachePath
	}
	if outRoot == "" {
		outRoot = s.outputDir()
	}

	targetDate := p.TargetDate
	if targetDate == "" {
		targetDate = time.Now().In(s.loc).Format("2006-01-02")
		p.TargetDate = targetDate
	}
	baseName := strings.TrimSuffix(filepath.Base(srcPath), filepath.Ext(srcPath))
	if baseName == "" {
		baseName = p.ID
	}
	finalOutDir := filepath.Join(outRoot, targetDate, baseName)

	_ = s.store.RecordEvent(ctx, p.ID, "LOCAL_EXTRACT_STARTED", "local_extract", "开始本地解压："+p.Name, true)
	res, err := s.extractor.Extract(ctx, extract.Options{
		Binary:           s.extractBin,
		Source:           srcPath,
		OutputDir:        finalOutDir,
		Passwords:        s.ArchivePasswords(ctx),
		MaxDictMB:        s.extractMaxDictMB,
		SquashRoot:       true,
		DisableRecursion: true,
	})
	if err != nil {
		p.State = "failed"
		p.Stage = "local_extract"
		p.ErrorCode = "LOCAL_EXTRACT_FAILED"
		p.ErrorMessage = err.Error()
		p.NextRunAt = 0
		_ = s.store.UpdatePipeline(ctx, p)
		_ = s.store.RecordEvent(ctx, p.ID, "FAILED", "local_extract", "本地解压失败（未删除原包）："+err.Error(), true)
		return nil
	}

	// Handle original archive after successful local extraction.
	origNote := ""
	if p.Kind == "cloud_archive" {
		if srcPath != "" {
			_ = os.RemoveAll(filepath.Dir(srcPath))
		}
		if cloud, cfg := s.snapshotCloud(); cloud != nil {
			origNote = s.handleCloudOriginalAfterSuccess(ctx, cloud, cfg, p, cloudLoc, rule)
		}
	} else {
		origNote = s.handleLocalOriginalAfterSuccess(ctx, p, members, targetDate, rule)
	}

	outBytes, _ := json.Marshal(res)
	p.OutputJSON = string(outBytes)
	p.State = "completed"
	p.Stage = "complete"
	p.ErrorCode = ""
	p.ErrorMessage = ""
	p.CompletedAt = &now
	p.NextRunAt = 0
	_ = s.store.UpdatePipeline(ctx, p)
	msg := fmt.Sprintf("本地解压完成：%s（%d 个文件）", p.Name, len(res.Files))
	if origNote != "" {
		msg += "，" + origNote
	}
	_ = s.store.RecordEvent(ctx, p.ID, "SUCCEEDED", "complete", msg, true)
	return nil
}

func (s *Service) handleLocalOriginalAfterSuccess(ctx context.Context, p store.Pipeline, members []string, targetDate string, rule ArchiveRuleConfig) string {
	if len(members) == 0 {
		return ""
	}
	switch rule.OriginalAction {
	case "delete":
		for _, m := range members {
			_ = os.Remove(m)
		}
		_ = s.store.RecordEvent(ctx, p.ID, "ORIGINAL_DELETED", "complete", "已按规则删除本地原压缩包", false)
		return "原包已删除"
	case "archive":
		destDir := filepath.Join(s.originalsDir(), targetDate)
		if err := os.MkdirAll(destDir, 0o755); err != nil {
			return ""
		}
		moved := 0
		for _, m := range members {
			dst := uniqueDestPath(filepath.Join(destDir, filepath.Base(m)))
			if err := moveFileSafe(m, dst); err == nil {
				moved++
			}
		}
		if moved > 0 {
			_ = s.store.RecordEvent(ctx, p.ID, "ORIGINAL_ARCHIVED", "complete", "本地原包已移入归档区/原包留存/"+targetDate, false)
			return "原包已移入归档区/原包留存"
		}
	}
	return ""
}

func uniqueDestPath(dst string) string {
	if _, err := os.Stat(dst); os.IsNotExist(err) {
		return dst
	}
	ext := filepath.Ext(dst)
	stem := strings.TrimSuffix(dst, ext)
	for i := 1; i <= 100; i++ {
		cand := fmt.Sprintf("%s_(%d)%s", stem, i, ext)
		if _, err := os.Stat(cand); os.IsNotExist(err) {
			return cand
		}
	}
	return fmt.Sprintf("%s_%d%s", stem, time.Now().Unix(), ext)
}

func moveFileSafe(src, dst string) error {
	if err := os.Rename(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	out, err := os.Create(dst)
	if err != nil {
		_ = in.Close()
		return err
	}
	buf := make([]byte, 32*1024)
	if _, err := io.CopyBuffer(out, in, buf); err != nil {
		_ = out.Close()
		_ = in.Close()
		_ = os.Remove(dst)
		return err
	}
	if err := out.Close(); err != nil {
		_ = in.Close()
		_ = os.Remove(dst)
		return err
	}
	_ = in.Close()
	return os.Remove(src)
}

func (s *Service) cacheDir() string {
	return config.LocalCacheDir(s.dataDir, s.workDir)
}

func (s *Service) outputDir() string {
	return config.LocalOutputDir(s.dataDir, s.workDir)
}

func (s *Service) originalsDir() string {
	return config.LocalOriginalsDir(s.dataDir, s.workDir)
}

func (s *Service) historyDir() string {
	return config.LocalHistoryDir(s.dataDir, s.workDir)
}

// ScanLocalNow triggers an immediate scan of local/NAS watched directories.
func (s *Service) ScanLocalNow(ctx context.Context) (int, error) {
	if s.localWatcher == nil {
		return 0, nil
	}
	return s.localWatcher.Scan(ctx, time.Now().In(s.loc))
}
