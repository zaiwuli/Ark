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

	"github.com/zaiwuli/Ark/internal/extract"
	"github.com/zaiwuli/Ark/internal/localwatch"
	"github.com/zaiwuli/Ark/internal/p115"
	"github.com/zaiwuli/Ark/internal/store"
)

type CloudLocator struct {
	FID       string `json:"fid"`
	CID       string `json:"cid"`
	Pickcode  string `json:"pickcode"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	SHA1      string `json:"sha1"`
	CachePath string `json:"cache_path,omitempty"`
}

type ArchiveRuleConfig struct {
	AutoFallback     bool   `json:"auto_fallback"`
	SquashRoot       bool   `json:"squash_root"`
	DisableRecursion bool   `json:"disable_recursion"`
	OutputDateDir    bool   `json:"output_date_dir"`
	OutputCID        string `json:"output_cid,omitempty"`
	OutputDir        string `json:"output_dir,omitempty"`
	DeleteOriginal   bool   `json:"delete_original"`
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
	_ = s.store.RecordEvent(ctx, p.ID, "LOCAL_FALLBACK_APPROVED", "cache", "已确认本地兜底，准备通过 115 直链缓存原包", false)
	return p, nil
}

// discoverOfflineArchives scans TargetCID for completed offline tasks and adopts archive files into cloud_archive pipelines.
func (s *Service) discoverOfflineArchives(ctx context.Context, cloud p115.Port, cfg P115Settings) error {
	now := time.Now().Unix()
	due, err := s.store.ListDuePipelines(ctx, "offline", []string{"discover"}, now, 10)
	if err != nil || len(due) == 0 {
		return err
	}

	for _, p := range due {
		if p.TargetCID == "" {
			continue
		}
		page, err := cloud.Files(ctx, p.TargetCID, 0)
		if err != nil {
			p.NextRunAt = now + 60
			_ = s.store.UpdatePipeline(ctx, p)
			continue
		}

		var candidates []p115.CloudFile
		for _, f := range page.Files {
			if !f.IsDir && extract.IsArchiveExt(f.Name) {
				candidates = append(candidates, f)
			} else if f.IsDir && strings.EqualFold(f.Name, p.Name) {
				if sub, subErr := cloud.Files(ctx, f.FID, 0); subErr == nil {
					for _, sf := range sub.Files {
						if !sf.IsDir && extract.IsArchiveExt(sf.Name) {
							candidates = append(candidates, sf)
						}
					}
				}
			}
		}

		adopted := 0
		for _, f := range candidates {
			locBytes, _ := json.Marshal(CloudLocator{
				FID: f.FID, CID: f.CID, Pickcode: f.Pickcode, Name: f.Name, Size: f.Size, SHA1: f.SHA1,
			})
			polBytes, _ := json.Marshal(ArchiveRuleConfig{
				SquashRoot:       true,
				DisableRecursion: true,
				OutputCID:        cfg.CloudOutputCID,
			})
			ident := fmt.Sprintf("cloud:%s:%d", f.FID, f.Size)
			if _, created, err := s.store.CreateCloudArchivePipeline(ctx, p.ID, "", ident, f.Name, f.Size, string(locBytes), string(polBytes)); err == nil && created {
				adopted++
			}
		}

		if adopted > 0 {
			p.State = "completed"
			p.Stage = "complete"
			p.CompletedAt = &now
			p.NextRunAt = 0
			_ = s.store.UpdatePipeline(ctx, p)
			_ = s.store.RecordEvent(ctx, p.ID, "ARCHIVE_HANDOFF", "complete", fmt.Sprintf("已发现 %d 个压缩包并转交云解压", adopted), false)
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

// stepCloudArchives advances cloud_archive pipelines through prepare -> inspect -> export -> progress -> caching.
func (s *Service) stepCloudArchives(ctx context.Context, cloud p115.Port, cfg P115Settings) error {
	now := time.Now().Unix()
	due, err := s.store.ListDuePipelines(ctx, "cloud_archive",
		[]string{"cloud_prepare", "cloud_inspect", "cloud_export", "cloud_progress", "caching"}, now, 5)
	if err != nil || len(due) == 0 {
		return err
	}
	passwords := s.ArchivePasswords(ctx)

	for _, p := range due {
		var loc CloudLocator
		_ = json.Unmarshal([]byte(p.LocatorJSON), &loc)
		var rule ArchiveRuleConfig
		_ = json.Unmarshal([]byte(p.PolicyJSON), &rule)

		switch p.State {
		case "cloud_prepare":
			pwd := ""
			if p.Attempts > 0 && p.Attempts <= len(passwords) {
				pwd = passwords[p.Attempts-1]
			}
			st, err := cloud.PrepareExtract(ctx, loc.Pickcode, pwd)
			if err != nil {
				s.failCloudToFallback(ctx, p, rule, "CLOUD_PREPARE_FAILED", err.Error())
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
					s.failCloudToFallback(ctx, p, rule, "CLOUD_PASSWORD_FAILED", "云解压密码不匹配")
					continue
				}
			case "failed":
				s.failCloudToFallback(ctx, p, rule, "CLOUD_PREPARE_FAILED", "115 云解压预检失败")
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
					s.failCloudToFallback(ctx, p, rule, "CLOUD_PASSWORD_FAILED", "云解压需要有效密码")
					continue
				}
			case "failed":
				s.failCloudToFallback(ctx, p, rule, "CLOUD_INSPECT_FAILED", "115 云解压分析失败")
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
			folderName := strings.TrimSuffix(loc.Name, filepath.Ext(loc.Name))
			if folderName == "" {
				folderName = p.ID
			}
			targetCID, err := cloud.EnsureFolder(ctx, outRoot, folderName)
			if err != nil {
				s.failCloudToFallback(ctx, p, rule, "CLOUD_OUTPUT_DIR_FAILED", err.Error())
				continue
			}
			remoteID, err := cloud.StartExtract(ctx, loc.Pickcode, targetCID)
			if err != nil {
				s.failCloudToFallback(ctx, p, rule, "CLOUD_EXPORT_FAILED", err.Error())
				continue
			}
			p.RemoteID = remoteID
			p.TargetCID = targetCID
			p.State = "cloud_progress"
			p.Stage = "cloud_progress"
			p.NextRunAt = now + 5
			_ = s.store.UpdatePipeline(ctx, p)
			_ = s.store.RecordEvent(ctx, p.ID, "CLOUD_EXPORT_STARTED", "cloud_progress", "已提交 115 云解压导出", false)

		case "cloud_progress":
			st, err := cloud.ExtractStatus(ctx, p.RemoteID)
			if err != nil {
				p.NextRunAt = now + 10
				_ = s.store.UpdatePipeline(ctx, p)
				continue
			}
			switch st {
			case "success":
				p.State = "completed"
				p.Stage = "complete"
				p.CompletedAt = &now
				p.NextRunAt = 0
				outBytes, _ := json.Marshal(map[string]any{"engine": "115-cloud", "output_cid": p.TargetCID})
				p.OutputJSON = string(outBytes)
				_ = s.store.UpdatePipeline(ctx, p)
				_ = s.store.RecordEvent(ctx, p.ID, "SUCCEEDED", "complete", "115 云解压完成："+p.Name, true)
			case "failed":
				s.failCloudToFallback(ctx, p, rule, "CLOUD_EXTRACT_FAILED", "115 云解压执行失败")
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

func (s *Service) failCloudToFallback(ctx context.Context, p store.Pipeline, rule ArchiveRuleConfig, code, msg string) {
	now := time.Now().Unix()
	if rule.AutoFallback {
		p.State = "caching"
		p.Stage = "cache"
		p.NextRunAt = now
		p.ErrorCode = code
		p.ErrorMessage = msg
		_ = s.store.UpdatePipeline(ctx, p)
		_ = s.store.RecordEvent(ctx, p.ID, "AUTO_LOCAL_FALLBACK", "cache", msg+"，已自动切换本地兜底解压", false)
		return
	}
	p.State = "wait_local_confirm"
	p.Stage = "local_fallback"
	p.WaitReason = msg
	p.ErrorCode = code
	p.ErrorMessage = msg
	p.NextRunAt = 0
	_ = s.store.UpdatePipeline(ctx, p)
	_ = s.store.RecordEvent(ctx, p.ID, "WAITING_APPROVAL", "local_fallback", msg+"，等待确认是否下载到本地解压", true)
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
	var srcPath, outRoot string
	var squash, disableRec, delOrig bool
	var members []string

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
		squash = cfg.SquashRoot
		disableRec = cfg.DisableRecursion
		delOrig = cfg.DeleteOriginal
	} else {
		var loc CloudLocator
		_ = json.Unmarshal([]byte(p.LocatorJSON), &loc)
		var rule ArchiveRuleConfig
		_ = json.Unmarshal([]byte(p.PolicyJSON), &rule)
		srcPath = loc.CachePath
		outRoot = rule.OutputDir
		squash = rule.SquashRoot
		disableRec = rule.DisableRecursion
	}
	if outRoot == "" {
		outRoot = s.outputDir()
	}

	baseName := strings.TrimSuffix(filepath.Base(srcPath), filepath.Ext(srcPath))
	if baseName == "" {
		baseName = p.ID
	}

	_ = s.store.RecordEvent(ctx, p.ID, "LOCAL_EXTRACT_STARTED", "local_extract", "开始本地解压："+p.Name, false)
	res, err := s.extractor.Extract(ctx, extract.Options{
		Binary:           s.extractBin,
		Source:           srcPath,
		OutputDir:        filepath.Join(outRoot, baseName),
		Passwords:        s.ArchivePasswords(ctx),
		MaxDictMB:        s.extractMaxDictMB,
		SquashRoot:       squash,
		DisableRecursion: disableRec,
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

	// Clean up downloaded cache for cloud fallback, or delete local original only if explicitly requested.
	if p.Kind == "cloud_archive" && srcPath != "" {
		_ = os.RemoveAll(filepath.Dir(srcPath))
	} else if p.Kind == "local_archive" && delOrig {
		for _, m := range members {
			_ = os.Remove(m)
		}
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
	_ = s.store.RecordEvent(ctx, p.ID, "SUCCEEDED", "complete",
		fmt.Sprintf("本地解压完成：%s（%d 个文件）", p.Name, len(res.Files)), true)
	return nil
}

func (s *Service) cacheDir() string {
	if s.workDir != "" {
		return filepath.Join(s.workDir, "cache")
	}
	return filepath.Join(s.dataDir, "cache")
}

func (s *Service) outputDir() string {
	if s.workDir != "" {
		return filepath.Join(s.workDir, "output")
	}
	return filepath.Join(s.dataDir, "output")
}

// ScanLocalNow triggers an immediate scan of local/NAS watched directories.
func (s *Service) ScanLocalNow(ctx context.Context) (int, error) {
	if s.localWatcher == nil {
		return 0, nil
	}
	return s.localWatcher.Scan(ctx, time.Now())
}
