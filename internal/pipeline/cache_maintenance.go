package pipeline

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/zaiwuli/Ark/internal/config"
)

type CacheSummaryInfo struct {
	CacheBytes int64 `json:"cache_bytes"`
	CacheFiles int   `json:"cache_files"`
}

type CacheReclaimResult struct {
	FreedBytes    int64 `json:"freed_bytes"`
	RemovedItems  int   `json:"removed_items"`
	SkippedActive int   `json:"skipped_active"`
}

func (s *Service) CacheDirs() []string {
	dirs := []string{config.LocalCacheDir(s.dataDir, s.workDir)}
	base := s.workDir
	if base == "" {
		base = s.dataDir
	}
	legacy := filepath.Join(base, "cache")
	if legacy != dirs[0] {
		dirs = append(dirs, legacy)
	}
	return dirs
}

func (s *Service) OutputDirs() []string {
	dirs := []string{config.LocalOutputDir(s.dataDir, s.workDir)}
	base := s.workDir
	if base == "" {
		base = s.dataDir
	}
	legacy := filepath.Join(base, "output")
	if legacy != dirs[0] {
		dirs = append(dirs, legacy)
	}
	return dirs
}

func measureDirBytes(root string) (int64, int) {
	var total int64
	var files int
	_ = filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if info, iErr := d.Info(); iErr == nil {
			total += info.Size()
			files++
		}
		return nil
	})
	return total, files
}

func findOrphanStagingDirs(outRoot string) []string {
	var found []string
	entries, err := os.ReadDir(outRoot)
	if err != nil {
		return nil
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		full := filepath.Join(outRoot, name)
		if strings.HasPrefix(name, ".ark-stage-") || strings.HasPrefix(name, ".ark-squash-") {
			found = append(found, full)
			continue
		}
		if len(name) == 10 && name[4] == '-' && name[7] == '-' {
			if sub, subErr := os.ReadDir(full); subErr == nil {
				for _, se := range sub {
					if se.IsDir() && (strings.HasPrefix(se.Name(), ".ark-stage-") || strings.HasPrefix(se.Name(), ".ark-squash-")) {
						found = append(found, filepath.Join(full, se.Name()))
					}
				}
			}
		}
	}
	return found
}

func (s *Service) CacheSummary() CacheSummaryInfo {
	var info CacheSummaryInfo
	for _, cDir := range s.CacheDirs() {
		b, f := measureDirBytes(cDir)
		info.CacheBytes += b
		info.CacheFiles += f
	}
	for _, oDir := range s.OutputDirs() {
		for _, orphan := range findOrphanStagingDirs(oDir) {
			b, f := measureDirBytes(orphan)
			info.CacheBytes += b
			info.CacheFiles += f
		}
	}
	return info
}

func (s *Service) ReclaimCache(ctx context.Context) (CacheReclaimResult, error) {
	activeIDs, err := s.store.ActiveCachePipelineIDs(ctx)
	if err != nil {
		return CacheReclaimResult{}, err
	}
	var res CacheReclaimResult
	res.SkippedActive = len(activeIDs)

	for _, cacheRoot := range s.CacheDirs() {
		entries, err := os.ReadDir(cacheRoot)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if activeIDs[e.Name()] {
				continue
			}
			full := filepath.Join(cacheRoot, e.Name())
			b, f := measureDirBytes(full)
			if !e.IsDir() {
				if info, iErr := e.Info(); iErr == nil {
					b = info.Size()
					f = 1
				}
			}
			if os.RemoveAll(full) == nil {
				res.FreedBytes += b
				if f == 0 {
					f = 1
				}
				res.RemovedItems += f
			}
		}
	}

	if len(activeIDs) == 0 {
		for _, outRoot := range s.OutputDirs() {
			for _, orphan := range findOrphanStagingDirs(outRoot) {
				b, f := measureDirBytes(orphan)
				if os.RemoveAll(orphan) == nil {
					res.FreedBytes += b
					if f == 0 {
						f = 1
					}
					res.RemovedItems += f
				}
			}
		}
	}

	_ = s.store.RecordEvent(ctx, "system", "CACHE_RECLAIMED", "complete",
		"回收本地下载缓存与临时残留目录完成", false)
	return res, nil
}
