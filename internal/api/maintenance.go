package api

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/zaiwuli/Ark/internal/config"
	"github.com/zaiwuli/Ark/internal/pipeline"
	"github.com/zaiwuli/Ark/internal/store"
)

func (s *Server) cacheDirs() []string {
	dirs := []string{config.LocalCacheDir(s.cfg.DataDir, s.cfg.WorkDir)}
	if s.cfg.WorkDir != "" {
		legacy := filepath.Join(s.cfg.WorkDir, "cache")
		if legacy != dirs[0] {
			dirs = append(dirs, legacy)
		}
	} else {
		legacy := filepath.Join(s.cfg.DataDir, "cache")
		if legacy != dirs[0] {
			dirs = append(dirs, legacy)
		}
	}
	return dirs
}

func (s *Server) outputDirs() []string {
	dirs := []string{config.LocalOutputDir(s.cfg.DataDir, s.cfg.WorkDir)}
	if s.cfg.WorkDir != "" {
		legacy := filepath.Join(s.cfg.WorkDir, "output")
		if legacy != dirs[0] {
			dirs = append(dirs, legacy)
		}
	} else {
		legacy := filepath.Join(s.cfg.DataDir, "output")
		if legacy != dirs[0] {
			dirs = append(dirs, legacy)
		}
	}
	return dirs
}

func measureDir(root string) (int64, int) {
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

func findOrphanStageDirs(outRoot string) []string {
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
		// Also check inside YYYY-MM-DD date folders.
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

func (s *Server) handleMaintenanceSummary(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.MaintenanceStats(r.Context(), s.cfg.DBPath())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load maintenance stats")
		return
	}
	var cacheBytes int64
	var cacheFiles int
	for _, cDir := range s.cacheDirs() {
		b, f := measureDir(cDir)
		cacheBytes += b
		cacheFiles += f
	}
	for _, oDir := range s.outputDirs() {
		for _, orphan := range findOrphanStageDirs(oDir) {
			b, f := measureDir(orphan)
			cacheBytes += b
			cacheFiles += f
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"stats":       stats,
		"cache_bytes": cacheBytes,
		"cache_files": cacheFiles,
	})
}

func (s *Server) handleReclaimCache(w http.ResponseWriter, r *http.Request) {
	activeIDs, err := s.store.ActiveCachePipelineIDs(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to inspect active tasks")
		return
	}
	var freedBytes int64
	var removedItems int

	for _, cacheRoot := range s.cacheDirs() {
		entries, err := os.ReadDir(cacheRoot)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if activeIDs[e.Name()] {
				continue
			}
			full := filepath.Join(cacheRoot, e.Name())
			b, f := measureDir(full)
			if !e.IsDir() {
				if info, iErr := e.Info(); iErr == nil {
					b = info.Size()
					f = 1
				}
			}
			if os.RemoveAll(full) == nil {
				freedBytes += b
				if f == 0 {
					f = 1
				}
				removedItems += f
			}
		}
	}

	// Clean orphaned temporary staging directories only if no local extraction is active.
	if len(activeIDs) == 0 {
		for _, outRoot := range s.outputDirs() {
			for _, orphan := range findOrphanStageDirs(outRoot) {
				b, f := measureDir(orphan)
				if os.RemoveAll(orphan) == nil {
					freedBytes += b
					if f == 0 {
						f = 1
					}
					removedItems += f
				}
			}
		}
	}

	_ = s.store.RecordEvent(r.Context(), "system", "CACHE_RECLAIMED", "complete",
		"手动回收本地下载缓存与临时残留目录完成", false)
	writeJSON(w, http.StatusOK, map[string]any{
		"freed_bytes":    freedBytes,
		"removed_items":  removedItems,
		"skipped_active": len(activeIDs),
	})
}

func (s *Server) handleClearPipelines(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Scope string `json:"scope"`
	}
	_ = decodeJSON(r, &req, 4096)
	if req.Scope == "" {
		req.Scope = "terminal"
	}
	deleted, err := s.store.ClearPipelines(r.Context(), req.Scope)
	if errors.Is(err, store.ErrInvalid) {
		writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "scope must be completed, failed, or terminal")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to clear pipelines")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}

func (s *Server) handleRetryFailedPipelines(w http.ResponseWriter, r *http.Request) {
	count, err := s.store.RetryAllFailed(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to retry tasks")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"retried": count})
}

func (s *Server) handleClearEvents(w http.ResponseWriter, r *http.Request) {
	deleted, err := s.store.ClearEvents(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to clear events")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}

func (s *Server) handleClearBarkMessages(w http.ResponseWriter, r *http.Request) {
	deleted, err := s.store.ClearBarkMessages(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to clear bark messages")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}

func (s *Server) handleVacuumDB(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Vacuum(r.Context()); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "vacuum failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleGetArchiveRule(w http.ResponseWriter, r *http.Request) {
	rule := s.pipeline.ArchiveRule(r.Context())
	writeJSON(w, http.StatusOK, rule)
}

func (s *Server) handlePutArchiveRule(w http.ResponseWriter, r *http.Request) {
	var req pipeline.ArchiveRuleConfig
	if err := decodeJSON(r, &req, 16*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	saved, err := s.pipeline.SaveArchiveRule(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, saved)
}

func (s *Server) handleTriggerCloudScan(w http.ResponseWriter, r *http.Request) {
	adopted, err := s.pipeline.ScanCloudSources(r.Context(), nil)
	if err != nil {
		s.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"adopted": adopted})
}

func (s *Server) handleGetOrganizerConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.pipeline.OrganizerConfig(r.Context()))
}

func (s *Server) handlePutOrganizerConfig(w http.ResponseWriter, r *http.Request) {
	var req pipeline.OrganizerConfig
	if err := decodeJSON(r, &req, 16*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	saved, err := s.pipeline.SaveOrganizerConfig(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, saved)
}

func (s *Server) handlePreviewOrganize(w http.ResponseWriter, r *http.Request) {
	plan, err := s.pipeline.PreviewOrganize(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (s *Server) handleRunOrganize(w http.ResponseWriter, r *http.Request) {
	res, err := s.pipeline.RunOrganize(r.Context(), false)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}
