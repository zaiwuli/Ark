package api

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/zaiwuli/Ark/internal/store"
)

func (s *Server) cacheRootDir() string {
	if s.cfg.WorkDir != "" {
		return filepath.Join(s.cfg.WorkDir, "cache")
	}
	return filepath.Join(s.cfg.DataDir, "cache")
}

func (s *Server) outputRootDir() string {
	if s.cfg.WorkDir != "" {
		return filepath.Join(s.cfg.WorkDir, "output")
	}
	return filepath.Join(s.cfg.DataDir, "output")
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

func (s *Server) handleMaintenanceSummary(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.MaintenanceStats(r.Context(), s.cfg.DBPath())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load maintenance stats")
		return
	}
	cacheBytes, cacheFiles := measureDir(s.cacheRootDir())
	// Also count orphaned .ark-stage-* dirs in output directory.
	if entries, err := os.ReadDir(s.outputRootDir()); err == nil {
		for _, e := range entries {
			if e.IsDir() && strings.HasPrefix(e.Name(), ".ark-stage-") {
				b, f := measureDir(filepath.Join(s.outputRootDir(), e.Name()))
				cacheBytes += b
				cacheFiles += f
			}
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

	cacheRoot := s.cacheRootDir()
	if entries, err := os.ReadDir(cacheRoot); err == nil {
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
		outRoot := s.outputRootDir()
		if entries, err := os.ReadDir(outRoot); err == nil {
			for _, e := range entries {
				if e.IsDir() && (strings.HasPrefix(e.Name(), ".ark-stage-") || strings.HasPrefix(e.Name(), ".ark-squash-")) {
					full := filepath.Join(outRoot, e.Name())
					b, f := measureDir(full)
					if os.RemoveAll(full) == nil {
						freedBytes += b
						if f == 0 {
							f = 1
						}
						removedItems += f
					}
				}
			}
		}
	}

	_ = s.store.RecordEvent(r.Context(), "system", "CACHE_RECLAIMED", "complete",
		"手动回收本地缓存与临时残留目录完成", false)
	writeJSON(w, http.StatusOK, map[string]any{
		"freed_bytes":   freedBytes,
		"removed_items": removedItems,
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
