package api

import (
	"errors"
	"net/http"

	"github.com/zaiwuli/Ark/internal/pipeline"
	"github.com/zaiwuli/Ark/internal/store"
)

func (s *Server) cacheDirs() []string {
	return s.pipeline.CacheDirs()
}

func (s *Server) handleMaintenanceSummary(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.MaintenanceStats(r.Context(), s.cfg.DBPath())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load maintenance stats")
		return
	}
	cs := s.pipeline.CacheSummary()
	writeJSON(w, http.StatusOK, map[string]any{
		"stats":       stats,
		"cache_bytes": cs.CacheBytes,
		"cache_files": cs.CacheFiles,
	})
}

func (s *Server) handleReclaimCache(w http.ResponseWriter, r *http.Request) {
	res, err := s.pipeline.ReclaimCache(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to reclaim cache")
		return
	}
	writeJSON(w, http.StatusOK, res)
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
	var req struct {
		Mode  string `json:"mode"`  // "today" | "depth"
		Depth int    `json:"depth"` // optional 1..5 override
	}
	_ = decodeJSON(r, &req, 4096)
	todayOnly := req.Mode == "today"
	adopted, err := s.pipeline.ScanCloudSourcesWithOptions(r.Context(), nil, todayOnly, req.Depth)
	if err != nil {
		s.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"adopted": adopted, "mode": req.Mode})
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
	plan, err := s.pipeline.PreviewOrganizeWithMode(r.Context(), true)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (s *Server) handleRunOrganize(w http.ResponseWriter, r *http.Request) {
	res, err := s.pipeline.RunOrganizeWithMode(r.Context(), false, true)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleListOrganizeTargets(w http.ResponseWriter, r *http.Request) {
	items, err := s.pipeline.ListOrganizeTargets(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleUpsertOrganizeTarget(w http.ResponseWriter, r *http.Request) {
	var req pipeline.OrganizeTarget
	if err := decodeJSON(r, &req, 16*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	saved, err := s.pipeline.UpsertOrganizeTarget(r.Context(), req)
	if errors.Is(err, store.ErrInvalid) {
		writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "name, kind (cloud/local), and target required")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"item": saved})
}

func (s *Server) handleDeleteOrganizeTarget(w http.ResponseWriter, r *http.Request) {
	err := s.pipeline.DeleteOrganizeTarget(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "TARGET_NOT_FOUND", "target not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

func (s *Server) handleCenterScan(w http.ResponseWriter, r *http.Request) {
	var req pipeline.CenterScanRequest
	if err := decodeJSON(r, &req, 32*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	jobID, err := s.pipeline.StartCenterScan(req)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error(), err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"job_id": jobID, "status": "running"})
}

func (s *Server) handleCenterExec(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ItemIDs []string `json:"item_ids"`
	}
	if err := decodeJSON(r, &req, 256*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	jobID, err := s.pipeline.StartCenterExec(req.ItemIDs)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error(), err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"job_id": jobID, "status": "running"})
}

func (s *Server) handleCenterState(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.pipeline.CenterJobState())
}

func (s *Server) handleCenterCancel(w http.ResponseWriter, _ *http.Request) {
	cancelled := s.pipeline.CancelCenterJob()
	writeJSON(w, http.StatusOK, map[string]any{"cancelled": cancelled})
}

func (s *Server) handleCenterUndoRename(w http.ResponseWriter, r *http.Request) {
	restored, err := s.pipeline.UndoLastRename(r.Context())
	if err != nil {
		writeError(w, http.StatusConflict, err.Error(), err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"restored": restored})
}

func (s *Server) handleGetWorkbenchConfig(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.pipeline.WorkbenchConfig(r.Context()))
}

func (s *Server) handlePutWorkbenchConfig(w http.ResponseWriter, r *http.Request) {
	var req pipeline.WorkbenchConfig
	if err := decodeJSON(r, &req, 64*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	saved, err := s.pipeline.SaveWorkbenchConfig(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, saved)
}

func (s *Server) handleBlacklistSHA1(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SHA1 string `json:"sha1"`
	}
	if err := decodeJSON(r, &req, 4096); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	saved, err := s.pipeline.AddJunkSHA1(r.Context(), req.SHA1)
	if errors.Is(err, store.ErrInvalid) {
		writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "valid sha1 required")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, saved)
}
