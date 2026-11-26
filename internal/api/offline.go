package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/zaiwuli/Ark/internal/links"
	"github.com/zaiwuli/Ark/internal/pipeline"
	"github.com/zaiwuli/Ark/internal/store"
	"github.com/zaiwuli/Ark/internal/torrent"
)

const (
	maxImportRequestBytes = 5 * 1024 * 1024
	maxIdemKeyLen         = 128
)

// requireImportAuth validates Bearer admin tokens when provided or when NoAuth is disabled.
// In default LAN NoAuth mode (ARK_NO_AUTH=true), same-origin Web UI imports work even after creating Bark/API tokens.
func (s *Server) requireImportAuth(next http.HandlerFunc) http.HandlerFunc {
	return s.requireRole("admin", next)
}

func (s *Server) handleOfflineImport(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxImportRequestBytes)
	idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idemKey) > maxIdemKeyLen {
		writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "Idempotency-Key exceeds 128 chars")
		return
	}
	principal := principalFrom(r.Context())

	ct := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	var res store.BatchResult
	var err error
	diagSummary := fmt.Sprintf("ct=%s", ct)

	switch {
	case strings.HasPrefix(ct, "multipart/form-data"):
		if parseErr := r.ParseMultipartForm(maxImportRequestBytes); parseErr != nil {
			writeError(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request exceeds limit")
			return
		}
		textVal := firstNonEmpty(
			r.FormValue("text"),
			r.FormValue("url"),
			r.FormValue("urls"),
			r.FormValue("link"),
			r.FormValue("links"),
			r.FormValue("content"),
		)
		if strings.TrimSpace(textVal) == "" && r.MultipartForm != nil {
			var extras []string
			for k, vals := range r.MultipartForm.Value {
				if strings.EqualFold(k, "auto_extract") {
					continue
				}
				for _, v := range vals {
					if strings.TrimSpace(v) != "" {
						extras = append(extras, v)
					}
				}
			}
			textVal = strings.Join(extras, "\n")
		}
		autoExtract, ok := parseAutoExtractForm(r.FormValue("auto_extract"))
		if !ok {
			writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "auto_extract must be boolean")
			return
		}
		file, header, fileErr := r.FormFile("file")
		if fileErr != nil && r.MultipartForm != nil {
			// Fallback: accept the first uploaded file under any field name (common in iOS Shortcuts).
			for _, fhs := range r.MultipartForm.File {
				if len(fhs) > 0 {
					if f, openErr := fhs[0].Open(); openErr == nil {
						file, header, fileErr = f, fhs[0], nil
						break
					}
				}
			}
		}
		if fileErr == nil {
			defer file.Close()
			raw, readErr := io.ReadAll(io.LimitReader(file, maxImportRequestBytes+1))
			if readErr != nil {
				writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "failed to read uploaded file")
				return
			}
			if len(raw) > maxImportRequestBytes {
				writeError(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "uploaded file exceeds limit")
				return
			}
			isTorrent := (header != nil && strings.HasSuffix(strings.ToLower(header.Filename), ".torrent")) || looksLikeTorrent(raw)
			if isTorrent {
				if strings.TrimSpace(textVal) != "" {
					writeError(w, http.StatusUnprocessableEntity, "TORRENT_IMPORT_SEPARATELY", "cannot mix torrent file with link text")
					return
				}
				if len(raw) > torrent.MaxTorrentBytes {
					writeError(w, http.StatusRequestEntityTooLarge, "TORRENT_SIZE_LIMIT", "torrent exceeds 2 MiB")
					return
				}
				res, err = s.pipeline.ImportTorrent(r.Context(), principal.TokenID, idemKey, raw, autoExtract)
			} else {
				fileText := strings.TrimPrefix(string(raw), "\xef\xbb\xbf")
				if strings.TrimSpace(textVal) != "" {
					textVal += "\n" + fileText
				} else {
					textVal = fileText
				}
				fname := ""
				if header != nil {
					fname = header.Filename
				}
				diagSummary = fmt.Sprintf("ct=multipart, file=%s, bytes=%d", fname, len(raw))
				res, err = s.pipeline.ImportText(r.Context(), principal.TokenID, idemKey, textVal, autoExtract)
			}
		} else {
			diagSummary = fmt.Sprintf("ct=multipart, text_len=%d", len(textVal))
			res, err = s.pipeline.ImportText(r.Context(), principal.TokenID, idemKey, textVal, autoExtract)
		}

	case strings.HasPrefix(ct, "application/x-www-form-urlencoded"):
		if parseErr := r.ParseForm(); parseErr != nil {
			writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid form body")
			return
		}
		textVal := firstNonEmpty(
			r.FormValue("text"),
			r.FormValue("url"),
			r.FormValue("urls"),
			r.FormValue("link"),
			r.FormValue("links"),
			r.FormValue("content"),
		)
		if strings.TrimSpace(textVal) == "" {
			var extras []string
			for k, vals := range r.Form {
				if strings.EqualFold(k, "auto_extract") {
					continue
				}
				extras = append(extras, vals...)
			}
			textVal = strings.Join(extras, "\n")
		}
		autoExtract, ok := parseAutoExtractForm(r.FormValue("auto_extract"))
		if !ok {
			writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "auto_extract must be boolean")
			return
		}
		diagSummary = fmt.Sprintf("ct=form, text_len=%d", len(textVal))
		res, err = s.pipeline.ImportText(r.Context(), principal.TokenID, idemKey, textVal, autoExtract)

	default:
		raw, readErr := io.ReadAll(io.LimitReader(r.Body, maxImportRequestBytes+1))
		if readErr != nil {
			writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "failed to read request body")
			return
		}
		if len(raw) > maxImportRequestBytes {
			writeError(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request exceeds limit")
			return
		}
		// Auto-detect raw .torrent binary uploads (e.g. iOS Shortcuts "Request Body: File").
		if looksLikeTorrent(raw) {
			if len(raw) > torrent.MaxTorrentBytes {
				writeError(w, http.StatusRequestEntityTooLarge, "TORRENT_SIZE_LIMIT", "torrent exceeds 2 MiB")
				return
			}
			res, err = s.pipeline.ImportTorrent(r.Context(), principal.TokenID, idemKey, raw, true)
			break
		}

		trimmed := bytes.TrimSpace(bytes.TrimPrefix(raw, []byte{0xef, 0xbb, 0xbf}))
		autoExtract := true
		var textVal string
		var jsonKeys []string

		if strings.HasPrefix(ct, "application/json") || (len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[' || trimmed[0] == '"')) {
			var anyVal any
			if decErr := json.Unmarshal(trimmed, &anyVal); decErr != nil {
				if strings.HasPrefix(ct, "application/json") {
					writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON payload")
					return
				}
				textVal = string(trimmed)
			} else {
				switch v := anyVal.(type) {
				case map[string]any:
					for k := range v {
						jsonKeys = append(jsonKeys, k)
					}
					textVal = extractTextFromJSONMap(v)
					if rawAuto, ok := v["auto_extract"]; ok {
						switch b := rawAuto.(type) {
						case bool:
							autoExtract = b
						case string:
							if parsed, bErr := strconv.ParseBool(strings.TrimSpace(b)); bErr == nil {
								autoExtract = parsed
							}
						}
					}
				case string:
					textVal = v
				case []any:
					textVal = flattenJSONStrings(v)
				}
			}
		} else {
			// Plain text / raw body (compatible with legacy 115tools & iOS Shortcuts text body).
			textVal = string(trimmed)
		}

		if strings.TrimSpace(textVal) == "" {
			q := r.URL.Query()
			textVal = firstNonEmpty(q.Get("text"), q.Get("url"), q.Get("link"), q.Get("links"))
		}
		sample := strings.ReplaceAll(strings.TrimSpace(textVal), "\n", " ")
		if rSample := []rune(sample); len(rSample) > 40 {
			sample = string(rSample[:40]) + "…"
		}
		diagSummary = fmt.Sprintf("ct=%s, bytes=%d, keys=%v, text_len=%d, sample=%q", ct, len(raw), jsonKeys, len(textVal), sample)
		res, err = s.pipeline.ImportText(r.Context(), principal.TokenID, idemKey, textVal, autoExtract)
	}

	if err != nil {
		if errors.Is(err, links.ErrNoLinks) {
			_ = s.store.RecordEvent(r.Context(), "system", "IMPORT_NO_LINKS", "offline_submit",
				"离线导入未识别到链接（"+diagSummary+"）", false)
			writeError(w, http.StatusUnprocessableEntity, "NO_LINKS",
				"未识别到有效磁链/ed2k/HTTP链接（"+diagSummary+"）")
			return
		}
		s.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, res)
}

func looksLikeTorrent(raw []byte) bool {
	if len(raw) < 20 || len(raw) > torrent.MaxTorrentBytes || raw[0] != 'd' {
		return false
	}
	_, err := torrent.ParseToMagnet(raw)
	return err == nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func parseAutoExtractForm(raw string) (bool, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return true, true
	}
	b, err := strconv.ParseBool(s)
	return b, err == nil
}

func extractTextFromJSONMap(obj map[string]any) string {
	keys := []string{"text", "url", "urls", "link", "links", "content", "magnet", "data"}
	for _, k := range keys {
		raw, ok := obj[k]
		if !ok || raw == nil {
			continue
		}
		if s := flattenJSONStrings(raw); s != "" {
			return s
		}
	}
	// Fallback: collect all non-auto_extract string values in the JSON object.
	var parts []string
	for k, raw := range obj {
		if strings.EqualFold(k, "auto_extract") || raw == nil {
			continue
		}
		if s := flattenJSONStrings(raw); s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, "\n")
}

func flattenJSONStrings(v any) string {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x)
	case []any:
		var parts []string
		for _, item := range x {
			if s := flattenJSONStrings(item); s != "" {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, "\n")
	case map[string]any:
		var parts []string
		for _, val := range x {
			if s := flattenJSONStrings(val); s != "" {
				parts = append(parts, s)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}

func (s *Server) handleGetBatch(w http.ResponseWriter, r *http.Request) {
	p := principalFrom(r.Context())
	batchID := r.PathValue("batch_id")
	detail, err := s.store.GetBatch(r.Context(), batchID, p.TokenID)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "BATCH_NOT_FOUND", "batch not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to read batch")
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleListPipelines(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	items, err := s.store.ListPipelines(r.Context(), q.Get("kind"), q.Get("state"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list pipelines")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleGetPipeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	item, err := s.store.GetPipeline(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "PIPELINE_NOT_FOUND", "pipeline not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to get pipeline")
		return
	}
	events, _ := s.store.ListEvents(r.Context(), id, 50)
	writeJSON(w, http.StatusOK, map[string]any{"item": item, "events": events})
}

func (s *Server) handleRetryPipeline(w http.ResponseWriter, r *http.Request) {
	item, err := s.store.RetryPipeline(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "PIPELINE_NOT_FOUND", "pipeline not found")
		return
	}
	if errors.Is(err, store.ErrPipelineBusy) {
		writeError(w, http.StatusConflict, "PIPELINE_NOT_RETRYABLE", "pipeline is not in a retryable state")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to retry pipeline")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"item": item})
}

func (s *Server) handleCancelPipeline(w http.ResponseWriter, r *http.Request) {
	item, err := s.store.CancelPipeline(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "PIPELINE_NOT_FOUND", "pipeline not found")
		return
	}
	if errors.Is(err, store.ErrPipelineBusy) {
		writeError(w, http.StatusConflict, "PIPELINE_NOT_CANCELLABLE", "completed pipeline cannot be cancelled")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to cancel pipeline")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"item": item})
}

func (s *Server) handleDeletePipeline(w http.ResponseWriter, r *http.Request) {
	qForce := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("force")))
	force := qForce == "1" || qForce == "true"
	err := s.store.DeletePipeline(r.Context(), r.PathValue("id"), force)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "PIPELINE_NOT_FOUND", "pipeline not found")
		return
	}
	if errors.Is(err, store.ErrPipelineBusy) {
		writeError(w, http.StatusConflict, "PIPELINE_BUSY", "cannot delete active pipeline without force=true")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to delete pipeline")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

func (s *Server) handleSchedulerPause(w http.ResponseWriter, r *http.Request) {
	if err := s.pipeline.SetPaused(r.Context(), true); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scheduler_paused": true})
}

func (s *Server) handleSchedulerResume(w http.ResponseWriter, r *http.Request) {
	if err := s.pipeline.SetPaused(r.Context(), false); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"scheduler_paused": false})
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	items, err := s.store.ListEvents(r.Context(), q.Get("pipeline_id"), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list events")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleGetP115(w http.ResponseWriter, _ *http.Request) {
	cfg := s.pipeline.P115Config()
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":                cfg.Enabled,
		"has_cookie":             cfg.Cookie != "",
		"cloud_root_cid":         cfg.CloudRootCID,
		"offline_root_cid":       cfg.OfflineRootCID,
		"cloud_output_cid":       cfg.CloudOutputCID,
		"cloud_archive_zone_cid": cfg.CloudArchiveZoneCID,
		"cloud_archive_cid":      cfg.CloudArchiveCID,
		"cloud_failed_cid":       cfg.CloudFailedCID,
		"cloud_history_cid":      cfg.CloudHistoryCID,
		"cloud_extract_by_date":  cfg.ExtractByDateEnabled(),
		"life_monitor_enabled":   cfg.LifeMonitorEnabled,
		"cloud_scan_depth":       cfg.ScanDepth(),
		"governor":               s.pipeline.GovernorStats(),
	})
}

func (s *Server) handlePutP115(w http.ResponseWriter, r *http.Request) {
	var req pipeline.P115Settings
	if err := decodeJSON(r, &req, 64*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	saved, err := s.pipeline.SaveP115Config(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"saved":                  true,
		"enabled":                saved.Enabled,
		"has_cookie":             saved.Cookie != "",
		"cloud_root_cid":         saved.CloudRootCID,
		"offline_root_cid":       saved.OfflineRootCID,
		"cloud_output_cid":       saved.CloudOutputCID,
		"cloud_archive_zone_cid": saved.CloudArchiveZoneCID,
		"cloud_archive_cid":      saved.CloudArchiveCID,
		"cloud_failed_cid":       saved.CloudFailedCID,
		"cloud_history_cid":      saved.CloudHistoryCID,
		"cloud_extract_by_date":  saved.ExtractByDateEnabled(),
		"life_monitor_enabled":   saved.LifeMonitorEnabled,
		"cloud_scan_depth":       saved.ScanDepth(),
	})
}

func (s *Server) handleInitCloudRoot(w http.ResponseWriter, r *http.Request) {
	var req struct {
		RootCID string `json:"root_cid"`
	}
	if err := decodeJSON(r, &req, 16*1024); err != nil || strings.TrimSpace(req.RootCID) == "" {
		writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "root_cid required")
		return
	}
	saved, err := s.pipeline.InitCloudRoot(r.Context(), req.RootCID)
	if err != nil {
		s.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"cloud_root_cid":         saved.CloudRootCID,
		"offline_root_cid":       saved.OfflineRootCID,
		"cloud_output_cid":       saved.CloudOutputCID,
		"cloud_archive_zone_cid": saved.CloudArchiveZoneCID,
		"cloud_archive_cid":      saved.CloudArchiveCID,
		"cloud_failed_cid":       saved.CloudFailedCID,
		"cloud_history_cid":      saved.CloudHistoryCID,
	})
}

func (s *Server) handleTriggerLifeCheck(w http.ResponseWriter, r *http.Request) {
	found, err := s.pipeline.TriggerLifeCheck(r.Context())
	if err != nil {
		s.writeDomainError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"archives_found": found})
}

func (s *Server) handleListSources(w http.ResponseWriter, r *http.Request) {
	items, err := s.store.ListSources(r.Context(), r.URL.Query().Get("kind"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list sources")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleUpsertSource(w http.ResponseWriter, r *http.Request) {
	var req store.Source
	if err := decodeJSON(r, &req, 64*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	saved, err := s.store.UpsertSource(r.Context(), req)
	if errors.Is(err, store.ErrInvalid) {
		writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "name and kind (cloud/local) required")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to save source")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"item": saved})
}

func (s *Server) handleDeleteSource(w http.ResponseWriter, r *http.Request) {
	err := s.store.DeleteSource(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "SOURCE_NOT_FOUND", "source not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to delete source")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

func (s *Server) handleListRules(w http.ResponseWriter, r *http.Request) {
	items, err := s.store.ListRules(r.Context(), r.URL.Query().Get("scope"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list rules")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleUpsertRule(w http.ResponseWriter, r *http.Request) {
	var req store.Rule
	if err := decodeJSON(r, &req, 64*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	saved, err := s.store.UpsertRule(r.Context(), req)
	if errors.Is(err, store.ErrInvalid) {
		writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "name and scope (global/source/seed) required")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to save rule")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"item": saved})
}

func (s *Server) handleDeleteRule(w http.ResponseWriter, r *http.Request) {
	err := s.store.DeleteRule(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "RULE_NOT_FOUND", "rule not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to delete rule")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

func (s *Server) writeDomainError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT", err.Error())
	case errors.Is(err, pipeline.ErrCloudRootRequired):
		writeError(w, http.StatusConflict, "CLOUD_ROOT_REQUIRED", "configure 115 cloud root first")
	case errors.Is(err, pipeline.ErrP115NotConfigured):
		writeError(w, http.StatusConflict, "115_CONNECTION_REQUIRED", "configure 115 cookie first")
	case errors.Is(err, links.ErrNoLinks), errors.Is(err, links.ErrImportLimit):
		writeError(w, http.StatusUnprocessableEntity, err.Error(), err.Error())
	case errors.Is(err, torrent.ErrSizeLimit):
		writeError(w, http.StatusRequestEntityTooLarge, "TORRENT_SIZE_LIMIT", "torrent exceeds 2 MiB")
	case errors.Is(err, torrent.ErrV2Unsupported), errors.Is(err, torrent.ErrInvalid):
		writeError(w, http.StatusUnprocessableEntity, "INVALID_TORRENT", err.Error())
	default:
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
	}
}
