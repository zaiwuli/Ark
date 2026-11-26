package api

import (
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/zaiwuli/Ark/internal/bark"
	"github.com/zaiwuli/Ark/internal/notify"
	"github.com/zaiwuli/Ark/internal/store"
)

func (s *Server) handleConfirmLocalFallback(w http.ResponseWriter, r *http.Request) {
	item, err := s.pipeline.ConfirmLocalFallback(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "PIPELINE_NOT_FOUND", "pipeline not found")
		return
	}
	if errors.Is(err, store.ErrPipelineBusy) {
		writeError(w, http.StatusConflict, "INVALID_STATE", "pipeline is not waiting for local fallback approval")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"item": item})
}

func (s *Server) handleTriggerLocalScan(w http.ResponseWriter, r *http.Request) {
	adopted, err := s.pipeline.ScanLocalNow(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"adopted": adopted})
}

func (s *Server) handleGetPasswords(w http.ResponseWriter, r *http.Request) {
	pwds := s.pipeline.ArchivePasswords(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"items": pwds, "count": len(pwds)})
}

func (s *Server) handlePutPasswords(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Items []string `json:"items"`
	}
	if err := decodeJSON(r, &req, 64*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	if err := s.pipeline.SaveArchivePasswords(r.Context(), req.Items); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", err.Error())
		return
	}
	saved := s.pipeline.ArchivePasswords(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"saved": true, "count": len(saved)})
}

func (s *Server) handleTestTelegram(w http.ResponseWriter, r *http.Request) {
	if s.notifier == nil {
		writeError(w, http.StatusServiceUnavailable, "NOTIFIER_UNAVAILABLE", "notifier not initialized")
		return
	}
	if err := s.notifier.SendTelegramTest(r.Context()); err != nil {
		writeError(w, http.StatusBadGateway, "TELEGRAM_SEND_FAILED", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"sent": true})
}

func (s *Server) handleGetWebhook(w http.ResponseWriter, r *http.Request) {
	cfg := s.notifier.LoadWebhook(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":    cfg.Enabled,
		"url":        cfg.URL,
		"has_secret": cfg.SecretHeader != "",
		"mode":       cfg.Mode,
	})
}

func (s *Server) handlePutWebhook(w http.ResponseWriter, r *http.Request) {
	var req notify.WebhookConfig
	if err := decodeJSON(r, &req, 64*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	existing := s.notifier.LoadWebhook(r.Context())
	if strings.TrimSpace(req.SecretHeader) == "" {
		req.SecretHeader = existing.SecretHeader
	}
	req.URL = strings.TrimSpace(req.URL)
	if req.Mode == "" {
		req.Mode = "result"
	}
	if err := s.store.SetSetting(r.Context(), "notify.webhook", req, true); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to save webhook settings")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"saved": true})
}

func (s *Server) handleBarkPush(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DeviceKey string `json:"device_key"`
		Title     string `json:"title"`
		Body      string `json:"body"`
		Group     string `json:"group"`
	}
	if err := decodeJSON(r, &req, 128*1024); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"code": 400, "message": "invalid json"})
		return
	}
	s.processBark(w, r, req.DeviceKey, bark.Incoming{Title: req.Title, Body: req.Body, Group: req.Group})
}

func (s *Server) handleBarkKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("device_key")
	in := s.extractBarkFields(r, "")
	s.processBark(w, r, key, in)
}

func (s *Server) handleBarkWildcard(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("device_key")
	rest := r.PathValue("rest")
	in := s.extractBarkFields(r, rest)
	s.processBark(w, r, key, in)
}

func (s *Server) extractBarkFields(r *http.Request, rest string) bark.Incoming {
	var in bark.Incoming
	ct := strings.ToLower(r.Header.Get("Content-Type"))
	if strings.Contains(ct, "application/json") {
		var req struct {
			Title string `json:"title"`
			Body  string `json:"body"`
			Group string `json:"group"`
		}
		_ = decodeJSON(r, &req, 128*1024)
		in.Title, in.Body, in.Group = req.Title, req.Body, req.Group
	} else if r.Method == http.MethodPost && strings.Contains(ct, "application/x-www-form-urlencoded") {
		_ = r.ParseForm()
		in.Title = r.FormValue("title")
		in.Body = r.FormValue("body")
		in.Group = r.FormValue("group")
	}
	q := r.URL.Query()
	if in.Title == "" {
		in.Title = q.Get("title")
	}
	if in.Body == "" {
		in.Body = q.Get("body")
	}
	if in.Group == "" {
		in.Group = q.Get("group")
	}
	rest = strings.Trim(rest, "/")
	if rest != "" {
		if idx := strings.IndexByte(rest, '/'); idx >= 0 {
			pTitle, _ := url.PathUnescape(rest[:idx])
			pBody, _ := url.PathUnescape(rest[idx+1:])
			if in.Title == "" {
				in.Title = pTitle
			}
			if in.Body == "" {
				in.Body = pBody
			}
		} else {
			pBody, _ := url.PathUnescape(rest)
			if in.Body == "" {
				in.Body = pBody
			}
		}
	}
	return in
}

func (s *Server) processBark(w http.ResponseWriter, r *http.Request, deviceKey string, in bark.Incoming) {
	deviceKey = strings.TrimSpace(deviceKey)
	tok, err := s.store.VerifyToken(r.Context(), deviceKey)
	if err != nil || tok.Role != "admin" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"code": 401, "message": "invalid or non-admin device_key"})
		return
	}
	if s.bark == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"code": 503, "message": "bark receiver unavailable"})
		return
	}
	// Never persist raw device_key inside message title/body.
	if deviceKey != "" {
		in.Title = strings.ReplaceAll(in.Title, deviceKey, "[令牌已隐藏]")
		in.Body = strings.ReplaceAll(in.Body, deviceKey, "[令牌已隐藏]")
	}
	msg, err := s.bark.Ingest(r.Context(), in)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"code": 500, "message": "failed to queue bark signal"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"code":      200,
		"message":   "success",
		"window_id": msg.WindowID,
	})
}

func (s *Server) handleListBarkMessages(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := s.store.ListBarkMessages(r.Context(), limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list bark messages")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}
