package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/zaiwuli/Ark/internal/bark"
	"github.com/zaiwuli/Ark/internal/config"
	"github.com/zaiwuli/Ark/internal/metrics"
	"github.com/zaiwuli/Ark/internal/notify"
	"github.com/zaiwuli/Ark/internal/pipeline"
	"github.com/zaiwuli/Ark/internal/store"
	"github.com/zaiwuli/Ark/web"
)

type ctxKey int

const principalKey ctxKey = 1

type Principal struct {
	Authenticated bool   `json:"authenticated"`
	Role          string `json:"role"` // "admin" | "readonly" | "local"
	TokenID       string `json:"token_id,omitempty"`
}

type Server struct {
	cfg      config.Config
	store    *store.Store
	metrics  *metrics.Collector
	pipeline *pipeline.Service
	notifier *notify.Dispatcher
	bark     *bark.Receiver
	mux      *http.ServeMux
}

func NewServer(cfg config.Config, st *store.Store, mc *metrics.Collector, pipe *pipeline.Service, notifier *notify.Dispatcher, barkRecv *bark.Receiver) *Server {
	s := &Server{
		cfg:      cfg,
		store:    st,
		metrics:  mc,
		pipeline: pipe,
		notifier: notifier,
		bark:     barkRecv,
		mux:      http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.securityMiddleware(s.mux)
}

func (s *Server) registerRoutes() {
	// Health probes (always public for Docker HEALTHCHECK)
	s.mux.HandleFunc("GET /health/live", s.handleHealthLive)
	s.mux.HandleFunc("GET /health/ready", s.handleHealthReady)

	// System status & memory diagnostics
	s.mux.HandleFunc("GET /api/status", s.requireRole("readonly", s.handleStatus))
	s.mux.HandleFunc("GET /api/v1/system/metrics", s.requireRole("readonly", s.handleMetrics))

	// Token management (admin)
	s.mux.HandleFunc("GET /api/v1/tokens", s.requireRole("admin", s.handleListTokens))
	s.mux.HandleFunc("POST /api/v1/tokens", s.requireRole("admin", s.handleCreateToken))
	s.mux.HandleFunc("DELETE /api/v1/tokens/{id}", s.requireRole("admin", s.handleDeleteToken))

	// Telegram & Webhook notification settings (admin)
	s.mux.HandleFunc("GET /api/v1/settings/telegram", s.requireRole("admin", s.handleGetTelegram))
	s.mux.HandleFunc("PUT /api/v1/settings/telegram", s.requireRole("admin", s.handlePutTelegram))
	s.mux.HandleFunc("POST /api/v1/settings/telegram/test", s.requireRole("admin", s.handleTestTelegram))
	s.mux.HandleFunc("GET /api/v1/settings/webhook", s.requireRole("admin", s.handleGetWebhook))
	s.mux.HandleFunc("PUT /api/v1/settings/webhook", s.requireRole("admin", s.handlePutWebhook))
	s.mux.HandleFunc("GET /api/v1/settings/passwords", s.requireRole("admin", s.handleGetPasswords))
	s.mux.HandleFunc("PUT /api/v1/settings/passwords", s.requireRole("admin", s.handlePutPasswords))
	s.mux.HandleFunc("GET /api/v1/settings/archive-rule", s.requireRole("admin", s.handleGetArchiveRule))
	s.mux.HandleFunc("PUT /api/v1/settings/archive-rule", s.requireRole("admin", s.handlePutArchiveRule))

	// 115 connection & cloud root management (admin)
	s.mux.HandleFunc("GET /api/v1/settings/p115", s.requireRole("admin", s.handleGetP115))
	s.mux.HandleFunc("PUT /api/v1/settings/p115", s.requireRole("admin", s.handlePutP115))
	s.mux.HandleFunc("POST /api/v1/settings/p115/init-root", s.requireRole("admin", s.handleInitCloudRoot))
	s.mux.HandleFunc("POST /api/v1/monitor/life-check", s.requireRole("admin", s.handleTriggerLifeCheck))
	s.mux.HandleFunc("POST /api/v1/monitor/local-scan", s.requireRole("admin", s.handleTriggerLocalScan))
	s.mux.HandleFunc("POST /api/v1/monitor/cloud-scan", s.requireRole("admin", s.handleTriggerCloudScan))

	// Offline import & token-scoped batch status (compatible with third-party API and legacy 115tools)
	s.mux.HandleFunc("POST /api/v1/offline/import", s.requireImportAuth(s.handleOfflineImport))
	s.mux.HandleFunc("POST /api/115/offline/import", s.requireImportAuth(s.handleOfflineImport))
	s.mux.HandleFunc("GET /api/v1/import/batches/{batch_id}", s.requireRole("readonly", s.handleGetBatch))

	// Bark trigger server endpoints (authenticated via device_key = admin API token)
	s.mux.HandleFunc("POST /bark/push", s.handleBarkPush)
	s.mux.HandleFunc("GET /bark/{device_key}", s.handleBarkKey)
	s.mux.HandleFunc("POST /bark/{device_key}", s.handleBarkKey)
	s.mux.HandleFunc("GET /bark/{device_key}/{rest...}", s.handleBarkWildcard)
	s.mux.HandleFunc("POST /bark/{device_key}/{rest...}", s.handleBarkWildcard)
	s.mux.HandleFunc("GET /api/v1/bark/messages", s.requireRole("admin", s.handleListBarkMessages))

	// Unified pipelines & events
	s.mux.HandleFunc("GET /api/v1/pipelines", s.requireRole("readonly", s.handleListPipelines))
	s.mux.HandleFunc("POST /api/v1/pipelines/clear", s.requireRole("admin", s.handleClearPipelines))
	s.mux.HandleFunc("POST /api/v1/pipelines/retry-failed", s.requireRole("admin", s.handleRetryFailedPipelines))
	s.mux.HandleFunc("POST /api/v1/scheduler/pause", s.requireRole("admin", s.handleSchedulerPause))
	s.mux.HandleFunc("POST /api/v1/scheduler/resume", s.requireRole("admin", s.handleSchedulerResume))
	s.mux.HandleFunc("GET /api/v1/pipelines/{id}", s.requireRole("readonly", s.handleGetPipeline))
	s.mux.HandleFunc("POST /api/v1/pipelines/{id}/retry", s.requireRole("admin", s.handleRetryPipeline))
	s.mux.HandleFunc("POST /api/v1/pipelines/{id}/cancel", s.requireRole("admin", s.handleCancelPipeline))
	s.mux.HandleFunc("POST /api/v1/pipelines/{id}/confirm-local", s.requireRole("admin", s.handleConfirmLocalFallback))
	s.mux.HandleFunc("DELETE /api/v1/pipelines/{id}", s.requireRole("admin", s.handleDeletePipeline))
	s.mux.HandleFunc("GET /api/v1/events", s.requireRole("readonly", s.handleListEvents))
	s.mux.HandleFunc("DELETE /api/v1/events", s.requireRole("admin", s.handleClearEvents))
	s.mux.HandleFunc("DELETE /api/v1/bark/messages", s.requireRole("admin", s.handleClearBarkMessages))

	// Maintenance, cache reclaim & date-folder organizer
	s.mux.HandleFunc("GET /api/v1/maintenance/summary", s.requireRole("readonly", s.handleMaintenanceSummary))
	s.mux.HandleFunc("POST /api/v1/maintenance/reclaim-cache", s.requireRole("admin", s.handleReclaimCache))
	s.mux.HandleFunc("POST /api/v1/maintenance/vacuum", s.requireRole("admin", s.handleVacuumDB))
	s.mux.HandleFunc("GET /api/v1/organizer/config", s.requireRole("admin", s.handleGetOrganizerConfig))
	s.mux.HandleFunc("PUT /api/v1/organizer/config", s.requireRole("admin", s.handlePutOrganizerConfig))
	s.mux.HandleFunc("POST /api/v1/organizer/preview", s.requireRole("admin", s.handlePreviewOrganize))
	s.mux.HandleFunc("POST /api/v1/organizer/run", s.requireRole("admin", s.handleRunOrganize))

	// Sources & Rules
	s.mux.HandleFunc("GET /api/v1/sources", s.requireRole("readonly", s.handleListSources))
	s.mux.HandleFunc("POST /api/v1/sources", s.requireRole("admin", s.handleUpsertSource))
	s.mux.HandleFunc("DELETE /api/v1/sources/{id}", s.requireRole("admin", s.handleDeleteSource))
	s.mux.HandleFunc("GET /api/v1/rules", s.requireRole("readonly", s.handleListRules))
	s.mux.HandleFunc("POST /api/v1/rules", s.requireRole("admin", s.handleUpsertRule))
	s.mux.HandleFunc("DELETE /api/v1/rules/{id}", s.requireRole("admin", s.handleDeleteRule))

	// Embedded lightweight static Web UI
	fileServer := http.FileServer(http.FS(web.DistFS()))
	s.mux.Handle("/", fileServer)
}

func (s *Server) securityMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "SAMEORIGIN")
		w.Header().Set("Referrer-Policy", "no-referrer")

		// Enforce strict cross-origin protection on browser mutations without Bearer token.
		if isMutation(r.Method) && !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			if origin := r.Header.Get("Origin"); origin != "" {
				if !sameHostOrigin(origin, r.Host) {
					writeError(w, http.StatusForbidden, "CROSS_ORIGIN_FORBIDDEN", "cross-origin mutation rejected")
					return
				}
			}
		}

		next.ServeHTTP(w, r)
	})
}

func (s *Server) requireRole(minRole string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, code, msg := s.authenticate(r)
		if code != "" {
			writeError(w, http.StatusUnauthorized, code, msg)
			return
		}
		if minRole == "admin" && p.Role == "readonly" {
			writeError(w, http.StatusForbidden, "INSUFFICIENT_SCOPE", "admin role required")
			return
		}
		ctx := context.WithValue(r.Context(), principalKey, p)
		next(w, r.WithContext(ctx))
	}
}

func (s *Server) authenticate(r *http.Request) (Principal, string, string) {
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if authHeader != "" {
		if !strings.HasPrefix(authHeader, "Bearer ") {
			return Principal{}, "INVALID_TOKEN", "expected Bearer token"
		}
		raw := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
		tok, err := s.store.VerifyToken(r.Context(), raw)
		if err != nil {
			return Principal{}, "INVALID_TOKEN", "token invalid, expired, or deleted"
		}
		return Principal{Authenticated: true, Role: tok.Role, TokenID: tok.ID}, "", ""
	}
	if s.cfg.NoAuth {
		return Principal{Authenticated: false, Role: "local"}, "", ""
	}
	return Principal{}, "LOGIN_REQUIRED", "authentication required"
}

func principalFrom(ctx context.Context) Principal {
	if p, ok := ctx.Value(principalKey).(Principal); ok {
		return p
	}
	return Principal{}
}

func (s *Server) handleHealthLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleHealthReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "DB_UNAVAILABLE", "database ping failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready"})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	dbReady := s.store.Ping(ctx) == nil
	p := principalFrom(r.Context())

	writeJSON(w, http.StatusOK, map[string]any{
		"app":            "Ark",
		"version":        config.Version,
		"commit":         config.Commit,
		"build_time":     config.BuildTime,
		"go_version":       runtime.Version(),
		"uptime_seconds":   s.metrics.UptimeSeconds(),
		"database_ready":   dbReady,
		"scheduler_paused": s.pipeline.IsPaused(),
		"auth": map[string]any{
			"role":          p.Role,
			"authenticated": p.Authenticated,
			"no_auth_mode":  s.cfg.NoAuth,
		},
		"components": map[string]any{
			"seven_zip": probeSevenZip(s.cfg.ExtractBin),
		},
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"memory":         metrics.Snapshot(),
		"startup_memory": s.metrics.StartupStages(),
		"uptime_seconds": s.metrics.UptimeSeconds(),
	})
}

func (s *Server) handleListTokens(w http.ResponseWriter, r *http.Request) {
	items, err := s.store.ListTokens(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to list tokens")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleCreateToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name      string `json:"name"`
		Note      string `json:"note"`
		Role      string `json:"role"`
		ExpiresAt *int64 `json:"expires_at"`
	}
	if err := decodeJSON(r, &req, 64*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	req.Name = strings.TrimSpace(req.Name)
	req.Note = strings.TrimSpace(req.Note)
	if req.Role == "" {
		req.Role = "admin"
	}
	tok, plain, err := s.store.CreateToken(r.Context(), req.Name, req.Note, req.Role, req.ExpiresAt)
	if errors.Is(err, store.ErrInvalid) {
		writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "name required and role must be admin or readonly")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to create token")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"item":  tok,
		"token": plain,
	})
}

func (s *Server) handleDeleteToken(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	err := s.store.DeleteToken(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "TOKEN_NOT_FOUND", "token not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to delete token")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": true})
}

type telegramSetting struct {
	Enabled        bool   `json:"enabled"`
	BotToken       string `json:"bot_token"`
	ChatID         string `json:"chat_id"`
	APIBase        string `json:"api_base"`
	ProxyURL       string `json:"proxy_url"`
	Mode           string `json:"mode"`             // "result" | "errors" | "all"
	BotMenuEnabled *bool  `json:"bot_menu_enabled"` // nil defaults to true
}

func (s *Server) handleGetTelegram(w http.ResponseWriter, r *http.Request) {
	var cur telegramSetting
	_, err := s.store.GetSetting(r.Context(), "notify.telegram", &cur)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to load telegram settings")
		return
	}
	if cur.Mode == "" {
		cur.Mode = "result"
	}
	botMenu := cur.BotMenuEnabled == nil || *cur.BotMenuEnabled
	writeJSON(w, http.StatusOK, map[string]any{
		"enabled":          cur.Enabled,
		"has_bot_token":    cur.BotToken != "",
		"chat_id":          cur.ChatID,
		"api_base":         cur.APIBase,
		"proxy_url":        cur.ProxyURL,
		"mode":             cur.Mode,
		"bot_menu_enabled": botMenu,
	})
}

func (s *Server) handlePutTelegram(w http.ResponseWriter, r *http.Request) {
	var req telegramSetting
	if err := decodeJSON(r, &req, 64*1024); err != nil {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "invalid JSON body")
		return
	}
	var existing telegramSetting
	_, _ = s.store.GetSetting(r.Context(), "notify.telegram", &existing)
	if strings.TrimSpace(req.BotToken) == "" {
		req.BotToken = existing.BotToken
	}
	if req.BotMenuEnabled == nil {
		if existing.BotMenuEnabled != nil {
			v := *existing.BotMenuEnabled
			req.BotMenuEnabled = &v
		} else {
			v := true
			req.BotMenuEnabled = &v
		}
	}
	req.ChatID = strings.TrimSpace(req.ChatID)
	req.APIBase = strings.TrimRight(strings.TrimSpace(req.APIBase), "/")
	req.ProxyURL = strings.TrimSpace(req.ProxyURL)
	if req.Mode == "" {
		req.Mode = "result"
	}
	if req.Mode != "result" && req.Mode != "errors" && req.Mode != "all" {
		writeError(w, http.StatusUnprocessableEntity, "VALIDATION_ERROR", "mode must be result, errors, or all")
		return
	}
	if err := s.store.SetSetting(r.Context(), "notify.telegram", req, true); err != nil {
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "failed to save telegram settings")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"saved":            true,
		"enabled":          req.Enabled,
		"has_bot_token":    req.BotToken != "",
		"chat_id":          req.ChatID,
		"api_base":         req.APIBase,
		"proxy_url":        req.ProxyURL,
		"mode":             req.Mode,
		"bot_menu_enabled": *req.BotMenuEnabled,
	})
}

var sevenZipVerRe = regexp.MustCompile(`7-Zip\s+(?:\([^)]*\)\s+)?(\d+\.\d+)`)

func probeSevenZip(bin string) map[string]any {
	res := map[string]any{
		"name":      "7-Zip",
		"available": false,
		"version":   "",
		"path":      bin,
	}
	if bin == "" {
		return res
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, "i").Output()
	if err != nil {
		return res
	}
	if m := sevenZipVerRe.FindStringSubmatch(string(out)); len(m) == 2 {
		res["available"] = true
		res["version"] = m[1]
	}
	return res
}

func isMutation(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func sameHostOrigin(origin, host string) bool {
	origin = strings.TrimPrefix(strings.TrimPrefix(origin, "https://"), "http://")
	if idx := strings.IndexByte(origin, '/'); idx >= 0 {
		origin = origin[:idx]
	}
	return strings.EqualFold(origin, host)
}

func decodeJSON(r *http.Request, dst any, maxBytes int64) error {
	r.Body = http.MaxBytesReader(nil, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	return dec.Decode(dst)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{
		"error": map[string]string{
			"code":    code,
			"message": message,
		},
	})
}
