package api

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zaiwuli/Ark/internal/bark"
	"github.com/zaiwuli/Ark/internal/config"
	"github.com/zaiwuli/Ark/internal/metrics"
	"github.com/zaiwuli/Ark/internal/notify"
	"github.com/zaiwuli/Ark/internal/pipeline"
	"github.com/zaiwuli/Ark/internal/store"
)

func newTestServer(t *testing.T, noAuth bool) (*Server, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := config.Config{
		ListenAddr: ":5656",
		DataDir:    dir,
		Timezone:   "Asia/Shanghai",
		NoAuth:     noAuth,
	}
	mc := metrics.NewCollector()
	mc.MarkStartup("test_ready")
	pipeSvc := pipeline.NewServiceWithOptions(st, pipeline.ServiceOptions{Timezone: cfg.Timezone, DataDir: dir})
	notifier := notify.NewDispatcher(st)
	barkRecv := bark.NewReceiver(st, 50*time.Millisecond, nil)
	t.Cleanup(barkRecv.Stop)
	return NewServer(cfg, st, mc, pipeSvc, notifier, barkRecv), st
}

func TestHealthAndStatusAndMetrics(t *testing.T) {
	srv, _ := newTestServer(t, true)
	h := srv.Handler()

	for _, path := range []string{"/health/live", "/health/ready", "/api/status", "/api/v1/system/metrics", "/"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s -> %d (%s)", path, rec.Code, rec.Body.String())
		}
	}
}

func TestTokenAuthAndScopeEnforcement(t *testing.T) {
	srv, st := newTestServer(t, false)
	h := srv.Handler()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/status", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}

	_, roPlain, err := st.CreateToken(t.Context(), "RO", "", "readonly", nil)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/status", nil)
	req.Header.Set("Authorization", "Bearer "+roPlain)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("readonly status expected 200, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewBufferString(`{"name":"x","role":"admin"}`))
	req.Header.Set("Authorization", "Bearer "+roPlain)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("readonly create token expected 403, got %d", rec.Code)
	}

	_, adminPlain, err := st.CreateToken(t.Context(), "Admin", "", "admin", nil)
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tokens", bytes.NewBufferString(`{"name":"Bot","role":"readonly"}`))
	req.Header.Set("Authorization", "Bearer "+adminPlain)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("admin create token expected 201, got %d: %s", rec.Code, rec.Body.String())
	}

	var created struct {
		Item  store.APIToken `json:"item"`
		Token string         `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if created.Token == "" || created.Item.ID == "" {
		t.Fatalf("unexpected create response: %+v", created)
	}
}

func TestTelegramSettingsRoundTripMasksSecret(t *testing.T) {
	srv, _ := newTestServer(t, true)
	h := srv.Handler()

	body := `{"enabled":true,"bot_token":"999:secret","chat_id":"12345","api_base":"https://tg.example.com/","proxy_url":"socks5://127.0.0.1:1080","mode":"result"}`
	req := httptest.NewRequest(http.MethodPut, "/api/v1/settings/telegram", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT telegram expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/settings/telegram", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET telegram expected 200, got %d", rec.Code)
	}
	if bytes.Contains(rec.Body.Bytes(), []byte("999:secret")) {
		t.Fatalf("bot token must not leak in GET response: %s", rec.Body.String())
	}
}

func TestOfflineImportAPIIdempotencyTorrentAndBatchIsolation(t *testing.T) {
	// Strict auth mode (NoAuth=false): requires Bearer admin token and enforces batch isolation.
	srv, st := newTestServer(t, false)
	h := srv.Handler()

	_, tokA, err := st.CreateToken(t.Context(), "ClientA", "", "admin", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, tokB, err := st.CreateToken(t.Context(), "ClientB", "", "admin", nil)
	if err != nil {
		t.Fatal(err)
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/offline/import",
		bytes.NewBufferString(`{"text":"magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when auth required, got %d", rec.Code)
	}

	// LAN NoAuth mode (NoAuth=true): Web UI offline import still works even after Bark tokens exist.
	lanSrv, lanSt := newTestServer(t, true)
	_, _, _ = lanSt.CreateToken(t.Context(), "BarkToken", "", "admin", nil)
	lanRec := httptest.NewRecorder()
	lanSrv.Handler().ServeHTTP(lanRec, httptest.NewRequest(http.MethodPost, "/api/v1/offline/import",
		bytes.NewBufferString(`{"text":"magnet:?xt=urn:btih:1111111111111111111111111111111111111111"}`)))
	if lanRec.Code != http.StatusAccepted {
		t.Fatalf("LAN NoAuth offline import should succeed even when Bark tokens exist, got %d: %s", lanRec.Code, lanRec.Body.String())
	}

	payload := `{"text":"magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=Demo","auto_extract":true}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/offline/import", bytes.NewBufferString(payload))
	req.Header.Set("Authorization", "Bearer "+tokA)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "req-001")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}
	var batchResp store.BatchResult
	_ = json.Unmarshal(rec.Body.Bytes(), &batchResp)
	if batchResp.BatchID == "" || batchResp.Queued != 1 {
		t.Fatalf("unexpected batchResp: %+v", batchResp)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/offline/import", bytes.NewBufferString(`{"text":"magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`))
	req2.Header.Set("Authorization", "Bearer "+tokA)
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Idempotency-Key", "req-001")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusConflict {
		t.Fatalf("expected 409 idempotency conflict, got %d: %s", rec2.Code, rec2.Body.String())
	}

	reqGetA := httptest.NewRequest(http.MethodGet, "/api/v1/import/batches/"+batchResp.BatchID, nil)
	reqGetA.Header.Set("Authorization", "Bearer "+tokA)
	recGetA := httptest.NewRecorder()
	h.ServeHTTP(recGetA, reqGetA)
	if recGetA.Code != http.StatusOK {
		t.Fatalf("owner batch query expected 200, got %d: %s", recGetA.Code, recGetA.Body.String())
	}

	reqGetB := httptest.NewRequest(http.MethodGet, "/api/v1/import/batches/"+batchResp.BatchID, nil)
	reqGetB.Header.Set("Authorization", "Bearer "+tokB)
	recGetB := httptest.NewRecorder()
	h.ServeHTTP(recGetB, reqGetB)
	if recGetB.Code != http.StatusNotFound {
		t.Fatalf("other token batch query expected 404, got %d", recGetB.Code)
	}

	var mpBuf bytes.Buffer
	mw := multipart.NewWriter(&mpBuf)
	fw, _ := mw.CreateFormFile("file", "sample.torrent")
	_, _ = fw.Write([]byte(`d4:infod4:name10:sample.mkv6:pieces20:01234567890123456789ee`))
	_ = mw.WriteField("auto_extract", "true")
	_ = mw.Close()

	reqTor := httptest.NewRequest(http.MethodPost, "/api/v1/offline/import", &mpBuf)
	reqTor.Header.Set("Authorization", "Bearer "+tokA)
	reqTor.Header.Set("Content-Type", mw.FormDataContentType())
	recTor := httptest.NewRecorder()
	h.ServeHTTP(recTor, reqTor)
	if recTor.Code != http.StatusAccepted {
		t.Fatalf("torrent upload expected 202, got %d: %s", recTor.Code, recTor.Body.String())
	}

	// Legacy 115tools & iOS Shortcut formats: raw text/plain, form-urlencoded, multipart .txt, and JSON field alias.
	rawReq := httptest.NewRequest(http.MethodPost, "/api/115/offline/import",
		bytes.NewBufferString("magnet:?xt=urn:btih:2222222222222222222222222222222222222222"))
	rawReq.Header.Set("Authorization", "Bearer "+tokA)
	rawReq.Header.Set("Content-Type", "text/plain; charset=utf-8")
	rawRec := httptest.NewRecorder()
	h.ServeHTTP(rawRec, rawReq)
	if rawRec.Code != http.StatusAccepted {
		t.Fatalf("plain text import expected 202, got %d: %s", rawRec.Code, rawRec.Body.String())
	}

	formReq := httptest.NewRequest(http.MethodPost, "/api/v1/offline/import",
		bytes.NewBufferString("text=magnet%3A%3Fxt%3Durn%3Abtih%3A3333333333333333333333333333333333333333"))
	formReq.Header.Set("Authorization", "Bearer "+tokA)
	formReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	formRec := httptest.NewRecorder()
	h.ServeHTTP(formRec, formReq)
	if formRec.Code != http.StatusAccepted {
		t.Fatalf("urlencoded import expected 202, got %d: %s", formRec.Code, formRec.Body.String())
	}

	var txtBuf bytes.Buffer
	txtWriter := multipart.NewWriter(&txtBuf)
	txtPart, _ := txtWriter.CreateFormFile("file", "links.txt")
	_, _ = txtPart.Write([]byte("\xef\xbb\xbfmagnet:?xt=urn:btih:4444444444444444444444444444444444444444\n"))
	_ = txtWriter.Close()
	txtReq := httptest.NewRequest(http.MethodPost, "/api/v1/offline/import", &txtBuf)
	txtReq.Header.Set("Authorization", "Bearer "+tokA)
	txtReq.Header.Set("Content-Type", txtWriter.FormDataContentType())
	txtRec := httptest.NewRecorder()
	h.ServeHTTP(txtRec, txtReq)
	if txtRec.Code != http.StatusAccepted {
		t.Fatalf("multipart txt upload expected 202, got %d: %s", txtRec.Code, txtRec.Body.String())
	}

	aliasReq := httptest.NewRequest(http.MethodPost, "/api/v1/offline/import",
		bytes.NewBufferString(`{"url":"magnet:?xt=urn:btih:5555555555555555555555555555555555555555"}`))
	aliasReq.Header.Set("Authorization", "Bearer "+tokA)
	aliasReq.Header.Set("Content-Type", "application/json")
	aliasRec := httptest.NewRecorder()
	h.ServeHTTP(aliasRec, aliasReq)
	if aliasRec.Code != http.StatusAccepted {
		t.Fatalf("json url alias expected 202, got %d: %s", aliasRec.Code, aliasRec.Body.String())
	}

	// Raw .torrent binary body (iOS Shortcuts "Request Body: File").
	rawTorReq := httptest.NewRequest(http.MethodPost, "/api/v1/offline/import",
		bytes.NewReader([]byte("d4:infod6:lengthi2048e4:name9:raw.7z.0112:piece lengthi16384e6:pieces20:11111111111111111111ee")))
	rawTorReq.Header.Set("Authorization", "Bearer "+tokA)
	rawTorReq.Header.Set("Content-Type", "application/x-bittorrent")
	rawTorRec := httptest.NewRecorder()
	h.ServeHTTP(rawTorRec, rawTorReq)
	if rawTorRec.Code != http.StatusAccepted {
		t.Fatalf("raw torrent body expected 202, got %d: %s", rawTorRec.Code, rawTorRec.Body.String())
	}
}

func TestBarkPushAndPasswordsAndLocalScanAPI(t *testing.T) {
	srv, st := newTestServer(t, true)
	h := srv.Handler()

	_, adminTok, _ := st.CreateToken(t.Context(), "BarkAdmin", "", "admin", nil)
	_, roTok, _ := st.CreateToken(t.Context(), "BarkRO", "", "readonly", nil)

	// Readonly token rejected on /bark/push.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bark/push",
		bytes.NewBufferString(`{"device_key":"`+roTok+`","title":"t","body":"b"}`)))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("readonly bark expected 401, got %d", rec.Code)
	}

	// Admin token accepted -> Bark code 200 (both /bark/push and /bark/{key} and /bark/{key}/{title}/{body with slashes}).
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bark/push",
		bytes.NewBufferString(`{"device_key":"`+adminTok+`","title":"离线完成","body":"路径：/115open/test","group":"115"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("admin bark expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	rec2 := httptest.NewRecorder()
	reqKey := httptest.NewRequest(http.MethodPost, "/bark/"+adminTok, bytes.NewBufferString(`{"title":"完成","body":"路径：/115open/test/sub"}`))
	reqKey.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(rec2, reqKey)
	if rec2.Code != http.StatusOK {
		t.Fatalf("POST /bark/{key} expected 200, got %d: %s", rec2.Code, rec2.Body.String())
	}
	rec3 := httptest.NewRecorder()
	h.ServeHTTP(rec3, httptest.NewRequest(http.MethodGet, "/bark/"+adminTok+"/离线完成/路径：/115open/test/a/b", nil))
	if rec3.Code != http.StatusOK {
		t.Fatalf("GET /bark/{key}/{rest...} with slashes expected 200, got %d: %s", rec3.Code, rec3.Body.String())
	}

	// Verify stored in /api/v1/bark/messages.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/bark/messages", nil))
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte("离线完成")) {
		t.Fatalf("expected bark message in list, got %d: %s", rec.Code, rec.Body.String())
	}

	// Archive password book round-trip.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/api/v1/settings/passwords",
		bytes.NewBufferString(`{"items":["pwd1","pwd2","pwd1"]}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT passwords expected 200, got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/settings/passwords", nil))
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(`"count":2`)) {
		t.Fatalf("GET passwords unexpected: %d %s", rec.Code, rec.Body.String())
	}
}

func TestMaintenanceClearAndCacheReclaim(t *testing.T) {
	srv, st := newTestServer(t, true)
	h := srv.Handler()

	// Seed an orphaned cache file and pipelines.
	cacheSub := filepath.Join(srv.cacheDirs()[0], "pl_old_done")
	if err := os.MkdirAll(cacheSub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cacheSub, "archive.rar"), bytes.Repeat([]byte("A"), 2048), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _ = st.DB().Exec(`INSERT INTO pipelines(id,kind,identity,name,state,stage,created_at,updated_at) VALUES('p_done','offline','id_done','done.zip','completed','complete',1,1)`)
	_, _ = st.DB().Exec(`INSERT INTO pipelines(id,kind,identity,name,state,stage,created_at,updated_at) VALUES('p_run','offline','id_run','run.zip','downloading','offline_poll',1,1)`)
	_, _ = st.DB().Exec(`INSERT INTO pipelines(id,kind,identity,name,state,stage,created_at,updated_at) VALUES('p_cfail','cloud_archive','id_cfail','bad.rar','failed','cloud_prepare',1,1)`)

	// Single-item retry on cloud_archive returns to cloud_prepare (not queued!).
	recRetry := httptest.NewRecorder()
	h.ServeHTTP(recRetry, httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/p_cfail/retry", nil))
	if recRetry.Code != http.StatusOK || !bytes.Contains(recRetry.Body.Bytes(), []byte(`"state":"cloud_prepare"`)) {
		t.Fatalf("cloud_archive single retry unexpected: %d %s", recRetry.Code, recRetry.Body.String())
	}

	// Cancel pauses an active task, and retry resumes it cleanly.
	recCancel := httptest.NewRecorder()
	h.ServeHTTP(recCancel, httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/p_cfail/cancel", nil))
	if recCancel.Code != http.StatusOK || !bytes.Contains(recCancel.Body.Bytes(), []byte(`"state":"cancelled"`)) {
		t.Fatalf("cancel unexpected: %d %s", recCancel.Code, recCancel.Body.String())
	}

	// Actively running cloud_inspect returns 409 without force, and succeeds with force=1.
	_, _ = st.DB().Exec(`INSERT INTO pipelines(id,kind,identity,name,state,stage,created_at,updated_at) VALUES('p_insp','cloud_archive','id_insp','insp.rar','cloud_inspect','cloud_inspect',1,1)`)
	recDel := httptest.NewRecorder()
	h.ServeHTTP(recDel, httptest.NewRequest(http.MethodDelete, "/api/v1/pipelines/p_insp", nil))
	if recDel.Code != http.StatusConflict {
		t.Fatalf("active cloud_inspect delete expected 409, got %d", recDel.Code)
	}
	recForceDel := httptest.NewRecorder()
	h.ServeHTTP(recForceDel, httptest.NewRequest(http.MethodDelete, "/api/v1/pipelines/p_insp?force=1", nil))
	if recForceDel.Code != http.StatusOK {
		t.Fatalf("force delete expected 200, got %d: %s", recForceDel.Code, recForceDel.Body.String())
	}

	// Scheduler pause/resume round-trip.
	recPause := httptest.NewRecorder()
	h.ServeHTTP(recPause, httptest.NewRequest(http.MethodPost, "/api/v1/scheduler/pause", nil))
	if recPause.Code != http.StatusOK || !bytes.Contains(recPause.Body.Bytes(), []byte(`"scheduler_paused":true`)) {
		t.Fatalf("scheduler pause unexpected: %d %s", recPause.Code, recPause.Body.String())
	}
	recResume := httptest.NewRecorder()
	h.ServeHTTP(recResume, httptest.NewRequest(http.MethodPost, "/api/v1/scheduler/resume", nil))
	if recResume.Code != http.StatusOK || !bytes.Contains(recResume.Body.Bytes(), []byte(`"scheduler_paused":false`)) {
		t.Fatalf("scheduler resume unexpected: %d %s", recResume.Code, recResume.Body.String())
	}

	// Archive rule & organizer endpoints round-trip.
	recRule := httptest.NewRecorder()
	h.ServeHTTP(recRule, httptest.NewRequest(http.MethodPut, "/api/v1/settings/archive-rule",
		bytes.NewBufferString(`{"original_action":"archive","cloud_failure_action":"auto_local"}`)))
	if recRule.Code != http.StatusOK || !bytes.Contains(recRule.Body.Bytes(), []byte(`"cloud_failure_action":"auto_local"`)) {
		t.Fatalf("archive-rule PUT unexpected: %d %s", recRule.Code, recRule.Body.String())
	}
	recOrg := httptest.NewRecorder()
	h.ServeHTTP(recOrg, httptest.NewRequest(http.MethodPost, "/api/v1/organizer/preview", nil))
	if recOrg.Code != http.StatusOK {
		t.Fatalf("organizer preview expected 200, got %d: %s", recOrg.Code, recOrg.Body.String())
	}

	// Reclaim cache frees the orphaned 2048-byte file.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/maintenance/reclaim-cache", nil))
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(`"freed_bytes":2048`)) {
		t.Fatalf("reclaim-cache unexpected: %d %s", rec.Code, rec.Body.String())
	}

	// Clear terminal pipelines deletes p_done and cancelled p_cfail, while preserving active p_run.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/clear", bytes.NewBufferString(`{"scope":"terminal"}`)))
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(`"deleted":2`)) {
		t.Fatalf("clear pipelines unexpected: %d %s", rec.Code, rec.Body.String())
	}
	if _, err := st.GetPipeline(t.Context(), "p_run"); err != nil {
		t.Fatalf("active pipeline must not be cleared: %v", err)
	}

	// Clear events & vacuum succeed.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/api/v1/events", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("clear events expected 200, got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/maintenance/vacuum", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("vacuum expected 200, got %d", rec.Code)
	}
}
