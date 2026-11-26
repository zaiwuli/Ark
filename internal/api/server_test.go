package api

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
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
	srv, st := newTestServer(t, true)
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
		t.Fatalf("expected 401 when tokens exist, got %d", rec.Code)
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

	// Admin token accepted -> Bark code 200.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/bark/push",
		bytes.NewBufferString(`{"device_key":"`+adminTok+`","title":"离线完成","body":"路径：/115open/test","group":"115"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("admin bark expected 200, got %d: %s", rec.Code, rec.Body.String())
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
