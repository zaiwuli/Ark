package p115

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientCIDVerificationAndCircuitBreaker(t *testing.T) {
	calls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch r.URL.Path {
		case "/files":
			cid := r.URL.Query().Get("cid")
			if cid == "999" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"state": true,
					"path":  []map[string]any{{"cid": "0"}},
					"data":  []any{},
				})
				return
			}
			if cid == "401" {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"state": false,
					"errno": "99",
				})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": true,
				"count": 1,
				"path":  []map[string]any{{"cid": "0"}, {"cid": cid}},
				"data":  []map[string]any{{"fid": "f1", "n": "pack.rar", "s": 1024, "pc": "pc1", "sha": "ABC"}},
			})
		}
	}))
	defer ts.Close()

	c, err := NewClient("UID=12345_A1_1; CID=x; SEID=y", NewGovernor(100))
	if err != nil {
		t.Fatal(err)
	}
	c.webAPIBase = ts.URL
	c.minInterval = 0

	page, err := c.Files(t.Context(), "100", 0)
	if err != nil || len(page.Files) != 1 || page.Files[0].SHA1 != "abc" {
		t.Fatalf("unexpected page: %+v err=%v", page, err)
	}
	if _, err := c.Files(t.Context(), "999", 0); err == nil {
		t.Fatalf("expected CID_NOT_VERIFIED")
	}
	if _, err := c.Files(t.Context(), "401", 0); err == nil {
		t.Fatalf("expected AUTH_OR_CHALLENGE_REQUIRED")
	}
	beforeCalls := calls
	if _, err := c.Files(t.Context(), "100", 0); err == nil {
		t.Fatalf("expected REMOTE_CIRCUIT_OPEN")
	}
	if calls != beforeCalls {
		t.Fatalf("circuit breaker should block remote call")
	}
	c.gov.ResetCircuit()
	if _, err := c.Files(t.Context(), "100", 0); err != nil {
		t.Fatalf("after reset expected success, got %v", err)
	}
}

func TestOfflineSnapshotCacheAndBatchSubmit(t *testing.T) {
	listCalls := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Query().Get("ac") {
		case "space":
			_ = json.NewEncoder(w).Encode(map[string]any{"state": true, "sign": "sig123", "time": time.Now().Unix()})
		case "add_task_urls":
			_ = json.NewEncoder(w).Encode(map[string]any{"state": true, "result": []map[string]any{{"state": true}}})
		case "task_lists":
			listCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": true,
				"tasks": []map[string]any{
					{"info_hash": "ABCD", "name": "test.zip", "status": 2, "wp_path_id": "200"},
				},
			})
		}
	}))
	defer ts.Close()

	c, err := NewClient("UID=12345_A1_1", NewGovernor(100))
	if err != nil {
		t.Fatal(err)
	}
	c.lixianBase = ts.URL
	c.minInterval = 0

	if err := c.SubmitOffline(t.Context(), []string{"magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567"}, "200"); err != nil {
		t.Fatalf("SubmitOffline: %v", err)
	}
	s1, err := c.OfflineSnapshot(t.Context())
	if err != nil || len(s1) != 1 || s1[0].Status != "success" {
		t.Fatalf("snapshot1: %+v err=%v", s1, err)
	}
	_, err = c.OfflineSnapshot(t.Context())
	if err != nil || listCalls != 1 {
		t.Fatalf("expected cached snapshot (listCalls=1), got %d err=%v", listCalls, err)
	}
}

func TestClientLargeNumericCIDPrecision(t *testing.T) {
	const bigCID = "2948172635481928374"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return 19-digit CID as an unquoted JSON number (exact 115 upstream behavior).
		_, _ = w.Write([]byte(`{"state":true,"count":0,"path":[{"cid":0},{"cid":2948172635481928374}],"data":[]}`))
	}))
	defer ts.Close()

	c, err := NewClient("UID=12345_A1_1", NewGovernor(100))
	if err != nil {
		t.Fatal(err)
	}
	c.webAPIBase = ts.URL
	c.minInterval = 0

	parts, err := c.DirectoryPath(t.Context(), bigCID)
	if err != nil {
		t.Fatalf("DirectoryPath failed on 19-digit numeric CID: %v", err)
	}
	if len(parts) != 2 || parts[1] != bigCID {
		t.Fatalf("CID precision corrupted: got %v want [%s]", parts, bigCID)
	}
}

func TestClientNatsortFallbackOn20130827(t *testing.T) {
	const targetCID = "3123456789012345975"
	natsortHit := false
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/files" {
			_, _ = w.Write([]byte(`{"state":false,"errNo":20130827,"error":"natural sort"}`))
			return
		}
		if r.URL.Path == "/natsort/files.php" {
			natsortHit = true
			_, _ = w.Write([]byte(`{"state":true,"count":1,"cid":3123456789012345975,"path":[{"cid":0},{"cid":3123456789012345975}],"data":[{"fid":"f1","n":"demo.rar","s":2048,"pc":"pc1"}]}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	c, err := NewClient("UID=12345_A1_1", NewGovernor(100))
	if err != nil {
		t.Fatal(err)
	}
	c.webAPIBase = ts.URL
	c.natsortBase = ts.URL
	c.minInterval = 0

	page, err := c.Files(t.Context(), targetCID, 0)
	if err != nil || !natsortHit || len(page.Files) != 1 || page.Files[0].Name != "demo.rar" {
		t.Fatalf("expected natsort fallback success, hit=%v page=%+v err=%v", natsortHit, page, err)
	}
}

func TestStartExtractUsesExtractListAndAddExtractFile(t *testing.T) {
	var gotFiles, gotDirs []string
	var gotPaths, gotToPID string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/files/extract_list":
			_, _ = w.Write([]byte(`{"state":true,"data":{"list":[{"file_name":"a.mp4","file_category":1},{"file_name":"sub","file_category":0}]}}`))
		case "/files/add_extract_file":
			if r.Method == http.MethodPost {
				_ = r.ParseForm()
				gotFiles = r.PostForm["extract_file[]"]
				gotDirs = r.PostForm["extract_dir[]"]
				gotPaths = r.PostForm.Get("paths")
				gotToPID = r.PostForm.Get("to_pid")
				_, _ = w.Write([]byte(`{"state":true,"data":{"extract_id":"ext_999"}}`))
			} else {
				_, _ = w.Write([]byte(`{"state":true,"data":{"extract_status":1,"percent":100}}`))
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	c, err := NewClient("UID=12345_A1_1", NewGovernor(100))
	if err != nil {
		t.Fatal(err)
	}
	c.webAPIBase = ts.URL
	c.minInterval = 0

	extID, err := c.StartExtract(t.Context(), "pc_demo", "out_cid_1")
	if err != nil || extID != "ext_999" {
		t.Fatalf("StartExtract unexpected: id=%s err=%v", extID, err)
	}
	if gotPaths != "文件" || gotToPID != "out_cid_1" || len(gotFiles) != 1 || gotFiles[0] != "a.mp4" || len(gotDirs) != 1 || gotDirs[0] != "sub" {
		t.Fatalf("unexpected add_extract_file form: paths=%q to_pid=%q files=%v dirs=%v", gotPaths, gotToPID, gotFiles, gotDirs)
	}
	st, err := c.ExtractStatus(t.Context(), extID)
	if err != nil || st != "success" {
		t.Fatalf("ExtractStatus unexpected: st=%s err=%v", st, err)
	}

	// Verify M115Encode produces valid 128-byte RSA PKCS#1 v1.5 block(s) base64-encoded.
	key, err := GenerateM115Key()
	if err != nil {
		t.Fatal(err)
	}
	enc := M115Encode([]byte(`{"pickcode":"pc_demo"}`), key)
	if len(enc) < 160 {
		t.Fatalf("unexpected M115Encode output length: %d", len(enc))
	}
}
