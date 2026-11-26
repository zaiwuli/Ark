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
