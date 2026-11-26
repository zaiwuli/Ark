package notify

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zaiwuli/Ark/internal/store"
)

func TestTelegramNotificationDeliveryAndModeFilter(t *testing.T) {
	var received []map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/bot123:tok/sendMessage") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		received = append(received, body)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()

	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	_ = st.SetSetting(t.Context(), "notify.telegram", TelegramConfig{
		Enabled:  true,
		BotToken: "123:tok",
		ChatID:   "-100999",
		APIBase:  ts.URL,
		Mode:     "result",
	}, true)

	_ = st.RecordEvent(t.Context(), "pl_1", "SUCCEEDED", "complete", "解压完成：demo.zip", true)
	d := NewDispatcher(st)
	if err := d.Flush(t.Context()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(received) != 1 || received[0]["chat_id"] != "-100999" {
		t.Fatalf("expected 1 telegram send, got %+v", received)
	}
	text, _ := received[0]["text"].(string)
	if !strings.Contains(text, "demo.zip") {
		t.Fatalf("unexpected telegram text: %q", text)
	}

	// Second flush should send nothing (marked sent).
	_ = d.Flush(t.Context())
	if len(received) != 1 {
		t.Fatalf("expected no duplicate send, got %d", len(received))
	}
}
