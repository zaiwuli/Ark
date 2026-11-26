package tgbot

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zaiwuli/Ark/internal/metrics"
	"github.com/zaiwuli/Ark/internal/notify"
	"github.com/zaiwuli/Ark/internal/pipeline"
	"github.com/zaiwuli/Ark/internal/store"
)

type apiCall struct {
	Method string
	Body   map[string]any
}

type mockTelegram struct {
	mu      sync.Mutex
	calls   []apiCall
	torrent []byte
	server  *httptest.Server
}

func newMockTelegram(t *testing.T, torrentBytes []byte) *mockTelegram {
	t.Helper()
	m := &mockTelegram{torrent: torrentBytes}
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/file/bot") {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(m.torrent)
			return
		}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		method := ""
		if len(parts) >= 2 {
			method = parts[len(parts)-1]
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		m.mu.Lock()
		m.calls = append(m.calls, apiCall{Method: method, Body: body})
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if method == "getFile" {
			_, _ = w.Write([]byte(`{"ok":true,"result":{"file_path":"documents/sample.torrent"}}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"result":true}`))
	}))
	t.Cleanup(m.server.Close)
	return m
}

func (m *mockTelegram) lastCall() apiCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.calls) == 0 {
		return apiCall{}
	}
	return m.calls[len(m.calls)-1]
}

func (m *mockTelegram) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

func setupTestBot(t *testing.T, torrentBytes []byte) (*Bot, *store.Store, *pipeline.Service, *mockTelegram, notify.TelegramConfig, string) {
	t.Helper()
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	workDir := filepath.Join(dir, "work")
	_ = os.MkdirAll(dataDir, 0o755)
	_ = os.MkdirAll(workDir, 0o755)

	dbPath := filepath.Join(dataDir, "ark.db")
	st, err := store.Open(dbPath, filepath.Join(dataDir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	pipe := pipeline.NewServiceWithOptions(st, pipeline.ServiceOptions{
		Timezone:     "Asia/Shanghai",
		DataDir:      dataDir,
		WorkDir:      workDir,
		StableWindow: time.Second,
	})
	notifier := notify.NewDispatcher(st)
	mc := metrics.NewCollector()
	mock := newMockTelegram(t, torrentBytes)

	cfg := notify.TelegramConfig{
		Enabled:  true,
		BotToken: "12345:secret-token",
		ChatID:   "998877",
		APIBase:  mock.server.URL,
		Mode:     "result",
	}
	_ = st.SetSetting(t.Context(), "notify.telegram", cfg, true)

	bot := New(st, pipe, notifier, mc, dbPath)
	return bot, st, pipe, mock, cfg, workDir
}

func TestUnauthorizedChatStrictlyIgnored(t *testing.T) {
	bot, _, _, mock, cfg, _ := setupTestBot(t, nil)

	bot.ProcessUpdate(t.Context(), cfg, tgUpdate{
		UpdateID: 1,
		Message: &tgMessage{
			MessageID: 10,
			Chat:      tgChat{ID: 111111}, // Not 998877
			Text:      "/menu",
		},
	})
	bot.ProcessUpdate(t.Context(), cfg, tgUpdate{
		UpdateID: 2,
		CallbackQuery: &tgCallbackQuery{
			ID:      "cb1",
			Message: &tgMessage{MessageID: 11, Chat: tgChat{ID: 222222}},
			Data:    "sched:toggle",
		},
	})

	if mock.count() != 0 {
		t.Fatalf("expected 0 API calls for unauthorized chats, got %d", mock.count())
	}
}

func TestCommandsMenuStatusPausePwdRules(t *testing.T) {
	bot, _, pipe, mock, cfg, _ := setupTestBot(t, nil)
	ctx := t.Context()

	// 1. /menu
	bot.ProcessUpdate(ctx, cfg, tgUpdate{
		Message: &tgMessage{Chat: tgChat{ID: 998877}, Text: "/menu"},
	})
	c := mock.lastCall()
	if c.Method != "sendMessage" || !strings.Contains(c.Body["text"].(string), "方舟 · 快捷主控台") {
		t.Fatalf("unexpected /menu response: %+v", c)
	}
	if c.Body["reply_markup"] == nil {
		t.Fatalf("expected inline keyboard on /menu")
	}

	// 2. /status
	bot.ProcessUpdate(ctx, cfg, tgUpdate{
		Message: &tgMessage{Chat: tgChat{ID: 998877}, Text: "/status"},
	})
	if txt := mock.lastCall().Body["text"].(string); !strings.Contains(txt, "进程内存") || !strings.Contains(txt, "本地缓存") {
		t.Fatalf("unexpected /status response: %q", txt)
	}

	// 3. /pause and /resume
	bot.ProcessUpdate(ctx, cfg, tgUpdate{
		Message: &tgMessage{Chat: tgChat{ID: 998877}, Text: "/pause"},
	})
	if !pipe.IsPaused() {
		t.Fatalf("expected scheduler paused after /pause")
	}
	bot.ProcessUpdate(ctx, cfg, tgUpdate{
		Message: &tgMessage{Chat: tgChat{ID: 998877}, Text: "/resume"},
	})
	if pipe.IsPaused() {
		t.Fatalf("expected scheduler resumed after /resume")
	}

	// 4. /pwd appends encrypted passwords
	bot.ProcessUpdate(ctx, cfg, tgUpdate{
		Message: &tgMessage{Chat: tgChat{ID: 998877}, Text: "/pwd pass1 pass2"},
	})
	pwds := pipe.ArchivePasswords(ctx)
	if len(pwds) != 2 || pwds[0] != "pass1" || pwds[1] != "pass2" {
		t.Fatalf("unexpected passwords after /pwd: %+v", pwds)
	}

	// 5. /rules + callback set_orig:delete & set_fail:auto_local
	bot.ProcessUpdate(ctx, cfg, tgUpdate{
		CallbackQuery: &tgCallbackQuery{
			ID:      "cb_orig",
			Message: &tgMessage{MessageID: 55, Chat: tgChat{ID: 998877}},
			Data:    "set_orig:delete",
		},
	})
	bot.ProcessUpdate(ctx, cfg, tgUpdate{
		CallbackQuery: &tgCallbackQuery{
			ID:      "cb_fail",
			Message: &tgMessage{MessageID: 55, Chat: tgChat{ID: 998877}},
			Data:    "set_fail:auto_local",
		},
	})
	rule := pipe.ArchiveRule(ctx)
	if rule.OriginalAction != "delete" || rule.CloudFailureAction != "auto_local" {
		t.Fatalf("expected updated rule, got %+v", rule)
	}
}

func TestDirectMagnetAndTorrentOfflineImport(t *testing.T) {
	sampleTorrent := []byte("d4:infod6:lengthi1024e4:name8:demo.zip12:piece lengthi16384e6:pieces20:01234567890123456789ee")
	bot, st, _, mock, cfg, _ := setupTestBot(t, sampleTorrent)
	ctx := t.Context()

	// Configure minimal 115 settings so ImportText / ImportTorrent accept jobs.
	_ = st.SetSetting(ctx, "p115.config", map[string]any{
		"enabled":          true,
		"cookie":           "UID=1; CID=2; SEID=3",
		"offline_root_cid": "1001",
		"cloud_output_cid": "1002",
	}, true)

	// 1. Direct magnet drop (no slash command)
	bot.ProcessUpdate(ctx, cfg, tgUpdate{
		Message: &tgMessage{
			Chat: tgChat{ID: 998877},
			Text: "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567&dn=SamplePack",
		},
	})
	if txt := mock.lastCall().Body["text"].(string); !strings.Contains(txt, "离线链接已接收") || !strings.Contains(txt, "新入队：<b>1</b>") {
		t.Fatalf("unexpected magnet reply: %q", txt)
	}

	// 2. Direct .torrent document drop
	bot.ProcessUpdate(ctx, cfg, tgUpdate{
		Message: &tgMessage{
			Chat: tgChat{ID: 998877},
			Document: &tgDocument{
				FileID:   "doc_123",
				FileName: "demo.torrent",
				FileSize: int64(len(sampleTorrent)),
			},
		},
	})
	if txt := mock.lastCall().Body["text"].(string); !strings.Contains(txt, "种子已解析并提交离线") || !strings.Contains(txt, "demo.torrent") {
		t.Fatalf("unexpected torrent reply: %q", txt)
	}

	items, err := st.ListPipelines(ctx, "", "", 20)
	if err != nil || len(items) != 2 {
		t.Fatalf("expected 2 offline pipelines queued, got %d (err=%v)", len(items), err)
	}
}

func TestAlertCallbacksConfirmLocalAndReclaimCache(t *testing.T) {
	bot, st, _, mock, cfg, workDir := setupTestBot(t, nil)
	ctx := t.Context()

	// Seed a pipeline waiting for local fallback approval.
	pipeID, created, err := st.CreateCloudArchivePipeline(ctx, "", "", "cloud:f_999", "big_pack.rar", "2026-11-26", 1024, `{"file_id":"f_999","pick_code":"pc1"}`, `{}`)
	if err != nil || !created {
		t.Fatalf("CreateCloudArchivePipeline failed: %v", err)
	}
	p, err := st.GetPipeline(ctx, pipeID)
	if err != nil {
		t.Fatal(err)
	}
	p.State = "wait_local_confirm"
	p.Stage = "local_fallback"
	if err := st.UpdatePipeline(ctx, p); err != nil {
		t.Fatal(err)
	}

	// Click "转本地 7-Zip 解压" inline button on alert message.
	bot.ProcessUpdate(ctx, cfg, tgUpdate{
		CallbackQuery: &tgCallbackQuery{
			ID:      "cb_loc",
			Message: &tgMessage{MessageID: 88, Chat: tgChat{ID: 998877}},
			Data:    "loc:" + p.ID,
		},
	})
	updated, err := st.GetPipeline(ctx, p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.State != "caching" {
		t.Fatalf("expected pipeline transitioned to caching, got %q", updated.State)
	}

	// Place an orphaned file in the download cache directory and reclaim via bot callback.
	cacheDir := filepath.Join(workDir, "归档区", "下载缓存")
	_ = os.MkdirAll(cacheDir, 0o755)
	_ = os.WriteFile(filepath.Join(cacheDir, "orphan.bin"), make([]byte, 4096), 0o644)

	bot.ProcessUpdate(ctx, cfg, tgUpdate{
		CallbackQuery: &tgCallbackQuery{
			ID:      "cb_cache",
			Message: &tgMessage{MessageID: 89, Chat: tgChat{ID: 998877}},
			Data:    "do_clean:cache",
		},
	})
	last := mock.lastCall()
	if last.Method != "editMessageText" || !strings.Contains(last.Body["text"].(string), "本地缓存回收完成") {
		t.Fatalf("unexpected cache reclaim reply: %+v", last)
	}
	if _, err := os.Stat(filepath.Join(cacheDir, "orphan.bin")); !os.IsNotExist(err) {
		t.Fatalf("expected orphan.bin removed by cache reclaim")
	}
}

func TestRegisterCommands(t *testing.T) {
	bot, _, _, mock, cfg, _ := setupTestBot(t, nil)
	if err := bot.registerCommands(t.Context(), cfg); err != nil {
		t.Fatalf("registerCommands failed: %v", err)
	}
	c := mock.lastCall()
	if c.Method != "setMyCommands" {
		t.Fatalf("expected setMyCommands, got %q", c.Method)
	}
	cmds, _ := c.Body["commands"].([]any)
	if len(cmds) < 8 {
		t.Fatalf("expected at least 8 registered commands, got %d", len(cmds))
	}
}
