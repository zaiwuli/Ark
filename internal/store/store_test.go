package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestMigrationsCreateAllCoreTables(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	expected := []string{
		"settings",
		"api_tokens",
		"sources",
		"rules",
		"pipelines",
		"batches",
		"batch_items",
		"task_events",
		"operation_journal",
		"bark_messages",
		"schema_migrations",
	}
	for _, tbl := range expected {
		var name string
		err := s.DB().QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name=?`, tbl).Scan(&name)
		if err != nil || name != tbl {
			t.Fatalf("expected table %q to exist, err=%v", tbl, err)
		}
	}

	// Re-running migration should be idempotent.
	if err := s.migrate(ctx); err != nil {
		t.Fatalf("idempotent migrate failed: %v", err)
	}
}

func TestSecretSettingsEncryptedAtRest(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	type tgCfg struct {
		BotToken string `json:"bot_token"`
		ChatID   string `json:"chat_id"`
	}
	want := tgCfg{BotToken: "123456:SUPER_SECRET_BOT_TOKEN", ChatID: "-1001234567890"}
	if err := s.SetSetting(ctx, "notify.telegram", want, true); err != nil {
		t.Fatalf("SetSetting: %v", err)
	}

	// Verify raw database value does not contain plaintext secret.
	var raw string
	var enc int
	if err := s.DB().QueryRowContext(ctx, `SELECT value_json, encrypted FROM settings WHERE key='notify.telegram'`).Scan(&raw, &enc); err != nil {
		t.Fatalf("query raw: %v", err)
	}
	if enc != 1 {
		t.Fatalf("expected encrypted=1, got %d", enc)
	}
	if raw == "" || raw == want.BotToken {
		t.Fatalf("secret should be encrypted in db")
	}

	var got tgCfg
	found, err := s.GetSetting(ctx, "notify.telegram", &got)
	if err != nil || !found {
		t.Fatalf("GetSetting: found=%v err=%v", found, err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestTokenLifecycle(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()

	tok, plain, err := s.CreateToken(ctx, "Shortcut", "iOS", "admin", nil)
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	if len(plain) < 20 {
		t.Fatalf("unexpected short token: %q", plain)
	}

	verified, err := s.VerifyToken(ctx, plain)
	if err != nil {
		t.Fatalf("VerifyToken: %v", err)
	}
	if verified.ID != tok.ID || verified.Role != "admin" {
		t.Fatalf("unexpected verified token: %+v", verified)
	}

	// Expired token must be rejected.
	past := time.Now().Unix() - 10
	_, expPlain, err := s.CreateToken(ctx, "Expired", "", "readonly", &past)
	if err != nil {
		t.Fatalf("CreateToken expired: %v", err)
	}
	if _, err := s.VerifyToken(ctx, expPlain); err != ErrNotFound {
		t.Fatalf("expected ErrNotFound for expired token, got %v", err)
	}

	if err := s.DeleteToken(ctx, tok.ID); err != nil {
		t.Fatalf("DeleteToken: %v", err)
	}
	if _, err := s.VerifyToken(ctx, plain); err != ErrNotFound {
		t.Fatalf("deleted token should not verify, got %v", err)
	}
}
