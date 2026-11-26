package bark

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zaiwuli/Ark/internal/store"
)

func TestBarkCoalescingWindowAndPrefixMatching(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	src, err := st.UpsertSource(t.Context(), store.Source{
		Name:       "CloudWatch",
		Kind:       "cloud",
		Enabled:    true,
		BarkPrefix: "/115open/最近接收/影视",
	})
	if err != nil {
		t.Fatal(err)
	}

	var fires atomic.Int32
	var gotTargets []Target
	recv := NewReceiver(st, 60*time.Millisecond, func(_ context.Context, targets []Target) {
		gotTargets = append(gotTargets, targets...)
		fires.Add(1)
	})
	defer recv.Stop()

	m1, err := recv.Ingest(t.Context(), Incoming{
		Title: "离线完成",
		Body:  "路径：/115open/最近接收/影视/2026-11-26/某电影",
		Group: "115",
	})
	if err != nil || m1.MatchedSourceID != src.ID {
		t.Fatalf("expected source match %s, got %+v err=%v", src.ID, m1, err)
	}
	m2, err := recv.Ingest(t.Context(), Incoming{Title: "第二条", Body: "合并到同一窗口"})
	if err != nil || m2.WindowID != m1.WindowID {
		t.Fatalf("expected same window id, got %s vs %s", m1.WindowID, m2.WindowID)
	}

	time.Sleep(120 * time.Millisecond)
	if fires.Load() != 1 {
		t.Fatalf("expected exactly 1 coalesced trigger, got %d", fires.Load())
	}
	if len(gotTargets) != 1 || gotTargets[0].SourceID != src.ID || gotTargets[0].RelativePath != "2026-11-26/某电影" {
		t.Fatalf("expected matched target relative path '2026-11-26/某电影', got %+v", gotTargets)
	}
	msgs, _ := st.ListBarkMessages(t.Context(), 10)
	if len(msgs) != 2 || msgs[0].Status != "dispatched" {
		t.Fatalf("expected bark messages marked dispatched, got %+v", msgs)
	}
}
