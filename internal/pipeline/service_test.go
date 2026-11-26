package pipeline

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/zaiwuli/Ark/internal/cleanup"
	"github.com/zaiwuli/Ark/internal/p115"
	"github.com/zaiwuli/Ark/internal/store"
)

func TestOfflineTorrentToSeedCleanupEndToEnd(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	mock := p115.NewMockClient()
	svc := NewService(st, "Asia/Shanghai")
	svc.SetPort(mock, P115Settings{Enabled: true, Cookie: "UID=1"})

	// 1. Initialize cloud root -> creates 3 top-level folders + 3 archive-zone subfolders.
	cfg, err := svc.InitCloudRoot(t.Context(), "root_100")
	if err != nil {
		t.Fatalf("InitCloudRoot: %v", err)
	}
	if cfg.OfflineRootCID == "" || cfg.CloudOutputCID == "" || cfg.CloudArchiveZoneCID == "" ||
		cfg.CloudArchiveCID == "" || cfg.CloudFailedCID == "" || cfg.CloudHistoryCID == "" {
		t.Fatalf("expected all 3-folder + archive-zone folders, got %+v", cfg)
	}

	// 2. Configure seed cleanup rule (real delete of .txt files).
	seedPol := cleanup.SeedPolicy{
		Enabled:    true,
		Mode:       "exclude",
		Extensions: []string{".txt"},
		EmptyDirs:  true,
		DryRun:     false,
	}
	polBytes, _ := json.Marshal(seedPol)
	if _, err := st.UpsertRule(t.Context(), store.Rule{
		Name:       "Default Seed Cleanup",
		Scope:      "seed",
		Enabled:    true,
		ConfigJSON: string(polBytes),
	}); err != nil {
		t.Fatal(err)
	}

	// 3. Import a .torrent file locally -> converts to magnet & snapshots seed rule.
	infoDict := `d4:name9:MyRelease6:pieces20:01234567890123456789e`
	rawTorrent := []byte("d4:info" + infoDict + "e")
	res, err := svc.ImportTorrent(t.Context(), "tok_1", "idem-1", rawTorrent, true)
	if err != nil || res.Queued != 1 {
		t.Fatalf("ImportTorrent: res=%+v err=%v", res, err)
	}

	// Idempotent replay with same key & content returns exact same batch.
	replay, err := svc.ImportTorrent(t.Context(), "tok_1", "idem-1", rawTorrent, true)
	if err != nil || replay.BatchID != res.BatchID || !replay.Replayed {
		t.Fatalf("expected idempotent replay, got %+v err=%v", replay, err)
	}

	// Same key with different auto_extract triggers conflict.
	if _, err := svc.ImportTorrent(t.Context(), "tok_1", "idem-1", rawTorrent, false); err != store.ErrIdempotencyConflict {
		t.Fatalf("expected ErrIdempotencyConflict, got %v", err)
	}

	// 4. First Tick submits the magnet to 115.
	if err := svc.Tick(t.Context()); err != nil {
		t.Fatalf("tick1 submit: %v", err)
	}
	if len(mock.Submissions) != 1 || len(mock.Submissions[0]) != 1 {
		t.Fatalf("expected 1 submission, got %+v", mock.Submissions)
	}

	// 5. Simulate 115 offline completion + seed folder files on cloud.
	pipes, _ := st.ListPipelines(t.Context(), "offline", "", 10)
	if len(pipes) != 1 || pipes[0].State != "submitted" {
		t.Fatalf("expected submitted pipeline, got %+v", pipes)
	}
	p := pipes[0]
	p.NextRunAt = 0
	_ = st.UpdatePipeline(t.Context(), p)

	mock.Snapshots = []p115.OfflineTask{
		{InfoHash: p.InfoHash, Name: "MyRelease", Status: "success", TargetID: p.TargetCID},
	}
	mock.Listings[p.TargetCID] = []p115.CloudFile{
		{FID: "seed_dir_1", CID: p.TargetCID, Name: "MyRelease", IsDir: true},
	}
	mock.Listings["seed_dir_1"] = []p115.CloudFile{
		{FID: "f_vid", CID: "seed_dir_1", Name: "video.mkv", Size: 100000},
		{FID: "f_ad", CID: "seed_dir_1", Name: "ad.txt", Size: 100},
	}

	// 6. Second Tick polls offline success AND runs seed cleanup in sequence -> completed!
	if err := svc.Tick(t.Context()); err != nil {
		t.Fatalf("tick2 poll+cleanup: %v", err)
	}
	final, err := st.GetPipeline(t.Context(), p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != "completed" || len(mock.Deleted) != 1 || mock.Deleted[0] != "f_ad" {
		t.Fatalf("expected completed with f_ad deleted; state=%s deleted=%v output=%s", final.State, mock.Deleted, final.OutputJSON)
	}
}

func TestSaveP115ConfigPreservesEnabledOnEmptyCookieUpdate(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	svc := NewService(st, "Asia/Shanghai")
	// 1. Save cookie initially -> enabled becomes true and client is created.
	s1, err := svc.SaveP115Config(t.Context(), P115Settings{Cookie: "UID=123_A1_1; CID=abc; SEID=xyz"})
	if err != nil || !s1.Enabled {
		t.Fatalf("first save failed: %+v err=%v", s1, err)
	}
	// 2. Save again without re-pasting cookie (e.g. toggling life monitor or clicking Save again).
	s2, err := svc.SaveP115Config(t.Context(), P115Settings{LifeMonitorEnabled: true})
	if err != nil || !s2.Enabled || s2.Cookie == "" {
		t.Fatalf("second save wiped enabled/cookie: %+v err=%v", s2, err)
	}
	cloud, _ := svc.snapshotCloud()
	if cloud == nil {
		t.Fatalf("expected active 115 client preserved after second save with empty cookie")
	}
}
