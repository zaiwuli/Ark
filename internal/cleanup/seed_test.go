package cleanup

import (
	"path/filepath"
	"testing"

	"github.com/zaiwuli/Ark/internal/p115"
	"github.com/zaiwuli/Ark/internal/store"
)

func TestSeedCleanupDryRunAndRealDelete(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	mock := p115.NewMockClient()
	mock.Listings["target_day"] = []p115.CloudFile{
		{FID: "seed_root", CID: "target_day", Name: "MyRelease", IsDir: true},
	}
	mock.Listings["seed_root"] = []p115.CloudFile{
		{FID: "f_movie", CID: "seed_root", Name: "movie.mkv", Size: 2_000_000_000},
		{FID: "f_ad", CID: "seed_root", Name: "promo.txt", Size: 512},
		{FID: "d_sub", CID: "seed_root", Name: "Extras", IsDir: true},
	}
	mock.Listings["d_sub"] = []p115.CloudFile{
		{FID: "f_nfo", CID: "d_sub", Name: "readme.nfo", Size: 100},
	}

	eng := NewEngine(st, mock)
	pipe := store.Pipeline{ID: "pl_1", Name: "MyRelease", TargetCID: "target_day"}

	// 1. Dry-run preview must NOT delete anything.
	pol := SeedPolicy{
		Enabled:    true,
		Mode:       "exclude",
		Extensions: []string{"txt", ".nfo"},
		EmptyDirs:  true,
		DryRun:     true,
	}
	rep, err := eng.Run(t.Context(), pipe, pol, "offline_root")
	if err != nil {
		t.Fatalf("dry-run failed: %v", err)
	}
	if rep.Result != "PREVIEW" || rep.Matched != 2 || len(mock.Deleted) != 0 {
		t.Fatalf("unexpected dry-run report=%+v deleted=%v", rep, mock.Deleted)
	}

	// 2. Real deletion removes matched files AND now-empty Extras subfolder.
	pol.DryRun = false
	rep, err = eng.Run(t.Context(), pipe, pol, "offline_root")
	if err != nil {
		t.Fatalf("real cleanup failed: %v", err)
	}
	if rep.Result != "SUCCEEDED" || len(rep.DeletedFiles) != 2 || len(rep.DeletedDirectories) != 1 {
		t.Fatalf("unexpected real cleanup report: %+v", rep)
	}
}
