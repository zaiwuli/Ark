package pipeline

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zaiwuli/Ark/internal/p115"
	"github.com/zaiwuli/Ark/internal/store"
)

func TestOrganizeCenterCustomTargetsScanAndBatchExec(t *testing.T) {
	dir := t.TempDir()
	workDir := filepath.Join(dir, "work")
	localCustom := filepath.Join(dir, "my_local_folder")
	_ = os.MkdirAll(localCustom, 0o755)
	_ = os.WriteFile(filepath.Join(localCustom, "a.mp4"), []byte("same-video-bytes"), 0o644)
	_ = os.WriteFile(filepath.Join(localCustom, "a_copy.mp4"), []byte("same-video-bytes"), 0o644)
	_ = os.WriteFile(filepath.Join(localCustom, "ad.url"), []byte("spam"), 0o644)

	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	mock := p115.NewMockClient()
	mock.Listings["custom_cid_1"] = []p115.CloudFile{
		{FID: "d_old", CID: "custom_cid_1", Name: "2026-10-01", IsDir: true},
		{FID: "f_v1", CID: "custom_cid_1", Name: "movie.mp4", Size: 10000000, SHA1: "aaa111", ModTime: 1790000000},
		{FID: "f_v2", CID: "custom_cid_1", Name: "movie_dup.mp4", Size: 10000000, SHA1: "aaa111", ModTime: 1790000001},
		{FID: "f_ad", CID: "custom_cid_1", Name: "宣传.txt", Size: 200, SHA1: "bbb222"},
	}

	svc := NewServiceWithOptions(st, ServiceOptions{Timezone: "Asia/Shanghai", DataDir: dir, WorkDir: workDir})
	svc.SetPort(mock, P115Settings{
		Enabled:             true,
		Cookie:              "UID=1",
		CloudRootCID:        "c_root",
		CloudArchiveZoneCID: "c_zone",
		CloudHistoryCID:     "c_hist",
	})

	// 1. Save custom cloud & local targets
	ctCloud, err := svc.UpsertOrganizeTarget(t.Context(), OrganizeTarget{
		Name: "影视收藏", Kind: "cloud", Target: "custom_cid_1", Depth: 1,
	})
	if err != nil || ctCloud.ID == "" {
		t.Fatalf("UpsertOrganizeTarget cloud failed: %v", err)
	}
	ctLocal, err := svc.UpsertOrganizeTarget(t.Context(), OrganizeTarget{
		Name: "本地杂物", Kind: "local", Target: localCustom, Depth: 1,
	})
	if err != nil || ctLocal.ID == "" {
		t.Fatalf("UpsertOrganizeTarget local failed: %v", err)
	}

	// 2. Async 5-in-1 scan
	if _, err := svc.StartCenterScan(CenterScanRequest{
		TargetKeys:        []string{"custom:" + ctCloud.ID, "custom:" + ctLocal.ID},
		EnableDateArchive: true,
		EnableDuplicate:   true,
		EnableJunk:        true,
		JunkExts:          ".txt,.url",
		EnableClassify:    true,
		ClassifyMode:      "type",
		EnableEmptyDir:    false,
	}); err != nil {
		t.Fatalf("StartCenterScan: %v", err)
	}

	var stSnap CenterJobState
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stSnap = svc.CenterJobState()
		if stSnap.Status != "running" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if stSnap.Status != "done" || stSnap.Plan == nil {
		t.Fatalf("expected scan done with plan, got %+v", stSnap)
	}
	if stSnap.Plan.Counts["date_archive"] != 1 || stSnap.Plan.Counts["duplicate"] != 2 || stSnap.Plan.Counts["junk"] != 2 {
		t.Fatalf("unexpected scan counts: %+v items=%+v", stSnap.Plan.Counts, stSnap.Plan.Items)
	}

	// 3. Async batch execution of all scanned items (except keep classify test clean: execute date_archive + duplicate + junk)
	var execIDs []string
	for _, it := range stSnap.Plan.Items {
		if it.Tool == "date_archive" || it.Tool == "duplicate" || it.Tool == "junk" {
			execIDs = append(execIDs, it.ID)
		}
	}
	if _, err := svc.StartCenterExec(execIDs); err != nil {
		t.Fatalf("StartCenterExec: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stSnap = svc.CenterJobState()
		if stSnap.Status != "running" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if stSnap.Status != "done" || stSnap.Result == nil || stSnap.Result.FailedCount != 0 {
		t.Fatalf("expected exec done without failures, got %+v", stSnap)
	}
	if stSnap.Result.MovedCount != 1 || stSnap.Result.DeletedCount != 4 {
		t.Fatalf("expected 1 moved + 4 deleted, got %+v", stSnap.Result)
	}
	if len(mock.Moved) != 1 || len(mock.Deleted) != 2 {
		t.Fatalf("expected 1 cloud batch move + 2 cloud batch deletes, got moved=%v deleted=%v", mock.Moved, mock.Deleted)
	}
}
