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

	// 4. When both EnableDateArchive and EnableEmptyDir are checked, an empty old date folder goes to empty_dir (delete) instead of date_archive.
	mock.Listings["custom_cid_1"] = []p115.CloudFile{
		{FID: "d_empty_date", CID: "custom_cid_1", Name: "2026-10-02", IsDir: true},
	}
	mock.Listings["d_empty_date"] = []p115.CloudFile{}
	if _, err := svc.StartCenterScan(CenterScanRequest{
		TargetKeys:        []string{"custom:" + ctCloud.ID},
		EnableDateArchive: true,
		EnableEmptyDir:    true,
	}); err != nil {
		t.Fatalf("StartCenterScan empty date: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stSnap = svc.CenterJobState()
		if stSnap.Status != "running" {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if stSnap.Status != "done" || stSnap.Plan == nil || stSnap.Plan.Counts["empty_dir"] != 1 || stSnap.Plan.Counts["date_archive"] != 0 {
		t.Fatalf("expected empty old date dir routed to empty_dir, got counts=%+v", stSnap.Plan.Counts)
	}
}

func TestWorkbenchRenameUndoKeywordsSHA1BlacklistAndFlatten(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	mock := p115.NewMockClient()
	mock.Listings["wb_cid"] = []p115.CloudFile{
		// Dirty watermarked video (50 MB -> must NOT be deleted by keyword guard < 20 MB, SHOULD be renamed!)
		{FID: "f_big", CID: "wb_cid", Name: "www.98T.la@【自转】福利合集.mp4", Size: 50 * 1024 * 1024, SHA1: "sha_big"},
		// Small ad image (1 MB -> matches keyword "宣传" & < 20 MB -> junk!)
		{FID: "f_kw_ad", CID: "wb_cid", Name: "扫码宣传图.jpg", Size: 1 * 1024 * 1024, SHA1: "sha_kw_ad"},
		// Blacklisted SHA1 ad video (30 MB -> killed by SHA1 blacklist regardless of size/name!)
		{FID: "f_bl_ad", CID: "wb_cid", Name: "normal_intro.mp4", Size: 30 * 1024 * 1024, SHA1: "deadbeef1234"},
		// Subtitle protected by safe whitelist (must NOT be deleted even with keyword!)
		{FID: "f_sub", CID: "wb_cid", Name: "必看字幕.srt", Size: 2048, SHA1: "sha_sub"},
		// Single-child nested folder -> flatten!
		{FID: "d_wrap", CID: "wb_cid", Name: "套娃壳", IsDir: true},
	}
	mock.Listings["d_wrap"] = []p115.CloudFile{
		{FID: "f_inner", CID: "d_wrap", Name: "正片.mkv", Size: 100 * 1024 * 1024, SHA1: "sha_inner"},
	}

	svc := NewServiceWithOptions(st, ServiceOptions{Timezone: "Asia/Shanghai", DataDir: dir, WorkDir: dir})
	svc.SetPort(mock, P115Settings{Enabled: true, Cookie: "UID=1", CloudRootCID: "c_root"})

	// Persist SHA1 blacklist
	if _, err := svc.AddJunkSHA1(t.Context(), "deadbeef1234"); err != nil {
		t.Fatal(err)
	}
	ct, err := svc.UpsertOrganizeTarget(t.Context(), OrganizeTarget{Name: "测试库", Kind: "cloud", Target: "wb_cid", Depth: 1})
	if err != nil {
		t.Fatal(err)
	}

	cleanAds := true
	kwMax := 20.0
	if _, err := svc.StartCenterScan(CenterScanRequest{
		TargetKeys:       []string{"custom:" + ct.ID},
		EnableRename:     true,
		EnableJunk:       true,
		EnableFlatten:    true,
		RenameCleanAds:   &cleanAds,
		JunkKeywords:     "福利,宣传,必看",
		JunkKeywordMaxMB: &kwMax,
	}); err != nil {
		t.Fatal(err)
	}
	var snap CenterJobState
	for range 200 {
		snap = svc.CenterJobState()
		if snap.Status != "running" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if snap.Status != "done" || snap.Plan == nil {
		t.Fatalf("scan failed: %+v", snap)
	}
	// Expect: junk=2 (f_kw_ad + f_bl_ad; f_big saved by 20MB guard, f_sub saved by .srt whitelist), rename=1 (f_big -> 福利合集.mp4), flatten=1 (d_wrap)
	if snap.Plan.Counts["junk"] != 2 || snap.Plan.Counts["rename"] != 1 || snap.Plan.Counts["flatten"] != 1 {
		t.Fatalf("unexpected counts: %+v items=%+v", snap.Plan.Counts, snap.Plan.Items)
	}

	// Execute all items
	ids := make([]string, 0, len(snap.Plan.Items))
	for _, it := range snap.Plan.Items {
		ids = append(ids, it.ID)
	}
	if _, err := svc.StartCenterExec(ids); err != nil {
		t.Fatal(err)
	}
	for range 200 {
		snap = svc.CenterJobState()
		if snap.Status != "running" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if snap.Status != "done" || snap.Result == nil || snap.Result.RenamedCount != 1 || snap.Result.DeletedCount != 2 || snap.Result.MovedCount != 1 {
		t.Fatalf("unexpected exec result: %+v", snap.Result)
	}
	if snap.UndoRenameCount != 1 {
		t.Fatalf("expected UndoRenameCount=1, got %d", snap.UndoRenameCount)
	}

	// 1-click Undo Rename restores original filename!
	restored, err := svc.UndoLastRename(t.Context())
	if err != nil || restored != 1 {
		t.Fatalf("UndoLastRename failed: restored=%d err=%v", restored, err)
	}
	if svc.CenterJobState().UndoRenameCount != 0 {
		t.Fatalf("expected UndoRenameCount=0 after undo")
	}
	// Auto-blacklist SHA1: deleted f_kw_ad's SHA1 ("sha_kw_ad") must now be persisted!
	wbAfter := svc.WorkbenchConfig(t.Context())
	foundAutoBL := false
	for _, h := range wbAfter.JunkSHA1Blacklist {
		if h == "sha_kw_ad" {
			foundAutoBL = true
			break
		}
	}
	if !foundAutoBL {
		t.Fatalf("expected deleted junk SHA1 'sha_kw_ad' to be auto-blacklisted, got %v", wbAfter.JunkSHA1Blacklist)
	}
}

func TestMultiSelectLocalRenamePlusClassifyAndCloudBFSSinglePass(t *testing.T) {
	tmp := t.TempDir()
	st, err := store.Open(filepath.Join(tmp, "ark.db"), filepath.Join(tmp, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc := NewServiceWithOptions(st, ServiceOptions{Timezone: "Asia/Shanghai", DataDir: tmp, WorkDir: tmp})

	// 1. Local multi-select: rename + classify on the SAME file must both succeed!
	localDir := filepath.Join(tmp, "mix")
	if err := os.MkdirAll(localDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(localDir, "www.98t.la@clip.mp4"), []byte("video"), 0o644); err != nil {
		t.Fatal(err)
	}
	lt, err := svc.UpsertOrganizeTarget(t.Context(), OrganizeTarget{Name: "本地混排", Kind: "local", Target: localDir, Depth: 1})
	if err != nil {
		t.Fatal(err)
	}
	cleanAds := true
	if _, err := svc.StartCenterScan(CenterScanRequest{
		TargetKeys:     []string{"custom:" + lt.ID},
		EnableRename:   true,
		EnableClassify: true,
		RenameCleanAds: &cleanAds,
	}); err != nil {
		t.Fatal(err)
	}
	var snap CenterJobState
	for range 200 {
		snap = svc.CenterJobState()
		if snap.Status != "running" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if snap.Status != "done" || snap.Plan == nil || snap.Plan.Counts["rename"] != 1 || snap.Plan.Counts["classify"] != 1 {
		t.Fatalf("unexpected local multi-select plan: %+v", snap.Plan)
	}
	ids := make([]string, 0, len(snap.Plan.Items))
	for _, it := range snap.Plan.Items {
		ids = append(ids, it.ID)
	}
	if _, err := svc.StartCenterExec(ids); err != nil {
		t.Fatal(err)
	}
	for range 200 {
		snap = svc.CenterJobState()
		if snap.Status != "running" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if snap.Status != "done" || snap.Result == nil || snap.Result.FailedCount != 0 || snap.Result.RenamedCount != 1 || snap.Result.MovedCount != 1 {
		t.Fatalf("expected 0 failures, 1 rename, 1 move; got %+v", snap.Result)
	}
	if _, err := os.Stat(filepath.Join(localDir, "视频", "clip.mp4")); err != nil {
		t.Fatalf("expected renamed+classified file at 视频/clip.mp4: %v", err)
	}

	// 2. Cloud depth=2 BFS: empty_dir + flatten + Option A keyword junk folder (with size guard!)
	mock := p115.NewMockClient()
	mock.Listings["bfs_root"] = []p115.CloudFile{
		{FID: "d_empty", CID: "bfs_root", Name: "空目录", IsDir: true},
		{FID: "d_wrap", CID: "bfs_root", Name: "套娃壳", IsDir: true},
		{FID: "d_ad_dir", CID: "bfs_root", Name: "扫码宣传福利", IsDir: true},
		{FID: "d_big_dir", CID: "bfs_root", Name: "最新福利电影合集", IsDir: true},
	}
	mock.Listings["d_empty"] = []p115.CloudFile{}
	mock.Listings["d_wrap"] = []p115.CloudFile{
		{FID: "f_inner", CID: "d_wrap", Name: "movie.mp4", Size: 100 * 1024 * 1024},
	}
	mock.Listings["d_ad_dir"] = []p115.CloudFile{
		{FID: "f_ad1", CID: "d_ad_dir", Name: "二维码.jpg", Size: 500 * 1024},
	}
	mock.Listings["d_big_dir"] = []p115.CloudFile{
		{FID: "f_real_mov", CID: "d_big_dir", Name: "正片.mp4", Size: 200 * 1024 * 1024},
		{FID: "f_real_mov2", CID: "d_big_dir", Name: "正片2.mp4", Size: 150 * 1024 * 1024},
	}
	svc.SetPort(mock, P115Settings{Enabled: true, Cookie: "UID=1", CloudRootCID: "root_cid"})
	ct, err := svc.UpsertOrganizeTarget(t.Context(), OrganizeTarget{Name: "深扫测试", Kind: "cloud", Target: "bfs_root", Depth: 2})
	if err != nil {
		t.Fatal(err)
	}
	kwMax := 20.0
	if _, err := svc.StartCenterScan(CenterScanRequest{
		TargetKeys:       []string{"custom:" + ct.ID},
		EnableEmptyDir:   true,
		EnableFlatten:    true,
		EnableJunk:       true,
		JunkKeywords:     "福利,宣传",
		JunkKeywordMaxMB: &kwMax,
	}); err != nil {
		t.Fatal(err)
	}
	for range 200 {
		snap = svc.CenterJobState()
		if snap.Status != "running" {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	// Expect: empty_dir=1 (d_empty), flatten=1 (d_wrap), junk=1 (d_ad_dir ONLY; d_big_dir protected by 20MB size guard!)
	if snap.Status != "done" || snap.Plan == nil || snap.Plan.Counts["empty_dir"] != 1 || snap.Plan.Counts["flatten"] != 1 || snap.Plan.Counts["junk"] != 1 {
		t.Fatalf("unexpected cloud BFS plan: %+v items=%+v", snap.Plan.Counts, snap.Plan.Items)
	}
}
