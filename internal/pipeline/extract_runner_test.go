package pipeline

import (
	"archive/zip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zaiwuli/Ark/internal/cleanup"
	"github.com/zaiwuli/Ark/internal/config"
	"github.com/zaiwuli/Ark/internal/p115"
	"github.com/zaiwuli/Ark/internal/store"
)

func TestLocalWatchAndBuiltinExtractEndToEnd(t *testing.T) {
	dir := t.TempDir()
	workDir := filepath.Join(dir, "work")
	watchDir := config.LocalWatchDir(dir, workDir)
	_ = os.MkdirAll(watchDir, 0o755)

	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Create a valid ZIP in 本地监控/
	zipPath := filepath.Join(watchDir, "release.zip")
	f, _ := os.Create(zipPath)
	zw := zip.NewWriter(f)
	w, _ := zw.Create("release/readme.txt")
	_, _ = w.Write([]byte("hello ark"))
	_ = zw.Close()
	_ = f.Close()

	svc := NewServiceWithOptions(st, ServiceOptions{
		Timezone:     "Asia/Shanghai",
		DataDir:      dir,
		WorkDir:      workDir,
		StableWindow: 10 * time.Millisecond,
	})

	// First scan observes the file (not yet stable).
	n1, _ := svc.ScanLocalNow(t.Context())
	if n1 != 0 {
		t.Fatalf("first scan should only observe, got %d", n1)
	}
	time.Sleep(25 * time.Millisecond)

	// Next ticks adopt, extract into 解压输出/YYYY-MM-DD/release, and archive original into 归档区/原包留存/YYYY-MM-DD/release.zip!
	if err := svc.Tick(t.Context()); err != nil {
		t.Fatalf("tick1: %v", err)
	}
	if err := svc.Tick(t.Context()); err != nil {
		t.Fatalf("tick2: %v", err)
	}

	items, _ := st.ListPipelines(t.Context(), "local_archive", "", 10)
	if len(items) != 1 || items[0].State != "completed" {
		t.Fatalf("expected completed local_archive, got %+v", items)
	}
	today := items[0].TargetDate
	if today == "" {
		t.Fatalf("expected target_date set on local_archive")
	}
	outFile := filepath.Join(config.LocalOutputDir(dir, workDir), today, "release", "readme.txt")
	if data, err := os.ReadFile(outFile); err != nil || string(data) != "hello ark" {
		t.Fatalf("expected date-layered extracted file at %s, err=%v data=%q", outFile, err, string(data))
	}
	archivedOrig := filepath.Join(config.LocalOriginalsDir(dir, workDir), today, "release.zip")
	if _, err := os.Stat(archivedOrig); err != nil {
		t.Fatalf("expected original archive moved to %s: %v", archivedOrig, err)
	}
	if _, err := os.Stat(zipPath); !os.IsNotExist(err) {
		t.Fatalf("expected original removed from watch dir after archiving")
	}
}

func TestCloudExtractEndToEndWithMockAndArchive(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	mock := p115.NewMockClient()
	svc := NewServiceWithOptions(st, ServiceOptions{Timezone: "Asia/Shanghai", DataDir: dir})
	svc.SetPort(mock, P115Settings{
		Enabled:         true,
		Cookie:          "UID=1",
		OfflineRootCID:  "off_root",
		CloudOutputCID:  "out_root",
		CloudArchiveCID: "arch_root",
	})

	id, created, err := st.CreateCloudArchivePipeline(t.Context(), "", "", "cloud:f99:1024", "pack.rar", "2026-11-26", 1024,
		`{"fid":"f99","cid":"day1","pickcode":"pc99","name":"pack.rar","size":1024}`,
		`{"output_cid":"out_root","original_action":"archive"}`)
	if err != nil || !created {
		t.Fatalf("CreateCloudArchivePipeline: err=%v created=%v", err, created)
	}

	for range 5 {
		p, _ := st.GetPipeline(t.Context(), id)
		p.NextRunAt = 0
		_ = st.UpdatePipeline(t.Context(), p)
		if err := svc.Tick(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	final, _ := st.GetPipeline(t.Context(), id)
	if final.State != "completed" {
		t.Fatalf("expected cloud_archive completed, got state=%s err=%s", final.State, final.ErrorMessage)
	}
	if mock.Folders["out_root/2026-11-26"] == "" || mock.Folders["cid_2026-11-26/pack"] == "" {
		t.Fatalf("expected date-layered output folder out_root/2026-11-26/pack, got folders=%+v", mock.Folders)
	}
	if len(mock.Moved) != 1 {
		t.Fatalf("expected original archive moved into 原包留存/2026-11-26, got moved=%v", mock.Moved)
	}
}

func TestCloudExtractAutoRetryOnceThenSucceedsAndExhaustedFallback(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Case 1: 1st PrepareExtract fails -> auto-retries once -> 2nd PrepareExtract succeeds!
	mock := p115.NewMockClient()
	mock.PrepareStatuses = []string{"failed", "success"}
	svc := NewServiceWithOptions(st, ServiceOptions{Timezone: "Asia/Shanghai", DataDir: dir})
	svc.SetPort(mock, P115Settings{
		Enabled:        true,
		Cookie:         "UID=1",
		CloudOutputCID: "out_root",
		CloudFailedCID: "fail_root",
	})

	id1, _, _ := st.CreateCloudArchivePipeline(t.Context(), "", "", "cloud:f1:100", "retry_ok.zip", "2026-11-26", 100,
		`{"fid":"f1","cid":"c1","pickcode":"pc1","name":"retry_ok.zip","size":100}`, `{}`)

	// Tick 1: prepare fails -> schedules retry (cloud_prepare with cloud_retries=1)
	_ = svc.Tick(t.Context())
	p1, _ := st.GetPipeline(t.Context(), id1)
	if p1.State != "cloud_prepare" || p1.NextRunAt <= time.Now().Unix() {
		t.Fatalf("expected scheduled auto-retry in cloud_prepare, got state=%s next=%d", p1.State, p1.NextRunAt)
	}

	// Advance past retry delay and step to completion
	for range 5 {
		p, _ := st.GetPipeline(t.Context(), id1)
		p.NextRunAt = 0
		_ = st.UpdatePipeline(t.Context(), p)
		_ = svc.Tick(t.Context())
	}
	p1, _ = st.GetPipeline(t.Context(), id1)
	if p1.State != "completed" {
		t.Fatalf("expected auto-retry to succeed, got state=%s err=%s", p1.State, p1.ErrorMessage)
	}

	// Case 2: Both attempts fail + rule move_failed -> moves to 归档区/异常待查 and marks failed.
	mock2 := p115.NewMockClient()
	mock2.PrepareStatuses = []string{"failed", "failed"}
	svc.SetPort(mock2, P115Settings{
		Enabled:        true,
		Cookie:         "UID=1",
		CloudOutputCID: "out_root",
		CloudFailedCID: "fail_root",
	})
	_, _ = svc.SaveArchiveRule(t.Context(), ArchiveRuleConfig{
		OriginalAction:     "archive",
		CloudFailureAction: "move_failed",
	})
	id2, _, _ := st.CreateCloudArchivePipeline(t.Context(), "", "", "cloud:f2:100", "bad.rar", "2026-11-26", 100,
		`{"fid":"f2","cid":"c1","pickcode":"pc2","name":"bad.rar","size":100}`, `{}`)
	for range 3 {
		p, _ := st.GetPipeline(t.Context(), id2)
		p.NextRunAt = 0
		_ = st.UpdatePipeline(t.Context(), p)
		_ = svc.Tick(t.Context())
	}
	p2, _ := st.GetPipeline(t.Context(), id2)
	if p2.State != "failed" || len(mock2.Moved) != 1 || mock2.Moved[0] != "f2->fail_root" {
		t.Fatalf("expected failed + moved to fail_root, got state=%s moved=%v", p2.State, mock2.Moved)
	}
}

func TestCloudSourceScanAndSeedCleanupDiscoverHandoff(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	mock := p115.NewMockClient()
	mock.Listings["watch_cid_1"] = []p115.CloudFile{
		{FID: "sub_day_cid", CID: "watch_cid_1", Name: "2026-11-26", IsDir: true},
	}
	mock.Listings["sub_day_cid"] = []p115.CloudFile{
		{FID: "sub_rel_cid", CID: "sub_day_cid", Name: "某电影合集", IsDir: true},
	}
	mock.Listings["sub_rel_cid"] = []p115.CloudFile{
		{FID: "f_p1", CID: "sub_rel_cid", Name: "movie.part1.rar", Pickcode: "pc1", Size: 500},
		{FID: "f_p2", CID: "sub_rel_cid", Name: "movie.part2.rar", Pickcode: "pc2", Size: 500},
		{FID: "f_txt", CID: "sub_rel_cid", Name: "readme.txt", Pickcode: "pc3", Size: 10},
	}
	src, err := st.UpsertSource(t.Context(), store.Source{
		Name:       "CloudMovies",
		Kind:       "cloud",
		Enabled:    true,
		BarkPrefix: "/115/movies",
		ConfigJSON: `{"cid":"watch_cid_1"}`,
	})
	if err != nil {
		t.Fatal(err)
	}

	svc := NewServiceWithOptions(st, ServiceOptions{Timezone: "Asia/Shanghai", DataDir: dir})
	svc.SetPort(mock, P115Settings{Enabled: true, Cookie: "UID=1", OfflineRootCID: "off_root", CloudOutputCID: "out_root"})

	// Targeted Bark trigger walks RelativePath "2026-11-26/某电影合集" and adopts ONLY primary volume movie.part1.rar!
	adopted, err := svc.HandleBarkTrigger(t.Context(), []BarkTarget{{
		SourceID:     src.ID,
		ReportedPath: "/115/movies/2026-11-26/某电影合集",
		RelativePath: "2026-11-26/某电影合集",
	}})
	if err != nil || adopted != 1 {
		t.Fatalf("expected 1 adopted cloud archive via Bark subpath walk, got %d err=%v", adopted, err)
	}

	// Seed cleanup with AutoExtract=true hands off to discover -> adopts archive inside seed dir!
	seedPol := cleanup.SeedPolicy{Enabled: true, Mode: "exclude", Extensions: []string{".txt"}, EmptyDirs: true, AutoExtract: true}
	spBytes, _ := json.Marshal(seedPol)
	_, _ = st.UpsertRule(t.Context(), store.Rule{Name: "Seed", Scope: "seed", Enabled: true, ConfigJSON: string(spBytes)})

	rawTorrent := []byte("d4:infod4:name7:PkgSeed6:pieces20:01234567890123456789ee")
	if _, err := svc.ImportTorrent(t.Context(), "tok1", "idem-seed-ext", rawTorrent, true); err != nil {
		t.Fatal(err)
	}
	_ = svc.Tick(t.Context()) // submit offline

	offList, _ := st.ListPipelines(t.Context(), "offline", "", 10)
	p := offList[0]
	p.NextRunAt = 0
	_ = st.UpdatePipeline(t.Context(), p)

	mock.Snapshots = []p115.OfflineTask{{InfoHash: p.InfoHash, Name: "PkgSeed", Status: "success", TargetID: p.TargetCID}}
	mock.Listings[p.TargetCID] = []p115.CloudFile{{FID: "seed_d", CID: p.TargetCID, Name: "PkgSeed", IsDir: true}}
	mock.Listings["seed_d"] = []p115.CloudFile{
		{FID: "f_ad", CID: "seed_d", Name: "ad.txt", Size: 10},
		{FID: "f_zip", CID: "seed_d", Name: "bonus.zip", Pickcode: "pc_zip", Size: 4096},
	}

	_ = svc.Tick(t.Context()) // poll -> seed_cleanup (deletes ad.txt) -> discover -> adopts bonus.zip!
	cloudArchives, _ := st.ListPipelines(t.Context(), "cloud_archive", "", 10)
	foundBonus := false
	for _, ca := range cloudArchives {
		if ca.Name == "bonus.zip" {
			foundBonus = true
		}
	}
	if !foundBonus {
		t.Fatalf("expected bonus.zip adopted after seed cleanup handoff, got %+v", cloudArchives)
	}
}

func TestDateFolderOrganizerCloudAndLocal(t *testing.T) {
	dir := t.TempDir()
	workDir := filepath.Join(dir, "work")
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Seed local date folders in 解压输出:
	// - 2025-11-12 (non-empty old year -> move into 归档区/历史归档/解压输出/2025年/2025-11/2025-11-12)
	// - 2026-09-01 (empty old month -> delete_empty)
	// - 2026-10-05 (active pipeline date -> skipped!)
	// - 2026-11-20 (current month -> kept in place!)
	// - 手工目录 (non-date -> ignored!)
	outRoot := config.LocalOutputDir(dir, workDir)
	_ = os.MkdirAll(filepath.Join(outRoot, "2025-11-12"), 0o755)
	_ = os.WriteFile(filepath.Join(outRoot, "2025-11-12", "a.txt"), []byte("hi"), 0o644)
	_ = os.MkdirAll(filepath.Join(outRoot, "2026-09-01"), 0o755)
	_ = os.MkdirAll(filepath.Join(outRoot, "2026-10-05"), 0o755)
	_ = os.WriteFile(filepath.Join(outRoot, "2026-10-05", "b.txt"), []byte("busy"), 0o644)
	_ = os.MkdirAll(filepath.Join(outRoot, "2026-11-20"), 0o755)
	_ = os.MkdirAll(filepath.Join(outRoot, "手工目录"), 0o755)

	// Mark 2026-10-05 as active pipeline date.
	_, _ = st.DB().Exec(`INSERT INTO pipelines(id,kind,identity,name,state,stage,target_date,created_at,updated_at)
		VALUES('p_busy','local_archive','id_busy','busy.zip','local_extract','local_extract','2026-10-05',1,1)`)

	// Seed cloud date folders in 解压输出:
	mock := p115.NewMockClient()
	mock.Listings["c_out"] = []p115.CloudFile{
		{FID: "d_old", CID: "c_out", Name: "2025-12-30", IsDir: true},
		{FID: "d_empty", CID: "c_out", Name: "2026-08-10", IsDir: true},
		{FID: "d_cur", CID: "c_out", Name: "2026-11-25", IsDir: true},
	}
	mock.Listings["d_old"] = []p115.CloudFile{{FID: "f1", CID: "d_old", Name: "video.mkv", Size: 99}}
	mock.Listings["d_empty"] = nil

	svc := NewServiceWithOptions(st, ServiceOptions{Timezone: "Asia/Shanghai", DataDir: dir, WorkDir: workDir})
	svc.SetPort(mock, P115Settings{
		Enabled:             true,
		Cookie:              "UID=1",
		CloudRootCID:        "c_root",
		CloudOutputCID:      "c_out",
		CloudArchiveZoneCID: "c_zone",
		CloudHistoryCID:     "c_hist",
	})

	now := time.Date(2026, 11, 26, 12, 0, 0, 0, svc.loc)
	plan, err := svc.buildOrganizePlan(t.Context(), now, false)
	if err != nil {
		t.Fatal(err)
	}
	if plan.MoveCount != 2 || plan.EmptyCount != 2 || plan.SkippedActive != 1 {
		t.Fatalf("unexpected plan counts: move=%d empty=%d skipped=%d actions=%+v",
			plan.MoveCount, plan.EmptyCount, plan.SkippedActive, plan.Actions)
	}

	res, err := svc.runOrganizeAt(t.Context(), now, false, false)
	if err != nil || res.MovedCount != 2 || res.DeletedEmpty != 2 || res.FailedCount != 0 {
		t.Fatalf("unexpected run result: %+v err=%v", res, err)
	}

	// Verify local folded path: 归档区/历史归档/解压输出/2025年/2025-11/2025-11-12/a.txt
	folded := filepath.Join(config.LocalHistoryDir(dir, workDir), "解压输出", "2025年", "2025-11", "2025-11-12", "a.txt")
	if _, err := os.Stat(folded); err != nil {
		t.Fatalf("expected folded local file at %s: %v", folded, err)
	}
	// Active date & current month & manual dir untouched.
	if _, err := os.Stat(filepath.Join(outRoot, "2026-10-05", "b.txt")); err != nil {
		t.Fatalf("active date folder must remain untouched")
	}
	if _, err := os.Stat(filepath.Join(outRoot, "2026-11-20")); err != nil {
		t.Fatalf("current month folder must remain untouched")
	}
	if _, err := os.Stat(filepath.Join(outRoot, "2026-09-01")); !os.IsNotExist(err) {
		t.Fatalf("empty old date folder should have been removed")
	}
	// Verify cloud move & empty delete.
	if len(mock.Moved) != 1 || len(mock.Deleted) != 1 || mock.Deleted[0] != "d_empty" {
		t.Fatalf("expected 1 cloud move and 1 empty delete, got moved=%v deleted=%v", mock.Moved, mock.Deleted)
	}

	// Manual mode archives yesterday (2026-11-25) & earlier, including selected extra cloud source,
	// while leaving today (2026-11-26) and non-date folders untouched.
	extraSrc, err := st.UpsertSource(t.Context(), store.Source{
		Name:       "综合讨论",
		Kind:       "cloud",
		Enabled:    true,
		ConfigJSON: `{"cid":"extra_cid"}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	mock.Listings["extra_cid"] = []p115.CloudFile{
		{FID: "ex_yest", CID: "extra_cid", Name: "2026-11-25", IsDir: true},
		{FID: "ex_today", CID: "extra_cid", Name: "2026-11-26", IsDir: true},
		{FID: "ex_misc", CID: "extra_cid", Name: "固定分类文件夹", IsDir: true},
	}
	mock.Listings["ex_yest"] = []p115.CloudFile{{FID: "f_y1", CID: "ex_yest", Name: "item.zip", Size: 100}}
	mock.Listings["ex_today"] = []p115.CloudFile{{FID: "f_t1", CID: "ex_today", Name: "today.zip", Size: 100}}

	_, err = svc.SaveOrganizerConfig(t.Context(), OrganizerConfig{
		RetentionMode:   "current_month",
		CleanEmpty:      true,
		IncludeCloud:    false,
		IncludeLocal:    false,
		CloudCategories: []string{},
		LocalCategories: []string{},
		ExtraSourceIDs:  []string{extraSrc.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	manPlan, err := svc.buildOrganizePlan(t.Context(), now, true)
	if err != nil {
		t.Fatal(err)
	}
	if manPlan.MoveCount != 1 || len(manPlan.Actions) != 1 || manPlan.Actions[0].Category != "综合讨论" || manPlan.Actions[0].Date != "2026-11-25" {
		t.Fatalf("expected manual plan to archive only 综合讨论/2026-11-25, got %+v", manPlan.Actions)
	}

	// Default scan depth = 1 should NOT descend into subfolders; depth=2 should descend.
	n1, err := svc.ScanCloudSourcesWithOptions(t.Context(), []string{extraSrc.ID}, false, 1)
	if err != nil || n1 != 0 {
		t.Fatalf("depth=1 should not descend into date subfolders, got n=%d err=%v", n1, err)
	}
	n2, err := svc.ScanCloudSourcesWithOptions(t.Context(), []string{extraSrc.ID}, false, 2)
	if err != nil || n2 != 2 {
		t.Fatalf("depth=2 should adopt both subfolder archives, got n=%d err=%v", n2, err)
	}
}
