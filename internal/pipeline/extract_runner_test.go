package pipeline

import (
	"archive/zip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zaiwuli/Ark/internal/p115"
	"github.com/zaiwuli/Ark/internal/store"
)

func TestLocalWatchAndBuiltinExtractEndToEnd(t *testing.T) {
	dir := t.TempDir()
	workDir := filepath.Join(dir, "work")
	watchDir := filepath.Join(workDir, "watch")
	_ = os.MkdirAll(watchDir, 0o755)

	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	// Create a valid ZIP in watch/
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

	// Second tick adopts AND extracts it!
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
	outFile := filepath.Join(workDir, "output", "release", "readme.txt")
	if data, err := os.ReadFile(outFile); err != nil || string(data) != "hello ark" {
		t.Fatalf("expected extracted file at %s, err=%v data=%q", outFile, err, string(data))
	}
}

func TestCloudExtractEndToEndWithMock(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "ark.db"), filepath.Join(dir, "secret.key"), "")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	mock := p115.NewMockClient()
	svc := NewServiceWithOptions(st, ServiceOptions{Timezone: "Asia/Shanghai", DataDir: dir})
	svc.SetPort(mock, P115Settings{Enabled: true, Cookie: "UID=1", OfflineRootCID: "off_root", CloudOutputCID: "out_root"})

	// Create a cloud_archive pipeline directly and step through prepare -> export -> progress -> completed.
	id, created, err := st.CreateCloudArchivePipeline(t.Context(), "", "", "cloud:f99:1024", "pack.rar", 1024,
		`{"fid":"f99","cid":"day1","pickcode":"pc99","name":"pack.rar","size":1024}`,
		`{"output_cid":"out_root","squash_root":true}`)
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
}
