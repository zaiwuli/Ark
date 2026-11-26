package extract

import (
	"archive/zip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func createTestZip(t *testing.T, zipPath string, entries map[string]string) {
	t.Helper()
	f, err := os.Create(zipPath)
	if err != nil {
		t.Fatal(err)
	}
	zw := zip.NewWriter(f)
	for name, content := range entries {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(content))
	}
	_ = zw.Close()
	_ = f.Close()
}

func TestBuiltinExtractSquashRootAndSecurityGuards(t *testing.T) {
	dir := t.TempDir()
	eng := NewEngine()

	// 1. Valid nested single-wrapper archive with SquashRoot=true.
	goodZip := filepath.Join(dir, "album.zip")
	createTestZip(t, goodZip, map[string]string{
		"album/album/photo1.jpg": "jpgdata1",
		"album/album/photo2.jpg": "jpgdata2",
	})
	res, err := eng.Extract(t.Context(), Options{
		Source:     goodZip,
		OutputDir:  filepath.Join(dir, "out", "album"),
		SquashRoot: true,
	})
	if err != nil {
		t.Fatalf("Extract failed: %v", err)
	}
	if len(res.Files) != 2 || res.Files[0].Path != "photo1.jpg" {
		t.Fatalf("expected squashed files, got %+v", res.Files)
	}

	// 2. Path traversal archive MUST be rejected and leave no output dir.
	evilZip := filepath.Join(dir, "evil.zip")
	createTestZip(t, evilZip, map[string]string{
		"../escape.txt": "pwn",
	})
	evilOut := filepath.Join(dir, "out", "evil")
	if _, err := eng.Extract(t.Context(), Options{Source: evilZip, OutputDir: evilOut}); err != ErrUnsafePath {
		t.Fatalf("expected ErrUnsafePath, got %v", err)
	}
	if _, err := os.Stat(evilOut); !os.IsNotExist(err) {
		t.Fatalf("evil output dir should not exist")
	}
}

func TestDictParserAndLongNameAndVolumes(t *testing.T) {
	if got := ParseMethodDictMB("LZMA2:26"); got != 64 {
		t.Fatalf("LZMA2:26 = %d, want 64", got)
	}
	if got := ParseMethodDictMB("LZMA2:128m BCJ"); got != 128 {
		t.Fatalf("LZMA2:128m = %d, want 128", got)
	}
	if got := ParseMethodDictMB("LZMA2:24"); got != 16 {
		t.Fatalf("LZMA2:24 = %d, want 16", got)
	}

	longName := strings.Repeat("中", 100) + ".mkv" // 304 bytes UTF-8
	mapped, changed := MapLongName(longName)
	if !changed || len(mapped) > maxFilenameUTF8Bytes || !strings.HasSuffix(mapped, ".mkv") {
		t.Fatalf("unexpected MapLongName: changed=%v len=%d val=%q", changed, len(mapped), mapped)
	}

	// Split volume completeness check.
	vdir := t.TempDir()
	p1 := filepath.Join(vdir, "movie.part1.rar")
	p3 := filepath.Join(vdir, "movie.part3.rar")
	_ = os.WriteFile(p1, []byte("a"), 0o644)
	_ = os.WriteFile(p3, []byte("c"), 0o644)

	st := InspectVolume(p1)
	if !st.IsPrimary || st.Complete {
		t.Fatalf("expected missing part2, got %+v", st)
	}
	_ = os.WriteFile(filepath.Join(vdir, "movie.part2.rar"), []byte("b"), 0o644)
	st2 := InspectVolume(p1)
	if !st2.IsPrimary || !st2.Complete || len(st2.Members) != 3 {
		t.Fatalf("expected complete 3-part volume, got %+v", st2)
	}
	if stPart2 := InspectVolume(filepath.Join(vdir, "movie.part2.rar")); stPart2.IsPrimary {
		t.Fatalf("part2 must not be primary")
	}
}
