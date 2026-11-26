package torrent

import (
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/zaiwuli/Ark/internal/links"
)

func TestParseTorrentV1AndV2Rejection(t *testing.T) {
	infoDict := `d4:name8:test.rar6:pieces20:01234567890123456789e`
	raw := []byte("d8:announce15:http://t.local/4:info" + infoDict + "e")

	got, err := ParseToMagnet(raw)
	if err != nil {
		t.Fatalf("ParseToMagnet: %v", err)
	}
	wantSum := sha1.Sum([]byte(infoDict))
	wantHash := hex.EncodeToString(wantSum[:])
	if got.InfoHash != wantHash {
		t.Fatalf("hash=%s want=%s", got.InfoHash, wantHash)
	}
	if got.Identity != "btih:"+wantHash || got.Name != "test.rar" {
		t.Fatalf("unexpected link: %+v", got)
	}
	if !strings.HasPrefix(got.Original, "magnet:?xt=urn:btih:"+wantHash) {
		t.Fatalf("unexpected magnet: %s", got.Original)
	}

	// Pure BT v2 (meta version without pieces) must be rejected.
	v2 := []byte("d4:infod12:meta versioni2e4:name4:v2.0ee")
	if _, err := ParseToMagnet(v2); err != ErrV2Unsupported {
		t.Fatalf("expected ErrV2Unsupported, got %v", err)
	}

	// Empty or oversized rejected.
	if _, err := ParseToMagnet(nil); err != ErrSizeLimit {
		t.Fatalf("expected ErrSizeLimit, got %v", err)
	}
}

func TestLinkExtractionAndNormalization(t *testing.T) {
	text := `
	magnet:?xt=urn:btih:0123456789ABCDEF0123456789ABCDEF01234567&dn=Sample+Pack
	ed2k://|file|Movie.mkv|1048576|0123456789abcdef0123456789abcdef|/
	https://Example.COM:443/dl/File.zip?sig=AbC
	`
	got, errs, err := links.Extract(text)
	if err != nil || len(errs) != 0 || len(got) != 3 {
		t.Fatalf("got=%d errs=%v err=%v", len(got), errs, err)
	}
	if got[0].Identity != "btih:0123456789abcdef0123456789abcdef01234567" {
		t.Fatalf("magnet identity mismatch: %+v", got[0])
	}
	if got[1].Identity != "ed2k:0123456789abcdef0123456789abcdef:1048576" || got[1].Size != 1048576 {
		t.Fatalf("ed2k mismatch: %+v", got[1])
	}
	if !strings.HasPrefix(got[2].Identity, "url:") || got[2].Name != "File.zip" {
		t.Fatalf("url mismatch: %+v", got[2])
	}
}
