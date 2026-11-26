package links

import (
	"encoding/base64"
	"net/url"
	"testing"
)

func TestExtractPlainURLEncodedAndBase64(t *testing.T) {
	sample := "ed2k://|file|www.98T.la@分手当晚我拿下了前女友妈妈 (1).mp4|26384024|B61D6B8B85DF04F33B9578E00625A45A|/?tag=Avdb\n" +
		"ed2k://|file|100%测试.zip|8169716|89B8751E9774FF14FFF9D191F790F6FF|/"

	// 1. Plain text (including literal '%' in ed2k filename)
	got, errs, err := Extract(sample)
	if err != nil || len(errs) != 0 || len(got) != 2 {
		t.Fatalf("plain extract failed: got=%d errs=%v err=%v", len(got), errs, err)
	}

	// 2. URL-encoded text (e.g. iOS Shortcuts "URL Encode" step before POST)
	urlEncoded := url.PathEscape(sample)
	got2, errs2, err2 := Extract(urlEncoded)
	if err2 != nil || len(errs2) != 0 || len(got2) != 2 {
		t.Fatalf("url-encoded extract failed: got=%d errs=%v err=%v", len(got2), errs2, err2)
	}

	// 3. Base64-encoded text (e.g. iOS Shortcuts passing .txt file in JSON)
	b64 := base64.StdEncoding.EncodeToString([]byte(sample))
	got3, errs3, err3 := Extract(b64)
	if err3 != nil || len(errs3) != 0 || len(got3) != 2 {
		t.Fatalf("base64 extract failed: got=%d errs=%v err=%v", len(got3), errs3, err3)
	}
}
