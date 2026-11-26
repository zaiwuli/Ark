package links

import (
	"crypto/sha256"
	"encoding/base32"
	"encoding/hex"
	"errors"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const MaxImportLinks = 1000

var (
	ErrNoLinks        = errors.New("NO_LINKS")
	ErrImportLimit    = errors.New("IMPORT_LIMIT_1000")
	ErrInvalidLink    = errors.New("INVALID_LINK")
	ErrInvalidED2K    = errors.New("INVALID_ED2K")
	ErrInvalidMagnet  = errors.New("INVALID_BTIH")
	ErrUnsupportedURL = errors.New("UNSUPPORTED_LINK")
)

var (
	linkPattern = regexp.MustCompile(`(?i)ed2k://\|file\|.*?\|/|magnet:\?[^\s<>"` + "`" + `]+|(?:https?|ftp)://[^\s<>"` + "`" + `]+`)
	hex32Re     = regexp.MustCompile(`^[0-9a-fA-F]{32}$`)
	hex40Re     = regexp.MustCompile(`^[0-9a-fA-F]{40}$`)
	b32Re       = regexp.MustCompile(`^[A-Za-z2-7]{32}$`)
)

type Link struct {
	Original string `json:"original"`
	Identity string `json:"identity"`
	Kind     string `json:"kind"` // magnet | ed2k | http | https | ftp
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	InfoHash string `json:"info_hash"`
}

// MatchName normalizes a filename for case-insensitive matching.
func MatchName(name string) string {
	if decoded, err := url.QueryUnescape(name); err == nil {
		name = decoded
	}
	return strings.ToLower(strings.TrimSpace(name))
}

func Parse(raw string) (Link, error) {
	v := strings.TrimSpace(raw)
	if v == "" || len(v) > 16384 {
		return Link{}, ErrInvalidLink
	}
	for i := 0; i < len(v); i++ {
		if v[i] < 32 {
			return Link{}, ErrInvalidLink
		}
	}

	lower := strings.ToLower(v)
	if strings.HasPrefix(lower, "ed2k://") {
		return parseED2K(v)
	}
	if strings.HasPrefix(lower, "magnet:") {
		return parseMagnet(v)
	}
	return parseWebURL(v)
}

func parseED2K(v string) (Link, error) {
	if !strings.HasSuffix(v, "|/") {
		return Link{}, ErrInvalidED2K
	}
	parts := strings.Split(v, "|")
	if len(parts) < 6 || strings.ToLower(parts[1]) != "file" {
		return Link{}, ErrInvalidED2K
	}
	if !hex32Re.MatchString(parts[4]) {
		return Link{}, ErrInvalidED2K
	}
	size, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || size <= 0 {
		return Link{}, ErrInvalidED2K
	}
	name, err := url.QueryUnescape(parts[2])
	if err != nil || strings.TrimSpace(name) == "" {
		return Link{}, ErrInvalidED2K
	}
	digest := strings.ToLower(parts[4])
	return Link{
		Original: v,
		Identity: "ed2k:" + digest + ":" + strconv.FormatInt(size, 10),
		Kind:     "ed2k",
		Name:     name,
		Size:     size,
		InfoHash: digest,
	}, nil
}

func parseMagnet(v string) (Link, error) {
	u, err := url.Parse(v)
	if err != nil {
		return Link{}, ErrInvalidMagnet
	}
	q := u.Query()
	var btihs []string
	for _, xt := range q["xt"] {
		if len(xt) > 9 && strings.EqualFold(xt[:9], "urn:btih:") {
			btihs = append(btihs, xt[9:])
		}
	}
	if len(btihs) != 1 {
		return Link{}, ErrInvalidMagnet
	}
	digest := btihs[0]
	switch {
	case hex40Re.MatchString(digest):
		digest = strings.ToLower(digest)
	case b32Re.MatchString(digest):
		decoded, err := base32.StdEncoding.DecodeString(strings.ToUpper(digest))
		if err != nil || len(decoded) != 20 {
			return Link{}, ErrInvalidMagnet
		}
		digest = hex.EncodeToString(decoded)
	default:
		return Link{}, ErrInvalidMagnet
	}
	name := q.Get("dn")
	if strings.TrimSpace(name) == "" {
		name = "Magnet-" + digest[:8]
	}
	return Link{
		Original: v,
		Identity: "btih:" + digest,
		Kind:     "magnet",
		Name:     name,
		InfoHash: digest,
	}, nil
}

func parseWebURL(v string) (Link, error) {
	u, err := url.Parse(v)
	if err != nil {
		return Link{}, ErrUnsupportedURL
	}
	scheme := strings.ToLower(u.Scheme)
	if (scheme != "http" && scheme != "https" && scheme != "ftp") || u.Hostname() == "" || u.User != nil {
		return Link{}, ErrUnsupportedURL
	}
	host := strings.ToLower(u.Hostname())
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	port := u.Port()
	defaultPort := map[string]string{"http": "80", "https": "443", "ftp": "21"}[scheme]
	if port != "" && port != defaultPort {
		host += ":" + port
	}
	p := u.EscapedPath()
	if p == "" {
		p = "/"
	}
	normalized := scheme + "://" + host + p
	if u.RawQuery != "" {
		normalized += "?" + u.RawQuery
	}
	sum := sha256.Sum256([]byte(normalized))
	name := host
	if idx := strings.LastIndex(u.Path, "/"); idx >= 0 && idx+1 < len(u.Path) {
		if unesc, err := url.PathUnescape(u.Path[idx+1:]); err == nil && unesc != "" {
			name = unesc
		}
	}
	return Link{
		Original: v,
		Identity: "url:" + hex.EncodeToString(sum[:]),
		Kind:     scheme,
		Name:     name,
	}, nil
}

func Extract(text string) ([]Link, []error, error) {
	matches := linkPattern.FindAllString(text, -1)
	if len(matches) > MaxImportLinks {
		return nil, nil, ErrImportLimit
	}
	if len(matches) == 0 {
		return nil, nil, ErrNoLinks
	}
	out := make([]Link, 0, len(matches))
	var errs []error
	for _, m := range matches {
		l, err := Parse(m)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		out = append(out, l)
	}
	return out, errs, nil
}
