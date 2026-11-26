package torrent

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"net/url"
	"strconv"
	"unicode/utf8"

	"github.com/zaiwuli/Ark/internal/links"
)

const MaxTorrentBytes = 2 * 1024 * 1024

var (
	ErrSizeLimit     = errors.New("TORRENT_SIZE_LIMIT")
	ErrV2Unsupported = errors.New("TORRENT_V2_UNSUPPORTED")
	ErrInvalid       = errors.New("INVALID_TORRENT")
)

// ParseToMagnet decodes a BitTorrent v1 or hybrid .torrent locally and converts it to a magnet link.
// The SHA-1 infohash is computed over the exact raw bytes of the top-level "info" dictionary.
func ParseToMagnet(raw []byte) (links.Link, error) {
	if len(raw) == 0 || len(raw) > MaxTorrentBytes {
		return links.Link{}, ErrSizeLimit
	}
	d := &decoder{data: raw}
	rootVal, err := d.decode(0)
	if err != nil || d.pos != len(raw) || len(d.infoRaw) == 0 {
		return links.Link{}, ErrInvalid
	}
	root, ok := rootVal.(map[string]any)
	if !ok {
		return links.Link{}, ErrInvalid
	}
	info, ok := root["info"].(map[string]any)
	if !ok || len(info) == 0 {
		return links.Link{}, ErrInvalid
	}
	if _, hasMetaVer := info["meta version"]; hasMetaVer {
		if _, hasPieces := info["pieces"]; !hasPieces {
			return links.Link{}, ErrV2Unsupported
		}
	}

	var nameBytes []byte
	if b, ok := info["name.utf-8"].([]byte); ok && len(b) > 0 {
		nameBytes = b
	} else if b, ok := info["name"].([]byte); ok && len(b) > 0 {
		nameBytes = b
	} else {
		nameBytes = []byte("Unknown_Torrent")
	}

	name := sanitizeUTF8(nameBytes)
	if name == "" || len(name) > 1024 {
		return links.Link{}, ErrInvalid
	}
	for _, r := range name {
		if r < 32 {
			return links.Link{}, ErrInvalid
		}
	}

	sum := sha1.Sum(d.infoRaw)
	digest := hex.EncodeToString(sum[:])
	magnet := "magnet:?xt=urn:btih:" + digest + "&dn=" + url.QueryEscape(name)
	return links.Link{
		Original: magnet,
		Identity: "btih:" + digest,
		Kind:     "magnet",
		Name:     name,
		InfoHash: digest,
	}, nil
}

func sanitizeUTF8(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return string(bytes.ToValidUTF8(b, []byte("\uFFFD")))
}

type decoder struct {
	data    []byte
	pos     int
	infoRaw []byte
}

func (d *decoder) decode(depth int) (any, error) {
	if depth > 128 || d.pos >= len(d.data) {
		return nil, ErrInvalid
	}
	switch d.data[d.pos] {
	case 'i':
		d.pos++
		end := bytes.IndexByte(d.data[d.pos:], 'e')
		if end <= 0 {
			return nil, ErrInvalid
		}
		numBytes := d.data[d.pos : d.pos+end]
		if (len(numBytes) > 1 && numBytes[0] == '0') || string(numBytes) == "-0" {
			return nil, ErrInvalid
		}
		v, err := strconv.ParseInt(string(numBytes), 10, 64)
		if err != nil {
			return nil, ErrInvalid
		}
		d.pos += end + 1
		return v, nil

	case 'l':
		d.pos++
		var out []any
		for d.pos < len(d.data) && d.data[d.pos] != 'e' {
			item, err := d.decode(depth + 1)
			if err != nil {
				return nil, err
			}
			out = append(out, item)
		}
		if d.pos >= len(d.data) || d.data[d.pos] != 'e' {
			return nil, ErrInvalid
		}
		d.pos++
		return out, nil

	case 'd':
		d.pos++
		out := make(map[string]any)
		for d.pos < len(d.data) && d.data[d.pos] != 'e' {
			keyRaw, err := d.decodeBytes()
			if err != nil {
				return nil, err
			}
			key := string(keyRaw)
			if _, dup := out[key]; dup {
				return nil, ErrInvalid
			}
			valStart := d.pos
			val, err := d.decode(depth + 1)
			if err != nil {
				return nil, err
			}
			if depth == 0 && key == "info" {
				d.infoRaw = d.data[valStart:d.pos]
			}
			out[key] = val
		}
		if d.pos >= len(d.data) || d.data[d.pos] != 'e' {
			return nil, ErrInvalid
		}
		d.pos++
		return out, nil

	default:
		return d.decodeBytes()
	}
}

func (d *decoder) decodeBytes() ([]byte, error) {
	colon := bytes.IndexByte(d.data[d.pos:], ':')
	if colon <= 0 {
		return nil, ErrInvalid
	}
	lenStr := string(d.data[d.pos : d.pos+colon])
	n, err := strconv.Atoi(lenStr)
	if err != nil || n < 0 {
		return nil, ErrInvalid
	}
	start := d.pos + colon + 1
	end := start + n
	if end > len(d.data) {
		return nil, ErrInvalid
	}
	d.pos = end
	return d.data[start:end], nil
}
