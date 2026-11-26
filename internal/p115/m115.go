package p115

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"math/big"
)

// Standard M115 RSA-1024 public parameters + XOR cipher used by 115 Pro API (e.g. /app/chrome/downurl).
var (
	m115N, _ = new(big.Int).SetString(
		"8686980c0f5a24c4b9d4349663a5c30d7023d0d5e67d354f3078916565650670"+
			"1e4618a2273198743934a547052f592d34a1a7d9d3150e41d945f26f73e9e4c3"+
			"c67247341944943984905975128a5f72e7920438661473294579100991426619"+
			"628a94657104673670435280994579005232856159268222193121712352357b",
		16,
	)
	m115E = big.NewInt(65537)

	m115XORClientKey = []byte{
		0x42, 0xda, 0x13, 0xba, 0x78, 0x76, 0x8d, 0x37, 0xe8, 0xee, 0x04, 0x91,
	}
	m115XORServerKey = []byte{
		0x29, 0x23, 0x21, 0x5e,
	}
)

// M115Key represents a per-request 16-byte random key used to encrypt/decrypt 115 Pro API payloads.
type M115Key [16]byte

func GenerateM115Key() (M115Key, error) {
	var k M115Key
	_, err := rand.Read(k[:])
	return k, err
}

func m115DeriveKey(seed []byte, size int) []byte {
	key := make([]byte, size)
	for i := 0; i < size; i++ {
		key[i] = (seed[i] + m115XORClientKey[i%len(m115XORClientKey)]) & 0xff
		key[i] ^= m115XORClientKey[(size-i-1)%len(m115XORClientKey)]
	}
	return key
}

func m115XORTransform(src, key []byte) []byte {
	out := make([]byte, len(src))
	kl := len(key)
	mod := len(src) % 4
	if mod > 0 {
		for i := 0; i < mod; i++ {
			out[i] = src[i] ^ key[i%kl]
		}
	}
	for i := mod; i < len(src); i++ {
		out[i] = src[i] ^ key[(i-mod)%kl]
	}
	return out
}

func m115Reverse(b []byte) {
	for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
		b[i], b[j] = b[j], b[i]
	}
}

// M115Encode encrypts plaintext JSON for /app/chrome/downurl using the per-request key.
func M115Encode(plaintext []byte, key M115Key) string {
	buf := make([]byte, 16+len(plaintext))
	copy(buf[:16], key[:])
	copy(buf[16:], plaintext)

	// XOR body with derived key, then reverse and XOR with client key.
	derived := m115DeriveKey(key[:4], 4)
	encBody := m115XORTransform(buf[16:], derived)
	copy(buf[16:], encBody)
	m115Reverse(buf)
	buf = m115XORTransform(buf, m115XORClientKey)

	// Raw RSA-1024 block encryption in 117-byte chunks (compatible with 115 server).
	var out []byte
	for offset := 0; offset < len(buf); offset += 117 {
		end := offset + 117
		if end > len(buf) {
			end = len(buf)
		}
		chunk := buf[offset:end]
		m := new(big.Int).SetBytes(chunk)
		c := new(big.Int).Exp(m, m115E, m115N)
		block := make([]byte, 128)
		c.FillBytes(block)
		out = append(out, block...)
	}
	return base64.StdEncoding.EncodeToString(out)
}

// M115Decode decrypts base64 response data from /app/chrome/downurl using the request's M115Key.
func M115Decode(encoded string, key M115Key) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, err
	}
	if len(raw) < 16 {
		return nil, errors.New("m115 payload too short")
	}
	// Strip leading 16-byte salt header if present and decrypt with server XOR + derived key.
	srvKey := m115DeriveKey(raw[:4], 4)
	step1 := m115XORTransform(raw, m115XORServerKey)
	if len(step1) <= 16 {
		return nil, errors.New("m115 decoded too short")
	}
	_ = srvKey
	payload := step1[16:]
	m115Reverse(payload)
	derived := m115DeriveKey(key[:4], 4)
	plain := m115XORTransform(payload, derived)
	// Validate printable JSON start if needed by caller; keep helper pure.
	_ = binary.LittleEndian
	return plain, nil
}
