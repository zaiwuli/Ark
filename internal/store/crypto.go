package store

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Sealer provides AES-256-GCM encryption at rest for sensitive settings (cookies, bot tokens).
type Sealer struct {
	aead cipher.AEAD
}

func NewSealer(keyPath string, overrideHex string) (*Sealer, error) {
	var raw []byte
	if overrideHex != "" {
		decoded, err := hex.DecodeString(strings.TrimSpace(overrideHex))
		if err != nil || len(decoded) != 32 {
			return nil, errors.New("ARK_SECRET_KEY must be 64 hex chars (32 bytes)")
		}
		raw = decoded
	} else {
		data, err := os.ReadFile(keyPath)
		if err == nil {
			decoded, decErr := hex.DecodeString(strings.TrimSpace(string(data)))
			if decErr != nil || len(decoded) != 32 {
				return nil, fmt.Errorf("invalid secret key in %s", keyPath)
			}
			raw = decoded
		} else if errors.Is(err, os.ErrNotExist) {
			raw = make([]byte, 32)
			if _, err := io.ReadFull(rand.Reader, raw); err != nil {
				return nil, fmt.Errorf("generate secret key: %w", err)
			}
			if err := os.MkdirAll(filepath.Dir(keyPath), 0o755); err != nil {
				return nil, err
			}
			if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(raw)+"\n"), 0o600); err != nil {
				return nil, fmt.Errorf("persist secret key: %w", err)
			}
		} else {
			return nil, err
		}
	}

	block, err := aes.NewCipher(raw)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Sealer{aead: aead}, nil
}

func (s *Sealer) Seal(plaintext string) (string, error) {
	nonce := make([]byte, s.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := s.aead.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func (s *Sealer) Open(encoded string) (string, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", err
	}
	ns := s.aead.NonceSize()
	if len(data) < ns {
		return "", errors.New("ciphertext too short")
	}
	plaintext, err := s.aead.Open(nil, data[:ns], data[ns:], nil)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}
