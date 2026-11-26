package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
	ErrInvalid  = errors.New("invalid argument")
)

type APIToken struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Note       string `json:"note"`
	Role       string `json:"role"`
	Prefix     string `json:"prefix"`
	ExpiresAt  *int64 `json:"expires_at,omitempty"`
	LastUsedAt *int64 `json:"last_used_at,omitempty"`
	CreatedAt  int64  `json:"created_at"`
}

func HashToken(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func (s *Store) CreateToken(ctx context.Context, name, note, role string, expiresAt *int64) (APIToken, string, error) {
	if name == "" || (role != "admin" && role != "readonly") {
		return APIToken{}, "", ErrInvalid
	}
	rawBytes := make([]byte, 24)
	if _, err := rand.Read(rawBytes); err != nil {
		return APIToken{}, "", err
	}
	plain := "ark_" + hex.EncodeToString(rawBytes)
	tok := APIToken{
		ID:        NewID("tok"),
		Name:      name,
		Note:      note,
		Role:      role,
		Prefix:    plain[:12],
		ExpiresAt: expiresAt,
		CreatedAt: time.Now().Unix(),
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO api_tokens(id, name, note, role, token_hash, prefix, expires_at, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
	`, tok.ID, tok.Name, tok.Note, tok.Role, HashToken(plain), tok.Prefix, tok.ExpiresAt, tok.CreatedAt)
	if err != nil {
		return APIToken{}, "", err
	}
	return tok, plain, nil
}

func (s *Store) VerifyToken(ctx context.Context, plain string) (*APIToken, error) {
	if plain == "" {
		return nil, ErrNotFound
	}
	var tok APIToken
	var exp, last sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, note, role, prefix, expires_at, last_used_at, created_at
		FROM api_tokens WHERE token_hash = ?
	`, HashToken(plain)).Scan(&tok.ID, &tok.Name, &tok.Note, &tok.Role, &tok.Prefix, &exp, &last, &tok.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	if exp.Valid {
		if exp.Int64 <= now {
			return nil, ErrNotFound
		}
		v := exp.Int64
		tok.ExpiresAt = &v
	}
	if last.Valid {
		v := last.Int64
		tok.LastUsedAt = &v
	}
	_, _ = s.db.ExecContext(ctx, `UPDATE api_tokens SET last_used_at = ? WHERE id = ?`, now, tok.ID)
	return &tok, nil
}

func (s *Store) ListTokens(ctx context.Context) ([]APIToken, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, name, note, role, prefix, expires_at, last_used_at, created_at
		FROM api_tokens ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]APIToken, 0)
	for rows.Next() {
		var tok APIToken
		var exp, last sql.NullInt64
		if err := rows.Scan(&tok.ID, &tok.Name, &tok.Note, &tok.Role, &tok.Prefix, &exp, &last, &tok.CreatedAt); err != nil {
			return nil, err
		}
		if exp.Valid {
			v := exp.Int64
			tok.ExpiresAt = &v
		}
		if last.Valid {
			v := last.Int64
			tok.LastUsedAt = &v
		}
		list = append(list, tok)
	}
	return list, rows.Err()
}

func (s *Store) DeleteToken(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM api_tokens WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) HasAnyTokens(ctx context.Context) (bool, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM api_tokens`).Scan(&count); err != nil {
		return false, err
	}
	return count > 0, nil
}
