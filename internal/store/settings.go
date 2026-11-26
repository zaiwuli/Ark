package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

func (s *Store) SetSetting(ctx context.Context, key string, value any, secret bool) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	stored := string(raw)
	encFlag := 0
	if secret {
		sealed, err := s.sealer.Seal(stored)
		if err != nil {
			return err
		}
		stored = sealed
		encFlag = 1
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO settings(key, value_json, encrypted, updated_at)
		VALUES(?, ?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET
			value_json = excluded.value_json,
			encrypted = excluded.encrypted,
			updated_at = excluded.updated_at
	`, key, stored, encFlag, time.Now().Unix())
	return err
}

func (s *Store) GetSetting(ctx context.Context, key string, dest any) (bool, error) {
	var stored string
	var encFlag int
	err := s.db.QueryRowContext(ctx, `SELECT value_json, encrypted FROM settings WHERE key = ?`, key).Scan(&stored, &encFlag)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if encFlag == 1 {
		opened, err := s.sealer.Open(stored)
		if err != nil {
			return false, err
		}
		stored = opened
	}
	if err := json.Unmarshal([]byte(stored), dest); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) DeleteSetting(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM settings WHERE key = ?`, key)
	return err
}
