package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type Source struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Kind       string `json:"kind"` // cloud | local
	Enabled    bool   `json:"enabled"`
	ConfigJSON string `json:"config_json"`
	BarkPrefix string `json:"bark_prefix"`
	Cursor     string `json:"cursor"`
	NextRunAt  int64  `json:"next_run_at"`
	CreatedAt  int64  `json:"created_at"`
	UpdatedAt  int64  `json:"updated_at"`
}

type Rule struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Scope      string `json:"scope"` // global | source | seed
	SourceID   string `json:"source_id"`
	Enabled    bool   `json:"enabled"`
	ConfigJSON string `json:"config_json"`
	CreatedAt  int64  `json:"created_at"`
	UpdatedAt  int64  `json:"updated_at"`
}

func (s *Store) ListSources(ctx context.Context, kind string) ([]Source, error) {
	q := `SELECT id, name, kind, enabled, config_json, bark_prefix, cursor, next_run_at, created_at, updated_at FROM sources`
	var args []any
	if kind != "" {
		q += ` WHERE kind = ?`
		args = append(args, kind)
	}
	q += ` ORDER BY created_at ASC`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Source, 0)
	for rows.Next() {
		var src Source
		var en int
		if err := rows.Scan(&src.ID, &src.Name, &src.Kind, &en, &src.ConfigJSON, &src.BarkPrefix, &src.Cursor, &src.NextRunAt, &src.CreatedAt, &src.UpdatedAt); err != nil {
			return nil, err
		}
		src.Enabled = en == 1
		out = append(out, src)
	}
	return out, rows.Err()
}

func (s *Store) UpsertSource(ctx context.Context, src Source) (Source, error) {
	if src.Name == "" || (src.Kind != "cloud" && src.Kind != "local") {
		return Source{}, ErrInvalid
	}
	now := time.Now().Unix()
	if src.ID == "" {
		src.ID = NewID("src")
		src.CreatedAt = now
	}
	if src.ConfigJSON == "" {
		src.ConfigJSON = "{}"
	}
	src.UpdatedAt = now
	en := 0
	if src.Enabled {
		en = 1
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sources(id, name, kind, enabled, config_json, bark_prefix, cursor, next_run_at, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name=excluded.name, enabled=excluded.enabled, config_json=excluded.config_json,
			bark_prefix=excluded.bark_prefix, updated_at=excluded.updated_at
	`, src.ID, src.Name, src.Kind, en, src.ConfigJSON, src.BarkPrefix, src.Cursor, src.NextRunAt, src.CreatedAt, src.UpdatedAt)
	return src, err
}

func (s *Store) DeleteSource(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM sources WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ListRules(ctx context.Context, scope string) ([]Rule, error) {
	q := `SELECT id, name, scope, source_id, enabled, config_json, created_at, updated_at FROM rules`
	var args []any
	if scope != "" {
		q += ` WHERE scope = ?`
		args = append(args, scope)
	}
	q += ` ORDER BY created_at ASC`
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Rule, 0)
	for rows.Next() {
		var r Rule
		var en int
		if err := rows.Scan(&r.ID, &r.Name, &r.Scope, &r.SourceID, &en, &r.ConfigJSON, &r.CreatedAt, &r.UpdatedAt); err != nil {
			return nil, err
		}
		r.Enabled = en == 1
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) GetSeedRule(ctx context.Context) (*Rule, error) {
	var r Rule
	var en int
	err := s.db.QueryRowContext(ctx, `
		SELECT id, name, scope, source_id, enabled, config_json, created_at, updated_at
		FROM rules WHERE scope = 'seed' AND enabled = 1 ORDER BY updated_at DESC LIMIT 1
	`).Scan(&r.ID, &r.Name, &r.Scope, &r.SourceID, &en, &r.ConfigJSON, &r.CreatedAt, &r.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	r.Enabled = en == 1
	return &r, nil
}

func (s *Store) UpsertRule(ctx context.Context, r Rule) (Rule, error) {
	if r.Name == "" || (r.Scope != "global" && r.Scope != "source" && r.Scope != "seed") {
		return Rule{}, ErrInvalid
	}
	now := time.Now().Unix()
	if r.ID == "" {
		r.ID = NewID("rul")
		r.CreatedAt = now
	}
	if r.ConfigJSON == "" {
		r.ConfigJSON = "{}"
	}
	r.UpdatedAt = now
	en := 0
	if r.Enabled {
		en = 1
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO rules(id, name, scope, source_id, enabled, config_json, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			name=excluded.name, scope=excluded.scope, source_id=excluded.source_id,
			enabled=excluded.enabled, config_json=excluded.config_json, updated_at=excluded.updated_at
	`, r.ID, r.Name, r.Scope, r.SourceID, en, r.ConfigJSON, r.CreatedAt, r.UpdatedAt)
	return r, err
}

func (s *Store) DeleteRule(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM rules WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}
