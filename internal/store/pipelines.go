package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/zaiwuli/Ark/internal/links"
)

var (
	ErrIdempotencyConflict = errors.New("IDEMPOTENCY_KEY_CONFLICT")
	ErrPipelineBusy        = errors.New("PIPELINE_BUSY")
)

type Pipeline struct {
	ID              string `json:"id"`
	Kind            string `json:"kind"` // offline | cloud_archive | local_archive
	Identity        string `json:"identity"`
	ParentID        string `json:"parent_id,omitempty"`
	BatchID         string `json:"batch_id,omitempty"`
	SourceID        string `json:"source_id,omitempty"`
	Name            string `json:"name"`
	OriginalLink    string `json:"original_link,omitempty"`
	InfoHash        string `json:"info_hash,omitempty"`
	Size            int64  `json:"size"`
	State           string `json:"state"`
	Stage           string `json:"stage"`
	WaitReason      string `json:"wait_reason,omitempty"`
	ErrorCode       string `json:"error_code,omitempty"`
	ErrorMessage    string `json:"error_message,omitempty"`
	TargetDate      string `json:"target_date,omitempty"`
	TargetCID       string `json:"target_cid,omitempty"`
	AutoExtract     bool   `json:"auto_extract"`
	Attempts        int    `json:"attempts"`
	MaxAttempts     int    `json:"max_attempts"`
	NextRunAt       int64  `json:"next_run_at"`
	CancelRequested bool   `json:"cancel_requested"`
	LocatorJSON     string `json:"locator_json,omitempty"`
	PolicyJSON      string `json:"policy_json,omitempty"`
	OutputJSON      string `json:"output_json,omitempty"`
	ProgressJSON    string `json:"progress_json,omitempty"`
	RemoteID        string `json:"remote_id,omitempty"`
	CreatedAt       int64  `json:"created_at"`
	UpdatedAt       int64  `json:"updated_at"`
	CompletedAt     *int64 `json:"completed_at,omitempty"`
}

type BatchResult struct {
	BatchID     string `json:"batch_id"`
	Queued      int    `json:"queued"`
	Duplicates  int    `json:"duplicates"`
	Invalid     int    `json:"invalid"`
	Message     string `json:"message"`
	OperationID string `json:"operation_id,omitempty"`
	Replayed    bool   `json:"-"`
}

type BatchItemView struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Stage  string `json:"stage"`
}

type BatchDetail struct {
	BatchID string          `json:"batch_id"`
	Items   []BatchItemView `json:"items"`
}

type TaskEvent struct {
	ID           int64  `json:"id"`
	PipelineID   string `json:"pipeline_id"`
	Kind         string `json:"kind"`
	Stage        string `json:"stage"`
	Message      string `json:"message"`
	NotifyStatus string `json:"notify_status"`
	CreatedAt    int64  `json:"created_at"`
}

type RegisterBatchInput struct {
	TokenID        string
	IdempotencyKey string
	RequestHash    string
	TargetDate     string
	AutoExtract    bool
	Links          []links.Link
	InvalidCount   int
	PolicyJSON     string
}

func (s *Store) RegisterOfflineBatch(ctx context.Context, in RegisterBatchInput) (BatchResult, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return BatchResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if in.IdempotencyKey != "" {
		var existingID, existingHash string
		var q, d, inv int
		err := tx.QueryRowContext(ctx, `
			SELECT id, request_hash, queued_count, duplicate_count, invalid_count
			FROM batches WHERE token_id = ? AND idempotency_key = ?
		`, in.TokenID, in.IdempotencyKey).Scan(&existingID, &existingHash, &q, &d, &inv)
		if err == nil {
			if existingHash != in.RequestHash {
				return BatchResult{}, ErrIdempotencyConflict
			}
			return BatchResult{
				BatchID:    existingID,
				Queued:     q,
				Duplicates: d,
				Invalid:    inv,
				Message:    fmt.Sprintf("Queued %d; duplicates %d; invalid %d", q, d, inv),
				Replayed:   true,
			}, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return BatchResult{}, err
		}
	}

	now := time.Now().Unix()
	batchID := NewID("bat")
	policy := in.PolicyJSON
	if strings.TrimSpace(policy) == "" {
		policy = "{}"
	}
	autoInt := 0
	if in.AutoExtract {
		autoInt = 1
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO batches(id, token_id, idempotency_key, request_hash, target_date, auto_extract, queued_count, duplicate_count, invalid_count, created_at)
		VALUES(?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
	`, batchID, in.TokenID, in.IdempotencyKey, in.RequestHash, in.TargetDate, autoInt, in.InvalidCount, now); err != nil {
		return BatchResult{}, err
	}

	queued, dups := 0, 0
	seenInBatch := make(map[string]bool)
	for _, l := range in.Links {
		var pipeID string
		err := tx.QueryRowContext(ctx, `SELECT id FROM pipelines WHERE identity = ?`, l.Identity).Scan(&pipeID)
		dup := 0
		if errors.Is(err, sql.ErrNoRows) {
			pipeID = NewID("pl")
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO pipelines(
					id, kind, identity, batch_id, name, original_link, info_hash, size,
					state, stage, target_date, auto_extract, next_run_at, policy_json,
					created_at, updated_at
				) VALUES(?, 'offline', ?, ?, ?, ?, ?, ?, 'queued', 'offline_submit', ?, ?, ?, ?, ?, ?)
			`, pipeID, l.Identity, batchID, l.Name, l.Original, l.InfoHash, l.Size,
				in.TargetDate, autoInt, now, policy, now, now); err != nil {
				return BatchResult{}, err
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO task_events(pipeline_id, kind, stage, message, notify_status, created_at)
				VALUES(?, 'REGISTERED', 'offline_submit', '离线链接已登记排队', 'none', ?)
			`, pipeID, now); err != nil {
				return BatchResult{}, err
			}
			queued++
		} else if err != nil {
			return BatchResult{}, err
		} else {
			dups++
			dup = 1
		}
		if !seenInBatch[pipeID] {
			seenInBatch[pipeID] = true
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO batch_items(batch_id, pipeline_id, duplicate) VALUES(?, ?, ?)
			`, batchID, pipeID, dup); err != nil {
				return BatchResult{}, err
			}
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE batches SET queued_count = ?, duplicate_count = ? WHERE id = ?
	`, queued, dups, batchID); err != nil {
		return BatchResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return BatchResult{}, err
	}
	return BatchResult{
		BatchID:     batchID,
		Queued:      queued,
		Duplicates:  dups,
		Invalid:     in.InvalidCount,
		Message:     fmt.Sprintf("Queued %d; duplicates %d; invalid %d", queued, dups, in.InvalidCount),
		OperationID: batchID,
	}, nil
}

func (s *Store) GetBatch(ctx context.Context, batchID, tokenID string) (BatchDetail, error) {
	var owner string
	err := s.db.QueryRowContext(ctx, `SELECT token_id FROM batches WHERE id = ?`, batchID).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return BatchDetail{}, ErrNotFound
	}
	if err != nil {
		return BatchDetail{}, err
	}
	if owner != tokenID {
		return BatchDetail{}, ErrNotFound
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT p.id, p.name, p.state, p.stage
		FROM batch_items bi
		JOIN pipelines p ON p.id = bi.pipeline_id
		WHERE bi.batch_id = ?
		ORDER BY bi.id ASC
	`, batchID)
	if err != nil {
		return BatchDetail{}, err
	}
	defer rows.Close()

	items := make([]BatchItemView, 0)
	for rows.Next() {
		var it BatchItemView
		if err := rows.Scan(&it.ID, &it.Name, &it.Status, &it.Stage); err != nil {
			return BatchDetail{}, err
		}
		items = append(items, it)
	}
	return BatchDetail{BatchID: batchID, Items: items}, rows.Err()
}

func scanPipeline(scanner interface{ Scan(...any) error }) (Pipeline, error) {
	var p Pipeline
	var autoInt, cancelInt int
	var comp sql.NullInt64
	err := scanner.Scan(
		&p.ID, &p.Kind, &p.Identity, &p.ParentID, &p.BatchID, &p.SourceID,
		&p.Name, &p.OriginalLink, &p.InfoHash, &p.Size,
		&p.State, &p.Stage, &p.WaitReason, &p.ErrorCode, &p.ErrorMessage,
		&p.TargetDate, &p.TargetCID, &autoInt, &p.Attempts, &p.MaxAttempts,
		&p.NextRunAt, &cancelInt, &p.LocatorJSON, &p.PolicyJSON, &p.OutputJSON,
		&p.ProgressJSON, &p.RemoteID, &p.CreatedAt, &p.UpdatedAt, &comp,
	)
	if err != nil {
		return Pipeline{}, err
	}
	p.AutoExtract = autoInt == 1
	p.CancelRequested = cancelInt == 1
	if comp.Valid {
		v := comp.Int64
		p.CompletedAt = &v
	}
	return p, nil
}

const pipelineSelectCols = `
	id, kind, identity, parent_id, batch_id, source_id,
	name, original_link, info_hash, size,
	state, stage, wait_reason, error_code, error_message,
	target_date, target_cid, auto_extract, attempts, max_attempts,
	next_run_at, cancel_requested, locator_json, policy_json, output_json,
	progress_json, remote_id, created_at, updated_at, completed_at
`

func (s *Store) GetPipeline(ctx context.Context, id string) (Pipeline, error) {
	p, err := scanPipeline(s.db.QueryRowContext(ctx, `SELECT `+pipelineSelectCols+` FROM pipelines WHERE id = ?`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Pipeline{}, ErrNotFound
	}
	return p, err
}

func (s *Store) ListPipelines(ctx context.Context, kind, state string, limit int) ([]Pipeline, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	var conds []string
	var args []any
	if kind != "" {
		conds = append(conds, "kind = ?")
		args = append(args, kind)
	}
	if state != "" {
		conds = append(conds, "state = ?")
		args = append(args, state)
	}
	q := `SELECT ` + pipelineSelectCols + ` FROM pipelines`
	if len(conds) > 0 {
		q += ` WHERE ` + strings.Join(conds, " AND ")
	}
	q += ` ORDER BY created_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]Pipeline, 0)
	for rows.Next() {
		p, err := scanPipeline(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, p)
	}
	return list, rows.Err()
}

func (s *Store) ListDuePipelines(ctx context.Context, kind string, states []string, now int64, limit int) ([]Pipeline, error) {
	if len(states) == 0 {
		return nil, nil
	}
	placeholders := make([]string, len(states))
	args := make([]any, 0, len(states)+3)
	args = append(args, kind)
	for i, st := range states {
		placeholders[i] = "?"
		args = append(args, st)
	}
	args = append(args, now, limit)

	q := fmt.Sprintf(`SELECT %s FROM pipelines WHERE kind = ? AND state IN (%s) AND next_run_at <= ? ORDER BY created_at ASC LIMIT ?`,
		pipelineSelectCols, strings.Join(placeholders, ","))
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]Pipeline, 0)
	for rows.Next() {
		p, err := scanPipeline(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, p)
	}
	return list, rows.Err()
}

func (s *Store) UpdatePipeline(ctx context.Context, p Pipeline) error {
	p.UpdatedAt = time.Now().Unix()
	autoInt := 0
	if p.AutoExtract {
		autoInt = 1
	}
	cancelInt := 0
	if p.CancelRequested {
		cancelInt = 1
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE pipelines SET
			name=?, state=?, stage=?, wait_reason=?, error_code=?, error_message=?,
			target_cid=?, auto_extract=?, attempts=?, next_run_at=?, cancel_requested=?,
			locator_json=?, policy_json=?, output_json=?, progress_json=?, remote_id=?,
			updated_at=?, completed_at=?
		WHERE id = ?
	`, p.Name, p.State, p.Stage, p.WaitReason, p.ErrorCode, p.ErrorMessage,
		p.TargetCID, autoInt, p.Attempts, p.NextRunAt, cancelInt,
		p.LocatorJSON, p.PolicyJSON, p.OutputJSON, p.ProgressJSON, p.RemoteID,
		p.UpdatedAt, p.CompletedAt, p.ID)
	return err
}

func (s *Store) RetryPipeline(ctx context.Context, id string) (Pipeline, error) {
	p, err := s.GetPipeline(ctx, id)
	if err != nil {
		return Pipeline{}, err
	}
	if p.State != "failed" && p.State != "submit_failed" && p.State != "seed_cleanup_failed" {
		return Pipeline{}, ErrPipelineBusy
	}
	now := time.Now().Unix()
	if p.State == "seed_cleanup_failed" {
		p.State = "seed_cleanup"
		p.Stage = "seed_cleanup"
	} else {
		p.State = "queued"
		p.Stage = "offline_submit"
	}
	p.ErrorCode = ""
	p.ErrorMessage = ""
	p.NextRunAt = now
	p.CompletedAt = nil
	if err := s.UpdatePipeline(ctx, p); err != nil {
		return Pipeline{}, err
	}
	_ = s.RecordEvent(ctx, p.ID, "RETRIED", p.Stage, "用户触发重试", false)
	return p, nil
}

func (s *Store) DeletePipeline(ctx context.Context, id string) error {
	p, err := s.GetPipeline(ctx, id)
	if err != nil {
		return err
	}
	switch p.State {
	case "submitting", "submission_unknown", "running", "extracting", "caching", "reconciling":
		return ErrPipelineBusy
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM pipelines WHERE id = ?`, id)
	return err
}

func (s *Store) RecordEvent(ctx context.Context, pipelineID, kind, stage, message string, notify bool) error {
	status := "none"
	if notify {
		status = "pending"
	}
	now := time.Now().Unix()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO task_events(pipeline_id, kind, stage, message, notify_status, notify_next_at, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)
	`, pipelineID, kind, stage, message, status, now, now)
	return err
}

func (s *Store) ListEvents(ctx context.Context, pipelineID string, limit int) ([]TaskEvent, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	var rows *sql.Rows
	var err error
	if pipelineID != "" {
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, pipeline_id, kind, stage, message, notify_status, created_at
			FROM task_events WHERE pipeline_id = ? ORDER BY id DESC LIMIT ?
		`, pipelineID, limit)
	} else {
		rows, err = s.db.QueryContext(ctx, `
			SELECT id, pipeline_id, kind, stage, message, notify_status, created_at
			FROM task_events ORDER BY id DESC LIMIT ?
		`, limit)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]TaskEvent, 0)
	for rows.Next() {
		var ev TaskEvent
		if err := rows.Scan(&ev.ID, &ev.PipelineID, &ev.Kind, &ev.Stage, &ev.Message, &ev.NotifyStatus, &ev.CreatedAt); err != nil {
			return nil, err
		}
		list = append(list, ev)
	}
	return list, rows.Err()
}

// Operation Journal helpers for idempotent remote writes.
type JournalEntry struct {
	OpKey        string
	PipelineID   string
	Kind         string
	Status       string // prepared | completed | uncertain | failed
	EvidenceJSON string
}

func (s *Store) JournalGet(ctx context.Context, opKey string) (*JournalEntry, error) {
	var e JournalEntry
	err := s.db.QueryRowContext(ctx, `
		SELECT op_key, pipeline_id, kind, status, evidence_json
		FROM operation_journal WHERE op_key = ?
	`, opKey).Scan(&e.OpKey, &e.PipelineID, &e.Kind, &e.Status, &e.EvidenceJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func (s *Store) JournalPrepare(ctx context.Context, opKey, pipelineID, kind string, evidence any) error {
	raw, _ := json.Marshal(evidence)
	now := time.Now().Unix()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO operation_journal(op_key, pipeline_id, kind, status, evidence_json, created_at, updated_at)
		VALUES(?, ?, ?, 'prepared', ?, ?, ?)
		ON CONFLICT(op_key) DO UPDATE SET status='prepared', evidence_json=excluded.evidence_json, updated_at=excluded.updated_at
	`, opKey, pipelineID, kind, string(raw), now, now)
	return err
}

type LocalArchiveInput struct {
	Identity    string
	SourceID    string
	Name        string
	Size        int64
	LocatorJSON string
	PolicyJSON  string
}

// CreateLocalArchivePipeline inserts a local_archive pipeline if its identity does not already exist.
func (s *Store) CreateLocalArchivePipeline(ctx context.Context, in LocalArchiveInput) (bool, error) {
	now := time.Now().Unix()
	id := NewID("pl")
	res, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO pipelines(
			id, kind, identity, source_id, name, size,
			state, stage, auto_extract, next_run_at,
			locator_json, policy_json, created_at, updated_at
		) VALUES(?, 'local_archive', ?, ?, ?, ?, 'local_extract', 'local_extract', 1, ?, ?, ?, ?, ?)
	`, id, in.Identity, in.SourceID, in.Name, in.Size, now, in.LocatorJSON, in.PolicyJSON, now, now)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		_ = s.RecordEvent(ctx, id, "DISCOVERED", "local_extract", "本地监控发现稳定压缩包："+in.Name, false)
		return true, nil
	}
	return false, nil
}

// CreateCloudArchivePipeline inserts a cloud_archive pipeline linked to a parent offline task or cloud source.
func (s *Store) CreateCloudArchivePipeline(ctx context.Context, parentID, sourceID, identity, name, targetDate string, size int64, locatorJSON, policyJSON string) (string, bool, error) {
	now := time.Now().Unix()
	id := NewID("pl")
	res, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO pipelines(
			id, kind, identity, parent_id, source_id, name, size, target_date,
			state, stage, auto_extract, next_run_at,
			locator_json, policy_json, created_at, updated_at
		) VALUES(?, 'cloud_archive', ?, ?, ?, ?, ?, ?, 'cloud_prepare', 'cloud_prepare', 1, ?, ?, ?, ?, ?)
	`, id, identity, parentID, sourceID, name, size, targetDate, now, locatorJSON, policyJSON, now, now)
	if err != nil {
		return "", false, err
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		_ = s.RecordEvent(ctx, id, "DISCOVERED", "cloud_prepare", "发现云端压缩包，准备云解压："+name, false)
		return id, true, nil
	}
	return "", false, nil
}

func (s *Store) JournalSetStatus(ctx context.Context, opKey, status string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE operation_journal SET status = ?, updated_at = ? WHERE op_key = ?
	`, status, time.Now().Unix(), opKey)
	return err
}
