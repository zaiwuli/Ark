package store

import (
	"context"
	"time"
)

type BarkMessage struct {
	ID              int64  `json:"id"`
	Title           string `json:"title"`
	Body            string `json:"body"`
	GroupName       string `json:"group_name"`
	MatchedSourceID string `json:"matched_source_id"`
	Status          string `json:"status"`
	WindowID        string `json:"window_id"`
	CreatedAt       int64  `json:"created_at"`
}

func (s *Store) ListPendingNotifications(ctx context.Context, now int64, limit int) ([]TaskEvent, error) {
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, pipeline_id, kind, stage, message, notify_status, created_at
		FROM task_events
		WHERE notify_status = 'pending' AND notify_next_at <= ?
		ORDER BY id ASC LIMIT ?
	`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TaskEvent
	for rows.Next() {
		var ev TaskEvent
		if err := rows.Scan(&ev.ID, &ev.PipelineID, &ev.Kind, &ev.Stage, &ev.Message, &ev.NotifyStatus, &ev.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (s *Store) MarkNotificationSent(ctx context.Context, eventID int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE task_events SET notify_status = 'sent', notify_error = '' WHERE id = ?
	`, eventID)
	return err
}

func (s *Store) MarkNotificationRetryOrFail(ctx context.Context, eventID int64, errMsg string) error {
	var attempts int
	if err := s.db.QueryRowContext(ctx, `SELECT notify_attempts FROM task_events WHERE id = ?`, eventID).Scan(&attempts); err != nil {
		return err
	}
	attempts++
	status := "pending"
	nextAt := time.Now().Unix() + int64(attempts*60)
	if attempts >= 3 {
		status = "failed"
		nextAt = 0
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE task_events
		SET notify_status = ?, notify_attempts = ?, notify_next_at = ?, notify_error = ?
		WHERE id = ?
	`, status, attempts, nextAt, errMsg, eventID)
	return err
}

func (s *Store) SaveBarkMessage(ctx context.Context, m BarkMessage) (BarkMessage, error) {
	if m.CreatedAt == 0 {
		m.CreatedAt = time.Now().Unix()
	}
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO bark_messages(title, body, group_name, matched_source_id, status, window_id, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)
	`, m.Title, m.Body, m.GroupName, m.MatchedSourceID, m.Status, m.WindowID, m.CreatedAt)
	if err != nil {
		return BarkMessage{}, err
	}
	id, _ := res.LastInsertId()
	m.ID = id
	// Keep latest 50 sanitized Bark messages.
	_, _ = s.db.ExecContext(ctx, `
		DELETE FROM bark_messages WHERE id NOT IN (
			SELECT id FROM bark_messages ORDER BY id DESC LIMIT 50
		)
	`)
	return m, nil
}

func (s *Store) MarkBarkWindowDispatched(ctx context.Context, windowID string) error {
	if windowID == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `UPDATE bark_messages SET status = 'dispatched' WHERE window_id = ?`, windowID)
	return err
}

func (s *Store) ListBarkMessages(ctx context.Context, limit int) ([]BarkMessage, error) {
	if limit <= 0 || limit > 50 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, title, body, group_name, matched_source_id, status, window_id, created_at
		FROM bark_messages ORDER BY id DESC LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]BarkMessage, 0)
	for rows.Next() {
		var m BarkMessage
		if err := rows.Scan(&m.ID, &m.Title, &m.Body, &m.GroupName, &m.MatchedSourceID, &m.Status, &m.WindowID, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
