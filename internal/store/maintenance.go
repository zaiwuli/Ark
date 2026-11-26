package store

import (
	"context"
	"os"
	"time"
)

type MaintenanceStats struct {
	CompletedPipelines int64 `json:"completed_pipelines"`
	FailedPipelines    int64 `json:"failed_pipelines"`
	ActivePipelines    int64 `json:"active_pipelines"`
	EventCount         int64 `json:"event_count"`
	BarkCount          int64 `json:"bark_count"`
	DBBytes            int64 `json:"db_bytes"`
}

func (s *Store) MaintenanceStats(ctx context.Context, dbPath string) (MaintenanceStats, error) {
	var st MaintenanceStats
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM pipelines WHERE state = 'completed'`).Scan(&st.CompletedPipelines)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM pipelines WHERE state IN ('failed', 'submit_failed', 'seed_cleanup_failed')`).Scan(&st.FailedPipelines)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM pipelines WHERE state NOT IN ('completed', 'failed', 'submit_failed', 'seed_cleanup_failed')`).Scan(&st.ActivePipelines)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM task_events`).Scan(&st.EventCount)
	_ = s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM bark_messages`).Scan(&st.BarkCount)
	for _, p := range []string{dbPath, dbPath + "-wal", dbPath + "-shm"} {
		if info, err := os.Stat(p); err == nil {
			st.DBBytes += info.Size()
		}
	}
	return st, nil
}

// ClearPipelines deletes pipeline records by scope without ever touching real files.
func (s *Store) ClearPipelines(ctx context.Context, scope string) (int64, error) {
	var query string
	switch scope {
	case "completed":
		query = `DELETE FROM pipelines WHERE state = 'completed'`
	case "failed":
		query = `DELETE FROM pipelines WHERE state IN ('failed', 'submit_failed', 'seed_cleanup_failed', 'cancelled')`
	case "terminal", "":
		query = `DELETE FROM pipelines WHERE state IN ('completed', 'failed', 'submit_failed', 'seed_cleanup_failed', 'cancelled')`
	case "queued":
		query = `DELETE FROM pipelines WHERE state IN ('queued', 'cloud_prepare', 'wait_local_confirm', 'cancelled')`
	case "all":
		query = `DELETE FROM pipelines`
	default:
		return 0, ErrInvalid
	}
	res, err := s.db.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// RetryAllFailed re-queues all definitely failed pipelines.
func (s *Store) RetryAllFailed(ctx context.Context) (int64, error) {
	now := time.Now().Unix()
	res1, err := s.db.ExecContext(ctx, `
		UPDATE pipelines
		SET state = 'seed_cleanup', stage = 'seed_cleanup', error_code = '', error_message = '', next_run_at = ?, updated_at = ?
		WHERE state = 'seed_cleanup_failed'
	`, now, now)
	if err != nil {
		return 0, err
	}
	n1, _ := res1.RowsAffected()

	res2, err := s.db.ExecContext(ctx, `
		UPDATE pipelines
		SET state = 'queued', stage = 'offline_submit', error_code = '', error_message = '', next_run_at = ?, updated_at = ?
		WHERE kind = 'offline' AND state IN ('failed', 'submit_failed')
	`, now, now)
	if err != nil {
		return n1, err
	}
	n2, _ := res2.RowsAffected()

	res3, err := s.db.ExecContext(ctx, `
		UPDATE pipelines
		SET state = 'local_extract', stage = 'local_extract', error_code = '', error_message = '', next_run_at = ?, updated_at = ?
		WHERE kind = 'local_archive' AND state = 'failed'
	`, now, now)
	if err != nil {
		return n1 + n2, err
	}
	n3, _ := res3.RowsAffected()

	res4, err := s.db.ExecContext(ctx, `
		UPDATE pipelines
		SET state = 'cloud_prepare', stage = 'cloud_prepare', error_code = '', error_message = '', next_run_at = ?, updated_at = ?
		WHERE kind = 'cloud_archive' AND state = 'failed'
	`, now, now)
	if err != nil {
		return n1 + n2 + n3, err
	}
	n4, _ := res4.RowsAffected()
	return n1 + n2 + n3 + n4, nil
}

// ActiveCachePipelineIDs returns pipeline IDs currently caching or extracting locally so cache reclaim never touches them.
func (s *Store) ActiveCachePipelineIDs(ctx context.Context) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM pipelines WHERE state IN ('caching', 'local_extract')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	active := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			active[id] = true
		}
	}
	return active, rows.Err()
}

// ActiveDatesAndCIDs returns target_date and target_cid sets referenced by non-terminal pipelines
// so the date-folder organizer never moves a folder still in active use.
func (s *Store) ActiveDatesAndCIDs(ctx context.Context) (dates map[string]bool, cids map[string]bool, err error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT target_date, target_cid
		FROM pipelines
		WHERE state NOT IN ('completed', 'failed', 'submit_failed', 'seed_cleanup_failed')
	`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	dates = make(map[string]bool)
	cids = make(map[string]bool)
	for rows.Next() {
		var d, c string
		if err := rows.Scan(&d, &c); err == nil {
			if d != "" {
				dates[d] = true
			}
			if c != "" {
				cids[c] = true
			}
		}
	}
	return dates, cids, rows.Err()
}

func (s *Store) ClearEvents(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM task_events`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) ClearBarkMessages(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM bark_messages`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) Vacuum(ctx context.Context) error {
	_, _ = s.db.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)
	_, err := s.db.ExecContext(ctx, `VACUUM`)
	return err
}
