package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// メモリに反映済みの変更を、まとめて非同期にDBへ書き込む。
// コミットごとの fsync を待たずに応答でき、再起動時はDBから状態を復元できる。
// 一意制約やクーポンの排他が必要な書き込みは、ここを通さずハンドラ内で同期的に行う。

// まとめてDBへ書き込む間隔
const dbWriteInterval = 100 * time.Millisecond

// 1つの文にまとめる最大行数
const dbWriteChunkSize = 1000

type pendingExec struct {
	query string
	args  []any
}

type pendingSentAt struct {
	id string
	at time.Time
}

var (
	writeBufferMu      sync.Mutex
	pendingLocations   []ChairLocation
	pendingStatuses    []RideStatus
	pendingAppSentAt   []pendingSentAt
	pendingChairSentAt []pendingSentAt
	pendingExecs       []pendingExec

	// 書き込みと初期化を直列化する
	dbWriterFlushMu sync.Mutex
)

func enqueueExec(query string, args ...any) {
	writeBufferMu.Lock()
	pendingExecs = append(pendingExecs, pendingExec{query: query, args: args})
	writeBufferMu.Unlock()
}

func enqueueChairLocation(l ChairLocation) {
	writeBufferMu.Lock()
	pendingLocations = append(pendingLocations, l)
	writeBufferMu.Unlock()
}

func enqueueRideStatus(s RideStatus) {
	writeBufferMu.Lock()
	pendingStatuses = append(pendingStatuses, s)
	writeBufferMu.Unlock()
}

// column は app_sent_at か chair_sent_at
func enqueueSentAt(column string, id string, at time.Time) {
	writeBufferMu.Lock()
	if column == "app_sent_at" {
		pendingAppSentAt = append(pendingAppSentAt, pendingSentAt{id: id, at: at})
	} else {
		pendingChairSentAt = append(pendingChairSentAt, pendingSentAt{id: id, at: at})
	}
	writeBufferMu.Unlock()
}

func startDBWriter() {
	go func() {
		for range time.Tick(dbWriteInterval) {
			flushWrites(context.Background())
		}
	}()
}

func flushWrites(ctx context.Context) {
	dbWriterFlushMu.Lock()
	defer dbWriterFlushMu.Unlock()

	writeBufferMu.Lock()
	locations, statuses, appSentAt, chairSentAt, execs := pendingLocations, pendingStatuses, pendingAppSentAt, pendingChairSentAt, pendingExecs
	pendingLocations, pendingStatuses, pendingAppSentAt, pendingChairSentAt, pendingExecs = nil, nil, nil, nil, nil
	writeBufferMu.Unlock()

	if err := bulkInsert(ctx, "INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at)", locations, func(l ChairLocation) []any {
		return []any{l.ID, l.ChairID, l.Latitude, l.Longitude, l.CreatedAt}
	}); err != nil {
		slog.Error("failed to insert chair locations", "error", err)
	}
	// 通知済みフラグを更新する前に、状態遷移の行を作っておく
	if err := bulkInsert(ctx, "INSERT INTO ride_statuses (id, ride_id, status, created_at)", statuses, func(s RideStatus) []any {
		return []any{s.ID, s.RideID, s.Status, s.CreatedAt}
	}); err != nil {
		slog.Error("failed to insert ride statuses", "error", err)
	}
	if err := bulkUpdateSentAt(ctx, "app_sent_at", appSentAt); err != nil {
		slog.Error("failed to update app_sent_at", "error", err)
	}
	if err := bulkUpdateSentAt(ctx, "chair_sent_at", chairSentAt); err != nil {
		slog.Error("failed to update chair_sent_at", "error", err)
	}
	if err := execInTx(ctx, execs); err != nil {
		slog.Error("failed to exec pending writes", "error", err)
	}
}

// 複数行の INSERT にまとめて実行する
func bulkInsert[T any](ctx context.Context, insert string, rows []T, values func(T) []any) error {
	for start := 0; start < len(rows); start += dbWriteChunkSize {
		chunk := rows[start:min(start+dbWriteChunkSize, len(rows))]
		placeholders := make([]string, 0, len(chunk))
		args := []any{}
		for _, row := range chunk {
			v := values(row)
			placeholders = append(placeholders, "("+strings.Repeat("?, ", len(v)-1)+"?)")
			args = append(args, v...)
		}
		if _, err := db.ExecContext(ctx, insert+" VALUES "+strings.Join(placeholders, ", "), args...); err != nil {
			return err
		}
	}
	return nil
}

// CASE 式で複数行の通知日時を1つの UPDATE にまとめる
func bulkUpdateSentAt(ctx context.Context, column string, updates []pendingSentAt) error {
	for start := 0; start < len(updates); start += dbWriteChunkSize {
		chunk := updates[start:min(start+dbWriteChunkSize, len(updates))]
		args := make([]any, 0, len(chunk)*3)
		for _, u := range chunk {
			args = append(args, u.id, u.at)
		}
		for _, u := range chunk {
			args = append(args, u.id)
		}
		query := "UPDATE ride_statuses SET " + column + " = CASE id" + strings.Repeat(" WHEN ? THEN ?", len(chunk)) +
			" END WHERE id IN (" + strings.Repeat("?, ", len(chunk)-1) + "?)"
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}

func execInTx(ctx context.Context, execs []pendingExec) error {
	if len(execs) == 0 {
		return nil
	}
	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// 1件の失敗で残りを捨てないよう、エラーは記録して続ける
	for _, e := range execs {
		if _, err := tx.ExecContext(ctx, e.query, e.args...); err != nil {
			slog.Error("failed to exec pending write", "query", e.query, "error", err)
		}
	}
	return tx.Commit()
}

// 初期化前の変更が初期化後のDBへ書き込まれないよう、書き込みを止めてバッファを捨てる
func lockDBWriterForInitialize() (unlock func()) {
	dbWriterFlushMu.Lock()
	writeBufferMu.Lock()
	pendingLocations, pendingStatuses, pendingAppSentAt, pendingChairSentAt, pendingExecs = nil, nil, nil, nil, nil
	writeBufferMu.Unlock()
	return dbWriterFlushMu.Unlock
}
