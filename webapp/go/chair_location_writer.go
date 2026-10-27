package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// 椅子の位置情報をまとめてDBへ書き込む間隔
const chairLocationFlushInterval = 100 * time.Millisecond

var (
	chairLocationBufferMu sync.Mutex
	chairLocationBuffer   []ChairLocation

	// 書き込みと初期化を直列化する
	chairLocationFlushMu sync.Mutex
)

func enqueueChairLocation(l ChairLocation) {
	chairLocationBufferMu.Lock()
	chairLocationBuffer = append(chairLocationBuffer, l)
	chairLocationBufferMu.Unlock()
}

func startChairLocationWriter() {
	go func() {
		for range time.Tick(chairLocationFlushInterval) {
			if err := flushChairLocations(context.Background()); err != nil {
				slog.Error("failed to flush chair locations", "error", err)
			}
		}
	}()
}

func flushChairLocations(ctx context.Context) error {
	chairLocationFlushMu.Lock()
	defer chairLocationFlushMu.Unlock()

	chairLocationBufferMu.Lock()
	locations := chairLocationBuffer
	chairLocationBuffer = nil
	chairLocationBufferMu.Unlock()

	const chunkSize = 1000
	for start := 0; start < len(locations); start += chunkSize {
		chunk := locations[start:min(start+chunkSize, len(locations))]
		placeholders := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)*5)
		for _, l := range chunk {
			placeholders = append(placeholders, "(?, ?, ?, ?, ?)")
			args = append(args, l.ID, l.ChairID, l.Latitude, l.Longitude, l.CreatedAt)
		}
		query := "INSERT INTO chair_locations (id, chair_id, latitude, longitude, created_at) VALUES " + strings.Join(placeholders, ", ")
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return err
		}
	}
	return nil
}

// 初期化前の位置情報が初期化後のDBへ書き込まれないよう、書き込みを止めてバッファを捨てる
func lockChairLocationWriterForInitialize() (unlock func()) {
	chairLocationFlushMu.Lock()
	chairLocationBufferMu.Lock()
	chairLocationBuffer = nil
	chairLocationBufferMu.Unlock()
	return chairLocationFlushMu.Unlock
}
