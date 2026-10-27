package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
)

// 状態が変わったことを通知ストリームへ伝える。チャネルの容量を1にして、取りこぼしなく通知をまとめる
type notifier struct {
	mu    sync.Mutex
	chans map[string]chan struct{}
}

var (
	appNotifier   = &notifier{chans: map[string]chan struct{}{}}
	chairNotifier = &notifier{chans: map[string]chan struct{}{}}
)

func (n *notifier) channel(id string) chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := n.chans[id]
	if ch == nil {
		ch = make(chan struct{}, 1)
		n.chans[id] = ch
	}
	return ch
}

func (n *notifier) notify(id string) {
	select {
	case n.channel(id) <- struct{}{}:
	default:
	}
}

// ライドの状態遷移をユーザーと割り当て済みの椅子に伝える
func notifyRide(userID, chairID string) {
	appNotifier.notify(userID)
	if chairID != "" {
		chairNotifier.notify(chairID)
	}
}

// 通知を SSE で送る。
// 接続直後は最新の状態を送り、以降は未通知の状態遷移を古い順にすべて送ってから、次の変化を待つ。
// next は送るべき通知が無ければ nil を返す。resendLatest のときは未通知が無くても最新の状態を返す。
func streamNotifications(w http.ResponseWriter, r *http.Request, wake <-chan struct{}, sentColumn string, next func(resendLatest bool) (any, *rideStatusState)) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, errors.New("streaming is not supported"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	// nginx にバッファリングさせない
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()
	resendLatest := true
	for {
		data, sent := next(resendLatest)
		resendLatest = false
		if data != nil {
			b, err := json.Marshal(data)
			if err != nil {
				slog.Error("failed to marshal notification", "error", err)
				return
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return
			}
			flusher.Flush()
		}
		if sent != nil {
			enqueueSentAt(sentColumn, sent.id, now())
			// 続けて未通知の状態があれば送る
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-wake:
		}
	}
}
