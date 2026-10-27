package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

var erroredUpstream = errors.New("errored upstream")

// 既定の Transport は同一ホストへのアイドル接続を2本しか保持しないので広げる
var paymentClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 512,
		IdleConnTimeout:     90 * time.Second,
	},
	Timeout: 10 * time.Second,
}

type paymentGatewayPostPaymentRequest struct {
	Amount int `json:"amount"`
}

// Idempotency-Key を付けて決済を要求する。同じキーなら二重に決済されないので、成功するまで単純にリトライする
func requestPaymentGatewayPostPayment(ctx context.Context, paymentGatewayURL string, token string, idempotencyKey string, param *paymentGatewayPostPaymentRequest) error {
	b, err := json.Marshal(param)
	if err != nil {
		return err
	}

	var lastErr error
	for retry := 0; retry < 30; retry++ {
		if retry > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, paymentGatewayURL+"/payments", bytes.NewBuffer(b))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", idempotencyKey)

		res, err := paymentClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		io.Copy(io.Discard, res.Body)
		res.Body.Close()

		switch {
		case res.StatusCode == http.StatusNoContent:
			return nil
		case res.StatusCode >= 500 || res.StatusCode == http.StatusConflict:
			// 一時的な障害、または同じキーのリクエストが処理中
			lastErr = fmt.Errorf("[POST /payments] unexpected status code (%d)", res.StatusCode)
		default:
			// リクエスト内容の誤りはリトライしても成功しない
			return fmt.Errorf("[POST /payments] unexpected status code (%d). %w", res.StatusCode, erroredUpstream)
		}
	}

	return fmt.Errorf("%v. %w", lastErr, erroredUpstream)
}
