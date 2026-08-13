package onedrive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/mt3hr/hbg/storage"
)

// Graph の失敗は HTTP の状態コードと code の2段で表されます。
//
//	401 → 認証が通っていない
//	403 → 権限がない、または容量が足りない（code で見分ける）
//	404 → 存在しない
//	429 → 要求が多すぎる（Retry-After 秒待つ）
//	507 → 容量が足りない
//	5xx → 一時的な障害
//
// unknownTotal は分割送信で全体の大きさがまだ分からないことを表します。
const unknownTotal int64 = -1

// wrapErr は OneDrive のエラーを storage のエラーに変換します。
func (s *Storage) wrapErr(op, path string, err error) error {
	if err == nil {
		return nil
	}

	v := classify(err)
	if v.sentinel != nil && !errors.Is(err, v.sentinel) {
		// 元のエラーも失わないよう、両方を包む。
		err = fmt.Errorf("%w (%w)", v.sentinel, err)
	}

	return &storage.OpError{
		Op:         op,
		Storage:    s.name,
		Path:       path,
		Class:      v.class,
		RetryAfter: v.retryAfter,
		Err:        err,
	}
}

// verdict は失敗の見立てです。
type verdict struct {
	// sentinel は対応する番兵エラーです。該当するものがなければ nil です。
	sentinel error
	class    storage.Class
	// retryAfter はサーバーから指示された待ち時間です。
	retryAfter time.Duration
}

// classify はエラーの見立てを求めます。
func classify(err error) verdict {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return verdict{class: storage.ClassCanceled}
	case errors.Is(err, storage.ErrNotEmpty), errors.Is(err, storage.ErrIsDir),
		errors.Is(err, storage.ErrNotDir), errors.Is(err, storage.ErrExist):
		return verdict{class: storage.ClassPermanent}
	case errors.Is(err, storage.ErrNotFound):
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	}

	var graphErr *graphError
	if errors.As(err, &graphErr) {
		v := classifyStatus(graphErr.Status, graphErr.Code)
		v.retryAfter = graphErr.RetryAfter
		return v
	}

	// 接続そのものが切れた場合。繋ぎ直せば通ることがある。
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
		return verdict{class: storage.ClassRetryable}
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return verdict{class: storage.ClassRetryable}
	}

	return verdict{class: storage.ClassUnknown}
}

// classifyStatus は状態コードと code から判断します。
func classifyStatus(status int, code string) verdict {
	// code のほうが具体的なので先に見る。
	switch code {
	case "itemNotFound", "resourceNotFound":
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case "nameAlreadyExists":
		return verdict{sentinel: storage.ErrExist, class: storage.ClassPermanent}
	case "quotaLimitReached", "insufficientStorage":
		// 容量が足りない。待っても直らない。
		return verdict{class: storage.ClassPermanent}
	case "activityLimitReached", "throttledRequest":
		return verdict{class: storage.ClassRateLimit}
	case "unauthenticated", "invalidAuthenticationToken":
		return verdict{class: storage.ClassAuth}
	case "accessDenied":
		return verdict{class: storage.ClassAuth}
	case "resourceModified", "notAllowed", "malwareDetected":
		return verdict{class: storage.ClassPermanent}
	case "serviceNotAvailable", "unknownError":
		return verdict{class: storage.ClassRetryable}
	}

	switch status {
	case http.StatusNotFound:
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case http.StatusUnauthorized:
		return verdict{class: storage.ClassAuth}
	case http.StatusForbidden:
		return verdict{class: storage.ClassAuth}
	case http.StatusConflict:
		return verdict{sentinel: storage.ErrExist, class: storage.ClassPermanent}
	case http.StatusTooManyRequests:
		return verdict{class: storage.ClassRateLimit}
	case http.StatusInsufficientStorage:
		return verdict{class: storage.ClassPermanent}
	case http.StatusRequestEntityTooLarge, http.StatusBadRequest:
		return verdict{class: storage.ClassPermanent}
	case http.StatusRequestTimeout:
		return verdict{class: storage.ClassRetryable}
	}

	switch {
	case status >= 500 && status <= 599:
		return verdict{class: storage.ClassRetryable}
	case status >= 400 && status <= 499:
		return verdict{class: storage.ClassPermanent}
	}
	return verdict{class: storage.ClassUnknown}
}

// isNotFound はエラーが「存在しない」を表すかを返します。
func isNotFound(err error) bool {
	v := classify(err)
	return v.sentinel != nil && errors.Is(v.sentinel, storage.ErrNotFound)
}

// newBytesReader は読み終わった先頭ぶんを読み直せるようにします。
func newBytesReader(b []byte) io.Reader { return bytes.NewReader(b) }
