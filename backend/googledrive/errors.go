package googledrive

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/mt3hr/hbg/storage"
	"google.golang.org/api/googleapi"
)

// Google API のエラーは HTTP の状態コードと reason の2段で表されます。
//
//	401 → 認証が通っていない
//	403 → 権限がないか、要求が多すぎる（reason で区別する）
//	404 → 存在しない
//	429 → 要求が多すぎる
//	5xx → 一時的な障害
//
// 403 が権限と流量制限の両方に使われるのが厄介なところで、
// reason を見ないと「待てば直るのか」を判断できません。

// wrapErr は Drive のエラーを storage のエラーに変換します。
func (g *Storage) wrapErr(op, path string, err error) error {
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
		Storage:    g.name,
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
	case errors.Is(err, storage.ErrNotFound):
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case errors.Is(err, storage.ErrUnsupported), errors.Is(err, storage.ErrIsDir),
		errors.Is(err, storage.ErrNotDir), errors.Is(err, storage.ErrNotEmpty),
		errors.Is(err, storage.ErrExist):
		return verdict{class: storage.ClassPermanent}
	}

	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		return verdict{class: storage.ClassUnknown}
	}
	return classifyStatus(apiErr.Code, reasonOf(apiErr))
}

// classifyStatus は状態コードと reason から判断します。
func classifyStatus(code int, reason string) verdict {
	switch code {
	case http.StatusNotFound:
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case http.StatusUnauthorized:
		return verdict{class: storage.ClassAuth}
	case http.StatusTooManyRequests:
		return verdict{class: storage.ClassRateLimit}
	case http.StatusForbidden:
		// 403 は権限と流量制限の両方に使われる。
		// reason を見ないと、待てば直るのかどうかが分からない。
		switch reason {
		case "rateLimitExceeded", "userRateLimitExceeded", "sharingRateLimitExceeded":
			return verdict{class: storage.ClassRateLimit}
		case "storageQuotaExceeded", "quotaExceeded", "teamDriveFileLimitExceeded":
			// 容量が足りない。待っても直らない。
			return verdict{class: storage.ClassPermanent}
		case "appNotAuthorizedToFile", "insufficientFilePermissions", "domainPolicy":
			return verdict{class: storage.ClassAuth}
		}
		return verdict{class: storage.ClassPermanent}
	case http.StatusBadRequest, http.StatusConflict, http.StatusPreconditionFailed:
		return verdict{class: storage.ClassPermanent}
	}

	if code >= 500 && code <= 599 {
		return verdict{class: storage.ClassRetryable}
	}
	return verdict{class: storage.ClassUnknown}
}

// reasonOf はエラーの reason を取り出します。
func reasonOf(err *googleapi.Error) string {
	for _, e := range err.Errors {
		if e.Reason != "" {
			return e.Reason
		}
	}
	return ""
}
