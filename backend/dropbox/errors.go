package dropbox

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	dbxapi "github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox"
	dbxauth "github.com/dropbox/dropbox-sdk-go-unofficial/v6/dropbox/auth"
	"github.com/mt3hr/hbg/storage"
)

// Dropbox API のエラーは、HTTP の状態コードと error_summary の2段で表されます。
//
//	401 → 認証が通っていない
//	403 → 権限がない
//	409 → 操作そのものの失敗（error_summary に理由が入る）
//	429 → 要求が多すぎる（retry_after 秒待つ）
//	5xx → 一時的な障害
//
// 409 の理由は "path/not_found/..." のように "/" 区切りで並びます。
// 末尾は将来増えうるので、先頭側の語だけを見て判断します。

// wrapErr は Dropbox のエラーを storage のエラーに変換します。
//
// 呼び出し側が SDK のエラー型を知らずに済むよう、ここで
// 番兵エラーと失敗の種類を決めてしまいます。
func (d *Storage) wrapErr(op, path string, err error) error {
	if err == nil {
		return nil
	}

	v := classify(err)
	if v.sentinel != nil {
		// 元のエラーも失わないよう、両方を包む。
		err = fmt.Errorf("%w (%w)", v.sentinel, err)
	}

	return &storage.OpError{
		Op:         op,
		Storage:    d.name,
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
		return verdict{class: storage.ClassPermanent}
	}

	var rateErr dbxauth.RateLimitAPIError
	if errors.As(err, &rateErr) {
		wait := time.Duration(0)
		if rateErr.RateLimitError != nil {
			wait = time.Duration(rateErr.RateLimitError.RetryAfter) * time.Second
		}
		return verdict{class: storage.ClassRateLimit, retryAfter: wait}
	}

	var authErr dbxauth.AuthAPIError
	if errors.As(err, &authErr) {
		return verdict{class: storage.ClassAuth}
	}
	var accessErr dbxauth.AccessAPIError
	if errors.As(err, &accessErr) {
		return verdict{class: storage.ClassAuth}
	}
	var serverErr dbxauth.ServerError
	if errors.As(err, &serverErr) {
		return verdict{class: storage.ClassRetryable}
	}
	var badReq dbxauth.BadRequest
	if errors.As(err, &badReq) {
		return verdict{class: storage.ClassPermanent}
	}

	// ここから先は 409。error_summary で理由を判断する。
	summary := summaryOf(err)
	if summary == "" {
		// 分類できないものは保守的に再試行の対象にする。
		return verdict{class: storage.ClassUnknown}
	}
	return classifySummary(summary)
}

// classifySummary は error_summary から判断します。
func classifySummary(summary string) verdict {
	for _, w := range strings.Split(summary, "/") {
		switch w {
		case "not_found":
			return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
		case "not_folder":
			return verdict{sentinel: storage.ErrNotDir, class: storage.ClassPermanent}
		case "not_file":
			return verdict{sentinel: storage.ErrIsDir, class: storage.ClassPermanent}
		case "conflict":
			return verdict{sentinel: storage.ErrExist, class: storage.ClassPermanent}
		case "restricted_content", "no_write_permission", "insufficient_space",
			"disallowed_name", "malformed_path", "unsupported_file",
			"team_folder", "invalid_path_root", "payload_too_large":
			return verdict{class: storage.ClassPermanent}
		case "too_many_write_operations", "too_many_requests", "too_many_shared_folder_targets":
			// 同時に書き込みすぎている。少し待てば通る。
			return verdict{class: storage.ClassRateLimit}
		}
	}
	return verdict{class: storage.ClassUnknown}
}

// summaryOf は Dropbox のエラーから error_summary を取り出します。
//
// SDK は経路ごとに別々のエラー型を生成します（GetMetadataAPIError など）。
// どれも dropbox.APIError を埋め込んでいますが、埋め込みは別の型なので
// errors.As では取り出せません。埋め込みの中身を直接見ます。
func summaryOf(err error) string {
	var apiErr dbxapi.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorSummary
	}

	for e := err; e != nil; e = errors.Unwrap(e) {
		if summary, ok := embeddedSummary(e); ok {
			return summary
		}
	}
	return ""
}

// embeddedSummary は、埋め込まれた dropbox.APIError から error_summary を取り出します。
func embeddedSummary(err error) (string, bool) {
	v := reflect.ValueOf(err)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return "", false
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return "", false
	}

	f := v.FieldByName("APIError")
	if !f.IsValid() || !f.CanInterface() {
		return "", false
	}
	api, ok := f.Interface().(dbxapi.APIError)
	if !ok {
		return "", false
	}
	return api.ErrorSummary, true
}

// isConflict は「すでに存在する」ことによる失敗かを返します。
// Mkdir を冪等にするのに使います。
func isConflict(err error) bool {
	return errors.Is(err, storage.ErrExist) ||
		strings.Contains(summaryOf(err), "conflict")
}

// isNotFound は「存在しない」ことによる失敗かを返します。
func isNotFound(err error) bool {
	return errors.Is(err, storage.ErrNotFound) ||
		strings.Contains(summaryOf(err), "not_found")
}
