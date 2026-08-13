package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

// よくある失敗を表す番兵エラーです。
// バックエンドはこれらを包んで返し、呼び出し側は errors.Is で判定します。
var (
	// ErrNotFound は対象が存在しないことを表します。
	ErrNotFound = errors.New("見つかりません")
	// ErrIsDir はファイルを期待した場所がディレクトリだったことを表します。
	ErrIsDir = errors.New("ディレクトリです")
	// ErrNotDir はディレクトリを期待した場所がファイルだったことを表します。
	ErrNotDir = errors.New("ディレクトリではありません")
	// ErrExist はすでに存在することを表します。
	ErrExist = errors.New("すでに存在します")
	// ErrNotEmpty はディレクトリが空でないことを表します。
	ErrNotEmpty = errors.New("空ではありません")
	// ErrUnsupported はそのストレージが対応していない操作であることを表します。
	ErrUnsupported = errors.New("対応していない操作です")
)

// Class は失敗の種類です。再試行してよいかを決めるのに使います。
type Class int

const (
	// ClassUnknown は分類できなかったことを表します。
	// 保守的に再試行の対象とします。
	ClassUnknown Class = iota
	// ClassPermanent は待っても直らない失敗です。
	// 存在しない、権限がない、パスが不正、といったものです。
	ClassPermanent
	// ClassRetryable は一時的な失敗です。ネットワークの瞬断や 5xx など。
	ClassRetryable
	// ClassRateLimit は要求が多すぎることによる拒否です。
	// 間隔を空けて再試行します。
	ClassRateLimit
	// ClassAuth は認証・認可の失敗です。
	// 再試行しても直らないので、処理全体を止めます。
	ClassAuth
	// ClassCanceled は取り消されたことを表します。再試行しません。
	ClassCanceled
)

func (c Class) String() string {
	switch c {
	case ClassPermanent:
		return "permanent"
	case ClassRetryable:
		return "retryable"
	case ClassRateLimit:
		return "ratelimit"
	case ClassAuth:
		return "auth"
	case ClassCanceled:
		return "canceled"
	}
	return "unknown"
}

// Retryable は、この種類の失敗を再試行してよいかを返します。
func (c Class) Retryable() bool {
	return c == ClassRetryable || c == ClassRateLimit || c == ClassUnknown
}

// OpError は、どのストレージのどの操作が失敗したかを表すエラーです。
//
// 失敗の分類はバックエンドの中で行い、呼び出し側は errors.As で
// この型を取り出すだけで済むようにします。SDK ごとのエラー型を
// 転送エンジンが知る必要はありません。
type OpError struct {
	// Op は操作の名前です（"list", "open", "put" など）。
	Op string
	// Storage はストレージの名前です。
	Storage string
	// Path は対象のパスです。
	Path string
	// Class は失敗の種類です。
	Class Class
	// RetryAfter は、サーバーから待ち時間を指示された場合にその値です。
	RetryAfter time.Duration
	Err        error
}

func (e *OpError) Error() string {
	if e.Path == "" {
		return fmt.Sprintf("%s %s: %v", e.Storage, e.Op, e.Err)
	}
	return fmt.Sprintf("%s:%s の %s に失敗しました: %v", e.Storage, e.Path, e.Op, e.Err)
}

func (e *OpError) Unwrap() error { return e.Err }

// Wrap は操作の失敗を OpError で包みます。err が nil なら nil を返します。
func Wrap(op, storageName, path string, class Class, err error) error {
	if err == nil {
		return nil
	}
	return &OpError{
		Op:      op,
		Storage: storageName,
		Path:    path,
		Class:   class,
		Err:     err,
	}
}

// ClassOf はエラーの種類を判定します。
//
// OpError に分類が入っていればそれを使います。入っていない場合は、
// 取り消しやネットワークの状態から一般的な判定を行います。
func ClassOf(err error) Class {
	if err == nil {
		return ClassUnknown
	}

	var opErr *OpError
	if errors.As(err, &opErr) && opErr.Class != ClassUnknown {
		return opErr.Class
	}

	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ClassCanceled
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrIsDir),
		errors.Is(err, ErrNotDir), errors.Is(err, ErrUnsupported):
		return ClassPermanent
	case errors.Is(err, io.ErrUnexpectedEOF):
		return ClassRetryable
	}

	// 一時的なネットワークの失敗は再試行してよい。
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ClassRetryable
	}

	return ClassUnknown
}

// RetryAfterOf は、サーバーから指示された待ち時間を返します。
// 指示がなければ 0 を返します。
func RetryAfterOf(err error) time.Duration {
	var opErr *OpError
	if errors.As(err, &opErr) {
		return opErr.RetryAfter
	}
	return 0
}
