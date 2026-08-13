package webdav

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"

	"github.com/mt3hr/hbg/storage"
)

// WebDAV の失敗は HTTP の状態コードで返ります。
// 手続きによって同じコードの意味が変わるものもあるので、
// そこは呼び出し側で見分けます（MKCOL の 405 など）。

// wrapErr は WebDAV のエラーを storage のエラーに変換します。
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
		Op:      op,
		Storage: s.name,
		Path:    path,
		Class:   v.class,
		Err:     err,
	}
}

// verdict は失敗の見立てです。
type verdict struct {
	// sentinel は対応する番兵エラーです。該当するものがなければ nil です。
	sentinel error
	class    storage.Class
}

// classify はエラーの見立てを求めます。
func classify(err error) verdict {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return verdict{class: storage.ClassCanceled}
	case errors.Is(err, storage.ErrNotEmpty), errors.Is(err, storage.ErrIsDir),
		errors.Is(err, storage.ErrNotDir):
		return verdict{class: storage.ClassPermanent}
	case errors.Is(err, os.ErrNotExist):
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case errors.Is(err, os.ErrPermission):
		return verdict{class: storage.ClassAuth}
	}

	if status := statusOfDav(err); status != 0 {
		return classifyStatus(status)
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

// statusOfDav はエラーから HTTP の状態コードを取り出します。
// 状態コードによる失敗でなければ 0 を返します。
func statusOfDav(err error) int {
	var davErr *davError
	if errors.As(err, &davErr) {
		return davErr.Status
	}
	return 0
}

// classifyStatus は状態コードから判断します。
func classifyStatus(status int) verdict {
	switch status {
	case http.StatusNotFound, http.StatusGone:
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case http.StatusUnauthorized, http.StatusForbidden:
		return verdict{class: storage.ClassAuth}
	case http.StatusMethodNotAllowed:
		// MKCOL をすでにあるものに対して行った場合など。
		return verdict{sentinel: storage.ErrExist, class: storage.ClassPermanent}
	case http.StatusConflict:
		// 親のディレクトリがない場合にこれが返る。
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case http.StatusPreconditionFailed:
		return verdict{sentinel: storage.ErrExist, class: storage.ClassPermanent}
	case http.StatusLocked, http.StatusFailedDependency:
		// 誰かが編集中。待てば通る。
		return verdict{class: storage.ClassRetryable}
	case http.StatusTooManyRequests:
		return verdict{class: storage.ClassRateLimit}
	case http.StatusRequestEntityTooLarge, http.StatusInsufficientStorage:
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
