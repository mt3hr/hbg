package smb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/mt3hr/hbg/storage"
)

// SMB の失敗は NTSTATUS という番号で返りますが、go-smb2 はそれを
// os のエラーに寄せて返してくれます。判断できるものはそこから、
// 残りは番号の名前を見て判断します。

// wrapErr は SMB のエラーを storage のエラーに変換します。
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
		errors.Is(err, storage.ErrNotDir), errors.Is(err, storage.ErrExist):
		return verdict{class: storage.ClassPermanent}
	case errors.Is(err, os.ErrNotExist):
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case errors.Is(err, os.ErrPermission):
		return verdict{class: storage.ClassAuth}
	case errors.Is(err, os.ErrExist):
		return verdict{sentinel: storage.ErrExist, class: storage.ClassPermanent}
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

	return classifyStatusName(err.Error())
}

// classifyStatusName は NTSTATUS の名前から判断します。
//
// go-smb2 は番号を文字列として返してくるので、そこから読み取ります。
func classifyStatusName(message string) verdict {
	switch {
	case containsAny(message,
		"STATUS_OBJECT_NAME_NOT_FOUND", "STATUS_OBJECT_PATH_NOT_FOUND",
		"STATUS_NO_SUCH_FILE", "STATUS_BAD_NETWORK_NAME"):
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}

	case containsAny(message, "STATUS_OBJECT_NAME_COLLISION"):
		return verdict{sentinel: storage.ErrExist, class: storage.ClassPermanent}

	case containsAny(message, "STATUS_DIRECTORY_NOT_EMPTY"):
		return verdict{sentinel: storage.ErrNotEmpty, class: storage.ClassPermanent}

	case containsAny(message, "STATUS_NOT_A_DIRECTORY"):
		return verdict{sentinel: storage.ErrNotDir, class: storage.ClassPermanent}

	case containsAny(message, "STATUS_FILE_IS_A_DIRECTORY"):
		return verdict{sentinel: storage.ErrIsDir, class: storage.ClassPermanent}

	case containsAny(message,
		"STATUS_ACCESS_DENIED", "STATUS_LOGON_FAILURE", "STATUS_ACCOUNT_DISABLED",
		"STATUS_PASSWORD_EXPIRED", "STATUS_WRONG_PASSWORD", "STATUS_ACCOUNT_LOCKED_OUT"):
		return verdict{class: storage.ClassAuth}

	case containsAny(message, "STATUS_DISK_FULL", "STATUS_MEDIA_WRITE_PROTECTED",
		"STATUS_OBJECT_NAME_INVALID", "STATUS_NAME_TOO_LONG"):
		return verdict{class: storage.ClassPermanent}

	case containsAny(message,
		"STATUS_SHARING_VIOLATION", "STATUS_FILE_LOCK_CONFLICT",
		"STATUS_LOCK_NOT_GRANTED", "STATUS_NETWORK_BUSY",
		"STATUS_INSUFF_SERVER_RESOURCES", "STATUS_CONNECTION_DISCONNECTED",
		"STATUS_USER_SESSION_DELETED"):
		// 誰かが使っている、あるいは相手が混んでいる。待てば通る。
		return verdict{class: storage.ClassRetryable}
	}

	return verdict{class: storage.ClassUnknown}
}

func containsAny(s string, needles ...string) bool {
	for _, n := range needles {
		if strings.Contains(s, n) {
			return true
		}
	}
	return false
}

// isNotExist はエラーが「存在しない」を表すかを返します。
func isNotExist(err error) bool {
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, storage.ErrNotFound) {
		return true
	}
	return classify(err).sentinel != nil &&
		errors.Is(classify(err).sentinel, storage.ErrNotFound)
}
