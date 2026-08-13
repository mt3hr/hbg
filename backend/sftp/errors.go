package sftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/mt3hr/hbg/storage"
	sftpc "github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// SFTP version 3 のエラーは番号がごく少なく、たいていのことが
// SSH_FX_FAILURE（4）にまとめられてしまいます。
// 判断できるものだけを判断し、残りは保守的に再試行の対象とします。

// wrapErr は SFTP のエラーを storage のエラーに変換します。
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
	case errors.Is(err, storage.ErrNotEmpty):
		return verdict{class: storage.ClassPermanent}
	case errors.Is(err, storage.ErrIsDir), errors.Is(err, storage.ErrNotDir):
		return verdict{class: storage.ClassPermanent}
	case errors.Is(err, os.ErrNotExist):
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case errors.Is(err, os.ErrPermission):
		return verdict{class: storage.ClassPermanent}
	}

	var status *sftpc.StatusError
	if errors.As(err, &status) {
		return classifyStatus(status.Code)
	}

	// 接続そのものが切れた場合。繋ぎ直せば通ることがある。
	var openErr *ssh.OpenChannelError
	switch {
	case errors.As(err, &openErr):
		return verdict{class: storage.ClassRetryable}
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
		return verdict{class: storage.ClassRetryable}
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return verdict{class: storage.ClassRetryable}
	}

	return verdict{class: storage.ClassUnknown}
}

// classifyStatus は SFTP の状態番号から判断します。
func classifyStatus(code uint32) verdict {
	switch code {
	case uint32(sftpc.ErrSSHFxNoSuchFile):
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case uint32(sftpc.ErrSSHFxPermissionDenied):
		return verdict{class: storage.ClassPermanent}
	case uint32(sftpc.ErrSSHFxOpUnsupported):
		return verdict{sentinel: storage.ErrUnsupported, class: storage.ClassPermanent}
	case uint32(sftpc.ErrSSHFxNoConnection), uint32(sftpc.ErrSSHFxConnectionLost):
		return verdict{class: storage.ClassRetryable}
	case uint32(sftpc.ErrSSHFxFailure):
		// version 3 では、容量不足も名前の衝突も空でないディレクトリも
		// すべてこの番号になる。待てば直るとは限らないが、
		// 一時的なものである可能性も残るので決めつけない。
		return verdict{class: storage.ClassUnknown}
	}
	return verdict{class: storage.ClassUnknown}
}
