package ftp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"

	"net/textproto"

	"github.com/mt3hr/hbg/storage"
)

// FTP の失敗は3桁の番号で返ります。
//
//	421 サービスが使えない（接続が切れる）
//	425 / 426 データの通信路がうまくいかなかった
//	450 / 550 ファイルが使えない（存在しない、権限がない）
//	452 / 552 容量が足りない
//	530 ログインできていない
//	5xx そのほかの誤り
//
// 番号の意味は相手によって幅があるので、判断できるものだけを判断し、
// 残りは保守的に扱います。

// wrapErr は FTP のエラーを storage のエラーに変換します。
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
		errors.Is(err, storage.ErrNotDir), errors.Is(err, storage.ErrExist),
		errors.Is(err, storage.ErrUnsupported):
		return verdict{class: storage.ClassPermanent}
	case errors.Is(err, storage.ErrNotFound):
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	}

	if code, ok := statusCode(err); ok {
		return classifyCode(code)
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

// statusCode はエラーから FTP の3桁の番号を取り出します。
//
// jlaffaye/ftp は net/textproto.Error をそのまま返してきます。
// 独自の型に包み直さないので、こちらで取り出します。
func statusCode(err error) (int, bool) {
	var protoErr *textproto.Error
	if errors.As(err, &protoErr) {
		return protoErr.Code, true
	}

	// 番号だけが文字列で返ってくることもある。
	return leadingCode(err.Error())
}

// leadingCode は "550 Not found" のような文字列から番号を読み取ります。
func leadingCode(message string) (int, bool) {
	if len(message) < 3 {
		return 0, false
	}
	code := 0
	for i := range 3 {
		c := message[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		code = code*10 + int(c-'0')
	}
	return code, true
}

// classifyCode は3桁の番号から判断します。
func classifyCode(code int) verdict {
	switch code {
	case 450, 550:
		// ファイルが使えない。存在しないことがほとんど。
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case 530:
		return verdict{class: storage.ClassAuth}
	case 421:
		// サービスが使えない。接続が切れる。
		return verdict{class: storage.ClassRetryable}
	case 425, 426:
		// データの通信路がうまくいかなかった。
		return verdict{class: storage.ClassRetryable}
	case 452, 552:
		// 容量が足りない。待っても直らない。
		return verdict{class: storage.ClassPermanent}
	case 553:
		// 名前が受け付けられない。
		return verdict{class: storage.ClassPermanent}
	case 500, 501, 502, 504:
		// 命令そのものが通らない。
		return verdict{sentinel: storage.ErrUnsupported, class: storage.ClassPermanent}
	}

	switch {
	case code >= 400 && code < 500:
		// 一時的な断り。
		return verdict{class: storage.ClassRetryable}
	case code >= 500 && code < 600:
		return verdict{class: storage.ClassPermanent}
	}
	return verdict{class: storage.ClassUnknown}
}

// isNotFound はエラーが「存在しない」を表すかを返します。
func isNotFound(err error) bool {
	v := classify(err)
	return v.sentinel != nil && errors.Is(v.sentinel, storage.ErrNotFound)
}

// isConnectionBroken は、その接続を使い回せなくなったかを返します。
//
// 壊れた接続を戻すと、次に借りた側まで巻き添えになります。
func isConnectionBroken(err error) bool {
	if err == nil {
		return false
	}

	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
		return true
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		// 途中でやめた場合、通信路の状態が分からない。
		return true
	}

	if code, ok := statusCode(err); ok && code == 421 {
		return true
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	// 応答の形が壊れている場合も、以降のやりとりが噛み合わなくなる。
	return strings.Contains(err.Error(), "unexpected response")
}
