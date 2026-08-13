package progress

import (
	"io"
	"os"
	"time"

	"golang.org/x/term"
)

// Mode は進捗の表示方法です。
type Mode string

const (
	// ModeAuto は端末かどうかで自動的に決めます。
	ModeAuto Mode = "auto"
	// ModeAlways は常に端末向けの表示にします。
	ModeAlways Mode = "always"
	// ModeNever は常に行ログにします。
	ModeNever Mode = "never"
	// ModeNone は何も表示しません。
	ModeNone Mode = "none"
)

// ParseMode は文字列から表示方法を求めます。
func ParseMode(s string) (Mode, bool) {
	switch Mode(s) {
	case ModeAuto, ModeAlways, ModeNever, ModeNone:
		return Mode(s), true
	}
	return "", false
}

// ModeNames は指定できる値を返します。
func ModeNames() []string {
	return []string{string(ModeAuto), string(ModeAlways), string(ModeNever), string(ModeNone)}
}

// isTerminal は書き出し先が端末かどうかを返します。
func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}

	// TERM=dumb は「装飾を解釈できない端末」を意味する。
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	// ジョブとして実行されている場合は行ログのほうが読みやすい。
	if os.Getenv("CI") != "" {
		return false
	}

	return term.IsTerminal(int(f.Fd()))
}

// Options は Reporter の作り方です。
type Options struct {
	// Mode は表示方法です。
	Mode Mode
	// Writer は書き出し先です。nil なら標準エラー出力。
	Writer io.Writer
	// MaxBars は同時に出すファイルごとのバーの本数です。
	MaxBars int
	// StatsInterval は行ログのときに集計行を出す間隔です。
	StatsInterval time.Duration
}

// New は設定に合った Reporter を返します。
//
// 端末なら進捗バー、そうでなければ1行ずつの表示にします。
// パイプやジョブに制御文字を書き込まないためです。
func New(opts Options) Reporter {
	w := opts.Writer
	if w == nil {
		w = defaultWriter()
	}

	switch opts.Mode {
	case ModeNone:
		return NewNop()
	case ModeAlways:
		// 端末でなくても描くよう明示的に指示されている。
		return NewBars(BarsOptions{Writer: w, MaxBars: opts.MaxBars, ForceTTY: !isTerminal(w)})
	case ModeNever:
		return NewPlain(PlainOptions{Writer: w, StatsInterval: opts.StatsInterval})
	}

	if isTerminal(w) {
		return NewBars(BarsOptions{Writer: w, MaxBars: opts.MaxBars})
	}
	return NewPlain(PlainOptions{Writer: w, StatsInterval: opts.StatsInterval})
}
