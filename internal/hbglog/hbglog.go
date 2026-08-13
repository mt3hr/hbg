package hbglog

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/mt3hr/hbg/internal/hbghome"
)

// Mode は出力の分け方です。
type Mode int

const (
	// ModeMergedAndSplit は統合ファイルとレベル別ファイルの両方に書きます。
	ModeMergedAndSplit Mode = iota
	// ModeSplitOnly はレベル別ファイルにのみ書きます。
	ModeSplitOnly
	// ModeMergedOnly は統合ファイルにのみ書きます。
	ModeMergedOnly
)

// Options はログの設定です。
type Options struct {
	// Dir はログの出力先です。空なら $HOME/hbg/logs。
	Dir string
	// MinLevel はこれ未満のレベルを出力しません。
	MinLevel slog.Level
	// Mode は出力の分け方です。
	Mode Mode
	// MaxSizeBytes は 1 ファイルの上限です。0 なら既定値。
	MaxSizeBytes int64
	// MaxBackups は保持する世代数です。
	MaxBackups int
	// MaxAge はこれより古いログを削除します。0 なら削除しません。
	MaxAge time.Duration
	// Stdout に書き出すかどうか。
	Stdout bool
	// Version は全レコードに付ける hbg のバージョンです。
	Version string
}

// Logger はログの出力先をまとめたものです。
type Logger struct {
	handler *routingHandler
	logger  *slog.Logger
	closers []io.Closer
}

var (
	mu      sync.Mutex
	current *Logger
)

// Init はログを初期化し、slog の既定のロガーに設定します。
//
// MinLevel が LevelNone の場合はファイルを作らず、何も出力しません。
func Init(opts Options) (*Logger, error) {
	if opts.MinLevel >= LevelNone {
		l := &Logger{logger: slog.New(discardHandler{})}
		setCurrent(l)
		return l, nil
	}

	dir := opts.Dir
	if dir == "" {
		d, err := hbghome.LogsDir()
		if err != nil {
			return nil, err
		}
		dir = d
	}
	if err := hbghome.EnsureDir(dir); err != nil {
		return nil, err
	}
	removeOldLogs(dir, opts.MaxAge)

	l := &Logger{}
	h := &routingHandler{
		minLevel: opts.MinLevel,
		attrs: []slog.Attr{
			slog.String("app", "hbg"),
		},
	}
	if opts.Version != "" {
		h.attrs = append(h.attrs, slog.String("version", opts.Version))
	}

	newWriter := func(name string) (io.Writer, error) {
		rf, err := newRotatingFile(
			joinPath(dir, name),
			orDefaultInt64(opts.MaxSizeBytes, DefaultMaxSizeBytes),
			orDefaultInt(opts.MaxBackups, DefaultMaxBackups),
		)
		if err != nil {
			return nil, err
		}
		l.closers = append(l.closers, rf)
		return rf, nil
	}

	if opts.Mode != ModeSplitOnly {
		w, err := newWriter("hbg.log")
		if err != nil {
			return nil, err
		}
		h.merged = newJSONHandler(w, opts.MinLevel)
	}

	if opts.Mode != ModeMergedOnly {
		h.split = map[slog.Level]slog.Handler{}
		for _, e := range levelNames {
			if e.level < opts.MinLevel {
				continue
			}
			w, err := newWriter(levelFileName(e.level))
			if err != nil {
				return nil, err
			}
			h.split[e.level] = newJSONHandler(w, opts.MinLevel)
		}
	}

	if opts.Stdout {
		h.stdout = newJSONHandler(os.Stdout, opts.MinLevel)
	}

	l.handler = h
	l.logger = slog.New(h)
	setCurrent(l)
	return l, nil
}

func setCurrent(l *Logger) {
	mu.Lock()
	defer mu.Unlock()

	if current != nil {
		_ = current.Close()
	}
	current = l
	slog.SetDefault(l.logger)
}

// Close はログファイルを閉じます。
func (l *Logger) Close() error {
	var firstErr error
	for _, c := range l.closers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	l.closers = nil
	return firstErr
}

// Close は現在のロガーを閉じます。
func Close() error {
	mu.Lock()
	l := current
	current = nil
	mu.Unlock()

	if l == nil {
		return nil
	}
	return l.Close()
}

// newJSONHandler は JSON 形式のハンドラを作ります。
//
// gkill の実装はレコードごとにハンドラを生成しており、詳細なログを
// 出すときのコストが高くなっていました。ここでは初期化時に一度だけ作ります。
func newJSONHandler(w io.Writer, minLevel slog.Level) slog.Handler {
	return slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level:     minLevel,
		AddSource: true,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			// 独自レベルの名前が数値で出ないようにする
			if a.Key == slog.LevelKey {
				if lv, ok := a.Value.Any().(slog.Level); ok {
					a.Value = slog.StringValue(LevelName(lv))
				}
			}
			if a.Key == slog.TimeKey {
				if t, ok := a.Value.Any().(time.Time); ok {
					a.Value = slog.StringValue(t.Format(time.RFC3339Nano))
				}
			}
			return a
		},
	})
}

// routingHandler は 1 レコードを統合ファイル・レベル別ファイル・標準出力へ振り分けます。
type routingHandler struct {
	minLevel slog.Level
	merged   slog.Handler
	split    map[slog.Level]slog.Handler
	stdout   slog.Handler

	attrs  []slog.Attr
	groups []string
}

func (h *routingHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.minLevel
}

func (h *routingHandler) Handle(ctx context.Context, r slog.Record) error {
	var firstErr error
	handle := func(target slog.Handler) {
		if target == nil {
			return
		}
		// 属性とグループは Handle の直前に適用する。
		target = applyAttrs(target, h.attrs, h.groups)
		if err := target.Handle(ctx, r.Clone()); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	handle(h.merged)
	handle(h.stdout)
	if h.split != nil {
		handle(h.split[nearestLevel(r.Level)])
	}
	return firstErr
}

// WithAttrs は属性を追加したハンドラを返します。
//
// gkill の実装はここで属性を捨てており、共通フィールドが出力に現れて
// いませんでした。ここでは保持して Handle 時に適用します。
func (h *routingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	clone := *h
	clone.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &clone
}

// WithGroup はグループを追加したハンドラを返します。
func (h *routingHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	clone := *h
	clone.groups = append(append([]string{}, h.groups...), name)
	return &clone
}

func applyAttrs(target slog.Handler, attrs []slog.Attr, groups []string) slog.Handler {
	if len(attrs) > 0 {
		target = target.WithAttrs(attrs)
	}
	for _, g := range groups {
		target = target.WithGroup(g)
	}
	return target
}

// nearestLevel は、レコードのレベルを分割ファイルのレベルに対応づけます。
// 定義された値と一致しない場合は、それ以下でもっとも近いレベルを返します。
func nearestLevel(l slog.Level) slog.Level {
	best := levelNames[0].level
	for _, e := range levelNames {
		if e.level <= l && e.level >= best {
			best = e.level
		}
	}
	return best
}

// discardHandler は何も出力しないハンドラです。
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (d discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return d }
func (d discardHandler) WithGroup(string) slog.Handler           { return d }

func joinPath(dir, name string) string {
	return fmt.Sprintf("%s%c%s", dir, os.PathSeparator, name)
}

func orDefaultInt64(v, def int64) int64 {
	if v <= 0 {
		return def
	}
	return v
}

func orDefaultInt(v, def int) int {
	if v < 0 {
		return def
	}
	return v
}
