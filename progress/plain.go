package progress

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Plain は1行ずつ書き出す Reporter です。
//
// 端末でない場合（パイプ、ジョブ、CI）に使います。
// 制御文字を書かないので、ログに残しても読めます。
type Plain struct {
	w io.Writer
	// StatsInterval ごとに集計行を出します。0 なら出しません。
	statsInterval time.Duration

	mu       sync.Mutex
	started  time.Time
	lastStat time.Time

	// 走査の途中経過
	scanDirs  atomic.Int64
	scanFiles atomic.Int64
	scanBytes atomic.Int64
	scanDone  atomic.Bool

	// 転送の途中経過
	doneFiles atomic.Int64
	doneBytes atomic.Int64
}

// PlainOptions は Plain の設定です。
type PlainOptions struct {
	// Writer は書き出し先です。nil なら標準エラー出力。
	Writer io.Writer
	// StatsInterval ごとに集計行を出します。0 なら出しません。
	StatsInterval time.Duration
}

// NewPlain は行ログ形式の Reporter を返します。
func NewPlain(opts PlainOptions) *Plain {
	w := opts.Writer
	if w == nil {
		w = defaultWriter()
	}
	return &Plain{
		w:             w,
		statsInterval: opts.StatsInterval,
		started:       time.Now(),
		lastStat:      time.Now(),
	}
}

func (p *Plain) ScanStarted() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.started = time.Now()
	fmt.Fprintln(p.w, "コピー対象を調べています...")
}

func (p *Plain) ScanProgress(dirs, files int64, bytes int64) {
	p.scanDirs.Store(dirs)
	p.scanFiles.Store(files)
	p.scanBytes.Store(bytes)
	p.maybeStats()
}

func (p *Plain) ScanDone(dirs, files int64, bytes int64) {
	p.scanDirs.Store(dirs)
	p.scanFiles.Store(files)
	p.scanBytes.Store(bytes)
	p.scanDone.Store(true)

	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w, "調査が終わりました: %d件 / %s\n", files, HumanBytes(bytes))
}

func (p *Plain) StartFile(name string, size int64) FileTracker {
	return &plainTracker{p: p, name: name, size: size, started: time.Now()}
}

func (p *Plain) Skipped(string, int64) {
	// 転送しないものを1件ずつ書くと量が多くなるので、集計だけにする。
}

func (p *Plain) Logf(format string, a ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w, format+"\n", a...)
}

func (p *Plain) Done(s Summary) {
	p.mu.Lock()
	defer p.mu.Unlock()

	fmt.Fprintf(p.w, "\n転送: %d件 / %s", s.Transferred, HumanBytes(s.Bytes))
	if s.Elapsed > 0 {
		fmt.Fprintf(p.w, " (%s, 平均 %s)", s.Elapsed.Round(time.Millisecond), HumanRate(s.Bytes, s.Elapsed))
	}
	fmt.Fprintln(p.w)

	if s.Skipped > 0 {
		fmt.Fprintf(p.w, "スキップ: %d件 / %s\n", s.Skipped, HumanBytes(s.BytesSkipped))
	}
	if s.Failed > 0 {
		fmt.Fprintf(p.w, "失敗: %d件\n", s.Failed)
	}
}

func (p *Plain) Close() error { return nil }

// maybeStats は、間隔を過ぎていれば集計行を書きます。
func (p *Plain) maybeStats() {
	if p.statsInterval <= 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if time.Since(p.lastStat) < p.statsInterval {
		return
	}
	p.lastStat = time.Now()

	done := p.doneFiles.Load()
	total := p.scanFiles.Load()
	doneBytes := p.doneBytes.Load()
	totalBytes := p.scanBytes.Load()

	suffix := ""
	if !p.scanDone.Load() {
		suffix = "（調査中）"
	}
	fmt.Fprintf(p.w, "%d/%d件 %s/%s %s%s\n",
		done, total, HumanBytes(doneBytes), HumanBytes(totalBytes),
		HumanRate(doneBytes, time.Since(p.started)), suffix)
}

// plainTracker は Plain 用の記録係です。
type plainTracker struct {
	p       *Plain
	name    string
	size    int64
	started time.Time
	read    atomic.Int64
}

func (t *plainTracker) Wrap(r io.Reader) io.Reader {
	return &countingReader{r: r, onRead: func(n int) {
		t.read.Add(int64(n))
		t.p.doneBytes.Add(int64(n))
	}}
}

func (t *plainTracker) Reset() {
	// 再試行では、いったん数えたぶんを取り消す。
	n := t.read.Swap(0)
	t.p.doneBytes.Add(-n)
}

func (t *plainTracker) Complete(n int64) {
	t.read.Add(n)
	t.p.doneBytes.Add(n)
}

func (t *plainTracker) Abort() {}

func (t *plainTracker) Finish() {
	t.p.doneFiles.Add(1)

	elapsed := time.Since(t.started)
	n := t.read.Load()

	t.p.mu.Lock()
	defer t.p.mu.Unlock()
	fmt.Fprintf(t.p.w, "コピー %s  %s  %s\n", t.name, HumanBytes(n), HumanRate(n, elapsed))
	t.p.maybeStatsLocked()
}

// maybeStatsLocked はロックを取得済みの状態で集計行を書きます。
func (p *Plain) maybeStatsLocked() {
	if p.statsInterval <= 0 || time.Since(p.lastStat) < p.statsInterval {
		return
	}
	p.lastStat = time.Now()

	done := p.doneFiles.Load()
	total := p.scanFiles.Load()
	fmt.Fprintf(p.w, "%d/%d件 %s %s\n", done, total,
		HumanBytes(p.doneBytes.Load()), HumanRate(p.doneBytes.Load(), time.Since(p.started)))
}

// countingReader は読んだ量を数える Reader です。
type countingReader struct {
	r      io.Reader
	onRead func(n int)
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 && c.onRead != nil {
		c.onRead(n)
	}
	return n, err
}
