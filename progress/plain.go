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

	// stat は表示に使う集計です。Bars と同じものを使います。
	stat *stats

	mu       sync.Mutex
	lastStat time.Time
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
		stat:          newStats(),
		lastStat:      time.Now(),
	}
}

func (p *Plain) ScanStarted() {
	p.stat.beginScan()

	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintln(p.w, "コピー対象を調べています...")
}

func (p *Plain) ScanProgress(dirs, files int64, bytes int64) {
	p.stat.scanProgress(dirs, files, bytes)
	p.maybeStats()
}

func (p *Plain) ScanDone(dirs, files int64, bytes int64) {
	p.stat.endScan(dirs, files, bytes)

	p.mu.Lock()
	defer p.mu.Unlock()

	// 何件を転送しないと判断したのかも書く。
	// 進みぐあいが速い理由がここで分かる。
	if n := p.stat.skipFiles.Load(); n > 0 {
		fmt.Fprintf(p.w, "調査が終わりました: %d件 / %s（うち %d件 / %s は転送不要）\n",
			p.stat.scanFiles.Load(), HumanBytes(p.stat.scanBytes.Load()),
			n, HumanBytes(p.stat.skipBytes.Load()))
		return
	}
	fmt.Fprintf(p.w, "調査が終わりました: %d件 / %s\n",
		p.stat.scanFiles.Load(), HumanBytes(p.stat.scanBytes.Load()))
}

func (p *Plain) StartFile(name string, size int64) FileTracker {
	return &plainTracker{p: p, name: name, size: size, started: time.Now()}
}

// Skipped は転送不要と判断したことを記録します。
//
// 1件ずつ書くと量が多くなるので、集計にだけ入れます。
// 集計に入れておかないと、進みぐあいも残り時間も当てになりません。
func (p *Plain) Skipped(_ string, size int64) {
	p.stat.skip(size)
}

func (p *Plain) Logf(format string, a ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w, format+"\n", a...)
}

func (p *Plain) Done(s Summary) {
	p.stat.finished.Store(true)

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
	p.maybeStatsLocked()
}

// maybeStatsLocked はロックを取得済みの状態で集計行を書きます。
func (p *Plain) maybeStatsLocked() {
	if p.statsInterval <= 0 || time.Since(p.lastStat) < p.statsInterval {
		return
	}
	p.lastStat = time.Now()

	line := fmt.Sprintf("%d/%d件 %s/%s %s %s",
		p.stat.doneFiles.Load(), p.stat.scanFiles.Load(),
		HumanBytes(p.stat.dealtBytes()), HumanBytes(p.stat.scanBytes.Load()),
		p.stat.rateText(), p.stat.etaText())

	if n := p.stat.skipFiles.Load(); n > 0 {
		line += fmt.Sprintf("（スキップ %d件）", n)
	}
	if p.stat.scanning.Load() {
		line += "（調査中）"
	}
	fmt.Fprintln(p.w, line)
}

// plainTracker は Plain 用の記録係です。
type plainTracker struct {
	p       *Plain
	name    string
	size    int64
	started time.Time

	read    atomic.Int64
	aborted atomic.Bool
}

func (t *plainTracker) Wrap(r io.Reader) io.Reader {
	return &countingReader{r: r, onRead: func(n int, _ time.Duration) {
		t.read.Add(int64(n))
		t.p.stat.doneBytes.Add(int64(n))
	}}
}

func (t *plainTracker) Reset() {
	// 再試行では、いったん数えたぶんを取り消す。
	n := t.read.Swap(0)
	t.p.stat.doneBytes.Add(-n)
}

func (t *plainTracker) Complete(n int64) {
	t.read.Add(n)
	t.p.stat.doneBytes.Add(n)
}

func (t *plainTracker) Abort() { t.aborted.Store(true) }

func (t *plainTracker) Finish() {
	elapsed := time.Since(t.started)
	n := t.read.Load()

	if t.aborted.Load() {
		t.p.stat.failedFiles.Add(1)
	} else {
		t.p.stat.doneFiles.Add(1)
	}
	// 読めなかった残りも片付いたぶんとして数える。
	// 数えないと、失敗したファイルのぶんだけ進みぐあいが足りなくなる。
	if rest := t.size - n; rest > 0 {
		t.p.stat.settledBytes.Add(rest)
	}

	if t.aborted.Load() {
		return
	}

	t.p.mu.Lock()
	defer t.p.mu.Unlock()
	fmt.Fprintf(t.p.w, "コピー %s  %s  %s\n", t.name, HumanBytes(n), HumanRate(n, elapsed))
	t.p.maybeStatsLocked()
}
