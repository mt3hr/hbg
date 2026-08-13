package progress

import (
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// Bars は端末に進捗バーを描く Reporter です。
type Bars struct {
	p   *mpb.Progress
	out io.Writer

	total *mpb.Bar
	scan  *mpb.Bar

	// 走査の途中経過
	scanDirs  atomic.Int64
	scanFiles atomic.Int64
	scanBytes atomic.Int64
	scanEnded atomic.Bool

	// ファイルごとのバーの本数を抑える
	mu      sync.Mutex
	visible int
	maxBars int
	// active は表示中のファイルごとのバーです。
	// 片付け忘れると mpb の Wait が終わらないので覚えておきます。
	active map[*mpb.Bar]struct{}

	closed atomic.Bool
}

// BarsOptions は Bars の設定です。
type BarsOptions struct {
	// Writer は書き出し先です。nil なら標準エラー出力。
	Writer io.Writer
	// MaxBars は同時に出すファイルごとのバーの本数です。0 なら既定値。
	MaxBars int
	// ForceTTY を真にすると、書き出し先が端末でなくてもバーを描きます。
	//
	// mpb は端末でない場合に自動更新を行わないので、
	// --progress always のように明示的に指示された場合はこれを立てます。
	ForceTTY bool
}

// DefaultMaxBars はファイルごとのバーの既定の本数です。
const DefaultMaxBars = 8

// NewBars は進捗バーを描く Reporter を返します。
func NewBars(opts BarsOptions) *Bars {
	w := opts.Writer
	if w == nil {
		w = defaultWriter()
	}
	maxBars := opts.MaxBars
	if maxBars <= 0 {
		maxBars = DefaultMaxBars
	}

	containerOpts := []mpb.ContainerOption{
		mpb.WithOutput(w),
		mpb.WithWidth(80),
		mpb.WithRefreshRate(150 * time.Millisecond),
		// 完了したバーは上へ流して消す
		mpb.PopCompletedMode(),
	}
	if opts.ForceTTY {
		// ForceTTY は書き出し先を端末として扱わせるだけで、
		// 自動更新は別に指定する必要がある。
		containerOpts = append(containerOpts, mpb.ForceTTY(), mpb.WithAutoRefresh())
	}

	p := mpb.New(containerOpts...)

	b := &Bars{p: p, out: w, maxBars: maxBars, active: map[*mpb.Bar]struct{}{}}
	b.total = b.newTotalBar()
	b.scan = b.newScanBar()
	return b
}

// newTotalBar は全体の進みぐあいを表すバーを作ります。
//
// 走査と転送が並行するので、総量は途中で増えていきます。
// mpb では SetTotal を繰り返し呼ぶことでこれを表現します。
// 速度と残り時間は移動平均で求めるので、総量が増えても暴れません。
func (b *Bars) newTotalBar() *mpb.Bar {
	return b.p.New(0,
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]"),
		// 常に一番上に置く
		mpb.BarPriority(math.MinInt),
		mpb.PrependDecorators(
			decor.Name("全体 ", decor.WC{C: decor.DindentRight}),
			decor.CountersKibiByte("% .1f / % .1f"),
		),
		mpb.AppendDecorators(
			// 全体の速度は実時間から求める。
			//
			// 移動平均は「1回の読み取りに何秒かかったか」を見るため、
			// 複数のワーカーが並行して読むと、1本ぶんの速度しか出ない。
			// 合計の実効速度を示したいので、経過時間で割る形にする。
			decor.AverageSpeed(decor.SizeB1024(0), " % .1f", decor.WC{W: 11}),
			decor.Name(" 残り"),
			decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO, decor.WC{W: 7}), "  完了"),
		),
	)
}

// newScanBar は走査の進みぐあいを表す行を作ります。
// 総数が分からないので、バーではなく回転する印にします。
func (b *Bars) newScanBar() *mpb.Bar {
	return b.p.New(0,
		mpb.SpinnerStyle(),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name("調査中 ", decor.WC{C: decor.DindentRight}),
		),
		mpb.AppendDecorators(
			decor.Any(func(decor.Statistics) string {
				return fmt.Sprintf("%d件 / %s（%dディレクトリ）",
					b.scanFiles.Load(), HumanBytes(b.scanBytes.Load()), b.scanDirs.Load())
			}),
		),
	)
}

func (b *Bars) ScanStarted() {}

func (b *Bars) ScanProgress(dirs, files int64, bytes int64) {
	b.scanDirs.Store(dirs)
	b.scanFiles.Store(files)
	b.scanBytes.Store(bytes)

	// 総量が増えるたびに知らせる。第2引数を false にすることで、
	// 総量に達しても完了扱いにせず、あとから増やせるようにする。
	b.total.SetTotal(bytes, false)
}

func (b *Bars) ScanDone(dirs, files int64, bytes int64) {
	if b.scanEnded.Swap(true) {
		return
	}

	b.scanDirs.Store(dirs)
	b.scanFiles.Store(files)
	b.scanBytes.Store(bytes)
	b.total.SetTotal(bytes, false)

	// 走査の行を消す
	b.scan.Abort(true)
}

func (b *Bars) StartFile(name string, size int64) FileTracker {
	b.mu.Lock()
	over := b.visible >= b.maxBars
	if !over {
		b.visible++
	}
	b.mu.Unlock()

	if over {
		// 本数が多すぎると画面が埋まるので、全体のバーだけを進める。
		return &barTracker{bars: b, total: b.total}
	}

	bar := b.p.New(size,
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]"),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(elide(name, 22)+" ", decor.WC{W: 24, C: decor.DindentRight}),
		),
		mpb.AppendDecorators(
			decor.CountersKibiByte("% .1f/% .1f", decor.WC{W: 17}),
			decor.EwmaSpeed(decor.SizeB1024(0), " % .1f", 20, decor.WC{W: 11}),
		),
	)
	b.mu.Lock()
	b.active[bar] = struct{}{}
	b.mu.Unlock()

	return &barTracker{bars: b, total: b.total, file: bar, counted: true}
}

func (b *Bars) Skipped(string, int64) {}

// Logf はバーを崩さずにメッセージを書きます。
//
// mpb.Progress は io.Writer なので、ここへ書けばバーの上に流れます。
// 以前は複数のゴルーチンから直接 fmt.Printf しており、
// 出力が混ざっていました。
func (b *Bars) Logf(format string, a ...any) {
	if b.closed.Load() {
		fmt.Fprintf(b.out, format+"\n", a...)
		return
	}
	fmt.Fprintf(b.p, format+"\n", a...)
}

func (b *Bars) Done(s Summary) {
	// 全体のバーを完了させる
	b.total.SetTotal(b.total.Current(), true)
}

func (b *Bars) Close() error {
	if b.closed.Swap(true) {
		return nil
	}

	// 残っているファイルごとのバーを片付ける。
	// 未完了のバーがあると mpb の Wait が戻ってこない。
	b.mu.Lock()
	for bar := range b.active {
		bar.Abort(true)
	}
	b.active = map[*mpb.Bar]struct{}{}
	b.mu.Unlock()

	b.scan.Abort(true)
	b.total.Abort(true)
	b.p.Wait()
	return nil
}

// Writer は進捗の表示を崩さずに書ける io.Writer を返します。
// ログの出力先として使えます。
func (b *Bars) Writer() io.Writer { return b.p }

// barTracker は1ファイルぶんの記録係です。
type barTracker struct {
	bars  *Bars
	total *mpb.Bar
	file  *mpb.Bar
	// counted はファイルごとのバーを持っているかどうかです。
	counted bool

	read     atomic.Int64
	finished atomic.Bool
}

func (t *barTracker) Wrap(r io.Reader) io.Reader {
	return &dualCountingReader{t: t, r: r}
}

// Reset は数えた量を0に戻します。再試行のときに使います。
func (t *barTracker) Reset() {
	n := t.read.Swap(0)
	if n == 0 {
		return
	}

	// 全体のバーからも、いったん数えたぶんを引く。
	// mpb は負の増分を受け付けないので現在値を直接設定する。
	t.total.SetCurrent(max(0, t.total.Current()-n))
	if t.file != nil {
		t.file.SetCurrent(0)
	}
}

func (t *barTracker) Complete(n int64) {
	t.read.Add(n)
	t.total.IncrInt64(n)
	if t.file != nil {
		t.file.IncrInt64(n)
	}
}

func (t *barTracker) Abort() {
	if t.file == nil {
		return
	}
	t.file.Abort(true)

	t.bars.mu.Lock()
	delete(t.bars.active, t.file)
	t.bars.mu.Unlock()
}

func (t *barTracker) Finish() {
	if t.finished.Swap(true) {
		return
	}
	if t.file != nil {
		// 完了させて表示から消す
		t.file.SetTotal(t.file.Current(), true)
	}
	if t.counted {
		t.bars.mu.Lock()
		t.bars.visible--
		delete(t.bars.active, t.file)
		t.bars.mu.Unlock()
	}
}

// dualCountingReader は、ファイルごとのバーと全体のバーを同時に進めます。
//
// mpb の ProxyReader を2重に重ねると読み取りが二度数えられるので、
// 1つの Reader から両方へ知らせます。移動平均のために所要時間も渡します。
type dualCountingReader struct {
	t *barTracker
	r io.Reader
}

func (d *dualCountingReader) Read(p []byte) (int, error) {
	started := time.Now()
	n, err := d.r.Read(p)
	elapsed := time.Since(started)

	if n > 0 {
		d.t.read.Add(int64(n))
		d.t.total.EwmaIncrInt64(int64(n), elapsed)
		if d.t.file != nil {
			d.t.file.EwmaIncrInt64(int64(n), elapsed)
		}
	}
	return n, err
}

// elide は長い名前を縮めます。末尾のほうが見分けやすいので前を落とします。
func elide(s string, limit int) string {
	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	if limit <= 3 {
		return string(runes[len(runes)-limit:])
	}
	return "..." + string(runes[len(runes)-(limit-3):])
}
