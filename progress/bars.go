package progress

import (
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mattn/go-runewidth"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// Bars は端末に進捗バーを描く Reporter です。
type Bars struct {
	p   *mpb.Progress
	out io.Writer

	// stat は表示に使う集計です。速度も残り時間もここから作ります。
	stat *stats

	total *mpb.Bar

	mu sync.Mutex
	// scan は走査中だけ出す行です。走査が終わると消します。
	scan *mpb.Bar
	// ファイルごとのバーの本数を抑える
	visible  int
	maxBars  int
	barDelay time.Duration
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
	// BarDelay はファイルごとのバーを出すまでの待ちです。0 なら既定値。
	BarDelay time.Duration
}

// DefaultMaxBars はファイルごとのバーの既定の本数です。
const DefaultMaxBars = 8

// DefaultBarDelay はファイルごとのバーを出すまでの既定の待ちです。
//
// すぐ終わるファイルにまでバーを出すと、行が増えたり減ったりを
// 描き直しのたびに繰り返して画面が落ち着きません。少し待って、
// それでもまだ終わっていないものだけを出します。
const DefaultBarDelay = 400 * time.Millisecond

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
	barDelay := opts.BarDelay
	if barDelay <= 0 {
		barDelay = DefaultBarDelay
	}

	containerOpts := []mpb.ContainerOption{
		mpb.WithOutput(w),
		mpb.WithWidth(80),
		mpb.WithRefreshRate(150 * time.Millisecond),
	}
	if opts.ForceTTY {
		// ForceTTY は書き出し先を端末として扱わせるだけで、
		// 自動更新は別に指定する必要がある。
		containerOpts = append(containerOpts, mpb.ForceTTY(), mpb.WithAutoRefresh())
	}

	// PopCompletedMode は使わない。
	//
	// あれは終わったバーを画面に残したまま上へ流す仕組みで、
	// BarRemoveOnComplete より優先される。失敗や中断で
	// 止めたバーまで書き途中のまま端末に焼き付いてしまうので、
	// 消えるべきものが消えるようにここでは使わない。
	p := mpb.New(containerOpts...)

	b := &Bars{
		p:        p,
		out:      w,
		stat:     newStats(),
		maxBars:  maxBars,
		barDelay: barDelay,
		active:   map[*mpb.Bar]struct{}{},
	}
	b.total = b.newTotalBar()
	return b
}

// newTotalBar は全体の進みぐあいを表すバーを作ります。
//
// 走査と転送が並行するので、総量は途中で増えていきます。
// mpb では SetTotal を繰り返し呼ぶことでこれを表現します。
func (b *Bars) newTotalBar() *mpb.Bar {
	return b.p.New(0,
		mpb.BarStyle().Lbound("[").Filler("=").Tip(">").Padding(" ").Rbound("]"),
		// 常に一番上に置く
		mpb.BarPriority(math.MinInt),
		mpb.PrependDecorators(
			decor.Name("全体 ", decor.WC{C: decor.DindentRight}),
			decor.Any(func(decor.Statistics) string {
				return fmt.Sprintf("%s / %s",
					HumanBytes(b.stat.dealtBytes()), HumanBytes(b.stat.scanBytes.Load()))
			}, decor.WC{W: 21, C: decor.DindentRight}),
		),
		mpb.AppendDecorators(
			// 速度も残り時間も自前の集計から作る。
			//
			// mpb の平均は「バーの現在値」から求めるが、そこには
			// 転送しないと判断したぶんも入っている。速度としては
			// 出ていない量なので、転送したぶんだけで割り直す。
			decor.Any(func(decor.Statistics) string {
				return b.stat.rateText()
			}, decor.WC{W: 12}),
			decor.Any(func(decor.Statistics) string {
				return b.stat.etaText()
			}, decor.WC{W: 13}),
		),
	)
}

// newScanBar は走査の進みぐあいを表す行を作ります。
// 総数が分からないので、バーではなく回転する印にします。
func (b *Bars) newScanBar() *mpb.Bar {
	return b.p.New(0,
		mpb.SpinnerStyle(),
		mpb.BarRemoveOnComplete(),
		// 回る印は1桁ぶんだけにする。
		// 指定しないと行の幅いっぱいに広げられ、
		// 印と件数の間が大きく空いてしまう。
		mpb.BarWidth(1),
		// 全体のバーのすぐ下に置く
		mpb.BarPriority(math.MinInt+1),
		mpb.PrependDecorators(
			decor.Name("調査中 ", decor.WC{C: decor.DindentRight}),
		),
		mpb.AppendDecorators(
			decor.Any(func(decor.Statistics) string {
				text := fmt.Sprintf("%d件 / %s（%dディレクトリ）",
					b.stat.scanFiles.Load(), HumanBytes(b.stat.scanBytes.Load()), b.stat.scanDirs.Load())
				// 転送の要否は走査しながら判断するので、
				// スキップの数もここに出す。全体のバーが速く進む
				// 理由が分からないと、進みぐあいを信じられない。
				if n := b.stat.skipFiles.Load(); n > 0 {
					text += fmt.Sprintf("  スキップ %d件", n)
				}
				return text
			}),
		),
	)
}

// ScanStarted は走査の開始を伝えます。
//
// 全体のやり直しでもう一度呼ばれることがあるので、
// そのたびに走査の行を作り直します。
func (b *Bars) ScanStarted() {
	b.stat.beginScan()

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() || b.scan != nil {
		return
	}
	b.scan = b.newScanBar()
}

func (b *Bars) ScanProgress(dirs, files int64, bytes int64) {
	b.stat.scanProgress(dirs, files, bytes)

	// 総量が増えるたびに知らせる。第2引数を false にすることで、
	// 総量に達しても完了扱いにせず、あとから増やせるようにする。
	b.total.SetTotal(b.stat.scanBytes.Load(), false)
}

func (b *Bars) ScanDone(dirs, files int64, bytes int64) {
	b.stat.endScan(dirs, files, bytes)
	b.total.SetTotal(b.stat.scanBytes.Load(), false)

	b.mu.Lock()
	scan := b.scan
	b.scan = nil
	b.mu.Unlock()

	if scan == nil {
		return
	}
	// 走査の行を消す。消えたあとも分かるように、結果は一行残す。
	scan.Abort(true)
	b.logScanResult()
}

// logScanResult は走査の結果を一行残します。
//
// 走査の行は消えてしまうので、何件のうち何件を転送しないと
// 判断したのかが分からなくなります。あとから読めるように書き出します。
func (b *Bars) logScanResult() {
	files, bytes := b.stat.scanFiles.Load(), b.stat.scanBytes.Load()
	if n := b.stat.skipFiles.Load(); n > 0 {
		b.Logf("調査が終わりました: %d件 / %s（うち %d件 / %s は転送不要）",
			files, HumanBytes(bytes), n, HumanBytes(b.stat.skipBytes.Load()))
		return
	}
	b.Logf("調査が終わりました: %d件 / %s", files, HumanBytes(bytes))
}

func (b *Bars) StartFile(name string, size int64) FileTracker {
	return &barTracker{bars: b, name: name, size: size, started: time.Now()}
}

// Skipped は転送不要と判断したことを記録します。
//
// 進みぐあいに数え入れるのが肝心です。数えないと、すでにコピー済みの
// ものが多いときにバーがいつまでも進まず、残り時間も当てになりません。
func (b *Bars) Skipped(_ string, size int64) {
	b.stat.skip(size)
	b.syncTotal()
}

// syncTotal は片付いた量を全体のバーへ反映します。
//
// 値は必ず集計から作り直します。増分を足し込む形にすると、
// 再試行で巻き戻すときに、同時に走っている他のワーカーの
// 進みぐあいまで一緒に消えてしまいます。
func (b *Bars) syncTotal() {
	b.total.SetCurrent(b.stat.dealtBytes())
}

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

// Done は転送が終わったことを伝えます。
//
// ここではバーを完了させません。--retry-pass では転送のあとに
// もう一度走査から始まるので、完了させると以後の表示が止まります。
// 最後の後始末は Close で行います。
func (b *Bars) Done(_ Summary) {
	b.stat.finished.Store(true)
	b.syncTotal()
}

func (b *Bars) Close() error {
	if b.closed.Swap(true) {
		return nil
	}
	b.stat.finished.Store(true)

	// 残っているファイルごとのバーを片付ける。
	// 未完了のバーがあると mpb の Wait が戻ってこない。
	b.mu.Lock()
	for bar := range b.active {
		bar.Abort(true)
	}
	b.active = map[*mpb.Bar]struct{}{}
	scan := b.scan
	b.scan = nil
	b.mu.Unlock()

	if scan != nil {
		scan.Abort(true)
	}

	b.syncTotal()
	b.finishTotal()
	b.p.Wait()
	return nil
}

// finishTotal は全体のバーを最後の姿にします。
//
// 片付いていれば埋めます。総量が0のときは割合が定まらず、
// バーが空のまま残ってしまうので、そこも埋めます。
// 0バイトのファイルばかりだった場合や、転送するものが
// 1件もなかった場合がこれにあたります。
//
// 中断や失敗で片付いていないぶんが残っている場合は埋めません。
// 止まったところを、そのまま最後の表示として残します。
func (b *Bars) finishTotal() {
	if b.stat.remainingBytes() > 0 || b.stat.remainingFiles() > 0 {
		b.total.Abort(false)
		return
	}

	// 第2引数の true で、総量まで進めたうえで完了させる。
	b.total.SetTotal(max(b.stat.scanBytes.Load(), 1), true)
}

// Writer は進捗の表示を崩さずに書ける io.Writer を返します。
// ログの出力先として使えます。
func (b *Bars) Writer() io.Writer { return b.p }

// takeSlot はファイルごとのバーを出す枠を取ります。
func (b *Bars) takeSlot() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed.Load() || b.visible >= b.maxBars {
		return false
	}
	b.visible++
	return true
}

// releaseSlot は枠を返します。
func (b *Bars) releaseSlot(bar *mpb.Bar) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.visible--
	delete(b.active, bar)
}

// newFileBar は1ファイルぶんのバーを作ります。
func (b *Bars) newFileBar(name string, size, current int64) *mpb.Bar {
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
	if current > 0 {
		bar.SetCurrent(current)
	}

	b.mu.Lock()
	b.active[bar] = struct{}{}
	b.mu.Unlock()
	return bar
}

// barTracker は1ファイルぶんの記録係です。
type barTracker struct {
	bars    *Bars
	name    string
	size    int64
	started time.Time

	// file は表示中のバーです。すぐ終わるファイルには出さないので、
	// 待ちを過ぎてから作ります。
	mu   sync.Mutex
	file *mpb.Bar
	slot bool

	read     atomic.Int64
	aborted  atomic.Bool
	finished atomic.Bool
}

func (t *barTracker) Wrap(r io.Reader) io.Reader {
	return &countingReader{r: r, onRead: t.advance}
}

// advance は読み取った量を記録します。
func (t *barTracker) advance(n int, elapsed time.Duration) {
	t.read.Add(int64(n))
	t.bars.stat.doneBytes.Add(int64(n))
	t.bars.syncTotal()

	if bar := t.ensureBar(); bar != nil {
		// ファイルごとの速度は移動平均で出す。
		// 1本ぶんの速さを見たいので、こちらは実時間ではなく
		// 1回の読み取りにかかった時間を渡す。
		bar.EwmaIncrInt64(int64(n), elapsed)
	}
}

// ensureBar は、待ちを過ぎていればバーを作って返します。
//
// 枠が空いていなければ作りません。そのファイルは全体のバーにだけ現れます。
func (t *barTracker) ensureBar() *mpb.Bar {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.file != nil || t.finished.Load() {
		return t.file
	}
	if time.Since(t.started) < t.bars.barDelay {
		return nil
	}
	if !t.bars.takeSlot() {
		return nil
	}
	t.slot = true
	t.file = t.bars.newFileBar(t.name, t.size, t.read.Load())
	return t.file
}

// Reset は数えた量を0に戻します。再試行のときに使います。
func (t *barTracker) Reset() {
	n := t.read.Swap(0)
	if n == 0 {
		return
	}

	t.bars.stat.doneBytes.Add(-n)
	t.bars.syncTotal()

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.file != nil {
		t.file.SetCurrent(0)
	}
}

func (t *barTracker) Complete(n int64) {
	t.read.Add(n)
	t.bars.stat.doneBytes.Add(n)
	t.bars.syncTotal()

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.file != nil {
		t.file.IncrInt64(n)
	}
}

func (t *barTracker) Abort() {
	t.aborted.Store(true)

	t.mu.Lock()
	bar, slot := t.file, t.slot
	t.file, t.slot = nil, false
	t.mu.Unlock()

	// 途中まで描いたバーは消す。残すと、書きかけの姿が
	// そのまま端末に焼き付いてしまう。
	if bar != nil {
		bar.Abort(true)
	}
	if slot {
		t.bars.releaseSlot(bar)
	}
}

func (t *barTracker) Finish() {
	if t.finished.Swap(true) {
		return
	}

	t.mu.Lock()
	bar, slot := t.file, t.slot
	t.file, t.slot = nil, false
	t.mu.Unlock()

	if bar != nil {
		// 完了させて表示から消す
		bar.SetTotal(-1, true)
	}
	if slot {
		t.bars.releaseSlot(bar)
	}

	if t.aborted.Load() {
		t.bars.stat.failedFiles.Add(1)
	} else {
		t.bars.stat.doneFiles.Add(1)
	}

	// 読めなかった残りを、片付いたぶんとして数える。
	// そうしないと、失敗したファイルのぶんだけバーが最後まで届かない。
	if rest := t.size - t.read.Load(); rest > 0 {
		t.bars.stat.settledBytes.Add(rest)
		t.bars.syncTotal()
	}
}

// elide は長い名前を、端末での表示幅で縮めます。
//
// 文字数ではなく幅で数えます。日本語のように1文字で2桁ぶんの幅を
// とる文字があるので、文字数で切ると行ごとに長さがそろわず、
// バーの右端が揃わなくなります。末尾のほうが見分けやすいので前を落とします。
func elide(s string, limit int) string {
	if runewidth.StringWidth(s) <= limit {
		return s
	}

	const mark = "..."
	room := limit - len(mark)
	if room < 1 {
		return runewidth.Truncate(s, limit, "")
	}

	runes := []rune(s)
	width, i := 0, len(runes)
	for i > 0 {
		w := runewidth.RuneWidth(runes[i-1])
		if width+w > room {
			break
		}
		width += w
		i--
	}
	return mark + string(runes[i:])
}
