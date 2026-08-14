package progress

import (
	"sync/atomic"
	"time"
)

// stats は進みぐあいの集計です。Bars と Plain が共有します。
//
// 「見つけた量」と「片付いた量」を分けて数えます。片付いた量には、
// 転送しないと判断したぶんも入れます。ここを入れないと、すでに
// コピー済みのものが多いときに、見かけの進みぐあいがいつまでも
// 上がらず、残り時間も実際の何倍にもなってしまいます。
type stats struct {
	// started は数え始めた時刻です。速度と残り時間の分母になります。
	started time.Time

	// 走査で見つけた量。走査は転送と並行するので途中で増えていきます。
	scanDirs  atomic.Int64
	scanFiles atomic.Int64
	scanBytes atomic.Int64

	// scanning は走査がまだ続いているかどうかです。
	// 続いている間は総量が確定しないので、残り時間は下限にしかなりません。
	scanning atomic.Bool
	// finished は転送が終わったかどうかです。
	finished atomic.Bool

	// 全体のやり直し（--retry-pass）では走査が数え直しになるので、
	// 前回までのぶんを控えておいて足します。控えないと、
	// 2回目の走査が始まった瞬間に総量が縮んで表示が飛びます。
	baseDirs  atomic.Int64
	baseFiles atomic.Int64
	baseBytes atomic.Int64

	// 転送しないと判断したぶん
	skipFiles atomic.Int64
	skipBytes atomic.Int64

	// 実際に転送したぶん
	doneFiles atomic.Int64
	doneBytes atomic.Int64

	// 失敗して諦めた件数
	failedFiles atomic.Int64

	// 転送を経ずに片付いたぶんです。失敗して読めなかった残りや、
	// --dry-run のように実際には運ばないぶんがここに入ります。
	// これを足しておかないと、バーが最後まで届きません。
	settledBytes atomic.Int64
}

// newStats は集計を始めます。
func newStats() *stats {
	return &stats{started: time.Now()}
}

// beginScan は走査の開始を記録します。
//
// やり直しでもう一度呼ばれた場合は、そこまでの数を土台にして数え直します。
func (s *stats) beginScan() {
	s.baseDirs.Store(s.scanDirs.Load())
	s.baseFiles.Store(s.scanFiles.Load())
	s.baseBytes.Store(s.scanBytes.Load())
	s.scanning.Store(true)
	s.finished.Store(false)
}

// scanProgress は走査の途中経過を記録します。
func (s *stats) scanProgress(dirs, files, bytes int64) {
	s.scanDirs.Store(s.baseDirs.Load() + dirs)
	s.scanFiles.Store(s.baseFiles.Load() + files)
	s.scanBytes.Store(s.baseBytes.Load() + bytes)
}

// endScan は走査の終わりを記録します。
func (s *stats) endScan(dirs, files, bytes int64) {
	s.scanProgress(dirs, files, bytes)
	s.scanning.Store(false)
}

// skip は転送しないと判断したことを記録します。
func (s *stats) skip(size int64) {
	s.skipFiles.Add(1)
	if size > 0 {
		s.skipBytes.Add(size)
	}
}

// elapsed は数え始めてからの時間です。
func (s *stats) elapsed() time.Duration { return time.Since(s.started) }

// dealtBytes は片付いた量です。
func (s *stats) dealtBytes() int64 {
	return s.doneBytes.Load() + s.skipBytes.Load() + s.settledBytes.Load()
}

// remainingBytes はこれから転送する量です。
//
// 転送の要否は見つけた順にその場で判断するので、見つけたのに
// 片付いていないぶんは、これから転送するものとみなせます。
// 走査が終わっていればこの値は確定し、途中なら下限になります。
func (s *stats) remainingBytes() int64 {
	return max(0, s.scanBytes.Load()-s.dealtBytes())
}

// remainingFiles はこれから転送する件数です。
func (s *stats) remainingFiles() int64 {
	settled := s.doneFiles.Load() + s.failedFiles.Load() + s.skipFiles.Load()
	return max(0, s.scanFiles.Load()-settled)
}

// eta は残り時間の見積もりです。求められない場合は false を返します。
//
// 残りは「これから転送する量」だけを見ます。転送しないと判断したぶんは
// 時間がかからないので、残りに含めると見積もりが何倍にも膨らみます。
//
// 速さは経過時間で割った実効値です。走査の待ちや要否の判断にかかった
// 時間も分母に入るので、それらが同じ調子で続くぶんも見積もりに入ります。
//
// 小さなファイルが大量にある場合、時間を決めるのは量ではなく件数です。
// そこで量から求めた見積もりと件数から求めた見積もりを両方出し、
// 大きいほうを採ります。どちらか一方だけでは、
// 小さなファイルが並ぶところで実際の何分の一にもなってしまいます。
func (s *stats) eta() (time.Duration, bool) {
	elapsed := s.elapsed()
	if elapsed <= 0 {
		return 0, false
	}

	remBytes, remFiles := s.remainingBytes(), s.remainingFiles()
	if remBytes <= 0 && remFiles <= 0 {
		return 0, true
	}

	var eta time.Duration
	known := false
	if done := s.doneBytes.Load(); done > 0 && remBytes > 0 {
		eta = scaleDuration(elapsed, remBytes, done)
		known = true
	}
	if done := s.doneFiles.Load(); done > 0 && remFiles > 0 {
		if byFiles := scaleDuration(elapsed, remFiles, done); byFiles > eta {
			eta = byFiles
		}
		known = true
	}
	return eta, known
}

// etaText は残り時間の表示です。
//
// 走査が終わるまで総量は確定しないので、そこまでの見積もりは下限にすぎません。
// 数字だけを出すと確定した値に見えるので、末尾に + を付けて区別します。
func (s *stats) etaText() string {
	if s.finished.Load() {
		if s.remainingBytes() <= 0 && s.remainingFiles() <= 0 {
			return "完了"
		}
		return ""
	}

	eta, ok := s.eta()
	if !ok {
		return "残り --:--"
	}
	if s.scanning.Load() {
		if eta <= 0 {
			// 見つけたぶんは片付いてしまって、次が見つかるのを
			// 待っている状態。0 と出すと、すぐ終わるように見えてしまう。
			return "残り --:--"
		}
		return "残り " + HumanDuration(eta) + "+"
	}
	return "残り " + HumanDuration(eta)
}

// rateText は転送の実効速度の表示です。
//
// 転送しないと判断したぶんは数えません。数えてしまうと、
// すでにコピー済みのものが多いときに、実際には出ていない速度が出ます。
func (s *stats) rateText() string {
	return HumanRate(s.doneBytes.Load(), s.elapsed())
}

// scaleDuration は d を num/den 倍します。
// 桁あふれを避けるため浮動小数点で計算します。
func scaleDuration(d time.Duration, num, den int64) time.Duration {
	if den <= 0 {
		return 0
	}
	return time.Duration(float64(d) * float64(num) / float64(den))
}
