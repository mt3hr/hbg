package transfer

import (
	"context"
	"io"
	"sync"

	"golang.org/x/time/rate"

	"github.com/mt3hr/hbg/storage"
)

// limiterSet はストレージごとの呼び出し頻度の制限です。
//
// クラウドストレージは短時間に叩きすぎると拒否されます。
// 以前は MkDir の失敗時に1秒待つという場当たり的な対処が
// 直接書かれているだけで、通常の経路には何の制限もありませんでした。
//
// 制限をストレージの包み紙として実装しないのは、包むと
// Mover や Hasher といった追加の能力が隠れてしまうためです。
// 呼び出す側で待つほうが、結果として単純になります。
type limiterSet struct {
	mu       sync.Mutex
	limiters map[string]*rate.Limiter
	// perType は種別ごとの1秒あたりの呼び出し回数です。0 なら無制限。
	perType map[string]float64
	// def は種別に指定がない場合の値です。
	def float64
}

func newLimiterSet(defaultTPS float64, perType map[string]float64) *limiterSet {
	if perType == nil {
		perType = map[string]float64{}
	}
	return &limiterSet{
		limiters: map[string]*rate.Limiter{},
		perType:  perType,
		def:      defaultTPS,
	}
}

// wait は、そのストレージへの呼び出しが許されるまで待ちます。
func (l *limiterSet) wait(ctx context.Context, s storage.Storage) error {
	lim := l.limiterFor(s)
	if lim == nil {
		return nil
	}
	return lim.Wait(ctx)
}

func (l *limiterSet) limiterFor(s storage.Storage) *rate.Limiter {
	key := s.Type() + ":" + s.Name()

	l.mu.Lock()
	defer l.mu.Unlock()

	if lim, ok := l.limiters[key]; ok {
		return lim
	}

	tps, ok := l.perType[s.Type()]
	if !ok {
		tps = l.def
	}
	// ローカルファイルシステムは制限しない。
	if s.Features() != nil && s.Features().OSPath {
		tps = 0
	}
	if tps <= 0 {
		l.limiters[key] = nil
		return nil
	}

	lim := rate.NewLimiter(rate.Limit(tps), max(1, int(tps)))
	l.limiters[key] = lim
	return lim
}

// bandwidthLimiter は転送の帯域を制限します。
type bandwidthLimiter struct {
	lim *rate.Limiter
}

// newBandwidthLimiter は1秒あたりのバイト数で帯域を制限します。
// 0 以下なら制限しません。
func newBandwidthLimiter(bytesPerSec int64) *bandwidthLimiter {
	if bytesPerSec <= 0 {
		return &bandwidthLimiter{}
	}
	// 一度に読む量が突出しないよう、バケットの大きさを抑える。
	burst := int(min(bytesPerSec, 1<<20))
	return &bandwidthLimiter{lim: rate.NewLimiter(rate.Limit(bytesPerSec), burst)}
}

// wrap は読み取りに帯域制限をかけます。
func (b *bandwidthLimiter) wrap(ctx context.Context, r io.Reader) io.Reader {
	if b.lim == nil {
		return r
	}
	return &limitedReader{ctx: ctx, r: r, lim: b.lim}
}

type limitedReader struct {
	ctx context.Context
	r   io.Reader
	lim *rate.Limiter
}

func (l *limitedReader) Read(p []byte) (int, error) {
	// バケットの大きさを超えて要求すると永久に待つので、上限で切る。
	if burst := l.lim.Burst(); len(p) > burst {
		p = p[:burst]
	}

	n, err := l.r.Read(p)
	if n > 0 {
		if waitErr := l.lim.WaitN(l.ctx, n); waitErr != nil {
			return n, waitErr
		}
	}
	return n, err
}
