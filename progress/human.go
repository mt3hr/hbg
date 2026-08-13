package progress

import (
	"fmt"
	"io"
	"os"
	"time"
)

// バイト数の単位。1024進です。
const (
	kib int64 = 1 << (10 * (iota + 1))
	mib
	gib
	tib
	pib
)

// HumanBytes はバイト数を読みやすい文字列にします。
//
// 小数第1位まで、切り捨てで表示します。
func HumanBytes(n int64) string {
	if n < 0 {
		return "-" + HumanBytes(-n)
	}

	switch {
	case n >= pib:
		return withUnit(n, pib, "PiB")
	case n >= tib:
		return withUnit(n, tib, "TiB")
	case n >= gib:
		return withUnit(n, gib, "GiB")
	case n >= mib:
		return withUnit(n, mib, "MiB")
	case n >= kib:
		return withUnit(n, kib, "KiB")
	}
	return fmt.Sprintf("%dB", n)
}

// withUnit は単位で割って小数第1位まで表示します。
// 浮動小数点を使わないので丸め誤差が出ません。
func withUnit(n, unit int64, suffix string) string {
	whole := n / unit
	frac := (n % unit) * 10 / unit
	return fmt.Sprintf("%d.%d%s", whole, frac, suffix)
}

// HumanRate は転送速度を読みやすい文字列にします。
func HumanRate(bytes int64, elapsed time.Duration) string {
	if elapsed <= 0 || bytes <= 0 {
		return "-"
	}
	perSec := int64(float64(bytes) / elapsed.Seconds())
	return HumanBytes(perSec) + "/s"
}

// HumanETA は残り時間を読みやすい文字列にします。
func HumanETA(remaining int64, bytes int64, elapsed time.Duration) string {
	if remaining <= 0 {
		return "-"
	}
	if bytes <= 0 || elapsed <= 0 {
		return "--:--"
	}

	perSec := float64(bytes) / elapsed.Seconds()
	if perSec <= 0 {
		return "--:--"
	}
	eta := time.Duration(float64(remaining)/perSec) * time.Second
	return HumanDuration(eta)
}

// HumanDuration は時間を読みやすい文字列にします。
func HumanDuration(d time.Duration) string {
	if d < 0 {
		return "-"
	}
	d = d.Round(time.Second)

	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%d:%02d", m, s)
}

// defaultWriter は既定の書き出し先を返します。
//
// 標準出力はコマンドの結果を流すために空けておき、
// 進捗は標準エラー出力へ書きます。
func defaultWriter() io.Writer { return os.Stderr }
