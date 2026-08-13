package cli

import "testing"

func TestHumanReadableSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		size int64
		want string
	}{
		{name: "0バイト", size: 0, want: "0B"},
		{name: "1バイト", size: 1, want: "1B"},
		{name: "1023バイト（KB未満）", size: 1023, want: "1023B"},

		{name: "ちょうど1KB", size: KB, want: "1.0K"},
		{name: "1.5KB", size: KB + KB/2, want: "1.5K"},
		{name: "1023.9KB", size: MB - 1, want: "1023.9K"},

		{name: "ちょうど1MB", size: MB, want: "1.0M"},
		{name: "1.5MB", size: MB + MB/2, want: "1.5M"},
		// 期待値は2進で正確に表せる分数で作る。
		// MB*3/10 のような値は整数除算で切り捨てられ、
		// 実装側の小数切り捨てと二重に効いて期待値がずれる。
		{name: "12.25MB は切り捨てて 12.2M", size: 12*MB + MB/4, want: "12.2M"},

		{name: "ちょうど1GB", size: GB, want: "1.0G"},
		{name: "2.5GB", size: 2*GB + GB/2, want: "2.5G"},

		{name: "ちょうど1TB", size: TB, want: "1.0T"},
		{name: "3.75TB は切り捨てて 3.7T", size: 3*TB + TB*3/4, want: "3.7T"},
		{name: "TBを超えても T のまま", size: 2048 * TB, want: "2048.0T"},

		// 小数は切り捨て。四捨五入しない。
		{name: "切り捨て（1.99K は 1.9K）", size: KB + KB*99/100, want: "1.9K"},

		// 負値は通常ありえないが、防御的に扱えること
		{name: "負値", size: -KB, want: "-1.0K"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := humanReadableSize(tt.size); got != tt.want {
				t.Errorf("humanReadableSize(%d) = %q, want %q", tt.size, got, tt.want)
			}
		})
	}
}

// 単位の境界で単位が繰り上がることを確認する。
// 旧実装は剰余の取り方を誤っていたため、ここが崩れやすい箇所だった。
func TestHumanReadableSizeBoundaries(t *testing.T) {
	t.Parallel()

	boundaries := []struct {
		unit   int64
		suffix string
	}{
		{KB, "K"},
		{MB, "M"},
		{GB, "G"},
		{TB, "T"},
	}

	for _, b := range boundaries {
		// 境界の1バイト手前は、ひとつ下の単位で表示される
		below := humanReadableSize(b.unit - 1)
		if len(below) == 0 || string(below[len(below)-1]) == b.suffix {
			t.Errorf("humanReadableSize(%d) = %q: 境界手前なのに単位 %q が使われている",
				b.unit-1, below, b.suffix)
		}

		// 境界ちょうどは、その単位で "1.0<suffix>" になる
		at := humanReadableSize(b.unit)
		if at != "1.0"+b.suffix {
			t.Errorf("humanReadableSize(%d) = %q, want %q", b.unit, at, "1.0"+b.suffix)
		}
	}
}
