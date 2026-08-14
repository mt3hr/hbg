package progress

import (
	"testing"
	"time"
)

// 転送しないと判断したぶんも、片付いた量に入ることを確認します。
//
// ここが入らないと、すでにコピー済みのものが多いときに
// 進みぐあいがいつまでも上がりません。
func TestStatsDealtIncludesSkipped(t *testing.T) {
	s := newStats()
	s.scanProgress(1, 3, 3000)
	s.skip(1000)
	s.skip(1000)
	s.doneBytes.Add(500)

	if got := s.dealtBytes(); got != 2500 {
		t.Errorf("片付いた量 = %d, want 2500", got)
	}
	if got := s.remainingBytes(); got != 500 {
		t.Errorf("残りの量 = %d, want 500", got)
	}
}

// 残り時間が、転送しないぶんに引きずられないことを確認します。
func TestStatsETAIgnoresSkipped(t *testing.T) {
	s := newStats()
	s.started = time.Now().Add(-10 * time.Second)

	// 10GB のうち 9GB は転送不要。1GB のうち半分を10秒で転送した。
	s.scanProgress(1, 10, 10_000_000_000)
	s.skipFiles.Store(9)
	s.skipBytes.Store(9_000_000_000)
	s.doneFiles.Store(0)
	s.doneBytes.Store(500_000_000)
	s.scanning.Store(false)

	eta, ok := s.eta()
	if !ok {
		t.Fatal("残り時間が求められなかった")
	}
	// 残り 500MB を 50MB/秒 で運ぶので、およそ10秒。
	if eta < 9*time.Second || eta > 11*time.Second {
		t.Errorf("残り時間 = %v, want 約10秒", eta)
	}
}

// 件数が多く1件が小さい場合に、量だけで見積もらないことを確認します。
//
// 小さなファイルが並ぶところでは、時間を決めるのは量ではなく件数です。
func TestStatsETAUsesFileCount(t *testing.T) {
	s := newStats()
	s.started = time.Now().Add(-100 * time.Second)

	// 1000件のうち100件を100秒で転送した。
	// 量のうえでは9割がた終わっているが、件数では1割しか終わっていない。
	s.scanProgress(1, 1000, 10000)
	s.doneFiles.Store(100)
	s.doneBytes.Store(9000)
	s.scanning.Store(false)

	eta, ok := s.eta()
	if !ok {
		t.Fatal("残り時間が求められなかった")
	}
	// 量から求めると残り1000バイトぶんで11秒だが、
	// 残り900件を1秒に1件で運ぶので実際には900秒かかる。
	if eta < 890*time.Second || eta > 910*time.Second {
		t.Errorf("残り時間 = %v, want 900秒ほど", eta)
	}
}

// 走査中は、残り時間が下限であることが分かる形になっていることを確認します。
func TestStatsETATextWhileScanning(t *testing.T) {
	s := newStats()
	s.started = time.Now().Add(-10 * time.Second)
	s.beginScan()
	s.scanProgress(1, 2, 2000)
	s.doneBytes.Store(1000)

	if got := s.etaText(); got != "残り 0:10+" {
		t.Errorf("走査中の表示 = %q, want \"残り 0:10+\"", got)
	}

	s.endScan(1, 2, 2000)
	if got := s.etaText(); got != "残り 0:10" {
		t.Errorf("走査後の表示 = %q, want \"残り 0:10\"", got)
	}
}

// 見つけたぶんが片付いてしまっているときは、
// 走査中である限り残り時間を出さないことを確認します。
//
// ここで 0:00 と出すと、すぐ終わるように見えてしまいます。
// すでにコピー済みのものが多い場合、実際にはこのあと何分も走査が続きます。
func TestStatsETATextWaitingForScan(t *testing.T) {
	s := newStats()
	s.started = time.Now().Add(-10 * time.Second)
	s.beginScan()
	s.scanProgress(1, 2, 2000)
	s.skip(1000)
	s.skip(1000)

	if got := s.etaText(); got != "残り --:--" {
		t.Errorf("走査中の表示 = %q, want \"残り --:--\"", got)
	}
}

// やり直しで走査を数え直しても、総量が縮まないことを確認します。
func TestStatsRescanKeepsTotal(t *testing.T) {
	s := newStats()
	s.beginScan()
	s.scanProgress(2, 10, 5000)
	s.endScan(2, 10, 5000)

	// 2回目の走査は0から数え直しになる。
	s.beginScan()
	s.scanProgress(1, 3, 1500)

	if got := s.scanBytes.Load(); got != 6500 {
		t.Errorf("総量 = %d, want 6500", got)
	}
	if got := s.scanFiles.Load(); got != 13 {
		t.Errorf("総件数 = %d, want 13", got)
	}
}

// 転送に失敗したぶんも片付いた扱いになることを確認します。
// でないと、失敗した量だけバーが最後まで届きません。
func TestStatsSettledFillsGap(t *testing.T) {
	s := newStats()
	s.scanProgress(1, 1, 1000)

	// 300バイトまで読んで失敗した。
	s.doneBytes.Add(300)
	s.failedFiles.Add(1)
	s.settledBytes.Add(700)

	if got := s.remainingBytes(); got != 0 {
		t.Errorf("残りの量 = %d, want 0", got)
	}
	if got := s.remainingFiles(); got != 0 {
		t.Errorf("残りの件数 = %d, want 0", got)
	}
}
