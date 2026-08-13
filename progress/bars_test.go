package progress

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer は複数のゴルーチンから書ける bytes.Buffer です。
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// stripANSI は制御文字を取り除いて、内容だけを取り出します。
func stripANSI(s string) string {
	var out strings.Builder
	for i := 0; i < len(s); {
		if s[i] == 0x1b && i+1 < len(s) && s[i+1] == '[' {
			j := i + 2
			// 制御列は英字で終わる
			for j < len(s) && (s[j] < 'A' || s[j] > 'Z') && (s[j] < 'a' || s[j] > 'z') {
				j++
			}
			i = j + 1
			continue
		}
		out.WriteByte(s[i])
		i++
	}
	return out.String()
}

// 端末でない書き出し先でも、明示的に指示すればバーが描かれることを確認します。
//
// mpb は端末でない場合に自動更新を行わないため、
// ForceTTY を渡さないと何も出力されません。
func TestBarsRendersWithForceTTY(t *testing.T) {
	buf := &syncBuffer{}
	b := NewBars(BarsOptions{Writer: buf, ForceTTY: true})

	b.ScanProgress(1, 2, 2000)

	tracker := b.StartFile("example.bin", 1000)
	r := tracker.Wrap(strings.NewReader(strings.Repeat("x", 1000)))

	// 少しずつ読んで、描画の機会を作る
	chunk := make([]byte, 100)
	for {
		n, err := r.Read(chunk)
		if n > 0 {
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			break
		}
	}
	tracker.Finish()

	b.ScanDone(1, 2, 2000)
	b.Done(Summary{Transferred: 1, Bytes: 1000})
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	out := stripANSI(buf.String())
	if out == "" {
		t.Fatal("何も出力されていない")
	}
	for _, want := range []string{"全体", "example.bin"} {
		if !strings.Contains(out, want) {
			t.Errorf("出力に %q が含まれていない:\n%s", want, out)
		}
	}
}

// 端末でない書き出し先に、指示がなければ描かないことを確認します。
// パイプやジョブに制御文字を書き込まないためです。
func TestBarsQuietWithoutForceTTY(t *testing.T) {
	buf := &syncBuffer{}
	b := NewBars(BarsOptions{Writer: buf})

	tracker := b.StartFile("x.bin", 10)
	io.Copy(io.Discard, tracker.Wrap(strings.NewReader("0123456789")))
	tracker.Finish()
	b.Done(Summary{})
	b.Close()

	if out := buf.String(); strings.Contains(out, "\x1b[") {
		t.Errorf("端末でないのに制御文字を書いている:\n%q", out)
	}
}

// バーを崩さずにメッセージを書けることを確認します。
func TestBarsLogf(t *testing.T) {
	buf := &syncBuffer{}
	b := NewBars(BarsOptions{Writer: buf, ForceTTY: true})

	b.Logf("再試行します: %s", "a.txt")
	time.Sleep(300 * time.Millisecond)
	b.Done(Summary{})
	b.Close()

	if out := stripANSI(buf.String()); !strings.Contains(out, "再試行します: a.txt") {
		t.Errorf("メッセージが出力されていない:\n%s", out)
	}
}

// 再試行のときに、数えた量が巻き戻ることを確認します。
func TestBarTrackerReset(t *testing.T) {
	buf := &syncBuffer{}
	b := NewBars(BarsOptions{Writer: buf, ForceTTY: true})
	defer b.Close()

	tracker := b.StartFile("a.bin", 100)

	// 途中まで読む
	r := tracker.Wrap(strings.NewReader(strings.Repeat("x", 100)))
	io.CopyN(io.Discard, r, 50)

	before := b.total.Current()
	if before != 50 {
		t.Fatalf("全体の進みぐあい = %d, want 50", before)
	}

	tracker.Reset()
	if after := b.total.Current(); after != 0 {
		t.Errorf("巻き戻し後の全体の進みぐあい = %d, want 0", after)
	}
}

// ファイルごとのバーの本数に上限があることを確認します。
func TestBarsLimitsVisibleBars(t *testing.T) {
	buf := &syncBuffer{}
	b := NewBars(BarsOptions{Writer: buf, ForceTTY: true, MaxBars: 2})
	defer b.Close()

	trackers := make([]FileTracker, 0, 5)
	for i := range 5 {
		trackers = append(trackers, b.StartFile(string(rune('a'+i))+".bin", 10))
	}

	b.mu.Lock()
	visible := b.visible
	b.mu.Unlock()

	if visible > 2 {
		t.Errorf("表示中のバーが %d 本、上限は 2", visible)
	}

	// 上限を超えたぶんも、全体の進みぐあいには反映されること
	for _, tr := range trackers {
		tr.Complete(10)
	}
	if got := b.total.Current(); got != 50 {
		t.Errorf("全体の進みぐあい = %d, want 50（上限を超えたぶんも数える）", got)
	}
}

func TestElide(t *testing.T) {
	tests := []struct {
		in    string
		limit int
		want  string
	}{
		{"short.txt", 20, "short.txt"},
		// 上限は「...」を含めた文字数。10文字なら「...」+ 末尾7文字。
		{"very-long-file-name.txt", 10, "...ame.txt"},
		{"日本語のファイル名です.txt", 10, "...名です.txt"},
	}
	for _, tt := range tests {
		if got := elide(tt.in, tt.limit); got != tt.want {
			t.Errorf("elide(%q, %d) = %q, want %q", tt.in, tt.limit, got, tt.want)
		}
	}
}
