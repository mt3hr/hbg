package cli

import (
	"testing"
	"time"

	"github.com/mt3hr/hbg"
)

// shouldSkipCopy の挙動を固定する回帰テスト。
//
// このロジックは「どのファイルを転送しないか」を決めるものであり、
// 誤ればデータの取りこぼしや無駄な再転送に直結する。
// 今後インターフェースを作り直す際の安全網として、
// 現在の挙動（意図的でないものも含む）をここで明文化しておく。
func TestShouldSkipCopy(t *testing.T) {
	t.Parallel()

	base := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)

	src := func(name string, size int64, mod time.Time) *hbg.FileInfo {
		return &hbg.FileInfo{Name: name, Size: size, LastMod: mod}
	}

	tests := []struct {
		name           string
		src            *hbg.FileInfo
		dest           []*hbg.FileInfo
		updateDuration time.Duration
		want           bool
		why            string
	}{
		{
			name:           "コピー先が空ならスキップしない",
			src:            src("a.txt", 100, base),
			dest:           nil,
			updateDuration: time.Second,
			want:           false,
		},
		{
			name:           "同名・同サイズ・同時刻ならスキップする",
			src:            src("a.txt", 100, base),
			dest:           []*hbg.FileInfo{src("a.txt", 100, base)},
			updateDuration: time.Second,
			want:           true,
		},
		{
			name:           "名前が違えばスキップしない",
			src:            src("a.txt", 100, base),
			dest:           []*hbg.FileInfo{src("b.txt", 100, base)},
			updateDuration: time.Second,
			want:           false,
		},
		{
			name:           "サイズが違えばスキップしない",
			src:            src("a.txt", 100, base),
			dest:           []*hbg.FileInfo{src("a.txt", 101, base)},
			updateDuration: time.Second,
			want:           false,
			why:            "同時刻でもサイズが違えば転送する。破損検知の役割も兼ねている",
		},
		{
			name:           "時刻差が更新期間ちょうどならスキップする（境界・以下）",
			src:            src("a.txt", 100, base.Add(time.Second)),
			dest:           []*hbg.FileInfo{src("a.txt", 100, base)},
			updateDuration: time.Second,
			want:           true,
			why:            "比較は d <= updateDuration なので境界は含む",
		},
		{
			name:           "時刻差が更新期間を1ナノ秒でも超えればスキップしない",
			src:            src("a.txt", 100, base.Add(time.Second+time.Nanosecond)),
			dest:           []*hbg.FileInfo{src("a.txt", 100, base)},
			updateDuration: time.Second,
			want:           false,
		},
		{
			name:           "コピー先のほうが新しくても、差が更新期間内ならスキップする",
			src:            src("a.txt", 100, base),
			dest:           []*hbg.FileInfo{src("a.txt", 100, base.Add(time.Second))},
			updateDuration: time.Second,
			want:           true,
		},
		{
			name:           "コピー先のほうが新しく、差が更新期間を超えるとスキップしない（＝上書きする）",
			src:            src("a.txt", 100, base),
			dest:           []*hbg.FileInfo{src("a.txt", 100, base.Add(time.Hour))},
			updateDuration: time.Second,
			want:           false,
			why: "時刻差を絶対値で見ているため、コピー先が新しくても古いファイルで上書きしてしまう。" +
				"これは既存の挙動であり、将来 --update フラグで変更する予定（計画 S7）",
		},
		{
			name: "タイムゾーンが違っても同一時刻ならスキップする",
			src:  src("a.txt", 100, base),
			dest: []*hbg.FileInfo{
				src("a.txt", 100, base.In(time.FixedZone("JST", 9*60*60))),
			},
			updateDuration: time.Second,
			want:           true,
			why:            "UTC に正規化してから比較している",
		},
		{
			name: "同名が複数あるとき、どれか1つでも条件を満たせばスキップする",
			src:  src("a.txt", 100, base),
			dest: []*hbg.FileInfo{
				src("a.txt", 999, base),
				src("a.txt", 100, base),
			},
			updateDuration: time.Second,
			want:           true,
			why:            "Google Drive は同一ディレクトリに同名ファイルを許すため実際に起こりうる",
		},
		{
			name:           "更新期間0なら完全一致した時刻のみスキップする",
			src:            src("a.txt", 100, base),
			dest:           []*hbg.FileInfo{src("a.txt", 100, base)},
			updateDuration: 0,
			want:           true,
		},
		{
			name:           "更新期間0で1ナノ秒でもずれればスキップしない",
			src:            src("a.txt", 100, base.Add(time.Nanosecond)),
			dest:           []*hbg.FileInfo{src("a.txt", 100, base)},
			updateDuration: 0,
			want:           false,
		},
		{
			name:           "サイズ0同士でもスキップ判定は働く",
			src:            src("empty.txt", 0, base),
			dest:           []*hbg.FileInfo{src("empty.txt", 0, base)},
			updateDuration: time.Second,
			want:           true,
		},
		{
			name:           "FAT の2秒粒度は既定の1秒では吸収できない",
			src:            src("a.txt", 100, base.Add(2*time.Second)),
			dest:           []*hbg.FileInfo{src("a.txt", 100, base)},
			updateDuration: time.Second,
			want:           false,
			why:            "--update_duration 2s を指定しないと毎回再転送になる",
		},
		{
			name:           "更新期間を2秒にすればFATの粒度を吸収できる",
			src:            src("a.txt", 100, base.Add(2*time.Second)),
			dest:           []*hbg.FileInfo{src("a.txt", 100, base)},
			updateDuration: 2 * time.Second,
			want:           true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := shouldSkipCopy(tt.src, tt.dest, tt.updateDuration)
			if got != tt.want {
				t.Errorf("shouldSkipCopy() = %v, want %v", got, tt.want)
				if tt.why != "" {
					t.Logf("補足: %s", tt.why)
				}
			}
		})
	}
}

func TestGlob(t *testing.T) {
	t.Parallel()

	files := []*hbg.FileInfo{
		{Name: "a.txt", Path: "/data/a.txt"},
		{Name: "b.txt", Path: "/data/b.txt"},
		{Name: "c.jpg", Path: "/data/c.jpg"},
		{Name: "sub", Path: "/data/sub", IsDir: true},
	}

	tests := []struct {
		name    string
		pattern string
		want    []string
	}{
		{name: "完全一致", pattern: "/data/a.txt", want: []string{"/data/a.txt"}},
		{name: "拡張子ワイルドカード", pattern: "/data/*.txt", want: []string{"/data/a.txt", "/data/b.txt"}},
		{name: "全件", pattern: "/data/*", want: []string{"/data/a.txt", "/data/b.txt", "/data/c.jpg", "/data/sub"}},
		{name: "一致なし", pattern: "/data/*.png", want: []string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := glob(files, tt.pattern)
			if err != nil {
				t.Fatalf("glob() unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("glob() returned %d files, want %d: %v", len(got), len(tt.want), got)
			}
			for i, f := range got {
				if f.Path != tt.want[i] {
					t.Errorf("glob()[%d].Path = %q, want %q", i, f.Path, tt.want[i])
				}
			}
		})
	}
}

// glob はユーザーが指定したコピー元パスをそのままパターンとして解釈するため、
// パターンとして不正な文字を含むファイル名を渡すとパニックする。
// これは既知の不具合であり、計画 S7 で MustCompile を排除して解消する。
// ここではその事実を記録し、修正時にこのテストを反転させる。
func TestGlobPanicsOnMalformedPattern(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Error("glob() がパニックしなくなっている。" +
				"MustCompile を排除した場合は、このテストを『エラーを返すこと』の検証に書き換えること")
		}
	}()

	files := []*hbg.FileInfo{{Name: "a[.txt", Path: "/data/a[.txt"}}
	_, _ = glob(files, "/data/a[.txt")
}
