package transfer_test

import (
	"context"
	"strings"
	"testing"

	"github.com/mt3hr/hbg/transfer"
)

func TestFilterMatch(t *testing.T) {
	tests := []struct {
		name string
		spec transfer.FilterSpec
		path string
		size int64
		want bool
	}{
		{
			name: "指定がなければすべて対象",
			spec: transfer.FilterSpec{}, path: "a/b.txt", size: 100, want: true,
		},
		{
			name: "名前が一致したら除外",
			spec: transfer.FilterSpec{Ignore: []string{"Thumbs.db"}},
			path: "photos/Thumbs.db", size: 100, want: false,
		},
		{
			name: "拡張子で絞り込む",
			spec: transfer.FilterSpec{Include: []string{"*.jpg"}},
			path: "photos/a.jpg", size: 100, want: true,
		},
		{
			name: "絞り込みに合わないものは対象外",
			spec: transfer.FilterSpec{Include: []string{"*.jpg"}},
			path: "photos/a.txt", size: 100, want: false,
		},
		{
			name: "パターンで除外する",
			spec: transfer.FilterSpec{Exclude: []string{"*.tmp"}},
			path: "work/a.tmp", size: 100, want: false,
		},
		{
			// 以前は名前の完全一致しか見ておらず、
			// 「特定のディレクトリの下だけ除く」ができなかった。
			name: "パスで除外する",
			spec: transfer.FilterSpec{Exclude: []string{"cache/**"}},
			path: "cache/x/y.txt", size: 100, want: false,
		},
		{
			name: "* はディレクトリの境界を越えない",
			spec: transfer.FilterSpec{Include: []string{"*.txt"}},
			path: "a/b/c.txt", size: 100, want: true, // 名前でも照合するため一致する
		},
		{
			name: "小さすぎるものを除外",
			spec: transfer.FilterSpec{MinSize: 1000},
			path: "a.txt", size: 100, want: false,
		},
		{
			name: "大きすぎるものを除外",
			spec: transfer.FilterSpec{MaxSize: 1000},
			path: "a.bin", size: 5000, want: false,
		},
		{
			name: "範囲内なら対象",
			spec: transfer.FilterSpec{MinSize: 100, MaxSize: 1000},
			path: "a.bin", size: 500, want: true,
		},
		{
			name: "除外は絞り込みより優先",
			spec: transfer.FilterSpec{Include: []string{"*.jpg"}, Exclude: []string{"secret*"}},
			path: "secret.jpg", size: 100, want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := transfer.NewFilter(tt.spec)
			if err != nil {
				t.Fatalf("NewFilter: %v", err)
			}
			if got := f.Match(tt.path, tt.size); got != tt.want {
				t.Errorf("Match(%q, %d) = %v, want %v", tt.path, tt.size, got, tt.want)
			}
		})
	}
}

// パターンの誤りは、起動時にエラーとして知らせることを確認します。
//
// 以前はコピー元のパスをそのままパターンとして解釈しており、
// [ や { を含むファイル名を指定するとパニックしていました。
func TestNewFilterRejectsBadPattern(t *testing.T) {
	_, err := transfer.NewFilter(transfer.FilterSpec{Exclude: []string{"[不正な"}})
	if err == nil {
		t.Fatal("不正なパターンなのに成功した")
	}
	if !strings.Contains(err.Error(), "--exclude") {
		t.Errorf("どのフラグの指定が悪いのか分からない: %v", err)
	}
}

// 記号を含むファイル名でもパニックしないことを確認します。
func TestFilterHandlesSpecialCharactersInNames(t *testing.T) {
	f, err := transfer.NewFilter(transfer.FilterSpec{})
	if err != nil {
		t.Fatalf("NewFilter: %v", err)
	}

	for _, name := range []string{"a[1].txt", "b{2}.txt", "c*.txt", "d?.txt"} {
		if !f.Match(name, 10) {
			t.Errorf("%q が対象外になっている", name)
		}
	}
}

func TestFilterMatchDir(t *testing.T) {
	f, err := transfer.NewFilter(transfer.FilterSpec{
		Ignore:  []string{".git"},
		Exclude: []string{"cache/**", "node_modules"},
	})
	if err != nil {
		t.Fatalf("NewFilter: %v", err)
	}

	tests := map[string]bool{
		"src":          true,
		".git":         false,
		"node_modules": false,
		"cache":        false,
	}
	for dir, want := range tests {
		if got := f.MatchDir(dir); got != want {
			t.Errorf("MatchDir(%q) = %v, want %v", dir, got, want)
		}
	}
}

// エンジン全体として絞り込みが働くことを確認します。
func TestRunWithFilter(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.jpg", "1")
	put(t, src, "/data/b.txt", "2")
	put(t, src, "/data/cache/c.jpg", "3")

	opts := baseOptions(src, dst)
	opts.Filter = mustFilter(t, transfer.FilterSpec{
		Include: []string{"*.jpg"},
		Exclude: []string{"cache/**"},
	})

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Transferred != 1 {
		t.Errorf("Transferred=%d, want 1（a.jpg だけ）", result.Transferred)
	}

	snap := dst.Snapshot()
	if _, ok := snap["/backup/data/a.jpg"]; !ok {
		t.Error("a.jpg が転送されていない")
	}
	if _, ok := snap["/backup/data/b.txt"]; ok {
		t.Error("絞り込みに合わない b.txt が転送されている")
	}
	if _, ok := snap["/backup/data/cache/c.jpg"]; ok {
		t.Error("除外したはずの cache/c.jpg が転送されている")
	}
}
