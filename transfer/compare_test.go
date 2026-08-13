package transfer_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mt3hr/hbg/backend/memory"
	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/transfer"
)

// 転送の要否の判断を、規則の表そのままに確認します。
//
// 以前は時刻の差を絶対値で見ていたため、コピー先のほうが新しくても
// 差が許容幅を超えていれば上書きしていました。
func TestComparerDecisionTable(t *testing.T) {
	base := time.Date(2024, 5, 1, 12, 0, 0, 0, time.UTC)
	const window = 2 * time.Second

	info := func(size int64, mod time.Time) storage.FileInfo {
		return storage.FileInfo{Name: "a.txt", Size: size, ModTime: mod}
	}

	tests := []struct {
		name       string
		src        storage.FileInfo
		dst        *storage.FileInfo
		update     bool
		wantAction transfer.Action
		wantReason string
	}{
		{
			name: "コピー先にない",
			src:  info(100, base), dst: nil,
			update: true, wantAction: transfer.ActionCopy, wantReason: "コピー先にない",
		},
		{
			name: "時刻差が許容幅内でサイズも同じ",
			src:  info(100, base.Add(time.Second)), dst: ptr(info(100, base)),
			update: true, wantAction: transfer.ActionSkip,
		},
		{
			name: "時刻差が許容幅内だがサイズが違う",
			src:  info(101, base.Add(time.Second)), dst: ptr(info(100, base)),
			update: true, wantAction: transfer.ActionCopy, wantReason: "サイズが違う",
		},
		{
			name: "コピー元のほうが新しい",
			src:  info(100, base.Add(time.Hour)), dst: ptr(info(100, base)),
			update: true, wantAction: transfer.ActionCopy, wantReason: "コピー元のほうが新しい",
		},
		{
			// これが以前は上書きされていた
			name: "コピー先のほうが新しい（--update）",
			src:  info(100, base), dst: ptr(info(100, base.Add(time.Hour))),
			update: true, wantAction: transfer.ActionSkip, wantReason: "コピー先のほうが新しい",
		},
		{
			name: "コピー先のほうが新しい（--overwrite）",
			src:  info(100, base), dst: ptr(info(100, base.Add(time.Hour))),
			update: false, wantAction: transfer.ActionCopy, wantReason: "上書きする",
		},
		{
			// 更新時刻の判断はサイズの比較より先に行う必要がある。
			// 先にサイズを見てしまうと、--update を指定していても
			// 新しいファイルが古いもので上書きされる。
			name: "コピー先のほうが新しく、サイズも違う（--update）",
			src:  info(50, base), dst: ptr(info(100, base.Add(time.Hour))),
			update: true, wantAction: transfer.ActionSkip, wantReason: "コピー先のほうが新しい",
		},
		{
			name: "境界ちょうど（許容幅と同じ差）",
			src:  info(100, base.Add(window)), dst: ptr(info(100, base)),
			update: true, wantAction: transfer.ActionSkip,
		},
		{
			name: "境界を1ナノ秒超える",
			src:  info(100, base.Add(window+time.Nanosecond)), dst: ptr(info(100, base)),
			update: true, wantAction: transfer.ActionCopy, wantReason: "コピー元のほうが新しい",
		},
		{
			name: "コピー元の時刻が不明ならサイズだけで判断",
			src:  info(100, time.Time{}), dst: ptr(info(100, base)),
			update: true, wantAction: transfer.ActionSkip,
		},
		{
			name: "コピー元の時刻が不明でサイズが違えば転送",
			src:  info(200, time.Time{}), dst: ptr(info(100, base)),
			update: true, wantAction: transfer.ActionCopy, wantReason: "サイズが違う",
		},
	}

	src, dst := memory.New("src"), memory.New("dst")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := transfer.NewComparer(transfer.ComparePolicy{
				Fields:       []transfer.CompareField{transfer.CompareSize, transfer.CompareModTime},
				ModifyWindow: window,
				Update:       tt.update,
			}, src, dst)
			if err != nil {
				t.Fatalf("NewComparer: %v", err)
			}

			action, reason, err := c.Decide(context.Background(), tt.src, tt.dst)
			if err != nil {
				t.Fatalf("Decide: %v", err)
			}
			if action != tt.wantAction {
				t.Errorf("action = %v, want %v（理由: %s）", action, tt.wantAction, reason)
			}
			if tt.wantReason != "" && !strings.Contains(reason, tt.wantReason) {
				t.Errorf("理由 = %q, %q を含むべき", reason, tt.wantReason)
			}
		})
	}
}

func ptr(v storage.FileInfo) *storage.FileInfo { return &v }

// 許容幅が両側の分解能から決まることを確認します。
func TestComparerResolvesWindow(t *testing.T) {
	src := memory.New("src") // 分解能は1ナノ秒
	dst := memory.New("dst")

	c, err := transfer.NewComparer(transfer.DefaultComparePolicy(), src, dst)
	if err != nil {
		t.Fatalf("NewComparer: %v", err)
	}
	// 分解能が細かくても、最低1秒は見る
	if got := c.Window(); got < time.Second {
		t.Errorf("Window = %v, 最低でも1秒であるべき", got)
	}

	// 明示した値のほうが大きければそちらを使う
	c, err = transfer.NewComparer(transfer.ComparePolicy{
		Fields:       []transfer.CompareField{transfer.CompareSize, transfer.CompareModTime},
		ModifyWindow: 2 * time.Second,
	}, src, dst)
	if err != nil {
		t.Fatalf("NewComparer: %v", err)
	}
	if got := c.Window(); got != 2*time.Second {
		t.Errorf("Window = %v, want 2s", got)
	}
}

// 共通のハッシュがない組み合わせでハッシュ比較を求められたら、
// 黙ってサイズ比較に落とさずエラーにすることを確認します。
func TestComparerRejectsHashWithoutCommonAlgorithm(t *testing.T) {
	src := memory.New("src")
	dst := &noHashStorage{Storage: memory.New("dst")}

	_, err := transfer.NewComparer(transfer.ComparePolicy{
		Fields: []transfer.CompareField{transfer.CompareSize, transfer.CompareHash},
	}, src, dst)
	if err == nil {
		t.Fatal("共通のハッシュがないのに成功した")
	}
	if !strings.Contains(err.Error(), "共通して使えるハッシュ") {
		t.Errorf("err = %v", err)
	}
}

// noHashStorage はハッシュを扱えないストレージです。
type noHashStorage struct {
	storage.Storage
}

func (n *noHashStorage) Features() *storage.Features {
	f := *n.Storage.Features()
	f.Hashes = nil
	return &f
}

// 更新時刻を保持できないストレージ相手に時刻での比較を求められたら、
// 毎回転送し直すことになるので、あらかじめ知らせることを確認します。
func TestComparerRejectsModTimeWhenNotSupported(t *testing.T) {
	src := memory.New("src")
	dst := &noModTimeStorage{Storage: memory.New("dst")}

	_, err := transfer.NewComparer(transfer.DefaultComparePolicy(), src, dst)
	if err == nil {
		t.Fatal("更新時刻を保持できないのに成功した")
	}
	if !strings.Contains(err.Error(), "最終更新時刻") {
		t.Errorf("err = %v", err)
	}
}

type noModTimeStorage struct {
	storage.Storage
}

func (n *noModTimeStorage) Features() *storage.Features {
	f := *n.Storage.Features()
	f.CanSetModTime = false
	return &f
}

// ハッシュでの比較が実際に働くことを確認します。
func TestComparerUsesHash(t *testing.T) {
	ctx := context.Background()
	src, dst := memory.New("src"), memory.New("dst")

	// サイズも更新時刻も同じだが内容が違う
	modTime := time.Now()
	put := func(s *memory.Storage, path, content string) {
		if _, err := s.Put(ctx, path, strings.NewReader(content),
			storage.ObjectMeta{Size: int64(len(content)), ModTime: modTime}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	put(src, "/a.txt", "AAA")
	put(dst, "/a.txt", "BBB")

	srcInfo, _ := src.Stat(ctx, "/a.txt")
	dstInfo, _ := dst.Stat(ctx, "/a.txt")

	// サイズと時刻だけならスキップされる
	sizeTime, err := transfer.NewComparer(transfer.DefaultComparePolicy(), src, dst)
	if err != nil {
		t.Fatalf("NewComparer: %v", err)
	}
	if action, _, _ := sizeTime.Decide(ctx, *srcInfo, dstInfo); action != transfer.ActionSkip {
		t.Error("サイズと時刻が同じならスキップされるべき")
	}

	// ハッシュを見れば違いに気づく
	withHash, err := transfer.NewComparer(transfer.ComparePolicy{
		Fields: []transfer.CompareField{transfer.CompareSize, transfer.CompareHash},
	}, src, dst)
	if err != nil {
		t.Fatalf("NewComparer: %v", err)
	}
	action, reason, err := withHash.Decide(ctx, *srcInfo, dstInfo)
	if err != nil {
		t.Fatalf("Decide: %v", err)
	}
	if action != transfer.ActionCopy {
		t.Errorf("内容が違うのに転送されない（理由: %s）", reason)
	}
}

func TestComparerIgnoreExisting(t *testing.T) {
	src, dst := memory.New("src"), memory.New("dst")

	c, err := transfer.NewComparer(transfer.ComparePolicy{
		Fields:         []transfer.CompareField{transfer.CompareSize},
		IgnoreExisting: true,
	}, src, dst)
	if err != nil {
		t.Fatalf("NewComparer: %v", err)
	}

	srcInfo := storage.FileInfo{Name: "a.txt", Size: 100}
	dstInfo := &storage.FileInfo{Name: "a.txt", Size: 999}

	action, _, _ := c.Decide(context.Background(), srcInfo, dstInfo)
	if action != transfer.ActionSkip {
		t.Error("--ignore-existing なのに転送される")
	}
}

func TestParseCompareFields(t *testing.T) {
	tests := []struct {
		in      string
		want    []transfer.CompareField
		wantErr bool
	}{
		{in: "size", want: []transfer.CompareField{transfer.CompareSize}},
		{in: "size,modtime", want: []transfer.CompareField{transfer.CompareSize, transfer.CompareModTime}},
		{in: "size, hash", want: []transfer.CompareField{transfer.CompareSize, transfer.CompareHash}},
		{in: "SIZE", want: []transfer.CompareField{transfer.CompareSize}},
		{in: "", wantErr: true},
		{in: "そんな項目はない", wantErr: true},
	}

	for _, tt := range tests {
		got, err := transfer.ParseCompareFields(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseCompareFields(%q) がエラーにならない", tt.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseCompareFields(%q): %v", tt.in, err)
			continue
		}
		if len(got) != len(tt.want) {
			t.Errorf("ParseCompareFields(%q) = %v, want %v", tt.in, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("ParseCompareFields(%q) = %v, want %v", tt.in, got, tt.want)
				break
			}
		}
	}
}

// エンジン全体として、コピー先のほうが新しいファイルを
// 上書きしないことを確認します。
func TestRunDoesNotOverwriteNewerDestination(t *testing.T) {
	ctx := context.Background()
	src, dst := memory.New("src"), memory.New("dst")

	old := time.Now().Add(-time.Hour)
	newer := time.Now()

	if _, err := src.Put(ctx, "/data/a.txt", strings.NewReader("古い内容"),
		storage.ObjectMeta{Size: 12, ModTime: old}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := dst.Put(ctx, "/backup/data/a.txt", strings.NewReader("新しい内容"),
		storage.ObjectMeta{Size: 15, ModTime: newer}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	opts := baseOptions(src, dst)
	result, err := transfer.Run(ctx, opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Transferred != 0 {
		t.Errorf("Transferred=%d, コピー先のほうが新しいので転送されないべき", result.Transferred)
	}
	if got := dst.Snapshot()["/backup/data/a.txt"]; got != "新しい内容" {
		t.Errorf("転送先が上書きされている: %q", got)
	}

	// --overwrite なら上書きする
	opts.Compare.Update = false
	if _, err := transfer.Run(ctx, opts); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := dst.Snapshot()["/backup/data/a.txt"]; got != "古い内容" {
		t.Errorf("--overwrite なのに上書きされない: %q", got)
	}
}
