package storage_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/mt3hr/hbg/backend/memory"
	"github.com/mt3hr/hbg/storage"
)

// "hello world" の既知のハッシュ（各アルゴリズムの定義から得られる値）。
var helloWorld = map[storage.HashType]string{
	storage.MD5:    "5eb63bbbe01eeed093cb22bb8f5acdc3",
	storage.SHA1:   "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
	storage.SHA256: "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9",
}

func init() {
	// 4MiB 未満なら、Dropbox の content hash は SHA-256 をもう一度 SHA-256 したもの。
	first := sha256.Sum256([]byte("hello world"))
	second := sha256.Sum256(first[:])
	helloWorld[storage.DropboxContent] = hex.EncodeToString(second[:])
}

// noHasher は Hasher を隠した包みです。
// ハッシュを持たないストレージ（SFTP など）の代わりに使います。
type noHasher struct{ storage.Storage }

// failingHasher は Hasher が決まったエラーを返す包みです。
type failingHasher struct {
	storage.Storage
	err error
}

func (f failingHasher) Hash(context.Context, string, storage.HashType) (string, error) {
	return "", f.err
}

func putFile(t *testing.T, s storage.Storage, p, content string) storage.FileInfo {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Put(ctx, p, strings.NewReader(content), storage.ObjectMeta{Size: int64(len(content))}); err != nil {
		t.Fatalf("Put(%s): %v", p, err)
	}
	info, err := s.Stat(ctx, p)
	if err != nil {
		t.Fatalf("Stat(%s): %v", p, err)
	}
	return *info
}

// countOpens は Open が呼ばれた回数を数えます。
func countOpens(m *memory.Storage) *atomic.Int32 {
	var n atomic.Int32
	m.SetHooks(memory.Hooks{BeforeOp: func(op, _ string) error {
		if op == "open" {
			n.Add(1)
		}
		return nil
	}})
	return &n
}

func TestHashFileAllTypes(t *testing.T) {
	m := memory.New("mem")
	info := putFile(t, m, "/a.txt", "hello world")

	stores := map[string]storage.Storage{
		"Hasher を使う":        m,
		"Hasher がなければ読み出す":  noHasher{m},
		"Hasher が対応外なら読み出す": failingHasher{m, fmt.Errorf("%w: 試験", storage.ErrUnsupported)},
	}
	for name, s := range stores {
		for ht, want := range helloWorld {
			t.Run(name+"/"+string(ht), func(t *testing.T) {
				got, err := storage.HashFile(context.Background(), s, info, ht)
				if err != nil {
					t.Fatalf("HashFile: %v", err)
				}
				if got != want {
					t.Errorf("got %s, want %s", got, want)
				}
			})
		}
	}
}

// メタデータにあるハッシュは、読み出さずにそのまま使います。
func TestHashFileUsesMetadata(t *testing.T) {
	m := memory.New("mem")
	info := putFile(t, m, "/a.txt", "hello world")
	opens := countOpens(m)

	info.Hashes = map[storage.HashType]string{storage.MD5: "ABCDEF"}
	got, err := storage.HashFile(context.Background(), noHasher{m}, info, storage.MD5)
	if err != nil {
		t.Fatalf("HashFile: %v", err)
	}
	if got != "abcdef" {
		t.Errorf("got %s, want abcdef（小文字にそろえる）", got)
	}
	if n := opens.Load(); n != 0 {
		t.Errorf("Open が %d 回呼ばれた。メタデータがあれば読み出さないはず", n)
	}
}

// 「対応していない」以外の失敗は、読み出しに落とさずにそのまま返します。
func TestHashFileOtherErrorIsReturned(t *testing.T) {
	m := memory.New("mem")
	info := putFile(t, m, "/a.txt", "hello world")
	opens := countOpens(m)

	boom := errors.New("通信に失敗")
	_, err := storage.HashFile(context.Background(), failingHasher{m, boom}, info, storage.SHA256)
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if n := opens.Load(); n != 0 {
		t.Errorf("Open が %d 回呼ばれた", n)
	}
}

func TestHashFileRejects(t *testing.T) {
	m := memory.New("mem")
	info := putFile(t, m, "/a.txt", "hello world")
	opens := countOpens(m)

	if _, err := storage.HashFile(context.Background(), noHasher{m}, info, storage.QuickXor); !errors.Is(err, storage.ErrUnsupported) {
		t.Errorf("quickxor: err = %v, want ErrUnsupported", err)
	}
	if n := opens.Load(); n != 0 {
		t.Errorf("計算できない種類なのに Open が %d 回呼ばれた", n)
	}

	dir := storage.FileInfo{Path: "/", IsDir: true}
	if _, err := storage.HashFile(context.Background(), m, dir, storage.SHA256); !errors.Is(err, storage.ErrIsDir) {
		t.Errorf("ディレクトリ: err = %v, want ErrIsDir", err)
	}
}

// buildTree は試験用の木を作ります。
func buildTree(t *testing.T) *memory.Storage {
	t.Helper()
	m := memory.New("mem")
	putFile(t, m, "/root/b.txt", "b")
	putFile(t, m, "/root/a.txt", "a")
	putFile(t, m, "/root/sub/c.txt", "c")
	putFile(t, m, "/root/sub/deep/d.txt", "d")
	putFile(t, m, "/root/a.txt.bak", "a2")
	if err := m.Mkdir(context.Background(), "/root/empty"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	putFile(t, m, "/outside.txt", "x")
	return m
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestHashTree(t *testing.T) {
	m := buildTree(t)

	wantPaths := []string{"a.txt", "a.txt.bak", "b.txt", "sub/c.txt", "sub/deep/d.txt"}
	wantContent := []string{"a", "a2", "b", "c", "d"}

	var manifest strings.Builder
	for i, p := range wantPaths {
		manifest.WriteString(sha256Hex(wantContent[i]) + "  " + p + "\n")
	}
	wantSum := sha256Hex(manifest.String())

	for _, workers := range []int{1, 4} {
		for name, s := range map[string]storage.Storage{"Hasher あり": m, "Hasher なし": noHasher{m}} {
			t.Run(fmt.Sprintf("%s/workers=%d", name, workers), func(t *testing.T) {
				var calls atomic.Int32
				res, err := storage.HashTree(context.Background(), s, "/root", storage.SHA256, workers,
					func(storage.TreeEntry) { calls.Add(1) })
				if err != nil {
					t.Fatalf("HashTree: %v", err)
				}
				if len(res.Entries) != len(wantPaths) {
					t.Fatalf("件数 %d, want %d: %+v", len(res.Entries), len(wantPaths), res.Entries)
				}
				for i, e := range res.Entries {
					if e.RelPath != wantPaths[i] {
						t.Errorf("[%d] RelPath = %s, want %s", i, e.RelPath, wantPaths[i])
					}
					if e.Hash != sha256Hex(wantContent[i]) {
						t.Errorf("[%d] Hash = %s", i, e.Hash)
					}
				}
				if res.Sum != wantSum {
					t.Errorf("Sum = %s, want %s", res.Sum, wantSum)
				}
				if res.Failed != 0 {
					t.Errorf("Failed = %d", res.Failed)
				}
				if int(calls.Load()) != len(wantPaths) {
					t.Errorf("fn の呼び出し %d 回, want %d", calls.Load(), len(wantPaths))
				}
			})
		}
	}
}

// 空のディレクトリはまとめた値に影響しません。
func TestHashTreeIgnoresEmptyDirs(t *testing.T) {
	m := buildTree(t)
	ctx := context.Background()

	before, err := storage.HashTree(ctx, m, "/root", storage.SHA256, 2, nil)
	if err != nil {
		t.Fatalf("HashTree: %v", err)
	}
	if err := m.Mkdir(ctx, "/root/sub/another-empty"); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	after, err := storage.HashTree(ctx, m, "/root", storage.SHA256, 2, nil)
	if err != nil {
		t.Fatalf("HashTree: %v", err)
	}
	if before.Sum != after.Sum {
		t.Errorf("空のディレクトリを足すと値が変わる: %s -> %s", before.Sum, after.Sum)
	}
}

// 1件の失敗では止まらず、まとめた値は出しません。
func TestHashTreePartialFailure(t *testing.T) {
	m := buildTree(t)
	m.SetHooks(memory.Hooks{BeforeOp: func(op, p string) error {
		if op == "open" && strings.HasSuffix(p, "/sub/c.txt") {
			return errors.New("読めない")
		}
		return nil
	}})

	res, err := storage.HashTree(context.Background(), noHasher{m}, "/root", storage.SHA256, 2, nil)
	if err != nil {
		t.Fatalf("HashTree: %v", err)
	}
	if res.Failed != 1 {
		t.Errorf("Failed = %d, want 1", res.Failed)
	}
	if res.Sum != "" {
		t.Errorf("失敗があるのに Sum = %s", res.Sum)
	}
	ok := 0
	for _, e := range res.Entries {
		if e.Err == nil && e.Hash != "" {
			ok++
		}
		if e.RelPath == "sub/c.txt" && e.Err == nil {
			t.Error("sub/c.txt が失敗として記録されていない")
		}
	}
	if ok != 4 {
		t.Errorf("成功した件数 %d, want 4", ok)
	}
}

// 一覧の失敗は全体を止めます。
func TestHashTreeListFailure(t *testing.T) {
	m := buildTree(t)
	boom := errors.New("一覧できない")
	m.SetHooks(memory.Hooks{BeforeOp: func(op, p string) error {
		if op == "list" && strings.HasSuffix(p, "/sub") {
			return boom
		}
		return nil
	}})
	if _, err := storage.HashTree(context.Background(), m, "/root", storage.SHA256, 2, nil); !errors.Is(err, boom) {
		t.Errorf("err = %v, want %v", err, boom)
	}
}

func TestHashTreeCanceled(t *testing.T) {
	m := buildTree(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := storage.HashTree(ctx, m, "/root", storage.SHA256, 2, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

func TestHashTreeMissingDir(t *testing.T) {
	m := buildTree(t)
	if _, err := storage.HashTree(context.Background(), m, "/nothing", storage.SHA256, 2, nil); !storage.IsNotFound(err) {
		t.Errorf("err = %v, want ErrNotFound", err)
	}
}
