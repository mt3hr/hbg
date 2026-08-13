package local_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mt3hr/hbg/backend/local"
	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/storage/storagetest"
)

// 適合性テストを通すことが、このバックエンドの受け入れ条件です。
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			return local.New("local"), filepath.ToSlash(t.TempDir())
		},
	})
}

// 中断した書き込みの一時ファイルが残らないことを確認します。
func TestPutLeavesNoPartialFile(t *testing.T) {
	ctx := context.Background()
	s := local.New("local")
	root := filepath.ToSlash(t.TempDir())

	// 読み取りの途中で失敗する Reader
	r := &failingReader{data: strings.Repeat("x", 100000), failAfter: 4096}

	_, err := s.Put(ctx, root+"/broken.txt", r, storage.ObjectMeta{Size: 100000})
	if err == nil {
		t.Fatal("読み取りが失敗したのに Put が成功した")
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		t.Errorf("失敗した書き込みのファイルが残っている: %s", e.Name())
	}
}

type failingReader struct {
	data      string
	off       int
	failAfter int
}

func (f *failingReader) Read(p []byte) (int, error) {
	if f.off >= f.failAfter {
		return 0, errRead
	}
	n := copy(p, f.data[f.off:])
	f.off += n
	return n, nil
}

var errRead = &readError{}

type readError struct{}

func (*readError) Error() string { return "読み取りに失敗しました" }

// シンボリックリンクを辿らないことを確認します。
// 以前は os.Stat を使っており、リンク先を見ていました。
func TestListDoesNotFollowSymlinks(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("Windows ではシンボリックリンクの作成に権限が要るため飛ばします")
	}

	ctx := context.Background()
	s := local.New("local")
	root := t.TempDir()

	target := filepath.Join(root, "target.txt")
	if err := os.WriteFile(target, []byte("内容"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	link := filepath.Join(root, "link.txt")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("シンボリックリンクを作れないため飛ばします: %v", err)
	}

	entries, err := storage.ListAll(ctx, s, filepath.ToSlash(root))
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("List が %d 件, want 2", len(entries))
	}
}

func TestFeaturesDeclaresOptionalInterfaces(t *testing.T) {
	s := local.New("local")

	// 申告した能力が実際に実装されていること
	if _, ok := any(s).(storage.Hasher); !ok {
		t.Error("Hasher を実装していない")
	}
	if _, ok := any(s).(storage.Mover); !ok {
		t.Error("Mover を実装していない")
	}
	if _, ok := any(s).(storage.Purger); !ok {
		t.Error("Purger を実装していない")
	}
	if _, ok := any(s).(storage.RangeOpener); !ok {
		t.Error("RangeOpener を実装していない")
	}
	if _, ok := any(s).(storage.SetModTimer); !ok {
		t.Error("SetModTimer を実装していない")
	}

	f := s.Features()
	if !f.AtomicPut {
		t.Error("一時ファイル経由で書いているので AtomicPut は真であるべき")
	}
	if !f.OSPath {
		t.Error("OS のパス規則に従うので OSPath は真であるべき")
	}
}
