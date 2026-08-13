// Package storagetest は、すべてのバックエンドが満たすべき振る舞いを
// 共通のテストとして提供します。
//
// 新しいバックエンドを追加するときは、このスイートを通すことを
// 受け入れの条件にします。バックエンドごとに個別のテストを書く量が
// 減るうえ、「このストレージだけ日本語のファイル名で壊れる」といった
// 非対称な不具合を構造的に防げます。
//
// ストレージにできないことは Features を見て自動的に飛ばします。
package storagetest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/mt3hr/hbg/storage"
)

// Harness はテスト対象のストレージを用意する方法です。
type Harness struct {
	// NewStorage はテスト用のストレージと、書き込んでよい空のディレクトリを返します。
	// 後始末は t.Cleanup で行ってください。
	NewStorage func(t *testing.T) (storage.Storage, string)

	// SkipLargeDirs を真にすると、件数の多いディレクトリの試験を飛ばします。
	// 実際のクラウドに対して実行する場合に使います。
	SkipLargeDirs bool

	// LargeDirCount は件数の多いディレクトリの試験で作る数です。0 なら既定値。
	LargeDirCount int
}

// Run は適合性テストを実行します。
func Run(t *testing.T, h Harness) {
	t.Helper()

	tests := []struct {
		name string
		fn   func(*testing.T, Harness)
	}{
		{"基本操作", testBasics},
		{"サイズ不明でも全部書き込まれる", testSizeUnknown},
		{"空のファイル", testEmptyFile},
		{"上書き", testOverwrite},
		{"存在しないものへの操作", testNotFound},
		{"ディレクトリ", testDirectories},
		{"入れ子のディレクトリ", testNestedDirectories},
		{"更新時刻の保持", testModTime},
		{"変わった名前のファイル", testUnusualNames},
		{"Listの打ち切り", testListEarlyStop},
		{"件数の多いディレクトリ", testManyEntries},
		{"取り消し", testCancellation},
		{"書き込み中の取り消しで壊れたものが残らない", testCancelDuringPut},
		{"並行して書き込む", testConcurrentPut},
		{"Featuresとの整合", testFeaturesConsistency},
		{"ハッシュ", testHash},
		{"範囲読み出し", testRangeOpen},
		{"移動", testMove},
		{"まとめて削除", testPurge},
		{"大きめのファイル", testLargerFile},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(t, h)
		})
	}
}

// --- 補助 ---

func setup(t *testing.T, h Harness) (context.Context, storage.Storage, string) {
	t.Helper()
	s, root := h.NewStorage(t)
	return context.Background(), s, root
}

func put(t *testing.T, ctx context.Context, s storage.Storage, p, content string) *storage.FileInfo {
	t.Helper()
	info, err := s.Put(ctx, p, strings.NewReader(content), storage.ObjectMeta{
		Size: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("Put(%s): %v", p, err)
	}
	return info
}

func read(t *testing.T, ctx context.Context, s storage.Storage, p string) string {
	t.Helper()
	rc, _, err := s.Open(ctx, p)
	if err != nil {
		t.Fatalf("Open(%s): %v", p, err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("読み取り(%s): %v", p, err)
	}
	return string(data)
}

func listNames(t *testing.T, ctx context.Context, s storage.Storage, dir string) []string {
	t.Helper()
	entries, err := storage.ListAllSorted(ctx, s, dir)
	if err != nil {
		t.Fatalf("List(%s): %v", dir, err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name)
	}
	return names
}

// --- 各テスト ---

func testBasics(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	p := path.Join(root, "hello.txt")
	info := put(t, ctx, s, p, "hello")

	if info.Size != 5 {
		t.Errorf("Put が返した Size = %d, want 5", info.Size)
	}
	if got := read(t, ctx, s, p); got != "hello" {
		t.Errorf("内容 = %q, want hello", got)
	}

	stat, err := s.Stat(ctx, p)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Size != 5 {
		t.Errorf("Stat の Size = %d, want 5", stat.Size)
	}
	if stat.IsDir {
		t.Error("ファイルなのに IsDir が真")
	}
	if stat.Name != "hello.txt" {
		t.Errorf("Name = %q, want hello.txt", stat.Name)
	}

	if names := listNames(t, ctx, s, root); len(names) != 1 || names[0] != "hello.txt" {
		t.Errorf("List = %v, want [hello.txt]", names)
	}

	if err := s.Remove(ctx, p); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := s.Stat(ctx, p); !storage.IsNotFound(err) {
		t.Errorf("削除後の Stat = %v, ErrNotFound であるべき", err)
	}
}

// サイズが分からない場合でも、内容がすべて書き込まれることを確認します。
//
// 以前の Dropbox 実装は、宣言されたサイズだけを見て転送方法を決めており、
// サイズが 0 として渡されると内容が先頭だけに切り詰められ、
// しかもエラーにならないという不具合がありました。
func testSizeUnknown(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	content := strings.Repeat("量が分かっていない内容。", 5000)
	p := path.Join(root, "unknown-size.txt")

	// サイズを SizeUnknown として、Reader からも長さを推測できない形で渡す
	info, err := s.Put(ctx, p, io.NopCloser(strings.NewReader(content)), storage.ObjectMeta{
		Size: storage.SizeUnknown,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	if info.Size != int64(len(content)) {
		t.Errorf("書き込まれた Size = %d, want %d", info.Size, len(content))
	}
	if got := read(t, ctx, s, p); got != content {
		t.Errorf("内容が %d バイト、期待は %d バイト（切り詰められている可能性）", len(got), len(content))
	}
}

func testEmptyFile(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	p := path.Join(root, "empty.txt")
	info := put(t, ctx, s, p, "")

	if info.Size != 0 {
		t.Errorf("Size = %d, want 0", info.Size)
	}
	if got := read(t, ctx, s, p); got != "" {
		t.Errorf("内容 = %q, want 空", got)
	}

	// 0 が「サイズ不明」と混同されていないこと
	stat, err := s.Stat(ctx, p)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Size != 0 {
		t.Errorf("Stat の Size = %d, want 0", stat.Size)
	}
}

func testOverwrite(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	p := path.Join(root, "overwrite.txt")
	put(t, ctx, s, p, "最初の内容はこちらです")
	put(t, ctx, s, p, "短い")

	if got := read(t, ctx, s, p); got != "短い" {
		t.Errorf("内容 = %q, want 短い（前の内容が残っている可能性）", got)
	}

	stat, err := s.Stat(ctx, p)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if stat.Size != int64(len("短い")) {
		t.Errorf("Size = %d, want %d", stat.Size, len("短い"))
	}
}

func testNotFound(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	missing := path.Join(root, "ないファイル.txt")

	if _, err := s.Stat(ctx, missing); !storage.IsNotFound(err) {
		t.Errorf("Stat = %v, ErrNotFound であるべき", err)
	}
	if _, _, err := s.Open(ctx, missing); !storage.IsNotFound(err) {
		t.Errorf("Open = %v, ErrNotFound であるべき", err)
	}
	if err := s.List(ctx, path.Join(root, "ないディレクトリ"), func(storage.FileInfo) error { return nil }); !storage.IsNotFound(err) {
		t.Errorf("List = %v, ErrNotFound であるべき", err)
	}

	// 存在しないものへの操作は、待っても直らない失敗として分類されること
	_, err := s.Stat(ctx, missing)
	if class := storage.ClassOf(err); class != storage.ClassPermanent {
		t.Errorf("失敗の種類 = %v, want permanent（再試行しても無駄なので）", class)
	}
}

func testDirectories(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	dir := path.Join(root, "ディレクトリ")
	if err := s.Mkdir(ctx, dir); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	// 冪等であること
	if err := s.Mkdir(ctx, dir); err != nil {
		t.Errorf("2回目の Mkdir でエラー: %v", err)
	}

	if !s.Features().EmptyDirs {
		t.Skip("空のディレクトリを表現できないストレージのため、以降を飛ばします")
	}

	stat, err := s.Stat(ctx, dir)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !stat.IsDir {
		t.Error("ディレクトリなのに IsDir が偽")
	}

	if names := listNames(t, ctx, s, dir); len(names) != 0 {
		t.Errorf("空のはずのディレクトリに %v がある", names)
	}
	if names := listNames(t, ctx, s, root); len(names) != 1 || names[0] != "ディレクトリ" {
		t.Errorf("親の List = %v", names)
	}

	if err := s.Remove(ctx, dir); err != nil {
		t.Errorf("空ディレクトリの Remove: %v", err)
	}
}

func testNestedDirectories(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	deep := path.Join(root, "a", "b", "c")
	if err := s.Mkdir(ctx, deep); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}

	p := path.Join(deep, "file.txt")
	put(t, ctx, s, p, "深い場所のファイル")

	if got := read(t, ctx, s, p); got != "深い場所のファイル" {
		t.Errorf("内容 = %q", got)
	}

	// 各階層が見えること
	for _, dir := range []string{path.Join(root, "a"), path.Join(root, "a", "b")} {
		names := listNames(t, ctx, s, dir)
		if len(names) != 1 {
			t.Errorf("%s の List = %v, 1件であるべき", dir, names)
		}
	}
}

func testModTime(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	if !s.Features().CanSetModTime {
		t.Skip("更新時刻を指定できないストレージのため飛ばします")
	}

	want := time.Date(2021, 6, 15, 12, 34, 56, 0, time.UTC)
	p := path.Join(root, "modtime.txt")

	if _, err := s.Put(ctx, p, strings.NewReader("x"), storage.ObjectMeta{
		Size:    1,
		ModTime: want,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	stat, err := s.Stat(ctx, p)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}

	// 分解能のぶんはずれてもよい。最低でも1秒は許容する。
	tolerance := max(s.Features().ModTimePrecision, time.Second)
	if diff := stat.ModTime.Sub(want); diff > tolerance || diff < -tolerance {
		t.Errorf("更新時刻 = %v, want %v（許容 %v）", stat.ModTime.UTC(), want, tolerance)
	}
}

func testUnusualNames(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	names := []string{
		"日本語のファイル名.txt",
		"space in name.txt",
		"記号 #+&=@.txt",
		"括弧[と]波括弧{と}.txt",
		"アスタリスクは避ける.txt",
		"絵文字🎉入り.txt",
		strings.Repeat("長", 60) + ".txt",
		"ドットで.始まる.複数.txt",
	}

	// OS のパス規則に従うストレージでは、区切りに使われるので試さない。
	if !s.Features().OSPath {
		// クラウドストレージではふつうの文字。区切りに読み替えて
		// しまうと、別の場所のファイルとして扱われる。
		names = append(names, `逆斜線\を含む.txt`)
	}

	illegal := s.Features().IllegalChars
	for _, name := range names {
		if illegal != "" && strings.ContainsAny(name, illegal) {
			continue
		}

		t.Run(name, func(t *testing.T) {
			p := path.Join(root, name)
			content := "内容: " + name
			put(t, ctx, s, p, content)

			if got := read(t, ctx, s, p); got != content {
				t.Errorf("内容 = %q, want %q", got, content)
			}

			stat, err := s.Stat(ctx, p)
			if err != nil {
				t.Fatalf("Stat: %v", err)
			}
			if stat.Name != name {
				t.Errorf("Name = %q, want %q", stat.Name, name)
			}
		})
	}
}

func testListEarlyStop(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	for i := range 5 {
		put(t, ctx, s, path.Join(root, fmt.Sprintf("f%d.txt", i)), "x")
	}

	sentinel := errors.New("ここで打ち切る")
	seen := 0
	err := s.List(ctx, root, func(storage.FileInfo) error {
		seen++
		if seen == 2 {
			return sentinel
		}
		return nil
	})

	if !errors.Is(err, sentinel) {
		t.Errorf("List = %v, コールバックが返したエラーをそのまま返すべき", err)
	}
	if seen != 2 {
		t.Errorf("コールバックが %d 回呼ばれた, want 2（打ち切られていない）", seen)
	}
}

// 件数の多いディレクトリで、すべての項目が返ることを確認します。
//
// Google Drive の実装には、ページ分割の続きをたどっておらず
// 1000件を超えるぶんが黙って欠落する不具合がありました。
func testManyEntries(t *testing.T, h Harness) {
	if h.SkipLargeDirs {
		t.Skip("件数の多いディレクトリの試験は飛ばす設定です")
	}

	ctx, s, root := setup(t, h)

	count := h.LargeDirCount
	if count == 0 {
		count = 1100 // ページ分割の境界（1000件）を超える数
	}

	dir := path.Join(root, "many")
	if err := s.Mkdir(ctx, dir); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	for i := range count {
		put(t, ctx, s, path.Join(dir, fmt.Sprintf("file-%05d.txt", i)), "x")
	}

	entries, err := storage.ListAll(ctx, s, dir)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != count {
		t.Errorf("List が %d 件、期待は %d 件（ページ分割の続きを辿っていない可能性）", len(entries), count)
	}
}

func testCancellation(t *testing.T, h Harness) {
	_, s, root := setup(t, h)

	p := path.Join(root, "cancel.txt")
	put(t, context.Background(), s, p, "内容")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	checks := map[string]func() error{
		"List": func() error { return s.List(ctx, root, func(storage.FileInfo) error { return nil }) },
		"Stat": func() error { _, err := s.Stat(ctx, p); return err },
		"Open": func() error { _, _, err := s.Open(ctx, p); return err },
		"Put": func() error {
			_, err := s.Put(ctx, path.Join(root, "x.txt"), strings.NewReader("x"), storage.ObjectMeta{Size: 1})
			return err
		},
		"Mkdir":  func() error { return s.Mkdir(ctx, path.Join(root, "d")) },
		"Remove": func() error { return s.Remove(ctx, p) },
	}

	for name, fn := range checks {
		err := fn()
		if err == nil {
			t.Errorf("%s: 取り消し済みの ctx なのに成功した", name)
			continue
		}
		if class := storage.ClassOf(err); class != storage.ClassCanceled {
			t.Errorf("%s: 失敗の種類 = %v, want canceled（err=%v）", name, class, err)
		}
	}
}

// 書き込みの途中で取り消された場合に、中身の欠けたファイルが
// 正規の名前で残らないことを確認します。
func testCancelDuringPut(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	if !s.Features().AtomicPut {
		t.Skip("不可分な書き込みができないストレージのため飛ばします")
	}

	p := path.Join(root, "interrupted.txt")
	putCtx, cancel := context.WithCancel(ctx)

	// 途中まで読ませてから取り消す
	r := &cancelingReader{
		data:   bytes.Repeat([]byte("x"), 1<<20),
		cancel: cancel,
		after:  4096,
	}

	_, err := s.Put(putCtx, p, r, storage.ObjectMeta{Size: 1 << 20})
	if err == nil {
		t.Fatal("取り消したのに Put が成功した")
	}

	if _, err := s.Stat(ctx, p); !storage.IsNotFound(err) {
		t.Errorf("中断した書き込み先にファイルが残っている: %v", err)
	}
}

// cancelingReader は、一定量を読ませたところで ctx を取り消します。
type cancelingReader struct {
	data   []byte
	off    int
	cancel context.CancelFunc
	after  int
	done   bool
}

func (c *cancelingReader) Read(p []byte) (int, error) {
	if c.off >= len(c.data) {
		return 0, io.EOF
	}
	n := copy(p, c.data[c.off:])
	c.off += n

	if !c.done && c.off >= c.after {
		c.done = true
		c.cancel()
	}
	return n, nil
}

func testConcurrentPut(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	const n = 8
	errs := make(chan error, n)
	for i := range n {
		go func(i int) {
			_, err := s.Put(ctx,
				path.Join(root, fmt.Sprintf("concurrent-%d.txt", i)),
				strings.NewReader(fmt.Sprintf("内容 %d", i)),
				storage.ObjectMeta{Size: storage.SizeUnknown})
			errs <- err
		}(i)
	}
	for range n {
		if err := <-errs; err != nil {
			t.Errorf("並行 Put: %v", err)
		}
	}

	if names := listNames(t, ctx, s, root); len(names) != n {
		t.Errorf("List が %d 件, want %d", len(names), n)
	}
}

// Features の申告と、実際に実装されているインターフェースが
// 食い違っていないことを確認します。
func testFeaturesConsistency(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	f := s.Features()
	if f == nil {
		t.Fatal("Features が nil")
	}

	if f.ModTimePrecision <= 0 {
		t.Error("ModTimePrecision が設定されていない")
	}
	if f.CanSetModTime && f.ModTimePrecision > time.Hour {
		t.Error("更新時刻を設定できるのに分解能が粗すぎる")
	}

	// ハッシュを申告しているなら、実際に取得できること
	if len(f.Hashes) > 0 {
		if _, ok := s.(storage.Hasher); !ok {
			// 一覧のついでに返す形でもよいので、その場合は実ファイルで確認する
			p := path.Join(root, "hashcheck.txt")
			put(t, ctx, s, p, "内容")
			info, err := s.Stat(ctx, p)
			if err != nil {
				t.Fatalf("Stat: %v", err)
			}
			found := false
			for _, ht := range f.Hashes {
				if _, ok := info.Hashes[ht]; ok {
					found = true
					break
				}
			}
			if !found {
				t.Error("ハッシュを申告しているが、Hasher も実装せずメタデータにも含まれていない")
			}
		}
	}
}

func testHash(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	hasher, ok := s.(storage.Hasher)
	if !ok {
		t.Skip("Hasher を実装していないため飛ばします")
	}

	p := path.Join(root, "hash.txt")
	put(t, ctx, s, p, "hello world")

	for _, ht := range s.Features().Hashes {
		want, err := knownHash(ht, "hello world")
		if err != nil {
			continue // 参照値を用意していない種類は飛ばす
		}

		got, err := hasher.Hash(ctx, p, ht)
		if err != nil {
			t.Errorf("Hash(%s): %v", ht, err)
			continue
		}
		if !strings.EqualFold(got, want) {
			t.Errorf("Hash(%s) = %s, want %s", ht, got, want)
		}
	}
}

func testRangeOpen(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	opener, ok := s.(storage.RangeOpener)
	if !ok {
		t.Skip("RangeOpener を実装していないため飛ばします")
	}

	p := path.Join(root, "range.txt")
	put(t, ctx, s, p, "0123456789")

	tests := []struct {
		offset, length int64
		want           string
	}{
		{0, 3, "012"},
		{5, 3, "567"},
		{7, -1, "789"},
		{0, -1, "0123456789"},
	}

	for _, tt := range tests {
		rc, err := opener.OpenRange(ctx, p, tt.offset, tt.length)
		if err != nil {
			t.Errorf("OpenRange(%d, %d): %v", tt.offset, tt.length, err)
			continue
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Errorf("読み取り: %v", err)
			continue
		}
		if string(data) != tt.want {
			t.Errorf("OpenRange(%d, %d) = %q, want %q", tt.offset, tt.length, data, tt.want)
		}
	}
}

func testMove(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	src := path.Join(root, "src.txt")
	dst := path.Join(root, "dst.txt")
	put(t, ctx, s, src, "移動する内容")

	// Mover がなくてもヘルパがコピーと削除で肩代わりする
	if err := storage.Move(ctx, s, src, dst); err != nil {
		t.Fatalf("Move: %v", err)
	}

	if got := read(t, ctx, s, dst); got != "移動する内容" {
		t.Errorf("移動先の内容 = %q", got)
	}
	if _, err := s.Stat(ctx, src); !storage.IsNotFound(err) {
		t.Errorf("移動元が残っている: %v", err)
	}
}

func testPurge(t *testing.T, h Harness) {
	ctx, s, root := setup(t, h)

	dir := path.Join(root, "purge")
	put(t, ctx, s, path.Join(dir, "a.txt"), "a")
	put(t, ctx, s, path.Join(dir, "sub", "b.txt"), "b")

	// Purger がなくてもヘルパが再帰的に消す
	if err := storage.PurgeAll(ctx, s, dir); err != nil {
		t.Fatalf("PurgeAll: %v", err)
	}
	if _, err := s.Stat(ctx, dir); !storage.IsNotFound(err) {
		t.Errorf("削除したはずのディレクトリが残っている: %v", err)
	}
}

func testLargerFile(t *testing.T, h Harness) {
	if testing.Short() {
		t.Skip("-short のため飛ばします")
	}

	ctx, s, root := setup(t, h)

	// 内容が完全に一致することを確かめる。
	// 途中で切り詰められたり、詰め物が入ったりしないこと。
	size := 3 * 1024 * 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251)
	}

	p := path.Join(root, "large.bin")
	info, err := s.Put(ctx, p, bytes.NewReader(data), storage.ObjectMeta{Size: int64(size)})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if info.Size != int64(size) {
		t.Errorf("Size = %d, want %d", info.Size, size)
	}

	rc, _, err := s.Open(ctx, p)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("読み取り: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("内容が一致しない（%d バイト読めた、期待は %d バイト）", len(got), size)
	}
}
