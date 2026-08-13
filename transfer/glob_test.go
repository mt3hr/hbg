package transfer_test

import (
	"context"
	"strings"
	"testing"

	"github.com/mt3hr/hbg/transfer"
)

// コピー元のワイルドカードは、一致したものを転送先の直下へ運びます。
//
// dropbox:/photos/* のような書き方は、ディレクトリを指定したときと違って
// 階層をひとつ増やしません。スクリプトはこの違いに依存しています。
func TestRunCopiesGlobMatchesFlat(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")
	put(t, src, "/data/b.txt", "bbb")
	put(t, src, "/data/sub/c.txt", "ccc")

	opts := baseOptions(src, dst)
	opts.SrcPath = "/data/*"

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Transferred != 3 || result.Failed != 0 {
		t.Errorf("Transferred=%d Failed=%d, want 3 と 0", result.Transferred, result.Failed)
	}

	// 中身が backup 直下に来ること。backup/data/... ではない。
	for _, want := range []string{"/backup/a.txt", "/backup/b.txt", "/backup/sub/c.txt"} {
		if _, err := dst.Stat(context.Background(), want); err != nil {
			t.Errorf("%s が無い: %v", want, err)
		}
	}
	if _, err := dst.Stat(context.Background(), "/backup/data/a.txt"); err == nil {
		t.Error("/backup/data/a.txt がある。1階層深くなっている")
	}
}

// 拡張子での絞り込みも効きます。
func TestRunCopiesGlobWithExtension(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/user_config.db", "db")
	put(t, src, "/data/other.db", "db2")
	put(t, src, "/data/readme.txt", "txt")

	opts := baseOptions(src, dst)
	opts.SrcPath = "/data/*.db"

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Transferred != 2 {
		t.Errorf("Transferred=%d, want 2", result.Transferred)
	}
	if _, err := dst.Stat(context.Background(), "/backup/user_config.db"); err != nil {
		t.Errorf("user_config.db が無い: %v", err)
	}
	if _, err := dst.Stat(context.Background(), "/backup/readme.txt"); err == nil {
		t.Error("パターンに合わない readme.txt が運ばれている")
	}
}

// 一致するものが無ければ失敗させます。
//
// 0件を成功にすると、パターンの打ち間違いがスクリプトからは
// 成功に見えてしまいます。
func TestRunFailsWhenGlobMatchesNothing(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")

	opts := baseOptions(src, dst)
	opts.SrcPath = "/data/*.db"

	_, err := transfer.Run(context.Background(), opts)
	if err == nil {
		t.Fatal("一致しないのに成功した")
	}
	if !strings.Contains(err.Error(), "一致するものがありません") {
		t.Errorf("err = %v", err)
	}
}

// 削除を伴う転送ではワイルドカードを断ります。
func TestRunRejectsGlobWithDelete(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")
	put(t, dst, "/backup/old.txt", "old")

	opts := baseOptions(src, dst)
	opts.SrcPath = "/data/*"
	opts.Delete = true

	_, err := transfer.Run(context.Background(), opts)
	if err == nil {
		t.Fatal("断られなかった")
	}
	if !strings.Contains(err.Error(), "削除できません") {
		t.Errorf("err = %v", err)
	}
	// 断ったのだから、消えていないこと。
	if _, err := dst.Stat(context.Background(), "/backup/old.txt"); err != nil {
		t.Errorf("断ったのにコピー先が消えている: %v", err)
	}
}

// ワイルドカードを含まないパスは、これまでどおり1階層増えます。
func TestRunWithoutGlobKeepsDirectoryLevel(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "aaa")

	if _, err := transfer.Run(context.Background(), baseOptions(src, dst)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := dst.Stat(context.Background(), "/backup/data/a.txt"); err != nil {
		t.Errorf("/backup/data/a.txt が無い: %v", err)
	}
}
