package transfer_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mt3hr/hbg/backend/memory"
	"github.com/mt3hr/hbg/transfer"
)

// ミラーとして使うときに効いてくる、失敗の抱え方をここで固定します。
// robocopy /MIR から乗り換えられるかどうかがこの3つで決まります。

// 置き去りにされた書き込み中ファイルが片付くことを確かめます。
//
// 強制終了や電源断で残ったものは、以前は一律に対象外だったため
// 一度残ると永久に残っていました。
func TestSyncDeletesStalePartFiles(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "1")
	put(t, dst, "/backup/data/a.txt", "1")
	put(t, dst, "/backup/data/.b.txt.hbgpart", "書きかけ")

	// 今回の実行より前に置き去りにされたことにする。
	stale := time.Now().Add(-time.Hour)
	if err := dst.SetModTime(context.Background(), "/backup/data/.b.txt.hbgpart", stale); err != nil {
		t.Fatalf("SetModTime: %v", err)
	}

	opts := baseOptions(src, dst)
	opts.Delete = true

	if _, err := transfer.Run(context.Background(), opts); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, ok := dst.Snapshot()["/backup/data/.b.txt.hbgpart"]; ok {
		t.Error("置き去りにされた書き込み中ファイルが残っている")
	}
}

// 置き去りの残骸は、絞り込みで対象外でも片付くことを確かめます。
//
// hbg 自身が作ったものであって利用者のデータではないので、
// 「転送していないものは消さない」という決まりは当てはまりません。
func TestSyncDeletesStalePartFilesEvenWhenFiltered(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.jpg", "1")
	put(t, dst, "/backup/data/a.jpg", "1")
	put(t, dst, "/backup/data/.b.jpg.hbgpart", "書きかけ")

	stale := time.Now().Add(-time.Hour)
	if err := dst.SetModTime(context.Background(), "/backup/data/.b.jpg.hbgpart", stale); err != nil {
		t.Fatalf("SetModTime: %v", err)
	}

	opts := baseOptions(src, dst)
	opts.Delete = true
	opts.Filter = mustFilter(t, transfer.FilterSpec{Include: []string{"*.jpg"}})

	if _, err := transfer.Run(context.Background(), opts); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, ok := dst.Snapshot()["/backup/data/.b.jpg.hbgpart"]; ok {
		t.Error("絞り込みを理由に残骸が片付けられていない")
	}
}

// 一覧できないディレクトリがあっても、走査全体が止まらないことを確かめます。
//
// 以前はここで error を返しており、読めないディレクトリが1つあるだけで
// 他のファイルの転送まで巻き添えで止まっていました。
func TestScanContinuesWhenDirectoryListFails(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/よめる/a.txt", "1")
	put(t, src, "/data/よめない/b.txt", "2")

	src.SetHooks(memory.Hooks{
		BeforeOp: func(op, path string) error {
			if op == "list" && path == "/data/よめない" {
				return errors.New("読めません")
			}
			return nil
		},
	})

	opts := baseOptions(src, dst)

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v（1つ読めないだけで全体を止めてはいけない）", err)
	}
	if result.Failed != 1 {
		t.Errorf("失敗した件数 = %d, want 1", result.Failed)
	}
	if _, ok := dst.Snapshot()["/backup/data/よめる/a.txt"]; !ok {
		t.Error("読めるディレクトリのファイルが転送されていない")
	}
}

// 一覧できなかったディレクトリの中身は、削除されないことを確かめます。
//
// 中身が分からないものを「コピー元にない」と判断してはいけません。
// --delete-on-partial を付けても、ここだけは守ります。
func TestUnlistableDirectoryContentsSurviveDelete(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/よめない/b.txt", "2")
	put(t, dst, "/backup/data/よめない/b.txt", "2")
	put(t, dst, "/backup/data/よめない/大事.txt", "3")

	src.SetHooks(memory.Hooks{
		BeforeOp: func(op, path string) error {
			if op == "list" && path == "/data/よめない" {
				return errors.New("読めません")
			}
			return nil
		},
	})

	opts := baseOptions(src, dst)
	opts.Delete = true
	opts.DeleteOnPartial = true

	if _, err := transfer.Run(context.Background(), opts); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, ok := dst.Snapshot()["/backup/data/よめない/大事.txt"]; !ok {
		t.Error("中身が分からないディレクトリから削除している")
	}
}

// --delete-on-partial を付けると、失敗があっても削除が進むことを確かめます。
//
// 件数が多いミラーでは「1件も失敗しない」という前提が成り立たず、
// 既定のままだと消したはずのものが転送先に残り続けます。
func TestSyncDeleteOnPartialProceedsAfterFailure(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/こわれる.txt", "1")
	put(t, dst, "/backup/data/よぶん.txt", "2")

	src.SetHooks(memory.Hooks{
		BeforeOp: func(op, path string) error {
			if op == "open" && path == "/data/こわれる.txt" {
				return errors.New("読めません")
			}
			return nil
		},
	})

	opts := baseOptions(src, dst)
	opts.Delete = true
	opts.DeleteOnPartial = true

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Failed == 0 {
		t.Fatal("失敗するはずの転送が成功した")
	}
	if _, ok := dst.Snapshot()["/backup/data/よぶん.txt"]; ok {
		t.Error("--delete-on-partial を付けたのに削除されていない")
	}
}
