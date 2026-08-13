package transfer_test

import (
	"context"
	"errors"
	"testing"

	"github.com/mt3hr/hbg/backend/memory"
	"github.com/mt3hr/hbg/transfer"
)

// 同期での削除は取り返しがつかないので、判断の規則をここで固定します。

// コピー元にないものが消えることを確かめます。
func TestSyncDeletesExtraneous(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/のこす.txt", "1")
	put(t, dst, "/backup/data/のこす.txt", "1")
	put(t, dst, "/backup/data/よぶん.txt", "2")
	put(t, dst, "/backup/data/よぶんな階層/中身.txt", "3")

	opts := baseOptions(src, dst)
	opts.Delete = true

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	snap := dst.Snapshot()
	if _, ok := snap["/backup/data/のこす.txt"]; !ok {
		t.Error("コピー元にあるものが消えている")
	}
	for _, p := range []string{"/backup/data/よぶん.txt", "/backup/data/よぶんな階層/中身.txt"} {
		if _, ok := snap[p]; ok {
			t.Errorf("コピー元にない %s が残っている", p)
		}
	}
	if result.Deleted < 2 {
		t.Errorf("削除した件数 = %d, want 2以上", result.Deleted)
	}
}

// --delete を付けなければ何も消えないことを確かめます。
func TestSyncWithoutDeleteKeepsEverything(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "1")
	put(t, dst, "/backup/data/よぶん.txt", "2")

	opts := baseOptions(src, dst)

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Deleted != 0 {
		t.Errorf("削除した件数 = %d, want 0", result.Deleted)
	}
	if _, ok := dst.Snapshot()["/backup/data/よぶん.txt"]; !ok {
		t.Error("--delete を付けていないのに消えている")
	}
}

// 転送に失敗があったら削除しないことを確かめます。
//
// コピー元を読めなかっただけで「向こうには無い」と判断すると、
// 取っておきたいものを消すことになります。
func TestSyncSkipsDeleteWhenTransferFailed(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/こわれる.txt", "1")
	put(t, src, "/data/ふつう.txt", "2")
	put(t, dst, "/backup/data/よぶん.txt", "3")

	// 1つだけ読めなくする。
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

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Failed == 0 {
		t.Fatal("失敗するはずの転送が成功した")
	}
	if result.Deleted != 0 {
		t.Errorf("削除した件数 = %d, want 0（失敗があったら消さない）", result.Deleted)
	}
	if _, ok := dst.Snapshot()["/backup/data/よぶん.txt"]; !ok {
		t.Error("転送に失敗したのに削除が行われている")
	}
}

// 絞り込みで対象外にしたものは消さないことを確かめます。
//
// 転送していないものを消すのは筋が通りません。
func TestSyncDoesNotDeleteExcluded(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.jpg", "1")
	put(t, dst, "/backup/data/a.jpg", "1")
	put(t, dst, "/backup/data/大事.txt", "2")

	opts := baseOptions(src, dst)
	opts.Delete = true
	opts.Filter = mustFilter(t, transfer.FilterSpec{Include: []string{"*.jpg"}})

	if _, err := transfer.Run(context.Background(), opts); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if _, ok := dst.Snapshot()["/backup/data/大事.txt"]; !ok {
		t.Error("絞り込みで対象外にしたものが消えている")
	}
}

// --dry-run では何も消えないことを確かめます。
func TestSyncDryRunDeletesNothing(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "1")
	put(t, dst, "/backup/data/よぶん.txt", "2")

	opts := baseOptions(src, dst)
	opts.Delete = true
	opts.DryRun = true

	events := []transfer.DecisionEvent{}
	opts.OnDecision = func(ev transfer.DecisionEvent) { events = append(events, ev) }

	result, err := transfer.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, ok := dst.Snapshot()["/backup/data/よぶん.txt"]; !ok {
		t.Error("--dry-run なのに消えている")
	}
	if result.Deleted == 0 {
		t.Error("何が消えるかが数えられていない")
	}

	// 何が消えるかを知らせること。
	found := false
	for _, ev := range events {
		if ev.Action == transfer.ActionDelete && ev.Path == "よぶん.txt" {
			found = true
		}
	}
	if !found {
		t.Errorf("削除の予定が知らされていない: %v", events)
	}
}

// 深い場所から順に消すことを確かめます。
//
// ディレクトリは中身が無くならないと消せません。
func TestSyncDeletesDeepestFirst(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/のこす.txt", "1")
	put(t, dst, "/backup/data/のこす.txt", "1")
	put(t, dst, "/backup/data/深い/もっと深い/中身.txt", "2")

	opts := baseOptions(src, dst)
	opts.Delete = true

	if _, err := transfer.Run(context.Background(), opts); err != nil {
		t.Fatalf("Run: %v", err)
	}

	snap := dst.Snapshot()
	for p := range snap {
		if p != "/backup/data/のこす.txt" && p != "/backup" && p != "/backup/data" {
			t.Errorf("消えていないものがある: %s", p)
		}
	}
}

// 書き込み中のものは削除の対象にしないことを確かめます。
//
// 別のところで走っている転送を邪魔しないためです。
func TestSyncIgnoresPartFiles(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "1")
	put(t, dst, "/backup/data/.b.txt.hbgpart", "書き込み中")

	opts := baseOptions(src, dst)
	opts.Delete = true

	if _, err := transfer.Run(context.Background(), opts); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, ok := dst.Snapshot()["/backup/data/.b.txt.hbgpart"]; !ok {
		t.Error("書き込み中のものを消している")
	}
}

// やり直しをまたいでも、削除の件数が数え落とされないことを確かめます。
//
// 何回かに分けて実行したとき、集計の側で拾い忘れると
// 「消したのに0件と報告される」ことになります。
func TestSyncDeleteCountSurvivesPasses(t *testing.T) {
	src, dst := newPair(t)
	put(t, src, "/data/a.txt", "1")
	put(t, dst, "/backup/data/よぶん.txt", "2")

	opts := baseOptions(src, dst)
	opts.Delete = true

	result, err := transfer.RunWithPasses(context.Background(), opts,
		transfer.PassPolicy{MaxPasses: 2}, nil)
	if err != nil {
		t.Fatalf("RunWithPasses: %v", err)
	}
	if result.Deleted == 0 {
		t.Error("削除した件数が数えられていない")
	}
}
