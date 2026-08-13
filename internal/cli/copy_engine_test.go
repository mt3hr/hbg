package cli

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mt3hr/hbg"
)

// newTestLocalStorage はテスト用の一時ディレクトリと local ストレージを返します。
func newTestLocalStorage(t *testing.T) (hbg.Storage, string) {
	t.Helper()
	dir := filepath.ToSlash(t.TempDir())
	return hbg.NewLocalFileSystem("local"), dir
}

// writeFile はテスト用にファイルを作ります。
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// readFile はファイルの中身を読みます。存在しなければテストを失敗させます。
func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func TestCopyTree(t *testing.T) {
	t.Parallel()

	t.Run("入れ子のディレクトリごとコピーできる", func(t *testing.T) {
		t.Parallel()
		storage, root := newTestLocalStorage(t)

		writeFile(t, root+"/src/a.txt", "a")
		writeFile(t, root+"/src/sub/b.txt", "b")
		writeFile(t, root+"/src/sub/deep/c.txt", "c")

		result, err := copyTree(storage, storage, root+"/src", root+"/dst", time.Second, nil, 2)
		if err != nil {
			t.Fatalf("copyTree: %v", err)
		}
		if result.Transferred != 3 || result.Failed != 0 {
			t.Errorf("Transferred=%d Failed=%d, want 3 と 0", result.Transferred, result.Failed)
		}

		for path, want := range map[string]string{
			root + "/dst/src/a.txt":          "a",
			root + "/dst/src/sub/b.txt":      "b",
			root + "/dst/src/sub/deep/c.txt": "c",
		} {
			if got := readFile(t, path); got != want {
				t.Errorf("%s の内容が %q、期待は %q", path, got, want)
			}
		}
	})

	// もとはコピー先のパスを「子ファイルの親ディレクトリ」から導出していたため、
	// 子を持たないディレクトリでは空文字になり、コピー先に作られないばかりか
	// 続く List("") が失敗してコピー全体がエラーになっていた。
	t.Run("空のディレクトリもコピー先に作られる", func(t *testing.T) {
		t.Parallel()
		storage, root := newTestLocalStorage(t)

		if err := os.MkdirAll(root+"/src/emptydir", 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		writeFile(t, root+"/src/a.txt", "a")

		result, err := copyTree(storage, storage, root+"/src", root+"/dst", time.Second, nil, 1)
		if err != nil {
			t.Fatalf("copyTree: %v", err)
		}
		if result.Failed != 0 {
			t.Errorf("Failed=%d, want 0", result.Failed)
		}

		info, err := os.Stat(root + "/dst/src/emptydir")
		if err != nil {
			t.Fatalf("空ディレクトリがコピー先に作られていない: %v", err)
		}
		if !info.IsDir() {
			t.Error("ディレクトリとして作られていない")
		}
	})

	t.Run("空のディレクトリそのものをコピーしてもエラーにならない", func(t *testing.T) {
		t.Parallel()
		storage, root := newTestLocalStorage(t)

		if err := os.MkdirAll(root+"/empty", 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}

		result, err := copyTree(storage, storage, root+"/empty", root+"/dst", time.Second, nil, 1)
		if err != nil {
			t.Fatalf("copyTree: %v", err)
		}
		if result.Transferred != 0 || result.Failed != 0 {
			t.Errorf("Transferred=%d Failed=%d, want 0 と 0", result.Transferred, result.Failed)
		}
	})

	// もとは一致するものがなくても「0件成功」で正常終了しており、
	// コピー元のパスを打ち間違えてもスクリプトからは成功に見えていた。
	t.Run("コピー元が存在しなければエラーになる", func(t *testing.T) {
		t.Parallel()
		storage, root := newTestLocalStorage(t)

		_, err := copyTree(storage, storage, root+"/nonexistent", root+"/dst", time.Second, nil, 1)
		if err == nil {
			t.Fatal("コピー元が存在しないのにエラーにならなかった")
		}
		if !strings.Contains(err.Error(), "コピー元が見つかりません") {
			t.Errorf("エラーメッセージが期待と違う: %v", err)
		}
	})

	// もとは worker が 0 だと容量0のチャネルとワーカー0個になり、
	// 最初の送信で永久にブロックしていた。
	t.Run("workerが0でもデッドロックしない", func(t *testing.T) {
		t.Parallel()
		storage, root := newTestLocalStorage(t)
		writeFile(t, root+"/src/a.txt", "a")

		done := make(chan struct{})
		go func() {
			defer close(done)
			if _, err := copyTree(storage, storage, root+"/src", root+"/dst", time.Second, nil, 0); err != nil {
				t.Errorf("copyTree: %v", err)
			}
		}()

		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("worker=0 でデッドロックした")
		}
	})

	t.Run("2回目は同一ファイルがスキップされる", func(t *testing.T) {
		t.Parallel()
		storage, root := newTestLocalStorage(t)
		writeFile(t, root+"/src/a.txt", "a")

		first, err := copyTree(storage, storage, root+"/src", root+"/dst", time.Second, nil, 1)
		if err != nil {
			t.Fatalf("1回目: %v", err)
		}
		if first.Transferred != 1 {
			t.Fatalf("1回目 Transferred=%d, want 1", first.Transferred)
		}

		second, err := copyTree(storage, storage, root+"/src", root+"/dst", time.Second, nil, 1)
		if err != nil {
			t.Fatalf("2回目: %v", err)
		}
		if second.Transferred != 0 {
			t.Errorf("2回目 Transferred=%d, want 0（スキップされるはず）", second.Transferred)
		}
	})

	t.Run("無視リストのファイルはコピーされない", func(t *testing.T) {
		t.Parallel()
		storage, root := newTestLocalStorage(t)
		writeFile(t, root+"/src/a.txt", "a")
		writeFile(t, root+"/src/Thumbs.db", "x")

		result, err := copyTree(storage, storage, root+"/src", root+"/dst", time.Second, defaultIgnores, 1)
		if err != nil {
			t.Fatalf("copyTree: %v", err)
		}
		if result.Transferred != 1 {
			t.Errorf("Transferred=%d, want 1", result.Transferred)
		}
		if _, err := os.Stat(root + "/dst/src/Thumbs.db"); !os.IsNotExist(err) {
			t.Error("無視するはずのファイルがコピーされている")
		}
	})

	t.Run("最終更新時刻が保持される", func(t *testing.T) {
		t.Parallel()
		storage, root := newTestLocalStorage(t)
		writeFile(t, root+"/src/a.txt", "a")

		want := time.Date(2020, 3, 4, 5, 6, 7, 0, time.Local)
		if err := os.Chtimes(root+"/src/a.txt", want, want); err != nil {
			t.Fatalf("chtimes: %v", err)
		}

		if _, err := copyTree(storage, storage, root+"/src", root+"/dst", time.Second, nil, 1); err != nil {
			t.Fatalf("copyTree: %v", err)
		}

		info, err := os.Stat(root + "/dst/src/a.txt")
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if diff := info.ModTime().Sub(want); diff > time.Second || diff < -time.Second {
			t.Errorf("最終更新時刻が %v、期待は %v", info.ModTime(), want)
		}
	})
}

// copyResult のサマリ表示を検証します。
func TestCopyResultWriteSummary(t *testing.T) {
	t.Parallel()

	t.Run("失敗がなければ失敗一覧を出さない", func(t *testing.T) {
		t.Parallel()
		var sb strings.Builder
		(&copyResult{Transferred: 3}).writeSummary(&sb)

		out := sb.String()
		if !strings.Contains(out, "3件成功") || !strings.Contains(out, "0件失敗") {
			t.Errorf("サマリが期待と違う: %q", out)
		}
		if strings.Contains(out, "失敗した内容") {
			t.Errorf("失敗がないのに一覧が出ている: %q", out)
		}
	})

	t.Run("失敗が上限を超えたら件数を添える", func(t *testing.T) {
		t.Parallel()
		r := &copyResult{Transferred: 1, Failed: 25}
		for i := 0; i < maxReportedErrors; i++ {
			r.Errors = append(r.Errors, io.ErrUnexpectedEOF)
		}

		var sb strings.Builder
		r.writeSummary(&sb)

		out := sb.String()
		if !strings.Contains(out, "ほか 5件") {
			t.Errorf("打ち切られた件数が表示されていない: %q", out)
		}
	})
}
