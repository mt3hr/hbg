package dropbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/storage/storagetest"
)

// 適合性テストを偽サーバーに対して実行します。
//
// 認証情報がなくても、ページ分割・チャンク分割・変わった名前・
// 取り消し・並行書き込みまで確かめられます。
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			f := newFakeDropbox()
			s := f.start(t)

			root := "/試験"
			if err := s.Mkdir(context.Background(), root); err != nil {
				t.Fatalf("試験用のディレクトリを作れません: %v", err)
			}
			return s, root
		},
		// 偽サーバーは1ページ3件なので、1100件だと367回のやりとりになる。
		// ページ分割の確認としては十分なので少なめにする。
		LargeDirCount: 120,
	})
}

func newTestStorage(t *testing.T) (context.Context, *fakeDropbox, *Storage) {
	t.Helper()
	f := newFakeDropbox()
	return context.Background(), f, f.start(t)
}

func put(t *testing.T, ctx context.Context, s *Storage, p, content string) *storage.FileInfo {
	t.Helper()
	fi, err := s.Put(ctx, p, strings.NewReader(content), storage.ObjectMeta{
		Size: int64(len(content)),
	})
	if err != nil {
		t.Fatalf("Put(%s): %v", p, err)
	}
	return fi
}

// サイズが実際より小さく伝えられても、内容が切り詰められないことを確かめます。
//
// 以前の実装は宣言されたサイズをもとに io.LimitReader を掛けており、
// Google Drive のネイティブ形式のようにサイズが 0 と伝わるファイルは
// 中身が空のまま転送されていました。しかもエラーにならないため、
// バックアップが取れているつもりで中身が消えていることに気づけません。
func TestPutIgnoresDeclaredSize(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	content := strings.Repeat("あ", 1000)
	for _, declared := range []int64{0, storage.SizeUnknown, 3} {
		t.Run(fmt.Sprintf("宣言サイズ %d", declared), func(t *testing.T) {
			p := fmt.Sprintf("/宣言%d.txt", declared)

			written, err := s.Put(ctx, p, strings.NewReader(content), storage.ObjectMeta{
				Size: declared,
			})
			if err != nil {
				t.Fatalf("Put: %v", err)
			}
			if written.Size != int64(len(content)) {
				t.Errorf("書き込まれたサイズ = %d, want %d", written.Size, len(content))
			}

			rc, _, err := s.Open(ctx, p)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer rc.Close()

			got, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if string(got) != content {
				t.Errorf("内容の長さ = %d, want %d", len(got), len(content))
			}
		})
	}
}

// 1回で送る経路とセッションで送る経路の両方を通ることを確かめます。
func TestPutChunkBoundary(t *testing.T) {
	// 境界を小さくして、大きなファイルを作らずに両方の経路を通す。
	orig := smallUploadLimit
	smallUploadLimit = 16
	t.Cleanup(func() { smallUploadLimit = orig })

	sizes := []int{0, 1, 15, 16, 17, 33, 100}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("%dバイト", size), func(t *testing.T) {
			ctx, f, s := newTestStorage(t)

			content := strings.Repeat("x", size)
			put(t, ctx, s, "/chunk.bin", content)

			if got := readAll(t, ctx, s, "/chunk.bin"); got != content {
				t.Errorf("内容の長さ = %d, want %d", len(got), size)
			}

			// 境界を超えたものだけセッションを使うこと。
			sessions := f.callCount("upload_session/start")
			wantSession := size > 16
			if (sessions > 0) != wantSession {
				t.Errorf("セッションの利用 = %v, want %v（%dバイト）", sessions > 0, wantSession, size)
			}
		})
	}
}

func readAll(t *testing.T, ctx context.Context, s *Storage, p string) string {
	t.Helper()
	rc, _, err := s.Open(ctx, p)
	if err != nil {
		t.Fatalf("Open(%s): %v", p, err)
	}
	defer rc.Close()

	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll(%s): %v", p, err)
	}
	return string(b)
}

// 内容が途中で欠けた場合に、書き込みが成功したことにならないのを確かめます。
func TestPutRejectsCorruptedContent(t *testing.T) {
	ctx, f, s := newTestStorage(t)

	// content_hash の照合を必ず失敗させる。
	f.failNext("upload", 1, 409, `{"error_summary":"payload_too_large/.","error":{".tag":"payload_too_large"}}`)

	_, err := s.Put(ctx, "/壊れた.txt", strings.NewReader("なかみ"), storage.ObjectMeta{Size: 9})
	if err == nil {
		t.Fatal("内容が欠けているのに成功した")
	}
	if class := storage.ClassOf(err); class != storage.ClassPermanent {
		t.Errorf("失敗の種類 = %v, want permanent", class)
	}
}

// 件数の多いディレクトリで、続きの取得が漏れないことを確かめます。
//
// Google Drive 側では 1000件を超えると黙って欠落する不具合がありました。
// Dropbox も cursor をたどらないと同じことが起きます。
func TestListFollowsCursor(t *testing.T) {
	ctx, f, s := newTestStorage(t)

	const n = 50
	for i := range n {
		put(t, ctx, s, fmt.Sprintf("/多い/%03d.txt", i), "x")
	}

	names := map[string]bool{}
	if err := s.List(ctx, "/多い", func(fi storage.FileInfo) error {
		names[fi.Name] = true
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(names) != n {
		t.Errorf("列挙できた件数 = %d, want %d", len(names), n)
	}
	// 1ページ3件なので、続きの取得が実際に走っていること。
	if f.callCount("list_folder/continue") == 0 {
		t.Error("続きの取得が1度も行われていない")
	}
}

// 要求が多すぎるときに、待って再試行されることを確かめます。
func TestRetriesOnRateLimit(t *testing.T) {
	ctx, f, s := newTestStorage(t)

	f.failNext("get_metadata", 2, 429,
		`{"error_summary":"too_many_requests/.","error":{".tag":"too_many_requests","retry_after":0}}`)

	put(t, ctx, s, "/待つ.txt", "なかみ")

	if _, err := s.Stat(ctx, "/待つ.txt"); err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := f.callCount("get_metadata"); got < 3 {
		t.Errorf("get_metadata の呼び出し = %d, want 3以上（2回失敗して再試行）", got)
	}
}

// 429 が再試行しきれなかった場合の分類を確かめます。
func TestRateLimitClass(t *testing.T) {
	f := newFakeDropbox()
	s := f.startNoRetry(t)
	ctx := context.Background()

	f.failNext("get_metadata", 100, 429,
		`{"error_summary":"too_many_requests/.","error":{".tag":"too_many_requests","retry_after":7}}`)

	_, err := s.Stat(ctx, "/どこか.txt")
	if err == nil {
		t.Fatal("成功してしまった")
	}
	if class := storage.ClassOf(err); class != storage.ClassRateLimit {
		t.Errorf("失敗の種類 = %v, want ratelimit", class)
	}
	if got := storage.RetryAfterOf(err); got != 7*time.Second {
		t.Errorf("待ち時間 = %v, want 7s（サーバーの指示に従うこと）", got)
	}
}

// 認証の失敗は再試行せず、処理全体を止めることを確かめます。
func TestAuthErrorIsFatal(t *testing.T) {
	f := newFakeDropbox()
	s := f.startNoRetry(t)
	ctx := context.Background()

	f.failNext("get_metadata", 100, 401,
		`{"error_summary":"expired_access_token/.","error":{".tag":"expired_access_token"}}`)

	_, err := s.Stat(ctx, "/どこか.txt")
	if class := storage.ClassOf(err); class != storage.ClassAuth {
		t.Errorf("失敗の種類 = %v, want auth", class)
	}
	if storage.ClassOf(err).Retryable() {
		t.Error("認証の失敗が再試行の対象になっている")
	}
}

// Mkdir が冪等で、親も作られることを確かめます。
func TestMkdirCreatesParents(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	deep := "/親/子/孫"
	if err := s.Mkdir(ctx, deep); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	if err := s.Mkdir(ctx, deep); err != nil {
		t.Errorf("2回目の Mkdir: %v", err)
	}

	for _, dir := range []string{"/親", "/親/子", deep} {
		fi, err := s.Stat(ctx, dir)
		if err != nil {
			t.Fatalf("Stat(%s): %v", dir, err)
		}
		if !fi.IsDir {
			t.Errorf("%s がディレクトリになっていない", dir)
		}
	}
}

// 同じ名前のファイルがある場所にディレクトリを作ろうとしたら失敗すること。
func TestMkdirOverFile(t *testing.T) {
	ctx, _, s := newTestStorage(t)
	put(t, ctx, s, "/じゃま", "ファイルです")

	err := s.Mkdir(ctx, "/じゃま")
	if !errors.Is(err, storage.ErrExist) {
		t.Errorf("Mkdir = %v, want ErrExist", err)
	}
}

// Remove が中身ごと消してしまわないことを確かめます。
//
// Dropbox の delete は再帰的なので、そのまま呼ぶと
// 「1ファイルか空ディレクトリだけ消す」という約束を破ります。
func TestRemoveRefusesNonEmptyDir(t *testing.T) {
	ctx, _, s := newTestStorage(t)
	put(t, ctx, s, "/消さない/中身.txt", "だいじ")

	err := s.Remove(ctx, "/消さない")
	if !errors.Is(err, storage.ErrNotEmpty) {
		t.Fatalf("Remove = %v, want ErrNotEmpty", err)
	}

	if _, err := s.Stat(ctx, "/消さない/中身.txt"); err != nil {
		t.Errorf("中身が消えている: %v", err)
	}

	// Purge なら消えること。
	if err := s.Purge(ctx, "/消さない"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if _, err := s.Stat(ctx, "/消さない"); !storage.IsNotFound(err) {
		t.Errorf("Purge のあとも残っている: %v", err)
	}
}

// content hash が取得でき、ローカルの計算と一致することを確かめます。
func TestHash(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	content := strings.Repeat("hash", 3000)
	fi := put(t, ctx, s, "/ハッシュ.txt", content)

	want := contentHash([]byte(content))

	if got := fi.Hashes[storage.DropboxContent]; got != want {
		t.Errorf("書き込み結果のハッシュ = %s, want %s", got, want)
	}

	got, err := s.Hash(ctx, "/ハッシュ.txt", storage.DropboxContent)
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	if got != want {
		t.Errorf("Hash = %s, want %s", got, want)
	}

	// 扱えないハッシュは黙って別のもので代用しないこと。
	if _, err := s.Hash(ctx, "/ハッシュ.txt", storage.MD5); !errors.Is(err, storage.ErrUnsupported) {
		t.Errorf("MD5 の要求 = %v, want ErrUnsupported", err)
	}
}

// サーバー側コピーと移動を確かめます。
func TestServerSideCopyAndMove(t *testing.T) {
	ctx, _, s := newTestStorage(t)
	put(t, ctx, s, "/元.txt", "なかみ")

	if _, err := s.ServerSideCopy(ctx, "/元.txt", "/複製.txt"); err != nil {
		t.Fatalf("ServerSideCopy: %v", err)
	}
	if got := readAll(t, ctx, s, "/複製.txt"); got != "なかみ" {
		t.Errorf("複製の内容 = %q", got)
	}
	if got := readAll(t, ctx, s, "/元.txt"); got != "なかみ" {
		t.Error("コピーなのに元が消えている")
	}

	if err := s.Move(ctx, "/複製.txt", "/移動先.txt"); err != nil {
		t.Fatalf("Move: %v", err)
	}
	if _, err := s.Stat(ctx, "/複製.txt"); !storage.IsNotFound(err) {
		t.Error("移動なのに元が残っている")
	}

	// 移動先にすでにある場合も、上書きとして成立すること。
	put(t, ctx, s, "/邪魔.txt", "ふるい")
	if err := s.Move(ctx, "/移動先.txt", "/邪魔.txt"); err != nil {
		t.Fatalf("上書きの Move: %v", err)
	}
	if got := readAll(t, ctx, s, "/邪魔.txt"); got != "なかみ" {
		t.Errorf("上書き後の内容 = %q", got)
	}
}

// 更新時刻が秒に丸められて保存されることを確かめます。
func TestModTimeRoundTrip(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	// 秒未満を含む時刻を渡す。
	want := time.Date(2021, 6, 15, 12, 34, 56, 789_000_000, time.UTC)

	written, err := s.Put(ctx, "/時刻.txt", strings.NewReader("x"), storage.ObjectMeta{
		Size:    1,
		ModTime: want,
	})
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Dropbox は秒までしか保持しない。
	// 丸めずに渡すと、読み戻した時刻が違って見えて毎回コピーし直しになる。
	wantTruncated := want.Truncate(time.Second)
	if !written.ModTime.Equal(wantTruncated) {
		t.Errorf("書き込み結果の時刻 = %v, want %v", written.ModTime, wantTruncated)
	}

	stat, err := s.Stat(ctx, "/時刻.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !stat.ModTime.Equal(wantTruncated) {
		t.Errorf("読み戻した時刻 = %v, want %v", stat.ModTime, wantTruncated)
	}
}

func TestOpenRange(t *testing.T) {
	ctx, _, s := newTestStorage(t)
	put(t, ctx, s, "/範囲.txt", "0123456789")

	tests := []struct {
		offset, length int64
		want           string
	}{
		{0, 3, "012"},
		{4, 3, "456"},
		{7, -1, "789"},
		{0, -1, "0123456789"},
	}
	for _, tt := range tests {
		rc, err := s.OpenRange(ctx, "/範囲.txt", tt.offset, tt.length)
		if err != nil {
			t.Fatalf("OpenRange(%d,%d): %v", tt.offset, tt.length, err)
		}
		got, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(got) != tt.want {
			t.Errorf("OpenRange(%d,%d) = %q, want %q", tt.offset, tt.length, got, tt.want)
		}
	}
}

func TestNormalize(t *testing.T) {
	tests := map[string]string{
		"":          "",
		"/":         "",
		".":         "",
		"/a":        "/a",
		"a":         "/a",
		"/a/":       "/a",
		"/a//b":     "/a/b",
		"/a/./b":    "/a/b",
		"/a/b/../c": "/a/c",
		// 逆斜線は区切りではなく、名前の一部として扱う。
		"\\a\\b":    "/\\a\\b",
		"/日本語/ファイル": "/日本語/ファイル",
	}
	for in, want := range tests {
		if got := normalize(in); got != want {
			t.Errorf("normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestClassifySummary(t *testing.T) {
	tests := []struct {
		summary  string
		sentinel error
		class    storage.Class
	}{
		{"path/not_found/...", storage.ErrNotFound, storage.ClassPermanent},
		{"path/not_folder/", storage.ErrNotDir, storage.ClassPermanent},
		{"path/not_file/", storage.ErrIsDir, storage.ClassPermanent},
		{"path/conflict/file/", storage.ErrExist, storage.ClassPermanent},
		{"path/insufficient_space/", nil, storage.ClassPermanent},
		{"too_many_write_operations/", nil, storage.ClassRateLimit},
		{"知らないもの/", nil, storage.ClassUnknown},
	}
	for _, tt := range tests {
		got := classifySummary(tt.summary)
		if !errors.Is(got.sentinel, tt.sentinel) {
			t.Errorf("classifySummary(%q) の番兵 = %v, want %v", tt.summary, got.sentinel, tt.sentinel)
		}
		if got.class != tt.class {
			t.Errorf("classifySummary(%q) の種類 = %v, want %v", tt.summary, got.class, tt.class)
		}
	}
}
