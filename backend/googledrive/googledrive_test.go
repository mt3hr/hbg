package googledrive

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
	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

// 適合性テストを偽サーバーに対して実行します。
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			f := newFakeDrive()
			s := f.start(t)

			root := "/試験"
			if err := s.Mkdir(context.Background(), root); err != nil {
				t.Fatalf("試験用のディレクトリを作れません: %v", err)
			}
			return s, root
		},
		// 偽サーバーは1ページ3件なので、件数を増やすとやりとりが増える。
		// ページ分割の確認としては十分な数にする。
		LargeDirCount: 120,
	})
}

func newTestStorage(t *testing.T) (context.Context, *fakeDrive, *Storage) {
	t.Helper()
	f := newFakeDrive()
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

// 件数の多いフォルダで、続きの取得が漏れないことを確かめます。
//
// 以前は nextPageToken を Fields に指定しながらたどっておらず、
// 子が1000件を超えるフォルダでは超過分がエラーも警告もなく
// 欠落していました。同期の道具としては「コピーされないファイルが
// 静かに出る」ということになります。
func TestListFollowsPages(t *testing.T) {
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
	if f.callCount("list") < n/f.pageSize {
		t.Errorf("一覧の呼び出し = %d, 続きの取得が足りない", f.callCount("list"))
	}
}

// 存在しない途中の段があったら、その場で失敗することを確かめます。
//
// 以前は見つからない段を黙って読み飛ばし、ひとつ上の階層の中身を
// そのまま結果として返していました。存在しない場所を指定したのに
// エラーにならないため、別のディレクトリを対象にコピーや削除を
// 行ってしまう恐れがあります。
func TestResolveDoesNotFallThrough(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	put(t, ctx, s, "/実在/中身.txt", "だいじ")

	err := s.List(ctx, "/実在/ない階層/さらに下", func(storage.FileInfo) error {
		t.Error("存在しない場所なのに中身が返ってきた")
		return nil
	})
	if !storage.IsNotFound(err) {
		t.Fatalf("List = %v, want ErrNotFound", err)
	}

	if _, err := s.Stat(ctx, "/実在/ない階層/中身.txt"); !storage.IsNotFound(err) {
		t.Errorf("Stat = %v, want ErrNotFound", err)
	}
}

// ディレクトリと同名のファイルが解決先を横取りしないことを確かめます。
func TestResolveRejectsFileAsDirectory(t *testing.T) {
	ctx, _, s := newTestStorage(t)
	put(t, ctx, s, "/まぎらわしい", "これはファイル")

	err := s.List(ctx, "/まぎらわしい", func(storage.FileInfo) error { return nil })
	if !errors.Is(err, storage.ErrNotDir) {
		t.Errorf("List = %v, want ErrNotDir", err)
	}
}

// 引用符を含む名前でも、検索式が壊れないことを確かめます。
func TestQuotedNames(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	names := []string{"it's.txt", `back\slash.txt`, "'both'\\.txt"}
	for _, name := range names {
		p := "/引用符/" + name
		put(t, ctx, s, p, name)

		if got := readAll(t, ctx, s, p); got != name {
			t.Errorf("%q の内容 = %q", name, got)
		}
	}

	got := map[string]bool{}
	if err := s.List(ctx, "/引用符", func(fi storage.FileInfo) error {
		got[fi.Name] = true
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, name := range names {
		if !got[name] {
			t.Errorf("%q が一覧に出ていない", name)
		}
	}
}

// Google の独自形式を、空のファイルとして扱わないことを確かめます。
//
// 独自形式はサイズを持たないため、以前の実装ではサイズ 0 と伝わり、
// Dropbox 側の切り詰めと合わさって「中身が空のファイル」が
// できあがっていました。しかも失敗として報告されません。
func TestNativeFilesAreNotSilentlyEmpty(t *testing.T) {
	ctx, f, s := newTestStorage(t)

	f.mu.Lock()
	f.files["doc1"] = &fakeFile{
		id:       "doc1",
		name:     "設計メモ",
		mimeType: "application/vnd.google-apps.document",
		parents:  []string{"root"},
		modified: time.Now().UTC(),
	}
	f.mu.Unlock()

	var found *storage.FileInfo
	if err := s.List(ctx, "/", func(fi storage.FileInfo) error {
		if fi.Name == "設計メモ" {
			found = &fi
		}
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if found == nil {
		t.Fatal("独自形式のファイルが一覧に出ていない")
	}
	if found.Size != storage.SizeUnknown {
		t.Errorf("サイズ = %d, want SizeUnknown（0 だと空ファイルと区別できない）", found.Size)
	}

	// 読もうとしたら、はっきり失敗すること。
	_, _, err := s.Open(ctx, "/設計メモ")
	if !errors.Is(err, storage.ErrUnsupported) {
		t.Errorf("Open = %v, want ErrUnsupported", err)
	}
	if class := storage.ClassOf(err); class != storage.ClassPermanent {
		t.Errorf("失敗の種類 = %v, want permanent", class)
	}
}

// native_files: skip を指定すると一覧から外れることを確かめます。
func TestNativeFilesSkip(t *testing.T) {
	f := newFakeDrive()
	srv := f.start(t)
	s, err := newWithService(Config{Name: "skip版", NativeFiles: nativeSkip}, srv.srv)
	if err != nil {
		t.Fatalf("newWithService: %v", err)
	}
	ctx := context.Background()

	f.mu.Lock()
	f.files["doc1"] = &fakeFile{
		id: "doc1", name: "設計メモ",
		mimeType: "application/vnd.google-apps.document",
		parents:  []string{"root"}, modified: time.Now().UTC(),
	}
	f.mu.Unlock()
	put(t, ctx, s, "/ふつう.txt", "なかみ")

	names := []string{}
	if err := s.List(ctx, "/", func(fi storage.FileInfo) error {
		names = append(names, fi.Name)
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}

	for _, name := range names {
		if name == "設計メモ" {
			t.Error("skip を指定したのに独自形式が一覧に出ている")
		}
	}
	if len(names) != 1 {
		t.Errorf("一覧 = %v, want [ふつう.txt]", names)
	}
}

// 削除が既定でゴミ箱に入ることを確かめます。
//
// 以前は完全削除だったため、誤って消したものを取り戻す手立てが
// ありませんでした。
func TestRemoveUsesTrash(t *testing.T) {
	ctx, f, s := newTestStorage(t)
	fi := put(t, ctx, s, "/消す.txt", "なかみ")

	if err := s.Remove(ctx, "/消す.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if _, err := s.Stat(ctx, "/消す.txt"); !storage.IsNotFound(err) {
		t.Errorf("Stat = %v, want ErrNotFound", err)
	}

	// 実体は残っていて、ゴミ箱に入っているだけであること。
	f.mu.Lock()
	defer f.mu.Unlock()
	e, ok := f.files[fi.ID]
	if !ok {
		t.Fatal("完全に消えている。ゴミ箱に入るはず")
	}
	if !e.trashed {
		t.Error("ゴミ箱に入っていない")
	}
}

// 完全削除を選べることを確かめます。
func TestRemoveWithoutTrash(t *testing.T) {
	f := newFakeDrive()
	base := f.start(t)

	no := false
	s, err := newWithService(Config{Name: "完全削除版", UseTrash: &no}, base.srv)
	if err != nil {
		t.Fatalf("newWithService: %v", err)
	}
	ctx := context.Background()

	fi := put(t, ctx, s, "/消す.txt", "なかみ")
	if err := s.Remove(ctx, "/消す.txt"); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.files[fi.ID]; ok {
		t.Error("完全削除を指定したのに残っている")
	}
}

// Remove が中身ごと消してしまわないことを確かめます。
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

	if err := s.Purge(ctx, "/消さない"); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if _, err := s.Stat(ctx, "/消さない"); !storage.IsNotFound(err) {
		t.Errorf("Purge のあとも残っている: %v", err)
	}
}

// 同じ場所へ2度書いても、同名のものが2つできないことを確かめます。
//
// Drive は同じフォルダに同名のファイルを作れてしまうため、
// 毎回 Create すると重複が増えていきます。
func TestPutOverwritesInsteadOfDuplicating(t *testing.T) {
	ctx, f, s := newTestStorage(t)

	first := put(t, ctx, s, "/一度きり.txt", "ふるい")
	second := put(t, ctx, s, "/一度きり.txt", "あたらしい")

	if first.ID != second.ID {
		t.Errorf("IDが変わっている（%s → %s）。上書きではなく作り直している", first.ID, second.ID)
	}
	if got := readAll(t, ctx, s, "/一度きり.txt"); got != "あたらしい" {
		t.Errorf("内容 = %q", got)
	}

	count := 0
	if err := s.List(ctx, "/", func(fi storage.FileInfo) error {
		if fi.Name == "一度きり.txt" {
			count++
		}
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if count != 1 {
		t.Errorf("同名のものが %d 件ある", count)
	}
	_ = f
}

// 分割送信の経路を通ることを確かめます。
func TestResumableUpload(t *testing.T) {
	// 分割の単位は 256KiB 単位に丸められる。
	orig := uploadChunkSize
	uploadChunkSize = googleapi.MinUploadChunkSize
	t.Cleanup(func() { uploadChunkSize = orig })

	ctx, f, s := newTestStorage(t)

	content := strings.Repeat("あ", 300*1024) // 900KiB 程度
	put(t, ctx, s, "/大きい.bin", content)

	if got := readAll(t, ctx, s, "/大きい.bin"); got != content {
		t.Errorf("内容の長さ = %d, want %d", len(got), len(content))
	}
	if f.callCount("upload_chunk") < 2 {
		t.Errorf("分割送信の回数 = %d, 分割されていない", f.callCount("upload_chunk"))
	}
}

// サイズが実際より小さく伝えられても、内容が切り詰められないことを確かめます。
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
			if got := readAll(t, ctx, s, p); got != content {
				t.Errorf("内容の長さ = %d, want %d", len(got), len(content))
			}
		})
	}
}

// ハッシュが取得できることを確かめます。
func TestHash(t *testing.T) {
	ctx, _, s := newTestStorage(t)
	fi := put(t, ctx, s, "/ハッシュ.txt", "なかみ")

	for _, ht := range []storage.HashType{storage.MD5, storage.SHA1, storage.SHA256} {
		want, ok := fi.Hashes[ht]
		if !ok || want == "" {
			t.Errorf("書き込み結果に %s がない", ht)
			continue
		}
		got, err := s.Hash(ctx, "/ハッシュ.txt", ht)
		if err != nil {
			t.Errorf("Hash(%s): %v", ht, err)
			continue
		}
		if got != want {
			t.Errorf("Hash(%s) = %s, want %s", ht, got, want)
		}
	}

	// 扱えないハッシュは黙って別のもので代用しないこと。
	if _, err := s.Hash(ctx, "/ハッシュ.txt", storage.DropboxContent); !errors.Is(err, storage.ErrUnsupported) {
		t.Errorf("dropbox 形式の要求 = %v, want ErrUnsupported", err)
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

	if err := s.Move(ctx, "/複製.txt", "/入れ物/移動先.txt"); err != nil {
		t.Fatalf("Move: %v", err)
	}
	if _, err := s.Stat(ctx, "/複製.txt"); !storage.IsNotFound(err) {
		t.Error("移動なのに元が残っている")
	}
	if got := readAll(t, ctx, s, "/入れ物/移動先.txt"); got != "なかみ" {
		t.Errorf("移動先の内容 = %q", got)
	}
}

// 消したあとに同じ名前で作り直しても、古いIDを掴まないことを確かめます。
func TestResolverForgetsRemovedDirs(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	put(t, ctx, s, "/入れ替え/中身.txt", "ふるい")
	if err := s.Purge(ctx, "/入れ替え"); err != nil {
		t.Fatalf("Purge: %v", err)
	}

	put(t, ctx, s, "/入れ替え/中身.txt", "あたらしい")
	if got := readAll(t, ctx, s, "/入れ替え/中身.txt"); got != "あたらしい" {
		t.Errorf("内容 = %q, want あたらしい（古いIDを掴んでいる）", got)
	}
}

// 一時的な失敗と待っても直らない失敗を取り違えないことを確かめます。
func TestClassifyStatus(t *testing.T) {
	tests := []struct {
		code   int
		reason string
		want   storage.Class
	}{
		{404, "notFound", storage.ClassPermanent},
		{401, "authError", storage.ClassAuth},
		{429, "", storage.ClassRateLimit},
		// 403 は権限と流量制限の両方に使われる。reason で分ける。
		{403, "userRateLimitExceeded", storage.ClassRateLimit},
		{403, "rateLimitExceeded", storage.ClassRateLimit},
		{403, "storageQuotaExceeded", storage.ClassPermanent},
		{403, "insufficientFilePermissions", storage.ClassAuth},
		{500, "backendError", storage.ClassRetryable},
		{503, "backendError", storage.ClassRetryable},
		{400, "badRequest", storage.ClassPermanent},
	}
	for _, tt := range tests {
		if got := classifyStatus(tt.code, tt.reason); got.class != tt.want {
			t.Errorf("classifyStatus(%d, %q) = %v, want %v", tt.code, tt.reason, got.class, tt.want)
		}
	}
}

// API のエラーが正しく分類されることを確かめます。
func TestErrorClassification(t *testing.T) {
	ctx, f, s := newTestStorage(t)
	put(t, ctx, s, "/ある.txt", "x")

	f.failNext("list", 100, 403,
		`{"error":{"code":403,"message":"多すぎます","errors":[{"reason":"userRateLimitExceeded"}]}}`)

	_, err := s.Stat(ctx, "/ある.txt")
	if class := storage.ClassOf(err); class != storage.ClassRateLimit {
		t.Errorf("失敗の種類 = %v, want ratelimit", class)
	}
}

func TestCleanPath(t *testing.T) {
	tests := map[string]string{
		"":          "/",
		"/":         "/",
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
		if got := cleanPath(in); got != want {
			t.Errorf("cleanPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEscapeQuery(t *testing.T) {
	tests := map[string]string{
		"ふつう":     "ふつう",
		"it's":    `it\'s`,
		`back\sl`: `back\\sl`,
	}
	for in, want := range tests {
		if got := escapeQuery(in); got != want {
			t.Errorf("escapeQuery(%q) = %q, want %q", in, got, want)
		}
	}
}

// 同名のものが複数ある場合に、選び方が実行ごとに変わらないことを確かめます。
func TestDuplicateNamesAreDeterministic(t *testing.T) {
	ctx, f, s := newTestStorage(t)

	f.mu.Lock()
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	recent := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	f.files["dupA"] = &fakeFile{id: "dupA", name: "同名.txt", mimeType: "text/plain",
		parents: []string{"root"}, modified: old, data: []byte("ふるい")}
	f.files["dupB"] = &fakeFile{id: "dupB", name: "同名.txt", mimeType: "text/plain",
		parents: []string{"root"}, modified: recent, data: []byte("あたらしい")}
	f.mu.Unlock()

	for range 5 {
		if got := readAll(t, ctx, s, "/同名.txt"); got != "あたらしい" {
			t.Fatalf("内容 = %q, want あたらしい（更新の新しいほうを選ぶこと）", got)
		}
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

// 共有ドライブの指定が呼び出しに反映されることを確かめます。
func TestSharedDriveParameters(t *testing.T) {
	f := newFakeDrive()
	base := f.start(t)

	s, err := newWithService(Config{Name: "共有ドライブ", DriveID: "drive123"}, base.srv)
	if err != nil {
		t.Fatalf("newWithService: %v", err)
	}
	if s.rootID != "drive123" {
		t.Errorf("ルート = %q, want drive123", s.rootID)
	}

	call := s.listCall(context.Background(), "'x' in parents")
	if call == nil {
		t.Fatal("呼び出しを組み立てられない")
	}

	// 共有ドライブを指定していないほうには付かないこと。
	plain, err := newWithService(Config{Name: "個人"}, base.srv)
	if err != nil {
		t.Fatalf("newWithService: %v", err)
	}
	if plain.driveID != "" {
		t.Errorf("共有ドライブID = %q, want 空", plain.driveID)
	}
	if plain.rootID != "root" {
		t.Errorf("ルート = %q, want root", plain.rootID)
	}
}

// 設定の誤りは起動時に知らせることを確かめます。
func TestRejectsUnknownNativeFilesSetting(t *testing.T) {
	f := newFakeDrive()
	base := f.start(t)

	_, err := newWithService(Config{Name: "誤設定", NativeFiles: "ignore"}, base.srv)
	if err == nil {
		t.Fatal("知らない指定なのに通ってしまった")
	}
	if !strings.Contains(err.Error(), "native_files") {
		t.Errorf("どの設定が悪いのか分からない: %v", err)
	}
}

var _ = drive.File{}
