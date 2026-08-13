package s3

import (
	"context"
	"crypto/md5"
	"encoding/hex"
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
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			f := newFakeS3()
			s := f.start(t)

			root := "/試験"
			if err := s.Mkdir(context.Background(), root); err != nil {
				t.Fatalf("試験用のディレクトリを作れません: %v", err)
			}
			return s, root
		},
		LargeDirCount: 120,
	})
}

func newTestStorage(t *testing.T, mutate ...func(*Config)) (context.Context, *fakeS3, *Storage) {
	t.Helper()
	f := newFakeS3()
	return context.Background(), f, f.start(t, mutate...)
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

// 名前の中の "/" が階層として見えることを確かめます。
//
// オブジェクトストレージにディレクトリはありません。
// 区切り文字を指定して問い合わせることで、同じ接頭辞を持つものが
// ひとまとめになって返ってくるのを、ディレクトリとして見せています。
func TestDirectoriesAreVirtual(t *testing.T) {
	ctx, f, s := newTestStorage(t)

	put(t, ctx, s, "/写真/2024/a.jpg", "A")
	put(t, ctx, s, "/写真/2024/b.jpg", "B")
	put(t, ctx, s, "/写真/2023/c.jpg", "C")
	put(t, ctx, s, "/直下.txt", "D")

	// 印を書いていないので、入っているのは4件だけ。
	if got := f.objectCount(); got != 4 {
		t.Errorf("入っている件数 = %d, want 4", got)
	}

	names := map[string]bool{}
	dirs := map[string]bool{}
	if err := s.List(ctx, "/", func(fi storage.FileInfo) error {
		if fi.IsDir {
			dirs[fi.Name] = true
		} else {
			names[fi.Name] = true
		}
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}

	if !dirs["写真"] {
		t.Error("写真 がディレクトリとして見えていない")
	}
	if !names["直下.txt"] {
		t.Error("直下.txt が見えていない")
	}
	if len(names)+len(dirs) != 2 {
		t.Errorf("ルートの一覧 = %v %v, 2件であるべき", dirs, names)
	}

	// 1階層下も同じように見えること。
	sub := map[string]bool{}
	if err := s.List(ctx, "/写真", func(fi storage.FileInfo) error {
		sub[fi.Name] = fi.IsDir
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if !sub["2024"] || !sub["2023"] {
		t.Errorf("写真 の下 = %v", sub)
	}
}

// 元のファイルの更新時刻が保たれることを確かめます。
//
// オブジェクトストレージが持つ時刻は「書き込まれた時刻」なので、
// 元の時刻は利用者定義の項目に入れておく必要があります。
// これがないと、次回の比較で毎回コピーし直すことになります。
func TestModTimeSurvivesRoundTrip(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	want := time.Date(2021, 6, 15, 12, 34, 56, 123456789, time.UTC)
	if _, err := s.Put(ctx, "/時刻.txt", strings.NewReader("x"), storage.ObjectMeta{
		Size:    1,
		ModTime: want,
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Stat でも一覧でも、書き込まれた時刻ではなく元の時刻が見えること。
	fi, err := s.Stat(ctx, "/時刻.txt")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !fi.ModTime.Equal(want) {
		t.Errorf("Stat の更新時刻 = %v, want %v", fi.ModTime, want)
	}

	var listed storage.FileInfo
	if err := s.List(ctx, "/", func(e storage.FileInfo) error {
		if e.Name == "時刻.txt" {
			listed = e
		}
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if !listed.ModTime.Equal(want) {
		t.Errorf("一覧の更新時刻 = %v, want %v", listed.ModTime, want)
	}

	// 読み出したときも元の時刻が付いてくること。
	_, info, err := s.Open(ctx, "/時刻.txt")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if !info.ModTime.Equal(want) {
		t.Errorf("Open の更新時刻 = %v, want %v", info.ModTime, want)
	}
}

// list_metadata: none では問い合わせが増えないことを確かめます。
func TestListMetadataNone(t *testing.T) {
	ctx, f, s := newTestStorage(t, func(c *Config) { c.ListMetadata = ListMetadataNone })

	for i := range 5 {
		put(t, ctx, s, fmt.Sprintf("/多い/%d.txt", i), "x")
	}

	before := f.callCount("head")
	if err := s.List(ctx, "/多い", func(storage.FileInfo) error { return nil }); err != nil {
		t.Fatalf("List: %v", err)
	}
	if got := f.callCount("head") - before; got != 0 {
		t.Errorf("一覧のための問い合わせ = %d件, want 0", got)
	}
}

// 更新時刻の書式が rclone と同じであることを確かめます。
//
// 同じ入れ物を両方の道具から使えるようにするためです。
func TestModTimeFormat(t *testing.T) {
	tests := []struct {
		t    time.Time
		want string
	}{
		{time.Unix(1700000000, 0).UTC(), "1700000000.000000000"},
		{time.Unix(1700000000, 123456789).UTC(), "1700000000.123456789"},
		{time.Unix(0, 0).UTC(), "0.000000000"},
	}
	for _, tt := range tests {
		got := formatModTime(tt.t)
		if got != tt.want {
			t.Errorf("formatModTime(%v) = %q, want %q", tt.t, got, tt.want)
		}
		back, ok := metaModTime(map[string]string{mtimeMeta: got})
		if !ok {
			t.Errorf("%q を読み戻せない", got)
			continue
		}
		if !back.Equal(tt.t) {
			t.Errorf("読み戻し = %v, want %v", back, tt.t)
		}
	}

	// 桁の足りない書式も読めること。他の道具が書いたものに備える。
	if got, ok := metaModTime(map[string]string{mtimeMeta: "1700000000.5"}); !ok ||
		!got.Equal(time.Unix(1700000000, 500000000).UTC()) {
		t.Errorf("桁の少ない書式 = %v, %v", got, ok)
	}
	if got, ok := metaModTime(map[string]string{mtimeMeta: "1700000000"}); !ok ||
		!got.Equal(time.Unix(1700000000, 0).UTC()) {
		t.Errorf("小数のない書式 = %v, %v", got, ok)
	}
}

// 分割して送っても内容が壊れないことを確かめます。
func TestMultipartUpload(t *testing.T) {
	// 分割の下限は 5MiB。それを超える大きさで試す。
	ctx, f, s := newTestStorage(t, func(c *Config) { c.UploadPartSizeMiB = 5 })

	content := strings.Repeat("0123456789", 1200000) // 12MB 程度
	put(t, ctx, s, "/大きい.bin", content)

	if f.callCount("create_multipart") == 0 {
		t.Error("分割送信が使われていない")
	}
	if got := readAll(t, ctx, s, "/大きい.bin"); got != content {
		t.Errorf("内容の長さ = %d, want %d", len(got), len(content))
	}
}

// 分割して送ったものでも、元の MD5 が分かれば照合に使えることを確かめます。
//
// 分割送信の ETag は MD5 ではありません。転送元で分かっている場合に
// 控えておかないと、内容の比較ができなくなります。
func TestMD5OfMultipartObject(t *testing.T) {
	ctx, _, s := newTestStorage(t, func(c *Config) { c.UploadPartSizeMiB = 5 })

	content := strings.Repeat("x", 6*1024*1024)
	sum := md5.Sum([]byte(content))
	want := hex.EncodeToString(sum[:])

	if _, err := s.Put(ctx, "/控えあり.bin", strings.NewReader(content), storage.ObjectMeta{
		Size:   int64(len(content)),
		Hashes: map[storage.HashType]string{storage.MD5: want},
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := s.Hash(ctx, "/控えあり.bin", storage.MD5)
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	if got != want {
		t.Errorf("Hash = %s, want %s", got, want)
	}

	// 控えがない場合は、できないとはっきり伝えること。
	// 黙って ETag を MD5 として使うと、常に食い違うことになる。
	if _, err := s.Put(ctx, "/控えなし.bin", strings.NewReader(content), storage.ObjectMeta{
		Size: int64(len(content)),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := s.Hash(ctx, "/控えなし.bin", storage.MD5); !errors.Is(err, storage.ErrUnsupported) {
		t.Errorf("Hash = %v, want ErrUnsupported", err)
	}
}

// 1回で送ったものは ETag から MD5 が取れることを確かめます。
func TestMD5OfSinglePartObject(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	content := "なかみ"
	sum := md5.Sum([]byte(content))
	want := hex.EncodeToString(sum[:])

	put(t, ctx, s, "/ふつう.txt", content)

	got, err := s.Hash(ctx, "/ふつう.txt", storage.MD5)
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	if got != want {
		t.Errorf("Hash = %s, want %s", got, want)
	}
}

func TestEtagMD5(t *testing.T) {
	tests := map[string]string{
		`"5eb63bbbe01eeed093cb22bb8f5acdc3"`: "5eb63bbbe01eeed093cb22bb8f5acdc3",
		"5eb63bbbe01eeed093cb22bb8f5acdc3":   "5eb63bbbe01eeed093cb22bb8f5acdc3",
		// 分割して送ったものは MD5 ではない。
		`"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-3"`: "",
		"":                                     "",
	}
	for in, want := range tests {
		if got := etagMD5(in); got != want {
			t.Errorf("etagMD5(%q) = %q, want %q", in, got, want)
		}
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

// root を指定すると、その下が起点になることを確かめます。
func TestRootIsApplied(t *testing.T) {
	ctx, f, s := newTestStorage(t, func(c *Config) { c.Root = "起点" })

	put(t, ctx, s, "/中身.txt", "なかみ")

	f.mu.Lock()
	_, ok := f.objects["起点/中身.txt"]
	f.mu.Unlock()
	if !ok {
		t.Error("起点の下に書かれていない")
	}

	if got := readAll(t, ctx, s, "/中身.txt"); got != "なかみ" {
		t.Errorf("内容 = %q", got)
	}
}

// 続きの取得が漏れないことを確かめます。
func TestListFollowsContinuation(t *testing.T) {
	ctx, _, s := newTestStorage(t)

	const n = 20
	for i := range n {
		put(t, ctx, s, fmt.Sprintf("/多い/%03d.txt", i), "x")
	}

	count := 0
	if err := s.List(ctx, "/多い", func(storage.FileInfo) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if count != n {
		t.Errorf("列挙できた件数 = %d, want %d", count, n)
	}
}

// エラーの分類を確かめます。
func TestErrorClassification(t *testing.T) {
	tests := []struct {
		status int
		code   string
		want   storage.Class
	}{
		{404, "NoSuchKey", storage.ClassPermanent},
		{403, "AccessDenied", storage.ClassAuth},
		{403, "SignatureDoesNotMatch", storage.ClassAuth},
		{429, "TooManyRequests", storage.ClassRateLimit},
		// SlowDown は 503 で返ってくる。待って試し直す。
		{503, "SlowDown", storage.ClassRateLimit},
		{500, "InternalError", storage.ClassRetryable},
		{400, "EntityTooLarge", storage.ClassPermanent},
	}
	for _, tt := range tests {
		if got := classifyStatus(tt.status, tt.code); got.class != tt.want {
			t.Errorf("classifyStatus(%d, %q) = %v, want %v", tt.status, tt.code, got.class, tt.want)
		}
	}
}

// 権限の問題は再試行せず、処理全体を止めることを確かめます。
func TestAccessDeniedIsFatal(t *testing.T) {
	ctx, f, s := newTestStorage(t)
	f.failNext("head", 100, 403, "AccessDenied")

	_, err := s.Stat(ctx, "/どこか.txt")
	if class := storage.ClassOf(err); class != storage.ClassAuth {
		t.Errorf("失敗の種類 = %v, want auth", class)
	}
	if storage.ClassOf(err).Retryable() {
		t.Error("権限の問題が再試行の対象になっている")
	}
}

// 設定の誤りは接続を試みる前に知らせることを確かめます。
func TestValidate(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"入れ物がない", Config{}, "bucket"},
		{"知らない提供元", Config{Bucket: "b", Provider: "どこか"}, "provider"},
		{"知らない一覧の指定", Config{Bucket: "b", ListMetadata: "ときどき"}, "list_metadata"},
		{"r2 に接続先も口座IDもない", Config{Bucket: "b", Provider: ProviderR2}, "account_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.validate()
			if err == nil {
				t.Fatal("誤りなのに通ってしまった")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("どこが悪いのか分からない: %v", err)
			}
		})
	}
}

// 提供元から接続先が決まることを確かめます。
func TestEndpoint(t *testing.T) {
	tests := []struct {
		cfg  Config
		want string
	}{
		{Config{Provider: ProviderR2, AccountID: "ACCOUNT"}, "https://ACCOUNT.r2.cloudflarestorage.com"},
		{Config{Provider: ProviderR2, Endpoint: "https://例.invalid"}, "https://例.invalid"},
		{Config{Provider: ProviderAWS}, ""},
	}
	for _, tt := range tests {
		if got := tt.cfg.endpoint(); got != tt.want {
			t.Errorf("endpoint(%+v) = %q, want %q", tt.cfg, got, tt.want)
		}
	}
}

func TestCleanPathAndKey(t *testing.T) {
	s := &Storage{root: ""}
	keys := map[string]string{
		"/":        "",
		"/a/b.txt": "a/b.txt",
		"a/b.txt":  "a/b.txt",
		"/a//b":    "a/b",
		// 逆斜線は区切りではなく、名前の一部として扱う。
		"/a\\b": "a\\b",
	}
	for in, want := range keys {
		if got := s.key(in); got != want {
			t.Errorf("key(%q) = %q, want %q", in, got, want)
		}
	}

	rooted := &Storage{root: "起点"}
	if got := rooted.key("/a.txt"); got != "起点/a.txt" {
		t.Errorf("起点つき key = %q", got)
	}
	if got := rooted.pathOf("起点/a.txt"); got != "/a.txt" {
		t.Errorf("起点つき pathOf = %q", got)
	}
}
