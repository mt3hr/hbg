package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"
)

// expectedDropboxHash は仕様どおりに素朴に計算した参照実装です。
// 逐次計算する実装と一致することを確かめるために使います。
func expectedDropboxHash(data []byte) string {
	digests := []byte{}
	for len(data) > 0 {
		n := min(dropboxBlockSize, len(data))
		sum := sha256.Sum256(data[:n])
		digests = append(digests, sum[:]...)
		data = data[n:]
	}
	overall := sha256.Sum256(digests)
	return hex.EncodeToString(overall[:])
}

func TestDropboxContentHash(t *testing.T) {
	tests := []struct {
		name string
		size int
	}{
		{"空", 0},
		{"1バイト", 1},
		{"ブロック未満", 1000},
		{"ブロック境界のちょうど手前", dropboxBlockSize - 1},
		{"ブロック境界ちょうど", dropboxBlockSize},
		{"ブロック境界の1バイト後", dropboxBlockSize + 1},
		{"2ブロックちょうど", dropboxBlockSize * 2},
		{"2ブロックと端数", dropboxBlockSize*2 + 12345},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]byte, tt.size)
			for i := range data {
				data[i] = byte(i % 251)
			}

			h := NewDropboxContentHash()
			if _, err := h.Write(data); err != nil {
				t.Fatalf("Write: %v", err)
			}
			got := hex.EncodeToString(h.Sum(nil))

			if want := expectedDropboxHash(data); got != want {
				t.Errorf("ハッシュが一致しない\n got: %s\nwant: %s", got, want)
			}
		})
	}
}

// 書き込みを細切れにしても結果が変わらないことを確認します。
// 転送は少しずつ読み書きされるため、ここが崩れると検証が成立しません。
func TestDropboxContentHashChunkedWrites(t *testing.T) {
	data := make([]byte, dropboxBlockSize*2+9999)
	for i := range data {
		data[i] = byte(i % 251)
	}
	want := expectedDropboxHash(data)

	for _, chunk := range []int{1, 7, 4096, 65536, dropboxBlockSize - 1, dropboxBlockSize + 1} {
		h := NewDropboxContentHash()
		for i := 0; i < len(data); i += chunk {
			end := min(i+chunk, len(data))
			if _, err := h.Write(data[i:end]); err != nil {
				t.Fatalf("Write: %v", err)
			}
		}
		if got := hex.EncodeToString(h.Sum(nil)); got != want {
			t.Errorf("%d バイトずつ書いた場合にハッシュが変わる\n got: %s\nwant: %s", chunk, got, want)
		}
	}
}

// Sum は状態を変えてはいけません。
func TestDropboxContentHashSumDoesNotMutate(t *testing.T) {
	h := NewDropboxContentHash()
	h.Write([]byte("hello"))

	first := hex.EncodeToString(h.Sum(nil))
	second := hex.EncodeToString(h.Sum(nil))
	if first != second {
		t.Errorf("Sum を2回呼ぶと結果が変わる: %s / %s", first, second)
	}

	// Sum のあとも書き足せること
	h.Write([]byte(" world"))
	got := hex.EncodeToString(h.Sum(nil))
	if want := expectedDropboxHash([]byte("hello world")); got != want {
		t.Errorf("Sum のあとの Write で結果がおかしい\n got: %s\nwant: %s", got, want)
	}
}

func TestDropboxContentHashReset(t *testing.T) {
	h := NewDropboxContentHash()
	h.Write(make([]byte, dropboxBlockSize+100))
	h.Reset()
	h.Write([]byte("abc"))

	if got, want := hex.EncodeToString(h.Sum(nil)), expectedDropboxHash([]byte("abc")); got != want {
		t.Errorf("Reset 後の結果がおかしい\n got: %s\nwant: %s", got, want)
	}
}

func TestCommonHash(t *testing.T) {
	tests := []struct {
		name  string
		a, b  HashSet
		want  HashType
		found bool
	}{
		{
			name: "共通するものがある",
			a:    HashSet{SHA256, MD5}, b: HashSet{MD5},
			want: MD5, found: true,
		},
		{
			name: "優先度の高いほうが選ばれる",
			a:    HashSet{SHA256, MD5}, b: HashSet{MD5, SHA256},
			want: SHA256, found: true,
		},
		{
			name: "共通するものがない",
			a:    HashSet{DropboxContent}, b: HashSet{MD5},
			found: false,
		},
		{
			name: "片方が空",
			a:    HashSet{}, b: HashSet{MD5},
			found: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := CommonHash(tt.a, tt.b)
			if ok != tt.found {
				t.Fatalf("found = %v, want %v", ok, tt.found)
			}
			if ok && got != tt.want {
				t.Errorf("got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMultiHasher(t *testing.T) {
	data := "hello world"

	w, sum, err := MultiHasher(MD5, SHA256, DropboxContent)
	if err != nil {
		t.Fatalf("MultiHasher: %v", err)
	}
	if _, err := io.Copy(w, strings.NewReader(data)); err != nil {
		t.Fatalf("Copy: %v", err)
	}

	got := sum()
	if len(got) != 3 {
		t.Fatalf("結果の数 = %d, want 3", len(got))
	}
	// MD5("hello world") の既知の値
	if got[MD5] != "5eb63bbbe01eeed093cb22bb8f5acdc3" {
		t.Errorf("MD5 = %s", got[MD5])
	}
	if got[DropboxContent] != expectedDropboxHash([]byte(data)) {
		t.Errorf("DropboxContent = %s", got[DropboxContent])
	}
}

func TestNewHashUnsupported(t *testing.T) {
	if _, err := NewHash(QuickXor); err == nil {
		t.Error("未実装のハッシュでエラーにならない")
	}
}

func TestCleanPath(t *testing.T) {
	tests := map[string]string{
		"":        "/",
		"/":       "/",
		"a/b":     "/a/b",
		"/a/b":    "/a/b",
		"/a//b":   "/a/b",
		"/a/./b":  "/a/b",
		"/a/../b": "/b",
		"/a/b/":   "/a/b",
		// 逆斜線は区切りではなく、名前の一部として扱う。
		// クラウドストレージではファイル名に使えるふつうの文字なので、
		// 区切りに読み替えると別の場所のファイルになってしまう。
		"a\\b": "/a\\b",
	}
	for in, want := range tests {
		if got := CleanPath(in); got != want {
			t.Errorf("CleanPath(%q) = %q, want %q", in, got, want)
		}
	}
}
