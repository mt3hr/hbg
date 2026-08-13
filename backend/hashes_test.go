package backend_test

import (
	"testing"

	"github.com/mt3hr/hbg/backend/dropbox"
	"github.com/mt3hr/hbg/backend/googledrive"
	"github.com/mt3hr/hbg/backend/local"
	"github.com/mt3hr/hbg/storage"
)

// --checksum が使える組み合わせを固定します。
//
// 共通して使えるハッシュがない組み合わせでは、hbg は黙ってサイズ比較に
// 落とさず起動時にエラーにします。どの組み合わせで内容の比較ができるかは
// 利用者に約束していることなので、ここで表として押さえておきます。
func TestCommonHashBetweenBackends(t *testing.T) {
	// Features はどれも接続を必要としないので、空の値から取れる。
	hashes := map[string]storage.HashSet{
		"local":       local.New("local").Features().Hashes,
		"dropbox":     (&dropbox.Storage{}).Features().Hashes,
		"googledrive": (&googledrive.Storage{}).Features().Hashes,
	}

	tests := []struct {
		src, dst string
		want     storage.HashType
		found    bool
	}{
		// ローカルは dropbox 形式のハッシュも計算できる。
		{"local", "dropbox", storage.DropboxContent, true},
		{"dropbox", "local", storage.DropboxContent, true},
		{"local", "googledrive", storage.SHA256, true},
		{"googledrive", "local", storage.SHA256, true},
		{"local", "local", storage.SHA256, true},
		// Dropbox は独自形式、Drive は sha/md5 で、重なるものがない。
		{"dropbox", "googledrive", "", false},
		{"googledrive", "dropbox", "", false},
	}

	for _, tt := range tests {
		got, ok := storage.CommonHash(hashes[tt.src], hashes[tt.dst])
		if ok != tt.found {
			t.Errorf("%s → %s: 共通のハッシュの有無 = %v, want %v", tt.src, tt.dst, ok, tt.found)
			continue
		}
		if ok && got != tt.want {
			t.Errorf("%s → %s: 選ばれたハッシュ = %s, want %s", tt.src, tt.dst, got, tt.want)
		}
	}
}
