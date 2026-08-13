package auth

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/mt3hr/hbg/internal/hbghome"
	"golang.org/x/oauth2"
)

func TestFileStore(t *testing.T) {
	root := t.TempDir()
	t.Setenv(hbghome.EnvHome, root)

	store := NewFileStore()

	t.Run("保存前は ErrNoToken", func(t *testing.T) {
		_, err := store.Load("dropbox", "main")
		if !errors.Is(err, ErrNoToken) {
			t.Errorf("err = %v, want ErrNoToken", err)
		}
	})

	t.Run("保存して読み戻せる", func(t *testing.T) {
		want := &oauth2.Token{
			AccessToken:  "access",
			RefreshToken: "refresh",
			TokenType:    "Bearer",
			Expiry:       time.Now().Add(time.Hour).Truncate(time.Second),
		}
		if err := store.Save("dropbox", "main", want); err != nil {
			t.Fatalf("Save: %v", err)
		}

		got, err := store.Load("dropbox", "main")
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if got.AccessToken != want.AccessToken || got.RefreshToken != want.RefreshToken {
			t.Errorf("読み戻した内容が違う: %+v", got)
		}
		if !got.Expiry.Equal(want.Expiry) {
			t.Errorf("Expiry = %v, want %v", got.Expiry, want.Expiry)
		}
	})

	t.Run("$HOME/hbg/tokens 配下に保存される", func(t *testing.T) {
		path := filepath.Join(root, "tokens", "dropbox_main.json")
		if _, err := os.Stat(path); err != nil {
			t.Errorf("期待した場所にない %s: %v", path, err)
		}
	})

	t.Run("名前ごとに分かれる", func(t *testing.T) {
		if err := store.Save("dropbox", "work", &oauth2.Token{AccessToken: "work-token"}); err != nil {
			t.Fatalf("Save: %v", err)
		}
		main, err := store.Load("dropbox", "main")
		if err != nil {
			t.Fatalf("Load main: %v", err)
		}
		if main.AccessToken != "access" {
			t.Errorf("別名の保存が既存を壊している: %q", main.AccessToken)
		}
	})

	t.Run("削除できる", func(t *testing.T) {
		if err := store.Delete("dropbox", "work"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if _, err := store.Load("dropbox", "work"); !errors.Is(err, ErrNoToken) {
			t.Errorf("削除後の Load = %v, want ErrNoToken", err)
		}
		// 無いものを削除しても成功すること
		if err := store.Delete("dropbox", "work"); err != nil {
			t.Errorf("2回目の Delete: %v", err)
		}
	})

	t.Run("中身が空のトークンは無いものとして扱う", func(t *testing.T) {
		if err := store.Save("dropbox", "empty", &oauth2.Token{}); err != nil {
			t.Fatalf("Save: %v", err)
		}
		if _, err := store.Load("dropbox", "empty"); !errors.Is(err, ErrNoToken) {
			t.Errorf("err = %v, want ErrNoToken", err)
		}
	})
}

// countingSource は Token が呼ばれた回数を数える TokenSource です。
type countingSource struct {
	mu    sync.Mutex
	calls int
	tok   *oauth2.Token
}

func (c *countingSource) Token() (*oauth2.Token, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return c.tok, nil
}

func TestPersistingTokenSource(t *testing.T) {
	root := t.TempDir()
	t.Setenv(hbghome.EnvHome, root)
	store := NewFileStore()

	t.Run("更新されたトークンが保存される", func(t *testing.T) {
		// 期限切れのトークンから始めると、oauth2 は更新を要求する。
		expired := &oauth2.Token{
			AccessToken: "old",
			Expiry:      time.Now().Add(-time.Hour),
		}
		refreshed := &oauth2.Token{
			AccessToken:  "new",
			RefreshToken: "refresh",
			Expiry:       time.Now().Add(time.Hour),
		}
		src := &countingSource{tok: refreshed}

		ts := PersistingTokenSource(src, store, "googledrive", "main", expired)

		got, err := ts.Token()
		if err != nil {
			t.Fatalf("Token: %v", err)
		}
		if got.AccessToken != "new" {
			t.Errorf("AccessToken = %q, want new", got.AccessToken)
		}

		saved, err := store.Load("googledrive", "main")
		if err != nil {
			t.Fatalf("更新後のトークンが保存されていない: %v", err)
		}
		if saved.AccessToken != "new" || saved.RefreshToken != "refresh" {
			t.Errorf("保存された内容が違う: %+v", saved)
		}
	})

	t.Run("有効なトークンでは更新も保存もしない", func(t *testing.T) {
		valid := &oauth2.Token{
			AccessToken: "valid",
			Expiry:      time.Now().Add(time.Hour),
		}
		src := &countingSource{tok: &oauth2.Token{AccessToken: "should-not-be-used"}}

		ts := PersistingTokenSource(src, store, "googledrive", "unused", valid)
		for i := 0; i < 5; i++ {
			got, err := ts.Token()
			if err != nil {
				t.Fatalf("Token: %v", err)
			}
			if got.AccessToken != "valid" {
				t.Errorf("AccessToken = %q, want valid", got.AccessToken)
			}
		}

		if src.calls != 0 {
			t.Errorf("有効なトークンなのに %d 回更新された", src.calls)
		}
		// 保存もされない
		if _, err := store.Load("googledrive", "unused"); !errors.Is(err, ErrNoToken) {
			t.Errorf("更新していないのに保存されている")
		}
	})
}
