package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/mt3hr/hbg/internal/hbghome"
	"golang.org/x/oauth2"
)

// ErrNoToken は、保存されたトークンが見つからないことを表します。
var ErrNoToken = errors.New("保存されたトークンがありません")

// Store は OAuth2 トークンの保存先です。
type Store interface {
	// Load は保存されたトークンを返します。
	// 無い場合は ErrNoToken を包んだエラーを返します。
	Load(storageType, name string) (*oauth2.Token, error)
	// Save はトークンを保存します。
	Save(storageType, name string, tok *oauth2.Token) error
	// Delete は保存されたトークンを削除します。無い場合も成功とします。
	Delete(storageType, name string) error
	// Path は保存先を人間向けに示す文字列を返します。
	Path(storageType, name string) string
}

// FileStore は $HOME/hbg/tokens 配下にトークンを保存します。
type FileStore struct{}

// NewFileStore は既定のトークン保存先を使う Store を返します。
func NewFileStore() *FileStore { return &FileStore{} }

func (s *FileStore) file(storageType, name string) (string, error) {
	return hbghome.TokenFile(storageType, name)
}

// Path は保存先のパスを返します。解決できない場合は空文字を返します。
func (s *FileStore) Path(storageType, name string) string {
	path, err := s.file(storageType, name)
	if err != nil {
		return ""
	}
	return path
}

// Load は保存されたトークンを読み込みます。
func (s *FileStore) Load(storageType, name string) (*oauth2.Token, error) {
	path, err := s.file(storageType, name)
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrNoToken, path)
		}
		return nil, fmt.Errorf("トークンを読み込めませんでした %s: %w", path, err)
	}

	tok := &oauth2.Token{}
	if err := json.Unmarshal(data, tok); err != nil {
		return nil, fmt.Errorf("トークンの内容を解釈できませんでした %s: %w", path, err)
	}
	if tok.AccessToken == "" && tok.RefreshToken == "" {
		return nil, fmt.Errorf("%w: %s は空です", ErrNoToken, path)
	}
	return tok, nil
}

// Save はトークンを保存します。所有者だけが読める権限で書きます。
func (s *FileStore) Save(storageType, name string, tok *oauth2.Token) error {
	path, err := s.file(storageType, name)
	if err != nil {
		return err
	}

	data, err := json.MarshalIndent(tok, "", "  ")
	if err != nil {
		return fmt.Errorf("トークンを書き出せませんでした: %w", err)
	}
	return hbghome.WriteSecretFile(path, data)
}

// Delete は保存されたトークンを削除します。
func (s *FileStore) Delete(storageType, name string) error {
	path, err := s.file(storageType, name)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("トークンを削除できませんでした %s: %w", path, err)
	}
	return nil
}

// persistingSource は、更新されたトークンを保存先に書き戻す TokenSource です。
//
// oauth2 のクライアントは期限切れのアクセストークンをリフレッシュトークンで
// 自動更新しますが、更新結果はメモリ上にしか残りません。書き戻さないと
// 次回の起動時にまた古いトークンから始めることになります。
type persistingSource struct {
	src         oauth2.TokenSource
	store       Store
	storageType string
	name        string

	mu   sync.Mutex
	last string
}

func (p *persistingSource) Token() (*oauth2.Token, error) {
	tok, err := p.src.Token()
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	sig := tok.AccessToken + "\x00" + tok.RefreshToken
	if sig == p.last {
		return tok, nil
	}
	if err := p.store.Save(p.storageType, p.name, tok); err != nil {
		// 保存に失敗しても、取得したトークン自体は使える。
		// 転送を止めるほどの問題ではないので警告にとどめる。
		fmt.Fprintf(os.Stderr, "hbg: 警告: トークンを保存できませんでした: %v\n", err)
		return tok, nil
	}
	p.last = sig
	return tok, nil
}

// PersistingTokenSource は、トークンの自動更新と保存を行う TokenSource を返します。
//
// oauth2.ReuseTokenSource の内側に置くので、毎回の API 呼び出しで
// 保存処理が走ることはありません。実際に更新されたときだけ書き戻します。
func PersistingTokenSource(src oauth2.TokenSource, store Store, storageType, name string, initial *oauth2.Token) oauth2.TokenSource {
	p := &persistingSource{
		src:         src,
		store:       store,
		storageType: storageType,
		name:        name,
	}
	if initial != nil {
		p.last = initial.AccessToken + "\x00" + initial.RefreshToken
	}
	return oauth2.ReuseTokenSource(initial, p)
}
