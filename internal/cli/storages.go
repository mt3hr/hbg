package cli

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/backend/dropbox"
	"github.com/mt3hr/hbg/backend/googledrive"
	"github.com/mt3hr/hbg/backend/onedrive"
	"github.com/spf13/cast"
)

// ストレージは、種別を type で指定してひとつの一覧に並べて書きます。
//
//	storages:
//	  - name: local
//	    type: local
//	  - name: nas
//	    type: sftp
//	    host: ...
//
// 種別ごとの項目は type と name 以外そのまま Params へ入るので、
// 新しいストレージを足してもここは変わりません。

// StorageEntry は storages: の1件です。
type StorageEntry struct {
	// Name はコマンドで "名前:パス" として指定する名前です。
	Name string
	// Type はストレージの種別です。
	Type string
	// Params は name と type 以外の指定をまとめたものです。
	// 種別ごとに何が使えるかは hbg config init が書き出す雛形を見てください。
	Params map[string]any `mapstructure:",remain"`
}

// params は種別ごとの設定を文字列に揃えて返します。
//
// ${環境変数} は展開します。秘密情報を設定ファイルに直接書かずに済むよう、
// どの項目でも使えるようにしています。
func (e StorageEntry) params() backend.Params {
	p := backend.Params{}
	for k, v := range e.Params {
		p.Set(k, os.ExpandEnv(cast.ToString(v)))
	}
	return p
}

// storageEntries は設定に書かれたすべてのストレージを返します。
func storageEntries(c *Config) ([]backend.Entry, error) {
	entries := []backend.Entry{}

	for i, e := range c.Storages {
		switch {
		case e.Name == "":
			return nil, fmt.Errorf("storages の %d番目に name がありません", i+1)
		case e.Type == "":
			return nil, fmt.Errorf("ストレージ %q に type がありません（使えるのは %s）",
				e.Name, strings.Join(backend.Types(), ", "))
		}
		entries = append(entries, backend.Entry{
			Name:   e.Name,
			Type:   e.Type,
			Params: e.params(),
		})
	}

	return entries, nil
}

// findStorageEntry は名前からストレージの設定を探します。
func findStorageEntry(c *Config, name string) (backend.Entry, bool) {
	entries, err := storageEntries(c)
	if err != nil {
		return backend.Entry{}, false
	}
	for _, e := range entries {
		if e.Name == name {
			return e, true
		}
	}
	return backend.Entry{}, false
}

// authRequiredEntries は認証が必要なストレージを名前順に返します。
func authRequiredEntries(c *Config) []backend.Entry {
	entries, err := storageEntries(c)
	if err != nil {
		return nil
	}

	out := []backend.Entry{}
	for _, e := range entries {
		if needsAuth(e.Type) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// needsAuth は、その種別が hbg auth login を必要とするかを返します。
func needsAuth(storageType string) bool {
	switch storageType {
	case dropbox.Type, googledrive.Type, onedrive.Type:
		return true
	}
	return false
}
