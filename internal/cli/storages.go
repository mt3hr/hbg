package cli

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/mt3hr/hbg/backend"
	"github.com/mt3hr/hbg/backend/dropbox"
	"github.com/mt3hr/hbg/backend/googledrive"
	"github.com/mt3hr/hbg/backend/local"
	"github.com/spf13/cast"
)

// 設定ファイルには2つの書き方があります。
//
// 新しい書き方は、種別を type で指定してひとつの一覧に並べるものです。
//
//	storages:
//	  - name: local
//	    type: local
//	  - name: nas
//	    type: sftp
//	    host: ...
//
// 古い書き方は、種別ごとに別々の項目を持つものです。
//
//	Local:
//	  name: local
//	Dropbox:
//	  - name: dropbox
//
// 古い書き方では、新しく足したストレージを書き表せません
// （項目そのものが決め打ちなので）。既存の設定をそのまま使えるよう
// 両方を受け付け、混ぜて書くこともできるようにしています。

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
//
// 新しい書き方と古い書き方の両方を集めます。
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

	entries = append(entries, legacyEntries(c)...)
	return entries, nil
}

// legacyEntries は古い書き方の項目を集めます。
func legacyEntries(c *Config) []backend.Entry {
	entries := []backend.Entry{}

	if c.Local.Name != "" {
		entries = append(entries, backend.Entry{
			Name: c.Local.Name,
			Type: local.Type,
		})
	}

	for _, cfg := range c.Dropbox {
		entries = append(entries, backend.Entry{
			Name: cfg.Name,
			Type: dropbox.Type,
			Params: backend.Params{
				"app_key":      os.ExpandEnv(cfg.AppKey),
				"access_token": os.ExpandEnv(cfg.accessToken()),
			},
		})
	}

	for _, cfg := range c.GoogleDrive {
		entries = append(entries, backend.Entry{
			Name: cfg.Name,
			Type: googledrive.Type,
			Params: backend.Params{
				"client_id":      os.ExpandEnv(cfg.ClientID),
				"client_secret":  os.ExpandEnv(cfg.ClientSecret),
				"drive_id":       os.ExpandEnv(cfg.DriveID),
				"root_folder_id": os.ExpandEnv(cfg.RootFolderID),
				"native_files":   cfg.NativeFiles,
			},
		})
	}

	return entries
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
	case dropbox.Type, googledrive.Type:
		return true
	}
	return false
}
