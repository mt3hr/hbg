package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

// loadConfigFrom は文字列の設定を読み込みます。
func loadConfigFrom(t *testing.T, yaml string) *Config {
	t.Helper()

	file := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(file, []byte(yaml), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	v := viper.New()
	v.SetConfigFile(file)
	if err := v.ReadInConfig(); err != nil {
		t.Fatalf("設定を読めません: %v", err)
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		t.Fatalf("設定を解釈できません: %v", err)
	}
	return cfg
}

// 新しい書き方の設定が読めることを確かめます。
func TestStorageEntriesFromList(t *testing.T) {
	t.Setenv("HBG_TEST_PASSWORD", "ひみつ")

	cfg := loadConfigFrom(t, `
storages:
  - name: local
    type: local
  - name: nas
    type: sftp
    host: example.invalid
    user: someone
    port: 2222
    password: ${HBG_TEST_PASSWORD}
    use_agent: true
`)

	entries, err := storageEntries(cfg)
	if err != nil {
		t.Fatalf("storageEntries: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("件数 = %d, want 2", len(entries))
	}

	nas := entries[1]
	if nas.Name != "nas" || nas.Type != "sftp" {
		t.Fatalf("2件目 = %+v", nas)
	}

	tests := map[string]string{
		"host": "example.invalid",
		"user": "someone",
		// 数も真偽値も文字列に揃えて渡す。
		"port":      "2222",
		"use_agent": "true",
		// ${環境変数} は展開する。設定ファイルに秘密を書かずに済ませるため。
		"password": "ひみつ",
	}
	for key, want := range tests {
		if got := nas.Params.Get(key); got != want {
			t.Errorf("params[%s] = %q, want %q", key, got, want)
		}
	}
}

// 書き方の誤りは、接続を試みる前に知らせることを確かめます。
func TestStorageEntriesRejectsIncomplete(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "名前がない",
			yaml: "storages:\n  - type: local\n",
			want: "name",
		},
		{
			name: "種別がない",
			yaml: "storages:\n  - name: どこか\n",
			want: "type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := storageEntries(loadConfigFrom(t, tt.yaml))
			if err == nil {
				t.Fatal("誤りなのに通ってしまった")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("どこが悪いのか分からない: %v", err)
			}
		})
	}
}

// 知らない種別は接続を試みる前に弾かれることを確かめます。
func TestResolverRejectsUnknownType(t *testing.T) {
	cfg := loadConfigFrom(t, "storages:\n  - name: どこか\n    type: しらない\n")

	_, err := resolverFromConfig(cfg)
	if err == nil {
		t.Fatal("知らない種別なのに通ってしまった")
	}
	if !strings.Contains(err.Error(), "しらない") {
		t.Errorf("どの種別が悪いのか分からない: %v", err)
	}
}

// hbg config init が書き出す雛形が、そのまま読めることを確かめます。
//
// 雛形は登録されているバックエンドから組み立てるので、
// 新しい種別を足したときにここで壊れていないか分かります。
func TestDefaultConfigIsLoadable(t *testing.T) {
	cfg := loadConfigFrom(t, defaultConfigYAML())

	if cfg.DefaultWorker == 0 {
		t.Error("DefaultWorker が読めていない")
	}

	entries, err := storageEntries(cfg)
	if err != nil {
		t.Fatalf("雛形を解釈できません: %v", err)
	}

	found := map[string]bool{}
	for _, e := range entries {
		found[e.Type] = true
	}
	for _, typ := range []string{"local", "dropbox", "googledrive"} {
		if !found[typ] {
			t.Errorf("雛形に %s がない", typ)
		}
	}

	if _, err := resolverFromConfig(cfg); err != nil {
		t.Errorf("雛形から解決器を作れません: %v", err)
	}
}
