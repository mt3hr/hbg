package hbghome

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRoot(t *testing.T) {
	t.Run("HBG_HOME があればそれを使う", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv(EnvHome, dir)

		got, err := Root()
		if err != nil {
			t.Fatalf("Root: %v", err)
		}
		want, _ := filepath.Abs(dir)
		if got != want {
			t.Errorf("Root() = %q, want %q", got, want)
		}
	})

	t.Run("HBG_HOME がなければ $HOME/hbg", func(t *testing.T) {
		t.Setenv(EnvHome, "")

		got, err := Root()
		if err != nil {
			t.Fatalf("Root: %v", err)
		}
		home, err := os.UserHomeDir()
		if err != nil {
			t.Skip("ホームディレクトリを取得できないためスキップ")
		}
		if want := filepath.Join(home, "hbg"); got != want {
			t.Errorf("Root() = %q, want %q", got, want)
		}
	})

	t.Run("相対パスは絶対パスに解決される", func(t *testing.T) {
		t.Setenv(EnvHome, "relative-hbg-home")

		got, err := Root()
		if err != nil {
			t.Fatalf("Root: %v", err)
		}
		if !filepath.IsAbs(got) {
			t.Errorf("Root() = %q は絶対パスではない", got)
		}
	})
}

func TestPaths(t *testing.T) {
	root := t.TempDir()
	t.Setenv(EnvHome, root)

	tests := []struct {
		name string
		fn   func() (string, error)
		want string
	}{
		{"ConfigsDir", ConfigsDir, filepath.Join(root, "configs")},
		{"ConfigFile", ConfigFile, filepath.Join(root, "configs", "config.yaml")},
		{"TokensDir", TokensDir, filepath.Join(root, "tokens")},
		{"CredentialsDir", CredentialsDir, filepath.Join(root, "credentials")},
		{"LogsDir", LogsDir, filepath.Join(root, "logs")},
		{"CachesDir", CachesDir, filepath.Join(root, "caches")},
		{"KnownHostsFile", KnownHostsFile, filepath.Join(root, "configs", "known_hosts")},
		{"ShellHistoryFile", ShellHistoryFile, filepath.Join(root, "shell_history")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.fn()
			if err != nil {
				t.Fatalf("%s: %v", tt.name, err)
			}
			if got != tt.want {
				t.Errorf("%s() = %q, want %q", tt.name, got, tt.want)
			}
		})
	}
}

func TestTokenFile(t *testing.T) {
	root := t.TempDir()
	t.Setenv(EnvHome, root)

	got, err := TokenFile("dropbox", "main")
	if err != nil {
		t.Fatalf("TokenFile: %v", err)
	}
	want := filepath.Join(root, "tokens", "dropbox_main.json")
	if got != want {
		t.Errorf("TokenFile() = %q, want %q", got, want)
	}
}

func TestWriteSecretFile(t *testing.T) {
	root := t.TempDir()
	t.Setenv(EnvHome, root)

	path := filepath.Join(root, "tokens", "nested", "secret.json")
	if err := WriteSecretFile(path, []byte(`{"a":1}`)); err != nil {
		t.Fatalf("WriteSecretFile: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != `{"a":1}` {
		t.Errorf("内容が %q", got)
	}

	// Windows ではパーミッションがほぼ意味を持たないので、Unix でのみ検査する。
	if os.PathSeparator == '/' {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat: %v", err)
		}
		if perm := info.Mode().Perm(); perm != FilePerm {
			t.Errorf("パーミッションが %o、期待は %o", perm, FilePerm)
		}
	}
}

func TestMigrate(t *testing.T) {
	// 移行元はホームディレクトリを見るため、HOME ごと差し替える。
	fakeHome := t.TempDir()
	setFakeHome(t, fakeHome)

	hbgHome := filepath.Join(fakeHome, "hbg")
	t.Setenv(EnvHome, hbgHome)

	// 旧レイアウトのファイルを用意する
	writeTestFile(t, filepath.Join(fakeHome, "hbg_config.yaml"), "DefaultWorker: 2\n")
	writeTestFile(t, filepath.Join(fakeHome, "hbg_token_dropbox_main.json"), `{"access_token":"x"}`)
	writeTestFile(t, filepath.Join(fakeHome, "hbg_token_googledrive_work.json"), `{"access_token":"y"}`)
	// 関係ないファイルは移行しない
	writeTestFile(t, filepath.Join(fakeHome, "unrelated.txt"), "no")

	pending, err := PendingMigrations()
	if err != nil {
		t.Fatalf("PendingMigrations: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("移行対象が %d件、期待は3件: %v", len(pending), pending)
	}

	done, err := Migrate()
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if len(done) != 3 {
		t.Fatalf("移行結果が %d件、期待は3件", len(done))
	}

	// 移行先にあること
	for path, want := range map[string]string{
		filepath.Join(hbgHome, "configs", "config.yaml"):          "DefaultWorker: 2\n",
		filepath.Join(hbgHome, "tokens", "dropbox_main.json"):     `{"access_token":"x"}`,
		filepath.Join(hbgHome, "tokens", "googledrive_work.json"): `{"access_token":"y"}`,
	} {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("移行先にない %s: %v", path, err)
			continue
		}
		if string(got) != want {
			t.Errorf("%s の内容が %q、期待は %q", path, got, want)
		}
	}

	// 移行元は削除されず .migrated にリネームされていること
	for _, name := range []string{"hbg_config.yaml", "hbg_token_dropbox_main.json"} {
		if _, err := os.Stat(filepath.Join(fakeHome, name)); !os.IsNotExist(err) {
			t.Errorf("%s が残っている", name)
		}
		if _, err := os.Stat(filepath.Join(fakeHome, name+migratedSuffix)); err != nil {
			t.Errorf("%s%s が作られていない: %v", name, migratedSuffix, err)
		}
	}

	// 関係ないファイルは触られていないこと
	if _, err := os.Stat(filepath.Join(fakeHome, "unrelated.txt")); err != nil {
		t.Errorf("関係ないファイルが移動されている: %v", err)
	}

	// 2回目は移行対象がないこと
	pending, err = PendingMigrations()
	if err != nil {
		t.Fatalf("PendingMigrations(2回目): %v", err)
	}
	if len(pending) != 0 {
		t.Errorf("2回目の移行対象が %d件、期待は0件: %v", len(pending), pending)
	}
}

func TestMigrateDoesNotOverwrite(t *testing.T) {
	fakeHome := t.TempDir()
	setFakeHome(t, fakeHome)

	hbgHome := filepath.Join(fakeHome, "hbg")
	t.Setenv(EnvHome, hbgHome)

	writeTestFile(t, filepath.Join(fakeHome, "hbg_config.yaml"), "old\n")
	writeTestFile(t, filepath.Join(hbgHome, "configs", "config.yaml"), "new\n")

	done, err := Migrate()
	if err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	var found bool
	for _, m := range done {
		if strings.HasSuffix(m.From, "hbg_config.yaml") {
			found = true
			if !m.Skipped {
				t.Error("移行先にファイルがあるのにスキップされていない")
			}
		}
	}
	if !found {
		t.Fatal("設定ファイルが移行対象に含まれていない")
	}

	got, err := os.ReadFile(filepath.Join(hbgHome, "configs", "config.yaml"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "new\n" {
		t.Errorf("移行先が上書きされている: %q", got)
	}
}

// setFakeHome は os.UserHomeDir が返す値をテスト用に差し替えます。
func setFakeHome(t *testing.T, dir string) {
	t.Helper()
	// os.UserHomeDir は Windows では USERPROFILE、それ以外では HOME を見る。
	t.Setenv("HOME", dir)
	t.Setenv("USERPROFILE", dir)
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
