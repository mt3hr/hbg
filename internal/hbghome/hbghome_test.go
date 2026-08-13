package hbghome

import (
	"os"
	"path/filepath"
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
