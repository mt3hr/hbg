package cli

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/mt3hr/hbg/internal/hbghome"
)

// 初回起動のときに、置き場所と設定ファイルが用意されることを確かめます。
func TestCreateInitialConfig(t *testing.T) {
	root := t.TempDir()
	t.Setenv(hbghome.EnvHome, root)

	path, err := createInitialConfig()
	if err != nil {
		t.Fatalf("createInitialConfig: %v", err)
	}

	// 置き場所が一式できていること。
	for _, name := range []string{"configs", "tokens", "credentials", "logs", "caches"} {
		dir := filepath.Join(root, name)
		info, err := os.Stat(dir)
		if err != nil {
			t.Errorf("%s が作られていない: %v", name, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("%s がディレクトリでない", name)
		}
	}

	// 設定ファイルができ、そのまま読めること。
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("設定ファイルを読めません: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("設定ファイルが空です")
	}

	cfg := loadConfigFrom(t, string(data))
	if _, err := storageEntries(cfg); err != nil {
		t.Errorf("作った設定を解釈できません: %v", err)
	}
}

// 認証情報を置く場所なので、他の利用者から読めないことを確かめます。
//
// 以前の hbg は設定ファイルを 0777 で作っていました。
func TestInitialConfigIsNotWorldReadable(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows では POSIX の権限を持たないため飛ばします")
	}

	root := t.TempDir()
	t.Setenv(hbghome.EnvHome, root)

	path, err := createInitialConfig()
	if err != nil {
		t.Fatalf("createInitialConfig: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Errorf("設定ファイルの権限 = %04o, 他の利用者から読めてはいけない", perm)
	}

	dirInfo, err := os.Stat(filepath.Join(root, "credentials"))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if perm := dirInfo.Mode().Perm(); perm&0o077 != 0 {
		t.Errorf("認証情報の置き場所の権限 = %04o, 他の利用者から読めてはいけない", perm)
	}
}

// すでにある設定ファイルを上書きしないことを確かめます。
func TestLoadConfigKeepsExistingFile(t *testing.T) {
	root := t.TempDir()
	t.Setenv(hbghome.EnvHome, root)

	if err := hbghome.EnsureLayout(); err != nil {
		t.Fatalf("EnsureLayout: %v", err)
	}
	path, err := hbghome.ConfigFile()
	if err != nil {
		t.Fatalf("ConfigFile: %v", err)
	}

	original := "DefaultWorker: 7\nstorages:\n  - name: mine\n    type: local\n"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	rootOpt.configfile = ""
	t.Cleanup(func() { rootOpt.configfile = "" })

	if err := loadConfig(); err != nil {
		t.Fatalf("loadConfig: %v", err)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(after) != original {
		t.Error("すでにある設定ファイルが書き換えられている")
	}
	if getConfig().DefaultWorker != 7 {
		t.Errorf("DefaultWorker = %d, want 7", getConfig().DefaultWorker)
	}
}

// 初回起動では、作ったことを知らせることを確かめます。
//
// 黙って作ると、利用者は自分が編集すべきファイルの場所を知る手立てが
// ありません。
func TestFirstRunTellsWhereConfigIs(t *testing.T) {
	root := t.TempDir()
	t.Setenv(hbghome.EnvHome, root)

	stderr := captureStderr(t, func() {
		if _, err := createInitialConfig(); err != nil {
			t.Fatalf("createInitialConfig: %v", err)
		}
	})

	if !strings.Contains(stderr, "設定ファイルを作成しました") {
		t.Errorf("作ったことが知らされていない: %q", stderr)
	}
	if !strings.Contains(stderr, filepath.Join(root, "configs")) {
		t.Errorf("どこに作ったのかが分からない: %q", stderr)
	}
}

// captureStderr は標準エラーへの出力を集めます。
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()

	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}

	original := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = original }()

	done := make(chan string, 1)
	go func() {
		var sb strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := r.Read(buf)
			sb.Write(buf[:n])
			if err != nil {
				break
			}
		}
		done <- sb.String()
	}()

	fn()
	_ = w.Close()
	return <-done
}
