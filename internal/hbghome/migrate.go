package hbghome

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// migratedSuffix は移行済みの旧ファイルに付ける接尾辞です。
//
// 旧ファイルは削除せずリネームして残します。
// 移行がうまくいかなかった場合に手で戻せるようにするためです。
const migratedSuffix = ".migrated"

// Migration は 1 件の移行結果です。
type Migration struct {
	From string
	To   string
	// Skipped が真の場合、移行先にすでにファイルがあったため何もしていません。
	Skipped bool
}

func (m Migration) String() string {
	if m.Skipped {
		return fmt.Sprintf("%s は移行しませんでした（移行先にすでにファイルがあります: %s）", m.From, m.To)
	}
	return fmt.Sprintf("%s -> %s", m.From, m.To)
}

// legacyPaths は、旧レイアウトのファイルと移行先の対応を返します。
func legacyPaths() ([]Migration, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		// ホームが分からなければ移行対象も見つけられない。移行なしとして扱う。
		return nil, nil
	}

	configFile, err := ConfigFile()
	if err != nil {
		return nil, err
	}
	tokensDir, err := TokensDir()
	if err != nil {
		return nil, err
	}
	historyFile, err := ShellHistoryFile()
	if err != nil {
		return nil, err
	}

	migrations := []Migration{
		{From: filepath.Join(home, "hbg_config.yaml"), To: configFile},
		{From: filepath.Join(os.TempDir(), "hbg_shell_history"), To: historyFile},
	}

	// トークンは名前が可変なのでホームディレクトリを走査して集める。
	// hbg_token_<type>_<name>.json -> tokens/<type>_<name>.json
	entries, err := os.ReadDir(home)
	if err != nil {
		return migrations, nil
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		rest, ok := strings.CutPrefix(name, "hbg_token_")
		if !ok || !strings.HasSuffix(rest, ".json") {
			continue
		}
		migrations = append(migrations, Migration{
			From: filepath.Join(home, name),
			To:   filepath.Join(tokensDir, rest),
		})
	}

	sort.Slice(migrations, func(i, j int) bool { return migrations[i].From < migrations[j].From })
	return migrations, nil
}

// PendingMigrations は、まだ移行されていない旧レイアウトのファイルを返します。
func PendingMigrations() ([]Migration, error) {
	candidates, err := legacyPaths()
	if err != nil {
		return nil, err
	}

	pending := []Migration{}
	for _, m := range candidates {
		if _, err := os.Stat(m.From); err != nil {
			continue // 旧ファイルがない
		}
		pending = append(pending, m)
	}
	return pending, nil
}

// Migrate は旧レイアウトのファイルを新しい配置へ移します。
//
// 移行先にすでにファイルがある場合は上書きせず、Skipped として報告します。
// 移行元のファイルは削除せず、.migrated を付けてリネームします。
func Migrate() ([]Migration, error) {
	pending, err := PendingMigrations()
	if err != nil {
		return nil, err
	}

	done := make([]Migration, 0, len(pending))
	for _, m := range pending {
		if _, err := os.Stat(m.To); err == nil {
			m.Skipped = true
			done = append(done, m)
			continue
		}

		data, err := os.ReadFile(m.From)
		if err != nil {
			return done, fmt.Errorf("移行元を読み込めませんでした %s: %w", m.From, err)
		}
		if err := WriteSecretFile(m.To, data); err != nil {
			return done, fmt.Errorf("移行先に書き込めませんでした %s: %w", m.To, err)
		}
		if err := os.Rename(m.From, m.From+migratedSuffix); err != nil {
			// リネームに失敗しても移行自体は済んでいる。
			// 次回また移行対象として拾われるが、移行先が既にあるので Skipped になる。
			return done, fmt.Errorf("移行元をリネームできませんでした %s: %w", m.From, err)
		}
		done = append(done, m)
	}
	return done, nil
}
