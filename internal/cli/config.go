package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/mt3hr/hbg/internal/hbghome"
	"github.com/spf13/viper"
)

// errConfigNotFound は設定ファイルが見つからなかったことを表します。
var errConfigNotFound = errors.New("設定ファイルが見つかりません")

// defaultConfigYAML は hbg config init が書き出す設定ファイルの内容です。
//
// ストレージの部分は、登録されているバックエンドから組み立てます。
// 新しい種別を足しても、ここを書き換える必要はありません。
func defaultConfigYAML() string {
	return `# hbg の設定ファイル
#
# ストレージは名前を付けて定義し、コマンドでは "名前:パス" の形式で指定します。
#   hbg copy local:C:/photos dropbox:/backup
#
# 同じ種類のストレージに別々の名前を与えると、複数アカウントを使い分けられます。
# 使わないストレージの項目は削除するかコメントアウトしてください。
# クラウドは初回に hbg auth login <名前> で認証してください。

# 同時処理数。copy の -w で上書きできます。
DefaultWorker: 2

` + defaultConfigFromDescriptors()
}

// findConfigFile は設定ファイルのパスを返します。
// 見つからない場合は errConfigNotFound を返します。
//
// 探すのは $HOME/hbg/configs/config.yaml（HBG_HOME で変えられる）だけです。
// 置き場所が1つなら、読まれたのがどれかを考えずに済みます。
func findConfigFile() (string, error) {
	path, err := hbghome.ConfigFile()
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(path); err != nil {
		return "", errConfigNotFound
	}
	return path, nil
}

// loadConfig は設定ファイルを読み込み、パッケージ変数 config に格納します。
//
// 見つからない場合は、初回起動とみなして雛形を作ります。
//
// 以前も同じことをしていましたが、置き場所がホームディレクトリ直下で、
// 権限が 0777 で、しかも --help を打つだけでも書かれていました。
// いまは $HOME/hbg 配下に 0600 で作り、実際に設定を必要とする
// コマンドのときだけ作ります。作ったことは必ず知らせます。
func loadConfig() error {
	cfg := getConfig()

	configFile := getConfigFile()
	if configFile == "" {
		found, err := findConfigFile()
		if err != nil {
			if !errors.Is(err, errConfigNotFound) {
				return err
			}
			// 初回起動。置き場所と雛形を用意する。
			created, createErr := createInitialConfig()
			if createErr != nil {
				return createErr
			}
			found = created
		}
		configFile = found
	}

	v := viper.New()
	v.SetConfigFile(configFile)

	if err := v.ReadInConfig(); err != nil {
		return fmt.Errorf("設定ファイルを読み込めませんでした %s: %w", configFile, err)
	}
	if err := v.Unmarshal(cfg); err != nil {
		return fmt.Errorf("設定ファイルの内容を解釈できませんでした %s: %w", configFile, err)
	}

	loadedConfigFile = configFile
	return nil
}

// createInitialConfig は初回起動時に置き場所と設定ファイルを用意します。
//
// 作ったことは必ず知らせます。黙って作ると、利用者は自分が編集すべき
// ファイルがどこにあるのか分からないままになります。
func createInitialConfig() (string, error) {
	if err := hbghome.EnsureLayout(); err != nil {
		return "", fmt.Errorf("設定の置き場所を用意できませんでした: %w", err)
	}

	path, err := hbghome.ConfigFile()
	if err != nil {
		return "", err
	}
	if err := hbghome.WriteSecretFile(path, []byte(defaultConfigYAML())); err != nil {
		return "", fmt.Errorf("設定ファイルを作成できませんでした %s: %w", path, err)
	}

	fmt.Fprintf(os.Stderr, "hbg: 設定ファイルを作成しました: %s\n", path)
	fmt.Fprintln(os.Stderr, "     使うストレージに合わせて編集してください。")
	return path, nil
}

// loadedConfigFile は実際に読み込んだ設定ファイルのパスです。
// hbg config path が表示に使います。
var loadedConfigFile string

func mustRoot() string {
	root, err := hbghome.Root()
	if err != nil {
		return "$HOME/hbg"
	}
	return root
}

func mustConfigFile() string {
	p, err := hbghome.ConfigFile()
	if err != nil {
		return "$HOME/hbg/configs/" + hbghome.ConfigFileName
	}
	return p
}
