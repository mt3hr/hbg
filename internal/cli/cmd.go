package cli

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/mt3hr/hbg"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Execute .
// コマンドを実行します。main関数から呼び出されます。
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}

// Config .
// コンフィグファイルのデータモデル
type Config struct {
	DefaultWorker int
	Dropbox       []struct {
		Name  string
		Token string
	}
	GoogleDrive []struct {
		Name string
	}
	Local struct {
		Name string
	}
}

var (
	rootCmd = &cobra.Command{
		Use:   "hbg",
		Short: "ローカルとクラウドストレージの間でファイルをコピー・同期する",
		Long: `hbg はローカルファイルシステムとクラウドストレージの間で
ファイルをコピー・同期するためのコマンドラインツールです。

対応しているストレージ:
  local        ローカルファイルシステム
  dropbox      Dropbox
  googledrive  Google Drive

ストレージは設定ファイル hbg_config.yaml で名前を付けて定義し、
コマンドでは "名前:パス" の形式で指定します。`,
		SilenceUsage: true,
		PersistentPreRun: func(_ *cobra.Command, _ []string) {
			err := loadConfig()
			if err != nil {
				err = fmt.Errorf("error at load config file: %w", err)
				log.Fatal(err)
			}
		},
		Run: func(_ *cobra.Command, _ []string) {},
	}

	rootOpt = &struct {
		configfile string
	}{}

	config = &Config{}
)

func init() {
	rootCmd.AddCommand(copyCmd)
	rootCmd.AddCommand(removeCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(shellCmd)

	rootPf := rootCmd.PersistentFlags()
	rootPf.StringVar(&rootOpt.configfile, "config_file", "", "コンフィグファイル")
}

func storageMapFromConfig(c *Config) (map[string]hbg.Storage, error) {
	storages := map[string]hbg.Storage{}

	// localの読み込み
	storages[c.Local.Name] = hbg.NewLocalFileSystem(c.Local.Name)

	// dropboxの読み込み
	for _, dbxCfg := range c.Dropbox {
		dropbox, err := hbg.NewDropbox(dbxCfg.Name)
		if err != nil {
			err = fmt.Errorf("failed load dropbox %s. %w", dbxCfg.Name, err)
			return nil, err
		}
		_, exist := storages[dbxCfg.Name]
		if exist {
			err := fmt.Errorf("confrict name of dropbox storage '%s'", dbxCfg.Name)
			return nil, err
		}
		storages[dbxCfg.Name] = dropbox
	}

	// googledriveの読み込み
	for _, gdvCfg := range c.GoogleDrive {
		googleDrive, err := hbg.NewGoogleDrive(gdvCfg.Name)
		if err != nil {
			err = fmt.Errorf("failed load google drive %s. %w", gdvCfg.Name, err)
			return nil, err
		}
		_, exist := storages[gdvCfg.Name]
		if exist {
			err := fmt.Errorf("confrict name of google drive storage '%s'", gdvCfg.Name)
			return nil, err
		}
		storages[gdvCfg.Name] = googleDrive
	}
	return storages, nil
}

func getConfigFile() string {
	return rootOpt.configfile
}
func getConfig() *Config {
	return config
}
func getConfigName() string {
	return "hbg_config"
}
func getConfigExt() string {
	return ".yaml"
}
func createDefaultConfigYAML() string {
	return `DefaultWorker: 2
Dropbox:
- name: dropbox
# Googledrive:
# - name: googledrive
Local:
  name: local
`
}

func loadConfig() error {
	configOpt := getConfigFile()
	config := getConfig()
	configName := getConfigName()
	configExt := getConfigExt()

	v := viper.New()
	configPaths := []string{}
	if configOpt != "" {
		// コンフィグファイルが明示的に指定された場合はそれを
		v.SetConfigFile(configOpt)
		configPaths = append(configPaths, configOpt)
	} else {
		// 実行ファイルの親ディレクトリ、カレントディレクトリ、ホームディレクトリの順に
		v.SetConfigName(configName)
		exe, err := os.Executable()
		if err != nil {
			err = fmt.Errorf("error at get executable file path: %w", err)
			log.Print(err)
		} else {
			v.AddConfigPath(filepath.Dir(exe))
			configPaths = append(configPaths, filepath.Join(filepath.Dir(exe), configName+configExt))
		}

		v.AddConfigPath(".")
		configPaths = append(configPaths, filepath.Join(".", configName+configExt))

		home, err := os.UserHomeDir()
		if err != nil {
			err = fmt.Errorf("error at get user home directory: %w", err)
			log.Print(err)
		} else {
			v.AddConfigPath(home)
			configPaths = append(configPaths, filepath.Join(home, configName+configExt))
		}
	}

	// 読み込んでcfgを作成する
	existConfigPath := false
	for _, configPath := range configPaths {
		if _, err := os.Stat(configPath); err == nil {
			existConfigPath = true
			break
		}
	}
	if !existConfigPath {
		// コンフィグファイルが指定されていなくてコンフィグファイルが見つからなかった場合、
		// ホームディレクトリにデフォルトコンフィグファイルを作成する。
		// できなければカレントディレクトリにコンフィグファイルを作成する。
		if configOpt == "" {
			configDir := ""
			home, err := os.UserHomeDir()
			if err != nil {
				err = fmt.Errorf("error at get user home directory: %w", err)
				log.Print(err)
				configDir = "."
			} else {
				configDir = home
			}

			configFileName := filepath.Join(configDir, configName+configExt)
			err = os.WriteFile(configFileName, []byte(createDefaultConfigYAML()), os.ModePerm)
			if err != nil {
				err = fmt.Errorf("error at write file to %s: %w", configFileName, err)
				return err
			}
			v.SetConfigFile(configFileName)
		} else {
			err := fmt.Errorf("コンフィグファイルが見つかりませんでした。")
			return err
		}
	}

	err := v.ReadInConfig()
	if err != nil {
		err = fmt.Errorf("error at read in config: %w", err)
		return err
	}

	err = v.Unmarshal(config)
	if err != nil {
		err = fmt.Errorf("error at unmarshal config file: %w", err)
		return err
	}
	return nil
}
