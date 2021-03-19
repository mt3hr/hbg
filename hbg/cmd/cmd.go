package cmd

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
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
	Dropbox []struct {
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
func createDefaultConfig() (*viper.Viper, error) {
	v := viper.New()

	v.Set("dropbox", []struct {
		Name string
	}{{
		Name: "dropbox",
	}})
	v.Set("local", struct {
		Name string
	}{
		Name: "local",
	})
	v.Set("googledrive", []struct {
		Name string
	}{{
		Name: "googledrive",
	}})

	return v, nil
}

func loadConfig() error {
	configOpt := getConfigFile()
	config := getConfig()
	configName := getConfigName()
	configExt := getConfigExt()
	createDefaultConfig := createDefaultConfig

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
			log.Printf(err.Error())
		} else {
			v.AddConfigPath(filepath.Dir(exe))
			configPaths = append(configPaths, filepath.Dir(exe))
		}

		v.AddConfigPath(".")
		configPaths = append(configPaths, ".")

		home, err := homedir.Dir()
		if err != nil {
			err = fmt.Errorf("error at get user home directory: %w", err)
			log.Printf(err.Error())
		} else {
			v.AddConfigPath(home)
			configPaths = append(configPaths, home)
		}
	}

	// 読み込んでcfgを作成する
	existConfigPath := false
	for _, configPath := range configPaths {
		if _, err := os.Stat(filepath.Join(configPath, configName+configExt)); err == nil {
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
			home, err := homedir.Dir()
			if err != nil {
				err = fmt.Errorf("error at get user home directory: %w", err)
				log.Printf(err.Error())
				configDir = "."
			} else {
				configDir = home
			}

			v, err = createDefaultConfig()
			if err != nil {
				err = fmt.Errorf("error at create defaul config: %w", err)
				return err
			}

			configFileName := filepath.Join(configDir, configName+configExt)
			v.SetConfigFile(configFileName)
			err = v.WriteConfig()
			if err != nil {
				err = fmt.Errorf("error at write config to %s: %w", configFileName, err)
				return err
			}
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
