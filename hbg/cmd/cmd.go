package cmd

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		log.Fatal(err)
	}
}

type Cfg struct {
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
	FTP []struct {
		Name     string
		Address  string
		UserName string
		Password string
	}
}

var (
	rootCmd = &cobra.Command{}

	rootOpt = &struct {
		configFile string
	}{}

	cfg = &Cfg{}
)

func init() {
	rootCmd.AddCommand(copyCmd)

	rootPf := rootCmd.PersistentFlags()
	rootPf.StringVar(&rootOpt.configFile, "config_file", "", "コンフィグファイル")

	rootCmd.PersistentPreRun = func(_ *cobra.Command, _ []string) {
		err := loadConfig()
		if err != nil {
			err = fmt.Errorf("failed to load config file: %w", err)
			log.Fatal(err)
		}
	}
}

// コンフィグファイルを読み込みます。
// コマンドラインオプション（viperとBindされているはず）から指定されていればそこから、
// そうでなければ、実行ファイルの親ディレクトリ、カレントディレクトリ、ホームディレクトリの順に、
// configFileNameなファイルを探索して読み込みます。
func loadConfig() error {
	///////////////////////////////////////////////////////////////
	// ここから
	///////////////////////////////////////////////////////////////
	configOpt := rootOpt.configFile
	config := cfg
	configName := "hbg_config"
	configExt := ".yaml"
	createDefaultConfig := func() *viper.Viper {
		v := viper.New()

		v.Set("dropbox", []struct {
			Name  string
			Token string
		}{{
			Name:  "dropbox",
			Token: "",
		}})
		v.Set("local", struct {
			Name string
		}{
			Name: "local",
		})
		v.Set("ftp", []struct {
			Name     string
			Address  string
			Username string
			Password string
		}{})
		v.Set("googledrive", []struct {
			Name string
		}{})

		return v
	}
	///////////////////////////////////////////////////////////////
	// ここまで
	///////////////////////////////////////////////////////////////

	v := viper.New()
	if configOpt != "" {
		// コンフィグファイルが明示的に指定された場合はそれを
		v.SetConfigFile(configOpt)
	} else {
		// 実行ファイルの親ディレクトリ、カレントディレクトリ、ホームディレクトリの順に
		v.SetConfigName(configName)
		exe, err := os.Executable()
		if err != nil {
			err = fmt.Errorf("failed to get executable file path: %w", err)
			log.Printf(err.Error())
		} else {
			v.AddConfigPath(filepath.Dir(exe))
		}

		v.AddConfigPath(".")

		home, err := homedir.Dir()
		if err != nil {
			err = fmt.Errorf("failed to get user home directory: %w", err)
			log.Printf(err.Error())
		} else {
			v.AddConfigPath(home)
		}
	}

	// 読み込んでcfgを作成する
	err := v.ReadInConfig()
	if err != nil {
		// コンフィグファイルが存在しない場合はホームディレクトリに作成する
		// なければカレントディレクトリ
		configDir := ""
		home, err := homedir.Dir()
		if err != nil {
			err = fmt.Errorf("failed to get user home directory: %w", err)
			log.Printf(err.Error())
			configDir = "."
		} else {
			configDir = home
		}

		v = createDefaultConfig()
		configFileName := filepath.Join(configDir, configName+configExt)
		v.SetConfigFile(configFileName)
		err = v.WriteConfig()
		if err != nil {
			err = fmt.Errorf("failed to write config to %s: %w", configFileName, err)
			return err
		}
	}

	err = v.Unmarshal(config)
	if err != nil {
		err = fmt.Errorf("failed unmarshal config file: %w", err)
		return err
	}
	return nil
}
