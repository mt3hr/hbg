package cmd

import (
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
		println(err.Error())
	}
}

var (
	rootCmd = &cobra.Command{
		Long: `dropboxを使う場合、${HOME}/hbg_config.yamlのdropboxtokenを設定してください。`,
	}

	rootOpt = &struct {
		configFile string
	}{}

	cfg = &struct {
		DropboxToken string
	}{}
)

func init() {
	rootCmd.AddCommand(copyCmd)

	rootPf := rootCmd.PersistentFlags()
	rootPf.StringVar(&rootOpt.configFile, "config_file", "", "コンフィグファイル")

	rootCmd.PersistentPreRun = func(_ *cobra.Command, _ []string) {
		err := loadConfig()
		if err != nil {
			log.Fatal(err)
		}
	}
}

// コンフィグファイルを読み込みます。
// コマンドラインオプション（viperとBindされているはず）から指定されていればそこから、
// そうでなければ、実行ファイルの親ディレクトリ、カレントディレクトリ、ホームディレクトリの順に、
// configFileNameなファイルを探索して読み込みます。
func loadConfig() error {
	v := viper.New()
	if rootOpt.configFile != "" {
		// コンフィグファイルが明示的に指定された場合はそれを
		v.SetConfigFile(rootOpt.configFile)
	} else {
		// 実行ファイルの親ディレクトリ、カレントディレクトリ、ホームディレクトリの順に
		v.SetConfigName("hbg_config")
		exe, err := os.Executable()
		if err != nil {
			log.Printf("error: err = %+v\n", err)
		} else {
			v.AddConfigPath(filepath.Dir(exe))
		}

		v.AddConfigPath(".")

		home, err := homedir.Dir()
		if err != nil {
			log.Printf("error: err = %+v\n", err)
		} else {
			v.AddConfigPath(home)
		}
	}

	// 読み込んでcfgを作成する
	err := v.ReadInConfig()
	if err != nil {
		// コンフィグファイルが存在しない場合はホームディレクトリに作成する.
		home, err := homedir.Dir()
		if err != nil {
			log.Printf("error: err = %+v\n", err)
		} else {
			v.AddConfigPath(home)
		}

		v = viper.New()
		v.Set("dropboxtoken", "")
		v.SetConfigFile(filepath.Join(home, "hbg_config.yaml"))
		err = v.WriteConfig()
		if err != nil {
			panic(err)
		}
	}

	err = v.Unmarshal(cfg)
	if err != nil {
		return err
	}
	return nil
}
