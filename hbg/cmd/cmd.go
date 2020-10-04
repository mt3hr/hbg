package cmd

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"bitbucket.org/mt3hr/hbg"
	"github.com/jlaffaye/ftp"
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
	rootCmd = &cobra.Command{
		PersistentPreRun: func(_ *cobra.Command, _ []string) {
			err := loadConfig()
			if err != nil {
				err = fmt.Errorf("failed to load config file: %w", err)
				log.Fatal(err)
			}
		},
	}

	rootOpt = &struct {
		configFile string
	}{}

	cfg = &Cfg{}
)

func init() {
	rootCmd.AddCommand(copyCmd)
	rootCmd.AddCommand(removeCmd)
	rootCmd.AddCommand(listCmd)

	rootPf := rootCmd.PersistentFlags()
	rootPf.StringVar(&rootOpt.configFile, "config_file", "", "コンフィグファイル")
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

func storageMapFromConfig(c *Cfg) (map[string]hbg.Storage, error) {
	storages := map[string]hbg.Storage{}

	// localの読み込み
	storages[c.Local.Name] = &hbg.LocalFileSystem{}

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

	// ftpの読み込み
	// プログラム終了時まで閉じられることがない問題
	for _, ftpCfg := range c.FTP {
		conn, err := ftp.Connect(ftpCfg.Address)
		if err != nil {
			err = fmt.Errorf("failed to connect to ftp server %s: %w", ftpCfg.Address, err)
			return nil, err
		}
		if ftpCfg.UserName != "" || ftpCfg.Password != "" {
			conn.Login(ftpCfg.UserName, ftpCfg.Password)
		}

		ftp := &hbg.FTP{Conn: conn}
		_, exist := storages[ftpCfg.Name]
		if exist {
			err := fmt.Errorf("confrict name of ftp storage '%s'", ftpCfg.Name)
			return nil, err
		}
		storages[ftpCfg.Name] = ftp
	}

	return storages, nil
}
