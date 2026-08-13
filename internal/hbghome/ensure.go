package hbghome

// EnsureLayout は $HOME/hbg 配下のディレクトリを一式作ります。
//
// 初回起動のときに1度だけ効きます。すでにあれば何もしません。
//
// 権限は 0700 です。この下には認証情報を置くので、他の利用者から
// 読めてはいけません。以前の hbg は設定ファイルを 0777 で作っており、
// しかも --help を打つだけで書かれていました。
func EnsureLayout() error {
	dirs, err := layoutDirs()
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		if err := EnsureDir(dir); err != nil {
			return err
		}
	}
	return nil
}

// layoutDirs は作るべきディレクトリを返します。
func layoutDirs() ([]string, error) {
	root, err := Root()
	if err != nil {
		return nil, err
	}

	dirs := []string{root}
	for _, name := range []string{
		configsDirName, tokensDirName, credentialsDirName,
		logsDirName, cachesDirName,
	} {
		dir, err := sub(name)
		if err != nil {
			return nil, err
		}
		dirs = append(dirs, dir)
	}
	return dirs, nil
}
