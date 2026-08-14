//go:build windows

package local

import "os"

// clearReadOnly は置き換え先の読み取り専用属性を落とします。
//
// Windows の os.Rename は MOVEFILE_REPLACE_EXISTING なので、
// 置き換え先が読み取り専用だと Access is denied で失敗します。
// 消すときは os.Remove が自分で属性を落としてから消しているのに、
// 置き換えるときだけ失敗するのは辻褄が合わないので、そちらに合わせます。
//
// os.Chmod は Windows では読み取り専用属性の上げ下げしか行わないため、
// 書き込みできるファイルの権限まで変えてしまう心配はありません。
//
// 属性を戻すことはしません。hbg が運ぶのは中身と更新時刻だけで、
// 新しく作ったファイルにも属性は付かないため、
// 置き換えたときだけ属性が残るほうが揃いません。
func clearReadOnly(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		// 置き換え先が無いなら落とすものもない
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	perm := info.Mode().Perm()
	if info.IsDir() || perm&0o200 != 0 {
		return nil
	}
	return os.Chmod(path, perm|0o200)
}
