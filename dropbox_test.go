package hbg

import (
	"bufio"
	"log"
	"os"
	"testing"

	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox"
	dbx "github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
)

func getClient() dbx.Client {
	key := "dbxtoken"
	token := os.Getenv(key)
	if token == "" {
		log.Fatalf("error: 環境変数%sにdropboxのtokenを収めてから実行してください。", key)
	}
	return dbx.New(dropbox.Config{Token: token})
}

func TestDropboxList(t *testing.T) {
	var d Storage = &Dropbox{getClient()}

	paths, err := d.List("/")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("info: list root dir at dropbox.")
	for _, path := range paths {
		t.Logf("info: path.Name = %+v\n", path.Name)
	}
}

// 実行ファイルをアップロードした後にダウンロードし、[]byteが同値であるかを検証します。
// アップロードファイルを変更したいときはlocalFromPathの値をいじってください。
func TestDropboxPushGetAndTime(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	_ = exe

	localFromPath := exe                    // アップロードするローカルのファイル
	dropboxToPath := "/hbgTest/hbgTest.bin" //アップロード先

	var l Storage = &LocalFilesystem{}
	var d Storage = &Dropbox{getClient()}

	t.Logf("info: dropboxToPath = %+v\n", dropboxToPath)
	t.Logf("info: localFromPath = %+v\n", localFromPath)

	// Push.
	lfile, err := l.Get(localFromPath)
	if err != nil {
		t.Fatal(err)
	}
	err = d.Push(dropboxToPath, lfile)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("info: pass push to dropbox.")

	// Get。lfileもGetし直す。Closeされているので
	dfile, err := d.Get(dropboxToPath)
	if err != nil {
		t.Fatal(err)
	}
	lfile, err = l.Get(localFromPath)
	if err != nil {
		t.Fatal(err)
	}

	// データの検証。
	dr := bufio.NewReader(dfile.Data)
	lr := bufio.NewReader(lfile.Data)
	defer deht(func() error { return lfile.Data.Close() }, &err, t)
	defer deht(func() error { return dfile.Data.Close() }, &err, t)
	for {
		db, derr := dr.ReadByte()
		lb, lerr := lr.ReadByte()

		if derr != nil && lerr != nil {
			break
		}
		if db != lb || (derr == nil && lerr != nil) || (derr != nil && lerr == nil) {
			t.Fatal("error: 元ファイルとアップロードされたファイルのデータが異なります。")
		}
	}
	t.Log("info: pass data equality")

	// timeの検証。
	dtime := dfile.ModTime
	ltime := TimeToDropbox(lfile.ModTime)
	if !dtime.Equal(ltime) {
		t.Fatalf("時刻が違うみたいです。 ltime=%s dtime=%s", ltime, dtime)
	}
	t.Log("info: pass time equality")
}

// test用のdehです。
func deht(f func() error, err *error, t *testing.T) {
	deh(f, err)
	if *err != nil {
		t.Fatal(*err)
	}
}
