# 試験

## 方針

**docker も認証情報も要りません。**

```console
go test ./...
```

これでクラウドを含む9種類すべての適合性スイートが走ります。
クラウドのバックエンドは、それぞれ偽のサーバーを立てて試験しています。

外部の用意が必要な試験は、走らせられないぶん**書かれなくなります**。
書かれない試験は、直したはずの不具合が戻ってきても教えてくれません。

## 適合性スイート

`storage/storagetest` にあります。新しいバックエンドはこれを通すのが
受け入れの条件です。

```go
func TestConformance(t *testing.T) {
	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			// ストレージと、書き込んでよい空のディレクトリを返す
		},
	})
}
```

### 試験の内容

| 試験群 | 何を確かめるか |
| --- | --- |
| 基本操作 | Mkdir → Stat → List → Put → Open → Remove |
| サイズ不明でも全部書き込まれる | `SizeUnknown` で Put しても全バイト書かれる（切り詰めの回帰） |
| 空のファイル | 0バイトが 0バイトとして往復する |
| 上書き | すでにあるものを置き換えられる |
| 存在しないものへの操作 | `ErrNotFound` が返り、`ClassPermanent` に分類される |
| ディレクトリ | 冪等な Mkdir、空ディレクトリの Remove |
| 入れ子のディレクトリ | 各階層が見える |
| 更新時刻の保持 | 分解能の範囲で保たれる |
| 変わった名前のファイル | 日本語・空白・記号・絵文字・200文字 |
| Listの打ち切り | コールバックがエラーを返したら即座に止まる |
| 件数の多いディレクトリ | ページ分割が漏れない（Drive の欠落の回帰） |
| 取り消し | 取り消し済み ctx で全メソッドが `context.Canceled` |
| 書き込み中の取り消しで壊れたものが残らない | 部分ファイルが残らない |
| 並行して書き込む | `-race` で問題が出ない |
| Featuresとの整合 | 申告と実装が食い違っていない |
| ハッシュ / 範囲読み出し / 移動 / まとめて削除 | オプショナルな能力 |
| 大きめのファイル | 分割送信の経路 |

できないことは `Features` を見て自動的に飛ばされます。

### `Harness` の調整

```go
storagetest.Harness{
    NewStorage:       ...,
    LargeDirCount:    120,   // 件数の多いディレクトリで作る数
    SkipLargeDirs:    false, // 実物に対して走らせる場合
    IllegalNameChars: `\`,   // 試験の環境が扱えない文字
}
```

`IllegalNameChars` は**試験の環境**の都合で使います。ストレージ自身の
制限は `Features.IllegalChars` で表してください。両者を混ぜると、
「そのストレージが本当にできないこと」が分からなくなります。

例: SFTP の試験用サーバーを Windows で動かすと、名前の中の逆斜線が
区切りとして解釈されます。実物の SFTP サーバーは POSIX なので問題ありません。

## 偽サーバー

クラウドのバックエンドは、それぞれ試験用のサーバーを持ちます。

| バックエンド | 仕掛け |
| --- | --- |
| dropbox | SDK が試験用に公開している `Config.URLGenerator` を httptest へ向ける |
| googledrive | `option.WithEndpoint` + `WithoutAuthentication` |
| onedrive | 自前の REST なので、`baseOverride` で入口を差し替える |
| s3 | `BaseEndpoint` + `UsePathStyle` で httptest へ向ける |
| sftp | `pkg/sftp` のサーバー実装をその場に立てる（**本物の手続き**） |
| webdav | `golang.org/x/net/webdav` のサーバー実装（**本物の手続き**） |
| ftp | `fclairamb/ftpserverlib`（**本物の手続き**） |
| smb | ファイル操作をインターフェースに切り出し、ローカルの FS で代用 |

### 偽サーバーに持たせている性質

**実物の厄介なところを再現します。** そうしないと、そこを試験できません。

- Dropbox / Drive / S3 / OneDrive: 1ページ3件しか返さない。
  どんなに小さいディレクトリでも続きの取得を必ず通る
- S3: 分割送信の ETag を実物と同じ形（各分割の MD5 を連ねたものの MD5 に
  分割数を添えた形）で返す。これがないと「分割送信では MD5 を取得できない」
  という振る舞いを試験できない
- OneDrive: 分割の大きさが 320KiB の倍数であることを確かめる。
  間違えれば試験が落ちる
- OneDrive: 分割送信の送り先に認証の情報が付いていないことを確かめる
- WebDAV: `X-OC-Mtime` を実装する（`x/net/webdav` にはない）。
  preset ごとの振る舞いの違いを試験できる
- FTP: `MFMT` に応じないサーバーも模せる。
  「できないと申告する」ほうの振る舞いを試験できる

### 障害の注入

どの偽サーバーも `failNext(操作, 回数, 状態コード, ...)` を持ちます。

```go
f.failNext("get_metadata", 2, 429, `{"error_summary":"too_many_requests/.",...}`)
```

再試行が働くこと、失敗が正しく分類されることを、これで確かめています。

### SMB だけ違う理由

SMB のサーバーを Go で立てる手立てがありません。そこでファイル操作を
小さなインターフェースに切り出し、共有の中身をこの計算機のディレクトリで
代用しています。

**何を試験していて、何を試験していないか**をコードに書いてあります。

- 試験している: パスの組み立て、別名で書いてから置き換える手順、
  空でないディレクトリを消さない判断、書き込み中のものを一覧に出さないこと、
  失敗の分類
- 試験していない: SMB の手続き・認証・文字符号（go-smb2 の受け持ち）

## live 試験

実物に対する試験です。`live` の印が付いています。

```console
HBG_TEST_SMB_HOST=127.0.0.1 \
HBG_TEST_SMB_PORT=1445 \
HBG_TEST_SMB_SHARE=共有 \
HBG_TEST_SMB_USER=試験利用者 \
HBG_TEST_SMB_PASSWORD=ひみつ \
go test -tags live ./backend/smb/
```

環境変数が指定されていなければ飛ばされます。CI では走らせません。

## そのほかの試験

| 場所 | 内容 |
| --- | --- |
| `transfer/compare_test.go` | 判断の表をそのままテーブルにしたもの |
| `transfer/delete_test.go` | 同期での削除の決まり |
| `transfer/retry_test.go` | 再試行（`Permanent` は試さない、`Retry-After` を優先する、など） |
| `transfer/cancel_test.go` | Ctrl-C で部分ファイルが残らない |
| `storage/hash_test.go` | Dropbox 独自ハッシュ（ブロック境界 ±1） |
| `internal/dircache/dircache_test.go` | 並行して呼ばれても作成が1度だけ |
| `internal/cli/storages_test.go` | 設定の2つの書き方、雛形がそのまま読めること |
| `backend/hashes_test.go` | どの組み合わせで `--checksum` が使えるか |

## CI

`.github/workflows/ci.yml` で ubuntu / windows / macos の3つで走らせます。

**Windows は必須です。** パスの正規化（ドライブレター・UNC・区切り文字）の
Windows 固有の不具合を拾うためです。実際、逆斜線を区切りとして扱っていた
問題はここで見つかりました。

`go mod tidy` の差分も検査します。以前 `go.mod` / `go.sum` が
`.gitignore` されており、クリーンクローンからビルドできない状態でした。
その回帰を防ぐためです。

## 試験を書くときの決まり

- **名前で何を確かめているかが分かるようにする。**
  `TestSyncSkipsDeleteWhenTransferFailed` のように
- **なぜその試験があるのかをコメントに書く。**
  過去にどういう不具合があったか、が分かるように
- 直した不具合には回帰試験を必ず書く

---

[リバース資料の目次へ戻る](README.md)
