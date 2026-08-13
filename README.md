# hbg

ローカルファイルシステムとクラウドストレージの間で、ファイルをコピー・同期するコマンドラインツールです。

```console
$ hbg copy local:C:/photos dropbox:/backup
調査中  ⠹  1204件 / 18.3GiB（87ディレクトリ）
全体 6.2 GiB / 18.3 GiB [=========>              ]  42.1 MiB/s 残り  4m48s
2024/a.jpg               3.0 MiB/3.8 MiB   8.2 MiB/s
2024/b.jpg               1.2 MiB/5.1 MiB   6.9 MiB/s
```

対応しているストレージ:

| タイプ | 説明 |
| --- | --- |
| `local` | ローカルファイルシステム |
| `dropbox` | Dropbox |
| `googledrive` | Google Drive |
| `onedrive` | OneDrive（個人用・職場用・SharePoint） |
| `s3` | S3 互換（Amazon S3 / Cloudflare R2 / Backblaze B2 / MinIO / Wasabi） |
| `sftp` | SFTP（SSH 越しのファイル転送） |
| `smb` | SMB（Windows のファイル共有・Samba） |
| `webdav` | WebDAV（Nextcloud / ownCloud など） |
| `ftp` | FTP（既定で AUTH TLS） |

同じタイプのストレージに別々の名前を割り当てることで、複数アカウントを使い分けられます。

## インストール

### 配布物を使う

GitHub の Releases から、お使いの環境向けのファイルを取得して展開し、
`hbg`（Windows なら `hbg.exe`）をパスの通った場所に置いてください。

Windows・macOS・Linux の 64bit 版と、macOS・Linux の arm64 版があります。

### ソースから

```console
go install github.com/mt3hr/hbg/cmd/hbg@latest
```

Go 1.25 以上が必要です。

## はじめて使う

```console
hbg copy local:C:/photos dropbox:/backup
```

**初回はこれだけで動きます。** 設定ファイルがなければ、置き場所ごと
雛形が作られます。どこに作ったかは画面に出ます。

```
hbg: 設定ファイルを作成しました: .../hbg/configs/config.yaml
     使うストレージに合わせて編集してください。
```

クラウドを使う場合は、設定ファイルを編集してから認証してください。

```console
hbg auth login dropbox
```

設定ファイルは `$HOME/hbg/configs/config.yaml` です。
ストレージは名前を付けて定義し、コマンドでは `名前:パス` の形で指定します。

```yaml
DefaultWorker: 2

storages:
  - name: local
    type: local
  - name: dropbox
    type: dropbox
```

書き方の詳しいところは [ストレージの設定](documents/hbg_storages_document.md) を
見てください。

## 資料の在り処

| 資料 | 対象読者 | 内容 |
| --- | --- | --- |
| [使い方](documents/hbg_user_document.md) | すべての人 | コマンドの説明・終了コード・ファイルの置き場所 |
| [ストレージの設定](documents/hbg_storages_document.md) | すべての人 | 9種類の設定の仕方と、それぞれにできること |
| [認証](documents/hbg_auth_document.md) | クラウドを使う人 | Dropbox・Google Drive・OneDrive の認証 |
| [ログ](documents/hbg_logging_document.md) | 結果を追いたい人 | 何がどこに記録されるか |
| [リバース資料](documents/reverse/README.md) | 手を入れる人 | ソースから起こした設計資料（ソース対応済） |

コマンドの使い方は `hbg <コマンド> --help` でも見られます。

リバース資料には、用語集・設計思想・フォルダ構成・ストレージの抽象・
転送エンジン・バックエンドごとの実装・失敗の分類・試験の8編があります。
新しいストレージを足す場合は
[ストレージの抽象](documents/reverse/storage-interface.md) から読んでください。

## 既知の制限

現時点で把握している問題です。順次修正していきます。

- SFTP・SMB・WebDAV・FTP には内容のハッシュを求める方法がないため、`--checksum` を使えません。
- OneDrive も `--checksum` を使えません。OneDrive が返すのは `quickXorHash` という
  独自のハッシュで、hbg 側でこれを計算できないためです。実装自体は難しく
  ありませんが、公式の照合用の値と突き合わせて確かめないかぎり、
  「検証したつもりで検証されていない」という状態を作りかねないので入れていません。
- 一般の WebDAV サーバーでは更新時刻を保持できません（上記参照）。
- SMB1 しか話せない古い NAS には繋げません。OS 側でマウントして `local` から使ってください。
- S3 に分割して送られたオブジェクトの ETag は MD5 ではありません。
  hbg が書いたものは元の MD5 を項目に控えるので比較できますが、
  他の道具が分割して書いたものは `--checksum` で比較できません。
- Google ドキュメントなどの独自形式は転送できません（上記参照）。
  書き出し形式を選んで変換する仕組みは未実装です。
- Google Drive は同じフォルダに同じ名前のものを複数作れます。
  その場合、hbg は更新のいちばん新しいものを対象にします。
- クラウドストレージのパスの区切りは `/` だけです。`\` は
  ファイル名の一部として扱うので、`dropbox:\写真` は見つかりません。
  ファイル名に `\` を含むファイルを正しく扱うためです。

## ライセンス

[MIT License](LICENSE)
