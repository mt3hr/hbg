# hbg

ローカルファイルシステムとクラウドストレージの間で、ファイルをコピー・同期するコマンドラインツールです。

```console
$ hbg copy local:C:/photos dropbox:/backup
1204つのファイルのコピーを開始します
copy local:C:/photos/2024/a.jpg -> dropbox:/backup/2024
...
```

対応しているストレージ:

| タイプ | 説明 |
| --- | --- |
| `local` | ローカルファイルシステム |
| `dropbox` | Dropbox |
| `googledrive` | Google Drive |

同じタイプのストレージに別々の名前を割り当てることで、複数アカウントを使い分けられます。

## インストール

```console
go install github.com/mt3hr/hbg/cmd/hbg@latest
```

Go 1.25 以上が必要です。

## 設定

設定ファイルは `hbg_config.yaml` です。以下の順に探索されます。

1. `--config_file` で指定したパス
2. 実行ファイルのあるディレクトリ
3. カレントディレクトリ
4. ホームディレクトリ

どこにも見つからない場合、ホームディレクトリに既定の設定ファイルが作成されます。

```yaml
DefaultWorker: 2
local:
  name: local
dropbox:
  - name: dropbox
googledrive:
  - name: googledrive
```

- `DefaultWorker` は同時処理数です。**省略しないでください**（後述の「既知の制限」を参照）。
- `name` はコマンドで `名前:パス` の形式で指定するときの名前です。
- 使わないストレージの行は削除するかコメントアウトしてください。

同じタイプに複数の名前を与えると、複数アカウントを使い分けられます。

```yaml
dropbox:
  - name: dropbox_private
  - name: dropbox_work
```

### 認証

Dropbox / Google Drive は、その名前で初めて使うときに認証を求められます。
表示された URL をブラウザで開き、得られたコードを貼り付けてください。
取得したトークンはホームディレクトリに保存されます。

- `$HOME/hbg_token_dropbox_<name>.json`
- `$HOME/hbg_token_googledrive_<name>.json`

> **Google Drive の認証は現在動作しません。**
> Google が 2023 年 1 月にこの認証方式（OOB フロー）を廃止したためです。
> ブラウザでのローカルリダイレクト方式への移行を予定しています。

## 使い方

### copy — コピー・同期

```console
hbg copy [flags] srcStorage:srcPath destStorage:destDirPath
```

```console
hbg copy local:C:/hoge/test.txt dropbox:/hbg
hbg copy dropbox:/hbg/test.txt local:/home/user/documents
hbg copy -w 10 local:C:/hoge local:C:/fuga
```

コピー先に同名のファイルがあり、**最終更新時刻の差が `--update_duration` 以内かつサイズが一致する**場合はスキップされます。

| フラグ | 既定値 | 説明 |
| --- | --- | --- |
| `-w`, `--worker` | 設定ファイルの `DefaultWorker` | 同時処理数 |
| `--update_duration` | `1s` | この時間以内の更新差は同一とみなす |
| `-i`, `--ignore` | `.nomedia`, `desktop.ini`, `thumbnails`, `.thumbnails`, `Thumbs.db`, `.DS_Store`, `.localized` | 無視するファイル名（完全一致） |

### list — 一覧

```console
hbg list [flags] storage:path
hbg ls -l -r dropbox:/hbg
```

| フラグ | 説明 |
| --- | --- |
| `-l`, `--long` | 詳細表示 |
| `-r`, `--human-readable` | サイズを読みやすい単位で表示 |

### remove — 削除

```console
hbg remove storage:path
```

指定したパスとその中身をすべて削除します。**確認は求められません。**

### shell — 対話シェル

```console
hbg shell
```

`cd` / `cs`（ストレージ切り替え）/ `pwd` / `ls` / `cp` / `rm` / `exit` が使えます。

## 既知の制限

現時点で把握している問題です。順次修正していきます。

- **Google Drive の新規セットアップができません。** 認証方式（OOB フロー）が廃止されたためです。
  ブラウザでのローカルリダイレクト方式への移行を予定しています。
- **Dropbox のアクセストークンは約4時間で失効します。** その都度、再認証が必要です。
  リフレッシュトークンへの対応を予定しています。
- **転送の進捗・速度・残り時間は表示されません。** また、コピー対象の走査が終わるまで出力がありません。
  大きなディレクトリでは、しばらく何も表示されない状態が続きます。
- **転送中に中断すると、コピー先に壊れたファイルが残ることがあります。**
  一時ファイルに書いてから置き換える方式への変更を予定しています。
- **転送が途中で失敗しても自動では再試行されません。**
- コピー先のほうが新しいファイルでも、更新時刻の差が `--update_duration` を超えていれば上書きされます。
- FAT / exFAT は更新時刻の粒度が2秒のため、既定の `--update_duration 1s` では毎回再コピーになります。
  `--update_duration 2s` を指定してください。
- ファイル名に `[` `{` `*` `?` を含むものはコピーできません（パス指定がパターンとして解釈されるため）。
- Google Drive の共有ドライブには対応していません。

### 終了コード

| コード | 意味 |
| --- | --- |
| 0 | 全件成功（0件だった場合も成功） |
| 1 | 実行そのものの失敗（コピー元が見つからない、設定の読み込み失敗など） |
| 2 | 引数や設定の記述の誤り |
| 3 | 一部のファイルの転送に失敗 |

## 開発

```console
go build ./...
go vet ./...
go test -race ./...
gofmt -l .
golangci-lint run
```

`go.mod` / `go.sum` はリポジトリで管理しています。依存を変更したら `go mod tidy` の結果もコミットしてください
（CI で差分が出ていないか検査しています）。

## ライセンス

[MIT License](LICENSE)
