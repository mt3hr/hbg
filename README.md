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

同じタイプのストレージに別々の名前を割り当てることで、複数アカウントを使い分けられます。

## インストール

```console
go install github.com/mt3hr/hbg/cmd/hbg@latest
```

Go 1.25 以上が必要です。

## ファイルの置き場所

設定・認証情報・ログ・キャッシュはすべて `$HOME/hbg` の下にまとめて保存されます。
環境変数 `HBG_HOME` で変更できます。

```
$HOME/hbg/
├── configs/
│   └── config.yaml     設定ファイル
├── tokens/             OAuth トークン
├── credentials/        ストレージ固有の資格情報
├── logs/               ログ
├── caches/             キャッシュ・再開情報
└── shell_history       対話シェルの履歴
```

`hbg config path` で実際の場所を確認できます。

以前のバージョンはこれらをホームディレクトリ直下や一時ディレクトリに置いていました。
`hbg config migrate` で移行できます（移動元は `.migrated` を付けて残ります）。
移行しないままでも、旧パスの設定ファイルは読み込まれます。

## 設定

```console
hbg config init
```

で `$HOME/hbg/configs/config.yaml` に雛形を作成します。

```yaml
DefaultWorker: 2

storages:
  - name: local
    type: local
  - name: dropbox
    type: dropbox
  - name: googledrive
    type: googledrive
```

- `DefaultWorker` は同時処理数です。`copy` の `-w` で上書きできます。
- `name` はコマンドで `名前:パス` の形式で指定するときの名前です。
- `type` はストレージの種類です。`hbg copy --help` で一覧を確認できます。
- 使わないストレージの項目は削除するかコメントアウトしてください。

同じ種類に複数の名前を与えると、複数アカウントを使い分けられます。

```yaml
storages:
  - name: dropbox_private
    type: dropbox
  - name: dropbox_work
    type: dropbox
```

`name` と `type` 以外の項目は、ストレージごとの設定として渡されます。
どの項目でも `${環境変数}` を書けるので、秘密を設定ファイルに直接
書かずに済みます。

`--config_file` で任意のパスを指定することもできます。

### 古い書き方

種類ごとに項目を分ける書き方も引き続き読めます。

```yaml
Local:
  name: local
Dropbox:
  - name: dropbox
```

ただしこの書き方では、あとから追加したストレージ（SFTP など）を
書き表せません。両方を混ぜて書けるので、既存の設定に `storages:` を
足していけば全部を書き直さずに済みます。

### SFTP の指定

```yaml
storages:
  - name: nas
    type: sftp
    host: サーバーのホスト名
    user: ログイン名
    # port: 22
    # key_file: 秘密鍵の場所（省略時は $HOME/hbg/credentials/sftp_<名前>.key）
    # key_passphrase: ${SFTP_KEY_PASSPHRASE}
    # password: ${SFTP_PASSWORD}
    # use_agent: false
    # known_hosts_file: 省略時は $HOME/hbg/configs/known_hosts
    # strict_host_key_checking: yes  # yes / accept-new / no
    # root: 起点にするディレクトリ
```

ホスト鍵は `$HOME/hbg/configs/known_hosts` で確かめます。
`~/.ssh/known_hosts` を既定にしていないのは、hbg が他の道具の設定を
書き換えないためです。使いたい場合は `known_hosts_file` で指定してください。

はじめて接続するときは記録がないので拒否されます。
`strict_host_key_checking: accept-new` を指定すると、鍵の指紋を
表示したうえで記録し、次回からはその鍵とだけ接続します。
**記録済みの鍵が変わった場合は `accept-new` でも拒否します。**

書き込みは `.名前.hbgpart` という一時ファイルに行い、書き終えてから
本来の名前に置き換えます。途中で止めても中身の欠けたファイルは残りません。

### FTP の指定

古い NAS など、FTP しか話せない相手のためのものです。

```yaml
storages:
  - name: ftp
    type: ftp
    host: サーバーのホスト名
    user: ログイン名          # 省略すると anonymous
    password: ${FTP_PASSWORD}
    # port: 21
    # tls: explicit          # explicit / implicit / none
    # insecure_skip_verify: false
    # disable_epsv: false    # 古いサーバー向け
    # disable_mlsd: false    # 一覧の形が壊れているサーバー向け
    # max_conns: 4
    # root: 起点にするディレクトリ
```

**既定で `AUTH TLS` による暗号化を試みます。** FTP は本来、合言葉も
中身も平文で流れます。相手が暗号化に応じない場合は `tls: none` を
明示してください。

FTP は1つの接続で1つのやりとりしかできないので、同時に転送するぶんだけ
接続を開きます。`max_conns` で上限を決められます。

更新時刻は相手が `MFMT` に応じる場合に保持されます。応じない場合は
「保持できない」と申告するので、`--compare modtime`（既定）を指定すると
起動時にエラーになります。

### OneDrive の指定

```yaml
storages:
  - name: onedrive
    type: onedrive
    client_id: ${HBG_MICROSOFT_CLIENT_ID}
    drive_type: personal   # personal / business / sharepoint
    # tenant: 組織の識別子（省略すると個人用と職場用の両方を受け付ける）
    # drive_id: ドライブのID
    # site_id: SharePoint のサイト（drive_type が sharepoint のとき）
    # root: 起点にするディレクトリ
```

Microsoft Entra（旧 Azure AD）でアプリを登録し、**パブリッククライアント**
として設定してください。PKCE を使うのでクライアントシークレットは不要です。
リダイレクト先は「モバイル アプリケーションとデスクトップ アプリケーション」
の `http://localhost` を有効にしてください。

必要な権限は `Files.ReadWrite.All`、`offline_access`、`User.Read` です。
`offline_access` がないとアクセストークンが1時間ほどで失効し、
そのたびに認証が必要になります。

### WebDAV の指定

```yaml
storages:
  - name: nextcloud
    type: webdav
    url: https://例.invalid/remote.php/dav/files/利用者名/
    user: ログイン名
    password: ${WEBDAV_PASSWORD}
    preset: nextcloud    # generic / nextcloud / owncloud
    # root: 起点にするディレクトリ
```

Nextcloud などではアプリ用の合言葉を発行して使ってください。

#### 更新時刻について

**一般の WebDAV サーバーでは更新時刻を保持できません。** WebDAV には
時刻を書き換える標準の方法がなく、`{DAV:}getlastmodified` はサーバーが
管理する項目なので変更できないためです。

Nextcloud と ownCloud だけは `X-OC-Mtime` という独自のヘッダを
受け付けるので、`preset` を指定すれば保持できます。

`preset: generic` のまま書き込む場合、hbg は「更新時刻を保持できない」と
判断して起動時にエラーにします。`--compare size` を指定してください。
黙ってサイズだけの比較に落とすと、比較したつもりで比較されていない
状態になるためです。

### SMB の指定

Windows のファイル共有・Samba に繋ぎます。

```yaml
storages:
  - name: nas
    type: smb
    host: 計算機の名前
    share: 共有の名前
    user: ログイン名
    password: ${SMB_PASSWORD}
    # port: 445
    # domain: 所属
    # root: 起点にするディレクトリ
```

`\\計算機\共有` という書き方は受け付けません。Windows のパスの書き方と
ファイル名の区切りが混ざって、どこまでが計算機名でどこからが共有名なのか
決められなくなるためです。`host` と `share` に分けて書いてください。

ログイン情報は `$HOME/hbg/credentials/smb_<名前>.yaml` にも置けます。

```yaml
user: ログイン名
password: ...
domain: 所属
```

**Windows で共有をドライブに割り当てている場合は、`local` でそのドライブを
指すほうが速くて確実です。** このバックエンドは、割り当てずに直接繋ぎたい
場合や、Windows 以外から使う場合のものです。

SMB1 しか話せない古い NAS には繋げません。その場合は OS 側でマウントして
`local` から使ってください。

### S3 互換の指定

Amazon S3 のほか、Cloudflare R2・Backblaze B2・MinIO・Wasabi など、
同じ口を持つものに使えます。

```yaml
storages:
  - name: s3
    type: s3
    provider: aws       # aws / r2 / b2 / minio / wasabi / other
    bucket: 入れ物の名前
    region: ap-northeast-1
    # endpoint: 接続先（提供元から決まる場合は不要）
    # account_id: Cloudflare R2 の口座ID
    # access_key_id: ${AWS_ACCESS_KEY_ID}
    # secret_access_key: ${AWS_SECRET_ACCESS_KEY}
    # profile: ~/.aws のどの設定を使うか
    # force_path_style: false   # MinIO では true
    # storage_class: STANDARD
    # list_metadata: head
    # directory_markers: true
    # root: 起点にする接頭辞
```

認証情報は次の順で探します。

1. 設定ファイルの `access_key_id` / `secret_access_key`
2. `$HOME/hbg/credentials/s3_<名前>.yaml`
3. 環境変数や `~/.aws`（`profile` を指定した場合もここ）

```yaml
# $HOME/hbg/credentials/s3_s3.yaml
access_key_id: ...
secret_access_key: ...
```

#### 更新時刻について

オブジェクトストレージが持つ時刻は「書き込まれた時刻」で、元のファイルの
更新時刻とは別ものです。hbg は元の時刻を `x-amz-meta-mtime` に入れておき、
そちらを比較に使います。書式は rclone と同じなので、同じ入れ物を
両方の道具から使えます。

ただし一覧の応答にはこの項目が含まれないため、既定
（`list_metadata: head`）では1件ずつ問い合わせます。要求の回数が
件数ぶん増えるので、気になる場合は `list_metadata: none` を指定してください。
そのかわり比較には書き込まれた時刻が使われ、S3 から取り出す向きの
同期で毎回コピーし直すことになります。

#### 空のディレクトリについて

オブジェクトストレージにディレクトリはありません。`写真/2024/a.jpg` の
ような名前を `/` で切って、階層があるかのように見せているだけです。

中身のないディレクトリを表すために、既定では末尾が `/` の空のオブジェクトを
書きます（rclone と同じ）。不要なら `directory_markers: false` にしてください。

### Google Drive の指定

```yaml
GoogleDrive:
  - name: 共有ドライブ
    drive_id: ${DRIVE_ID}     # 共有ドライブのID。省略するとマイドライブ
    root_folder_id: ""        # 特定のフォルダをルートとして扱う
    native_files: error       # Google ドキュメントの扱い（error または skip）
```

Google ドキュメント・スプレッドシートなどの独自形式は、実体のファイルを
持たないためそのままでは取り出せません。`native_files: error`（既定）では
一覧には出るものの、読もうとした時点で失敗として報告されます。
`skip` を指定すると一覧から外れ、転送の対象になりません。

削除は既定でゴミ箱に入ります。

## 認証

```console
hbg auth login <ストレージ名>    # ブラウザで認証する
hbg auth status                  # 認証の状態を確認する
hbg auth logout <ストレージ名>    # 保存されたトークンを削除する
```

`auth login` はブラウザを開き、許可のあとリダイレクトされてくる認可コードを
ローカルで受け取ります。取得したトークンは `$HOME/hbg/tokens` に保存され、
期限が切れても自動的に更新されます。

コピーの途中で認証が始まることはありません。未認証のストレージを使おうとすると
エラーになるので、先に `auth login` を実行してください。

### OAuth クライアントの用意

hbg には認証情報が同梱されていません。ご自身でアプリを登録してください。
未設定のまま `hbg auth login` を実行すると、手順が表示されます。

**Dropbox** — [App Console](https://www.dropbox.com/developers/apps) で Scoped access のアプリを作成し、
Redirect URI に `http://localhost:53682/callback`（および 53683、53684）を登録します。
App key を設定ファイルか環境変数 `HBG_DROPBOX_APP_KEY` に設定してください。
PKCE を使うため App secret は不要です。

```yaml
Dropbox:
  - name: dropbox
    app_key: ${HBG_DROPBOX_APP_KEY}
```

**Google Drive** — [Google Cloud Console](https://console.cloud.google.com/) でプロジェクトを作成し、
Drive API を有効化して「デスクトップアプリ」の OAuth クライアント ID を発行します。

```yaml
GoogleDrive:
  - name: googledrive
    client_id: ${HBG_GOOGLE_CLIENT_ID}
    client_secret: ${HBG_GOOGLE_CLIENT_SECRET}
```

> OAuth 同意画面の公開ステータスを「本番環境」にしてください。
> 「テスト」のままだとリフレッシュトークンが7日で失効します。

Google Drive 全体へのアクセスは「制限付きスコープ」に分類されており、
アプリを一般公開するには年次のセキュリティ評価が必要です。
そのため hbg では利用者自身のプロジェクトを使う方式にしています。

### 長期トークンを直接使う

アプリコンソールで発行した長期トークンを使うこともできます。

```yaml
Dropbox:
  - name: dropbox
    access_token: ${DROPBOX_TOKEN}
```

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

#### 何を転送するかの判断

既定では、コピー先に同名のファイルがあり、**最終更新時刻の差が許容幅以内で、
かつサイズが一致する**場合はスキップされます。

**コピー先のほうが新しいファイルは上書きされません**（`--overwrite` で従来どおりに戻せます）。

| フラグ | 既定値 | 説明 |
| --- | --- | --- |
| `--compare` | `size,modtime` | 比較に使う項目（`size`, `modtime`, `hash` をカンマ区切り） |
| `--checksum` | false | 内容のハッシュで比較する（`--compare size,hash` と同じ） |
| `--size-only` | false | サイズだけで比較する |
| `--modify-window` | 自動 | この時間以内の更新時刻の差は同一とみなす |
| `--update` | true | コピー先のほうが新しい場合は上書きしない |
| `--overwrite` | false | コピー先のほうが新しくても上書きする |
| `--ignore-existing` | false | コピー先にあるものは内容を問わず転送しない |
| `--verify` | `auto` | 転送後の内容の検証（`auto`, `always`, `never`） |

許容幅は両側のストレージの分解能から自動的に決まります（Dropbox は秒単位なので1秒、など）。
FAT / exFAT は2秒刻みですが実行時に判別できないため、その上で使う場合は
`--modify-window 2s` を指定してください。指定しないと毎回コピーし直しになります。

サイズも更新時刻も同じで内容だけが違う場合（転送が途中で終わっていた、など）は
既定の比較では気づけません。`--checksum` を使うと内容のハッシュで比べます。
両側に共通して使えるハッシュがない組み合わせでは、黙ってサイズ比較に落とさず
起動時にエラーになります。

#### 絞り込み

| フラグ | 説明 |
| --- | --- |
| `-i`, `--ignore` | 名前が完全に一致するものを転送しない |
| `--include` | このパターンに一致するものだけを転送する |
| `--exclude` | このパターンに一致するものを転送しない |
| `--min-size` / `--max-size` | サイズで絞り込む（例: `1M`, `1G`） |

パターンはパスと名前のどちらでも照合されます。`*` はディレクトリの境界を
越えません。越えたい場合は `**` を使ってください。

```console
hbg copy --include "*.jpg" --exclude "cache/**" local:C:/photos dropbox:/backup
```

走査と転送は並行して進みます。対象を数え終わるのを待たずに転送が始まるので、
大きなディレクトリでも待たされません。使用メモリはファイル数によらず一定です。

端末では進捗バーが表示されます。全体の進みぐあい・転送速度・残り時間と、
転送中のファイルごとの状況が分かります。走査は転送と並行するので、
全体の総量は調査が進むにつれて増えていきます。

パイプやジョブとして実行した場合は、制御文字を含まない行ログに切り替わります。

Ctrl-C で安全に中断できます。書き込み中のファイルは一時ファイルに書かれ、
完了してから置き換えられるため、中断しても壊れたファイルは残りません。

| フラグ | 既定値 | 説明 |
| --- | --- | --- |
| `-w`, `--worker` | 設定ファイルの `DefaultWorker` | 同時処理数 |
| `--dry-run` | false | 実際には転送せず、何が転送されるかだけを表示する |
| `--tps` | 0 | 1秒あたりの API 呼び出し回数の上限（0で無制限） |
| `--bwlimit` | なし | 転送速度の上限（例: `10M`, `512K`） |
| `--max-errors` | 0 | この件数を超えて失敗したら中断する（0で無制限） |
| `--progress` | `auto` | 進捗の表示（`auto`, `always`, `never`, `none`） |
| `--progress-bars` | 8 | 同時に表示するファイルごとのバーの本数 |
| `--stats` | `30s` | 進捗バーを使わないときに集計を表示する間隔 |
| `-q`, `--quiet` | false | 進捗を表示しない |

#### 失敗したときの再試行

2段構えになっています。

```console
hbg copy --retry 3 --retry-wait 5s --retry-pass 2 --retry-pass-wait 60s \
    local:C:/photos dropbox:/backup
```

```
[pass 1/3]
  a.jpg  ... 失敗 (429) → 5秒待機 → 再試行 1/3
  a.jpg  ... OK
  b.mp4  ... 失敗 (3回とも) → 失敗として記録
コピー完了: 1203件成功, 1件失敗
1件失敗しました。60秒待機して再実行します...

[pass 2/3]
  (成功済みの1203件はスキップ判定で即座に飛ばされる)
  b.mp4  ... OK
コピー完了: 1件成功, 0件失敗
```

| フラグ | 既定値 | 説明 |
| --- | --- | --- |
| `--retry` | 3 | 1ファイルの転送に失敗したときの再試行回数（0で無効） |
| `--retry-wait` | `5s` | 再試行までの待ち時間 |
| `--retry-backoff` | false | 待ち時間を試行ごとに伸ばす（既定は一定） |
| `--retry-pass` | 0 | 失敗が残っていたときに全体をやり直す回数（0で無効） |
| `--retry-pass-wait` | `1m` | 全体をやり直すまでの待ち時間 |

やり直しでは、転送済みのファイルはスキップ判定で飛ばされるので、
実質的に失敗したぶんだけが再試行されます。

存在しない・権限がないといった、待っても直らない失敗は再試行しません。
サーバーから待ち時間を指示された場合（429 の `Retry-After`）はそちらを優先します。
1件も転送できなかったやり直しがあれば、回数が残っていても打ち切ります。

### check — 差分の確認

```console
hbg check local:C:/photos dropbox:/backup
```

転送せずに、何が転送されるかを一覧します。差分があれば終了コード 3 を返すので、
バックアップが取れているかの確認をジョブから行えます。

```
older.txt     4B  サイズが違う（4 と 6）
onlysrc.txt   5B  コピー先にない
sub/deep.txt  2B  コピー先にない

差分 3件 / 11B、一致 2件
```

`copy` と同じ比較・絞り込みの指定が使えます。`--all` で一致しているものも表示します。

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

Google Drive では既定でゴミ箱に入ります。それ以外は元に戻せません。

### shell — 対話シェル

```console
hbg shell
```

`cd` / `cs`（ストレージ切り替え）/ `pwd` / `ls` / `cp` / `rm` / `exit` が使えます。

### config — 設定の操作

```console
hbg config init      # 設定ファイルの雛形を作る
hbg config path      # 設定・認証情報・ログの場所を表示する
hbg config migrate   # 旧レイアウトのファイルを $HOME/hbg 配下へ移す
```

## ログ

実行の記録は `$HOME/hbg/logs` に JSON で残ります（1行1レコード）。
端末の表示とは独立しているので、あとから「どのファイルが失敗したか」を追えます。

| ファイル | 内容 |
| --- | --- |
| `hbg.log` | すべてのレベルを統合したもの |
| `hbg_transfer.log` | 転送1件につき1レコード（送受信元・サイズ・所要時間・速度・結果） |
| `hbg_info.log` | 実行の要約 |
| `hbg_warn.log` / `hbg_error.log` | 警告・エラー |
| `hbg_debug.log` / `hbg_trace.log` | 詳細（`--log debug` 以上のとき） |

```console
$ cat ~/hbg/logs/hbg_transfer.log | jq -r '"\(.result)\t\(.src_path)\t\(.bytes)B"'
copied  /photos/a.jpg   204813B
failed  /photos/b.jpg   null
```

| フラグ | 既定値 | 説明 |
| --- | --- | --- |
| `--log` | `info` | `none`, `error`, `warn`, `info`, `transfer`, `debug`, `trace` |
| `--log-stdout` | false | 標準出力にも書く |
| `--log-max-size` | 10 | 1ファイルの上限 (MiB) |
| `--log-max-backups` | 5 | 保持する世代数 |
| `--log-max-age` | 30 | 保持する日数（0 で削除しない） |

サイズと世代でローテーションするので、`--log trace` を常用してもファイルは
際限なく大きくなりません。

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

### ストレージごとにできること

| | ローカル | Dropbox | Google Drive | OneDrive | SFTP | SMB | WebDAV | FTP | S3 互換 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 更新時刻の保持 | ○ | ○（秒） | ○（ミリ秒） | ○（ミリ秒） | ○（秒） | ○（100ns） | △（preset 次第） | △（MFMT 次第） | ○（項目に保存） |
| ハッシュ | sha256 / md5 / sha1 / dropbox | dropbox | sha256 / sha1 / md5 | － | － | － | － | － | md5 |
| サーバー側コピー | － | ○ | ○ | － | － | － | ○ | － | ○ |
| 移動・改名 | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○（コピーして削除） |
| 途中からの読み出し | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ |
| 分割送信 | － | ○ | ○ | ○ | － | － | － | － | ○ |
| 空のディレクトリ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | ○ | △（印を書く） |

`--checksum` は両側に共通して使えるハッシュがある組み合わせでのみ動きます。
ローカルは dropbox 形式のハッシュも計算できるので、ローカルと Dropbox、
ローカルと Google Drive のどちらでも内容の比較ができます。
Dropbox と Google Drive の間には共通のハッシュがないため、
`--checksum` を指定すると起動時にエラーになります。

### 終了コード

| コード | 意味 |
| --- | --- |
| 0 | 全件成功（0件だった場合も成功） |
| 1 | 実行そのものの失敗（コピー元が見つからない、設定の読み込み失敗など） |
| 2 | 引数や設定の記述の誤り |
| 3 | 一部のファイルの転送に失敗 |
| 130 | Ctrl-C で中断 |

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
