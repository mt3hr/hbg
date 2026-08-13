# ストレージの設定

hbg が扱えるストレージと、それぞれの指定の仕方です。

対象読者: hbg を使うすべての人

## ストレージごとにできること

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
storages:
  - name: 共有ドライブ
    type: googledrive
    drive_id: ${DRIVE_ID}     # 共有ドライブのID。省略するとマイドライブ
    root_folder_id: ""        # 特定のフォルダをルートとして扱う
    native_files: error       # Google ドキュメントの扱い（error または skip）
```

Google ドキュメント・スプレッドシートなどの独自形式は、実体のファイルを
持たないためそのままでは取り出せません。`native_files: error`（既定）では
一覧には出るものの、読もうとした時点で失敗として報告されます。
`skip` を指定すると一覧から外れ、転送の対象になりません。

削除は既定でゴミ箱に入ります。

---

[資料の在り処へ戻る](../README.md#資料の在り処)
