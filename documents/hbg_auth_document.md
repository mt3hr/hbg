# 認証

Dropbox・Google Drive・OneDrive は、初回に認証が必要です。

対象読者: クラウドストレージを使う人

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
storages:
  - name: dropbox
    type: dropbox
    app_key: ${HBG_DROPBOX_APP_KEY}
```

**Google Drive** — [Google Cloud Console](https://console.cloud.google.com/) でプロジェクトを作成し、
Drive API を有効化して「デスクトップアプリ」の OAuth クライアント ID を発行します。

```yaml
storages:
  - name: googledrive
    type: googledrive
    client_id: ${HBG_GOOGLE_CLIENT_ID}
    client_secret: ${HBG_GOOGLE_CLIENT_SECRET}
```

> OAuth 同意画面の公開ステータスを「本番環境」にしてください。
> 「テスト」のままだとリフレッシュトークンが7日で失効します。

Google Drive 全体へのアクセスは「制限付きスコープ」に分類されており、
アプリを一般公開するには年次のセキュリティ評価が必要です。
そのため hbg では利用者自身のプロジェクトを使う方式にしています。

**OneDrive** — [Azure Portal](https://portal.azure.com) の「アプリの登録」で新規登録し、
「認証」でプラットフォーム「モバイル アプリケーションとデスクトップ アプリケーション」を
追加して、リダイレクト URI に `http://localhost:53685/callback`（および 53686、53687）を
登録します。「パブリック クライアント フローを許可する」を「はい」にし、
Microsoft Graph の委任されたアクセス許可 `offline_access` / `Files.ReadWrite.All` /
`User.Read` を追加してください。

```yaml
storages:
  - name: onedrive
    type: onedrive
    client_id: ${HBG_MICROSOFT_CLIENT_ID}
```

クライアント シークレットは不要です（PKCE を使います）。

### 長期トークンを直接使う

アプリコンソールで発行した長期トークンを使うこともできます。

```yaml
storages:
  - name: dropbox
    type: dropbox
    access_token: ${DROPBOX_TOKEN}
```

---

[資料の在り処へ戻る](../README.md#資料の在り処)
