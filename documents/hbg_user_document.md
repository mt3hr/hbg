# 使い方

hbg のコマンドと、その使い分けです。

対象読者: hbg を使うすべての人

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

### sync — コピー先をコピー元に合わせる

```console
hbg sync local:C:/photos dropbox:/backup
hbg sync --delete local:C:/photos dropbox:/backup
```

`--delete` を付けると、コピー元にないものをコピー先から削除します。
付けない場合の動きは `copy` と同じです。

削除には次の決まりがあります。取り返しのつかない操作なので、
判断に迷いのある場面では消しません。

- **転送に1件でも失敗があれば削除しません。** コピー元を読めなかった
  だけで「向こうには無い」と判断すると、取っておきたいものを消すことに
  なるためです。
- **`--include` や `--exclude` で対象外にしたものは消しません。**
  転送していないものを消すのは筋が通らないためです。
- 深いものから消します。ディレクトリは中身が無くならないと消せません。

**はじめて実行するときは `--dry-run` で確かめてください。**

```console
hbg sync --delete --dry-run local:C:/photos dropbox:/backup
```

### move — 移動・改名

```console
hbg move local:C:/photos/a.jpg local:C:/photos/2024/a.jpg
hbg move dropbox:/古い名前 dropbox:/新しい名前
```

同じストレージの中でファイルやディレクトリを移動・改名します。
中身は運ばれないので、大きなファイルでもすぐ終わります。

別のストレージへは移せません。`copy` してから `remove` してください。
1つの操作にまとめると、コピーに失敗したのに元を消してしまう、といった
事故が起こりえます。

### mkdir — ディレクトリを作る

```console
hbg mkdir dropbox:/backup/2024
```

途中のディレクトリも必要なら作ります。すでにある場合は何もしません。

### 機械可読な出力

`--json` を付けると、1行に1つの JSON を標準出力へ流します。
人向けの表示は標準エラーへ出るので、混ざりません。

```console
hbg copy --json local:C:/photos dropbox:/backup | jq -r 'select(.type=="transfer") | .dst_path'
```

`copy` / `sync` / `check` で使えます。

| `type` | 意味 |
| --- | --- |
| `transfer` | 1ファイルの転送が終わった（`result` は `copied` / `failed`） |
| `skip` | 転送しないと判断した |
| `delete` | コピー先から消した（`sync --delete` のとき） |
| `copy` | 転送すると判断した（`check` のときだけ） |
| `summary` | 最後にまとめて1つ |

```json
{"type":"transfer","src_storage":"local","src_path":"...","dst_storage":"dropbox","dst_path":"...","bytes":4,"duration_ms":4,"attempts":1,"result":"copied"}
{"type":"skip","path":"b.txt","size":5,"reason":"同じ"}
{"type":"summary","transferred":1,"skipped":1,"failed":0,"bytes":4,"bytes_skipped":5,"elapsed_ms":8}
```

## check — 差分の確認

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

### completion — 入力補完

```console
hbg completion bash      # bash / zsh / fish / powershell
```

シェルの入力補完の設定を書き出します。設定の仕方は
`hbg completion --help` に書いてあります。

### config — 設定の操作

```console
hbg config init      # 設定ファイルの雛形を作る
hbg config path      # 設定・認証情報・ログの場所を表示する
```

### 終了コード

| コード | 意味 |
| --- | --- |
| 0 | 全件成功（0件だった場合も成功） |
| 1 | 実行そのものの失敗（コピー元が見つからない、設定の読み込み失敗など） |
| 2 | 引数や設定の記述の誤り |
| 3 | 一部のファイルの転送に失敗 |
| 130 | Ctrl-C で中断 |

## 初回起動

設定ファイルがない状態でコマンドを実行すると、置き場所ごと雛形が
作られます。`hbg config init` を先に打つ必要はありません。

```
hbg: 設定ファイルを作成しました: .../hbg/configs/config.yaml
     使うストレージに合わせて編集してください。
```

作られるのは次のとおりです。権限は 0700（ファイルは 0600）で、
他の利用者からは読めません。認証情報を置く場所だからです。

```
$HOME/hbg/
├── configs/config.yaml
├── tokens/
├── credentials/
├── logs/
└── caches/
```

すでにある設定ファイルは書き換えません。`--help` や `version` を
打っただけでは何も作られません。

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

hbg が読み書きするのはこの配置だけです。ほかの場所に置いた設定ファイルは
読みません（`--config_file` で明示したときを除く）。

---

[資料の在り処へ戻る](../README.md#資料の在り処)
