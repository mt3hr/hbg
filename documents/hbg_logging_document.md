# ログ

何がどこに記録されるかです。

対象読者: 転送の結果をあとから追いたい人・不具合を調べる人

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

---

[資料の在り処へ戻る](../README.md#資料の在り処)
