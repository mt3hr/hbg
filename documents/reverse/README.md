# hbg リバースエンジニアリング設計資料集

## 概要

このディレクトリには、hbg のソースコードから起こした設計資料を収録しています。

### 作成の背景・目的

hbg は2018年から書かれてきた道具ですが、体系的な設計資料がありませんでした。
2026年の作り直しにあたって、次の目的で用意しました。

- 既存のコードを読み解くための足がかり
- 新しくストレージを足す人が、何を守ればよいかを知るため
- 設計判断の背景と意図を残すため（なぜそうしたのか、が失われやすい）
- 直したはずの不具合が戻らないよう、判断の根拠を書き留めるため

**ソースに対応させて書いています。** 実装を変えたら、こちらも合わせて
書き換えてください。

## 推奨する読み順

前の資料の内容が、後の資料の理解を助けます。

1. **[glossary.md](glossary.md)** — 用語集。以降の資料で使う言葉の定義です。
   最初に読んでください。
2. **[design-philosophy.md](design-philosophy.md)** — 設計思想。hbg が
   何を守ろうとしているかです。ここを外すと、あとの判断が読み解けません。
3. **[folder-structure.md](folder-structure.md)** — フォルダ構成。
   どこに何があるかです。
4. **[storage-interface.md](storage-interface.md)** — ストレージの抽象。
   すべてのバックエンドが満たす約束です。
5. **[transfer-engine.md](transfer-engine.md)** — 転送エンジン。走査・比較・
   再試行・削除の流れです。
6. **[backends.md](backends.md)** — バックエンドごとの実装。9種類それぞれの
   癖と、それにどう対処しているかです。
7. **[error-handling.md](error-handling.md)** — 失敗の分類。待てば直るのか、
   直らないのかをどう決めているかです。
8. **[testing-guide.md](testing-guide.md)** — 試験。適合性スイートと、
   偽サーバーの作り方です。

## 各資料の概要

| ファイル | 内容 | 主な読者・用途 |
| --- | --- | --- |
| [glossary.md](glossary.md) | 言葉の定義 | 全員。読んでいて分からない言葉が出たとき |
| [design-philosophy.md](design-philosophy.md) | 設計思想と、その背景にある実際の不具合 | 判断の理由を知りたいとき |
| [folder-structure.md](folder-structure.md) | パッケージの構成と依存の向き | 初回、ファイルを探すとき |
| [storage-interface.md](storage-interface.md) | `storage.Storage` の仕様と守るべき約束 | バックエンドを足す・直すとき |
| [transfer-engine.md](transfer-engine.md) | 転送の流れと、判断の表 | 転送の挙動を変えるとき |
| [backends.md](backends.md) | 9種類の実装の要点 | 特定のストレージを直すとき |
| [error-handling.md](error-handling.md) | 失敗の分類と再試行 | エラーまわりを触るとき |
| [testing-guide.md](testing-guide.md) | 適合性スイート・偽サーバー・live 試験 | 試験を書くとき |

## 図の見かた

Mermaid で書いています。GitHub 上ではそのまま図として表示されます。
手元で見る場合は、Mermaid に対応した表示器を使ってください。

## 開発環境

```console
go build ./...
go vet ./...
go test -race ./...
gofmt -l .
golangci-lint run
```

Go 1.25 以上が必要です。

Android 向けにビルドする場合は `CGO_ENABLED=1` が必須で、Linux と NDK が要ります。
`scripts/build_android_arm64.sh` を使ってください。理由は
[README の「Android 向けにビルドするとき」](../../README.md#android-向けにビルドするとき)に書いてあります。

`go.mod` / `go.sum` はリポジトリで管理しています。依存を変えたら
`go mod tidy` の結果も一緒にコミットしてください（CI で差分を検査しています）。

試験は docker も認証情報も必要としません。詳しくは
[testing-guide.md](testing-guide.md) を見てください。
