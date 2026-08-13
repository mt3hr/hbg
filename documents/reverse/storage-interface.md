# ストレージの抽象

すべてのバックエンドが満たす約束です。

## `storage.Storage`

```go
type Storage interface {
    Type() string
    Name() string
    Features() *Features

    List(ctx context.Context, dir string, fn func(FileInfo) error) error
    Stat(ctx context.Context, path string) (*FileInfo, error)
    Open(ctx context.Context, path string) (io.ReadCloser, *FileInfo, error)
    Put(ctx context.Context, path string, r io.Reader, meta ObjectMeta) (*FileInfo, error)
    Mkdir(ctx context.Context, dir string) error
    Remove(ctx context.Context, path string) error
    Close() error
}
```

ここに並ぶのは、**どのストレージでも実装できる最小限**だけです。
追加の能力はオプショナルインターフェースで表します。

## 守るべき約束

### `List`

- `fn` が非 nil を返したら列挙を打ち切り、その値をそのまま返す
- ページ分割は内部で完結させる。**全件をメモリに溜めない**
- `dir` が存在しなければ `ErrNotFound` を含むエラーを返す
- 書きかけ（`.hbgpart`）は出さない

### `Put`

- `r` を **Close しない**。所有権は呼び出し側にある
- `meta.Size` は `SizeUnknown` のことがある。**宣言されたサイズではなく、
  読み終わるまでを書き込む**
- すでにある場合は上書きする
- 途中経過が見えないよう、できるだけ不可分に書き込む
- `ctx` が取り消されたら、中途半端な結果を残さない
- 戻り値は**書き込み後の実際のメタデータ**（呼び出し側がサイズを検査する）

### `Remove`

- **1つのファイル、または空のディレクトリ**だけを消す
- 中身のあるディレクトリには `ErrNotEmpty` を返す
- 中身ごと消したい場合は `Purger` を使う

多くのクラウドの削除は再帰的なので、ディレクトリの場合は空であることを
先に確かめる実装になっています。`hbg rm` が想定より多く消してしまわない
ためです。

### `Mkdir`

- 必要なら親ごと作る
- すでにあれば何もしない（冪等）

## `Features`

そのストレージにできることの申告です。

```go
type Features struct {
    ModTimePrecision time.Duration  // 更新時刻の分解能
    CanSetModTime    bool           // 書き込み時に時刻を指定できるか
    CaseInsensitive  bool           // 大文字小文字を区別しないか
    Hashes           HashSet        // 追加の入出力なしで得られるハッシュ

    ImplicitDirs bool  // Put が親ディレクトリを自動で作るか
    EmptyDirs    bool  // 空のディレクトリを表現できるか
    AtomicPut    bool  // 書き込みの途中経過が観測されないか
    OSPath       bool  // OS のパス規則（ドライブレター・UNC）に従うか

    MaxFileSize  int64
    IllegalChars string
}
```

**できないことをできると言わない**のが、いちばん大事な約束です。

- `CanSetModTime` を偽にすると、時刻での比較が起動時に断られます
- `Hashes` を空にすると、`--checksum` が起動時に断られます
- `EmptyDirs` を偽にすると、適合性スイートの該当試験が飛ばされます

申告と実装が食い違っていないかは、適合性スイートの
「Featuresとの整合」で確かめています。

## オプショナルインターフェース

```go
type Hasher interface {
    Hash(ctx context.Context, path string, ht HashType) (string, error)
}
type ServerSideCopier interface {
    ServerSideCopy(ctx context.Context, srcPath, dstPath string) (*FileInfo, error)
}
type Mover interface {
    Move(ctx context.Context, srcPath, dstPath string) error
}
type RangeOpener interface {
    OpenRange(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error)
}
type Purger interface {
    Purge(ctx context.Context, dir string) error
}
type SetModTimer interface {
    SetModTime(ctx context.Context, path string, t time.Time) error
}
```

**型アサーションは `storage` パッケージのヘルパに閉じ込めます。**
呼び出し側に散らすと、「できるなら速い方法、できないなら確実な方法」の
分岐がそこら中に生えます。

| ヘルパ | できる場合 | できない場合 |
| --- | --- | --- |
| `storage.Copy` | 同一ストレージなら `ServerSideCopy` | 読んで書く |
| `storage.Move` | `Mover` | コピーしてから削除 |
| `storage.PurgeAll` | `Purger` | 後行順にたどって1件ずつ |
| `storage.GetHash` | `FileInfo.Hashes` → `Hasher` | `ErrUnsupported` |

## `FileInfo` と `ObjectMeta`

```go
type FileInfo struct {
    Path    string              // ストレージのルートからの絶対パス。区切りは "/"
    Name    string
    IsDir   bool
    Size    int64               // 分からなければ SizeUnknown
    ModTime time.Time           // ゼロ値なら不明
    Hashes  map[HashType]string // 追加の入出力なしで得られたものだけ
    ID      string              // バックエンド固有の識別子
}
```

`Size` に `SizeUnknown`（-1）があるのは、**0（空ファイル）と「分からない」を
区別する**ためです。ここを区別しなかったことが、Dropbox への転送で内容が
無警告に切り詰められる不具合の原因でした。

## パスの扱い

- 区切りは `/`
- **`\` は区切りとして扱わない。** クラウドストレージではファイル名に
  使えるふつうの文字で、区切りに読み替えると別の場所を指すことになります
- 例外は `local`（`Features.OSPath` が真）と `smb`。こちらは Windows の
  作法に従います

`storage.CleanPath` は `OSPath` が真のストレージには使わないでください。
ドライブレターが `C:/...` から `/C:/...` に壊れます。

## 新しいバックエンドの足しかた

1. `backend/<名前>/` を作る
2. `storage.Storage` を実装する
3. `init()` で `backend.Register` を呼ぶ
4. `internal/cli/cmd.go` に blank import を1行足す
5. 適合性スイートを通す

```go
func init() {
    backend.Register(backend.Descriptor{
        Type:      Type,
        Summary:   "一行の説明",
        ConfigDoc: "  # - name: ...\n",  // 設定ファイルの雛形に載る
        New: func(ctx context.Context, name string, params backend.Params) (storage.Storage, error) {
            return New(ctx, Config{Name: name, ...})
        },
    })
}
```

ヘルプの一覧も設定の雛形も、ここから組み立てられます。
書き足す場所は他にありません。

---

[リバース資料の目次へ戻る](README.md)
