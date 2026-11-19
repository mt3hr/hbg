package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync/atomic"

	"github.com/mt3hr/hbg/storage"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var hashCmd = &cobra.Command{
	Use:     "hash storage:path",
	Aliases: []string{"sum"},
	Short:   "ファイルやフォルダのハッシュを求める",
	Long: `ファイルやフォルダの内容のハッシュを求めます。

ファイルなら1行、フォルダなら中のファイルを再帰的にたどって
「<ハッシュ>  <相対パス>」の形で1件1行ずつ出します（sha256sum と同じ形）。
--summary を付けると、フォルダ全体を1つにまとめた値だけを出します。

ストレージがその種類のハッシュを持っていればそれを使い、
持っていなければ内容を読み出して計算します。SFTP・SMB・WebDAV・FTP・
OneDrive などでは、内容をすべて転送することになります。

まとめた値は、一覧の出力をそのまま同じ種類のハッシュに通したものです。
空のフォルダは数えません。`,
	Example: `使用例
hbg hash local:C:/photos/a.jpg
hbg hash sftp:/backup/photos
hbg hash --summary dropbox:/backup/photos
hbg hash --type md5 --json s3:/bucket/photos
`,
	Args: cobra.ExactArgs(1),
	PreRunE: func(_ *cobra.Command, args []string) error {
		target := args[0]
		spl := strings.SplitN(target, ":", 2)
		if len(spl) < 2 {
			return withExitCode(ExitUsage, fmt.Errorf("pathの記述が変です: %q（storage:path の形式で指定してください）", target))
		}
		hashOpt.targetStorage = spl[0]
		hashOpt.targetPath = spl[1]

		if _, err := parseHashType(hashOpt.hashType); err != nil {
			return withExitCode(ExitUsage, err)
		}

		if hashOpt.worker == 0 {
			hashOpt.worker = config.DefaultWorker
		}
		if hashOpt.worker < 1 {
			hashOpt.worker = defaultWorker
		}
		return nil
	},
	RunE: runHash,
}

var hashOpt = &struct {
	targetStorage string
	targetPath    string

	hashType string
	summary  bool
	jsonOut  bool
	worker   int
}{}

func init() {
	fs := hashCmd.Flags()
	fs.StringVarP(&hashOpt.hashType, "type", "t", string(storage.SHA256),
		"ハッシュの種類 ("+strings.Join(hashTypeNames(), ", ")+")")
	fs.BoolVarP(&hashOpt.summary, "summary", "s", false, "フォルダ全体を1つにまとめた値だけを出す")
	fs.BoolVar(&hashOpt.jsonOut, "json", false, "1行に1つの JSON で出す")
	fs.IntVarP(&hashOpt.worker, "worker", "w", 0, "同時処理数。0だとconfigファイルの値で動きます。")
}

// hashTypes は hbg hash で選べる種類です。
// 内容を読み出して計算できるものだけを並べます。
var hashTypes = []storage.HashType{storage.SHA256, storage.SHA1, storage.MD5, storage.DropboxContent}

func hashTypeNames() []string {
	names := make([]string, len(hashTypes))
	for i, t := range hashTypes {
		names[i] = string(t)
	}
	return names
}

func parseHashType(s string) (storage.HashType, error) {
	for _, t := range hashTypes {
		if strings.EqualFold(s, string(t)) {
			return t, nil
		}
	}
	return "", fmt.Errorf("ハッシュの種類 %q は使えません（%s のどれかを指定してください）",
		s, strings.Join(hashTypeNames(), ", "))
}

func runHash(cmd *cobra.Command, _ []string) error {
	ctx := cmd.Context()

	resolver, err := resolverFromConfig(config)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}
	defer resolver.Close()

	s, err := resolver.Get(ctx, hashOpt.targetStorage)
	if err != nil {
		return withExitCode(ExitUsage, err)
	}

	ht, _ := parseHashType(hashOpt.hashType) // PreRunE で確かめ済み
	failed, err := printHashes(ctx, os.Stdout, os.Stderr, s, hashOpt.targetPath, hashOptions{
		hashType: ht,
		summary:  hashOpt.summary,
		jsonOut:  hashOpt.jsonOut,
		workers:  hashOpt.worker,
		progress: !hashOpt.jsonOut && isTerminalFile(os.Stderr),
	})
	if err != nil {
		if isCanceled(err) {
			return withExitCode(ExitInterrupted, fmt.Errorf("中断しました"))
		}
		return err
	}
	if failed > 0 {
		return withExitCode(ExitTransferFailed, fmt.Errorf("%d件のハッシュを求められませんでした", failed))
	}
	return nil
}

// hashOptions は printHashes の動作を指定します。
type hashOptions struct {
	hashType storage.HashType
	// summary が真なら、フォルダ全体をまとめた値だけを出します。
	summary bool
	// jsonOut が真なら、1行に1つの JSON で出します。
	jsonOut bool
	workers int
	// progress が真なら、求め終わった件数を errOut に出し続けます。
	progress bool
}

// jsonHash はハッシュ1件ぶんです。
type jsonHash struct {
	Type     string `json:"type"`
	Path     string `json:"path"`
	HashType string `json:"hash_type"`
	Hash     string `json:"hash,omitempty"`
	Error    string `json:"error,omitempty"`
}

// jsonHashSummary はフォルダ全体のまとめです。
type jsonHashSummary struct {
	Type     string `json:"type"`
	Path     string `json:"path"`
	HashType string `json:"hash_type"`
	Hash     string `json:"hash,omitempty"`
	Files    int    `json:"files"`
	Failed   int    `json:"failed"`
}

// printHashes は p のハッシュを out に書き出し、求められなかった件数を返します。
//
// 求められなかった件の理由は errOut に出します。
// hbg hash と対話シェルの hash で共用します。
func printHashes(ctx context.Context, out, errOut io.Writer, s storage.Storage, p string, opts hashOptions) (int, error) {
	info, err := s.Stat(ctx, p)
	if err != nil {
		return 0, fmt.Errorf("error at hash %s. %w", p, err)
	}

	var jsonw *jsonWriter
	if opts.jsonOut {
		jsonw = newJSONWriter(out)
	}

	if !info.IsDir {
		sum, hashErr := storage.HashFile(ctx, s, *info, opts.hashType)
		if hashErr != nil {
			return 0, fmt.Errorf("error at hash %s. %w", p, hashErr)
		}
		if jsonw != nil {
			jsonw.emit(jsonHash{Type: "hash", Path: p, HashType: string(opts.hashType), Hash: sum})
		} else {
			fmt.Fprintln(out, storage.FormatHashLine(sum, p))
		}
		return 0, nil
	}

	var onEntry func(storage.TreeEntry)
	var done atomic.Int64
	if opts.progress {
		onEntry = func(storage.TreeEntry) {
			fmt.Fprintf(errOut, "\r求め終わった件数 %d件", done.Add(1))
		}
	}

	res, err := storage.HashTree(ctx, s, p, opts.hashType, opts.workers, onEntry)
	if opts.progress && done.Load() > 0 {
		// 進み具合の行を空白で消す。エスケープシーケンスは古い Windows の端末で解釈されない。
		fmt.Fprintf(errOut, "\r%s\r", strings.Repeat(" ", 40))
	}
	if err != nil {
		return 0, fmt.Errorf("error at hash %s. %w", p, err)
	}

	for _, e := range res.Entries {
		if e.Err != nil {
			fmt.Fprintf(errOut, "hbg: %s: %v\n", e.RelPath, e.Err)
		}
	}

	if jsonw != nil {
		if !opts.summary {
			for _, e := range res.Entries {
				row := jsonHash{Type: "hash", Path: e.RelPath, HashType: string(opts.hashType), Hash: e.Hash}
				if e.Err != nil {
					row.Error = e.Err.Error()
				}
				jsonw.emit(row)
			}
		}
		jsonw.emit(jsonHashSummary{
			Type:     "summary",
			Path:     p,
			HashType: string(opts.hashType),
			Hash:     res.Sum,
			Files:    len(res.Entries),
			Failed:   res.Failed,
		})
		return res.Failed, nil
	}

	if opts.summary {
		// 欠けた一覧から作った値は出さない。失敗の件数は呼び出し側が伝える。
		if res.Sum != "" {
			fmt.Fprintln(out, storage.FormatHashLine(res.Sum, strings.TrimSuffix(p, "/")+"/"))
		}
		return res.Failed, nil
	}
	for _, e := range res.Entries {
		if e.Err == nil {
			fmt.Fprintln(out, storage.FormatHashLine(e.Hash, e.RelPath))
		}
	}
	return res.Failed, nil
}

// isTerminalFile は w が端末かどうかを返します。
func isTerminalFile(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok || os.Getenv("TERM") == "dumb" || os.Getenv("CI") != "" {
		return false
	}
	return term.IsTerminal(int(f.Fd()))
}
