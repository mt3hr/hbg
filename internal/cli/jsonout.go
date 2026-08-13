package cli

import (
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/mt3hr/hbg/transfer"
)

// --json を付けると、1行に1つの JSON を標準出力へ流します。
//
// 人向けの表示は標準エラーへ出るので、混ざりません。
// パイプで他の道具へ渡したり、あとから集計したりするためのものです。
//
// 種類は type で見分けます。
//
//	transfer  1ファイルの転送が終わった
//	skip      転送しないと判断した
//	delete    コピー先から消した（--delete のとき）
//	summary   最後にまとめて1つ
//
// 途中経過は流しません。1件が終わるたびに1行が出るので、
// 受け取る側は行ごとに処理できます。

// jsonWriter は機械向けの出力先です。
type jsonWriter struct {
	mu  sync.Mutex
	enc *json.Encoder
}

func newJSONWriter(w io.Writer) *jsonWriter {
	return &jsonWriter{enc: json.NewEncoder(w)}
}

// emit は1件を書き出します。
func (j *jsonWriter) emit(v any) {
	if j == nil {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	_ = j.enc.Encode(v)
}

// jsonTransfer は転送1件ぶんです。
type jsonTransfer struct {
	Type       string `json:"type"`
	SrcStorage string `json:"src_storage"`
	SrcPath    string `json:"src_path"`
	DstStorage string `json:"dst_storage"`
	DstPath    string `json:"dst_path"`
	Bytes      int64  `json:"bytes"`
	DurationMS int64  `json:"duration_ms"`
	Attempts   int    `json:"attempts"`
	Result     string `json:"result"`
	Error      string `json:"error,omitempty"`
}

// jsonDecision は判断1件ぶんです。
type jsonDecision struct {
	Type   string `json:"type"`
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Reason string `json:"reason"`
}

// jsonSummary は最後のまとめです。
type jsonSummary struct {
	Type         string   `json:"type"`
	Transferred  int      `json:"transferred"`
	Skipped      int      `json:"skipped"`
	Failed       int      `json:"failed"`
	Deleted      int      `json:"deleted,omitempty"`
	DeleteFailed int      `json:"delete_failed,omitempty"`
	Bytes        int64    `json:"bytes"`
	BytesSkipped int64    `json:"bytes_skipped"`
	ElapsedMS    int64    `json:"elapsed_ms"`
	Aborted      bool     `json:"aborted,omitempty"`
	Errors       []string `json:"errors,omitempty"`
}

// onTransfer は転送1件を書き出す関数を返します。
func (j *jsonWriter) onTransfer(srcStorage, dstStorage string) func(transfer.TransferEvent) {
	return func(ev transfer.TransferEvent) {
		out := jsonTransfer{
			Type:       "transfer",
			SrcStorage: srcStorage,
			SrcPath:    ev.SrcPath,
			DstStorage: dstStorage,
			DstPath:    ev.DstPath,
			Bytes:      ev.Bytes,
			DurationMS: ev.Duration.Milliseconds(),
			Attempts:   ev.Attempts,
			Result:     "copied",
		}
		switch {
		case ev.Err != nil:
			out.Result = "failed"
			out.Error = ev.Err.Error()
		case ev.Skipped:
			out.Result = "skipped"
		}
		j.emit(out)
	}
}

// onDecision は判断1件を書き出す関数を返します。
func (j *jsonWriter) onDecision() func(transfer.DecisionEvent) {
	return func(ev transfer.DecisionEvent) {
		kind := ""
		switch ev.Action {
		case transfer.ActionSkip:
			kind = "skip"
		case transfer.ActionDelete:
			kind = "delete"
		default:
			// 転送するという判断は、終わったときに transfer として出る。
			return
		}

		j.emit(jsonDecision{
			Type:   kind,
			Path:   ev.Path,
			Size:   ev.Size,
			Reason: ev.Reason,
		})
	}
}

// summary は最後のまとめを書き出します。
func (j *jsonWriter) summary(r *transfer.Result) {
	if r == nil {
		return
	}

	out := jsonSummary{
		Type:         "summary",
		Transferred:  r.Transferred,
		Skipped:      r.Skipped,
		Failed:       r.Failed,
		Deleted:      r.Deleted,
		DeleteFailed: r.DeleteFailed,
		Bytes:        r.Bytes,
		BytesSkipped: r.BytesSkipped,
		ElapsedMS:    r.Elapsed.Round(time.Millisecond).Milliseconds(),
		Aborted:      r.Aborted,
	}
	for _, err := range r.Errors {
		out.Errors = append(out.Errors, err.Error())
	}
	j.emit(out)
}

// onDecisionAll は、転送するという判断も含めて書き出す関数を返します。
// hbg check のように、判断そのものを見たい場合に使います。
func (j *jsonWriter) onDecisionAll() func(transfer.DecisionEvent) {
	return func(ev transfer.DecisionEvent) {
		kind := "copy"
		switch ev.Action {
		case transfer.ActionSkip:
			kind = "skip"
		case transfer.ActionDelete:
			kind = "delete"
		}

		j.emit(jsonDecision{
			Type:   kind,
			Path:   ev.Path,
			Size:   ev.Size,
			Reason: ev.Reason,
		})
	}
}
