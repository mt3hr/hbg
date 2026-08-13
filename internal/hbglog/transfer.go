package hbglog

import (
	"context"
	"log/slog"
	"time"
)

// TransferRecord は 1 ファイルの転送結果です。
//
// 転送 1 件につき 1 レコードを hbg_transfer.log に記録します。
// 端末の表示とは独立しているので、あとから「どのファイルが失敗したか」を
// 追えます。
type TransferRecord struct {
	SrcStorage string
	SrcPath    string
	DstStorage string
	DstPath    string
	Bytes      int64
	Duration   time.Duration
	// Result は "copied" / "skipped" / "failed" のいずれかです。
	Result string
	Err    error
}

// 転送結果の種別。
const (
	ResultCopied  = "copied"
	ResultSkipped = "skipped"
	ResultFailed  = "failed"
)

// LogTransfer は転送結果を記録します。
func LogTransfer(rec TransferRecord) {
	attrs := []slog.Attr{
		slog.String("result", rec.Result),
		slog.String("src_storage", rec.SrcStorage),
		slog.String("src_path", rec.SrcPath),
		slog.String("dst_storage", rec.DstStorage),
		slog.String("dst_path", rec.DstPath),
	}
	if rec.Bytes > 0 {
		attrs = append(attrs, slog.Int64("bytes", rec.Bytes))
	}
	if rec.Duration > 0 {
		attrs = append(attrs,
			slog.String("duration", rec.Duration.String()),
			slog.Int64("duration_ms", rec.Duration.Milliseconds()),
		)
		if rec.Bytes > 0 && rec.Duration > 0 {
			bps := float64(rec.Bytes) / rec.Duration.Seconds()
			attrs = append(attrs, slog.Int64("bytes_per_sec", int64(bps)))
		}
	}
	if rec.Err != nil {
		attrs = append(attrs, slog.String("error", rec.Err.Error()))
	}

	slog.LogAttrs(context.Background(), LevelTransfer, "transfer", attrs...)
}

// LogSummary は転送全体の集計を記録します。
func LogSummary(transferred, failed int, elapsed time.Duration) {
	slog.LogAttrs(context.Background(), LevelInfo, "copy finished",
		slog.Int("transferred", transferred),
		slog.Int("failed", failed),
		slog.String("elapsed", elapsed.String()),
	)
}
