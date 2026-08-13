package s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/mt3hr/hbg/storage"
)

// S3 のエラーは、HTTP の状態コードと Code の2段で表されます。
//
//	403 → 認証や権限の問題
//	404 → 存在しない
//	429 / 503 SlowDown → 要求が多すぎる
//	5xx → 一時的な障害
//
// 提供元によって Code の綴りが少し違うことがあるので、
// 状態コードを主に見て、Code は補助として使います。

// wrapErr は S3 のエラーを storage のエラーに変換します。
func (s *Storage) wrapErr(op, path string, err error) error {
	if err == nil {
		return nil
	}

	v := classify(err)
	if v.sentinel != nil && !errors.Is(err, v.sentinel) {
		// 元のエラーも失わないよう、両方を包む。
		err = fmt.Errorf("%w (%w)", v.sentinel, err)
	}

	return &storage.OpError{
		Op:      op,
		Storage: s.name,
		Path:    path,
		Class:   v.class,
		Err:     err,
	}
}

// verdict は失敗の見立てです。
type verdict struct {
	// sentinel は対応する番兵エラーです。該当するものがなければ nil です。
	sentinel error
	class    storage.Class
}

// classify はエラーの見立てを求めます。
func classify(err error) verdict {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return verdict{class: storage.ClassCanceled}
	case errors.Is(err, storage.ErrNotFound):
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case errors.Is(err, storage.ErrNotEmpty), errors.Is(err, storage.ErrNotDir),
		errors.Is(err, storage.ErrIsDir), errors.Is(err, storage.ErrUnsupported):
		return verdict{class: storage.ClassPermanent}
	}

	// 存在しないことは型でも表される。
	var noKey *s3types.NoSuchKey
	var noBucket *s3types.NoSuchBucket
	var notFound *s3types.NotFound
	switch {
	case errors.As(err, &noKey), errors.As(err, &notFound):
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case errors.As(err, &noBucket):
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	}

	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		return classifyStatus(respErr.HTTPStatusCode(), apiErrorCode(err))
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return classifyCode(apiErr.ErrorCode())
	}

	return verdict{class: storage.ClassUnknown}
}

// apiErrorCode は Code を取り出します。分からなければ空です。
func apiErrorCode(err error) string {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode()
	}
	return ""
}

// classifyStatus は状態コードから判断します。
func classifyStatus(status int, code string) verdict {
	switch status {
	case http.StatusNotFound:
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case http.StatusForbidden, http.StatusUnauthorized:
		return verdict{class: storage.ClassAuth}
	case http.StatusTooManyRequests:
		return verdict{class: storage.ClassRateLimit}
	case http.StatusRequestTimeout, http.StatusConflict:
		return verdict{class: storage.ClassRetryable}
	case http.StatusServiceUnavailable:
		// SlowDown も 503 で返ってくる。どちらにせよ待って試し直す。
		return verdict{class: storage.ClassRateLimit}
	case http.StatusBadRequest:
		return classifyCode(code)
	}

	if status >= 500 && status <= 599 {
		return verdict{class: storage.ClassRetryable}
	}
	return classifyCode(code)
}

// classifyCode は Code から判断します。
func classifyCode(code string) verdict {
	switch code {
	case "NoSuchKey", "NoSuchBucket", "NotFound":
		return verdict{sentinel: storage.ErrNotFound, class: storage.ClassPermanent}
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
		"ExpiredToken", "InvalidToken", "AccountProblem":
		return verdict{class: storage.ClassAuth}
	case "SlowDown", "RequestLimitExceeded", "TooManyRequests":
		return verdict{class: storage.ClassRateLimit}
	case "RequestTimeout", "InternalError", "ServiceUnavailable":
		return verdict{class: storage.ClassRetryable}
	case "EntityTooLarge", "InvalidBucketName", "InvalidObjectName",
		"MethodNotAllowed", "QuotaExceeded":
		return verdict{class: storage.ClassPermanent}
	}
	return verdict{class: storage.ClassUnknown}
}

// isNotFound はエラーが「存在しない」を表すかを返します。
func isNotFound(err error) bool {
	return classify(err).sentinel != nil && errors.Is(classify(err).sentinel, storage.ErrNotFound)
}
