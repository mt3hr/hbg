package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/mt3hr/hbg/backend/memory"
	"github.com/mt3hr/hbg/storage"
)

func sha256Of(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func hashTestStorage(t *testing.T) *memory.Storage {
	t.Helper()
	m := memory.New("mem")
	ctx := context.Background()
	for p, c := range map[string]string{
		"/d/a.txt":     "a",
		"/d/sub/b.txt": "b",
	} {
		if _, err := m.Put(ctx, p, strings.NewReader(c), storage.ObjectMeta{Size: int64(len(c))}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	return m
}

func TestPrintHashes(t *testing.T) {
	m := hashTestStorage(t)
	listing := sha256Of("a") + "  a.txt\n" + sha256Of("b") + "  sub/b.txt\n"

	tests := []struct {
		name string
		path string
		opts hashOptions
		want string
	}{
		{
			name: "ファイル1件",
			path: "/d/a.txt",
			want: sha256Of("a") + "  /d/a.txt\n",
		},
		{
			name: "フォルダは1件1行",
			path: "/d",
			want: listing,
		},
		{
			name: "--summary は一覧を sha256 したもの",
			path: "/d",
			opts: hashOptions{summary: true},
			want: sha256Of(listing) + "  /d/\n",
		},
		{
			name: "--type md5",
			path: "/d/a.txt",
			opts: hashOptions{hashType: storage.MD5},
			want: "0cc175b9c0f1b6a831c399e269772661  /d/a.txt\n", // md5("a")
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.opts.hashType == "" {
				tt.opts.hashType = storage.SHA256
			}
			tt.opts.workers = 2
			var out, errOut bytes.Buffer
			failed, err := printHashes(context.Background(), &out, &errOut, m, tt.path, tt.opts)
			if err != nil {
				t.Fatalf("printHashes: %v", err)
			}
			if failed != 0 {
				t.Errorf("failed = %d", failed)
			}
			if out.String() != tt.want {
				t.Errorf("出力\n got: %q\nwant: %q", out.String(), tt.want)
			}
			if errOut.Len() != 0 {
				t.Errorf("標準エラーに出力がある: %q", errOut.String())
			}
		})
	}
}

func TestPrintHashesJSON(t *testing.T) {
	m := hashTestStorage(t)
	var out, errOut bytes.Buffer
	_, err := printHashes(context.Background(), &out, &errOut, m, "/d",
		hashOptions{hashType: storage.SHA256, jsonOut: true, workers: 1})
	if err != nil {
		t.Fatalf("printHashes: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("行数 %d, want 3: %q", len(lines), out.String())
	}
	var first jsonHash
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if first.Type != "hash" || first.Path != "a.txt" || first.Hash != sha256Of("a") || first.HashType != "sha256" {
		t.Errorf("1行目 = %+v", first)
	}
	var sum jsonHashSummary
	if err := json.Unmarshal([]byte(lines[2]), &sum); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if sum.Type != "summary" || sum.Files != 2 || sum.Failed != 0 || sum.Hash == "" {
		t.Errorf("まとめ = %+v", sum)
	}
}

// 失敗した件は標準エラーへ出し、一覧からもまとめからも外します。
func TestPrintHashesFailure(t *testing.T) {
	m := hashTestStorage(t)
	m.SetHooks(memory.Hooks{BeforeOp: func(op, p string) error {
		if op == "open" && strings.HasSuffix(p, "/b.txt") {
			return errors.New("読めない")
		}
		return nil
	}})

	for _, summary := range []bool{false, true} {
		var out, errOut bytes.Buffer
		failed, err := printHashes(context.Background(), &out, &errOut, m, "/d",
			hashOptions{hashType: storage.SHA256, summary: summary, workers: 1})
		if err != nil {
			t.Fatalf("printHashes: %v", err)
		}
		if failed != 1 {
			t.Errorf("summary=%v: failed = %d, want 1", summary, failed)
		}
		if !strings.Contains(errOut.String(), "sub/b.txt") {
			t.Errorf("summary=%v: 失敗した件が標準エラーにない: %q", summary, errOut.String())
		}
		want := sha256Of("a") + "  a.txt\n"
		if summary {
			want = ""
		}
		if out.String() != want {
			t.Errorf("summary=%v: 出力 %q, want %q", summary, out.String(), want)
		}
	}
}

func TestParseHashType(t *testing.T) {
	for _, s := range []string{"sha256", "SHA1", "md5", "dropbox"} {
		if _, err := parseHashType(s); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
	for _, s := range []string{"quickxor", "crc32", ""} {
		if _, err := parseHashType(s); err == nil {
			t.Errorf("%q が受け入れられた", s)
		}
	}
}
