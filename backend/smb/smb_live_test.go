//go:build live

package smb

import (
	"context"
	"os"
	"testing"

	"github.com/mt3hr/hbg/storage"
	"github.com/mt3hr/hbg/storage/storagetest"
)

// 実物の SMB サーバーに対する適合性テストです。
//
// 通信そのものを確かめられる唯一の試験なので、SMB の相手が
// 用意できるときは通してください。
//
//	docker run --rm -p 1445:445 \
//	  -e "USER=試験利用者;ひみつ" -e "SHARE=共有;/mount;;;;試験利用者" \
//	  dperson/samba -p
//
//	HBG_TEST_SMB_HOST=127.0.0.1 \
//	HBG_TEST_SMB_PORT=1445 \
//	HBG_TEST_SMB_SHARE=共有 \
//	HBG_TEST_SMB_USER=試験利用者 \
//	HBG_TEST_SMB_PASSWORD=ひみつ \
//	go test -tags live ./backend/smb/
func TestLiveConformance(t *testing.T) {
	host := os.Getenv("HBG_TEST_SMB_HOST")
	share := os.Getenv("HBG_TEST_SMB_SHARE")
	if host == "" || share == "" {
		t.Skip("HBG_TEST_SMB_HOST と HBG_TEST_SMB_SHARE が指定されていないため飛ばします")
	}

	port := 0
	if raw := os.Getenv("HBG_TEST_SMB_PORT"); raw != "" {
		for _, c := range raw {
			port = port*10 + int(c-'0')
		}
	}

	storagetest.Run(t, storagetest.Harness{
		NewStorage: func(t *testing.T) (storage.Storage, string) {
			s, err := New(context.Background(), Config{
				Name:     "実物smb",
				Host:     host,
				Port:     port,
				Share:    share,
				User:     os.Getenv("HBG_TEST_SMB_USER"),
				Password: os.Getenv("HBG_TEST_SMB_PASSWORD"),
				Domain:   os.Getenv("HBG_TEST_SMB_DOMAIN"),
			})
			if err != nil {
				t.Fatalf("接続できません: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })

			// 前回の残骸があれば片付けてから始める。
			root := "/hbg試験"
			ctx := context.Background()
			_ = s.Purge(ctx, root)
			if err := s.Mkdir(ctx, root); err != nil {
				t.Fatalf("試験用のディレクトリを作れません: %v", err)
			}
			t.Cleanup(func() { _ = s.Purge(context.Background(), root) })

			return s, root
		},
		// 実物とのやりとりは遅いので、件数を抑える。
		LargeDirCount: 50,
	})
}
