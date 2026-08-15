#!/usr/bin/env bash
#
# Android(arm64) 向けの hbg をビルドします。
#
#   ./scripts/build_android_arm64.sh [出力先]
#
# 既定の出力先はリポジトリ直下の hbg-android-arm64 です。
#
# ── なぜスクリプトにしてあるか ────────────────────────────────
#
# Android 向けは CGO_ENABLED=1 でなければなりません。切ると、
# ビルドは通り、ARM64 の ELF もできて、hbg version まで動くのに、
# 名前解決だけができないバイナリができあがります。
#
# Android には /etc/resolv.conf が無く、純 Go のリゾルバは
# 127.0.0.1:53 を引きにいって connection refused になります。
# Go は android では cgo のリゾルバを使う作りですが、
# CGO_ENABLED=0 でクロスビルドすると cgo リゾルバが入りません。
#
# 手でコマンドを組むと CGO の指定を落としても気づけないので、
# ここで固定し、できたものを検査します。
# ────────────────────────────────────────────────────────

set -euo pipefail

# 出力を検査する。作れたことは正しさの証明にならない。
verify() {
    local out=$1

    # ELF の magic と、e_machine(オフセット 0x12)が 0xB7 = AArch64 であること。
    local head
    head=$(od -An -tx1 -N20 "$out" | tr -d ' \n')
    case "$head" in
    7f454c46*) ;;
    *)
        echo "ビルドしたものが ELF ではありません: $out" >&2
        echo "  Windows 上で cgo を使おうとすると、Go が Windows の" >&2
        echo "  コンパイラへ落ちて PE を吐きます。WSL などの Linux で実行してください。" >&2
        return 1
        ;;
    esac
    if [ "${head:36:4}" != "b700" ]; then
        echo "ビルドしたものが AArch64 ではありません: $out" >&2
        return 1
    fi

    # cgo が実際に入っているか。ここが本題。
    if ! go version -m "$out" | grep -q 'CGO_ENABLED=1'; then
        echo "ビルドしたものに cgo が入っていません: $out" >&2
        echo "  このバイナリは起動しますが名前解決ができません。" >&2
        return 1
    fi
}

repo_root=$(cd "$(dirname "$0")/.." && pwd)
out=${1:-"$repo_root/hbg-android-arm64"}

ndk=${NDK:-${ANDROID_NDK_HOME:-}}
if [ -z "$ndk" ]; then
    echo "Android NDK の場所が分かりません。NDK か ANDROID_NDK_HOME を設定してください。" >&2
    echo "  例: export NDK=\$HOME/Android/ndk/android-ndk-r26d" >&2
    exit 1
fi

# クロスコンパイラは Linux 版しか同梱されていないので、Linux でしか動きません。
cc="$ndk/toolchains/llvm/prebuilt/linux-x86_64/bin/aarch64-linux-android21-clang"
if [ ! -x "$cc" ]; then
    echo "NDK のクロスコンパイラが見つかりません: $cc" >&2
    echo "  NDK のパスが正しいか、Linux(WSL を含む)で実行しているかを確かめてください。" >&2
    exit 1
fi

echo "NDK:   $ndk"
echo "出力:  $out"

CGO_ENABLED=1 \
    GOOS=android \
    GOARCH=arm64 \
    CC="$cc" \
    go build -buildvcs=true -trimpath -ldflags "-s -w" -o "$out" "$repo_root/cmd/hbg"

verify "$out"

echo "できました: $out"
go version -m "$out" | grep -E '^\s+build\s+(GOOS|GOARCH|CGO_ENABLED|vcs\.revision|vcs\.modified)'
