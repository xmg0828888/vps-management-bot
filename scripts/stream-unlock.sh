#!/usr/bin/env bash
#
# Stream Unlock Installer — 流媒体 / AI 解锁一键脚本
# 版本: 2.0
#
# 两种角色:
#   解锁机 (unlocker)   : 安装 sniproxy, 接收被解锁机的 TLS 流量, 在本机出口转发
#   被解锁机 (client)   : 安装 smartdns, 把指定服务的域名解析到解锁机 IP
#
# 使用:
#   bash <(curl -sL mjjtop.com/unlock)                 # 交互菜单
#   bash <(curl -sL mjjtop.com/unlock) status          # 查状态 (非交互)
#   bash <(curl -sL mjjtop.com/unlock) test netflix.com
#   bash <(curl -sL mjjtop.com/unlock) uninstall [--yes]
#   bash <(curl -sL mjjtop.com/unlock) --help
#
# 设计说明:
#   * sniproxy 只做 SNI 转发, 不关心 DNS. 出口到服务的真实 IP 解析由解锁机本地
#     DNS 处理, 所以解锁机必须位于能原生访问目标服务的区域 (例如日本机解锁 HBO).
#   * 被解锁机的 smartdns 把 netflix.com 等域名 address 到解锁机 IP,
#     客户端 TLS 握手时的 SNI 被 sniproxy 看见后按 table 规则转发到真实目的地.
#   * 解锁机不需要开放 53 端口 (不做 DNS). 只需要开 80/443 给被解锁机 IP 白名单.
#
# 兼容: Debian 11/12/13, Ubuntu 20/22/24, CentOS 8+, Rocky/Alma, Arch
# 仅 IPv4 转发; IPv6-only 主机会在启动时报错退出.
#
set -Eeuo pipefail

# ============ 常量 ============
readonly SCRIPT_VERSION="2.0"
readonly LOG_FILE="/var/log/stream-unlock.log"
readonly BACKUP_ROOT="/etc/stream-unlock-backup"
readonly STATE_FILE="/etc/stream-unlock.state"
readonly SNIPROXY_CONF="/etc/sniproxy.conf"
readonly SNIPROXY_SERVICE="/etc/systemd/system/sniproxy.service"
readonly SMARTDNS_CONF="/etc/smartdns/smartdns.conf"
readonly SMARTDNS_REPO="pymumu/smartdns"
readonly SNIPROXY_REPO="https://github.com/dlundquist/sniproxy.git"
readonly SNIPROXY_TAG="master"     # upstream 只发 master; 编译时固定到一个 commit 以保证可重现

# 颜色
if [[ -t 1 ]]; then
    readonly RED=$'\033[0;31m'
    readonly GREEN=$'\033[0;32m'
    readonly YELLOW=$'\033[1;33m'
    readonly BLUE=$'\033[0;34m'
    readonly BOLD=$'\033[1m'
    readonly NC=$'\033[0m'
else
    readonly RED='' GREEN='' YELLOW='' BLUE='' BOLD='' NC=''
fi

# 服务域名 (key 必须和菜单号对应)
declare -A SERVICE_DOMAINS
SERVICE_DOMAINS[netflix_disney]="netflix.com netflix.net nflximg.com nflximg.net nflxvideo.net nflxext.com nflxso.net disneyplus.com disney-plus.net dssott.com bamgrid.com"
SERVICE_DOMAINS[youtube_google]="youtube.com youtu.be ytimg.com googlevideo.com youtubei.googleapis.com youtube-nocookie.com"
SERVICE_DOMAINS[ai]="openai.com chatgpt.com ai.com oaistatic.com oaiusercontent.com auth0.openai.com anthropic.com claude.ai statsig.anthropic.com gemini.google.com generativelanguage.googleapis.com copilot.microsoft.com perplexity.ai midjourney.com character.ai poe.com"
SERVICE_DOMAINS[tiktok]="tiktok.com tiktokv.com tiktokcdn.com tiktokcdn-us.com byteoversea.com musical.ly"
SERVICE_DOMAINS[hbo]="hbomax.com hbo.com hbogo.com hbonow.com max.com"
SERVICE_DOMAINS[prime]="primevideo.com aiv-cdn.net aiv-delivery.net media-amazon.com"
SERVICE_DOMAINS[spotify]="spotify.com scdn.co spotifycdn.com spotifycdn.net"

# 全局状态
OS=""              # debian / rhel / arch
PKG=""             # apt / dnf / pacman
OS_VERSION=""
SELECTED_SERVICES=()
SELECTED_IPS=()    # 解锁机白名单用的被解锁机 IP
FORCE=0            # --force 跳过安全检查

# ============ 日志 / 错误 ============
log() {
    local level="$1"; shift
    local msg="$*"
    local line
    line="$(date '+%Y-%m-%d %H:%M:%S') [$level] $msg"
    # 写日志文件 (非 root 时可能失败, 忽略)
    if [[ -w "${LOG_FILE%/*}" ]] || [[ -w "$LOG_FILE" ]]; then
        echo "$line" >> "$LOG_FILE" 2>/dev/null || true
    fi
    case "$level" in
        ERR)  echo -e "${RED}[ERR]${NC} $msg" >&2 ;;
        WARN) echo -e "${YELLOW}[WARN]${NC} $msg" >&2 ;;
        OK)   echo -e "${GREEN}[OK]${NC}  $msg" ;;
        INFO) echo -e "${BLUE}[..]${NC}  $msg" ;;
        *)    echo "$msg" ;;
    esac
}
info()  { log INFO "$@"; }
ok()    { log OK   "$@"; }
warn()  { log WARN "$@"; }
err()   { log ERR  "$@"; }
die()   { err "$@"; exit 1; }

on_err() {
    local rc=$? cmd=${BASH_COMMAND:-?} line=${BASH_LINENO[0]:-?}
    err "第 ${line} 行执行失败 (退出码 $rc): $cmd"
    err "请查看日志: $LOG_FILE"
    exit $rc
}
trap on_err ERR

# ============ 预检 ============
check_root() {
    [[ $EUID -eq 0 ]] || die "请以 root 运行 (sudo bash <(curl -sL mjjtop.com/unlock))"
}

detect_os() {
    if [[ ! -f /etc/os-release ]]; then
        die "无法识别系统 (缺少 /etc/os-release)"
    fi
    # shellcheck disable=SC1091
    . /etc/os-release
    OS_VERSION="${VERSION_ID:-unknown}"
    case "${ID:-}${ID_LIKE:-}" in
        *debian*|*ubuntu*) OS="debian"; PKG="apt" ;;
        *rhel*|*centos*|*rocky*|*alma*|*fedora*) OS="rhel"; PKG="$(command -v dnf >/dev/null && echo dnf || echo yum)" ;;
        *arch*) OS="arch"; PKG="pacman" ;;
        *)
            # 再按命令探测一次
            if command -v apt-get >/dev/null; then OS="debian"; PKG="apt"
            elif command -v dnf >/dev/null; then OS="rhel"; PKG="dnf"
            elif command -v yum >/dev/null; then OS="rhel"; PKG="yum"
            elif command -v pacman >/dev/null; then OS="arch"; PKG="pacman"
            else die "不支持的发行版: ${ID:-unknown}"
            fi
            ;;
    esac
    info "系统: ${ID:-?} ${OS_VERSION} (family=$OS, pkg=$PKG)"
}

check_ipv4() {
    # 需要至少一个全球可达 IPv4; 允许 NAT 后面的机器 (家庭/内网) 但提示
    local ipv4
    ipv4=$(curl -4 -fsS --max-time 5 https://api.ipify.org 2>/dev/null || true)
    if [[ -z "$ipv4" ]]; then
        warn "未检测到 IPv4 出口; 如果本机只有 IPv6, 此脚本无法工作"
        if [[ $FORCE -ne 1 ]]; then
            die "加 --force 可强行继续"
        fi
    else
        info "IPv4 公网地址: $ipv4"
    fi
    echo "$ipv4"
}

get_public_ip() {
    local ip
    for src in "https://api.ipify.org" "https://ip.sb" "https://ifconfig.me"; do
        ip=$(curl -4 -fsS --max-time 5 "$src" 2>/dev/null | tr -d '[:space:]' || true)
        if [[ -n "$ip" ]] && [[ "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "$ip"; return 0
        fi
    done
    return 1
}

# ============ 备份 / 回滚 ============
ensure_backup_dir() {
    mkdir -p "$BACKUP_ROOT"
    chmod 700 "$BACKUP_ROOT"
}

snapshot_configs() {
    ensure_backup_dir
    local stamp snap
    stamp="$(date +%Y%m%d-%H%M%S)"
    snap="$BACKUP_ROOT/$stamp"
    mkdir -p "$snap"
    local f
    for f in "$SNIPROXY_CONF" "$SMARTDNS_CONF" /etc/resolv.conf "$SNIPROXY_SERVICE" "$STATE_FILE"; do
        [[ -e "$f" ]] && cp -a "$f" "$snap/" 2>/dev/null || true
    done
    echo "$snap" > "$BACKUP_ROOT/.latest"
    info "已备份到 $snap"
}

restore_latest() {
    [[ -f "$BACKUP_ROOT/.latest" ]] || { warn "没有可用备份"; return 1; }
    local snap; snap=$(cat "$BACKUP_ROOT/.latest")
    [[ -d "$snap" ]] || { warn "备份目录不存在: $snap"; return 1; }
    info "从 $snap 恢复"
    local name
    for f in "$snap"/*; do
        [[ -e "$f" ]] || continue
        name="$(basename "$f")"
        case "$name" in
            sniproxy.conf) cp -a "$f" "$SNIPROXY_CONF" ;;
            smartdns.conf) cp -a "$f" "$SMARTDNS_CONF" ;;
            resolv.conf)   cp -a "$f" /etc/resolv.conf ;;
            sniproxy.service) cp -a "$f" "$SNIPROXY_SERVICE" ;;
        esac
    done
    systemctl daemon-reload 2>/dev/null || true
    ok "已恢复最近一次备份"
}

save_state() {
    local role="$1"; shift || true
    cat > "$STATE_FILE" <<EOF
role=$role
version=$SCRIPT_VERSION
installed_at=$(date -Iseconds)
$*
EOF
    chmod 600 "$STATE_FILE"
}

get_state() {
    local key="$1"
    [[ -f "$STATE_FILE" ]] || return 1
    grep -E "^${key}=" "$STATE_FILE" 2>/dev/null | head -1 | cut -d= -f2-
}

# ============ 公共 UI ============
print_banner() {
    echo -e "${BLUE}"
    cat <<'B'
╔════════════════════════════════════════════╗
║   Stream Unlock Installer v2.0             ║
║   流媒体 / AI 解锁一键脚本                   ║
╚════════════════════════════════════════════╝
B
    echo -e "${NC}"
}

pkg_install() {
    # $@ = packages
    case "$PKG" in
        apt)
            DEBIAN_FRONTEND=noninteractive apt-get update -qq
            DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "$@"
            ;;
        dnf|yum) "$PKG" install -y "$@" ;;
        pacman) pacman -Sy --noconfirm "$@" ;;
    esac
}

# ============ 防火墙 ============
fw_has_iptables_drop_policy() {
    command -v iptables >/dev/null || return 1
    # INPUT / FORWARD 是 DROP 就算
    iptables -S 2>/dev/null | grep -Eq '^-P (INPUT|FORWARD) DROP'
}

fw_allow_ssh_first() {
    # 在启用 ufw / firewalld 之前无条件保证 SSH 不被锁
    local ssh_port
    ssh_port="$(sshd -T 2>/dev/null | awk '/^port /{print $2; exit}')"
    [[ -z "$ssh_port" ]] && ssh_port=22
    case "$OS" in
        debian|arch)
            command -v ufw >/dev/null || return 0
            ufw allow "${ssh_port}/tcp" >/dev/null 2>&1 || true
            ok "已放行 SSH (${ssh_port}/tcp)"
            ;;
        rhel)
            command -v firewall-cmd >/dev/null || return 0
            firewall-cmd --permanent --add-port="${ssh_port}/tcp" >/dev/null 2>&1 || true
            ;;
    esac
}

fw_enable_unlocker() {
    # 放 80/443 给指定 IP, 启用防火墙
    local ip
    if fw_has_iptables_drop_policy && [[ $FORCE -ne 1 ]]; then
        warn "检测到 iptables 已有 DROP 策略; 启用 ufw 可能覆盖现有规则"
        warn "如果你清楚自己在做什么, 用 --force 跳过此检查"
        return 1
    fi
    fw_allow_ssh_first
    case "$OS" in
        debian|arch)
            for ip in "${SELECTED_IPS[@]}"; do
                ufw allow from "$ip" to any port 80 proto tcp  >/dev/null
                ufw allow from "$ip" to any port 443 proto tcp >/dev/null
                ok "放行 $ip -> 80,443"
            done
            ufw --force enable >/dev/null
            ;;
        rhel)
            for ip in "${SELECTED_IPS[@]}"; do
                firewall-cmd --permanent \
                  --add-rich-rule="rule family=ipv4 source address=$ip port port=80 protocol=tcp accept" >/dev/null
                firewall-cmd --permanent \
                  --add-rich-rule="rule family=ipv4 source address=$ip port port=443 protocol=tcp accept" >/dev/null
                ok "放行 $ip -> 80,443"
            done
            firewall-cmd --reload >/dev/null
            ;;
    esac
}

# ============ sniproxy 安装 ============
sniproxy_detect_binary() {
    local b
    for b in /usr/sbin/sniproxy /usr/local/sbin/sniproxy /usr/bin/sniproxy /usr/local/bin/sniproxy; do
        [[ -x "$b" ]] && { echo "$b"; return 0; }
    done
    # PATH 兜底
    command -v sniproxy 2>/dev/null || return 1
}

sniproxy_write_systemd_unit() {
    local bin="$1"
    cat > "$SNIPROXY_SERVICE" <<EOF
[Unit]
Description=sniproxy (TLS SNI forwarder)
After=network.target
Documentation=https://github.com/dlundquist/sniproxy

[Service]
Type=forking
ExecStart=$bin -c $SNIPROXY_CONF
PIDFile=/var/run/sniproxy.pid
Restart=on-failure
RestartSec=3
# 最低权限
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE
NoNewPrivileges=yes
ProtectSystem=full
ProtectHome=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
}

sniproxy_build_from_source() {
    info "从源码编译 sniproxy..."
    local build_deps
    case "$OS" in
        debian) build_deps=(build-essential autoconf automake libtool libev-dev libpcre2-dev libudns-dev pkg-config git ca-certificates) ;;
        rhel)   build_deps=(gcc make autoconf automake libtool libev-devel pcre2-devel udns-devel pkgconfig git ca-certificates) ;;
        arch)   build_deps=(base-devel libev pcre2 udns pkgconf git) ;;
    esac
    pkg_install "${build_deps[@]}"
    local src=/usr/local/src/sniproxy
    rm -rf "$src"
    git clone --depth 1 "$SNIPROXY_REPO" "$src"
    pushd "$src" >/dev/null
    ./autogen.sh
    ./configure --prefix=/usr/local --sysconfdir=/etc
    make -j"$(nproc)"
    make install
    popd >/dev/null
    rm -rf "$src"
}

sniproxy_write_config() {
    cat > "$SNIPROXY_CONF" <<'EOF'
# sniproxy.conf - stream-unlock managed
# 只解析 SNI 转发, 不做 DNS

user daemon
pidfile /var/run/sniproxy.pid

error_log {
    syslog daemon
    priority notice
}

listen 80 {
    proto http
    access_log off
}

listen 443 {
    proto tls
    access_log off
}

table {
EOF
    local svc domain line
    for svc in "${SELECTED_SERVICES[@]}"; do
        [[ -n "${SERVICE_DOMAINS[$svc]:-}" ]] || continue
        echo "    # --- $svc ---" >> "$SNIPROXY_CONF"
        for domain in ${SERVICE_DOMAINS[$svc]}; do
            # 精确匹配 + 子域通配
            line="    .*\\.${domain//./\\.}$ *"
            echo "$line" >> "$SNIPROXY_CONF"
            line="    ^${domain//./\\.}$ *"
            echo "$line" >> "$SNIPROXY_CONF"
        done
    done
    echo "}" >> "$SNIPROXY_CONF"
    ok "sniproxy.conf 已写入 (服务数: ${#SELECTED_SERVICES[@]})"
}

install_sniproxy() {
    snapshot_configs
    info "安装 sniproxy..."
    case "$OS" in
        debian)
            if ! DEBIAN_FRONTEND=noninteractive apt-get install -y sniproxy 2>/dev/null; then
                sniproxy_build_from_source
            fi
            ;;
        rhel)
            "$PKG" install -y epel-release 2>/dev/null || true
            "$PKG" install -y sniproxy 2>/dev/null || sniproxy_build_from_source
            ;;
        arch)
            pacman -Sy --noconfirm sniproxy 2>/dev/null || sniproxy_build_from_source
            ;;
    esac
    local bin; bin="$(sniproxy_detect_binary)" || die "sniproxy 安装后找不到二进制"
    info "sniproxy 二进制: $bin"
    sniproxy_write_systemd_unit "$bin"
    sniproxy_write_config
    systemctl enable sniproxy >/dev/null
    systemctl restart sniproxy
    sleep 1
    systemctl is-active --quiet sniproxy \
        && ok "sniproxy 运行中" \
        || die "sniproxy 启动失败, 查看 journalctl -u sniproxy"
}

# ============ smartdns 安装 ============
smartdns_detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)  echo "x86_64-linux-all" ;;
        aarch64|arm64) echo "aarch64-linux-all" ;;
        armv7l|armhf)  echo "arm-linux-gnueabihf-all" ;;
        *) return 1 ;;
    esac
}

smartdns_github_asset() {
    local suffix="$1"
    # 取 latest 的 tar.gz asset; 不命中就退出
    curl -fsSL "https://api.github.com/repos/$SMARTDNS_REPO/releases/latest" 2>/dev/null \
        | grep -oE '"browser_download_url":[[:space:]]*"[^"]+"' \
        | cut -d'"' -f4 \
        | grep -E "${suffix}\.tar\.gz$" \
        | head -1
}

install_smartdns_manual() {
    local arch url tmp
    arch="$(smartdns_detect_arch)" || die "未知架构 $(uname -m), 请手动安装 smartdns"
    info "查询 smartdns 最新版本 ($arch)..."
    url="$(smartdns_github_asset "$arch")" || true
    if [[ -z "$url" ]]; then
        die "GitHub API 没找到 $arch 的 smartdns 资源; 检查网络或手动下载"
    fi
    info "下载: $url"
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' RETURN
    curl -fsSL "$url" -o "$tmp/smartdns.tar.gz"
    tar -C "$tmp" -xzf "$tmp/smartdns.tar.gz"
    local installer
    installer="$(find "$tmp" -maxdepth 3 -name install -type f | head -1)"
    if [[ -n "$installer" ]]; then
        (cd "$(dirname "$installer")" && bash install -i)
    else
        # 手动放置
        local bin; bin="$(find "$tmp" -maxdepth 4 -name smartdns -type f | head -1)"
        [[ -n "$bin" ]] || die "解压后未找到 smartdns 二进制"
        install -m 0755 "$bin" /usr/sbin/smartdns
        mkdir -p /etc/smartdns
        cat > /etc/systemd/system/smartdns.service <<'EOF'
[Unit]
Description=SmartDNS
After=network.target

[Service]
Type=forking
ExecStart=/usr/sbin/smartdns -p /var/run/smartdns.pid -c /etc/smartdns/smartdns.conf
PIDFile=/var/run/smartdns.pid
Restart=on-failure

[Install]
WantedBy=multi-user.target
EOF
        systemctl daemon-reload
    fi
    ok "smartdns 手动安装完成"
}

install_smartdns() {
    snapshot_configs
    if [[ "$OS" == "debian" ]] && DEBIAN_FRONTEND=noninteractive apt-get install -y smartdns 2>/dev/null; then
        ok "通过 apt 安装 smartdns 成功"
    else
        warn "发行版仓库没有 smartdns 或安装失败, 走手动下载"
        install_smartdns_manual
    fi
    [[ -d /etc/smartdns ]] || mkdir -p /etc/smartdns
    [[ -f "$SMARTDNS_CONF" ]] || cat > "$SMARTDNS_CONF" <<'EOF'
# smartdns.conf - stream-unlock managed
bind :53
bind [::]:53

# 上游
server 1.1.1.1
server 8.8.8.8
server 223.5.5.5
server 119.29.29.29

cache-size 4096
speed-check-mode ping,tcp:443
serve-expired yes
log-level warn
log-file /var/log/smartdns.log
log-size 10m
log-num 2
EOF
    ok "smartdns 基础配置就绪"
}

smartdns_add_service() {
    local unlocker_ip="$1" svc="$2"
    [[ -n "${SERVICE_DOMAINS[$svc]:-}" ]] || { warn "未知服务 $svc"; return 1; }
    # 去除旧条目 (同一服务同一解锁机重复写会污染)
    local marker_begin="# >>> stream-unlock:$svc"
    local marker_end="# <<< stream-unlock:$svc"
    if grep -Fq -- "$marker_begin" "$SMARTDNS_CONF"; then
        sed -i.bak "/$marker_begin/,/$marker_end/d" "$SMARTDNS_CONF"
    fi
    {
        echo ""
        echo "$marker_begin ($(date -Iseconds))"
        local domain
        for domain in ${SERVICE_DOMAINS[$svc]}; do
            echo "address /$domain/$unlocker_ip"
        done
        echo "$marker_end"
    } >> "$SMARTDNS_CONF"
    ok "smartdns 已添加 $svc -> $unlocker_ip"
}

configure_client_resolv() {
    # 把系统 DNS 指向 127.0.0.1
    # 处理 systemd-resolved / NetworkManager, 不再用 chattr
    if systemctl is-active --quiet systemd-resolved 2>/dev/null; then
        warn "检测到 systemd-resolved 在运行"
        echo -e "${YELLOW}是否禁用 systemd-resolved 让 smartdns 接管 53 端口? [y/N]${NC}"
        local ans; read -r ans
        if [[ "$ans" =~ ^[Yy]$ ]]; then
            systemctl disable --now systemd-resolved
            rm -f /etc/resolv.conf
        else
            warn "未禁用 systemd-resolved; smartdns 会试图绑 53 但可能失败"
        fi
    fi
    if [[ -d /etc/NetworkManager/conf.d ]]; then
        cat > /etc/NetworkManager/conf.d/90-stream-unlock.conf <<'EOF'
[main]
dns=none
EOF
        systemctl reload NetworkManager 2>/dev/null || true
    fi
    # 写 resolv.conf (如果是 symlink 先删)
    [[ -L /etc/resolv.conf ]] && rm -f /etc/resolv.conf
    cat > /etc/resolv.conf <<'EOF'
# Managed by stream-unlock
nameserver 127.0.0.1
options edns0 timeout:2 attempts:2
EOF
    ok "系统 DNS 已指向 127.0.0.1"
}

restart_smartdns() {
    systemctl daemon-reload
    systemctl enable smartdns >/dev/null
    systemctl restart smartdns
    sleep 1
    systemctl is-active --quiet smartdns \
        && ok "smartdns 运行中" \
        || die "smartdns 启动失败, 查看 journalctl -u smartdns"
}

# ============ 服务选择 ============
select_services() {
    SELECTED_SERVICES=()
    echo ""
    echo -e "${YELLOW}选择要解锁的服务 (可多选, 空格分隔):${NC}"
    echo "  1) Netflix + Disney+"
    echo "  2) YouTube + Google"
    echo "  3) AI 全家桶 (ChatGPT/Claude/Gemini/Copilot/Perplexity/...)"
    echo "  4) TikTok"
    echo "  5) HBO Max"
    echo "  6) Prime Video"
    echo "  7) Spotify"
    echo "  8) 全部"
    echo ""
    echo -e "${GREEN}示例: 3 只解锁 AI; 1 2 4 解锁 Netflix+YouTube+TikTok${NC}"
    local choices c
    read -r -p "请输入选项: " choices
    for c in $choices; do
        case "$c" in
            1) SELECTED_SERVICES+=(netflix_disney) ;;
            2) SELECTED_SERVICES+=(youtube_google) ;;
            3) SELECTED_SERVICES+=(ai) ;;
            4) SELECTED_SERVICES+=(tiktok) ;;
            5) SELECTED_SERVICES+=(hbo) ;;
            6) SELECTED_SERVICES+=(prime) ;;
            7) SELECTED_SERVICES+=(spotify) ;;
            8) SELECTED_SERVICES=(netflix_disney youtube_google ai tiktok hbo prime spotify); break ;;
            *) warn "忽略无效选项: $c" ;;
        esac
    done
    [[ ${#SELECTED_SERVICES[@]} -gt 0 ]] || die "没选任何服务"
    info "已选: ${SELECTED_SERVICES[*]}"
}

select_client_ips() {
    SELECTED_IPS=()
    echo ""
    echo -e "${YELLOW}输入允许访问本解锁机的被解锁机 IP (输入 done 结束):${NC}"
    local ip
    while true; do
        read -r -p "IP> " ip
        [[ "$ip" == "done" ]] && break
        [[ -z "$ip" ]] && continue
        if [[ ! "$ip" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]]; then
            warn "不是合法 IPv4: $ip"; continue
        fi
        SELECTED_IPS+=("$ip")
        ok "加入白名单: $ip"
    done
    [[ ${#SELECTED_IPS[@]} -gt 0 ]] || die "白名单不能为空, 否则解锁机只能自用"
}

# ============ 菜单: 解锁机 ============
menu_unlocker() {
    local my_ip; my_ip="$(get_public_ip || echo unknown)"
    print_banner
    echo -e "本机角色: ${BOLD}解锁机${NC}   IP: ${GREEN}$my_ip${NC}"
    echo ""
    echo "  1) 全新安装 (推荐)"
    echo "  2) 追加被解锁机 IP 到白名单"
    echo "  3) 追加解锁服务"
    echo "  4) 查看当前配置"
    echo "  5) 卸载"
    echo ""
    local c; read -r -p "请选择: " c
    case "$c" in
        1)
            select_services
            select_client_ips
            install_sniproxy
            fw_enable_unlocker
            save_state unlocker \
                "services=${SELECTED_SERVICES[*]}" \
                "clients=${SELECTED_IPS[*]}" \
                "ip=$my_ip"
            echo ""
            ok "解锁机配置完成"
            echo -e "${BOLD}下一步${NC}:"
            echo "  1) 在被解锁机运行: bash <(curl -sL mjjtop.com/unlock)"
            echo "  2) 选 [被解锁机] 模式, 输入解锁机 IP: $my_ip"
            echo "  3) 验证: stream-unlock test netflix.com"
            ;;
        2) select_client_ips; fw_enable_unlocker ;;
        3) select_services; sniproxy_write_config; systemctl restart sniproxy; ok "已追加服务并重启" ;;
        4) cmd_status ;;
        5) cmd_uninstall ;;
        *) warn "无效选项" ;;
    esac
}

# ============ 菜单: 被解锁机 ============
menu_client() {
    local my_ip; my_ip="$(get_public_ip || echo unknown)"
    print_banner
    echo -e "本机角色: ${BOLD}被解锁机${NC}   IP: ${GREEN}$my_ip${NC}"
    echo ""
    echo "  1) 全新安装 smartdns + 分流 (推荐)"
    echo "  2) 追加分流服务"
    echo "  3) 查看当前配置"
    echo "  4) 测试解锁"
    echo "  5) 卸载"
    echo ""
    local c; read -r -p "请选择: " c
    case "$c" in
        1)
            install_smartdns
            local unlocker_ip
            read -r -p "输入解锁机 IP: " unlocker_ip
            [[ "$unlocker_ip" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}$ ]] || die "解锁机 IP 格式不对"
            select_services
            local svc
            for svc in "${SELECTED_SERVICES[@]}"; do
                smartdns_add_service "$unlocker_ip" "$svc"
            done
            configure_client_resolv
            restart_smartdns
            save_state client \
                "unlocker=$unlocker_ip" \
                "services=${SELECTED_SERVICES[*]}"
            echo ""
            ok "被解锁机配置完成"
            echo -e "${BOLD}测试:${NC} dig @127.0.0.1 netflix.com +short  (期望返回 $unlocker_ip)"
            echo -e "${BOLD}或:${NC}    curl -s https://www.netflix.com/title/80018499 -o /dev/null -w '%{http_code}\\n'"
            ;;
        2)
            local cur
            cur="$(get_state unlocker || true)"
            local unlocker_ip
            if [[ -n "$cur" ]]; then
                read -r -p "解锁机 IP [$cur]: " unlocker_ip
                unlocker_ip="${unlocker_ip:-$cur}"
            else
                read -r -p "输入解锁机 IP: " unlocker_ip
            fi
            select_services
            local svc
            for svc in "${SELECTED_SERVICES[@]}"; do
                smartdns_add_service "$unlocker_ip" "$svc"
            done
            restart_smartdns
            ;;
        3) cmd_status ;;
        4)
            read -r -p "要测试的域名 [netflix.com]: " dom
            cmd_test "${dom:-netflix.com}"
            ;;
        5) cmd_uninstall ;;
        *) warn "无效选项" ;;
    esac
}

# ============ 顶级主菜单 ============
menu_root() {
    print_banner
    local existing_role=""
    if [[ -f "$STATE_FILE" ]]; then
        existing_role="$(get_state role || true)"
        [[ -n "$existing_role" ]] && echo -e "${YELLOW}检测到本机已配置为: ${BOLD}$existing_role${NC}"
    fi
    echo ""
    echo "  1) 解锁机 (sniproxy)      - 提供出口给其他机器"
    echo "  2) 被解锁机 (smartdns)    - 把流量分流到解锁机"
    echo "  3) 状态 / 测试"
    echo "  4) 卸载"
    echo "  0) 退出"
    echo ""
    local c; read -r -p "请选择: " c
    case "$c" in
        1) menu_unlocker ;;
        2) menu_client ;;
        3) cmd_status ;;
        4) cmd_uninstall ;;
        0) exit 0 ;;
        *) warn "无效选项" ;;
    esac
}

# ============ 子命令: status ============
cmd_status() {
    echo -e "${BOLD}=== stream-unlock 状态 ===${NC}"
    if [[ ! -f "$STATE_FILE" ]]; then
        warn "未安装 (找不到 $STATE_FILE)"
        return 0
    fi
    cat "$STATE_FILE"
    echo ""
    local role; role="$(get_state role)"
    case "$role" in
        unlocker)
            systemctl is-active --quiet sniproxy && ok "sniproxy: active" || err "sniproxy: 未运行"
            echo -e "${BOLD}监听端口:${NC}"
            ss -tlnp 2>/dev/null | awk '$4 ~ /:(80|443)$/' | head -10
            echo -e "${BOLD}防火墙:${NC}"
            if command -v ufw >/dev/null && ufw status | grep -q Status; then
                ufw status | sed -n '1,20p'
            elif command -v firewall-cmd >/dev/null; then
                firewall-cmd --list-all | head -30
            fi
            ;;
        client)
            systemctl is-active --quiet smartdns && ok "smartdns: active" || err "smartdns: 未运行"
            local unlocker; unlocker="$(get_state unlocker)"
            echo -e "${BOLD}解锁机:${NC} $unlocker"
            echo -e "${BOLD}分流条目 (头 10):${NC}"
            grep -E '^address ' "$SMARTDNS_CONF" 2>/dev/null | head -10 || true
            echo -e "${BOLD}系统 DNS:${NC}"
            grep -E '^nameserver' /etc/resolv.conf 2>/dev/null
            ;;
    esac
}

# ============ 子命令: test ============
cmd_test() {
    local domain="${1:?用法: stream-unlock test <domain>}"
    echo -e "${BOLD}测试 $domain ${NC}"
    if command -v dig >/dev/null; then
        echo -e "${BLUE}[1] DNS 解析 (dig @127.0.0.1):${NC}"
        dig @127.0.0.1 "$domain" +short +time=3 || warn "DNS 解析失败"
    else
        echo -e "${BLUE}[1] DNS 解析 (getent):${NC}"
        getent hosts "$domain" || warn "DNS 解析失败"
    fi
    echo -e "${BLUE}[2] TLS 握手 + HTTP 状态:${NC}"
    local code
    code="$(curl -k -fsS --resolve "$domain:443:$(getent hosts "$domain" | awk '{print $1; exit}')" \
            -o /dev/null -w '%{http_code}' --max-time 8 "https://$domain/" 2>&1 || echo failed)"
    echo "  HTTP status: $code"
    echo -e "${BLUE}[3] SNI 转发 (TCP 443 可达性):${NC}"
    local target; target="$(getent hosts "$domain" | awk '{print $1; exit}')"
    if [[ -n "$target" ]]; then
        timeout 3 bash -c "</dev/tcp/$target/443" 2>/dev/null \
            && ok "TCP 443 可达 $target" \
            || err "TCP 443 不可达 $target"
    fi
}

# ============ 子命令: uninstall ============
cmd_uninstall() {
    local role; role="$(get_state role 2>/dev/null || true)"
    local confirm="${1:-}"
    if [[ "$confirm" != "--yes" ]]; then
        echo -e "${YELLOW}准备卸载 stream-unlock${NC} (role=${role:-未知})"
        read -r -p "确定? [y/N] " ans
        [[ "$ans" =~ ^[Yy]$ ]] || { info "取消"; return 0; }
    fi
    snapshot_configs
    case "$role" in
        unlocker)
            systemctl disable --now sniproxy 2>/dev/null || true
            rm -f "$SNIPROXY_SERVICE" "$SNIPROXY_CONF"
            systemctl daemon-reload || true
            # 源码装的 sniproxy 也删一下 (只删 /usr/local/)
            rm -f /usr/local/sbin/sniproxy /usr/local/bin/sniproxy
            # 包管理器
            case "$PKG" in
                apt) apt-get remove -y sniproxy 2>/dev/null || true ;;
                dnf|yum) "$PKG" remove -y sniproxy 2>/dev/null || true ;;
                pacman) pacman -Rns --noconfirm sniproxy 2>/dev/null || true ;;
            esac
            ;;
        client)
            systemctl disable --now smartdns 2>/dev/null || true
            rm -f /etc/systemd/system/smartdns.service
            systemctl daemon-reload || true
            rm -rf /etc/smartdns /var/log/smartdns.log
            rm -f /usr/sbin/smartdns /usr/local/sbin/smartdns
            case "$PKG" in
                apt) apt-get remove -y smartdns 2>/dev/null || true ;;
                dnf|yum) "$PKG" remove -y smartdns 2>/dev/null || true ;;
                pacman) pacman -Rns --noconfirm smartdns 2>/dev/null || true ;;
            esac
            # 还原 resolv.conf
            if [[ -f /etc/resolv.conf ]] && grep -q 'Managed by stream-unlock' /etc/resolv.conf; then
                cat > /etc/resolv.conf <<'EOF'
nameserver 1.1.1.1
nameserver 8.8.8.8
EOF
            fi
            rm -f /etc/NetworkManager/conf.d/90-stream-unlock.conf
            systemctl reload NetworkManager 2>/dev/null || true
            ;;
        *)
            warn "未知角色, 尝试清理所有可能的文件"
            ;;
    esac
    rm -f "$STATE_FILE"
    ok "卸载完成 (备份保留在 $BACKUP_ROOT, 可用 'stream-unlock rollback' 还原)"
}

# ============ 子命令: rollback ============
cmd_rollback() {
    restore_latest
    # 尽量重启服务
    systemctl restart sniproxy 2>/dev/null || true
    systemctl restart smartdns 2>/dev/null || true
}

# ============ 帮助 ============
show_help() {
    cat <<EOF
stream-unlock v$SCRIPT_VERSION  流媒体/AI 解锁工具

用法:
  stream-unlock                       交互菜单
  stream-unlock status                查看当前状态
  stream-unlock test <domain>         测试解锁 (dns/tls/tcp)
  stream-unlock uninstall [--yes]     卸载
  stream-unlock rollback              回滚到最近一次配置
  stream-unlock -h | --help           显示此帮助

一键用法 (curl):
  bash <(curl -sL mjjtop.com/unlock)
  bash <(curl -sL mjjtop.com/unlock) status
  bash <(curl -sL mjjtop.com/unlock) test netflix.com

环境变量:
  FORCE=1          跳过 iptables DROP 策略 / IPv6-only 安全检查

日志:   $LOG_FILE
备份:   $BACKUP_ROOT
状态:   $STATE_FILE
EOF
}

# ============ main ============
main() {
    check_root
    detect_os
    # 预先建日志
    mkdir -p "$(dirname "$LOG_FILE")" 2>/dev/null || true
    touch "$LOG_FILE" 2>/dev/null || true

    case "${1:-}" in
        ""|menu) check_ipv4 >/dev/null; menu_root ;;
        status) cmd_status ;;
        test)   shift; cmd_test "${1:-netflix.com}" ;;
        uninstall) shift || true; cmd_uninstall "${1:-}" ;;
        rollback) cmd_rollback ;;
        unlocker) check_ipv4 >/dev/null; menu_unlocker ;;
        client)   check_ipv4 >/dev/null; menu_client ;;
        -h|--help|help) show_help ;;
        *) show_help; exit 1 ;;
    esac
}

# 支持 --force 任意位置
for arg in "$@"; do
    [[ "$arg" == "--force" ]] && FORCE=1
done
main "$@"
