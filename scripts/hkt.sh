#!/usr/bin/env bash
set -u

# =========================================================
# 默认出站源地址管理器（精简美化版）
# 目标：
#   - 默认新建连接优先使用内网IP作为源地址
#   - 实际下一跳仍走当前公网网关
#   - 保留公网IP独立策略，避免SSH/公网入站回包异常
# =========================================================

[[ $EUID -eq 0 ]] || { echo "请使用 root 运行"; exit 1; }

STATE_DIR="/var/lib/default-src-ip"
STATE_FILE="$STATE_DIR/state.env"
APPLY_BIN="/usr/local/sbin/default-src-ip-apply"
SERVICE_NAME="default-src-ip.service"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}"
SYSCTL_FILE="/etc/sysctl.d/90-default-src-ip.conf"

mkdir -p "$STATE_DIR"

# ---------- 颜色（不使用红色） ----------
C_RESET='\033[0m'
C_BOLD='\033[1m'
C_DIM='\033[2m'
C_WHITE='\033[1;37m'
C_CYAN='\033[1;36m'
C_BLUE='\033[1;34m'
C_GREEN='\033[1;32m'
C_YELLOW='\033[1;33m'
C_GRAY='\033[0;37m'

# ---------- UI ----------
line() {
  printf "%b%s%b\n" "$C_GRAY" "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" "$C_RESET"
}

subline() {
  printf "%b%s%b\n" "$C_GRAY" "────────────────────────────────────────────────────────────" "$C_RESET"
}

header() {
  clear 2>/dev/null || true
  echo
}

section() {
  echo
  printf "  %b%s%b\n" "$C_CYAN$C_BOLD" "$1" "$C_RESET"
  subline
}

ok()   { printf "%b[OK]%b %s\n"   "$C_GREEN"  "$C_RESET" "$*"; }
info() { printf "%b[INFO]%b %s\n" "$C_CYAN"   "$C_RESET" "$*"; }
warn() { printf "%b[WARN]%b %s\n" "$C_YELLOW" "$C_RESET" "$*"; }

kv() {
  printf "  %b%-10s%b %s\n" "$C_GRAY" "$1" "$C_RESET" "$2"
}

menu_item() {
  local num="$1"
  local title="$2"
  printf "  %b[%s]%b %s\n" "$C_BLUE$C_BOLD" "$num" "$C_RESET" "$title"
}

pause() {
  echo
  read -r -p "按回车继续..." _
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "缺少命令: $1"
    exit 1
  }
}

for c in ip awk grep cut sed head tr sysctl systemctl curl ping; do
  need_cmd "$c"
done

# ---------- IP 类型判断 ----------
is_private_ipv4() {
  local ip="$1"
  [[ "$ip" =~ ^10\. ]] && return 0
  [[ "$ip" =~ ^192\.168\. ]] && return 0
  [[ "$ip" =~ ^172\.(1[6-9]|2[0-9]|3[0-1])\. ]] && return 0
  return 1
}

# ---------- 自动检测 ----------
detect_env() {
  IFACE="$(ip -4 route show default | awk 'NR==1{print $5}')"
  PUBLIC_GW="$(ip -4 route show default | awk 'NR==1{print $3}')"

  PUBLIC_IP=""
  PUBLIC_CIDR=""
  PRIVATE_IP=""
  PRIVATE_CIDR=""

  while read -r linebuf; do
    local cidr ip prefix
    cidr="$(awk '{print $4}' <<<"$linebuf")"
    ip="${cidr%/*}"
    prefix="${cidr#*/}"

    if is_private_ipv4 "$ip"; then
      if [[ -z "$PRIVATE_IP" ]]; then
        PRIVATE_IP="$ip"
        PRIVATE_CIDR="$prefix"
      fi
    else
      if [[ -z "$PUBLIC_IP" ]]; then
        PUBLIC_IP="$ip"
        PUBLIC_CIDR="$prefix"
      fi
    fi
  done < <(ip -o -4 addr show dev "$IFACE" scope global 2>/dev/null)

  [[ -n "${IFACE:-}" ]] || return 1
  [[ -n "${PUBLIC_GW:-}" ]] || return 1
  [[ -n "${PUBLIC_IP:-}" ]] || return 1
  [[ -n "${PRIVATE_IP:-}" ]] || return 1
  return 0
}

save_state() {
  cat > "$STATE_FILE" <<EOF_STATE
IFACE="$IFACE"
PUBLIC_GW="$PUBLIC_GW"
PUBLIC_IP="$PUBLIC_IP"
PUBLIC_CIDR="$PUBLIC_CIDR"
PRIVATE_IP="$PRIVATE_IP"
PRIVATE_CIDR="$PRIVATE_CIDR"
EOF_STATE
}

load_state_or_detect() {
  if [[ -f "$STATE_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$STATE_FILE"
    if [[ -n "${IFACE:-}" && -n "${PUBLIC_GW:-}" && -n "${PUBLIC_IP:-}" && -n "${PRIVATE_IP:-}" ]]; then
      return 0
    fi
  fi
  detect_env || return 1
  save_state
  return 0
}

refresh_state() {
  detect_env || return 1
  save_state
  return 0
}

# ---------- 当前模式 ----------
current_mode() {
  local def
  def="$(ip route show default 2>/dev/null | head -n1)"

  if [[ -n "${PRIVATE_IP:-}" ]] && grep -q "src ${PRIVATE_IP}" <<<"$def"; then
    echo "默认源地址 = 内网IP（172 优先）"
  elif [[ -n "${PUBLIC_IP:-}" ]] && grep -q "src ${PUBLIC_IP}" <<<"$def"; then
    echo "默认源地址 = 公网IP"
  else
    echo "默认源地址 = 未识别"
  fi
}

# ---------- 显示摘要 ----------
show_summary() {
  load_state_or_detect || {
    warn "自动识别失败，请检查默认路由、网卡与 IP 配置。"
    return 1
  }

  local mode
  mode="$(current_mode)"

  section "当前环境"
  kv "网卡" "${IFACE}"
  kv "公网IP" "${PUBLIC_IP}"
  kv "内网IP" "${PRIVATE_IP}"
  kv "公网网关" "${PUBLIC_GW}"
  kv "当前模式" "${mode}"
}

# ---------- 清理策略 ----------
clean_policy_only() {
  ip rule del pref 100 2>/dev/null || true
  ip rule del pref 110 2>/dev/null || true
  ip route flush table 100 2>/dev/null || true
  ip route flush table 200 2>/dev/null || true
  ip route flush cache 2>/dev/null || true
}

# ---------- 应用策略 ----------
apply_private_as_default_src() {
  refresh_state || {
    warn "自动识别失败，无法应用。"
    return 1
  }

  info "正在应用：默认新连接优先使用 ${PRIVATE_IP} 出站"
  info "下一跳保持：${PUBLIC_GW}"

  clean_policy_only

  # 主路由表：默认新连接使用内网IP为源
  ip route replace default via "${PUBLIC_GW}" dev "${IFACE}" src "${PRIVATE_IP}"

  # 表100：源地址=内网IP，明确走公网网关，源保持内网IP
  ip route replace default via "${PUBLIC_GW}" dev "${IFACE}" src "${PRIVATE_IP}" table 100

  # 表200：源地址=公网IP，明确走公网网关，源保持公网IP
  ip route replace default via "${PUBLIC_GW}" dev "${IFACE}" src "${PUBLIC_IP}" table 200

  # 策略规则
  ip rule add pref 100 from "${PRIVATE_IP}/32" table 100
  ip rule add pref 110 from "${PUBLIC_IP}/32" table 200

  ip route flush cache 2>/dev/null || true

  ok "应用完成"
  echo
  show_route_status
}

# ---------- 回滚 ----------
rollback_public_as_default_src() {
  refresh_state || {
    warn "自动识别失败，无法回滚。"
    return 1
  }

  info "正在恢复：默认新连接优先使用 ${PUBLIC_IP} 出站"

  clean_policy_only
  ip route replace default via "${PUBLIC_GW}" dev "${IFACE}" src "${PUBLIC_IP}"
  ip route flush cache 2>/dev/null || true

  ok "已恢复为公网IP默认出站"
  echo
  show_route_status
}

# ---------- 状态 ----------
show_route_status() {
  load_state_or_detect || {
    warn "自动识别失败"
    return 1
  }

  section "当前详细状态"
  kv "网卡" "${IFACE}"
  kv "公网IP" "${PUBLIC_IP}/${PUBLIC_CIDR}"
  kv "内网IP" "${PRIVATE_IP}/${PRIVATE_CIDR}"
  kv "公网网关" "${PUBLIC_GW}"
  kv "当前模式" "$(current_mode)"
  echo

  printf "%b主默认路由%b\n" "$C_CYAN" "$C_RESET"
  ip route show default | sed 's/^/  /'
  echo

  printf "%b策略规则%b\n" "$C_CYAN" "$C_RESET"
  ip rule | sed 's/^/  /'
  echo

  printf "%b表100（内网IP源）%b\n" "$C_CYAN" "$C_RESET"
  ip route show table 100 2>/dev/null | sed 's/^/  /'
  echo

  printf "%b表200（公网IP源）%b\n" "$C_CYAN" "$C_RESET"
  ip route show table 200 2>/dev/null | sed 's/^/  /'
  echo
}

# ---------- 连通性测试 ----------
test_now() {
  load_state_or_detect || {
    warn "自动识别失败"
    return 1
  }

  section "测试当前出站效果"

  printf "%b默认新连接选路%b\n" "$C_CYAN" "$C_RESET"
  ip route get 1.1.1.1 | sed 's/^/  /'
  echo

  printf "%b从内网IP出站选路%b\n" "$C_CYAN" "$C_RESET"
  ip route get 1.1.1.1 from "${PRIVATE_IP}" | sed 's/^/  /'
  echo

  printf "%b从公网IP出站选路%b\n" "$C_CYAN" "$C_RESET"
  ip route get 1.1.1.1 from "${PUBLIC_IP}" | sed 's/^/  /'
  echo

  printf "%bPing（绑定内网IP）%b\n" "$C_CYAN" "$C_RESET"
  ping -I "${PRIVATE_IP}" -c 3 1.1.1.1 || true
  echo

  printf "%b公网IP查询（绑定内网IP）%b\n" "$C_CYAN" "$C_RESET"
  curl -4 --interface "${PRIVATE_IP}" --connect-timeout 5 --max-time 10 https://api.ipify.org ; echo
  echo

  printf "%b公网IP查询（默认新连接）%b\n" "$C_CYAN" "$C_RESET"
  curl -4 --connect-timeout 5 --max-time 10 https://api.ipify.org ; echo
  echo
}

# ---------- 启动脚本 ----------
install_apply_bin() {
  cat > "$APPLY_BIN" <<'EOF_APPLY'
#!/usr/bin/env bash
set -u

STATE_FILE="/var/lib/default-src-ip/state.env"
[[ -f "$STATE_FILE" ]] || exit 1

# shellcheck disable=SC1090
source "$STATE_FILE"

ip rule del pref 100 2>/dev/null || true
ip rule del pref 110 2>/dev/null || true
ip route flush table 100 2>/dev/null || true
ip route flush table 200 2>/dev/null || true

ip route replace default via "${PUBLIC_GW}" dev "${IFACE}" src "${PRIVATE_IP}"
ip route replace default via "${PUBLIC_GW}" dev "${IFACE}" src "${PRIVATE_IP}" table 100
ip route replace default via "${PUBLIC_GW}" dev "${IFACE}" src "${PUBLIC_IP}" table 200

ip rule add pref 100 from "${PRIVATE_IP}/32" table 100
ip rule add pref 110 from "${PUBLIC_IP}/32" table 200

ip route flush cache 2>/dev/null || true
EOF_APPLY

  chmod +x "$APPLY_BIN"
}

install_sysctl() {
  cat > "$SYSCTL_FILE" <<'EOF_SYSCTL'
net.ipv4.conf.all.rp_filter = 2
net.ipv4.conf.default.rp_filter = 2
EOF_SYSCTL
  sysctl --system >/dev/null 2>&1 || true
}

install_service() {
  refresh_state || {
    warn "自动识别失败，无法安装开机自启。"
    return 1
  }

  install_apply_bin
  install_sysctl

  cat > "$SERVICE_FILE" <<EOF_SERVICE
[Unit]
Description=Use private IP as default source for outbound traffic
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=${APPLY_BIN}

[Install]
WantedBy=multi-user.target
EOF_SERVICE

  systemctl daemon-reload
  systemctl enable --now "${SERVICE_NAME}"

  ok "已安装开机自动应用"
  echo
  systemctl status "${SERVICE_NAME}" --no-pager || true
}

remove_service() {
  systemctl disable --now "${SERVICE_NAME}" 2>/dev/null || true
  rm -f "$SERVICE_FILE"
  rm -f "$APPLY_BIN"
  rm -f "$SYSCTL_FILE"
  systemctl daemon-reload
  sysctl --system >/dev/null 2>&1 || true
  ok "已移除开机自动应用"
}

# ---------- 卸载 ----------
full_uninstall() {
  warn "开始卸载并恢复为公网IP默认出站"
  remove_service
  rollback_public_as_default_src || true
  rm -f "$STATE_FILE"
  ok "卸载完成"
}

# ---------- 菜单 ----------
menu() {
  while true; do
    header
    show_summary

    section "功能菜单"
    menu_item 1 "重新自动识别环境"
    menu_item 2 "应用内网IP默认出站"
    menu_item 3 "测试当前出站效果"
    menu_item 4 "查看当前详细状态"
    menu_item 5 "回滚为公网IP默认出站"
    menu_item 6 "安装开机自动应用"
    menu_item 7 "移除开机自动应用"
    menu_item 8 "仅清理策略规则"
    menu_item 9 "卸载并恢复默认"
    menu_item 0 "退出"

    echo
    read -r -p "请输入编号 [0-9]: " choice
    echo

    case "$choice" in
      1)
        if refresh_state; then
          ok "自动识别完成"
          show_summary
        else
          warn "自动识别失败"
        fi
        pause
        ;;
      2)
        apply_private_as_default_src
        pause
        ;;
      3)
        test_now
        pause
        ;;
      4)
        show_route_status
        pause
        ;;
      5)
        rollback_public_as_default_src
        pause
        ;;
      6)
        install_service
        pause
        ;;
      7)
        remove_service
        pause
        ;;
      8)
        clean_policy_only
        ok "策略规则已清理"
        pause
        ;;
      9)
        full_uninstall
        pause
        ;;
      0)
        exit 0
        ;;
      *)
        warn "无效选项，请输入 0-9"
        pause
        ;;
    esac
  done
}

menu
