#!/bin/bash
#
# Stream Unlock Installer
# 流媒体/AI 解锁一键脚本
# 
# 解锁机：安装 sniproxy，提供 DNS 解锁服务
# 被解锁机：安装 smartdns，分流指定服务到解锁机
#

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 服务域名配置
declare -A SERVICE_DOMAINS

# Netflix + Disney+
SERVICE_DOMAINS["netflix_disney"]="netflix.com netflix.net nflximg.com nflximg.net nflxvideo.net nflxext.com nflxso.net
disneyplus.com disney-plus.net dssott.com bamgrid.com"

# YouTube + Google
SERVICE_DOMAINS["youtube_google"]="youtube.com youtu.be ytimg.com googlevideo.com youtubei.googleapis.com youtube-nocookie.com
google.com googleapis.com gstatic.com"

# AI 全家桶 (ChatGPT, Claude, Gemini, Copilot...)
SERVICE_DOMAINS["ai"]="openai.com chatgpt.com ai.com oaistatic.com oaiusercontent.com auth0.openai.com
anthropic.com claude.ai statsig.anthropic.com
gemini.google.com generativelanguage.googleapis.com
copilot.microsoft.com
perplexity.ai
midjourney.com
character.ai
poe.com"

# TikTok
SERVICE_DOMAINS["tiktok"]="tiktok.com tiktokv.com tiktokcdn.com tiktokcdn-us.com byteoversea.com musical.ly"

# HBO
SERVICE_DOMAINS["hbo"]="hbomax.com hbo.com hbogo.com hbonow.com"

# Prime Video
SERVICE_DOMAINS["prime"]="primevideo.com amazon.com amazon.co.jp amazon.co.uk"

# Spotify
SERVICE_DOMAINS["spotify"]="spotify.com scdn.co spotifycdn.com spotifycdn.net"

# ============ 工具函数 ============

print_banner() {
    echo -e "${BLUE}"
    echo "╔═══════════════════════════════════════════╗"
    echo "║     Stream Unlock Installer v1.0          ║"
    echo "║     流媒体/AI 解锁一键脚本                 ║"
    echo "╚═══════════════════════════════════════════╝"
    echo -e "${NC}"
}

check_root() {
    if [[ $EUID -ne 0 ]]; then
        echo -e "${RED}请使用 root 用户运行此脚本${NC}"
        exit 1
    fi
}

detect_os() {
    if [[ -f /etc/debian_version ]]; then
        OS="debian"
        PKG_MANAGER="apt"
    elif [[ -f /etc/redhat-release ]]; then
        OS="centos"
        PKG_MANAGER="yum"
    elif [[ -f /etc/arch-release ]]; then
        OS="arch"
        PKG_MANAGER="pacman"
    else
        # 尝试检测其他系统
        if command -v apt &>/dev/null; then
            OS="debian"
            PKG_MANAGER="apt"
        elif command -v yum &>/dev/null; then
            OS="centos"
            PKG_MANAGER="yum"
        elif command -v pacman &>/dev/null; then
            OS="arch"
            PKG_MANAGER="pacman"
        else
            echo -e "${RED}不支持的系统，请手动安装 sniproxy${NC}"
            echo -e "${YELLOW}支持的系统: Debian/Ubuntu, CentOS/RHEL, Arch Linux${NC}"
            exit 1
        fi
    fi
    echo -e "${GREEN}检测到系统: $OS${NC}"
}

get_public_ip() {
    local ip=""
    ip=$(curl -s4 ip.sb 2>/dev/null) || \
    ip=$(curl -s4 ifconfig.me 2>/dev/null) || \
    ip=$(curl -s4 api.ipify.org 2>/dev/null)
    echo "$ip"
}

# ============ 解锁机安装 ============

install_sniproxy() {
    echo -e "${GREEN}[解锁机] 安装 sniproxy...${NC}"
    echo -e "${YELLOW}系统类型: $OS${NC}"
    
    # 安装依赖
    if [[ "$OS" == "debian" ]]; then
        apt update
        # 先尝试直接安装
        if ! apt install -y sniproxy 2>/dev/null; then
            echo -e "${YELLOW}sniproxy 不在默认仓库，从源码编译...${NC}"
            # 安装编译依赖
            apt install -y build-essential libev-dev libudns-dev pkg-config git
            # 克隆并编译
            cd /tmp
            git clone https://github.com/dlundquist/sniproxy.git
            cd sniproxy
            ./configure --prefix=/usr
            make -j$(nproc)
            make install
            # 创建 systemd 服务
            cat > /etc/systemd/system/sniproxy.service << 'SERVICE'
[Unit]
Description=sniproxy
After=network.target

[Service]
Type=forking
ExecStart=/usr/sbin/sniproxy -c /etc/sniproxy.conf
PIDFile=/var/run/sniproxy.pid
Restart=on-failure

[Install]
WantedBy=multi-user.target
SERVICE
            systemctl daemon-reload
            cd /
            rm -rf /tmp/sniproxy
        fi
        apt install -y dnsmasq ufw
    elif [[ "$OS" == "centos" ]]; then
        yum install -y epel-release
        yum install -y sniproxy dnsmasq firewalld
    elif [[ "$OS" == "arch" ]]; then
        pacman -Sy --noconfirm sniproxy dnsmasq ufw
    else
        echo -e "${RED}不支持的系统: $OS${NC}"
        echo -e "${YELLOW}请手动安装 sniproxy 和 dnsmasq${NC}"
        return 1
    fi
    
    # 备份原配置
    [[ -f /etc/sniproxy.conf ]] && cp /etc/sniproxy.conf /etc/sniproxy.conf.bak
    
    # 创建 sniproxy 配置
    cat > /etc/sniproxy.conf << 'EOF'
user daemon
pidfile /var/run/sniproxy.pid

listener 80 {
    proto http
    access_log {
        filename /var/log/sniproxy/http_access.log
    }
}

listener 443 {
    proto tls
    access_log {
        filename /var/log/sniproxy/https_access.log
    }
}

table {
    # 流媒体
    netflix.* *
    disneyplus.* *
    hbo.* *
    primevideo.* *
    
    # AI
    openai.* *
    chatgpt.* *
    anthropic.* *
    claude.* *
    gemini.* *
    
    # 短视频
    tiktok.* *
    youtube.* *
    ytimg.* *
    
    # 默认
    .* *
}
EOF
    
    # 创建日志目录
    mkdir -p /var/log/sniproxy
    chmod 755 /var/log/sniproxy
    
    # 配置防火墙
    configure_firewall_unlocker
    
    # 启动服务
    systemctl enable sniproxy
    systemctl restart sniproxy
    
    echo -e "${GREEN}[解锁机] sniproxy 安装完成${NC}"
}

configure_firewall_unlocker() {
    echo -e "${GREEN}[解锁机] 配置防火墙白名单...${NC}"
    
    local allowed_ips=""
    while true; do
        read -p "输入要放行的被解锁机 IP（输入 done 结束）: " ip
        [[ "$ip" == "done" ]] && break
        [[ -z "$ip" ]] && continue
        
        if [[ "$OS" == "debian" ]]; then
            ufw allow from "$ip" to any port 80
            ufw allow from "$ip" to any port 443
            ufw allow from "$ip" to any port 53
        else
            firewall-cmd --permanent --add-rich-rule="rule family=ipv4 source address=$ip port protocol=tcp port=80 accept"
            firewall-cmd --permanent --add-rich-rule="rule family=ipv4 source address=$ip port protocol=tcp port=443 accept"
            firewall-cmd --permanent --add-rich-rule="rule family=ipv4 source address=$ip port protocol=udp port=53 accept"
        fi
        
        allowed_ips="$allowed_ips $ip"
        echo -e "${GREEN}已添加: $ip${NC}"
    done
    
    # 开放本地 DNS
    if [[ "$OS" == "debian" ]]; then
        # 先放行 SSH，防止把自己锁在外面
        ufw allow 22/tcp comment 'SSH'
        ufw allow 53/udp
        ufw allow 53/tcp
        ufw --force enable
    else
        firewall-cmd --permanent --add-port=22/tcp
        firewall-cmd --permanent --add-port=53/udp
        firewall-cmd --permanent --add-port=53/tcp
        firewall-cmd --reload
    fi
    
    echo -e "${GREEN}[解锁机] 防火墙配置完成，已放行:$allowed_ips${NC}"
}

add_service_to_sniproxy() {
    local service_type="$1"
    local domains="${SERVICE_DOMAINS[$service_type]}"
    
    if [[ -z "$domains" ]]; then
        echo -e "${RED}未知服务类型: $service_type${NC}"
        return 1
    fi
    
    echo -e "${GREEN}添加服务到 sniproxy: $service_type${NC}"
    
    # 为每个域名添加规则
    for domain in $domains; do
        # 提取主域名作为模式
        local pattern="${domain%%.*}.*"
        
        # 检查是否已存在
        if ! grep -q "$pattern" /etc/sniproxy.conf 2>/dev/null; then
            # 在 table 块中添加
            sed -i "/table {/a\\    $pattern *" /etc/sniproxy.conf
            echo "  添加: $pattern"
        else
            echo "  已存在: $pattern"
        fi
    done
    
    # 重启服务
    systemctl restart sniproxy
    echo -e "${GREEN}sniproxy 已重启${NC}"
}

# ============ 被解锁机安装 ============

install_smartdns() {
    echo -e "${GREEN}[被解锁机] 安装 smartdns...${NC}"
    
    # 方法1: 尝试 apt 安装
    if [[ "$OS" == "debian" ]]; then
        # 添加 smartdns 官方源或直接安装
        if apt install -y smartdns 2>/dev/null; then
            echo -e "${GREEN}通过 apt 安装 smartdns 成功${NC}"
        else
            echo -e "${YELLOW}apt 安装失败，尝试手动下载...${NC}"
            
            # 获取最新版本
            local latest_url=$(curl -s https://api.github.com/repos/pymumu/smartdns/releases/latest | grep 'browser_download_url.*x86_64-linux-all.tar.gz' | head -1 | cut -d'"' -f4)
            
            if [[ -z "$latest_url" ]]; then
                # 备用下载地址
                latest_url="https://github.com/pymumu/smartdns/releases/download/Release45/smartdns.1.2024.08.08-1827.x86_64-linux-all.tar.gz"
            fi
            
            echo -e "${YELLOW}下载: $latest_url${NC}"
            local tmp_dir="/tmp/smartdns"
            mkdir -p "$tmp_dir"
            cd "$tmp_dir"
            
            if curl -sL "$latest_url" -o smartdns.tar.gz && tar -xzf smartdns.tar.gz; then
                chmod +x smartdns
                ./smartdns install -u
            else
                echo -e "${RED}下载安装失败，请手动安装 smartdns${NC}"
                echo -e "${YELLOW}参考: https://github.com/pymumu/smartdns${NC}"
                return 1
            fi
            
            cd -
            rm -rf "$tmp_dir"
        fi
    elif [[ "$OS" == "centos" ]]; then
        # CentOS 尝试 yum 或手动下载
        if ! yum install -y smartdns 2>/dev/null; then
            local latest_url=$(curl -s https://api.github.com/repos/pymumu/smartdns/releases/latest | grep 'browser_download_url.*x86_64-linux-all.tar.gz' | head -1 | cut -d'"' -f4)
            [[ -z "$latest_url" ]] && latest_url="https://github.com/pymumu/smartdns/releases/download/Release45/smartdns.1.2024.08.08-1827.x86_64-linux-all.tar.gz"
            
            local tmp_dir="/tmp/smartdns"
            mkdir -p "$tmp_dir"
            cd "$tmp_dir"
            curl -sL "$latest_url" -o smartdns.tar.gz && tar -xzf smartdns.tar.gz && chmod +x smartdns && ./smartdns install -u
            cd -
            rm -rf "$tmp_dir"
        fi
    else
        echo -e "${RED}不支持的系统，请手动安装 smartdns${NC}"
        return 1
    fi
    
    # 备份原配置
    [[ -f /etc/smartdns/smartdns.conf ]] && cp /etc/smartdns/smartdns.conf /etc/smartdns/smartdns.conf.bak
    
    # 创建基础配置
    mkdir -p /etc/smartdns
    cat > /etc/smartdns/smartdns.conf << 'EOF'
# SmartDNS 配置
bind :53

# 上游 DNS（简化格式，兼容 apt 版本）
server 8.8.8.8
server 1.1.1.1
server 223.5.5.5

# 缓存配置
cache-size 4096
EOF
    
    echo -e "${GREEN}[被解锁机] smartdns 安装完成${NC}"
}

configure_smartdns_unlocker() {
    local unlocker_ip="$1"
    local service_type="$2"
    local domains="${SERVICE_DOMAINS[$service_type]}"
    
    if [[ -z "$domains" ]]; then
        echo -e "${RED}未知服务类型: $service_type${NC}"
        return 1
    fi
    
    echo -e "${GREEN}配置分流规则: $service_type -> $unlocker_ip${NC}"
    
    local conf_file="/etc/smartdns/smartdns.conf"
    
    # 添加注释
    echo "" >> "$conf_file"
    echo "# $service_type 解锁规则 - $(date)" >> "$conf_file"
    
    # 为每个域名添加 address 规则
    for domain in $domains; do
        echo "address /$domain/$unlocker_ip" >> "$conf_file"
        echo "  添加: $domain -> $unlocker_ip"
    done
    
    echo -e "${GREEN}分流规则已添加${NC}"
}

restart_smartdns() {
    # 先杀掉可能存在的旧进程
    pkill -9 smartdns 2>/dev/null || true
    sleep 1
    
    systemctl daemon-reload
    systemctl enable smartdns
    systemctl start smartdns
    
    # 设置系统 DNS
    if command -v resolvconf &>/dev/null; then
        echo "nameserver 127.0.0.1" | resolvconf -a lo.smartdns
    else
        # 备份原 DNS 配置
        cp /etc/resolv.conf /etc/resolv.conf.bak 2>/dev/null || true
        chattr -i /etc/resolv.conf 2>/dev/null || true
        echo "nameserver 127.0.0.1" > /etc/resolv.conf
        # 防止被覆盖
        chattr +i /etc/resolv.conf 2>/dev/null || true
    fi
    
    echo -e "${GREEN}smartdns 已启动并设为系统 DNS${NC}"
}

# ============ 服务选择菜单 ============

SELECTED_SERVICES=()

select_services() {
    SELECTED_SERVICES=()
    
    echo ""
    echo -e "${YELLOW}选择要解锁的服务（可多选，空格分隔）：${NC}"
    echo "  1) Netflix + Disney+"
    echo "  2) YouTube + Google"
    echo "  3) AI 全家桶 (ChatGPT/Claude/Gemini/Copilot...)"
    echo "  4) TikTok"
    echo "  5) HBO"
    echo "  6) Prime Video"
    echo "  7) Spotify"
    echo "  8) 全部"
    echo ""
    echo -e "${GREEN}示例：输入 3 只解锁 AI；输入 1 2 4 解锁 Netflix+YouTube+TikTok${NC}"
    echo ""
    
    read -p "请输入选项: " choices
    
    for choice in $choices; do
        case $choice in
            1) SELECTED_SERVICES+=("netflix_disney") ;;
            2) SELECTED_SERVICES+=("youtube_google") ;;
            3) SELECTED_SERVICES+=("ai") ;;
            4) SELECTED_SERVICES+=("tiktok") ;;
            5) SELECTED_SERVICES+=("hbo") ;;
            6) SELECTED_SERVICES+=("prime") ;;
            7) SELECTED_SERVICES+=("spotify") ;;
            8) SELECTED_SERVICES=("netflix_disney" "youtube_google" "ai" "tiktok" "hbo" "prime" "spotify"); break ;;
            *) echo -e "${RED}无效选项: $choice${NC}" ;;
        esac
    done
}

# ============ 主菜单 ============

menu_unlocker() {
    clear
    print_banner
    
    local my_ip=$(get_public_ip)
    echo -e "本机 IP: ${GREEN}$my_ip${NC}"
    echo ""
    echo -e "${YELLOW}[解锁机模式]${NC}"
    echo "  1) 安装 sniproxy（首次安装）"
    echo "  2) 添加被解锁机 IP 白名单"
    echo "  3) 添加解锁服务"
    echo "  4) 查看当前配置"
    echo "  5) 卸载 sniproxy"
    echo ""
    
    read -p "请选择: " choice
    
    case $choice in
        1)
            install_sniproxy
            # 解锁机不需要选服务，sniproxy 配置已包含常见域名
            echo -e "${GREEN}解锁机配置完成！${NC}"
            echo -e "${YELLOW}下一步：在被解锁机上运行此脚本，选择「被解锁机」模式${NC}"
            ;;
        2)
            configure_firewall_unlocker
            ;;
        3)
            select_services
            for svc in "${SELECTED_SERVICES[@]}"; do
                add_service_to_sniproxy "$svc"
            done
            ;;
        4)
            echo -e "${GREEN}sniproxy 配置:${NC}"
            cat /etc/sniproxy.conf 2>/dev/null || echo "配置文件不存在"
            echo ""
            echo -e "${GREEN}防火墙规则:${NC}"
            if [[ "$OS" == "debian" ]]; then
                ufw status
            else
                firewall-cmd --list-all
            fi
            ;;
        5)
            echo -e "${RED}确定要卸载 sniproxy？(y/n): ${NC}"
            read -p "" confirm
            [[ "$confirm" == "y" ]] && {
                systemctl stop sniproxy
                systemctl disable sniproxy
                apt remove -y sniproxy 2>/dev/null || yum remove -y sniproxy 2>/dev/null
                rm -f /etc/sniproxy.conf
                echo -e "${GREEN}sniproxy 已卸载${NC}"
            }
            ;;
        *)
            echo -e "${RED}无效选项${NC}"
            ;;
    esac
}

menu_client() {
    clear
    print_banner
    
    local my_ip=$(get_public_ip)
    echo -e "本机 IP: ${GREEN}$my_ip${NC}"
    echo ""
    echo -e "${YELLOW}[被解锁机模式]${NC}"
    echo "  1) 安装 smartdns 并配置分流（首次安装）"
    echo "  2) 添加新的分流服务"
    echo "  3) 查看当前配置"
    echo "  4) 测试解锁"
    echo "  5) 卸载 smartdns"
    echo ""
    
    read -p "请选择: " choice
    
    case $choice in
        1)
            install_smartdns
            
            echo ""
            read -p "输入解锁机 IP: " unlocker_ip
            [[ -z "$unlocker_ip" ]] && {
                echo -e "${RED}解锁机 IP 不能为空${NC}"
                return 1
            }
            
            select_services
            for svc in "${SELECTED_SERVICES[@]}"; do
                configure_smartdns_unlocker "$unlocker_ip" "$svc"
            done
            
            restart_smartdns
            
            echo ""
            echo -e "${GREEN}配置完成！${NC}"
            echo "解锁机: $unlocker_ip"
            echo "分流服务: ${SELECTED_SERVICES[*]}"
            ;;
        2)
            local current_unlocker=$(grep "address /netflix.com/" /etc/smartdns/smartdns.conf 2>/dev/null | head -1 | awk -F'/' '{print $3}')
            
            if [[ -z "$current_unlocker" ]]; then
                read -p "输入解锁机 IP: " unlocker_ip
            else
                read -p "解锁机 IP [$current_unlocker]: " unlocker_ip
                unlocker_ip=${unlocker_ip:-$current_unlocker}
            fi
            
            select_services
            for svc in "${SELECTED_SERVICES[@]}"; do
                configure_smartdns_unlocker "$unlocker_ip" "$svc"
            done
            
            restart_smartdns
            ;;
        3)
            echo -e "${GREEN}smartdns 配置:${NC}"
            cat /etc/smartdns/smartdns.conf 2>/dev/null || echo "配置文件不存在"
            ;;
        4)
            echo -e "${GREEN}测试 DNS 解析...${NC}"
            echo ""
            echo "Netflix: $(dig +short netflix.com @127.0.0.1 2>/dev/null | head -1)"
            echo "ChatGPT: $(dig +short chatgpt.com @127.0.0.1 2>/dev/null | head -1)"
            echo "TikTok: $(dig +short tiktok.com @127.0.0.1 2>/dev/null | head -1)"
            ;;
        5)
            echo -e "${RED}确定要卸载并恢复原状？(y/n): ${NC}"
            read -p "" confirm
            [[ "$confirm" == "y" ]] && {
                echo -e "${YELLOW}正在卸载 smartdns...${NC}"
                systemctl stop smartdns 2>/dev/null || true
                systemctl disable smartdns 2>/dev/null || true
                pkill -9 smartdns 2>/dev/null || true
                
                # 恢复原 DNS
                chattr -i /etc/resolv.conf 2>/dev/null || true
                if [[ -f /etc/resolv.conf.bak ]]; then
                    mv /etc/resolv.conf.bak /etc/resolv.conf
                    echo -e "${GREEN}已恢复原 DNS 配置${NC}"
                else
                    # 创建默认 DNS 配置
                    cat > /etc/resolv.conf << 'EOF'
nameserver 8.8.8.8
nameserver 1.1.1.1
EOF
                    echo -e "${GREEN}已恢复默认 DNS${NC}"
                fi
                
                # 删除 smartdns
                rm -rf /etc/smartdns
                rm -f /var/run/smartdns.pid
                
                # 可选：卸载软件包
                read -p "是否同时卸载 smartdns 软件包？(y/n): " remove_pkg
                [[ "$remove_pkg" == "y" ]] && {
                    apt remove -y smartdns 2>/dev/null || yum remove -y smartdns 2>/dev/null || true
                    echo -e "${GREEN}smartdns 软件包已卸载${NC}"
                }
                
                echo -e "${GREEN}=== 卸载完成，系统已恢复原状 ===${NC}"
            }
            ;;
        *)
            echo -e "${RED}无效选项${NC}"
            ;;
    esac
}

# ============ 主入口 ============

main() {
    check_root
    detect_os
    
    clear
    print_banner
    
    local my_ip=$(get_public_ip)
    echo -e "本机 IP: ${GREEN}$my_ip${NC}"
    echo ""
    echo "请选择本机角色："
    echo "  1) 解锁机（我能解锁，帮别人解锁）"
    echo "  2) 被解锁机（我不能解锁，用别人的解锁机）"
    echo "  3) 退出"
    echo ""
    
    read -p "请选择: " mode
    
    case $mode in
        1) menu_unlocker ;;
        2) menu_client ;;
        3) exit 0 ;;
        *) echo -e "${RED}无效选项${NC}" ;;
    esac
}

main "$@"
