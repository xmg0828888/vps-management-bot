import asyncio
import html
import json
import math
import os
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import paramiko
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    MenuButtonCommands,
    ReplyKeyboardRemove,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
KEYS_DIR = os.path.join(BASE_DIR, "keys")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(KEYS_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "app.db")
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")


def esc(s: str) -> str:
    return html.escape(str(s), quote=False)


def load_config_file():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config_file(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)


CONFIG = load_config_file()
TOKEN = CONFIG["bot_token"]
ADMIN_IDS = set(CONFIG.get("admin_ids", []))
BOT_NAME = CONFIG.get("bot_name", "VPS 多机中控")

SETTINGS_DEFAULTS = {
    "default_user": CONFIG.get("default_user", "root"),
    "default_port": str(CONFIG.get("default_port", 22)),
    "default_password": CONFIG.get("default_password", ""),
    "default_key_path": CONFIG.get("default_key_path", "/opt/vpsbot8/keys/koipy_key"),
    "check_interval": "60",
    "fail_threshold": "3",
    "notify_recovery": "1",
    "notify_offline": "1",
    "cpu_alert_threshold": "0",
    "mem_alert_threshold": "0",
    "disk_alert_threshold": "0",
    "expire_remind_days": "7",
}

READ_ONLY_BATCH = {
    "hostname": ("🏷 主机名", "hostname"),
    "disk": ("💽 磁盘", "df -h /"),
    "failed": ("🚨 失败服务", "systemctl --failed --no-pager --no-legend || true"),
    "docker": ("🐳 Docker", "docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}' 2>/dev/null || echo docker_not_available"),
}

ADD_FLOW = ["name", "group_name", "host", "port", "user"]
SERVICE_ACTIONS = {
    "status": "状态",
    "restart": "重启",
    "start": "启动",
    "stop": "停止",
}
READONLY_CUSTOM_PREFIXES = (
    "uname", "hostname", "whoami", "uptime", "date", "df", "free", "cat /proc/",
    "docker ps", "docker images", "systemctl status", "systemctl is-active", "systemctl list-units",
    "ss ", "netstat ", "ip a", "ip r", "curl -I", "curl -sI", "ps ", "top -bn1", "lsb_release", "neofetch"
)
BLOCKED_CUSTOM_TOKENS = (
    " rm ", " reboot", " shutdown", " poweroff", " init 0", " init 6", " mkfs", " dd ", " chmod ", " chown ",
    " useradd", " userdel", " passwd", " systemctl restart", " systemctl stop", " systemctl start", " docker stop", " docker restart",
    " docker rm", " apt ", " apt-get", " yum ", " dnf ", " apk ", " pacman ", " pip install", " npm install", " sed -i", " tee ",
    ">", "| sh", "| bash", "&&", ";", "\n"
)


@dataclass
class Node:
    id: int
    name: str
    group_name: str
    host: str
    port: int
    user: str
    auth_type: str
    auth_value: str
    enabled: int
    created_at: int
    remark: str = ""
    expires_at: int = 0
    monthly_price: float = 0.0
    price_currency: str = "U"
    price_cycle: str = "month"


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS nodes (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          group_name TEXT NOT NULL DEFAULT 'default',
          host TEXT NOT NULL,
          port INTEGER NOT NULL DEFAULT 22,
          user TEXT NOT NULL DEFAULT 'root',
          password TEXT NOT NULL DEFAULT '',
          auth_type TEXT DEFAULT 'password',
          auth_value TEXT DEFAULT '',
          enabled INTEGER NOT NULL DEFAULT 1,
          created_at INTEGER NOT NULL,
          remark TEXT NOT NULL DEFAULT '',
          expires_at INTEGER NOT NULL DEFAULT 0,
          monthly_price REAL NOT NULL DEFAULT 0,
          price_currency TEXT NOT NULL DEFAULT 'U',
          price_cycle TEXT NOT NULL DEFAULT 'month'
        );

        CREATE TABLE IF NOT EXISTS settings (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS node_state (
          node_id INTEGER PRIMARY KEY,
          fail_count INTEGER NOT NULL DEFAULT 0,
          is_online INTEGER NOT NULL DEFAULT 1,
          last_error TEXT DEFAULT '',
          last_change INTEGER NOT NULL DEFAULT 0,
          FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS resource_state (
          node_id INTEGER NOT NULL,
          metric TEXT NOT NULL,
          alerted INTEGER NOT NULL DEFAULT 0,
          last_value REAL NOT NULL DEFAULT 0,
          updated_at INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (node_id, metric),
          FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS expiry_state (
          node_id INTEGER PRIMARY KEY,
          last_days_left INTEGER NOT NULL DEFAULT 999999,
          last_notified_at INTEGER NOT NULL DEFAULT 0,
          FOREIGN KEY (node_id) REFERENCES nodes(id) ON DELETE CASCADE
        );
        """
    )
    cols = {row[1] for row in conn.execute("PRAGMA table_info(nodes)").fetchall()}
    if "auth_type" not in cols:
        conn.execute("ALTER TABLE nodes ADD COLUMN auth_type TEXT DEFAULT 'password'")
    if "auth_value" not in cols:
        conn.execute("ALTER TABLE nodes ADD COLUMN auth_value TEXT DEFAULT ''")
    if "group_name" not in cols:
        conn.execute("ALTER TABLE nodes ADD COLUMN group_name TEXT DEFAULT 'default'")
    if "password" in cols:
        conn.execute("UPDATE nodes SET auth_value=password WHERE (auth_value='' OR auth_value IS NULL) AND password IS NOT NULL")
        conn.execute("UPDATE nodes SET auth_type='password' WHERE auth_type IS NULL OR auth_type='' ")
    if "remark" not in cols:
        conn.execute("ALTER TABLE nodes ADD COLUMN remark TEXT NOT NULL DEFAULT ''")
    if "expires_at" not in cols:
        conn.execute("ALTER TABLE nodes ADD COLUMN expires_at INTEGER NOT NULL DEFAULT 0")
    if "monthly_price" not in cols:
        conn.execute("ALTER TABLE nodes ADD COLUMN monthly_price REAL NOT NULL DEFAULT 0")
    if "price_currency" not in cols:
        conn.execute("ALTER TABLE nodes ADD COLUMN price_currency TEXT NOT NULL DEFAULT 'U'")
    if "price_cycle" not in cols:
        conn.execute("ALTER TABLE nodes ADD COLUMN price_cycle TEXT NOT NULL DEFAULT 'month'")
    for k, v in SETTINGS_DEFAULTS.items():
        conn.execute("INSERT OR IGNORE INTO settings(key,value) VALUES(?,?)", (k, v))
    conn.commit()
    conn.close()


def get_setting(key: str) -> str:
    conn = db()
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    conn.close()
    if row:
        return row[0]
    return SETTINGS_DEFAULTS[key]


def set_setting(key: str, value: str):
    conn = db()
    conn.execute("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))
    conn.commit()
    conn.close()
    if key in {"default_user", "default_port", "default_password", "default_key_path"}:
        cfg = load_config_file()
        cfg[key] = int(value) if key == "default_port" else value
        save_config_file(cfg)


def defaults_dict():
    return {
        "user": get_setting("default_user"),
        "port": int(get_setting("default_port")),
        "password": get_setting("default_password"),
        "key_path": get_setting("default_key_path"),
    }

def format_expiry(ts: int) -> str:
    if not ts:
        return "未设置"
    return datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d")


def days_left_text(ts: int) -> str:
    if not ts:
        return "未设置"
    days = int((int(ts) - int(time.time())) // 86400)
    if days > 0:
        return f"{days} 天"
    if days == 0:
        return "今天到期"
    return f"已过期 {abs(days)} 天"


def alert_settings_text() -> str:
    return (
        f"巡检间隔：<code>{get_setting('check_interval')}</code> 秒\n"
        f"失败阈值：<code>{get_setting('fail_threshold')}</code> 次\n"
        f"掉线通知：<code>{'开' if get_setting('notify_offline') == '1' else '关'}</code>\n"
        f"恢复通知：<code>{'开' if get_setting('notify_recovery') == '1' else '关'}</code>"
    )


def add_prompts():
    d = defaults_dict()
    return {
        "name": "发我节点名称，例如：<code>HK-1</code>",
        "group_name": f"发我节点分组，例如：<code>HK</code> / <code>生产</code>。留空默认 <code>default</code>\n现有分组：<code>{', '.join([g for g,_ in list_groups()][:8]) or 'default'}</code>",
        "host": "发我节点 IP 或域名，例如：<code>1.2.3.4</code>",
        "port": f"发我 SSH 端口，默认 <code>{d['port']}</code>，留空也行。",
        "user": f"发我 SSH 用户，默认 <code>{d['user']}</code>，留空也行。",
        "custom_password": "发我这台机器的 SSH 密码。",
        "custom_key": "发我私钥路径，或者直接把私钥内容整段发过来。",
        "set_default_password": "发我新的默认 SSH 密码。",
        "set_default_key": "发我新的默认私钥路径，或者直接发整段私钥内容。",
        "set_default_user": "发我新的默认 SSH 用户。",
        "set_default_port": "发我新的默认 SSH 端口。",
        "set_check_interval": "发我新的巡检间隔（秒），例如 60。",
        "set_fail_threshold": "发我新的失败阈值，例如 3。",
        "set_cpu_alert_threshold": "发我新的 CPU 告警阈值。建议用 load 值，例如 4。填 0 为关闭。",
        "set_mem_alert_threshold": "发我新的内存告警阈值（百分比）。填 0 为关闭。",
        "set_disk_alert_threshold": "发我新的磁盘告警阈值（百分比）。填 0 为关闭。",
        "set_expire_remind_days": "发我新的到期提前提醒天数，例如 7。",
        "edit_name": "发我新的节点名称。",
        "edit_host": "发我新的 IP 或域名。",
        "edit_port": "发我新的 SSH 端口。",
        "edit_user": "发我新的 SSH 用户。",
        "edit_auth_password": "发我新的 SSH 密码。",
        "edit_auth_key": "发我新的私钥路径，或者直接发整段私钥内容。",
        "edit_remark": "发我新的节点备注。留空可发一个 - 代表清空。",
        "edit_monthly_price": "发我新的月付(U)，例如 4.99。填 0 代表未设置。",
        "edit_expires_at": "发我新的到期日期，格式：YYYY-MM-DD。填 0 代表清空。",
        "batch_service_name": "发我要批量操作的 systemd 服务名，例如：nginx / docker / openclaw。",
        "batch_readonly_command": "发我要批量执行的只读命令，例如：df -h /、free -m、docker ps、systemctl status nginx。",
    }


def maybe_store_private_key(raw: str, prefix: str = "custom") -> str:
    text = (raw or "").strip()
    if "BEGIN OPENSSH PRIVATE KEY" in text or "BEGIN RSA PRIVATE KEY" in text or "BEGIN PRIVATE KEY" in text:
        path = os.path.join(KEYS_DIR, f"{prefix}_{int(time.time()*1000)}.key")
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
            if not text.endswith("\n"):
                f.write("\n")
        os.chmod(path, 0o600)
        return path
    return text


def is_admin(update: Update) -> bool:
    user = update.effective_user
    return bool(user and user.id in ADMIN_IDS)


async def guard(update: Update):
    if not is_admin(update):
        if update.message:
            await update.message.reply_text("你不在管理员白名单里。")
        elif update.callback_query:
            await update.callback_query.answer("你不在管理员白名单里。", show_alert=True)
        return False
    return True


def main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 总览", callback_data="nodes:overview"), InlineKeyboardButton("🖥 节点", callback_data="nodes:list")],
        [InlineKeyboardButton("➕ 添加", callback_data="nodes:add"), InlineKeyboardButton("🗂 分组", callback_data="groups:list")],
        [InlineKeyboardButton("🔔 告警", callback_data="settings:alerts"), InlineKeyboardButton("🔐 认证", callback_data="settings:auth")],
        [InlineKeyboardButton("💰 账单", callback_data="billing:summary"), InlineKeyboardButton("⏰ 到期", callback_data="billing:expiring")],
        [InlineKeyboardButton("🧰 批量操作", callback_data="batch:menu")],
    ])


def load_node(node_id: int) -> Optional[Node]:
    conn = db()
    row = conn.execute(
        "SELECT id,name,group_name,host,port,user,COALESCE(auth_type,'password') auth_type, COALESCE(NULLIF(auth_value,''),password) auth_value, enabled, created_at, COALESCE(remark,'') remark, COALESCE(expires_at,0) expires_at, COALESCE(monthly_price,0) monthly_price, COALESCE(price_currency,'U') price_currency, COALESCE(price_cycle,'month') price_cycle FROM nodes WHERE id=?",
        (node_id,),
    ).fetchone()
    conn.close()
    return Node(**dict(row)) if row else None


def all_nodes(enabled_only=True):
    conn = db()
    sql = "SELECT id,name,group_name,host,port,user,COALESCE(auth_type,'password') auth_type, COALESCE(NULLIF(auth_value,''),password) auth_value, enabled, created_at, COALESCE(remark,'') remark, COALESCE(expires_at,0) expires_at, COALESCE(monthly_price,0) monthly_price, COALESCE(price_currency,'U') price_currency, COALESCE(price_cycle,'month') price_cycle FROM nodes"
    if enabled_only:
        sql += " WHERE enabled=1"
    sql += " ORDER BY group_name ASC, id DESC"
    rows = conn.execute(sql).fetchall()
    conn.close()
    return [Node(**dict(r)) for r in rows]


def nodes_by_group(group_name: str):
    conn = db()
    rows = conn.execute(
        "SELECT id,name,group_name,host,port,user,COALESCE(auth_type,'password') auth_type, COALESCE(NULLIF(auth_value,''),password) auth_value, enabled, created_at, COALESCE(remark,'') remark, COALESCE(expires_at,0) expires_at, COALESCE(monthly_price,0) monthly_price, COALESCE(price_currency,'U') price_currency, COALESCE(price_cycle,'month') price_cycle FROM nodes WHERE enabled=1 AND group_name=? ORDER BY id DESC",
        (group_name,),
    ).fetchall()
    conn.close()
    return [Node(**dict(r)) for r in rows]


def list_groups():
    conn = db()
    rows = conn.execute("SELECT group_name, COUNT(*) c FROM nodes WHERE enabled=1 GROUP BY group_name ORDER BY group_name ASC").fetchall()
    conn.close()
    return [(r[0], r[1]) for r in rows]


def auth_label(node: Node) -> str:
    d = defaults_dict()
    if node.auth_type == "key":
        if node.auth_value == d["key_path"]:
            return "默认Key"
        return "自定义Key"
    if node.auth_value == d["password"] and d["password"]:
        return "默认密码"
    return "自定义密码"


async def run_ssh(node: Node, command: str, timeout: int = 12):
    def _run():
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kwargs = dict(
            hostname=node.host,
            port=node.port,
            username=node.user,
            timeout=timeout,
            banner_timeout=timeout,
            auth_timeout=timeout,
            look_for_keys=False,
            allow_agent=False,
        )
        if node.auth_type == "key":
            kwargs["key_filename"] = node.auth_value
        else:
            kwargs["password"] = node.auth_value
        try:
            client.connect(**kwargs)
            stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
            out = stdout.read().decode("utf-8", errors="ignore")
            err = stderr.read().decode("utf-8", errors="ignore")
            code = stdout.channel.recv_exit_status()
            return code, out.strip(), err.strip()
        finally:
            client.close()
    return await asyncio.to_thread(_run)


async def collect_node_summary(node: Node):
    cmd = r'''bash -lc '
HOST=$(hostname 2>/dev/null || echo unknown)
UP=$(uptime -p 2>/dev/null || uptime)
LOAD=$(cat /proc/loadavg 2>/dev/null | awk "{print \$1,\$2,\$3}" || echo -)
MEM=$(free -m 2>/dev/null | awk "/Mem:/ {printf \"%s/%s MB\", \$3, \$2}" || echo -)
DISK=$(df -h / 2>/dev/null | awk "NR==2 {printf \"%s/%s (%s)\", \$3, \$2, \$5}" || echo -)
echo "HOST=$HOST"
echo "UP=$UP"
echo "LOAD=$LOAD"
echo "MEM=$MEM"
echo "DISK=$DISK"
' '''
    code, out, err = await run_ssh(node, cmd, timeout=12)
    if code != 0:
        raise RuntimeError(err or out or "SSH 执行失败")
    data = {}
    for line in out.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            data[k] = v
    return data


async def collect_docker_list(node: Node):
    cmd = r'''bash -lc '
if ! command -v docker >/dev/null 2>&1; then
  echo "DOCKER_MISSING=1"
  exit 0
fi
docker ps -a --format "{{.Names}}|{{.Status}}|{{.Image}}" 2>/dev/null
' '''
    code, out, err = await run_ssh(node, cmd, timeout=15)
    if code != 0:
        raise RuntimeError(err or out or "获取 Docker 列表失败")
    if "DOCKER_MISSING=1" in out:
        return None
    items = []
    for line in out.splitlines():
        if not line.strip() or "|" not in line:
            continue
        name, status, image = (line.split("|", 2) + ["", "", ""])[:3]
        items.append({"name": name, "status": status, "image": image})
    return items


async def docker_action(node: Node, container: str, action: str):
    return await run_ssh(node, f"docker {action} {container}", timeout=20)


async def docker_logs(node: Node, container: str):
    return await run_ssh(node, f"docker logs --tail 40 {container}", timeout=20)


def set_node_state(node_id: int, fail_count: int, is_online: int, last_error: str):
    conn = db()
    conn.execute(
        "INSERT INTO node_state(node_id,fail_count,is_online,last_error,last_change) VALUES(?,?,?,?,?) ON CONFLICT(node_id) DO UPDATE SET fail_count=excluded.fail_count,is_online=excluded.is_online,last_error=excluded.last_error,last_change=excluded.last_change",
        (node_id, fail_count, is_online, last_error[:500], int(time.time())),
    )
    conn.commit()
    conn.close()


def get_node_state(node_id: int):
    conn = db()
    row = conn.execute("SELECT fail_count,is_online,last_error,last_change FROM node_state WHERE node_id=?", (node_id,)).fetchone()
    conn.close()
    if row:
        return dict(row)
    return {"fail_count": 0, "is_online": 1, "last_error": "", "last_change": 0}

def update_node_group(node_id: int, group_name: str):
    conn = db()
    conn.execute("UPDATE nodes SET group_name=? WHERE id=?", (group_name or 'default', node_id))
    conn.commit()
    conn.close()

def move_group_nodes(old_group: str, new_group: str = 'default'):
    conn = db()
    conn.execute("UPDATE nodes SET group_name=? WHERE group_name=?", (new_group or 'default', old_group))
    conn.commit()
    conn.close()

def update_node_field(node_id: int, field: str, value):
    if field not in {"name", "host", "port", "user", "auth_type", "auth_value", "group_name", "remark", "expires_at", "monthly_price", "price_currency", "price_cycle"}:
        raise ValueError("bad field")
    conn = db()
    conn.execute(f"UPDATE nodes SET {field}=? WHERE id=?", (value, node_id))
    conn.commit()
    conn.close()

def get_resource_state(node_id: int, metric: str):
    conn = db()
    row = conn.execute("SELECT alerted,last_value,updated_at FROM resource_state WHERE node_id=? AND metric=?", (node_id, metric)).fetchone()
    conn.close()
    if row:
        return dict(row)
    return {"alerted": 0, "last_value": 0, "updated_at": 0}

def set_resource_state(node_id: int, metric: str, alerted: int, last_value: float):
    conn = db()
    conn.execute("INSERT INTO resource_state(node_id,metric,alerted,last_value,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(node_id,metric) DO UPDATE SET alerted=excluded.alerted,last_value=excluded.last_value,updated_at=excluded.updated_at", (node_id, metric, int(alerted), float(last_value), int(time.time())))
    conn.commit()
    conn.close()

def parse_resource_usage(info: dict):
    result = {"cpu": None, "mem": None, "disk": None}
    try:
        result["cpu"] = float((info.get("LOAD") or "-").split()[0])
    except Exception:
        pass
    try:
        mem = info.get("MEM", "")
        left = mem.split(" MB")[0]
        used, total = left.split("/")
        result["mem"] = round(float(used) / float(total) * 100, 1)
    except Exception:
        pass
    try:
        disk = info.get("DISK", "")
        pct = disk.split("(")[-1].split("%)")[0].replace("%", "")
        result["disk"] = float(pct)
    except Exception:
        pass
    return result


def get_expiry_state(node_id: int):
    conn = db()
    row = conn.execute("SELECT last_days_left,last_notified_at FROM expiry_state WHERE node_id=?", (node_id,)).fetchone()
    conn.close()
    if row:
        return dict(row)
    return {"last_days_left": 999999, "last_notified_at": 0}


def set_expiry_state(node_id: int, last_days_left: int):
    conn = db()
    conn.execute("INSERT INTO expiry_state(node_id,last_days_left,last_notified_at) VALUES(?,?,?) ON CONFLICT(node_id) DO UPDATE SET last_days_left=excluded.last_days_left,last_notified_at=excluded.last_notified_at", (node_id, int(last_days_left), int(time.time())))
    conn.commit()
    conn.close()

def choose_group_buttons(node_id: int):
    buttons = []
    groups = [g for g, _ in list_groups()]
    for g in groups[:12]:
        buttons.append([InlineKeyboardButton(f"🗂 {g}", callback_data=f"node:setgroupto:{node_id}:{g}")])
    buttons.append([InlineKeyboardButton("✍️ 新建/输入新分组", callback_data=f"node:setgroupinput:{node_id}")])
    buttons.append([InlineKeyboardButton("⬅️ 返回节点", callback_data=f"node:view:{node_id}")])
    return InlineKeyboardMarkup(buttons)

def expiring_nodes(limit_days: int = 30):
    now = int(time.time())
    end = now + limit_days * 86400
    conn = db()
    rows = conn.execute(
        "SELECT id,name,group_name,host,port,user,COALESCE(auth_type,'password') auth_type, COALESCE(NULLIF(auth_value,''),password) auth_value, enabled, created_at, COALESCE(remark,'') remark, COALESCE(expires_at,0) expires_at, COALESCE(monthly_price,0) monthly_price, COALESCE(price_currency,'U') price_currency, COALESCE(price_cycle,'month') price_cycle FROM nodes WHERE enabled=1 AND expires_at>0 AND expires_at<=? ORDER BY expires_at ASC, id DESC",
        (end,),
    ).fetchall()
    conn.close()
    return [Node(**dict(r)) for r in rows]


def monthly_cost_total():
    conn = db()
    row = conn.execute("SELECT COALESCE(SUM(monthly_price),0) s FROM nodes WHERE enabled=1").fetchone()
    conn.close()
    return float(row[0] or 0)


def monthly_cost_by_group():
    conn = db()
    rows = conn.execute("SELECT group_name, COUNT(*) c, COALESCE(SUM(monthly_price),0) s FROM nodes WHERE enabled=1 GROUP BY group_name ORDER BY s DESC, group_name ASC").fetchall()
    conn.close()
    return [(r[0], int(r[1]), float(r[2] or 0)) for r in rows]


def is_safe_readonly_command(command: str) -> bool:
    c = (command or "").strip().lower()
    if not c:
        return False
    if any(tok in c for tok in BLOCKED_CUSTOM_TOKENS):
        return False
    return c.startswith(READONLY_CUSTOM_PREFIXES)


def cycle_label(cycle: str) -> str:
    return {"month": "月付", "quarter": "季付", "year": "年付"}.get((cycle or "month"), "月付")


def format_price(node: Node) -> str:
    amount = float(node.monthly_price or 0)
    if amount <= 0:
        return "未设置"
    currency = (node.price_currency or "U").upper()
    return f"{amount:.2f} {currency}/{cycle_label(node.price_cycle)}"


def annual_cost_total():
    return round(monthly_cost_total() * 12, 2)


def top_cost_nodes(limit: int = 10):
    conn = db()
    rows = conn.execute(
        "SELECT id,name,group_name,host,port,user,COALESCE(auth_type,'password') auth_type, COALESCE(NULLIF(auth_value,''),password) auth_value, enabled, created_at, COALESCE(remark,'') remark, COALESCE(expires_at,0) expires_at, COALESCE(monthly_price,0) monthly_price, COALESCE(price_currency,'U') price_currency, COALESCE(price_cycle,'month') price_cycle FROM nodes WHERE enabled=1 ORDER BY monthly_price DESC, expires_at ASC, id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    conn.close()
    return [Node(**dict(r)) for r in rows]


async def notify_admins(app: Application, text: str):
    for uid in ADMIN_IDS:
        try:
            await app.bot.send_message(chat_id=uid, text=text)
        except Exception:
            pass


async def monitor_once(app: Application):
    nodes = all_nodes(enabled_only=True)
    threshold = max(1, int(get_setting("fail_threshold")))
    expire_days = max(0, int(get_setting("expire_remind_days") or "0"))
    now = int(time.time())
    for node in nodes:
        state = get_node_state(node.id)
        try:
            code, out, err = await run_ssh(node, "echo ok", timeout=8)
            if code == 0:
                if state["is_online"] == 0 and get_setting("notify_recovery") == "1":
                    await notify_admins(app, f"✅ 节点恢复\n{node.name} ({node.host})\n分组: {node.group_name}")
                set_node_state(node.id, 0, 1, "")
            else:
                fail_count = state["fail_count"] + 1
                is_online = 0 if fail_count >= threshold else 1
                if is_online == 0 and state["is_online"] == 1 and get_setting("notify_offline") == "1":
                    await notify_admins(app, f"🚨 节点掉线\n{node.name} ({node.host})\n分组: {node.group_name}\n错误: {(err or out or 'SSH failed')[:200]}")
                set_node_state(node.id, fail_count, is_online, err or out or "SSH failed")
        except Exception as e:
            fail_count = state["fail_count"] + 1
            is_online = 0 if fail_count >= threshold else 1
            if is_online == 0 and state["is_online"] == 1 and get_setting("notify_offline") == "1":
                await notify_admins(app, f"🚨 节点掉线\n{node.name} ({node.host})\n分组: {node.group_name}\n错误: {str(e)[:200]}")
            set_node_state(node.id, fail_count, is_online, str(e))

        if int(node.expires_at or 0) > 0 and expire_days > 0:
            days_left = math.floor((int(node.expires_at) - now) / 86400)
            exp_state = get_expiry_state(node.id)
            remind_points = sorted({expire_days, 7, 3, 0}, reverse=True)
            should_notify = False
            title = None
            if days_left < 0:
                last_at = int(exp_state.get("last_notified_at") or 0)
                if last_at <= 0 or (now - last_at) >= 86400:
                    should_notify = True
                    title = "⛔ 节点已过期"
            else:
                for point in remind_points:
                    if point < 0:
                        continue
                    if days_left <= point and int(exp_state.get("last_days_left", 999999)) > point:
                        should_notify = True
                        title = "⏰ 节点即将到期" if days_left > 0 else "⚠️ 节点今天到期"
                        break
            if should_notify:
                await notify_admins(app, f"{title}\n{node.name} ({node.host})\n分组: {node.group_name}\n到期: {format_expiry(node.expires_at)}\n剩余: {days_left_text(node.expires_at)}\n账单: {format_price(node)}")
            set_expiry_state(node.id, days_left)


async def monitor_loop(app: Application):
    await asyncio.sleep(8)
    while True:
        try:
            await monitor_once(app)
        except Exception:
            pass
        await asyncio.sleep(max(15, int(get_setting("check_interval"))))


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    total = len(all_nodes())
    groups = len(list_groups())
    text = (
        f"<b>{BOT_NAME}</b>\n"
        f"多 VPS 中控台\n\n"
        f"节点数：<code>{total}</code>\n"
        f"分组数：<code>{groups}</code>\n"
        f"告警：<code>{'开' if get_setting('notify_offline') == '1' else '关'}</code>"
    )
    if update.message:
        await update.message.reply_text(text + "\n\n请选择操作👇", parse_mode=ParseMode.HTML, reply_markup=main_menu())
    else:
        await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu())


async def show_nodes(query, group_name: Optional[str] = None):
    rows = nodes_by_group(group_name) if group_name else all_nodes()
    title = f"分组：{group_name}" if group_name else "节点列表（按分组）"
    if not rows:
        await query.edit_message_text(
            f"{title}\n\n还没有节点。",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("➕ 添加节点", callback_data="nodes:add")],
                [InlineKeyboardButton("🏠 首页", callback_data="home")],
            ]),
        )
        return
    buttons = []
    last_group = None
    for r in rows[:50]:
        if r.group_name != last_group:
            buttons.append([InlineKeyboardButton(f"🗂 {r.group_name}", callback_data=f"batch:group:{r.group_name}")])
            last_group = r.group_name
        buttons.append([InlineKeyboardButton(f"   └ 🖥️ {r.name} ({r.host})", callback_data=f"node:view:{r.id}")])
    buttons.append([InlineKeyboardButton("🧰 批量操作", callback_data="batch:menu")])
    buttons.append([InlineKeyboardButton("🏠 首页", callback_data="home")])
    await query.edit_message_text(title, reply_markup=InlineKeyboardMarkup(buttons))


async def show_groups(query):
    groups = list_groups()
    if not groups:
        await query.edit_message_text("还没有任何分组。", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 首页", callback_data="home")]]))
        return
    buttons = []
    for g, c in groups:
        buttons.append([
            InlineKeyboardButton(f"🗂 {g} ({c})", callback_data=f"groups:view:{g}"),
            InlineKeyboardButton("🧰 批量", callback_data=f"batch:group:{g}"),
            InlineKeyboardButton("🗑 删除", callback_data=f"groups:delask:{g}"),
        ])
    buttons.append([InlineKeyboardButton("🌐 全部节点批量", callback_data="batch:scope:all")])
    buttons.append([InlineKeyboardButton("🏠 首页", callback_data="home")])
    await query.edit_message_text("分组中心", reply_markup=InlineKeyboardMarkup(buttons))


async def begin_add(query, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["add_node"] = {}
    context.user_data["add_step"] = ADD_FLOW[0]
    await query.edit_message_text(
        "开始添加节点\n\n" + add_prompts()[ADD_FLOW[0]],
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 首页", callback_data="home")]]),
    )


async def prompt_auth_choice(message, data):
    summary = (
        f"名称：<code>{esc(data['name'])}</code>\n"
        f"分组：<code>{esc(data['group_name'])}</code>\n"
        f"地址：<code>{esc(data['host'])}:{data['port']}</code>\n"
        f"用户：<code>{esc(data['user'])}</code>\n\n"
        f"现在选 SSH 认证方式："
    )
    await message.reply_text(summary, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🔐 默认密码", callback_data="auth:default_password"), InlineKeyboardButton("🗝️ 默认Key", callback_data="auth:default_key")],
        [InlineKeyboardButton("✍️ 自定义密码", callback_data="auth:custom_password"), InlineKeyboardButton("📁 自定义Key", callback_data="auth:custom_key")],
    ]))


async def save_node_from_context(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = context.user_data.get("add_node", {})
    d = defaults_dict()
    conn = db()
    conn.execute(
        "INSERT INTO nodes(name,group_name,host,port,user,password,auth_type,auth_value,enabled,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (data["name"], data["group_name"], data["host"], data["port"], data["user"], data.get("auth_value", ""), data["auth_type"], data["auth_value"], 1, int(time.time())),
    )
    conn.commit()
    node_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.close()
    context.user_data.pop("add_step", None)
    context.user_data.pop("add_node", None)
    auth_text = "默认密码" if data["auth_type"] == "password" and data["auth_value"] == d["password"] else ("默认Key" if data["auth_type"] == "key" and data["auth_value"] == d["key_path"] else ("自定义密码" if data["auth_type"] == "password" else "自定义Key"))
    # 自动测试连接
    import subprocess
    try:
        r = subprocess.run(["ping", "-c", "1", "-W", "2", data["host"]], capture_output=True, timeout=3)
        ping_ok = r.returncode == 0
    except Exception:
        ping_ok = False
    ping_text = "\n\n✅ Ping 通过" if ping_ok else "\n\n⚠️ Ping 不通，请检查 IP"
    await update.effective_chat.send_message(
        f"✅ 节点已添加{ping_text}\n\n名称：{esc(data['name'])}\n分组：{esc(data['group_name'])}\n地址：{esc(data['host'])}:{data['port']}\n用户：{esc(data['user'])}\n认证：{esc(auth_text)}",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔌 测试连接", callback_data=f"node:test:{node_id}"), InlineKeyboardButton("📄 查看节点", callback_data=f"node:view:{node_id}")],
            [InlineKeyboardButton("🏠 首页", callback_data="home")],
        ]),
    )


async def show_node(query, node_id: int):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    state = get_node_state(node.id)
    status = "在线" if state["is_online"] else f"离线({state['fail_count']})"
    price = format_price(node)
    expiry = format_expiry(node.expires_at)
    expiry_left = days_left_text(node.expires_at) if int(node.expires_at or 0) > 0 else "未设置"
    text = (
        f"<b>{esc(node.name)}</b>\n"
        f"分组：<code>{esc(node.group_name)}</code>\n"
        f"地址：<code>{esc(node.host)}:{node.port}</code>\n"
        f"用户：<code>{esc(node.user)}</code>\n"
        f"认证：<code>{esc(auth_label(node))}</code>\n"
        f"状态：<code>{esc(status)}</code>\n"
        f"账单：<code>{esc(price)}</code>\n"
        f"到期：<code>{esc(expiry)}</code>\n"
        f"剩余：<code>{esc(expiry_left)}</code>"
    )
    if node.remark:
        text += f"\n备注：<code>{esc(node.remark)}</code>"
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🔌 测试", callback_data=f"node:test:{node.id}"), InlineKeyboardButton("📊 概览", callback_data=f"node:summary:{node.id}")],
        [InlineKeyboardButton("🐳 Docker", callback_data=f"node:docker:{node.id}"), InlineKeyboardButton("💰 账单", callback_data=f"node:billing:{node.id}")],
        [InlineKeyboardButton("✏️ 编辑", callback_data=f"node:edit:{node.id}"), InlineKeyboardButton("🗂 改分组", callback_data=f"node:setgroup:{node.id}")],
        [InlineKeyboardButton("🧰 批量同组", callback_data=f"batch:group:{node.group_name}"), InlineKeyboardButton("🗑 删除", callback_data=f"node:delask:{node.id}")],
        [InlineKeyboardButton("🏠 首页", callback_data="home")],
    ]))


async def node_test(query, node_id: int):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    await query.edit_message_text(f"正在测试 <b>{esc(node.name)}</b>…", parse_mode=ParseMode.HTML)
    try:
        code, out, err = await run_ssh(node, "echo ok && hostname && whoami", timeout=10)
        if code == 0:
            lines = [x for x in out.splitlines() if x.strip()]
            host = lines[1] if len(lines) > 1 else "-"
            user = lines[2] if len(lines) > 2 else node.user
            text = f"✅ 连接成功\n\n节点：<b>{esc(node.name)}</b>\n主机：<code>{esc(host)}</code>\n用户：<code>{esc(user)}</code>"
        else:
            text = f"❌ 连接失败\n\n<code>{esc(err or out or '未知错误')}</code>"
    except Exception as e:
        text = f"❌ 连接失败\n\n<code>{esc(str(e))}</code>"
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ 返回节点", callback_data=f"node:view:{node_id}")],
    ]))


async def node_summary(query, node_id: int):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    await query.edit_message_text(f"正在拉取 <b>{esc(node.name)}</b> 概览…", parse_mode=ParseMode.HTML)
    try:
        info = await collect_node_summary(node)
        text = (
            f"<b>{esc(node.name)}</b>\n\n"
            f"主机名：<code>{esc(info.get('HOST','-'))}</code>\n"
            f"运行时间：<code>{esc(info.get('UP','-'))}</code>\n"
            f"负载：<code>{esc(info.get('LOAD','-'))}</code>\n"
            f"内存：<code>{esc(info.get('MEM','-'))}</code>\n"
            f"磁盘：<code>{esc(info.get('DISK','-'))}</code>"
        )
    except Exception as e:
        text = f"❌ 拉取失败\n\n<code>{esc(str(e))}</code>"
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ 返回节点", callback_data=f"node:view:{node_id}")],
    ]))


async def nodes_overview(query):
    nodes = all_nodes()
    if not nodes:
        await query.edit_message_text("还没有节点。", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ 添加节点", callback_data="nodes:add")], [InlineKeyboardButton("🏠 首页", callback_data="home")]]))
        return
    await query.edit_message_text("正在拉取全部节点概览…")
    results = await asyncio.gather(*[collect_node_summary(n) for n in nodes], return_exceptions=True)
    now = int(time.time())
    lines = ["<b>批量总览</b>"]
    ok = 0
    for node, result in zip(nodes, results):
        # 到期颜色
        exp_icon = ""
        if int(node.expires_at or 0) > 0:
            days_left = math.floor((int(node.expires_at) - now) / 86400)
            if days_left < 0:
                exp_icon = " ⛔"
            elif days_left <= 3:
                exp_icon = " 🔴"
            elif days_left <= 7:
                exp_icon = " 🟡"
        # 费用
        price_tag = f" | 💰{esc(format_price(node))}" if float(node.monthly_price or 0) > 0 else ""
        if isinstance(result, Exception):
            lines.append(f"\n🔴 <b>{esc(node.name)}</b> [{esc(node.group_name)}]{exp_icon}{price_tag}\n<code>{esc(str(result))[:120]}</code>")
        else:
            ok += 1
            lines.append(f"\n🟢 <b>{esc(node.name)}</b> [{esc(node.group_name)}]{exp_icon}{price_tag}\n负载 <code>{esc(result.get('LOAD','-'))}</code> | 内存 <code>{esc(result.get('MEM','-'))}</code> | 磁盘 <code>{esc(result.get('DISK','-'))}</code>")
    lines.insert(1, f"在线：<code>{ok}/{len(nodes)}</code>\n")
    await query.edit_message_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 刷新", callback_data="nodes:overview")],
        [InlineKeyboardButton("🏠 首页", callback_data="home")],
    ]))


async def show_auth_settings(query):
    d = defaults_dict()
    text = (
        "<b>默认认证设置</b>\n\n"
        f"默认用户：<code>{esc(d['user'])}</code>\n"
        f"默认端口：<code>{d['port']}</code>\n"
        f"默认密码：<code>{'已设置' if d['password'] else '未设置'}</code>\n"
        f"默认Key：<code>{esc(d['key_path'])}</code>"
    )
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("改默认密码", callback_data="settings:set_default_password"), InlineKeyboardButton("改默认Key", callback_data="settings:set_default_key")],
        [InlineKeyboardButton("改默认用户", callback_data="settings:set_default_user"), InlineKeyboardButton("改默认端口", callback_data="settings:set_default_port")],
        [InlineKeyboardButton("🏠 首页", callback_data="home")],
    ]))


async def show_alert_settings(query):
    text = "<b>告警设置</b>\n\n" + alert_settings_text()
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("改巡检间隔", callback_data="settings:set_check_interval"), InlineKeyboardButton("改失败阈值", callback_data="settings:set_fail_threshold")],
        [InlineKeyboardButton("CPU 阈值", callback_data="settings:set_cpu_alert_threshold"), InlineKeyboardButton("内存阈值", callback_data="settings:set_mem_alert_threshold")],
        [InlineKeyboardButton("磁盘阈值", callback_data="settings:set_disk_alert_threshold"), InlineKeyboardButton("到期提醒天数", callback_data="settings:set_expire_remind_days")],
        [InlineKeyboardButton("切换掉线通知", callback_data="settings:toggle_notify_offline"), InlineKeyboardButton("切换恢复通知", callback_data="settings:toggle_notify_recovery")],
        [InlineKeyboardButton("立即巡检一次", callback_data="monitor:run")],
        [InlineKeyboardButton("🏠 首页", callback_data="home")],
    ]))


async def show_billing_summary(query):
    nodes = all_nodes()
    priced = [n for n in nodes if float(n.monthly_price or 0) > 0]
    upcoming = expiring_nodes(30)
    group_rows = monthly_cost_by_group()
    top_nodes = [n for n in top_cost_nodes(8) if float(n.monthly_price or 0) > 0]
    lines = ["<b>账单总览</b>"]
    lines.append(f"节点：<code>{len(nodes)}</code> | 已填账单：<code>{len(priced)}</code>")
    lines.append("总成本：<code>多币种，按节点明细查看</code>")
    if group_rows:
        lines.append("\n<b>分组覆盖</b>")
        for g, c, s in group_rows[:8]:
            lines.append(f"• <b>{esc(g)}</b>：<code>{c} 台</code>")
    if top_nodes:
        lines.append("\n<b>账单节点</b>")
        for n in top_nodes[:5]:
            lines.append(f"• <b>{esc(n.name)}</b> [{esc(n.group_name)}]：<code>{esc(format_price(n))}</code>")
    if upcoming:
        lines.append("\n<b>30 天内到期</b>")
        for n in upcoming[:5]:
            lines.append(f"• <b>{esc(n.name)}</b>：<code>{esc(format_expiry(n.expires_at))}</code>（{esc(days_left_text(n.expires_at))}）")
    else:
        lines.append("\n30 天内没有到期节点。")
    await query.edit_message_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("⏰ 即将到期", callback_data="billing:expiring"), InlineKeyboardButton("🔄 刷新", callback_data="billing:summary")],
        [InlineKeyboardButton("🏠 首页", callback_data="home")],
    ]))


async def show_expiring_nodes(query, limit_days: int = 30):
    rows = expiring_nodes(limit_days)
    if not rows:
        await query.edit_message_text(f"未来 {limit_days} 天没有到期节点。", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 返回账单", callback_data="billing:summary")],
            [InlineKeyboardButton("🏠 首页", callback_data="home")],
        ]))
        return
    lines = [f"<b>{limit_days} 天内到期</b>"]
    for n in rows[:30]:
        price = f" | {format_price(n)}" if float(n.monthly_price or 0) > 0 else ""
        remark = f"\n<code>{esc(n.remark)}</code>" if n.remark else ""
        lines.append(f"\n• <b>{esc(n.name)}</b> [{esc(n.group_name)}]\n<code>{esc(format_expiry(n.expires_at))}</code>（{esc(days_left_text(n.expires_at))}）{price}{remark}")
    await query.edit_message_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("7 天内", callback_data="billing:expiring:7"), InlineKeyboardButton("30 天内", callback_data="billing:expiring:30")],
        [InlineKeyboardButton("💰 返回账单", callback_data="billing:summary")],
        [InlineKeyboardButton("🏠 首页", callback_data="home")],
    ]))


async def show_batch_menu(query):
    buttons = [
        [InlineKeyboardButton("🖥 预置只读命令", callback_data="batch:readonly")],
        [InlineKeyboardButton("⌨️ 自定义只读命令", callback_data="batchcustom:menu")],
        [InlineKeyboardButton("⚙️ 批量服务管理", callback_data="batchsvc:menu")],
        [InlineKeyboardButton("🏠 首页", callback_data="home")],
    ]
    await query.edit_message_text("选择批量操作类型：", reply_markup=InlineKeyboardMarkup(buttons))


async def show_batch_actions(query, scope: str):
    buttons = [[InlineKeyboardButton(title, callback_data=f"batch:run:{scope}:{action}")] for action, (title, _) in READ_ONLY_BATCH.items()]
    buttons.append([InlineKeyboardButton("⬅️ 返回", callback_data="batch:menu")])
    await query.edit_message_text(f"范围：<code>{esc(scope)}</code>\n选择一个只读批量动作：", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))


async def run_batch_action(query, scope: str, action: str):
    nodes = all_nodes() if scope == "all" else nodes_by_group(scope)
    if not nodes:
        await query.edit_message_text("这个范围下没有节点。", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回", callback_data="batch:menu")]]))
        return
    title, command = READ_ONLY_BATCH[action]
    await query.edit_message_text(f"正在执行 {title}…")
    results = await asyncio.gather(*[run_ssh(n, command, timeout=15) for n in nodes], return_exceptions=True)
    lines = [f"<b>{esc(title)}</b> | 范围 <code>{esc(scope)}</code>"]
    for node, result in zip(nodes, results):
        if isinstance(result, Exception):
            lines.append(f"\n🔴 <b>{esc(node.name)}</b>\n<code>{esc(str(result))[:250]}</code>")
        else:
            code, out, err = result
            body = (out or err or "(空输出)")[:350]
            status = "🟢" if code == 0 else "🔴"
            lines.append(f"\n{status} <b>{esc(node.name)}</b>\n<pre>{esc(body)}</pre>")
    await query.edit_message_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ 返回批量菜单", callback_data="batch:menu")],
        [InlineKeyboardButton("🏠 首页", callback_data="home")],
    ]))


async def show_batch_custom_scope(query):
    groups = list_groups()
    buttons = [[InlineKeyboardButton("全部节点", callback_data="batchcustom:scope:all")]]
    for g, c in groups[:20]:
        buttons.append([InlineKeyboardButton(f"分组 {g} ({c})", callback_data=f"batchcustom:scope:{g}")])
    buttons.append([InlineKeyboardButton("⬅️ 返回批量菜单", callback_data="batch:menu")])
    await query.edit_message_text("选择自定义只读命令范围：", reply_markup=InlineKeyboardMarkup(buttons))


async def show_batch_service_scope(query):
    groups = list_groups()
    buttons = [[InlineKeyboardButton("全部节点", callback_data="batchsvc:scope:all")]]
    for g, c in groups[:20]:
        buttons.append([InlineKeyboardButton(f"分组 {g} ({c})", callback_data=f"batchsvc:scope:{g}")])
    buttons.append([InlineKeyboardButton("⬅️ 返回批量菜单", callback_data="batch:menu")])
    await query.edit_message_text("选择批量服务管理范围：", reply_markup=InlineKeyboardMarkup(buttons))


async def show_batch_service_actions(query, scope: str):
    buttons = [
        [InlineKeyboardButton("查看状态", callback_data=f"batchsvc:ask:{scope}:status"), InlineKeyboardButton("重启服务", callback_data=f"batchsvc:ask:{scope}:restart")],
        [InlineKeyboardButton("启动服务", callback_data=f"batchsvc:ask:{scope}:start"), InlineKeyboardButton("停止服务", callback_data=f"batchsvc:ask:{scope}:stop")],
        [InlineKeyboardButton("⬅️ 返回范围选择", callback_data="batchsvc:menu")],
    ]
    await query.edit_message_text(f"范围：<code>{esc(scope)}</code>\n选择服务动作：", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))


async def run_batch_service_action(query, scope: str, service_name: str, action: str):
    nodes = all_nodes() if scope == "all" else nodes_by_group(scope)
    if not nodes:
        await query.edit_message_text("这个范围下没有节点。", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回", callback_data="batchsvc:menu")]]))
        return
    service_name = service_name.strip()
    action_label = SERVICE_ACTIONS.get(action, action)
    if action == "status":
        command = f"systemctl status --no-pager --full {service_name} 2>&1 || systemctl is-active {service_name} 2>&1 || true"
        timeout = 20
    else:
        command = f"systemctl {action} {service_name} 2>&1"
        timeout = 25
    await query.edit_message_text(f"正在对 <code>{esc(scope)}</code> 执行 <b>{esc(action_label)}</b>：<code>{esc(service_name)}</code>", parse_mode=ParseMode.HTML)
    results = await asyncio.gather(*[run_ssh(n, command, timeout=timeout) for n in nodes], return_exceptions=True)
    lines = [f"<b>批量服务 {esc(action_label)}</b> | 范围 <code>{esc(scope)}</code> | 服务 <code>{esc(service_name)}</code>"]
    for node, result in zip(nodes, results):
        if isinstance(result, Exception):
            lines.append(f"\n🔴 <b>{esc(node.name)}</b>\n<code>{esc(str(result))[:220]}</code>")
        else:
            code, out, err = result
            body = (out or err or "(空输出)")[:320]
            status = "🟢" if code == 0 else "🔴"
            lines.append(f"\n{status} <b>{esc(node.name)}</b>\n<pre>{esc(body)}</pre>")
    await query.edit_message_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️ 返回服务菜单", callback_data=f"batchsvc:scope:{scope}")],
        [InlineKeyboardButton("⬅️ 返回批量菜单", callback_data="batch:menu")],
        [InlineKeyboardButton("🏠 首页", callback_data="home")],
    ]))


async def show_docker(query, node_id: int):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    await query.edit_message_text(f"正在拉取 <b>{esc(node.name)}</b> 的 Docker 列表…", parse_mode=ParseMode.HTML)
    try:
        items = await collect_docker_list(node)
        if items is None:
            await query.edit_message_text("这台机器没装 Docker。", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回节点", callback_data=f"node:view:{node.id}")]]))
            return
        if not items:
            await query.edit_message_text("这台机器当前没有容器。", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回节点", callback_data=f"node:view:{node.id}")]]))
            return
        lines = [f"<b>{esc(node.name)}</b> Docker"]
        buttons = []
        for item in items[:12]:
            lines.append(f"\n• <b>{esc(item['name'])}</b>\n<code>{esc(item['status'])}</code>\n<code>{esc(item['image'])}</code>")
            buttons.append([InlineKeyboardButton(f"⚙️ {item['name']}", callback_data=f"docker:menu:{node.id}:{item['name']}")])
        buttons.append([InlineKeyboardButton("⬅️ 返回节点", callback_data=f"node:view:{node.id}")])
        await query.edit_message_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        await query.edit_message_text(f"❌ 获取 Docker 列表失败\n\n<code>{esc(str(e))}</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回节点", callback_data=f"node:view:{node.id}")]]))


async def show_docker_menu(query, node_id: int, container: str):
    await query.edit_message_text(f"容器：<b>{esc(container)}</b>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ 启动", callback_data=f"docker:act:{node_id}:{container}:start"), InlineKeyboardButton("⏹️ 停止", callback_data=f"docker:act:{node_id}:{container}:stop")],
        [InlineKeyboardButton("🔄 重启", callback_data=f"docker:act:{node_id}:{container}:restart")],
        [InlineKeyboardButton("📜 日志", callback_data=f"docker:logs:{node_id}:{container}")],
        [InlineKeyboardButton("⬅️ 返回 Docker", callback_data=f"node:docker:{node_id}")],
    ]))


async def do_docker_action(query, node_id: int, container: str, action: str):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    await query.edit_message_text(f"正在执行 {action}…")
    try:
        code, out, err = await docker_action(node, container, action)
        text = f"{'✅' if code == 0 else '❌'} {action}\n\n<code>{esc(out or err or '(空输出)')[:3500]}</code>"
    except Exception as e:
        text = f"❌ 执行失败\n\n<code>{esc(str(e))}</code>"
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回容器菜单", callback_data=f"docker:menu:{node_id}:{container}")]]))


async def show_docker_logs(query, node_id: int, container: str):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    await query.edit_message_text("正在读取日志…")
    try:
        code, out, err = await docker_logs(node, container)
        text = f"<b>{esc(container)}</b> 日志\n\n<pre>{esc((out or err or '(空日志)')[:3500])}</pre>"
    except Exception as e:
        text = f"❌ 日志获取失败\n\n<code>{esc(str(e))}</code>"
    await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回容器菜单", callback_data=f"docker:menu:{node_id}:{container}")]]))


async def show_billing_menu(query, node_id: int):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    price = format_price(node)
    expiry = format_expiry(node.expires_at)
    left = days_left_text(node.expires_at) if int(node.expires_at or 0) > 0 else "未设置"
    remark = node.remark or "未设置"
    await query.edit_message_text(
        f"<b>账单信息</b>\n\n节点：<b>{esc(node.name)}</b>\n备注：<code>{esc(remark)}</code>\n账单：<code>{esc(price)}</code>\n到期：<code>{esc(expiry)}</code>\n剩余：<code>{esc(left)}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("改备注", callback_data=f"node:editfield:{node_id}:remark"), InlineKeyboardButton("改金额", callback_data=f"node:editfield:{node_id}:monthly_price")],
            [InlineKeyboardButton("周期", callback_data=f"node:cycle:{node_id}"), InlineKeyboardButton("货币", callback_data=f"node:currency:{node_id}")],
            [InlineKeyboardButton("改到期日", callback_data=f"node:editfield:{node_id}:expires_at")],
            [InlineKeyboardButton("⬅️ 返回节点", callback_data=f"node:view:{node_id}")],
        ]),
    )


async def show_cycle_menu(query, node_id: int):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    await query.edit_message_text(
        f"<b>选择账单周期</b>\n\n节点：<b>{esc(node.name)}</b>\n当前：<code>{esc(cycle_label(node.price_cycle))}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("月付", callback_data=f"node:setcycle:{node_id}:month"), InlineKeyboardButton("季付", callback_data=f"node:setcycle:{node_id}:quarter")],
            [InlineKeyboardButton("年付", callback_data=f"node:setcycle:{node_id}:year")],
            [InlineKeyboardButton("⬅️ 返回账单", callback_data=f"node:billing:{node_id}")],
        ]),
    )


async def show_currency_menu(query, node_id: int):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    current = (node.price_currency or "U").upper()
    await query.edit_message_text(
        f"<b>选择货币单位</b>\n\n节点：<b>{esc(node.name)}</b>\n当前：<code>{esc(current)}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("U", callback_data=f"node:setcurrency:{node_id}:U"), InlineKeyboardButton("CNY", callback_data=f"node:setcurrency:{node_id}:CNY")],
            [InlineKeyboardButton("USD", callback_data=f"node:setcurrency:{node_id}:USD"), InlineKeyboardButton("HKD", callback_data=f"node:setcurrency:{node_id}:HKD")],
            [InlineKeyboardButton("TWD", callback_data=f"node:setcurrency:{node_id}:TWD")],
            [InlineKeyboardButton("⬅️ 返回账单", callback_data=f"node:billing:{node_id}")],
        ]),
    )


async def show_edit_menu(query, node_id: int):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    await query.edit_message_text(
        f"编辑节点：<b>{esc(node.name)}</b>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("改名称", callback_data=f"node:editfield:{node_id}:name"), InlineKeyboardButton("改地址", callback_data=f"node:editfield:{node_id}:host")],
            [InlineKeyboardButton("改端口", callback_data=f"node:editfield:{node_id}:port"), InlineKeyboardButton("改用户", callback_data=f"node:editfield:{node_id}:user")],
            [InlineKeyboardButton("改密码", callback_data=f"node:editauth:{node_id}:password"), InlineKeyboardButton("改Key", callback_data=f"node:editauth:{node_id}:key")],
            [InlineKeyboardButton("💰 账单信息", callback_data=f"node:billing:{node_id}")],
            [InlineKeyboardButton("⬅️ 返回节点", callback_data=f"node:view:{node_id}")],
        ]),
    )

async def ask_delete(query, node_id: int):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    await query.edit_message_text(f"确认删除 <b>{esc(node.name)}</b> 吗？", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("⚠️ 确认删除", callback_data=f"node:del:{node.id}")],
        [InlineKeyboardButton("取消", callback_data=f"node:view:{node.id}")],
    ]))


async def delete_node(query, node_id: int):
    node = load_node(node_id)
    if not node:
        await query.answer("节点不存在", show_alert=True)
        return
    conn = db()
    conn.execute("DELETE FROM node_state WHERE node_id=?", (node_id,))
    conn.execute("DELETE FROM nodes WHERE id=?", (node_id,))
    conn.commit()
    conn.close()
    await query.edit_message_text(f"✅ 已删除 {esc(node.name)}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 首页", callback_data="home")]]))


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    step = context.user_data.get("add_step")
    if not step:
        return
    value = (update.message.text or "").strip()
    prompts = add_prompts()
    defaults = defaults_dict()
    data = context.user_data.get("add_node", {})

    if step == "port":
        value = value or str(defaults["port"])
        if not value.isdigit():
            await update.message.reply_text("端口必须是数字，重新发。")
            return
        data["port"] = int(value)
    elif step == "user":
        data["user"] = value or defaults["user"]
    elif step == "group_name":
        data["group_name"] = value or "default"
    elif step == "custom_password":
        if not value:
            await update.message.reply_text("密码不能为空。")
            return
        data["auth_type"] = "password"
        data["auth_value"] = value
        context.user_data["add_node"] = data
        await save_node_from_context(update, context)
        return
    elif step == "custom_key":
        if not value:
            await update.message.reply_text("Key 路径或私钥内容不能为空。")
            return
        data["auth_type"] = "key"
        data["auth_value"] = maybe_store_private_key(value, "node")
        context.user_data["add_node"] = data
        await save_node_from_context(update, context)
        return
    elif step == "edit_group_name":
        node_id = context.user_data.get("edit_node_id")
        if not node_id:
            await update.message.reply_text("没有找到要修改的节点。")
            return
        group_name = value or "default"
        update_node_group(int(node_id), group_name)
        context.user_data.pop("add_step", None)
        context.user_data.pop("edit_node_id", None)
        await update.message.reply_text(f"✅ 分组已改为：{group_name}", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📄 返回节点", callback_data=f"node:view:{node_id}")]]))
        return
    elif step in {"edit_name", "edit_host", "edit_port", "edit_user", "edit_auth_password", "edit_auth_key", "edit_remark", "edit_monthly_price", "edit_expires_at"}:
        node_id = context.user_data.get("edit_node_id")
        if not node_id:
            await update.message.reply_text("没有找到要修改的节点。")
            return
        if step == "edit_port":
            if not value.isdigit():
                await update.message.reply_text("端口必须是数字。")
                return
            update_node_field(int(node_id), "port", int(value))
        elif step == "edit_name":
            update_node_field(int(node_id), "name", value)
        elif step == "edit_host":
            update_node_field(int(node_id), "host", value)
        elif step == "edit_user":
            update_node_field(int(node_id), "user", value)
        elif step == "edit_auth_password":
            update_node_field(int(node_id), "auth_type", "password")
            update_node_field(int(node_id), "auth_value", value)
        elif step == "edit_auth_key":
            update_node_field(int(node_id), "auth_type", "key")
            update_node_field(int(node_id), "auth_value", maybe_store_private_key(value, "edit"))
        elif step == "edit_remark":
            update_node_field(int(node_id), "remark", "" if value == "-" else value)
        elif step == "edit_monthly_price":
            try:
                price = float(value)
            except Exception:
                await update.message.reply_text("金额格式不对，重新发，例如 4.99")
                return
            update_node_field(int(node_id), "monthly_price", price)
        elif step == "edit_expires_at":
            if value == "0":
                update_node_field(int(node_id), "expires_at", 0)
            else:
                try:
                    ts = int(datetime.strptime(value, "%Y-%m-%d").timestamp())
                except Exception:
                    await update.message.reply_text("日期格式不对，按 YYYY-MM-DD 发。")
                    return
                update_node_field(int(node_id), "expires_at", ts)
        context.user_data.pop("add_step", None)
        context.user_data.pop("edit_node_id", None)
        await update.message.reply_text("✅ 节点已更新。", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📄 返回节点", callback_data=f"node:view:{node_id}")]]))
        return
    elif step == "batch_service_name":
        scope = context.user_data.get("batch_service_scope")
        action = context.user_data.get("batch_service_action")
        if not scope or not action:
            await update.message.reply_text("批量服务上下文丢了，请重新进入批量服务菜单。")
            return
        if not value:
            await update.message.reply_text("服务名不能为空。")
            return
        context.user_data.pop("add_step", None)
        context.user_data.pop("batch_service_scope", None)
        context.user_data.pop("batch_service_action", None)
        wait = await update.message.reply_text(f"收到，开始批量{SERVICE_ACTIONS.get(action, action)}：{value}")
        class _Q:
            async def edit_message_text(self, *args, **kwargs):
                return await wait.reply_text(*args, **kwargs)
        await run_batch_service_action(_Q(), scope, value, action)
        return
    elif step == "batch_readonly_command":
        scope = context.user_data.get("batch_custom_scope")
        if not scope:
            await update.message.reply_text("批量命令上下文丢了，请重新进入自定义只读命令菜单。")
            return
        if not is_safe_readonly_command(value):
            await update.message.reply_text("这条命令不在只读白名单里。\n可用示例：df -h /、free -m、docker ps、systemctl status nginx")
            return
        context.user_data.pop("add_step", None)
        context.user_data.pop("batch_custom_scope", None)
        wait = await update.message.reply_text(f"收到，开始批量执行只读命令：{value}")
        class _Q:
            async def edit_message_text(self, *args, **kwargs):
                return await wait.reply_text(*args, **kwargs)
        nodes = all_nodes() if scope == "all" else nodes_by_group(scope)
        if not nodes:
            await wait.reply_text("这个范围下没有节点。")
            return
        results = await asyncio.gather(*[run_ssh(n, value, timeout=20) for n in nodes], return_exceptions=True)
        lines = [f"<b>批量只读命令</b> | 范围 <code>{esc(scope)}</code>", f"<code>{esc(value)}</code>"]
        for node, result in zip(nodes, results):
            if isinstance(result, Exception):
                lines.append(f"\n🔴 <b>{esc(node.name)}</b>\n<code>{esc(str(result))[:220]}</code>")
            else:
                code, out, err = result
                body = (out or err or "(空输出)")[:320]
                status = "🟢" if code == 0 else "🔴"
                lines.append(f"\n{status} <b>{esc(node.name)}</b>\n<pre>{esc(body)}</pre>")
        await _Q().edit_message_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("⬅️ 返回自定义命令", callback_data="batchcustom:menu")],
            [InlineKeyboardButton("🏠 首页", callback_data="home")],
        ]))
        return
    elif step in {"set_default_password", "set_default_key", "set_default_user", "set_default_port", "set_check_interval", "set_fail_threshold", "set_cpu_alert_threshold", "set_mem_alert_threshold", "set_disk_alert_threshold", "set_expire_remind_days"}:
        if step in {"set_default_port", "set_check_interval", "set_fail_threshold", "set_cpu_alert_threshold", "set_mem_alert_threshold", "set_disk_alert_threshold", "set_expire_remind_days"} and not value.isdigit():
            await update.message.reply_text("这里必须是数字，重新发。")
            return
        if step == "set_default_password":
            set_setting("default_password", value)
        elif step == "set_default_key":
            set_setting("default_key_path", maybe_store_private_key(value, "default"))
        elif step == "set_default_user":
            set_setting("default_user", value or "root")
        elif step == "set_default_port":
            set_setting("default_port", value)
        elif step == "set_check_interval":
            set_setting("check_interval", value)
        elif step == "set_fail_threshold":
            set_setting("fail_threshold", value)
        elif step == "set_cpu_alert_threshold":
            set_setting("cpu_alert_threshold", value)
        elif step == "set_mem_alert_threshold":
            set_setting("mem_alert_threshold", value)
        elif step == "set_disk_alert_threshold":
            set_setting("disk_alert_threshold", value)
        elif step == "set_expire_remind_days":
            set_setting("expire_remind_days", value)
        context.user_data.pop("add_step", None)
        await update.message.reply_text("✅ 设置已更新。", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 首页", callback_data="home")]]))
        return
    else:
        if not value:
            await update.message.reply_text("这一项不能为空。")
            return
        data[step] = value

    if step in ADD_FLOW:
        idx = ADD_FLOW.index(step)
        if idx == len(ADD_FLOW) - 1:
            context.user_data["add_node"] = data
            context.user_data["add_step"] = "auth_choice"
            await prompt_auth_choice(update.message, data)
            return
        next_step = ADD_FLOW[idx + 1]
        context.user_data["add_node"] = data
        context.user_data["add_step"] = next_step
        await update.message.reply_text(prompts[next_step], parse_mode=ParseMode.HTML)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    q = update.callback_query
    await q.answer()
    data = q.data or ""

    if data == "home":
        await start(update, context)
    elif data == "nodes:list":
        await show_nodes(q)
    elif data == "nodes:overview":
        await nodes_overview(q)
    elif data == "nodes:add":
        await begin_add(q, context)
    elif data == "groups:list":
        await show_groups(q)
    elif data.startswith("groups:view:"):
        await show_nodes(q, data.split(":", 2)[2])
    elif data == "settings:auth":
        await show_auth_settings(q)
    elif data == "settings:alerts":
        await show_alert_settings(q)
    elif data == "billing:summary":
        await show_billing_summary(q)
    elif data == "billing:expiring":
        await show_expiring_nodes(q, 30)
    elif data.startswith("billing:expiring:"):
        await show_expiring_nodes(q, int(data.split(":")[-1]))
    elif data.startswith("settings:set_"):
        step = data.split(":", 1)[1]
        context.user_data["add_step"] = step
        await q.edit_message_text(add_prompts()[step], parse_mode=ParseMode.HTML)
    elif data == "settings:toggle_notify_offline":
        set_setting("notify_offline", "0" if get_setting("notify_offline") == "1" else "1")
        await show_alert_settings(q)
    elif data == "settings:toggle_notify_recovery":
        set_setting("notify_recovery", "0" if get_setting("notify_recovery") == "1" else "1")
        await show_alert_settings(q)
    elif data == "monitor:run":
        await q.edit_message_text("正在手动巡检一次…")
        await monitor_once(context.application)
        await q.message.reply_text("✅ 已手动巡检一次。", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔔 返回告警设置", callback_data="settings:alerts")]]))
    elif data.startswith("auth:"):
        current = context.user_data.get("add_node")
        if not current:
            await q.edit_message_text("当前没有进行中的添加流程。", reply_markup=main_menu())
            return
        defaults = defaults_dict()
        choice = data.split(":", 1)[1]
        if choice == "default_password":
            current["auth_type"] = "password"
            current["auth_value"] = defaults["password"]
            context.user_data["add_node"] = current
            context.user_data["add_step"] = None
            await save_node_from_context(update, context)
        elif choice == "default_key":
            current["auth_type"] = "key"
            current["auth_value"] = defaults["key_path"]
            context.user_data["add_node"] = current
            context.user_data["add_step"] = None
            await save_node_from_context(update, context)
        elif choice == "custom_password":
            context.user_data["add_step"] = "custom_password"
            await q.edit_message_text(add_prompts()["custom_password"], parse_mode=ParseMode.HTML)
        elif choice == "custom_key":
            context.user_data["add_step"] = "custom_key"
            await q.edit_message_text(add_prompts()["custom_key"], parse_mode=ParseMode.HTML)
    elif data.startswith("node:setgroupto:"):
        _, _, rest = data.split(":", 2)
        node_id_str, group_name = rest.split(":", 1)
        node_id = int(node_id_str)
        update_node_group(node_id, group_name)
        await q.edit_message_text(f"✅ 分组已改为：<code>{esc(group_name)}</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📄 返回节点", callback_data=f"node:view:{node_id}")]]))
    elif data.startswith("node:setgroupinput:"):
        node_id = int(data.split(":")[-1])
        node = load_node(node_id)
        if not node:
            await q.answer("节点不存在", show_alert=True)
            return
        context.user_data["add_step"] = "edit_group_name"
        context.user_data["edit_node_id"] = node_id
        groups = ', '.join([g for g,_ in list_groups()][:8]) or 'default'
        await q.edit_message_text(f"当前节点：<b>{esc(node.name)}</b>\n当前分组：<code>{esc(node.group_name)}</code>\n\n直接发新的分组名即可。\n现有分组：<code>{esc(groups)}</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回分组选择", callback_data=f"node:setgroup:{node_id}")]]))
    elif data.startswith("node:setgroup:"):
        node_id = int(data.split(":")[-1])
        node = load_node(node_id)
        if not node:
            await q.answer("节点不存在", show_alert=True)
            return
        await q.edit_message_text(
            f"当前节点：<b>{esc(node.name)}</b>\n当前分组：<code>{esc(node.group_name)}</code>\n\n选一个现有分组，或者新建分组。",
            parse_mode=ParseMode.HTML,
            reply_markup=choose_group_buttons(node_id),
        )
    elif data.startswith("node:billing:"):
        await show_billing_menu(q, int(data.split(":")[-1]))
    elif data.startswith("node:cycle:"):
        await show_cycle_menu(q, int(data.split(":")[-1]))
    elif data.startswith("node:setcycle:"):
        _, _, node_id, cycle = data.split(":", 3)
        update_node_field(int(node_id), "price_cycle", cycle)
        await show_billing_menu(q, int(node_id))
    elif data.startswith("node:currency:"):
        await show_currency_menu(q, int(data.split(":")[-1]))
    elif data.startswith("node:setcurrency:"):
        _, _, node_id, currency = data.split(":", 3)
        update_node_field(int(node_id), "price_currency", currency.upper())
        await show_billing_menu(q, int(node_id))
    elif data.startswith("node:edit:"):
        await show_edit_menu(q, int(data.split(":")[-1]))
    elif data.startswith("node:editfield:"):
        _, _, node_id, field = data.split(":", 3)
        context.user_data["add_step"] = f"edit_{field}"
        context.user_data["edit_node_id"] = int(node_id)
        await q.edit_message_text(add_prompts()[f"edit_{field}"], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回编辑", callback_data=f"node:edit:{node_id}")]]))
    elif data.startswith("node:editauth:"):
        _, _, node_id, kind = data.split(":", 3)
        step = "edit_auth_password" if kind == "password" else "edit_auth_key"
        context.user_data["add_step"] = step
        context.user_data["edit_node_id"] = int(node_id)
        await q.edit_message_text(add_prompts()[step], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回编辑", callback_data=f"node:edit:{node_id}")]]))
    elif data.startswith("node:view:"):
        await show_node(q, int(data.split(":")[-1]))
    elif data.startswith("node:test:"):
        await node_test(q, int(data.split(":")[-1]))
    elif data.startswith("node:summary:"):
        await node_summary(q, int(data.split(":")[-1]))
    elif data.startswith("node:docker:"):
        await show_docker(q, int(data.split(":")[-1]))
    elif data.startswith("node:delask:"):
        await ask_delete(q, int(data.split(":")[-1]))
    elif data.startswith("node:del:"):
        await delete_node(q, int(data.split(":")[-1]))
    elif data == "batch:menu":
        await show_batch_menu(q)
    elif data == "batch:readonly":
        await show_batch_actions(q, "all")
    elif data == "batchcustom:menu":
        await show_batch_custom_scope(q)
    elif data.startswith("batchcustom:scope:"):
        scope = data.split(":", 2)[2]
        context.user_data["add_step"] = "batch_readonly_command"
        context.user_data["batch_custom_scope"] = scope
        await q.edit_message_text(f"范围：<code>{esc(scope)}</code>\n\n发我要批量执行的只读命令。\n示例：<code>df -h /</code>、<code>free -m</code>、<code>docker ps</code>、<code>systemctl status nginx</code>", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回范围选择", callback_data="batchcustom:menu")]]))
    elif data == "batchsvc:menu":
        await show_batch_service_scope(q)
    elif data.startswith("batchsvc:scope:"):
        await show_batch_service_actions(q, data.split(":", 2)[2])
    elif data.startswith("batchsvc:ask:"):
        _, _, scope, action = data.split(":", 3)
        context.user_data["add_step"] = "batch_service_name"
        context.user_data["batch_service_scope"] = scope
        context.user_data["batch_service_action"] = action
        await q.edit_message_text(f"范围：<code>{esc(scope)}</code>\n动作：<b>{esc(SERVICE_ACTIONS.get(action, action))}</b>\n\n发我要操作的 systemd 服务名。", parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ 返回服务菜单", callback_data=f"batchsvc:scope:{scope}")]]))
    elif data.startswith("batch:scope:"):
        await show_batch_actions(q, data.split(":", 2)[2])
    elif data.startswith("batch:run:"):
        _, _, scope, action = data.split(":", 3)
        await run_batch_action(q, scope, action)
    elif data.startswith("batch:group:"):
        await show_batch_actions(q, data.split(":", 2)[2])
    elif data.startswith("docker:menu:"):
        _, _, node_id, container = data.split(":", 3)
        await show_docker_menu(q, int(node_id), container)
    elif data.startswith("docker:act:"):
        _, _, node_id, container, action = data.split(":", 4)
        await do_docker_action(q, int(node_id), container, action)
    elif data.startswith("docker:logs:"):
        _, _, node_id, container = data.split(":", 3)
        await show_docker_logs(q, int(node_id), container)


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    context.user_data.pop("add_step", None)
    context.user_data.pop("add_node", None)
    await update.message.reply_text("已取消当前操作。", reply_markup=ReplyKeyboardRemove())
    await update.message.reply_text("主菜单", reply_markup=main_menu())


async def cmd_nodes(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    rows = all_nodes()
    if not rows:
        await update.message.reply_text("还没有节点，先 /add。")
        return
    buttons = [[InlineKeyboardButton(f"🖥️ {r.name} [{r.group_name}]", callback_data=f"node:view:{r.id}")] for r in rows[:50]]
    await update.message.reply_text("节点列表", reply_markup=InlineKeyboardMarkup(buttons))


async def cmd_billing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    nodes = all_nodes()
    priced = [n for n in nodes if float(n.monthly_price or 0) > 0]
    group_rows = monthly_cost_by_group()
    upcoming = expiring_nodes(30)
    top_nodes = [n for n in top_cost_nodes(8) if float(n.monthly_price or 0) > 0]
    lines = ["<b>账单总览</b>"]
    lines.append(f"节点：<code>{len(nodes)}</code> | 已填账单：<code>{len(priced)}</code>")
    lines.append("总成本：<code>多币种，按节点明细查看</code>")
    if group_rows:
        lines.append("\n<b>分组覆盖</b>")
        for g, c, s in group_rows[:8]:
            lines.append(f"• <b>{esc(g)}</b>：<code>{c} 台</code>")
    if top_nodes:
        lines.append("\n<b>账单节点</b>")
        for n in top_nodes[:5]:
            lines.append(f"• <b>{esc(n.name)}</b> [{esc(n.group_name)}]：<code>{esc(format_price(n))}</code>")
    if upcoming:
        lines.append("\n<b>30 天内到期</b>")
        for n in upcoming[:5]:
            lines.append(f"• <b>{esc(n.name)}</b>：<code>{esc(format_expiry(n.expires_at))}</code>（{esc(days_left_text(n.expires_at))}）")
    else:
        lines.append("\n30 天内没有到期节点。")
    await update.message.reply_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("⏰ 即将到期", callback_data="billing:expiring"), InlineKeyboardButton("🔄 刷新", callback_data="billing:summary")],
    ]))


def _find_node(name_or_ip: str):
    """模糊匹配节点名或IP"""
    nodes = all_nodes()
    q = name_or_ip.lower()
    # 精确匹配
    for n in nodes:
        if n.name.lower() == q or n.host == q:
            return n
    # 包含匹配
    for n in nodes:
        if q in n.name.lower() or q in n.host:
            return n
    return None


async def cmd_quick_exec(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/s 节点名 命令 — 快捷执行"""
    if not await guard(update):
        return
    args = context.args
    if not args or len(args) < 2:
        await update.message.reply_text("用法：<code>/s 节点名 命令</code>\n例：<code>/s 灵车 df -h</code>", parse_mode=ParseMode.HTML)
        return
    name = args[0]
    cmd = " ".join(args[1:])
    node = _find_node(name)
    if not node:
        await update.message.reply_text(f"❌ 找不到节点：<code>{esc(name)}</code>", parse_mode=ParseMode.HTML)
        return
    if not is_safe_readonly_command(cmd):
        await update.message.reply_text("⚠️ 只允许只读命令。", parse_mode=ParseMode.HTML)
        return
    wait = await update.message.reply_text(f"⏳ <code>{esc(node.name)}</code> 执行中…", parse_mode=ParseMode.HTML)
    try:
        out = await run_ssh(node, cmd, timeout=15)
        text = f"🖥 <b>{esc(node.name)}</b> [{esc(node.host)}]\n$ <code>{esc(cmd)}</code>\n\n<code>{esc(out[:3500])}</code>"
        if len(out) > 3500:
            text += f"\n\n…截断（共{len(out)}字符）"
    except Exception as e:
        text = f"❌ <code>{esc(node.name)}</code> 执行失败\n<code>{esc(str(e)[:300])}</code>"
    await wait.reply_text(text[:4096], parse_mode=ParseMode.HTML)


async def cmd_group_exec(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/g 分组名 命令 — 分组批量执行"""
    if not await guard(update):
        return
    args = context.args
    if not args or len(args) < 2:
        await update.message.reply_text("用法：<code>/g 分组名 命令</code>\n例：<code>/g VPS uptime</code>", parse_mode=ParseMode.HTML)
        return
    group = args[0]
    cmd = " ".join(args[1:])
    if not is_safe_readonly_command(cmd):
        await update.message.reply_text("⚠️ 只允许只读命令。", parse_mode=ParseMode.HTML)
        return
    nodes = nodes_by_group(group)
    if not nodes:
        await update.message.reply_text(f"❌ 分组 <code>{esc(group)}</code> 下没有节点。", parse_mode=ParseMode.HTML)
        return
    wait = await update.message.reply_text(f"⏳ 对 <code>{esc(group)}</code>（{len(nodes)}台）执行中…", parse_mode=ParseMode.HTML)
    results = await asyncio.gather(*[run_ssh(n, cmd, timeout=15) for n in nodes], return_exceptions=True)
    lines = [f"📊 <b>{esc(group)}</b> | $ <code>{esc(cmd)}</code>"]
    for node, result in zip(nodes, results):
        if isinstance(result, Exception):
            lines.append(f"\n🔴 <b>{esc(node.name)}</b>：<code>{esc(str(result)[:120])}</code>")
        else:
            lines.append(f"\n🟢 <b>{esc(node.name)}</b>：<code>{esc(result[:500])}</code>")
    await wait.reply_text("\n".join(lines)[:4096], parse_mode=ParseMode.HTML)


async def cmd_batch_exec(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/batch all 命令 — 全部节点执行"""
    if not await guard(update):
        return
    args = context.args
    if not args:
        await update.message.reply_text("用法：<code>/batch all 命令</code>\n例：<code>/batch all uptime</code>", parse_mode=ParseMode.HTML)
        return
    scope = args[0].lower()
    if scope != "all" and scope not in [g for g, _ in list_groups()]:
        await update.message.reply_text(f"❌ 范围 <code>{esc(scope)}</code> 不存在。用 <code>all</code> 或分组名。", parse_mode=ParseMode.HTML)
        return
    cmd = " ".join(args[1:])
    if not cmd:
        await update.message.reply_text("用法：<code>/batch all 命令</code>", parse_mode=ParseMode.HTML)
        return
    if not is_safe_readonly_command(cmd):
        await update.message.reply_text("⚠️ 只允许只读命令。", parse_mode=ParseMode.HTML)
        return
    nodes = all_nodes() if scope == "all" else nodes_by_group(scope)
    if not nodes:
        await update.message.reply_text("没有节点。", parse_mode=ParseMode.HTML)
        return
    wait = await update.message.reply_text(f"⏳ 对 <code>{esc(scope)}</code>（{len(nodes)}台）执行中…", parse_mode=ParseMode.HTML)
    results = await asyncio.gather(*[run_ssh(n, cmd, timeout=15) for n in nodes], return_exceptions=True)
    lines = [f"📊 <b>{esc(scope)}</b> | $ <code>{esc(cmd)}</code>"]
    for node, result in zip(nodes, results):
        if isinstance(result, Exception):
            lines.append(f"\n🔴 <b>{esc(node.name)}</b>：<code>{esc(str(result)[:120])}</code>")
        else:
            lines.append(f"\n🟢 <b>{esc(node.name)}</b>：<code>{esc(result[:500])}</code>")
    await wait.reply_text("\n".join(lines)[:4096], parse_mode=ParseMode.HTML)


def _parse_host_port_user(text: str):
    """解析 root@1.2.3.4:22 格式"""
    user, port = "root", 22
    if "@" in text:
        user, text = text.split("@", 1)
    if ":" in text:
        parts = text.rsplit(":", 1)
        if parts[1].isdigit():
            text, port = parts[0], int(parts[1])
    return user, text.strip(), port


async def cmd_add(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/add [IP] — 快速添加或进入多步流程"""
    if not await guard(update):
        return
    # 快速添加：/add 1.2.3.4 或 /add root@1.2.3.4:22
    if context.args:
        raw = " ".join(context.args)
        user, host, port = _parse_host_port_user(raw)
        d = defaults_dict()
        # 自动命名：用 IP 最后一段或主机名
        name = host.split(".")[-1] if host.count(".") >= 3 else host
        # 存入 context，跳到认证选择
        context.user_data["add_node"] = {"name": name, "group_name": d.get("group", "default"), "host": host, "port": port, "user": user}
        context.user_data["add_step"] = "_quick_auth"
        await prompt_auth_choice(update.message, context.user_data["add_node"])
        return
    # 原有多步流程
    context.user_data["add_node"] = {}
    context.user_data["add_step"] = ADD_FLOW[0]
    await update.message.reply_text("开始添加节点\n\n" + add_prompts()[ADD_FLOW[0]], parse_mode=ParseMode.HTML)


async def cmd_overview(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    nodes = all_nodes()
    if not nodes:
        await update.message.reply_text("还没有节点，先 /add。")
        return
    wait = await update.message.reply_text("正在拉取全部节点概览…")
    results = await asyncio.gather(*[collect_node_summary(n) for n in nodes], return_exceptions=True)
    now = int(time.time())
    lines = ["<b>批量总览</b>"]
    ok = 0
    for node, result in zip(nodes, results):
        exp_icon = ""
        if int(node.expires_at or 0) > 0:
            days_left = math.floor((int(node.expires_at) - now) / 86400)
            if days_left < 0:
                exp_icon = " ⛔"
            elif days_left <= 3:
                exp_icon = " 🔴"
            elif days_left <= 7:
                exp_icon = " 🟡"
        price_tag = f" | 💰{esc(format_price(node))}" if float(node.monthly_price or 0) > 0 else ""
        if isinstance(result, Exception):
            lines.append(f"\n🔴 <b>{esc(node.name)}</b> [{esc(node.group_name)}]{exp_icon}{price_tag}\n<code>{esc(str(result))[:120]}</code>")
        else:
            ok += 1
            lines.append(f"\n🟢 <b>{esc(node.name)}</b> [{esc(node.group_name)}]{exp_icon}{price_tag}\n负载 <code>{esc(result.get('LOAD','-'))}</code> | 内存 <code>{esc(result.get('MEM','-'))}</code> | 磁盘 <code>{esc(result.get('DISK','-'))}</code>")
    lines.insert(1, f"在线：<code>{ok}/{len(nodes)}</code>\n")
    await wait.reply_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML)


async def cmd_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    d = defaults_dict()
    text = (
        "<b>默认认证设置</b>\n\n"
        f"默认用户：<code>{esc(d['user'])}</code>\n"
        f"默认端口：<code>{d['port']}</code>\n"
        f"默认密码：<code>{'已设置' if d['password'] else '未设置'}</code>\n"
        f"默认Key：<code>{esc(d['key_path'])}</code>"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("改默认密码", callback_data="settings:set_default_password"), InlineKeyboardButton("改默认Key", callback_data="settings:set_default_key")],
        [InlineKeyboardButton("改默认用户", callback_data="settings:set_default_user"), InlineKeyboardButton("改默认端口", callback_data="settings:set_default_port")],
    ]))


async def cmd_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    await update.message.reply_text("<b>告警设置</b>\n\n" + alert_settings_text(), parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("改巡检间隔", callback_data="settings:set_check_interval"), InlineKeyboardButton("改失败阈值", callback_data="settings:set_fail_threshold")],
        [InlineKeyboardButton("CPU 阈值", callback_data="settings:set_cpu_alert_threshold"), InlineKeyboardButton("内存阈值", callback_data="settings:set_mem_alert_threshold")],
        [InlineKeyboardButton("磁盘阈值", callback_data="settings:set_disk_alert_threshold"), InlineKeyboardButton("到期提醒天数", callback_data="settings:set_expire_remind_days")],
        [InlineKeyboardButton("切换掉线通知", callback_data="settings:toggle_notify_offline"), InlineKeyboardButton("切换恢复通知", callback_data="settings:toggle_notify_recovery")],
        [InlineKeyboardButton("立即巡检一次", callback_data="monitor:run")],
    ]))


async def cmd_billing(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update):
        return
    nodes = all_nodes()
    priced = [n for n in nodes if float(n.monthly_price or 0) > 0]
    group_rows = monthly_cost_by_group()
    upcoming = expiring_nodes(30)
    top_nodes = [n for n in top_cost_nodes(8) if float(n.monthly_price or 0) > 0]
    lines = ["<b>账单总览</b>"]
    lines.append(f"节点：<code>{len(nodes)}</code> | 已填账单：<code>{len(priced)}</code>")
    lines.append("总成本：<code>多币种，按节点明细查看</code>")
    if group_rows:
        lines.append("\n<b>分组覆盖</b>")
        for g, c, s in group_rows[:8]:
            lines.append(f"• <b>{esc(g)}</b>：<code>{c} 台</code>")
    if top_nodes:
        lines.append("\n<b>账单节点</b>")
        for n in top_nodes[:5]:
            lines.append(f"• <b>{esc(n.name)}</b> [{esc(n.group_name)}]：<code>{esc(format_price(n))}</code>")
    if upcoming:
        lines.append("\n<b>30 天内到期</b>")
        for n in upcoming[:5]:
            lines.append(f"• <b>{esc(n.name)}</b>：<code>{esc(format_expiry(n.expires_at))}</code>（{esc(days_left_text(n.expires_at))}）")
    else:
        lines.append("\n30 天内没有到期节点。")
    await update.message.reply_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("⏰ 即将到期", callback_data="billing:expiring"), InlineKeyboardButton("🔄 刷新", callback_data="billing:summary")],
    ]))


async def setup_bot(app: Application):
    await app.bot.set_my_commands([
        BotCommand("start", "打开主菜单"),
        BotCommand("nodes", "节点列表"),
        BotCommand("add", "添加节点（/add IP 快速添加）"),
        BotCommand("overview", "批量总览"),
        BotCommand("s", "快捷执行：/s 节点名 命令"),
        BotCommand("g", "分组执行：/g 分组 命令"),
        BotCommand("batch", "批量执行：/batch all 命令"),
        BotCommand("auth", "默认认证设置"),
        BotCommand("alerts", "告警设置"),
        BotCommand("billing", "账单总览"),
        BotCommand("cancel", "取消当前操作"),
    ])
    try:
        await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
    except Exception:
        pass
    app.bot_data["monitor_task"] = asyncio.create_task(monitor_loop(app))


if __name__ == "__main__":
    init_db()
    app = Application.builder().token(TOKEN).post_init(setup_bot).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("cancel", cancel))
    app.add_handler(CommandHandler("nodes", cmd_nodes))
    app.add_handler(CommandHandler("add", cmd_add))
    app.add_handler(CommandHandler("overview", cmd_overview))
    app.add_handler(CommandHandler("auth", cmd_auth))
    app.add_handler(CommandHandler("alerts", cmd_alerts))
    app.add_handler(CommandHandler("billing", cmd_billing))
    app.add_handler(CommandHandler("s", cmd_quick_exec))
    app.add_handler(CommandHandler("g", cmd_group_exec))
    app.add_handler(CommandHandler("batch", cmd_batch_exec))
    app.add_handler(CallbackQueryHandler(on_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    print("bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)
