import os
import sqlite3
import time
from contextlib import contextmanager
from typing import Dict, List, Optional

import requests
from flask import Flask, jsonify, request

GROUPME_POST_URL = "https://api.groupme.com/v3/bots/post"
GROUPME_API_BASE = "https://api.groupme.com/v3"

BOT_ID = os.getenv("GROUPME_BOT_ID", "").strip()
BOT_ID_MAP_RAW = os.getenv("GROUPME_BOT_ID_MAP", "").strip()
ACCESS_TOKEN = os.getenv("GROUPME_ACCESS_TOKEN", "").strip()
COMMAND = "!wordcount"
DB_PATH = os.getenv("BOT_DB_PATH", "wordcount.db").strip()
MAX_REPLY_LEN = int(os.getenv("MAX_REPLY_LEN", "900"))
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_SECONDS", "300"))
MAX_SYNC_PAGES = int(os.getenv("MAX_SYNC_PAGES", "25"))
MEMBER_CACHE_SECONDS = int(os.getenv("MEMBER_CACHE_SECONDS", "600"))
BOTS_CACHE_SECONDS = int(os.getenv("BOTS_CACHE_SECONDS", "300"))
FULL_SYNC_ON_QUERY = os.getenv("FULL_SYNC_ON_QUERY", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

app = Flask(__name__)
_last_sync_by_group: Dict[str, int] = {}
_member_cache_by_group: Dict[str, Dict] = {}
_bot_lookup_cache: Dict[str, Dict] = {}


def parse_bot_id_map(raw: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not raw:
        return out
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    for part in parts:
        if ":" not in part:
            continue
        group_id, bot_id = part.split(":", 1)
        group_id = group_id.strip()
        bot_id = bot_id.strip()
        if group_id and bot_id:
            out[group_id] = bot_id
    return out


BOT_ID_MAP = parse_bot_id_map(BOT_ID_MAP_RAW)


def normalize_callback_url(url: str) -> str:
    return url.rstrip("/")


def fetch_bots() -> List[Dict]:
    if not ACCESS_TOKEN:
        return []

    now_ts = int(time.time())
    cache_entry = _bot_lookup_cache.get("all_bots")
    if cache_entry and now_ts - safe_int(cache_entry.get("fetched_at")) < BOTS_CACHE_SECONDS:
        return cache_entry.get("bots") or []

    try:
        response = requests.get(
            f"{GROUPME_API_BASE}/bots",
            params={"token": ACCESS_TOKEN},
            timeout=15,
        )
    except requests.RequestException as exc:
        app.logger.warning("GroupMe bots request failed: %s", exc)
        return []

    if response.status_code >= 300:
        app.logger.warning(
            "GroupMe bots request failed: status=%s body=%s",
            response.status_code,
            response.text,
        )
        return []

    body = response.json() if response.content else {}
    bots = ((body or {}).get("response") or [])
    if not isinstance(bots, list):
        bots = []
    _bot_lookup_cache["all_bots"] = {"fetched_at": now_ts, "bots": bots}
    return bots


@contextmanager
def db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with db_connection() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                group_id TEXT NOT NULL,
                sender_id TEXT,
                sender_name TEXT,
                sender_type TEXT,
                text TEXT,
                created_at INTEGER,
                like_count INTEGER NOT NULL DEFAULT 0,
                stored_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_messages_group_created
                ON messages(group_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_messages_group_sender_created
                ON messages(group_id, sender_id, created_at);
            """
        )
        conn.commit()


def safe_int(value, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_text(value: Optional[str]) -> str:
    return (value or "").strip()


def like_count_from_payload(payload: Dict) -> int:
    likes = payload.get("favorited_by")
    if isinstance(likes, list):
        return len(likes)
    return 0


def extract_callback_message(payload: Dict) -> Dict:
    return {
        "id": str(payload.get("id", "")).strip(),
        "group_id": str(payload.get("group_id", "")).strip(),
        "sender_id": str(payload.get("sender_id") or payload.get("user_id") or "").strip(),
        "sender_name": str(payload.get("name", "")).strip(),
        "sender_type": str(payload.get("sender_type", "user")).strip() or "user",
        "text": normalize_text(payload.get("text")),
        "created_at": safe_int(payload.get("created_at")),
        "like_count": like_count_from_payload(payload),
    }


def extract_api_message(payload: Dict) -> Dict:
    sender_type = "system" if payload.get("system") else "user"
    return {
        "id": str(payload.get("id", "")).strip(),
        "group_id": str(payload.get("group_id", "")).strip(),
        "sender_id": str(payload.get("user_id", "")).strip(),
        "sender_name": str(payload.get("name", "")).strip(),
        "sender_type": sender_type,
        "text": normalize_text(payload.get("text")),
        "created_at": safe_int(payload.get("created_at")),
        "like_count": like_count_from_payload(payload),
    }


def upsert_messages(messages: List[Dict]) -> None:
    if not messages:
        return

    now_ts = int(time.time())
    with db_connection() as conn:
        conn.executemany(
            """
            INSERT INTO messages (
                id, group_id, sender_id, sender_name, sender_type,
                text, created_at, like_count, stored_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                sender_id=excluded.sender_id,
                sender_name=excluded.sender_name,
                sender_type=excluded.sender_type,
                text=excluded.text,
                created_at=excluded.created_at,
                like_count=excluded.like_count,
                stored_at=excluded.stored_at
            """,
            [
                (
                    m["id"],
                    m["group_id"],
                    m["sender_id"],
                    m["sender_name"],
                    m["sender_type"],
                    m["text"],
                    m["created_at"],
                    m["like_count"],
                    now_ts,
                )
                for m in messages
                if m.get("id") and m.get("group_id")
            ],
        )
        conn.commit()


def discover_bot_id_for_group(group_id: str, callback_url: str = "") -> str:
    bots = fetch_bots()
    if not bots:
        return ""

    group_matches = [bot for bot in bots if str(bot.get("group_id", "")).strip() == group_id]
    if not group_matches:
        return ""

    active_matches = [bot for bot in group_matches if bool(bot.get("active", True))]
    candidates = active_matches if active_matches else group_matches
    if not candidates:
        return ""

    wanted_callback = normalize_callback_url(callback_url)
    if wanted_callback:
        callback_matches = [
            bot
            for bot in candidates
            if normalize_callback_url(str(bot.get("callback_url", "")).strip()) == wanted_callback
        ]
        if callback_matches:
            return str(callback_matches[0].get("bot_id", "")).strip()

    return str(candidates[0].get("bot_id", "")).strip()


def get_bot_id_for_group(group_id: str, callback_url: str = "") -> str:
    explicit = BOT_ID_MAP.get(group_id, BOT_ID)
    if explicit:
        return explicit
    return discover_bot_id_for_group(group_id, callback_url)


def post_to_groupme(text: str, group_id: str, callback_url: str = "") -> None:
    bot_id = get_bot_id_for_group(group_id, callback_url)
    if not bot_id:
        app.logger.warning("No bot_id configured for group_id=%s; cannot post message", group_id)
        return

    payload = {"bot_id": bot_id, "text": text[:MAX_REPLY_LEN]}
    try:
        response = requests.post(GROUPME_POST_URL, json=payload, timeout=10)
        if response.status_code >= 300:
            app.logger.warning(
                "GroupMe post failed: status=%s body=%s",
                response.status_code,
                response.text,
            )
    except requests.RequestException as exc:
        app.logger.warning("GroupMe post exception: %s", exc)


def split_for_groupme(text: str, limit: int) -> List[str]:
    if len(text) <= limit:
        return [text]

    chunks: List[str] = []
    current = ""
    for line in text.splitlines():
        candidate = line if not current else f"{current}\n{line}"
        if len(candidate) <= limit:
            current = candidate
            continue

        if current:
            chunks.append(current)
            current = ""

        while len(line) > limit:
            chunks.append(line[:limit])
            line = line[limit:]
        current = line

    if current:
        chunks.append(current)
    return chunks or [text[:limit]]


def post_reply(text: str, group_id: str, callback_url: str = "") -> None:
    for chunk in split_for_groupme(text, MAX_REPLY_LEN):
        post_to_groupme(chunk, group_id, callback_url)


def maybe_sync_group_messages(group_id: str) -> None:
    if not ACCESS_TOKEN:
        return

    now_ts = int(time.time())
    last_sync = _last_sync_by_group.get(group_id, 0)
    if not FULL_SYNC_ON_QUERY and now_ts - last_sync < SYNC_INTERVAL_SECONDS:
        return

    before_id = None
    pages = 0

    while pages < MAX_SYNC_PAGES:
        params = {"token": ACCESS_TOKEN, "limit": 100}
        if before_id:
            params["before_id"] = before_id

        try:
            response = requests.get(
                f"{GROUPME_API_BASE}/groups/{group_id}/messages",
                params=params,
                timeout=15,
            )
        except requests.RequestException as exc:
            app.logger.warning("GroupMe sync request failed: %s", exc)
            break

        if response.status_code == 304:
            break

        if response.status_code >= 300:
            app.logger.warning(
                "GroupMe sync failed: status=%s body=%s",
                response.status_code,
                response.text,
            )
            break

        body = response.json() if response.content else {}
        api_messages = (((body or {}).get("response") or {}).get("messages") or [])
        if not api_messages:
            break

        converted = [extract_api_message(m) for m in api_messages]
        upsert_messages(converted)

        before_id = str(api_messages[-1].get("id", "")).strip() or None
        pages += 1
        if not before_id:
            break

    _last_sync_by_group[group_id] = now_ts


def get_group_members_map(group_id: str) -> Dict[str, Dict]:
    if not ACCESS_TOKEN:
        return {}

    now_ts = int(time.time())
    cache_entry = _member_cache_by_group.get(group_id)
    if cache_entry and now_ts - safe_int(cache_entry.get("fetched_at")) < MEMBER_CACHE_SECONDS:
        return cache_entry.get("members") or {}

    try:
        response = requests.get(
            f"{GROUPME_API_BASE}/groups/{group_id}",
            params={"token": ACCESS_TOKEN},
            timeout=15,
        )
    except requests.RequestException as exc:
        app.logger.warning("GroupMe group members request failed: %s", exc)
        return {}

    if response.status_code >= 300:
        app.logger.warning(
            "GroupMe group members request failed: status=%s body=%s",
            response.status_code,
            response.text,
        )
        return {}

    body = response.json() if response.content else {}
    members = (((body or {}).get("response") or {}).get("members") or [])
    out: Dict[str, Dict] = {}
    for member in members:
        member_user_id = str(member.get("user_id", "")).strip()
        if not member_user_id:
            continue
        out[member_user_id] = {
            "nickname": str(member.get("nickname", "")).strip(),
            "real_name": str(member.get("name", "")).strip(),
        }

    _member_cache_by_group[group_id] = {"fetched_at": now_ts, "members": out}
    return out


def format_member_display_name(nickname: str, real_name: str, fallback_name: str) -> str:
    nick = nickname.strip()
    real = real_name.strip()
    fallback = fallback_name.strip()

    if nick and real and nick.lower() != real.lower():
        return f"{nick} ({real})"
    if real:
        return real
    if nick:
        return nick
    return fallback or "Unknown"


def normalize_name_query(value: str) -> str:
    q = value.strip()
    if q.startswith("@"):
        q = q[1:].strip()
    if len(q) >= 2 and q[0] == "#" and q[-1] == "#":
        q = q[1:-1].strip()
    return q.lower()


def aggregate_user_rows(group_id: str) -> List[sqlite3.Row]:
    with db_connection() as conn:
        return conn.execute(
            """
            SELECT
                sender_id,
                MAX(sender_name) AS sender_name,
                COUNT(*) AS message_count,
                MIN(created_at) AS first_seen
            FROM messages
            WHERE group_id = ?
              AND sender_type = 'user'
              AND sender_id IS NOT NULL
              AND TRIM(sender_id) <> ''
            GROUP BY sender_id
            HAVING COUNT(*) > 0
            """,
            (group_id,),
        ).fetchall()


def build_user_rates(group_id: str) -> List[Dict]:
    maybe_sync_group_messages(group_id)
    member_map = get_group_members_map(group_id)
    user_rows = aggregate_user_rows(group_id)
    now_ts = int(time.time())

    message_rows_by_sender: Dict[str, sqlite3.Row] = {}
    for row in user_rows:
        sender_id = str(row["sender_id"]).strip()
        if sender_id:
            message_rows_by_sender[sender_id] = row

    all_sender_ids = set(message_rows_by_sender.keys())
    all_sender_ids.update(member_map.keys())

    entries: List[Dict] = []
    for sender_id in all_sender_ids:
        row = message_rows_by_sender.get(sender_id)
        fallback_name = (row["sender_name"] if row else "").strip() if row else ""
        member = member_map.get(sender_id, {})
        nickname = str(member.get("nickname", "")).strip()
        real_name = str(member.get("real_name", "")).strip()
        display_name = format_member_display_name(nickname, real_name, fallback_name)

        msgs = safe_int(row["message_count"]) if row else 0
        first_seen = safe_int(row["first_seen"], now_ts) if row else now_ts
        elapsed_days = max(0.0, (now_ts - first_seen) / 86400)
        days_display = int(elapsed_days)
        days_for_rate = max(elapsed_days, 1 / 86400) if msgs > 0 else 1.0
        rate = msgs / days_for_rate if msgs > 0 else 0.0

        entries.append(
            {
                "sender_id": sender_id,
                "messages": msgs,
                "days": days_display,
                "rate": rate,
                "display_name": display_name,
                "nickname": nickname,
                "real_name": real_name,
                "fallback_name": fallback_name,
            }
        )
    return entries


def get_member_only_entry_for_query(group_id: str, name_query: str) -> Optional[Dict]:
    member_map = get_group_members_map(group_id)
    if not member_map:
        return None

    needle = normalize_name_query(name_query)
    if not needle:
        return None

    for sender_id, member in member_map.items():
        nickname = str(member.get("nickname", "")).strip()
        real_name = str(member.get("real_name", "")).strip()
        display = format_member_display_name(nickname, real_name, "")
        alias_values = [
            normalize_name_query(display),
            normalize_name_query(nickname),
            normalize_name_query(real_name),
        ]
        alias_values = [a for a in alias_values if a]
        if needle in alias_values or any(needle in a for a in alias_values):
            return {
                "sender_id": sender_id,
                "messages": 0,
                "days": 0,
                "rate": 0.0,
                "display_name": display or "Unknown",
                "nickname": nickname,
                "real_name": real_name,
                "fallback_name": display or "Unknown",
            }
    return None


def find_user_entry_by_name(entries: List[Dict], name_query: str) -> Optional[Dict]:
    needle = normalize_name_query(name_query)
    if not needle:
        return None

    def aliases(entry: Dict) -> List[str]:
        return [
            normalize_name_query(entry.get("display_name", "")),
            normalize_name_query(entry.get("nickname", "")),
            normalize_name_query(entry.get("real_name", "")),
            normalize_name_query(entry.get("fallback_name", "")),
        ]

    exact = [entry for entry in entries if needle in set(a for a in aliases(entry) if a)]
    if exact:
        return sorted(exact, key=lambda e: e["messages"], reverse=True)[0]

    partial = []
    for entry in entries:
        alias_values = [a for a in aliases(entry) if a]
        if any(needle in a for a in alias_values):
            partial.append(entry)
    if partial:
        return sorted(partial, key=lambda e: e["messages"], reverse=True)[0]
    return None


def format_ratio(ratio: float) -> str:
    if ratio == 0:
        return "0"
    out = f"{ratio:.5f}".rstrip("0").rstrip(".")
    return out


def leaderboard_text(group_id: str) -> str:
    maybe_sync_group_messages(group_id)
    member_map = get_group_members_map(group_id)
    if not member_map:
        return "Could not fetch current member list right now."
    current_member_ids = set(member_map.keys())

    with db_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                sender_id,
                MAX(sender_name) AS sender_name,
                COUNT(*) AS message_count,
                COALESCE(SUM(like_count), 0) AS like_total
            FROM messages
            WHERE group_id = ?
              AND sender_type = 'user'
              AND sender_id IS NOT NULL
              AND TRIM(sender_id) <> ''
            GROUP BY sender_id
            HAVING COUNT(*) > 0
            """,
            (group_id,),
        ).fetchall()
    rows = [row for row in rows if str(row["sender_id"] or "").strip() in current_member_ids]

    if not rows:
        return "Not enough data yet."

    ranked = []
    for row in rows:
        msgs = safe_int(row["message_count"])
        likes = safe_int(row["like_total"])
        ratio = likes / msgs if msgs else 0.0
        sender_id = str(row["sender_id"] or "").strip()
        member = member_map.get(sender_id, {})
        name = format_member_display_name(
            str(member.get("nickname", "")),
            str(member.get("real_name", "")),
            str(row["sender_name"] or "Unknown"),
        )
        ranked.append(
            {
                "name": name,
                "ratio": ratio,
                "likes": likes,
                "messages": msgs,
            }
        )

    ranked.sort(key=lambda r: (r["ratio"], r["messages"], r["name"].lower()), reverse=True)

    top_slice = ranked[:10]
    bottom_pool = [row for row in ranked if row["messages"] > 10]
    bottom_total = len(bottom_pool)
    bottom_start = max(bottom_total - 10, 0)
    bottom_slice = bottom_pool[bottom_start:]

    lines = ["top 10"]
    for i, row in enumerate(top_slice, start=1):
        lines.append(
            f"{i}. {row['name']} - {format_ratio(row['ratio'])} likes per message "
            f"with {row['likes']} likes on {row['messages']} messages"
        )

    lines.append("")
    lines.append("")
    lines.append("bottom 10")
    if not bottom_slice:
        lines.append("No users with more than 10 messages yet.")
    else:
        for idx, row in enumerate(bottom_slice, start=bottom_start + 1):
            lines.append(
                f"{idx}. {row['name']} - {format_ratio(row['ratio'])} likes per message "
                f"with {row['likes']} likes on {row['messages']} messages"
            )

    return "\n".join(lines)


def top_liked_messages_text(group_id: str) -> str:
    maybe_sync_group_messages(group_id)
    with db_connection() as conn:
        rows = conn.execute(
            """
            SELECT text, sender_name, like_count
            FROM messages
            WHERE group_id = ?
              AND sender_type = 'user'
              AND text IS NOT NULL
              AND TRIM(text) <> ''
            ORDER BY like_count DESC, created_at DESC
            LIMIT 5
            """,
            (group_id,),
        ).fetchall()

    if not rows:
        return "Not enough data yet."

    lines = ["top 5:"]
    for row in rows:
        text = (row["text"] or "").strip().replace("\n", " ")
        if len(text) > 80:
            text = text[:77] + "..."
        sender_name = (row["sender_name"] or "Unknown").strip() or "Unknown"
        likes = safe_int(row["like_count"])
        lines.append(f"\"{text}\" - {sender_name} - {likes} likes")
    return "\n".join(lines)


def find_previous_non_command_sender(group_id: str, current_message_id: str) -> Optional[sqlite3.Row]:
    with db_connection() as conn:
        row = conn.execute(
            """
            SELECT sender_id, sender_name
            FROM messages
            WHERE group_id = ?
              AND sender_type = 'user'
              AND id != ?
              AND text IS NOT NULL
              AND TRIM(text) <> ''
              AND LOWER(TRIM(text)) NOT LIKE '!wordcount%'
            ORDER BY created_at DESC, rowid DESC
            LIMIT 1
            """,
            (group_id, current_message_id),
        ).fetchone()
    return row


def ordinal_suffix(value: int) -> str:
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def percentile_text_for_entry(entries: List[Dict], target: Dict) -> str:
    below_or_equal = sum(1 for entry in entries if entry["rate"] <= target["rate"])
    percentile = max(1, round((below_or_equal / len(entries)) * 100))
    per_day = format_ratio(target["rate"])
    return (
        f"{target['display_name']} has sent {target['messages']} messages in {target['days']} days "
        f"({per_day}/day) "
        f"since his first message ({ordinal_suffix(percentile)} percentile)."
    )


def percentile_text_for_target(group_id: str, target_sender_id: str, target_sender_name: str = "") -> str:
    entries = build_user_rates(group_id)
    if not entries:
        return "Not enough data yet."

    target = next((entry for entry in entries if entry["sender_id"] == target_sender_id), None)
    if target:
        return percentile_text_for_entry(entries, target)

    fallback_target = {
        "sender_id": target_sender_id,
        "messages": 0,
        "days": 1,
        "rate": 0.0,
        "display_name": target_sender_name or "Unknown",
        "nickname": "",
        "real_name": "",
        "fallback_name": target_sender_name or "Unknown",
    }
    entries.append(fallback_target)
    return percentile_text_for_entry(entries, fallback_target)


def percentile_text_for_name(group_id: str, name_query: str) -> str:
    entries = build_user_rates(group_id)
    if not entries:
        return "Not enough data yet."

    target = find_user_entry_by_name(entries, name_query)
    if not target:
        member_only_target = get_member_only_entry_for_query(group_id, name_query)
        if not member_only_target:
            return f"Could not find member '{name_query}'."
        entries.append(member_only_target)
        target = member_only_target
    return percentile_text_for_entry(entries, target)


def yap_leaderboard_text(group_id: str) -> str:
    entries = build_user_rates(group_id)
    ranked = [entry for entry in entries if entry["messages"] > 0]
    if not ranked:
        return "Not enough data yet."

    ranked.sort(
        key=lambda e: (e["rate"], e["messages"], e["display_name"].lower()),
        reverse=True,
    )
    top = ranked[:10]

    lines = ["top 10 yap:"]
    for idx, entry in enumerate(top, start=1):
        lines.append(
            f"{idx}. {entry['display_name']} - {format_ratio(entry['rate'])}/day "
            f"({entry['messages']} messages in {entry['days']} days)"
        )
    return "\n".join(lines)


def run_command(payload: Dict) -> Optional[str]:
    text = normalize_text(payload.get("text"))
    if not text.lower().startswith(COMMAND):
        return None

    group_id = str(payload.get("group_id", "")).strip()
    message_id = str(payload.get("id", "")).strip()
    if not group_id:
        return None

    tail = text[len(COMMAND) :].strip()
    if not tail:
        prev = find_previous_non_command_sender(group_id, message_id)
        if not prev:
            return "No previous non-command message found yet."
        return percentile_text_for_target(
            group_id,
            str(prev["sender_id"]),
            str(prev["sender_name"] or "Unknown"),
        )

    if tail.lower() == "leaderboard":
        return leaderboard_text(group_id)
    if tail.lower() == "likes":
        return top_liked_messages_text(group_id)
    if tail.lower() == "yap":
        return yap_leaderboard_text(group_id)

    return percentile_text_for_name(group_id, tail)


@app.get("/")
def root():
    return jsonify(
        {
            "ok": True,
            "service": "groupme-wordcount-bot",
            "callback_path": "/groupme/callback",
            "command": COMMAND,
            "leaderboard_command": f"{COMMAND} leaderboard",
            "needs_access_token_for_likes": True,
        }
    )


@app.post("/groupme/callback")
def groupme_callback():
    payload = request.get_json(silent=True) or {}

    callback_message = extract_callback_message(payload)
    upsert_messages([callback_message])

    if callback_message["sender_type"] == "bot":
        return ("", 204)

    reply = run_command(payload)
    if reply:
        post_reply(reply, callback_message["group_id"], request.base_url)

    return ("", 204)


if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
else:
    init_db()
