# GroupMe Wordcount Bot

This bot supports exactly two commands:

- `!wordcount leaderboard`
- `!wordcount`
- `!wordcount <name>`
- `!wordcount likes`

## Command behavior

### `!wordcount leaderboard`
Returns likes-per-message leaderboard using this exact structure:

```text
top 10
1. Name - 5.52 likes per message
...
bottom 10
94. Name - 0.00002 likes per message
```
Only current group members are included in this calculation.

### `!wordcount`
Looks at the most recent non-command message before `!wordcount`, then returns:

```text
Name has sent X messages in Y days since his first message (Zth percentile).
```

### `!wordcount <name>`
Looks up a specific member by nickname or real name, then returns the same message/rate output.

### `!wordcount likes`
Returns top 5 highest-like messages of all time in the chat, regardless of current/past membership:

```text
top 5:
"stuff stuff stuff" - Name - 42 likes
```

Comparison is based on each user's `messages / days since first seen message` rate, compared across all users in the group data the bot has.

## Important limitations
- GroupMe bot callbacks alone are not enough for accurate likes history over time.
- For accurate likes-per-message, set `GROUPME_ACCESS_TOKEN` so the bot can sync messages from the GroupMe Messages API.
- GroupMe does not expose a reliable "joined at" field in the bot callback payload, so `days` is measured from each user's first message in the bot's dataset.

## Environment variables
- `GROUPME_ACCESS_TOKEN` (required for zero-config bot-id discovery, and strongly recommended for leaderboard accuracy)
- `GROUPME_BOT_ID` (optional override for single-group usage)
- `GROUPME_BOT_ID_MAP` (optional override for multi-group usage), format:
  - `group_id_1:bot_id_1,group_id_2:bot_id_2`
- `BOT_DB_PATH` (optional, default `wordcount.db`)
- `MAX_REPLY_LEN` (optional, default `900`)
- `SYNC_INTERVAL_SECONDS` (optional, default `300`)
- `MAX_SYNC_PAGES` (optional, default `25`)
- `BOTS_CACHE_SECONDS` (optional, default `300`)

## Callback URL
Use this when creating/updating your bot in GroupMe:

`https://<your-pythonanywhere-username>.pythonanywhere.com/groupme/callback`

With `GROUPME_ACCESS_TOKEN` set, you can skip manual bot ID wiring:
- Create bot on GroupMe
- Set callback URL to your deployed `/groupme/callback`
- The service auto-detects the bot for that group via GroupMe API

## Local run
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export GROUPME_BOT_ID="YOUR_BOT_ID"
export GROUPME_ACCESS_TOKEN="YOUR_USER_ACCESS_TOKEN"
python wordcount_bot.py
```
