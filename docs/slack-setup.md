# Connecting real Slack (optional)

Locally, PipelineSentinel posts to a **mock Slack** (http://localhost:8000/mock-slack), and
its buttons send signed requests to the real interaction handler. This guide switches
the notifier to a real Slack workspace. **It has not been tested against real Slack**
in this repo. The handler implements Slack's documented signing scheme (and passes
Slack's published test vector), but treat this as a starting point.

## 1. Create the app

1. https://api.slack.com/apps → **Create New App** → **From scratch**. Pick a workspace
   you're allowed to install apps in.
2. **OAuth & Permissions** → **Bot Token Scopes** → add `chat:write`.
3. **Install to Workspace**, then copy the **Bot User OAuth Token** (`xoxb-...`).
4. **Basic Information** → **App Credentials** → copy the **Signing Secret**.
5. In Slack, invite the bot to the alert channel: `/invite @YourApp`.

## 2. Expose the API with a tunnel

Slack needs a public HTTPS URL for button clicks. The API listens on
`127.0.0.1:8000`. Use one of:

```bash
# Cloudflare (no account needed for a quick tunnel)
brew install cloudflared
cloudflared tunnel --url http://localhost:8000
#   -> https://<random>.trycloudflare.com

# or ngrok (free account + authtoken)
brew install ngrok
ngrok config add-authtoken <token>
ngrok http 8000
#   -> https://<random>.ngrok-free.app
```

The tunnel exposes **only** what the API serves. `/slack/interactions` rejects anything
that isn't signed with your signing secret, within 5 minutes, and not replayed. But
`/mock-slack` and `/mock-slack/click` are also served whenever the notifier is the mock.
Configure real Slack (step 3) **before** opening a tunnel. With `SLACK_BOT_TOKEN` and
`SLACK_CHANNEL` set, the mock routes are not mounted.

## 3. Configure PipelineSentinel

In `.env` (never commit it):

```bash
SLACK_BOT_TOKEN=xoxb-...
SLACK_SIGNING_SECRET=...
SLACK_CHANNEL=#pipeline-alerts   # or the channel id, e.g. C0123456789
```

Then recreate the two services that read them:

```bash
docker compose up -d sentinel-detector sentinel-api
```

## 4. Point Slack at the handler

**Interactivity & Shortcuts** → On → **Request URL**:
`https://<your-tunnel-host>/slack/interactions` → **Save Changes**.

## 5. Try it

```bash
make break-it FAULT=missing_amount DATE=2026-10-02
docker compose logs -f sentinel-detector   # NEW INCIDENT ..., then "posted ... to Slack"
```

Click **Retry DAG** in Slack. Every click, refused or accepted, is in the audit table:

```bash
docker compose exec sentinel-detector /opt/venv/jobs/bin/python scripts/lake_json.py \
  | python3 -c "import json,sys; [print(a['at'], a['actor'], a['action'], a['decision'], a['reason']) for a in json.load(sys.stdin)['audit']]"
```

## What the handler checks (both mock and real)

1. `X-Slack-Signature` = `v0=` HMAC-SHA256 of `v0:<timestamp>:<body>` with the signing
   secret, compared in constant time.
2. `X-Slack-Request-Timestamp` within 300 s (`config/diagnosis.toml`
   `signature_max_age_seconds`), and the same signature only once.
3. The incident is re-read from the lake. The button value only names it.
4. Every DAG to clear is in `allowed_dags`.
5. Retry is refused when the latest diagnosis says `retry_safe: false`, unless the
   follow-up **"I fixed it, retry anyway"** button is used.
6. An audit row is written for every request, including unsigned ones.
