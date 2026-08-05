# SpotFix Operator Engine (Delta Exchange)

BTC / ETH / XAUT options automation on **Delta Exchange India**, with a locked operator console.

## Setup

```bash
cd /Users/vishnulal/Desktop/DELTA
python3 -m pip install -r requirements.txt
cp .env.example .env
# fill DELTA_API_KEY / DELTA_API_SECRET / UI_PASSWORD / WEBHOOK_TOKEN
```

```bash
python3 iron_condor.py
```

Public URL: **https://delta.spotopscrew.com/**  
Engine binds `127.0.0.1` only; nginx terminates HTTPS.

## Operator console

Login required. UI shows ops status only (spot, size, enter/close) and safe settings (API, clocks, targets, lot size).  
Strategy geometry and risk-engine internals are **not** exposed in the UI or public APIs.

```bash
UI_USERNAME=admin
UI_PASSWORD=your-strong-password
WEBHOOK_TOKEN=your-webhook-token
```

## Webhook

Requires `X-Webhook-Token` (or a logged-in session):

```bash
curl -X POST https://delta.spotopscrew.com/iron \
  -H 'Content-Type: application/json' \
  -H 'X-Webhook-Token: YOUR_TOKEN' \
  -d '{"symbol":"BTC","bias":"NONE","enter":true}'
```

## Safety

- Default `DRY_RUN=true`
- Dashboard + APIs require login
- `/iron` requires token or session
- Public `/health` returns only `{"status":"ok"}`
