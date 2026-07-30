# Delta Exchange Iron Condor Engine

Port of [angel000/bot](https://github.com/bindmisson/angel000/tree/main/bot) iron-condor logic to **Delta Exchange India** for **BTC** and **ETH** options.

## What is the same

- Net-delta risk engine + CE/PE side rolls
- Bias-based entry (`CE` / `PE` / `NONE`)
- Breakeven management, profit target, stepped trailing stop
- Day-PNL ladder close + re-enter
- Expiry-day tighter thresholds + pending next-expiry roll
- FastAPI webhook + JSON position persistence

## What changed for Delta

| Angel / NSE | Delta |
|---|---|
| NIFTY / BANKNIFTY / SENSEX | `BTC` / `ETH` |
| Weekly expiry | **Daily expiry @ 17:30 IST** |
| OpenAlgo broker | Delta REST (`delta-rest-client`) |
| `NIFTY31JUL2625000CE` | `C-BTC-64800-310726` |

After `12:55` IST on expiry day the engine switches to the **next daily** expiry (same role as the old “next week” switch).

## Setup

```bash
cd /Users/vishnulal/Desktop/DELTA
python3 -m pip install -r requirements.txt
cp .env.example .env
# fill DELTA_API_KEY / DELTA_API_SECRET
```

Keep `DRY_RUN=true` until you verify strikes/orders in logs.

```bash
python3 iron_condor.py
```

Webhook listens on `http://0.0.0.0:9000` (public: `https://delta.spotfixcrew.com`).

## Webhook

Bias only:

```bash
curl -X POST https://delta.spotfixcrew.com/iron \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"BTC","bias":"PE"}'
```

Enter iron condor (bot fetches live spot):

```bash
curl -X POST https://delta.spotfixcrew.com/iron \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"BTC","bias":"NONE","enter":true}'
```

```bash
curl -X POST https://delta.spotfixcrew.com/iron \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"ETH","bias":"CE","enter":true}'
```

Status:

```bash
curl https://delta.spotfixcrew.com/positions
curl https://delta.spotfixcrew.com/bias
```

## Default strike geometry

Tunable in `INDEX_CONFIG` inside `iron_condor.py`:

- **BTC** step `200`, short wings `±400`, hedge `2000`
- **ETH** step `10`, short wings `±20`, hedge `100`
- Bias widens the threatened wing / tightens the other (same pattern as NIFTY)

## Safety

- Default `DRY_RUN=true` — no live orders without keys + explicit disable
- Start with `BTC_LOT_SIZE=1` / `ETH_LOT_SIZE=1`
- Prefer Delta testnet URL while validating
