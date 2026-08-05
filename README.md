# Delta Exchange Iron Condor Engine

Port of [angel000/bot](https://github.com/bindmisson/angel000/tree/main/bot) iron-condor logic to **Delta Exchange India** for **BTC**, **ETH**, and **XAUT** options — each with its own settings.

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
| NIFTY / BANKNIFTY / SENSEX | `BTC` / `ETH` / `XAUT` |
| Weekly expiry | **Daily expiry** (BTC/ETH `17:30` IST, XAUT `21:30` IST) |
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

Webhook listens on `http://0.0.0.0:9000` (public: `https://delta.spotopscrew.com`).

## Settings UI

Open **https://delta.spotopscrew.com/** (or `http://HOST:9000/`) for a live settings panel covering:

- Global risk knobs (delta zones, BE buffers, TP %, day-PnL, TSL, clocks)
- Per-symbol geometry (BTC / ETH / XAUT lot size, strikes, hedges, bias distances)

Saves to `state/settings.json` and applies **live** to the running process (no restart for most knobs). API:

```bash
curl https://delta.spotopscrew.com/api/settings
curl -X PUT https://delta.spotopscrew.com/api/settings \
  -H 'Content-Type: application/json' \
  -d '{"config":{"DELTA_ADJUST":0.5,"EXPIRY_DELTA_ADJUST":0.5}}'
```

## Webhook

Bias only:

```bash
curl -X POST https://delta.spotopscrew.com/iron \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"BTC","bias":"PE"}'
```

Enter iron condor (bot fetches live spot):

```bash
curl -X POST https://delta.spotopscrew.com/iron \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"BTC","bias":"NONE","enter":true}'
```

```bash
curl -X POST https://delta.spotopscrew.com/iron \
  -H 'Content-Type: application/json' \
  -d '{"symbol":"ETH","bias":"CE","enter":true}'
```

Status:

```bash
curl https://delta.spotopscrew.com/positions
curl https://delta.spotopscrew.com/bias
```

## Per-symbol settings (`INDEX_CONFIG`)

Each underlying has independent geometry, clocks, lot size, day-PnL, TSL, and profit targets:

| | BTC | ETH | XAUT |
|---|---|---|---|
| Spot ticker | BTCUSD | ETHUSD | XAUTUSD |
| Settlement IST | 17:30 | 17:30 | 21:30 |
| Market end IST | 17:25 | 17:25 | 21:25 |
| Strike step | 200 | 10 | 10 |
| Short wings | ±400 | ±20 | ±40 |
| Hedge | 2000 | 100 | 150 |

Bias widens the threatened wing / tightens the other (same pattern as NIFTY).

## Safety

- Default `DRY_RUN=true` — no live orders without keys + explicit disable
- Start with `BTC_LOT_SIZE=1` / `ETH_LOT_SIZE=1`
- Prefer Delta testnet URL while validating
