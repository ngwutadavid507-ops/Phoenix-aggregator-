# 🔥 Phoenix Terminal PRO v2.0

**Professional Multi-Source Crypto Data Aggregation Platform**

CoinGecko-grade market data with charts, 100% real APIs from 12 sources, zero hardcoding.

## ✨ Features

- **12 Data Sources**: CoinGecko, CoinPaprika, CryptoCompare, CoinMarketCap, Binance, Bybit, OKX, Kraken, KuCoin, Gate.io, MEXC, Bitget, BingX
- **Real-Time Charts**: Market dominance doughnut, volume bar charts via Chart.js
- **Full Market Data**: Prices, market cap, volume, supply, ATH/ATL, dominance, global metrics
- **API Key Authentication**: Free/Pro/Enterprise tiers with rate limiting
- **WebSocket Streaming**: Real-time data push
- **Professional Dashboard**: CoinGecko-grade UI with dark theme
- **Public API**: Free API keys for external integration

## 🚀 Deploy to Render (Free)

```bash
# 1. Push to GitHub
git init && git add . && git commit -m "Phoenix Terminal PRO v2.0"
git remote add origin https://github.com/YOUR_USERNAME/phoenix-terminal-pro.git
git push -u origin main

# 2. Deploy backend to Render
# New Web Service → Connect repo
# Build: pip install -r requirements.txt
# Start: cd app && uvicorn main:app --host 0.0.0.0 --port $PORT
# Plan: Free

# 3. Visit /dashboard for the professional UI
```

## 🔌 API Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/api/v1/health` | No | System health & source status |
| GET | `/api/v1/sources` | No | List all 12 data sources |
| POST | `/api/v1/keys/create` | No | Create API key |
| GET | `/api/v1/coins` | Yes | Top coins (paginated) |
| GET | `/api/v1/coins/{id}` | Yes | Specific coin details |
| GET | `/api/v1/coins/search` | Yes | Search coins |
| GET | `/api/v1/coins/trending` | Yes | Trending coins |
| GET | `/api/v1/coins/categories` | Yes | Coin categories |
| GET | `/api/v1/global` | Yes | Global market metrics |
| GET | `/api/v1/exchanges` | Yes | Exchange data |
| GET | `/api/v1/exchanges/{id}` | Yes | Specific exchange |
| WS | `/ws/live` | No | Real-time stream |

## 🔑 Get Your API Key

```bash
curl -X POST https://your-app.onrender.com/api/v1/keys/create \
  -H "Content-Type: application/json" \
  -d '{"name": "My Project", "tier": "free"}'
```

## 📊 Rate Limits

| Tier | Per Minute | Per Day |
|------|-----------|---------|
| Free | 100 | 1,000 |
| Pro | 1,000 | 100,000 |
| Enterprise | 10,000 | 1,000,000 |

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Dashboard  │────▶│ Phoenix Terminal  │────▶│  12 Data Sources │
│  (/dashboard)│     │   (FastAPI)      │     │  (Real APIs)    │
└─────────────┘     └──────────────────┘     └─────────────────┘
                           │
                    ┌──────┴──────┐
                    │  WebSocket   │
                    │  /ws/live    │
                    └─────────────┘
```

## 📝 License

MIT License
