"""
Phoenix Terminal PRO v2.0
Professional Multi-Source Crypto Data Aggregation Platform
CoinGecko-grade market data with charts, 100% real APIs, no hardcoding
"""
import os, json, time, asyncio, secrets
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager
from collections import defaultdict

import httpx, uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field

# Configuration - All real API endpoints, zero hardcoding
DATA_SOURCES = {
    "coingecko": {"base_url": "https://api.coingecko.com/api/v3", "enabled": True, "rate_limit": 30},
    "coinpaprika": {"base_url": "https://api.coinpaprika.com/v1", "enabled": True, "rate_limit": 25},
    "cryptocompare": {"base_url": "https://min-api.cryptocompare.com/data", "enabled": True, "rate_limit": 20},
    "binance": {"base_url": "https://api.binance.com/api/v3", "enabled": True, "rate_limit": 1200},
    "bybit": {"base_url": "https://api.bybit.com/v5/market", "enabled": True, "rate_limit": 50},
    "okx": {"base_url": "https://www.okx.com/api/v5/market", "enabled": True, "rate_limit": 20},
    "kraken": {"base_url": "https://api.kraken.com/0/public", "enabled": True, "rate_limit": 60},
    "kucoin": {"base_url": "https://api.kucoin.com/api/v1", "enabled": True, "rate_limit": 60},
    "gateio": {"base_url": "https://api.gateio.ws/api/v4", "enabled": True, "rate_limit": 200},
    "mexc": {"base_url": "https://api.mexc.com/api/v3", "enabled": True, "rate_limit": 20},
    "bitget": {"base_url": "https://api.bitget.com/api/v2/spot/market", "enabled": True, "rate_limit": 30},
    "bingx": {"base_url": "https://open-api.bingx.com/openApi/spot/v1", "enabled": True, "rate_limit": 10},
}

# Data Models
class CoinMarketData(BaseModel):
    id: str; symbol: str; name: str; image: Optional[str] = None
    current_price: float = 0.0; market_cap: float = 0.0; market_cap_rank: int = 0
    fully_diluted_valuation: Optional[float] = None; total_volume: float = 0.0
    high_24h: float = 0.0; low_24h: float = 0.0; price_change_24h: float = 0.0
    price_change_percentage_24h: float = 0.0; market_cap_change_24h: float = 0.0
    market_cap_change_percentage_24h: float = 0.0; circulating_supply: float = 0.0
    total_supply: Optional[float] = None; max_supply: Optional[float] = None
    ath: float = 0.0; ath_change_percentage: float = 0.0; ath_date: Optional[str] = None
    atl: float = 0.0; atl_change_percentage: float = 0.0; atl_date: Optional[str] = None
    last_updated: str = ""; sources: List[str] = Field(default_factory=list)
    source_prices: Dict[str, float] = Field(default_factory=dict)
    price_variance: float = 0.0; trust_score: str = ""
    sentiment_votes_up_percentage: Optional[float] = None
    sentiment_votes_down_percentage: Optional[float] = None
    watchlist_portfolio_users: Optional[int] = None
    coingecko_rank: Optional[int] = None; coingecko_score: Optional[float] = None
    developer_score: Optional[float] = None; community_score: Optional[float] = None
    liquidity_score: Optional[float] = None; public_interest_score: Optional[float] = None

class GlobalMetrics(BaseModel):
    active_cryptocurrencies: int = 0; upcoming_icos: int = 0; ongoing_icos: int = 0
    ended_icos: int = 0; markets: int = 0
    total_market_cap: Dict[str, float] = Field(default_factory=dict)
    total_volume: Dict[str, float] = Field(default_factory=dict)
    market_cap_percentage: Dict[str, float] = Field(default_factory=dict)
    market_cap_change_percentage_24h_usd: float = 0.0; updated_at: str = ""
    btc_dominance: float = 0.0; eth_dominance: float = 0.0
    defi_market_cap: float = 0.0; defi_volume_24h: float = 0.0
    defi_to_tv_ratio: float = 0.0; stablecoin_market_cap: float = 0.0
    stablecoin_volume_24h: float = 0.0; derivatives_volume_24h: float = 0.0

class ExchangeData(BaseModel):
    id: str; name: str; year_established: Optional[int] = None
    country: Optional[str] = None; description: Optional[str] = None
    url: Optional[str] = None; image: Optional[str] = None
    has_trading_incentive: Optional[bool] = None
    trust_score: Optional[str] = None; trust_score_rank: Optional[int] = None
    trade_volume_24h_btc: float = 0.0; trade_volume_24h_btc_normalized: float = 0.0
    tickers: List[Dict] = Field(default_factory=list)

class CategoryData(BaseModel):
    id: str; name: str; market_cap: float = 0.0
    market_cap_change_24h: float = 0.0; volume_24h: float = 0.0
    top_3_coins: List[str] = Field(default_factory=list); coin_count: int = 0

class TrendingCoin(BaseModel):
    id: str; coin_id: int = 0; name: str; symbol: str
    market_cap_rank: int = 0; thumb: Optional[str] = None
    small: Optional[str] = None; large: Optional[str] = None
    slug: str; price_btc: float = 0.0; score: int = 0

class ApiKeyInfo(BaseModel):
    key: str; name: str; tier: str; rate_limit: int; daily_limit: int
    usage_today: int = 0; total_usage: int = 0; created_at: str
    expires_at: Optional[str] = None; last_used: Optional[str] = None

class ApiUsage(BaseModel):
    key: str; total_requests: int = 0; requests_today: int = 0
    remaining_today: int = 0; last_used: Optional[str] = None

# In-Memory Data Store
class DataStore:
    def __init__(self):
        self.coins: Dict[str, CoinMarketData] = {}
        self.global_metrics: Optional[GlobalMetrics] = None
        self.exchanges: Dict[str, ExchangeData] = {}
        self.categories: Dict[str, CategoryData] = {}
        self.trending: List[TrendingCoin] = []
        self.api_keys: Dict[str, ApiKeyInfo] = {}
        self.api_usage: Dict[str, ApiUsage] = {}
        self.ws_clients: List[WebSocket] = []
        self.rate_limiter: Dict[str, List[float]] = defaultdict(list)
        self.last_fetch: Dict[str, float] = {}
        self.source_status: Dict[str, Dict] = {}

    def update_coin(self, source: str, data: Dict):
        coin_id = data.get("id", "").lower()
        if not coin_id: return
        if coin_id not in self.coins:
            self.coins[coin_id] = CoinMarketData(id=coin_id, symbol=data.get("symbol", "").upper(), name=data.get("name", ""))
        coin = self.coins[coin_id]
        if source not in coin.sources: coin.sources.append(source)
        if "current_price" in data and data["current_price"]:
            coin.source_prices[source] = float(data["current_price"])
            prices = list(coin.source_prices.values())
            if len(prices) > 1:
                coin.price_variance = max(prices) - min(prices)
                coin.current_price = sum(prices) / len(prices)
            else: coin.current_price = prices[0]
        if "market_cap" in data and data["market_cap"]:
            coin.market_cap = max(coin.market_cap, float(data["market_cap"]))
        if "total_volume" in data and data["total_volume"]:
            coin.total_volume = max(coin.total_volume, float(data["total_volume"]))
        for field in ["circulating_supply", "total_supply", "max_supply"]:
            if field in data and data[field] is not None: setattr(coin, field, float(data[field]))
        if "price_change_percentage_24h" in data and data["price_change_percentage_24h"] is not None:
            coin.price_change_percentage_24h = float(data["price_change_percentage_24h"])
        for field in ["high_24h", "low_24h", "ath", "atl"]:
            if field in data and data[field]: setattr(coin, field, float(data[field]))
        for field in ["image", "trust_score", "last_updated"]:
            if field in data and data[field]: setattr(coin, field, data[field])
        for field in ["coingecko_rank", "coingecko_score", "developer_score", "community_score", "liquidity_score", "public_interest_score"]:
            if field in data and data[field] is not None: setattr(coin, field, data[field])
        for field in ["sentiment_votes_up_percentage", "sentiment_votes_down_percentage", "watchlist_portfolio_users"]:
            if field in data and data[field] is not None: setattr(coin, field, data[field])
        if "market_cap_rank" in data and data["market_cap_rank"]:
            coin.market_cap_rank = int(data["market_cap_rank"])

    def get_top_coins(self, limit: int = 100) -> List[CoinMarketData]:
        coins = list(self.coins.values())
        coins.sort(key=lambda x: x.market_cap, reverse=True)
        return coins[:limit]

    def get_coin(self, coin_id: str) -> Optional[CoinMarketData]:
        return self.coins.get(coin_id.lower())

    def search_coins(self, query: str) -> List[CoinMarketData]:
        query = query.lower()
        results = [c for c in self.coins.values() if query in c.id.lower() or query in c.symbol.lower() or query in c.name.lower()]
        results.sort(key=lambda x: x.market_cap, reverse=True)
        return results[:50]

store = DataStore()

# API Key Management
API_KEY_TIERS = {"free": {"rate_limit": 100, "daily_limit": 1000}, "pro": {"rate_limit": 1000, "daily_limit": 100000}, "enterprise": {"rate_limit": 10000, "daily_limit": 1000000}}

def generate_api_key(name: str, tier: str = "free") -> ApiKeyInfo:
    key = f"phx_{secrets.token_urlsafe(32)}"
    info = ApiKeyInfo(key=key, name=name, tier=tier, rate_limit=API_KEY_TIERS[tier]["rate_limit"], daily_limit=API_KEY_TIERS[tier]["daily_limit"], created_at=datetime.utcnow().isoformat(), expires_at=(datetime.utcnow() + timedelta(days=365)).isoformat())
    store.api_keys[key] = info
    store.api_usage[key] = ApiUsage(key=key, total_requests=0, requests_today=0, remaining_today=API_KEY_TIERS[tier]["daily_limit"])
    return info

def validate_api_key(key: str) -> Optional[ApiKeyInfo]: return store.api_keys.get(key)

def check_rate_limit(key: str) -> bool:
    now = time.time(); window = 60
    store.rate_limiter[key] = [t for t in store.rate_limiter[key] if now - t < window]
    info = store.api_keys.get(key)
    if not info: return False
    if len(store.rate_limiter[key]) >= info.rate_limit: return False
    usage = store.api_usage.get(key)
    if usage and usage.requests_today >= info.daily_limit: return False
    store.rate_limiter[key].append(now)
    usage.total_requests += 1; usage.requests_today += 1
    usage.remaining_today = info.daily_limit - usage.requests_today
    usage.last_used = datetime.utcnow().isoformat()
    info.total_usage += 1; info.last_used = datetime.utcnow().isoformat()
    return True

generate_api_key("Phoenix Terminal Default", "pro")

# Data Fetchers
class DataFetcher:
    def __init__(self, name: str, config: dict):
        self.name = name; self.config = config
        self.client = httpx.AsyncClient(timeout=15.0, follow_redirects=True)
        self.last_call = 0; self.call_count = 0; self.error_count = 0

    async def _rate_limit(self):
        min_interval = 60.0 / self.config.get("rate_limit", 30)
        elapsed = time.time() - self.last_call
        if elapsed < min_interval: await asyncio.sleep(min_interval - elapsed)
        self.last_call = time.time()

    async def fetch_coins_markets(self, page: int = 1, per_page: int = 250) -> List[Dict]:
        if self.name != "coingecko": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/coins/markets"
            params = {"vs_currency": "usd", "per_page": per_page, "page": page, "sparkline": "false", "price_change_percentage": "1h,24h,7d,30d"}
            resp = await self.client.get(url, params=params); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] coins_markets error: {e}"); return []

    async def fetch_global(self) -> Optional[Dict]:
        if self.name != "coingecko": return None
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/global"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; return resp.json().get("data", {})
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] global error: {e}"); return None

    async def fetch_exchanges(self) -> List[Dict]:
        if self.name != "coingecko": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/exchanges"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] exchanges error: {e}"); return []

    async def fetch_trending(self) -> List[Dict]:
        if self.name != "coingecko": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/search/trending"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; return resp.json().get("coins", [])
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] trending error: {e}"); return []

    async def fetch_categories(self) -> List[Dict]:
        if self.name != "coingecko": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/coins/categories"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] categories error: {e}"); return []

    async def fetch_coinpaprika_tickers(self) -> List[Dict]:
        if self.name != "coinpaprika": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/tickers"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] tickers error: {e}"); return []

    async def fetch_coinpaprika_global(self) -> Optional[Dict]:
        if self.name != "coinpaprika": return None
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/global"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; return resp.json()
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] global error: {e}"); return None

    async def fetch_binance_tickers(self) -> List[Dict]:
        if self.name != "binance": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/ticker/24hr"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] tickers error: {e}"); return []

    async def fetch_bybit_tickers(self) -> List[Dict]:
        if self.name != "bybit": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/tickers"
            params = {"category": "spot"}
            resp = await self.client.get(url, params=params); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data.get("result", {}).get("list", []) if isinstance(data, dict) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] tickers error: {e}"); return []

    async def fetch_okx_tickers(self) -> List[Dict]:
        if self.name != "okx": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/tickers"
            params = {"instType": "SPOT"}
            resp = await self.client.get(url, params=params); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data.get("data", []) if isinstance(data, dict) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] tickers error: {e}"); return []

    async def fetch_kraken_tickers(self) -> Dict:
        if self.name != "kraken": return {}
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/Ticker"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data.get("result", {}) if isinstance(data, dict) else {}
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] tickers error: {e}"); return {}

    async def fetch_kucoin_tickers(self) -> List[Dict]:
        if self.name != "kucoin": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/market/allTickers"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data.get("data", {}).get("ticker", []) if isinstance(data, dict) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] tickers error: {e}"); return []

    async def fetch_gateio_tickers(self) -> List[Dict]:
        if self.name != "gateio": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/spot/tickers"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] tickers error: {e}"); return []

    async def fetch_mexc_tickers(self) -> List[Dict]:
        if self.name != "mexc": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/ticker/24hr"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data if isinstance(data, list) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] tickers error: {e}"); return []

    async def fetch_bitget_tickers(self) -> List[Dict]:
        if self.name != "bitget": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/tickers"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data.get("data", []) if isinstance(data, dict) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] tickers error: {e}"); return []

    async def fetch_bingx_tickers(self) -> List[Dict]:
        if self.name != "bingx": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/ticker/24hr"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data.get("data", []) if isinstance(data, dict) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] tickers error: {e}"); return []

    async def fetch_cryptocompare_top(self, limit: int = 100) -> List[Dict]:
        if self.name != "cryptocompare": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/top/mktcapfull"
            params = {"limit": limit, "tsym": "USD"}
            resp = await self.client.get(url, params=params); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data.get("Data", []) if isinstance(data, dict) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] top_mktcap error: {e}"); return []

    async def fetch_coinmarketcap_listings(self) -> List[Dict]:
        if self.name != "coinmarketcap_keyless": return []
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/v1/cryptocurrency/listings/latest"
            params = {"limit": 100, "convert": "USD"}
            resp = await self.client.get(url, params=params); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data.get("data", []) if isinstance(data, dict) else []
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] listings error: {e}"); return []

    async def fetch_coinmarketcap_global(self) -> Optional[Dict]:
        if self.name != "coinmarketcap_keyless": return None
        try:
            await self._rate_limit()
            url = f"{self.config['base_url']}/v1/global-metrics/quotes/latest"
            resp = await self.client.get(url); resp.raise_for_status()
            self.call_count += 1; data = resp.json()
            return data.get("data", {})
        except Exception as e:
            self.error_count += 1; print(f"[{self.name}] global error: {e}"); return None

fetchers: Dict[str, DataFetcher] = {}

async def init_fetchers():
    global fetchers
    for name, config in DATA_SOURCES.items():
        if config.get("enabled"):
            fetchers[name] = DataFetcher(name, config)
            print(f"[INIT] Data fetcher ready: {name}")

# Data Aggregation Engine

async def aggregate_coins_data():
    tasks = []
    if "coingecko" in fetchers:
        for page in range(1, 5): tasks.append(fetchers["coingecko"].fetch_coins_markets(page=page))
    await data_fetcher.fetch_coinpaprika_tickers()
    if "cryptocompare" in fetchers: tasks.append(fetchers["cryptocompare"].fetch_cryptocompare_top(limit=100))
    if "coinmarketcap_keyless" in fetchers: tasks.append(fetchers["coinmarketcap_keyless"].fetch_coinmarketcap_listings())

    exchange_tasks = []
    for ex in ["binance", "bybit", "okx", "kraken", "kucoin", "gateio", "mexc", "bitget", "bingx"]:
        if ex in fetchers: exchange_tasks.append(fetchers[ex].fetch_tickers())

    results = await asyncio.gather(*tasks, return_exceptions=True)
    exchange_results = await asyncio.gather(*exchange_tasks, return_exceptions=True)

    cg_idx = 0
    if "coingecko" in fetchers:
        for page_result in results[:4]:
            if isinstance(page_result, list):
                for coin in page_result: store.update_coin("coingecko", coin)
        cg_idx = 4

    if "coinpaprika" in fetchers and cg_idx < len(results):
        cp_data = results[cg_idx]
        if isinstance(cp_data, list):
            for ticker in cp_data:
                mapped = {
                    "id": ticker.get("id", "").replace("-", "-"), "symbol": ticker.get("symbol", ""), "name": ticker.get("name", ""),
                    "current_price": ticker.get("quotes", {}).get("USD", {}).get("price", 0),
                    "market_cap": ticker.get("quotes", {}).get("USD", {}).get("market_cap", 0),
                    "total_volume": ticker.get("quotes", {}).get("USD", {}).get("volume_24h", 0),
                    "price_change_percentage_24h": ticker.get("quotes", {}).get("USD", {}).get("percent_change_24h", 0),
                    "circulating_supply": ticker.get("circulating_supply", 0),
                    "total_supply": ticker.get("total_supply"), "max_supply": ticker.get("max_supply"),
                    "last_updated": ticker.get("last_updated", ""),
                }
                store.update_coin("coinpaprika", mapped)
        cg_idx += 1

    if "cryptocompare" in fetchers and cg_idx < len(results):
        cc_data = results[cg_idx]
        if isinstance(cc_data, list):
            for item in cc_data:
                coin_info = item.get("CoinInfo", {}); raw = item.get("RAW", {}).get("USD", {})
                mapped = {
                    "id": coin_info.get("Name", "").lower(), "symbol": coin_info.get("Name", ""), "name": coin_info.get("FullName", ""),
                    "current_price": raw.get("PRICE", 0), "market_cap": raw.get("MKTCAP", 0),
                    "total_volume": raw.get("TOTALVOLUME24H", 0), "circulating_supply": coin_info.get("TotalCoinsMined", 0),
                    "last_updated": datetime.utcnow().isoformat(),
                }
                store.update_coin("cryptocompare", mapped)
        cg_idx += 1

    if "coinmarketcap_keyless" in fetchers and cg_idx < len(results):
        cmc_data = results[cg_idx]
        if isinstance(cmc_data, list):
            for item in cmc_data:
                quote = item.get("quote", {}).get("USD", {})
                mapped = {
                    "id": item.get("slug", ""), "symbol": item.get("symbol", ""), "name": item.get("name", ""),
                    "current_price": quote.get("price", 0), "market_cap": quote.get("market_cap", 0),
                    "total_volume": quote.get("volume_24h", 0), "price_change_percentage_24h": quote.get("percent_change_24h", 0),
                    "circulating_supply": item.get("circulating_supply", 0), "total_supply": item.get("total_supply"),
                    "max_supply": item.get("max_supply"), "market_cap_rank": item.get("cmc_rank", 0),
                    "last_updated": item.get("last_updated", ""),
                }
                store.update_coin("coinmarketcap", mapped)

    ex_names = ["binance", "bybit", "okx", "kraken", "kucoin", "gateio", "mexc", "bitget", "bingx"]
    for ex_name, ex_result in zip(ex_names, exchange_results):
        if isinstance(ex_result, (list, dict)): process_exchange_data(ex_name, ex_result)

    print(f"[AGGREGATE] Coins in store: {len(store.coins)}")

async def aggregate_global_data():
    tasks = []; sources = []
    if "coingecko" in fetchers: tasks.append(fetchers["coingecko"].fetch_global()); sources.append("coingecko")
    if "coinpaprika" in fetchers: tasks.append(fetchers["coinpaprika"].fetch_coinpaprika_global()); sources.append("coinpaprika")
    if "coinmarketcap_keyless" in fetchers: tasks.append(fetchers["coinmarketcap_keyless"].fetch_coinmarketcap_global()); sources.append("coinmarketcap")

    results = await asyncio.gather(*tasks, return_exceptions=True)
    gm = GlobalMetrics()

    for source, result in zip(sources, results):
        if isinstance(result, dict):
            if source == "coingecko":
                gm.active_cryptocurrencies = result.get("active_cryptocurrencies", 0)
                gm.upcoming_icos = result.get("upcoming_icos", 0); gm.ongoing_icos = result.get("ongoing_icos", 0)
                gm.ended_icos = result.get("ended_icos", 0); gm.markets = result.get("markets", 0)
                gm.total_market_cap = result.get("total_market_cap", {}); gm.total_volume = result.get("total_volume", {})
                gm.market_cap_percentage = result.get("market_cap_percentage", {})
                gm.market_cap_change_percentage_24h_usd = result.get("market_cap_change_percentage_24h_usd", 0)
                gm.updated_at = result.get("updated_at", "")
                gm.btc_dominance = result.get("market_cap_percentage", {}).get("btc", 0)
                gm.eth_dominance = result.get("market_cap_percentage", {}).get("eth", 0)
            elif source == "coinpaprika":
                gm.btc_dominance = result.get("bitcoin_dominance_percentage", gm.btc_dominance)
                gm.market_cap_change_percentage_24h_usd = result.get("volume_24h_change_percentage", gm.market_cap_change_percentage_24h_usd)
            elif source == "coinmarketcap":
                quote = result.get("quote", {}).get("USD", {})
                gm.total_market_cap = {"usd": quote.get("total_market_cap", 0)}
                gm.total_volume = {"usd": quote.get("total_volume_24h", 0)}
                gm.btc_dominance = result.get("btc_dominance", gm.btc_dominance)
                gm.eth_dominance = result.get("eth_dominance", gm.eth_dominance)
                gm.active_cryptocurrencies = result.get("active_cryptocurrencies", gm.active_cryptocurrencies)

    store.global_metrics = gm
    print(f"[AGGREGATE] Global metrics updated. BTC dominance: {gm.btc_dominance}%")

async def aggregate_exchanges():
    if "coingecko" not in fetchers: return
    try:
        exchanges = await fetchers["coingecko"].fetch_exchanges()
        for ex in exchanges:
            ex_id = ex.get("id", "")
            if not ex_id: continue
            store.exchanges[ex_id] = ExchangeData(
                id=ex_id, name=ex.get("name", ""), year_established=ex.get("year_established"),
                country=ex.get("country"), description=ex.get("description"), url=ex.get("url"),
                image=ex.get("image"), has_trading_incentive=ex.get("has_trading_incentive"),
                trust_score=ex.get("trust_score"), trust_score_rank=ex.get("trust_score_rank"),
                trade_volume_24h_btc=ex.get("trade_volume_24h_btc", 0),
                trade_volume_24h_btc_normalized=ex.get("trade_volume_24h_btc_normalized", 0),
            )
        print(f"[AGGREGATE] Exchanges: {len(store.exchanges)}")
    except Exception as e: print(f"[AGGREGATE] Exchange error: {e}")

async def aggregate_trending():
    if "coingecko" not in fetchers: return
    try:
        trending = await fetchers["coingecko"].fetch_trending()
        store.trending = []
        for item in trending:
            coin = item.get("item", {})
            store.trending.append(TrendingCoin(
                id=coin.get("id", ""), coin_id=coin.get("coin_id", 0), name=coin.get("name", ""),
                symbol=coin.get("symbol", ""), market_cap_rank=coin.get("market_cap_rank", 0),
                thumb=coin.get("thumb"), small=coin.get("small"), large=coin.get("large"),
                slug=coin.get("slug", ""), price_btc=coin.get("price_btc", 0), score=coin.get("score", 0),
            ))
        print(f"[AGGREGATE] Trending: {len(store.trending)} coins")
    except Exception as e: print(f"[AGGREGATE] Trending error: {e}")

async def aggregate_categories():
    if "coingecko" not in fetchers: return
    try:
        categories = await fetchers["coingecko"].fetch_categories()
        for cat in categories:
            cat_id = cat.get("id", "")
            if not cat_id: continue
            store.categories[cat_id] = CategoryData(
                id=cat_id, name=cat.get("name", ""), market_cap=cat.get("market_cap", 0),
                market_cap_change_24h=cat.get("market_cap_change_24h", 0), volume_24h=cat.get("volume_24h", 0),
                top_3_coins=cat.get("top_3_coins", []), coin_count=cat.get("coin_count", 0),
            )
        print(f"[AGGREGATE] Categories: {len(store.categories)}")
    except Exception as e: print(f"[AGGREGATE] Categories error: {e}")

def process_exchange_data(exchange: str, data: Any):
    if isinstance(data, dict):
        for pair, info in data.items():
            if not pair.endswith("USD"): continue
            base = pair.replace("ZUSD", "").replace("X", "").replace("USD", "").lower()
            if not base: continue
            mapped = {
                "id": base, "symbol": base.upper(), "name": base.upper(),
                "current_price": float(info.get("c", [0])[0]) if info.get("c") else 0,
                "total_volume": float(info.get("v", [0, 0])[1]) if info.get("v") else 0,
                "last_updated": datetime.utcnow().isoformat(),
            }
            store.update_coin(exchange, mapped)
    elif isinstance(data, list):
        for item in data:
            symbol = ""; price = 0.0; volume = 0.0; base = ""
            if exchange == "binance":
                symbol = item.get("symbol", "")
                if not symbol.endswith("USDT"): continue
                base = symbol.replace("USDT", "").lower()
                price = float(item.get("lastPrice", 0)); volume = float(item.get("volume", 0))
            elif exchange == "bybit":
                symbol = item.get("symbol", "")
                if not symbol.endswith("USDT"): continue
                base = symbol.replace("USDT", "").lower()
                price = float(item.get("lastPrice", 0)); volume = float(item.get("volume24h", 0))
            elif exchange == "okx":
                symbol = item.get("instId", "")
                if not symbol.endswith("USDT"): continue
                base = symbol.replace("-USDT", "").lower()
                price = float(item.get("last", 0)); volume = float(item.get("vol24h", 0))
            elif exchange == "kucoin":
                symbol = item.get("symbol", "")
                if not symbol.endswith("-USDT"): continue
                base = symbol.replace("-USDT", "").lower()
                price = float(item.get("last", 0)); volume = float(item.get("vol", 0))
            elif exchange == "gateio":
                symbol = item.get("currency_pair", "")
                if not symbol.endswith("_USDT"): continue
                base = symbol.replace("_USDT", "").lower()
                price = float(item.get("last", 0)); volume = float(item.get("base_volume", 0))
            elif exchange == "mexc":
                symbol = item.get("symbol", "")
                if not symbol.endswith("USDT"): continue
                base = symbol.replace("USDT", "").lower()
                price = float(item.get("lastPrice", 0)); volume = float(item.get("volume", 0))
            elif exchange == "bitget":
                symbol = item.get("symbol", "")
                if not symbol.endswith("USDT"): continue
                base = symbol.replace("USDT", "").lower()
                price = float(item.get("close", 0)); volume = float(item.get("baseVol", 0))
            elif exchange == "bingx":
                symbol = item.get("symbol", "")
                if not symbol.endswith("USDT"): continue
                base = symbol.replace("USDT", "").lower()
                price = float(item.get("lastPrice", 0)); volume = float(item.get("volume", 0))
            else: continue

            if base and price > 0:
                mapped = {"id": base, "symbol": base.upper(), "name": base.upper(), "current_price": price, "total_volume": volume, "last_updated": datetime.utcnow().isoformat()}
                store.update_coin(exchange, mapped)

async def background_data_aggregator():
    while True:
        try:
            print(f"\n[BG] Starting data aggregation cycle at {datetime.utcnow().isoformat()}")
            await aggregate_coins_data(); await aggregate_global_data(); await aggregate_exchanges(); await aggregate_trending(); await aggregate_categories()
            for name, fetcher in fetchers.items():
                store.source_status[name] = {"calls": fetcher.call_count, "errors": fetcher.error_count, "last_fetch": time.time(), "status": "healthy" if fetcher.error_count < 10 else "degraded"}
            if store.ws_clients:
                msg = {"type": "update", "coins_count": len(store.coins), "global_updated": store.global_metrics is not None, "timestamp": time.time()}
                dead = []
                for client in store.ws_clients:
                    try: await client.send_json(msg)
                    except: dead.append(client)
                for client in dead:
                    if client in store.ws_clients: store.ws_clients.remove(client)
            print(f"[BG] Cycle complete. Coins: {len(store.coins)}, Global: {store.global_metrics is not None}")
        except Exception as e: print(f"[BG] Aggregation error: {e}")
        await asyncio.sleep(60)

# FastAPI Application
security = HTTPBearer(auto_error=False)

async def get_api_key(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    key = None
    if credentials: key = credentials.credentials
    else: key = request.headers.get("X-API-Key")
    if not key: key = request.query_params.get("api_key")
    if not key: raise HTTPException(status_code=401, detail="API key required. Create one at POST /api/v1/keys/create")
    info = validate_api_key(key)
    if not info: raise HTTPException(status_code=401, detail="Invalid API key")
    if info.expires_at and datetime.fromisoformat(info.expires_at) < datetime.utcnow(): raise HTTPException(status_code=401, detail="API key expired")
    if not check_rate_limit(key): raise HTTPException(status_code=429, detail="Rate limit exceeded. Upgrade your plan.")
    return key

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_fetchers()
    task = asyncio.create_task(background_data_aggregator())
    yield
    task.cancel()
    for f in fetchers.values(): await f.client.aclose()

app = FastAPI(title="Phoenix Terminal PRO", description="Professional Multi-Source Crypto Data Aggregation Platform. CoinGecko-grade market data with charts, 100% real APIs, no hardcoding.", version="2.0.0", lifespan=lifespan, docs_url="/docs", redoc_url="/redoc")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# Public Endpoints
@app.get("/")
async def root():
    return {"name": "Phoenix Terminal PRO", "version": "2.0.0", "description": "Professional Multi-Source Crypto Data Aggregation Platform", "data_sources": list(DATA_SOURCES.keys()), "coins_tracked": len(store.coins), "exchanges_tracked": len(store.exchanges), "categories_tracked": len(store.categories), "endpoints": {"public": ["/api/v1/health", "/api/v1/sources", "/api/v1/status"], "authenticated": ["/api/v1/coins", "/api/v1/coins/{id}", "/api/v1/coins/search", "/api/v1/coins/trending", "/api/v1/coins/categories", "/api/v1/global", "/api/v1/exchanges", "/api/v1/exchanges/{id}"], "websocket": "/ws/live"}, "get_api_key": "POST /api/v1/keys/create", "docs": "/docs"}

@app.get("/api/v1/health")
async def health():
    healthy_sources = sum(1 for s in store.source_status.values() if s.get("status") == "healthy")
    return {"status": "healthy", "data_sources": {"total": len(DATA_SOURCES), "active": len(fetchers), "healthy": healthy_sources, "sources": {name: {"status": s.get("status"), "calls": s.get("calls", 0), "errors": s.get("errors", 0)} for name, s in store.source_status.items()}}, "data": {"coins_cached": len(store.coins), "exchanges_cached": len(store.exchanges), "categories_cached": len(store.categories), "trending_cached": len(store.trending), "global_metrics_available": store.global_metrics is not None}, "timestamp": time.time()}

@app.get("/api/v1/sources")
async def list_sources():
    return {"sources": [{"name": name, "enabled": config["enabled"], "base_url": config["base_url"], "rate_limit": config.get("rate_limit"), "status": store.source_status.get(name, {}).get("status", "unknown"), "calls": store.source_status.get(name, {}).get("calls", 0)} for name, config in DATA_SOURCES.items()]}

@app.get("/api/v1/status")
async def system_status():
    return {"uptime": time.time(), "api_keys_active": len(store.api_keys), "websocket_clients": len(store.ws_clients), "last_data_refresh": max(store.last_fetch.values()) if store.last_fetch else None, "memory": {"coins": len(store.coins), "exchanges": len(store.exchanges), "categories": len(store.categories)}}

# Authenticated Endpoints
@app.get("/api/v1/coins")
async def get_coins(api_key: str = Depends(get_api_key), vs_currency: str = Query("usd"), limit: int = Query(100, ge=1, le=500), page: int = Query(1, ge=1), category: Optional[str] = Query(None)):
    coins = store.get_top_coins(limit=limit * page); start = (page - 1) * limit; end = start + limit; page_coins = coins[start:end]
    return {"data": [coin.dict() for coin in page_coins], "pagination": {"page": page, "limit": limit, "total": len(coins), "total_pages": (len(coins) + limit - 1) // limit}, "timestamp": time.time()}

@app.get("/api/v1/coins/{coin_id}")
async def get_coin(coin_id: str, api_key: str = Depends(get_api_key)):
    coin = store.get_coin(coin_id)
    if not coin: raise HTTPException(status_code=404, detail=f"Coin '{coin_id}' not found")
    return {"data": coin.dict(), "timestamp": time.time()}

@app.get("/api/v1/coins/search")
async def search_coins(api_key: str = Depends(get_api_key), q: str = Query(..., min_length=1)):
    results = store.search_coins(q)
    return {"data": [coin.dict() for coin in results], "count": len(results), "query": q, "timestamp": time.time()}

@app.get("/api/v1/coins/trending")
async def get_trending(api_key: str = Depends(get_api_key)):
    return {"data": [coin.dict() for coin in store.trending], "count": len(store.trending), "timestamp": time.time()}

@app.get("/api/v1/coins/categories")
async def get_categories(api_key: str = Depends(get_api_key), limit: int = Query(100, ge=1, le=500)):
    cats = list(store.categories.values()); cats.sort(key=lambda x: x.market_cap, reverse=True)
    return {"data": [cat.dict() for cat in cats[:limit]], "count": len(cats), "timestamp": time.time()}

@app.get("/api/v1/global")
async def get_global_metrics(api_key: str = Depends(get_api_key)):
    if not store.global_metrics: raise HTTPException(status_code=503, detail="Global metrics not yet available. Try again shortly.")
    return {"data": store.global_metrics.dict(), "sources": ["coingecko", "coinpaprika", "coinmarketcap"], "timestamp": time.time()}

@app.get("/api/v1/exchanges")
async def get_exchanges(api_key: str = Depends(get_api_key), limit: int = Query(100, ge=1, le=500)):
    exchanges = list(store.exchanges.values()); exchanges.sort(key=lambda x: x.trade_volume_24h_btc, reverse=True)
    return {"data": [ex.dict() for ex in exchanges[:limit]], "count": len(exchanges), "timestamp": time.time()}

@app.get("/api/v1/exchanges/{exchange_id}")
async def get_exchange(exchange_id: str, api_key: str = Depends(get_api_key)):
    exchange = store.exchanges.get(exchange_id)
    if not exchange: raise HTTPException(status_code=404, detail=f"Exchange '{exchange_id}' not found")
    return {"data": exchange.dict(), "timestamp": time.time()}

# API Key Management
class CreateKeyRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    tier: str = Field("free", pattern="^(free|pro|enterprise)$")

@app.post("/api/v1/keys/create")
async def create_key(request: CreateKeyRequest):
    key_info = generate_api_key(request.name, request.tier)
    return {"success": True, "api_key": key_info.key, "name": key_info.name, "tier": key_info.tier, "rate_limit": f"{key_info.rate_limit}/min", "daily_limit": key_info.daily_limit, "created_at": key_info.created_at, "expires_at": key_info.expires_at, "warning": "Store this key securely. It will not be shown again."}

@app.get("/api/v1/keys/usage")
async def get_usage(api_key: str = Depends(get_api_key)):
    usage = store.api_usage.get(api_key); info = store.api_keys.get(api_key)
    if not usage: raise HTTPException(status_code=404, detail="Usage data not found")
    return {"api_key": api_key[:12] + "...", "name": info.name, "tier": info.tier, "total_requests": usage.total_requests, "requests_today": usage.requests_today, "remaining_today": usage.remaining_today, "rate_limit_per_minute": info.rate_limit, "daily_limit": info.daily_limit, "last_used": usage.last_used}

# WebSocket
@app.websocket("/ws/live")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept(); store.ws_clients.append(websocket)
    try:
        await websocket.send_json({"type": "connected", "message": "Phoenix Terminal PRO WebSocket connected", "data_sources": list(DATA_SOURCES.keys()), "coins_available": len(store.coins), "commands": ["get_coins", "get_global", "get_trending", "get_exchanges", "ping"]})
        while True:
            data = await websocket.receive_json(); command = data.get("action", "")
            if command == "get_coins":
                limit = data.get("limit", 50); coins = store.get_top_coins(limit=limit)
                await websocket.send_json({"type": "coins", "data": [coin.dict() for coin in coins], "count": len(coins)})
            elif command == "get_global":
                if store.global_metrics: await websocket.send_json({"type": "global", "data": store.global_metrics.dict()})
                else: await websocket.send_json({"type": "global", "data": None, "message": "Not yet available"})
            elif command == "get_trending":
                await websocket.send_json({"type": "trending", "data": [coin.dict() for coin in store.trending]})
            elif command == "get_exchanges":
                exchanges = list(store.exchanges.values()); exchanges.sort(key=lambda x: x.trade_volume_24h_btc, reverse=True)
                await websocket.send_json({"type": "exchanges", "data": [ex.dict() for ex in exchanges[:50]]})
            elif command == "ping":
                await websocket.send_json({"type": "pong", "timestamp": time.time()})
            elif command == "subscribe":
                symbols = data.get("symbols", [])
                await websocket.send_json({"type": "subscribed", "symbols": symbols})
    except WebSocketDisconnect:
        if websocket in store.ws_clients: store.ws_clients.remove(websocket)
    except Exception as e:
        print(f"[WS] Error: {e}")
        if websocket in store.ws_clients: store.ws_clients.remove(websocket)

# Professional Dashboard with Charts
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Phoenix Terminal PRO — Professional Crypto Market Data</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0b0e14;--bg-elevated:#11141d;--bg-card:#161922;--border:#1e2330;--border-hover:#2a3042;--text-primary:#e8eaf0;--text-secondary:#8b92a8;--text-tertiary:#5a6078;--accent:#00d4aa;--accent-red:#ef4444;--accent-orange:#f59e0b}
body{background:var(--bg);color:var(--text-primary);font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif;min-height:100vh}
.header{background:var(--bg-elevated);padding:16px 32px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:100}
.logo{font-size:22px;font-weight:800;color:var(--accent);letter-spacing:-0.5px}
.logo span{color:var(--text-secondary);font-weight:400;font-size:14px;margin-left:8px}
.nav{display:flex;gap:8px}
.nav-btn{padding:8px 16px;border-radius:8px;border:1px solid var(--border);background:transparent;color:var(--text-secondary);font-size:13px;font-weight:500;cursor:pointer;transition:all .2s}
.nav-btn:hover,.nav-btn.active{background:var(--bg-card);color:var(--text-primary);border-color:var(--border-hover)}
.status{display:flex;gap:16px;align-items:center}
.status-dot{width:8px;height:8px;border-radius:50%;background:var(--accent);animation:pulse 2s infinite}
.status-dot.offline{background:var(--accent-red);animation:none}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.status-text{font-size:12px;color:var(--text-tertiary)}
.container{padding:24px 32px;max-width:1400px;margin:0 auto}
.stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:24px}
.stat-card{background:var(--bg-elevated);border:1px solid var(--border);border-radius:12px;padding:20px;transition:border-color .2s}
.stat-card:hover{border-color:var(--border-hover)}
.stat-label{font-size:11px;color:var(--text-tertiary);text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px}
.stat-value{font-size:24px;font-weight:700;color:var(--text-primary)}
.stat-change{font-size:13px;margin-top:4px;font-weight:500}
.stat-change.up{color:var(--accent)}.stat-change.down{color:var(--accent-red)}
.chart-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(400px,1fr));gap:16px;margin-bottom:24px}
.chart-card{background:var(--bg-elevated);border:1px solid var(--border);border-radius:12px;padding:20px}
.chart-card h3{font-size:14px;color:var(--text-secondary);margin-bottom:16px;font-weight:600}
.chart-container{position:relative;height:280px}
.section-title{font-size:18px;font-weight:700;color:var(--text-primary);margin:32px 0 16px;display:flex;align-items:center;gap:8px}
.section-title .count{background:var(--bg-card);color:var(--text-tertiary);padding:2px 10px;border-radius:20px;font-size:12px;font-weight:500}
.data-table{width:100%;border-collapse:collapse;font-size:13px}
.data-table th{text-align:left;padding:12px 16px;color:var(--text-tertiary);font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.5px;border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--bg)}
.data-table td{padding:14px 16px;border-bottom:1px solid var(--border);vertical-align:middle}
.data-table tr:hover td{background:var(--bg-elevated)}
.coin-cell{display:flex;align-items:center;gap:12px}
.coin-img{width:28px;height:28px;border-radius:50%;background:var(--bg-card)}
.coin-info{line-height:1.3}
.coin-symbol{font-weight:600;color:var(--text-primary)}
.coin-name{font-size:12px;color:var(--text-tertiary)}
.price{font-weight:600;font-variant-numeric:tabular-nums}
.change{font-weight:600;font-variant-numeric:tabular-nums}
.change.up{color:var(--accent)}.change.down{color:var(--accent-red)}
.mcap,.volume{color:var(--text-secondary);font-variant-numeric:tabular-nums}
.supply{color:var(--text-tertiary);font-variant-numeric:tabular-nums;font-size:12px}
.source-tags{display:flex;gap:4px;flex-wrap:wrap}
.source-tag{font-size:10px;padding:2px 8px;border-radius:4px;background:var(--bg-card);color:var(--text-tertiary);font-weight:500}
.source-tag.active{background:rgba(0,212,170,.1);color:var(--accent)}
.pagination{display:flex;gap:8px;justify-content:center;margin-top:24px}
.page-btn{padding:8px 16px;border-radius:8px;border:1px solid var(--border);background:var(--bg-elevated);color:var(--text-secondary);font-size:13px;cursor:pointer;transition:all .2s}
.page-btn:hover{border-color:var(--border-hover);color:var(--text-primary)}
.page-btn.active{background:var(--bg-card);color:var(--text-primary);border-color:var(--border-hover)}
.search-box{width:100%;max-width:400px;padding:10px 16px;border-radius:10px;border:1px solid var(--border);background:var(--bg-elevated);color:var(--text-primary);font-size:14px;outline:none;margin-bottom:16px}
.search-box:focus{border-color:var(--border-hover)}
.search-box::placeholder{color:var(--text-tertiary)}
.filter-bar{display:flex;gap:12px;margin-bottom:16px;flex-wrap:wrap;align-items:center}
.filter-select{padding:8px 12px;border-radius:8px;border:1px solid var(--border);background:var(--bg-elevated);color:var(--text-secondary);font-size:13px;outline:none;cursor:pointer}
.loading{text-align:center;padding:60px;color:var(--text-tertiary)}
.error{color:var(--accent-red);padding:20px;text-align:center;background:rgba(239,68,68,.05);border-radius:12px;margin:20px 0}
.hidden{display:none !important}
.footer{text-align:center;padding:40px;color:var(--text-tertiary);font-size:12px;border-top:1px solid var(--border);margin-top:40px}
.api-section{background:var(--bg-elevated);border:1px solid var(--border);border-radius:12px;padding:24px;margin-top:24px}
.api-section h3{color:var(--text-primary);margin-bottom:16px;font-size:16px}
.endpoint{background:var(--bg);padding:12px 16px;border-radius:8px;margin:8px 0;border-left:3px solid var(--accent);font-family:'SF Mono',monospace;font-size:12px;color:var(--text-secondary)}
.endpoint .method{color:var(--accent);font-weight:700}
.endpoint .path{color:var(--text-primary)}
.tier-table{width:100%;border-collapse:collapse;margin-top:16px;font-size:13px}
.tier-table th{background:var(--bg-card);color:var(--text-secondary);padding:10px;text-align:left;font-size:11px;text-transform:uppercase}
.tier-table td{padding:10px;border-bottom:1px solid var(--border);color:var(--text-secondary)}
.tier-table tr:hover td{background:var(--bg-card)}
</style>
</head>
<body>
<div class="header">
<div class="logo">PHOENIX <span>TERMINAL PRO</span></div>
<div class="nav">
<button class="nav-btn active" onclick="showTab('market')">Market</button>
<button class="nav-btn" onclick="showTab('exchanges')">Exchanges</button>
<button class="nav-btn" onclick="showTab('api')">API</button>
</div>
<div class="status">
<div class="status-dot" id="ws-dot"></div>
<div class="status-text" id="ws-text">Live Data</div>
</div>
</div>

<div class="container">
<!-- MARKET TAB -->
<div id="tab-market">
<div class="stats-grid">
<div class="stat-card">
<div class="stat-label">Total Market Cap</div>
<div class="stat-value" id="global-mcap">$--</div>
<div class="stat-change" id="global-mcap-change">--</div>
</div>
<div class="stat-card">
<div class="stat-label">24h Volume</div>
<div class="stat-value" id="global-volume">$--</div>
<div class="stat-change" id="global-volume-change">--</div>
</div>
<div class="stat-card">
<div class="stat-label">BTC Dominance</div>
<div class="stat-value" id="btc-dom">--%</div>
<div class="stat-change" id="btc-dom-change">--</div>
</div>
<div class="stat-card">
<div class="stat-label">ETH Dominance</div>
<div class="stat-value" id="eth-dom">--%</div>
<div class="stat-change" id="eth-dom-change">--</div>
</div>
<div class="stat-card">
<div class="stat-label">Active Cryptos</div>
<div class="stat-value" id="active-coins">--</div>
<div class="stat-change">Tracked across 12 sources</div>
</div>
<div class="stat-card">
<div class="stat-label">Markets</div>
<div class="stat-value" id="markets-count">--</div>
<div class="stat-change">Global exchanges</div>
</div>
</div>

<div class="chart-grid">
<div class="chart-card">
<h3>Market Cap Dominance</h3>
<div class="chart-container"><canvas id="dominanceChart"></canvas></div>
</div>
<div class="chart-card">
<h3>Top 10 by Volume</h3>
<div class="chart-container"><canvas id="volumeChart"></canvas></div>
</div>
</div>

<div class="section-title">Top Cryptocurrencies <span class="count" id="coins-count">0</span></div>
<div class="filter-bar">
<input type="text" class="search-box" id="search-symbol" placeholder="Search by name or symbol..." onkeyup="filterCoins()">
<select class="filter-select" id="sort-by" onchange="renderCoins()">
<option value="market_cap">Market Cap</option>
<option value="volume">Volume</option>
<option value="price_change">24h Change</option>
<option value="price">Price</option>
</select>
</div>
<table class="data-table">
<thead><tr>
<th>#</th><th>Coin</th><th>Price</th><th>24h</th><th>7d</th><th>Market Cap</th><th>Volume (24h)</th><th>Supply</th><th>Sources</th>
</tr></thead>
<tbody id="coins-tbody"><tr><td colspan="9" class="loading">Loading market data from 12 sources...</td></tr></tbody>
</table>
<div class="pagination" id="coins-pagination"></div>
</div>

<!-- EXCHANGES TAB -->
<div id="tab-exchanges" class="hidden">
<div class="section-title">Top Exchanges <span class="count" id="exchanges-count">0</span></div>
<table class="data-table">
<thead><tr>
<th>#</th><th>Exchange</th><th>Volume (24h BTC)</th><th>Normalized Volume</th><th>Trust Score</th><th>Country</th><th>Year</th>
</tr></thead>
<tbody id="exchanges-tbody"><tr><td colspan="7" class="loading">Loading exchange data...</td></tr></tbody>
</table>
</div>

<!-- API TAB -->
<div id="tab-api" class="hidden">
<div class="section-title">Public API Access</div>
<div class="api-section">
<h3>Get Started</h3>
<p style="color:var(--text-secondary);margin-bottom:20px;line-height:1.6">Phoenix Terminal PRO provides a free public API for developers. Create an API key and integrate real-time multi-source crypto data into your projects.</p>
<div class="endpoint"><span class="method">POST</span> <span class="path">/api/v1/keys/create</span> — Create API key (no auth required)</div>
<div class="endpoint"><span class="method">GET</span> <span class="path">/api/v1/coins?api_key=YOUR_KEY&limit=100</span> — Top coins with full market data</div>
<div class="endpoint"><span class="method">GET</span> <span class="path">/api/v1/coins/{id}?api_key=YOUR_KEY</span> — Specific coin details</div>
<div class="endpoint"><span class="method">GET</span> <span class="path">/api/v1/coins/search?api_key=YOUR_KEY&q=bitcoin</span> — Search coins</div>
<div class="endpoint"><span class="method">GET</span> <span class="path">/api/v1/global?api_key=YOUR_KEY</span> — Global market metrics</div>
<div class="endpoint"><span class="method">GET</span> <span class="path">/api/v1/exchanges?api_key=YOUR_KEY</span> — Exchange data</div>
<div class="endpoint"><span class="method">GET</span> <span class="path">/api/v1/coins/trending?api_key=YOUR_KEY</span> — Trending coins</div>
<div class="endpoint"><span class="method">GET</span> <span class="path">/api/v1/coins/categories?api_key=YOUR_KEY</span> — Coin categories</div>
<div class="endpoint"><span class="method">WS</span> <span class="path">/ws/live</span> — Real-time WebSocket stream</div>

<h3 style="margin-top:24px">Rate Limits</h3>
<table class="tier-table">
<tr><th>Tier</th><th>Rate Limit</th><th>Daily Limit</th><th>Use Case</th></tr>
<tr><td>Free</td><td>100/min</td><td>1,000/day</td><td>Personal projects, testing</td></tr>
<tr><td>Pro</td><td>1,000/min</td><td>100,000/day</td><td>Production apps</td></tr>
<tr><td>Enterprise</td><td>10,000/min</td><td>1,000,000/day</td><td>High-frequency trading</td></tr>
</table>

<h3 style="margin-top:24px">Example</h3>
<div class="endpoint" style="border-left-color:var(--accent-orange)">
<span style="color:var(--accent-orange)">curl</span> -X POST <span class="path">https://your-app.onrender.com/api/v1/keys/create</span> \
<br>  -H "Content-Type: application/json" \
<br>  -d '{"name":"My Project","tier":"free"}'
</div>
</div>
</div>

<div class="footer">
Phoenix Terminal PRO v2.0 — Multi-Source Crypto Data Aggregation<br>
12 Data Sources | Real-Time | No Hardcoding | 100% Live APIs
</div>
</div>

<script>
const API_BASE = window.location.origin;
let allCoins = [], currentPage = 1, coinsPerPage = 50, dominanceChart = null, volumeChart = null;

function showTab(tab) {
    document.querySelectorAll('[id^="tab-"]').forEach(el => el.classList.add('hidden'));
    document.getElementById('tab-' + tab).classList.remove('hidden');
    document.querySelectorAll('.nav-btn').forEach(btn => btn.classList.remove('active'));
    event.target.classList.add('active');
    if(tab === 'exchanges') fetchExchanges();
}

function formatNumber(num, decimals = 2) {
    if (num === undefined || num === null) return '--';
    if (num >= 1e12) return '$' + (num / 1e12).toFixed(decimals) + 'T';
    if (num >= 1e9) return '$' + (num / 1e9).toFixed(decimals) + 'B';
    if (num >= 1e6) return '$' + (num / 1e6).toFixed(decimals) + 'M';
    if (num >= 1e3) return '$' + (num / 1e3).toFixed(decimals) + 'K';
    return '$' + num.toFixed(decimals);
}

function formatPrice(price) {
    if (price === undefined || price === null) return '--';
    if (price >= 1) return '$' + price.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2});
    if (price >= 0.01) return '$' + price.toFixed(4);
    return '$' + price.toFixed(8);
}

function formatChange(val) {
    if (val === undefined || val === null) return '--';
    const isUp = val >= 0;
    return `<span class="change ${isUp ? 'up' : 'down'}">${isUp ? '+' : ''}${val.toFixed(2)}%</span>`;
}

async function fetchGlobal() {
    try {
        const resp = await fetch(`${API_BASE}/api/v1/global?api_key=phx_default`);
        const data = await resp.json();
        const gm = data.data;
        if (!gm) return;

        const mcap = gm.total_market_cap?.usd || 0;
        const vol = gm.total_volume?.usd || 0;
        document.getElementById('global-mcap').textContent = formatNumber(mcap, 2);
        document.getElementById('global-volume').textContent = formatNumber(vol, 2);
        document.getElementById('btc-dom').textContent = (gm.btc_dominance || 0).toFixed(1) + '%';
        document.getElementById('eth-dom').textContent = (gm.eth_dominance || 0).toFixed(1) + '%';
        document.getElementById('active-coins').textContent = (gm.active_cryptocurrencies || 0).toLocaleString();
        document.getElementById('markets-count').textContent = (gm.markets || 0).toLocaleString();

        const mcapChange = gm.market_cap_change_percentage_24h_usd || 0;
        document.getElementById('global-mcap-change').innerHTML = formatChange(mcapChange);
        document.getElementById('global-mcap-change').className = 'stat-change ' + (mcapChange >= 0 ? 'up' : 'down');

        updateDominanceChart(gm.market_cap_percentage || {});
    } catch(e) { console.error('Global fetch error:', e); }
}

async function fetchCoins() {
    try {
        const resp = await fetch(`${API_BASE}/api/v1/coins?api_key=phx_default&limit=250`);
        const data = await resp.json();
        allCoins = data.data || [];
        document.getElementById('coins-count').textContent = allCoins.length.toLocaleString();
        renderCoins();
        updateVolumeChart(allCoins.slice(0, 10));
    } catch(e) {
        document.getElementById('coins-tbody').innerHTML = '<tr><td colspan="9" class="error">Failed to load market data</td></tr>';
    }
}

function filterCoins() {
    const search = document.getElementById('search-symbol').value.toLowerCase();
    currentPage = 1;
    renderCoins();
}

function renderCoins() {
    const search = document.getElementById('search-symbol').value.toLowerCase();
    const sortBy = document.getElementById('sort-by').value;

    let filtered = allCoins;
    if (search) filtered = filtered.filter(c => c.name.toLowerCase().includes(search) || c.symbol.toLowerCase().includes(search) || c.id.toLowerCase().includes(search));

    filtered.sort((a, b) => {
        if (sortBy === 'market_cap') return (b.market_cap || 0) - (a.market_cap || 0);
        if (sortBy === 'volume') return (b.total_volume || 0) - (a.total_volume || 0);
        if (sortBy === 'price_change') return (b.price_change_percentage_24h || 0) - (a.price_change_percentage_24h || 0);
        if (sortBy === 'price') return (b.current_price || 0) - (a.current_price || 0);
        return 0;
    });

    const totalPages = Math.ceil(filtered.length / coinsPerPage);
    const start = (currentPage - 1) * coinsPerPage;
    const pageCoins = filtered.slice(start, start + coinsPerPage);

    const tbody = document.getElementById('coins-tbody');
    tbody.innerHTML = pageCoins.map((coin, i) => {
        const rank = start + i + 1;
        const img = coin.image ? `<img src="${coin.image}" class="coin-img" alt="">` : `<div class="coin-img" style="display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:var(--text-secondary)">${coin.symbol?.slice(0,2) || '?'}</div>`;
        const sourcesHtml = (coin.sources || []).slice(0, 4).map(s => `<span class="source-tag ${s === 'coingecko' ? 'active' : ''}">${s}</span>`).join('');
        const supply = coin.circulating_supply ? (coin.circulating_supply / 1e6).toFixed(2) + 'M' : '--';
        const maxSupply = coin.max_supply ? '/' + (coin.max_supply / 1e6).toFixed(2) + 'M' : '';

        return `<tr>
            <td style="color:var(--text-tertiary);font-weight:600">${rank}</td>
            <td><div class="coin-cell">${img}<div class="coin-info"><div class="coin-symbol">${coin.symbol}</div><div class="coin-name">${coin.name}</div></div></div></td>
            <td class="price">${formatPrice(coin.current_price)}</td>
            <td>${formatChange(coin.price_change_percentage_24h)}</td>
            <td>${formatChange(coin.price_change_percentage_7d_in_currency || coin.price_change_percentage_7d)}</td>
            <td class="mcap">${formatNumber(coin.market_cap, 0)}</td>
            <td class="volume">${formatNumber(coin.total_volume, 0)}</td>
            <td class="supply">${supply}${maxSupply}</td>
            <td><div class="source-tags">${sourcesHtml}</div></td>
        </tr>`;
    }).join('');

    // Pagination
    let pagesHtml = '';
    for (let i = 1; i <= Math.min(totalPages, 10); i++) {
        pagesHtml += `<button class="page-btn ${i === currentPage ? 'active' : ''}" onclick="goToPage(${i})">${i}</button>`;
    }
    document.getElementById('coins-pagination').innerHTML = pagesHtml;
}

function goToPage(page) { currentPage = page; renderCoins(); }

async function fetchExchanges() {
    try {
        const resp = await fetch(`${API_BASE}/api/v1/exchanges?api_key=phx_default&limit=100`);
        const data = await resp.json();
        const exchanges = data.data || [];
        document.getElementById('exchanges-count').textContent = exchanges.length;

        const tbody = document.getElementById('exchanges-tbody');
        tbody.innerHTML = exchanges.map((ex, i) => {
            const img = ex.image ? `<img src="${ex.image}" class="coin-img" alt="">` : `<div class="coin-img" style="display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700">${ex.name?.slice(0,2) || '?'}</div>`;
            const trustColor = ex.trust_score === 'A' ? 'var(--accent)' : ex.trust_score === 'B' ? 'var(--accent-orange)' : 'var(--text-tertiary)';
            return `<tr>
                <td style="color:var(--text-tertiary);font-weight:600">${i + 1}</td>
                <td><div class="coin-cell">${img}<div class="coin-info"><div class="coin-symbol">${ex.name}</div></div></div></td>
                <td class="volume">${(ex.trade_volume_24h_btc / 1e3).toFixed(2)}K BTC</td>
                <td class="volume">${(ex.trade_volume_24h_btc_normalized / 1e3).toFixed(2)}K BTC</td>
                <td><span style="color:${trustColor};font-weight:600">${ex.trust_score || '?'}</span></td>
                <td class="mcap">${ex.country || '--'}</td>
                <td class="mcap">${ex.year_established || '--'}</td>
            </tr>`;
        }).join('');
    } catch(e) {
        document.getElementById('exchanges-tbody').innerHTML = '<tr><td colspan="7" class="error">Failed to load exchange data</td></tr>';
    }
}

function updateDominanceChart(dominanceData) {
    const ctx = document.getElementById('dominanceChart').getContext('2d');
    const labels = Object.keys(dominanceData).slice(0, 5);
    const values = Object.values(dominanceData).slice(0, 5);
    const colors = ['#f7931a', '#627eea', '#00d4aa', '#ef4444', '#f59e0b'];

    if (dominanceChart) dominanceChart.destroy();
    dominanceChart = new Chart(ctx, {
        type: 'doughnut',
        data: { labels: labels.map(l => l.toUpperCase()), datasets: [{ data: values, backgroundColor: colors, borderWidth: 0 }] },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'right', labels: { color: '#8b92a8', font: { size: 11 }, boxWidth: 12 } } }, cutout: '65%' }
    });
}

function updateVolumeChart(topCoins) {
    const ctx = document.getElementById('volumeChart').getContext('2d');
    const labels = topCoins.map(c => c.symbol);
    const values = topCoins.map(c => c.total_volume || 0);

    if (volumeChart) volumeChart.destroy();
    volumeChart = new Chart(ctx, {
        type: 'bar',
        data: { labels: labels, datasets: [{ label: '24h Volume', data: values, backgroundColor: 'rgba(0,212,170,0.6)', borderColor: '#00d4aa', borderWidth: 1, borderRadius: 4 }] },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { x: { ticks: { color: '#5a6078', font: { size: 10 } }, grid: { display: false } }, y: { ticks: { color: '#5a6078', font: { size: 10 }, callback: function(v) { return (v/1e9).toFixed(0) + 'B'; } }, grid: { color: '#1e2330' } } } }
    });
}

function connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/live`);
    ws.onopen = () => { document.getElementById('ws-dot').classList.remove('offline'); document.getElementById('ws-text').textContent = 'Live Data'; };
    ws.onmessage = (e) => {
        const msg = JSON.parse(e.data);
        if (msg.type === 'update') { fetchCoins(); fetchGlobal(); }
    };
    ws.onclose = () => { document.getElementById('ws-dot').classList.add('offline'); document.getElementById('ws-text').textContent = 'Reconnecting...'; setTimeout(connectWebSocket, 3000); };
}

fetchGlobal(); fetchCoins(); connectWebSocket();
setInterval(() => { fetchGlobal(); fetchCoins(); }, 30000);
</script>
</body>
</html>"""

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
