"""
Worker Client for Kick Master Panel
Handles high-performance HTTP communication for all Worker Miner nodes.
Features in-memory caching and non-blocking parallel querying.
"""

import aiohttp
import asyncio
import json
import time
from typing import Dict, Any, Optional, List
from loguru import logger


class WorkerNodeClient:
    def __init__(self, node_config: Dict[str, Any]):
        self.node_id = node_config.get("id", "worker")
        self.name = node_config.get("name", "Worker")
        self.api_url = node_config.get("api_url", "http://127.0.0.1:5000").rstrip("/")
        self.enabled = node_config.get("enabled", True)
        self.readonly = node_config.get("readonly", False)

        self.last_status: Dict[str, Any] = {
            "status": "offline",
            "memory_mb": 0,
            "total_accounts": 0,
            "active_viewers": 0,
            "total_points": 0,
            "streamers": {}
        }
        self.last_accounts: List[Dict[str, Any]] = []
        self._last_fetch_time: float = 0.0
        self.cache_ttl: float = 25.0
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self, timeout_sec: float = 10.0) -> aiohttp.ClientSession:
        """Reuses a persistent TCP/TLS session per worker for ultra-fast parallel requests."""
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=40)
            timeout = aiohttp.ClientTimeout(total=timeout_sec)
            self._session = aiohttp.ClientSession(connector=connector, timeout=timeout, headers={"Accept-Encoding": "gzip"})
        return self._session

    def _update_status_from_normalized(self, normalized: List[Dict[str, Any]]):
        total_pts = 0
        active_v = 0
        streamers_seen: Dict[str, bool] = {}

        for a in normalized:
            total_pts += a.get("total_points", 0)
            if a.get("is_watching"):
                active_v += 1
            for s_name in a.get("watching", []):
                streamers_seen[s_name] = True

        self.last_status = {
            "status": "online",
            "node_id": self.node_id,
            "name": self.name,
            "total_accounts": len(normalized),
            "active_viewers": active_v,
            "total_points": total_pts,
            "streamers": streamers_seen,
            "readonly": self.readonly
        }

    async def fetch_status(self) -> Dict[str, Any]:
        """Queries the Worker health / status endpoint using shared 25s accounts cache."""
        if not self.enabled:
            return {"status": "disabled", "node_id": self.node_id, "name": self.name}

        now = time.time()
        if (now - self._last_fetch_time) < self.cache_ttl and self.last_status.get("status") == "online":
            return self.last_status

        prev_ts = self._last_fetch_time
        accs = await self.fetch_accounts(force=True)
        if accs and self._last_fetch_time > prev_ts:
            return self.last_status

        # Fallback to lightweight health ping
        try:
            timeout = aiohttp.ClientTimeout(total=3.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"{self.api_url}/health") as resp:
                    if resp.status == 200:
                        self.last_status["status"] = "online"
                        self.last_status["node_id"] = self.node_id
                        self.last_status["name"] = self.name
                        self.last_status["readonly"] = self.readonly
                        return self.last_status
        except Exception:
            pass

        self.last_status["status"] = "offline"
        self.last_status["node_id"] = self.node_id
        self.last_status["name"] = self.name
        self.last_status["readonly"] = self.readonly
        return self.last_status

    async def fetch_accounts(self, force: bool = False) -> List[Dict[str, Any]]:
        """Queries the Worker API /api/accounts endpoint with shared 25s cache to save miner bandwidth."""
        if not self.enabled:
            return []

        now = time.time()
        if not force and self.last_accounts and (now - self._last_fetch_time) < self.cache_ttl:
            return self.last_accounts

        try:
            timeout = aiohttp.ClientTimeout(total=6.0)
            headers = {"Accept-Encoding": "gzip"}
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                url = f"{self.api_url}/api/accounts"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        raw_list = data if isinstance(data, list) else data.get("accounts", [])
                        normalized = []
                        for item in raw_list:
                            alias = item.get("alias", "Unknown")
                            st_map = item.get("streamers", {})
                            points_by_streamer = {}
                            watching_streamers = []
                            total_pts = 0

                            if isinstance(st_map, dict):
                                for s_name, s_info in st_map.items():
                                    pts = s_info.get("points") or 0
                                    points_by_streamer[s_name] = pts
                                    total_pts += pts
                                    if s_info.get("watching"):
                                        watching_streamers.append(s_name)
                            else:
                                total_pts = item.get("points", 0)

                            normalized.append({
                                "node_id": self.node_id,
                                "node_name": self.name,
                                "alias": alias,
                                "total_points": total_pts,
                                "points_by_streamer": points_by_streamer,
                                "watching": watching_streamers,
                                "is_watching": len(watching_streamers) > 0 or item.get("is_watching", False),
                                "proxy": bool(item.get("proxy")) and item.get("proxy") != "direct",
                                "readonly": self.readonly
                            })
                        self.last_accounts = normalized
                        self._update_status_from_normalized(normalized)
                        self._last_fetch_time = time.time()
                        return self.last_accounts
        except Exception as e:
            logger.debug(f"[{self.name}] Failed to fetch accounts: {e}")

        return self.last_accounts

    async def send_bet(self, alias: str, streamer: str, outcome_id: str, amount: int) -> Dict[str, Any]:
        """Dispatches a prediction bet to this worker node reusing persistent connection pool."""
        if self.readonly:
            return {"success": False, "error": f"{self.name} is configured as read-only / untouched"}

        payload = {
            "alias": alias,
            "streamer": streamer,
            "outcome_id": outcome_id,
            "amount": amount
        }
        try:
            session = await self._get_session()
            async with session.post(f"{self.api_url}/api/bet", json=payload, timeout=aiohttp.ClientTimeout(total=8.0)) as resp:
                data = await resp.json()
                return data
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def send_bet_batch(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Dispatches an entire batch of bets to a worker via /api/bet/batch, with parallel /api/bet fallback."""
        if not items:
            return []
        if self.readonly:
            return [{"alias": it["alias"], "success": False, "error": "readonly"} for it in items]

        try:
            session = await self._get_session()
            async with session.post(
                f"{self.api_url}/api/bet/batch",
                json={"bets": items},
                timeout=aiohttp.ClientTimeout(total=20.0),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if isinstance(data, dict) and isinstance(data.get("results"), list):
                        return data["results"]
        except Exception as e:
            logger.debug(f"[{self.name}] Batch bet endpoint fallback notice: {e}")

        # Fallback for workers without /api/bet/batch (e.g. untouched miner-2): fire concurrently via shared TCP pool
        async def _one(it: Dict[str, Any]) -> Dict[str, Any]:
            res = await self.send_bet(it["alias"], it["streamer"], it["outcome_id"], it["amount"])
            res.setdefault("alias", it["alias"])
            return res

        raw_results = await asyncio.gather(*[_one(it) for it in items], return_exceptions=True)
        out: List[Dict[str, Any]] = []
        for idx, r in enumerate(raw_results):
            if isinstance(r, dict):
                out.append(r)
            else:
                out.append({"alias": items[idx]["alias"], "success": False, "error": str(r)})
        return out
