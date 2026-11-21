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

    async def fetch_status(self) -> Dict[str, Any]:
        """Queries the Worker health / status endpoint."""
        if not self.enabled:
            return {"status": "disabled", "node_id": self.node_id, "name": self.name}

        now = time.time()
        if (now - self._last_fetch_time) < 3.0 and self.last_status.get("status") == "online":
            return self.last_status

        # Try /api/accounts to compute live stats directly
        try:
            timeout = aiohttp.ClientTimeout(total=4.5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"{self.api_url}/api/accounts") as resp:
                    if resp.status == 200:
                        raw_accs = await resp.json()
                        accounts_list = raw_accs if isinstance(raw_accs, list) else raw_accs.get("accounts", [])
                        total_pts = 0
                        active_v = 0
                        streamers_seen = {}

                        for a in accounts_list:
                            st_map = a.get("streamers", {})
                            acc_is_watching = False
                            for s_name, s_info in st_map.items():
                                pts = s_info.get("points") or 0
                                total_pts += pts
                                if s_info.get("watching"):
                                    acc_is_watching = True
                                    streamers_seen[s_name] = True
                            if acc_is_watching or a.get("is_watching"):
                                active_v += 1

                        self.last_status = {
                            "status": "online",
                            "node_id": self.node_id,
                            "name": self.name,
                            "total_accounts": len(accounts_list),
                            "active_viewers": active_v,
                            "total_points": total_pts,
                            "streamers": streamers_seen,
                            "readonly": self.readonly
                        }
                        self._last_fetch_time = now
                        return self.last_status
        except Exception as e:
            logger.debug(f"[{self.name}] Status fetch notice: {e}")

        # Fallback to health ping
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

    async def fetch_accounts(self) -> List[Dict[str, Any]]:
        """Queries the Worker API /api/accounts endpoint and normalizes account data."""
        if not self.enabled:
            return []

        try:
            timeout = aiohttp.ClientTimeout(total=5.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
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
                        return self.last_accounts
        except Exception as e:
            logger.debug(f"[{self.name}] Failed to fetch accounts: {e}")

        return self.last_accounts

    async def send_bet(self, alias: str, streamer: str, outcome_id: str, amount: int) -> Dict[str, Any]:
        """Dispatches a prediction bet to this worker node."""
        if self.readonly:
            return {"success": False, "error": f"{self.name} is configured as read-only / untouched"}

        payload = {
            "alias": alias,
            "streamer": streamer,
            "outcome_id": outcome_id,
            "amount": amount
        }
        try:
            timeout = aiohttp.ClientTimeout(total=8.0)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f"{self.api_url}/api/bet", json=payload) as resp:
                    data = await resp.json()
                    return data
        except Exception as e:
            return {"success": False, "error": str(e)}
