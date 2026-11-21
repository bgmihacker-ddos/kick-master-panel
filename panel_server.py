"""
Kick Master Panel - Web Server & REST API
Clean, lightweight channel point miner master control panel.
Zero vision / zero chat bloat. Runs on Python aiohttp for low memory footprint (< 50MB).
"""

import os
import json
import asyncio
from typing import List, Dict, Any, Optional
from aiohttp import web
from loguru import logger

from worker_client import WorkerNodeClient
from master_cross_bet import MasterCrossBetEngine
from master_auto_redeem import MasterAutoRedeem


class PanelServer:
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        workers: Optional[List[WorkerNodeClient]] = None,
        cross_bet_engine: Optional[MasterCrossBetEngine] = None,
        auto_redeem_engine: Optional[MasterAutoRedeem] = None,
        target_streamers: Optional[List[str]] = None,
    ):
        self.host = host
        self.port = port
        self.workers = workers or []
        self.cross_bet_engine = cross_bet_engine
        self.auto_redeem_engine = auto_redeem_engine
        self.target_streamers = target_streamers or ["soulaman", "xyroplayzz"]
        self.app = web.Application()
        self.runner: Optional[web.AppRunner] = None
        self._setup_routes()

    def _setup_routes(self):
        # HTML Page
        self.app.router.add_get("/", self.handle_index)

        # Health
        self.app.router.add_get("/health", self.handle_health)

        # Static Assets
        static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
        if os.path.exists(static_dir):
            self.app.router.add_static("/static/", path=static_dir, name="static")

        # REST APIs
        self.app.router.add_get("/api/stats", self.handle_get_stats)
        self.app.router.add_get("/api/accounts", self.handle_get_accounts)
        self.app.router.add_get("/api/prediction", self.handle_get_prediction)
        self.app.router.add_post("/api/bet/trigger", self.handle_trigger_bet)
        self.app.router.add_post("/api/redeem/trigger", self.handle_trigger_redeem)
        self.app.router.add_post("/api/streamer/set", self.handle_set_streamer)

    async def handle_health(self, request: web.Request) -> web.Response:
        return web.Response(text="OK", content_type="text/plain")

    async def handle_index(self, request: web.Request) -> web.Response:
        tpl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")
        if os.path.exists(tpl_path):
            with open(tpl_path, "r", encoding="utf-8") as f:
                content = f.read()
            return web.Response(text=content, content_type="text/html")
        return web.Response(text="index.html not found", status=404)

    async def handle_get_stats(self, request: web.Request) -> web.Response:
        tasks = [w.fetch_status() for w in self.workers]
        statuses = await asyncio.gather(*tasks, return_exceptions=True)
        worker_statuses = []
        total_fleet_points = 0
        total_accounts = 0
        total_viewers = 0

        for i, st in enumerate(statuses):
            if isinstance(st, dict):
                worker_statuses.append(st)
                total_fleet_points += st.get("total_points", 0)
                total_accounts += st.get("total_accounts", 0)
                total_viewers += st.get("active_viewers", 0)
            else:
                worker_statuses.append(self.workers[i].last_status)

        active_pred = self.cross_bet_engine.active_prediction if self.cross_bet_engine else None
        history = self.cross_bet_engine.bet_history if self.cross_bet_engine else []
        auto_redeem_info = {
            "enabled": self.auto_redeem_engine.enabled if self.auto_redeem_engine else False,
            "target_streamer": self.auto_redeem_engine.target_streamer if self.auto_redeem_engine else "soulaman",
            "history": self.auto_redeem_engine.redemption_history if self.auto_redeem_engine else [],
            "available_rewards": self.auto_redeem_engine.available_rewards if self.auto_redeem_engine else []
        }

        return web.json_response({
            "workers": worker_statuses,
            "target_streamers": self.target_streamers,
            "total_fleet_points": total_fleet_points,
            "total_accounts": total_accounts,
            "total_viewers": total_viewers,
            "active_miners_count": sum(1 for w in worker_statuses if w.get("status") == "online"),
            "active_prediction": active_pred,
            "bet_history": history,
            "auto_redeem": auto_redeem_info
        })

    async def handle_get_accounts(self, request: web.Request) -> web.Response:
        tasks = [w.fetch_accounts() for w in self.workers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_accounts = []
        for accs in results:
            if isinstance(accs, list):
                all_accounts.extend(accs)
        return web.json_response({"accounts": all_accounts})

    async def handle_get_prediction(self, request: web.Request) -> web.Response:
        pred = self.cross_bet_engine.active_prediction if self.cross_bet_engine else None
        return web.json_response({"prediction": pred})

    async def handle_trigger_bet(self, request: web.Request) -> web.Response:
        if not self.cross_bet_engine or not self.cross_bet_engine.active_prediction:
            return web.json_response({"success": False, "error": "No active prediction detected"}, status=400)

        asyncio.create_task(
            self.cross_bet_engine.execute_fleet_cross_bet(self.cross_bet_engine.active_prediction)
        )
        return web.json_response({"success": True, "message": "Cross-bet execution initiated across fleet"})

    async def handle_trigger_redeem(self, request: web.Request) -> web.Response:
        if not self.auto_redeem_engine:
            return web.json_response({"success": False, "error": "Auto-redeem engine not initialized"}, status=400)
        try:
            body = await request.json()
            streamer = body.get("streamer") or self.auto_redeem_engine.target_streamer
            reward = body.get("reward")
            if not reward:
                return web.json_response({"success": False, "error": "Missing reward data"}, status=400)
            res = await asyncio.to_thread(self.auto_redeem_engine.execute_redeem, streamer, reward)
            return web.json_response({"success": True, "result": res})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def handle_set_streamer(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
            new_streamer = body.get("streamer")
            if new_streamer:
                self.target_streamers = [new_streamer]
                if self.cross_bet_engine:
                    self.cross_bet_engine.target_streamers = [new_streamer]
                return web.json_response({"success": True, "target_streamers": self.target_streamers})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=400)
        return web.json_response({"success": False, "error": "Missing streamer"}, status=400)

    async def start(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()
        logger.success(f"🌍 Kick Master Panel HTTP server listening on {self.host}:{self.port}")

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()
