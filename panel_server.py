"""
Kick Master Panel - Web Server & REST API
Built with aiohttp for low memory consumption and asynchronous execution.
"""

import os
import json
import asyncio
from typing import List, Dict, Any, Optional
from aiohttp import web
from loguru import logger

from worker_client import WorkerNodeClient
from vision_engine import StreamVisionEngine
from comment_dispatcher import CommentDispatcher


class PanelServer:
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        workers: Optional[List[WorkerNodeClient]] = None,
        vision_engine: Optional[StreamVisionEngine] = None,
        dispatcher: Optional[CommentDispatcher] = None,
        target_streamers: Optional[List[str]] = None,
    ):
        self.host = host
        self.port = port
        self.workers = workers or []
        self.vision_engine = vision_engine
        self.dispatcher = dispatcher
        self.target_streamers = target_streamers or ["xyroplayzz"]
        self.app = web.Application()
        self.runner: Optional[web.AppRunner] = None
        self._setup_routes()

    def _setup_routes(self):
        # HTML Page
        self.app.router.add_get("/", self.handle_index)

        # Static Assets
        static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")
        self.app.router.add_static("/static/", path=static_dir, name="static")

        # REST APIs
        self.app.router.add_get("/api/stats", self.handle_get_stats)
        self.app.router.add_get("/api/accounts", self.handle_get_accounts)
        self.app.router.add_get("/api/frame", self.handle_get_frame)
        self.app.router.add_post("/api/vision/trigger", self.handle_vision_trigger)
        self.app.router.add_post("/api/comment/send", self.handle_send_comment)
        self.app.router.add_post("/api/autocomment/toggle", self.handle_toggle_autocomment)
        self.app.router.add_post("/api/worker/gc", self.handle_worker_gc)
        self.app.router.add_post("/api/worker/restart", self.handle_worker_restart)

    async def handle_index(self, request: web.Request) -> web.Response:
        tpl_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "index.html")
        if os.path.exists(tpl_path):
            with open(tpl_path, "r", encoding="utf-8") as f:
                content = f.read()
            return web.Response(text=content, content_type="text/html")
        return web.Response(text="index.html not found", status=404)

    async def handle_get_stats(self, request: web.Request) -> web.Response:
        # Fetch all worker statuses concurrently in parallel
        tasks = [w.fetch_status() for w in self.workers]
        statuses = await asyncio.gather(*tasks, return_exceptions=True)
        worker_statuses = []
        for i, st in enumerate(statuses):
            if isinstance(st, dict):
                worker_statuses.append(st)
            else:
                worker_statuses.append(self.workers[i].last_status)

        target = self.target_streamers[0] if self.target_streamers else "xyroplayzz"
        vision_data = self.vision_engine.last_analysis if self.vision_engine else {}
        auto_disp = self.dispatcher.auto_dispatch if self.dispatcher else False
        queue_len = len(self.dispatcher.fleet_rotation_queue) if (self.dispatcher and hasattr(self.dispatcher, "fleet_rotation_queue")) else len(self.dispatcher.comment_queue if self.dispatcher else [])
        history = self.dispatcher.history if self.dispatcher else []

        return web.json_response({
            "workers": worker_statuses,
            "target_streamer": target,
            "vision": vision_data,
            "auto_dispatch": auto_disp,
            "queue_count": queue_len,
            "history": history
        })

    async def handle_get_accounts(self, request: web.Request) -> web.Response:
        tasks = [w.fetch_accounts() for w in self.workers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        all_accounts = []
        for accs in results:
            if isinstance(accs, list):
                all_accounts.extend(accs)
        return web.json_response({"accounts": all_accounts})

    async def handle_get_frame(self, request: web.Request) -> web.Response:
        frame_path = self.vision_engine.output_frame_path if self.vision_engine else "static/latest_frame.jpg"
        if os.path.exists(frame_path):
            try:
                with open(frame_path, "rb") as f:
                    data = f.read()
                return web.Response(body=data, content_type="image/jpeg")
            except Exception:
                pass

        # Return a fallback 1x1 black pixel GIF if frame does not exist
        fallback_pixel = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x01D\x00;"
        return web.Response(body=fallback_pixel, content_type="image/gif")

    async def handle_vision_trigger(self, request: web.Request) -> web.Response:
        if not self.vision_engine:
            return web.json_response({"success": False, "error": "Vision engine not configured"}, status=400)

        streamer = self.target_streamers[0] if self.target_streamers else "xyroplayzz"
        analysis = await self.vision_engine.analyze_live_stream(streamer)

        # If reactions returned and auto_dispatch is true, add to dispatcher
        if analysis.get("reactions") and self.dispatcher and self.dispatcher.auto_dispatch:
            self.dispatcher.add_reactions(analysis["reactions"])

        return web.json_response({
            "success": True,
            "analysis": analysis
        })

    async def handle_send_comment(self, request: web.Request) -> web.Response:
        if not self.dispatcher:
            return web.json_response({"success": False, "error": "Dispatcher not available"}, status=500)

        try:
            body = await request.json()
        except Exception:
            return web.json_response({"success": False, "error": "Invalid JSON"}, status=400)

        message = body.get("message", "").strip()
        account = body.get("account") or None
        streamer = body.get("streamer") or (self.target_streamers[0] if self.target_streamers else "xyroplayzz")

        if not message:
            return web.json_response({"success": False, "error": "Message is empty"}, status=400)

        res = await self.dispatcher.dispatch_single_comment(message, streamer=streamer, account_alias=account)
        return web.json_response(res)

    async def handle_toggle_autocomment(self, request: web.Request) -> web.Response:
        if not self.dispatcher:
            return web.json_response({"success": False, "error": "Dispatcher not available"}, status=500)

        try:
            body = await request.json()
            enabled = bool(body.get("enabled", False))
            self.dispatcher.auto_dispatch = enabled
            if enabled and not self.dispatcher.is_running:
                self.dispatcher.start()
            return web.json_response({"success": True, "auto_dispatch": enabled})
        except Exception as e:
            return web.json_response({"success": False, "error": str(e)}, status=400)

    async def handle_worker_gc(self, request: web.Request) -> web.Response:
        total_collected = 0
        import aiohttp
        for w in self.workers:
            if w.enabled:
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.post(f"{w.api_url}/gc", timeout=5) as r:
                            if r.status == 200:
                                d = await r.json()
                                total_collected += d.get("collected_objects", 0)
                except Exception:
                    pass
        return web.json_response({"success": True, "collected_objects": total_collected})

    async def handle_worker_restart(self, request: web.Request) -> web.Response:
        results = []
        for w in self.workers:
            if w.enabled:
                res = await asyncio.to_thread(w.restart_process_via_ssh)
                results.append(res)
        return web.json_response({"success": True, "results": results})

    async def start(self):
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()
        logger.success(f"🌐 Kick Master Web Panel running at http://{self.host}:{self.port}")

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()
