"""
Comment Dispatcher for Kick Master Panel
Coordinates outgoing chat campaigns with human-like jitter and multi-account rotation.
"""

import asyncio
import random
import time
from typing import List, Dict, Any, Optional
from loguru import logger
from worker_client import WorkerNodeClient


class CommentDispatcher:
    def __init__(self, workers: List[WorkerNodeClient]):
        self.workers = workers
        self.comment_queue: List[str] = []
        self.history: List[Dict[str, Any]] = []
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self.current_streamer = "xyroplayzz"
        self.auto_dispatch = False
        self.min_interval = 35
        self.max_interval = 70

    def add_reactions(self, reactions: List[str]):
        """Adds freshly generated reactions to the dispatch queue."""
        for r in reactions:
            if r and r not in self.comment_queue:
                self.comment_queue.append(r.strip())
        logger.info(f"[CommentDispatcher] Added {len(reactions)} reactions to queue. Total in queue: {len(self.comment_queue)}")

    async def dispatch_single_comment(self, comment: str, streamer: str = "xyroplayzz", account_alias: Optional[str] = None) -> Dict[str, Any]:
        """Dispatches a single comment immediately through the appropriate worker node."""
        target_worker = None
        if account_alias:
            # Find the worker holding this account
            for w in self.workers:
                if w.enabled and any(a.get("alias") == account_alias for a in w.last_accounts):
                    target_worker = w
                    break

        # If no account specified or account not cached, pick an online worker randomly
        if not target_worker:
            online_workers = [w for w in self.workers if w.enabled and w.last_status.get("status") == "online"]
            if not online_workers:
                online_workers = [w for w in self.workers if w.enabled]
            if online_workers:
                target_worker = random.choice(online_workers)

        if not target_worker:
            return {"success": False, "error": "No active worker node available"}

        res = await target_worker.send_chat_message(comment, streamer=streamer, account_alias=account_alias)
        log_entry = {
            "timestamp": time.strftime("%H:%M:%S"),
            "account": res.get("account", account_alias or "auto"),
            "streamer": streamer,
            "message": comment,
            "success": res.get("success", False),
            "status": res.get("status", res.get("error", "Unknown"))
        }
        self.history.insert(0, log_entry)
        if len(self.history) > 100:
            self.history.pop()

        if res.get("success"):
            logger.success(f"[CommentDispatcher] Sent via {log_entry['account']}: \"{comment}\"")
        else:
            logger.warning(f"[CommentDispatcher] Failed sending via {log_entry['account']}: {log_entry['status']}")

        return res

    async def _auto_dispatch_loop(self):
        """Continuously pops comments from the queue and sends them with human jitter."""
        while self.is_running:
            try:
                if self.auto_dispatch and self.comment_queue:
                    comment = self.comment_queue.pop(0)
                    await self.dispatch_single_comment(comment, streamer=self.current_streamer)

                # Randomized human delay between comments
                interval = random.uniform(self.min_interval, self.max_interval)
                await asyncio.sleep(interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[CommentDispatcher] Dispatch loop error: {e}")
                await asyncio.sleep(10)

    def start(self):
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._auto_dispatch_loop())
            logger.info("🤖 Auto-Comment Dispatcher STARTED")

    def stop(self):
        self.is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
        logger.info("⏹️ Auto-Comment Dispatcher STOPPED")
