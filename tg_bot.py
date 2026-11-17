"""
Lightweight Async Telegram Bot for Kick Master Panel
Runs inside the main asyncio event loop using pure aiohttp.
"""

import os
import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
from loguru import logger

from worker_client import WorkerNodeClient
from vision_engine import StreamVisionEngine
from comment_dispatcher import CommentDispatcher


class TelegramCommander:
    def __init__(
        self,
        bot_token: str,
        allowed_users: Optional[List[int]] = None,
        workers: Optional[List[WorkerNodeClient]] = None,
        vision_engine: Optional[StreamVisionEngine] = None,
        dispatcher: Optional[CommentDispatcher] = None,
        target_streamers: Optional[List[str]] = None,
    ):
        self.bot_token = bot_token
        self.allowed_users = set(allowed_users or [])
        self.workers = workers or []
        self.vision_engine = vision_engine
        self.dispatcher = dispatcher
        self.target_streamers = target_streamers or ["xyroplayzz"]
        self.api_base = f"https://api.telegram.org/bot{self.bot_token}"
        self.last_update_id = 0
        self.is_running = False
        self._task: Optional[asyncio.Task] = None

    def is_authorized(self, user_id: int) -> bool:
        if not self.allowed_users:
            return True
        return user_id in self.allowed_users

    async def send_message(self, chat_id: int, text: str, parse_mode: str = "HTML"):
        url = f"{self.api_base}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": parse_mode
        }
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json=payload, timeout=10) as r:
                    return await r.json()
        except Exception as e:
            logger.warning(f"[TelegramBot] Send message error: {e}")

    async def send_photo(self, chat_id: int, photo_path: str, caption: str = ""):
        if not os.path.exists(photo_path):
            await self.send_message(chat_id, caption)
            return

        url = f"{self.api_base}/sendPhoto"
        try:
            data = aiohttp.FormData()
            data.add_field("chat_id", str(chat_id))
            data.add_field("caption", caption)
            data.add_field("photo", open(photo_path, "rb"), filename="frame.jpg", content_type="image/jpeg")
            async with aiohttp.ClientSession() as s:
                async with s.post(url, data=data, timeout=20) as r:
                    return await r.json()
        except Exception as e:
            logger.warning(f"[TelegramBot] Send photo error: {e}")
            await self.send_message(chat_id, caption)

    async def handle_update(self, update: Dict[str, Any]):
        msg = update.get("message")
        if not msg:
            return

        chat_id = msg.get("chat", {}).get("id")
        user_id = msg.get("from", {}).get("id")
        text = msg.get("text", "").strip()

        if not text or not self.is_authorized(user_id):
            return

        cmd_parts = text.split(maxsplit=1)
        command = cmd_parts[0].lower()
        args = cmd_parts[1].strip() if len(cmd_parts) > 1 else ""

        if command in ["/start", "/help"]:
            await self.cmd_help(chat_id)
        elif command == "/status":
            await self.cmd_status(chat_id)
        elif command == "/points":
            await self.cmd_points(chat_id)
        elif command == "/vision":
            await self.cmd_vision(chat_id)
        elif command == "/say":
            await self.cmd_say(chat_id, args)
        elif command == "/autochat":
            await self.cmd_autochat(chat_id, args)
        elif command == "/restart":
            await self.cmd_restart(chat_id)

    async def cmd_help(self, chat_id: int):
        help_text = (
            "⚡ <b>Kick Fleet Master Commander</b>\n\n"
            "🎮 <b>Commands:</b>\n"
            "/status - Worker node RAM, active viewers & stream status\n"
            "/points - Channel points leaderboard & total\n"
            "/vision - Live frame capture & Gemini AI reaction generation\n"
            "/say &lt;message&gt; - Dispatch comment to Kick chat\n"
            "/autochat on|off - Toggle AI auto-comment dispatcher\n"
            "/restart - Restart remote worker miner via SSH\n"
            "/help - Show this guide"
        )
        await self.send_message(chat_id, help_text)

    async def cmd_status(self, chat_id: int):
        await self.send_message(chat_id, "⏳ <i>Checking worker fleet...</i>")

        worker_stats = []
        for w in self.workers:
            st = await w.fetch_status()
            worker_stats.append((w.name, st))

        total_acc = sum(st.get("total_accounts", 0) for _, st in worker_stats)
        total_viewers = sum(st.get("active_viewers", 0) for _, st in worker_stats)
        total_mem = sum(st.get("memory_mb", 0) for _, st in worker_stats)
        target = self.target_streamers[0] if self.target_streamers else "xyroplayzz"

        is_live_any = any(st.get("streamers", {}).get(target, {}).get("online") for _, st in worker_stats)
        is_live_str = "🟢 ONLINE" if is_live_any else "⚪ OFFLINE"
        auto_status = "🟢 ACTIVE" if (self.dispatcher and self.dispatcher.auto_dispatch) else "⚪ OFF"

        lines = [
            "🖥️ <b>Kick Fleet Status</b>\n",
            f"• <b>Total Fleet Accounts:</b> <code>{total_acc}</code>",
            f"• <b>Active Viewers:</b> <code>{total_viewers}</code>",
            f"• <b>Target ({target}):</b> {is_live_str}",
            f"• <b>Auto-Commenter:</b> {auto_status}",
            f"• <b>Total Fleet RAM:</b> <code>{total_mem:.1f} MB</code>\n",
            "<b>Worker Nodes:</b>"
        ]

        for name, st in worker_stats:
            st_str = "🟢" if st.get("status") == "online" else "🔴"
            acc_cnt = st.get("total_accounts", 0)
            mem = st.get("memory_mb", 0)
            lines.append(f" {st_str} <b>{name}:</b> {acc_cnt} accs | {mem} MB")

        await self.send_message(chat_id, "\n".join(lines))

    async def cmd_points(self, chat_id: int):
        if not self.workers:
            await self.send_message(chat_id, "No workers configured.")
            return

        all_accs = []
        for w in self.workers:
            try:
                accs = await w.fetch_accounts()
                all_accs.extend(accs)
            except Exception:
                pass

        if not all_accs:
            await self.send_message(chat_id, "No accounts data retrieved from workers.")
            return

        total_pts = sum(a.get("points", 0) for a in all_accs)
        sorted_accs = sorted(all_accs, key=lambda x: x.get("points", 0), reverse=True)

        lines = [f"🪙 <b>Total Fleet Points:</b> <code>{total_pts:,}</code> across {len(all_accs)} accounts\n<b>Top Accounts:</b>"]
        for a in sorted_accs[:12]:
            lines.append(f"• <code>{a.get('alias')}</code> ({a.get('node_name', 'node')}): {a.get('points', 0):,} pts")

        if len(sorted_accs) > 12:
            lines.append(f"<i>...and {len(sorted_accs) - 12} more accounts</i>")

        await self.send_message(chat_id, "\n".join(lines))

    async def cmd_vision(self, chat_id: int):
        if not self.vision_engine:
            await self.send_message(chat_id, "Vision engine not enabled.")
            return

        target = self.target_streamers[0] if self.target_streamers else "xyroplayzz"
        await self.send_message(chat_id, f"👁️ <i>Analyzing live stream for {target}...</i>")

        analysis = await self.vision_engine.analyze_live_stream(target)
        game = analysis.get("game", "Unknown")
        event = analysis.get("event", "No event")
        reactions = analysis.get("reactions", [])

        caption = (
            f"🎯 <b>Stream Vision: {target}</b>\n\n"
            f"🎮 <b>Game:</b> {game}\n"
            f"⚡ <b>Situation:</b> {event}\n\n"
            f"💬 <b>Reactions:</b>\n" + "\n".join(f"• <i>\"{r}\"</i>" for r in reactions)
        )

        frame_path = self.vision_engine.output_frame_path
        if os.path.exists(frame_path):
            await self.send_photo(chat_id, frame_path, caption=caption)
        else:
            await self.send_message(chat_id, caption)

    async def cmd_say(self, chat_id: int, message: str):
        if not message:
            await self.send_message(chat_id, "❌ Usage: <code>/say your message here</code>")
            return

        if not self.dispatcher:
            await self.send_message(chat_id, "❌ Dispatcher not available.")
            return

        target = self.target_streamers[0] if self.target_streamers else "xyroplayzz"
        res = await self.dispatcher.dispatch_single_comment(message, streamer=target)
        if res.get("success"):
            await self.send_message(chat_id, f"✅ Sent via <b>{res.get('account')}</b>: \"{message}\"")
        else:
            await self.send_message(chat_id, f"❌ Failed: {res.get('error', res.get('status'))}")

    async def cmd_autochat(self, chat_id: int, mode: str):
        if not self.dispatcher:
            await self.send_message(chat_id, "Dispatcher not available.")
            return

        mode = mode.lower().strip()
        if mode in ["on", "true", "start", "1"]:
            self.dispatcher.auto_dispatch = True
            if not self.dispatcher.is_running:
                self.dispatcher.start()
            await self.send_message(chat_id, "🤖 <b>Auto-Commenter ENABLED</b> (staggered ~45s)")
        elif mode in ["off", "false", "stop", "0"]:
            self.dispatcher.auto_dispatch = False
            await self.send_message(chat_id, "⏹️ <b>Auto-Commenter DISABLED</b>")
        else:
            await self.send_message(chat_id, "Usage: <code>/autochat on</code> or <code>/autochat off</code>")

    async def cmd_restart(self, chat_id: int):
        if not self.workers:
            await self.send_message(chat_id, "No workers configured.")
            return

        await self.send_message(chat_id, "🔄 <i>Restarting worker process on Serv00...</i>")
        res = await asyncio.to_thread(self.workers[0].restart_process_via_ssh)
        if res.get("success"):
            await self.send_message(chat_id, f"✅ {res.get('message')}")
        else:
            await self.send_message(chat_id, f"❌ Restart failed: {res.get('error')}")

    async def _poll_loop(self):
        logger.info("🤖 Telegram Commander started polling...")
        timeout = 25
        while self.is_running:
            try:
                url = f"{self.api_base}/getUpdates?offset={self.last_update_id + 1}&timeout={timeout}"
                async with aiohttp.ClientSession() as s:
                    async with s.get(url, timeout=timeout + 5) as r:
                        if r.status == 200:
                            data = await r.json()
                            updates = data.get("result", [])
                            for u in updates:
                                self.last_update_id = u["update_id"]
                                asyncio.create_task(self.handle_update(u))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[TelegramBot] Poll error: {e}")
                await asyncio.sleep(5)

    def start(self):
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._poll_loop())

    def stop(self):
        self.is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
