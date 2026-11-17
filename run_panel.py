"""
Kick Master Panel - Main Orchestrator
Launches the Web Dashboard, AI Stream Vision Engine, Auto-Commenter, and Telegram Bot.
"""

import os
import sys
import json
import asyncio
import signal
from loguru import logger

from worker_client import WorkerNodeClient
from vision_engine import StreamVisionEngine
from comment_dispatcher import CommentDispatcher
from panel_server import PanelServer
from tg_bot import TelegramCommander


# Configure logger
logger.remove()
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
    level="INFO"
)
logger.add("master_panel.log", rotation="10 MB", retention="5 days", level="DEBUG")


def load_config() -> dict:
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
    if not os.path.exists(config_path):
        logger.error(f"Config file not found at {config_path}")
        sys.exit(1)
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


async def periodic_vision_task(vision_engine: StreamVisionEngine, dispatcher: CommentDispatcher, target_streamer: str, interval: int):
    """Background task that periodically captures and analyzes stream frames."""
    logger.info(f"🎯 Vision background monitor active for {target_streamer} (interval: {interval}s)")
    while True:
        try:
            await asyncio.sleep(interval)
            analysis = await vision_engine.analyze_live_stream(target_streamer)
            if analysis.get("is_live") and analysis.get("reactions"):
                if dispatcher.auto_dispatch:
                    dispatcher.add_reactions(analysis["reactions"])
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[VisionLoop] Monitor error: {e}")
            await asyncio.sleep(15)


async def render_keepalive_pinger_task(interval_seconds: int = 240):
    """Pings all Render worker endpoints and the master panel itself every 4 minutes to prevent Render free-tier sleep."""
    logger.info(f"💓 Render Keep-Alive active (pinging every {interval_seconds}s)")
    endpoints = [
        "https://miner-2-dl7d.onrender.com/health",
        "https://miner-3-1.onrender.com/health",
        "https://miner-4.onrender.com/health",
        "https://kick-master-panel.onrender.com/api/stats"
    ]
    await asyncio.sleep(15)
    import aiohttp
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                for ep in endpoints:
                    try:
                        async with session.get(ep, timeout=15) as resp:
                            logger.debug(f"[KeepAlive] Ping {ep} -> {resp.status}")
                    except Exception as err:
                        logger.debug(f"[KeepAlive] Ping notice for {ep}: {err}")
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"[KeepAlive] Loop notice: {e}")
            await asyncio.sleep(30)


async def main():
    logger.info("==================================================")
    logger.info("🚀 STARTING KICK MASTER CONTROL PANEL v2.0")
    logger.info("==================================================")

    config = load_config()

    master_cfg = config.get("MasterPanel", {})
    workers_cfg = config.get("Workers", [])
    vision_cfg = config.get("VisionAI", {})
    tg_cfg = config.get("Telegram", {})
    target_streamers = config.get("TargetStreamers", ["xyroplayzz"])
    primary_streamer = target_streamers[0] if target_streamers else "xyroplayzz"

    # 1. Initialize Worker Clients
    workers = [WorkerNodeClient(w) for w in workers_cfg]
    logger.info(f"Initialized {len(workers)} worker client node(s)")

    # 2. Initialize Central Fleet AutoChat Comment Dispatcher
    autochat_cfg = config.get("AutoChat", {})
    dispatcher = CommentDispatcher(workers, config=autochat_cfg)
    dispatcher.current_streamer = autochat_cfg.get("target_streamer", primary_streamer)
    dispatcher.auto_dispatch = autochat_cfg.get("enabled", True)
    dispatcher.min_interval = autochat_cfg.get("min_interval_seconds", 35)
    dispatcher.max_interval = autochat_cfg.get("max_interval_seconds", 65)
    if dispatcher.auto_dispatch:
        dispatcher.start()

    # 3. Initialize Vision Engine
    vision_engine = None
    vision_task = None
    if vision_cfg.get("enabled", True):
        api_key = vision_cfg.get("api_key", "")
        frame_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "latest_frame.jpg")
        vision_engine = StreamVisionEngine(api_key=api_key, output_frame_path=frame_path)
        v_interval = vision_cfg.get("interval_seconds", 90)
        vision_task = asyncio.create_task(
            periodic_vision_task(vision_engine, dispatcher, primary_streamer, v_interval)
        )

    # 4. Initialize Telegram Bot
    tg_bot = None
    if tg_cfg.get("enabled", False) and tg_cfg.get("bot_token"):
        tg_bot = TelegramCommander(
            bot_token=tg_cfg["bot_token"],
            allowed_users=tg_cfg.get("allowed_users", []),
            workers=workers,
            vision_engine=vision_engine,
            dispatcher=dispatcher,
            target_streamers=target_streamers
        )
        tg_bot.start()

    # 5. Initialize & Start Web Panel Server
    panel_server = PanelServer(
        host=master_cfg.get("host", "0.0.0.0"),
        port=int(os.environ.get("PORT", master_cfg.get("port", 8080))),
        workers=workers,
        vision_engine=vision_engine,
        dispatcher=dispatcher,
        target_streamers=target_streamers
    )
    await panel_server.start()

    # 6. Start Render Anti-Sleep Keep-Alive Loop (Every 4 mins)
    keepalive_task = asyncio.create_task(render_keepalive_pinger_task(interval_seconds=240))

    logger.success("✅ Master Panel fully operational!")
    logger.info(f"Dashboard: http://localhost:{master_cfg.get('port', 8080)}")
    logger.info("Press Ctrl+C to stop.")

    # Keep running until cancelled
    try:
        while True:
            await asyncio.sleep(3600)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        logger.info("Shutting down Master Panel...")
        if keepalive_task:
            keepalive_task.cancel()
        if vision_task:
            vision_task.cancel()
        if dispatcher:
            dispatcher.stop()
        if tg_bot:
            tg_bot.stop()
        await panel_server.stop()
        logger.info("Master Panel stopped cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
