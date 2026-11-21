"""
Kick Master Panel - Main Orchestrator
Pure, high-performance channel points master control panel and cross-betting brain.
Zero vision / zero chat bloat. Manages all worker miner instances centrally.
"""

import os
import sys
import json
import asyncio
from loguru import logger

from worker_client import WorkerNodeClient
from master_cross_bet import MasterCrossBetEngine
from master_auto_redeem import MasterAutoRedeem
from panel_server import PanelServer

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


async def render_keepalive_pinger_task(workers: list, interval_seconds: int = 240):
    """Pings all Render worker endpoints every 4 minutes to prevent Render free-tier sleeping."""
    logger.info(f"💓 Render Keep-Alive active (pinging {len(workers)} workers every {interval_seconds}s)")
    await asyncio.sleep(15)
    import aiohttp
    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for w in workers:
                    try:
                        async with session.get(f"{w.api_url}/health") as resp:
                            logger.debug(f"[KeepAlive] Ping {w.name} -> {resp.status}")
                    except Exception as err:
                        logger.debug(f"[KeepAlive] Ping notice for {w.name}: {err}")
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"[KeepAlive] Loop notice: {e}")
            await asyncio.sleep(30)


async def main():
    logger.info("==================================================")
    logger.info("🚀 STARTING KICK MASTER CONTROL PANEL & CROSS-BET BRAIN")
    logger.info("==================================================")

    config = load_config()
    master_cfg = config.get("MasterPanel", {})
    workers_cfg = config.get("Workers", [])
    target_streamers = config.get("TargetStreamers", ["soulaman", "xyroplayzz"])
    cross_bet_cfg = config.get("CrossBet", {
        "enabled": True,
        "hero_alias": "4 valo 01",
        "hero_bet_pct": 0.30,
        "feeder_bet_pct": 0.50,
        "streamers": target_streamers
    })

    # 1. Initialize Worker Node Clients
    workers = [WorkerNodeClient(w) for w in workers_cfg if w.get("enabled", True)]
    logger.info(f"Initialized {len(workers)} worker client node(s)")

    # 2. Initialize Master Cross-Bet Engine
    cross_bet_engine = MasterCrossBetEngine(workers, config=cross_bet_cfg)
    cross_bet_task = asyncio.create_task(cross_bet_engine.run_loop())

    # 2.5 Initialize Master Auto-Redeem Engine
    auto_redeem_cfg = config.get("AutoRedeem", {})
    auto_redeem_engine = MasterAutoRedeem(auto_redeem_cfg)

    async def get_hero_points():
        for w in workers:
            accs = await w.fetch_accounts()
            for a in accs:
                if a.get("alias") == auto_redeem_cfg.get("hero_alias", "4 valo 01"):
                    st_map = a.get("points_by_streamer") or {}
                    return st_map.get(auto_redeem_cfg.get("target_streamer", "soulaman"), 0)
        return 0

    auto_redeem_task = asyncio.create_task(auto_redeem_engine.run_loop(get_hero_points))

    # 3. Initialize & Start Web Panel Server
    panel_port = int(os.environ.get("PORT", master_cfg.get("port", 8080)))
    panel_server = PanelServer(
        host=master_cfg.get("host", "0.0.0.0"),
        port=panel_port,
        workers=workers,
        cross_bet_engine=cross_bet_engine,
        auto_redeem_engine=auto_redeem_engine,
        target_streamers=target_streamers
    )
    await panel_server.start()

    # 4. Start Render Keep-Alive Loop
    keepalive_task = asyncio.create_task(render_keepalive_pinger_task(workers, interval_seconds=240))

    logger.success("✅ Master Panel fully operational!")
    logger.info(f"Dashboard: http://localhost:{panel_port}")
    logger.info("Press Ctrl+C to stop.")

    try:
        while True:
            await asyncio.sleep(3600)
    except (asyncio.CancelledError, KeyboardInterrupt):
        pass
    finally:
        logger.info("Shutting down Master Panel...")
        if keepalive_task:
            keepalive_task.cancel()
        if cross_bet_task:
            cross_bet_task.cancel()
        if auto_redeem_task:
            auto_redeem_task.cancel()
        await panel_server.stop()
        logger.info("Master Panel stopped cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
