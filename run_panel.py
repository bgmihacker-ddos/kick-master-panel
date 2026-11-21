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
        cfg = json.load(f)

    hero_tok = os.environ.get("HERO_TOKEN")
    hero_prx = os.environ.get("HERO_PROXY")
    if hero_tok:
        cfg.setdefault("CrossBet", {})["hero_token"] = hero_tok
        cfg.setdefault("AutoRedeem", {})["hero_token"] = hero_tok
    if hero_prx:
        cfg.setdefault("CrossBet", {})["hero_proxy"] = hero_prx
        cfg.setdefault("AutoRedeem", {})["hero_proxy"] = hero_prx

    return cfg


async def render_keepalive_pinger_task(workers: list, interval_seconds: int = 300):
    """
    Anti-Sleep Cross-Pinger:
    Every 5 minutes, pings all worker endpoints AND Master Panel itself
    to prevent Render's 15-minute free-tier inactivity sleep.
    """
    logger.info(f"💓 Render Anti-Sleep Mesh active (pinging {len(workers) + 1} endpoints every {interval_seconds}s)")
    await asyncio.sleep(15)
    import aiohttp

    self_url = os.environ.get("RENDER_EXTERNAL_URL") or "https://kick-master-panel-pwz6.onrender.com"

    while True:
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            headers = {"User-Agent": "Mozilla/5.0 KickAntiSleepMesh/1.0"}
            async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                # 1. Ping self to ensure master panel stays awake
                try:
                    async with session.get(f"{self_url.rstrip('/')}/health") as resp:
                        logger.debug(f"[AntiSleep] Self-ping master panel -> {resp.status}")
                except Exception as err:
                    logger.debug(f"[AntiSleep] Self-ping notice: {err}")

                # 2. Ping all miner nodes
                for w in workers:
                    try:
                        async with session.get(f"{w.api_url}/health") as resp:
                            logger.debug(f"[AntiSleep] Ping {w.name} -> {resp.status}")
                    except Exception as err:
                        logger.debug(f"[AntiSleep] Ping notice for {w.name}: {err}")

            logger.info(f"💓 [AntiSleep] Mesh pulse delivered to Master Panel & all {len(workers)} workers (Cluster Awake)")
            await asyncio.sleep(interval_seconds)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.debug(f"[AntiSleep] Loop notice: {e}")
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

    async def get_heroes_data():
        heroes_map = {}
        target_aliases = set(auto_redeem_cfg.get("hero_aliases", [auto_redeem_cfg.get("hero_alias", "4 valo 01")]))
        for w in workers:
            accs = await w.fetch_accounts()
            for a in accs:
                alias = a.get("alias")
                if alias in target_aliases:
                    st_map = a.get("points_by_streamer") or {}
                    heroes_map[alias] = st_map
        return heroes_map

    async def get_target_streamers():
        return config.get("TargetStreamers", ["soulaman"])

    auto_redeem_task = asyncio.create_task(auto_redeem_engine.run_loop(get_heroes_data, get_target_streamers))

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

    # 4. Start Render Anti-Sleep Cross-Pinger Mesh (every 5 minutes)
    keepalive_task = asyncio.create_task(render_keepalive_pinger_task(workers, interval_seconds=300))

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
