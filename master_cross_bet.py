"""
Master Cross-Betting & Liquidity Funneling Engine for Kick Master Panel
Orchestrates predictions across all distributed worker miner nodes.
Coordinates point funneling into Hero account (4 valo 01) and delta-neutral hedging.
"""

import time
import json
import random
import asyncio
import logging
import urllib.request
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from loguru import logger

from worker_client import WorkerNodeClient


class MasterCrossBetEngine:
    def __init__(
        self,
        workers: List[WorkerNodeClient],
        config: Optional[Dict[str, Any]] = None,
        tg_bot: Optional[Any] = None
    ):
        self.workers = workers
        self.config = config or {}
        self.tg_bot = tg_bot

        self.enabled = self.config.get("enabled", True)
        self.hero_alias = self.config.get("hero_alias", "4 valo 01")
        self.hero_aliases = self.config.get("hero_aliases", [
            "Main Account", "second account", "3 nand 549", "4 valo 01", "5 raju",
            "npfc0912", "n9654", "n09144", "reward0912", "valo02"
        ])
        self.hero_accounts_map = self.config.get("hero_accounts", {})
        self.hero_token = self.config.get("hero_token")
        self.hero_proxy = self.config.get("hero_proxy")
        self.hero_bet_pct = float(self.config.get("hero_bet_pct", 0.30))
        self.feeder_bet_pct = float(self.config.get("feeder_bet_pct", 0.50))
        self.max_hero_bet = int(self.config.get("max_hero_bet", 15000))
        self.max_feeder_bet = int(self.config.get("max_feeder_bet", 5000))
        self.min_consensus_pct = float(self.config.get("min_consensus_pct", 55.0))
        self.target_streamers = self.config.get("streamers", ["soulaman", "xyroplayzz"])

        self.processed_predictions = set()
        self.active_prediction: Optional[Dict[str, Any]] = None
        self.bet_history: List[Dict[str, Any]] = []
        self._is_stopped = False

    def fetch_streamer_prediction(self, streamer: str) -> Optional[Dict[str, Any]]:
        """Fetch latest prediction payload directly from Kick API using curl_cffi."""
        url = f"https://kick.com/api/v2/channels/{streamer}/predictions"
        try:
            from curl_cffi import requests
            proxies = {"http": self.hero_proxy, "https": self.hero_proxy} if self.hero_proxy else None
            sess = requests.Session(impersonate="chrome120", proxies=proxies)
            resp = sess.get(url, timeout=8)
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            pass
        return None

    def parse_prediction(self, raw_data: Any) -> Optional[Dict[str, Any]]:
        if not raw_data:
            return None
        if isinstance(raw_data, list):
            raw_data = raw_data[0] if raw_data else {}
        if not isinstance(raw_data, dict):
            return None

        pred = raw_data.get("prediction") or raw_data.get("data", {}).get("prediction") or raw_data
        if not isinstance(pred, dict):
            return None

        pred_id = pred.get("id")
        if not pred_id:
            return None

        state = (pred.get("state") or pred.get("status") or "").upper()
        if state != "ACTIVE":
            return None

        outcomes = pred.get("outcomes", [])
        if len(outcomes) < 2:
            return None

        duration = int(pred.get("duration", 60) or 60)
        remaining = duration

        created_at_str = pred.get("created_at")
        if created_at_str:
            try:
                dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                elapsed = (datetime.now(timezone.utc) - dt).total_seconds()
                remaining = max(0, int(duration - elapsed))
            except Exception:
                pass

        return {
            "id": pred_id,
            "title": pred.get("title", "Prediction"),
            "state": state,
            "duration": duration,
            "remaining_seconds": remaining,
            "outcomes": outcomes,
            "raw": pred
        }

    def analyze_prediction(self, pred: Dict[str, Any], streamer: str) -> Optional[Dict[str, Any]]:
        outcomes = pred.get("outcomes", [])
        if len(outcomes) < 2:
            return None

        total_voters = sum((o.get("vote_count", 0) or o.get("voters_count", 0) or 0) for o in outcomes)
        total_points = sum((o.get("total_vote_amount", 0) or 0) for o in outcomes)
        is_controlled = (streamer.lower() == "xyroplayzz")

        if is_controlled:
            out_0, out_1 = outcomes[0], outcomes[1]
            t0 = (out_0.get("title") or "").lower()
            t1 = (out_1.get("title") or "").lower()
            if "us" in t0 or "win" in t0:
                hero_outcome, feeder_outcome = out_0, out_1
            elif "us" in t1 or "win" in t1:
                hero_outcome, feeder_outcome = out_1, out_0
            else:
                hero_outcome, feeder_outcome = out_0, out_1
            strategy = "controlled_funnel"
            consensus_pct = 100.0
        else:
            sorted_outcomes = sorted(
                outcomes,
                key=lambda o: (
                    o.get("vote_count", 0) or o.get("voters_count", 0) or 0,
                    o.get("total_vote_amount", 0) or 0
                ),
                reverse=True
            )
            outcome_0, outcome_1 = sorted_outcomes[0], sorted_outcomes[1]
            voters_0 = outcome_0.get("vote_count", 0) or outcome_0.get("voters_count", 0) or 0
            consensus_pct = (voters_0 / total_voters * 100.0) if total_voters > 0 else 50.0

            if consensus_pct >= self.min_consensus_pct:
                hero_outcome, feeder_outcome = outcome_0, outcome_1
                strategy = "consensus_funnel"
            else:
                hero_outcome, feeder_outcome = outcome_0, outcome_1
                strategy = "balanced_arbitrage"

        voters_0 = hero_outcome.get("vote_count", 0) or hero_outcome.get("voters_count", 0) or 0
        voters_1 = feeder_outcome.get("vote_count", 0) or feeder_outcome.get("voters_count", 0) or 0
        odds_0 = float(hero_outcome.get("return_rate", 0) or hero_outcome.get("multiplier", 0) or 1.0)
        odds_1 = float(feeder_outcome.get("return_rate", 0) or feeder_outcome.get("multiplier", 0) or 1.0)

        return {
            "prediction_id": pred["id"],
            "title": pred.get("title", ""),
            "streamer": streamer,
            "strategy": strategy,
            "hero_outcome_id": hero_outcome.get("id"),
            "hero_outcome_title": hero_outcome.get("title"),
            "hero_odds": odds_0,
            "feeder_outcome_id": feeder_outcome.get("id"),
            "feeder_outcome_title": feeder_outcome.get("title"),
            "feeder_odds": odds_1,
            "consensus_pct": consensus_pct,
            "is_controlled": is_controlled,
            "remaining_seconds": pred.get("remaining_seconds", 30),
            "total_points": total_points,
            "total_voters": total_voters
        }

    async def execute_fleet_cross_bet(self, analysis: Dict[str, Any]):
        """Dispatches cross-bets across all eligible accounts on all worker instances."""
        pred_id = analysis["prediction_id"]
        streamer = analysis["streamer"]
        strategy = analysis["strategy"]
        hero_id = analysis["hero_outcome_id"]
        hero_title = analysis["hero_outcome_title"]
        feeder_id = analysis["feeder_outcome_id"]
        feeder_title = analysis["feeder_outcome_title"]

        # Fetch accounts across all workers
        all_accounts = []
        for w in self.workers:
            accs = await w.fetch_accounts()
            for a in accs:
                pts = a.get("points_by_streamer", {}).get(streamer, 0)
                if pts >= 10:
                    all_accounts.append({
                        "alias": a.get("alias"),
                        "worker": w,
                        "points": pts
                    })

        if not all_accounts:
            logger.debug(f"[MasterCrossBet] No eligible accounts with points >= 10 on {streamer}")
            return

        logger.info(
            f"⚡ Master Cross-Bet Triggered on {streamer} for '{analysis['title']}' ({len(all_accounts)} accounts available)..."
        )
        self.processed_predictions.add(pred_id)

        hero_aliases_set = set(self.hero_aliases)
        hero_accs = [a for a in all_accounts if a["alias"] in hero_aliases_set]
        other_accs = [a for a in all_accounts if a["alias"] not in hero_aliases_set]
        executed_bets = []

        # 1. Multi-Hero Bet Execution (Top 10 Hero accounts bet on the winning side FIRST)
        for h_acc in hero_accs:
            amt = max(10, int(h_acc["points"] * self.hero_bet_pct))
            amt = min(amt, self.max_hero_bet)
            h_alias = h_acc["alias"]
            logger.info(f"👑 Executing Hero [{h_alias}] bet on '{hero_title}' ({amt:,} pts)...")
            h_token = self.hero_accounts_map.get(h_alias, {}).get("token") or (self.hero_token if h_alias == self.hero_alias else None)
            h_proxy = self.hero_accounts_map.get(h_alias, {}).get("proxy") or self.hero_proxy
            if h_token:
                try:
                    from kick_prediction import KickPrediction
                    pred_client = KickPrediction(proxy=h_proxy)
                    ok, res = await asyncio.to_thread(
                        pred_client.vote_prediction,
                        streamer, hero_id, amt, h_token
                    )
                    if ok:
                        logger.success(f"👑 Hero [{h_alias}] bet successfully placed! Result: {res}")
                        executed_bets.append({
                            "account": h_alias,
                            "worker": h_acc["worker"].name + " (Direct Kick API)",
                            "role": "HERO_COLLECTOR",
                            "outcome": hero_title,
                            "amount": amt,
                            "status": "success"
                        })
                    else:
                        logger.warning(f"Hero [{h_alias}] direct vote notice: {res}")
                except Exception as e:
                    logger.error(f"Error placing direct Hero [{h_alias}] bet: {e}")
            elif not getattr(h_acc["worker"], "readonly", False):
                res = await h_acc["worker"].send_bet(h_alias, streamer, hero_id, amt)
                if res.get("success"):
                    executed_bets.append({
                        "account": h_alias,
                        "worker": h_acc["worker"].name,
                        "role": "HERO_COLLECTOR",
                        "outcome": hero_title,
                        "amount": amt,
                        "status": "success"
                    })

        # 2. Feeder Bets Execution (Paced at 0.18s to prevent 429)
        for idx, acc in enumerate(other_accs):
            if getattr(acc["worker"], "readonly", False):
                continue
            balance = acc["points"]
            alias = acc["alias"]
            is_priority = any(p in alias.lower() for p in ["nand", "raju", "second"])

            if strategy == "balanced_arbitrage":
                pick_hero_side = (idx % 2 == 0)
                out_id = hero_id if pick_hero_side else feeder_id
                out_title = hero_title if pick_hero_side else feeder_title
                role = "ARBITRAGE_SIDE_A" if pick_hero_side else "ARBITRAGE_SIDE_B"
                amt = max(10, int(balance * 0.25))
            elif is_priority:
                out_id = hero_id
                out_title = hero_title
                role = "PRIORITY_ALLY"
                amt = max(10, int(balance * 0.25))
            else:
                out_id = feeder_id
                out_title = feeder_title
                role = "FEEDER_DONOR"
                amt = max(10, int(balance * self.feeder_bet_pct))
                amt = min(amt, self.max_feeder_bet)

            await asyncio.sleep(random.uniform(0.15, 0.25))
            res = await acc["worker"].send_bet(alias, streamer, out_id, amt)
            if res.get("success"):
                executed_bets.append({
                    "account": alias,
                    "worker": acc["worker"].name,
                    "role": role,
                    "outcome": out_title,
                    "amount": amt,
                    "status": "success"
                })

        # Record in history
        history_record = {
            "prediction_id": pred_id,
            "title": analysis["title"],
            "streamer": streamer,
            "strategy": strategy,
            "timestamp": datetime.now().strftime("%H:%M:%S"),
            "total_bets": len(executed_bets),
            "total_points": sum(b["amount"] for b in executed_bets),
            "bets": executed_bets
        }
        self.bet_history.insert(0, history_record)
        if len(self.bet_history) > 50:
            self.bet_history.pop()

        logger.success(f"🎉 Fleet Cross-Bet Complete! Dispatched {len(executed_bets)} bets on {streamer}!")

    async def run_loop(self):
        """Central loop monitoring live predictions across streamers."""
        logger.info("🎲 Master Cross-Bet Engine active: monitoring fleet predictions")
        await asyncio.sleep(5)

        while not self._is_stopped:
            try:
                for streamer in self.target_streamers:
                    raw_p = await asyncio.to_thread(self.fetch_streamer_prediction, streamer)
                    pred_obj = self.parse_prediction(raw_p)
                    if not pred_obj:
                        continue

                    analysis = self.analyze_prediction(pred_obj, streamer)
                    if not analysis:
                        continue

                    self.active_prediction = analysis
                    pred_id = pred_obj["id"]
                    if pred_id in self.processed_predictions:
                        continue

                    remaining = pred_obj.get("remaining_seconds", 30)
                    is_controlled = (streamer.lower() == "xyroplayzz")

                    # Timing window:
                    # xyroplayzz: execute immediately if >= 2s
                    # public: execute between 3s and 30s
                    if is_controlled:
                        if remaining < 2:
                            continue
                    else:
                        if remaining > 30 or remaining < 3:
                            continue

                    await self.execute_fleet_cross_bet(analysis)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[MasterCrossBet] Loop error: {e}")

            await asyncio.sleep(5)
