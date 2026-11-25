"""
Master Cross-Betting & Liquidity Funneling Engine for Kick Master Panel
Orchestrates predictions across all distributed worker miner nodes.
Coordinates point funneling into Hero accounts (dynamically prioritizing account closest to redeem)
and executing delta-neutral / liquidity hedging across the entire fleet.
"""

import time
import json
import random
import asyncio
import logging
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
        self.hero_proxy = None
        self.hero_bet_pct = float(self.config.get("hero_bet_pct", 0.35))
        self.feeder_bet_pct = float(self.config.get("feeder_bet_pct", 0.50))
        self.max_hero_bet = int(self.config.get("max_hero_bet", 15000))
        self.max_feeder_bet = int(self.config.get("max_feeder_bet", 5000))
        self.min_consensus_pct = float(self.config.get("min_consensus_pct", 55.0))
        self.target_streamers = self.config.get("streamers", [
            "soulaman", "xyroplayzz", "hydraflick", "s8ul_sid", "klurge",
            "harshkhelraay", "soulzeref", "nonstopgaming", "pinkcess", "binks69",
            "sikhwarrior", "sherlock", "8bit_goldy", "mortal", "soymamazon",
            "zyrorewards", "barisg", "combatry", "cuffem", "adinross", "sandraskins"
        ])

        self.processed_predictions = set()
        self.active_prediction: Optional[Dict[str, Any]] = None
        self.bet_history: List[Dict[str, Any]] = []
        self._is_stopped = False

    def fetch_streamer_prediction(self, streamer: str) -> Optional[Dict[str, Any]]:
        """Fetch latest prediction payload directly from Kick API using curl_cffi (No proxy)."""
        url = f"https://kick.com/api/v2/channels/{streamer}/predictions/latest"
        from curl_cffi import requests
        try:
            sess = requests.Session(impersonate="chrome120")
            sess.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://kick.com",
                "Referer": f"https://kick.com/{streamer}"
            })
            resp = sess.get(url, timeout=5)
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

    def _resolve_standard_prediction(self, outcomes: List[Dict[str, Any]], total_voters: int):
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

        if consensus_pct >= self.min_consensus_pct and total_voters >= 5:
            return outcome_0, outcome_1, "consensus_funnel", consensus_pct
        else:
            return outcome_0, outcome_1, "balanced_arbitrage", consensus_pct

    def analyze_prediction(self, pred: Dict[str, Any], streamer: str) -> Optional[Dict[str, Any]]:
        outcomes = pred.get("outcomes", [])
        if len(outcomes) < 2:
            return None

        title = (pred.get("title") or "").strip()
        title_lower = title.lower()
        streamer_lower = streamer.lower()

        total_voters = sum((o.get("vote_count", 0) or o.get("voters_count", 0) or 0) for o in outcomes)
        total_points = sum((o.get("total_vote_amount", 0) or 0) for o in outcomes)
        is_controlled = (streamer_lower == "xyroplayzz")

        if is_controlled:
            out_0, out_1 = outcomes[0], outcomes[1]
            t0 = (out_0.get("title") or "").lower()
            t1 = (out_1.get("title") or "").lower()
            if "us" in t0 or "win" in t0 or "yes" in t0:
                hero_outcome, feeder_outcome = out_0, out_1
            elif "us" in t1 or "win" in t1 or "yes" in t1:
                hero_outcome, feeder_outcome = out_1, out_0
            else:
                hero_outcome, feeder_outcome = out_0, out_1
            strategy = "controlled_funnel"
            consensus_pct = 100.0

        elif any(br_s in streamer_lower for br_s in ["binks69", "nonstopgaming", "mortal", "s8ul_sid", "hydraflick"]) and \
             any(kw in title_lower for kw in ["win", "match", "chicken", "winner", "jeetenage", "we dip", "kill", "top 3", "top 5"]):
            # Battle Royale match outcomes: Streamers lose 80%+ of the time.
            # Hero bets the statistical winner ('no' / 'nahi' / 'lose' / 'har' / 'dip'), while chat bets 'yes'.
            out_no = None
            out_yes = None
            for o in outcomes:
                ot = (o.get("title") or "").lower()
                if any(neg in ot for neg in ["no", "nahi", "nah", "lose", "dip", "har", "out", "dead", "nhi"]):
                    out_no = o
                elif any(pos in ot for pos in ["yes", "haan", "win", "live", "chicken"]):
                    out_yes = o

            if out_no and out_yes:
                hero_outcome = out_no
                feeder_outcome = out_yes
                strategy = "statistical_edge"
                consensus_pct = 80.0
            else:
                hero_outcome, feeder_outcome, strategy, consensus_pct = self._resolve_standard_prediction(outcomes, total_voters)

        elif streamer_lower == "sherlock" or any(kw in title_lower for kw in ["coin", "flip", "heads", "tails", "bo3", "dice", "roll", "odd", "even", "aa", "bb", "lower side", "higher side"]):
            # 50/50 Coin Flip / Random Dice Gamble: CAPITAL PRESERVATION MODE!
            # Never risk 35% on a coin flip. Sort outcomes, Hero bets safe capped amount.
            sorted_outcomes = sorted(
                outcomes,
                key=lambda o: (
                    o.get("vote_count", 0) or o.get("voters_count", 0) or 0,
                    o.get("total_vote_amount", 0) or 0
                ),
                reverse=True
            )
            hero_outcome = sorted_outcomes[0]
            feeder_outcome = sorted_outcomes[1]
            strategy = "random_gamble"
            consensus_pct = 50.0

        else:
            hero_outcome, feeder_outcome, strategy, consensus_pct = self._resolve_standard_prediction(outcomes, total_voters)

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

        # Parallel fetch accounts across all workers
        all_accounts = []
        worker_acc_lists = await asyncio.gather(*[w.fetch_accounts() for w in self.workers], return_exceptions=True)
        for idx, accs in enumerate(worker_acc_lists):
            if isinstance(accs, list):
                w = self.workers[idx]
                for a in accs:
                    alias = a.get("alias")
                    pts = a.get("points_by_streamer", {}).get(streamer, 0)
                    tot = a.get("total_points", 0)
                    all_accounts.append({
                        "alias": alias,
                        "worker": w,
                        "points": pts if pts >= 10 else (max(10, min(500, int(tot * 0.1))) if tot > 0 else 50),
                        "actual_pts": pts
                    })

        if not all_accounts:
            logger.debug(f"[MasterCrossBet] No accounts found across workers for {streamer}")
            return

        logger.info(
            f"⚡ Master Cross-Bet Triggered on {streamer} for '{analysis['title']}' [{strategy}] ({len(all_accounts)} accounts available)..."
        )
        self.processed_predictions.add(pred_id)

        hero_aliases_set = set(self.hero_aliases)
        hero_accs = [a for a in all_accounts if a["alias"] in hero_aliases_set]
        other_accs = [a for a in all_accounts if a["alias"] not in hero_aliases_set]
        executed_bets = []

        # Sort Hero accounts by actual points descending: closest to redeem is prioritized!
        hero_accs = sorted(hero_accs, key=lambda a: a.get("actual_pts", 0), reverse=True)
        if hero_accs:
            top_hero = hero_accs[0]
            logger.info(
                f"🏆 Primary Target Hero (Closest to Redeem): [{top_hero['alias']}] with {top_hero.get('actual_pts', 0):,} pts on {streamer}"
            )

        # 1. Multi-Hero Bet Execution with Strategy-Aware Capital Protection
        for idx, h_acc in enumerate(hero_accs):
            h_alias = h_acc["alias"]
            balance = h_acc.get("actual_pts", 0)

            if strategy == "controlled_funnel":
                # 100% win certainty (xyroplayzz): Bet 35% of balance to maximize profit
                bet_pct = self.hero_bet_pct if idx == 0 else max(0.25, self.hero_bet_pct - 0.05)
                amt = max(10, min(self.max_hero_bet, int(balance * bet_pct))) if balance >= 10 else 10
            elif strategy == "statistical_edge":
                # High edge (BGMI streamer loss): Hero bets 15% (max 1,000 pts) on 'NO'
                bet_pct = 0.15 if idx == 0 else 0.10
                amt = max(10, min(1000, int(balance * bet_pct))) if balance >= 10 else 10
            elif strategy == "consensus_funnel":
                # Chat consensus (>75%): Hero bets 10% (max 750 pts)
                bet_pct = 0.10 if idx == 0 else 0.07
                amt = max(10, min(750, int(balance * bet_pct))) if balance >= 10 else 10
            elif strategy == "random_gamble":
                # 50/50 Coin Flip / Dice: CAPITAL PRESERVATION MODE!
                # STRICTLY CAP HERO BET to 3% of balance (max 150 pts, min 10 pts)
                # Never bleed thousands of points on a 50/50 coin flip!
                amt = max(10, min(150, int(balance * 0.03))) if balance >= 10 else 10
            else:
                # Balanced arbitrage: conservative 5% (max 250 pts)
                amt = max(10, min(250, int(balance * 0.05))) if balance >= 10 else 10

            logger.info(f"👑 Executing Hero [{h_alias}] bet on '{hero_title}' ({amt:,} pts)...")
            
            # 1a. Send bet via worker node
            res = await h_acc["worker"].send_bet(h_alias, streamer, hero_id, amt)
            if res.get("success"):
                logger.success(f"👑 Hero [{h_alias}] bet placed via worker node! Amount: {amt} pts")
                executed_bets.append({
                    "account": h_alias,
                    "worker": h_acc["worker"].name,
                    "role": "PRIMARY_HERO_COLLECTOR" if idx == 0 else "HERO_COLLECTOR",
                    "outcome": hero_title,
                    "amount": amt,
                    "status": "success"
                })
            else:
                # 1b. Fallback to direct Kick API vote if worker call did not succeed
                h_token = self.hero_accounts_map.get(h_alias, {}).get("token") or (self.hero_token if h_alias == self.hero_alias else None)
                if h_token:
                    try:
                        from kick_prediction import KickPrediction
                        pred_client = KickPrediction(proxy=None)
                        ok, vote_res = await asyncio.to_thread(
                            pred_client.vote_prediction,
                            streamer, hero_id, amt, h_token
                        )
                        if ok:
                            logger.success(f"👑 Hero [{h_alias}] direct bet placed! Result: {vote_res}")
                            executed_bets.append({
                                "account": h_alias,
                                "worker": h_acc["worker"].name + " (Direct Kick API)",
                                "role": "PRIMARY_HERO_COLLECTOR" if idx == 0 else "HERO_COLLECTOR",
                                "outcome": hero_title,
                                "amount": amt,
                                "status": "success"
                            })
                    except Exception as e:
                        logger.error(f"Error placing direct Hero [{h_alias}] bet: {e}")

        # 2. Feeder Bets Execution (Dispatched concurrently in fast batches of 10 with 0.08s stagger)
        async def dispatch_single_feeder(idx: int, acc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            balance = acc.get("actual_pts", 0)
            alias = acc["alias"]

            if strategy == "controlled_funnel":
                # 100% feeders dump into losing side to pump Hero pool
                out_id = feeder_id
                out_title = feeder_title
                role = "FEEDER_DONOR"
                amt = max(10, min(self.max_feeder_bet, int(balance * self.feeder_bet_pct))) if balance >= 10 else 10

            elif strategy == "statistical_edge":
                # 80% feeders bet on emotional chat outcome (feeder_id / YES) to pump odds for Hero (NO)
                # 20% hedge on Hero side
                pick_hero_side = (idx % 5 == 0)
                out_id = hero_id if pick_hero_side else feeder_id
                out_title = hero_title if pick_hero_side else feeder_title
                role = "ARBITRAGE_HEDGE" if pick_hero_side else "ODDS_PUMP"
                amt = max(10, min(1000, int(balance * 0.25))) if balance >= 10 else 10

            elif strategy == "random_gamble":
                # 50/50 split across feeders: Fleet stays delta-neutral
                pick_hero_side = (idx % 2 == 0)
                out_id = hero_id if pick_hero_side else feeder_id
                out_title = hero_title if pick_hero_side else feeder_title
                role = "COINFLIP_HEDGE_HERO" if pick_hero_side else "COINFLIP_HEDGE_FEEDER"
                amt = max(10, min(250, int(balance * 0.15))) if balance >= 10 else 10

            elif strategy == "consensus_funnel":
                # Feeders dump into feeder_id to pump Hero payout
                out_id = feeder_id
                out_title = feeder_title
                role = "FEEDER_DONOR"
                amt = max(10, min(self.max_feeder_bet, int(balance * self.feeder_bet_pct))) if balance >= 10 else 10

            else:
                # Balanced arbitrage: 67% feeder outcome, 33% hero outcome
                pick_hero_side = (idx % 3 == 0)
                out_id = hero_id if pick_hero_side else feeder_id
                out_title = hero_title if pick_hero_side else feeder_title
                role = "ARBITRAGE_HEDGE" if pick_hero_side else "LIQUIDITY_DONOR"
                amt = max(10, min(500, int(balance * 0.20))) if balance >= 10 else 10

            try:
                res = await acc["worker"].send_bet(alias, streamer, out_id, amt)
                if res.get("success"):
                    return {
                        "account": alias,
                        "worker": acc["worker"].name,
                        "role": role,
                        "outcome": out_title,
                        "amount": amt,
                        "status": "success"
                    }
                elif res.get("skipped"):
                    logger.debug(f"Feeder [{alias}] skipped on {streamer}: insufficient points")
            except Exception as e:
                logger.debug(f"Feeder [{alias}] bet notice: {e}")
            return None

        batch_size = 10
        for i in range(0, len(other_accs), batch_size):
            chunk = other_accs[i : i + batch_size]
            tasks = [dispatch_single_feeder(i + idx, acc) for idx, acc in enumerate(chunk)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if isinstance(r, dict) and r.get("status") == "success":
                    executed_bets.append(r)
            await asyncio.sleep(0.08)

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

        analysis["auto_executed"] = True
        analysis["executed"] = True
        analysis["executed_bets_count"] = len(executed_bets)

        logger.success(f"🎉 Fleet Cross-Bet Complete! Dispatched {len(executed_bets)} bets on {streamer}!")

    async def check_single_streamer(self, streamer: str):
        """Polls a single streamer for active prediction and fires instant auto-bet."""
        try:
            raw_p = await asyncio.to_thread(self.fetch_streamer_prediction, streamer)
            pred_obj = self.parse_prediction(raw_p)
            if not pred_obj:
                return

            analysis = self.analyze_prediction(pred_obj, streamer)
            if not analysis:
                return

            pred_id = pred_obj["id"]
            if pred_id in self.processed_predictions:
                analysis["auto_executed"] = True
                analysis["executed"] = True
                if self.active_prediction and self.active_prediction.get("prediction_id") == pred_id:
                    analysis["executed_bets_count"] = self.active_prediction.get("executed_bets_count", 0)
                self.active_prediction = analysis
                return

            self.active_prediction = analysis

            remaining = pred_obj.get("remaining_seconds", 30)
            if remaining < 2:
                return

            # Mark processed immediately so no duplicate execution
            self.processed_predictions.add(pred_id)
            analysis["auto_executed"] = True

            logger.info(
                f"⚡ [InstantAutoBet] Triggering automatic fleet cross-bet for {streamer} on '{analysis.get('title')}' ({remaining}s remaining)..."
            )
            await self.execute_fleet_cross_bet(analysis)
        except Exception as e:
            logger.debug(f"[StreamerCheck] {streamer} notice: {e}")

    async def run_loop(self):
        """Central loop monitoring live predictions across streamers concurrently."""
        logger.info("🎲 Master Cross-Bet Engine active: monitoring fleet predictions concurrently")
        await asyncio.sleep(3)

        while not self._is_stopped:
            try:
                # Concurrent parallel check across all target streamers
                tasks = [self.check_single_streamer(s) for s in self.target_streamers]
                await asyncio.gather(*tasks, return_exceptions=True)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[MasterCrossBet] Loop error: {e}")

            await asyncio.sleep(3)
