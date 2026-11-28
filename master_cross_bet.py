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
        self.first_seen_predictions: Dict[str, float] = {}
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
        elapsed_secs: Optional[int] = None

        created_at_str = pred.get("created_at")
        if created_at_str:
            try:
                dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
                elapsed = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())
                elapsed_secs = int(elapsed)
                remaining = max(0, int(duration - elapsed))
            except Exception:
                pass

        return {
            "id": pred_id,
            "title": pred.get("title", "Prediction"),
            "state": state,
            "duration": duration,
            "elapsed_seconds": elapsed_secs,
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

        if consensus_pct >= self.min_consensus_pct and total_voters >= 10:
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

        elif any(br_s in streamer_lower for br_s in ["soulaman", "s8ul_sid", "binks69", "nonstopgaming", "mortal", "hydraflick"]) and \
             any(kw in title_lower for kw in ["win", "match", "chicken", "winner", "jeetenage", "we dip", "kill", "kills", "top 3", "top 4", "top 5", "top 6", "top 8", "wwcd", "booyah", "6/6", "#1"]) or \
             (any(br_s in streamer_lower for br_s in ["soulaman", "s8ul_sid", "binks69", "nonstopgaming", "mortal", "hydraflick"]) and (" cd" in f" {title_lower}" or title_lower.startswith("cd"))):
            # Battle Royale match / challenge outcomes: streamers/teams fail hard challenges ~80% of the time.
            # Hero bets the statistical winner ('no' / 'nahi' / 'lose' / 'har' / 'dip'), while emotional fans & feeders pump 'yes'.
            out_no = None
            out_yes = None
            for o in outcomes:
                ot = (o.get("title") or "").strip().lower()
                if any(neg == ot or neg in ot.split() for neg in ["no", "nahi", "nah", "lose", "dip", "har", "out", "dead", "nhi"]):
                    out_no = o
                elif any(pos == ot or pos in ot.split() for pos in ["yes", "haan", "win", "live", "chicken"]):
                    out_yes = o

            if out_no and out_yes:
                hero_outcome = out_no
                feeder_outcome = out_yes
                strategy = "statistical_edge"
                consensus_pct = 80.0
            else:
                hero_outcome, feeder_outcome, strategy, consensus_pct = self._resolve_standard_prediction(outcomes, total_voters)

        elif "8bit_goldy" in streamer_lower and any(kw in title_lower for kw in ["bank", "heist", "robbery", "sandy", "cop", "pd"]):
            # GTA RP Heist: Cops win / Jail is statistically heavily favored
            out_cop = None
            out_crim = None
            for o in outcomes:
                ot = (o.get("title") or "").strip().lower()
                if any(k in ot for k in ["cop", "jail", "pd win", "police"]):
                    out_cop = o
                else:
                    out_crim = o
            if out_cop and out_crim and len(outcomes) == 2:
                hero_outcome = out_cop
                feeder_outcome = out_crim
                strategy = "statistical_edge"
                consensus_pct = 75.0
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

        # Parallel fetch fresh accounts across all workers right before placing bets
        all_accounts = []
        worker_acc_lists = await asyncio.gather(*[w.fetch_accounts(force=True) for w in self.workers], return_exceptions=True)
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
        executed_bets: List[Dict[str, Any]] = []

        # 1. Single-Spearhead Hero Task (Only #1 Closest-to-Redeem Hero bets so 100% of Hero winnings go to ONE account!)
        async def dispatch_spearhead_hero() -> Optional[Dict[str, Any]]:
            if not hero_accs:
                return None
            top_hero = hero_accs[0]
            h_alias = top_hero["alias"]
            balance = top_hero.get("actual_pts", 0)
            logger.info(
                f"🏆 Single-Spearhead Hero (Closest to Redeem): [{h_alias}] with {balance:,} pts on {streamer} (other {max(0, len(hero_accs)-1)} Heroes hold safely)"
            )

            vp_upi_priority = streamer.lower() in {"harshkhelraay", "s8ul_sid", "hydraflick", "nonstopgaming", "soulzeref", "klurge", "binks69"}

            if strategy == "controlled_funnel":
                amt = max(10, min(self.max_hero_bet, int(balance * self.hero_bet_pct))) if balance >= 10 else 10
            elif strategy == "statistical_edge":
                pct = 0.30 if vp_upi_priority else 0.25
                cap = 4000 if vp_upi_priority else 3000
                amt = max(10, min(cap, int(balance * pct))) if balance >= 10 else 10
            elif strategy == "consensus_funnel":
                high_conf = analysis.get("consensus_pct", 55.0) >= 65.0 and analysis.get("total_voters", 0) >= 20
                if high_conf:
                    bet_pct = 0.25 if vp_upi_priority else 0.22
                    cap = 3500 if vp_upi_priority else 2500
                else:
                    bet_pct = 0.18 if vp_upi_priority else 0.15
                    cap = 1600 if vp_upi_priority else 1200
                amt = max(10, min(cap, int(balance * bet_pct))) if balance >= 10 else 10
            elif strategy == "random_gamble":
                # 50/50 Coin Flip / Dice: CAPITAL PRESERVATION MODE (3%, max 150 pts)
                amt = max(10, min(150, int(balance * 0.03))) if balance >= 10 else 10
            else:
                # Balanced arbitrage: conservative 5% (max 250 pts)
                amt = max(10, min(250, int(balance * 0.05))) if balance >= 10 else 10

            logger.info(f"👑 Executing Spearhead Hero [{h_alias}] bet on '{hero_title}' ({amt:,} pts)...")
            res = await top_hero["worker"].send_bet(h_alias, streamer, hero_id, amt)
            if res.get("success"):
                logger.success(f"👑 Spearhead [{h_alias}] bet placed via worker node! Amount: {amt} pts")
                return {
                    "account": h_alias,
                    "worker": top_hero["worker"].name,
                    "role": "PRIMARY_HERO_COLLECTOR",
                    "outcome": hero_title,
                    "amount": amt,
                    "status": "success"
                }

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
                        logger.success(f"👑 Spearhead [{h_alias}] direct bet placed! Result: {vote_res}")
                        return {
                            "account": h_alias,
                            "worker": top_hero["worker"].name + " (Direct Kick API)",
                            "role": "PRIMARY_HERO_COLLECTOR",
                            "outcome": hero_title,
                            "amount": amt,
                            "status": "success"
                        }
                except Exception as e:
                    logger.error(f"Error placing direct Spearhead [{h_alias}] bet: {e}")
            return None

        # 2. Prepare Feeder Batches Grouped Per Worker (All 7 miners fire concurrently via /api/bet/batch!)
        worker_batches: Dict[Any, List[Dict[str, Any]]] = {}
        feeder_meta: Dict[str, Dict[str, Any]] = {}

        for idx, acc in enumerate(other_accs):
            balance = acc.get("actual_pts", 0)
            alias = acc["alias"]
            if balance < 10:
                continue

            if strategy == "controlled_funnel":
                out_id, out_title, role = feeder_id, feeder_title, "FEEDER_DONOR"
                amt = max(10, min(self.max_feeder_bet, int(balance * self.feeder_bet_pct)))
            elif strategy == "statistical_edge":
                pick_hero_side = (idx % 5 == 0)
                out_id = hero_id if pick_hero_side else feeder_id
                out_title = hero_title if pick_hero_side else feeder_title
                role = "ARBITRAGE_HEDGE" if pick_hero_side else "ODDS_PUMP"
                amt = max(10, min(1000, int(balance * 0.25)))
            elif strategy == "random_gamble":
                pick_hero_side = (idx % 2 == 0)
                out_id = hero_id if pick_hero_side else feeder_id
                out_title = hero_title if pick_hero_side else feeder_title
                role = "COINFLIP_HEDGE_HERO" if pick_hero_side else "COINFLIP_HEDGE_FEEDER"
                amt = max(10, min(250, int(balance * 0.15)))
            elif strategy == "consensus_funnel":
                out_id, out_title, role = feeder_id, feeder_title, "FEEDER_DONOR"
                amt = max(10, min(self.max_feeder_bet, int(balance * self.feeder_bet_pct)))
            else:
                pick_hero_side = (idx % 3 == 0)
                out_id = hero_id if pick_hero_side else feeder_id
                out_title = hero_title if pick_hero_side else feeder_title
                role = "ARBITRAGE_HEDGE" if pick_hero_side else "LIQUIDITY_DONOR"
                amt = max(10, min(500, int(balance * 0.20)))

            w = acc["worker"]
            worker_batches.setdefault(w, []).append({
                "alias": alias,
                "streamer": streamer,
                "outcome_id": out_id,
                "amount": amt
            })
            feeder_meta[alias] = {"worker": w.name, "role": role, "outcome": out_title, "amount": amt}

        async def dispatch_worker_feeders(w: WorkerNodeClient, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            results = await w.send_bet_batch(items)
            ok_bets = []
            for r in results:
                if isinstance(r, dict) and r.get("success"):
                    al = r.get("alias")
                    meta = feeder_meta.get(al, {})
                    ok_bets.append({
                        "account": al,
                        "worker": meta.get("worker", w.name),
                        "role": meta.get("role", "FEEDER"),
                        "outcome": meta.get("outcome", feeder_title),
                        "amount": r.get("amount", meta.get("amount", 0)),
                        "status": "success"
                    })
            return ok_bets

        # Fire Spearhead Hero + all 7 miner batches simultaneously!
        t_start = time.time()
        all_tasks = [dispatch_spearhead_hero()] + [
            dispatch_worker_feeders(w, items) for w, items in worker_batches.items()
        ]
        gathered = await asyncio.gather(*all_tasks, return_exceptions=True)

        if isinstance(gathered[0], dict):
            executed_bets.append(gathered[0])
        for worker_res in gathered[1:]:
            if isinstance(worker_res, list):
                executed_bets.extend(worker_res)
        elapsed_sec = round(time.time() - t_start, 2)

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

        logger.success(f"🎉 Fleet Cross-Bet Complete in {elapsed_sec}s! Dispatched {len(executed_bets)} bets on {streamer}!")

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

            if pred_id not in self.first_seen_predictions:
                self.first_seen_predictions[pred_id] = time.time()
                logger.info(
                    f"👀 [PredictionDetected] {streamer}: '{analysis.get('title')}' (duration={pred_obj.get('duration')}s, remaining={pred_obj.get('remaining_seconds')}s, voters={analysis.get('total_voters')})"
                )
                if len(self.first_seen_predictions) > 200:
                    # Prune old keys safely
                    self.first_seen_predictions = {k: v for k, v in list(self.first_seen_predictions.items())[-100:]}

            remaining = pred_obj.get("remaining_seconds", 30)
            if remaining < 2:
                return

            duration = pred_obj.get("duration", 60)
            elapsed = pred_obj.get("elapsed_seconds")
            if elapsed is None:
                elapsed = int(time.time() - self.first_seen_predictions.get(pred_id, time.time()))

            voters = analysis.get("total_voters", 0)
            strategy = analysis.get("strategy")
            late_remaining_trigger = max(10, min(20, int(duration * 0.22)))
            max_wait_elapsed = 18 if duration <= 90 else 28

            # Wait briefly for real chat votes on standard predictions; fast <8s dispatch handles short 60s preds safely
            should_fire_now = (
                strategy in ("controlled_funnel", "statistical_edge", "random_gamble")
                or (voters >= 20 and strategy == "consensus_funnel")
                or (elapsed >= max_wait_elapsed)
                or (remaining <= late_remaining_trigger)
            )

            if not should_fire_now:
                analysis["waiting_for_consensus"] = True
                analysis["auto_executed"] = False
                self.active_prediction = analysis
                return

            analysis["waiting_for_consensus"] = False
            self.active_prediction = analysis

            # Mark processed immediately so no duplicate execution
            self.processed_predictions.add(pred_id)
            analysis["auto_executed"] = True

            logger.info(
                f"⚡ [SmartAutoBet] Triggering fleet cross-bet for {streamer} on '{analysis.get('title')}' [{strategy}] (voters={voters}, {remaining}s remaining)..."
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
