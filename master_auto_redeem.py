"""
Master Auto-Redeem Engine for Kick Fleet Panel
Monitors channel point rewards on target streamers and auto-redeems for the Top 10 Hero accounts.
Sends instant Telegram notifications when points are ready to redeem and upon successful redemption.
"""

import os
import time
import json
import asyncio
import urllib.request
import urllib.error
from typing import Dict, List, Optional, Any
from loguru import logger

ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def generate_ulid() -> str:
    """Generate 26-character Crockford Base32 ULID."""
    timestamp = int(time.time() * 1000)
    rand_bytes = os.urandom(10)
    time_chars = [''] * 10
    for i in range(9, -1, -1):
        time_chars[i] = ENCODING[timestamp % 32]
        timestamp //= 32
    rand_chars = [''] * 16
    rand_int = int.from_bytes(rand_bytes, 'big')
    for i in range(15, -1, -1):
        rand_chars[i] = ENCODING[rand_int % 32]
        rand_int //= 32
    return ''.join(time_chars) + ''.join(rand_chars)


class MasterAutoRedeem:
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.enabled = self.config.get("enabled", True)
        self.auto_redeem_enabled = bool(self.config.get("auto_redeem_enabled", False))
        self.min_reward_cost = int(self.config.get("min_reward_cost", 10000))
        self.target_streamer = self.config.get("target_streamer", "soulaman")
        self.target_keywords = [
            k.lower() for k in self.config.get("target_keywords", [
                "badge", "jersey", "vp", "valorant", "gift", "card",
                "steam", "upi", "gpay", "uc", "bgmi", "code", "redeem", "reedem",
                "in game", "robux", "epin", "mouse", "keyboard", "earphone",
                "headset", "iem", "pc"
            ])
        ]
        
        # Multi-Hero accounts dictionary: alias -> {token, proxy}
        raw_heroes = self.config.get("hero_accounts", {})
        self.hero_accounts: Dict[str, Dict[str, Any]] = {}
        if isinstance(raw_heroes, dict):
            for alias, val in raw_heroes.items():
                if isinstance(val, dict):
                    self.hero_accounts[alias] = val
                elif isinstance(val, str):
                    self.hero_accounts[alias] = {"alias": alias, "token": val, "proxy": None}
        
        # Single fallback for legacy calls
        self.hero_alias = self.config.get("hero_alias", "4 valo 01")
        self.hero_token = self.config.get("hero_token")
        self.hero_proxy = self.config.get("hero_proxy")
        self.input_message = self.config.get("input_message", "Insta: _.nand._.patel._ | Discord: nandpatel2758")
        
        # Telegram notification config
        self.tg_bot_token = self.config.get("tg_bot_token", "9139027889:AAFqdc3lsAYDH-9MEByjZlIdR6-KGbaBRhw")
        self.tg_chat_id = self.config.get("tg_chat_id", "6181084198")

        self.available_rewards: List[Dict[str, Any]] = []
        self.rewards_cache: Dict[str, tuple] = {}
        self.rewards_cache_ttl = 1800  # 30 minutes per-streamer cache to avoid Kick 429
        self.redemption_history: List[Dict[str, Any]] = []
        self.redeemed_cache: Dict[str, float] = {}
        self.ready_alerted_cache: Dict[str, float] = {}
        self.cooldown_seconds = 43200  # 12 hours
        self.ready_alert_cooldown = 43200  # 12 hours per alert to avoid spam
        self._is_stopped = False

    def is_vp_or_upi_reward(self, reward: Dict[str, Any]) -> bool:
        title = (reward.get("title") or "").lower()
        desc = (reward.get("description") or "").lower()
        text = f"{title} {desc}"
        return any(k in text for k in ["vp", "valorant", "upi", "gpay", "paytm", "cash", "reedem", "redeem code"])

    def send_telegram_ready_alert(self, alias: str, streamer: str, reward_title: str, cost: int, current_pts: int, is_priority: bool = False):
        """Sends an instant alert to the Redeem Telegram bot when points are READY TO REDEEM."""
        if not self.tg_bot_token or not self.tg_chat_id:
            return

        header = "🔥 *TOP PRIORITY READY (VP / UPI CASH)!*" if is_priority else "🔔 *REWARD READY TO REDEEM! (Notify-Only)*"
        text = (
            f"{header}\n\n"
            f"👑 *Hero Account:* `{alias}`\n"
            f"📺 *Streamer:* `{streamer}`\n"
            f"🎁 *Qualified Reward:* {reward_title}\n"
            f"💰 *Reward Cost:* {cost:,} Points\n"
            f"💳 *Current Balance:* {current_pts:,} Points\n"
            f"ℹ️ *Status:* Auto-redeem disabled — redeem manually when ready.\n"
            f"⏰ *Time:* {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"
        )

        try:
            url = f"https://api.telegram.org/bot{self.tg_bot_token}/sendMessage"
            payload = json.dumps({
                "chat_id": self.tg_chat_id,
                "text": text,
                "parse_mode": "Markdown"
            }).encode("utf-8")
            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                logger.success(f"[AutoRedeem] 📱 Telegram READY alert sent for [{alias}] on {streamer} ({cost:,} pts)")
        except Exception as e:
            logger.error(f"[AutoRedeem] Failed to send Telegram READY alert: {e}")

    def send_telegram_notification(self, reward_title: str, cost: int, streamer: str, hero_alias: str, msg: str = ""):
        """Sends an instant receipt when a reward has been successfully redeemed."""
        if not self.tg_bot_token or not self.tg_chat_id:
            return

        text = (
            f"🎉 *KICK REWARD REDEEMED SUCCESSFULLY!*\n\n"
            f"🏆 *Reward:* {reward_title}\n"
            f"💰 *Cost:* {cost:,} Points\n"
            f"📺 *Streamer:* `{streamer}`\n"
            f"👑 *Hero Account:* `{hero_alias}`\n"
        )
        if msg:
            text += f"💬 *Contact / Message:* `{msg}`\n"
        text += f"⏰ *Time:* {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}"

        try:
            url = f"https://api.telegram.org/bot{self.tg_bot_token}/sendMessage"
            payload = json.dumps({
                "chat_id": self.tg_chat_id,
                "text": text,
                "parse_mode": "Markdown"
            }).encode("utf-8")
            req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                logger.success(f"[AutoRedeem] 📱 Telegram REDEEMED receipt sent to {self.tg_chat_id}")
        except Exception as e:
            logger.error(f"[AutoRedeem] Failed to send Telegram REDEEMED alert: {e}")

    def fetch_streamer_rewards(self, streamer: str, allow_network: bool = True) -> tuple:
        """Returns (rewards_list, hit_rate_limit) using a 30-minute per-streamer cache."""
        cached = self.rewards_cache.get(streamer)
        now = time.time()
        if cached and (now - cached[0]) < self.rewards_cache_ttl:
            return cached[1], False
        if not allow_network:
            return (cached[1] if cached else []), False

        url = f"https://kick.com/api/v2/channels/{streamer}/rewards"
        try:
            from curl_cffi import requests
            sess = requests.Session(impersonate="chrome120")
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json, text/plain, */*",
                "Origin": "https://kick.com",
                "Referer": f"https://kick.com/{streamer}/",
            }
            resp = sess.get(url, headers=headers, timeout=10)
            if resp.status_code == 429:
                logger.warning(f"[RewardNotifier] HTTP 429 rate-limited fetching rewards for {streamer}; pausing uncached fetches")
                return (cached[1] if cached else []), True
            if resp.status_code in (401, 403) and self.hero_token:
                headers["Authorization"] = f"Bearer {self.hero_token}"
                resp = sess.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                rews = (data.get("data", []) or []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                self.rewards_cache[streamer] = (now, rews)
                return rews, False
            else:
                logger.warning(f"[RewardNotifier] Rewards HTTP {resp.status_code} for {streamer}")
        except Exception as e:
            logger.warning(f"[RewardNotifier] Error fetching rewards for {streamer}: {e}")
        return (cached[1] if cached else []), False

    def is_target_reward(self, reward: Dict[str, Any]) -> bool:
        if not reward.get("is_enabled", True) or reward.get("is_paused", False):
            return False
        cost = int(reward.get("cost", 0) or 0)
        if cost < self.min_reward_cost:
            return False
        title = (reward.get("title") or "").lower()
        desc = (reward.get("description") or "").lower()
        text = f"{title} {desc}"
        return any(k in text for k in self.target_keywords)

    def execute_redeem(
        self,
        streamer: str,
        reward: Dict[str, Any],
        account_alias: Optional[str] = None,
        account_token: Optional[str] = None,
        account_proxy: Optional[str] = None
    ) -> Dict[str, Any]:
        alias = account_alias or self.hero_alias
        token = account_token or self.hero_token
        proxy = account_proxy or self.hero_proxy

        # Lookup in hero_accounts map if not explicitly passed
        if (not token or not alias) and alias in self.hero_accounts:
            token = self.hero_accounts[alias].get("token")
            proxy = self.hero_accounts[alias].get("proxy") or proxy

        if not token:
            return {"status": "error", "message": f"Token for [{alias}] not configured"}

        reward_id = reward.get("id")
        reward_title = reward.get("title", "Reward")
        cost = reward.get("cost", 0)

        cache_key = f"{streamer}:{reward_id}:{alias}"
        if time.time() - self.redeemed_cache.get(cache_key, 0) < self.cooldown_seconds:
            return {"status": "cooldown", "message": "Reward on cooldown"}

        url = f"https://kick.com/api/v2/channels/{streamer}/rewards/{reward_id}/redeem"
        ulid = generate_ulid()
        msg = self.input_message if reward.get("is_user_input_required", False) else ""

        payload = {
            "transaction_id": ulid,
            "message": msg
        }

        try:
            from curl_cffi import requests
            proxies = {"http": proxy, "https": proxy} if proxy else None
            sess = requests.Session(impersonate="chrome120", proxies=proxies)
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": "https://kick.com",
                "Referer": f"https://kick.com/{streamer}",
            }
            resp = sess.post(url, json=payload, headers=headers, timeout=12)
            if resp.status_code in (200, 201):
                self.redeemed_cache[cache_key] = time.time()
                record = {
                    "account": alias,
                    "streamer": streamer,
                    "reward_title": reward_title,
                    "cost": cost,
                    "message": msg,
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "SUCCESS"
                }
                self.redemption_history.insert(0, record)
                logger.success(f"🎉 [AutoRedeem] SUCCESS! Redeemed '{reward_title}' ({cost:,} pts) on {streamer} for [{alias}]!")
                
                # Send instant Telegram notification
                self.send_telegram_notification(reward_title, cost, streamer, alias, msg)
                
                return {"status": "success", "reward": reward_title, "cost": cost, "account": alias}
            else:
                logger.warning(f"[AutoRedeem] HTTP {resp.status_code} redeeming '{reward_title}' for [{alias}]: {resp.text}")
                return {"status": "error", "code": resp.status_code, "message": resp.text}
        except Exception as e:
            logger.error(f"[AutoRedeem] Error redeeming '{reward_title}' for [{alias}]: {e}")
            return {"status": "error", "message": str(e)}

    async def scan_and_redeem_all_heroes(self, heroes_points_map: Dict[str, Dict[str, int]], target_streamers: List[str]):
        """
        Scans all Hero accounts against all target streamers.
        Notify-Only mode by default: sends Telegram notifications to the Redeem bot when ready,
        without auto-spending points unless auto_redeem_enabled=True.
        """
        if not self.enabled:
            return

        collected_rewards: List[Dict[str, Any]] = []
        rate_limited = False
        for streamer in target_streamers:
            was_cached = streamer in self.rewards_cache and (time.time() - self.rewards_cache[streamer][0]) < self.rewards_cache_ttl
            rewards, hit_429 = await asyncio.to_thread(self.fetch_streamer_rewards, streamer, not rate_limited)
            if hit_429:
                rate_limited = True
            if not was_cached and not hit_429:
                await asyncio.sleep(1.5)
            if not rewards:
                continue

            target_rews = [r for r in rewards if self.is_target_reward(r)]
            for tr in target_rews:
                tr_copy = dict(tr)
                tr_copy["streamer"] = streamer
                tr_copy["is_priority_vp_upi"] = self.is_vp_or_upi_reward(tr)
                collected_rewards.append(tr_copy)

            if not target_rews:
                continue

            for r in target_rews:
                cost = r.get("cost", 0)
                reward_id = r.get("id")
                reward_title = r.get("title", "Reward")
                is_pri = self.is_vp_or_upi_reward(r)

                for alias, h_info in self.hero_accounts.items():
                    st_pts = heroes_points_map.get(alias, {}).get(streamer, 0)
                    if st_pts >= cost and cost > 0:
                        # 1. Check ready alert cooldown and notify Redeem Telegram bot
                        alert_key = f"ready:{alias}:{streamer}:{reward_id}"
                        last_alert = self.ready_alerted_cache.get(alert_key, 0)
                        if time.time() - last_alert > self.ready_alert_cooldown:
                            self.ready_alerted_cache[alert_key] = time.time()
                            logger.info(f"🔔 [RewardNotifier] {'PRIORITY VP/UPI ' if is_pri else ''}Points READY for [{alias}] on {streamer}: '{reward_title}' ({cost:,} pts, balance={st_pts:,})")
                            self.send_telegram_ready_alert(alias, streamer, reward_title, cost, st_pts, is_priority=is_pri)

                        # 2. Never auto-redeem unless explicitly enabled
                        if self.auto_redeem_enabled:
                            token = h_info.get("token")
                            proxy = h_info.get("proxy")
                            res = await asyncio.to_thread(
                                self.execute_redeem,
                                streamer, r, alias, token, proxy
                            )
                            if res.get("status") == "success":
                                heroes_points_map[alias][streamer] = max(0, st_pts - cost)

            await asyncio.sleep(0.05)

        if collected_rewards:
            # Put ALL Valorant VP & UPI Cash rewards strictly at the top, ordered by reachability (cost ascending)
            collected_rewards.sort(key=lambda rw: (0 if rw.get("is_priority_vp_upi") else 1, int(rw.get("cost", 0) or 0)))
            self.available_rewards = collected_rewards[:24]

    async def run_loop(self, get_heroes_data_callback, get_streamers_callback):
        """Periodic background loop scanning all 10 Heroes across all streamers."""
        mode_str = "AUTO-REDEEM" if self.auto_redeem_enabled else "NOTIFY-ONLY (No auto-redeem)"
        logger.info(f"🎁 Multi-Hero Reward Monitor active [{mode_str}]: scanning {len(self.hero_accounts)} Hero accounts")
        await asyncio.sleep(15)
        while not self._is_stopped:
            try:
                heroes_data = await get_heroes_data_callback()
                streamers = await get_streamers_callback()
                if heroes_data and streamers:
                    await self.scan_and_redeem_all_heroes(heroes_data, streamers)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[AutoRedeem] Scan loop notice: {e}")
            await asyncio.sleep(45 if not self.available_rewards else 300)  # Retry faster if cold boot hit 429
