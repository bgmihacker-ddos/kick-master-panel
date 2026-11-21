"""
Master Auto-Redeem Engine for Kick Fleet Panel
Monitors channel point rewards on target streamers and auto-redeems for the Hero account
when target threshold is met.
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
        self.target_streamer = self.config.get("target_streamer", "soulaman")
        self.target_keywords = [
            k.lower() for k in self.config.get("target_keywords", ["badge", "jersey", "highlight", "vp", "valorant"])
        ]
        self.hero_alias = self.config.get("hero_alias", "4 valo 01")
        self.hero_token = self.config.get("hero_token")
        self.hero_proxy = self.config.get("hero_proxy")
        self.input_message = self.config.get("input_message", "Insta: _.nand._.patel._ | Discord: nandpatel2758")
        
        self.available_rewards: List[Dict[str, Any]] = []
        self.redemption_history: List[Dict[str, Any]] = []
        self.redeemed_cache: Dict[str, float] = {}
        self.cooldown_seconds = 43200  # 12 hours
        self._is_stopped = False

    def fetch_streamer_rewards(self, streamer: str) -> List[Dict[str, Any]]:
        url = f"https://kick.com/api/v2/channels/{streamer}/rewards"
        try:
            from curl_cffi import requests
            proxies = {"http": self.hero_proxy, "https": self.hero_proxy} if self.hero_proxy else None
            sess = requests.Session(impersonate="chrome120", proxies=proxies)
            resp = sess.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("data", [])
        except Exception as e:
            logger.debug(f"[AutoRedeem] Error fetching rewards for {streamer}: {e}")
        return []

    def is_target_reward(self, reward: Dict[str, Any]) -> bool:
        if not reward.get("is_enabled", True) or reward.get("is_paused", False):
            return False
        title = (reward.get("title") or "").lower()
        desc = (reward.get("description") or "").lower()
        text = f"{title} {desc}"
        return any(k in text for k in self.target_keywords)

    def execute_redeem(self, streamer: str, reward: Dict[str, Any]) -> Dict[str, Any]:
        if not self.hero_token:
            return {"status": "error", "message": "Hero token not configured"}

        reward_id = reward.get("id")
        reward_title = reward.get("title", "Reward")
        cost = reward.get("cost", 0)

        cache_key = f"{streamer}:{reward_id}"
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
            proxies = {"http": self.hero_proxy, "https": self.hero_proxy} if self.hero_proxy else None
            sess = requests.Session(impersonate="chrome120", proxies=proxies)
            headers = {
                "Authorization": f"Bearer {self.hero_token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Origin": "https://kick.com",
                "Referer": f"https://kick.com/{streamer}",
            }
            resp = sess.post(url, json=payload, headers=headers, timeout=12)
            if resp.status_code in (200, 201):
                self.redeemed_cache[cache_key] = time.time()
                record = {
                    "streamer": streamer,
                    "reward_title": reward_title,
                    "cost": cost,
                    "message": msg,
                    "time": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "SUCCESS"
                }
                self.redemption_history.insert(0, record)
                logger.success(f"🎉 [AutoRedeem] SUCCESS! Redeemed '{reward_title}' ({cost:,} pts) on {streamer} for Hero {self.hero_alias}!")
                return {"status": "success", "reward": reward_title, "cost": cost}
            else:
                logger.warning(f"[AutoRedeem] HTTP {resp.status_code} redeeming '{reward_title}': {resp.text}")
                return {"status": "error", "code": resp.status_code, "message": resp.text}
        except Exception as e:
            logger.error(f"[AutoRedeem] Error redeeming '{reward_title}': {e}")
            return {"status": "error", "message": str(e)}

    async def check_hero_points_and_redeem(self, hero_points: int) -> List[Dict[str, Any]]:
        if not self.enabled or not self.hero_token:
            return []

        rewards = await asyncio.to_thread(self.fetch_streamer_rewards, self.target_streamer)
        self.available_rewards = rewards
        results = []

        for r in rewards:
            cost = r.get("cost", 0)
            if hero_points >= cost and self.is_target_reward(r):
                logger.info(f"🎁 [AutoRedeem] Hero [{self.hero_alias}] qualifies for '{r.get('title')}' ({cost:,} pts, balance: {hero_points:,})")
                res = await asyncio.to_thread(self.execute_redeem, self.target_streamer, r)
                results.append(res)
                if res.get("status") == "success":
                    hero_points -= cost
                    if hero_points <= 0:
                        break
        return results

    async def run_loop(self, get_hero_points_callback):
        """Periodic background check for available rewards."""
        logger.info(f"🎁 Auto-Redeem Engine active: monitoring rewards for '{self.target_streamer}'")
        await asyncio.sleep(5)
        while not self._is_stopped:
            try:
                # Always refresh rewards list
                rewards = await asyncio.to_thread(self.fetch_streamer_rewards, self.target_streamer)
                if rewards:
                    self.available_rewards = rewards

                hero_pts = await get_hero_points_callback()
                if hero_pts and hero_pts > 0:
                    await self.check_hero_points_and_redeem(hero_pts)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"[AutoRedeem] Loop notice: {e}")
            await asyncio.sleep(300)  # Check every 5 minutes
