from curl_cffi import requests
import json
import time
import random
from typing import Optional, Dict, Any, Tuple
from loguru import logger
from datetime import datetime, timezone


class KickPrediction:
    def __init__(self, proxy: Optional[str] = None):
        self.proxy = proxy
        proxies = None
        if proxy:
            proxies = {"http": proxy, "https": proxy}

        self.session = requests.Session(
            impersonate="chrome120",
            proxies=proxies,
        )

        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Origin": "https://kick.com",
            "Sec-Ch-Ua": (
                '"Chromium";v="120", "Google Chrome";v="120", '
                '"Not?A_Brand";v="99"'
            ),
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Connection": "keep-alive",
        })
        self._initialized = False

    def _ensure_session(self):
        if self._initialized:
            return
        try:
            resp = self.session.get("https://kick.com", timeout=15)
            for name, value in {
                "showMatureContent": "true",
                "USER_LOCALE": "en",
            }.items():
                self.session.cookies.set(name, value, domain="kick.com")
            time.sleep(random.uniform(0.3, 0.8))
            self._initialized = True
        except Exception as e:
            logger.error(f"Prediction session init error: {e}")

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass

    def get_latest_prediction(self, channel_slug: str, token: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieves the latest prediction for the given channel.
        Returns a dict containing 'prediction' and optional 'user_vote', or None if failed/none.
        """
        self._ensure_session()
        headers = {
            "Referer": f"https://kick.com/{channel_slug}/",
            "Origin": "https://kick.com",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            resp = self.session.get(
                f"https://kick.com/api/v2/channels/{channel_slug}/predictions/latest",
                headers=headers,
                timeout=12,
            )

            if resp.status_code == 404:
                return None
            if resp.status_code != 200:
                logger.debug(f"Predictions latest [{channel_slug}] HTTP {resp.status_code}")
                return None

            data = json.loads(resp.content.decode("utf-8", errors="ignore"))
            inner = data.get("data")
            if isinstance(inner, dict):
                return inner
            return None

        except Exception as e:
            logger.error(f"Error getting latest prediction for {channel_slug}: {e}")
            return None

    def vote_prediction(
        self,
        channel_slug: str,
        outcome_id: str,
        amount: int,
        token: str,
    ) -> Tuple[bool, Any]:
        """
        Submits a prediction bet.
        """
        self._ensure_session()
        headers = {
            "Referer": f"https://kick.com/{channel_slug}/",
            "Origin": "https://kick.com",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        payload = {
            "amount": amount,
            "outcome_id": outcome_id,
        }

        try:
            resp = self.session.post(
                f"https://kick.com/api/v2/channels/{channel_slug}/predictions/vote",
                headers=headers,
                json=payload,
                timeout=15,
            )

            if resp.status_code in (200, 201):
                try:
                    res_data = json.loads(resp.content.decode("utf-8", errors="ignore"))
                    return True, res_data
                except Exception:
                    return True, resp.text

            raw_err = resp.content.decode("utf-8", errors="ignore")
            logger.warning(
                f"Prediction vote failed for {channel_slug} (HTTP {resp.status_code}): {raw_err}"
            )
            return False, raw_err

        except Exception as e:
            logger.error(f"Exception placing vote for {channel_slug}: {e}")
            return False, str(e)
