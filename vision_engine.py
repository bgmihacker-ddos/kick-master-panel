"""
AI Stream Vision Engine for Kick Master Panel
Captures low-res frames from the live Kick HLS stream and analyzes them via Google Gemini API.
Generates contextual, high-engagement chat reactions tailored for Kick gaming streams.
"""

import os
import time
import json
import base64
import urllib.request
import subprocess
import asyncio
from typing import Dict, Any, List, Optional
from loguru import logger
from curl_cffi import requests


class StreamVisionEngine:
    def __init__(self, api_key: str, output_frame_path: str = "static/latest_frame.jpg"):
        self.api_key = api_key
        self.output_frame_path = output_frame_path
        self.last_analysis: Dict[str, Any] = {
            "timestamp": 0,
            "streamer": "None",
            "is_live": False,
            "game": "Unknown",
            "event": "Waiting for live stream...",
            "reactions": []
        }
        self.is_analyzing = False

    def get_playback_url(self, streamer_slug: str) -> Optional[str]:
        """Resolves the live HLS m3u8 playback URL from Kick's channel API."""
        slug = streamer_slug.lower().strip()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": f"https://kick.com/{slug}",
        }
        try:
            with requests.Session(impersonate="chrome120") as s:
                resp = s.get(f"https://kick.com/api/v2/channels/{slug}", headers=headers, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    livestream = data.get("livestream")
                    if livestream and livestream.get("is_live"):
                        return livestream.get("playback_url")
        except Exception as e:
            logger.debug(f"[VisionEngine] Could not fetch channel API for {slug}: {e}")
        return None

    def capture_stream_frame(self, playback_url: str) -> bool:
        """Grabs a single 640x360 frame from the HLS stream using ffmpeg."""
        os.makedirs(os.path.dirname(self.output_frame_path) or ".", exist_ok=True)
        cmd = [
            "ffmpeg", "-y",
            "-headers", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\r\nReferer: https://kick.com/\r\n",
            "-i", playback_url,
            "-frames:v", "1",
            "-vf", "scale=640:360",
            self.output_frame_path
        ]
        try:
            res = subprocess.run(cmd, capture_output=True, timeout=15)
            return res.returncode == 0 and os.path.exists(self.output_frame_path)
        except Exception as e:
            logger.warning(f"[VisionEngine] ffmpeg capture error: {e}")
            return False

    async def analyze_live_stream(self, streamer_slug: str) -> Dict[str, Any]:
        """Captures and analyzes the latest frame of the streamer."""
        if self.is_analyzing:
            return self.last_analysis

        self.is_analyzing = True
        try:
            playback_url = await asyncio.to_thread(self.get_playback_url, streamer_slug)
            if not playback_url:
                self.last_analysis = {
                    "timestamp": time.time(),
                    "streamer": streamer_slug,
                    "is_live": False,
                    "game": "Offline",
                    "event": f"Streamer {streamer_slug} is currently offline.",
                    "reactions": []
                }
                return self.last_analysis

            # Capture frame
            captured = await asyncio.to_thread(self.capture_stream_frame, playback_url)
            if not captured:
                self.last_analysis = {
                    "timestamp": time.time(),
                    "streamer": streamer_slug,
                    "is_live": True,
                    "game": "Stream Online (Frame Capture Failed)",
                    "event": "Stream is live, but could not capture frame via ffmpeg.",
                    "reactions": ["W stream", "lets gooo", "ggs", "sheesh"]
                }
                return self.last_analysis

            # Send to Gemini Vision
            analysis = await asyncio.to_thread(self._query_gemini_vision, self.output_frame_path)
            self.last_analysis = {
                "timestamp": time.time(),
                "streamer": streamer_slug,
                "is_live": True,
                "game": analysis.get("game", "Unknown Game"),
                "event": analysis.get("event", "Live gameplay in progress"),
                "reactions": analysis.get("reactions", [])
            }
            logger.success(f"[VisionEngine] Analyzed {streamer_slug}: Game={self.last_analysis['game']} | Generated {len(self.last_analysis['reactions'])} reactions")
            return self.last_analysis

        except Exception as e:
            logger.error(f"[VisionEngine] Analysis exception: {e}")
            return self.last_analysis
        finally:
            self.is_analyzing = False

    def _query_gemini_vision(self, image_path: str) -> Dict[str, Any]:
        """Calls Google Gemini Vision API to analyze the frame."""
        with open(image_path, "rb") as f:
            b64_img = base64.b64encode(f.read()).decode("utf-8")

        prompt = """You are an expert esports gaming spectator and Kick chat analyst.
Analyze this video frame from the Kick gaming livestream.

Return a valid JSON object with the following schema:
{
  "game": "Name of game (e.g. Valorant, BGMI, Apex Legends, GTA V, Just Chatting)",
  "event": "Brief description of the action (e.g. 1v2 clutch, sniping on A-site, buying weapons)",
  "reactions": [
    "1x Pure hype / clip praise (e.g. insane shot brooo, W aim)",
    "1x Natural Indian Gamer Slang / Hinglish (e.g. kya spray mara bhai, mast clutch)",
    "1x Gameplay / weapon observation (e.g. vandal tap clean tha, nice crosshair placement)",
    "1x Casual chat banter (e.g. lobby easy lag rahi aaj, senses on fire)",
    "1x Short emote / hype reaction (e.g. W, sheesh, GGs)"
  ]
}
Return ONLY pure JSON. No markdown backticks or explanation."""

        payload = {
            "contents": [
                {
                    "parts": [
                        {"text": prompt},
                        {
                            "inlineData": {
                                "mimeType": "image/jpeg",
                                "data": b64_img
                            }
                        }
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.7,
                "responseMimeType": "application/json"
            }
        }

        # Try gemini-2.0-flash first, fallback to gemini-1.5-flash
        models = ["gemini-2.0-flash", "gemini-1.5-flash"]
        for m in models:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{m}:generateContent?key={self.api_key}"
            try:
                req = urllib.request.Request(
                    url,
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=15) as resp:
                    res_json = json.loads(resp.read().decode("utf-8"))
                    text = res_json["candidates"][0]["content"]["parts"][0]["text"]
                    clean_text = text.replace("```json", "").replace("```", "").strip()
                    return json.loads(clean_text)
            except Exception as e:
                logger.debug(f"[VisionEngine] Gemini model {m} notice: {e}")

        # Fallback reactions if API unavailable
        return {
            "game": "Live Stream",
            "event": "Active gameplay",
            "reactions": ["W clutch bro", "insane spray control", "kya aim hai bhai", "sheesh W", "GGs"]
        }
