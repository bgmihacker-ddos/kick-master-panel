"""
Comment Dispatcher for Kick Master Panel
Centralized AI Auto-Chat Engine with Fleet-Wide Multi-Account Round-Robin Rotation.
Dispatches humanized comments evenly across all connected worker nodes.
"""

import asyncio
import random
import time
import re
from typing import List, Dict, Any, Optional, Tuple
from loguru import logger
from worker_client import WorkerNodeClient

# Safe universal comments for background loop (ultra-authentic, varied, human)
SAFE_BACKGROUND_COMMENTS = [
    # Stream Vibe & Quality
    "w stream",
    "chill stream aaj",
    "good vibes here",
    "audio ekdum crisp hai",
    "stream quality clean hai aaj",
    "mast chal raha hai stream",
    "bhai full chill stream",
    "cleaaan stream",
    "video bitrate mast aa raha hai",
    "audio thoda sa badha do bro",
    "frame drop toh nahi ho raha na aaj",
    "stream quality 10/10 hai",
    "clean setup bro",
    "bhai lag toh nahi hai na",
    "crisp audio",
    
    # Casual viewer greetings & chatter
    "hello bhai",
    "ram ram bhai",
    "kya haal hai bhai",
    "bhai chai pi li kya",
    "aaram se khelo",
    "bhai discord link pinned hai kya",
    "bgmi khelega baadme kya",
    "which game after this bro",
    "which server is this",
    "stream kitne baje tak chalega bhai",
    "are you solo or duo",
    "bhai match mast chal raha hai",
    "sahi khel rahe ho",
    "full focus mode on hai",
    "lets go chat",
    "kya scene chal raha hai",
    "koi na aaram se khelo bro",
    "bhai clutch time",
    "shabaash",
    "badhiya gameplay",
    "bhai chat padho thoda",
    "subah se stream kar rahe ho kya",
    "maza aa raha hai stream dekhne me",
    "kya baat hai bro",
    "bhai badhiya vibes hai",
    
    # In-game reactions
    "clean shot",
    "w aim bro",
    "sheeeesh",
    "clutch god",
    "op gameplay",
    "damnn that was clean",
    "crazy flick haha",
    "what a shot",
    "insane reflex bhai",
    "bro is locked in",
    "w gameplay",
    "bhai fire aim",
    "smooth spray control",
    "flick dekho yaar",
    "aim locked in hai aaj",
    "ez kill",
    "bhai god level gameplay",
    "nice spray",
    "kya timing thi",
    "crazy spray transfer",
    "god tier aim",
    
    # Micro neutral chat reactions
    "w",
    "gg",
    "ggs",
    "sheesh",
    "op",
    "hahaha",
    "lol",
    "lmaooo",
    "xd",
    "bruh",
    "arre re",
    "wait what",
    "nice one",
    "pog",
    "hyped",
    "bhai op",
    "nt nt",
    "lets gooo",
    "W stream",
    "GG"
]

def humanize_comment(text: str) -> str:
    """Transforms comments into natural live stream chatter."""
    if not text:
        return ""
    
    t = text.strip().strip('"').strip("'")
    t = re.sub(r'[\.;]+$', '', t).strip()
    
    words = t.split()
    processed_words = []
    for w in words:
        clean_w = re.sub(r'[^\w]', '', w.lower())
        if clean_w in {"w", "gg", "ggs", "op", "lmao", "nt"}:
            processed_words.append(clean_w.upper())
        elif clean_w in {"xd", "kekw"}:
            processed_words.append(clean_w)
        else:
            processed_words.append(w.lower())
    
    t = " ".join(processed_words)
    
    # Realistic letter stretching (e.g. bhai -> bhaiii)
    if random.random() < 0.25:
        stretch_map = {
            r'\bbhai\b': 'bhaiii',
            r'\byaar\b': 'yaarrr',
            r'\bsheesh\b': 'sheeeesh',
            r'\bclean\b': 'cleaaan',
            r'\blets go\b': 'lets gooo',
            r'\blol\b': 'lolll',
            r'\blmao\b': 'lmaooo',
            r'\bop\b': 'oppp',
            r'\bnice\b': 'niceee',
            r'\bclutch\b': 'clutchhh',
            r'\barre re\b': 'arre reee'
        }
        for pat, repl in stretch_map.items():
            if re.search(pat, t, re.IGNORECASE) and random.random() < 0.6:
                t = re.sub(pat, repl, t, flags=re.IGNORECASE)
                break

    # Subtle emote / reaction (18% chance)
    if random.random() < 0.18 and not any(em in t for em in ['🔥', '💀', '😂', '🫡', '❤️']):
        emojis = ['🔥', '💀', '😂', 'gg', 'xd', 'W']
        t = f"{t} {random.choice(emojis)}"
        
    return t


class CommentDispatcher:
    def __init__(self, workers: List[WorkerNodeClient], config: Optional[dict] = None):
        self.workers = workers
        cfg = config or {}
        
        self.comment_queue: List[str] = []
        self.history: List[Dict[str, Any]] = []
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        
        # Target streamer
        self.current_streamer = cfg.get("target_streamer", "xyroplayzz").lower().strip()
        self.auto_dispatch = cfg.get("enabled", True)
        
        # Increased chat rate (average ~50s)
        self.min_interval = int(cfg.get("min_interval_seconds", 35))
        self.max_interval = int(cfg.get("max_interval_seconds", 65))
        
        custom_comments = cfg.get("comments", [])
        self.comments: List[str] = list(custom_comments) if custom_comments else list(SAFE_BACKGROUND_COMMENTS)
        
        # Fleet-Wide Round-Robin Rotation State
        self.fleet_rotation_queue: List[Dict[str, Any]] = []  # [{"worker": WorkerNodeClient, "alias": str}]
        self.account_comment_counts: Dict[str, int] = {}       # {alias: total_sent}
        self.account_last_comment_time: Dict[str, float] = {}   # {alias: timestamp}
        self.recent_comments: List[str] = []
        
        self.total_sent = 0
        self.last_comment_time: float = 0.0
        self.next_interval: float = random.uniform(self.min_interval, self.max_interval)

    def add_reactions(self, reactions: List[str]):
        """Adds freshly generated reactions to the dispatch queue."""
        for r in reactions:
            if r and r not in self.comment_queue:
                self.comment_queue.append(r.strip())
        logger.info(f"[CommentDispatcher] Added {len(reactions)} reaction(s) to queue. Total: {len(self.comment_queue)}")

    async def refill_rotation_queue(self):
        """
        Polls accounts from all connected worker nodes and creates a global
        fair round-robin rotation queue across the entire fleet.
        """
        all_accounts = []
        for w in self.workers:
            if not w.enabled:
                continue
            accounts = await w.fetch_accounts()
            for acc in accounts:
                alias = acc.get("alias")
                if alias:
                    all_accounts.append({
                        "worker": w,
                        "alias": alias
                    })
        
        if not all_accounts:
            # Fallback to last known accounts if live fetch returned empty
            for w in self.workers:
                for acc in w.last_accounts:
                    alias = acc.get("alias")
                    if alias:
                        all_accounts.append({
                            "worker": w,
                            "alias": alias
                        })

        if all_accounts:
            # Sort by least commented accounts first, then shuffle within
            all_accounts.sort(key=lambda a: (
                self.account_comment_counts.get(a["alias"], 0),
                self.account_last_comment_time.get(a["alias"], 0)
            ))
            # Shuffle in chunks to randomize order while maintaining fair distribution
            random.shuffle(all_accounts)
            self.fleet_rotation_queue = all_accounts
            logger.info(f"[AutoChat] 🔄 Fleet Rotation Queue refilled with {len(self.fleet_rotation_queue)} accounts across {len(self.workers)} workers.")

    async def get_next_rotating_account(self) -> Tuple[Optional[WorkerNodeClient], Optional[str]]:
        """Picks the next account in fair round-robin order across all miners."""
        if not self.fleet_rotation_queue:
            await self.refill_rotation_queue()

        if not self.fleet_rotation_queue:
            # If still empty, pick any online worker with None account (worker picks random)
            online_workers = [w for w in self.workers if w.enabled and w.last_status.get("status") == "online"]
            if not online_workers:
                online_workers = [w for w in self.workers if w.enabled]
            return (random.choice(online_workers) if online_workers else None, None)

        item = self.fleet_rotation_queue.pop(0)
        return (item["worker"], item["alias"])

    async def is_streamer_online(self, streamer: str) -> bool:
        """Checks if the target streamer is currently live across workers or Kick API."""
        streamer_slug = streamer.lower().strip()
        # 1. Check cached worker statuses
        for w in self.workers:
            if not w.enabled:
                continue
            s_data = w.last_status.get("streamers", {}).get(streamer_slug)
            if s_data and s_data.get("online"):
                return True
        
        # 2. Check Kick API directly
        try:
            import aiohttp
            async with aiohttp.ClientSession() as s:
                async with s.get(f"https://kick.com/api/v2/channels/{streamer_slug}", timeout=6) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        livestream = data.get("livestream")
                        return bool(livestream and livestream.get("is_live", False))
        except Exception:
            pass

        return False

    async def dispatch_single_comment(self, comment: str, streamer: str = "xyroplayzz", account_alias: Optional[str] = None) -> Dict[str, Any]:
        """Dispatches a single comment immediately through the appropriate worker node."""
        target_worker = None
        if account_alias:
            for w in self.workers:
                if w.enabled and any(a.get("alias") == account_alias for a in w.last_accounts):
                    target_worker = w
                    break

        if not target_worker:
            target_worker, account_alias = await self.get_next_rotating_account()

        if not target_worker:
            return {"success": False, "error": "No active worker node available"}

        res = await target_worker.send_chat_message(comment, streamer=streamer, account_alias=account_alias)
        success = res.get("success", False)
        actual_account = res.get("account", account_alias or "auto")
        
        if success:
            self.total_sent += 1
            self.account_comment_counts[actual_account] = self.account_comment_counts.get(actual_account, 0) + 1
            self.account_last_comment_time[actual_account] = time.time()
            self.recent_comments.append(comment)
            if len(self.recent_comments) > 25:
                self.recent_comments.pop(0)

        log_entry = {
            "timestamp": time.strftime("%H:%M:%S"),
            "account": actual_account,
            "worker": target_worker.name,
            "streamer": streamer,
            "message": comment,
            "success": success,
            "status": res.get("status", res.get("error", "Sent" if success else "Failed"))
        }
        self.history.insert(0, log_entry)
        if len(self.history) > 100:
            self.history.pop()

        if success:
            logger.success(f"[CommentDispatcher] 💬 [{actual_account} via {target_worker.name}] -> {streamer}: \"{comment}\"")
        else:
            logger.warning(f"[CommentDispatcher] Failed via {actual_account} ({target_worker.name}): {log_entry['status']}")

        return res

    async def _auto_dispatch_loop(self):
        """Continuously dispatches comments with true multi-miner round-robin rotation."""
        # Initial delay on startup
        await asyncio.sleep(random.uniform(20, 45))
        await self.refill_rotation_queue()
        self.last_comment_time = time.time()
        self.next_interval = random.uniform(self.min_interval, self.max_interval)

        while self.is_running:
            try:
                await asyncio.sleep(5)
                if not self.auto_dispatch:
                    continue

                now = time.time()
                if (now - self.last_comment_time) < self.next_interval:
                    continue

                target = self.current_streamer
                
                # Check if streamer is live
                is_live = await self.is_streamer_online(target)
                if not is_live:
                    logger.debug(f"[AutoChat] Streamer '{target}' is offline. Standing by...")
                    await asyncio.sleep(25)
                    continue

                # Determine comment
                if self.comment_queue:
                    raw_comment = self.comment_queue.pop(0)
                else:
                    avail = [c for c in self.comments if c not in self.recent_comments[-15:]]
                    if not avail:
                        avail = self.comments
                    raw_comment = random.choice(avail)

                final_comment = humanize_comment(raw_comment)

                # Dispatch
                res = await self.dispatch_single_comment(final_comment, streamer=target)
                
                self.last_comment_time = time.time()
                self.next_interval = random.uniform(self.min_interval, self.max_interval)
                
                if res.get("success"):
                    logger.info(f"[AutoChat] ⏳ Next comment scheduled in {self.next_interval:.0f}s (Queue left: {len(self.fleet_rotation_queue)})")
                else:
                    # Retry sooner if failed
                    self.last_comment_time = time.time() - (self.next_interval - 15)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[CommentDispatcher] Dispatch loop error: {e}")
                await asyncio.sleep(15)

    def start(self):
        if not self.is_running:
            self.is_running = True
            self._task = asyncio.create_task(self._auto_dispatch_loop())
            logger.info(
                f"🤖 Central AutoChat STARTED for '{self.current_streamer}' "
                f"(Rate: {self.min_interval}-{self.max_interval}s, Fleet Round-Robin Rotation: ACTIVE, Pool: {len(self.comments)} comments)"
            )

    def stop(self):
        self.is_running = False
        if self._task and not self._task.done():
            self._task.cancel()
        logger.info("🛑 Central AutoChat STOPPED")
