"""
Worker Client for Kick Master Panel
Handles HTTP communication and SSH management for all Worker Miner nodes.
"""

import aiohttp
import asyncio
import paramiko
from typing import Dict, Any, Optional, List
from loguru import logger


class WorkerNodeClient:
    def __init__(self, node_config: Dict[str, Any]):
        self.node_id = node_config.get("id", "worker")
        self.name = node_config.get("name", "Worker")
        self.api_url = node_config.get("api_url", "http://127.0.0.1:5000").rstrip("/")
        self.ssh_host = node_config.get("ssh_host")
        self.ssh_user = node_config.get("ssh_user")
        self.ssh_pass = node_config.get("ssh_pass")
        self.remote_dir = node_config.get("remote_dir", "/home/Nandpatel/miner")
        self.enabled = node_config.get("enabled", True)

        self.last_status: Dict[str, Any] = {
            "status": "offline",
            "memory_mb": 0,
            "total_accounts": 0,
            "active_viewers": 0,
            "total_points": 0,
            "streamers": {}
        }
        self.last_accounts: List[Dict[str, Any]] = []

    async def fetch_status(self) -> Dict[str, Any]:
        """Queries the Worker API /status endpoint."""
        if not self.enabled:
            return {"status": "disabled", "node_id": self.node_id, "name": self.name}

        try:
            timeout = aiohttp.ClientTimeout(total=6)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"{self.api_url}/status") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.last_status = data
                        self.last_status["node_id"] = self.node_id
                        self.last_status["name"] = self.name
                        return self.last_status
        except Exception as e:
            logger.debug(f"[WorkerClient] {self.name} HTTP status error: {e}")

        # If HTTP fails, check if process is alive via SSH
        is_alive = await asyncio.to_thread(self._check_ssh_process_alive)
        self.last_status = {
            "node_id": self.node_id,
            "name": self.name,
            "status": "online (ssh only)" if is_alive else "offline",
            "memory_mb": 0,
            "total_accounts": 0,
            "active_viewers": 0,
            "total_points": 0,
            "streamers": {}
        }
        return self.last_status

    async def fetch_accounts(self) -> List[Dict[str, Any]]:
        """Queries the Worker API /accounts endpoint."""
        try:
            timeout = aiohttp.ClientTimeout(total=8)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"{self.api_url}/accounts") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.last_accounts = data.get("accounts", [])
                        return self.last_accounts
        except Exception:
            pass
        return self.last_accounts

    async def send_chat_message(self, message: str, streamer: str = "xyroplayzz", account_alias: Optional[str] = None) -> Dict[str, Any]:
        """Dispatches an outgoing chat message through this worker node."""
        payload = {
            "message": message,
            "streamer": streamer,
            "account": account_alias
        }
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f"{self.api_url}/chat", json=payload) as resp:
                    return await resp.json()
        except Exception as e:
            return {"success": False, "error": f"Failed reaching worker: {e}"}

    def _check_ssh_process_alive(self) -> bool:
        if not self.ssh_host or not self.ssh_user:
            return False
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ssh_host, username=self.ssh_user, password=self.ssh_pass, timeout=6)
            stdin, stdout, stderr = ssh.exec_command("pgrep -f 'python3 main.py'")
            out = stdout.read().decode().strip()
            ssh.close()
            return bool(out)
        except Exception:
            return False

    def restart_process_via_ssh(self) -> Dict[str, Any]:
        """Restarts the worker process over SSH."""
        if not self.ssh_host or not self.ssh_user:
            return {"success": False, "error": "No SSH credentials configured"}

        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ssh_host, username=self.ssh_user, password=self.ssh_pass, timeout=10)
            
            # Kill existing
            ssh.exec_command("pkill -9 -f 'python3 main.py'")
            # Launch in background
            cmd = f"cd {self.remote_dir} && nohup python3 main.py > miner.log 2>&1 &"
            ssh.exec_command(cmd)
            ssh.close()
            return {"success": True, "message": f"Worker '{self.name}' restarted successfully via SSH"}
        except Exception as e:
            return {"success": False, "error": str(e)}
