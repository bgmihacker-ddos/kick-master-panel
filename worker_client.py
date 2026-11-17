"""
Worker Client for Kick Master Panel
Handles high-performance HTTP communication and SSH failover for all Worker Miner nodes.
Features in-memory caching and non-blocking parallel querying.
"""

import aiohttp
import asyncio
import paramiko
import json
import time
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
        self._last_fetch_time: float = 0.0

    async def fetch_status(self) -> Dict[str, Any]:
        """Queries the Worker API /status endpoint with short cache and SSH fallback."""
        if not self.enabled:
            return {"status": "disabled", "node_id": self.node_id, "name": self.name}

        # Return cached status if queried within last 4 seconds
        now = time.time()
        if (now - self._last_fetch_time) < 4.0 and self.last_status.get("status") == "online":
            return self.last_status

        # 1. Try direct HTTP (tight 3s timeout)
        try:
            timeout = aiohttp.ClientTimeout(total=3.5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"{self.api_url}/status") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.last_status = data
                        self.last_status["node_id"] = self.node_id
                        self.last_status["name"] = self.name
                        self._last_fetch_time = now
                        return self.last_status
        except Exception:
            pass

        # 2. Try SSH fallback if configured (e.g. Serv00)
        if self.ssh_host and self.ssh_user:
            ssh_data = await asyncio.to_thread(self._fetch_status_via_ssh)
            if ssh_data:
                self.last_status = ssh_data
                self.last_status["node_id"] = self.node_id
                self.last_status["name"] = self.name
                self._last_fetch_time = now
                return self.last_status

        # Keep previous known values if alive previously to prevent UI flicker
        if self.last_status.get("total_accounts", 0) > 0:
            self.last_status["status"] = "online"
        else:
            self.last_status["status"] = "offline"

        return self.last_status

    def _fetch_status_via_ssh(self) -> Optional[Dict[str, Any]]:
        """Queries local worker API over SSH."""
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ssh_host, username=self.ssh_user, password=self.ssh_pass, timeout=4)
            # Local curl to worker port 9645
            stdin, stdout, stderr = ssh.exec_command("curl -s -m 2 http://127.0.0.1:9645/status")
            out = stdout.read().decode().strip()
            ssh.close()
            if out:
                return json.loads(out)
        except Exception:
            pass
        return None

    async def fetch_accounts(self) -> List[Dict[str, Any]]:
        """Queries the Worker API /accounts endpoint with SSH failover."""
        # 1. Try HTTP
        try:
            timeout = aiohttp.ClientTimeout(total=4)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"{self.api_url}/accounts") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        accounts = data.get("accounts", [])
                        for a in accounts:
                            a["node_id"] = self.node_id
                            a["node_name"] = self.name
                        self.last_accounts = accounts
                        return self.last_accounts
        except Exception:
            pass

        # 2. Try SSH for Serv00
        if self.ssh_host and self.ssh_user:
            ssh_accs = await asyncio.to_thread(self._fetch_accounts_via_ssh)
            if ssh_accs:
                self.last_accounts = ssh_accs
                return self.last_accounts

        return self.last_accounts

    def _fetch_accounts_via_ssh(self) -> Optional[List[Dict[str, Any]]]:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ssh_host, username=self.ssh_user, password=self.ssh_pass, timeout=4)
            stdin, stdout, stderr = ssh.exec_command("curl -s -m 3 http://127.0.0.1:9645/accounts")
            out = stdout.read().decode().strip()
            ssh.close()
            if out:
                data = json.loads(out)
                accounts = data.get("accounts", [])
                for a in accounts:
                    a["node_id"] = self.node_id
                    a["node_name"] = self.name
                return accounts
        except Exception:
            pass
        return None

    async def send_chat_message(self, message: str, streamer: str = "xyroplayzz", account_alias: Optional[str] = None) -> Dict[str, Any]:
        """Dispatches an outgoing chat message through this worker node with SSH failover."""
        payload = {
            "message": message,
            "streamer": streamer,
            "account": account_alias
        }
        # 1. Try HTTP
        try:
            timeout = aiohttp.ClientTimeout(total=8)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(f"{self.api_url}/chat", json=payload) as resp:
                    if resp.status in (200, 400):
                        return await resp.json()
        except Exception:
            pass

        # 2. Try SSH for Serv00
        if self.ssh_host and self.ssh_user:
            return await asyncio.to_thread(self._send_chat_via_ssh, message, streamer, account_alias)

        return {"success": False, "error": f"Failed reaching worker node {self.name}"}

    def _send_chat_via_ssh(self, message: str, streamer: str, account_alias: Optional[str]) -> Dict[str, Any]:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ssh_host, username=self.ssh_user, password=self.ssh_pass, timeout=5)
            payload_str = json.dumps({"message": message, "streamer": streamer, "account": account_alias}).replace('"', '\\"')
            cmd = f"curl -s -m 5 -X POST -H 'Content-Type: application/json' -d \"{payload_str}\" http://127.0.0.1:9645/chat"
            stdin, stdout, stderr = ssh.exec_command(cmd)
            out = stdout.read().decode().strip()
            ssh.close()
            if out:
                return json.loads(out)
        except Exception as e:
            return {"success": False, "error": f"SSH chat error: {e}"}
        return {"success": False, "error": "Empty response from SSH chat command"}

    def _check_ssh_process_alive(self) -> bool:
        if not self.ssh_host or not self.ssh_user:
            return False
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(self.ssh_host, username=self.ssh_user, password=self.ssh_pass, timeout=4)
            stdin, stdout, stderr = ssh.exec_command("pgrep -f 'main.py'")
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
            ssh.connect(self.ssh_host, username=self.ssh_user, password=self.ssh_pass, timeout=6)
            ssh.exec_command("pkill -9 -f 'main.py'")
            cmd = f"cd {self.remote_dir} && nohup /usr/local/bin/python3 main.py </dev/null >miner.log 2>&1 &"
            ssh.exec_command(cmd)
            ssh.close()
            return {"success": True, "message": f"Worker '{self.name}' restarted successfully via SSH"}
        except Exception as e:
            return {"success": False, "error": str(e)}
