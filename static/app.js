// Kick Master Panel Client Application

let allAccounts = [];
let isScanningVision = false;

document.addEventListener("DOMContentLoaded", () => {
  fetchStats();
  fetchAccounts();
  setInterval(fetchStats, 4000);
  setInterval(fetchAccounts, 15000);
});

// Fetch Real-time Master Stats
async function fetchStats() {
  try {
    const res = await fetch("/api/stats");
    if (!res.ok) return;
    const data = await res.json();

    // Fleet Summary
    const worker = (data.workers && data.workers[0]) || {};
    const totalWorkers = (data.workers || []).length;
    const onlineWorkers = (data.workers || []).filter(w => w.status === "online").length;
    
    document.getElementById("statWorkers").innerText = `${onlineWorkers} / ${totalWorkers} Online`;
    const memMb = worker.memory_mb || 0;
    document.getElementById("statRam").innerText = `Memory: ${memMb} MB / 512 MB`;
    document.getElementById("footerRam").innerText = `${memMb} MB`;

    // Fleet Indicator
    const fleetInd = document.getElementById("fleetIndicator");
    const fleetText = document.getElementById("fleetStatusText");
    if (onlineWorkers > 0) {
      fleetInd.className = "pulse-indicator online";
      fleetText.innerText = "FLEET ONLINE";
    } else {
      fleetInd.className = "pulse-indicator offline";
      fleetText.innerText = "FLEET OFFLINE";
    }

    // Viewers & Accounts
    const totalAcc = worker.total_accounts || 0;
    const activeViewers = worker.active_viewers || 0;
    document.getElementById("statViewers").innerText = `${activeViewers} / ${totalAcc}`;
    const rate = totalAcc > 0 ? Math.round((activeViewers / totalAcc) * 100) : 0;
    document.getElementById("statWatchingRate").innerText = `${rate}% Watching`;

    // Total Channel Points
    const pts = (worker.total_points || 0).toLocaleString();
    document.getElementById("statTotalPoints").innerText = pts;

    // Streamer Live Status
    const streamers = worker.streamers || {};
    const targetStreamer = data.target_streamer || "xyroplayzz";
    const stStatus = streamers[targetStreamer] || {};
    const isLive = stStatus.online || false;
    const liveBadge = document.getElementById("streamerLiveBadge");
    if (isLive) {
      liveBadge.className = "badge badge-success";
      liveBadge.innerText = "ONLINE";
    } else {
      liveBadge.className = "badge badge-primary";
      liveBadge.innerText = "OFFLINE / IDLE";
    }

    // Auto-Commenter
    const autoDisp = data.auto_dispatch || false;
    document.getElementById("statAutoComment").innerText = autoDisp ? "ACTIVE" : "OFF";
    document.getElementById("statAutoComment").className = autoDisp ? "stat-value accent" : "stat-value";
    document.getElementById("toggleAutoDispatch").checked = autoDisp;
    document.getElementById("statQueueCount").innerText = `Queue: ${data.queue_count || 0} comments`;

    // Vision Analysis
    const vision = data.vision || {};
    if (vision.game) {
      document.getElementById("visionGame").innerText = vision.game;
    }
    if (vision.event) {
      document.getElementById("visionEventText").innerText = vision.event;
    }

    // Reaction Chips
    renderReactionChips(vision.reactions || []);

    // Chat Dispatch History
    renderDispatchHistory(data.history || []);

  } catch (err) {
    console.error("fetchStats error:", err);
  }
}

// Fetch Accounts List
async function fetchAccounts() {
  try {
    const res = await fetch("/api/accounts");
    if (!res.ok) return;
    const data = await res.json();
    allAccounts = data.accounts || [];
    renderAccountsTable(allAccounts);
    renderAccountDropdown(allAccounts);
  } catch (err) {
    console.error("fetchAccounts error:", err);
  }
}

// Render Reaction Chips
function renderReactionChips(reactions) {
  const container = document.getElementById("reactionChips");
  if (!reactions || reactions.length === 0) {
    container.innerHTML = '<span class="chip-placeholder">Reactions will appear when stream is analyzed</span>';
    return;
  }

  container.innerHTML = "";
  reactions.forEach(r => {
    const chip = document.createElement("button");
    chip.type = "button";
    chip.className = "reaction-chip";
    chip.innerText = r;
    chip.onclick = () => sendReaction(r);
    container.appendChild(chip);
  });
}

// Render Accounts Table
function renderAccountsTable(accounts) {
  const tbody = document.getElementById("accountsTableBody");
  document.getElementById("accountCount").innerText = accounts.length;

  if (accounts.length === 0) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="5">No accounts found on worker</td></tr>';
    return;
  }

  let html = "";
  accounts.forEach(acc => {
    const watchingBadge = acc.is_watching
      ? '<span class="badge badge-success">● Watching</span>'
      : '<span class="badge badge-primary">○ Idle</span>';

    const proxyText = (acc.proxy && acc.proxy !== "direct")
      ? acc.proxy.split("@").pop()
      : "Direct (Resident)";

    html += `
      <tr>
        <td class="account-cell">${escapeHtml(acc.alias || "acc")}</td>
        <td>${watchingBadge}</td>
        <td class="points-cell">${(acc.points || 0).toLocaleString()}</td>
        <td style="color: var(--text-dim); font-size: 11px;">${escapeHtml(proxyText)}</td>
        <td>
          <button class="btn-sm" onclick="selectSenderAccount('${escapeHtml(acc.alias)}')">Select</button>
        </td>
      </tr>
    `;
  });
  tbody.innerHTML = html;
}

// Render Dropdown Selector
function renderAccountDropdown(accounts) {
  const select = document.getElementById("selectAccount");
  const currentVal = select.value;
  select.innerHTML = '<option value="">Auto (Pick Random Active Account)</option>';

  accounts.forEach(acc => {
    const opt = document.createElement("option");
    opt.value = acc.alias;
    opt.innerText = `${acc.alias} (${(acc.points || 0).toLocaleString()} pts)`;
    select.appendChild(opt);
  });

  if (currentVal) select.value = currentVal;
}

// Render Dispatch History
function renderDispatchHistory(history) {
  const tbody = document.getElementById("dispatchLogBody");
  if (!history || history.length === 0) {
    return;
  }

  let html = "";
  history.slice(0, 30).forEach(item => {
    const statusBadge = item.success
      ? '<span class="badge badge-success">SENT</span>'
      : '<span class="badge badge-danger">FAIL</span>';

    html += `
      <tr>
        <td style="font-family: var(--font-mono); font-size: 11px; color: var(--text-dim);">${escapeHtml(item.timestamp || "--:--")}</td>
        <td class="account-cell" style="font-size: 11px;">${escapeHtml(item.account || "auto")}</td>
        <td style="color: var(--text-main); font-weight: 500;">${escapeHtml(item.message || "")}</td>
        <td>${statusBadge}</td>
      </tr>
    `;
  });
  tbody.innerHTML = html;
}

// Trigger Manual Vision Scan
async function triggerVisionScan() {
  if (isScanningVision) return;
  isScanningVision = true;
  const btn = document.getElementById("btnTriggerVision");
  btn.innerHTML = '<span>⏳</span> Scanning...';

  try {
    const res = await fetch("/api/vision/trigger", { method: "POST" });
    const data = await res.json();
    if (data.success && data.analysis) {
      document.getElementById("visionGame").innerText = data.analysis.game || "Detected";
      document.getElementById("visionEventText").innerText = data.analysis.event || "Analyzed";
      renderReactionChips(data.analysis.reactions || []);
      // Refresh frame image
      const frameImg = document.getElementById("liveStreamFrame");
      frameImg.src = "/api/frame?t=" + Date.now();
    }
  } catch (e) {
    console.error("Vision trigger error:", e);
  } finally {
    isScanningVision = false;
    btn.innerHTML = '<span class="icon">👁️</span> Scan Vision';
    fetchStats();
  }
}

// Send Reaction Chip
async function sendReaction(reaction) {
  document.getElementById("inputComment").value = reaction;
  await sendCommentInternal(reaction);
}

// Form Comment Send
async function sendComment(e) {
  e.preventDefault();
  const input = document.getElementById("inputComment");
  const msg = input.value.trim();
  if (!msg) return;

  const btn = document.getElementById("btnSendComment");
  btn.disabled = true;
  btn.innerText = "Sending...";

  await sendCommentInternal(msg);

  input.value = "";
  btn.disabled = false;
  btn.innerHTML = '<span>Send</span> 🚀';
}

async function sendCommentInternal(message) {
  const account = document.getElementById("selectAccount").value;
  const streamer = document.getElementById("inputStreamer").value.trim() || "xyroplayzz";

  try {
    const res = await fetch("/api/comment/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ message, account, streamer })
    });
    const result = await res.json();
    fetchStats();
  } catch (err) {
    alert("Error sending comment: " + err.message);
  }
}

// Quick Tag Insertion
function insertTag(tag) {
  const input = document.getElementById("inputComment");
  input.value = input.value ? `${input.value} ${tag}` : tag;
  input.focus();
}

function selectSenderAccount(alias) {
  const select = document.getElementById("selectAccount");
  select.value = alias;
  document.getElementById("inputComment").focus();
}

// Toggle Auto-Dispatch
async function toggleAutoDispatch(enabled) {
  try {
    await fetch("/api/autocomment/toggle", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ enabled })
    });
    fetchStats();
  } catch (e) {
    console.error("toggleAutoDispatch error:", e);
  }
}

// Trigger Garbage Collector on Worker
async function triggerGC() {
  const btn = document.getElementById("btnRunGc");
  btn.innerText = "Cleaning...";
  try {
    const res = await fetch("/api/worker/gc", { method: "POST" });
    const data = await res.json();
    alert(`Garbage collection finished. Freed objects: ${data.collected_objects || 0}`);
    fetchStats();
  } catch (e) {
    alert("GC error: " + e.message);
  } finally {
    btn.innerHTML = '<span class="icon">🧹</span> Free RAM';
  }
}

// Restart Worker via SSH
async function restartWorker() {
  if (!confirm("Are you sure you want to restart the remote Worker Miner process on Serv00?")) {
    return;
  }
  const btn = document.getElementById("btnRestartWorker");
  btn.innerText = "Restarting...";
  try {
    const res = await fetch("/api/worker/restart", { method: "POST" });
    const data = await res.json();
    alert(data.message || (data.success ? "Worker restarted" : "Failed to restart"));
    setTimeout(fetchStats, 3000);
  } catch (e) {
    alert("Restart error: " + e.message);
  } finally {
    btn.innerHTML = '<span class="icon">🔄</span> Restart Worker';
  }
}

// Account Table Filter
function filterAccounts() {
  const query = document.getElementById("accountSearch").value.toLowerCase().trim();
  if (!query) {
    renderAccountsTable(allAccounts);
    return;
  }
  const filtered = allAccounts.filter(acc => 
    (acc.alias && acc.alias.toLowerCase().includes(query)) ||
    (acc.proxy && acc.proxy.toLowerCase().includes(query))
  );
  renderAccountsTable(filtered);
}

function clearDispatchLog() {
  document.getElementById("dispatchLogBody").innerHTML = '<tr class="empty-row"><td colspan="4">Log cleared</td></tr>';
}

function escapeHtml(text) {
  if (!text) return "";
  return String(text)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}
