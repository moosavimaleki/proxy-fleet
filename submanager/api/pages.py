from __future__ import annotations


def render_dashboard_html() -> str:
    return _render_layout("fleet", "Proxy Fleet", _dashboard_body(), _dashboard_script())


def render_client_status_html() -> str:
    return _render_layout("clients", "Client Lens", _client_body(), _client_script())


def render_diag_html() -> str:
    return _render_layout("diag", "Diagnostics", _diag_body(), _diag_script())


def render_docs_html() -> str:
    return _render_layout("docs", "API Docs", _docs_body(), "")


def render_logs_html() -> str:
    return _render_layout("logs", "System Logs", _logs_body(), _logs_script())


def render_node_history_html() -> str:
    return _render_layout("fleet", "Node History", _history_body(), _history_script())


def render_manual_import_html() -> str:
    return _render_layout("fleet", "Manual Import", _manual_import_body(), _manual_import_script())


def _render_layout(active: str, title: str, body: str, script: str) -> str:
    nav = """
      <nav class="nav panel">
        <a href="/" class="nav-link {fleet}">Fleet</a>
        <a href="/clients" class="nav-link {clients}">Client Lens</a>
        <a href="/diag" class="nav-link {diag}">Diag</a>
        <a href="/logs" class="nav-link {logs}">Logs</a>
        <a href="/docs" class="nav-link {docs}">API Docs</a>
      </nav>
    """.format(
        fleet="active" if active == "fleet" else "",
        clients="active" if active == "clients" else "",
        diag="active" if active == "diag" else "",
        logs="active" if active == "logs" else "",
        docs="active" if active == "docs" else "",
    )
    return (
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        f"<title>{title}</title><style>{_base_css()}</style></head><body>"
        "<div class=\"shell\">"
        "<header class=\"topbar\">"
        "<div><p class=\"eyebrow\">SubManager</p>"
        f"<h1>{title}</h1></div>"
        "<p class=\"lead\">Live views over the proxy pool, client-specific circuit state, and the public API surface.</p>"
        "</header>"
        + nav
        + body
        + f"<script>{_shared_script()}{script}</script>"
        + "</div></body></html>"
    )


def _base_css() -> str:
    return """
    :root {
      --bg: #f3efe6;
      --panel: rgba(255, 253, 248, 0.92);
      --ink: #1f1d19;
      --muted: #6c655a;
      --line: #d8cfbf;
      --accent: #bb4d00;
      --green: #1f7a4d;
      --amber: #a86f00;
      --red: #b42318;
      --blue: #005a9c;
      --purple: #6e3cbc;
      --shadow: 0 12px 30px rgba(74, 53, 24, 0.06);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      font: 14px/1.45 "IBM Plex Sans", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, #fff8ea 0, transparent 28%),
        linear-gradient(180deg, #efe7da 0%, var(--bg) 100%);
    }
    .shell { max-width: 1600px; margin: 0 auto; padding: 24px; }
    .topbar {
      display: grid;
      gap: 10px;
      grid-template-columns: 1.2fr 1fr;
      align-items: end;
      margin-bottom: 16px;
    }
    .eyebrow {
      margin: 0 0 6px;
      color: var(--accent);
      font: 700 12px/1 "IBM Plex Sans", sans-serif;
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }
    h1 {
      margin: 0;
      font: 700 34px/1.05 "Iosevka Aile", "IBM Plex Sans", sans-serif;
      letter-spacing: -0.04em;
    }
    .lead {
      margin: 0;
      color: var(--muted);
      max-width: 62ch;
      justify-self: end;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      box-shadow: var(--shadow);
    }
    .nav {
      display: flex;
      gap: 8px;
      padding: 8px;
      margin-bottom: 18px;
      position: sticky;
      top: 12px;
      z-index: 10;
      backdrop-filter: blur(8px);
    }
    .nav-link {
      color: var(--muted);
      text-decoration: none;
      padding: 10px 14px;
      border-radius: 12px;
      font-weight: 600;
    }
    .nav-link:hover { background: rgba(187, 77, 0, 0.08); color: var(--ink); }
    .nav-link.active { background: var(--accent); color: white; }
    .summary {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }
    .card {
      padding: 16px;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: var(--shadow);
    }
    .label {
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .value {
      margin-top: 6px;
      font: 700 28px/1 "Iosevka Aile", monospace;
    }
    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
      margin-bottom: 14px;
    }
    .toolbar input, .toolbar select, .toolbar button {
      min-height: 42px;
      border-radius: 12px;
      font: inherit;
    }
    .toolbar input, .toolbar select {
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--ink);
      padding: 10px 12px;
    }
    .toolbar button {
      border: 0;
      background: var(--accent);
      color: white;
      padding: 10px 16px;
      cursor: pointer;
      font-weight: 600;
    }
    .toolbar .ghost {
      background: #ece2d2;
      color: var(--ink);
    }
    .muted { color: var(--muted); }
    .mono {
      font-family: "Iosevka Aile", "SFMono-Regular", monospace;
      font-size: 12px;
      word-break: break-word;
    }
    .table-wrap {
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: var(--panel);
      box-shadow: var(--shadow);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 1580px;
      table-layout: fixed;
    }
    th, td {
      padding: 12px 10px;
      border-bottom: 1px solid rgba(216, 207, 191, 0.8);
      vertical-align: top;
      text-align: left;
    }
    th {
      position: sticky;
      top: 0;
      background: #f7f2e8;
      z-index: 1;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      color: var(--muted);
    }
    tr:hover td { background: rgba(255, 248, 235, 0.58); }
    .status {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 5px 10px;
      border-radius: 999px;
      font-weight: 700;
      font-size: 12px;
      border: 1px solid currentColor;
      background: rgba(255, 255, 255, 0.65);
    }
    .status.ACTIVE, .status.CLOSED { color: var(--green); }
    .status.PROBATION, .status.HALF_OPEN { color: var(--amber); }
    .status.DEAD, .status.OPEN { color: var(--red); }
    .status.TESTING { color: var(--blue); }
    .status.CANDIDATE, .status.WAITING_FOR_PORT, .status.UNSEEN { color: var(--purple); }
    details summary {
      cursor: pointer;
      color: var(--accent);
      font-weight: 600;
    }
    pre {
      margin: 10px 0 0;
      padding: 12px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: #f7f2e8;
      overflow: auto;
      white-space: pre-wrap;
    }
    .stack {
      display: grid;
      gap: 14px;
    }
    .doc-grid {
      display: grid;
      gap: 14px;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    }
    .doc-card { padding: 18px; }
    .doc-card h3 { margin: 0 0 8px; }
    .doc-card p { margin: 0 0 12px; color: var(--muted); }
    .log-table { min-width: 1320px; }
    .log-details {
      max-height: 180px;
      margin: 0;
      font-size: 12px;
      white-space: pre-wrap;
    }
    .empty { padding: 32px; text-align: center; color: var(--muted); }
    .tight { white-space: nowrap; }
    .wrap { word-break: break-word; }
    .actions {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
    }
    .mini-btn {
      border: 1px solid var(--line);
      background: #fff7ec;
      color: var(--ink);
      border-radius: 10px;
      padding: 8px 10px;
      cursor: pointer;
      font: 600 12px/1 "IBM Plex Sans", sans-serif;
    }
    .mini-btn.primary {
      background: var(--accent);
      color: white;
      border-color: var(--accent);
    }
    .pill {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 999px;
      background: #efe3d1;
      color: var(--ink);
      font: 700 11px/1.2 "IBM Plex Sans", sans-serif;
    }
    .modal-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(31, 29, 25, 0.42);
      display: none;
      align-items: center;
      justify-content: center;
      padding: 20px;
      z-index: 100;
    }
    .modal-backdrop.open { display: flex; }
    .modal {
      display: block;
      position: relative;
      flex: 0 1 1120px;
      width: min(1120px, 100%);
      max-height: 88vh;
      overflow: auto;
      background: #fffdf8;
      border: 1px solid var(--line);
      border-radius: 22px;
      box-shadow: 0 24px 80px rgba(31, 29, 25, 0.18);
    }
    .modal-backdrop.open .modal {
      display: block;
      visibility: visible;
      opacity: 1;
    }
    .modal-head {
      position: sticky;
      top: 0;
      background: #fffaf1;
      border-bottom: 1px solid var(--line);
      padding: 18px 20px;
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: start;
    }
    .modal-body { padding: 18px 20px 24px; }
    .modal-close {
      border: 0;
      background: #efe3d1;
      color: var(--ink);
      border-radius: 12px;
      padding: 10px 12px;
      cursor: pointer;
      font-weight: 700;
    }
    .history-list {
      display: grid;
      gap: 12px;
    }
    .history-item {
      border: 1px solid var(--line);
      background: #fffcf5;
      border-radius: 16px;
      padding: 14px;
    }
    .history-item h4 {
      margin: 0 0 6px;
      font: 700 16px/1.2 "Iosevka Aile", monospace;
    }
    .history-meta {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 10px;
      margin-top: 10px;
    }
    .hero-strip {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }
    @media (max-width: 900px) {
      .shell { padding: 14px; }
      .topbar { grid-template-columns: 1fr; }
      .lead { justify-self: start; }
      .nav { top: 8px; overflow: auto; }
    }
    """


def _shared_script() -> str:
    return """
    function esc(value) {
      return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }
    function prettyDate(value) {
      if (!value) return "-";
      const d = new Date(value);
      if (Number.isNaN(d.getTime())) return value;
      return d.toLocaleString();
    }
    function metric(value, suffix = "") {
      if (value === null || value === undefined || value === "") return "-";
      return `${value}${suffix}`;
    }
    """


def _dashboard_body() -> str:
    return """
    <section id="summary" class="summary"></section>
    <section class="toolbar">
      <input id="search" type="search" placeholder="Search by remark, host, source, status, hash">
      <select id="status-filter">
        <option value="">All statuses</option>
      </select>
      <select id="country-filter">
        <option value="">All countries</option>
      </select>
      <a href="/manual-import" class="nav-link" style="background:var(--accent);color:white;">Add Manual</a>
      <a href="/diag" class="nav-link" style="background:#ece2d2;">Diag</a>
      <button id="refresh-btn" type="button">Refresh now</button>
      <span id="last-refresh" class="muted"></span>
    </section>
    <section class="table-wrap">
      <table>
        <colgroup>
          <col style="width: 140px">
          <col style="width: 180px">
          <col style="width: 170px">
          <col style="width: 140px">
          <col style="width: 150px">
          <col style="width: 150px">
          <col style="width: 170px">
          <col style="width: 190px">
          <col style="width: 190px">
          <col style="width: 220px">
        </colgroup>
        <thead>
          <tr>
            <th>Status</th>
            <th>Proxy</th>
            <th>Remark</th>
            <th>Runtime</th>
            <th>Relay</th>
            <th>Load</th>
            <th>Updated</th>
            <th>Actions</th>
            <th>Config</th>
            <th>Sources</th>
          </tr>
        </thead>
        <tbody id="rows">
          <tr><td class="empty" colspan="10">Loading...</td></tr>
        </tbody>
      </table>
    </section>
    <section id="modal-backdrop" class="modal-backdrop">
      <div class="modal">
        <div class="modal-head">
          <div>
            <p class="eyebrow" id="modal-eyebrow">Details</p>
            <h2 id="modal-title" style="margin:0;font:700 24px/1.1 'Iosevka Aile', monospace;"></h2>
          </div>
          <button id="modal-close" class="modal-close" type="button">Close</button>
        </div>
        <div id="modal-body" class="modal-body"></div>
      </div>
    </section>
    """


def _history_body() -> str:
    return """
    <section id="history-hero" class="hero-strip"></section>
    <section class="toolbar">
      <input id="history-node-input" type="search" placeholder="Enter a node id">
      <button id="history-load-btn" type="button">Load history</button>
      <a href="/" class="nav-link" style="background:#ece2d2;">Back to Fleet</a>
      <span id="history-last-refresh" class="muted"></span>
    </section>
    <section id="history-list" class="history-list"></section>
    """


def _manual_import_body() -> str:
    return """
    <section class="stack">
      <article class="panel doc-card">
        <h3>Paste Share Links</h3>
        <p>Paste one or more share links, newline separated. The service treats them like subscription entries, queues them into the candidate pool, and the candidate worker tests them on its normal cycle.</p>
        <textarea id="manual-import-text" style="width:100%;min-height:320px;border:1px solid #d8cfbf;border-radius:14px;padding:12px;background:#fffdf8;font:13px/1.45 'Iosevka Aile', monospace;"></textarea>
        <div class="actions" style="margin-top:12px;">
          <button id="manual-import-submit" class="mini-btn primary" type="button">Import to Candidate</button>
          <a href="/" class="nav-link" style="background:#ece2d2;">Back to Fleet</a>
        </div>
      </article>
      <article id="manual-import-result-card" class="panel doc-card">
        <h3>Result</h3>
        <div id="manual-import-result" class="muted">No import submitted yet.</div>
      </article>
    </section>
    """


def _diag_body() -> str:
    return """
    <section class="stack">
      <article class="panel doc-card">
        <h3>Operational Actions</h3>
        <p>Use this page for maintenance actions that change pool state or clean the database. These actions run immediately.</p>
        <div class="actions" style="margin-top:12px;">
          <button id="diag-reload-subs-btn" class="mini-btn primary" type="button">Reload subscriptions</button>
          <button id="diag-clear-dead-btn" class="mini-btn" type="button">Clear dead pool</button>
          <button id="diag-db-cleanup-btn" class="mini-btn" type="button">DB cleanup</button>
          <a href="/manual-import" class="nav-link" style="background:#ece2d2;">Manual Import</a>
          <a href="/logs" class="nav-link" style="background:#ece2d2;">System Logs</a>
          <a href="/" class="nav-link" style="background:#ece2d2;">Back to Fleet</a>
        </div>
      </article>
      <article class="panel doc-card">
        <h3>Current State</h3>
        <div id="diag-summary" class="history-list"></div>
      </article>
      <article class="panel doc-card">
        <h3>Last Action Result</h3>
        <div id="diag-result" class="muted">No action executed yet.</div>
      </article>
    </section>
    """


def _logs_body() -> str:
    return """
    <section class="toolbar">
      <select id="log-component">
        <option value="">All components</option>
        <option value="network">network</option>
        <option value="subscription">subscription</option>
        <option value="candidate">candidate</option>
        <option value="health">health</option>
        <option value="dead-janitor">dead-janitor</option>
        <option value="vip">vip</option>
      </select>
      <select id="log-level">
        <option value="">All levels</option>
        <option value="info">info</option>
        <option value="warning">warning</option>
        <option value="error">error</option>
      </select>
      <input id="log-limit" type="number" min="1" max="1000" value="200" style="width:110px;">
      <button id="log-refresh-btn" type="button">Refresh logs</button>
      <button id="log-live-btn" class="ghost" type="button" data-live="1">Live: on</button>
      <a href="/diag" class="nav-link" style="background:#ece2d2;">Diag</a>
      <span id="log-last-refresh" class="muted"></span>
    </section>
    <section class="table-wrap">
      <table class="log-table">
        <colgroup>
          <col style="width: 190px">
          <col style="width: 100px">
          <col style="width: 150px">
          <col style="width: 190px">
          <col style="width: 320px">
          <col style="width: 370px">
        </colgroup>
        <thead>
          <tr>
            <th>Time</th>
            <th>Level</th>
            <th>Component</th>
            <th>Event</th>
            <th>Message</th>
            <th>Details</th>
          </tr>
        </thead>
        <tbody id="log-rows">
          <tr><td class="empty" colspan="6">Loading...</td></tr>
        </tbody>
      </table>
    </section>
    """


def _logs_script() -> str:
    return """
    const logState = { live: true };

    function renderLogRows(events) {
      const tbody = document.getElementById("log-rows");
      if (!events || !events.length) {
        tbody.innerHTML = '<tr><td class="empty" colspan="6">No system events found.</td></tr>';
        return;
      }
      tbody.innerHTML = events.map((item) => `
        <tr>
          <td class="mono">${esc(prettyDate(item.created_at))}</td>
          <td><span class="status ${esc(item.level === "error" ? "DEAD" : item.level === "warning" ? "PROBATION" : "ACTIVE")}">${esc(item.level)}</span></td>
          <td class="mono">${esc(item.component)}</td>
          <td class="mono wrap">${esc(item.event)}</td>
          <td class="wrap">${esc(item.message)}</td>
          <td><pre class="log-details">${esc(JSON.stringify(item.details || {}, null, 2))}</pre></td>
        </tr>
      `).join("");
    }

    async function refreshLogs() {
      const component = document.getElementById("log-component").value;
      const level = document.getElementById("log-level").value;
      const limit = Math.max(1, Math.min(1000, Number(document.getElementById("log-limit").value || 200)));
      const params = new URLSearchParams({ limit: String(limit) });
      if (component) params.set("component", component);
      if (level) params.set("level", level);
      const response = await fetch(`/api/v1/logs?${params.toString()}`, { headers: { "Accept": "application/json" } });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      renderLogRows(payload.events || []);
      document.getElementById("log-last-refresh").textContent = `Last refresh: ${new Date().toLocaleTimeString()}`;
    }

    function showLogError(error) {
      document.getElementById("log-rows").innerHTML = `<tr><td class="empty" colspan="6">Failed to load logs: ${esc(error.message)}</td></tr>`;
    }

    document.getElementById("log-refresh-btn").addEventListener("click", () => refreshLogs().catch(showLogError));
    document.getElementById("log-component").addEventListener("change", () => refreshLogs().catch(showLogError));
    document.getElementById("log-level").addEventListener("change", () => refreshLogs().catch(showLogError));
    document.getElementById("log-limit").addEventListener("change", () => refreshLogs().catch(showLogError));
    document.getElementById("log-live-btn").addEventListener("click", () => {
      logState.live = !logState.live;
      const button = document.getElementById("log-live-btn");
      button.dataset.live = logState.live ? "1" : "0";
      button.textContent = `Live: ${logState.live ? "on" : "off"}`;
    });

    refreshLogs().catch(showLogError);
    setInterval(() => {
      if (logState.live) refreshLogs().catch(showLogError);
    }, 10000);
    """


def _dashboard_script() -> str:
    return """
    const fleetState = { payload: null };
    let activeModalNode = null;

    function countryFlag(countryCode) {
      const code = String(countryCode || "").trim().toUpperCase();
      if (!/^[A-Z]{2}$/.test(code)) return "";
      return String.fromCodePoint(...Array.from(code).map((char) => 127397 + char.charCodeAt(0)));
    }

    function locationLabel(node) {
      const parts = [node.exit_city, node.exit_region].filter(Boolean);
      if (parts.length) return parts.join(", ");
      return node.exit_ip || node.exit_org || "";
    }

    function renderFleetSummary(payload) {
      const wrap = document.getElementById("summary");
      const vipLabel = payload.vip?.enabled ? `VIP ${payload.vip.port}` : "VIP disabled";
      const vipValue = payload.vip?.node_id ? `${payload.vip.port}` : "offline";
      const networkLabel = payload.network?.enabled ? "Network" : "Guard off";
      const networkValue = payload.network?.enabled ? (payload.network?.online ? "online" : "offline") : "-";
      const cards = [
        ["Total Nodes", payload.total_nodes],
        ["Active", payload.status_counts.ACTIVE || 0],
        ["Probation", payload.status_counts.PROBATION || 0],
        ["Dead", payload.status_counts.DEAD || 0],
        ["Testing", payload.status_counts.TESTING || 0],
        ["Waiting", payload.status_counts.WAITING_FOR_PORT || 0],
        [vipLabel, vipValue],
        [networkLabel, networkValue],
      ];
      wrap.innerHTML = cards.map(([label, value]) => `
        <article class="card">
          <div class="label">${esc(label)}</div>
          <div class="value">${esc(value)}</div>
        </article>
      `).join("");
    }

    function renderFleetStatusFilter(payload) {
      const select = document.getElementById("status-filter");
      const current = select.value;
      const options = ["", ...Object.keys(payload.status_counts)];
      select.innerHTML = options.map((value) => {
        const label = value || "All statuses";
        const selected = value === current ? " selected" : "";
        return `<option value="${esc(value)}"${selected}>${esc(label)}</option>`;
      }).join("");
    }

    function renderFleetCountryFilter(payload) {
      const select = document.getElementById("country-filter");
      const current = select.value;
      const countries = Array.from(new Set((payload.nodes || []).map((node) => String(node.exit_country || "").trim()).filter(Boolean))).sort();
      const options = ["", ...countries];
      select.innerHTML = options.map((value) => {
        const label = value || "All countries";
        const selected = value === current ? " selected" : "";
        const prefix = value ? `${countryFlag(value)} ` : "";
        return `<option value="${esc(value)}"${selected}>${esc(prefix + label)}</option>`;
      }).join("");
    }

    function renderFleetRows() {
      const payload = fleetState.payload;
      const tbody = document.getElementById("rows");
      if (!payload) return;

      const search = document.getElementById("search").value.trim().toLowerCase();
      const statusFilter = document.getElementById("status-filter").value;
      const countryFilter = document.getElementById("country-filter").value;
      const rows = payload.nodes.filter((node) => {
        if (statusFilter && node.status !== statusFilter) return false;
        if (countryFilter && String(node.exit_country || "") !== countryFilter) return false;
        if (!search) return true;
        const blob = [
          node.id, node.status, node.protocol, node.remark, node.server, node.config_hash, node.exit_country, node.exit_city, node.exit_org, ...(node.source_subs || [])
        ].join(" ").toLowerCase();
        return blob.includes(search);
      }).sort((left, right) => {
        const leftVip = left.is_vip ? 0 : 1;
        const rightVip = right.is_vip ? 0 : 1;
        if (leftVip !== rightVip) return leftVip - rightVip;
        const leftStatus = String(left.status || "");
        const rightStatus = String(right.status || "");
        if (leftStatus !== rightStatus) return leftStatus.localeCompare(rightStatus);
        const leftDelay = Number.isFinite(left.relay_delay_ms) ? left.relay_delay_ms : Number.MAX_SAFE_INTEGER;
        const rightDelay = Number.isFinite(right.relay_delay_ms) ? right.relay_delay_ms : Number.MAX_SAFE_INTEGER;
        if (leftDelay !== rightDelay) return leftDelay - rightDelay;
        return String(left.id || "").localeCompare(String(right.id || ""));
      });

      if (!rows.length) {
        tbody.innerHTML = '<tr><td class="empty" colspan="10">No rows match the current filter.</td></tr>';
        return;
      }

      tbody.innerHTML = rows.map((node) => `
        <tr>
          <td class="tight"><span class="status ${esc(node.status)}">${esc(node.status)}</span>${node.exit_country ? `<br><span class="muted" title="${esc(locationLabel(node) || node.exit_country)}">${esc(countryFlag(node.exit_country))} ${esc(node.exit_country)}</span>` : ''}${node.is_vip ? '<br><span class="pill" style="margin-top:8px;">HOT PORT</span>' : ''}</td>
          <td class="mono wrap">${esc(node.protocol)}<br>${esc(node.server)}:${esc(node.remote_port)}<br><span class="muted">${esc(node.id.slice(0, 12))}</span></td>
          <td class="wrap"><strong>${esc(node.remark || "-")}</strong><br><span class="muted mono">${esc(node.config_hash.slice(0, 18))}</span></td>
          <td class="mono">run: ${esc(node.runtime_running)}<br>main: ${esc(metric(node.main_port))}<br>rt: ${esc(metric(node.runtime_port))}</td>
          <td class="mono">lat: ${esc(metric(node.relay_delay_ms, " ms"))}<br>spd: ${esc(metric(node.download_kbps, " kbps"))}<br>ewma: ${esc(node.health_success_ewma)}</td>
          <td class="mono">open: ${esc(node.open_assignments)}<br>total: ${esc(node.total_assignments)}<br>clients: ${esc(node.total_clients)}</td>
          <td class="mono">test: ${esc(prettyDate(node.last_test_at))}<br>health: ${esc(prettyDate(node.last_health_check_at))}<br>updated: ${esc(prettyDate(node.updated_at))}</td>
          <td>
            <div class="actions">
              <button class="mini-btn primary" data-action="test" data-node-id="${esc(node.id)}">Test now</button>
              <button class="mini-btn" data-action="history" data-node-id="${esc(node.id)}">History</button>
            </div>
          </td>
          <td>
            <div class="actions">
              <button class="mini-btn" data-action="copy-config" data-node-id="${esc(node.id)}">Copy config</button>
            </div>
          </td>
          <td class="mono wrap">${esc((node.source_subs || []).join("\\n") || "-")}</td>
        </tr>
      `).join("");
    }

    async function refreshFleet() {
      const response = await fetch("/api/v1/nodes", { headers: { "Accept": "application/json" } });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      fleetState.payload = await response.json();
      renderFleetSummary(fleetState.payload);
      renderFleetStatusFilter(fleetState.payload);
      renderFleetCountryFilter(fleetState.payload);
      renderFleetRows();
      const net = fleetState.payload.network;
      const netText = net?.enabled ? `network=${net.online ? "online" : "offline"}` : "network-guard=disabled";
      document.getElementById("last-refresh").textContent = `Last refresh: ${new Date().toLocaleTimeString()} | ${netText}`;
    }

    document.getElementById("search").addEventListener("input", renderFleetRows);
    document.getElementById("status-filter").addEventListener("change", renderFleetRows);
    document.getElementById("country-filter").addEventListener("change", renderFleetRows);
    document.getElementById("refresh-btn").addEventListener("click", () => refreshFleet().catch(showFleetError));

    function showFleetError(error) {
      document.getElementById("rows").innerHTML = `<tr><td class="empty" colspan="11">Failed to load dashboard data: ${esc(error.message)}</td></tr>`;
    }

    function openModal(title, eyebrow, bodyHtml, nodeId = null) {
      activeModalNode = nodeId;
      document.getElementById("modal-title").textContent = title;
      document.getElementById("modal-eyebrow").textContent = eyebrow;
      document.getElementById("modal-body").innerHTML = bodyHtml;
      document.getElementById("modal-backdrop").classList.add("open");
    }

    function closeModal() {
      activeModalNode = null;
      document.getElementById("modal-backdrop").classList.remove("open");
    }

    function renderHistoryHtml(items) {
      if (!items || !items.length) {
        return '<p class="muted">No test history recorded yet for this node.</p>';
      }
      return '<div class="history-list">' + items.map((item) => {
        const meta = [
          { label: "Finished", value: prettyDate(item.finished_at) },
          { label: "Latency", value: metric(item.latency_ms, " ms") },
          { label: "Speed", value: metric(item.download_kbps, " kbps") },
          { label: "Network", value: String(item.network_online) },
          { label: "State", value: `${item.status_before} -> ${item.status_after}` },
        ];
        const metaHtml = meta.map((entry) => (
          `<div><span class="label">${esc(entry.label)}</span><div class="mono">${esc(entry.value)}</div></div>`
        )).join("");
        const payload = {
          error: item.error,
          details: item.details,
          started_at: item.started_at,
        };
        return `
          <article class="history-item">
            <h4>${esc(item.ok ? "PASS" : "FAIL")} · ${esc(item.trigger)} · ${esc(item.test_kind)}</h4>
            <div class="history-meta">${metaHtml}</div>
            <pre>${esc(JSON.stringify(payload, null, 2))}</pre>
          </article>
        `;
      }).join("") + '</div>';
    }

    async function handleFleetAction(event) {
      const button = event.target.closest("button[data-action]");
      if (!button) return;
      const action = button.dataset.action;
      const nodeId = button.dataset.nodeId;
      const node = (fleetState.payload?.nodes || []).find((item) => item.id === nodeId);
      if (!node) return;

      if (action === "copy-config") {
        try {
          await navigator.clipboard.writeText(node.raw_config || "");
        } catch (error) {
          openModal(node.remark || node.id, "Copy Config", `<p class="muted">Copy failed: ${esc(error.message)}</p>`, nodeId);
        }
        return;
      }

      if (action === "history") {
        window.location.href = `/history?node=${encodeURIComponent(nodeId)}`;
        return;
      }

      if (action === "test") {
        button.disabled = true;
        button.textContent = "Testing...";
        try {
          const response = await fetch(`/api/v1/nodes/${encodeURIComponent(nodeId)}/test`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: "{}",
          });
          const payload = await response.json();
          if (!response.ok) throw new Error(payload.message || payload.error || `HTTP ${response.status}`);
          await refreshFleet();
          openModal(
            node.remark || node.id,
            "Manual Test Result",
            `<pre>${esc(JSON.stringify(payload, null, 2))}</pre>`,
            nodeId,
          );
        } catch (error) {
          openModal(node.remark || node.id, "Manual Test Result", `<p class="muted">Manual test failed: ${esc(error.message)}</p>`, nodeId);
        } finally {
          button.disabled = false;
          button.textContent = "Test now";
        }
      }
    }

    refreshFleet().catch(showFleetError);
    setInterval(() => refreshFleet().catch(showFleetError), 10000);
    document.getElementById("rows").addEventListener("click", handleFleetAction);
    document.getElementById("modal-close").addEventListener("click", closeModal);
    document.getElementById("modal-backdrop").addEventListener("click", (event) => {
      if (event.target.id === "modal-backdrop") closeModal();
    });
    """


def _diag_script() -> str:
    return """
    function renderDiagSummary(fleet, network, vip) {
      const wrap = document.getElementById("diag-summary");
      const statusCounts = fleet.status_counts || {};
      const rows = [
        ["Total nodes", fleet.total_nodes ?? 0],
        ["Active", statusCounts.ACTIVE || 0],
        ["Candidate", statusCounts.CANDIDATE || 0],
        ["Testing", statusCounts.TESTING || 0],
        ["Probation", statusCounts.PROBATION || 0],
        ["Dead", statusCounts.DEAD || 0],
        ["Network", network?.enabled ? (network.online ? "online" : "offline") : "disabled"],
        ["VIP", vip?.enabled ? `${vip.port} -> ${vip.node_id || "offline"}` : "disabled"],
      ];
      wrap.innerHTML = rows.map(([label, value]) => `
        <article class="history-item">
          <div class="label">${esc(label)}</div>
          <div class="mono" style="margin-top:6px;font-size:14px;">${esc(value)}</div>
        </article>
      `).join("");
    }

    async function loadDiagState() {
      const response = await fetch("/api/v1/nodes", { headers: { "Accept": "application/json" } });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      renderDiagSummary(payload, payload.network, payload.vip);
    }

    function renderDiagResult(title, payload, isError = false) {
      const wrap = document.getElementById("diag-result");
      const color = isError ? "var(--red)" : "var(--ink)";
      wrap.innerHTML = `<div style="color:${color};font-weight:700;margin-bottom:8px;">${esc(title)}</div><pre>${esc(JSON.stringify(payload, null, 2))}</pre>`;
    }

    async function runDiagAction(buttonId, url, title, confirmMessage = "") {
      if (confirmMessage && !window.confirm(confirmMessage)) return;
      const button = document.getElementById(buttonId);
      const original = button.textContent;
      button.disabled = true;
      button.textContent = "Running...";
      try {
        const response = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}",
        });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.message || payload.error || `HTTP ${response.status}`);
        renderDiagResult(title, payload, false);
        await loadDiagState();
      } catch (error) {
        renderDiagResult(title, { error: error.message }, true);
      } finally {
        button.disabled = false;
        button.textContent = original;
      }
    }

    document.getElementById("diag-reload-subs-btn").addEventListener("click", () => {
      runDiagAction("diag-reload-subs-btn", "/api/v1/subscriptions/reload", "Reload subscriptions");
    });
    document.getElementById("diag-clear-dead-btn").addEventListener("click", () => {
      runDiagAction("diag-clear-dead-btn", "/api/v1/nodes/dead/clear", "Clear dead pool", "Delete every node currently in the dead pool?");
    });
    document.getElementById("diag-db-cleanup-btn").addEventListener("click", () => {
      runDiagAction("diag-db-cleanup-btn", "/api/v1/db/cleanup", "DB cleanup", "Delete stored history/event logs and vacuum the database?");
    });

    loadDiagState().catch((error) => {
      renderDiagResult("Diagnostics bootstrap", { error: error.message }, true);
    });
    """


def _client_body() -> str:
    return """
    <section id="client-summary" class="summary"></section>
    <section class="toolbar">
      <select id="client-select">
        <option value="">Known clients</option>
      </select>
      <input id="client-input" type="search" placeholder="Enter a client id">
      <button id="client-load-btn" type="button">Load client view</button>
      <button id="client-copy-btn" class="ghost" type="button">Copy direct link</button>
      <span id="client-last-refresh" class="muted"></span>
    </section>
    <section class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Node</th>
            <th>Client Circuit</th>
            <th>Feedback History</th>
            <th>Cooldown</th>
            <th>Runtime</th>
            <th>Node Health</th>
            <th>Timeline</th>
            <th>Details</th>
          </tr>
        </thead>
        <tbody id="client-rows">
          <tr><td class="empty" colspan="8">Loading...</td></tr>
        </tbody>
      </table>
    </section>
    """


def _client_script() -> str:
    return """
    const clientState = { payload: null };

    function selectedClientValue() {
      const manual = document.getElementById("client-input").value.trim();
      if (manual) return manual;
      return document.getElementById("client-select").value;
    }

    function renderClientSummary(payload) {
      const rows = payload.nodes || [];
      const engaged = rows.filter((row) => row.client_state !== "UNSEEN");
      const open = rows.filter((row) => row.client_state === "OPEN").length;
      const half = rows.filter((row) => row.client_state === "HALF_OPEN").length;
      const closed = rows.filter((row) => row.client_state === "CLOSED").length;
      const wrap = document.getElementById("client-summary");
      const cards = [
        ["Selected Client", payload.selected_client || "-"],
        ["Known Clients", payload.known_clients.length],
        ["Tracked Nodes", rows.length],
        ["Seen By Client", engaged.length],
        ["Open Circuits", open],
        ["Half Open", half],
        ["Closed", closed],
        ["VIP Port", payload.vip?.enabled ? payload.vip.port : "disabled"],
        ["Network", payload.network?.enabled ? (payload.network.online ? "online" : "offline") : "disabled"],
      ];
      wrap.innerHTML = cards.map(([label, value]) => `
        <article class="card">
          <div class="label">${esc(label)}</div>
          <div class="value">${esc(value)}</div>
        </article>
      `).join("");
    }

    function renderClientSelector(payload) {
      const select = document.getElementById("client-select");
      const current = payload.selected_client || select.value;
      select.innerHTML = ['<option value="">Known clients</option>'].concat(
        payload.known_clients.map((client) => `<option value="${esc(client)}"${client === current ? " selected" : ""}>${esc(client)}</option>`)
      ).join("");
      if (payload.selected_client) {
        document.getElementById("client-input").value = payload.selected_client;
      }
    }

    function renderClientRows() {
      const payload = clientState.payload;
      const tbody = document.getElementById("client-rows");
      if (!payload) return;
      const rows = payload.nodes || [];
      if (!rows.length) {
        tbody.innerHTML = '<tr><td class="empty" colspan="8">No nodes are available yet for this client view.</td></tr>';
        return;
      }

      tbody.innerHTML = rows.map((node) => `
        <tr>
          <td class="mono">${esc(node.protocol)}<br>${esc(node.server)}:${esc(node.remote_port)}<br><strong>${esc(node.remark || "-")}</strong></td>
          <td class="mono"><span class="status ${esc(node.client_state)}">${esc(node.client_state)}</span><br>fail streak: ${esc(node.client_fail_streak)}<br>rate-limit streak: ${esc(node.client_rate_limit_streak)}<br>success ewma: ${esc(node.success_rate_ewma)}</td>
          <td class="mono">assignments: ${esc(node.client_total_assignments)}<br>open: ${esc(node.client_open_assignments)}<br>used/broken/rl: ${esc(node.client_used_feedback_count)}/${esc(node.client_broken_feedback_count)}/${esc(node.client_rate_limited_feedback_count)}</td>
          <td class="mono">cooldown: ${esc(prettyDate(node.cooldown_until))}<br>recent usage: ${esc(node.recent_usage_score)}<br>usage count: ${esc(node.usage_count)}</td>
          <td class="mono">node status: <span class="status ${esc(node.status)}">${esc(node.status)}</span><br>running: ${esc(node.runtime_running)}<br>local: ${esc(metric(node.main_port))}</td>
          <td class="mono">latency: ${esc(metric(node.relay_delay_ms, " ms"))}<br>speed: ${esc(metric(node.download_kbps, " kbps"))}<br>health ewma: ${esc(node.health_success_ewma)}</td>
          <td class="mono">assigned: ${esc(prettyDate(node.last_assigned_at))}<br>feedback: ${esc(prettyDate(node.last_feedback_at))}<br>success: ${esc(prettyDate(node.last_success_at))}<br>failure: ${esc(prettyDate(node.last_failure_at))}</td>
          <td>
            <details>
              <summary>View config</summary>
              <pre>${esc(JSON.stringify({ node_id: node.id, config_hash: node.config_hash, normalized_config: node.normalized_config, raw_config: node.raw_config }, null, 2))}</pre>
            </details>
          </td>
        </tr>
      `).join("");
    }

    async function refreshClient(forceClient) {
      const chosen = forceClient ?? selectedClientValue();
      const query = chosen ? `?client=${encodeURIComponent(chosen)}` : "";
      const response = await fetch(`/api/v1/client-status${query}`, { headers: { "Accept": "application/json" } });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      clientState.payload = await response.json();
      renderClientSummary(clientState.payload);
      renderClientSelector(clientState.payload);
      renderClientRows();
      document.getElementById("client-last-refresh").textContent = `Last refresh: ${new Date().toLocaleTimeString()}`;
      const url = new URL(window.location.href);
      if (clientState.payload.selected_client) {
        url.searchParams.set("client", clientState.payload.selected_client);
      } else {
        url.searchParams.delete("client");
      }
      window.history.replaceState({}, "", url);
    }

    function showClientError(error) {
      document.getElementById("client-rows").innerHTML = `<tr><td class="empty" colspan="8">Failed to load client view: ${esc(error.message)}</td></tr>`;
    }

    document.getElementById("client-select").addEventListener("change", () => {
      const value = document.getElementById("client-select").value;
      document.getElementById("client-input").value = value;
      refreshClient(value).catch(showClientError);
    });
    document.getElementById("client-load-btn").addEventListener("click", () => refreshClient().catch(showClientError));
    document.getElementById("client-copy-btn").addEventListener("click", async () => {
      const url = new URL(window.location.href);
      const client = selectedClientValue();
      if (client) {
        url.searchParams.set("client", client);
      } else {
        url.searchParams.delete("client");
      }
      await navigator.clipboard.writeText(url.toString());
    });

    const bootstrapClient = new URL(window.location.href).searchParams.get("client") || "";
    if (bootstrapClient) {
      document.getElementById("client-input").value = bootstrapClient;
    }
    refreshClient(bootstrapClient).catch(showClientError);
    setInterval(() => refreshClient().catch(showClientError), 15000);
    """


def _history_script() -> str:
    return """
    function renderHistoryCards(payload) {
      const node = payload.node || {};
      const wrap = document.getElementById("history-hero");
      const cards = [
        ["Node", node.id || "-"],
        ["Status", node.status || "-"],
        ["Country", node.exit_country || "-"],
        ["Remark", node.remark || "-"],
        ["Proxy", node.server ? `${node.protocol} ${node.server}:${node.remote_port}` : "-"],
        ["Records", (payload.history || []).length],
      ];
      wrap.innerHTML = cards.map(([label, value]) => `
        <article class="card">
          <div class="label">${esc(label)}</div>
          <div class="value" style="font-size:18px;line-height:1.25;">${esc(value)}</div>
        </article>
      `).join("");
    }

    function renderHistoryList(items) {
      const wrap = document.getElementById("history-list");
      if (!items || !items.length) {
        wrap.innerHTML = '<article class="panel doc-card"><p class="muted">No test history recorded yet for this node.</p></article>';
        return;
      }
      wrap.innerHTML = items.map((item) => {
        const meta = [
          ["Finished", prettyDate(item.finished_at)],
          ["Started", prettyDate(item.started_at)],
          ["Latency", metric(item.latency_ms, " ms")],
          ["Speed", metric(item.download_kbps, " kbps")],
          ["Network", String(item.network_online)],
          ["State", `${item.status_before} -> ${item.status_after}`],
        ];
        const metaHtml = meta.map(([label, value]) => `<div><span class="label">${esc(label)}</span><div class="mono">${esc(value)}</div></div>`).join("");
        return `
          <article class="history-item">
            <h4>${esc(item.ok ? "PASS" : "FAIL")} · ${esc(item.trigger)} · ${esc(item.test_kind)}</h4>
            <div class="history-meta">${metaHtml}</div>
            <pre>${esc(JSON.stringify({ error: item.error, details: item.details }, null, 2))}</pre>
          </article>
        `;
      }).join("");
    }

    async function loadHistory(nodeId) {
      if (!nodeId) return;
      const response = await fetch(`/api/v1/nodes/${encodeURIComponent(nodeId)}/history?limit=120`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const payload = await response.json();
      renderHistoryCards(payload);
      renderHistoryList(payload.history || []);
      document.getElementById("history-last-refresh").textContent = `Last refresh: ${new Date().toLocaleTimeString()}`;
    }

    document.getElementById("history-load-btn").addEventListener("click", async () => {
      const nodeId = document.getElementById("history-node-input").value.trim();
      if (!nodeId) return;
      const url = new URL(window.location.href);
      url.searchParams.set("node", nodeId);
      window.history.replaceState({}, "", url);
      await loadHistory(nodeId);
    });

    const bootstrapNode = new URL(window.location.href).searchParams.get("node") || "";
    document.getElementById("history-node-input").value = bootstrapNode;
    if (bootstrapNode) {
      loadHistory(bootstrapNode).catch((error) => {
        document.getElementById("history-list").innerHTML = `<article class="panel doc-card"><p class="muted">Failed to load history: ${esc(error.message)}</p></article>`;
      });
    }
    """


def _manual_import_script() -> str:
    return """
    document.getElementById("manual-import-submit").addEventListener("click", async () => {
      const button = document.getElementById("manual-import-submit");
      const resultBox = document.getElementById("manual-import-result");
      const text = document.getElementById("manual-import-text").value || "";
      button.disabled = true;
      button.textContent = "Importing...";
      resultBox.textContent = "";
      try {
        const response = await fetch("/api/v1/manual-import", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ configs: text }),
        });
        const payload = await response.json();
        if (!response.ok) throw new Error(payload.error || `HTTP ${response.status}`);
        resultBox.innerHTML = `<pre>${esc(JSON.stringify(payload, null, 2))}</pre>`;
      } catch (error) {
        resultBox.innerHTML = `<p class="muted">Import failed: ${esc(error.message)}</p>`;
      } finally {
        button.disabled = false;
        button.textContent = "Import to Candidate";
      }
    });
    """


def _docs_body() -> str:
    return """
    <section class="stack">
      <article class="panel doc-card">
        <h3>GET /health</h3>
        <p>Liveness and readiness probe used by Docker healthcheck.</p>
        <pre>curl http://127.0.0.1:8080/health</pre>
      </article>
      <article class="panel doc-card">
        <h3>GET /api/v1/nodes</h3>
        <p>Fleet-wide proxy inventory with latest runtime, relay, assignment, client aggregate metrics, network guard state, and VIP state.</p>
        <pre>curl http://127.0.0.1:8080/api/v1/nodes</pre>
      </article>
      <article class="panel doc-card">
        <h3>GET /api/v1/client-status?client=&lt;id&gt;</h3>
        <p>Client-specific circuit and feedback lens across the full proxy fleet.</p>
        <pre>curl 'http://127.0.0.1:8080/api/v1/client-status?client=telegram-bot-1'</pre>
      </article>
      <article class="panel doc-card">
        <h3>GET /api/v1/network</h3>
        <p>Current host online/offline state from the sentinel checks, including streaks and last error.</p>
        <pre>curl http://127.0.0.1:8080/api/v1/network</pre>
      </article>
      <article class="panel doc-card">
        <h3>GET /api/v1/vip</h3>
        <p>Current global VIP relay status and the fixed hot port bound to the best global node.</p>
        <pre>curl http://127.0.0.1:8080/api/v1/vip</pre>
      </article>
      <article class="panel doc-card">
        <h3>GET /api/v1/logs?limit=200&amp;component=&lt;name&gt;&amp;level=&lt;level&gt;</h3>
        <p>Newest internal worker events, including network sentinel, subscription cycles, candidate batches, health checks, dead-pool cleanup, and VIP manager runs.</p>
        <pre>curl 'http://127.0.0.1:8080/api/v1/logs?limit=200'
curl 'http://127.0.0.1:8080/api/v1/logs?component=candidate&amp;level=info&amp;limit=100'</pre>
      </article>
      <article class="panel doc-card">
        <h3>GET /api/v1/nodes/&lt;node_id&gt;/history?limit=120</h3>
        <p>Full test history for one node, newest first.</p>
        <pre>curl 'http://127.0.0.1:8080/api/v1/nodes/NODE_ID/history?limit=120'</pre>
      </article>
      <article class="panel doc-card">
        <h3>POST /api/v1/best</h3>
        <p>Return the best currently available node for a client.</p>
        <pre>curl -X POST http://127.0.0.1:8080/api/v1/best \
  -H 'Content-Type: application/json' \
  -d '{
    "client": "telegram-bot-1"
  }'</pre>
      </article>
      <article class="panel doc-card">
        <h3>POST /api/v1/feedback</h3>
        <p>Report whether the assigned proxy worked, broke, or got rate limited.</p>
        <pre>curl -X POST http://127.0.0.1:8080/api/v1/feedback \
  -H 'Content-Type: application/json' \
  -d '{
    "client": "telegram-bot-1",
    "node_id": "NODE_ID",
    "status": "used"
  }'</pre>
      </article>
      <article class="panel doc-card">
        <h3>POST /api/v1/nodes/&lt;node_id&gt;/test</h3>
        <p>Run the fast two-stage test for a specific node: relay first, then download only if relay passes.</p>
        <pre>curl -X POST http://127.0.0.1:8080/api/v1/nodes/NODE_ID/test \
  -H 'Content-Type: application/json' \
  -d '{}'</pre>
      </article>
      <article class="panel doc-card">
        <h3>POST /api/v1/subscriptions/reload</h3>
        <p>Pull all configured subscriptions immediately and run the normal ingest/test flow for new configs.</p>
        <pre>curl -X POST http://127.0.0.1:8080/api/v1/subscriptions/reload \
  -H 'Content-Type: application/json' \
  -d '{}'</pre>
      </article>
      <article class="panel doc-card">
        <h3>POST /api/v1/nodes/dead/clear</h3>
        <p>Delete every node currently in the dead pool, including its stored history and event rows.</p>
        <pre>curl -X POST http://127.0.0.1:8080/api/v1/nodes/dead/clear \
  -H 'Content-Type: application/json' \
  -d '{}'</pre>
      </article>
      <article class="panel doc-card">
        <h3>POST /api/v1/db/cleanup</h3>
        <p>Remove accumulated test history and event logs, then compact the SQLite database.</p>
        <pre>curl -X POST http://127.0.0.1:8080/api/v1/db/cleanup \
  -H 'Content-Type: application/json' \
  -d '{}'</pre>
      </article>
      <article class="panel doc-card">
        <h3>POST /api/v1/manual-import</h3>
        <p>Paste newline-separated share links and queue them into the candidate pool using the same parsing and dedupe rules as subscription configs.</p>
        <pre>curl -X POST http://127.0.0.1:8080/api/v1/manual-import \
  -H 'Content-Type: application/json' \
  -d '{
    "configs": "vmess://...\\nvless://...\\nss://..."
  }'</pre>
      </article>
      <article class="panel doc-card">
        <h3>UI Routes</h3>
        <pre>/
/clients
/diag
/docs
/logs
/history?node=NODE_ID</pre>
      </article>
      <article class="panel doc-card">
        <h3>Best Node Response</h3>
        <pre>{
  "node_id": "0b89c3...",
  "port": 20112,
  "client": "telegram-bot-1",
  "assignment_id": "7d0a6f...",
  "relay_delay_ms": 284,
  "expires_in_seconds": 60
}</pre>
      </article>
    </section>
    """
