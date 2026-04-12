from __future__ import annotations


def get_debug_dashboard_html() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MachinoCare Realtime Debug Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    :root {
      --bg: radial-gradient(circle at 15% 10%, #eef7ff 0%, #f8f3e1 42%, #eef8f2 100%);
      --ink: #10263a;
      --muted: #4b6477;
      --card: rgba(255, 255, 255, 0.95);
      --border: rgba(16, 38, 58, 0.18);
      --ok: #15803d;
      --warn: #c2410c;
      --bad: #b91c1c;
      --link: #0f5cab;
    }

    body {
      margin: 0;
      font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
      color: var(--ink);
      background: var(--bg);
    }

    .wrap {
      max-width: 1320px;
      margin: 1.2rem auto;
      padding: 0 1rem 2rem;
    }

    h1 {
      margin: 0 0 0.35rem;
      letter-spacing: 0.02em;
      font-size: 1.9rem;
    }

    .sub {
      margin: 0 0 1rem;
      color: var(--muted);
      font-size: 0.95rem;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 0.8rem;
    }

    .card {
      background: var(--card);
      border: 1px solid var(--border);
      border-radius: 14px;
      box-shadow: 0 10px 24px rgba(16, 38, 58, 0.08);
      padding: 0.75rem 0.9rem;
    }

    .controls { grid-column: span 12; }
    .status { grid-column: span 12; }
    .chart { grid-column: span 8; }
    .profiles { grid-column: span 4; }
    .logs { grid-column: span 8; }
    .payload { grid-column: span 4; }

    @media (max-width: 1050px) {
      .chart, .profiles, .logs, .payload { grid-column: span 12; }
    }

    .row {
      display: flex;
      gap: 0.6rem;
      flex-wrap: wrap;
      align-items: center;
      margin: 0.35rem 0;
    }

    label {
      font-size: 0.8rem;
      color: var(--muted);
      margin-right: 0.3rem;
    }

    input, textarea, select, button {
      font: inherit;
      border-radius: 8px;
      border: 1px solid #c7d4de;
      padding: 0.35rem 0.5rem;
      background: #fff;
    }

    textarea { min-height: 70px; width: 100%; }

    button {
      cursor: pointer;
      background: linear-gradient(180deg, #eff5fb, #e8f1f9);
      color: var(--ink);
      border-color: #b8cde0;
      font-weight: 600;
    }

    button.primary {
      background: linear-gradient(180deg, #0f5cab, #0e4d90);
      color: #fff;
      border-color: #0c4179;
    }

    .pill {
      display: inline-block;
      border-radius: 999px;
      padding: 0.2rem 0.7rem;
      font-size: 0.8rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
    }

    .pill.ok { color: var(--ok); border: 1px solid #86efac; background: #f0fdf4; }
    .pill.warn { color: var(--warn); border: 1px solid #fdba74; background: #fff7ed; }
    .pill.bad { color: var(--bad); border: 1px solid #fca5a5; background: #fef2f2; }

    .mini {
      display: grid;
      grid-template-columns: repeat(5, minmax(120px, 1fr));
      gap: 0.5rem;
    }

    .mini .m {
      border: 1px solid #d9e4ed;
      border-radius: 10px;
      background: #fff;
      padding: 0.45rem 0.55rem;
    }

    .m .k { color: var(--muted); font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.06em; }
    .m .v { margin-top: 0.2rem; font-weight: 700; font-size: 1.02rem; }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.84rem;
    }

    th, td {
      border-bottom: 1px solid #e0e8ef;
      padding: 0.42rem 0.35rem;
      vertical-align: top;
      text-align: left;
    }

    tr:hover { background: #f7fbff; }

    .scroll {
      max-height: 360px;
      overflow: auto;
      border: 1px solid #dbe7ef;
      border-radius: 8px;
      background: #fff;
    }

    pre {
      margin: 0;
      white-space: pre-wrap;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
      font-size: 0.77rem;
      line-height: 1.35;
    }

    .hint {
      color: var(--muted);
      font-size: 0.78rem;
    }

    a { color: var(--link); }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>MachinoCare Realtime Debug Dashboard</h1>
    <p class="sub">No-flicker live dashboard for backend + ESP telemetry. Current firmware stays unchanged.</p>

    <div class="grid">
      <section class="card controls">
        <div class="row">
          <label for="profileSelect">Profile</label>
          <select id="profileSelect" style="min-width:260px;"></select>
          <span id="profileSwitching" class="hint" style="display:none;">Switching profile...</span>
          <label for="lookback">Lookback seconds</label>
          <input id="lookback" type="number" value="120" min="10" max="86400" />
          <button id="connectBtn" class="primary">Connect Live Feed</button>
          <button id="disconnectBtn">Disconnect</button>
          <span id="connState" class="pill warn">disconnected</span>
        </div>
        <div class="row" id="fieldControls"></div>
      </section>

      <section class="card status">
        <div class="mini">
          <div class="m"><div class="k">Health</div><div class="v" id="statusLabel">UNKNOWN</div></div>
          <div class="m"><div class="k">Acc</div><div class="v" id="accVal">n/a</div></div>
          <div class="m"><div class="k">Score</div><div class="v" id="scoreVal">n/a</div></div>
          <div class="m"><div class="k">Threshold</div><div class="v" id="thrVal">n/a</div></div>
          <div class="m"><div class="k">Calibration</div><div class="v" id="calibVal">idle</div></div>
        </div>
      </section>

      <section class="card chart">
        <div id="chart" style="height:420px;"></div>
      </section>

      <section class="card profiles">
        <h3 style="margin:0 0 0.5rem;">Device Profile + Calibration</h3>
        <div class="row">
          <button id="saveProfileBtn" class="primary">Save Profile</button>
          <button id="startCalibBtn">Start Calibration</button>
          <button id="reloadProfilesBtn">Reload Profiles</button>
        </div>
        <div class="row">
          <button id="associateBtn" class="primary">Associate Stream</button>
          <button id="clearAssocBtn">Clear Association</button>
          <button id="deleteProfileBtn">Delete Profile</button>
        </div>
        <div class="row"><label for="displayName">Display name</label><input id="displayName" style="min-width:220px;" /></div>
        <div class="row"><label for="newDisplayName">New profile</label><input id="newDisplayName" style="min-width:220px;" /><button id="createProfileBtn">Create Profile</button></div>
        <div class="row"><label for="sr">sample_rate_hz</label><input id="sr" type="number" value="10" min="1" max="500" /></div>
        <div class="row"><label for="ws">window_seconds</label><input id="ws" type="number" value="1" min="1" max="10" /></div>
        <div class="row"><label for="fb">fallback_seconds</label><input id="fb" type="number" value="300" min="10" max="86400" /></div>
        <div class="row"><label for="cont">contamination</label><input id="cont" type="number" value="0.05" min="0.01" max="0.40" step="0.01" /></div>
        <div class="row"><label for="minw">min_consecutive_windows</label><input id="minw" type="number" value="3" min="1" max="10" /></div>
        <div class="row" style="align-items:flex-start;">
          <div style="width:100%;">
            <label for="notes">notes</label>
            <textarea id="notes"></textarea>
          </div>
        </div>
        <div class="hint" id="bindingHint">Active stream association: none.</div>
        <div class="hint" id="profileHint">Select a profile by display name to manage settings.</div>
      </section>

      <section class="card logs">
        <div class="row" style="justify-content:space-between;">
          <h3 style="margin:0;">Live API Logs (ESP/backend)</h3>
          <span class="hint">Newest first. Click a row to inspect payloads.</span>
        </div>
        <div class="scroll">
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Method</th>
                <th>Endpoint</th>
                <th>Status</th>
                <th>Latency</th>
                <th>Machine</th>
                <th>Device</th>
              </tr>
            </thead>
            <tbody id="logsBody"></tbody>
          </table>
        </div>
      </section>

      <section class="card payload">
        <h3 style="margin:0 0 0.5rem;">Selected Log Payload</h3>
        <div class="scroll" style="max-height:360px;">
          <pre id="payloadView">Select a log row to inspect request/response payloads.</pre>
        </div>
      </section>
    </div>
  </div>

  <script>
    const fieldConfig = [
      { key: 'acc_mag', label: 'acc_mag', color: '#0f62fe' },
      { key: 'gyro_mag', label: 'gyro_mag', color: '#0e9f6e' },
      { key: 'gx', label: 'gx', color: '#8b5cf6' },
      { key: 'gy', label: 'gy', color: '#f59e0b' },
      { key: 'gz', label: 'gz', color: '#ef4444' },
      { key: 'sw420', label: 'sw420', color: '#334155' },
      { key: 'score', label: 'score', color: '#b91c1c' },
      { key: 'decision_threshold', label: 'decision_threshold', color: '#0f172a' }
    ];

    const traces = {};
    const timestamps = [];
    const logs = [];
    const profilesByKey = new Map();

    let ws = null;
    let lastTimestamp = '';
    let lastLogId = 0;
    let activeProfileKey = '';

    function el(id) { return document.getElementById(id); }

    function profileKey(machine, device) {
      return `${String(machine || '').trim()}::${String(device || '').trim()}`;
    }

    function slugifyName(name) {
      const slug = String(name || '').toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
      return slug || 'profile';
    }

    function currentProfile() {
      if (!activeProfileKey) return null;
      return profilesByKey.get(activeProfileKey) || null;
    }

    function setProfileSwitching(active, text) {
      const node = el('profileSwitching');
      if (!node) return;
      node.style.display = active ? 'inline-block' : 'none';
      node.textContent = text || 'Switching profile...';
    }

    function toWsUrl(machine, device, lookback) {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const qp = new URLSearchParams({ machine_id: machine, device_id: device, lookback_seconds: String(lookback), last_log_id: String(lastLogId) });
      return `${protocol}//${window.location.host}/api/v1/ws/live?${qp.toString()}`;
    }

    function setConnState(state, variant) {
      const target = el('connState');
      target.textContent = state;
      target.className = `pill ${variant}`;
    }

    function selectedFields() {
      return fieldConfig.filter(f => el(`f_${f.key}`) && el(`f_${f.key}`).checked).map(f => f.key);
    }

    function renderFieldControls() {
      const root = el('fieldControls');
      root.innerHTML = '';
      for (const field of fieldConfig) {
        const id = `f_${field.key}`;
        const wrap = document.createElement('span');
        wrap.innerHTML = `<label style="display:flex; align-items:center; gap:0.25rem;"><input id="${id}" type="checkbox" checked/>${field.label}</label>`;
        root.appendChild(wrap);
        setTimeout(() => {
          const control = el(id);
          if (control) control.addEventListener('change', renderChart);
        }, 0);
      }
    }

    function clearProfileForm() {
      el('displayName').value = '';
      el('sr').value = 10;
      el('ws').value = 1;
      el('fb').value = 300;
      el('cont').value = 0.05;
      el('minw').value = 3;
      el('notes').value = '';
    }

    function applyProfileToForm(profile) {
      if (!profile) {
        clearProfileForm();
        return;
      }

      el('displayName').value = profile.display_name || profile.display_label || '';
      el('sr').value = profile.sample_rate_hz ?? 10;
      el('ws').value = profile.window_seconds ?? 1;
      el('fb').value = profile.fallback_seconds ?? 300;
      el('cont').value = profile.contamination ?? 0.05;
      el('minw').value = profile.min_consecutive_windows ?? 3;
      el('notes').value = profile.notes || '';
    }

    function appendSample(sample, status) {
      if (!sample || !sample.timestamp || sample.timestamp === lastTimestamp) return;
      lastTimestamp = sample.timestamp;
      timestamps.push(sample.timestamp);

      const values = {
        acc_mag: sample.acc_mag,
        gyro_mag: sample.gyro_mag,
        gx: sample.gx,
        gy: sample.gy,
        gz: sample.gz,
        sw420: sample.sw420,
        score: status && status.current ? status.current.score : null,
        decision_threshold: status && status.current ? status.current.decision_threshold : null,
      };

      for (const cfg of fieldConfig) {
        if (!traces[cfg.key]) traces[cfg.key] = [];
        traces[cfg.key].push(values[cfg.key] ?? null);
        if (traces[cfg.key].length > 600) traces[cfg.key].shift();
      }

      if (timestamps.length > 600) timestamps.shift();
    }

    function renderChart() {
      const fields = selectedFields();
      const plotData = [];
      for (const cfg of fieldConfig) {
        if (!fields.includes(cfg.key)) continue;
        plotData.push({
          x: timestamps,
          y: traces[cfg.key] || [],
          name: cfg.label,
          mode: 'lines',
          line: { width: 2, color: cfg.color }
        });
      }

      Plotly.react('chart', plotData, {
        margin: { l: 42, r: 18, t: 30, b: 35 },
        template: 'plotly_white',
        legend: { orientation: 'h' },
        xaxis: { title: 'timestamp' },
        yaxis: { title: 'value' },
        hovermode: 'x unified'
      }, { displayModeBar: true, responsive: true });
    }

    function updateStatus(status) {
      if (!status) return;
      const label = status.status_label || 'UNKNOWN';
      el('statusLabel').textContent = label;
      el('accVal').textContent = status.current && status.current.acc_mag != null ? Number(status.current.acc_mag).toFixed(2) : 'n/a';
      el('scoreVal').textContent = status.current && status.current.score != null ? Number(status.current.score).toFixed(3) : 'n/a';
      el('thrVal').textContent = status.current && status.current.decision_threshold != null ? Number(status.current.decision_threshold).toFixed(3) : 'n/a';

      const cal = status.calibration || {};
      const stage = cal.stage || 'idle';
      const progress = cal.progress == null ? 0 : cal.progress;
      el('calibVal').textContent = `${stage} (${progress}%)`;
    }

    function renderLogs() {
      const body = el('logsBody');
      body.innerHTML = '';
      const top = logs.slice(0, 200);
      for (const entry of top) {
        const tr = document.createElement('tr');
        tr.dataset.logId = String(entry.id);
        tr.innerHTML = `
          <td>${entry.created_at || ''}</td>
          <td>${entry.method || ''}</td>
          <td>${entry.endpoint || ''}</td>
          <td>${entry.status_code == null ? '' : entry.status_code}</td>
          <td>${entry.latency_ms == null ? '' : entry.latency_ms + 'ms'}</td>
          <td>${entry.machine_id || ''}</td>
          <td>${entry.device_id || ''}</td>
        `;
        tr.addEventListener('click', () => {
          el('payloadView').textContent = JSON.stringify({
            id: entry.id,
            created_at: entry.created_at,
            endpoint: entry.endpoint,
            method: entry.method,
            status_code: entry.status_code,
            latency_ms: entry.latency_ms,
            correlation_id: entry.correlation_id,
            error_text: entry.error_text,
            request_payload: entry.request_payload,
            response_payload: entry.response_payload
          }, null, 2);
        });
        body.appendChild(tr);
      }
    }

    function mergeLogs(newLogs) {
      if (!Array.isArray(newLogs) || newLogs.length === 0) return;
      for (const item of newLogs) {
        logs.unshift(item);
        if (item.id && item.id > lastLogId) lastLogId = item.id;
      }
      const dedup = new Map();
      for (const item of logs) {
        if (!dedup.has(item.id)) dedup.set(item.id, item);
      }
      logs.length = 0;
      logs.push(...dedup.values());
      if (logs.length > 400) logs.length = 400;
      renderLogs();
    }

    function subscribeMessage(machine, device, lookback) {
      return {
        type: 'subscribe',
        machine_id: machine,
        device_id: device,
        lookback_seconds: Number(lookback || 120),
        last_log_id: Number(lastLogId || 0)
      };
    }

    function updateBindingHint(binding) {
      if (binding && binding.is_active && binding.machine_id && binding.device_id) {
        const key = profileKey(binding.machine_id, binding.device_id);
        const profile = profilesByKey.get(key);
        const label = profile ? profile.display_label : `${binding.machine_id}/${binding.device_id}`;
        el('bindingHint').textContent = `Active stream association: ${label}`;
        return;
      }
      el('bindingHint').textContent = 'Active stream association: none.';
    }

    async function refreshBinding() {
      const response = await fetch('/api/v1/stream-binding');
      if (!response.ok) {
        updateBindingHint(null);
        return null;
      }

      const binding = await response.json();
      updateBindingHint(binding);
      return binding;
    }

    async function loadProfiles(preferredKey = '') {
      setProfileSwitching(true, 'Loading profiles...');
      const select = el('profileSelect');
      select.innerHTML = '';
      profilesByKey.clear();

      const response = await fetch('/api/v1/device-profiles?limit=500');
      if (!response.ok) {
        activeProfileKey = '';
        clearProfileForm();
        el('profileHint').textContent = 'Failed to load profiles.';
        setProfileSwitching(false);
        return;
      }

      const data = await response.json();
      const rows = Array.isArray(data.profiles) ? data.profiles : [];
      const nameCounts = new Map();

      rows.forEach((row, index) => {
        const machine = String(row.machine_id || '').trim();
        const device = String(row.device_id || '').trim();
        if (!machine || !device) return;

        const base = String(row.display_name || '').trim() || `Unnamed profile ${index + 1}`;
        const count = (nameCounts.get(base) || 0) + 1;
        nameCounts.set(base, count);
        const display_label = count === 1 ? base : `${base} #${count}`;
        const key = profileKey(machine, device);

        const profile = { ...row, machine_id: machine, device_id: device, key, display_label };
        profilesByKey.set(key, profile);
        select.add(new Option(display_label, key));
      });

      if (profilesByKey.size === 0) {
        activeProfileKey = '';
        clearProfileForm();
        el('profileHint').textContent = 'No profiles found. Create one to begin.';
        setProfileSwitching(false);
        return;
      }

      let nextKey = preferredKey && profilesByKey.has(preferredKey) ? preferredKey : activeProfileKey;
      if (!profilesByKey.has(nextKey)) {
        nextKey = select.options[0].value;
      }
      select.value = nextKey;
      activeProfileKey = nextKey;
      applyProfileToForm(currentProfile());
      setProfileSwitching(false);
    }

    function connectLive() {
      const profile = currentProfile();
      if (!profile) {
        el('profileHint').textContent = 'Select or create a profile first.';
        return;
      }

      const machine = profile.machine_id;
      const device = profile.device_id;
      const lookback = Number(el('lookback').value || 120);

      if (ws) {
        ws.close();
        ws = null;
      }

      setConnState('connecting', 'warn');
      ws = new WebSocket(toWsUrl(machine, device, lookback));

      ws.onopen = () => {
        setConnState('connected', 'ok');
        ws.send(JSON.stringify(subscribeMessage(machine, device, lookback)));
      };

      ws.onmessage = (evt) => {
        const packet = JSON.parse(evt.data);
        if (packet.type === 'connected') {
          updateBindingHint(packet.active_stream_binding || null);
        }
        if (packet.type === 'snapshot') {
          if (packet.latest_sample) appendSample(packet.latest_sample, packet.status || null);
          if (packet.status) updateStatus(packet.status);
          updateBindingHint(packet.active_stream_binding || null);
          mergeLogs(packet.new_logs || []);
          renderChart();
        }
        if (packet.type === 'error') {
          setConnState('error', 'bad');
          el('payloadView').textContent = JSON.stringify(packet, null, 2);
        }
      };

      ws.onclose = () => {
        ws = null;
        setConnState('disconnected', 'warn');
      };
      ws.onerror = () => setConnState('error', 'bad');
    }

    async function onProfileSelectionChange() {
      const key = el('profileSelect').value;
      if (!key || !profilesByKey.has(key)) return;

      setProfileSwitching(true, 'Switching profile...');
      activeProfileKey = key;
      applyProfileToForm(currentProfile());
      await refreshBinding();
      if (ws) connectLive();
      setProfileSwitching(false);
    }

    async function saveProfile() {
      const profile = currentProfile();
      if (!profile) {
        el('profileHint').textContent = 'Select a profile before saving.';
        return;
      }

      const payload = {
        machine_id: profile.machine_id,
        device_id: profile.device_id,
        display_name: el('displayName').value.trim() || null,
        sample_rate_hz: Number(el('sr').value),
        window_seconds: Number(el('ws').value),
        fallback_seconds: Number(el('fb').value),
        contamination: Number(el('cont').value),
        min_consecutive_windows: Number(el('minw').value),
        notes: el('notes').value.trim() || null,
      };

      const response = await fetch('/api/v1/device-profiles', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      const text = await response.text();
      if (!response.ok) {
        el('profileHint').textContent = `Save failed: ${text}`;
        return;
      }

      const result = JSON.parse(text);
      const key = profileKey(result.machine_id, result.device_id);
      await loadProfiles(key);
      await refreshBinding();
      el('profileHint').textContent = `Saved profile ${result.display_name || 'updated'}`;
    }

    async function createProfile() {
      const displayName = el('newDisplayName').value.trim();
      if (!displayName) {
        el('profileHint').textContent = 'Enter a display name for the new profile.';
        return;
      }

      const existingNames = Array.from(profilesByKey.values())
        .map(item => String(item.display_name || '').trim().toLowerCase())
        .filter(Boolean);
      if (existingNames.includes(displayName.toLowerCase())) {
        el('profileHint').textContent = 'A profile with that display name already exists.';
        return;
      }

      const base = slugifyName(displayName);
      let suffix = 1;
      let machine = '';
      let device = '';
      while (true) {
        machine = suffix === 1 ? base : `${base}_${suffix}`;
        device = `${machine}_device`;
        if (!profilesByKey.has(profileKey(machine, device))) break;
        suffix += 1;
      }

      const payload = {
        machine_id: machine,
        device_id: device,
        display_name: displayName,
        sample_rate_hz: Number(el('sr').value),
        window_seconds: Number(el('ws').value),
        fallback_seconds: Number(el('fb').value),
        contamination: Number(el('cont').value),
        min_consecutive_windows: Number(el('minw').value),
        notes: el('notes').value.trim() || null,
      };

      const response = await fetch('/api/v1/device-profiles', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });

      const text = await response.text();
      if (!response.ok) {
        el('profileHint').textContent = `Create failed: ${text}`;
        return;
      }

      const created = JSON.parse(text);
      const key = profileKey(created.machine_id, created.device_id);
      el('newDisplayName').value = '';
      await loadProfiles(key);
      await refreshBinding();
      el('profileHint').textContent = `Created profile ${created.display_name || displayName}`;
    }

    async function startCalibration() {
      const profile = currentProfile();
      if (!profile) {
        el('profileHint').textContent = 'Select a profile before starting calibration.';
        return;
      }

      const response = await fetch(`/api/v1/calibrate/start/profile/${encodeURIComponent(profile.machine_id)}/${encodeURIComponent(profile.device_id)}?new_device_setup=true&trigger_source=debug_dashboard`, {
        method: 'POST'
      });

      const text = await response.text();
      if (!response.ok) {
        el('profileHint').textContent = `Calibration trigger failed: ${text}`;
        return;
      }

      const payload = JSON.parse(text);
      el('profileHint').textContent = `Calibration job ${payload.job_id} (${payload.status})`;
    }

    async function associateStream() {
      const profile = currentProfile();
      if (!profile) {
        el('profileHint').textContent = 'Select a profile before associating stream data.';
        return;
      }

      const response = await fetch('/api/v1/stream-binding', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          machine_id: profile.machine_id,
          device_id: profile.device_id,
          source: 'debug_dashboard',
        }),
      });
      const text = await response.text();
      if (!response.ok) {
        el('profileHint').textContent = `Association failed: ${text}`;
        return;
      }

      const binding = JSON.parse(text);
      updateBindingHint(binding);
      el('profileHint').textContent = `Incoming stream now routes to ${profile.display_label}`;
    }

    async function clearAssociation() {
      const response = await fetch('/api/v1/stream-binding?source=debug_dashboard', {
        method: 'DELETE',
      });
      const text = await response.text();
      if (!response.ok) {
        el('profileHint').textContent = `Clear association failed: ${text}`;
        return;
      }

      const binding = JSON.parse(text);
      updateBindingHint(binding);
      el('profileHint').textContent = 'Incoming stream association cleared.';
    }

    async function deleteProfile() {
      const profile = currentProfile();
      if (!profile) {
        el('profileHint').textContent = 'Select a profile before deleting.';
        return;
      }
      if (!window.confirm(`Delete profile ${profile.display_label}?`)) {
        return;
      }

      const response = await fetch(`/api/v1/device-profiles/${encodeURIComponent(profile.machine_id)}/${encodeURIComponent(profile.device_id)}`, {
        method: 'DELETE'
      });
      const text = await response.text();
      if (!response.ok) {
        el('profileHint').textContent = `Delete failed: ${text}`;
        return;
      }

      const deletedLabel = profile.display_label;
      await loadProfiles('');
      await refreshBinding();
      disconnectLive();
      el('profileHint').textContent = `Deleted profile ${deletedLabel}`;
    }

    function disconnectLive() {
      if (ws) {
        ws.close();
        ws = null;
      }
      setConnState('disconnected', 'warn');
    }

    async function init() {
      renderFieldControls();
      renderChart();
      setConnState('disconnected', 'warn');

      await loadProfiles('');
      const binding = await refreshBinding();
      if (binding && binding.is_active) {
        const key = profileKey(binding.machine_id, binding.device_id);
        if (profilesByKey.has(key)) {
          activeProfileKey = key;
          el('profileSelect').value = key;
          applyProfileToForm(currentProfile());
        }
      }

      el('connectBtn').addEventListener('click', connectLive);
      el('disconnectBtn').addEventListener('click', disconnectLive);
      el('profileSelect').addEventListener('change', onProfileSelectionChange);
      el('saveProfileBtn').addEventListener('click', saveProfile);
      el('createProfileBtn').addEventListener('click', createProfile);
      el('reloadProfilesBtn').addEventListener('click', async () => {
        await loadProfiles(activeProfileKey);
        await refreshBinding();
      });
      el('startCalibBtn').addEventListener('click', startCalibration);
      el('associateBtn').addEventListener('click', associateStream);
      el('clearAssocBtn').addEventListener('click', clearAssociation);
      el('deleteProfileBtn').addEventListener('click', deleteProfile);
    }

    init();
  </script>
</body>
</html>
"""
