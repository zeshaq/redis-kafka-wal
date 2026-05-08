// Vanilla-JS frontend for redis-kafka-wal-lab.
//
//   * EventSource(`${API}/api/events`)        live event stream from all 3 Kafkas
//   * fetch(`${API}/api/regions/:r/state`)    polled every STATE_POLL_MS to refresh region panels
//   * POST `${API}/api/produce`               write op (requires bearer token)
//
// Settings (API base URL, token) are persisted in localStorage.

const REGIONS = ["us", "eu", "ap"];
const STATE_POLL_MS = 1500;
const MAX_EVENTS = 500;

const ls = {
  get: (k, def = "") => {
    try { return localStorage.getItem(k) ?? def; } catch { return def; }
  },
  set: (k, v) => {
    try { localStorage.setItem(k, v); } catch {}
  },
};

const settings = {
  apiBase: () => (ls.get("api_base", "") || guessApiBase()).replace(/\/+$/, ""),
  token:   () => ls.get("api_token", ""),
};

function guessApiBase() {
  // Override for local dev.
  if (location.hostname === "localhost" || location.hostname.startsWith("127.")) {
    return "http://localhost:18080";
  }
  // Production deployment: bridge is exposed via Cloudflare Tunnel at
  // a stable hostname on the lab's domain. Override in settings if you
  // self-host the bridge somewhere else.
  return "https://redis-kafka-wal-api.zeteq.com";
}

// --- DOM helpers ---------------------------------------------------------

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => document.querySelectorAll(sel);

function el(tag, props = {}, children = []) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (k === "class") e.className = v;
    else if (k === "text") e.textContent = v;
    else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
    else e.setAttribute(k, v);
  }
  for (const c of [].concat(children)) {
    if (c == null) continue;
    e.appendChild(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return e;
}

// --- region state panel --------------------------------------------------

const lastState = { us: null, eu: null, ap: null };

async function refreshRegion(region) {
  try {
    const resp = await fetch(`${settings.apiBase()}/api/regions/${region}/state`, { cache: "no-store" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const s = await resp.json();
    renderRegion(region, s);
    setConn("ok");
  } catch (err) {
    renderRegionError(region, err);
  }
}

function renderRegion(region, s) {
  const root = $(`#state-${region}`);
  const prev = lastState[region];
  lastState[region] = s;

  const kvs = [];
  kvs.push(kvBlock(
    "inventory:laptop",
    s["inventory:laptop"]?.value ?? "",
    changed(prev, s, "inventory:laptop", "value"),
  ));

  kvs.push(kvBlock(
    "sales:total",
    s["sales:total"]?.value ?? "0",
    changed(prev, s, "sales:total", "value"),
  ));

  const carts = (s["cart:active"]?.members || []).slice().sort();
  const cartLabel = carts.length === 0
    ? ""
    : carts.length <= 6 ? carts.join(", ") : `${carts.slice(0, 6).join(", ")}  (+${carts.length - 6} more)`;
  kvs.push(kvBlock(
    `cart:active  (${carts.length})`,
    cartLabel,
    changedJSON(prev, s, "cart:active", "members"),
  ));

  const lb = (s["leaderboard:spenders"]?.entries || [])
    .map(e => `${pad(e.member, 18)} ${e.score}`)
    .join("\n");
  kvs.push(kvBlock(
    "leaderboard:spenders  (top 10)",
    lb,
    changedJSON(prev, s, "leaderboard:spenders", "entries"),
  ));

  const xlen = s["orders:feed"]?.xlen ?? 0;
  const recent = (s["orders:feed"]?.recent || [])
    .slice(0, 3)
    .map(e => formatStreamEntry(e))
    .join("\n");
  kvs.push(kvBlock(
    `orders:feed  (XLEN=${xlen})`,
    recent,
    changed(prev, s, "orders:feed", "xlen"),
  ));

  kvs.push(kvBlock("dbsize", String(s.dbsize ?? "?"), false));

  root.replaceChildren(...kvs);
}

function pad(s, n) {
  s = String(s ?? "");
  return s.length >= n ? s : s + " ".repeat(n - s.length);
}

function formatStreamEntry(e) {
  if (!e) return "";
  const id = e.ID || e.id || "?";
  const fields = e.Values || e.values || {};
  const parts = Object.entries(fields)
    .filter(([k]) => !k.startsWith("__"))
    .map(([k, v]) => `${k}=${v}`)
    .join(" ");
  return `${id.split("-")[0]}  ${parts}`;
}

function changed(prev, cur, key, field) {
  if (!prev) return false;
  return (prev[key]?.[field] ?? "") !== (cur[key]?.[field] ?? "");
}
function changedJSON(prev, cur, key, field) {
  if (!prev) return false;
  return JSON.stringify(prev[key]?.[field] ?? null) !== JSON.stringify(cur[key]?.[field] ?? null);
}

function kvBlock(label, value, didChange) {
  const valEl = el("div", { class: "kv-value" + (value === "" ? " empty" : "") }, value || "(empty)");
  const block = el("div", { class: "kv" + (didChange ? " kv-just-changed" : "") }, [
    el("div", { class: "kv-label", text: label }),
    valEl,
  ]);
  return block;
}

function renderRegionError(region, err) {
  const root = $(`#state-${region}`);
  root.replaceChildren(el("div", { class: "kv" }, [
    el("div", { class: "kv-label", text: "error" }),
    el("div", { class: "kv-value", text: String(err.message || err) }),
  ]));
}

// --- live event log ------------------------------------------------------

let eventCount = 0;
const eventsByID = new Map(); // event_id -> {origins observed in arrival order}

function appendEvent(line) {
  const log = $("#event-log");
  const ts = new Date(line.seen_at || Date.now()).toLocaleTimeString();

  const id = line.event_id;
  let row = eventsByID.get(id);
  if (!row) {
    // first time we see this event; create the row at top
    row = { observed: new Set(), node: null };
    eventsByID.set(id, row);
    eventCount++;
    $("#evt-count").textContent = String(eventCount);

    const obs = el("span", { class: "evt-observed" }, [
      el("span", { class: "obs", "data-r": "us" }),
      el("span", { class: "obs", "data-r": "eu" }),
      el("span", { class: "obs", "data-r": "ap" }),
    ]);

    const node = el("div", { class: `evt evt-origin-${line.origin_region}` }, [
      el("span", { class: "evt-time", text: ts }),
      el("span", { class: `evt-origin ${line.origin_region}`, text: line.origin_region.toUpperCase() }),
      el("span", { class: "evt-op", text: line.op }),
      el("span", { class: "evt-key", text: `${line.key}  ` }, [
        el("span", { class: "evt-payload", text: payloadShort(line.payload) }),
      ]),
      obs,
    ]);
    row.node = node;
    log.prepend(node);

    // cap log
    while (log.children.length > MAX_EVENTS) {
      const drop = log.lastChild;
      log.removeChild(drop);
    }
  }

  // mark this region as observed
  row.observed.add(line.observed_at);
  const dotSelector = `.obs[data-r="${line.observed_at}"]`;
  const dot = row.node.querySelector(dotSelector);
  if (dot) dot.classList.add(line.observed_at);
}

function payloadShort(p) {
  if (!p) return "";
  if (p.value !== undefined) return `= ${truncate(p.value, 40)}`;
  if (p.delta !== undefined) return `+${p.delta}`;
  if (p.member !== undefined && p.score !== undefined) return `${p.member}=${p.score}`;
  if (p.member !== undefined) return p.member;
  if (p.fields)  return JSON.stringify(p.fields);
  return JSON.stringify(p);
}

function truncate(s, n) {
  s = String(s);
  return s.length > n ? s.slice(0, n) + "…" : s;
}

// --- SSE -----------------------------------------------------------------

let evtSrc = null;
function connectSSE() {
  if (evtSrc) evtSrc.close();
  setConn("pending");
  evtSrc = new EventSource(`${settings.apiBase()}/api/events`);
  evtSrc.onopen    = () => setConn("ok");
  evtSrc.onmessage = (m) => {
    try { appendEvent(JSON.parse(m.data)); } catch (e) { console.warn("bad event", e); }
  };
  evtSrc.onerror = (e) => {
    console.warn("sse error", e);
    setConn("err");
    // EventSource auto-reconnects; we just reflect status.
  };
}
function setConn(state) {
  const s = $("#conn-status");
  s.classList.remove("conn-pending", "conn-ok", "conn-err");
  s.classList.add(`conn-${state}`);
  s.textContent = state === "ok" ? "live" : state === "err" ? "disconnected" : "connecting…";
}

// --- produce -------------------------------------------------------------

const opFields = {
  SET:  [["value", "text"], ["ttl_ms", "number"]],
  DEL:  [],
  INCR: [["delta", "number"]],
  SADD: [["member", "text"]],
  SREM: [["member", "text"]],
  ZADD: [["member", "text"], ["score", "number"]],
  XADD: [["fields_kv", "text", 'k=v,k2=v2']],
};

function renderOpFields(op) {
  const row = $("#op-fields-row");
  row.replaceChildren();
  for (const [name, type, placeholder] of (opFields[op] || [])) {
    const input = el("input", { name, type, placeholder: placeholder || "" });
    if (type === "number") input.step = "any";
    row.appendChild(el("label", {}, [name, input]));
  }
}

async function submitProduce(ev) {
  ev.preventDefault();
  const form = ev.target;
  const fd = new FormData(form);
  const body = {
    region: fd.get("region"),
    op:     fd.get("op"),
    key:    fd.get("key"),
  };
  for (const [name, type] of (opFields[body.op] || [])) {
    const raw = fd.get(name);
    if (raw == null || raw === "") continue;
    if (type === "number") body[name === "fields_kv" ? "fields_kv" : name] = Number(raw);
    else body[name] = raw;
  }
  if (body.op === "XADD" && body.fields_kv) {
    body.fields = parseKV(body.fields_kv);
    delete body.fields_kv;
  }

  const result = $("#produce-result");
  result.classList.remove("ok", "err");
  result.textContent = "producing…";

  try {
    const resp = await fetch(`${settings.apiBase()}/api/produce`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(settings.token() ? { "Authorization": `Bearer ${settings.token()}` } : {}),
      },
      body: JSON.stringify(body),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      result.classList.add("err");
      result.textContent = `error: ${resp.status} ${data?.error || ""}`;
      return;
    }
    result.classList.add("ok");
    result.textContent = `ok offset=${data.offset} hlc=${data.hlc?.phys}.${data.hlc?.logical}`;
  } catch (e) {
    result.classList.add("err");
    result.textContent = `network: ${e.message}`;
  }
}

function parseKV(s) {
  const out = {};
  for (const part of String(s).split(",")) {
    const i = part.indexOf("=");
    if (i < 0) continue;
    out[part.slice(0, i).trim()] = part.slice(i + 1).trim();
  }
  return out;
}

// --- bootstrap -----------------------------------------------------------

function setupSettings() {
  $("#api-base").value  = ls.get("api_base", "");
  $("#api-token").value = ls.get("api_token", "");
  $("#settings-btn").addEventListener("click", () => $("#settings-panel").classList.toggle("hidden"));
  $("#settings-save").addEventListener("click", () => {
    ls.set("api_base",  $("#api-base").value.trim());
    ls.set("api_token", $("#api-token").value);
    $("#settings-panel").classList.add("hidden");
    // re-bootstrap with new settings
    connectSSE();
    REGIONS.forEach(refreshRegion);
  });
}

function setupProduce() {
  $("#op-select").addEventListener("change", (e) => renderOpFields(e.target.value));
  renderOpFields("SET");
  $("#produce-form").addEventListener("submit", submitProduce);
}

function setupClearLog() {
  $("#clear-log").addEventListener("click", () => {
    $("#event-log").replaceChildren();
    eventsByID.clear();
    eventCount = 0;
    $("#evt-count").textContent = "0";
  });
}

// --- traffic simulator ---------------------------------------------------

let simState = { running: false, rate: 5, produced: 0, uptime_s: 0 };
let simRateDirty = false; // user changed rate while running; need PATCH

async function refreshSim() {
  try {
    const resp = await fetch(`${settings.apiBase()}/api/sim`, { cache: "no-store" });
    if (resp.ok) {
      simState = await resp.json();
      renderSim();
    }
  } catch { /* ignore — refreshes again next tick */ }
}

function renderSim() {
  const status = $("#sim-status");
  const btn = $("#sim-toggle");
  const stats = $("#sim-stats-text");
  const rateInput = $("#sim-rate");

  if (simState.running) {
    status.textContent = `running · ${simState.rate}/sec`;
    status.classList.remove("sim-off");
    status.classList.add("sim-on");
    btn.textContent = "stop";
    btn.classList.add("running");
    const rps = simState.uptime_s > 0
      ? (simState.produced / simState.uptime_s).toFixed(1)
      : "—";
    stats.textContent = `produced ${simState.produced} events · uptime ${simState.uptime_s}s · observed ${rps}/sec`;
  } else {
    status.textContent = "off";
    status.classList.remove("sim-on");
    status.classList.add("sim-off");
    btn.textContent = "start";
    btn.classList.remove("running");
    stats.textContent = "stopped";
  }

  // Don't stomp on the user's rate edit; only sync when not focused.
  if (document.activeElement !== rateInput && !simRateDirty) {
    rateInput.value = simState.rate;
  }
}

async function toggleSim() {
  if (!settings.token()) {
    flashSimError("set a bearer token in settings first");
    return;
  }
  const rate = Math.max(1, Math.min(50, Number($("#sim-rate").value) || 5));
  const path = simState.running ? "/api/sim/stop" : "/api/sim/start";
  try {
    const resp = await fetch(`${settings.apiBase()}${path}`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${settings.token()}`,
      },
      body: simState.running ? null : JSON.stringify({ rate }),
    });
    if (!resp.ok) {
      flashSimError(`${resp.status} ${await resp.text()}`);
      return;
    }
    simState = await resp.json();
    simRateDirty = false;
    renderSim();
  } catch (e) {
    flashSimError(`network: ${e.message}`);
  }
}

async function pushSimRate() {
  if (!simState.running || !settings.token()) return;
  const rate = Math.max(1, Math.min(50, Number($("#sim-rate").value) || 5));
  try {
    const resp = await fetch(`${settings.apiBase()}/api/sim/rate`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${settings.token()}`,
      },
      body: JSON.stringify({ rate }),
    });
    if (resp.ok) {
      simState = await resp.json();
      simRateDirty = false;
      renderSim();
    }
  } catch { /* ignore */ }
}

function flashSimError(msg) {
  const stats = $("#sim-stats-text");
  stats.textContent = `error: ${msg}`;
  stats.style.color = "var(--err)";
  setTimeout(() => { stats.style.color = ""; renderSim(); }, 3000);
}

function setupSim() {
  $("#sim-toggle").addEventListener("click", toggleSim);
  $("#sim-rate").addEventListener("input", () => { simRateDirty = true; });
  // Push rate change after the user stops typing (debounced).
  let t;
  $("#sim-rate").addEventListener("change", () => {
    clearTimeout(t);
    t = setTimeout(pushSimRate, 250);
  });
}

function pollState() {
  REGIONS.forEach(refreshRegion);
  refreshSim();
  setTimeout(pollState, STATE_POLL_MS);
}

setupSettings();
setupProduce();
setupClearLog();
setupSim();
pollState();
connectSSE();
