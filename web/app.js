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
  kvs.push(kvBlock("user:42:name", s["user:42:name"]?.value ?? "", changed(prev, s, "user:42:name", "value")));
  kvs.push(kvBlock("counter:hits", s["counter:hits"]?.value ?? "", changed(prev, s, "counter:hits", "value")));

  const tags = (s["tags"]?.members || []).slice().sort();
  kvs.push(kvBlock("tags", tags.join(", "), changedJSON(prev, s, "tags", "members")));

  const lb = (s["leaderboard"]?.entries || []).map(e => `${e.member}=${e.score}`).join("\n");
  kvs.push(kvBlock("leaderboard", lb, changedJSON(prev, s, "leaderboard", "entries")));

  const xlen = s["feed"]?.xlen ?? 0;
  kvs.push(kvBlock("feed (XLEN)", String(xlen), changed(prev, s, "feed", "xlen")));

  kvs.push(kvBlock("dbsize", String(s.dbsize ?? "?"), false));

  root.replaceChildren(...kvs);
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

function pollState() {
  REGIONS.forEach(refreshRegion);
  setTimeout(pollState, STATE_POLL_MS);
}

setupSettings();
setupProduce();
setupClearLog();
pollState();
connectSSE();
