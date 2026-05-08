// Live-event app frontend.
//
// Reads /api/event-state?region=<r> every 800ms; writes via /api/produce.
// Reactions, Q&A, and polls all map to existing op families:
//
//   reactions:  INCR event:reactions:<emoji>          (G-counter)
//   ask Q:      XADD event:qa:stream {qid, text, ...}
//   upvote Q:   INCR event:qa:votes:<qid>             (G-counter)
//   poll vote:  INCR event:poll:tally:<i>             (G-counter)
//   attendees:  SADD event:attendees <session-token> (heartbeat-refreshed)
//
// Open this URL in three tabs with three different regions and watch the
// counters drift apart for ~MM2 lag and reconverge.

const POLL_MS = 800;
const HEARTBEAT_MS = 20000;
const REACTION_THROTTLE_MS = 80;

const REACTIONS = [
  { key: "heart",     emoji: "❤️" },
  { key: "fire",      emoji: "🔥" },
  { key: "clap",      emoji: "👏" },
  { key: "laugh",     emoji: "😂" },
  { key: "celebrate", emoji: "🎉" },
];

// --- localStorage --------------------------------------------------------

const ls = {
  get: (k, def = "") => { try { return localStorage.getItem(k) ?? def; } catch { return def; } },
  set: (k, v) => { try { localStorage.setItem(k, v); } catch {} },
};

const settings = {
  apiBase:  () => (ls.get("api_base", "") || guessApiBase()).replace(/\/+$/, ""),
  token:    () => ls.get("api_token", ""),
  region:   () => ls.get("live_region", randomRegion()),
  name:     () => ls.get("live_name", "Anonymous"),
  session:  () => {
    let s = ls.get("live_session", "");
    if (!s) {
      s = "s_" + Math.random().toString(36).slice(2, 12);
      ls.set("live_session", s);
    }
    return s;
  },
};

function randomRegion() { return ["us","eu","ap"][Math.floor(Math.random() * 3)]; }

function guessApiBase() {
  if (location.hostname === "localhost" || location.hostname.startsWith("127.")) return "http://localhost:18080";
  return "https://redis-kafka-wal-api.zeteq.com";
}

// --- DOM helpers ---------------------------------------------------------

const $ = (sel) => document.querySelector(sel);

function el(tag, props = {}, children = []) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (k === "class") e.className = v;
    else if (k === "text") e.textContent = v;
    else if (k === "html") e.innerHTML = v;
    else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
    else if (v !== false && v != null) e.setAttribute(k, v);
  }
  for (const c of [].concat(children)) {
    if (c == null) continue;
    e.appendChild(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return e;
}

function toast(msg, ms = 3000) {
  const t = el("div", { class: "toast", text: msg });
  document.body.appendChild(t);
  setTimeout(() => t.remove(), ms);
}

// --- API helpers ---------------------------------------------------------

async function produce(body) {
  const token = settings.token();
  if (!token) {
    toast("set bearer token in settings to write");
    return false;
  }
  try {
    const resp = await fetch(`${settings.apiBase()}/api/produce`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Authorization": `Bearer ${token}` },
      body: JSON.stringify({ region: settings.region(), ...body }),
    });
    if (!resp.ok) {
      toast(`${resp.status}: ${await resp.text()}`);
      return false;
    }
    return true;
  } catch (e) {
    toast(`network: ${e.message}`);
    return false;
  }
}

async function fetchState() {
  try {
    const resp = await fetch(`${settings.apiBase()}/api/event-state?region=${settings.region()}`, { cache: "no-store" });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return await resp.json();
  } catch (e) {
    return null;
  }
}

// --- Reactions -----------------------------------------------------------

const reactionState = {};
const reactionLastClick = {};

function setupReactions() {
  const row = $("#reactions-row");
  for (const r of REACTIONS) {
    const btn = el("button", { class: "reaction-btn", type: "button", "data-key": r.key }, [
      el("span", { class: "reaction-emoji", text: r.emoji }),
      el("span", { class: "reaction-count", id: `react-${r.key}`, text: "0" }),
    ]);
    btn.addEventListener("click", () => onReact(btn, r));
    row.appendChild(btn);
    reactionState[r.key] = 0;
  }
}

function onReact(btn, r) {
  // throttle to one per ~80ms per reaction
  const now = Date.now();
  if ((reactionLastClick[r.key] || 0) + REACTION_THROTTLE_MS > now) return;
  reactionLastClick[r.key] = now;

  // optimistic
  reactionState[r.key]++;
  const cnt = $(`#react-${r.key}`);
  cnt.textContent = String(reactionState[r.key]);
  cnt.classList.add("bumping");
  setTimeout(() => cnt.classList.remove("bumping"), 200);

  spawnFloater(btn, r.emoji);
  produce({ op: "INCR", key: `event:reactions:${r.key}`, delta: 1 });
}

function spawnFloater(btn, emoji) {
  const fx = $("#reactions-fx");
  const rect = btn.getBoundingClientRect();
  const fxRect = fx.getBoundingClientRect();
  const e = el("div", { class: "fx-emoji", text: emoji });
  e.style.left = `${rect.left - fxRect.left + rect.width / 2 - 14}px`;
  e.style.top = `${rect.top - fxRect.top + 8}px`;
  e.style.setProperty("--dx", `${(Math.random() - 0.5) * 60}px`);
  fx.appendChild(e);
  setTimeout(() => e.remove(), 1500);
}

function renderReactions(reactions) {
  for (const r of REACTIONS) {
    const cur = reactions[r.key] ?? 0;
    if (cur !== reactionState[r.key]) {
      reactionState[r.key] = cur;
      const cnt = $(`#react-${r.key}`);
      if (cnt) {
        cnt.textContent = String(cur);
        cnt.classList.add("bumping");
        setTimeout(() => cnt.classList.remove("bumping"), 200);
      }
    }
  }
}

// --- Q&A -----------------------------------------------------------------

const votedQs = new Set(JSON.parse(ls.get("voted_qs", "[]") || "[]"));

function setupQA() {
  $("#qa-form").addEventListener("submit", onAsk);
}

async function onAsk(e) {
  e.preventDefault();
  const input = $("#qa-input");
  const text = input.value.trim();
  if (!text) return;
  const qid = "q_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 7);
  const ok = await produce({
    op: "XADD",
    key: "event:qa:stream",
    fields: {
      qid,
      text,
      asker: settings.name(),
      region: settings.region(),
    },
  });
  if (ok) {
    input.value = "";
    // Auto-upvote your own question
    await produce({ op: "INCR", key: `event:qa:votes:${qid}`, delta: 1 });
    votedQs.add(qid);
    persistVoted();
  }
}

function onUpvote(qid) {
  if (votedQs.has(qid)) return;
  votedQs.add(qid);
  persistVoted();
  produce({ op: "INCR", key: `event:qa:votes:${qid}`, delta: 1 });
  // Optimistic UI nudge: re-render after a short tick
  setTimeout(refresh, 100);
}

function persistVoted() {
  ls.set("voted_qs", JSON.stringify([...votedQs].slice(-200)));
}

function renderQA(qa) {
  const list = $("#qa-list");
  $("#qa-count").textContent = `${qa.total ?? 0} question${(qa.total === 1) ? "" : "s"}`;

  const seen = new Set();
  list.replaceChildren(...(qa.questions || []).map(q => {
    seen.add(q.qid);
    const voted = votedQs.has(q.qid);
    const region = (q.region || "").toLowerCase();
    return el("li", { class: `qa-item origin-${region}` }, [
      el("button", {
        class: "qa-vote" + (voted ? " voted" : ""),
        type: "button",
        title: voted ? "you've upvoted" : "upvote",
        onclick: () => onUpvote(q.qid),
      }, [
        el("span", { class: "arrow", text: "▲" }),
        el("span", { class: "count", text: String(q.votes ?? 0) }),
      ]),
      el("div", { class: "qa-text" }, [
        el("div", { class: "qa-text-body", text: q.text }),
        el("div", { class: "qa-text-meta", text: `${q.asker || "anonymous"} · ${q.region || "?"}` }),
      ]),
    ]);
  }));
}

// --- Poll ----------------------------------------------------------------

const votedOption = { current: null };
function pollVoteKey(question) { return `poll_voted:${question}`; }

function setupPoll() { /* nothing eager — rendered on first state */ }

async function onVote(question, idx) {
  const key = pollVoteKey(question);
  if (ls.get(key, "") !== "") return; // one vote per poll per browser
  ls.set(key, String(idx));
  votedOption.current = idx;
  await produce({ op: "INCR", key: `event:poll:tally:${idx}`, delta: 1 });
  setTimeout(refresh, 100);
}

function renderPoll(poll) {
  const q = $("#poll-question");
  const opts = $("#poll-options");
  const totalEl = $("#poll-total");

  if (!poll || !poll.question) {
    q.textContent = "— no poll configured —";
    opts.replaceChildren();
    totalEl.textContent = "—";
    return;
  }
  q.textContent = poll.question;
  totalEl.textContent = `${poll.total_votes ?? 0} vote${(poll.total_votes === 1) ? "" : "s"}`;

  const myVote = ls.get(pollVoteKey(poll.question), "");
  const total = poll.total_votes || 1;
  votedOption.current = myVote === "" ? null : Number(myVote);

  opts.replaceChildren(...(poll.options || []).map(o => {
    const pct = Math.round((o.votes / total) * 100);
    const voted = String(o.index) === myVote;
    return el("div", {
      class: "poll-option" + (voted ? " voted" : ""),
      onclick: () => onVote(poll.question, o.index),
      title: voted ? "you voted for this" : "click to vote",
    }, [
      el("div", { class: "poll-option-bar", style: `width: ${pct}%` }),
      el("div", { class: "poll-option-text", text: o.text }),
      el("div", { class: "poll-option-stat", text: `${pct}% · ${o.votes} vote${o.votes === 1 ? "" : "s"}` }),
    ]);
  }));
}

// --- Hero + region pill --------------------------------------------------

function renderHero(state) {
  const e = state.event || {};
  document.title = e.title ? `${e.title} — live` : "Live event";
  $("#event-title").textContent = e.title || "(no event configured)";
  $("#hero-title").textContent = e.title || "(no event configured)";
  $("#hero-speaker").textContent = e.speaker || "—";
  $("#hero-topic").textContent = e.topic || "—";
  const status = (e.status || "draft").toLowerCase();
  const sEl = $("#hero-status");
  sEl.textContent = status.toUpperCase();
  sEl.classList.toggle("live", status === "live");
  sEl.classList.toggle("ended", status === "ended");

  const att = e.attendees || 0;
  $("#attendees-pill").textContent = `${att} online`;
  $("#attendees-pill").classList.remove("conn-pending");
  $("#attendees-pill").classList.add("conn-ok");
}

function renderRegionPill() {
  const r = settings.region();
  const pill = $("#region-pill");
  pill.setAttribute("data-region", r);
  $("#region-pill-label").textContent = `you're in ${r.toUpperCase()}`;
}

// --- Settings ------------------------------------------------------------

function setupSettings() {
  $("#api-base").value = ls.get("api_base", "");
  $("#api-token").value = ls.get("api_token", "");
  $("#display-name").value = settings.name();
  $("#region-select").value = settings.region();

  $("#settings-btn").addEventListener("click", () => $("#settings-panel").classList.toggle("hidden"));
  $("#settings-save").addEventListener("click", async () => {
    ls.set("api_base", $("#api-base").value.trim());
    ls.set("api_token", $("#api-token").value);
    ls.set("live_name", $("#display-name").value.trim() || "Anonymous");
    const newRegion = $("#region-select").value;
    if (newRegion !== settings.region()) {
      ls.set("live_region", newRegion);
      // Re-join attendees on the new region
      heartbeat();
    }
    $("#settings-panel").classList.add("hidden");
    renderRegionPill();
    refresh();
  });
}

// --- Attendees heartbeat -------------------------------------------------

let heartbeatTimer = null;
function heartbeat() {
  produce({ op: "SADD", key: "event:attendees", member: settings.session() });
}
function setupHeartbeat() {
  heartbeat();
  heartbeatTimer = setInterval(heartbeat, HEARTBEAT_MS);
  window.addEventListener("beforeunload", () => {
    navigator.sendBeacon?.(
      `${settings.apiBase()}/api/produce`,
      new Blob([JSON.stringify({
        region: settings.region(),
        op: "SREM",
        key: "event:attendees",
        member: settings.session(),
      })], { type: "application/json" }),
    );
  });
}

// --- Main loop -----------------------------------------------------------

async function refresh() {
  const state = await fetchState();
  if (!state) return;
  renderHero(state);
  renderReactions(state.reactions || {});
  renderQA(state.qa || {});
  renderPoll(state.poll || null);
}

async function loop() {
  await refresh();
  setTimeout(loop, POLL_MS);
}

setupSettings();
setupReactions();
setupQA();
setupPoll();
renderRegionPill();
setupHeartbeat();
loop();
