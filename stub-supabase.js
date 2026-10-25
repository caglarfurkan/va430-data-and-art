/* Universal multi-transport realtime client (drop-in window.supabase replacement) for VA430/530 Data and Art.
   Combines 3 independent transport layers so classroom Wi-Fi (eduroam) or cellular firewalls never block the exercise:
   1. Port-443 HTTPS + WSS Relay (ntfy.sh JSON history + WebSocket + periodic HTTPS poll fallback) — passes through strict Port-443-only campus firewalls.
   2. Dual-broker MQTT over WebSockets (EMQX + HiveMQ) with retained messages — handles instant cross-device state + live moving slider ghosts.
   3. BroadcastChannel + localStorage — instant same-device multi-tab testing and offline resilience. */
(() => {
  const BROKERS = [
    "wss://broker.emqx.io:8084/mqtt",
    "wss://broker.hivemq.com:8884/mqtt"
  ];
  const ntfyTopic = s => `va430_su_${String(s).replace(/[^a-zA-Z0-9_-]/g, "_")}`;

  const bc = typeof BroadcastChannel !== "undefined" ? new BroadcastChannel("stub-supabase") : null;
  const load = () => { try { return JSON.parse(localStorage.getItem("stub-db") || "{}"); } catch (e) { return {}; } };
  const save = d => { try { localStorage.setItem("stub-db", JSON.stringify(d)); } catch (e) {} };
  const getPhase = () => { try { return localStorage.getItem("stub-phase") || ""; } catch (e) { return ""; } };
  const setPhase = p => { try { localStorage.setItem("stub-phase", p); } catch (e) {} };
  const getResetAt = s => { try { return +(localStorage.getItem("stub-reset-" + s) || 0); } catch (e) { return 0; } };
  const setResetAt = (s, ts) => { try { localStorage.setItem("stub-reset-" + s, String(ts)); } catch (e) {} };

  const channels = new Set();
  const presence = {};
  const mqttClients = [];
  const subscribedSessions = new Set();
  const ntfySockets = new Map();
  const seenRowKeys = new Set();
  let lastNtfyPhase = "";
  let lastNtfyPhaseTs = 0;

  const rowKey = (table, r) => {
    if (!r) return "";
    return table === "va430_ratings"
      ? `r:${r.session}:${r.round}:${r.device}`
      : `w:${r.session}:${r.device}`;
  };

  const deliver = msg => channels.forEach(ch => ch._recv(msg));

  function clearLocalSession(session, resetTs) {
    setResetAt(session, resetTs);
    const d = load();
    ["va430_ratings", "va430_words"].forEach(t => {
      if (Array.isArray(d[t])) d[t] = d[t].filter(r => r.session !== session);
    });
    save(d);
    for (const k of [...seenRowKeys]) {
      if (k.includes(`:${session}:`)) seenRowKeys.delete(k);
    }
    try {
      for (let i = localStorage.length - 1; i >= 0; i--) {
        const k = localStorage.key(i);
        if (k && k.includes(session) && (k.startsWith("va430-done-") || k.startsWith("va430-mine-") || k.startsWith("va430-wseq-") || k.startsWith("va430-mywords-"))) {
          localStorage.removeItem(k);
        }
      }
    } catch (e) {}
    deliver({ kind: "reset", session, resetAt: resetTs });
  }

  function ingestRow(table, row) {
    if (!row || !row.session || !row.device) return false;
    const resetAt = getResetAt(row.session);
    if (row.ts && resetAt && row.ts < resetAt) return false;
    const rk = rowKey(table, row);
    const d = load();
    d[table] = d[table] || [];
    const exists = d[table].some(r => r.session === row.session && r.device === row.device && (table !== "va430_ratings" || r.round === row.round));
    if (!exists) {
      d[table].push(row);
      save(d);
    }
    if (!seenRowKeys.has(rk)) {
      seenRowKeys.add(rk);
      deliver({ kind: "insert", table, row });
      return true;
    }
    return false;
  }

  function handleRemotePacket(session, pkt) {
    if (!pkt || typeof pkt !== "object") return;
    if (pkt.resetAt && pkt.resetAt > getResetAt(session)) {
      clearLocalSession(session, pkt.resetAt);
    }
    if (pkt.kind === "phase" && pkt.phase) {
      lastNtfyPhase = pkt.phase;
      setPhase(pkt.phase);
      deliver({ kind: "broadcast", name: "va430-room-" + session, event: "phase", payload: { phase: pkt.phase } });
    } else if (pkt.kind === "ratings" && pkt.row) {
      ingestRow("va430_ratings", pkt.row);
    } else if (pkt.kind === "words" && pkt.row) {
      ingestRow("va430_words", pkt.row);
    }
  }

  /* -------- Layer 1: Port-443 HTTPS + WSS (ntfy.sh) -------- */
  function ensureNtfy(session) {
    if (!session || ntfySockets.has(session)) return;
    const topic = ntfyTopic(session);
    const state = { ws: null, lastTime: 0, pollTimer: null };
    ntfySockets.set(session, state);

    const fetchHistory = async () => {
      try {
        const since = state.lastTime ? String(state.lastTime) : "all";
        const res = await fetch(`https://ntfy.sh/${topic}/json?poll=1&since=${since}`, { cache: "no-store" });
        if (!res.ok) return;
        const text = await res.text();
        const lines = text.split(/\r?\n/).filter(Boolean);
        let latestPhase = null;
        for (const line of lines) {
          try {
            const env = JSON.parse(line);
            if (env.time && env.time > state.lastTime) state.lastTime = env.time;
            if (env.event === "message" && env.message) {
              const pkt = JSON.parse(env.message);
              if (pkt.resetAt && pkt.resetAt > getResetAt(session)) {
                clearLocalSession(session, pkt.resetAt);
              }
              if (pkt.kind === "phase" && pkt.phase) latestPhase = pkt.phase;
              else handleRemotePacket(session, pkt);
            }
          } catch (e) {}
        }
        if (latestPhase) {
          lastNtfyPhase = latestPhase;
          setPhase(latestPhase);
          deliver({ kind: "broadcast", name: "va430-room-" + session, event: "phase", payload: { phase: latestPhase } });
        }
      } catch (e) {}
    };

    fetchHistory();

    const connectWs = () => {
      try {
        const ws = new WebSocket(`wss://ntfy.sh/${topic}/ws`);
        state.ws = ws;
        ws.onmessage = ev => {
          try {
            const env = JSON.parse(ev.data);
            if (env.time && env.time > state.lastTime) state.lastTime = env.time;
            if (env.event === "message" && env.message) {
              handleRemotePacket(session, JSON.parse(env.message));
            }
          } catch (e) {}
        };
        ws.onclose = () => { setTimeout(connectWs, 4000); };
      } catch (e) {}
    };
    connectWs();

    // Gentle fallback poll only if WebSocket is not connected (every 30s) to avoid 429
    state.pollTimer = setInterval(() => {
      if (!state.ws || state.ws.readyState !== 1) {
        fetchHistory();
      }
    }, 30000);
  }

  function pubNtfy(session, pkt) {
    if (!session) return;
    const topic = ntfyTopic(session);
    try {
      fetch(`https://ntfy.sh/${topic}`, {
        method: "POST",
        body: JSON.stringify(pkt),
        keepalive: true
      }).catch(() => {});
    } catch (e) {}
  }

  /* -------- Layer 2: Dual-Broker MQTT over WebSockets -------- */
  function ensureMqtt(session) {
    if (!session) return;
    ensureNtfy(session);
    if (typeof window.mqtt === "undefined") return;
    const topicRoot = `va430/${session}`;
    if (!mqttClients.length) {
      BROKERS.forEach(url => {
        try {
          const client = window.mqtt.connect(url, {
            keepalive: 30,
            reconnectPeriod: 2500,
            connectTimeout: 8000,
            clean: true
          });
          client.on("connect", () => {
            subscribedSessions.forEach(s => {
              client.subscribe(`va430/${s}/#`, { qos: 0 });
            });
          });
          client.on("message", (topic, payload) => {
            let data = null;
            try { data = JSON.parse(payload.toString()); } catch (e) { return; }
            if (!data) return;
            const parts = topic.split("/");
            if (parts[0] !== "va430" || !parts[1]) return;
            const s = parts[1], kind = parts[2];
            if (kind === "phase") {
              if (data.resetAt && data.resetAt > getResetAt(s)) {
                clearLocalSession(s, data.resetAt);
              }
              if (data.phase) {
                setPhase(data.phase);
                deliver({ kind: "broadcast", name: "va430-room-" + s, event: "phase", payload: { phase: data.phase } });
              }
            } else if (kind === "ratings") {
              ingestRow("va430_ratings", data);
            } else if (kind === "words") {
              ingestRow("va430_words", data);
            } else if (kind === "presence" && data.key && data.meta) {
              presence[data.key] = { ...data.meta, _ts: Date.now() };
              deliver({ kind: "presence", name: "va430-room-" + s, key: data.key, meta: data.meta });
            }
          });
          mqttClients.push(client);
        } catch (e) {}
      });
    }
    if (!subscribedSessions.has(session)) {
      subscribedSessions.add(session);
      mqttClients.forEach(c => {
        try { if (c.connected) c.subscribe(`${topicRoot}/#`, { qos: 0 }); } catch (e) {}
      });
    }
  }

  function pubMqtt(topic, obj, retain = false) {
    const str = JSON.stringify(obj);
    mqttClients.forEach(c => {
      try { c.publish(topic, str, { qos: retain ? 1 : 0, retain }); } catch (e) {}
    });
  }

  if (bc) {
    bc.onmessage = e => {
      const m = e.data;
      if (!m) return;
      if (m.kind === "presence") presence[m.key] = { ...m.meta, _ts: Date.now() };
      if (m.kind === "broadcast" && m.event === "phase" && m.payload && m.payload.phase) setPhase(m.payload.phase);
      if (m.kind === "insert" && m.table && m.row) { ingestRow(m.table, m.row); return; }
      if (m.kind === "reset" && m.session && m.resetAt) { clearLocalSession(m.session, m.resetAt); return; }
      deliver(m);
    };
  }

  addEventListener("storage", e => {
    if (e.key === "stub-phase" && e.newValue) {
      subscribedSessions.forEach(s => {
        deliver({ kind: "broadcast", name: "va430-room-" + s, event: "phase", payload: { phase: e.newValue } });
      });
    }
  });

  window.va430ResetLiveSession = (session, currentPhase = "r1") => {
    const ts = Date.now();
    clearLocalSession(session, ts);
    if (bc) bc.postMessage({ kind: "reset", session, resetAt: ts });
    ensureMqtt(session);
    pubMqtt(`va430/${session}/phase`, { phase: currentPhase, resetAt: ts, ts }, true);
    pubNtfy(session, { kind: "phase", phase: currentPhase, resetAt: ts, ts });
  };

  window.supabase = {
    createClient() {
      return {
        from(table) {
          return {
            select() {
              return {
                eq(col, val) {
                  if (col === "session" && val) ensureMqtt(val);
                  const resetAt = col === "session" ? getResetAt(val) : 0;
                  const rows = (load()[table] || []).filter(r => r[col] === val && (!r.ts || !resetAt || r.ts >= resetAt));
                  rows.forEach(r => seenRowKeys.add(rowKey(table, r)));
                  return Promise.resolve({ data: rows, error: null });
                }
              };
            },
            insert(row) {
              const s = row.session;
              ensureMqtt(s);
              const resetAt = getResetAt(s);
              const d = load();
              d[table] = (d[table] || []).filter(r => !r.ts || !resetAt || r.ts >= resetAt);
              if (d[table].some(r => r.session === row.session && r.device === row.device && (table !== "va430_ratings" || r.round === row.round))) {
                return Promise.resolve({ error: { code: "23505" } });
              }
              const fullRow = { ...row, ts: Date.now() };
              d[table].push(fullRow);
              save(d);
              seenRowKeys.add(rowKey(table, fullRow));
              const msg = { kind: "insert", table, row: fullRow };
              if (bc) bc.postMessage(msg);
              deliver(msg);
              if (table === "va430_ratings") {
                pubMqtt(`va430/${s}/ratings/${fullRow.round}/${encodeURIComponent(fullRow.device)}`, fullRow, true);
                pubNtfy(s, { kind: "ratings", row: fullRow, ts: fullRow.ts });
              } else if (table === "va430_words") {
                pubMqtt(`va430/${s}/words/${encodeURIComponent(fullRow.device)}`, fullRow, true);
                pubNtfy(s, { kind: "words", row: fullRow, ts: fullRow.ts });
              }
              return Promise.resolve({ error: null });
            }
          };
        },
        channel(name, opts) {
          const sMatch = /^va430-(?:room|db)-(.+)$/.exec(name);
          const session = sMatch ? sMatch[1] : "";
          if (session) ensureMqtt(session);
          const key = (opts && opts.config && opts.config.presence && opts.config.presence.key) || Math.random().toString(36).slice(2);
          return {
            handlers: [],
            on(type, filter, cb) { this.handlers.push({ type, filter, cb }); return this; },
            subscribe(cb) {
              channels.add(this);
              setTimeout(() => {
                cb && cb("SUBSCRIBED");
                this._sync();
                const savedPhase = getPhase();
                if (savedPhase) {
                  this.handlers.filter(h => h.type === "broadcast" && h.filter.event === "phase").forEach(h => h.cb({ payload: { phase: savedPhase } }));
                }
              }, 30);
              return this;
            },
            send(m) {
              if (m.event === "phase" && m.payload && m.payload.phase) {
                const nextPhase = m.payload.phase;
                setPhase(nextPhase);
                if (session) {
                  pubMqtt(`va430/${session}/phase`, { phase: nextPhase, resetAt: getResetAt(session), ts: Date.now() }, true);
                  const now = Date.now();
                  if (nextPhase !== lastNtfyPhase || now - lastNtfyPhaseTs > 45000) {
                    lastNtfyPhase = nextPhase;
                    lastNtfyPhaseTs = now;
                    pubNtfy(session, { kind: "phase", phase: nextPhase, resetAt: getResetAt(session), ts: now });
                  }
                }
              }
              const msg = { kind: "broadcast", name, event: m.event, payload: m.payload };
              if (bc) bc.postMessage(msg);
              deliver(msg);
              return Promise.resolve("ok");
            },
            track(meta) {
              presence[key] = { ...meta, _ts: Date.now() };
              const msg = { kind: "presence", name, key, meta };
              if (bc) bc.postMessage(msg);
              deliver(msg);
              if (session) {
                pubMqtt(`va430/${session}/presence/${encodeURIComponent(key)}`, { key, meta, ts: Date.now() }, false);
              }
              return Promise.resolve("ok");
            },
            presenceState() {
              const now = Date.now(), o = {};
              Object.entries(presence).forEach(([k, m]) => {
                if (!m._ts || now - m._ts < 60000) o[k] = [m];
              });
              return o;
            },
            _sync() { this.handlers.filter(h => h.type === "presence").forEach(h => h.cb()); },
            _recv(msg) {
              if (msg.kind === "insert") {
                this.handlers.filter(h => h.type === "postgres_changes" && h.filter.table === msg.table).forEach(h => h.cb({ new: msg.row }));
              }
              if (msg.kind === "broadcast" && (msg.name === name || msg.event === "phase")) {
                this.handlers.filter(h => h.type === "broadcast" && h.filter.event === msg.event).forEach(h => h.cb({ payload: msg.payload }));
              }
              if (msg.kind === "presence" && msg.name === name) {
                this._sync();
              }
            }
          };
        }
      };
    }
  };
})();
