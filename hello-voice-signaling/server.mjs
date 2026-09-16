import http from "node:http";
import crypto from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 8787);
const MAX_SPEAKERS = 5;
const LISTENERS_PER_RELAY = 5;
const MAX_LISTENERS = MAX_SPEAKERS * LISTENERS_PER_RELAY; // 25
const rooms = new Map();

function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload));
}
function broadcast(room, payload, except) {
  for (const x of room.peers.values()) if (x.ws !== except) send(x.ws, payload);
}
function entries(room) { return [...room.peers.values()]; }
function speakers(room) { return entries(room).filter(x => x.user.role === "speaker"); }
function listeners(room) { return entries(room).filter(x => x.user.role === "listener"); }
function roomMembers(room) { return entries(room).map(x => x.user); }
function capacity(room) { return Math.min(MAX_LISTENERS, speakers(room).length * LISTENERS_PER_RELAY); }
function counts(room) {
  const all = entries(room);
  const speakerCount = all.filter(x => x.user.role === "speaker").length;
  const audienceCount = all.filter(x => x.user.role === "listener").length;
  return {
    participantCount: all.length,
    speakerCount,
    audienceCount,
    listenerCapacity: Math.min(MAX_LISTENERS, speakerCount * LISTENERS_PER_RELAY),
    maxSpeakers: MAX_SPEAKERS,
    maxListeners: MAX_LISTENERS,
    listenersPerRelay: LISTENERS_PER_RELAY
  };
}
function roomStats() {
  return Object.fromEntries([...rooms].map(([id, room]) => [id, counts(room)]));
}
function updateMember(room, peerId, patch, announce = true) {
  const entry = room.peers.get(peerId);
  if (!entry) return null;
  entry.user = { ...entry.user, ...patch };
  if (announce) broadcast(room, { type: "member-updated", peer: entry.user });
  return entry;
}

function relayLoads(room) {
  const load = new Map(speakers(room).map(x => [x.user.id, 0]));
  for (const l of listeners(room)) {
    if (l.user.relayPeerId && load.has(l.user.relayPeerId)) {
      load.set(l.user.relayPeerId, (load.get(l.user.relayPeerId) || 0) + 1);
    }
  }
  return load;
}

function chooseRelay(room, allowOverflow = false) {
  const s = speakers(room);
  if (!s.length) return null;
  const load = relayLoads(room);
  const ordered = s.slice().sort((a, b) => (load.get(a.user.id) || 0) - (load.get(b.user.id) || 0));
  const best = ordered[0];
  const bestLoad = load.get(best.user.id) || 0;
  if (!allowOverflow && bestLoad >= LISTENERS_PER_RELAY) return null;
  return best.user.id;
}

function rebalance(room, allowOverflow = true) {
  const s = speakers(room);
  if (!s.length) {
    for (const l of listeners(room)) {
      if (l.user.relayPeerId) updateMember(room, l.user.id, { relayPeerId: null });
      send(l.ws, { type: "relay-unavailable" });
    }
    return;
  }

  // Reassign listeners evenly. During failover, allow temporary >5 per relay so listeners
  // keep audio. New joins are still capped at 5 listeners per active speaker.
  const load = new Map(s.map(x => [x.user.id, 0]));
  for (const l of listeners(room)) {
    const valid = l.user.relayPeerId && load.has(l.user.relayPeerId);
    if (valid) load.set(l.user.relayPeerId, (load.get(l.user.relayPeerId) || 0) + 1);
  }

  for (const l of listeners(room)) {
    const current = l.user.relayPeerId;
    const currentLoad = current && load.has(current) ? (load.get(current) || 0) : Infinity;
    const ordered = s.slice().sort((a, b) => (load.get(a.user.id) || 0) - (load.get(b.user.id) || 0));
    const best = ordered[0]?.user.id;
    const bestLoad = best ? (load.get(best) || 0) : Infinity;
    const shouldMove = !current || !load.has(current) || (best && currentLoad - bestLoad > 1);
    if (!shouldMove || !best) continue;
    if (!allowOverflow && bestLoad >= LISTENERS_PER_RELAY) continue;
    if (current && load.has(current)) load.set(current, Math.max(0, (load.get(current) || 0) - 1));
    load.set(best, (load.get(best) || 0) + 1);
    updateMember(room, l.user.id, { relayPeerId: best });
  }
}

function leave(ws) {
  const meta = ws.meta;
  if (!meta) return;
  const room = rooms.get(meta.roomId);
  if (room) {
    const leaving = room.peers.get(meta.peerId)?.user;
    room.peers.delete(meta.peerId);
    broadcast(room, { type: "peer-left", peerId: meta.peerId });
    if (room.peers.size === 0) {
      room.messages.length = 0;
      rooms.delete(meta.roomId);
    } else if (leaving?.role === "speaker") {
      rebalance(room, true);
    }
  }
  ws.meta = null;
}

const server = http.createServer((req, res) => {
  res.setHeader("access-control-allow-origin", "*");
  res.setHeader("cache-control", "no-store");
  res.setHeader("content-type", "application/json");
  if (req.url === "/rooms") {
    res.writeHead(200);
    return res.end(JSON.stringify({ rooms: roomStats(), maxSpeakers: MAX_SPEAKERS, maxListeners: MAX_LISTENERS, listenersPerRelay: LISTENERS_PER_RELAY }));
  }
  res.writeHead(200);
  res.end(JSON.stringify({ ok: true, service: "hello-voice-signaling", mode: "distributed-p2p-relay", maxSpeakers: MAX_SPEAKERS, maxListeners: MAX_LISTENERS, listenersPerRelay: LISTENERS_PER_RELAY }));
});

const wss = new WebSocketServer({ server });
wss.on("connection", ws => {
  ws.meta = null;
  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { return send(ws, { type: "error", message: "Invalid JSON" }); }

    if (msg.type === "join") {
      leave(ws);
      const roomId = String(msg.roomId || "").trim();
      const peerId = String(msg.peerId || "").trim();
      const name = String(msg.name || "Guest").trim().slice(0, 32);
      const isHost = Boolean(msg.isHost);
      if (!roomId || !peerId) return send(ws, { type: "error", message: "roomId and peerId are required" });

      let room = rooms.get(roomId);
      if (!room) {
        room = { peers: new Map(), messages: [] };
        rooms.set(roomId, room);
      }

      const requestedRole = msg.role === "speaker" ? "speaker" : "listener";
      const speakerCount = speakers(room).length;
      const role = requestedRole === "speaker" && speakerCount < MAX_SPEAKERS ? "speaker" : "listener";

      let relayPeerId = null;
      if (role === "listener") {
        if (listeners(room).length >= capacity(room)) {
          return send(ws, { type: "room-full", maxAudience: capacity(room), maxListeners: MAX_LISTENERS });
        }
        relayPeerId = chooseRelay(room, false);
        if (!relayPeerId) return send(ws, { type: "room-full", maxAudience: capacity(room), maxListeners: MAX_LISTENERS });
      }

      const peers = roomMembers(room);
      const user = { id: peerId, name, role, relayPeerId, speaking: false, requestedMic: false, isHost };
      room.peers.set(peerId, { ws, user });
      ws.meta = { roomId, peerId };
      send(ws, {
        type: "joined",
        peerId,
        self: user,
        peers,
        role,
        relayPeerId,
        maxSpeakers: MAX_SPEAKERS,
        maxAudience: capacity(room),
        maxListeners: MAX_LISTENERS,
        listenersPerRelay: LISTENERS_PER_RELAY,
        messages: room.messages
      });
      broadcast(room, { type: "peer-joined", peer: user }, ws);
      return;
    }

    const meta = ws.meta;
    if (!meta) return send(ws, { type: "error", message: "Join a room first" });
    const room = rooms.get(meta.roomId);
    const sender = room?.peers.get(meta.peerId);
    if (!room || !sender) return;

    if (msg.type === "signal") {
      const target = room.peers.get(String(msg.to || ""));
      if (target) send(target.ws, { type: "signal", from: meta.peerId, data: msg.data });
      return;
    }
    if (msg.type === "chat") {
      const text = String(msg.text || "").trim().slice(0, 400);
      if (!text) return;
      const message = { id: crypto.randomUUID(), userId: meta.peerId, name: sender.user.name, text, at: Date.now() };
      room.messages.push(message);
      if (room.messages.length > 100) room.messages.shift();
      broadcast(room, { type: "chat", message });
      return;
    }
    if (msg.type === "speaking") {
      if (sender.user.role !== "speaker") return;
      updateMember(room, meta.peerId, { speaking: Boolean(msg.speaking) });
      return;
    }
    if (msg.type === "request-mic") {
      if (sender.user.role !== "listener") return;
      sender.user.requestedMic = true;
      const host = entries(room).find(x => x.user.isHost);
      if (host) send(host.ws, { type: "mic-requested", peerId: meta.peerId });
      broadcast(room, { type: "member-updated", peer: sender.user });
      return;
    }
    if (msg.type === "cancel-mic") {
      if (sender.user.role !== "listener") return;
      sender.user.requestedMic = false;
      broadcast(room, { type: "member-updated", peer: sender.user });
      return;
    }
    if (msg.type === "invite-mic") {
      if (!sender.user.isHost) return;
      const target = room.peers.get(String(msg.peerId || ""));
      if (target && target.user.role === "listener") send(target.ws, { type: "invite-mic" });
      return;
    }
    if (msg.type === "reject-mic") {
      if (!sender.user.isHost) return;
      const peerId = String(msg.peerId || "");
      const target = room.peers.get(peerId);
      if (!target || target.user.role !== "listener") return;
      updateMember(room, peerId, { requestedMic: false });
      send(target.ws, { type: "error", message: "Your mic request was declined." });
      return;
    }
    if (msg.type === "accept-mic") {
      if (sender.user.role === "speaker") return;
      if (speakers(room).length >= MAX_SPEAKERS) return send(ws, { type: "error", message: "All speaker seats are full" });
      updateMember(room, meta.peerId, { role: "speaker", relayPeerId: null, requestedMic: false, speaking: false });
      rebalance(room, false);
      return;
    }
    if (msg.type === "demote") {
      if (!sender.user.isHost) return;
      const peerId = String(msg.peerId || "");
      if (peerId === meta.peerId) return;
      const target = room.peers.get(peerId);
      if (target?.user.role === "speaker") {
        // Demoted speaker becomes an audience member and must receive a relay.
        const afterDemoteSpeakers = speakers(room).filter(x => x.user.id !== peerId);
        if (!afterDemoteSpeakers.length) return send(ws, { type: "error", message: "At least one speaker is required" });
        updateMember(room, peerId, { role: "listener", relayPeerId: null, requestedMic: false, speaking: false });
        rebalance(room, true);
      }
      return;
    }
    if (msg.type === "kick") {
      if (!sender.user.isHost) return;
      const peerId = String(msg.peerId || "");
      if (peerId === meta.peerId) return;
      const target = room.peers.get(peerId);
      if (!target) return;
      const wasSpeaker = target.user.role === "speaker";
      send(target.ws, { type: "kicked" });
      target.ws.meta = null;
      room.peers.delete(peerId);
      broadcast(room, { type: "peer-left", peerId });
      try { target.ws.close(1000, "Removed by host"); } catch {}
      if (wasSpeaker) rebalance(room, true);
      return;
    }
    if (msg.type === "close-room") {
      if (!sender.user.isHost) return;
      broadcast(room, { type: "room-closed" });
      for (const x of room.peers.values()) {
        x.ws.meta = null;
        try { x.ws.close(1000, "Room closed"); } catch {}
      }
      room.messages.length = 0;
      rooms.delete(meta.roomId);
      return;
    }
  });
  ws.on("close", () => leave(ws));
  ws.on("error", () => leave(ws));
});

server.listen(PORT, "0.0.0.0", () => console.log(`hello-voice-signaling listening on port ${PORT}`));
