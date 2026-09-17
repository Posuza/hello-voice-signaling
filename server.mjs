import http from "node:http";
import crypto from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 8787);
const MAX_SPEAKERS = 5;
const LISTENERS_PER_RELAY = 5;
const MAX_LISTENERS = MAX_SPEAKERS * LISTENERS_PER_RELAY;
const rooms = new Map();

function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload));
}
function broadcast(room, payload, except) {
  for (const entry of room.peers.values()) if (entry.ws !== except) send(entry.ws, payload);
}
function entries(room) { return [...room.peers.values()]; }
function speakers(room) { return entries(room).filter(entry => entry.user.role === "speaker"); }
function listeners(room) { return entries(room).filter(entry => entry.user.role === "listener"); }
function members(room) { return entries(room).map(entry => entry.user); }
function listenerCapacity(room) { return Math.min(MAX_LISTENERS, speakers(room).length * LISTENERS_PER_RELAY); }

function counts(room) {
  return {
    participantCount: room.peers.size,
    speakerCount: speakers(room).length,
    audienceCount: listeners(room).length,
    listenerCapacity: listenerCapacity(room),
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
  if (announce) broadcast(room, { type: "member-updated", member: entry.user });
  return entry;
}

function relayLoads(room) {
  const loads = new Map(speakers(room).map(entry => [entry.user.id, 0]));
  for (const listener of listeners(room)) {
    const relayId = listener.user.relayPeerId;
    if (relayId && loads.has(relayId)) loads.set(relayId, (loads.get(relayId) || 0) + 1);
  }
  return loads;
}

function chooseRelay(room, allowOverflow = false) {
  const available = speakers(room);
  if (!available.length) return null;
  const loads = relayLoads(room);
  const ordered = available.slice().sort((a, b) => (loads.get(a.user.id) || 0) - (loads.get(b.user.id) || 0));
  const best = ordered[0];
  const bestLoad = loads.get(best.user.id) || 0;
  if (!allowOverflow && bestLoad >= LISTENERS_PER_RELAY) return null;
  return best.user.id;
}

function rebalance(room, allowOverflow = true) {
  const available = speakers(room);
  if (!available.length) {
    for (const listener of listeners(room)) {
      if (listener.user.relayPeerId) updateMember(room, listener.user.id, { relayPeerId: null });
      send(listener.ws, { type: "relay-unavailable" });
    }
    return;
  }

  const loads = new Map(available.map(entry => [entry.user.id, 0]));
  for (const listener of listeners(room)) {
    const relayId = listener.user.relayPeerId;
    if (relayId && loads.has(relayId)) loads.set(relayId, (loads.get(relayId) || 0) + 1);
  }

  for (const listener of listeners(room)) {
    const current = listener.user.relayPeerId;
    const currentLoad = current && loads.has(current) ? (loads.get(current) || 0) : Infinity;
    const ordered = available.slice().sort((a, b) => (loads.get(a.user.id) || 0) - (loads.get(b.user.id) || 0));
    const best = ordered[0]?.user.id;
    const bestLoad = best ? (loads.get(best) || 0) : Infinity;
    const shouldMove = !current || !loads.has(current) || (best && currentLoad - bestLoad > 1);
    if (!shouldMove || !best) continue;
    if (!allowOverflow && bestLoad >= LISTENERS_PER_RELAY) continue;
    if (current && loads.has(current)) loads.set(current, Math.max(0, (loads.get(current) || 0) - 1));
    loads.set(best, (loads.get(best) || 0) + 1);
    updateMember(room, listener.user.id, { relayPeerId: best });
  }
}

function removeSocket(ws, { announce = true } = {}) {
  const meta = ws.meta;
  if (!meta) return;
  const room = rooms.get(meta.roomId);
  if (!room) { ws.meta = null; return; }

  const current = room.peers.get(meta.peerId);
  if (!current || current.ws !== ws || current.sessionId !== meta.sessionId) {
    ws.meta = null;
    return;
  }

  const leaving = current.user;
  room.peers.delete(meta.peerId);
  if (announce) broadcast(room, { type: "member-left", peerId: meta.peerId });

  if (room.peers.size === 0) {
    room.messages.length = 0;
    rooms.delete(meta.roomId);
  } else if (leaving.role === "speaker") {
    rebalance(room, true);
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
  res.end(JSON.stringify({ ok: true, service: "hello-voice-signaling", protocol: 29, mode: "distributed-p2p-relay", maxSpeakers: MAX_SPEAKERS, maxListeners: MAX_LISTENERS, listenersPerRelay: LISTENERS_PER_RELAY }));
});

const wss = new WebSocketServer({ server });
wss.on("connection", ws => {
  ws.meta = null;

  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { return send(ws, { type: "error", message: "Invalid JSON" }); }

    if (msg.type === "join") {
      removeSocket(ws);
      const roomId = String(msg.roomId || "").trim();
      const peerId = String(msg.peerId || "").trim();
      const sessionId = String(msg.sessionId || crypto.randomUUID()).trim();
      const name = String(msg.name || "Guest").trim().slice(0, 32);
      const requestedHost = Boolean(msg.isHost);
      if (!roomId || !peerId || !sessionId) return send(ws, { type: "error", message: "roomId, peerId and sessionId are required" });

      let room = rooms.get(roomId);
      if (!room) {
        room = { peers: new Map(), messages: [], hostPeerId: null };
        rooms.set(roomId, room);
      }

      const existing = room.peers.get(peerId);
      if (existing && existing.ws !== ws) {
        send(existing.ws, { type: "session-replaced" });
        existing.ws.meta = null;
        room.peers.delete(peerId);
        broadcast(room, { type: "member-left", peerId });
        try { existing.ws.close(4001, "Replaced by a newer session"); } catch {}
      }

      if (!room.hostPeerId && requestedHost) room.hostPeerId = peerId;
      const isHost = room.hostPeerId === peerId;
      const requestedRole = msg.role === "speaker" ? "speaker" : "listener";
      const role = requestedRole === "speaker" && speakers(room).length < MAX_SPEAKERS ? "speaker" : "listener";

      let relayPeerId = null;
      if (role === "listener") {
        const capacity = listenerCapacity(room);
        if (listeners(room).length >= capacity) return send(ws, { type: "room-full", listenerCapacity: capacity, maxListeners: MAX_LISTENERS });
        relayPeerId = chooseRelay(room, false);
        if (!relayPeerId) return send(ws, { type: "room-full", listenerCapacity: capacity, maxListeners: MAX_LISTENERS });
      }

      const existingMembers = members(room);
      const user = { id: peerId, name, role, relayPeerId, speaking: false, muted: false, requestedMic: false, isHost };
      room.peers.set(peerId, { ws, user, sessionId });
      ws.meta = { roomId, peerId, sessionId };

      send(ws, {
        type: "room-snapshot",
        roomId,
        sessionId,
        self: user,
        members: existingMembers,
        messages: [...room.messages],
        limits: {
          maxSpeakers: MAX_SPEAKERS,
          maxListeners: MAX_LISTENERS,
          listenersPerRelay: LISTENERS_PER_RELAY,
          listenerCapacity: listenerCapacity(room)
        }
      });
      broadcast(room, { type: "member-joined", member: user }, ws);
      return;
    }

    const meta = ws.meta;
    if (!meta) return send(ws, { type: "error", message: "Join a room first" });
    const room = rooms.get(meta.roomId);
    const sender = room?.peers.get(meta.peerId);
    if (!room || !sender || sender.ws !== ws || sender.sessionId !== meta.sessionId) return;

    if (msg.type === "leave-room") {
      removeSocket(ws);
      return;
    }
    if (msg.type === "signal") {
      const target = room.peers.get(String(msg.to || ""));
      if (target) send(target.ws, { type: "signal", from: meta.peerId, data: msg.data });
      return;
    }
    if (msg.type === "self-muted") {
      if (sender.user.role !== "speaker") return;
      updateMember(room, meta.peerId, { muted: Boolean(msg.muted), speaking: Boolean(msg.muted) ? false : sender.user.speaking });
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
      updateMember(room, meta.peerId, { requestedMic: true });
      return;
    }
    if (msg.type === "cancel-mic") {
      if (sender.user.role !== "listener") return;
      updateMember(room, meta.peerId, { requestedMic: false });
      return;
    }
    if (msg.type === "approve-mic") {
      if (!sender.user.isHost) return;
      const target = room.peers.get(String(msg.peerId || ""));
      if (!target || target.user.role !== "listener" || !target.user.requestedMic) return;
      if (speakers(room).length >= MAX_SPEAKERS) return send(ws, { type: "error", message: "All speaker seats are full" });
      // A mic request already represents the listener's intent to speak.
      // Approval tells their browser to acquire the mic and complete promotion.
      send(target.ws, { type: "mic-approved" });
      return;
    }
    if (msg.type === "invite-mic") {
      if (!sender.user.isHost) return;
      const target = room.peers.get(String(msg.peerId || ""));
      if (target?.user.role === "listener") send(target.ws, { type: "invite-mic", fromName: sender.user.name });
      return;
    }
    if (msg.type === "reject-mic") {
      if (!sender.user.isHost) return;
      const target = room.peers.get(String(msg.peerId || ""));
      if (!target || target.user.role !== "listener") return;
      updateMember(room, target.user.id, { requestedMic: false });
      send(target.ws, { type: "mic-request-rejected" });
      return;
    }
    if (msg.type === "accept-mic") {
      if (sender.user.role === "speaker") return;
      if (speakers(room).length >= MAX_SPEAKERS) return send(ws, { type: "error", message: "All speaker seats are full" });
      updateMember(room, meta.peerId, { role: "speaker", relayPeerId: null, requestedMic: false, speaking: false, muted: false });
      rebalance(room, false);
      return;
    }
    if (msg.type === "force-mute") {
      if (!sender.user.isHost) return;
      const peerId = String(msg.peerId || "");
      if (peerId === meta.peerId) return;
      const target = room.peers.get(peerId);
      if (!target || target.user.role !== "speaker") return;
      updateMember(room, peerId, { speaking: false, muted: true });
      send(target.ws, { type: "force-muted" });
      return;
    }
    if (msg.type === "leave-table") {
      if (sender.user.role !== "speaker") return;
      const remainingSpeakers = speakers(room).filter(entry => entry.user.id !== meta.peerId);
      if (!remainingSpeakers.length) return send(ws, { type: "error", message: "At least one table speaker is required" });
      updateMember(room, meta.peerId, { role: "listener", relayPeerId: null, requestedMic: false, speaking: false, muted: false });
      rebalance(room, true);
      return;
    }
    if (msg.type === "demote") {
      if (!sender.user.isHost) return;
      const peerId = String(msg.peerId || "");
      if (peerId === meta.peerId) return;
      const target = room.peers.get(peerId);
      if (target?.user.role !== "speaker") return;
      const remainingSpeakers = speakers(room).filter(entry => entry.user.id !== peerId);
      if (!remainingSpeakers.length) return send(ws, { type: "error", message: "At least one speaker is required" });
      updateMember(room, peerId, { role: "listener", relayPeerId: null, requestedMic: false, speaking: false, muted: false });
      rebalance(room, true);
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
      broadcast(room, { type: "member-left", peerId });
      try { target.ws.close(1000, "Removed by host"); } catch {}
      if (wasSpeaker) rebalance(room, true);
      return;
    }
    if (msg.type === "close-room") {
      if (!sender.user.isHost) return;
      broadcast(room, { type: "room-closed" });
      for (const entry of room.peers.values()) {
        entry.ws.meta = null;
        try { entry.ws.close(1000, "Room closed"); } catch {}
      }
      room.messages.length = 0;
      rooms.delete(meta.roomId);
      return;
    }
  });

  ws.on("close", () => removeSocket(ws));
  ws.on("error", () => removeSocket(ws));
});

server.listen(PORT, "0.0.0.0", () => console.log(`hello-voice-signaling v20 listening on port ${PORT}`));
