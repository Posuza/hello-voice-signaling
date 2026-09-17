import http from "node:http";
import crypto from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 8787);
const MAX_SPEAKERS = 5;
const LISTENERS_PER_RELAY = 5;
const MAX_LISTENERS = 25;
const MAX_RELAYS = Math.ceil(MAX_LISTENERS / LISTENERS_PER_RELAY);
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
function listenerCapacity(room) { return speakers(room).length > 0 ? MAX_LISTENERS : 0; }

function nextSeatIndex(room) {
  const used = new Set(speakers(room).map(entry => entry.user.seatIndex).filter(index => Number.isInteger(index)));
  for (let i = 0; i < MAX_SPEAKERS; i++) if (!used.has(i)) return i;
  return null;
}

function counts(room) {
  return {
    participantCount: room.peers.size,
    speakerCount: speakers(room).length,
    audienceCount: listeners(room).length,
    listenerCapacity: listenerCapacity(room),
    maxSpeakers: MAX_SPEAKERS,
    maxListeners: MAX_LISTENERS,
    listenersPerRelay: LISTENERS_PER_RELAY,
    casterPeerId: room.casterPeerId || null,
    standbyCasterPeerId: room.standbyCasterPeerId || null,
    relayCount: listeners(room).filter(entry => entry.user.broadcastRole === "relay").length
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

function patchMember(room, peerId, patch) {
  const entry = room.peers.get(peerId);
  if (!entry) return null;
  const changed = Object.entries(patch).some(([key, value]) => entry.user[key] !== value);
  if (!changed) return entry;
  return updateMember(room, peerId, patch);
}

function pickCaster(room) {
  const available = speakers(room).slice().sort((a, b) => {
    if (a.user.isHost !== b.user.isHost) return a.user.isHost ? -1 : 1;
    return (a.user.joinOrder || 0) - (b.user.joinOrder || 0);
  });
  if (!available.length) return { casterId: null, standbyId: null };

  const currentCaster = available.find(entry => entry.user.broadcastRole === "caster");
  const previousStandby = available.find(entry => entry.user.broadcastRole === "standby");
  // If the primary disappears, promote the already-warm standby first.
  const caster = currentCaster || previousStandby || available[0];
  const currentStandby = available.find(entry => entry.user.id !== caster.user.id && entry.user.broadcastRole === "standby");
  const standby = currentStandby || available.find(entry => entry.user.id !== caster.user.id) || null;
  return { casterId: caster.user.id, standbyId: standby?.user.id ?? null };
}

function rebalanceBroadcast(room) {
  const { casterId, standbyId } = pickCaster(room);
  room.casterPeerId = casterId;
  room.standbyCasterPeerId = standbyId;

  for (const speaker of speakers(room)) {
    const broadcastRole = speaker.user.id === casterId ? "caster" : speaker.user.id === standbyId ? "standby" : null;
    patchMember(room, speaker.user.id, { broadcastRole, broadcastParentPeerId: null, relayPeerId: null });
  }

  const audience = listeners(room).slice().sort((a, b) => (a.user.joinOrder || 0) - (b.user.joinOrder || 0));
  if (!casterId || !audience.length) {
    for (const listener of audience) {
      patchMember(room, listener.user.id, { broadcastRole: null, broadcastParentPeerId: null, relayPeerId: null });
      if (!casterId) send(listener.ws, { type: "relay-unavailable" });
    }
    return;
  }

  const requiredRelays = Math.min(MAX_RELAYS, Math.ceil(audience.length / LISTENERS_PER_RELAY));
  const relayIds = [];

  for (const listener of audience) {
    if (listener.user.broadcastRole === "relay" && relayIds.length < requiredRelays) relayIds.push(listener.user.id);
  }
  for (const listener of audience) {
    if (relayIds.length >= requiredRelays) break;
    if (!relayIds.includes(listener.user.id)) relayIds.push(listener.user.id);
  }

  const relaySet = new Set(relayIds);
  const childLoads = new Map(relayIds.map(id => [id, 0]));
  const childCapacity = Math.max(0, LISTENERS_PER_RELAY - 1);

  for (const relayId of relayIds) {
    patchMember(room, relayId, { broadcastRole: "relay", broadcastParentPeerId: casterId, relayPeerId: casterId });
  }

  // Preserve stable child assignments when the relay still exists and has room.
  const preservedChildren = new Set();
  for (const listener of audience) {
    if (relaySet.has(listener.user.id)) continue;
    const currentParent = listener.user.broadcastParentPeerId || listener.user.relayPeerId;
    if (!currentParent || !relaySet.has(currentParent)) continue;
    const load = childLoads.get(currentParent) || 0;
    if (load >= childCapacity) continue;
    childLoads.set(currentParent, load + 1);
    preservedChildren.add(listener.user.id);
    patchMember(room, listener.user.id, { broadcastRole: null, broadcastParentPeerId: currentParent, relayPeerId: currentParent });
  }

  for (const listener of audience) {
    if (relaySet.has(listener.user.id) || preservedChildren.has(listener.user.id)) continue;
    const target = relayIds
      .slice()
      .sort((a, b) => (childLoads.get(a) || 0) - (childLoads.get(b) || 0))[0];
    if (!target) {
      patchMember(room, listener.user.id, { broadcastRole: null, broadcastParentPeerId: null, relayPeerId: null });
      continue;
    }
    childLoads.set(target, (childLoads.get(target) || 0) + 1);
    patchMember(room, listener.user.id, { broadcastRole: null, broadcastParentPeerId: target, relayPeerId: target });
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
  } else {
    rebalanceBroadcast(room);
  }
  ws.meta = null;
}

const server = http.createServer((req, res) => {
  res.setHeader("access-control-allow-origin", "*");
  res.setHeader("cache-control", "no-store");
  res.setHeader("content-type", "application/json");
  if (req.url === "/rooms") {
    res.writeHead(200);
    return res.end(JSON.stringify({ rooms: roomStats(), maxSpeakers: MAX_SPEAKERS, maxListeners: MAX_LISTENERS, listenersPerRelay: LISTENERS_PER_RELAY, maxRelays: MAX_RELAYS }));
  }
  res.writeHead(200);
  res.end(JSON.stringify({ ok: true, service: "hello-voice-signaling", protocol: 44, mode: "p2p-conversation-caster-relay", maxSpeakers: MAX_SPEAKERS, maxListeners: MAX_LISTENERS, listenersPerRelay: LISTENERS_PER_RELAY, maxRelays: MAX_RELAYS }));
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
        room = { peers: new Map(), messages: [], hostPeerId: null, nextJoinOrder: 1, casterPeerId: null, standbyCasterPeerId: null };
        rooms.set(roomId, room);
      }

      const existing = room.peers.get(peerId);
      const previousUser = existing?.user ? { ...existing.user } : null;
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
      const preservedRole = previousUser?.role === "speaker" ? "speaker" : null;
      const role = isHost ? "speaker" : (preservedRole || (requestedRole === "speaker" && speakers(room).length < MAX_SPEAKERS ? "speaker" : "listener"));

      let relayPeerId = null;
      if (role === "listener") {
        const capacity = listenerCapacity(room);
        if (listeners(room).length >= capacity) return send(ws, { type: "room-full", listenerCapacity: capacity, maxListeners: MAX_LISTENERS });
      }

      const joinOrder = previousUser?.joinOrder ?? room.nextJoinOrder++;
      const seatIndex = role === "speaker" ? (isHost ? 0 : (previousUser?.seatIndex ?? nextSeatIndex(room))) : null;
      const selfMuted = Boolean(previousUser?.selfMuted);
      const hostMuted = Boolean(previousUser?.hostMuted);
      const muted = role === "speaker" ? (selfMuted || hostMuted) : false;
      const user = {
        id: peerId, name, role, relayPeerId, broadcastRole: null, broadcastParentPeerId: null, speaking: false,
        selfMuted: role === "speaker" ? selfMuted : false,
        hostMuted: role === "speaker" ? hostMuted : false,
        muted, requestedMic: false, isHost, joinOrder, seatIndex
      };
      room.peers.set(peerId, { ws, user, sessionId });
      ws.meta = { roomId, peerId, sessionId };
      rebalanceBroadcast(room);
      const currentSelf = room.peers.get(peerId)?.user || user;
      const existingMembers = members(room).filter(member => member.id !== peerId);

      send(ws, {
        type: "room-snapshot",
        roomId,
        sessionId,
        self: currentSelf,
        members: existingMembers,
        messages: [...room.messages],
        limits: {
          maxSpeakers: MAX_SPEAKERS,
          maxListeners: MAX_LISTENERS,
          listenersPerRelay: LISTENERS_PER_RELAY,
          listenerCapacity: listenerCapacity(room),
          maxRelays: MAX_RELAYS,
          relayGroupSize: LISTENERS_PER_RELAY
        }
      });
      broadcast(room, { type: "member-joined", member: currentSelf }, ws);
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
      const selfMuted = Boolean(msg.muted);
      if (sender.user.hostMuted && !selfMuted) {
        send(ws, { type: "mic-control", disabled: true, muted: true });
        return;
      }
      const muted = selfMuted || Boolean(sender.user.hostMuted);
      updateMember(room, meta.peerId, { selfMuted, muted, speaking: muted ? false : sender.user.speaking });
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
      updateMember(room, meta.peerId, { role: "speaker", relayPeerId: null, requestedMic: false, speaking: false, selfMuted: false, hostMuted: false, muted: false, seatIndex: nextSeatIndex(room) });
      rebalanceBroadcast(room);
      return;
    }
    if (msg.type === "set-mic-disabled") {
      if (!sender.user.isHost) return;
      const peerId = String(msg.peerId || "");
      if (peerId === meta.peerId) return;
      const target = room.peers.get(peerId);
      if (!target || target.user.role !== "speaker") return;
      const disabled = Boolean(msg.disabled);
      const muted = disabled || Boolean(target.user.selfMuted);
      updateMember(room, peerId, { hostMuted: disabled, muted, speaking: muted ? false : target.user.speaking });
      send(target.ws, { type: "mic-control", disabled, muted });
      return;
    }
    if (msg.type === "leave-table") {
      if (sender.user.role !== "speaker" || sender.user.isHost) return;
      const remainingSpeakers = speakers(room).filter(entry => entry.user.id !== meta.peerId);
      if (!remainingSpeakers.length) return send(ws, { type: "error", message: "At least one table speaker is required" });
      updateMember(room, meta.peerId, { role: "listener", relayPeerId: null, requestedMic: false, speaking: false, selfMuted: false, hostMuted: false, muted: false, seatIndex: null });
      rebalanceBroadcast(room);
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
      updateMember(room, peerId, { role: "listener", relayPeerId: null, requestedMic: false, speaking: false, selfMuted: false, hostMuted: false, muted: false, seatIndex: null });
      rebalanceBroadcast(room);
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
      rebalanceBroadcast(room);
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

server.listen(PORT, "0.0.0.0", () => console.log(`hello-voice-signaling v44 listening on port ${PORT}`));
