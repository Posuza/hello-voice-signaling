import http from "node:http";
import crypto from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 8787);
let MAX_SPEAKERS = 5;
const LISTENERS_PER_RELAY = 5;
let MAX_LISTENERS = 25;
let MAX_RELAYS = Math.ceil(MAX_LISTENERS / LISTENERS_PER_RELAY);
// Optional server-side policy refresh from the public, non-secret Next.js config endpoint.
// Defaults remain in effect if the API is unreachable; capacities never exceed tested ceilings.
const POLICY_URL = process.env.COMMUNICATION_CONFIG_URL || "";
async function refreshRoomPolicy() {
  if (!POLICY_URL) return;
  try {
    const response = await fetch(POLICY_URL, { signal: AbortSignal.timeout(4000), headers: { accept: "application/json" } });
    if (!response.ok) return;
    const json = await response.json();
    const room = json?.settings?.voiceRooms;
    if (!room || !Number.isInteger(room.maxSpeakers) || !Number.isInteger(room.maxListeners)) return;
    if (room.maxSpeakers < 1 || room.maxSpeakers > 5 || room.maxListeners < 1 || room.maxListeners > 25) return;
    MAX_SPEAKERS = room.maxSpeakers; MAX_LISTENERS = room.maxListeners;
    MAX_RELAYS = Math.ceil(MAX_LISTENERS / LISTENERS_PER_RELAY);
  } catch { /* retain last known policy */ }
}
if (POLICY_URL) { void refreshRoomPolicy(); setInterval(refreshRoomPolicy, 60000).unref(); }

// v92 single Upstash stream: fanout is independent from Turso persistence.
const liveClients=new Map();
const UPSTASH_URL=(process.env.UPSTASH_REDIS_REST_URL||'').replace(/\/$/,'');
const UPSTASH_TOKEN=process.env.UPSTASH_REDIS_REST_TOKEN||'';
const PERSIST_URL=process.env.MESSAGE_PERSIST_URL||'';
const WORKER_SECRET=process.env.QUEUE_WORKER_SECRET||'';
let liveCursor='$'; let pollBusy=false; let persistBusy=false;
async function streamCommand(command){
 const r=await fetch(UPSTASH_URL,{method:'POST',headers:{Authorization:`Bearer ${UPSTASH_TOKEN}`,'content-type':'application/json'},body:JSON.stringify(command),signal:AbortSignal.timeout(3500)});
 if(!r.ok)throw new Error(`Upstash ${r.status}`);
 const data=await r.json();if(data.error)throw new Error('Stream read failed');return data.result;
}
async function pollLive(){
 if(pollBusy||!UPSTASH_URL||!UPSTASH_TOKEN)return;
 pollBusy=true;
 try{
  if(liveCursor==='$') {const tail=await streamCommand(['XREVRANGE','hv:messages:v92','+','-','COUNT',1]);liveCursor=tail?.[0]?.[0]||'0-0';}
  const rows=await streamCommand(['XRANGE','hv:messages:v92',`(${liveCursor}`,'+','COUNT',100]);
  for(const [id,fields] of rows||[]){
   const i=fields.indexOf('payload');if(i<0)continue;
   let m;try{m=JSON.parse(fields[i+1])}catch{continue}
   if(m.kind==='message:new'&&typeof m.receiverId==='string'){
     for(const uid of [m.receiverId,m.senderId])for(const ws of liveClients.get(uid)||[])send(ws,{type:'chat-message',message:m});
   }
   liveCursor=id;
  }
 }catch(e){console.warn('Live stream poll:',e.message)}finally{pollBusy=false}
}
async function persistLive(){
 if(persistBusy||!PERSIST_URL||WORKER_SECRET.length<24)return;
 persistBusy=true;
 try{const r=await fetch(PERSIST_URL,{method:'POST',headers:{authorization:`Bearer ${WORKER_SECRET}`},signal:AbortSignal.timeout(12000)});if(!r.ok)console.warn('Turso persistence retry',r.status)}
 catch(e){console.warn('Persistence worker:',e.message)}finally{persistBusy=false}
}
if(UPSTASH_URL&&UPSTASH_TOKEN){setInterval(pollLive,20000).unref();}
if(PERSIST_URL&&WORKER_SECRET){setInterval(persistLive,30000).unref();}

const rooms = new Map();
// Dedicated authenticated private-call sockets. These are independent of room membership.
const callUsers = new Map(); // verified user ID -> socket
const activeCalls = new Map(); // call ID -> participants + state
const CALL_VERIFY_ORIGIN = (process.env.HELLO_VOICE_APP_ORIGIN || 'https://silly-kulfi-e876fc.netlify.app').replace(/\/$/, '');
async function verifyCallToken(token) {
  if (typeof token !== 'string' || token.length > 4096) return null;
  // Verify with the authoritative Next.js server. Render does not need a copied
  // call signing secret; never take verifier URLs from the browser.
  if (CALL_VERIFY_ORIGIN) {
    try {
      const r = await fetch(CALL_VERIFY_ORIGIN+'/api/call-verify', {
        method:'POST',headers:{'content-type':'application/json'},
        body:JSON.stringify({token}),signal:AbortSignal.timeout(5000)
      });
      if (r.ok) {
        const answer=await r.json();
        if(answer?.valid && typeof answer.id==='string' && typeof answer.name==='string')return {id:answer.id,name:answer.name};
      }
      return null;
    } catch { return null; }
  }

  try {
    const secret = process.env.CALL_SESSION_SECRET || "";
    if (secret.length < 32 || typeof token !== "string") return null;
    const bits = token.split(".");
    if (bits.length !== 2) return null;
    const [payload, signature] = bits;
    const expected = crypto.createHmac("sha256", secret).update(payload).digest("base64url");
    const a = Buffer.from(signature), b = Buffer.from(expected);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
    const data = JSON.parse(Buffer.from(payload, "base64url").toString());
    if (typeof data.sub !== "string" || !data.sub || typeof data.name !== "string" || data.exp < Date.now() || data.exp > Date.now() + 8 * 24 * 3600 * 1000) return null;
    return { id: data.sub, name: data.name };
  } catch { return null; }
}
function callEnd(callId, reason = "ended") {
  const call = activeCalls.get(callId);
  if (!call) return;
  activeCalls.delete(callId);
  for (const id of [call.from, call.to]) send(callUsers.get(id), { type: "call-ended", callId, reason });
}
async function handleCall(ws, msg) {
  if (msg.type === "call-register") {
    const identity = await verifyCallToken(msg.token);
    if (!identity) { console.warn("[call] registration rejected: token verification failed; check HELLO_VOICE_APP_ORIGIN and /api/call-verify reachability"); send(ws, { type: "call-error", message: "Authentication required" }); ws.close(4003, "Authentication required"); return; }
    if (ws.callIdentity) return;
    const previous = callUsers.get(identity.id);
    if (previous && previous !== ws) { try { previous.close(4001, "New connection"); } catch {} }
    ws.callIdentity = identity;
    console.info("[call] registration accepted", { userId: identity.id });
    callUsers.set(identity.id, ws);
    send(ws, { type: "call-registered", userId: identity.id });
    return;
  }
  const id = ws.callIdentity?.id;
  if (!id || callUsers.get(id) !== ws) { send(ws, { type: "call-error", message: "Register first" }); return; }
  if (msg.type === "call-invite") {
    const target = String(msg.to || "");
    if (!target || target === id || !callUsers.has(target)) return send(ws, { type: "call-error", message: "User is not online for calls" });
    if ([...activeCalls.values()].some(c => [c.from,c.to].includes(id) || [c.from,c.to].includes(target))) return send(ws, { type: "call-error", message: "A caller is busy" });
    const video = msg.media === "video";
    const callId = crypto.randomUUID();
    const call = { from: id, to: target, media: video ? "video" : "audio", status: "ringing", expires: Date.now() + 30000 };
    activeCalls.set(callId, call);
    send(ws, { type: "call-ringing", callId, to: target });
    send(callUsers.get(target), { type: "call-incoming", callId, from: id, name: ws.callIdentity.name, media: call.media });
    return;
  }
  const callId = String(msg.callId || "");
  const call = activeCalls.get(callId);
  if (!call || (call.from !== id && call.to !== id)) return;
  if (msg.type === "call-decline" || msg.type === "call-end") { callEnd(callId, msg.type === "call-decline" ? "declined" : "ended"); return; }
  if (msg.type === "call-accept") {
    if (call.to !== id || call.status !== "ringing" || call.expires < Date.now()) { callEnd(callId, "expired"); return; }
    call.status = "accepted";
    send(callUsers.get(call.from), { type: "call-accepted", callId, media: call.media });
    send(ws, { type: "call-accepted", callId, media: call.media });
    return;
  }
  if (msg.type === "call-signal" && call.status === "accepted") {
    const data = msg.data;
    const validDescription = data && (data.type === "offer" || data.type === "answer") && typeof data.sdp === "string" && data.sdp.length <= 48 * 1024;
    const validCandidate = data && typeof data.candidate === "string" && data.candidate.length <= 4096;
    if (!validDescription && !validCandidate) return;
    const other = id === call.from ? call.to : call.from;
    send(callUsers.get(other), { type: "call-signal", callId, data });
  }
}
setInterval(() => {
  for (const [callId, call] of activeCalls) if (call.status === "ringing" && call.expires < Date.now()) callEnd(callId, "timeout");
}, 5000).unref();


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

function patchMember(room, peerId, patch, announce = true) {
  const entry = room.peers.get(peerId);
  if (!entry) return null;
  const changed = Object.entries(patch).some(([key, value]) => entry.user[key] !== value);
  if (!changed) return entry;
  return updateMember(room, peerId, patch, announce);
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

function rebalanceBroadcast(room, announce = true) {
  const { casterId, standbyId } = pickCaster(room);
  room.casterPeerId = casterId;
  room.standbyCasterPeerId = standbyId;

  for (const speaker of speakers(room)) {
    const broadcastRole = speaker.user.id === casterId ? "caster" : speaker.user.id === standbyId ? "standby" : null;
    patchMember(room, speaker.user.id, { broadcastRole, broadcastParentPeerId: null, relayPeerId: null }, announce);
  }

  const audience = listeners(room).slice().sort((a, b) => (a.user.joinOrder || 0) - (b.user.joinOrder || 0));
  if (!casterId || !audience.length) {
    for (const listener of audience) {
      patchMember(room, listener.user.id, { broadcastRole: null, broadcastParentPeerId: null, relayPeerId: null }, announce);
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
    patchMember(room, relayId, { broadcastRole: "relay", broadcastParentPeerId: casterId, relayPeerId: casterId }, announce);
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
    patchMember(room, listener.user.id, { broadcastRole: null, broadcastParentPeerId: currentParent, relayPeerId: currentParent }, announce);
  }

  for (const listener of audience) {
    if (relaySet.has(listener.user.id) || preservedChildren.has(listener.user.id)) continue;
    const target = relayIds
      .slice()
      .sort((a, b) => (childLoads.get(a) || 0) - (childLoads.get(b) || 0))[0];
    if (!target) {
      patchMember(room, listener.user.id, { broadcastRole: null, broadcastParentPeerId: null, relayPeerId: null }, announce);
      continue;
    }
    childLoads.set(target, (childLoads.get(target) || 0) + 1);
    patchMember(room, listener.user.id, { broadcastRole: null, broadcastParentPeerId: target, relayPeerId: target }, announce);
  }
}


// A single authoritative topology update repairs missed/out-of-order role updates.
// The browser reconciles individual peer links rather than tearing down the entire room.
function publishTopology(room) {
  const current = members(room);
  for (const entry of room.peers.values()) {
    send(entry.ws, { type: "topology-snapshot", self: entry.user, members: current.filter(m => m.id !== entry.user.id) });
  }
}
function scheduleTopology(room) {
  if (room.topologyQueued) return;
  room.topologyQueued = true;
  queueMicrotask(() => {
    room.topologyQueued = false;
    if (room.peers.size) publishTopology(room);
  });
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
  if (room.hostPeerId === meta.peerId) {
    const next = entries(room).sort((a,b) => (a.user.joinOrder||0) - (b.user.joinOrder||0))[0];
    room.hostPeerId = next?.user.id ?? null;
    if (next) updateMember(room, next.user.id, { isHost: true });
  }
  if (announce) broadcast(room, { type: "member-left", peerId: meta.peerId });

  if (room.peers.size === 0) {
    room.messages.length = 0;
    rooms.delete(meta.roomId);
  } else {
    rebalanceBroadcast(room);
  }
  if (room.peers.size) scheduleTopology(room);
  ws.meta = null;
}

const server = http.createServer((req, res) => {
  if(req.url==='/internal/live-message'&&req.method==='POST'){
    if(WORKER_SECRET.length<24||req.headers.authorization!==`Bearer ${WORKER_SECRET}`){res.writeHead(401);return res.end('Unauthorized')}
    let content='';req.on('data',chunk=>{content+=chunk.toString();if(content.length>6000)req.destroy()});
    req.on('end',()=>{
      try{
        const m=JSON.parse(content);
        if(m.kind!=='message:new'||typeof m.id!=='string'||typeof m.senderId!=='string'||typeof m.receiverId!=='string'||typeof m.text!=='string')throw Error('Invalid event');
        for(const uid of [m.receiverId,m.senderId])for(const ws of liveClients.get(uid)||[])send(ws,{type:'chat-message',message:m});
        res.writeHead(200);res.end('OK');
      }catch{res.writeHead(400);res.end('Invalid event')}
    });return;
  }
  res.setHeader("access-control-allow-origin", "*");
  res.setHeader("cache-control", "no-store");
  res.setHeader("content-type", "application/json");
  if (req.url === "/rooms") {
    res.writeHead(200);
    return res.end(JSON.stringify({ rooms: roomStats(), maxSpeakers: MAX_SPEAKERS, maxListeners: MAX_LISTENERS, listenersPerRelay: LISTENERS_PER_RELAY, maxRelays: MAX_RELAYS }));
  }
  res.writeHead(200);
  res.end(JSON.stringify({ ok: true, service: "hello-voice-signaling", protocol: 83, mode: "p2p-conversation-caster-relay", maxSpeakers: MAX_SPEAKERS, maxListeners: MAX_LISTENERS, listenersPerRelay: LISTENERS_PER_RELAY, maxRelays: MAX_RELAYS }));
});

// Bound incoming frames to protect the signaling process from huge messages.
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch { ws.terminate(); }
  }
}, 30000);
wss.on("close", () => clearInterval(heartbeat));
wss.on("connection", ws => {
  ws.meta = null;
  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });
  ws.on("close", () => {
    const chatId=ws.chatIdentity?.id;
    if(chatId){const clients=liveClients.get(chatId);clients?.delete(ws);if(!clients?.size)liveClients.delete(chatId)}
    const id = ws.callIdentity?.id;
    if (id && callUsers.get(id) === ws) {
      callUsers.delete(id);
      for (const [callId, call] of activeCalls) if (call.from === id || call.to === id) callEnd(callId, "disconnected");
    }
  });
  ws.signalWindowAt = Date.now();
  ws.signalCount = 0;

  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { return send(ws, { type: "error", message: "Invalid JSON" }); }

    if(msg.type==='chat-register'){
      void (async () => {const identity=await verifyCallToken(msg.token);
      if(!identity){send(ws,{type:'chat-error',message:'Unauthorized'});return}
      ws.chatIdentity=identity;
      if(!liveClients.has(identity.id))liveClients.set(identity.id,new Set());
      liveClients.get(identity.id).add(ws);
      send(ws,{type:'chat-registered'});})(); return;
    }
    if (typeof msg.type === "string" && msg.type.startsWith("call-")) {
      void handleCall(ws, msg).catch(() => send(ws,{type:"call-error",message:"Signaling verification unavailable"}));
      return;
    }

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
      scheduleTopology(room);
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
      // Signaling is allowed only between members of the same active room.
      const now = Date.now();
      if (now - ws.signalWindowAt >= 1000) { ws.signalWindowAt = now; ws.signalCount = 0; }
      if (++ws.signalCount > 90) return;
      const data = msg.data;
      const validDescription = data && (data.type === "offer" || data.type === "answer") && typeof data.sdp === "string" && data.sdp.length <= 48 * 1024;
      const validCandidate = data && typeof data.candidate === "string" && data.candidate.length <= 4096;
      if (!validDescription && !validCandidate) return send(ws, { type: "error", message: "Invalid WebRTC signal" });
      const recipient = String(msg.to || "");
      if (!recipient || recipient === meta.peerId) return;
      const target = room.peers.get(recipient);
      if (target) send(target.ws, { type: "signal", from: meta.peerId, data });
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
      updateMember(room, meta.peerId, { role: "speaker", relayPeerId: null, requestedMic: false, speaking: false, selfMuted: false, hostMuted: false, muted: false, seatIndex: nextSeatIndex(room) }, false);
      rebalanceBroadcast(room, false);
      scheduleTopology(room);
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
      updateMember(room, meta.peerId, { role: "listener", relayPeerId: null, requestedMic: false, speaking: false, selfMuted: false, hostMuted: false, muted: false, seatIndex: null }, false);
      rebalanceBroadcast(room, false);
      scheduleTopology(room);
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
      updateMember(room, peerId, { role: "listener", relayPeerId: null, requestedMic: false, speaking: false, selfMuted: false, hostMuted: false, muted: false, seatIndex: null }, false);
      rebalanceBroadcast(room, false);
      scheduleTopology(room);
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
      scheduleTopology(room);
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

  ws.on("close", () => {
    removeSocket(ws);
  });
  ws.on("error", () => removeSocket(ws));
});

server.listen(PORT, "0.0.0.0", () => console.log(`hello-voice-signaling voice-only listening on port ${PORT}`));
