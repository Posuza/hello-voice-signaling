import http from "node:http";
import crypto from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 8787);
const MAX_SPEAKERS = 5;
const LISTENERS_PER_RELAY = 5;
const MAX_LISTENERS = 25;
const MAX_RELAYS = Math.ceil(MAX_LISTENERS / LISTENERS_PER_RELAY);
const rooms = new Map();

// v52 game platform: isolated from the existing voice-room state.
const gameRooms = new Map();
const gameMatches = new Map();
const gameConnections = new Map();
const GAME_TEAMS = [
  { color:"#2f80ed", emblem:"crown", assetTheme:"royal-blue" },
  { color:"#e0565b", emblem:"dragon", assetTheme:"crimson" },
  { color:"#29a66a", emblem:"oak", assetTheme:"emerald" },
  { color:"#8d63d6", emblem:"raven", assetTheme:"violet" },
  { color:"#d7a629", emblem:"sun", assetTheme:"gold" }
];
const KINGDOM_MAPS = {
  "green-valley": {
    spawns:[[260,260],[2140,260],[2140,1340],[260,1340],[1200,210]],
    territories:[
      {id:"center",x:1200,y:800,radius:120,kind:"central"},
      {id:"gold-nw",x:720,y:520,radius:85,kind:"gold"},
      {id:"wood-ne",x:1680,y:520,radius:85,kind:"wood"},
      {id:"food-sw",x:720,y:1080,radius:85,kind:"food"},
      {id:"gold-se",x:1680,y:1080,radius:85,kind:"gold"}
    ]
  },
  "riverlands": {
    spawns:[[300,300],[2050,280],[2100,1290],[320,1280],[1750,780]],
    territories:[
      {id:"center",x:1200,y:800,radius:110,kind:"central"},
      {id:"gold-west",x:620,y:650,radius:82,kind:"gold"},
      {id:"wood-east",x:1780,y:650,radius:82,kind:"wood"},
      {id:"food-south",x:1200,y:1210,radius:82,kind:"food"},
      {id:"gold-north",x:1200,y:360,radius:82,kind:"gold"}
    ]
  },
  "black-forest": {
    spawns:[[320,260],[2080,290],[2060,1320],[330,1320],[1200,1320]],
    territories:[
      {id:"center",x:1200,y:760,radius:105,kind:"central"},
      {id:"wood-nw",x:700,y:500,radius:78,kind:"wood"},
      {id:"wood-ne",x:1700,y:500,radius:78,kind:"wood"},
      {id:"food-sw",x:760,y:1110,radius:78,kind:"food"},
      {id:"gold-se",x:1650,y:1110,radius:78,kind:"gold"}
    ]
  },
  "crown-basin": {
    spawns:[[280,800],[1200,250],[2120,800],[1720,1320],[680,1320]],
    territories:[
      {id:"center",x:1200,y:800,radius:145,kind:"central"},
      {id:"gold-west",x:620,y:800,radius:78,kind:"gold"},
      {id:"wood-east",x:1780,y:800,radius:78,kind:"wood"},
      {id:"food-north",x:1200,y:420,radius:78,kind:"food"},
      {id:"gold-south",x:1200,y:1190,radius:78,kind:"gold"}
    ]
  }
};
const GAME_MIN_PLAYERS = 2;
const GAME_MAX_PLAYERS = 5;
const GAME_RECONNECT_GRACE_MS = 45_000;

const UNIT_CONFIG = {
  swordsman: { hp:120, attack:20, defense:8, speed:58, range:44, cost:{gold:55,wood:10,food:25} },
  archer: { hp:82, attack:18, defense:3, speed:54, range:160, cost:{gold:60,wood:25,food:20} },
  spearman: { hp:110, attack:17, defense:7, speed:55, range:58, cost:{gold:50,wood:20,food:24} },
  knight: { hp:165, attack:28, defense:12, speed:82, range:46, cost:{gold:100,wood:15,food:45} },
  siege: { hp:210, attack:52, defense:5, speed:28, range:190, cost:{gold:140,wood:90,food:30} }
};
const BUILDING_CONFIG = {
  farm:{hp:220,cost:{gold:30,wood:60,food:0}}, mine:{hp:260,cost:{gold:25,wood:80,food:0}}, lumberMill:{hp:250,cost:{gold:25,wood:55,food:0}},
  barracks:{hp:360,cost:{gold:80,wood:110,food:0}}, stable:{hp:340,cost:{gold:120,wood:130,food:0}}, tower:{hp:420,cost:{gold:85,wood:100,food:0}}, wall:{hp:500,cost:{gold:15,wood:45,food:0}}
};


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


function gameRoomPublic(room) {
  return {
    id: room.id, gameId: room.gameId, name: room.name, hostUserId: room.hostUserId, state: room.state,
    mapId: room.mapId, mode: room.mode, minPlayers: room.minPlayers, maxPlayers: room.maxPlayers,
    players: [...room.players.values()].map(p => ({ userId:p.userId, name:p.name, ready:p.ready, isHost:p.userId === room.hostUserId, connected:p.connected, color:p.color, emblem:p.emblem, assetTheme:p.assetTheme })),
    matchId: room.matchId || null, createdAt: room.createdAt
  };
}
function broadcastGameRoom(room, payload) {
  for (const p of room.players.values()) if (p.ws) send(p.ws, payload);
}
function broadcastGameRoomList(gameId = "kingdom") {
  const payload = { type:"game:room-list", gameId, rooms:[...gameRooms.values()].filter(r => r.gameId === gameId && r.state !== "CLOSED").map(gameRoomPublic) };
  for (const [ws, meta] of gameConnections) if (meta.gameId === gameId || !meta.roomId) send(ws, payload);
}
function createKingdomMatch(room) {
  const matchId = crypto.randomUUID();
  const players = [...room.players.values()];
  const map = KINGDOM_MAPS[room.mapId] || KINGDOM_MAPS["green-valley"];
  const spawns = map.spawns;
  const state = {
    matchId, roomId:room.id, gameId:"kingdom", mapId:room.mapId, state:"PLAYING", world:{width:2400,height:1600}, startedAt:Date.now(), winnerId:null,
    players: players.map((p,i)=>({userId:p.userId,name:p.name,alive:true,resources:{gold:320,wood:280,food:220},color:p.color,emblem:p.emblem,assetTheme:p.assetTheme})),
    units:[], buildings:[], territories:map.territories.map(t=>({...t,ownerId:null})), stats:{}
  };
  players.forEach((p,i)=>{
    const [x,y] = spawns[i] || [1200,800];
    state.stats[p.userId] = {unitsTrained:2,unitsLost:0,buildingsBuilt:1,damage:0,territories:0};
    state.buildings.push({id:crypto.randomUUID(),ownerId:p.userId,type:"castle",x,y,hp:900,maxHp:900,level:1});
    for (let n=0;n<2;n++) state.units.push({id:crypto.randomUUID(),ownerId:p.userId,type:n?"archer":"swordsman",x:x+70+n*32,y:y+80,tx:x+70+n*32,ty:y+80,hp:n?82:120,maxHp:n?82:120,attack:n?18:20,defense:n?3:8,speed:n?54:58,range:n?160:44,targetId:null});
  });
  gameMatches.set(matchId,{state,roomId:room.id,lastTick:Date.now()});
  room.matchId = matchId; room.state = "PLAYING";
  return state;
}
function gamePlayerState(match, userId) { return match.state.players.find(p => p.userId === userId); }
function canAfford(res,cost){return res.gold>=cost.gold&&res.wood>=cost.wood&&res.food>=cost.food}
function spend(res,cost){res.gold-=cost.gold;res.wood-=cost.wood;res.food-=cost.food}
function gameCommand(match, userId, command) {
  const state = match.state; const player = gamePlayerState(match,userId); if (!player || !player.alive || state.state !== "PLAYING") return;
  const kind = String(command.kind||"");
  if (kind === "unit:create") {
    const type=String(command.unitType||""); const cfg=UNIT_CONFIG[type]; if(!cfg||!canAfford(player.resources,cfg.cost))return;
    const castle=state.buildings.find(b=>b.ownerId===userId&&b.type==="castle"); if(!castle)return; spend(player.resources,cfg.cost);
    const unit={id:crypto.randomUUID(),ownerId:userId,type,x:castle.x+90+Math.random()*50,y:castle.y+80+Math.random()*50,tx:castle.x+90,ty:castle.y+80,hp:cfg.hp,maxHp:cfg.hp,attack:cfg.attack,defense:cfg.defense,speed:cfg.speed,range:cfg.range,targetId:null};
    state.units.push(unit); state.stats[userId].unitsTrained++;
  } else if (kind === "unit:move") {
    const ids=Array.isArray(command.unitIds)?command.unitIds.map(String):[String(command.unitId||"")]; const x=Math.max(20,Math.min(2380,Number(command.x)||0)); const y=Math.max(20,Math.min(1580,Number(command.y)||0));
    state.units.filter(u=>ids.includes(u.id)&&u.ownerId===userId).forEach((u,i)=>{u.tx=x+(i%4)*18;u.ty=y+Math.floor(i/4)*18;u.targetId=null});
  } else if (kind === "unit:stop" || kind === "unit:defend") {
    const ids=Array.isArray(command.unitIds)?command.unitIds.map(String):[String(command.unitId||"")];
    state.units.filter(u=>ids.includes(u.id)&&u.ownerId===userId).forEach(u=>{u.tx=u.x;u.ty=u.y;u.targetId=null});
  } else if (kind === "unit:attack") {
    const ids=Array.isArray(command.unitIds)?command.unitIds.map(String):[String(command.unitId||"")]; const targetId=String(command.targetId||"");
    const target = state.units.find(u=>u.id===targetId&&u.ownerId!==userId)||state.buildings.find(b=>b.id===targetId&&b.ownerId!==userId); if(!target)return;
    state.units.filter(u=>ids.includes(u.id)&&u.ownerId===userId).forEach(u=>u.targetId=targetId);
  } else if (kind === "building:create") {
    const type=String(command.buildingType||""); const cfg=BUILDING_CONFIG[type]; if(!cfg||!canAfford(player.resources,cfg.cost))return;
    const x=Math.max(80,Math.min(2320,Number(command.x)||0)); const y=Math.max(80,Math.min(1520,Number(command.y)||0));
    const tooClose=state.buildings.some(b=>Math.hypot(b.x-x,b.y-y)<90); if(tooClose)return; spend(player.resources,cfg.cost);
    state.buildings.push({id:crypto.randomUUID(),ownerId:userId,type,x,y,hp:cfg.hp,maxHp:cfg.hp,level:1}); state.stats[userId].buildingsBuilt++;
  } else if (kind === "building:upgrade") {
    const b=state.buildings.find(b=>b.id===String(command.buildingId||"")&&b.ownerId===userId); if(!b||b.level>=3)return; const cost={gold:80*b.level,wood:70*b.level,food:0}; if(!canAfford(player.resources,cost))return; spend(player.resources,cost);b.level++;b.maxHp=Math.round(b.maxHp*1.25);b.hp=b.maxHp;
  } else if (kind === "surrender") { player.alive=false; }
}
function tickGame(match, dt) {
  const s=match.state;if(s.state!=="PLAYING")return;
  for(const p of s.players){if(!p.alive)continue; const buildings=s.buildings.filter(b=>b.ownerId===p.userId); const terr=s.territories.filter(t=>t.ownerId===p.userId); p.resources.gold+=dt*(3+buildings.filter(b=>b.type==="mine").length*2.2+terr.filter(t=>t.kind==="gold"||t.kind==="central").length*1.5);p.resources.wood+=dt*(2.5+buildings.filter(b=>b.type==="lumberMill").length*2+terr.filter(t=>t.kind==="wood"||t.kind==="central").length*1.3);p.resources.food+=dt*(2.3+buildings.filter(b=>b.type==="farm").length*2.4+terr.filter(t=>t.kind==="food"||t.kind==="central").length*1.3);}
  for(const u of s.units){
    const target = u.targetId ? (s.units.find(x=>x.id===u.targetId)||s.buildings.find(x=>x.id===u.targetId)) : null;
    if(target){const d=Math.hypot(target.x-u.x,target.y-u.y);if(d<=u.range){let mult=1;if(target.type){if(u.type==="spearman"&&target.type==="knight")mult=1.55;else if(u.type==="knight"&&target.type==="archer")mult=1.3;else if(u.type==="archer"&&target.type==="swordsman")mult=1.2;}else if(u.type==="siege")mult=1.8;const dmg=Math.max(2,(u.attack-(target.defense||0)*.35)*mult)*dt;target.hp-=dmg;s.stats[u.ownerId].damage+=dmg;}else{u.tx=target.x;u.ty=target.y;}}
    const dx=u.tx-u.x,dy=u.ty-u.y,d=Math.hypot(dx,dy);if(d>2){const step=Math.min(d,u.speed*dt);u.x+=dx/d*step;u.y+=dy/d*step;}
  }
  const deadUnits=s.units.filter(u=>u.hp<=0);for(const u of deadUnits)if(s.stats[u.ownerId])s.stats[u.ownerId].unitsLost++;s.units=s.units.filter(u=>u.hp>0);
  const deadBuildings=s.buildings.filter(b=>b.hp<=0);s.buildings=s.buildings.filter(b=>b.hp>0);
  for(const b of deadBuildings){if(b.type==="castle"){const p=gamePlayerState(match,b.ownerId);if(p)p.alive=false;}}
  for(const t of s.territories){const close=s.units.filter(u=>Math.hypot(u.x-t.x,u.y-t.y)<t.radius);const owners=[...new Set(close.map(u=>u.ownerId))];if(owners.length===1&&t.ownerId!==owners[0]){t.ownerId=owners[0];for(const id of Object.keys(s.stats))s.stats[id].territories=s.territories.filter(x=>x.ownerId===id).length;}}
  const alive=s.players.filter(p=>p.alive);if(alive.length<=1&&s.players.length>=2){s.state="FINISHED";s.endedAt=Date.now();s.winnerId=alive[0]?.userId||null;const room=gameRooms.get(match.roomId);if(room){room.state="FINISHED";broadcastGameRoom(room,{type:"game:end",snapshot:s});broadcastGameRoom(room,{type:"game:room-snapshot",room:gameRoomPublic(room)});}setTimeout(()=>gameMatches.delete(s.matchId),10*60_000);}
}
setInterval(()=>{const now=Date.now();for(const match of gameMatches.values()){const dt=Math.min(.25,(now-match.lastTick)/1000);match.lastTick=now;tickGame(match,dt);const room=gameRooms.get(match.roomId);if(room&&match.state.state==="PLAYING")broadcastGameRoom(room,{type:"game:delta",snapshot:match.state});}},200);
function handleGameMessage(ws,msg){
  if(!String(msg.type||"").startsWith("game:"))return false;
  const type=String(msg.type);const userId=String(msg.userId||"").trim();const name=String(msg.name||"Player").trim().slice(0,32);const gameId=String(msg.gameId||"kingdom");
  if(type==="game:list"){gameConnections.set(ws,{gameId,roomId:null,userId});send(ws,{type:"game:room-list",gameId,rooms:[...gameRooms.values()].filter(r=>r.gameId===gameId&&r.state!=="CLOSED").map(gameRoomPublic)});return true;}
  if(type==="game:create"){
    if(!userId)return send(ws,{type:"game:error",message:"Sign in first"})||true; const id=crypto.randomUUID();const requestedMap=String(msg.mapId||"green-valley");const mapId=KINGDOM_MAPS[requestedMap]?requestedMap:"green-valley";const room={id,gameId,name:String(msg.roomName||"Kingdom Room").trim().slice(0,50)||"Kingdom Room",hostUserId:userId,state:"WAITING",mapId,mode:"free-for-all",minPlayers:2,maxPlayers:5,players:new Map(),matchId:null,createdAt:Date.now()};
    const team=GAME_TEAMS[0];room.players.set(userId,{userId,name,ready:false,connected:true,ws,color:team.color,emblem:team.emblem,assetTheme:team.assetTheme,disconnectedAt:null});gameRooms.set(id,room);gameConnections.set(ws,{gameId,roomId:id,userId});send(ws,{type:"game:room-snapshot",room:gameRoomPublic(room)});broadcastGameRoomList(gameId);return true;
  }
  if(type==="game:join"||type==="game:reconnect"){
    const room=gameRooms.get(String(msg.roomId||""));if(!room)return send(ws,{type:"game:error",message:"Game room not found"})||true;if(!userId)return true;
    let p=room.players.get(userId);if(!p){if(room.state!=="WAITING")return send(ws,{type:"game:error",message:"Match already started"})||true;if(room.players.size>=room.maxPlayers)return send(ws,{type:"game:error",message:"Game room is full"})||true;const team=GAME_TEAMS[room.players.size%GAME_TEAMS.length];p={userId,name,ready:false,connected:true,ws,color:team.color,emblem:team.emblem,assetTheme:team.assetTheme,disconnectedAt:null};room.players.set(userId,p);}else{p.ws=ws;p.connected=true;p.disconnectedAt=null;p.name=name||p.name;}
    gameConnections.set(ws,{gameId:room.gameId,roomId:room.id,userId});send(ws,{type:"game:room-snapshot",room:gameRoomPublic(room)});broadcastGameRoom(room,{type:"game:room-snapshot",room:gameRoomPublic(room)});if(room.matchId){const m=gameMatches.get(room.matchId);if(m)send(ws,{type:"game:snapshot",snapshot:m.state});}broadcastGameRoomList(room.gameId);return true;
  }
  if(type==="game:snapshot") {
    const m=gameMatches.get(String(msg.matchId||"")); if(!m) return send(ws,{type:"game:error",message:"Match not found"})||true;
    const r=gameRooms.get(m.roomId); const p=r?.players.get(userId); if(!r||!p) return send(ws,{type:"game:error",message:"You are not a participant in this match"})||true;
    p.ws=ws; p.connected=true; p.disconnectedAt=null; gameConnections.set(ws,{gameId:r.gameId,roomId:r.id,userId}); send(ws,{type:"game:snapshot",snapshot:m.state}); broadcastGameRoom(r,{type:"game:room-snapshot",room:gameRoomPublic(r)}); return true;
  }
  const meta=gameConnections.get(ws);const room=meta?.roomId?gameRooms.get(meta.roomId):null;
  if(type==="game:leave"){if(room&&userId){if(room.matchId){const m=gameMatches.get(room.matchId);const gp=m&&gamePlayerState(m,userId);if(gp)gp.alive=false;}room.players.delete(userId);if(room.hostUserId===userId){const next=[...room.players.keys()][0];if(next)room.hostUserId=next;}if(!room.players.size){room.state="CLOSED";gameRooms.delete(room.id);}else broadcastGameRoom(room,{type:"game:room-snapshot",room:gameRoomPublic(room)});broadcastGameRoomList(room.gameId);}gameConnections.set(ws,{gameId,roomId:null,userId});return true;}
  if(!room||!userId)return true;
  if(type==="game:settings"){if(room.hostUserId!==userId||room.state!=="WAITING")return true;const requestedMap=String(msg.mapId||room.mapId);if(KINGDOM_MAPS[requestedMap])room.mapId=requestedMap;if(msg.mode==="free-for-all")room.mode="free-for-all";broadcastGameRoom(room,{type:"game:room-snapshot",room:gameRoomPublic(room)});broadcastGameRoomList(room.gameId);return true;}
  if(type==="game:ready"){const p=room.players.get(userId);if(p&&room.state==="WAITING")p.ready=Boolean(msg.ready);broadcastGameRoom(room,{type:"game:room-snapshot",room:gameRoomPublic(room)});return true;}
  if(type==="game:start"){if(room.hostUserId!==userId||room.state!=="WAITING")return true;const ps=[...room.players.values()];if(ps.length<room.minPlayers)return send(ws,{type:"game:error",message:`Need at least ${room.minPlayers} players`})||true;if(ps.some(p=>p.userId!==userId&&!p.ready))return send(ws,{type:"game:error",message:"Every other player must be ready"})||true;room.state="STARTING";broadcastGameRoom(room,{type:"game:room-snapshot",room:gameRoomPublic(room)});const snap=createKingdomMatch(room);broadcastGameRoom(room,{type:"game:match-started",matchId:snap.matchId,snapshot:snap,room:gameRoomPublic(room)});broadcastGameRoomList(room.gameId);return true;}
  if(type==="game:command"){const p=room.players.get(userId);const now=Date.now();if(!p)return true;if(p.lastCommandAt&&now-p.lastCommandAt<30)return true;p.lastCommandAt=now;const m=gameMatches.get(String(msg.matchId||room.matchId||""));if(m)gameCommand(m,userId,msg.command||{});return true;}
  return true;
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
  if (req.url?.startsWith("/game-rooms")) {
    const gameId = new URL(req.url, `http://${req.headers.host || "localhost"}`).searchParams.get("gameId") || "kingdom";
    res.writeHead(200);
    return res.end(JSON.stringify({ gameId, rooms:[...gameRooms.values()].filter(r => r.gameId === gameId && r.state !== "CLOSED").map(gameRoomPublic) }));
  }
  if (req.url === "/rooms") {
    res.writeHead(200);
    return res.end(JSON.stringify({ rooms: roomStats(), maxSpeakers: MAX_SPEAKERS, maxListeners: MAX_LISTENERS, listenersPerRelay: LISTENERS_PER_RELAY, maxRelays: MAX_RELAYS }));
  }
  res.writeHead(200);
  res.end(JSON.stringify({ ok: true, service: "hello-voice-signaling", protocol: 56, mode: "p2p-conversation-caster-relay", maxSpeakers: MAX_SPEAKERS, maxListeners: MAX_LISTENERS, listenersPerRelay: LISTENERS_PER_RELAY, maxRelays: MAX_RELAYS }));
});

const wss = new WebSocketServer({ server });
wss.on("connection", ws => {
  ws.meta = null;

  ws.on("message", raw => {
    let msg;
    try { msg = JSON.parse(raw.toString()); }
    catch { return send(ws, { type: "error", message: "Invalid JSON" }); }

    if (handleGameMessage(ws, msg)) return;

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

  ws.on("close", () => {
    const gm = gameConnections.get(ws);
    if (gm?.roomId) {
      const room = gameRooms.get(gm.roomId);
      const p = room?.players.get(gm.userId);
      if (p && p.ws === ws) { p.connected = false; p.ws = null; p.disconnectedAt = Date.now(); broadcastGameRoom(room, { type:"game:room-snapshot", room:gameRoomPublic(room) }); }
      setTimeout(() => {
        const r = gameRooms.get(gm.roomId); const pp = r?.players.get(gm.userId);
        if (r && pp && !pp.connected && pp.disconnectedAt && Date.now()-pp.disconnectedAt >= GAME_RECONNECT_GRACE_MS) {
          if (r.matchId) { const m=gameMatches.get(r.matchId); const gp=m&&gamePlayerState(m,gm.userId); if(gp) gp.alive=false; }
          r.players.delete(gm.userId); if (r.hostUserId === gm.userId) r.hostUserId = [...r.players.keys()][0] || "";
          if (!r.players.size) gameRooms.delete(r.id); else broadcastGameRoom(r,{type:"game:room-snapshot",room:gameRoomPublic(r)}); broadcastGameRoomList(r.gameId);
        }
      }, GAME_RECONNECT_GRACE_MS + 100);
    }
    gameConnections.delete(ws);
    removeSocket(ws);
  });
  ws.on("error", () => removeSocket(ws));
});

server.listen(PORT, "0.0.0.0", () => console.log(`hello-voice-signaling v52 + game-platform listening on port ${PORT}`));
