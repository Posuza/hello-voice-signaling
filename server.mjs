import http from "node:http";
import { WebSocketServer, WebSocket } from "ws";
const PORT=Number(process.env.PORT||8787),MAX_ROOM_SIZE=5;
const rooms=new Map(); // roomId -> Map(peerId,{ws,user}) ; messages are ephemeral
function send(ws,p){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(p))}
function broadcast(room,p,except){for(const x of room.peers.values())if(x.ws!==except)send(x.ws,p)}
function leave(ws){const m=ws.meta;if(!m)return;const room=rooms.get(m.roomId);if(room){room.peers.delete(m.peerId);broadcast(room,{type:"peer-left",peerId:m.peerId});if(room.peers.size===0){room.messages.length=0;rooms.delete(m.roomId)}}ws.meta=null}
const server=http.createServer((req,res)=>{res.writeHead(200,{"content-type":"application/json"});res.end(JSON.stringify({ok:true,service:"hello-voice-signaling",maxRoomSize:MAX_ROOM_SIZE}))});
const wss=new WebSocketServer({server});
wss.on("connection",ws=>{ws.meta=null;ws.on("message",raw=>{let msg;try{msg=JSON.parse(raw.toString())}catch{return send(ws,{type:"error",message:"Invalid JSON"})}
 if(msg.type==="join"){leave(ws);const roomId=String(msg.roomId||"").trim(),peerId=String(msg.peerId||"").trim(),name=String(msg.name||"Guest").trim().slice(0,32);if(!roomId||!peerId)return send(ws,{type:"error",message:"roomId and peerId are required"});let room=rooms.get(roomId);if(!room){room={peers:new Map(),messages:[]};rooms.set(roomId,room)}if(!room.peers.has(peerId)&&room.peers.size>=MAX_ROOM_SIZE)return send(ws,{type:"room-full",maxRoomSize:MAX_ROOM_SIZE});const peers=[...room.peers.values()].map(x=>x.user);room.peers.set(peerId,{ws,user:{id:peerId,name}});ws.meta={roomId,peerId};send(ws,{type:"joined",peerId,peers,maxRoomSize:MAX_ROOM_SIZE,messages:room.messages});broadcast(room,{type:"peer-joined",peer:{id:peerId,name}},ws);return}
 const m=ws.meta;if(!m)return send(ws,{type:"error",message:"Join a room first"});const room=rooms.get(m.roomId);if(!room)return;
 if(msg.type==="signal"){const target=room.peers.get(String(msg.to||""));if(target)send(target.ws,{type:"signal",from:m.peerId,data:msg.data});return}
 if(msg.type==="chat"){const sender=room.peers.get(m.peerId);const text=String(msg.text||"").trim().slice(0,400);if(!text||!sender)return;const message={id:crypto.randomUUID(),userId:m.peerId,name:sender.user.name,text,at:Date.now()};room.messages.push(message);if(room.messages.length>100)room.messages.shift();broadcast(room,{type:"chat",message});return}
 if(msg.type==="close-room"){broadcast(room,{type:"room-closed"});for(const x of room.peers.values()){x.ws.meta=null;try{x.ws.close(1000,"Room closed")}catch{}}room.messages.length=0;rooms.delete(m.roomId);return}
 });ws.on("close",()=>leave(ws));ws.on("error",()=>leave(ws))});
server.listen(PORT,"0.0.0.0",()=>console.log(`hello-voice-signaling listening on port ${PORT}`));
