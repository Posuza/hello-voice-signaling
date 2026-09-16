import http from "node:http";
import { WebSocketServer, WebSocket } from "ws";

const PORT = Number(process.env.PORT || 8787);
const MAX_ROOM_SIZE = 5;
const rooms = new Map();

const server = http.createServer((req, res) => {
  res.writeHead(200, { "content-type": "application/json" });
  res.end(JSON.stringify({
    ok: true,
    service: "hello-voice-signaling",
    maxRoomSize: MAX_ROOM_SIZE
  }));
});

const wss = new WebSocketServer({ server });

function send(ws, payload) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(payload));
  }
}

function leaveRoom(ws) {
  const meta = ws.meta;
  if (!meta) return;

  const room = rooms.get(meta.roomId);
  if (!room) {
    ws.meta = null;
    return;
  }

  room.delete(meta.peerId);

  for (const peer of room.values()) {
    send(peer, { type: "peer-left", peerId: meta.peerId });
  }

  if (room.size === 0) rooms.delete(meta.roomId);
  ws.meta = null;
}

wss.on("connection", (ws) => {
  ws.meta = null;

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch {
      return send(ws, { type: "error", message: "Invalid JSON" });
    }

    if (msg.type === "join") {
      leaveRoom(ws);

      const roomId = String(msg.roomId || "").trim();
      const peerId = String(msg.peerId || "").trim();

      if (!roomId || !peerId) {
        return send(ws, {
          type: "error",
          message: "roomId and peerId are required"
        });
      }

      let room = rooms.get(roomId);
      if (!room) {
        room = new Map();
        rooms.set(roomId, room);
      }

      if (!room.has(peerId) && room.size >= MAX_ROOM_SIZE) {
        return send(ws, {
          type: "room-full",
          maxRoomSize: MAX_ROOM_SIZE
        });
      }

      const peers = [...room.keys()].filter((id) => id !== peerId);
      room.set(peerId, ws);
      ws.meta = { roomId, peerId };

      send(ws, {
        type: "joined",
        peerId,
        peers,
        maxRoomSize: MAX_ROOM_SIZE
      });

      for (const [id, peer] of room) {
        if (id !== peerId) {
          send(peer, { type: "peer-joined", peerId });
        }
      }
      return;
    }

    const meta = ws.meta;
    if (!meta) {
      return send(ws, { type: "error", message: "Join a room first" });
    }

    const targetId = String(msg.to || "").trim();
    if (!targetId) return;

    const target = rooms.get(meta.roomId)?.get(targetId);
    if (!target) return;

    if (["offer", "answer", "ice-candidate", "signal"].includes(msg.type)) {
      send(target, { ...msg, from: meta.peerId });
    }
  });

  ws.on("close", () => leaveRoom(ws));
  ws.on("error", () => leaveRoom(ws));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`hello-voice-signaling listening on port ${PORT}`);
});
