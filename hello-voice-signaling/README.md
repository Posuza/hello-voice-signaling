# Hello Voice Signaling

Small WebSocket signaling server for the HelloTalk-style voice-room prototype.

## What it does

- WebRTC signaling only
- P2P audio does not pass through this server
- Maximum 5 peers per room
- Supports offer / answer / ICE candidate routing
- HTTP health endpoint at `/`

## Run locally

```bash
npm install
npm start
```

Local WebSocket URL:

```text
ws://localhost:8787
```

## Deploy to Render

This ZIP includes `render.yaml`.

1. Upload these files to your GitHub repo.
2. In Render, create a new Blueprint or Web Service from the repo.
3. Build command: `npm install`
4. Start command: `npm start`
5. Health check: `/`
6. Free plan is enough for the prototype.

Render provides `PORT` automatically.

After deploy, if the service URL is:

```text
https://hello-voice-signaling.onrender.com
```

set this in the frontend:

```text
NEXT_PUBLIC_SIGNALING_URL=wss://hello-voice-signaling.onrender.com
```
