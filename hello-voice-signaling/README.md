# Hello Voice signaling v6

Render-ready WebSocket signaling server for the distributed P2P relay design.

Limits:
- 5 speakers maximum
- 5 listeners per active speaker relay
- 25 listeners maximum when all 5 speaker relays are active

Listener capacity is adaptive. A room with only one active speaker has 5 listener seats. Two speakers provide 10, and so on. On relay failure, connected listeners are redistributed across remaining speaker relays.

Deploy on Render with:
- Build: `npm install`
- Start: `npm start`
- No manual `PORT` variable required
