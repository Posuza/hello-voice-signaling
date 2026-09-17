# Hello Voice signaling v44

Protocol 44 implements automatic browser-caster and audience-relay assignment for the Hello Voice v44 frontend.

- max table speakers: 5
- max audience: 25 independent of speaker count
- relay group size: 5 audience users (1 relay + up to 4 children)
- max relay peers: 5
- automatic primary caster election
- automatic warm standby caster election
- standby promoted first when caster disconnects
- automatic relay rebalance on join/leave/table-role changes
- Render carries signaling/control only; media remains WebRTC P2P

Deploy this signaling version together with the v44 frontend.
