# HFT Sandbox Leaderboard

React/Vite dashboard for the telemetry API.

```bash
cd leaderboard
npm install
VITE_API_URL=http://localhost:8000 npm run dev
```

The UI reads `GET /api/v1/leaderboard` once, then follows
`GET /api/v1/leaderboard/stream` as a Server-Sent Events feed.
