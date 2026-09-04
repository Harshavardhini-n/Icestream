# IceStream Frontend Dashboard

This frontend consumes the real FastAPI endpoints from the IceStream API service and renders a monitoring dashboard for live checkout events.

## Local development

1. Copy `.env.example` to `.env` if you need a local override.
2. Install dependencies: `npm install`
3. Start the app: `npm run dev`
4. Open: `http://localhost:5173`

## Environment
The app reads the API base URL from `VITE_API_BASE_URL`.