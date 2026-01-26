Frontend SSE viewer

This is a minimal Vue 3 single-file (no build) static frontend that connects to the API SSE endpoint `/sse/next-record` and cycles through the first 5 parameters of each record every 10 seconds.

How to run:

1. Ensure the backend API is running (see `api/main.py`). By default it listens at http://127.0.0.1:8000.

2. Open the frontend file `frontend/index.html` in a browser. For SSE to work properly (and avoid CORS issues), either:
   - Serve it from the same origin as the API (for example, put it behind a local static server), or
   - Use the backend's CORS policy which currently allows all origins (development only).

Quick static server (Python 3):

```powershell
cd frontend
python -m http.server 5500
```

Then open: http://127.0.0.1:5500

Notes:
- The page assumes the SSE endpoint is at `${location.origin}/sse/next-record`. If you serve the frontend on a different origin, modify the `sseUrl` variable in `index.html` to `http://127.0.0.1:8000/sse/next-record`.
- This is intentionally minimal; if you want a full Vue CLI / Vite app with components and hot reload I can scaffold that instead.

