from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse, HTMLResponse
import asyncio
import json
from typing import Optional

app = FastAPI(title="tests.sse_test_app")

# Shared in-memory message store for tests
MESSAGES = [
    {"rowid": i + 1, "table": "test", "record": {"value": f"msg{i + 1}"}}
    for i in range(10)
]

# State to simulate reconnect behavior
SERVER_STATE = {
    "reconnect_next": 0,
    "retention_min_rowid": 3,  # rows with rowid < 3 are considered expired
}


def sse_encode(data: str, event: Optional[str] = None) -> str:
    out = ""
    if event:
        out += f"event: {event}\n"
    lines = data.splitlines() or [""]
    for line in lines:
        out += f"data: {line}\n"
    out += "\n"
    return out


async def gen_messages(start: int = 0, count: Optional[int] = None, delay: float = 0.1, request: Optional[Request] = None):
    # yield messages from MESSAGES starting at index `start`
    idx = start
    sent = 0
    while idx < len(MESSAGES):
        if count is not None and sent >= count:
            break
        try:
            if request is not None and await request.is_disconnected():
                break
        except Exception:
            pass
        payload = json.dumps(MESSAGES[idx])
        yield sse_encode(payload)
        idx += 1
        sent += 1
        await asyncio.sleep(delay)


async def gen_end(request: Request):
    # produce only end event
    if request:
        try:
            if await request.is_disconnected():
                return
        except Exception:
            pass
    yield sse_encode(json.dumps({"info": "end"}), event="end")


async def gen_malformed(request: Request):
    # first send malformed data then a valid message
    yield sse_encode("not-a-json")
    await asyncio.sleep(0.05)
    yield sse_encode(json.dumps(MESSAGES[0]))


async def gen_reconnect(request: Request):
    # send one message then close; on next connection resume from next index
    idx = SERVER_STATE.get("reconnect_next", 0)
    if idx >= len(MESSAGES):
        yield sse_encode(json.dumps({"info": "end"}), event="end")
        return
    # send single message
    yield sse_encode(json.dumps(MESSAGES[idx]))
    # advance pointer and return to close connection
    SERVER_STATE["reconnect_next"] = idx + 1


async def gen_high_throughput(request: Request):
    # produce many messages fast
    for i in range(20):
        try:
            if await request.is_disconnected():
                break
        except Exception:
            pass
        payload = json.dumps({"rowid": i + 1, "table": "high", "record": {"value": f"h{i + 1}"}})
        yield sse_encode(payload)
        await asyncio.sleep(0.01)


async def gen_resume(request: Request, last_rowid: int = 0):
    # resume sending records with rowid > last_rowid, or emit retention error
    if last_rowid < SERVER_STATE.get("retention_min_rowid", 0):
        # retention error event
        yield sse_encode(json.dumps({"error": "retention_expired", "min_rowid": SERVER_STATE["retention_min_rowid"]}), event="retention")
        return
    # find starting index
    start_idx = 0
    for i, m in enumerate(MESSAGES):
        if m["rowid"] > last_rowid:
            start_idx = i
            break
    async for part in gen_messages(start=start_idx, count=None, delay=0.05, request=request):
        yield part


@app.get("/sse/test")
async def sse_test(request: Request):
    return StreamingResponse(gen_messages(request=request), media_type="text/event-stream")


@app.get("/sse/end")
async def sse_end(request: Request):
    return StreamingResponse(gen_end(request), media_type="text/event-stream")


@app.get("/sse/malformed")
async def sse_malformed(request: Request):
    return StreamingResponse(gen_malformed(request), media_type="text/event-stream")


@app.get("/sse/reconnect")
async def sse_reconnect(request: Request):
    return StreamingResponse(gen_reconnect(request), media_type="text/event-stream")


@app.get("/sse/high")
async def sse_high(request: Request):
    return StreamingResponse(gen_high_throughput(request), media_type="text/event-stream")


@app.get("/sse/auth")
async def sse_auth(request: Request, token: Optional[str] = None):
    # accept token via query param or Authorization header Bearer
    auth = request.headers.get("authorization")
    valid = False
    if token == "secret-token":
        valid = True
    if auth and auth.lower().startswith("bearer ") and auth.split(None, 1)[1] == "secret-token":
        valid = True
    if not valid:
        return Response(status_code=401, content=b"Unauthorized")
    return StreamingResponse(gen_messages(request=request), media_type="text/event-stream")


@app.get("/sse/resume")
async def sse_resume(request: Request, last_rowid: int = 0):
    return StreamingResponse(gen_resume(request, last_rowid=last_rowid), media_type="text/event-stream")


@app.get("/testpage")
async def test_page():
    html = """
    <!doctype html>
    <html>
      <head>
        <meta charset="utf-8" />
        <title>SSE Test Page</title>
      </head>
      <body>
        <h1>SSE Test Page</h1>
        <script>
          // window-scoped structures for tests
          window._sse_msgs = [];
          window._sse_parse_errors = [];
          window._sse_ended = false;

          function qs(name) {
            const params = new URLSearchParams(window.location.search);
            return params.get(name);
          }

          const stream = qs('stream') || 'test';
          const token = qs('token');
          const last_rowid = qs('last_rowid');
          let endpoint = '/sse/' + stream;
          const qp = [];
          if (token) qp.push('token=' + encodeURIComponent(token));
          if (last_rowid) qp.push('last_rowid=' + encodeURIComponent(last_rowid));
          if (qp.length) endpoint += '?' + qp.join('&');

          const s = new EventSource(endpoint);
          s.addEventListener('message', e => {
            try {
              // try parse
              const parsed = JSON.parse(e.data);
              window._sse_msgs.push(parsed);
            } catch (err) {
              window._sse_parse_errors.push(String(err));
              window._sse_msgs.push(e.data);
            }
          });
          s.addEventListener('end', e => { try { s.close(); } catch (e) {} ; window._sse_ended = true; });
          s.addEventListener('retention', e => { try { s.close(); } catch (e) {} ; window._sse_retention = JSON.parse(e.data); });
          s.addEventListener('error', e => { try { s.close(); } catch (e) {} });
        </script>
      </body>
    </html>
    """
    return HTMLResponse(content=html)
