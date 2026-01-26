#!/usr/bin/env python3
"""Simple FastAPI SSE server that streams the next record from
Penmanshiel_Data table in the penmanshiel_all_data.db every 10 seconds.

Connect from the frontend using EventSource('/sse/next-record').

Notes:
- This implementation streams rows sequentially ordered by rowid.
- Each client connection starts at the first row by default; you can provide
  a `start_rowid` query parameter to start from a different row.
- The payload is JSON with keys: rowid and the table columns.
"""

from pathlib import Path
import asyncio
import json
import re
from typing import AsyncGenerator, Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import aiosqlite

# Configuration (existing penmanshiel data)
BASE_DB_DIR = Path(r"C:\Users\adamc\PycharmProjects\windturbinesscada\data\sqlitedbs")
PENMANSHIEL_DATA_DB = BASE_DB_DIR / "penmanshiel_all_data.db"
PENMANSHIEL_TABLE = "Penmanshiel_Data"
DEFAULT_SEND_INTERVAL_SECONDS = 10

# Additional DBs
KELMARSH_DATA_DB = BASE_DB_DIR / "kelmarsh_all_data.db"
PENMANSHIEL_STATUS_DB = BASE_DB_DIR / "penmanshiel_all_status.db"
KELMARSH_STATUS_DB = BASE_DB_DIR / "kelmarsh_all_status.db"

app = FastAPI(title="SSE multi-source API")

# allow all origins for convenience during development (adjust in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def sse_encode(data: str, event: Optional[str] = None) -> str:
    """Encode a string payload as an SSE message."""
    out = ""
    if event:
        out += f"event: {event}\n"
    # splitlines preserves lines; ensure at least one data line
    lines = data.splitlines() or [""]
    for line in lines:
        out += f"data: {line}\n"
    out += "\n"
    return out


async def first_user_table(conn: aiosqlite.Connection) -> Optional[str]:
    q = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT 1"
    async with conn.execute(q) as cur:
        row = await cur.fetchone()
    return row[0] if row else None


async def stream_table_rows_from_db(db_path: Path, table: Optional[str], request: Request, start_rowid: Optional[int], wait_seconds: float) -> AsyncGenerator[str, None]:
    """Stream rows from a sqlite DB table as SSE messages.

    - db_path: path to sqlite file
    - table: optional table name; if None, detect first user table
    - request: FastAPI Request for disconnect checks
    - start_rowid: optional rowid to start from
    - wait_seconds: seconds to sleep between records (float)
    """
    if not db_path.exists():
        yield sse_encode(json.dumps({"error": "db_not_found", "path": str(db_path)}), event="error")
        return

    qdb = str(db_path)
    try:
        ws = float(wait_seconds)
    except Exception:
        ws = float(DEFAULT_SEND_INTERVAL_SECONDS)
    ws = max(0.0, ws)

    async with aiosqlite.connect(qdb) as conn:
        # resolve table name if not provided
        tbl = table
        if not tbl:
            tbl = await first_user_table(conn)
            if not tbl:
                yield sse_encode(json.dumps({"error": "no_table_found"}), event="error")
                return
        qtbl = tbl.replace("'", "''")

        # get column names
        pragma = f"PRAGMA table_info('{qtbl}')"
        cols = []
        async with conn.execute(pragma) as cur:
            async for r in cur:
                cols.append(r[1])
        if not cols:
            yield sse_encode(json.dumps({"error": "no_columns_found", "table": tbl}), event="error")
            return

        # build select SQL
        if start_rowid:
            sql = f"SELECT rowid, * FROM '{qtbl}' WHERE rowid >= ? ORDER BY rowid ASC"
            params = (start_rowid,)
        else:
            sql = f"SELECT rowid, * FROM '{qtbl}' ORDER BY rowid ASC"
            params = ()

        async with conn.execute(sql, params) as cur:
            async for row in cur:
                # check disconnect
                try:
                    if await request.is_disconnected():
                        break
                except Exception:
                    pass

                rowid = row[0]
                values = row[1:]
                record = {col: (None if val is None else (val.isoformat() if hasattr(val, 'isoformat') and not isinstance(val, str) else str(val)))
                          for col, val in zip(cols, values)}
                payload = json.dumps({"rowid": rowid, "table": tbl, "record": record}, ensure_ascii=False)
                yield sse_encode(payload)

                # look for a Duration-like field in the record (case-insensitive)
                duration_raw = None
                for k, v in record.items():
                    if k and 'duration' in k.lower():
                        duration_raw = v
                        break

                duration_sec = None
                if duration_raw is not None:
                    try:
                        s = str(duration_raw).strip()
                        if s:
                            parts = [p for p in s.split(':') if p != '']
                            nums = []
                            for p in parts:
                                # keep digits and decimal point only
                                cleaned = re.sub(r"[^0-9.]", "", p)
                                if cleaned == "":
                                    continue
                                try:
                                    nums.append(float(cleaned))
                                except Exception:
                                    continue
                            if nums:
                                sec = nums[-1]
                                minute = nums[-2] if len(nums) >= 2 else 0
                                hour = nums[-3] if len(nums) >= 3 else 0
                                duration_sec = float(hour) * 3600.0 + float(minute) * 60.0 + float(sec)
                    except Exception:
                        duration_sec = None

                # choose effective sleep: prefer duration if it's >0 and <10, otherwise use ws
                effective_sleep = ws
                if duration_sec is not None:
                    try:
                        if 0 < duration_sec < 10:
                            effective_sleep = float(duration_sec)
                    except Exception:
                        pass

                await asyncio.sleep(effective_sleep)

        # finished
        yield sse_encode(json.dumps({"info": "end_of_data", "table": tbl}), event="end")


# Keep existing simple endpoint for penmanshiel next-record (default)
@app.get("/sse/next-record")
async def sse_next_record(request: Request, start_rowid: Optional[int] = Query(None, description="Optional starting rowid (inclusive)"), wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0, description="Seconds between records")):
    gen = stream_table_rows_from_db(PENMANSHIEL_DATA_DB, PENMANSHIEL_TABLE, request, start_rowid, wait_seconds)
    return StreamingResponse(gen, media_type="text/event-stream")


# Additional endpoints for kelmarsh data and both status DBs
@app.get("/sse/kelmarsh_all_data")
async def sse_kelmarsh_data(request: Request, start_rowid: Optional[int] = Query(None), wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0)):
    gen = stream_table_rows_from_db(KELMARSH_DATA_DB, None, request, start_rowid, wait_seconds)
    return StreamingResponse(gen, media_type="text/event-stream")


@app.get("/sse/penmanshiel_all_status")
async def sse_penmanshiel_status(request: Request, start_rowid: Optional[int] = Query(None), wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0)):
    gen = stream_table_rows_from_db(PENMANSHIEL_STATUS_DB, None, request, start_rowid, wait_seconds)
    return StreamingResponse(gen, media_type="text/event-stream")


@app.get("/sse/kelmarsh_all_status")
async def sse_kelmarsh_status(request: Request, start_rowid: Optional[int] = Query(None), wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0)):
    gen = stream_table_rows_from_db(KELMARSH_STATUS_DB, None, request, start_rowid, wait_seconds)
    return StreamingResponse(gen, media_type="text/event-stream")


if __name__ == '__main__':
    # Allow running `python api/main.py` or running the file from PyCharm 'Run'
    try:
        import uvicorn
    except Exception:
        raise RuntimeError("uvicorn is required to run the server. Install with: pip install uvicorn[standard]")

    # Note: reload should be False when running programmatically; use --reload from CLI if desired.
    uvicorn.run(app, host="127.0.0.1", port=8000)
