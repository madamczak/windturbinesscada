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

# New: per-turbine DBs directory (created by previous scripts)
DATA_BY_TURBINE_DIR = BASE_DB_DIR / "data_by_turbine"
PENMANSHIEL_DATA_BY_TURBINE_DB = DATA_BY_TURBINE_DIR / "penmanshiel_data_by_turbine.db"
PENMANSHIEL_STATUS_BY_TURBINE_DB = DATA_BY_TURBINE_DIR / "penmanshiel_status_by_turbine.db"
KELMARSH_DATA_BY_TURBINE_DB = DATA_BY_TURBINE_DIR / "kelmarsh_data_by_turbine.db"
KELMARSH_STATUS_BY_TURBINE_DB = DATA_BY_TURBINE_DIR / "kelmarsh_status_by_turbine.db"

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


async def stream_table_rows_from_db(db_path: Path, table: Optional[str], request: Request, start_rowid: Optional[int], wait_seconds: float, since_ms: Optional[int] = None) -> AsyncGenerator[str, None]:
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

        # If since_ms was provided, try to resolve it to a starting rowid using likely timestamp-like columns
        if since_ms is not None and start_rowid is None:
            # find candidate columns for timestamps
            cand_cols = [c for c in cols if re.search(r"timestamp|datetime|date|time|created_at|ts", c, re.IGNORECASE)]
            resolved_rowid = None
            for col in cand_cols:
                # try a query that handles several storage formats:
                # - numeric epoch stored in seconds or milliseconds (CAST column as integer)
                # - ISO-like text that strftime can parse (strftime('%s', col) gives seconds)
                # We compare in milliseconds so scale accordingly.
                try:
                    q = f"SELECT rowid FROM '{qtbl}' WHERE ((CAST(\"{col}\" AS INTEGER) >= ?) OR ((CAST(\"{col}\" AS INTEGER) * 1000) >= ?) OR ((strftime('%s', \"{col}\") IS NOT NULL) AND (strftime('%s', \"{col}\") * 1000 >= ?))) ORDER BY rowid ASC LIMIT 1"
                    async with conn.execute(q, (since_ms, since_ms, since_ms)) as cur:
                        r = await cur.fetchone()
                        if r:
                            resolved_rowid = r[0]
                            break
                except Exception:
                    # ignore and try next candidate
                    continue
            if resolved_rowid:
                start_rowid = int(resolved_rowid)

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
async def sse_next_record(request: Request,
                          start_rowid: Optional[int] = Query(None, description="Optional starting rowid (inclusive)"),
                          wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0, description="Seconds between records"),
                          since_ms: Optional[int] = Query(None, description="Optional start timestamp in milliseconds since epoch")):
    gen = stream_table_rows_from_db(PENMANSHIEL_DATA_DB, PENMANSHIEL_TABLE, request, start_rowid, wait_seconds, since_ms)
    return StreamingResponse(gen, media_type="text/event-stream")


# Additional endpoints for kelmarsh data and both status DBs
@app.get("/sse/kelmarsh_all_data")
async def sse_kelmarsh_data(request: Request,
                             start_rowid: Optional[int] = Query(None),
                             wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0),
                             since_ms: Optional[int] = Query(None, description="Optional start timestamp in milliseconds since epoch")):
    gen = stream_table_rows_from_db(KELMARSH_DATA_DB, None, request, start_rowid, wait_seconds, since_ms)
    return StreamingResponse(gen, media_type="text/event-stream")


@app.get("/sse/penmanshiel_all_status")
async def sse_penmanshiel_status(request: Request,
                                  start_rowid: Optional[int] = Query(None),
                                  wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0),
                                  since_ms: Optional[int] = Query(None, description="Optional start timestamp in milliseconds since epoch")):
    gen = stream_table_rows_from_db(PENMANSHIEL_STATUS_DB, None, request, start_rowid, wait_seconds, since_ms)
    return StreamingResponse(gen, media_type="text/event-stream")


@app.get("/sse/kelmarsh_all_status")
async def sse_kelmarsh_status(request: Request,
                               start_rowid: Optional[int] = Query(None),
                               wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0),
                               since_ms: Optional[int] = Query(None, description="Optional start timestamp in milliseconds since epoch")):
    gen = stream_table_rows_from_db(KELMARSH_STATUS_DB, None, request, start_rowid, wait_seconds, since_ms)
    return StreamingResponse(gen, media_type="text/event-stream")


# New endpoints: stream per-turbine tables from the *_by_turbine DBs
@app.get("/sse/by-turbine/penmanshiel_data/{turbine}")
async def sse_penmanshiel_data_by_turbine(request: Request,
                                          turbine: int,
                                          start_rowid: Optional[int] = Query(None),
                                          wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0),
                                          since_ms: Optional[int] = Query(None, description="Optional start timestamp in milliseconds since epoch")):
    if turbine < 1 or turbine > 15:
        return StreamingResponse((sse_encode(json.dumps({"error": "invalid_turbine_range", "min": 1, "max": 15}), event="error") for _ in ()), media_type="text/event-stream")
    tbl = f"turbine_{turbine}"
    gen = stream_table_rows_from_db(PENMANSHIEL_DATA_BY_TURBINE_DB, tbl, request, start_rowid, wait_seconds, since_ms)
    return StreamingResponse(gen, media_type="text/event-stream")


@app.get("/sse/by-turbine/penmanshiel_status/{turbine}")
async def sse_penmanshiel_status_by_turbine(request: Request,
                                            turbine: int,
                                            start_rowid: Optional[int] = Query(None),
                                            wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0),
                                            since_ms: Optional[int] = Query(None, description="Optional start timestamp in milliseconds since epoch")):
    if turbine < 1 or turbine > 15:
        return StreamingResponse((sse_encode(json.dumps({"error": "invalid_turbine_range", "min": 1, "max": 15}), event="error") for _ in ()), media_type="text/event-stream")
    tbl = f"turbine_{turbine}"
    gen = stream_table_rows_from_db(PENMANSHIEL_STATUS_BY_TURBINE_DB, tbl, request, start_rowid, wait_seconds, since_ms)
    return StreamingResponse(gen, media_type="text/event-stream")


@app.get("/sse/by-turbine/kelmarsh_data/{turbine}")
async def sse_kelmarsh_data_by_turbine(request: Request,
                                       turbine: int,
                                       start_rowid: Optional[int] = Query(None),
                                       wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0),
                                       since_ms: Optional[int] = Query(None, description="Optional start timestamp in milliseconds since epoch")):
    if turbine < 1 or turbine > 6:
        return StreamingResponse((sse_encode(json.dumps({"error": "invalid_turbine_range", "min": 1, "max": 6}), event="error") for _ in ()), media_type="text/event-stream")
    tbl = f"turbine_{turbine}"
    gen = stream_table_rows_from_db(KELMARSH_DATA_BY_TURBINE_DB, tbl, request, start_rowid, wait_seconds, since_ms)
    return StreamingResponse(gen, media_type="text/event-stream")


@app.get("/sse/by-turbine/kelmarsh_status/{turbine}")
async def sse_kelmarsh_status_by_turbine(request: Request,
                                         turbine: int,
                                         start_rowid: Optional[int] = Query(None),
                                         wait_seconds: float = Query(DEFAULT_SEND_INTERVAL_SECONDS, ge=0.0),
                                         since_ms: Optional[int] = Query(None, description="Optional start timestamp in milliseconds since epoch")):
    if turbine < 1 or turbine > 6:
        return StreamingResponse((sse_encode(json.dumps({"error": "invalid_turbine_range", "min": 1, "max": 6}), event="error") for _ in ()), media_type="text/event-stream")
    tbl = f"turbine_{turbine}"
    gen = stream_table_rows_from_db(KELMARSH_STATUS_BY_TURBINE_DB, tbl, request, start_rowid, wait_seconds, since_ms)
    return StreamingResponse(gen, media_type="text/event-stream")


@app.get("/sse/resolve-rowid")
async def resolve_rowid(source: str = Query(..., description="One of: penmanshiel_data, penmanshiel_status, kelmarsh_data, kelmarsh_status"),
                        turbine: int = Query(..., ge=1),
                        since_ms: Optional[int] = Query(None, description="timestamp in ms")):
    """Resolve the first rowid in the given turbine table where the likely timestamp column is >= since_ms.
    Returns JSON: {"rowid": <int> } or {"rowid": null} on not found.
    """
    # map source to DB path
    src_map = {
        'penmanshiel_data': PENMANSHIEL_DATA_BY_TURBINE_DB,
        'penmanshiel_status': PENMANSHIEL_STATUS_BY_TURBINE_DB,
        'kelmarsh_data': KELMARSH_DATA_BY_TURBINE_DB,
        'kelmarsh_status': KELMARSH_STATUS_BY_TURBINE_DB,
    }
    if source not in src_map:
        return {"error": "unknown_source", "source": source}
    db_path = src_map[source]
    if not db_path.exists():
        return {"error": "db_not_found", "path": str(db_path)}

    tbl = f"turbine_{turbine}"
    qdb = str(db_path)
    async with aiosqlite.connect(qdb) as conn:
        # get columns
        qtbl = tbl.replace("'", "''")
        pragma = f"PRAGMA table_info('{qtbl}')"
        cols = []
        try:
            async with conn.execute(pragma) as cur:
                async for r in cur:
                    cols.append(r[1])
        except Exception:
            return {"error": "no_such_table", "table": tbl}

        if since_ms is None:
            return {"rowid": None}

        # find candidate timestamp-like columns
        cand_cols = [c for c in cols if re.search(r"timestamp|datetime|date|time|created_at|ts", c, re.IGNORECASE)]
        resolved_rowid = None
        for col in cand_cols:
            try:
                q = f"SELECT rowid FROM '{qtbl}' WHERE ((CAST(\"{col}\" AS INTEGER) >= ?) OR ((CAST(\"{col}\" AS INTEGER) * 1000) >= ?) OR ((strftime('%s', \"{col}\") IS NOT NULL) AND (strftime('%s', \"{col}\") * 1000 >= ?))) ORDER BY rowid ASC LIMIT 1"
                async with conn.execute(q, (since_ms, since_ms, since_ms)) as cur:
                    r = await cur.fetchone()
                    if r:
                        resolved_rowid = int(r[0])
                        break
            except Exception:
                continue
        return {"rowid": resolved_rowid}


def parse_duration_to_seconds(duration_str: str) -> Optional[float]:
    """Parse a duration string like 'HH:MM:SS' or 'HHH:MM:SS' to seconds."""
    if not duration_str or not isinstance(duration_str, str):
        return None
    try:
        parts = duration_str.strip().split(':')
        if len(parts) == 3:
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds = float(parts[2])
            return hours * 3600 + minutes * 60 + seconds
        elif len(parts) == 2:
            minutes = int(parts[0])
            seconds = float(parts[1])
            return minutes * 60 + seconds
    except (ValueError, IndexError):
        pass
    return None


# Time compression factor: data records are 10 minutes apart but sent every 1 second
# So 600 seconds of real time = 1 second of stream time
TIME_COMPRESSION_FACTOR = 600.0


async def stream_combined_kelmarsh(turbine: int, request: Request, data_interval: float, max_status_wait: float = 300.0) -> AsyncGenerator[str, None]:
    """Stream combined data and status for a single Kelmarsh turbine.

    - Data updates every data_interval seconds (default 1 second)
    - Status updates based on the Duration field in the status record,
      divided by TIME_COMPRESSION_FACTOR (600) to match data time scale
    - max_status_wait caps status wait time for very long durations
    """
    data_db = str(KELMARSH_DATA_BY_TURBINE_DB)
    status_db = str(KELMARSH_STATUS_BY_TURBINE_DB)
    tbl = f"turbine_{turbine}"

    if not KELMARSH_DATA_BY_TURBINE_DB.exists() or not KELMARSH_STATUS_BY_TURBINE_DB.exists():
        yield sse_encode(json.dumps({"error": "db_not_found"}), event="error")
        return

    try:
        data_ws = max(0.1, float(data_interval))
    except Exception:
        data_ws = 1.0

    try:
        max_status_ws = max(1.0, float(max_status_wait))
    except Exception:
        max_status_ws = 300.0

    data_rowid = 0
    status_rowid = 0

    # Track when the next status update should happen
    status_next_update_time = 0.0  # epoch time when status should next update

    async with aiosqlite.connect(data_db) as data_conn, aiosqlite.connect(status_db) as status_conn:
        # Get column names for both tables
        data_cols = []
        status_cols = []

        async with data_conn.execute(f"PRAGMA table_info('{tbl}')") as cur:
            async for r in cur:
                data_cols.append(r[1])

        async with status_conn.execute(f"PRAGMA table_info('{tbl}')") as cur:
            async for r in cur:
                status_cols.append(r[1])

        if not data_cols or not status_cols:
            yield sse_encode(json.dumps({"error": "table_not_found", "table": tbl}), event="error")
            return

        import time
        current_time = time.time()
        status_next_update_time = current_time  # Allow immediate first status update

        while True:
            # Check disconnect
            try:
                if await request.is_disconnected():
                    break
            except Exception:
                pass

            current_time = time.time()

            # Always fetch next data record (every iteration)
            data_record = None
            async with data_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (data_rowid,)) as cur:
                row = await cur.fetchone()
                if row:
                    data_rowid = row[0]
                    values = row[1:]
                    data_record = {col: (None if val is None else str(val)) for col, val in zip(data_cols, values)}

            # Only fetch next status record if it's time
            status_record = None
            status_updated = False
            if current_time >= status_next_update_time:
                async with status_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (status_rowid,)) as cur:
                    row = await cur.fetchone()
                    if row:
                        status_rowid = row[0]
                        values = row[1:]
                        status_record = {col: (None if val is None else str(val)) for col, val in zip(status_cols, values)}
                        status_updated = True

                        # Extract duration and schedule next status update
                        duration_col = next((col for col in status_cols if col.lower() == 'duration'), None)
                        duration_seconds = None
                        if duration_col and status_record.get(duration_col):
                            duration_seconds = parse_duration_to_seconds(status_record[duration_col])

                        if duration_seconds is not None and duration_seconds > 0:
                            # Apply time compression factor (600:1) to match data time scale
                            # e.g., 10 minutes (600s) real time = 1 second stream time
                            compressed_duration = duration_seconds / TIME_COMPRESSION_FACTOR
                            # Cap at max_status_wait
                            wait_time = min(compressed_duration, max_status_ws)
                        else:
                            wait_time = data_ws  # fallback to data interval

                        status_next_update_time = current_time + wait_time

            # Send combined payload
            # Include status info even if not updated (show current status)
            payload = {
                "turbine": turbine,
                "data": {"rowid": data_rowid, "record": data_record} if data_record else None,
                "status": {"rowid": status_rowid, "record": status_record, "updated": status_updated} if status_updated else None
            }
            yield sse_encode(json.dumps(payload, ensure_ascii=False))

            # Wait for data interval (data updates every data_interval seconds)
            await asyncio.sleep(data_ws)

            # If data stream exhausted, send end event and break
            if data_record is None:
                yield sse_encode(json.dumps({"info": "end_of_data", "turbine": turbine}), event="end")
                break


@app.get("/sse/kelmarsh-combined/{turbine}")
async def sse_kelmarsh_combined(request: Request,
                                 turbine: int,
                                 data_interval: float = Query(1.0, ge=0.1, description="Seconds between data updates"),
                                 max_status_wait: float = Query(300.0, ge=1.0, description="Maximum wait time for status (caps long durations)")):
    """Stream combined data and status for a single Kelmarsh turbine.

    - Data updates every data_interval seconds (default 1 second)
    - Status updates based on the Duration field in the status record
    - max_status_wait caps very long status durations (default 5 minutes)
    """
    if turbine < 1 or turbine > 6:
        return StreamingResponse(
            (sse_encode(json.dumps({"error": "invalid_turbine_range", "min": 1, "max": 6}), event="error") for _ in ()),
            media_type="text/event-stream"
        )
    gen = stream_combined_kelmarsh(turbine, request, data_interval, max_status_wait)
    return StreamingResponse(gen, media_type="text/event-stream")


if __name__ == '__main__':
    # Allow running `python api/main.py` or running the file from PyCharm 'Run'
    try:
        import uvicorn
    except Exception:
        raise RuntimeError("uvicorn is required to run the server. Install with: pip install uvicorn[standard]")

    # Note: reload should be False when running programmatically; use --reload from CLI if desired.
    uvicorn.run(app, host="127.0.0.1", port=8000)
