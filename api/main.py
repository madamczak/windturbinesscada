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
from datetime import datetime

from fastapi import FastAPI, Query, Request
from fastapi.responses import StreamingResponse, FileResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import aiosqlite

# Get the base directory (parent of api folder)
BASE_DIR = Path(__file__).resolve().parent.parent

# Configuration (existing penmanshiel data) - relative paths for Docker
BASE_DB_DIR = BASE_DIR / "data" / "sqlitedbs"
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

# Dennis & Ciara storm databases
DENNIS_CIARA_DIR = BASE_DB_DIR / "dennisciara"
KELMARSH_DATA_STORMS_DB = DENNIS_CIARA_DIR / "kelmarsh_data_storms.db"
KELMARSH_STATUS_STORMS_DB = DENNIS_CIARA_DIR / "kelmarsh_status_storms.db"
PENMANSHIEL_DATA_STORMS_DB = DENNIS_CIARA_DIR / "penmanshiel_data_storms.db"
PENMANSHIEL_STATUS_STORMS_DB = DENNIS_CIARA_DIR / "penmanshiel_status_storms.db"

# Frontend directory
FRONTEND_DIR = BASE_DIR / "frontend"

app = FastAPI(title="Storm Dennis & Ciara - Wind Turbine SCADA API")

# allow all origins for convenience during development (adjust in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# Serve frontend static files
@app.get("/")
async def serve_frontend():
    """Serve the main index.html"""
    index_path = FRONTEND_DIR / "index.html"
    if not index_path.exists():
        return {"error": "index.html not found", "path": str(index_path), "frontend_dir": str(FRONTEND_DIR), "base_dir": str(BASE_DIR)}
    return FileResponse(index_path)


@app.get("/favicon.ico")
async def favicon():
    """Return empty favicon to avoid 404"""
    return Response(status_code=204)


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
    gen = stream_table_rows_from_db(PENMANSHIEL_DATA_STORMS_DB, tbl, request, start_rowid, wait_seconds, since_ms)
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
    gen = stream_table_rows_from_db(PENMANSHIEL_STATUS_STORMS_DB, tbl, request, start_rowid, wait_seconds, since_ms)
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
    gen = stream_table_rows_from_db(KELMARSH_DATA_STORMS_DB, tbl, request, start_rowid, wait_seconds, since_ms)
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
    gen = stream_table_rows_from_db(KELMARSH_STATUS_STORMS_DB, tbl, request, start_rowid, wait_seconds, since_ms)
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
        'penmanshiel_data': PENMANSHIEL_DATA_STORMS_DB,
        'penmanshiel_status': PENMANSHIEL_STATUS_STORMS_DB,
        'kelmarsh_data': KELMARSH_DATA_STORMS_DB,
        'kelmarsh_status': KELMARSH_STATUS_STORMS_DB,
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


def parse_timestamp_to_datetime(ts_str: str) -> Optional[datetime]:
    """Parse a timestamp string to datetime object."""
    if not ts_str or not isinstance(ts_str, str):
        return None
    try:
        return datetime.strptime(ts_str.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        pass
    try:
        return datetime.strptime(ts_str.strip(), "%Y-%m-%d")
    except ValueError:
        pass
    return None


async def stream_combined_kelmarsh(turbine: int, request: Request, data_interval: float) -> AsyncGenerator[str, None]:
    """Stream combined data and status for a single Kelmarsh turbine.

    - Data updates every data_interval seconds (default 1 second)
    - Status updates are synchronized with data timestamps:
      The next status is shown when the current data timestamp >= status's Timestamp end
    """
    data_db = str(KELMARSH_DATA_STORMS_DB)
    status_db = str(KELMARSH_STATUS_STORMS_DB)
    tbl = f"turbine_{turbine}"

    if not KELMARSH_DATA_STORMS_DB.exists() or not KELMARSH_STATUS_STORMS_DB.exists():
        yield sse_encode(json.dumps({"error": "db_not_found"}), event="error")
        return

    try:
        data_ws = max(0.1, float(data_interval))
    except Exception:
        data_ws = 1.0

    data_rowid = 0
    status_rowid = 0

    # Track the current status's end timestamp - when data reaches this time, fetch next status
    current_status_end_dt: Optional[datetime] = None
    current_status_record = None

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

        # Find timestamp column names
        data_ts_col = next((col for col in data_cols if 'date' in col.lower() or 'time' in col.lower() or 'timestamp' in col.lower()), None)
        status_end_col = next((col for col in status_cols if 'timestamp end' in col.lower()), None)

        # Fetch the first status record immediately
        async with status_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (status_rowid,)) as cur:
            row = await cur.fetchone()
            if row:
                status_rowid = row[0]
                values = row[1:]
                current_status_record = {col: (None if val is None else str(val)) for col, val in zip(status_cols, values)}
                # Get the end timestamp for this status
                if status_end_col and current_status_record.get(status_end_col):
                    current_status_end_dt = parse_timestamp_to_datetime(current_status_record[status_end_col])

        # Send initial status immediately
        first_payload = {
            "turbine": turbine,
            "data": None,
            "status": {"rowid": status_rowid, "record": current_status_record, "updated": True} if current_status_record else None
        }
        yield sse_encode(json.dumps(first_payload, ensure_ascii=False))

        while True:
            # Check disconnect
            try:
                if await request.is_disconnected():
                    break
            except Exception:
                pass

            # Fetch next data record
            data_record = None
            current_data_dt: Optional[datetime] = None
            async with data_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (data_rowid,)) as cur:
                row = await cur.fetchone()
                if row:
                    data_rowid = row[0]
                    values = row[1:]
                    data_record = {col: (None if val is None else str(val)) for col, val in zip(data_cols, values)}
                    # Get the current data timestamp
                    if data_ts_col and data_record.get(data_ts_col):
                        current_data_dt = parse_timestamp_to_datetime(data_record[data_ts_col])

            # Check if we should advance to the next status record
            # This happens when the current data timestamp >= current status's end timestamp
            status_updated = False
            if current_data_dt and current_status_end_dt and current_data_dt >= current_status_end_dt:
                # Fetch next status record(s) until we find one whose end time is > current data time
                while True:
                    async with status_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (status_rowid,)) as cur:
                        row = await cur.fetchone()
                        if row:
                            status_rowid = row[0]
                            values = row[1:]
                            current_status_record = {col: (None if val is None else str(val)) for col, val in zip(status_cols, values)}
                            status_updated = True
                            # Get the end timestamp for this status
                            if status_end_col and current_status_record.get(status_end_col):
                                current_status_end_dt = parse_timestamp_to_datetime(current_status_record[status_end_col])
                                # If this status's end time is still <= current data time, continue to next
                                if current_status_end_dt and current_data_dt >= current_status_end_dt:
                                    continue  # Fetch next status
                            break  # Found a status with end time > current data time, or no end time
                        else:
                            # No more status records
                            current_status_end_dt = None
                            break

            # Send combined payload
            payload = {
                "turbine": turbine,
                "data": {"rowid": data_rowid, "record": data_record} if data_record else None,
                "status": {"rowid": status_rowid, "record": current_status_record, "updated": status_updated} if status_updated else None
            }
            yield sse_encode(json.dumps(payload, ensure_ascii=False))

            # Wait for data interval
            await asyncio.sleep(data_ws)

            # If data stream exhausted, send end event and break
            if data_record is None:
                yield sse_encode(json.dumps({"info": "end_of_data", "turbine": turbine}), event="end")
                break


@app.get("/sse/kelmarsh-combined/{turbine}")
async def sse_kelmarsh_combined(request: Request,
                                 turbine: int,
                                 data_interval: float = Query(1.0, ge=0.1, description="Seconds between data updates")):
    """Stream combined data and status for a single Kelmarsh turbine.

    - Data updates every data_interval seconds (default 1 second)
    - Status updates are synchronized with data timestamps:
      The next status is shown when the current data timestamp >= current status's Timestamp end
    """
    if turbine < 1 or turbine > 6:
        return StreamingResponse(
            (sse_encode(json.dumps({"error": "invalid_turbine_range", "min": 1, "max": 6}), event="error") for _ in ()),
            media_type="text/event-stream"
        )
    gen = stream_combined_kelmarsh(turbine, request, data_interval)
    return StreamingResponse(gen, media_type="text/event-stream")


async def stream_combined_penmanshiel(turbine: int, request: Request, data_interval: float) -> AsyncGenerator[str, None]:
    """Stream combined data and status for a single Penmanshiel turbine.

    - Data updates every data_interval seconds (default 1 second)
    - Status updates are synchronized with data timestamps:
      The next status is shown when the current data timestamp >= status's Timestamp end
    """
    data_db = str(PENMANSHIEL_DATA_STORMS_DB)
    status_db = str(PENMANSHIEL_STATUS_STORMS_DB)
    tbl = f"turbine_{turbine}"

    if not PENMANSHIEL_DATA_STORMS_DB.exists() or not PENMANSHIEL_STATUS_STORMS_DB.exists():
        yield sse_encode(json.dumps({"error": "db_not_found"}), event="error")
        return

    try:
        data_ws = max(0.1, float(data_interval))
    except Exception:
        data_ws = 1.0

    data_rowid = 0
    status_rowid = 0

    # Track the current status's end timestamp - when data reaches this time, fetch next status
    current_status_end_dt: Optional[datetime] = None
    current_status_record = None

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

        # Find timestamp column names
        data_ts_col = next((col for col in data_cols if 'date' in col.lower() or 'time' in col.lower() or 'timestamp' in col.lower()), None)
        status_end_col = next((col for col in status_cols if 'timestamp end' in col.lower()), None)

        # Fetch the first status record immediately
        async with status_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (status_rowid,)) as cur:
            row = await cur.fetchone()
            if row:
                status_rowid = row[0]
                values = row[1:]
                current_status_record = {col: (None if val is None else str(val)) for col, val in zip(status_cols, values)}
                # Get the end timestamp for this status
                if status_end_col and current_status_record.get(status_end_col):
                    current_status_end_dt = parse_timestamp_to_datetime(current_status_record[status_end_col])

        # Send initial status immediately
        first_payload = {
            "turbine": turbine,
            "data": None,
            "status": {"rowid": status_rowid, "record": current_status_record, "updated": True} if current_status_record else None
        }
        yield sse_encode(json.dumps(first_payload, ensure_ascii=False))

        while True:
            # Check disconnect
            try:
                if await request.is_disconnected():
                    break
            except Exception:
                pass

            # Fetch next data record
            data_record = None
            current_data_dt: Optional[datetime] = None
            async with data_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (data_rowid,)) as cur:
                row = await cur.fetchone()
                if row:
                    data_rowid = row[0]
                    values = row[1:]
                    data_record = {col: (None if val is None else str(val)) for col, val in zip(data_cols, values)}
                    # Get the current data timestamp
                    if data_ts_col and data_record.get(data_ts_col):
                        current_data_dt = parse_timestamp_to_datetime(data_record[data_ts_col])

            # Check if we should advance to the next status record
            # This happens when the current data timestamp >= current status's end timestamp
            status_updated = False
            if current_data_dt and current_status_end_dt and current_data_dt >= current_status_end_dt:
                # Fetch next status record(s) until we find one whose end time is > current data time
                while True:
                    async with status_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (status_rowid,)) as cur:
                        row = await cur.fetchone()
                        if row:
                            status_rowid = row[0]
                            values = row[1:]
                            current_status_record = {col: (None if val is None else str(val)) for col, val in zip(status_cols, values)}
                            status_updated = True
                            # Get the end timestamp for this status
                            if status_end_col and current_status_record.get(status_end_col):
                                current_status_end_dt = parse_timestamp_to_datetime(current_status_record[status_end_col])
                                # If this status's end time is still <= current data time, continue to next
                                if current_status_end_dt and current_data_dt >= current_status_end_dt:
                                    continue  # Fetch next status
                            break  # Found a status with end time > current data time, or no end time
                        else:
                            # No more status records
                            current_status_end_dt = None
                            break

            # Send combined payload
            payload = {
                "turbine": turbine,
                "data": {"rowid": data_rowid, "record": data_record} if data_record else None,
                "status": {"rowid": status_rowid, "record": current_status_record, "updated": status_updated} if status_updated else None
            }
            yield sse_encode(json.dumps(payload, ensure_ascii=False))

            # Wait for data interval
            await asyncio.sleep(data_ws)

            # If data stream exhausted, send end event and break
            if data_record is None:
                yield sse_encode(json.dumps({"info": "end_of_data", "turbine": turbine}), event="end")
                break


@app.get("/sse/penmanshiel-combined/{turbine}")
async def sse_penmanshiel_combined(request: Request,
                                    turbine: int,
                                    data_interval: float = Query(1.0, ge=0.1, description="Seconds between data updates")):
    """Stream combined data and status for a single Penmanshiel turbine.

    - Data updates every data_interval seconds (default 1 second)
    - Status updates are synchronized with data timestamps:
      The next status is shown when the current data timestamp >= current status's Timestamp end
    """
    if turbine < 1 or turbine > 15:
        return StreamingResponse(
            (sse_encode(json.dumps({"error": "invalid_turbine_range", "min": 1, "max": 15}), event="error") for _ in ()),
            media_type="text/event-stream"
        )
    # Skip turbine 3 which has no data
    gen = stream_combined_penmanshiel(turbine, request, data_interval)
    return StreamingResponse(gen, media_type="text/event-stream")


async def stream_all_turbines(site: str, request: Request, data_interval: float) -> AsyncGenerator[str, None]:
    """Stream combined data and status for ALL turbines of a site in a single SSE connection.

    This avoids browser connection limits by using just one connection for all turbines.
    Each message contains data for all turbines.
    """
    if site == 'kelmarsh':
        data_db = str(KELMARSH_DATA_STORMS_DB)
        status_db = str(KELMARSH_STATUS_STORMS_DB)
        turbine_list = [1, 2, 3, 4, 5, 6]
    else:  # penmanshiel
        data_db = str(PENMANSHIEL_DATA_STORMS_DB)
        status_db = str(PENMANSHIEL_STATUS_STORMS_DB)
        turbine_list = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]  # skip 3

    try:
        data_ws = max(0.1, float(data_interval))
    except Exception:
        data_ws = 1.0

    # Track state for each turbine
    turbine_state = {}

    async with aiosqlite.connect(data_db) as data_conn, aiosqlite.connect(status_db) as status_conn:
        # Initialize state for each turbine
        for turbine in turbine_list:
            tbl = f"turbine_{turbine}"

            # Get column names
            data_cols = []
            status_cols = []

            try:
                async with data_conn.execute(f"PRAGMA table_info('{tbl}')") as cur:
                    async for r in cur:
                        data_cols.append(r[1])
            except:
                pass

            try:
                async with status_conn.execute(f"PRAGMA table_info('{tbl}')") as cur:
                    async for r in cur:
                        status_cols.append(r[1])
            except:
                pass

            if not data_cols or not status_cols:
                continue

            data_ts_col = next((col for col in data_cols if 'date' in col.lower() or 'time' in col.lower() or 'timestamp' in col.lower()), None)
            status_end_col = next((col for col in status_cols if 'timestamp end' in col.lower()), None)

            # Fetch first status record
            current_status_record = None
            current_status_end_dt = None
            status_rowid = 0

            try:
                async with status_conn.execute(f"SELECT rowid, * FROM '{tbl}' ORDER BY rowid ASC LIMIT 1") as cur:
                    row = await cur.fetchone()
                    if row:
                        status_rowid = row[0]
                        values = row[1:]
                        current_status_record = {col: (None if val is None else str(val)) for col, val in zip(status_cols, values)}
                        if status_end_col and current_status_record.get(status_end_col):
                            current_status_end_dt = parse_timestamp_to_datetime(current_status_record[status_end_col])
            except:
                pass

            turbine_state[turbine] = {
                'data_cols': data_cols,
                'status_cols': status_cols,
                'data_ts_col': data_ts_col,
                'status_end_col': status_end_col,
                'data_rowid': 0,
                'status_rowid': status_rowid,
                'current_status_record': current_status_record,
                'current_status_end_dt': current_status_end_dt
            }

        # Send initial status for all turbines
        initial_payload = {"site": site, "turbines": {}}
        for turbine, state in turbine_state.items():
            initial_payload["turbines"][turbine] = {
                "data": None,
                "status": {"record": state['current_status_record'], "updated": True} if state['current_status_record'] else None
            }
        yield sse_encode(json.dumps(initial_payload, ensure_ascii=False))

        # Main streaming loop
        while True:
            try:
                if await request.is_disconnected():
                    break
            except:
                pass

            all_data_exhausted = True
            batch_payload = {"site": site, "turbines": {}}

            for turbine, state in turbine_state.items():
                tbl = f"turbine_{turbine}"
                data_cols = state['data_cols']
                status_cols = state['status_cols']
                data_ts_col = state['data_ts_col']
                status_end_col = state['status_end_col']

                # Fetch next data record
                data_record = None
                current_data_dt = None
                try:
                    async with data_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (state['data_rowid'],)) as cur:
                        row = await cur.fetchone()
                        if row:
                            state['data_rowid'] = row[0]
                            values = row[1:]
                            data_record = {col: (None if val is None else str(val)) for col, val in zip(data_cols, values)}
                            all_data_exhausted = False
                            if data_ts_col and data_record.get(data_ts_col):
                                current_data_dt = parse_timestamp_to_datetime(data_record[data_ts_col])
                except:
                    pass

                # Check if we should advance status
                status_updated = False
                if current_data_dt and state['current_status_end_dt'] and current_data_dt >= state['current_status_end_dt']:
                    while True:
                        try:
                            async with status_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (state['status_rowid'],)) as cur:
                                row = await cur.fetchone()
                                if row:
                                    state['status_rowid'] = row[0]
                                    values = row[1:]
                                    state['current_status_record'] = {col: (None if val is None else str(val)) for col, val in zip(status_cols, values)}
                                    status_updated = True
                                    if status_end_col and state['current_status_record'].get(status_end_col):
                                        state['current_status_end_dt'] = parse_timestamp_to_datetime(state['current_status_record'][status_end_col])
                                        if state['current_status_end_dt'] and current_data_dt >= state['current_status_end_dt']:
                                            continue
                                    break
                                else:
                                    state['current_status_end_dt'] = None
                                    break
                        except:
                            break

                batch_payload["turbines"][turbine] = {
                    "data": {"rowid": state['data_rowid'], "record": data_record} if data_record else None,
                    "status": {"record": state['current_status_record'], "updated": status_updated} if status_updated else None
                }

            yield sse_encode(json.dumps(batch_payload, ensure_ascii=False))

            if all_data_exhausted:
                yield sse_encode(json.dumps({"info": "end_of_data", "site": site}), event="end")
                break

            await asyncio.sleep(data_ws)


@app.get("/sse/all-turbines/{site}")
async def sse_all_turbines(request: Request,
                           site: str,
                           data_interval: float = Query(1.0, ge=0.1, description="Seconds between data updates")):
    """Stream combined data and status for ALL turbines of a site in a single SSE connection.

    This avoids browser connection limits (typically 6 per domain).
    """
    if site not in ('kelmarsh', 'penmanshiel'):
        return StreamingResponse(
            (sse_encode(json.dumps({"error": "invalid_site", "valid": ["kelmarsh", "penmanshiel"]}), event="error") for _ in ()),
            media_type="text/event-stream"
        )
    gen = stream_all_turbines(site, request, data_interval)
    return StreamingResponse(gen, media_type="text/event-stream")


@app.get("/api/available-dates/{site}")
async def get_available_dates(site: str):
    """Get list of dates that have data for a site.

    Returns JSON with min_date, max_date, and array of all available dates.
    """
    if site == 'kelmarsh':
        data_db = KELMARSH_DATA_STORMS_DB
        date_col = "Date and time"
    elif site == 'penmanshiel':
        data_db = PENMANSHIEL_DATA_STORMS_DB
        date_col = "Date and time"
    else:
        return {"error": "invalid_site", "valid": ["kelmarsh", "penmanshiel"]}

    if not data_db.exists():
        return {"error": "db_not_found"}

    async with aiosqlite.connect(str(data_db)) as conn:
        # Get first turbine table
        async with conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'turbine_%' ORDER BY name LIMIT 1") as cur:
            row = await cur.fetchone()
            if not row:
                return {"error": "no_tables"}
            table = row[0]

        # Get distinct dates
        async with conn.execute(f"SELECT DISTINCT DATE([{date_col}]) as d FROM [{table}] ORDER BY d") as cur:
            dates = [row[0] for row in await cur.fetchall()]

        if not dates:
            return {"error": "no_dates"}

        return {
            "site": site,
            "min_date": dates[0],
            "max_date": dates[-1],
            "dates": dates,
            "count": len(dates)
        }


@app.get("/sse/all-turbines/{site}/from/{start_date}")
async def sse_all_turbines_from_date(request: Request,
                                      site: str,
                                      start_date: str,
                                      data_interval: float = Query(1.0, ge=0.1, description="Seconds between data updates")):
    """Stream combined data and status for ALL turbines starting from a specific date.

    start_date format: YYYY-MM-DD
    """
    if site not in ('kelmarsh', 'penmanshiel'):
        return StreamingResponse(
            (sse_encode(json.dumps({"error": "invalid_site", "valid": ["kelmarsh", "penmanshiel"]}), event="error") for _ in ()),
            media_type="text/event-stream"
        )

    # Validate date format
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
    except ValueError:
        return StreamingResponse(
            (sse_encode(json.dumps({"error": "invalid_date_format", "expected": "YYYY-MM-DD"}), event="error") for _ in ()),
            media_type="text/event-stream"
        )

    gen = stream_all_turbines_from_date(site, request, data_interval, start_date)
    return StreamingResponse(gen, media_type="text/event-stream")


async def stream_all_turbines_from_date(site: str, request: Request, data_interval: float, start_date: str) -> AsyncGenerator[str, None]:
    """Stream combined data and status for ALL turbines of a site starting from a specific date."""
    if site == 'kelmarsh':
        data_db = str(KELMARSH_DATA_STORMS_DB)
        status_db = str(KELMARSH_STATUS_STORMS_DB)
        turbine_list = [1, 2, 3, 4, 5, 6]
        date_col = "Date and time"
    else:  # penmanshiel
        data_db = str(PENMANSHIEL_DATA_STORMS_DB)
        status_db = str(PENMANSHIEL_STATUS_STORMS_DB)
        turbine_list = [1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]  # skip 3
        date_col = "Date and time"

    try:
        data_ws = max(0.1, float(data_interval))
    except Exception:
        data_ws = 1.0

    # Parse start date to create datetime string for comparison
    start_datetime = start_date + " 00:00:00"

    # Track state for each turbine
    turbine_state = {}

    async with aiosqlite.connect(data_db) as data_conn, aiosqlite.connect(status_db) as status_conn:
        # Initialize state for each turbine
        for turbine in turbine_list:
            tbl = f"turbine_{turbine}"

            # Get column names
            data_cols = []
            status_cols = []

            try:
                async with data_conn.execute(f"PRAGMA table_info('{tbl}')") as cur:
                    async for r in cur:
                        data_cols.append(r[1])
            except:
                pass

            try:
                async with status_conn.execute(f"PRAGMA table_info('{tbl}')") as cur:
                    async for r in cur:
                        status_cols.append(r[1])
            except:
                pass

            if not data_cols or not status_cols:
                continue

            data_ts_col = next((col for col in data_cols if 'date' in col.lower() or 'time' in col.lower() or 'timestamp' in col.lower()), None)
            status_end_col = next((col for col in status_cols if 'timestamp end' in col.lower()), None)
            status_start_col = next((col for col in status_cols if 'timestamp start' in col.lower()), None)

            # Find starting rowid for data based on date
            data_start_rowid = 0
            if data_ts_col:
                try:
                    async with data_conn.execute(f"SELECT rowid FROM '{tbl}' WHERE [{data_ts_col}] >= ? ORDER BY rowid ASC LIMIT 1", (start_datetime,)) as cur:
                        row = await cur.fetchone()
                        if row:
                            data_start_rowid = row[0] - 1  # -1 because we use > in the main loop
                except:
                    pass

            # Find starting rowid for status based on date
            status_start_rowid = 0
            if status_start_col:
                try:
                    async with status_conn.execute(f"SELECT rowid FROM '{tbl}' WHERE [{status_start_col}] >= ? ORDER BY rowid ASC LIMIT 1", (start_datetime,)) as cur:
                        row = await cur.fetchone()
                        if row:
                            status_start_rowid = row[0] - 1
                except:
                    pass

            # Fetch first status record from the start date
            current_status_record = None
            current_status_end_dt = None
            status_rowid = status_start_rowid

            try:
                async with status_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (status_rowid,)) as cur:
                    row = await cur.fetchone()
                    if row:
                        status_rowid = row[0]
                        values = row[1:]
                        current_status_record = {col: (None if val is None else str(val)) for col, val in zip(status_cols, values)}
                        if status_end_col and current_status_record.get(status_end_col):
                            current_status_end_dt = parse_timestamp_to_datetime(current_status_record[status_end_col])
            except:
                pass

            turbine_state[turbine] = {
                'data_cols': data_cols,
                'status_cols': status_cols,
                'data_ts_col': data_ts_col,
                'status_end_col': status_end_col,
                'data_rowid': data_start_rowid,
                'status_rowid': status_rowid,
                'current_status_record': current_status_record,
                'current_status_end_dt': current_status_end_dt
            }

        # Send initial status for all turbines
        initial_payload = {"site": site, "start_date": start_date, "turbines": {}}
        for turbine, state in turbine_state.items():
            initial_payload["turbines"][turbine] = {
                "data": None,
                "status": {"record": state['current_status_record'], "updated": True} if state['current_status_record'] else None
            }
        yield sse_encode(json.dumps(initial_payload, ensure_ascii=False))

        # Main streaming loop (same as stream_all_turbines)
        while True:
            try:
                if await request.is_disconnected():
                    break
            except:
                pass

            all_data_exhausted = True
            batch_payload = {"site": site, "turbines": {}}

            for turbine, state in turbine_state.items():
                tbl = f"turbine_{turbine}"
                data_cols = state['data_cols']
                status_cols = state['status_cols']
                data_ts_col = state['data_ts_col']
                status_end_col = state['status_end_col']

                # Fetch next data record
                data_record = None
                current_data_dt = None
                try:
                    async with data_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (state['data_rowid'],)) as cur:
                        row = await cur.fetchone()
                        if row:
                            state['data_rowid'] = row[0]
                            values = row[1:]
                            data_record = {col: (None if val is None else str(val)) for col, val in zip(data_cols, values)}
                            all_data_exhausted = False
                            if data_ts_col and data_record.get(data_ts_col):
                                current_data_dt = parse_timestamp_to_datetime(data_record[data_ts_col])
                except:
                    pass

                # Check if we should advance status
                status_updated = False
                if current_data_dt and state['current_status_end_dt'] and current_data_dt >= state['current_status_end_dt']:
                    while True:
                        try:
                            async with status_conn.execute(f"SELECT rowid, * FROM '{tbl}' WHERE rowid > ? ORDER BY rowid ASC LIMIT 1", (state['status_rowid'],)) as cur:
                                row = await cur.fetchone()
                                if row:
                                    state['status_rowid'] = row[0]
                                    values = row[1:]
                                    state['current_status_record'] = {col: (None if val is None else str(val)) for col, val in zip(status_cols, values)}
                                    status_updated = True
                                    if status_end_col and state['current_status_record'].get(status_end_col):
                                        state['current_status_end_dt'] = parse_timestamp_to_datetime(state['current_status_record'][status_end_col])
                                        if state['current_status_end_dt'] and current_data_dt >= state['current_status_end_dt']:
                                            continue
                                    break
                                else:
                                    state['current_status_end_dt'] = None
                                    break
                        except:
                            break

                batch_payload["turbines"][turbine] = {
                    "data": {"rowid": state['data_rowid'], "record": data_record} if data_record else None,
                    "status": {"record": state['current_status_record'], "updated": status_updated} if status_updated else None
                }

            yield sse_encode(json.dumps(batch_payload, ensure_ascii=False))

            if all_data_exhausted:
                yield sse_encode(json.dumps({"info": "end_of_data", "site": site}), event="end")
                break

            await asyncio.sleep(data_ws)


if __name__ == '__main__':
    # Allow running `python api/main.py` or running the file from PyCharm 'Run'
    try:
        import uvicorn
    except Exception:
        raise RuntimeError("uvicorn is required to run the server. Install with: pip install uvicorn[standard]")

    # Note: reload should be False when running programmatically; use --reload from CLI if desired.
    uvicorn.run(app, host="127.0.0.1", port=8000)
