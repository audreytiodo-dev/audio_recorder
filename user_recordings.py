#!/usr/bin/env python3
"""
Yeastar P-Series Appliance — Recording Downloader
--------------------------------------------------
1. Authenticates with the PBX and retrieves an access token.
2. Searches ALL recordings within the configured date range.
3. Downloads every recording file into a local output folder.

Usage:
    python user_recordings.py

Configuration is loaded from a .env file in the same directory.
Copy .env.example to .env and fill in your values before running.
"""

import json
import os
import re
import sys
import time
import requests
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime
from threading import Lock
from dotenv import load_dotenv

# Load .env file
load_dotenv()

def _require(key: str) -> str:
    """Read a required env var, exit with a clear message if missing."""
    value = os.getenv(key)
    if not value:
        print(f"[CONFIG ERROR] Required variable '{key}' is missing from your .env file.")
        sys.exit(1)
    return value

def _optional(key: str, default: str | None = None) -> str | None:
    """Read an optional env var, returning default if not set or empty."""
    value = os.getenv(key)
    return value if value else default

# CONFIG — loaded from .env
PBX_HOST      = _require("PBX_HOST")
PBX_PORT      = int(_optional("PBX_PORT", "8088"))
USE_HTTPS     = _optional("USE_HTTPS", "false").lower() == "true"
CLIENT_ID     = _require("CLIENT_ID")
CLIENT_SECRET = _require("CLIENT_SECRET")
TARGET_NUMBER = _optional("TARGET_NUMBER")  # Optional: leave empty to fetch ALL recordings
OUTPUT_FOLDER = _optional("OUTPUT_FOLDER", "/recordings/yeaster")

START_TIME    = _optional("START_TIME")   # e.g. "2024-01-01 00:00:00"
END_TIME      = _optional("END_TIME")     # e.g. "2024-12-31 23:59:59"

PAGE_SIZE     = int(_optional("PAGE_SIZE", "100"))
MAX_WORKERS   = int(_optional("MAX_WORKERS", "5"))
VERIFY_SSL    = _optional("VERIFY_SSL", "false").lower() == "true"

# Stop paginating once the results predate START_TIME instead of walking the
# whole database. Set to false to force the exhaustive scan.
FAST_SEARCH   = _optional("FAST_SEARCH", "true").lower() == "true"

# Thread-safe print lock
_print_lock = Lock()

def tprint(*args, **kwargs):
    """Thread-safe print."""
    with _print_lock:
        print(*args, **kwargs)


# Suppress SSL warnings when VERIFY_SSL is False
if not VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


MAX_RETRIES     = 5
RETRY_DELAY     = 5    # base seconds between attempts (grows linearly)
REQUEST_TIMEOUT = 60   # seconds

# Transient server-side failures worth retrying. The Yeastar RAS gateway
# returns 502/503/504 under load, which arrive as valid HTTP responses rather
# than connection errors — so they must be handled explicitly.
RETRY_STATUSES = {429, 500, 502, 503, 504}


def request_with_retry(method: str, url: str, token_mgr=None, **kwargs):
    """Issue an HTTP request, retrying on transient network and server failures.

    The PBX is reached through the Yeastar RAS tunnel, which intermittently
    resets connections and returns gateway errors mid-run. Without this, a
    single hiccup aborts a search that takes hundreds of pages to complete.

    Pass token_mgr for any authenticated call: the access token is then read
    fresh on every attempt and injected as the access_token query parameter.
    A retry sequence can span minutes — longer than the 60s expiry buffer —
    so replaying a token captured up front would burn every attempt on
    TOKEN EXPIRED. This also removes the race where another thread refreshes
    the token between a caller's two requests.

    Never pass token_mgr from TokenManager._do_refresh: refreshing the token
    would then require a valid token, recursing forever. The refresh call
    authenticates with the refresh_token instead.

    On persistent failure the last response is returned as-is, so the caller's
    raise_for_status() reports the real status code.
    """
    kwargs.setdefault("timeout", REQUEST_TIMEOUT)

    for attempt in range(1, MAX_RETRIES + 1):
        if token_mgr is not None:
            kwargs.setdefault("params", {})["access_token"] = token_mgr.get_token()
        try:
            resp = requests.request(method, url, **kwargs)
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            if attempt == MAX_RETRIES:
                raise
            reason = type(e).__name__
        else:
            if resp.status_code not in RETRY_STATUSES:
                return resp
            if attempt == MAX_RETRIES:
                return resp
            resp.close()  # release the socket before retrying (matters for stream=True)
            reason = f"HTTP {resp.status_code}"

        delay = RETRY_DELAY * attempt
        tprint(f"  [RETRY] {reason}, attempt {attempt + 1}/{MAX_RETRIES} in {delay}s...")
        time.sleep(delay)


def base_url() -> str:
    scheme = "https" if USE_HTTPS else "http"
    return f"{scheme}://{PBX_HOST}:{PBX_PORT}"


def api_url(path: str) -> str:
    return f"{base_url()}/openapi/v1.0/{path}"


class TokenManager:
    """
    Thread-safe token holder that proactively refreshes the access token
    60 seconds before it expires, so long download phases never hit an
    auth failure due to a stale token.
    """

    REFRESH_BUFFER = 60  # seconds before expiry to trigger a refresh

    def __init__(self, access_token: str, refresh_token: str, expires_in: int):
        self._access_token = access_token
        self._refresh_token = refresh_token
        self._expires_at = time.time() + expires_in
        self._lock = Lock()

    def get_token(self) -> str:
        """Return a valid access token, refreshing first if near expiry."""
        with self._lock:
            if time.time() >= self._expires_at - self.REFRESH_BUFFER:
                self._do_refresh()
            return self._access_token

    def _do_refresh(self) -> None:
        tprint("[AUTH] Access token expiring soon — refreshing...")
        resp = request_with_retry(
            "POST",
            api_url("refresh_token"),
            json={"refresh_token": self._refresh_token},
            headers={"Content-Type": "application/json", "User-Agent": "OpenAPI"},
            verify=VERIFY_SSL,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("errcode") != 0:
            raise RuntimeError(
                f"Token refresh failed: {data.get('errmsg')} (code {data.get('errcode')})"
            )

        self._access_token = data["access_token"]
        self._refresh_token = data["refresh_token"]
        expires_in = data.get("access_token_expire_time", 1800)
        self._expires_at = time.time() + expires_in
        tprint(f"[AUTH] Token refreshed. New token expires in {expires_in}s.")


def get_access_token() -> TokenManager:
    """
    Authenticate with the PBX using Client ID / Secret.
    Returns a TokenManager that auto-refreshes before the token expires.
    """
    tprint("[AUTH] Requesting access token...")
    resp = request_with_retry(
        "POST",
        api_url("get_token"),
        json={"username": CLIENT_ID, "password": CLIENT_SECRET},
        headers={"Content-Type": "application/json", "User-Agent": "OpenAPI"},
        verify=VERIFY_SSL,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("errcode") != 0:
        raise RuntimeError(f"Auth failed: {data.get('errmsg')} (code {data.get('errcode')})")

    token = data["access_token"]
    refresh = data["refresh_token"]
    expires_in = data.get("access_token_expire_time", 1800)
    tprint(f"[AUTH] Token obtained. Expires in {expires_in}s.")
    return TokenManager(token, refresh, expires_in)


def record_timestamp(rec: dict) -> int | None:
    """Unix timestamp of a record's 'time' field, or None if unparseable."""
    try:
        return int(datetime.strptime(rec.get("time", ""), "%m/%d/%Y %H:%M:%S").timestamp())
    except (ValueError, TypeError):
        return None


def search_all_recordings(token_mgr: "TokenManager") -> list[dict]:
    """
    Fetch recordings from the PBX, newest first. No caller/callee filter.

    The PBX ignores start_time/end_time on this endpoint and always reports the
    whole database, so the window is really applied locally by the caller. With
    FAST_SEARCH we exploit the descending sort to stop paginating once an entire
    page predates START_TIME: a recent day then costs a handful of pages instead
    of walking ~220 of them over the RAS tunnel.

    Safety: the descending order is re-checked on every record. If the PBX ever
    stops honouring it, the early stop is abandoned and the exhaustive scan
    resumes, so a broken sort can never silently truncate the result. Stopping
    only after a *fully* out-of-window page also keeps this safe while new calls
    shift records down mid-scan.
    """
    tprint("[SEARCH] Fetching recordings from PBX (newest first)...")
    start_ts   = to_unix(START_TIME) if START_TIME else None
    early_stop = FAST_SEARCH and start_ts is not None
    if early_stop:
        tprint(f"[SEARCH] Fast mode: will stop once results predate {START_TIME}.")

    results  = []
    page     = 1
    prev_ts  = None
    sort_ok  = True

    while True:
        params = {
            "page": page,
            "page_size": PAGE_SIZE,
            "sort_by": "time",
            "order_by": "desc",
        }
        if START_TIME:
            params["start_time"] = to_unix(START_TIME)
        if END_TIME:
            params["end_time"] = to_unix(END_TIME)

        resp = request_with_retry(
            "GET",
            api_url("recording/search"),
            token_mgr=token_mgr,
            params=params,
            headers={"User-Agent": "OpenAPI"},
            verify=VERIFY_SSL,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("errcode") != 0:
            raise RuntimeError(
                f"Search failed: {data.get('errmsg')} "
                f"(code {data.get('errcode')})"
            )

        batch = data.get("data") or []
        results.extend(batch)
        total = data.get("total_number", 0)
        tprint(f"  Page {page}: {len(batch)} records (total reported: {total})")

        # Guard: confirm the PBX really is returning newest-first.
        if early_stop and sort_ok:
            for rec in batch:
                ts = record_timestamp(rec)
                if ts is None:
                    continue
                if prev_ts is not None and ts > prev_ts:
                    sort_ok = False
                    tprint("  [WARN] Records are NOT in descending order — "
                           "early stop disabled, continuing with a full scan.")
                    break
                prev_ts = ts

        if len(results) >= total or not batch:
            break

        # Every record on this page predates the window, and the descending sort
        # guarantees each later page is older still — nothing left to collect.
        if early_stop and sort_ok:
            page_times = [t for t in (record_timestamp(r) for r in batch) if t is not None]
            if page_times and max(page_times) < start_ts:
                skipped = max(0, (total - len(results)) // PAGE_SIZE)
                tprint(f"  [SEARCH] Page {page} is entirely older than START_TIME — "
                       f"stopping early (~{skipped} pages skipped).")
                break

        page += 1

    tprint(f"[SEARCH] Found {len(results)} total recordings.")
    return results


def search_recordings_by_number(token_mgr: "TokenManager", role: str, number: str) -> list[dict]:
    """
    Fetch all recordings where `number` is either 'caller' or 'callee'.
    Paginates automatically until all results are retrieved.
    `role` must be 'caller' or 'callee'.
    """
    tprint(f"[SEARCH] Looking for recordings where {role} = {number}...")
    results = []
    page = 1

    while True:
        params = {
            "page": page,
            "page_size": PAGE_SIZE,
            "sort_by": "time",
            "order_by": "desc",
            role: number,
        }
        if START_TIME:
            params["start_time"] = to_unix(START_TIME)
        if END_TIME:
            params["end_time"] = to_unix(END_TIME)

        resp = request_with_retry(
            "GET",
            api_url("recording/search"),
            token_mgr=token_mgr,
            params=params,
            headers={"User-Agent": "OpenAPI"},
            verify=VERIFY_SSL,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("errcode") != 0:
            raise RuntimeError(
                f"Search failed ({role}={number}): {data.get('errmsg')} "
                f"(code {data.get('errcode')})"
            )

        batch = data.get("data") or []
        results.extend(batch)
        total = data.get("total_number", 0)
        tprint(f"  Page {page}: {len(batch)} records (total reported: {total})")

        if len(results) >= total or not batch:
            break
        page += 1

    tprint(f"[SEARCH] Found {len(results)} records as {role}.")
    return results


def merge_recordings(as_caller: list[dict], as_callee: list[dict]) -> list[dict]:
    """
    Merge two recording lists and deduplicate by recording ID.
    """
    seen_ids = set()
    merged = []
    for rec in as_caller + as_callee:
        rec_id = rec.get("id")
        if rec_id not in seen_ids:
            seen_ids.add(rec_id)
            merged.append(rec)

    # Sort by time descending
    merged.sort(key=lambda r: r.get("time", ""), reverse=True)
    tprint(f"[MERGE] Total unique recordings: {len(merged)}")
    return merged


def deduplicate_by_file(recordings: list[dict]) -> list[dict]:
    """Collapse call-legs that point to the SAME physical recording file.

    The PBX returns one record per call leg (queue entry, agent answer, transfer…)
    and several of these legs reference the same underlying audio file — which would
    otherwise be downloaded as byte-identical duplicates. For each physical file we
    keep a single record, preferring the agent/extension leg over the 'Queue' leg.
    Records without a 'file' value are kept as-is (cannot be matched safely).
    """
    def is_queue(rec: dict) -> bool:
        return "Queue" in (rec.get("call_to") or rec.get("call_to_number") or "")

    by_file: dict[str, list[dict]] = {}
    keep_as_is = []
    for rec in recordings:
        fname = rec.get("file")
        if fname:
            by_file.setdefault(fname, []).append(rec)
        else:
            keep_as_is.append(rec)

    deduped = list(keep_as_is)
    dropped = 0
    for group in by_file.values():
        if len(group) == 1:
            deduped.append(group[0])
            continue
        agents = [r for r in group if not is_queue(r)]
        pool = agents or group
        keeper = min(pool, key=lambda r: r.get("id", 0))
        deduped.append(keeper)
        dropped += len(group) - 1

    deduped.sort(key=lambda r: r.get("id", 0), reverse=True)
    if dropped:
        tprint(f"[DEDUPE] Collapsed {dropped} duplicate call-legs "
               f"(same recording file) -> {len(deduped)} unique recordings.")
    return deduped


def get_download_url(token_mgr: "TokenManager", recording_id: int) -> tuple[str, str]:
    """
    Retrieve the temporary download URL for a given recording ID.
    Returns (filename, download_resource_url).
    """
    resp = request_with_retry(
        "GET",
        api_url("recording/download"),
        token_mgr=token_mgr,
        params={"id": recording_id},
        headers={"User-Agent": "OpenAPI"},
        verify=VERIFY_SSL,
    )
    resp.raise_for_status()
    data = resp.json()

    if data.get("errcode") != 0:
        raise RuntimeError(
            f"Failed to get download URL for ID {recording_id}: "
            f"{data.get('errmsg')} (code {data.get('errcode')})"
        )

    return data["file"], data["download_resource_url"]


def download_file(token_mgr: "TokenManager", resource_url: str, dest_path: Path) -> None:
    """
    Download a recording file using the temporary resource URL.

    The bytes land in a temporary '.part' file that is renamed only once the
    transfer completes. A connection dropped mid-stream therefore never leaves
    a truncated file at dest_path — which a later run would otherwise treat as
    'already downloaded' and skip forever.
    """
    full_url = f"{base_url()}{resource_url}"
    tmp_path = dest_path.with_name(dest_path.name + ".part")

    try:
        resp = request_with_retry(
            "GET",
            full_url,
            token_mgr=token_mgr,
            headers={"User-Agent": "OpenAPI"},
            stream=True,
            verify=VERIFY_SSL,
        )
        resp.raise_for_status()

        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        tmp_path.replace(dest_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def to_unix(dt_str: str) -> int:
    """Convert 'YYYY-MM-DD HH:MM:SS' to Unix timestamp."""
    dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    return int(dt.timestamp())


def save_manifest(folder: Path, recordings: list[dict]) -> None:
    """Save a JSON manifest of all recordings to the output folder."""
    manifest_path = folder / "manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(recordings, f, indent=2)
    tprint(f"[MANIFEST] Saved to {manifest_path}")


def download_recording(token_mgr: TokenManager, rec: dict, output_dir: Path, index: int, total: int) -> dict:
    """
    Worker function executed in a thread pool for each recording.
    The TokenManager is passed down rather than a token string, so every
    request attempt reads a token that is still valid and runs longer than
    30 minutes never hit auth failures.
    Returns a result dict with status, filename, and any error message.
    """
    rec_id    = rec.get("id")
    rec_time  = rec.get("time", "unknown_time")
    call_from = rec.get("call_from_number") or rec.get("call_from", "unknown")
    call_to   = rec.get("call_to_number") or rec.get("call_to", "unknown")
    call_type = rec.get("call_type", "")

    label = f"[{index}/{total}] ID={rec_id} | {rec_time} | {call_from} -> {call_to} ({call_type})"

    try:
        filename, resource_url = get_download_url(token_mgr, rec_id)
        # Keep the PBX native filename so local files match the PBX (and the Drive)
        # exactly. Strip the characters Windows forbids in a filename.
        local_name = re.sub(r'[*?"<>|]', '', filename)

        # Extract date parts to create YYYYMMDD folder
        date_match = re.search(r'(\d{2,4})[-/](\d{2})[-/](\d{2,4})', rec_time)
        if date_match:
            p1, p2, p3 = date_match.groups()
            if len(p1) == 4:
                year, month, day = p1, p2, p3
            else:
                month, day, year = p1, p2, p3
            folder_name = f"{year}{month}{day}"
        else:
            folder_name = "unknown_date"
            
        date_dir = output_dir / folder_name
        date_dir.mkdir(parents=True, exist_ok=True)
        dest = date_dir / local_name

        if dest.exists():
            tprint(f"  {label}\n    -> Already exists, skipping.")
            return {"id": rec_id, "status": "skipped", "file": local_name}

        download_file(token_mgr, resource_url, dest)
        size_kb = dest.stat().st_size // 1024
        tprint(f"  {label}\n    -> Saved: {local_name} ({size_kb} KB)")
        return {"id": rec_id, "status": "success", "file": local_name}

    except Exception as e:
        tprint(f"  {label}\n    FAILED: {e}")
        return {"id": rec_id, "status": "failed", "error": str(e)}


def main():
    # ── Setup output folder ──
    output_dir = Path(OUTPUT_FOLDER)
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[INFO] Output folder: {output_dir.resolve()}")

    # ── Step 1: Authenticate ──
    # The manager, not a bare token string, is threaded through everything below
    # so every attempt reads a token that is still valid.
    token_mgr = get_access_token()

    # ── Step 2: Search recordings ──
    if TARGET_NUMBER:
        print(f"[INFO] Filtering recordings for number: {TARGET_NUMBER}")
        as_caller = search_recordings_by_number(token_mgr, "caller", TARGET_NUMBER)
        as_callee = search_recordings_by_number(token_mgr, "callee", TARGET_NUMBER)
        recordings = merge_recordings(as_caller, as_callee)
    else:
        print("[INFO] No TARGET_NUMBER set -- fetching ALL recordings.")
        recordings = search_all_recordings(token_mgr)

    if not recordings:
        print("[INFO] No recordings found. Exiting.")
        return

    # ── Strict Local Date Filtering ──
    # The Yeastar API sometimes ignores date filters on global searches.
    filtered_recordings = []
    start_ts = to_unix(START_TIME) if START_TIME else 0
    end_ts = to_unix(END_TIME) if END_TIME else float('inf')

    for rec in recordings:
        rec_time_str = rec.get("time", "")
        try:
            # PBX returns dates as MM/DD/YYYY HH:MM:SS
            dt = datetime.strptime(rec_time_str, "%m/%d/%Y %H:%M:%S")
            rec_ts = int(dt.timestamp())
            if start_ts <= rec_ts <= end_ts:
                filtered_recordings.append(rec)
        except Exception:
            filtered_recordings.append(rec)
            
    recordings = filtered_recordings
    print(f"[INFO] Records kept after local date filtering: {len(recordings)}")
    
    if not recordings:
        print("[INFO] No recordings left after date filtering. Exiting.")
        return

    # ── De-duplicate call-legs that share the same physical recording file ──
    # The PBX returns one record per leg (queue, agent, transfers); several point to
    # the same audio file. Keep the agent/extension leg, drop the 'Queue' leg, so the
    # same recording is never downloaded twice.
    recordings = deduplicate_by_file(recordings)
    print(f"[INFO] Records kept after de-duplication: {len(recordings)}")

    save_manifest(output_dir, recordings)

    # ── Step 3: Parallel downloads ──
    total = len(recordings)
    print(f"\n[DOWNLOAD] Starting {total} downloads with {MAX_WORKERS} parallel workers...\n")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(download_recording, token_mgr, rec, output_dir, i, total): rec
            for i, rec in enumerate(recordings, start=1)
        }
        for future in as_completed(futures):
            results.append(future.result())

    # ── Summary ──
    success = sum(1 for r in results if r["status"] == "success")
    skipped = sum(1 for r in results if r["status"] == "skipped")
    failed  = sum(1 for r in results if r["status"] == "failed")

    print(f"\n{'-' * 50}")
    print(f"[DONE] Success: {success} | Skipped: {skipped} | Failed: {failed}")
    print(f"[DONE] Files saved to: {output_dir.resolve()}")


if __name__ == "__main__":
    main()