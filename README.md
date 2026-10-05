# Yeastar P-Series Recording Tools

Batch tools for the **Yeastar P-Series Appliance Edition** PBX: download call
recordings in bulk and clean up duplicates.

---

## Features (`user_recordings.py`)

- Authenticates with the PBX using OAuth 2.0 (Client ID + Client Secret)
- Searches in both directions (number as **caller** and as **callee**), or fetches **all** recordings when no number is set
- Merges and de-duplicates results by recording id
- **De-duplicates call-legs** that point to the same physical recording file (keeps the agent/extension leg, drops the queue leg) so identical audio isn't downloaded twice
- Downloads recording files in **parallel** using a configurable thread pool
- **Auto-refreshes the access token** (thread-safe `TokenManager`) so long runs never hit auth failures
- Saves files under their **PBX native filename**, so local copies match the PBX exactly
- **Retries** transient network drops and gateway errors (429/500/502/503/504) with a linear backoff
- Streams each file through a temporary `.part` renamed only on completion, so an interrupted transfer never leaves a truncated file behind
- **Stops paginating early** once results predate `START_TIME`, turning a ~250-page scan into a handful
- Saves a `manifest.json` of all recording metadata to the output folder
- Skips files already downloaded (safe to re-run) and supports optional date-range filtering

---

## Requirements

- Python 3.10+
- [`requests`](https://pypi.org/project/requests/) and [`python-dotenv`](https://pypi.org/project/python-dotenv/)

```bash
pip install requests python-dotenv
```

---

## PBX Setup

Enable the API on your PBX and obtain credentials:

1. Log in to the PBX web portal as an administrator.
2. Navigate to **Integrations > API**.
3. Enable the API feature if not already enabled.
4. Copy the **Client ID** and **Client Secret** — used as the `username` / `password` for authentication.

---

## Configuration

Settings are read from a `.env` file in this directory. A template is provided as `env.example`:

```bash
cp env.example .env
```

### Variable reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `PBX_HOST` | ✅ | — | RAS hostname (`your-company.ras.yeastar.com`) for remote access, or the PBX's local IP on the office network |
| `CLIENT_ID` | ✅ | — | Client ID from **Integrations > API** |
| `CLIENT_SECRET` | ✅ | — | Client Secret from **Integrations > API** |
| `TARGET_NUMBER` | No | _(all)_ | Phone number to filter on; leave empty to fetch every recording |
| `PBX_PORT` | No | `8088` | API port |
| `USE_HTTPS` | No | `false` | Set `true` if your PBX uses HTTPS |
| `OUTPUT_FOLDER` | No | `./recordings/data` | Local path to save downloaded files |
| `START_TIME` / `END_TIME` | No | _(all dates)_ | Date range, PBX local time, `YYYY-MM-DD HH:MM:SS` |
| `PAGE_SIZE` | No | `100` | Records fetched per API page |
| `MAX_WORKERS` | No | `5` | Parallel download threads (keep ≤ 8) |
| `VERIFY_SSL` | No | `false` | Set `true` only if the PBX has a valid SSL certificate |
| `FAST_SEARCH` | No | `true` | Stop paginating once results predate `START_TIME` — see below |

**`MAX_WORKERS`:** the Yeastar API allows max **8 access tokens per application** — keep it ≤ 5. If recordings are `.mp3`, set `MAX_WORKERS=1` (the PBX allows only one concurrent `.mp3` download).

**`FAST_SEARCH`:** the PBX ignores `start_time` / `end_time` on `recording/search` and always reports the whole database, so the window is applied locally. It does honour the descending sort, though — so once an entire page predates `START_TIME`, every later page is older still and pagination can stop. A recent day then costs a handful of pages instead of ~250, which matters a great deal over a RAS tunnel that occasionally resets.

The descending order is re-checked on every record: if the PBX ever stops honouring it, the early stop is abandoned and the exhaustive scan resumes, so a broken sort cannot silently truncate the result. Set `FAST_SEARCH=false` to force the full scan.

---

## Usage — download

```bash
python user_recordings.py
```

Recordings are saved under `OUTPUT_FOLDER`, organised into per-day folders (`YYYYMMDD/`), keeping the **PBX native filename** so local copies match the PBX — and any mirror such as Drive — exactly:

```
YYYYMMDDHHMMSS-{internal call id}-{caller}-{callee}-{direction}.wav
```

for example `20260914081623-1788000000.1234-77XXXXXXX-1000-Inbound.wav`. Characters Windows forbids in a filename (`* ? " < > |`) are stripped.

Because the name is the PBX's own, re-running a window recognises what is already on disk and skips it, so a run can safely be repeated to pick up files a previous one failed on.

A `manifest.json` of all matched recordings is written before downloads begin (useful for auditing even if some downloads fail).

---

## Usage — duplicate cleanup (`dedupe_recordings.py`)

The PBX exposes one record per **call leg** (queue entry, agent answer, transfer), and several legs can reference the **same physical audio file** — so a folder downloaded before de-dup existed may contain byte-identical duplicates.

```bash
python dedupe_recordings.py            # dry-run: lists what would be removed
python dedupe_recordings.py --apply    # moves duplicates to _doublons_quarantine/
```

It groups files by **identical content AND same call** (same caller + near-identical timestamp), keeps the **agent/extension** leg over the `Queue` leg, and **never hard-deletes** — duplicates are moved to a reversible `_doublons_quarantine/` folder. Empty / 0-byte recordings are left untouched.

*New downloads via `user_recordings.py` are already de-duplicated on the fly, so this tool is mainly for cleaning previously-downloaded data.*

---

## How it works

```
POST /openapi/v1.0/get_token          → obtain access token (auto-refreshed)
GET  /openapi/v1.0/recording/search   → search (caller and callee, or all)
     → merge + de-duplicate (by id, then by physical file)
GET  /openapi/v1.0/recording/download → get a 30-min temporary URL per recording
GET  {download_resource_url}          → stream the file to disk
```

Downloads are parallelised with `ThreadPoolExecutor`; a thread-safe `TokenManager` refreshes the token ~60 s before expiry, so long runs keep working without manual intervention.

---

## Limitations & notes

- The Yeastar `caller` / `callee` filters use **exact matching**. A number may be stored both with and without its country code (e.g. `77XXXXXXX` and `22177XXXXXXX`) — search each format if needed.
- `.mp3` recordings don't support concurrent downloads on the PBX side — set `MAX_WORKERS=1`.
- The PBX has a **limited retention** (older audio is eventually purged), so download regularly to avoid losing recordings.

---

## API Reference

- [Yeastar P-Series Appliance — Developer Guide](https://help.yeastar.com/en/p-series-appliance-edition/developer-guide/about-this-guide.html)
- [Get Access Token](https://help.yeastar.com/en/p-series-appliance-edition/developer-guide/get-access-token.html)
- [Search Specific Recording](https://help.yeastar.com/en/p-series-appliance-edition/developer-guide/search-specific-recording.html)
- [Download a Recording File](https://help.yeastar.com/en/p-series-appliance-edition/developer-guide/download-a-recording-file.html)
