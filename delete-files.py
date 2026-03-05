#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "aiohttp>=3.9",
#   "ijson>=3.2",
# ]
# ///

from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
import logging.handlers
import os
import random
import signal
import sys
import urllib.parse
from pathlib import Path

import re

import aiohttp
import ijson

# ── Constants ─────────────────────────────────────────────────────────────────

MAX_ATTEMPTS = 6
BASE_DELAY = 1.0

# Matches YYYY-MM-DD date segments inside storage paths
_DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")

# ── .env loader ───────────────────────────────────────────────────────────────

def _load_dotenv(path: str = ".env") -> None:
    """
    Load key=value pairs from a .env file into os.environ.
    Uses setdefault so real shell environment variables always take precedence.
    Silently skips if the file does not exist.
    """
    env_path = Path(path)
    if not env_path.exists():
        return
    with env_path.open() as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # Strip optional surrounding quotes
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            os.environ.setdefault(key, value)


_load_dotenv()

# ── Environment ───────────────────────────────────────────────────────────────

STORAGE_ZONE: str = os.environ.get("BUNNYCDN_STORAGE_ZONE", "")
API_KEY: str = os.environ.get("BUNNYCDN_API_KEY", "")
REGION: str = os.environ.get("BUNNYCDN_REGION", "ny")


def check_env() -> None:
    errors: list[str] = []
    if not STORAGE_ZONE:
        errors.append(
            "ERROR: Storage zone not set. "
            "Use --storage-zone or set BUNNYCDN_STORAGE_ZONE."
        )
    if not API_KEY:
        errors.append(
            "ERROR: API key not set. "
            "Use --api-key or set BUNNYCDN_API_KEY."
        )
    if errors:
        print("\n".join(errors), file=sys.stderr)
        sys.exit(1)


def get_base_url() -> str:
    if REGION == "de":
        return "https://storage.bunnycdn.com"
    return f"https://{REGION}.storage.bunnycdn.com"


# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete files from a Bunny CDN storage zone.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  ./delete-files.py -d images/ -r --before 2024-01-01\n"
            "  ./delete-files.py -d images/ -r --since 2023-01-01 --before 2024-01-01\n"
        ),
    )
    parser.add_argument(
        "-d", "--directory",
        required=True,
        metavar="PATH",
        help="Target directory path inside the storage zone (required)",
    )
    parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="Recurse into sub-directories",
    )
    parser.add_argument(
        "--since",
        metavar="YYYY-MM-DD",
        help="Only delete files created on or after this date",
    )
    parser.add_argument(
        "--before",
        required=True,
        metavar="YYYY-MM-DD",
        help="Only delete files created before this date (required)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=20,
        metavar="N",
        help="Max concurrent delete workers (default: 20)",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=20,
        metavar="N",
        dest="progress_every",
        help="Refresh progress counter every N completed operations (default: 20)",
    )

    # Credential overrides — take priority over environment variables
    creds = parser.add_argument_group("credentials (override environment variables)")
    creds.add_argument(
        "--storage-zone",
        default=None,
        metavar="NAME",
        help="Storage zone name (overrides BUNNYCDN_STORAGE_ZONE)",
    )
    creds.add_argument(
        "--api-key",
        default=None,
        metavar="KEY",
        help="API access key (overrides BUNNYCDN_API_KEY)",
    )
    creds.add_argument(
        "--region",
        default=None,
        metavar="REGION",
        help="Region prefix: ny, la, sg, syd, de (overrides BUNNYCDN_REGION, default: ny)",
    )

    args = parser.parse_args()

    # Normalize directory — strip surrounding slashes, re-add trailing slash
    args.directory = args.directory.strip("/") + "/"

    # Validate and parse dates
    args.since_date: datetime.date | None = None
    args.before_date: datetime.date | None = None

    if args.since:
        try:
            args.since_date = datetime.date.fromisoformat(args.since)
        except ValueError:
            parser.error(f"Invalid --since value '{args.since}'. Expected YYYY-MM-DD.")

    try:
        args.before_date = datetime.date.fromisoformat(args.before)
    except ValueError:
        parser.error(f"Invalid --before value '{args.before}'. Expected YYYY-MM-DD.")

    if args.since_date and args.before_date and args.since_date > args.before_date:
        parser.error(
            f"--since ({args.since}) must not be later than --before ({args.before})."
        )

    return args


# ── Logging ───────────────────────────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    today = datetime.date.today().isoformat()
    log_dir = Path("logs") / "delete-files"
    log_dir.mkdir(parents=True, exist_ok=True)

    handler = logging.handlers.RotatingFileHandler(
        log_dir / f"log-{today}.log",
        maxBytes=100 * 1024 * 1024,  # 100 MB per file
        backupCount=1,
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s  %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
    )

    logger = logging.getLogger("bunny_delete")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# ── Exception list ────────────────────────────────────────────────────────────

# Holds exact file paths and directory prefixes separately for efficient lookup.
ExceptionSets = tuple[set[str], set[str]]  # (exact_paths, dir_prefixes)


def load_exceptions() -> ExceptionSets:
    """
    Globs all exception.list* files in the current directory and merges them.
    Entries ending with '/' are treated as directory prefixes — every file
    under that directory will be skipped.
    """
    exc_files = sorted(Path(".").glob("exception.list*"))

    if not exc_files:
        print(
            "\nWARNING: No exception.list* files found.\n"
            "Without them, static assets required by the application "
            "may be permanently deleted.\n"
        )
        try:
            answer = input("Continue anyway? [y/N]: ").strip()
        except EOFError:
            answer = ""
        if answer.lower() != "y":
            print("Aborted.")
            sys.exit(0)
        return set(), set()

    exact: set[str] = set()
    dirs: set[str] = set()

    for exc_file in exc_files:
        if exc_file.stat().st_size == 0:
            continue
        print(f"  Loading exceptions from {exc_file} ...", flush=True)
        with exc_file.open() as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Normalize to unencoded form so both
                # "Text%20Background%20Page" and "Text Background Page"
                # match the raw ObjectName/Path values returned by the API.
                path = urllib.parse.unquote(urllib.parse.urlparse(line).path)
                if path.endswith("/"):
                    dirs.add(path)
                else:
                    exact.add(path)

    total = len(exact) + len(dirs)
    print(
        f"  Loaded {len(exact)} exact path(s) and {len(dirs)} directory "
        f"prefix(es) from {len(exc_files)} file(s). ({total} total exceptions)\n",
        flush=True,
    )
    return exact, dirs


def _is_exception(file_key: str, exact: set[str], dirs: set[str]) -> bool:
    """Return True if file_key is an exact exception or falls under an excepted directory."""
    if file_key in exact:
        return True
    return any(file_key.startswith(d) for d in dirs)


def _exception_reason(file_key: str, exact: set[str], dirs: set[str]) -> str | None:
    """Return a human-readable skip reason if file_key is an exception, else None."""
    if file_key in exact:
        return "exception list (exact match)"
    for d in dirs:
        if file_key.startswith(d):
            return f"exception list (protected dir: {d})"
    return None


# ── Progress helpers ──────────────────────────────────────────────────────────

def _elapsed(start: datetime.datetime) -> str:
    secs = int((datetime.datetime.now() - start).total_seconds())
    return f"{secs // 3600}h{(secs % 3600) // 60:02d}m{secs % 60:02d}s"


def _print_progress(counters: dict[str, int], start: datetime.datetime) -> None:
    print(
        f"\rDeleted: {counters['deleted']} | "
        f"Skipped: {counters['skipped']} | "
        f"Errors: {counters['errors']} | "
        f"Elapsed: {_elapsed(start)}   ",
        end="",
        flush=True,
    )


def _print_summary(
    counters: dict[str, int],
    start: datetime.datetime,
    logger: logging.Logger,
) -> None:
    elapsed = _elapsed(start)
    msg = (
        f"Deleted: {counters['deleted']} | "
        f"Skipped: {counters['skipped']} | "
        f"Errors: {counters['errors']} | "
        f"Elapsed: {elapsed}"
    )
    print(f"\n[DONE]  {msg}")
    logger.info(f"[SUMMARY]  {msg}")


# ── Filters ───────────────────────────────────────────────────────────────────

def _file_key(item: dict, zone: str) -> str:
    """Return the path without the /{zone} prefix for exception-list matching."""
    full = item["Path"] + item["ObjectName"]
    prefix = f"/{zone}"
    return full[len(prefix):] if full.startswith(prefix) else full


def _passes_date_filter(
    item: dict,
    since: datetime.date | None,
    before: datetime.date | None,
) -> bool:
    if not since and not before:
        return True
    try:
        file_date = datetime.date.fromisoformat(item.get("DateCreated", "")[:10])
    except ValueError:
        return False
    if since and file_date < since:
        return False
    if before and file_date >= before:
        return False
    return True


def _date_skip_reason(
    item: dict,
    since: datetime.date | None,
    before: datetime.date | None,
) -> str | None:
    """Return a human-readable skip reason if the item fails the date filter, else None."""
    if not since and not before:
        return None
    try:
        file_date = datetime.date.fromisoformat(item.get("DateCreated", "")[:10])
    except ValueError:
        return "date filter (DateCreated missing or unparseable)"
    if since and file_date < since:
        return f"too old: created at {file_date},  (--since cutoff is {since})"
    if before and file_date >= before:
        return f"too recent: created at {file_date}, (--before cutoff is {before})"
    return None


def _dir_key(dir_path: str, zone: str) -> str:
    """Strip the /{zone} prefix from a queue path to get the exception-comparable key."""
    prefix = f"/{zone}"
    return dir_path[len(prefix):] if dir_path.startswith(prefix) else dir_path


def _extract_path_date(path_key: str) -> datetime.date | None:
    """
    Return the rightmost YYYY-MM-DD date found in a storage path, or None.
    The rightmost date is the most specific (deepest directory level).
    """
    matches = list(_DATE_RE.finditer(path_key))
    if not matches:
        return None
    try:
        return datetime.date.fromisoformat(matches[-1].group())
    except ValueError:
        return None


def _any_exception_under(dir_key: str, exc_exact: set[str], exc_dirs: set[str]) -> bool:
    """
    Return True if any exception entry falls within or overlaps with dir_key.
    Covers three cases:
      - An exact exception file lives inside this directory
      - An exception directory is nested inside this directory
      - This directory is nested inside an exception directory (it is itself protected)
    """
    for exc in exc_exact:
        if exc.startswith(dir_key):
            return True
    for exc_dir in exc_dirs:
        if exc_dir.startswith(dir_key) or dir_key.startswith(exc_dir):
            return True
    return False


# ── API helpers ───────────────────────────────────────────────────────────────

async def _request_with_retry(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    **kwargs,
) -> aiohttp.ClientResponse | None:
    """
    Non-streaming request with exponential back-off on 429/5xx.
    The caller is responsible for releasing the returned response.
    """
    last_exc: Exception | None = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            resp = await session.request(method, url, **kwargs)
            if resp.status == 429:
                await resp.release()
                await asyncio.sleep(BASE_DELAY * (2 ** attempt) + random.random())
                continue
            if resp.status >= 500:
                await resp.release()
                await asyncio.sleep(BASE_DELAY + random.random())
                continue
            return resp
        except aiohttp.ClientError as exc:
            last_exc = exc
            await asyncio.sleep(BASE_DELAY * (2 ** attempt) + random.random())
    return None


async def list_files(
    session: aiohttp.ClientSession,
    dir_path: str,
    base: str,
):
    """
    Async generator that streams one storage-object dict at a time via ijson.
    dir_path must include the storage zone prefix, e.g. /myzone/images/.
    Raises RuntimeError after all retries are exhausted.
    """
    url = f"{base}{dir_path}"
    last_exc: Exception | None = None

    for attempt in range(MAX_ATTEMPTS):
        streaming_started = False
        try:
            async with session.get(url) as resp:
                if resp.status == 429:
                    await asyncio.sleep(BASE_DELAY * (2 ** attempt) + random.random())
                    continue
                if resp.status >= 500:
                    await asyncio.sleep(BASE_DELAY + random.random())
                    continue
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status} for GET {url}")
                streaming_started = True
                async for item in ijson.items_async(resp.content, "item"):
                    yield item
                return
        except RuntimeError:
            raise
        except Exception as exc:
            if streaming_started:
                # Don't retry after partial yield — items would be duplicated
                raise RuntimeError(
                    f"Stream interrupted mid-listing for {dir_path}: {exc}"
                ) from exc
            last_exc = exc
            if attempt < MAX_ATTEMPTS - 1:
                await asyncio.sleep(BASE_DELAY * (2 ** attempt) + random.random())

    raise RuntimeError(
        f"Failed to list {dir_path} after {MAX_ATTEMPTS} attempts"
        + (f": {last_exc}" if last_exc else "")
    )


async def delete_file(
    session: aiohttp.ClientSession,
    item_path: str,
    item_name: str,
    base: str,
) -> tuple[bool, str]:
    """
    DELETE a single file.
    Returns (True, "") on success, or (False, reason) on permanent failure.
    """
    encoded_name = urllib.parse.quote(item_name, safe="")
    url = f"{base}{item_path}{encoded_name}"
    resp = await _request_with_retry(session, "DELETE", url)
    if resp is None:
        return False, "no response after retries"
    if resp.status == 200:
        await resp.release()
        return True, ""
    body = (await resp.text())[:300].strip()
    detail = f"HTTP {resp.status}" + (f": {body}" if body else "")
    await resp.release()
    return False, detail


# ── Delete worker ─────────────────────────────────────────────────────────────

async def delete_worker(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    item_path: str,
    item_name: str,
    base: str,
    counters: dict[str, int],
    logger: logging.Logger,
    start: datetime.datetime,
    progress_every: int,
) -> None:
    display = item_path + item_name
    try:
        async with sem:
            success, detail = await delete_file(session, item_path, item_name, base)
        if success:
            counters["deleted"] += 1
            logger.info(f"[DELETED]  {display}")
        else:
            counters["errors"] += 1
            logger.error(f"[ERROR]    {display} ({detail})")
    except Exception as exc:
        counters["errors"] += 1
        logger.error(f"[ERROR]    {display}: {exc}")

    counters["total"] += 1
    if counters["total"] % progress_every == 0:
        _print_progress(counters, start)
        d, s, e = counters["deleted"], counters["skipped"], counters["errors"]
        logger.info(
            f"[SUMMARY]  Deleted: {d} | Skipped: {s} | "
            f"Errors: {e} | Elapsed: {_elapsed(start)}"
        )


# ── Directory bulk-delete worker ─────────────────────────────────────────────

async def delete_directory_worker(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    dir_path: str,
    base: str,
    counters: dict[str, int],
    logger: logging.Logger,
    start: datetime.datetime,
    progress_every: int,
) -> None:
    """Delete an entire directory with a single API call (Bunny deletes recursively)."""
    url = f"{base}{dir_path}"
    try:
        async with sem:
            resp = await _request_with_retry(session, "DELETE", url)
        if resp is not None and resp.status == 200:
            await resp.release()
            counters["deleted"] += 1
            logger.info(f"[DELETED DIR] {dir_path}")
        elif resp is not None and resp.status == 404:
            # Bunny returns 404 when the directory has no contents to delete.
            # This is a success — the directory is empty or already gone.
            await resp.release()
            logger.info(f"[EMPTY DIR]   {dir_path}")
        else:
            if resp is not None:
                body = (await resp.text())[:300].strip()
                detail = f"HTTP {resp.status}" + (f": {body}" if body else "")
                await resp.release()
            else:
                detail = "no response after retries"
            counters["errors"] += 1
            logger.error(f"[ERROR DIR]   {dir_path} ({detail})")
    except Exception as exc:
        counters["errors"] += 1
        logger.error(f"[ERROR DIR]   {dir_path}: {exc}")

    counters["total"] += 1
    if counters["total"] % progress_every == 0:
        _print_progress(counters, start)
        d, s, e = counters["deleted"], counters["skipped"], counters["errors"]
        logger.info(
            f"[SUMMARY]  Deleted: {d} | Skipped: {s} | "
            f"Errors: {e} | Elapsed: {_elapsed(start)}"
        )


# ── Empty-directory helpers ───────────────────────────────────────────────────

async def _is_empty_dir(
    session: aiohttp.ClientSession,
    dir_path: str,
    base: str,
) -> bool:
    """Return True if the directory currently has no items (files or subdirs)."""
    async for _ in list_files(session, dir_path, base):
        return False
    return True


async def cleanup_empty_dirs(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    visited_dirs: set[str],
    base: str,
    counters: dict[str, int],
    logger: logging.Logger,
    start: datetime.datetime,
    progress_every: int,
) -> None:
    """
    Second pass: delete any visited directories that are now empty.
    Processes deepest directories first (sorted by path depth descending)
    so that a parent is only checked after all its children have been removed.
    """
    if not visited_dirs:
        return

    sorted_dirs = sorted(visited_dirs, key=lambda p: p.count("/"), reverse=True)
    for dir_path in sorted_dirs:
        try:
            if await _is_empty_dir(session, dir_path, base):
                await delete_directory_worker(
                    session, sem, dir_path, base,
                    counters, logger, start, progress_every,
                )
        except RuntimeError:
            pass  # listing failed — leave the directory alone


# ── BFS main loop ─────────────────────────────────────────────────────────────

async def run(args: argparse.Namespace) -> None:
    logger = setup_logging()
    exc_exact, exc_dirs = load_exceptions()
    base = get_base_url()
    start = datetime.datetime.now()
    counters: dict[str, int] = {"deleted": 0, "skipped": 0, "errors": 0, "total": 0}
    sem = asyncio.Semaphore(args.workers)
    tasks: set[asyncio.Task] = set()
    shutdown_event = asyncio.Event()
    # Directories processed via normal listing (not fast-path bulk delete).
    # Used by the post-run empty-directory cleanup pass.
    visited_dirs: set[str] = set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        loop.add_signal_handler(sig, shutdown_event.set)

    # BFS queue holds full paths including zone prefix, e.g. /myzone/images/
    queue: asyncio.Queue[str] = asyncio.Queue()
    await queue.put(f"/{STORAGE_ZONE}/{args.directory}")

    connector = aiohttp.TCPConnector(limit=args.workers + 10)
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"AccessKey": API_KEY},
    ) as session:
        try:
            while not queue.empty() and not shutdown_event.is_set():
                dir_path = await queue.get()
                dk = _dir_key(dir_path, STORAGE_ZONE)

                # ── Path-date fast path ───────────────────────────────────────
                # If the directory path contains a date segment, we can decide
                # whether to skip or bulk-delete without listing at all.
                path_date = _extract_path_date(dk)
                if path_date is not None:
                    if not _passes_date_filter(
                        {"DateCreated": str(path_date)}, args.since_date, args.before_date
                    ):
                        # Entire directory is outside the date window — skip it.
                        if args.since_date and path_date < args.since_date:
                            dir_skip_reason = f"date filter: dir date {path_date}, too old (--since cutoff is {args.since_date})"
                        else:
                            dir_skip_reason = f"date filter: dir date {path_date}, too recent (--before cutoff is {args.before_date})"
                        logger.info(f"[SKIPPED DIR] {dk} ({dir_skip_reason})")
                        continue

                    if not _any_exception_under(dk, exc_exact, exc_dirs):
                        # Date passes and no exceptions inside — bulk delete.
                        task = asyncio.create_task(
                            delete_directory_worker(
                                session, sem, dir_path, base,
                                counters, logger, start, args.progress_every,
                            )
                        )
                        tasks.add(task)
                        task.add_done_callback(tasks.discard)
                        continue
                    # else: date passes but exceptions exist — fall through to listing
                # ─────────────────────────────────────────────────────────────

                visited_dirs.add(dir_path)
                had_items = False
                try:
                    async for item in list_files(session, dir_path, base):
                        if shutdown_event.is_set():
                            break

                        had_items = True

                        if item["IsDirectory"]:
                            if args.recursive:
                                subdir = item["Path"] + item["ObjectName"] + "/"
                                await queue.put(subdir)
                            continue

                        file_key = _file_key(item, STORAGE_ZONE)

                        exc_reason = _exception_reason(file_key, exc_exact, exc_dirs)
                        if exc_reason is not None:
                            counters["skipped"] += 1
                            counters["total"] += 1
                            logger.info(f"[SKIPPED]  {file_key} ({exc_reason})")
                            if counters["total"] % args.progress_every == 0:
                                _print_progress(counters, start)
                            continue

                        date_reason = _date_skip_reason(item, args.since_date, args.before_date)
                        if date_reason is not None:
                            counters["skipped"] += 1
                            counters["total"] += 1
                            logger.info(f"[SKIPPED]  {file_key} ({date_reason})")
                            if counters["total"] % args.progress_every == 0:
                                _print_progress(counters, start)
                            continue

                        task = asyncio.create_task(
                            delete_worker(
                                session, sem,
                                item["Path"], item["ObjectName"],
                                base, counters, logger, start, args.progress_every,
                            )
                        )
                        tasks.add(task)
                        task.add_done_callback(tasks.discard)

                    if not had_items and not shutdown_event.is_set():
                        # Directory was already empty — delete it immediately.
                        task = asyncio.create_task(
                            delete_directory_worker(
                                session, sem, dir_path, base,
                                counters, logger, start, args.progress_every,
                            )
                        )
                        tasks.add(task)
                        task.add_done_callback(tasks.discard)
                        visited_dirs.discard(dir_path)  # no need to re-check later

                except RuntimeError as exc:
                    logger.error(f"[ERROR]    Failed to list {dir_path}: {exc}")
                    counters["errors"] += 1
                    counters["total"] += 1

            if shutdown_event.is_set():
                print("\n[INTERRUPTED] Waiting for in-flight deletes to finish...")

            if tasks:
                await asyncio.gather(*list(tasks), return_exceptions=True)

            # Post-run cleanup: delete directories that became empty after file deletion.
            if not shutdown_event.is_set():
                await cleanup_empty_dirs(
                    session, sem, visited_dirs, base,
                    counters, logger, start, args.progress_every,
                )

        finally:
            _print_summary(counters, start, logger)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = parse_args()

    # Flag credentials take priority over environment variables
    if args.storage_zone:
        STORAGE_ZONE = args.storage_zone
    if args.api_key:
        API_KEY = args.api_key
    if args.region:
        REGION = args.region

    check_env()
    asyncio.run(run(args))
