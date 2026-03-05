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

import aiohttp
import ijson

# ── Constants ─────────────────────────────────────────────────────────────────

MAX_ATTEMPTS = 6
BASE_DELAY = 1.0

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
        metavar="YYYY-MM-DD",
        help="Only delete files created before this date",
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

    if args.before:
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
    log_dir = Path(f"deletion-logs-{today}")
    log_dir.mkdir(exist_ok=True)

    handler = logging.handlers.RotatingFileHandler(
        log_dir / "log-1.log",
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
) -> bool:
    """DELETE a single file. Returns True on 200, False on permanent failure."""
    encoded_name = urllib.parse.quote(item_name, safe="")
    url = f"{base}{item_path}{encoded_name}"
    resp = await _request_with_retry(session, "DELETE", url)
    if resp is None:
        return False
    ok = resp.status == 200
    await resp.release()
    return ok


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
            success = await delete_file(session, item_path, item_name, base)
        if success:
            counters["deleted"] += 1
            logger.info(f"[DELETED]  {display}")
        else:
            counters["errors"] += 1
            logger.error(f"[ERROR]    {display}")
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

                try:
                    async for item in list_files(session, dir_path, base):
                        if shutdown_event.is_set():
                            break

                        if item["IsDirectory"]:
                            if args.recursive:
                                subdir = item["Path"] + item["ObjectName"] + "/"
                                await queue.put(subdir)
                            continue

                        file_key = _file_key(item, STORAGE_ZONE)

                        if _is_exception(file_key, exc_exact, exc_dirs):
                            counters["skipped"] += 1
                            counters["total"] += 1
                            logger.info(f"[SKIPPED]  {file_key} (exception)")
                            if counters["total"] % args.progress_every == 0:
                                _print_progress(counters, start)
                            continue

                        if not _passes_date_filter(
                            item, args.since_date, args.before_date
                        ):
                            counters["skipped"] += 1
                            counters["total"] += 1
                            logger.info(f"[SKIPPED]  {file_key} (date filter)")
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

                except RuntimeError as exc:
                    logger.error(f"[ERROR]    Failed to list {dir_path}: {exc}")
                    counters["errors"] += 1
                    counters["total"] += 1

            if shutdown_event.is_set():
                print("\n[INTERRUPTED] Waiting for in-flight deletes to finish...")

            if tasks:
                await asyncio.gather(*list(tasks), return_exceptions=True)

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
