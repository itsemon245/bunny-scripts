#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "aiohttp>=3.9",
#   "ijson>=3.2",
#   "rich>=13.7",
# ]
# ///

"""
Compute the total size of a directory in a Bunny CDN storage zone.

Bunny CDN does not expose a dedicated "directory size" endpoint, so this
script lists the directory (recursively by default) via the Storage API
and sums the `Length` field of every file.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import aiohttp
import ijson
from rich.console import Console
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)


# ── .env loader ───────────────────────────────────────────────────────────────

def _load_dotenv(path: str = ".env") -> None:
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
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            os.environ.setdefault(key, value)


_load_dotenv()


# ── Environment ───────────────────────────────────────────────────────────────

STORAGE_ZONE: str = os.environ.get("BUNNYCDN_STORAGE_ZONE", "")
API_KEY: str = os.environ.get("BUNNYCDN_API_KEY", "")
REGION: str = os.environ.get("BUNNYCDN_REGION", "ny")


def get_base_url() -> str:
    if REGION == "de":
        return "https://storage.bunnycdn.com"
    return f"https://{REGION}.storage.bunnycdn.com"


def check_env() -> None:
    errors: list[str] = []
    if not STORAGE_ZONE:
        errors.append("ERROR: Storage zone not set. Use --storage-zone or set BUNNYCDN_STORAGE_ZONE.")
    if not API_KEY:
        errors.append("ERROR: API key not set. Use --api-key or set BUNNYCDN_API_KEY.")
    if errors:
        print("\n".join(errors), file=sys.stderr)
        sys.exit(1)


# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Print the total size of a directory in a Bunny CDN storage zone.",
    )
    parser.add_argument(
        "-d", "--directory",
        required=True,
        metavar="PATH",
        help="Target directory path inside the storage zone (required). Use '/' for the zone root.",
    )
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Only sum files directly in the directory (do not recurse).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        metavar="N",
        help="Max concurrent directory listings (default: 6). "
             "Bunny's storage API is rate-limited, so keep this modest.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        metavar="N",
        help="Retries per listing on 429/5xx/transport errors (default: 4).",
    )

    creds = parser.add_argument_group("credentials (override environment variables)")
    creds.add_argument("--storage-zone", default=None, metavar="NAME")
    creds.add_argument("--api-key", default=None, metavar="KEY")
    creds.add_argument("--region", default=None, metavar="REGION",
                       help="ny, la, sg, syd, or de")

    args = parser.parse_args()
    args.directory = args.directory.strip("/")
    if args.directory:
        args.directory += "/"
    return args


# ── Human-readable formatting ─────────────────────────────────────────────────

def _human(n: int) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    size = float(n)
    for u in units:
        if size < 1024 or u == units[-1]:
            return f"{size:.2f} {u}"
        size /= 1024
    return f"{n} B"


# ── API helpers ───────────────────────────────────────────────────────────────

RETRY_STATUSES = {429, 500, 502, 503, 504}


def _backoff(attempt: int) -> float:
    """Exponential backoff: 0.5, 1, 2, 4, 8 ... capped at 15s."""
    return min(15.0, 0.5 * (2 ** attempt))


def _retry_after(resp: aiohttp.ClientResponse, attempt: int) -> float:
    """Honor the server's Retry-After header; fall back to exponential backoff."""
    ra = resp.headers.get("Retry-After")
    if ra is not None:
        try:
            return max(0.0, float(ra))
        except ValueError:
            pass
    return _backoff(attempt)


async def list_dir(
    session: aiohttp.ClientSession,
    dir_path: str,
    base: str,
    max_retries: int,
):
    """Yield items in a directory; retry the initial GET on 429/5xx/transport errors.

    Retries only happen before any items have been yielded — once streaming
    begins, a mid-stream failure surfaces as RuntimeError to the caller.
    """
    url = f"{base}{dir_path}"
    for attempt in range(max_retries + 1):
        delay: float | None = None
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    async for item in ijson.items_async(resp.content, "item"):
                        yield item
                    return
                if resp.status in RETRY_STATUSES and attempt < max_retries:
                    delay = _retry_after(resp, attempt)
                else:
                    body = (await resp.text())[:300].strip()
                    raise RuntimeError(
                        f"HTTP {resp.status} for GET {url}: {body}"
                    )
        except aiohttp.ClientError as exc:
            if attempt >= max_retries:
                raise RuntimeError(f"client error on GET {url}: {exc}") from exc
            delay = _backoff(attempt)
        if delay is not None:
            await asyncio.sleep(delay)


async def walk_dir(
    session: aiohttp.ClientSession,
    dir_path: str,
    base: str,
    recursive: bool,
    sem: asyncio.Semaphore,
    totals: dict[str, int],
    console: Console,
    max_retries: int,
) -> None:
    """Recursively walk dir_path and accumulate file count and total bytes."""
    subdirs: list[str] = []
    async with sem:
        try:
            async for item in list_dir(session, dir_path, base, max_retries):
                if item.get("IsDirectory"):
                    if recursive:
                        subdirs.append(item["Path"] + item["ObjectName"] + "/")
                    continue
                totals["files"] += 1
                totals["bytes"] += int(item.get("Length") or 0)
            totals["dirs"] += 1
        except RuntimeError as exc:
            console.print(f"[yellow]WARNING:[/] {exc}")
            totals["errors"] += 1
            return

    if subdirs:
        await asyncio.gather(*(
            walk_dir(session, sd, base, recursive, sem, totals, console, max_retries)
            for sd in subdirs
        ))


# ── Main ──────────────────────────────────────────────────────────────────────

async def _refresh_progress(
    progress: Progress, task_id, totals: dict[str, int]
) -> None:
    """Periodically push updated stats into the progress bar."""
    try:
        while True:
            progress.update(
                task_id,
                dirs=totals["dirs"],
                files=totals["files"],
                size=_human(totals["bytes"]),
            )
            await asyncio.sleep(0.2)
    except asyncio.CancelledError:
        # Final sync before exit.
        progress.update(
            task_id,
            dirs=totals["dirs"],
            files=totals["files"],
            size=_human(totals["bytes"]),
        )


async def run(args: argparse.Namespace) -> None:
    base = get_base_url()
    root = f"/{STORAGE_ZONE}/{args.directory}"
    totals: dict[str, int] = {"files": 0, "bytes": 0, "errors": 0, "dirs": 0}
    sem = asyncio.Semaphore(args.workers)

    console = Console()
    interrupted = False

    connector = aiohttp.TCPConnector(limit=args.workers + 5)
    timeout = aiohttp.ClientTimeout(connect=30, sock_read=120)

    progress = Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold blue]{task.description}"),
        TextColumn(
            "[cyan]dirs[/] {task.fields[dirs]}  "
            "[green]files[/] {task.fields[files]}  "
            "[magenta]size[/] {task.fields[size]}"
        ),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    )

    async with aiohttp.ClientSession(
        connector=connector,
        headers={"AccessKey": API_KEY},
        timeout=timeout,
    ) as session:
        console.print(f"[bold cyan]Scanning[/] {root} ...")
        with progress:
            task_id = progress.add_task(
                "Walking", total=None, dirs=0, files=0, size="0 B"
            )
            refresher = asyncio.create_task(
                _refresh_progress(progress, task_id, totals)
            )
            walker = asyncio.create_task(
                walk_dir(
                    session,
                    root,
                    base,
                    not args.no_recursive,
                    sem,
                    totals,
                    console,
                    args.retries,
                )
            )
            try:
                await walker
            except (asyncio.CancelledError, KeyboardInterrupt):
                interrupted = True
                walker.cancel()
                # Wait for the walker (and any subtasks gather created) to finish unwinding.
                try:
                    await walker
                except (asyncio.CancelledError, Exception):
                    pass
            finally:
                refresher.cancel()
                try:
                    await refresher
                except asyncio.CancelledError:
                    pass

    mode = "recursive" if not args.no_recursive else "top-level only"
    console.print()
    if interrupted:
        console.print("[yellow]Interrupted — reporting partial totals.[/]")
    console.print(f"Directory : {root}  ({mode})")
    console.print(f"Dirs      : {totals['dirs']:,}")
    console.print(f"Files     : {totals['files']:,}")
    console.print(
        f"Total size: {totals['bytes']:,} bytes  ({_human(totals['bytes'])})"
    )
    if totals["errors"]:
        console.print(
            f"[red]Errors    :[/] {totals['errors']} listing failure(s) — "
            "total may be incomplete"
        )
        if not interrupted:
            sys.exit(2)
    if interrupted:
        sys.exit(130)


if __name__ == "__main__":
    args = parse_args()
    if args.storage_zone:
        STORAGE_ZONE = args.storage_zone
    if args.api_key:
        API_KEY = args.api_key
    if args.region:
        REGION = args.region
    check_env()
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        # Fallback if Ctrl+C lands outside run()'s handler (e.g. during shutdown).
        sys.exit(130)
