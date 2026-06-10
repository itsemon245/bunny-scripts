#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "aiohttp>=3.9",
#   "ijson>=3.2",
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
        default=10,
        metavar="N",
        help="Max concurrent directory listings (default: 10)",
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

async def list_dir(session: aiohttp.ClientSession, dir_path: str, base: str):
    """Yield items in a directory by streaming the listing response."""
    url = f"{base}{dir_path}"
    async with session.get(url) as resp:
        if resp.status != 200:
            body = (await resp.text())[:300].strip()
            raise RuntimeError(f"HTTP {resp.status} for GET {url}: {body}")
        async for item in ijson.items_async(resp.content, "item"):
            yield item


async def walk_dir(
    session: aiohttp.ClientSession,
    dir_path: str,
    base: str,
    recursive: bool,
    sem: asyncio.Semaphore,
    totals: dict[str, int],
) -> None:
    """Recursively walk dir_path and accumulate file count and total bytes."""
    subdirs: list[str] = []
    async with sem:
        try:
            async for item in list_dir(session, dir_path, base):
                if item.get("IsDirectory"):
                    if recursive:
                        subdirs.append(item["Path"] + item["ObjectName"] + "/")
                    continue
                totals["files"] += 1
                totals["bytes"] += int(item.get("Length") or 0)
        except RuntimeError as exc:
            print(f"  WARNING: {exc}", file=sys.stderr)
            totals["errors"] += 1
            return

    if subdirs:
        await asyncio.gather(*(
            walk_dir(session, sd, base, recursive, sem, totals)
            for sd in subdirs
        ))


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(args: argparse.Namespace) -> None:
    base = get_base_url()
    root = f"/{STORAGE_ZONE}/{args.directory}"
    totals: dict[str, int] = {"files": 0, "bytes": 0, "errors": 0}
    sem = asyncio.Semaphore(args.workers)

    connector = aiohttp.TCPConnector(limit=args.workers + 5)
    timeout = aiohttp.ClientTimeout(connect=30, sock_read=120)
    async with aiohttp.ClientSession(
        connector=connector,
        headers={"AccessKey": API_KEY},
        timeout=timeout,
    ) as session:
        print(f"Scanning {root} ...", flush=True)
        await walk_dir(session, root, base, not args.no_recursive, sem, totals)

    mode = "recursive" if not args.no_recursive else "top-level only"
    print()
    print(f"Directory : {root}  ({mode})")
    print(f"Files     : {totals['files']:,}")
    print(f"Total size: {totals['bytes']:,} bytes  ({_human(totals['bytes'])})")
    if totals["errors"]:
        print(f"Errors    : {totals['errors']} listing failure(s) — total may be incomplete",
              file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    args = parse_args()
    if args.storage_zone:
        STORAGE_ZONE = args.storage_zone
    if args.api_key:
        API_KEY = args.api_key
    if args.region:
        REGION = args.region
    check_env()
    asyncio.run(run(args))
