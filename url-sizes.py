#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "aiohttp>=3.9",
#   "rich>=13.7",
# ]
# ///

"""
Compute size statistics for a list of URLs supplied via a CSV file.

For every URL the script issues a HEAD request (falling back to a ranged
GET if the server doesn't return Content-Length on HEAD) and collects the
reported byte size. It then prints the total size, average size, and the
5 largest, 5 smallest, and 5 closest-to-average URLs.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import statistics
import sys
from pathlib import Path

import aiohttp
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)


# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute size statistics for URLs listed in a CSV file.",
    )
    parser.add_argument(
        "csv_file",
        metavar="CSV",
        help="Path to the CSV file containing the URLs.",
    )
    parser.add_argument(
        "column",
        nargs="?",
        default="1",
        metavar="COLUMN",
        help="Column to read URLs from: 1-based index or column header name (default: 1).",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Treat the first CSV row as data rather than a header row.",
    )
    parser.add_argument(
        "-N", "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Only probe the first N URLs (useful for smoke-testing).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=6,
        metavar="N",
        help="Max concurrent HTTP requests (default: 6). Bunny rate-limits the "
             "storage API, so keep this modest.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="Per-request timeout in seconds (default: 30).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        metavar="N",
        help="How many times to retry a request after 429/5xx (default: 4).",
    )
    return parser.parse_args()


# ── CSV reading ───────────────────────────────────────────────────────────────

def resolve_column_index(header: list[str] | None, column: str) -> int:
    """Resolve a column spec (1-based index or header name) to a 0-based index."""
    if column.isdigit():
        idx = int(column) - 1
        if idx < 0:
            print(f"ERROR: column index must be >= 1 (got {column}).", file=sys.stderr)
            sys.exit(1)
        return idx
    if header is None:
        print(
            f"ERROR: column name '{column}' was supplied but --no-header is set.",
            file=sys.stderr,
        )
        sys.exit(1)
    try:
        return header.index(column)
    except ValueError:
        print(
            f"ERROR: column '{column}' not found in header {header!r}.",
            file=sys.stderr,
        )
        sys.exit(1)


def read_urls(csv_path: Path, column: str, has_header: bool) -> list[str]:
    if not csv_path.exists():
        print(f"ERROR: file not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    with csv_path.open(newline="") as fh:
        reader = csv.reader(fh)
        header: list[str] | None = None
        if has_header:
            try:
                header = next(reader)
            except StopIteration:
                return []
        col_idx = resolve_column_index(header, column)

        urls: list[str] = []
        for row_num, row in enumerate(reader, start=2 if has_header else 1):
            if not row:
                continue
            if col_idx >= len(row):
                print(
                    f"  WARNING: row {row_num} has only {len(row)} columns; skipping.",
                    file=sys.stderr,
                )
                continue
            url = row[col_idx].strip()
            if url:
                urls.append(url)
    return urls


# ── Human-readable formatting ─────────────────────────────────────────────────

def _human(n: float) -> str:
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    size = float(n)
    for u in units:
        if size < 1024 or u == units[-1]:
            return f"{size:.2f} {u}"
        size /= 1024
    return f"{n} B"


# ── HTTP size probing ─────────────────────────────────────────────────────────

RETRY_STATUSES = {429, 500, 502, 503, 504}


def _retry_delay(resp: aiohttp.ClientResponse, attempt: int) -> float:
    """Honor Retry-After if present; otherwise exponential backoff with cap."""
    ra = resp.headers.get("Retry-After")
    if ra is not None:
        try:
            return max(0.0, float(ra))
        except ValueError:
            pass
    # 0.5, 1, 2, 4, 8 ... capped at 15s
    return min(15.0, 0.5 * (2 ** attempt))


async def _attempt(
    session: aiohttp.ClientSession, url: str
) -> tuple[int | None, str | None, bool, float | None]:
    """One probe attempt. Returns (size, error, retryable, retry_after_seconds)."""
    try:
        async with session.head(url, allow_redirects=True) as resp:
            if resp.status in RETRY_STATUSES:
                return None, f"HTTP {resp.status}", True, _retry_delay(resp, 0)
            if resp.status >= 400:
                # 4xx (not 429) are not going to change on retry.
                return None, f"HTTP {resp.status} on HEAD", False, None
            length = resp.headers.get("Content-Length")
            if length is not None:
                return int(length), None, False, None

        async with session.get(
            url, headers={"Range": "bytes=0-0"}, allow_redirects=True
        ) as resp:
            if resp.status in RETRY_STATUSES:
                return None, f"HTTP {resp.status}", True, _retry_delay(resp, 0)
            if resp.status not in (200, 206):
                return None, f"HTTP {resp.status} on ranged GET", False, None
            cr = resp.headers.get("Content-Range")
            if cr and "/" in cr:
                tail = cr.rsplit("/", 1)[1]
                if tail.isdigit():
                    return int(tail), None, False, None
            length = resp.headers.get("Content-Length")
            if length is not None and resp.status == 200:
                return int(length), None, False, None
            return None, "no size headers returned", False, None
    except asyncio.TimeoutError:
        return None, "timeout", True, None
    except aiohttp.ClientError as exc:
        return None, f"client error: {exc}", True, None


async def probe_size(
    session: aiohttp.ClientSession,
    url: str,
    sem: asyncio.Semaphore,
    max_retries: int,
) -> tuple[str, int | None, str | None]:
    """Return (url, size_in_bytes_or_None, error_message_or_None).

    Retry policy:
      - Retryable failures (429, 5xx, timeout, ClientError) — up to max_retries.
      - Non-retryable failures (4xx other than 429, missing size headers) —
        still given one extra attempt, then surfaced.
    """
    async with sem:
        last_err: str | None = None
        attempt = 0
        # At least 1 retry (= 2 attempts) for every failure mode.
        hard_cap = max(max_retries, 1)
        while True:
            size, error, retryable, retry_after = await _attempt(session, url)
            if size is not None:
                return url, size, None
            last_err = error
            if attempt >= hard_cap:
                break
            # Stop retrying non-retryable failures after the first redo.
            if not retryable and attempt >= 1:
                break
            delay = retry_after if retry_after is not None else min(
                15.0, 0.5 * (2 ** attempt)
            )
            await asyncio.sleep(delay)
            attempt += 1
        return url, None, f"{last_err} after {attempt + 1} attempts"


# ── Main ──────────────────────────────────────────────────────────────────────

async def run(args: argparse.Namespace) -> None:
    urls = read_urls(Path(args.csv_file), args.column, has_header=not args.no_header)
    if not urls:
        print("No URLs found in the CSV.", file=sys.stderr)
        sys.exit(1)
    if args.limit is not None and args.limit > 0:
        urls = urls[: args.limit]

    console = Console()
    console.print(
        f"[bold cyan]Probing[/] {len(urls):,} URL(s) "
        f"with [bold]{args.workers}[/] workers "
        f"(retries up to [bold]{args.retries}[/])"
    )

    sem = asyncio.Semaphore(args.workers)
    connector = aiohttp.TCPConnector(limit=args.workers + 5)
    timeout = aiohttp.ClientTimeout(total=args.timeout)

    results: list[tuple[str, int | None, str | None]] = []
    tasks: list[asyncio.Task] = []

    progress = Progress(
        SpinnerColumn(style="cyan"),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=None, complete_style="green", finished_style="green"),
        MofNCompleteColumn(),
        TextColumn("•"),
        TextColumn("[green]✓ {task.fields[ok]}[/]  [red]✗ {task.fields[err]}[/]"),
        TextColumn("•"),
        TimeElapsedColumn(),
        TextColumn("elapsed •"),
        TimeRemainingColumn(),
        TextColumn("left"),
        console=console,
        transient=False,
    )

    interrupted = False

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            asyncio.create_task(probe_size(session, url, sem, args.retries))
            for url in urls
        ]
        ok = err = 0
        with progress:
            task_id = progress.add_task(
                "Sizing", total=len(tasks), ok=0, err=0
            )
            try:
                for coro in asyncio.as_completed(tasks):
                    url, size, error = await coro
                    results.append((url, size, error))
                    if size is not None:
                        ok += 1
                    else:
                        err += 1
                    progress.update(task_id, advance=1, ok=ok, err=err)
            except (asyncio.CancelledError, KeyboardInterrupt):
                interrupted = True
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    if interrupted:
        console.print(
            f"[yellow]Interrupted — reporting on "
            f"{len(results):,}/{len(urls):,} URL(s) probed so far.[/]"
        )

    successes: list[tuple[str, int]] = [
        (u, s) for u, s, e in results if s is not None and e is None
    ]
    failures: list[tuple[str, str]] = [
        (u, e) for u, s, e in results if s is None and e is not None
    ]

    if not successes:
        print("No URLs returned a usable size.", file=sys.stderr)
        for url, err in failures[:10]:
            print(f"  {err}: {url}", file=sys.stderr)
        sys.exit(2)

    sizes = [s for _, s in successes]
    total_bytes = sum(sizes)
    avg = total_bytes / len(sizes)
    median = statistics.median(sizes)

    successes_sorted = sorted(successes, key=lambda x: x[1])
    smallest = successes_sorted[:5]
    largest = list(reversed(successes_sorted[-5:]))
    closest_to_avg = sorted(successes, key=lambda x: abs(x[1] - avg))[:5]

    print()
    print(f"URLs processed : {len(urls):,}")
    print(f"URLs sized OK  : {len(successes):,}")
    print(f"URLs failed    : {len(failures):,}")
    print(f"Total size     : {total_bytes:,} bytes  ({_human(total_bytes)})")
    print(f"Average size   : {avg:,.0f} bytes  ({_human(avg)})")
    print(f"Median size    : {median:,.0f} bytes  ({_human(median)})")

    def _print_group(title: str, items: list[tuple[str, int]]) -> None:
        print()
        print(title)
        print("-" * len(title))
        for url, size in items:
            print(f"  {_human(size):>12}  {url}")

    _print_group("Top 5 largest", largest)
    _print_group("Top 5 smallest", smallest)
    _print_group("Top 5 closest to average", closest_to_avg)

    if failures:
        print()
        print(f"Failures ({len(failures)}):", file=sys.stderr)
        for url, err in failures[:20]:
            print(f"  {err}: {url}", file=sys.stderr)
        if len(failures) > 20:
            print(f"  ... and {len(failures) - 20} more", file=sys.stderr)


if __name__ == "__main__":
    try:
        asyncio.run(run(parse_args()))
    except KeyboardInterrupt:
        # A second Ctrl+C during reporting falls through here.
        sys.exit(130)
