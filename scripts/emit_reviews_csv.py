#!/usr/bin/env python3
"""Stream cleaned review rows (IDREVIEW, HOTELID, REVIEW) in Postgres COPY text format."""

from __future__ import annotations

import re
import sys
from pathlib import Path

DATA_DIR = Path("/app/processed_data")
CHUNK_GLOB = "reviews_chunk_*.csv"
ROW_START = re.compile(r"^(\d+),(\d+),(.*)$")


def escape_pg_text(value: str) -> str:
    out: list[str] = []
    for ch in value:
        code = ord(ch)
        if ch == "\t":
            out.append(r"\t")
        elif ch == "\n":
            out.append(r"\n")
        elif ch == "\r":
            out.append(r"\r")
        elif ch == "\\":
            out.append(r"\\")
        elif code == 0:
            continue
        elif code < 32:
            out.append(f"\\{code:03o}")
        else:
            out.append(ch)
    return "".join(out)


def stream_reviews() -> int:
    files = sorted(DATA_DIR.glob(CHUNK_GLOB))
    if not files:
        print("No review chunk files found", file=sys.stderr)
        return 1

    print(f"Streaming {len(files)} review chunks", file=sys.stderr)

    current_id: str | None = None
    current_hotel: str | None = None
    current_lines: list[str] = []

    def flush_current() -> None:
        if current_id is None or current_hotel is None:
            return
        text = "\n".join(current_lines)
        sys.stdout.write(f"{current_id}\t{current_hotel}\t{escape_pg_text(text)}\n")

    for path in files:
        with path.open(encoding="utf-8", errors="ignore") as fh:
            header = True
            for raw_line in fh:
                line = raw_line.rstrip("\r\n")
                if header:
                    header = False
                    continue
                match = ROW_START.match(line)
                if match:
                    flush_current()
                    rid, hid, remainder = match.groups()
                    current_id = rid
                    current_hotel = hid
                    current_lines = [remainder]
                else:
                    if current_id is None:
                        continue
                    current_lines.append(line)
            flush_current()
            current_id = None
            current_hotel = None
            current_lines = []

    return 0


if __name__ == "__main__":
    raise SystemExit(stream_reviews())
