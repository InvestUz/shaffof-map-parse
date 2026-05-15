#!/usr/bin/env python3
"""
Extract investment service records (statuses: 6, 2, 3, 4) from dx.mc.uz into XLSX.

Minimal run (starts Chrome with debugging on 9222 if needed, then scrapes):

  python scrape_invest.py

Resume after stop or re-login:

  python scrape_invest.py --resume

Only detail pages from an existing checkpoint (list rows + detail_url already saved; no list pagination):

  python scrape_invest.py --details-from-checkpoint --checkpoint invest_ckpt.json -o out.xlsx

By default that mode also re-fetches rows whose saved detail has an empty fields{} (old runs timed out
before the React card rendered). Disable with --no-refetch-empty-fields.

Re-scrape every detail (e.g. after improving field extraction) and merge into XLSX:

  python scrape_invest.py --details-from-checkpoint --refetch-details --checkpoint invest_ckpt.json -o out.xlsx

Authentication options:
  * Default: attach to http://127.0.0.1:9222, or auto-launch Chrome with a temp profile
    (--no-auto-chrome to disable launch and fail if nothing listens on 9222).
  * Or: python scrape_invest.py --storage auth.json --headless

Older manual CDP flow still works:

  python scrape_invest.py --cdp http://127.0.0.1:9222

Outputs:
  Sheet "Main records" — one row per application (list + detail fields).
  Sheet "Additional documents" — rows from "Qo'shimcha hujjatlar" UI (linked by record_id).
  Sheet "Attachments" — file-like links from detail pages (linked by record_id).
  Embedded PDFs sometimes appear as data:...;base64,... in link hrefs. By default those are replaced
  with a short note so JSONL/checkpoint stay small; use --data-url-dir DIR to decode them to files.

Checkpoints (--checkpoint + --resume):
  * invest_ckpt.json stores next_list_page (resume listing from that page), all table rows,
    and every detail scraped so far.
  * --details-from-checkpoint loads that file without requiring --resume, skips listing, visits each
    row's detail_url, and writes/updates the XLSX (and checkpoint if configured).
  * Before each list page fetch, checkpoint is updated to that page number (safe if connection drops).
  * With --resume, invest_live_details.jsonl is merged into memory so rows not yet in the JSON
    checkpoint are still skipped.
  * On any exit (crash, lost connection, Ctrl+C), a final checkpoint + XLSX flush runs in finally.

Speed:
  * Detail pages no longer use the listing-page selector (that caused ~45s waits per row).
  * --stream-records / --stream-details append-only JSON lines (fast live log).
  * --list-only — only pagination + table (no per-row detail); fits ~1 hour for thousands of rows.
  * --fast skips heavier DOM passes.
  * --xlsx-every 0 (default) avoids rewriting the whole Excel on every row (very slow).
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import errno
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import pandas as pd
from playwright.async_api import Browser, BrowserContext, Error, Page, async_playwright

LIST_URL_TEMPLATE = "https://dx.mc.uz/dashboard/services/invest?status={status}&page={page}"
VIEW_PATH_RE = re.compile(r"/dashboard/services/invest/view/(\d+)", re.I)
UZ_TOTAL_RE = re.compile(
    r"(\d[\d,\s]*)\s+ta\s+arizadan\s+(\d+)\s*[-–]\s*(\d+)\s+tasi",
    re.I,
)
PER_PAGE_FALLBACK = 30
MAX_RETRIES = 4
RETRY_DELAY_S = 2.5
STATE_VERSION = 1
DEFAULT_CDP = "http://127.0.0.1:9222"
ALLOWED_STATUSES = (2, 3, 4, 6)
AUTO_CHROME_PROFILE = Path(os.environ.get("TEMP", os.environ.get("TMP", "."))) / "chrome-dx-debug-scraper"


def _cdp_port(cdp_url: str) -> int:
    u = urlparse(cdp_url)
    return int(u.port or 9222)


def find_chrome_executable() -> Path | None:
    env = os.environ.get("CHROME_PATH") or os.environ.get("GOOGLE_CHROME_BIN")
    cands: list[str | Path] = []
    if env:
        cands.append(env)
    if sys.platform == "win32":
        for base in (
            os.environ.get("PROGRAMFILES", ""),
            os.environ.get("ProgramFiles(x86)", ""),
            os.environ.get("PROGRAMFILES(X86)", ""),
        ):
            if base.strip():
                cands.append(Path(base) / "Google/Chrome/Application/chrome.exe")
        cands.extend(
            [
                r"C:\Program Files\Google\Chrome\Application\chrome.exe",
                r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            ]
        )
    else:
        cands.extend(["/Applications/Google Chrome.app/Contents/MacOS/Google Chrome", "google-chrome", "chromium"])
    for raw in cands:
        p = Path(raw) if not isinstance(raw, Path) else raw
        s = str(p)
        if not s or s == ".":
            continue
        if p.is_file():
            return p
        if sys.platform != "win32" and isinstance(raw, str) and os.path.isfile(raw):
            return Path(raw)
    return None


def start_chrome_with_cdp(port: int, user_data_dir: Path | None = None) -> None:
    exe = find_chrome_executable()
    if not exe:
        raise RuntimeError(
            "Google Chrome not found. Install it or set CHROME_PATH to chrome.exe (full path)."
        )
    udir = Path(user_data_dir) if user_data_dir else AUTO_CHROME_PROFILE
    udir.mkdir(parents=True, exist_ok=True)
    args = [str(exe), f"--remote-debugging-port={port}", f"--user-data-dir={str(udir.resolve())}"]
    creationflags = 0
    if sys.platform == "win32":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    subprocess.Popen(
        args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        close_fds=sys.platform != "win32",
        creationflags=creationflags,
        start_new_session=sys.platform != "win32",
    )
    logging.info(
        "Started Chrome for scraping (profile %s, port %s). Log in to dx.mc.uz if needed.",
        udir,
        port,
    )


async def wait_for_cdp_url(cdp_url: str, *, timeout_s: float = 90.0) -> None:
    chk = cdp_url.rstrip("/") + "/json/version"
    deadline = time.monotonic() + timeout_s

    def probe() -> bool:
        try:
            with urllib.request.urlopen(chk, timeout=2) as r:  # nosec - localhost CDP
                return r.status == 200
        except (urllib.error.URLError, OSError, TimeoutError):
            return False

    while time.monotonic() < deadline:
        if await asyncio.to_thread(probe):
            return
        await asyncio.sleep(0.35)
    raise RuntimeError(
        f"Chrome did not expose DevTools at {chk!r} within {timeout_s:.0f}s "
        "(check firewall / another app using the port)."
    )


def setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


@dataclass
class ListRow:
    record_id: str
    detail_url: str
    cells: list[str] = field(default_factory=list)


@dataclass
class DetailResult:
    record_id: str
    fields: dict[str, str]
    additional_doc_rows: list[dict[str, Any]]
    attachments: list[dict[str, Any]]
    errors: list[str] = field(default_factory=list)


def _list_row_to_dict(lr: ListRow) -> dict[str, Any]:
    return {"record_id": lr.record_id, "detail_url": lr.detail_url, "cells": lr.cells}


def _list_row_from_dict(d: dict[str, Any]) -> ListRow:
    return ListRow(
        record_id=str(d["record_id"]),
        detail_url=str(d["detail_url"]),
        cells=list(d.get("cells") or []),
    )


def _detail_to_dict(d: DetailResult) -> dict[str, Any]:
    return {
        "record_id": d.record_id,
        "fields": d.fields,
        "additional_doc_rows": d.additional_doc_rows,
        "attachments": d.attachments,
        "errors": d.errors,
    }


def _detail_from_dict(d: dict[str, Any]) -> DetailResult:
    return DetailResult(
        record_id=str(d["record_id"]),
        fields=dict(d.get("fields") or {}),
        additional_doc_rows=list(d.get("additional_doc_rows") or []),
        attachments=list(d.get("attachments") or []),
        errors=list(d.get("errors") or []),
    )


def save_progress(
    checkpoint_path: Path,
    *,
    status: int,
    list_complete: bool,
    next_list_page: int,
    all_list_rows: list[ListRow],
    seen_ids: set[str],
    details: dict[str, DetailResult],
    errors_log: list[dict[str, Any]],
    total_pages: int | None,
) -> None:
    payload: dict[str, Any] = {
        "version": STATE_VERSION,
        "status": status,
        "list_complete": list_complete,
        "next_list_page": next_list_page,
        "seen_ids": sorted(seen_ids),
        "total_pages": total_pages,
        "all_list_rows": [_list_row_to_dict(lr) for lr in all_list_rows],
        "details": {k: _detail_to_dict(v) for k, v in details.items()},
        "errors_log": errors_log,
    }
    data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = checkpoint_path.with_suffix(checkpoint_path.suffix + ".tmp")
    tmp.write_text(data, encoding="utf-8")
    try:
        os.replace(str(tmp), str(checkpoint_path))
    except OSError as exc:
        winerr = getattr(exc, "winerror", None)
        if winerr == 5 or exc.errno in (errno.EACCES, errno.EPERM, 13):
            logging.warning(
                "Checkpoint rename blocked (%s). Writing directly — close invest_ckpt.json in other apps; "
                "if this persists, use a path outside OneDrive, e.g. --checkpoint %%TEMP%%\\invest_ckpt.json",
                exc,
            )
            checkpoint_path.write_text(data, encoding="utf-8")
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            return
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def append_jsonl(path: Path | None, record: dict[str, Any], *, file_lock: threading.Lock | None) -> None:
    if path is None:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
    if file_lock:
        with file_lock:
            with path.open("a", encoding="utf-8") as f:
                f.write(line)
                f.flush()
    else:
        with path.open("a", encoding="utf-8") as f:
            f.write(line)
            f.flush()


def load_stream_details(path: Path | None, details: dict[str, DetailResult]) -> None:
    if path is None or not path.exists():
        return
    with path.open(encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            d = _detail_from_dict(json.loads(raw))
            details[d.record_id] = d


def flatten_main_record(lr: ListRow, d: DetailResult | None) -> dict[str, Any]:
    row: dict[str, Any] = {
        "record_id": lr.record_id,
        "detail_url": lr.detail_url,
    }
    for i, c in enumerate(lr.cells):
        row[f"list_col_{i+1}"] = c
    if d:
        for k, v in d.fields.items():
            row[f"detail__{k}"] = v
        if d.errors:
            row["_extraction_warnings"] = " | ".join(d.errors)
    return row


def load_checkpoint(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    ver = int(raw.get("version", 0))
    if ver != STATE_VERSION:
        raise ValueError(f"Unsupported checkpoint version {ver!r} (expected {STATE_VERSION})")
    return raw


def _dedupe_list_rows_by_record_id(rows: list[ListRow]) -> tuple[list[ListRow], int]:
    """Keep first occurrence per record_id (checkpoint bugs or retries can duplicate rows)."""
    out: list[ListRow] = []
    seen: set[str] = set()
    dropped = 0
    for lr in rows:
        if lr.record_id in seen:
            dropped += 1
            continue
        seen.add(lr.record_id)
        out.append(lr)
    return out, dropped


def hydrate_checkpoint(raw: dict[str, Any]) -> tuple[
    list[ListRow],
    set[str],
    dict[str, DetailResult],
    list[dict[str, Any]],
    bool,
    int,
    int | None,
]:
    all_list_rows = [_list_row_from_dict(x) for x in (raw.get("all_list_rows") or [])]
    all_list_rows, list_dupes = _dedupe_list_rows_by_record_id(all_list_rows)
    if list_dupes:
        logging.warning(
            "Checkpoint contained %s duplicate list row(s) for the same record_id (kept first of each)",
            list_dupes,
        )
    seen_ids = set(str(x) for x in (raw.get("seen_ids") or []))
    details = {str(k): _detail_from_dict(v) for k, v in (raw.get("details") or {}).items()}
    errors_log = list(raw.get("errors_log") or [])
    list_complete = bool(raw.get("list_complete"))
    next_list_page = int(raw.get("next_list_page") or 1)
    total_pages = raw.get("total_pages")
    total_pages_i: int | None = int(total_pages) if total_pages is not None else None
    return all_list_rows, seen_ids, details, errors_log, list_complete, next_list_page, total_pages_i


def load_list_rows_from_xlsx(path: Path, *, sheet_name: str = "Main records") -> list[ListRow]:
    """
    Read list rows from a previously exported XLSX.
    Requires at least `detail_url`; uses `record_id` when present, otherwise derives it from URL.
    """
    df = pd.read_excel(path, sheet_name=sheet_name)
    if "detail_url" not in df.columns:
        raise ValueError(f"{path} sheet={sheet_name!r} missing required column: detail_url")

    list_cols = sorted(
        [c for c in df.columns if str(c).startswith("list_col_")],
        key=lambda x: int(re.search(r"\d+", str(x)).group()) if re.search(r"\d+", str(x)) else 0,
    )
    out: list[ListRow] = []
    for i, r in df.iterrows():
        detail = str(r.get("detail_url") or "").strip()
        if not detail:
            continue
        rid = str(r.get("record_id") or "").strip()
        if not rid:
            m = VIEW_PATH_RE.search(detail)
            rid = m.group(1) if m else ""
        if not rid:
            logging.debug("xlsx row %s skipped (cannot derive record_id): %s", i + 2, detail)
            continue
        cells: list[str] = []
        for c in list_cols:
            v = r.get(c)
            cells.append("" if pd.isna(v) else str(v))
        out.append(ListRow(record_id=rid, detail_url=detail, cells=cells))
    out, dropped = _dedupe_list_rows_by_record_id(out)
    if dropped:
        logging.warning("Input XLSX had %s duplicate record_id row(s); kept first of each", dropped)
    return out


async def _goto_with_retry(page: Page, url: str, *, kind: str) -> None:
    last: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if kind == "detail":
                await page.goto(url, wait_until="domcontentloaded", timeout=90_000)
                try:
                    await page.wait_for_selector(
                        ".table-custom, dl, [class*='detail'], main, [role='main'], article",
                        timeout=15_000,
                    )
                except Error:
                    await page.wait_for_timeout(500)
            else:
                await page.goto(url, wait_until="load", timeout=120_000)
                try:
                    await page.wait_for_selector(
                        "table tbody tr, a[href*='/dashboard/services/invest/view/']",
                        timeout=25_000,
                    )
                except Error:
                    await page.wait_for_timeout(1200)
            return
        except Error as e:
            last = e
            logging.warning("goto %s attempt %s/%s: %s", url, attempt, MAX_RETRIES, e)
            await asyncio.sleep(RETRY_DELAY_S * attempt)
    raise RuntimeError(f"Failed to load {url}: {last}")


def _abs_url(base: str, href: str | None) -> str | None:
    if not href or href.strip().startswith("#") or href.lower().startswith("javascript:"):
        return None
    return urljoin(base, href.strip())


def externalize_or_summarize_data_url(
    url: str,
    record_id: str,
    name_hint: str,
    data_url_dir: Path | None,
) -> str:
    """Replace giant data:...;base64,... hrefs with a short note or a saved file path."""
    if not url.startswith("data:"):
        return url
    try:
        meta, _, b64part = url.partition(",")
        mime = "application/octet-stream"
        if meta.startswith("data:"):
            rest = meta[5:]
            semi = rest.find(";")
            mime = rest[:semi] if semi >= 0 else rest
        raw = base64.b64decode(b64part, validate=False)
    except Exception:
        return f"<data-url undecodable, {len(url)} chars>"

    if data_url_dir is not None:
        dest_dir = data_url_dir / record_id
        dest_dir.mkdir(parents=True, exist_ok=True)
        ext = ".pdf" if "pdf" in mime.lower() else ".bin"
        safe = re.sub(r"[^\w\u0400-\u04FF\-]+", "_", name_hint).strip("_")[:80] or "file"
        dest = dest_dir / f"{safe}{ext}"
        n = 0
        while dest.exists():
            n += 1
            dest = dest_dir / f"{safe}_{n}{ext}"
        dest.write_bytes(raw)
        return str(dest.resolve())

    return f"<inline {mime} {len(raw)} bytes — not embedded; use --data-url-dir to save files>"


def _parse_total_and_pages(html: str, per_page_hint: int | None) -> tuple[int | None, int | None]:
    """Return (total_records, total_pages) if derivable from list footer text."""
    m = UZ_TOTAL_RE.search(html.replace("\xa0", " "))
    if not m:
        return None, None
    total_s = m.group(1).replace(",", "").replace(" ", "").strip()
    hi_s = m.group(3).replace(",", "").replace(" ", "").strip()
    low_s = m.group(2).replace(",", "").replace(" ", "").strip()
    try:
        total = int(total_s)
        low = int(low_s)
        hi = int(hi_s)
    except ValueError:
        return None, None
    per = (hi - low + 1) if hi >= low > 0 else (per_page_hint or PER_PAGE_FALLBACK)
    pages = (total + per - 1) // per if per else None
    return total, pages


async def extract_list_page(page: Page, status: int, page_num: int) -> tuple[list[ListRow], int | None]:
    url = LIST_URL_TEMPLATE.format(status=status, page=page_num)
    await _goto_with_retry(page, url, kind="list")

    base = page.url
    html = await page.content()
    _, total_pages = _parse_total_and_pages(html, PER_PAGE_FALLBACK)

    rows: list[ListRow] = []
    seen: set[str] = set()

    # Primary: table rows with a /view/{id} link
    for tr in await page.locator("table tbody tr").all():
        try:
            link_el = tr.locator('a[href*="/dashboard/services/invest/view/"]').first
            if await link_el.count() == 0:
                continue
            href = await link_el.get_attribute("href")
            detail = _abs_url(base, href)
            if not detail:
                continue
            vm = VIEW_PATH_RE.search(detail)
            if not vm:
                continue
            rid = vm.group(1)
            if rid in seen:
                continue
            seen.add(rid)
            cells: list[str] = []
            for td in await tr.locator("td").all():
                t = (await td.inner_text()).strip()
                cells.append(re.sub(r"\s+", " ", t))
            rows.append(ListRow(record_id=rid, detail_url=detail, cells=cells))
        except Error as e:
            logging.debug("skip tr: %s", e)

    if not rows:
        # Fallback: any view links in main area
        for a in await page.locator('a[href*="/dashboard/services/invest/view/"]').all():
            href = await a.get_attribute("href")
            detail = _abs_url(base, href)
            if not detail:
                continue
            vm = VIEW_PATH_RE.search(detail)
            if not vm:
                continue
            rid = vm.group(1)
            if rid in seen:
                continue
            seen.add(rid)
            txt = re.sub(r"\s+", " ", (await a.inner_text()).strip())
            rows.append(ListRow(record_id=rid, detail_url=detail, cells=[txt]))

    return rows, total_pages


async def discover_max_page_from_pager(page: Page) -> int | None:
    """Parse pagination controls for the highest ?page=N link."""
    best = 1
    found = False
    for a in await page.locator('a[href*="page="]').all():
        try:
            href = await a.get_attribute("href")
            if not href:
                continue
            u = urlparse(urljoin(page.url, href))
            q = u.query or ""
            for part in q.split("&"):
                if part.startswith("page="):
                    n = int(part.split("=", 1)[1])
                    found = True
                    best = max(best, n)
        except (ValueError, Error):
            continue
    return best if found else None


async def _click_optional_extras(page: Page) -> None:
    """Open 'Qo'shimcha hujjatlar' if it is a control that reveals links."""
    selectors = [
        'button:has-text("Qo\'shimcha hujjatlar")',
        'button:has-text("Qoʻsimcha hujjatlar")',
        'a:has-text("Qo\'shimcha hujjatlar")',
        'a:has-text("Qoʻsimcha hujjatlar")',
        '*[role="button"]:has-text("hujjatlar")',
    ]
    for sel in selectors:
        loc = page.locator(sel).first
        if await loc.count():
            try:
                await loc.click(timeout=8000)
                await page.wait_for_timeout(1200)
            except Error:
                pass
            break


# Detail pages render label/value rows inside .table-custom (Bootstrap grid), not <dl>.
_TABLE_CUSTOM_KV_JS = r"""() => {
  const norm = (s) => (s || "").replace(/\s+/g, " ").trim();
  const root = document.querySelector(".table-custom");
  if (!root) return [];
  const out = [];
  for (const row of root.querySelectorAll(":scope > div.row")) {
    const lc = row.querySelector(".col-lg-4");
    const rc = row.querySelector(".col-lg-8");
    if (!lc || !rc) continue;
    const lab =
      lc.querySelector("span.text-grey") || lc.querySelector('span[class*="text-grey"]');
    if (!lab) continue;
    const key = norm(lab.textContent);
    if (!key) continue;
    const val = norm(rc.textContent);
    out.push([key, val]);
  }
  return out;
}"""

# Invest detail pages paint labels first; values arrive via XHR. Wait until the card is "real".
_DETAIL_POPULATED_JS = """(rid) => {
  const r = String(rid || "").trim();
  if (!r) return false;
  const root = document.querySelector(".table-custom");
  if (!root) return false;
  const block = (root.innerText || "").replace(/\\s+/g, " ");
  if (!block.includes(r)) return false;
  let meaningful = 0;
  for (const row of root.querySelectorAll(":scope > div.row")) {
    const rc = row.querySelector(".col-lg-8");
    if (!rc) continue;
    const t = (rc.textContent || "").replace(/\\s+/g, " ").trim();
    if (t && t !== "-" && t.length >= 2) meaningful++;
  }
  return meaningful >= 1;
}"""


async def _wait_lazy_detail_ready(page: Page, record_id: str, errors: list[str]) -> None:
    """Block until SPA fills .table-custom (not just label shell with '-' placeholders)."""
    rid = str(record_id).strip()
    if not rid:
        return
    for round_i in range(2):
        try:
            await page.wait_for_function(
                _DETAIL_POPULATED_JS,
                arg=rid,
                timeout=78_000 if round_i == 0 else 50_000,
            )
            return
        except Error as e:
            if round_i == 0:
                logging.warning(
                    "Detail %s: values still empty (lazy load); soft-reloading once: %s",
                    rid,
                    e,
                )
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=90_000)
                except Error as e2:
                    errors.append(f"detail reload: {e2}")
                    return
            else:
                errors.append(f"lazy detail: data did not appear after wait+reload: {e}")


def _normalize_detail_field_key(key: str) -> str:
    """Strip redundant whitespace and a trailing colon for stable XLSX column names."""
    k = re.sub(r"\s+", " ", (key or "").strip())
    k = re.sub(r"\s*:\s*$", "", k).strip()
    return k


async def extract_fields_table_custom(page: Page) -> dict[str, str]:
    """Key/value pairs from the invest detail card (.table-custom), including Holati badges."""
    try:
        raw = await page.evaluate(_TABLE_CUSTOM_KV_JS)
    except Error:
        return {}
    out: dict[str, str] = {}
    for pair in raw or []:
        if not isinstance(pair, (list, tuple)) or len(pair) < 2:
            continue
        k = _normalize_detail_field_key(str(pair[0]))
        v = re.sub(r"\s+", " ", str(pair[1]).strip())
        if k:
            out[k] = v
    return out


async def _poll_table_custom_fields(page: Page, errors: list[str]) -> dict[str, str]:
    """Wait for the React detail card; re-read until we see enough rows or attempts exhausted."""
    best: dict[str, str] = {}
    for attempt in range(1, 12):
        try:
            tout = 22_000 if attempt <= 2 else 2_500
            await page.wait_for_selector(".table-custom", state="attached", timeout=tout)
        except Error:
            pass
        chunk = await extract_fields_table_custom(page)
        if len(chunk) > len(best):
            best = dict(chunk)
        # Typical invest detail has ~11 labelled rows; 3+ means the card is likely populated.
        if len(best) >= 3:
            break
        await page.wait_for_timeout(450)
    if len(best) < 3:
        errors.append(f"table-custom: only {len(best)} field(s) after wait/poll (expected several)")
    return best


async def extract_detail(
    page: Page,
    detail_url: str,
    record_id: str,
    *,
    fast: bool = False,
    data_url_dir: Path | None = None,
) -> DetailResult:
    await _goto_with_retry(page, detail_url, kind="detail")
    base = page.url
    fields: dict[str, str] = {}
    additional_doc_rows: list[dict[str, Any]] = []
    attachments: list[dict[str, Any]] = []
    errors: list[str] = []

    try:
        await _wait_lazy_detail_ready(page, record_id, errors)
    except Error as e:
        errors.append(f"wait lazy detail: {e}")

    try:
        for fk, fv in (await _poll_table_custom_fields(page, errors)).items():
            fields[fk] = fv
    except Error as e:
        errors.append(f"table-custom fields: {e}")

    # Definition lists
    for dl in await page.locator("dl").all():
        dts = await dl.locator("dt").all()
        dds = await dl.locator("dd").all()
        for i, dt in enumerate(dts):
            key = _normalize_detail_field_key(re.sub(r"\s+", " ", (await dt.inner_text()).strip()))
            if not key:
                continue
            dd = dds[i] if i < len(dds) else None
            val = ""
            if dd:
                val = re.sub(r"\s+", " ", (await dd.inner_text()).strip())
            if key and val and key not in fields:
                fields[key] = val

    if not fast:
        # Rows that look like "Label ... Value" in grid/flex cards
        for row in await page.locator('[class*="detail"] dt, .row label, .info-row').all():
            try:
                key = _normalize_detail_field_key(
                    re.sub(r"\s+", " ", (await row.inner_text()).strip().splitlines()[0])
                )
                sib = row.locator("xpath=./following-sibling::*[1]")
                if await sib.count():
                    val = re.sub(r"\s+", " ", (await sib.inner_text()).strip())
                    if key and val and key not in fields and len(key) < 120:
                        fields[key] = val
            except Error:
                pass

        # Generic pairs: sibling divs/spans in description lists (MUI/Ant-style) — slow on large DOM
        pairs_js = """() => {
          const out = [];
          document.querySelectorAll('[class*="MuiGrid"], .ant-descriptions-item, .row, tr').forEach((el) => {
            const t = el.innerText || '';
            const lines = t.split(/\\r?\\n/).map(s => s.trim()).filter(Boolean);
            if (lines.length >= 2 && lines[0].length < 200) {
              out.push([lines[0], lines.slice(1).join(' ')]);
            }
          });
          return out;
        }"""
        try:
            raw_pairs = await page.evaluate(pairs_js)
            for k, v in raw_pairs or []:
                kk = _normalize_detail_field_key(re.sub(r"\s+", " ", str(k).strip()))
                vv = re.sub(r"\s+", " ", str(v).strip())
                if kk and vv and kk not in fields and len(kk) < 200:
                    fields[kk] = vv
        except Error:
            pass

    # Collect download/document anchors before opening modals
    def looks_like_file(h: str) -> bool:
        h = h.lower()
        return any(
            x in h
            for x in ("/download", "/file", "/api/", ".pdf", ".doc", ".docx", ".zip", "storage", "attachment")
        )

    for a in await page.locator("a[href]").all():
        try:
            href = await a.get_attribute("href")
            full = _abs_url(base, href)
            if not full:
                continue
            name = re.sub(r"\s+", " ", (await a.inner_text()).strip()) or full
            if looks_like_file(full) or await a.get_attribute("download") is not None:
                attachments.append(
                    {
                        "record_id": record_id,
                        "name": name[:500],
                        "url": full,
                        "source": "detail_page_link",
                    }
                )
        except Error:
            pass

    await _click_optional_extras(page)

    # Dialog / drawer links
    for scope in (
        page.locator('[role="dialog"]'),
        page.locator(".modal"),
        page.locator(".ant-modal"),
        page.locator(".MuiDialog-root"),
    ):
        if await scope.count() == 0:
            continue
        box = scope.first
        for a in await box.locator("a[href]").all():
            try:
                href = await a.get_attribute("href")
                full = _abs_url(base, href)
                if not full:
                    continue
                name = re.sub(r"\s+", " ", (await a.inner_text()).strip()) or full
                row = {"record_id": record_id, "name": name[:500], "url": full}
                additional_doc_rows.append(row)
                attachments.append({**row, "source": "qosimcha_hujjatlar_modal"})
            except Error as e:
                errors.append(f"modal link: {e}")

    # Download buttons that are actually links
    for sel in (
        'a:has-text("Arizaning PDF")',
        'a:has-text("Elektr")',
        'a:has-text("Qoʻsimcha")',
        'a:has-text("Qo\'shimcha")',
        'a:has-text("Xabarnoma")',
        'a:has-text("Xulosa")',
    ):
        for a in await page.locator(sel).all():
            try:
                href = await a.get_attribute("href")
                full = _abs_url(base, href)
                if full:
                    name = re.sub(r"\s+", " ", (await a.inner_text()).strip()) or sel
                    attachments.append(
                        {"record_id": record_id, "name": name[:300], "url": full, "source": "named_action_link"}
                    )
            except Error:
                pass

    for attr in ("data-href", "data-url", "data-link", "data-file-url"):
        for el in await page.locator(f"[{attr}]").all():
            try:
                raw = await el.get_attribute(attr)
                full = _abs_url(base, raw)
                if not full:
                    continue
                label = re.sub(r"\s+", " ", (await el.inner_text()).strip()) or attr
                attachments.append(
                    {
                        "record_id": record_id,
                        "name": label[:300],
                        "url": full,
                        "source": f"attr:{attr}",
                    }
                )
            except Error:
                pass

    # De-dupe attachments by (record_id, url)
    uniq: dict[tuple[str, str], dict[str, Any]] = {}
    for att in attachments:
        k = (str(att.get("record_id", "")), str(att.get("url", "")))
        if k[1] and k not in uniq:
            uniq[k] = att
    attachments = list(uniq.values())

    for att in attachments:
        u = att.get("url")
        if isinstance(u, str):
            att["url"] = externalize_or_summarize_data_url(
                u, record_id, str(att.get("name", "")), data_url_dir
            )
    for row in additional_doc_rows:
        u = row.get("url")
        if isinstance(u, str):
            row["url"] = externalize_or_summarize_data_url(
                u, record_id, str(row.get("name", "")), data_url_dir
            )

    return DetailResult(
        record_id=record_id,
        fields=fields,
        additional_doc_rows=additional_doc_rows,
        attachments=attachments,
        errors=errors,
    )


async def get_browser_context(
    p,
    *,
    cdp: str | None,
    storage: str | None,
    headless: bool,
    auto_chrome: bool,
) -> tuple[Browser | None, BrowserContext, bool]:
    """Returns (browser, context, chrome_was_auto_started)."""
    if storage:
        browser = await p.chromium.launch(headless=headless, channel="chrome")
        ctx = await browser.new_context(storage_state=storage, locale="uz-UZ")
        return browser, ctx, False

    cdp_url = (cdp or os.environ.get("PLAYWRIGHT_CDP_URL") or DEFAULT_CDP).rstrip("/")
    try:
        browser = await p.chromium.connect_over_cdp(cdp_url)
    except Exception as e:
        if not auto_chrome:
            raise
        logging.warning("CDP connect failed (%s); launching Chrome for you…", e)
        start_chrome_with_cdp(_cdp_port(cdp_url))
        await wait_for_cdp_url(cdp_url)
        browser = await p.chromium.connect_over_cdp(cdp_url)
        contexts = browser.contexts
        if not contexts:
            ctx = await browser.new_context()
        else:
            ctx = contexts[0]
        return browser, ctx, True
    contexts = browser.contexts
    if not contexts:
        ctx = await browser.new_context()
    else:
        ctx = contexts[0]
    return browser, ctx, False


def _filename_from_response_headers(headers: dict[str, str], fallback: str) -> str:
    cd = headers.get("content-disposition") or headers.get("Content-Disposition") or ""
    m = re.search(r"filename\*=UTF-8''([^;]+)|filename=\"([^\"]+)\"", cd, re.I)
    if m:
        name = unquote((m.group(1) or m.group(2) or "").strip())
        if name:
            return Path(name).name
    path = urlparse(fallback).path
    return Path(path).name or "download"


async def download_attachments(
    context: BrowserContext,
    attachments: list[dict[str, Any]],
    base_folder: Path,
) -> None:
    base_folder.mkdir(parents=True, exist_ok=True)
    req = context.request
    seen_key: set[tuple[str, str]] = set()
    for att in attachments:
        url = str(att.get("url") or "")
        rid = str(att.get("record_id") or "unknown")
        if not url:
            continue
        if url.startswith("<") or url.startswith("data:"):
            continue
        try:
            local = Path(url)
            if local.is_file():
                att["local_path"] = str(local.resolve())
                continue
        except OSError:
            pass
        key = (rid, url)
        if key in seen_key:
            continue
        seen_key.add(key)
        dest_dir = base_folder / rid
        dest_dir.mkdir(parents=True, exist_ok=True)
        try:
            resp = await req.get(url, timeout=120_000)
            if not resp.ok:
                logging.warning("download failed %s status=%s", url[:120], resp.status)
                continue
            hdrs = {k.lower(): v for k, v in resp.headers.items()}
            name = _filename_from_response_headers(hdrs, url)
            dest = dest_dir / name
            if dest.exists():
                stem, suf = dest.stem, dest.suffix
                dest = dest_dir / f"{stem}_{abs(hash(url)) % 10_000_000}{suf}"
            body = await resp.body()
            dest.write_bytes(body)
            att["local_path"] = str(dest.resolve())
        except Exception as e:
            logging.warning("download error %s: %s", url[:120], e)


def build_xlsx(
    path: str,
    list_rows: list[ListRow],
    details: dict[str, DetailResult],
    errors_log: list[dict[str, Any]],
) -> None:
    records_out: list[dict[str, Any]] = []
    for lr in list_rows:
        d = details.get(lr.record_id)
        row: dict[str, Any] = {
            "record_id": lr.record_id,
            "detail_url": lr.detail_url,
        }
        for i, c in enumerate(lr.cells):
            row[f"list_col_{i+1}"] = c
        if d:
            for k, v in d.fields.items():
                row[f"detail__{k}"] = v
            if d.errors:
                row["_extraction_warnings"] = " | ".join(d.errors)
        records_out.append(row)

    add_docs: list[dict[str, Any]] = []
    att_all: list[dict[str, Any]] = []
    for rid, d in details.items():
        for r in d.additional_doc_rows:
            add_docs.append(dict(r))
        for a in d.attachments:
            att_all.append(dict(a))

    err_df = pd.DataFrame(errors_log) if errors_log else pd.DataFrame(columns=["record_id", "error"])

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        pd.DataFrame(records_out).to_excel(writer, sheet_name="Main records", index=False)
        pd.DataFrame(add_docs).to_excel(writer, sheet_name="Additional documents", index=False)
        pd.DataFrame(att_all).to_excel(writer, sheet_name="Attachments", index=False)
        err_df.to_excel(writer, sheet_name="errors", index=False)


async def run(args: argparse.Namespace) -> int:
    setup_logging(args.verbose)
    out_path: str = args.output or f"invest_status_{args.status}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    xlsx_every = max(0, int(args.xlsx_every))
    checkpoint_every = max(1, int(args.checkpoint_every))
    workers = max(1, int(args.workers))
    data_url_dir: Path | None = None
    if args.data_url_dir is not None:
        data_url_dir = Path(args.data_url_dir).expanduser().resolve()

    all_list_rows: list[ListRow] = []
    seen_ids: set[str] = set()
    details: dict[str, DetailResult] = {}
    errors_log: list[dict[str, Any]] = []
    list_complete_flag = False
    saved_next_list_page = max(1, args.start_page)
    total_pages: int | None = None
    ckpt: Path | None = args.checkpoint
    stream_lock = threading.Lock()
    stream_details_path: Path | None = args.stream_details
    stream_records_path: Path | None = args.stream_records
    stream_attachments_path: Path | None = args.stream_attachments
    input_xlsx: Path | None = args.input_xlsx

    details_from_ckpt = bool(getattr(args, "details_from_checkpoint", False))
    if details_from_ckpt and (ckpt is None or not ckpt.exists()):
        logging.error("--details-from-checkpoint requires an existing --checkpoint file")
        return 2

    if ckpt and ckpt.exists():
        if args.resume or details_from_ckpt:
            raw = load_checkpoint(ckpt)
            if int(raw.get("status", args.status)) != args.status:
                logging.warning(
                    "Checkpoint status=%s differs from --status=%s; continuing anyway.",
                    raw.get("status"),
                    args.status,
                )
            (
                all_list_rows,
                seen_ids,
                details,
                errors_log,
                list_complete_flag,
                saved_next_list_page,
                total_pages,
            ) = hydrate_checkpoint(raw)
            if details_from_ckpt:
                list_complete_flag = True
                logging.info(
                    "Details-from-checkpoint: loaded %s list rows, %s detail objects; listing skipped",
                    len(all_list_rows),
                    len(details),
                )
            else:
                logging.info(
                    "Resumed from %s: list_complete=%s next_list_page=%s rows=%s details=%s",
                    ckpt,
                    list_complete_flag,
                    saved_next_list_page,
                    len(all_list_rows),
                    len(details),
                )
        else:
            logging.warning(
                "Checkpoint %s exists; a fresh run will overwrite it on next save. Use --resume to continue.",
                ckpt,
            )

    if input_xlsx is not None:
        if not input_xlsx.exists():
            logging.error("--input-xlsx not found: %s", input_xlsx)
            return 2
        xlsx_rows = load_list_rows_from_xlsx(input_xlsx, sheet_name=args.input_sheet)
        all_list_rows = xlsx_rows
        seen_ids = {x.record_id for x in xlsx_rows}
        list_complete_flag = True
        saved_next_list_page = 1
        total_pages = None
        logging.info(
            "Input XLSX mode: loaded %s rows from %s (sheet=%s); listing skipped, scraping by detail_url",
            len(all_list_rows),
            input_xlsx,
            args.input_sheet,
        )

    if stream_details_path and stream_details_path.exists() and (
        args.resume or args.merge_stream or details_from_ckpt
    ):
        before = len(details)
        load_stream_details(stream_details_path, details)
        logging.info(
            "Merged stream-details %s (+ %s ids, total cached %s)",
            stream_details_path,
            len(details) - before,
            len(details),
        )

    stats = {"finished": 0, "xlsx_accum": 0}
    browser: Browser | None = None

    async with async_playwright() as p:
        try:
            browser, ctx, launched_chrome = await get_browser_context(
                p,
                cdp=args.cdp,
                storage=args.storage,
                headless=args.headless,
                auto_chrome=not args.no_auto_chrome,
            )
            page = await ctx.new_page()

            warm = max(0, int(args.warmup_seconds))
            if warm > 0:
                logging.info("Warmup: waiting %s s (login / load dx.mc.uz if needed)", warm)
                await asyncio.sleep(warm)
            elif launched_chrome:
                logging.info(
                    "Chrome was auto-started — log in at https://dx.mc.uz in that window, then rerun or use "
                    "--warmup-seconds 90 on this command to wait before the first list fetch."
                )

            touched_listing = False
            if not list_complete_flag:
                page_num = (
                    saved_next_list_page
                    if (args.resume and ckpt is not None and ckpt.exists())
                    else max(1, args.start_page)
                )
                if args.resume and ckpt is not None and ckpt.exists() and saved_next_list_page > 1:
                    logging.info("Continuing listing from page %s", page_num)

                while True:
                    touched_listing = True
                    if args.max_pages and (page_num - max(1, args.start_page)) >= args.max_pages:
                        logging.info("Stopping listing: reached --max-pages")
                        if ckpt:
                            save_progress(
                                ckpt,
                                status=args.status,
                                list_complete=False,
                                next_list_page=page_num,
                                all_list_rows=all_list_rows,
                                seen_ids=seen_ids,
                                details=details,
                                errors_log=errors_log,
                                total_pages=total_pages,
                            )
                        break

                    if ckpt:
                        save_progress(
                            ckpt,
                            status=args.status,
                            list_complete=False,
                            next_list_page=page_num,
                            all_list_rows=all_list_rows,
                            seen_ids=seen_ids,
                            details=details,
                            errors_log=errors_log,
                            total_pages=total_pages,
                        )

                    try:
                        rows, discovered_pages = await extract_list_page(page, args.status, page_num)
                    except Exception as e:
                        errors_log.append({"record_id": f"list_page_{page_num}", "error": str(e)})
                        logging.exception("List page %s failed", page_num)
                        if ckpt:
                            save_progress(
                                ckpt,
                                status=args.status,
                                list_complete=False,
                                next_list_page=page_num,
                                all_list_rows=all_list_rows,
                                seen_ids=seen_ids,
                                details=details,
                                errors_log=errors_log,
                                total_pages=total_pages,
                            )
                        break

                    if discovered_pages and total_pages is None:
                        total_pages = discovered_pages
                        logging.info("Detected ~%s pages (from footer text)", total_pages)
                    if total_pages is None and page_num == max(1, args.start_page):
                        pager_max = await discover_max_page_from_pager(page)
                        if pager_max:
                            total_pages = pager_max
                            logging.info("Detected %s pages (from pagination links)", total_pages)

                    if not rows:
                        logging.warning(
                            "No rows on page %s — often: not logged in yet, wrong status filter, or SPA still loading. "
                            "Try: open the list in the browser tab, confirm you see the table, then --resume or "
                            "--warmup-seconds 90",
                            page_num,
                        )
                        if ckpt:
                            save_progress(
                                ckpt,
                                status=args.status,
                                list_complete=False,
                                next_list_page=page_num,
                                all_list_rows=all_list_rows,
                                seen_ids=seen_ids,
                                details=details,
                                errors_log=errors_log,
                                total_pages=total_pages,
                            )
                        break

                    new_count = 0
                    for r in rows:
                        if r.record_id in seen_ids:
                            continue
                        seen_ids.add(r.record_id)
                        all_list_rows.append(r)
                        new_count += 1
                    logging.info(
                        "Page %s: +%s rows (total unique ids %s)", page_num, new_count, len(seen_ids)
                    )

                    done_list = (total_pages is not None and page_num >= total_pages) or (new_count == 0)
                    saved_next_list_page = page_num + 1
                    list_complete_flag = done_list

                    if ckpt:
                        save_progress(
                            ckpt,
                            status=args.status,
                            list_complete=list_complete_flag,
                            next_list_page=saved_next_list_page,
                            all_list_rows=all_list_rows,
                            seen_ids=seen_ids,
                            details=details,
                            errors_log=errors_log,
                            total_pages=total_pages,
                        )

                    if done_list:
                        break

                    page_num += 1
            else:
                logging.info("Listing marked complete in checkpoint (%s rows).", len(all_list_rows))

            detail_todo: list[tuple[int, ListRow]] = []
            if not args.list_only:
                refetch = bool(getattr(args, "refetch_details", False))
                refetch_empty = bool(getattr(args, "refetch_empty_fields", False))
                for idx, lr in enumerate(all_list_rows, start=1):
                    if args.max_records and idx > args.max_records:
                        break
                    d = details.get(lr.record_id)
                    need = (
                        refetch
                        or lr.record_id not in details
                        or (refetch_empty and d is not None and not (d.fields or {}))
                    )
                    if need:
                        detail_todo.append((idx, lr))
            else:
                logging.info(
                    "List-only mode: skipping detail pages (%s table rows).",
                    len(all_list_rows),
                )
                if stream_records_path and touched_listing:
                    for idx, lr in enumerate(all_list_rows, start=1):
                        if args.max_records and idx > args.max_records:
                            break
                        append_jsonl(
                            stream_records_path,
                            flatten_main_record(lr, details.get(lr.record_id)),
                            file_lock=stream_lock,
                        )
                if ckpt and touched_listing:
                    save_progress(
                        ckpt,
                        status=args.status,
                        list_complete=list_complete_flag,
                        next_list_page=saved_next_list_page,
                        all_list_rows=all_list_rows,
                        seen_ids=seen_ids,
                        details=details,
                        errors_log=errors_log,
                        total_pages=total_pages,
                    )

            if detail_todo:
                logging.info(
                    "Scraping %s detail pages (%s concurrent workers, ~mode=%s)",
                    len(detail_todo),
                    workers,
                    "fast" if args.fast else "full",
                )

            sem = asyncio.Semaphore(workers)
            io_lock = asyncio.Lock()

            async def persist_after_job(idx: int, lr: ListRow, res: DetailResult | None, err: Exception | None) -> None:
                async with io_lock:
                    if res is not None:
                        details[lr.record_id] = res
                        append_jsonl(stream_details_path, _detail_to_dict(res), file_lock=stream_lock)
                        append_jsonl(stream_records_path, flatten_main_record(lr, res), file_lock=stream_lock)
                        if stream_attachments_path:
                            for att in res.attachments:
                                append_jsonl(stream_attachments_path, dict(att), file_lock=stream_lock)
                    if err is not None:
                        errors_log.append({"record_id": lr.record_id, "error": str(err)})

                    stats["finished"] += 1
                    if ckpt and stats["finished"] % checkpoint_every == 0:
                        save_progress(
                            ckpt,
                            status=args.status,
                            list_complete=list_complete_flag,
                            next_list_page=saved_next_list_page,
                            all_list_rows=all_list_rows,
                            seen_ids=seen_ids,
                            details=details,
                            errors_log=errors_log,
                            total_pages=total_pages,
                        )

                    if xlsx_every > 0 and res is not None:
                        stats["xlsx_accum"] += 1
                        if stats["xlsx_accum"] >= xlsx_every:
                            build_xlsx(out_path, all_list_rows, details, errors_log)
                            logging.info("Saved XLSX (%s new detail(s)) → %s", xlsx_every, out_path)
                            stats["xlsx_accum"] = 0

            async def scrape_one(entry: tuple[int, ListRow]) -> None:
                idx, lr = entry
                async with sem:
                    pg = await ctx.new_page()
                    res: DetailResult | None = None
                    err: Exception | None = None
                    try:
                        logging.info("Detail %s/%s id=%s", idx, len(all_list_rows), lr.record_id)
                        res = await extract_detail(
                            pg,
                            lr.detail_url,
                            lr.record_id,
                            fast=bool(args.fast),
                            data_url_dir=data_url_dir,
                        )
                    except Exception as e:
                        err = e
                        logging.exception("Detail failed for %s", lr.record_id)
                    finally:
                        await pg.close()
                    await persist_after_job(idx, lr, res, err)

            if not args.list_only:
                try:
                    await asyncio.gather(*(scrape_one(e) for e in detail_todo))
                except asyncio.CancelledError:
                    raise
                except KeyboardInterrupt:
                    if ckpt:
                        save_progress(
                            ckpt,
                            status=args.status,
                            list_complete=list_complete_flag,
                            next_list_page=saved_next_list_page,
                            all_list_rows=all_list_rows,
                            seen_ids=seen_ids,
                            details=details,
                            errors_log=errors_log,
                            total_pages=total_pages,
                        )
                    build_xlsx(out_path, all_list_rows, details, errors_log)
                    logging.info("Interrupted; saved partial XLSX to %s", out_path)
                    raise

            if args.download_dir:
                all_atts: list[dict[str, Any]] = []
                for d in details.values():
                    all_atts.extend(d.attachments)
                await download_attachments(ctx, all_atts, Path(args.download_dir))

        finally:
            try:
                if ckpt:
                    save_progress(
                        ckpt,
                        status=args.status,
                        list_complete=list_complete_flag,
                        next_list_page=saved_next_list_page,
                        all_list_rows=all_list_rows,
                        seen_ids=seen_ids,
                        details=details,
                        errors_log=errors_log,
                        total_pages=total_pages,
                    )
            except Exception as ex:
                logging.warning("Could not write checkpoint: %s", ex)
            try:
                build_xlsx(out_path, all_list_rows, details, errors_log)
            except Exception as ex:
                logging.warning("Could not write XLSX: %s", ex)
            if browser is not None and not args.cdp:
                try:
                    await browser.close()
                except Exception:
                    pass

    n_list = len(all_list_rows)
    n_detail_keys = len(details)
    rows_with_detail_blob = sum(1 for lr in all_list_rows if lr.record_id in details)
    rows_missing_detail = n_list - rows_with_detail_blob
    rows_empty_fields = sum(
        1
        for lr in all_list_rows
        if lr.record_id in details and not (details[lr.record_id].fields or {})
    )
    detail_errors = sum(
        1 for e in errors_log if not str(e.get("record_id", "")).startswith("list_page_")
    )
    logging.info(
        "Finished: XLSX=%s list_rows=%s detail_ids_in_cache=%s list_rows_with_detail=%s "
        "list_rows_missing_detail=%s list_rows_with_empty_fields=%s detail_errors_in_log=%s",
        out_path,
        n_list,
        n_detail_keys,
        rows_with_detail_blob,
        rows_missing_detail,
        rows_empty_fields,
        detail_errors,
    )
    return 0


def main() -> None:
    ap = argparse.ArgumentParser(description="Scrape dx.mc.uz invest dashboard to XLSX")
    ap.add_argument(
        "--cdp",
        default=os.getenv("PLAYWRIGHT_CDP_URL"),
        help=f"Chrome CDP URL (default when unset: {DEFAULT_CDP})",
    )
    ap.add_argument("--storage", help="Use saved Playwright storage JSON instead of CDP (no auto Chrome)")
    ap.add_argument("--no-auto-chrome", action="store_true", help="Do not launch Chrome; require CDP already open")
    ap.add_argument(
        "--warmup-seconds",
        type=int,
        default=0,
        help="Wait this many seconds after browser connect before listing (use ~60–120 after auto-start Chrome to log in)",
    )
    ap.add_argument(
        "--status",
        type=int,
        default=3,
        choices=ALLOWED_STATUSES,
        help="Status filter. Allowed values only: 2, 3, 4, 6 (default: 6)",
    )
    ap.add_argument("--output", "-o", help="Output .xlsx path (default: timestamped invest_status_*.xlsx)")
    ap.add_argument(
        "--input-xlsx",
        type=Path,
        default=None,
        help="Use an existing XLSX as input source (must contain detail_url; record_id optional); "
        "when set, list pagination is skipped and detail pages are scraped from this file",
    )
    ap.add_argument(
        "--input-sheet",
        default="Main records",
        help="Sheet name inside --input-xlsx (default: Main records)",
    )
    ap.add_argument("--start-page", type=int, default=1)
    ap.add_argument("--max-pages", type=int, default=0, help="0 = no limit")
    ap.add_argument("--max-records", type=int, default=0, help="0 = all collected ids")
    ap.add_argument(
        "--list-only",
        action="store_true",
        help="Only scrape listing pages (table columns); skip every /view/ detail — fast, fits ~1h for full grid",
    )
    ap.add_argument(
        "--details-from-checkpoint",
        action="store_true",
        help="Load existing --checkpoint (no --resume needed), skip list pagination, open each row's detail_url "
        "for ids not yet in details, merge Ariza Raqami / Buyurtmachi / … into XLSX + checkpoint",
    )
    ap.add_argument(
        "--refetch-details",
        action="store_true",
        help="Re-scrape detail pages even when record_id is already in details (useful after code changes; "
        "pairs with --details-from-checkpoint or a normal --resume run)",
    )
    ap.add_argument(
        "--refetch-empty-fields",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Re-scrape when an id exists in details but fields is empty (stale load). "
        "Default: on with --details-from-checkpoint, off otherwise (use --no-refetch-empty-fields to disable)",
    )
    ap.add_argument("--headless", action="store_true", help="With --storage only")
    ap.add_argument(
        "--download-dir",
        help="Optional folder to save attachment binaries (reuses session cookies)",
    )
    ap.add_argument(
        "--data-url-dir",
        type=Path,
        default=None,
        help="Decode data:...;base64,... links (e.g. inline PDFs) into this folder and store paths; "
        "if omitted, replace with a short placeholder so --stream-details / checkpoint stay small",
    )
    ap.add_argument(
        "--checkpoint",
        type=Path,
        default=Path("invest_ckpt.json"),
        help="JSON checkpoint path (default: invest_ckpt.json)",
    )
    ap.add_argument("--resume", action="store_true", help="Continue from checkpoint / streams")
    ap.add_argument(
        "--merge-stream",
        action="store_true",
        help="Preload --stream-details even without --resume (normally --resume merges stream automatically)",
    )
    ap.add_argument(
        "--checkpoint-every",
        type=int,
        default=1,
        help="Save invest_ckpt.json every N finished detail jobs (default 1 = safest; raise if disk is slow)",
    )
    ap.add_argument(
        "--xlsx-every",
        type=int,
        default=0,
        help="Rewrite XLSX every N new details; 0 = only at end / on interrupt (much faster)",
    )
    ap.add_argument(
        "--stream-records",
        type=Path,
        default=Path("invest_live_records.jsonl"),
        help="Append one JSON line per row (default: invest_live_records.jsonl; disable with --no-stream-files)",
    )
    ap.add_argument(
        "--stream-details",
        type=Path,
        default=Path("invest_live_details.jsonl"),
        help="Append one DetailResult JSON per row (default: invest_live_details.jsonl)",
    )
    ap.add_argument(
        "--stream-attachments",
        type=Path,
        default=Path("invest_live_attachments.jsonl"),
        help="Append one JSON line per attachment (default: invest_live_attachments.jsonl)",
    )
    ap.add_argument(
        "--no-stream-files",
        action="store_true",
        help="Do not write stream *.jsonl files (checkpoint/XLSX only)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Parallel Playwright tabs for detail pages (same session cookies)",
    )
    ap.add_argument(
        "--fast",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Skip slower DOM fallback passes (default: on; use --no-fast for fuller capture)",
    )
    ap.add_argument("--verbose", "-v", action="store_true")
    args = ap.parse_args()
    if args.refetch_empty_fields is None:
        args.refetch_empty_fields = bool(args.details_from_checkpoint)
    if args.list_only and args.details_from_checkpoint:
        ap.error("--list-only cannot be used with --details-from-checkpoint")
    if args.list_only and args.input_xlsx:
        ap.error("--list-only cannot be used with --input-xlsx (input-xlsx mode already skips listing)")
    if args.no_stream_files:
        args.stream_records = None
        args.stream_details = None
        args.stream_attachments = None
    if args.resume and args.checkpoint is None:
        ap.error("--resume requires --checkpoint")
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()