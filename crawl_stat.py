"""
Crawl statistics collector with configurable output levels
and interactive control via signals or control files.
"""

import os
import time
import json
import signal
import threading
from dataclasses import dataclass, field
from collections import defaultdict
from pathlib import Path
from enum import Flag, auto
from typing import Optional


class StatLevel(Flag):
    """Which statistics to output."""
    NONE = 0
    PROGRESS = auto()
    LINKS = auto()
    ASSETS = auto()
    SIZES = auto()
    DEPTH = auto()
    STRUCTURE = auto()
    TIMING = auto()
    ALL = PROGRESS | LINKS | ASSETS | SIZES | DEPTH | STRUCTURE | TIMING


@dataclass
class PageStats:
    """Per-page statistics."""
    url: str
    depth: int
    html_size_bytes: int = 0
    num_links_found: int = 0
    num_links_in_scope: int = 0
    num_links_new: int = 0
    num_links_duplicate: int = 0
    num_assets_found: int = 0
    num_assets_downloaded: int = 0
    num_assets_cached: int = 0
    response_time_ms: float = 0
    content_type: str = ""
    was_redirect: bool = False
    redirect_url: str = ""
    has_code_blocks: bool = False
    num_code_blocks: int = 0
    has_tables: bool = False
    num_tables: int = 0
    has_navigation: bool = False
    has_footer: bool = False


class CrawlStats:
    """
    Central crawl statistics collector.

    Usage:
        stats = CrawlStats(active_levels=StatLevel.ALL)
        stats.begin_page(url, depth)
        stats.record_links(found=15, in_scope=10, new=7, duplicate=3)
        stats.end_page(html_size=len(html))
        stats.log()
        stats.dump_snapshot("stats.json")

    Interactive control:
        - SIGUSR1 signal: toggle verbose (Unix only)
        - SIGUSR2 signal: dump snapshot (Unix only)
        - Create file _dump_stats in control_dir: dump snapshot and delete file
        - Create file _verbose_on / _verbose_off: switch levels
        - Create file _set_levels_PROGRESS_LINKS: set specific levels
    """

    def __init__(
            self,
            active_levels: StatLevel = StatLevel.PROGRESS | StatLevel.LINKS,
            log_every_n: int = 1,
            control_dir: str = ".",
            logger=None,
    ):
        self.active_levels = active_levels
        self.log_every_n = log_every_n
        self.control_dir = Path(control_dir)
        self.logger = logger or self._default_logger()

        # Global counters
        self.total_pages_processed = 0
        self.total_pages_skipped = 0
        self.total_links_found = 0
        self.total_links_in_scope = 0
        self.total_links_new = 0
        self.total_links_duplicate = 0
        self.total_assets_found = 0
        self.total_assets_downloaded = 0
        self.total_assets_cached = 0
        self.total_bytes_html = 0
        self.total_bytes_assets = 0
        self.total_request_time_ms = 0.0

        # Structural statistics
        self.pages_with_code = 0
        self.pages_with_tables = 0
        self.total_code_blocks = 0
        self.total_tables = 0
        self.content_types_seen: dict[str, int] = defaultdict(int)
        self.redirects: list[tuple[str, str]] = []
        self.depth_distribution: dict[int, int] = defaultdict(int)
        self.errors: list[tuple[str, str]] = []

        # Current page (begin/end pattern)
        self._current_page: Optional[PageStats] = None
        self._current_start_time: float = 0

        # Full per-page history
        self.page_history: list[PageStats] = []

        # Queue tracking (updated externally)
        self.current_queue_size = 0
        self.peak_queue_size = 0

        self._setup_signals()

    def _default_logger(self):
        import logging
        log = logging.getLogger("crawl_stats")
        if not log.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                "%(asctime)s [STATS] %(message)s", datefmt="%H:%M:%S"
            ))
            log.addHandler(handler)
            log.setLevel(logging.INFO)
        return log

    def _setup_signals(self):
        """Register SIGUSR1/2 for interactive control (Unix only)."""
        try:
            signal.signal(signal.SIGUSR1, self._handle_toggle_verbose)
            signal.signal(signal.SIGUSR2, self._handle_dump)
        except (AttributeError, OSError):
            pass

    def _handle_toggle_verbose(self, signum, frame):
        if self.active_levels == StatLevel.ALL:
            self.active_levels = StatLevel.PROGRESS
            self.logger.info(">>> Switched to PROGRESS only")
        else:
            self.active_levels = StatLevel.ALL
            self.logger.info(">>> Switched to ALL stats")

    def _handle_dump(self, signum, frame):
        self.dump_snapshot()
        self.logger.info(">>> Snapshot dumped via signal")

    def check_control_files(self):
        """
        Check for control files in control_dir. Call periodically from crawl loop.

        Control files:
            _dump_stats      -> dump snapshot + delete file
            _verbose_on      -> set ALL levels
            _verbose_off     -> set PROGRESS only
            _set_levels_X_Y  -> set specific levels (e.g. _set_levels_PROGRESS_LINKS_DEPTH)
        """
        dump_file = self.control_dir / "_dump_stats"
        if dump_file.exists():
            self.dump_snapshot()
            dump_file.unlink()
            self.logger.info(">>> Snapshot dumped via control file")

        verbose_on = self.control_dir / "_verbose_on"
        if verbose_on.exists():
            self.active_levels = StatLevel.ALL
            verbose_on.unlink()
            self.logger.info(">>> Verbose ON")

        verbose_off = self.control_dir / "_verbose_off"
        if verbose_off.exists():
            self.active_levels = StatLevel.PROGRESS
            verbose_off.unlink()
            self.logger.info(">>> Verbose OFF")

        for f in self.control_dir.glob("_set_levels_*"):
            parts = f.name.replace("_set_levels_", "").split("_")
            new_level = StatLevel.NONE
            for part in parts:
                try:
                    new_level |= StatLevel[part]
                except KeyError:
                    pass
            self.active_levels = new_level
            f.unlink()
            self.logger.info(f">>> Levels set to: {self.active_levels}")

    # --- Per-page lifecycle ---

    def begin_page(self, url: str, depth: int):
        """Call before starting to process a page."""
        self._current_start_time = time.time()
        self._current_page = PageStats(url=url, depth=depth)

    def record_response(self, content_type: str, was_redirect: bool = False,
                        redirect_url: str = ""):
        """Record HTTP response metadata."""
        if self._current_page:
            self._current_page.content_type = content_type
            self._current_page.was_redirect = was_redirect
            self._current_page.redirect_url = redirect_url
            self.content_types_seen[content_type] += 1
            if was_redirect:
                self.redirects.append((self._current_page.url, redirect_url))

    def record_links(self, found: int, in_scope: int, new: int, duplicate: int):
        """Record link extraction results for current page."""
        if self._current_page:
            self._current_page.num_links_found = found
            self._current_page.num_links_in_scope = in_scope
            self._current_page.num_links_new = new
            self._current_page.num_links_duplicate = duplicate

        self.total_links_found += found
        self.total_links_in_scope += in_scope
        self.total_links_new += new
        self.total_links_duplicate += duplicate

    def record_assets(self, found: int, downloaded: int, cached: int):
        """Record asset download results for current page."""
        if self._current_page:
            self._current_page.num_assets_found = found
            self._current_page.num_assets_downloaded = downloaded
            self._current_page.num_assets_cached = cached

        self.total_assets_found += found
        self.total_assets_downloaded += downloaded
        self.total_assets_cached += cached

    def record_structure(self, soup):
        """Analyze structural elements of the page (code, tables, nav, footer)."""
        if not self._current_page:
            return

        code_blocks = soup.find_all(["code", "pre"])
        self._current_page.num_code_blocks = len(code_blocks)
        self._current_page.has_code_blocks = len(code_blocks) > 0

        tables = soup.find_all("table")
        self._current_page.num_tables = len(tables)
        self._current_page.has_tables = len(tables) > 0

        nav = soup.find_all(["nav", "aside"]) or soup.find_all(
            class_=lambda c: c and any(
                x in str(c).lower() for x in ["sidebar", "toc", "nav", "menu"]
            )
        )
        self._current_page.has_navigation = len(nav) > 0

        footer = soup.find_all("footer") or soup.find_all(
            class_=lambda c: c and "footer" in str(c).lower()
        )
        self._current_page.has_footer = len(footer) > 0

        if self._current_page.has_code_blocks:
            self.pages_with_code += 1
            self.total_code_blocks += self._current_page.num_code_blocks
        if self._current_page.has_tables:
            self.pages_with_tables += 1
            self.total_tables += self._current_page.num_tables

    def record_skip(self, url: str, reason: str):
        """Record a skipped page with reason."""
        self.total_pages_skipped += 1
        self.errors.append((url, reason))

    def end_page(self, html_size: int):
        """Call after finishing page processing."""
        elapsed = (time.time() - self._current_start_time) * 1000

        if self._current_page:
            self._current_page.html_size_bytes = html_size
            self._current_page.response_time_ms = elapsed
            self.page_history.append(self._current_page)

        self.total_pages_processed += 1
        self.total_bytes_html += html_size
        self.total_request_time_ms += elapsed
        self.depth_distribution[self._current_page.depth if self._current_page else 0] += 1

        if self.total_pages_processed % self.log_every_n == 0:
            self.log()

        if self.total_pages_processed % 10 == 0:
            self.check_control_files()

        self._current_page = None

    def update_queue_size(self, size: int):
        """Update current queue size (call from crawl loop)."""
        self.current_queue_size = size
        self.peak_queue_size = max(self.peak_queue_size, size)

    # --- Output ---

    def log(self):
        """Output statistics according to active levels."""
        parts = []

        if StatLevel.PROGRESS in self.active_levels:
            parts.append(
                f"pages={self.total_pages_processed} "
                f"queue={self.current_queue_size} "
                f"peak_q={self.peak_queue_size} "
                f"skipped={self.total_pages_skipped}"
            )

        if StatLevel.LINKS in self.active_levels and self._current_page:
            p = self._current_page
            parts.append(
                f"links: found={p.num_links_found} "
                f"scope={p.num_links_in_scope} "
                f"new={p.num_links_new} "
                f"dup={p.num_links_duplicate}"
            )

        if StatLevel.ASSETS in self.active_levels and self._current_page:
            p = self._current_page
            parts.append(
                f"assets: found={p.num_assets_found} "
                f"dl={p.num_assets_downloaded} "
                f"cached={p.num_assets_cached}"
            )

        if StatLevel.SIZES in self.active_levels and self._current_page:
            size_kb = self._current_page.html_size_bytes / 1024
            total_kb = self.total_bytes_html / 1024
            parts.append(f"size={size_kb:.1f}KB total_html={total_kb:.1f}KB")

        if StatLevel.DEPTH in self.active_levels and self._current_page:
            parts.append(f"depth={self._current_page.depth}")

        if StatLevel.STRUCTURE in self.active_levels and self._current_page:
            p = self._current_page
            flags = []
            if p.has_code_blocks:
                flags.append(f"code={p.num_code_blocks}")
            if p.has_tables:
                flags.append(f"tables={p.num_tables}")
            if p.has_navigation:
                flags.append("nav")
            if p.has_footer:
                flags.append("footer")
            if p.was_redirect:
                flags.append(f"redirect->{p.redirect_url}")
            if flags:
                parts.append("struct: " + " ".join(flags))

        if StatLevel.TIMING in self.active_levels and self._current_page:
            avg = (self.total_request_time_ms / max(self.total_pages_processed, 1))
            parts.append(
                f"time={self._current_page.response_time_ms:.0f}ms "
                f"avg={avg:.0f}ms"
            )

        if parts:
            msg = " | ".join(parts)
            self.logger.info(msg)

    def dump_snapshot(self, path: str = "crawl_stats_snapshot.json"):
        """Full statistics dump to JSON."""
        snapshot = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "totals": {
                "pages_processed": self.total_pages_processed,
                "pages_skipped": self.total_pages_skipped,
                "links_found": self.total_links_found,
                "links_in_scope": self.total_links_in_scope,
                "links_new": self.total_links_new,
                "links_duplicate": self.total_links_duplicate,
                "assets_found": self.total_assets_found,
                "assets_downloaded": self.total_assets_downloaded,
                "assets_cached": self.total_assets_cached,
                "html_bytes": self.total_bytes_html,
                "asset_bytes": self.total_bytes_assets,
                "avg_request_ms": (
                        self.total_request_time_ms /
                        max(self.total_pages_processed, 1)
                ),
                "pages_with_code": self.pages_with_code,
                "pages_with_tables": self.pages_with_tables,
                "total_code_blocks": self.total_code_blocks,
                "total_tables": self.total_tables,
            },
            "queue": {
                "current": self.current_queue_size,
                "peak": self.peak_queue_size,
            },
            "depth_distribution": dict(self.depth_distribution),
            "content_types": dict(self.content_types_seen),
            "redirects": self.redirects[:50],
            "errors": self.errors[:50],
            "active_levels": str(self.active_levels),
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2, ensure_ascii=False)

    def summary(self) -> str:
        """Final human-readable summary string."""
        avg_links = self.total_links_found / max(self.total_pages_processed, 1)
        avg_size = self.total_bytes_html / max(self.total_pages_processed, 1) / 1024
        lines = [
            "=" * 60,
            "CRAWL SUMMARY",
            "=" * 60,
            f"Pages processed:     {self.total_pages_processed}",
            f"Pages skipped:       {self.total_pages_skipped}",
            f"Total HTML:          {self.total_bytes_html / 1024 / 1024:.2f} MB",
            f"Avg page size:       {avg_size:.1f} KB",
            f"Avg links/page:      {avg_links:.1f}",
            f"Unique assets:       {self.total_assets_downloaded}",
            f"Peak queue size:     {self.peak_queue_size}",
            f"Pages with code:     {self.pages_with_code} ({self.total_code_blocks} blocks)",
            f"Pages with tables:   {self.pages_with_tables} ({self.total_tables} tables)",
            f"Content types seen:  {dict(self.content_types_seen)}",
            f"Redirects:           {len(self.redirects)}",
            f"Errors:              {len(self.errors)}",
            f"Depth distribution:  {dict(sorted(self.depth_distribution.items()))}",
            "=" * 60,
        ]
        return "\n".join(lines)