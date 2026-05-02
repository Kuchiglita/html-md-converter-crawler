"""
Documentation web crawler.
Downloads HTML pages following links within configured URL boundaries,
saves them locally preserving path structure, and produces a manifest
for subsequent conversion to a single MD file.
"""

import os
import time
import hashlib
import logging
from urllib.parse import urljoin, urlparse, urldefrag
from pathlib import Path
from dataclasses import dataclass, field
from collections import deque
from crawl_stat import CrawlStats, StatLevel

import requests
from bs4 import BeautifulSoup

from generator_detector import DocTypeDetector

logging.basicConfig(
    filename="crawler.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class CrawlConfig:
    """Crawler configuration."""
    start_url: str
    output_dir: str = "crawled_docs"
    max_depth: int = 50
    download_assets: bool = True
    asset_extensions: tuple = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
    page_extensions: tuple = (".html", ".htm", "")
    timeout: int = 30
    delay: float = 0.2
    user_agent: str = "DocCrawler/1.0"
    additional_boundaries: list = field(default_factory=list)
    exclude_patterns: list = field(default_factory=list)


@dataclass
class CrawledPage:
    """Single downloaded page with metadata."""
    url: str  # normalized url
    original_url: str  # original url
    local_path: str
    title: str = ""
    outgoing_links: dict = field(default_factory=dict)
    assets: dict = field(default_factory=dict)
    depth: int = 0
    generator: str = "unknown"
    selector: str = ""


from playwright.sync_api import sync_playwright

class DocCrawler:
    """
    Documentation crawler (BFS).

    Tracks:
    - Page uniqueness via normalized URL set
    - URL boundaries (primary prefix + additional allowed prefixes)
    - Local path structure mirroring original URLs
    - Outgoing links per page for later MD cross-referencing
    """

    def __init__(self, config: CrawlConfig):
        self.config = config

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.user_agent})

        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        self.context = self.browser.new_context(user_agent=config.user_agent)
        self.browser_page = self.context.new_page()

        self.context = self.browser.new_context(user_agent=config.user_agent)
        self.page = self.context.new_page()

        self.visited: set[str] = set()
        self.pages: dict[str, CrawledPage] = {}
        self.assets: dict[str, str] = {}

        # Primary boundary: parent directory of start URL
        parsed = urlparse(config.start_url)
        base_path = parsed.path
        if not base_path.endswith("/"):
            base_path = base_path.rsplit("/", 1)[0] + "/"
        self.primary_boundary = f"{parsed.scheme}://{parsed.netloc}{base_path}"

        # Additional boundaries (other allowed URL prefixes)
        self.boundaries: list[str] = [self.primary_boundary]
        for boundary in config.additional_boundaries:
            if not boundary.startswith(("http://", "https://")):
                boundary = f"https://{boundary}"
            self.boundaries.append(boundary)

        logger.info(f"Crawl boundaries: {self.boundaries}")

        Path(config.output_dir).mkdir(parents=True, exist_ok=True)

        self.stats = CrawlStats(
            active_levels=StatLevel.PROGRESS | StatLevel.LINKS | StatLevel.DEPTH,
            log_every_n=1,
            control_dir=config.output_dir,
        )

        self.detector = DocTypeDetector()

    def normalize_url(self, url: str) -> str:
        """Strip fragment (#section and ? queries) for deduplication."""
        parsed = urlparse(url)
        if parsed.query:
            logger.info(f"Query string detected and stripped: '?{parsed.query}' from {url}")
        return parsed._replace(query="", fragment="").geturl()

    def is_in_scope(self, url: str) -> bool:
        """Check if URL falls within allowed boundaries and not excluded."""
        for pattern in self.config.exclude_patterns:
            if pattern in url:
                return False

        for boundary in self.boundaries:
            if url.startswith(boundary):
                return True
        return False

    def url_to_local_path(self, url: str, is_asset: bool = False) -> str:
        """
        Convert URL to local file path.

        Includes hostname to avoid cross-domain collisions.
        Paths without extension become dir/index.html to prevent
        file/directory name conflicts.
        """
        parsed = urlparse(url)

        netloc = parsed.netloc.replace(":", "_").replace("@", "_")
        path = parsed.path

        if path.startswith("/"):
            path = path[1:]

        full_path = f"{netloc}/{path}" if path else netloc

        if is_asset:
            return full_path

        if full_path.endswith("/"):
            return f"{full_path}index.html"

        path_obj = Path(full_path)
        ext = path_obj.suffix.lower()

        if ext in (".html", ".htm"):
            return full_path
        elif ext and ext in self.config.asset_extensions:
            return full_path
        else:
            return f"{full_path}/index.html"

    def download_page(self, url: str) -> tuple[str | None, str]:
        """
        Download an HTML page.
        Returns (html_text, response, final_url) or (None, response_or_none, url).
        final_url reflects the URL after any redirects.
        """
        try:
            time.sleep(self.config.delay)
            # go by URL
            response = self.page.goto(url, wait_until="networkidle", timeout=self.config.timeout * 1000)

            if not response or not response.ok:
                self.stats.record_skip(url, f"Status: {response.status if response else 'No Response'}")
                return None, url

            final_url = self.browser_page.url
            content_type = response.headers.get("content-type", "")

            was_redirect = final_url != url
            self.stats.record_response(
                content_type=content_type,
                was_redirect=was_redirect,
                redirect_url=final_url if was_redirect else ""
            )

            if "text/html" not in content_type and "application/xhtml" not in content_type:
                self.stats.record_skip(url, f"non-HTML: {content_type}")
                # logger.debug(f"Skipping non-HTML: {url} (content-type: {content_type})")
                return None, final_url

            return self.browser_page.content(), final_url

        except requests.RequestException as e:
            self.stats.record_skip(url, str(e))
            logger.warning(f"Failed to download {url}: {e}")
            return None, url

    def download_asset(self, url: str) -> bool:
        """Download a binary asset (image). Returns success."""
        if url in self.assets:
            return True

        local_path = self.url_to_local_path(url, is_asset=True)
        full_path = Path(self.config.output_dir) / local_path

        try:
            time.sleep(self.config.delay / 2)

            resp = self.session.get(url, timeout=self.config.timeout, stream=True)
            resp.raise_for_status()

            full_path.parent.mkdir(parents=True, exist_ok=True)
            with open(full_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            self.assets[url] = local_path
            logger.info(f"  Asset saved: {local_path}")
            return True

        except requests.RequestException as e:
            logger.warning(f"Failed to download asset {url}: {e}")
            return False

    def extract_links(self, soup: BeautifulSoup, page_url: str) -> list[tuple[str, str]]:
        """Extract in-scope documentation links from page HTML."""
        all_hrefs = soup.find_all("a", href=True)
        total_found = len(all_hrefs)

        results = []
        for a_tag in all_hrefs:
            href = a_tag["href"]

            if href.startswith(("#", "javascript:", "mailto:")):
                continue

            absolute_url = urljoin(page_url, href)
            normalized = self.normalize_url(absolute_url)
            if self.is_in_scope(normalized):
                results.append((normalized, absolute_url))

        in_scope_normalized = [r[0] for r in results]
        new_count = sum(1 for u in in_scope_normalized if u not in self.visited)

        self.stats.record_links(
            found=total_found,
            in_scope=len(results),
            new=new_count,
            duplicate=len(results) - new_count,
        )

        return results

    def extract_assets(self, soup: BeautifulSoup, page_url: str) -> list[str]:
        """Extract image and SVG asset URLs from page HTML."""
        asset_urls = []

        tags_attrs = [("img", "src"), ("object", "data")]

        for tag_name, attr in tags_attrs:
            for tag in soup.find_all(tag_name, **{attr: True}):
                raw_url = tag[attr]
                if raw_url.startswith('data:'): continue  # skip base64

                absolute_url = urljoin(page_url, raw_url)
                # normalize: cut off ?queries and #anchors
                normalized_url = self.normalize_url(absolute_url)

                parsed = urlparse(normalized_url)
                ext = Path(parsed.path).suffix.lower()
                if ext in self.config.asset_extensions:
                    asset_urls.append(normalized_url)

        return list(set(asset_urls))

    def save_html(self, html: str, local_path: str, url: str) -> bool:
        """Save HTML content to disk. Returns success."""
        full_path = Path(self.config.output_dir) / local_path

        try:
            full_path.parent.mkdir(parents=True, exist_ok=True)
            with open(full_path, "w", encoding="utf-8") as f:
                f.write(html)
            return True
        except OSError as e:
            logger.error(f"Failed to save {url} -> {full_path}: {e}")
            return False

    def close(self):
        """Clean up Playwright resources."""
        self.browser_page.close()
        self.context.close()
        self.browser.close()
        self.playwright.stop()

    def crawl(self) -> dict[str, CrawledPage]:
        """
        Main BFS crawl loop.
        Returns dict of url -> CrawledPage with all metadata
        needed for conversion and cross-linking.
        """
        queue: deque[tuple[str, int, str]] = deque()

        start_raw = self.config.start_url
        start_norm = self.normalize_url(start_raw)

        queue.append((start_norm, 0, start_raw))
        self.visited.add(start_norm)

        while queue:
            url, depth, raw_url = queue.popleft()
            self.stats.update_queue_size(len(queue))

            if depth > self.config.max_depth:
                self.stats.record_skip(url, f"max_depth exceeded ({depth})")
                continue

            self.stats.begin_page(url=url, depth=depth, original_url=raw_url)

            html, final_url = self.download_page(url)
            if html is None:
                self.stats.end_page(html_size=0)
                continue

            # Mark redirected URL as visited to avoid re-downloading
            if final_url != url:
                self.visited.add(self.normalize_url(final_url))

            soup = BeautifulSoup(html, "html.parser")
            detection = self.detector.detect(html, url)
            local_path = self.url_to_local_path(final_url)

            title_tag = soup.find("title")
            title = title_tag.get_text(strip=True) if title_tag else ""

            page = CrawledPage(
                url=final_url,
                original_url=raw_url,
                local_path=local_path,
                title=title,
                depth=depth,
                generator = detection.generator_name,
                selector = detection.content_selector
            )

            self.stats.record_structure(soup)

            # Extract and download assets
            if self.config.download_assets:
                asset_urls = self.extract_assets(soup, final_url)
                downloaded = 0
                cached = 0
                for asset_url in asset_urls:
                    if asset_url in self.assets:
                        cached += 1
                        page.assets[asset_url] = self.assets[asset_url]
                    elif self.download_asset(asset_url):
                        downloaded += 1
                        page.assets[asset_url] = self.assets[asset_url]
                self.stats.record_assets(found=len(asset_urls), downloaded=downloaded, cached=cached)

            # Extract links and enqueue new ones
            child_links = self.extract_links(soup, final_url)
            for link_norm, link_raw in child_links:
                link_local = self.url_to_local_path(link_norm)
                page.outgoing_links[link_norm] = link_local

                if link_norm not in self.visited:
                    self.visited.add(link_norm)
                    queue.append((link_norm, depth + 1, link_raw))

            # Save page to disk
            if not self.save_html(html, local_path, final_url):
                self.stats.record_skip(final_url, "failed to save HTML")
                self.stats.end_page(html_size=0)
                continue

            self.pages[final_url] = page
            self.stats.end_page(html_size=len(html.encode("utf-8")))

        print(self.stats.summary())
        self.stats.dump_snapshot(
            str(Path(self.config.output_dir) / "crawl_stats.json")
        )

        return self.pages

    def save_manifest(self, path: str | None = None) -> None:
        """
        Save manifest JSON with all pages, links and assets.
        Used by the converter for cross-page linking in the final MD file.
        """
        import json
        from typing import Any

        if path is None:
            path = str(Path(self.config.output_dir) / "manifest.json")

        manifest: dict[str, Any] = {
            "start_url": self.config.start_url,
            "boundary": self.primary_boundary,
            "pages": {},
            "assets": self.assets,
        }

        for url, page in self.pages.items():
            manifest["pages"][url] = {
                "local_path": page.local_path,
                "original_url": page.original_url,
                "title": page.title,
                "depth": page.depth,
                "generator": page.generator,
                "selector": page.selector,
                "outgoing_links": page.outgoing_links,
                "assets": page.assets,
            }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        logger.info(f"Manifest saved: {path}")


if __name__ == "__main__":
    config = CrawlConfig(
        start_url="https://inlong.apache.org/docs/introduction",
        output_dir="crawled_docs/inlong",
        max_depth=100,
        download_assets=True,
        delay=0.5,
        #additional_boundaries=["https://javadoc.io/doc/org.apache.shiro"],
    )

    crawler = DocCrawler(config)
    try:
        pages = crawler.crawl()
        crawler.save_manifest()
    finally:
        crawler.close()

    # print(f"\n{'=' * 60}")
    # print(f"Crawled {len(pages)} pages")
    # print(f"Downloaded {len(crawler.assets)} assets")
    # print(f"\nPages by depth:")
    # from collections import Counter
    #
    # depth_counts = Counter(p.depth for p in pages.values())
    # for d in sorted(depth_counts):
    #     print(f"  depth {d}: {depth_counts[d]} pages")