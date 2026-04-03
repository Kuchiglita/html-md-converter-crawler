
import os
import hashlib
import logging
from urllib.parse import urljoin, urlparse, urldefrag
from pathlib import Path
from dataclasses import dataclass, field
from collections import deque

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

logging.basicConfig(
    filename="crawler.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class CrawlConfig:
    """Конфигурация краулера."""
    # Стартовая страница
    start_url: str
    # Директория для сохранения
    output_dir: str = "crawled_docs"
    # Максимальная глубина обхода (от стартовой страницы)
    max_depth: int = 5
    # Скачивать ли картинки/диаграммы
    download_assets: bool = True
    # Расширения картинок, которые считаем полезными
    asset_extensions: tuple = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp")
    # Допустимые расширения для страниц (пустое = любые html-подобные)
    page_extensions: tuple = (".html", ".htm", "")
    # Таймаут запроса
    timeout: int = 30
    # Задержка между запросами (секунды) — чтобы не ddos-ить
    delay: float = 0.2
    # User-Agent
    user_agent: str = "DocCrawler/1.0"


@dataclass
class CrawledPage:
    """Результат скачивания одной страницы."""
    url: str
    # Локальный путь относительно output_dir
    local_path: str
    # Заголовок страницы
    title: str = ""
    # Ссылки на другие страницы (url -> local_path)
    outgoing_links: dict = field(default_factory=dict)
    # Ассеты (url -> local_path)
    assets: dict = field(default_factory=dict)
    depth: int = 0


class DocCrawler:
    """
    Краулер для документации.

    Ключевые решения:
    - Уникальность: по нормализованному URL (без фрагмента #...)
    - Границы: остаёмся в пределах "базового пути" стартового URL
    - Структура на диске повторяет структуру URL-путей
    - Каждая страница знает свои исходящие ссылки → потом легко линковать в MD
    """

    def __init__(self, config: CrawlConfig):
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": config.user_agent})

        # Множество уже посещённых URL (нормализованных)
        self.visited: set[str] = set()
        # url → CrawledPage для всех скачанных страниц
        self.pages: dict[str, CrawledPage] = {}
        # url → local_path для всех скачанных ассетов
        self.assets: dict[str, str] = {}

        # Определяем "границу" краулинга — prefix URL
        parsed = urlparse(config.start_url)
        # Берём путь до последнего "/" как базу
        base_path = parsed.path
        if not base_path.endswith("/"):
            base_path = base_path.rsplit("/", 1)[0] + "/"
        self.url_boundary = f"{parsed.scheme}://{parsed.netloc}{base_path}"

        # DEBUG START
        self.links_downloaded = 0
        self.download_decisions: list[str] = []
        # DEBUG END

        logger.info(f"Crawl boundary: {self.url_boundary}")

        # Создаём корневую директорию
        Path(config.output_dir).mkdir(parents=True, exist_ok=True)

    def normalize_url(self, url: str) -> str:
        """Убираем фрагмент (#section), trailing слеши для консистентности."""
        url, _ = urldefrag(url)
        # Убираем trailing slash только если это не директория-index
        return url

    def is_in_scope(self, url: str) -> bool:
        """Проверяем, что URL в пределах нашей документации."""
        return url.startswith(self.url_boundary)

    def url_to_local_path(self, url: str, is_asset: bool = False) -> str:
        """
        Преобразуем URL в локальный путь.

        https://example.com/docs/project/guide/intro.html
        → docs/project/guide/intro.html

        Сохраняем структуру — это критично для линковки потом.
        """
        parsed = urlparse(url)
        path = parsed.path

        # Убираем ведущий слеш
        if path.startswith("/"):
            path = path[1:]

        # Если путь пустой или заканчивается на "/", добавляем index.html
        if not path or path.endswith("/"):
            path = path + "index.html"

        # Если нет расширения и это не ассет — предполагаем .html
        if not is_asset and "." not in Path(path).name:
            path = path + ".html"

        return path

    def download_page(self, url: str) -> str | None:
        """Скачиваем HTML-страницу, возвращаем текст или None."""
        try:
            import time
            time.sleep(self.config.delay)
            resp = self.session.get(url, timeout=self.config.timeout)
            resp.raise_for_status()

            content_type = resp.headers.get("content-type", "")
            if "text/html" not in content_type and "application/xhtml" not in content_type:
                logger.debug(f"Skipping non-HTML: {url} (content-type: {content_type})")
                return None

            return resp.text
        except requests.RequestException as e:
            logger.warning(f"Failed to download {url}: {e}")
            return None

    def download_asset(self, url: str) -> bool:
        """Скачиваем бинарный ассет (картинку). Возвращаем успех."""
        if url in self.assets:
            return True  # уже скачан

        local_path = self.url_to_local_path(url, is_asset=True)
        full_path = Path(self.config.output_dir) / local_path

        try:
            import time
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

    def extract_links(self, soup: BeautifulSoup, page_url: str) -> list[str]:
        """Извлекаем ссылки на другие страницы документации."""
        links = []
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]

            # Пропускаем якоря, javascript, mailto
            if href.startswith(("#", "javascript:", "mailto:")):
                continue

            absolute_url = urljoin(page_url, href)
            normalized = self.normalize_url(absolute_url)

            if self.is_in_scope(normalized):
                links.append(normalized)
                self.download_decisions.append(normalized) # debug

        return links

    def extract_assets(self, soup: BeautifulSoup, page_url: str) -> list[str]:
        """Извлекаем ссылки на картинки и SVG."""
        asset_urls = []

        # <img src="...">
        for img_tag in soup.find_all("img", src=True):
            src = img_tag["src"]
            absolute_url = urljoin(page_url, src)
            parsed = urlparse(absolute_url)
            ext = Path(parsed.path).suffix.lower()
            if ext in self.config.asset_extensions:
                asset_urls.append(absolute_url)

        # <object data="..."> (часто используется для SVG в доках)
        for obj_tag in soup.find_all("object", data=True):
            data = obj_tag["data"]
            absolute_url = urljoin(page_url, data)
            parsed = urlparse(absolute_url)
            ext = Path(parsed.path).suffix.lower()
            if ext in self.config.asset_extensions:
                asset_urls.append(absolute_url)

        return asset_urls

    def save_html(self, html: str, local_path: str) -> None:
        """Сохраняем HTML на диск."""
        full_path = Path(self.config.output_dir) / local_path
        full_path.parent.mkdir(parents=True, exist_ok=True)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(html)

    def crawl(self) -> dict[str, CrawledPage]:
        """
        Основной цикл краулинга (BFS).

        Возвращает словарь url → CrawledPage со всей мета-информацией
        для последующей конвертации и линковки.
        """
        queue: deque[tuple[str, int]] = deque()  # (url, depth)
        start_normalized = self.normalize_url(self.config.start_url)
        queue.append((start_normalized, 0))
        self.visited.add(start_normalized)

        pbar = tqdm(total=None, unit="page", desc="Crawling docs", ascii=True) # debug

        while queue:
            url, depth = queue.popleft()

            if depth > self.config.max_depth:
                print(f"exceeded depth of {self.config.max_depth}")
                continue

            logger.info(f"[depth={depth}] Crawling: {url}")

            html = self.download_page(url)
            if html is None:
                continue

            pbar.update(1) # debug
            pbar.set_postfix({"queue": len(queue)}) # debug

            soup = BeautifulSoup(html, "html.parser")

            # Определяем локальный путь
            local_path = self.url_to_local_path(url)

            # Заголовок страницы
            title_tag = soup.find("title")
            title = title_tag.get_text(strip=True) if title_tag else ""

            # Создаём запись о странице
            page = CrawledPage(
                url=url,
                local_path=local_path,
                title=title,
                depth=depth,
            )

            # Извлекаем и скачиваем ассеты
            if self.config.download_assets:
                for asset_url in self.extract_assets(soup, url):
                    if self.download_asset(asset_url):
                        page.assets[asset_url] = self.assets[asset_url]

            # Извлекаем ссылки
            child_links = self.extract_links(soup, url)
            for link_url in child_links:
                link_local = self.url_to_local_path(link_url)
                page.outgoing_links[link_url] = link_local

                if link_url not in self.visited:
                    self.visited.add(link_url)
                    queue.append((link_url, depth + 1))

            # Сохраняем HTML
            self.save_html(html, local_path)
            self.pages[url] = page

            logger.info(
                f"  Saved: {local_path} | "
                f"Links: {len(page.outgoing_links)} | "
                f"Assets: {len(page.assets)}"
                f"Queue length: {len(queue)}"
            )

        logger.info(f"Crawling complete. Pages: {len(self.pages)}, Assets: {len(self.assets)}")
        # DEBUG START
        pbar.close()
        import json
        with open("debug_list_of_downloads", "w", encoding="utf-8") as f:
            json.dump(self.download_decisions, f, indent=2, ensure_ascii=False)
        # DEBUG END
        return self.pages

    def save_manifest(self, path: str | None = None) -> None:
        """
        Сохраняем манифест — JSON с информацией о всех страницах и связях.
        Это понадобится для этапа конвертации в MD с линковкой.
        """
        import json

        if path is None:
            path = str(Path(self.config.output_dir) / "manifest.json")

        manifest = {
            "start_url": self.config.start_url,
            "boundary": self.url_boundary,
            "pages": {},
            "assets": self.assets,
        }

        for url, page in self.pages.items():
            manifest["pages"][url] = {
                "local_path": page.local_path,
                "title": page.title,
                "depth": page.depth,
                "outgoing_links": page.outgoing_links,
                "assets": page.assets,
            }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, ensure_ascii=False)

        logger.info(f"Manifest saved: {path}")


# тестим
if __name__ == "__main__":
    config = CrawlConfig(
        start_url="https://shiro.apache.org/documentation.html",
        output_dir="crawled_docs/shiro",
        max_depth=10,
        download_assets=True,
        delay=0.3,
    )

    crawler = DocCrawler(config)
    pages = crawler.crawl()
    crawler.save_manifest()

    print(f"\n{'=' * 60}")
    print(f"Crawled {len(pages)} pages")
    print(f"Downloaded {len(crawler.assets)} assets")
    print(f"\nPages by depth:")
    from collections import Counter

    depth_counts = Counter(p.depth for p in pages.values())
    for d in sorted(depth_counts):
        print(f"  depth {d}: {depth_counts[d]} pages")