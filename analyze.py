"""
Downloaded docs analyzer. Analyzes crawled HTML files to understand
documentation structure before converting to a single MD file.
"""

import json
from pathlib import Path
from dataclasses import dataclass, field
from collections import defaultdict
from bs4 import BeautifulSoup
import re


@dataclass
class PageAnalysis:
    """Analysis result for a single page."""
    local_path: str
    url: str = ""

    has_content: bool = True
    content_length: int = 0

    is_spa_shell: bool = False
    has_noscript_warning: bool = False
    js_frameworks_detected: list = field(default_factory=list)

    code_blocks: list = field(default_factory=list)  # [(language, code_preview), ...]
    code_blocks_without_language: int = 0

    has_tables: bool = False
    tables_count: int = 0
    has_images: bool = False
    images: list = field(default_factory=list)  # [(src, alt), ...]

    has_sidebar: bool = False
    has_breadcrumbs: bool = False
    has_footer: bool = False
    has_header_nav: bool = False

    problems: list = field(default_factory=list)


class DocsAnalyzer:
    """Analyzer for crawled documentation."""

    def __init__(self, docs_dir: str, manifest_path: str | None = None):
        self.docs_dir = Path(docs_dir)
        self.manifest = None

        manifest_file = Path(manifest_path) if manifest_path else self.docs_dir / "manifest.json"
        if manifest_file.exists():
            with open(manifest_file, "r", encoding="utf-8") as f:
                self.manifest = json.load(f)

        self.analyses: dict[str, PageAnalysis] = {}

        self.stats = {
            "total_pages": 0,
            "spa_shells": 0,
            "pages_without_content": 0,
            "total_code_blocks": 0,
            "code_blocks_without_lang": 0,
            "total_tables": 0,
            "total_images": 0,
            "languages_found": defaultdict(int),
            "problems_by_type": defaultdict(int),
        }

    def analyze_all(self) -> dict[str, PageAnalysis]:
        """Analyze all HTML files in docs_dir."""
        html_files = list(self.docs_dir.rglob("*.html"))
        print(f"Found {len(html_files)} HTML files")

        for html_path in html_files:
            relative_path = str(html_path.relative_to(self.docs_dir))
            analysis = self.analyze_page(html_path)
            self.analyses[relative_path] = analysis
            self._update_stats(analysis)

        return self.analyses

    def analyze_page(self, html_path: Path) -> PageAnalysis:
        """Analyze a single HTML page."""
        relative_path = str(html_path.relative_to(self.docs_dir))

        url = ""
        if self.manifest:
            for page_url, page_info in self.manifest.get("pages", {}).items():
                if page_info.get("local_path") == relative_path:
                    url = page_url
                    break

        analysis = PageAnalysis(local_path=relative_path, url=url)

        try:
            with open(html_path, "r", encoding="utf-8") as f:
                html = f.read()
        except Exception as e:
            analysis.problems.append(f"Cannot read file: {e}")
            return analysis

        soup = BeautifulSoup(html, "html.parser")

        self._check_spa_shell(soup, html, analysis)
        self._extract_code_blocks(soup, analysis)
        self._check_tables(soup, analysis)
        self._extract_images(soup, analysis)
        self._check_navigation_elements(soup, analysis)
        self._check_content(soup, analysis)

        return analysis

    def _check_spa_shell(self, soup: BeautifulSoup, html: str, analysis: PageAnalysis):
        """Check if page is a JS-rendered SPA shell with no real content."""
        body = soup.find("body")
        if not body:
            analysis.problems.append("No <body> tag")
            analysis.has_content = False
            return

        body_text = body.get_text(strip=True)
        analysis.content_length = len(body_text)

        scripts = soup.find_all("script")
        if len(body_text) < 200 and len(scripts) > 3:
            analysis.is_spa_shell = True
            analysis.problems.append("Likely SPA shell (little text, many scripts)")

        spa_markers = ["root", "app", "__next", "__nuxt", "__docusaurus"]
        for marker in spa_markers:
            container = soup.find(id=marker)
            if container and len(container.get_text(strip=True)) < 100:
                analysis.is_spa_shell = True
                analysis.js_frameworks_detected.append(marker)

        noscript = soup.find("noscript")
        if noscript:
            noscript_text = noscript.get_text().lower()
            if "javascript" in noscript_text or "enable" in noscript_text:
                analysis.has_noscript_warning = True
                analysis.problems.append("Has noscript warning - may need JS")

    def _extract_code_blocks(self, soup: BeautifulSoup, analysis: PageAnalysis):
        """Extract code block info: language and preview."""
        code_elements = []

        for pre in soup.find_all("pre"):
            code = pre.find("code")
            if code:
                code_elements.append(code)
            else:
                code_elements.append(pre)

        for code in soup.find_all("code", class_=True):
            if code.parent.name != "pre":
                classes = code.get("class", [])
                if any(c.startswith(("language-", "lang-", "highlight-")) for c in classes):
                    code_elements.append(code)

        for elem in code_elements:
            language = self._detect_code_language(elem)
            code_text = elem.get_text()
            preview = code_text[:100].replace("\n", "\\n") if code_text else ""

            analysis.code_blocks.append((language, preview))

            if not language:
                analysis.code_blocks_without_language += 1

    def _detect_code_language(self, elem) -> str:
        """Detect code language from element classes or attributes."""
        classes = elem.get("class", [])

        for cls in classes:
            for prefix in ("language-", "lang-", "highlight-", "source-"):
                if cls.startswith(prefix):
                    return cls[len(prefix):]

            # SyntaxHighlighter format: brush: java
            if cls.startswith("brush:"):
                return cls.split(":")[1].strip()

        lang = elem.get("data-lang") or elem.get("data-language")
        if lang:
            return lang

        parent = elem.parent
        if parent:
            parent_classes = parent.get("class", [])
            for cls in parent_classes:
                for prefix in ("language-", "highlight-", "source-"):
                    if cls.startswith(prefix):
                        return cls[len(prefix):]

        return ""

    def _check_tables(self, soup: BeautifulSoup, analysis: PageAnalysis):
        """Check tables, flag complex ones with colspan/rowspan."""
        tables = soup.find_all("table")
        analysis.tables_count = len(tables)
        analysis.has_tables = len(tables) > 0

        for table in tables:
            if table.find(attrs={"colspan": True}) or table.find(attrs={"rowspan": True}):
                analysis.problems.append("Complex table with colspan/rowspan - may not convert well to MD")
                break

    def _extract_images(self, soup: BeautifulSoup, analysis: PageAnalysis):
        """Extract image sources and alt texts."""
        for img in soup.find_all("img"):
            src = img.get("src", "")
            alt = img.get("alt", "")
            analysis.images.append((src, alt))

        for obj in soup.find_all("object", type="image/svg+xml"):
            data = obj.get("data", "")
            analysis.images.append((data, "SVG object"))

        analysis.has_images = len(analysis.images) > 0

    def _check_navigation_elements(self, soup: BeautifulSoup, analysis: PageAnalysis):
        """Detect navigation elements: sidebar, breadcrumbs, footer, header nav."""
        sidebar_selectors = [
            ("nav", {}),
            ("aside", {}),
            ("div", {"class": re.compile(r"sidebar|sidenav|toc|menu", re.I)}),
            ("div", {"id": re.compile(r"sidebar|sidenav|toc|menu", re.I)}),
        ]
        for tag, attrs in sidebar_selectors:
            if soup.find(tag, attrs):
                analysis.has_sidebar = True
                break

        if soup.find(class_=re.compile(r"breadcrumb", re.I)):
            analysis.has_breadcrumbs = True

        if soup.find("footer") or soup.find(class_=re.compile(r"footer", re.I)):
            analysis.has_footer = True

        header = soup.find("header")
        if header and header.find("nav"):
            analysis.has_header_nav = True

    def _check_content(self, soup: BeautifulSoup, analysis: PageAnalysis):
        """Check if page has meaningful main content."""
        main_selectors = [
            ("main", {}),
            ("article", {}),
            ("div", {"role": "main"}),
            ("div", {"class": re.compile(r"content|main|article|body", re.I)}),
            ("div", {"id": re.compile(r"content|main|article", re.I)}),
        ]

        main_content = None
        for tag, attrs in main_selectors:
            main_content = soup.find(tag, attrs)
            if main_content:
                break

        if main_content:
            text = main_content.get_text(strip=True)
            if len(text) < 50:
                analysis.has_content = False
                analysis.problems.append("Main content area has very little text")
        else:
            body = soup.find("body")
            if body:
                text = body.get_text(strip=True)
                if len(text) < 100:
                    analysis.has_content = False
                    analysis.problems.append("Page has very little text content")

    def _update_stats(self, analysis: PageAnalysis):
        """Update global stats with data from a single page analysis."""
        self.stats["total_pages"] += 1

        if analysis.is_spa_shell:
            self.stats["spa_shells"] += 1

        if not analysis.has_content:
            self.stats["pages_without_content"] += 1

        self.stats["total_code_blocks"] += len(analysis.code_blocks)
        self.stats["code_blocks_without_lang"] += analysis.code_blocks_without_language
        self.stats["total_tables"] += analysis.tables_count
        self.stats["total_images"] += len(analysis.images)

        for lang, _ in analysis.code_blocks:
            if lang:
                self.stats["languages_found"][lang] += 1

        for problem in analysis.problems:
            problem_type = problem.split(":")[0].split("-")[0].strip()
            self.stats["problems_by_type"][problem_type] += 1

    def print_summary(self):
        """Print aggregated statistics."""
        print("\n" + "=" * 60)
        print("DOCUMENTATION ANALYSIS SUMMARY")
        print("=" * 60)

        print(f"\nTotal pages: {self.stats['total_pages']}")
        print(f"SPA shells (JS-rendered): {self.stats['spa_shells']}")
        print(f"Pages without content: {self.stats['pages_without_content']}")

        print(f"\nCode blocks: {self.stats['total_code_blocks']}")
        print(f"  Without language: {self.stats['code_blocks_without_lang']}")

        if self.stats["languages_found"]:
            print("\n  Languages found:")
            for lang, count in sorted(self.stats["languages_found"].items(),
                                      key=lambda x: -x[1])[:15]:
                print(f"    {lang}: {count}")

        print(f"\nTables: {self.stats['total_tables']}")
        print(f"Images: {self.stats['total_images']}")

        if self.stats["problems_by_type"]:
            print("\nProblems found:")
            for problem, count in sorted(self.stats["problems_by_type"].items(),
                                         key=lambda x: -x[1]):
                print(f"  {problem}: {count}")

        print("=" * 60)

    def print_problems(self, limit: int = 50):
        """Print pages that have problems, up to limit."""
        print("\n" + "=" * 60)
        print("PAGES WITH PROBLEMS")
        print("=" * 60)

        count = 0
        for path, analysis in self.analyses.items():
            if analysis.problems:
                print(f"\n{path}")
                if analysis.url:
                    print(f"  URL: {analysis.url}")
                for problem in analysis.problems:
                    print(f"  ⚠ {problem}")
                count += 1
                if count >= limit:
                    remaining = sum(1 for a in self.analyses.values() if a.problems) - limit
                    if remaining > 0:
                        print(f"\n... and {remaining} more pages with problems")
                    break

    def print_spa_shells(self):
        """Print all detected SPA shell pages."""
        print("\n" + "=" * 60)
        print("SPA SHELL PAGES (JS-rendered, no content)")
        print("=" * 60)

        for path, analysis in self.analyses.items():
            if analysis.is_spa_shell:
                print(f"\n{path}")
                if analysis.url:
                    print(f"  URL: {analysis.url}")
                print(f"  Content length: {analysis.content_length} chars")
                if analysis.js_frameworks_detected:
                    print(f"  Frameworks: {analysis.js_frameworks_detected}")
                for problem in analysis.problems:
                    print(f"    {problem}")

    def save_report(self, path: str = "analysis_report.json"):
        """Save full analysis report to JSON."""
        report = {
            "stats": {
                **self.stats,
                "languages_found": dict(self.stats["languages_found"]),
                "problems_by_type": dict(self.stats["problems_by_type"]),
            },
            "pages": {}
        }

        for page_path, analysis in self.analyses.items():
            report["pages"][page_path] = {
                "url": analysis.url,
                "has_content": analysis.has_content,
                "content_length": analysis.content_length,
                "is_spa_shell": analysis.is_spa_shell,
                "code_blocks": len(analysis.code_blocks),
                "code_blocks_without_language": analysis.code_blocks_without_language,
                "languages": [lang for lang, _ in analysis.code_blocks if lang],
                "tables_count": analysis.tables_count,
                "images_count": len(analysis.images),
                "has_sidebar": analysis.has_sidebar,
                "has_breadcrumbs": analysis.has_breadcrumbs,
                "has_footer": analysis.has_footer,
                "problems": analysis.problems,
            }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

        print(f"\nFull report saved to: {path}")


if __name__ == "__main__":
    analyzer = DocsAnalyzer("crawled_docs/shiro")
    analyzer.analyze_all()
    analyzer.print_summary()
    analyzer.print_problems(limit=20)
    analyzer.print_spa_shells()
    analyzer.save_report("crawled_docs/shiro/analysis_report.json")