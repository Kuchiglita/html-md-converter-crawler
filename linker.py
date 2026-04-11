import re
import json
from pathlib import Path
from urllib.parse import urlparse, urljoin
from dataclasses import dataclass, field
from bs4 import BeautifulSoup, Tag
from markdownify import MarkdownConverter
from typing import Optional
from collections import defaultdict


@dataclass
class ConverterConfig:
    """Converter configuration."""
    docs_dir: str
    output_file: str = "documentation.md"
    manifest_path: Optional[str] = None
    exclude_patterns: list = field(default_factory=lambda: [
        "/xref/",
        "/cpd.html",
        "/dependencies.html",
        "/apidocs/",
    ])
    min_content_length: int = 100
    guess_code_language: bool = True
    default_code_language: str = "text"
    page_separator: str = "\n\n---\n\n"


class CodeLanguageGuesser:
    """Guesses code language by content heuristics."""

    @staticmethod
    def guess(code: str) -> str:
        code_stripped = code.strip()
        code_lower = code_stripped.lower()

        if not code_stripped:
            return "text"

        if code_stripped.startswith("<?xml") or code_stripped.startswith("<!DOCTYPE"):
            return "xml"
        if re.match(r'^\s*<[a-zA-Z]', code_stripped):
            if code_stripped.count('<') > 1 or code_stripped.endswith('>'):
                if '<bean' in code_lower or '<beans' in code_lower:
                    return "xml"
                if '<html' in code_lower or '<div' in code_lower:
                    return "html"
                return "xml"

        if (code_stripped.startswith('{') and code_stripped.endswith('}')) or \
                (code_stripped.startswith('[') and code_stripped.endswith(']')):
            if '"' in code_stripped and ':' in code_stripped:
                return "json"

        java_patterns = [
            r'\b(public|private|protected)\s+(static\s+)?(class|interface|enum|void|final)',
            r'\bimport\s+(java|javax|org\.apache)\.',
            r'\bnew\s+[A-Z][a-zA-Z]*\s*\(',
            r'@(Override|Test|Autowired|Bean|Controller)',
            r'\bextends\s+[A-Z]',
            r'\bimplements\s+[A-Z]',
        ]
        for pattern in java_patterns:
            if re.search(pattern, code_stripped):
                return "java"

        if re.search(r'^\[(main|users|roles|urls)\]', code_stripped, re.MULTILINE):
            return "ini"

        if re.search(r'^[a-zA-Z][a-zA-Z0-9_.]*\s*=\s*.+', code_stripped, re.MULTILINE):
            if not re.search(r'[{};]', code_stripped):
                return "properties"

        if code_stripped.startswith('$ ') or code_stripped.startswith('# '):
            return "bash"
        shell_commands = ['chmod', 'mkdir', 'cd', 'export', 'echo', 'curl', 'wget', 'mvn', 'gradle']
        if any(re.search(rf'\b{cmd}\b', code_lower) for cmd in shell_commands):
            return "bash"

        if 'def ' in code_stripped or "'" in code_stripped and '{' in code_stripped:
            if 'compile ' in code_lower or 'implementation ' in code_lower:
                return "groovy"

        sql_keywords = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE TABLE', 'ALTER TABLE']
        if any(kw in code_stripped.upper() for kw in sql_keywords):
            return "sql"

        return "text"


class AnchorPreservingConverter(MarkdownConverter):
    """Extended markdownify converter with anchor preservation and improved code handling."""

    def __init__(self, page_id: str = "", guess_language: bool = True, **kwargs):
        super().__init__(**kwargs)
        self.page_id = page_id
        self.guess_language = guess_language

    def convert_a(self, el, text, **_):
        """Preserve named anchors, convert regular links."""
        href = el.get('href', '')
        anchor_id = el.get('name') or el.get('id')

        if anchor_id and not href:
            full_id = f"{self.page_id}--{anchor_id}" if self.page_id else anchor_id
            return f'<a id="{full_id}"></a>{text}'

        if href:
            title = el.get('title', '')
            if title:
                return f'[{text}]({href} "{title}")'
            return f'[{text}]({href})'

        return text

    def convert_pre(self, el, text, **_):
        """Extract code and language from pre blocks."""
        code_el = el.find('code')
        if code_el:
            code_text = code_el.get_text()
            language = self._detect_language(code_el)
        else:
            code_text = el.get_text()
            language = self._detect_language(el)

        if not language and self.guess_language:
            language = CodeLanguageGuesser.guess(code_text)

        code_text = code_text.strip('\n')

        return f'\n\n```{language}\n{code_text}\n```\n\n'

    def convert_code(self, el, text, **_):
        """Inline code, skipped if inside pre (handled by convert_pre)."""
        if el.parent and el.parent.name == 'pre':
            return text
        return f'`{text}`'

    def convert_table(self, el, text, **_):
        """Simple tables to MD, complex ones (colspan/rowspan) stay as HTML."""
        if el.find(attrs={'colspan': True}) or el.find(attrs={'rowspan': True}):
            clean_html = self._clean_table_html(el)
            return f'\n\n{clean_html}\n\n'

        return self._convert_simple_table(el)

    def _detect_language(self, el) -> str:
        """Detect code language from element class or data attributes."""
        classes = el.get('class', [])
        if isinstance(classes, str):
            classes = classes.split()

        for cls in classes:
            for prefix in ('language-', 'lang-', 'highlight-', 'source-', 'brush:'):
                if cls.startswith(prefix):
                    lang = cls[len(prefix):].strip(';').lower()
                    lang_map = {
                        'js': 'javascript',
                        'py': 'python',
                        'sh': 'bash',
                        'shell': 'bash',
                        'zsh': 'bash',
                    }
                    return lang_map.get(lang, lang)

        lang = el.get('data-lang') or el.get('data-language')
        if lang:
            return lang.lower()

        if el.parent:
            return self._detect_language(el.parent)

        return ""

    def _clean_table_html(self, table_el) -> str:
        """Strip styling attributes from complex table, keep only structure."""
        table_copy = BeautifulSoup(str(table_el), 'html.parser').find('table')

        for tag in table_copy.find_all(True):
            attrs_to_keep = ['colspan', 'rowspan', 'href']
            for attr in list(tag.attrs.keys()):
                if attr not in attrs_to_keep:
                    del tag[attr]

        return str(table_copy)

    def _convert_simple_table(self, el) -> str:
        """Convert simple table to MD pipe format."""
        rows = el.find_all('tr')
        if not rows:
            return ''

        md_rows = []
        header_done = False

        for row in rows:
            cells = row.find_all(['th', 'td'])
            cell_texts = []
            for cell in cells:
                text = cell.get_text(strip=True).replace('|', '\\|').replace('\n', ' ')
                cell_texts.append(text)

            if not cell_texts:
                continue

            md_row = '| ' + ' | '.join(cell_texts) + ' |'
            md_rows.append(md_row)

            if not header_done:
                separator = '| ' + ' | '.join(['---'] * len(cell_texts)) + ' |'
                md_rows.append(separator)
                header_done = True

        return '\n\n' + '\n'.join(md_rows) + '\n\n'


class DocumentationConverter:
    """Converts crawled documentation into a single MD file."""

    def __init__(self, config: ConverterConfig):
        self.config = config
        self.docs_dir = Path(config.docs_dir)

        manifest_path = config.manifest_path or (self.docs_dir / "manifest.json")
        with open(manifest_path, "r", encoding="utf-8") as f:
            self.manifest = json.load(f)

        self.url_to_page_id: dict[str, str] = {}
        self.path_to_page_id: dict[str, str] = {}
        self.converted_pages: list[dict] = []

        self._build_page_ids()

    def _build_page_ids(self):
        """Build URL -> page_id and local_path -> page_id mappings from manifest."""
        for url, page_info in self.manifest.get("pages", {}).items():
            local_path = page_info["local_path"]

            if self._should_exclude(local_path):
                continue

            page_id = self._path_to_id(local_path)
            self.url_to_page_id[url] = page_id
            self.path_to_page_id[local_path] = page_id

    def _path_to_id(self, local_path: str) -> str:
        """Convert local file path to a valid MD anchor id.

        Example: shiro.apache.org/authentication.html -> shiro-apache-org-authentication
        """
        page_id = local_path.replace('\\', '/').replace('/', '-').replace('.html', '')
        page_id = re.sub(r'[^a-zA-Z0-9_-]', '-', page_id)
        page_id = re.sub(r'-+', '-', page_id).strip('-').lower()
        return page_id

    def _should_exclude(self, local_path: str) -> bool:
        """Check if page matches any exclude pattern."""
        path_normalized = local_path.replace('\\', '/')
        for pattern in self.config.exclude_patterns:
            if pattern in path_normalized:
                return True
        return False

    def _fix_internal_links(self, soup: BeautifulSoup, current_url: str, current_page_id: str):
        """Replace internal href links with anchor references into the single MD file."""
        for a in soup.find_all('a', href=True):
            href = a['href']

            if href.startswith(('http://', 'https://', 'mailto:', 'javascript:')):
                parsed = urlparse(href)
                full_url = href.split('#')[0]

                if full_url in self.url_to_page_id:
                    target_page_id = self.url_to_page_id[full_url]
                    fragment = parsed.fragment
                    if fragment:
                        a['href'] = f"#{target_page_id}--{fragment}"
                    else:
                        a['href'] = f"#{target_page_id}"
                continue

            if href.startswith('#'):
                anchor = href[1:]
                a['href'] = f"#{current_page_id}--{anchor}"
            else:
                resolved = urljoin(current_url, href)
                base_url = resolved.split('#')[0]
                fragment = urlparse(resolved).fragment

                if base_url in self.url_to_page_id:
                    target_page_id = self.url_to_page_id[base_url]
                    if fragment:
                        a['href'] = f"#{target_page_id}--{fragment}"
                    else:
                        a['href'] = f"#{target_page_id}"

    def _fix_image_paths(self, soup: BeautifulSoup, current_local_path: str):
        """Make image src paths relative to docs_dir."""
        current_dir = Path(current_local_path).parent

        for img in soup.find_all('img', src=True):
            src = img['src']

            if src.startswith(('http://', 'https://', 'data:')):
                continue

            if src.startswith('/'):
                img['src'] = src[1:]
            else:
                resolved = (current_dir / src).as_posix()
                img['src'] = str(Path(resolved)).replace('\\', '/')

    def _extract_main_content(self, soup: BeautifulSoup) -> Optional[Tag]:
        """Extract page body, stripping only scripts and styles."""
        for el in soup.select('script, style'):
            el.decompose()

        selectors = [
            ('main', {}),
            ('article', {}),
            ('div', {'role': 'main'}),
            ('div', {'class': 'content'}),
            ('div', {'class': 'document'}),
            ('div', {'class': 'body'}),
            ('div', {'id': 'content'}),
            ('body', {}),
        ]

        for tag, attrs in selectors:
            content = soup.find(tag, **attrs)
            if content:
                text = content.get_text(strip=True)
                if len(text) >= self.config.min_content_length:
                    return content

        return None

    def _dl_to_ul(self, soup: BeautifulSoup, dl_tag: Tag) -> Tag:
        """Convert <dl>/<dt>/<dd> to <ul>/<li> recursively."""
        ul = soup.new_tag('ul')
        current_li = None

        for child in dl_tag.find_all(['dt', 'dd'], recursive=False):
            if child.name == 'dt':
                current_li = soup.new_tag('li')
                for c in list(child.children):
                    current_li.append(c.extract())
                ul.append(current_li)
            elif child.name == 'dd' and current_li is not None:
                nested_dl = child.find('dl', recursive=False)
                if nested_dl:
                    current_li.append(self._dl_to_ul(soup, nested_dl))
                else:
                    for c in list(child.children):
                        current_li.append(c.extract())

        return ul

    def convert_page(self, local_path: str, url: str, title: str) -> tuple[Optional[str], str]:
        """Convert a single HTML page to MD. Returns (markdown, skip_reason)."""
        full_path = self.docs_dir / local_path

        if not full_path.exists():
            return None, "file not found"

        try:
            with open(full_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
        except Exception as e:
            return None, f"cannot read: {e}"

        soup = BeautifulSoup(html_content, 'html.parser')
        page_id = self.path_to_page_id.get(local_path, self._path_to_id(local_path))

        self._fix_internal_links(soup, url, page_id)
        self._fix_image_paths(soup, local_path)

        content = self._extract_main_content(soup)
        if not content:
            return None, "no main content found"

        for dl in content.find_all('dl'):
            dl.replace_with(self._dl_to_ul(soup, dl))

        converter = AnchorPreservingConverter(
            page_id=page_id,
            guess_language=self.config.guess_code_language,
            heading_style="ATX",
            bullets="-",
            code_language=self.config.default_code_language,
            strip=['script', 'style'],
        )

        markdown = converter.convert(str(content))
        markdown = re.sub(r'\n{3,}', '\n\n', markdown).strip()

        if not markdown or len(markdown) < 50:
            return None, f"too short after conversion ({len(markdown)} chars)"

        page_title = title or Path(local_path).stem.replace('-', ' ').title()
        header = f'<a id="{page_id}"></a>\n\n# {page_title}\n\n'

        return header + markdown, ""

    def convert_all(self) -> str:
        """Convert all pages from manifest into a single MD string."""
        pages = self.manifest.get("pages", {})

        sorted_pages = sorted(
            pages.items(),
            key=lambda x: (x[1].get("depth", 0), x[1].get("local_path", ""))
        )

        print(f"Converting {len(sorted_pages)} pages...")

        converted = []
        skip_reasons: dict[str, int] = defaultdict(int)

        for url, page_info in sorted_pages:
            local_path = page_info["local_path"]
            title = page_info.get("title", "")

            if self._should_exclude(local_path):
                skip_reasons["excluded by pattern"] += 1
                continue

            md_content, reason = self.convert_page(local_path, url, title)

            if md_content:
                converted.append({
                    "page_id": self.path_to_page_id.get(local_path, ""),
                    "title": title,
                    "content": md_content,
                    "depth": page_info.get("depth", 0),
                })
            else:
                skip_reasons[reason] += 1

        print(f"\nConverted: {len(converted)}, Skipped: {sum(skip_reasons.values())}")
        print("Skip reasons:")
        for reason, count in sorted(skip_reasons.items(), key=lambda x: -x[1]):
            print(f"  {count:4d}x  {reason}")

        self.converted_pages = converted

        parts = [page["content"] for page in converted]
        return self.config.page_separator.join(parts)

    def save(self, content: str, output_path: Optional[str] = None):
        """Save converted MD to file."""
        output = Path(output_path or self.config.output_file)
        output.parent.mkdir(parents=True, exist_ok=True)

        with open(output, 'w', encoding='utf-8') as f:
            f.write(content)

        size_mb = output.stat().st_size / 1024 / 1024
        print(f"\nSaved to: {output} ({size_mb:.2f} MB)")


def convert_documentation(
        docs_dir: str,
        output_file: str = "documentation.md",
        **kwargs
) -> str:
    """
    Convenience wrapper for DocumentationConverter.

    Args:
        docs_dir:    Directory with crawled HTML files and manifest.json.
        output_file: Output MD file path.
        **kwargs:    Additional ConverterConfig fields.

    Returns:
        Converted markdown string.
    """
    config = ConverterConfig(
        docs_dir=docs_dir,
        output_file=output_file,
        **kwargs
    )

    converter = DocumentationConverter(config)
    content = converter.convert_all()
    converter.save(content)

    return content


if __name__ == "__main__":
    content = convert_documentation(
        docs_dir="crawled_docs/shiro",
        output_file="crawled_docs/shiro/shiro_documentation.md",
        exclude_patterns=["/xref/", "/cpd.html", "/dependencies.html"],
    )

    print(f"\nTotal length: {len(content)} characters")