import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup, Tag
from markdownify import MarkdownConverter


class _AnchorPreservingConverter(MarkdownConverter):
    """Сохраняет <a name="..."> / <a id="..."> как сырые HTML-якоря."""

    def convert_a(self, el, text, **kwargs):
        href = el.get('href')
        anchor_id = el.get('name') or el.get('id')
        if anchor_id and not href:
            return f'<a id="{anchor_id}"></a>{text}'
        return super().convert_a(el, text, **kwargs)


def _dl_to_ul(soup: BeautifulSoup, dl_tag: Tag) -> Tag:
    """
    Рекурсивно конвертирует <dl>/<dt>/<dd> → <ul>/<li>.

    HTML-документации часто используют definition lists для оглавлений,
    а markdownify превращает их в уродливый синтаксис с двоеточиями.
    """
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
                current_li.append(_dl_to_ul(soup, nested_dl))
            else:
                for c in list(child.children):
                    current_li.append(c.extract())

    return ul

# Inner helper functions
def _fix_self_links(content: Tag, page_url: str) -> None:
    """Абсолютные self-ссылки → относительные якоря (#anchor)."""
    page_path = urlparse(page_url).path
    for a in content.find_all('a', href=True):
        parsed = urlparse(a['href'])
        if parsed.fragment and parsed.path == page_path:
            a['href'] = '#' + parsed.fragment


def _fix_toc_lists(soup: BeautifulSoup, content: Tag) -> None:
    """Конвертирует все <dl> в TOC-блоках и list-of-tables в <ul>/<li>."""
    for toc_div in content.find_all('div', class_=['toc', 'list-of-tables']):
        for dl in toc_div.find_all('dl', recursive=False):
            dl.replace_with(_dl_to_ul(soup, dl))


def _extract_main_content(soup: BeautifulSoup) -> Tag:
    """Пытается найти основной контент страницы."""
    for selector in [
        ('div', {'class': 'content'}),
        ('main', {}),
        ('article', {}),
        ('body', {}),
    ]:
        tag = soup.find(selector[0], **selector[1])
        if tag:
            return tag
    raise ValueError("Не удалось найти основной контент на странице")


def html_to_markdown(
    html: str,
    page_url: str = "",
    code_language: str = "java",
) -> str:
    """
    Конвертирует HTML-строку документации в Markdown.

    Args:
        html:          Сырой HTML.
        page_url:      URL страницы (для исправления self-ссылок).
                       Если пустой — self-ссылки не трогаем.
        code_language: Язык по умолчанию для блоков кода, в тестовом только текст, поэтому не важно

    Returns:
        Markdown-строка.
    """
    soup = BeautifulSoup(html, 'html.parser')
    content = _extract_main_content(soup)

    if page_url:
        _fix_self_links(content, page_url)
    _fix_toc_lists(soup, content)

    markdown = _AnchorPreservingConverter(
        heading_style="ATX",
        bullets="-",
        code_language=code_language,
        strip=['script', 'style', 'nav', 'footer', 'header'],
    ).convert(str(content))

    markdown = re.sub(r'\n{3,}', '\n\n', markdown).strip()

    return markdown


def download_and_convert(
    url: str,
    output_file: str,
    code_language: str = "java",
) -> str:
    """
    Скачивает HTML по URL, конвертирует в Markdown и сохраняет в файл.

    Args:
        url:           URL страницы документации.
        output_file:   Путь к выходному .md файлу.
        code_language: Язык по умолчанию для блоков кода.

    Returns:
        Markdown-строка (на случай если нужна дальнейшая обработка).
    """
    import requests

    response = requests.get(url)
    response.raise_for_status()
    response.encoding = 'utf-8'

    # statistics
    html_size_kb = len(response.content) / 1024
    print(f"Скачано: {html_size_kb:.2f} KB")

    markdown = html_to_markdown(
        html=response.text,
        page_url=url,
        code_language=code_language,
    )

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(markdown)

    import os
    file_size_kb = й.path.getsize(output_file) / 1024
    print(f"Готово: {output_file} ({len(markdown)} символов, {file_size_kb:.2f} KB)")
    return markdown

