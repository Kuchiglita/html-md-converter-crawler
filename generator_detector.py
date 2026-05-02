import re
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from bs4 import BeautifulSoup, Comment
from urllib.parse import urlparse

@dataclass
class DetectionResult:
    generator_name: str
    confidence: float  # (0.0 - 1.0)
    content_selector: str
    markup_type: str = "html"  # ?
    version: Optional[str] = None


class DocTypeDetector:
    def __init__(self):
        self.threshold = 60

        #  Registry of generator traits and specifications to be detected on
        self.registry = {
            "docusaurus": {
                "priority": 100,
                "content_selector": "article",
                "markup_type": "html",
                "rules": [
                    {"type": "meta", "key": "generator", "pattern": r"docusaurus", "weight": 100},
                    {"type": "class", "pattern": r"theme-doc-markdown", "weight": 60},
                    {"type": "path", "pattern": r"docusaurus", "weight": 40},
                    {"type": "attr", "tag": "html", "key": "data-rh", "weight": 30}
                ]
            },
            "maven-site": {
                "priority": 100,
                "content_selector": "#bodyColumn, .main-body",
                "markup_type": "html",
                "rules": [
                    {"type": "id", "pattern": r"bodyColumn", "weight": 90},
                    {"type": "id", "pattern": r"bannerLeft", "weight": 80},
                    {"type": "text_clue", "pattern": r"Built by Apache Maven", "weight": 100}
                ]
            },
            "asciidoctor": {
                "priority": 60,
                "content_selector": "#content, .main-content",
                "markup_type": "asciidoc",
                "rules": [
                    {"type": "class", "pattern": r"admonitionblock", "weight": 90}, # UNIQUE!
                    {"type": "class", "pattern": r"listingblock", "weight": 70},    # UNIQUE!
                    {"type": "id", "pattern": r"toctitle", "weight": 50}
                ]
            },
            "hugo": {
                "priority": 90,
                "content_selector": "article, .content, #content",
                "markup_type": "md",
                "rules": [
                    {"type": "meta", "key": "generator", "pattern": r"Hugo", "weight": 100},
                    {"type": "path", "pattern": r"hugo", "weight": 40}
                ]
            },
            "apache-anakia": {
                "priority": 80,
                "content_selector": "table",  # Old docs often store everything in tables
                "markup_type": "html",
                "rules": [
                    {"type": "id", "pattern": r"banner", "weight": 50},
                    {"type": "attr", "tag": "table", "key": "bgcolor", "weight": 30},
                    {"type": "path", "pattern": r"images/asf_logo", "weight": 20}
                ]
            },
            "sphinx": {
                "priority": 100,
                "content_selector": ".body, [role='main']",
                "markup_type": "rst",  # reStructuredText
                "rules": [
                    {"type": "path", "pattern": r"_static/searchtools.js", "weight": 80},
                    {"type": "class", "pattern": r"sphinxsidebar", "weight": 60},
                    {"type": "id", "pattern": r"searchbox", "weight": 40}
                ]
            },
            "jbake": {
                "priority": 105,  # Чуть выше Asciidoctor, чтобы побеждать в "матрешках"
                "content_selector": ".container, #content, .main-content",
                "markup_type": "html",
                "rules": [
                    {"type": "meta", "key": "generator", "pattern": r"JBake", "weight": 100},
                    {"type": "path", "pattern": r"jbake", "weight": 50},
                    # often in the footer is text "Baked with JBake"
                    {"type": "text_clue", "pattern": r"Baked with JBake", "weight": 80}
                ]
            },
            "amaya": {
                "priority": 110,
                "content_selector": "body",  # content in Amaya is the whole "body" container
                "markup_type": "html",
                "rules": [
                    {"type": "meta", "key": "generator", "pattern": r"Amaya", "weight": 100},
                    {"type": "attr", "tag": "html", "key": "xmlns", "pattern": r"w3.org", "weight": 20}
                ]
            },
            "docbook": {
                "priority": 95,
                "content_selector": "div.book, div.chapter, div.section, div.article",
                "markup_type": "xml",
                "rules": [
                    {"type": "meta", "key": "generator", "pattern": r"DocBook XSL", "weight": 100},
                    # unique DocBook XSL markers (the most trustful)
                    {"type": "class", "pattern": r"^navheader$", "weight": 40},
                    {"type": "class", "pattern": r"^navfooter$", "weight": 40},
                    {"type": "class", "pattern": r"^titlepage$", "weight": 30},
                    # structural classes (not so unique but still good)
                    {"type": "class", "pattern": r"^book$|^chapter$|^article$", "weight": 20}
                ]
            },
            "vuepress": {
                "priority": 110,
                "content_selector": ".theme-default-content",
                "markup_type": "html",
                "rules": [
                    {"type": "script_var", "pattern": r"__VUEPRESS_STATE__", "weight": 100},
                    {"type": "id", "pattern": r"^app$", "weight": 30}
                ]
            },
            "doxygen": {
                "priority": 110,
                "content_selector": ".contents",
                "markup_type": "html",
                "rules": [
                    {"type": "text_clue", "pattern": r"Generated by Doxygen", "weight": 100},
                    {"type": "class", "pattern": r"contents", "weight": 60},
                    {"type": "id", "pattern": r"doc-content", "weight": 80}
                ]
            },
            "antora": {
                "priority": 110,
                "content_selector": "article.doc",
                "markup_type": "asciidoc",
                "rules": [
                    {"type": "class", "pattern": r"^doc$", "weight": 40},
                    {"type": "path", "pattern": r"antora", "weight": 60},
                    {"type": "meta", "key": "generator", "pattern": r"Antora", "weight": 100}
                ]
            },
            # better Javadoc (для Lucene)
            "javadoc": {
                "priority": 110,
                "content_selector": "main, .indexContainer, .contentContainer",
                "markup_type": "javadoc",
                "rules": [
                    {"type": "comment", "pattern": r"Generated by javadoc", "weight": 100},
                    {"type": "class", "pattern": r"indexContainer|inheritance|methodDetails", "weight": 70},
                    {"type": "path", "pattern": r"jquery-ui", "weight": 30}
                ]
            },
            # Spark and Zeppelin
            "jekyll": {
                "priority": 70,
                "content_selector": ".content, #content, .container",
                "markup_type": "md",
                "rules": [
                    {"type": "path", "pattern": r"jekyll-gist", "weight": 60},
                    {"type": "comment", "pattern": r"jekyll", "weight": 80},
                    {"type": "path", "pattern": r"assets/themes", "weight": 40}
                ]
            }
        }
    def detect(self, html: str, url: str) -> DetectionResult:
        soup = BeautifulSoup(html, 'html.parser')
        scores = {name: 0 for name in self.registry}
        detected_versions: Dict[str, Optional[str]] = {name: None for name in self.registry}

        all_ids = [el.get('id') for el in soup.find_all(id=True)]
        all_classes = []
        for el in soup.find_all(class_=True):
            all_classes.extend(el.get('class'))

        all_resource_paths = []
        for tag in soup.find_all(['script', 'link']):
            path = tag.get('src') or tag.get('href')
            if path: all_resource_paths.append(path.lower())

        meta_tags = {m.get('name', '').lower(): m.get('content', '') for m in soup.find_all('meta')}

        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        all_comments_str = " ".join(comments).lower()

        all_scripts_str = " ".join([s.string for s in soup.find_all('script') if s.string]).lower()

        for gen_name, config in self.registry.items():
            for rule in config['rules']:
                match = False

                if rule['type'] == 'meta':
                    content = meta_tags.get(rule['key'].lower(), '')
                    if re.search(rule['pattern'], content, re.I):
                        match = True
                        detected_versions[gen_name] = content  # Saving version string

                elif rule['type'] == 'id':
                    if any(re.search(rule['pattern'], str(id_val), re.I) for id_val in all_ids):
                        match = True

                elif rule['type'] == 'class':
                    if any(re.search(rule['pattern'], str(cls), re.I) for cls in all_classes):
                        match = True

                elif rule['type'] == 'path':
                    if any(re.search(rule['pattern'], str(path), re.I) for path in all_resource_paths):
                        match = True

                elif rule['type'] == 'comment':
                    if re.search(rule['pattern'], all_comments_str, re.I):
                        match = True

                elif rule['type'] == 'script_var':
                    if re.search(rule['pattern'], all_scripts_str, re.I):
                        match = True

                if match:
                    scores[gen_name] += rule['weight']


        candidates = []
        for gen_name, score in scores.items():
            if score >= self.threshold:
                candidates.append({
                    "name": gen_name,
                    "score": score,
                    "priority": self.registry[gen_name]["priority"]
                })

        if not candidates:
            return self._fallback_detection(soup)

        winner_info = sorted(candidates, key=lambda x: (x['score'], x['priority']), reverse=True)[0]
        gen_name = winner_info['name']

        return DetectionResult(
            generator_name=gen_name,
            confidence=min(winner_info['score'] / 100.0, 1.0),
            content_selector=self.registry[gen_name]['content_selector'],
            markup_type=self.registry[gen_name]['markup_type'],
            version=detected_versions[gen_name]
        )


    def _fallback_detection(self, soup: BeautifulSoup) -> DetectionResult:
        """Logic for unrecognized HTML."""
        # check for bootstrap
        is_bootstrap = any("container" in str(cls) for el in soup.find_all(class_=True)
                           for cls in el.get('class'))
        if is_bootstrap:
            return DetectionResult(
                generator_name="generic-bootstrap",
                confidence=0.5,
                content_selector=".container, .main, #content",  # trying typical sections
                markup_type="html"
            )

        return DetectionResult(
            generator_name="generic-html",
            confidence=0.1,
            content_selector="body",  # take everything
            markup_type="html"
        )


import requests


def fetch_html(url: str) -> str:
    # Заголовок, чтобы прикинуться браузером
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    }

    try:
        response = requests.get(url, headers=headers, timeout=15, allow_redirects=True)
        response.raise_for_status()

        return response.text

    except requests.RequestException as e:
        print(f"Ошибка при загрузке {url}: {e}")
        return ""

##########################
urls = [
"https://impala.apache.org/docs/build/plain-html/index.html",
"https://jmeter.apache.org/usermanual/index.html",
"https://opennlp.apache.org/docs/2.5.7/manual/opennlp.html",
"https://tapestry.apache.org/documentation.html",
"https://inlong.apache.org/docs/introduction",
"https://iotdb.apache.org/UserGuide/latest/IoTDB-Introduction/IoTDB-Introduction_apache.html",
"https://ant.apache.org/ivy/history/2.5.3/index.html",
"https://jena.apache.org/tutorials/rdf_api.html",
"https://johnzon.apache.org/project-info.html",
"https://spark.apache.org/docs/latest/",
"https://zookeeper.apache.org/doc/r3.9.4/index.html",
"https://netbeans.apache.org/tutorial/main/kb/docs/java/",
"https://maven.apache.org/guides/index.html",
"https://tomcat.apache.org/tomcat-11.0-doc/index.html",
"https://dlcdn.apache.org/karaf/documentation/4_x.html",
"https://knox.apache.org/books/knox-2-1-0/user-guide.html",
"https://kudu.apache.org/docs/",
"https://kylin.apache.org/docs/overview",
"https://lens.apache.org/developer/contribute.html",
"https://linkis.apache.org/docs/latest/about/introduction",
"https://lucene.apache.org/core/10_3_2/index.html",
"https://madlib.apache.org/docs/latest/index.html",
"https://mahout.apache.org/docs/",
"https://mina.apache.org/mina-project/documentation.html",
"https://nifi.apache.org/components/",
"https://ode.apache.org/userguide/",
"https://olingo.apache.org/doc/odata4/overview.html",
"https://oozie.apache.org/docs/5.2.1/index.html",
"https://openwebbeans.apache.org/documentation.html",
"https://ozone.apache.org/docs",
"https://pulsar.apache.org/docs/4.1.x/",
"https://axis.apache.org/axis2/java/sandesha/userGuide.html",
"https://shardingsphere.apache.org/document/current/en/overview/",
"https://shenyu.apache.org/docs/",
"https://shiro.apache.org/documentation.html",
"https://singa.apache.org/docs/installation/",
"https://sling.apache.org/documentation.html",
"https://solr.apache.org/guide/solr/latest/index.html",
"https://sis.apache.org/documentation.html",
"https://stanbol.apache.org/docs/trunk/",
"https://storm.apache.org/releases/2.8.3/index.html",
"https://stormcrawler.apache.org/docs/3.5.1/index.html",
"https://streampipes.apache.org/docs/user-guide-introduction/",
"https://submarine.apache.org/docs/gettingStarted/quickstart",
"https://synapse.apache.org/userguide/installation.html",
"https://syncope.apache.org/docs/4.0/getting-started.html",
"https://teaclave.apache.org/trustzone-sdk-docs/#quick-start",
"https://tez.apache.org/install.html",
"https://tomee.apache.org/tomee-10.1/docs/",
"https://db.apache.org/torque/torque-7.1/documentation/tutorial/index.html",
"https://turbine.apache.org/turbine/turbine-7-1/index.html",
"https://uima.apache.org/documentation.html",
"https://unomi.apache.org/manual/3_0_x/index.html",
"https://velocity.apache.org/dvsl/1.0/",
"https://vxquery.apache.org/developer_get_started.html",
"https://whirr.apache.org/docs/0.8.2/quick-start-guide.html",
"https://creadur.apache.org/whisker/project-info.html",
"https://nightlies.apache.org/wicket/guide/10.x/single.html",
"https://zeppelin.apache.org/docs/0.12.0/",
"https://logging.apache.org/log4j/2.x/manual/index.html",
]
import time

# 1. Выносим создание детектора за пределы цикла
detector = DocTypeDetector()

def download_and_detect(url: str, detector_instance: DocTypeDetector) -> str:
    print(f"  --> Downloading {url}...", end=" ", flush=True)
    html = fetch_html(url)

    if not html:
        print("FAILED")
        return f"{url:<70}  :  ERROR (No HTML)\n"

    # Используем переданный url, а не хардкод
    result = detector_instance.detect(html, url)
    print("DONE")

    return f"{url:<70}  :  {result.generator_name} ({result.confidence})\n"


# 2. Открываем файл ОДИН раз (режим 'w' для перезаписи или 'a' для добавления)
with open('html_generators_debug.txt', 'a', encoding='utf-8') as file:
    counter = 0
    total = len(urls)

    for url in urls:
        counter += 1
        print(f"[{counter}/{total}]", end="")

        # Получаем строку результата
        line = download_and_detect(url, detector)

        # Записываем и ПРИНУДИТЕЛЬНО сбрасываем на диск (flush)
        file.write(line)
        file.flush()  # Это гарантирует, что данные появятся в файле сразу

        # Небольшая пауза, чтобы не забанили (опционально)
        # time.sleep(0.5)

print("\n--- ВСЕ ГОТОВО! Проверь файл html_generators_debug.txt ---")







