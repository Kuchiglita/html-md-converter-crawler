# from html2md import download_and_convert
#
# download_and_convert("https://opennlp.apache.org/docs/2.5.7/manual/opennlp.html","opennlp.md")

import os
import logging
from pathlib import Path
from urllib.parse import urlparse

from web_crawler import DocCrawler, CrawlConfig
from linker import convert_documentation

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

COMMON_EXCLUDES = [
    "/apidocs/", "/testapidocs/", "/xref/", "/xref-test/",
    "/cobertura/", "/checkstyle", "/pmd", "/dependencies.html",
    "/dependency-info.html", "/project-info.html", "/project-reports.html",
    "/license.html", "/summary.html", "/team-list.html",
    # Confluence specific (Tapestry and alike)
    ".data/",
    "export=word",
    "reply.html", # commentaries
    "showComments=", # commentaries
    "showCommentArea=", # commentaries
    "/download/attachments/", # files, not pages
    "view_action",
    # Sparc specific
    "/api/python/",
    "/api/scala/",
    "/api/java/",
    "/api/sql/",
    "/api/r/",
    # Javadoc specific (Lucene, Solr, and others)
    "/class-use/",
    "index-all.html",
    "help-doc.html",
    "deprecated-list.html",
    "constant-values.html",
    "serialized-form.html",
    "package-tree.html",
    "package-summary.html",
    "package-use.html",
]


def get_repo_name(url: str) -> str:
    """takes repo name from URL (e.g., johnzon.apache.org -> johnzon)"""
    parsed = urlparse(url)
    netloc = parsed.netloc
    return netloc.split('.')[0] if '.' in netloc else "doc_repo"


def process_all_docs(docs_list: list[str]):
    base_output_dir = Path("crawled_docs")

    for url in docs_list:
        repo_name = get_repo_name(url)
        output_dir = base_output_dir / repo_name
        md_file = output_dir / f"{repo_name}_full.md"

        logger.info(f"\n{'=' * 50}\nSTARTING: {url}\n{'=' * 50}")

        config = CrawlConfig(
            start_url=url,
            output_dir=str(output_dir),
            max_depth=50,
            download_assets=True,
            delay=0.5,
            exclude_patterns=COMMON_EXCLUDES
        )

        crawler = DocCrawler(config)
        try:
            logger.info(f"Crawling {url} into {output_dir}...")
            crawler.crawl()
            crawler.save_manifest()
        except Exception as e:
            logger.error(f"Failed to crawl {url}: {e}")
            continue
        finally:
            crawler.close()

        try:
            logger.info(f"Converting {output_dir} to Markdown...")
            convert_documentation(
                docs_dir=str(output_dir),
                output_file=str(md_file),
            )
            logger.info(f"SUCCESS: {md_file}")
        except Exception as e:
            logger.error(f"Failed to convert {repo_name}: {e}")


if __name__ == "__main__":
    DOCUMENTATION_SITES = [
        # "https://impala.apache.org/docs/build/plain-html/index.html",
        # "https://jmeter.apache.org/usermanual/index.html",
        # "https://opennlp.apache.org/docs/2.5.7/manual/opennlp.html",
        # "https://tapestry.apache.org/documentation.html",
        # "https://inlong.apache.org/docs/introduction",
        # "https://iotdb.apache.org/UserGuide/latest/IoTDB-Introduction/IoTDB-Introduction_apache.html",
        # "https://ant.apache.org/ivy/history/2.5.3/index.html",
        # "https://jena.apache.org/tutorials/rdf_api.html",
        # "https://johnzon.apache.org/project-info.html",
        # "https://spark.apache.org/docs/latest/",
        # "https://zookeeper.apache.org/doc/r3.9.4/index.html",
        # "https://netbeans.apache.org/tutorial/main/kb/docs/java/",
        # "https://maven.apache.org/guides/index.html",
        # "https://tomcat.apache.org/tomcat-11.0-doc/index.html",
        # "https://dlcdn.apache.org/karaf/documentation/4_x.html",
        # "https://knox.apache.org/books/knox-2-1-0/user-guide.html",
        # "https://kudu.apache.org/docs/",
        # "https://kylin.apache.org/docs/overview",
        # "https://lens.apache.org/developer/contribute.html",
        # "https://linkis.apache.org/docs/latest/about/introduction",
        # "https://lucene.apache.org/core/10_3_2/index.html", # too much
        # "https://madlib.apache.org/docs/latest/index.html",
        # "https://mahout.apache.org/docs/",
        # "https://mina.apache.org/mina-project/documentation.html",
        # "https://nifi.apache.org/components/",
        # "https://ode.apache.org/userguide/",
        # "https://olingo.apache.org/doc/odata4/overview.html",
        # "https://oozie.apache.org/docs/5.2.1/index.html",
        # "https://openwebbeans.apache.org/documentation.html",
        # "https://ozone.apache.org/docs",
        # "https://pulsar.apache.org/docs/4.1.x/",
        # "https://axis.apache.org/axis2/java/sandesha/userGuide.html",
        # "https://shardingsphere.apache.org/document/current/en/overview/",
        # "https://shenyu.apache.org/docs/",
        # "https://shiro.apache.org/documentation.html",
        # "https://singa.apache.org/docs/installation/",
        # "https://sling.apache.org/documentation.html",
        # "https://solr.apache.org/guide/solr/latest/index.html",
        # "https://sis.apache.org/documentation.html",
        # "https://stanbol.apache.org/docs/trunk/",
        # "https://storm.apache.org/releases/2.8.3/index.html",
        # "https://stormcrawler.apache.org/docs/3.5.1/index.html",
        # "https://streampipes.apache.org/docs/user-guide-introduction/",
        # "https://submarine.apache.org/docs/gettingStarted/quickstart",
        # "https://synapse.apache.org/userguide/installation.html",
        # "https://syncope.apache.org/docs/4.0/getting-started.html",
        # "https://teaclave.apache.org/trustzone-sdk-docs/#quick-start",
        # "https://tez.apache.org/install.html", # large, need to cut off something
        # "https://tomee.apache.org/tomee-10.1/docs/",
        # "https://db.apache.org/torque/torque-7.1/documentation/tutorial/index.html",
        # "https://turbine.apache.org/turbine/turbine-7-1/index.html",
        # "https://uima.apache.org/documentation.html", # large, need to cut off something
        # "https://unomi.apache.org/manual/3_0_x/index.html",
        # "https://velocity.apache.org/dvsl/1.0/",
        # "https://vxquery.apache.org/developer_get_started.html",
        # "https://whirr.apache.org/docs/0.8.2/quick-start-guide.html",
        # "https://creadur.apache.org/whisker/project-info.html",
        # "https://nightlies.apache.org/wicket/guide/10.x/single.html",
        # "https://zeppelin.apache.org/docs/0.12.0/",
        # "https://logging.apache.org/log4j/2.x/manual/index.html",
    ]

    # process_all_docs(DOCUMENTATION_SITES)