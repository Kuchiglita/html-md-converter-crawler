<a id="zeppelin-apache-org-docs-0-12-0-index"></a>

# Apache Zeppelin 0.12.0 Documentation:

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-index--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-index--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-index--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-index--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-index--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code"></a>

# Apache Zeppelin 0.12.0 Documentation: Contributing to Apache Zeppelin (Code)

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website"></a>

# Apache Zeppelin 0.12.0 Documentation: Contributing to Apache Zeppelin (Website)

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools"></a>

# Apache Zeppelin 0.12.0 Documentation: Useful Developer Tools

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-development-helium-overview"></a>

# Apache Zeppelin 0.12.0 Documentation: Helium

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-development-helium-overview--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-development-helium-overview--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-development-helium-overview--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-development-helium-overview--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-development-helium-overview--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-development-helium-writing_application"></a>

# Apache Zeppelin 0.12.0 Documentation: Writing a new Helium Application

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell"></a>

# Apache Zeppelin 0.12.0 Documentation: Writing a new Helium Spell

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic"></a>

# Apache Zeppelin 0.12.0 Documentation: Writing a new Helium Visualization: basic

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation"></a>

# Apache Zeppelin 0.12.0 Documentation: Transformations in Zeppelin Visualization

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter"></a>

# Apache Zeppelin 0.12.0 Documentation: Writing a New Interpreter

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-alluxio"></a>

# Apache Zeppelin 0.12.0 Documentation: Alluxio Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-bigquery"></a>

# Apache Zeppelin 0.12.0 Documentation: BigQuery Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-cassandra"></a>

# Apache Zeppelin 0.12.0 Documentation: Cassandra CQL Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch"></a>

# Apache Zeppelin 0.12.0 Documentation: Elasticsearch Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-flink"></a>

# Apache Zeppelin 0.12.0 Documentation: Flink Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-flink--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-flink--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-flink--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-flink--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-flink--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-groovy"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Groovy Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-hbase"></a>

# Apache Zeppelin 0.12.0 Documentation: HBase Shell Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-hdfs"></a>

# Apache Zeppelin 0.12.0 Documentation: HDFS File System Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-hive"></a>

# Apache Zeppelin 0.12.0 Documentation: Hive Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-hive--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-hive--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-hive--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-hive--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-hive--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-influxdb"></a>

# Apache Zeppelin 0.12.0 Documentation: InfluxDB Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-java"></a>

# Apache Zeppelin 0.12.0 Documentation: Java interpreter in Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-java--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-java--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-java--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-java--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-java--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-jdbc"></a>

# Apache Zeppelin 0.12.0 Documentation: Generic JDBC Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-jupyter"></a>

# Apache Zeppelin 0.12.0 Documentation: Jupyter Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-livy"></a>

# Apache Zeppelin 0.12.0 Documentation: Livy Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-livy--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-livy--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-livy--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-livy--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-livy--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-mahout"></a>

# Apache Zeppelin 0.12.0 Documentation: Mahout Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-markdown"></a>

# Apache Zeppelin 0.12.0 Documentation: Markdown Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-mongodb"></a>

# Apache Zeppelin 0.12.0 Documentation: MongoDB Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-neo4j"></a>

# Apache Zeppelin 0.12.0 Documentation: Neo4j Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-postgresql"></a>

# Apache Zeppelin 0.12.0 Documentation: PostgreSQL, Apache HAWQ (incubating) Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-python"></a>

# Apache Zeppelin 0.12.0 Documentation: Python 2 & 3 Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-python--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-python--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-python--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-python--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-python--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-r"></a>

# Apache Zeppelin 0.12.0 Documentation: R Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-r--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-r--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-r--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-r--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-r--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-shell"></a>

# Apache Zeppelin 0.12.0 Documentation: Shell interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-shell--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-shell--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-shell--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-shell--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-shell--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-spark"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Spark Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-spark--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-spark--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-spark--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-spark--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-spark--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-interpreter-sparql"></a>

# Apache Zeppelin 0.12.0 Documentation: SPARQL Interpreter for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-quickstart-docker"></a>

# Apache Zeppelin 0.12.0 Documentation: Install

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-quickstart-docker--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-quickstart-docker--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-quickstart-docker--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-quickstart-docker--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-quickstart-docker--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui"></a>

# Apache Zeppelin 0.12.0 Documentation: Explore Apache Zeppelin UI

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin"></a>

# Apache Zeppelin 0.12.0 Documentation: Flink with Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-quickstart-install"></a>

# Apache Zeppelin 0.12.0 Documentation: Install

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-quickstart-install--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-quickstart-install--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-quickstart-install--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-quickstart-install--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-quickstart-install--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes"></a>

# Apache Zeppelin 0.12.0 Documentation: Install

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin"></a>

# Apache Zeppelin 0.12.0 Documentation: Python with Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin"></a>

# Apache Zeppelin 0.12.0 Documentation: R with Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin"></a>

# Apache Zeppelin 0.12.0 Documentation: Spark with Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin"></a>

# Apache Zeppelin 0.12.0 Documentation: SQL with Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-quickstart-tutorial"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin Tutorial

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-quickstart-yarn"></a>

# Apache Zeppelin 0.12.0 Documentation: Zeppelin on Yarn

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-search"></a>

# Apache Zeppelin 0.12.0 Documentation:

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-search--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-search--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-search--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-search--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-search--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration"></a>

# Apache Zeppelin 0.12.0 Documentation: How to integrate with hadoop

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build"></a>

# Apache Zeppelin 0.12.0 Documentation: How to Build Zeppelin from source

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support"></a>

# Apache Zeppelin 0.12.0 Documentation: Multi-user Support

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-basics-systemd"></a>

# Apache Zeppelin 0.12.0 Documentation: Manage Zeppelin with systemd

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-basics-systemd--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-basics-systemd--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-basics-systemd--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-basics-systemd--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-basics-systemd--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin on CDH

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster"></a>

# Apache Zeppelin 0.12.0 Documentation: Install Zeppelin with Flink and Spark in cluster mode

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin on Spark cluster mode

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin on Vagrant Virtual Machine

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-operation-configuration"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin Configuration

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin Monitoring

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting"></a>

# Apache Zeppelin 0.12.0 Documentation: Proxy Setting in Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting"></a>

# Apache Zeppelin 0.12.0 Documentation: Trouble Shooting

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading"></a>

# Apache Zeppelin 0.12.0 Documentation: Manual Zeppelin version upgrade procedure

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx"></a>

# Apache Zeppelin 0.12.0 Documentation: HTTP Basic Auth using NGINX

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization"></a>

# Apache Zeppelin 0.12.0 Documentation: Data Source Authorization in Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers"></a>

# Apache Zeppelin 0.12.0 Documentation: Setting up HTTP Response Headers

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization"></a>

# Apache Zeppelin 0.12.0 Documentation: Notebook Authorization in Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Shiro Authentication for Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend"></a>

# Apache Zeppelin 0.12.0 Documentation: Backend Angular API in Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend"></a>

# Apache Zeppelin 0.12.0 Documentation: Frontend Angular API in Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-display_system-basic"></a>

# Apache Zeppelin 0.12.0 Documentation: Basic Display System in Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro"></a>

# Apache Zeppelin 0.12.0 Documentation: Dynamic Form in Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management"></a>

# Apache Zeppelin 0.12.0 Documentation: Dependency Management for Interpreter

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks"></a>

# Apache Zeppelin 0.12.0 Documentation: Interpreter Execution Hooks (Experimental)

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation"></a>

# Apache Zeppelin 0.12.0 Documentation: Installing Interpreters

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode"></a>

# Apache Zeppelin 0.12.0 Documentation: Interpreter Binding Mode

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview"></a>

# Apache Zeppelin 0.12.0 Documentation: Interpreter in Apache Zeppelin

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation"></a>

# Apache Zeppelin 0.12.0 Documentation: Impersonation

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler"></a>

# Apache Zeppelin 0.12.0 Documentation: Running a Notebook on a Given Schedule Automatically

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage"></a>

# Apache Zeppelin 0.12.0 Documentation: Customizing Apache Zeppelin homepage

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions"></a>

# Apache Zeppelin 0.12.0 Documentation: Notebook Actions

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode"></a>

# Apache Zeppelin 0.12.0 Documentation: Personalized Mode

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs"></a>

# Apache Zeppelin 0.12.0 Documentation: How can you publish your paragraphs

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context"></a>

# Apache Zeppelin 0.12.0 Documentation: Zeppelin-Context

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin Configuration REST API

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin Credential REST API

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin Helium REST API

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin Interpreter REST API

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin Notebook REST API

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin notebook repository REST API

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin Server REST API

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin SDK - ZeppelinClient API

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)

---

<a id="zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api"></a>

# Apache Zeppelin 0.12.0 Documentation: Apache Zeppelin SDK - Session API

Toggle navigation

[
![I'm zeppelin](zeppelin.apache.org/docs/0.12.0/assets/themes/zeppelin/img/zeppelin_logo.png)
Zeppelin
](http://zeppelin.apache.org)[ 0.12.0
](/docs/0.12.0)

- [Quick Start ](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api--)
  - Getting Started
  - [Install](#zeppelin-apache-org-docs-0-12-0-quickstart-install)
  - [Explore UI](#zeppelin-apache-org-docs-0-12-0-quickstart-explore_ui)
  - [Tutorial](#zeppelin-apache-org-docs-0-12-0-quickstart-tutorial)
  - Run Mode
  - [Kubernetes](#zeppelin-apache-org-docs-0-12-0-quickstart-kubernetes)
  - [Docker](#zeppelin-apache-org-docs-0-12-0-quickstart-docker)
  - [Yarn](#zeppelin-apache-org-docs-0-12-0-quickstart-yarn)
  - [Spark with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-spark_with_zeppelin)
  - [Flink with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-flink_with_zeppelin)
  - [SQL with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-sql_with_zeppelin)
  - [Python with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-python_with_zeppelin)
  - [R with Zeppelin](#zeppelin-apache-org-docs-0-12-0-quickstart-r_with_zeppelin)
- [Usage](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api--)
  - Dynamic Form
  - [What is Dynamic Form?](#zeppelin-apache-org-docs-0-12-0-usage-dynamic_form-intro)
  - Display System
  - [Text Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--text)
  - [HTML Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--html)
  - [Table Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--table)
  - [Network Display](#zeppelin-apache-org-docs-0-12-0-usage-display_system-basic--network)
  - [Angular Display using Backend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_backend)
  - [Angular Display using Frontend API](#zeppelin-apache-org-docs-0-12-0-usage-display_system-angular_frontend)
  - Interpreter
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Interpreter Binding Mode](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-interpreter_binding_mode)
  - [User Impersonation](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-user_impersonation)
  - [Dependency Management](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-dependency_management)
  - [Installing Interpreters](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-installation)
  - [Execution Hooks (Experimental)](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-execution_hooks)
  - Other Features
  - [Publishing Paragraphs](#zeppelin-apache-org-docs-0-12-0-usage-other_features-publishing_paragraphs)
  - [Personalized Mode](#zeppelin-apache-org-docs-0-12-0-usage-other_features-personalized_mode)
  - [Customizing Zeppelin Homepage](#zeppelin-apache-org-docs-0-12-0-usage-other_features-customizing_homepage)
  - [Notebook Actions](#zeppelin-apache-org-docs-0-12-0-usage-other_features-notebook_actions)
  - [Cron Scheduler](#zeppelin-apache-org-docs-0-12-0-usage-other_features-cron_scheduler)
  - [Zeppelin Context](#zeppelin-apache-org-docs-0-12-0-usage-other_features-zeppelin_context)
  - REST API
  - [Interpreter API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-interpreter)
  - [Zeppelin Server API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-zeppelin_server)
  - [Notebook API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook)
  - [Notebook Repository API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-notebook_repository)
  - [Configuration API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-configuration)
  - [Credential API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-credential)
  - [Helium API](#zeppelin-apache-org-docs-0-12-0-usage-rest_api-helium)
  - Zeppelin SDK
  - [Client API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-client_api)
  - [Session API](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api)
- [Setup](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api--)
  - Basics
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Hadoop Integration](#zeppelin-apache-org-docs-0-12-0-setup-basics-hadoop_integration)
  - [Multi-user Support](#zeppelin-apache-org-docs-0-12-0-setup-basics-multi_user_support)
  - Deployment
  - [Spark Cluster Mode: Standalone](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-standalone-mode)
  - [Spark Cluster Mode: YARN](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-yarn-mode)
  - [Spark Cluster Mode: Mesos](#zeppelin-apache-org-docs-0-12-0-setup-deployment-spark_cluster_mode--spark-on-mesos-mode)
  - [Zeppelin with Flink, Spark Cluster](#zeppelin-apache-org-docs-0-12-0-setup-deployment-flink_and_spark_cluster)
  - [Zeppelin on CDH](#zeppelin-apache-org-docs-0-12-0-setup-deployment-cdh)
  - [Zeppelin on VM: Vagrant](#zeppelin-apache-org-docs-0-12-0-setup-deployment-virtual_machine)
  - Security
  - [HTTP Basic Auth using NGINX](#zeppelin-apache-org-docs-0-12-0-setup-security-authentication_nginx)
  - [Shiro Authentication](#zeppelin-apache-org-docs-0-12-0-setup-security-shiro_authentication)
  - [Notebook Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-notebook_authorization)
  - [Data Source Authorization](#zeppelin-apache-org-docs-0-12-0-setup-security-datasource_authorization)
  - [HTTP Security Headers](#zeppelin-apache-org-docs-0-12-0-setup-security-http_security_headers)
  - Notebook Storage
  - [Git Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-local-git-repository)
  - [S3 Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-s3)
  - [Azure Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-azure)
  - [Google Cloud Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-google-cloud-storage)
  - [OSS Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-oss)
  - [MongoDB Storage](/docs/0.12.0/setup/storage/storage.html#notebook-storage-in-mongodb)
  - Operation
  - [Configuration](#zeppelin-apache-org-docs-0-12-0-setup-operation-configuration)
  - [Monitoring](#zeppelin-apache-org-docs-0-12-0-setup-operation-monitoring)
  - [Proxy Setting](#zeppelin-apache-org-docs-0-12-0-setup-operation-proxy_setting)
  - [Upgrading](#zeppelin-apache-org-docs-0-12-0-setup-operation-upgrading)
  - [Trouble Shooting](#zeppelin-apache-org-docs-0-12-0-setup-operation-trouble_shooting)
- [Interpreter ](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api--)
  - Interpreters
  - [Overview](#zeppelin-apache-org-docs-0-12-0-usage-interpreter-overview)
  - [Spark](#zeppelin-apache-org-docs-0-12-0-interpreter-spark)
  - [Flink](#zeppelin-apache-org-docs-0-12-0-interpreter-flink)
  - [JDBC](#zeppelin-apache-org-docs-0-12-0-interpreter-jdbc)
  - [Python](#zeppelin-apache-org-docs-0-12-0-interpreter-python)
  - [R](#zeppelin-apache-org-docs-0-12-0-interpreter-r)
  - [Alluxio](#zeppelin-apache-org-docs-0-12-0-interpreter-alluxio)
  - [BigQuery](#zeppelin-apache-org-docs-0-12-0-interpreter-bigquery)
  - [Cassandra](#zeppelin-apache-org-docs-0-12-0-interpreter-cassandra)
  - [Elasticsearch](#zeppelin-apache-org-docs-0-12-0-interpreter-elasticsearch)
  - [Groovy](#zeppelin-apache-org-docs-0-12-0-interpreter-groovy)
  - [HBase](#zeppelin-apache-org-docs-0-12-0-interpreter-hbase)
  - [HDFS](#zeppelin-apache-org-docs-0-12-0-interpreter-hdfs)
  - [Hive](#zeppelin-apache-org-docs-0-12-0-interpreter-hive)
  - [influxDB](#zeppelin-apache-org-docs-0-12-0-interpreter-influxdb)
  - [Java](#zeppelin-apache-org-docs-0-12-0-interpreter-java)
  - [Jupyter](#zeppelin-apache-org-docs-0-12-0-interpreter-jupyter)
  - [Livy](#zeppelin-apache-org-docs-0-12-0-interpreter-livy)
  - [Mahout](#zeppelin-apache-org-docs-0-12-0-interpreter-mahout)
  - [Markdown](#zeppelin-apache-org-docs-0-12-0-interpreter-markdown)
  - [MongoDB](#zeppelin-apache-org-docs-0-12-0-interpreter-mongodb)
  - [Neo4j](#zeppelin-apache-org-docs-0-12-0-interpreter-neo4j)
  - [Postgresql, HAWQ](#zeppelin-apache-org-docs-0-12-0-interpreter-postgresql)
  - [Shell](#zeppelin-apache-org-docs-0-12-0-interpreter-shell)
  - [Sparql](#zeppelin-apache-org-docs-0-12-0-interpreter-sparql)
- [More](#zeppelin-apache-org-docs-0-12-0-usage-zeppelin_sdk-session_api--)
  - Extending Zeppelin
  - [Writing Zeppelin Interpreter](#zeppelin-apache-org-docs-0-12-0-development-writing_zeppelin_interpreter)
  - Helium (Experimental)
  - [Overview](#zeppelin-apache-org-docs-0-12-0-development-helium-overview)
  - [Writing Helium Application](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_application)
  - [Writing Helium Spell](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_spell)
  - [Writing Helium Visualization: Basics](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_basic)
  - [Writing Helium Visualization: Transformation](#zeppelin-apache-org-docs-0-12-0-development-helium-writing_visualization_transformation)
  - Contributing to Zeppelin
  - [How to Build Zeppelin](#zeppelin-apache-org-docs-0-12-0-setup-basics-how_to_build)
  - [Useful Developer Tools](#zeppelin-apache-org-docs-0-12-0-development-contribution-useful_developer_tools)
  - [How to Contribute (code)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_code)
  - [How to Contribute (website)](#zeppelin-apache-org-docs-0-12-0-development-contribution-how_to_contribute_website)
  - External Resources
  - [Mailing List](https://zeppelin.apache.org/community.html)
  - [Apache Zeppelin Wiki](https://cwiki.apache.org/confluence/display/ZEPPELIN/Zeppelin+Home)
  - [Stackoverflow Questions about Zeppelin](http://stackoverflow.com/questions/tagged/apache-zeppelin)
- [

  ](#zeppelin-apache-org-docs-0-12-0-search)