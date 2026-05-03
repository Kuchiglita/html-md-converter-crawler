<a id="linkis-apache-org-docs-latest-about-introduction-index"></a>

# Introduction | Apache Linkis

- [](/)
- About Linkis
- Introduction
Version: 1.8.0

On this page

# Introduction

Linkis builds a layer of computation middleware between upper applications and underlying engines. By using standard interfaces such as REST/WS/JDBC provided by Linkis, the upper applications can easily access the underlying engines such as MySQL/Spark/Hive/Presto/Flink, etc., and achieve the intercommunication of user resources like unified variables, scripts, UDFs, functions and resource files，and provides data source and metadata management services through REST standard interface. at the same time.

As a computation middleware, Linkis provides powerful connectivity, reuse, orchestration, expansion, and governance capabilities. By decoupling the application layer and the engine layer, it simplifies the complex network call relationship, and thus reduces the overall complexity and saves the development and maintenance costs as well.

Since the first release of Linkis in 2019, it has accumulated more than **700** trial companies and **1000+** sandbox trial users, which involving diverse industries, from finance, banking, tele-communication, to manufactory, internet companies and so on. Lots of companies have already used Linkis as a unified entrance for the underlying computation and storage engines of the big data platform.

![linkis-intro-01](linkis.apache.org/assets/images/linkis-intro-01-5c9940b8de17f2a39890005ae55c2452.png)

![linkis-intro-03](linkis.apache.org/assets/images/linkis-intro-03-5ce80c779d78561c2c734abf7bede592.png)

## Features

- **Support for diverse underlying computation storage engines** : Spark, Hive, Python, Shell, Flink, JDBC, Pipeline, Sqoop, OpenLooKeng, Presto, ElasticSearch, Trino, SeaTunnel, etc.;
- **Support for diverse language** : SparkSQL, HiveSQL, Python, Shell, Pyspark, Scala, JSON and Java;
- **Powerful computing governance capability** : It can provide task routing, load balancing, multi-tenant, traffic control, resource control and other capabilities based on multi-level labels;
- **Support full stack computation/storage engine** : The ability to receive, execute and manage tasks and requests for various compute and storage engines, including offline batch tasks, interactive query tasks, real-time streaming tasks and data lake tasks;
- **Unified context service** : supports cross-user, system and computing engine to associate and manage user and system resource files (JAR, ZIP, Properties, etc.), result sets, parameter variables, functions, UDFs, etc., one setting, automatic reference everywhere;
- **Unified materials** : provides system and user level material management, can share and flow, share materials across users, across systems;
- **Unified data source management** : provides the ability to add, delete, check and change information of Hive, ElasticSearch, Mysql, Kafka, MongoDB and other data sources, version control, connection test, and query metadata information of corresponding data sources;
- **Error code capability** : provides error codes and solutions for common errors of tasks, which is convenient for users to locate problems by themselves;

## Supported engine types

| Engine name | Support underlying component version(default dependency version) | Linkis Version Requirements | Included in Release Package By Default | Description |
| --- | --- | --- | --- | --- |
| Spark | Apache 2.0.0~2.4.7,CDH >= 5.4.0,(default Apache Spark 2.4.3) | >=1.0.3 | Yes | Spark EngineConn, supports SQL , Scala, Pyspark and R code |
| Hive | Apache >= 1.0.0,CDH >= 5.4.0,(default Apache Hive 2.3.3) | >=1.0.3 | Yes | Hive EngineConn, supports HiveQL code |
| Python | Python >= 2.6,(default Python2*) | >=1.0.3 | Yes | Python EngineConn, supports python code |
| Shell | Bash >= 2.0 | >=1.0.3 | Yes | Shell EngineConn, supports Bash shell code |
| JDBC | MySQL >= 5.0, Hive >=1.2.1,(default Hive-jdbc 2.3.4) | >=1.0.3 | No | JDBC EngineConn, already supports Mysql,Oracle,KingBase,PostgreSQL,SqlServer,DB2,Greenplum,DM,Doris,ClickHouse,TiDB,Starrocks,GaussDB and OceanBase, can be extended quickly Support other engines with JDBC Driver package, such as SQLite |
| Flink | Flink >= 1.12.2,(default Apache Flink 1.12.2) | >=1.0.2 | No | Flink EngineConn, supports FlinkSQL code, also supports starting a new Yarn in the form of Flink Jar Application |
| Pipeline | - | >=1.0.2 | No | Pipeline EngineConn, supports file import and export |
| openLooKeng | openLooKeng >= 1.5.0,(default openLookEng 1.5.0) | >=1.1.1 | No | openLooKeng EngineConn, supports querying data virtualization engine with Sql openLooKeng |
| Sqoop | Sqoop >= 1.4.6,(default Apache Sqoop 1.4.6) | >=1.1.2 | No | Sqoop EngineConn, support data migration tool Sqoop engine |
| Presto | Presto >= 0.180 | >=1.2.0 | No | Presto EngineConn, supports Presto SQL code |
| ElasticSearch | ElasticSearch >=6.0 | >=1.2.0 | No | ElasticSearch EngineConn, supports SQL and DSL code |
| Trino | Trino >=371 | >=1.3.1 | No | Trino EngineConn， supports Trino SQL code |
| Seatunnel | Seatunnel >=2.1.2 | >=1.3.1 | No | Seatunnel EngineConn， supportt Seatunnel SQL code |

## Download

Please go to the [Linkis releases page](https://github.com/apache/linkis/releases) to download a compiled distribution or a source code package of Linkis.

## Compile and deploy

Please follow [Compile Guide](/docs/latest/development/build) to compile Linkis from source code.  
Please refer to [Deployment\_Documents](/docs/latest/deployment/deploy-quick) to do the deployment.

## Examples and Guidance

- [Engine Usage Guidelines](/docs/latest/engine-usage/overview)
- [API Documentation](/docs/latest/api/overview)

## Documentation

The documentation of linkis is in [Linkis-WebSite](https://github.com/apache/linkis-website)

## Architecture

Linkis services could be divided into three categories: computation governance services, public enhancement services and microservice governance services.

- The computation governance services, support the 3 major stages of processing a task/request: submission -> preparation -> execution.
- The public enhancement services, including the material library service, context service, and data source service.
- The microservice governance services, including Spring Cloud Gateway, Eureka and Open Feign.

Below is the Linkis architecture diagram. You can find more detailed architecture docs in [Architecture](/docs/latest/architecture/overview).
![architecture](linkis.apache.org/assets/images/Linkis_1.0_architecture-e91c8fbabb890c6beaf4317cf22f5151.png)

Based on Linkis the computation middleware, we've built a lot of applications and tools on top of it in the big data platform suite [WeDataSphere](https://github.com/WeBankFinTech/WeDataSphere). Below are the currently available open-source projects.

![wedatasphere_stack_Linkis](linkis.apache.org/assets/images/wedatasphere_stack_Linkis-7f1308b2505ad1cdabf5e39ed185a804.png)

- [**DataSphere Studio** - Data Application Integration& Development Framework](https://github.com/WeBankFinTech/DataSphereStudio)
- [**Scriptis** - Data Development IDE Tool](https://github.com/WeBankFinTech/Scriptis)
- [**Visualis** - Data Visualization Tool](https://github.com/WeBankFinTech/Visualis)
- [**Schedulis** - Workflow Task Scheduling Tool](https://github.com/WeBankFinTech/Schedulis)
- [**Qualitis** - Data Quality Tool](https://github.com/WeBankFinTech/Qualitis)
- [**MLLabis** - Machine Learning Notebook IDE](https://github.com/WeBankFinTech/prophecis)

More projects upcoming, please stay tuned.

## Contributing

Contributions are always welcomed, we need more contributors to build Linkis together. either code, or doc or other supports that could help the community.  
For code and documentation contributions, please follow the [contribution guide](/community/how-to-contribute).

## Contact Us

Any questions or suggestions please kindly submit an issue.  
You can scan the QR code below to join our WeChat group to get more immediate response.

![introduction05](linkis.apache.org/assets/images/wedatasphere_contact_01-82be02c217a5c09915715064a0f348dc.png)

Meetup videos on [Bilibili](https://space.bilibili.com/598542776?from=search&seid=14344213924133040656).

## Who is Using Linkis

We opened [an issue](https://github.com/apache/linkis/issues/23) for users to feedback and record who is using Linkis.  
Since the first release of Linkis in 2019, it has accumulated more than **700** trial companies and **1000+** sandbox trial users, which involving diverse industries, from finance, banking, tele-communication, to manufactory, internet companies and so on.

[Edit this page](https://github.com/apache/linkis-website/edit/dev/versioned_docs/version-1.8.0/about/introduction.md)

---

<a id="linkis-apache-org-docs-latest-about-configuration-index"></a>

# Recommended Configuration | Apache Linkis

- [](/)
- About Linkis
- Recommended Configuration
Version: 1.8.0

On this page

# Recommended Configuration

## 1. Recommended configuration of hardware and software environment

Linkis builds a layer of computing middleware between the upper application and the underlying engine. As an open source distributed computing middleware, it can be well deployed and run on Intel architecture servers and mainstream virtualization environments, and supports mainstream Linux operating system environments

### 1.1. Linux operating system version requirements

| OS | Version |
| --- | --- |
| Red Hat Enterprise Linux | 7.0 and above |
| CentOS | 7.0 and above |
| Oracle Enterprise Linux | 7.0 and above |
| Ubuntu LTS | 16.04 and above |

> **Note:** The above Linux operating systems can run on physical servers and mainstream virtualization environments such as VMware, KVM, and XEN

### 1.2. Server recommended configuration

Linkis supports 64-bit general-purpose hardware server platforms running on the Intel x86-64 architecture. The following recommendations are made for the server hardware configuration of the production environment:

#### Production Environment

| CPU | Memory | Disk type | Network | Number of instances |
| --- | --- | --- | --- | --- |
| 16 cores + | 32GB + | SAS | Gigabit network card | 1+ |

> **Note:**
>
> - The above recommended configuration is the minimum configuration for deploying Linkis, and a higher configuration is strongly recommended for production environments
> - The hard disk size configuration is recommended to be 50GB+, and the system disk and data disk are separated

### 1.3. Software requirements

Linkis binary packages are compiled based on the following software versions:

| Component | Version | Description |
| --- | --- | --- |
| Hadoop | 3.3.4 |  |
| Hive | 3.1.3 |  |
| Spark | 3.2.1 |  |
| Flink | 1.12.2 |  |
| openLooKeng | 1.5.0 |  |
| Sqoop | 1.4.6 |  |
| ElasticSearch | 7.6.2 |  |
| Presto | 0.234 |  |
| Python | Python2 |  |

> **Note:**
> If the locally installed component version is inconsistent with the above, you need to modify the corresponding component version and compile the binary package yourself for installation.

### 1.4. Client web browser requirements

Linkis recommends Chrome version 73 for front-end access

## 2. Common scenarios

### 2.1 Open test mode

The development process requires a password-free interface, which can be replaced or appended to `linkis.properties`

| parameter name | default value | description |
| --- | --- | --- |
| wds.linkis.test.mode | false | Whether to enable debugging mode, if set to true, all microservices support password-free login, and all EngineConn open remote debugging ports |
| wds.linkis.test.user | hadoop | When wds.linkis.test.mode=true, the default login user for password-free login |

![](linkis.apache.org/assets/images/test-mode-ab84afe3444112b960a11dc9c04d24b1.png)

### 2.2 Login user settings

Apache Linkis uses configuration files to manage admin users by default, and this configuration can be replaced or appended to `linkis-mg-gateway.properties`. For multi-user access LDAP implementation.

| parameter name | default value | description |
| --- | --- | --- |
| wds.linkis.admin.user | hadoop | admin username |
| wds.linkis.admin.password | 123456 | Admin user password |

![](linkis.apache.org/assets/images/login-user-058dbc811831c9c5a3d647032f4b77a0.png)

### 2.3 LDAP Settings

Apache Linkis can access LDAP through parameters to achieve multi-user management, and this configuration can be replaced or added in `linkis-mg-gateway.properties`.

| parameter name | default value | description |
| --- | --- | --- |
| wds.linkis.ldap.proxy.url | None | LDAP URL address |
| wds.linkis.ldap.proxy.baseDN | None | LDAP baseDN address |
| wds.linkis.ldap.proxy.userNameFormat | None |  |

![](linkis.apache.org/assets/images/ldap-d8623897124ad621c0013d7e35c3bc22.png)

### 2.4 OAuth Settings

Apache Linkis can use OAuth to authenticate users, and this configuration can be replaced or added in `linkis-mg-gateway.properties`.

| parameter name | default value | description |
| --- | --- | --- |
| wds.linkis.gateway.conf.enable.oauth.auth | false | Whether to enable OAuth authentication |
| wds.linkis.gateway.auth.oauth.authentication.url |  | OAuth 2.0 authorization endpoint URL for obtaining authorization code |
| wds.linkis.gateway.auth.oauth.exchange.url |  | Token exchange endpoint URL for converting authorization code to access token |
| wds.linkis.gateway.auth.oauth.validate.url |  | User validation endpoint URL for retrieving user identity via access token |
| wds.linkis.gateway.auth.oauth.validate.field |  | JSON response field name containing username |
| wds.linkis.gateway.auth.oauth.client.id |  | OAuth client ID |
| wds.linkis.gateway.auth.oauth.client.secret |  | OAuth client secret |
| wds.linkis.gateway.auth.oauth.scope |  | OAuth scope |

### 2.5 Turn off resource checking

Apache Linkis sometimes debugs exceptions when submitting tasks, such as: insufficient resources; you can replace or append this configuration in `linkis-cg-linkismanager.properties`.

| parameter name | default value | description |
| --- | --- | --- |
| wds.linkis.manager.rm.request.enable | true | resource check |

![](linkis.apache.org/assets/images/resource-enable-548b1bfdb4f8e206c4ddbd4485f65a73.png)

### 2.6 Enable engine debugging

Apache Linkis EC can enable debugging mode, and this configuration can be replaced or added in `linkis-cg-linkismanager.properties`.

| parameter name | default value | description |
| --- | --- | --- |
| wds.linkis.engineconn.debug.enable | true | Whether to enable engine debugging |

![](linkis.apache.org/assets/images/engine-debug-d6d34a5e0ff4318bee1c2ec6145d2aad.png)

### 2.7 Hive metadata configuration

The public-service service of Apache Linkis needs to read hive metadata; this configuration can be replaced or appended in `linkis-ps-publicservice.properties`.

| parameter name | default value | description |
| --- | --- | --- |
| hive.meta.url | None | The URL of the HiveMetaStore database. |
| hive.meta.user | none | user of the HiveMetaStore database |
| hive.meta.password | None | password for the HiveMetaStore database |

![](linkis.apache.org/assets/images/hive-meta-d9866b2a627ba0e9323b902bce2c5c94.png)

### 2.8 Linkis database configuration

Apache Linkis access uses Mysql as data storage by default, you can replace or append this configuration in `linkis.properties`.

| parameter name | default value | description |
| --- | --- | --- |
| wds.linkis.server.mybatis.datasource.url | None | Database connection string, for example: jdbc:mysql://127.0.0.1:3306/dss?characterEncoding=UTF-8 |
| wds.linkis.server.mybatis.datasource.username | None | Database user name, for example: root |
| wds.linkis.server.mybatis.datasource.password | None | Database password, for example: root |

![](linkis.apache.org/assets/images/linkis-db-c9e5a90db880655bc6169b28fbc5822c.png)

### 2.9 Linkis Session cache configuration

Apache Linkis supports using redis for session sharing; this configuration can be replaced or appended in `linkis.properties`.

| parameter name | default value | description |
| --- | --- | --- |
| linkis.session.redis.cache.enabled | None | Whether to enable |
| linkis.session.redis.host | 127.0.0.1 | hostname |
| linkis.session.redis.port | 6379 | Port, eg |
| linkis.session.redis.password | None | password |

![](linkis.apache.org/assets/images/redis-93c88ca8d619cc6b131b5b77d31097c3.png)

### 2.10 Linkis module development configuration

When developing Apache Linkis, you can use this parameter to customize the database, Rest interface, and entity objects of the loading module; you can modify it in `linkis-ps-publicservice.properties`, and use commas to separate multiple modules.

| parameter name | default value | description |
| --- | --- | --- |
| wds.linkis.server.restful.scan.packages | None | restful scan packages, for example: org.apache.linkis.basedatamanager.server.restful |
| wds.linkis.server.mybatis.mapperLocations | None | Mybatis mapper file path, for example: classpath:org/apache/linkis/basedatamanager/server/dao/mapper/.xml |
| wds.linkis.server.mybatis.typeAliasesPackage | None | Entity alias scanning package, for example: org.apache.linkis.basedatamanager.server.domain |
| wds.linkis.server.mybatis.BasePackage | None | Database dao layer scan, for example: org.apache.linkis.basedatamanager.server.dao |

![](linkis.apache.org/assets/images/deverlop-conf-09543dc2b1c9e5b57ffbc9f87a402873.png)

### 2.11 Linkis module development configuration

This parameter can be used to customize the route of loading modules during Apache Linkis development; it can be modified in `linkis.properties`, and commas are used to separate multiple modules.

| parameter name | default value | description |
| --- | --- | --- |
| wds.linkis.gateway.conf.publicservice.list | cs,contextservice,data-source-manager,metadataQuery,metadatamanager,query,jobhistory,application,configuration,filesystem,udf,variable,microservice,errorcode,bml,datasource,basedata -manager | publicservice services support routing modules |

![](linkis.apache.org/assets/images/list-conf-63213b57ab66a41758c14cdb731a9731.png)

### 2.12 Linkis file system and material storage path

This parameter can be used to customize the route of loading modules during Apache Linkis development; it can be modified in `linkis.properties`, and commas are used to separate multiple modules.

| parameter name | default value | description |
| --- | --- | --- |
| wds.linkis.filesystem.root.path | file:///tmp/linkis/ | Local user directory, a folder named after the user name needs to be created under this directory |
| wds.linkis.filesystem.hdfs.root.path | hdfs:///tmp/ | HDFS user directory |
| wds.linkis.bml.is.hdfs | true | Whether to enable hdfs |
| wds.linkis.bml.hdfs.prefix | /apps-data | hdfs path |
| wds.linkis.bml.local.prefix | /apps-data | local path |

![](linkis.apache.org/assets/images/fs-conf-4dd57caa5f812496174bda5b25d6f36b.png)

[Edit this page](https://github.com/apache/linkis-website/edit/dev/versioned_docs/version-1.8.0/about/configuration.md)

---

<a id="linkis-apache-org-docs-latest-about-glossary-index"></a>

# Glossary | Apache Linkis

- [](/)
- About Linkis
- Glossary
Version: 1.8.0

On this page

# Glossary

## 1. Glossary

Linkis is developed based on the microservice architecture, and its services can be divided into 3 types of service groups (groups): computing governance service group, public enhancement service group and microservice governance service group.

- Computation Governance Services: The core service for processing tasks, supporting the 4 main stages of the computing task/request processing flow (submit->prepare->execute->result);
- Public Enhancement Services: Provide basic support services, including context services, engine/udf material management services, job history and other public services and data source management services;
- Microservice Governance Services: Customized Spring Cloud Gateway, Eureka. Provides a base for microservices

The following will introduce the key Glossary and services of these three groups of services:

### 1.1 Key module nouns

| Abbreviation | Name | Main Functions |
| --- | --- | --- |
| MG/mg | Microservice Governance | Microservice Governance |
| CG/cg | Computation Governance | Computation Governance |
| EC/ec | EngineConn | Engine Connector |
| - | Engine | The underlying computing storage engine, such as spark, hive, shell |
| ECM/ecm | EngineConnManager | Management of Engine Connectors |
| ECP/ecp | EngineConnPlugin | Engine Connector Plugin |
| RM/rm | ResourceManager | Resource manager for managing task resource and user resource usage and control |
| AM/am | AppManager | Application Manager to manage EngineConn and ECM services |
| LM/lm | LinkisManager | Linkis manager service, including: RM, AM, LabelManager and other modules |
| PES/pes | Public Enhancement Services |  |
| - | Orchestrator | Orchestrator, used for Linkis task orchestration, task multi-active, mixed calculation, AB and other policy support |
| UJES | Unified Job Execute Service | Unified Job Execute Service |
| DDL/ddl | Data Definition Language | Database Definition Language |
| DML/dml | Data Manipulation Language | Data Manipulation Language |

### 1.2 Mission key nouns

- JobRequest: job request, corresponding to the job submitted by the Client to Linkis, including the execution content, user, label and other information of the job
- RuntimeMap: task runtime parameters, task level take effect, such as data source information for placing multiple data sources
- StartupMap: Engine connector startup parameters, used to start the EngineConn connected machine, the EngineConn process takes effect, such as setting spark.executor.memory=4G
- UserCreator: Task creator information: contains user information User and Client submitted application information Creator, used for tenant isolation of tasks and resources
- submitUser: task submit user
- executeUser: the real execution user of the task
- JobSource: Job source information, record the IP or script address of the job
- errorCode: error code, task error code information
- JobHistory: task history persistence module, providing historical information query of tasks
- ResultSet: The result set, the result set corresponding to the task, is saved with the .dolphin file suffix by default
- JobInfo: Job runtime information, such as logs, progress, resource information, etc.
- Resource: resource information, each task consumes resources
- RequestTask: The smallest execution unit of EngineConn, the task unit transmitted to EngineConn for execution

## 2. Service Introduction

This section mainly introduces the services of Linkis, what services will be available after Linkis is started, and the functions of the services.

## 2.1 Service List

After Linkis is started, the microservices included in each service group (group) are as follows:

| Belonging to the microservice group (group) | Service name | Main functions |
| --- | --- | --- |
| MGS | linkis-mg-eureka | Responsible for service registration and discovery, other upstream components will also reuse the linkis registry, such as dss |
| MGS | linkis-mg-gateway | As the gateway entrance of Linkis, it is mainly responsible for request forwarding and user access authentication |
| CGS | linkis-cg-entrance | The task submission entry is a service responsible for receiving, scheduling, forwarding execution requests, and life cycle management of computing tasks, and can return calculation results, logs, and progress to the caller |
| CGS | linkis-cg-linkismanager | Provides AppManager (application management), ResourceManager (resource management), LabelManager (label management), Engine connector plug-in manager capabilities |
| CGS | linkis-cg-engineconnmanager | Manager for EngineConn, providing lifecycle management of engines |
| CGS | linkis-cg-engineconn | The engine connector service is the actual connection service with the underlying computing storage engine (Hive/Spark), including session information with the actual engine. For the underlying computing storage engine, it acts as a client and is triggered and started by tasks |
| PES | linkis-ps-publicservice | Public Enhanced Service Group Module Service, which provides functions such as unified configuration management, context service, BML material library, data source management, microservice management, and historical task query for other microservice modules |

All services seen by open source after startup are as follows:
![Linkis_Eureka](linkis.apache.org/assets/images/Linkis_combined_eureka-df062f77476458ba147b315953ebd60e.png)

## 2.1 Detailed explanation of public enhanced services

After version 1.3.1, the Public Enhanced Service Group (PES) merges related module services into one service linkis-ps-publicservice by default to provide related functions. Of course, if you want to deploy separately, it is also supported. You only need to package and deploy the services of the corresponding modules.
The combined public enhanced service mainly includes the following functions:

| Abbreviation | Service Name | Main Functions |
| --- | --- | --- |
| CS/cs | Context Service | Context Service, used to transfer result sets, variables, files, etc. between tasks |
| UDF/udf | UDF | UDF management module, provides management functions for UDF and functions, supports sharing and version control |
| variable | Variable | Global custom module, providing management functions for global custom variables |
| script | Script-dev | Script file operation service, providing script editing and saving, script directory management functions |
| jobHistory | JobHistory | Task history persistence module, providing historical information query of tasks |
| BML/bml | BigData Material library |  |
| - | Configuration | Configuration management, providing management and viewing of configuration parameters |
| - | instance-label | Microservice management service, providing mapping management functions for microservices and routing labels |
| - | error-code | Error code management, providing the function of managing through error codes |
| DMS/dms | Data Source Manager Service | Data Source Management Service |
| MDS/mds | MetaData Manager Service | Metadata Management Service |
| - | linkis-metadata | Provides Hive metadata information viewing function, which will be merged into MDS later |
| - | basedata-manager | Basic data management, used to manage Linkis' own basic metadata information |

### 3 Module Introduction

This section mainly introduces the major modules and functions of Linkis.

- linkis-commons: The public modules of linkis, including public tool modules, RPC modules, microservice foundation and other modules
- linkis-computation-governance: Computing governance module, including modules for computing governance multiple services: Entrance, LinkisManager, EngineConnManager, EngineConn, etc.
- linkis-engineconn-plugins: Engine connector plugin module, contains all engine connector plugin implementations
- linkis-extensions: The extension enhancement module of Linkis, not a necessary function module, now mainly includes the IO module for file proxy operation
- linkis-orchestrator: Orchestration module for Linkis task orchestration, advanced strategy support such as task multi-active, mixed calculation, AB, etc.
- linkis-public-enhancements: public enhancement module, which contains all public services for invoking linkis internal and upper-layer application components
- linkis-spring-cloud-services: Spring cloud related service modules, including gateway, registry, etc.
- linkis-web: front-end module

[Edit this page](https://github.com/apache/linkis-website/edit/dev/versioned_docs/version-1.8.0/about/glossary.md)