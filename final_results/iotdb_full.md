<a id="iotdb-apache-org-userguide-latest-iotdb-introduction-iotdb-introduction_apache"></a>

# IoTDB Introduction | IoTDB Website

# IoTDB Introduction

9/29/24About 2 min

---

On This Page

- [1. Product Ecosystem](#iotdb-apache-org-userguide-latest-iotdb-introduction-iotdb-introduction_apache--_1-product-ecosystem)
- [2. IoTDB Architecture](#iotdb-apache-org-userguide-latest-iotdb-introduction-iotdb-introduction_apache--_2-iotdb-architecture)
- [3. Key Features](#iotdb-apache-org-userguide-latest-iotdb-introduction-iotdb-introduction_apache--_3-key-features)
- [4. TimechoDB](#iotdb-apache-org-userguide-latest-iotdb-introduction-iotdb-introduction_apache--_4-timechodb)

# [IoTDB Introduction](#iotdb-apache-org-userguide-latest-iotdb-introduction-iotdb-introduction_apache--iotdb-introduction)

Apache IoTDB is a low-cost, high-performance IoT-native time-series database. It addresses challenges faced by enterprises in managing time-series data for IoT big data platforms, including complex application scenarios, massive data volumes, high sampling frequencies, frequent out-of-order data, time-consuming data processing, diverse analytical requirements, and high storage and maintenance costs.

- GitHub Repository: [https://github.com/apache/iotdb](https://github.com/apache/iotdb)
- Open-Source Installation Packages: [https://iotdb.apache.org/Download/](https://iotdb.apache.org/Download/)
- Installation, Deployment, and Usage Documentation: [Quick Start](/UserGuide/latest/QuickStart/QuickStart_apache.html)

## [1. Product Ecosystem](#iotdb-apache-org-userguide-latest-iotdb-introduction-iotdb-introduction_apache--_1-product-ecosystem)

The IoTDB ecosystem consists of multiple components designed to efficiently manage and analyze massive IoT-generated time-series data.

![Introduction-en-apache.png](/img/Introduction-en-apache.png)

Key components include:

1. **Time-Series Database (Apache IoTDB)**: The core component for time-series data storage, offering high compression, rich query capabilities, real-time stream processing, high availability, and scalability. It provides security guarantees, configuration tools, multi-language APIs, and integration with external systems for building business applications.
2. **Time-Series File Format (Apache TsFile)**: A specialized storage format for time-series data, enabling efficient storage and querying. TsFile underpins IoTDB and AINode, unifying data management across collection, storage, and analysis phases.
3. **Time-Series Model Training-Inference Engine (IoTDB AINode)**: A unified engine for intelligent analysis, supporting model training, data management, and integration with machine/deep learning frameworks.

## [2. IoTDB Architecture](#iotdb-apache-org-userguide-latest-iotdb-introduction-iotdb-introduction_apache--_2-iotdb-architecture)

The diagram below illustrates a typical IoTDB cluster deployment (3 ConfigNodes and 3 DataNodes):

![](/img/Cluster-Concept03N.png)

## [3. Key Features](#iotdb-apache-org-userguide-latest-iotdb-introduction-iotdb-introduction_apache--_3-key-features)

Apache IoTDB offers the following advantages:

- **Flexible Deployment**:

  - One-click cloud deployment
  - Out-of-the-box terminal usage
  - Seamless terminal-cloud synchronization
- **Cost-Effective Storage**:

  - High-compression disk storage
  - Unified management of historical and real-time data
- **Hierarchical Measurement Point Management**:

  - Aligns with industrial device hierarchies
  - Supports directory browsing and search
- **High Throughput Read/Write**:

  - Supports millions of devices
  - Handles high-speed, out-of-order, and multi-frequency data ingestion
- **Rich Query Capabilities**:

  - Native time-series computation engine
  - Timestamp alignment during queries
  - Over 100 built-in aggregation and time-series functions
  - AI-ready time-series feature analysis
- **High Availability & Scalability**:

  - HA distributed architecture with 24/7 uptime
  - Automatic load balancing for node scaling
  - Heterogeneous cluster support
- **Low Learning Curve**:

  - SQL-like query language
  - Multi-language SDKs
  - Comprehensive toolchain (e.g., console)
- **Ecosystem Integration**:

  - Hadoop, Spark, Grafana, ThingsBoard, DataEase, etc.

## [4. TimechoDB](#iotdb-apache-org-userguide-latest-iotdb-introduction-iotdb-introduction_apache--_4-timechodb)

Timecho Technology has developed **TimechoDB**, a commercial product based on Apache IoTDB, to provide enterprise-grade solutions and services for businesses and commercial clients. TimechoDB addresses the multifaceted challenges enterprises face when building IoT big data platforms for managing time-series data, including complex application scenarios, massive data volumes, high sampling frequencies, frequent out-of-order data, time-consuming data processing, diverse analytical requirements, and high storage and maintenance costs.

Leveraging **TimechoDB**, Timecho Technology offers a broader range of product features, enhanced performance and stability, and a richer suite of efficiency tools. Additionally, it provides comprehensive enterprise services, delivering commercial clients with superior product capabilities and an optimized experience in development, operation, and usage.

- **Timecho Technology Official Website**: [https://www.timecho.com/](https://www.timecho.com/)
- **TimechoDB Documentation**: [Quick Start](https://www.timecho.com/docs/zh/UserGuide/latest/QuickStart/QuickStart_timecho.html)

[Found Error? Edit this page on GitHub](https://github.com/apache/iotdb-docs/edit/main/src/UserGuide/latest/IoTDB-Introduction/IoTDB-Introduction_apache.md)

Last Updated: 4/10/26, 5:33 AM

[

Next

Scenario

](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario)

---

<a id="iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache"></a>

# Release History | IoTDB Website

# Release History

8/18/25About 9 min

---

On This Page

- [V2.0.8](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-8)
- [V2.0.7](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-7)
- [V2.0.6](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-6)
- [V2.0.5](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-5)
- [V2.0.4](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-4)
- [V2.0.3](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-3)
- [V2.0.2](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-2)
- [V2.0.1-beta](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-1-beta)
- [V1.3.7](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-7)
- [V1.3.6](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-6)
- [V1.3.5](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-5)
- [V1.3.4](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-4)
- [V1.3.3](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-3)
- [V1.3.2](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-2)
- [V1.3.1](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-1)
- [V1.3.0](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-0)
- [V1.2.0](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-2-0)
- [V1.1.0](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-1-0)
- [V1.0.0](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-0-0)

# [Release History](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--release-history)

## [V2.0.8](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-8)

> Release Date: 2026.04.14

V2.0.8 expands AI model capabilities by adding support for Chronos-2, one of the mainstream time-series foundation models, while enabling concurrent inference for built-in large models. On the database side, TIME columns now support custom naming, data synchronization path configuration flexibility has been optimized, and comprehensive improvements have been made to database monitoring, performance, and stability. The specific release contents are as follows:

- **Query Module**: Added functionality to display the list of available DataNode nodes
- **Query Module**: Added system table for统计查询延迟信息 in the table model
- **Query Module**: Python SessionDataset now supports converting TsBlock to DataFrame and returning DataFrames in batches
- **Storage Module**: TIME column now supports custom column naming
- **Storage Module**: Added SQL support for viewing the complete definition statements of created tables/views
- **Stream Processing Module**: Pipe synchronization now supports excluding specified devices/measurement points from synchronization
- **Stream Processing Module**: Multiple exact paths can now be specified in a single Pipe
- **Stream Processing Module**: When filtering paths in Pipe, `source.pattern` and `source.path` parameters can now be used together with comma separation
- **AI Module**: Built-in Chronos-2 model with prediction support
- **AI Module**: Timer-XL and Sundial built-in models now support concurrent inference
- **System Module**: Added system table for displaying DataNode node connection status in the table model
- **Miscellaneous**: Fixed security vulnerabilities CVE-2025-12183, CVE-2025-66566, and CVE-2025-11226

## [V2.0.7](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-7)

> Release Date: 2026.03.04

V2.0.7 focuses on security hardening and stability optimization. It removes high-risk RPC interfaces and JEXL functions, strengthens naming convention validation and service address configuration logic, optimizes the automatic deletion mechanism for partitioned tables, and provides comprehensive improvements to database monitoring, performance, and stability. The specific release contents are as follows:

- **Miscellaneous**: Removed high-risk RPC interfaces
- **Miscellaneous**: Removed JEXL functions
- **Miscellaneous**: Added naming合法性校验 when creating Pipe
- **Miscellaneous**: Changed the default client RPC service address to 127.0.0.1
- **Miscellaneous**: Adjusted code logic so that internal services bind to the address configured by `dn_internal_address` instead of the default address

## [V2.0.6](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-6)

> Release Date: 2026.01.20

V2.0.6 is the official release of the dual-mode (tree and table) architecture. It introduces **query-writeback capability for the table mode**, new **bitwise operation functions** (built-in scalar functions), and **push-down-capable time functions**, while delivering comprehensive improvements in database monitoring, performance, and stability. Specific release contents are as follows:

- **Query Module**: Added support for query-writeback functionality in the table mode.
- **Query Module**: Enhanced row-pattern recognition in the table mode to support aggregate functions, enabling analysis and computation over consecutive data sequences.
- **Query Module**: Introduced built-in scalar bitwise operation functions for the table mode.
- **Query Module**: Added a push-down-capable `EXTRACT` time function for the table mode.
- **Others**: Fixed security vulnerabilities CVE-2025-12183, CVE-2025-66566, and CVE-2025-11226.

## [V2.0.5](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-5)

> Release Date: 2025.08.21

V2.0.5, as the official release of the Dual-Mode Tree-Table system, primarily introduces the tree-to-table view, window functions for the table mode, the aggregate function approx\_most\_frequent, and supports LEFT & RIGHT JOIN as well as ASOF LEFT JOIN. The AINode now includes two new built-in models, Timer-XL and Timer-Sundial, and supports inference capabilities for both tree and table models. Additionally, this version brings comprehensive improvements to database monitoring, performance, and stability. The specific updates are as follows:

- **Query Module**: Supports manual creation of tree-to-table views
- **Query Module**: Adds window functions for the table mode
- **Query Module**: Adds the aggregate function approx\_most\_frequent for the table model
- **Query Module**: Extends JOIN functionality for the table model, supporting LEFT & RIGHT JOIN and ASOF LEFT JOIN
- **Query Module**: The table model now supports row pattern recognition, enabling the capture of continuous data for analysis and computation
- **Storage Module**: Adds multiple system tables for the table model, such as VIEWS (table view information) and MODELS (model information)
- **AI Module**: AINode adds two new built-in models, Timer-XL and Timer-Sundial
- **AI Module**: AINode supports inference functions for both tree and table models

## [V2.0.4](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-4)

> Release Date: 2025.07.09

V2.0.4 serves as the official release of the dual-model system for tree and table structures. The table model primarily introduces user-defined table functions (UDTF) and multiple built-in table functions, adds the aggregate function approx\_count\_distinct, and supports ASOF INNER JOIN for time columns. Additionally, script tools have been categorized and reorganized, with Windows-specific scripts separated. The release also includes comprehensive improvements in database monitoring, performance, and stability. The detailed updates are as follows:

- **Query Module**: The table model introduces user-defined table functions (UDTF) and multiple built-in table functions.
- **Query Module**: The table model supports ASOF INNER JOIN for time columns.
- **Query Module**: The table model adds the aggregate function approx\_count\_distinct.
- **Stream Processing**: Supports asynchronous loading of TsFile via SQL.
- **System Module**: During capacity reduction, replica selection now supports disaster recovery load balancing strategies.
- **System Module**: Compatibility with Windows Server 2025 has been added.
- **Scripts & Tools**: Script tools have been categorized and reorganized, with Windows-specific scripts separated.

## [V2.0.3](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-3)

> Release Date: 2025.05.30

As the official release of the dual tree-table model, V2.0.3 primarily introduces metadata import/export script adaptation for the table model, Spark ecosystem integration (table model), timestamp addition to AINode results, and new aggregate/scalar functions for the table model. Comprehensive improvements have been made to database monitoring, performance, and stability. Key updates include:

- ​**​Query Module​**​:
  - New aggregate function `count_if` and scalar functions `greatest/least` for table model
  - Significant performance improvement for full-table `count(*)` queries in table model
- ​**​AI Module​**​:
  - AINode results now include timestamps
- ​**​System Module​**​:
  - Optimized metadata module performance for table model
  - Added proactive TsFile monitoring and loading for table model
  - Python/Go client query interfaces now support TsBlock deserialization
- ​**​Ecosystem Integration​**​:
  - Spark integration for table model
- ​**​Scripts & Tools​**​:
  - `import-schema/export-schema` scripts now support table model metadata import/export

## [V2.0.2](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-2)

> Release Date: 2025.04.18

As the official release of the dual tree-table model, V2.0.2 introduces table model permission management, user management, and related authentication, along with UDFs, system tables, and nested queries for the table model. Comprehensive improvements to monitoring, performance, and stability include:

- ​**​Query Module​**​:
  - Added UDF management, user-defined scalar functions (UDSF), and aggregate functions (UDAF) for table model
  - Permission/user management and operation authentication for table model
  - New system tables and administrative statements
- ​**​System Module​**​:
  - Full isolation between tree and table models at database level
  - Built-in MQTT Service adapted for table model
  - C# and Go clients now support table model
  - New C++ Session write interface for table model
- ​**​Data Sync​**​:
  - Metadata synchronization and sync-delete operations for table model
- ​**​Scripts & Tools​**​:
  - `import-data/export-data` scripts now support table model and local TsFile load

## [V2.0.1-beta](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v2-0-1-beta)

> Release Date: 2025.02.18

V2.0.1-beta introduces dual tree-table model configuration, supporting standard SQL query syntax, various functions/operators, stream processing, and Benchmark capabilities for the table model. Additional updates include:

- ​**​Table Mode​**​:
  - Supports standard SQL syntax (SELECT/WHERE/JOIN/GROUP BY/ORDER BY/LIMIT/subqueries)
  - Various functions including logical operators, mathematical functions, and time-series functions like DIFF
- ​**​Storage Module​**​:
  - Python client adds support for four new data types: String, Blob, Date, Timestamp
  - Optimized merge task priority rules
- ​**​Stream Processing​**​:
  - Supports specifying authentication info at sender
  - TsFile Load supports table model
  - Stream processing plugins adapted for table model
- ​**​System Module​**​:
  - Enhanced DataNode scaling stability
  - Supports DROP DATABASE in readonly mode
- ​**​Scripts & Tools​**​:
  - Benchmark tool adapted for table model
  - Supports four new data types: String, Blob, Date, Timestamp
  - Unified import/export support for TsFile, CSV and SQL formats
- ​**​Ecosystem Integration​**​:
  - Kubernetes Operator support

## [V1.3.7](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-7)

> Release Date: 2026.03.04

V1.3.7 focuses on security hardening and stability optimization. It removes high-risk RPC interfaces and JEXL functions, strengthens naming convention validation and service address configuration logic, optimizes the automatic deletion mechanism for partitioned tables, and provides comprehensive improvements to database monitoring, performance, and stability. The specific release contents are as follows:

- **Miscellaneous**: Removed high-risk RPC interfaces
- **Miscellaneous**: Removed JEXL functions
- **Miscellaneous**: Added naming合法性校验 when creating Pipe
- **Miscellaneous**: Changed the default client RPC service address to 127.0.0.1
- **Miscellaneous**: Adjusted code logic so that internal services bind to the address configured by `dn_internal_address` instead of the default address

## [V1.3.6](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-6)

> Release Date: 2026.01.20

V1.3.6 is a maintenance update within the 1.X series, delivering deep optimizations across three core areas: query performance, data synchronization stability, and memory management mechanisms—resulting in comprehensive enhancements to database monitoring, performance, and overall system stability. The specific release contents are as follows:

- **Query Module**: Optimized query performance in multiple scenarios, including multi-series Last queries.
- **Query Module**: Added a new FastLastQuery interface in the Java SDK to support more efficient Last query operations.
- **Query Module**: Adjusted the tree model’s fetchSchema to return results in segmented streaming mode, improving response speed in large-data-volume scenarios.
- **Storage Module**: Enhanced memory management to prevent memory leaks and ensure long-term system stability.
- **Storage Module**: Optimized the file compaction mechanism to improve compaction efficiency and reduce storage resource consumption.
- **Data Synchronization**: Improved Pipe SQL parameter configuration to support specifying asynchronous loading methods.
- **Data Synchronization**: Introduced syntactic sugar to automatically split full-data Pipe creation SQL into real-time and historical synchronization components.
- **System Module**: Added a global configuration option for data-type-specific compression strategies, enabling on-demand tuning of storage compression policies.
- **Others**: Fixed security vulnerabilities CVE-2025-12183, CVE-2025-66566, and CVE-2025-11226.

## [V1.3.5](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-5)

> Release Date: 2025.09.12

V1.3.5, as a bugfix release based on the previous 1.3.x versions, primarily adjusts the user password encryption algorithm to further enhance data access security. It also optimizes kernel stability and addresses issues reported by the community.

## [V1.3.4](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-4)

> Release Date: 2025.04.18

V1.3.4 primarily introduces pattern matching functions, continuously optimizes the data subscription mechanism, improves stability, merges data import/export scripts, and extends support for new data types. It also delivers comprehensive enhancements in database monitoring, performance, and stability. Key updates include:

- ​**​Query Module​**​:
  - Users can now configure UDFs, PipePlugins, Triggers, and AINodes to load JARs via URI.
  - Added monitoring for cached TimeIndex during merge operations.
- ​**​System Module​**​:
  - Extended UDF functionality with the new `pattern_match` pattern-matching function.
  - Python Session SDK now supports connection timeout parameters.
  - Added authentication for cluster management operations.
  - ConfigNode/DataNode now supports scaling down via SQL.
  - ConfigNode automatically cleans up partition information exceeding TTL (every 2 hours).
- ​**​Data Synchronization​**​:
  - Supports specifying authentication information at the sender.
- ​**​Ecosystem Integration​**​:
  - Added Kubernetes Operator support.
- ​**​Scripts & Tools​**​:
  - Extended `import-data/export-data` scripts to support new data types (strings, BLOBs, dates, timestamps).
  - Unified script support for importing/exporting TsFile, CSV, and SQL data formats.

## [V1.3.3](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-3)

> Release Date: 2024.11.20

V1.3.3 introduces support for ​**​String, Blob, Date, and Timestamp​**​ data types, enhances data subscription capabilities, enables DataNodes to actively monitor and load TsFiles, and adds observability metrics. It also optimizes configuration file integration, client query load balancing, and more, while addressing bugs and performance issues. Key updates:

- ​**​Storage Module​**​:
  - New data types: String, Blob, Date, Timestamp.
  - Improved memory control during merge operations.
- ​**​Query Module​**​:
  - Optimized client query request load balancing.
  - Added active metadata statistics queries.
  - Enhanced Filter performance for faster aggregation and WHERE queries.
- ​**​Data Synchronization​**​:
  - Senders can now transfer files to a specified directory, with receivers automatically loading them into IoTDB.
  - Added automatic data type conversion at the receiver.
- ​**​Data Subscription​**​:
  - New subscription capability for data points or TsFile-based updates.
- ​**​Data Loading​**​:
  - DataNodes actively monitor and load TsFiles with added observability metrics.
- ​**​Stream Processing​**​:
  - `ALTER PIPE` now supports `ALTER SOURCE`.
- ​**​System Module​**​:
  - Simplified configuration files (merged into one).
  - Added configuration interface settings.
  - Improved restart recovery performance.
- ​**​Scripts & Tools​**​:
  - New metadata import/export scripts.
  - Added Kubernetes Helm support.

## [V1.3.2](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-2)

> Release Date: 2024.07.01

V1.3.2 introduces ​**​`EXPLAIN ANALYZE`​**​ for SQL query profiling, a ​**​UDAF framework​**​, metadata synchronization, and tools for counting data points under specified paths. It also supports rolling cluster upgrades and plugin distribution. Key updates:

- ​**​Query Module​**​:
  - `EXPLAIN ANALYZE` to profile SQL execution stages.
  - New UDAF (User-Defined Aggregation Function) framework.
  - `MaxBy/MinBy` functions to retrieve timestamps with max/min values.
- ​**​Data Sync​**​:
  - Wildcard support for path matching.
  - Metadata synchronization (including time series attributes and permissions).
- ​**​System Module​**​:
  - TsFile load operations now contribute to data point statistics.
- ​**​Scripts & Tools​**​:
  - Local upgrade/backup tools (via hard links).
  - `export-data/import-data` scripts for CSV/TsFile/SQL formats.
  - Windows support for distinguishing ConfigNode/DataNode/Cli via window titles.

## [V1.3.1](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-1)

> Release Date: 2024.04.22

V1.3.1 introduces several new features including one-click cluster control scripts, instance information collection scripts, and multiple built-in functions. It also optimizes existing data synchronization, log output strategies, and query execution processes, while enhancing system observability and addressing various product bugs and performance issues. Key updates include:

- Added one-click cluster start/stop scripts (start-all/stop-all.sh & start-all/stop-all.bat)
- Added one-click instance information collection scripts ([collect-info.sh](http://collect-info.sh) & collect-info.bat)
- New built-in aggregate functions: standard deviation and variance
- Added TsFile repair command
- FILL clause now supports timeout threshold setting (no filling when exceeding time limit)
- Simplified time range specification for data synchronization (direct start/end time setting)
- Enhanced system observability (added cluster node divergence monitoring and distributed task scheduling observability)
- Optimized default log output strategy
- Improved memory control for Load TsFile operations (full-process coverage)
- REST interface (V2) now returns column types
- Optimized query execution process
- Clients now automatically fetch available DataNode lists

## [V1.3.0](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-3-0)

> Release Date: 2024.01.01

V1.3.0 introduces new features including SSL communication encryption and data synchronization monitoring statistics. It optimizes the syntax and logic of the permission module, metrics algorithm library performance, Python client write performance, and query efficiency in specific scenarios, while fixing various product bugs and performance issues. Key updates include:

- Security Module:
  - Enhanced permission module with time-series granular permission control
  - Added SSL communication encryption for client-server connections
- Query Module:
  - Calculation-type views now support LAST queries
- Stream Processing:
  - Added pipe-related monitoring metrics
- Storage Module:
  - Support for negative timestamp writing
- Scripts & Tools:
  - Load script imported data now included in data point monitoring statistics
- Client Module:
  - Optimized Python client performance
- Query Module Improvements:
  - Resolved long response time for SHOW PATH commands
  - Improved EXPLAIN statement display alignment
- System Module:
  - Added unified memory configuration item MEMORY\_SIZE to environment configuration scripts
  - Renamed configuration item target\_config\_node\_list to seed\_config\_node
  - Renamed configuration item storage\_query\_schema\_consensus\_free\_memory\_proportion to datanode\_memory\_proportion

## [V1.2.0](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-2-0)

> Release Date: 2023.06.30

V1.2.0 introduces major new features including a stream processing framework, dynamic templates, and built-in query functions (substring/replace/round). It enhances built-in statements like SHOW REGION/SHOW TIMESERIES/SHOW VARIABLE and Session interfaces, while optimizing built-in monitoring metrics and fixing various bugs and performance issues.

- ​**​Stream Processing​**​: New stream processing framework
- ​**​Metadata Module​**​: Added dynamic template expansion
- ​**​Storage Module​**​: New SPRINTZ and RLBE encoding schemes with LZMA2 compression
- ​**​Query Module​**​:
  - New built-in scalar functions: CAST, ROUND, SUBSTR, REPLACE
  - New aggregate functions: TIME\_DURATION, MODE
  - SQL now supports CASE WHEN syntax
  - SQL now supports ORDER BY expressions
- ​**​Interface Module​**​:
  - Python API supports connecting to multiple distributed nodes
  - Python client supports write redirection
  - Session API adds batch time series creation via templates

## [V1.1.0](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-1-0)

> Release Date: 2023.04.03

V1.1.0 introduces new segmentation methods (GROUP BY VARIATION/CONDITION) and utility functions (DIFF, COUNT\_IF), along with a pipeline execution engine for faster queries. It also fixes issues including:

- Aligned sequence LAST queries with ORDER BY TIMESERIES
- LIMIT & OFFSET failures
- Metadata template errors after restart
- Sequence creation errors after deleting all databases

Key updates:

- ​**​Query Module​**​:
  - ALIGN BY DEVICE now supports ORDER BY TIME
  - New SHOW QUERIES command
  - New KILL QUERY command
  - Aggregate queries support GROUP BY VARIATION/CONDITION
  - SELECT INTO supports type casting
  - New built-in functions: DIFF (scalar), COUNT\_IF (aggregate)
- ​**​System Module​**​:
  - SHOW REGIONS supports database specification
  - New SHOW VARIABLES for cluster parameters
  - SHOW REGIONS displays creation time
  - Supports modifying dn\_rpc\_port and dn\_rpc\_address

## [V1.0.0](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache--v1-0-0)

> Release Date: 2022.12.03

V1.0.0 stabilizes the distributed architecture while fixing:

- Partition calculation issues
- Undeleted historical snapshots
- Query execution and SessionPool memory problems

Major features:

- ​**​System Module​**​:
  - Distributed high-availability architecture
  - Multi-replica storage
  - Port conflict detection during node startup
  - Cluster management SQL
  - ConfigNode/DataNode lifecycle control (start/stop/remove)
  - Configurable consensus protocols: Simple, IoTConsensus, Ratis
  - Multi-replica support for data/metadata/ConfigNodes
- ​**​Query Module​**​: MPP framework for distributed read/write
- ​**​Stream Processing​**​:
  - Stream processing framework
  - Cross-cluster data synchronization

[Found Error? Edit this page on GitHub](https://github.com/apache/iotdb-docs/edit/main/src/UserGuide/latest/IoTDB-Introduction/Release-history_apache.md)

Last Updated: 4/15/26, 1:39 PM

[

Prev

Scenario

](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario)

---

<a id="iotdb-apache-org-userguide-latest-iotdb-introduction-scenario"></a>

# Scenario | IoTDB Website

# Scenario

7/10/23About 3 min

---

On This Page

- [1. Internet of Vehicles](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_1-internet-of-vehicles)
- - [1.1 Background](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_1-1-background)
  - [1.2 Architecture](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_1-2-architecture)
- [2. Intelligent Operation and Maintenance](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_2-intelligent-operation-and-maintenance)
- - [2.1 Background](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_2-1-background)
  - [2.2 Architecture](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_2-2-architecture)
- [3. Smart Factory](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_3-smart-factory)
- - [3.1 Background](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_3-1-background)
  - [3.2 Architecture](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_3-2-architecture)
- [4. Condition monitoring](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_4-condition-monitoring)
- - [4.1 Background](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_4-1-background)
  - [4.2 Architecture](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_4-2-architecture)

# [Scenario](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--scenario)

## [1. Internet of Vehicles](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_1-internet-of-vehicles)

### [1.1 Background](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_1-1-background)

> - Challenge: a large number of vehicles and time series

A car company has a huge business volume and needs to deal with a large number of vehicles and a large amount of data. It has hundreds of millions of data measurement points, over ten million new data points per second, millisecond-level collection frequency, posing high requirements on real-time writing, storage and processing of databases.

In the original architecture, the HBase cluster was used as the storage database. The query delay was high, and the system maintenance was difficult and costly. The HBase cluster cannot meet the demand. On the contrary, IoTDB supports high-frequency data writing with millions of measurement points and millisecond-level query response speed. The efficient data processing capability allows users to obtain the required data quickly and accurately. Therefore, IoTDB is chosen as the data storage layer, which has a lightweight architecture, reduces operation and maintenance costs, and supports elastic expansion and contraction and high availability to ensure system stability and availability.

### [1.2 Architecture](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_1-2-architecture)

The data management architecture of the car company using IoTDB as the time-series data storage engine is shown in the figure below.

![](/img/architecture1.png)

The vehicle data is encoded based on TCP and industrial protocols and sent to the edge gateway, and the gateway sends the data to the message queue Kafka cluster, decoupling the two ends of production and consumption. Kafka sends data to Flink for real-time processing, and the processed data is written into IoTDB. Both historical data and latest data are queried in IoTDB, and finally the data flows into the visualization platform through API for application.

## [2. Intelligent Operation and Maintenance](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_2-intelligent-operation-and-maintenance)

### [2.1 Background](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_2-1-background)

A steel factory aims to build a low-cost, large-scale access-capable remote intelligent operation and maintenance software and hardware platform, access hundreds of production lines, more than one million devices, and tens of millions of time series, to achieve remote coverage of intelligent operation and maintenance.

There are many challenges in this process:

> - Wide variety of devices, protocols, and data types
> - Time series data, especially high-frequency data, has a huge amount of data
> - The reading and writing speed of massive time series data cannot meet business needs
> - Existing time series data management components cannot meet various advanced application requirements

After selecting IoTDB as the storage database of the intelligent operation and maintenance platform, it can stably write multi-frequency and high-frequency acquisition data, covering the entire steel process, and use a composite compression algorithm to reduce the data size by more than 10 times, saving costs. IoTDB also effectively supports downsampling query of historical data of more than 10 years, helping enterprises to mine data trends and assist enterprises in long-term strategic analysis.

### [2.2 Architecture](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_2-2-architecture)

The figure below shows the architecture design of the intelligent operation and maintenance platform of the steel plant.

![](/img/architecture2.jpg)

## [3. Smart Factory](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_3-smart-factory)

### [3.1 Background](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_3-1-background)

> - Challenge：Cloud-edge collaboration

A cigarette factory hopes to upgrade from a "traditional factory" to a "high-end factory". It uses the Internet of Things and equipment monitoring technology to strengthen information management and services to realize the free flow of data within the enterprise and to help improve productivity and lower operating costs.

### [3.2 Architecture](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_3-2-architecture)

The figure below shows the factory's IoT system architecture. IoTDB runs through the three-level IoT platform of the company, factory, and workshop to realize unified joint debugging and joint control of equipment. The data at the workshop level is collected, processed and stored in real time through the IoTDB at the edge layer, and a series of analysis tasks are realized. The preprocessed data is sent to the IoTDB at the platform layer for data governance at the business level, such as device management, connection management, and service support. Eventually, the data will be integrated into the IoTDB at the group level for comprehensive analysis and decision-making across the organization.

![](/img/architecture3.jpg)

## [4. Condition monitoring](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_4-condition-monitoring)

### [4.1 Background](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_4-1-background)

> - Challenge: Smart heating, cost reduction and efficiency increase

A power plant needs to monitor tens of thousands of measuring points of main and auxiliary equipment such as fan boiler equipment, generators, and substation equipment. In the previous heating process, there was a lack of prediction of the heat supply in the next stage, resulting in ineffective heating, overheating, and insufficient heating.

After using IoTDB as the storage and analysis engine, combined with meteorological data, building control data, household control data, heat exchange station data, official website data, heat source side data, etc., all data are time-aligned in IoTDB to provide reliable data basis to realize smart heating. At the same time, it also solves the problem of monitoring the working conditions of various important components in the relevant heating process, such as on-demand billing and pipe network, heating station, etc., to reduce manpower input.

### [4.2 Architecture](#iotdb-apache-org-userguide-latest-iotdb-introduction-scenario--_4-2-architecture)

The figure below shows the data management architecture of the power plant in the heating scene.

![](/img/architecture4.jpg)

[Found Error? Edit this page on GitHub](https://github.com/apache/iotdb-docs/edit/main/src/UserGuide/latest/IoTDB-Introduction/Scenario.md)

Last Updated: 5/21/25, 9:15 AM

[

Prev

IoTDB Introduction

](#iotdb-apache-org-userguide-latest-iotdb-introduction-iotdb-introduction_apache)[

Next

Release History

](#iotdb-apache-org-userguide-latest-iotdb-introduction-release-history_apache)