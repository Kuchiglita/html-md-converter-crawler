<a id="knox-apache-org-books-knox-2-1-0-user-guide"></a>

# User Guide

![Knox](knox-logo.gif)

![Apache](apache-logo.gif)

# <a id="knox-apache-org-books-knox-2-1-0-user-guide--Apache+Knox+Gateway+2.1.x+User's+Guide"></a>Apache Knox Gateway 2.1.x User’s Guide [](#knox-apache-org-books-knox-2-1-0-user-guide--Apache+Knox+Gateway+2.1.x+User's+Guide)

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Table+Of+Contents"></a>Table Of Contents [](#knox-apache-org-books-knox-2-1-0-user-guide--Table+Of+Contents)

- [Introduction](#knox-apache-org-books-knox-2-1-0-user-guide--Introduction)
- [Quick Start](#knox-apache-org-books-knox-2-1-0-user-guide--Quick+Start)
- [Gateway Samples](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Samples)
- [Apache Knox Details](#knox-apache-org-books-knox-2-1-0-user-guide--Apache+Knox+Details)
  - [Apache Knox Directory Layout](#knox-apache-org-books-knox-2-1-0-user-guide--Apache+Knox+Directory+Layout)
  - [Supported Services](#knox-apache-org-books-knox-2-1-0-user-guide--Supported+Services)
- [Gateway Details](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Details)
  - [URL Mapping](#knox-apache-org-books-knox-2-1-0-user-guide--URL+Mapping)
    - [Default Topology URLs](#knox-apache-org-books-knox-2-1-0-user-guide--Default+Topology+URLs)
    - [Fully Qualified URLs](#knox-apache-org-books-knox-2-1-0-user-guide--Fully+Qualified+URLs)
    - [Topology Port Mapping](#knox-apache-org-books-knox-2-1-0-user-guide--Topology+Port+Mapping)
  - [Configuration](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)
    - [Gateway Server Configuration](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Server+Configuration)
    - [Simplified Topology Descriptors](#knox-apache-org-books-knox-2-1-0-user-guide--Simplified+Topology+Descriptors)
    - [Externalized Provider Configurations](#knox-apache-org-books-knox-2-1-0-user-guide--Externalized+Provider+Configurations)
    - [Sharing HA Providers](#knox-apache-org-books-knox-2-1-0-user-guide--Sharing+HA+Providers)
    - [Simplified Descriptor Files](#knox-apache-org-books-knox-2-1-0-user-guide--Simplified+Descriptor+Files)
  - [Cluster Configuration Monitoring](#knox-apache-org-books-knox-2-1-0-user-guide--Cluster+Configuration+Monitoring)
    - [Remote Configuration Monitor](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Monitor)
    - [Remote Configuration Registry Clients](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Registry+Clients)
    - [Topology Descriptors](#knox-apache-org-books-knox-2-1-0-user-guide--Topology+Descriptors)
    - [Hostmap Provider](#knox-apache-org-books-knox-2-1-0-user-guide--Hostmap+Provider)
  - [Remote Alias Service](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Alias+Service)
  - [Knox CLI](#knox-apache-org-books-knox-2-1-0-user-guide--Knox+CLI)
  - [Admin API](#knox-apache-org-books-knox-2-1-0-user-guide--Admin+API)
  - [X-Forwarded-\* Headers Support](#knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-*+Headers+Support)
  - [Metrics](#knox-apache-org-books-knox-2-1-0-user-guide--Metrics)
- [Authentication](#knox-apache-org-books-knox-2-1-0-user-guide--Authentication)
  - [Advanced LDAP Authentication](#knox-apache-org-books-knox-2-1-0-user-guide--Advanced+LDAP+Authentication)
  - [LDAP Authentication Caching](#knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Authentication+Caching)
  - [LDAP Group Lookup](#knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Group+Lookup)
  - [PAM based Authentication](#knox-apache-org-books-knox-2-1-0-user-guide--PAM+based+Authentication)
  - [HadoopAuth Authentication Provider](#knox-apache-org-books-knox-2-1-0-user-guide--HadoopAuth+Authentication+Provider)
  - [Preauthenticated SSO Provider](#knox-apache-org-books-knox-2-1-0-user-guide--Preauthenticated+SSO+Provider)
  - [SSO Cookie Provider](#knox-apache-org-books-knox-2-1-0-user-guide--SSO+Cookie+Provider)
  - [JWT Provider](#knox-apache-org-books-knox-2-1-0-user-guide--JWT+Provider)
  - [Pac4j Provider - CAS / OAuth / SAML / OpenID Connect](#knox-apache-org-books-knox-2-1-0-user-guide--Pac4j+Provider+-+CAS+/+OAuth+/+SAML+/+OpenID+Connect)
  - [KnoxSSO Setup and Configuration](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxSSO+Setup+and+Configuration)
  - [KnoxToken Configuration](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+Configuration)
  - [Mutual Authentication with SSL](#knox-apache-org-books-knox-2-1-0-user-guide--Mutual+Authentication+with+SSL)
  - [TLS Client Certificate Provider](#knox-apache-org-books-knox-2-1-0-user-guide--TLS+Client+Certificate+Provider)
  - [Knox Auth Service](#knox-apache-org-books-knox-2-1-0-user-guide--Knox+Auth+Service)
- [Authorization](#knox-apache-org-books-knox-2-1-0-user-guide--Authorization)
  - [Service Level Authorization](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Level+Authorization)
  - [Composite Authorization Provider](#knox-apache-org-books-knox-2-1-0-user-guide--Composite+Authorization+Provider)
  - [Path Based Authorization](#knox-apache-org-books-knox-2-1-0-user-guide--Path+Based+Authorization)
- [Identity Assertion](#knox-apache-org-books-knox-2-1-0-user-guide--Identity+Assertion)
  - [Default Identity Assertion Provider](#knox-apache-org-books-knox-2-1-0-user-guide--Default+Identity+Assertion+Provider)
  - [Concat Identity Assertion Provider](#knox-apache-org-books-knox-2-1-0-user-guide--Concat+Identity+Assertion+Provider)
  - [SwitchCase Identity Assertion Provider](#knox-apache-org-books-knox-2-1-0-user-guide--SwitchCase+Identity+Assertion+Provider)
  - [Regular Expression Identity Assertion Provider](#knox-apache-org-books-knox-2-1-0-user-guide--Regular+Expression+Identity+Assertion+Provider)
  - [Hadoop Group Lookup Provider](#knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+Group+Lookup+Provider)
- [Secure Clusters](#knox-apache-org-books-knox-2-1-0-user-guide--Secure+Clusters)
- [High Availability](#knox-apache-org-books-knox-2-1-0-user-guide--High+Availability)
- [Web App Security Provider](#knox-apache-org-books-knox-2-1-0-user-guide--Web+App+Security+Provider)
  - [CSRF](#knox-apache-org-books-knox-2-1-0-user-guide--CSRF)
  - [CORS](#knox-apache-org-books-knox-2-1-0-user-guide--CORS)
  - [X-Frame-Options](#knox-apache-org-books-knox-2-1-0-user-guide--X-Frame-Options)
  - [X-XSS-Protection](#knox-apache-org-books-knox-2-1-0-user-guide--X-XSS-Protection)
  - [X-Content-Type-Options](#knox-apache-org-books-knox-2-1-0-user-guide--X-Content-Type-Options)
  - [HTTP Strict-Transport-Security - HSTS](#knox-apache-org-books-knox-2-1-0-user-guide--HTTP+Strict-Transport-Security+-+HSTS)
  - [Rate limiting](#knox-apache-org-books-knox-2-1-0-user-guide--Rate+limiting)
- [Websocket Support](#knox-apache-org-books-knox-2-1-0-user-guide--Websocket+Support)
- [Server-Sent Events Support](#knox-apache-org-books-knox-2-1-0-user-guide--Server-Sent+Events+Support)
- [Audit](#knox-apache-org-books-knox-2-1-0-user-guide--Audit)
- [Client Details](#knox-apache-org-books-knox-2-1-0-user-guide--Client+Details)
  - [Client Quickstart](#knox-apache-org-books-knox-2-1-0-user-guide--Client+Quickstart)
  - [Client Token Sessions](#knox-apache-org-books-knox-2-1-0-user-guide--Client+Token+Sessions)
    - [Server Setup](#knox-apache-org-books-knox-2-1-0-user-guide--Server+Setup)
  - [Client DSL and SDK Details](#knox-apache-org-books-knox-2-1-0-user-guide--Client+DSL+and+SDK+Details)
- [Service Details](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Details)
  - [WebHDFS](#knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS)
  - [WebHCat](#knox-apache-org-books-knox-2-1-0-user-guide--WebHCat)
  - [Oozie](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie)
  - [HBase](#knox-apache-org-books-knox-2-1-0-user-guide--HBase)
  - [Hive](#knox-apache-org-books-knox-2-1-0-user-guide--Hive)
  - [Yarn](#knox-apache-org-books-knox-2-1-0-user-guide--Yarn)
  - [Kafka](#knox-apache-org-books-knox-2-1-0-user-guide--Kafka)
  - [Storm](#knox-apache-org-books-knox-2-1-0-user-guide--Storm)
  - [Solr](#knox-apache-org-books-knox-2-1-0-user-guide--Solr)
  - [Avatica](#knox-apache-org-books-knox-2-1-0-user-guide--Avatica)
  - [Cloudera Manager](#knox-apache-org-books-knox-2-1-0-user-guide--Cloudera+Manager)
  - [Livy Server](#knox-apache-org-books-knox-2-1-0-user-guide--Livy+Server)
  - [Elasticsearch](#knox-apache-org-books-knox-2-1-0-user-guide--Elasticsearch)
  - [Common Service Config](#knox-apache-org-books-knox-2-1-0-user-guide--Common+Service+Config)
  - [Default Service HA support](#knox-apache-org-books-knox-2-1-0-user-guide--Default+Service+HA+support)
  - [TLS/SSL Certificate Trust](#knox-apache-org-books-knox-2-1-0-user-guide--TLS/SSL+Certificate+Trust)
- [UI Service Details](#knox-apache-org-books-knox-2-1-0-user-guide--UI+Service+Details)
- [Admin UI](#knox-apache-org-books-knox-2-1-0-user-guide--Admin+UI)
- [Webshell](#knox-apache-org-books-knox-2-1-0-user-guide--Webshell)
- [Limitations](#knox-apache-org-books-knox-2-1-0-user-guide--Limitations)
- [Troubleshooting](#knox-apache-org-books-knox-2-1-0-user-guide--Troubleshooting)
- [Export Controls](#knox-apache-org-books-knox-2-1-0-user-guide--Export+Controls)

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Introduction"></a>Introduction [](#knox-apache-org-books-knox-2-1-0-user-guide--Introduction)

The Apache Knox Gateway is a system that provides a single point of authentication and access for Apache Hadoop services in a cluster. The goal is to simplify Hadoop security for both users (i.e. who access the cluster data and execute jobs) and operators (i.e. who control access and manage the cluster). The gateway runs as a server (or cluster of servers) that provide centralized access to one or more Hadoop clusters. In general the goals of the gateway are as follows:

- Provide perimeter security for Hadoop REST APIs to make Hadoop security easier to setup and use
  - Provide authentication and token verification at the perimeter
  - Enable authentication integration with enterprise and cloud identity management systems
  - Provide service level authorization at the perimeter
- Expose a single URL hierarchy that aggregates REST APIs of a Hadoop cluster
  - Limit the network endpoints (and therefore firewall holes) required to access a Hadoop cluster
  - Hide the internal Hadoop cluster topology from potential attackers

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Quick+Start"></a>Quick Start [](#knox-apache-org-books-knox-2-1-0-user-guide--Quick+Start)

Here are the steps to have Apache Knox up and running against a Hadoop Cluster:

1. Verify system requirements
2. Download a virtual machine (VM) with Hadoop
3. Download Apache Knox Gateway
4. Start the virtual machine with Hadoop
5. Install Knox
6. Start the LDAP embedded within Knox
7. Start the Knox Gateway
8. Do Hadoop with Knox

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--1+-+Requirements"></a>1 - Requirements [](#knox-apache-org-books-knox-2-1-0-user-guide--1+-+Requirements)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Java"></a>Java [](#knox-apache-org-books-knox-2-1-0-user-guide--Java)

Java 1.8 is required for the Knox Gateway runtime. Use the command below to check the version of Java installed on the system where Knox will be running.

```text
java -version
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hadoop"></a>Hadoop [](#knox-apache-org-books-knox-2-1-0-user-guide--Hadoop)

Knox 2.0.0 supports Hadoop 2.x and 3.x, the quick start instructions assume a Hadoop 2.x virtual machine based environment.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--2+-+Download+Hadoop+2.x+VM"></a>2 - Download Hadoop 2.x VM [](#knox-apache-org-books-knox-2-1-0-user-guide--2+-+Download+Hadoop+2.x+VM)

The quick start provides a link to download Hadoop 2.0 based Hortonworks virtual machine [Sandbox](http://hortonworks.com/products/hdp-2/#install). Please note Knox supports other Hadoop distributions and is configurable against a full-blown Hadoop cluster. Configuring Knox for Hadoop 2.x version, or Hadoop deployed in EC2 or a custom Hadoop cluster is documented in advance deployment guide.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--3+-+Download+Apache+Knox+Gateway"></a>3 - Download Apache Knox Gateway [](#knox-apache-org-books-knox-2-1-0-user-guide--3+-+Download+Apache+Knox+Gateway)

Download one of the distributions below from the [Apache mirrors](http://www.apache.org/dyn/closer.cgi/knox).

- Source archive: [knox-2.0.0-src.zip](http://www.apache.org/dyn/closer.cgi/knox/2.0.0/knox-2.0.0-src.zip) ([PGP signature](https://downloads.apache.org//knox/2.0.0/knox-2.0.0-src.zip.asc), [SHA1 digest](https://downloads.apache.org//knox/2.0.0/knox-2.0.0-src.zip.sha1), [MD5 digest](https://downloads.apache.org//knox/2.0.0/knox-2.0.0-src.zip.md5))
- Binary archive: [knox-2.0.0.zip](http://www.apache.org/dyn/closer.cgi/knox/2.0.0/knox-2.0.0.zip) ([PGP signature](https://downloads.apache.org//knox/2.0.0/knox-2.0.0.zip.asc), [SHA1 digest](https://downloads.apache.org//knox/2.0.0/knox-2.0.0.zip.sha1), [MD5 digest](https://downloads.apache.org//knox/2.0.0/knox-2.0.0.zip.md5))

Apache Knox Gateway releases are available under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0). See the NOTICE file contained in each release artifact for applicable copyright attribution notices.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Verify"></a>Verify [](#knox-apache-org-books-knox-2-1-0-user-guide--Verify)

While recommended, verification of signatures is an optional step. You can verify the integrity of any downloaded files using the PGP signatures. Please read [Verifying Apache HTTP Server Releases](http://httpd.apache.org/dev/verification.html) for more information on why you should verify our releases.

The PGP signatures can be verified using PGP or GPG. First download the [KEYS](https://dist.apache.org/repos/dist/release/knox/KEYS) file as well as the `.asc` signature files for the relevant release packages. Make sure you get these files from the main distribution directory linked above, rather than from a mirror. Then verify the signatures using one of the methods below.

```text
% pgpk -a KEYS
% pgpv knox-2.0.0.zip.asc
```

or

```text
% pgp -ka KEYS
% pgp knox-2.0.0.zip.asc
```

or

```text
% gpg --import KEYS
% gpg --verify knox-2.0.0.zip.asc
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--4+-+Start+Hadoop+virtual+machine"></a>4 - Start Hadoop virtual machine [](#knox-apache-org-books-knox-2-1-0-user-guide--4+-+Start+Hadoop+virtual+machine)

Start the Hadoop virtual machine.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--5+-+Install+Knox"></a>5 - Install Knox [](#knox-apache-org-books-knox-2-1-0-user-guide--5+-+Install+Knox)

The steps required to install the gateway will vary depending upon which distribution format (zip | rpm) was downloaded. In either case you will end up with a directory where the gateway is installed. This directory will be referred to as your `{GATEWAY_HOME}` throughout this document.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--ZIP"></a>ZIP [](#knox-apache-org-books-knox-2-1-0-user-guide--ZIP)

If you downloaded the Zip distribution you can simply extract the contents into a directory. The example below provides a command that can be executed to do this. Note the `{VERSION}` portion of the command must be replaced with an actual Apache Knox Gateway version number. This might be 2.0.0 for example.

```text
unzip knox-{VERSION}.zip
```

This will create a directory `knox-{VERSION}` in your current directory. The directory `knox-{VERSION}` will considered your `{GATEWAY_HOME}`

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--6+-+Start+LDAP+embedded+in+Knox"></a>6 - Start LDAP embedded in Knox [](#knox-apache-org-books-knox-2-1-0-user-guide--6+-+Start+LDAP+embedded+in+Knox)

Knox comes with an LDAP server for demonstration purposes. Note: If the tool used to extract the contents of the Tar or tar.gz file was not capable of making the files in the bin directory executable

```bash
cd {GATEWAY_HOME}
bin/ldap.sh start
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--7+-+Create+the+Master+Secret"></a>7 - Create the Master Secret [](#knox-apache-org-books-knox-2-1-0-user-guide--7+-+Create+the+Master+Secret)

Run the `knoxcli.sh create-master` command in order to persist the master secret that is used to protect the key and credential stores for the gateway instance.

```bash
cd {GATEWAY_HOME}
bin/knoxcli.sh create-master
```

The CLI will prompt you for the master secret (i.e. password).

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--7+-+Start+Knox"></a>7 - Start Knox [](#knox-apache-org-books-knox-2-1-0-user-guide--7+-+Start+Knox)

The gateway can be started using the provided shell script.

The server will discover the persisted master secret during start up and complete the setup process for demo installs. A demo install will consist of a Knox gateway instance with an identity certificate for localhost. This will require clients to be on the same machine or to turn off hostname verification. For more involved deployments, See the Knox CLI section of this document for additional configuration options, including the ability to create a self-signed certificate for a specific hostname.

```bash
cd {GATEWAY_HOME}
bin/gateway.sh start
```

When starting the gateway this way the process will be run in the background. The log files will be written to `{GATEWAY_HOME}/logs` and the process ID files (PIDs) will be written to `{GATEWAY_HOME}/pids`.

In order to stop a gateway that was started with the script use this command:

```bash
cd {GATEWAY_HOME}
bin/gateway.sh stop
```

If for some reason the gateway is stopped other than by using the command above you may need to clear the tracking PID:

```bash
cd {GATEWAY_HOME}
bin/gateway.sh clean
```

**NOTE: This command will also clear any `.out` and `.err` file from the `{GATEWAY_HOME}/logs` directory so use this with caution.**

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--8+-+Access+Hadoop+with+Knox"></a>8 - Access Hadoop with Knox [](#knox-apache-org-books-knox-2-1-0-user-guide--8+-+Access+Hadoop+with+Knox)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Invoke+the+LISTSTATUS+operation+on+WebHDFS+via+the+gateway."></a>Invoke the LISTSTATUS operation on WebHDFS via the gateway. [](#knox-apache-org-books-knox-2-1-0-user-guide--Invoke+the+LISTSTATUS+operation+on+WebHDFS+via+the+gateway.)

This will return a directory listing of the root (i.e. `/`) directory of HDFS.

```bash
curl -i -k -u guest:guest-password -X GET \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/?op=LISTSTATUS'
```

The results of the above command should result in something to along the lines of the output below. The exact information returned is subject to the content within HDFS in your Hadoop cluster. Successfully executing this command at a minimum proves that the gateway is properly configured to provide access to WebHDFS. It does not necessarily mean that any of the other services are correctly configured to be accessible. To validate that see the sections for the individual services in [Service Details](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Details).

```text
HTTP/1.1 200 OK
Content-Type: application/json
Content-Length: 760
Server: Jetty(6.1.26)

{"FileStatuses":{"FileStatus":[
{"accessTime":0,"blockSize":0,"group":"hdfs","length":0,"modificationTime":1350595859762,"owner":"hdfs","pathSuffix":"apps","permission":"755","replication":0,"type":"DIRECTORY"},
{"accessTime":0,"blockSize":0,"group":"mapred","length":0,"modificationTime":1350595874024,"owner":"mapred","pathSuffix":"mapred","permission":"755","replication":0,"type":"DIRECTORY"},
{"accessTime":0,"blockSize":0,"group":"hdfs","length":0,"modificationTime":1350596040075,"owner":"hdfs","pathSuffix":"tmp","permission":"777","replication":0,"type":"DIRECTORY"},
{"accessTime":0,"blockSize":0,"group":"hdfs","length":0,"modificationTime":1350595857178,"owner":"hdfs","pathSuffix":"user","permission":"755","replication":0,"type":"DIRECTORY"}
]}}
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Put+a+file+in+HDFS+via+Knox."></a>Put a file in HDFS via Knox. [](#knox-apache-org-books-knox-2-1-0-user-guide--Put+a+file+in+HDFS+via+Knox.)

```bash
curl -i -k -u guest:guest-password -X PUT \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp/LICENSE?op=CREATE'

curl -i -k -u guest:guest-password -T LICENSE -X PUT \
    '{Value of Location header from response above}'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Get+a+file+in+HDFS+via+Knox."></a>Get a file in HDFS via Knox. [](#knox-apache-org-books-knox-2-1-0-user-guide--Get+a+file+in+HDFS+via+Knox.)

```bash
curl -i -k -u guest:guest-password -X GET \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp/LICENSE?op=OPEN'

curl -i -k -u guest:guest-password -X GET \
    '{Value of Location header from command response above}'
```

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Apache+Knox+Details"></a>Apache Knox Details [](#knox-apache-org-books-knox-2-1-0-user-guide--Apache+Knox+Details)

This section provides everything you need to know to get the Knox gateway up and running against a Hadoop cluster.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hadoop"></a>Hadoop [](#knox-apache-org-books-knox-2-1-0-user-guide--Hadoop)

An existing Hadoop 2.x or 3.x cluster is required for Knox to sit in front of and protect. It is possible to use a Hadoop cluster deployed on EC2 but this will require additional configuration not covered here. It is also possible to protect access to a services of a Hadoop cluster that is secured with Kerberos. This too requires additional configuration that is described in other sections of this guide. See [Supported Services](#knox-apache-org-books-knox-2-1-0-user-guide--Supported+Services) for details on what is supported for this release.

The instructions that follow assume a few things:

1. The gateway is *not* collocated with the Hadoop clusters themselves.
2. The host names and IP addresses of the cluster services are accessible by the gateway where ever it happens to be running.

All of the instructions and samples provided here are tailored and tested to work “out of the box” against a [Hortonworks Sandbox 2.x VM](https://hortonworks.com/products/sandbox/).

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Apache+Knox+Directory+Layout"></a>Apache Knox Directory Layout [](#knox-apache-org-books-knox-2-1-0-user-guide--Apache+Knox+Directory+Layout)

Knox can be installed by expanding the zip/archive file.

The table below provides a brief explanation of the important files and directories within `{GATEWAY_HOME}`

| Directory | Purpose |
| --- | --- |
| conf/ | Contains configuration files that apply to the gateway globally (i.e. not cluster specific ). |
| data/ | Contains security and topology specific artifacts that require read/write access at runtime |
| conf/topologies/ | Contains topology files that represent Hadoop clusters which the gateway uses to deploy cluster proxies |
| data/security/ | Contains the persisted master secret and keystore dir |
| data/security/keystores/ | Contains the gateway identity keystore and credential stores for the gateway and each deployed cluster topology |
| data/services | Contains service behavior definitions for the services currently supported. |
| bin/ | Contains the executable shell scripts, batch files and JARs for clients and servers. |
| data/deployments/ | Contains deployed cluster topologies used to protect access to specific Hadoop clusters. |
| lib/ | Contains the JARs for all the components that make up the gateway. |
| dep/ | Contains the JARs for all of the components upon which the gateway depends. |
| ext/ | A directory where user supplied extension JARs can be placed to extends the gateways functionality. |
| pids/ | Contains the process ids for running LDAP and gateway servers |
| samples/ | Contains a number of samples that can be used to explore the functionality of the gateway. |
| templates/ | Contains default configuration files that can be copied and customized. |
| README | Provides basic information about the Apache Knox Gateway. |
| ISSUES | Describes significant know issues. |
| CHANGES | Enumerates the changes between releases. |
| LICENSE | Documents the license under which this software is provided. |
| NOTICE | Documents required attribution notices for included dependencies. |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Supported+Services"></a>Supported Services [](#knox-apache-org-books-knox-2-1-0-user-guide--Supported+Services)

This table enumerates the versions of various Hadoop services that have been tested to work with the Knox Gateway.

| Service | Version | Non-Secure | Secure | HA |
| --- | --- | --- | --- | --- |
| WebHDFS | 2.4.0 |  |  |  |
| WebHCat/Templeton | 0.13.0 |  |  |  |
| Oozie | 4.0.0 |  |  |  |
| HBase | 0.98.0 |  |  |  |
| Hive (via WebHCat) | 0.13.0 |  |  |  |
| Hive (via JDBC/ODBC) | 0.13.0 |  |  |  |
| Yarn ResourceManager | 2.5.0 |  |  |  |
| Kafka (via REST Proxy) | 0.10.0 |  |  |  |
| Storm | 0.9.3 |  |  |  |
| Solr | 5.5+ and 6+ |  |  |  |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--More+Examples"></a>More Examples [](#knox-apache-org-books-knox-2-1-0-user-guide--More+Examples)

These examples provide more detail about how to access various Apache Hadoop services via the Apache Knox Gateway.

- [WebHDFS Examples](#knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+Examples)
- [WebHCat Examples](#knox-apache-org-books-knox-2-1-0-user-guide--WebHCat+Examples)
- [Oozie Examples](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie+Examples)
- [HBase Examples](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+Examples)
- [Hive Examples](#knox-apache-org-books-knox-2-1-0-user-guide--Hive+Examples)
- [Yarn Examples](#knox-apache-org-books-knox-2-1-0-user-guide--Yarn+Examples)
- [Storm Examples](#knox-apache-org-books-knox-2-1-0-user-guide--Storm+Examples)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Samples"></a>Gateway Samples [](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Samples)

The purpose of the samples within the `{GATEWAY_HOME}/samples` directory is to demonstrate the capabilities of the Apache Knox Gateway to provide access to the numerous APIs that are available from the service components of a Hadoop cluster.

Depending on exactly how your Knox installation was done, there will be some number of steps required in order fully install and configure the samples for use.

This section will help describe the assumptions of the samples and the steps to get them to work in a couple of different deployment scenarios.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Assumptions+of+the+Samples"></a>Assumptions of the Samples [](#knox-apache-org-books-knox-2-1-0-user-guide--Assumptions+of+the+Samples)

The samples were initially written with the intent of working out of the box for the various Hadoop demo environments that are deployed as a single node cluster inside of a VM. The following assumptions were made from that context and should be understood in order to get the samples to work in other deployment scenarios:

- That there is a valid java JDK on the PATH for executing the samples
- The Knox Demo LDAP server is running on localhost and port 33389 which is the default port for the ApacheDS LDAP server.
- That the LDAP directory in use has a set of demo users provisioned with the convention of username and username“-password” as the password. Most of the samples have some variation of this pattern with “guest” and “guest-password”.
- That the Knox Gateway instance is running on the same machine which you will be running the samples from - therefore “localhost” and that the default port of “8443” is being used.
- Finally, that there is a properly provisioned sandbox.xml topology in the `{GATEWAY_HOME}/conf/topologies` directory that is configured to point to the actual host and ports of running service components.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Steps+for+Demo+Single+Node+Clusters"></a>Steps for Demo Single Node Clusters [](#knox-apache-org-books-knox-2-1-0-user-guide--Steps+for+Demo+Single+Node+Clusters)

There should be little to do if anything in a demo environment that has been provisioned with illustrating the use of Apache Knox.

However, the following items will be worth ensuring before you start:

1. The `sandbox.xml` topology is configured properly for the deployed services
2. That there is a LDAP server running with guest/guest-password user available in the directory

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Steps+for+Ambari+deployed+Knox+Gateway"></a>Steps for Ambari deployed Knox Gateway [](#knox-apache-org-books-knox-2-1-0-user-guide--Steps+for+Ambari+deployed+Knox+Gateway)

Apache Knox instances that are under the management of Ambari are generally assumed not to be demo instances. These instances are in place to facilitate development, testing or production Hadoop clusters.

The Knox samples can however be made to work with Ambari managed Knox instances with a few steps:

1. You need to have SSH access to the environment in order for the localhost assumption within the samples to be valid
2. The Knox Demo LDAP Server is started - you can start it from Ambari
3. The `default.xml` topology file can be copied to `sandbox.xml` in order to satisfy the topology name assumption in the samples
4. Be sure to use an actual Java JRE to run the sample with something like:

   /usr/jdk64/jdk1.7.0\_67/bin/java -jar bin/shell.jar samples/ExampleWebHdfsLs.groovy

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Steps+for+a+manually+installed+Knox+Gateway"></a>Steps for a manually installed Knox Gateway [](#knox-apache-org-books-knox-2-1-0-user-guide--Steps+for+a+manually+installed+Knox+Gateway)

For manually installed Knox instances, there is really no way for the installer to know how to configure the topology file for you.

Essentially, these steps are identical to the Ambari deployed instance except that #3 should be replaced with the configuration of the out of the box `sandbox.xml` to point the configuration at the proper hosts and ports.

1. You need to have SSH access to the environment in order for the localhost assumption within the samples to be valid.
2. The Knox Demo LDAP Server is started - you can start it from Ambari
3. Change the hosts and ports within the `{GATEWAY_HOME}/conf/topologies/sandbox.xml` to reflect your actual cluster service locations.
4. Be sure to use an actual Java JRE to run the sample with something like:

   /usr/jdk64/jdk1.7.0\_67/bin/java -jar bin/shell.jar samples/ExampleWebHdfsLs.groovy

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Details"></a>Gateway Details [](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Details)

This section describes the details of the Knox Gateway itself. Including:

- How URLs are mapped between a gateway that services multiple Hadoop clusters and the clusters themselves
- How the gateway is configured through `gateway-site.xml` and cluster specific topology files
- How to configure the various policy enforcement provider features such as authentication, authorization, auditing, hostmapping, etc.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--URL+Mapping"></a>URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--URL+Mapping)

The gateway functions much like a reverse proxy. As such, it maintains a mapping of URLs that are exposed externally by the gateway to URLs that are provided by the Hadoop cluster.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Default+Topology+URLs"></a>Default Topology URLs [](#knox-apache-org-books-knox-2-1-0-user-guide--Default+Topology+URLs)

In order to provide compatibility with the Hadoop Java client and existing CLI tools, the Knox Gateway has provided a feature called the *Default Topology*. This refers to a topology deployment that will be able to route URLs without the additional context that the gateway uses for differentiating from one Hadoop cluster to another. This allows the URLs to match those used by existing clients that may access WebHDFS through the Hadoop file system abstraction.

When a topology file is deployed with a file name that matches the configured default topology name, a specialized mapping for URLs is installed for that particular topology. This allows the URLs that are expected by the existing Hadoop CLIs for WebHDFS to be used in interacting with the specific Hadoop cluster that is represented by the default topology file.

The configuration for the default topology name is found in `gateway-site.xml` as a property called: `default.app.topology.name`.

The default value for this property is empty.

When deploying the `sandbox.xml` topology and setting `default.app.topology.name` to `sandbox`, both of the following example URLs work for the same underlying Hadoop cluster:

```text
https://{gateway-host}:{gateway-port}/webhdfs
https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/webhdfs
```

These default topology URLs exist for all of the services in the topology.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Fully+Qualified+URLs"></a>Fully Qualified URLs [](#knox-apache-org-books-knox-2-1-0-user-guide--Fully+Qualified+URLs)

Examples of mappings for WebHDFS, WebHCat, Oozie and HBase are shown below. These mapping are generated from the combination of the gateway configuration file (i.e. `{GATEWAY_HOME}/conf/gateway-site.xml`) and the cluster topology descriptors (e.g. `{GATEWAY_HOME}/conf/topologies/{cluster-name}.xml`). The port numbers shown for the Cluster URLs represent the default ports for these services. The actual port number may be different for a given cluster.

- WebHDFS
  - Gateway: `https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/webhdfs`
  - Cluster: `http://{webhdfs-host}:50070/webhdfs`
- WebHCat (Templeton)
  - Gateway: `https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/templeton`
  - Cluster: `http://{webhcat-host}:50111/templeton}`
- Oozie
  - Gateway: `https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/oozie`
  - Cluster: `http://{oozie-host}:11000/oozie}`
- HBase
  - Gateway: `https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/hbase`
  - Cluster: `http://{hbase-host}:8080`
- Hive JDBC
  - Gateway: `jdbc:hive2://{gateway-host}:{gateway-port}/;ssl=true;sslTrustStore={gateway-trust-store-path};trustStorePassword={gateway-trust-store-password};transportMode=http;httpPath={gateway-path}/{cluster-name}/hive`
  - Cluster: `http://{hive-host}:10001/cliservice`

The values for `{gateway-host}`, `{gateway-port}`, `{gateway-path}` are provided via the gateway configuration file (i.e. `{GATEWAY_HOME}/conf/gateway-site.xml`).

The value for `{cluster-name}` is derived from the file name of the cluster topology descriptor (e.g. `{GATEWAY_HOME}/deployments/{cluster-name}.xml`).

The value for `{webhdfs-host}`, `{webhcat-host}`, `{oozie-host}`, `{hbase-host}` and `{hive-host}` are provided via the cluster topology descriptor (e.g. `{GATEWAY_HOME}/conf/topologies/{cluster-name}.xml`).

Note: The ports 50070 (9870 for Hadoop 3.x), 50111, 11000, 8080 and 10001 are the defaults for WebHDFS, WebHCat, Oozie, HBase and Hive respectively. Their values can also be provided via the cluster topology descriptor if your Hadoop cluster uses different ports.

Note: The HBase REST API uses port 8080 by default. This often clashes with other running services. In the Hortonworks Sandbox, Apache Ambari might be running on this port, so you might have to change it to a different port (e.g. 60080).

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Topology+Port+Mapping"></a>Topology Port Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Topology+Port+Mapping)

This feature allows mapping of a topology to a port, as a result one can have a specific topology listening exclusively on a configured port. This feature routes URLs to these port-mapped topologies without the additional context that the gateway uses for differentiating from one Hadoop cluster to another, just like the [Default Topology URLs](#knox-apache-org-books-knox-2-1-0-user-guide--Default+Topology+URLs) feature, but on a dedicated port.

NOTE: Once the topologies are configured to listen on a dedicated port they will not be available on the default gateway port.

The configuration for Topology Port Mapping goes in `gateway-site.xml` file. The configuration uses the property name and value model. The format for the property name is `gateway.port.mapping.{topologyName}` and value is the port number that this topology will listen on.

In the following example, the topology `development` will listen on 9443 (if the port is not already taken).

```xml
  <property>
      <name>gateway.port.mapping.development</name>
      <value>9443</value>
      <description>Topology and Port mapping</description>
  </property>
```

An example of how one can access WebHDFS URL using the above configuration is

```text
 https://{gateway-host}:9443/webhdfs
 https://{gateway-host}:9443/{gateway-path}/development/webhdfs
```

All of the above URL will be valid URLs for the above described configuration.

This feature is turned on by default. Use the property `gateway.port.mapping.enabled` to turn it on/off. e.g.

```xml
 <property>
     <name>gateway.port.mapping.enabled</name>
     <value>true</value>
     <description>Enable/Disable port mapping feature.</description>
 </property>
```

If a topology mapped port is in use by another topology or a process, an ERROR message is logged and gateway startup continues as normal. Default gateway port cannot be used for port mapping, use [Default Topology URLs](#knox-apache-org-books-knox-2-1-0-user-guide--Default+Topology+URLs) feature instead.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

Configuration for Apache Knox includes:

1. [Related Cluster Configuration](#knox-apache-org-books-knox-2-1-0-user-guide--Related+Cluster+Configuration) that must be done within the Hadoop cluster to allow Knox to communicate with various services
2. [Gateway Server Configuration](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Server+Configuration) - which is the configurable elements of the server itself which applies to behavior that spans all topologies or managed Hadoop clusters
3. [Topology Descriptors](#knox-apache-org-books-knox-2-1-0-user-guide--Topology+Descriptors) which are the descriptors for controlling access to Hadoop clusters in various ways

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Related+Cluster+Configuration"></a>Related Cluster Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Related+Cluster+Configuration)

The following configuration changes must be made to your cluster to allow Apache Knox to dispatch requests to the various service components on behalf of end users.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Grant+Proxy+privileges+for+Knox+user+in+`core-site.xml`+on+Hadoop+master+nodes"></a>Grant Proxy privileges for Knox user in `core-site.xml` on Hadoop master nodes [](#knox-apache-org-books-knox-2-1-0-user-guide--Grant+Proxy+privileges+for+Knox+user+in+`core-site.xml`+on+Hadoop+master+nodes)

Update `core-site.xml` and add the following lines towards the end of the file.

Replace `FQDN_OF_KNOX_HOST` with the fully qualified domain name of the host running the Knox gateway. You can usually find this by running `hostname -f` on that host.

You can use `*` for local developer testing if the Knox host does not have a static IP.

```xml
<property>
    <name>hadoop.proxyuser.knox.groups</name>
    <value>users</value>
</property>
<property>
    <name>hadoop.proxyuser.knox.hosts</name>
    <value>FQDN_OF_KNOX_HOST</value>
</property>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Grant+proxy+privilege+for+Knox+in+`webhcat-site.xml`+on+Hadoop+master+nodes"></a>Grant proxy privilege for Knox in `webhcat-site.xml` on Hadoop master nodes [](#knox-apache-org-books-knox-2-1-0-user-guide--Grant+proxy+privilege+for+Knox+in+`webhcat-site.xml`+on+Hadoop+master+nodes)

Update `webhcat-site.xml` and add the following lines towards the end of the file.

Replace `FQDN_OF_KNOX_HOST` with the fully qualified domain name of the host running the Knox gateway. You can use `*` for local developer testing if the Knox host does not have a static IP.

```xml
<property>
    <name>webhcat.proxyuser.knox.groups</name>
    <value>users</value>
</property>
<property>
    <name>webhcat.proxyuser.knox.hosts</name>
    <value>FQDN_OF_KNOX_HOST</value>
</property>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Grant+proxy+privilege+for+Knox+in+`oozie-site.xml`+on+Oozie+host"></a>Grant proxy privilege for Knox in `oozie-site.xml` on Oozie host [](#knox-apache-org-books-knox-2-1-0-user-guide--Grant+proxy+privilege+for+Knox+in+`oozie-site.xml`+on+Oozie+host)

Update `oozie-site.xml` and add the following lines towards the end of the file.

Replace `FQDN_OF_KNOX_HOST` with the fully qualified domain name of the host running the Knox gateway. You can use `*` for local developer testing if the Knox host does not have a static IP.

```xml
<property>
    <name>oozie.service.ProxyUserService.proxyuser.knox.groups</name>
    <value>users</value>
</property>
<property>
    <name>oozie.service.ProxyUserService.proxyuser.knox.hosts</name>
    <value>FQDN_OF_KNOX_HOST</value>
</property>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Enable+http+transport+mode+and+use+substitution+in+HiveServer2"></a>Enable http transport mode and use substitution in HiveServer2 [](#knox-apache-org-books-knox-2-1-0-user-guide--Enable+http+transport+mode+and+use+substitution+in+HiveServer2)

Update `hive-site.xml` and set the following properties on HiveServer2 hosts. Some of the properties may already be in the hive-site.xml. Ensure that the values match the ones below.

```xml
<property>
    <name>hive.server2.allow.user.substitution</name>
    <value>true</value>
</property>

<property>
    <name>hive.server2.transport.mode</name>
    <value>http</value>
    <description>Server transport mode. "binary" or "http".</description>
</property>

<property>
    <name>hive.server2.thrift.http.port</name>
    <value>10001</value>
    <description>Port number when in HTTP mode.</description>
</property>

<property>
    <name>hive.server2.thrift.http.path</name>
    <value>cliservice</value>
    <description>Path component of URL endpoint when in HTTP mode.</description>
</property>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Server+Configuration"></a>Gateway Server Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Server+Configuration)

The following table illustrates the configurable elements of the Apache Knox Gateway at the server level via gateway-site.xml.

| Property | Description | Default |
| --- | --- | --- |
| gateway.deployment.dir | The directory withinGATEWAY_HOMEthat contains gateway topology deployments | {GATEWAY_HOME}/data/deployments |
| gateway.security.dir | The directory withinGATEWAY_HOMEthat contains the required security artifacts | {GATEWAY_HOME}/data/security |
| gateway.data.dir | The directory withinGATEWAY_HOMEthat contains the gateway instance data | {GATEWAY_HOME}/data |
| gateway.services.dir | The directory withinGATEWAY_HOMEthat contains the gateway services definitions | {GATEWAY_HOME}/services |
| gateway.hadoop.conf.dir | The directory withinGATEWAY_HOMEthat contains the gateway configuration | {GATEWAY_HOME}/conf |
| gateway.frontend.url | The URL that should be used during rewriting so that it can rewrite the URLs with the correct “frontend” URL | none |
| gateway.server.header.enabled | Indicates whether Knox displays service info in HTTP response | false |
| gateway.xforwarded.enabled | Indicates whether support for some X-Forwarded-* headers is enabled | true |
| gateway.trust.all.certs | Indicates whether all presented client certs should establish trust | false |
| gateway.client.auth.needed | Indicates whether clients are required to establish a trust relationship with client certificates | false |
| gateway.truststore.password.alias | OPTIONAL Alias for the password to the truststore file holding the trusted client certificates. NOTE: An alias with the provided name should be created usingknoxcli.sh create-aliasinorder to provide the password; else the master secret will be used. | gateway-truststore-password |
| gateway.truststore.path | Location of the truststore for client certificates to be trusted | null |
| gateway.truststore.type | Indicates the type of truststore at the path declared ingateway.truststore.path | JKS |
| gateway.jdk.tls.ephemeralDHKeySize | jdk.tls.ephemeralDHKeySize, is defined to customize the ephemeral DH key sizes. The minimum acceptable DH key size is 1024 bits, except for exportable cipher suites or legacy mode (jdk.tls.ephemeralDHKeySize=legacy) | 2048 |
| gateway.threadpool.max | The maximum concurrent requests the server will process. The default is 254. Connections beyond this will be queued. | 254 |
| gateway.httpclient.connectionTimeout | The amount of time to wait when attempting a connection. The natural unit is milliseconds, but a ‘s’ or ‘m’ suffix may be used for seconds or minutes respectively. | 20s |
| gateway.httpclient.maxConnections | The maximum number of connections that a single HttpClient will maintain to a single host:port. | 32 |
| gateway.httpclient.socketTimeout | The amount of time to wait for data on a socket before aborting the connection. The natural unit is milliseconds, but a ‘s’ or ‘m’ suffix may be used for seconds or minutes respectively. | 20s |
| gateway.httpclient.truststore.password.alias | OPTIONAL Alias for the password to the truststore file holding the trusted service certificates. NOTE: An alias with the provided name should be created usingknoxcli.sh create-aliasinorder to provide the password; else the master secret will be used. | gateway-httpclient-truststore-password |
| gateway.httpclient.truststore.path | Location of the truststore for service certificates to be trusted | null |
| gateway.httpclient.truststore.type | Indicates the type of truststore at the path declared ingateway.httpclient.truststore.path | JKS |
| gateway.httpserver.requestBuffer | The size of the HTTP server request buffer in bytes | 16384 |
| gateway.httpserver.requestHeaderBuffer | The size of the HTTP server request header buffer in bytes | 8192 |
| gateway.httpserver.responseBuffer | The size of the HTTP server response buffer in bytes | 32768 |
| gateway.httpserver.responseHeaderBuffer | The size of the HTTP server response header buffer in bytes | 8192 |
| gateway.websocket.feature.enabled | Enable/Disable WebSocket feature | false |
| gateway.tls.keystore.password.alias | OPTIONAL Alias for the password to the keystore file holding the Gateway’s TLS certificate and keypair. NOTE: An alias with the provided name should be created usingknoxcli.sh create-aliasinorder to provide the password; else the master secret will be used. | gateway-identity-keystore-password |
| gateway.tls.keystore.path | OPTIONAL The path to the keystore file where the Gateway’s TLS certificate and keypair are stored. If not set, the default keystore file will be used - data/security/keystores/gateway.jks. | null |
| gateway.tls.keystore.type | OPTIONAL The type of the keystore file where the Gateway’s TLS certificate and keypair are stored. Seegateway.tls.keystore.path. | JKS |
| gateway.tls.key.alias | OPTIONAL The alias for the Gateway’s TLS certificate and keypair within the default keystore or the keystore specified viagateway.tls.keystore.path. | gateway-identity |
| gateway.tls.key.passphrase.alias | OPTIONAL The alias for passphrase for the Gateway’s TLS private key stored within the default keystore or the keystore specified viagateway.tls.keystore.path. NOTE: An alias with the provided name should be created usingknoxcli.sh create-aliasinorder to provide the password; else the keystore password or the master secret will be used. Seegateway.tls.keystore.password.alias | gateway-identity-passphrase |
| gateway.signing.keystore.name | OPTIONAL Filename of keystore file that contains the signing keypair. NOTE: An alias needs to be created usingknoxcli.sh create-aliasfor the alias namesigning.key.passphrasein order to provide the passphrase to access the keystore. | null |
| gateway.signing.keystore.password.alias | OPTIONAL Alias for the password to the keystore file holding the signing keypair. NOTE: An alias with the provided name should be created usingknoxcli.sh create-aliasinorder to provide the password; else the master secret will be used. | signing.keystore.password |
| gateway.signing.keystore.type | OPTIONAL The type of the keystore file where the signing keypair is stored. Seegateway.signing.keystore.name. | JKS |
| gateway.signing.key.alias | OPTIONAL alias for the signing keypair within the keystore specified viagateway.signing.keystore.name | null |
| gateway.signing.key.passphrase.alias | OPTIONAL The alias for passphrase for signing private key stored within the default keystore or the keystore specified viagateway.signing.keystore.name. NOTE: An alias with the provided name should be created usingknoxcli.sh create-aliasinorder to provide the password; else the keystore password or the master secret will be used. Seegateway.signing.keystore.password.alias | signing.key.passphrase |
| ssl.enabled | Indicates whether SSL is enabled for the Gateway | true |
| ssl.include.ciphers | A comma or pipe separated list of ciphers to accept for SSL. See theJSSE Provider docsfor possible ciphers. These can also contain regular expressions as shown in theJetty documentation. | all |
| ssl.exclude.ciphers | A comma or pipe separated list of ciphers to reject for SSL. See theJSSE Provider docsfor possible ciphers. These can also contain regular expressions as shown in theJetty documentation. | none |
| ssl.exclude.protocols | Excludes a comma or pipe separated list of protocols to not accept for SSL or “none” | SSLv3 |
| gateway.remote.config.monitor.client | A reference to theremote configuration registry clientthe remote configuration monitor will employ | null |
| gateway.remote.config.monitor.client.allowUnauthenticatedReadAccess | When a remote registry client is configured to access a registry securely, this property can be set to allow unauthenticated clients to continue to read the content from that registry by setting the ACLs accordingly. | false |
| gateway.remote.config.registry.<name> | A namedremote configuration registry clientdefinition, wherenameis an arbitrary identifier for the connection | null |
| gateway.cluster.config.monitor.ambari.enabled | Indicates whether the Ambari cluster monitoring and associated dynamic topology updating is enabled | false |
| gateway.cluster.config.monitor.ambari.interval | The interval (in seconds) at which the Ambari cluster monitor will poll for cluster configuration changes | 60 |
| gateway.cluster.config.monitor.cm.enabled | Indicates whether the ClouderaManager cluster monitoring and associated dynamic topology updating is enabled | false |
| gateway.cluster.config.monitor.cm.interval | The interval (in seconds) at which the ClouderaManager cluster monitor will poll for cluster configuration changes | 60 |
| gateway.remote.alias.service.enabled | Turn on/off remote alias service | true |
| gateway.read.only.override.topologies | A comma-delimited list of topology names which should be forcibly treated as read-only. | none |
| gateway.discovery.default.address | The default discovery address, which is applied if no address is specified in a descriptor. | null |
| gateway.discovery.default.cluster | The default discovery cluster name, which is applied if no cluster name is specified in a descriptor. | null |
| gateway.dispatch.whitelist | A semicolon-delimited list of regular expressions for controlling to which endpoints Knox dispatches and redirects will be permitted. IfDEFAULTis specified, or the property is omitted entirely, then a default domain-based whitelist will be derived from the Knox host. IfHTTPS_ONLYis specified a default domain-based whitelist will be derived from the Knox host for only HTTPS urls. An empty value means no dispatches will be permitted. | null |
| gateway.dispatch.whitelist.services | A comma-delimited list of service roles to which thegateway.dispatch.whitelistwill be applied. | none |
| gateway.strict.topology.validation | If true, topology XML files will be validated against the topology schema during redeploy | false |
| gateway.topology.redeploy.requires.changes | Iftrue, XML topology redeployment will happen only if the topology content is different than the actually deployed one. That is, a simpletouchcommand will not yield in topology redeployment in this case. | false |
| gateway.global.rules.services | Set the list of service names that have global rules, all services that are not in this list have rules that are treated as scoped to only to that service. | "NAMENODE","JOBTRACKER", "WEBHDFS", "WEBHCAT", "OOZIE", "WEBHBASE", "HIVE", "RESOURCEMANAGER" |
| gateway.xforwarded.header.context.append.servicename | Add service name to x-forward-context header for the defined list of services. | LIVYSERVER |
| gateway.knox.token.exp.server-managed | Default server-managed token state configuration for all KnoxToken service and JWT provider deployments | false |
| gateway.knox.token.eviction.interval | The period (seconds) about which the token state reaper will evict state for expired tokens. This configuration only applies when server-managed token state is enabled either in gateway-site or at the topology level. | 300(5 minutes) |
| gateway.knox.token.eviction.grace.period | A duration (seconds) beyond a token’s expiration to wait before evicting its state. This configuration only applies when server-managed token state is enabled either in gateway-site or at the topology level. | 86400(24 hours) |
| gateway.knox.token.permissive.validation | When this feature is enabled and server managed state is enabled and Knox is presented with a valid token which is absent in server managed state, Knox will verify it without throwing an UnknownTokenException | false |
| gateway.jetty.max.form.content.size | This optional parameter allows end-user to configure the form content in Knox’s embedded Jetty server that a request can process is limited to protect from Denial of Service attacks. The size in bytes is limited by Jetty’s ContextHandler#getMaxFormContentSize() or if there is no context then the “org.eclipse.jetty.server.Request.maxFormContentSize” attribute. | 200000 |
| gateway.jetty.max.form.keys | This optional parameter allows end-user to configure the number of parameters keys is limited by Knox’s embedded Jetty’s ContextHandler#getMaxFormKeys() or if there is no context then theorg.eclipse.jetty.server.Request.maxFormKeysattribute. | 1000 |
| gateway.strict.transport.enabled | This parameter enables the HTTP Strict-Transport-Security response header globally for every response. The topology wide config with WebAppSec provider takes precedence if it is enabled. | false |
| gateway.strict.transport.option | This optional parameter specifies a particular value for the HTTP Strict-Transport-Security header in case the global config is enabled. | max-age=31536000; includeSubDomains |
| gateway.server.append.classpath | A;delimited list of paths that are appended to the gateway server’s classpath. | null |
| gateway.server.prepend.classpath | A;delimited list of paths that are prepended to the gateway server’s classpath. | null |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Topology+Descriptors"></a>Topology Descriptors [](#knox-apache-org-books-knox-2-1-0-user-guide--Topology+Descriptors)

The topology descriptor files provide the gateway with per-cluster configuration information. This includes configuration for both the providers within the gateway and the services within the Hadoop cluster. These files are located in `{GATEWAY_HOME}/conf/topologies`. The general outline of this document looks like this.

```xml
<topology>
    <gateway>
        <provider>
        </provider>
    </gateway>
    <service>
    </service>
</topology>
```

There are typically multiple `<provider>` and `<service>` elements.

- /topologyDefines the provider and configuration and service topology for a single Hadoop cluster.
- /topology/gatewayGroups all of the provider elements
- /topology/gateway/providerDefines the configuration of a specific provider for the cluster.
- /topology/serviceDefines the location of a specific Hadoop service within the Hadoop cluster.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Provider+Configuration"></a>Provider Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Provider+Configuration)

Provider configuration is used to customize the behavior of a particular gateway feature. The general outline of a provider element looks like this.

```xml
<provider>
    <role>authentication</role>
    <name>ShiroProvider</name>
    <enabled>true</enabled>
    <param>
        <name></name>
        <value></value>
    </param>
</provider>
```

- /topology/gateway/providerGroups information for a specific provider.
- /topology/gateway/provider/roleDefines the role of a particular provider. There are a number of pre-defined roles used by out-of-the-box provider plugins for the gateway. These roles are: authentication, identity-assertion, rewrite and hostmap
- /topology/gateway/provider/nameDefines the name of the provider for which this configuration applies. There can be multiple provider implementations for a given role. Specifying the name is used to identify which particular provider is being configured. Typically each topology descriptor should contain only one provider for each role but there are exceptions.
- /topology/gateway/provider/enabledAllows a particular provider to be enabled or disabled via `true` or `false` respectively. When a provider is disabled any filters associated with that provider are excluded from the processing chain.
- /topology/gateway/provider/paramThese elements are used to supply provider configuration. There can be zero or more of these per provider.
- /topology/gateway/provider/param/nameThe name of a parameter to pass to the provider.
- /topology/gateway/provider/param/valueThe value of a parameter to pass to the provider.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Service+Configuration"></a>Service Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Configuration)

Service configuration is used to specify the location of services within the Hadoop cluster. The general outline of a service element looks like this.

```xml
<service>
    <role>WEBHDFS</role>
    <url>http://localhost:50070/webhdfs</url>
</service>
```

- /topology/serviceProvider information about a particular service within the Hadoop cluster. Not all services are necessarily exposed as gateway endpoints.
- /topology/service/roleIdentifies the role of this service. Currently supported roles are: WEBHDFS, WEBHCAT, WEBHBASE, OOZIE, HIVE, NAMENODE, JOBTRACKER, RESOURCEMANAGER Additional service roles can be supported via plugins. Note: The role names are case sensitive and must be upper case.
- topology/service/urlThe URL identifying the location of a particular service within the Hadoop cluster.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hostmap+Provider"></a>Hostmap Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--Hostmap+Provider)

The purpose of the Hostmap provider is to handle situations where hosts are known by one name within the cluster and another name externally. This frequently occurs when virtual machines are used and in particular when using cloud hosting services. Currently, the Hostmap provider is configured as part of the topology file. The basic structure is shown below.

```xml
<topology>
    <gateway>
        ...
        <provider>
            <role>hostmap</role>
            <name>static</name>
            <enabled>true</enabled>
            <param><name>external-host-name</name><value>internal-host-name</value></param>
        </provider>
        ...
    </gateway>
    ...
</topology>
```

This mapping is required because the Hadoop services running within the cluster are unaware that they are being accessed from outside the cluster. Therefore URLs returned as part of REST API responses will typically contain internal host names. Since clients outside the cluster will be unable to resolve those host name they must be mapped to external host names.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hostmap+Provider+Example+-+EC2"></a>Hostmap Provider Example - EC2 [](#knox-apache-org-books-knox-2-1-0-user-guide--Hostmap+Provider+Example+-+EC2)

Consider an EC2 example where two VMs have been allocated. Each VM has an external host name by which it can be accessed via the internet. However the EC2 VM is unaware of this external host name and instead is configured with the internal host name.

```text
External HOSTNAMES:
ec2-23-22-31-165.compute-1.amazonaws.com
ec2-23-23-25-10.compute-1.amazonaws.com

Internal HOSTNAMES:
ip-10-118-99-172.ec2.internal
ip-10-39-107-209.ec2.internal
```

The Hostmap configuration required to allow access external to the Hadoop cluster via the Apache Knox Gateway would be this:

```xml
<topology>
    <gateway>
        ...
        <provider>
            <role>hostmap</role>
            <name>static</name>
            <enabled>true</enabled>
            <param>
                <name>ec2-23-22-31-165.compute-1.amazonaws.com</name>
                <value>ip-10-118-99-172.ec2.internal</value>
            </param>
            <param>
                <name>ec2-23-23-25-10.compute-1.amazonaws.com</name>
                <value>ip-10-39-107-209.ec2.internal</value>
            </param>
        </provider>
        ...
    </gateway>
    ...
</topology>
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hostmap+Provider+Example+-+Sandbox"></a>Hostmap Provider Example - Sandbox [](#knox-apache-org-books-knox-2-1-0-user-guide--Hostmap+Provider+Example+-+Sandbox)

The Hortonworks Sandbox 2.x poses a different challenge for host name mapping. This version of the Sandbox uses port mapping to make the Sandbox VM appear as though it is accessible via localhost. However the Sandbox VM is internally configured to consider sandbox.hortonworks.com as the host name. So from the perspective of a client accessing Sandbox the external host name is localhost. The Hostmap configuration required to allow access to Sandbox from the host operating system is this.

```xml
<topology>
    <gateway>
        ...
        <provider>
            <role>hostmap</role>
            <name>static</name>
            <enabled>true</enabled>
            <param>
                <name>localhost</name>
                <value>sandbox,sandbox.hortonworks.com</value>
            </param>
        </provider>
        ...
    </gateway>
    ...
</topology>
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hostmap+Provider+Configuration"></a>Hostmap Provider Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Hostmap+Provider+Configuration)

Details about each provider configuration element is enumerated below.

- topology/gateway/provider/roleThe role for a Hostmap provider must always be `hostmap`.
- topology/gateway/provider/nameThe Hostmap provider supplied out-of-the-box is selected via the name `static`.
- topology/gateway/provider/enabledHost mapping can be enabled or disabled by providing `true` or `false`.
- topology/gateway/provider/paramHost mapping is configured by providing parameters for each external to internal mapping.
- topology/gateway/provider/param/nameThe parameter names represent the external host names associated with the internal host names provided by the value element. This can be a comma separated list of host names that all represent the same physical host. When mapping from internal to external host name the first external host name in the list is used.
- topology/gateway/provider/param/valueThe parameter values represent the internal host names associated with the external host names provider by the name element. This can be a comma separated list of host names that all represent the same physical host. When mapping from external to internal host names the first internal host name in the list is used.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Simplified+Topology+Descriptors"></a>Simplified Topology Descriptors [](#knox-apache-org-books-knox-2-1-0-user-guide--Simplified+Topology+Descriptors)

Simplified descriptors are a means to facilitate provider configuration sharing and service endpoint discovery. Rather than editing an XML topology descriptor, it’s possible to create a simpler YAML (or JSON) descriptor specifying the desired contents of a topology, which will yield a full topology descriptor and deployment.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Externalized+Provider+Configurations"></a>Externalized Provider Configurations [](#knox-apache-org-books-knox-2-1-0-user-guide--Externalized+Provider+Configurations)

Sometimes, the same provider configuration is applied to multiple Knox topologies. With the provider configuration externalized from the simple descriptors, a single configuration can be referenced by multiple topologies. This helps reduce the duplication of configuration, and the need to update multiple configuration files when a policy change is required. Updating a provider configuration will trigger an update to all those topologies that reference it.

The contents of externalized provider configuration details are identical to the contents of the gateway element from a full topology descriptor. The only difference is that those details are defined in a separate JSON/YAML file in `{GATEWAY_HOME}/conf/shared-providers/`, which is then referenced by one or more descriptors.

*Provider Configuration Example*

```json
{
  "providers": [
    {
      "role": "authentication",
      "name": "ShiroProvider",
      "enabled": "true",
      "params": {
        "sessionTimeout": "30",
        "main.ldapRealm": "org.apache.knox.gateway.shirorealm.KnoxLdapRealm",
        "main.ldapContextFactory": "org.apache.knox.gateway.shirorealm.KnoxLdapContextFactory",
        "main.ldapRealm.contextFactory": "$ldapContextFactory",
        "main.ldapRealm.userDnTemplate": "uid={0},ou=people,dc=hadoop,dc=apache,dc=org",
        "main.ldapRealm.contextFactory.url": "ldap://localhost:33389",
        "main.ldapRealm.contextFactory.authenticationMechanism": "simple",
        "urls./**": "authcBasic"
      }
    },
    {
      "name": "static",
      "role": "hostmape",
      "enabled": "true",
      "params": {
        "localhost": "sandbox,sandbox.hortonworks.com"
      }
    }
  ]
}
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Sharing+HA+Providers"></a>Sharing HA Providers [](#knox-apache-org-books-knox-2-1-0-user-guide--Sharing+HA+Providers)

HA Providers are a special concern with respect to sharing provider configuration because they include service-specific (and possibly cluster-specific) configuration.

This requires extra attention because the service configurations corresponding to the associated HA Provider configuration must contain the correct content to function properly.

For a shared provider configuration with an HA Provider service:

- If the referencing descriptor does not declare the corresponding service, then the HA Provider configuration is effectively ignored since the service isn’t exposed by the topology.
- If a corresponding service is declared in the descriptor
  - If service endpoint discovery is employed, then Knox should populate the URLs correctly to support the HA behavior.
  - Otherwise, the URLs must be explicitly specified for that service in the descriptor.
- If the descriptor content is correct, but the cluster service is not configured for HA, then the HA behavior obviously won’t work.

***Apache ZooKeeper-based HA Provider Services***

The HA Provider configuration for some services (e.g., [HiveServer2](#knox-apache-org-books-knox-2-1-0-user-guide--HiveServer2+HA), [Kafka](#knox-apache-org-books-knox-2-1-0-user-guide--Kafka+HA)) includes references to Apache ZooKeeper hosts (i.e., the ZooKeeper ensemble) and namespaces. It’s important to understand the relationship of that ensemble configuration to the topologies referencing it. These ZooKeeper details are often cluster-specific. If the ZooKeeper ensemble in the provider configuration is part of cluster *A*, then it’s probably incorrect to reference it in a topology for cluster *B* since the Hadoop service endpoints will probably be the wrong ones. However, if multiple clusters are working with a common ZooKeeper ensemble, then sharing this provider configuration *may* be appropriate.

*It’s always best to specify cluster-specific details in a descriptor rather than a provider configuration.*

All of the service attributes, which can be specified in the HaProvider, can also be specified as params in the corresponding service declaration in the descriptor. If an attribute is specified in both the service declaration and the HaProvider, then the service-level value **overrides** the HaProvider-level value.

```text
"services": [
  {
    "name": "HIVE",
    "params": {
      "enabled": "true",
      "zookeeperEnsemble": "host1:2181,host2:2181,host3:2181",
      "zookeeperNamespace" : "hiveserver2",
      "maxRetryAttempts" : "100"
    }
  }
]
```

Note that Knox can dynamically determine these ZooKeeper ensemble details for *some* services; for others, they are static provider configuration details. The services for which Knox can discover the cluster-specific ZooKeeper details include:

- YARN
- HIVE
- WEBHDFS
- WEBHBASE
- WEBHCAT
- OOZIE
- ATLAS
- ATLAS-API
- KAFKA

For a subset of these supported services, Knox can also determine whether ZooKeeper-based HA is enabled or not. This means that the *enabled* attribute of the HA Provider configuration for these services may be set to **auto**, and Knox will determine whether or not it is enabled based on that service’s configuration in the target cluster.

```json
{
  "providers": [
    {
      "role": "ha",
      "name": "HaProvider",
      "enabled": "true",
      "params": {
        "WEBHDFS": "maxFailoverAttempts=3;failoverSleep=1000;maxRetryAttempts=3;retrySleep=1000;enabled=true",
        "HIVE": "maxFailoverAttempts=10;failoverSleep=1000;maxRetryAttempts=5;retrySleep=1000;enabled=auto",
        "YARN": "maxFailoverAttempts=5;failoverSleep=5000;maxRetryAttempts=3;retrySleep=1000;enabled=auto"
      }
    }
  ]
}
```

These services include:

- YARN
- HIVE
- ATLAS
- ATLAS-API

Be sure to pay extra attention when sharing HA Provider configuration across topologies.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Simplified+Descriptor+Files"></a>Simplified Descriptor Files [](#knox-apache-org-books-knox-2-1-0-user-guide--Simplified+Descriptor+Files)

Simplified descriptors allow service URLs to be defined explicitly, just like full topology descriptors. However, if URLs are omitted for a service, Knox will attempt to discover that service’s URLs from the Hadoop cluster. Currently, this behavior is only supported for clusters managed by Apache Ambari or Cloudera Manager. In any case, the simplified descriptors are much more concise than a full topology descriptor.

*Descriptor Properties*

| Property | Description |
| --- | --- |
| discovery-type | The discovery source type. (Currently, the only supported types areAMBARIandClouderaManager). |
| discovery-address | The endpoint address for the discovery source. |
| discovery-user | The username with permission to access the discovery source. Optional, if the discovery type supports default credential aliases. |
| discovery-pwd-alias | The alias of the password for the user with permission to access the discovery source. Optional, if the discovery type supports default credential aliases. |
| provider-config-ref | A reference to a provider configuration in{GATEWAY_HOME}/conf/shared-providers/. |
| cluster | The name of the cluster from which the topology service endpoints should be determined. |
| services | The collection of services to be included in the topology. |
| applications | The collection of applications to be included in the topology. |

Two file formats are supported for two distinct purposes.

- YAML is intended for the individual hand-editing a simplified descriptor because of its readability.
- JSON is intended to be used for [API](#knox-apache-org-books-knox-2-1-0-user-guide--Admin+API) interactions.

That being said, there is nothing preventing the hand-editing of files in the JSON format. However, the API will *not* accept YAML files as input.

*YAML Example* (based on the HDP Docker Sandbox)

```text
---
# Discovery source config
discovery-type : AMBARI
discovery-address : http://sandbox.hortonworks.com:8080

# If this is not specified, the alias ambari.discovery.user is checked for a username
discovery-user : maria_dev

# If this is not specified, the default alias ambari.discovery.password is used
discovery-pwd-alias : sandbox.discovery.password

# Provider config reference, the contents of which will be included in the resulting topology descriptor
provider-config-ref : sandbox-providers

# The cluster for which the details should be discovered
cluster: Sandbox

# The services to declare in the resulting topology descriptor, whose URLs will be discovered (unless a value is specified)
services:
    - name: NAMENODE
    - name: JOBTRACKER
    - name: WEBHDFS
    - name: WEBHCAT
    - name: OOZIE
    - name: WEBHBASE
    - name: HIVE
    - name: RESOURCEMANAGER
    - name: KNOXSSO
      params:
          knoxsso.cookie.secure.only: true
          knoxsso.token.ttl: 100000
    - name: AMBARI
      urls:
          - http://sandbox.hortonworks.com:8080
    - name: AMBARIUI
      urls:
          - http://sandbox.hortonworks.com:8080
    - name: AMBARIWS
      urls:
          - ws://sandbox.hortonworks.com:8080
```

*JSON Example* (based on the HDP Docker Sandbox)

```json
{
  "discovery-type":"AMBARI",
  "discovery-address":"http://sandbox.hortonworks.com:8080",
  "discovery-user":"maria_dev",
  "discovery-pwd-alias":"sandbox.discovery.password",
  "provider-config-ref":"sandbox-providers",
  "cluster":"Sandbox",
  "services":[
    {"name":"NAMENODE"},
    {"name":"JOBTRACKER"},
    {"name":"WEBHDFS"},
    {"name":"WEBHCAT"},
    {"name":"OOZIE"},
    {"name":"WEBHBASE"},
    {"name":"HIVE"},
    {"name":"RESOURCEMANAGER"},
    {"name":"KNOXSSO",
      "params":{
      "knoxsso.cookie.secure.only":"true",
      "knoxsso.token.ttl":"100000"
      }
    },
    {"name":"AMBARI", "urls":["http://sandbox.hortonworks.com:8080"]},
    {"name":"AMBARIUI", "urls":["http://sandbox.hortonworks.com:8080"],
    {"name":"AMBARIWS", "urls":["ws://sandbox.hortonworks.com:8080"]}
  ]
}
```

Both of these examples illustrate the specification of credentials for the interaction with the discovery source. If no credentials are specified, then the default aliases are queried (if default aliases are supported for that discovery type). Use of the default aliases is sufficient for scenarios where Knox will only discover topology details from a single source. For multiple discovery sources however, it’s most likely that each will require different sets of credentials. The discovery-user and discovery-pwd-alias properties exist for this purpose. Note that whether using the default credential aliases or specifying a custom password alias, these [aliases must be defined](#knox-apache-org-books-knox-2-1-0-user-guide--Alias+creation) prior to any attempt to deploy a topology using a simplified descriptor.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Cluster+Discovery+Provider+Extensions"></a>Cluster Discovery Provider Extensions [](#knox-apache-org-books-knox-2-1-0-user-guide--Cluster+Discovery+Provider+Extensions)

To support the ability to dynamically discover the endpoints for services being proxied, Knox provides cluster discovery provider extensions for Apache Ambari and Cloudera Manager. The Ambari support has been available since Knox 1.1.0, and limited support for Cloudera Manager has been added in Knox 1.3.0.

These extensions allow discovery sources of the respective types to be queried for cluster details used to generate topologies from [simplified descriptors](#knox-apache-org-books-knox-2-1-0-user-guide--Simplified+Descriptor+Files). The extension to be employed is specified on a per-descriptor basis, using the `discovery-type` descriptor property.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Deployment+Directories"></a>Deployment Directories [](#knox-apache-org-books-knox-2-1-0-user-guide--Deployment+Directories)

Effecting topology changes is as simple as modifying files in two specific directories.

The `{GATEWAY_HOME}/conf/shared-providers/` directory is the location where Knox looks for provider configurations. This directory is monitored for changes, such that modifying a provider configuration file therein will trigger updates to any referencing simplified descriptors in the `{GATEWAY_HOME}/conf/descriptors/` directory. *Care should be taken when deleting these files if there are referencing descriptors; any subsequent modifications of referencing descriptors will fail when the deleted provider configuration cannot be found. The references should all be modified before deleting the provider configuration.*

Likewise, the `{GATEWAY_HOME}/conf/descriptors/` directory is monitored for changes, such that adding or modifying a simplified descriptor file in this directory will trigger the generation and deployment of a topology descriptor. Deleting a descriptor from this directory will conversely result in the removal of the previously-generated topology descriptor, and the associated topology will be undeployed.

If the service details for a deployed (generated) topology are changed in the cluster, then the Knox topology can be updated by ’touch’ing the simplified descriptor. This will trigger discovery and regeneration/redeployment of the topology descriptor.

Note that deleting a generated topology descriptor from `{GATEWAY_HOME}/conf/topologies/` is not sufficient for its removal. If the source descriptor is modified, or Knox is restarted, the topology descriptor will be regenerated and deployed. Removing generated topology descriptors should be done by removing the associated simplified descriptor. For the same reason, editing generated topology descriptors is strongly discouraged since they can be inadvertently overwritten.

Another means by which these topology changes can be effected is the [Admin API](#knox-apache-org-books-knox-2-1-0-user-guide--Admin+API).

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Cloud+Federation+Configuration"></a>Cloud Federation Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Cloud+Federation+Configuration)

Cloud Federation feature allows for a topology based federation from one Knox instance to another (from on-prem Knox instance to cloud knox instance).

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Cluster+Configuration+Monitoring"></a>Cluster Configuration Monitoring [](#knox-apache-org-books-knox-2-1-0-user-guide--Cluster+Configuration+Monitoring)

Another benefit gained through the use of simplified topology descriptors, and the associated service discovery, is the ability to monitor clusters for configuration changes. **This is currently only available for clusters managed by Ambari and ClouderaManager.**

The gateway can monitor cluster configurations, and respond to changes by dynamically regenerating and redeploying the affected topologies. The following properties in gateway-site.xml can be used to control this behavior.

```xml
<property>
    <name>gateway.cluster.config.monitor.ambari.enabled</name>
    <value>false</value>
    <description>Enable/disable Ambari cluster configuration monitoring.</description>
</property>

<property>
    <name>gateway.cluster.config.monitor.ambari.interval</name>
    <value>60</value>
    <description>The interval (in seconds) for polling Ambari for cluster configuration changes.</description>
</property>

<!-- Cloudera Manager specific configuration -->

<property>
    <name>gateway.cluster.config.monitor.cm.enabled</name>
    <value>false</value>
    <description>Enable/disable Cloudera Manager cluster configuration monitoring.</description>
</property>

<property>
    <name>gateway.cluster.config.monitor.cm.interval</name>
    <value>60</value>
    <description>The interval (in seconds) for polling Cloudera Manager for cluster configuration changes.</description>
</property>

<property>
    <name>gateway.cloudera.manager.descriptors.monitor.interval</name>
    <value>-1</value>
    <description>The interval (in milliseconds) for monitoring Hadoop XML resources in "GATEWAY_HOME/data/descriptors" with `.hxr` file postfix (see details below). If this property is set to a non-positive integer, monitoring of Hadoop XML resources is disabled.</description>
</property>

<property>
    <name>gateway.cloudera.manager.advanced.service.discovery.config.monitor.interval</name>
    <value>-1</value>
    <description>The interval (in milliseconds) for monitoring GATEWAY_HOME/conf/auto-discovery-advanced-configuration.properties (if exists) and notifies any AdvancedServiceDiscoveryConfigChangeListener if the file is changed since the last time it was loaded. Advanced configuration processing in Knox's service discovery flow helps fine-tuned service-level enablement on the topology level (see details below). If this property is set to a non-positive integer, this feature is disabled.</description>
</property>

<property>
    <name>gateway.cloudera.manager.service.discovery.maximum.retry.attemps</name>
    <value>3</value>
    <description>The maximum number of attempts Knox will try to connect to the configured Cloudera Manager cluster if there was a connection error.</description>
</property>

<property>
    <name>gateway.cloudera.manager.service.discovery.repository.cache.entry.ttl</name>
    <value>60</value>
    <description>Upon a successful Cloudera Manager service discovery event, Knox maintains an in-memory cache (repository) of discovered data (cluster, service, and role configs). This property indicates the entry TTL of this cache. See KNOX-2680 for more details.</description>
</property>
```

Since service discovery supports multiple Ambari or ClouderaManager instances as discovery sources, multiple instances can be monitored for cluster configuration changes.

For example, if the cluster monitor is enabled, deployment of the following simple descriptor would trigger monitoring of the *Sandbox* cluster managed by Ambari @ [http://sandbox.hortonworks.com:8080](http://sandbox.hortonworks.com:8080)

```text
---
discovery-address : http://sandbox.hortonworks.com:8080
discovery-user : maria_dev
discovery-pwd-alias : sandbox.discovery.password
cluster: Sandbox
provider-config-ref : sandbox-providers
services:
    - name: NAMENODE
    - name: JOBTRACKER
    - name: WEBHDFS
    - name: WEBHCAT
    - name: OOZIE
    - name: WEBHBASE
    - name: HIVE
    - name: RESOURCEMANAGER
```

Another *Sandbox* cluster, managed by a **different** Ambari instance, could simultaneously be monitored by the same gateway instance.

Now, topologies can be kept in sync with their respective target cluster configurations, without administrator intervention or service interruption.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+XML+resource+monitoring"></a>Hadoop XML resource monitoring [](#knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+XML+resource+monitoring)

As described in [KNOX-2160,](https://issues.apache.org/jira/browse/KNOX-2160) to support topology management in Cloudera Manager it’s beneficial that Knox is able to process a descriptor that CM can generate natively. As of now, Cloudera Manager’s CSD framework is capable of producing a file of its parameters in the following formats: - Hadoop XML - properties - gflags

As the `gateway-site.xml` uses the Hadoop XML format it’s quite obvious that the first option is the one that fits Knox the most. One XML type descriptor file may contain one or more Knox descriptors using the following structure:

- the configuration `name` would indicate the descriptor (topology) name
- the configuration `value` would list all properties of a Knox descriptor
  - service discovery related information (type, address, cluster, user/password alias)
  - services
    - name
    - url
    - version (optional)
    - parameters (optional)
  - applications (optional)
    - name
    - parameters (optional)

A sample descriptor file would look like this:

```xml
<configuration>
  <property>
    <name>topology1</name>
    <value>
        discoveryType=ClouderaManager;
        discoveryAddress=http://host:123;
        discoveryUser=user;
        discoveryPasswordAlias=alias;
        cluster=Cluster 1;
        providerConfigRef=topology1-provider;
        app:knoxauth:param1.name=param1.value;
        app:KNOX;
        HIVE:url=http://localhost:389;
        HIVE:version=1.0;
        HIVE:httpclient.connectionTimeout=5m;
        HIVE:httpclient.socketTimeout=100m
    </value>
  </property>
  <property>
    <name>topology2</name>
    <value>
        discoveryType=ClouderaManager;
        discoveryAddress=http://host:123;
        discoveryUser=user;
        discoveryPasswordAlias=alias;
        cluster=Cluster 1;
        providerConfigRef=topology2-provider;
        app:KNOX;
        HDFS.url=https://localhost:443;
        HDFS:httpclient.connectionTimeout=5m;
        HDFS:httpclient.socketTimeout=100m
     </value>
  </property>
</configuration>
```

Workflow:

1. this kind of descriptor should also be placed in Knox’s descriptor directory using a `.hxr` file prefix (e.g. `cm-descriptors.hxr`)
2. once it’s added or modified Knox’s Hadoop XML resource monitor should parse the XML and build one or more instance(s) of `org.apache.knox.gateway.topology.simple.SimpleDescriptor`
3. after the Java object(s) got created it (they) should be saved in the Knox descriptor directory in JSON format. As a result, the same monitor should parse the new/modified JSON descriptor(s) and re-deploys it (them) using the already existing mechanism

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Advanced+service+discovery+configuration+monitoring"></a>Advanced service discovery configuration monitoring [](#knox-apache-org-books-knox-2-1-0-user-guide--Advanced+service+discovery+configuration+monitoring)

With the above-discussed Hadoop XML resource monitoring Knox is capable of processing a Hadoop XML configuration file and turn its content into Knox providers.

Advanced service discovery configuration monitor extends that feature in a way it supports for the following use cases that are also Cloudera Manager integration specific:

1.) Cloudera Manager reports if auto-discovery is enabled for each known services. That is, a list of boolean properties can be generated by CM indicating if `SERVICE_X` is enabled or not in the following form: `gateway.auto.discovery.enabled.SERVICE_NAME=[true|false]`

The new Hadoop XML configuration parser should take this information into account, and add a certain service into the generated Knox descriptor **iff** that service is explicitly enabled or there is no boolean flag within the CM generated properties with that service name (indicating an unknown - custom - service).

Additionally, a set of known (expected) topologies can be listed by CM. These topologies (Knox descriptors) should be populated with an enabled service (only with its name) even if that particular service was not listed in the new style Hadoop XML configuration file within the descriptor that matches any of the expected topology names.

2.) There are some services - mainly UI services - that are not working without some more required services in place (mainly their API counterpart). For instance: `RANGERUI` won’t work properly if `RANGER` is not available.

Knox’s Hadoop XML configuration parser was modified to exclude any service from the generated Knox descriptor unless - all required services are available (if any) - all required services are enabled (see the previous point)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Monitor"></a>Remote Configuration Monitor [](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Monitor)

In addition to monitoring local directories for provider configurations and simplified descriptors, the gateway similarly supports monitoring either ZooKeeper or an SQL database.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Zookeeper+based+monitor"></a>Zookeeper based monitor [](#knox-apache-org-books-knox-2-1-0-user-guide--Zookeeper+based+monitor)

This monitor depends on a [remote configuration registry client](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Registry+Clients), and that client must be specified by setting the following property in gateway-site.xml

```xml
<property>
    <name>gateway.service.remoteconfigurationmonitor.impl</name>
    <value>org.apache.knox.gateway.topology.monitor.db.ZkRemoteConfigurationMonitorService</value>
</property>

<property>
    <name>gateway.remote.config.monitor.client</name>
    <value>sandbox-zookeeper-client</value>
    <description>Remote configuration monitor client name.</description>
</property>
```

This client identifier is a reference to a remote configuration registry client, as in this example (also defined in gateway-site.xml)

```xml
<property>
    <name>gateway.remote.config.registry.sandbox-zookeeper-client</name>
    <value>type=ZooKeeper;address=localhost:2181</value>
    <description>ZooKeeper configuration registry client details.</description>
</property>
```

*The actual name of the client (e.g., sandbox-zookeeper-client) is not important, except that the reference matches the name specified in the client definition.*

With this configuration, the gateway will monitor the following znodes in the specified ZooKeeper instance

```text
/knox
    /config
        /shared-providers
        /descriptors
```

The creation of these znodes, and the population of their respective contents, is an activity **not** currently managed by the gateway. However, the [KNOX CLI](#knox-apache-org-books-knox-2-1-0-user-guide--Knox+CLI) includes commands for managing the contents of these znodes.

These znodes are treated similarly to the local *shared-providers* and *descriptors* directories described in [Deployment Directories](#knox-apache-org-books-knox-2-1-0-user-guide--Deployment+Directories). When the monitor notices a change to these znodes, it will attempt to effect the same change locally.

If a provider configuration is added to the */knox/config/shared-providers* znode, the monitor will download the new configuration to the local shared-providers directory. Likewise, if a descriptor is added to the */knox/config/descriptors* znode, the monitor will download the new descriptor to the local descriptors directory, which will trigger an attempt to generate and deploy a corresponding topology.

Modifications to the contents of these znodes, will yield the same behavior as can be seen resulting from the corresponding local modification.

| znode | action | result |
| --- | --- | --- |
| /knox/config/shared-providers | add | Download the new file to the local shared-providers directory |
| /knox/config/shared-providers | modify | Download the new file to the local shared-providers directory; If there are any existing descriptor references, then topology will be regenerated and redeployed for those referencing descriptors. |
| /knox/config/shared-providers | delete | Delete the corresponding file from the local shared-providers directory |
| /knox/config/descriptors | add | Download the new file to the local descriptors directory; A corresponding topology will be generated and deployed. |
| /knox/config/descriptors | modify | Download the new file to the local descriptors directory; The corresponding topology will be regenerated and redeployed. |
| /knox/config/descriptors | delete | Delete the corresponding file from the local descriptors directory |

This simplifies the configuration for HA gateway deployments, in that the gateway instances can all be configured to monitor the same ZooKeeper instance, and changes to the znodes’ contents will be applied to all those gateway instances. With this approach, it is no longer necessary to manually deploy topologies to each of the gateway instances.

*A Note About ACLs*

```text
While the gateway does not currently require secure interactions with remote registries, it is recommended
that ACLs be applied to restrict at least writing of the entries referenced by this monitor. If write
access is available to everyone, then the contents of the configuration cannot be known to be trustworthy,
and there is the potential for malicious activity. Be sure to carefully consider who will have the ability
to define configuration in monitored remote registries and apply the necessary measures to ensure its
trustworthiness.
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Registry+Clients"></a>Remote Configuration Registry Clients [](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Registry+Clients)

One or more features of the gateway employ remote configuration registry (e.g., ZooKeeper) clients. These clients are configured by setting properties in the gateway configuration (`gateway-site.xml`).

Each client configuration is a single property, the name of which is prefixed with **gateway.remote.config.registry.** and suffixed by the client identifier. The value of such a property, is a registry-type-specific set of semicolon-delimited properties for that client, including the type of registry with which it will interact.

```xml
<property>
    <name>gateway.remote.config.registry.a-zookeeper-client</name>
    <value>type=ZooKeeper;address=zkhost1:2181,zkhost2:2181,zkhost3:2181</value>
    <description>ZooKeeper configuration registry client details.</description>
</property>
```

In the preceeding example, the client identifier is **a-zookeeper-client**, by way of the property name **gateway.remote.config.registry.a-zookeeper-client**.

The property value specifies that the client is intended to interact with ZooKeeper. It also specifies the particular ZooKeeper ensemble with which it will interact; this could be a single ZooKeeper instance as well.

The property value may also include an optional namespace, to which the client will be restricted (i.e., “chroot” the client).

```xml
<property>
    <name>gateway.remote.config.registry.a-zookeeper-client</name>
    <value>type=ZooKeeper;address=zkhost1:2181,zkhost2:2181,zkhost3:2181;namespace=/knox/config</value>
    <description>ZooKeeper configuration registry client details.</description>
</property>
```

At least for the ZooKeeper type, authentication details may also be specified as part of the property value, for interacting with instances for which authentication is required.

**Digest Authentication Example**

```xml
<property>
    <name>gateway.remote.config.registry.a-zookeeper-client</name>
    <value>type=ZooKeeper;address=zkhost1:2181,zkhost2:2181,zkhost3:2181;authType=Digest;principal=myzkuser;credentialAlias=myzkpass</value>
    <description>ZooKeeper configuration registry client details.</description>
</property>
```

**Kerberos Authentication Example**

```xml
<property>
    <name>gateway.remote.config.registry.a-zookeeper-client</name>
    <value>type=ZooKeeper;address=zkhost1:2181,zkhost2:2181,zkhost3:2181;authType=Kerberos;principal=myzkuser;keytab=/home/user/myzk.keytab;useKeyTab=true;useTicketCache=false</value>
    <description>ZooKeeper configuration registry client details.</description>
</property>
```

*While multiple such clients can be configured, for ZooKeeper clients, there is currently a limitation with respect to authentication. Multiple clients cannot each have distinct authentication configurations. This limitation is imposed by the underlying ZooKeeper client. Therefore, the clients must all be insecure (no authentication configured), or they must all authenticate to the same ZooKeeper using the same credentials.*

The [remote configuration monitor](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Monitor) facility uses these client configurations to perform its function.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--SQL+databse+based+monitor"></a>SQL databse based monitor [](#knox-apache-org-books-knox-2-1-0-user-guide--SQL+databse+based+monitor)

The SQL based remote configuration monitor works like the Zookeeper monitor, but it monitors a relational database.

Enabling this monitor requires setting gateway.service.remoteconfigurationmonitor.impl in gateway-site.xml to org.apache.knox.gateway.topology.monitor.db.DbRemoteConfigurationMonitorService.

```xml
<property>
  <name>gateway.service.remoteconfigurationmonitor.impl</name>
  <value>org.apache.knox.gateway.topology.monitor.db.DbRemoteConfigurationMonitorService</value>
</property>
```

Valid database settings need to be configured in gateway-site.xml. See “Configuring the JDBC token state service” for more information about the database configuration.

```xml
<property>
  <name>gateway.database.type</name>
  <value>mysql</value>
</property>
<property>
  <name>gateway.database.connection.url</name>
  <value>jdbc:mysql://localhost:3306/knox</value>
</property>
```

With this configuration, the gateway will periodically check the content of the KNOX\_PROVIDERS and KNOX\_DESCRIPTORS tables and it will modify (create, update or delete) the local files under the shared-providers and descriptors directories respectively.

The interval (in seconds) at which the remote configuration monitor will poll the database is controlled by the following property.

```text
gateway.remote.config.monitor.db.poll.interval.seconds
```

The default value is 30 seconds.

- If a remote configuration exists in the datbase but doesn’t exist on the file system, then the monitor is going to create the file with corresponding content.
- If an existing remote configuration was deleted from the database (logical deletion) but it still exists on the local file system, then the monitor is going to delete the corresponding file. The logically deleted records are eventually cleared up by the monitor.
- If a remote configuration exists in the database with a different content than the local file, then the monitor is going to update the content of the local file with the content from the database. However to avoid unnecessary IO operations the monitor only updates the content once and if there were no further changes in the database since this last update time, then it will skip changing the local content until a new change happens in the database (indicated by the last\_modified\_time column). This means you can do temporary changes on the local file system without losing your modifications (until a change happens in the database).

The content of the database *must* be changed by the Knox Admin UI or the by the Admin API.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Remote+Alias+Service"></a>Remote Alias Service [](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Alias+Service)

Knox can be configured to use a remote alias service. The remote alias service is pluggable to support multiple different backends. The feature can be disabled by setting the property `gateway.remote.alias.service.enabled` to `false` in `gateway-site.xml`. Knox needs to be restarted for this change to take effect.

```xml
<property>
    <name>gateway.remote.alias.service.enabled</name>
    <value>false</value>
    <description>Turn on/off Remote Alias service (true by default)</description>
</property>
```

The type of remote alias service can be configured by default using `gateway.remote.alias.service.config.type`. If necessary the remote alias service config prefix can be changed with `gateway.remote.alias.service.config.prefix`. Changing the prefix affects all remote alias service configurations.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Remote+Alias+Service+-+HashiCorp+Vault"></a>Remote Alias Service - HashiCorp Vault [](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Alias+Service+-+HashiCorp+Vault)

The HashiCorp Vault remote alias service is deigned to store aliases into HashiCorp Vault. It is configured by setting `gateway.remote.alias.service.config.type` to `hashicorp.vault` in gateway-site.xml. The table below highlights configuration parameters for the HashiCorp Vault remote alias service. Knox needs to be restarted for this change to take effect.

| Property | Description |
| --- | --- |
| gateway.remote.alias.service.config.hashicorp.vault.address | Address of the HashiCorp Vault server |
| gateway.remote.alias.service.config.hashicorp.vault.secrets.engine | HashiCorp Vault secrets engine |
| gateway.remote.alias.service.config.hashicorp.vault.path.prefix | HashiCorp Vault secrets engine path prefix |

There are multiple authentication mechanisms supported by HashiCorp Vault. Knox supports pluggable authentication mechanisms. The authentication type is configured by setting `gateway.remote.alias.service.config.hashicorp.vault.authentication.type` in gateway-site.xml.

**Token Authentication**

Token authentication takes a single setting `gateway.remote.alias.service.config.hashicorp.vault.authentication.token` and takes either the value of the authentication token or a local alias configured with `${ALIAS=token_name}`.

**Kubernetes Authentication**

Kubernetes authentication takes a single setting `gateway.remote.alias.service.config.hashicorp.vault.authentication.kubernetes.role` which defines the role to use when connecting to Vault. The Kubernetes authentication mechanism uses the secrets prepopulated into a K8S pod to authenticate to Vault. Knox can then use the secrets from Vault after being authenticated.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Remote+Alias+Service+-+Zookeeper"></a>Remote Alias Service - Zookeeper [](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Alias+Service+-+Zookeeper)

The Zookeeper remote alias service is designed to store aliases into Apache Zookeeper. It supports monitoring for remote aliases that are added, deleted or updated. The Zookeeper remote alias service is configured by turning the Remote Configuration Monitor on and setting `gateway.remote.alias.service.config.type` to `zookeeper` in gateway-site.xml. Knox needs to be restarted for this change to take effect.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Logging"></a>Logging [](#knox-apache-org-books-knox-2-1-0-user-guide--Logging)

If necessary you can enable additional logging by editing the `log4j2.xml` file in the `conf` directory. Changing the `rootLogger` value from `ERROR` to `DEBUG` will generate a large amount of debug logging. A number of useful, more fine loggers are also provided in the file.

With the 2.0 release, Knox uses Log4j 2 instead of Log4j 1. Apache Log4j 2 is the successor of Log4j 1, but it is incompatible with its predecessor. The main differene is that from now on Knox uses XML file format for configuring logging properties instead of `.property` files.

If you have an existing deployment with customized Log4j 1 `.property` files you will need to convert them to the new Log4j 2 format.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Launcher+changes"></a>Launcher changes [](#knox-apache-org-books-knox-2-1-0-user-guide--Launcher+changes)

The JVM property name that specifies the location of the Log4j configuration file, was changed to `log4j.configurationFile`.

For example

```text
-Dlog4j.configurationFile=gateway-log4j2.xml
```

Instead of

```text
-Dlog4j.configuration=conf/gateway-log4j.properties
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration+file+changes"></a>Configuration file changes [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration+file+changes)

Log4j 2 uses a different configuration file format than Log4j 1.

```text
app.log.dir=logs
app.log.file=${launcher.name}.log
log4j.rootLogger=ERROR, drfa
log4j.appender.stdout=org.apache.log4j.ConsoleAppender
log4j.appender.stdout.layout=org.apache.log4j.PatternLayout
log4j.appender.stdout.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n
log4j.appender.drfa=org.apache.log4j.DailyRollingFileAppender
log4j.appender.drfa.File=${app.log.dir}/${app.log.file}
log4j.appender.drfa.DatePattern=.yyyy-MM-dd
log4j.appender.drfa.layout=org.apache.log4j.PatternLayout
log4j.appender.drfa.layout.ConversionPattern=%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n
log4j.logger.org.apache.http.impl.conn=INFO
log4j.logger.org.apache.http.impl.client=INFO
log4j.logger.org.apache.http.client=INFO
```

For example the equivalent of the above Log4j 1 config file looks like this:

```xml
<Configuration>
    <Properties>
        <Property name="app.log.dir">logs</Property>
        <Property name="app.log.file">${sys:launcher.name}.log</Property>
    </Properties>
    <Appenders>
        <Console name="stdout" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n" />
        </Console>
        <RollingFile name="drfa" fileName="${app.log.dir}/${app.log.file}" filePattern="${app.log.dir}/${app.log.file}.%d{yyyy-MM-dd}">
            <PatternLayout pattern="%d{ISO8601} %-5p %c{2} (%F:%M(%L)) - %m%n" />
            <TimeBasedTriggeringPolicy />
        </RollingFile>
    </Appenders>
    <Loggers>
        <Logger name="org.apache.http.impl.client" level="INFO" />
        <Logger name="org.apache.http.client" level="INFO" />
        <Logger name="org.apache.http.impl.conn" level="INFO" />
        <Root level="ERROR">
            <AppenderRef ref="drfa" />
        </Root>
    </Loggers>
</Configuration>
```

Log levels can be set on individual Java packages similarly as before. The Loggers inherit properties like logging level and appender types from their ancestors.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Log4j+Migration"></a>Log4j Migration [](#knox-apache-org-books-knox-2-1-0-user-guide--Log4j+Migration)

If you have an existing Knox installation which uses the default Logging settings, with no customizations, then you can simply upgrade to the new version and overwrite the `*-log4j.property` files with the `*-log4j2.xml` files.

Old Log4j 1.x property files:

```text
-rw-r--r--  1 user  group  3880 Sep 10 10:49 gateway-log4j.properties
-rw-r--r--  1 user  group  1481 Sep 10 10:49 knoxcli-log4j.properties
-rw-r--r--  1 user  group  1493 Sep 10 10:49 ldap-log4j.properties
-rw-r--r--  1 user  group  1436 Sep 10 10:49 shell-log4j.properties
```

The new Log4j 2 XML configuration files:

```text
-rw-r--r--  1 user  group  4619 Jan 22  2020 gateway-log4j2.xml
-rw-r--r--  1 user  group  1684 Jan 22  2020 knoxcli-log4j2.xml
-rw-r--r--  1 user  group  1765 Jan 22  2020 ldap-log4j2.xml
-rw-r--r--  1 user  group  1621 Jan 22  2020 shell-log4j2.xml
```

If you have a lot of customizations in place, you will need to convert the property files to XML file format.

There is a third party script that helps you with the conversion:

```text
https://github.com/mulesoft-labs/log4j2-migrator
```

The final result is not always 100% correct, you might need to do some manual adjustments.

Usage:

```bash
$ groovy log4j2migrator.groovy log4j.properties > log4j2.xml
```

The scripts uses â€‹â€‹AsyncLoggers by default which requires the com.lmax:disruptor library which is not distributed with Knox by default.

To avoid having this dependency you can replace all the `AsyncLogger` tags to `Logger`s.

```bash
 $ groovy log4j2migrator.groovy log4j.properties > log4j2.xml | sed 's/AsyncLogger/Logger/g'
```

Pay attention to the custom date time patterns in the configuration file. The script doesn’t always convert them properly.

You can find more information about the Log4j 2 configuration file format at here: [https://logging.apache.org/log4j/2.x/manual/configuration.html#XML](https://logging.apache.org/log4j/2.x/manual/configuration.html#XML)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Custom+Appenders+and+Layouts"></a>Custom Appenders and Layouts [](#knox-apache-org-books-knox-2-1-0-user-guide--Custom+Appenders+and+Layouts)

Custom Appenders or Layouts based on the Log4j 1 API, are not going to work with Log4j 2. Those need to be rewritten using the new API.

The `AppenderSkeleton` class does not exist in Log4j 2, you should extend from `AbstractAppender` instead.

You can read more about extending Log4j 2 at: [https://logging.apache.org/log4j/2.x/manual/extending.html](https://logging.apache.org/log4j/2.x/manual/extending.html)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Java+VM+Options"></a>Java VM Options [](#knox-apache-org-books-knox-2-1-0-user-guide--Java+VM+Options)

TODO - Java VM options doc.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Persisting+the+Master+Secret"></a>Persisting the Master Secret [](#knox-apache-org-books-knox-2-1-0-user-guide--Persisting+the+Master+Secret)

The master secret is required to start the server. This secret is used to access secured artifacts by the gateway instance. By default, the keystores, trust stores, and credential stores are all protected with the master secret. However, if a custom keystore is set, it and the contained keys may have different passwords.

You may persist the master secret by supplying the *-persist-master* switch at startup. This will result in a warning indicating that persisting the secret is less secure than providing it at startup. We do make some provisions in order to protect the persisted password.

It is encrypted with AES 128 bit encryption and where possible the file permissions are set to only be accessible by the user that the gateway is running as.

After persisting the secret, ensure that the file at `data/security/master` has the appropriate permissions set for your environment. This is probably the most important layer of defense for master secret. Do not assume that the encryption is sufficient protection.

A specific user should be created to run the gateway. This user will be the only user with permissions for the persisted master file.

See the Knox CLI section for descriptions of the command line utilities related to the master secret.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Management+of+Security+Artifacts"></a>Management of Security Artifacts [](#knox-apache-org-books-knox-2-1-0-user-guide--Management+of+Security+Artifacts)

There are a number of artifacts that are used by the gateway in ensuring the security of wire level communications, access to protected resources and the encryption of sensitive data. These artifacts can be managed from outside of the gateway instances or generated and populated by the gateway instance itself.

The following is a description of how this is coordinated with both standalone (development, demo, etc.) gateway instances and instances as part of a cluster of gateways in mind.

Upon start of the gateway server:

1. Look for a credential store at `data/security/keystores/__gateway-credentials.jceks`. This credential store is used to store secrets/passwords that are used by the gateway. For instance, this is where the passphrase for accessing the gateway’s TLS certificate is kept.
   - If the credential store is not found, then one is created and encrypted with the provided master secret.
   - If the credential store is found, then it is loaded using the provided master secret.
2. Look for an identity keystore at the configured location. If a configured location was not set, the default location, `data/security/keystores/gateway.jks`, will be used. The identity keystore contains the certificate and private key used to represent the identity of the server for TLS/SSL connections.
   - If the identity keystore was not found, one is created in the configured or default location.  
      Then a self-signed certificate for use in standalone/demo mode is generated and stored with the configured identity alias or the default alias of “gateway-identity”.
   - If the identity keystore is found, then it is loaded using either the password found in the credential store under the configured alias name or the provided master secret. It is tested to ensure that a certificate and private key are found using the configured alias name or the default alias of “gateway-identity”.
3. Look for a signing keystore at the configured location or the default location of `data/security/keystores/gateway.jks`.  
    The signing keystore contains the public and private key used for signature creation.
   - If the signing keystore was not found, the server will fail to start.
   - If the signing keystore is found, then it is loaded using either the password found in the credential store under the configured alias name or the provided master secret. It is tested to ensure that a public and private key are found using the configured alias name or the default alias of “gateway-identity”.

Upon deployment of a Hadoop cluster topology within the gateway we:

1. Look for a credential store for the topology. For instance, we have a sample topology that gets deployed out of the box. We look for `data/security/keystores/sandbox-credentials.jceks`. This topology specific credential store is used for storing secrets/passwords that are used for encrypting sensitive data with topology specific keys.
   - If no credential store is found for the topology being deployed then one is created for it. Population of the aliases is delegated to the configured providers within the system that will require the use of a secret for a particular task. They may programmatically set the value of the secret or choose to have the value for the specified alias generated through the AliasService.
   - If a credential store is found then we ensure that it can be loaded with the provided master secret and the configured providers have the opportunity to ensure that the aliases are populated and if not to populate them.

By leveraging the algorithm described above we can provide a window of opportunity for management of these artifacts in a number of ways.

1. Using a single gateway instance as a master instance the artifacts can be generated or placed into the expected location and then replicated across all of the slave instances before startup.
2. Using an NFS mount as a central location for the artifacts would provide a single source of truth without the need to replicate them over the network. Of course, NFS mounts have their own challenges.
3. Using the KnoxCLI to create and manage the security artifacts.

See the Knox CLI section for descriptions of the command line utilities related to the security artifact management.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Keystores"></a>Keystores [](#knox-apache-org-books-knox-2-1-0-user-guide--Keystores)

In order to provide your own certificate for use by the Gateway, you may either

- provide your own keystore
- import an existing key pair into the default Gateway keystore
- generate a self-signed cert using the Java keytool

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Providing+your+own+keystore"></a>Providing your own keystore [](#knox-apache-org-books-knox-2-1-0-user-guide--Providing+your+own+keystore)

A keystore in one of the following formats may be specified:

- JKS: Java KeyStore file
- JCEKS: Java Cryptology Extension KeyStore file
- PKCS12: PKCS #12 file

See `gateway.tls.keystore.password.alias`, `gateway.tls.keystore.path`, `gateway.tls.keystore.type`, `gateway.tls.key.alias`, and `gateway.tls.key.passphrase.alias` under *Gateway Server Configuration* for information on configuring the Gateway to use this keystore.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Importing+a+key+pair+into+a+Java+keystore"></a>Importing a key pair into a Java keystore [](#knox-apache-org-books-knox-2-1-0-user-guide--Importing+a+key+pair+into+a+Java+keystore)

One way to accomplish this is to start with a PKCS12 store for your key pair and then convert it to a Java keystore or JKS.

The following example uses OpenSSL to create a PKCS12 encoded store from your provided certificate and private key that are in PEM format.

```bash
openssl pkcs12 -export -in cert.pem -inkey key.pem > server.p12
```

The next example converts the PKCS12 store into a Java keystore (JKS). It should prompt you for the keystore and key passwords for the destination keystore. You must use either the master-secret for the keystore password and key passphrase or choose you own. If you choose your own passwords, you must use the Knox CLI utility to provide them to the Gateway.

```text
keytool -importkeystore -srckeystore server.p12 -destkeystore gateway.jks -srcstoretype pkcs12
```

While using this approach a couple of important things to be aware of:

1. The alias MUST be properly set. If it is not the default value (“gateway-identity”), it must be set in the configuration using `gateway.tls.key.alias`. You may need to change it using keytool after the import of the PKCS12 store. You can use keytool to do this - for example:

   ```text
   keytool -changealias -alias "1" -destalias "gateway-identity" -keystore gateway.jks -storepass {knoxpw}
   ```
2. The path to the identity keystore for the gateway MUST be the default path (`{GATEWAY_HOME}/data/security/keystores/gateway.jks`) or MUST be specified in the configuration using `gateway.tls.keystore.path` and `gateway.tls.keystore.type`
3. The passwords for the keystore and the imported key may both be set to the master secret for the gateway install. If a custom password is used, the passwords must be imported into the credential store using the Knox CLI `create-alias` command. The aliases for the passwords must then be set in the configuration using `gateway.tls.keystore.password.alias` and `gateway.tls.key.passphrase.alias`.  
    If both the keystore password and the key passphrase are the same, only `gateway.tls.keystore.password.alias` needs to be set. You can change the key passphrase after import using keytool. You may need to do this in order to provision the password in the credential store as described later in this section. For example:

   ```text
   keytool -keypasswd -alias gateway-identity -keystore gateway.jks
   ```

The following will allow you to provision the password for the keystore or the passphrase for the private key that was set during keystore creation above - it will prompt you for the actual password/passphrase.

```text
bin/knoxcli.sh create-alias <alias name>
```

The default alias for the keystore password is `gateway-identity-keystore-password`, to use a different alias, set `gateway.tls.keystore.password.alias` in the configuration. The default alias for the key passphrase is `gateway-identity-keystore-password`, to use a different alias, set `gateway.tls.key.passphrase.alias` in the configuration.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Generating+a+self-signed+cert+for+use+in+testing+or+development+environments"></a>Generating a self-signed cert for use in testing or development environments [](#knox-apache-org-books-knox-2-1-0-user-guide--Generating+a+self-signed+cert+for+use+in+testing+or+development+environments)

```text
keytool -genkey -keyalg RSA -alias gateway-identity -keystore gateway.jks \
    -storepass {master-secret} -validity 360 -keysize 2048
```

Keytool will prompt you for a number of elements used will comprise the distinguished name (DN) within your certificate.

*NOTE:* When it prompts you for your First and Last name be sure to type in the hostname of the machine that your gateway instance will be running on. This is used by clients during hostname verification to ensure that the presented certificate matches the hostname that was used in the URL for the connection - so they need to match.

*NOTE:* When it prompts for the key password just press enter to ensure that it is the same as the keystore password. Which, as was described earlier, must match the master secret for the gateway instance. Alternatively, you can set it to another passphrase - take note of it and set the gateway-identity-passphrase alias to that passphrase using the Knox CLI.

See the Knox CLI section for descriptions of the command line utilities related to the management of the keystores.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Using+a+CA+Signed+Key+Pair"></a>Using a CA Signed Key Pair [](#knox-apache-org-books-knox-2-1-0-user-guide--Using+a+CA+Signed+Key+Pair)

For certain deployments a certificate key pair that is signed by a trusted certificate authority is required. There are a number of different ways in which these certificates are acquired and can be converted and imported into the Apache Knox keystore.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Option+#1"></a>Option #1 [](#knox-apache-org-books-knox-2-1-0-user-guide--Option+#1)

One way to do this is to install the certificate and keys in the default identity keystore and the master secret. The following steps have been used to do this and are provided here for guidance in your installation. You may have to adjust according to your environment.

1. Stop Knox gateway and back up all files in `{GATEWAY_HOME}/data/security/keystores`

   ```text
   gateway.sh stop
   ```
2. Create a new master key for Knox and persist it. The master key will be referred to in following steps as `$master-key`

   ```text
   knoxcli.sh create-master -force
   ```
3. Create identity keystore gateway.jks. cert in alias gateway-identity

   ```bash
   cd {GATEWAY_HOME}/data/security/keystore  
   keytool -genkeypair -alias gateway-identity -keyalg RSA -keysize 1024 -dname "CN=$fqdn_knox,OU=hdp,O=sdge" -keypass $keypass -keystore gateway.jks -storepass $master-key -validity 300  
   ```

   NOTE: `$fqdn_knox` is the hostname of the Knox host. Some may choose `$keypass` to be the same as `$master-key`.
4. Create credential store to store the `$keypass` in step 3. This creates `__gateway-credentials.jceks` file

   ```text
   knoxcli.sh create-alias gateway-identity-passphrase --value $keypass
   ```
5. Generate a certificate signing request from the gateway.jks

   ```text
   keytool -keystore gateway.jks -storepass $master-key -alias gateway-identity -certreq -file knox.csr
   ```
6. Send the `knox.csr` file to the CA authority and get back the signed certificate (`knox.signed`). You also need the CA certificate, which normally can be requested through an openssl command or web browser or from the CA.
7. Import both the CA authority certificate (referred as `corporateCA.cer`) and the signed Knox certificate back into `gateway.jks`

   ```text
   keytool -keystore gateway.jks -storepass $master-key -alias $hwhq -import -file corporateCA.cer  
   keytool -keystore gateway.jks -storepass $master-key -alias gateway-identity -import -file knox.signed  
   ```

   NOTE: Use any alias appropriate for the corporate CA.
8. Restart Knox gateway. Check `gateway.log` to check whether the gateway started properly and clusters are deployed. You can check the timestamp on cluster deployment files

   ```text
   gateway.sh start
   ls -alrt {GATEWAY_HOME}/data/deployment
   ```
9. Verify that clients can use the CA authority cert to access Knox (which is the goal of using public signed cert) using curl or a web browser which has the CA certificate installed

   ```bash
   curl --cacert PATH_TO_CA_CERT -u tom:tom-password -X GET https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY
   ```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Option+#2"></a>Option #2 [](#knox-apache-org-books-knox-2-1-0-user-guide--Option+#2)

Another way to do this is to place the CA signed keypair in it’s own keystore. The creation of this keystore is out of scope for this document. However, once created, the following steps can be use to configure the Knox Gateway to use it.

1. Move the new keystore into a location that the Knox Gateway can access.
2. Edit the gateway-site.xml file to set the configurations

   - `gateway.tls.keystore.password.alias` - Alias of the password for the keystore
   - `gateway.tls.keystore.path` - Path to the keystore file
   - `gateway.tls.keystore.type` - Type of keystore file (JKS, JCEKS, PKCS12)
   - `gateway.tls.key.alias` - Alias for the certificate and key
   - `gateway.tls.key.passphrase.alias` - Alias of the passphrase for the key (needed if the passphrase is different than the keystore password)
3. Provision the relevant passwords using the Knox CLI

   ```text
   knoxcli.sh create-alias <alias> --value <password/passphrase>
   ```
4. Stop Knox gateway

   ```text
   gateway.sh stop
   ```
5. Restart Knox gateway. Check `gateway.log` to check whether the gateway started properly and clusters are deployed. You can check the timestamp on cluster deployment files

   ```text
   gateway.sh start
   ls -alrt {GATEWAY_HOME}/data/deployment
   ```
6. Verify that clients can use the CA authority cert to access Knox (which is the goal of using public signed cert) using curl or a web browser which has the CA certificate installed

   ```bash
   curl --cacert PATH_TO_CA_CERT -u tom:tom-password -X GET https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY
   ```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Credential+Store"></a>Credential Store [](#knox-apache-org-books-knox-2-1-0-user-guide--Credential+Store)

Whenever you provide your own keystore with either a self-signed cert or an issued certificate signed by a trusted authority, you will need to set an alias for the keystore password and key passphrase. This is necessary for the current release in order for the system to determine the correct passwords to use for the keystore and the key.

The credential stores in Knox use the JCEKS keystore type as it allows for the storage of general secrets in addition to certificates.

Keytool may be used to create credential stores but the Knox CLI section details how to create aliases. These aliases are managed within credential stores which are created by the CLI as needed. The simplest approach is to create the relevant aliases with the Knox CLI. This will create the credential store if it does not already exist and add the password or passphrase.

See the Knox CLI section for descriptions of the command line utilities related to the management of the credential stores.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Provisioning+of+Keystores"></a>Provisioning of Keystores [](#knox-apache-org-books-knox-2-1-0-user-guide--Provisioning+of+Keystores)

Once you have created these keystores you must move them into place for the gateway to discover them and use them to represent its identity for SSL connections. This is done by copying the keystores to the `{GATEWAY_HOME}/data/security/keystores` directory for your gateway install.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Summary+of+Secrets+to+be+Managed"></a>Summary of Secrets to be Managed [](#knox-apache-org-books-knox-2-1-0-user-guide--Summary+of+Secrets+to+be+Managed)

1. Master secret - the same for all gateway instances in a cluster of gateways
2. All security related artifacts are protected with the master secret
3. Secrets used by the gateway itself are stored within the gateway credential store and are the same across all gateway instances in the cluster of gateways
4. Secrets used by providers within cluster topologies are stored in topology specific credential stores and are the same for the same topology across the cluster of gateway instances. However, they are specific to the topology - so secrets for one Hadoop cluster are different from those of another. This allows for fail-over from one gateway instance to another even when encryption is being used while not allowing the compromise of one encryption key to expose the data for all clusters.

NOTE: the SSL certificate will need special consideration depending on the type of certificate. Wildcard certs may be able to be shared across all gateway instances in a cluster. When certs are dedicated to specific machines the gateway identity store will not be able to be blindly replicated as host name verification problems will ensue. Obviously, trust-stores will need to be taken into account as well.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Knox+CLI"></a>Knox CLI [](#knox-apache-org-books-knox-2-1-0-user-guide--Knox+CLI)

The Knox CLI is a command line utility for the management of various aspects of the Knox deployment. It is primarily concerned with the management of the security artifacts for the gateway instance and each of the deployed topologies or Hadoop clusters that are gated by the Knox Gateway instance.

The various security artifacts are also generated and populated automatically by the Knox Gateway runtime when they are not found at startup. The assumptions made in those cases are appropriate for a test or development gateway instance and assume ‘localhost’ for hostname specific activities. For production deployments the use of the CLI may aid in managing some production deployments.

The `knoxcli.sh` script is located in the `{GATEWAY_HOME}/bin` directory.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Help"></a>Help [](#knox-apache-org-books-knox-2-1-0-user-guide--Help)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+[--help]`"></a>`bin/knoxcli.sh [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+[--help]`)

prints help for all commands

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Knox+Version+Info"></a>Knox Version Info [](#knox-apache-org-books-knox-2-1-0-user-guide--Knox+Version+Info)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+version+[--help]`"></a>`bin/knoxcli.sh version [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+version+[--help]`)

Displays Knox version information.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Master+secret+persistence"></a>Master secret persistence [](#knox-apache-org-books-knox-2-1-0-user-guide--Master+secret+persistence)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+create-master+[--force]+[--master+mastersecret]+[--generate]`"></a>`bin/knoxcli.sh create-master [--force] [--master mastersecret] [--generate]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+create-master+[--force]+[--master+mastersecret]+[--generate]`)

The create-master command persists the master secret in a file located at: `{GATEWAY_HOME}/data/security/master`.

It will prompt the user for the secret to persist.

Use `--force` to overwrite the master secret.

Use `--master` to pass in a master secret to persist. This can be used to persist the secret without any user interaction. Be careful as the secret might appear in shell histories or process listings.  
 Instead of `--master` it is usually a better idea to use `--generate` instead!

Use `--generate` to have Knox automatically generate a random secret. The generated secret will not be printed or otherwise exposed.

Do not specify both `--master` and `--generate` at the same time.

NOTE: This command fails when there is an existing master file in the expected location. You may force it to overwrite the master file with the --force switch. NOTE: this will require you to change passwords protecting the keystores for the gateway identity keystores and all credential stores.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Alias+creation"></a>Alias creation [](#knox-apache-org-books-knox-2-1-0-user-guide--Alias+creation)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+create-alias+name+[--cluster+c]+[--value+v]+[--generate]+[--help]`"></a>`bin/knoxcli.sh create-alias name [--cluster c] [--value v] [--generate] [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+create-alias+name+[--cluster+c]+[--value+v]+[--generate]+[--help]`)

Creates a password alias and stores it in a credential store within the `{GATEWAY_HOME}/data/security/keystores` dir.

| Argument | Description |
| --- | --- |
| name | Name of the alias to create |
| --cluster | Name of Hadoop cluster for the cluster specific credential store otherwise assumes that it is for the gateway itself |
| --value | Parameter for specifying the actual password otherwise prompted. Escape complex passwords or surround with single quotes |
| --generate | Boolean flag to indicate whether the tool should just generate the value. This assumes that --value is not set - will result in error otherwise. User will not be prompted for the value when --generate is set. |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Batch+alias+creation"></a>Batch alias creation [](#knox-apache-org-books-knox-2-1-0-user-guide--Batch+alias+creation)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+create-aliases+--alias+alias1+[--value+value1]+--alias+alias2+[--value+value2]+--alias+aliasN+[--value+valueN]+...+[--cluster+clustername]+[--generate]`"></a>`bin/knoxcli.sh create-aliases --alias alias1 [--value value1] --alias alias2 [--value value2] --alias aliasN [--value valueN] ... [--cluster clustername] [--generate]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+create-aliases+--alias+alias1+[--value+value1]+--alias+alias2+[--value+value2]+--alias+aliasN+[--value+valueN]+...+[--cluster+clustername]+[--generate]`)

Creates multiple password aliases and stores them in a credential store within the `{GATEWAY_HOME}/data/security/keystores` dir.

| Argument | Description |
| --- | --- |
| --alias | Name of an alias to create. |
| --value | Parameter for specifying the actual password otherwise prompted. Escape complex passwords or surround with single quotes. |
| --generate | Boolean flag to indicate whether the tool should just generate the value. This assumes that --value is not set - will result in error otherwise. User will not be prompted for the value when --generate is set. |
| --cluster | Name of Hadoop cluster for the cluster specific credential store otherwise assumes that it is for the gateway itself |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Alias+deletion"></a>Alias deletion [](#knox-apache-org-books-knox-2-1-0-user-guide--Alias+deletion)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+delete-alias+name+[--cluster+c]+[--help]`"></a>`bin/knoxcli.sh delete-alias name [--cluster c] [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+delete-alias+name+[--cluster+c]+[--help]`)

Deletes a password and alias mapping from a credential store within `{GATEWAY_HOME}/data/security/keystores`.

| Argument | Description |
| --- | --- |
| name | Name of the alias to delete |
| --cluster | Name of Hadoop cluster for the cluster specific credential store otherwise assumes ’__gateway’ |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Alias+listing"></a>Alias listing [](#knox-apache-org-books-knox-2-1-0-user-guide--Alias+listing)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+list-alias+[--cluster+c]+[--help]`"></a>`bin/knoxcli.sh list-alias [--cluster c] [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+list-alias+[--cluster+c]+[--help]`)

Lists the alias names for the credential store within `{GATEWAY_HOME}/data/security/keystores`.

NOTE: This command will list the aliases in lowercase which is a result of the underlying credential store implementation. Lookup of credentials is a case insensitive operation - so this is not an issue.

| Argument | Description |
| --- | --- |
| --cluster | Name of Hadoop cluster for the cluster specific credential store otherwise assumes ’__gateway’ |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Self-signed+cert+creation"></a>Self-signed cert creation [](#knox-apache-org-books-knox-2-1-0-user-guide--Self-signed+cert+creation)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+create-cert+[--hostname+n]+[--help]`"></a>`bin/knoxcli.sh create-cert [--hostname n] [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+create-cert+[--hostname+n]+[--help]`)

Creates and stores a self-signed certificate to represent the identity of the gateway instance. This is stored within the `{GATEWAY_HOME}/data/security/keystores/gateway.jks` keystore.

| Argument | Description |
| --- | --- |
| --hostname | Name of the host to be used in the self-signed certificate. This allows multi-host deployments to specify the proper hostnames for hostname verification to succeed on the client side of the SSL connection. The default is ‘localhost’. |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Certificate+Export"></a>Certificate Export [](#knox-apache-org-books-knox-2-1-0-user-guide--Certificate+Export)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+export-cert+[--type+JKS|PEM|JCEKS|PKCS12]+[--help]`"></a>`bin/knoxcli.sh export-cert [--type JKS|PEM|JCEKS|PKCS12] [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+export-cert+[--type+JKS|PEM|JCEKS|PKCS12]+[--help]`)

The export-cert command exports the public certificate from the a gateway.jks keystore with the alias of gateway-identity. It will be exported to `{GATEWAY_HOME}/data/security/keystores/` with a name of `gateway-client-trust.<type>`. Using the `--type` option you can specify which keystore type you need (default: PEM)

**NOTE:** The password for the JKS, JCEKS and PKCS12 types is `changeit`. It can be changed using: `keytool -storepasswd -storetype <type> -keystore gateway-client-trust.<type>`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Topology+Redeploy"></a>Topology Redeploy [](#knox-apache-org-books-knox-2-1-0-user-guide--Topology+Redeploy)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+redeploy+[--cluster+c]`"></a>`bin/knoxcli.sh redeploy [--cluster c]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+redeploy+[--cluster+c]`)

Redeploys one or all of the gateway’s clusters (a.k.a topologies).

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Topology+Listing"></a>Topology Listing [](#knox-apache-org-books-knox-2-1-0-user-guide--Topology+Listing)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+list-topologies+[--help]`"></a>`bin/knoxcli.sh list-topologies [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+list-topologies+[--help]`)

Lists all of the topologies found in Knox’s topologies directory. Useful for specifying a valid –cluster argument.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Topology+Validation"></a>Topology Validation [](#knox-apache-org-books-knox-2-1-0-user-guide--Topology+Validation)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+validate-topology+[--cluster+c]+[--path+path]+[--help]`"></a>`bin/knoxcli.sh validate-topology [--cluster c] [--path path] [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+validate-topology+[--cluster+c]+[--path+path]+[--help]`)

This ensures that a cluster’s description (a.k.a. topology) follows the correct formatting rules. It is possible to specify a name of a cluster already in the topology directory, or a path to any file.

| Argument | Description |
| --- | --- |
| --cluster | Name of Hadoop cluster for which you want to validate |
| --path | Path to topology file that you wish to validate. |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Authentication+and+Authorization"></a>LDAP Authentication and Authorization [](#knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Authentication+and+Authorization)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+user-auth-test+[--cluster+c]+[--u+username]+[--p+password]+[--g]+[--d]+[--help]`"></a>`bin/knoxcli.sh user-auth-test [--cluster c] [--u username] [--p password] [--g] [--d] [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+user-auth-test+[--cluster+c]+[--u+username]+[--p+password]+[--g]+[--d]+[--help]`)

This command will test a topology’s ability to connect, authenticate, and authorize a user with an LDAP server. The only required argument is the –cluster argument to specify the name of the topology you wish to use. The topology must be valid (passes validate-topology command). If a `--u` and `--p` argument are not specified, the command line will prompt for a username and password. If authentication is successful then the command will attempt to use the topology to do an LDAP group lookup. The topology must be configured correctly to do this. If it is not, groups will not return and no errors will be printed unless the `--g` command is specified. Currently this command only works if a topology supports the use of ShiroProvider for authentication.

| Argument | Description |
| --- | --- |
| --cluster | Required; Name of cluster for which you want to test authentication |
| --u | Optional; Username you wish you authenticate with |
| --p | Optional; Password you wish to authenticate with |
| --g | Optional; Specify that you are looking to return a user’s groups. If not specified, group lookup errors won’t return |
| --d | Optional; Print extra debug info on failed authentication |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Topology+LDAP+Bind"></a>Topology LDAP Bind [](#knox-apache-org-books-knox-2-1-0-user-guide--Topology+LDAP+Bind)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+system-user-auth-test+[--cluster+c]+[--d]+[--help]`"></a>`bin/knoxcli.sh system-user-auth-test [--cluster c] [--d] [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+system-user-auth-test+[--cluster+c]+[--d]+[--help]`)

This command will test a given topology’s ability to connect, bind, and authenticate with the LDAP server from the settings specified in the topology file. The bind currently only will with Shiro as the authentication provider. There are also two parameters required inside of the topology for these

| Argument | Description |
| --- | --- |
| --cluster | Required; Name of cluster for which you want to test authentication |
| --d | Optional; Print extra debug info on failed authentication |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Service+Test"></a>Gateway Service Test [](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Service+Test)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+service-test+[--cluster+c]+[--hostname+hostname]+[--port+port]+[--u+username]+[--p+password]+[--d]+[--help]`"></a>`bin/knoxcli.sh service-test [--cluster c] [--hostname hostname] [--port port] [--u username] [--p password] [--d] [--help]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+service-test+[--cluster+c]+[--hostname+hostname]+[--port+port]+[--u+username]+[--p+password]+[--d]+[--help]`)

This will test a topology configuration’s ability to connect to multiple Hadoop services. Each service found in a topology will be tested with multiple URLs. Results are printed to the console in JSON format.

| Argument | Description |
| --- | --- |
| --cluster | Required; Name of cluster for which you want to test authentication |
| --hostname | Required; Hostname of the cluster currently running on the machine |
| --port | Optional; Port that the cluster is running on. If not supplied CLI will try to read config files to find the port. |
| --u | Required; Username to authorize against Hadoop services |
| --p | Required; Password to match username |
| --d | Optional; Print extra debug info on failed authentication |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Registry+Client+Listing"></a>Remote Configuration Registry Client Listing [](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Registry+Client+Listing)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+list-registry-clients`"></a>`bin/knoxcli.sh list-registry-clients` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+list-registry-clients`)

Lists the [remote configuration registry clients](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Registry+Clients) defined in ‘{GATEWAY\_HOME}/conf/gateway-site.xml’.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--List+Provider+Configurations+in+a+Remote+Configuration+Registry"></a>List Provider Configurations in a Remote Configuration Registry [](#knox-apache-org-books-knox-2-1-0-user-guide--List+Provider+Configurations+in+a+Remote+Configuration+Registry)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+list-provider-configs+--registry-client+name`"></a>`bin/knoxcli.sh list-provider-configs --registry-client name` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+list-provider-configs+--registry-client+name`)

List the provider configurations in the remote configuration registry for which the referenced client provides access.

| Argument | Description |
| --- | --- |
| --registry-client | Required; The name of aremote configuration registry client, as defined in gateway-site.xml |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--List+Descriptors+in+a+Remote+Configuration+Registry"></a>List Descriptors in a Remote Configuration Registry [](#knox-apache-org-books-knox-2-1-0-user-guide--List+Descriptors+in+a+Remote+Configuration+Registry)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+list-descriptors+--registry-client+name`"></a>`bin/knoxcli.sh list-descriptors --registry-client name` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+list-descriptors+--registry-client+name`)

List the descriptors in the remote configuration registry for which the referenced client provides access.

| Argument | Description |
| --- | --- |
| --registry-client | Required; The name of aremote configuration registry client, as defined in gateway-site.xml |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Upload+Provider+Configuration+to+a+Remote+Configuration+Registry"></a>Upload Provider Configuration to a Remote Configuration Registry [](#knox-apache-org-books-knox-2-1-0-user-guide--Upload+Provider+Configuration+to+a+Remote+Configuration+Registry)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+upload-provider-config+providerConfigFile+--registry-client+name+[--entry-name+entryName]`"></a>`bin/knoxcli.sh upload-provider-config providerConfigFile --registry-client name [--entry-name entryName]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+upload-provider-config+providerConfigFile+--registry-client+name+[--entry-name+entryName]`)

Upload a provider configuration file to the remote configuration registry for which the referenced client provides access. By default, the entry name will be the same as the uploaded file’s name.

| Argument | Description |
| --- | --- |
| --registry-client | Required; The name of aremote configuration registry client, as defined in gateway-site.xml |
| --entry-name | Optional; The name of the entry for the uploaded content in the registry. |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Upload+Descriptor+to+a+Remote+Configuration+Registry"></a>Upload Descriptor to a Remote Configuration Registry [](#knox-apache-org-books-knox-2-1-0-user-guide--Upload+Descriptor+to+a+Remote+Configuration+Registry)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+upload-descriptor+descriptorFile+--registry-client+name+[--entry-name+entryName]`"></a>`bin/knoxcli.sh upload-descriptor descriptorFile --registry-client name [--entry-name entryName]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+upload-descriptor+descriptorFile+--registry-client+name+[--entry-name+entryName]`)

Upload a descriptor file to the remote configuration registry for which the referenced client provides access. By default, the entry name will be the same as the uploaded file’s name.

| Argument | Description |
| --- | --- |
| --registry-client | Required; The name of aremote configuration registry client, as defined in gateway-site.xml |
| --entry-name | Optional; The name of the entry for the uploaded content in the registry. |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Delete+a+Provider+Configuration+From+a+Remote+Configuration+Registry"></a>Delete a Provider Configuration From a Remote Configuration Registry [](#knox-apache-org-books-knox-2-1-0-user-guide--Delete+a+Provider+Configuration+From+a+Remote+Configuration+Registry)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+delete-provider-config+providerConfig+--registry-client+name`"></a>`bin/knoxcli.sh delete-provider-config providerConfig --registry-client name` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+delete-provider-config+providerConfig+--registry-client+name`)

Delete a provider configuration from the remote configuration registry for which the referenced client provides access.

| Argument | Description |
| --- | --- |
| --registry-client | Required; The name of aremote configuration registry client, as defined in gateway-site.xml |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Delete+a+Descriptor+From+a+Remote+Configuration+Registry"></a>Delete a Descriptor From a Remote Configuration Registry [](#knox-apache-org-books-knox-2-1-0-user-guide--Delete+a+Descriptor+From+a+Remote+Configuration+Registry)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+delete-descriptor+descriptor+--registry-client+name`"></a>`bin/knoxcli.sh delete-descriptor descriptor --registry-client name` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+delete-descriptor+descriptor+--registry-client+name`)

Delete a descriptor from the remote configuration registry for which the referenced client provides access.

| Argument | Description |
| --- | --- |
| --registry-client | Required; The name of aremote configuration registry client, as defined in gateway-site.xml |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Get+the+ACL+For+an+Entry+in+a+Remote+Configuration+Registry"></a>Get the ACL For an Entry in a Remote Configuration Registry [](#knox-apache-org-books-knox-2-1-0-user-guide--Get+the+ACL+For+an+Entry+in+a+Remote+Configuration+Registry)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+get-registry-acl+entry+--registry-client+name`"></a>`bin/knoxcli.sh get-registry-acl entry --registry-client name` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+get-registry-acl+entry+--registry-client+name`)

List the ACL set for the specified entry in the remote configuration registry for which the referenced client provides access.

| Argument | Description |
| --- | --- |
| --registry-client | Required; The name of aremote configuration registry client, as defined in gateway-site.xml |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Convert+topology+file+to+provider+and+descriptor+config+files"></a>Convert topology file to provider and descriptor config files [](#knox-apache-org-books-knox-2-1-0-user-guide--Convert+topology+file+to+provider+and+descriptor+config+files)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+convert-topology+--path+path/to/topology.xml+--provider-name+my-prov.json+[--descriptor-name+my-desc.json]+`"></a>`bin/knoxcli.sh convert-topology --path path/to/topology.xml --provider-name my-prov.json [--descriptor-name my-desc.json]` [](#knox-apache-org-books-knox-2-1-0-user-guide--`bin/knoxcli.sh+convert-topology+--path+path/to/topology.xml+--provider-name+my-prov.json+[--descriptor-name+my-desc.json]+`)

Convert topology xml files to provider and descriptor config files.

| Argument | Description |
| --- | --- |
| --path | Required; Path to topology xml file. |
| --provider-name | Required; Name of the provider json config file (including .json extension). |
| --descriptor-name | Optional; Name of descriptor json config file (including .json extension). |
| --topology-name | Optional; topology-name can be use instead of --path option, if used, KnoxCLI will attempt to find topology from deployed topologies directory. If not provided, topology name will be used as descriptor name |
| --output-path | Optional; Output directory to save provider and descriptor config files if not provided, config files will be saved in appropriate Knox config directory. |
| --force | Optional; Force rewriting of existing files, if not used, command will fail when the config files with same name already exist. |
| --cluster | Optional; Cluster name, required for service discovery. |
| --discovery-url | Optional; Service discovery URL, required for service discovery. |
| --discovery-user | Optional; Service discovery user, required for service discovery. |
| --discovery-pwd-alias | Optional; Password alias for service discovery user, required for service discovery. |
| --discovery-type | Optional; Service discovery type, required for service discovery. |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Admin+API"></a>Admin API [](#knox-apache-org-books-knox-2-1-0-user-guide--Admin+API)

Access to the administrator functions of Knox are provided by the Admin REST API.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Admin+API+URL"></a>Admin API URL [](#knox-apache-org-books-knox-2-1-0-user-guide--Admin+API+URL)

The URL mapping for the Knox Admin API is:

| GatewayAPI | https://{gateway-host}:{gateway-port}/{gateway-path}/admin/api/v1 |
| --- | --- |

Please note that to access this API, the user attempting to connect must have admin credentials configured on the LDAP Server

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--API+Documentation"></a>API Documentation [](#knox-apache-org-books-knox-2-1-0-user-guide--API+Documentation)

| Resource | Operation | Description |
| --- | --- | --- |
| version | GET | Get the gateway version and the associated version hash |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/version -H Accept:application/json |
|  | Example Response | {   "ServerVersion" : {     "version" : "VERSION_ID",     "hash" : "VERSION_HASH"   } } |
| topologies | GET | Get an enumeration of the topologies currently deployed in the gateway. |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/topologies -H Accept:application/json |
|  | Example Response | {    "topologies" : {       "topology" : [ {          "name" : "admin",          "timestamp" : "1501508536000",          "uri" : "https://localhost:8443/gateway/admin",          "href" : "https://localhost:8443/gateway/admin/api/v1/topologies/admin"       }, {          "name" : "sandbox",          "timestamp" : "1501508536000",          "uri" : "https://localhost:8443/gateway/sandbox",          "href" : "https://localhost:8443/gateway/admin/api/v1/topologies/sandbox"       } ]    } } |
| topologies/{id} | GET | Get a JSON representation of the specified topology |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/topologies/admin -H Accept:application/json |
|  | Example Response | {   "name": "admin",   "providers": [{     "enabled": true,     "name": "ShiroProvider",     "params": {       "sessionTimeout": "30",       "main.ldapRealm": "org.apache.knox.gateway.shirorealm.KnoxLdapRealm",       "main.ldapRealm.userDnTemplate": "uid={0},ou=people,dc=hadoop,dc=apache,dc=org",       "main.ldapRealm.contextFactory.url": "ldap://localhost:33389",       "main.ldapRealm.contextFactory.authenticationMechanism": "simple",       "urls./**": "authcBasic"     },     "role": "authentication"   }, {     "enabled": true,     "name": "AclsAuthz",     "params": {       "knox.acl": "admin;*;*"     },     "role": "authorization"   }, {     "enabled": true,     "name": "Default",     "params": {},     "role": "identity-assertion"   }, {     "enabled": true,     "name": "static",     "params": {       "localhost": "sandbox,sandbox.hortonworks.com"     },     "role": "hostmap"   }],   "services": [{       "name": null,       "params": {},       "role": "KNOX",       "url": null   }],   "timestamp": 1406672646000,   "uri": "https://localhost:8443/gateway/admin" } |
|  | PUT | Add (and deploy) a topology |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/topologies/mytopology \      -X PUT \      -H Content-Type:application/xml      -d "@mytopology.xml" |
|  | Example Response | <?xml version="1.0" encoding="UTF-8"?> <topology>    <uri>https://localhost:8443/gateway/mytopology</uri>    <name>mytopology</name>    <timestamp>1509720338000</timestamp>    <gateway>       <provider>          <role>authentication</role>          <name>ShiroProvider</name>          <enabled>true</enabled>          <param>             <name>sessionTimeout</name>             <value>30</value>          </param>          <param>             <name>main.ldapRealm</name>             <value>org.apache.knox.gateway.shirorealm.KnoxLdapRealm</value>          </param>          <param>             <name>main.ldapContextFactory</name>             <value>org.apache.knox.gateway.shirorealm.KnoxLdapContextFactory</value>          </param>          <param>             <name>main.ldapRealm.contextFactory</name>             <value>$ldapContextFactory</value>          </param>          <param>             <name>main.ldapRealm.userDnTemplate</name>             <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>          </param>          <param>             <name>main.ldapRealm.contextFactory.url</name>             <value>ldap://localhost:33389</value>          </param>          <param>             <name>main.ldapRealm.contextFactory.authenticationMechanism</name>             <value>simple</value>          </param>          <param>             <name>urls./**</name>             <value>authcBasic</value>          </param>       </provider>       <provider>          <role>identity-assertion</role>          <name>Default</name>          <enabled>true</enabled>       </provider>       <provider>          <role>hostmap</role>          <name>static</name>          <enabled>true</enabled>          <param>             <name>localhost</name>             <value>sandbox,sandbox.hortonworks.com</value>          </param>       </provider>    </gateway>    <service>       <role>NAMENODE</role>       <url>hdfs://localhost:8020</url>    </service>    <service>       <role>JOBTRACKER</role>       <url>rpc://localhost:8050</url>    </service>    <service>       <role>WEBHDFS</role>       <url>http://localhost:50070/webhdfs</url>    </service>    <service>       <role>WEBHCAT</role>       <url>http://localhost:50111/templeton</url>    </service>    <service>       <role>OOZIE</role>       <url>http://localhost:11000/oozie</url>    </service>    <service>       <role>WEBHBASE</role>       <url>http://localhost:60080</url>    </service>    <service>       <role>HIVE</role>       <url>http://localhost:10001/cliservice</url>    </service>    <service>       <role>RESOURCEMANAGER</role>       <url>http://localhost:8088/ws</url>    </service> </topology> |
|  | DELETE | Delete (and undeploy) a topology |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/topologies/mytopology -X DELETE |
|  | Example Response | { "deleted" : true } |
| providerconfig | GET | Get an enumeration of the shared provider configurations currently deployed to the gateway. |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/providerconfig |
|  | Example Response | {   "href" : "https://localhost:8443/gateway/admin/api/v1/providerconfig",   "items" : [ {     "href" : "https://localhost:8443/gateway/admin/api/v1/providerconfig/myproviders",     "name" : "myproviders.xml"   },{    "href" : "https://localhost:8443/gateway/admin/api/v1/providerconfig/sandbox-providers",    "name" : "sandbox-providers.xml"   } ] } |
| providerconfig/{id} | GET | Get the XML content of the specified shared provider configuration. |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/providerconfig/sandbox-providers \      -H Accept:application/xml |
|  | Example Response | <gateway>     <provider>         <role>authentication</role>         <name>ShiroProvider</name>         <enabled>true</enabled>         <param>             <name>sessionTimeout</name>             <value>30</value>         </param>         <param>             <name>main.ldapRealm</name>             <value>org.apache.knox.gateway.shirorealm.KnoxLdapRealm</value>         </param>         <param>             <name>main.ldapContextFactory</name>             <value>org.apache.knox.gateway.shirorealm.KnoxLdapContextFactory</value>         </param>         <param>             <name>main.ldapRealm.contextFactory</name>             <value>$ldapContextFactory</value>         </param>         <param>             <name>main.ldapRealm.userDnTemplate</name>             <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>         </param>         <param>             <name>main.ldapRealm.contextFactory.url</name>             <value>ldap://localhost:33389</value>         </param>         <param>             <name>main.ldapRealm.contextFactory.authenticationMechanism</name>             <value>simple</value>         </param>         <param>             <name>urls./**</name>             <value>authcBasic</value>         </param>     </provider>      <provider>         <role>identity-assertion</role>         <name>Default</name>         <enabled>true</enabled>     </provider>      <provider>         <role>hostmap</role>         <name>static</name>         <enabled>true</enabled>         <param>             <name>localhost</name>             <value>sandbox,sandbox.hortonworks.com</value>         </param>     </provider> </gateway> |
|  | PUT | Add a shared provider configuration. |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/providerconfig/sandbox-providers \      -X PUT \       -H Content-Type:application/xml \      -d "@sandbox-providers.xml" |
|  | Example Response | HTTP 201 Created |
|  | DELETE | Delete a shared provider configuration |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/providerconfig/sandbox-providers -X DELETE |
|  | Example Response | { "deleted" : "provider config sandbox-providers" } |
| descriptors | GET | Get an enumeration of the simple descriptors currently deployed to the gateway. |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/descriptors -H Accept:application/json |
|  | Example Response | {    "href" : "https://localhost:8443/gateway/admin/api/v1/descriptors",    "items" : [ {       "href" : "https://localhost:8443/gateway/admin/api/v1/descriptors/docker-sandbox",       "name" : "docker-sandbox.json"    }, {       "href" : "https://localhost:8443/gateway/admin/api/v1/descriptors/mytopology",       "name" : "mytopology.yml"    } ] } |
| descriptors/{id} | GET | Get the content of the specified descriptor. |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/descriptors/docker-sandbox \      -H Accept:application/json |
|  | Example Response | {   "discovery-type":"AMBARI",   "discovery-address":"http://sandbox.hortonworks.com:8080",   "provider-config-ref":"sandbox-providers",   "cluster":"Sandbox",   "services":[     {"name":"NAMENODE"},     {"name":"JOBTRACKER"},     {"name":"WEBHDFS"},     {"name":"WEBHCAT"},     {"name":"OOZIE"},     {"name":"WEBHBASE"},     {"name":"HIVE"},     {"name":"RESOURCEMANAGER"} ] } |
|  | PUT | Add a simple descriptor (and generate and deploy a full topology descriptor). |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/descriptors/docker-sandbox \      -X PUT \      -H Content-Type:application/json \      -d "@docker-sandbox.json" |
|  | Example Response | HTTP 201 Created |
|  | DELETE | Delete a simple descriptor (and undeploy the associated topology) |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/descriptors/docker-sandbox -X DELETE |
|  | Example Response | { "deleted" : "descriptor docker-sandbox" } |
| aliases/{topology} | GET | Get the aliases associated with the specified topology. |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/aliases/sandbox |
|  | Example Response | {   "topology":"sandbox",   "aliases":["myalias","encryptquerystring"] } |
| aliases/{topology}/{alias} | PUT | Add the specified alias for the specified topology. |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/aliases/sandbox/putalias -X PUT \      -H "Content-Type: application/json" \      -d "value=mysecret" |
|  | Example Response | {   "created" : {     "topology": "sandbox",     "alias": "putalias"   } } |
|  | POST | Add the specified alias for the specified topology. |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/aliases/sandbox/postalias -X POST \      -H "Content-Type: application/json" \      -d "value=mysecret" |
|  | Example Response | {   "created" : {     "topology": "sandbox",     "alias": "postalias"   } } |
|  | DELETE | Remove the specified alias for the specified topology. |
|  | Example Request | curl -iku admin:admin-password {GatewayAPI}/aliases/sandbox/myalias -X DELETE |
|  | Example Response | {   "deleted" : {     "topology": "sandbox",     "alias": "myalias"   } } |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-*+Headers+Support"></a>X-Forwarded-\* Headers Support [](#knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-*+Headers+Support)

Out-of-the-box Knox provides support for some `X-Forwarded-*` headers through the use of a Servlet Filter. Specifically the headers handled/populated by Knox are:

- X-Forwarded-For
- X-Forwarded-Proto
- X-Forwarded-Port
- X-Forwarded-Host
- X-Forwarded-Server
- X-Forwarded-Context

This functionality can be turned off by a configuration setting in the file gateway-site.xml and redeploying the necessary topology/topologies.

The setting is (under the ‘configuration’ tag) :

```xml
<property>
    <name>gateway.xforwarded.enabled</name>
    <value>false</value>
</property>
```

If this setting is absent, the default behavior is that the `X-Forwarded-*` header support is on or in other words, `gateway.xforwarded.enabled` is set to `true` by default.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Header+population"></a>Header population [](#knox-apache-org-books-knox-2-1-0-user-guide--Header+population)

The following are the various rules for population of these headers:

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-For"></a>X-Forwarded-For [](#knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-For)

This header represents a list of client IP addresses. If the header is already present Knox adds a comma separated value to the list. The value added is the client’s IP address as Knox sees it. This value is added to the end of the list.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-Proto"></a>X-Forwarded-Proto [](#knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-Proto)

The protocol used in the client request. If this header is passed into Knox its value is maintained, otherwise Knox will populate the header with the value ‘https’ if the request is a secure one or ‘http’ otherwise.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-Port"></a>X-Forwarded-Port [](#knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-Port)

The port used in the client request. If this header is passed into Knox its value is maintained, otherwise Knox will populate the header with the value of the port that the request was made coming into Knox.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-Host"></a>X-Forwarded-Host [](#knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-Host)

Represents the original host requested by the client in the Host HTTP request header. The value passed into Knox is maintained by Knox. If no value is present, Knox populates the header with the value of the HTTP Host header.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-Server"></a>X-Forwarded-Server [](#knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-Server)

The hostname of the server Knox is running on.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-Context"></a>X-Forwarded-Context [](#knox-apache-org-books-knox-2-1-0-user-guide--X-Forwarded-Context)

This header value contains the context path of the request to Knox.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Metrics"></a>Metrics [](#knox-apache-org-books-knox-2-1-0-user-guide--Metrics)

See the KIP for details on the implementation of metrics available in the gateway.

[Metrics KIP](https://cwiki.apache.org/confluence/display/KNOX/KIP-2+Metrics)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Metrics+Configuration"></a>Metrics Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Metrics+Configuration)

Metrics configuration can be done in `gateway-site.xml`.

The initial configuration is mainly for turning on or off the metrics collection and then enabling reporters with their required config.

The two initial reporters implemented are JMX and Graphite.

```text
gateway.metrics.enabled 
```

Turns on or off the metrics, default is ‘true’

```text
gateway.jmx.metrics.reporting.enabled
```

Turns on or off the jmx reporter, default is ‘true’

```text
gateway.graphite.metrics.reporting.enabled
```

Turns on or off the graphite reporter, default is ‘false’

```text
gateway.graphite.metrics.reporting.host
gateway.graphite.metrics.reporting.port
gateway.graphite.metrics.reporting.frequency
```

The above are the host, port and frequency of reporting (in seconds) parameters for the graphite reporter.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Authentication"></a>Authentication [](#knox-apache-org-books-knox-2-1-0-user-guide--Authentication)

There are two types of providers supported in Knox for establishing a user’s identity:

1. Authentication Providers
2. Federation Providers

Authentication providers directly accept a user’s credentials and validates them against some particular user store. Federation providers, on the other hand, validate a token that has been issued for the user by a trusted Identity Provider (IdP).

The current release of Knox ships with an authentication provider based on the Apache Shiro project and is initially configured for BASIC authentication against an LDAP store. This has been specifically tested against Apache Directory Server and Active Directory.

This section will cover the general approach to leveraging Shiro within the bundled provider including:

1. General mapping of provider config to `shiro.ini` config
2. Specific configuration for the bundled BASIC/LDAP configuration
3. Some tips into what may need to be customized for your environment
4. How to setup the use of LDAP over SSL or LDAPS

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--General+Configuration+for+Shiro+Provider"></a>General Configuration for Shiro Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--General+Configuration+for+Shiro+Provider)

As is described in the configuration section of this document, providers have a name-value based configuration - as is the common pattern in the rest of Hadoop.

The following example shows the format of the configuration for a given provider:

```xml
<provider>
    <role>authentication</role>
    <name>ShiroProvider</name>
    <enabled>true</enabled>
    <param>
        <name>{name}</name>
        <value>{value}</value>
    </param>
</provider>
```

Conversely, the Shiro provider currently expects a `shiro.ini` file in the `WEB-INF` directory of the cluster specific web application.

The following example illustrates a configuration of the bundled BASIC/LDAP authentication config in a `shiro.ini` file:

```ini
[urls]
/**=authcBasic
[main]
ldapRealm=org.apache.shiro.realm.ldap.JndiLdapRealm
ldapRealm.contextFactory.authenticationMechanism=simple
ldapRealm.contextFactory.url=ldap://localhost:33389
ldapRealm.userDnTemplate=uid={0},ou=people,dc=hadoop,dc=apache,dc=org
```

In order to fit into the context of an INI file format, at deployment time we interrogate the parameters provided in the provider configuration and parse the INI section out of the parameter names. The following provider config illustrates this approach. Notice that the section names in the above shiro.ini match the beginning of the parameter names that are in the following config:

```xml
<gateway>
    <provider>
        <role>authentication</role>
        <name>ShiroProvider</name>
        <enabled>true</enabled>
        <param>
            <name>main.ldapRealm</name>
            <value>org.apache.shiro.realm.ldap.JndiLdapRealm</value>
        </param>
        <param>
            <name>main.ldapRealm.userDnTemplate</name>
            <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
        </param>
        <param>
            <name>main.ldapRealm.contextFactory.url</name>
            <value>ldap://localhost:33389</value>
        </param>
        <param>
            <name>main.ldapRealm.contextFactory.authenticationMechanism</name>
            <value>simple</value>
        </param>
        <param>
            <name>urls./**</name>
            <value>authcBasic</value>
        </param>
    </provider>
```

This happens to be the way that we are currently configuring Shiro for BASIC/LDAP authentication. This same config approach may be used to achieve other authentication mechanisms or variations on this one. We however have not tested additional uses for it for this release.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Configuration"></a>LDAP Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Configuration)

This section discusses the LDAP configuration used above for the Shiro Provider. Some of these configuration elements will need to be customized to reflect your deployment environment.

**main.ldapRealm** - this element indicates the fully qualified class name of the Shiro realm to be used in authenticating the user. The class name provided by default in the sample is the `org.apache.shiro.realm.ldap.JndiLdapRealm` this implementation provides us with the ability to authenticate but by default has authorization disabled. In order to provide authorization - which is seen by Shiro as dependent on an LDAP schema that is specific to each organization - an extension of JndiLdapRealm is generally used to override and implement the doGetAuthorizationInfo method. In this particular release we are providing a simple authorization provider that can be used along with the Shiro authentication provider.

**main.ldapRealm.userDnTemplate** - in order to bind a simple username to an LDAP server that generally requires a full distinguished name (DN), we must provide the template into which the simple username will be inserted. This template allows for the creation of a DN by injecting the simple username into the common name (CN) portion of the DN. **This element will need to be customized to reflect your deployment environment.** The template provided in the sample is only an example and is valid only within the LDAP schema distributed with Knox and is represented by the `users.ldif` file in the `{GATEWAY_HOME}/conf` directory.

**main.ldapRealm.contextFactory.url** - this element is the URL that represents the host and port of the LDAP server. It also includes the scheme of the protocol to use. This may be either `ldap` or `ldaps` depending on whether you are communicating with the LDAP over SSL (highly recommended). **This element will need to be customized to reflect your deployment environment.**.

**main.ldapRealm.contextFactory.authenticationMechanism** - this element indicates the type of authentication that should be performed against the LDAP server. The current default value is `simple` which indicates a simple bind operation. This element should not need to be modified and no mechanism other than a simple bind has been tested for this particular release.

**urls./**\*\* - this element represents a single URL\_Ant\_Path\_Expression and the value the Shiro filter chain to apply to it. This particular sample indicates that all paths into the application have the same Shiro filter chain applied. The paths are relative to the application context path. The use of the value `authcBasic` here indicates that BASIC authentication is expected for every path into the application. Adding an additional Shiro filter to that chain for validating that the request isSecure() and over SSL can be achieved by changing the value to `ssl, authcBasic`. This parameter can be used to exclude endpoints from authentication, this is important in case of jwks endpoints which need not require authentication. We have support for unauthenticated paths in other authenitcation providers and this support can be extended here using the `urls` parameter. Following is an example of how `/knoxtoken/api/v1/jwks.json` endpoint can be excluded from authentication in shiro configuration.

```xml
    <param>
        <name>urls./knoxtoken/api/v1/jwks.json</name>
        <value>anon</value>
    </param>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Active+Directory+-+Special+Note"></a>Active Directory - Special Note [](#knox-apache-org-books-knox-2-1-0-user-guide--Active+Directory+-+Special+Note)

You would use LDAP configuration as documented above to authenticate against Active Directory as well.

Some Active Directory specific things to keep in mind:

Typical AD `main.ldapRealm.userDnTemplate` value looks slightly different, such as

```text
cn={0},cn=users,DC=lab,DC=sample,dc=com
```

Please compare this with a typical Apache DS `main.ldapRealm.userDnTemplate` value and make note of the difference:

```text
`uid={0},ou=people,dc=hadoop,dc=apache,dc=org`
```

If your AD is configured to authenticate based on just the cn and password and does not require user DN, you do not have to specify value for `main.ldapRealm.userDnTemplate`.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--LDAP+over+SSL+(LDAPS)+Configuration"></a>LDAP over SSL (LDAPS) Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--LDAP+over+SSL+(LDAPS)+Configuration)

In order to communicate with your LDAP server over SSL (again, highly recommended), you will need to modify the topology file in a couple ways and possibly provision some keying material.

1. **main.ldapRealm.contextFactory.url** must be changed to have the `ldaps` protocol scheme and the port must be the SSL listener port on your LDAP server.
2. Identity certificate (keypair) provisioned to LDAP server - your LDAP server specific documentation should indicate what is required for providing a cert or keypair to represent the LDAP server identity to connecting clients.
3. Trusting the LDAP Server’s public key - if the LDAP Server’s identity certificate is issued by a well known and trusted certificate authority and is already represented in the JRE’s cacerts truststore then you don’t need to do anything for trusting the LDAP server’s cert. If, however, the cert is self-signed or issued by an untrusted authority you will need to either add it to the cacerts keystore or to another truststore that you may direct Knox to utilize through a system property (`javax.net.ssl.trustStore` and `javax.net.ssl.trustStorePassword`).

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Session+Configuration"></a>Session Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Session+Configuration)

Knox maps each cluster topology to a web application and leverages standard JavaEE session management.

To configure session idle timeout for the topology, please specify value of parameter sessionTimeout for ShiroProvider in your topology file. If you do not specify the value for this parameter, it defaults to 30 minutes.

The definition would look like the following in the topology file:

```text
...
<provider>
    <role>authentication</role>
    <name>ShiroProvider</name>
    <enabled>true</enabled>
    <param>
        <!--
        Session timeout in minutes. This is really idle timeout.
        Defaults to 30 minutes, if the property value is not defined.
        Current client authentication will expire if client idles
        continuously for more than this value
        -->
        <name>sessionTimeout</name>
        <value>30</value>
    </param>
<provider>
...
```

At present, ShiroProvider in Knox leverages JavaEE session to maintain authentication state for a user across requests using JSESSIONID cookie. So, a client that authenticated with Knox could pass the JSESSIONID cookie with repeated requests as long as the session has not timed out instead of submitting userid/password with every request. Presenting a valid session cookie in place of userid/password would also perform better as additional credential store lookups are avoided.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Advanced+LDAP+Authentication"></a>Advanced LDAP Authentication [](#knox-apache-org-books-knox-2-1-0-user-guide--Advanced+LDAP+Authentication)

The default configuration computes the bind DN for incoming user based on userDnTemplate. This does not work in enterprises where users could belong to multiple branches of LDAP tree. You could instead enable advanced configuration that would compute bind DN of incoming user with an LDAP search.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Problem+with+userDnTemplate+based+Authentication"></a>Problem with userDnTemplate based Authentication [](#knox-apache-org-books-knox-2-1-0-user-guide--Problem+with+userDnTemplate+based+Authentication)

UserDnTemplate based authentication uses the configuration parameter `ldapRealm.userDnTemplate`. Typical values of userDNTemplate would look like `uid={0},ou=people,dc=hadoop,dc=apache,dc=org`.

To compute bind DN of the client, we swap the place holder `{0}` with the login id provided by the client. For example, if the login id provided by the client is "guest’,  
the computed bind DN would be `uid=guest,ou=people,dc=hadoop,dc=apache,dc=org`.

This keeps configuration simple.

However, this does not work if users belong to different branches of LDAP DIT. For example, if there are some users under `ou=people,dc=hadoop,dc=apache,dc=org` and some users under `ou=contractors,dc=hadoop,dc=apache,dc=org`,  
we cannot come up with userDnTemplate that would work for all the users.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Using+advanced+LDAP+Authentication"></a>Using advanced LDAP Authentication [](#knox-apache-org-books-knox-2-1-0-user-guide--Using+advanced+LDAP+Authentication)

With advanced LDAP authentication, we find the bind DN of the user by searching the LDAP directory instead of interpolating the bind DN from userDNTemplate.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Example+search+filter+to+find+the+client+bind+DN"></a>Example search filter to find the client bind DN [](#knox-apache-org-books-knox-2-1-0-user-guide--Example+search+filter+to+find+the+client+bind+DN)

Assuming

- ldapRealm.userSearchAttributeName=uid
- ldapRealm.userObjectClass=person
- client specified login id = “guest”

The LDAP Filter for doing a search to find the bind DN would be

```text
(&(uid=guest)(objectclass=person))
```

This could find the bind DN to be

```properties
uid=guest,ou=people,dc=hadoop,dc=apache,dc=org
```

Please note that the `userSearchAttributeName` need not be part of bindDN.

For example, you could use

- ldapRealm.userSearchAttributeName=email
- ldapRealm.userObjectClass=person
- client specified login id = “[bill.clinton@gmail.com](mailto:bill.clinton@gmail.com)”

The LDAP Filter for doing a search to find the bind DN would be

```text
(&(email=bill.clinton@gmail.com)(objectclass=person))
```

This could find the bind DN to be

```properties
uid=billc,ou=contractors,dc=hadoop,dc=apache,dc=org
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Advanced+LDAP+configuration+parameters"></a>Advanced LDAP configuration parameters [](#knox-apache-org-books-knox-2-1-0-user-guide--Advanced+LDAP+configuration+parameters)

The table below provides a brief description and sample of the available advanced bind and search configuration parameters.

| Parameter | Description | Default | Sample |
| --- | --- | --- | --- |
| principalRegex | Parses the principal for insertion into templates via regex. | (.*) | (.*?)\\(.*)(e.g. match US\tom: {0}=US\tom, {1}=US, {2}=tom) |
| userDnTemplate | Direct user bind DN template. | {0} | cn={2},dc={1},dc=qa,dc=company,dc=com |
| userSearchBase | Search based template. Used with config below. | none | dc={1},dc=qa,dc=company,dc=com |
| userSearchAttributeName | Attribute name for simplified search filter. | none | sAMAccountName |
| userSearchAttributeTemplate | Attribute template for simplified search filter. | {0} | {2} |
| userSearchFilter | Advanced search filter template. Note & is &amp; in XML. | none | (&amp;(objectclass=person)(sAMAccountName={2})) |
| userSearchScope | Search scope: subtree, onelevel, object. | subtree | onelevel |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Advanced+LDAP+configuration+combinations"></a>Advanced LDAP configuration combinations [](#knox-apache-org-books-knox-2-1-0-user-guide--Advanced+LDAP+configuration+combinations)

There are also only certain valid combinations of advanced LDAP configuration parameters.

- User DN Template
  - userDnTemplate (Required)
  - principalRegex (Optional)
- User Search by Attribute
  - userSearchBase (Required)
  - userAttributeName (Required)
  - userAttributeTemplate (Optional)
  - userSearchScope (Optional)
  - principalRegex (Optional)
- User Search by Filter
  - userSearchBase (Required)
  - userSearchFilter (Required)
  - userSearchScope (Optional)
  - principalRegex (Optional)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Advanced+LDAP+configuration+precedence"></a>Advanced LDAP configuration precedence [](#knox-apache-org-books-knox-2-1-0-user-guide--Advanced+LDAP+configuration+precedence)

The presence of multiple configuration combinations should be avoided. The rules below clarify which combinations take precedence when present.

1. userSearchBase takes precedence over userDnTemplate
2. userSearchFilter takes precedence over userSearchAttributeName

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Example+provider+configuration+to+use+advanced+LDAP+authentication"></a>Example provider configuration to use advanced LDAP authentication [](#knox-apache-org-books-knox-2-1-0-user-guide--Example+provider+configuration+to+use+advanced+LDAP+authentication)

The example configuration appears verbose due to the presence of liberal comments and illustration of optional parameters and default values. The configuration that you would use could be much shorter if you rely on default values.

```xml
<provider>

    <role>authentication</role>
    <name>ShiroProvider</name>
    <enabled>true</enabled>

    <param>
        <name>main.ldapRealm</name>
        <value>org.apache.knox.gateway.shirorealm.KnoxLdapRealm</value>
    </param>

    <param>
        <name>main.ldapContextFactory</name>
        <value>org.apache.knox.gateway.shirorealm.KnoxLdapContextFactory</value>
    </param>

    <param>
        <name>main.ldapRealm.contextFactory</name>
        <value>$ldapContextFactory</value>
    </param>

    <!-- update the value based on your ldap directory protocol, host and port -->
    <param>
        <name>main.ldapRealm.contextFactory.url</name>
        <value>ldap://hdp.example.com:389</value>
    </param>

    <!-- optional, default value: simple
         Update the value based on mechanisms supported by your ldap directory -->
    <param>
        <name>main.ldapRealm.contextFactory.authenticationMechanism</name>
        <value>simple</value>
    </param>

    <!-- optional, default value: {0}
         update the value based on your ldap DIT(directory information tree).
         ignored if value is defined for main.ldapRealm.userSearchAttributeName -->
    <param>
        <name>main.ldapRealm.userDnTemplate</name>
        <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
    </param>

    <!-- optional, default value: null
         If you specify a value for this attribute, useDnTemplate 
         specified above would be ignored and user bind DN would be computed using
         ldap search
         update the value based on your ldap DIT(directory information layout)
         value of search attribute should identity the user uniquely -->
    <param>
        <name>main.ldapRealm.userSearchAttributeName</name>
        <value>uid</value>
    </param>

    <!-- optional, default value: false  
         If the value is true, groups in which user is a member are looked up 
         from LDAP and made available  for service level authorization checks -->
    <param>
        <name>main.ldapRealm.authorizationEnabled</name>
        <value>true</value>
    </param>

    <!-- bind DN used to search for groups and user bind DN.  
         Required if a value is defined for main.ldapRealm.userSearchAttributeName
         or if the value of main.ldapRealm.authorizationEnabled is true -->
    <param>
        <name>main.ldapRealm.contextFactory.systemUsername</name>
        <value>uid=guest,ou=people,dc=hadoop,dc=apache,dc=org</value>
    </param>

    <!-- password for systemUserName.
         Required if a value is defined for main.ldapRealm.userSearchAttributeName
         or if the value of main.ldapRealm.authorizationEnabled is true -->
    <param>
        <name>main.ldapRealm.contextFactory.systemPassword</name>
        <value>${ALIAS=ldcSystemPassword}</value>
    </param>

    <!-- optional, default value: simple
         Update the value based on mechanisms supported by your ldap directory -->
    <param>
        <name>main.ldapRealm.contextFactory.systemAuthenticationMechanism</name>
        <value>simple</value>
    </param>

    <!-- optional, default value: person
         Objectclass to identify user entries in ldap, used to build search 
         filter to search for user bind DN -->
    <param>
        <name>main.ldapRealm.userObjectClass</name>
        <value>person</value>
    </param>

    <!-- search base used to search for user bind DN and groups -->
    <param>
        <name>main.ldapRealm.searchBase</name>
        <value>dc=hadoop,dc=apache,dc=org</value>
    </param>

    <!-- search base used to search for user bind DN.
         Defaults to the value of main.ldapRealm.searchBase. 
         If main.ldapRealm.userSearchAttributeName is defined, 
         value for main.ldapRealm.searchBase  or main.ldapRealm.userSearchBase 
         should be defined -->
    <param>
        <name>main.ldapRealm.userSearchBase</name>
        <value>dc=hadoop,dc=apache,dc=org</value>
    </param>

    <!-- search base used to search for groups.
         Defaults to the value of main.ldapRealm.searchBase.
         If value of main.ldapRealm.authorizationEnabled is true,
         value for main.ldapRealm.searchBase  or main.ldapRealm.groupSearchBase should be defined -->
    <param>
        <name>main.ldapRealm.groupSearchBase</name>
        <value>dc=hadoop,dc=apache,dc=org</value>
    </param>

    <!-- optional, default value: groupOfNames
         Objectclass to identify group entries in ldap, used to build search 
         filter to search for group entries --> 
    <param>
        <name>main.ldapRealm.groupObjectClass</name>
        <value>groupOfNames</value>
    </param>

    <!-- optional, default value: member
         If value is memberUrl, we treat found groups as dynamic groups -->
    <param>
        <name>main.ldapRealm.memberAttribute</name>
        <value>member</value>
    </param>

    <!-- optional, default value: uid={0}
         Ignored if value is defined for main.ldapRealm.userSearchAttributeName -->
    <param>
        <name>main.ldapRealm.memberAttributeValueTemplate</name>
        <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
    </param>

    <!-- optional, default value: cn -->
    <param>
        <name>main.ldapRealm.groupIdAttribute</name>
        <value>cn</value>
    </param>

    <param>
        <name>urls./**</name>
        <value>authcBasic</value>
    </param>

    <!-- optional, default value: 30min -->
    <param>
        <name>sessionTimeout</name>
        <value>30</value>
    </param>

</provider>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Special+note+on+parameter+main.ldapRealm.contextFactory.systemPassword"></a>Special note on parameter main.ldapRealm.contextFactory.systemPassword [](#knox-apache-org-books-knox-2-1-0-user-guide--Special+note+on+parameter+main.ldapRealm.contextFactory.systemPassword)

The value for this could have one of the following 2 formats

- plaintextpassword
- ${ALIAS=ldcSystemPassword}

The first format specifies the password in plain text in the provider configuration. Use of this format should be limited for testing and troubleshooting.

We strongly recommend using the second format `${ALIAS=ldcSystemPassword}` in production. This format uses an alias for the password stored in credential store. In the example `${ALIAS=ldcSystemPassword}`, ldcSystemPassword is the alias for the password stored in credential store.

Assuming the plain text password is “hadoop”, and your topology file name is “hdp.xml”, you would use following command to create the right password alias in credential store.

```text
{GATEWAY_HOME}/bin/knoxcli.sh  create-alias ldcSystemPassword --cluster hdp --value hadoop
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Authentication+Caching"></a>LDAP Authentication Caching [](#knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Authentication+Caching)

Knox can be configured to cache LDAP authentication information. Knox leverages Shiro’s built in caching mechanisms and has been tested with Shiro’s EhCache cache manager implementation.

The following provider snippet demonstrates how to configure turning on the cache using the ShiroProvider. In addition to using `org.apache.knox.gateway.shirorealm.KnoxLdapRealm` in the Shiro configuration, and setting up the cache you *must* set the flag for enabling caching authentication to true. Please see the property, `main.ldapRealm.authenticationCachingEnabled` below. If caching is enabled on more than one topology, advanced caching configs with differing persistence directories have to be provided. The reason for this is separate topologies manage their own caches in different directories. Two cache has to be in different places otherwise ehcache won’t be able to lock the directory and the topology deployment will fail.

```xml
<provider>
    <role>authentication</role>
    <name>ShiroProvider</name>
    <enabled>true</enabled>
    <param>
        <name>main.ldapRealm</name>
        <value>org.apache.knox.gateway.shirorealm.KnoxLdapRealm</value>
    </param>
    <param>
        <name>main.ldapGroupContextFactory</name>
        <value>org.apache.knox.gateway.shirorealm.KnoxLdapContextFactory</value>
    </param>
    <param>
        <name>main.ldapRealm.contextFactory</name>
        <value>$ldapGroupContextFactory</value>
    </param>
    <param>
        <name>main.ldapRealm.contextFactory.url</name>
        <value>ldap://localhost:33389</value>
    </param>
    <param>
        <name>main.ldapRealm.userDnTemplate</name>
        <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
    </param>
    <param>
        <name>main.ldapRealm.authorizationEnabled</name>
        <!-- defaults to: false -->
        <value>true</value>
    </param>
    <param>
        <name>main.ldapRealm.searchBase</name>
        <value>ou=groups,dc=hadoop,dc=apache,dc=org</value>
    </param>
    <param>
        <name>main.cacheManager</name>
        <value>org.apache.knox.gateway.shirorealm.KnoxCacheManager</value>
    </param>
    <param>
        <name>main.securityManager.cacheManager</name>
        <value>$cacheManager</value>
    </param>
    <param>
        <name>main.ldapRealm.authenticationCachingEnabled</name>
        <value>true</value>
    </param>
    <param>
        <name>main.ldapRealm.memberAttributeValueTemplate</name>
        <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
    </param>
    <param>
        <name>main.ldapRealm.contextFactory.systemUsername</name>
        <value>uid=guest,ou=people,dc=hadoop,dc=apache,dc=org</value>
    </param>
    <param>
        <name>main.ldapRealm.contextFactory.systemPassword</name>
        <value>guest-password</value>
    </param>
    <param>
        <name>urls./**</name>
        <value>authcBasic</value>
    </param>
</provider>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Trying+out+caching"></a>Trying out caching [](#knox-apache-org-books-knox-2-1-0-user-guide--Trying+out+caching)

Knox bundles a template topology files that can be used to try out the caching functionality. The template file located under `{GATEWAY_HOME}/templates` is `sandbox.knoxrealm.ehcache.xml`.

To try this out

```bash
cd {GATEWAY_HOME}
cp templates/sandbox.knoxrealm.ehcache.xml conf/topologies/sandbox.xml
bin/ldap.sh start
bin/gateway.sh start
```

The following call to WebHDFS should report: `{"Path":"/user/tom"}`

```bash
curl  -i -v  -k -u tom:tom-password  -X GET https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY
```

In order to see the cache working, LDAP can now be shutdown and the user will still authenticate successfully.

```text
bin/ldap.sh stop
```

and then the following should still return successfully like it did earlier.

```bash
curl  -i -v  -k -u tom:tom-password  -X GET https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Advanced+Caching+Config"></a>Advanced Caching Config [](#knox-apache-org-books-knox-2-1-0-user-guide--Advanced+Caching+Config)

By default the EhCache support in Shiro contains the ehcache.xml in its classpath which is the following

```xml
<config xmlns="http://www.ehcache.org/v3">

    <persistence directory="${java.io.tmpdir}/shiro-ehcache"/>

    <cache alias="shiro-activeSessionCache">
        <key-type serializer="org.ehcache.impl.serialization.CompactJavaSerializer">
            java.lang.Object
        </key-type>
        <value-type serializer="org.ehcache.impl.serialization.CompactJavaSerializer">
            java.lang.Object
        </value-type>

        <resources>
            <heap unit="entries">10000</heap>
            <disk unit="GB">1</disk>
        </resources>
    </cache>

    <cache alias="org.apache.shiro.realm.text.PropertiesRealm-0-accounts">
        <key-type serializer="org.ehcache.impl.serialization.CompactJavaSerializer">
            java.lang.Object
        </key-type>
        <value-type serializer="org.ehcache.impl.serialization.CompactJavaSerializer">
            java.lang.Object
        </value-type>

        <resources>
            <heap unit="entries">1000</heap>
            <disk unit="GB">1</disk>
        </resources>
    </cache>

    <cache-template name="defaultCacheConfiguration">
        <expiry>
            <tti unit="seconds">120</tti>
        </expiry>
        <heap unit="entries">10000</heap>
    </cache-template>

</config>
```

A custom configuration file (ehcache.xml) can be used in place of this in order to set specific caching configuration.

In order to set the ehcache.xml file to use for a particular topology, set the following parameter in the configuration for the ShiroProvider:

```xml
<param>
    <name>main.cacheManager.cacheManagerConfigFile</name>
    <value>classpath:ehcache.xml</value>
</param>
```

In the above example, place the ehcache.xml file under `{GATEWAY_HOME}/conf` and restart the gateway server.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Group+Lookup"></a>LDAP Group Lookup [](#knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Group+Lookup)

Knox can be configured to look up LDAP groups that the authenticated user belong to. Knox can look up both Static LDAP Groups and Dynamic LDAP Groups. The looked up groups are populated as Principal(s) in the Java Subject of the authenticated user. Therefore service authorization rules can be defined in terms of LDAP groups looked up from a LDAP directory.

To look up LDAP groups of authenticated user from LDAP, you have to use `org.apache.knox.gateway.shirorealm.KnoxLdapRealm` in Shiro configuration.

Please see below a sample Shiro configuration snippet from a topology file that was tested looking LDAP groups.

```xml
<provider>
    <role>authentication</role>
    <name>ShiroProvider</name>
    <enabled>true</enabled>
    <!-- 
    session timeout in minutes,  this is really idle timeout,
    defaults to 30mins, if the property value is not defined,, 
    current client authentication would expire if client idles continuously for more than this value
    -->
    <!-- defaults to: 30 minutes
    <param>
        <name>sessionTimeout</name>
        <value>30</value>
    </param>
    -->

    <!--
      Use single KnoxLdapRealm to do authentication and ldap group look up
    -->
    <param>
        <name>main.ldapRealm</name>
        <value>org.apache.knox.gateway.shirorealm.KnoxLdapRealm</value>
    </param>
    <param>
        <name>main.ldapGroupContextFactory</name>
        <value>org.apache.knox.gateway.shirorealm.KnoxLdapContextFactory</value>
    </param>
    <param>
        <name>main.ldapRealm.contextFactory</name>
        <value>$ldapGroupContextFactory</value>
    </param>
    <!-- defaults to: simple
    <param>
        <name>main.ldapRealm.contextFactory.authenticationMechanism</name>
        <value>simple</value>
    </param>
    -->
    <param>
        <name>main.ldapRealm.contextFactory.url</name>
        <value>ldap://localhost:33389</value>
    </param>
    <param>
        <name>main.ldapRealm.userDnTemplate</name>
        <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
    </param>

    <param>
        <name>main.ldapRealm.authorizationEnabled</name>
        <!-- defaults to: false -->
        <value>true</value>
    </param>
    <!-- defaults to: simple
    <param>
        <name>main.ldapRealm.contextFactory.systemAuthenticationMechanism</name>
        <value>simple</value>
    </param>
    -->
    <param>
        <name>main.ldapRealm.searchBase</name>
        <value>ou=groups,dc=hadoop,dc=apache,dc=org</value>
    </param>
    <!-- defaults to: groupOfNames
    <param>
        <name>main.ldapRealm.groupObjectClass</name>
        <value>groupOfNames</value>
    </param>
    -->
    <!-- defaults to: member
    <param>
        <name>main.ldapRealm.memberAttribute</name>
        <value>member</value>
    </param>
    -->
    <param>
         <name>main.cacheManager</name>
         <value>org.apache.shiro.cache.MemoryConstrainedCacheManager</value>
    </param>
    <param>
        <name>main.securityManager.cacheManager</name>
        <value>$cacheManager</value>
    </param>
    <param>
        <name>main.ldapRealm.memberAttributeValueTemplate</name>
        <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
    </param>
    <!-- the above element is the template for most ldap servers 
        for active directory use the following instead and
        remove the above configuration.
    <param>
        <name>main.ldapRealm.memberAttributeValueTemplate</name>
        <value>cn={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
    </param>
    -->
    <param>
        <name>main.ldapRealm.contextFactory.systemUsername</name>
        <value>uid=guest,ou=people,dc=hadoop,dc=apache,dc=org</value>
    </param>
    <param>
        <name>main.ldapRealm.contextFactory.systemPassword</name>
        <value>${ALIAS=ldcSystemPassword}</value>
    </param>

    <param>
        <name>urls./**</name> 
        <value>authcBasic</value>
    </param>

</provider>
```

The configuration shown above would look up Static LDAP groups of the authenticated user and populate the group principals in the Java Subject corresponding to the authenticated user.

If you want to look up Dynamic LDAP Groups instead of Static LDAP Groups, you would have to specify groupObjectClass and memberAttribute params as shown below:

```xml
<param>
    <name>main.ldapRealm.groupObjectClass</name>
    <value>groupOfUrls</value>
</param>
<param>
    <name>main.ldapRealm.memberAttribute</name>
    <value>memberUrl</value>
</param>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Template+topology+files+and+LDIF+files+to+try+out+LDAP+Group+Look+up"></a>Template topology files and LDIF files to try out LDAP Group Look up [](#knox-apache-org-books-knox-2-1-0-user-guide--Template+topology+files+and+LDIF+files+to+try+out+LDAP+Group+Look+up)

Knox bundles some template topology files and ldif files that you can use to try and test LDAP Group Lookup and associated authorization ACLs. All these template files are located under `{GATEWAY_HOME}/templates`.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Static+Group+Lookup+Templates,+authentication+and+group+lookup+from+the+same+directory"></a>LDAP Static Group Lookup Templates, authentication and group lookup from the same directory [](#knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Static+Group+Lookup+Templates,+authentication+and+group+lookup+from+the+same+directory)

- topology file: sandbox.knoxrealm1.xml
- ldif file: users.ldapgroups.ldif

To try this out

```bash
cd {GATEWAY_HOME}
cp templates/sandbox.knoxrealm1.xml conf/topologies/sandbox.xml
cp templates/users.ldapgroups.ldif conf/users.ldif
java -jar bin/ldap.jar conf
java -Dsandbox.ldcSystemPassword=guest-password -jar bin/gateway.jar -persist-master
```

Following call to WebHDFS should report HTTP/1.1 401 Unauthorized As guest is not a member of group “analyst”, authorization provider states user should be member of group “analyst”

```bash
curl  -i -v  -k -u guest:guest-password  -X GET https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY
```

Following call to WebHDFS should report: {“Path”:“/user/sam”} As sam is a member of group “analyst”, authorization provider states user should be member of group “analyst”

```bash
curl  -i -v  -k -u sam:sam-password  -X GET https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Static+Group+Lookup+Templates,+authentication+and+group+lookup+from+different++directories"></a>LDAP Static Group Lookup Templates, authentication and group lookup from different directories [](#knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Static+Group+Lookup+Templates,+authentication+and+group+lookup+from+different++directories)

- topology file: sandbox.knoxrealm2.xml
- ldif file: users.ldapgroups.ldif

To try this out

```bash
cd {GATEWAY_HOME}
cp templates/sandbox.knoxrealm2.xml conf/topologies/sandbox.xml
cp templates/users.ldapgroups.ldif conf/users.ldif
java -jar bin/ldap.jar conf
java -Dsandbox.ldcSystemPassword=guest-password -jar bin/gateway.jar -persist-master
```

Following call to WebHDFS should report HTTP/1.1 401 Unauthorized As guest is not a member of group “analyst”, authorization provider states user should be member of group “analyst”

```bash
curl  -i -v  -k -u guest:guest-password  -X GET https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY
```

Following call to WebHDFS should report: {“Path”:“/user/sam”} As sam is a member of group “analyst”, authorization provider states user should be member of group “analyst”

```bash
curl  -i -v  -k -u sam:sam-password  -X GET https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Dynamic+Group+Lookup+Templates,+authentication+and+dynamic+group+lookup+from+same++directory"></a>LDAP Dynamic Group Lookup Templates, authentication and dynamic group lookup from same directory [](#knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Dynamic+Group+Lookup+Templates,+authentication+and+dynamic+group+lookup+from+same++directory)

- topology file: sandbox.knoxrealmdg.xml
- ldif file: users.ldapdynamicgroups.ldif

To try this out

```bash
cd {GATEWAY_HOME}
cp templates/sandbox.knoxrealmdg.xml conf/topologies/sandbox.xml
cp templates/users.ldapdynamicgroups.ldif conf/users.ldif
java -jar bin/ldap.jar conf
java -Dsandbox.ldcSystemPassword=guest-password -jar bin/gateway.jar -persist-master
```

Please note that user.ldapdynamicgroups.ldif also loads necessary schema to create dynamic groups in Apache DS.

Following call to WebHDFS should report HTTP/1.1 401 Unauthorized As guest is not a member of dynamic group “directors”, authorization provider states user should be member of group “directors”

```bash
curl  -i -v  -k -u guest:guest-password  -X GET https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY
```

Following call to WebHDFS should report: {“Path”:“/user/bob”} As bob is a member of dynamic group “directors”, authorization provider states user should be member of group “directors”

```bash
curl  -i -v  -k -u sam:sam-password  -X GET https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--PAM+based+Authentication"></a>PAM based Authentication [](#knox-apache-org-books-knox-2-1-0-user-guide--PAM+based+Authentication)

There is a large number of pluggable authentication modules available on many Linux installations and from vendors of authentication solutions that are great to leverage for authenticating access to Hadoop through the Knox Gateway. In addition to LDAP support described in this guide, the ShiroProvider also includes support for PAM based authentication for unix based systems.

This opens up the integration possibilities to many other readily available authentication mechanisms as well as other implementations for LDAP based authentication. More flexibility may be available through various PAM modules for group lookup, more complicated LDAP schemas or other areas where the KnoxLdapRealm is not sufficient.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Overview"></a>Overview [](#knox-apache-org-books-knox-2-1-0-user-guide--Overview)

The primary motivation for leveraging PAM based authentication is to provide the ability to use the configuration provided by existing PAM modules that are available in a system’s `/etc/pam.d/` directory. Therefore, the solution provided here is as simple as possible in order to allow the PAM module config itself to be the source of truth. What we do need to configure is the fact that we are using PAM through the `main.pamRealm` parameter and the KnoxPamRealm classname and the particular PAM module to use with the `main.pamRealm.service` parameter in the below example we have ‘login’.

```xml
<provider> 
   <role>authentication</role> 
   <name>ShiroProvider</name> 
   <enabled>true</enabled> 
   <param> 
        <name>sessionTimeout</name> 
        <value>30</value>
    </param>                                              
    <param>
        <name>main.pamRealm</name> 
        <value>org.apache.knox.gateway.shirorealm.KnoxPamRealm</value>
    </param> 
    <param>                                                    
       <name>main.pamRealm.service</name> 
       <value>login</value> 
    </param>
    <param>                                                    
       <name>urls./**</name> 
       <value>authcBasic</value> 
   </param>
</provider>
```

As a non-normative example of a PAM config file see the below from my MacBook `/etc/pam.d/login`:

```bash
# login: auth account password session
auth       optional       pam_krb5.so use_kcminit
auth       optional       pam_ntlm.so try_first_pass
auth       optional       pam_mount.so try_first_pass
auth       required       pam_opendirectory.so try_first_pass
account    required       pam_nologin.so
account    required       pam_opendirectory.so
password   required       pam_opendirectory.so
session    required       pam_launchd.so
session    required       pam_uwtmp.so
session    optional       pam_mount.so
```

The first four fields are: service-name, module-type, control-flag and module-filename. The fifth and greater fields are for optional arguments that are specific to the individual authentication modules.

The second field in the configuration file is the module-type, it indicates which of the four PAM management services the corresponding module will provide to the application. Our sample configuration file refers to all four groups:

- auth: identifies the PAMs that are invoked when the application calls pam\_authenticate() and pam\_setcred().
- account: maps to the pam\_acct\_mgmt() function.
- session: indicates the mapping for the pam\_open\_session() and pam\_close\_session() calls.
- password: group refers to the pam\_chauthtok() function.

Generally, you only need to supply mappings for the functions that are needed by a specific application. For example, the standard password changing application, passwd, only requires a password group entry; any other entries are ignored.

The third field indicates what action is to be taken based on the success or failure of the corresponding module. Choices for tokens to fill this field are:

- requisite: Failure instantly returns control to the application indicating the nature of the first module failure.
- required: All these modules are required to succeed for libpam to return success to the application.
- sufficient: Given that all preceding modules have succeeded, the success of this module leads to an immediate and successful return to the application (failure of this module is ignored).
- optional: The success or failure of this module is generally not recorded.

The fourth field contains the name of the loadable module, pam\_\*.so. For the sake of readability, the full pathname of each module is not given. Before Linux-PAM-0.56 was released, there was no support for a default authentication-module directory. If you have an earlier version of Linux-PAM installed, you will have to specify the full path for each of the modules. Your distribution most likely placed these modules exclusively in one of the following directories: /lib/security/ or /usr/lib/security/.

Also, find below a non-normative example of a PAM config file(/etc/pam.d/login) for Ubuntu:

```text
#%PAM-1.0

auth       required     pam_sepermit.so
# pam_selinux.so close should be the first session rule
session    required     pam_selinux.so close
session    required     pam_loginuid.so
# pam_selinux.so open should only be followed by sessions to be executed in the user context
session    required     pam_selinux.so open env_params
session    optional     pam_keyinit.so force revoke

session    required     pam_env.so user_readenv=1 envfile=/etc/default/locale
@include password-auth
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Identity+Assertion"></a>Identity Assertion [](#knox-apache-org-books-knox-2-1-0-user-guide--Identity+Assertion)

The identity assertion provider within Knox plays the critical role of communicating the identity principal to be used within the Hadoop cluster to represent the identity that has been authenticated at the gateway.

The general responsibilities of the identity assertion provider is to interrogate the current Java Subject that has been established by the authentication or federation provider and:

1. determine whether it matches any principal mapping rules and apply them appropriately
2. determine whether it matches any group principal mapping rules and apply them
3. if it is determined that the principal will be impersonating another through a principal mapping rule then a Subject.doAS is required so providers farther downstream can determine the appropriate effective principal name and groups for the user

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Default+Identity+Assertion+Provider"></a>Default Identity Assertion Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--Default+Identity+Assertion+Provider)

The following configuration is required for asserting the users identity to the Hadoop cluster using Pseudo or Simple “authentication” and for using Kerberos/SPNEGO for secure clusters.

```xml
<provider>
    <role>identity-assertion</role>
    <name>Default</name>
    <enabled>true</enabled>
</provider>
```

This particular configuration indicates that the Default identity assertion provider is enabled and that there are no principal mapping rules to apply to identities flowing from the authentication in the gateway to the backend Hadoop cluster services. The primary principal of the current subject will therefore be asserted via a query parameter or as a form parameter - ie. `?user.name={primaryPrincipal}`

```xml
<provider>
    <role>identity-assertion</role>
    <name>Default</name>
    <enabled>true</enabled>
    <param>
        <name>principal.mapping</name>
        <value>guest=hdfs;</value>
    </param>
    <param>
        <name>group.principal.mapping</name>
        <value>*=users;hdfs=admin</value>
    </param>
    <param>
       <name>hadoop.proxyuser.impersonation.enabled</name>
       <value>false</value>
     </param>
     <param>
       <name>hadoop.proxyuser.admin.users</name>
       <value>*</value>
     </param>
     <param>
       <name>hadoop.proxyuser.admin.groups</name>
       <value>*</value>
     </param>
     <param>
       <name>hadoop.proxyuser.admin.hosts</name>
       <value>*</value>
     </param>
</provider>
```

This configuration identifies the same identity assertion provider but does provide principal and group mapping rules. In this case, when a user is authenticated as “guest” his identity is actually asserted to the Hadoop cluster as “hdfs”. In addition, since there are group principal mappings defined, he will also be considered as a member of the groups “users” and “admin”. In this particular example the wildcard "\*“ is used to indicate that all authenticated users need to be considered members of the ”users“ group and that only the user ”hdfs“ is mapped to be a member of the ”admin" group.

**NOTE: These group memberships are currently only meaningful for Service Level Authorization using the AclsAuthorization provider. The groups are not currently asserted to the Hadoop cluster at this time. See the Authorization section within this guide to see how this is used.**

The principal mapping aspect of the identity assertion provider is important to understand in order to fully utilize the authorization features of this provider.

This feature allows us to map the authenticated principal to a runAs or impersonated principal to be asserted to the Hadoop services in the backend.

When a principal mapping is defined that results in an impersonated principal, this impersonated principal is then the effective principal.

If there is no mapping to another principal then the authenticated or primary principal is the effective principal.

Another way to impersonate principals is to apply Hadoop Proxyuser-based impersonations as described in the next section.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+Proxyuser+impersonation"></a>Hadoop Proxyuser impersonation [](#knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+Proxyuser+impersonation)

From v2.0.0, an authenticated user can impersonate other user(s) leveraging Hadoop’s proxuyser configuration mechanism. This feature was implemented in [KNOX-2839](https://issues.apache.org/jira/browse/KNOX-2839) and requires the following configuration to work:

- `hadoop.proxyuser.impersonation.enabled` - a `boolean` flag indicates if token impersonation is enabled. Defaults to `true`
- `hadoop.proxyuser.$username.users` - indicates the list of users for whom `$username` is allowed to impersonate. It is possible to set this to a 1-element list using the `*` wildcard which means `$username` can impersonate everyone. Defaults to an empty list that is equivalent to `$username` is not allowed to impersonate anyone.
- `hadoop.proxyuser.$username.groups` - indicates the list of group names for whose members `$username` is allowed to impersonate. It is possible to set this to a 1-element list using the `*` wildcard which means `$username` can impersonate members of any group. Defaults to an empty list that is equivalent to `$username` is not allowed to impersonate members from any group.
- `hadoop.proxyuser.$username.hosts` - indicates a list of hostnames from where the requests are allowed to be accepted in case the `doAs` parameter is used when impersonating requests. It is possible to set this to a 1-element list using the `*` wildcard which means `$username` can impersonate incoming requests from any host. Defaults to an empty list that is equivalent to `$username` is not allowed to impersonate requests from any host.

Please note this configuration is applied **iff** the `doAs` query parameter is present in the incoming request and impersonation is enabled in the affected topology.

***Important note:*** this new-type impersonation support on the identity assertion layer is ignored if the topology uses the `HadoopAuth` authentication provider because the `doAs` support is working OOTB there, therefore a second authorization is useless going forward.

It’s also worth articulating that Hadoop Proxyuser-based impersonation works together with the already existing principal mapping (see below). At first, Knox applies the Hadoop Proxyuser impersonation, then it proceeds with principal mappings (if any). Let see a sample:

- `hadoop.proxyuser.admin.users` is set to `bob` (`admin` is allowed to impersonate `bob`)
- `principal.mapping` is set to `bob=tom` (`bob` is mapped as `tom` )

The `admin` user sends the following request:

```bash
curl https://KNOX_HOST:8443/gateway/sandbox/service/path?doAs=bob
```

In the request processing flow, after the identity assertion phase is completed, `tom` will be the effective user. As you can see, the rules were applied transitively.

For other use cases you may want to check out [GitHub Pull Request #681](https://github.com/apache/knox/pull/681).

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Principal+Mapping"></a>Principal Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Principal+Mapping)

```xml
<param>
    <name>principal.mapping</name>
    <value>{primaryPrincipal}[,...]={impersonatedPrincipal}[;...]</value>
</param>
```

For instance:

```xml
<param>
    <name>principal.mapping</name>
    <value>guest=hdfs</value>
</param>
```

For multiple mappings:

```xml
<param>
    <name>principal.mapping</name>
    <value>guest,alice=hdfs;mary=alice2</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Expression-Based+Principal+Mapping"></a>Expression-Based Principal Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Expression-Based+Principal+Mapping)

Alternatively, you can use an expression language to define principal mappings.

```xml
<param>
  <name>expression.principal.mapping</name>
  <!-- expression that returns the new principal -->
  <value>...</value>
</param>
```

The value of `expression.principal.mapping` must be a valid expression that evaluates to a string.

For example, the following expression will map all users to one constant user, ‘bob’.

```xml
<param>
  <name>expression.principal.mapping</name>
  <value>'bob'</value>
</param>
```

By adding a conditional you can selectively apply the mapping to specific users.

```xml
<param>
  <name>expression.principal.mapping</name>
  <!-- Only map sam/tom to bob -->
  <value>
    (if (or (= username 'sam') 
            (= username 'tom')) 
        'bob')
  </value>
</param>
```

The `if` expression expects ether 2 or 3 parameters. The first one is always a conditional that should return a boolean value. The second parameter is the consequent branch that is only evaluated if the conditional is true. The third, optional part is the alternative branch that is evaluated if the conditional is false.

```text
(if (< (strlen username) 5)
    (concat username '_suffix') 
    (concat 'prefix_' username))
```

Here the user `admin` will be mapped to `prefix_admin`, while `sam` will be mapped to `sam_suffix`.

In an XML topology, the less than and greater than operators should be either encoded as `&lt;` `&gt;`, or the expression should be put inside a CDATA section.

```text
(&lt; (strlen username) 5)
```

The following expression capitalizes the principal:

```text
(concat
  (uppercase (substr username 0 1))
  (lowercase (substr username 1)))
```

The functionality of the Regex-based identity assertion provider is exposed via the `regex-template` function.

```text
(regex-template  username '(.*)@(.*?)\..*' '{1}_{[2]}' (hash 'us' 'USA' 'ca' 'CANADA') true)
```

The above expression turns `nobody@us.imaginary.tld` to `nobody_USA`.

See [KNOX-2983](https://issues.apache.org/jira/browse/KNOX-2983) for the complete list of functions.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Group+Principal+Mapping"></a>Group Principal Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Group+Principal+Mapping)

```xml
<param>
    <name>group.principal.mapping</name>
    <value>{userName[,*|userName...]}={groupName[,groupName...]}[,...]</value>
</param>
```

For instance:

```xml
<param>
    <name>group.principal.mapping</name>
    <value>*=users;hdfs=admin</value>
</param>
```

this configuration indicates that all (\*) authenticated users are members of the “users” group and that user “hdfs” is a member of the admin group. Group principal mapping has been added along with the authorization provider described in this document.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Group+Mapping+Based+on+Predicates"></a>Group Mapping Based on Predicates [](#knox-apache-org-books-knox-2-1-0-user-guide--Group+Mapping+Based+on+Predicates)

```xml
<param>
    <name>group.mapping.{mappedGroupName}</name>
    <value>{predicate}</value>
</param>
```

The predicate-based group mapping offers more flexibility by allowing the use of arbitrary logical expressions to define the mappings. The predicate expression must evaluate to a boolean result, true or false. The syntax is inspired by the Lisp language, but it is limited to boolean expressions and comparisons.

For instance:

```xml
<param>
    <name>group.mapping.admin</name>
    <value>(or (username 'guest') (member 'analyst'))</value>
</param>
```

This configuration maps the user to the “admin” group if either the authenticated user is “guest” or the authenticated user is a member of the “analyst” group.

The syntax of the language is based on parenthesized prefix notation, meaning that the operators precede their operands.

The language is made up of two basic building blocks: atoms (boolean, string, number, symbol) and lists. A list is written with its elements separated by whitespace, and surrounded by parentheses. A list can contain other lists or atoms.

A function call or an operator is written as a list with the function or operator’s name first, and the arguments following. For instance, a function f that takes three arguments would be called as (f arg1 arg2 arg3).

Lists are of arbitrary length and can be nested to express more complex conditionals.

```text
(or 
    (and
        (member 'admin')
        (member 'scientist'))
    (or
        (username 'tom')
        (username 'sam')))
```

Returns:

1. True if the user is either ‘tom’ or ‘sam’
2. True if the user is both in the ‘admin’ and the ‘scientist’ group.
3. False otherwise.

The following predicate checks if the user is a member of any group:

```text
(!= (size groups) 0)

(not (empty groups))

(match groups '.*')
```

This predicate checks if the username is either “tom” or “sam”:

```text
(match username 'tom|sam')
```

This checks the username in a case insensitive manner:

```text
(= (lowercase username) 'bob')
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Supported+functions+and+operators"></a>Supported functions and operators [](#knox-apache-org-books-knox-2-1-0-user-guide--Supported+functions+and+operators)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--or"></a>or [](#knox-apache-org-books-knox-2-1-0-user-guide--or)

Evaluates true if one or more of its operands is true. Supports short-circuit evaluation and variable number of arguments.

Number of arguments: 1..N

```text
(or bool1 bool2 ... boolN)
```

Example

```text
(or true false true)
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--and"></a>and [](#knox-apache-org-books-knox-2-1-0-user-guide--and)

Evaluates true if all of its operands are true. Supports short-circuit evaluation and variable number of arguments.

Number of arguments: 1..N

```text
(and bool1 bool2 ... boolN)
```

Example

```text
 (and true false true)
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--not"></a>not [](#knox-apache-org-books-knox-2-1-0-user-guide--not)

Negates the operand.

Number of arguments: 1

```text
 (not aBool)
```

Example

```text
 (not true)
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--="></a>= [](#knox-apache-org-books-knox-2-1-0-user-guide--=)

Evaluates true if the two operands are equal.

Number of arguments: 2

```text
 (= op1 op2)
```

Example

```text
 (= 'apple' 'orange')
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--!="></a>!= [](#knox-apache-org-books-knox-2-1-0-user-guide--!=)

Evaluates true if the two operands are not equal.

Number of arguments: 2

```text
 (!= op1 op2)
```

Example

```text
  (!= 'apple' 'orange')
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--member"></a>member [](#knox-apache-org-books-knox-2-1-0-user-guide--member)

Evaluates true if the current user is a member of the given group

Number of arguments: 1

```text
 (member aString)
```

Example

```text
 (member 'analyst')
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--username"></a>username [](#knox-apache-org-books-knox-2-1-0-user-guide--username)

Evaluates true if the current user has the given username

Number of arguments: 1

```text
(username aString)
```

Example

```text
(username 'admin')
```

This is a shorter version of (= username ‘admin’)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--size"></a>size [](#knox-apache-org-books-knox-2-1-0-user-guide--size)

Gets the size of a list

Number of arguments: 1

```text
 (size alist)
```

Example

```text
 (size groups)
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--empty"></a>empty [](#knox-apache-org-books-knox-2-1-0-user-guide--empty)

Evaluates to true if the given list is empty

Number of arguments: 1

```text
 (empty alist)
```

Example

```text
 (empty groups)
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--match"></a>match [](#knox-apache-org-books-knox-2-1-0-user-guide--match)

Evaluates true if the given string matches to the given regexp. Or any items of the given list matches the given regexp.

Number of arguments: 2

```text
(match aString aRegExpString)

(match aList aRegExpString)
```

Example

```text
(match username 'tom|sam')
```

This function can also take a list as a first argument. In this case it will return true if the regexp matches to any of the items in the list.

```text
(match groups 'analyst|scientist')
```

This returns true if the user is either in the ‘analyst’ group or in the ‘scientist’ group. The same can be expressed by combining 2 member functions with an or expression.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--request-header"></a>request-header [](#knox-apache-org-books-knox-2-1-0-user-guide--request-header)

Returns the value of the specified request header as a String. If the given key doesn’t exist empty string is returned.

Number of arguments: 1

```text
(request-header aString)
```

Example

```text
(request-header 'User-Agent')
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--request-attribute"></a>request-attribute [](#knox-apache-org-books-knox-2-1-0-user-guide--request-attribute)

Returns the value of the specified request attribute as a String. If the given key doesn’t exist empty string is returned.

Number of arguments: 1

```text
(request-attribute aString)
```

Example

```text
(request-attribute 'sourceRequestUrl')
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--session"></a>session [](#knox-apache-org-books-knox-2-1-0-user-guide--session)

Returns the value of the specified session attribute as a String. If the given key doesn’t exist empty string is returned.

Number of arguments: 1

```text
 (session aString)
```

Example

```text
 (session 'subject.userRoles')
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--lowercase"></a>lowercase [](#knox-apache-org-books-knox-2-1-0-user-guide--lowercase)

Converts the given string to lowercase.

Number of arguments: 1

```text
 (lowercase aString)
```

Example

```text
 (lowercase 'KNOX')
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--uppercase"></a>uppercase [](#knox-apache-org-books-knox-2-1-0-user-guide--uppercase)

Converts the given string to uppercase.

Number of arguments: 1

```text
 (uppercase aString)
```

Example

```text
(uppercase 'knox')
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Constants"></a>Constants [](#knox-apache-org-books-knox-2-1-0-user-guide--Constants)

The following constants are populated automatically from the current security context.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--username"></a>username [](#knox-apache-org-books-knox-2-1-0-user-guide--username)

The username (principal) of the current user, derived from javax.security.auth.Subject.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--groups"></a>groups [](#knox-apache-org-books-knox-2-1-0-user-guide--groups)

The groups of the current user (as determined by the authentication provider), derived from subject.getPrincipals(GroupPrincipal.class).

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Concat+Identity+Assertion+Provider"></a>Concat Identity Assertion Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--Concat+Identity+Assertion+Provider)

The Concat identity assertion provider allows for composition of a new user principal through the concatenation of optionally configured prefix and/or suffix provider parameters. This is a useful assertion provider for converting an incoming identity into a disambiguated identity within the Hadoop cluster based on what topology is used to access Hadoop.

The following configuration would convert the user principal into a value that represents a domain specific identity where the identities used inside the Hadoop cluster represent this same separation.

```xml
<provider>
    <role>identity-assertion</role>
    <name>Concat</name>
    <enabled>true</enabled>
    <param>
        <name>concat.suffix</name>
        <value>_domain1</value>
    </param>
</provider>
```

The above configuration will result in all user interactions through that topology to have their principal communicated to the Hadoop cluster with a domain designator concatenated to the username. Possibly useful for multi-tenant deployment scenarios.

In addition to the concat.suffix parameter, the provider supports the setting of a prefix through a `concat.prefix` parameter.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--SwitchCase+Identity+Assertion+Provider"></a>SwitchCase Identity Assertion Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--SwitchCase+Identity+Assertion+Provider)

The SwitchCase identity assertion provider solves issues where down stream ecosystem components require user and group principal names to be a specific case. An example of how this provider is enabled and configured within the `<gateway>` section of a topology file is shown below. This particular example will switch user principals names to lower case and group principal names to upper case.

```xml
<provider>
    <role>identity-assertion</role>
    <name>SwitchCase</name>
    <param>
        <name>principal.case</name>
        <value>lower</value>
    </param>
    <param>
        <name>group.principal.case</name>
        <value>upper</value>
    </param>
    <enabled>true</enabled>
</provider>
```

These are the configuration parameters used to control the behavior of the provider.

| Param | Description |
| --- | --- |
| principal.case | The case mapping of user principal names. Choices are: lower, upper, none. Defaults to lower. |
| group.principal.case | The case mapping of group principal names. Choices are: lower, upper, none. Defaults to setting of principal.case. |

If no parameters are provided the full defaults will results in both user and group principal names being switched to lower case. A setting of “none” or anything other than “upper” or “lower” leaves the case of the principal name unchanged.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Regular+Expression+Identity+Assertion+Provider"></a>Regular Expression Identity Assertion Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--Regular+Expression+Identity+Assertion+Provider)

The regular expression identity assertion provider allows incoming identities to be translated using a regular expression, template and lookup table. This will probably be most useful in conjunction with the HeaderPreAuth federation provider.

There are three configuration parameters used to control the behavior of the provider.

| Param | Description |
| --- | --- |
| input | This is a regular expression that will be applied to the incoming identity. The most critical part of the regular expression is the group notation within the expression. In regular expressions, groups are expressed within parenthesis. For example in the regular expression “(.*)@(.*?)\..*” there are two groups. When this regular expression is applied to “nobody@us.imaginary.tld” group 1 matches “nobody” and group 2 matches “us”. |
| output | This is a template that assembles the result identity. The result is assembled from the static text and the matched groups from the input regular expression. In addition, the matched group values can be looked up in the lookup table. An output value of “{1}_{2}” of will result in “nobody_us”. |
| lookup | This lookup table provides a simple (albeit limited) way to translate text in the incoming identities. This configuration takes the form of “=” separated name values pairs separated by “;”. For example a lookup setting is “us=USA;ca=CANADA”. The lookup is invoked in the output setting by surrounding the desired group number in square brackets (i.e. []). Putting it all together, output setting of “{1}_[{2}]” combined with input of “(.*)@(.*?)\..*” and lookup of “us=USA;ca=CANADA” will turn “nobody@us.imaginary.tld” into "nobody@USA". |
| use.original.on.lookup.failure | (Optional) Default value is false. If set to true, it will preserve the original string if there is no match. e.g. In the above lookup case for emailnobody@uk.imaginary.tld, it will be transformed to nobody@ , if this property is set to true it will be transformed tonobody@uk. |

Within the topology file the provider configuration might look like this.

```xml
<provider>
    <role>identity-assertion</role>
    <name>Regex</name>
    <enabled>true</enabled>
    <param>
        <name>input</name>
        <value>(.*)@(.*?)\..*</value>
    </param>
    <param>
        <name>output</name>
        <value>{1}_{[2]}</value>
    </param>
    <param>
        <name>lookup</name>
        <value>us=USA;ca=CANADA</value>
    </param>
</provider>  
```

Using curl with this type of configuration might produce the following results.

```bash
curl -k --header "SM_USER: nobody@us.imaginary.tld" 'https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY'

{"Path":"/user/member_USA"}

url -k --header "SM_USER: nobody@ca.imaginary.tld" 'https://localhost:8443/gateway/sandbox/webhdfs/v1?op=GETHOMEDIRECTORY'

{"Path":"/user/member_CANADA"}
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+Group+Lookup+Provider"></a>Hadoop Group Lookup Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+Group+Lookup+Provider)

An identity assertion provider that looks up user’s ‘group membership’ for authenticated users using Hadoop’s group mapping service (GroupMappingServiceProvider).

This allows existing investments in the Hadoop to be leveraged within Knox and used within the access control policy enforcement at the perimeter.

The ‘role’ for this provider is ‘identity-assertion’ and name is ‘HadoopGroupProvider’.

```xml
    <provider>
        <role>identity-assertion</role>
        <name>HadoopGroupProvider</name>
        <enabled>true</enabled>
        <<param> ... </param>
    </provider>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

All the configuration for ‘HadoopGroupProvider’ resides in the provider section in a gateway topology file. The ‘hadoop.security.group.mapping’ property determines the implementation. This configuration may be centralized within the gateway-site.xml through the use of a special param to this provider called CENTRAL\_GROUP\_CONFIG\_PREFIX. This indicates to the provider that the required configuration can be found within the gateway-site.xml file with the provided prefix.

```xml
     <param>
        <name>CENTRAL_GROUP_CONFIG_PREFIX</name>
        <value>gateway.group.config.</value>
     </param>
```

Some of the valid implementations are as follows:

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback"></a>org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback [](#knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback)

This is the default implementation and will be picked up if ‘hadoop.security.group.mapping’ is not specified. This implementation will determine if the Java Native Interface (JNI) is available. If JNI is available, the implementation will use the API within Hadoop to resolve a list of groups for a user. If JNI is not available then the shell implementation, `org.apache.hadoop.security.ShellBasedUnixGroupsMapping`, is used, which shells out with the `bash -c id -gn <user> ; id -Gn <user>` command (for a Linux/Unix environment) or the `groups -F <user>` command (for a Windows environment) to resolve a list of groups for a user.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMappingWithFallback"></a>org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMappingWithFallback [](#knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMappingWithFallback)

As above, if JNI is available then we get the netgroup membership using Hadoop native API, else fallback on ShellBasedUnixGroupsNetgroupMapping to resolve list of groups for a user.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.ShellBasedUnixGroupsMapping"></a>org.apache.hadoop.security.ShellBasedUnixGroupsMapping [](#knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.ShellBasedUnixGroupsMapping)

Uses the `bash -c id -gn <user> ; id -Gn <user>` command (for a Linux/Unix environment) or the `groups -F <user>` command (for a Windows environment) to resolve list of groups for a user.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping"></a>org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping [](#knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping)

Similar to `org.apache.hadoop.security.ShellBasedUnixGroupsMapping` except it uses `getent netgroup` command to get netgroup membership.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.LdapGroupsMapping"></a>org.apache.hadoop.security.LdapGroupsMapping [](#knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.LdapGroupsMapping)

This implementation connects directly to an LDAP server to resolve the list of groups. However, this should only be used if the required groups reside exclusively in LDAP, and are not materialized on the Unix servers.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.CompositeGroupsMapping"></a>org.apache.hadoop.security.CompositeGroupsMapping [](#knox-apache-org-books-knox-2-1-0-user-guide--org.apache.hadoop.security.CompositeGroupsMapping)

This implementation asks multiple other group mapping providers for determining group membership, see [Composite Groups Mapping](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/GroupsMapping.html#Composite_Groups_Mapping) for more details.

For more information on the implementation and properties refer to Hadoop Group Mapping.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Example"></a>Example [](#knox-apache-org-books-knox-2-1-0-user-guide--Example)

The following example snippet works with the demo ldap server that ships with Apache Knox. Replace the existing ‘Default’ identity-assertion provider with the one below (HadoopGroupProvider).

```xml
    <provider>
        <role>identity-assertion</role>
        <name>HadoopGroupProvider</name>
        <enabled>true</enabled>
        <param>
            <name>hadoop.security.group.mapping</name>
            <value>org.apache.hadoop.security.LdapGroupsMapping</value>
        </param>
        <param>
            <name>hadoop.security.group.mapping.ldap.bind.user</name>
            <value>uid=tom,ou=people,dc=hadoop,dc=apache,dc=org</value>
        </param>
        <param>
            <name>hadoop.security.group.mapping.ldap.bind.password</name>
            <value>tom-password</value>
        </param>
        <param>
            <name>hadoop.security.group.mapping.ldap.url</name>
            <value>ldap://localhost:33389</value>
        </param>
        <param>
            <name>hadoop.security.group.mapping.ldap.base</name>
            <value></value>
        </param>
        <param>
            <name>hadoop.security.group.mapping.ldap.search.filter.user</name>
            <value>(&amp;(|(objectclass=person)(objectclass=applicationProcess))(cn={0}))</value>
        </param>
        <param>
            <name>hadoop.security.group.mapping.ldap.search.filter.group</name>
            <value>(objectclass=groupOfNames)</value>
        </param>
        <param>
            <name>hadoop.security.group.mapping.ldap.search.attr.member</name>
            <value>member</value>
        </param>
        <param>
            <name>hadoop.security.group.mapping.ldap.search.attr.group.name</name>
            <value>cn</value>
        </param>
    </provider>
```

Here, we are working with the demo LDAP server running at ‘[ldap://localhost:33389](ldap://localhost:33389)’ which populates some dummy users for testing that we will use in this example. This example uses the user ‘tom’ for LDAP binding. If you have different LDAP/AD settings, you will have to update the properties accordingly.

Let’s test our setup using the following command (assuming the gateway is started and listening on localhost:8443). Note that we are using credentials for the user ‘sam’ along with the command.

```bash
    curl -i -k -u sam:sam-password -X GET 'https://localhost:8443/gateway/sandbox/webhdfs/v1/?op=LISTSTATUS' 
```

The command should be executed successfully and you should see the groups ‘scientist’ and ‘analyst’ to which user ‘sam’ belongs to in gateway-audit.log i.e.

```text
    ||a99aa0ab-fc06-48f2-8df3-36e6fe37c230|audit|WEBHDFS|sam|||identity-mapping|principal|sam|success|Groups: [scientist, analyst]
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Authorization"></a>Authorization [](#knox-apache-org-books-knox-2-1-0-user-guide--Authorization)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Service+Level+Authorization"></a>Service Level Authorization [](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Level+Authorization)

The Knox Gateway has an out-of-the-box authorization provider that allows administrators to restrict access to the individual services within a Hadoop cluster.

This provider utilizes a simple and familiar pattern of using ACLs to protect Hadoop resources by specifying users, groups and ip addresses that are permitted access.

Note: This feature will not work as expected if ‘anonymous’ authentication is used.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

ACLs are bound to services within the topology descriptors by introducing the authorization provider with configuration like:

```xml
<provider>
    <role>authorization</role>
    <name>AclsAuthz</name>
    <enabled>true</enabled>
</provider>
```

The above configuration enables the authorization provider but does not indicate any ACLs yet and therefore there is no restriction to accessing the Hadoop services. In order to indicate the resources to be protected and the specific users, groups or ip’s to grant access, we need to provide parameters like the following:

```xml
<param>
    <name>{serviceName}.acl</name>
    <value>username[,*|username...];group[,*|group...];ipaddr[,*|ipaddr...]</value>
</param>
```

where `{serviceName}` would need to be the name of a configured Hadoop service within the topology.

NOTE: ipaddr is unique among the parts of the ACL in that you are able to specify a wildcard within an ipaddr to indicate that the remote address must being with the String prior to the asterisk within the ipaddr ACL. For instance:

```xml
<param>
    <name>{serviceName}.acl</name>
    <value>*;*;192.168.*</value>
</param>
```

This indicates that the request must come from an IP address that begins with ‘192.168.’ in order to be granted access.

Note also that configuration without any ACLs defined is equivalent to:

```xml
<param>
    <name>{serviceName}.acl</name>
    <value>*;*;*</value>
</param>
```

meaning: all users, groups and IPs have access. Each of the elements of the ACL parameter support multiple values via comma separated list and the `*` wildcard to match any.

For instance:

```xml
<param>
    <name>webhdfs.acl</name>
    <value>hdfs;admin;127.0.0.2,127.0.0.3</value>
</param>
```

this configuration indicates that ALL of the following have to be satisfied to be granted access:

1. The user name must be “hdfs” AND
2. the user must be in the group “admin” AND
3. the user must come from either 127.0.0.2 or 127.0.0.3

This allows us to craft policy that restricts the members of a large group to a subset that should have access. The user being removed from the group will allow access to be denied even though their username may have been in the ACL.

An additional configuration element may be used to alter the processing of the ACL to be OR instead of the default AND behavior:

```xml
<param>
    <name>{serviceName}.acl.mode</name>
    <value>OR</value>
</param>
```

this processing behavior requires that the effective user satisfy one of the parts of the ACL definition in order to be granted access. For instance:

```xml
<param>
    <name>webhdfs.acl</name>
    <value>hdfs,guest;admin;127.0.0.2,127.0.0.3</value>
</param>
```

You may also set the ACL processing mode at the top level for the topology. This essentially sets the default for the managed cluster. It may then be overridden at the service level as well.

```xml
<param>
    <name>acl.mode</name>
    <value>OR</value>
</param>
```

this configuration indicates that ONE of the following must be satisfied to be granted access:

1. The user is “hdfs” or “guest” OR
2. the user is in “admin” group OR
3. the request is coming from 127.0.0.2 or 127.0.0.3

Following are a few concrete examples on how to use this feature.

Note: In the examples below `{serviceName}` represents a real service name (e.g. WEBHDFS) and would be replaced with these values in an actual configuration.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Usecases"></a>Usecases [](#knox-apache-org-books-knox-2-1-0-user-guide--Usecases)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-1:+Restrict+access+to+specific+Hadoop+services+to+specific+Users"></a>USECASE-1: Restrict access to specific Hadoop services to specific Users [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-1:+Restrict+access+to+specific+Hadoop+services+to+specific+Users)

```xml
<param>
    <name>{serviceName}.acl</name>
    <value>guest;*;*</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-2:+Restrict+access+to+specific+Hadoop+services+to+specific+Groups"></a>USECASE-2: Restrict access to specific Hadoop services to specific Groups [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-2:+Restrict+access+to+specific+Hadoop+services+to+specific+Groups)

```xml
<param>
    <name>{serviceName}.acls</name>
    <value>*;admins;*</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-3:+Restrict+access+to+specific+Hadoop+services+to+specific+Remote+IPs"></a>USECASE-3: Restrict access to specific Hadoop services to specific Remote IPs [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-3:+Restrict+access+to+specific+Hadoop+services+to+specific+Remote+IPs)

```xml
<param>
    <name>{serviceName}.acl</name>
    <value>*;*;127.0.0.1</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-4:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+OR+users+within+specific+Groups"></a>USECASE-4: Restrict access to specific Hadoop services to specific Users OR users within specific Groups [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-4:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+OR+users+within+specific+Groups)

```xml
<param>
    <name>{serviceName}.acl.mode</name>
    <value>OR</value>
</param>
<param>
    <name>{serviceName}.acl</name>
    <value>guest;admin;*</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-5:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+OR+users+from+specific+Remote+IPs"></a>USECASE-5: Restrict access to specific Hadoop services to specific Users OR users from specific Remote IPs [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-5:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+OR+users+from+specific+Remote+IPs)

```xml
<param>
    <name>{serviceName}.acl.mode</name>
    <value>OR</value>
</param>
<param>
    <name>{serviceName}.acl</name>
    <value>guest;*;127.0.0.1</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-6:+Restrict+access+to+specific+Hadoop+services+to+users+within+specific+Groups+OR+from+specific+Remote+IPs"></a>USECASE-6: Restrict access to specific Hadoop services to users within specific Groups OR from specific Remote IPs [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-6:+Restrict+access+to+specific+Hadoop+services+to+users+within+specific+Groups+OR+from+specific+Remote+IPs)

```xml
<param>
    <name>{serviceName}.acl.mode</name>
    <value>OR</value>
</param>
<param>
    <name>{serviceName}.acl</name>
    <value>*;admin;127.0.0.1</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-7:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+OR+users+within+specific+Groups+OR+from+specific+Remote+IPs"></a>USECASE-7: Restrict access to specific Hadoop services to specific Users OR users within specific Groups OR from specific Remote IPs [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-7:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+OR+users+within+specific+Groups+OR+from+specific+Remote+IPs)

```xml
<param>
    <name>{serviceName}.acl.mode</name>
    <value>OR</value>
</param>
<param>
    <name>{serviceName}.acl</name>
    <value>guest;admin;127.0.0.1</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-8:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+AND+users+within+specific+Groups"></a>USECASE-8: Restrict access to specific Hadoop services to specific Users AND users within specific Groups [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-8:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+AND+users+within+specific+Groups)

```xml
<param>
    <name>{serviceName}.acl</name>
    <value>guest;admin;*</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-9:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+AND+users+from+specific+Remote+IPs"></a>USECASE-9: Restrict access to specific Hadoop services to specific Users AND users from specific Remote IPs [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-9:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+AND+users+from+specific+Remote+IPs)

```xml
<param>
    <name>{serviceName}.acl</name>
    <value>guest;*;127.0.0.1</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-10:+Restrict+access+to+specific+Hadoop+services+to+users+within+specific+Groups+AND+from+specific+Remote+IPs"></a>USECASE-10: Restrict access to specific Hadoop services to users within specific Groups AND from specific Remote IPs [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-10:+Restrict+access+to+specific+Hadoop+services+to+users+within+specific+Groups+AND+from+specific+Remote+IPs)

```xml
<param>
    <name>{serviceName}.acl</name>
    <value>*;admins;127.0.0.1</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-11:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+AND+users+within+specific+Groups+AND+from+specific+Remote+IPs"></a>USECASE-11: Restrict access to specific Hadoop services to specific Users AND users within specific Groups AND from specific Remote IPs [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-11:+Restrict+access+to+specific+Hadoop+services+to+specific+Users+AND+users+within+specific+Groups+AND+from+specific+Remote+IPs)

```xml
<param>
    <name>{serviceName}.acl</name>
    <value>guest;admins;127.0.0.1</value>
</param>
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--USECASE-12:+Full+example+including+identity+assertion/principal+mapping"></a>USECASE-12: Full example including identity assertion/principal mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--USECASE-12:+Full+example+including+identity+assertion/principal+mapping)

The principal mapping aspect of the identity assertion provider is important to understand in order to fully utilize the authorization features of this provider.

This feature allows us to map the authenticated principal to a runAs or impersonated principal to be asserted to the Hadoop services in the backend. It is fully documented in the Identity Assertion section of this guide.

These additional mapping capabilities are used together with the authorization ACL policy. An example of a full topology that illustrates these together is below.

```xml
<topology>
    <gateway>
        <provider>
            <role>authentication</role>
            <name>ShiroProvider</name>
            <enabled>true</enabled>
            <param>
                <name>main.ldapRealm</name>
                <value>org.apache.shiro.realm.ldap.JndiLdapRealm</value>
            </param>
            <param>
                <name>main.ldapRealm.userDnTemplate</name>
                <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
            </param>
            <param>
                <name>main.ldapRealm.contextFactory.url</name>
                <value>ldap://localhost:33389</value>
            </param>
            <param>
                <name>main.ldapRealm.contextFactory.authenticationMechanism</name>
                <value>simple</value>
            </param>
            <param>
                <name>urls./**</name>
                <value>authcBasic</value>
            </param>
        </provider>
        <provider>
            <role>identity-assertion</role>
            <name>Default</name>
            <enabled>true</enabled>
            <param>
                <name>principal.mapping</name>
                <value>guest=hdfs;</value>
            </param>
            <param>
                <name>group.principal.mapping</name>
                <value>*=users;hdfs=admin</value>
            </param>
        </provider>
        <provider>
            <role>authorization</role>
            <name>AclsAuthz</name>
            <enabled>true</enabled>
            <param>
                <name>acl.mode</name>
                <value>OR</value>
            </param>
            <param>
                <name>webhdfs.acl.mode</name>
                <value>AND</value>
            </param>
            <param>
                <name>webhdfs.acl</name>
                <value>hdfs;admin;127.0.0.2,127.0.0.3</value>
            </param>
            <param>
                <name>webhcat.acl</name>
                <value>hdfs;admin;127.0.0.2,127.0.0.3</value>
            </param>
        </provider>
        <provider>
            <role>hostmap</role>
            <name>static</name>
            <enabled>true</enabled>
            <param>
                <name>localhost</name>
                <value>sandbox,sandbox.hortonworks.com</value>
            </param>
        </provider>
    </gateway>

    <service>
        <role>JOBTRACKER</role>
        <url>rpc://localhost:8050</url>
    </service>

    <service>
        <role>WEBHDFS</role>
        <url>http://localhost:50070/webhdfs</url>
    </service>

    <service>
        <role>WEBHCAT</role>
        <url>http://localhost:50111/templeton</url>
    </service>

    <service>
        <role>OOZIE</role>
        <url>http://localhost:11000/oozie</url>
    </service>

    <service>
        <role>WEBHBASE</role>
        <url>http://localhost:8080</url>
    </service>

    <service>
        <role>HIVE</role>
        <url>http://localhost:10001/cliservice</url>
    </service>
</topology>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Composite+Authorization+Provider"></a>Composite Authorization Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--Composite+Authorization+Provider)

By providing a composite authz provider, we are able to configure multiple authz providers in a single topology. This allows the use of both the AclsAuthz provider and something like the Ranger Knox plugin where available.

All authorization providers used within the CompositeAuthz provider will need to grant access for the request processing to continue to the protected resource. This is a logical AND across all the providers.

The following is an example of what configuration of the CompositeAuthz provider is like.

```xml
    <provider>
        <role>authorization</role>
        <name>CompositeAuthz</name>
        <enabled>true</enabled>
        <param>
            <name>composite.provider.names</name>
            <value>AclsAuthz,SomeOther</value>
        </param>
        <param>
            <name>AclsAuthz.webhdfs.acl</name>
            <value>admin;*;*</value>
        </param>
        <param>
            <name>SomeOther.provider.specific.param</name>
            <value>provider.specific-value</value>
        </param>
    </provider>
```

Note the comma separated list of provider names in composite.provider.names param.

Also Note the use of those names as prefixes to the params to be set on the respective providers.

The prefixes are removed and the expected param names are set on the actual providers as appropriate.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Path+Based+Authorization"></a>Path Based Authorization [](#knox-apache-org-books-knox-2-1-0-user-guide--Path+Based+Authorization)

Path based authorization (`PathAclsAuthz`) enforces Acls authorization on a configured path. The semantics of Path based authorization are similar to Acls authz. Authorization is done based on path matching similar to rewrite rules.

Format is very similar to AclsAuthz provider with an addition of path argument. The format is `{path};{users};{groups}:{ips}`. For details on the format please see [Service Level Authorization](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Level+Authorization). One important thing to note here is that the path is not plural, there has to be one and only one path defined.

In case one wants multiple paths they can define multiple rules with rule name as a parameter e.g. `KNOXTOKEN.{rule_name}.path.acl`

Following are special cases for rule names:

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--This+rule+will+be+applied+to+ALL+services+defined+in+the+topology"></a>This rule will be applied to ALL services defined in the topology [](#knox-apache-org-books-knox-2-1-0-user-guide--This+rule+will+be+applied+to+ALL+services+defined+in+the+topology)

This rule be applied to all services in the topology. Which means any service that has `api` as a context path needs the user to be `admin` for successful authorization.

```xml
    <provider>
        <role>authorization</role>
        <name>PathAclsAuthz</name>
        <enabled>true</enabled>
        <param>
            <name>path.acl</name>
            <value>https://*:*/**/api/**;admin;*;*</value> 
        </param>
    </provider>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--This+rule+will+be+applied+to+only+the+service+{service_name}"></a>This rule will be applied to only the service {service\_name} [](#knox-apache-org-books-knox-2-1-0-user-guide--This+rule+will+be+applied+to+only+the+service+{service_name})

This rule be applied to only `{service_name}` services in the topology. Any request for `{service_name}` that has `api` as a context path needs the user to be `admin` for successful authorization.

```xml
    <provider>
        <role>authorization</role>
        <name>PathAclsAuthz</name>
        <enabled>true</enabled>
        <param>
            <name>{service_name}.path.acl</name>
            <value>https://*:*/**/api/**;admin;*;*</value> 
        </param>
    </provider>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--ALL+of+these+rules+will+be+applied+to+service+{service_name}"></a>ALL of these rules will be applied to service {service\_name} [](#knox-apache-org-books-knox-2-1-0-user-guide--ALL+of+these+rules+will+be+applied+to+service+{service_name})

*NOTE:* {rule\_1} and {rule\_2} should be any unique names. Similar to previous cases for a service `{service_name}`, for any request to be successful with `api` and `api2` as context paths, it needs to have user `admin`.

```xml
    <provider>
        <role>authorization</role>
        <name>PathAclsAuthz</name>
        <enabled>true</enabled>
        <param>
            <name>{service_name}.{rule_1}.path.acl</name>
            <value>https://*:*/**/api/**;admin;*;*</value> 
        </param>
        <param>
            <name>{service_name}.{rule_2}.path.acl</name>
            <value>https://*:*/**/api2/**;admin;*;*</value> 
        </param>
    </provider>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Examples"></a>Examples [](#knox-apache-org-books-knox-2-1-0-user-guide--Examples)

Following are concrete examples of the the above rules:

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--This+rule+will+be+applied+to+ALL+services+defined+in+the+topology"></a>This rule will be applied to ALL services defined in the topology [](#knox-apache-org-books-knox-2-1-0-user-guide--This+rule+will+be+applied+to+ALL+services+defined+in+the+topology)

```xml
    <provider>
        <role>authorization</role>
        <name>PathAclsAuthz</name>
        <enabled>true</enabled>
        <param>
            <name>path.acl</name>
            <value>https://*:*/**/knoxtoken/api/**;admin;*;*</value> 
        </param>
    </provider>
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--This+rule+will+be+applied+to+only+to+KNOXTOKEN+service"></a>This rule will be applied to only to KNOXTOKEN service [](#knox-apache-org-books-knox-2-1-0-user-guide--This+rule+will+be+applied+to+only+to+KNOXTOKEN+service)

```xml
    <provider>
        <role>authorization</role>
        <name>PathAclsAuthz</name>
        <enabled>true</enabled>
        <param>
            <name>KNOXTOKEN.path.acl</name>
            <value>https://*:*/**/knoxtoken/api/**;admin;*;*</value> 
        </param>
    </provider>
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--All+of+these+rules+will+be+applied+to+only+to+KNOXTOKEN+service"></a>All of these rules will be applied to only to KNOXTOKEN service [](#knox-apache-org-books-knox-2-1-0-user-guide--All+of+these+rules+will+be+applied+to+only+to+KNOXTOKEN+service)

```xml
    <provider>
        <role>authorization</role>
        <name>PathAclsAuthz</name>
        <enabled>true</enabled>
        <param>
            <name>KNOXTOKEN.rule_1.path.acl</name>
            <value>https://*:*/**/knoxtoken/api/**;admin;*;*</value> 
        </param>
        <param>
            <name>KNOXTOKEN.rule_2.path.acl</name>
            <value>https://*:*/**/knoxtoken/foo/**;knox;*;*</value> 
        </param>
        <param>
            <name>KNOXTOKEN.rule_3.path.acl</name>
            <value>https://*:*/**/knoxtoken/bar/**;sam;admin;*</value> 
        </param>
    </provider>  
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Secure+Clusters"></a>Secure Clusters [](#knox-apache-org-books-knox-2-1-0-user-guide--Secure+Clusters)

See the Hadoop documentation for setting up a secure Hadoop cluster [http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SecureMode.html](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SecureMode.html)

Once you have a Hadoop cluster that is using Kerberos for authentication, you have to do the following to configure Knox to work with that cluster.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Create+Unix+account+for+Knox+on+Hadoop+master+nodes"></a>Create Unix account for Knox on Hadoop master nodes [](#knox-apache-org-books-knox-2-1-0-user-guide--Create+Unix+account+for+Knox+on+Hadoop+master+nodes)

```text
useradd -g hadoop knox
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Create+Kerberos+principal,+keytab+for+Knox"></a>Create Kerberos principal, keytab for Knox [](#knox-apache-org-books-knox-2-1-0-user-guide--Create+Kerberos+principal,+keytab+for+Knox)

One way of doing this, assuming your KDC realm is EXAMPLE.COM, is to ssh into your host running KDC and execute `kadmin.local` That will result in an interactive session in which you can execute commands.

ssh into your host running KDC

```text
kadmin.local
add_principal -randkey knox/knox@EXAMPLE.COM
ktadd -k knox.service.keytab -norandkey knox/knox@EXAMPLE.COM
exit
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Copy+knox+keytab+to+Knox+host"></a>Copy knox keytab to Knox host [](#knox-apache-org-books-knox-2-1-0-user-guide--Copy+knox+keytab+to+Knox+host)

Add unix account for the knox user on Knox host

```text
useradd -g hadoop knox
```

Copy knox.service.keytab created on KDC host on to your Knox host `{GATEWAY_HOME}/conf/knox.service.keytab`

```bash
chown knox knox.service.keytab
chmod 400 knox.service.keytab
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Update+`krb5.conf`+at+`{GATEWAY_HOME}/conf/krb5.conf`+on+Knox+host"></a>Update `krb5.conf` at `{GATEWAY_HOME}/conf/krb5.conf` on Knox host [](#knox-apache-org-books-knox-2-1-0-user-guide--Update+`krb5.conf`+at+`{GATEWAY_HOME}/conf/krb5.conf`+on+Knox+host)

You could copy the `{GATEWAY_HOME}/templates/krb5.conf` file provided in the Knox binary download and customize it to suit your cluster.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Update+`krb5JAASLogin.conf`+at+`/etc/knox/conf/krb5JAASLogin.conf`+on+Knox+host"></a>Update `krb5JAASLogin.conf` at `/etc/knox/conf/krb5JAASLogin.conf` on Knox host [](#knox-apache-org-books-knox-2-1-0-user-guide--Update+`krb5JAASLogin.conf`+at+`/etc/knox/conf/krb5JAASLogin.conf`+on+Knox+host)

You could copy the `{GATEWAY_HOME}/templates/krb5JAASLogin.conf` file provided in the Knox binary download and customize it to suit your cluster.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Update+`gateway-site.xml`+on+Knox+host"></a>Update `gateway-site.xml` on Knox host [](#knox-apache-org-books-knox-2-1-0-user-guide--Update+`gateway-site.xml`+on+Knox+host)

Update `conf/gateway-site.xml` in your Knox installation and set the value of `gateway.hadoop.kerberos.secured` to true.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Restart+Knox"></a>Restart Knox [](#knox-apache-org-books-knox-2-1-0-user-guide--Restart+Knox)

After you do the above configurations and restart Knox, Knox would use SPNEGO to authenticate with Hadoop services and Oozie. There is no change in the way you make calls to Knox whether you use curl or Knox DSL.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--High+Availability"></a>High Availability [](#knox-apache-org-books-knox-2-1-0-user-guide--High+Availability)

This describes how Knox itself can be made highly available.

All Knox instances must be configured to use the same topology credential keystores. These files are located under `{GATEWAY_HOME}/data/security/keystores/{TOPOLOGY_NAME}-credentials.jceks`. They are generated after the first topology deployment.

In addition to these topology-specific credentials, gateway credentials and topologies must also be kept in-sync for Knox to operate in an HA manner.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Manually+Synchronize+Knox+Instances"></a>Manually Synchronize Knox Instances [](#knox-apache-org-books-knox-2-1-0-user-guide--Manually+Synchronize+Knox+Instances)

Here are the steps to manually sync topology credential keystores:

1. Choose a Knox instance that will be the source for topology credential keystores. Let’s call it *keystores master*
2. Replace the topology credential keystores in the other Knox instances with topology credential keystores from the *keystores master*
3. Restart Knox instances

Manually synchronizing the gateway credentials and topologies involves using ssh/scp to copy the topology-related files to all the participating Knox instances, and running the Knox CLI on each participating instance to define the gateway credential aliases.

This manual process can be tedious and error-prone. As such, [ZooKeeper-based HA](#knox-apache-org-books-knox-2-1-0-user-guide--High+Availability+with+Apache+ZooKeeper) is recommended to simplify the management of these deployments.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--High+Availability+with+Apache+ZooKeeper"></a>High Availability with Apache ZooKeeper [](#knox-apache-org-books-knox-2-1-0-user-guide--High+Availability+with+Apache+ZooKeeper)

Rather than manually keeping Knox HA instances in sync (in terms of credentials and topology), Knox can get it’s state from Apache ZooKeeper. By configuring all the Knox instances to monitor the same ZooKeeper ensemble, they can be kept in-sync by modifying the topology-related configuration and/or credential aliases at only one of the instances (using the Admin UI, Admin API, or Knox CLI).

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--What+is+Automatically+Synchronized+Across+Instances?"></a>What is Automatically Synchronized Across Instances? [](#knox-apache-org-books-knox-2-1-0-user-guide--What+is+Automatically+Synchronized+Across+Instances?)

- Provider Configurations
- Descriptors
- *Topologies* (generated only)
- Credential Aliases

When a provider configuration or descriptor is added or updated to the ZooKeeper ensemble, all of the participating Knox instances will get the change, and the affected topologies will be [re]generated and [re]deployed. Similarly, if one of these is deleted, the affected topologies will be deleted and undeployed.

When provider configurations and descriptors are added, modified or removed using the Admin UI or API (when the Knox instance is configured to monitor a ZooKeeper ensemble), then those changes will be automatically reflected in the associated ZooKeeper ensemble. Those changes will subsequently be consumed by all the other Knox instances monitoring that ensemble. By using the Admin UI or API, ssh/scp access to the Knox hosts can be avoided completely for the purpose of effecting topology changes.

Similarly, when the Knox CLI is used to create or delete a gateway alias (when the Knox instance is configured to monitor a ZooKeeper ensemble), that alias change is reflected in the ZooKeeper ensemble, and all other Knox instances montoring that ensemble will apply the change.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--What+is+NOT+Automatically+Synchronized+Across+Instances?"></a>What is NOT Automatically Synchronized Across Instances? [](#knox-apache-org-books-knox-2-1-0-user-guide--What+is+NOT+Automatically+Synchronized+Across+Instances?)

- Topologies (XML)
- Gateway config (e.g., gateway-site, gateway-logging, etc…)

If you’re creating/modifying topology XML files directly, then there is no automated support for keeping these in sync across Knox HA instances.

However, if the Knox instances are running in an Apache Ambari-managed cluster, there is limited support for keeping topology XML files and gateway configuration synchronized across those instances.

  

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--High+Availability+with+Apache+HTTP+Server+++mod_proxy+++mod_proxy_balancer"></a>High Availability with Apache HTTP Server + mod\_proxy + mod\_proxy\_balancer [](#knox-apache-org-books-knox-2-1-0-user-guide--High+Availability+with+Apache+HTTP+Server+++mod_proxy+++mod_proxy_balancer)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--1+-+Requirements"></a>1 - Requirements [](#knox-apache-org-books-knox-2-1-0-user-guide--1+-+Requirements)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--openssl-devel"></a>openssl-devel [](#knox-apache-org-books-knox-2-1-0-user-guide--openssl-devel)

openssl-devel is required for Apache Module mod\_ssl.

```text
sudo yum install openssl-devel
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Apache+HTTP+Server"></a>Apache HTTP Server [](#knox-apache-org-books-knox-2-1-0-user-guide--Apache+HTTP+Server)

Apache HTTP Server 2.4.6 or later is required. See this document for installing and setting up Apache HTTP Server: [http://httpd.apache.org/docs/2.4/install.html](http://httpd.apache.org/docs/2.4/install.html)

Hint: pass `--enable-ssl` to the `./configure` command to enable the generation of the Apache Module *mod\_ssl*.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Apache+Module+mod_proxy"></a>Apache Module mod\_proxy [](#knox-apache-org-books-knox-2-1-0-user-guide--Apache+Module+mod_proxy)

See this document for setting up Apache Module mod\_proxy: [http://httpd.apache.org/docs/2.4/mod/mod\_proxy.html](http://httpd.apache.org/docs/2.4/mod/mod_proxy.html)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Apache+Module+mod_proxy_balancer"></a>Apache Module mod\_proxy\_balancer [](#knox-apache-org-books-knox-2-1-0-user-guide--Apache+Module+mod_proxy_balancer)

See this document for setting up Apache Module mod\_proxy\_balancer: [http://httpd.apache.org/docs/2.4/mod/mod\_proxy\_balancer.html](http://httpd.apache.org/docs/2.4/mod/mod_proxy_balancer.html)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Apache+Module+mod_ssl"></a>Apache Module mod\_ssl [](#knox-apache-org-books-knox-2-1-0-user-guide--Apache+Module+mod_ssl)

See this document for setting up Apache Module mod\_ssl: [http://httpd.apache.org/docs/2.4/mod/mod\_ssl.html](http://httpd.apache.org/docs/2.4/mod/mod_ssl.html)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--2+-+Configuration+example"></a>2 - Configuration example [](#knox-apache-org-books-knox-2-1-0-user-guide--2+-+Configuration+example)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Generate+certificate+for+Apache+HTTP+Server"></a>Generate certificate for Apache HTTP Server [](#knox-apache-org-books-knox-2-1-0-user-guide--Generate+certificate+for+Apache+HTTP+Server)

See this document for an example: [http://www.akadia.com/services/ssh\_test\_certificate.html](http://www.akadia.com/services/ssh_test_certificate.html)

By convention, Apache HTTP Server and Knox certificates are put into the `/etc/apache2/ssl/` folder.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Update+Apache+HTTP+Server+configuration+file"></a>Update Apache HTTP Server configuration file [](#knox-apache-org-books-knox-2-1-0-user-guide--Update+Apache+HTTP+Server+configuration+file)

This file is located under {APACHE\_HOME}/conf/httpd.conf.

Following directives have to be added or uncommented in the configuration file:

- LoadModule proxy\_module modules/mod\_proxy.so
- LoadModule proxy\_http\_module modules/mod\_proxy\_http.so
- LoadModule proxy\_balancer\_module modules/mod\_proxy\_balancer.so
- LoadModule ssl\_module modules/mod\_ssl.so
- LoadModule lbmethod\_byrequests\_module modules/mod\_lbmethod\_byrequests.so
- LoadModule lbmethod\_bytraffic\_module modules/mod\_lbmethod\_bytraffic.so
- LoadModule lbmethod\_bybusyness\_module modules/mod\_lbmethod\_bybusyness.so
- LoadModule lbmethod\_heartbeat\_module modules/mod\_lbmethod\_heartbeat.so
- LoadModule slotmem\_shm\_module modules/mod\_slotmem\_shm.so

Also following lines have to be added to file. Replace placeholders (${…}) with real data:

```text
Listen 443
<VirtualHost *:443>
   SSLEngine On
   SSLProxyEngine On
   SSLCertificateFile ${PATH_TO_CERTIFICATE_FILE}
   SSLCertificateKeyFile ${PATH_TO_CERTIFICATE_KEY_FILE}
   SSLProxyCACertificateFile ${PATH_TO_PROXY_CA_CERTIFICATE_FILE}

   ProxyRequests Off
   ProxyPreserveHost Off

   RequestHeader set X-Forwarded-Port "443"
   Header add Set-Cookie "ROUTEID=.%{BALANCER_WORKER_ROUTE}e; path=/" env=BALANCER_ROUTE_CHANGED
   <Proxy balancer://mycluster>
     BalancerMember ${HOST_#1} route=1
     BalancerMember ${HOST_#2} route=2
     ...
     BalancerMember ${HOST_#N} route=N

     ProxySet failontimeout=On lbmethod=${LB_METHOD} stickysession=ROUTEID 
   </Proxy>

   ProxyPass / balancer://mycluster/
   ProxyPassReverse / balancer://mycluster/
</VirtualHost>
```

Note:

- SSLProxyEngine enables SSL between Apache HTTP Server and Knox instances;
- SSLCertificateFile and SSLCertificateKeyFile have to point to certificate data of Apache HTTP Server. User will use this certificate for communications with Apache HTTP Server;
- SSLProxyCACertificateFile has to point to Knox certificates.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Start/stop+Apache+HTTP+Server"></a>Start/stop Apache HTTP Server [](#knox-apache-org-books-knox-2-1-0-user-guide--Start/stop+Apache+HTTP+Server)

```text
APACHE_HOME/bin/apachectl -k start
APACHE_HOME/bin/apachectl -k stop
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Verify"></a>Verify [](#knox-apache-org-books-knox-2-1-0-user-guide--Verify)

Use Knox samples.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Web+App+Security+Provider"></a>Web App Security Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--Web+App+Security+Provider)

Knox is a Web API (REST) Gateway for Hadoop. The fact that REST interactions are HTTP based means that they are vulnerable to a number of web application security vulnerabilities. This project introduces a web application security provider for plugging in various protection filters.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Overview"></a>Overview [](#knox-apache-org-books-knox-2-1-0-user-guide--Overview)

As with all providers in the Knox gateway, the web app security provider is configured through provider parameters. Unlike many other providers, the web app security provider may actually host multiple vulnerability/security filters. Currently, we only have implementations for CSRF, CORS and HTTP STS but others might follow, and you may be interested in creating your own.

Because of this one-to-many provider/filter relationship, there is an extra configuration element for this provider per filter. As you can see in the sample below, the actual filter configuration is defined entirely within the parameters of the WebAppSec provider.

```xml
<provider>
    <role>webappsec</role>
    <name>WebAppSec</name>
    <enabled>true</enabled>
    <param><name>csrf.enabled</name><value>true</value></param>
    <param><name>csrf.customHeader</name><value>X-XSRF-Header</value></param>
    <param><name>csrf.methodsToIgnore</name><value>GET,OPTIONS,HEAD</value></param>
    <param><name>cors.enabled</name><value>false</value></param>
    <param><name>xframe.options.enabled</name><value>true</value></param>
    <param><name>xss.protection.enabled</name><value>true</value></param>
    <param><name>strict.transport.enabled</name><value>true</value></param>
</provider>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Descriptions"></a>Descriptions [](#knox-apache-org-books-knox-2-1-0-user-guide--Descriptions)

The following tables describes the configuration options for the web app security provider:

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--CSRF"></a>CSRF [](#knox-apache-org-books-knox-2-1-0-user-guide--CSRF)

Cross site request forgery (CSRF) attacks attempt to force an authenticated user to execute functionality without their knowledge. By presenting them with a link or image that when clicked invokes a request to another site with which the user may have already established an active session.

CSRF is entirely a browser-based attack. Some background knowledge of how browsers work enables us to provide a filter that will prevent CSRF attacks. HTTP requests from a web browser performed via form, image, iframe, etc. are unable to set custom HTTP headers. The only way to create a HTTP request from a browser with a custom HTTP header is to use a technology such as JavaScript XMLHttpRequest or Flash. These technologies can set custom HTTP headers but have security policies built in to prevent web sites from sending requests to each other unless specifically allowed by policy.

This means that a website www.bad.com cannot send a request to [http://bank.example.com](http://bank.example.com) with the custom header X-XSRF-Header unless they use a technology such as a XMLHttpRequest. That technology would prevent such a request from being made unless the bank.example.com domain specifically allowed it. This then results in a REST endpoint that can only be called via XMLHttpRequest (or similar technology).

NOTE: by enabling this protection within the topology, this custom header will be required for *all* clients that interact with it - not just browsers.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Config"></a>Config [](#knox-apache-org-books-knox-2-1-0-user-guide--Config)

| Name | Description | Default |
| --- | --- | --- |
| csrf.enabled | This parameter enables the CSRF protection capabilities | false |
| csrf.customHeader | This is an optional parameter that indicates the name of the header to be used in order to determine that the request is from a trusted source. It defaults to the header name described by the NSA in its guidelines for dealing with CSRF in REST. | X-XSRF-Header |
| csrf.methodsToIgnore | This is also an optional parameter that enumerates the HTTP methods to allow through without the custom HTTP header. This is useful for allowing things like GET requests from the URL bar of a browser, but it assumes that the GET request adheres to REST principals in terms of being idempotent. If this cannot be assumed then it would be wise to not include GET in the list of methods to ignore. | GET,OPTIONS,HEAD |

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--REST+Invocation"></a>REST Invocation [](#knox-apache-org-books-knox-2-1-0-user-guide--REST+Invocation)

The following curl command can be used to request a directory listing from HDFS while passing in the expected header X-XSRF-Header.

```bash
curl -k -i --header "X-XSRF-Header: valid" -v -u guest:guest-password https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp?op=LISTSTATUS
```

Omitting the `--header "X-XSRF-Header: valid"` above should result in an HTTP 400 bad\_request.

Disabling the provider will then allow a request that is missing the header through.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--CORS"></a>CORS [](#knox-apache-org-books-knox-2-1-0-user-guide--CORS)

For security reasons, browsers restrict cross-origin HTTP requests initiated from within scripts. For example, XMLHttpRequest follows the same-origin policy. So, a web application using XMLHttpRequest could only make HTTP requests to its own domain. To improve web applications, developers asked browser vendors to allow XMLHttpRequest to make cross-domain requests.

Cross Origin Resource Sharing is a way to explicitly alter the same-origin policy for a given application or API. In order to allow for applications to make cross domain requests through Apache Knox, we need to configure the CORS filter of the WebAppSec provider.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Config"></a>Config [](#knox-apache-org-books-knox-2-1-0-user-guide--Config)

| Name | Description | Default |
| --- | --- | --- |
| cors.enabled | Setting this parameter to true allows cross origin requests. The default of false prevents cross origin requests. | false |
| cors.allowGenericHttpRequests | {true\|false} defaults to true. If true, generic HTTP requests will be allowed to pass through the filter, else only valid and accepted CORS requests will be allowed (strict CORS filtering). | true |
| cors.allowOrigin | {“*”\|origin-list} defaults to “*”. Whitespace-separated list of origins that the CORS filter must allow. Requests from origins not included here will be refused with an HTTP 403 “Forbidden” response. If set to * (asterisk) any origin will be allowed. | “*” |
| cors.allowSubdomains | {true\|false} defaults to false. If true, the CORS filter will allow requests from any origin which is a subdomain origin of the allowed origins. A subdomain is matched by comparing its scheme and suffix (host name / IP address and optional port number). | false |
| cors.supportedMethods | {method-list} defaults to GET, POST, HEAD, OPTIONS. List of the supported HTTP methods. These are advertised through the Access-Control-Allow-Methods header and must also be implemented by the actual CORS web service. Requests for methods not included here will be refused by the CORS filter with an HTTP 405 “Method not allowed” response. | GET, POST, HEAD, OPTIONS |
| cors.supportedHeaders | {“*”\|header-list} defaults to *. The names of the supported author request headers. These are advertised through the Access-Control-Allow-Headers header. If the configuration property value is set to * (asterisk) any author request header will be allowed. The CORS Filter implements this by simply echoing the requested value back to the browser. | * |
| cors.exposedHeaders | {header-list} defaults to empty list. List of the response headers other than simple response headers that the browser should expose to the author of the cross-domain request through the XMLHttpRequest.getResponseHeader() method. The CORS filter supplies this information through the Access-Control-Expose-Headers header. | empty |
| cors.supportsCredentials | {true\|false} defaults to true. Indicates whether user credentials, such as cookies, HTTP authentication or client-side certificates, are supported. The CORS filter uses this value in constructing the Access-Control-Allow-Credentials header. | true |
| cors.maxAge | {int} defaults to -1 (unspecified). Indicates how long the results of a preflight request can be cached by the web browser, in seconds. If -1 unspecified. This information is passed to the browser via the Access-Control-Max-Age header. | -1 |
| cors.tagRequests | {true\|false} defaults to false (no tagging). Enables HTTP servlet request tagging to provide CORS information to downstream handlers (filters and/or servlets). | false |

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--X-Frame-Options"></a>X-Frame-Options [](#knox-apache-org-books-knox-2-1-0-user-guide--X-Frame-Options)

Cross Frame Scripting and Clickjacking are attacks that can be prevented by controlling the ability for a third-party to embed an application or resource within a Frame, IFrame or Object html element. This can be done adding the X-Frame-Options HTTP header to responses.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Config"></a>Config [](#knox-apache-org-books-knox-2-1-0-user-guide--Config)

| Name | Description | Default |
| --- | --- | --- |
| xframe.options.enabled | This parameter enables the X-Frame-Options capabilities | false |
| xframe.options.value | This parameter specifies a particular value for the X-Frame-Options header. Most often the default value of DENY will be most appropriate. You can also use SAMEORIGIN or ALLOW-FROM uri | DENY |

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--X-XSS-Protection"></a>X-XSS-Protection [](#knox-apache-org-books-knox-2-1-0-user-guide--X-XSS-Protection)

Cross-site Scripting (XSS) type attacks can be prevented by adding the X-XSS-Protection header to HTTP response. The `1; mode=block` value will force browser to stop rendering the page if XSS attack is detected.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Config"></a>Config [](#knox-apache-org-books-knox-2-1-0-user-guide--Config)

| Name | Description | Default |
| --- | --- | --- |
| xss.protection.enabled | This parameter specifies a particular value for the X-XSS-Protection header. When it is set to true, it will addX-Xss-Protection: '1; mode=block'header to HTTP response | false |

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--X-Content-Type-Options"></a>X-Content-Type-Options [](#knox-apache-org-books-knox-2-1-0-user-guide--X-Content-Type-Options)

Browser MIME content type sniffing can be exploited for malicious purposes. Adding the X-Content-Type-Options HTTP header to responses directs the browser to honor the type specified in the Content-Type header, rather than trying to determine the type from the content itself. Most modern browsers support this.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Config"></a>Config [](#knox-apache-org-books-knox-2-1-0-user-guide--Config)

| Name | Description | Default |
| --- | --- | --- |
| xcontent-type.options.enabled | This param enables the X-Content-Type-Options header inclusion | false |
| xcontent-type.options | This param specifies a particular value for the X-Content-Type-Options header. The default value is really the only meaningful value | nosniff |

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HTTP+Strict+Transport+Security"></a>HTTP Strict Transport Security [](#knox-apache-org-books-knox-2-1-0-user-guide--HTTP+Strict+Transport+Security)

HTTP Strict Transport Security (HSTS) is a web security policy mechanism which helps to protect websites against protocol downgrade attacks and cookie hijacking. It allows web servers to declare that web browsers (or other complying user agents) should only interact with it using secure HTTPS connections and never via the insecure HTTP protocol.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Config"></a>Config [](#knox-apache-org-books-knox-2-1-0-user-guide--Config)

| Name | Description | Default |
| --- | --- | --- |
| strict.transport.enabled | This parameter enables the HTTP Strict-Transport-Security response header | false |
| strict.transport | This parameter specifies a particular value for the HTTP Strict-Transport-Security header. Default value is max-age=31536000. You can also usemax-age=<expire-time>ormax-age=<expire-time>; includeSubDomainsormax-age=<expire-time>;preload | max-age=31536000 |

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Rate+limiting"></a>Rate limiting [](#knox-apache-org-books-knox-2-1-0-user-guide--Rate+limiting)

Rate limiting is very useful for limiting exposure to abuse from request flooding, whether malicious, or as a result of a misconfigured client. Following are the configurable options:

| Config Name | Description | Default |
| --- | --- | --- |
| rate.limiting.maxRequestsPerSec | Maximum number of requests from a connection per second. Requests in excess of this are first delayed, then throttled. | 25 |
| rate.limiting.delayMs | Delay imposed on all requests over the rate limit, before they are considered at all, in ms.-1 = Reject request,0 = No delay,any other value = Delay in ms.NOTE:with a non-negative value (including0) thegateway.servlet.async.supportedproperty in thegateway-site.xmlhas to be set totrue(it is false by default). | 100 |
| rate.limiting.maxWaitMs | Length of time to blocking wait for the throttle semaphore in ms. | 50 |
| rate.limiting.throttledRequests | Number of requests over the rate limit able to be considered at once. | 5 |
| rate.limiting.throttleMs | Length of time, in ms, to async wait for semaphore. | 30000L |
| rate.limiting.maxRequestMs | Length of time, in ms, to allow the request to run. | 30000L |
| rate.limiting.maxIdleTrackerMs | Length of time, in ms, to keep track of request rates for a connection, before deciding that the user has gone away, and discarding it. | 30000L |
| rate.limiting.insertHeaders | If true, insert the DoSFilter headers into the response. | true |
| rate.limiting.trackSessions | If true, usage rate is tracked by session if a session exists. | true |
| rate.limiting.remotePort | If true and session tracking is not used, then rate is tracked by IP and port (effectively connection). | false |
| rate.limiting.ipWhitelist | A comma-separated list of IP addresses that will not be rate limited. | empty |

When using the gateway with long running requests the `rate.limiting.maxRequestMs` parameter should be configured accordingly, otherwise the requests running longer then the default `30000ms` value, will be unsuccessful.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Config"></a>Config [](#knox-apache-org-books-knox-2-1-0-user-guide--Config)

| Name | Description | Default |
| --- | --- | --- |
| rate.limiting.enabled | This parameter enables the rate limiting feature | false |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HadoopAuth+Authentication+Provider"></a>HadoopAuth Authentication Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--HadoopAuth+Authentication+Provider)

The HadoopAuth authentication provider for Knox integrates the use of the Apache Hadoop module for SPNEGO and delegation token based authentication. This introduces the same authentication pattern used across much of the Hadoop ecosystem to Apache Knox and allows clients to using the strong authentication and SSO capabilities of Kerberos.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Overview"></a>Overview [](#knox-apache-org-books-knox-2-1-0-user-guide--Overview)

As with all providers in the Knox gateway, the HadoopAuth provider is configured through provider parameters. The configuration parameters are the same parameters used within Apache Hadoop for the same capabilities. In this section, we provide an example configuration and description of each of the parameters. We do encourage the reader to refer to the Hadoop documentation for this as well. (see [http://hadoop.apache.org/docs/current/hadoop-auth/Configuration.html](http://hadoop.apache.org/docs/current/hadoop-auth/Configuration.html))

One of the interesting things to note about this configuration is the use of the `config.prefix` parameter. In Hadoop there may be multiple components with their own specific configuration values for these parameters and since they may get mixed into the same Configuration object - there needs to be a way to identify the component specific values. The `config.prefix` parameter is used for this and is prepended to each of the configuration parameters for this provider. Below, you see an example configuration where the value for config.prefix happens to be `hadoop.auth.config`. You will also notice that this same value is prepended to the name of the rest of the configuration parameters.

```xml
<provider>
  <role>authentication</role>
  <name>HadoopAuth</name>
  <enabled>true</enabled>
  <param>
    <name>config.prefix</name>
    <value>hadoop.auth.config</value>
  </param>
  <param>
    <name>hadoop.auth.config.signature.secret</name>
    <value>knox-signature-secret</value>
  </param>
  <param>
    <name>hadoop.auth.config.type</name>
    <value>kerberos</value>
  </param>
  <param>
    <name>hadoop.auth.config.simple.anonymous.allowed</name>
    <value>false</value>
  </param>
  <param>
    <name>hadoop.auth.config.token.validity</name>
    <value>1800</value>
  </param>
  <param>
    <name>hadoop.auth.config.cookie.domain</name>
    <value>novalocal</value>
  </param>
  <param>
    <name>hadoop.auth.config.cookie.path</name>
    <value>gateway/default</value>
  </param>
  <param>
    <name>hadoop.auth.config.kerberos.principal</name>
    <value>HTTP/lmccay-knoxft-24m-r6-sec-160422-1327-2.novalocal@EXAMPLE.COM</value>
  </param>
  <param>
    <name>hadoop.auth.config.kerberos.keytab</name>
    <value>/etc/security/keytabs/spnego.service.keytab</value>
  </param>
  <param>
    <name>hadoop.auth.config.kerberos.name.rules</name>
    <value>DEFAULT</value>
  </param>
</provider>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Descriptions"></a>Descriptions [](#knox-apache-org-books-knox-2-1-0-user-guide--Descriptions)

The following tables describes the configuration parameters for the HadoopAuth provider:

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Config"></a>Config [](#knox-apache-org-books-knox-2-1-0-user-guide--Config)

| Name | Description | Default |
| --- | --- | --- |
| config.prefix | If specified, all other configuration parameter names must start with the prefix. | none |
| signature.secret | This is the secret used to sign the delegation token in the hadoop.auth cookie. This same secret needs to be used across all instances of the Knox gateway in a given cluster. Otherwise, the delegation token will fail validation and authentication will be repeated each request. | A simple random number |
| type | This parameter needs to be set tokerberos | none, would throw exception |
| simple.anonymous.allowed | This should always be false for a secure deployment. | true |
| token.validity | The validity -in seconds- of the generated authentication token. This is also used for the rollover interval whensigner.secret.provideris set to random or ZooKeeper. | 36000 seconds |
| cookie.domain | Domain to use for the HTTP cookie that stores the authentication token | null |
| cookie.path | Path to use for the HTTP cookie that stores the authentication token | null |
| kerberos.principal | The web-application Kerberos principal name. The Kerberos principal name must start with HTTP/…. For example:HTTP/localhost@LOCALHOST | null |
| kerberos.keytab | The path to the keytab file containing the credentials for the kerberos principal. For example:/Users/lmccay/lmccay.keytab | null |
| kerberos.name.rules | The name of the ruleset for extracting the username from the kerberos principal. | DEFAULT |

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--REST+Invocation"></a>REST Invocation [](#knox-apache-org-books-knox-2-1-0-user-guide--REST+Invocation)

Once a user logs in with kinit then their Kerberos session may be used across client requests with things like curl. The following curl command can be used to request a directory listing from HDFS while authenticating with SPNEGO via the `--negotiate` flag

```bash
curl -k -i --negotiate -u : https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp?op=LISTSTATUS
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Preauthenticated+SSO+Provider"></a>Preauthenticated SSO Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--Preauthenticated+SSO+Provider)

A number of SSO solutions provide mechanisms for federating an authenticated identity across applications. These mechanisms are at times simple HTTP Header type tokens that can be used to propagate the identity across process boundaries.

Knox Gateway needs a pluggable mechanism for consuming these tokens and federating the asserted identity through an interaction with the Hadoop cluster.

**CAUTION: The use of this provider requires that proper network security and identity provider configuration and deployment does not allow requests directly to the Knox gateway. Otherwise, this provider will leave the gateway exposed to identity spoofing.**

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Overview"></a>Overview [](#knox-apache-org-books-knox-2-1-0-user-guide--Overview)

This provider was designed for use with identity solutions such as those provided by CA’s SiteMinder and IBM’s Tivoli Access Manager. While direct testing with these products has not been done, there has been extensive unit and functional testing that ensure that it should work with such providers.

The HeaderPreAuth provider is configured within the topology file and has a minimal configuration that assumes SM\_USER for CA SiteMinder. The following example is the bare minimum configuration for SiteMinder (with no IP address validation).

```xml
<provider>
    <role>federation</role>
    <name>HeaderPreAuth</name>
    <enabled>true</enabled>
</provider>
```

The following table describes the configuration options for the web app security provider:

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Descriptions"></a>Descriptions [](#knox-apache-org-books-knox-2-1-0-user-guide--Descriptions)

| Name | Description | Default |
| --- | --- | --- |
| preauth.validation.method | Optional parameter that indicates the types of trust validation to perform on incoming requests. There could be one or more comma-separated validators defined in this property. If there are multiple validators, Apache Knox validates each validator in the same sequence as it is configured. This works similar to short-circuit AND operation i.e. if any validator fails, Knox does not perform further validation and returns overall failure immediately. Possible values are: null, preauth.default.validation, preauth.ip.validation, custom validator (details described inCustom Validator). Failure results in a 403 forbidden HTTP status response. | null - which means ‘preauth.default.validation’ that is no validation will be performed and that we are assuming that the network security and external authentication system is sufficient. |
| preauth.ip.addresses | Optional parameter that indicates the list of trusted ip addresses. When preauth.ip.validation is indicated as the validation method this parameter must be provided to indicate the trusted ip address set. Wildcarded IPs may be used to indicate subnet level trust. ie. 127.0.* | null - which means that no validation will be performed. |
| preauth.custom.header | Required parameter for indicating a custom header to use for extracting the preauthenticated principal. The value extracted from this header is utilized as the PrimaryPrincipal within the established Subject. An incoming request that is missing the configured header will be refused with a 401 unauthorized HTTP status. | SM_USER for SiteMinder usecase |
| preauth.custom.group.header | Optional parameter for indicating a HTTP header name that contains a comma separated list of groups. These are added to the authenticated Subject as group principals. A missing group header will result in no groups being extracted from the incoming request and a log entry but processing will continue. | null - which means that there will be no group principals extracted from the request and added to the established Subject. |

NOTE: Mutual authentication can be used to establish a strong trust relationship between clients and servers while using the Preauthenticated SSO provider. See the configuration for Mutual Authentication with SSL in this document.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration+for+SiteMinder"></a>Configuration for SiteMinder [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration+for+SiteMinder)

The following is an example of a configuration of the preauthenticated SSO provider that leverages the default SM\_USER header name - assuming use with CA SiteMinder. It further configures the validation based on the IP address from the incoming request.

```xml
<provider>
    <role>federation</role>
    <name>HeaderPreAuth</name>
    <enabled>true</enabled>
    <param><name>preauth.validation.method</name><value>preauth.ip.validation</value></param>
    <param><name>preauth.ip.addresses</name><value>127.0.0.2,127.0.0.1</value></param>
</provider>
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--REST+Invocation+for+SiteMinder"></a>REST Invocation for SiteMinder [](#knox-apache-org-books-knox-2-1-0-user-guide--REST+Invocation+for+SiteMinder)

The following curl command can be used to request a directory listing from HDFS while passing in the expected header SM\_USER.

```bash
curl -k -i --header "SM_USER: guest" -v https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp?op=LISTSTATUS
```

Omitting the `--header "SM_USER: guest"` above will result in a rejected request.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration+for+IBM+Tivoli+AM"></a>Configuration for IBM Tivoli AM [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration+for+IBM+Tivoli+AM)

As an example for configuring the preauthenticated SSO provider for another SSO provider, the following illustrates the values used for IBM’s Tivoli Access Manager:

```xml
<provider>
    <role>federation</role>
    <name>HeaderPreAuth</name>
    <enabled>true</enabled>
    <param><name>preauth.custom.header</name><value>iv_user</value></param>
    <param><name>preauth.custom.group.header</name><value>iv_group</value></param>
    <param><name>preauth.validation.method</name><value>preauth.ip.validation</value></param>
    <param><name>preauth.ip.addresses</name><value>127.0.0.2,127.0.0.1</value></param>
</provider>
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--REST+Invocation+for+Tivoli+AM"></a>REST Invocation for Tivoli AM [](#knox-apache-org-books-knox-2-1-0-user-guide--REST+Invocation+for+Tivoli+AM)

The following curl command can be used to request a directory listing from HDFS while passing in the expected headers of iv\_user and iv\_group. Note that the iv\_group value in this command matches the expected ACL for webhdfs in the above topology file. Changing this from “admin” to “admin2” should result in a 401 unauthorized response.

```bash
curl -k -i --header "iv_user: guest" --header "iv_group: admin" -v https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp?op=LISTSTATUS
```

Omitting the `--header "iv_user: guest"` above will result in a rejected request.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--SSO+Cookie+Provider"></a>SSO Cookie Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--SSO+Cookie+Provider)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Overview"></a>Overview [](#knox-apache-org-books-knox-2-1-0-user-guide--Overview)

The SSOCookieProvider enables the federation of the authentication event that occurred through KnoxSSO. KnoxSSO is a typical SP initiated websso mechanism that sets a cookie to be presented by browsers to participating applications and cryptographically verified.

Knox Gateway needs a pluggable mechanism for consuming these cookies and federating the KnoxSSO authentication event as an asserted identity in its interaction with the Hadoop cluster for REST API invocations. This provider is useful when an application that is integrated with KnoxSSO for authentication also consumes REST APIs through the Knox Gateway.

Based on our understanding of the WebSSO flow it should behave like:

- SSOCookieProvider checks for hadoop-jwt cookie and in its absence redirects to the configured SSO provider URL (knoxsso endpoint)
- The configured Provider on the KnoxSSO endpoint challenges the user in a provider specific way (presents form, redirects to SAML IdP, etc.)
- The authentication provider on KnoxSSO validates the identity of the user through credentials/tokens
- The WebSSO service exchanges the normalized Java Subject into a JWT token and sets it on the response as a cookie named `hadoop-jwt`
- The WebSSO service then redirects the user agent back to the originally requested URL - the requested Knox service subsequent invocations will find the cookie in the incoming request and not need to engage the WebSSO service again until it expires.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--sandbox.json+Topology+Example"></a>sandbox.json Topology Example [](#knox-apache-org-books-knox-2-1-0-user-guide--sandbox.json+Topology+Example)

Configuring one of the cluster topologies to use the SSOCookieProvider instead of the out of the box ShiroProvider would look something like the following:

sso-provider.json

```json
{
  "providers": [
    {
      "role": "federation",
      "name": "SSOCookieProvider",
      "enabled": "true",
      "params": {
        "sso.authentication.provider.url": "https://localhost:9443/gateway/idp/api/v1/websso"
      }
    }
  ]
}
```

sandbox.json

```json
{
  "provider-config-ref": "sso-provider",
  "services": [
    {
      "name": "WEBHDFS",
      "urls": [
        "http://localhost:50070/webhdfs"
      ]
    },
    {
      "name": "WEBHCAT",
      "urls": [
        "http://localhost:50111/templeton"
      ]
    }
  ]
}
```

The following table describes the configuration options for the sso cookie provider:

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Descriptions"></a>Descriptions [](#knox-apache-org-books-knox-2-1-0-user-guide--Descriptions)

| Name | Description | Default |
| --- | --- | --- |
| sso.authentication.provider.url | Required parameter that indicates the location of the KnoxSSO endpoint and where to redirect the useragent when no SSO cookie is found in the incoming request. | N/A |
| sso.token.verification.pem | Optional parameter that specifies public key used to validate hadoop-jwt token. The key must be in PEM encoded format excluding the header and footer lines. | N/A |
| sso.expected.audiences | Optional parameter used to constrain the use of tokens on this endpoint to those that have tokens with at least one of the configured audience claims. | N/A |
| sso.unauthenticated.path.list | Optional - List of paths that should bypass the SSO flow. | favicon.ico |
| sso.use.original.url.from.header | Optional - Enable/disable the original URL from header feature which supports extracting the original URL from HTTP request headers instead of URL query param | false |
| sso.original.url.from.header.name | Optional - Name of the header containing the original URL for original URL from header feature. | X-Original-URL |
| sso.original.url.from.header.verify.domain | Optional - Enable domain validation for security for original URL from header feature. | false |
| sso.original.url.from.header.domain.whitelist | Optional - Comma-separated list of allowed domains for original URL from header feature. | N/A |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--JWT+Provider"></a>JWT Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--JWT+Provider)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Overview"></a>Overview [](#knox-apache-org-books-knox-2-1-0-user-guide--Overview)

The JWT federation provider accepts JWT tokens as Bearer tokens within the Authorization header of the incoming request. Upon successfully extracting and verifying the token, the request is then processed on behalf of the user represented by the JWT token.

This provider is closely related to the [Knox Token Service](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+Configuration) and is essentially the provider that is used to consume the tokens issued by the [Knox Token Service](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+Configuration).

Typical deployments have the KnoxToken service defined in a topology that authenticates users based on username and password with the ShiroProvider. They also have another topology dedicated to clients that wish to use KnoxTokens to access Hadoop resources through Knox. The following provider configuration can be used with such a topology.

```text
"providers": [
  {
    "role": "federation",
    "name": "JWTProvider",
    "enabled": "true",
    "params": {
      "knox.token.audiences": "tokenbased"
    }
  }
]
```

The `knox.token.audiences` parameter above indicates that any token in an incoming request must contain an audience claim called “tokenbased”. In this case, the idea is that the issuing KnoxToken service will be configured to include such an audience claim and that the resulting token is valid to use in the topology that contains configuration like above. This would generally be the name of the topology but you can standardize on anything.

The following table describes the configuration options for the JWT federation provider:

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Descriptions"></a>Descriptions [](#knox-apache-org-books-knox-2-1-0-user-guide--Descriptions)

| Name | Description | Default |
| --- | --- | --- |
| knox.token.audiences | Optional parameter. This parameter allows the administrator to constrain the use of tokens on this endpoint to those that have tokens with at least one of the configured audience claims. These claims have associated configuration within the KnoxToken service as well. This provides an interesting way to make sure that the token issued based on authentication to a particular LDAP server or other IdP is accepted but not others. | N/A |
| knox.token.exp.server-managed | Optional parameter for specifying that server-managed token state should be referenced for evaluating token validity. | false |
| knox.token.verification.pem | Optional parameter that specifies public key used to validate the token. The key must be in PEM encoded format excluding the header and footer lines. | N/A |
| knox.token.use.cookie | Optional parameter that indicates if the JWT token can be retrieved from an HTTP cookie instead of the Authorization header. If this is set totrue, then Knox will first check if thehadoop-jwtcookie (the cookie name is configurable) is available in the request and, if that’s the case, Knox will try to fetch a JWT from that cookie. If the cookie is not present in the request, Knox will continue its authentication flow using the Authorization header. If the cookie is there, but it holds an invalid JWT, then authentication will fail. Sample use cases andcurlcommands are available in thisGitHub Pull Request. | false |
| knox.token.cookie.name | Optional parameter to use a custom cookie name in the request ifknox.token.use.cookie = true. | hadoop-jwt |
| knox.token.allowed.jws.types | WithKNOX-2149, one can define their own JWKS URL which Knox can use for verification. Previous Knox implementations only supported JWTs with"typ: JWT"in their headers (or not type definition at all). In previous JOSE versions, there were other supported types such asat+jwtwhich Knox can support from now on. Please note, this configuration is only applied if token verification goes through the JWKS verification path. | JWT |

The optional `knox.token.exp.server-managed` parameter indicates that Knox is managing the state of tokens it issues (e.g., expiration) external from the token, and this external state should be referenced when validating tokens. This parameter can be ommitted if the global default is configured in gateway-site (see [gateway.knox.token.exp.server-managed](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Server+Configuration)), and matches the requirements of this provider. Otherwise, this provider parameter overrides the gateway configuration for the provider’s deployment.

See the [documentation for the Knox Token service](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+Configuration) for related details.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Pac4j+Provider+-+CAS+/+OAuth+/+SAML+/+OpenID+Connect"></a>Pac4j Provider - CAS / OAuth / SAML / OpenID Connect [](#knox-apache-org-books-knox-2-1-0-user-guide--Pac4j+Provider+-+CAS+/+OAuth+/+SAML+/+OpenID+Connect)

![](https://www.pac4j.org/img/logo-knox.png)

[pac4j](https://github.com/pac4j/pac4j) is a Java security engine to authenticate users, get their profiles and manage their authorizations in order to secure Java web applications.

It supports many authentication mechanisms for UI and web services and is implemented by many frameworks and tools.

For Knox, it is used as a federation provider to support the OAuth, CAS, SAML and OpenID Connect protocols. It must be used for SSO, in association with the KnoxSSO service and optionally with the SSOCookieProvider for access to REST APIs.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--SSO+topology"></a>SSO topology [](#knox-apache-org-books-knox-2-1-0-user-guide--SSO+topology)

To enable SSO for REST API access through the Knox gateway, you need to protect your Hadoop services with the SSOCookieProvider configured to use the KnoxSSO service (sandbox.xml topology):

```xml
<gateway>
  <provider>
    <role>federation</role>
    <name>SSOCookieProvider</name>
    <enabled>true</enabled>
    <param>
      <name>sso.authentication.provider.url</name>
      <value>https://127.0.0.1:8443/gateway/knoxsso/api/v1/websso</value>
    </param>
  </provider>
  <provider>
    <role>identity-assertion</role>
    <name>Default</name>
    <enabled>true</enabled>
  </provider>
</gateway>

<service>
  <role>NAMENODE</role>
  <url>hdfs://localhost:8020</url>
</service>

...
```

and protect the KnoxSSO service by the pac4j provider (knoxsso.xml topology):

```xml
<gateway>
  <provider>
    <role>federation</role>
    <name>pac4j</name>
    <enabled>true</enabled>
    <param>
      <name>pac4j.callbackUrl</name>
      <value>https://127.0.0.1:8443/gateway/knoxsso/api/v1/websso</value>
    </param>
    <param>
      <name>cas.loginUrl</name>
      <value>https://casserverpac4j.herokuapp.com/login</value>
    </param>
  </provider>
  <provider>
    <role>identity-assertion</role>
    <name>Default</name>
    <enabled>true</enabled>
  </provider>
</gateway>

<service>
  <role>KNOXSSO</role>
  <param>
    <name>knoxsso.cookie.secure.only</name>
    <value>true</value>
  </param>
  <param>
    <name>knoxsso.token.ttl</name>
    <value>100000</value>
  </param>
  <param>
     <name>knoxsso.redirect.whitelist.regex</name>
     <value>^https?:\/\/(localhost|127\.0\.0\.1|0:0:0:0:0:0:0:1|::1):[0-9].*$</value>
  </param>
</service>
```

Notice that the pac4j callback URL is the KnoxSSO URL (`pac4j.callbackUrl` parameter). An additional `pac4j.cookie.domain.suffix` parameter allows you to define the domain suffix for the pac4j cookies.

In this example, the pac4j provider is configured to authenticate users via a CAS server hosted at: [https://casserverpac4j.herokuapp.com/login](https://casserverpac4j.herokuapp.com/login).

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Parameters"></a>Parameters [](#knox-apache-org-books-knox-2-1-0-user-guide--Parameters)

You can define the identity provider client/s to be used for authentication with the appropriate parameters - as defined below. When configuring any pac4j identity provider client there is a mandatory parameter that must be defined to indicate the order in which the providers should be engaged with the first in the comma separated list being the default. Consuming applications may indicate their desire to use one of the configured clients with a query parameter called client\_name. When there is no client\_name specified, the default (first) provider is selected.

```xml
<param>
  <name>clientName</name>
  <value>CLIENTNAME[,CLIENTNAME]</value>
</param>
```

Valid client names are: `FacebookClient`, `TwitterClient`, `CasClient`, `SAML2Client` or `OidcClient`

For tests only, you can use a basic authentication where login equals password by defining the following configuration:

```xml
<param>
  <name>clientName</name>
  <value>testBasicAuth</value>
</param>
```

NOTE: This is NOT a secure mechanism and must NOT be used in production deployments.

By default Knox will accept the subject of the returned UserProfile and pass it as the PrimaryPrincipal to the proxied service. If you want to use a different user attribute, you can set the UserProfile attribute name as configuration parameter called pac4j.id\_attribute.

```xml
<param>
  <name>pac4j.id_attribute</name>
  <value>nickname</value>
</param>
```

Otherwise, you can use Facebook, Twitter, a CAS server, a SAML IdP or an OpenID Connect provider by using the following parameters:

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--For+OAuth+support:"></a>For OAuth support: [](#knox-apache-org-books-knox-2-1-0-user-guide--For+OAuth+support:)

| Name | Value |
| --- | --- |
| facebook.id | Identifier of the OAuth Facebook application |
| facebook.secret | Secret of the OAuth Facebook application |
| facebook.scope | Requested scope at Facebook login |
| facebook.fields | Fields returned by Facebook |
| twitter.id | Identifier of the OAuth Twitter application |
| twitter.secret | Secret of the OAuth Twitter application |

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--For+CAS+support:"></a>For CAS support: [](#knox-apache-org-books-knox-2-1-0-user-guide--For+CAS+support:)

| Name | Value |
| --- | --- |
| cas.loginUrl | Login URL of the CAS server |
| cas.protocol | CAS protocol (CAS10,CAS20,CAS20_PROXY,CAS30,CAS30_PROXY,SAML) |

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--For+SAML+support:"></a>For SAML support: [](#knox-apache-org-books-knox-2-1-0-user-guide--For+SAML+support:)

| Name | Value |
| --- | --- |
| saml.keystorePassword | Password of the keystore (storepass) |
| saml.privateKeyPassword | Password for the private key (keypass) |
| saml.keystorePath | Path of the keystore |
| saml.identityProviderMetadataPath | Path of the identity provider metadata |
| saml.maximumAuthenticationLifetime | Maximum lifetime for authentication |
| saml.serviceProviderEntityId | Identifier of the service provider |
| saml.serviceProviderMetadataPath | Path of the service provider metadata |

> Get more details on the [pac4j wiki](https://github.com/pac4j/pac4j/wiki/Clients#saml-support).

The SSO URL in your SAML 2 provider config will need to include a special query parameter that lets the pac4j provider know that the request is coming back from the provider rather than from a redirect from a KnoxSSO participating application. This query parameter is “pac4jCallback=true”.

This results in a URL that looks something like:

```text
https://hostname:8443/gateway/knoxsso/api/v1/websso?pac4jCallback=true&client_name=SAML2Client
```

This also means that the SP Entity ID should also include this query parameter as appropriate for your provider. Often something like the above URL is used for both the SSO URL and SP Entity ID.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--For+OpenID+Connect+support:"></a>For OpenID Connect support: [](#knox-apache-org-books-knox-2-1-0-user-guide--For+OpenID+Connect+support:)

| Name | Value |
| --- | --- |
| oidc.id | Identifier of the OpenID Connect provider |
| oidc.secret | Secret of the OpenID Connect provider |
| oidc.discoveryUri | Direcovery URI of the OpenID Connect provider |
| oidc.useNonce | Whether to use nonce during login process |
| oidc.preferredJwsAlgorithm | Preferred JWS algorithm |
| oidc.maxClockSkew | Max clock skew during login process |
| oidc.customParamKey1 | Key of the first custom parameter |
| oidc.customParamValue1 | Value of the first custom parameter |
| oidc.customParamKey2 | Key of the second custom parameter |
| oidc.customParamValue2 | Value of the second custom parameter |

> Get more details on the [pac4j wiki](https://github.com/pac4j/pac4j/wiki/Clients#openid-connect-support).

In fact, you can even define several identity providers at the same time, the first being chosen by default unless you define a `client_name` parameter to specify it (`FacebookClient`, `TwitterClient`, `CasClient`, `SAML2Client` or `OidcClient`).

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--UI+invocation"></a>UI invocation [](#knox-apache-org-books-knox-2-1-0-user-guide--UI+invocation)

In a browser, when calling your Hadoop service (for example: `https://127.0.0.1:8443/gateway/sandbox/webhdfs/v1/tmp?op=LISTSTATUS`), you are redirected to the identity provider for login. Then, after a successful authentication, your are redirected back to your originally requested URL and your KnoxSSO session is initialized.

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--KnoxSSO+Setup+and+Configuration"></a>KnoxSSO Setup and Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxSSO+Setup+and+Configuration)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Introduction"></a>Introduction [](#knox-apache-org-books-knox-2-1-0-user-guide--Introduction)

---

Authentication of the Hadoop component UIs, and those of the overall ecosystem, is usually limited to Kerberos (which requires SPNEGO to be configured for the user’s browser) and simple/pseudo. This often results in the UIs not being secured - even in secured clusters. This is where KnoxSSO provides value by providing WebSSO capabilities to the Hadoop cluster.

By leveraging the hadoop-auth module in Hadoop common, we have introduced the ability to consume a common SSO cookie for web UIs while retaining the non-web browser authentication through kerberos/SPNEGO. We do this by extending the AltKerberosAuthenticationHandler class which provides the useragent based multiplexing.

We also provide integration guidance within the developers guide for other applications to be able to participate in these SSO capabilities.

The flexibility of the Apache Knox authentication and federation providers allows KnoxSSO to provide a normalization of authentication events through token exchange resulting in a common JWT (JSON WebToken) based token.

KnoxSSO provides an abstraction for integrating any number of authentication systems and SSO solutions and enables participating web applications to scale to those solutions more easily. Without the token exchange capabilities offered by KnoxSSO each component UI would need to integrate with each desired solution on its own. With KnoxSSO they only need to integrate with the single solution and common token.

In addition, KnoxSSO comes with its own form-based IdP. This allows for easily integrating a form-based login with the enterprise AD/LDAP server.

This document describes the overall setup requirements for KnoxSSO and participating applications.

In v2.0.0, the Knox team implemented an extension to the KnoxSSO feature that controls the number of concurrent UI sessions the users can have. For more informstion please check out the [Concurrent Session Verification](#knox-apache-org-books-knox-2-1-0-user-guide--Concurrent+Session+Verification) section below.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Form-based+IdP+Setup"></a>Form-based IdP Setup [](#knox-apache-org-books-knox-2-1-0-user-guide--Form-based+IdP+Setup)

By default the `knoxsso.xml` topology contains an application element for the knoxauth login application. This is a simple single page application for providing a login page and authenticating the user with HTTP basic auth against AD/LDAP.

```xml
<application>
    <name>knoxauth</name>
</application>
```

The Shiro Provider has specialized configuration beyond the typical HTTP Basic authentication requirements for REST APIs or other non-knoxauth applications. You will notice below that there are a couple additional elements - namely, **redirectToUrl** and **restrictedCookies with WWW-Authenticate**. These are used to short-circuit the browser’s HTTP basic dialog challenge so that we can use a form instead.

```xml
<provider>
   <role>authentication</role>
   <name>ShiroProvider</name>
   <enabled>true</enabled>
   <param>
      <name>sessionTimeout</name>
      <value>30</value>
   </param>
   <param>
      <name>redirectToUrl</name>
      <value>/gateway/knoxsso/knoxauth/login.html</value>
   </param>
   <param>
      <name>restrictedCookies</name>
      <value>rememberme,WWW-Authenticate</value>
   </param>
   <param>
      <name>main.ldapRealm</name>
      <value>org.apache.knox.gateway.shirorealm.KnoxLdapRealm</value>
   </param>
   <param>
      <name>main.ldapContextFactory</name>
      <value>org.apache.knox.gateway.shirorealm.KnoxLdapContextFactory</value>
   </param>
   <param>
      <name>main.ldapRealm.contextFactory</name>
      <value>$ldapContextFactory</value>
   </param>
   <param>
      <name>main.ldapRealm.userDnTemplate</name>
      <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
   </param>
   <param>
      <name>main.ldapRealm.contextFactory.url</name>
      <value>ldap://localhost:33389</value>
   </param>
   <param>
      <name>main.ldapRealm.authenticationCachingEnabled</name>
      <value>false</value>
   </param>
   <param>
      <name>main.ldapRealm.contextFactory.authenticationMechanism</name>
      <value>simple</value>
   </param>
   <param>
      <name>urls./**</name>
      <value>authcBasic</value>
   </param>
</provider>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--KnoxSSO+Service+Setup"></a>KnoxSSO Service Setup [](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxSSO+Service+Setup)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--knoxsso.xml+Topology"></a>knoxsso.xml Topology [](#knox-apache-org-books-knox-2-1-0-user-guide--knoxsso.xml+Topology)

To enable KnoxSSO, we use the KnoxSSO topology for exposing an API that can be used to abstract the use of any number of enterprise or customer IdPs. By default, the `knoxsso.xml` file is configured for using the simple KnoxAuth application for form-based authentication against LDAP/AD. By swapping the Shiro authentication provider that is there out-of-the-box with another authentication or federation provider, an admin may leverage many of the existing providers for SSO for the UI components that participate in KnoxSSO.

Just as with any Knox service, the KNOXSSO service is protected by the gateway providers defined above it. In this case, the ShiroProvider is taking care of HTTP Basic Auth against LDAP for us. Once the user authenticates the request processing continues to the KNOXSSO service that will create the required cookie and do the necessary redirects.

The knoxsso.xml topology will result in a KnoxSSO URL that looks something like:

```text
https://{gateway_host}:{gateway_port}/gateway/knoxsso/api/v1/websso
```

This URL is needed when configuring applications that participate in KnoxSSO for a given deployment. We will refer to this as the Provider URL.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--KnoxSSO+Configuration+Parameters"></a>KnoxSSO Configuration Parameters [](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxSSO+Configuration+Parameters)

| Parameter | Description | Default |
| --- | --- | --- |
| knoxsso.cookie.name | This optional setting allows the admin to set the name of the sso cookie to use to represent a successful authentication event. | hadoop-jwt |
| knoxsso.cookie.secure.only | This determines whether the browser is allowed to send the cookie over unsecured channels. This should always be set to true in production systems. If during development a relying party is not running SSL then you can turn this off. Running with it off exposes the cookie and underlying token for capture and replay by others. | The value of the gateway-site property namedssl.enabled(which defaults totrue). |
| knoxsso.cookie.max.age | optional: This indicates that a cookie can only live for a specified amount of time - in seconds. This should probably be left to the default which makes it a session cookie. Session cookies are discarded once the browser session is closed. | session |
| knoxsso.cookie.domain.suffix | optional: This indicates the portion of the request hostname that represents the domain to be used for the cookie domain. For single host development scenarios, the default behavior should be fine. For production deployments, the expected domain should be set and all configured URLs that are related to SSO should use this domain. Otherwise, the cookie will not be presented by the browser to mismatched URLs. | Default cookie domain or a domain derived from a hostname that includes more than 2 dots. |
| knoxsso.token.ttl | This indicates the lifespan of the token within the cookie. Once it expires a new cookie must be acquired from KnoxSSO. This is in milliseconds. The 36000000 in the topology above gives you 10 hrs. | 30000 That is 30 seconds. |
| knoxsso.token.audiences | This is a comma separated list of audiences to add to the JWT token. This is used to ensure that a token received by a participating application knows that the token was intended for use with that application. It is optional. In the event that an application has expected audiences and they are not present the token must be rejected. In the event where the token has audiences and the application has none expected then the token is accepted. | empty |
| knoxsso.redirect.whitelist.regex | A semicolon-delimited list of regular expressions. The incoming originalUrl must match one of the expressions in order for KnoxSSO to redirect to it after authentication. Note that cookie use is still constrained to redirect destinations in the same domain as the KnoxSSO service - regardless of the expressions specified here. | The value of the gateway-site property namedgateway.dispatch.whitelist. If that is not defined, the default allows only relative paths, localhost or destinations in the same domain as the Knox host (with or without SSL). This may need to be opened up for production use and actual participating applications. |
| knoxsso.expected.params | Optional: Comma separated list of query parameters that are expected and consumed by KnoxSSO and will not be passed on to originalUrl | empty |
| knoxsso.signingkey.keystore.name | Optional: name of a JKS keystore in gateway security directory that has required signing key certificate | empty |
| knoxsso.signingkey.keystore.alias | Optional: alias of the signing key certificate in theknoxsso.signingkey.keystore.namekeystore | empty |
| knoxsso.signingkey.keystore.passphrase.alias | Optional: passphrase alias for the signing key certificate | empty |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Participating+Application+Configuration"></a>Participating Application Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Participating+Application+Configuration)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+Configuration+Example"></a>Hadoop Configuration Example [](#knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+Configuration+Example)

The following is used as the KnoxSSO configuration in the Hadoop JWTRedirectAuthenticationHandler implementation. Any participating application will need similar configuration. Since JWTRedirectAuthenticationHandler extends the AltKerberosAuthenticationHandler, the typical Kerberos configuration parameters for authentication are also required.

```xml
<property>
    <name>hadoop.http.authentication.type</name
    <value>org.apache.hadoop.security.authentication.server.JWTRedirectAuthenticationHandler</value>
</property>
```

This is the handler classname in Hadoop auth for JWT token (KnoxSSO) support.

```xml
<property>
    <name>hadoop.http.authentication.authentication.provider.url</name>
    <value>https://c6401.ambari.apache.org:8443/gateway/knoxsso/api/v1/websso</value>
</property>
```

The above property is the SSO provider URL that points to the knoxsso endpoint.

```xml
<property>
    <name>hadoop.http.authentication.public.key.pem</name>
    <value>MIICVjCCAb+gAwIBAgIJAPPvOtuTxFeiMA0GCSqGSIb3DQEBBQUAMG0xCzAJBgNV
  BAYTAlVTMQ0wCwYDVQQIEwRUZXN0MQ0wCwYDVQQHEwRUZXN0MQ8wDQYDVQQKEwZI
  YWRvb3AxDTALBgNVBAsTBFRlc3QxIDAeBgNVBAMTF2M2NDAxLmFtYmFyaS5hcGFj
  aGUub3JnMB4XDTE1MDcxNjE4NDcyM1oXDTE2MDcxNTE4NDcyM1owbTELMAkGA1UE
  BhMCVVMxDTALBgNVBAgTBFRlc3QxDTALBgNVBAcTBFRlc3QxDzANBgNVBAoTBkhh
  ZG9vcDENMAsGA1UECxMEVGVzdDEgMB4GA1UEAxMXYzY0MDEuYW1iYXJpLmFwYWNo
  ZS5vcmcwgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBAMFs/rymbiNvg8lDhsdA
  qvh5uHP6iMtfv9IYpDleShjkS1C+IqId6bwGIEO8yhIS5BnfUR/fcnHi2ZNrXX7x
  QUtQe7M9tDIKu48w//InnZ6VpAqjGShWxcSzR6UB/YoGe5ytHS6MrXaormfBg3VW
  tDoy2MS83W8pweS6p5JnK7S5AgMBAAEwDQYJKoZIhvcNAQEFBQADgYEANyVg6EzE
  2q84gq7wQfLt9t047nYFkxcRfzhNVL3LB8p6IkM4RUrzWq4kLA+z+bpY2OdpkTOe
  wUpEdVKzOQd4V7vRxpdANxtbG/XXrJAAcY/S+eMy1eDK73cmaVPnxPUGWmMnQXUi
  TLab+w8tBQhNbq6BOQ42aOrLxA8k/M4cV1A=</value>
</property>
```

The above property holds the KnoxSSO server’s public key for signature verification. Adding it directly to the config like this is convenient and is easily done through Ambari to existing config files that take custom properties. Config is generally protected as root access only as well - so it is a pretty good solution.

Individual UIs within the Hadoop ecosystem will have similar configuration for participating in the KnoxSSO websso capabilities.

Blogs will be provided on the Apache Knox project site for these usecases as they become available.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--KnoxSSO+Cookie+Invalidation"></a>KnoxSSO Cookie Invalidation [](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxSSO+Cookie+Invalidation)

This feature was implemented in the scope of [KNOX-2691](https://issues.apache.org/jira/browse/KNOX-2691).

The user story is that there is a need for a new feature that would allow a pre-configured superuser to invalidate previously issued Knox SSO tokens for (a) particular user(s) in case there is a malicious attack in terms of one (or more) of those users’ SSO tokens got compromised.

To be able to achieve this goal, the `KNOXSSO` service is modified in a way such that it saves the generated SSO cookie using Knox’s token state service capabilities in case token management is enabled in KNOXSSO’s configuration (using the well-known `knox.token.exp.server-managed=true` parameter, by default this is set to `false` in the relevant topologies).

This is only the SSO cookie generation side of the feature. The verification side also needs to be configured the same way: the `SSOCookieProvider` configuration must have the same parameter to enable this new feature.

It is very important to highlight, that turning this feature on will make previously initiated `KNOX SSO sessions` invalid, therefore the browsers must be closed, and/or the cookies have to be removed. This will ensure new user logins which will be captured by the enabled token state service.

There is another essential configuration when `KNOXSSO` is configured to use the [Pac4J federation filter](#knox-apache-org-books-knox-2-1-0-user-guide--Pac4j+Provider+-+CAS+/+OAuth+/+SAML+/+OpenID+Connect). In this case, the `knox.global.logout.page.url` configuration is a must-have parameter in `gateway-site.xml` which usually points to the logout endpoint of the pre-configured SAML/OIDC callback.

Together with the new [Token Management UI](#knox-apache-org-books-knox-2-1-0-user-guide--Token+Management), pre-configured “superusers” can disable (invalidate) SSO cookies. This will result in forcing the users to log in again, which, for obvious reasons, the malicious user(s) cannot do.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Concurrent+Session+Verification"></a>Concurrent Session Verification [](#knox-apache-org-books-knox-2-1-0-user-guide--Concurrent+Session+Verification)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Overview"></a>Overview [](#knox-apache-org-books-knox-2-1-0-user-guide--Overview)

This feature allows end-users limiting the number of concurrent UI sessions the users can have. In order to reach this goal the users can be sorted out into three groups: non-privileged, privileged, unlimited.

The non-privileged and privileged groups each have a configurable limit, which the members of the group can not exceed. The members of the unlimited group are able to create unlimited number of concurrent sessions.

All of the users, who are not configured in neither the privileged nor in the unlimited group, shall become automatically the member of the non-privileged group.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

The following table shows the relevant gateway-level parameters that are essential for this feature to work. Please note these parameters are not listed above in the Gateway Server Configuration table.

| Parameter | Description | Default |
| --- | --- | --- |
| gateway.service.concurrentsessionverifier.impl | To enable the session verification feature, end-users should set this parameter toorg.apache.knox.gateway.session.control.InMemoryConcurrentSessionVerifier | org.apache.knox.gateway.session.control.EmptyConcurrentSessionVerifier |
| gateway.session.verification.privileged.users | Indicates a list of users that are qualified ‘privileged’. | Empty list |
| gateway.session.verification.unlimited.users | Indicates a list of (super) users that can have as many UI sessions as they want. | Empty list |
| gateway.session.verification.privileged.user.limit | The number of UI sessions a ‘privileged’ user can have | 3 |
| gateway.session.verification.non.privileged.user.limit | The number of UI sessions a ‘non-privileged’ user can have | 2 |
| gateway.session.verification.expired.tokens.cleaning.period | The time period (seconds) about the expired session verification data is removed from the background storage | 1800 |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--How+this+works"></a>How this works [](#knox-apache-org-books-knox-2-1-0-user-guide--How+this+works)

If the verifier is disabled it will not do anything even if the other parameters are configured.

When the verifier is enabled all of the users are considered as a non-privileged user by default and they will not be able to create more concurrent sessions than the non-privileged limit. The same is true after you added someone in the privileged user group: that user will not be able to create more UI sessions than the configured privileged user limit. Whereas the members of the unlimited users group are able to create unlimited number of concurrent sessions even if they are configured in the privileged group as well.

The underlying verifier stores verification data (included JWTs) provided by KnoxSSO and used for login. In order to save resources the expired tokens are deleted periodically. The cleaning period can also be updated thru the `gateway.session.verification.expired.tokens.cleaning.period` parameter.

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+Configuration"></a>KnoxToken Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+Configuration)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Introduction"></a>Introduction [](#knox-apache-org-books-knox-2-1-0-user-guide--Introduction)

---

The Knox Token Service enables the ability for clients to acquire the same JWT token that is used for KnoxSSO with WebSSO flows for UIs to be used for accessing REST APIs. By acquiring the token and setting it as a Bearer token on a request, a client is able to access REST APIs that are protected with the JWTProvider federation provider.

This section describes the overall setup requirements and options for KnoxToken service.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+service"></a>KnoxToken service [](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+service)

The Knox Token Service configuration can be configured in any descriptor/topology, tailored to issue tokens to authenticated users, and constrain the usage of the tokens in a number of ways.

```text
"services": [
  {
    "name": "KNOXTOKEN",
    "params": {
      "knox.token.ttl": "36000000",
      "knox.token.audiences": "tokenbased",
      "knox.token.target.url": "https://localhost:8443/gateway/tokenbased",
      "knox.token.exp.server-managed": "false",
      "knox.token.renewer.whitelist": "admin",
      "knox.token.exp.renew-interval": "86400000",
      "knox.token.exp.max-lifetime": "604800000",
      "knox.token.type": "JWT"
    }
  }
]
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+Configuration+Parameters"></a>KnoxToken Configuration Parameters [](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+Configuration+Parameters)

| Parameter | Description | Default |
| --- | --- | --- |
| knox.token.ttl | This indicates the lifespan (milliseconds) of the token. Once it expires a new token must be acquired from KnoxToken service. The 36000000 in the topology above gives you 10 hrs. | 30000 (30 seconds) |
| knox.token.audiences | This is a comma-separated list of audiences to add to the JWT token. This is used to ensure that a token received by a participating application knows that the token was intended for use with that application. It is optional. In the event that an endpoint has expected audiences and they are not present the token must be rejected. In the event where the token has audiences and the endpoint has none expected then the token is accepted. | empty |
| knox.token.target.url | This is an optional configuration parameter to indicate the intended endpoint for which the token may be used. The KnoxShell token credential collector can pull this URL from a knoxtokencache file to be used in scripts. This eliminates the need to prompt for or hardcode endpoints in your scripts. | n/a |
| knox.token.exp.server-managed | This is an optional configuration parameter to enable/disable server-managed token state, to support the associated token renewal and revocation APIs. | false |
| knox.token.renewer.whitelist | This is an optional configuration parameter to authorize the comma-separated list of users to invoke the associated token renewal and revocation APIs. |  |
| knox.token.exp.renew-interval | This is an optional configuration parameter to specify the amount of time (milliseconds) to be added to a token’s TTL when a renewal request is approved. | 86400000 (24 hours) |
| knox.token.exp.max-lifetime | This is an optional configuration parameter to specify the maximum allowed lifetime (milliseconds) of a token, after which renewal will not be permitted. | 604800000 (7 days) |
| knox.token.type | If this is configured the generated JWT’s header will have this value as thetypproperty |  |
| knox.token.issuer | This is an optional configuration parameter to specify the issuer of a token. | KNOXSSO |

Note that server-managed token state can be configured for all KnoxToken service deployments in gateway-site (see [gateway.knox.token.exp.server-managed](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+Server+Configuration)). If it is configured at the gateway level, then the associated service parameter, if configured, will override the gateway configuration.

Adding the KnoxToken configuration shown above to a topology that is protected with the ShrioProvider is a very simple and effective way to expose an endpoint from which a Knox token can be requested. Once it is acquired it may be used to access resources at intended endpoints until it expires.

The following curl command can be used to acquire a token from the Knox Token service as configured in the sandbox topology:

```bash
curl -ivku guest:guest-password https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token
```

Resulting in a JSON response that contains the token, the expiration and the optional target endpoint:

```text
      `{"access_token":"eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJndWVzdCIsImF1ZCI6InRva2VuYmFzZWQiLCJpc3MiOiJLTk9YU1NPIiwiZXhwIjoxNDg5OTQyMTg4fQ.bcqSK7zMnABEM_HVsm3oWNDrQ_ei7PcMI4AtZEERY9LaPo9dzugOg3PA5JH2BRF-lXM3tuEYuZPaZVf8PenzjtBbuQsCg9VVImuu2r1YNVJlcTQ7OV-eW50L6OTI0uZfyrFwX6C7jVhf7d7YR1NNxs4eVbXpS1TZ5fDIRSfU3MU","target_url":"https://localhost:8443/gateway/tokenbased","token_type":"Bearer ","expires_in":1489942188233}`
```

The following curl example shows how to add a bearer token to an Authorization header:

```bash
curl -ivk -H "Authorization: Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJndWVzdCIsImF1ZCI6InRva2VuYmFzZWQiLCJpc3MiOiJLTk9YU1NPIiwiZXhwIjoxNDg5OTQyMTg4fQ.bcqSK7zMnABEM_HVsm3oWNDrQ_ei7PcMI4AtZEERY9LaPo9dzugOg3PA5JH2BRF-lXM3tuEYuZPaZVf8PenzjtBbuQsCg9VVImuu2r1YNVJlcTQ7OV-eW50L6OTI0uZfyrFwX6C7jVhf7d7YR1NNxs4eVbXpS1TZ5fDIRSfU3MU" https://localhost:8443/gateway/tokenbased/webhdfs/v1/tmp?op=LISTSTATUS
```

If you want tokens to include group membership informations, add a `knox.token.include.groups` query parameter to the URL.

```bash
curl -u admin:admin-password -k "https://localhost:8443/gateway/homepage/knoxtoken/api/v1/token?knox.token.include.groups=true"
```

The response contains the token with the group information:

```json
{
      "sub": "admin",
      "jku": "https://localhost:8443/gateway/homepage/knoxtoken/api/v1/jwks.json",
      "kid": "oigA7mZCwA2d7oimQyUaB0oDAfhI-1Bjq9y1n-Mw_OU",
      "iss": "KNOXSSO",
      "exp": 1649777837,
      "knox.groups": [
        "admin-group2",
        "admin-group1"
      ],
      "managed.token": "true",
      "knox.id": "dfeb8979-7f00-4938-bbff-1bc7574bb53d"
}
```

This feature is enabled by default. If you want to disable it, add the following configuration to the KNOXTOKEN service.

```xml
     <param>
        <name>knox.token.include.groups.allowed</name> <!-- default = true -->
        <value>false</value>
    </param>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+Renewal,+Revocation+and+Enable/Disable+actions"></a>KnoxToken Renewal, Revocation and Enable/Disable actions [](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+Renewal,+Revocation+and+Enable/Disable+actions)

The KnoxToken service supports the renewal and explicit revocation of tokens it has issued. Support for both requires server-managed token state to be enabled with at least one renewer white-listed.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Renewal"></a>Renewal [](#knox-apache-org-books-knox-2-1-0-user-guide--Renewal)

```bash
curl -ivku admin:admin-password -X PUT -d $TOKEN 'https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/renew'
```

The JSON responses include a flag indicating success or failure.

A successful result includes the updated expiration time.

```json
{
  "renewed": "true",
  "expires": "1584278311658"
}
```

Error results include a message describing the reason for failure.

Invalid token

```json
{
  "renewed": "false",
  "error": "Unknown token: 9caf743e-1e0d-4708-a9ac-a684a576067c"
}
```

Unauthorized caller

```json
{
  "renewed": "false",
  "error": "Caller (guest) not authorized to renew tokens."
}
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Revocation"></a>Revocation [](#knox-apache-org-books-knox-2-1-0-user-guide--Revocation)

```bash
curl -ivku admin:admin-password -X DELETE -d $TOKEN 'https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/revoke'
```

The JSON responses include a flag indicating success or failure.

```json
{
  "revoked": "true"
}
```

Error results include a message describing the reason for the failure.

Invalid token

```json
{
  "revoked": "false",
  "error": "Unknown token: 9caf743e-1e0d-4708-a9ac-a684a576067c"
}
```

Unauthorized caller

```json
{
  "revoked": "false",
  "error": "Caller (guest) not authorized to revoke tokens."
}
```

KnoxSSO Cookies must not be revoked

```bash
$ curl -iku admin:admin-password  -H "Content-Type: application/json" -d 'c236d20c-4a05-4cfa-b35e-2ba6dc451de0' -X DELETE https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/revoke
HTTP/1.1 403 Forbidden
Date: Fri, 09 Oct 2023 08:55:25 GMT
Set-Cookie: KNOXSESSIONID=node03e9y0cy8giy31rh00xc1mrcfx0.node0; Path=/gateway/sandbox; Secure; HttpOnly
Expires: Thu, 01 Jan 1970 00:00:00 GMT
Set-Cookie: rememberMe=deleteMe; Path=/gateway/sandbox; Max-Age=0; Expires=Thu, 05-Oct-2023 08:55:25 GMT; SameSite=lax
Content-Type: application/json
Content-Length: 113

{
  "revoked": "false",
  "error": "SSO cookie (c236d20c...2ba6dc451de0) cannot not be revoked.",
  "code": 20
}
```

Revoke multiple tokens in one batch:

```bash
$ curl -iku admin:admin-password -H "Content-Type: application/json" -d '["3c043de7-f9e9-4c1a-b32f-abfbc3dcbcb2","5735f5ae-bddd-4ed1-9383-47a839b9ae2b"]' -X DELETE https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/revokeTokens
HTTP/1.1 200 OK
Date: Tue, 10 Oct 2023 07:20:39 GMT
...

{
  "revoked": "true"
}
```

When revoking multiple tokens, current token state check is executed one by one. This means, if there was at least failed token revocation, the HTTP response will indicate that despite the fact that the rest of the token revocation actions succeeded.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Enable"></a>Enable [](#knox-apache-org-books-knox-2-1-0-user-guide--Enable)

This endpoint added in the scope of [KNOX-2602](https://issues.apache.org/jira/browse/KNOX-2602).

```bash
$ curl -ku admin:admin-password -d "1e2f286e-9df1-4123-8d41-e6af523d6923" -X PUT https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/enable

{
  "setEnabledFlag": "true",
  "isEnabled": "true"
}
```

The JSON responses include a flag (`setEnabledFlag`) indicating success or failure along with the token state after the action is executed (`isEnabled`).

Trying to enable an already enabled token:

```bash
$ curl -ku admin:admin-password -d "1e2f286e-9df1-4123-8d41-e6af523d6923" -X PUT https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/enable
{
  "setEnabledFlag": "false",
  "error": "Token is already enabled"
}
```

Disabled KnoxSSO Cookies must not be (re-)enabled:

```bash
$ curl -iku admin:admin-password  -H "Content-Type: application/json" -d '107824ab-c54d-4db3-b3b5-5c964892ad05' -X PUT https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/enable
HTTP/1.1 400 Bad Request
Date: Fri, 06 Oct 2023 08:57:58 GMT
Set-Cookie: KNOXSESSIONID=node011ejmvgcjnlpl13mchqmqjtdjc1.node0; Path=/gateway/sandbox; Secure; HttpOnly
Expires: Thu, 01 Jan 1970 00:00:00 GMT
Set-Cookie: rememberMe=deleteMe; Path=/gateway/sandbox; Max-Age=0; Expires=Thu, 05-Oct-2023 08:57:58 GMT; SameSite=lax
Content-Type: application/json
Content-Length: 107

{
  "setEnabledFlag": "false",
  "error": "Disabled KnoxSSO Cookies cannot not be enabled",
  "code": 80
}
```

Enable multiple tokens in one batch:

```bash
$ curl -iku admin:admin-password -H "Content-Type: application/json" -d '["3c043de7-f9e9-4c1a-b32f-abfbc3dcbcb2","5735f5ae-bddd-4ed1-9383-47a839b9ae2b"]' -X PUT https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/enableTokens
HTTP/1.1 200 OK
Date: Tue, 10 Oct 2023 07:19:23 GMT
...

{
  "setEnabledFlag": "true",
  "isEnabled": "true"
}
```

When enabling multiple tokens, current token state check is not executed. This means, if you are enabling tokens that were already enabled before the batch operation, they remain enabled.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Disable"></a>Disable [](#knox-apache-org-books-knox-2-1-0-user-guide--Disable)

This endpoint added in the scope of [KNOX-2602](https://issues.apache.org/jira/browse/KNOX-2602).

```bash
$ curl -ku admin:admin-password -d "1e2f286e-9df1-4123-8d41-e6af523d6923" -X PUT https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/disable
{
  "setEnabledFlag": "true",
  "isEnabled": "false"
}
```

The JSON responses include a flag (`setEnabledFlag`) indicating success or failure along with the token state after the action is executed (`isEnabled`).

Trying to enable an already enabled token:

```bash
$ curl -ku admin:admin-password -d "1e2f286e-9df1-4123-8d41-e6af523d6923" -X PUT https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/disable
{
  "setEnabledFlag": "false",
  "error": "Token is already disabled"
}
```

Disable multiple tokens in one batch:

```bash
$ curl -iku admin:admin-password -H "Content-Type: application/json" -d '["3c043de7-f9e9-4c1a-b32f-abfbc3dcbcb2","5735f5ae-bddd-4ed1-9383-47a839b9ae2b"]' -X PUT https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/disableTokens
HTTP/1.1 200 OK
Date: Tue, 10 Oct 2023 07:16:14 GMT
...

{
  "setEnabledFlag": "true",
  "isEnabled": "false"
}
```

When disabling multiple tokens, current token state check is not executed. This means, if you are disabling tokens that were already disabled before the batch operation, they remain disabled.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Fetching+tokens+for+users"></a>Fetching tokens for users [](#knox-apache-org-books-knox-2-1-0-user-guide--Fetching+tokens+for+users)

Fetching tokens by `userName`:

```bash
$ curl -iku admin:admin-password -X GET https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/getUserTokens?userName=admin
HTTP/1.1 200 OK
Date: Tue, 10 Oct 2023 07:02:32 GMT
Set-Cookie: KNOXSESSIONID=node01vfrmf5kpjt0ku6mt9765wwx64.node0; Path=/gateway/sandbox; Secure; HttpOnly
Expires: Thu, 01 Jan 1970 00:00:00 GMT
Set-Cookie: rememberMe=deleteMe; Path=/gateway/sandbox; Max-Age=0; Expires=Mon, 09-Oct-2023 07:02:32 GMT; SameSite=lax
Content-Type: application/json
Content-Length: 822

{"tokens":[{"tokenId":"5244358f-19a3-4834-b16f-aa7ddb2e7fe1","issueTime":"2023-10-10T09:02:03.904+0200","expiration":"2023-10-11T09:02:03.000+0200","maxLifetime":"2023-10-17T09:02:03.904+0200","metadata":{"customMetadataMap":{},"knoxSsoCookie":true,"createdBy":null,"userName":"admin","enabled":true,"comment":null},"issueTimeLong":1696921323904,"expirationLong":1697007723000,"maxLifetimeLong":1697526123904},{"tokenId":"9b37e838-4aa2-43fd-b2f1-b35660b33778","issueTime":"2023-10-10T09:02:14.271+0200","expiration":"2023-10-10T10:02:14.242+0200","maxLifetime":"2023-10-17T09:02:14.271+0200","metadata":{"customMetadataMap":{},"knoxSsoCookie":false,"createdBy":null,"userName":"admin","enabled":true,"comment":"admin token 1"},"issueTimeLong":1696921334271,"expirationLong":1696924934242,"maxLifetimeLong":1697526134271}]}
```

Fetching tokens by `createdBy`:

```bash
$ curl -iku admin:admin-password -X GET https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/getUserTokens?createdBy=admin
HTTP/1.1 200 OK
Date: Tue, 10 Oct 2023 07:05:45 GMT
Set-Cookie: KNOXSESSIONID=node047nn0zjkauc41qzexnjmlhj2j6.node0; Path=/gateway/sandbox; Secure; HttpOnly
Expires: Thu, 01 Jan 1970 00:00:00 GMT
Set-Cookie: rememberMe=deleteMe; Path=/gateway/sandbox; Max-Age=0; Expires=Mon, 09-Oct-2023 07:05:46 GMT; SameSite=lax
Content-Type: application/json
Content-Length: 436

{"tokens":[{"tokenId":"3c043de7-f9e9-4c1a-b32f-abfbc3dcbcb2","issueTime":"2023-10-10T09:02:29.146+0200","expiration":"2023-10-10T10:02:29.127+0200","maxLifetime":"2023-10-17T09:02:29.146+0200","metadata":{"customMetadataMap":{},"knoxSsoCookie":false,"createdBy":"admin","userName":"guest","enabled":true,"comment":"admin token 1 for guest"},"issueTimeLong":1696921349146,"expirationLong":1696924949127,"maxLifetimeLong":1697526149146}]}
```

Fetching tokens by `userNameOrCreatedBy`:

```bash
$ curl -iku admin:admin-password -X GET https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/getUserTokens?userNameOrCreatedBy=admin
HTTP/1.1 200 OK
Date: Tue, 10 Oct 2023 07:07:02 GMT
Set-Cookie: KNOXSESSIONID=node0rt50pq4getaj1s1owcj3pvgfm7.node0; Path=/gateway/sandbox; Secure; HttpOnly
Expires: Thu, 01 Jan 1970 00:00:00 GMT
Set-Cookie: rememberMe=deleteMe; Path=/gateway/sandbox; Max-Age=0; Expires=Mon, 09-Oct-2023 07:07:02 GMT; SameSite=lax
Content-Type: application/json
Content-Length: 1246

{"tokens":[{"tokenId":"5244358f-19a3-4834-b16f-aa7ddb2e7fe1","issueTime":"2023-10-10T09:02:03.904+0200","expiration":"2023-10-11T09:02:03.000+0200","maxLifetime":"2023-10-17T09:02:03.904+0200","metadata":{"customMetadataMap":{},"knoxSsoCookie":true,"createdBy":null,"userName":"admin","enabled":true,"comment":null},"issueTimeLong":1696921323904,"expirationLong":1697007723000,"maxLifetimeLong":1697526123904},{"tokenId":"9b37e838-4aa2-43fd-b2f1-b35660b33778","issueTime":"2023-10-10T09:02:14.271+0200","expiration":"2023-10-10T10:02:14.242+0200","maxLifetime":"2023-10-17T09:02:14.271+0200","metadata":{"customMetadataMap":{},"knoxSsoCookie":false,"createdBy":null,"userName":"admin","enabled":true,"comment":"admin token 1"},"issueTimeLong":1696921334271,"expirationLong":1696924934242,"maxLifetimeLong":1697526134271},{"tokenId":"3c043de7-f9e9-4c1a-b32f-abfbc3dcbcb2","issueTime":"2023-10-10T09:02:29.146+0200","expiration":"2023-10-10T10:02:29.127+0200","maxLifetime":"2023-10-17T09:02:29.146+0200","metadata":{"customMetadataMap":{},"knoxSsoCookie":false,"createdBy":"admin","userName":"guest","enabled":true,"comment":"admin token 1 for guest"},"issueTimeLong":1696921349146,"expirationLong":1696924949127,"maxLifetimeLong":1697526149146}]}
```

Fetching `all` tokens:

```bash
$ curl -iku admin:admin-password -X GET https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/getUserTokens?allTokens=true
HTTP/1.1 200 OK
Date: Tue, 10 Oct 2023 07:08:08 GMT
Set-Cookie: KNOXSESSIONID=node0fctcnhp9fm3w1gq1mc2z993109.node0; Path=/gateway/sandbox; Secure; HttpOnly
Expires: Thu, 01 Jan 1970 00:00:00 GMT
Set-Cookie: rememberMe=deleteMe; Path=/gateway/sandbox; Max-Age=0; Expires=Mon, 09-Oct-2023 07:08:08 GMT; SameSite=lax
Content-Type: application/json
Content-Length: 2048

{"tokens":[{"tokenId":"5244358f-19a3-4834-b16f-aa7ddb2e7fe1","issueTime":"2023-10-10T09:02:03.904+0200","expiration":"2023-10-11T09:02:03.000+0200","maxLifetime":"2023-10-17T09:02:03.904+0200","metadata":{"customMetadataMap":{},"knoxSsoCookie":true,"createdBy":null,"userName":"admin","enabled":true,"comment":null},"issueTimeLong":1696921323904,"expirationLong":1697007723000,"maxLifetimeLong":1697526123904},{"tokenId":"9b37e838-4aa2-43fd-b2f1-b35660b33778","issueTime":"2023-10-10T09:02:14.271+0200","expiration":"2023-10-10T10:02:14.242+0200","maxLifetime":"2023-10-17T09:02:14.271+0200","metadata":{"customMetadataMap":{},"knoxSsoCookie":false,"createdBy":null,"userName":"admin","enabled":true,"comment":"admin token 1"},"issueTimeLong":1696921334271,"expirationLong":1696924934242,"maxLifetimeLong":1697526134271},{"tokenId":"3c043de7-f9e9-4c1a-b32f-abfbc3dcbcb2","issueTime":"2023-10-10T09:02:29.146+0200","expiration":"2023-10-10T10:02:29.127+0200","maxLifetime":"2023-10-17T09:02:29.146+0200","metadata":{"customMetadataMap":{},"knoxSsoCookie":false,"createdBy":"admin","userName":"guest","enabled":true,"comment":"admin token 1 for guest"},"issueTimeLong":1696921349146,"expirationLong":1696924949127,"maxLifetimeLong":1697526149146},{"tokenId":"75f1b921-680d-433d-976f-270a100a1cf9","issueTime":"2023-10-10T09:07:50.871+0200","expiration":"2023-10-11T09:07:50.000+0200","maxLifetime":"2023-10-17T09:07:50.871+0200","metadata":{"customMetadataMap":{},"knoxSsoCookie":true,"createdBy":null,"userName":"sam","enabled":true,"comment":null},"issueTimeLong":1696921670871,"expirationLong":1697008070000,"maxLifetimeLong":1697526470871},{"tokenId":"5735f5ae-bddd-4ed1-9383-47a839b9ae2b","issueTime":"2023-10-10T09:07:55.293+0200","expiration":"2023-10-10T10:07:55.276+0200","maxLifetime":"2023-10-17T09:07:55.293+0200","metadata":{"customMetadataMap":{},"knoxSsoCookie":false,"createdBy":null,"userName":"sam","enabled":true,"comment":"sam token"},"issueTimeLong":1696921675293,"expirationLong":1696925275276,"maxLifetimeLong":1697526475293}]}
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Token+Generation/Management+UIs"></a>Token Generation/Management UIs [](#knox-apache-org-books-knox-2-1-0-user-guide--Token+Generation/Management+UIs)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Overview"></a>Overview [](#knox-apache-org-books-knox-2-1-0-user-guide--Overview)

In Apache Knox v2.0.0 the team added two new UIs that are directly accessible from the Knox Home page:

- Token Generation
- Token Management

By default, the `homepage` topology comes with the `KNOXTOKEN` service enabled with the following attributes:

- token TTL is set to 120 days
- token service is enabled (default to keystore-based token state service)
- the admin user is allowed to renew/revoke tokens

In this topology, homepage, two new applications were added in order to display the above-listed UIs:

- `tokengen`: this is an old-style JSP UI, with a relatively simple JS code included. The source is located in the [gateway-applications](https://github.com/apache/knox/tree/v2.0.0/gateway-applications/src/main/resources/applications/tokengen) Maven sub-module.
- `token-management`: this is an Angular UI. The source is located in its own [knox-token-management-ui](https://github.com/apache/knox/tree/v2.0.0/knox-token-management-ui) Maven sub-module.

On the Knox Home page, you will see a new town in the General Proxy Information table like this:

![](knoxtokenmanagement_homepage.png)

However, the *Integration Token* links are disabled by default, because token integration requires a gateway-level alias - called `knox.token.hash.key` - being created and without that alias, it [does not make sense to show those links](https://github.com/apache/knox/pull/512).

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Creating+the+token+hash+key"></a>Creating the token hash key [](#knox-apache-org-books-knox-2-1-0-user-guide--Creating+the+token+hash+key)

As explained, if you would like to use Knox’s token generation features, you will have to create a gateway-level alias with a 256, 384, or 512-bit length JWK. You can do it in - at least - two different ways:

1. You generate your own MAC (using [this online tool](https://8gwifi.org/jwkfunctions.jsp) for instance) and save it as an alias using Knox CLI.
2. You do it running the following Knox CLI command:  
   `generate-jwk --saveAlias knox.token.hash.key`

The second option involves a newly created Knox CLI command called `generate-jwk`:

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Token+state+service+implementations"></a>Token state service implementations [](#knox-apache-org-books-knox-2-1-0-user-guide--Token+state+service+implementations)

There was an important step the Knox team made to provide more flexibility for our end-users: there are some internal service implementations in Knox that were hard-coded in the Java source code. One of those services is the `Token State` service implementation which you can change in gateway-site.xml going forward by setting the `gateway.service.tokenstate.impl` property to any of:

1. `org.apache.knox.gateway.services.token.impl.DefaultTokenStateService` - keeps all token information in memory, therefore all of this information is lost when Knox is shut down
2. `org.apache.knox.gateway.services.token.impl.AliasBasedTokenStateService` - token information is stored in the gateway credential store. This is a durable option, but not suitable for HA deployments
3. `org.apache.knox.gateway.services.token.impl.JournalBasedTokenStateService` - token information is stored in plain files within `$KNOX_DATA_DIR/security/token-state` folder. This option also provides a durable persistence layer for tokens and it might be good for HA scenarios too (in case of KNOX\_DATA\_DIR is on a shared drive), but the token data is written out in plain text (i.e. not encrypted) so it’s less secure.
4. `org.apache.knox.gateway.services.token.impl.ZookeeperTokenStateService` - this is an extension of the keystore-based approach. In this case, token information is stored in Zookeeper using Knox aliases. The token’s alias name equals to its generated token ID.
5. `org.apache.knox.gateway.services.token.impl.JDBCTokenStateService` - stores token information in relational databases. It’s not only durable, but it’s perfectly fine with HA deployments. Currently, PostgreSQL and MySQL databases are supported.

By default, the `AliasBasedTokenStateService` implementation is used.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuring+the+JDBC+token+state+service"></a>Configuring the JDBC token state service [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuring+the+JDBC+token+state+service)

If you want to use the newly implemented database token management, youâ€™ve to set `gateway.service.tokenstate.impl` in *gateway-site.xml* to `org.apache.knox.gateway.services.token.impl.JDBCTokenStateService`.

Now, that you have configured your token state backend, you need to configure a valid database in *gateway-site.xml*. There are two ways to do that:

1. You either declare database connection properties one-by-one:  
   `gateway.database.type` - should be set to `postgresql` or `mysql`  
   `gateway.database.host` - the host where your DB server is running  
   `gateway.database.port` - the port that your DB server is listening on  
   `gateway.database.name` - the name of the database you are connecting to
2. Or you declare an all-in-one JDBC connection string called `gateway.database.connection.url`. The following value will show you how to connect to an SSL enabled PostgreSQL server:  
   `jdbc:[postgresql://$myPostgresServerHost:5432/postgres?user=postgres&ssl=true&sslmode=verify-full&sslrootcert=/usr/local/var/postgresql@10/data/root.crt](postgresql://$myPostgresServerHost:5432/postgres?user=postgres&ssl=true&sslmode=verify-full&sslrootcert=/usr/local/var/postgresql@10/data/root.crt)`

If your database requires user/password authentication, the following aliases must be saved into the Knox Gatewayâ€™s credential store (\_\_gateway-credentials.jceks):

- `gateway_database_user` - the username
- `gateway_database_password` - the password

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Database+design"></a>Database design [](#knox-apache-org-books-knox-2-1-0-user-guide--Database+design)

![](JDBC_TSS_DB_Design.png)

As you can see, there are only 2 tables:

- `KNOXTOKENS` contains basic information about the generated token
- `KNOX_TOKEN_METADATA` contains an arbitrary number of metadata information for the generated token. At the time of this document being written the following metadata exist:
  - `passcode` - this is the BASE-64 encoded value of the generated passcode token MAC. That is, the BASE-64 decoded value is a generated MAC.
  - `userName` - the logged-in user who generated the token
  - `enabled` - this is a boolean flag indicating that the given token is enabled or not (a *disabled* token cannot be used for authentication purposes)
  - `comment` - this is optional metadata, saved only if the user enters something in the *Comment* input field on the *Token Generation* page (see below)
  - `createdBy` - if the token is an impersonated token, this metadata holds the name if the user who generated the token (see *Token impersonation* below)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Generating+a+token"></a>Generating a token [](#knox-apache-org-books-knox-2-1-0-user-guide--Generating+a+token)

Once you configured the `knox.token.hash.key` alias and optionally customized your token state service, you are all set to generate Knox tokens using the new Token Generation UI:

![](knoxtokenmanagement_token_generation_ui-1.png)

The following sections are displayed on the page:

- status bar: here you can see an informative message on the configured Token State backend. There are 3 different statuses:
  - ERROR: shown in red. This indicates a problem with the service backend which makes the feature not work. Usually, this is visible when end-users configure JDBC token state service, but they make a mistake in their DB settings
  - WARN: displayed in yellow (see above picture). This indicates that the feature is enabled and working, but there are some limitations
  - INFO: displayed in green. This indicates when the token management backend is properly configured for HA and production deployments
- there is an information label explaining the purpose of the token generation page
- comment: this is an *optional* input field that allows end-users to add meaningful comments (mnemonics) to their generated tokens. The maximum length is 255 characters.
- the `Configured maximum lifetime` informs the clients about the `knox.token.ttl` property set in the `homepage` topology (defaults to 120 days). If that property is not set (e.g. someone removes it from he homepage topology), Knox uses a hard-coded value of 30 seconds (aka. default Knox token TTL)
- Custom token lifetime can be set by adjusting the days/hours/minutes spinners. The default configuration will yield one hour.
- Token impersonation: an optional free text input field that makes it possible to generate a token for someone else.
- Clicking the Generate Token button will try to create a token for you.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--About+the+generated+token+TTL"></a>About the generated token TTL [](#knox-apache-org-books-knox-2-1-0-user-guide--About+the+generated+token+TTL)

Out of the box, Knox will display the custom lifetime spinners on the Token Generation page. However, they can be hidden by setting the `knox.token.lifespan.input.enabled` property to `false` in the `homepage` topology. Given that possibility and the configured maximum lifetime the generated token can have the following TTL value:

- there is no configured token TTL and lifespan inputs are disabled -> the default TTL is used (30 seconds)
- there is configured TTL and lifespan inputs are disabled -> the configured TTL is used
- there is configured TTL and lifespan inputs are enabled and lifespan inputs result in a value that is less than or equal to the configured TTL -> the lifespan query param is used
- there is configured TTL and lifespan inputs are enabled and lifespan inputs result in a value that is greater than the configured TTL -> the configured TTL is used

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Successful+token+generation"></a>Successful token generation [](#knox-apache-org-books-knox-2-1-0-user-guide--Successful+token+generation)

![](knoxtokenmanagement_token_generation_ui-successful.png)

On the resulting page there is two sensitive information that you can use in Knox to authenticate your request:

1. **JWT token** - this is the serialized JWT and is fully compatible with the old-style Bearer authorization method. Clicking the `JWT Token` label on the page will copy the value into the clipboard. You might want to use it as the â€˜Tokenâ€™ user:

   `$ curl -ku Token:eyJqa3UiOiJodHRwczpcL1wvbG9jYWxob3N0Ojg0NDNcL2dhdGV3YXlcL2hvbWVwYWdlXC9rbm94dG9rZW5cL2FwaVwvdjFcL2p3a3MuanNvbiIsImtpZCI6IkdsOTZfYTM2MTJCZWFsS2tURFRaOTZfVkVsLVhNRVRFRmZuNTRMQ1A2UDQiLCJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiIsImprdSI6Imh0dHBzOlwvXC9sb2NhbGhvc3Q6ODQ0M1wvZ2F0ZXdheVwvaG9tZXBhZ2VcL2tub3h0b2tlblwvYXBpXC92MVwvandrcy5qc29uIiwia2lkIjoiR2w5Nl9hMzYxMkJlYWxLa1REVFo5Nl9WRWwtWE1FVEVGZm41NExDUDZQNCIsImlzcyI6IktOT1hTU08iLCJleHAiOjE2MzY2MjU3MTAsIm1hbmFnZWQudG9rZW4iOiJ0cnVlIiwia25veC5pZCI6ImQxNjFjYWMxLWY5M2UtNDIyOS1hMGRkLTNhNzdhYjkxNDg3MSJ9.e_BNPf_G1iBrU0m3hul5VmmSbpw0w1pUAXl3czOcuxFOQ0Tki-Gq76fCBFUNdKt4QwLpNXxM321cH1TeMG4IhL-92QORSIZgRxY4OUtUgERzcU7-27VNYOzJbaRCjrx-Vb4bSriRJJDwbbXyAoEw_bjiP8EzFFJTPmGcctEzrOLWFk57cLO-2QLd2nbrNd4qmrRR6sEfP81Jg8UL-Ptp66vH_xalJJWuoyoNgGRmH8IMdLVwBgeLeVHiI7NmokuhO-vbctoEwV3Rt4pMpA0VSWGFN0MI4WtU0crjXXHg8U9xSZyOeyT3fMZBXctvBomhGlWaAvuT5AxQGyMMP3VLGw https:/localhost:8443/gateway/sandbox/webhdfs/v1?op=LISTSTATUS` `{"FileStatuses":{"FileStatus":[{"accessTime":0,"blockSize":0,"childrenNum":1,"fileId":16386,"group":"supergroup","length":0,"modificationTime":1621238405734,"owner":"hdfs","pathSuffix":"tmp","permission":"1777","replication":0,"storagePolicy":0,"type":"DIRECTORY"},{"accessTime":0,"blockSize":0,"childrenNum":1,"fileId":16387,"group":"supergroup","length":0,"modificationTime":1621238326078,"owner":"hdfs","pathSuffix":"user","permission":"755","replication":0,"storagePolicy":0,"type":"DIRECTORY"}]}}`
2. **Passcode token** - this is the serialized passcode token, which you can use as the â€˜Passcodeâ€™ user (Clicking the `Passcode Token` label on the page will copy the value into the clipboard):

   `$ curl -ku Passcode:WkRFMk1XTmhZekV0WmprelpTMDBNakk1TFdFd1pHUXRNMkUzTjJGaU9URTBPRGN4OjpPVEV5Tm1KbFltUXROVEUyWkMwME9HSTBMVGd4TTJZdE1HRmxaalJrWlRVNFpXRTA= https://localhost:8443/gateway/sandbox/webhdfs/v1?op=LISTSTATUS` `{"FileStatuses":{"FileStatus":[{"accessTime":0,"blockSize":0,"childrenNum":1,"fileId":16386,"group":"supergroup","length":0,"modificationTime":1621238405734,"owner":"hdfs","pathSuffix":"tmp","permission":"1777","replication":0,"storagePolicy":0,"type":"DIRECTORY"},{"accessTime":0,"blockSize":0,"childrenNum":1,"fileId":16387,"group":"supergroup","length":0,"modificationTime":1621238326078,"owner":"hdfs","pathSuffix":"user","permission":"755","replication":0,"storagePolicy":0,"type":"DIRECTORY"}]}}`

The reason, we needed to support the shorter `Passcode token`, is that there are 3rd party tools where the long JWT exceeds input fields limitations so we need to address this issue with shorter token values.

The rest of the fields are complementary information such as the expiration date/time of the generated token or the user who created it.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Token+generation+failed"></a>Token generation failed [](#knox-apache-org-books-knox-2-1-0-user-guide--Token+generation+failed)

If there was an error during token generation, you will see a failure right under the input field boxes (above the Generate Token button):

![](knoxtokenmanagement_token_generation_ui-fail.png)

The above error message indicates a failure that the admin user already generated more tokens than they are allowed to. This limitation is configurable in the `gateway-site.xml`:

- `gateway.knox.token.limit.per.user` - indicates the maximum number of tokens a user can manage at the same time. `-1` means that users are allowed to create/manage as many tokens as they want. This configuration only applies when the server-managed token state is enabled either in `gateway-site` or at the `topology` level. Defaults to 10.

This behavior can be changed by setting the following `KNOXTOKEN` service-level parameter called `knox.token.user.limit.exceeded.action`. This property may have the following values:

- `REMOVE_OLDEST` - if thatâ€™s configured, the oldest token of the user, who the token is being generated for, will be removed
- `RETURN_ERROR` - if thatâ€™s configured, Knox will return an error response with 403 error code (as it did in previous versions)

The default value is `RETURN_ERROR`.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Token+Management"></a>Token Management [](#knox-apache-org-books-knox-2-1-0-user-guide--Token+Management)

In addition to the token generation UI, Knox comes with the Token Management UI where logged-in users can see all the active tokens that were generated before. That is, if a token got expired and was removed from the underlying token store, it won’t be displayed here. Based on a configuration you can find below, users can see only their tokens or all of them.

![](knoxtokenmanagement_token_management_ui-1.png)

On this page, you will a table with the following information:

1. Each row starts with a selection checkbox for batch operations (except for disabled KnoxSSO cookies, as there is no point in doing anything with them)
2. A unique token identifer. Disabled token’s Token ID value is shown in orange
3. Information on when the token was created and when it will expire
   1. if the token is already expired, the expiration time is shown in red
   2. if the token is still valid, the expiration time is shown in green
4. Username indicates the user for whom the token is created for
5. Impersonated is a boolean flag indicating if this is an impersonated token:
   1. green check: yes, this is impersonated. You’ll see the user who created the token under the icon
   2. red cross: no, this is not an impersonated token
6. KnoxSSO is another boolean flag that indicates if this token is created by the `KNOXSSO` service if the feature was enabled
   1. green check: yes, this is KnoxSSO cookie (token)
   2. red cross: no, this is not a KnoxSSO cookie (it was created by a regular token API call or on the Token Generation page)
7. In the Actions column you will see
   1. the enable/disable/revoke actions are visible for impersonated tokens too
   2. KnoxSSO cookies cannot be revoked nor re-enabled

In order to refresh the table, you can use the `Refresh icon` above the table (if you generated tokens on another tab for instance).

**Batch operations**

When at least one token is selected, the following buttons are shown under the table:

- `Disable Selected Tokens`: when executed, all the selected tokens become disabled (if they were disabled originally, they will remain disabled)
- `Enable Selected Tokens`: when executed, all the selected tokens become enabled (if they were enabled originally, they will remain enabled)
- `Revoke Selected Tokens`: when executed, all the selected tokens will be revoked. Please note this option is shown only, if there is no KnoxSSO cookie (token) selected (i.e. batch revocation only works with regular tokens).

**Toggles**

- `Show Disabled KnoxSSO Cookies`: this is true by default. Since disabled KnoxSSO cookies remain in the underlying token state service until they expire, it may bother users to see them in the tokens table. Flipping this toggle button helps to hide them.
- `Show My Tokens Only`: this toggle button is only visible to users, who can see all tokens. By default, this is false. Enabling it will filter the tokens table in a way such that it will contain tokens only that were generated for the logged in user (impersonated or not).

**Configuration**

By default, logged in users can see token that were generated by them or for them (in caase of token impersonation). However, you may want to edit the `gateway.knox.token.management.users.can.see.all.tokens` parameter in `gateway-site.xml` to allow other users than `admin` to become such a “superuser”, who can see all tokens on the Token Management UI.

```xml
 <property>
    <name>gateway.knox.token.management.users.can.see.all.tokens</name>
    <value>admin</value>
    <description>A comma-separated list of user names who can see all tokens on the Token Management page</description>
</property>
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Token+impersonation"></a>Token impersonation [](#knox-apache-org-books-knox-2-1-0-user-guide--Token+impersonation)

On the token generation page end-users can generate tokens on behalf of other users by specifying the desired user name in the token `impersonation` field. The following screenshot sows a successful token generation for user `tom` (the logged in user is `admin`).

![](knoxtokenmanagement_token_generation_ui-successful-doas.png)

For this to work, the topology has to be configured with the HadoopAuth authentication provider, or an identity assertion provider where impersonation is enabled In both cases, `doAs` support will only work with a valid Hadoop proxyuser configuration (see [Hadoop Proxyuser impersonation](#knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+Proxyuser+impersonation) above)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Token+metadata"></a>Token metadata [](#knox-apache-org-books-knox-2-1-0-user-guide--Token+metadata)

As indicated above, the `KNOXTOKEN` service maintains some hard-coded token metadata out-of-the-box:

- userName
- comment
- enabled
- passcode
- createdBy (in case of impersonated tokens)

In v2.0.0, the Knox team implemented a change in this service that allows end-users to add accept query parameters starting with the `md_` prefix and treat them as Knox Token Metadata.

For instance:

`curl -iku admin:admin-password -X GET 'https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token?md_notebookName=accountantKnoxToken&md_souldBeRemovedBy=31March2022&md_otherMeaningfuMetadata=KnoxIsCool'` When such a token is created by Knox, we should save the following metadata too:

- notebookName=accountantKnoxToken
- shouldBeRemovedBy=31March2022
- otherMeaningfulMetadata=KnoxIsCool

It’s not only Knox can save these metadata, but the Knox’s existing `getUserTokens API` endpoint is able to fetch basic token information using the supplied metadata name besides the user name information.

It’s important to note the following: the `getUserTokens` API returns tokens if *any* of the supplied metadata exists for the given token. Metadata values may or may not be matched: you can either use the `*` wildcard to match all metadata values with a given name *or* you can further filter the stored metadata information by specifying the desired value.

For instance:

`curl -iku admin:admin-password -X GET 'https://localhost:8443/gateway/sandbox/knoxtoken/api/v1/token/getUserTokens?userName=admin&md_notebookName=accountantKnoxToken&md_name=*'`

will return all Knox tokens where metadata with `notebookName` exists and equals `accountantKnoxToken` OR metadata with `name` exists.

Another sample:

1. Create token1 with `md_Name=reina&md_Score=50`
2. Create token2 with `md_Name=mary&md_Score=100`
3. Create token3 with `md_Name=mary&md_Score=20&md_Grade=A`

The following table shows the returned token(s) in case metadata filtering is added in the `getUserTokens` API:

| Metadata | Token returned |
| --- | --- |
| md_Name=reina | token1 |
| md_Name=mary | token2 and token3 |
| md_Score=100 | token2 |
| md_Name=mary&md_Score=20 | token2 and token3 |
| md_Name=mary&md_Name=reina | token1, token2 and token3 |
| md_Name=* | token1, token2 and token3 |
| md_Uknown=* | Empty list |

You may want to check out [GitHub Pull Request #542](https://github.com/apache/knox/pull/542) for sample `curl` commands.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Mutual+Authentication+with+SSL"></a>Mutual Authentication with SSL [](#knox-apache-org-books-knox-2-1-0-user-guide--Mutual+Authentication+with+SSL)

To establish a stronger trust relationship between client and server, we provide mutual authentication with SSL via client certs. This is particularly useful in providing additional validation for Preauthenticated SSO with HTTP Headers. Rather than just IP address validation, connections will only be accepted by Knox from clients presenting trusted certificates.

This behavior is configured for the entire gateway instance within the gateway-site.xml file. All topologies deployed within the configured gateway instance will require incoming connections to present trusted client certificates during the SSL handshake. Otherwise, connections will be refused.

The following table describes the configuration elements related to mutual authentication and their defaults:

| Configuration Element | Description |
| --- | --- |
| gateway.client.auth.needed | True\|False - indicating the need for client authentication. Default is False. |
| gateway.truststore.path | Fully qualified path to the trust store to use. Default is the keystore used to hold the Gateway’s identity. Seegateway.tls.keystore.path. |
| gateway.truststore.type | Keystore type of the trust store. Default is JKS. |
| gateway.truststore.password.alias | Alias for the password to the trust store. |
| gateway.trust.all.certs | Allows for all certificates to be trusted. Default is false. |

By only indicating that it is needed with `gateway.client.auth.needed`, the keystore identified by `gateway.tls.keystore.path` is used. By default this is `{GATEWAY_HOME}/data/security/keystores/gateway.jks`. This is the identity keystore for the server, which can also be used as the truststore. To use a dedicated truststore, `gateway.truststore.path` may be set to the absolute path of the truststore file.  
The type of truststore file should be set using `gateway.truststore.type`; else, JKS will be assumed.  
If the truststore password is different from the Gateway’s master secret then it can be set using

```text
knoxcli.sh create-alias {password-alias} --value {pwd} 
```

The password alias name (`{password-alias}`) is set using `gateway.truststore.password.alias`; else, the alias name of “gateway-truststore-password” should be used.  
If a password is not found using the provided (or default) alias name, then the Gateway’s master secret will be used.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Exclude+a+Topology+from+mTLS"></a>Exclude a Topology from mTLS [](#knox-apache-org-books-knox-2-1-0-user-guide--Exclude+a+Topology+from+mTLS)

There is a possibility to exclude specific topologies from mutual authentication.

| Configuration Element | Description |
| --- | --- |
| gateway.client.auth.needed | True - Indicating the need for client authentication. |
| gateway.port.mapping.enabled | True - Enabling the port mapping feature. It is turned on by default. |
| gateway.port.mapping.{topologyName} | The port number that this topology will listen on. |
| gateway.client.auth.exclude | The names of the topologies separated by comma. These topologies will be excluded from mTLS. |

To exclude a topology from mTLS we use the port mapping feature. The `gateway.port.mapping.enabled` feature has to be enabled which is the default behaviour and a port number has to be provided for the topology with the `gateway.port.mapping.{topologyName}` property. The same topology needs to be added to the `gateway.client.auth.exclude` property.

The below example excludes the `health` topology from mTLS on the 9443 port.

```xml
  <property>
      <name>gateway.port.mapping.health</name>
      <value>9443</value>
      <description>Topology and Port mapping</description>
  </property>
  <property>
      <name>gateway.client.auth.exclude</name>
      <value>health</value>
      <description>Topology excluded from mTLS</description>
  </property>
```

An example how one can access the health topology on port 9443 without mTLS.

```text
 https://{gateway-host}:9443/{gateway-path}/health
```

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--TLS+Client+Certificate+Provider"></a>TLS Client Certificate Provider [](#knox-apache-org-books-knox-2-1-0-user-guide--TLS+Client+Certificate+Provider)

The TLS client certificate authentication provider enables establishing the user based on the client provided TLS certificate. The user will be the DN from the certificate. This provider requires that the gateway is configured to require client authentication with either `gateway.client.auth.wanted` or `gateway.client.auth.needed` ( [Mutual Authentication with SSL](#knox-apache-org-books-knox-2-1-0-user-guide--Mutual+Authentication+with+SSL) ).

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

```xml
<provider>
    <role>authentication</role>
    <name>ClientCert</name>
    <enabled>true</enabled>
</provider>
```

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebSocket+Support"></a>WebSocket Support [](#knox-apache-org-books-knox-2-1-0-user-guide--WebSocket+Support)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Introduction"></a>Introduction [](#knox-apache-org-books-knox-2-1-0-user-guide--Introduction)

WebSocket is a communication protocol that allows full duplex communication over a single TCP connection. Knox provides out-of-the-box support for the WebSocket protocol, currently only text messages are supported.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

By default WebSocket functionality is disabled, it can be easily enabled by changing the `gateway.websocket.feature.enabled` property to `true` in `<KNOX-HOME>/conf/gateway-site.xml` file.

```xml
  <property>
      <name>gateway.websocket.feature.enabled</name>
      <value>true</value>
      <description>Enable/Disable websocket feature.</description>
  </property>
```

Service and rewrite rules need to changed accordingly to match the appropriate websocket context.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Example"></a>Example [](#knox-apache-org-books-knox-2-1-0-user-guide--Example)

In the following sample configuration we assume that the backend WebSocket URL is [ws://myhost:9999/ws](ws://myhost:9999/ws). And ‘gateway.websocket.feature.enabled’ property is set to ‘true’ as shown above.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--rewrite"></a>rewrite [](#knox-apache-org-books-knox-2-1-0-user-guide--rewrite)

Example code snippet from `<KNOX-HOME>/data/services/{myservice}/{version}/rewrite.xml` where myservice = websocket and version = 0.6.0

```xml
  <rules>
    <rule dir="IN" name="WEBSOCKET/ws/inbound" pattern="*://*:*/**/ws">
      <rewrite template="{$serviceUrl[WEBSOCKET]}/ws"/>
    </rule>
  </rules>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--service"></a>service [](#knox-apache-org-books-knox-2-1-0-user-guide--service)

Example code snippet from `<KNOX-HOME>/data/services/{myservice}/{version}/service.xml` where myservice = websocket and version = 0.6.0

```xml
  <service role="WEBSOCKET" name="websocket" version="0.6.0">
    <policies>
          <policy role="webappsec"/>
          <policy role="authentication" name="Anonymous"/>
          <policy role="rewrite"/>
          <policy role="authorization"/>
    </policies>
    <routes>
      <route path="/ws">
          <rewrite apply="WEBSOCKET/ws/inbound" to="request.url"/>
      </route>
    </routes>
  </service>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--topology"></a>topology [](#knox-apache-org-books-knox-2-1-0-user-guide--topology)

Finally, update the topology file at `<KNOX-HOME>/conf/{topology}.xml` with the backend service URL

```xml
  <service>
      <role>WEBSOCKET</role>
      <url>ws://myhost:9999/ws</url>
  </service>
```

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Server-Sent+Events+Support"></a>Server-Sent Events Support [](#knox-apache-org-books-knox-2-1-0-user-guide--Server-Sent+Events+Support)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Introduction"></a>Introduction [](#knox-apache-org-books-knox-2-1-0-user-guide--Introduction)

Server-Sent Events (SSE) allows a server to push updates to the browser over a single longlived HTTP connection. The media type is test/event-stream.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

Asynchronous behaviour and therefore SSE support is not enabled by default. Modify the `gateway.servlet.async.supported` property to `true` in `<KNOX-HOME>/conf/gateway-site.xml` file.

```xml
  <property>
      <name>gateway.servlet.async.supported</name>
      <value>true</value>
      <description>Enable/Disable async support.</description>
  </property>
```

Service and rewrite rules need the be updated as well. Both Ha and non Ha configurations work.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Example"></a>Example [](#knox-apache-org-books-knox-2-1-0-user-guide--Example)

In the following sample configuration we assume that the backend SSE service URL is [http://myhost:7435](http://myhost:7435). And `gateway.servlet.async.supported` property is set to `true` as shown above.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Rewrite"></a>Rewrite [](#knox-apache-org-books-knox-2-1-0-user-guide--Rewrite)

Example code snippet from `<KNOX-HOME>/data/services/{myservice}/{version}/rewrite.xml` where myservice = sservice and version = 0.1

```xml
  <rules>
      <rule dir="IN" name="SSERVICE/sservice/inbound" pattern="*://*:*/**/sservice/*">
        <rewrite template="{$serviceUrl[SSERVICE]}/sse"/>
      </rule>
  </rules>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Service"></a>Service [](#knox-apache-org-books-knox-2-1-0-user-guide--Service)

Example code snippet from `<KNOX-HOME>/data/services/{myservice}/{version}/service.xml` where myservice = sservice and version = 0.1

```xml
  <service role="SSERVICE" name="sservice" version="0.1">
    <routes>
      <route path="/sservice/**"></route>
    </routes>
    <dispatch classname="org.apache.knox.gateway.sse.SSEDispatch" ha-classname="org.apache.knox.gateway.ha.dispatch.SSEHaDispatch"/>
  </service>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Topology"></a>Topology [](#knox-apache-org-books-knox-2-1-0-user-guide--Topology)

Finally, update the topology file at `<KNOX-HOME>/conf/{topology}.xml` with the backend service URL

```xml
  <service>
    <role>SSERVICE</role>
    <url>http://myhost:7435</url>
  </service>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Audit"></a>Audit [](#knox-apache-org-books-knox-2-1-0-user-guide--Audit)

The Audit facility within the Knox Gateway introduces functionality for tracking actions that are executed by Knox per user’s request or that are produced by Knox internal events like topology deploy, etc. The Knox Audit module is based on [Apache log4j](http://logging.apache.org/log4j/1.2/).

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration+needed"></a>Configuration needed [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration+needed)

Out of the box, the Knox Gateway includes preconfigured auditing capabilities. To change its configuration please read the following sections.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Where+audit+logs+go"></a>Where audit logs go [](#knox-apache-org-books-knox-2-1-0-user-guide--Where+audit+logs+go)

The Audit module is preconfigured to write audit records to the log file `{GATEWAY_HOME}/log/gateway-audit.log`.

This behavior can be changed in the `{GATEWAY_HOME}/conf/gateway-log4j.properties` file. `app.audit.file` can be used to change the location. The `log4j.appender.auditfile.*` properties can be used for further customization. For detailed information read the [Apache log4j](http://logging.apache.org/log4j/1.2/) documentation.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Audit+format"></a>Audit format [](#knox-apache-org-books-knox-2-1-0-user-guide--Audit+format)

Out of the box, the audit record format is defined by `org.apache.knox.gateway.audit.log4j.layout.AuditLayout`. Its structure is as follows:

```text
EVENT_PUBLISHING_TIME ROOT_REQUEST_ID|PARENT_REQUEST_ID|REQUEST_ID|LOGGER_NAME|TARGET_SERVICE_NAME|USER_NAME|PROXY_USER_NAME|SYSTEM_USER_NAME|ACTION|RESOURCE_TYPE|RESOURCE_NAME|OUTCOME|LOGGING_MESSAGE
```

The audit record format can be changed by setting `log4j.appender.auditfile.layout` property in `{GATEWAY_HOME}/conf/gateway-log4j.properties` to another class that extends `org.apache.log4j.Layout` or its subclasses.

For detailed information read [Apache log4j](http://logging.apache.org/log4j/1.2/).

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--How+to+interpret+audit+log"></a>How to interpret audit log [](#knox-apache-org-books-knox-2-1-0-user-guide--How+to+interpret+audit+log)

| Component | Description |
| --- | --- |
| EVENT_PUBLISHING_TIME | Time when audit record was published. |
| ROOT_REQUEST_ID | The root request ID if this is a sub-request. Currently it is empty. |
| PARENT_REQUEST_ID | The parent request ID if this is a sub-request. Currently it is empty. |
| REQUEST_ID | A unique value representing the current, active request. If the current request id value is different from the current parent request id value then the current request id value is moved to the parent request id before it is replaced by the provided request id. If the root request id is not set it will be set with the first non-null value of either the parent request id or the passed request id. |
| LOGGER_NAME | The name of the logger |
| TARGET_SERVICE_NAME | Name of Hadoop service. Can be empty if audit record is not linked to any Hadoop service, for example, audit record for topology deployment. |
| USER_NAME | Name of user that initiated session with Knox |
| PROXY_USER_NAME | Mapped user name. For detailed information readIdentity Assertion. |
| SYSTEM_USER_NAME | Currently is empty. |
| ACTION | Type of action that was executed. Following actions are defined: authentication, authorization, redeploy, deploy, undeploy, identity-mapping, dispatch, access. |
| RESOURCE_TYPE | Type of resource for which action was executed. Following resource types are defined: uri, topology, principal. |
| RESOURCE_NAME | Name of resource. For resource of type topology it is name of topology. For resource of type uri it is inbound or dispatch request path. For resource of type principal it is a name of mapped user. |
| OUTCOME | Action result type. Following outcomes are defined: success, failure, unavailable. |
| LOGGING_MESSAGE | Logging message. Contains additional tracking information. |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Audit+log+rotation"></a>Audit log rotation [](#knox-apache-org-books-knox-2-1-0-user-guide--Audit+log+rotation)

Audit logging is preconfigured with `org.apache.log4j.DailyRollingFileAppender`. [Apache log4j](http://logging.apache.org/log4j/1.2/) contains information about other Appenders.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--How+to+change+the+audit+level+or+disable+it"></a>How to change the audit level or disable it [](#knox-apache-org-books-knox-2-1-0-user-guide--How+to+change+the+audit+level+or+disable+it)

All audit messages are logged at `INFO` level and this behavior can’t be changed.

Disabling auditing can be done by decreasing the log level for the Audit appender or setting it to `OFF`.

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Knox+Auth+Service"></a>Knox Auth Service [](#knox-apache-org-books-knox-2-1-0-user-guide--Knox+Auth+Service)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Introduction"></a>Introduction [](#knox-apache-org-books-knox-2-1-0-user-guide--Introduction)

With workloads moving to containers, it was necessary that Knox supports new ways of authentication needs of containers. As part of this effort, the Knox team developed a new internal service, called `KNOX-AUTH-SERVICE`. This service gathers a collection of public REST API endpoints that allows other developers to integrate Knox in their microservice/DEVOPS architectures using containers (such as docker or k9s).

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

This service can be added to any Knox topology as an internal service as follows:

```xml
<service>
     <role>KNOX-AUTH-SERVICE</role>
     <param>
       <name>preauth.auth.header.actor.id.name</name>
       <value>X-Knox-Actor-ID</value>
     </param>
     <param>
       <name>preauth.auth.header.actor.groups.prefix</name>
       <value>X-Knox-Actor-Groups</value>
     </param>
     <param>
       <name>preauth.group.filter.pattern</name>
       <value>.*</value>
     </param>
     <param>
       <name>auth.bearer.token.env</name>
       <value>BEARER_AUTH_TOKEN</value>
     </param>
</service>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Available+REST+API+endpoints"></a>Available REST API endpoints [](#knox-apache-org-books-knox-2-1-0-user-guide--Available+REST+API+endpoints)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--auth/api/v1/pre"></a>auth/api/v1/pre [](#knox-apache-org-books-knox-2-1-0-user-guide--auth/api/v1/pre)

This REST API endpoint has a very simple job: if a valid principal is found in the incoming request, a header is added to the response (by default `X-Knox-Actor-ID`) with the principal name. In addition, if the authenticated subject has group(s), it (they) will be added as comma-separated entries in the header(s) of the default form of `X-Knox-Actor-Groups-#num`. Each group header has a character limit of 1000 to keep them reasonably sized. The header names can be customized via the `preauth.auth.header.actor.id.name` and `preauth.auth.header.actor.groups.prefix` service parameters.

End users may filter user groups by setting the `preauth.group.filter.pattern` service parameter to a valid regular expression. By default, all the user gropus are added into the `X-Knox-Actor-Groups-#num` header.

Sample `curl` commands are available in this [GitHub Pull Request](https://github.com/apache/knox/pull/625).

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--auth/api/v1/bearer"></a>auth/api/v1/bearer [](#knox-apache-org-books-knox-2-1-0-user-guide--auth/api/v1/bearer)

This REST API enpoint populates the HTTP “Authorization” header with the `Bearer Token` in the HTTP response object obtained from an environment variable. The current implementation assumes that the token is not rotated as it never gets exposed to the end-user. By default, the `BEARER_AUTH_TOKEN` environment variable is expected to hold the Bearer token. This can be customized by configuring the `auth.bearer.token.env` service parameter to the desired value.

Sample `curl` commands are available in this [GitHub Pull Request](https://github.com/apache/knox/pull/627).

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Client+Details"></a>Client Details [](#knox-apache-org-books-knox-2-1-0-user-guide--Client+Details)

The KnoxShell release artifact provides a small footprint client environment that removes all unnecessary server dependencies, configuration, binary scripts, etc. It is comprised a couple different things that empower different sorts of users.

- A set of SDK type classes for providing access to Hadoop resources over HTTP
- A Groovy based DSL for scripting access to Hadoop resources based on the underlying SDK classes
- A KnoxShell Token based Sessions to provide a CLI SSO session for executing multiple scripts

The following sections provide an overview and quickstart for the KnoxShell.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Client+Quickstart"></a>Client Quickstart [](#knox-apache-org-books-knox-2-1-0-user-guide--Client+Quickstart)

The following installation and setup instructions should get you started with using the KnoxShell very quickly.

1. Download a knoxshell-x.x.x.zip or tar file and unzip it in your preferred location `{GATEWAY_CLIENT_HOME}`

   ```text
   home:knoxshell-0.12.0 larry$ ls -l
   total 296
   -rw-r--r--@  1 larry  staff  71714 Mar 14 14:06 LICENSE
   -rw-r--r--@  1 larry  staff    164 Mar 14 14:06 NOTICE
   -rw-r--r--@  1 larry  staff  71714 Mar 15 20:04 README
   drwxr-xr-x@ 12 larry  staff    408 Mar 15 21:24 bin
   drwxr--r--@  3 larry  staff    102 Mar 14 14:06 conf
   drwxr-xr-x+  3 larry  staff    102 Mar 15 12:41 logs
   drwxr-xr-x@ 18 larry  staff    612 Mar 14 14:18 samples
   ```

   | Directory | Description |
   | --- | --- |
   | bin | contains the main knoxshell jar and related shell scripts |
   | conf | only contains log4j config |
   | logs | contains the knoxshell.log file |
   | samples | has numerous examples to help you get started |
2. cd `{GATEWAY_CLIENT_HOME}`
3. Get/setup truststore for the target Knox instance or fronting load balancer
   - As of 1.3.0 release you may use the KnoxShell command buildTrustStore to create the truststore. `
   - if you have access to the server you may also use the command `knoxcli.sh export-cert --type JKS`
   - copy the resulting `gateway-client-identity.jks` to your user home directory
4. Execute the an example script from the `{GATEWAY_CLIENT_HOME}/samples` directory - for instance:
   - `bin/knoxshell.sh samples/ExampleWebHdfsLs.groovy`

     ```text
     home:knoxshell-0.12.0 larry$ bin/knoxshell.sh samples/ExampleWebHdfsLs.groovy
     Enter username: guest
     Enter password:
     [app-logs, apps, mapred, mr-history, tmp, user]
     ```

At this point, you should have seen something similar to the above output - probably with different directories listed. You should get the idea from the above. Take a look at the sample that we ran above:

```java
import groovy.json.JsonSlurper
import org.apache.knox.gateway.shell.Hadoop
import org.apache.knox.gateway.shell.hdfs.Hdfs

import org.apache.knox.gateway.shell.Credentials

gateway = "https://localhost:8443/gateway/sandbox"

credentials = new Credentials()
credentials.add("ClearInput", "Enter username: ", "user")
                .add("HiddenInput", "Enter pas" + "sword: ", "pass")
credentials.collect()

username = credentials.get("user").string()
pass = credentials.get("pass").string()

session = Hadoop.login( gateway, username, pass )

text = Hdfs.ls( session ).dir( "/" ).now().string
json = (new JsonSlurper()).parseText( text )
println json.FileStatuses.FileStatus.pathSuffix
session.shutdown()
```

Some things to note about this sample:

1. The gateway URL is hardcoded
   - Alternatives would be passing it as an argument to the script, using an environment variable or prompting for it with a ClearInput credential collector
2. Credential collectors are used to gather credentials or other input from various sources. In this sample the HiddenInput and ClearInput collectors prompt the user for the input with the provided prompt text and the values are acquired by a subsequent get call with the provided name value.
3. The Hadoop.login method establishes a login session of sorts which will need to be provided to the various API classes as an argument.
4. The response text is easily retrieved as a string and can be parsed by the JsonSlurper or whatever you like

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Build+Truststore+for+use+with+KnoxShell+Client+Applications"></a>Build Truststore for use with KnoxShell Client Applications [](#knox-apache-org-books-knox-2-1-0-user-guide--Build+Truststore+for+use+with+KnoxShell+Client+Applications)

The buildTrustStore command in KnoxShell allows remote clients that only have access to the KnoxShell install to build a local trustore from the server they intend to use. It should be understood that this mechanism is less secure than getting the cert directly from the Knox CLI - as a MITM could present you with a certificate that will be trusted when doing this remotely.

buildTrustStore  - downloads the given gateway server’s public certificate and builds a trust store to be used by KnoxShell example: knoxshell.sh buildTrustStore [https://localhost:8443/](https://localhost:8443/)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Client+Token+Sessions"></a>Client Token Sessions [](#knox-apache-org-books-knox-2-1-0-user-guide--Client+Token+Sessions)

Building on the Quickstart above we will drill into some of the token session details here and walk through another sample.

Unlike the quickstart, token sessions require the server to be configured in specific ways to allow the use of token sessions/federation.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Server+Setup"></a>Server Setup [](#knox-apache-org-books-knox-2-1-0-user-guide--Server+Setup)

1. KnoxToken service should be added to your `sandbox` descriptor - see the [KnoxToken Configuration](#knox-apache-org-books-knox-2-1-0-user-guide--KnoxToken+Configuration)

   ```text
   "services": [
     {
       "name": "KNOXTOKEN",
       "params": {
         "knox.token.ttl": "36000000",
         "knox.token.audiences": "tokenbased",
         "knox.token.target.url": "https://localhost:8443/gateway/tokenbased"
       }
     }
   ]
   ```
2. Include the following in the provider configuration referenced from the `tokenbased` descriptor to accept tokens as federation tokens for access to exposed resources with the [JWTProvider](#knox-apache-org-books-knox-2-1-0-user-guide--JWT+Provider)

   ```text
   "providers": [
     {
       "role": "federation",
       "name": "JWTProvider",
       "enabled": "true",
       "params": {
         "knox.token.audiences": "tokenbased"
       }
     }
   ]
   ```
3. Use the KnoxShell token commands to establish and manage your session

   - bin/knoxshell.sh init [https://localhost:8443/gateway/sandbox](https://localhost:8443/gateway/sandbox) to acquire a token and cache in user home directory
   - bin/knoxshell.sh list to display the details of the cached token, the expiration time and optionally the target url
   - bin/knoxshell destroy to remove the cached session token and terminate the session
4. Execute a script that can take advantage of the token credential collector and target URL

   ```java
   import groovy.json.JsonSlurper
   import java.util.HashMap
   import java.util.Map
   import org.apache.knox.gateway.shell.Credentials
   import org.apache.knox.gateway.shell.KnoxSession
   import org.apache.knox.gateway.shell.hdfs.Hdfs

   credentials = new Credentials()
   credentials.add("KnoxToken", "none: ", "token")
   credentials.collect()

   token = credentials.get("token").string()

   gateway = System.getenv("KNOXSHELL_TOPOLOGY_URL")
   if (gateway == null || gateway.equals("")) {
     gateway = credentials.get("token").getTargetUrl()
   }

   println ""
   println "*****************************GATEWAY INSTANCE**********************************"
   println gateway
   println "*******************************************************************************"
   println ""

   headers = new HashMap()
   headers.put("Authorization", "Bearer " + token)

   session = KnoxSession.login( gateway, headers )

   if (args.length > 0) {
     dir = args[0]
   } else {
     dir = "/"
   }

   text = Hdfs.ls( session ).dir( dir ).now().string
   json = (new JsonSlurper()).parseText( text )
   statuses = json.get("FileStatuses");

   println statuses

   session.shutdown()
   ```

Note the following about the above sample script:

1. Use of the KnoxToken credential collector
2. Use of the targetUrl from the credential collector
3. Optional override of the target url with environment variable
4. The passing of the headers map to the session creation in Hadoop.login
5. The passing of an argument for the ls command for the path to list or default to “/”

Also note that there is no reason to prompt for username and password as long as the token has not been destroyed or expired. There is also no hardcoded endpoint for using the token - it is specified in the token cache or overridden by environment variable.

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Client+DSL+and+SDK+Details"></a>Client DSL and SDK Details [](#knox-apache-org-books-knox-2-1-0-user-guide--Client+DSL+and+SDK+Details)

The lack of any formal SDK or client for REST APIs in Hadoop led to thinking about a very simple client that could help people use and evaluate the gateway. The list below outlines the general requirements for such a client.

- Promote the evaluation and adoption of the Apache Knox Gateway
- Simple to deploy and use on data worker desktops for access to remote Hadoop clusters
- Simple to extend with new commands both by other Hadoop projects and by the end user
- Support the notion of a SSO session for multiple Hadoop interactions
- Support the multiple authentication and federation token capabilities of the Apache Knox Gateway
- Promote the use of REST APIs as the dominant remote client mechanism for Hadoop services
- Promote the sense of Hadoop as a single unified product
- Aligned with the Apache Knox Gateway’s overall goals for security

The result is a very simple DSL ([Domain Specific Language](http://en.wikipedia.org/wiki/Domain-specific_language)) of sorts that is used via [Groovy](http://groovy.codehaus.org) scripts. Here is an example of a command that copies a file from the local file system to HDFS.

*Note: The variables `session`, `localFile` and `remoteFile` are assumed to be defined.*

```text
Hdfs.put(session).file(localFile).to(remoteFile).now()
```

*This work is in very early development but is already very useful in its current state.* *We are very interested in receiving feedback about how to improve this feature and the DSL in particular.*

A note of thanks to [REST-assured](https://code.google.com/p/rest-assured/) which provides a [Fluent interface](http://en.wikipedia.org/wiki/Fluent_interface) style DSL for testing REST services. It served as the initial inspiration for the creation of this DSL.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Assumptions"></a>Assumptions [](#knox-apache-org-books-knox-2-1-0-user-guide--Assumptions)

This document assumes a few things about your environment in order to simplify the examples.

- The JVM is executable as simply `java`.
- The Apache Knox Gateway is installed and functional.
- The example commands are executed within the context of the `GATEWAY_HOME` current directory. The `GATEWAY_HOME` directory is the directory within the Apache Knox Gateway installation that contains the README file and the bin, conf and deployments directories.
- A few examples require the use of commands from a standard Groovy installation. These examples are optional but to try them you will need Groovy [installed](http://groovy.codehaus.org/Installing+Groovy).

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Basics"></a>Basics [](#knox-apache-org-books-knox-2-1-0-user-guide--Basics)

In order for secure connections to be made to the Knox gateway server over SSL, the user will need to trust the certificate presented by the gateway while connecting. The knoxcli command export-cert may be used to get access the gateway-identity cert. It can then be imported into cacerts on the client machine or put into a keystore that will be discovered in:

- The user’s home directory
- In a directory specified in an environment variable: `KNOX_CLIENT_TRUSTSTORE_DIR`
- In a directory specified with the above variable with the keystore filename specified in the variable: `KNOX_CLIENT_TRUSTSTORE_FILENAME`
- Default password “changeit” or password may be specified in environment variable: `KNOX_CLIENT_TRUSTSTORE_PASS`
- Or the JSSE system property `javax.net.ssl.trustStore` can be used to specify its location

The DSL requires a shell to interpret the Groovy script. The shell can either be used interactively or to execute a script file. To simplify use, the distribution contains an embedded version of the Groovy shell.

The shell can be run interactively. Use the command `exit` to exit.

```text
java -jar bin/shell.jar
```

When running interactively it may be helpful to reduce some of the output generated by the shell console. Use the following command in the interactive shell to reduce that output. This only needs to be done once as these preferences are persisted.

```text
set verbosity QUIET
set show-last-result false
```

Also when running interactively use the `exit` command to terminate the shell. Using `^C` to exit can sometimes leaves the parent shell in a problematic state.

The shell can also be used to execute a script by passing a single filename argument.

```text
java -jar bin/shell.jar samples/ExampleWebHdfsPutGet.groovy
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Examples"></a>Examples [](#knox-apache-org-books-knox-2-1-0-user-guide--Examples)

Once the shell can be launched the DSL can be used to interact with the gateway and Hadoop. Below is a very simple example of an interactive shell session to upload a file to HDFS.

```text
java -jar bin/shell.jar
knox:000> session = Hadoop.login( "https://localhost:8443/gateway/sandbox", "guest", "guest-password" )
knox:000> Hdfs.put( session ).file( "README" ).to( "/tmp/example/README" ).now()
```

The `knox:000>` in the example above is the prompt from the embedded Groovy console. If you output doesn’t look like this you may need to set the verbosity and show-last-result preferences as described above in the Usage section.

If you receive an error `HTTP/1.1 403 Forbidden` it may be because that file already exists. Try deleting it with the following command and then try again.

```text
knox:000> Hdfs.rm(session).file("/tmp/example/README").now()
```

Without using some other tool to browse HDFS it is hard to tell that this command did anything. Execute this to get a bit more feedback.

```properties
knox:000> println "Status=" + Hdfs.put( session ).file( "README" ).to( "/tmp/example/README2" ).now().statusCode
Status=201
```

Notice that a different filename is used for the destination. Without this an error would have resulted. Of course the DSL also provides a command to list the contents of a directory.

```text
knox:000> println Hdfs.ls( session ).dir( "/tmp/example" ).now().string
{"FileStatuses":{"FileStatus":[{"accessTime":1363711366977,"blockSize":134217728,"group":"hdfs","length":19395,"modificationTime":1363711366977,"owner":"guest","pathSuffix":"README","permission":"644","replication":1,"type":"FILE"},{"accessTime":1363711375617,"blockSize":134217728,"group":"hdfs","length":19395,"modificationTime":1363711375617,"owner":"guest","pathSuffix":"README2","permission":"644","replication":1,"type":"FILE"}]}}
```

It is a design decision of the DSL to not provide type safe classes for various request and response payloads. Doing so would provide an undesirable coupling between the DSL and the service implementation. It also would make adding new commands much more difficult. See the Groovy section below for a variety capabilities and tools for working with JSON and XML to make this easy. The example below shows the use of JsonSlurper and GPath to extract content from a JSON response.

```java
knox:000> import groovy.json.JsonSlurper
knox:000> text = Hdfs.ls( session ).dir( "/tmp/example" ).now().string
knox:000> json = (new JsonSlurper()).parseText( text )
knox:000> println json.FileStatuses.FileStatus.pathSuffix
[README, README2]
```

*In the future, “built-in” methods to slurp JSON and XML may be added to make this a bit easier.* *This would allow for the following type of single line interaction:*

```text
println Hdfs.ls(session).dir("/tmp").now().json().FileStatuses.FileStatus.pathSuffix
```

Shell sessions should always be ended with shutting down the session. The examples above do not touch on it but the DSL supports the simple execution of commands asynchronously. The shutdown command attempts to ensures that all asynchronous commands have completed before existing the shell.

```text
knox:000> session.shutdown()
knox:000> exit
```

All of the commands above could have been combined into a script file and executed as a single line.

```text
java -jar bin/shell.jar samples/ExampleWebHdfsPutGet.groovy
```

This would be the content of that script.

```java
import org.apache.knox.gateway.shell.Hadoop
import org.apache.knox.gateway.shell.hdfs.Hdfs
import groovy.json.JsonSlurper

gateway = "https://localhost:8443/gateway/sandbox"
username = "guest"
password = "guest-password"
dataFile = "README"

session = Hadoop.login( gateway, username, password )
Hdfs.rm( session ).file( "/tmp/example" ).recursive().now()
Hdfs.put( session ).file( dataFile ).to( "/tmp/example/README" ).now()
text = Hdfs.ls( session ).dir( "/tmp/example" ).now().string
json = (new JsonSlurper()).parseText( text )
println json.FileStatuses.FileStatus.pathSuffix
session.shutdown()
exit
```

Notice the `Hdfs.rm` command. This is included simply to ensure that the script can be rerun. Without this an error would result the second time it is run.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Futures"></a>Futures [](#knox-apache-org-books-knox-2-1-0-user-guide--Futures)

The DSL supports the ability to invoke commands asynchronously via the later() invocation method. The object returned from the `later()` method is a `java.util.concurrent.Future` parameterized with the response type of the command. This is an example of how to asynchronously put a file to HDFS.

```properties
future = Hdfs.put(session).file("README").to("/tmp/example/README").later()
println future.get().statusCode
```

The `future.get()` method will block until the asynchronous command is complete. To illustrate the usefulness of this however multiple concurrent commands are required.

```properties
readmeFuture = Hdfs.put(session).file("README").to("/tmp/example/README").later()
licenseFuture = Hdfs.put(session).file("LICENSE").to("/tmp/example/LICENSE").later()
session.waitFor( readmeFuture, licenseFuture )
println readmeFuture.get().statusCode
println licenseFuture.get().statusCode
```

The `session.waitFor()` method will wait for one or more asynchronous commands to complete.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Closures"></a>Closures [](#knox-apache-org-books-knox-2-1-0-user-guide--Closures)

Futures alone only provide asynchronous invocation of the command. What if some processing should also occur asynchronously once the command is complete. Support for this is provided by closures. Closures are blocks of code that are passed into the `later()` invocation method. In Groovy these are contained within `{}` immediately after a method. These blocks of code are executed once the asynchronous command is complete.

```text
Hdfs.put(session).file("README").to("/tmp/example/README").later(){ println it.statusCode }
```

In this example the `put()` command is executed on a separate thread and once complete the `println it.statusCode` block is executed on that thread. The `it` variable is automatically populated by Groovy and is a reference to the result that is returned from the future or `now()` method. The future example above can be rewritten to illustrate the use of closures.

```text
readmeFuture = Hdfs.put(session).file("README").to("/tmp/example/README").later() { println it.statusCode }
licenseFuture = Hdfs.put(session).file("LICENSE").to("/tmp/example/LICENSE").later() { println it.statusCode }
session.waitFor( readmeFuture, licenseFuture )
```

Again, the `session.waitFor()` method will wait for one or more asynchronous commands to complete.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Constructs"></a>Constructs [](#knox-apache-org-books-knox-2-1-0-user-guide--Constructs)

In order to understand the DSL there are three primary constructs that need to be understood.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Session"></a>Session [](#knox-apache-org-books-knox-2-1-0-user-guide--Session)

This construct encapsulates the client side session state that will be shared between all command invocations. In particular it will simplify the management of any tokens that need to be presented with each command invocation. It also manages a thread pool that is used by all asynchronous commands which is why it is important to call one of the shutdown methods.

The syntax associated with this is expected to change. We expect that credentials will not need to be provided to the gateway. Rather it is expected that some form of access token will be used to initialize the session.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--ClientContext"></a>ClientContext [](#knox-apache-org-books-knox-2-1-0-user-guide--ClientContext)

The ClientContext encapsulates the connection parameters, such as the URL, socket timeout parameters, retry configuration and connection pool parameters.

```text
ClientContext context = ClientContext.with("http://localhost:8443");
context.connection().retryCount(2).requestSentRetryEnabled(false).retryIntervalMillis(1000).end();
KnoxSession session = KnoxSession.login(context);
```

- retryCount - how many times to retry; -1 means no retries
- requestSentRetryEnabled - true if it’s OK to retry requests that have been sent
- retryIntervalMillis - The interval between the subsequent auto-retries when the service is unavailable

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Services"></a>Services [](#knox-apache-org-books-knox-2-1-0-user-guide--Services)

Services are the primary extension point for adding new suites of commands. The current built-in examples are: Hdfs, Job and Workflow. The desire for extensibility is the reason for the slightly awkward `Hdfs.ls(session)` syntax. Certainly something more like `session.hdfs().ls()` would have been preferred but this would prevent adding new commands easily. At a minimum it would result in extension commands with a different syntax from the “built-in” commands.

The service objects essentially function as a factory for a suite of commands.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Commands"></a>Commands [](#knox-apache-org-books-knox-2-1-0-user-guide--Commands)

Commands provide the behavior of the DSL. They typically follow a Fluent interface style in order to allow for single line commands. There are really three parts to each command: Request, Invocation, Response

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Request"></a>Request [](#knox-apache-org-books-knox-2-1-0-user-guide--Request)

The request is populated by all of the methods following the “verb” method and the “invoke” method. For example in `Hdfs.rm(session).ls(dir).now()` the request is populated between the “verb” method `rm()` and the “invoke” method `now()`.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Invocation"></a>Invocation [](#knox-apache-org-books-knox-2-1-0-user-guide--Invocation)

The invocation method controls how the request is invoked. Currently supported synchronous and asynchronous invocation. The `now()` method executes the request and returns the result immediately. The `later()` method submits the request to be executed later and returns a future from which the result can be retrieved. In addition `later()` invocation method can optionally be provided a closure to execute when the request is complete. See the Futures and Closures sections below for additional detail and examples.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Response"></a>Response [](#knox-apache-org-books-knox-2-1-0-user-guide--Response)

The response contains the results of the invocation of the request. In most cases the response is a thin wrapper over the HTTP response. In fact many commands will share a single BasicResponse type that only provides a few simple methods.

```java
public int getStatusCode()
public long getContentLength()
public String getContentType()
public String getContentEncoding()
public InputStream getStream()
public String getString()
public byte[] getBytes()
public void close();
```

Thanks to Groovy these methods can be accessed as attributes. In the some of the examples the staticCode was retrieved for example.

```text
println Hdfs.put(session).rm(dir).now().statusCode
```

Groovy will invoke the getStatusCode method to retrieve the statusCode attribute.

The three methods `getStream()`, `getBytes()` and `getString()` deserve special attention. Care must be taken that the HTTP body is fully read once and only once. Therefore one of these methods (and only one) must be called once and only once. Calling one of these more than once will cause an error. Failing to call one of these methods once will result in lingering open HTTP connections. The `close()` method may be used if the caller is not interested in reading the result body. Most commands that do not expect a response body will call close implicitly. If the body is retrieved via `getBytes()` or `getString()`, the `close()` method need not be called. When using `getStream()`, care must be taken to consume the entire body otherwise lingering open HTTP connections will result. The `close()` method may be called after reading the body partially to discard the remainder of the body.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Services"></a>Services [](#knox-apache-org-books-knox-2-1-0-user-guide--Services)

The built-in supported client DSL for each Hadoop service can be found in the [Service Details](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Details) section.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Extension"></a>Extension [](#knox-apache-org-books-knox-2-1-0-user-guide--Extension)

Extensibility is a key design goal of the KnoxShell and client DSL. There are two ways to provide extended functionality for use with the shell. The first is to simply create Groovy scripts that use the DSL to perform a useful task. The second is to add new services and commands. In order to add new service and commands new classes must be written in either Groovy or Java and added to the classpath of the shell. Fortunately there is a very simple way to add classes and JARs to the shell classpath. The first time the shell is executed it will create a configuration file in the same directory as the JAR with the same base name and a `.cfg` extension.

```text
bin/shell.jar
bin/shell.cfg
```

That file contains both the main class for the shell as well as a definition of the classpath. Currently that file will by default contain the following.

```text
main.class=org.apache.knox.gateway.shell.Shell
class.path=../lib; ../lib/*.jar; ../ext; ../ext/*.jar
```

Therefore to extend the shell you should copy any new service and command class either to the `ext` directory or if they are packaged within a JAR copy the JAR to the `ext` directory. The `lib` directory is reserved for JARs that may be delivered with the product.

Below are samples for the service and command classes that would need to be written to add new commands to the shell. These happen to be Groovy source files but could - with very minor changes - be Java files. The easiest way to add these to the shell is to compile them directly into the `ext` directory. *Note: This command depends upon having the Groovy compiler installed and available on the execution path.*

```text
groovy -d ext -cp bin/shell.jar samples/SampleService.groovy \
    samples/SampleSimpleCommand.groovy samples/SampleComplexCommand.groovy
```

These source files are available in the samples directory of the distribution but are included here for convenience.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Sample+Service+(Groovy)"></a>Sample Service (Groovy) [](#knox-apache-org-books-knox-2-1-0-user-guide--Sample+Service+(Groovy))

```java
import org.apache.knox.gateway.shell.Hadoop

class SampleService {

    static String PATH = "/webhdfs/v1"

    static SimpleCommand simple( Hadoop session ) {
        return new SimpleCommand( session )
    }

    static ComplexCommand.Request complex( Hadoop session ) {
        return new ComplexCommand.Request( session )
    }

}
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Sample+Simple+Command+(Groovy)"></a>Sample Simple Command (Groovy) [](#knox-apache-org-books-knox-2-1-0-user-guide--Sample+Simple+Command+(Groovy))

```java
import org.apache.knox.gateway.shell.AbstractRequest
import org.apache.knox.gateway.shell.BasicResponse
import org.apache.knox.gateway.shell.Hadoop
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder

import java.util.concurrent.Callable

class SimpleCommand extends AbstractRequest<BasicResponse> {

    SimpleCommand( Hadoop session ) {
        super( session )
    }

    private String param
    SimpleCommand param( String param ) {
        this.param = param
        return this
    }

    @Override
    protected Callable<BasicResponse> callable() {
        return new Callable<BasicResponse>() {
            @Override
            BasicResponse call() {
                URIBuilder uri = uri( SampleService.PATH, param )
                addQueryParam( uri, "op", "LISTSTATUS" )
                HttpGet get = new HttpGet( uri.build() )
                return new BasicResponse( execute( get ) )
            }
        }
    }

}
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Sample+Complex+Command+(Groovy)"></a>Sample Complex Command (Groovy) [](#knox-apache-org-books-knox-2-1-0-user-guide--Sample+Complex+Command+(Groovy))

```java
import com.jayway.jsonpath.JsonPath
import org.apache.knox.gateway.shell.AbstractRequest
import org.apache.knox.gateway.shell.BasicResponse
import org.apache.knox.gateway.shell.Hadoop
import org.apache.http.HttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.utils.URIBuilder

import java.util.concurrent.Callable

class ComplexCommand {

    static class Request extends AbstractRequest<Response> {

        Request( Hadoop session ) {
            super( session )
        }

        private String param;
        Request param( String param ) {
            this.param = param;
            return this;
        }

        @Override
        protected Callable<Response> callable() {
            return new Callable<Response>() {
                @Override
                Response call() {
                    URIBuilder uri = uri( SampleService.PATH, param )
                    addQueryParam( uri, "op", "LISTSTATUS" )
                    HttpGet get = new HttpGet( uri.build() )
                    return new Response( execute( get ) )
                }
            }
        }

    }

    static class Response extends BasicResponse {

        Response(HttpResponse response) {
            super(response)
        }

        public List<String> getNames() {
            return JsonPath.read( string, "\$.FileStatuses.FileStatus[*].pathSuffix" )
        }

    }

}
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Groovy"></a>Groovy [](#knox-apache-org-books-knox-2-1-0-user-guide--Groovy)

The shell included in the distribution is basically an unmodified packaging of the Groovy shell. The distribution does however provide a wrapper that makes it very easy to setup the class path for the shell. In fact the JARs required to execute the DSL are included on the class path by default. Therefore these command are functionally equivalent if you have Groovy installed. See below for a description of what is required for JARs required by the DSL from `lib` and `dep` directories.

```text
java -jar bin/shell.jar samples/ExampleWebHdfsPutGet.groovy
groovy -classpath {JARs required by the DSL from lib and dep} samples/ExampleWebHdfsPutGet.groovy
```

The interactive shell isn’t exactly equivalent. However the only difference is that the shell.jar automatically executes some additional imports that are useful for the KnoxShell client DSL. So these two sets of commands should be functionality equivalent. *However there is currently a class loading issue that prevents the groovysh command from working properly.*

```java
java -jar bin/shell.jar

groovysh -classpath {JARs required by the DSL from lib and dep}
import org.apache.knox.gateway.shell.Hadoop
import org.apache.knox.gateway.shell.hdfs.Hdfs
import org.apache.knox.gateway.shell.job.Job
import org.apache.knox.gateway.shell.workflow.Workflow
import java.util.concurrent.TimeUnit
```

Alternatively, you can use the Groovy Console which does not appear to have the same class loading issue.

```java
groovyConsole -classpath {JARs required by the DSL from lib and dep}

import org.apache.knox.gateway.shell.Hadoop
import org.apache.knox.gateway.shell.hdfs.Hdfs
import org.apache.knox.gateway.shell.job.Job
import org.apache.knox.gateway.shell.workflow.Workflow
import java.util.concurrent.TimeUnit
```

The JARs currently required by the client DSL are

```text
lib/gateway-shell-{GATEWAY_VERSION}.jar
dep/httpclient-4.3.6.jar
dep/httpcore-4.3.3.jar
dep/commons-lang3-3.4.jar
dep/commons-codec-1.7.jar
```

So on Linux/MacOS you would need this command

```text
groovy -cp lib/gateway-shell-0.10.0.jar:dep/httpclient-4.3.6.jar:dep/httpcore-4.3.3.jar:dep/commons-lang3-3.4.jar:dep/commons-codec-1.7.jar samples/ExampleWebHdfsPutGet.groovy
```

and on Windows you would need this command

```text
groovy -cp lib/gateway-shell-0.10.0.jar;dep/httpclient-4.3.6.jar;dep/httpcore-4.3.3.jar;dep/commons-lang3-3.4.jar;dep/commons-codec-1.7.jar samples/ExampleWebHdfsPutGet.groovy
```

The exact list of required JARs is likely to change from release to release so it is recommended that you utilize the wrapper `bin/shell.jar`.

In addition because the DSL can be used via standard Groovy, the Groovy integrations in many popular IDEs (e.g. IntelliJ, Eclipse) can also be used. This makes it particularly nice to develop and execute scripts to interact with Hadoop. The code-completion features in modern IDEs in particular provides immense value. All that is required is to add the `gateway-shell-{GATEWAY_VERSION}.jar` to the projects class path.

There are a variety of Groovy tools that make it very easy to work with the standard interchange formats (i.e. JSON and XML). In Groovy the creation of XML or JSON is typically done via a “builder” and parsing done via a “slurper”. In addition once JSON or XML is “slurped” the GPath, an XPath like feature build into Groovy can be used to access data.

- XML
  - Markup Builder [Overview](http://groovy.codehaus.org/Creating+XML+using+Groovy's+MarkupBuilder), [API](http://groovy.codehaus.org/api/groovy/xml/MarkupBuilder.html)
  - XML Slurper [Overview](http://groovy.codehaus.org/Reading+XML+using+Groovy's+XmlSlurper), [API](http://groovy.codehaus.org/api/groovy/util/XmlSlurper.html)
  - XPath [Overview](http://groovy.codehaus.org/GPath), [API](http://docs.oracle.com/javase/1.5.0/docs/api/javax/xml/xpath/XPath.html)
- JSON
  - JSON Builder [API](http://groovy.codehaus.org/gapi/groovy/json/JsonBuilder.html)
  - JSON Slurper [API](http://groovy.codehaus.org/gapi/groovy/json/JsonSlurper.html)
  - JSON Path [API](https://code.google.com/p/json-path/)
  - GPath [Overview](http://groovy.codehaus.org/GPath)

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Service+Details"></a>Service Details [](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Details)

In the sections that follow, the integrations currently available out of the box with the gateway will be described. In general these sections will include examples that demonstrate how to access each of these services via the gateway. In many cases this will include both the use of [cURL](http://curl.haxx.se/) as a REST API client as well as the use of the Knox Client DSL. You may notice that there are some minor differences between using the REST API of a given service via the gateway. In general this is necessary in order to achieve the goal of not leaking internal Hadoop cluster details to the client.

Keep in mind that the gateway uses a plugin model for supporting Hadoop services. Check back with the [Apache Knox](https://knox.apache.org) site for the latest news on plugin availability. You can also create your own custom plugin to extend the capabilities of the gateway.

These are the current Hadoop services with built-in support.

- [WebHDFS](#knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS)
- [WebHCat](#knox-apache-org-books-knox-2-1-0-user-guide--WebHCat)
- [Oozie](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie)
- [HBase](#knox-apache-org-books-knox-2-1-0-user-guide--HBase)
- [Hive](#knox-apache-org-books-knox-2-1-0-user-guide--Hive)
- [Yarn](#knox-apache-org-books-knox-2-1-0-user-guide--Yarn)
- [Kafka](#knox-apache-org-books-knox-2-1-0-user-guide--Kafka)
- [Storm](#knox-apache-org-books-knox-2-1-0-user-guide--Storm)
- [Solr](#knox-apache-org-books-knox-2-1-0-user-guide--Solr)
- [Avatica](#knox-apache-org-books-knox-2-1-0-user-guide--Avatica)
- [Cloudera Manager](#knox-apache-org-books-knox-2-1-0-user-guide--Cloudera+Manager)
- [Livy Server](#knox-apache-org-books-knox-2-1-0-user-guide--Livy+Server)
- [Elasticsearch](#knox-apache-org-books-knox-2-1-0-user-guide--Elasticsearch)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Assumptions"></a>Assumptions [](#knox-apache-org-books-knox-2-1-0-user-guide--Assumptions)

This document assumes a few things about your environment in order to simplify the examples.

- The JVM is executable as simply `java`.
- The Apache Knox Gateway is installed and functional.
- The example commands are executed within the context of the `GATEWAY_HOME` current directory. The `GATEWAY_HOME` directory is the directory within the Apache Knox Gateway installation that contains the README file and the bin, conf and deployments directories.
- The [cURL](http://curl.haxx.se/) command line HTTP client utility is installed and functional.
- A few examples optionally require the use of commands from a standard Groovy installation. These examples are optional but to try them you will need Groovy [installed](http://groovy.codehaus.org/Installing+Groovy).
- The default configuration for all of the samples is setup for use with Hortonworks’ [Sandbox](https://hortonworks.com/products/sandbox/) version 2.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Customization"></a>Customization [](#knox-apache-org-books-knox-2-1-0-user-guide--Customization)

Using these samples with other Hadoop installations will require changes to the steps described here as well as changes to referenced sample scripts. This will also likely require changes to the gateway’s default configuration. In particular host names, ports, user names and password may need to be changed to match your environment. These changes may need to be made to gateway configuration and also the Groovy sample script files in the distribution. All of the values that may need to be customized in the sample scripts can be found together at the top of each of these files.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--cURL"></a>cURL [](#knox-apache-org-books-knox-2-1-0-user-guide--cURL)

The cURL HTTP client command line utility is used extensively in the examples for each service. In particular this form of the cURL command line is used repeatedly.

```bash
curl -i -k -u guest:guest-password ...
```

The option `-i` (aka `--include`) is used to output HTTP response header information. This will be important when the content of the HTTP Location header is required for subsequent requests.

The option `-k` (aka `--insecure`) is used to avoid any issues resulting from the use of demonstration SSL certificates.

The option `-u` (aka `--user`) is used to provide the credentials to be used when the client is challenged by the gateway.

Keep in mind that the samples do not use the cookie features of cURL for the sake of simplicity. Therefore each request via cURL will result in an authentication.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS"></a>WebHDFS [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS)

REST API access to HDFS in a Hadoop cluster is provided by WebHDFS or HttpFS. Both services provide the same API. The [WebHDFS REST API](http://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/WebHDFS.html) documentation is available online. WebHDFS must be enabled in the `hdfs-site.xml` configuration file and exposes the API on each NameNode and DataNode. HttpFS however is a separate server to be configured and started separately. In the sandbox this configuration file is located at `/etc/hadoop/conf/hdfs-site.xml`. Note the properties shown below as they are related to configuration required by the gateway. Some of these represent the default values and may not actually be present in `hdfs-site.xml`.

```xml
<property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
</property>
<property>
    <name>dfs.namenode.rpc-address</name>
    <value>sandbox.hortonworks.com:8020</value>
</property>
<property>
    <name>dfs.namenode.http-address</name>
    <value>sandbox.hortonworks.com:50070</value>
</property>
<property>
    <name>dfs.https.namenode.https-address</name>
    <value>sandbox.hortonworks.com:50470</value>
</property>
```

The values above need to be reflected in each topology descriptor file deployed to the gateway. The gateway by default includes a sample topology descriptor file `{GATEWAY_HOME}/deployments/sandbox.xml`. The values in this sample are configured to work with an installed Sandbox VM.

Please also note that the port changed from 50070 to 9870 in Hadoop 3.0.

```xml
<service>
    <role>NAMENODE</role>
    <url>hdfs://localhost:8020</url>
</service>
<service>
    <role>WEBHDFS</role>
    <url>http://localhost:50070/webhdfs</url>
</service>
```

The URL provided for the role NAMENODE does not result in an endpoint being exposed by the gateway. This information is only required so that other URLs can be rewritten that reference the Name Node’s RPC address. This prevents clients from needing to be aware of the internal cluster details.

By default the gateway is configured to use the HTTP endpoint for WebHDFS in the Sandbox. This could alternatively be configured to use the HTTPS endpoint by providing the correct address.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HDFS+NameNode+Federation"></a>HDFS NameNode Federation [](#knox-apache-org-books-knox-2-1-0-user-guide--HDFS+NameNode+Federation)

NameNode federation introduces some additional complexity when determining to which URL(s) Knox should proxy HDFS-related requests.

The HDFS core-site.xml configuration includes additional properties, which represent options in terms of the NameNode endpoints.

| Property Name | Description | Example Value |
| --- | --- | --- |
| dfs.internal.nameservices | The list of defined namespaces | ns1,ns2 |

For each value enumerated by *dfs.internal.nameservices*, there is another property defined, for specifying the associated NameNode names.

| Property Name | Description | Example Value |
| --- | --- | --- |
| dfs.ha.namenodes.ns1 | The NameNode identifiers associated with the ns1 namespace | nn1,nn2 |
| dfs.ha.namenodes.ns2 | The NameNode identifiers associated with the ns2 namespace | nn3,nn4 |

For each namenode name enumerated by each of these properties, there are other properties defined, for specifying the associated host addresses.

| Property Name | Description | Example Value |
| --- | --- | --- |
| dfs.namenode.http-address.ns1.nn1 | The HTTP host address of nn1 NameNode in the ns1 namespace | host1:50070 |
| dfs.namenode.https-address.ns1.nn1 | The HTTPS host address of nn1 NameNode in the ns1 namespace | host1:50470 |
| dfs.namenode.http-address.ns1.nn2 | The HTTP host address of nn2 NameNode in the ns1 namespace | host2:50070 |
| dfs.namenode.https-address.ns1.nn2 | The HTTPS host address of nn2 NameNode in the ns1 namespace | host2:50470 |
| dfs.namenode.http-address.ns2.nn3 | The HTTP host address of nn3 NameNode in the ns2 namespace | host3:50070 |
| dfs.namenode.https-address.ns2.nn3 | The HTTPS host address of nn3 NameNode in the ns2 namespace | host3:50470 |
| dfs.namenode.http-address.ns2.nn4 | The HTTP host address of nn4 NameNode in the ns2 namespace | host4:50070 |
| dfs.namenode.https-address.ns2.nn4 | The HTTPS host address of nn4 NameNode in the ns2 namespace | host4:50470 |

So, if Knox should proxy the NameNodes associated with *ns1*, and the configuration does not dictate HTTPS, then the WEBHDFS service must contain URLs based on the values of *dfs.namenode.http-address.ns1.nn1* and *dfs.namenode.http-address.ns1.nn2*. Likewise, if Knox should proxy the NameNodes associated with *ns2*, the WEBHDFS service must contain URLs based on the values of *dfs.namenode.http-address.ns2.nn3* and *dfs.namenode.http-address.ns2.nn3*.

Fortunately, for Ambari-managed clusters, [descriptors](#knox-apache-org-books-knox-2-1-0-user-guide--Simplified+Descriptor+Files) and service discovery can handle this complexity for administrators. In the descriptor, the service can be declared without any endpoints, and the desired namespace can be specified to disambiguate which endpoint(s) should be proxied by way of a parameter named *discovery-namespace*.

```text
"services": [
  {
    "name": "WEBHDFS",
    "params": {
      "discovery-nameservice": "ns2"
    }
  },
```

If no namespace is specified, then the default namespace will be applied. This default namespace is derived from the value of the property named *fs.defaultFS* defined in the HDFS *core-site.xml* configuration.

  

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+URL+Mapping"></a>WebHDFS URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+URL+Mapping)

For Name Node URLs, the mapping of Knox Gateway accessible WebHDFS URLs to direct WebHDFS URLs is simple.

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/webhdfs |
| --- | --- |
| Cluster | http://{webhdfs-host}:50070/webhdfs |

However, there is a subtle difference to URLs that are returned by WebHDFS in the Location header of many requests. Direct WebHDFS requests may return Location headers that contain the address of a particular DataNode. The gateway will rewrite these URLs to ensure subsequent requests come back through the gateway and internal cluster details are protected.

A WebHDFS request to the NameNode to retrieve a file will return a URL of the form below in the Location header.

```text
http://{datanode-host}:{data-node-port}/webhdfs/v1/{path}?...
```

Note that this URL contains the network location of a DataNode. The gateway will rewrite this URL to look like the URL below.

```text
https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/webhdfs/data/v1/{path}?_={encrypted-query-parameters}
```

The `{encrypted-query-parameters}` will contain the `{datanode-host}` and `{datanode-port}` information. This information along with the original query parameters are encrypted so that the internal Hadoop details are protected.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+Examples"></a>WebHDFS Examples [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+Examples)

The examples below upload a file, download the file and list the contents of the directory.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+via+client+DSL"></a>WebHDFS via client DSL [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+via+client+DSL)

You can use the Groovy example scripts and interpreter provided with the distribution.

```text
java -jar bin/shell.jar samples/ExampleWebHdfsPutGet.groovy
java -jar bin/shell.jar samples/ExampleWebHdfsLs.groovy
```

You can manually type the client DSL script into the KnoxShell interactive Groovy interpreter provided with the distribution. The command below starts the KnoxShell in interactive mode.

```text
java -jar bin/shell.jar
```

Each line below could be typed or copied into the interactive shell and executed. This is provided as an example to illustrate the use of the client DSL.

```java
// Import the client DSL and a useful utilities for working with JSON.
import org.apache.knox.gateway.shell.Hadoop
import org.apache.knox.gateway.shell.hdfs.Hdfs
import groovy.json.JsonSlurper

// Setup some basic config.
gateway = "https://localhost:8443/gateway/sandbox"
username = "guest"
password = "guest-password"

// Start the session.
session = Hadoop.login( gateway, username, password )

// Cleanup anything leftover from a previous run.
Hdfs.rm( session ).file( "/user/guest/example" ).recursive().now()

// Upload the README to HDFS.
Hdfs.put( session ).file( "README" ).to( "/user/guest/example/README" ).now()

// Download the README from HDFS.
text = Hdfs.get( session ).from( "/user/guest/example/README" ).now().string
println text

// List the contents of the directory.
text = Hdfs.ls( session ).dir( "/user/guest/example" ).now().string
json = (new JsonSlurper()).parseText( text )
println json.FileStatuses.FileStatus.pathSuffix

// Cleanup the directory.
Hdfs.rm( session ).file( "/user/guest/example" ).recursive().now()

// Clean the session.
session.shutdown()
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+via+cURL"></a>WebHDFS via cURL [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+via+cURL)

Users can use cURL to directly invoke the REST APIs via the gateway.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Optionally+cleanup+the+sample+directory+in+case+a+previous+example+was+run+without+cleaning+up."></a>Optionally cleanup the sample directory in case a previous example was run without cleaning up. [](#knox-apache-org-books-knox-2-1-0-user-guide--Optionally+cleanup+the+sample+directory+in+case+a+previous+example+was+run+without+cleaning+up.)

```bash
curl -i -k -u guest:guest-password -X DELETE \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/user/guest/example?op=DELETE&recursive=true'
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Register+the+name+for+a+sample+file+README+in+/user/guest/example."></a>Register the name for a sample file README in /user/guest/example. [](#knox-apache-org-books-knox-2-1-0-user-guide--Register+the+name+for+a+sample+file+README+in+/user/guest/example.)

```bash
curl -i -k -u guest:guest-password -X PUT \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/user/guest/example/README?op=CREATE'
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Upload+README+to+/user/guest/example.++Use+the+README+in+{GATEWAY_HOME}."></a>Upload README to /user/guest/example. Use the README in {GATEWAY\_HOME}. [](#knox-apache-org-books-knox-2-1-0-user-guide--Upload+README+to+/user/guest/example.++Use+the+README+in+{GATEWAY_HOME}.)

```bash
curl -i -k -u guest:guest-password -T README -X PUT \
    '{Value of Location header from command above}'
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--List+the+contents+of+the+directory+/user/guest/example."></a>List the contents of the directory /user/guest/example. [](#knox-apache-org-books-knox-2-1-0-user-guide--List+the+contents+of+the+directory+/user/guest/example.)

```bash
curl -i -k -u guest:guest-password -X GET \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/user/guest/example?op=LISTSTATUS'
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Request+the+content+of+the+README+file+in+/user/guest/example."></a>Request the content of the README file in /user/guest/example. [](#knox-apache-org-books-knox-2-1-0-user-guide--Request+the+content+of+the+README+file+in+/user/guest/example.)

```bash
curl -i -k -u guest:guest-password -X GET \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/user/guest/example/README?op=OPEN'
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Read+the+content+of+the+file."></a>Read the content of the file. [](#knox-apache-org-books-knox-2-1-0-user-guide--Read+the+content+of+the+file.)

```bash
curl -i -k -u guest:guest-password -X GET \
    '{Value of Location header from command above}'
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Optionally+cleanup+the+example+directory."></a>Optionally cleanup the example directory. [](#knox-apache-org-books-knox-2-1-0-user-guide--Optionally+cleanup+the+example+directory.)

```bash
curl -i -k -u guest:guest-password -X DELETE \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/user/guest/example?op=DELETE&recursive=true'
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+client+DSL"></a>WebHDFS client DSL [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+client+DSL)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--get()+-+Get+a+file+from+HDFS+(OPEN)."></a>get() - Get a file from HDFS (OPEN). [](#knox-apache-org-books-knox-2-1-0-user-guide--get()+-+Get+a+file+from+HDFS+(OPEN).)

- Request
  - from( String name ) - The full name of the file in HDFS.
  - file( String name ) - The name of a local file to create with the content. If this isn’t specified the file content must be read from the response.
- Response
  - BasicResponse
  - If file parameter specified content will be streamed to file.
- Example
  - `Hdfs.get( session ).from( "/user/guest/example/README" ).now().string`

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--ls()+-+Query+the+contents+of+a+directory+(LISTSTATUS)"></a>ls() - Query the contents of a directory (LISTSTATUS) [](#knox-apache-org-books-knox-2-1-0-user-guide--ls()+-+Query+the+contents+of+a+directory+(LISTSTATUS))

- Request
  - dir( String name ) - The full name of the directory in HDFS.
- Response
  - BasicResponse
- Example
  - `Hdfs.ls( session ).dir( "/user/guest/example" ).now().string`

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--mkdir()+-+Create+a+directory+in+HDFS+(MKDIRS)"></a>mkdir() - Create a directory in HDFS (MKDIRS) [](#knox-apache-org-books-knox-2-1-0-user-guide--mkdir()+-+Create+a+directory+in+HDFS+(MKDIRS))

- Request
  - dir( String name ) - The full name of the directory to create in HDFS.
  - perm( String perm ) - The permissions for the directory (e.g. 644). Optional: default=“777”
- Response
  - EmptyResponse - Implicit close().
- Example
  - `Hdfs.mkdir( session ).dir( "/user/guest/example" ).now()`

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--put()+-+Write+a+file+into+HDFS+(CREATE)"></a>put() - Write a file into HDFS (CREATE) [](#knox-apache-org-books-knox-2-1-0-user-guide--put()+-+Write+a+file+into+HDFS+(CREATE))

- Request
  - text( String text ) - Text to upload to HDFS. Takes precedence over file if both present.
  - file( String name ) - The name of a local file to upload to HDFS.
  - to( String name ) - The fully qualified name to create in HDFS.
- Response
  - EmptyResponse - Implicit close().
- Example
  - `Hdfs.put( session ).file( README ).to( "/user/guest/example/README" ).now()`

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--rm()+-+Delete+a+file+or+directory+(DELETE)"></a>rm() - Delete a file or directory (DELETE) [](#knox-apache-org-books-knox-2-1-0-user-guide--rm()+-+Delete+a+file+or+directory+(DELETE))

- Request
  - file( String name ) - The fully qualified file or directory name in HDFS.
  - recursive( Boolean recursive ) - Delete directory and all of its contents if True. Optional: default=False
- Response
  - BasicResponse - Implicit close().
- Example
  - `Hdfs.rm( session ).file( "/user/guest/example" ).recursive().now()`

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+HA"></a>WebHDFS HA [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS+HA)

Knox provides basic failover functionality for REST API calls made to WebHDFS when HDFS HA has been configured and enabled.

To enable HA functionality for WebHDFS in Knox the following configuration has to be added to the topology file.

```xml
<provider>
   <role>ha</role>
   <name>HaProvider</name>
   <enabled>true</enabled>
   <param>
       <name>WEBHDFS</name>
       <value>maxFailoverAttempts=3;failoverSleep=1000;enabled=true</value>
   </param>
</provider>
```

The role and name of the provider above must be as shown. The name in the ‘param’ section must match that of the service role name that is being configured for HA and the value in the ‘param’ section is the configuration for that particular service in HA mode. In this case the name is ‘WEBHDFS’.

The various configuration parameters are described below:

- maxFailoverAttempts - This is the maximum number of times a failover will be attempted. The failover strategy at this time is very simplistic in that the next URL in the list of URLs provided for the service is used and the one that failed is put at the bottom of the list. If the list is exhausted and the maximum number of attempts is not reached then the first URL that failed will be tried again (the list will start again from the original top entry).
- failoverSleep - The amount of time in milliseconds that the process will wait or sleep before attempting to failover.
- enabled - Flag to turn the particular service on or off for HA.

And for the service configuration itself the additional URLs should be added to the list. The active URL (at the time of configuration) should ideally be added to the top of the list.

```xml
<service>
    <role>WEBHDFS</role>
    <url>http://{host1}:50070/webhdfs</url>
    <url>http://{host2}:50070/webhdfs</url>
</service>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHCat"></a>WebHCat [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHCat)

WebHCat (also called *Templeton*) is a related but separate service from HiveServer2. As such it is installed and configured independently. The [WebHCat wiki pages](https://cwiki.apache.org/confluence/display/Hive/WebHCat) describe this processes. In sandbox this configuration file for WebHCat is located at `/etc/hadoop/hcatalog/webhcat-site.xml`. Note the properties shown below as they are related to configuration required by the gateway.

```xml
<property>
    <name>templeton.port</name>
    <value>50111</value>
</property>
```

Also important is the configuration of the JOBTRACKER RPC endpoint. For Hadoop 2 this can be found in the `yarn-site.xml` file. In Sandbox this file can be found at `/etc/hadoop/conf/yarn-site.xml`. The property `yarn.resourcemanager.address` within that file is relevant for the gateway’s configuration.

```xml
<property>
    <name>yarn.resourcemanager.address</name>
    <value>sandbox.hortonworks.com:8050</value>
</property>
```

See [WebHDFS](#knox-apache-org-books-knox-2-1-0-user-guide--WebHDFS) for details about locating the Hadoop configuration for the NAMENODE endpoint.

The gateway by default includes a sample topology descriptor file `{GATEWAY_HOME}/deployments/sandbox.xml`. The values in this sample are configured to work with an installed Sandbox VM.

```xml
<service>
    <role>NAMENODE</role>
    <url>hdfs://localhost:8020</url>
</service>
<service>
    <role>JOBTRACKER</role>
    <url>rpc://localhost:8050</url>
</service>
<service>
    <role>WEBHCAT</role>
    <url>http://localhost:50111/templeton</url>
</service>
```

The URLs provided for the role NAMENODE and JOBTRACKER do not result in an endpoint being exposed by the gateway. This information is only required so that other URLs can be rewritten that reference the appropriate RPC address for Hadoop services. This prevents clients from needing to be aware of the internal cluster details. Note that for Hadoop 2 the JOBTRACKER RPC endpoint is provided by the Resource Manager component.

By default the gateway is configured to use the HTTP endpoint for WebHCat in the Sandbox. This could alternatively be configured to use the HTTPS endpoint by providing the correct address.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHCat+URL+Mapping"></a>WebHCat URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHCat+URL+Mapping)

For WebHCat URLs, the mapping of Knox Gateway accessible URLs to direct WebHCat URLs is simple.

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/templeton |
| --- | --- |
| Cluster | http://{webhcat-host}:{webhcat-port}/templeton} |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHCat+via+cURL"></a>WebHCat via cURL [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHCat+via+cURL)

Users can use cURL to directly invoke the REST APIs via the gateway. For the full list of available REST calls look at the WebHCat documentation. This is a simple curl command to test the connection:

```bash
curl -i -k -u guest:guest-password 'https://localhost:8443/gateway/sandbox/templeton/v1/status'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHCat+Example"></a>WebHCat Example [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHCat+Example)

This example will submit the familiar WordCount Java MapReduce job to the Hadoop cluster via the gateway using the KnoxShell DSL. There are several ways to do this depending upon your preference.

You can use the “embedded” Groovy interpreter provided with the distribution.

```text
java -jar bin/shell.jar samples/ExampleWebHCatJob.groovy
```

You can manually type in the KnoxShell DSL script into the “embedded” Groovy interpreter provided with the distribution.

```text
java -jar bin/shell.jar
```

Each line from the file `samples/ExampleWebHCatJob.groovy` would then need to be typed or copied into the interactive shell.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHCat+Client+DSL"></a>WebHCat Client DSL [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHCat+Client+DSL)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--submitJava()+-+Submit+a+Java+MapReduce+job."></a>submitJava() - Submit a Java MapReduce job. [](#knox-apache-org-books-knox-2-1-0-user-guide--submitJava()+-+Submit+a+Java+MapReduce+job.)

- Request
  - jar (String) - The remote file name of the JAR containing the app to execute.
  - app (String) - The app name to execute. This is *wordcount* for example not the class name.
  - input (String) - The remote directory name to use as input for the job.
  - output (String) - The remote directory name to store output from the job.
- Response
  - jobId : String - The job ID of the submitted job. Consumes body.
- Example

  Job.submitJava(session) .jar(remoteJarName) .app(appName) .input(remoteInputDir) .output(remoteOutputDir) .now() .jobId

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--submitPig()+-+Submit+a+Pig+job."></a>submitPig() - Submit a Pig job. [](#knox-apache-org-books-knox-2-1-0-user-guide--submitPig()+-+Submit+a+Pig+job.)

- Request
  - file (String) - The remote file name of the pig script.
  - arg (String) - An argument to pass to the script.
  - statusDir (String) - The remote directory to store status output.
- Response
  - jobId : String - The job ID of the submitted job. Consumes body.
- Example
  - `Job.submitPig(session).file(remotePigFileName).arg("-v").statusDir(remoteStatusDir).now()`

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--submitHive()+-+Submit+a+Hive+job."></a>submitHive() - Submit a Hive job. [](#knox-apache-org-books-knox-2-1-0-user-guide--submitHive()+-+Submit+a+Hive+job.)

- Request
  - file (String) - The remote file name of the hive script.
  - arg (String) - An argument to pass to the script.
  - statusDir (String) - The remote directory to store status output.
- Response
  - jobId : String - The job ID of the submitted job. Consumes body.
- Example
  - `Job.submitHive(session).file(remoteHiveFileName).arg("-v").statusDir(remoteStatusDir).now()`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--submitSqoop+Job+API"></a>submitSqoop Job API [](#knox-apache-org-books-knox-2-1-0-user-guide--submitSqoop+Job+API)

Using the Knox DSL, you can now easily submit and monitor [Apache Sqoop](https://sqoop.apache.org) jobs. The WebHCat Job class now supports the `submitSqoop` command.

```text
Job.submitSqoop(session)
    .command("import --connect jdbc:mysql://hostname:3306/dbname ... ")
    .statusDir(remoteStatusDir)
    .now().jobId
```

The `submitSqoop` command supports the following arguments:

- command (String) - The sqoop command string to execute.
- files (String) - Comma separated files to be copied to the templeton controller job.
- optionsfile (String) - The remote file which contain Sqoop command need to run.
- libdir (String) - The remote directory containing jdbc jar to include with sqoop lib
- statusDir (String) - The remote directory to store status output.

A complete example is available here: [https://cwiki.apache.org/confluence/display/KNOX/2016/11/08/Running+SQOOP+job+via+KNOX+Shell+DSL](https://cwiki.apache.org/confluence/display/KNOX/2016/11/08/Running+SQOOP+job+via+KNOX+Shell+DSL)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--queryQueue()+-+Return+a+list+of+all+job+IDs+registered+to+the+user."></a>queryQueue() - Return a list of all job IDs registered to the user. [](#knox-apache-org-books-knox-2-1-0-user-guide--queryQueue()+-+Return+a+list+of+all+job+IDs+registered+to+the+user.)

- Request
  - No request parameters.
- Response
  - BasicResponse
- Example
  - `Job.queryQueue(session).now().string`

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--queryStatus()+-+Check+the+status+of+a+job+and+get+related+job+information+given+its+job+ID."></a>queryStatus() - Check the status of a job and get related job information given its job ID. [](#knox-apache-org-books-knox-2-1-0-user-guide--queryStatus()+-+Check+the+status+of+a+job+and+get+related+job+information+given+its+job+ID.)

- Request
  - jobId (String) - The job ID to check. This is the ID received when the job was created.
- Response
  - BasicResponse
- Example
  - `Job.queryStatus(session).jobId(jobId).now().string`

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--WebHCat+HA"></a>WebHCat HA [](#knox-apache-org-books-knox-2-1-0-user-guide--WebHCat+HA)

Please look at [Default Service HA support](#knox-apache-org-books-knox-2-1-0-user-guide--Default+Service+HA+support)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Oozie"></a>Oozie [](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie)

Oozie is a Hadoop component that provides complex job workflows to be submitted and managed. Please refer to the latest [Oozie documentation](http://oozie.apache.org/docs/4.2.0/) for details.

In order to make Oozie accessible via the gateway there are several important Hadoop configuration settings. These all relate to the network endpoint exposed by various Hadoop services.

The HTTP endpoint at which Oozie is running can be found via the `oozie.base.url property` in the `oozie-site.xml` file. In a Sandbox installation this can typically be found in `/etc/oozie/conf/oozie-site.xml`.

```xml
<property>
    <name>oozie.base.url</name>
    <value>http://sandbox.hortonworks.com:11000/oozie</value>
</property>
```

The RPC address at which the Resource Manager exposes the JOBTRACKER endpoint can be found via the `yarn.resourcemanager.address` in the `yarn-site.xml` file. In a Sandbox installation this can typically be found in `/etc/hadoop/conf/yarn-site.xml`.

```xml
<property>
    <name>yarn.resourcemanager.address</name>
    <value>sandbox.hortonworks.com:8050</value>
</property>
```

The RPC address at which the Name Node exposes its RPC endpoint can be found via the `dfs.namenode.rpc-address` in the `hdfs-site.xml` file. In a Sandbox installation this can typically be found in `/etc/hadoop/conf/hdfs-site.xml`.

```xml
<property>
    <name>dfs.namenode.rpc-address</name>
    <value>sandbox.hortonworks.com:8020</value>
</property>
```

If HDFS has been configured to be in High Availability mode (HA), then instead of the RPC address mentioned above for the Name Node, look up and use the logical name of the service found via `dfs.nameservices` in `hdfs-site.xml`. For example,

```xml
<property>
    <name>dfs.nameservices</name>
    <value>ha-service</value>
</property>
```

Please note, only one of the URLs, either the RPC endpoint or the HA service name should be used as the NAMENODE HDFS URL in the gateway topology file.

The information above must be provided to the gateway via a topology descriptor file. These topology descriptor files are placed in `{GATEWAY_HOME}/deployments`. An example that is setup for the default configuration of the Sandbox is `{GATEWAY_HOME}/deployments/sandbox.xml`. These values will need to be changed for non-default Sandbox or other Hadoop cluster configuration.

```xml
<service>
    <role>NAMENODE</role>
    <url>hdfs://localhost:8020</url>
</service>
<service>
    <role>JOBTRACKER</role>
    <url>rpc://localhost:8050</url>
</service>
<service>
    <role>OOZIE</role>
    <url>http://localhost:11000/oozie</url>
    <param>
        <name>replayBufferSize</name>
        <value>8</value>
    </param>
</service>
```

A default replayBufferSize of 8KB is shown in the sample topology file above. This may need to be increased if your request size is larger.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Oozie+URL+Mapping"></a>Oozie URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie+URL+Mapping)

For Oozie URLs, the mapping of Knox Gateway accessible URLs to direct Oozie URLs is simple.

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/oozie |
| --- | --- |
| Cluster | http://{oozie-host}:{oozie-port}/oozie} |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Oozie+Request+Changes"></a>Oozie Request Changes [](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie+Request+Changes)

TODO - In some cases the Oozie requests needs to be slightly different when made through the gateway. These changes are required in order to protect the client from knowing the internal structure of the Hadoop cluster.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Oozie+Example+via+Client+DSL"></a>Oozie Example via Client DSL [](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie+Example+via+Client+DSL)

This example will also submit the familiar WordCount Java MapReduce job to the Hadoop cluster via the gateway using the KnoxShell DSL. However in this case the job will be submitted via a Oozie workflow. There are several ways to do this depending upon your preference.

You can use the “embedded” Groovy interpreter provided with the distribution.

```text
java -jar bin/shell.jar samples/ExampleOozieWorkflow.groovy
```

You can manually type in the KnoxShell DSL script into the “embedded” Groovy interpreter provided with the distribution.

```text
java -jar bin/shell.jar
```

Each line from the file `samples/ExampleOozieWorkflow.groovy` will need to be typed or copied into the interactive shell.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Oozie+Example+via+cURL"></a>Oozie Example via cURL [](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie+Example+via+cURL)

The example below illustrates the sequence of curl commands that could be used to run a “word count” map reduce job via an Oozie workflow.

It utilizes the hadoop-examples.jar from a Hadoop install for running a simple word count job. A copy of that jar has been included in the samples directory for convenience.

In addition a workflow definition and configuration file is required. These have not been included but are available for download. Download [workflow-definition.xml](workflow-definition.xml) and [workflow-configuration.xml](workflow-configuration.xml) and store them in the `{GATEWAY_HOME}` directory. Review the contents of workflow-configuration.xml to ensure that it matches your environment.

Take care to follow the instructions below where replacement values are required. These replacement values are identified with `{ }` markup.

```bash
# 0. Optionally cleanup the test directory in case a previous example was run without cleaning up.
curl -i -k -u guest:guest-password -X DELETE \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/user/guest/example?op=DELETE&recursive=true'

# 1. Create the inode for workflow definition file in /user/guest/example
curl -i -k -u guest:guest-password -X PUT \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/user/guest/example/workflow.xml?op=CREATE'

# 2. Upload the workflow definition file.  This file can be found in {GATEWAY_HOME}/templates
curl -i -k -u guest:guest-password -T workflow-definition.xml -X PUT \
    '{Value Location header from command above}'

# 3. Create the inode for hadoop-examples.jar in /user/guest/example/lib
curl -i -k -u guest:guest-password -X PUT \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/user/guest/example/lib/hadoop-examples.jar?op=CREATE'

# 4. Upload hadoop-examples.jar to /user/guest/example/lib.  Use a hadoop-examples.jar from a Hadoop install.
curl -i -k -u guest:guest-password -T samples/hadoop-examples.jar -X PUT \
    '{Value Location header from command above}'

# 5. Create the inode for a sample input file readme.txt in /user/guest/example/input.
curl -i -k -u guest:guest-password -X PUT \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/user/guest/example/input/README?op=CREATE'

# 6. Upload readme.txt to /user/guest/example/input.  Use the readme.txt in {GATEWAY_HOME}.
# The sample below uses this README file found in {GATEWAY_HOME}.
curl -i -k -u guest:guest-password -T README -X PUT \
    '{Value of Location header from command above}'

# 7. Submit the job via Oozie
# Take note of the Job ID in the JSON response as this will be used in the next step.
curl -i -k -u guest:guest-password -H Content-Type:application/xml -T workflow-configuration.xml \
    -X POST 'https://localhost:8443/gateway/sandbox/oozie/v1/jobs?action=start'

# 8. Query the job status via Oozie.
curl -i -k -u guest:guest-password -X GET \
    'https://localhost:8443/gateway/sandbox/oozie/v1/job/{Job ID from JSON body}'

# 9. List the contents of the output directory /user/guest/example/output
curl -i -k -u guest:guest-password -X GET \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/user/guest/example/output?op=LISTSTATUS'

# 10. Optionally cleanup the test directory
curl -i -k -u guest:guest-password -X DELETE \
    'https://localhost:8443/gateway/sandbox/webhdfs/v1/user/guest/example?op=DELETE&recursive=true'
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Oozie+Client+DSL"></a>Oozie Client DSL [](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie+Client+DSL)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--submit()+-+Submit+a+workflow+job."></a>submit() - Submit a workflow job. [](#knox-apache-org-books-knox-2-1-0-user-guide--submit()+-+Submit+a+workflow+job.)

- Request
  - text (String) - XML formatted workflow configuration string.
  - file (String) - A filename containing XML formatted workflow configuration.
  - action (String) - The initial action to take on the job. Optional: Default is “start”.
- Response
  - BasicResponse
- Example
  - `Workflow.submit(session).file(localFile).action("start").now()`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--status()+-+Query+the+status+of+a+workflow+job."></a>status() - Query the status of a workflow job. [](#knox-apache-org-books-knox-2-1-0-user-guide--status()+-+Query+the+status+of+a+workflow+job.)

- Request
  - jobId (String) - The job ID to check. This is the ID received when the job was created.
- Response
  - BasicResponse
- Example
  - `Workflow.status(session).jobId(jobId).now().string`

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Oozie+HA"></a>Oozie HA [](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie+HA)

Please look at [Default Service HA support](#knox-apache-org-books-knox-2-1-0-user-guide--Default+Service+HA+support)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase"></a>HBase [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase)

HBase provides an optional REST API (previously called Stargate). See the HBase REST Setup section below for getting started with the HBase REST API and Knox with the Hortonworks Sandbox environment.

The gateway by default includes a sample topology descriptor file `{GATEWAY_HOME}/deployments/sandbox.xml`. The value in this sample is configured to work with an installed Sandbox VM.

```xml
<service>
    <role>WEBHBASE</role>
    <url>http://localhost:60080</url>
    <param>
        <name>replayBufferSize</name>
        <value>8</value>
    </param>
</service>
```

By default the gateway is configured to use port 60080 for Hbase in the Sandbox. Please see the steps to configure the port mapping below.

A default replayBufferSize of 8KB is shown in the sample topology file above. This may need to be increased if your query size is larger.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase+URL+Mapping"></a>HBase URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+URL+Mapping)

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/hbase |
| --- | --- |
| Cluster | http://{hbase-rest-host}:8080/ |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase+Examples"></a>HBase Examples [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+Examples)

The examples below illustrate the set of basic operations with HBase instance using the REST API. Use following link to get more details about HBase REST API: [http://hbase.apache.org/book.html#\_rest](http://hbase.apache.org/book.html#_rest).

Note: Some HBase examples may not work due to enabled [Access Control](http://hbase.apache.org/book.html#_securing_access_to_your_data). User may not be granted access for performing operations in the samples. In order to check if Access Control is configured in the HBase instance verify `hbase-site.xml` for a presence of `org.apache.hadoop.hbase.security.access.AccessController` in `hbase.coprocessor.master.classes` and `hbase.coprocessor.region.classes` properties.  
To grant the Read, Write, Create permissions to `guest` user execute the following command:

```bash
echo grant 'guest', 'RWC' | hbase shell
```

If you are using a cluster secured with Kerberos you will need to have used `kinit` to authenticate to the KDC.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase+REST+API+Setup"></a>HBase REST API Setup [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+REST+API+Setup)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Launch+REST+API"></a>Launch REST API [](#knox-apache-org-books-knox-2-1-0-user-guide--Launch+REST+API)

The command below launches the REST daemon on port 8080 (the default)

```text
sudo {HBASE_BIN}/hbase-daemon.sh start rest
```

Where `{HBASE_BIN}` is `/usr/hdp/current/hbase-master/bin/` in the case of a HDP install.

To use a different port use the `-p` option:

```text
sudo {HBASE_BIN/hbase-daemon.sh start rest -p 60080
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configure+Sandbox+port+mapping+for+VirtualBox"></a>Configure Sandbox port mapping for VirtualBox [](#knox-apache-org-books-knox-2-1-0-user-guide--Configure+Sandbox+port+mapping+for+VirtualBox)

1. Select the VM
2. Select menu Machine>Settings…
3. Select tab Network
4. Select Adapter 1
5. Press Port Forwarding button
6. Press Plus button to insert new rule: Name=HBASE REST, Host Port=60080, Guest Port=60080
7. Press OK to close the rule window
8. Press OK to Network window save the changes

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase+Restart"></a>HBase Restart [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+Restart)

If it becomes necessary to restart HBase you can log into the hosts running HBase and use these steps.

```text
sudo {HBASE_BIN}/hbase-daemon.sh stop rest
sudo -u hbase {HBASE_BIN}/hbase-daemon.sh stop regionserver
sudo -u hbase {HBASE_BIN}/hbase-daemon.sh stop master
sudo -u hbase {HBASE_BIN}/hbase-daemon.sh stop zookeeper

sudo -u hbase {HBASE_BIN}/hbase-daemon.sh start regionserver
sudo -u hbase {HBASE_BIN}/hbase-daemon.sh start master
sudo -u hbase {HBASE_BIN}/hbase-daemon.sh start zookeeper
sudo {HBASE_BIN}/hbase-daemon.sh start rest -p 60080
```

Where `{HBASE_BIN}` is `/usr/hdp/current/hbase-master/bin/` in the case of a HDP Sandbox install.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase+client+DSL"></a>HBase client DSL [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+client+DSL)

For more details about client DSL usage please look at the chapter about the client DSL in this guide.

After launching the shell, execute the following command to be able to use the snippets below. `import org.apache.knox.gateway.shell.hbase.HBase;`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--systemVersion()+-+Query+Software+Version."></a>systemVersion() - Query Software Version. [](#knox-apache-org-books-knox-2-1-0-user-guide--systemVersion()+-+Query+Software+Version.)

- Request
  - No request parameters.
- Response
  - BasicResponse
- Example
  - `HBase.session(session).systemVersion().now().string`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--clusterVersion()+-+Query+Storage+Cluster+Version."></a>clusterVersion() - Query Storage Cluster Version. [](#knox-apache-org-books-knox-2-1-0-user-guide--clusterVersion()+-+Query+Storage+Cluster+Version.)

- Request
  - No request parameters.
- Response
  - BasicResponse
- Example
  - `HBase.session(session).clusterVersion().now().string`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--status()+-+Query+Storage+Cluster+Status."></a>status() - Query Storage Cluster Status. [](#knox-apache-org-books-knox-2-1-0-user-guide--status()+-+Query+Storage+Cluster+Status.)

- Request
  - No request parameters.
- Response
  - BasicResponse
- Example
  - `HBase.session(session).status().now().string`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table().list()+-+Query+Table+List."></a>table().list() - Query Table List. [](#knox-apache-org-books-knox-2-1-0-user-guide--table().list()+-+Query+Table+List.)

- Request
  - No request parameters.
- Response
  - BasicResponse
- Example
- `HBase.session(session).table().list().now().string`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).schema()+-+Query+Table+Schema."></a>table(String tableName).schema() - Query Table Schema. [](#knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).schema()+-+Query+Table+Schema.)

- Request
  - No request parameters.
- Response
  - BasicResponse
- Example
  - `HBase.session(session).table().schema().now().string`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).create()+-+Create+Table+Schema."></a>table(String tableName).create() - Create Table Schema. [](#knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).create()+-+Create+Table+Schema.)

- Request
  - attribute(String name, Object value) - the table’s attribute.
  - family(String name) - starts family definition. Has sub requests:
  - attribute(String name, Object value) - the family’s attribute.
  - endFamilyDef() - finishes family definition.
- Response
  - EmptyResponse
- Example

  HBase.session(session).table(tableName).create() .attribute(“tb\_attr1”, “value1”) .attribute(“tb\_attr2”, “value2”) .family(“family1”) .attribute(“fm\_attr1”, “value3”) .attribute(“fm\_attr2”, “value4”) .endFamilyDef() .family(“family2”) .family(“family3”) .endFamilyDef() .attribute(“tb\_attr3”, “value5”) .now()

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).update()+-+Update+Table+Schema."></a>table(String tableName).update() - Update Table Schema. [](#knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).update()+-+Update+Table+Schema.)

- Request
  - family(String name) - starts family definition. Has sub requests:
  - attribute(String name, Object value) - the family’s attribute.
  - endFamilyDef() - finishes family definition.
- Response
  - EmptyResponse
- Example

  HBase.session(session).table(tableName).update() .family(“family1”) .attribute(“fm\_attr1”, “new\_value3”) .endFamilyDef() .family(“family4”) .attribute(“fm\_attr3”, “value6”) .endFamilyDef() .now()```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).regions()+-+Query+Table+Metadata."></a>table(String tableName).regions() - Query Table Metadata. [](#knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).regions()+-+Query+Table+Metadata.)

- Request
  - No request parameters.
- Response
  - BasicResponse
- Example
  - `HBase.session(session).table(tableName).regions().now().string`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).delete()+-+Delete+Table."></a>table(String tableName).delete() - Delete Table. [](#knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).delete()+-+Delete+Table.)

- Request
  - No request parameters.
- Response
  - EmptyResponse
- Example
  - `HBase.session(session).table(tableName).delete().now()`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).row(String+rowId).store()+-+Cell+Store."></a>table(String tableName).row(String rowId).store() - Cell Store. [](#knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).row(String+rowId).store()+-+Cell+Store.)

- Request
  - column(String family, String qualifier, Object value, Long time) - the data to store; “qualifier” may be “null”; “time” is optional.
- Response
  - EmptyResponse
- Example

  HBase.session(session).table(tableName).row(“row\_id\_1”).store() .column(“family1”, “col1”, “col\_value1”) .column(“family1”, “col2”, “col\_value2”, 1234567890l) .column(“family2”, null, “fam\_value1”) .now()

  HBase.session(session).table(tableName).row(“row\_id\_2”).store() .column(“family1”, “row2\_col1”, “row2\_col\_value1”) .now()

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).row(String+rowId).query()+-+Cell+or+Row+Query."></a>table(String tableName).row(String rowId).query() - Cell or Row Query. [](#knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).row(String+rowId).query()+-+Cell+or+Row+Query.)

- rowId is optional. Querying with null or empty rowId will select all rows.
- Request
  - column(String family, String qualifier) - the column to select; “qualifier” is optional.
  - startTime(Long) - the lower bound for filtration by time.
  - endTime(Long) - the upper bound for filtration by time.
  - times(Long startTime, Long endTime) - the lower and upper bounds for filtration by time.
  - numVersions(Long) - the maximum number of versions to return.
- Response
  - BasicResponse
- Example

  HBase.session(session).table(tableName).row(“row\_id\_1”) .query() .now().string

  HBase.session(session).table(tableName).row().query().now().string

  HBase.session(session).table(tableName).row().query() .column(“family1”, “row2\_col1”) .column(“family2”) .times(0, Long.MAX\_VALUE) .numVersions(1) .now().string

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).row(String+rowId).delete()+-+Row,+Column,+or+Cell+Delete."></a>table(String tableName).row(String rowId).delete() - Row, Column, or Cell Delete. [](#knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).row(String+rowId).delete()+-+Row,+Column,+or+Cell+Delete.)

- Request
  - column(String family, String qualifier) - the column to delete; “qualifier” is optional.
  - time(Long) - the upper bound for time filtration.
- Response
  - EmptyResponse
- Example

  HBase.session(session).table(tableName).row(“row\_id\_1”) .delete() .column(“family1”, “col1”) .now()```

  HBase.session(session).table(tableName).row(“row\_id\_1”) .delete() .column(“family2”) .time(Long.MAX\_VALUE) .now()```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).scanner().create()+-+Scanner+Creation."></a>table(String tableName).scanner().create() - Scanner Creation. [](#knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).scanner().create()+-+Scanner+Creation.)

- Request
  - startRow(String) - the lower bound for filtration by row id.
  - endRow(String) - the upper bound for filtration by row id.
  - rows(String startRow, String endRow) - the lower and upper bounds for filtration by row id.
  - column(String family, String qualifier) - the column to select; “qualifier” is optional.
  - batch(Integer) - the batch size.
  - startTime(Long) - the lower bound for filtration by time.
  - endTime(Long) - the upper bound for filtration by time.
  - times(Long startTime, Long endTime) - the lower and upper bounds for filtration by time.
  - filter(String) - the filter XML definition.
  - maxVersions(Integer) - the maximum number of versions to return.
- Response
  - scannerId : String - the scanner ID of the created scanner. Consumes body.
- Example

  HBase.session(session).table(tableName).scanner().create() .column(“family1”, “col2”) .column(“family2”) .startRow(“row\_id\_1”) .endRow(“row\_id\_2”) .batch(1) .startTime(0) .endTime(Long.MAX\_VALUE) .filter("") .maxVersions(100) .now()```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).scanner(String+scannerId).getNext()+-+Scanner+Get+Next."></a>table(String tableName).scanner(String scannerId).getNext() - Scanner Get Next. [](#knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).scanner(String+scannerId).getNext()+-+Scanner+Get+Next.)

- Request
  - No request parameters.
- Response
  - BasicResponse
- Example
  - `HBase.session(session).table(tableName).scanner(scannerId).getNext().now().string`

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).scanner(String+scannerId).delete()+-+Scanner+Deletion."></a>table(String tableName).scanner(String scannerId).delete() - Scanner Deletion. [](#knox-apache-org-books-knox-2-1-0-user-guide--table(String+tableName).scanner(String+scannerId).delete()+-+Scanner+Deletion.)

- Request
  - No request parameters.
- Response
  - EmptyResponse
- Example
  - `HBase.session(session).table(tableName).scanner(scannerId).delete().now()`

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase+via+Client+DSL"></a>HBase via Client DSL [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+via+Client+DSL)

This example illustrates sequence of all basic HBase operations: 1. get system version 2. get cluster version 3. get cluster status 4. create the table 5. get list of tables 6. get table schema 7. update table schema 8. insert single row into table 9. query row by id 10. query all rows 11. delete cell from row 12. delete entire column family from row 13. get table regions 14. create scanner 15. fetch values using scanner 16. drop scanner 17. drop the table

There are several ways to do this depending upon your preference.

You can use the Groovy interpreter provided with the distribution.

```text
java -jar bin/shell.jar samples/ExampleHBase.groovy
```

You can manually type in the KnoxShell DSL script into the interactive Groovy interpreter provided with the distribution.

```text
java -jar bin/shell.jar
```

Each line from the file below will need to be typed or copied into the interactive shell.

```java
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.knox.gateway.shell.hbase

import org.apache.knox.gateway.shell.Hadoop

import static java.util.concurrent.TimeUnit.SECONDS

gateway = "https://localhost:8443/gateway/sandbox"
username = "guest"
password = "guest-password"
tableName = "test_table"

session = Hadoop.login(gateway, username, password)

println "System version : " + HBase.session(session).systemVersion().now().string

println "Cluster version : " + HBase.session(session).clusterVersion().now().string

println "Status : " + HBase.session(session).status().now().string

println "Creating table '" + tableName + "'..."

HBase.session(session).table(tableName).create()  \
    .attribute("tb_attr1", "value1")  \
    .attribute("tb_attr2", "value2")  \
    .family("family1")  \
        .attribute("fm_attr1", "value3")  \
        .attribute("fm_attr2", "value4")  \
    .endFamilyDef()  \
    .family("family2")  \
    .family("family3")  \
    .endFamilyDef()  \
    .attribute("tb_attr3", "value5")  \
    .now()

println "Done"

println "Table List : " + HBase.session(session).table().list().now().string

println "Schema for table '" + tableName + "' : " + HBase.session(session)  \
    .table(tableName)  \
    .schema()  \
    .now().string

println "Updating schema of table '" + tableName + "'..."

HBase.session(session).table(tableName).update()  \
    .family("family1")  \
        .attribute("fm_attr1", "new_value3")  \
    .endFamilyDef()  \
    .family("family4")  \
        .attribute("fm_attr3", "value6")  \
    .endFamilyDef()  \
    .now()

println "Done"

println "Schema for table '" + tableName + "' : " + HBase.session(session)  \
    .table(tableName)  \
    .schema()  \
    .now().string

println "Inserting data into table..."

HBase.session(session).table(tableName).row("row_id_1").store()  \
    .column("family1", "col1", "col_value1")  \
    .column("family1", "col2", "col_value2", 1234567890l)  \
    .column("family2", null, "fam_value1")  \
    .now()

HBase.session(session).table(tableName).row("row_id_2").store()  \
    .column("family1", "row2_col1", "row2_col_value1")  \
    .now()

println "Done"

println "Querying row by id..."

println HBase.session(session).table(tableName).row("row_id_1")  \
    .query()  \
    .now().string

println "Querying all rows..."

println HBase.session(session).table(tableName).row().query().now().string

println "Querying row by id with extended settings..."

println HBase.session(session).table(tableName).row().query()  \
    .column("family1", "row2_col1")  \
    .column("family2")  \
    .times(0, Long.MAX_VALUE)  \
    .numVersions(1)  \
    .now().string

println "Deleting cell..."

HBase.session(session).table(tableName).row("row_id_1")  \
    .delete()  \
    .column("family1", "col1")  \
    .now()

println "Rows after delete:"

println HBase.session(session).table(tableName).row().query().now().string

println "Extended cell delete"

HBase.session(session).table(tableName).row("row_id_1")  \
    .delete()  \
    .column("family2")  \
    .time(Long.MAX_VALUE)  \
    .now()

println "Rows after delete:"

println HBase.session(session).table(tableName).row().query().now().string

println "Table regions : " + HBase.session(session).table(tableName)  \
    .regions()  \
    .now().string

println "Creating scanner..."

scannerId = HBase.session(session).table(tableName).scanner().create()  \
    .column("family1", "col2")  \
    .column("family2")  \
    .startRow("row_id_1")  \
    .endRow("row_id_2")  \
    .batch(1)  \
    .startTime(0)  \
    .endTime(Long.MAX_VALUE)  \
    .filter("")  \
    .maxVersions(100)  \
    .now().scannerId

println "Scanner id=" + scannerId

println "Scanner get next..."

println HBase.session(session).table(tableName).scanner(scannerId)  \
    .getNext()  \
    .now().string

println "Dropping scanner with id=" + scannerId

HBase.session(session).table(tableName).scanner(scannerId).delete().now()

println "Done"

println "Dropping table '" + tableName + "'..."

HBase.session(session).table(tableName).delete().now()

println "Done"

session.shutdown(10, SECONDS)
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase+via+cURL"></a>HBase via cURL [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+via+cURL)

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Get+software+version"></a>Get software version [](#knox-apache-org-books-knox-2-1-0-user-guide--Get+software+version)

Set Accept Header to “text/plain”, “text/xml”, “application/json” or “application/x-protobuf”

```bash
%  curl -ik -u guest:guest-password\
 -H "Accept:  application/json"\
 -X GET 'https://localhost:8443/gateway/sandbox/hbase/version'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Get+version+information+regarding+the+HBase+cluster+backing+the+REST+API+instance"></a>Get version information regarding the HBase cluster backing the REST API instance [](#knox-apache-org-books-knox-2-1-0-user-guide--Get+version+information+regarding+the+HBase+cluster+backing+the+REST+API+instance)

Set Accept Header to “text/plain”, “text/xml” or “application/x-protobuf”

```bash
%  curl -ik -u guest:guest-password\
 -H "Accept: text/xml"\
 -X GET 'https://localhost:8443/gateway/sandbox/hbase/version/cluster'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Get+detailed+status+on+the+HBase+cluster+backing+the+REST+API+instance."></a>Get detailed status on the HBase cluster backing the REST API instance. [](#knox-apache-org-books-knox-2-1-0-user-guide--Get+detailed+status+on+the+HBase+cluster+backing+the+REST+API+instance.)

Set Accept Header to “text/plain”, “text/xml”, “application/json” or “application/x-protobuf”

```bash
curl -ik -u guest:guest-password\
 -H "Accept: text/xml"\
 -X GET 'https://localhost:8443/gateway/sandbox/hbase/status/cluster'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Get+the+list+of+available+tables."></a>Get the list of available tables. [](#knox-apache-org-books-knox-2-1-0-user-guide--Get+the+list+of+available+tables.)

Set Accept Header to “text/plain”, “text/xml”, “application/json” or “application/x-protobuf”

```bash
curl -ik -u guest:guest-password\
 -H "Accept: text/xml"\
 -X GET 'https://localhost:8443/gateway/sandbox/hbase'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Create+table+with+two+column+families+using+xml+input"></a>Create table with two column families using xml input [](#knox-apache-org-books-knox-2-1-0-user-guide--Create+table+with+two+column+families+using+xml+input)

```bash
curl -ik -u guest:guest-password\
 -H "Accept: text/xml"   -H "Content-Type: text/xml"\
 -d '<?xml version="1.0" encoding="UTF-8"?><TableSchema name="table1"><ColumnSchema name="family1"/><ColumnSchema name="family2"/></TableSchema>'\
 -X PUT 'https://localhost:8443/gateway/sandbox/hbase/table1/schema'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Create+table+with+two+column+families+using+JSON+input"></a>Create table with two column families using JSON input [](#knox-apache-org-books-knox-2-1-0-user-guide--Create+table+with+two+column+families+using+JSON+input)

```bash
curl -ik -u guest:guest-password\
 -H "Accept: application/json"  -H "Content-Type: application/json"\
 -d '{"name":"table2","ColumnSchema":[{"name":"family3"},{"name":"family4"}]}'\
 -X PUT 'https://localhost:8443/gateway/sandbox/hbase/table2/schema'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Get+table+metadata"></a>Get table metadata [](#knox-apache-org-books-knox-2-1-0-user-guide--Get+table+metadata)

```bash
curl -ik -u guest:guest-password\
 -H "Accept: text/xml"\
 -X GET 'https://localhost:8443/gateway/sandbox/hbase/table1/regions'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Insert+single+row+table"></a>Insert single row table [](#knox-apache-org-books-knox-2-1-0-user-guide--Insert+single+row+table)

```bash
curl -ik -u guest:guest-password\
 -H "Content-Type: text/xml"\
 -H "Accept: text/xml"\
 -d '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><CellSet><Row key="cm93MQ=="><Cell column="ZmFtaWx5MTpjb2wx" >dGVzdA==</Cell></Row></CellSet>'\
 -X POST 'https://localhost:8443/gateway/sandbox/hbase/table1/row1'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Insert+multiple+rows+into+table"></a>Insert multiple rows into table [](#knox-apache-org-books-knox-2-1-0-user-guide--Insert+multiple+rows+into+table)

```bash
curl -ik -u guest:guest-password\
 -H "Content-Type: text/xml"\
 -H "Accept: text/xml"\
 -d '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><CellSet><Row key="cm93MA=="><Cell column=" ZmFtaWx5Mzpjb2x1bW4x" >dGVzdA==</Cell></Row><Row key="cm93MQ=="><Cell column=" ZmFtaWx5NDpjb2x1bW4x" >dGVzdA==</Cell></Row></CellSet>'\
 -X POST 'https://localhost:8443/gateway/sandbox/hbase/table2/false-row-key'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Get+all+data+from+table"></a>Get all data from table [](#knox-apache-org-books-knox-2-1-0-user-guide--Get+all+data+from+table)

Set Accept Header to “text/plain”, “text/xml”, “application/json” or “application/x-protobuf”

```bash
curl -ik -u guest:guest-password\
 -H "Accept: text/xml"\
 -X GET 'https://localhost:8443/gateway/sandbox/hbase/table1/*'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Execute+cell+or+row+query"></a>Execute cell or row query [](#knox-apache-org-books-knox-2-1-0-user-guide--Execute+cell+or+row+query)

Set Accept Header to “text/plain”, “text/xml”, “application/json” or “application/x-protobuf”

```bash
curl -ik -u guest:guest-password\
 -H "Accept: text/xml"\
 -X GET 'https://localhost:8443/gateway/sandbox/hbase/table1/row1/family1:col1'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Delete+entire+row+from+table"></a>Delete entire row from table [](#knox-apache-org-books-knox-2-1-0-user-guide--Delete+entire+row+from+table)

```bash
curl -ik -u guest:guest-password\
 -H "Accept: text/xml"\
 -X DELETE 'https://localhost:8443/gateway/sandbox/hbase/table2/row0'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Delete+column+family+from+row"></a>Delete column family from row [](#knox-apache-org-books-knox-2-1-0-user-guide--Delete+column+family+from+row)

```bash
curl -ik -u guest:guest-password\
 -H "Accept: text/xml"\
 -X DELETE 'https://localhost:8443/gateway/sandbox/hbase/table2/row0/family3'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Delete+specific+column+from+row"></a>Delete specific column from row [](#knox-apache-org-books-knox-2-1-0-user-guide--Delete+specific+column+from+row)

```bash
curl -ik -u guest:guest-password\
 -H "Accept: text/xml"\
 -X DELETE 'https://localhost:8443/gateway/sandbox/hbase/table2/row0/family3'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Create+scanner"></a>Create scanner [](#knox-apache-org-books-knox-2-1-0-user-guide--Create+scanner)

Scanner URL will be in Location response header

```bash
curl -ik -u guest:guest-password\
 -H "Content-Type: text/xml"\
 -d '<Scanner batch="1"/>'\
 -X PUT 'https://localhost:8443/gateway/sandbox/hbase/table1/scanner'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Get+the+values+of+the+next+cells+found+by+the+scanner"></a>Get the values of the next cells found by the scanner [](#knox-apache-org-books-knox-2-1-0-user-guide--Get+the+values+of+the+next+cells+found+by+the+scanner)

```bash
curl -ik -u guest:guest-password\
 -H "Accept: application/json"\
 -X GET 'https://localhost:8443/gateway/sandbox/hbase/table1/scanner/13705290446328cff5ed'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Delete+scanner"></a>Delete scanner [](#knox-apache-org-books-knox-2-1-0-user-guide--Delete+scanner)

```bash
curl -ik -u guest:guest-password\
 -H "Accept: text/xml"\
 -X DELETE 'https://localhost:8443/gateway/sandbox/hbase/table1/scanner/13705290446328cff5ed'
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Delete+table"></a>Delete table [](#knox-apache-org-books-knox-2-1-0-user-guide--Delete+table)

```bash
curl -ik -u guest:guest-password\
 -X DELETE 'https://localhost:8443/gateway/sandbox/hbase/table1/schema'
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase+REST+HA"></a>HBase REST HA [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+REST+HA)

Please look at [Default Service HA support](#knox-apache-org-books-knox-2-1-0-user-guide--Default+Service+HA+support) if you wish to explicitly list the URLs under the service definition.

If you run the HBase REST Server from the HBase Region Server nodes, you can utilize more advanced HA support. The HBase REST Server does not register itself with ZooKeeper. So the Knox HA component looks in ZooKeeper for instances of HBase Region Servers and then performs a light weight ping for the presence of the REST Server on the same hosts. The user should not supply URLs in the service definition.

Note: Users of Ambari must manually startup the HBase REST Server.

To enable HA functionality for HBase in Knox the following configuration has to be added to the topology file.

```xml
<provider>
    <role>ha</role>
    <name>HaProvider</name>
    <enabled>true</enabled>
    <param>
        <name>WEBHBASE</name>
        <value>maxFailoverAttempts=3;failoverSleep=1000;enabled=true;zookeeperEnsemble=machine1:2181,machine2:2181,machine3:2181</value>
   </param>
</provider>
```

The role and name of the provider above must be as shown. The name in the ‘param’ section must match that of the service role name that is being configured for HA and the value in the ‘param’ section is the configuration for that particular service in HA mode. In this case the name is ‘WEBHBASE’.

The various configuration parameters are described below:

- maxFailoverAttempts - This is the maximum number of times a failover will be attempted. The failover strategy at this time is very simplistic in that the next URL in the list of URLs provided for the service is used and the one that failed is put at the bottom of the list. If the list is exhausted and the maximum number of attempts is not reached then the first URL will be tried again after the list is fetched again from Zookeeper (a refresh of the list is done at this point)
- failoverSleep - The amount of time in millis that the process will wait or sleep before attempting to failover.
- enabled - Flag to turn the particular service on or off for HA.
- zookeeperEnsemble - A comma separated list of host names (or IP addresses) of the ZooKeeper hosts that consist of the ensemble that the HBase servers register their information with.

And for the service configuration itself the URLs need NOT be added to the list. For example:

```xml
<service>
    <role>WEBHBASE</role>
</service>
```

Please note that there is no `<url>` tag specified here as the URLs for the Kafka servers are obtained from ZooKeeper.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hive"></a>Hive [](#knox-apache-org-books-knox-2-1-0-user-guide--Hive)

The [Hive wiki pages](https://cwiki.apache.org/confluence/display/Hive/Home) describe Hive installation and configuration processes. In sandbox configuration file for Hive is located at `/etc/hive/hive-site.xml`. Hive Server has to be started in HTTP mode. Note the properties shown below as they are related to configuration required by the gateway.

```xml
<property>
    <name>hive.server2.thrift.http.port</name>
    <value>10001</value>
    <description>Port number when in HTTP mode.</description>
</property>

<property>
    <name>hive.server2.thrift.http.path</name>
    <value>cliservice</value>
    <description>Path component of URL endpoint when in HTTP mode.</description>
</property>

<property>
    <name>hive.server2.transport.mode</name>
    <value>http</value>
    <description>Server transport mode. "binary" or "http".</description>
</property>

<property>
    <name>hive.server2.allow.user.substitution</name>
    <value>true</value>
</property>
```

The gateway by default includes a sample topology descriptor file `{GATEWAY_HOME}/deployments/sandbox.xml`. The value in this sample is configured to work with an installed Sandbox VM.

```xml
<service>
    <role>HIVE</role>
    <url>http://localhost:10001/cliservice</url>
    <param>
        <name>replayBufferSize</name>
        <value>8</value>
    </param>
</service>
```

By default the gateway is configured to use the binary transport mode for Hive in the Sandbox.

A default replayBufferSize of 8KB is shown in the sample topology file above. This may need to be increased if your query size is larger.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hive+JDBC+URL+Mapping"></a>Hive JDBC URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Hive+JDBC+URL+Mapping)

| Gateway | jdbc:hive2://{gateway-host}:{gateway-port}/;ssl=true;sslTrustStore={gateway-trust-store-path};trustStorePassword={gateway-trust-store-password};transportMode=http;httpPath={gateway-path}/{cluster-name}/hive |
| --- | --- |
| Cluster | http://{hive-host}:{hive-port}/{hive-path} |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hive+Examples"></a>Hive Examples [](#knox-apache-org-books-knox-2-1-0-user-guide--Hive+Examples)

This guide provides detailed examples for how to do some basic interactions with Hive via the Apache Knox Gateway.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hive+Setup"></a>Hive Setup [](#knox-apache-org-books-knox-2-1-0-user-guide--Hive+Setup)

1. Make sure you are running the correct version of Hive to ensure JDBC/Thrift/HTTP support.
2. Make sure Hive Server is running on the correct port.
3. Make sure Hive Server is running in HTTP mode.
4. Client side (JDBC):
   1. Hive JDBC in HTTP mode depends on following minimal libraries set to run successfully(must be in the classpath):
      - hive-jdbc-0.14.0-standalone.jar;
      - commons-logging-1.1.3.jar;
   2. Connection URL has to be the following: `jdbc:hive2://{gateway-host}:{gateway-port}/;ssl=true;sslTrustStore={gateway-trust-store-path};trustStorePassword={gateway-trust-store-password};transportMode=http;httpPath={gateway-path}/{cluster-name}/hive`
   3. Look at [https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-DDLOperations](https://cwiki.apache.org/confluence/display/Hive/GettingStarted#GettingStarted-DDLOperations) for examples. Hint: For testing it would be better to execute `set hive.security.authorization.enabled=false` as the first statement. Hint: Good examples of Hive DDL/DML can be found here [http://gettingstarted.hadooponazure.com/hw/hive.html](http://gettingstarted.hadooponazure.com/hw/hive.html)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Customization"></a>Customization [](#knox-apache-org-books-knox-2-1-0-user-guide--Customization)

This example may need to be tailored to the execution environment. In particular host name, host port, user name, user password and context path may need to be changed to match your environment. In particular there is one example file in the distribution that may need to be customized. Take a moment to review this file. All of the values that may need to be customized can be found together at the top of the file.

- samples/hive/java/jdbc/sandbox/HiveJDBCSample.java

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Client+JDBC+Example"></a>Client JDBC Example [](#knox-apache-org-books-knox-2-1-0-user-guide--Client+JDBC+Example)

Sample example for creating new table, loading data into it from the file system local to the Hive server and querying data from that table.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Java"></a>Java [](#knox-apache-org-books-knox-2-1-0-user-guide--Java)

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.logging.Level;
import java.util.logging.Logger;

public class HiveJDBCSample {

  public static void main( String[] args ) {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    try {
      String user = "guest";
      String password = user + "-password";
      String gatewayHost = "localhost";
      int gatewayPort = 8443;
      String trustStore = "/usr/lib/knox/data/security/keystores/gateway.jks";
      String trustStorePassword = "knoxsecret";
      String contextPath = "gateway/sandbox/hive";
      String connectionString = String.format( "jdbc:hive2://%s:%d/;ssl=true;sslTrustStore=%s;trustStorePassword=%s?hive.server2.transport.mode=http;hive.server2.thrift.http.path=/%s", gatewayHost, gatewayPort, trustStore, trustStorePassword, contextPath );

      // load Hive JDBC Driver
      Class.forName( "org.apache.hive.jdbc.HiveDriver" );

      // configure JDBC connection
      connection = DriverManager.getConnection( connectionString, user, password );

      statement = connection.createStatement();

      // disable Hive authorization - it could be omitted if Hive authorization
      // was configured properly
      statement.execute( "set hive.security.authorization.enabled=false" );

      // create sample table
      statement.execute( "CREATE TABLE logs(column1 string, column2 string, column3 string, column4 string, column5 string, column6 string, column7 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '" );

      // load data into Hive from file /tmp/log.txt which is placed on the local file system
      statement.execute( "LOAD DATA LOCAL INPATH '/tmp/log.txt' OVERWRITE INTO TABLE logs" );

      resultSet = statement.executeQuery( "SELECT * FROM logs" );

      while ( resultSet.next() ) {
        System.out.println( resultSet.getString( 1 ) + " --- " + resultSet.getString( 2 ) + " --- " + resultSet.getString( 3 ) + " --- " + resultSet.getString( 4 ) );
      }
    } catch ( ClassNotFoundException ex ) {
      Logger.getLogger( HiveJDBCSample.class.getName() ).log( Level.SEVERE, null, ex );
    } catch ( SQLException ex ) {
      Logger.getLogger( HiveJDBCSample.class.getName() ).log( Level.SEVERE, null, ex );
    } finally {
      if ( resultSet != null ) {
        try {
          resultSet.close();
        } catch ( SQLException ex ) {
          Logger.getLogger( HiveJDBCSample.class.getName() ).log( Level.SEVERE, null, ex );
        }
      }
      if ( statement != null ) {
        try {
          statement.close();
        } catch ( SQLException ex ) {
          Logger.getLogger( HiveJDBCSample.class.getName() ).log( Level.SEVERE, null, ex );
        }
      }
      if ( connection != null ) {
        try {
          connection.close();
        } catch ( SQLException ex ) {
          Logger.getLogger( HiveJDBCSample.class.getName() ).log( Level.SEVERE, null, ex );
        }
      }
    }
  }
}
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Groovy"></a>Groovy [](#knox-apache-org-books-knox-2-1-0-user-guide--Groovy)

Make sure that `{GATEWAY_HOME/ext}` directory contains the following libraries for successful execution:

- hive-jdbc-0.14.0-standalone.jar;
- commons-logging-1.1.3.jar;

There are several ways to execute this sample depending upon your preference.

You can use the Groovy interpreter provided with the distribution.

```text
java -jar bin/shell.jar samples/hive/groovy/jdbc/sandbox/HiveJDBCSample.groovy
```

You can manually type in the KnoxShell DSL script into the interactive Groovy interpreter provided with the distribution.

```text
java -jar bin/shell.jar
```

Each line from the file below will need to be typed or copied into the interactive shell.

```java
import java.sql.DriverManager

user = "guest";
password = user + "-password";
gatewayHost = "localhost";
gatewayPort = 8443;
trustStore = "/usr/lib/knox/data/security/keystores/gateway.jks";
trustStorePassword = "knoxsecret";
contextPath = "gateway/sandbox/hive";
connectionString = String.format( "jdbc:hive2://%s:%d/;ssl=true;sslTrustStore=%s;trustStorePassword=%s?hive.server2.transport.mode=http;hive.server2.thrift.http.path=/%s", gatewayHost, gatewayPort, trustStore, trustStorePassword, contextPath );

// Load Hive JDBC Driver
Class.forName( "org.apache.hive.jdbc.HiveDriver" );

// Configure JDBC connection
connection = DriverManager.getConnection( connectionString, user, password );

statement = connection.createStatement();

// Disable Hive authorization - This can be omitted if Hive authorization is configured properly
statement.execute( "set hive.security.authorization.enabled=false" );

// Create sample table
statement.execute( "CREATE TABLE logs(column1 string, column2 string, column3 string, column4 string, column5 string, column6 string, column7 string) ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '" );

// Load data into Hive from file /tmp/log.txt which is placed on the local file system
statement.execute( "LOAD DATA LOCAL INPATH '/tmp/sample.log' OVERWRITE INTO TABLE logs" );

resultSet = statement.executeQuery( "SELECT * FROM logs" );

while ( resultSet.next() ) {
  System.out.println( resultSet.getString( 1 ) + " --- " + resultSet.getString( 2 ) );
}

resultSet.close();
statement.close();
connection.close();
```

Examples use ‘log.txt’ with content:

```text
2012-02-03 18:35:34 SampleClass6 [INFO] everything normal for id 577725851
2012-02-03 18:35:34 SampleClass4 [FATAL] system problem at id 1991281254
2012-02-03 18:35:34 SampleClass3 [DEBUG] detail for id 1304807656
2012-02-03 18:35:34 SampleClass3 [WARN] missing id 423340895
2012-02-03 18:35:34 SampleClass5 [TRACE] verbose detail for id 2082654978
2012-02-03 18:35:34 SampleClass0 [ERROR] incorrect id  1886438513
2012-02-03 18:35:34 SampleClass9 [TRACE] verbose detail for id 438634209
2012-02-03 18:35:34 SampleClass8 [DEBUG] detail for id 2074121310
2012-02-03 18:35:34 SampleClass0 [TRACE] verbose detail for id 1505582508
2012-02-03 18:35:34 SampleClass0 [TRACE] verbose detail for id 1903854437
2012-02-03 18:35:34 SampleClass7 [DEBUG] detail for id 915853141
2012-02-03 18:35:34 SampleClass3 [TRACE] verbose detail for id 303132401
2012-02-03 18:35:34 SampleClass6 [TRACE] verbose detail for id 151914369
2012-02-03 18:35:34 SampleClass2 [DEBUG] detail for id 146527742
...
```

Expected output:

```text
2012-02-03 --- 18:35:34 --- SampleClass6 --- [INFO]
2012-02-03 --- 18:35:34 --- SampleClass4 --- [FATAL]
2012-02-03 --- 18:35:34 --- SampleClass3 --- [DEBUG]
2012-02-03 --- 18:35:34 --- SampleClass3 --- [WARN]
2012-02-03 --- 18:35:34 --- SampleClass5 --- [TRACE]
2012-02-03 --- 18:35:34 --- SampleClass0 --- [ERROR]
2012-02-03 --- 18:35:34 --- SampleClass9 --- [TRACE]
2012-02-03 --- 18:35:34 --- SampleClass8 --- [DEBUG]
2012-02-03 --- 18:35:34 --- SampleClass0 --- [TRACE]
2012-02-03 --- 18:35:34 --- SampleClass0 --- [TRACE]
2012-02-03 --- 18:35:34 --- SampleClass7 --- [DEBUG]
2012-02-03 --- 18:35:34 --- SampleClass3 --- [TRACE]
2012-02-03 --- 18:35:34 --- SampleClass6 --- [TRACE]
2012-02-03 --- 18:35:34 --- SampleClass2 --- [DEBUG]
...
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HiveServer2+HA"></a>HiveServer2 HA [](#knox-apache-org-books-knox-2-1-0-user-guide--HiveServer2+HA)

Knox provides basic failover functionality for calls made to Hive Server when more than one HiveServer2 instance is installed in the cluster and registered with the same ZooKeeper ensemble. The HA functionality in this case fetches the HiveServer2 URL information from a ZooKeeper ensemble, so the user need only supply the necessary ZooKeeper configuration and not the Hive connection URLs.

To enable HA functionality for Hive in Knox the following configuration has to be added to the topology file.

```xml
<provider>
    <role>ha</role>
    <name>HaProvider</name>
    <enabled>true</enabled>
    <param>
        <name>HIVE</name>
        <value>maxFailoverAttempts=3;failoverSleep=1000;enabled=true;zookeeperEnsemble=machine1:2181,machine2:2181,machine3:2181;zookeeperNamespace=hiveserver2</value>
   </param>
</provider>
```

The role and name of the provider above must be as shown. The name in the ‘param’ section must match that of the service role name that is being configured for HA and the value in the ‘param’ section is the configuration for that particular service in HA mode. In this case the name is ‘HIVE’.

The various configuration parameters are described below:

- maxFailoverAttempts - This is the maximum number of times a failover will be attempted. The failover strategy at this time is very simplistic in that the next URL in the list of URLs provided for the service is used and the one that failed is put at the bottom of the list. If the list is exhausted and the maximum number of attempts is not reached then the first URL will be tried again after the list is fetched again from Zookeeper (a refresh of the list is done at this point)
- failoverSleep - The amount of time in millis that the process will wait or sleep before attempting to failover.
- enabled - Flag to turn the particular service on or off for HA.
- zookeeperEnsemble - A comma separated list of host names (or IP addresses) of the zookeeper hosts that consist of the ensemble that the Hive servers register their information with. This value can be obtained from Hive’s config file hive-site.xml as the value for the parameter ‘hive.zookeeper.quorum’.
- zookeeperNamespace - This is the namespace under which HiveServer2 information is registered in the Zookeeper ensemble. This value can be obtained from Hive’s config file hive-site.xml as the value for the parameter ‘hive.server2.zookeeper.namespace’.

And for the service configuration itself the URLs need not be added to the list. For example.

```xml
<service>
    <role>HIVE</role>
</service>
```

Please note that there is no `<url>` tag specified here as the URLs for the Hive servers are obtained from Zookeeper.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Yarn"></a>Yarn [](#knox-apache-org-books-knox-2-1-0-user-guide--Yarn)

Knox provides gateway functionality for the REST APIs of the ResourceManager. The ResourceManager REST APIs allow the user to get information about the cluster - status on the cluster, metrics on the cluster, scheduler information, information about nodes in the cluster, and information about applications on the cluster. Also as of Hadoop version 2.5.0, the user can submit a new application as well as kill it (or get state) using the ‘Writable’ APIs.

The docs for this can be found here

[http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html)

To enable this functionality, a topology file needs to have the following configuration:

```xml
<service>
    <role>RESOURCEMANAGER</role>
    <url>http://<hostname>:<port>/ws</url>
</service>
```

The default resource manager http port is 8088. If it is configured to some other port, that configuration can be found in `yarn-site.xml` under the property `yarn.resourcemanager.webapp.address`.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Yarn+URL+Mapping"></a>Yarn URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Yarn+URL+Mapping)

For Yarn URLs, the mapping of Knox Gateway accessible URLs to direct Yarn URLs is the following.

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/resourcemanager |
| --- | --- |
| Cluster | http://{yarn-host}:{yarn-port}/ws} |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Yarn+Examples+via+cURL"></a>Yarn Examples via cURL [](#knox-apache-org-books-knox-2-1-0-user-guide--Yarn+Examples+via+cURL)

Some of the various calls that can be made and examples using curl are listed below.

```bash
# 0. Getting cluster info

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster'

# 1. Getting cluster metrics

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/metrics'

To get the same information in an xml format

curl -ikv -u guest:guest-password -H Accept:application/xml -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/metrics'

# 2. Getting scheduler information

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/scheduler'

# 3. Getting all the applications listed and their information

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/apps'

# 4. Getting applications statistics

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/appstatistics'

Also query params can be used as below to filter the results

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/appstatistics?states=accepted,running,finished&applicationTypes=mapreduce'

# 5. To get a specific application (please note, replace the application id with a real value)

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/apps/{application_id}'

# 6. To get the attempts made for a particular application

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/apps/{application_id}/appattempts'

# 7. To get information about the various nodes

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/nodes'

Also to get a specific node, use an id obtained in the response from above (the node id is scrambled) and issue the following

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/nodes/{node_id}'

# 8. To create a new Application

curl -ikv -u guest:guest-password -X POST 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/apps/new-application'

An application id is returned from the request above and this can be used to submit an application.

# 9. To submit an application, put together a request containing the application id received in the above response (please refer to Yarn REST
API documentation).

curl -ikv -u guest:guest-password -T request.json -H Content-Type:application/json -X POST 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/apps'

Here the request is saved in a file called request.json

#10. To get application state

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/apps/{application_id}/state'

curl -ikv -u guest:guest-password -H Content-Type:application/json -X PUT -T state-killed.json 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/apps/application_1409008107556_0007/state'

# 11. To kill an application that is running issue the below command with the application id of the application that is to be killed.
The contents of the state-killed.json file are :

{
  "state":"KILLED"
}

curl -ikv -u guest:guest-password -H Content-Type:application/json -X PUT -T state-killed.json 'https://localhost:8443/gateway/sandbox/resourcemanager/v1/cluster/apps/{application_id}/state'
```

Note: The sensitive data like nodeHttpAddress, nodeHostName, id will be hidden.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Kafka"></a>Kafka [](#knox-apache-org-books-knox-2-1-0-user-guide--Kafka)

Knox provides gateway functionality to Kafka when used with the Confluent Kafka REST Proxy. The Kafka REST APIs allow the user to view the status of the cluster, perform administrative actions and produce messages.

Note: Consumption of messages via Knox at this time is not supported.

The docs for the Confluent Kafka REST Proxy can be found here: [http://docs.confluent.io/current/kafka-rest/docs/index.html](http://docs.confluent.io/current/kafka-rest/docs/index.html)

To enable this functionality, a topology file needs to have the following configuration:

```xml
<service>
    <role>KAFKA</role>
    <url>http://<kafka-rest-host>:<kafka-rest-port></url>
</service>
```

The default Kafka REST Proxy port is 8082. If it is configured to some other port, that configuration can be found in `kafka-rest.properties` under the property `listeners`.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Kafka+URL+Mapping"></a>Kafka URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Kafka+URL+Mapping)

For Kafka URLs, the mapping of Knox Gateway accessible URLs to direct Kafka URLs is the following.

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/kafka |
| --- | --- |
| Cluster | http://{kakfa-rest-host}:{kafka-rest-port}} |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Kafka+Examples+via+cURL"></a>Kafka Examples via cURL [](#knox-apache-org-books-knox-2-1-0-user-guide--Kafka+Examples+via+cURL)

Some of the various calls that can be made and examples using curl are listed below.

```bash
# 0. Getting topic info

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/kafka/topics'

# 1. Publish message to topic

curl -ikv -u guest:guest-password -X POST 'https://localhost:8443/gateway/sandbox/kafka/topics/TOPIC1' -H 'Content-Type: application/vnd.kafka.json.v2+json' -H 'Accept: application/vnd.kafka.v2+json' --data '"records":[{"value":{"foo":"bar"}}]}'
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Kafka+HA"></a>Kafka HA [](#knox-apache-org-books-knox-2-1-0-user-guide--Kafka+HA)

Knox provides basic failover functionality for calls made to Kafka. Since the Confluent Kafka REST Proxy does not register itself with ZooKeeper, the HA component looks in ZooKeeper for instances of Kafka and then performs a light weight ping for the presence of the REST Proxy on the same hosts. As such the Kafka REST Proxy must be installed on the same host as Kafka. The user should not supply URLs in the service definition.

Note: Users of Ambari must manually startup the Confluent Kafka REST Proxy.

To enable HA functionality for Kafka in Knox the following configuration has to be added to the topology file.

```xml
<provider>
    <role>ha</role>
    <name>HaProvider</name>
    <enabled>true</enabled>
    <param>
        <name>KAFKA</name>
        <value>maxFailoverAttempts=3;failoverSleep=1000;enabled=true;zookeeperEnsemble=machine1:2181,machine2:2181,machine3:2181</value>
   </param>
</provider>
```

The role and name of the provider above must be as shown. The name in the ‘param’ section must match that of the service role name that is being configured for HA and the value in the ‘param’ section is the configuration for that particular service in HA mode. In this case the name is ‘KAFKA’.

The various configuration parameters are described below:

- maxFailoverAttempts - This is the maximum number of times a failover will be attempted. The failover strategy at this time is very simplistic in that the next URL in the list of URLs provided for the service is used and the one that failed is put at the bottom of the list. If the list is exhausted and the maximum number of attempts is not reached then the first URL will be tried again after the list is fetched again from Zookeeper (a refresh of the list is done at this point)
- failoverSleep - The amount of time in millis that the process will wait or sleep before attempting to failover.
- enabled - Flag to turn the particular service on or off for HA.
- zookeeperEnsemble - A comma separated list of host names (or IP addresses) of the ZooKeeper hosts that consist of the ensemble that the Kafka servers register their information with.

And for the service configuration itself the URLs need NOT be added to the list. For example:

```xml
<service>
    <role>KAFKA</role>
</service>
```

Please note that there is no `<url>` tag specified here as the URLs for the Kafka servers are obtained from ZooKeeper.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Storm"></a>Storm [](#knox-apache-org-books-knox-2-1-0-user-guide--Storm)

Storm is a distributed realtime computation system. Storm exposes REST APIs for UI functionality that can be used for retrieving metrics data and configuration information as well as management operations such as starting or stopping topologies.

The docs for this can be found here

[https://github.com/apache/storm/blob/master/docs/STORM-UI-REST-API.md](https://github.com/apache/storm/blob/master/docs/STORM-UI-REST-API.md)

To enable this functionality, a topology file needs to have the following configuration:

```xml
<service>
    <role>STORM</role>
    <url>http://<hostname>:<port></url>
</service>
```

The default UI daemon port is 8744. If it is configured to some other port, that configuration can be found in `storm.yaml` as the value for the property `ui.port`.

In addition to the storm service configuration above, a STORM-LOGVIEWER service must be configured if the log files are to be retrieved through Knox. The value of the port for the logviewer can be found by the property `logviewer.port` also in the file `storm.yaml`.

```xml
<service>
    <role>STORM-LOGVIEWER</role>
    <url>http://<hostname>:<port></url>
</service>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Storm+URL+Mapping"></a>Storm URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Storm+URL+Mapping)

For Storm URLs, the mapping of Knox Gateway accessible URLs to direct Storm URLs is the following.

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/storm |
| --- | --- |
| Cluster | http://{storm-host}:{storm-port} |

For the log viewer the mapping is as follows

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/storm/logviewer |
| --- | --- |
| Cluster | http://{storm-logviewer-host}:{storm-logviewer-port} |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Storm+Examples"></a>Storm Examples [](#knox-apache-org-books-knox-2-1-0-user-guide--Storm+Examples)

Some of the various calls that can be made and examples using curl are listed below.

```bash
# 0. Getting cluster configuration

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/storm/api/v1/cluster/configuration'

# 1. Getting cluster summary information

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/storm/api/v1/cluster/summary'

# 2. Getting supervisor summary information

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/storm/api/v1/supervisor/summary'

# 3. topologies summary information

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/storm/api/v1/topology/summary'

# 4. Getting specific topology information. Substitute {id} with the topology id.

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/storm/api/v1/topology/{id}'

# 5. To get component level information. Substitute {id} with the topology id and {component} with the component id e.g. 'spout'

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/storm/api/v1/topology/{id}/component/{component}'
```

The following POST operations all require a ‘x-csrf-token’ header along with other information that can be stored in a cookie file. In particular the ‘ring-session’ header and ‘JSESSIONID’.

```bash
# 6. To activate a topology. Substitute {id} with the topology id and {token-value} with the x-csrf-token value.

curl -ik -b ~/cookiejar.txt -c ~/cookiejar.txt -u guest:guest-password -H 'x-csrf-token:{token-value}' -X POST \
 http://localhost:8744/api/v1/topology/{id}/activate

# 7. To de-activate a topology. Substitute {id} with the topology id and {token-value} with the x-csrf-token value.

curl -ik -b ~/cookiejar.txt -c ~/cookiejar.txt -u guest:guest-password -H 'x-csrf-token:{token-value}' -X POST \
 http://localhost:8744/api/v1/topology/{id}/deactivate

# 8. To rebalance a topology. Substitute {id} with the topology id and {token-value} with the x-csrf-token value.

curl -ik -b ~/cookiejar.txt -c ~/cookiejar.txt -u guest:guest-password -H 'x-csrf-token:{token-value}' -X POST \
 http://localhost:8744/api/v1/topology/{id}/rebalance/0

# 9. To kill a topology. Substitute {id} with the topology id and {token-value} with the x-csrf-token value.

curl -ik -b ~/cookiejar.txt -c ~/cookiejar.txt -u guest:guest-password -H 'x-csrf-token:{token-value}' -X POST \
 http://localhost:8744/api/v1/topology/{id}/kill/0
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Solr"></a>Solr [](#knox-apache-org-books-knox-2-1-0-user-guide--Solr)

Knox provides gateway functionality to Solr with support for versions 5.5+ and 6+. The Solr REST APIs allow the user to view the status of the collections, perform administrative actions and query collections.

See the Solr Quickstart ([http://lucene.apache.org/solr/quickstart.html](http://lucene.apache.org/solr/quickstart.html)) section of the Solr documentation for examples of the Solr REST API.

Since Knox provides an abstraction over Solr and ZooKeeper, the use of the SolrJ CloudSolrClient is no longer supported. You should replace instances of CloudSolrClient with HttpSolrClient.

Note: Updates to Solr via Knox require a POST operation require the use of preemptive authentication which is not directly supported by the
SolrJ API at this time.

To enable this functionality, a topology file needs to have the following configuration:

```xml
<service>
    <role>SOLR</role>
    <version>6.0.0</version>
    <url>http://<solr-host>:<solr-port></url>
</service>
```

The default Solr port is 8983. Adjust the version specified to either ’5.5.0 or ‘6.0.0’.

For Solr 5.5.0 you also need to change the role name to `SOLRAPI` like this:

```xml
<service>
    <role>SOLRAPI</role>
    <version>5.5.0</version>
    <url>http://<solr-host>:<solr-port></url>
</service>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Solr+URL+Mapping"></a>Solr URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Solr+URL+Mapping)

For Solr URLs, the mapping of Knox Gateway accessible URLs to direct Solr URLs is the following.

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/solr |
| --- | --- |
| Cluster | http://{solr-host}:{solr-port}/solr |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Solr+Examples+via+cURL"></a>Solr Examples via cURL [](#knox-apache-org-books-knox-2-1-0-user-guide--Solr+Examples+via+cURL)

Some of the various calls that can be made and examples using curl are listed below.

```bash
# 0. Query collection

curl -ikv -u guest:guest-password -X GET 'https://localhost:8443/gateway/sandbox/solr/select?q=*:*&wt=json'

# 1. Query cluster status

curl -ikv -u guest:guest-password -X POST 'https://localhost:8443/gateway/sandbox/solr/admin/collections?action=CLUSTERSTATUS' 
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Solr+HA"></a>Solr HA [](#knox-apache-org-books-knox-2-1-0-user-guide--Solr+HA)

Knox provides basic failover functionality for calls made to Solr Cloud when more than one Solr instance is installed in the cluster and registered with the same ZooKeeper ensemble. The HA functionality in this case fetches the Solr URL information from a ZooKeeper ensemble, so the user need only supply the necessary ZooKeeper configuration and not the Solr connection URLs.

To enable HA functionality for Solr Cloud in Knox the following configuration has to be added to the topology file.

```xml
<provider>
    <role>ha</role>
    <name>HaProvider</name>
    <enabled>true</enabled>
    <param>
        <name>SOLR</name>
        <value>maxFailoverAttempts=3;failoverSleep=1000;enabled=true;zookeeperEnsemble=machine1:2181,machine2:2181,machine3:2181</value>
   </param>
</provider>
```

The role and name of the provider above must be as shown. The name in the ‘param’ section must match that of the service role name that is being configured for HA and the value in the ‘param’ section is the configuration for that particular service in HA mode. In this case the name is ‘SOLR’.

The various configuration parameters are described below:

- maxFailoverAttempts - This is the maximum number of times a failover will be attempted. The failover strategy at this time is very simplistic in that the next URL in the list of URLs provided for the service is used and the one that failed is put at the bottom of the list. If the list is exhausted and the maximum number of attempts is not reached then the first URL will be tried again after the list is fetched again from ZooKeeper (a refresh of the list is done at this point)
- failoverSleep - The amount of time in millis that the process will wait or sleep before attempting to failover.
- enabled - Flag to turn the particular service on or off for HA.
- zookeeperEnsemble - A comma separated list of host names (or IP addresses) of the zookeeper hosts that consist of the ensemble that the Solr servers register their information with.

And for the service configuration itself the URLs need NOT be added to the list. For example.

```xml
<service>
    <role>SOLR</role>
    <version>6.0.0</version>
</service>
```

Please note that there is no `<url>` tag specified here as the URLs for the Solr servers are obtained from ZooKeeper.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Common+Service+Config"></a>Common Service Config [](#knox-apache-org-books-knox-2-1-0-user-guide--Common+Service+Config)

It is possible to override a few of the global configuration settings provided in gateway-site.xml at the service level. These overrides are specified as name/value pairs within the <service> elements of a particular service. The overridden settings apply only to that service.

The following table shows the common configuration settings available at the service level via service level parameters. Individual services may support additional service level parameters.

| Property | Description | Default |
| --- | --- | --- |
| httpclient.maxConnections | The maximum number of connections that a single httpclient will maintain to a single host:port. | 32 |
| httpclient.connectionTimeout | The amount of time to wait when attempting a connection. The natural unit is milliseconds, but a ‘s’ or ‘m’ suffix may be used for seconds or minutes respectively. The default timeout is system dependent. | 20s |
| httpclient.socketTimeout | The amount of time to wait for data on a socket before aborting the connection. The natural unit is milliseconds, but a ‘s’ or ‘m’ suffix may be used for seconds or minutes respectively. The default timeout is system dependent but is likely to be indefinite. | 20s |

The example below demonstrates how these service level parameters are used.

```xml
<service>
     <role>HIVE</role>
     <param>
         <name>httpclient.socketTimeout</name>
         <value>180s</value>
     </param>
</service>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Default+Service+HA+support"></a>Default Service HA support [](#knox-apache-org-books-knox-2-1-0-user-guide--Default+Service+HA+support)

Knox provides connectivity based failover functionality for service calls that can be made to more than one server instance in a cluster. To enable this functionality HaProvider configuration needs to be enabled for the service and the service itself needs to be configured with more than one URL in the topology file.

The default HA functionality works on a simple round robin algorithm which can be configured to run in the following modes

- The top of the list of URLs is used to route all of a service’s REST calls until a connection error occurs (Default).
- Round robin all the requests, distributing the load evenly across all the HA url’s (`enableLoadBalancing`)
- Round robin with sticky session, requires cookies. Here, only new sessions will round robin ensuring sticky sessions. In case of failure next configured HA url is picked up to dispatch the request which might cause loss of session depending on the backend implementation (`enableStickySession`).
- Round robin with sticky session and no fallback, depends on `enableStickySession` to be true. Here, new sessions will round robin ensuring sticky sessions. In case of failure, Knox returns http status code 502. By default noFallback is turned off (`noFallback`).
- Turn off HA round robin feature for a request based on user-agent header property (`disableLoadBalancingForUserAgents`).

This goes on until the setting of ‘maxFailoverAttempts’ is reached.

At present the following services can use this default High Availability functionality and have been tested for the same:

- WEBHCAT
- HBASE
- OOZIE
- HIVE

To enable HA functionality for a service in Knox the following configuration has to be added to the topology file.

```xml
<provider>
     <role>ha</role>
     <name>HaProvider</name>
     <enabled>true</enabled>
     <param>
         <name>{SERVICE}</name>
         <value>maxFailoverAttempts=3;failoverSleep=1000;enabled=true;enableStickySession=true;</value>
     </param>
</provider>
```

The role and name of the provider above must be as shown. The name in the ‘param’ section i.e. `{SERVICE}` must match that of the service role name that is being configured for HA and the value in the ‘param’ section is the configuration for that particular service in HA mode. For example, the value of `{SERVICE}` can be ‘WEBHCAT’, ‘HBASE’ or ‘OOZIE’.

To configure multiple services in HA mode, additional ‘param’ sections can be added.

For example,

```xml
<provider>
     <role>ha</role>
     <name>HaProvider</name>
     <enabled>true</enabled>
     <param>
         <name>OOZIE</name>
         <value>maxFailoverAttempts=3;failoverSleep=1000;enabled=true</value>
     </param>
     <param>
         <name>HBASE</name>
         <value>maxFailoverAttempts=3;failoverSleep=1000;enabled=true</value>
     </param>
     <param>
         <name>WEBHCAT</name>
         <value>maxFailoverAttempts=3;failoverSleep=1000;enabled=true</value>
     </param>
</provider>
```

The various configuration parameters are described below:

- maxFailoverAttempts - This is the maximum number of times a failover will be attempted. The failover strategy at this time is very simplistic in that the next URL in the list of URLs provided for the service is used and the one that failed is put at the bottom of the list. If the list is exhausted and the maximum number of attempts is not reached then the first URL will be tried again.
- failoverSleep - The amount of time in millis that the process will wait or sleep before attempting to failover.
- enabled - Flag to turn the particular service on or off for HA.
- enableLoadBalancing - Round robin all the requests, distributing the load evenly across all the HA url’s (no sticky sessions)
- enableStickySession - Round robin with sticky session.
- noFallback - Round robin with sticky session and no fallback, requires `enableStickySession` to be true.
- stickySessionCookieName - Customize sticky session cookie name, default is ‘KNOX\_BACKEND-{serviceName}’.

And for the service configuration itself the additional URLs should be added to the list.

```xml
<service>
    <role>{SERVICE}</role>
    <url>http://host1:port1</url>
    <url>http://host2:port2</url>
</service>
```

For example,

```xml
<service>
    <role>OOZIE</role>
    <url>http://sandbox1:11000/oozie</url>
    <url>http://sandbox2:11000/oozie</url>
</service>
```

- disableLoadBalancingForUserAgents - HA round robin feature can be turned off based on client user-agent header property. To turn it off for a specific user-agent add the user-agent value to parameter `disableLoadBalancingForUserAgents` which takes a list of user-agents (NOTE: user-agent value does not have to be an exact match, partial match will work). Default value is `ClouderaODBCDriverforApacheHive`

example:

```xml
<provider>
     <role>ha</role>
     <name>HaProvider</name>
     <enabled>true</enabled>
     <param>
         <name>HIVE</name>
         <value>enableStickySession=true;enableLoadBalancing=true;enabled=true;disableLoadBalancingForUserAgents=Test User Agent, Test User Agent2,Test User Agent3 ,Test User Agent4 ;retrySleep=1000</value>
     </param>
</provider>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Avatica"></a>Avatica [](#knox-apache-org-books-knox-2-1-0-user-guide--Avatica)

Knox provides gateway functionality for access to all Apache Avatica-based servers. The gateway can be used to provide authentication and encryption for clients to servers like the Apache Phoenix Query Server.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Gateway+configuration"></a>Gateway configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+configuration)

The Gateway can be configured for Avatica by modifying the topology XML file and providing a new service XML file.

In the topology XML file, add the following with the correct hostname:

```xml
<service>
  <role>AVATICA</role>
  <url>http://avatica:8765</url>
</service>
```

Your installation likely already contains the following service files. Ensure that they are present in your installation. In `services/avatica/1.9.0/rewrite.xml`:

```xml
<rules>
    <rule dir="IN" name="AVATICA/avatica/inbound/root" pattern="*://*:*/**/avatica/">
        <rewrite template="{$serviceUrl[AVATICA]}/"/>
    </rule>
    <rule dir="IN" name="AVATICA/avatica/inbound/path" pattern="*://*:*/**/avatica/{**}">
        <rewrite template="{$serviceUrl[AVATICA]}/{**}"/>
    </rule>
</rules>
```

And in `services/avatica/1.9.0/service.xml`:

```xml
<service role="AVATICA" name="avatica" version="1.9.0">
    <policies>
        <policy role="webappsec"/>
        <policy role="authentication"/>
        <policy role="rewrite"/>
        <policy role="authorization"/>
    </policies>
    <routes>
        <route path="/avatica">
            <rewrite apply="AVATICA/avatica/inbound/root" to="request.url"/>
        </route>
        <route path="/avatica/**">
            <rewrite apply="AVATICA/avatica/inbound/path" to="request.url"/>
        </route>
    </routes>
</service>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--JDBC+Drivers"></a>JDBC Drivers [](#knox-apache-org-books-knox-2-1-0-user-guide--JDBC+Drivers)

In most cases, users only need to modify the hostname of the Avatica server to instead be the Knox Gateway. To enable authentication, some of the Avatica property need to be added to the Properties object used when constructing the `Connection` or to the JDBC URL directly.

The JDBC URL can be modified like:

```text
jdbc:avatica:remote:url=https://knox_gateway.domain:8443/gateway/sandbox/avatica;avatica_user=username;avatica_password=password;authentication=BASIC
```

Or, using the `Properties` class:

```java
Properties props = new Properties();
props.setProperty("avatica_user", "username");
props.setProperty("avatica_password", "password");
props.setProperty("authentication", "BASIC");
DriverManager.getConnection(url, props);
```

Additionally, when the TLS certificate of the Knox Gateway is not trusted by your JVM installation, it will be necessary for you to pass in a custom truststore and truststore password to perform the necessary TLS handshake. This can be realized with the `truststore` and `truststore_password` properties using the same approaches as above.

Via the JDBC URL:

```text
jdbc:avatica:remote:url=https://...;authentication=BASIC;truststore=/tmp/knox_truststore.jks;truststore_password=very_secret
```

Using Java code:

```text
...
props.setProperty("truststore", "/tmp/knox_truststore.jks");
props.setProperty("truststore_password", "very_secret");
DriverManager.getConnection(url, props);
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Cloudera+Manager"></a>Cloudera Manager [](#knox-apache-org-books-knox-2-1-0-user-guide--Cloudera+Manager)

Knox provides proxied access to Cloudera Manager API.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Gateway+configuration"></a>Gateway configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+configuration)

The Gateway can be configured for Cloudera Manager API by modifying the topology XML file and providing a new service XML file.

In the topology XML file, add the following with the correct hostname:

```xml
<service>
  <role>CM-API</role>
  <url>http://<cloudera-manager>:7180/api</url>
</service>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Livy+Server"></a>Livy Server [](#knox-apache-org-books-knox-2-1-0-user-guide--Livy+Server)

Knox provides proxied access to Livy server for submitting Spark jobs. The gateway can be used to provide authentication and encryption for clients to servers like Livy.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Gateway+configuration"></a>Gateway configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+configuration)

The Gateway can be configured for Livy by modifying the topology XML file and providing a new service XML file.

In the topology XML file, add the following with the correct hostname:

```xml
<service>
  <role>LIVYSERVER</role>
  <url>http://<livy-server>:8998</url>
</service>
```

Livy server will use proxyUser to run the Spark session. To avoid that a user can provide here any user (e.g. a more privileged), Knox will need to rewrite the JSON body to replace what so ever is the value of proxyUser is with the username of the authenticated user.

```json
{  
  "driverMemory":"2G",
  "executorCores":4,
  "executorMemory":"8G",
  "proxyUser":"bernhard",
  "conf":{  
    "spark.master":"yarn-cluster",
    "spark.jars.packages":"com.databricks:spark-csv_2.10:1.5.0"
  }
} 
```

The above is an example request body to be used to create a Spark session via Livy server and illustrates the “proxyUser” that requires rewrite.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Elasticsearch"></a>Elasticsearch [](#knox-apache-org-books-knox-2-1-0-user-guide--Elasticsearch)

Elasticsearch provides a REST API for communicating with Elasticsearch via JSON over HTTP. Elasticsearch uses X-Pack to do its own security (authentication and authorization). Therefore, the Knox Gateway is to forward the user credentials to Elasticsearch, and treats the Elasticsearch-authenticated user as “anonymous” to the backend service via a doas query param while Knox will authenticate to backend services as itself.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Gateway+configuration"></a>Gateway configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Gateway+configuration)

The Gateway can be configured for Elasticsearch by modifying the topology XML file and providing a new service XML file.

In the topology XML file, add the following new service named “ELASTICSEARCH” with the correct elasticsearch-rest-server hostname and port number (e.g., 9200):

```xml
 <service>
   <role>ELASTICSEARCH</role>
   <url>http://<elasticsearch-rest-server>:9200/</url>
   <name>elasticsearch</name>
 </service>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Elasticsearch+via+Knox+Gateway"></a>Elasticsearch via Knox Gateway [](#knox-apache-org-books-knox-2-1-0-user-guide--Elasticsearch+via+Knox+Gateway)

After adding the above to a topology, you can make a cURL request similar to the following structures:

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--1.++Elasticsearch+Node+Root+Query"></a>1. Elasticsearch Node Root Query [](#knox-apache-org-books-knox-2-1-0-user-guide--1.++Elasticsearch+Node+Root+Query)

```bash
curl -i -k -u username:password -H "Accept: application/json"  -X GET  "https://{gateway-hostname}:{gateway-port}/gateway/{topology-name}/elasticsearch"

or

curl -i -k -u username:password -H "Accept: application/json"  -X GET  "https://{gateway-hostname}:{gateway-port}/gateway/{topology-name}/elasticsearch/"
```

The quotation marks around the URL, can be single quotes or double quotes on both sides, and can also be omitted (Note: This is true for all other Elasticsearch queries via Knox). Below is an example response:

```text
 HTTP/1.1 200 OK
 Date: Wed, 23 May 2018 16:36:34 GMT
 Content-Type: application/json; charset=UTF-8
 Content-Length: 356
 Server: Jetty(9.2.15.v20160210)

 {"name":"w0A80p0","cluster_name":"elasticsearch","cluster_uuid":"poU7j48pSpu5qQONr64HLQ","version":{"number":"6.2.4","build_hash":"ccec39f","build_date":"2018-04-12T20:37:28.497551Z","build_snapshot":false,"lucene_version":"7.2.1","minimum_wire_compatibility_version":"5.6.0","minimum_index_compatibility_version":"5.0.0"},"tagline":"You Know, for Search"}
```

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--2.++Elasticsearch+Index+-+Creation,+Deletion,+Refreshing+and+Data+Operations+-+Writing,+Updating+and+Retrieval"></a>2. Elasticsearch Index - Creation, Deletion, Refreshing and Data Operations - Writing, Updating and Retrieval [](#knox-apache-org-books-knox-2-1-0-user-guide--2.++Elasticsearch+Index+-+Creation,+Deletion,+Refreshing+and+Data+Operations+-+Writing,+Updating+and+Retrieval)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--(1)+Index+Creation"></a>(1) Index Creation [](#knox-apache-org-books-knox-2-1-0-user-guide--(1)+Index+Creation)

```bash
curl -i -k -u username:password -H "Content-Type: application/json"  -X PUT  "https://{gateway-hostname}:{gateway-port}/gateway/{topology-name}/elasticsearch/{index-name}"  -d '{
"settings" : {
    "index" : {
        "number_of_shards" : {index-shards-number},
        "number_of_replicas" : {index-replicas-number}
    }
  }
}'
```

Below is an example response:

```text
 HTTP/1.1 200 OK
 Date: Wed, 23 May 2018 16:51:31 GMT
 Content-Type: application/json; charset=UTF-8
 Content-Length: 65
 Server: Jetty(9.2.15.v20160210)

 {"acknowledged":true,"shards_acknowledged":true,"index":"estest"}
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--(2)+Index+Data+Writing"></a>(2) Index Data Writing [](#knox-apache-org-books-knox-2-1-0-user-guide--(2)+Index+Data+Writing)

For adding a “Hello Joe Smith” document:

```bash
curl -i -k -u username:password -H "Content-Type: application/json"  -X PUT  "https://{gateway-hostname}:{gateway-port}/gateway/{topology-name}/elasticsearch/{index-name}/{document-type-name}/{document-id}"  -d '{
    "title":"Hello Joe Smith" 
}'
```

Below is an example response:

```text
 HTTP/1.1 201 Created
 Date: Wed, 23 May 2018 17:00:17 GMT
 Location: /estest/greeting/1
 Content-Type: application/json; charset=UTF-8
 Content-Length: 158
 Server: Jetty(9.2.15.v20160210)

 {"_index":"estest","_type":"greeting","_id":"1","_version":1,"result":"created","_shards":{"total":1,"successful":1,"failed":0},"_seq_no":0,"_primary_term":1}
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--(3)+Index+Refreshing"></a>(3) Index Refreshing [](#knox-apache-org-books-knox-2-1-0-user-guide--(3)+Index+Refreshing)

```bash
curl -i -k -u username:password  -X POST  "https://{gateway-hostname}:{gateway-port}/gateway/{topology-name}/elasticsearch/{index-name}/_refresh" 
```

Below is an example response:

```text
 HTTP/1.1 200 OK
 Date: Wed, 23 May 2018 17:02:32 GMT
 Content-Type: application/json; charset=UTF-8
 Content-Length: 49
 Server: Jetty(9.2.15.v20160210)

 {"_shards":{"total":1,"successful":1,"failed":0}}
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--(4)+Index+Data+Upgrading"></a>(4) Index Data Upgrading [](#knox-apache-org-books-knox-2-1-0-user-guide--(4)+Index+Data+Upgrading)

For changing the Person Joe Smith to Tom Smith:

```bash
curl -i -k -u username:password -H "Content-Type: application/json"  -X PUT  "https://{gateway-hostname}:{gateway-port}/gateway/{topology-name}/elasticsearch/{index-name}/{document-type-name}/{document-id}"  -d '{ 
"title":"Hello Tom Smith" 
}'
```

Below is an example response:

```sql
 HTTP/1.1 200 OK
 Date: Wed, 23 May 2018 17:09:59 GMT
 Content-Type: application/json; charset=UTF-8
 Content-Length: 158
 Server: Jetty(9.2.15.v20160210)

 {"_index":"estest","_type":"greeting","_id":"1","_version":2,"result":"updated","_shards":{"total":1,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1}
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--(5)+Index+Data+Retrieval+or+Search"></a>(5) Index Data Retrieval or Search [](#knox-apache-org-books-knox-2-1-0-user-guide--(5)+Index+Data+Retrieval+or+Search)

For finding documents with “title”:“Hello” in a specified document-type:

```bash
curl -i -k -u username:password -H "Accept: application/json" -X GET  "https://{gateway-hostname}:{gateway-port}/gateway/{topology-name}/elasticsearch/{index-name}/{document-type-name}/ _search?pretty=true;q=title:Hello"
```

Below is an example response:

```text
 HTTP/1.1 200 OK
 Date: Wed, 23 May 2018 17:13:08 GMT
 Content-Type: application/json; charset=UTF-8
 Content-Length: 244
 Server: Jetty(9.2.15.v20160210)

 {"took":0,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":1,"max_score":0.2876821,"hits":[{"_index":"estest","_type":"greeting","_id":"1","_score":0.2876821,"_source":{"title":"Hello Tom Smith"}}]}}
```

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--(6)+Index+Deleting"></a>(6) Index Deleting [](#knox-apache-org-books-knox-2-1-0-user-guide--(6)+Index+Deleting)

```bash
curl -i -k -u username:password  -X DELETE  "https://{gateway-hostname}:{gateway-port}/gateway/{topology-name}/elasticsearch/{index-name}"
```

Below is an example response:

```text
 HTTP/1.1 200 OK
 Date: Wed, 23 May 2018 17:20:19 GMT
 Content-Type: application/json; charset=UTF-8
 Content-Length: 21
 Server: Jetty(9.2.15.v20160210)

 {"acknowledged":true}
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--TLS/SSL+Certificate+Trust"></a>TLS/SSL Certificate Trust [](#knox-apache-org-books-knox-2-1-0-user-guide--TLS/SSL+Certificate+Trust)

When the Gateway dispatches requests to a configured service using TLS/SSL, that service’s certificate must be trusted inorder for the connection to succeed. To do this, the Gateway checks a configured trust store for the service’s certificate or the certificate of the CA that issued that certificate.

If not explicitly set, the Gateway will use its configured identity keystore as the trust store. By default, this keystore is located at `{GATEWAY_HOME}/data/security/keystores/gateway.jks`; however, a custom identity keystore may be set in the gateway-site.xml file. See `gateway.tls.keystore.password.alias`, `gateway.tls.keystore.path`, and `gateway.tls.keystore.type`.

The trust store is configured at the Gatway-level. There is no support to set a different trust store per service. To use a specific trust store, the following configuration elements may be set in the gateway-site.xml file:

| Configuration Element | Description |
| --- | --- |
| gateway.httpclient.truststore.path | Fully qualified path to the trust store to use. Default is the keystore used to hold the Gateway’s identity. Seegateway.tls.keystore.path. |
| gateway.httpclient.truststore.type | Keystore type of the trust store. Default is JKS. |
| gateway.httpclient.truststore.password.alias | Alias for the password to the trust store. |

If `gateway.httpclient.truststore.path` is not set, the keystore used to hold the Gateway’s identity will be used as the trust store.

However, if `gateway.httpclient.truststore.path` is set, it is expected that `gateway.httpclient.truststore.type` and `gateway.httpclient.truststore.password.alias` are set appropriately. If `gateway.httpclient.truststore.type` is not set, the Gateway will assume the trust store is a JKS file. If `gateway.httpclient.truststore.password.alias` is not set, the Gateway will assume the alias name is “gateway-httpclient-truststore-password”. In any case, if the trust store password is different from the Gateway’s master secret then it can be set using

```text
knoxcli.sh create-alias {password-alias} --value {pwd} 
```

If a password is not found using the provided (or default) alias name, then the Gateway’s master secret will be used.

All topologies deployed within the Gateway instance will use the configured trust store to verify a service’s identity.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Service+Test+API"></a>Service Test API [](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Test+API)

The gateway supports a Service Test API that can be used to test Knox’s ability to connect to each of the different Hadoop services via a simple HTTP GET request. To be able to access this API, one must add the following lines into the topology for which you wish to run the service test.

```xml
<service>
  <role>SERVICE-TEST</role>
</service>
```

After adding the above to a topology, you can make a cURL request with the following structure

```bash
curl -i -k "https://{gateway-hostname}:{gateway-port}/gateway/{topology-name}/service-test?username=guest&password=guest-password"
```

An alternate method of providing credentials:

```bash
curl -i -k -u guest:guest-password https://{gateway-hostname}:{gateway-port}/gateway/{topology-name}/service-test
```

Below is an example response. The gateway is also capable of returning XML if specified in the request’s “Accept” HTTP header.

```json
{
    "serviceTestWrapper": {
     "Tests": {
      "ServiceTest": [
       {
        "serviceName": "WEBHDFS",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/webhdfs/v1/?op=LISTSTATUS",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "WEBHCAT",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/templeton/v1/status",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "WEBHCAT",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/templeton/v1/version",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "WEBHCAT",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/templeton/v1/version/hive",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "WEBHCAT",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/templeton/v1/version/hadoop",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "OOZIE",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/oozie/v1/admin/build-version",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "OOZIE",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/oozie/v1/admin/status",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "OOZIE",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/oozie/versions",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "WEBHBASE",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/hbase/version",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "WEBHBASE",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/hbase/version/cluster",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "WEBHBASE",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/hbase/status/cluster",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "WEBHBASE",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/hbase",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "RESOURCEMANAGER",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/resourcemanager/v1/{topology-name}/info",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "RESOURCEMANAGER",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/resourcemanager/v1/{topology-name}/metrics",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "RESOURCEMANAGER",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/resourcemanager/v1/{topology-name}/apps",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "FALCON",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/falcon/api/admin/stack",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "FALCON",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/falcon/api/admin/version",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "FALCON",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/falcon/api/metadata/lineage/serialize",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "FALCON",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/falcon/api/metadata/lineage/vertices/all",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "FALCON",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/falcon/api/metadata/lineage/edges/all",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "STORM",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/storm/api/v1/cluster/configuration",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "STORM",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/storm/api/v1/cluster/summary",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "STORM",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/storm/api/v1/supervisor/summary",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       },
       {
        "serviceName": "STORM",
        "requestURL": "http://{gateway-host}:{gateway-port}/gateway/{topology-name}/storm/api/v1/topology/summary",
        "responseContent": "Content-Length:0,Content-Type: application/json;charset=utf-8",
        "httpCode": 200,
        "message": "Request sucessful."
       }
      ]
     },
     "messages": {
      "message": [

      ]
     }
    }
}
```

We can see that this service-test makes HTTP requests to each of the services through Knox using the specified topology. The test will only make calls to those services that have entries within the topology file.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Adding+and+Changing+test+URLs"></a>Adding and Changing test URLs [](#knox-apache-org-books-knox-2-1-0-user-guide--Adding+and+Changing+test+URLs)

URLs for each service are stored in `{GATEWAY_HOME}/data/services/{service-name}/{service-version}/service.xml`. Each `<testURL>` element represents a service resource that will be tested if the service is set up in the topology. You can add or remove these from the `service.xml` file. Just note if you add URLs there is no guarantee in the order they will be tested. All default URLs have been tested and work on various clusters. If a new URL is added and doesn’t respond in a way the user expects then it is up to the user to determine whether the URL is correct or not.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Some+important+things+to+note:"></a>Some important things to note: [](#knox-apache-org-books-knox-2-1-0-user-guide--Some+important+things+to+note:)

- In the first cURL request, the quotes are necessary around the URL or else a command line terminal will not include the `&password` query parameter in the request.
- This API call does not require any credentials to receive a response from Knox, but expect to receive 401 responses from each of the services if none are provided.

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--UI+Service+Details"></a>UI Service Details [](#knox-apache-org-books-knox-2-1-0-user-guide--UI+Service+Details)

In the sections that follow, the integrations for proxying various UIs currently available out of the box with the gateway will be described. These sections will include examples that demonstrate how to access each of these services via the gateway.

These are the current Hadoop services with built-in support for their UIs.

- [Name Node UI](#knox-apache-org-books-knox-2-1-0-user-guide--Name+Node+UI)
- [Job History UI](#knox-apache-org-books-knox-2-1-0-user-guide--Job+History+UI)
- [Oozie UI](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie+UI)
- [HBase UI](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+UI)
- [Yarn UI](#knox-apache-org-books-knox-2-1-0-user-guide--Yarn+UI)
- [Spark UI](#knox-apache-org-books-knox-2-1-0-user-guide--Spark+UI)
- [Ambari UI](#knox-apache-org-books-knox-2-1-0-user-guide--Ambari+UI)
- [Ranger Admin Console](#knox-apache-org-books-knox-2-1-0-user-guide--Ranger+Admin+Console)
- [Atlas UI](#knox-apache-org-books-knox-2-1-0-user-guide--Atlas+UI)
- [Zeppelin UI](#knox-apache-org-books-knox-2-1-0-user-guide--Zeppelin+UI)
- [Nifi UI](#knox-apache-org-books-knox-2-1-0-user-guide--Nifi+UI)
- [Hue UI](#knox-apache-org-books-knox-2-1-0-user-guide--Hue+UI)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Assumptions"></a>Assumptions [](#knox-apache-org-books-knox-2-1-0-user-guide--Assumptions)

This section assumes an environment setup similar to the one in the REST services section [Service Details](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Details)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Name+Node+UI"></a>Name Node UI [](#knox-apache-org-books-knox-2-1-0-user-guide--Name+Node+UI)

The Name Node UI is available on the same host and port combination that WebHDFS is available on. As mentioned in the WebHDFS REST service configuration section, the values for the host and port can be obtained from the following properties in hdfs-site.xml

```xml
<property>
    <name>dfs.namenode.http-address</name>
    <value>sandbox.hortonworks.com:50070</value>
</property>
<property>
    <name>dfs.https.namenode.https-address</name>
    <value>sandbox.hortonworks.com:50470</value>
</property>
```

The values above need to be reflected in each topology descriptor file deployed to the gateway. The gateway by default includes a sample topology descriptor file `{GATEWAY_HOME}/deployments/sandbox.xml`. The values in this sample are configured to work with an installed Sandbox VM.

```xml
<service>
    <role>HDFSUI</role>
    <url>http://sandbox.hortonworks.com:50070</url>
</service>
```

In addition to the service configuration for HDFSUI, the REST service configuration for WEBHDFS is also required.

```xml
<service>
    <role>NAMENODE</role>
    <url>hdfs://sandbox.hortonworks.com:8020</url>
</service>
<service>
    <role>WEBHDFS</role>
    <url>http://sandbox.hortonworks.com:50070/webhdfs</url>
</service>
```

By default the gateway is configured to use the HTTP endpoint for WebHDFS in the Sandbox. This could alternatively be configured to use the HTTPS endpoint by providing the correct address.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Name+Node+UI+URL+Mapping"></a>Name Node UI URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Name+Node+UI+URL+Mapping)

For Name Node UI URLs, the mapping of Knox Gateway accessible HDFS UI URLs to direct HDFS UI URLs is:

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/hdfs |
| --- | --- |
| Cluster | http://{webhdfs-host}:50070/ |

For example to browse the file system using the NameNode UI the URL in a web browser would be:

```text
http://sandbox.hortonworks.com:50070/explorer.html#
```

And using the gateway to access the same page the URL would be (where the gateway host:port is ‘localhost:8443’)

```text
https://localhost:8443/gateway/sandbox/hdfs/explorer.html#
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Job+History+UI"></a>Job History UI [](#knox-apache-org-books-knox-2-1-0-user-guide--Job+History+UI)

The Job History UI service can be configured in a topology by adding the following snippet. The values in this sample are configured to work with an installed Sandbox VM.

```xml
<service>
    <role>JOBHISTORYUI</role>
    <url>http://sandbox.hortonworks.com:19888</url>
</service>
```

The values for the host and port can be obtained from the following property in mapred-site.xml

```xml
<property>
    <name>mapreduce.jobhistory.webapp.address</name>
    <value>sandbox.hortonworks.com:19888</value>
</property>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Job+History+UI+URL+Mapping"></a>Job History UI URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Job+History+UI+URL+Mapping)

For Job History UI URLs, the mapping of Knox Gateway accessible Job History UI URLs to direct Job History UI URLs is:

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/jobhistory |
| --- | --- |
| Cluster | http://{jobhistory-host}:19888/jobhistory |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Oozie+UI"></a>Oozie UI [](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie+UI)

The Oozie UI service can be configured in a topology by adding the following snippet. The values in this sample are configured to work with an installed Sandbox VM.

```xml
<service>
    <role>OOZIEUI</role>
    <url>http://sandbox.hortonworks.com:11000/oozie</url>
</service>
```

The value for the URL can be obtained from the following property in oozie-site.xml

```xml
<property>
    <name>oozie.base.url</name>
    <value>http://sandbox.hortonworks.com:11000/oozie</value>
</property>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Oozie+UI+URL+Mapping"></a>Oozie UI URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Oozie+UI+URL+Mapping)

For Oozie UI URLs, the mapping of Knox Gateway accessible Oozie UI URLs to direct Oozie UI URLs is:

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/oozie/ |
| --- | --- |
| Cluster | http://{oozie-host}:11000/oozie/ |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase+UI"></a>HBase UI [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+UI)

The HBase UI service can be configured in a topology by adding the following snippet. The values in this sample are configured to work with an installed Sandbox VM.

```xml
<service>
    <role>HBASEUI</role>
    <url>http://sandbox.hortonworks.com:16010</url>
</service>
```

The values for the host and port can be obtained from the following property in hbase-site.xml. Below the hostname of the HBase master is used since the bindAddress is 0.0.0.0

```xml
<property>
    <name>hbase.master.info.bindAddress</name>
    <value>0.0.0.0</value>
</property>
<property>
    <name>hbase.master.info.port</name>
    <value>16010</value>
</property>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase+UI+URL+Mapping"></a>HBase UI URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+UI+URL+Mapping)

For HBase UI URLs, the mapping of Knox Gateway accessible HBase UI URLs to direct HBase Master UI URLs is:

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/hbase/webui/ |
| --- | --- |
| Cluster | http://{hbase-master-host}:16010/ |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--YARN+UI"></a>YARN UI [](#knox-apache-org-books-knox-2-1-0-user-guide--YARN+UI)

The YARN UI service can be configured in a topology by adding the following snippet. The values in this sample are configured to work with an installed Sandbox VM.

```xml
<service>
    <role>YARNUI</role>
    <url>http://sandbox.hortonworks.com:8088</url>
</service>
```

The values for the host and port can be obtained from the following property in mapred-site.xml

```xml
<property>
    <name>yarn.resourcemanager.webapp.address</name>
    <value>sandbox.hortonworks.com:8088</value>
</property>
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--YARN+UI+URL+Mapping"></a>YARN UI URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--YARN+UI+URL+Mapping)

For Resource Manager UI URLs, the mapping of Knox Gateway accessible Resource Manager UI URLs to direct Resource Manager UI URLs is:

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/yarn |
| --- | --- |
| Cluster | http://{resource-manager-host}:8088/cluster |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Spark+UI"></a>Spark UI [](#knox-apache-org-books-knox-2-1-0-user-guide--Spark+UI)

The Spark History UI service can be configured in a topology by adding the following snippet. The values in this sample are configured to work with an installed Sandbox VM.

```xml
<service>
    <role>SPARKHISTORYUI</role>
    <url>http://sandbox.hortonworks.com:18080/</url>
</service>
```

Please, note that for Spark versions older than 2.4.0 you need to set `spark.ui.proxyBase` to `/{gateway-path}/{cluster-name}/sparkhistory` (for more information, please refer to SPARK-24209).

Moreover, before Spark 2.3.1, there is a bug is Spark which prevents the UI to work properly when the proxy is accessed using a link which ends with “/” (SPARK-23644). So if the list of the applications is empty when you access the UI though the gateway, please check that there is a “/” at the end of the URL you are using.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Spark+History+UI+URL+Mapping"></a>Spark History UI URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Spark+History+UI+URL+Mapping)

For Spark History UI URLs, the mapping of Knox Gateway accessible Spark History UI URLs to direct Spark History UI URLs is:

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/sparkhistory |
| --- | --- |
| Cluster | http://{spark-history-host}:18080 |

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Ambari+UI"></a>Ambari UI [](#knox-apache-org-books-knox-2-1-0-user-guide--Ambari+UI)

Ambari UI has functionality around provisioning and managing services in a Hadoop cluster. This UI can now be used behind the Knox gateway.

To enable this functionality, a topology file needs to have the following configuration (for AMBARIUI and AMBARIWS):

```xml
<service>
    <role>AMBARIUI</role>
    <url>http://<hostname>:<port></url>
</service>

<service>
    <role>AMBARIWS</role>
    <url>ws://<hostname>:<port></url>
</service>
```

The default Ambari http port is 8080. Also, please note that the UI service also requires the Ambari REST API service and Ambari Websocket service to be enabled to function properly. An example of a more complete topology is given below.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Ambari+UI+URL+Mapping"></a>Ambari UI URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Ambari+UI+URL+Mapping)

For Ambari UI URLs, the mapping of Knox Gateway accessible URLs to direct Ambari UI URLs is the following.

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/ambari/ |
| --- | --- |
| Cluster | http://{ambari-host}:{ambari-port}/} |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Example+Topology"></a>Example Topology [](#knox-apache-org-books-knox-2-1-0-user-guide--Example+Topology)

The Ambari UI service may require a separate topology file due to its requirements around authentication. Knox passes through authentication challenge and credentials to the service in this case.

```xml
<topology>
    <gateway>
        <provider>
            <role>authentication</role>
            <name>Anonymous</name>
            <enabled>true</enabled>
        </provider>
        <provider>
            <role>identity-assertion</role>
            <name>Default</name>
            <enabled>false</enabled>
        </provider>
    </gateway>
    <service>
        <role>AMBARI</role>
        <url>http://localhost:8080</url>
    </service>

    <service>
        <role>AMBARIUI</role>
        <url>http://localhost:8080</url>
    </service>
    <service>
        <role>AMBARIWS</role>
        <url>ws://localhost:8080</url>
    </service>
</topology>
```

Please look at JIRA issue [KNOX-705] for a known issue with this release.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Ranger+Admin+Console"></a>Ranger Admin Console [](#knox-apache-org-books-knox-2-1-0-user-guide--Ranger+Admin+Console)

The Ranger Admin console can now be used behind the Knox gateway.

To enable this functionality, a topology file needs to have the following configuration:

```xml
<service>
    <role>RANGERUI</role>
    <url>http://<hostname>:<port></url>
</service>
```

The default Ranger http port is 8060. Also, please note that the UI service also requires the Ranger REST API service to be enabled to function properly. An example of a more complete topology is given below.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Ranger+Admin+Console+URL+Mapping"></a>Ranger Admin Console URL Mapping [](#knox-apache-org-books-knox-2-1-0-user-guide--Ranger+Admin+Console+URL+Mapping)

For Ranger Admin console URLs, the mapping of Knox Gateway accessible URLs to direct Ranger Admin console URLs is the following.

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{cluster-name}/ranger/ |
| --- | --- |
| Cluster | http://{ranger-host}:{ranger-port}/} |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Example+Topology"></a>Example Topology [](#knox-apache-org-books-knox-2-1-0-user-guide--Example+Topology)

The Ranger UI service may require a separate topology file due to its requirements around authentication. Knox passes through authentication challenge and credentials to the service in this case.

```xml
<topology>
    <gateway>
        <provider>
            <role>authentication</role>
            <name>Anonymous</name>
            <enabled>true</enabled>
        </provider>
        <provider>
            <role>identity-assertion</role>
            <name>Default</name>
            <enabled>false</enabled>
        </provider>
    </gateway>
    <service>
        <role>RANGER</role>
        <url>http://localhost:8060</url>
    </service>

    <service>
        <role>RANGERUI</role>
        <url>http://localhost:8060</url>
    </service>
</topology>

    </service>
</topology>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Atlas+UI"></a>Atlas UI [](#knox-apache-org-books-knox-2-1-0-user-guide--Atlas+UI)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Atlas+Rest+API"></a>Atlas Rest API [](#knox-apache-org-books-knox-2-1-0-user-guide--Atlas+Rest+API)

The Atlas Rest API can now be used behind the Knox gateway. To enable this functionality, a topology file needs to have the following configuration.

```xml
<service>
    <role>ATLAS-API</role>
    <url>http://<ATLAS_HOST>:<ATLAS_PORT></url>
</service>
```

The default Atlas http port is 21000. Also, please note that the UI service also requires the Atlas REST API service to be enabled to function properly. An example of a more complete topology is given below.

Atlas Rest API URL Mapping For Atlas Rest URLs, the mapping of Knox Gateway accessible URLs to direct Atlas Rest URLs is the following.

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{topology}/atlas/ |
| --- | --- |
| Cluster | http://{atlas-host}:{atlas-port}/} |

Access Atlas API using cULR call

```bash
 curl -i -k -L -u admin:admin -X GET \
           'https://knox-gateway:8443/gateway/{topology}/atlas/api/atlas/v2/types/typedefs?type=classification&_=1495442879421'
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Atlas+UI"></a>Atlas UI [](#knox-apache-org-books-knox-2-1-0-user-guide--Atlas+UI)

In addition to the Atlas REST API, from this release there is the ability to access some of the functionality via a web. The initial functionality is very limited and serves more as a starting point/placeholder. The details are below. Atlas UI URL

The URL mapping for the Atlas UI is:

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{topology}/atlas/index.html |
| --- | --- |

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Example+Topology+for+Atlas"></a>Example Topology for Atlas [](#knox-apache-org-books-knox-2-1-0-user-guide--Example+Topology+for+Atlas)

```xml
            <topology>
                <gateway>
                    <provider>
                        <role>authentication</role>
                        <name>Anonymous</name>
                        <enabled>true</enabled>
                    </provider>
                    <provider>
                        <role>identity-assertion</role>
                        <name>Default</name>
                        <enabled>false</enabled>
                    </provider>
                </gateway>

                <service>
                    <role>ATLAS-API</role>
                    <url>http://<ATLAS_HOST>:<ATLAS_PORT></url>
                </service>

                <service>
                    <role>ATLAS</role>
                    <url>http://<ATLAS_HOST>:<ATLAS_PORT></url>
                </service>
            </topology>
```

Note: This feature will allow for ‘anonymous’ authentication. Essentially bypassing any LDAP or other authentication done by Knox and allow the proxied service to do the actual authentication.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Zeppelin+UI"></a>Zeppelin UI [](#knox-apache-org-books-knox-2-1-0-user-guide--Zeppelin+UI)

Apache Knox can be used to proxy Zeppelin UI and also supports WebSocket protocol used by Zeppelin.

The URL mapping for the Zeppelin UI is:

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{topology}/zeppelin/ |
| --- | --- |

By default WebSocket functionality is disabled, it needs to be enabled for Zeppelin UI to work properly, it can be enabled by changing the `gateway.websocket.feature.enabled` property to ‘true’ in `<KNOX-HOME>/conf/gateway-site.xml` file, for e.g.

```xml
<property>
    <name>gateway.websocket.feature.enabled</name>
    <value>true</value>
    <description>Enable/Disable websocket feature.</description>
</property>
```

Example service definition for Zeppelin in topology file is as follows, note that both ZEPPELINWS and ZEPPELINUI service declarations are required.

```xml
<service>
    <role>ZEPPELINWS</role>
    <url>ws://<ZEPPELIN_HOST>:<ZEPPELIN_PORT>/ws</url>
</service>

<service>
    <role>ZEPPELINUI</role>
    <url>http://<ZEPPELIN_HOST>:<ZEPPELIN_PORT></url>
</service>
```

Knox also supports secure Zeppelin UIs, for secure UIs one needs to provision Zeppelin certificate into Knox truststore.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Nifi+UI"></a>Nifi UI [](#knox-apache-org-books-knox-2-1-0-user-guide--Nifi+UI)

You can use the Apache Knox Gateway to provide authentication access security for your NiFi services.

The URL mapping for the NiFi UI is:

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{topology}/nifi-app/nifi/ |
| --- | --- |

The Gateway can be configured for Nifi by modifying the topology XML file.

In the topology XML file, add the following with the correct hostname and port:

```xml
<service>
  <role>NIFI</role>
  <url><NIFI_HTTP_SCHEME>://<NIFI_HOST>:<NIFI_HTTP_SCHEME_PORT></url>
  <param name="useTwoWaySsl" value="true"/>
</service>
```

Note the setting of the useTwoWaySsl param above. Nifi requires mutual authentication via SSL and this param tells the dispatch to present a client cert to the server.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hue+UI"></a>Hue UI [](#knox-apache-org-books-knox-2-1-0-user-guide--Hue+UI)

You can use the Apache Knox Gateway to provide authentication access security for your Hue UI.

The URL mapping for the Hue UI is:

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/{topology}/hue/ |
| --- | --- |

The Gateway can be configured for Hue by modifying the topology XML file.

In the topology XML file, add the following with the correct hostname and port:

```xml
<service>
  <role>HUE</role>
  <url><HUE_HTTP_SCHEME>://<HUE_HOST>:<HUE_HTTP_SCHEME_PORT></url>
</service>
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Admin+UI"></a>Admin UI [](#knox-apache-org-books-knox-2-1-0-user-guide--Admin+UI)

The Admin UI is a web application hosted by Knox, which provides the ability to manage provider configurations, descriptors, topologies ans service definitions.

As an authoring facility, it eliminates the need for ssh/scp access to the Knox host(s) to effect topology changes.  
 Furthermore, using the Admin UI simplifies the management of topologies in Knox HA deployments by eliminating the need to copy files to multiple Knox hosts.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Admin+UI+URL"></a>Admin UI URL [](#knox-apache-org-books-knox-2-1-0-user-guide--Admin+UI+URL)

The URL mapping for the Knox Admin UI is:

| Gateway | https://{gateway-host}:{gateway-port}/{gateway-path}/manager/admin-ui/ |
| --- | --- |

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Authentication"></a>Authentication [](#knox-apache-org-books-knox-2-1-0-user-guide--Authentication)

The admin UI is deployed using the **manager** topology. The out-of-box authentication mechanism is KNOXSSO, backed by the demo LDAP server. Only someone in the **admin** role can access the UI functionality.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Basic+Navigation"></a>Basic Navigation [](#knox-apache-org-books-knox-2-1-0-user-guide--Basic+Navigation)

Initially, the Admin UI presents the types of resources which can be managed: [**Provider Configurations**](#knox-apache-org-books-knox-2-1-0-user-guide--Provider+Configurations), [**Descriptors**](#knox-apache-org-books-knox-2-1-0-user-guide--Descriptors), [**Topologies**](#knox-apache-org-books-knox-2-1-0-user-guide--Topologies) and [**Service Definitions**](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Definitions).

![](adminui/image1.png)

Selecting a resource type yields a listing of the existing resources of that type in the adjacent column, and selecting an individual resource presents the details of that selected resource.

For the provider configuration, descriptor and service definition resources types, the ![](adminui/plus-icon.png) icon next to the resource list header is the trigger for the respective facility for creating a new resource of that type.  
 Modification options, including deletion, are available from the detail view for an individual resource.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Provider+Configurations"></a>Provider Configurations [](#knox-apache-org-books-knox-2-1-0-user-guide--Provider+Configurations)

The Admin UI lists the provider configurations currently deployed to Knox.

By choosing a particular provider configuration from the list, its details can be viewed and edited.  
 The provider configuration can also be deleted (as long as there are no referencing descriptors).

By default, there is a provider configuration named ***default-providers***.

![](adminui/image2.png)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Editing+Provider+Configurations"></a>Editing Provider Configurations [](#knox-apache-org-books-knox-2-1-0-user-guide--Editing+Provider+Configurations)

For each provider in a given provider configuration, the attributes can be modified:

- The provider can be enabled/disabled
- Parameters can be added (![](adminui/plus-icon.png)) or removed (![](adminui/x-icon.png))
- Parameter values can be modified (by clicking on the value) ![](adminui/image21.png)

  

To persist changes, the ![](adminui//save-icon.png) button must be clicked. To revert *unsaved* changes, click the ![](adminui//undo-icon.png) button or simply choose another resource.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Create+Provider+Configurations"></a>Create Provider Configurations [](#knox-apache-org-books-knox-2-1-0-user-guide--Create+Provider+Configurations)

The Admin UI provides the ability to define new provider configurations, which can subsequently be referenced by one or more descriptors.

These provider configurations can be created based on the functionality needed, rather than requiring intimate knowledge of the various provider names and their respective parameter names.

A provider configuration is a named set of providers. The wizard allows an administrator to specify the name, and add providers to it.

![](adminui/image3.png)

To add a provider, first a category must be chosen.

![](adminui/image4.png)

After choosing a category, the type within that category must be selected.

![](adminui/image5.png)

Finally, for the selected type, the type-specific parameter values can be specified.

![](adminui/image6.png)

After adding a provider, others can be added similarly by way of the **Add Provider** button.

![](adminui/image7.png)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Composite+Provider+Types"></a>Composite Provider Types [](#knox-apache-org-books-knox-2-1-0-user-guide--Composite+Provider+Types)

The wizard for some provider types, such as the HA provider, behave a little differently than the other provider types.

For example, when you choose the HA provider category, you subsequently choose a service role (e.g., WEBHDFS), and specify the parameter values for that service role’s entry in the HA provider.

![](adminui/image8.png)
![](adminui/image9.png)

If multiple services are configured in this way, the result is still a single HA provider, which contains all of the service role configurations.

![](adminui/image10.png)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Persisting+the+New+Provider+Configuration"></a>Persisting the New Provider Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Persisting+the+New+Provider+Configuration)

After adding all the desired providers to the new configuration, choosing ![](adminui/ok-button.png) persists it.

![](adminui/image11.png)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Descriptors"></a>Descriptors [](#knox-apache-org-books-knox-2-1-0-user-guide--Descriptors)

A descriptor is essentially a named set of service roles to be proxied with a provider configuration reference. The Admin UI lists the descriptors currently deployed to Knox.

By choosing a particular descriptor from the list, its details can be viewed and edited. The provider configuration can also be deleted.

Modifications to descriptors will result in topology changes. When a descriptor is saved or deleted, the corresponding topology is [re]generated or deleted/undeployed respectively.

![](adminui/image12.png)
![](adminui/image13.png)
![](adminui/image14.png)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Create+Descriptors"></a>Create Descriptors [](#knox-apache-org-books-knox-2-1-0-user-guide--Create+Descriptors)

The Admin UI provides the ability to define new descriptors, which result in the generation and deployment of corresponding topologies.

The **new descriptor** dialog provides the ability to specify the name, which will also be the name of the resulting topology. It also allows one or more supported service roles to be selected for inclusion.

![](adminui/image15.png)

The provider configuration reference can entered manually, or the provider configuration selector can be used, to specify the name of an existing provider configuration.

![](adminui/image16.png)

Optionally, discovery details can also be specified to direct Knox to discover the endpoints for the declared service roles from a supported discovery source for the target cluster.

![](adminui/image17.png)

Choosing ![](adminui/ok-button.png) results in the persistence of the descriptor, and subsequently, the generation and deployment of the associated topology.

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Service+Discovery"></a>Service Discovery [](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Discovery)

Descriptors are a means to *declaratively* specify which services should be proxied by a particular topology, allowing Knox to interrogate a discovery source to determine the endpoint URLs for those declared services. The Service Discovery options tell Knox how to connect to the desired discovery source to perform this endpoint discovery.

*Type*

This property specifies the type of discovery source for the cluster hosting the services whose endpoints are to be discovered.

*Address*

This property specifies the address of the discovery source managing the cluster hosting the services whose endpoints are to be discovered.

*Cluster*

This property specifies from which of the clusters, among those being managed by the specified discovery source, the service endpoints should be determined.

*Username*

This is the identity of the discovery source user (e.g., Ambari *Cluster User* role), which will be used to get service configuration details from the discovery source.

*Password Alias*

This is the Knox alias whose value is the password associated with the specified username.

This alias must have been defined prior to specifying it in a descriptor, or else the service discovery will fail for authentication reasons.

![](adminui/image18.png)

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Topologies"></a>Topologies [](#knox-apache-org-books-knox-2-1-0-user-guide--Topologies)

The Admin UI allows an administrator to view, modify, duplicate and delete topologies which are currently deployed to the Knox instance. Changes to a topology results in the [re]deployment of that topology, and deleting a topology results in its undeployment.

![](adminui/image19.png)
![](adminui/image20.png)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Read-Only+Protections"></a>Read-Only Protections [](#knox-apache-org-books-knox-2-1-0-user-guide--Read-Only+Protections)

Topologies which are generated from descriptors are treated as read-only in the Admin UI. This is to avoid the potential confusion resulting from an administrator directly editing a generated topology only to have those changes overwritten by a regeneration of that same topology because the source descriptor or provider configuration changed.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Knox+HA+Considerations"></a>Knox HA Considerations [](#knox-apache-org-books-knox-2-1-0-user-guide--Knox+HA+Considerations)

If the Knox instance which is hosting the Admin UI is configured for [remote configuration monitoring](#knox-apache-org-books-knox-2-1-0-user-guide--Remote+Configuration+Monitor), then provider configuration and descriptor changes will be persisted in the configured ZooKeeper ensemble. Then, every Knox instance which is also configured to monitor configuration in this same ZooKeeper will apply those changes, and [re]generate/[re]deploy the affected topologies. In this way, Knox HA deployments can be managed by making changes once, and from any of the Knox instances.

##### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Service+Definitions"></a>Service Definitions [](#knox-apache-org-books-knox-2-1-0-user-guide--Service+Definitions)

The Admin UI allows an administrator to view, modify, create and delete service definitions which are currently supported by Knox.

![](adminui/image22.png)

A service definition is a declarative way of plugging-in a new Service. It consists of two separate files as described in the relevant section in [Knox’s Developer Guide](https://knox.apache.org/books/knox-2-0-0/dev-guide.html#Service+Definition+Files). The Admin UI lists the service definitions currently supported by Knox in a data table ordered by the service name. If a particular service has more than one service definition (for different versions), a separate service definition entry is displayed in the table. Under the table there is a pagination panel that let’s end-users to navigate to the desired service definition.

By choosing a particular service definition from the table, its details can be viewed and edited. The service definition can also be deleted.

![](adminui/image23.png)

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Editing+Service+Definitions"></a>Editing Service Definitions [](#knox-apache-org-books-knox-2-1-0-user-guide--Editing+Service+Definitions)

When a particular service definition is selected, the Admin UI displays a free-text area where the content can be updated and saved. End-users will see the following structure in this text area:

```xml
<serviceDefinition>
    <service>
    ...
    </service>
    <rules>
    ...
    </rules>
</serviceDefinition>
```

Everything within the `<service>` section will be written into the given service’s `service.xml` file whereas the content of `rules` are going into the `rewrite.xml`.

To persist changes, the ![](adminui//save-icon.png) button must be clicked. To revert *unsaved* changes, simply choose another resource. In case you choose to save your changes, a confirmation window is shown asking for your approval, where you can make your changes final by clicking the ![](adminui/ok-button.png) button.

![](adminui/image24.png)
  
  
![](adminui/image25.png)

If you are unsure about the change you made, you can still click the `Cancel` button and select another resource to revert your unsaved change.

**Important note:** editing a service definition will result in redeploying all topologies that include the updated service (identified by it’s name and, optionally, version).

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Deleting+Service+Definitions"></a>Deleting Service Definitions [](#knox-apache-org-books-knox-2-1-0-user-guide--Deleting+Service+Definitions)

Similarly to the service definition editing function, end-users have to select the service defintion first they are about to remove.

The service definition details are displayed along with the ![](adminui//delete-icon.png) button in the bottom-left corner of the service definition details window. To remove the selected service definition, you have to cick that button and you will be shown a confirmation window where you can verify the service definition removal by clicking the ![](adminui/ok-button.png) button.

![](adminui/image26.png)
  
  
![](adminui/image27.png)

**Important note:** deleting a service definition will result in redeploying all topologies that included the removed service (identified by it’s name and, optionally, version).

###### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Creating+Service+Definitions"></a>Creating Service Definitions [](#knox-apache-org-books-knox-2-1-0-user-guide--Creating+Service+Definitions)

The Admin UI provides the ability to define new service definitions which can be included in topologies later on.

The new service definition dialog provides the ability to specify the service name, role and version as well as all the required information in `service.xml` and `rewrite.xml` files such as routes and rewrite rules.

To create a new service provider, please click the ![](adminui/plus-icon.png) button after you selected `Service Definitions` from the `Resource Types` list.

![](adminui/image28.png)

After defining all details, you have to click the ![](adminui/ok-button.png) button to save the newly created service definition on the disk.

![](adminui/tip-icon.png) You may want to copy-paste a valid service definition before you open the new service definition dialog and self-tailor the content for your needs.

  

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Webshell"></a>Webshell [](#knox-apache-org-books-knox-2-1-0-user-guide--Webshell)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Introduction"></a>Introduction [](#knox-apache-org-books-knox-2-1-0-user-guide--Introduction)

This feature enables shell access to the machine running Apache Knox. Users can SSO into Knox and then access shell using the Knox WebShell URL on knox homepage. There are some out of band configuration changes that are required for the feature to work.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Prerequisite"></a>Prerequisite [](#knox-apache-org-books-knox-2-1-0-user-guide--Prerequisite)

This feature works only on \*nix systems given it relies on sudoers file. Make sure following prerequisite are met before turning on the feature.

- Make sure Knox process user (user under which knox process runs) exists on local machine.
- Make sure the user used to login exists on local machine.
- Make sure you have proper rights to create/update sudoers file descibed in “Configuration” section below.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Configuration"></a>Configuration [](#knox-apache-org-books-knox-2-1-0-user-guide--Configuration)

Webshell is not turned on by default. To enable Webshell following properties needs to be changed in `gateway-site.xml`

```text
property>
    <name>gateway.websocket.feature.enabled</name>
    <value>true</value>
    <description>Enable/Disable websocket feature.</description>
</property>

<property>
    <name>gateway.webshell.feature.enabled</name>
    <value>true</value>
    <description>Enable/Disable webshell feature.</description>
</property>
<!-- in case JWT cookie validation for websockets is needed -->
<property>
    <name>gateway.websocket.JWT.validation.feature.enabled</name>
    <value>true</value>
    <description>Enable/Disable websocket JWT validation feature.</description>
</property>
```

Create a sudoers file `/etc/sudoers.d/knox` (assuming, Apache Knox process is running as user `knox`) with mappings for all the users that need WebShell acess on the machine running Apache Knox.

e.g. the following settings in `sudoers` file let’s user `sam` and `knoxui` login to WebShell. Further restrictions on user `sam` and `knoxui` can be applied in `sudoers` file. More info: [https://linux.die.net/man/5/sudoers](https://linux.die.net/man/5/sudoers). Here users `sam` and `knoxui` are SSO users that login using Knox authentication providers such as LDAP, PAM etc.

```text
Defaults env_keep += JAVA_HOME
Defaults always_set_home
knox ALL=(sam:ALL) NOPASSWD: /bin/bash
knox ALL=(knoxui:ALL) NOPASSWD: /bin/bash
```

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Limitations"></a>Limitations [](#knox-apache-org-books-knox-2-1-0-user-guide--Limitations)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Secure+Oozie+POST/PUT+Request+Payload+Size+Restriction"></a>Secure Oozie POST/PUT Request Payload Size Restriction [](#knox-apache-org-books-knox-2-1-0-user-guide--Secure+Oozie+POST/PUT+Request+Payload+Size+Restriction)

With one exception there are no known size limits for requests or responses payloads that pass through the gateway. The exception involves POST or PUT request payload sizes for Oozie in a Kerberos secured Hadoop cluster. In this one case there is currently a 4Kb payload size limit for the first request made to the Hadoop cluster. This is a result of how the gateway negotiates a trust relationship between itself and the cluster via SPNEGO. There is an undocumented configuration setting to modify this limit’s value if required. In the future this will be made more easily configurable and at that time it will be documented.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Group+Membership+Propagation"></a>Group Membership Propagation [](#knox-apache-org-books-knox-2-1-0-user-guide--Group+Membership+Propagation)

Groups that are acquired via Shiro Group Lookup and/or Identity Assertion Group Principal Mapping are not propagated to the Hadoop services. Therefore, groups used for Service Level Authorization policy may not match those acquired within the cluster via GroupMappingServiceProvider plugins.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Knox+Consumer+Restriction"></a>Knox Consumer Restriction [](#knox-apache-org-books-knox-2-1-0-user-guide--Knox+Consumer+Restriction)

Consumption of messages via Knox at this time is not supported. The Confluent Kafka REST Proxy that Knox relies upon is stateful when used for consumption of messages.

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Troubleshooting"></a>Troubleshooting [](#knox-apache-org-books-knox-2-1-0-user-guide--Troubleshooting)

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Finding+Logs"></a>Finding Logs [](#knox-apache-org-books-knox-2-1-0-user-guide--Finding+Logs)

When things aren’t working the first thing you need to do is examine the diagnostic logs. Depending upon how you are running the gateway these diagnostic logs will be output to different locations.

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--java+-jar+bin/gateway.jar"></a>java -jar bin/gateway.jar [](#knox-apache-org-books-knox-2-1-0-user-guide--java+-jar+bin/gateway.jar)

When the gateway is run this way the diagnostic output is written directly to the console. If you want to capture that output you will need to redirect the console output to a file using OS specific techniques.

```text
java -jar bin/gateway.jar > gateway.log
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--bin/gateway.sh+start"></a>bin/gateway.sh start [](#knox-apache-org-books-knox-2-1-0-user-guide--bin/gateway.sh+start)

When the gateway is run this way the diagnostic output is written to `{GATEWAY_HOME}/log/knox.out` and `{GATEWAY_HOME}/log/knox.err`. Typically only knox.out will have content.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Increasing+Logging"></a>Increasing Logging [](#knox-apache-org-books-knox-2-1-0-user-guide--Increasing+Logging)

The `log4j.properties` files `{GATEWAY_HOME}/conf` can be used to change the granularity of the logging done by Knox. The Knox server must be restarted in order for these changes to take effect. There are various useful loggers pre-populated but commented out.

```properties
log4j.logger.org.apache.knox.gateway=DEBUG # Use this logger to increase the debugging of Apache Knox itself.
log4j.logger.org.apache.shiro=DEBUG          # Use this logger to increase the debugging of Apache Shiro.
log4j.logger.org.apache.http=DEBUG           # Use this logger to increase the debugging of Apache HTTP components.
log4j.logger.org.apache.http.client=DEBUG    # Use this logger to increase the debugging of Apache HTTP client component.
log4j.logger.org.apache.http.headers=DEBUG   # Use this logger to increase the debugging of Apache HTTP header.
log4j.logger.org.apache.http.wire=DEBUG      # Use this logger to increase the debugging of Apache HTTP wire traffic.
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Server+Connectivity+Issues"></a>LDAP Server Connectivity Issues [](#knox-apache-org-books-knox-2-1-0-user-guide--LDAP+Server+Connectivity+Issues)

If the gateway cannot contact the configured LDAP server you will see errors in the gateway diagnostic output.

```sql
13/11/15 16:30:17 DEBUG authc.BasicHttpAuthenticationFilter: Attempting to execute login with headers [Basic Z3Vlc3Q6Z3Vlc3QtcGFzc3dvcmQ=]
13/11/15 16:30:17 DEBUG ldap.JndiLdapRealm: Authenticating user 'guest' through LDAP
13/11/15 16:30:17 DEBUG ldap.JndiLdapContextFactory: Initializing LDAP context using URL    [ldap://localhost:33389] and principal [uid=guest,ou=people,dc=hadoop,dc=apache,dc=org] with pooling disabled
13/11/15 16:30:17 DEBUG servlet.SimpleCookie: Added HttpServletResponse Cookie [rememberMe=deleteMe; Path=/gateway/vaultservice; Max-Age=0; Expires=Thu, 14-Nov-2013 21:30:17 GMT]
13/11/15 16:30:17 DEBUG authc.BasicHttpAuthenticationFilter: Authentication required: sending 401 Authentication challenge response.
```

The client should see something along the lines of:

```text
HTTP/1.1 401 Unauthorized
WWW-Authenticate: BASIC realm="application"
Content-Length: 0
Server: Jetty(8.1.12.v20130726)
```

Resolving this will require ensuring that the LDAP server is running and that connection information is correct. The LDAP server connection information is configured in the cluster’s topology file (e.g. {GATEWAY\_HOME}/deployments/sandbox.xml).

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+Cluster+Connectivity+Issues"></a>Hadoop Cluster Connectivity Issues [](#knox-apache-org-books-knox-2-1-0-user-guide--Hadoop+Cluster+Connectivity+Issues)

If the gateway cannot contact one of the services in the configured Hadoop cluster you will see errors in the gateway diagnostic output.

```text
13/11/18 18:49:45 WARN knox.gateway: Connection exception dispatching request: http://localhost:50070/webhdfs/v1/?user.name=guest&op=LISTSTATUS org.apache.http.conn.HttpHostConnectException: Connection to http://localhost:50070 refused
org.apache.http.conn.HttpHostConnectException: Connection to http://localhost:50070 refused
  at org.apache.http.impl.conn.DefaultClientConnectionOperator.openConnection(DefaultClientConnectionOperator.java:190)
  at org.apache.http.impl.conn.ManagedClientConnectionImpl.open(ManagedClientConnectionImpl.java:294)
  at org.apache.http.impl.client.DefaultRequestDirector.tryConnect(DefaultRequestDirector.java:645)
  at org.apache.http.impl.client.DefaultRequestDirector.execute(DefaultRequestDirector.java:480)
  at org.apache.http.impl.client.AbstractHttpClient.execute(AbstractHttpClient.java:906)
  at org.apache.http.impl.client.AbstractHttpClient.execute(AbstractHttpClient.java:805)
  at org.apache.http.impl.client.AbstractHttpClient.execute(AbstractHttpClient.java:784)
  at org.apache.knox.gateway.dispatch.HttpClientDispatch.executeRequest(HttpClientDispatch.java:99)
```

The resulting behavior on the client will differ by client. For the client DSL executing the `{GATEWAY_HOME}/samples/ExampleWebHdfsLs.groovy` the output will look like this.

```text
Caught: org.apache.knox.gateway.shell.HadoopException: org.apache.knox.gateway.shell.ErrorResponse: HTTP/1.1 500 Server Error
org.apache.knox.gateway.shell.HadoopException: org.apache.knox.gateway.shell.ErrorResponse: HTTP/1.1 500 Server Error
  at org.apache.knox.gateway.shell.AbstractRequest.now(AbstractRequest.java:72)
  at org.apache.knox.gateway.shell.AbstractRequest$now.call(Unknown Source)
  at ExampleWebHdfsLs.run(ExampleWebHdfsLs.groovy:28)
```

When executing commands requests via cURL the output might look similar to the following example.

```text
Set-Cookie: JSESSIONID=16xwhpuxjr8251ufg22f8pqo85;Path=/gateway/sandbox;Secure
Content-Type: text/html;charset=ISO-8859-1
Cache-Control: must-revalidate,no-cache,no-store
Content-Length: 21856
Server: Jetty(8.1.12.v20130726)

<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1"/>
<title>Error 500 Server Error</title>
</head>
<body><h2>HTTP ERROR 500</h2>
```

Resolving this will require ensuring that the Hadoop services are running and that connection information is correct. Basic Hadoop connectivity can be evaluated using cURL as described elsewhere. Otherwise the Hadoop cluster connection information is configured in the cluster’s topology file (e.g. `{GATEWAY_HOME}/deployments/sandbox.xml`).

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HTTP+vs+HTTPS+protocol+issues"></a>HTTP vs HTTPS protocol issues [](#knox-apache-org-books-knox-2-1-0-user-guide--HTTP+vs+HTTPS+protocol+issues)

When Knox is configured to accept requests over SSL and is presented with a request over plain HTTP, the client is presented with an error such as seen in the following:

```bash
curl -i -k -u guest:guest-password -X GET 'http://localhost:8443/gateway/sandbox/webhdfs/v1/?op=LISTSTATUS'
the following error is returned
curl: (52) Empty reply from server
```

This is the default behavior for Jetty SSL listener. While the credentials to the default authentication provider continue to be username and password, we do not want to encourage sending these in clear text. Since preemptively sending BASIC credentials is a common pattern with REST APIs it would be unwise to redirect to a HTTPS listener thus allowing clear text passwords.

To resolve this issue, we have two options:

1. change the scheme in the URL to https and deal with any trust relationship issues with the presented server certificate
2. Disabling SSL in gateway-site.xml - this is not encouraged due to the reasoning described above.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Check+Hadoop+Cluster+Access+via+cURL"></a>Check Hadoop Cluster Access via cURL [](#knox-apache-org-books-knox-2-1-0-user-guide--Check+Hadoop+Cluster+Access+via+cURL)

When you are experiencing connectivity issue it can be helpful to “bypass” the gateway and invoke the Hadoop REST APIs directly. This can easily be done using the cURL command line utility or many other REST/HTTP clients. Exactly how to use cURL depends on the configuration of your Hadoop cluster. In general however you will use a command line the one that follows.

```bash
curl -ikv -X GET 'http://namenode-host:50070/webhdfs/v1/?op=LISTSTATUS'
```

If you are using Sandbox the WebHDFS or NameNode port will be mapped to localhost so this command can be used.

```bash
curl -ikv -X GET 'http://localhost:50070/webhdfs/v1/?op=LISTSTATUS'
```

If you are using a cluster secured with Kerberos you will need to have used `kinit` to authenticate to the KDC. Then the command below should verify that WebHDFS in the Hadoop cluster is accessible.

```bash
curl -ikv --negotiate -u : -X 'http://localhost:50070/webhdfs/v1/?op=LISTSTATUS'
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Authentication+Issues"></a>Authentication Issues [](#knox-apache-org-books-knox-2-1-0-user-guide--Authentication+Issues)

The following log information is available when you enable debug level logging for shiro. This can be done within the conf/log4j.properties file. Not the “Password not correct for user” message.

```sql
13/11/15 16:37:15 DEBUG authc.BasicHttpAuthenticationFilter: Attempting to execute login with headers [Basic Z3Vlc3Q6Z3Vlc3QtcGFzc3dvcmQw]
13/11/15 16:37:15 DEBUG ldap.JndiLdapRealm: Authenticating user 'guest' through LDAP
13/11/15 16:37:15 DEBUG ldap.JndiLdapContextFactory: Initializing LDAP context using URL [ldap://localhost:33389] and principal [uid=guest,ou=people,dc=hadoop,dc=apache,dc=org] with pooling disabled
2013-11-15 16:37:15,899 INFO  Password not correct for user 'uid=guest,ou=people,dc=hadoop,dc=apache,dc=org'
2013-11-15 16:37:15,899 INFO  Authenticator org.apache.directory.server.core.authn.SimpleAuthenticator@354c78e3 failed to authenticate: BindContext for DN 'uid=guest,ou=people,dc=hadoop,dc=apache,dc=org', credentials <0x67 0x75 0x65 0x73 0x74 0x2D 0x70 0x61 0x73 0x73 0x77 0x6F 0x72 0x64 0x30 >
2013-11-15 16:37:15,899 INFO  Cannot bind to the server
13/11/15 16:37:15 DEBUG servlet.SimpleCookie: Added HttpServletResponse Cookie [rememberMe=deleteMe; Path=/gateway/vaultservice; Max-Age=0; Expires=Thu, 14-Nov-2013 21:37:15 GMT]
13/11/15 16:37:15 DEBUG authc.BasicHttpAuthenticationFilter: Authentication required: sending 401 Authentication challenge response.
```

The client will likely see something along the lines of:

```text
HTTP/1.1 401 Unauthorized
WWW-Authenticate: BASIC realm="application"
Content-Length: 0
Server: Jetty(8.1.12.v20130726)
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Using+ldapsearch+to+verify+LDAP+connectivity+and+credentials"></a>Using ldapsearch to verify LDAP connectivity and credentials [](#knox-apache-org-books-knox-2-1-0-user-guide--Using+ldapsearch+to+verify+LDAP+connectivity+and+credentials)

If your authentication to Knox fails and you believe you’re using correct credentials, you could try to verify the connectivity and credentials using ldapsearch, assuming you are using LDAP directory for authentication.

Assuming you are using the default values that came out of box with Knox, your ldapsearch command would be like the following

```text
ldapsearch -h localhost -p 33389 -D "uid=guest,ou=people,dc=hadoop,dc=apache,dc=org" -w guest-password -b "uid=guest,ou=people,dc=hadoop,dc=apache,dc=org" "objectclass=*"
```

This should produce output like the following

```bash
# extended LDIF

LDAPv3
base <uid=guest,ou=people,dc=hadoop,dc=apache,dc=org> with scope subtree
filter: objectclass=*
requesting: ALL

# guest, people, hadoop.apache.org
dn: uid=guest,ou=people,dc=hadoop,dc=apache,dc=org
objectClass: organizationalPerson
objectClass: person
objectClass: inetOrgPerson
objectClass: top
uid: guest
cn: Guest
sn: User
userpassword:: Z3Vlc3QtcGFzc3dvcmQ=

# search result
search: 2
result: 0 Success

# numResponses: 2
# numEntries: 1
```

In a more general form the ldapsearch command would be

```text
ldapsearch -h {HOST} -p {PORT} -D {DN of binding user} -w {bind password} -b {DN of binding user} "objectclass=*}
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Hostname+Resolution+Issues"></a>Hostname Resolution Issues [](#knox-apache-org-books-knox-2-1-0-user-guide--Hostname+Resolution+Issues)

The deployments/sandbox.xml topology file has the host mapping feature enabled. This is required due to the way networking is setup in the Sandbox VM. Specifically the VM’s internal hostname is sandbox.hortonworks.com. Since this hostname cannot be resolved to the actual VM Knox needs to map that hostname to something resolvable.

If for example host mapping is disabled but the Sandbox VM is still used you will see an error in the diagnostic output similar to the below.

```text
13/11/18 19:11:35 WARN knox.gateway: Connection exception dispatching request: http://sandbox.hortonworks.com:50075/webhdfs/v1/user/guest/example/README?op=CREATE&namenoderpcaddress=sandbox.hortonworks.com:8020&user.name=guest&overwrite=false java.net.UnknownHostException: sandbox.hortonworks.com
java.net.UnknownHostException: sandbox.hortonworks.com
  at java.net.Inet6AddressImpl.lookupAllHostAddr(Native Method)
```

On the other hand if you are migrating from the Sandbox based configuration to a cluster you have deployment you may see a similar error. However in this case you may need to disable host mapping. This can be done by modifying the topology file (e.g. deployments/sandbox.xml) for the cluster.

```text
...
<provider>
    <role>hostmap</role>
    <name>static</name>
    <enabled>false</enabled>
    <param><name>localhost</name><value>sandbox,sandbox.hortonworks.com</value></param>
</provider>
....
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Job+Submission+Issues+-+HDFS+Home+Directories"></a>Job Submission Issues - HDFS Home Directories [](#knox-apache-org-books-knox-2-1-0-user-guide--Job+Submission+Issues+-+HDFS+Home+Directories)

If you see error like the following in your console while submitting a Job using groovy shell, it is likely that the authenticated user does not have a home directory on HDFS.

```text
Caught: org.apache.knox.gateway.shell.HadoopException: org.apache.knox.gateway.shell.ErrorResponse: HTTP/1.1 403 Forbidden
org.apache.knox.gateway.shell.HadoopException: org.apache.knox.gateway.shell.ErrorResponse: HTTP/1.1 403 Forbidden
```

You would also see this error if you try file operation on the home directory of the authenticating user.

The error would look a little different as shown below if you are attempting to the operation with cURL.

```text
{"RemoteException":{"exception":"AccessControlException","javaClassName":"org.apache.hadoop.security.AccessControlException","message":"Permission denied: user=tom, access=WRITE, inode=\"/user\":hdfs:hdfs:drwxr-xr-x"}}* 
```

#### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Resolution"></a>Resolution [](#knox-apache-org-books-knox-2-1-0-user-guide--Resolution)

Create the home directory for the user on HDFS. The home directory is typically of the form `/user/{userid}` and should be owned by the user. user ‘hdfs’ can create such a directory and make the user owner of the directory.

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Job+Submission+Issues+-+OS+Accounts"></a>Job Submission Issues - OS Accounts [](#knox-apache-org-books-knox-2-1-0-user-guide--Job+Submission+Issues+-+OS+Accounts)

If the Hadoop cluster is not secured with Kerberos, the user submitting a job need not have an OS account on the Hadoop NodeManagers.

If the Hadoop cluster is secured with Kerberos, the user submitting the job should have an OS account on Hadoop NodeManagers.

In either case if the user does not have such OS account, his file permissions are based on user ownership of files or “other” permission in “ugo” posix permission. The user does not get any file permission as a member of any group if you are using default `hadoop.security.group.mapping`.

TODO: add sample error message from running test on secure cluster with missing OS account

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--HBase+Issues"></a>HBase Issues [](#knox-apache-org-books-knox-2-1-0-user-guide--HBase+Issues)

If you experience problems running the HBase samples with the Sandbox VM it may be necessary to restart HBase and the HBASE REST API. This can sometimes occur with the Sandbox VM is restarted from a saved state. If the client hangs after emitting the last line in the sample output below you are most likely affected.

```text
System version : {...}
Cluster version : 0.96.0.2.0.6.0-76-hadoop2
Status : {...}
Creating table 'test_table'...
```

HBase and the HBASE REST API can be restarted using the following commands on the Hadoop Sandbox VM. You will need to ssh into the VM in order to run these commands.

```text
sudo -u hbase /usr/lib/hbase/bin/hbase-daemon.sh stop master
sudo -u hbase /usr/lib/hbase/bin/hbase-daemon.sh start master
sudo -u hbase /usr/lib/hbase/bin/hbase-daemon.sh restart rest
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--SSL+Certificate+Issues"></a>SSL Certificate Issues [](#knox-apache-org-books-knox-2-1-0-user-guide--SSL+Certificate+Issues)

Clients that do not trust the certificate presented by the server will behave in different ways. A browser will typically warn you of the inability to trust the received certificate and give you an opportunity to add an exception for the particular certificate. Curl will present you with the follow message and instructions for turning of certificate verification:

```bash
curl performs SSL certificate verification by default, using a "bundle" 
 of Certificate Authority (CA) public keys (CA certs). If the default
 bundle file isn't adequate, you can specify an alternate file
 using the --cacert option.
If this HTTPS server uses a certificate signed by a CA represented 
 the bundle, the certificate verification probably failed due to a
 problem with the certificate (it might be expired, or the name might
 not match the domain name in the URL).
If you'd like to turn off curl's verification of the certificate, use
 the -k (or --insecure) option.
```

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--SPNego+Authentication+Issues"></a>SPNego Authentication Issues [](#knox-apache-org-books-knox-2-1-0-user-guide--SPNego+Authentication+Issues)

Calls from Knox to Secure Hadoop Cluster fails, with SPNego authentication problems, if there was a TGT for Knox in disk cache when Knox was started.

You are likely to run into this situation on developer machines where the developer could have kinited for some testing.

Work Around: clear TGT of Knox from disk cache (calling `kdestroy` would do it), before starting Knox

### <a id="knox-apache-org-books-knox-2-1-0-user-guide--Filing+Bugs"></a>Filing Bugs [](#knox-apache-org-books-knox-2-1-0-user-guide--Filing+Bugs)

Bugs can be filed using [Jira](https://issues.apache.org/jira/browse/KNOX). Please include the results of this command below in the Environment section. Also include the version of Hadoop being used in the same section.

```bash
cd {GATEWAY_HOME}
java -jar bin/gateway.jar -version
```

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Export+Controls"></a>Export Controls [](#knox-apache-org-books-knox-2-1-0-user-guide--Export+Controls)

Apache Knox Gateway includes cryptographic software. The country in which you currently reside may have restrictions on the import, possession, use, and/or re-export to another country, of encryption software. BEFORE using any encryption software, please check your country’s laws, regulations and policies concerning the import, possession, or use, and re-export of encryption software, to see if this is permitted. See [http://www.wassenaar.org](http://www.wassenaar.org) for more information.

The U.S. Government Department of Commerce, Bureau of Industry and Security (BIS), has classified this software as Export Commodity Control Number (ECCN) 5D002.C.1, which includes information security software using or performing cryptographic functions with asymmetric algorithms. The form and manner of this Apache Software Foundation distribution makes it eligible for export under the License Exception ENC Technology Software Unrestricted (TSU) exception (see the BIS Export Administration Regulations, Section 740.13) for both object code and source code.

The following provides more details on the included cryptographic software:

- Apache Knox Gateway uses the ApacheDS which in turn uses Bouncy Castle generic encryption libraries.
- See [http://www.bouncycastle.org](http://www.bouncycastle.org) for more details on Bouncy Castle.
- See [http://directory.apache.org/apacheds](http://directory.apache.org/apacheds) for more details on ApacheDS.

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Trademarks"></a>Trademarks [](#knox-apache-org-books-knox-2-1-0-user-guide--Trademarks)

Apache Knox, Apache Knox Gateway, Apache, the Apache feather logo and the Apache Knox Gateway project logos are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--License"></a>License [](#knox-apache-org-books-knox-2-1-0-user-guide--License)

Apache Knox uses the standard [Apache license](https://www.apache.org/licenses/LICENSE-2.0).

## <a id="knox-apache-org-books-knox-2-1-0-user-guide--Privacy+Policy"></a>Privacy Policy [](#knox-apache-org-books-knox-2-1-0-user-guide--Privacy+Policy)

Apache Knox uses the standard Apache privacy policy.

Information about your use of this website is collected using server access logs and a tracking cookie. The collected information consists of the following:

- The IP address from which you access the website;
- The type of browser and operating system you use to access our site;
- The date and time you access our site;
- The pages you visit; and
- The addresses of pages from where you followed a link to our site.

Part of this information is gathered using a tracking cookie set by the [Google Analytics](http://www.google.com/analytics/) service. Google’s policy for the use of this information is described in their [privacy policy](http://www.google.com/privacy.html). See your browser’s documentation for instructions on how to disable the cookie if you prefer not to share this data with Google.

We use the gathered information to help us make our site more useful to visitors and to better understand how and when our site is used. We do not track or collect personally identifiable information or associate gathered data with any personally identifying information from other sources.

By using this website, you consent to the collection of this data in the manner and for the purpose described above.

---

<a id="knox-apache-org-books-knox-2-1-0-dev-guide"></a>

# Dev Guide

![Knox](knox-logo.gif)
![Apache](apache-logo.gif)

# <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Apache+Knox+Gateway+2.1.x+Developer's+Guide"></a>Apache Knox Gateway 2.1.x Developer’s Guide [](#knox-apache-org-books-knox-2-1-0-dev-guide--Apache+Knox+Gateway+2.1.x+Developer's+Guide)

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Table+Of+Contents"></a>Table Of Contents [](#knox-apache-org-books-knox-2-1-0-dev-guide--Table+Of+Contents)

- [Overview](#knox-apache-org-books-knox-2-1-0-dev-guide--Overview)
  - [Architecture Overview](#knox-apache-org-books-knox-2-1-0-dev-guide--Architecture+Overview)
  - [Project Overview](#knox-apache-org-books-knox-2-1-0-dev-guide--Project+Overview)
  - [Development Processes](#knox-apache-org-books-knox-2-1-0-dev-guide--Development+Processes)
  - [Docker Image](#knox-apache-org-books-knox-2-1-0-dev-guide--Docker+Image)
- [Behavior](#knox-apache-org-books-knox-2-1-0-dev-guide--Behavior)
  - [Runtime Behavior](#knox-apache-org-books-knox-2-1-0-dev-guide--Runtime+Behavior)
  - [Deployment Behavior](#knox-apache-org-books-knox-2-1-0-dev-guide--Deployment+Behavior)
- [Extension Logistics](#knox-apache-org-books-knox-2-1-0-dev-guide--Extension+Logistics)
  - [Providers](#knox-apache-org-books-knox-2-1-0-dev-guide--Providers)
  - [Services](#knox-apache-org-books-knox-2-1-0-dev-guide--Services)
  - [Service Discovery](#knox-apache-org-books-knox-2-1-0-dev-guide--Service+Discovery)
- [Standard Providers](#knox-apache-org-books-knox-2-1-0-dev-guide--Standard+Providers)
  - [Rewrite Provider](#knox-apache-org-books-knox-2-1-0-dev-guide--Rewrite+Provider)
- [Gateway Services](#knox-apache-org-books-knox-2-1-0-dev-guide--Gateway+Services)
- [KnoxSSO Integration](#knox-apache-org-books-knox-2-1-0-dev-guide--KnoxSSO+Integration)
- [Health Monitoring API](#knox-apache-org-books-knox-2-1-0-dev-guide--Health+Monitoring+API)
- [Auditing](#knox-apache-org-books-knox-2-1-0-dev-guide--Auditing)
- [Logging](#knox-apache-org-books-knox-2-1-0-dev-guide--Logging)
- [Internationalization](#knox-apache-org-books-knox-2-1-0-dev-guide--Internationalization)
- [Admin UI](#knox-apache-org-books-knox-2-1-0-dev-guide--Admin+UI)

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Overview"></a>Overview [](#knox-apache-org-books-knox-2-1-0-dev-guide--Overview)

Apache Knox gateway is a specialized reverse proxy gateway for various Hadoop REST APIs. However, the gateway is built entirely upon a fairly generic framework. This framework is used to “plug-in” all of the behavior that makes it specific to Hadoop in general and any particular Hadoop REST API. It would be equally as possible to create a customized reverse proxy for other non-Hadoop HTTP endpoints. This approach is taken to ensure that the Apache Knox gateway can scale with the rapidly evolving Hadoop ecosystem.

Throughout this guide we will be using a publicly available REST API to demonstrate the development of various extension mechanisms. [http://openweathermap.org/](http://openweathermap.org/)

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Architecture+Overview"></a>Architecture Overview [](#knox-apache-org-books-knox-2-1-0-dev-guide--Architecture+Overview)

The gateway itself is a layer over an embedded Jetty JEE server. At the very highest level the gateway processes requests by using request URLs to lookup specific JEE Servlet Filter chain that is used to process the request. The gateway framework provides extensible mechanisms to assemble chains of custom filters that support secured access to services.

The gateway has two primary extensibility mechanisms: Service and Provider. The Service extensibility framework provides a way to add support for new HTTP/REST endpoints. For example, the support for WebHdfs is plugged into the Knox gateway as a Service. The Provider extensibility framework allows adding new features to the gateway that can be used across Services. An example of a Provider is an authentication provider. Providers can also expose APIs that other service and provider extensions can utilize.

Service and Provider integrations interact with the gateway framework in two distinct phases: Deployment and Runtime. The gateway framework can be thought of as a layer over the JEE Servlet framework. Specifically all runtime processing within the gateway is performed by JEE Servlet Filters. The two phases interact with this JEE Servlet Filter based model in very different ways. The first phase, Deployment, is responsible for converting fairly simple to understand configuration called topology into JEE WebArchive (WAR) based implementation details. The second phase, Runtime, is the processing of requests via a set of Filters configured in the WAR.

From an “ethos” perspective, Service and Provider extensions should attempt to incur complexity associated with configuration in the deployment phase. This should allow for very streamlined request processing that is very high performance and easily testable. The preference at runtime, in OO style, is for small classes that perform a specific function. The ideal set of implementation classes are then assembled by the Service and Provider plugins during deployment.

A second critical design consideration is streaming. The processing infrastructure is build around JEE Servlet Filters as they provide a natural streaming interception model. All Provider implementations should make every attempt to maintaining this streaming characteristic.

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Project+Overview"></a>Project Overview [](#knox-apache-org-books-knox-2-1-0-dev-guide--Project+Overview)

The table below describes the purpose of the current modules in the project. Of particular importance are the root pom.xml and the gateway-release module. The root pom.xml is critical because this is where all dependency version must be declared. There should be no dependency version information in module pom.xml files. The gateway-release module is critical because the dependencies declared there essentially define the classpath of the released gateway server. This is also true of the other -release modules in the project.

| File/Module | Description |
| --- | --- |
| LICENSE | The license for all source files in the release. |
| NOTICE | Attributions required by dependencies. |
| README | A brief overview of the Knox project. |
| CHANGES | A description of the changes for each release. |
| ISSUES | The knox issues for the current release. |
| gateway-util-common | Common low level utilities used by many modules. |
| gateway-util-launcher | The launcher framework. |
| gateway-util-urltemplate | The i18n logging and resource framework. |
| gateway-i18n | The URL template and rewrite utilities |
| gateway-i18n-logging-log4j | The integration of i18n logging with log4j. |
| gateway-i18n-logging-sl4j | The integration of i18n logging with sl4j. |
| gateway-spi | The SPI for service and provider extensions. |
| gateway-provider-identity-assertion-common | The identity assertion provider base |
| gateway-provider-identity-assertion-concat | An identity assertion provider that facilitates prefix and suffix concatenation. |
| gateway-provider-identity-assertion-pseudo | The default identity assertion provider. |
| gateway-provider-jersey | The jersey display provider. |
| gateway-provider-rewrite | The URL rewrite provider. |
| gateway-provider-rewrite-func-hostmap-static | Host mapping function extension to rewrite. |
| gateway-provider-rewrite-func-service-registry | Service registry function extension to rewrite. |
| gateway-provider-rewrite-step-secure-query | Crypto step extension to rewrite. |
| gateway-provider-security-authz-acls | Service level authorization. |
| gateway-provider-security-jwt | JSON Web Token utilities. |
| gateway-provider-security-preauth | Preauthenticated SSO header support. |
| gateway-provider-security-shiro | Shiro authentiation integration. |
| gateway-provider-security-webappsec | Filters to prevent common webapp security issues. |
| gateway-service-as | The implementation of the Access service POC. |
| gateway-service-definitions | The implementation of the Service definition and rewrite files. |
| gateway-service-hbase | The implementation of the HBase service. |
| gateway-service-hive | The implementation of the Hive service. |
| gateway-service-oozie | The implementation of the Oozie service. |
| gateway-service-tgs | The implementation of the Ticket Granting service POC. |
| gateway-service-webhdfs | The implementation of the WebHdfs service. |
| gateway-discovery-ambari | The Ambari service URL discovery implementation. |
| gateway-service-remoteconfig | The implementation of the RemoteConfigurationRegistryClientService. |
| gateway-server | The implementation of the Knox gateway server. |
| gateway-shell | The implementation of the Knox Groovy shell. |
| gateway-test-ldap | Pulls in all of the dependencies of the test LDAP server. |
| gateway-server-launcher | The launcher definition for the gateway. |
| gateway-shell-launcher | The launcher definition for the shell. |
| knox-cli-launcher | A module to pull in all of the dependencies of the CLI. |
| gateway-test-ldap-launcher | The launcher definition for the test LDAP server. |
| gateway-release | The definition of the gateway binary release. Contains content and dependencies to be included in binary gateway package. |
| gateway-test-utils | Various utilities used in unit and system tests. |
| gateway-test | The functional tests. |
| pom.xml | The top level pom. |
| build.xml | A collection of utility for building and releasing. |

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Development+Processes"></a>Development Processes [](#knox-apache-org-books-knox-2-1-0-dev-guide--Development+Processes)

The project uses Maven in general with a few convenience Ant targets.

Building the project can be built via Maven or Ant. The two commands below are equivalent.

```bash
mvn clean install
ant
```

A more complete build can be done that builds and generates the unsigned ZIP release artifacts. You will find these in the target/{version} directory (e.g. target/0.XX.0-SNAPSHOT).

```bash
mvn -Ppackage clean install
ant release
```

There are a few other Ant targets that are especially convenient for testing.

This command installs the gateway into the {{{install}}} directory of the project. Note that this command does not first build the project.

```text
ant install-test-home
```

This command starts the gateway and LDAP servers installed by the command above into a test GATEWAY\_HOME (i.e. install). Note that this command does not first install the test home.

```text
ant start-test-servers
```

So putting things together the following Ant command will build a release, install it and start the servers ready for manual testing.

```text
ant release install-test-home start-test-servers
```

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Docker+Image"></a>Docker Image [](#knox-apache-org-books-knox-2-1-0-dev-guide--Docker+Image)

Apache Knox ships with a `docker` Maven module that will build a Docker image. To build the Knox Docker image, you must have Docker running on your machine. The following Maven command will build Knox and package it into a Docker image.

```bash
mvn -Ppackage,release,docker clean package
```

This will build 2 Docker images:

- `apache/knox:gateway-2.1.0-SNAPSHOT`
- `apache/knox:ldap-2.1.0-SNAPSHOT`

The `gateway` image will use an entrypoint to start Knox Gateway. The `ldap` image will use an entrypoint to start Knox Demo LDAP.

An example of using the Docker images would be the following:

```text
docker run -d --name knox-ldap -p 33389:33389 apache/knox:ldap-2.1.0-SNAPSHOT
docker run -d --name knox-gateway -p 8443:8443 apache/knox:gateway-2.1.0-SNAPSHOT
```

Using docker-compose that would look like this:

```text
docker-compose -f gateway-docker/src/main/resources/docker-compose.yml up
```

The images are designed to be a base that can be built on to add your own providers, descriptors, and topologies as necessary.

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Behavior"></a>Behavior [](#knox-apache-org-books-knox-2-1-0-dev-guide--Behavior)

There are two distinct phases in the behavior of the gateway. These are the deployment and runtime phases. The deployment phase is responsible for converting topology descriptors into an executable JEE style WAR. The runtime phase is the processing of requests via WAR created during the deployment phase.

The deployment phase is arguably the more complex of the two phases. This is because runtime relies on well known JEE constructs while deployment introduces new framework concepts. The base concept of the deployment framework is that of a “contributor”. In the framework, contributors are pluggable component responsible for generating JEE WAR artifacts from topology files.

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Deployment+Behavior"></a>Deployment Behavior [](#knox-apache-org-books-knox-2-1-0-dev-guide--Deployment+Behavior)

The goal of the deployment phase is to take easy to understand topology descriptions and convert them into optimized runtime artifacts. Our goal is not only should the topology descriptors be easy to understand, but have them be easy for a management system (e.g. Ambari) to generate. Think of deployment as compiling an assembly descriptor into a JEE WAR. WARs are then deployed to an embedded JEE container (i.e. Jetty).

Consider the results of starting the gateway the first time. There are two sets of files that are relevant for deployment. The first is the topology file `<GATEWAY_HOME>/conf/topologies/sandbox.xml`. This second set is the WAR structure created during the deployment of the topology file.

```text
data/deployments/sandbox.war.143bfef07f0/WEB-INF
  web.xml
  gateway.xml
  shiro.ini
  rewrite.xml
  hostmap.txt
```

Notice that the directory `sandbox.war.143bfef07f0` is an “unzipped” representation of a JEE WAR file. This specifically means that it contains a `WEB-INF` directory which contains a `web.xml` file. For the curious the strange number (i.e. 143bfef07f0) in the name of the WAR directory is an encoded timestamp. This is the timestamp of the topology file (i.e. sandbox.xml) at the time the deployment occurred. This value is used to determine when topology files have changed and redeployment is required.

Here is a brief overview of the purpose of each file in the WAR structure.

- web.xml
  A standard JEE WAR descriptor. In this case a build-in GatewayServlet is mapped to the url pattern /\*.
- gateway.xml
  The configuration file for the GatewayServlet. Defines the filter chain that will be applied to each service’s various URLs.
- shiro.ini
  The configuration file for the Shiro authentication provider’s filters. This information is derived from the information in the provider section of the topology file.
- rewrite.xml
  The configuration file for the rewrite provider’s filter. This captures all of the rewrite rules for the services. These rules are contributed by the contributors for each service.
- hostmap.txt
  The configuration file the hostmap provider’s filter. This information is derived from the information in the provider section of the topology file.

The deployment framework follows “visitor” style patterns. Each topology file is parsed and the various constructs within it are “visited”. The appropriate contributor for each visited construct is selected by the framework. The contributor is then passed the contrust from the topology file and asked to update the JEE WAR artifacts. Each contributor is free to inspect and modify any portion of the WAR artifacts.

The diagram below provides an overview of the deployment processing. Detailed descriptions of each step follow the diagram.

![](deployment-overview.png)

1. The gateway server loads a topology file from conf/topologies into an internal structure.
2. The gateway server delegates to a deployment factory to create the JEE WAR structure.
3. The deployment factory first creates a basic WAR structure with WEB-INF/web.xml.
4. Each provider and service in the topology is visited and the appropriate deployment contributor invoked. Each contributor is passed the appropriate information from the topology and modifies the WAR structure.
5. A complete WAR structure is returned to the gateway service.
6. The gateway server uses internal container APIs to dynamically deploy the WAR.

The Java method below is the actual code from the DeploymentFactory that implements this behavior. You will note the initialize, contribute, finalize sequence. Each contributor is given three opportunities to interact with the topology and archive. This allows the various contributors to interact if required. For example, the service contributors use the deployment descriptor added to the WAR by the rewrite provider.

```sql
public static WebArchive createDeployment( GatewayConfig config, Topology topology ) {
  Map<String,List<ProviderDeploymentContributor>> providers;
  Map<String,List<ServiceDeploymentContributor>> services;
  DeploymentContext context;

  providers = selectContextProviders( topology );
  services = selectContextServices( topology );
  context = createDeploymentContext( config, topology.getName(), topology, providers, services );

  initialize( context, providers, services );
  contribute( context, providers, services );
  finalize( context, providers, services );

  return context.getWebArchive();
}
```

Below is a diagram that provides more detail. This diagram focuses on the interactions between the deployment factory and the service deployment contributors. Detailed description of each step follow the diagram.

![](deployment-service.png)

1. The gateway server loads global configuration (i.e. /conf/gateway-site.xml
2. The gateway server loads a topology descriptor file.
3. The gateway server delegates to the deployment factory to create a deployable WAR structure.
4. The deployment factory creates a runtime descriptor to configure that gateway servlet.
5. The deployment factory creates a basic WAR structure and adds the gateway servlet runtime descriptor to it.
6. The deployment factory creates a deployment context object and adds the WAR structure to it.
7. For each service defined in the topology descriptor file the appropriate service deployment contributor is selected and invoked. The correct service deployment contributor is determined by matching the role of a service in the topology descriptor to a value provided by the getRole() method of the ServiceDeploymentContributor interface. The initializeContribution method from *each* service identified in the topology is called. Each service deployment contributor is expected to setup any runtime artifacts in the WAR that other services or provides may need.
8. The contributeService method from *each* service identified in the topology is called. This is where the service deployment contributors will modify any runtime descriptors.
9. One of they ways that a service deployment contributor can modify the runtime descriptors is by asking the framework to contribute filters. This is how services are loosely coupled to the providers of features. For example a service deployment contributor might ask the framework to contribute the filters required for authorization. The deployment framework will then delegate to the correct provider deployment contributor to add filters for that feature.
10. Finally the finalizeContribution method for each service is invoked. This provides an opportunity to react to anything done via the contributeService invocations and tie up any loose ends.
11. The populated WAR is returned to the gateway server.

The following diagram will provided expanded detail on the behavior of provider deployment contributors. Much of the beginning and end of the sequence shown overlaps with the service deployment sequence above. Those steps (i.e. 1-6, 17) will not be described below for brevity. The remaining steps have detailed descriptions following the diagram.

![](deployment-provider.png)

1. For each provider the appropriate provider deployment contributor is selected and invoked. The correct service deployment contributor is determined by first matching the role of a provider in the topology descriptor to a value provided by the getRole() method of the ProviderDeploymentContributor interface. If this is ambiguous, the name from the topology is used match the value provided by the getName() method of the ProviderDeploymentContributor interface. The initializeContribution method from *each* provider identified in the topology is called. Each provider deployment contributor is expected to setup any runtime artifacts in the WAR that other services or provides may need. Note: In addition, others provider not explicitly referenced in the topology may have their initializeContribution method called. If this is the case only one default instance for each role declared vis the getRole() method will be used. The method used to determine the default instance is non-deterministic so it is best to select a particular named instance of a provider for each role.
2. Each provider deployment contributor will typically add any runtime deployment descriptors it requires for operation. These descriptors are added to the WAR structure within the deployment context.
3. The contributeProvider method of each configured or default provider deployment contributor is invoked.
4. Each provider deployment contributor populates any runtime deployment descriptors based on information in the topology.
5. Provider deployment contributors are never asked to contribute to the deployment directly. Instead a service deployment contributor will ask to have a particular provider role (e.g. authentication) contribute to the deployment.
6. A service deployment contributor asks the framework to contribute filters for a given provider role.
7. The framework selects the appropriate provider deployment contributor and invokes its contributeFilter method.
8. During this invocation the provider deployment contributor populate populate service specific information. In particular it will add filters to the gateway servlet’s runtime descriptor by adding JEE Servlet Filters. These filters will be added to the resources (or URLs) identified by the service deployment contributor.
9. The finalizeContribute method of all referenced and default provider deployment contributors is invoked.
10. The provider deployment contributor is expected to perform any final modifications to the runtime descriptors in the WAR structure.

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Runtime+Behavior"></a>Runtime Behavior [](#knox-apache-org-books-knox-2-1-0-dev-guide--Runtime+Behavior)

The runtime behavior of the gateway is somewhat simpler as it more or less follows well known JEE models. There is one significant wrinkle. The filter chains are managed within the GatewayServlet as opposed to being managed by the JEE container. This is the result of an early decision made in the project. The intention is to allow more powerful URL matching than is provided by the JEE Servlet mapping mechanisms.

The diagram below provides a high level overview of the runtime processing. An explanation for each step is provided after the diagram.

![](runtime-overview.png)

1. A REST client makes a HTTP request that is received by the embedded JEE container.
2. A filter chain is looked up in a map of URLs to filter chains.
3. The filter chain, which is itself a filter, is invoked.
4. Each filter invokes the filters that follow it in the chain. The request and response objects can be wrapped in typically JEE Filter fashion. Filters may not continue chain processing and return if that is appropriate.
5. Eventually the end of the last filter in the chain is invoked. Typically this is a special “dispatch” filter that is responsible for dispatching the request to the ultimate endpoint. Dispatch filters are also responsible for reading the response.
6. The response may be in the form of a number of content types (e.g. application/json, text/xml).
7. The response entity is streamed through the various response wrappers added by the filters. These response wrappers may rewrite various portions of the headers and body as per their configuration.
8. The return of the response entity to the client is ultimately “pulled through” the filter response wrapper by the container.
9. The response entity is returned original client.

This diagram providers a more detailed breakdown of the request processing. Again descriptions of each step follow the diagram.

![](runtime-request-processing.png)

1. A REST client makes a HTTP request that is received by the embedded JEE container.
2. The embedded container looks up the servlet mapped to the URL and invokes the service method. This our case the GatewayServlet is mapped to /\* and therefore receives all requests for a given topology. Keep in mind that the WAR itself is deployed on a root context path that typically contains a level for the gateway and the name of the topology. This means that there is a single GatewayServlet per topology and it is effectivly mapped to //\*.
3. The GatewayServlet holds a single reference to a GatewayFilter which is a specialized JEE Servlet Filter. This choice was made to allow the GatewayServlet to dynamically deploy modified topologies. This is done by building a new GatewayFilter instance and replacing the old in an atomic fashion.
4. The GatewayFilter contains another layer of URL mapping as defined in the gateway.xml runtime descriptor. The various service deployment contributor added these mappings at deployment time. Each service may add a number of different sub-URLs depending in their requirements. These sub-URLs will all be mapped to independently configured filter chains.
5. The GatewayFilter invokes the doFilter method on the selected chain.
6. The chain invokes the doFilter method of the first filter in the chain.
7. Each filter in the chain continues processing by invoking the doFilter on the next filter in the chain. Ultimately a dispatch filter forward the request to the real service instead of invoking another filter. This is sometimes referred to as pivoting.

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Gateway+Servlet+&+Gateway+Filter"></a>Gateway Servlet & Gateway Filter [](#knox-apache-org-books-knox-2-1-0-dev-guide--Gateway+Servlet+&+Gateway+Filter)

TODO

```xml
<web-app>

  <servlet>
    <servlet-name>sample</servlet-name>
    <servlet-class>org.apache.knox.gateway.GatewayServlet</servlet-class>
    <init-param>
      <param-name>gatewayDescriptorLocation</param-name>
      <param-value>gateway.xml</param-value>
    </init-param>
  </servlet>

  <servlet-mapping>
    <servlet-name>sandbox</servlet-name>
    <url-pattern>/*</url-pattern>
  </servlet-mapping>

  <listener>
    <listener-class>org.apache.knox.gateway.services.GatewayServicesContextListener</listener-class>
  </listener>

  ...

</web-app>
```

```xml
<gateway>

  <resource>
    <role>WEATHER</role>
    <pattern>/weather/**?**</pattern>

    <filter>
      <role>authentication</role>
      <name>sample</name>
      <class>...</class>
    </filter>

    <filter>...</filter>*

  </resource>

</gateway>
```

```java
@Test
public void testDevGuideSample() throws Exception {
  Template pattern, input;
  Matcher<String> matcher;
  Matcher<String>.Match match;

  // GET http://api.openweathermap.org/data/2.5/weather?q=Palo+Alto
  pattern = Parser.parse( "/weather/**?**" );
  input = Parser.parse( "/weather/2.5?q=Palo+Alto" );

  matcher = new Matcher<String>();
  matcher.add( pattern, "fake-chain" );
  match = matcher.match( input );

  assertThat( match.getValue(), is( "fake-chain") );
}
```

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Extension+Logistics"></a>Extension Logistics [](#knox-apache-org-books-knox-2-1-0-dev-guide--Extension+Logistics)

There are a number of extension points available in the gateway: services, providers, rewrite steps and functions, etc. All of these use the Java ServiceLoader mechanism for their discovery. There are two ways to make these extensions available on the class path at runtime. The first way to add a new module to the project and have the extension “built-in”. The second is to add the extension to the class path of the server after it is installed. Both mechanism are described in more detail below.

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Service+Loaders"></a>Service Loaders [](#knox-apache-org-books-knox-2-1-0-dev-guide--Service+Loaders)

Extensions are discovered via Java’s [Service Loader](http://docs.oracle.com/javase/6/docs/api/java/util/ServiceLoader.html) mechanism. There are good [tutorials](http://docs.oracle.com/javase/tutorial/ext/basics/spi.html) available for learning more about this. The basics come down to two things.

1. Implement the service contract interface (e.g. ServiceDeploymentContributor, ProviderDeploymentContributor)
2. Create a file in META-INF/services of the JAR that will contain the extension. This file will be named as the fully qualified name of the contract interface (e.g. org.apache.knox.gateway.deploy.ProviderDeploymentContributor). The contents of the file will be the fully qualified names of any implementation of that contract interface in that JAR.

One tip is to include a simple test with each of you extension to ensure that it will be properly discovered. This is very helpful in situations where a refactoring fails to change the a class in the META-INF/services files. An example of one such test from the project is shown below.

```java
  @Test
  public void testServiceLoader() throws Exception {
    ServiceLoader loader = ServiceLoader.load( ProviderDeploymentContributor.class );
    Iterator iterator = loader.iterator();
    assertThat( "Service iterator empty.", iterator.hasNext() );
    while( iterator.hasNext() ) {
      Object object = iterator.next();
      if( object instanceof ShiroDeploymentContributor ) {
        return;
      }
    }
    fail( "Failed to find " + ShiroDeploymentContributor.class.getName() + " via service loader." );
  }
```

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Class+Path"></a>Class Path [](#knox-apache-org-books-knox-2-1-0-dev-guide--Class+Path)

One way to extend the functionality of the server without having to recompile is to add the extension JARs to the servers class path. As an extensible server this is made straight forward but it requires some understanding of how the server’s classpath is setup. In the  directory there are four class path related directories (i.e. bin, lib, dep, ext).

The bin directory contains very small “launcher” jars that contain only enough code to read configuration and setup a class path. By default the configuration of a launcher is embedded with the launcher JAR but it may also be extracted into a .cfg file. In that file you will see how the class path is defined.

```text
class.path=../lib/*.jar,../dep/*.jar;../ext;../ext/*.jar
```

The paths are all relative to the directory that contains the launcher JAR.

- ../lib/\*.jar
  These are the “built-in” jars that are part of the project itself. Information is provided elsewhere in this document for how to integrate a built-in extension.
- ../dep/\*.jar
  These are the JARs for all of the external dependencies of the project. This separation between the generated JARs and dependencies help keep licensing issues straight.
- ../ext
  This directory is for post-install extensions and is empty by default. Including the directory (vs \*.jar) allows for individual classes to be placed in this directory.
- ../ext/\*.jar
  This would pick up all extension JARs placed in the ext directory.

Note that order is significant. The lib JARs take precedence over dep JARs and they take precedence over ext classes and JARs.

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Maven+Module"></a>Maven Module [](#knox-apache-org-books-knox-2-1-0-dev-guide--Maven+Module)

Integrating an extension into the project follows well established Maven patterns for adding modules. Below are several points that are somewhat unique to the Knox project.

1. Add the module to the root pom.xml file’s  list. Take care to ensure that the module is in the correct place in the list based on its dependencies. Note: In general modules should not have non-test dependencies on gateway-server but rather gateway-spi
2. Any new dependencies must be represented in the root pom.xml file’s  section. The required version of the dependencies will be declared there. The new sub-module’s pom.xml file must not include dependency version information. This helps prevent dependency version conflict issues.
3. If the extension is to be “built into” the released gateway server it needs to be added as a dependency to the gateway-release module. This is done by adding to the  section of the gateway-release’s pom.xml file. If this isn’t done the JARs for the module will not be automatically packaged into the release artifacts. This can be useful while an extension is under development but not yet ready for inclusion in the release.

More detailed examples of adding both a service and a provider extension are provided in subsequent sections.

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Services"></a>Services [](#knox-apache-org-books-knox-2-1-0-dev-guide--Services)

Services are extensions that are responsible for converting information in the topology file to runtime descriptors. Typically services do not require their own runtime descriptors. Rather, they modify either the gateway runtime descriptor (i.e. gateway.xml) or descriptors of other providers (e.g. rewrite.xml).

The service provider interface for a Service is ServiceDeploymentContributor and is shown below.

```java
package org.apache.knox.gateway.deploy;
import org.apache.knox.gateway.topology.Service;
public interface ServiceDeploymentContributor {
  String getRole();
  void initializeContribution( DeploymentContext context );
  void contributeService( DeploymentContext context, Service service ) throws Exception;
  void finalizeContribution( DeploymentContext context );
}
```

Each service provides an implementation of this interface that is discovered via the ServerLoader mechanism previously described. The meaning of this is best understood in the context of the structure of the topology file. A fragment of a topology file is shown below.

```xml
<topology>
    <gateway>
        ....
    </gateway>
    <service>
        <role>WEATHER</role>
        <url>http://api.openweathermap.org/data</url>
    </service>
    ....
</topology>
```

With these two things a more detailed description of the purpose of each ServiceDeploymentContributor method should be helpful.

- String getRole();
  This is the value the framework uses to associate a given `<service><role>` with a particular ServiceDeploymentContributor implementation. See below how the example WeatherDeploymentContributor implementation returns the role WEATHER that matches the value in the topology file. This will result in the WeatherDeploymentContributor’s methods being invoked when a WEATHER service is encountered in the topology file.

```java
public class WeatherDeploymentContributor extends ServiceDeploymentContributorBase {
  private static final String ROLE = "WEATHER";
  @Override
  public String getRole() {
    return ROLE;
  }
  ...
}
```

- void initializeContribution( DeploymentContext context );
  In this method a contributor would create, initialize and add any descriptors it was responsible for to the deployment context. For the weather service example this isn’t required so the empty method isn’t shown here.
- void contributeService( DeploymentContext context, Service service ) throws Exception;
  In this method a service contributor typically add and configures any features it requires. This method will be dissected in more detail below.
- void finalizeContribution( DeploymentContext context );
  In this method a contributor would finalize any descriptors it was responsible for to the deployment context. For the weather service example this isn’t required so the empty method isn’t shown here.

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Service+Contribution+Behavior"></a>Service Contribution Behavior [](#knox-apache-org-books-knox-2-1-0-dev-guide--Service+Contribution+Behavior)

In order to understand the job of the ServiceDeploymentContributor a few runtime descriptors need to be introduced.

- Gateway Runtime Descriptor: WEB-INF/gateway.xml
  This runtime descriptor controls the behavior of the GatewayFilter. It defines a mapping between resources (i.e. URL patterns) and filter chains. The sample gateway runtime descriptor helps illustrate.

```xml
<gateway>
  <resource>
    <role>WEATHER</role>
    <pattern>/weather/**?**</pattern>
    <filter>
      <role>authentication</role>
      <name>sample</name>
      <class>...</class>
    </filter>
    <filter>...</filter>*
    ...
  </resource>
</gateway>
```

- Rewrite Provider Runtime Descriptor: WEB-INF/rewrite.xml
  The rewrite provider runtime descriptor controls the behavior of the rewrite filter. Each service contributor is responsible for adding the rules required to control the URL rewriting required by that service. Later sections will provide more detail about the capabilities of the rewrite provider.

```xml
<rules>
  <rule dir="IN" name="WEATHER/openweathermap/inbound/versioned/file"
      pattern="*://*:*/**/weather/{version}?{**}">
    <rewrite template="{$serviceUrl[WEATHER]}/{version}/weather?{**}"/>
  </rule>
</rules>
```

With these two descriptors in mind a detailed breakdown of the WeatherDeploymentContributor’s contributeService method will make more sense. At a high level the important concept is that contributeService is invoked by the framework for each  in the topology file.

```java
public class WeatherDeploymentContributor extends ServiceDeploymentContributorBase {
  ...
  @Override
  public void contributeService( DeploymentContext context, Service service ) throws Exception {
    contributeResources( context, service );
    contributeRewriteRules( context );
  }

  private void contributeResources( DeploymentContext context, Service service ) throws URISyntaxException {
    ResourceDescriptor resource = context.getGatewayDescriptor().addResource();
    resource.role( service.getRole() );
    resource.pattern( "/weather/**?**" );
    addAuthenticationFilter( context, service, resource );
    addRewriteFilter( context, service, resource );
    addDispatchFilter( context, service, resource );
  }

  private void contributeRewriteRules( DeploymentContext context ) throws IOException {
    UrlRewriteRulesDescriptor allRules = context.getDescriptor( "rewrite" );
    UrlRewriteRulesDescriptor newRules = loadRulesFromClassPath();
    allRules.addRules( newRules );
  }

  ...
}
```

The DeploymentContext parameter contains information about the deployment as well as the WAR structure being created via deployment. The Service parameter is the object representation of the  element in the topology file. Details about particularly important lines follow the code block.

- ResourceDescriptor resource = context.getGatewayDescriptor().addResource();
  Obtains a reference to the gateway runtime descriptor and adds a new resource element. Note that many of the APIs in the deployment framework follow a fluent vs bean style.
- resource.role( service.getRole() );
  Sets the role for a particular resource. Many of the filters may need access to this role information in order to make runtime decisions.
- resource.pattern( “/weather/\*\*?\*\*” );
  Sets the URL pattern to which the filter chain that will follow will be mapped within the GatewayFilter.
- add\*Filter( context, service, resource );
  These are taken from a base class. A representation of the implementation of that method from the base class is shown below. Notice how this essentially delegates back to the framework to add the filters required by a particular provider role (e.g. “rewrite”).

```java
  protected void addRewriteFilter( DeploymentContext context, Service service, ResourceDescriptor resource ) {
    context.contributeFilter( service, resource, "rewrite", null, null );
  }
```

- UrlRewriteRulesDescriptor allRules = context.getDescriptor( “rewrite” );
  Here the rewrite provider runtime descriptor is obtained by name from the deployment context. This does represent a tight coupling in this case between this service and the default rewrite provider. The rewrite provider however is unlikely to be related with alternate implementations.
- UrlRewriteRulesDescriptor newRules = loadRulesFromClassPath();
  This is convenience method for loading partial rewrite descriptor information from the classpath. Developing and maintaining these rewrite rules is far easier as an external resource. The rewrite descriptor API could however have been used to achieve the same result.
- allRules.addRules( newRules );
  Here the rewrite rules for the weather service are merged into the larger set of rewrite rules.

```xml
<project>
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.apache.knox</groupId>
        <artifactId>gateway</artifactId>
        <version>2.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>gateway-service-weather</artifactId>
    <name>gateway-service-weather</name>
    <description>A sample extension to the gateway for a weather REST API.</description>

    <licenses>
        <license>
            <name>The Apache Software License, Version 2.0</name>
            <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
            <distribution>repo</distribution>
        </license>
    </licenses>

    <dependencies>
        <dependency>
            <groupId>${gateway-group}</groupId>
            <artifactId>gateway-spi</artifactId>
        </dependency>
        <dependency>
            <groupId>${gateway-group}</groupId>
            <artifactId>gateway-provider-rewrite</artifactId>
        </dependency>

        ... Test Dependencies ...

    </dependencies>

</project>
```

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Service+Definition+Files"></a>Service Definition Files [](#knox-apache-org-books-knox-2-1-0-dev-guide--Service+Definition+Files)

As of release 0.6.0, the gateway now also supports a declarative way of plugging-in a new Service. A Service can be defined with a combination of two files, these are:

```text
service.xml
rewrite.xml
```

The rewrite.xml file contains the rewrite rules as defined in other sections of this guide, and the service.xml file contains the various routes (paths) to be provided by the Service and the rewrite rule bindings to those paths. This will be described in further detail in this section.

While the service.xml file is absolutely required, the rewrite.xml file in theory is optional (though it is highly unlikely that no rewrite rules are needed).

To add a new service, simply add a service.xml and rewrite.xml file in an appropriate directory (see [Service Definition Directory Structure](#knox-apache-org-books-knox-2-1-0-dev-guide--Service+Definition+Directory+Structure)) in the module gateway-service-definitions to make the new service part of the Knox build.

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--service.xml"></a>service.xml [](#knox-apache-org-books-knox-2-1-0-dev-guide--service.xml)

Below is a sample of a very simple service.xml file, taking the same weather api example.

```xml
<service role="WEATHER" name="weather" version="0.1.0">
    <routes>
        <route path="/weather/**?**"/>
    </routes>
</service>
```

- **service**The root tag is ‘service’ that has the three required attributes: *role*, *name* and *version*. These three values disambiguate this service definition from others. To ensure the exact same service definition is being used in a topology file, all values should be specified,

```xml
<topology>
    <gateway>
        ....
    </gateway>
    <service>
        <role>WEATHER</role>
        <name>weather</name>
        <version>0.1.0</version>
        <url>http://api.openweathermap.org/data</url>
        <dispatch>
            <contributor-name>custom-client</contributor-name>
            <ha-contributor-name>ha-client</ha-contributor-name>
            <classname>org.apache.knox.gateway.dispatch.PassAllHeadersDispatch</classname>
            <ha-classname></ha-classname>
            <http-client-factory></http-client-factory>
            <use-two-way-ssl>false</use-two-way-ssl>
        </dispatch>
    </service>
    ....
</topology>
```

If only *role* is specified in the topology file (the only required element other than *url*) then the first service definition of that role found will be used with the highest version of that role and name. Similarly if only the *version* is omitted from the topology specification of the service, the service definition of the highest version will be used. It is therefore important to specify a version for a service if it is desired that a topology be locked down to a specific version of a service.

The optional `dispatch` element can be used to override the dispatch specified in service.xml file for the service, this can be useful in cases where you want a specific topology to override the dispatch, which is useful for topology based federation from one Knox instance to another. `dispatch\classname` is the most commonly used option, but other options are available if one wants more fine grained control over topology dispatch override.

- **routes**Wrapper element for one or more routes.
- **route**A route specifies the *path* that the service is routing as well as any rewrite bindings or policy bindings. Another child element that may be used here is a *dispatch* element.
- **rewrite**A rewrite rule or function that is to be applied to the path. A rewrite element contains a *apply* attribute that references the rewrite function or rule by name. Along with the *apply* attribute, a *to* attribute must be used. The *to* specifies what part of the request or response to rewrite. The valid values for the *to* attribute are:

- request.url
- request.headers
- request.cookies
- request.body
- response.headers
- response.cookies
- response.body

Below is an example of a snippet from the WebHDFS service definition

```xml
    <route path="/webhdfs/v1/**?**">
        <rewrite apply="WEBHDFS/webhdfs/inbound/namenode/file" to="request.url"/>
        <rewrite apply="WEBHDFS/webhdfs/outbound/namenode/headers" to="response.headers"/>
    </route>
```

- **dispatch**The dispatch element can be used to plug-in a custom dispatch class. The interface for Dispatch can be found in the module gateway-spi, org.apache.knox.gateway.dispatch.Dispatch.

This element can be used at the service level (i.e. as a child of the service tag) or at the route level. A dispatch specified at the route level takes precedence over a dispatch specified at the service level. By default the dispatch used is org.apache.knox.gateway.dispatch.DefaultDispatch.

The dispatch tag has four attributes that can be specified.

*contributor-name* : This attribute can be used to specify a deployment contributor to be invoked for a custom dispatch.

*classname* : This attribute can be used to specify a custom dispatch class.

*ha-contributor-name* : This attribute can be used to specify a deployment contributor to be invoked for custom HA dispatch functionality.

*ha-classname* : This attribute can be used to specify a custom dispatch class with HA functionality.

Only one of contributor-name or classname should be specified and one of ha-contributor-name or ha-classname should be specified.

If providing a custom dispatch, either a jar should be provided, see [Class Path](#knox-apache-org-books-knox-2-1-0-dev-guide--Class+Path) or a [Maven Module](#knox-apache-org-books-knox-2-1-0-dev-guide--Maven+Module) should be created.

Check out [ConfigurableDispatch](#knox-apache-org-books-knox-2-1-0-dev-guide--ConfigurableDispatch) about configurable dispatch type.

- **policies**This is a wrapper tag for *policy* elements and can be a child of the *service* tag or the *route* tag. Once again, just like with dispatch, the route level policies defined override the ones at the service level.

This element can contain one or more *policy* elements. The order of the *policy* elements is important as that will be the order of execution.

- **policy**At this time the policy element just has two attributes, *role* and *name*. These are used to execute a deployment contributor by that role and name. Therefore new policies must be added by using the deployment contributor mechanism.

For example,

```xml
<service role="FOO" name="foo" version="1.6.0">
    <policies>
        <policy role="webappsec"/>
        <policy role="authentication"/>
        <policy role="rewrite"/>
        <policy role="identity-assertion"/>
        <policy role="authorization"/>
    </policies>
    <routes>
        <route path="/foo/?**">
            <rewrite apply="FOO/foo/inbound" to="request.url"/>
            <policies>
                <policy role="webappsec"/>
                <policy role="federation"/>
                <policy role="identity-assertion"/>
                <policy role="authorization"/>
                <policy role="rewrite"/>
            </policies>
            <dispatch contributor-name="http-client" />
        </route>
    </routes>
    <dispatch contributor-name="custom-client" ha-contributor-name="ha-client"/>
</service>
```

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--ConfigurableDispatch"></a>ConfigurableDispatch [](#knox-apache-org-books-knox-2-1-0-dev-guide--ConfigurableDispatch)

The `ConfigurableDispatch` allows service definition writers to:

- exclude certain header(s) from the outbound HTTP request
- exclude certain header(s) from the outbound HTTP response
- declares whether parameters should be URL-encoded or not

This dispatch type can be set in `service.xml` as follows:

```xml
<dispatch classname="org.apache.knox.gateway.dispatch.ConfigurableDispatch" ha-classname="org.apache.knox.gateway.ha.dispatch.ConfigurableHADispatch">
  <param>
    <name>requestExcludeHeaders</name>
    <value>Authorization,Content-Length</value>
  </param>
  <param>
    <name>responseExcludeHeaders</name>
    <value>SET-COOKIE,WWW-AUTHENTICATE</value>
  </param>
  <param>
    <name>removeUrlEncoding</name>
    <value>true</value>
  </param>
</dispatch>
```

The default values of these parameters are:

- `requestExcludeHeaders` = `Host,Authorization,Content-Length,Transfer-Encoding`
- `responseExcludeHeaders` = `SET-COOKIE,WWW-AUTHENTICATE`
- `removeUrlEncoding` = `false`

The `responseExcludeHeaders` handling allows excluding only certain directives of the `SET-COOKIE` HTTP header. The following sample shows how to ecxlude only the `HttpOnly` directive from `SET-COOKIE` and the `WWW-AUTHENTICATE` header entirely in the routbound response HTTP header:

```xml
<dispatch classname="org.apache.knox.gateway.dispatch.ConfigurableDispatch" ha-classname="org.apache.knox.gateway.ha.dispatch.ConfigurableHADispatch">>
  <param>
    <name>responseExcludeHeaders</name>
    <value>WWW-AUTHENTICATE,SET-COOKIE:HttpOnly</value>
  </param>
</dispatch>
```

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--rewrite.xml"></a>rewrite.xml [](#knox-apache-org-books-knox-2-1-0-dev-guide--rewrite.xml)

The rewrite.xml file that accompanies the service.xml file follows the same rules as described in the section [Rewrite Provider](#knox-apache-org-books-knox-2-1-0-dev-guide--Rewrite+Provider).

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Service+Definition+Directory+Structure"></a>Service Definition Directory Structure [](#knox-apache-org-books-knox-2-1-0-dev-guide--Service+Definition+Directory+Structure)

On installation of the Knox gateway, the following directory structure can be found under ${GATEWAY\_HOME}/data. This is a mirror of the directories and files under the module gateway-service-definitions.

```text
services
    |______ service name
                    |______ version
                                |______service.xml
                                |______rewrite.xml
```

For example,

```text
services
    |______ webhdfs
               |______ 2.4.0
                         |______service.xml
                         |______rewrite.xml
```

To test out a new service, you can just add the appropriate files (service.xml and rewrite.xml) in a directory under ${GATEWAY\_HOME}/data/services. If you want to make the service contribution to the Knox build, they files need to go in the gateway-service-definitions module.

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Service+Definition+Runtime+Behavior"></a>Service Definition Runtime Behavior [](#knox-apache-org-books-knox-2-1-0-dev-guide--Service+Definition+Runtime+Behavior)

The runtime artifacts as well as the behavior does not change whether the service is plugged in via the deployment descriptors or through a service.xml file.

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Custom+Dispatch+Dependency+Injection"></a>Custom Dispatch Dependency Injection [](#knox-apache-org-books-knox-2-1-0-dev-guide--Custom+Dispatch+Dependency+Injection)

When writing a custom dispatch class, one often needs configuration or gateway services. A lightweight dependency injection system is used that can inject instances of classes or primitives available in the filter configuration’s init params or as a servlet context attribute.

Details of this can be found in the module gateway-util-configinjector and also an example use of it is in the class org.apache.knox.gateway.dispatch.DefaultDispatch. Look at the following method for example:

```java
 @Configure
   protected void setReplayBufferSize(@Default("8") int size) {
      replayBufferSize = size;
   }
```

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Service+Discovery"></a>Service Discovery [](#knox-apache-org-books-knox-2-1-0-dev-guide--Service+Discovery)

Knox supports the ability to dynamically determine endpoint URLs in topologies for supported Hadoop services. *This functionality is currently only supported for Ambari-managed Hadoop clusters.* There are a number of these services, which are officially supported, but this set can be extended by modifying the source or speciyfing external configuration.

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Service+URL+Definitions"></a>Service URL Definitions [](#knox-apache-org-books-knox-2-1-0-dev-guide--Service+URL+Definitions)

The service discovery system determines service URLs by processing mappings of Hadoop service configuration properties and corresponding URL templates. The knowledge about converting these arbitrary service configuration properties into correct service endpoint URLs is defined in a configuration file internal to the Ambari service discovery module.

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Configuration+Details"></a>Configuration Details [](#knox-apache-org-books-knox-2-1-0-dev-guide--Configuration+Details)

This internal configuration file ( **ambari-service-discovery-url-mappings.xml** ) in the **gateway-discovery-ambari** module is used to specify a URL template and the associated configuration properties to populate it. A limited degree of conditional logic is supported to accommodate things like *http* vs *https* configurations.

The simplest way to describe its contents will be by examples.

**Example 1**

The simplest example of one such mapping involves a service component for which there is a single configuration property which specifies the complete URL

```text
<!-- This is the service mapping declaration. The name must match what is specified as a Knox topology service role -->
<service name="OOZIE">

  <!-- This is the URL pattern with palceholder(s) for values provided by properties -->
  <url-pattern>{OOZIE_URL}</url-pattern>

  <properties>

    <!-- This is a property, which in this simple case, matches a template placeholder -->
    <property name="OOZIE_URL">

      <!-- This is the component whose configuration will be used to lookup the value of the subsequent config property name -->
      <component>OOZIE_SERVER</component>

      <!-- This is the name of the component config property whose value should be assigned to the OOZIE_URL value -->
      <config-property>oozie.base.url</config-property>

    </property>
  </properties>
</service>
```

The *OOZIE\_SERVER* component configuration is **oozie-site** If **oozie-site.xml** has the property named **oozie.base.url** with the value [http://ooziehost:11000](http://ooziehost:11000), then the resulting URL for the *OOZIE* service will be [http://ooziehost:11000](http://ooziehost:11000)

**Example 2**

A slightly more complicated example involves a service component for which the complete URL is not described by a single detail, but rather multiple endpoint URL details

```xml
<service name="WEBHCAT">
  <url-pattern>http://{HOST}:{PORT}/templeton</url-pattern>
  <properties>
    <property name="HOST">
      <component>WEBHCAT_SERVER</component> 

      <!-- This tells discovery to get the hostname for the WEBHCAT_SERVER component from Ambari -->
      <hostname/>

    </property>
    <property name="PORT">
      <component>WEBHCAT_SERVER</component>

      <!-- This is the name of the component config property whose value should be assigned to -->
      <!-- the PORT value -->
      <config-property>templeton.port</config-property>

    </property>
  </properties>
</service>
```

**Example 3**

An even more complicated example involves a service for which *HTTPS* is supported, and which employs the limited conditional logic support

```xml
<service name="ATLAS">
  <url-pattern>{SCHEME}://{HOST}:{PORT}</url-pattern>
  <properties>

    <!-- Property for getting the ATLAS_SERVER component hostname from Ambari -->
    <property name="HOST">
      <component>ATLAS_SERVER</component>
      <hostname/>
    </property>

    <!-- Property for capturing whether TLS is enabled or not; This is not a template placeholder property -->
    <property name="TLS_ENABLED">
      <component>ATLAS_SERVER</component>
      <config-property>atlas.enableTLS</config-property>
    </property>
    <!-- Property for getting the http port ; also NOT a template placeholder property -->
    <property name="HTTP_PORT">
      <component>ATLAS_SERVER</component>
      <config-property>atlas.server.http.port</config-property>
    </property>

    <!-- Property for getting the https port ; also NOT a template placeholder property -->
    <property name="HTTPS_PORT">
      <component>ATLAS_SERVER</component>
      <config-property>atlas.server.https.port</config-property>
    </property>

    <!-- Template placeholder property, dependent on the TLS_ENABLED property value -->
    <property name="PORT">
      <config-property>
        <if property="TLS_ENABLED" value="true">
          <then>HTTPS_PORT</then>
          <else>HTTP_PORT</else>
        </if>
      </config-property>
    </property>

    <!-- Template placeholder property, dependent on the TLS_ENABLED property value -->
    <property name="SCHEME">
      <config-property>
        <if property="TLS_ENABLED" value="true">
          <then>https</then>
          <else>http</else>
        </if>
      </config-property>
    </property>
  </properties>
</service>
```

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--External+Configuration"></a>External Configuration [](#knox-apache-org-books-knox-2-1-0-dev-guide--External+Configuration)

The internal configuration for URL construction can be overridden or augmented by way of a configuration file in the gateway configuration directory, or an alternative file specified by a Java system property. This mechanism is useful for developing support for new services, for custom solutions, or any scenario for which rebuilding Knox is not desirable.

The default file, for which Knox will search first, is ***{GATEWAY\_HOME}*/conf/ambari-discovery-url-mappings.xml**

If Knox doesn’t find that file, it will check for a Java system property named **org.apache.gateway.topology.discovery.ambari.config**, whose value is the fully-qualified path to an XML file. This file’s contents must adhere to the format outlined above.

If this configuration exists, Knox will apply it as if it were part of the internal configuration.

**Example**

If Apache Solr weren’t supported, then it could be added by creating the following definition in ***{GATEWAY\_HOME}*/conf/ambari-discovery-url-mappings.xml** :

```xml
<?xml version="1.0" encoding="utf-8"?>
<service-discovery-url-mappings>
  <service name="SOLR">
    <url-pattern>http://{HOST}:{PORT}</url-pattern>
    <properties>
      <property name="HOST">
        <component>INFRA_SOLR</component>
        <hostname/>
      </property>
      <property name="PORT">
        <component>INFRA_SOLR</component>
        <config-property>infra_solr_port</config-property>
      </property>
    </properties>
  </service>
</service-discovery-url-mappings>
```

***N.B. Knox must be restarted for changes to this external configuration to be applied.***

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Component+Configuration+Mapping"></a>Component Configuration Mapping [](#knox-apache-org-books-knox-2-1-0-dev-guide--Component+Configuration+Mapping)

To support URL construction from service configuration files, Ambari service discovery requires knowledge of the service component types and their respective relationships to configuration types. This knowledge is defined in a configuration file internal to the Ambari service discovery module.

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Configuration+Details"></a>Configuration Details [](#knox-apache-org-books-knox-2-1-0-dev-guide--Configuration+Details)

This internal configuration file ( **ambari-service-discovery-component-config-mapping.properties** ) in the **gateway-discovery-ambari** module is used to define the mapping of Hadoop service component names to the configuration type from which Knox will lookup property values.

**Example**

```properties
NAMENODE=hdfs-site
RESOURCEMANAGER=yarn-site
HISTORYSERVER=mapred-site
OOZIE_SERVER=oozie-site
HIVE_SERVER=hive-site
WEBHCAT_SERVER=webhcat-site
```

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--External+Configuration"></a>External Configuration [](#knox-apache-org-books-knox-2-1-0-dev-guide--External+Configuration)

The internal configuration for component configuration mappings can be overridden or augmented by way of a configuration file in the gateway configuration directory, or an alternative file specified by a Java system property. This mechanism is useful for developing support for new services, for custom solutions, or any scenario for which rebuilding Knox is not desirable.

The default file, for which Knox will search first, is ***{GATEWAY\_HOME}*/conf/ambari-discovery-component-config.properties** If Knox doesn’t find that file, it will check for a Java system property named **org.apache.knox.gateway.topology.discovery.ambari.component.mapping**, whose value is the fully-qualified path to a properties file. This file’s contents must adhere to the format outlined above.

If this configuration exists, Knox will apply it as if it were part of the internal configuration.

**Example**

Following the aforementioned SOLR example, Knox needs to know in which configuration file to find the *INFRA\_SOLR* component configuration property, so the following property must be defined in ***{GATEWAY\_HOME}*/conf/ambari-discovery-component-config.properties** :

```properties
INFRA_SOLR=infra-solr-env
```

This tells Knox to look for the **infra\_solr\_port** property in the **infra-solr-env** configuration.

***N.B. Knox must be restarted for changes to this external configuration to be applied.***

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Validator"></a>Validator [](#knox-apache-org-books-knox-2-1-0-dev-guide--Validator)

Apache Knox provides preauth federation authentication where  
Knox supports two built-in validators for verifying incoming requests. In this section, we describe how to write a custom validator for this scenario. The provided validators include:

- *preauth.default.validation:* This default behavior does not perform any validation check. All requests will pass.
- *preauth.ip.validation* : This validation checks if a request is originated from an IP address which is configured in Knox service through property *preauth.ip.addresses*.

However, these built-in validation choices may not fulfill the internal requirments of some organization. Therefore, Knox supports (since 0.12) a pluggble framework where anyone can include a custom validator.

In essence, a user can add a custom validator by following these steps. The corresponding code examples are incorporated after that:

1. Create a separate Java package (e.g. com.company.knox.validator) in a new or existing Maven project.
2. Create a new class (e.g. *CustomValidator*) that implements *org.apache.knox.gateway.preauth.filter.PreAuthValidator*.
3. The class should implement the method *String getName()* that may returns a string constant. The step-9 will need this user defined string constant.
4. The class should implement the method *boolean validate(HttpServletRequest httpRequest, FilterConfig filterConfig)*. This is the key method which will validate the request based on ‘httpRequest’ and ‘filterConfig’. In most common cases, user may need to use HTTP headers value to validate. For example, client can get a token from an authentication service and pass it as HTTP header. This validate method needs to extract that header and verify the token. In some instance, the server may need to contact the same authentication service to validate.
5. Create a text file src/resources/META-INF/services and add fully qualified name of your custom validator class (e.g. *com.company.knox.validator.CustomValidator*).
6. You may need to include the packages “org.apache.knox.gateway-provider-security-preauth” of version 0.12+ and “javax.servlet.javax.servlet-api” of version 3.1.0+ in pom.xml.
7. Build your custom jar.
8. Deploy the jar in $GATEWAY\_HOME/ext directory.
9. Add/modify a parameter called *preauth.validation.method* with the name of validator used in step #3. Optionally, you may add any new parameter that may be required only for your CustomValidator.

**Validator Class (Step 2-4)**

```java
package com.company.knox.validator;

import org.apache.knox.gateway.preauth.filter.PreAuthValidationException;
import org.apache.knox.gateway.preauth.filter.PreAuthValidator;
import com.google.common.base.Strings;

import javax.servlet.FilterConfig;
import javax.servlet.http.HttpServletRequest;

public class CustomValidator extends PreAuthValidator {
  //Any string constant value should work for these 3 variables
  //This string will be used in 'services' file.
  public static final String CUSTOM_VALIDATOR_NAME = "fooValidator"; 
  //Optional: User may want to pass soemthign through HTTP header. (per client request)
  public static final String CUSTOM_TOKEN_HEADER_NAME = "foo_claim";

  /**
   * @param httpRequest
   * @param filterConfig
   * @return
   * @throws PreAuthValidationException
   */
  @Override
  public boolean validate(HttpServletRequest httpRequest, FilterConfig filterConfig) throws PreAuthValidationException {
    String claimToken = httpRequest.getHeader(CUSTOM_TOKEN_HEADER_NAME);
    if (!Strings.isNullOrEmpty(claimToken)) {
      return checkCustomeToken(claimToken); //to be implemented
    } else {
      log.warn("Claim token was empty for header name '" + CUSTOM_TOKEN_HEADER_NAME + "'");
      return false;
    }
  }

  /**
   * Define unique validator name
   *
   * @return
   */
  @Override
  public String getName() {
    return CUSTOM_VALIDATOR_NAME;
  }
}
```

**META-INF/services contents (Step-5)**

`com.company.knox.validator.CustomValidator`

**POM file (Step-6)**

```xml
<dependency>
    <groupId>javax.servlet</groupId>
    <artifactId>javax.servlet-api</artifactId>
    <scope>provided</scope>
</dependency>

<dependency>
    <groupId>org.apache.knox</groupId>
    <artifactId>gateway-test-utils</artifactId>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.apache.knox</groupId>
    <artifactId>gateway-provider-security-preauth</artifactId>
    <scope>provided</scope>
</dependency>
```

**Deploy Custom Jar (Step-7-8)**

Build the jar (e.g. customValidation.jar) using ‘mvn clean package’ `cp customValidation.jar $GATEWAY_HOME/ext/`

**Topology Config (Step-9)**

```xml
<provider>
    <role>federation</role>
    <name>HeaderPreAuth</name>
    <enabled>true</enabled>
    <param><name>preauth.validation.method</name>
    <!--Same as CustomeValidator.CUSTOM_VALIDATOR_NAME   ->
    <value>fooValidator</value></param>
</provider>
```

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Providers"></a>Providers [](#knox-apache-org-books-knox-2-1-0-dev-guide--Providers)

```java
public interface ProviderDeploymentContributor {
  String getRole();
  String getName();

  void initializeContribution( DeploymentContext context );
  void contributeProvider( DeploymentContext context, Provider provider );
  void contributeFilter(
      DeploymentContext context,
      Provider provider,
      Service service,
      ResourceDescriptor resource,
      List<FilterParamDescriptor> params );

  void finalizeContribution( DeploymentContext context );
}
```

```xml
<project>
    <modelVersion>4.0.0</modelVersion>
    <parent>
        <groupId>org.apache.knox</groupId>
        <artifactId>gateway</artifactId>
        <version>2.1.0-SNAPSHOT</version>
    </parent>

    <artifactId>gateway-provider-security-authn-sample</artifactId>
    <name>gateway-provider-security-authn-sample</name>
    <description>A simple sample authorization provider.</description>

    <licenses>
        <license>
            <name>The Apache Software License, Version 2.0</name>
            <url>https://www.apache.org/licenses/LICENSE-2.0.txt</url>
            <distribution>repo</distribution>
        </license>
    </licenses>

    <dependencies>
        <dependency>
            <groupId>${gateway-group}</groupId>
            <artifactId>gateway-spi</artifactId>
        </dependency>
    </dependencies>

</project>
```

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Deployment+Context"></a>Deployment Context [](#knox-apache-org-books-knox-2-1-0-dev-guide--Deployment+Context)

```java
package org.apache.knox.gateway.deploy;

import ...

public interface DeploymentContext {

  GatewayConfig getGatewayConfig();

  Topology getTopology();

  WebArchive getWebArchive();

  WebAppDescriptor getWebAppDescriptor();

  GatewayDescriptor getGatewayDescriptor();

  void contributeFilter(
      Service service,
      ResourceDescriptor resource,
      String role,
      String name,
      List<FilterParamDescriptor> params );

  void addDescriptor( String name, Object descriptor );

  <T> T getDescriptor( String name );

}
```

```java
public class Topology {

  public URI getUri() {...}
  public void setUri( URI uri ) {...}

  public String getName() {...}
  public void setName( String name ) {...}

  public long getTimestamp() {...}
  public void setTimestamp( long timestamp ) {...}

  public Collection<Service> getServices() {...}
  public Service getService( String role, String name ) {...}
  public void addService( Service service ) {...}

  public Collection<Provider> getProviders() {...}
  public Provider getProvider( String role, String name ) {...}
  public void addProvider( Provider provider ) {...}
}
```

```java
public interface GatewayDescriptor {
  List<GatewayParamDescriptor> params();
  GatewayParamDescriptor addParam();
  GatewayParamDescriptor createParam();
  void addParam( GatewayParamDescriptor param );
  void addParams( List<GatewayParamDescriptor> params );

  List<ResourceDescriptor> resources();
  ResourceDescriptor addResource();
  ResourceDescriptor createResource();
  void addResource( ResourceDescriptor resource );
}
```

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Gateway+Services"></a>Gateway Services [](#knox-apache-org-books-knox-2-1-0-dev-guide--Gateway+Services)

TODO - Describe the service registry and other global services.

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Standard+Providers"></a>Standard Providers [](#knox-apache-org-books-knox-2-1-0-dev-guide--Standard+Providers)

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Rewrite+Provider"></a>Rewrite Provider [](#knox-apache-org-books-knox-2-1-0-dev-guide--Rewrite+Provider)

gateway-provider-rewrite org.apache.knox.gateway.filter.rewrite.api.UrlRewriteRulesDescriptor

```xml
<rules>
  <rule
      dir="IN"
      name="WEATHER/openweathermap/inbound/versioned/file"
      pattern="*://*:*/**/weather/{version}?{**}">
    <rewrite template="{$serviceUrl[WEATHER]}/{version}/weather?{**}"/>
  </rule>
</rules>
```

```xml
<rules>
  <filter name="WEBHBASE/webhbase/status/outbound">
    <content type="*/json">
      <apply path="$[LiveNodes][*][name]" rule="WEBHBASE/webhbase/address/outbound"/>
    </content>
    <content type="*/xml">
      <apply path="/ClusterStatus/LiveNodes/Node/@name" rule="WEBHBASE/webhbase/address/outbound"/>
    </content>
  </filter>
</rules>
```

```java
@Test
public void testDevGuideSample() throws Exception {
  URI inputUri, outputUri;
  Matcher<Void> matcher;
  Matcher<Void>.Match match;
  Template input, pattern, template;

  inputUri = new URI( "http://sample-host:8443/gateway/topology/weather/2.5?q=Palo+Alto" );

  input = Parser.parse( inputUri.toString() );
  pattern = Parser.parse( "*://*:*/**/weather/{version}?{**}" );
  template = Parser.parse( "http://api.openweathermap.org/data/{version}/weather?{**}" );

  matcher = new Matcher<Void>();
  matcher.add( pattern, null );
  match = matcher.match( input );

  outputUri = Expander.expand( template, match.getParams(), null );

  assertThat(
      outputUri.toString(),
      is( "http://api.openweathermap.org/data/2.5/weather?q=Palo+Alto" ) );
}
```

```java
@Test
public void testDevGuideSampleWithEvaluator() throws Exception {
  URI inputUri, outputUri;
  Matcher<Void> matcher;
  Matcher<Void>.Match match;
  Template input, pattern, template;
  Evaluator evaluator;

  inputUri = new URI( "http://sample-host:8443/gateway/topology/weather/2.5?q=Palo+Alto" );
  input = Parser.parse( inputUri.toString() );

  pattern = Parser.parse( "*://*:*/**/weather/{version}?{**}" );
  template = Parser.parse( "{$serviceUrl[WEATHER]}/{version}/weather?{**}" );

  matcher = new Matcher<Void>();
  matcher.add( pattern, null );
  match = matcher.match( input );

  evaluator = new Evaluator() {
    @Override
    public List<String> evaluate( String function, List<String> parameters ) {
      return Arrays.asList( "http://api.openweathermap.org/data" );
    }
  };

  outputUri = Expander.expand( template, match.getParams(), evaluator );

  assertThat(
      outputUri.toString(),
      is( "http://api.openweathermap.org/data/2.5/weather?q=Palo+Alto" ) );
}
```

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Rewrite+Filters"></a>Rewrite Filters [](#knox-apache-org-books-knox-2-1-0-dev-guide--Rewrite+Filters)

TODO - Cover the supported content types. TODO - Provide a XML and JSON “properties” example where one NVP is modified based on value of another name.

```xml
<rules>
  <filter name="WEBHBASE/webhbase/regions/outbound">
    <content type="*/json">
      <apply path="$[Region][*][location]" rule="WEBHBASE/webhbase/address/outbound"/>
    </content>
    <content type="*/xml">
      <apply path="/TableInfo/Region/@location" rule="WEBHBASE/webhbase/address/outbound"/>
    </content>
  </filter>
</rules>
```

```xml
<gateway>
  ...
  <resource>
    <role>WEBHBASE</role>
    <pattern>/hbase/*/regions?**</pattern>
    ...
    <filter>
      <role>rewrite</role>
      <name>url-rewrite</name>
      <class>org.apache.knox.gateway.filter.rewrite.api.UrlRewriteServletFilter</class>
      <param>
        <name>response.body</name>
        <value>WEBHBASE/webhbase/regions/outbound</value>
      </param>
    </filter>
    ...
  </resource>
  ...
</gateway>
```

HBaseDeploymentContributor

```text
    params = new ArrayList<FilterParamDescriptor>();
    params.add( regionResource.createFilterParam().name( "response.body" ).value( "WEBHBASE/webhbase/regions/outbound" ) );
    addRewriteFilter( context, service, regionResource, params );
```

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Rewrite+Functions"></a>Rewrite Functions [](#knox-apache-org-books-knox-2-1-0-dev-guide--Rewrite+Functions)

TODO - Provide an lowercase function as an example.

```xml
<rules>
  <functions>
    <hostmap config="/WEB-INF/hostmap.txt"/>
  </functions>
  ...
</rules>
```

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Rewrite+Steps"></a>Rewrite Steps [](#knox-apache-org-books-knox-2-1-0-dev-guide--Rewrite+Steps)

TODO - Provide an lowercase step as an example.

```xml
<rules>
  <rule dir="OUT" name="WEBHDFS/webhdfs/outbound/namenode/headers/location">
    <match pattern="{scheme}://{host}:{port}/{path=**}?{**}"/>
    <rewrite template="{gateway.url}/webhdfs/data/v1/{path=**}?{scheme}?host={$hostmap(host)}?{port}?{**}"/>
    <encrypt-query/>
  </rule>
</rules>
```

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Identity+Assertion+Provider"></a>Identity Assertion Provider [](#knox-apache-org-books-knox-2-1-0-dev-guide--Identity+Assertion+Provider)

Adding a new identity assertion provider is as simple as extending the AbstractIdentityAsserterDeploymentContributor and the CommonIdentityAssertionFilter from the gateway-provider-identity-assertion-common module to initialize any specific configuration from filter init params and implement two methods:

1. String mapUserPrincipal(String principalName);
2. String[] mapGroupPrincipals(String principalName, Subject subject);

To implement a simple toUpper or toLower identity assertion provider:

```java
package org.apache.knox.gateway.identityasserter.caseshifter.filter;

import org.apache.knox.gateway.identityasserter.common.filter.AbstractIdentityAsserterDeploymentContributor;

public class CaseShifterIdentityAsserterDeploymentContributor extends AbstractIdentityAsserterDeploymentContributor {

  @Override
  public String getName() {
    return "CaseShifter";
  }

  protected String getFilterClassname() {
    return CaseShifterIdentityAssertionFilter.class.getName();
  }
}
```

We merely need to provide the provider name for use in the topology and the filter classname for the contributor to add to the filter chain.

For the identity assertion filter itself it is just a matter of extension and the implementation of the two methods described earlier:

```java
package org.apache.knox.gateway.identityasserter.caseshifter.filter;

import javax.security.auth.Subject;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import org.apache.knox.gateway.identityasserter.common.filter.CommonIdentityAssertionFilter;

public class CaseShifterIdentityAssertionFilter extends CommonIdentityAssertionFilter {
  private boolean toUpper = false;
  
  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    String upper = filterConfig.getInitParameter("caseshift.upper");
    if ("true".equals(upper)) {
      toUpper = true;
    }
  }

  @Override
  public String[] mapGroupPrincipals(String mappedPrincipalName, Subject subject) {
    return null;
  }

  @Override
  public String mapUserPrincipal(String principalName) {
    if (toUpper) {
      principalName = principalName.toUpperCase();
    }
    else {
      principalName = principalName.toLowerCase();
    }
    return principalName;
  }
}
```

Note that the above:

1. looks for specific filter init parameters for configuration of whether to convert to upper or to lower case
2. it no-ops the mapGroupPrincipals so that it returns null. This indicates that there are no changes needed to the groups contained within the Subject. If there are groups then they should be continued to flow through the system unchanged. This is actually the same implementation as the base class and is therefore not required to be overridden. We include it here for illustration.
3. based upon the configuration interrogated in the init method the principalName is convert to either upper or lower case.

That is the extent of what is needed to implement a new identity assertion provider module.

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Jersey+Provider"></a>Jersey Provider [](#knox-apache-org-books-knox-2-1-0-dev-guide--Jersey+Provider)

TODO

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--KnoxSSO+Integration"></a>KnoxSSO Integration [](#knox-apache-org-books-knox-2-1-0-dev-guide--KnoxSSO+Integration)

# Knox SSO Integration for UIs

## Introduction

KnoxSSO provides an abstraction for integrating any number of authentication systems and SSO solutions and enables participating web applications to scale to those solutions more easily. Without the token exchange capabilities offered by KnoxSSO each component UI would need to integrate with each desired solution on its own.

This document examines the way to integrate with Knox SSO in the form of a Servlet Filter. This approach should be easily extrapolated into other frameworks - ie. Spring Security.

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--General+Flow"></a>General Flow [](#knox-apache-org-books-knox-2-1-0-dev-guide--General+Flow)

The following is a generic sequence diagram for SAML integration through KnoxSSO.

![](general_saml_flow.png)

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--KnoxSSO+Setup"></a>KnoxSSO Setup [](#knox-apache-org-books-knox-2-1-0-dev-guide--KnoxSSO+Setup)

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--knoxsso.xml+Topology"></a>knoxsso.xml Topology [](#knox-apache-org-books-knox-2-1-0-dev-guide--knoxsso.xml+Topology)

In order to enable KnoxSSO, we need to configure the IdP topology. The following is an example of this topology that is configured to use HTTP Basic Auth against the Knox Demo LDAP server. This is the lowest barrier of entry for your development environment that actually authenticates against a real user store. Whatâ€™s great is if you work against the IdP with Basic Auth then you will work with SAML or anything else as well.

```xml
		<?xml version="1.0" encoding="utf-8"?>
		<topology>
    		<gateway>
        		<provider>
            		<role>authentication</role>
            		<name>ShiroProvider</name>
            		<enabled>true</enabled>
            		<param>
	                	<name>sessionTimeout</name>
                		<value>30</value>
            		</param>
            		<param>
                		<name>main.ldapRealm</name>
                		<value>org.apache.knox.gateway.shirorealm.KnoxLdapRealm</value>
            		</param>
            		<param>
                		<name>main.ldapContextFactory</name>
                		<value>org.apache.knox.gateway.shirorealm.KnoxLdapContextFactory</value>
            		</param>
            		<param>
                		<name>main.ldapRealm.contextFactory</name>
                		<value>$ldapContextFactory</value>
            		</param>
            		<param>
                		<name>main.ldapRealm.userDnTemplate</name>
                		<value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
            		</param>
            		<param>
                		<name>main.ldapRealm.contextFactory.url</name>
                		<value>ldap://localhost:33389</value>
            		</param>
            		<param>
                		<name>main.ldapRealm.contextFactory.authenticationMechanism</name>
                		<value>simple</value>
            		</param>
            		<param>
                		<name>urls./**</name>
                		<value>authcBasic</value>
            		</param>
        		</provider>
            <provider>
        		    <role>identity-assertion</role>
            		<name>Default</name>
            		<enabled>true</enabled>
        		</provider>
    		</gateway>
        <service>
        		<role>KNOXSSO</role>
        		<param>
          			<name>knoxsso.cookie.secure.only</name>
          			<value>true</value>
        		</param>
        		<param>
          			<name>knoxsso.token.ttl</name>
          			<value>100000</value>
        		</param>
    		</service>
		</topology>
```

Just as with any Knox service, the KNOXSSO service is protected by the gateway providers defined above it. In this case, the ShiroProvider is taking care of HTTP Basic Auth against LDAP for us. Once the user authenticates the request processing continues to the KNOXSSO service that will create the required cookie and do the necessary redirects.

The authenticate/federation provider can be swapped out to fit your deployment environment.

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--sandbox.xml+Topology"></a>sandbox.xml Topology [](#knox-apache-org-books-knox-2-1-0-dev-guide--sandbox.xml+Topology)

In order to see the end to end story and use it as an example in your development, you can configure one of the cluster topologies to use the SSOCookieProvider instead of the out of the box ShiroProvider. The following is an example sandbox.xml topology that is configured for using KnoxSSO to protect access to the Hadoop REST APIs.

```xml
	<?xml version="1.0" encoding="utf-8"?>
<topology>
  <gateway>
    <provider>
        <role>federation</role>
        <name>SSOCookieProvider</name>
        <enabled>true</enabled>
        <param>
            <name>sso.authentication.provider.url</name>
            <value>https://localhost:9443/gateway/idp/api/v1/websso</value>
        </param>
    </provider>
    <provider>
        <role>identity-assertion</role>
        <name>Default</name>
        <enabled>true</enabled>
    </provider>
  </gateway>    
  <service>
      <role>NAMENODE</role>
      <url>hdfs://localhost:8020</url>
  </service>
  <service>
      <role>JOBTRACKER</role>
      <url>rpc://localhost:8050</url>
  </service>
  <service>
      <role>WEBHDFS</role>
      <url>http://localhost:50070/webhdfs</url>
  </service>
  <service>
      <role>WEBHCAT</role>
      <url>http://localhost:50111/templeton</url>
  </service>
  <service>
      <role>OOZIE</role>
      <url>http://localhost:11000/oozie</url>
  </service>
  <service>
      <role>WEBHBASE</role>
      <url>http://localhost:60080</url>
  </service>
  <service>
      <role>HIVE</role>
      <url>http://localhost:10001/cliservice</url>
  </service>
  <service>
      <role>RESOURCEMANAGER</role>
      <url>http://localhost:8088/ws</url>
  </service>
</topology>
```

- NOTE: Be aware that when using Chrome as your browser that cookies donâ€™t seem to work for â€œlocalhostâ€. Either use a VM or like I did - use 127.0.0.1. Safari works with localhost without problems.

As you can see above, the only thing being configured is the SSO provider URL. Since Knox is the issuer of the cookie and token, we donâ€™t need to configure the public key since we have programmatic access to the actual keystore for use at verification time.

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Curl+the+Flow"></a>Curl the Flow [](#knox-apache-org-books-knox-2-1-0-dev-guide--Curl+the+Flow)

We should now be able to walk through the SSO Flow at the command line with curl to see everything that happens.

First, issue a request to WEBHDFS through knox.

```bash
	bash-3.2$ curl -iku guest:guest-password https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp?op+LISTSTATUS
	
	HTTP/1.1 302 Found
	Location: https://localhost:8443/gateway/idp/api/v1/websso?originalUrl=https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp?op+LISTSTATUS
	Content-Length: 0
	Server: Jetty(8.1.14.v20131031)
```

Note the redirect to the knoxsso endpoint and the loginUrl with the originalUrl request parameter. We need to see that come from your integration as well.

Letâ€™s manually follow that redirect with curl now:

```bash
	bash-3.2$ curl -iku guest:guest-password "https://localhost:8443/gateway/idp/api/v1/websso?originalUrl=https://localhost:9443/gateway/sandbox/webhdfs/v1/tmp?op=LISTSTATUS"

	HTTP/1.1 307 Temporary Redirect
	Set-Cookie: JSESSIONID=mlkda4crv7z01jd0q0668nsxp;Path=/gateway/idp;Secure;HttpOnly
	Set-Cookie: hadoop-jwt=eyJhbGciOiJSUzI1NiJ9.eyJleHAiOjE0NDM1ODUzNzEsInN1YiI6Imd1ZXN0IiwiYXVkIjoiSFNTTyIsImlzcyI6IkhTU08ifQ.RpA84Qdr6RxEZjg21PyVCk0G1kogvkuJI2bo302bpwbvmc-i01gCwKNeoGYzUW27MBXf6a40vylHVR3aZuuBUxsJW3aa_ltrx0R5ztKKnTWeJedOqvFKSrVlBzJJ90PzmDKCqJxA7JUhyo800_lDHLTcDWOiY-ueWYV2RMlCO0w;Path=/;Domain=localhost;Secure;HttpOnly
	Expires: Thu, 01 Jan 1970 00:00:00 GMT
	Location: https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp?op=LISTSTATUS
	Content-Length: 0
	Server: Jetty(8.1.14.v20131031)
```

Note the redirect back to the original URL in the Location header and the Set-Cookie for the hadoop-jwt cookie. This is what the SSOCookieProvider in sandbox (and ultimately in your integration) will be looking for.

Finally, we should be able to take the above cookie and pass it to the original url as indicated in the Location header for our originally requested resource:

```bash
	bash-3.2$ curl -ikH "Cookie: hadoop-jwt=eyJhbGciOiJSUzI1NiJ9.eyJleHAiOjE0NDM1ODY2OTIsInN1YiI6Imd1ZXN0IiwiYXVkIjoiSFNTTyIsImlzcyI6IkhTU08ifQ.Os5HEfVBYiOIVNLRIvpYyjeLgAIMbBGXHBWMVRAEdiYcNlJRcbJJ5aSUl1aciNs1zd_SHijfB9gOdwnlvQ_0BCeGHlJBzHGyxeypIoGj9aOwEf36h-HVgqzGlBLYUk40gWAQk3aRehpIrHZT2hHm8Pu8W-zJCAwUd8HR3y6LF3M;Path=/;Domain=localhost;Secure;HttpOnly" https://localhost:9443/gateway/sandbox/webhdfs/v1/tmp?op=LISTSTATUS

	TODO: cluster was down and needs to be recreated :/
```

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Browse+the+Flow"></a>Browse the Flow [](#knox-apache-org-books-knox-2-1-0-dev-guide--Browse+the+Flow)

At this point, we can use a web browser instead of the command line and see how the browser will challenge the user for Basic Auth Credentials and then manage the cookies such that the SSO and token exchange aspects of the flow are hidden from the user.

Simply, try to invoke the same webhdfs API from the browser URL bar.

```text
		https://localhost:8443/gateway/sandbox/webhdfs/v1/tmp?op=LISTSTATUS
```

Based on our understanding of the flow it should behave like:

- SSOCookieProvider checks for hadoop-jwt cookie and in its absence redirects to the configured SSO provider URL (knoxsso endpoint)
- ShiroProvider on the KnoxSSO endpoint returns a 401 and the browser challenges the user for username/password
- The ShiroProvider authenticates the user against the Demo LDAP Server using a simple LDAP bind and establishes the security context for the WebSSO request
- The WebSSO service exchanges the normalized Java Subject into a JWT token and sets it on the response as a cookie named hadoop-jwt
- The WebSSO service then redirects the user agent back to the originally requested URL - the webhdfs Knox service subsequent invocations will find the cookie in the incoming request and not need to engage the WebSSO service again until it expires.

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Filter+by+Example"></a>Filter by Example [](#knox-apache-org-books-knox-2-1-0-dev-guide--Filter+by+Example)

We have added a federation provider to Knox for accepting KnoxSSO cookies for REST APIs. This provides us with a couple benefits: KnoxSSO support for REST APIs for XmlHttpRequests from JavaScript (basic CORS functionality is also included). This is still rather basic and considered beta code. A model and real world usecase for others to base their integrations on

In addition, [https://issues.apache.org/jira/browse/HADOOP-11717](https://issues.apache.org/jira/browse/HADOOP-11717) added support for the Hadoop UIs to the hadoop-auth module and it can be used as another example.

We will examine the new SSOCookieFederationFilter in Knox here.

```java
package org.apache.knox.gateway.provider.federation.jwt.filter;

	import java.io.IOException;
		import java.security.Principal;
		import java.security.PrivilegedActionException;
		import java.security.PrivilegedExceptionAction;
		import java.util.ArrayList;
		import java.util.Date;
		import java.util.HashSet;
		import java.util.List;
		import java.util.Set;
		
		import javax.security.auth.Subject;
		import javax.servlet.Filter;
		import javax.servlet.FilterChain;
		import javax.servlet.FilterConfig;
		import javax.servlet.ServletException;
		import javax.servlet.ServletRequest;
		import javax.servlet.ServletResponse;
		import javax.servlet.http.Cookie;
		import javax.servlet.http.HttpServletRequest;
		import javax.servlet.http.HttpServletResponse;
		
		import org.apache.knox.gateway.i18n.messages.MessagesFactory;
		import org.apache.knox.gateway.provider.federation.jwt.JWTMessages;
		import org.apache.knox.gateway.security.PrimaryPrincipal;
		import org.apache.knox.gateway.services.GatewayServices;
		import org.apache.knox.gateway.services.security.token.JWTokenAuthority;
		import org.apache.knox.gateway.services.security.token.TokenServiceException;
		import org.apache.knox.gateway.services.security.token.impl.JWTToken;
		
		public class SSOCookieFederationFilter implements Filter {
		  private static JWTMessages log = MessagesFactory.get( JWTMessages.class );
		  private static final String ORIGINAL_URL_QUERY_PARAM = "originalUrl=";
		  private static final String SSO_COOKIE_NAME = "sso.cookie.name";
		  private static final String SSO_EXPECTED_AUDIENCES = "sso.expected.audiences";
		  private static final String SSO_AUTHENTICATION_PROVIDER_URL = "sso.authentication.provider.url";
		  private static final String DEFAULT_SSO_COOKIE_NAME = "hadoop-jwt";
```

The above represent the configurable aspects of the integration

```java
    private JWTokenAuthority authority = null;
    private String cookieName = null;
    private List<String> audiences = null;
    private String authenticationProviderUrl = null;

    @Override
    public void init( FilterConfig filterConfig ) throws ServletException {
      GatewayServices services = (GatewayServices) filterConfig.getServletContext().getAttribute(GatewayServices.GATEWAY_SERVICES_ATTRIBUTE);
      authority = (JWTokenAuthority)services.getService(GatewayServices.TOKEN_SERVICE);
```

The above is a Knox specific internal service that we use to issue and verify JWT tokens. This will be covered separately and you will need to be implement something similar in your filter implementation.

```text
    // configured cookieName
    cookieName = filterConfig.getInitParameter(SSO_COOKIE_NAME);
    if (cookieName == null) {
      cookieName = DEFAULT_SSO_COOKIE_NAME;
    }
```

The configurable cookie name is something that can be used to change a cookie name to fit your deployment environment. The default name is hadoop-jwt which is also the default in the Hadoop implementation. This name must match the name being used by the KnoxSSO endpoint when setting the cookie.

```text
    // expected audiences or null
    String expectedAudiences = filterConfig.getInitParameter(SSO_EXPECTED_AUDIENCES);
    if (expectedAudiences != null) {
      audiences = parseExpectedAudiences(expectedAudiences);
    }
```

Audiences are configured as a comma separated list of audience strings. Names of intended recipients or intents. The semantics that we are using for this processing is that - if not configured than any (or none) audience is accepted. If there are audiences configured then as long as one of the expected ones is found in the set of claims in the token it is accepted.

```text
    // url to SSO authentication provider
    authenticationProviderUrl = filterConfig.getInitParameter(SSO_AUTHENTICATION_PROVIDER_URL);
    if (authenticationProviderUrl == null) {
      log.missingAuthenticationProviderUrlConfiguration();
    }
  }
```

This is the URL to the KnoxSSO endpoint. It is required and SSO/token exchange will not work without this set correctly.

```text
	/**
   	* @param expectedAudiences
   	* @return
   	*/
   	private List<String> parseExpectedAudiences(String expectedAudiences) {
     ArrayList<String> audList = null;
       // setup the list of valid audiences for token validation
       if (expectedAudiences != null) {
         // parse into the list
         String[] audArray = expectedAudiences.split(",");
         audList = new ArrayList<String>();
         for (String a : audArray) {
           audList.add(a);
         }
       }
       return audList;
     }
```

The above method parses the comma separated list of expected audiences and makes it available for interrogation during token validation.

```java
    public void destroy() {
    }

    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) 
        throws IOException, ServletException {
      String wireToken = null;
      HttpServletRequest req = (HttpServletRequest) request;
  
      String loginURL = constructLoginURL(req);
      wireToken = getJWTFromCookie(req);
      if (wireToken == null) {
        if (req.getMethod().equals("OPTIONS")) {
          // CORS preflight requests to determine allowed origins and related config
          // must be able to continue without being redirected
          Subject sub = new Subject();
          sub.getPrincipals().add(new PrimaryPrincipal("anonymous"));
          continueWithEstablishedSecurityContext(sub, req, (HttpServletResponse) response, chain);
        }
        log.sendRedirectToLoginURL(loginURL);
        ((HttpServletResponse) response).sendRedirect(loginURL);
      }
      else {
        JWTToken token = new JWTToken(wireToken);
        boolean verified = false;
        try {
          verified = authority.verifyToken(token);
          if (verified) {
            Date expires = token.getExpiresDate();
            if (expires == null || new Date().before(expires)) {
              boolean audValid = validateAudiences(token);
              if (audValid) {
                Subject subject = createSubjectFromToken(token);
                continueWithEstablishedSecurityContext(subject, (HttpServletRequest)request, (HttpServletResponse)response, chain);
              }
              else {
                log.failedToValidateAudience();
                ((HttpServletResponse) response).sendRedirect(loginURL);
              }
            }
            else {
              log.tokenHasExpired();
            ((HttpServletResponse) response).sendRedirect(loginURL);
            }
          }
          else {
            log.failedToVerifyTokenSignature();
          ((HttpServletResponse) response).sendRedirect(loginURL);
          }
        } catch (TokenServiceException e) {
          log.unableToVerifyToken(e);
        ((HttpServletResponse) response).sendRedirect(loginURL);
        }
      }
    }
```

The doFilter method above is where all the real work is done. We look for a cookie by the configured name. If it isnâ€™t there then we redirect to the configured SSO provider URL in order to acquire one. That is unless it is an OPTIONS request which may be a preflight CORS request. You shouldnâ€™t need to worry about this aspect. It is really a REST API concern not a web app UI one.

Once we get a cookie, the underlying JWT token is extracted and returned as the wireToken from which we create a Knox specific JWTToken. This abstraction is around the use of the nimbus JWT library which you can use directly. We will cover those details separately.

We then ask the token authority component to verify the token. This involves signature validation of the signed token. In order to verify the signature of the token you will need to have the public key of the Knox SSO server configured and provided to the nimbus library through its API at verification time. NOTE: This is a good place to look at the Hadoop implementation as an example.

Once we know the token is signed by a trusted party we then validate whether it is expired and that it has an expected (or no) audience claims.

Finally, when we have a valid token, we create a Java Subject from it and continue the request through the filterChain as the authenticated user.

```text
	/**
   	* Encapsulate the acquisition of the JWT token from HTTP cookies within the
   	* request.
   	*
   	* @param req servlet request to get the JWT token from
   	* @return serialized JWT token
   	*/
  	protected String getJWTFromCookie(HttpServletRequest req) {
    String serializedJWT = null;
    Cookie[] cookies = req.getCookies();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookieName.equals(cookie.getName())) {
          log.cookieHasBeenFound(cookieName);
          serializedJWT = cookie.getValue();
          break;
        }
      }
    }
    return serializedJWT;
  	}
```

The above method extracts the serialized token from the cookie and returns it as the wireToken.

```text
  	/**
   	* Create the URL to be used for authentication of the user in the absence of
   	* a JWT token within the incoming request.
   	*
   	* @param request for getting the original request URL
   	* @return url to use as login url for redirect
   	*/
  	protected String constructLoginURL(HttpServletRequest request) {
    String delimiter = "?";
    if (authenticationProviderUrl.contains("?")) {
      delimiter = "&";
    }
    String loginURL = authenticationProviderUrl + delimiter
        + ORIGINAL_URL_QUERY_PARAM
        + request.getRequestURL().toString()+ getOriginalQueryString(request);
    	return loginURL;
  	}

  	private String getOriginalQueryString(HttpServletRequest request) {
    	String originalQueryString = request.getQueryString();
    	return (originalQueryString == null) ? "" : "?" + originalQueryString;
  	}
```

The above method creates the full URL to be used in redirecting to the KnoxSSO endpoint. It includes the SSO provider URL as well as the original request URL so that we can redirect back to it after authentication and token exchange.

```text
  	/**
   	* Validate whether any of the accepted audience claims is present in the
   	* issued token claims list for audience. Override this method in subclasses
   	* in order to customize the audience validation behavior.
   	*
   	* @param jwtToken
   	*          the JWT token where the allowed audiences will be found
   	* @return true if an expected audience is present, otherwise false
   	*/
  	protected boolean validateAudiences(JWTToken jwtToken) {
    	boolean valid = false;
    	String[] tokenAudienceList = jwtToken.getAudienceClaims();
    	// if there were no expected audiences configured then just
    	// consider any audience acceptable
    	if (audiences == null) {
      		valid = true;
    	} else {
      		// if any of the configured audiences is found then consider it
      		// acceptable
      		for (String aud : tokenAudienceList) {
        	if (audiences.contains(aud)) {
          		//log.debug("JWT token audience has been successfully validated");
          		log.jwtAudienceValidated();
          		valid = true;
          		break;
        	}
      	}
    }
    return valid;
  	}
```

The above method implements the audience claim semantics explained earlier.

```java
	private void continueWithEstablishedSecurityContext(Subject subject, final 		HttpServletRequest request, final HttpServletResponse response, final FilterChain chain) throws IOException, ServletException {
    try {
      Subject.doAs(
        subject,
        new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            chain.doFilter(request, response);
            return null;
          }
        }
        );
    }
    catch (PrivilegedActionException e) {
      Throwable t = e.getCause();
      if (t instanceof IOException) {
        throw (IOException) t;
      }
      else if (t instanceof ServletException) {
        throw (ServletException) t;
      }
      else {
        throw new ServletException(t);
      }
    }
  	}
```

This method continues the filter chain processing upon successful validation of the token. This would need to be replaced with your environmentâ€™s equivalent of continuing the request or login to the app as the authenticated user.

```java
  	private Subject createSubjectFromToken(JWTToken token) {
    	final String principal = token.getSubject();
    	@SuppressWarnings("rawtypes")
    	HashSet emptySet = new HashSet();
    	Set<Principal> principals = new HashSet<Principal>();
    	Principal p = new PrimaryPrincipal(principal);
    	principals.add(p);
    	javax.security.auth.Subject subject = new javax.security.auth.Subject(true, principals, emptySet, emptySet);
    	return subject;
  	}
```

This method takes a JWTToken and creates a Java Subject with the principals expected by the rest of the Knox processing. This would need to be implemented in a way appropriate for your operating environment as well. For instance, the Hadoop handler implementation returns a Hadoop AuthenticationToken to the calling filter which in turn ends up in the Hadoop auth cookie.

```text
	}
```

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Token+Signature+Validation"></a>Token Signature Validation [](#knox-apache-org-books-knox-2-1-0-dev-guide--Token+Signature+Validation)

The following is the method from the Hadoop handler implementation that validates the signature.

```java
	/** 
 	* Verify the signature of the JWT token in this method. This method depends on the 	* public key that was established during init based upon the provisioned public key. 	* Override this method in subclasses in order to customize the signature verification behavior.
 	* @param jwtToken the token that contains the signature to be validated
 	* @return valid true if signature verifies successfully; false otherwise
 	*/
	protected boolean validateSignature(SignedJWT jwtToken){
  		boolean valid=false;
  		if (JWSObject.State.SIGNED == jwtToken.getState()) {
    		LOG.debug("JWT token is in a SIGNED state");
    		if (jwtToken.getSignature() != null) {
      			LOG.debug("JWT token signature is not null");
      			try {
        			JWSVerifier verifier=new RSASSAVerifier(publicKey);
        			if (jwtToken.verify(verifier)) {
          			valid=true;
          			LOG.debug("JWT token has been successfully verified");
        		}
 			else {
          		LOG.warn("JWT signature verification failed.");
        	}
      	}
 		catch (JOSEException je) {
        	LOG.warn("Error while validating signature",je);
      	}
    }
  	}
  	return valid;
	}
```

Hadoop Configuration Example The following is like the configuration in the Hadoop handler implementation.

OBSOLETE but in the proper spirit of HADOOP-11717 ( HADOOP-11717 - Add Redirecting WebSSO behavior with JWT Token in Hadoop Auth RESOLVED )

```xml
	<property>
  		<name>hadoop.http.authentication.type</name>
		<value>org.apache.hadoop/security.authentication/server.JWTRedirectAuthenticationHandler</value>
	</property>
```

This is the handler classname in Hadoop auth for JWT token (KnoxSSO) support.

```xml
	<property>
  		<name>hadoop.http.authentication.authentication.provider.url</name>
  		<value>http://c6401.ambari.apache.org:8888/knoxsso</value>
	</property>
```

The above property is the SSO provider URL that points to the knoxsso endpoint.

```xml
	<property>
   		<name>hadoop.http.authentication.public.key.pem</name>
   		<value>MIICVjCCAb+gAwIBAgIJAPPvOtuTxFeiMA0GCSqGSIb3DQEBBQUAMG0xCzAJBgNV
   	BAYTAlVTMQ0wCwYDVQQIEwRUZXN0MQ0wCwYDVQQHEwRUZXN0MQ8wDQYDVQQKEwZI
   	YWRvb3AxDTALBgNVBAsTBFRlc3QxIDAeBgNVBAMTF2M2NDAxLmFtYmFyaS5hcGFj
   	aGUub3JnMB4XDTE1MDcxNjE4NDcyM1oXDTE2MDcxNTE4NDcyM1owbTELMAkGA1UE
   	BhMCVVMxDTALBgNVBAgTBFRlc3QxDTALBgNVBAcTBFRlc3QxDzANBgNVBAoTBkhh
   	ZG9vcDENMAsGA1UECxMEVGVzdDEgMB4GA1UEAxMXYzY0MDEuYW1iYXJpLmFwYWNo
   	ZS5vcmcwgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBAMFs/rymbiNvg8lDhsdA
   	qvh5uHP6iMtfv9IYpDleShjkS1C+IqId6bwGIEO8yhIS5BnfUR/fcnHi2ZNrXX7x
   	QUtQe7M9tDIKu48w//InnZ6VpAqjGShWxcSzR6UB/YoGe5ytHS6MrXaormfBg3VW
   	tDoy2MS83W8pweS6p5JnK7S5AgMBAAEwDQYJKoZIhvcNAQEFBQADgYEANyVg6EzE
   	2q84gq7wQfLt9t047nYFkxcRfzhNVL3LB8p6IkM4RUrzWq4kLA+z+bpY2OdpkTOe
   	wUpEdVKzOQd4V7vRxpdANxtbG/XXrJAAcY/S+eMy1eDK73cmaVPnxPUGWmMnQXUi
   	TLab+w8tBQhNbq6BOQ42aOrLxA8k/M4cV1A=</value>
	</property>
```

The above property holds the KnoxSSO serverâ€™s public key for signature verification. Adding it directly to the config like this is convenient and is easily done through Ambari to existing config files that take custom properties. Config is generally protected as root access only as well - so it is a pretty good solution.

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Public+Key+Parsing"></a>Public Key Parsing [](#knox-apache-org-books-knox-2-1-0-dev-guide--Public+Key+Parsing)

In order to turn the pem encoded config item into a public key the hadoop handler implementation does the following in the init() method.

```java
   	if (publicKey == null) {
     String pemPublicKey = config.getProperty(PUBLIC_KEY_PEM);
     if (pemPublicKey == null) {
       throw new ServletException(
           "Public key for signature validation must be provisioned.");
     }
     publicKey = CertificateUtil.parseRSAPublicKey(pemPublicKey);
   }
```

and the CertificateUtil class is below:

```java
	package org.apache.hadoop.security.authentication.util;

	import java.io.ByteArrayInputStream;
	import java.io.UnsupportedEncodingException;
	import java.security.PublicKey;
	import java.security.cert.CertificateException;
	import java.security.cert.CertificateFactory;
	import java.security.cert.X509Certificate;
	import java.security.interfaces.RSAPublicKey;

	import javax.servlet.ServletException;

	public class CertificateUtil {
		private static final String PEM_HEADER = "-----BEGIN CERTIFICATE-----\n";
		private static final String PEM_FOOTER = "\n-----END CERTIFICATE-----";

	 /**
	  * Gets an RSAPublicKey from the provided PEM encoding.
 	  *
  	  * @param pem
      *          - the pem encoding from config without the header and footer
      * @return RSAPublicKey the RSA public key
      * @throws ServletException thrown if a processing error occurred
      */
 	public static RSAPublicKey parseRSAPublicKey(String pem) throws ServletException {
   		String fullPem = PEM_HEADER + pem + PEM_FOOTER;
   		PublicKey key = null;
   		try {
     		CertificateFactory fact = CertificateFactory.getInstance("X.509");
     		ByteArrayInputStream is = new ByteArrayInputStream(
         		fullPem.getBytes("UTF8"));
     		X509Certificate cer = (X509Certificate) fact.generateCertificate(is);
     		key = cer.getPublicKey();
   		} catch (CertificateException ce) {
     		String message = null;
     		if (pem.startsWith(PEM_HEADER)) {
       			message = "CertificateException - be sure not to include PEM header "
           			+ "and footer in the PEM configuration element.";
     		} else {
       			message = "CertificateException - PEM may be corrupt";
     		}
     		throw new ServletException(message, ce);
   		} catch (UnsupportedEncodingException uee) {
     		throw new ServletException(uee);
   		}
   		return (RSAPublicKey) key;
 		}
	}
```

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Health+Monitoring+API"></a>Health Monitoring API [](#knox-apache-org-books-knox-2-1-0-dev-guide--Health+Monitoring+API)

# Health Monitoring REST API

Knox provides REST-ful API for monitoring the core service. It primarily exposes the health of the Knox service that includes service status (up/down) as well as other health metrics. This is a work-in-progress feature, which started with an extensible framework to support basic functionalities. In particular, it currently supports the API to A) *ping* the service and B) time-based statistics related to all API calls.

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Health+Monitoring+Setup"></a>Health Monitoring Setup [](#knox-apache-org-books-knox-2-1-0-dev-guide--Health+Monitoring+Setup)

The basic setup includes two major steps A) add configurations to enable the metrics collection and reporting B) write a topology file and upload it into *topologies* directory.

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Service+Configurations"></a>Service Configurations [](#knox-apache-org-books-knox-2-1-0-dev-guide--Service+Configurations)

At first, we need to make sure the gateway configurations to gather and report to JMX are turned on in *gateway-site.xml*. The following two configurations into *gateway-site.xml* will serve the purpose.

```xml
<property>
   <name>gateway.metrics.enabled</name>
   <value>true</value>
   <description>Boolean flag indicates whether to enable the metrics collection</description>
</property>
<property>
   <name>gateway.jmx.metrics.reporting.enabled</name>
   <value>true</value>
   <description>Boolean flag indicates whether to enable the metrics reporting using JMX</description>
</property>
```

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--health.xml+Topology"></a>health.xml Topology [](#knox-apache-org-books-knox-2-1-0-dev-guide--health.xml+Topology)

In order to enable health monitoring REST service, you need to add a new topology file (i.e. *health.xml*). The following is an example that is configured to test the basic functionalities of Knox service. It is highly recommended using more restricted authentication mechanism.

```xml
<topology>

    <gateway>

        <provider>
            <role>authentication</role>
            <name>ShiroProvider</name>
            <enabled>true</enabled>
            <param>
                <!-- 
                session timeout in minutes,  this is really idle timeout,
                defaults to 30 mins, if the property value is not defined,, 
                current client authentication would expire if client idles continuously for more than this value
                -->
                <name>sessionTimeout</name>
                <value>30</value>
            </param>
            <param>
                <name>main.ldapRealm</name>
                <value>org.apache.knox.gateway.shirorealm.KnoxLdapRealm</value>
            </param>
            <param>
                <name>main.ldapContextFactory</name>
                <value>org.apache.knox.gateway.shirorealm.KnoxLdapContextFactory</value>
            </param>
            <param>
                <name>main.ldapRealm.contextFactory</name>
                <value>$ldapContextFactory</value>
            </param>
            <param>
                <name>main.ldapRealm.userDnTemplate</name>
                <value>uid={0},ou=people,dc=hadoop,dc=apache,dc=org</value>
            </param>
            <param>
                <name>main.ldapRealm.contextFactory.url</name>
                <value>ldap://localhost:33389</value>
            </param>
            <param>
                <name>main.ldapRealm.contextFactory.authenticationMechanism</name>
                <value>simple</value>
            </param>
            <param>
                <name>urls./**</name>
                <value>authcBasic</value>
            </param>
        </provider>

        <provider>
            <role>authorization</role>
            <name>AclsAuthz</name>
            <enabled>false</enabled>
            <param>
                <name>knox.acl</name>
                <value>admin;*;*</value>
            </param>
        </provider>

        <provider>
            <role>identity-assertion</role>
            <name>Default</name>
            <enabled>false</enabled>
        </provider>

        <provider>
            <role>hostmap</role>
            <name>static</name>
            <enabled>true</enabled>
            <param><name>localhost</name><value>sandbox,sandbox.hortonworks.com</value></param>
        </provider>

    </gateway>

    <service>
        <role>HEALTH</role>
    </service>

</topology>
```

Just as with any Knox service, the gateway providers protect the health monitoring REST service defined above it. In this case, the ShiroProvider is taking care of HTTP Basic Auth using LDAP. Once the user authenticates with LDAP, the request processing continues to the *Health* service that will perform the necessary actions.

The authenticate/federation provider can be swapped out to fit your deployment environment.

After creating the file health.xml with above contents, you need to copy the file to *KNOX\_HOME/conf/topologies* directory. If Knox/gateway service is not running, you can start it using “*bin/gateway.sh start*”. Otherwise the service would automatically pick this new ‘*health*’ service. When gateway service registers the new service, it displays the following log messages in *log/gateway.log*.

```text
2017-08-22 03:44:25,045 INFO  knox.gateway (GatewayServer.java:handleCreateDeployment(677)) - Deploying topology health to /home/joe/knox/knox-0.12.0/bin/../data/deployments/health.topo.15e080a91c0
2017-08-22 03:44:25,045 INFO  knox.gateway (GatewayServer.java:internalDeactivateTopology(596)) - Deactivating topology health
2017-08-22 03:44:25,119 INFO  knox.gateway (DefaultGatewayServices.java:initializeContribution(197)) - Creating credential store for the cluster: health
2017-08-22 03:44:25,142 INFO  knox.gateway (GatewayServer.java:internalActivateTopology(566)) - Activating topology health
2017-08-22 03:44:25,142 INFO  knox.gateway (GatewayServer.java:internalActivateArchive(576)) - Activating topology health archive %2F
```

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Verify"></a>Verify [](#knox-apache-org-books-knox-2-1-0-dev-guide--Verify)

Once the health service is active, you can verify it by using the following *curl* command. The ‘*ping*’ end point displays if the service is up. This end point can be utilized for monitoring the basic health of a Knox service.

```bash
$ curl -i -k -u guest:guest-password -X GET 'https://localhost:8445/gateway/health/v1/ping'
HTTP/1.1 200 OK
Date: Tue, 22 Aug 2017 07:09:37 GMT
Set-Cookie: JSESSIONID=1o82bcvoqbhbb1apt7zs8ubybb;Path=/gateway/health;Secure;HttpOnly
Expires: Thu, 01 Jan 1970 00:00:00 GMT
Set-Cookie: rememberMe=deleteMe; Path=/gateway/health; Max-Age=0; Expires=Mon, 21-Aug-2017 07:09:37 GMT
Cache-Control: must-revalidate,no-cache,no-store
Content-Type: text/plain; charset=ISO-8859-1
Content-Length: 3
Server: Jetty(9.2.15.v20160210)

OK
```

To retrieve the meaningful metrics details of various service calls, you may need to run multiple REST calls such as the followings. After that, execute the metrics REST call as shown below with a sample output. As shown, metrics output is returned in JSON format.

```bash
curl -i -k -u guest:guest-password -X GET 'https://localhost:8445/gateway/sandbox/webhdfs/v1/?op=LISTSTATUS'
```

```bash
$ curl -i -k -u guest:guest-password -X GET 'https://localhost:8445/gateway/health/v1/metrics?pretty=true'
HTTP/1.1 200 OK
Date: Tue, 22 Aug 2017 07:10:44 GMT
Set-Cookie: JSESSIONID=kqntcdaje9uai3pup7ffvfw4;Path=/gateway/health;Secure;HttpOnly
Expires: Thu, 01 Jan 1970 00:00:00 GMT
Set-Cookie: rememberMe=deleteMe; Path=/gateway/health; Max-Age=0; Expires=Mon, 21-Aug-2017 07:10:44 GMT
Content-Type: application/json
Cache-Control: must-revalidate,no-cache,no-store
Transfer-Encoding: chunked
Server: Jetty(9.2.15.v20160210)

{
  "version" : "3.0.0",
  "gauges" : { },
  "counters" : { },
  "histograms" : { },
  "meters" : { },
  "timers" : {
    "client./gateway/health/v1/metrics.GET-requests" : {
      "count" : 5,
      "max" : 0.624587973,
      "mean" : 0.027655743001736188,
      "min" : 0.006145587,
      "p50" : 0.010020548,
      "p75" : 0.010020548,
      "p95" : 0.074454725,
      "p98" : 0.624587973,
      "p99" : 0.624587973,
      "p999" : 0.624587973,
      "stddev" : 0.0929226225229978,
      "m15_rate" : 2.657500857422334E-7,
      "m1_rate" : 5.770087852901534E-89,
      "m5_rate" : 4.769163772973399E-19,
      "mean_rate" : 4.0952378345310894E-4,
      "duration_units" : "seconds",
      "rate_units" : "calls/second"
    },
    "client./gateway/health/v1/ping.GET-requests" : {
      "count" : 1,
      "max" : 0.017257638000000002,
      "mean" : 0.017257638000000002,
      "min" : 0.017257638000000002,
      "p50" : 0.017257638000000002,
      "p75" : 0.017257638000000002,
      "p95" : 0.017257638000000002,
      "p98" : 0.017257638000000002,
      "p99" : 0.017257638000000002,
      "p999" : 0.017257638000000002,
      "stddev" : 0.0,
      "m15_rate" : 0.18710139700632353,
      "m1_rate" : 0.0735758882342885,
      "m5_rate" : 0.1637461506155964,
      "mean_rate" : 0.014990517517814805,
      "duration_units" : "seconds",
      "rate_units" : "calls/second"
    },
    "client./gateway/sandbox/health/v1/.GET-requests" : {
      "count" : 1,
      "max" : 4.01873E-4,
      "mean" : 4.01873E-4,
      "min" : 4.01873E-4,
      "p50" : 4.01873E-4,
      "p75" : 4.01873E-4,
      "p95" : 4.01873E-4,
      "p98" : 4.01873E-4,
      "p99" : 4.01873E-4,
      "p999" : 4.01873E-4,
      "stddev" : 0.0,
      "m15_rate" : 2.536740427767808E-7,
      "m1_rate" : 7.074903404511115E-90,
      "m5_rate" : 4.081014139447941E-19,
      "mean_rate" : 8.179827684854002E-5,
      "duration_units" : "seconds",
      "rate_units" : "calls/second"
    },
    "client./gateway/sandbox/v1/health/.GET-requests" : {
      "count" : 1,
      "max" : 5.470700000000001E-4,
      "mean" : 5.470700000000001E-4,
      "min" : 5.470700000000001E-4,
      "p50" : 5.470700000000001E-4,
      "p75" : 5.470700000000001E-4,
      "p95" : 5.470700000000001E-4,
      "p98" : 5.470700000000001E-4,
      "p99" : 5.470700000000001E-4,
      "p999" : 5.470700000000001E-4,
      "stddev" : 0.0,
      "m15_rate" : 2.413022137213267E-7,
      "m1_rate" : 3.341947732164585E-90,
      "m5_rate" : 3.512561421726287E-19,
      "mean_rate" : 8.149518570285245E-5,
      "duration_units" : "seconds",
      "rate_units" : "calls/second"
    },
    "client./gateway/sandbox/webhdfs/v1/.GET-requests" : {
      "count" : 4,
      "max" : 0.463745401,
      "mean" : 0.024924118143299912,
      "min" : 0.016542244,
      "p50" : 0.024799078000000002,
      "p75" : 0.033933548,
      "p95" : 0.033933548,
      "p98" : 0.033933548,
      "p99" : 0.033933548,
      "p999" : 0.033933548,
      "stddev" : 0.007284773511002474,
      "m15_rate" : 2.120680068580741E-8,
      "m1_rate" : 4.7541228609699333E-91,
      "m5_rate" : 1.5806080232092864E-20,
      "mean_rate" : 2.7314359915623396E-4,
      "duration_units" : "seconds",
      "rate_units" : "calls/second"
    },
    "service./gateway/sandbox/webhdfs/v1/.get-requests" : {
      "count" : 3,
      "max" : 0.014635496000000001,
      "mean" : 0.00342438191233768,
      "min" : 0.0020088890000000002,
      "p50" : 0.0020088890000000002,
      "p75" : 0.005144646,
      "p95" : 0.005144646,
      "p98" : 0.005144646,
      "p99" : 0.005144646,
      "p999" : 0.005144646,
      "stddev" : 0.0015604555820128599,
      "m15_rate" : 1.9913776931949195E-8,
      "m1_rate" : 3.1334281325640874E-91,
      "m5_rate" : 1.055281734633953E-20,
      "mean_rate" : 2.0486339070804923E-4,
      "duration_units" : "seconds",
      "rate_units" : "calls/second"
    }
  }
}
```

#### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--REST+End+Points"></a>REST End Points [](#knox-apache-org-books-knox-2-1-0-dev-guide--REST+End+Points)

As mentioned above, currently Knox provides a few monitoring APIs to start with. The list will gradually grow to support new use-cases.

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--/ping"></a>/ping [](#knox-apache-org-books-knox-2-1-0-dev-guide--/ping)

This end-point can be used to determine if a Knox gateway service is alive or not. It is useful for basic health monitoring of the core service. Although most of the results of REST calls are in JSON format, this one (\*/ping\*) is in plain text.

Sample response

```text
OK
```

##### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--/metrics"></a>/metrics [](#knox-apache-org-books-knox-2-1-0-dev-guide--/metrics)

This end-point returns all Knox metrics grouped by individual call type. For example, timer metrics for all *webhdfs* calls are aggregated into one set of metrics and then returned in a separate JSON element. This end-point also supports an option (\*/metrics?pretty=true\*) to pretty print the metrics output.

A sample response with *pretty=true* is shown below:

```json
{
  "version" : "3.0.0",
  "gauges" : { },
  "counters" : { },
  "histograms" : { },
  "meters" : { },
  "timers" : {
    "client./gateway/health/v1/ping.GET-requests" : {
      "count" : 1,
      "max" : 0.017257638000000002,
      "mean" : 0.017257638000000002,
      "min" : 0.017257638000000002,
      "p50" : 0.017257638000000002,
      "p75" : 0.017257638000000002,
      "p95" : 0.017257638000000002,
      "p98" : 0.017257638000000002,
      "p99" : 0.017257638000000002,
      "p999" : 0.017257638000000002,
      "stddev" : 0.0,
      "m15_rate" : 0.18710139700632353,
      "m1_rate" : 0.0735758882342885,
      "m5_rate" : 0.1637461506155964,
      "mean_rate" : 0.014990517517814805,
      "duration_units" : "seconds",
      "rate_units" : "calls/second"
    },
    "client./gateway/sandbox/v1/health/.GET-requests" : {
      "count" : 1,
      "max" : 5.470700000000001E-4,
      "mean" : 5.470700000000001E-4,
      "min" : 5.470700000000001E-4,
      "p50" : 5.470700000000001E-4,
      "p75" : 5.470700000000001E-4,
      "p95" : 5.470700000000001E-4,
      "p98" : 5.470700000000001E-4,
      "p99" : 5.470700000000001E-4,
      "p999" : 5.470700000000001E-4,
      "stddev" : 0.0,
      "m15_rate" : 2.413022137213267E-7,
      "m1_rate" : 3.341947732164585E-90,
      "m5_rate" : 3.512561421726287E-19,
      "mean_rate" : 8.149518570285245E-5,
      "duration_units" : "seconds",
      "rate_units" : "calls/second"
    },
    "client./gateway/sandbox/webhdfs/v1/.GET-requests" : {
      "count" : 4,
      "max" : 0.463745401,
      "mean" : 0.024924118143299912,
      "min" : 0.016542244,
      "p50" : 0.024799078000000002,
      "p75" : 0.033933548,
      "p95" : 0.033933548,
      "p98" : 0.033933548,
      "p99" : 0.033933548,
      "p999" : 0.033933548,
      "stddev" : 0.007284773511002474,
      "m15_rate" : 2.120680068580741E-8,
      "m1_rate" : 4.7541228609699333E-91,
      "m5_rate" : 1.5806080232092864E-20,
      "mean_rate" : 2.7314359915623396E-4,
      "duration_units" : "seconds",
      "rate_units" : "calls/second"
    }
  }
}
```

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Auditing"></a>Auditing [](#knox-apache-org-books-knox-2-1-0-dev-guide--Auditing)

```java
public class AuditingSample {

  private static Auditor AUDITOR = AuditServiceFactory.getAuditService().getAuditor(
      "sample-channel", "sample-service", "sample-component" );

  public void sampleMethod() {
      ...
      AUDITOR.audit( Action.AUTHORIZATION, sourceUrl, ResourceType.URI, ActionOutcome.SUCCESS );
      ...
  }

}
```

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Logging"></a>Logging [](#knox-apache-org-books-knox-2-1-0-dev-guide--Logging)

```java
@Messages( logger = "org.apache.project.module" )
public interface CustomMessages {

  @Message( level = MessageLevel.FATAL, text = "Failed to parse command line: {0}" )
  void failedToParseCommandLine( @StackTrace( level = MessageLevel.DEBUG ) ParseException e );

}
```

```java
public class CustomLoggingSample {

  private static GatewayMessages MSG = MessagesFactory.get( GatewayMessages.class );

  public void sampleMethod() {
    ...
    MSG.failedToParseCommandLine( e );
    ...
  }

}
```

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Internationalization"></a>Internationalization [](#knox-apache-org-books-knox-2-1-0-dev-guide--Internationalization)

```java
@Resources
public interface CustomResources {

  @Resource( text = "Apache Hadoop Gateway {0} ({1})" )
  String gatewayVersionMessage( String version, String hash );

}
```

```java
public class CustomResourceSample {

  private static GatewayResources RES = ResourcesFactory.get( GatewayResources.class );

  public void sampleMethod() {
    ...
    String s = RES.gatewayVersionMessage( "0.0.0", "XXXXXXX" ) );
    ...
  }

}
```

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Admin+UI"></a>Admin UI [](#knox-apache-org-books-knox-2-1-0-dev-guide--Admin+UI)

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Introduction"></a>Introduction [](#knox-apache-org-books-knox-2-1-0-dev-guide--Introduction)

The Admin UI is a work in progress. It has started with viewpoint of being a simple web interface for Admin API functions but will hopefully grow into being able to also provide visibility into the gateway in terms of logs and metrics.

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Source+and+Binaries"></a>Source and Binaries [](#knox-apache-org-books-knox-2-1-0-dev-guide--Source+and+Binaries)

The Admin UI application follows the architecture of a hosted application in Knox. To that end it needs to be packaged up in the gateway-applications module in the source tree so that in the installation it can wind up here

`<GATEWAY_HOME>/data/applications/admin-ui`

However since the application is built using angular and various node modules the source tree is not something we want to place into the gateway-applications module. Instead we will place the production ‘binaries’ in gateway-applications and have the source in a module called ‘gateway-admin-ui’.

To work with the angular application you need to install some prerequisite tools.

The main tool needed is the [angular cli](https://github.com/angular/angular-cli#installation) and while installing that you will get its dependencies which should fulfill any other requirements [Prerequisites](https://github.com/angular/angular-cli#prerequisites)

### <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Manager+Topology"></a>Manager Topology [](#knox-apache-org-books-knox-2-1-0-dev-guide--Manager+Topology)

The Admin UI is deployed to a fixed topology. The topology file can be found under

`<GATEWAY_HOME>/conf/topologies/manager.xml`

The topology hosts an instance of the Admin API for the UI to use. The reason for this is that the existing Admin API needs to have a different security model from that used by the Admin UI. The key components of this topology are:

```xml
<provider>
    <role>webappsec</role>
    <name>WebAppSec</name>
    <enabled>true</enabled>
    <param><name>csrf.enabled</name><value>true</value></param>
    <param><name>csrf.customHeader</name><value>X-XSRF-Header</value></param>
    <param><name>csrf.methodsToIgnore</name><value>GET,OPTIONS,HEAD</value></param>
    <param><name>xframe-options.enabled</name><value>true</value></param>
    <param><name>strict.transport.enabled</name><value>true</value></param>
</provider>
```

and

```xml
<application>
    <role>admin-ui</role>
</application>
```

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Trademarks"></a>Trademarks [](#knox-apache-org-books-knox-2-1-0-dev-guide--Trademarks)

Apache Knox, Apache Knox Gateway, Apache, the Apache feather logo and the Apache Knox Gateway project logos are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--License"></a>License [](#knox-apache-org-books-knox-2-1-0-dev-guide--License)

Apache Knox uses the standard [Apache license](https://www.apache.org/licenses/LICENSE-2.0).

## <a id="knox-apache-org-books-knox-2-1-0-dev-guide--Privacy+Policy"></a>Privacy Policy [](#knox-apache-org-books-knox-2-1-0-dev-guide--Privacy+Policy)

Apache Knox uses the standard Apache privacy policy.

Information about your use of this website is collected using server access logs and a tracking cookie. The collected information consists of the following:

- The IP address from which you access the website;
- The type of browser and operating system you use to access our site;
- The date and time you access our site;
- The pages you visit; and
- The addresses of pages from where you followed a link to our site.

Part of this information is gathered using a tracking cookie set by the [Google Analytics](http://www.google.com/analytics/) service. Google’s policy for the use of this information is described in their [privacy policy](http://www.google.com/privacy.html). See your browser’s documentation for instructions on how to disable the cookie if you prefer not to share this data with Google.

We use the gathered information to help us make our site more useful to visitors and to better understand how and when our site is used. We do not track or collect personally identifiable information or associate gathered data with any personally identifying information from other sources.

By using this website, you consent to the collection of this data in the manner and for the purpose described above.