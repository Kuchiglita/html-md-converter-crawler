<a id="syncope-apache-org-docs-4-0-getting-started"></a>

# Apache Syncope 4.0.5 - Getting Started

![Apache Syncope logo](syncope.apache.org/docs/4.0/images/apache-syncope-logo-small.jpg)

|  | This document is under active development and discussion!If you find errors or omissions in this document, please don’t hesitate tosubmit an issueoropen a pull requestwith a fix. We also encourage you to ask questions and discuss any aspects of the project on themailing lists or IRC. New contributors are always welcome! |
| --- | --- |

## Preface

This guide shows you how to get started with Apache Syncope services for:

- identity management, provisioning and compliance;
- access management, single sign-on, authentication and authorization;
- API gateway, secure proxy, service mesh, request routing.

## [1. Introduction](#syncope-apache-org-docs-4-0-getting-started--introduction)

**Apache Syncope** is an Open Source system for managing digital identities in enterprise environments, implemented in
Jakarta EE technology and released under the Apache 2.0 license.

Often, *Identity Management* and *Access Management* are jointly referred, mainly because their two management worlds
likely coexist in the same project or in the same environment.

The two topics are however completely different: each one has its own context, its own rules, its own best practices.

On the other hand, some products provide unorthodox implementations so it is indeed possible to do the same thing with
both of them.

- Identity Management

  Tools and practices to keep identity data consistent and synchronized across repositories, data
  formats and models.
- Access Management

  Systems, protocols and technologies supporting user authentication (how Users are let accessing a
  given system) and authorization (which capabilities each user owns on a given system).

From the definitions above, Identity Management and Access Management can be seen as complementary: very often, the data
synchronized by the former are then used by the latter to provide its features - e.g. authentication and authorization.

### [1.1. What is Identity Management, anyway?](#syncope-apache-org-docs-4-0-getting-started--what-is-identity-management-anyway)

- Account

  Computers work with records of data about people. Such records contain technical information needed by the
  system for which the account is created and managed.
- (Digital) Identity

  A representation of a set of claims made by one digital subject about itself. **It’s you!**

Have you ever been hired by a company, entered an organization or just created a new Google account?
Companies, organizations and cloud entities work with applications that need your data to function properly:
username, password, e-mail, first name, surname, and more.

Where is this information going to come from? And what happens when you need to be enabled for more applications? And what if
you get promoted and acquire more rights on the applications you already had access to?
Most important, what happens when you quit or they gently let you go?

In brief, Identity Management takes care of managing identity data throughout what is called the **Identity Lifecycle**.

![Identity Lifecycle](syncope.apache.org/docs/4.0/images/identityLifecycle.png)

Figure 1. Identity Lifecycle

Users, Groups and Any Objects

Since Apache Syncope 2.0.0, the managed identities are not limited anymore to Users and Groups. New object types can be
defined so that Any Object’s data can be managed through Syncope: workstations, printers, folders, sensors, services,
and so on. This positions Apache Syncope at the forefront for bringing Identity Management to the IoT world.

### [1.2. What is Access Management, anyway?](#syncope-apache-org-docs-4-0-getting-started--what-is-access-management-anyway)

Authenticate, authorize and audit access to applications and IT systems: access management solutions help strengthen
security and reduce risk by tightly controlling access to on-premises and cloud-based applications, services, and IT
infrastructure.  
Access Management help ensure the right users have access to the right resources at the right times for the right
reasons.

Single sign-on (SSO) is an authentication scheme that allows a user to access multiple, independent applications with a
single set of login credentials, without re-entering authentication factors.  
Very often, SSO is achieved by implementing some of the most popular protocols as
[SAML](https://en.wikipedia.org/wiki/Security_Assertion_Markup_Language) and [OpenID Connect](http://openid.net/connect/).

Social login, designed to simplify logins, is a form of single sign-on using existing information from a social
networking service to sign into a third-party website instead of creating a new login account specifically for that
website.

### [1.3. Identity and Access Management - Reference Scenario](#syncope-apache-org-docs-4-0-getting-started--identity-and-access-management-reference-scenario)

![IAM Scenario](syncope.apache.org/docs/4.0/images/iam-scenario.png)

Figure 2. IAM Scenario

The picture above shows the technologies involved in a complete IAM solution:

- ***Identity Store*** (examples are relational databases, LDAP, Active Directory, meta- and virtual-directories,
  cloud resources, …​): the repository for account data
- ***Identity Manager***: synchronizes account data across Identity Stores and a broad range of data formats, models,
  meanings and purposes
- ***Access Manager***: security mediator to all applications, focused on application front-end, taking care of
  authentication, authorization and federation
- ***Secure Proxy***: enforces security policies on API and legacy applications

#### [1.3.1. Aren’t Identity Stores enough?](#syncope-apache-org-docs-4-0-getting-started--arent-identity-stores-enough)

One might suppose that a single Identity Store can solve all the identity needs inside an organization, but there
are a few drawbacks with this approach:

1. Heterogeneity of systems
2. Lack of a single source of information (HR for corporate id, Groupware for mail address, …​)
3. Often applications require a local user database
4. Inconsistent policies across the infrastructure
5. Lack of workflow management
6. Hidden infrastructure management cost, growing with the size of the organization

### [1.4. A bird’s eye view on the Architecture](#syncope-apache-org-docs-4-0-getting-started--a-birds-eye-view-on-the-architecture)

![Architecture](syncope.apache.org/docs/4.0/images/architecture.png)

Figure 3. Architecture

***Keymaster*** allows for dynamic service discovery so that other components are able to find each other.

***Admin UI*** is the web-based console for configuring and administering running deployments, with full support
for delegated administration.

***End-user UI*** is the web-based application for self-registration, self-service and password reset.

***Web Access*** or ***WA*** is the central hub for authentication, authorization and single sign-on.

***Secure Remote Access*** or ***SRA*** is a security-enabled API gateway with HTTP reverse proxying capabilities.

***Core*** is the component providing IdM services and acting as central repository for other components' configuration.  
It exposes a fully-compliant [Jakarta RESTful Web Services 3.1](https://en.wikipedia.org/wiki/Jakarta_RESTful_Web_Services)
[RESTful](https://en.wikipedia.org/wiki/Representational_state_transfer) interface which enables third-party applications,
written in any programming language, to consume IdM services.

- ***Logic*** implements the overall business logic that can be triggered via REST services, and controls some additional
  features (notifications, reports and auditing)
- ***Provisioning*** is involved with managing the internal (via workflow) and external (via specific connectors)
  representation of Users, Groups and Any Objects.  
  This component often needs to be tailored to meet the requirements of a specific deployment, as it is the crucial decision
  point for defining and enforcing the consistency and transformations between internal and external data. The default
  all-Java implementation can be extended for this purpose.
- ***Workflow*** is one of the pluggable aspects of Apache Syncope: this lets every deployment choose the preferred engine
  from a provided list - including one based on [Flowable](https://www.flowable.org/), the reference open source
  [BPMN 2.0](http://www.bpmn.org/) implementations - or define new, custom ones.
- ***Persistence*** manages all data (users, groups, attributes, resources, …​) at a high level
  using a standard [Jakarta Persistence 3.1](https://en.wikipedia.org/wiki/Jakarta_Persistence) approach. The data is persisted to an underlying
  database, referred to as ***Internal Storage***. Consistency is ensured via the comprehensive
  [transaction management](https://docs.spring.io/spring-framework/reference/6.2/data-access/transaction.html)
  provided by the Spring Framework.  
  Globally, this offers the ability to easily scale up to a million entities and at the same time allows great portability with no code
  changes: PostgreSQL, MySQL, MariaDB and Oracle are fully supported deployment options.
- ***Security*** defines a fine-grained set of entitlements which can be granted to administrators, thus enabling the
  implementation of delegated administration scenarios.

Third-party applications are provided full access to IdM services by leveraging the REST interface, either via the
Java Client Library (the basis of Admin UI and End-user UI) or plain HTTP calls.

ConnId

The ***Provisioning*** layer relies on [ConnId](http://connid.tirasa.net); ConnId is designed to separate the
implementation of an application from the dependencies of the system that the application is attempting to connect to.

ConnId is the continuation of The Identity Connectors Framework (Sun ICF), a project that used to be part of market
leader Sun IdM and has since been released by Sun Microsystems as an Open Source project. This makes the connectors layer
particularly reliable because most connectors have already been implemented in the framework and widely tested.

The new ConnId project, featuring contributors from several companies, provides all that is required nowadays for a
modern Open Source project, including an Apache Maven driven build, artifacts and mailing lists. Additional connectors –
such as for SOAP, CSV, PowerShell and Active Directory – are also provided.

## [2. System Requirements](#syncope-apache-org-docs-4-0-getting-started--system-requirements)

### [2.1. Hardware](#syncope-apache-org-docs-4-0-getting-started--hardware)

The hardware requirements depend greatly on the given deployment, in particular the total number of
managed entities (Users, Groups and Any Objects), their attributes and resources.

- CPU: dual core, 2 GHz (minimum)
- RAM: 8 GB (minimum)
- Disk: 200 MB (minimum)

### [2.2. Java](#syncope-apache-org-docs-4-0-getting-started--java)

Apache Syncope 4.0.5 requires the latest JDK 21 that is available. Works with later versions.

### [2.3. Jakarta EE Container](#syncope-apache-org-docs-4-0-getting-started--jakarta-ee-container)

Apache Syncope 4.0.5 is verified with the following Jakarta EE containers:

1. [Apache Tomcat 10](https://tomcat.apache.org/download-10.cgi)
2. [Payara Server 6](https://www.payara.fish/)
3. [Wildfly 38](https://www.wildfly.org/)

### [2.4. Internal Storage](#syncope-apache-org-docs-4-0-getting-started--internal-storage)

Apache Syncope 4.0.5 is verified with the recent versions of the following DBMSes, for internal storage:

1. [PostgreSQL](https://www.postgresql.org/) (>= 17-alpine, JDBC driver >= 42.7.10)
2. [MariaDB](https://mariadb.org/) (>= 12, JDBC driver >= 3.5.7)
3. [MySQL](https://www.mysql.com/) (>= 9.0, JDBC driver >= 9.6.0)
4. [Oracle Database](https://www.oracle.com/database/index.html) (>= 23-slim-faststart, JDBC driver >= ojdbc11 23.26.1.0.0)

## [3. Obtain Apache Syncope](#syncope-apache-org-docs-4-0-getting-started--obtain-apache-syncope)

There are several ways to obtain Apache Syncope: each of which has advantages or caveats for different types of users.

### [3.1. Standalone](#syncope-apache-org-docs-4-0-getting-started--standalone)

The standalone distribution is the simplest way to start exploring Apache Syncope: it contains a fully working, in-memory
Tomcat-based environment that can be easily grabbed and put at work on any modern laptop, workstation or server.

|  | Target AudienceFirst approach, especially with Admin and End-user UIs; does not require technical skills.Not meant for any production environment. |
| --- | --- |

Getting ready in a few easy steps:

1. [download](https://syncope.apache.org/downloads) the standalone distribution
2. unzip the distribution archive
3. go into the created Apache Tomcat directory
4. start Apache Tomcat

   - GNU / Linux, Mac OS X

     ```bash
     $ chmod 755 ./bin/*.sh
     $ ./bin/startup.sh
     ```
   - Windows

     ```dos
     > bin/startup.bat
     ```

|  | Please refer to theApache Tomcat documentationfor more advanced setup and instructions. |
| --- | --- |

#### [3.1.1. Components](#syncope-apache-org-docs-4-0-getting-started--standalone-components)

The set of provided components, including access URLs and credentials, is the same as reported for
[embedded mode](#syncope-apache-org-docs-4-0-getting-started--paths-and-components), with the exception of log files, available here under `$CATALINA_HOME/logs`.

### [3.2. Docker](#syncope-apache-org-docs-4-0-getting-started--docker)

[Docker](https://www.docker.com/) images ready to use, published to [Docker Hub](https://hub.docker.com).

|  | Target AudienceGetting up and running quickly on Docker.All configurations available to set, difficult customizations. |
| --- | --- |

|  | Working with these images requires to have Docker correctly installed and configured. |
| --- | --- |

|  | The Docker images can be used with orchestration tools asDocker ComposeorKubernetes. |
| --- | --- |

#### [3.2.1. Docker images](#syncope-apache-org-docs-4-0-getting-started--docker-images)

All images share a commong set of environment variables:

- `KEYMASTER_ADDRESS`: Keymaster address
- `KEYMASTER_USERNAME`: username for Keymaster authentication
- `KEYMASTER_PASSWORD`: password for Keymaster authentication
- `SERVICE_DISCOVERY_ADDRESS`: address to publish to Keymaster for the current instance
- `ANONYMOUS_USER`: username for service-to-service authentication
- `ANONYMOUS_KEY`: password for service-to-service authentication

##### [Core](#syncope-apache-org-docs-4-0-getting-started--core)

Apache Syncope Core, see [above](#syncope-apache-org-docs-4-0-getting-started--a-birds-eye-view-on-the-architecture) for information.

Port exposed: `8080`.

Environment variables:

- `DB_URL`: JDBC URL of internal storage
- `DB_USER`: username for internal storage authentication
- `DB_PASSWORD`: password for internal storage authentication
- `DB_POOL_MAX`: internal storage connection pool: ceiling
- `DB_POOL_MIN`: internal storage connection pool: floor
- `OPENJPA_REMOTE_COMMIT`: configure multiple instances, with high availability; valid values are the ones accepted by
  OpenJPA for
  [remote event notification](https://openjpa.apache.org/builds/4.0.1/apache-openjpa/docs/ref_guide_event.html) including
  `sjvm` (single instance)

##### [Console](#syncope-apache-org-docs-4-0-getting-started--console)

Apache Syncope Admin UI, see [above](#syncope-apache-org-docs-4-0-getting-started--a-birds-eye-view-on-the-architecture) for information.

Port exposed: `8080`.

##### [Enduser](#syncope-apache-org-docs-4-0-getting-started--enduser)

Apache Syncope Enduser UI, see [above](#syncope-apache-org-docs-4-0-getting-started--a-birds-eye-view-on-the-architecture) for information.

Port exposed: `8080`.

##### [WA](#syncope-apache-org-docs-4-0-getting-started--wa)

Apache Syncope Web Access, see [above](#syncope-apache-org-docs-4-0-getting-started--a-birds-eye-view-on-the-architecture) for information.

Port exposed: `8080`.

Environment variables:

- `CAS_SERVER_NAME`: public base URL to reach this instance; in case of clustered setup, this is the public-facing
  address and not the individual node address

##### [SRA](#syncope-apache-org-docs-4-0-getting-started--sra)

Apache Syncope Secure Remote Access, see [above](#syncope-apache-org-docs-4-0-getting-started--a-birds-eye-view-on-the-architecture) for information.

Port exposed: `8080`.

#### [3.2.2. Docker Compose samples](#syncope-apache-org-docs-4-0-getting-started--docker-compose-samples)

Besides the ones reported below, more samples are
[available](https://github.com/apache/syncope/tree/syncope-4.0.5/docker/src/main/resources/docker-compose).

Example 1. Core, Admin UI and Enduser UI with PostgreSQL, with embedded Keymaster

The `docker-compose.yml` below will create and connect 4 Docker containers to provide an IdM-only, single
instance, Apache Syncope deployment. All referenced images are available on Docker Hub.

|  | In this sample we are configuring an embedded, REST-based Keymaster henceKEYMASTER_USERNAME/KEYMASTER_PASSWORDare passed with same values asANONYMOUS_USER/ANONYMOUS_KEY. |
| --- | --- |

```yaml
services:
   db: (1)
     image: postgres:latest
     restart: always
     environment:
       POSTGRES_DB: syncope
       POSTGRES_USER: syncope
       POSTGRES_PASSWORD: syncope

   syncope: (2)
     depends_on:
       - db
     image: apache/syncope:4.0.5
     ports:
       - "18080:8080"
     restart: always
     environment:
       SPRING_PROFILES_ACTIVE: docker,postgresql,saml2
       DB_URL: jdbc:postgresql://db:5432/syncope?stringtype=unspecified
       DB_USER: syncope
       DB_PASSWORD: syncope
       DB_POOL_MAX: 20
       DB_POOL_MIN: 5
       OPENJPA_REMOTE_COMMIT: sjvm
       KEYMASTER_ADDRESS: http://localhost:8080/syncope/rest/keymaster
       KEYMASTER_USERNAME: ${ANONYMOUS_USER}
       KEYMASTER_PASSWORD: ${ANONYMOUS_KEY}
       SERVICE_DISCOVERY_ADDRESS: https://syncope:8080/syncope/rest/
       ANONYMOUS_USER: ${ANONYMOUS_USER}
       ANONYMOUS_KEY: ${ANONYMOUS_KEY}

   syncope-console: (3)
     depends_on:
       - syncope
     image: apache/syncope-console:4.0.5
     ports:
       - "28080:8080"
     restart: always
     environment:
       SPRING_PROFILES_ACTIVE: docker,saml2
       KEYMASTER_ADDRESS: https://syncope:8080/syncope/rest/keymaster
       KEYMASTER_USERNAME: ${ANONYMOUS_USER}
       KEYMASTER_PASSWORD: ${ANONYMOUS_KEY}
       SERVICE_DISCOVERY_ADDRESS: https://syncope-console:8080/syncope-console/
       ANONYMOUS_USER: ${ANONYMOUS_USER}
       ANONYMOUS_KEY: ${ANONYMOUS_KEY}

   syncope-enduser: (4)
     depends_on:
       - syncope
     image: apache/syncope-enduser:4.0.5
     ports:
       - "38080:8080"
     restart: always
     environment:
       SPRING_PROFILES_ACTIVE: docker,saml2
       KEYMASTER_ADDRESS: https://syncope:8080/syncope/rest/keymaster
       KEYMASTER_USERNAME: ${ANONYMOUS_USER}
       KEYMASTER_PASSWORD: ${ANONYMOUS_KEY}
       SERVICE_DISCOVERY_ADDRESS: https://syncope-enduser:8080/syncope-enduser/
       ANONYMOUS_USER: ${ANONYMOUS_USER}
       ANONYMOUS_KEY: ${ANONYMOUS_KEY}
```

| 1 | Database container for usage as internal storage, based on latest PostgreSQL image available |
| --- | --- |
| 2 | Apache Syncope Core, single instance, port18080exposed |
| 3 | Apache Syncope Admin UI, port28080exposed |
| 4 | Apache Syncope Enduser UI, port38080exposed |

Example 2. Full deployment (Core, Admin UI, Enduser UI, WA, SRA) on PostgreSQL, with Keymaster on Zookeeper

The `docker-compose.yml` below will create and connect 7 Docker containers to provide a full-fledged, single
instance, Apache Syncope deployment. All referenced images are available on Docker Hub.

|  | Zookeeper is configured without JAAS, hence emptyKEYMASTER_USERNAME/KEYMASTER_PASSWORDare passed to other containers. |
| --- | --- |

```yaml
services:
   keymaster: (1)
     image: zookeeper:latest
     restart: always

   db: (2)
     image: postgres:latest
     restart: always
     environment:
       POSTGRES_DB: syncope
       POSTGRES_USER: syncope
       POSTGRES_PASSWORD: syncope

   syncope: (3)
     depends_on:
       - db
       - keymaster
     image: apache/syncope:4.0.5
     ports:
       - "18080:8080"
     restart: always
     environment:
       SPRING_PROFILES_ACTIVE: docker,postgresql,saml2
       DB_URL: jdbc:postgresql://db:5432/syncope?stringtype=unspecified
       DB_USER: syncope
       DB_PASSWORD: syncope
       DB_POOL_MAX: 20
       DB_POOL_MIN: 5
       OPENJPA_REMOTE_COMMIT: sjvm
       KEYMASTER_ADDRESS: keymaster:2181
       KEYMASTER_USERNAME: ${KEYMASTER_USERNAME:-}
       KEYMASTER_PASSWORD: ${KEYMASTER_PASSWORD:-}
       SERVICE_DISCOVERY_ADDRESS: https://syncope:8080/syncope/rest/
       ANONYMOUS_USER: ${ANONYMOUS_USER}
       ANONYMOUS_KEY: ${ANONYMOUS_KEY}

   syncope-console: (4)
     depends_on:
       - syncope
       - keymaster
     image: apache/syncope-console:4.0.5
     ports:
       - "28080:8080"
     restart: always
     environment:
       SPRING_PROFILES_ACTIVE: docker,saml2
       KEYMASTER_ADDRESS: keymaster:2181
       KEYMASTER_USERNAME: ${KEYMASTER_USERNAME:-}
       KEYMASTER_PASSWORD: ${KEYMASTER_PASSWORD:-}
       SERVICE_DISCOVERY_ADDRESS: https://syncope-console:8080/syncope-console/
       ANONYMOUS_USER: ${ANONYMOUS_USER}
       ANONYMOUS_KEY: ${ANONYMOUS_KEY}

   syncope-enduser: (5)
     depends_on:
       - syncope
       - keymaster
     image: apache/syncope-enduser:4.0.5
     ports:
       - "38080:8080"
     restart: always
     environment:
       SPRING_PROFILES_ACTIVE: docker,saml2
       KEYMASTER_ADDRESS: keymaster:2181
       KEYMASTER_USERNAME: ${KEYMASTER_USERNAME:-}
       KEYMASTER_PASSWORD: ${KEYMASTER_PASSWORD:-}
       SERVICE_DISCOVERY_ADDRESS: https://syncope-enduser:8080/syncope-enduser/
       ANONYMOUS_USER: ${ANONYMOUS_USER}
       ANONYMOUS_KEY: ${ANONYMOUS_KEY}

   syncope-wa: (6)
     depends_on:
       - syncope
       - keymaster
     image: apache/syncope-wa:4.0.5
     ports:
       - "48080:8080"
     restart: always
     environment:
       SPRING_PROFILES_ACTIVE: docker,saml2
       KEYMASTER_ADDRESS: keymaster:2181
       KEYMASTER_USERNAME: ${KEYMASTER_USERNAME:-}
       KEYMASTER_PASSWORD: ${KEYMASTER_PASSWORD:-}
       SERVICE_DISCOVERY_ADDRESS: https://syncope-wa:8080/syncope-wa/
       CAS_SERVER_NAME: http://localhost:48080
       ANONYMOUS_USER: ${ANONYMOUS_USER}
       ANONYMOUS_KEY: ${ANONYMOUS_KEY}

   syncope-sra: (7)
     depends_on:
       - syncope
       - keymaster
     image: apache/syncope-sra:4.0.5
     ports:
       - "58080:8080"
     restart: always
     environment:
       SPRING_PROFILES_ACTIVE: docker,saml2
       KEYMASTER_ADDRESS: keymaster:2181
       KEYMASTER_USERNAME: ${KEYMASTER_USERNAME:-}
       KEYMASTER_PASSWORD: ${KEYMASTER_PASSWORD:-}
       SERVICE_DISCOVERY_ADDRESS: https://syncope-sra:8080/
       ANONYMOUS_USER: ${ANONYMOUS_USER}
       ANONYMOUS_KEY: ${ANONYMOUS_KEY}
```

| 1 | Apache Syncope Keymaster, based onApache Zookeeper |
| --- | --- |
| 2 | Database container for usage as internal storage, based on latest PostgreSQL image available |
| 3 | Apache Syncope Core, single instance, port18080exposed |
| 4 | Apache Syncope Admin UI, port28080exposed |
| 5 | Apache Syncope Enduser UI, port38080exposed |
| 6 | Apache Syncope WA, port48080exposed |
| 7 | Apache Syncope SRA, port58080exposed |

##### [How to start the containers](#syncope-apache-org-docs-4-0-getting-started--how-to-start-the-containers)

1. Save the example file locally.
2. Download and start the containers:

   ```bash
   $ SYNCOPE_VERSION=4.0.5 \
   ANONYMOUS_USER=anonymous \
   ANONYMOUS_KEY=anonymousKey \
   KEYMASTER_USERNAME=anonymous \
   KEYMASTER_PASSWORD=anonymousKey \
   docker compose -f /path/to/docker-compose.yml up
   ```

The following services will be available:

| REST API reference | http://localhost:18080/syncope/ |
| --- | --- |
| Admin UI | http://localhost:28080/syncope-console/Credentials:admin/password |
| End-user UI | http://localhost:38080/syncope-enduser/ |
| WA (only with Example 2) | http://localhost:48080/syncope-wa/ |
| SRA (only with Example 2) | http://localhost:58080/ |

#### [3.2.3. Kubernetes sample](#syncope-apache-org-docs-4-0-getting-started--kubernetes-sample)

A set of example [Helm](https://www.helm.sh/) charts is
[available](https://github.com/apache/syncope/tree/syncope-4.0.5/docker/src/main/resources/kubernetes),
that can be used to install Apache Syncope directly in Kubernetes.

Some assumptions are made:

- a working Kubernetes Cluster to install into - if not available, follow this
  [tutorial](https://kubernetes.io/docs/setup/)

  |  | Any other cloud provider or local install (e.g. AWS, Minikube, OpenShift) can be used |
  | --- | --- |
- Helm installed - follow these [instructions](https://docs.helm.sh/using_helm/) if you don’t
- allow for [dynamic provisioning](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/) of persistent
  volumes - otherwise you will need to manually create the volume

The install process is broken into two separate Helm charts; this is due to the fact that Apache Syncope doesn’t startup
properly if the database used as internal storage is not fully initialized yet:

- `postgres` chart; this will install the PostgreSQL database and configure a persistent volume and persistent volume
  claim to store the data
- `syncope` chart; this is the actual Apache Syncope install, which will deploy three separate pods
  (Core, Console, and Enduser)

![SyncopeLayoutInK8s](syncope.apache.org/docs/4.0/images/SyncopeLayoutInK8s.png)

The installation steps are:

1. Open a terminal and navigate to the `kubernetes`
   [folder](https://github.com/apache/syncope/tree/syncope-4.0.5/docker/src/main/resources/kubernetes),
   wherever you downloaded it
2. Set your actual values in `postgres/values.yaml`
3. Install PostgreSQL

   ```bash
   helm install postgres --name postgres --namespace <YOUR_NAMESPACE> -f postgres/values.yaml
   ```

   Wait until PostgreSQL is initialized (watch logs for confirmation)
4. Set your actual values in `syncope/values.yaml`
5. Install Apache Syncope

   ```bash
   helm install syncope --name syncope --namespace <YOUR_NAMESPACE> -f syncope/values.yaml
   ```

### [3.3. Maven Project](#syncope-apache-org-docs-4-0-getting-started--maven-project)

This is the **preferred method** for working with Apache Syncope, giving access to the whole set of customization
and extension capabilities.

|  | Target AudienceProvides access to the full capabilities of Apache Syncope, and almost all extensions that are possible.Requires Apache Maven (and potentiallyDevOps) skills. |
| --- | --- |

#### [3.3.1. Prerequisites](#syncope-apache-org-docs-4-0-getting-started--maven-prerequisites)

1. [Apache Maven](http://maven.apache.org/) (version 3.9.5 or higher) installed
2. Some basic knowledge about Maven
3. Some basic knowledge about [Maven archetypes](http://maven.apache.org/guides/introduction/introduction-to-archetypes.html).

#### [3.3.2. Create project](#syncope-apache-org-docs-4-0-getting-started--create-project)

Maven archetypes are templates of projects. Maven can generate a new project from such a template.
In the folder in which the new project folder should be created, type the command shown below.
On Windows, run the command on a single line and leave out the line continuation characters ('\').

```bash
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.syncope \
    -DarchetypeArtifactId=syncope-archetype \
    -DarchetypeRepository=https://repo1.maven.org/maven2 \
    -DarchetypeVersion=4.0.5
```

The archetype is configured with default values for all required properties; if you want to customize any of these
property values, type 'n' when prompted for confirmation.

You will be asked for:

- groupId

  something like 'com.mycompany'
- artifactId

  something like 'myproject'
- version number

  You can use the default; it is good practice to have 'SNAPSHOT' in the version number during development and the
  maven release plugin makes use of that string. But ensure to comply with the desired numbering scheme for your project.
- package name

  The java package name. A folder structure according to this name will be generated automatically; by default, equal
  to the groupId.
- secretKey

  Provide any pseudo-random string here that will be used in the generated project for AES ciphering.
- anonymousKey

  Provide any pseudo-random string here that will be used as an authentication key for anonymous requests.

Maven will create a project for you (in a newly created directory named after the value of the `artifactId` property
specified above) containing seven modules: `common`, `core`, `console`, `enduser`, `wa`, `sra` and `fit`.

You are now able to perform the first build via

```bash
$ mvn clean install
```

After downloading all of the needed dependencies, the following artifacts will be produced:

1. `core/target/syncope.war`
2. `console/target/syncope-console.war`
3. `enduser/target/syncope-enduser.war`
4. `wa/target/syncope-wa.war`
5. `sra/target/syncope-sra.jar`

If no failures are encountered, your basic Apache Syncope project is now ready to go.

|  | Before actual deployment as executable or onto a Jakarta EE container, you need to further check theCustomizationchapter of theApache Syncope Reference Guide. |
| --- | --- |

#### [3.3.3. Embedded Mode](#syncope-apache-org-docs-4-0-getting-started--embedded-mode)

Every Apache Syncope project has the ability to run a full-blown in-memory environment, particularly useful either when
evaluating the product and during the development phase of an IdM solution.

|  | Don’t forget that this environment is completely in-memory: this means that every time Maven is stopped, all changes made are lost. |
| --- | --- |

From the top-level directory of your project, execute:

```bash
$ mvn -P all clean install
```

|  | The switch-P allis used here in order to build with all extensions available, with paths and settings configured for the embedded mode.When building for production, instead, it is recommended to check theCustomizationchapter of theApache Syncope Reference Guide. |
| --- | --- |

then, from the `fit` subdirectory, execute:

```bash
$ mvn -P embedded,all
```

##### [Paths and Components](#syncope-apache-org-docs-4-0-getting-started--paths-and-components)

|  | While accessing some of the URLs below, your browser will warn that the presented TLS certificate is invalid: it is safe to just ignore the message, take the risk and discover how deep does the rabbit hole go. Not for production, of course. |
| --- | --- |

| Log files | Available undercore/target/log,console/target/log,enduser/target/log,wa/target/logandsra/target/log |
| --- | --- |
| ConnId bundles | Available undercore/target/bundles |
| REST API reference | https://localhost:9443/syncope/ |
| Admini UI | https://localhost:9443/syncope-console/Credentials:admin/password |
| End-user UI | https://localhost:9443/syncope-enduser/ |
| WA | https://localhost:9443/syncope-wa/ |
| SRA | http://localhost:8080/ |
| Internal storage | jdbc:postgresql://localhost:5432/syncope?stringtype=unspecifiedCredentials:syncope/syncope |
| External resource: LDAP | An embedded instance is available.You can configure any LDAP client (such asJXplorer, for example) with the following information:host:localhostport:1389base DN:o=ispbind DN:uid=admin,ou=systembind password:secret |
| External resource: SOAP and REST | Example SOAP and REST services are available athttps://localhost:9443/syncope-fit-build-tools/cxf/ |
| External resource: database | H2TCP database is available.A SQL web interface is available athttp://localhost:9082/Choose configuration 'Generic H2 (Server)'Insertjdbc:h2:tcp://localhost:9092/mem:testdbas JDBC URLSetsaas passwordClick 'Connect' button |
| External resource: Apache Kafka | Broker listening at localhost:19092 |

#### [3.3.4. Docker Mode](#syncope-apache-org-docs-4-0-getting-started--docker-mode)

It is possible to build and run projects generated from Maven archetype by configuring and extending the published
[Docker images](#syncope-apache-org-docs-4-0-getting-started--docker-images).

From the top-level directory of your project, execute:

```bash
$ mvn -P docker,all clean install
```

then, from the `fit` subdirectory, execute:

```bash
$ mvn -P docker
```

|  | The settings shown infit/pom.xmlunder thedockerprofile can be taken as reference to orchestrate actual deployments. |
| --- | --- |

##### [Paths and Components](#syncope-apache-org-docs-4-0-getting-started--paths-and-components-2)

|  | While accessing some of the URLs below, your browser will warn that the presented TLS certificate is invalid: it is safe to just ignore the message, take the risk and discover how deep does the rabbit hole go. Not for production, of course. |
| --- | --- |

|  | The hostnames below, e.g.syncopesyncope-consolesyncope-endusersyncope-sraare to be manually resolved to their respective local IP addresses in use by your current deployment.For example:$ docker inspect -f \   '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \   syncopewill return the actual IP address assigned to thesyncopecontainer. |
| --- | --- |

The following services will be available:

| REST API reference | http://syncope:8080/syncope/ |
| --- | --- |
| Admin UI | http://syncope-console:8080/syncope-console/Credentials:admin/password |
| End-user UI | http://syncope-enduser:8080/syncope-enduser/ |
| WA | https://localhost:9443/syncope-wa/ |
| SRA | http://syncope-sra:8080/ |

## [4. Moving Forward](#syncope-apache-org-docs-4-0-getting-started--moving-forward)

Once you have obtained a working installation of Apache Syncope using one of the methods reported above, you should consider
reading the
[Apache Syncope Reference Guide.](#syncope-apache-org-docs-4-0-reference-guide)
to understand how to configure, extend, customize and deploy your new Apache Syncope project.

Before deploying your Apache Syncope installation into production, it is essential to ensure that the default values for
various security properties have been changed to values specific to your deployment.

The following values must be changed from the defaults in the `core.properties` file:

- **adminPassword** - The cleartext password as encoded per the `adminPasswordAlgorithm` value (`SSHA256` by default), the
  default value of which is "password".
- **secretKey** - The secret key value used for AES ciphering; AES is used by the use cases below:

  - if the value for `adminPasswordAlgorithm` is `AES` or the configuration parameter `password.cipher.algorithm` is
    changed to `AES`
  - if set for Encrypted Plain Schema instances
  - for Linked Accounts' password values
  - to securely store Access Token’s cached authorities
  - within some of the predefined rules used by Password Policies
- **anonymousKey** - The key value to use for anonymous requests.
- **jwsKey** - The symmetric signing key used to sign access tokens. See section 4.4.1 "REST Authentication and
  Authorization" of the Reference Guide for more information.

Note that if you installed Syncope using the maven archetype method, then you will have already supplied custom values
for `secretKey`, `anonymousKey` and `jwsKey`.

---

<a id="syncope-apache-org-docs-4-0-reference-guide"></a>

# Apache Syncope 4.0.5 - Reference Guide

![Apache Syncope logo](syncope.apache.org/docs/4.0/images/apache-syncope-logo-small.jpg)

|  | This document is under active development and discussion!If you find errors or omissions in this document, please don’t hesitate tosubmit an issueoropen a pull requestwith a fix. We also encourage you to ask questions and discuss any aspects of the project on themailing lists or IRC. New contributors are always welcome! |
| --- | --- |

## Preface

This guide covers Apache Syncope services for:

- identity management, provisioning and compliance;
- access management, single sign-on, authentication and authorization;
- API gateway, secure proxy, service mesh, request routing.

## [1. Introduction](#syncope-apache-org-docs-4-0-reference-guide--introduction)

**Apache Syncope** is an Open Source system for managing digital identities in enterprise environments, implemented in
Jakarta EE technology and released under the Apache 2.0 license.

Often, *Identity Management* and *Access Management* are jointly referred, mainly because their two management worlds
likely coexist in the same project or in the same environment.

The two topics are however completely different: each one has its own context, its own rules, its own best practices.

On the other hand, some products provide unorthodox implementations so it is indeed possible to do the same thing with
both of them.

- Identity Management

  Tools and practices to keep identity data consistent and synchronized across repositories, data
  formats and models.
- Access Management

  Systems, protocols and technologies supporting user authentication (how Users are let accessing a
  given system) and authorization (which capabilities each user owns on a given system).

From the definitions above, Identity Management and Access Management can be seen as complementary: very often, the data
synchronized by the former are then used by the latter to provide its features - e.g. authentication and authorization.

### [1.1. Identity Technologies](#syncope-apache-org-docs-4-0-reference-guide--identity-technologies)

Identity and Access Management (IAM) is not implemented by a single technology; it is instead a composition of
heterogeneous technologies - differing by maturity, scope, applicability and feature coverage - which require some
'glue' to fit together.

As with other application domains, it can be observed that tools that appeared earlier tend to partially overlap with more
recent, targeted products.

#### [1.1.1. Identity Stores](#syncope-apache-org-docs-4-0-reference-guide--identity-stores)

*Identity Stores* are the places where identity-related information is stored.

An Identity Store can be shared among several systems: as a result, there is a single place where account data is
managed by administrators, and the same password can be used for the same user for accessing different applications.

Various Identity Store types are available:

- Flat files (XML, CSV, …​)
- LDAP
- Relational databases (MySQL, Oracle, Microsoft SQL Server, PostgreSQL, …​)
- Platform-specific (Microsoft Active Directory, FreeIPA, PowerShell, …​)
- Web services (REST, SOAP, …​)
- Cloud providers
- …​and much more.

![Apache Syncope and the external world](syncope.apache.org/docs/4.0/images/theExternalWorld.jpg)

Figure 1. Apache Syncope and the external world

ConnId

Apache Syncope relies on [ConnId](http://connid.tirasa.net) for communication with Identity Stores; ConnId is designed to
separate the implementation of an application from the dependencies of the system that the application is attempting to
connect to.

ConnId is the continuation of The Identity Connectors Framework (Sun ICF), a project that used to be part of market
leader Sun IdM and has since been released by Sun Microsystems as an Open Source project. This makes the connectors layer
particularly reliable because most connectors have already been implemented in the framework and widely tested.

The new ConnId project, featuring contributors from several companies, provides all that is required nowadays for a
modern Open Source project, including an Apache Maven driven build, artifacts and mailing lists. Additional connectors –
such as for SOAP, CSV, PowerShell and Active Directory – are also provided.

|  | Aren’t Identity Stores enough?One might suppose that a single Identity Store can solve all the identity needs inside an organization, but there are a few drawbacks with this approach:Heterogeneity of systemsLack of a single source of information (HR for corporate id, Groupware for mail address, …​)Often applications require a local user databaseInconsistent policies across the infrastructureLack of workflow managementHidden infrastructure management cost, growing with the size of the organization |
| --- | --- |

#### [1.1.2. Identity Managers](#syncope-apache-org-docs-4-0-reference-guide--identity-managers)

The main role of *Identity Managers* is to keep Identity Stores synchronized as much as possible.

Some other characteristics and features provided:

- Adapt to Identity Store data and application models
- Do not require changes in Identity Stores or applications
- Build virtual unified view of identity data distributed across several Identity Stores
- Allow to define and enforce security policies
- Permit workflow definition, with transitions subject to approval
- Focused on application back-end

In brief, Identity Managers take heterogeneous Identity Stores (and business requirements) as input and build up
high-level identity data management throughout what is called the **Identity Lifecycle**.

![Identity Lifecycle](syncope.apache.org/docs/4.0/images/identityLifecycle.png)

Figure 2. Identity Lifecycle

|  | Applications can typically integrate with Identity Managers by:exposing some sort of provisioning API (often via REST or SOAP) being invoked by Identity Managers - also callednative integration;having their identity repository externally managed by Identity Managers - also calledlegacy integration. |
| --- | --- |

#### [1.1.3. Access Managers](#syncope-apache-org-docs-4-0-reference-guide--access-managers)

*Access Managers* focus on the application front-end, enforcing application access via authentication
(how users are let access a given system) and authorization (which capabilities each user owns on a given system).

Several practices and standards can be implemented by Access Managers:

- [Single Sign-On](https://en.wikipedia.org/wiki/Single_sign-on)
- [Multi-Factor Authentication](https://en.wikipedia.org/wiki/Multi-factor_authentication)
- [OAuth](https://oauth.net/)
- [SAML](https://en.wikipedia.org/wiki/Security_Assertion_Markup_Language)
- [OpenID Connect](https://openid.net/connect/)

|  | Applications can typically integrate with Access Managers by:implementing at least one of the most diffuse protocols as OpenID Connect or SAML - also callednative integration;being protected by a security-enabled HTTP reverse proxy, which will in turn interact with Access Managers - also calledlegacy integration. |
| --- | --- |

#### [1.1.4. The Complete Picture](#syncope-apache-org-docs-4-0-reference-guide--the-complete-picture)

The picture below shows a typical scenario where an organization’s infrastructure is helped by identity technologies in
providing secure and trusted application access to end-Users, while keeping different levels of data and processes under
control for business owners, help-desk operators and system administrators.

![Identity Technologies - The Complete Picture](syncope.apache.org/docs/4.0/images/iam-scenario.png)

Figure 3. Identity Technologies - The Complete Picture

## [2. Architecture](#syncope-apache-org-docs-4-0-reference-guide--architecture)

Apache Syncope is made of several components, which are logically summarized in the picture below.

![Architecture](syncope.apache.org/docs/4.0/images/architecture.png)

Figure 4. Architecture

### [2.1. Keymaster](#syncope-apache-org-docs-4-0-reference-guide--keymaster)

The ***Keymaster*** allows for dynamic service discovery so that other components are able to find each other.  
On startup, all other component instances will register themselves into Keymaster so that their references
can be found later, for intra-component communication.

In addition, the Keymaster is also used as key / value store for [configuration parameters](#syncope-apache-org-docs-4-0-reference-guide--configuration-parameters)
and as a directory for defined [domains](#syncope-apache-org-docs-4-0-reference-guide--domains).

Two different implementations are provided, following the actual needs:

1. as an additional set of RESTful services exposed by the Core, for traditional deployments
   (also known as *Self Keymaster*);
2. as a separate container / pod based on [Apache Zookeeper](https://zookeeper.apache.org/), for microservice-oriented
   deployments.

### [2.2. Core](#syncope-apache-org-docs-4-0-reference-guide--core)

The ***Core*** is the component providing IdM services and acting as central repository for other components' configuration.

The Core is internally further structured into several layers, each one taking care of specific aspects of the identity
management services.

#### [2.2.1. REST](#syncope-apache-org-docs-4-0-reference-guide--rest)

The primary way to consume Core services is the [RESTful](https://en.wikipedia.org/wiki/Representational_state_transfer)
interface, which enables full access to all the features provided.
This interface enables third-party applications, written in any programming language, to consume IdM services.

The rich pre-defined set of endpoints can be [extended](#syncope-apache-org-docs-4-0-reference-guide--extensions) by adding new ones, which might be needed on a
given Apache Syncope deployment to complement the native features with domain-specific operations.

At a technical level, the RESTful interface is a fully-compliant
[Jakarta RESTful Web Services 3.1](https://en.wikipedia.org/wiki/Jakarta_RESTful_Web_Services) implementation based on
[Apache CXF](http://cxf.apache.org), natively dealing either with JSON, YAML and XML payloads.

More details are available in the dedicated [usage](#syncope-apache-org-docs-4-0-reference-guide--core-usage) section.

#### [2.2.2. Logic](#syncope-apache-org-docs-4-0-reference-guide--logic)

Right below the external interface level, the overall business logic is responsible for orchestrating the other layers,
by implementing the operations that can be triggered via REST services. It is also responsible for controlling some
additional features (notifications, reports and auditing).

#### [2.2.3. Provisioning](#syncope-apache-org-docs-4-0-reference-guide--provisioning-layer)

The Provisioning layer is involved with managing the internal (via workflow) and external (via specific connectors)
representation of Users, Groups and Any Objects.

One of the most important features provided is the [mapping](#syncope-apache-org-docs-4-0-reference-guide--mapping) definition: internal data (Users, for example)
representation is correlated with information available on the available Identity Stores.  
Such definitions constitute the pillars of inbound (pull) and outbound (propagation / push)
[provisioning](#syncope-apache-org-docs-4-0-reference-guide--provisioning).

![Internal / External Mapping](syncope.apache.org/docs/4.0/images/mapping.png)

Figure 5. Internal / External Mapping

The default implementation can be sometimes tailored to meet the requirements of a specific deployment, as
it is the crucial decision point for defining and enforcing the consistency and transformations between internal and
external data.

#### [2.2.4. Workflow](#syncope-apache-org-docs-4-0-reference-guide--workflow-layer)

The Workflow layer is responsible for managing the internal lifecycle of Users, Groups and Any Objects.

Besides the default engine, another engine is available based on [Flowable](https://www.flowable.org/), the
reference open source [BPMN 2.0](http://www.bpmn.org/) implementation. It enables advanced features such as approval
management and new statuses definitions; a web-based GUI editor to model workflows and user requests is also available.

![Default Flowable user workflow](syncope.apache.org/docs/4.0/images/userWorkflow.png)

Figure 6. Default Flowable user workflow

Besides Flowable, new workflow engines - possibly integrating with third-party tools as
[Camunda](https://camunda.org/) or [jBPM](http://jbpm.jboss.org/), can be written and plugged into specific deployments.

#### [2.2.5. Persistence](#syncope-apache-org-docs-4-0-reference-guide--persistence)

All data (users, groups, attributes, resources, …​) is internally managed at a high level using a standard
[Jakarta Persistence 3.1](https://en.wikipedia.org/wiki/Jakarta_Persistence) approach based on [Apache OpenJPA](https://openjpa.apache.org).
The data is persisted into an underlying
database, referred to as ***Internal Storage***. Consistency is ensured via the comprehensive
[transaction management](https://docs.spring.io/spring-framework/reference/6.2/data-access/transaction.html)
provided by the Spring Framework.

Globally, this offers the ability to easily scale up to a million entities and at the same time allows great portability
with no code changes: PostgreSQL, MySQL, MariaDB and Oracle are fully supported
[deployment options](#syncope-apache-org-docs-4-0-reference-guide--dbms).

[Domains](#syncope-apache-org-docs-4-0-reference-guide--domains) allow to manage data belonging to different [tenants](https://en.wikipedia.org/wiki/Multitenancy) into
separate database instances.

#### [2.2.6. Security](#syncope-apache-org-docs-4-0-reference-guide--security)

Rather than being a separate layer, Security features are triggered throughout incoming request processing.

A fine-grained set of entitlements is defined which can be granted to administrators, thus enabling the
implementation of [delegated administration](#syncope-apache-org-docs-4-0-reference-guide--delegated-administration) scenarios.

### [2.3. Web Access](#syncope-apache-org-docs-4-0-reference-guide--web-access)

The ***Web Access*** component is based on [Apereo CAS](https://apereo.github.io/cas/).

In addition to all the configuration options and features from Apereo CAS, the Web Access is integrated with Keymaster,
Core and Admin UI to offer centralized configuration and management.

### [2.4. Secure Remote Access](#syncope-apache-org-docs-4-0-reference-guide--secure-remote-access)

The ***Secure Remote Access*** component is built on [Spring Cloud Gateway](https://spring.io/projects/spring-cloud-gateway).

In addition to all the configuration options and features from Spring Cloud Gateway, the Secure Remote Access is
integrated with Keymaster, Core and Admin UI to offer centralized configuration and management.

The Secure Remote Access allows to protect legacy applications by integrating with the Web Access or other third-party
Access Managers implementing standard protocols as OpenID Connect or SAML.

### [2.5. Admin UI](#syncope-apache-org-docs-4-0-reference-guide--admin-console-component)

The ***Admin UI*** is the web-based console for configuring and administering running deployments, with full support
for delegated administration.

The communication between Admin UI and Core is exclusively REST-based.

More details are available in the dedicated [usage](#syncope-apache-org-docs-4-0-reference-guide--admin-console) section.

### [2.6. End-user UI](#syncope-apache-org-docs-4-0-reference-guide--enduser-component)

The ***End-user UI*** is the web-based application for self-registration, self-service and [password reset](#syncope-apache-org-docs-4-0-reference-guide--password-reset).

The communication between End-user UI and Core is exclusively REST-based.

More details are available in the dedicated [usage](#syncope-apache-org-docs-4-0-reference-guide--enduser-application) section.

### [2.7. Third Party Applications](#syncope-apache-org-docs-4-0-reference-guide--third-party-applications)

Third-party applications are provided full access to IdM services by leveraging the REST interface, either via the
Java [Client Library](#syncope-apache-org-docs-4-0-reference-guide--client-library) (the basis of Admin UI and End-user UI) or plain HTTP calls.

## [3. Concepts](#syncope-apache-org-docs-4-0-reference-guide--concepts)

### [3.1. Users, Groups and Any Objects](#syncope-apache-org-docs-4-0-reference-guide--users-groups-and-any-objects)

Users, Groups and Any Objects are definitely the key entities to manage: as explained [above](#syncope-apache-org-docs-4-0-reference-guide--introduction)
in fact, the whole identity management concept is literally about managing identity data.

The following identities are supported:

- **Users** represent the virtual identities build up of account information fragmented across the associated external
  resources
- **Groups** have the dual purpose of representing entities on external resources supporting this concept (say LDAP or
  Active Directory) and putting together Users or Any Objects for implementing group-based provisioning, e.g. to
  dynamically associate Users or Any Objects to external resources
- **Any Objects** actually cover very different entities that can be modeled: printers, services, sensors, …​

For each of the identities above, Apache Syncope is capable of maintaining:

1. `name` (`username`, for Users) - string value uniquely identifying a specific user, group or any object instance;
2. `password` (Users only) - hashed or encrypted value, depending on the selected `password.cipher.algorithm` - see
   [below](#syncope-apache-org-docs-4-0-reference-guide--configuration-parameters) for details - which can be used for authentication;
3. set of attributes, with each attribute being a `(key,values)` pair where

   - `key` is a string label (e.g. `surname`);
   - `values` is a (possibly singleton) collection of data (e.g. `[Doe]` but also
     `[john.doe@syncope.apache.org, jdoe@gmail.com]`)
     ; the type of values that can be assigned to each attribute is defined via the [schema](#syncope-apache-org-docs-4-0-reference-guide--schema) matching the `key`
     value (e.g. *plain* and *derived*);
4. associations with [external resources](#syncope-apache-org-docs-4-0-reference-guide--external-resources), for [provisioning](#syncope-apache-org-docs-4-0-reference-guide--provisioning).

|  | Which schemas can be populated for a given user / group / any object?Each user / group / any object will be able to hold values for all schemas:defined in theAny Type classesassociated to theirAny Type;defined in theAny Type classesconfigured asauxiliaryfor the specific instance. |
| --- | --- |

Moreover, Users and Any Objects can be part of Groups, or associated to other any objects.

|  | Memberships and RelationshipsWhen an user or an any object is assigned to a group, amembershipis defined; the (static) members of a group benefit fromtype extensions.When an user or an any object is associated to another any object, arelationshipis defined, of one of availablerelationship types. |
| --- | --- |

|  | Static and Dynamic MembershipsUsers and Any Objects arestaticallyassigned to Groups when memberships are explicitly set.With group definition, however, a condition can be expressed so that all matching Users and Any Objects aredynamicmembers of the group.Dynamic memberships have some limitations: for example,type extensionsdo not apply; group-based provisioning is still effective. |
| --- | --- |

|  | Security QuestionsThepassword resetprocess can be strengthened by requesting users to provide their configured answer to a given security question, chosen among the ones defined. |
| --- | --- |

### [3.2. Type Management](#syncope-apache-org-docs-4-0-reference-guide--type-management)

In order to manage which attributes can be owned by Users, Groups and any object, and which values can be provided,
Apache Syncope defines a simple yet powerful type management system, vaguely inspired by the LDAP/X.500 information
model.

#### [3.2.1. Schema](#syncope-apache-org-docs-4-0-reference-guide--schema)

A schema instance describes the values that attributes with that schema will hold; it can be defined plain or derived.

It is possible to define i18n labels for each schema, with purpose of improving presentation with Admin and End-user UIs.

##### [Plain](#syncope-apache-org-docs-4-0-reference-guide--plain)

Values for attributes with such schema types are provided during realm, user, group or any object create / update.

When defining a plain schema, the following information must be provided:

- Type

  - `String`
  - `Long` - allows to specify a *conversion pattern* to / from string, according to
    [DecimalFormat](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/text/DecimalFormat.html)
  - `Double` - allows to specify a *conversion pattern* to / from string, according to
    [DecimalFormat](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/text/DecimalFormat.html)
  - `Boolean`
  - `Date` - allows to specify a *conversion pattern* to / from string, according to
    [DateFormat](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/text/DateFormat.html)
  - `Enum` - allows to specify which predetermined value(s) can be selected
  - `Dropdown` - allows to specify an [implementation](#syncope-apache-org-docs-4-0-reference-guide--implementations) which will dynamically return the value(s) that
    can be selected
  - `Encrypted`

    - secret key (stored or referenced as [Spring property](https://docs.spring.io/spring-framework/reference/6.2/core/beans/environment.html#beans-using-propertysource))
    - cipher algorithm
    - whether transparent encryption is to be enabled, e.g. attribute values are stored as encrypted but available as
      cleartext on-demand (requires AES ciphering)
  - `Binary` - it is required to provide the declared mime type
- Validator - (optional) [implementation](#syncope-apache-org-docs-4-0-reference-guide--implementations) validating the value(s) provided for attributes, see
  [EmailAddressValidator](https://github.com/apache/syncope/blob/syncope-4.0.5/core/persistence-common/src/main/java/org/apache/syncope/core/persistence/common/attrvalue/EmailAddressValidator.java)
  for reference
- Mandatory condition - [JEXL](#syncope-apache-org-docs-4-0-reference-guide--jexl) expression indicating whether values for this schema must be necessarily provided
  or not; compared to simple boolean value, such condition allows to express complex statements like 'be mandatory only if
  this other attribute value is above 14', and so on
- Unique constraint - make sure that no duplicate value(s) for this schema are found
- Multivalue flag - whether single or multiple values are supported
- Read-only flag - whether value(s) for this schema are modifiable only via internal code (say workflow tasks) or
  can be instead provided during ordinary [provisioning](#syncope-apache-org-docs-4-0-reference-guide--provisioning)

##### [Derived](#syncope-apache-org-docs-4-0-reference-guide--derived)

Sometimes it is useful to obtain values as arbitrary combinations of other attributes' values: for example, with
`firstname` and `surname` plain schemas, it is natural to think that `fullname` could be somehow defined as the
concatenation of `firstname` 's and `surname` 's values, separated by a blank space.

Derived schemas are always read-only and require a [JEXL](#syncope-apache-org-docs-4-0-reference-guide--jexl) expression to be specified that references plain schema
types.  
For the sample above, it would be

```text
firstname + ' ' + surname
```

With derived attributes, values are not stored into the [internal storage](#syncope-apache-org-docs-4-0-reference-guide--persistence) but calculated on request, by
evaluating the related JEXL expression

#### [3.2.2. AnyTypeClass](#syncope-apache-org-docs-4-0-reference-guide--anytypeclass)

Any type classes are aggregations of plain and derived schemas, provided with unique identifiers.

Classes can be assigned to [any types](#syncope-apache-org-docs-4-0-reference-guide--anytype), [realms](#syncope-apache-org-docs-4-0-reference-guide--realms) and are also available as auxiliary (hence to be
specified on a given user / group / any object instance) and for [type extensions](#syncope-apache-org-docs-4-0-reference-guide--type-extensions).

#### [3.2.3. AnyType](#syncope-apache-org-docs-4-0-reference-guide--anytype)

Any types represent the type of identities that Apache Syncope is able to manage; besides the predefined `USER` and
`GROUP`, more types can be created to model workstations, printers, folders, sensors, services, …​

For all Any Types that are defined, a set of [classes](#syncope-apache-org-docs-4-0-reference-guide--anytypeclass) can be selected so that instances of a given
Any Type will be enabled to populate attributes for schemas in those classes.

Example 1. Any types and attributes allowed for Users, Groups and Any Objects

Assuming that the following schemas are available:

1. plain: `firstname`, `surname`, `email`
2. derived: `fullname`

and that the following Any Type classes are defined:

1. `minimal` - containing `firstname`, `surname` and `fullname`
2. `member` - containing `email` and `enrollment`

and that the `USER` Any Type has only `minimal` assigned, then the following Users are valid (details are simplified to
increase readability):

```json
{
  "key": "74cd8ece-715a-44a4-a736-e17b46c4e7e6",
  "type": "USER",
  "realm": "/",
  "username": "verdi",
  "plainAttrs": [
    {
      "schema": "surname",
      "values": [
        "Verdi"
      ]
    },
    {
      "schema": "firstname",
      "values": [
        "Giuseppe"
      ]
    }
  ],
  "derAttrs": [
    {
      "schema": "fullname",
      "values": [
        "Giuseppe Verdi"
      ]
    }
  ]
}

{
  "key": "1417acbe-cbf6-4277-9372-e75e04f97000",
  "type": "USER",
  "realm": "/",
  "username": "rossini",
  "auxClasses": [ "member" ],
  "plainAttrs": [
    {
      "schema": "surname",
      "values": [
        "Rossini"
      ]
    },
    {
      "schema": "firstname",
      "values": [
        "Gioacchino"
      ]
    },
    {
      "schema": "email",
      "values": [
        "gioacchino.rossini@syncope.apache.org"
      ]
    }
  ],
  "derAttrs": [
    {
      "schema": "fullname",
      "values": [
        "Gioacchino Rossini"
      ]
    }
  ]
}
```

#### [3.2.4. RelationshipType](#syncope-apache-org-docs-4-0-reference-guide--relationshiptype)

Relationships allow the creation of a link between a user, a group or an any object with an any object; relationship
types define the available link types.

Example 2. Relationship between Any Objects (printers)

The following any object of type `PRINTER` contains a relationship of type `neighbourhood` with another `PRINTER`
(details are simplified to increase readability):

```json
{
  "key": "fc6dbc3a-6c07-4965-8781-921e7401a4a5",
  "type": "PRINTER",
  "realm": "/",
  "name": "HP LJ 1300n",
  "auxClasses": [],
  "plainAttrs": [
    {
      "schema": "model",
      "values": [
        "Canon MFC8030"
      ]
    },
    {
      "schema": "location",
      "values": [
        "1st floor"
      ]
    }
  ],
  "relationships": [
    {
      "type": "neighborhood",
      "end": "LEFT",
      "otherEndType": "PRINTER",
      "otherEndKey": "8559d14d-58c2-46eb-a2d4-a7d35161e8f8",
      "otherEndName": "Canon MF 8030cn"
    },
    {
      "type": "neighborhood",
      "end": "RIGHT",
      "otherEndType": "USER",
      "otherEndKey": "c9b2dec2-00a7-4855-97c0-d854842b4b24",
      "otherEndName": "bellini"
    }
  ]
}
```

#### [3.2.5. Type Extensions](#syncope-apache-org-docs-4-0-reference-guide--type-extensions)

When a user (or an any object) is part of a group, a *membership* is defined.

It is sometimes useful to define attributes which are bound to a particular membership: if, for example, the
`University A` and `University B` Groups are available, a student might have different e-mail addresses for each
university. How can this be modeled?

Type extensions define a set of [classes](#syncope-apache-org-docs-4-0-reference-guide--anytypeclass) associated to a group, that can be automatically
assigned to a given user (or any object) when becoming a member of such group.

Example 3. Membership with type extension

With reference to the sample above (details are simplified to increase readability):

```json
{
  "key": "c9b2dec2-00a7-4855-97c0-d854842b4b24",
  "type": "USER",
  "realm": "/",
  "username": "bellini",
  "memberships": [
    {
      "type": "Membership",
      "rightType": "GROUP",
      "rightKey": "bf825fe1-7320-4a54-bd64-143b5c18ab97",
      "groupName": "University A",
      "plainAttrs": [
        {
          "schema": "email",
          "values": [
            "bellini@university_a.net"
          ]
        }
      ]
    },
    {
      "type": "Membership",
      "rightType": "GROUP",
      "rightKey": "bf825fe1-7320-4a54-bd64-143b5c18ab96",
      "groupName": "University B",
      "plainAttrs": [
        {
          "schema": "email",
          "values": [
            "bellini@university_b.net"
          ]
        }
      ]
    }
  ]
}
```

### [3.3. External Resources](#syncope-apache-org-docs-4-0-reference-guide--external-resources)

- Connector Bundles

  The components able to connect to Identity Stores; not specifically bound to Apache Syncope,
  as they are part of the [ConnId](http://connid.tirasa.net) project.
- Connector Instances

  Instances of connector bundles, obtained by assigning values to the defined configuration
  properties. For instance, there is only a single `DatabaseTable` (the bundle) that can be instantiated
  several times, for example if there is a need to connect to different databases.
- External Resources

  Meant to encapsulate all information about how Apache Syncope will use connector instances for
  provisioning. For each entity supported by the related connector bundle (user, group, printer, services, …​),
  [mapping](#syncope-apache-org-docs-4-0-reference-guide--mapping) information can be specified.

#### [3.3.1. Connector Bundles](#syncope-apache-org-docs-4-0-reference-guide--connector-bundles)

Several Connector Bundles come included with Apache Syncope:

- [Active Directory](https://connid.atlassian.net/wiki/pages/viewpage.action?pageId=360482)
- [Azure](https://connid.atlassian.net/wiki/display/BASE/Azure)
- [CMD ](https://connid.atlassian.net/wiki/display/BASE/CMD)
- [CSV Directory](https://connid.atlassian.net/wiki/display/BASE/CSV+Directory)
- [Database](https://connid.atlassian.net/wiki/display/BASE/Database)
- [Google Apps](https://connid.atlassian.net/wiki/display/BASE/Google+Apps)
- [Apache Kafka](https://connid.atlassian.net/wiki/display/BASE/Kafka)
- [LDAP](https://connid.atlassian.net/wiki/display/BASE/LDAP)
- [Okta](https://connid.atlassian.net/wiki/display/BASE/Okta)
- [Scripted REST](https://connid.atlassian.net/wiki/display/BASE/REST)
- [ServiceNow](https://connid.atlassian.net/wiki/display/BASE/ServiceNow)
- [SCIM](https://connid.atlassian.net/wiki/display/BASE/SCIM)
- [SOAP](https://connid.atlassian.net/wiki/display/BASE/SOAP)

More Connector Bundles can be [installed](#syncope-apache-org-docs-4-0-reference-guide--install-connector-bundles), if needed.

#### [3.3.2. Connector Instance details](#syncope-apache-org-docs-4-0-reference-guide--connector-instance-details)

When defining a connector instance, the following information must be provided:

- administration realm - the [Realm](#syncope-apache-org-docs-4-0-reference-guide--realms) under which administrators need to own [entitlements](#syncope-apache-org-docs-4-0-reference-guide--entitlements) in
  order to be allowed to manage this connector and all related external resources
- connector bundle - one of the several
  [already available](https://github.com/Tirasa/ConnId/blob/4_0_X/README.md#available-connectors), or some to be
  [made from scratch](https://connid.atlassian.net/wiki/display/BASE/Create+new+connector), in order to fulfill specific
  requirements
- pooling information
- configuration - depending on the selected bundle, these are properties with configuration values: for example,
  with [LDAP](https://connid.atlassian.net/wiki/display/BASE/LDAP#LDAP-Configuration) this means host, port, bind DN,
  object classes while with
  [DBMS](https://connid.atlassian.net/wiki/display/BASE/Database+Table#DatabaseTable-ConfigurationProperties) it would
  be JDBC URL, table name, etc.
- capabilities - define what operations are allowed on this connector: during [provisioning](#syncope-apache-org-docs-4-0-reference-guide--provisioning), if a
  certain operation is invoked but the corresponding capability is not set on the related connector instance, no actual
  action is performed on the underlying connector; the capabilities are:

  - `AUTHENTICATE` - consent to [pass-through authentication](#syncope-apache-org-docs-4-0-reference-guide--pass-through-authentication)
  - `CREATE` - create objects on the underlying connector
  - `UPDATE` - update objects on the underlying connector
  - `DELETE` - delete objects on the underlying connector
  - `SEARCH` - search / read objects from the underlying connector; used during [pull](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull) with
    `FULL RECONCILIATION` or `FILTERED RECONCILIATION` [mode](#syncope-apache-org-docs-4-0-reference-guide--pull-mode)
  - `SYNC` - synchronize objects from the underlying connector; used during [pull](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull) with
    `INCREMENTAL` [mode](#syncope-apache-org-docs-4-0-reference-guide--pull-mode)
  - `LIVE_SYNC` - synchronize objects by subscribing to queue systems; used during [live sync](#syncope-apache-org-docs-4-0-reference-guide--provisioning-livesync)

|  | Configuration and capability overrideCapabilities and individual configuration properties can be set foroverride: in this case, all the external resources using the given connector instance will have the chance to override some configuration values, or the capabilities set.This can be useful when the same connector instance is shared among different resources, with little difference in the required configuration or capabilities. |
| --- | --- |

#### [3.3.3. External Resource details](#syncope-apache-org-docs-4-0-reference-guide--external-resource-details)

Given a selected connector instance, the following information is required to define an external resource:

- priority - integer value, in use by the default [propagation task executor](#syncope-apache-org-docs-4-0-reference-guide--propagation)
- propagation actions - which [actions](#syncope-apache-org-docs-4-0-reference-guide--propagationactions) shall be executed during propagation
- trace levels - control how much tracing (including logs and execution details) shall be carried over during
  [propagation](#syncope-apache-org-docs-4-0-reference-guide--propagation), [pull](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull), [live sync](#syncope-apache-org-docs-4-0-reference-guide--provisioning-livesync) and
  [push](#syncope-apache-org-docs-4-0-reference-guide--provisioning-push)
- [configuration](#syncope-apache-org-docs-4-0-reference-guide--connector-instance-details)
- [capabilities](#syncope-apache-org-docs-4-0-reference-guide--connector-instance-details)
- [account policy](#syncope-apache-org-docs-4-0-reference-guide--policies-account) to enforce on Users, Groups and Any Objects assigned to
  this external resource
- [password policy](#syncope-apache-org-docs-4-0-reference-guide--policies-password) to enforce on Users, Groups and Any Objects assigned to
  this external resource
- [propagation policy](#syncope-apache-org-docs-4-0-reference-guide--policies-propagation) to apply during [propagation](#syncope-apache-org-docs-4-0-reference-guide--propagation) on this external resource
- [inbound policy](#syncope-apache-org-docs-4-0-reference-guide--policies-inbound) to apply during [pull](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull) or
  [live sync](#syncope-apache-org-docs-4-0-reference-guide--provisioning-livesync) on this external resource
- [push policy](#syncope-apache-org-docs-4-0-reference-guide--policies-push) to apply during [push](#syncope-apache-org-docs-4-0-reference-guide--provisioning-push) on this external resource

#### [3.3.4. Mapping](#syncope-apache-org-docs-4-0-reference-guide--mapping)

The mapping between internal and external data is of crucial importance when
configuring an external resource. Such information, in fact, plays a key role for [provisioning](#syncope-apache-org-docs-4-0-reference-guide--provisioning).

![Sample mapping](syncope.apache.org/docs/4.0/images/mapping.png)

Figure 7. Sample mapping

For each of the [any types](#syncope-apache-org-docs-4-0-reference-guide--anytype) supported by the underlying connector, a different mapping is provided.

A mapping is essentially a collection of *mapping items* describing the correspondence between an user / group / any
object attribute and its counterpart on the Identity Store represented by the current external resource. Each item
specifies:

- internal attribute - the [schema](#syncope-apache-org-docs-4-0-reference-guide--schema) acting as the source or destination of provisioning operations; it must be
  specified by an expression matching one of the following models:

  - `schema` - resolves to the attribute for the given `schema`, owned by the mapped entity (user, group, any object)
  - `groups[groupName].schema` - resolves to the attribute for the given `schema`, owned by the group with name
    `groupName`, if a membership for the mapped entity exists
  - `users[userName].schema` - resolves to the attribute for the given `schema`, owned by the user with name
    `userName`, if a relationship with the mapped entity exists
  - `anyObjects[anyObjectName].schema` - resolves to the attribute for the given `schema`, owned by the any object with
    name `anyObjectName`, if a relationship with the mapped entity exists
  - `relationships[relationshipType][relationshipAnyType].schema` - resolves to the attribute for the given `schema`,
    owned by the any object of type `relationshipAnyType`, if a relationship of type `relationshipType` with the mapped entity exists
  - `memberships[groupName].schema` - resolves to the attribute for the given `schema`, owned by the membership for group
    `groupName` of the mapped entity (user, any object), if such a membership exists
- external attribute - the name of the attribute on the Identity Store
- transformers - [JEXL](#syncope-apache-org-docs-4-0-reference-guide--jexl) expression or Java class implementing
  [ItemTransformer](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-api/src/main/java/org/apache/syncope/core/provisioning/api/data/ItemTransformer.java)
  ; the purpose is to transform values before they are sent to or received from the underlying connector
- mandatory condition - [JEXL](#syncope-apache-org-docs-4-0-reference-guide--jexl) expression indicating whether values for this mapping item must be necessarily
  available or not; compared to a simple boolean value, such condition allows complex statements to be expressed such as
  'be mandatory only if this other attribute value is above 14', and so on
- remote key flag - should this item be considered as the key value on the Identity Store, if no
  [inbound](#syncope-apache-org-docs-4-0-reference-guide--inbound-correlation-rules) or [push](#syncope-apache-org-docs-4-0-reference-guide--push-correlation-rules) correlation rules are applicable?
- password flag (Users only) - should this item be treated as the password value?
- purpose - should this item be considered for [propagation](#syncope-apache-org-docs-4-0-reference-guide--propagation) / [push](#syncope-apache-org-docs-4-0-reference-guide--provisioning-push),
  [pull](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull), both or none?

Besides the items documented above, some more data needs to be specified for a complete mapping:

- which
  [object class](http://connid.tirasa.net/apidocs/1.6/org/identityconnectors/framework/common/objects/ObjectClass.html)
  shall be used during communication with the Identity Store; predefined are `__ACCOUNT__` for Users and
  `__GROUP__` for Groups
- whether matches between user / group / any object’s attribute values and their counterparts on the Identity Store
  should be performed in a case-sensitive fashion or not
- which schema shall be used to hold values for identifiers generated upon create by the Identity Store - required by
  some cloud providers not accepting provided values as unique references
- the model for generating the DN (distinguished name) values - only required by some connector bundles as
  [LDAP](https://connid.atlassian.net/wiki/display/BASE/LDAP) and
  [Active Directory](https://connid.atlassian.net/wiki/pages/viewpage.action?pageId=360482)

Example 4. Mapping items

The following mapping item binds the mandatory internal `name` schema with the external attribute `cn` for both
propagation / push and pull.

```json
{
  "key": "a2bf43c8-74cb-4250-92cf-fb8889409ac1",
  "intAttrName": "name",
  "extAttrName": "cn",
  "connObjectKey": true,
  "password": false,
  "mandatoryCondition": "true",
  "purpose": "BOTH"
}
```

The following mapping item binds the optional internal `aLong` schema for the membership of the `additional` group
with the external attribute `age` for propagation / push only; in addition, it specifies a JEXL expression which appends `.0`
to the selected `aLong` value before sending it out to the underlying connector.

```json
{
  "key": "9dde8bd5-f158-499e-9d81-3d7fcf9ea1e8",
  "intAttrName": "memberships[additional].aLong",
  "extAttrName": "age",
  "connObjectKey": false,
  "password": false,
  "mandatoryCondition": "false",
  "purpose": "PROPAGATION",
  "propagationJEXLTransformer": "value + '.0'"
}
```

|  | Object link and Realms hierarchyWhen Object link is applicable - typically with LDAP or Active Directory, as said - the need may arise to map the Realms hierarchy into nested structures, asOrganizational Units.In such cases, the following JEXL expressions can be set for Object link (assumingo=ispis the root suffix), for example, which leverage thesyncope:fullPath2Dn()custom JEXL function:Realms:syncope:fullPath2Dn(fullPath, 'ou') + ',o=isp'Users:'uid=' + name + syncope:fullPath2Dn(realm, 'ou', ',') + ',o=isp'Groups:'cn=' + name + syncope:fullPath2Dn(realm, 'ou', ',') + ',o=isp' |
| --- | --- |

#### [3.3.5. Linked Accounts](#syncope-apache-org-docs-4-0-reference-guide--linked-accounts)

Sometimes the information provided by the [mapping](#syncope-apache-org-docs-4-0-reference-guide--mapping) is not enough to define a one-to-one correspondence
between Users / Groups / Any Objects and objects on External Resources.

There can be many reasons for this situation, including existence of so-called *service accounts* (typical with LDAP or
Active Directory), or simply the uncomfortable reality that system integrators keep encountering when legacy systems
are to be enrolled into a brand new IAM system.

Users can have, on a given External Resource with `USER` mapping defined:

1. zero or one *mapped account*  
   if the External Resource is assigned either directly or via Group membership.
2. zero or more *linked accounts*  
   as internal representation of objects on the External Resource, defined in terms of username, password and / or plain
   attribute values override, with reference to the owning User.

Linked Accounts are propagated alongside with owning User - following the existing
[push correation rule](#syncope-apache-org-docs-4-0-reference-guide--push-correlation-rules) if available - and pulled according to the given
[inbound correation rule](#syncope-apache-org-docs-4-0-reference-guide--inbound-correlation-rules), if present.

![Linked Accounts](syncope.apache.org/docs/4.0/images/linked_accounts.png)

Figure 8. Linked Accounts

### [3.4. Realms](#syncope-apache-org-docs-4-0-reference-guide--realms)

Realms define a hierarchical security domain tree, primarily meant for containing Users, Groups and
Any Objects.

Each realm:

1. has a unique name and a parent realm - except for the pre-defined *root realm*, which is named `/`;
2. is either a leaf or root of a sub-tree of realms;
3. is uniquely identified by the path from the root realm, e.g. `/a/b/c` identifies the sub-realm `c` in the sub-tree
   rooted at `b`, having in turn `a` as parent realm, directly under the root realm;
4. refers to [any type class(es)](#syncope-apache-org-docs-4-0-reference-guide--anytypeclass) which allow to define plain and derived attributes;
5. optionally refers to various [policies](#syncope-apache-org-docs-4-0-reference-guide--policies) that are enforced on all Users, Groups and Any Objects in the given
   realm and sub-realms, unless some sub-realms define their own policies.
6. optionally refers to [logic action(s)](#syncope-apache-org-docs-4-0-reference-guide--logicactions)
7. optionally defines [entity templates](#syncope-apache-org-docs-4-0-reference-guide--logic-templates)

If Users, Groups and Any Objects are members of a realm then they are also members of the parent realm: as a result, the
root realm contains everything, and other realms can be seen as containers that split up the total number of entities
into smaller pools.

This partition allows fine-grained control over policy enforcement and, alongside with
[entitlements](#syncope-apache-org-docs-4-0-reference-guide--entitlements) and [roles](#syncope-apache-org-docs-4-0-reference-guide--roles), helps to implement
[delegated administration](#syncope-apache-org-docs-4-0-reference-guide--delegated-administration).

Dynamic Realms

Realms provide a means to model static containment hierarchies.  
This might not be the ideal fit for situations where the set of Users, Groups and Any Objects to administer
cannot be statically defined by containment.

Dynamic Realms can be used to identify Users, Groups and Any Objects according to some attributes' value, resource
assignment, group membership or any other condition available, with purpose of granting
[delegated administration](#syncope-apache-org-docs-4-0-reference-guide--delegated-administration) rights.

|  | Logic TemplatesAs withpullit is also possible to add templates to a realm.The values specified in the template are applied to entities belonging to that realm, hence this can be used as a mechanism for setting default values for attributes or external resources on entities.Logic Templates apply to all operations passing through thelogic layer, e.g. triggered by REST requests. |
| --- | --- |

#### [3.4.1. Realm Provisioning](#syncope-apache-org-docs-4-0-reference-guide--realm-provisioning)

[Provisioning](#syncope-apache-org-docs-4-0-reference-guide--provisioning) can be enabled for realms: [mapping](#syncope-apache-org-docs-4-0-reference-guide--mapping) information can be provided so that realms
are considered during [propagation](#syncope-apache-org-docs-4-0-reference-guide--propagation), [pull](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull) and [push](#syncope-apache-org-docs-4-0-reference-guide--provisioning-push) execution.

A typical use case for realm provisioning is to model an organization-like structure on Identity Stores, as
with LDAP and Active Directory.

#### [3.4.2. LogicActions](#syncope-apache-org-docs-4-0-reference-guide--logicactions)

When Users, Groups or Any Objects get created, updated or deleted in a realm, custom logic can be invoked
by associating the given Realm with one or more [implementations](#syncope-apache-org-docs-4-0-reference-guide--implementations) of the
[LogicActions](https://github.com/apache/syncope/blob/syncope-4.0.5/core/idrepo/logic/src/main/java/org/apache/syncope/core/logic/api/LogicActions.java)
interface.

|  | LogicActions apply to all operations passing through thelogic layer, e.g. triggered by REST requests. |
| --- | --- |

### [3.5. Entitlements](#syncope-apache-org-docs-4-0-reference-guide--entitlements)

Entitlements are basically strings describing the right to perform an operation on Syncope.

The components in the [logic layer](#syncope-apache-org-docs-4-0-reference-guide--logic) are annotated with
[Spring Security](https://spring.io/projects/spring-security) to implement declarative security; in the following
code snippet taken from
[RealmLogic](https://github.com/apache/syncope/blob/syncope-4.0.5/core/idrepo/logic/src/main/java/org/apache/syncope/core/logic/RealmLogic.java)
, the
[`hasRole` expression](https://docs.spring.io/spring-security/reference/6.4/servlet/authorization/method-security.html#authorization-expressions)
is used together with one of the standard entitlements to restrict access only to Users owning the `REALM_SEARCH`
entitlement.

```java
@PreAuthorize("hasRole('" + IdRepoEntitlement.REALM_SEARCH + "')")
public List<RealmTO> list(final String fullPath) {
```

Entitlements are granted via [roles](#syncope-apache-org-docs-4-0-reference-guide--roles) to Users, scoped under certain [realms](#syncope-apache-org-docs-4-0-reference-guide--realms), thus allowing
[delegated administration](#syncope-apache-org-docs-4-0-reference-guide--delegated-administration).

|  | The set of available entitlements isstatically defined- even thoughextensionshave the ability toenlarge the initial list: this is because entitlements are the pillars of the internal security model and are not meant for external usage. |
| --- | --- |

### [3.6. Roles](#syncope-apache-org-docs-4-0-reference-guide--roles)

Roles map a set of [entitlements](#syncope-apache-org-docs-4-0-reference-guide--entitlements) to a set of [realms](#syncope-apache-org-docs-4-0-reference-guide--realms) and / or
[dynamic realms](#syncope-apache-org-docs-4-0-reference-guide--dynamic-realms).

|  | Static and Dynamic MembershipsUsers arestaticallyassigned to roles when assignments are explicitly set.However, a condition can be expressed in the role definition so that all matching Users aredynamicmembers of the role. |
| --- | --- |

#### [3.6.1. Delegated Administration](#syncope-apache-org-docs-4-0-reference-guide--delegated-administration)

The idea is that any user U assigned to a role R, which provides entitlements E1…​En for realms Re1…​Rem, can
exercise Ei on entities (Users, Groups, Any Objects of given types - depending on Ei - or Connector Instances and
External Resources) under any Rej or related sub-realms.

Moreover, any user U assigned to a role R, which provides entitlements E1…​En for dynamic realms DR1..DRn, can
exercise Ei on entities (Users, Groups, Any Objects of given types, depending on Ei) matching the conditions defined
for any DRk.

|  | Dynamic Realms limitationsUsers to whom administration rights were granted via Dynamic Realms can onlyupdateUsers, Groups and Any Objects, not create nor delete.Moreover, the only accepted changes on a given entity are the ones that do not change any Dynamic Realm’s matching condition for such entity. |
| --- | --- |

Example 5. Authorization

Let’s suppose that we want to implement the following scenario:

Administrator A can create Users under realm R5 but not under realm R7, administrator B can update users under
realm R6 and R8, administrator C can update Groups under realm R8.

As by default, Apache Syncope will have defined the following entitlements, among others:

- `USER_CREATE`
- `USER_UPDATE`
- `GROUP_UPDATE`

Hence, here is how entitlements should be assigned (via roles) to administrators in order to implement the scenario
above:

- Administrator A: `USER_CREATE` on R5
- Administrator B: `USER_UPDATE` on R6 and R8
- Administrator C: `GROUP_UPDATE` on R8

|  | Delegated Administration via Admin ConsoleWhen administering viaREST, the entitlements to be granted to delegated administrators are straightforward:USER_CREATEfor certainRealmswill allow to create users under such Realms.When using theAdmin Console, instead, more entitlements are generally required: this because the underlying implementation takes care of simplifying the UX as much as possible.For example, the following entitlements are normally required to be granted for user administration, besides the actualUSER_CREATE,USER_UPDATEandUSER_DELETE:USER_SEARCHANYTYPECLASS_READANYTYPE_LISTANYTYPECLASS_LISTRELATIONSHIPTYPE_LISTUSER_READANYTYPE_READREALM_SEARCHGROUP_SEARCH |
| --- | --- |

##### [Group Ownership](#syncope-apache-org-docs-4-0-reference-guide--group-ownership)

Groups can designate a User or another Group as *owner*.

The practical consequence of this setting is that Users owning a Group (either because they are directly set as owners
or members of the owning Group) is that they are entitled to

- perform all operations (create, update, delete, …​) on the owned Group
- perform all operations (create, update, delete, …​) on all User and Any Object members of the owner Group, with
  exception of removing members from the Group itself

regardless of the Realm.

The actual Entitlements are assigned through the predefined `GROUP_OWNER` Role:

1. `USER_SEARCH`
2. `USER_READ`
3. `USER_CREATE`
4. `USER_UPDATE`
5. `USER_DELETE`
6. `ANYTYPECLASS_READ`
7. `ANYTYPE_LIST`
8. `ANYTYPECLASS_LIST`
9. `RELATIONSHIPTYPE_LIST`
10. `ANYTYPE_READ`
11. `REALM_SEARCH`
12. `GROUP_SEARCH`
13. `GROUP_READ`
14. `GROUP_UPDATE`
15. `GROUP_DELETE`

The `GROUP_OWNER` Role can be updated to adjust the set of assigned Entitlements.

#### [3.6.2. Delegation](#syncope-apache-org-docs-4-0-reference-guide--delegation)

With Delegation, any user can delegate other users to perform operations on their behalf.

In order to set up a Delegation, the following information shall be provided:

- delegating User (mandatory) - administrators granted with `DELEGATION_CREATE` Entitlement can create Delegations for
  all defined Users; otherwise, the only accepted value is the User itself;
- delegated User (mandatory) - any User defined, distinct from delegating;
- start (mandatory) - initial timestamp from which the Delegation is considered effective;
- end (optional) - final timestamp after which the Delegation is not considered effective: when not provided, Delegation
  will remain valid unless deleted;
- roles (optional) - set of Roles granted by delegating to delegated User: only Roles owned by delegating can be
  granted, when not provided all owned Roles are considered as part of the Delegation.

|  | Auditevents generated when operating under Delegation will report both delegating and delegated users. |
| --- | --- |

### [3.7. Provisioning](#syncope-apache-org-docs-4-0-reference-guide--provisioning)

As described [above](#syncope-apache-org-docs-4-0-reference-guide--identity-managers), provisioning is actually *the* core feature provided by Apache Syncope.

Essentially, it can be seen as the process of keeping the identity data synchronized between Syncope and related external resources, according to the specifications provided by the [mapping](#syncope-apache-org-docs-4-0-reference-guide--mapping). It does this by performing create, update and
delete operations onto the [internal storage](#syncope-apache-org-docs-4-0-reference-guide--persistence) or external resources via connectors.

#### [3.7.1. Overview](#syncope-apache-org-docs-4-0-reference-guide--overview)

The picture below contains an expanded view of the [core architecture](#syncope-apache-org-docs-4-0-reference-guide--architecture), with particular reference to the
components involved in the provisioning process.

![Provisioning flow](syncope.apache.org/docs/4.0/images/provisioningFlow.png)

Figure 9. Provisioning flow

The provisioning operations can be initiated in several different ways:

- by creating, updating or deleting Users, Groups or Any Objects via REST (thus involving the underlying
  [logic](#syncope-apache-org-docs-4-0-reference-guide--logic) layer)
- by requesting execution of pull or push tasks via REST
- by triggering periodic pull or push task executions

|  | Provisioning ManagersThe provisioning operations are defined by the provisioning manager interfaces:UserProvisioningManagerGroupProvisioningManagerAnyObjectProvisioningManagerDefault implementations are available:DefaultUserProvisioningManagerDefaultGroupProvisioningManagerDefaultAnyObjectProvisioningManager |
| --- | --- |

#### [3.7.2. Propagation](#syncope-apache-org-docs-4-0-reference-guide--propagation)

Whenever a change is performed via REST on Realms, Users, Groups or Any Objects:

1. a set of [propagation tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-propagation) is generated, one for each associated external resource for which the
   [mapping](#syncope-apache-org-docs-4-0-reference-guide--mapping) is defined for the given [any type](#syncope-apache-org-docs-4-0-reference-guide--anytype) or [realm](#syncope-apache-org-docs-4-0-reference-guide--realm-provisioning);
2. the generated propagation tasks are executed, e.g. the corresponding operations (create, update or delete) are sent
   out, via connectors, to the configured Identity Stores, according to the configured
   [propagation policy](#syncope-apache-org-docs-4-0-reference-guide--policies-propagation), if defined; the tasks can be saved for later re-execution.

|  | Which external resources?Depending on the entity being created / updated / deleted, different external resources are taken into account by the propagation process:Group: only the external resources directly assignedUser: the external resources directly assigned plus the ones assigned to Groups configured for the UserAny Object: the external resources directly assigned plus the ones assigned to Groups configured for the Any Object |
| --- | --- |

|  | Adequate capabilities to Connectors and External ResourcesEnsure to provide an adequate set ofcapabilitiesto underlying Connectors and External Resources for the actual operations to perform, otherwise the Propagation Tasks will reportNOT_ATTEMPTEDas execution status. |
| --- | --- |

|  | Propagate password valuesPassword values are kept in the internal storage according to thepassword.cipher.algorithmconfiguration parameter, whose value isSHA1by default.SHA1is a hash algorithm: this means that, once stored, the cleartext value cannot be reverted any more.During propagation, Syncope fetches all data of the given User, then prepares the attributes to propagate, according to the provided mapping; password has a special treatment:if cleartext value is available (this cannot happen duringPush), it is sent to the External Resourceifpassword.cipher.algorithmisAES(the only supported reversible algorithm), then the ciphered password value is made cleartext again, and sent to the External Resourceif theGenerateRandomPasswordPropagationActionsis enabled, a random password value is generated according to the definedpassword policyand sent to the External Resourceotherwise, anullvalue is sent to the External ResourcePassword values are always sent to External Resources wrapped as ConnIdGuardedStringobjects. |
| --- | --- |

By default, the propagation process is controlled by the
[PriorityPropagationTaskExecutor](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-java/src/main/java/org/apache/syncope/core/provisioning/java/propagation/PriorityPropagationTaskExecutor.java),
which implements the following logic:

- sort the tasks according to the related resource’s *priority*, then execute sequentially
- tasks for resources with no priority are executed afterwards, concurrently
- the execution of a given set of tasks is halted (and global failure is reported) whenever the first sequential task
  fails
- status and eventual error message (in case of no resource priority) can be saved for reporting, in the case where the related
  external resource was configured with adequate tracing
- minimize the set of operations to be actually performed onto the Identity Store by attempting to read the external
  object corresponding to the internal entity and comparing with the modifications provided

|  | Create or update?The minimization performed byPriorityPropagationTaskExecutormight lead to behaviors which look at first unexpected, but sound perfectly understandable once explained; in particular:aCREATEpropagation task might result in an effectiveUPDATEsent to the Connectorif preliminary read returns an external object matching the same remote key of the object requested to be createdanUPDATEpropagation task might result in an effectiveCREATEsent to the Connectorif preliminary read does not find any external object matching the remote key of the objected requested to be updatedPlease also note that this behavior is affected by the configuredpropagation policy, if available: in particular, whether fetching around provisioning is enabled or not. |
| --- | --- |

Different implementations of the
[PropagationTaskExecutor](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-api/src/main/java/org/apache/syncope/core/provisioning/api/propagation/PropagationTaskExecutor.java)
interface can be provided, in case the required behavior does not fit into the provided implementation.

##### [PropagationActions](#syncope-apache-org-docs-4-0-reference-guide--propagationactions)

The propagation process can be decorated with custom logic to be invoked around task execution, by associating
external resources to one or more [implementations](#syncope-apache-org-docs-4-0-reference-guide--implementations) of the
[PropagationActions](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-api/src/main/java/org/apache/syncope/core/provisioning/api/propagation/PropagationActions.java)
interface.

Some examples are included by default, see table below.

| AzurePropagationActions | Required for setup of an External Resource based on theConnId Azure connector bundle. |
| --- | --- |
| DBPasswordPropagationActions | If no password value was already provided in the propagation task, sends out the internal password hash value to DBMS; the cipher algorithm associated with the password must match the value ofPassword cipher algorithmfor theConnId DatabaseTable connector bundle. |
| GenerateRandomPasswordPropagationActions | If no password value was already provided in the propagation task, random password value is generated according to the definedpassword policyand sent to the External Resource. |
| GoogleAppsPropagationActions | Required for setup of an External Resource based on theConnId GoogleApps connector bundle. |
| LDAPMembershipPropagationActions | If a User is associated with a Group in Syncope, keep the corresponding User as a member of the corresponding Group in LDAP or AD. |
| LDAPPasswordPropagationActions | If no password value was already provided in the propagation task, sends out the internal password hash value to LDAP; the cipher algorithm associated with the password must match the value ofpasswordHashAlgorithmfor theLDAP connector bundle. |

#### [3.7.3. Pull](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull)

Pull is the mechanism used to acquire identity data from Identity Stores; for each external resource, one or more
[pull tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-pull) can be defined, run and scheduled for period execution.

Pull task execution involves querying the external resource for all [mapped](#syncope-apache-org-docs-4-0-reference-guide--mapping) [any types](#syncope-apache-org-docs-4-0-reference-guide--anytype), sorted
according to the order defined by a custom implementation of
[ProvisionSorter](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-api/src/main/java/org/apache/syncope/core/provisioning/api/ProvisionSorter.java)
or its default implementation
[DefaultProvisionSorter](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-java/src/main/java/org/apache/syncope/core/provisioning/java/pushpull/DefaultProvisionSorter.java)
.

Each entity is then processed in an isolated transaction; a retrieved entity can be:

1. *matching* if a corresponding internal entity was found, according to the [mapping](#syncope-apache-org-docs-4-0-reference-guide--mapping) of - or the
   [inbound policy](#syncope-apache-org-docs-4-0-reference-guide--policies-inbound) set for, if present - the enclosing external resource;
2. *unmatching* otherwise.

Once this has been assessed, entities are processed according to the matching / unmatching rules specified for the pull task:
by default, unmatching entities get created internally, and matching entities are updated.

Matching Rules

- `IGNORE`: do not perform any action;
- `UPDATE`: update matching entity;
- `DEPROVISION`: delete external entity;
- `UNLINK`: remove association with external resource, without performing any (de-)provisioning operation;
- `LINK`: associate with external resource, without performing any (de-)provisioning operation;
- `UNASSIGN`: unlink and delete.

Unmatching Rules

- `IGNORE`: do not perform any action;
- `UNLINK`: do not perform any action;
- `ASSIGN`: create internally, assign the external resource;
- `PROVISION`: create internally, do not assign the external resource.

|  | Pull ModeThe Identity Store can be queried in different ways, depending on thepull modethat is specified:FULL RECONCILIATIONThe complete list of entities available is processed.FILTERED RECONCILIATIONThe subset matching the filter (provided by the selected implementation ofReconFilterBuilder) of all available entities is processed.INCREMENTALOnly the actual modifications performed since the last pull task execution are considered. This mode requires the underlying connector bundle to implement the ConnIdSYNCoperation - only some of the available bundles match this condition.This is the only mode which allows pulling delete events, which may end up causing the removal of internal entities. |
| --- | --- |

|  | Pull TemplatesWith everypull taskit is possible to add a template for each definedany type.As the values specified in the template are applied to pulled entities, this can be used as mechanism for setting default values for attributes or external resources on entities.A typical use case is, when pulling Users from the external resourceR, to automatically assignRso that every further modification in Apache Syncope to such Users will bepropagatedback toR. |
| --- | --- |

##### [InboundActions](#syncope-apache-org-docs-4-0-reference-guide--inboundactions)

The pull process can be decorated with custom logic to be invoked around task execution, by associating
pull tasks to one or more [implementations](#syncope-apache-org-docs-4-0-reference-guide--implementations) of the
[InboundActions](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-api/src/main/java/org/apache/syncope/core/provisioning/api/pushpull/InboundActions.java)
interface.

Some examples are included by default, see the table below.

| ADMembershipPullActions | If a User is associated with a Group in AD, keep the corresponding User as a member of the corresponding Group in Syncope. |
| --- | --- |
| LDAPMembershipPullActions | If a User is associated with a Group in LDAP, keep the corresponding User as a member of the corresponding Group in Syncope. |
| LDAPPasswordPullActions | Import hashed password values from LDAP; the cipher algorithm associated with the password must match the value ofpasswordHashAlgorithmfor theLDAP connector bundle. |
| DBPasswordPullActions | Import hashed password values from DBMS; the cipher algorithm associated with the password must match the value ofPassword cipher algorithmfor theDatabaseTable connector bundle. |
| KafkaInboundActions | Instructs to fetch the attributes required during thelive syncprocess for theApache Kafka connector bundle. |

##### [Remediation](#syncope-apache-org-docs-4-0-reference-guide--remediation)

Errors during pull might arise for various reasons: values might not be provided for all mandatory attributes or
fail the configured validation, delete User as consequence of an incremental change’s processing might be blocked
because such User is configured as Group owner, and so on.

When Remediation is enabled for a certain [Pull Task](#syncope-apache-org-docs-4-0-reference-guide--tasks-pull), execution errors are reported to administrators,
which are given the chance to examine and possibly fix, or just discard.

#### [3.7.4. Live Sync](#syncope-apache-org-docs-4-0-reference-guide--provisioning-livesync)

Live sync allows to acquire identity data from records published to queue systems, like as
[Apache Kafka](https://kafka.apache.org/), [Apache ActiveMQ](https://activemq.apache.org/),
[Google PubSub](https://cloud.google.com/pubsub/) or similar.  
Compared to [pull](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull), records are processed as soon as they are published in the queue system,
while the [live sync task](#syncope-apache-org-docs-4-0-reference-guide--tasks-livesync) is running.

For each external resource, a single [live sync task](#syncope-apache-org-docs-4-0-reference-guide--tasks-livesync) can be defined: once started, it will remain
active until stopped.

Live sync tasks will be triggered by the publication of matching records on the external resource for all
[mapped](#syncope-apache-org-docs-4-0-reference-guide--mapping) [any types](#syncope-apache-org-docs-4-0-reference-guide--anytype), sorted according to the order defined by a custom implementation of
[ProvisionSorter](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-api/src/main/java/org/apache/syncope/core/provisioning/api/ProvisionSorter.java)
or its default implementation
[DefaultProvisionSorter](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-java/src/main/java/org/apache/syncope/core/provisioning/java/pushpull/DefaultProvisionSorter.java)
.

Once a record is received, the configured [implementation](#syncope-apache-org-docs-4-0-reference-guide--implementations) of
[LiveSyncDeltaMapper](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-api/src/main/java/org/apache/syncope/core/provisioning/api/pushpull/LiveSyncDeltaMapper.java)
is invoked to transform the record into a format which is in turn provided to an internally created and processed
[pull task](#syncope-apache-org-docs-4-0-reference-guide--tasks-pull).

#### [3.7.5. Push](#syncope-apache-org-docs-4-0-reference-guide--provisioning-push)

With push, the matching set of internal entities can be sent to Identity Stores - mainly for
(re)initialization purposes; for each external resource, one or more [push tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-push) can be defined, run and
scheduled for period execution.

Push task execution involves querying the internal storage for all [mapped](#syncope-apache-org-docs-4-0-reference-guide--mapping) [any types](#syncope-apache-org-docs-4-0-reference-guide--anytype), sorted
according to the order defined by a custom implementation of
[ProvisionSorter](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-api/src/main/java/org/apache/syncope/core/provisioning/api/ProvisionSorter.java)
or its default implementation
[DefaultProvisionSorter](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-java/src/main/java/org/apache/syncope/core/provisioning/java/pushpull/DefaultProvisionSorter.java)
.

Each entity is then processed in an isolated transaction; an internal entity can be:

1. *matching* if a corresponding remote entity was found, according to the [push policy](#syncope-apache-org-docs-4-0-reference-guide--policies-push) set for the
   enclosing external resource;
2. *unmatching* otherwise.

Once this has been assessed, entities are processed according to the matching / unmatching rules specified for the push task:
by default, unmatching entities are pushed to Identity Stores, and matching entities are updated.

Matching Rules

- `IGNORE`: do not perform any action;
- `UPDATE`: update matching entity;
- `DEPROVISION`: delete internal entity;
- `UNLINK`: remove association with external resource, without performing any (de-)provisioning operation;
- `LINK`: associate with external resource, without performing any (de-)provisioning operation;
- `UNASSIGN`: unlink and delete.

Unmatching Rules

- `IGNORE`: do not perform any action;
- `UNLINK`: remove association with external resource, without performing any (de-)provisioning operation;
- `ASSIGN`: create externally, assign the external resource;
- `PROVISION`: create externally, do not assign the external resource.

##### [PushActions](#syncope-apache-org-docs-4-0-reference-guide--pushactions)

The push process can be decorated with custom logic to be invoked around task execution, by associating
push tasks to one or more [implementations](#syncope-apache-org-docs-4-0-reference-guide--implementations) of the
[PushActions](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-api/src/main/java/org/apache/syncope/core/provisioning/api/pushpull/PushActions.java)
interface.

#### [3.7.6. Password Reset](#syncope-apache-org-docs-4-0-reference-guide--password-reset)

When users lost their password, a feature is available to help gaining back access to Apache Syncope: password reset.

The process can be outlined as follows:

1. user asks for password reset, typically via end-user
2. user is asked to provide an answer to the security question that was selected during self-registration or self-update
3. if the expected answer is provided, a unique token with time-constrained validity is internally generated and an
   e-mail is sent to the configured address for the user with a link - again, typically to the
   end-user - containing such token value
4. user clicks on the received link and provides new password value, typically via end-user
5. user receives confirmation via e-mail

|  | The outlined procedure requires a workinge-mail configuration.In particular:the first e-mail is generated from therequestPasswordResetnotification template: hence, the token-based access link to the end-user is managed there;the second e-mail is generated from theconfirmPasswordResetnotification template. |
| --- | --- |

|  | The process above requires the availability ofsecurity questionsthat users can pick up and provide answers for.The usage of security questions can be however disabled by setting thepasswordReset.securityQuestionvalue - seebelowfor details. |
| --- | --- |

|  | Once provided via Enduser Application, the answers to security questions areneverreported, neither via REST or Admin UI to administrators, nor to end-users via Enduser Application.This to avoid any information disclosure which can potentially lead attackers to reset other users' passwords. |
| --- | --- |

|  | In addition to the password reset feature, administrators can set a flag on a given user so that he / she is forced to update their password value at next login. |
| --- | --- |

### [3.8. Policies](#syncope-apache-org-docs-4-0-reference-guide--policies)

Policies control different aspects. They can be used to fine-tune and adapt the overall mechanisms to the
particularities of the specific domain in which a given Apache Syncope deployment is running.

|  | Policy CompositionWhen defining policies and associating them with different realms and resources, it is common to observe that several policies of the same type have to be enforced on the same user, group or any object.In such cases, Apache Syncope transparently composes all of the candidate policies and obtains a single applicable policy which contains all the conditions of the composing policies; this process, however, is not guaranteed to be successful, as different policies of the same type might provide conflicting clauses. |
| --- | --- |

#### [3.8.1. Account](#syncope-apache-org-docs-4-0-reference-guide--policies-account)

Account policies allow the imposition of constraints on username values, and are involved in the authentication process.

|  | When set for realm R, an account policy is enforced on all Users of R and sub-realms.When set for resource R, an account policy is enforced on all Users that have R assigned. |
| --- | --- |

When defining an account policy, the following information must be provided:

- max authentication attempts - how many times Users are allowed to fail authentication before getting suspended
- propagate suspension - when suspended as a consequence of too many authentication failures, should Users also be
  suspended on associated resources or not?
- pass-through resources - which [external resources](#syncope-apache-org-docs-4-0-reference-guide--external-resource-details) are involved with
  [pass-through authentication](#syncope-apache-org-docs-4-0-reference-guide--pass-through-authentication)
- rules - set of account rules to evaluate with the current policy

##### [Account Rules](#syncope-apache-org-docs-4-0-reference-guide--account-rules)

Account rules define constraints to apply to username values.

Some implementations are provided out-of-the-box, custom ones can be provided on given deployment.

|  | AsJAVAimplementation, writing custom account rules means:providing configuration parameters in an implementation ofAccountRuleConfenforcing in an implementation ofAccountRuleannotated via@AccountRuleConfClassreferring to the configuration class.AsGROOVYimplementation, writing custom account rules means implementingAccountRule |
| --- | --- |

###### [Default Account Rule](#syncope-apache-org-docs-4-0-reference-guide--default-account-rule)

The default account rule (enforced by
[DefaultAccountRule](https://github.com/apache/syncope/blob/syncope-4.0.5/core/spring/src/main/java/org/apache/syncope/core/spring/policy/DefaultAccountRule.java)
and configurable via
[DefaultAccountRuleConf](https://github.com/apache/syncope/blob/syncope-4.0.5/common/idrepo/lib/src/main/java/org/apache/syncope/common/lib/policy/DefaultAccountRuleConf.java)
) contains the following controls:

- maximum length - the maximum length to allow; `0` means no limit set;
- minimum length - the minimum length to allow; `0` means no limit set;
- pattern - [Java regular expression pattern](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/regex/Pattern.html) to
  match; `NULL` means no match is attempted;
- all uppercase - are lowercase characters allowed?
- all lowercase - are uppercase characters allowed?
- words not permitted - list of words that cannot be present, even as a substring;
- schemas not permitted - list of [schemas](#syncope-apache-org-docs-4-0-reference-guide--schema) whose values cannot be present, even as a substring;
- prefixes not permitted - list of strings that cannot be present as a prefix;
- suffixes not permitted - list of strings that cannot be present as a suffix.

|  | Before being able to configure the default account rule as mentioned above, you will need to first create aJAVAACCOUNT_RULEimplementationfor theorg.apache.syncope.common.lib.policy.DefaultAccountRuleConfclass. |
| --- | --- |

##### [Pass-through Authentication](#syncope-apache-org-docs-4-0-reference-guide--pass-through-authentication)

During user authentication, if the [resulting](#syncope-apache-org-docs-4-0-reference-guide--policy-composition) applicable account policy defines pass-through
resources, the provided credentials are verified first against the internal storage, then against each configured
external resource (provided that the underlying [connector instance](#syncope-apache-org-docs-4-0-reference-guide--connector-instance-details) has the `AUTHENTICATE`
capability set): the first check that succeeds will successfully authenticate the user.

This feature allows, for example, to reuse credentials contained in Identity Stores (without extracting them),
instead of storing password values in the internal storage. It also facilitates implementing authentication chains.

#### [3.8.2. Password](#syncope-apache-org-docs-4-0-reference-guide--policies-password)

Password policies allow the imposition of constraints on password values.

|  | When set for realm R, a password policy is enforced on all Users of R and sub-realms.When set for resource R, a password policy is enforced on all Users that have R assigned. |
| --- | --- |

When defining a password policy, the following information must be provided:

- allow null password - whether a password is mandatory for Users or not
- history length - how many values shall be considered in the history
- rules - set of password rules to evaluate with the current policy

##### [Password Rules](#syncope-apache-org-docs-4-0-reference-guide--password-rules)

Password rules define constraints to apply to password values.

Some implementations are provided out-of-the-box, custom ones can be provided on given deployment.

|  | AsJAVAimplementation, writing custom password rules means:providing configuration parameters in an implementation ofPasswordRuleConfenforcing in an implementation ofPasswordRuleannotated via@PasswordRuleConfClassreferring to the configuration class.AsGROOVYimplementation, writing custom account rules means implementingPasswordRule |
| --- | --- |

###### [Default Password Rule](#syncope-apache-org-docs-4-0-reference-guide--default-password-rule)

The default password rule (enforced by
[DefaultPasswordRule](https://github.com/apache/syncope/blob/syncope-4.0.5/core/spring/src/main/java/org/apache/syncope/core/spring/policy/DefaultPasswordRule.java)
and configurable via
[DefaultPasswordRuleConf](https://github.com/apache/syncope/blob/syncope-4.0.5/common/idrepo/lib/src/main/java/org/apache/syncope/common/lib/policy/DefaultPasswordRuleConf.java)
) is based on [Passay](https://www.passay.org/) and contains the following controls:

- maximum length - the maximum length to allow (`0` means no limit set);
- minimum length - the minimum length to allow (`0` means no limit set);
- alphabetical - the number of alphabetical characters required;
- uppercase - the number of uppercase characters required;
- lowercase - the number of lowercase characters required;
- digit - the number of digits required;
- special - the number of special characters required;
- special chars - the set of special characters allowed;
- illegal chars - the set of characters not allowed;
- repeat same - the size of the longest sequence of repeating characters allowed;
- username allowed - whether a username value can be used;
- words not permitted - list of words that cannot be present, even as a substring;
- schemas not permitted - list of [schemas](#syncope-apache-org-docs-4-0-reference-guide--schema) whose values cannot be present, even as a substring;

|  | The default password rule can be extended to cover specific needs, relying on thewhole set of featuresprovided by Passay. |
| --- | --- |

|  | Before being able to configure the default password rule as mentioned above, you will need to first create aJAVAPASSWORD_RULEimplementationfor theorg.apache.syncope.common.lib.policy.DefaultPasswordRuleConfclass. |
| --- | --- |

###### ["Have I Been Pwned?" Password Rule](#syncope-apache-org-docs-4-0-reference-guide--have-i-been-pwned-password-rule)

This password rule (enforced by
[HaveIBeenPwnedPasswordRule](https://github.com/apache/syncope/blob/syncope-4.0.5/core/spring/src/main/java/org/apache/syncope/core/spring/policy/HaveIBeenPwnedPasswordRule.java)
and configurable via
[HaveIBeenPwnedPasswordRuleConf](https://github.com/apache/syncope/blob/syncope-4.0.5/common/idrepo/lib/src/main/java/org/apache/syncope/common/lib/policy/HaveIBeenPwnedPasswordRuleConf.java)
) checks the provided password values against the popular
["Have I Been Pwned?"](https://haveibeenpwned.com) service.

|  | Before being able to configure the "Have I Been Pwned?" password rule as mentioned above, you will need to first create aJAVAPASSWORD_RULEimplementationfor theorg.apache.syncope.common.lib.policy.HaveIBeenPwnedPasswordRuleConfclass. |
| --- | --- |

#### [3.8.3. Access](#syncope-apache-org-docs-4-0-reference-guide--policies-access)

Access policies provide fine-grained control over the access rules to apply to
[client applications](#syncope-apache-org-docs-4-0-reference-guide--client-applications).

The following access policy configurations are available by default:

| DefaultAccessPolicyConf | It describes whether the client application is allowed to use WA, allowed to participate in single sign-on authentication, etc; additionally, it may be configured to require a certain set of principal attributes that must exist before access can be granted. |
| --- | --- |
| HttpRequestAccessPolicyConf | Make access decisions based on HTTP request properties as client IP address and user-agent. |
| OpenFGAAccessPolicyConf | Builds an authorization request and submits it toOpenFGA'scheckAPI endpoint. |
| RemoteEndpointAccessPolicyConf | Delegate access decisions to a remote endpoint by receiving the authenticated principal as url parameter of aGETrequest; the response code that the endpoint returns is then compared against the policy setting and if a match is found, access is granted. |
| TimeBasedAccessPolicyConf | Access is only allowed within the configured timeframe. |

|  | Access Policy instances are dynamically translated intoCAS Service Access Strategy. |
| --- | --- |

#### [3.8.4. Attribute Release](#syncope-apache-org-docs-4-0-reference-guide--policies-attribute-release)

Attribute Release policies decide how attributes are selected and provided to a given
[client application](#syncope-apache-org-docs-4-0-reference-guide--client-applications) in the final WA response.  
Additionally, each instance has the ability to apply an optional filter to weed out their attributes based on their
values.

|  | Attribute Release Policy instances are dynamically translated intoCAS Attribute Release Policy. |
| --- | --- |

#### [3.8.5. Authentication](#syncope-apache-org-docs-4-0-reference-guide--policies-authentication)

WA presents a number of strategies for handling authentication security policies, based on the defined
[authentication modules](#syncope-apache-org-docs-4-0-reference-guide--authentication-modules).  
Authentication Policies in general control the following:

1. Should the authentication chain be stopped after a certain kind of authentication failure?
2. Given multiple authentication handlers in a chain, what constitutes a successful authentication event?

Authentication Policies are typically activated after:

1. An authentication failure has occurred.
2. The authentication chain has finished execution.

Typical use cases of authentication policies may include:

1. Enforce a specific authentication module’s successful execution, for the entire authentication event to be considered
   successful.
2. Ensure a specific class of failure is not evident in the authentication chain’s execution log.
3. Ensure that all authentication modules in the chain are executed successfully, for the entire authentication event to
   be considered successful.

|  | Authentication Policy instances are dynamically translated intoCAS Authentication Policy. |
| --- | --- |

#### [3.8.6. Propagation](#syncope-apache-org-docs-4-0-reference-guide--policies-propagation)

Propagation policies are evaluated during the execution of [propagation tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-propagation) and are meant to
tweak the propagation process by setting the pre-fetch option or letting Syncope to retry the configured operations in
case of failures.

When defining a propagation policy, the following information must be provided:

- fetch around provisioning - the default behavior is to attempt to read upfront the object being propagated (to ensure
  it exists or not, depending on the actual operation scheduled to perform) and to read it again afterwards (to check the
  effective results); this can be disabled
- update delta - in case of update, all object attributes are propagated by default; when enabled, only the changed
  attributes will be instead propagated
- max number of attempts
- back-off strategy

  - `FIXED` - pauses for a fixed period of time before continuing
  - `EXPONENTIAL` - increases the back off period for each retry attempt in a given set up to a limit
  - `RANDOM` - chooses a random multiple of the interval that would come from a simple deterministic exponential

#### [3.8.7. Inbound](#syncope-apache-org-docs-4-0-reference-guide--policies-inbound)

Inbound policies are evaluated during the execution of [pull tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-pull) and are meant to:

1. help match existing Users, Groups and Any Objects during [pull](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull), thus generating update events
   (rather than create)
2. determine which action shall be taken in case such match is not unique (e.g. what to do if the same external account
   can be mapped to two distinct Users in Apache Syncope?)

|  | When set for resource R, an inbound policy is enforced on all Users, Groups and Any Objects pulled from R. |
| --- | --- |

When defining an inbound policy, the following information must be provided:

- conflict resolution action

  - `IGNORE` - do nothing
  - `FIRSTMATCH` - pull first matching object only
  - `LASTMATCH` - pull last matching object only
  - `ALL` - pull all matching objects
- rules - set of correlation rules to evaluate with the current policy; for each defined [any type](#syncope-apache-org-docs-4-0-reference-guide--anytype), a
  different rule is required

##### [Inbound Correlation Rules](#syncope-apache-org-docs-4-0-reference-guide--inbound-correlation-rules)

Inbound correlation rules define how to match objects received from [External Resources](#syncope-apache-org-docs-4-0-reference-guide--external-resources)
with existing Users (including [Linked Accounts](#syncope-apache-org-docs-4-0-reference-guide--linked-accounts)), Groups or Any Objects.

The
[default](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-java/src/main/java/org/apache/syncope/core/provisioning/java/pushpull/DefaultInboundCorrelationRule.java)
implementation attempts to match entities on the basis of the values of the provided plain attributes,
according to the available [mapping](#syncope-apache-org-docs-4-0-reference-guide--mapping).

|  | Custom inbound correlation rules can be provided byimplementingtheInboundCorrelationRuleinterface. |
| --- | --- |

#### [3.8.8. Push](#syncope-apache-org-docs-4-0-reference-guide--policies-push)

Push policies are evaluated during the execution of [push tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-push).

|  | When set for resource R, a push policy is enforced on all Users, Groups and Any Objects pushed to R. |
| --- | --- |

##### [Push Correlation Rules](#syncope-apache-org-docs-4-0-reference-guide--push-correlation-rules)

Push correlation rules define how to match Users (including [Linked Accounts](#syncope-apache-org-docs-4-0-reference-guide--linked-accounts)), Groups or Any Objects with
objects existing on [External Resources](#syncope-apache-org-docs-4-0-reference-guide--external-resources).

The
[default](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-java/src/main/java/org/apache/syncope/core/provisioning/java/pushpull/DefaultPushCorrelationRule.java)
]
implementation attempts to match entities on the basis of the values of the provided plain attributes,
according to the available [mapping](#syncope-apache-org-docs-4-0-reference-guide--mapping).

|  | Custom push correlation rules can be provided byimplementingthePushCorrelationRuleinterface. |
| --- | --- |

#### [3.8.9. Ticket Expiration](#syncope-apache-org-docs-4-0-reference-guide--policies-ticket-expiration)

Ticket Expiration policies control the duration of various types of WA sessions.

|  | Ticket Expiration Policy instances are dynamically translated intotheir CAS equivalent. |
| --- | --- |

### [3.9. Workflow](#syncope-apache-org-docs-4-0-reference-guide--workflow)

Workflow manages the internal identity lifecycle by defining statuses and transitions that every user, group or any
object in Apache Syncope will traverse. A workflow instance is started once identities get created, and shut down when
they are removed.

Workflow is triggered during the [provisioning](#syncope-apache-org-docs-4-0-reference-guide--provisioning) process as the first step in creating, updating or deleting
identities into the internal storage.

|  | Workflow AdaptersThe workflow features are defined by the workflow adapter interfaces:UserWorkflowAdapterGroupWorkflowAdapterAnyObjectWorkflowAdapterDefault implementations are available:DefaultUserWorkflowAdapterDefaultGroupWorkflowAdapterDefaultAnyObjectWorkflowAdapterCustom adapters can be provided by implementing the related interfaces, also as bridges towards third-party tools asCamundaorjBPM. |
| --- | --- |

|  | Which workflow adapter for users?Do you needapprovalmanagement?FlowableIf approval management is not needed, do you want to customize the internal user processing, or attach custom logic to it? Provide a Java class with your customizations, extendingDefaultUserWorkflowAdapterNo approval nor customizations needed? Stick withDefaultUserWorkflowAdapter |
| --- | --- |

#### [3.9.1. Flowable User Workflow Adapter](#syncope-apache-org-docs-4-0-reference-guide--flowable-user-workflow-adapter)

An advanced adapter is provided for Users, based on [Flowable](https://www.flowable.org/), one of reference open
source [BPMN 2.0](http://www.bpmn.org/) implementations.

The
[FlowableUserWorkflowAdapter](https://github.com/apache/syncope/blob/syncope-4.0.5/ext/flowable/flowable-bpmn/src/main/java/org/apache/syncope/core/flowable/impl/FlowableUserWorkflowAdapter.java)
is bootstrapped from
[userWorkflow.bpmn20.xml](https://github.com/apache/syncope/blob/syncope-4.0.5/ext/flowable/flowable-bpmn/src/main/resources/userWorkflow.bpmn20.xml)
and presents several advantages and more features, if compared to the default user adapter:

1. Besides mandatory statuses, which are modeled as BPMN `userTask` instances, more can be freely added
   at runtime, provided that adequate transitions and conditions are also inserted; more details about available BPMN
   constructs are available in the [Flowable User Guide](https://www.flowable.com/open-source/docs/bpmn/ch07b-BPMN-Constructs).  
   Additional statuses and transitions allow the internal processes of Apache Syncope to better adapt to suit organizational flows.
2. Custom logic can be injected into the workflow process by providing BPMN `serviceTask` instances.
3. Flowable forms are used for implementing [approval](#syncope-apache-org-docs-4-0-reference-guide--approval).
4. [admin console](#syncope-apache-org-docs-4-0-reference-guide--admin-console) supports web-based graphical modeling of the workflow definition.

![Default Flowable user workflow](syncope.apache.org/docs/4.0/images/userWorkflow.png)

Figure 10. Default Flowable user workflow

##### [Approval](#syncope-apache-org-docs-4-0-reference-guide--approval)

Every transition in the Flowable user workflow definition can be subjected to approval.

The underlying idea is that some kind of self-modifications (group memberships, external resource assignments, …​)
might not be allowed to 'plain' Users, as there could be conditions which require management approval.
Managers could also be asked to complete the information provided before the requested operation is finished.

In order to define an approval form, a dedicated BPMN `userTask` needs to be defined, following the rules established
for Flowable forms.

|  | What is required for administrators to manage approval?The following conditions must be met, for an UserUto act as administrator for approval:Umust own the followingentitlements, for all the required realms:USER_REQUEST_FORM_CLAIMUSER_REQUEST_FORM_LISTUSER_REQUEST_FORM_SUBMITUSER_READThe BPMNuserTaskmust either indicateUamongcandidateUsersor at least one of the groups assigned toUamongcandidateGroups, as required byFlowable’s task assignment rulesThe special super-useradminis entitled to manage all approvals, even those not specifying anycandidateUsersorcandidateGroups. |
| --- | --- |

Example 6. Approving self-registration

The snippet below shows how to define an approval form in XML; the same operation can be performed via the GUI editor
provided by [admin console](#syncope-apache-org-docs-4-0-reference-guide--admin-console).

```xml
<userTask id="createApproval" name="Create approval"
          flowable:candidateGroups="managingDirector"
          flowable:formKey="createApproval"> (1)
  <extensionElements>
    <flowable:formProperty id="username" name="Username" type="string"
                           expression="${userTO.username}" writable="false"/> (2)
    <flowable:formProperty id="approve" name="Approve?" type="boolean"
                           variable="approve" required="true"/> (3)
    <flowable:formProperty id="rejectReason" name="Reason for rejecting" type="string"
                           variable="rejectReason"/>
  </extensionElements>
</userTask>
```

| 1 | formKeyandidmust be unique across the workflow definition,nameis displayed by the admin console;candidateGroupsandcandidateUsersmight be defined, even both, to indicate which Groups or Users should be managing these approvals; if none are specified, onlyadminis entitled to manage such approval |
| --- | --- |
| 2 | expressionwill be evaluated against the current requestinguser(as workflow variable) and related properties; read-only form input can be defined by settingwritable="false" |
| 3 | exporting approval inputs into workflow variables is possible via thevariableattribute; required form input can be defined by settingrequired="true" |

Once the form is defined, any modification subject to that approval will be manageable via the admin console, according to
the following flow (the actual operations on the admin console for the sample above are reported [below](#syncope-apache-org-docs-4-0-reference-guide--console-approval)):

1. administrator A sees the new approval notifications
2. administrator A claims the approval and is then allowed to manage it
3. administrator A reviews the updated user, with ongoing modification applied (no actual modification performed yet)
4. administrator A can approve or reject such modification

##### [Request Management](#syncope-apache-org-docs-4-0-reference-guide--request-management)

Request management is a key-feature of Identity Governance and allows to define and manage, in a structured way,
whatever process intended to update identity attributes, memberships and relationships.  
Request examples are "assign mobile phone", "grant groups on AD" or "consent access to application".

Users can initiate whichever request among the ones defined; once initiated, such requests will follow their own path,
which might also include one or more [approval](#syncope-apache-org-docs-4-0-reference-guide--approval) steps.

Example 7. Assigning printer to user

The BPMN process below shows how to define an user request in XML; the same operation can be performed via the GUI
editor provided by [admin console](#syncope-apache-org-docs-4-0-reference-guide--admin-console).

In this user request definition:

1. user selects one of printers defined in the system, for self-assignment
2. administrator approves user’s selection
3. a [relationship](#syncope-apache-org-docs-4-0-reference-guide--memberships-relationships) between user and printer is established

```xml
<process id="assignPrinterRequest" name="Assign printer" isExecutable="true">
  <startEvent id="startevent1" name="Start"/>
  <endEvent id="endevent1" name="End"/>
  <sequenceFlow id="flow1" sourceRef="startevent1" targetRef="selectPrinter"/>
  <userTask id="selectPrinter" name="Select printer" flowable:formKey="selectPrinter"
            flowable:assignee="${wfExecutor}"> (1)
    <extensionElements>
      <flowable:formProperty id="printer" name="Printer"
                             variable="printer" type="dropdown" required="true"> (2)
        <flowable:value id="dropdownValueProvider" name="printersValueProvider"/>
      </flowable:formProperty>
      <flowable:formProperty id="printMode" name="Preferred print mode?" type="enum">
        <flowable:value id="bw" name="Black / White"/>
        <flowable:value id="color" name="Color"/>
      </flowable:formProperty>
    </extensionElements>
  </userTask>
  <userTask id="approvePrinter" name="Approve printer" flowable:formKey="approvePrinter"> (3)
    <extensionElements>
      <flowable:formProperty id="username" name="Username" type="string"
                             expression="${userTO.username}" writable="false"/>
      <flowable:formProperty id="printer" name="Selected printer" type="string"
                             expression="${printer}" writable="false"/>
      <flowable:formProperty id="approve" name="Approve?" type="boolean"
                             variable="approve" required="true"/>
    </extensionElements>
  </userTask>
  <sequenceFlow id="sid-D7047714-8E57-46B8-B6D4-4844DE330329"
                sourceRef="selectPrinter" targetRef="approvePrinter"/>
  <serviceTask id="createARelationship" name="Create ARelationship"
               flowable:delegateExpression="${createARelationship}"/> (4)
  <sequenceFlow id="sid-33880AE7-35C6-4A39-8E5B-12D8BA53F042"
                sourceRef="approvePrinter" targetRef="createARelationship"/>
  <sequenceFlow id="sid-831E1896-EDF9-4F7D-AA42-E86CC1F8C5D3"
                sourceRef="createARelationship" targetRef="endevent1"/>
</process>
```

| 1 | the first form defined is self-assigned to the user which has started this request |
| --- | --- |
| 2 | thedropdowntype is a Syncope extension of the form property types supported by Flowable and allows to inject a list of elements via thedropdownValueProvidervalue (with nameprintersValueProviderin this sample), which must be a Spring bean implementing theDropdownValueProviderinterface |
| 3 | the second form is a traditional approval form, as seenabove |
| 4 | this is aFlowableServiceTaskimplementation which takes care of establishing the relationship |

### [3.10. Notifications](#syncope-apache-org-docs-4-0-reference-guide--notifications)

Apache Syncope can be instructed to send out notification e-mails when certain [events](#syncope-apache-org-docs-4-0-reference-guide--notification-events) occur.

Every notification generates one or more [notification tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-notification), holding the actual
e-mails to be sent. The tasks are ordinarily scheduled for execution according to the value provided for
`notificationjob.cronExpression` - see [below](#syncope-apache-org-docs-4-0-reference-guide--configuration-parameters) for details - and can be saved for later
re-execution.

When defining a notification, the following information must be provided:

- [notification template](#syncope-apache-org-docs-4-0-reference-guide--notification-templates) - template for e-mail generation
- sender - e-mail address appearing in the `From` field of the generated e-mail(s)
- subject - text used as e-mail subject
- recipient e-mail attribute - which user attribute shall be considered as e-mail address for delivery (as users might
  in principle have different e-mail attributes)
- recipient(s) - the actual e-mail recipient(s) which can be specified either as:

  - list of static e-mail addresses
  - matching condition to be applied to available users
  - Java class implementing the
    [RecipientsProvider](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-api/src/main/java/org/apache/syncope/core/provisioning/api/notification/RecipientsProvider.java)
    interface
- [notification event(s)](#syncope-apache-org-docs-4-0-reference-guide--notification-events) - event(s) triggering the enclosing notification
- about - the condition matching Users, Groups or Any Objects which are evaluated for the specified events; for users,
  the matching entities can be also considered as additional recipients
- trace level - control how much tracing (including logs and execution details) shall be carried over during execution
  of the generated [notification tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-notification)

#### [3.10.1. Notification Events](#syncope-apache-org-docs-4-0-reference-guide--notification-events)

Notification (and [Audit](#syncope-apache-org-docs-4-0-reference-guide--audit-events)) events are essentially a means of identifying the invocation of specific methods
within the [Core](#syncope-apache-org-docs-4-0-reference-guide--core), in line with *join points* in the
[Aspect Oriented Programming (AOP)](https://en.wikipedia.org/wiki/Aspect-oriented_programming).

An event is identified by the following five coordinates:

1. type - which can be one of

   - `LOGIC`
   - `TASK`
   - `PROPAGATION`
   - `PULL`
   - `PUSH`
   - `CUSTOM`
2. category - the possible values depend on the selected type: for `LOGIC` the [Logic](#syncope-apache-org-docs-4-0-reference-guide--logic) components available,
   for `TASK` the various [Scheduled Tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-scheduled) configured, for `PROPAGATION`, `PULL` and `PUSH` the defined Any Types
3. subcategory - completes category with external resource name, when selecting `PROPAGATION`, `PULL` or `PUSH`
4. event type - the final identification of the event; depends on the other coordinates
5. success or failure - whether the current event shall be considered in case of success or failure

The admin console provides [tooling](#syncope-apache-org-docs-4-0-reference-guide--console-configuration-notifications) to assist with the specification of valid events.

|  | An event is uniquely identified by a string of the following form:[type]:[category]:[subcategory]:[event type]:[SUCCESS\|FAILURE]Some samples:[PUSH]:[GROUP]:[resource-db-scripted]:[matchingrule_deprovision]:[SUCCESS]successful Grouppushto the external resourceresource-db-scripted, when deprovisioning matching entities[LOGIC]:[RealmLogic]:[]:[create]:[FAILURE]unsuccessful Realm creation[CUSTOM]:[]:[]:[unexpected identification]:[SUCCESS]successful execution of the event identified by theunexpected identificationstring |
| --- | --- |

|  | Custom events can be used to trigger notifications from non-predefined joint points, as BPMNuserTaskinstances within theFlowable User Workflow Adapter,PropagationActions,PushActions,InboundActionsor other custom code. |
| --- | --- |

#### [3.10.2. Notification Templates](#syncope-apache-org-docs-4-0-reference-guide--notification-templates)

A notification template is defined as a pair of [JEXL](#syncope-apache-org-docs-4-0-reference-guide--jexl) expressions, to be used respectively for plaintext and
HTML e-mails, and is available for selection in the notification specification.

|  | Notification templates can be easily managed via theadmin console. |
| --- | --- |

The full power of JEXL expressions is available.  
For example, the `user` variable, an instance of
[UserTO](https://github.com/apache/syncope/blob/syncope-4.0.5/common/idrepo/lib/src/main/java/org/apache/syncope/common/lib/to/UserTO.java)
with actual value matching the *about* condition as introduced above, can be used.

Example 8. Plaintext notification template

```text
Hi ${user.getPlainAttr("firstname").get().values[0]} ${user.getPlainAttr("surname").get().values[0]},
  welcome to Syncope!

Your username is ${user.username}.
Your email address is ${user.getPlainAttr("email").get().values[0]}.

Best regards.
```

Example 9. HTML notification template

```html
<html>
  <body>
    <h3>Hi ${user.getPlainAttr("firstname").get().values[0]} ${user.getPlainAttr("surname").get().values[0]},
      welcome to Syncope!</h3>
    <p>Your username is ${user.username}.<br/>
    Your email address is ${user.getPlainAttr("email").get().values[0]}.</p>
    <p>Best regards.</p>
  </body>
</html>
```

### [3.11. Commands](#syncope-apache-org-docs-4-0-reference-guide--commands)

A Command is defined via an [Implementation](#syncope-apache-org-docs-4-0-reference-guide--implementations) of type `COMMAND`, providing a Java or Groovy class
for the
[Command](https://github.com/apache/syncope/blob/syncope-4.0.5/core/idrepo/logic/src/main/java/org/apache/syncope/core/logic/api/Command.java),
interface, designed to optionally take parameters.

The typical use case is to encapsulate, in a single logical unit, the equivalent of two or more [REST](#syncope-apache-org-docs-4-0-reference-guide--rest) calls.

Once defined, Commands can be executed via dedicated REST endpoints, or via [Console UI](#syncope-apache-org-docs-4-0-reference-guide--engagements).

### [3.12. Tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks)

Tasks control the effective operations that are ongoing in the [Core](#syncope-apache-org-docs-4-0-reference-guide--core).

Whilst tasks define what and how to perform, they are supposed to be run by some entity (depending on the actual task
type, see below for details); their execution result can be saved for later examination.

#### [3.12.1. Propagation](#syncope-apache-org-docs-4-0-reference-guide--tasks-propagation)

A propagation task encapsulates all the information that is required - according to the defined [mapping](#syncope-apache-org-docs-4-0-reference-guide--mapping) - to create,
update or delete a given User, Group or Any Object, to / from a certain Identity Store:

- operation - `CREATE`, `UPDATE` or `DELETE`
- connObjectKey - value for ConnId
  [unique identifier](http://connid.tirasa.net/apidocs/1.6/org/identityconnectors/framework/common/objects/Uid.html)
  on the Identity Store
- oldConnObjectKey - the former unique identifier on the Identity Store: bears value only during updates involving the
  unique identifier
- attributes - set of ConnId
  [attributes](http://connid.tirasa.net/apidocs/1.6/org/identityconnectors/framework/common/objects/Attribute.html) built
  upon internal identity data and configured mapping
- resource - related [external resource](#syncope-apache-org-docs-4-0-reference-guide--external-resources)
- objectClass - ConnId
  [object class](http://connid.tirasa.net/apidocs/1.6/org/identityconnectors/framework/common/objects/ObjectClass.html)
- entity - reference to the internal identity: User, Group or Any Object

|  | Propagation tasks are automatically generated via the configuredPropagationManager, executed (by default) via thePriorityPropagationTaskExecutorduring thepropagationprocess, and are permanently saved - for later re-execution or for examining the execution details - depending on the trace levels set on the relatedexternal resource.Automatic retry in case of failure can be configured by mean of apropagation policy, for the related external resource. |
| --- | --- |

#### [3.12.2. Pull](#syncope-apache-org-docs-4-0-reference-guide--tasks-pull)

Pull tasks are required to define and trigger the [pull](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull) process from Identity Stores.

When defining a pull task, the following information must be provided:

- related [external resource](#syncope-apache-org-docs-4-0-reference-guide--external-resources)
- chosen [pull mode](#syncope-apache-org-docs-4-0-reference-guide--pull-mode)
- destination [Realm](#syncope-apache-org-docs-4-0-reference-guide--realms) - where entities selected for creation are going to be placed
- whether creation, update or deletion on internal storage are allowed or not
- whether [remediation](#syncope-apache-org-docs-4-0-reference-guide--remediation) is enabled
- whether to synchronize the status information from the related identity store
- selected [matching and unmatching rules](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull)
- optional [inbound action(s)](#syncope-apache-org-docs-4-0-reference-guide--inboundactions)
- [entity templates](#syncope-apache-org-docs-4-0-reference-guide--pull-templates)
- scheduling information:

  - when to start
  - [cron expression](https://docs.spring.io/spring-framework/reference/6.2/integration/scheduling.html#scheduling-cron-expression)

|  | Pull tasks are executed, either upon request or due to a schedule, via thePullJobDelegateduring thepullprocess, and are permanently saved - for later re-execution or for examining the execution details - depending on the trace level set on the relatedexternal resource. |
| --- | --- |

|  | DryRunIt is possible to simulate the execution of a pull (or push) task without performing any actual modification by selecting theDryRunoption. The execution results will be still available for examination. |
| --- | --- |

|  | Concurrent Pull Task ExecutionsBy default, pull tasks are set to accept and sequentially process the objects received from the configured External Resource; it is also possible to configure a pull task to work on several objects at once in order to speed up the overall execution time. |
| --- | --- |

#### [3.12.3. Live Sync](#syncope-apache-org-docs-4-0-reference-guide--tasks-livesync)

Live sync tasks are required to define and trigger the [live sync](#syncope-apache-org-docs-4-0-reference-guide--provisioning-livesync) process from Identity Stores.

When defining a live sync task, the following information must be provided:

- related [external resource](#syncope-apache-org-docs-4-0-reference-guide--external-resources)
- destination [Realm](#syncope-apache-org-docs-4-0-reference-guide--realms) - where entities selected for creation are going to be placed
- whether creation, update or deletion on internal storage are allowed or not
- whether [remediation](#syncope-apache-org-docs-4-0-reference-guide--remediation) is enabled
- whether to synchronize the status information from the related identity store
- selected [live sync delta mapper](#syncope-apache-org-docs-4-0-reference-guide--provisioning-livesync)
- selected [matching and unmatching rules](#syncope-apache-org-docs-4-0-reference-guide--provisioning-pull)
- optional [inbound action(s)](#syncope-apache-org-docs-4-0-reference-guide--inboundactions)
- [entity templates](#syncope-apache-org-docs-4-0-reference-guide--pull-templates)

|  | Live sync tasks are executed via theLiveSyncJobDelegateduring thelive syncprocess; the execution results are permanently saved - for examining the execution details - depending on the trace level set on the relatedexternal resource. |
| --- | --- |

|  | Concurrent Live Sync Task ExecutionsBy default, live sync tasks are set to accept and sequentially process the objects received from the configured External Resource; it is also possible to configure a live sync task to work on several objects at once in order to speed up the overall execution time. |
| --- | --- |

#### [3.12.4. Push](#syncope-apache-org-docs-4-0-reference-guide--tasks-push)

Push tasks are required to define and trigger the [push](#syncope-apache-org-docs-4-0-reference-guide--provisioning-push) process to Identity Stores.

When defining a push task, the following information must be provided:

- related [external resource](#syncope-apache-org-docs-4-0-reference-guide--external-resources)
- source [Realm](#syncope-apache-org-docs-4-0-reference-guide--realms) - where entities to push will be read from
- filter information for selecting which internal entities will be pushed onto the identity store
- whether creation, update or deletion on the identity store are allowed or not
- whether to synchronize the status information with internal storage
- selected [matching and unmatching rules](#syncope-apache-org-docs-4-0-reference-guide--provisioning-push)
- optional [push action(s)](#syncope-apache-org-docs-4-0-reference-guide--pushactions)
- scheduling information:

  - when to start
  - [cron expression](https://docs.spring.io/spring-framework/reference/6.2/integration/scheduling.html#scheduling-cron-expression)

|  | Push tasks are executed, either upon request or due to a schedule, via thePushJobDelegateduring thepushprocess, and are permanently saved - for later re-execution or for examining the execution details - depending on the trace level set on the relatedexternal resource. |
| --- | --- |

|  | Concurrent Push Task ExecutionsBy default, push tasks are set to sequentially send items to the configured External Resource; it is also possible to configure a push task to work on several objects at once in order to speed up the overall execution time. |
| --- | --- |

#### [3.12.5. Notification](#syncope-apache-org-docs-4-0-reference-guide--tasks-notification)

A notification task encapsulates all the information that is required to send out a notification e-mail, according to the
specification provided in a given [notification](#syncope-apache-org-docs-4-0-reference-guide--notifications):

- entity - reference to the internal identity - User, Group or Any Object - the notification task refers to
- sender e-mail address
- e-mail subject
- effective e-mail recipient(s)
- e-mail body as plaintext and / or HTML

|  | Notification tasks are automatically generated via theNotificationManager, executed via theNotificationJoband are permanently saved - for later re-execution or for examining the execution details - depending on the trace level  set on the relatednotification. |
| --- | --- |

#### [3.12.6. Macros](#syncope-apache-org-docs-4-0-reference-guide--tasks-macro)

Macro tasks are meant to group one or more [Commands](#syncope-apache-org-docs-4-0-reference-guide--commands) into a given execution sequence, alongside with
arguments required to run, with option to define an input form to drive user interaction.

When defining a macro task, the following information must be provided:

- commands to run with their args, either statically defined or mapped to form properties by [JEXL](#syncope-apache-org-docs-4-0-reference-guide--jexl) expressions
- [Realm](#syncope-apache-org-docs-4-0-reference-guide--realms) for [delegated administration](#syncope-apache-org-docs-4-0-reference-guide--delegated-administration) to restrict the set of users entitled to
  list, update or execute the given macro task
- scheduling information:

  - when to start
  - [cron expression](https://docs.spring.io/spring-framework/reference/6.2/integration/scheduling.html#scheduling-cron-expression)

##### [MacroActions](#syncope-apache-org-docs-4-0-reference-guide--macroactions)

Macro task execution can be decorated with custom logic to be invoked around task execution, by associating
macro tasks to a given [Implementation](#syncope-apache-org-docs-4-0-reference-guide--implementations) of the
[MacroActions](https://github.com/apache/syncope/blob/syncope-4.0.5/core/idrepo/logic/src/main/java/org/apache/syncope/core/logic/api/MacroActions.java)
interface.

Example 10. Macro task with input form

Let’s assume there are two `Command` instances defined, alongside with their arguments:

1. `NewPrinterCommand` which creates a new Realm and an AnyObject instance of type `PRINTER` beloging to it
2. `BinaryCommand` which simply logs about the received argument

```java
public class NewPrinterCommandArgs extends CommandArgs {

    @NotEmpty
    @Schema(description = "parent realm", example = "/even/two", defaultValue = "/",
            requiredMode = Schema.RequiredMode.REQUIRED)
    private String parentRealm = "/";

    @NotEmpty
    @Schema(description = "new realm name", example = "realm123",
            requiredMode = Schema.RequiredMode.REQUIRED)
    private String realmName;

    @NotEmpty
    @Schema(description = "printer name", example = "printer123",
            requiredMode = Schema.RequiredMode.REQUIRED)
    private String printerName;

    // getter and setter methods omitted
}
```

```java
public class NewPrinterCommand implements Command<NewPrinterCommandArgs> {

    private static final Logger LOG = LoggerFactory.getLogger(NewPrinterCommand.class);

    @Autowired
    private RealmLogic realmLogic;

    @Autowired
    private AnyObjectLogic anyObjectLogic;

    private Optional<RealmTO> getRealm(final String fullPath) {
        return realmLogic.search(null, Set.of(fullPath), Pageable.unpaged()).get().
                filter(realm -> fullPath.equals(realm.getFullPath())).findFirst();
    }

    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    @Override
    public Result run(final TestCommandArgs args) {
        // 1. create new Realm
        RealmTO realm = new RealmTO();
        realm.setName(args.getRealmName());
        realm.setParent(getRealm(args.getParentRealm()).map(RealmTO::getKey).orElse(null));
        realm = realmLogic.create(args.getParentRealm(), realm).getEntity();
        LOG.info("Realm created: {}", realm.getFullPath());

        // 2. create new PRINTER
        AnyObjectTO anyObject = anyObjectLogic.create(new AnyObjectCR.Builder(
                        realm.getFullPath(), "PRINTER", args.getPrinterName()).
                        plainAttr(new Attr.Builder("location").value("location").build()).
                        build(),
                false).getEntity();
        LOG.info("PRINTER created: {}", anyObject.getName());

        return new Result(
                "Realm created: " + realm.getFullPath()
                + "; PRINTER created: " + anyObject.getName(),
                Map.of("realm", realm.getKey(), "PRINTER", anyObject.getKey()));
    }
}
```

```java
public class BinaryCommandArgs extends CommandArgs {

    private static final long serialVersionUID = -8257974017887359696L;

    private String binaryParam;

    // getter and setter method omitted
}
```

```java
public class BinaryCommand implements Command<BinaryCommandArgs> {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryCommand.class);

    private static final Tika TIKA = new Tika();

    static {
        TIKA.setMaxStringLength(-1);
    }

    @Override
    public Result run(final BinaryCommandArgs args) {
        String base64 = args.getBinaryParam();
        LOG.info("Input value received: {}", base64);

        byte[] binaryValue = Base64.getDecoder().decode(base64);
        LOG.info("Byte array with length {} and mime type {}",
                 binaryValue.length, TIKA.detect(binaryValue));

        return new Result("SUCCESS", Map.of());
    }
}
```

Let’s also assume that the following `MacroActions` instance is available, showing how to populate dropdown values,
how to perform some example validation and also allowing to alter the arguments value before the related command is
executed:

```java
public class SampleMacroActions implements MacroActions {

    @Autowired
    private RealmDAO realmDAO;

    @Autowired
    private RealmSearchDAO realmSearchDAO;

    @Transactional(readOnly = true)
    @Override
    public Map<String, String> getDropdownValues(final String formProperty) {
        return realmSearchDAO.findChildren(realmDAO.getRoot()).stream().
                collect(Collectors.toMap(Realm::getFullPath, Realm::getName));
    }

    @Override
    public void validate(final SyncopeForm form, final Map<String, Object> vars)
        throws ValidationException {

        Object binaryValue = vars.get("binaryField");
        if (!(binaryValue instanceof byte[])) {
            throw new ValidationException(
              "Expected byte[], found " + binaryValue.getClass().getName());
        }
    }

    @Override
    public void beforeCommand(final Command<CommandArgs> command, final CommandArgs args) {
        if (args instanceof BinaryCommandArgs binaryCommandArgs) {
            // option to alter binaryCommandArgs before the related command is executed
        }
    }
}
```

It is now possible to define the following Macro task (details are simplified to increase readability):

```json
    {
      "key": "019a35af-1a5a-75d1-920c-7f388696be0d",
      "cronExpression": null,
      "jobDelegate": "MacroJobDelegate",
      "name": "BinaryMacro",
      "realm": "/",
      "formPropertyDefs": [
        {
          "name": "parent",  (1)
          "type": "Dropdown",
          "readable": true,
          "writable": true,
          "required": true
        },
        {
          "name": "realm", (2)
          "type": "String",
          "readable": true,
          "writable": true,
          "required": true
        },
        {
          "name": "binaryField", (3)
          "type": "Binary",
          "readable": true,
          "writable": true,
          "required": true,
          "mimeType": "application/pdf"
        }
      ],
      "commands": [
        {
          "key": "NewPrinterCommand",
          "args": {
            "_class": "org.apache.syncope.common.lib.command.NewPrinterCommandArgs",
            "parentRealm": "${parent}",  (4)
            "realmName": "${realm}"  (5)
          }
        },
        {
          "key": "BinaryCommand",
          "args": {
            "_class": "org.apache.syncope.common.lib.command.BinaryCommandArgs",
            "binaryParam": "${syncope:base64Encode(binaryField)}"  (6)
          }
        }
      ],
      "continueOnError": false,
      "saveExecs": true,
      "macroActions": "SampleMacroActions"
    }
```

| 1 | dropdown form property whose values are generated bySampleMacroActions#getDropdownValues |
| --- | --- |
| 2 | string form property |
| 3 | binary form property expecting a PDF input file |
| 4 | binds theparentRealmproperty ofNewPrinterCommandArgsto the value provided for the form propertyparent |
| 5 | binds therealmNameproperty ofNewPrinterCommandArgsto the value provided for the form propertyrealm |
| 6 | binds thebinaryParamproperty ofBinaryCommandArgsto the Base64-decoded value provided for the form propertybinaryField |

|  | Please take into account that both defining and running a Macro task form are a much pleasant experience when performed in theAdmin Console. |
| --- | --- |

#### [3.12.7. Scheduled](#syncope-apache-org-docs-4-0-reference-guide--tasks-scheduled)

Scheduled tasks allow for the injection of custom logic into the [Core](#syncope-apache-org-docs-4-0-reference-guide--core) in the area of execution and scheduling.

When defining a scheduled task, the following information must be provided:

- job delegate class: Java class extending
  [AbstractSchedTaskJobDelegate](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-java/src/main/java/org/apache/syncope/core/provisioning/java/job/AbstractSchedTaskJobDelegate.java)
  providing the custom logic to execute
- scheduling information:

  - when to start
  - [cron expression](https://docs.spring.io/spring-framework/reference/6.2/integration/scheduling.html#scheduling-cron-expression)

|  | Scheduled tasks are ideal for implementing periodic checks or clean-up operations, possibly in coordination with other components; some examples:move users from "pending delete" to "deleted" status 15 days after they reached the "pending delete" status (requires interaction withFlowable User Workflow Adapter)send out notification e-mails to users whose password is about to expire on an Identity Storedisable all users not logging into the system for the past 6 months |
| --- | --- |

### [3.13. Reports](#syncope-apache-org-docs-4-0-reference-guide--reports)

Reports are a powerful tool to extract, filter and format relevant information from a running Apache Syncope deployment,
for a wide range of purposes: from business to [DevOps](https://en.wikipedia.org/wiki/DevOps).

When defining a report, the following information must be provided:

- mime type and file extension: the type of content that the report is expected to generate
- job delegate class: Java class extending
  [AbstractReportJobDelegate](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-java/src/main/java/org/apache/syncope/core/provisioning/java/job/report/AbstractReportJobDelegate.java)
  providing the custom logic to extract information from Syncope and generate output according to the configured mime type
- scheduling information:

  - when to start
  - [cron expression](https://docs.spring.io/spring-framework/reference/6.2/integration/scheduling.html#scheduling-cron-expression)

### [3.14. Audit](#syncope-apache-org-docs-4-0-reference-guide--audit)

The audit feature allows to capture [events](#syncope-apache-org-docs-4-0-reference-guide--audit-events) occurring within the [Core](#syncope-apache-org-docs-4-0-reference-guide--core) and to store relevant information
about them.  
By default, events are written as entries into the `AuditEvent` table of the internal storage.  
Audit events can also be processed differently, for example when using the [Elasticsearch](#syncope-apache-org-docs-4-0-reference-guide--elasticsearch) extension.

Once events are reported, they can be used as input for external tools.

#### [3.14.1. Audit Events](#syncope-apache-org-docs-4-0-reference-guide--audit-events)

The information provided for [notification events](#syncope-apache-org-docs-4-0-reference-guide--notification-events) is also valid for audit events, including examples -
except for the admin console [tooling](#syncope-apache-org-docs-4-0-reference-guide--console-configuration-audit), which is naturally distinct.

#### [3.14.2. Audit Event Processors](#syncope-apache-org-docs-4-0-reference-guide--audit-event-processors)

In addition to default processing, events are also available for custom handling via Audit Event Processors.
This allows to write implementations to route audit events to files, queues, sockets, syslog, etc.

Custom implementations must implement the
[AuditEventProcessor](https://github.com/apache/syncope/blob/syncope-4.0.5/core/provisioning-api/src/main/java/org/apache/syncope/core/provisioning/api/AuditEventProcessor.java)
interface.

### [3.15. Routes](#syncope-apache-org-docs-4-0-reference-guide--routes)

Routes represents the main configuration to instruct [SRA](#syncope-apache-org-docs-4-0-reference-guide--secure-remote-access) to respond to HTTP requests.

Every route is defined by providing the following information:

1. name - unique reference to the current route
2. target - base URI to proxy requests for
3. error URI - where to redirect in case of errors
4. type - `PUBLIC` or `PROTECTED`: the latter requires authentication against the configured Access Manager
5. logout - whether to proceed with logout against the configured Access Manager
6. post-logout URI - where to redirect after logging out
7. CSRF - whether protection against [Cross-Site Request Forgery](https://en.wikipedia.org/wiki/Cross-site_request_forgery)
   shall be applied to incoming requests
8. order - value to sort routes for evaluation
9. predicates - composed condition, supporting logic operators as `AND`, `OR` and `NOT`, to specify if incoming requests
   shall match the owning route
10. filters - ordered list of elements allowing to perform modification of the incoming request and / or outgoing response

![SRA request processing](syncope.apache.org/docs/4.0/images/sra-request.png)

Figure 11. SRA request processing

When an HTTP request is received, SRA evaluates all the configured *predicates*, sorted by their owning *route*'s *order*,
to determine the first matching route among the ones defined.

If the matching route has *type* `PROTECTED`, the configured Access Manager is involved to authorize the request; while
[WA](#syncope-apache-org-docs-4-0-reference-guide--web-access) works out-of-the-box, others can be configured, provided that they implement standard protocols as
OpenID Connect or SAML.

The incoming request is then pre-processed by matching route’s *filters* and sent to the configured *target*.  
The received response, after being post-processed by matching route’s *filters*, is finally returned to the initial caller.

#### [3.15.1. Predicates](#syncope-apache-org-docs-4-0-reference-guide--predicates)

Inside Route definition, each predicate will be referring to some Spring Cloud Gateway’s
[Predicate factory](https://docs.spring.io/spring-cloud-gateway/reference/4.2/spring-cloud-gateway/request-predicates-factories.html):

- `AFTER` matches requests that happen after the specified datetime;
- `BEFORE` matches requests that happen before the specified datetime;
- `BETWEEN` matches requests that happen after first datetime and before second datetime;
- `COOKIE` matches cookies that have the given name and whose values match the regular expression;
- `HEADER` matches with a header that has the given name whose value matches the regular expression;
- `HOST` matches the `Host` header;
- `METHOD` matches the provided HTTP method(s);
- `PATH` matches the request path;
- `QUERY` matches the query string;
- `REMOTE_ADDR` matches the caller IP address;
- `WEIGHT` matches according to the weights provided per group of target URIs;
- `CUSTOM` matches according to a provided class extending
  [CustomRoutePredicateFactory](https://github.com/apache/syncope/blob/syncope-4.0.5/sra/src/main/java/org/apache/syncope/sra/predicates/CustomRoutePredicateFactory.java).

#### [3.15.2. Filters](#syncope-apache-org-docs-4-0-reference-guide--filters)

Inside Route definition, each filter will be referring to some Spring Cloud Gateway’s
[Filter factory](https://docs.spring.io/spring-cloud-gateway/reference/4.2/spring-cloud-gateway/gatewayfilter-factories.html):

- `ADD_REQUEST_HEADER` adds a header to the downstream request’s headers;
- `ADD_REQUEST_PARAMETER` adds a parameter too the downstream request’s query string;
- `ADD_RESPONSE_HEADER` adds a header to the downstream response’s headers;
- `CLIENT_CERTS_TO_REQUEST_HEADER` takes SSL certificates associated with the request to downstream request’s headers;
- `DEDUPE_RESPONSE_HEADER` removes duplicate values of response headers;
- `FALLBACK_HEADERS` after an execution exception occurs, the request is forwarded to a fallback endpoint; the
  headers with the exception type, message and (if available) root cause exception type and message are added to that
  request;
- `LINK_REWRITE` rewrites HTTP links in the response body before it is sent back to the client;
- `MAP_REQUEST_HEADER` creates a new named header with the value extracted out of an existing named header from
  the incoming request;
- `PREFIX_PATH` will prefix a part to the path of the incoming request;
- `PRESERVE_HOST_HEADER` sets a request attribute that the routing filter inspects to determine if the original host
  header should be sent, rather than the host header determined by the HTTP client;
- `PRINCIPAL_TO_REQUEST_HEADER` takes authenticated principal to downstream request’s headers;
- `QUERY_PARAM_TO_REQUEST_HEADER` takes incoming query params to downstream request’s headers;
- `REDIRECT_TO` will send a HTTP status `30x` with a `Location` header to perform a redirect;
- `REMOVE_REQUEST_HEADER` removes a header to the downstream request’s headers;
- `REMOVE_RESPONSE_HEADER` removes a header to the downstream response’s headers;
- `REQUEST_HEADER_TO_REQUEST_URI` changes the request URI by a request header;
- `REQUEST_RATE_LIMITER` determines if the current request is allowed to proceed: if it is not, a HTTP status `429`
  is returned;
- `RETRY` attempts to connect to downstream request’s target for the given number of retries before giving up;
- `REWRITE_PATH` uses regular expressions to rewrite the request path;
- `REWRITE_LOCATION` modifies the value of the `Location` response header;
- `REWRITE_RESPONSE_HEADER` modifies the value of response header;
- `SECURE_HEADERS` adds a number of recommended security headers to the response;
- `SAVE_SESSION` forces to save the current HTTP session before forwarding the call downstream;
- `SET_PATH` manipulates the request path;
- `SET_REQUEST_HEADER` replaces a header to the downstream request’s headers;
- `SET_RESPONSE_HEADER` replaces a header to the downstream response’s headers;
- `SET_STATUS` sets HTTP status to return to caller;
- `SET_REQUEST_SIZE` restricts a request from reaching the downstream service;
- `SET_REQUEST_HOST` sets host header to the downstream request’s headers;
- `STRIP_PREFIX` removes parts from the path of the incoming request;
- `CUSTOM` will manipulate downstream request or response according to a provided class extending
  [CustomGatewayFilterFactory](https://github.com/apache/syncope/blob/syncope-4.0.5/sra/src/main/java/org/apache/syncope/sra/filters/CustomGatewayFilterFactory.java).

### [3.16. Authentication Modules](#syncope-apache-org-docs-4-0-reference-guide--authentication-modules)

Authentication Modules allow to specify how [WA](#syncope-apache-org-docs-4-0-reference-guide--web-access) shall check the provided credentials against specific
technology or repository, in the context of a certain [Authentication Policy](#syncope-apache-org-docs-4-0-reference-guide--policies-authentication).

Several authentication modules are provided:

- Principal Authentication:

  - [Database](https://apereo.github.io/cas/7.2.x/authentication/Database-Authentication.html)
  - [JAAS](https://apereo.github.io/cas/7.2.x/authentication/JAAS-Authentication.html)
  - [LDAP](https://apereo.github.io/cas/7.2.x/authentication/LDAP-Authentication.html)
  - [SPNEGO](https://apereo.github.io/cas/7.2.x/authentication/SPNEGO-Authentication.html)
  - [Syncope](https://apereo.github.io/cas/7.2.x/authentication/Syncope-Authentication.html)
  - [Azure Active Directory](https://apereo.github.io/cas/7.2.x/authentication/Azure-ActiveDirectory-Authentication.html)
  - [Okta](https://apereo.github.io/cas/7.2.x/authentication/Okta-Authentication.html)
  - [X509](https://apereo.github.io/cas/7.2.x/authentication/X509-Authentication.html)
  - [OpenID Connect](https://apereo.github.io/cas/7.2.x/integration/Delegate-Authentication-Generic-OpenID-Connect.html)
  - [OAuth2](https://apereo.github.io/cas/7.2.x/integration/Delegate-Authentication-OAuth20.html)
  - [SAML](https://apereo.github.io/cas/7.2.x/integration/Delegate-Authentication-SAML.htmll)
  - [Apple Signin](https://apereo.github.io/cas/7.2.x/integration/Delegate-Authentication-Apple.html)
  - [Azure Active Directory (OIDC)](https://apereo.github.io/cas/7.2.x/integration/Delegate-Authentication-Azure-AD.html)
  - [Google OpenID](https://apereo.github.io/cas/7.2.x/integration/Delegate-Authentication-Google-OpenID-Connect.html)
  - [Keycloak](https://apereo.github.io/cas/7.2.x/integration/Delegate-Authentication-Keycloak.html)
- MFA:

  - [Duo Security](https://apereo.github.io/cas/7.2.x/mfa/DuoSecurity-Authentication.html)
  - [Google Authenticator](https://apereo.github.io/cas/7.2.x/mfa/GoogleAuthenticator-Authentication.html)

|  | Custom authentication modules can be provided by implementing theAuthModuleConfinterface and extending appropriately theWAPropertySourceLocatorclass. |
| --- | --- |

|  | Authentication Modules are dynamically translated intoCAS Authentication Handlers. |
| --- | --- |

### [3.17. Attribute Repositories](#syncope-apache-org-docs-4-0-reference-guide--attribute-repositories)

Attribute Repositories allow to enrich the profile of an user authenticated by [WA](#syncope-apache-org-docs-4-0-reference-guide--web-access), in the context of a
certain [Attribute Release Policy](#syncope-apache-org-docs-4-0-reference-guide--policies-attribute-release).

Some attribute repositories are provided:

- [Database](https://apereo.github.io/cas/7.2.x/integration/Attribute-Resolution-JDBC.html)
- [LDAP](https://apereo.github.io/cas/7.2.x/integration/Attribute-Resolution-LDAP.html)
- [Stub](https://apereo.github.io/cas/7.2.x/integration/Attribute-Resolution-Stub.html)
- [Syncope](https://apereo.github.io/cas/7.2.x/integration/Attribute-Resolution-Syncope.html)
- [Azure Active Directory](https://apereo.github.io/cas/7.2.x/integration/Attribute-Resolution-AzureAD.html)
- [Okta](https://apereo.github.io/cas/7.2.x/integration/Attribute-Resolution-Okta.html)

|  | Custom authentication modules can be provided by implementing theAttrRepoConfinterface and extending appropriately theWAPropertySourceLocatorclass. |
| --- | --- |

|  | Attribute Repositories are dynamically translated intoCAS Attribute Resolutionconfiguration. |
| --- | --- |

### [3.18. Client Applications](#syncope-apache-org-docs-4-0-reference-guide--client-applications)

Client Applications represent web applications (including [SRA](#syncope-apache-org-docs-4-0-reference-guide--secure-remote-access)) allowed to integrate with
[WA](#syncope-apache-org-docs-4-0-reference-guide--web-access).

Depending on the communication protocol, the following client applications are supported:

- OpenID Connect Relying Party
- SAML 2.0 Service Provider
- CAS Service

When defining a client application, the following parameters shall be specified:

1. id - unique number identifier of the current client application
2. [realm](#syncope-apache-org-docs-4-0-reference-guide--realms) - used to inherit policies
3. name - regular expression to match requests
4. description - optional textual description
5. username attribute provider, mapping to
   [CAS Attribute-based Principal Id](https://apereo.github.io/cas/7.2.x/integration/Attribute-Release-PrincipalId-Attribute.html)
6. [authentication policy](#syncope-apache-org-docs-4-0-reference-guide--policies-authentication)
7. [access policy](#syncope-apache-org-docs-4-0-reference-guide--policies-access)
8. [attribute release policy](#syncope-apache-org-docs-4-0-reference-guide--policies-attribute-release)
9. [ticket expiration policy](#syncope-apache-org-docs-4-0-reference-guide--policies-ticket-expiration)
10. additional properties
11. logout type, mapping to
    [the equivalent CAS setting](https://apereo.github.io/cas/7.2.x/installation/Logout-Single-Signout.html#slo-requests)

More parameters are required to be specified depending on the actual client application type.

|  | Client Applications are dynamically translated intoCAS Services. |
| --- | --- |

### [3.19. Domains](#syncope-apache-org-docs-4-0-reference-guide--domains)

Domains are built to facilitate [multitenancy](https://en.wikipedia.org/wiki/Multitenancy).

Domains allow the physical separation of all data managed by Apache Syncope, by storing the data for different domains
into different database instances. Therefore, Apache Syncope can facilitate Users, Groups, Any Objects,
External Resources, Policies, Tasks, etc. from different domains (e.g. tenants) in a single [Core](#syncope-apache-org-docs-4-0-reference-guide--core) instance.

By default, a single `Master` domain is defined, which also bears the configuration for additional domains.

![Domains](syncope.apache.org/docs/4.0/images/domains.png)

Figure 12. Domains

|  | Each domain’s persistence unit can be configured to work with one of thesupported DBMSes:Mastercan be on MySQL,Domain1on PostgreSQL,DomainNon Oracle and so on. |
| --- | --- |

### [3.20. Implementations](#syncope-apache-org-docs-4-0-reference-guide--implementations)

It is possible to provide implementations suitable for [customization](#syncope-apache-org-docs-4-0-reference-guide--customization-core) as:

1. Java classes
2. [Apache Groovy](http://www.groovy-lang.org/) classes

While the former shows some advantages about execution performance, the latter is extremely useful as it allows for
runtime updates, freeing from the hassle to redeploy when something needs to be changed.

|  | With great power comes great responsibilityCustomizing and extending the Core behavior by uploading a Groovy class via REST adds further flexibility to the platform, allows to speed up the development cycle and can be used as Swiss army knife for maintenance and administration.Please beware that granting the permission to manage Implementations to non-admin users shall be performed with great care. The Groovy code is anyway going to be executed in a sandbox, where the set of forbidden classes and methods can be configured on each deployment.Check the providedgroovy.blacklist.The default Groovy sandbox controls can be tweaked by configuring a local copy ofgroovy.blacklist, which will then have to be referenced by adjusting the value of thesecurity.groovyBlacklistproperty in thecore.propertiesfile. |
| --- | --- |

### [3.21. Extensions](#syncope-apache-org-docs-4-0-reference-guide--extensions)

The *vanilla* Apache Syncope deployment can be optional enriched with useful features via an Extension, instead of bloating
every single deployment with unneeded libraries and configurations.

With reference to [architecture](#syncope-apache-org-docs-4-0-reference-guide--architecture), an extension might add a [REST](#syncope-apache-org-docs-4-0-reference-guide--rest) endpoint, manage the
[persistence](#syncope-apache-org-docs-4-0-reference-guide--persistence) of additional entities, extend the [security](#syncope-apache-org-docs-4-0-reference-guide--security) mechanisms, tweak the
[provisioning layer](#syncope-apache-org-docs-4-0-reference-guide--provisioning-layer), add features to the [Admin UI](#syncope-apache-org-docs-4-0-reference-guide--admin-console-component) or
the [End-user UI](#syncope-apache-org-docs-4-0-reference-guide--enduser-component), or even bring all such things together.

Extensions are available from different sources:

1. as Maven artifacts published from the Apache Syncope codebase, part of the official releases - this is the case of the
   ones detailed below;
2. as Maven artifacts published by third parties;
3. as part of a given deployment source code, as explained [in the following](#syncope-apache-org-docs-4-0-reference-guide--customization-extensions).

#### [3.21.1. SAML 2.0 Service Provider for UI](#syncope-apache-org-docs-4-0-reference-guide--saml2sp4ui)

This extension can be leveraged to provide
[SAML 2.0](https://en.wikipedia.org/wiki/Security_Assertion_Markup_Language)-based
[Single Sign-On](https://en.wikipedia.org/wiki/Single_sign-on) access to the [Admin UI](#syncope-apache-org-docs-4-0-reference-guide--admin-console-component),
the [End-user UI](#syncope-apache-org-docs-4-0-reference-guide--enduser-component) or any other Java application dealing with the [Core](#syncope-apache-org-docs-4-0-reference-guide--core).

Once installed, one or more [Identity Providers](https://en.wikipedia.org/wiki/Identity_provider) can be imported from
their [metadata](https://en.wikipedia.org/wiki/SAML_2.0#SAML_2.0_Metadata).
For each Identity Provider, it is to configure which one of the attributes - returned as part of the assertion
containing the attribute statements - is going to be used by Syncope to match the internal users.

|  | Extension SourcesThe source code of this extension is available from the Apache Syncopesource tree. |
| --- | --- |

|  | This extension adds features to all components and layers that are available, and can be taken as reference when creatingnew extensions. |
| --- | --- |

#### [3.21.2. OpenID Connect Client for UI](#syncope-apache-org-docs-4-0-reference-guide--oidcc4ui)

This extension can be leveraged to provide [OpenID Connect](http://openid.net/connect/)-based
[Single Sign-On](https://en.wikipedia.org/wiki/Single_sign-on) access to the [Admin UI](#syncope-apache-org-docs-4-0-reference-guide--admin-console-component),
the [End-user UI](#syncope-apache-org-docs-4-0-reference-guide--enduser-component) or any other Java application dealing with the [Core](#syncope-apache-org-docs-4-0-reference-guide--core).

Once installed, one or more OpenID Providers can be created either from
the [discovery document](http://openid.net/specs/openid-connect-discovery-1_0.html) if it is supported or from inserting
manually the required attributes, in any case the `client_id` and the `client_secret` from the OAuth 2.0 credential and the issuer
are required.
After configuring the OpenID provider, the [Authorization Code Flow](http://openid.net/specs/openid-connect-core-1_0.html#CodeFlowAuth)
is going to be implemented in order to reach the user information to be used by Syncope to match the internal users.

|  | Extension SourcesThe source code of this extension is available from the Apache Syncopesource tree. |
| --- | --- |

|  | This extension adds features to all components and layers that are available, and can be taken as reference when creatingnew extensions. |
| --- | --- |

#### [3.21.3. Elasticsearch](#syncope-apache-org-docs-4-0-reference-guide--elasticsearch)

This extension provides an alternate internal search engine for [Users, Groups and Any Objects](#syncope-apache-org-docs-4-0-reference-guide--users-groups-and-any-objects),[Realms](#syncope-apache-org-docs-4-0-reference-guide--realms) and
[Audit Events](#syncope-apache-org-docs-4-0-reference-guide--audit-events), requiring an external [Elasticsearch](https://www.elastic.co/) cluster.

|  | This extension supports Elasticsearch server versions starting from 8.x. |
| --- | --- |

|  | As search operations are central for different aspects of theprovisioning process, the global performance is expected to improve when using this extension. |
| --- | --- |

|  | Extension SourcesThe source code of this extension is available from the Apache Syncopesource tree. |
| --- | --- |

#### [3.21.4. OpenSearch](#syncope-apache-org-docs-4-0-reference-guide--opensearch)

This extension provides an alternate internal search engine for [Users, Groups and Any Objects](#syncope-apache-org-docs-4-0-reference-guide--users-groups-and-any-objects),[Realms](#syncope-apache-org-docs-4-0-reference-guide--realms) and
[Audit Events](#syncope-apache-org-docs-4-0-reference-guide--audit-events), requiring an external [OpenSearch](https://opensearch.org/) cluster.

|  | As search operations are central for different aspects of theprovisioning process, the global performance is expected to improve when using this extension. |
| --- | --- |

|  | Extension SourcesThe source code of this extension is available from the Apache Syncopesource tree. |
| --- | --- |

#### [3.21.5. SCIM](#syncope-apache-org-docs-4-0-reference-guide--scim)

[SCIM](http://www.simplecloud.info/) (System for Cross-domain Identity Management) 2.0 is the open API for managing
identities, published under the IETF:

1. [Definitions, Overview, Concepts, and Requirements](https://tools.ietf.org/html/rfc7642)
2. [Core Schema](https://tools.ietf.org/html/rfc7643)
3. [Protocol](https://tools.ietf.org/html/rfc7644)

This extension enables an additional `/scim` REST endpoint, implementing the communication according to the SCIM 2.0
standard, in order to provision User, Enterprise User and Group SCIM entities to Apache Syncope.

|  | Extension SourcesThe source code of this extension is available from the Apache Syncopesource tree. |
| --- | --- |

#### [3.21.6. OpenFGA](#syncope-apache-org-docs-4-0-reference-guide--openfga)

This extension provides seamless integration with an [OpenFGA](https://openfga.dev/) server.

When this extension is enabled:

- all [AnyType](#syncope-apache-org-docs-4-0-reference-guide--anytype) and [RelationshipType](#syncope-apache-org-docs-4-0-reference-guide--relationshiptype) instances are transparently mirrored to
  OpenFGA’s [authorization model](https://openfga.dev/docs/concepts#what-is-an-authorization-model)
- all [Users, Groups and Any Objects](#syncope-apache-org-docs-4-0-reference-guide--users-groups-and-any-objects) and their [Memberships and Relationships](#syncope-apache-org-docs-4-0-reference-guide--memberships-relationships) are transparently mirrored as
  OpenFGA’s [tuple](https://openfga.dev/docs/concepts#what-is-a-relationship-tuple) objects

|  | Extension SourcesThe source code of this extension is available from the Apache Syncopesource tree. |
| --- | --- |

## [4. Usage](#syncope-apache-org-docs-4-0-reference-guide--usage)

Before proceeding, please ensure that you have access to a running Apache Syncope deployment.
You can take a look at the
[Apache Syncope Getting Started Guide](#syncope-apache-org-docs-4-0-getting-started)
to check system requirements and to choose among the various options for obtaining Apache Syncope.

### [4.1. Admin Console](#syncope-apache-org-docs-4-0-reference-guide--admin-console)

Once the deployment is ready, the admin console can be accessed at:

```text
protocol://host:port/syncope-console/
```

where `protocol`, `host` and `port` reflect your deployment.

You should be greeted by the following web page.

![console-login](syncope.apache.org/docs/4.0/images/consoleLogin.png)

You can use the [default admin credentials](#syncope-apache-org-docs-4-0-reference-guide--set-admin-credentials) to login.

#### [4.1.1. Accessibility](#syncope-apache-org-docs-4-0-reference-guide--admin-console-accessibility)

The Admin UI is accessible to the visually impaired.

The `H` [accesskey](https://developer.mozilla.org/en-US/docs/Web/HTML/Global_attributes/accesskey) shortcut can
be used to easily toggle "High contrast mode" by using the keyboard.
In this mode, the website colors are switched to a higher contrast color schema.

E.g.

| Shortcut | Purpose |
| --- | --- |
| Alt+Shift+H | Toggle "High contrast mode" on Firefox and Chrome browsers on Linux |

The `F` [accesskey](https://developer.mozilla.org/en-US/docs/Web/HTML/Global_attributes/accesskey) shortcut can
be used to easily toggle "Increased font mode" by using the keyboard.
In this mode, the website font size is increased.

E.g.

| Shortcut | Purpose |
| --- | --- |
| Alt+Shift+F | Toggle "Increased font mode" on Firefox and Chrome browsers on Linux |

#### [4.1.2. Pages](#syncope-apache-org-docs-4-0-reference-guide--pages)

##### Dashboard

The dashboard provides an overall view of the current state of the Apache Syncope deployment. It
consists of various widgets and tabs that show the different metrics and details of each component that is available.

![console-dashboard](syncope.apache.org/docs/4.0/images/consoleDashboard.png)

##### Realms

The realms page provides the designated administrators with the power to manage [Realms](#syncope-apache-org-docs-4-0-reference-guide--realms) as well as
[Users, Groups and Any Objects](#syncope-apache-org-docs-4-0-reference-guide--users-groups-and-any-objects), for all [any types](#syncope-apache-org-docs-4-0-reference-guide--anytype) that are defined.

![console-realms-user](syncope.apache.org/docs/4.0/images/realmsUser.png)

##### Engagements

From the engagements page it is possible to administer [scheduled tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-scheduled), [commands](#syncope-apache-org-docs-4-0-reference-guide--commands) and
[macros](#syncope-apache-org-docs-4-0-reference-guide--tasks-macro).

![console-engagements](syncope.apache.org/docs/4.0/images/engagements.png)

##### Reports

The reports page presents the designated administrators with the list of [reports](#syncope-apache-org-docs-4-0-reference-guide--reports) configured on the given
deployment.

![console-reports](syncope.apache.org/docs/4.0/images/consoleReports.png)

##### Topology

The topology page provides a mapped view of the [connectors](#syncope-apache-org-docs-4-0-reference-guide--connector-instance-details) and
[external resources](#syncope-apache-org-docs-4-0-reference-guide--external-resource-details) that are available and configured in the given deployment.  
Different actions are available when clicking on the various nodes.

![console-topology](syncope.apache.org/docs/4.0/images/consoleTopology.png)

##### SRA

From the SRA page it is possible to manage the [routes](#syncope-apache-org-docs-4-0-reference-guide--routes) served and to immediately deploy the updated
configuration.

![console-sra](syncope.apache.org/docs/4.0/images/sra.png)

##### WA

The WA page allows to manage [authentication modules](#syncope-apache-org-docs-4-0-reference-guide--authentication-modules),
[client applications](#syncope-apache-org-docs-4-0-reference-guide--client-applications) and other access management features, and to immediately deploy the updated
configuration.

![console-wa](syncope.apache.org/docs/4.0/images/wa.png)

##### Keymaster

###### Domains

Allows for [domain](#syncope-apache-org-docs-4-0-reference-guide--domains) management.

![keymaster domains](syncope.apache.org/docs/4.0/images/keymaster_domains.png)

###### Network Services

Displays the components as registered in the configured [keymaster](#syncope-apache-org-docs-4-0-reference-guide--keymaster) instance.

![keymaster networkservices](syncope.apache.org/docs/4.0/images/keymaster_networkservices.png)

###### Parameters

Presents the administrators with the list of defined [configuration parameters](#syncope-apache-org-docs-4-0-reference-guide--configuration-parameters) used in the
given deployment such as `token.expireTime` and `password.cipher.algorithm`.
These can be edited to further customize the deployment.  
New parameters can also be added, for use with custom code.

![keymaster parameters](syncope.apache.org/docs/4.0/images/keymaster_parameters.png)

##### Configuration

The configuration pages allow the designated administrators to customize the given deployment to fit the needs of the
organization.

- Audit

  Controls the configuration of the [auditing](#syncope-apache-org-docs-4-0-reference-guide--audit) features.

- Implementations

  Allows the administrators to manage [implementations](#syncope-apache-org-docs-4-0-reference-guide--implementations).

- Logs

  The logging levels available can be dynamically adjusted; for example, the admin can set it
  to display only the errors of `io.swagger`, in which case the warning and information logs will not be reported.

- Notifications

  Gives access to the [notification](#syncope-apache-org-docs-4-0-reference-guide--notifications) management.  
  This page also allows the administrators to create and edit [notification templates](#syncope-apache-org-docs-4-0-reference-guide--notification-templates).

- Policies

  Allows the administrators to manage all available type of [policies](#syncope-apache-org-docs-4-0-reference-guide--policies).

- Security

  Displays and provides editing functionality for the security aspects, including [roles](#syncope-apache-org-docs-4-0-reference-guide--roles),
  [delegations](#syncope-apache-org-docs-4-0-reference-guide--delegation) and [security questions](#syncope-apache-org-docs-4-0-reference-guide--security-questions).

- Types

  Entry point for [type management](#syncope-apache-org-docs-4-0-reference-guide--type-management).

##### Extensions

The [extensions](#syncope-apache-org-docs-4-0-reference-guide--extensions) configured for the given deployment are dynamically reported in the navigation menu: each
extension generally produces one or more pages and makes one or more widgets available in the [dashboard](#syncope-apache-org-docs-4-0-reference-guide--dashboard).

##### Approval

The images below refer to the self-registration approval [sample](#syncope-apache-org-docs-4-0-reference-guide--sample-selfreg-approval) and to the typical approval
flow as explained [above](#syncope-apache-org-docs-4-0-reference-guide--approval).

![Approval notification](syncope.apache.org/docs/4.0/images/approval1.png)

Figure 13. Approval notification

![Claiming an approval](syncope.apache.org/docs/4.0/images/approval2.png)

Figure 14. Claiming an approval

![Managing an approval](syncope.apache.org/docs/4.0/images/approval3.png)

Figure 15. Managing an approval

![Approval form](syncope.apache.org/docs/4.0/images/approval4.png)

Figure 16. Approval form

![Reviewing modifications](syncope.apache.org/docs/4.0/images/approval5.png)

Figure 17. Reviewing modifications

![Approving modifications](syncope.apache.org/docs/4.0/images/approval6.png)

Figure 18. Approving modifications

##### User Requests

User requests are managed exactly in the same way how [approvals](#syncope-apache-org-docs-4-0-reference-guide--console-approval) are managed: check the
typical request management flow as explained [above](#syncope-apache-org-docs-4-0-reference-guide--request-management).

### [4.2. Enduser Application](#syncope-apache-org-docs-4-0-reference-guide--enduser-application)

Once the deployment is ready, the enduser application can be accessed at:

```text
protocol://host:port/syncope-enduser/
```

where `protocol`, `host` and `port` reflect your deployment.

The scope of the enduser application is primarily to provide a dedicated web-based entry-point for self-registration,
self-service and [password reset](#syncope-apache-org-docs-4-0-reference-guide--password-reset).

![enduser-login](syncope.apache.org/docs/4.0/images/enduserLogin.png)

#### [4.2.1. Accessibility](#syncope-apache-org-docs-4-0-reference-guide--enduser-accessibility)

The End-user UI is accessible to the visually impaired.

Two icons are present in the main page, in the right corner:

![Enduser accessibility icons](syncope.apache.org/docs/4.0/images/accessibility-enduser01.png)

Figure 19. Enduser accessibility icons

By clicking the top right corner icon ![Accessibility HC mode](syncope.apache.org/docs/4.0/images/accessibility-icon01.png) it is possible to
toggle the "High contrast mode".
In this mode, the website colors are switched to a higher contrast color schema.

|  | TheHaccesskeyshortcut can be used to easily toggle "High contrast mode" by using the keyboard.E.g.ShortcutPurposeAlt+Shift+HToggle "High contrast mode" on Firefox and Chrome browsers on Linux | Shortcut | Purpose | Alt+Shift+H | Toggle "High contrast mode" on Firefox and Chrome browsers on Linux |
| --- | --- | --- | --- | --- | --- |
| Shortcut | Purpose |
| Alt+Shift+H | Toggle "High contrast mode" on Firefox and Chrome browsers on Linux |

By clicking the second icon ![Accessibility Increased Font mode](syncope.apache.org/docs/4.0/images/accessibility-icon02.png) it is possible
to toggle the "Increased font mode".
In this mode, the website font size is increased.

|  | TheFaccesskeyshortcut can be used to easily toggle "Increased font mode" by using the keyboard.E.g.ShortcutPurposeAlt+Shift+FToggle "Increased font mode" on Firefox and Chrome browsers on Linux | Shortcut | Purpose | Alt+Shift+F | Toggle "Increased font mode" on Firefox and Chrome browsers on Linux |
| --- | --- | --- | --- | --- | --- |
| Shortcut | Purpose |
| Alt+Shift+F | Toggle "Increased font mode" on Firefox and Chrome browsers on Linux |

To reset to the default mode, it is enough to click again on the specific icon.

#### [4.2.2. Pages](#syncope-apache-org-docs-4-0-reference-guide--pages-2)

##### Home

The Home page provides a welcome page for logged-in users.

![enduser-home](syncope.apache.org/docs/4.0/images/enduserHome.png)

##### Personal Information

![enduser-edit-profile](syncope.apache.org/docs/4.0/images/enduserEditProfile.png)

![enduser-change-password](syncope.apache.org/docs/4.0/images/enduserChangePassword.png)

![enduser-security-question](syncope.apache.org/docs/4.0/images/enduserSecurityQuestion.png)

##### User Requests

The images below refer to the printer assignment [sample](#syncope-apache-org-docs-4-0-reference-guide--sample-user-request) and to the typical request management
flow as explained [above](#syncope-apache-org-docs-4-0-reference-guide--request-management).

![Initial situation: no active requests](syncope.apache.org/docs/4.0/images/enduser_userrequests_none.png)

Figure 20. Initial situation: no active requests

![Starting new request](syncope.apache.org/docs/4.0/images/enduser_userrequests_start.png)

Figure 21. Starting new request

![Filling request form](syncope.apache.org/docs/4.0/images/enduser_userrequests_started.png)

Figure 22. Filling request form

After submit, the request is ready to be [managed](#syncope-apache-org-docs-4-0-reference-guide--console-user-requests) by the configured administrators.

##### Password Reset

![Password reset](syncope.apache.org/docs/4.0/images/passwordreset.png)

Figure 23. Password reset

### [4.3. Core](#syncope-apache-org-docs-4-0-reference-guide--core-usage)

All the features provided by the [Core](#syncope-apache-org-docs-4-0-reference-guide--core) are available as RESTful services.

The base URL for invoking such services is normally set as

```text
protocol://host:port/syncope/rest/
```

where `protocol`, `host` and `port` reflect your deployment.

|  | REST ReferenceA complete REST reference generated fromOpenAPI specification 3.0ispublishedas well as made available with each deployment atprotocol://host:port/syncope/rest/openapi.jsonwhereprotocol,hostandportreflect your deployment.REST APIs are available to visualize and interact viaSwagger UIatprotocol://host:port/syncope/ |
| --- | --- |

#### [4.3.1. REST Authentication and Authorization](#syncope-apache-org-docs-4-0-reference-guide--rest-authentication-and-authorization)

The [Core](#syncope-apache-org-docs-4-0-reference-guide--core) authentication and authorization is based on [Spring Security](https://spring.io/projects/spring-security).

As an initial step, authentication is required to obtain, in the `X-Syncope-Token` HTTP header, the
unique signed [JSON Web Token](https://en.wikipedia.org/wiki/JSON_Web_Token) to include in all subsequent requests.

By providing the token received in the initial exchange, the requester can be identified and checked for authorization,
based on owned [entitlements](#syncope-apache-org-docs-4-0-reference-guide--entitlements).

|  | Users can examine their own entitlements looking at theX-Syncope-Entitlementsheader value. |
| --- | --- |

|  | The relevant security configuration lies inWebSecurityContext; while normally not needed, this configuration can be anywaycustomized.HTTP Basic Authenticationis set for use by default. |
| --- | --- |

##### [JWTSSOProvider](#syncope-apache-org-docs-4-0-reference-guide--jwtssoprovider)

Besides validating and accepting the JSON Web Tokens generated during the authentication process as sketched above,
Apache Syncope can be enabled to cope with tokens generated by third parties, by providing implementations of the
[JWTSSOProvider](https://github.com/apache/syncope/blob/syncope-4.0.5/core/spring/src/main/java/org/apache/syncope/core/spring/security/JWTSSOProvider.java)
interface.

Authorization Summary

The set of RESTful services provided by Apache Syncope can be divided as:

1. endpoints accessible without any sort of authentication (e.g. truly anonymous), for self-registration and
   [password reset](#syncope-apache-org-docs-4-0-reference-guide--password-reset);
2. endpoints disclosing information about the given Syncope deployment (available [schema](#syncope-apache-org-docs-4-0-reference-guide--schema), configured
   [extensions](#syncope-apache-org-docs-4-0-reference-guide--extensions), Groups, …​), requiring some sort of shared authentication defined by the
   `anonymousKey` value in the `security.properties` file - for more information, read about Spring Security’s
   [Anonymous Authentication](https://docs.spring.io/spring-security/reference/6.4/servlet/authentication/anonymous.html#page-title);
3. endpoints for self-service (self-update, password change, …​), requiring user authentication and no entitlements;
4. endpoints for administrative operations, requiring user authentication with authorization granted by the related
   [entitlements](#syncope-apache-org-docs-4-0-reference-guide--entitlements), handed over to users via [roles](#syncope-apache-org-docs-4-0-reference-guide--roles).

#### [4.3.2. REST Headers](#syncope-apache-org-docs-4-0-reference-guide--rest-headers)

Apache Syncope supports a number of HTTP headers as detailed below, in addition to the common HTTP headers such as
`Accept`, `Content-Type`, etc.

|  | It is possible to deal with the headers below when using theClient Libraryvia theSyncopeClientclass methods. |
| --- | --- |

##### [X-Syncope-Token](#syncope-apache-org-docs-4-0-reference-guide--x-syncope-token)

`X-Syncope-Token` is returned on response to [successful authentication](#syncope-apache-org-docs-4-0-reference-guide--rest-authentication-and-authorization), and
contains the unique signed [JSON Web Token](https://en.wikipedia.org/wiki/JSON_Web_Token) identifying the authenticated
user.

The value returned for the `X-Syncope-Token` header must be included in all subsequent requests, in order for the
requester to be checked for authorization, as part of the standard [Bearer](https://tools.ietf.org/html/rfc6750)
`Authorization` header.

Example 11. Obtaining JWT with [curl](http://curl.haxx.se/)

```bash
curl -I -u admin:password -X POST http://localhost:9080/syncope/rest/accessTokens/login
```

returns

```text
HTTP/1.1 204
X-Syncope-Token: eyJ0e..
```

which can then be used to make a call to the REST API

```bash
curl -I -H "Authorization: Bearer eyJ0e.." http://localhost:9080/syncope/rest/users/self
```

The token duration can be configured via the `jwt.lifetime.minutes` property - see
[below](#syncope-apache-org-docs-4-0-reference-guide--configuration-parameters) for details.

##### [X-Syncope-Domain](#syncope-apache-org-docs-4-0-reference-guide--x-syncope-domain)

`X-Syncope-Domain` can be optionally set for requests (when not set, `Master` is assumed) to select the target
[domain](#syncope-apache-org-docs-4-0-reference-guide--domains).  
The value for this header is provided in all responses.

##### [X-Syncope-Key and Location](#syncope-apache-org-docs-4-0-reference-guide--x-syncope-key-and-location)

When creating an entity (User, Group, Schema, External Resource, …​) these two headers are populated respectively with
the entity key (which may be auto-generated) and the absolute URI identifying the new REST resource.

##### [X-Application-Error-Code and X-Application-Error-Info](#syncope-apache-org-docs-4-0-reference-guide--x-application-error-code-and-x-application-error-info)

If the requested operation is in error, `X-Application-Error-Code` will contain the error code (mostly from
[ClientExceptionType](https://github.com/apache/syncope/blob/syncope-4.0.5/common/idrepo/lib/src/main/java/org/apache/syncope/common/lib/types/ClientExceptionType.java))
and `X-Application-Error-Info` might be optionally populated with more details, if available.

##### [X-Syncope-Delegated-By](#syncope-apache-org-docs-4-0-reference-guide--x-syncope-delegated-by)

When requesting an operation under [Delegation](#syncope-apache-org-docs-4-0-reference-guide--delegation), this header must be provided to indicate the delegating
User, either by their username or key.

##### [X-Syncope-Null-Priority-Async](#syncope-apache-org-docs-4-0-reference-guide--x-syncope-null-priority-async)

When set to `true`, this request header instructs the [propagation process](#syncope-apache-org-docs-4-0-reference-guide--propagation) not to wait for completion
when communicating with [External Resources](#syncope-apache-org-docs-4-0-reference-guide--external-resource-details) with no priority set.

##### [Prefer and Preference-Applied](#syncope-apache-org-docs-4-0-reference-guide--prefer-and-preference-applied)

Some REST endpoints allow the clients to request certain behavior; this is done via the `Prefer` header.

When `Prefer` is specified in the request, the response will feature the `Preference-Applied` header, with value set
to the effective preference applied.

###### [return-content / return-no-content](#syncope-apache-org-docs-4-0-reference-guide--return-content-return-no-content)

REST endpoints for creating, updating or deleting Users, Groups or Any Objects return the entity in the response payload
by default.  
If this is not required, the `Prefer` request header can be set to `return-no-content` (`return-content` will instead
keep the default behavior).

|  | UsePrefer: return-no-contentin scenarios where it is important to avoid unnecessary data in the response payload. |
| --- | --- |

###### [respond-async](#syncope-apache-org-docs-4-0-reference-guide--respond-async)

The [Batch](#syncope-apache-org-docs-4-0-reference-guide--batch) endpoint can be requested for [asynchronous processing](#syncope-apache-org-docs-4-0-reference-guide--asynchronous-batch-processing).

##### [ETag, If-Match and If-None-Match](#syncope-apache-org-docs-4-0-reference-guide--etag-if-match-and-if-none-match)

For each response containing Users, Groups or Any Objects, the [ETag](https://en.wikipedia.org/wiki/HTTP_ETag) header is
generated, which contains the latest modification date.

This value can be passed, during subsequent requests to modify the same entity, via the `If-Match` or
`If-None-Match` headers.  
When the provided `If-Match` value does not match the latest modification date of the entity, an error is reported and
the requested operation is not performed.

|  | The combined usage ofETagandIf-Matchcan be enforced to implement optimistic concurrency control over Users, Groups and Any Objects operations. |
| --- | --- |

##### [X-Syncope-Entitlements](#syncope-apache-org-docs-4-0-reference-guide--x-syncope-entitlements)

When invoking the REST endpoint `/users/self` in `GET`, the `X-Syncope-Entitlements` response header will list all
the [entitlements](#syncope-apache-org-docs-4-0-reference-guide--entitlements) owned by the requesting user.

##### [X-Syncope-Delegations](#syncope-apache-org-docs-4-0-reference-guide--x-syncope-delegations)

When invoking the REST endpoint `/users/self` in `GET`, the `X-Syncope-Delegations` response header will list all
delegating users for each [Delegation](#syncope-apache-org-docs-4-0-reference-guide--delegation) for which the requesting user is delegated.

#### [4.3.3. Batch](#syncope-apache-org-docs-4-0-reference-guide--batch)

Batch requests allow grouping multiple operations into a single HTTP request payload.  
A batch request is represented as a [Multipart MIME v1.0 message](https://tools.ietf.org/html/rfc2046), a standard format
allowing the representation of multiple parts, each of which may have a different content type (currently
JSON, YAML or XML), within a single request.

Batch requests are handled by the `/batch` REST endpoint: via HTTP `POST` method to submit requests, via HTTP `GET`
method to fetch responses [asynchronously](#syncope-apache-org-docs-4-0-reference-guide--asynchronous-batch-processing).

|  | The specification and implementation of batch processing in Apache Syncope is inspired by the standards defined byOData 4.0 |
| --- | --- |

##### [Batch requests](#syncope-apache-org-docs-4-0-reference-guide--batch-requests)

The batch request must contain a `Content-Type` header specifying a content type of `multipart/mixed` and a boundary
specification as defined in [RFC2046](https://tools.ietf.org/html/rfc2046).

The body of a batch request is made up of a series of individual requests, each represented as a distinct MIME part
(i.e. separated by the boundary defined in the `Content-Type` header).

Core will process the requests within a batch request sequentially.

An individual request must include a `Content-Type` header with value `application/http` and a
`Content-Transfer-Encoding` header with value `binary`.

Example 12. Sample batch request

```sql
--batch_61bfef8d-0a00-41aa-b775-7b6efff37652 (1)
Content-Type: application/http
Content-Transfer-Encoding: binary
^M (2)
POST /users HTTP/1.1 (3)
Accept: application/json
Content-Length: 1157
Content-Type: application/json
^M
{"@class":"org.apache.syncope.common.lib.to.UserTO","key":null,"type":"USER","realm":"/"}
--batch_61bfef8d-0a00-41aa-b775-7b6efff37652
Content-Type: application/http
Content-Transfer-Encoding: binary
^M
POST /groups HTTP/1.1 (4)
Accept: application/xml
Content-Length: 628
Content-Type: application/xml
^M
<?xml version="1.0" encoding="UTF-8" standalone="yes"?><syncope30:group xmlns:syncope30="https://syncope.apache.org/3.0">
</syncope30:group>
--batch_61bfef8d-0a00-41aa-b775-7b6efff37652
Content-Type: application/http
Content-Transfer-Encoding: binary
^M
PATCH /users/24eb15aebatch@syncope.apache.org HTTP/1.1 (5)
Accept: application/json
Content-Length: 362
Content-Type: application/json
Prefer: return-no-content
^M
{"@class":"org.apache.syncope.common.lib.request.UserUR","key":"24eb15aebatch@syncope.apache.org"}
--batch_61bfef8d-0a00-41aa-b775-7b6efff37652
Content-Type: application/http
Content-Transfer-Encoding: binary
^M
DELETE /groups/287ede7c-98eb-44e8-979d-8777fa077e12 HTTP/1.1 (6)
--batch_61bfef8d-0a00-41aa-b775-7b6efff37652--
```

| 1 | message boundary |
| --- | --- |
| 2 | represents CR LF |
| 3 | user create, with JSON payload (shortened) |
| 4 | group create, with XML payload (shortened) |
| 5 | user update, with JSON payload (shortened) |
| 6 | group delete |

##### [Batch responses](#syncope-apache-org-docs-4-0-reference-guide--batch-responses)

Requests within a batch are evaluated according to the same semantics used when the request appears outside the context
of a batch.

The order of individual requests in a batch request is significant.

If the set of request headers of a batch request are valid (the `Content-Type` is set to `multipart/mixed`, etc.)
Core will return a `200 OK` HTTP response code to indicate that the request was accepted for processing, and the
related execution results.

If Core receives a batch request with an invalid set of headers it will return a `400 Bad Request` code and perform no
further processing of the request.

A response to a batch request must contain a `Content-Type` header with value `multipart/mixed`.

Structurally, a batch response body must match one-to-one with the corresponding batch request body, such that the same
multipart MIME message structure defined for requests is used for responses

Example 13. Sample batch response

```text
--batch_61bfef8d-0a00-41aa-b775-7b6efff37652 (1)
Content-Type: application/http
Content-Transfer-Encoding: binary
^M (2)
HTTP/1.1 201 Created (3)
Content-Type: application/json
Date: Thu, 09 Aug 2018 09:55:46 GMT
ETag: "1533808545975"
Location: http://localhost:9080/syncope/rest/users/d399ba84-12e3-43d0-99ba-8412e303d083
X-Syncope-Domain: Master
X-Syncope-Key: d399ba84-12e3-43d0-99ba-8412e303d083
^M
{"entity":{"@class":"org.apache.syncope.common.lib.to.UserTO"}
--batch_61bfef8d-0a00-41aa-b775-7b6efff37652
Content-Type: application/http
Content-Transfer-Encoding: binary
^M
HTTP/1.1 201 Created (4)
Content-Type: application/xml
Date: Thu, 09 Aug 2018 09:55:46 GMT
ETag: "1533808546342"
Location: http://localhost:9080/syncope/rest/groups/843b2fc3-b8a8-4a8b-bb2f-c3b8a87a8b2e
X-Syncope-Domain: Master
X-Syncope-Key: 843b2fc3-b8a8-4a8b-bb2f-c3b8a87a8b2e
^M
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<syncope30:provisioningResult xmlns:syncope30="https://syncope.apache.org/3.0"></syncope30:provisioningResult>
--batch_61bfef8d-0a00-41aa-b775-7b6efff37652
Content-Type: application/http
Content-Transfer-Encoding: binary
^M
HTTP/1.1 204 No Content (5)
Content-Length: 0
Date: Thu, 09 Aug 2018 09:55:47 GMT
Preference-Applied: return-no-content
X-Syncope-Domain: Master
^M
--batch_61bfef8d-0a00-41aa-b775-7b6efff37652
Content-Type: application/http
Content-Transfer-Encoding: binary
^M
HTTP/1.1 200 OK (6)
Content-Type: application/json
Date: Thu, 09 Aug 2018 09:55:47 GMT
X-Syncope-Domain: Master
^M
{"entity":{"@class":"org.apache.syncope.common.lib.to.GroupTO"}
--batch_61bfef8d-0a00-41aa-b775-7b6efff37652--
```

| 1 | message boundary (same as request) |
| --- | --- |
| 2 | represents CR LF |
| 3 | user create response, with JSON payload (shortened) |
| 4 | group create respose, with XML payload (shortened) |
| 5 | user update, no content asPrefer: return-no-contentwas specified |
| 6 | group delete response, with JSON payload (shortened) |

##### [Asynchronous Batch Processing](#syncope-apache-org-docs-4-0-reference-guide--asynchronous-batch-processing)

Batch requests may be executed asynchronously by [including](#syncope-apache-org-docs-4-0-reference-guide--respond-async) the `respond-async` preference in the
`Prefer` header.

Core will return an empty response, with status `202 Accepted`.

Clients can poll the `/batch` endpoint in `GET` by passing the same boundary used for request: if `202 Accepted` is
returned, then the request is still under processing; otherwise, `200 OK` will be returned, along with the full batch
response.  
Once retrieved, the batch response is not available any more from the `/batch` endpoint.

#### [4.3.4. Search](#syncope-apache-org-docs-4-0-reference-guide--search)

It is possible to search for Users, Groups and Any Objects matching a set of given conditions expressed through
[FIQL](https://cxf.apache.org/docs/jax-rs-search.html#JAX-RSSearch-FeedItemQueryLanguage).

The [Feed Item Query Language](https://tools.ietf.org/html/draft-nottingham-atompub-fiql-00) (FIQL, pronounced “fickle”)
is a simple but flexible, URI-friendly syntax for expressing filters across the entries in a syndicated feed.

The FIQL queries can be passed (among other parameters) to the search endpoints available, e.g.

- `GET /users?fiql=query`
- `GET /groups?fiql=query`
- `GET /anyObjects?fiql=query`
- `GET /resources/{resource}/{anytype}?fiql=query`

where:

- `query` is an URL-encoded string representation of the given FIQL query, as in the following examples;
- `resource` is one of defined [external resources](#syncope-apache-org-docs-4-0-reference-guide--external-resources);
- `anytype` is one of defined [any types](#syncope-apache-org-docs-4-0-reference-guide--anytype).

Example 14. Simple attribute match

```properties
username==rossini
```

Example 15. Wildcard attribute match

```properties
username==*ini
```

Example 16. Case-insensitive attribute match

```properties
username=~rOsSiNi
```

Example 17. Case-insensitive wildcard attribute match

```properties
username=~*iNi
```

Example 18. Null attribute match

```properties
loginDate==$null
```

Example 19. Date attribute comparison

```properties
lastLoginDate=ge=2016-03-02 15:21:22
```

Example 20. Auxiliary Any Type class assignment

```text
$auxClasses==csv
```

Example 21. Resource assignment match

```text
$resources==resource-ldap
```

Example 22. Group membership match (only for Users and Any Objects)

```text
$groups==root
```

Example 23. Wildcard group membership match (only for Users and Any Objects)

```text
$groups==*child
```

Example 24. Role membership match (only for Users)

```text
$roles==Other
```

Example 25. Relationship type match (only for Users and Any Objects)

```text
$relationshipTypes==neighborhood
```

Example 26. Relationship match (only for Users and Any Objects)

```text
$relationships==Canon MF 8030c
```

Example 27. Type match (only for Any Objects)

```text
$type==PRINTER
```

Example 28. Complex match (featuring logical AND and OR)

```text
username=~*iNi;(loginDate==$null,$roles==Other)
```

##### [Sorting Search Results](#syncope-apache-org-docs-4-0-reference-guide--sorting-search-results)

Search results can be requested for sorting by passing the optional `orderBy` query parameter to the search endpoints
available, e.g.

- `GET /users?fiql=query&orderBy=sort`
- `GET /groups?fiql=query&orderBy=sort`
- `GET /anyObjects?fiql=query&orderBy=sort`
- `GET /resources/{resource}/{anytype}?orderBy=sort`

where `sort` is an URL-encoded string representation of the sort request, as in the following examples.

Example 29. Single attribute sort, default direction (`ASC`)

```text
username
```

Example 30. Single attribute sort, with direction

```text
username DESC
```

Example 31. Multiple attribute sort, with directions

```text
email DESC, username ASC
```

#### [4.3.5. JEXL support](#syncope-apache-org-docs-4-0-reference-guide--jexl)

[Apache Commons JEXL](https://commons.apache.org/proper/commons-jexl/) is supported as a mean to implement templating
and dynamic value calculation.

Besides [standard syntax](https://commons.apache.org/proper/commons-jexl/reference/syntax.html), the following additional
functions are defined:

- `syncope:fullPath2Dn(fullPath, attr)` - converts full path into the equivalent DN; for example, `/a/b/c` becomes
  `ou=c,ou=b,ou=a`
- `syncope:fullPath2Dn(fullPath, attr, prefix)` - converts full path into the equivalent DN, with prefix;
  for example, `/a/b/c` with prefix `o=isp,` becomes `o=isp,ou=c,ou=b,ou=a`
- `syncope:connObjAttrValues(connObj, name)` - extracts the values of the attribute with given name from the given
  connector object, or empty list if not found
- `syncope:base64Encode(value)` - encodes the given byte array as Base64-encoded string
- `syncope:base64Decode(value)` - decodes the given string as byte array using Base64 encoding

|  | More custom functions can be added by providing implementations of theJexlFunctionsinterface and registering them as beans, taking care of chosing unique namespace identifiers other thansyncope. |
| --- | --- |

### [4.4. Client Library](#syncope-apache-org-docs-4-0-reference-guide--client-library)

The Java client library simplifies the interaction with the [Core](#syncope-apache-org-docs-4-0-reference-guide--core) by hiding the underlying HTTP
communication details and providing native methods and payload objects.

The library is available as a Maven artifact:

```xml
<dependency>
  <groupId>org.apache.syncope.client.idrepo</groupId>
  <artifactId>syncope-client-idrepo-lib</artifactId>
  <version>4.0.5</version>
</dependency>
```

##### Initialization

First you need to build an instance of `SyncopeClientFactoryBean` by providing the deployment base URL, as follows:

```java
SyncopeClientFactoryBean clientFactory = new SyncopeClientFactoryBean().
              setAddress("http://localhost:9080/syncope/rest/");
```

You might also select a specific [domain](#syncope-apache-org-docs-4-0-reference-guide--domains) - other than `Master`, choose to exchange XML payloads - rather
than JSON (default), to select
[HTTP compression](https://en.wikipedia.org/wiki/HTTP_compression) or to set the
[TLS client configuration](https://cxf.apache.org/javadoc/latest/org/apache/cxf/configuration/jsse/TLSClientParameters.html)
(more options in the
[Javadoc](https://syncope.apache.org/apidocs/4.0/org/apache/syncope/client/lib/SyncopeClientFactoryBean.html)):

```java
TLSClientParameters tlsClientParameters = ...;
SyncopeClientFactoryBean clientFactory = new SyncopeClientFactoryBean().
              setAddress("http://localhost:9080/syncope/rest/").
              setDomain("Two").
              setContentType(SyncopeClientFactoryBean.ContentType.XML).
              setUseCompression(true).
              setTlsClientParameters(tlsClientParameters);
```

At this point an instance of `SyncopeClient` can be obtained by passing the login credentials via:

```java
SyncopeClient client = clientFactory.create("admin", "password");
```

Or you can combine into a single statement as:

```java
SyncopeClient client = new SyncopeClientFactoryBean().
              setAddress("http://localhost:9080/syncope/rest/").
              create("admin", "password");
```

##### Examples

Select one of the
[RESTful services](https://syncope.apache.org/apidocs/4.0/org/apache/syncope/common/rest/api/service/package-summary.html)
and invoke one of the available methods:

```java
LoggerService loggerService = client.getService(LoggerService.class);

LoggerTO loggerTO = loggerService.read(LoggerType.LOG, "org.apache.syncope.core.connid");
loggerTO.setLevel(LoggerLevel.DEBUG);

loggerService.update(LoggerType.LOG, loggerTO);
```

|  | Advanced REST features are also available fromSyncopeClientinstances: checkthe javadocfor more information. |
| --- | --- |

Example 32. Search for Users, Groups or Any Objects

All search operations return
[paged result handlers](https://syncope.apache.org/apidocs/4.0/org/apache/syncope/common/lib/to/PagedResult.html)
which can be exploited both for getting the actual results and for extrapolating pagination coordinates.

```java
UserService userService = client.getService(UserService.class);

int count = userService.search(new AnyQuery.Builder().page(0).size(0).build()).getTotalCount(); (1)

PagedResult<UserTO> matchingUsers = userService.search(
    new AnyQuery.Builder().realm(SyncopeConstants.ROOT_REALM).
    fiql(SyncopeClient.getUserSearchConditionBuilder().is("username").
    equalTo("ros*ini").query()).build()); (2)

PagedResult<UserTO> matchingUsers = userService.search(
    new AnyQuery.Builder().realm(SyncopeConstants.ROOT_REALM).
    fiql(SyncopeClient.getUserSearchConditionBuilder().isNull("loginDate").query()).
    build()); (3)

PagedResult<UserTO> matchingUsers = userService.search(
    new AnyQuery.Builder().realm(SyncopeConstants.ROOT_REALM).
    fiql(SyncopeClient.getUserSearchConditionBuilder().inRoles("Other").query()).
    build()); (4)

AnyObjectService anyObjectService = client.getService(AnyObjectService.class);

PagedResult<AnyObjectTO> matchingAnyObjects = anyObjectService.search(
    new AnyQuery.Builder().realm(SyncopeConstants.ROOT_REALM).
    fiql(SyncopeClient.getAnyObjectSearchConditionBuilder("PRINTER").query()).
    build()); (5)

GroupService groupService = client.getService(GroupService.class);

PagedResult<GroupTO> matchingGroups = groupService.search(
    new AnyQuery.Builder().realm("/even/two").page(3).size(150).
    fiql(SyncopeClient.getGroupSearchConditionBuilder().
        is("name").equalTo("palo*").query()).
    build()); (6)
```

| 1 | get the total number of users available in the given deployment (anddomain) |
| --- | --- |
| 2 | get users in the root realm with username matching the provided wildcard expression |
| 3 | get users in the root realm with no values forloginDate, i.e. that have never authenticated to the given deployment |
| 4 | get users in the root realm withroleOtherassigned |
| 5 | get all any objects in the root realm withtypePRINTER |
| 6 | get all groups having name starting with prefix 'palo' - third page of the result, where each page contains 150 items |

Example 33. Delete several users at once

```java
BatchRequest batchRequest = client.batch(); (1)

UserService batchUserService = batchRequest.getService(UserService.class);

final int pageSize = 100;
final int count = userService.search(
        new AnyQuery.Builder().page(0).size(0).build()).getTotalCount(); (2)
for (int page = 1; page <= (count / pageSize) + 1; page++) {
    for (UserTO user : userService.search(
            new AnyQuery.Builder().page(page).size(pageSize).build()).getResult()) {  (3)

        batchUserService.delete(user.getKey()); (4)
    }
}

BatchResponse batchResponse = batchRequest.commit();  (5)
List<BatchResponseItem> batchResponseItems = batchResponse.getItems(); (6)
```

| 1 | begin the batch request |
| --- | --- |
| 2 | get the total number of users available in the given deployment (anddomain) |
| 3 | loop through all users available, using paginated search |
| 4 | add each user’s deletion to the batch request |
| 5 | send the batch request for processing |
| 6 | examine the batch results |

Example 34. Self-read own profile information

```java
SyncopeClient.Self self = client.self();
UserTO userTO = self.user(); (1)
Map<String, Set<String>> realm2entitlements = self.entitlements(); (2)
List<String> delegations = self.delegations(); (3)
```

| 1 | UserTOof the requesting user |
| --- | --- |
| 2 | for eachrealm, the ownedentitlements |
| 3 | delegationsassigned to the requesting user |

Example 35. Change user status

```java
String key = ...; (1)
StatusR statusR = new StatusR();
statusR.setKey(key);
statusR.setType(StatusRType.SUSPEND); (2)
UserTO userTO = userService.status(statusR).
  readEntity(new GenericType<ProvisioningResult<UserTO>>() {
  }).getEntity(); (3)
```

| 1 | assume the key of the user to be suspended is known in advance |
| --- | --- |
| 2 | ACTIVATE,SUSPEND,REACTIVATEvalues are accepted, and honoured depending on the actual status of the user being updated |
| 3 | request for user update and read back the updated entity |

### [4.5. Customization](#syncope-apache-org-docs-4-0-reference-guide--customization)

|  | Only Maven projects can be customized: if using Standalone, none of the customizations discussed below can be applied. |
| --- | --- |

Apache Syncope is designed to be as flexible as possible, to best suit the various environments
in which it can be deployed. Besides other aspects, this means that every feature and component can be extended or
replaced.

Once the project has been created from the provided Maven archetype, the generated source tree is available for either
adding new features or replacing existing components.

In general, the Embedded Mode (see the
[Apache Syncope Getting Started Guide](#syncope-apache-org-docs-4-0-getting-started)
for details) allows developers to work comfortably from a single workstation, with no need of additional setup; it is
effectively implemented as the `all`
[Maven profile](https://maven.apache.org/guides/introduction/introduction-to-profiles.html), where the available optional
components and extensions are enabled.  
When deploying the generated artifacts as [Standalone](#syncope-apache-org-docs-4-0-reference-guide--standalone) or into an external [JavaEE Container](#syncope-apache-org-docs-4-0-reference-guide--javaee-container) however, the required
components and extensions need to be explicitly selected and enabled, as shown in the following text.

The artifacts are generated by running the Maven command (with reference to the suggested
[directory layout](#syncope-apache-org-docs-4-0-reference-guide--deployment-directories)):

```bash
$ mvn clean verify
$ cp core/target/classes/*properties /opt/syncope/conf
$ cp console/target/classes/*properties /opt/syncope/conf
$ cp enduser/target/classes/*properties /opt/syncope/conf
$ cp enduser/target/classes/*json /opt/syncope/conf
$ cp wa/target/classes/*properties /opt/syncope/conf
$ cp sra/target/classes/*properties /opt/syncope/conf
```

After downloading all of the dependencies that are needed, three following artifacts will be produced:

1. `core/target/syncope.war`
2. `console/target/syncope-console.war`
3. `enduser/target/syncope-enduser.war`
4. `wa/target/syncope-wa.war`
5. `sra/target/syncope-sra.jar`

If no failures are encountered, your basic Apache Syncope project is now ready to be deployed.

Do not forget to define the following system properties:

- `-Dsyncope.conf.dir=/opt/syncope/conf`  
  (required by Core and WA)
- `-Dsyncope.connid.location=file:/opt/syncope/bundles`  
  (required by Core)
- `-Dsyncope.log.dir=/opt/syncope/log`  
  (required by all components)

|  | JPDA Debug in Embedded ModeThe Java™ Platform Debugger Architecture (JPDA) is a collection of APIs aimed to help with debugging Java code.Enhancing theembeddedprofile of thefitmodule to enable the JPDA socket is quite straightforward: just add the<profile>below tofit/pom.xml:<profile>   <id>debug</id>    <build>     <plugins>       <plugin>         <groupId>org.codehaus.cargo</groupId>         <artifactId>cargo-maven3-plugin</artifactId>         <inherited>true</inherited>         <configuration>           <configuration>             <properties>               <cargo.jvmargs>                 -Xdebug                 -Xrunjdwp:transport=dt_socket,address=8000,server=y,suspend=n                 -Dspring.profiles.active=embedded                 -Xmx1024m -Xms512m               </cargo.jvmargs>             </properties>           </configuration>         </configuration>       </plugin>     </plugins>   </build> </profile>Now, from thefitsubdirectory, execute:$ mvn -P embedded,debugAt this point your favourite IDE can be attached to the port8000. |
| --- | --- |

#### [4.5.1. General considerations](#syncope-apache-org-docs-4-0-reference-guide--customization-general)

##### [Override behavior](#syncope-apache-org-docs-4-0-reference-guide--override-behavior)

As a rule of thumb, any file of the local project will take precedence over a file with the same name in the same
package directory of the standard Apache Syncope release.

For example, if you place

```text
core/src/main/java/org/apache/syncope/core/spring/security/UsernamePasswordAuthenticationProvider.java
```

in the local project, this file will be picked up instead of
[UsernamePasswordAuthenticationProvider](https://github.com/apache/syncope/blob/syncope-4.0.5/core/spring/src/main/java/org/apache/syncope/core/spring/security/UsernamePasswordAuthenticationProvider.java).

The same happens with resources as images or HTML files; if you place

```text
console/src/main/resources/org/apache/syncope/client/console/pages/BasePage.html
```

in the local project, this file will be picked up instead of
[BasePage.html](https://github.com/apache/syncope/blob/syncope-4.0.5/client/idrepo/console/src/main/resources/org/apache/syncope/client/console/pages/BasePage.html).

##### [Extending configuration](#syncope-apache-org-docs-4-0-reference-guide--extending-configuration)

Apache Syncope [components](#syncope-apache-org-docs-4-0-reference-guide--architecture) are built on [Spring Boot](https://spring.io/projects/spring-boot),
hence designing and extending Syncope configuration very much comes down to
[their guide](https://docs.spring.io/spring-boot/3.4/index.html), some aspects of which are briefly
highlighted here.

To design your own configuration class, take inspiration from the following sample:

```java
package org.apache.syncope.custom.config;

@Configuration("SomethingConfiguration") (1)
@EnableConfigurationProperties(LogicProperties.class)
public class SomethingConfiguration {

    @Autowired
    private LogicProperties logicProperties;

    @Autowired
    @Qualifier("someOtherBeanId")
    private SomeBean someOtherBeanId;

    @RefreshScope (2)
    @Bean
    public MyBean myBean() {
        return new MyBean();
    }
}
```

| 1 | @Configurationclasses can be assigned an order with@Order(1984)which would place them in an ordered queue waiting to be loaded in that sequence; to be more explicit,@Configurationclasses can also be loaded exactly before/after another@Configurationcomponent with@AutoConfigureBeforeor@AutoConfigureAfterannotations. |
| --- | --- |
| 2 | The@Beandefinitions can also be tagged with@RefreshScopeto become auto-reloadable when the enclosing Syncope componet context is refreshed as a result of an external property change. |

In order to register your own configuration class, create a file named

```text
<component>/src/main/resources/META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
```

with content

```text
org.apache.syncope.custom.config.SomethingConfiguration
```

What if you needed to override the definition of a Syncope-provided bean and replace it entirely with your own?  
Most component/bean definitions are registered with some form of `@Conditional` tag that indicates to the bootstrapping
process to ignore their creation, if a bean definition with the same id is already defined. This means you can create
your own configuration class, register it and the design a `@Bean` definition only to have the context utilize yours
rather than what ships with Syncope by default.

|  | Bean NamesTo correctly define a conditional Bean, you generally need to make sure your own bean definition is created using the same name or identifier as its original equivalent. It is impractical and certainly overwheling to document all runtime bean definitions and their identifiers. So, you will need to study the Syncope codebase to find the correct onfiguration classes and bean defnitions to note their name. |
| --- | --- |

##### [Deployment directories](#syncope-apache-org-docs-4-0-reference-guide--deployment-directories)

Apache Syncope needs three base directories to be defined:

- bundles - where the [connector bundles](#syncope-apache-org-docs-4-0-reference-guide--connector-bundles) are stored;
- log - where all the system logs are written;
- conf - where configuration files are located.

|  | Thebundlesdirectory should only contain connector bundle JAR files.The presence of any other file might cause the unavailability of any connector bundle in Apache Syncope. |
| --- | --- |

For reference, the suggested directory layout can be created as follows:

```bash
$ mkdir /opt/syncope
$ mkdir /opt/syncope/bundles
$ mkdir /opt/syncope/log
$ mkdir /opt/syncope/conf
```

|  | Theconfdirectory must be configured for deployment, following Spring Boot’sExternalized Configurationsettings; with above reference:Standalone:--spring.config.additional-location=/opt/syncope/conf/JavaEE Container:-Dspring.config.additional-location=/opt/syncope/conf/ |
| --- | --- |

#### [4.5.2. Core](#syncope-apache-org-docs-4-0-reference-guide--customization-core)

|  | When providing custom Java classes implementing the defined interfaces or extending the existing implementations, their packagemustbe rooted underorg.apache.syncope.core, otherwise they will not be available at runtime. |
| --- | --- |

Besides replacing existing classes as explained [above](#syncope-apache-org-docs-4-0-reference-guide--override-behavior), new [implementations](#syncope-apache-org-docs-4-0-reference-guide--implementations) can
be provided - in the source tree under `core/src/main/java` when Java or via REST services if Groovy - for the following
components:

- [propagation](#syncope-apache-org-docs-4-0-reference-guide--propagationactions), [push](#syncope-apache-org-docs-4-0-reference-guide--pushactions), [inbound](#syncope-apache-org-docs-4-0-reference-guide--inboundactions), [macro](#syncope-apache-org-docs-4-0-reference-guide--macroactions) and [logic](#syncope-apache-org-docs-4-0-reference-guide--logicactions) actions
- [push](#syncope-apache-org-docs-4-0-reference-guide--push-correlation-rules) / [inbound](#syncope-apache-org-docs-4-0-reference-guide--inbound-correlation-rules) correlation rules
- [reconciliation filter builders](#syncope-apache-org-docs-4-0-reference-guide--pull-mode)
- [live sync delta mappers](#syncope-apache-org-docs-4-0-reference-guide--provisioning-livesync)
- [commands](#syncope-apache-org-docs-4-0-reference-guide--commands)
- [macros](#syncope-apache-org-docs-4-0-reference-guide--tasks-macro)
- [scheduled tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-scheduled)
- [reports](#syncope-apache-org-docs-4-0-reference-guide--reports)
- [account](#syncope-apache-org-docs-4-0-reference-guide--account-rules) and [password](#syncope-apache-org-docs-4-0-reference-guide--password-rules) rules for policies
- [plain schema validators and dropdown value providers](#syncope-apache-org-docs-4-0-reference-guide--plain)
- [mapping item transformers](#syncope-apache-org-docs-4-0-reference-guide--mapping)
- [workflow adapters](#syncope-apache-org-docs-4-0-reference-guide--workflow-adapters)
- [provisioning managers](#syncope-apache-org-docs-4-0-reference-guide--provisioning-managers)
- [notification recipient providers](#syncope-apache-org-docs-4-0-reference-guide--notifications)
- [JWT SSO providers](#syncope-apache-org-docs-4-0-reference-guide--jwtssoprovider)
- [audit event processors](#syncope-apache-org-docs-4-0-reference-guide--audit-event-processors)

##### Customize OpenJPA settings

Apache OpenJPA is at the core of the [persistence](#syncope-apache-org-docs-4-0-reference-guide--persistence) layer; its configuration can be tweaked under several
aspects - including [caching](https://openjpa.apache.org/builds/4.0.1/apache-openjpa/docs/ref_guide_caching.html) for
example, to best suit the various environments.

The main configuration classes are:

- [PersistenceContext](https://github.com/apache/syncope/blob/syncope-4.0.5/core/persistence-jpa/src/main/java/org/apache/syncope/core/persistence/jpa/PersistenceContext.java)
- [MasterDomain](https://github.com/apache/syncope/blob/syncope-4.0.5/core/persistence-jpa/src/main/java/org/apache/syncope/core/persistence/jpa/MasterDomain.java)

The `@Bean` declarations from these classes can be customized as explained [above](#syncope-apache-org-docs-4-0-reference-guide--extending-configuration).

##### Enable the [Flowable User Workflow Adapter](#syncope-apache-org-docs-4-0-reference-guide--flowable-user-workflow-adapter)

Add the following dependency to `core/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.flowable</groupId>
  <artifactId>syncope-ext-flowable-rest-cxf</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

##### Enable the [SAML 2.0 Service Provider for UI](#syncope-apache-org-docs-4-0-reference-guide--saml2sp4ui) extension

Add the following dependencies to `core/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.saml2sp4ui</groupId>
  <artifactId>syncope-ext-saml2sp4ui-rest-cxf</artifactId>
  <version>${syncope.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.syncope.ext.saml2sp4ui</groupId>
  <artifactId>syncope-ext-saml2sp4ui-persistence-jpa</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

Setup a [keystore](#syncope-apache-org-docs-4-0-reference-guide--keystore) and place it under the [configuration directory](#syncope-apache-org-docs-4-0-reference-guide--properties-files-location), then take
the properties from `core/src/test/resources/core-all.properties` into your configuration and review accordingly.

##### Enable the [OpenID Connect Client for UI](#syncope-apache-org-docs-4-0-reference-guide--oidcc4ui) extension

Add the following dependencies to `core/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.oidcc4ui</groupId>
  <artifactId>syncope-ext-oidcc4ui-rest-cxf</artifactId>
  <version>${syncope.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.syncope.ext.oidcc4ui</groupId>
  <artifactId>syncope-ext-oidcc4ui-persistence-jpa</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

##### Enable the [Elasticsearch](#syncope-apache-org-docs-4-0-reference-guide--elasticsearch) extension

Add the following dependencies to `core/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.elasticsearch</groupId>
  <artifactId>syncope-ext-elasticsearch-provisioning-java</artifactId>
  <version>${syncope.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.syncope.ext.elasticsearch</groupId>
  <artifactId>syncope-ext-elasticsearch-persistence</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

Create

```properties
elasticsearch.hosts[0]=http://localhost:9200
elasticsearch.indexMaxResultWindow=10000
elasticsearch.numberOfShards=1
elasticsearch.numberOfReplicas=1
```

as `core/src/main/resources/core-elasticsearch.properties`.

Do not forget to include `elasticsearch` as
[Spring Boot profile](https://docs.spring.io/spring-boot/3.4/reference/features/profiles.html)
for the Core application.

If needed, customize the `@Bean` declarations from
[ElasticsearchClientContext](https://github.com/apache/syncope/blob/syncope-4.0.5/ext/elasticsearch/client-elasticsearch/src/main/java/org/apache/syncope/ext/elasticsearch/client/ElasticsearchClientContext.java)
as explained [above](#syncope-apache-org-docs-4-0-reference-guide--extending-configuration).

It is also required to initialize the Elasticsearch indexes: add a new Java [implementation](#syncope-apache-org-docs-4-0-reference-guide--implementations) for
`TASKJOB_DELEGATE` and use `org.apache.syncope.core.provisioning.java.job.ElasticsearchReindex` as class.  
Then, create a new [scheduled task](#syncope-apache-org-docs-4-0-reference-guide--tasks-scheduled), select the implementation just created as job delegate and execute it.

|  | Theorg.apache.syncope.core.provisioning.java.job.ElasticsearchReindextask created above is not meant for scheduled execution; rather, it can be run every time you want to blank and re-create the Elasticsearch indexes starting from Syncope’s internal storage. |
| --- | --- |

##### Enable the [OpenSearch](#syncope-apache-org-docs-4-0-reference-guide--opensearch) extension

Add the following dependencies to `core/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.opensearch</groupId>
  <artifactId>syncope-ext-opensearch-provisioning-java</artifactId>
  <version>${syncope.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.syncope.ext.opensearch</groupId>
  <artifactId>syncope-ext-opensearch-persistence</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

Create

```properties
opensearch.hosts[0]=http://localhost:9200
opensearch.indexMaxResultWindow=10000
opensearch.numberOfShards=1
opensearch.numberOfReplicas=1
```

as `core/src/main/resources/core-opensearch.properties`.

Do not forget to include `opensearch` as
[Spring Boot profile](https://docs.spring.io/spring-boot/3.4/reference/features/profiles.html#features.profiles.adding-active-profiles)
for the Core application.

If needed, customize the `@Bean` declarations from
[OpenSearchClientContext](https://github.com/apache/syncope/blob/syncope-4.0.5/ext/opensearch/client-opensearch/src/main/java/org/apache/syncope/ext/opensearch/client/OpenSearchClientContext.java)
as explained [above](#syncope-apache-org-docs-4-0-reference-guide--extending-configuration).

It is also required to initialize the OpenSearch indexes: add a new Java [implementation](#syncope-apache-org-docs-4-0-reference-guide--implementations) for
`TASKJOB_DELEGATE` and use `org.apache.syncope.core.provisioning.java.job.OpenSearchReindex` as class.  
Then, create a new [scheduled task](#syncope-apache-org-docs-4-0-reference-guide--tasks-scheduled), select the implementation just created as job delegate and execute it.

|  | Theorg.apache.syncope.core.provisioning.java.job.OpenSearchReindextask created above is not meant for scheduled execution; rather, it can be run every time you want to blank and re-create the OpenSearch indexes starting from Syncope’s internal storage. |
| --- | --- |

##### Enable the [SCIM](#syncope-apache-org-docs-4-0-reference-guide--scim) extension

Add the following dependencies to `core/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.scimv2</groupId>
  <artifactId>syncope-ext-scimv2-rest-cxf</artifactId>
  <version>${syncope.version}</version>
</dependency>
<dependency>
  <groupId>org.apache.syncope.ext.scimv2</groupId>
  <artifactId>syncope-ext-scimv2-scim-rest-cxf</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

##### Enable the [OpenFGA](#syncope-apache-org-docs-4-0-reference-guide--openfga) extension

Add the following dependency to `core/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.openfga</groupId>
  <artifactId>syncope-ext-openfga-provisioning-java</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

Create

```text
openfga.api-url=http://localhost:8080
```

as `core/src/main/resources/core-openfga.properties`.  
More properties are available, as you can read from
[OpenFGAProperties](https://github.com/apache/syncope/blob/syncope-4.0.5/ext/openfga/client-openfga/src/main/java/org/apache/syncope/ext/openfga/client/OpenFGAProperties.java)
.

Do not forget to include `openfga` as
[Spring Boot profile](https://docs.spring.io/spring-boot/3.4/reference/features/profiles.html#features.profiles.adding-active-profiles)
for the Core application.

If needed, customize the `@Bean` declarations from
[OpenFGAClientContext](https://github.com/apache/syncope/blob/syncope-4.0.5/ext/openfga/client-openfga/src/main/java/org/apache/syncope/ext/openfga/client/OpenFGAClientContext.java)
as explained [above](#syncope-apache-org-docs-4-0-reference-guide--extending-configuration).

It is also required to initialize the OpenFGA authorization model: add a new Java [implementation](#syncope-apache-org-docs-4-0-reference-guide--implementations) for
`TASKJOB_DELEGATE` and use `org.apache.syncope.core.provisioning.java.job.OpenFGAReinit` as class.  
Then, create a new [scheduled task](#syncope-apache-org-docs-4-0-reference-guide--tasks-scheduled), select the implementation just created as job delegate and execute it.

|  | Theorg.apache.syncope.core.provisioning.java.job.OpenFGAReinittask created above is not meant for scheduled execution; rather, it can be run every time you want to blank and re-create the OpenFGA authorization model starting from Syncope’s internal storage. |
| --- | --- |

##### New REST endpoints

Adding a new REST endpoint involves several operations:

1. create - in an extension’s `rest-api` module or under `common` otherwise - a Java interface with package
   `org.apache.syncope.common.rest.api.service` and proper Jakarta RESTful Web Services annotations; check
   [BpmnProcessService](https://github.com/apache/syncope/blob/syncope-4.0.5/ext/flowable/rest-api/src/main/java/org/apache/syncope/common/rest/api/service/BpmnProcessService.java)
   for reference;
2. if needed, define supporting payload objects - in an extension’s `common-lib` module or under `common` otherwise;
   check
   [BpmnProcess](https://github.com/apache/syncope/blob/syncope-4.0.5/ext/flowable/common-lib/src/main/java/org/apache/syncope/common/lib/to/BpmnProcess.java)
   for reference;
3. implement - in an extension’s `rest-cxf` module or under `core` otherwise - the interface defined above in a Java
   class with package `org.apache.syncope.core.rest.cxf.service`; check
   [BpmnProcessServiceImpl](https://github.com/apache/syncope/blob/syncope-4.0.5/ext/flowable/rest-cxf/src/main/java/org/apache/syncope/core/rest/cxf/service/BpmnProcessServiceImpl.java)
   for reference.

By following such conventions, the new REST endpoint will be automatically picked up alongside the default services.

#### [4.5.3. Console](#syncope-apache-org-docs-4-0-reference-guide--customization-console)

|  | When providing custom Java classes implementing the defined interfaces or extending the existing implementations, their packagemustbe rooted underorg.apache.syncope.client.console, otherwise they will not be available at runtime. |
| --- | --- |

##### Enable the [Flowable User Workflow Adapter](#syncope-apache-org-docs-4-0-reference-guide--flowable-user-workflow-adapter)

Add the following dependency to `console/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.flowable</groupId>
  <artifactId>syncope-ext-flowable-client-console</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

##### Enable the [SAML 2.0 Service Provider for UI](#syncope-apache-org-docs-4-0-reference-guide--saml2sp4ui) extension

Add the following dependencies to `console/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.saml2sp4ui</groupId>
  <artifactId>syncope-ext-saml2sp4ui-client-console</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

##### Enable the [OpenID Connect Client for UI](#syncope-apache-org-docs-4-0-reference-guide--oidcc4ui) extension

Add the following dependencies to `console/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.oidcc4ui</groupId>
  <artifactId>syncope-ext-oidcc4ui-client-console</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

##### Enable the [SCIM](#syncope-apache-org-docs-4-0-reference-guide--scim) extension

Add the following dependencies to `console/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.scimv2</groupId>
  <artifactId>syncope-ext-scimv2-client-console</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

#### [4.5.4. Enduser](#syncope-apache-org-docs-4-0-reference-guide--customization-enduser)

|  | When providing custom Java classes implementing the defined interfaces or extending the existing implementations, their packagemustbe rooted underorg.apache.syncope.client.enduser, otherwise they will not be available at runtime. |
| --- | --- |

##### Enable the [Flowable User Workflow Adapter](#syncope-apache-org-docs-4-0-reference-guide--flowable-user-workflow-adapter)

Add the following dependency to `enduser/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.flowable</groupId>
  <artifactId>syncope-ext-flowable-client-enduser</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

##### Enable the [SAML 2.0 Service Provider for UI](#syncope-apache-org-docs-4-0-reference-guide--saml2sp4ui) extension

Add the following dependencies to `enduser/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.saml2sp4ui</groupId>
  <artifactId>syncope-ext-saml2sp4ui-client-enduser</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

##### Enable the [OpenID Connect Client for UI](#syncope-apache-org-docs-4-0-reference-guide--oidcc4ui) extension

Add the following dependencies to `enduser/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.ext.oidcc4ui</groupId>
  <artifactId>syncope-ext-oidcc4ui-client-enduser</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

##### [Form customization](#syncope-apache-org-docs-4-0-reference-guide--customization-enduser-form)

The [Enduser Application](#syncope-apache-org-docs-4-0-reference-guide--enduser-application) allows to customize the form in order to:

- hide / show attributes
- set attributes read-only for users
- provide default value(s)

Under the `enduser/src/main/resources` directory, the `customFormLayout.json` file is available, allowing to configure
form customization.

#### [4.5.5. WA](#syncope-apache-org-docs-4-0-reference-guide--customization-wa)

|  | When providing custom Java classes implementing the defined interfaces or extending the existing implementations, their packagemustbe rooted underorg.apache.syncope.wa, otherwise they will not be available at runtime. |
| --- | --- |

#### [4.5.6. SRA](#syncope-apache-org-docs-4-0-reference-guide--customization-sra)

|  | When providing custom Java classes implementing the defined interfaces or extending the existing implementations, their packagemustbe rooted underorg.apache.syncope.sra, otherwise they will not be available at runtime. |
| --- | --- |

#### [4.5.7. Extensions](#syncope-apache-org-docs-4-0-reference-guide--customization-extensions)

[Extensions](#syncope-apache-org-docs-4-0-reference-guide--extensions) can be part of a local project, to encapsulate special features which are specific to a given deployment.

### [4.6. Actuator Endpoints](#syncope-apache-org-docs-4-0-reference-guide--actuator-endpoints)

Spring Boot’s actuator endpoints let you monitor and interact with Syncope components.

Each individual endpoint can be enabled / disabled and exposed over HTTP (pre-defined, under the `/actuator` subcontext)
or JMX.

Besides a number of [built-in endpoints](https://docs.spring.io/spring-boot/3.4/reference/actuator/endpoints.html),
more are made available for each component, as reported below.

|  | The pre-definedhealthandinfoendpoints are extended by each Syncope component, to add sensible data for the given component. |
| --- | --- |

|  | The pre-definedhealthendpoint is typically used for liveness and readiness probes, even with Kubernetes. |
| --- | --- |

#### [4.6.1. Core](#syncope-apache-org-docs-4-0-reference-guide--actuator-core)

| entityCache | Allows to work withJPA cache statisticsGET- shows JPA cache statisticsPOST {ENABLE,DISABLE,RESET}- performs the requested operation onto JPA cacheDELETE- clears JPA cache’s current content |
| --- | --- |
| job | Allows to work with the various jobs defined afterTasksandReports.GET- shows the existing job dataPOST {START,STOP,DELETE}- performs the requested action on a given Job |

#### [4.6.2. WA](#syncope-apache-org-docs-4-0-reference-guide--actuator-wa)

| ssoSessions | More details |
| --- | --- |
| registeredServices | More details |
| authenticationHandlers | More details |
| authenticationPolicies | More details |
| resolveAttributes | More details |

#### [4.6.3. SRA](#syncope-apache-org-docs-4-0-reference-guide--actuator-sra)

| sraSessions | GET- lists the current sessionsGET {id}- reads the session with givenidDELETE {id}- removes the session with givenid |
| --- | --- |
| gateway | More details |

### [4.7. Loggers](#syncope-apache-org-docs-4-0-reference-guide--loggers)

Spring Boot actuator includes the ability to
[view and configure the log levels](https://docs.spring.io/spring-boot/3.4/reference/actuator/loggers.html) of all
Syncope modules at runtime.

In addition, Console provides a UI to view the list of logger’s configuration and set their level, for all
Syncope modules.

### [4.8. Metrics](#syncope-apache-org-docs-4-0-reference-guide--metrics)

Monitoring performance to ensure reliability and efficiency can be achieved by leveraging
[Spring Boot’s metrics](https://docs.spring.io/spring-boot/3.4/reference/actuator/metrics.html).

#### [4.8.1. Core](#syncope-apache-org-docs-4-0-reference-guide--metrics-core)

This can be enabled by adding the following dependency to `core/pom.xml`:

```xml
<dependency>
  <groupId>org.apache.syncope.core</groupId>
  <artifactId>syncope-core-metrics-starter</artifactId>
  <version>${syncope.version}</version>
</dependency>
```

Additional dependencies might be required, depending on the
[actual monitoring system in use](https://docs.spring.io/spring-boot/3.4/reference/actuator/metrics.html#actuator.metrics.export).

#### [4.8.2. WA](#syncope-apache-org-docs-4-0-reference-guide--metrics-wa)

This can be enabled by adding the following dependency to `wa/pom.xml`:

```xml
<dependency>
  <groupId>org.apereo.cas</groupId>
  <artifactId>cas-server-support-metrics</artifactId>
  <version>${cas.version}</version>
</dependency>
```

For further options and configuration, refer to [CAS documentation](https://apereo.github.io/cas/7.2.x/monitoring/Configuring-Metrics.html).

## [5. Configuration](#syncope-apache-org-docs-4-0-reference-guide--configuration-2)

Where are the configuration files?

Depending on which Apache Syncope distribution you are running, the configuration files mentioned in the following
text might reside in different locations.

- Standalone

  Assuming that `$CATALINA_HOME` is the Apache Tomcat base directory created when the distribution archive
  was unzipped, the configuration files are located under

  - `$CATALINA_HOME/webapps/syncope/WEB-INF/classes/`
  - `$CATALINA_HOME/webapps/syncope-console/WEB-INF/classes/`
  - `$CATALINA_HOME/webapps/syncope-enduser/WEB-INF/classes/`
  - `$CATALINA_HOME/webapps/syncope-wa/WEB-INF/classes/`
- Maven project

  Assuming that `$CONF_DIRECTORY` is the directory passed among
  [deployment directories](#syncope-apache-org-docs-4-0-reference-guide--deployment-directories) at build time and that `$SOURCE` is the path where the Maven project
  was generated, the configuration files will be first searched in
  `$CONF_DIRECTORY`, then under the selected deployment’s application classpath, according to the content of

  - `$SOURCE/core/target/classes/`
  - `$SOURCE/console/target/classes/`
  - `$SOURCE/enduser/target/classes/`
  - `$SOURCE/wa/target/classes/`
  - `$SOURCE/sra/target/classes/`

### [5.1. Deployment](#syncope-apache-org-docs-4-0-reference-guide--deployment)

Apache Syncope [components](#syncope-apache-org-docs-4-0-reference-guide--architecture) are built on [Spring Boot](https://spring.io/projects/spring-boot),
hence components can be generally deployed either as standalone applications or into one of the supported
Jakarta EE containers.

|  | The only exception isSecure Remote Accessthat, being based on Spring Cloud Gateway - which in turn is built onSpring WebFluxandProject Reactor, is only available as standalone application. |
| --- | --- |

|  | For all components, please ensure to reference the properKeymasterinstance by including the following properties:keymaster.address=<KEYMASTER_ADDRESS> keymaster.username=${anonymousUser} keymaster.password=${anonymousKey}where<KEYMASTER_ADDRESS>can be either:protocol://host:port/syncope/rest/keymasterpointing to theCoreinstance, in case ofSelf Keymaster;host:port(typicallyhost:2181) in case Apache Zookeeper is used. |
| --- | --- |

#### [5.1.1. Standalone](#syncope-apache-org-docs-4-0-reference-guide--standalone)

Projects generated from Maven archetype feature a dedicated `standalone` profile, which will re-package all
applications as standalone fat JAR or WAR files.

|  | Spring Boot applications can also beinstalled as system services. |
| --- | --- |

|  | Virtual Threadscan be enabled but JDK >= 24 is required in order to run properly. |
| --- | --- |

Example 36. Run Core application as standalone under GNU / Linux

Assuming that the JDBC driver JAR file for the configured [DBMS](#syncope-apache-org-docs-4-0-reference-guide--dbms) is available under `/opt/syncope/lib`,
the Core application can be built and run as follows:

```bash
$ mvn -P standalone clean verify
$ cp core/target/syncope.war /opt/syncope/lib
$ cp core/target/classes/*properties /opt/syncope/conf

$ export LOADER_PATH=/opt/syncope/conf,/opt/syncope/lib,BOOT-INF/classes/WEB-INF/classes
$ java -Dsyncope.conf.dir=/opt/syncope/conf \
  -Dsyncope.connid.location=file:/opt/syncope/bundles \
  -Dsyncope.log.dir=/opt/syncope/log \
  -jar /opt/syncope/lib/syncope.war
```

Further options can be passed to last command, according to Spring Boot
[documentation](https://docs.spring.io/spring-boot/3.4/appendix/application-properties/index.html);
for example:

- `--spring.config.additional-location=/path`  
  to customize the location of the configuration files
- `--server.port=8080`  
  to change the default HTTP port

#### [5.1.2. JavaEE Container](#syncope-apache-org-docs-4-0-reference-guide--javaee-container)

Deployment into the Jakarta EE containers listed below might require Maven project changes or tweaking some configuration
settings.

Database Connection Pool

The [internal storage](#syncope-apache-org-docs-4-0-reference-guide--persistence) is the central place where all data of a given [Core](#syncope-apache-org-docs-4-0-reference-guide--core) deployment are located.

After choosing the appropriate [DBMS](#syncope-apache-org-docs-4-0-reference-guide--dbms), it is of fundamental importance to provide an adequate configuration for the
related database [connection pool](https://en.wikipedia.org/wiki/Connection_pool).

The database connection pool can be:

1. Application-managed (default); based on [HikariCP](http://brettwooldridge.github.io/HikariCP/), the related
   parameters can be tuned in the related [domain](#syncope-apache-org-docs-4-0-reference-guide--domains) configuration file, e.g. `domains/Master.properties`,
   for the Master domain.
2. [JavaEE Container](#syncope-apache-org-docs-4-0-reference-guide--javaee-container)-managed, via the JNDI resource matching the name specified for a given [domain](#syncope-apache-org-docs-4-0-reference-guide--domains), e.g.
   `java:comp/env/jdbc/syncopeMasterDataSource` for the `Master` domain.  
   Each JavaEE Container provides its own way to accomplish this task:

   - [Apache Tomcat 10](https://tomcat.apache.org/tomcat-10.0-doc/jdbc-pool.html)
   - [Payara Server 6](https://docs.payara.fish/community/docs/Technical%20Documentation/Payara%20Server%20Documentation/Server%20Configuration%20And%20Management/JDBC%20Resource%20Management/JDBC.html)
   - [Wildfly 38](https://docs.wildfly.org/38/Admin_Guide.html#DataSource)

#### [5.1.3. Apache Tomcat 10](#syncope-apache-org-docs-4-0-reference-guide--apache-tomcat-10)

On GNU / Linux - Mac OS X, create `$CATALINA_HOME/bin/setenv.sh` with similar content
(keep everything on a single line):

```properties
JAVA_OPTS="-Djava.awt.headless=true -Dfile.encoding=UTF-8 -server \
-Dsyncope.conf.dir=/opt/syncope/conf \
-Dsyncope.connid.location=file:/opt/syncope/bundles \
-Dsyncope.log.dir=/opt/syncope/log \
-Xms1536m -Xmx1536m -XX:NewSize=256m -XX:MaxNewSize=256m -XX:+DisableExplicitGC \
-Djava.security.egd=file:/dev/./urandom"
```

On MS Windows, create `%CATALINA_HOME%\bin\setenv.bat` with similar content (keep everything on a single line):

```text
set JAVA_OPTS=-Djava.awt.headless=true -Dfile.encoding=UTF-8 -server \
-Dsyncope.conf.dir=C:\opt\syncope\conf \
-Dsyncope.connid.location=file:/C:\opt\syncope\bundles \
-Dsyncope.log.dir=C:\opt\syncope\log \
-Xms1536m -Xmx1536m -XX:NewSize=256m -XX:MaxNewSize=256m -XX:+DisableExplicitGC
```

#### [5.1.4. Payara Server 6](#syncope-apache-org-docs-4-0-reference-guide--payara-server-6)

Add

```xml
    <dependency>
      <groupId>org.glassfish</groupId>
      <artifactId>jakarta.faces</artifactId>
      <version>${jakarta.faces.version}</version>
    </dependency>
```

to `core/pom.xml`, `console/pom.xml`, `enduser/pom.xml` and `wa/pom.xml`,

then replace

```xml
    <dependency>
      <groupId>org.apache.syncope.core</groupId>
      <artifactId>syncope-core-starter</artifactId>
    </dependency>
```

with

```xml
    <dependency>
      <groupId>org.apache.syncope.core</groupId>
      <artifactId>syncope-core-starter</artifactId>
        <exclusions>
          <exclusion>
            <groupId>org.apache.tomcat.embed</groupId>
            <artifactId>tomcat-embed-el</artifactId>
          </exclusion>
        <exclusion>
          <groupId>com.github.ben-manes.caffeine</groupId>
          <artifactId>jcache</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
```

in `core/pom.xml`.

When using a datasource for internal storage, be sure to add

```xml
<resource-ref>
  <res-ref-name>jdbc/syncopeMasterDataSource</res-ref-name>
  <jndi-name>jdbc/syncopeMasterDataSource</jndi-name>
</resource-ref>
```

right after `</context-root>` in `core/src/main/webapp/WEB-INF/glassfish-web.xml`, assuming that your Payara Server
instance provides a datasource named `jdbc/syncopeMasterDataSource`.

|  | Do not forget to include the following system properties:-Dsyncope.conf.dir=/opt/syncope/conf(required by Core and WA)-Dsyncope.connid.location=file:/opt/syncope/bundles(required by Core)-Dsyncope.log.dir=/opt/syncope/log(required by all components) |
| --- | --- |

|  | For better performance under GNU / Linux, do not forget to include the system property:-Djava.security.egd=file:/dev/./urandom |
| --- | --- |

#### [5.1.5. Wildfly 38](#syncope-apache-org-docs-4-0-reference-guide--wildfly-38)

Add

```xml
    <dependency>
      <groupId>jakarta.xml.ws</groupId>
      <artifactId>jakarta.xml.ws-api</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.cxf</groupId>
      <artifactId>cxf-core</artifactId>
      <version>${cxf.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.cxf</groupId>
      <artifactId>cxf-rt-ws-policy</artifactId>
      <version>${cxf.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.cxf</groupId>
      <artifactId>cxf-rt-wsdl</artifactId>
      <version>${cxf.version}</version>
    </dependency>
```

as additional dependencies in `core/pom.xml` and `wa/pom.xml`,

then replace

```xml
    <dependency>
      <groupId>org.apache.syncope.core</groupId>
      <artifactId>syncope-core-starter</artifactId>
    </dependency>
```

with

```xml
    <dependency>
      <groupId>org.apache.syncope.core</groupId>
      <artifactId>syncope-core-starter</artifactId>
      <exclusions>
        <exclusion>
          <groupId>org.apache.tomcat.embed</groupId>
          <artifactId>tomcat-embed-el</artifactId>
        </exclusion>
        <exclusion>
          <groupId>org.springframework.boot</groupId>
          <artifactId>spring-boot-starter-tomcat</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
```

in `core/pom.xml` and create

```text
persistence.metaDataFactory=jpa(URLs=\
vfs:/content/${project.build.finalName}.war/WEB-INF/lib/syncope-core-persistence-jpa-${syncope.version}.jar; \
vfs:/content/${project.build.finalName}.war/WEB-INF/lib/syncope-core-self-keymaster-starter-${syncope.version}.jar, \
Resources=##orm##)

javadocPaths=/WEB-INF/lib/syncope-common-idrepo-rest-api-${syncope.version}-javadoc.jar,\
/WEB-INF/lib/syncope-common-idm-rest-api-${syncope.version}-javadoc.jar,\
/WEB-INF/lib/syncope-common-am-rest-api-${syncope.version}-javadoc.jar
```

as `core/src/main/resources/core-wildfly.properties`.

In addition, replace

```xml
    <dependency>
      <groupId>org.apache.syncope.client.idm</groupId>
      <artifactId>syncope-client-idm-console</artifactId>
    </dependency>
    <dependency>
      <groupId>org.apache.syncope.client.am</groupId>
      <artifactId>syncope-client-am-console</artifactId>
    </dependency>
```

with

```xml
    <dependency>
      <groupId>org.apache.syncope.client.idm</groupId>
      <artifactId>syncope-client-idm-console</artifactId>
      <exclusions>
        <exclusion>
          <groupId>org.apache.tomcat.embed</groupId>
          <artifactId>tomcat-embed-el</artifactId>
        </exclusion>
        <exclusion>
          <groupId>org.springframework.boot</groupId>
          <artifactId>spring-boot-starter-tomcat</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
    <dependency>
      <groupId>org.apache.syncope.client.am</groupId>
      <artifactId>syncope-client-am-console</artifactId>
      <exclusions>
        <exclusion>
          <groupId>org.apache.tomcat.embed</groupId>
          <artifactId>tomcat-embed-el</artifactId>
        </exclusion>
        <exclusion>
          <groupId>org.springframework.boot</groupId>
          <artifactId>spring-boot-starter-tomcat</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
```

in `console/pom.xml` and

```xml
    <dependency>
      <groupId>org.apache.syncope.client.am</groupId>
      <artifactId>syncope-client-am-enduser</artifactId>
    </dependency>
```

with

```xml
    <dependency>
      <groupId>org.apache.syncope.client.am</groupId>
      <artifactId>syncope-client-am-enduser</artifactId>
      <exclusions>
        <exclusion>
          <groupId>org.apache.tomcat.embed</groupId>
          <artifactId>tomcat-embed-el</artifactId>
        </exclusion>
        <exclusion>
          <groupId>org.springframework.boot</groupId>
          <artifactId>spring-boot-starter-tomcat</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
```

in `enduser/pom.xml`.

Do not forget to include `widlfly` as
[Spring Boot profile](https://docs.spring.io/spring-boot/3.4/reference/features/profiles.html)
for the Core application.

|  | Do not forget to include the following system properties:-Dsyncope.conf.dir=/opt/syncope/conf(required by all components)-Dsyncope.connid.location=file:/opt/syncope/bundles(required by Core)-Dsyncope.log.dir=/opt/syncope/log(required by all components) |
| --- | --- |

|  | For better performance under GNU / Linux, do not forget to include the system property:-Djava.security.egd=file:/dev/./urandom |
| --- | --- |

### [5.2. DBMS](#syncope-apache-org-docs-4-0-reference-guide--dbms)

#### [5.2.1. PostgreSQL](#syncope-apache-org-docs-4-0-reference-guide--postgresql)

|  | Apache Syncope 4.0.5 is verified with PostgreSQL server >= 17-alpine and JDBC driver >= 42.7.10. |
| --- | --- |

Create

```text
persistence.domain[0].key=Master
persistence.domain[0].jdbcDriver=org.postgresql.Driver
persistence.domain[0].jdbcURL=jdbc:postgresql://localhost:5432/syncope?stringtype=unspecified
persistence.domain[0].dbUsername=syncope
persistence.domain[0].dbPassword=syncope
persistence.domain[0].databasePlatform=org.apache.openjpa.jdbc.sql.PostgresDictionary
persistence.domain[0].poolMaxActive=20
persistence.domain[0].poolMinIdle=5
```

as `core/src/main/resources/core-postgresql.properties`.

Do not forget to include `postgresql` as
[Spring Boot profile](https://docs.spring.io/spring-boot/3.4/reference/features/profiles.html#features.profiles.adding-active-profiles)
for the Core application.

|  | This assumes that you have a PostgreSQL instance running on localhost, listening on its default port 5432 with a databasesyncopefully accessible by usersyncopewith passwordsyncope. |
| --- | --- |

#### [5.2.2. MySQL](#syncope-apache-org-docs-4-0-reference-guide--mysql)

|  | Apache Syncope 4.0.5 is verified with MySQL server >= 9.0 and JDBC driver >= 9.6.0. |
| --- | --- |

Create

```properties
persistence.indexesXML=classpath:META-INF/mysql/indexes.xml
persistence.viewsXML=classpath:META-INF/mysql/views.xml

persistence.domain[0].key=Master
persistence.domain[0].jdbcDriver=com.mysql.cj.jdbc.Driver
persistence.domain[0].jdbcURL=jdbc:mysql://localhost:3306/syncope?useSSL=false&allowPublicKeyRetrieval=true&characterEncoding=UTF-8
persistence.domain[0].dbUsername=syncope
persistence.domain[0].dbPassword=syncope
persistence.domain[0].databasePlatform=org.apache.openjpa.jdbc.sql.MySQLDictionary(blobTypeName=LONGBLOB,dateFractionDigits=3,useSetStringForClobs=true)
persistence.domain[0].orm=META-INF/mysql/spring-orm.xml
persistence.domain[0].poolMaxActive=20
persistence.domain[0].poolMinIdle=5
```

as `core/src/main/resources/core-mysql.properties`.

Do not forget to include `mysql` as
[Spring Boot profile](https://docs.spring.io/spring-boot/3.4/reference/features/profiles.html#features.profiles.adding-active-profiles)
for the Core application.

|  | It is important to set the collation toutf8_general_ciafter creation ofsyncopedatabase. |
| --- | --- |

|  | This assumes that you have a MySQL instance running on localhost, listening on its default port 3306 with a databasesyncopefully accessible by usersyncopewith passwordsyncope. |
| --- | --- |

#### [5.2.3. MariaDB](#syncope-apache-org-docs-4-0-reference-guide--mariadb)

|  | Apache Syncope 4.0.5 is verified with MariaDB server >= 12 and JDBC driver >= 3.5.7. |
| --- | --- |

Create

```properties
persistence.indexesXML=classpath:META-INF/mariadb/indexes.xml
persistence.viewsXML=classpath:META-INF/mariadb/views.xml

persistence.domain[0].key=Master
persistence.domain[0].jdbcDriver=org.mariadb.jdbc.Driver
persistence.domain[0].jdbcURL=jdbc:mariadb://localhost:3306/syncope?characterEncoding=UTF-8
persistence.domain[0].dbUsername=syncope
persistence.domain[0].dbPassword=syncope
persistence.domain[0].databasePlatform=org.apache.openjpa.jdbc.sql.MariaDBDictionary(blobTypeName=LONGBLOB,dateFractionDigits=3,useSetStringForClobs=true)
persistence.domain[0].orm=META-INF/mariadb/spring-orm.xml
persistence.domain[0].poolMaxActive=20
persistence.domain[0].poolMinIdle=5
```

as `core/src/main/resources/core-mariadb.properties`.

Do not forget to include `mariadb` as
[Spring Boot profile](https://docs.spring.io/spring-boot/3.4/reference/features/profiles.html#features.profiles.adding-active-profiles)
for the Core application.

|  | It is important to set the collation toutf8_general_ciafter creation ofsyncopedatabase. |
| --- | --- |

|  | It is necessary to useutf8mb4_unicode_ciinstead ofutf8mb4_general_ciif case-sensitive queries are required. In this case, setinit_connect = "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"under either the[mysqld]section or the[mariadb]section of youroption file. |
| --- | --- |

|  | This assumes that you have a MariaDB instance running on localhost, listening on its default port 3306 with a databasesyncopefully accessible by usersyncopewith passwordsyncope. |
| --- | --- |

#### [5.2.4. Oracle Database](#syncope-apache-org-docs-4-0-reference-guide--oracle-database)

|  | Apache Syncope 4.0.5 is verified with Oracle database >= 23-slim-faststart and JDBC driver >= ojdbc11 23.26.1.0.0. |
| --- | --- |

Create

```text
persistence.indexesXML=classpath:META-INF/oracle/indexes.xml
persistence.viewsXML=classpath:META-INF/oracle/views.xml

persistence.domain[0].key=Master
persistence.domain[0].jdbcDriver=oracle.jdbc.OracleDriver
persistence.domain[0].jdbcURL=jdbc:oracle:thin:@localhost}:1521/FREEPDB1
persistence.domain[0].dbSchema=SYNCOPE
persistence.domain[0].dbUsername=syncope
persistence.domain[0].dbPassword=syncope
persistence.domain[0].databasePlatform=org.apache.openjpa.jdbc.sql.OracleDictionary
persistence.domain[0].orm=META-INF/oracle/spring-orm.xml
persistence.domain[0].poolMaxActive=20
persistence.domain[0].poolMinIdle=5
```

as `core/src/main/resources/core-oracle.properties`.

Do not forget to include `oracle` as
[Spring Boot profile](https://docs.spring.io/spring-boot/3.4/reference/features/profiles.html#features.profiles.adding-active-profiles)
for the Core application.

|  | This assumes that you have an Oracle instance running on localhost, listening on its default port 1521 with a databasesyncopeunder tablespaceSYNCOPE, fully accessible by usersyncopewith passwordsyncope. |
| --- | --- |

### [5.3. High-Availability](#syncope-apache-org-docs-4-0-reference-guide--high-availability)

##### OpenJPA

When deploying multiple Syncope [Core](#syncope-apache-org-docs-4-0-reference-guide--core) instances with a single database or database cluster, it is of
fundamental importance that the contained OpenJPA instances are correctly configured for
[remote event notification](https://openjpa.apache.org/builds/4.0.1/apache-openjpa/docs/ref_guide_event.html).  
Such configuration, in fact, allows the OpenJPA data cache to remain synchronized when deployed in multiple JVMs, thus
enforcing data consistency across all Syncope Core instances.

The default configuration in `core.properties` is

```properties
persistence.remoteCommitProvider=sjvm
```

which is suited for single JVM installations; with multiple instances, more options like as TCP, JMS or Kubernetes
are available; see the OpenJPA documentation for reference.

|  | TheOpenJPA documentation's XML snippets refer to a different configuration style; for example, when used incore.properties, this:<property name="openjpa.RemoteCommitProvider" value="tcp(Addresses=10.0.1.10;10.0.1.11,TransmitPersistedObjectIds=true)"/>becomes:persistence.remoteCommitProvider=tcp(Addresses=10.0.1.10;10.0.1.11,TransmitPersistedObjectIds=true) |
| --- | --- |

### [5.4. Domains Management](#syncope-apache-org-docs-4-0-reference-guide--domains-management)

Besides the pre-defined `Master` domain, other [Domains](#syncope-apache-org-docs-4-0-reference-guide--domains) are bootstrapped during [Core](#syncope-apache-org-docs-4-0-reference-guide--core) startup from three files
in the [configuration directory](#syncope-apache-org-docs-4-0-reference-guide--properties-files-location); assuming that the domain name is `Two`, such files are:

- `domains/TwoSecurity.json` - admin credentials;
- `domains/TwoKeymasterConfParams.json` - for [Keymaster](#syncope-apache-org-docs-4-0-reference-guide--keymaster) initialization;
- `domains/TwoContent.xml` - for [content](#syncope-apache-org-docs-4-0-reference-guide--import) initialization.

|  | Starting from Syncope 3.0 it is also possible to create, update and delete Domains at runtime by managing the related configuration on the configuredKeymasterinstance. |
| --- | --- |

### [5.5. ConnId locations](#syncope-apache-org-docs-4-0-reference-guide--connid-locations)

Core can be configured to use either local or remote [connector bundles](#syncope-apache-org-docs-4-0-reference-guide--connector-bundles):

- **local** connector bundles are located somewhere in the same filesystem where Apache Syncope is deployed;
- **remote** connector bundles are provided via Java or .NET
  [connector server](https://connid.atlassian.net/wiki/display/BASE/Connector+Servers).

While local connector bundles feature an easy setup, remote connector bundles allow enhanced deployment scenarios and
are particularly useful when it is needed to deal with architectural security constraints or when a connector bundle
requires to run on a specific platform OS (say MS Windows) while Apache Syncope is deployed on another platform OS
(say GNU/Linux).

The `core.properties` file holds the configuration for defining which ConnId locations (either local or remote)
will be considered.

The format is quite straightforward:

```properties
provisioning.connIdLocation=location1,\
location2,\
...
locationN
```

where each location is the string representation of an URI of the form `file:/path/to/directory/` for local locations,
`connid://key@host:port` for remote non-SSL connector servers or finally `connids://key@host:port[?trustAllcerts=true]`
for remote SSL connector servers, with optional flag to disable certificate check.

Example 37. Single local location

```properties
provisioning.connIdLocation=file:/opt/syncope/bundles/
```

Example 38. Single remote location

```properties
provisioning.connIdLocation=connid://sampleKey@windows2008:4554
```

Example 39. Multiple locations

```properties
provisioning.connIdLocation=file:/opt/syncope/bundles/,\
file:/var/tmp/bundles/,\
connid://sampleKey@windows2008:4554,\
connids://anotherKey@windows2008:4559,\
connids://aThirdKey@linuxbox:9001?trustAllCerts=true
```

### [5.6. Install connector bundles](#syncope-apache-org-docs-4-0-reference-guide--install-connector-bundles)

[Connector bundles](#syncope-apache-org-docs-4-0-reference-guide--connector-bundles) are made available as JAR files and can be configured, for a given deployment:

- for Maven project, in local sources;
- for all distributions, at run-time.

#### [5.6.1. Local sources](#syncope-apache-org-docs-4-0-reference-guide--local-sources)

##### [Different version of predefined connector bundle](#syncope-apache-org-docs-4-0-reference-guide--different-version-of-predefined-connector-bundle)

First of all, verify which connector bundles are predefined in your project by looking at your project’s parent
[POM](https://repo1.maven.org/maven2/org/apache/syncope/syncope/4.0.5).

As you can see, there are several Maven properties on the form `connid.*.version`, controlling the related connector
bundle’s version.

If you want your own project to use a different version of a given connector bundle, all you need to do is to override
the related property in your own project’s root pom.xml.

Hence, supposing that you would like to use `net.tirasa.connid.bundles.db` version `3.0.0-SNAPSHOT` rather than
the one with version shipped with Apache Syncope, add the following property to your own project’s root `pom.xml`:

```xml
<properties>
   ...
   <connid.db.version>3.0.0-SNAPSHOT</connid.db.version>
</properties>
```

##### [Non-predefined connector bundle](#syncope-apache-org-docs-4-0-reference-guide--non-predefined-connector-bundle)

If the needed connector bundle is not in the predefined set as shown above, you will need to add a new property into
your own project’s root `pom.xml`:

```xml
<properties>
   ...
   <my.new.connector.version>1.0.0</my.new.connector.version>
</properties>
```

then change the `maven-dependency-plugin` configuration both in `core/pom.xml` and `console/pom.xml` from

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-dependency-plugin</artifactId>
  <inherited>true</inherited>
  <executions>
    <execution>
      <id>set-bundles</id>
      <phase>process-test-resources</phase>
      <goals>
        <goal>copy</goal>
      </goals>
    </execution>
  </executions>
</plugin>
```

to

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-dependency-plugin</artifactId>
  <inherited>true</inherited>
  <configuration>
    <artifactItems>
      <artifactItem>
        <groupId>my.new.connector.groupId</groupId>
        <artifactId>my.new.connector.artifactId</artifactId>
        <version>${my.new.connector.version}</version>
        <classifier>bundle</classifier>
      </artifactItem>
    </artifactItems>
  </configuration>
  <executions>
    <execution>
      <id>set-bundles</id>
      <phase>process-test-resources</phase>
      <goals>
        <goal>copy</goal>
      </goals>
    </execution>
  </executions>
</plugin>
```

#### [5.6.2. Run-time](#syncope-apache-org-docs-4-0-reference-guide--run-time)

Connector bundles can be added or replaced at run-time by performing the following steps:

1. [Download](https://github.com/Tirasa/ConnId/#available-connectors) the required connector bundle
   JAR file;
2. Copy the downloaded JAR file into one of configured [ConnId locations](#syncope-apache-org-docs-4-0-reference-guide--connid-locations), typically the
   `bundles` directory where the other connector bundles are already available.

### [5.7. E-mail Configuration](#syncope-apache-org-docs-4-0-reference-guide--e-mail-configuration)

The `core.properties` file holds the configuration options to enable the effective delivery of
[notification](#syncope-apache-org-docs-4-0-reference-guide--notifications) e-mails:

- `spring.mail.host` - the mail server host, typically an SMTP host;
- `spring.mail.port` - the mail server port;
- `spring.mail.username` - (optional) the username for the account at the mail host;
- `spring.mail.password` - (optional) the password for the account at the mail host;
- `spring.mail.properties.mail.smtp.auth` - when `true`, the configured `username` and `password` are sent to SMTP server;
- `spring.mail.properties.mail.smtp.starttls.enable` - when `true`, enable the use of the `STARTTLS` command to switch the connection to a
  TLS-protected connection before issuing any login commands;

All the [JavaMail™ properties](https://javaee.github.io/javamail/docs/api/com/sun/mail/smtp/package-summary.html#properties)
are available for usage with prefix `spring.mail.properties.`.

Example 40. Basic configuration, no authentication

```properties
spring.mail.host=your.local.smtp.server
spring.mail.port=25
spring.mail.username=
spring.mail.password=
spring.mail.properties.mail.smtp.auth=false
spring.mail.properties.mail.smtp.starttls.enable=false
```

Example 41. STARTTLS configuration, with authentication

```properties
spring.mail.host=smtp.gmail.com
spring.mail.port=587
spring.mail.username=your_username@gmail.com
spring.mail.password=your_password
spring.mail.properties.mail.smtp.auth=true
spring.mail.properties.mail.smtp.starttls.enable=true
```

|  | In order to make the changes tocore.propertieseffective, the deployment needs to be restarted. |
| --- | --- |

|  | Be sure to provide a sensible value for thenotificationjob.cronExpressionconfiguration parameter, otherwise thenotification taskswill not be triggered; seebelowfor details. |
| --- | --- |

### [5.8. Control JWT signature](#syncope-apache-org-docs-4-0-reference-guide--control-jwt-signature)

As explained [above](#syncope-apache-org-docs-4-0-reference-guide--rest-authentication-and-authorization), the REST authentication process generates, in case of
success, a unique signed JWT (JSON Web Token).  
Such JWT values are signed by Apache Syncope according to the [JWS](https://tools.ietf.org/html/rfc7515)
(JSON Web Signature) specification.

#### [5.8.1. Hash-based Message Authentication Code](#syncope-apache-org-docs-4-0-reference-guide--jws-hmac)

This is the default configuration, where Core and clients posses a shared secret, configured under `core.properties`
as the `jwsKey` property value.

Example 42. Default JWS configuration

```properties
security.jwsAlgorithm=HS512 (1)
security.jwsKey=ZW7pRixehFuNUtnY5Se47IemgMryTzazPPJ9CGX5LTCmsOJpOgHAQEuPQeV9A28f (2)
```

| 1 | Valid values areHS256,HS384andHS512 |
| --- | --- |
| 2 | Any alphanumeric value satisfying thelength requirementcan be used |

#### [5.8.2. RSA Public-Key Cryptography](#syncope-apache-org-docs-4-0-reference-guide--jws-rsa)

This configuration requires to specify a key pair: the former key value, said *private*, must be kept secret for internal
Core usage while the latter key value, said *public*, is to be shared with clients.

The commands below will generate the required key pair via OpenSSL and format their values for usage with
`core.properties`:

```bash
$ openssl genrsa -out private_key.pem 2048
$ openssl pkcs8 -topk8 -in private_key.pem -inform pem -out jws.privateKey -outform pem -nocrypt
$ openssl rsa -pubout -in private_key.pem -out jws.publicKey
$ echo `sed '1d;$d' jws.privateKey | awk '{printf "%s", $0}'`:`sed '1d;$d' jws.publicKey | awk '{printf "%s", $0}'`
```

Example 43. JWS configuration with RSA PKCS#1

```properties
security.jwsAlgorithm=RS512 (1)
security.jwsKey=MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCdXTaAPRoIAvWjm5MskNtcGakkME4HEhZ8oQ2J8XNU29ZT7Qq5TP769/O8OH5Pb56mPULswYSocycrAARPzjAKpxr+YN7w2/zo5MsBRZsASgpCxnCeYLCWtJzmzY/YYlAHdsu3jj/4wuAcYozR1xE5e2gEj0BQ6Xz7NELhceEZpbXIeKSDolLdCKrVZ1vdD0q/HdjY2qeBACqeG8yYXsj2MiAMJY6df80ZCqpHkcD9mhfzqUo5EcWCD7XzcOJQRNUKkBEObemq//tt5NHFbWnBeGeTJBcyXV7Uqqbjnd6hwBBS1d6usAagGQ4RWDHPBMk02BdEFyrZjgJXM1C1iU/9AgMBAAECggEBAJpbnaNKzCcBqCuU5ld3vARqk1QRIeijoHUdkWc29TdO8LygLr22vgI1h9qf255V0dwlCWmtJVAKrGfse05A5TT912egY+8FCt7z1gFoYnN1LP11I3DnTTB299UZ3DiXrwKzT368xRlhJm4RaSpIePfWiiC215LGhTbve48iongBXzkpzFYe1SCV1FmNl5Px6FE3C9GcTrFpe+rqVcIVrTLZ95+JDF4/YLgTRccW8V/YO+4OtqUo+vt8tckDGhrHrfwgTo53kxDQttecB4AryDg1eUe8vPMx1+yJz8VFwx0yaUa5fqEYlxPehRQiVJi0+YMosRqKtcm1mLxoGcwSyo0CgYEAynhB/FM9DnARwg/PsE/AuXVpXlxPU5F+shpYX2sF3rItTD4EWFr/glo26LT/MLw2ckNkLT11yAWdR8hAzVZ48Ly3Ur8Fi88iInLPEixunBIsPcR3dI2UoI9dswnTM+H/Z83yQ16VWGjtE3437LWSXBHEw/am9W9pArEunt3TQz8CgYEAxvgS7BAokIqASi0zBpmyogRVHGs0eC3mMWLG+t5VXJ5M1z1pV9dOuInnI29wJqBscefueOPcT6mNJngW/kHlcGGOxij+hRUnAdVltTod4CJ3Q/IyM6h/FzunEeumZyZ1BW3G5KTcpegcBquUW6impyJbnUvKV4p9rpLTEBooKcMCgYEAhB1skUWPdbhTHhpLH3UrANlIZDY/3Pv3fCgMulaPgf0p6bIeC7l1OI29fqN8UUS/Elg/KfYMwPRI6OoWvuZKDGxYAzp6V/xU/b2EuQsdMeH51GQ6vmcUMKDcN1OV6SjzC70q9CLnuMTezfVycJcaZdGCX4y27ThBgWw0S53bmOkCgYAdCHfiYF068irUKBJJBUZuo8kzk2UdoDz1ud8lHipAkIzP35MukSlYfi7vGcS4rjIE0P4YP8+XBDungGCCi2UKaAHoYnT5QGPnvZbQwgE4Am96x62RoiWhYz/2uncWmCL9Ps6F8JSN1Pe59XF5int+6eGKa1PEQF4kiiIoOFjh9wKBgG6XXGl84fBaOaTsCPu+oQcAAp1GzweSy4l1Y1L71YvbxU1bs5338vgiH5OeUA4d5w0Ei9d/bSw0PWV4aACWWGGclLhzv8ia6bEWqt0TskUiUJVzgTXWp3ojpsP/QE36Ty+uWWqckBXv6dnEXEgrLqzbA6qTAohSSFjV4FAjxBxa:MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAnV02gD0aCAL1o5uTLJDbXBmpJDBOBxIWfKENifFzVNvWU+0KuUz++vfzvDh+T2+epj1C7MGEqHMnKwAET84wCqca/mDe8Nv86OTLAUWbAEoKQsZwnmCwlrSc5s2P2GJQB3bLt44/+MLgHGKM0dcROXtoBI9AUOl8+zRC4XHhGaW1yHikg6JS3Qiq1Wdb3Q9Kvx3Y2NqngQAqnhvMmF7I9jIgDCWOnX/NGQqqR5HA/ZoX86lKORHFgg+183DiUETVCpARDm3pqv/7beTRxW1pwXhnkyQXMl1e1Kqm453eocAQUtXerrAGoBkOEVgxzwTJNNgXRBcq2Y4CVzNQtYlP/QIDAQAB (2)
```

| 1 | Valid values areRS256,RS384andRS512 |
| --- | --- |
| 2 | Value is obtained by the commands above; the public key value is the string after the:sign, e.g. |

```text
QB3bLt44/+MLgHGKM0dcROXtoBI9AUOl8+zRC4XHhGaW1yHikg6JS3Qiq1Wdb3Q9Kvx3Y2NqngQAqnhvMmF7I9jIgDCWOnX/NGQqqR5HA/ZoX86lKORHFgg+183DiUETVCpARDm3pqv/7beTRxW1pwXhnkyQXMl1e1Kqm453eocAQUtXerrAGoBkOEVgxzwTJNNgXRBcq2Y4CVzNQtYlP/QIDAQAB
```

|  | Longer RSA keys offer stronger protection against cracking. The JWS specification suggests at least 2048 bits. Please consider that higher CPU usage is involved with longer keys. |
| --- | --- |

### [5.9. Configuration Parameters](#syncope-apache-org-docs-4-0-reference-guide--configuration-parameters)

Most run-time configuration options are available as parameters and can be tuned via the admin console:

- `password.cipher.algorithm` - which cipher algorithm shall be used for encrypting password values; supported
  algorithms include `SHA-1`, `SHA-256`, `SHA-512`, `AES`, `S-MD5`, `S-SHA-1`, `S-SHA-256`, `S-SHA-512` and `BCRYPT`;
  salting options are available in the `core.properties` file;

  |  | The value of thesecurity.aesSecretKeyproperty in thecore.propertiesfile is used for AES-based encryption / decryption: besides password values, this is also used whenever reversible encryption is needed, throughout the whole system.The actual length of thesecurity.aesSecretKeyvalue is used to drive the AES algorithm variant selection: 16 characters impliesAES-128, 24 selectsAES-192and 32 configuresAES-256.When thesecurity.aesSecretKeyvalue has length less than 16, between 17 and 23 or between 25 and 31, it is right-padded by random characters during startup, to reach the nearest option. If the specified value is instead longer than 32 characters, it is truncated to 32.It isstronglyrecommended to provide a value long exactly 16, 24 or 32 characters, in order to avoid unexpected behaviors at runtime, expecially with high-availability. |
  | --- | --- |
- `jwt.lifetime.minutes` - validity of [JSON Web Token](https://en.wikipedia.org/wiki/JSON_Web_Token) values used for
  [authentication](#syncope-apache-org-docs-4-0-reference-guide--rest-authentication-and-authorization) (in minutes);
- `notificationjob.cronExpression` -
  [cron](https://docs.spring.io/spring-framework/reference/6.2/integration/scheduling.html#scheduling-cron-expression) expression describing how
  frequently the pending [notification tasks](#syncope-apache-org-docs-4-0-reference-guide--tasks-notification) are processed: empty means disabled;

  |  | Restarting the deployment is required when changing value for this parameter. |
  | --- | --- |
- `notification.maxRetries` - how many times the delivery of a given notification should be attempted before giving up;

  |  | Restarting the deployment is required when changing value for this parameter. |
  | --- | --- |
- `token.length` - the length of the random tokens that can be generated as part of various [workflow](#syncope-apache-org-docs-4-0-reference-guide--workflow)
  processes, including [password reset](#syncope-apache-org-docs-4-0-reference-guide--password-reset);
- `token.expireTime` - the time after which the generated random tokens expire;
- `selfRegistration.allowed` - whether self-registration (typically via the enduser application) is allowed;
- `passwordReset.allowed` - whether the [password reset](#syncope-apache-org-docs-4-0-reference-guide--password-reset) feature (typically via the enduser
  application) is allowed;
- `passwordReset.securityQuestion` - whether the [password reset](#syncope-apache-org-docs-4-0-reference-guide--password-reset) feature involves security questions;
- `authentication.attributes` - the list of attributes whose values can be passed as login name for authentication,
  defaults to `username`; please note that the related [plain schemas](#syncope-apache-org-docs-4-0-reference-guide--plain) must impose the unique constraint, for this
  mechanism to work properly;
- `authentication.statuses` - the list of [workflow](#syncope-apache-org-docs-4-0-reference-guide--workflow) statuses for which users are allowed to authenticate;

  |  | Suspended Users are anyway not allowed to authenticate. |
  | --- | --- |
- `log.lastlogindate` - whether the system updates the `lastLoginDate` field of users upon authentication;
- `return.password.value` - whether the hashed password value and the hashed security answer (if any) value shall be
- `connector.test.timeout` - timeout (in seconds) to check connector connection in [Admin Console](#syncope-apache-org-docs-4-0-reference-guide--admin-console);
  `0` to skip any check;

|  | This parameter is useful to avoid waiting for the default connector timeout, by setting a shorter value; or to completely disable connector connection testing. |
| --- | --- |

- `resource.test.timeout` - timeout (in seconds) to check resource connection in [Admin Console](#syncope-apache-org-docs-4-0-reference-guide--admin-console);
  `0` to skip any check;

|  | This parameter is useful to avoid waiting for the default resource timeout, by setting a shorter value; or to completely disable resource connection testing. |
| --- | --- |

Besides this default set, new configuration parameters can be defined to support [custom](#syncope-apache-org-docs-4-0-reference-guide--customization) code.

## [6. HOWTO](#syncope-apache-org-docs-4-0-reference-guide--howto)

### [6.1. Set admin credentials](#syncope-apache-org-docs-4-0-reference-guide--set-admin-credentials)

|  | The procedure below affects only theMasterdomain; for other domains checkabove. |
| --- | --- |

The credentials are defined in the `core.properties` file; text encoding must be set to UTF-8:

- `security.adminUser` - administrator username (default `admin`)
- `security.adminPassword` - administrator password (default `password`)'s hashed value
- `security.adminPasswordAlgorithm` - algorithm to be used for hash evaluation (default `SSHA256`, also supported are
  `SHA1`, `SHA256`, `SHA512`, `SMD5`, `SSHA1`, `SSHA512` and `BCRYPT`)

Example 44. Generate SHA1 password value on GNU / Linux

The `sha1sum` command-line tool of [GNU Core Utilities](http://www.gnu.org/software/coreutils/) can be used as follows:

```bash
echo -n "new_password" | sha1sum
```

Please beware that any shell special character must be properly escaped for the command above to produce the expected
hashed value.

Example 45. Generate SSHA256 password value on GNU / Linux

```bash
$ python3 pySSHA/ssha.py -p password -enc sha256 -s 666ac543 \
 | sed 's/{.*}//' | xargs echo -n | base64 -d | xxd -p | tr -d $'\n' | xargs echo
```

Several tools involved here:

- [pySSHA-slapd](https://github.com/peppelinux/pySSHA-slapd)
- [xargs](http://man7.org/linux/man-pages/man1/xargs.1.html)
- [echo](http://man7.org/linux/man-pages/man1/echo.1.html)
- [base64](http://man7.org/linux/man-pages/man1/base64.1.html)
- [xxd](https://linux.die.net/man/1/xxd)
- [tr](http://man7.org/linux/man-pages/man1/tr.1.html)

The command above will:

1. generate a `SHA256` hash for input value `password` with suffixed salt `666ac543` (4 bytes in hex format), via `ssha.py`
2. remove the `{SSHA256}` prefix from the generated value and newline, via `sed` and `xargs`
3. since the generated value is Base64-encoded while Syncope requires Hexadecimal format, perform the required conversion
   via `base64`, `xxd` and `tr`
4. append newline to ease copy / paste, via `xargs` and `echo`

### [6.2. Internal storage export - import](#syncope-apache-org-docs-4-0-reference-guide--internal-storage-export-import)

Almost every configurable aspect of a given deployment is contained in the [internal storage](#syncope-apache-org-docs-4-0-reference-guide--persistence):
schemas, connectors, resources, mapping, roles, groups, tasks and other parameters.

During the implementation phase of an Apache Syncope-based project, it might be useful to move such configuration back
and forth from one Apache Syncope instance to another (say developer’s laptop and production server).  
One option is clearly to act at a low level by empowering DBMS' dump & restore capabilities, but what if the developer
is running MySQL while the sysadmin features Oracle?

|  | Wipe existing contentThe internal storage’s data must be wiped before starting Apache Syncope, otherwise the provided content will be just ignored.Checkcore-persistence.logfor messageEmpty database found, loading default contentIf the internal storage is not empty, instead, you will getData found in the database, leaving untouched |
| --- | --- |

|  | All references in the following are set toMasterContent.xml; when otherdomainsare defined, the content file is renamed accordingly. For example,TwoContent.xmlif domain name isTwo. |
| --- | --- |

|  | MySQL and lower case table namesOn some platforms (namely, Mac OS X) MySQL is configured by default to be case insensitive: in such cases, you might want to edit the/etc/my.cnffile and add the following line in the[mysqld]section:lower_case_table_names=1 |
| --- | --- |

#### [6.2.1. Export](#syncope-apache-org-docs-4-0-reference-guide--export)

This task can be accomplished either via the admin console or by barely invoking the REST layer through
[curl](http://curl.haxx.se/), for example:

```bash
curl -X GET -u admin:password -o MasterContent.xml \
  http://localhost:9080/syncope/rest/configurations/stream?threshold=100
```

where `threshold` indicates the maximum number of rows to take for each element of internal storage.

It is possible to specify which element(s) to include in the export:

```bash
curl -X GET -u admin:password -o MasterContent.xml \
  http://localhost:9080/syncope/rest/configurations/stream?elements=Realm&elements=PushPolicy
```

which will include only `Realm` and `PushPolicy` elements from internal storage.

#### [6.2.2. Import](#syncope-apache-org-docs-4-0-reference-guide--import)

Basically, all you need to do is to replace the local `MasterContent.xml` with the one exported as explained above; this
file is located at:

- `$TOMCAT_HOME/webapps/syncope/WEB-INF/classes/domains/MasterContent.xml` for Standalone
- `core/src/test/resources/domains/MasterContent.xml` for Maven projects in embedded mode
- `core/src/main/resources/domains/MasterContent.xml` for Maven projects

### [6.3. Keystore](#syncope-apache-org-docs-4-0-reference-guide--keystore)

A [Java Keystore](https://en.wikipedia.org/wiki/Keystore) is a container for authorization certificates or public key
certificates, and is often used by Java-based applications for encryption, authentication, and serving over HTTPS.
Its entries are protected by a keystore password. A keystore entry is identified by an alias, and it consists of keys
and certificates that form a trust chain.

A keystore is currently required by the [SAML 2.0 Service Provider for UI](#syncope-apache-org-docs-4-0-reference-guide--saml2sp4ui) extension in order to sign and / or encrypt the
generated SAML 2.0 requests.

While a sample keystore is provided, it is **strongly** recommended to setup a production keystore; in the following, a
reference procedure for this is reported.

|  | The procedure below is not meant to cover all possible options and scenarios for generating a keystore, nor to provide complete coverage of thekeytoolcommand. |
| --- | --- |

##### Create new keystore

```bash
keytool -genkey \
  -keyalg RSA \
  -keysize 2048 \
  -alias saml2sp4ui \
  -dname "CN=SAML2SP,OU=Apache Syncope, O=The ASF, L=Wilmington, ST=Delaware, C=US" \
  -keypass akyepass \
  -storepass astorepass \
  -storetype JKS \
  -keystore saml2sp4ui.jks
```

This command will create a keystore file with name `saml2sp4ui.jks` in the execution directory, containing a new 2048-bit
RSA key pair, under the specified alias (`saml2sp4ui`); password values for `keypass` and `storepass` are also set.

##### Create new CSR

```bash
keytool -certreq \
  -alias saml2sp4ui \
  -keyalg RSA \
  -file certreq.pem \
  -keypass akyepass \
  -storepass astorepass \
  -storetype JKS \
  -keystore saml2sp4ui.jks
```

This command will create a CSR file with name `certreq.pem` in the execution directory, within the keystore generated
above.  
The generated CSR file can be sent to a Certificate Authority (CA) to request the issuance of a CA-signed certificate.

##### Have the CSR signed by a Certificate Authority (CA)

This step cannot be automated, and is definitely out of the scope of the this document.

Before proceeding, it is fundamental to have ready the root / intermediate CA certificate(s) and the signed certificate.

##### Import the certificates into the keystore

```bash
keytool -import \
  -alias root \
  -file cacert.pem \
  -keypass akyepass \
  -storepass astorepass \
  -storetype JKS \
  -keystore saml2sp4ui.jks
```

This command will import the root / intermediate CA certificate(s) from the `cacert.pem` file into the keystore
generated above.

```bash
keytool -import \
  -alias saml2sp4ui \
  -file cert.pem \
  -keypass akyepass \
  -storepass astorepass \
  -storetype JKS \
  -keystore saml2sp4ui.jks
```

This command will import the signed certificate from the `cert.pem` file into the keystore generated above.

##### Finalize

The keystore file `saml2sp4ui.jks` can now be placed in the [configuration directory](#syncope-apache-org-docs-4-0-reference-guide--properties-files-location); the
relevant part of the `core.properties` file should be:

```text
saml2.sp4ui.keystore=file://${syncope.conf.dir}/saml2sp4ui.jks
saml2.sp4ui.keystore.type=jks
saml2.sp4ui.keystore.storepass=astorepass
saml2.sp4ui.keystore.keypass=akyepass
```

### [6.4. Upgrade from 3.0](#syncope-apache-org-docs-4-0-reference-guide--upgrade-from-3-0)

Planning the upgrade of an existing Apache Syncope 3.0 deployment to Syncope 4.0 can be achieved by following the
indications below.

#### [6.4.1. Preparation](#syncope-apache-org-docs-4-0-reference-guide--preparation)

First of all, update the existing Syncope 3.0 deployment’s code / Docker images to the latest
[3.0 release](https://cwiki.apache.org/confluence/display/SYNCOPE/Maggiore) available.

Also, ensure to have a full backup of the existing database used as [internal storage](#syncope-apache-org-docs-4-0-reference-guide--persistence).

#### [6.4.2. Persistence Storage upgrade](#syncope-apache-org-docs-4-0-reference-guide--persistence-storage-upgrade)

|  | Ensure tocheck the compatibilityof the existing DBMS and upgrade, if needed. |
| --- | --- |

|  | The existing persistence data from Syncope 3.0 can be migrated to Syncope 4.0 only if one of the JSON persistence flavors were in use:PostgreSQL JSONBMySQL JSONMariaDB JSONOracle Database JSONIf it is not your case, then you can applythe same strategy providedfor the migration from Syncope 2.1 to Syncope 3.0. |
| --- | --- |

Download [`syncope-core-persistence-jpa-upgrader-4.0.5.jar`](https://syncope.apache.org/downloads):
this tool will generate the full set of SQL statements required to upgrade the internal storage to Syncope 4.0.

Run the tool depending on your actual DBMS by using the latest JDK 21 available; the generated SQL statements will be
sent to a new `upgrade.sql` file.

##### PostgreSQL

```bash
$ java \
 -Dloader.path=/path/to/postgresql.jar
 -Ddb.jdbcURL="jdbc:postgresql://localhost:5432/syncope?stringtype=unspecified" \
 -Ddb.username=syncope -Ddb.password=syncope \
 -jar syncope-core-persistence-jpa-upgrader-4.0.5.jar \
 upgrade.sql
```

assuming that:

- `/path/to/postgresql.jar` is the full path to the latest JDBC driver available for PostgreSQL
- you have a PostgreSQL instance running on `localhost`, listening on its default port `5432` with a database
  `syncope` fully accessible by user `syncope` with password `syncope`

##### MySQL

```bash
$ java \
 -Dloader.path=/path/to/mysql.jar
 -Dspring.profiles.active=mysql
 -Ddb.jdbcURL="jdbc:mysql://localhost:3306/syncope?useSSL=false&allowPublicKeyRetrieval=true" \
 -Ddb.username=syncope -Ddb.password=syncope \
 -jar syncope-core-persistence-jpa-upgrader-4.0.5.jar \
 upgrade.sql
```

assuming that:

- `/path/to/mysql.jar` is the full path to the latest JDBC driver available for MySQL
- you have a MySQL instance running on `localhost`, listening on its default port `3306` with a database
  `syncope` fully accessible by user `syncope` with password `syncope`

##### MariaDB

```bash
$ java \
 -Dloader.path=/path/to/mariadb.jar
 -Dspring.profiles.active=mariadb
 -Ddb.jdbcURL="jdbc:mariadb://localhost:3306/syncope?characterEncoding=UTF-8" \
 -Ddb.username=syncope -Ddb.password=syncope \
 -jar syncope-core-persistence-jpa-upgrader-4.0.5.jar \
 upgrade.sql
```

assuming that:

- `/path/to/mariadb.jar` is the full path to the latest JDBC driver available for MariaDB
- you have a MariaDB instance running on `localhost`, listening on its default port `3306` with a database
  `syncope` fully accessible by user `syncope` with password `syncope`

##### Oracle Database

```bash
$ java \
 -Dloader.path=/path/to/ojdbc11.jar
 -Dspring.profiles.active=oracle
 -Ddb.jdbcURL="jdbc:oracle:thin:@localhost}:1521/FREEPDB1" \
 -Ddb.username=syncope -Ddb.password=syncope \
 -jar syncope-core-persistence-jpa-upgrader-4.0.5.jar \
 upgrade.sql
```

assuming that:

- `/path/to/ojdbc11.jar` is the full path to the latest JDBC driver available for Oracle Database
- you have an Oracle instance running on `localhost`, listening on its default port `1521` with a database
  `syncope` fully accessible by user `syncope` with password `syncope`

#### [6.4.3. Finalization](#syncope-apache-org-docs-4-0-reference-guide--finalization)

1. shutdown the standalone process or the Java EE container running Apache Syncope 3.0 Core, to ensure no changes
   are pushed to the internal storage
2. execute the SQL statements as generated above against the internal storage: in case of errors, apply manual
   corrections until everything runs clear; consider to restore from the backup taken above if needed, before executing
   the updated SQL statements again
3. start the standalone process or the Jakarta EE container of Apache Syncope 4.0 Core, and watch the log files
   to check for any error