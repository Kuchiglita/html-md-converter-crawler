<a id="tomee-apache-org-tomee-10-1-docs-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Documentation

Testing Techniques

- [Alternate Descriptors](#tomee-apache-org-tomee-10-1-docs-alternate-descriptors)
- [Application discovery via the classpath](#tomee-apache-org-tomee-10-1-docs-application-discovery-via-the-classpath)
- [Configuring Containers in Tests](#tomee-apache-org-tomee-10-1-docs-configuring-containers-in-tests)
- [Configuring DataSources in Tests](#tomee-apache-org-tomee-10-1-docs-configuring-datasources-in-tests)
- [Configuring Logging in Tests](#tomee-apache-org-tomee-10-1-docs-configuring-logging-in-tests)
- [Configuring PersistenceUnits in Tests](#tomee-apache-org-tomee-10-1-docs-configuring-persistenceunits-in-tests)
- [Functional testing with OpenEJB, Jetty and Selenium](#tomee-apache-org-tomee-10-1-docs-functional-testing-with-openejb-jetty-and-selenium)
- [Local Client Injection](#tomee-apache-org-tomee-10-1-docs-local-client-injection)
- [OpenEJB Embedded Configuration](#tomee-apache-org-tomee-10-1-docs-embedded-configuration)

Discovery and Failover

- [EJB Client/Server Failover](#tomee-apache-org-tomee-10-1-docs-ejb-failover)
- [failover-logging](#tomee-apache-org-tomee-10-1-docs-failover-logging)
- [Multicast (UDP) Discovery](#tomee-apache-org-tomee-10-1-docs-multicast-discovery)
- [Multipoint (TCP) Discovery](#tomee-apache-org-tomee-10-1-docs-multipoint-discovery)
- [Multipoint Considerations](#tomee-apache-org-tomee-10-1-docs-multipoint-considerations)
- [Multipoint Recommendations](#tomee-apache-org-tomee-10-1-docs-multipoint-recommendations)
- [MultiPulse (UDP) Discovery](#tomee-apache-org-tomee-10-1-docs-multipulse-discovery)

Datasource

- [Common DataSource Configurations](#tomee-apache-org-tomee-10-1-docs-common-datasource-configurations)
- [DataSource Configuration](#tomee-apache-org-tomee-10-1-docs-datasource-config)
- [DataSource Creator](#tomee-apache-org-tomee-10-1-docs-datasource-configuration-by-creator)
- [DataSource Password Encryption](#tomee-apache-org-tomee-10-1-docs-datasource-password-encryption)
- [Dynamic Datasource](#tomee-apache-org-tomee-10-1-docs-dynamic-datasource)
- [XA DataSource Configuration](#tomee-apache-org-tomee-10-1-docs-configuring-datasources-xa)

JPA

- [JPA Concepts](#tomee-apache-org-tomee-10-1-docs-jpa-concepts)
- [JPA Usage](#tomee-apache-org-tomee-10-1-docs-jpa-usage)
- [OpenJPA](#tomee-apache-org-tomee-10-1-docs-openjpa)
- [persistence-context](#tomee-apache-org-tomee-10-1-docs-persistence-context)
- [persistence-unit-ref](#tomee-apache-org-tomee-10-1-docs-persistence-unit-ref)
- [TomEE and Hibernate](#tomee-apache-org-tomee-10-1-docs-tomee-and-hibernate)

TomEE Maven Plugin

- [TomEE Embedded Maven Plugin](#tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin)
- [TomEE Maven Plugin - Configuration](maven/index.html)
- [TomEE Maven Plugin - Getting Started](#tomee-apache-org-tomee-10-1-docs-tomee-mp-getting-started)
- [TomEE Maven Plugin - Goals & Configuration](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-tomee)
- [TomEE Maven Plugins - Introduction](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-plugins)

IDE

- [Debugging an Apache](#tomee-apache-org-tomee-10-1-docs-contrib-debug-debug-intellij)
- [OpenEJB Eclipse Plugin](#tomee-apache-org-tomee-10-1-docs-openejb-eclipse-plugin)
- [TomEE and Eclipse](#tomee-apache-org-tomee-10-1-docs-tomee-and-eclipse)
- [TomEE and Intellij](#tomee-apache-org-tomee-10-1-docs-tomee-and-intellij)
- [TomEE and NetBeans](#tomee-apache-org-tomee-10-1-docs-tomee-and-netbeans)

Tips and Tricks

- [Global Concurrency Management](#tomee-apache-org-tomee-10-1-docs-tip-concurrency)
- [Installing TomEE using the drop-in .war approach](#tomee-apache-org-tomee-10-1-docs-installation-drop-in-war)
- [Jersey Client](#tomee-apache-org-tomee-10-1-docs-tip-jersey-client)
- [WebLogic Lookup](#tomee-apache-org-tomee-10-1-docs-tip-weblogic)

General Information

- [About the 'tomee' webapp](#tomee-apache-org-tomee-10-1-docs-tomee-webapp)
- [Comparison](#tomee-apache-org-tomee-10-1-docs-comparison)
- [deploying-in-tomee](#tomee-apache-org-tomee-10-1-docs-deploying-in-tomee)
- [TomEE Directory Structure](#tomee-apache-org-tomee-10-1-docs-tomee-directory-structure)

Testing

- [Application Composer](#tomee-apache-org-tomee-10-1-docs-application-composer-index)
- [Application Composer Advanced](#tomee-apache-org-tomee-10-1-docs-application-composer-advanced)
- [Application Composer Getting Started](#tomee-apache-org-tomee-10-1-docs-application-composer-getting-started)
- [Application Composer History](#tomee-apache-org-tomee-10-1-docs-application-composer-history)

Installation

- [Installing TomEE](#tomee-apache-org-tomee-10-1-docs-installing-tomee)
- [Linux Service](#tomee-apache-org-tomee-10-1-docs-deamon-lin-service)
- [Windows Service](#tomee-apache-org-tomee-10-1-docs-deamon-win-service)

Spring

- [Spring](#tomee-apache-org-tomee-10-1-docs-spring)
- [Spring and OpenEJB 3.0](#tomee-apache-org-tomee-10-1-docs-spring-and-openejb-3-0)
- [Spring EJB and JPA](#tomee-apache-org-tomee-10-1-docs-spring-ejb-and-jpa)

Jakarta EE 9 Work

- [Eclipse Transformer](#tomee-apache-org-tomee-10-1-docs-jakartaee-9-eclipse-transformer)
- [Jakarta EE 9 Work](#tomee-apache-org-tomee-10-1-docs-jakartaee-9-index)

Arquillian

- [Getting started with Arquillian and TomEE](#tomee-apache-org-tomee-10-1-docs-arquillian-getting-started)
- [TomEE and Arquillian](#tomee-apache-org-tomee-10-1-docs-arquillian-available-adapters)

Debugging

- [Debugging JAX-RS Services](#tomee-apache-org-tomee-10-1-docs-contrib-debug-jaxrs)

ActiveMQ

- [ActiveMQResourceAdapter Configuration](#tomee-apache-org-tomee-10-1-docs-activemqresourceadapter-config)

TCK

- [Jakarta Annotations TCK](#tomee-apache-org-tomee-10-1-docs-tck-jakarta-annotations)

Configuration

- [Apache TomEE and security](#tomee-apache-org-tomee-10-1-docs-tomee-and-security)
- [Apache TomEE configuration](#tomee-apache-org-tomee-10-1-docs-configuring-in-tomee)
- [Changing JMS Implementations](#tomee-apache-org-tomee-10-1-docs-changing-jms-implementations)
- [Clients](#tomee-apache-org-tomee-10-1-docs-clients)
- [Configuring DataSources in tomee.xml](#tomee-apache-org-tomee-10-1-docs-configuring-datasources)

- [Configuring JavaMail](#tomee-apache-org-tomee-10-1-docs-configuring-javamail)
- [Containers and Resources](#tomee-apache-org-tomee-10-1-docs-containers-and-resources)
- [Deployments](#tomee-apache-org-tomee-10-1-docs-deployments)
- [EJB over SSL](#tomee-apache-org-tomee-10-1-docs-ejb-over-ssl)
- [JavaMailSession Configuration](#tomee-apache-org-tomee-10-1-docs-javamailsession-config)

- [JMS Resources and MDB Container](#tomee-apache-org-tomee-10-1-docs-jms-resources-and-mdb-container)
- [JNDI Names](#tomee-apache-org-tomee-10-1-docs-jndi-names)
- [Log4j2 Configuration with TomEE](#tomee-apache-org-tomee-10-1-docs-admin-configuration-log4j2)
- [Security](#tomee-apache-org-tomee-10-1-docs-security)
- [System Properties](#tomee-apache-org-tomee-10-1-docs-system-properties)

EJB

- [Deploying An Application To TomEE Or OpenEJB](#tomee-apache-org-tomee-10-1-docs-application-deployment-solutions)
- [Details on openejb-jar](#tomee-apache-org-tomee-10-1-docs-details-on-openejb-jar)
- [EJB Refs](#tomee-apache-org-tomee-10-1-docs-ejb-refs)
- [EJB Request Logging](#tomee-apache-org-tomee-10-1-docs-ejb-request-logging)
- [ejb-local-ref](#tomee-apache-org-tomee-10-1-docs-ejb-local-ref)
- [ejb-ref](#tomee-apache-org-tomee-10-1-docs-ejb-ref)

- [Ejbd Transport](#tomee-apache-org-tomee-10-1-docs-ejbd-transport)
- [Generating EJB 3 annotations](#tomee-apache-org-tomee-10-1-docs-generating-ejb-3-annotations)
- [Lookup of other EJBs Example](#tomee-apache-org-tomee-10-1-docs-lookup-of-other-ejbs-example)
- [New in OpenEJB 3.0](#tomee-apache-org-tomee-10-1-docs-new-in-openejb-3-0)
- [OpenEJB 3](#tomee-apache-org-tomee-10-1-docs-openejb-3)
- [OpenEJB Binaries](#tomee-apache-org-tomee-10-1-docs-openejb-binaries)

- [OpenEJB JSR-107 Integration](#tomee-apache-org-tomee-10-1-docs-openejb-jsr-107-integration)
- [openejb.xml](#tomee-apache-org-tomee-10-1-docs-openejb-xml)
- [Running a standalone OpenEJB server](#tomee-apache-org-tomee-10-1-docs-running-a-standalone-openejb-server)
- [Singleton EJB](#tomee-apache-org-tomee-10-1-docs-singleton-ejb)
- [TomEE/OpenEJB provisioning](#tomee-apache-org-tomee-10-1-docs-provisioning)

OpenEJB Standalone Server

- [Configuration](#tomee-apache-org-tomee-10-1-docs-configuration)
- [Deploy Tool](#tomee-apache-org-tomee-10-1-docs-deploy-tool)
- [Embedded and Remotable](#tomee-apache-org-tomee-10-1-docs-embedded-and-remotable)
- [Embedding](#tomee-apache-org-tomee-10-1-docs-embedding)
- [FAQ](#tomee-apache-org-tomee-10-1-docs-faq)
- [Getting Started](#tomee-apache-org-tomee-10-1-docs-getting-started)
- [Installation](#tomee-apache-org-tomee-10-1-docs-installation)

- [Local Server](#tomee-apache-org-tomee-10-1-docs-local-server)
- [Manual Installation](#tomee-apache-org-tomee-10-1-docs-manual-installation)
- [Properties Tool](#tomee-apache-org-tomee-10-1-docs-properties-tool)
- [Property Overriding](#tomee-apache-org-tomee-10-1-docs-property-overriding)
- [Quickstart](#tomee-apache-org-tomee-10-1-docs-quickstart)
- [Remote Server](#tomee-apache-org-tomee-10-1-docs-remote-server)
- [Securing a Web Service](#tomee-apache-org-tomee-10-1-docs-securing-a-web-service)

- [Startup](#tomee-apache-org-tomee-10-1-docs-startup)
- [System Properties Files](#tomee-apache-org-tomee-10-1-docs-system-properties-files)
- [Telnet Console](#tomee-apache-org-tomee-10-1-docs-telnet-console)
- [Understanding the Directory Layout](#tomee-apache-org-tomee-10-1-docs-understanding-the-directory-layout)
- [Validation Tool](#tomee-apache-org-tomee-10-1-docs-validation-tool)

Unknown

- [Administrator](#tomee-apache-org-tomee-10-1-docs-admin-index)
- [Advanced](#tomee-apache-org-tomee-10-1-docs-advanced-index)
- [Application Composer Maven Plugin](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-applicationcomposer)
- [Application Configuration](#tomee-apache-org-tomee-10-1-docs-admin-configuration-application)
- [ApplicationComposer with JBatch](#tomee-apache-org-tomee-10-1-docs-advanced-applicationcomposer-index)
- [ApplicationComposer: The TomEE Swiss Knife](#tomee-apache-org-tomee-10-1-docs-developer-testing-applicationcomposer-index)
- [Build Tools and Plugins](#tomee-apache-org-tomee-10-1-docs-developer-tools-index)
- [Clustering and High Availability (HA)](#tomee-apache-org-tomee-10-1-docs-admin-cluster-index)
- [Container Configuration](#tomee-apache-org-tomee-10-1-docs-admin-configuration-server)
- [CXF Configuration - JAX-RS (RESTful Services) and JAX-WS (Web Services)](#tomee-apache-org-tomee-10-1-docs-developer-configuration-cxf)
- [Developer](#tomee-apache-org-tomee-10-1-docs-developer-index)

- [Directory Structure](#tomee-apache-org-tomee-10-1-docs-admin-file-layout)
- [Fat / Uber jars - Using the Shade Plugin](#tomee-apache-org-tomee-10-1-docs-advanced-shading-index)
- [How to set up TomEE in production](#tomee-apache-org-tomee-10-1-docs-advanced-setup-index)
- [Integrated Development Environments (IDEs)](#tomee-apache-org-tomee-10-1-docs-developer-ide-index)
- [Java Naming and Directory Interface (JNDI)](#tomee-apache-org-tomee-10-1-docs-advanced-client-jndi)
- [Migrate from TomEE 1 to TomEE 7](#tomee-apache-org-tomee-10-1-docs-developer-migration-tomee-1-to-7)
- [Other Testing Techniques](#tomee-apache-org-tomee-10-1-docs-developer-testing-other-index)
- [refcard](refcard/refcard.html)
- [Resources](#tomee-apache-org-tomee-10-1-docs-admin-configuration-containers)
- [Resources](#tomee-apache-org-tomee-10-1-docs-admin-configuration-resources)
- [Server Configuration](#tomee-apache-org-tomee-10-1-docs-admin-configuration-index)

- [The TomEE ClassLoader](#tomee-apache-org-tomee-10-1-docs-developer-classloading-index)
- [TomEE and Apache Johnzon - JAX-RS JSON Provider](#tomee-apache-org-tomee-10-1-docs-developer-json-index)
- [TomEE and Arquillian](#tomee-apache-org-tomee-10-1-docs-developer-testing-arquillian-index)
- [TomEE Documentation](#tomee-apache-org-tomee-10-1-docs-docs)
- [TomEE Embedded](#tomee-apache-org-tomee-10-1-docs-advanced-tomee-embedded-index)
- [TomEE Embedded Maven Plugin](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-embedded)
- [TomEE Gradle Plugin](#tomee-apache-org-tomee-10-1-docs-developer-tools-gradle-plugins)
- [TomEE MicroProfile JWT](#tomee-apache-org-tomee-10-1-docs-microprofile-jwt)
- [Unit Testing](#tomee-apache-org-tomee-10-1-docs-developer-testing-index)
- [Why is my ActiveMQ/JMS MDB not scaling as expected?](#tomee-apache-org-tomee-10-1-docs-advanced-jms-jms-configuration)

Unrevised

- [@Resource](#tomee-apache-org-tomee-10-1-docs-resource-injection)
- [App Clients and JNDI](#tomee-apache-org-tomee-10-1-docs-app-clients-and-jndi)
- [Application Resources](#tomee-apache-org-tomee-10-1-docs-application-resources)
- [Basics - Getting Things](#tomee-apache-org-tomee-10-1-docs-basics-getting-things)
- [Basics - Security](#tomee-apache-org-tomee-10-1-docs-basics-security)
- [Basics - Transactions](#tomee-apache-org-tomee-10-1-docs-basics-transactions)
- [BmpEntityContainer Configuration](#tomee-apache-org-tomee-10-1-docs-bmpentitycontainer-config)
- [build-mojo](#tomee-apache-org-tomee-10-1-docs-maven-build-mojo)
- [Built-in Type Converters](#tomee-apache-org-tomee-10-1-docs-built-in-type-converters)
- [Callbacks](#tomee-apache-org-tomee-10-1-docs-callbacks)
- [Checking Your OpenEJB Version](#tomee-apache-org-tomee-10-1-docs-version-checker)
- [Client-Server Transports](#tomee-apache-org-tomee-10-1-docs-client-server-transports)
- [CmpEntityContainer Configuration](#tomee-apache-org-tomee-10-1-docs-cmpentitycontainer-config)
- [Collapsed EAR](#tomee-apache-org-tomee-10-1-docs-collapsed-ear)
- [Common Errors](#tomee-apache-org-tomee-10-1-docs-common-errors)
- [Common PersistenceProvider properties](#tomee-apache-org-tomee-10-1-docs-common-persistenceprovider-properties)
- [Concepts](#tomee-apache-org-tomee-10-1-docs-concepts)
- [configtest-mojo](#tomee-apache-org-tomee-10-1-docs-maven-configtest-mojo)
- [Configuring Durations](#tomee-apache-org-tomee-10-1-docs-configuring-durations)
- [Constructor Injection](#tomee-apache-org-tomee-10-1-docs-constructor-injection)
- [Custom Injection](#tomee-apache-org-tomee-10-1-docs-custom-injection)
- [debug-mojo](#tomee-apache-org-tomee-10-1-docs-maven-debug-mojo)
- [Declaring References](#tomee-apache-org-tomee-10-1-docs-declaring-references)
- [deploy-mojo](#tomee-apache-org-tomee-10-1-docs-maven-deploy-mojo)

- [Deployment ID](#tomee-apache-org-tomee-10-1-docs-deployment-id)
- [Documentation](#tomee-apache-org-tomee-10-1-docs-documentation)
- [Eclipse Plugin](#tomee-apache-org-tomee-10-1-docs-eclipse-plugin)
- [exec-mojo](#tomee-apache-org-tomee-10-1-docs-maven-exec-mojo)
- [Hello World](#tomee-apache-org-tomee-10-1-docs-hello-world)
- [help-mojo](#tomee-apache-org-tomee-10-1-docs-maven-help-mojo)
- [Hibernate](#tomee-apache-org-tomee-10-1-docs-hibernate)
- [How to use JULI for TomEE in WTP?](#tomee-apache-org-tomee-10-1-docs-tomee-logging-in-eclipse)
- [InitialContext Configuration](#tomee-apache-org-tomee-10-1-docs-initialcontext-config)
- [Installing Bouncy Castle](#tomee-apache-org-tomee-10-1-docs-bouncy-castle)
- [JAAS and TomEE](#tomee-apache-org-tomee-10-1-docs-tomee-jaas)
- [JavaAgent](#tomee-apache-org-tomee-10-1-docs-javaagent)
- [JavaAgent with Maven Surefire](#tomee-apache-org-tomee-10-1-docs-javaagent-with-maven-surefire)
- [JmsConnectionFactory Configuration](#tomee-apache-org-tomee-10-1-docs-jmsconnectionfactory-config)
- [list-mojo](#tomee-apache-org-tomee-10-1-docs-maven-list-mojo)
- [ManagedContainer Configuration](#tomee-apache-org-tomee-10-1-docs-managedcontainer-config)
- [Maven](#tomee-apache-org-tomee-10-1-docs-maven)
- [MessageDrivenContainer Configuration](#tomee-apache-org-tomee-10-1-docs-messagedrivencontainer-config)
- [Multiple Business Interface Hazzards](#tomee-apache-org-tomee-10-1-docs-multiple-business-interface-hazzards)
- [ProxyFactory Configuration](#tomee-apache-org-tomee-10-1-docs-proxyfactory-config)
- [Queue Configuration](#tomee-apache-org-tomee-10-1-docs-queue-config)
- [run-mojo](#tomee-apache-org-tomee-10-1-docs-maven-run-mojo)
- [Security Annotations](#tomee-apache-org-tomee-10-1-docs-security-annotations)
- [SecurityService Configuration](#tomee-apache-org-tomee-10-1-docs-securityservice-config)

- [Service Locator](#tomee-apache-org-tomee-10-1-docs-service-locator)
- [ServicePool and Services](#tomee-apache-org-tomee-10-1-docs-services)
- [Singleton Beans](#tomee-apache-org-tomee-10-1-docs-singleton-beans)
- [SingletonContainer Configuration](#tomee-apache-org-tomee-10-1-docs-singletoncontainer-config)
- [SSH](#tomee-apache-org-tomee-10-1-docs-ssh)
- [standalone-server](#tomee-apache-org-tomee-10-1-docs-standalone-server)
- [start-mojo](#tomee-apache-org-tomee-10-1-docs-maven-start-mojo)
- [StatefulContainer Configuration](#tomee-apache-org-tomee-10-1-docs-statefulcontainer-config)
- [StatelessContainer Configuration](#tomee-apache-org-tomee-10-1-docs-statelesscontainer-config)
- [stop-mojo](#tomee-apache-org-tomee-10-1-docs-maven-stop-mojo)
- [System Properties Listing](#tomee-apache-org-tomee-10-1-docs-properties-listing)
- [Tomcat Object Factory](#tomee-apache-org-tomee-10-1-docs-tomcat-object-factory)
- [TomEE and Java 7](#tomee-apache-org-tomee-10-1-docs-java7)
- [TomEE and WebSphere MQ](#tomee-apache-org-tomee-10-1-docs-tomee-and-webspheremq)
- [TomEE Maven Plugin - Configuration](#tomee-apache-org-tomee-10-1-docs-tomee-maven-plugin)
- [tomee-logging](#tomee-apache-org-tomee-10-1-docs-tomee-logging)
- [tomee-version-policies](#tomee-apache-org-tomee-10-1-docs-tomee-version-policies)
- [Topic Configuration](#tomee-apache-org-tomee-10-1-docs-topic-config)
- [Transaction Annotations](#tomee-apache-org-tomee-10-1-docs-transaction-annotations)
- [TransactionManager Configuration](#tomee-apache-org-tomee-10-1-docs-transactionmanager-config)
- [undeploy-mojo](#tomee-apache-org-tomee-10-1-docs-maven-undeploy-mojo)
- [Understanding Callbacks](#tomee-apache-org-tomee-10-1-docs-understanding-callbacks)
- [Unix Daemon](#tomee-apache-org-tomee-10-1-docs-unix-daemon)
- [Via annotation](#tomee-apache-org-tomee-10-1-docs-resource-ref-for-datasource)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-configuring-in-tomee"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Apache TomEE configuration

## Configuring Resources:

- Drivers are dropped into tomeeDir/lib
- Resources are configured in tomeeDir/conf/tomee.xml.
- The configurations take a very simple (XML+Property-file) syntax.
- Tag names match annotation names

For example,

```java
@Resource DataSource moviesDatabase
```

is injected with the following resource:

```xml
<Resource id="moviesDatabase" type="DataSource">
JdbcDriver org.hsqldb.jdbcDriver
JdbcUrl jdbc:mysql:localhost:3306/moviesdb
UserName sa
Password secret
JtaManaged true
</Resource>
```

For more on how to configure, read through
[configuring-datasources](/configuring-datasources.html),
[containers-and-resources](#tomee-apache-org-tomee-10-1-docs-containers-and-resources) docs.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-configuring-in-tomee--)

---

<a id="tomee-apache-org-tomee-10-1-docs-tck-jakarta-annotations"></a>

# Apache TomEE

```text
<!-- https://mvnrepository.com/artifact/org.apache.geronimo.specs/geronimo-annotation_1.3_spec -->
<dependency>
    <groupId>org.apache.geronimo.specs</groupId>
    <artifactId>geronimo-annotation_1.3_spec</artifactId>
    <version>1.2</version>
    <scope>provided</scope>
</dependency>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-activemqresourceadapter-config"></a>

# Apache TomEE

```xml
<Resource id="myActiveMQResourceAdapter" type="ActiveMQResourceAdapter">
    brokerXmlConfig = broker:(tcp://localhost:61616)?useJmx=false
    dataSource = Default Unmanaged JDBC Database
    serverUrl = vm://localhost?waitForStart=20000&async=true
    startupTimeout = 10 seconds
</Resource>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-admin-cluster-index"></a>

# Apache TomEE

```properties
server      = org.apache.openejb.server.discovery.MulticastDiscoveryAgent
bind        = 239.255.2.3
port        = 6142
disabled    = true
group       = default
```

---

<a id="tomee-apache-org-tomee-10-1-docs-admin-configuration-application"></a>

# Apache TomEE

```xml
<?xml version="1.0" encoding="UTF-8"?>
<resources>
  <Resource id="MySQL" aliases="myAppDataSourceName" type="DataSource">
    JdbcDriver = com.mysql.jdbc.Driver
    JdbcUrl = jdbc:mysql://${OPENSHIFT_MYSQL_DB_HOST}:${OPENSHIFT_MYSQL_DB_PORT}/rmannibucau?tcpKeepAlive=true
    UserName = ${OPENSHIFT_MYSQL_DB_USERNAME}
    Password = ${OPENSHIFT_MYSQL_DB_PASSWORD}
    ValidationQuery = SELECT 1
    ValidationInterval = 30000
    NumTestsPerEvictionRun = 5
    TimeBetweenEvictionRuns = 30 seconds
    TestWhileIdle = true
    MaxActive = 200
  </Resource>
</resources>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-admin-configuration-containers"></a>

# Apache TomEE

```xml
<Container id="Foo" type="STATELESS">
    AccessTimeout = 30 seconds
    MaxSize = 10
    MinSize = 0
    StrictPooling = true
    MaxAge = 0 hours
    ReplaceAged = true
    ReplaceFlushed = false
    MaxAgeOffset = -1
    IdleTimeout = 0 minutes
    GarbageCollection = false
    SweepInterval = 5 minutes
    CallbackThreads = 5
    CloseTimeout = 5 minutes
    UseOneSchedulerThreadByBean = false
    EvictionThreads = 1
</Container>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-admin-configuration-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[ Download as PDF](../../../../tomee-10.1/docs/admin/configuration/index.pdf)

# Server Configuration

## Container

TomEE specific configuration (ie not inherited one from Tomcat) is based on properties. Therefore,
you can fully configure TomEE using properties in `conf/system.properties`.
However, for convenience it also provides a hybrid XML alternative a.k.a. `conf/tomee.xml`.

- [Server Configuration: Properties](#tomee-apache-org-tomee-10-1-docs-admin-configuration-server).
- [Resources](#tomee-apache-org-tomee-10-1-docs-admin-configuration-resources)
- [Containers](#tomee-apache-org-tomee-10-1-docs-admin-configuration-containers)
- [Using log4j2](#tomee-apache-org-tomee-10-1-docs-admin-configuration-log4j2)

## Application

Some settings can be specific to applications, these are also properties based and
are read in `WEB-INF/application.properties`. When you can’t use `tomee.xml` to configure
resources you can use `WEB-INF/resources.xml` which inherit from `tomee.xml` its syntax
but binds the resources to the application and reuses the application classloader.

More about [Container Configuration](#tomee-apache-org-tomee-10-1-docs-admin-configuration-application).

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../privacy-policy.html)

##### [Documentation](../../../../latest/docs/)

- [How to configure](../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../latest/examples/)

- [CDI Interceptor](../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../community/index.html)

- [Contributors](../../../../community/contributors.html)
- [Social](../../../../community/social.html)
- [Sources](../../../../community/sources.html)

##### [Security](../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-admin-configuration-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-admin-configuration-log4j2"></a>

# Apache TomEE

```properties
JAVA_OPTS="$JAVA_OPTS -Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager"
LOGGING_CONFIG="-DnoOp"
LOGGING_MANAGER="-Djava.util.logging.manager=org.apache.logging.log4j.jul.LogManager"
CLASSPATH=".:$CATALINA_BASE/bin:$CATALINA_BASE/bin/log4j-core-2.17.1.jar:$CATALINA_BASE/bin/log4j-api-2.17.1.jar:$CATALINA_BASE/bin/log4j-jul-2.17.1.jar"
```

---

<a id="tomee-apache-org-tomee-10-1-docs-admin-configuration-resources"></a>

# Apache TomEE

```properties
myDataSource = new://Resource?type=DataSource
myDataSource.JdbcUrl = jdbc:hsqldb:mem:site
myDataSource.UserName = sa
```

---

<a id="tomee-apache-org-tomee-10-1-docs-admin-configuration-server"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[ Download as PDF](../../../../tomee-10.1/docs/admin/configuration/server.pdf)

# Container Configuration

## Server

| Name | Value | Description |
| --- | --- | --- |
| openejb.embedded.remotable | bool | activate or not the remote services when available |
| .bind, <service prefix>.port, <service prefix>.disabled, <service prefix>.threads | host or IP, port, bool | override the host. Available for ejbd and httpejbd services (used by jaxws and jaxrs), number of thread to manage requests |
| openejb.embedded.initialcontext.close | LOGOUT or DESTROY | configure the hook called when closing the initial context. Useful when starting OpenEJB from a new InitialContext([properties]) instantiation. By default, it simply logs out the logged user if it exists. DESTROY means clean the container. |
| jakarta.persistence.provider | string | override the JPA provider value |
| jakarta.persistence.transactionType | string | override the transaction type for persistence contexts |
| jakarta.persistence.jtaDataSource | string | override the JTA datasource value for persistence contexts |
| jakarta.persistence.nonJtaDataSource | string | override the non JTA datasource value for persistence contexts |
| openejb.descriptors.output | bool | dump memory deployment descriptors. Can be used to set complete metadata to true and avoid scanning when starting the container or to check the used configuration. |
| openejb.deployments.classpath.require.descriptor | CLIENT or EJB | can allow to filter what you want to scan (client modules or ejb modules) |
| openejb.descriptors.output.folder | path | where to dump deployment descriptors if activated. |
| openejb.strict.interface.declaration | bool | add some validations on session beans (spec validations in particular). false by default. |
| openejb.conf.file or openejb.configuration | string | OpenEJB configuration file path |
| openejb.debuggable-vm-hackery | bool | remove JMS information from deployment |
| openejb.validation.skip | bool | skip the validations done when OpenEJB deploys beans |
| openejb.deployments.classpath.ear | bool | deploy the classpath as an ear |
| openejb.webservices.enabled | bool | activate or not webservices |
| openejb.validation.output.level | TERSE or MEDIUM or VERBOSE | level of the logs used to report validation errors |
| openejb.user.mbeans.list	* or a list of classes separated by , | list of mbeans to deploy automatically | openejb.deploymentId.format	composition (+string) of {ejbName} {ejbType} {ejbClass} and {ejbClass.simpleName}	default {ejbName}. The format to use to deploy ejbs. |
| openejb.deployments.classpath | bool | whether or not deploy from classpath |
| openejb.deployments.classpath.include and openejb.deployments.classpath.exclude | regex | regex to filter the scanned classpath (when you are in this case) |
| openejb.deployments.package.include and openejb.deployments.package.exclude | regex | regex to filter scanned packages |
| openejb.autocreate.jta-datasource-from-non-jta-one | bool | whether or not auto create the jta datasource if it doesn’t exist but a non jta datasource exists. Useful when using hibernate to be able to get a real non jta datasource. |
| openejb.altdd.prefix | string | prefix use for altDD (example test to use a test.ejb-jar.xml). |
| org.apache.openejb.default.system.interceptors | class names | list of interceptor (qualified names) separated by a comma or a space	add these interceptor on all beans |
| openejb.jndiname.strategy.class | class name | an implementation of org.apache.openejb.assembler.classic.JndiBuilder.JndiNameStrategy |
| openejb.jndiname.failoncollision | bool | if a NameAlreadyBoundException is thrown or not when 2 EJBs have the same name |
| openejb.jndiname.format | string | composition of these properties: ejbType, ejbClass, ejbClass.simpleName, ejbClass.packageName, ejbName, deploymentId, interfaceType, interfaceType.annotationName, interfaceType.annotationNameLC, interfaceType.xmlName, interfaceType.xmlNameCc, interfaceType.openejbLegacyName, interfaceClass, interfaceClass.simpleName, interfaceClass.packageName	default {deploymentId}{interfaceType.annotationName}. Change the name used for the ejb. |
| openejb.org.quartz.threadPool.class | class | qualified name which implements org.quartz.spi.ThreadPool	the thread pool used by quartz (used to manage ejb timers) |
| openejb.localcopy | bool | default true. whether or not copy EJB arguments[/method/interface] for remote invocations. |
| openejb.cxf.jax-rs.providers | string | the list of the qualified name of the JAX-RS providers separated by comma or space. Note: to specify a provider for a specific service suffix its class qualified name by ".providers", the value follow the same rules. Note 2: default is a shortcut for jaxb and json providers. |
| openejb.wsAddress.format | string | composition of {ejbJarId}, ejbDeploymentId, ejbType, ejbClass, ejbClass.simpleName, ejbName, portComponentName, wsdlPort, wsdlService	default /{ejbDeploymentId}. The WS name format. |
| org.apache.openejb.server.webservices.saaj.provider | axis2, sun or null | specified the saaj configuration |
| [<uppercase service name>.]<service id>.<name> or [<uppercase service name>.]<service id> | whatever is supported (generally string, int …​) | set this value to the corresponding service. example: [EnterpriseBean.]<ejb-name>.activation.<property>, [PERSISTENCEUNIT.]<persistence unit name>.<property>, [RESOURCE.]<name> |
| log4j.category.OpenEJB.options | DEBUG, INFO, …​ | active one OpenEJB log level. need log4j in the classpath |
| openejb.jmx.active | bool | activate (by default) or not the OpenEJB JMX MBeans |
| openejb.nobanner | bool | activate or not the OpenEJB banner (activated by default) |
| openejb.check.classloader | bool | if true print some information about duplicated classes |
| openejb.check.classloader.verbose | bool | if true print classes intersections |
| openejb.additional.exclude | string separated by comma | list of prefixes you want to exclude and are not in the default list of exclusion |
| openejb.additional.include | string separated by comma | list of prefixes you want to remove from the default list of exclusion |
| openejb.offline | bool | if true can create datasources and containers automatically |
| openejb.exclude-include.order | include-exclude or exclude-include | if the inclusion/exclusion should win on conflicts (intersection) |
| openejb.log.color | bool | activate or not the color in the console in embedded mode |
| openejb.log.color.<level in lowercase> | color in uppercase | set a color for a particular level. Color are BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE, DEFAULT. |
| tomee.serialization.class.blacklist | string | default list of packages/classnames excluded for EJBd deserialization (needs to be set on server and client sides). Please see the description of Ejbd Transport for details. |
| tomee.serialization.class.whitelist | string | default list of packages/classnames allowed for EJBd deserialization (blacklist wins over whitelist, needs to be set on server and client sides). Please see the description of Ejbd Transport for details. |
| tomee.remote.support | boolean | if true /tomee webapp is auto-deployed and EJBd is active (true by default for 1.x, false for 7.x excepted for tomee maven plugin and arquillian) |
| openejb.crosscontext | bool | set the cross context property on tomcat context (can be done in the traditional way if the deployment is done through the webapp discovery and not the OpenEJB Deployer EJB) |
| openejb.jsessionid-support | bool | remove URL from session tracking modes for this context (see jakarta.servlet.SessionTrackingMode) |
| openejb.myfaces.disable-default-values | bool | by default TomEE will initialize myfaces with some its default values to avoid useless logging |
| openejb.web.xml.major | int | major version of web.xml. Can be useful to force tomcat to scan servlet 3 annotation when deploying with a servlet 2.x web.xml |
| tomee.jaxws.subcontext | string | sub context used to bind jaxws web services, default is webservices |
| openejb.servicemanager.enabled | bool | run all services detected or only known available services (WS and RS |
| tomee.jaxws.oldsubcontext | bool | whether or not activate old way to bind jaxws webservices directly on root context |
| openejb.modulename.useHash | bool | add a hash after the module name of the webmodule if it is generated from the webmodule location, it avoids conflicts between multiple deployment (through ear) of the same webapp. Note: it disactivated by default since names are less nice this way. |
| openejb.session.manager | qualified name (string) | configure a session manager to use for all contexts |
| tomee.tomcat.resource.wrap | bool | wrap tomcat resources (context.xml) as tomee resources if possible (true by default) |
| tomee.tomcat.datasource.wrap | bool | same as tomee.tomcat.resource.wrap for datasource (false by default). Note that setting it to true will create tomee datasources but can have the side effect to create twice singleton resources |
| openejb.environment.default | bool | should default JMS resources be created or not, defaults to false to ensure no port is bound or multiple resources are created and completely uncontrolled (doesn’t apply to datasources etc. for compatibility). For tests only! |

## Client

| Name | Value | Description |
| --- | --- | --- |
| openejb.client.identityResolver | implementation of org.apache.openejb.client.IdentityResolver | default org.apache.openejb.client.JaasIdentityResolver. The class to get the client identity. |
| openejb.client.connection.pool.timeout or openejb.client.connectionpool.timeout | int (ms) | the timeout of the client |
| openejb.client.connection.pool.size or openejb.client.connectionpool.size | int | size of the socket pool |
| openejb.client.keepalive | int (ms) | the keepalive duration |
| openejb.client.protocol.version | string | Optional legacy server protocol compatibility level. Allows 4.6.x clients to potentially communicate with older servers. OpenEJB 4.5.2 and older use version "3.1", and 4.6.x currently uses version "4.6" (Default). This does not allow old clients to communicate with new servers prior to 4.6.0 |
| tomee.serialization.class.blacklist | string | default list of packages/classnames excluded for EJBd deserialization (needs to be set on server and client sides). Please see the description of Ejbd Transport for details. |
| tomee.serialization.class.whitelist | string | default list of packages/classnames allowed for EJBd deserialization (blacklist wins over whitelist, needs to be set on server and client sides). Please see the description of Ejbd Transport for details. |

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../privacy-policy.html)

##### [Documentation](../../../../latest/docs/)

- [How to configure](../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../latest/examples/)

- [CDI Interceptor](../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../community/index.html)

- [Contributors](../../../../community/contributors.html)
- [Social](../../../../community/social.html)
- [Sources](../../../../community/sources.html)

##### [Security](../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-admin-configuration-server--)

---

<a id="tomee-apache-org-tomee-10-1-docs-admin-file-layout"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[ Download as PDF](../../../tomee-10.1/docs/admin/file-layout.pdf)

# Directory Structure

- apps

  - module1.jar
  - myapp
  - anotherapp.war
  - anotherapp
  - anotherapp2.ear
  - anotherapp2
- bin

  - bootstrap.jar
  - catalina.bat
  - catalina.bat.original
  - catalina.sh
  - catalina.sh.original
  - catalina-tasks.xml
  - commons-daemon.jar
  - commons-daemon-native.tar.gz
  - configtest.bat
  - configtest.sh
  - daemon.sh
  - digest.bat
  - digest.sh
  - service.bat
  - service.install.as.admin.bat
  - service.readme.txt
  - service.remove.as.admin.bat
  - setclasspath.bat
  - setclasspath.sh
  - setenv.sh
  - setenv.bat
  - shutdown.bat
  - shutdown.sh
  - startup.bat
  - startup.sh
  - tomcat-juli.jar
  - tomcat-native.tar.gz
  - TomEE…​.exe
  - tomee.bat
  - tomee.sh
  - tool-wrapper.bat
  - tool-wrapper.sh
  - version.bat
  - version.sh
- conf

  - Catalina
  - catalina.policy
  - catalina.properties
  - conf.d
  - context.xml
  - logging.properties
  - server.xml
  - server.xml.original
  - system.properties
  - tomcat-users.xml
  - tomcat-users.xml.original
  - tomcat-users.xsd
  - tomee.xml
  - web.xml
- lib

  - \*.jar
- logs

  - catalina.$day.log
  - xxx.2016-03-16.log
  - localhost.$day.log
  - localhost\_access\_log.$day.txt
- temp

  - OpenEJB-dejlzdbhjzbfrzeofrh
- webapps

  - myapp
  - anotherapp.war
  - anotherapp
- work

  - Catalina

    - localhost

      - myapp

        - org.apache.jsp.index\_jsp.java
        - org.apache.jsp.index\_jsp.class

Click on a tree node or open a folder to see the detail there.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-admin-file-layout--)

---

<a id="tomee-apache-org-tomee-10-1-docs-admin-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[ Download as PDF](../../../tomee-10.1/docs/admin/index.pdf)

# Administrator

Click [here](#tomee-apache-org-tomee-10-1-docs-docs) to find the documentation for administrators.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-admin-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-advanced-applicationcomposer-index"></a>

# Apache TomEE

```java
// helper class reusable for any batch
abstract class BatchApplication {
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("YYYYMMddHHmmss");

    protected Report runBatch(final String batchName, final Properties config) {
        final JobOperator operator = BatchRuntime.getJobOperator();
        final long id = operator.start(batchName, config);
        Batches.waitForEnd(operator, id);
        return new Report(operator.getJobExecution(id), operator.getParameters(id));
    }

    @Module // we enforce BatchEE to be initialized as an EJB context to get JNDI for JTA init, needed for TomEE 1
    public EjbModule ensureBatchEESetupIsDoneInTheRightContext() {
        final EjbJar ejbJar = new EjbJar().enterpriseBean(new SingletonBean(BatchEEBeanManagerInitializer.class));

        final Beans beans = new Beans();
        beans.addManagedClass(BatchEEBeanManagerInitializer.Init.class);

        final EjbModule ejbModule = new EjbModule(ejbJar);
        ejbModule.setModuleId("batchee-shared-components");
        ejbModule.setBeans(beans);
        return ejbModule;
    }

    public static class Report {
        private final JobExecution execution;
        private final Properties properties;

        public Report(final JobExecution execution, final Properties properties) {
            this.execution = execution;
            this.properties = properties;
        }

        public JobExecution getExecution() {
            return execution;
        }

        public Properties getProperties() {
            return properties;
        }
    }
}

@Classes(cdi = true, value = { MyFilter.class, MoveFile.class, InputFile.class, MyReader.class, LoggingListener.class })
public class MyBatch extends BatchApplication {
    private final Properties config;

    public Mybatch(final String[] args) { // main args
        this.config = new Properties() {{ // create the batch config
            setProperty("input-directory", args[0]);
        }};
    }

    public Report execute(final String inputDirectory) {
        return runBatch("sunstone", config);
    }

    public static void main(final String[] args) throws Exception {
        ApplicationComposers.run(MyBatch.class, args);
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-advanced-client-jndi"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

# Java Naming and Directory Interface (JNDI)

TomEE has several JNDI client intended for multiple usages.

## Default one

In a standalone instance you generally don’t need (or want) to specify anything
to do a lookup. Doing so you will inherit from the contextual environment:

```java
final Context ctx = new InitialContext();
ctx.lookup("java:....");
```

## LocalInitialContextFactory

This is the legacy context factory used by OpenEJB. It is still useful to fallback
on the "default" one in embedded mode where sometimes classloaders or libraries can mess
up the automatic contextual context.

Usage:

```java
Properties properties = new Properties();
properties.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.core.LocalInitialContextFactory");
final Context ctx = new InitialContext(properties);
ctx.lookup("java:....");
```

This context factory supports few more options when **you boot the container** creating a context:

| Name | Description |
| --- | --- |
| openejb.embedded.remotable | true/false: starts embedded services |
| Context.SECURITY_PRINCIPAL/Context.SECURITY_CREDENTIALS | theglobalsecurity identity for the whole container |

|  | Context.SECURITY_*shouldn’t be used for runtime lookups withLocalInitialContextFactory, it would leak a security identity and make the runtime no more thread safe. This factory was deprecated starting with 7.0.2 in favor oforg.apache.openejb.core.OpenEJBInitialContextFactory. |
| --- | --- |

## OpenEJBInitialContextFactory

This factory allows you to access local EJB and container resources.

```java
Properties properties = new Properties();
properties.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.core.OpenEJBInitialContextFactory");
final Context ctx = new InitialContext(properties);
ctx.lookup("java:....");
```

## RemoteInitialContextFactory

Intended to be used to contact a remote server, the `org.apache.openejb.client.RemoteInitialContextFactory` relies on the provider url
to contact a tomee instance:

```java
Properties p = new Properties();
p.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.client.RemoteInitialContextFactory");
p.put(Context.PROVIDER_URL, "failover:ejbd://192.168.1.20:4201,ejbd://192.168.1.30:4201");

final InitialContext remoteContext = new InitialContext(p);
ctx.lookup("java:....");
```

Contrarily to local one, the remote factory supports `Context.SECURITY_*` options in a thread safe manner, and you can do lookups at runtime using them.

See [Cluster](#tomee-apache-org-tomee-10-1-docs-admin-cluster-index) page for more details on the options.

### Security

The context configuration can take additional configuration to handle EJB security:

```properties
p.put("openejb.authentication.realmName", "my-realm"); // optional
p.put(Context.SECURITY_PRINCIPAL, "alfred");
p.put(Context.SECURITY_CREDENTIALS, "bat");
```

The realm will be used by JAAS to get the right LoginModules and principal/credentials to
do the actual authentication.

#### HTTP case

Often HTTP layer is secured and in this case you need to authenticate before the EJBd (remote EJB TomEE protocol) layer.
Thanks to TomEE/Tomcat integration login there will propagate to the EJBd context.

This can be done passing the token you need to set as `Authorization` header in the `PROVIDER_URL`:

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../privacy-policy.html)

##### [Documentation](../../../../latest/docs/)

- [How to configure](../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../latest/examples/)

- [CDI Interceptor](../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../community/index.html)

- [Contributors](../../../../community/contributors.html)
- [Social](../../../../community/social.html)
- [Sources](../../../../community/sources.html)

##### [Security](../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-advanced-client-jndi--)

---

<a id="tomee-apache-org-tomee-10-1-docs-advanced-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[ Download as PDF](../../../tomee-10.1/docs/advanced/index.pdf)

# Advanced

Click [here](#tomee-apache-org-tomee-10-1-docs-docs) to find advanced documentation.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-advanced-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-advanced-jms-jms-configuration"></a>

# Apache TomEE

```xml
<Resource id="my resource adapter" ....>
  # using -1 will make the server using cached threads (unbounded)
  # min recommanded: maxSessions + 1 (for connect())
  threadPoolSize = 30
</Resource>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-advanced-setup-index"></a>

# Apache TomEE

```bash
#! /bin/sh

# which java
export JAVA_HOME="/some/path/java/jdk-8u60"
# which tomee
export CATALINA_HOME="/some/path/tomee/tomee-7.0.0-M3"
# where is the application - to let tomcat/tomee finds the configuration
export CATALINA_BASE="/some/path/application1/"
# to let tomee be able to kill the instance if shutdown doesn't work (see shutdown script)
export CATALINA_PID="/some/path/application1/work/tomee.pid"
```

---

<a id="tomee-apache-org-tomee-10-1-docs-advanced-shading-index"></a>

# Apache TomEE

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-shade-plugin</artifactId>
  <version>2.3</version>
  <executions>
    <execution>
      <phase>package</phase>
      <goals>
        <goal>shade</goal>
      </goals>
      <configuration>
        <dependencyReducedPomLocation>${project.build.directory}/reduced-pom.xml</dependencyReducedPomLocation>
        <transformers>
          <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
            <mainClass>org.apache.tomee.embedded.FatApp</mainClass>
          </transformer>
          <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
            <resource>META-INF/cxf/bus-extensions.txt</resource>
          </transformer>
          <transformer implementation="org.apache.openwebbeans.maven.shade.OpenWebBeansPropertiesTransformer" />
        </transformers>
        <filters>
          <filter> <!-- we don't want JSF to be activated -->
            <artifact>*:*</artifact>
            <excludes>
              <exclude>META-INF/faces-config.xml</exclude>
            </excludes>
          </filter>
        </filters>
      </configuration>
    </execution>
  </executions>
  <dependencies>
    <dependency>
      <groupId>org.apache.openwebbeans</groupId>
      <artifactId>openwebbeans-maven</artifactId>
      <version>1.7.0/version>
    </dependency>
  </dependencies>
</plugin>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-advanced-tomee-embedded-index"></a>

# Apache TomEE

```java
try (final Container container = new Container(new Configuration()).deployClasspathAsWebApp()) {
    System.out.println("Started on http://localhost:" + container.getConfiguration().getHttpPort());

    // do something or wait until the end of the application
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-alternate-descriptors"></a>

# Apache TomEE

```java
 Properties properties = new Properties();
 properties.setProperty(Context.INITIAL_CONTEXT_FACTORY,
      "org.apache.openejb.client.LocalInitialContextFactory");
 properties.setProperty("openejb.altdd.prefix", "test");

 InitialContext initialContext = new InitialContext(properties);
```

---

<a id="tomee-apache-org-tomee-10-1-docs-app-clients-and-jndi"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# App Clients and JNDI

There are some slight differences between the way OpenEJB
does app clients and the way Geronimo does app clients

Neither uses the names created via the openejb.jndiname.format. So
changing that will (should) have no affect. The idea is that users
should be able to set it to be whatever they want it to be and that
should not break the App Client code. The openejb.jndiname.format is
specifically for "plain" clients and allows them to get the names as
they want them.

Internally, we bind each EJB proxy under essentially a hardcoded and
predictable format and then again using the user supplied format. So
there are at minimum two JNDI trees with every EJB proxy. It used to be
two at least. Now we have quite a few because of Java EE 6 global JNDI
and the support we added for `@LocalClient` and allowing the same
interface to be used as both `@Local` and `@Remote`.

Basically we have:

- openejb/Deployment/<hardcoded internal format>
- openejb/local/<strategy format>
- openejb/remote/<strategy format>

The 'openejb/Deployment' section is the non-changing fully qualified
name for use internally and by app clients.

The 'openejb/remote' section is for "pretty" names looked up via plain
clients using the RemoteInitialContextFactory. The protocol can tell
the difference between app clients and plain clients and knows which
area to look in.

The 'openejb/local' section is for "pretty" names looked up via the
LocalInitialContextFactory.

The "pretty" names are defined by the openejb.jndiname.format and since
the user has control of that formatting it’s possible that not all
proxies can be bound. Say the bean has both a local and remote
interface and the user has just "{deploymentId}" or "{ejbName}" as the
format. Hence, those bind calls use the "optional" set of binding
methods.

The format of the internal names bound into openejb/Deployment is
guaranteed to be unique. It’s not pretty to look at obviously, but
every possible proxy will be bound there guaranteed. For binding into
'openejb/Deployment' we don’t use the "optional" set of binding methods.
If something can’t be bound it’s a deployment issue.

The home interface is bound, but with the name of the corresponding
business interface rather than the home interface.

To be a little bit more clear - Both OpenEJB and Geronimo build their
own JNDI trees for the App Client. Geronimo prefers to have its own
JNDI tree for the App Client as there are other things in it that are
not EJB related. Either way the OpenEJB EJBd protocol can carry the
"id" of the App Client and both Geronimo and OpenEJB rely on that.

In Geronimo App Clients the id is set to "Deployments" and that tells
OpenEJB not to look in the "openejb/remote" section of JNDI as it
normally would. It will instead use the "openejb/Deployments" section
of JNDI were the names follow a predictable and unchanging format.

In OpenEJB App Clients the id is set to the name of the App Client, and
we instead look in "openejb/client//" where names are formatted by the
user via the application-client.xml.

When calls are made from client to server and the App Client module id
is not present, we look in openejb/remote/ where names are formatted
using the openejb.jndi.format

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-app-clients-and-jndi--)

---

<a id="tomee-apache-org-tomee-10-1-docs-application-composer-advanced"></a>

# Apache TomEE

```java
// runner if needed etc...
@Descriptors(@Descriptor(name = "persistence.xml", path = "META-INF/persistence.xml"))
public class MyTest {
   //...
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-application-composer-getting-started"></a>

# Apache TomEE

```xml
<dependency>
  <groupId>org.apache.openejb</groupId>
  <artifactId>openejb-core</artifactId>
  <version>${openejb.version></version>
</dependency>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-application-composer-history"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# Application Composer History

ApplicationComposer can look like a long story but following it, you’ll
realize it is finally quite natural.

## Internal tool

TomEE (former OpenEJB) is an Application Server. One of the most
important task writing an Application Server is to ensure the
implemented features do what is expected. However, it is hard to write N
test applications, in N modules to ensure it works smoothly. In
particular when you want to test a small part of the whole server.

So you immediately think to mocking what is not needed. It works but has
a big pitfall: test is often a noop or hide a lot of issues.

So the idea came to be able to shortcut the part we don’t care much
about runtime: the application packaging.

Here what is the ApplicationComposer: an (originally test) API to create
an EE application programmatically.

## Designs

ApplicationComposer design was aligned on this simple need. An
ApplicationComposer "test" (browsing other pages you’ll see it is much
more than test today) is composed of mainly 2 parts:

- modules: methods describing a module of an application. It can be a
  persistence.xml, an `ejb-jar.xml`, a `web.xml`…​but all programmatically.
- configuration: container configuration allowing to interact with
  container (creating resources for instance)

## Test but not only

ApplicationComposer was originally a JUnit only runner but was pretty
quickly extended to TestNG too, and today you can even use it to write
`main(String[])` - even in a shade!

API was greatly simplified, and it allows you pretty easily to deploy
with a simple shade a JAXRS/JAXWS/JMS service!

## Going further

If you want to go further you can browse:

- [Getting Started](#tomee-apache-org-tomee-10-1-docs-application-composer-getting-started)
- [Advanced](#tomee-apache-org-tomee-10-1-docs-application-composer-advanced)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-application-composer-history--)

---

<a id="tomee-apache-org-tomee-10-1-docs-application-composer-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# Application Composer

Here is the subdomain dedicated to the Application Composer.

If you don’t know at all what ApplicationComposer means,
[History](#tomee-apache-org-tomee-10-1-docs-application-composer-history) page will explain to you where does it come from
and what it can be used to today.

If you are already familiar with ApplicationComposer concept and are
just looking for a sample, [Getting Started](#tomee-apache-org-tomee-10-1-docs-application-composer-getting-started) is
designed for you.

Finally, if you already use ApplicationComposer and just desire to go
further, [Advanced](#tomee-apache-org-tomee-10-1-docs-application-composer-advanced) page is the one you need to look!

Children:

- [History](#tomee-apache-org-tomee-10-1-docs-application-composer-history)
- [Getting Started](#tomee-apache-org-tomee-10-1-docs-application-composer-getting-started)
- [Advanced](#tomee-apache-org-tomee-10-1-docs-application-composer-advanced)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-application-composer-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-application-deployment-solutions"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Deploying An Application To TomEE Or OpenEJB

## Deploying An Application To TomEE Or OpenEJB

### How to deploy my application under TomEE

#### Description

This aims to be more dynamic in the way you deploy your applications. It
is clearly cloud oriented.

#### Webapp and TomEE deployment

Webapp can be deployed as Tomcat does. Simply put it in webapps folder
(or the one you configured) and start TomEE.

#### TomEE specific deployment

By default, TomEE deploys applications (ear, war, jar) contained in
$CATALINA\_BASE/apps directory at start up.

#### Deployer

OpenEJB provides a Deployer EJB to do this task. It can be used in your
own software looking up remotely the "openejb/DeployerBusinessRemote"
EJB. Its interface is "org.apache.openejb.assembler.Deployer". The
needed dependency is org.apache.openejb:openejb-core.

Once you got your deployer simply invoke the "deploy" method. Give it
the location of your application (can be a file, http, https, maven
location depending on the way you configured your container, for more
information have a look to TomEE provisionning).

Note: the "undeploy" method exists too and take the same path.

The Deployer is the base of all other solutions

#### Maven plugin

[org.apache.openejb:tomee-maven-plugin](maven/index.html) can be used
to deploy/undeploy your application. Once this plugin is added to your
pom you have access to the following configuration:

- tomeeHttpPort
- tomeeHost

Then simply run

```bash
mvn tomee:deploy <path>
```

or

```bash
mvn tomee:undeploy <path>
```

##### The Deployer through TomEE Webapp

When you start TomEE you can locally access the TomEE webapps
([http://host:ip/tomee/](http://host:ip/tomee/)).

Then simply go to JNDI tree, select the deployer in the tree, then click
on "invoke this ejb", select the deploy (or undeploy) method, fill the
path and click on "invoke".

##### Cloud idea

If you want to cloudify your application, you’ll get a configuration
database (or any other storage system ;)).

So it means it is easy for you to get a host and a port…​so it is easy
to deploy on all your server using the deployer: simply use the maven
provisioning then run the deployer on all your nodes and that’s all!

#### Doing it with camel?

If you are using a route to deploy/undeploy your applications you can
have a look to the proposed camel-openejb component:

- base code:
  [http://svn.apache.org/repos/asf/tomee/sandbox/camel/camel-openejb/](http://svn.apache.org/repos/asf/tomee/sandbox/camel/camel-openejb/)
- proposed to be added to camel:
  [https://issues.apache.org/jira/browse/CAMEL-4935](https://issues.apache.org/jira/browse/CAMEL-4935)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-application-deployment-solutions--)

---

<a id="tomee-apache-org-tomee-10-1-docs-application-discovery-via-the-classpath"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Application discovery via the classpath

This document
details the various ways to get OpenEJB to detect applications you would
like deployed while in an embedded mode.

# Empty ejb-jar.xml approach (recommended)

Simplify the issue of searching for annotated applications by adding an
`ejb-jar.xml` like this to your app:

```xml
<ejb-jar/>
```

OpenEJB will find the app in the classpath and deploy it along with any
annotated beans it may contain.

The `ejb-jar.xml` can contain more than just "" as usual.

This is the recommended approach for people using OpenEJB for unit
testing as it allows OpenEJB to find your application in the classpath
without the need for you to specify any path information which tends to
complicate builds.

## Including/Excluding paths (advanced)

If you do not like the idea of having the `ejb-jar.xml` in your app or an
openejb.xml, we can search the classpath for annotated beans
(`@Stateless`, `@Stateful`, `@MessageDriven`) and load them automatically just
as if they contained an `ejb-jar.xml`.

This form of searching, however, is very expensive as it involves
iterating over every path in the classpath and reading in each class
definition contained thereunder and checking it for annotations.

This approach can only be made faster by helping us trim down or
pinpoint the paths we should search via the
*openejb.deployments.classpath.include* property which can be specified
as a *system property* or a property passed into the *InitialContext*.

The value of this property is a regular expression and therefore can be
absolute or relative. For example the path
"/Users/dblevins/work/swizzle/swizzle-stream/target/classes" which
contains the class files of an application you wish to test could be
included in any of the following values to the
"openejb.deployments.classpath.include" property:

- "file:///Users/dblevins/work/swizzle/swizzle-stream/target/classes/"
  *(an absolute path)*
- "file:///Users/dblevins/work/swizzle/.\*" *(relative)*
- ".**swizzle-stream.**" *(very relative)*
- ".**(swizzle-stream|swizzle-jira|acme-rocket-app).**" *(including
  several paths)*
- ".**(swizzle-stream|swizzle-jira|acme-rocket-app).**" *(including
  several paths with Win specific escapes)*

Note the filtering is done on URLs in the classpath, so forward slashes
should always be used even on OSs using backslash ("").

There are also the *openejb.deployments.classpath.exclude* and
*openejb.exclude-include.order* properties if you wish to work in the
opposite direction or change the processing order. The default values
for the properties are as follows:

```properties
  openejb.exclude-include.order=include-exclude //Defines the processing order
   openejb.deployments.classpath.include=""      //Include nothing
   openejb.deployments.classpath.exclude=".*"    //Exclude everything
```

The exclude and the include are applied separately and the results of
each are combined to create the list of paths OpenEJB will
scrape for annotations.

```java
*Note:* by default these settings will only affect which jars OpenEJB will
 scan for annotated components when no descriptor is found.  If you would
 like to use these settings to also filter out jars that do contain
 descriptors, set the *openejb.deployments.classpath.filter.descriptors*
 property to _true_.  The default is _false_.
```

## Troubleshooting

If the include/exclude is not being processed as you expect first try
reversing the order to *openejb.exclude-include.order*=exclude-include
There are a number of internal filters that may result in an unexpected
exclusion.

If you’re having trouble determining if the `META-INF/ejb-jar.xml` file
for your ejb module is in the classpath, a little debug code like this
in your test setup will help you see what OpenEJB sees (which may be
nothing):

```properties
Enumeration<URL> ejbJars =
this.getClass().getClassLoader().getResources("META-INF/ejb-jar.xml");
while (ejbJars.hasMoreElements()) {
    URL url = ejbJars.nextElement();
    System.out.println("app = " + url);
}
```

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-application-discovery-via-the-classpath--)

---

<a id="tomee-apache-org-tomee-10-1-docs-application-resources"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Application Resources

## Resources

TomEE provides a simple but powerful way to define resources that can be
injected into managed components inside your application, or looked up
via JNDI. To use a resource, it needs to be defined in the `tomee.xml`
configuration file, a `resources.xml` file within an application, or as
a system property. Defining a resource in `tomee.xml` will make it
available server-wide, whereas defining the resource within a
`resources.xml` file makes it available to a specific application.

As a simple example, a JMS queue can be defined within `tomee.xml` with
the following configuration.

```xml
<tomee>
    <Resource id="MyQueue" type="jakarta.jms.Queue"/>
</tomee>
```

Once the resource has been defined, the server will create an instance
of the resource during startup, and it will be available to be injected
into managed components using the `@Resource` annotation, as shown
below. The `name` attribute on the `@Resource` annotation should match
the `id` attribute on the `Resource` tag.

```java
public class JmsClient {

    @Resource(name="MyQueue")
    private Queue queue;

    public void sendMessage() {
        // implementation here...
    }

}
```

As an alternative to defining a resource in XML, resources can also be
defined using system properties:

```properties
MyQueue = new://Resource?type=jakarta.jms.Queue
```

Resources, or attributes for resources specified using system properties
will override definitions specified in `tomee.xml`. Server-wide
resources can be looked up in JNDI under the following name:
openejb:Resources/resource id.

## Defining Resources

The `<Resource>` tag has a number of attributes, and a resource may also
have a number of fields that can be configured by adding properties to
the body of the `Resource` tag.

For example, a DataSource resource needs a JDBC driver, URL, username
and password to be able to connect to a database. That would be
configured with the following syntax. Notice the key/value pair syntax
for the properties within the `<Resource>` tag.

```xml
<Resource id="DB" type="DataSource">
  JdbcDriver  com.mysql.jdbc.Driver
  JdbcUrl     jdbc:mysql://localhost/test
  UserName    test
  Password    password
</Resource>
```

Specifying the key/value pairs specific to a Resource can also be done
when defining the resource via system properties. This is done be
specifying an additional property for each key/value pair, using the
resource ID as a prefix: `<resourceId>.<propertyName>=<value>`. The
system properties equivalent of the resource above is:

```java
p.setProperty("DB", "new://Resource?type=DataSource");
p.setProperty("DB.JdbcDriver", "com.mysql.jdbc.Driver");
p.setProperty("DB,JdbcUrl", "jdbc:mysql://localhost/test");
p.setProperty("DB.UserName", "test");
p.setProperty("DB.Password", "password");
```

The `<Resource>` tag has a number of attributes which control the way
that the resource get created.

- type

A type that TomEE knows. The type is associated with a provider that
knows how to create that type, and also any default properties that the
resource should have if they are not specified in the resource
definition. See service-jar.xml for an example set of service providers
that come with TomEE.

- provider

Explicitly specifies a provider to create the resource, using defaults
for any properties not specified.

- class-name

The fully qualified class that creates the resource. This might the
resource class itself, which is created by calling the constructor, or a
factory class that provides a specific factory method to create the
resource.

- factory-name

The name of the method to call to create the resource. If this is not
specified, the constructor for the class specified by class-name will be
used.

- constructor

Specifies a comma separated list of constructor arguments. These can be
other services, or attributes on the resource itself.

## Custom resources

TomEE allows you to define resources using your own Java classes, and
these can also be injected into managed components in the same way as
known resource types are.

So the following simple resource

```java
public class Configuration {

    private String url;
    private String username;
    private int poolSize;

    // getters and setters
}
```

Can be defined in `tomee.xml` using the following configuration (note
the `class-name` attribute):

```xml
<Resource id="config" class-name="org.superbiz.Configuration">
    url http://localhost
    username tomee
    poolSize 20
</Resource>
```

This resource must be available in TomEE’s system classpath - i.e. it
must be defined in a .jar within the `lib/` directory.

## Field and properties

As shown above, a resource class can define a number of fields, and
TomEE will attempt to apply the values from the resource definition onto
those fields.

As an alternative to this, you can also add a properties field as shown
below, and this will have any used properties from the resource
configuration set added to it. So as an alternative to the above code,
you could do:

```java
public class Configuration {

    private Properties properties;

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

}
```

Using the same resource definition:

```xml
<Resource id="config" class-name="org.superbiz.Configuration">
    url http://localhost
    username tomee
    poolSize 20
</Resource>
```

the url, username and poolSize values will now be available in the
properties field, so for example, the username property could be
accessed via properties.getProperty("username");

## Application resources

Resources can also be defined within an application, and optionally use
classes from the application’s classpath. To define resources in a .war
file, include a `WEB-INF/resources.xml`. For an ejb-jar module, use
`META-INF/resources.xml`.

The format of `resources.xml` uses the same `<Resource>` tag as
`tomee.xml`. One key difference is the root element of the XML is
`<resources>` and not `<tomee>`.

```xml
<resources>
    <Resource id="config" class-name="org.superbiz.Configuration">
        url http://localhost
        username tomee
        poolSize 20
    </Resource>
</resources>
```

This mechanism allows you to package your custom resources within your
application, alongside your application code, rather than requiring a
.jar file in the `lib/` directory.

Application resources are bound in JNDI under
openejb:Resource/appname/resource id.

## Additional resource properties

Resources are typically discovered, created, and bound to JNDI very
early on in the deployment process, as other components depend on them.
This may lead to problems where the final classpath for the application
has not yet been determined, and therefore TomEE is unable to load your
custom resource.

The following properties can be used to change this behavior.

- Lazy

This is a boolean value, which when true, creates a proxy that defers
the actual instantiation of the resource until the first time it is
looked up from JNDI. This can be useful if the resource’s classpath
until the application is started (see below), or to improve startup time
by not fully initializing resources that might not be used.

- UseAppClassLoader

This boolean value forces a lazily instantiated resource to use the
application classloader, instead of the classloader available when the
resources were first processed.

- InitializeAfterDeployment

This boolean setting forces a resource created with the Lazy property to
be instantiated once the application has started, as opposed to waiting
for it to be looked up. Use this flag if you require the resource to be
loaded, irrespective of whether it is injected into a managed component
or manually looked up.

By default, all of these settings are `false`, unless TomEE encounters a
custom application resource that cannot be instantiated until the
application has started. In this case, it will set these three flags to
`true`, unless the `Lazy` flag has been explicitly set.

## Initializing resources

### constructor

By default, if no factory-name attribute and no constructor attribute is
specified on the `Resource`, TomEE will instantiate the resource using
its no-arg constructor. If you wish to pass constructor arguments,
specify the arguments as a comma separated list:

```xml
<Resource id="config" class-name="org.superbiz.Configuration" constructor="id, poolSize">
    url http://localhost
    username tomee
    poolSize 20
</Resource>
```

### constructor-types

When no types are explicitly defined, the underlying mechanism relies on implicit matching to bind
the declared properties to the constructor parameter names.

Because of this, it can be useful to explicitly define the constructor argument types to ensure that
the correct constructor or factory method is selected for the configured resource.

You can specify the types as shown below:

```xml
<Resource id="config" class-name="org.superbiz.Configuration" constructor="id, poolSize" constructor-types="java.lang.String,int">
    url http://localhost
    username tomee
    poolSize 20
</Resource>
```

### factory-name method

In some circumstances, it may be desirable to add some additional logic
to the creation process, or to use a factory pattern to create
resources. TomEE also provides this facility via the `factory-name`
method. The `factory-name` attribute on the resource can reference any
no argument method that returns an object on the class specified in the
`class-name` attribute.

For example:

```java
public class Factory {

    private Properties properties;

    public Object create() {

         MyResource resource = new MyResource();
         // some custom logic here, maybe using this.properties

         return resource;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

}

<resources>
    <Resource id="MyResource" class-name="org.superbiz.Factory" factory-name="create">
        UserName tomee
    </Resource>
</resources>
```

### @PostConstruct / @PreDestroy

As an alternative to using a factory method or a constructor, you can
use `@PostConstruct` and `@PreDestroy` methods within your resource class
(note that you cannot use this within a different factory class) to
manage any additional creation or cleanup activities. TomEE will
automatically call these methods when the application is started and
destroyed. Using `@PostConstruct` will effectively force a lazily loaded
resource to be instantiated when the application is starting - in the
same way that the `InitializeAfterDeployment` property does.

```java
public class MyClass {

    private Properties properties;

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }

    @PostConstruct
        public void postConstruct() throws MBeanRegistrationException {
            // some custom initialization
        }
    }

}
```

## Examples

The following examples demonstrate including custom resources within
your application:

- resources-jmx-example
- resources-declared-in-webapp

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-application-resources--)

---

<a id="tomee-apache-org-tomee-10-1-docs-arquillian-available-adapters"></a>

# Apache TomEE

```xml
<container qualifier="tomee" default="true">
    <configuration>
        <property name="httpPort">-1</property>
        <property name="stopPort">-1</property>
        <!--Optional Container Properties-->
        <property name="properties">
            aproperty=something
        </property>
        <!--Optional Remote Adapter Deployer Properties
        <property name="deployerProperties">
            aproperty=something
        </property>
        -->
    </configuration>
</container>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-arquillian-getting-started"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Getting started with Arquillian and TomEE

Arquillian is a testing framework on top of JUnit (or TestNG if you
prefer). It makes it easier to do integration tests in a managed
environment (JEE environment here after).

We provide an embedded and remote adapter, see
[the available adapters](#tomee-apache-org-tomee-10-1-docs-arquillian-available-adapters) for more
details.

In a managed environment it is usually quite difficult to perform unit
tests, due to the fact that most of the time you have to mock almost the
entire environment. It is very time-consuming and requires complicated
integration tests that must reflect the production environment as best
as possible. Unit tests lose their true value.

JEE always got seen as a heavy technology, impossible to test and to
use in development. OpenEJB always fought against that idea and proved
that it’s really possible.

As David Blevins said:

> "Do not blame EJBs (ie. Java EE) because your
> server is not testable."

With latest Java EE specifications (5 and especially 6), it becomes a
reality. Arquillian typically addresses that area. It is basically a
framework that aims at helping/managing the server/container in an
agnostic way. Arquillian is responsible for the lifecycle of the
container (start, deploy, undeploy, stop, etc).

TomEE community heavily invested on that framework to prove it’s really
useful and can really help testing Java EE application. That’s also an
opportunity to get the most out of TomEE (lightweight, fast,
feature-rich, etc).

|  | SeeArquillian.orgfor a great quick-start tutorial on Arquillian itself. |
| --- | --- |

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-arquillian-getting-started--)

---

<a id="tomee-apache-org-tomee-10-1-docs-basics-getting-things"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Basics - Getting Things

# Getting Stuff from the Container

Generally speaking the only way to get a
[Container-Managed Resource](container-managed-resource.html) is via
*dependency injection* or *lookup* from within a [Container-Managed
Component] .

The *unbreakable rules*. Read these over and over again when things
don’t work.

1. java:comp/env is the spec defined namespace for lookup of any
   [Container-Managed Resource](container-managed-resource.html)
2. java:comp/env is *empty* by default
3. java:comp/env is *read-only* at runtime
4. java:comp/env is populated by [Declaring
   References](#tomee-apache-org-tomee-10-1-docs-declaring-references) to [Container-Managed Resource] via xml or annotation
5. only [Container-Managed
   Component](container-managed-component.html) s, *not* their libraries, can [Declare References|Declaring
   References] via xml or annotation
6. only [Container-Managed
   Component](container-managed-component.html) s, *not* their libraries, can get dependency injection of
   [Container-Managed Resource] s
7. only [Container-Managed
   Component](container-managed-component.html) s, *and* their libraries, may look up from java:comp/env
8. you *must* use the *no-arg* 'new InitialContext()' constructor to
   lookup something from java:comp/env
9. the annotations and xml for [Declaring
   References](#tomee-apache-org-tomee-10-1-docs-declaring-references) are *identical* in functionality, both *always* configure
   lookup with *optional* dependency injection

## Common mistakes, misunderstandings, and myths

- *"I tried it via annotation, and it didn’t work, so I used xml and
  then it did work"*

See rule 9. If one form worked and the other didn’t, it means you simply
made a mistake in using one versus the other. Use what works for you,
but understand both annotations or xml will work for either lookup or
injection if used correctly.

- *"I need to use lookups, so I can’t use the annotation"*

See rule 9. Annotations are not just for injection, that is just how
they are typically used. Know that when you use an annotation for
injection, it will *always* create an entry in java:comp/env. As well
you can use the annotation at the *class level* and it will cause no
dependency injection and only the entry creation in java:comp/env.

- *"I don’t want injection, so I can’t use the annotation"*

See rule 9 and the above. You can use the annotation at the *class
level* and it will cause no dependency injection and only the entry
creation in java:comp/env.

- *"I tried to list java:comp/env but it’s empty?!"*

See rule 2 and rule 4. There will be nothing in java:comp/env unless you
[Declare a Reference](#tomee-apache-org-tomee-10-1-docs-declaring-references) to it. It does not
matter if is a DataSource configured at the server level, etc. Nothing
is bound into java:comp/env unless you explicitly declare a reference to
it. The Java EE 5 TCK (Technology Compatibility Kit) tests for this
extensively and is a rule we cannot break. Java EE 6 does finally offer
some new namesaces (java:global, java:app, and java:module) which will
offer some great new options for more global-style lookups.

- *"I deployed the EJB but can’t look it up, it’s name is Foo"*

See rule 2 and the above. Just creating an EJB doesn’t cause it to be
added to java:comp/env. If a
[Container-Managed Component](container-managed-component.html) wants
to look up the EJB they must [Declare a Reference|Declaring References]
to it via the `@EJB` annotation or <ejb-local-ref> or <ejb-ref> in xml.
In Java EE 6, however, EJBs will be automatically bound to
"java:global[/<app-name>]/<module-name>/<bean-name>[!<fully-qualified-interface-name>]"
and can be looked up without declaring a reference first.

- *"Which InitialContextFactory do I use for java:comp/env?"*

See rule 8. You are not allowed to use an InitialContextFactory for
java:comp/env lookups. Setting an InitialContextFactory via
'java.naming.factory.initial' in either System properties,
InitialContext properties, or a jndi.properties file is illegal and will
cause java:comp/env lookups to fail.

- *"My Client can’t look up the EJB from java:comp/env"*

See rule 7. A plain, standalone, Java application cannot use
java:comp/env. There is the official concept of a Java EE Application
Client which can be packaged in an ear and deployed into the Container.
In practice, most people find them restrictive, cumbersome, and hard to
use and are therefore rarely employed in "real world" projects. Most
people opt to use the non-standard, vendor-specific, approach to looking
up EJBs from their plain java clients. In OpenEJB this can be done via
either the RemoteInitialContextFactory (for remote clients) or the
LocalInitialContextFactory (for local clients of an embedded container).
The JNDI names can be configured as [shown here](#tomee-apache-org-tomee-10-1-docs-jndi-names) .

- *"I declared the reference, but still can’t look it up"*

See all of the above and reread the rules a few times. Always check the
log output as well.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-basics-getting-things--)

---

<a id="tomee-apache-org-tomee-10-1-docs-basics-security"></a>

# Apache TomEE

```java
import jakarta.ejb.EJB;
import javax.naming.InitialContext;

@EJB(name = "otherBean", beanInterface = IOtherBean.class)
public class MyBean
{
    public IOtherBean getOtherBean()
    {
    InitialContext context = new InitialContext();
    return (IOtherBean) context.lookup("java:comp/env/otherBean");
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-basics-transactions"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Basics - Transactions

One of the many benefits of EJB, is that
transactions within the EJB container are generally managed entirely
automatically. Any EJB component will, by default, partake in that
transaction.

Here are some basic rules to understand about transactions. Keep note
that this is the default behaviour, and the system can be configured to
behave differently, depending on the needs of your system, bean, or
individual methods of your beans.

## Participants

Various components and parts of the EJB system can be part of a
transaction. Examples are

1. Session bean
2. Message Driven Bean
3. EntityManager (a.k.a. Persistence context)

## Behaviour

The basic default behaviours are 1. A transaction starts at the
beginning of the first EJB method call, in a chain of calls that are
participating in the given transaction 1. A transaction ends at the end
of the first EJB method, in the same chain 1. If a bean that has started
a transaction, uses another bean, that bean will automatically use the
same transaction as the calling bean.

## Configuration

You can configure your beans in a variety of ways. Generally speaking, a
transaction is started when a method is called, but can be configured
using `@TransactionAttribute`(value = TransactionAttributeType.X), where X
is one of…​

1. REQUIRED - the default, which is to start a transaction if one does
   not exist, but to use the existing one if it has already been started.
2. REQUIRES\_NEW - the transaction is created on every call, and ends when
   the call is completed. Beans don’t partake in transactions created by
   other parts of the system.
3. MANDATORY - a transaction must always exist prior to the call, and it
   will be used. It is an error otherwise
4. NOT\_SUPPORTED - component not included in the transaction
5. SUPPORTS - transaction will be used if it exists, but will not be
   created if it does not exist
6. NEVER - if a transaction exists, it is an error to call the method

@TransactionAttribute applies to both methods and entire beans. You may
set one type of transaction behaviour (as seen above) on the bean, and a
different one on a specific method of that same bean, which overrides
the one configured for the overall bean. For instance, maybe you want to
make an audit entry in the database that you are about to attempt a
credit card payment. It really needs to be in its own transaction so
that it is IMMEDIATELY committed for audit purposes, if something goes
wrong with the credit card payment. So, perhaps you use MANDATORY on the
bean, and REQUIRES\_NEW on the method for audit logging. As soon as the
method that does the audit logging is complete, the transaction is
committed, and the credit card payment transaction continues on its way.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-basics-transactions--)

---

<a id="tomee-apache-org-tomee-10-1-docs-bmpentitycontainer-config"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# BmpEntityContainer Configuration

A BmpEntityContainer can be declared via xml in the
`<tomee-home>/conf/tomee.xml` file or in a `WEB-INF/resources.xml` file
using a declaration like the following. All properties in the element
body are optional.

```xml
<Container id="myBmpEntityContainer" type="BMP_ENTITY">
    poolSize = 10
</Container>
```

Alternatively, a BmpEntityContainer can be declared via properties in
the `<tomee-home>/conf/system.properties` file or via Java
VirtualMachine `-D` properties. The properties can also be used when
embedding TomEE via the `jakarta.ejb.embeddable.EJBContainer` API or
`InitialContext`

```properties
myBmpEntityContainer = new://Container?type=BMP_ENTITY
myBmpEntityContainer.poolSize = 10
```

Properties and xml can be mixed. Properties will override the xml
allowing for easy configuration change without the need for $\{} style
variable substitution. Properties are not case-sensitive. If a property
is specified that is not supported by the declared BmpEntityContainer a
warning will be logged. If a BmpEntityContainer is needed by the
application and one is not declared, TomEE will create one dynamically
using default settings. Multiple BmpEntityContainer declarations are
allowed. # Supported Properties

Property

Type

Default

Description

poolSize

int

10

Specifies the size of the bean pools for this bmp entity container.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-bmpentitycontainer-config--)

---

<a id="tomee-apache-org-tomee-10-1-docs-bouncy-castle"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Installing Bouncy Castle

|  | Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at .http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. |
| --- | --- |

Installation of Bouncy Castle for use in TomEE itself is done in two
steps:

1. Add the Bouncy Castle provider jar to the `$JAVA_HOME/jre/lib/ext`
   directory
2. Create a Bouncy Castle provider entry in the
   `$JAVA_HOME/jre/lib/security/java.security` file

The entry to `java.security` will look something like the following:

```properties
security.provider.N=org.bouncycastle.jce.provider.BouncyCastleProvider
```

Replace `N` with the order of precedence you would like to give Bouncy
Castle in comparison to the other providers in the file. **Recommended**
would be the last entry in the list — `N` being the highest number in
the list. **Warning** that configuring Bouncy Castle as the first
provider, `security.provider.1`, may cause JVM errors.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-bouncy-castle--)

---

<a id="tomee-apache-org-tomee-10-1-docs-built-in-type-converters"></a>

# Apache TomEE

```java
package org.superbiz.foo;

import java.util.Date;

@Stateless
public class MyBean {

    @Resource
    private Date myDate;
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-callbacks"></a>

# Apache TomEE

```java
import jakarta.ejb.Stateless;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.InvocationContext;

@Stateless
public class MyStatelessBean implements  MyBusinessInterface  {

    @PostConstruct
    public void constructed(){

    }

    @PreDestroy
    public void destroy(){

    }

    @AroundInvoke
    public Object invoke(InvocationContext invocationContext) throws Exception {
    return invocationContext.proceed();
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-changing-jms-implementations"></a>

# Apache TomEE

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ServiceJar>
  <ServiceProvider
      id="genericra"
      service="Resource"
      types="GenericJMSRA"
      class-name="com.sun.genericra.GenericJMSRA">
          UserName
          Password
          ProviderIntegrationMode
          ConnectionFactoryClassName
          QueueConnectionFactoryClassName
          TopicConnectionFactoryClassName
          XAConnectionFactoryClassName
          XAQueueConnectionFactoryClassName
          XATopicConnectionFactoryClassName
          UnifiedDestinationClassName
          TopicClassName
          QueueClassName
          SupportsXA
          ConnectionFactoryProperties
          JndiProperties
          CommonSetterMethodName
          RMPolicy
          LogLevel
          DeliveryType
          UseFirstXAForRedelivery
  </ServiceProvider>

  <ServiceProvider
      id="ConnectionFactory"
      service="Resource"
      types="jakarta.jms.ConnectionFactory, jakarta.jms.QueueConnectionFactory, jakarta.jms.TopicConnectionFactory, QueueConnectionFactory, TopicConnectionFactory"
      class-name="com.sun.genericra.outbound.ManagedJMSConnectionFactory">
          ConnectionFactoryJndiName
          ClientId
          ConnectionValidationEnabled
          ResourceAdapter
  </ServiceProvider>

  <ServiceProvider
      id="Queue"
      service="Resource"
      types="jakarta.jms.Queue, Queue"
      class-name="com.sun.genericra.outbound.QueueProxy">
          DestinationJndiName
          ResourceAdapter
          UserName
          Password
          JndiProperties
          QueueClassName
  </ServiceProvider>

  <ServiceProvider
      id="Topic"
      service="Resource"
      types="jakarta.jms.Topic, Topic"
      class-name="com.sun.genericra.outbound.TopicProxy">
          DestinationJndiName
          ResourceAdapter
          UserName
          Password
          JndiProperties
          TopicClassName
  </ServiceProvider>
</ServiceJar>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-client-server-transports"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Client-Server Transports

# Client/Server transports

jar

transport description

openejb-ejbd-3.0.jar

provides the 'ejbd' protocol. A binary protocol traveling over a socket

openejb-http-3.0.jar

supports the ejbd protocol over http

openejb-derbynet-3.0.jar

allows for derby to be accessed via it’s network driver

openejb-hsql-3.0.jar

allows for hsqldb to be accessed via it’s network driver

openejb-cxf-3.0.jar

turns on webservice ability, soap/http, via cxf

openejb-activemq-3.0.jar

supports remote jms clients via activemq

openejb-telnet-3.0.jar

allows for connecting to the server via telnet for monitoring

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-client-server-transports--)

---

<a id="tomee-apache-org-tomee-10-1-docs-clients"></a>

# Apache TomEE

```java
Properties p = new Properties();
p.put("java.naming.factory.initial", "org.apache.openejb.client.LocalInitialContextFactory");

InitialContext ctx = new InitialContext(p);

MyBean myBean = (MyBean) ctx.lookup("MyBeanRemote");
```

---

<a id="tomee-apache-org-tomee-10-1-docs-cmpentitycontainer-config"></a>

# Apache TomEE

```xml
<Container id="myCmpEntityContainer" type="CMP_ENTITY">
    cmpEngineFactory = org.apache.openejb.core.cmp.jpa.JpaCmpEngineFactory
</Container>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-collapsed-ear"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Collapsed EAR

# One archive

The basic idea of this approach is that your Servlets and EJBs are
together in your WAR file as one app.

- No classloader boundaries between Servlets and EJBs
- EJBs and Servlets can share all third-party libraries (like Spring!) -
  no EAR required.
- Can put the `web.xml` and `ejb-jar.xml` in the same archive (the WAR
  file).
- EJBs can see Servlet classes and vice versa.

# Not quite J2EE (it is truly Java EE6)

This is very different from J2EE or Java EE 5 as there aren’t several
levels of separation and classloader hierarchy. This is going to take
some getting used to, and it should be understood that this style of
packaging isn’t J2EE compliant. Who would care tough as it is a feature
of Java EE 6 we would’ve been waiting for so long.

J2EE classloading rules:

- You cannot ever have EJBs and servlets in the same classloader.
- Three classloader minimum; a classloader for the ear, one for each
  ejb-jar, and one for each WAR file.
- Servlets can see EJBs, but EJBs cannot see servlets.

To pull that off, J2EE has to kill you on packaging: \* You cannot have
EJB classes and Servlet classes in the same archive. \* You need at least
three archives to combine servlets and ejbs; 1 EAR containing 1 EJB jar
and 1 servlet WAR. \* Shared libraries must go in the EAR and be included
in a specially formatted 'Class-Path' entry in the EAR’s MANIFEST file.

Critically speaking, forcing more than one classloader on an application
is where J2EE "jumps the shark" for a large majority of people’s needs.

# Example with Tomcat

If you want to try to work with Servlets/JSP and OpenEJB using Tomcat,
see the openejbx30:tomcat.html[setup page] and the
"/webapps/ejb-examples" section of the
[openejb-examples.zip](downloads.html) available on the
[download page](http://tomee.apache.org/downloads.html).

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-collapsed-ear--)

---

<a id="tomee-apache-org-tomee-10-1-docs-common-datasource-configurations"></a>

# Apache TomEE

```xml
<Resource id="HSQLDB Database" type="DataSource">
    JdbcDriver org.hsqldb.jdbcDriver
    JdbcUrl jdbc:hsqldb:file:hsqldb
    UserName sa
    Password
</Resource>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-common-errors"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Common Errors

<a name="CommonErrors-Cannotfindcontainer"FOO"forbean"BAR""> # Cannot
find container "FOO" for bean "BAR"

When a bean gets deployed in OpenEJB, it gets associated with a
particular container. Subsequently, that container may not be configured
in that instance of the server. When the server loads the Jar with the
deployed beans, it places beans in the containers that the beans were
configured with. Here, the bean BAR wants to go into the container FOO,
which is not currently configured.

This message is displayed when the server is starting up. <a
name="CommonErrors-Cannotfindbean"FOO"referencedbybean"BAR"."> # Cannot
find bean "FOO" referenced by bean "BAR".

When a bean gets deployed in OpenEJB, it may contain references to other
beans. Subsequently, those beans may not be configured in that instance
of the server. When the server loads the Jar with the deployed beans, it
stores those references to those beans. Here, the bean BAR references
FOO, which is not currently configured in the JNDI namespace.

This message is displayed when the server is starting up.

This message is usually the result of a deployment descriptor that has
been created by hand.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-common-errors--)

---

<a id="tomee-apache-org-tomee-10-1-docs-common-persistenceprovider-properties"></a>

# Apache TomEE

```xml
<properties>

  <!--http://www.oracle.com/technology/products/ias/toplink/JPA/essentials/toplink-jpa-extensions.html-->
  <property name="toplink.ddl-generation" value="drop-and-create-tables"/>
  <property name="toplink.logging.level" value="FINEST"/>
  <property name="toplink.ddl-generation.output-mode" value="both"/>
  <property name="toplink.target-server" value="pl.zsk.samples.ejbservice.OpenEJBServerPlatform"/>
</properties>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-comparison"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Comparison

## Differences between TomEE versions and/or flavors

[See main comparison page.](../../comparison.html)

## <a id="tomee-apache-org-tomee-10-1-docs-comparison--specifications"></a> Detailed list of Jakarta EE 10 and MicroProfile 6.0 specifications

| Specifications | Tomcat | TomEE WebProfile | TomEE MicroProfile | TomEE Plus | TomEE Plume |
| --- | --- | --- | --- | --- | --- |
| Jakarta Annotations2.1 |  |  |  |  |  |
| Jakarta Authentication(JASPIC) 3.0 |  |  |  |  |  |
| Jakarta Debugging Support for Other Languages2.0 |  |  |  |  |  |
| Jakarta Servlet6.0 |  |  |  |  |  |
| Jakarta Server Pages(JSP) 3.1 |  |  |  |  |  |
| Jakarta Expression Language(EL) 5.0 |  |  |  |  |  |
| Jakarta WebSocket2.1 |  |  |  |  |  |
| Jakarta Web Profile specifications10 |  |  |  |  |  |
| Jakarta Activation2.1 |  |  |  |  |  |
| Jakarta Bean Validation3.0 |  |  |  |  |  |
| Jakarta Contexts and Dependency Injection(CDI) 4.0 |  |  |  |  |  |
| Jakarta Concurrency3.0 |  |  |  |  |  |
| Jakarta Dependency Injection(@Inject) 2.0 |  |  |  |  |  |
| Jakarta Enterprise Beans(EJB) 4.0 |  |  |  |  |  |
| Jakarta Faces(JSF) 4.0 |  |  |  |  |  |
| Jakarta Interceptors2.1 |  |  |  |  |  |
| Jakarta JSON Binding(JSON-B) 3.0 |  |  |  |  |  |
| Jakarta JSON Processing(JSON-P) 2.1 |  |  |  |  |  |
| Jakarta Managed Beans2.0 |  |  |  |  |  |
| Jakarta Persistence(JPA) 3.1 |  |  |  |  |  |
| Jakarta RESTful Web Services(JAX-RS) 3.1 |  |  |  |  |  |
| Jakarta Security(Enterprise Security) * 3.0 |  |  |  |  |  |
| Jakarta Standard Tag Library(JSTL) * 3.0 |  |  |  |  |  |
| Jakarta Transactions(JTA) 2.0 |  |  |  |  |  |
| Jakarta XML Binding(JAXB) 3.0 |  |  |  |  |  |
| MicroProfile specifications6.0 |  |  |  |  |  |
| MicroProfile Config3.0 |  |  |  |  |  |
| MicroProfile Fault Tolerance4.0 |  |  |  |  |  |
| MicroProfile Health4.0 |  |  |  |  |  |
| MicroProfile JWT Authentication2.0 |  |  |  |  |  |
| MicroProfile Metrics4.0 |  |  |  |  |  |
| MicroProfile OpenAPI3.0 |  |  |  |  |  |
| MicroProfile Telemetry1.0 |  |  |  |  |  |
| MicroProfile Rest Client3.0 |  |  |  |  |  |
| Jakarta EE specifications10 |  |  |  |  |  |
| Jakarta Authorization(JACC) 2.1 |  |  |  |  |  |
| Jakarta Batch(JBatch) 2.1 |  |  |  |  |  |
| Jakarta Connectors2.1 |  |  |  |  |  |
| Jakarta Enterprise Web Services2.0 |  |  |  |  |  |
| Jakarta Mail(JavaMail) 2.1 |  |  |  |  |  |
| Jakarta Messaging(JMS) 3.1 |  |  |  |  |  |
| Jakarta SOAP with Attachments(SAAJ) 2.0 |  |  |  |  |  |
| Jakarta Web Services Metadata(JWS) 3.0 |  |  |  |  |  |
| Jakarta XML Web Services(JAX-WS) 3.0 |  |  |  |  |  |
| Jakarta Faces (JSF) implementation |  | MyFaces | MyFaces | MyFaces | Mojarra |
| Jakarta Persistence (JPA) implementation(s) |  | OpenJPA | OpenJPA | OpenJPA | OpenJPA,EclipseLink |

- Please note that Tomcat does not ship with the jars for Standard Tag Library (JSTL) nor the jakarta.security.enterprise.\* packages.

## <a id="tomee-apache-org-tomee-10-1-docs-comparison--implementations"></a> Implementations of Jakarta EE and MicroProfile features in TomEE 10.x

| Specifications | Implementations included by TomEE 10.x |
| --- | --- |
| Jakarta Servlet, Server Pages (JSP), Expression Language (EL),Jakarta Annotations, Authentication (JASPIC), WebSocket, …​ | Apache Tomcat10.1.x |
| Jakarta Standard Tag Library (JSTL) | Apache Standard Taglib Implementation |
| Jakarta Faces (JSF) | Apache MyFaces(shipped in all TomEE flavors except Plume)Eclipse Mojarra(shipped in TomEE Plume) |
| Jakarta Contexts and Dependency Injection (CDI) | Apache OpenWebBeans4.x (with jakarta classifier) |
| Jakarta Enterprise Beans (EJB) | Apache OpenEJB |
| Jakarta Transactions (JTA) | Apache Geronimo Transaction Manager |
| Jakarta Persistence (JPA) | Apache OpenJPA4.0.x jakarta (shipped in all TomEE flavors)EclipseLink4.0.x(shipped in TomEE Plume) |
| Jakarta Bean Validation | Apache BVal |
| Web Services | Apache CXF4.1.x |
| Jakarta JSON Binding (JSON-B),Jakarta JSON Processing (JSON-P) | Apache Johnzon2.0.x |
| Jakarta XML Binding (JAXB) | Eclipse Implementation of JAXB3.0.x |
| Jakarta Mail (JavaMail) | Apache Geronimo JavaMail |
| MicroProfile | Apache Geronimo MicroProfile(ok with TomEE 7.1.x and 8.x)SmallRye MicroProfile(ok with TomEE 9.x and later) |
| Jakarta Batch (JBatch) | Apache BatchEE |
| Jakarta Messaging (JMS) | Apache ActiveMQ |

In bold : Implementations that differ between flavors or between versions

## <a id="tomee-apache-org-tomee-10-1-docs-comparison--Compatibility"></a> Compatibility with other implementations

| Specifications | Implementations alternatives |
| --- | --- |
| Jakarta Persistence (JPA) | Hibernate ORM6.6.x |
| Other containers (CDI, EJB, JTA, etc.) and frameworks | Spring6.0.x |

- Please note that TomEE does not ship with the jars for Hibernate ORM, Jersey, Krazo, Spring.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-comparison--)

---

<a id="tomee-apache-org-tomee-10-1-docs-concepts"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Concepts

OpenEJB was founded on the idea that it would be embedded into
third-party environments whom would likely already have three things:

- their one "server" platform with existing clients and protocols
- their own way to configure their platform
- existing services like TransactionManager, Security, and Connector

Thus, the focus of OpenEJB was to create an EJB implementation that would
be easily embeddable, configurable, and customizable.

Part of achieving that is a drive to be as simple as possible as to not
over-define and therefore restrict the ability to be embeddable,
configurable and customizable. Smaller third-party environments could
easily 'downscale' OpenEJB in their integrations by replacing standard
components with lighter implementations or removing them all together
and larger environments could 'upscale' OpenEJB by replacing and adding
heavier implementations of those standard components likely tailored to
their systems and infrastructure.

Container and Server are mentioned in the EJB spec as being separate
things but are never defined formally. In our world Containers, which
implement the basic component contract and lifecycle of a bean are not
coupled to any particular Server, which has the job of providing a
naming service and providing a way for its clients to reference and
invoke components (beans) hosted in Containers. Because Containers have
no dependence at all only Server, you can run OpenEJB without any Server
at all in an embedded environment for example without any work or any
extra overhead. Similarly, you can add as many new Server components as
you want without ever having to modify any Containers.

There is a very strong pluggability focus in OpenEJB as it was always
intended to be embedded and customized in other environments. As a
result all Containers are pluggable, isolated from each other, and no
one Container is bound to another Container and therefore removing or
adding a Container has no repercussions on the other Containers in the
system. TransactionManager, SecurityService and Connector also pluggable
and are services exposed to Containers. A Container may not be dependent
on specific implementations of those services. Service Providers define
what services they are offering (Container, Connector, Security,
Transaction, etc.) in a file they place in their jar called
service-jar.xml.

The service-jar.xml should be placed not in the META-INF but somewhere
in your package hierarchy (ours is in
/org/apache/openejb/service-jar.xml) which allows the services in your
service-jar.xml to be referenced by name (such as
DefaultStatefulContainer) or more specifically by package and id (such
as org.apache.openejb#DefaultStatefulContainer).

The same implementation of a service can be declared several times in a
service-jar.xml with different ids. This allows for you to set up
several different profiles or pre-configured versions of the services
you provide each with a different name and different set of default
values for its properties.

In your openejb.conf file when you declare Containers and Connectors, we
are actually hooking you up with Service Providers automatically. You
get what is in the org/apache/openejb/service-jar.xml by default, but
you are able to point specifically to a specific Service Provider by the
'provider' attribute on the Container, Connector, TransactionManager,
SecurityService, etc. elements of the openejb.conf file. When you
declare a service (Container, Connector, etc.) in your openejb.conf file
the properties you supply override the properties supplied by the
Service Provider, thus you only need to specify the properties you’d
like to change and can have your openejb.conf file as large or as small
as you would like it. The act of doing this can be thought of as
essentially instantiating the Service Provider and configuring that
instance for inclusion in the runtime system.

For example Container(id=NoTimeoutStatefulContainer,
provider=DefaultStatefulContainer) could be declared with its Timeout
property set to 0 for never, and a
Container(id=ShortTimeoutStatefulContainer,
provider=DefaultStatefulContainer) could be declared with its Timeout
property set to 15 minutes. Both would be instances of the
DefaultStatefulContainer Service Provider which is a service of type
Container.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-concepts--)

---

<a id="tomee-apache-org-tomee-10-1-docs-configuration"></a>

# Apache TomEE

```xml
<?xml version="1.0"?>
<openejb>
  <Container id="Default CMP Container" ctype="CMP_ENTITY">
    Global_TX_Database  c:/my/app/conf/postgresql.cmp_global_database.xml
    Local_TX_Database   c:/my/app/conf/postgresql.cmp_local_database.xml
  </Container>
  <Connector id="Default JDBC Database">
    JdbcDriver org.postgresql.Driver
    JdbcUrl jdbc:postgresql://localhost/mydb
    UserName username
    Password password
  </Connector>
  <SecurityService id="Default Security Service"/>
  <TransactionService id="Default Transaction Manager"/>
  <Deployments jar="c:/my/app/employee.jar"/>
  <Deployments dir="beans/" />
</openejb>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-configuring-containers-in-tests"></a>

# Apache TomEE

```java
Properties p = new Properties();
p.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.core.LocalInitialContextFactory");

p.put("myStatefulContainer", "new://Container?type=STATEFUL");
p.put("myStatefulContainer.PoolSize", "0");
p.put("myStatefulContainer.BulkPassivate", "1");

Context context = new InitialContext(p);
```

---

<a id="tomee-apache-org-tomee-10-1-docs-configuring-datasources-in-tests"></a>

# Apache TomEE

```java
Properties p = new Properties();
p.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.core.LocalInitialContextFactory");

p.put("myDataSource", "new://Resource?type=DataSource");
p.put("myDataSource.JdbcDriver", "org.apache.derby.jdbc.EmbeddedDriver");
p.put("myDataSource.JdbcUrl", "jdbc:derby:derbyDB;create=true");
p.put("myDataSource.JtaManaged", "true");

Context context = new InitialContext(p);
```

---

<a id="tomee-apache-org-tomee-10-1-docs-configuring-datasources-xa"></a>

# Apache TomEE

```xml
<Resource id="demo/jdbc/XADataSourceXA" type="XADataSource" class-name="com.mysql.cj.jdbc.MysqlXADataSource">
    Url jdbc:mysql://192.168.37.202:3306/movie
</Resource>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-configuring-datasources"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Configuring DataSources in tomee.xml

The *\_ element is used to configure a \_javax.sql.DataSource*. It is also
used to configure other resources like Timers, Topics, Queues. We will
see some examples of using to configure a DataSource.

The element is designed after `@Resource` annotation and has similar
attributes.

For example, this annotation in your bean:

```java
@Resource(name = "myDerbyDatasource", type = javax.sql.DataSource.class)
```

Would map to a Resource declared in your openejb.xml as follows:

```xml
<Resource id="myDerbyDatasource" type="javax.sql.DataSource">
 . . . .
<Resource>
```

Note that in the xml element, the *type* value of *javax.sql.DataSource*
can be abbreviated to just *DataSource* as follows:

```xml
<Resource id="myDerbyDatasource" type="DataSource">
 . . . .
<Resource>
```

It is also possible to specify the path to the driver jar file using a
classpath attribute like so:

```xml
<Resource id="myDerbyDatasource" type="DataSource" classpath="/path/to/driver.jar">
 . . . .
<Resource>
```

…​Or in a [Maven](http://maven.apache.org/) environment like so:

```xml
<Resource id="myDerbyDatasource" type="DataSource" classpath="mvn:org.apache.derby:derby:10.10.1.1">
 . . . .
<Resource>
```

See [Containers and Resources](#tomee-apache-org-tomee-10-1-docs-containers-and-resources) for a
complete list of supported DataSource properties.

See [DataSource Password
Encryption](#tomee-apache-org-tomee-10-1-docs-datasource-password-encryption) for information on specifying non-plain-text database
passwords in your openejb.xml file.

See [Common DataSource
Configurations](#tomee-apache-org-tomee-10-1-docs-common-datasource-configurations) for a list of the commonly used databases and their
driver configurations.

See [DataSource
Configuration by Creator](#tomee-apache-org-tomee-10-1-docs-datasource-configuration-by-creator) for a list of the different properties
supported for each data source creator.

You may also need data partitioning per customer or depending on any
other business criteria. That’s also an available feature. See
[Dynamic Datasource](#tomee-apache-org-tomee-10-1-docs-dynamic-datasource) for more details.

## JNDI names for configured DataSources

### Example 1

```xml
<Resource id="Default JDBC Database" type="DataSource">
   . . . . .
</Resource>
```

The global jndi name would be *java:openejb/Resource/Default JDBC
Database*

### Example 2

```xml
<Resource id="Derby Database"  type="DataSource">
  . . . . .
</Resource>
```

The global jndi name would be *java:openejb/Resource/Derby Database*

## Obtaining a DataSource

DataSource references in your ejb should get automatically mapped to the
Resource you declare. The shortest and easiest rule is that *if your
reference name matches a Resource in your openejb.xml, that’s the one
you get*.  Essentially, the rules for mapping are as follows.

1. Name Attribute Match - `@Resource` with a name attribute matching the
   resource name gets that resource injected
2. Injected Name Match - variable name matching the resource name gets
   that resource injected
3. No Match - nothing matches a resource name, so the first resource
   available gets injected

There are various ways one could obtain a DataSource now. Let’s take an
example of Derby.

With a Resource declaration in your openejb.xml like this:

```xml
<Resource id="myDerbyDatabase"  type="DataSource">
  . . . . .
</Resource>
```

There are several possible ways to refer to it, as follows.

*BY matching variable name to resource name*

```java
@Stateless
public class FooBean {
    @Resource DataSource myDerbyDatabase;
}
```

*OR BY matching name*

```java
@Stateless
public class FooBean {
    @Resource(name="myDerbyDatabase")
    DataSource dataSource;
}
```

*OR BY JNDI lookup*

```java
@Resource(name="myDerbyDatabase", type=javax.sql.DataSource.class)
@Stateless
public class FooBean {

    public void setSessionContext(SessionContext sessionContext) {
        DataSource dataSource = (DataSource)
        sessionContext.lookup("myDerbyDatabase");
    }

    public void someOtherMethod() throws Exception {
        InitialContext initialContext = new InitialContext();
        DataSource dataSource = (DataSource)
        initialContext.lookup("java:comp/env/myDerbyDatabase");
    }
}
```

*OR*

```xml
<resource-ref>
  <res-ref-name>myDerbyDatabase</res-ref-name>
  <res-type>javax.sql.DataSource</res-type>
</resource-ref>
```

*OR*

```xml
<resource-ref>
   <res-ref-name>jdbc/myDerbyDatabase</res-ref-name>
   <res-type>javax.sql.DataSource</res-type>
</resource-ref>
```

*OR*

```xml
<resource-ref>
   <res-ref-name>someOtherName</res-ref-name>
   <res-type>javax.sql.DataSource</res-type>
   <mapped-name>myDerbyDatabase</mapped-name>
</resource-ref>
```

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-configuring-datasources--)

---

<a id="tomee-apache-org-tomee-10-1-docs-configuring-durations"></a>

# Apache TomEE

```java
  if (u.equalsIgnoreCase("NANOSECONDS")) return TimeUnit.NANOSECONDS;
  if (u.equalsIgnoreCase("NANOSECOND")) return TimeUnit.NANOSECONDS;
  if (u.equalsIgnoreCase("NANOS")) return TimeUnit.NANOSECONDS;
  if (u.equalsIgnoreCase("NANO")) return TimeUnit.NANOSECONDS;
  if (u.equalsIgnoreCase("NS")) return TimeUnit.NANOSECONDS;

  if (u.equalsIgnoreCase("MICROSECONDS")) return TimeUnit.MICROSECONDS;
  if (u.equalsIgnoreCase("MICROSECOND")) return TimeUnit.MICROSECONDS;
  if (u.equalsIgnoreCase("MICROS")) return TimeUnit.MICROSECONDS;
  if (u.equalsIgnoreCase("MICRO")) return TimeUnit.MICROSECONDS;

  if (u.equalsIgnoreCase("MILLISECONDS")) return TimeUnit.MILLISECONDS;
  if (u.equalsIgnoreCase("MILLISECOND")) return TimeUnit.MILLISECONDS;
  if (u.equalsIgnoreCase("MILLIS")) return TimeUnit.MILLISECONDS;
  if (u.equalsIgnoreCase("MILLI")) return TimeUnit.MILLISECONDS;
  if (u.equalsIgnoreCase("MS")) return TimeUnit.MILLISECONDS;

  if (u.equalsIgnoreCase("SECONDS")) return TimeUnit.SECONDS;
  if (u.equalsIgnoreCase("SECOND")) return TimeUnit.SECONDS;
  if (u.equalsIgnoreCase("SEC")) return TimeUnit.SECONDS;
  if (u.equalsIgnoreCase("S")) return TimeUnit.SECONDS;

  if (u.equalsIgnoreCase("MINUTES")) return TimeUnit.MINUTES;
  if (u.equalsIgnoreCase("MINUTE")) return TimeUnit.MINUTES;
  if (u.equalsIgnoreCase("MIN")) return TimeUnit.MINUTES;
  if (u.equalsIgnoreCase("M")) return TimeUnit.MINUTES;

  if (u.equalsIgnoreCase("HOURS")) return TimeUnit.HOURS;
  if (u.equalsIgnoreCase("HOUR")) return TimeUnit.HOURS;
  if (u.equalsIgnoreCase("HRS")) return TimeUnit.HOURS;
  if (u.equalsIgnoreCase("HR")) return TimeUnit.HOURS;
  if (u.equalsIgnoreCase("H")) return TimeUnit.HOURS;

  if (u.equalsIgnoreCase("DAYS")) return TimeUnit.DAYS;
  if (u.equalsIgnoreCase("DAY")) return TimeUnit.DAYS;
  if (u.equalsIgnoreCase("D")) return TimeUnit.DAYS;
```

---

<a id="tomee-apache-org-tomee-10-1-docs-configuring-javamail"></a>

# Apache TomEE

```xml
<Resource id="SuperbizMail" type="jakarta.mail.Session">
   mail.smtp.host=mail.superbiz.org
   mail.smtp.port=25
   mail.transport.protocol=smtp
   mail.smtp.auth=true
   mail.smtp.user=someuser
   password=mypassword
</Resource>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-configuring-logging-in-tests"></a>

# Apache TomEE

```java
Properties p = new Properties();
p.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.core.LocalInitialContextFactory");

p.put("log4j.rootLogger", "fatal,C");
p.put("log4j.category.OpenEJB", "warn");
p.put("log4j.category.OpenEJB.options", "info");
p.put("log4j.category.OpenEJB.server", "info");
p.put("log4j.category.OpenEJB.startup", "info");
p.put("log4j.category.OpenEJB.startup.service", "warn");
p.put("log4j.category.OpenEJB.startup.config", "info");
p.put("log4j.category.OpenEJB.hsql", "info");
p.put("log4j.category.Transaction", "warn");
p.put("log4j.category.org.apache.activemq", "error");
p.put("log4j.category.org.apache.geronimo", "error");
p.put("log4j.category.openjpa", "error");
p.put("log4j.appender.C", "org.apache.log4j.ConsoleAppender");
p.put("log4j.appender.C.layout", "org.apache.log4j.SimpleLayout");

Context context = new InitialContext(p);
```

---

<a id="tomee-apache-org-tomee-10-1-docs-configuring-persistenceunits-in-tests"></a>

# Apache TomEE

```xml
<persistence>
  <persistence-unit name="movie-unit">
    <provider>org.hibernate.ejb.HibernatePersistence</provider>
    <jta-data-source>movieDatabase</jta-data-source>
    <non-jta-data-source>movieDatabaseUnmanaged</non-jta-data-source>
    <properties>
      <property name="hibernate.hbm2ddl.auto" value="create-drop"/>
      <property name="hibernate.max_fetch_depth" value="3"/>
    </properties>
  </persistence-unit>
</persistence>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-constructor-injection"></a>

# Apache TomEE

```java
@Stateless
public class WidgetBean implements Widget {

    @EJB(beanName = "FooBean")
    private final Foo foo;

    @Resource(name = "count")
    private final int count;

    @Resource
    private final DataSource ds;

    public WidgetBean(Integer count, Foo foo, DataSource ds) {
    this.count = count;
    this.foo = foo;
    this.ds = ds;
    }

    public int getCount() {
    return count;
    }

    public Foo getFoo() {
    return foo;
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-containers-and-resources"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Containers and Resources

## Containers

### CMP\_ENTITY

Declarable in `tomee.xml` via

Declarable in properties via:

```properties
Foo = new://Container?type=CMP_ENTITY
```

Supports the following properties:

| Property Name | Description |
| --- | --- |
| CmpEngineFactory | Default value isorg.apache.openejb.core.cmp.jpa.JpaCmpEngineFactory. |
| TransactionManager | Transaction manager used by the container. |

---

### BMP\_ENTITY

Declarable in `tomee.xml` via

Declarable in properties via:

```properties
Foo = new://Container?type=BMP_ENTITY
```

Supports the following properties:

| Property Name | Description |
| --- | --- |
| PoolSize | Specifies the size of the bean pools for this BMP entity container. Default value is10. |

---

### STATELESS

Declarable in `tomee.xml` via

Declarable in properties via:

```properties
Foo = new://Container?type=STATELESS
```

Supports the following properties:

| Property Name | Description |
| --- | --- |
| TimeOut | Specifies the time to wait between invocations, in milliseconds. A value of0means no timeout. Default value is0. |
| PoolSize | Specifies the size of the bean pools for this stateless SessionBean container. Default value is10. |
| StrictPooling | Controls behavior when the pool reaches its maximum size. Iftrue, requests wait for instances to become available and the pool never grows beyondPoolSize. Iffalse, temporary instances are created for one invocation and then discarded. Default value istrue. |

---

### STATEFUL

Declarable in `tomee.xml` via

Declarable in properties via:

```properties
Foo = new://Container?type=STATEFUL
```

Supports the following properties:

| Property Name | Description |
| --- | --- |
| Passivator | Responsible for writing beans to disk during passivation. Known implementations:org.apache.openejb.core.stateful.RAFPassivaterorg.apache.openejb.core.stateful.SimplePassivaterDefault value isorg.apache.openejb.core.stateful.SimplePassivater. |
| TimeOut | Specifies the time to wait between invocations, in minutes. A value of0means no timeout. Default value is20. |
| PoolSize | Specifies the size of the bean pools for this stateful SessionBean container. Default value is1000. |
| BulkPassivate | Number of instances to passivate at one time during bulk passivation. Default value is100. |

---

### MESSAGE

Declarable in `tomee.xml` via

Declarable in properties via:

```properties
Foo = new://Container?type=MESSAGE
```

Supports the following properties:

| Property Name | Description |
| --- | --- |
| ResourceAdapter | Resource adapter that delivers messages to the container. Default value isDefault JMS Resource Adapter. |
| MessageListenerInterface | Message listener interface handled by this container. Default value isjakarta.jms.MessageListener. |
| ActivationSpecClass | Activation specification class. Default value isorg.apache.activemq.ra.ActiveMQActivationSpec. |
| InstanceLimit | Maximum number of bean instances allowed per MDB deployment. Default value is10. |

---

## Resources

### javax.sql.DataSource

Declarable in `tomee.xml` via

Declarable in properties via:

```properties
Foo = new://Resource?type=javax.sql.DataSource
```

Supports the following properties:

| Property Name | Description |
| --- | --- |
| JtaManaged | Determines whether the datasource is JTA managed. Default value istrue. |
| JdbcDriver | JDBC driver class name. Default value isorg.hsqldb.jdbcDriver. |
| JdbcUrl | JDBC URL for creating connections. Default value isjdbc:hsqldb:file:data/hsqldb/hsqldb. |
| UserName | Default username. Default value issa. |
| Password | Default password. |
| ConnectionProperties | Properties passed to the JDBC driver when creating connections. Format:[propertyName=property;]*userandpasswordare passed explicitly. |
| DefaultAutoCommit | Default auto-commit state of new connections. Default value istrue. |
| DefaultReadOnly | Default read-only state of new connections. If not set,setReadOnlyis not called. |
| DefaultTransactionIsolation | Default transaction isolation level. Allowed values:NONE,READ_COMMITTED,READ_UNCOMMITTED,REPEATABLE_READ,SERIALIZABLE. |
| InitialSize | Initial number of connections created when the pool starts. Default value is0. |
| MaxActive | Maximum number of active connections. Default value is20. |
| MaxIdle | Maximum number of idle connections. Default value is20. |
| MinIdle | Minimum number of idle connections. Default value is0. |
| MaxWait | Maximum wait time (ms) for a connection before throwing an exception. Default value is-1. |
| ValidationQuery | SQL query used to validate connections. |
| TestOnBorrow | Validate connections before borrowing. Default value istrue. |
| TestOnReturn | Validate connections before returning to the pool. Default value isfalse. |
| TestWhileIdle | Validate connections during idle eviction runs. Default value isfalse. |
| TimeBetweenEvictionRunsMillis | Time between idle eviction runs (ms). Default value is-1. |
| NumTestsPerEvictionRun | Number of connections tested per eviction run. Default value is3. |
| MinEvictableIdleTimeMillis | Minimum idle time before eviction. Default value is1800000. |
| PoolPreparedStatements | Enables pooling of prepared statements. Default value isfalse. |
| MaxOpenPreparedStatements | Maximum open prepared statements. Default value is0. |
| AccessToUnderlyingConnectionAllowed | Allows access to the raw JDBC connection. Default value isfalse. |

---

### ActiveMQResourceAdapter

Declarable in `tomee.xml` via

Declarable in properties via:

```properties
Foo = new://Resource?type=ActiveMQResourceAdapter
```

Supports the following properties:

| Property Name | Description |
| --- | --- |
| BrokerXmlConfig | Broker configuration. Default value isbroker:(tcp://localhost:61616)?useJmx=false. |
| ServerUrl | Broker address. Default value isvm://localhost?async=true. |
| DataSource | Datasource for message persistence. Default value isDefault Unmanaged JDBC Database. |

---

### jakarta.jms.ConnectionFactory

Declarable in `tomee.xml` via

Declarable in properties via:

```properties
Foo = new://Resource?type=jakarta.jms.ConnectionFactory
```

Supports the following properties:

| Property Name | Description |
| --- | --- |
| ResourceAdapter | Default value isDefault JMS Resource Adapter. |
| TransactionSupport | Transaction support type:xa,local, ornone. Default value isxa. |
| PoolMaxSize | Maximum number of physical connections. Default value is10. |
| PoolMinSize | Minimum number of physical connections. Default value is0. |
| ConnectionMaxWaitMilliseconds | Maximum time to wait for a connection. Default value is5000. |
| ConnectionMaxIdleMinutes | Maximum idle time before reclaiming a connection. Default value is15. |

---

### jakarta.jms.Queue

Declarable in `tomee.xml` via

Declarable in properties via:

```properties
Foo = new://Resource?type=jakarta.jms.Queue
```

Supports the following properties:

| Property Name | Description |
| --- | --- |
| destination | Name of the queue. |

---

### jakarta.jms.Topic

Declarable in `tomee.xml` via

Declarable in properties via:

```properties
Foo = new://Resource?type=jakarta.jms.Topic
```

Supports the following properties:

| Property Name | Description |
| --- | --- |
| destination | Name of the topic. |

---

### jakarta.mail.Session

Declarable in `tomee.xml` via

Declarable in properties via:

```properties
Foo = new://Resource?type=jakarta.mail.Session
```

No properties.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-containers-and-resources--)

---

<a id="tomee-apache-org-tomee-10-1-docs-contrib-debug-debug-intellij"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

# Debugging an Apache

Stepping through the [TomEE](http://tomee.apache.org/apache-tomee.html)
source code is a must-to-follow step if you want to understand how TomEE
works and later contribute. This is a guide to quickly start your
debugging session with TomEE as a TomEE developer.

This guide assumes that:

- Linux is the OS
- IntelliJ IDEA 13.1.3 is the IDE
- Maven 3.0.5 or better is installed

## Download the Source Code

For beginners it is recommended not to start with the trunk, because it
is common to have some blockers or non-stable functionality which could
bring your learning to a halt. So first start with the latest stable
released source code. Move to trunk once you are ready to do some code
modification on TomEE.

[Click
here to download TomEE 1.7.1 Source code](http://www.apache.org/dyn/closer.cgi/tomee/tomee-1.7.1/openejb-4.7.1-source-release.zip)

## Build the Source Code

First extract the zip file named **openejb-4.7.1-source-release.zip** to
any location. Let’s assume it is your home folder.

> unzip openejb-4.7.1-source-release -d ~

The above command will create the **openejb-4.7.1** directory in your home
directory.

Even though you can do a full build, We will run the following command
to do a quick build so that you can have your meal before your hunger
kills you.

> mvn -Pquick -Dsurefire.useFile=false -DdisableXmlReport=true
> -DuniqueVersion=false -ff -Dassemble -DskipTests -DfailIfNoTests=false
> clean install

More details about building the product from the source can be found
[here](http://tomee.apache.org/dev/source-code.html).

## Deploy TomEE

The TomEE build builds several distributions (zip & war files) to cater
the different needs of different users. Here we discuss the tomee
plus distribution & TomEE war distribution only. TomEE+ is the full
feature packed distribution from TomEE.

TomEE+ zip location:

> ~/openejb-4.7.1/tomee/apache-tomee/target/apache-tomee-plus-1.7.1.zip

Unzip the zip into your home directory (or any other location)

> unzip
> ~/openejb-4.7.1/tomee/apache-tomee/target/apache-tomee-plus-1.7.1.zip -d
> ~

You will find the directory **apache-tomee-plus-1.7.1** in your home
folder. Let’s run the TomEE.

> cd ~/apache-tomee-plus-1.7.1/bin ./catalina.sh run

"INFO: Server startup in xxxx ms" is the Green light!

## Prepare your IDE

Let’s prepare our IntelliJ IDEA for the debugging session.

Start IntelliJ IDEA and Click the Import Project link

![image](idea1.png)

Select the ~/openejb-4.7.1 directory and press OK

Select import project from external model & Maven as the external model.

![image](tomee.apache.org/tomee-10.1/docs/contrib/debug/idea3.png)

Press Next on this screen.

![image](tomee.apache.org/tomee-10.1/docs/contrib/debug/idea4.png)

Select the main profile.

![image](tomee.apache.org/tomee-10.1/docs/contrib/debug/idea6.png)

Select the org.apache.openejb:openejb:4.7.1

![image](tomee.apache.org/tomee-10.1/docs/contrib/debug/idea7.png)

Select the JDK you want to use with.

![image](tomee.apache.org/tomee-10.1/docs/contrib/debug/idea8.png)

Give the project a name and press Finish.

![image](tomee.apache.org/tomee-10.1/docs/contrib/debug/idea9.png)

Now your IDE will load the project.

## First Breakpoint

Next step is to put a breakpoint at the place where the code is
triggered. Let’s understand how the code is triggered.

TomEE+ is created on top of Tomcat. TomEE registers a Tomcat Lifecycle
Listener **"org.apache.tomee.catalina.ServerListener"** on **server.xml**
file.

All the Tomcat lifecycle events i.e. before\_init, after\_init, start,
before\_stop etc…​ are received by the **lifecycleEvent** method of the
ServerListener.

The execution of TomEE code starts in this lifecycleEvent method. So the
first breakpoint should be on the lifecycleEvent method.

## Run TomEE+ in debug mode

If you simply run **catalina.sh jpda run** in the bin folder of tomee
deployment, the server starts in the debug mode, but it will quickly pass
your breakpoint before you attach your IDE to the server process.

So we set **JPDA\_SUSPEND="y"** before we start our debugging. This will
tell the server "Do not proceed until the Debugger tool is attached to
the process"

The convenient way of doing this is adding this line to catalina.sh file
right after the #!/bin/sh line.

> = !/bin/sh JPDA\_SUSPEND="y"
>
> Now to time to run TomEE+ on debug mode.

> ~/apache-tomee-plus-1.7.1/bin/catalina.sh jpda run

The terminal should hang with the message **"Listening for transport
dt\_socket at address: 8000"**

## Attach IntelliJ IDEA debugger

- Menu Bar > Run > Edit Configurations
- Press the "**+**" button in the top left corner to get the Add new
  configuration menu
- Select "Remote" from the Add new configuration menu
- Give a name (I gave "TomEE DEBUG") to this new configuration and set
  the Port to 8000
- Click OK.

![image](tomee.apache.org/tomee-10.1/docs/contrib/debug/idea10.png)

To start debugging your TomEE+

Main Menu > Run > Debug TomEE DEBUG

Congratulations! You hit the break point you put at the startup of the
TomEE code. Carry on with your debugging session to learn more.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../privacy-policy.html)

##### [Documentation](../../../../latest/docs/)

- [How to configure](../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../latest/examples/)

- [CDI Interceptor](../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../community/index.html)

- [Contributors](../../../../community/contributors.html)
- [Social](../../../../community/social.html)
- [Sources](../../../../community/sources.html)

##### [Security](../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-contrib-debug-debug-intellij--)

---

<a id="tomee-apache-org-tomee-10-1-docs-contrib-debug-jaxrs"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

# Debugging JAX-RS Services

Key classes:

- org.apache.cxf.jaxrs.model.OperationResourceInfo

Key breakpoints for deployment

- org.apache.openejb.server.rest.RESTService#afterApplicationCreated
- org.apache.openejb.server.rest.RESTService#deployApplication

Key breakpoints for runtime

- org.apache.cxf.jaxrs.utils.JAXRSUtils#findTargetMethod
  -

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../privacy-policy.html)

##### [Documentation](../../../../latest/docs/)

- [How to configure](../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../latest/examples/)

- [CDI Interceptor](../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../community/index.html)

- [Contributors](../../../../community/contributors.html)
- [Social](../../../../community/social.html)
- [Sources](../../../../community/sources.html)

##### [Security](../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-contrib-debug-jaxrs--)

---

<a id="tomee-apache-org-tomee-10-1-docs-custom-injection"></a>

# Apache TomEE

```java
@Stateless
public class Stratocaster {

    @Resource(name = "pickups")
    private List<Pickup> pickups;

    @Resource(name = "style")
    private Style style;

    @Resource(name = "dateCreated")
    private Date dateCreated;

    @Resource(name = "guitarStringGuages")
    private Map<String, Float> guitarStringGuages;

    @Resource(name = "certificateOfAuthenticity")
    private File certificateOfAuthenticity;

    public Date getDateCreated() {
        return dateCreated;
    }

    /**
     * Gets the guage of the electric guitar strings
     * used in this guitar.
     *
     * @param string
     * @return
     */
    public float getStringGuage(String string) {
        return guitarStringGuages.get(string);
    }

    public List<Pickup> getPickups() {
        return pickups;
    }

    public Style getStyle() {
        return style;
    }

    public File getCertificateOfAuthenticity() {
        return certificateOfAuthenticity;
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-datasource-config"></a>

# Apache TomEE

```xml
<Resource id="myDataSource" type="javax.sql.DataSource">
    accessToUnderlyingConnectionAllowed = false
    alternateUsernameAllowed = false
    connectionProperties =
    defaultAutoCommit = true
    defaultReadOnly =
    definition =
    ignoreDefaultValues = false
    initialSize = 0
    jdbcDriver = org.hsqldb.jdbcDriver
    jdbcUrl = jdbc:hsqldb:mem:hsqldb
    jtaManaged = true
    maxActive = 20
    maxIdle = 20
    maxOpenPreparedStatements = 0
    maxWaitTime = -1 millisecond
    minEvictableIdleTime = 30 minutes
    minIdle = 0
    numTestsPerEvictionRun = 3
    password =
    passwordCipher = PlainText
    poolPreparedStatements = false
    serviceId =
    testOnBorrow = true
    testOnReturn = false
    testWhileIdle = false
    timeBetweenEvictionRuns = -1 millisecond
    userName = sa
    validationQuery =
</Resource>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-datasource-configuration-by-creator"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# DataSource Creator

TomEE uses  `creator` to create the connection pool factory. In other
terms it means you can use any pool you want for DataSource in TomEE.

Default provided pools are DBCP (default in embedded mode) and Tomcat
JDBC (default in TomEE to be aligned on Tomcat).

Depending which one you use the accept configuration are not 100% the
same even if we try to align the most common entries to the historical
configuration (ie DBCP).

Here are a more detailed list of accepted properties by creator.

## DBCP2 (TomEE 7.x and 8.x)

Note: details are at
[http://tomee.apache.org/containers-and-resources.html](http://tomee.apache.org/containers-and-resources.html) (note:
[http://commons.apache.org/proper/commons-dbcp/configuration.html](http://commons.apache.org/proper/commons-dbcp/configuration.html) uses
the latest version of DBCP but TomEE 1.7.x is not using this version).

- AccessToUnderlyingConnectionAllowed
- ConnectionInitSqls
- ConnectionProperties
- DefaultAutoCommit
- DefaultCatalog
- DefaultReadOnly
- DefaultTransactionIsolation
- Delegate
- InitialSize
- JdbcDriver
- JdbcUrl
- LogAbandoned
- LogWriter
- LoginTimeout
- MaxActive
- MaxIdle
- MaxOpenPreparedStatements
- MaxWait
- MinEvictableIdleTimeMillis
- MinIdle
- Name
- NumTestsPerEvictionRun
- Password
- PasswordCipher
- PoolPreparedStatements
- RemoveAbandoned
- RemoveAbandonedTimeout
- TestOnBorrow
- TestOnReturn
- TestWhileIdle
- TimeBetweenEvictionRunsMillis
- UserName
- ValidationQuery
- ValidationQueryTimeout

## Tomcat JDBC

Note: details are at [https://tomcat.apache.org/tomcat-11.0-doc/jdbc-pool.html](https://tomcat.apache.org/tomcat-11.0-doc/jdbc-pool.html)

- AbandonWhenPercentageFull
- AccessToUnderlyingConnectionAllowed
- AlternateUsernameAllowed
- CommitOnReturn
- ConnectionProperties
- DataSource
- DataSourceJNDI
- DbProperties
- DefaultAutoCommit
- DefaultCatalog
- DefaultReadOnly
- DefaultTransactionIsolation
- DriverClassName
- FairQueue
- IgnoreExceptionOnPreLoad
- InitSQL
- InitialSize
- JdbcInterceptors
- JmxEnabled
- LogAbandoned
- LogValidationErrors
- LogWriter
- LoginTimeout
- MaxActive
- MaxAge
- MaxIdle
- MaxWait
- MinEvictableIdleTimeMillis
- MinIdle
- Name
- NumTestsPerEvictionRun
- Password
- PasswordCipher
- PoolProperties
- PropagateInterruptState
- RemoveAbandoned
- RemoveAbandonedTimeout
- RollbackOnReturn
- SuspectTimeout
- TestOnBorrow
- TestOnConnect
- TestOnReturn
- TestWhileIdle
- TimeBetweenEvictionRunsMillis
- Url
- UseDisposableConnectionFacade
- UseEquals
- UseLock
- Username
- ValidationInterval
- ValidationQuery
- ValidationQueryTimeout
- Validator
- ValidatorClassName

## DBCP2 (TomEE 7.x)

Note: details are at
[http://commons.apache.org/proper/commons-dbcp/configuration.html](http://commons.apache.org/proper/commons-dbcp/configuration.html)

- AccessToUnderlyingConnectionAllowed
- ConnectionInitSqls
- ConnectionProperties
- DefaultAutoCommit
- DefaultCatalog
- DefaultReadOnly
- DefaultTransactionIsolation
- Delegate
- InitialSize
- JdbcDriver
- JdbcUrl
- LogAbandoned
- LogWriter
- LoginTimeout
- MaxTotal
- MaxIdle
- MaxOpenPreparedStatements
- MaxWait
- MinEvictableIdleTimeMillis
- MinIdle
- Name
- NumTestsPerEvictionRun
- Password
- PasswordCipher
- PoolPreparedStatements
- RemoveAbandonedOnBorrow
- RemoveAbandonedOnMaintenance
- RemoveAbandonedTimeout
- TestOnBorrow
- TestOnReturn
- TestWhileIdle
- TimeBetweenEvictionRunsMillis
- UserName
- ValidationQuery
- ValidationQueryTimeout

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-datasource-configuration-by-creator--)

---

<a id="tomee-apache-org-tomee-10-1-docs-datasource-password-encryption"></a>

# Apache TomEE

```xml
<Resource id="MySQL Database" type="DataSource">
    #  MySQL example
    #
    #  This connector will not work until you download the driver at:
    #  http://www.mysql.com/downloads/api-jdbc-stable.html

    JdbcDriver  com.mysql.jdbc.Driver
    JdbcUrl jdbc:mysql://localhost/test
    UserName    test
    Password    Passw0rd
</Resource>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-deamon-lin-service"></a>

# Apache TomEE

```properties
[Unit]
Description=Apache TomEE
After=network.target

[Service]
User=<user to run as>
Type=forking
Environment=JAVA_HOME=/usr/lib/jvm/jre
Environment=CATALINA_PID=/opt/tomee/temp/tomee.pid
Environment=CATALINA_HOME=/opt/tomee
Environment=CATALINA_BASE=/opt/tomee
Environment=CATALINA_OPTS='-server'
Environment=JAVA_OPTS='-Djava.awt.headless=true'
ExecStart=/opt/tomee/bin/startup.sh
ExecStop=/opt/tomee/bin/shutdown.sh
KillSignal=SIGCONT

[Install]
WantedBy=multi-user.target
```

---

<a id="tomee-apache-org-tomee-10-1-docs-deamon-win-service"></a>

# Apache TomEE

```text
C:\Java\apache-tomee-plus-10.1.4\bin>service install TomEE-DEV
Installing the service 'TomEE-DEV' ...
Using CATALINA_HOME:    "C:\Java\apache-tomee-plus-10.1.4"
Using CATALINA_BASE:    "C:\Java\apache-tomee-plus-10.1.4"
Using JAVA_HOME:        "C:\Java\jdk-11.0.30+7
Using JRE_HOME:         "C:\Java\jdk-11.0.30+7"
Using JVM:              "C:\Java\jdk-11.0.30+7"\bin\server\jvm.dll"
Using Service User:     ""
Installed, will now configure TomEE
The service 'TomEE-DEV' has been installed.

C:\Java\apache-tomee-plus-10.1.4\bin>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-declaring-references"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Declaring References

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-declaring-references--)

---

<a id="tomee-apache-org-tomee-10-1-docs-deploy-tool"></a>

# Apache TomEE

```properties
Application deployed successfully at {0}
App(id=C:\samples\Calculator-new\hello-addservice.jar)
    EjbJar(id=hello-addservice.jar, path=C:\samples\Calculator-new\hello-addservice.jar)
    Ejb(ejb-name=HelloBean, id=HelloBean)
        Jndi(name=HelloBean)
        Jndi(name=HelloBeanLocal)

    Ejb(ejb-name=AddServiceBean, id=AddServiceBean)
        Jndi(name=AddServiceBean)
        Jndi(name=AddServiceBeanLocal)
```

---

<a id="tomee-apache-org-tomee-10-1-docs-deploying-in-tomee"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# null

|  | Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at .http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. |
| --- | --- |

# Deploying in TomEE

Deploying applications in TomEE is as simple as deploying them in
Tomcat.

You could deploy your application in Eclipse just like how you would
deploy with Tomcat. For an example,
[tomee-and-eclipse](#tomee-apache-org-tomee-10-1-docs-tomee-and-eclipse) shows how to use TomEE
with Eclipse.

Or you can simply package your application as a standard **WAR** file and
copy it to the **[TomEE]/webapps** folder, or as an **EAR** file and copy it
to the **[TomEE]/apps** folder.

Read on to learn more about packaging EJBs in a WAR file.

## Packaging

### One archive

The basic idea of this approach is that your Servlets and EJBs are
together in your WAR file as one application.

- No classloader boundaries between Servlets and EJBs
- EJBs and Servlets can share all third-party libraries (like Spring!)
- No EAR required.
- Can put the `web.xml` and `ejb-jar.xml` in the same archive (the WAR file)
- EJBs can see Servlet classes and vice versa

### Not quite J2EE (But it is Java EE 6)

This is very different from J2EE or Java EE 5 as there are not several
levels of separation and classloader hierarchy any more.  
This may take some time getting used to, and it is important to understand
that this style of packaging is **not** J2EE compliant.  
You should not worry though, as it is an accepted feature of Java EE 6.

### J2EE classloading rules:

- You cannot ever have EJBs and Servlets in the same classloader.
- Three classloader minimum; a classloader for the ear, one for each
  ejb-jar, and one for each WAR file.
- Servlets can see EJBs, but EJBs cannot see Servlets.

To pull that off, J2EE has to kill you on packaging: - You cannot have
EJB classes and Servlet classes in the same archive.

- You need at least three archives to combine Servlets and EJBs; 1 EAR
  containing 1 EJB jar and 1 Servlet WAR.
- Shared libraries must go in the EAR and be included in a specially
  formatted 'Class-Path' entry in the EAR’s MANIFEST file.

Critically speaking, forcing more than one classloader on an application
is where J2EE "jumps the shark" for a large majority of people’s needs.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-deploying-in-tomee--)

---

<a id="tomee-apache-org-tomee-10-1-docs-deployment-id"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Deployment ID

# What is a Deployment ID?

Every bean deployed in OpenEJB has a unique deployment-id that
identifies it within the scope of the entire container system. The
server and container system refer beans at run-time using the bean’s
deployment id.

## Like ejb-name

This deployment id is much like the element of the `ejb-jar.xml`, with
one very important difference. The is only required to be unique within
the scope of the `ejb-jar.xml` in the bean’s jar. The deployment id is
required to be unique across all beans and jars in OpenEJB. This is a
subtle, but important, distinction.

Remember that the EJB specification was designed so that enterprise
beans could be created, packaged, and sold by vendors (EJB Providers).
Furthermore, users should be able to buy a packaged set of beans (a jar
with an `ejb-jar.xml` in it) and deploy it into an EJB Container without
modification.

## The ejb-name is not unique

Let’s consider this, what happens if two vendors each sell a package
(jar) that contains a bean with the PurchaseOrder? Both are completely
different in terms functionality and are different beans in every other
respect. The EJB spec says, this is fine, ejb-names only have to unique
within the jar and that jar’s `ejb-jar.xml` file. It’s ridiculous to
expect EJB Providers to call each other up and ask, "Are you already
using the name 'PurchaseOrder' in your jar?" Remember that the EJB
specification was designed so that enterprise beans could be created,
packaged, and sold by vendors (EJB Providers). Furthermore, users should
be able to buy a packaged set of beans (a jar with an `ejb-jar.xml` in it)
and deploy it into an EJB Container without modification. This is all
fine and dandy, but it still leaves it up to the EJB Container/Server
providers to settle the difference.

## The deployment-id is unique

OpenEJB solves this with the OpenEJB-specific deployment id. By
requiring that each bean deployed into OpenEJB has a unique name, we can
guarantee that we are always referring to the right bean at all times.
Furthermore, it allows you to deploy different versions of the same
package several times in the same container system, each time giving the
beans new deployment ids.

## Using ejb-name as deployment-id anyway

If you’re lazy — as any truly great programmer should be — and don’t
want to type a deployment id for each bean every time you deploy a jar,
you can use the -D option of the Deploy Tool. This will throw caution to
the wind, and automatically assign the bean’s ejb-name as the value of
the bean’s OpenEJB deployment id. This leaves up to you to guarantee
that bean’s ejb-name will be unique across all beans and jars in the
container system. In other words, be very careful with the -D option!

# How is it used?

## In the container system

In the container system, the deployment id is used to index the bean in
a system-wide registry. This registry is refereed to on every call made
in the container system. Being able to safely hash and cache bean
information by id is a must. This stresses the importance of unique ids
for every bean deployed in OpenEJB.

## In the Local Server

The Local (IntraVM) Server is an integral part of the container system
and the two are, in many ways, inseparable. The Local Server takes care
of all bean to bean and client to bean invocations made inside the
virtual machine. For this reason, it often referred to as the IntraVM
Server.

For bean to bean communications, the Local Server must create a JNDI
namespace (JNDI ENC) for each bean as defined by the bean’s , , and
elements of the bean’s `ejb-jar.xml` file. Every bean literally gets its
very own JNDI namespace. When a bean makes a JNDI call, the Local Server
intercepts this call and uses the deployment id of the calling bean to
retrieve that bean’s private JNDI namespace from the container system’s
index. The Local Server then carries out the lookup on that bean’s
namespace.

All non-bean clients share one big global namespace. Since non-bean
clients are not deployed and do not have a deployment descriptor like an
`ejb-jar.xml`, the Local Server is unable to taylor a namespace for each
non-bean client as it can for bean clients. The Local server cannot
identify non-bean clients as they have no deployment id. All JNDI calls
made by clients that the Local Server cannot identify go to the public,
global namespace. The public, global JNDI namespace contains all beans
and resources in the container system. name.

Each bean is added to the public, global namespace using its deployment
id as its JNDI lookup. For example, if a bean had a deployment-id of
"/my/bean/foo", a non-bean client could lookup that bean as follows.

```java
...
Object bean = initialContext.lookup("/my/bean/Foo");
...
```

If a bean in the container system made the above JNDI call, the Local
Server would see the bean’s identity (deployment id) hidden in the
Thread, go get the bean’s private JNDI namespace and finish the lookup
on that. Since all names in bean’s JNDI namespace are required start
with "java:comp/env", the lookup would fail and the bean would receive a
javax.naming.NameNotFoundException.

In short…​

For beans:
- Each bean has its own private, personalized JNDI namespace
- The names in it are the same names it uses in its `ejb-jar.xml`
- Beans can only access their private namespace, period

For non-beans (everyone else): - Non-bean clients share the public,
global JNDI namespace - The names in it are the deployment ids of all
the beans - Non-bean clients can only access the one global namespace

## In the Remote Server

The Remote Server has a public, global namespace just as the Local
Server does. The difference being that the Remote Server only serves
clients outside the container system and outside the virtual machine.
So, all clients from the perspective of the Remote Server are non-bean
clients. As a result, the Remote Server only has the one public, global
JNDI namespace. Just as in the Local Server, the names in this namespace
consist of the deployment ids of the beans in the container system.

Just as before, clients can look up beans from the Remote Server using
the bean’s deployment id. For example, if a bean had a deployment-id of
"/my/bean/foo", a client could lookup that bean as follows.

```java
...
Object bean = initialContext.lookup("/my/bean/Foo");
...
```

# What happens if there is a duplicate deployment ID?

The deployment ID uniquely identifies the bean in the OpenEJB container
system. Therefore, no two beans can share the same deployment ID.

If a bean attempts to use a deployment ID that is already in use by
another bean, the second bean and all beans in its jar will not be
loaded. In addition, the system will log a warning like the following
one asking you to redeploy the jar and choose a different deployment ID
for the bean.

```properties
WARN : Jar C:\openejb\beans\fooEjbs.jar cannot be loaded.  The Deployment ID "/my/bean/foo" is already in use.  Please redeploy this jar and assign a different deployment ID to the bean with the ejb-name "FooBean".
```

For example, the acmeEjbs.jar contains a bean with the ejb-name
"DaffyDuckBean". The disneyEjbs.jar contains a bean with the
ejb-name "DonaldDuckBean".

We deploy the acmeEjbs.jar and give the "DaffyDuckBean" the deployment
ID of "/my/favorite/duck". Sometime afterward, we deploy the
disneyEjbs.jar and assign the "DonaldDuckBean" the deployment ID
"/my/favorite/duck", having forgotten that we already gave that unique
ID to the "DaffyDuckBean" in the acmeEjbs.jar.

When the container system is started, the system will begin loading all
the beans one jar at a time. It will first load the acmeEjbs.jar and
index each bean by deployment ID. But, when the system reaches the
disneyEjbs.jar, it will discover that it cannot index the
"DonaldDuckBean" using the deployment ID "/my/favorite/duck" because
that index is already taken.

The system cannot load the "DonaldDuckBean" and must also ignore the
rest of the beans in the disneyEjbs.jar as they may need the
"DonaldDuckBean" bean to function properly. The disneyEjbs.jar is
skipped and the following warning is logged.

```properties
WARN : Jar C:\openejb\beans\disneyEjbs.jar cannot be loaded.  The  Deployment ID "/my/favorite/duck" is already in use.  Please redeploy  this jar and assign a different deployment ID to the bean with the ejb-name "DonaldDuckBean".
```

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-deployment-id--)

---

<a id="tomee-apache-org-tomee-10-1-docs-deployments"></a>

# Apache TomEE

```xml
<tomee>
...
<Deployments jar="c:\my\app\superEjbs.jar" />
<Deployments jar="c:\someplace\purchasing.jar" />
<Deployments jar="timeTrack.jar" />
</tomee>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-details-on-openejb-jar"></a>

# Apache TomEE

```xml
<?xml version="1.0"?>
<openejb-jar xmlns="http://www.openejb.org/openejb-jar/1.1">
    <ejb-deployment  ejb-name="Hello"
         deployment-id="Hello"
         container-id="Default Stateless Container"/>
</openejb-jar>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-classloading-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[ Download as PDF](../../../../tomee-10.1/docs/developer/classloading/index.pdf)

# The TomEE ClassLoader

TomEE ClassLoading is directly mapped to Tomcat one.

- JVM

  - common.loader

    - shared.loader

      - webapp1
      - webapp2
      - application1

        - earwebapp1
        - earwebapp2

Click on the tree (JVM) on the left to see the detail there.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../privacy-policy.html)

##### [Documentation](../../../../latest/docs/)

- [How to configure](../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../latest/examples/)

- [CDI Interceptor](../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../community/index.html)

- [Contributors](../../../../community/contributors.html)
- [Social](../../../../community/social.html)
- [Sources](../../../../community/sources.html)

##### [Security](../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-developer-classloading-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-configuration-cxf"></a>

# Apache TomEE

```xml
<?xml version="1.0" encoding="UTF-8"?>
<openejb-jar>
 <pojo-deployment class-name="jaxrs-application">
   <properties>
     # here will go the config
   </properties>
 </pojo-deployment>
</openejb-jar>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-ide-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[ Download as PDF](../../../../tomee-10.1/docs/developer/ide/index.pdf)

# Integrated Development Environments (IDEs)

TomEE is supported by the main IDEs in the market:

- [Eclipse](https://eclipse.org/downloads/)
- [IntelliJ IDEA](https://www.jetbrains.com/idea/download/)
- [Netbeans](https://netbeans.org/downloads/)

## Eclipse

Eclipse is an integrated development environment used in computer programming, and is the most widely used Java IDE. It contains a base workspace and an extensible plug-in system for customizing the environment. [Wikipedia](https://en.wikipedia.org/wiki/Eclipse_(software))

## IntelliJ IDEA

IntelliJ IDEA is a Java integrated development environment for developing computer software. It is developed by JetBrains, and is available as an Apache 2 Licensed community edition, and in a proprietary commercial edition. Both can be used for commercial development. [Wikipedia](https://en.wikipedia.org/wiki/IntelliJ_IDEA)

## Netbeans

NetBeans is an integrated development environment for Java. NetBeans allows applications to be developed from a set of modular software components called modules. NetBeans runs on Microsoft Windows, macOS, Linux and Solaris. [Wikipedia](https://en.wikipedia.org/wiki/NetBeans)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../privacy-policy.html)

##### [Documentation](../../../../latest/docs/)

- [How to configure](../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../latest/examples/)

- [CDI Interceptor](../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../community/index.html)

- [Contributors](../../../../community/contributors.html)
- [Social](../../../../community/social.html)
- [Sources](../../../../community/sources.html)

##### [Security](../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-developer-ide-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[ Download as PDF](../../../tomee-10.1/docs/developer/index.pdf)

# Developer

Click [here](#tomee-apache-org-tomee-10-1-docs-docs) to find documentation for developers.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-developer-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-json-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[ Download as PDF](../../../../tomee-10.1/docs/developer/json/index.pdf)

# TomEE and Apache Johnzon - JAX-RS JSON Provider

Since TomEE 7.0, TomEE comes with Apache Johnzon.
It means you can use JSON-P out of the box but also Johnzon Mapper
which is the default JAX-RS provider for JSON.

**IMPORTANT** - this is a breaking change with 1.x which was using jettison.
This last one was relying on JAXB model to generate JSON which often led
to unexpected JSON tree and some unexpected escaping too.

## Getting started with Johnzon Mapper

[http://johnzon.apache.org/](http://johnzon.apache.org/) will get more information than this quick
getting started but here are the basics of the mapping with Johnzon.

The mapper uses a direct java to json representation.

For instance this java bean:

```java
public class MyModel {
  private int id;
  private String name;

  // getters/setters
}
```

will be mapped to:

```json
{
  "id": 1234,
  "name": "Johnzon doc"
}
```

Note that Johnzon supports several customization either directly on the MapperBuilder of through annotations.

### @JohnzonIgnore

@JohnzonIgnore is used to ignore a field. You can optionally say you ignore the field until some version
if the mapper has a version:

```java
public class MyModel {
  @JohnzonIgnore
  private String name;

  // getters/setters
}
```

Or to support name for version 3, 4, …​ but ignore it for 1 and 2:

```java
public class MyModel {
  @JohnzonIgnore(minVersion = 3)
  private String name;

  // getters/setters
}
```

### @JohnzonConverter

Converters are used for advanced mapping between java and json.

There are several converter types:

1. Converter: map java to json and the opposite based on the string representation
2. Adapter: a converter not limited to String
3. ObjectConverter.Reader: to converter from json to java at low level
4. ObjectConverter.Writer: to converter from java to json at low level
5. ObjectConverter.Codec: a Reader and Writer

The most common is to customize date format but they all take. For that simple case we often use a Converter:

```java
public class LocalDateConverter implements Converter<LocalDate> {
    @Override
    public String toString(final LocalDate instance) {
        return instance.toString();
    }

    @Override
    public LocalDate fromString(final String text) {
        return LocalDate.parse(text);
    }
}
```

If you need a more advanced use case and modify the structure of the json (wrapping the value for instance)
you will likely need Reader/Writer or a Codec.

Then once your converter developed you can either register globally on the MapperBuilder or simply decorate
the field you want to convert with `@JohnzonConverter`:

```java
public class MyModel {
  @JohnzonConverter(LocalDateConverter.class)
  private LocalDate date;

  // getters/setters
}
```

### @JohnzonProperty

Sometimes the json name is not java friendly (\_foo or foo-bar or even 200 for instance). For that cases
@JohnzonProperty allows to customize the name used:

```java
public class MyModel {
  @JohnzonProperty("__date")
  private LocalDate date;

  // getters/setters
}
```

### AccessMode

On MapperBuilder you have several AccessMode available by default but you can also create your own one.

The default available names are:

- field: to use fields model and ignore getters/setters
- method: use getters/setters (means if you have a getter but no setter you will serialize the property but not read it)
- strict-method (default based on Pojo convention): same as method but getters for collections are not used to write

You can use these names with setAccessModeName().

### Your own mapper

Since johnzon is in tomee libraries you can use it yourself (if you use maven/gradle set johnzon-mapper as provided):

```java
final MySuperObject object = createObject();

final Mapper mapper = new MapperBuilder().build();
mapper.writeObject(object, outputStream);

final MySuperObject otherObject = mapper.readObject(inputStream, MySuperObject.class);
```

## Johnzon and JAX-RS

TomEE uses by default Johnzon as JAX-RS provider for versions 7.x. If you want however to customize it you need to follow this procedure:

1. Create a WEB-INF/openejb-jar.xml:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<openejb-jar>
 <pojo-deployment class-name="jaxrs-application">
   <properties>
     # optional but requires to skip scanned providers if set to true
     cxf.jaxrs.skip-provider-scanning = true
     # list of providers we want
     cxf.jaxrs.providers = johnzon,org.apache.openejb.server.cxf.rs.EJBAccessExceptionMapper
   </properties>
 </pojo-deployment>
</openejb-jar>
```

1. Create a WEB-INF/resources.xml to define johnzon service which will be use to instantiate the provider

```xml
<?xml version="1.0" encoding="UTF-8"?>
<resources>
 <Service id="johnzon" class-name="org.apache.johnzon.jaxrs.ConfigurableJohnzonProvider">
   # 1M
   maxSize = 1048576
   bufferSize = 1048576

   # ordered attributes
   attributeOrder = $order

   # Additional types to ignore
   ignores = org.apache.cxf.jaxrs.ext.multipart.MultipartBody
 </Service>

 <Service id="order" class-name="com.company.MyAttributeSorter" />

</resources>
```

Note: as you can see you mainly just need to define a service with the id johnzon (same as in openejb-jar.xml)
and you can reference other instances using $id for services and `@id` for resources.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../privacy-policy.html)

##### [Documentation](../../../../latest/docs/)

- [How to configure](../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../latest/examples/)

- [CDI Interceptor](../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../community/index.html)

- [Contributors](../../../../community/contributors.html)
- [Social](../../../../community/social.html)
- [Sources](../../../../community/sources.html)

##### [Security](../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-developer-json-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-migration-tomee-1-to-7"></a>

# Apache TomEE

```java
@Provider
@Consumes("application/json")
@Produces("application/json")
public class MyAppJsonProvider extends JacksonJsonProvider {
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-testing-applicationcomposer-index"></a>

# Apache TomEE

```xml
<dependency>
  <groupId>org.apache.tomee</groupId>
  <artifactId>openejb-core</artifactId>
  <version>${openejb.version></version>
</dependency>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-testing-arquillian-index"></a>

# Apache TomEE

```xml
<dependency>
  <groupId>org.apache.tomee</groupId>
  <artifactId>arquillian-openejb-embedded</artifactId>
  <version>${tomee7.version}
</dependency>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-testing-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[ Download as PDF](../../../../tomee-10.1/docs/developer/testing/index.pdf)

# Unit Testing

- [ApplicationComposer](#tomee-apache-org-tomee-10-1-docs-developer-testing-applicationcomposer-index): Lightweight tests
- [Arquillian](#tomee-apache-org-tomee-10-1-docs-developer-testing-arquillian-index): The standard for EE tests
- [Going further](#tomee-apache-org-tomee-10-1-docs-developer-testing-other-index): OpenEJB JUnit, TomEE Embedded…​

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../privacy-policy.html)

##### [Documentation](../../../../latest/docs/)

- [How to configure](../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../latest/examples/)

- [CDI Interceptor](../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../community/index.html)

- [Contributors](../../../../community/contributors.html)
- [Social](../../../../community/social.html)
- [Sources](../../../../community/sources.html)

##### [Security](../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-developer-testing-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-testing-other-index"></a>

# Apache TomEE

```java
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import jakarta.ejb.embeddable.EJBContainer;
import jakarta.inject.Inject;
import javax.naming.NamingException;

import static org.junit.Assert.assertTrue;

public class ATest {
    @Inject
    private MyCDIBean aBean;

    @PersistenceContext
    private EntityManager em;

    @Resource
    private DataSource ds;

    @BeforeClass
    public static void start() throws NamingException {
        container = EJBContainer.createEJBContainer();
    }

    @AfterClass
    public static void shutdown() {
        if (container != null) {
            container.close();
        }
    }

    @Before
    public void inject() throws NamingException {
        container.getContext().bind("inject", this);
    }

    @After
    public void reset() throws NamingException {
        container.getContext().unbind("inject");
    }

    @Test
    public void aTest() {
        // ...
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-tools-gradle-plugins"></a>

# Apache TomEE

```java
buildscript {
   repositories {
       mavenCentral()
   }

   dependencies {
       classpath 'org.apache.tomee.gradle:tomee-embedded:7.0.0'
   }
}

apply plugin: 'org.apache.tomee.tomee-embedded'

// ...
```

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-tools-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[ Download as PDF](../../../../tomee-10.1/docs/developer/tools/index.pdf)

# Build Tools and Plugins

- [Maven Plugins](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-plugins)
- [Gradle Plugin](#tomee-apache-org-tomee-10-1-docs-developer-tools-gradle-plugins)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../privacy-policy.html)

##### [Documentation](../../../../latest/docs/)

- [How to configure](../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../latest/examples/)

- [CDI Interceptor](../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../community/index.html)

- [Contributors](../../../../community/contributors.html)
- [Social](../../../../community/social.html)
- [Sources](../../../../community/sources.html)

##### [Security](../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-developer-tools-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-tools-maven-plugins"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[ Download as PDF](../../../../tomee-10.1/docs/developer/tools/maven-plugins.pdf)

# TomEE Maven Plugins - Introduction

TomEE provides several maven plugins:

- one for a [standalone TomEE](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-tomee)
- one for [TomEE embedded](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-embedded)
- one for [application composer](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-applicationcomposer) based applications
- Note: there is one for `EJBContainer` but this one is easily replaced by one of the previous in general

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../privacy-policy.html)

##### [Documentation](../../../../latest/docs/)

- [How to configure](../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../latest/examples/)

- [CDI Interceptor](../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../community/index.html)

- [Contributors](../../../../community/contributors.html)
- [Social](../../../../community/social.html)
- [Sources](../../../../community/sources.html)

##### [Security](../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../docs.html)
- [Community](../../../../community/index.html)
- [Security](../../../../security/security.html)
- [Downloads](../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-plugins--)

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-tools-maven-applicationcomposer"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../../docs.html)
- [Community](../../../../../community/index.html)
- [Security](../../../../../security/security.html)
- [Downloads](../../../../../download.html)

[ Download as PDF](../../../../../tomee-10.1/docs/developer/tools/maven/applicationcomposer.pdf)

# Application Composer Maven Plugin

This plugin has two goal:

- `applicationcomposer:run`: to start the application from mvn command line
- `applicationcomposer:zip`: to package a zip with dependencies and start scripts

|  | the dependencies are retrieved withMavenProject.getArtifacts()which means you artifacts should be awar- maven doesn’t populate it with ajar- and the compile phase - at least - should be passed to ensure it is populated. |
| --- | --- |

## Run goal configuration

```bash
mvn process-classes applicationcomposer:run -DskipTests
```

| Name | Default | Description |
| --- | --- | --- |
| args | - | a list of application arguments |
| application | - | application qualified name |
| binaries | ${project.build.outputDirectory} | where is your module code (target/classes) |
| mavenLog | true | force to use maven logging in openejb |

## Zip goal configuration

```bash
mvn process-classes applicationcomposer:zip -DskipTests
```

| Name | Default | Description |
| --- | --- | --- |
| workDir | ${project.build.directory}/${project.build.finalName}-applicationcomposer | where the container can "work" and create temp files |
| zip | ${project.build.directory}/${project.build.finalName}-applicationcomposer.zip | where to create the zip |
| attach | true | attach the created artifact |
| classifier | - | artifact classifier if needed |
| application | - | application qualified name |
| binaries | ${project.build.outputDirectory} | where is your module code (target/classes) |

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../../privacy-policy.html)

##### [Documentation](../../../../../latest/docs/)

- [How to configure](../../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../../latest/examples/)

- [CDI Interceptor](../../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../../community/index.html)

- [Contributors](../../../../../community/contributors.html)
- [Social](../../../../../community/social.html)
- [Sources](../../../../../community/sources.html)

##### [Security](../../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../../docs.html)
- [Community](../../../../../community/index.html)
- [Security](../../../../../security/security.html)
- [Downloads](../../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-applicationcomposer--)

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-tools-maven-embedded"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../../docs.html)
- [Community](../../../../../community/index.html)
- [Security](../../../../../security/security.html)
- [Downloads](../../../../../download.html)

[ Download as PDF](../../../../../tomee-10.1/docs/developer/tools/maven/embedded.pdf)

# TomEE Embedded Maven Plugin

TomEE Embedded Maven plugin has a single goal: `tomee-embedded:run`.

## Configuration

| Name | Default | Description |
| --- | --- | --- |
| warFile | ${project.build.directory}/${project.build.finalName} | where is the binary |
| httpPort | 8080 | HTTP port |
| httpsPort | 8443 | HTTPS port |
| ajpPort | 8009 | AJP port |
| stopPort | 8005 | shutdown port |
| host | localhost | the server host |
| dir | ${project.build.directory}/apache-tomee-embedded | the work directory |
| keystoreFile | - | the keystore file for the HTTPS connector |
| keystorePass | - | the keystore password for the HTTPS connector |
| keystoreType | JKS | the keystore type for the HTTPS connector |
| clientAuth | - | should HTTPS use client authentication |
| keyAlias | - | the key to use for HTTPS |
| sslProtocol | - | the protocol to use for SSL/HTTPS |
| serverXml | - | a custom server.xml |
| ssl | false | is HTTPS active |
| withEjbRemote | false | is EJBd active |
| quickSession | true | is sessions using Random instead of SecureRandom to generate id (faster but less secure, good for dev purposes) |
| skipHttp | false | don’t activate HTTP connector (allow to have only HTTPS for instance) |
| classpathAsWar | false | deploy the classpath instead of the binary/war |
| useProjectClasspath | true | in previous case use the project classpath and not plugin one |
| webResourceCached | true | should web resources be cached |
| modules | ${project.build.outputDirectory} | list of module to add to the classpath of the application |
| docBase | ${project.basedir}/src/main/webapp | where is the docBase in classpath deployment mode (where are web resources) |
| context | - | which context to use for the main artifact/deployment |
| containerProperties | - | map of container properties |
| mavenLog | true | should the plugin use maven logger instead of JUL |
| keepServerXmlAsThis | false | don’t apply port/host configuration to the server.xml if provided |
| users | - | map of user/password |
| roles | - | map of role/users |
| forceJspDevelopment | true | ensure JSP are in development mode (updated) |
| applications | - | list of applications to deploy |
| applicationScopes | - | scope of the artifact to take into account for the classpath (ignore PROVIDED for instance) |
| skipCurrentProject | - | don’t deploy current project but only configured applications |
| applicationCopyFolder | - | a folder containing applications |
| workDir | - | tomee embedded work dir |
| inlinedServerXml | - | server.xml content directly in the pom |
| inlinedTomEEXml | - | tomee.xml content directly in the pom |
| liveReload | - | livereload configuration if activated. This is an object containing these options: {watchedFolder: 'src/main/webapp', path: '/', port: 35729} |
| withLiveReload | false | activate livereload for web resources |

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../../privacy-policy.html)

##### [Documentation](../../../../../latest/docs/)

- [How to configure](../../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../../latest/examples/)

- [CDI Interceptor](../../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../../community/index.html)

- [Contributors](../../../../../community/contributors.html)
- [Social](../../../../../community/social.html)
- [Sources](../../../../../community/sources.html)

##### [Security](../../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../../docs.html)
- [Community](../../../../../community/index.html)
- [Security](../../../../../security/security.html)
- [Downloads](../../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-embedded--)

---

<a id="tomee-apache-org-tomee-10-1-docs-developer-tools-maven-tomee"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../../../docs.html)
- [Community](../../../../../community/index.html)
- [Security](../../../../../security/security.html)
- [Downloads](../../../../../download.html)

[ Download as PDF](../../../../../tomee-10.1/docs/developer/tools/maven/tomee.pdf)

# TomEE Maven Plugin - Goals & Configuration

TomEE Maven Plugin is a set of goals for the development and to prepare to go in production:

- `tomee:build`
- `tomee:exec`
- `tomee:configtest`
- `tomee:debug`
- `tomee:deploy`
- `tomee:exec`
- `tomee:list`
- `tomee:run`
- `tomee:start`
- `tomee:stop`
- `tomee:undeploy`

## Run

The most commonly used goal, it allows to start a tomee with applications. Here is its configuration:

| Name | Default | Description |
| --- | --- | --- |
| synchronization | - | a synchronization (see after the table) |
| synchronizations | - | list of synchronizations |
| reloadOnUpdate | - | should the application be redeployed when a synchronization is triggered |
| skipCurrentProject | false | should the current project not be considered as a deployable even if its packaging is compatible (war typically) |
| tomeeVersion | auto, plugin one | which version of TomEE to use |
| tomeeGroupId | org.apache.tomee | TomEE artifact groupId |
| tomeeArtifactId | apache-tomee | TomEE artifact artifactId |
| tomeeType | zip | the type of the TomEE artifact , only zip supported at the moment |
| tomeeClassifier | webprofile | which flavor of TomEE to use (classifier) |
| tomeeShutdownPort | read from server.xml | the shutdown port |
| tomeeShutdownAttempts | 60 | how many times to wait for startup/shutdown (waits 1s in between) |
| tomeeShutdownCommand | SHUTDOWN | the shutdown command |
| tomeeAjpPort | read from the pom | the AJP port if needed |
| tomeeHttpsPort | read from the pom | the HTTPS port if needed |
| args | - | command line arguments (system properties, javaagent, JVM options …​) |
| debug | - | start and wait for a remote debugger to connect |
| debugPort | 5005 | used when debug to change the default port |
| simpleLog | false | use one line logs |
| extractWars | false | explode wars before starting |
| stripWarVersion | true | remove the version from the war name |
| stripVersion | false | remove the version from the artifact name whatever it is (even jar) |
| webappResources | ${project.basedir}/src/main/webapp | where web resources are |
| webappClasses and classes | ${project.build.outputDirectory} | where artifact binaries are |
| catalinaBase | ${project.build.directory}/apache-tomee | where to create the tomee instance |
| context | - | name of the current artifact (rename the war from the maven name to this one) |
| webappDir | webapps | path to webapps folder from tomee.base |
| appDir | apps | path to apps folder from tomee.base |
| libDir | lib | where is lib folder |
| mainDir | ${project.basedir}/src/main | used in openejb mode to change default config of conf/lib/bin folders to openejb instead of tomee |
| config | ${project.basedir}/src/main/tomee/conf | a conf folder synchronized with TomEE one |
| bin | ${project.basedir}/src/main/tomee/bin | a bin folder synchronized with TomEE one |
| lib | ${project.basedir}/src/main/tomee/lib | a lib folder synchronized with TomEE one |
| systemVariables | - | a map of system properties |
| classpaths | - | a list of additional entries for the startup classpath |
| customizers | - | a list of customizers |
| jsCustomizers | - | a list of js customizers (js scripts) |
| groovyCustomizers | - | a list of groovy customizers (groovy scripts) |
| webappDefaultConfig | false | auto config war oriented |
| quickSession | true | session generation will useRandominstead ofSecureRandom(for dev) |
| forceReloadable | false | ensure TomEE supports reloading/redeployment |
| forceJspDevelopment | true | JSP will be auto-recompiled on changes |
| libs | - | dependencies to add in lib, see after this table for advanced usage |
| endorsedLibs | - | dependencies to add in endorsed, see after this table for advanced usage |
| javaagents | - | javaagents to add on the JVM, supports maven coordinates |
| persistJavaagents | false | should javaagent be saved or just use for this plugin run |
| webapps | - | additional applications to deploy |
| warFile | ${project.build.directory}/${project.build.finalName}.${project.packaging} | the war to deploy |
| workWarFile | ${project.build.directory}/${project.build.finalName}" | the exploded war to deploy |
| removeDefaultWebapps | true | should default webapps (ROOT, docs, …​) be deleted |
| deployOpenEjbApplication | false | should openejb internal application be deployed |
| removeTomeeWebapp | true | should tomee webapp (with EJBd adapter) be deployed |
| tomeeAlreadyInstalled | false | skip all the setup configuration |
| ejbRemote | true | should EJBd be activated |
| checkStarted | false | should the plugin check the server is up (useful when used withpre-integrationphase |
| checkStartedAttempts | 60 | only active, ifcheckStartedis set totrue. Specifies amount of connection attempts (active waiting) for the server to be up |
| useConsole | true | wait for the end of the execution reading inputs from the console (likequitcommand) |
| useOpenEJB | false | use openejb-standalone instead of tomee |
| inlinedServerXml | - | a server.xml content in pom.xml directly |
| inlinedTomEEXml | - | a tomee.xml content in pom.xml directly |
| overrideOnUnzip | true | if when unzipping tomee a file is already there should it be overridden |
| skipRootFolderOnUnzip | true | ignore root folder of the zip |
| keystore | - | path to keystore for HTTPS connector |

Synchronization are blocks defining a source and target folder and both are synchronized. It typically copies
`src/main/webapp` resources in `target/apache-tomee/webapps/myapp/`.

### Customizers

Customizers are java classes loadable by the plugin and with a main or implementing `Runnable` and taking optionally
as constructor parameter a `File` representing `tomee.base` or no arguments.

They are executed when creating the TomEE instance.

There are two scripting flavors of that: js and groovy. Both will have some contextual variables:

- catalinaBase: tomee base path
- resolver: a maven resolver to get a dependency using maven. For instance: `resolver.resolve('group', 'artfact', 'version', 'type')`

### Dependencies (libs)

The format can be:

- a maven dependency:

```properties
groupId:artifactId:version
```

- a zip dependency and extracted in lib folder:

```properties
unzip:groupId:artifactId:version
```

- a matching prefix to remove:

```properties
remove:prefix
```

### Example

```xml
<plugin>
  <groupId>org.apache.tomee.maven</groupId>
  <artifactId>tomee-maven-plugin</artifactId>
  <version>${TOMEE_VERSION}</version>
  <configuration>
    <tomeeClassifier>plus</tomeeClassifier>
    <debug>false</debug>
    <debugPort>5005</debugPort>
    <args>-Dfoo=bar</args>
    <config>${project.basedir}/src/test/tomee/conf</config>
    <libs>
      <lib>mysql:mysql-connector-java:5.1.20</lib>
    </libs>
    <webapps>
       <webapp>org.superbiz:myapp:4.3?name=ROOT</webapp>
       <webapp>org.superbiz:api:1.1</webapp>
    </webapps>
    <apps>
        <app>org.superbiz:mybugapp:3.2:ear</app>
    </apps>
    <libs>
        <lib>mysql:mysql-connector-java:5.1.21</lib>
        <lib>unzip:org.superbiz:hibernate-bundle:4.1.0.Final:zip</lib>
        <lib>remove:openjpa-</lib>
    </libs>
  </configuration>
</plugin>
```

## Build

Excepted synchronization, build plugin inherit from `run` Mojo its configuration. It just adds the following:

| Name | Default | Description |
| --- | --- | --- |
| formats | - | map of configuration, keys are format (zip, tar.gz) and value the target location |
| zip | true | create a zip from the configured instance |
| attach | true | attach created artifacts |
| skipArchiveRootFolder | false | don’t add a root folder in the zip |

## Tomcat like goals

`configtest`, `start` and `stop` just execute these commands on the server (like on `catalina.sh`).

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../../../privacy-policy.html)

##### [Documentation](../../../../../latest/docs/)

- [How to configure](../../../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../../../latest/docs/admin/file-layout.html)
- [Testing](../../../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../../../latest/examples/)

- [CDI Interceptor](../../../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../../../latest/examples/rest-cdi.html)
- [EJB](../../../../../latest/examples/ejb-examples.html)
- [JSF](../../../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../../../community/index.html)

- [Contributors](../../../../../community/contributors.html)
- [Social](../../../../../community/social.html)
- [Sources](../../../../../community/sources.html)

##### [Security](../../../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../../../docs.html)
- [Community](../../../../../community/index.html)
- [Security](../../../../../security/security.html)
- [Downloads](../../../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-developer-tools-maven-tomee--)

---

<a id="tomee-apache-org-tomee-10-1-docs-docs"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[ Download as PDF](../../tomee-10.1/docs/docs.pdf)

# TomEE Documentation

## Administration

- [Server Configuration](#tomee-apache-org-tomee-10-1-docs-admin-configuration-index)
- [Directory Structure](#tomee-apache-org-tomee-10-1-docs-admin-file-layout)
- [Clustering and High Availability (HA)](#tomee-apache-org-tomee-10-1-docs-admin-cluster-index)

## Developers

- [IDEs - Eclipse, Intellij Idea and Netbeans](#tomee-apache-org-tomee-10-1-docs-developer-ide-index)
- [Unit Testing - Arquillian, OpenEJB JUnit, TomEE Embedded and ApplicationComposer](#tomee-apache-org-tomee-10-1-docs-developer-testing-index)
- [Build Tools and Plugins](#tomee-apache-org-tomee-10-1-docs-developer-tools-index)
- [Migrating From TomEE 1.x to 7.x](#tomee-apache-org-tomee-10-1-docs-developer-migration-tomee-1-to-7)
- [TomEE and Apache Johnzon - JAX-RS JSON Provider](#tomee-apache-org-tomee-10-1-docs-developer-json-index)
- [Apache CXF Configuration - JAX-RS (RESTful Services) and JAX-WS (Web Services)](#tomee-apache-org-tomee-10-1-docs-developer-configuration-cxf)
- [Understanding the TomEE ClassLoader](#tomee-apache-org-tomee-10-1-docs-developer-classloading-index)

## Advanced

- [`ApplicationComposer` with JBatch](#tomee-apache-org-tomee-10-1-docs-advanced-applicationcomposer-index)
- [How to set up TomEE in Production](#tomee-apache-org-tomee-10-1-docs-advanced-setup-index)
- [Fat / Uber Jar Deployment using the Maven Shade Plugin](#tomee-apache-org-tomee-10-1-docs-advanced-shading-index)
- [Java Naming and Directory Interface (JNDI)](#tomee-apache-org-tomee-10-1-docs-advanced-client-jndi)
- [Why is my ActiveMQ/JMS MDB not scaling as expected?](#tomee-apache-org-tomee-10-1-docs-advanced-jms-jms-configuration)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-docs--)

---

<a id="tomee-apache-org-tomee-10-1-docs-documentation"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Documentation

See also the [examples page](examples-trunk/index.html) for
downloadable, executable and code-focused view of Java EE and TomEE. You
can also find us on IRC freenode.org #openejb and #tomee

## IDE

- [Get started with Intellij](#tomee-apache-org-tomee-10-1-docs-tomee-and-intellij)
- [Debugging in Intellij](#tomee-apache-org-tomee-10-1-docs-contrib-debug-debug-intellij)
- [Get started with Eclipse (WTP)](#tomee-apache-org-tomee-10-1-docs-tomee-and-eclipse)
- [Get started with Eclipse
  (m2e-Webby)](getting-started-with-eclipse-and-webby.html)
- [Get started with Netbeans](#tomee-apache-org-tomee-10-1-docs-tomee-and-netbeans)

## General Information

- [Comparison](#tomee-apache-org-tomee-10-1-docs-comparison)
- [TomEE Directory structure](#tomee-apache-org-tomee-10-1-docs-tomee-directory-structure)
- [Deploying in TomEE](#tomee-apache-org-tomee-10-1-docs-deploying-in-tomee)
- [The 'tomee' webapp](#tomee-apache-org-tomee-10-1-docs-tomee-webapp)
- [TomEE Reference Card](refcard/refcard.html)
- [ApplicationComposer](#tomee-apache-org-tomee-10-1-docs-application-composer-index)

## Configuration

- [System Properties](#tomee-apache-org-tomee-10-1-docs-system-properties)
- [Deployments](#tomee-apache-org-tomee-10-1-docs-deployments)
- [Configuring Resources](#tomee-apache-org-tomee-10-1-docs-configuring-in-tomee)
- [Configuring DataSources](#tomee-apache-org-tomee-10-1-docs-configuring-datasources)
- [Containers and Resources](#tomee-apache-org-tomee-10-1-docs-containers-and-resources)
- [JMS Resources and MDB Container](#tomee-apache-org-tomee-10-1-docs-jms-resources-and-mdb-container)
- [Configuring JavaMail](#tomee-apache-org-tomee-10-1-docs-configuring-javamail)
- [TomEE Security](#tomee-apache-org-tomee-10-1-docs-tomee-and-security)
- [Security How To](#tomee-apache-org-tomee-10-1-docs-security)
- [EJB Clients](#tomee-apache-org-tomee-10-1-docs-clients)
- [EJB over SSL](#tomee-apache-org-tomee-10-1-docs-ejb-over-ssl)
- [JNDI Names](#tomee-apache-org-tomee-10-1-docs-jndi-names)
- [Changing JMS implementations](#tomee-apache-org-tomee-10-1-docs-changing-jms-implementations)
- [Changing JPA to Hibernate](#tomee-apache-org-tomee-10-1-docs-tomee-and-hibernate)

## Testing Techniques

- [Application discovery
  via the classpath](#tomee-apache-org-tomee-10-1-docs-application-discovery-via-the-classpath)
- [Embedded Configuration](#tomee-apache-org-tomee-10-1-docs-embedded-configuration)
- [Configuring DataSources in
  Tests](#tomee-apache-org-tomee-10-1-docs-configuring-datasources-in-tests)
- [Configuring
  PersistenceUnits in Tests](#tomee-apache-org-tomee-10-1-docs-configuring-persistenceunits-in-tests)
- [Configuring Containers in
  Tests](#tomee-apache-org-tomee-10-1-docs-configuring-containers-in-tests)
- [Configuring Logging in Tests](#tomee-apache-org-tomee-10-1-docs-configuring-logging-in-tests)
- [Alternate Descriptors](#tomee-apache-org-tomee-10-1-docs-alternate-descriptors)
- [Unit Testing Transactions](unit-testing-transactions.html)
- [TestCase with TestBean
  inner-class](testcase-with-testbean-inner-class.html)
- [TestCase Injection (`@LocalClient`)](#tomee-apache-org-tomee-10-1-docs-local-client-injection)

## Discovery and Failover

- [Overview](#tomee-apache-org-tomee-10-1-docs-ejb-failover)
- [Multicast Discovery (UDP)](#tomee-apache-org-tomee-10-1-docs-multicast-discovery)
- [Multipulse Discovery (UDP)](#tomee-apache-org-tomee-10-1-docs-multipulse-discovery)
- [Multipoint Discovery (TCP)](#tomee-apache-org-tomee-10-1-docs-multipoint-discovery)
- [Multipoint Considerations](#tomee-apache-org-tomee-10-1-docs-multipoint-considerations)
- [Multipoint Recommendations](#tomee-apache-org-tomee-10-1-docs-multipoint-recommendations)
- [Logging Events](#tomee-apache-org-tomee-10-1-docs-failover-logging)

## OpenEJB Standalone Server

- [Understanding the Directory
  Layout](#tomee-apache-org-tomee-10-1-docs-understanding-the-directory-layout)
- [Startup](#tomee-apache-org-tomee-10-1-docs-startup)
- [Deploy Tool](#tomee-apache-org-tomee-10-1-docs-deploy-tool)
- [Properties Tool](#tomee-apache-org-tomee-10-1-docs-properties-tool)

## Spring

- [Spring and OpenEJB 3.0](#tomee-apache-org-tomee-10-1-docs-spring-and-openejb-3-0)
- [Spring and OpenEJB 3.1 and later](#tomee-apache-org-tomee-10-1-docs-spring)
- [Spring, EJB and JPA example](#tomee-apache-org-tomee-10-1-docs-spring-ejb-and-jpa)

## Arquillian

- [Arquillian Primer - What you need
  to know](#tomee-apache-org-tomee-10-1-docs-arquillian-getting-started)
- [Using the TomEE Arquillian
  adapters](#tomee-apache-org-tomee-10-1-docs-arquillian-available-adapters)

## TomEE Maven Plugin

- [Getting started](#tomee-apache-org-tomee-10-1-docs-tomee-mp-getting-started)
- [tomee-maven-plugin reference documentation](maven/index.html)
- [tomee-embedded-maven-plugin
  reference documentation](#tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin)
- [TomEE simple webapp archetype
  documentation](#tomee-apache-org-tomee-10-1-docs-tomee-mp-getting-started)

## Tips and Tricks

- [Install TomEE using the drop-in
  WAR](#tomee-apache-org-tomee-10-1-docs-installation-drop-in-war)
- [Global Concurrency Management](#tomee-apache-org-tomee-10-1-docs-tip-concurrency)
- [WebLogic Lookup](#tomee-apache-org-tomee-10-1-docs-tip-weblogic)
- [Jersey Client](#tomee-apache-org-tomee-10-1-docs-tip-jersey-client)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-documentation--)

---

<a id="tomee-apache-org-tomee-10-1-docs-dynamic-datasource"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Dynamic Datasource

# OpenEJB dynamic datasource

## Goal

The openejb dynamic datasource api aims to allow to use multiple data
sources as one.

It can be useful for technical reasons (load balancing for example) or
functional reasons (filtering, aggregation, enriching…​).

## The API

The interface Router (*org.apache.openejb.resource.jdbc.Router*) have
only one method to get the datasource to use:

```java
Router.getDataSource()
```

The *org.apache.openejb.resource.jdbc.RoutedDataSource* wraps a
classical data source. It has to be used to declare your datasource.

You can implement all the policy you want in your Router implementation.

A class called *org.apache.openejb.resource.jdbc.AbstractRouter* is
available to ease router development.

## Known limitation(s)

You have to use the same kind of databases (same version, same
configuration…​).

All database have to be created when you use the router. The way to do
it automatically can depend on your JPA provider.

### OpenJPA

OpenJPA initializes its database when the entitymanager is called for
the first time so you need to initialize all your proxied datasource
before using the other one. It can be done using an Init EJB doing a
find() on each proxied datasource.

### Hibernate

Hibernate initializes the database when it starts so if you declare a
persistence unit by database all databases will be initialized at the
start-up.

## Example

### The story (the unit test example)

You want to use only one datasource in the code but you have a criteria
to set to choose the real database to use between three.

So in your code you want something like:

```java
public class RoutedEJBBean {
    @PersistenceContext(unitName = "router")
    private EntityManager em;

    // this router is not automatic, we
    // need it to select the database to use
    @Resource(name = "My Router")
    private DeterminedRouter router;

    public void persist(int id, String name, String clientDatasource) {
        router.setDataSource(clientDatasource);
        em.persist(new Person(id, name));
    }
}
```

## The router implementation

The router will simply manage a map to store proxied datasources and a
field to store the datasource used in the current thread (ThreadLocal).

```java
public class DeterminedRouter implements Router {
    private String dataSourceNames; // used to store configuration (openejb.xml)
    private String defaultDataSourceName; // defautl data source name
    private Map<String, DataSource> dataSources = null; // proxied data sources
    private ThreadLocal<DataSource> currentDataSource = new ThreadLocal<DataSource>(); // the datasource to use or null

    /**
     * @param datasourceList datasource resource name, separator is a space
     */
    public void setDataSourceNames(String datasourceList) {
        dataSourceNames = datasourceList;
    }

    /**
     * lookup datasource in openejb resources
     */
    private void init() { // looking up datasources declared as proxied
        dataSources = new ConcurrentHashMap<String, DataSource>();
        for (String ds : dataSourceNames.split(" ")) {
            ContainerSystem containerSystem = SystemInstance.get().getComponent(ContainerSystem.class);

            Object o = null;
            Context ctx = containerSystem.getJNDIContext();
            try {
                o = ctx.lookup("openejb:Resource/" + ds);
                if (o instanceof DataSource) {
                    dataSources.put(ds, (DataSource) o);
                }
            } catch (NamingException ignore) {
            }
        }
    }

    /**
     * @return the user selected data source if it is set
     *         or the default one
     *  @throws IllegalArgumentException if the data source is not found
     */
    public DataSource getDataSource() {
        // lazy init of routed datasources
        if (dataSources == null) {
            init();
        }

        // if no datasource is selected use the default one
        if (currentDataSource.get() == null) {
            if (dataSources.containsKey(defaultDataSourceName)) {
                return dataSources.get(defaultDataSourceName);

            } else {
                throw new IllegalArgumentException("you have to specify at least one datasource");
            }
        }

        // the developper set the datasource to use
        return currentDataSource.get();
    }

    /**
     *
     * @param datasourceName data source name
     */
    public void setDataSource(String datasourceName) {
        if (dataSources == null) {
            init();
        }
        if (!dataSources.containsKey(datasourceName)) {
            throw new IllegalArgumentException("data source called " + datasourceName + " can't be found.");
        }
        DataSource ds = dataSources.get(datasourceName);
        currentDataSource.set(ds);
    }

    public void setDefaultDataSourceName(String name) {
        this.defaultDataSourceName = name;
    }
}
```

## Creation of the service provider for the router

To be able to use your router add a file called service-jar.xml under
META-INF/. For example META-INF/org.router.

This file will contain something like:

```xml
<ServiceJar>
  <ServiceProvider id="DeterminedRouter" service="Resource"
           type="org.apache.openejb.resource.jdbc.Router" class-name="implementation class">
    Param defaultValue
    ParamWithNoDefaultValue
  </ServiceProvider>
</ServiceJar>
```

## openejb.xml

In the openejb.xml file, you have to declare your dynamic database and
in our example it needs the proxied datasources too:

```xml
<Resource id="router" type="<your implementation>" provider="<your provider>">
  Param value
</Resource>

<Resource id="route db" type="DataSource" provider="RoutedDataSource">
  Router router
</Resource>

<!–- real databases – for our example -->
<Resource id="db1" type="DataSource">
  JdbcDriver org.hsqldb.jdbcDriver
  JdbcUrl jdbc:hsqldb:mem:db1
  UserName sa
  Password
  JtaManaged true
</Resource>
<Resource id="db2" type="DataSource">
  JdbcDriver org.hsqldb.jdbcDriver
  JdbcUrl jdbc:hsqldb:mem:db2
  UserName sa
  Password
  JtaManaged true
</Resource>
<Resource id="db3" type="DataSource">
  JdbcDriver org.hsqldb.jdbcDriver
  JdbcUrl jdbc:hsqldb:mem:db3
  UserName sa
  Password
  JtaManaged true
</Resource>
```

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-dynamic-datasource--)

---

<a id="tomee-apache-org-tomee-10-1-docs-eclipse-plugin"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Eclipse Plugin

# What is it?

The *OpenEJB Eclipse Plugin* will be a suite of tools made available via
Eclipse to make EJB development with OpenEJB easier. The initial
offering will probably provide basic functionality by taking advantage
of [WebTools](http://www.eclipse.org/webtools) to allow for OpenEJB to be
an available container/runtime within Eclipse. This means full debugging
and Eclipse project integration. From there, the sky is the limit so
feel free to suggest features on the [OpenEJB Dev list|Mailing
Lists#MailingLists-DeveloperMailingList] .

# How to get involved?

Just the same as getting involved with any part of OpenEJB — send a
mail to the
[OpenEJB Dev
list](mailing-lists#mailinglists-developermailinglist.html) and say "Hi!" We’re a very relaxed group so no need to be perfect
or overly prepared. Just dive right in, we’re always happy to have more.

# Where do I get it?

The initiative is just launching, but you can grab what we have right
here and start hacking.

[http://svn.apache.org/repos/asf/tomee/openejb-eclipse-plugin/trunk/](http://svn.apache.org/repos/asf/tomee/openejb-eclipse-plugin/trunk/)

# What do I need to help?

[Eclipse](http://www.eclipse.org) [Eclipse
Web Tools](http://www.eclipse.org/webtools)

# Resources

[EclipseCon
Presentation for Extending WTP](http://eclipsezilla.eclipsecon.org/show_bug.cgi?id=3581)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-eclipse-plugin--)

---

<a id="tomee-apache-org-tomee-10-1-docs-ejb-failover"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# EJB Client/Server Failover

OpenEJB supports stateless failover. Specifically, the ability for an
EJB client to failover from one server to the next if a request cannot
be completed. No application state information is communicated between
the servers, so this functionality should be used only with applications
that are inherently stateless. A common term for this sort of setup is a
server farm.

The basic design assumption is that all servers in the same group have
the same applications deployed and are capable of doing the same job.
Servers can be brought online and offline while clients are running. As
members join/leave this information is sent to the client as part of
normal EJB request/response communication so active clients always have
the most current information on servers that can process their request
should communication with a particular server fail.

## Client Behavior

On each request to the server, the client will send the version number
associated with the list of servers in the cluster it is aware of.
Initially this version will be zero and the list will be empty. Only
when the server sees the client has an old list will the server send the
updated list. This is an important distinction as the list is not
transmitted back and forth on every request, only on change. If the
membership of the cluster is stable there is essentially no clustering
overhead to the protocol — 8 byte overhead to each request and 1 byte
on each response — so you will *not* see an exponential slowdown in
response times the more members are added to the cluster. This new list
takes affect for all proxies that share the same connection.

When a server shuts down, more connections are refused, existing
connections not in mid-request are closed, any remaining connections are
closed immediately after completion of the request in progress and
clients can failover gracefully to the next server in the list. If a
server crashes requests are retried on the next server in the list (or
depending on the `ConnectionStrategy`). This failover pattern is
followed until there are no more servers in the list at which point the
client attempts a final multicast search (if it was created with a
`PROVIDER_URL` starting with `multicast://`) before abandoning the
request and throwing an exception to the caller.

By default, the failover is ordered but random selection is supported.
The multicast discovery aspect of the client adds a nice randomness to
the selection of the first server.

## Discovery

Each discoverable service has a URI which is broadcast as a heartbeat to
other servers in the cluster. This URI advertises the service’s type,
its cluster group, and its location in the format of
'group:jbake-date: 2018-12-05
:jbake-type:location'. Say for example
"cluster1:ejb:ejbd://thehost:4201". The URI is sent out repeatedly in a
pulse and its presence on the network indicates its availability and its
absence indicates the service is no longer available.

The sending of this pulse (the heartbeat) can be done via UDP or TCP:
multicast and "multipoint" respectively. More on that in the following
section. The rate at which the heartbeat is pulsed to the network can be
specified via the 'heart\_rate' property. The default is 500
milliseconds. This rate is also used when listening for services on the
network. If a service goes missing for the duration of 'heart\_rate'
multiplied by 'max\_missed\_heartbeats', then the service is considered
dead.

The 'group' property, cluster1 in the example, is used to dissect the
servers on the network into smaller logical clusters. A given server
will broadcast all it’s services with the group prefixed in the URI, as
well it will ignore any services it sees broadcast if they do not share
the same group name.

# Details

Multicast

- [Multicast UDP Discovery](#tomee-apache-org-tomee-10-1-docs-multicast-discovery)
- [Multipulse UDP Discovery](#tomee-apache-org-tomee-10-1-docs-multipulse-discovery)

Multipoint

- [Multipoint TCP Discovery](#tomee-apache-org-tomee-10-1-docs-multipoint-discovery)
- [Considerations](#tomee-apache-org-tomee-10-1-docs-multipoint-considerations)
- [Recommendations](#tomee-apache-org-tomee-10-1-docs-multipoint-recommendations)

Logging

- [Failover Logging Events](#tomee-apache-org-tomee-10-1-docs-failover-logging)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-ejb-failover--)

---

<a id="tomee-apache-org-tomee-10-1-docs-ejb-local-ref"></a>

# Apache TomEE

```java
package org.superbiz.refs;

import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import javax.naming.InitialContext;

@Stateless
@EJB(name = "myFooEjb", beanInterface = FooLocal.class)
public class MyEjbLocalRefBean implements MyBeanInterface {

    @EJB
    private BarLocal myBarEjb;

    public void someBusinessMethod() throws Exception {
        if (myBarEjb == null) throw new NullPointerException("myBarEjb not injected");

        // Both can be looked up from JNDI as well
        InitialContext context = new InitialContext();
        FooLocal fooLocal = (FooLocal) context.lookup("java:comp/env/myFooEjb");
        BarLocal barLocal = (BarLocal) context.lookup("java:comp/env/org.superbiz.refs.MyEjbLocalRefBean/myBarEjb");
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-ejb-over-ssl"></a>

# Apache TomEE

```java
Properties p = new Properties();
p.put("java.naming.factory.initial", "org.apache.openejb.client.RemoteInitialContextFactory");
p.put("java.naming.provider.url", "https://127.0.0.1:8443/tomee/ejb");
// user and pass optional
p.put("java.naming.security.principal", "myuser");
p.put("java.naming.security.credentials", "mypass");

InitialContext ctx = new InitialContext(p);

MyBean myBean = (MyBean) ctx.lookup("MyBeanRemote");
```

---

<a id="tomee-apache-org-tomee-10-1-docs-ejb-ref"></a>

# Apache TomEE

```java
package org.superbiz.refs;

import jakarta.ejb.EJB;
import jakarta.ejb.Stateless;
import javax.naming.InitialContext;

@Stateless
@EJB(name = "myFooEjb", beanInterface = FooRemote.class)
public class MyEjbRemoteRefBean implements MyBeanInterface {

    @EJB
    private BarRemote myBarEjb;

    public void someBusinessMethod() throws Exception {
        if (myBarEjb == null) throw new NullPointerException("myBarEjb not injected");

        // Both can be looked up from JNDI as well
        InitialContext context = new InitialContext();
        FooRemote fooRemote = (FooRemote) context.lookup("java:comp/env/myFooEjb");
        BarRemote barRemote = (BarRemote) context.lookup("java:comp/env/org.superbiz.refs.MyEjbRemoteRefBean/myBarEjb");
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-ejb-refs"></a>

# Apache TomEE

```java
package com.foo.colors;

import jakarta.ejb.Stateless;

@Stateless
public class OrangeBean implements OrangeRemote {
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-ejb-request-logging"></a>

# Apache TomEE

```java
final long time = System.nanoTime() - start;
final String message = String.format("Invocation %sns - %s - Request(%s) - Response(%s)", time, conn.getURI(), req, res);
logger.log(Level.FINEST, message);
```

---

<a id="tomee-apache-org-tomee-10-1-docs-ejbd-transport"></a>

# Apache TomEE

```xml
<servlet>
    <servlet-name>ServerServlet</servlet-name>
    <servlet-class>org.apache.openejb.server.httpd.ServerServlet</servlet-class>
</servlet>

<servlet-mapping>
    <servlet-name>ServerServlet</servlet-name>
    <url-pattern>/ejb/*</url-pattern>
</servlet-mapping>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-embedded-and-remotable"></a>

# Apache TomEE

```java
-------------------------------------------------------
 T E S T S
-------------------------------------------------------
Running org.superbiz.telephone.TelephoneTest
Apache OpenEJB 3.0    build: 20080408-04:13
http://tomee.apache.org/
INFO - openejb.home =
```

---

<a id="tomee-apache-org-tomee-10-1-docs-embedded-configuration"></a>

# Apache TomEE

```java
Properties p = new Properties();

// set the initial context factory
p.put("java.naming.factory.initial ", "org.apache.openejb.client.LocalInitialContextFactory");

// change some logging
p.put("log4j.category.OpenEJB.options ", " debug");
p.put("log4j.category.OpenEJB.startup ", " debug");
p.put("log4j.category.OpenEJB.startup.config ", " debug");

// create some resources
p.put("movieDatabase", "new://Resource?type=DataSource");
p.put("movieDatabase.JdbcDriver ", " org.hsqldb.jdbcDriver");
p.put("movieDatabase.JdbcUrl ", " jdbc:hsqldb:mem:moviedb");

// override properties on your "movie-unit" persistence unit
p.put("movie-unit.hibernate.dialect ", "org.hibernate.dialect.HSQLDialect");

// set some openejb flags
p.put("openejb.jndiname.format ", " {ejbName}/{interfaceClass}");
p.put("openejb.descriptors.output ", " true");
p.put("openejb.validation.output.level ", " verbose");

InitialContext initialContext = new InitialContext(p);
```

---

<a id="tomee-apache-org-tomee-10-1-docs-embedding"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Embedding

The basic process for embedding OpenEJB:

1. Add the OpenEJB libraries to your classpath
2. Ensure your EJB modules are discoverable
3. Use the LocalInitialContextFactory to boot OpenEJB

## Important docs

- [Application
  discovery via the classpath](#tomee-apache-org-tomee-10-1-docs-application-discovery-via-the-classpath)
- [Embedded Configuration](#tomee-apache-org-tomee-10-1-docs-embedded-configuration)
- [Configuring DataSources in
  Tests](#tomee-apache-org-tomee-10-1-docs-configuring-datasources-in-tests)
- [Configuring
  PersistenceUnits in Tests](#tomee-apache-org-tomee-10-1-docs-configuring-persistenceunits-in-tests)
- [Configuring Containers in
  Tests](#tomee-apache-org-tomee-10-1-docs-configuring-containers-in-tests)
- [Configuring Logging in Tests](#tomee-apache-org-tomee-10-1-docs-configuring-logging-in-tests)
- [Alternate Descriptors](#tomee-apache-org-tomee-10-1-docs-alternate-descriptors)
- [Unit Testing Transactions](unit-testing-transactions.html)
- [TestCase with TestBean
  inner-class](testcase-with-testbean-inner-class.html)
- [TestCase Injection (@LocalClient)](local-client-injection.html)

## Examples

\{include:Examples Table}

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-embedding--)

---

<a id="tomee-apache-org-tomee-10-1-docs-failover-logging"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# null

Setting the following logging category to "debug" will open up some new
logging information.

```properties
log4j.category.OpenEJB.server.discovery = debug
```

Or more specifically as:

```properties
log4j.category.OpenEJB.server.discovery.multipoint = debug
log4j.category.OpenEJB.server.discovery.multicast = debug
```

The nature of the debug output is to display all configuration
information at startup:

```properties
DEBUG - Using default 'heart_rate=500'
DEBUG - Using default 'max_missed_heartbeats=10'
DEBUG - Using default 'max_reconnect_delay=30000'
DEBUG - Using default 'reconnect_delay=5000'
DEBUG - Using default 'exponential_backoff=0'
DEBUG - Using default 'max_reconnect_attempts=10'
INFO - Created Tracker{group='default', groupPrefix='default:', heartRate=500, maxMissedHeartbeats=10, reconnectDelay=5000, maxReconnectDelay=30000, maxReconnectAttempts=10, exponentialBackoff=0, useExponentialBackOff=false, registeredServices=0, discoveredServices=0}
```

Changing the configuration should reflect in the logging as follows:

```properties
INFO - Using 'heart_rate=200'
INFO - Using 'max_missed_heartbeats=2'
DEBUG - Using default 'max_reconnect_delay=30000'
DEBUG - Using default 'reconnect_delay=5000'
DEBUG - Using default 'exponential_backoff=0'
DEBUG - Using default 'max_reconnect_attempts=10'
INFO - Created Tracker{group='default', groupPrefix='default:', heartRate=200, maxMissedHeartbeats=2, reconnectDelay=5000, maxReconnectDelay=30000, maxReconnectAttempts=10, exponentialBackoff=0, useExponentialBackOff=false, registeredServices=0, discoveredServices=0}
```

As well as any events at runtime:

```properties
DEBUG - Expired Service{uri=green://localhost:0, broadcastString='default:green://localhost:0&#39;} Timeout{lastSeen=-5005, threshold=5000}

DEBUG - Added Service{uri=green://localhost:0}

DEBUG - Removed Service{uri=green://localhost:0}
```

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-failover-logging--)

---

<a id="tomee-apache-org-tomee-10-1-docs-faq"></a>

# Apache TomEE

```java
   protected void setUp() throws Exception {
       Properties properties = new Properties();
       properties.setProperty(Context.INITIAL_CONTEXT_FACTORY,
```

---

<a id="tomee-apache-org-tomee-10-1-docs-functional-testing-with-openejb-jetty-and-selenium"></a>

# Apache TomEE

```java
public class EmbeddedServer {
    private static EmbeddedServer instance = new EmbeddedServer();
    private Server server;

    private EmbeddedServer() {
        try {
            // initialize OpenEJB & add some test data
            Properties properties = new Properties();
            properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.client.LocalInitialContextFactory");
            InitialContext ic = new InitialContext(properties);
            PeopleFacade facade = (PeopleFacade) ic.lookup("PeopleFacadeEJBRemote");
            new TestFixture(facade).addTestData();

            // setup web app
            WebAppContext context = new WebAppContext();
            context.setWar(computeWarPath());
            InitialContext initialContext = setupJndi(context);

            // start the server
            context.setServletHandler(new EmbeddedServerServletHandler(initialContext));
            context.setContextPath("/");
            server = new Server(9091);
            server.addHandler(context);

            server.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private InitialContext setupJndi(WebAppContext context) throws NamingException {
        // setup local JNDI
        InitialContext initialContext = new InitialContext();
        WebApp webApp = getWebApp(context);
        Collection<EjbRef> refs = webApp.getEjbRef();
        for (EjbRef ref : refs) {
            String ejbLink = ref.getEjbLink();

            // get enterprise bean info
            EnterpriseBeanInfo beanInfo = new EJBHelper().getEJBInfo(ejbLink);
            if (beanInfo.jndiNames != null && beanInfo.jndiNames.size() > 0) {
            String jndiName = "java:openejb/ejb/" + beanInfo.jndiNames.get(0);
            initialContext.bind("java:comp/env/" + ref.getEjbRefName(), new LinkRef(jndiName));
            }
        }
        return initialContext;
    }

    private String computeWarPath() {
        String currentPath = new File(".").getAbsolutePath();
        String warPath;

            String[]  pathParts = currentPath.split("(\\\\|/)+");

        int webPart = Arrays.asList(pathParts).indexOf("PersonWEB");
        if (webPart == -1) {
            warPath = "PersonWEB/src/main/webapp";
        } else {
            StringBuffer buffer = new StringBuffer();

            for (int i = 0; i < webPart; i++) {
                    buffer.append(pathParts[i]);
            buffer.append(File.separator);
            }

            buffer.append("PersonWEB/src/main/webapp");
            warPath = buffer.toString();
        }
        return warPath;
    }

    public static EmbeddedServer getInstance() {
        return instance;
    }

    public Server getServer() {
        return server;
    }

    public static void main(String[]  args) {
        try {
            EmbeddedServer.getInstance().getServer().join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private WebApp getWebApp(WebAppContext context) {
        WebApp webApp = null;

        try {
            FileInputStream is = new FileInputStream(new File(context.getWar() + "/WEB-INF/web.xml").getAbsolutePath());
            webApp = (WebApp) JaxbJavaee.unmarshal(WebApp.class, is);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return webApp;
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-generating-ejb-3-annotations"></a>

# Apache TomEE

```xml
  <dependencies>
    ...
    <dependency>
      <groupId>org.apache.tomee</groupId>
      <artifactId>jakartaee-api</artifactId>
      <version>9.xxxx</version>
      <scope>provided</scope>
    </dependency>
  </dependencies>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-getting-started"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Getting Started

## The following instructions are written using Eclipse 3.2.

We will refer to the installation location of OpenEJB as OPENEJB\_HOME

Here are some basic steps you need to perform to get started with
OpenEJB 1. Download and install OpenEJB 1. Setup your development
environment 1. Write an EJB 1. Write an EJB client 1. Start the server
1. Deploy the EJB 1. Run the client 1. Stop the server

## 1. Download and Install OpenEJB

Follow
these [instructions](http://cwiki.apache.org/confluence/display/OPENEJB/Quickstart)

## 2. Setup your development environment

### Eclipse

- Open eclipse and create a new java project. Name it EJBProject
- Add the following jars to the build path of your project — OPENEJB\_HOME/lib/geronimo-ejb\_3.0\_spec-1.0.jar
- Now create another project named EJBClient. This is where we will
  write a test client
- Add the following jars to the build path of this project — OPENEJB\_HOME/lib/openejb-client-3.0.0-SNAPSHOT.jar
- Add the EJBProject to the classpath of the EJBClient project

## 3. Start the Server

Open the command prompt and run the following command:

```java
d:\openejb-3.0.0-SNAPSHOT\bin\openejb start
```

You will get the following message on the console:

```java
D:\openejb-3.0.0-SNAPSHOT>bin\openejb start
Apache OpenEJB 3.0.0-SNAPSHOT    build: 20070830-07:53
http://tomee.apache.org/
OpenEJB ready.
[OPENEJB:init]
 OpenEJB Remote Server
      ** Starting Services **
      NAME             IP          PORT
      httpejbd         0.0.0.0         4204
      admin thread         0.0.0.0         4200
      ejbd             0.0.0.0         4201
      hsql             0.0.0.0         9001
      telnet           0.0.0.0         4202
    -------
    Ready!
```

## 4. Write an EJB

In the EJB project create a new interface named Greeting

```java
package com.myejbs;

import jakarta.ejb.Remote;

@Remote
public interface Greeting {
  public String greet();
}
```

Now create a new class named GreetingBean which implements the above
interface (shown below)

```java
package com.myejbs;

import jakarta.ejb.Stateless;

@Stateless
public class GreetingBean implements Greeting {

    public String greet() {
        return "My First Remote Stateless Session Bean";
    }

}
```

## 5. Deploy the EJB

1. Export the EJBProject as a jar file. Name it greeting.jar and put it
   in the OPENEJB\_HOME/apps directory.
2. Open the command prompt and type in the following command:

   d:-3.0.0-SNAPSHOT > bindeploy apps.jar

This should give you the following output:

```java
D:\openejb-3.0.0-SNAPSHOT>bin\openejb deploy apps\greeting.jar

Application deployed successfully at \{0\}

App(id=D:\openejb-3.0.0-SNAPSHOT\apps\greeting.jar)

    EjbJar(id=greeting.jar, path=D:\openejb-3.0.0-SNAPSHOT\apps\greeting.jar)

    Ejb(ejb-name=GreetingBean, id=GreetingBean)

        Jndi(name=GreetingBeanRemote)
```

\{color:#330000}\{*}Notice the Jndi(name=GreetingBeanRemote)
information. Keep this handy as this is the JNDI name of the bean which
the client will use for lookup\{*}{color}

## 6. Write the Client

In the EJBClient project, create a class named Client (shown below)

```java
package com.myclient;

import com.myejbs.Greeting;

import javax.naming.InitialContext;
import java.util.Properties;

public class Client {
    public static void main(String[] args) {

        try {
            Properties p = new Properties();
            p.put("java.naming.factory.initial", "org.openejb.client.RemoteInitialContextFactory");
            p.put("java.naming.provider.url", "ejbd://127.0.0.1:4201");
            InitialContext ctx = new InitialContext(p);
            Greeting greeter = (Greeting) ctx.lookup("GreetingBeanRemote");
            String message = greeter.greet();
            System.out.println(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

## 7. Run the Client

Open Client.java in eclipse and run it as a java application. You should
see the following message in the console view:

```properties
My First Remote Stateless Session Bean
```

## 8. Stop the server

There are two ways to stop the server: 1. You can press Ctrl+c on the
command prompt to stop the server 1. On the command prompt type in the
following command:

```java
D:\openejb-3.0.0-SNAPSHOT>bin\openejb stop
```

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-getting-started--)

---

<a id="tomee-apache-org-tomee-10-1-docs-hello-world"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Hello World

This page shows the basic steps required to create, build, and
run an EJB and EJB client in its most minimum form. It does not hide
steps or rely on special build tools or IDEs and is about the most
stripped down you can get.

*See the [Examples](examples.html) page for a full list of examples
that range from [`@Stateles`|Simple Stateless Example] and
[@Stateful|Simple Stateful Example] beans, to [Dependency
Injection|Injection of env-entry Example] , JDBC [DataSources|Injection
of DataSource Example] , JPA [EntityManagers|Injection of EntityManager
Example] and more.*

## A basic EJB example

Here are some basic steps you need to perform to get started with
OpenEJB

1. Download and install OpenEJB
2. Setup your development environment
3. Write an EJB
4. Write an EJB client
5. Start the server
6. Deploy the EJB
7. Run the client
8. Stop the server

## Download and install OpenEJB

This example pertains to OpenEJB 3.0 which can be
[downloaded here](http://archive.apache.org/dist/openejb/3.0) . Once you
have downloaded OpenEJB, you can then simply extract the contents of the
downloaded file to whichever directory you want to install OpenEJB in.

After extracting the file contents, you should now see a directory named
openejb-3.0. If you look under this directory, you will find a few more
directories: - *bin*: Contains commands to start/stop the server (You
can also do a lot of other stuff like deploy/undeploy, but we will just
talk about things needed to get you started) - *lib*: Contains several
jar files (you only need of few of these jars in your classpath to do
EJB development) - *apps*: Once you create your EJB’s and jar them up,
you can place your jar file in this directory and start the server. The
server will automatically deploy all the EJB’s contained in this JAR. -
*conf*: This directory contains all the configuration files. Although
you may not see any file except for a README.txt file right now, but
after you start the server, the required configuration files will be
automatically created. It is highly recommended to read the README.txt
file under this directory - *logs*: Contains log files.

## Setup your development environment

### Create a working directory Assuming you are in your home directory,

create a directory named projects

```bash
karan@poweredge:~$ mkdir projects
```

Go to the projects directory

```bash
karan@poweredge:~$ cd projects
```

We will do all our work in this directory. # Install Java Download and
install Java (version 5 or higher). Also set it up so that you can run
the java and javac commands from any directory # Set OPENEJB\_HOME We
will set up this variable to refer to the openejb install location.

```bash
karan@poweredge:~/projects$ export
```

OPENEJB\_HOME=/home/karan/install/openejb-3.0

## Write an EJB Whatever files you create should be placed under the

projects directory # Create the Remote Interface Using your favorite
editor, create a file named Hello.java (shown below)

```java
package org.acme;
import jakarta.ejb.Remote;
@Remote
public interface Hello{
    public String sayHello();
}
```

### Create the Bean Class Now create a file named HelloBean.java (shown

below)

```java
package org.acme;
import jakarta.ejb.Stateless;
@Stateless
public class HelloBean implements Hello{
    public String sayHello(){
        return "Hello World!!!!";
    }
}
```

### Compile the source code Since we have imported the

jakarta.ejb.Stateless and jakarta.ejb.Remote annotations, we need these in
our classpath to compile our source code. These annotations can be found
in the $OPENEJB\_HOME/lib/javaee-5.0-1.jar. Let’s compile our source (make
sure you are in the projects directory)

```bash
karan@poweredge:~/projects$ javac -cp $OPENEJB_HOME/lib/javaee-5.0-1.jar -d
```

1. \*.java

The above will compile all the .java files and also create the required
packages. You should now see a package named org under the project’s
directory. All class files should be under org/acme directory. #
Package the EJB To package the EJB into a JAR, run the following command
while you are in the projects directory

```bash
karan@poweredge:~/projects$ jar cvf hello.jar org
```

The above command will package everything under the org directory
(including the org directory itself) into a jar file named hello.jar.
Below is the output from running the above command:

```bash
karan@poweredge:~/projects$ jar cvf hello.jar org
added manifest
adding: org/(in = 0) (out= 0)(stored 0%)
adding: org/acme/(in = 0) (out= 0)(stored 0%)
adding: org/acme/Hello.class(in = 203) (out= 168)(deflated 17%)
adding: org/acme/HelloBean.class(in = 383) (out= 275)(deflated 28%)
```

## Write an EJB Client Now we will write a Client class which will

lookup the EJB , invoke the sayHello() business method and print the
value returned from the method. While you are in the projects directory,
create a new file named HelloClient.java . Add the following to this
file:

```java
package org.acme;
import java.util.Properties;
import javax.naming.InitialContext;
import javax.naming.Context;
import javax.rmi.PortableRemoteObject;
public class HelloClient{
        public static void main(String[]
```

args) throws Exception\{ Properties props = new Properties();

props.put(Context.INITIAL\_CONTEXT\_FACTORY,"org.apache.openejb.client.RemoteInitialContextFactory");
props.put(Context.PROVIDER\_URL,"ejbd://127.0.0.1:4201"); Context ctx =
new InitialContext(props); Object ref = ctx.lookup("HelloBeanRemote");
Hello h = (Hello)PortableRemoteObject.narrow(ref,Hello.class); String
result = h.sayHello(); System.out.println(result); } }

### Compile HelloClient.java Run the following command:

```bash
karan@poweredge:~/projects$ javac  -d . HelloClient.java
```

## Start the Server Go to the OpenEJB install directory (i.e.

OPENEJB\_HOME) and run the following command:

```bash
karan@poweredge:~/install/openejb-3.0$ bin/openejb start
```

Once the Server starts, you will see an output similar to the below in
your console:

```bash
karan@poweredge:~/install/openejb-3.0$ bin/openejb start
Apache OpenEJB 3.0    build: 20070926-12:34
http://tomee.apache.org/
OpenEJB ready.
[OPENEJB:init]
```

OpenEJB Remote Server  **Starting Services**  NAME IP PORT  
httpejbd 0.0.0.0 4204  
telnet 0.0.0.0 4202  
ejbd 0.0.0.0 4201  
hsql 0.0.0.0 9001  
admin thread 0.0.0.0 4200  
------- Ready!

Take out a minute to browse through the conf and logs directories. You
should now see some configuration and log files under the respective
directories. ## Deploy the EJB We will now use the deploy command to
deploy the EJB in hello.jar. While you are in the projects directory,
run the following command:

```bash
karan@poweredge:~/projects$ $OPENEJB_HOME/bin/openejb deploy hello.jar
```

The above command should give you the following output:

```bash
karan@poweredge:~/projects$ $OPENEJB_HOME/bin/openejb deploy hello.jar
Application deployed successfully at "hello.jar"
App(id=/home/karan/projects/hello.jar)
    EjbJar(id=hello.jar, path=/home/karan/projects/hello.jar)
    Ejb(ejb-name=HelloBean, id=HelloBean)
        Jndi(name=HelloBeanRemote)
```

Notice how the output neatly lays out various deployment details. One
thing you might want to note from the output is the JNDI name. This is
the JNDI name we used in the client to lookup the EJB ## Run the Client
While you are in the projects directory, run the following command to
run the client:

```bash
karan@poweredge:~/projects$ java -cp
```

\(OPENEJB\_HOME/lib/openejb-client-3.0.jar:\)OPENEJB\_HOME/lib/javaee-5.0-1.jar:.
org.acme.HelloClient

The above should give you the following output:

```properties
Hello World!!!!
```

## Help! It didn’t work for me!!. No problem, we are here to help.

Just send us an email at [users@tomee.apache.org](mailto:users@tomee.apache.org). If possible, send us
the contents of logs/openejb.log file in the email.

# Looking for more?

More EJB 3.0 examples, sample applications, tutorials and howtos
available [here](examples.html) .

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-hello-world--)

---

<a id="tomee-apache-org-tomee-10-1-docs-hibernate"></a>

# Apache TomEE

```xml
<persistence version="1.0"
       xmlns="http://java.sun.com/xml/ns/persistence"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://java.sun.com/xml/ns/persistence
       http://java.sun.com/xml/ns/persistence/persistence_1_0.xsd">

  <persistence-unit name="movie-unit">
    <provider>org.hibernate.ejb.HibernatePersistence</provider>
    <jta-data-source>movieDatabase</jta-data-source>
    <non-jta-data-source>movieDatabaseUnmanaged</non-jta-data-source>

    <properties>
      <property name="hibernate.hbm2ddl.auto" value="create-drop"/>
      <property name="hibernate.transaction.manager_lookup_class"
                value="org.apache.openejb.hibernate.TransactionManagerLookup"/>
    </properties>
  </persistence-unit>
</persistence>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-initialcontext-config"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# InitialContext Configuration

A InitialContext can be declared via xml in the
`<tomee-home>/conf/tomee.xml` file or in a `WEB-INF/resources.xml` file
using a declaration like the following. All properties in the element
body are optional.

```xml
<JndiProvider id="myInitialContext" type="javax.naming.InitialContext">
</JndiProvider>
```

Alternatively, a InitialContext can be declared via properties in the
`<tomee-home>/conf/system.properties` file or via Java VirtualMachine
`-D` properties. The properties can also be used when embedding TomEE
via the `jakarta.ejb.embeddable.EJBContainer` API or `InitialContext`

```properties
myInitialContext = new://JndiProvider?type=javax.naming.InitialContext
```

Properties and xml can be mixed. Properties will override the xml
allowing for easy configuration change without the need for $\{} style
variable substitution. Properties are not case-sensitive. If a property
is specified that is not supported by the declared InitialContext a
warning will be logged. If a InitialContext is needed by the application
and one is not declared, TomEE will create one dynamically using default
settings. Multiple InitialContext declarations are allowed. # Supported
Properties

Property

Type

Default

Description

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-initialcontext-config--)

---

<a id="tomee-apache-org-tomee-10-1-docs-installation-drop-in-war"></a>

# Apache TomEE

```xml
 <!-- Activate/create these lines to get access to TomEE GUI -->
 <role rolename="tomee-admin" />
 <user username="tomee" password="tomee" roles="tomee-admin" />
```

---

<a id="tomee-apache-org-tomee-10-1-docs-installation"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Installation

# Installation

Installation is easiest from an update site. In Eclipse, select Help,
Software Updates, Find and install…​

!http://jrg.me.uk/openejb/install\_step\_1.jpg!

Select 'Search for new features to install'

!http://jrg.me.uk/openejb/install\_step\_2.jpg!

Select 'New Remote site'. Enter 'OpenEJB' for the name and
[http://people.apache.org/~jgallimore/update-site/](http://people.apache.org/~jgallimore/update-site/) for the URL. Click
'Ok' and make sure your new update site is selected. Then select
'Finish'

!http://jrg.me.uk/openejb/install\_step\_3.jpg!

Check the box to install the OpenEJB feature. Click 'Next'

!http://jrg.me.uk/openejb/install\_step\_4.jpg!

Read and make sure you’re happy with the license agreement.

Check the installation location, and change it if you wish to. Select
'Finish'.

Restarting the workbench when the installation is finished is
recommended.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-installation--)

---

<a id="tomee-apache-org-tomee-10-1-docs-installing-tomee"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Installing TomEE

|  | Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at .http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. |
| --- | --- |

## Downloading a Distribution Archive

Download a distribution archive of the latest TomEE release from the
[Downloads](download-ng.html) page. If you are not sure which one to
use, take apache-tomee-webprofile-x.y.z.zip.

If you want to try out the latest development snapshot (e.g. to test a
bugfix which has not yet been released), then download an archive
directly from the
[Apache
Maven Snapshots Repository](https://repository.apache.org/content/groups/snapshots/org/apache/tomee/apache-tomee) instead.

The instructions on this page work with the webprofile distribution but
apply to all TomEE distributions (webprofile, jaxrs or plus). If you
work with the jaxrs or plus distribution, simply replace the name where
appropriate.

## Unpacking the Archive

Unpack the archive in any directory. The top-level directory of the
unpacked archive is called apache-tomee-webprofile-x.y.z/. We refer to
this directory as $CATALINA\_HOME in the following.

## Prerequisites for Running TomEE

- A Java 6 or 7 runtime environment is in your PATH.
- TCP ports 8080, 8005 and 8009 are free.

## Starting TomEE

To start TomEE as a background process, invoke

```java
$CATALINA_HOME/bin/startup.sh
```

|  | On Windows, usestartup.bat |
| --- | --- |

To start TomEE in foreground, invoke

```java
$CATALINA_HOME/bin/catalina.sh run
```

|  | On Windows, usecatalina.bat |
| --- | --- |

## Log Messages

When running TomEE in foreground, it will print all log messages to the
console.

When running TomEE in background, it will only print a couple of
environment variables. The log messages go to a file

```java
$CATALINA_HOME/logs/catalina.out
```

## Stopping TomEE

To stop TomEE, invoke

```java
$CATALINA_HOME/bin/shutdown.sh
```

|  | On Windows, useshutdown.bat |
| --- | --- |

If you started TomEE in foreground via `catalina.sh run` or `catalina.bat run`, it is safe to
simply type `Ctrl-C`.

## Running as a service

To run TomEE as a service, see link:lin-service.html for Linux, and link:win-service.html for Windows.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-installing-tomee--)

---

<a id="tomee-apache-org-tomee-10-1-docs-jakartaee-9-eclipse-transformer"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# Eclipse Transformer

The Eclipse Transformer provides translation from javax to jakarta on various artifacts, including Java source source, compiled Java classes, along with jar, war and ear files.

## Building

The Transformer is a project at the Eclipse Foundation, and is built using Gradle. To check out and build from source, do the following:

```bash
git clone https://github.com/tbitonti/jakartaee-prototype
cd jakartaee-prototype
./gradlew assembleDist
```

This will produce a tar and a zip in the `transformer/build/distributions` directory.

## Running

Unzip the resulting zip file, and add the `bin` directory to your `PATH` variable, and you should be able to run the transformer using the `transformer` script.

Running `transformer -u` will provide the usage information:

```bash
usage: org.eclipse.transformer.Transformer input [ output ] [ options ]
Options:
 -d,--dryrun                    Dry run
 -h,--help                      Display help
 -i,--invert                    Invert transformation rules
 -lf,--logFile <arg>            Logging file
 -ll,--logLevel <arg>           Logging level
 -ln,--logName <arg>            Logger name
 -lp,--logProperty <arg>        Logging property
 -lpf,--logPropertyFile <arg>   Logging properties file
 -o,--overwrite                 Overwrite
 -q,--quiet                     Display quiet output
 -t,--type <arg>                Input file type
 -tb,--bundles <arg>            Transformation bundle updates URL
 -td,--direct <arg>             Transformation direct string replacements
 -tf,--xml <arg>                Map of XML filenames to property files
 -tr,--renames <arg>            Transformation package renames URL
 -ts,--selection <arg>          Transformation selections URL
 -tv,--versions <arg>           Transformation package versions URL
 -u,--usage                     Display usage
 -v,--verbose                   Display verbose output
Actions:
  [ NULL ]
  [ CLASS ]
  [ MANIFEST ]
  [ FEATURE ]
  [ SERVICE_LOADER_CONFIG ]
  [ XML ]
  [ ZIP ]
  [ JAR ]
  [ WAR ]
  [ RAR ]
  [ EAR ]
  [ JAVA ]
  [ DIRECTORY ]
Logging Properties:
  [ org.slf4j.simpleLogger.logFile ]
  [ org.slf4j.simpleLogger.cacheOutputStream ]
  [ org.slf4j.simpleLogger.defaultLogLevel ]
  [ org.slf4j.simpleLogger.log.a.b.c ]
  [ org.slf4j.simpleLogger.levelInBrackets ]
  [ org.slf4j.simpleLogger.showDateTime ]
  [ org.slf4j.simpleLogger.dateTimeFormat ]
  [ org.slf4j.simpleLogger.showThreadName ]
  [ org.slf4j.simpleLogger.showLogName ]
  [ org.slf4j.simpleLogger.showShortLogName ]
  [ org.slf4j.simpleLogger.warnLevelString ]
```

With an already built and extracted TomEE server, we can do some experimentation.

Rename the lib folder:

```bash
mv lib javaee-lib
```

Create a Jakarta EE lib folder:

```bash
mkdir jakartaee-lib
```

Translate each of the jars in the javaee-lib folder:

```bash
cd javaee-lib

for f in *; do transformer $f ../jakartaee-lib/$f -tr <path/to/rules/file>; done
```

See the rules section below for the rules file.

Symlink jakartaee-lib folder to lib:

```bash
cd ..
ln -s jakartaee-lib lib
```

You should now be able to start a Jakarta-ized version of TomEE. (Expect errors - these can be discussed on the list)

## Rules

The current rules for package renames are in the `transformer/jakarta-renames.properties` file in the main TomEE source repository.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-jakartaee-9-eclipse-transformer--)

---

<a id="tomee-apache-org-tomee-10-1-docs-jakartaee-9-index"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# Jakarta EE 9 Work

This page is something of a work-in-progress, but attempts to serve as an entrypoint for anyone looking to get involved with this work.

## Challenge

Jakarta EE 9 removes a very small number of specifications, and doesn’t introduce functional changes to the specifications. It does, however, introduce a package
rename, which will have a very wide impact. Not only do the specification jars and implementations in the server need to change their namespaces, user applications
will need to change any references as well.

## Goals

- Try and maintain a single codebase for javax and jakarta. It’s tempting to fork master and embark on a massive renaming exercise. That’s complex as we’d need to do that for various dependencies as well, who may also have other branches and timelines. Having two code-bases also means that any changes need to be applied twice, and with renamed packages, it’s unlikely the git merging or cherry-picking will work.
- Be backwards compatible - One goal I had in my mined, is that if you have an application that uses javax, you’d probably like to be able to run it on a new Jakarta EE server. There are some options here - I quite like the idea of running the Transformer as a javaagent, so any applications deployed using the old namespaces are converted on the fly at the bytecode level.
- Tooling - I wonder what tooling we could potentially provide? One thought I had was a Maven plugin that can transform a war/ear file for you as part of a build.

## Approaches

- Eclipse Transformer

Investigate the use of the Eclipse Transformer ([https://projects.eclipse.org/projects/technology.transformer](https://projects.eclipse.org/projects/technology.transformer)) to translate the server, and the example applications to the Jakarta namespace.

[Building and running the Eclipse Transformer](#tomee-apache-org-tomee-10-1-docs-jakartaee-9-eclipse-transformer)

- ???

## Tasks Tracker

TBD: something will appear here soon

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-jakartaee-9-index--)

---

<a id="tomee-apache-org-tomee-10-1-docs-java7"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# TomEE and Java 7

|  | Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at .http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. |
| --- | --- |

If you compile your applications on JDK7 you have to run Apache TomEE
1.0 on jdk7

## Configuring TomEE to use JDK7

If you have multiple JDK installed on your system you should set
JAVA\_HOME in your startup scripts. For example if your `JAVA_HOME` is
`/usr/local/java/current` edit `catalina.sh` and add a line

`JAVA_HOME=/usr/local/java/current/bin`

Alternatively, set `JAVA_HOME` as an environment variable prior to
calling `<tomee-home>/bin/startup.sh`

## Endorsed libraries directory

TomEE 1.0 package comes with and "endorsed" directory which contains
updates for core JDK6 libraries. If you are running JDK7 you should
remove al files in this directory.

TomEE 1.1 will detect JDK7 and will not load those files

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-java7--)

---

<a id="tomee-apache-org-tomee-10-1-docs-javaagent-with-maven-surefire"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# JavaAgent with Maven Surefire

## Maven2

In maven2 you can enable the javaagent for your tests by adding this to
your pom.xml file:

```xml
<build>
  <plugins>
    <!-- this configures the surefire plugin to run your tests with the
```

javaagent enabled -→ org.apache.maven.plugins maven-surefire-plugin
pertest

-javaagent:\({basedir}/target/openejb-javaagent-3.0.jar</argLine> <workingDirectory>\){basedir}/target

```xml
    <!-- this tells maven to copy the openejb-javaagent jar into your
```

target/ directory -→ org.apache.maven.plugins maven-dependency-plugin
copy process-resources copy org.apache.openejb openejb-javaagent 3.0

$\{project.build.directory}

```xml
  </plugins>
</build>
```

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-javaagent-with-maven-surefire--)

---

<a id="tomee-apache-org-tomee-10-1-docs-javaagent"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# JavaAgent

# Adding a JavaAgent

|  | The java agent is only required if using OpenJPA as your persistence provider or if using CMP. |
| --- | --- |

Adding a java agent is done via a vm parameter as follows:

```bash
java -javaagent:openejb-javaagent-4.6.0.jar _\[other params...](other-params....html)
```

## Maven2

In maven2 you can enable the javaagent for your tests by adding this to
your pom.xml file:

```xml
<build>
  <plugins>
    <!-- this configures the surefire plugin to run your tests with the javaagent enabled -->
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-surefire-plugin</artifactId>
      <configuration>
        <forkMode>pertest</forkMode>
        <argLine>-javaagent:${project.basedir}/target/openejb-javaagent-4.6.0.jar</argLine>
        <workingDirectory>${project.basedir}/target</workingDirectory>
      </configuration>
    </plugin>
    <!-- this tells maven to copy the openejb-javaagent jar into your target/ directory -->
    <!-- where surefire can see it -->
    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-dependency-plugin</artifactId>
      <executions>
        <execution>
          <id>copy</id>
          <phase>process-resources</phase>
          <goals>
            <goal>copy</goal>
          </goals>
          <configuration>
            <artifactItems>
              <artifactItem>
                <groupId>org.apache.openejb</groupId>
                <artifactId>openejb-javaagent</artifactId>
                <version>4.6.0</version>
                <outputDirectory>${project.build.directory}</outputDirectory>
              </artifactItem>
            </artifactItems>
          </configuration>
        </execution>
      </executions>
    </plugin>
  </plugins>
</build>
```

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-javaagent--)

---

<a id="tomee-apache-org-tomee-10-1-docs-javamailsession-config"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# JavaMailSession Configuration

A JavaMailSession can be declared via xml in the
`<tomee-home>/conf/tomee.xml` file or in a `WEB-INF/resources.xml` file
using a declaration like the following. All properties in the element
body are optional.

```xml
<Resource id="myJavaMailSession" type="jakarta.mail.Session">
</Resource>
```

Alternatively, a JavaMailSession can be declared via properties in the
`<tomee-home>/conf/system.properties` file or via Java VirtualMachine
`-D` properties. The properties can also be used when embedding TomEE
via the `jakarta.ejb.embeddable.EJBContainer` API or `InitialContext`

```properties
myJavaMailSession = new://Resource?type=jakarta.mail.Session
```

Properties and xml can be mixed. Properties will override the xml
allowing for easy configuration change without the need for $\{} style
variable substitution. Properties are not case-sensitive. If a property
is specified that is not supported by the declared JavaMailSession a
warning will be logged. If a JavaMailSession is needed by the
application and one is not declared, TomEE will create one dynamically
using default settings. Multiple JavaMailSession declarations are
allowed. # Supported Properties

Property

Type

Default

Description

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-javamailsession-config--)

---

<a id="tomee-apache-org-tomee-10-1-docs-jms-resources-and-mdb-container"></a>

# Apache TomEE

```xml
<tomee>
    <Resource id="MyJmsResourceAdapter" type="ActiveMQResourceAdapter">
        # Do not start the embedded ActiveMQ broker
        BrokerXmlConfig  =
        ServerUrl = tcp://someHostName:61616
    </Resource>

    <Resource id="MyJmsConnectionFactory" type="jakarta.jms.ConnectionFactory">
        ResourceAdapter = MyJmsResourceAdapter
    </Resource>

    <Container id="MyJmsMdbContainer" ctype="MESSAGE">
        ResourceAdapter = MyJmsResourceAdapter
    </Container>

    <Resource id="FooQueue" type="jakarta.jms.Queue"/>
    <Resource id="BarTopic" type="jakarta.jms.Topic"/>
</tomee>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-jmsconnectionfactory-config"></a>

# Apache TomEE

```xml
<Resource id="myJmsConnectionFactory" type="jakarta.jms.ConnectionFactory">
    connectionMaxIdleTime = 15 Minutes
    connectionMaxWaitTime = 5 seconds
    poolMaxSize = 10
    poolMinSize = 0
    resourceAdapter = Default JMS Resource Adapter
    transactionSupport = xa
</Resource>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-jndi-names"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# JNDI Names

# What’s My Bean’s JNDI Name? There are two things to keep in mind

before you start reading:

1 OpenEJB provides a default JNDI name to your EJB.  
2 You can customize the JNDI name.

## Default JNDI name The default JNDI name is in the following format:

```json
{deploymentId}{interfaceType.annotationName}
```

Let’s try and understand the above format. Both *deploymentId* and
*interfaceType.annotationName* are pre-defined variables. There are
other pre-defined variables available which you could use to customize
the JNDI name format.

# JNDI Name Formatting

The *openejb.jndiname.format* property allows you to supply a template
for the global JNDI names of all your EJBs. With it, you have complete
control over the structure of the JNDI layout can institute a design
pattern just right for your client apps. See the
[Service Locator](#tomee-apache-org-tomee-10-1-docs-service-locator) doc for clever ways to use
the JNDI name formatting functionality in client code.

variable

description

moduleId

Typically, the name of the ejb-jar file or the id value if specified

ejbType

STATEFUL, STATELESS, BMP\_ENTITY, CMP\_ENTITY, or MESSAGE\_DRIVEN

ejbClass

for a class named org.acme.superfun.WidgetBean results in
org.acme.superfun.WidgetBean

ejbClass.simpleName

for a class named org.acme.superfun.WidgetBean results in WidgetBean

ejbClass.packageName

for a class named org.acme.superfun.WidgetBean results in
org.acme.superfun

ejbName

The ejb-name as specified in xml or via the 'name' attribute in
@Stateful, `@Stateless`, or `@MessageDriven` annotation

deploymentId

The unique system id for the ejb. Typically, the ejbName unless specified
in the `openejb-jar.xml` or via changing the `openejb.deploymentId.format`

interfaceType

see interfaceType.annotationName

interfaceType.annotationName

Following the EJB 3 annotations `@RemoteHome`, `@LocalHome`, `@Remote` and
@Local RemoteHome (EJB 2 EJBHome) LocalHome (EJB 2 EJBLocalHome) Remote
(EJB 3 Business Remote) Local (EJB 3 Business Local) Endpoint (EJB
webservice endpoint)

interfaceType.annotationNameLC

This is the same as interfaceType.annotationName, but all in lower case.

interfaceType.xmlName

Following the `ejb-jar.xml` descriptor elements , , , , and : home (EJB 2
EJBHome) local-home (EJB 2 EJBLocalHome) business-remote (EJB 3 Business
Remote) business-local (EJB 3 Business Local) service-endpoint (EJB
webservice endpoint)

interfaceType.xmlNameCc

Camel-case version of interfaceType.xmlName: Home (EJB 2 EJBHome)
LocalHome (EJB 2 EJBLocalHome) BusinessRemote (EJB 3 Business Remote)
BusinessLocal (EJB 3 Business Local) ServiceEndpoint (EJB webservice
endpoint)

interfaceType.openejbLegacyName

Following the OpenEJB 1.0 hard-coded format: (empty string) (EJB 2
EJBHome) Local (EJB 2 EJBLocalHome) BusinessRemote (EJB 3 Business
Remote) BusinessLocal (EJB 3 Business Local) ServiceEndpoint (EJB
webservice endpoint)

interfaceClass

(business) for a class named org.acme.superfun.WidgetRemote results in
org.acme.superfun.WidgetRemote (home) for a class named
org.acme.superfun.WidgetHome results in org.acme.superfun.WidgetHome

interfaceClass.simpleName

(business) for a class named org.acme.superfun.WidgetRemote results in
WidgetRemote (home) for a class named org.acme.superfun.WidgetHome
results in WidgetHome

interfaceClass.packageName

for a class named org.acme.superfun.WidgetRemote results in
org.acme.superfun

# Setting the JNDI name

It’s possible to set the desired jndi name format for the whole server
level, an ejb-jar, an ejb, an ejb’s "local" interface
(local/remote/local-home/home), and for an individual interface the ejb
implements. More specific jndi name formats act as an override to any
more general formats. The most specific format dictates the jndi name
that will be used for any given interface of an ejb. It’s possible to
specify a general format for your server, override it at an ejb level
and override that further for a specific interface of that ejb.

## Via System property

The jndi name format can be set on a server level via a *system
property*, for example:

```java
$ ./bin/openejb start
-Dopenejb.jndiname.format=\{ejbName}/\{interfaceClass}"
```

As usual, other ways of specifying system properties are via the
conf/system.properties file in a standalone server, or via the
InitialContext properties when embedded.

## Via properties in the openejb-jar.xml

It’s possible to set the openejb.jndiname.format for an ejb-jar jar in a
`META-INF/openejb-jar.xml` file as follows:

```xml
<openejb-jar>
  <properties>
     openejb.deploymentId.format = {ejbName}
     openejb.jndiname.format = {deploymentId}{interfaceType.annotationName}
  </properties>
</openejb-jar>
```

## Via the tag for a specific ejb

The following sets the name specifically for the interface
org.superbiz.Foo.

```xml
<openejb-jar>
  <ejb-deployment ejb-name="FooBean">
    <jndi name="foo" interface="org.superbiz.Foo"/>
  </ejb-deployment>
</openejb-jar>
```

Or more generally…​

```xml
<openejb-jar>
  <ejb-deployment ejb-name="FooBean">
    <jndi name="foo" interface="Remote"/>
  </ejb-deployment>
</openejb-jar>
```

Or more generally still…​

```xml
<openejb-jar>
  <ejb-deployment ejb-name="FooBean">
    <jndi name="foo"/>
  </ejb-deployment>
</openejb-jar>
```

The 'name' attribute can still use templates if it likes, such as:

```xml
<openejb-jar>
  <ejb-deployment ejb-name="FooBean">
    <jndi name="ejb/{interfaceClass.simpleName}" interface="org.superbiz.Foo"/>
  </ejb-deployment>
</openejb-jar>
```

### Multiple tags

Multiple tags are allowed making it possible for you to be as specific
as you need about the jndi name of each interface or each logical group
of iterfaces (Local, Remote, LocalHome, RemoteHome).

Given an ejb, FooBean, with the following interfaces: - business-local:
org.superbiz.LocalOne - business-local: org.superbiz.LocalTwo -
business-remote: org.superbiz.RemoteOne - business-remote:
org.superbiz.RemoteTwo - home: org.superbiz.FooHome - local-home:
org.superbiz.FooLocalHome

The following four examples would yield the same jndi names. The
intention with these examples is to show the various ways you can
isolate specific interfaces or types of interfaces to gain more specific
control on how they are named.

```xml
<openejb-jar>
  <ejb-deployment ejb-name="FooBean">
    <jndi name="LocalOne" interface="org.superbiz.LocalOne"/>
    <jndi name="LocalTwo" interface="org.superbiz.LocalTwo"/>
    <jndi name="RemoteOne" interface="org.superbiz.RemoteOne"/>
    <jndi name="RemoteTwo" interface="org.superbiz.RemoteTwo"/>
    <jndi name="FooHome" interface="org.superbiz.FooHome"/>
    <jndi name="FooLocalHome" interface="org.superbiz.FooLocalHome"/>
  </ejb-deployment>
</openejb-jar>
```

Or

```xml
<openejb-jar>
  <ejb-deployment ejb-name="FooBean">
    <!-- applies to LocalOne and LocalTwo -->
    <jndi name="{interfaceClass.simpleName}" interface="Local"/>

    <!-- applies to RemoteOne and RemoteTwo -->
    <jndi name="{interfaceClass.simpleName}" interface="Remote"/>

    <!-- applies to FooHome -->
    <jndi name="{interfaceClass.simpleName}" interface="RemoteHome"/>

    <!-- applies to FooLocalHome -->
    <jndi name="{interfaceClass.simpleName}" interface="LocalHome"/>
  </ejb-deployment>
</openejb-jar>
```

Or

```xml
<openejb-jar>
  <ejb-deployment ejb-name="FooBean">
    <!-- applies to RemoteOne, RemoteTwo, FooHome, and FooLocalHome -->
    <jndi name="{interfaceClass.simpleName}"/>

    <!-- these two would count as an override on the above format -->
    <jndi name="LocalOne" interface="org.superbiz.LocalOne"/>
    <jndi name="LocalTwo" interface="org.superbiz.LocalTwo"/>
  </ejb-deployment>
</openejb-jar>
```

or

```xml
<openejb-jar>
  <ejb-deployment ejb-name="FooBean">
    <!-- applies to LocalOne, LocalTwo, RemoteOne, RemoteTwo, FooHome, and FooLocalHome -->
    <jndi name="{interfaceClass.simpleName}"/>
  </ejb-deployment>
</openejb-jar>
```

# Changing the Default Setting

*You are responsible for ensuring the names don’t conflict.*

## Conservative settings

A very conservative setting such as

"{deploymentId}/{interfaceClass}"

would guarantee that each and every single interface is bound to JNDI.
If your bean had a legacy EJBObject interface, three business remote
interfaces, and two business local interfaces, this pattern would result
in  
*six* proxies bound into JNDI.

Bordeline optimistic:

The above two settings would work if the interface wasn’t shared by
other beans.

### Pragmatic settings

A more middle ground setting such as
"{deploymentId}/\{interfaceType.annotationName}" would guarantee that
at least one proxy of each interface type is bound to JNDI. If your bean
had a legacy EJBObject interface, three business remote interfaces, and
two business local interfaces, this pattern would result in *three*
proxies bound into JNDI: one proxy dedicated to your EJBObject
interface; one proxy implementing all three business remote interfaces;
one proxy implementing the two business local interfaces.

Similarly pragmatic settings would be

### Optimistic settings

A very optimistic setting such as "{deploymentId}" would guarantee only
one proxy for the bean will be bound to JNDI. This would be fine if you
knew you only had one type of interface in your beans. For example, only
business remote interfaces, or only business local interfaces, or only
an EJBObject interface, or only an EJBLocalObject interface.

If a bean in the app did have more than one interface type, one business
local and one business remote for example, by default OpenEJB will
reject the app when it detects that it cannot bind the second interface.
This strict behavior can be disabled by setting the
*openejb.jndiname.failoncollision* system property to *false*. When this
property is set to false, we will simply log an error that the second
proxy cannot be bound to JNDI, tell you which ejb is using that name,
and continue loading your app.

Similarly optimistic settings would be:

### Advanced Details on EJB 3.0 Business Proxies (the simple part)

If you implement your business interfaces, your life is simple as your
proxies will also implement your business interfaces of the same type.
Meaning any proxy OpenEJB creates for a business local interface will
also implement your other business local interfaces. Similarly, any
proxy OpenEJB creates for a business remote interface will also
implement your other business remote interfaces.

### Advanced Details on EJB 3.0 Business Proxies (the complicated part)

*Who should read?*  
Read this section of either of these two apply to you:  
- You do not implement your business interfaces in your bean class  
- One or more of your business remote interfaces extend from
javax.rmi.Remote

If neither of these two items describe your apps, then there is no need
to read further. Go have fun.

### Not implementing business interfaces

If you do not implement your business interfaces it may not be possible
for us to implement all your business interfaces in a single interface.
Conflicts in the throws clauses and the return values can occur as
detailed [here](#tomee-apache-org-tomee-10-1-docs-multiple-business-interface-hazzards) . When
creating a proxy for an interface we will detect and remove any other
business interfaces that would conflict with the main interface.

### Business interfaces extending javax.rmi.Remote

Per spec rules many runtime exceptions (container or connection related)
are thrown from javax.rmi.Remote proxies as java.rmi.RemoteException
which is not a runtime exception and must be throwable via the proxy as
a checked exception. The issue is that conflicting throws clauses are
actually removed for two interfaces sharing the same method signature.
For example two methods such as these:  
- InterfaceA: void doIt() throws Foo;  
- InterfaceB: void doIt() throws RemoteException;

can be implemented by trimming out the conflicting throws clauses as
follows:  
- Implementation: void doIt()\{}

This is fine for a bean class as it does not need to throw the RMI
required javax.rmi.RemoteException. However, if we create a proxy from
these two interfaces it will also wind up with a 'doIt()\{}' method that
cannot throw javax.rmi.RemoteException. This is very bad as the
container does need to throw RemoteException to any business interfaces
extending java.rmi.Remote for any container related issues or connection
issues. If the container attempts to throw a RemoteException from the
proxies 'doIt()\{}' method, it will result in an
UndeclaredThrowableException thrown by the VM.

The only way to guarantee the proxy has the 'doIt() throws
RemoteException \{}' method of InterfaceB is to cut out InterfaceA when
we create the proxy dedicated to InterfaceB.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-jndi-names--)

---

<a id="tomee-apache-org-tomee-10-1-docs-jpa-concepts"></a>

# Apache TomEE

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<persistence xmlns="http://java.sun.com/xml/ns/persistence" version="1.0">

  <!-- Tutorial "unit" -->
  <persistence-unit name="Tutorial" transaction-type="RESOURCE_LOCAL">
    <non-jta-data-source>myNonJtaDataSource</non-jta-data-source>
    <class>org.superbiz.jpa.Account</class>
  </persistence-unit>

</persistence>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-jpa-usage"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# JPA Usage

# Things to watch out for

## Critical: Always set jta-data-source and non-jta-data-source

Always set the value of jta-data-source and non-jta-data-source in your
`persistence.xml` file. Regardless if targeting your EntityManager usage
for transaction-type="RESOURCE\_LOCAL" or transaction-type="TRANSACTION",
it’s very difficult to guarantee one or the other will be the only one
needed. Often times the JPA Provider itself will require both internally
to do various optimizations or other special features.

- The *jta-data-source* should always have it’s OpenEJB-specific
  '*JtaManaged*' property set to '*true*' (this is the default)
- The *non-jta-data-source* should always have it’s OpenEJB-specific
  '*JtaManaged*' property set to '*false*'.

See [Containers and Resources](#tomee-apache-org-tomee-10-1-docs-containers-and-resources) for how
to configure 'JtaManaged' and a full list of properties for DataSources.

## Be detach aware

A warning for any new JPA user is by default all objects will detach at
the end of a transaction. People typically discover this when the go to
remove or update an object they fetched previously and get an exception
like "You cannot perform operation delete on detached object".

All ejb methods start a transaction unless a) you
[configure them otherwise](#tomee-apache-org-tomee-10-1-docs-transaction-annotations) , or b) the
caller already has a transaction in progress when it calls the bean. If
you’re in a test case or a servlet, it’s most likely B that is biting
you. You’re asking an ejb for some persistent objects, it uses the
EntityManager in the scope of the transaction started around its method
and returns some persistent objects, by the time you get them the
transaction has completed and now the objects are detached.

### Solutions 1. Call EntityManager.merge(..) inside the bean code to

reattach your object. 1. Use PersistenceContextType.EXTENDED as in
'@PersistenceContext(unitName = "movie-unit", type =
PersistenceContextType.EXTENDED)' for EntityManager refs instead of the
default of PersistenceContextType.TRANSACTION. 1. If testing, use a
technique to execute transactions in your test code. That’s described
here in [Unit testing transactions](unit-testing-transactions.html)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-jpa-usage--)

---

<a id="tomee-apache-org-tomee-10-1-docs-local-client-injection"></a>

# Apache TomEE

```java
@LocalClient
public class MoviesTest extends TestCase {

    @EJB
    private Movies movies;

    @Resource
    private UserTransaction userTransaction;

    @PersistenceContext
    private EntityManager entityManager;

    public void setUp() throws Exception {
    Properties p = new Properties();
    p.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.client.LocalInitialContextFactory");
    InitialContext initialContext = new InitialContext(p);
    initialContext.bind("inject", this);
    }

    //... other test methods
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-local-server"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Local Server

!http://www.openejb.org/images/diagram-local-server.gif|valign=top,
align=right, hspace=15! # Accessing EJBs Locally

When OpenEJB embedded in your app, server, IDE, or JUnit, you can use
what we call the Local Server and avoid the network overhead and enjoy
an easy way to embed OpenEJB. Instead of putting the app in the server,
put the server in the app!

# Say what?! A local server?

Yes, you read correctly. OpenEJB can be embedded and treated as your
very own personal EJB container.

If they can have Local and Remote EJBs, why not Local and Remote EJB
Servers too?

Haven’t you ever wanted EJBs without the heavy? I mean you need the
"heavy" eventually, but not while you’re developing. Well, there’s the
advantage of an EJB implementation that was designed with a very clean
and well-defined server-container contract, you can cut the server part
out completely!

So, if you wish to access ejbs locally and not in client/server mode,
you can do so by embedding OpenEJB as a library and accessing ejbs
through OpenEJB’s built-in IntraVM (Local) Server. Why would someone
want to do this? \* Your application is a server or other middleware \*
You want to write an app that can be both stand alone *and* distributed
\* To test your EJBs with JUnit and don’t want to start/stop servers and
other nonsense \* Imagine the power from being able to use your IDE
debugger to step from your Client all the way into your EJB and back
with no remote debugging voodoo.

In this case, your application, test suite, IDE, or client accesses
beans as you would from any other EJB Server. The EJB Server just
happens to be running in the same virtual machine as your application.
This EJB Server is thusly called the IntraVM Server, and, for all
intents purposes, your application an IntraVM Client.

There are some interesting differences though. The IntraVM Server isn’t
a heavyweight server as one normally associates with EJB. It doesn’t
open connections, launch threads for processing requests, introduce
complex classloading hierarchies, or any of those "heavy" kind of
things. All it does is dish out proxies to your app that can be used to
shoot calls right into the EJB Container. Very light, very fast, very
easy for testing, debugging, developing, etc.

# Embedding

!http://www.openejb.org/images/diagram-local-server.gif|valign=top,
align=right, hspace=15! \{include:OPENEJBx30:Embedding}

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-local-server--)

---

<a id="tomee-apache-org-tomee-10-1-docs-lookup-of-other-ejbs-example"></a>

# Apache TomEE

```java
-------------------------------------------------------
 T E S T S
-------------------------------------------------------
Running org.superbiz.ejblookup.EjbDependencyTest
Apache OpenEJB 3.1.5-SNAPSHOT    build: 20101129-09:51
http://tomee.apache.org/
INFO - openejb.home =
```

---

<a id="tomee-apache-org-tomee-10-1-docs-managedcontainer-config"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# ManagedContainer Configuration

A ManagedContainer can be declared via xml in the
`<tomee-home>/conf/tomee.xml` file or in a `WEB-INF/resources.xml` file
using a declaration like the following. All properties in the element
body are optional.

```xml
<Container id="myManagedContainer" type="MANAGED">
</Container>
```

Alternatively, a ManagedContainer can be declared via properties in the
`<tomee-home>/conf/system.properties` file or via Java VirtualMachine
`-D` properties. The properties can also be used when embedding TomEE
via the `jakarta.ejb.embeddable.EJBContainer` API or `InitialContext`

```properties
myManagedContainer = new://Container?type=MANAGED
```

Properties and xml can be mixed. Properties will override the xml
allowing for easy configuration change without the need for $\{} style
variable substitution. Properties are not case-sensitive. If a property
is specified that is not supported by the declared ManagedContainer a
warning will be logged. If a ManagedContainer is needed by the
application and one is not declared, TomEE will create one dynamically
using default settings. Multiple ManagedContainer declarations are
allowed. # Supported Properties

Property

Type

Default

Description

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-managedcontainer-config--)

---

<a id="tomee-apache-org-tomee-10-1-docs-manual-installation"></a>

# Apache TomEE

```java
    C:\apache-tomcat-6.0.14>copy webapps\openejb\lib\openejb-loader-3.0.0-SNAPSHOT.jar lib\openejb-loader.jar
    1 file(s) copied.

    apache-tomcat-6.0.14$ cp webapps/openejb/lib/openejb-loader-*.jar lib/openejb-loader.jar
```

---

<a id="tomee-apache-org-tomee-10-1-docs-maven"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Maven

This page is intended to provide an insight into basic
[Maven](http://maven.apache.org/) usage for users that are not all that
familiar with [Maven](http://maven.apache.org/) projects. It is by no
means a tutorial and is designed to be more of a *quickstart* to get you
up and running.

You can find a really good [Maven](http://maven.apache.org/) tutorial
here: [http://books.sonatype.com/mvnex-book/reference/public-book.html](http://books.sonatype.com/mvnex-book/reference/public-book.html)

It is assumed that:

- You have downloaded and installed [Maven](http://maven.apache.org/) and
  that you can run **mvn --version** from any command prompt (or console).
- You have downloaded and installed
  [Subversion](http://subversion.apache.org/) and that you can run **svn
  --version** from any command prompt or console.

It is also assumed you have downloaded one of the following:

- One of the example projects from
  [](#tomee-apache-org-tomee-10-1-docs-maven)[http://svn.apache.org/repos/asf/tomee/tomee/trunk/examples](http://svn.apache.org/repos/asf/tomee/tomee/trunk/examples)
- The entire project source from
  [http://svn.apache.org/repos/asf/tomee/tomee/trunk](http://svn.apache.org/repos/asf/tomee/tomee/trunk)

Use [Subversion](http://subversion.apache.org/) to check out the example
sources from a console like so:

```bash
svn co http://svn.apache.org/repos/asf/tomee/tomee/trunk/examples/[example]
```

Or that you may of course also be using your own project pom.xml

If you want to use the latest snapshot locate the \_\_ section in your
pom.xml and ensure the following repository exists:

```xml
<repositories>
  <repository>
    <id>apache-m2-snapshot</id>
    <name>Apache M2 Snapshot Repository</name>
    <url>http://repository.apache.org/snapshots/</url>
    <releases>
      <enabled>false</enabled>
    </releases>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
  </repository>
</repositories>
```

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven--)

---

<a id="tomee-apache-org-tomee-10-1-docs-maven-build-mojo"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# null

tomee:build

Full name:

org.apache.openejb.maven:tomee-maven-plugin[:Current Version]:build

Description:

Create but not run a TomEE.

Attributes:

Requires a Maven project to be executed.

Requires dependency resolution of artifacts in scope: runtime+system.

Requires dependency collection of artifacts in scope: runtime.

Optional Parameters

Name

Type

Since

Description

apacheRepos

String

-

(no description)Default value is: snapshots.User property is:
tomee-plugin.apache-repos.

appDir

String

-

relative to tomee.base.Default value is: apps.

apps

List

-

(no description)

args

String

-

(no description)User property is: tomee-plugin.args.

attach

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.attach.

bin

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/bin.User property is:
tomee-plugin.bin.

catalinaBase

File

-

(no description)Default value is:
$\{project.build.directory}/apache-tomee.User property is:
tomee-plugin.catalina-base.

checkStarted

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.check-started.

classifier

String

-

(no description)User property is: tomee-plugin.classifier.

classpaths

List

-

(no description)

config

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/conf.User property is:
tomee-plugin.conf.

context

String

-

rename the current artifact

debug

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.debug.

debugPort

int

-

(no description)Default value is: 5005.User property is:
tomee-plugin.debugPort.

deployOpenEjbApplication

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.deploy-openejb-internal-application.

docBases

List

-

for TomEE and wars only, which docBase to use for this war.

ejbRemote

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.ejb-remote.

externalRepositories

List

-

for TomEE and wars only, add some external repositories to classloader.

forceReloadable

boolean

-

force webapp to be reloadableDefault value is: false.User property is:
tomee-plugin.force-reloadable.

javaagents

List

-

(no description)

keepServerXmlAsthis

boolean

-

(Removed since 7.0.0)Default value is: false.User property is:
tomee-plugin.keep-server-xml.

lib

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/lib.User property is:
tomee-plugin.lib.

libDir

String

-

relative to tomee.base.Default value is: lib.

libs

List

-

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

mainDir

File

-

(no description)Default value is: $\{project.basedir}/src/main.

password

String

-

(no description)User property is: tomee-plugin.pwd.

quickSession

boolean

-

use a real random instead of secure random. saves few ms at
startup.Default value is: true.User property is:
tomee-plugin.quick-session.

realm

String

-

(no description)User property is: tomee-plugin.realm.

removeDefaultWebapps

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-default-webapps.

removeTomeeWebapp

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-tomee-webapps.

simpleLog

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.simple-log.

skipCurrentProject

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.skipCurrentProject.

skipWarResources

boolean

-

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.Default value is: false.User property is:
tomee-plugin.skipWarResources.

systemVariables

Map

-

(no description)

target

File

-

(no description)Default value is: $\{project.build.directory}.

tomeeAjpPort

int

-

(no description)Default value is: 8009.User property is:
tomee-plugin.ajp.

tomeeAlreadyInstalled

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.exiting.

tomeeArtifactId

String

-

(no description)Default value is: apache-tomee.User property is:
tomee-plugin.artifactId.

tomeeClassifier

String

-

(no description)Default value is: webprofile.User property is:
tomee-plugin.classifier.

tomeeGroupId

String

-

(no description)Default value is: org.apache.openejb.User property is:
tomee-plugin.groupId.

tomeeHost

String

-

(no description)Default value is: localhost.User property is:
tomee-plugin.host.

tomeeHttpPort

int

-

(no description)Default value is: 8080.User property is:
tomee-plugin.http.

tomeeHttpsPort

Integer

-

(no description)User property is: tomee-plugin.https.

tomeeShutdownCommand

String

-

(no description)Default value is: SHUTDOWN.User property is:
tomee-plugin.shutdown-command.

tomeeShutdownPort

int

-

(no description)Default value is: 8005.User property is:
tomee-plugin.shutdown.

tomeeVersion

String

-

(no description)Default value is: -1.User property is:
tomee-plugin.version.

useConsole

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.use-console.

useOpenEJB

boolean

-

use openejb-standalone automatically instead of TomEEDefault value is:
false.User property is: tomee-plugin.openejb.

user

String

-

(no description)User property is: tomee-plugin.user.

warFile

File

-

(no description)Default value is:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}.

webappClasses

File

-

(no description)Default value is: $\{project.build.outputDirectory}.User
property is: tomee-plugin.webappClasses.

webappDefaultConfig

boolean

-

forcing nice default for war development (WEB-INF/classes and web
resources)Default value is: false.User property is:
tomee-plugin.webappDefaultConfig.

webappDir

String

-

relative to tomee.base.Default value is: webapps.

webappResources

File

-

(no description)Default value is:
$\{project.basedir}/src/main/webapp.User property is:
tomee-plugin.webappResources.

webapps

List

-

(no description)

zip

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.zip.

zipFile

File

-

(no description)Default value is:
\({project.build.directory}/\)\{project.build.finalName}.zip.User
property is: tomee-plugin.zip-file.

Parameter Details

apacheRepos:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.apache-repos

Default: snapshots

appDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: apps

apps:

(no description)

Type: java.util.List

Required: No

args:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.args

attach:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.attach

Default: true

bin:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.bin

Default: $\{project.basedir}/src/main/tomee/bin

catalinaBase:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.catalina-base

Default: $\{project.build.directory}/apache-tomee

checkStarted:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.check-started

Default: false

classifier:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.classifier

classpaths:

(no description)

Type: java.util.List

Required: No

config:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.conf

Default: $\{project.basedir}/src/main/tomee/conf

context:

rename the current artifact

Type: java.lang.String

Required: No

debug:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.debug

Default: false

debugPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.debugPort

Default: 5005

deployOpenEjbApplication:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.deploy-openejb-internal-application

Default: false

docBases:

for TomEE and wars only, which docBase to use for this war.

Type: java.util.List

Required: No

ejbRemote:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.ejb-remote

Default: true

externalRepositories:

for TomEE and wars only, add some external repositories to classloader.

Type: java.util.List

Required: No

forceReloadable:

force webapp to be reloadable

Type: boolean

Required: No

User Property: tomee-plugin.force-reloadable

Default: false

javaagents:

(no description)

Type: java.util.List

Required: No

keepServerXmlAsthis:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.keep-server-xml

Default: false

lib:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.lib

Default: $\{project.basedir}/src/main/tomee/lib

libDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: lib

libs:

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

Type: java.util.List

Required: No

mainDir:

(no description)

Type: java.io.File

Required: No

Default: $\{project.basedir}/src/main

password:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.pwd

quickSession:

use a real random instead of secure random. saves few ms at startup.

Type: boolean

Required: No

User Property: tomee-plugin.quick-session

Default: true

realm:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.realm

removeDefaultWebapps:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-default-webapps

Default: true

removeTomeeWebapp:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-tomee-webapps

Default: true

simpleLog:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.simple-log

Default: false

skipCurrentProject:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.skipCurrentProject

Default: false

skipWarResources:

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.

Type: boolean

Required: No

User Property: tomee-plugin.skipWarResources

Default: false

systemVariables:

(no description)

Type: java.util.Map

Required: No

target:

(no description)

Type: java.io.File

Required: No

Default: $\{project.build.directory}

tomeeAjpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.ajp

Default: 8009

tomeeAlreadyInstalled:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.exiting

Default: false

tomeeArtifactId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.artifactId

Default: apache-tomee

tomeeClassifier:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.classifier

Default: webprofile

tomeeGroupId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.groupId

Default: org.apache.openejb

tomeeHost:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.host

Default: localhost

tomeeHttpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.http

Default: 8080

tomeeHttpsPort:

(no description)

Type: java.lang.Integer

Required: No

User Property: tomee-plugin.https

tomeeShutdownCommand:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.shutdown-command

Default: SHUTDOWN

tomeeShutdownPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.shutdown

Default: 8005

tomeeVersion:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.version

Default: -1

useConsole:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.use-console

Default: true

useOpenEJB:

use openejb-standalone automatically instead of TomEE

Type: boolean

Required: No

User Property: tomee-plugin.openejb

Default: false

user:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.user

warFile:

(no description)

Type: java.io.File

Required: No

Default:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}

webappClasses:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappClasses

Default: $\{project.build.outputDirectory}

webappDefaultConfig:

forcing nice default for war development (WEB-INF/classes and web
resources)

Type: boolean

Required: No

User Property: tomee-plugin.webappDefaultConfig

Default: false

webappDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: webapps

webappResources:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappResources

Default: $\{project.basedir}/src/main/webapp

webapps:

(no description)

Type: java.util.List

Required: No

zip:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.zip

Default: true

zipFile:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.zip-file

Default:
\({project.build.directory}/\)\{project.build.finalName}.zip

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven-build-mojo--)

---

<a id="tomee-apache-org-tomee-10-1-docs-maven-configtest-mojo"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# null

tomee:configtest

Full name:

org.apache.openejb.maven:tomee-maven-plugin[:Current Version]:configtest

Description:

Run configtest Tomcat command.

Attributes:

Requires a Maven project to be executed.

Requires dependency resolution of artifacts in scope: runtime+system.

Requires dependency collection of artifacts in scope: runtime.

Optional Parameters

Name

Type

Since

Description

apacheRepos

String

-

(no description)Default value is: snapshots.User property is:
tomee-plugin.apache-repos.

appDir

String

-

relative to tomee.base.Default value is: apps.

apps

List

-

(no description)

args

String

-

(no description)User property is: tomee-plugin.args.

bin

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/bin.User property is:
tomee-plugin.bin.

catalinaBase

File

-

(no description)Default value is:
$\{project.build.directory}/apache-tomee.User property is:
tomee-plugin.catalina-base.

checkStarted

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.check-started.

classpaths

List

-

(no description)

config

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/conf.User property is:
tomee-plugin.conf.

context

String

-

rename the current artifact

debug

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.debug.

debugPort

int

-

(no description)Default value is: 5005.User property is:
tomee-plugin.debugPort.

deployOpenEjbApplication

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.deploy-openejb-internal-application.

docBases

List

-

for TomEE and wars only, which docBase to use for this war.

ejbRemote

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.ejb-remote.

externalRepositories

List

-

for TomEE and wars only, add some external repositories to classloader.

forceReloadable

boolean

-

force webapp to be reloadableDefault value is: false.User property is:
tomee-plugin.force-reloadable.

javaagents

List

-

(no description)

keepServerXmlAsthis

boolean

-

(Removed since 7.0.0)Default value is: false.User property is:
tomee-plugin.keep-server-xml.

lib

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/lib.User property is:
tomee-plugin.lib.

libDir

String

-

relative to tomee.base.Default value is: lib.

libs

List

-

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

mainDir

File

-

(no description)Default value is: $\{project.basedir}/src/main.

password

String

-

(no description)User property is: tomee-plugin.pwd.

quickSession

boolean

-

use a real random instead of secure random. saves few ms at
startup.Default value is: true.User property is:
tomee-plugin.quick-session.

realm

String

-

(no description)User property is: tomee-plugin.realm.

removeDefaultWebapps

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-default-webapps.

removeTomeeWebapp

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-tomee-webapps.

simpleLog

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.simple-log.

skipCurrentProject

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.skipCurrentProject.

skipWarResources

boolean

-

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.Default value is: false.User property is:
tomee-plugin.skipWarResources.

systemVariables

Map

-

(no description)

target

File

-

(no description)Default value is: $\{project.build.directory}.

tomeeAjpPort

int

-

(no description)Default value is: 8009.User property is:
tomee-plugin.ajp.

tomeeAlreadyInstalled

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.exiting.

tomeeArtifactId

String

-

(no description)Default value is: apache-tomee.User property is:
tomee-plugin.artifactId.

tomeeClassifier

String

-

(no description)Default value is: webprofile.User property is:
tomee-plugin.classifier.

tomeeGroupId

String

-

(no description)Default value is: org.apache.openejb.User property is:
tomee-plugin.groupId.

tomeeHost

String

-

(no description)Default value is: localhost.User property is:
tomee-plugin.host.

tomeeHttpPort

int

-

(no description)Default value is: 8080.User property is:
tomee-plugin.http.

tomeeHttpsPort

Integer

-

(no description)User property is: tomee-plugin.https.

tomeeShutdownCommand

String

-

(no description)Default value is: SHUTDOWN.User property is:
tomee-plugin.shutdown-command.

tomeeShutdownPort

int

-

(no description)Default value is: 8005.User property is:
tomee-plugin.shutdown.

tomeeVersion

String

-

(no description)Default value is: -1.User property is:
tomee-plugin.version.

useConsole

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.use-console.

useOpenEJB

boolean

-

use openejb-standalone automatically instead of TomEEDefault value is:
false.User property is: tomee-plugin.openejb.

user

String

-

(no description)User property is: tomee-plugin.user.

warFile

File

-

(no description)Default value is:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}.

webappClasses

File

-

(no description)Default value is: $\{project.build.outputDirectory}.User
property is: tomee-plugin.webappClasses.

webappDefaultConfig

boolean

-

forcing nice default for war development (WEB-INF/classes and web
resources)Default value is: false.User property is:
tomee-plugin.webappDefaultConfig.

webappDir

String

-

relative to tomee.base.Default value is: webapps.

webappResources

File

-

(no description)Default value is:
$\{project.basedir}/src/main/webapp.User property is:
tomee-plugin.webappResources.

webapps

List

-

(no description)

Parameter Details

apacheRepos:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.apache-repos

Default: snapshots

appDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: apps

apps:

(no description)

Type: java.util.List

Required: No

args:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.args

bin:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.bin

Default: $\{project.basedir}/src/main/tomee/bin

catalinaBase:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.catalina-base

Default: $\{project.build.directory}/apache-tomee

checkStarted:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.check-started

Default: false

classpaths:

(no description)

Type: java.util.List

Required: No

config:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.conf

Default: $\{project.basedir}/src/main/tomee/conf

context:

rename the current artifact

Type: java.lang.String

Required: No

debug:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.debug

Default: false

debugPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.debugPort

Default: 5005

deployOpenEjbApplication:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.deploy-openejb-internal-application

Default: false

docBases:

for TomEE and wars only, which docBase to use for this war.

Type: java.util.List

Required: No

ejbRemote:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.ejb-remote

Default: true

externalRepositories:

for TomEE and wars only, add some external repositories to classloader.

Type: java.util.List

Required: No

forceReloadable:

force webapp to be reloadable

Type: boolean

Required: No

User Property: tomee-plugin.force-reloadable

Default: false

javaagents:

(no description)

Type: java.util.List

Required: No

keepServerXmlAsthis:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.keep-server-xml

Default: false

lib:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.lib

Default: $\{project.basedir}/src/main/tomee/lib

libDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: lib

libs:

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

Type: java.util.List

Required: No

mainDir:

(no description)

Type: java.io.File

Required: No

Default: $\{project.basedir}/src/main

password:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.pwd

quickSession:

use a real random instead of secure random. saves few ms at startup.

Type: boolean

Required: No

User Property: tomee-plugin.quick-session

Default: true

realm:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.realm

removeDefaultWebapps:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-default-webapps

Default: true

removeTomeeWebapp:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-tomee-webapps

Default: true

simpleLog:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.simple-log

Default: false

skipCurrentProject:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.skipCurrentProject

Default: false

skipWarResources:

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.

Type: boolean

Required: No

User Property: tomee-plugin.skipWarResources

Default: false

systemVariables:

(no description)

Type: java.util.Map

Required: No

target:

(no description)

Type: java.io.File

Required: No

Default: $\{project.build.directory}

tomeeAjpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.ajp

Default: 8009

tomeeAlreadyInstalled:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.exiting

Default: false

tomeeArtifactId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.artifactId

Default: apache-tomee

tomeeClassifier:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.classifier

Default: webprofile

tomeeGroupId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.groupId

Default: org.apache.openejb

tomeeHost:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.host

Default: localhost

tomeeHttpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.http

Default: 8080

tomeeHttpsPort:

(no description)

Type: java.lang.Integer

Required: No

User Property: tomee-plugin.https

tomeeShutdownCommand:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.shutdown-command

Default: SHUTDOWN

tomeeShutdownPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.shutdown

Default: 8005

tomeeVersion:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.version

Default: -1

useConsole:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.use-console

Default: true

useOpenEJB:

use openejb-standalone automatically instead of TomEE

Type: boolean

Required: No

User Property: tomee-plugin.openejb

Default: false

user:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.user

warFile:

(no description)

Type: java.io.File

Required: No

Default:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}

webappClasses:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappClasses

Default: $\{project.build.outputDirectory}

webappDefaultConfig:

forcing nice default for war development (WEB-INF/classes and web
resources)

Type: boolean

Required: No

User Property: tomee-plugin.webappDefaultConfig

Default: false

webappDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: webapps

webappResources:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappResources

Default: $\{project.basedir}/src/main/webapp

webapps:

(no description)

Type: java.util.List

Required: No

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven-configtest-mojo--)

---

<a id="tomee-apache-org-tomee-10-1-docs-maven-debug-mojo"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# null

tomee:debug

Full name:

org.apache.openejb.maven:tomee-maven-plugin[:Current Version]:debug

Description:

As run but with debug activated.

Attributes:

Requires a Maven project to be executed.

Requires dependency resolution of artifacts in scope: runtime+system.

Requires dependency collection of artifacts in scope: runtime.

Optional Parameters

Name

Type

Since

Description

apacheRepos

String

-

(no description)Default value is: snapshots.User property is:
tomee-plugin.apache-repos.

appDir

String

-

relative to tomee.base.Default value is: apps.

apps

List

-

(no description)

args

String

-

(no description)User property is: tomee-plugin.args.

bin

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/bin.User property is:
tomee-plugin.bin.

catalinaBase

File

-

(no description)Default value is:
$\{project.build.directory}/apache-tomee.User property is:
tomee-plugin.catalina-base.

checkStarted

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.check-started.

classpaths

List

-

(no description)

config

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/conf.User property is:
tomee-plugin.conf.

context

String

-

rename the current artifact

debug

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.debug.

debugPort

int

-

(no description)Default value is: 5005.User property is:
tomee-plugin.debugPort.

deployOpenEjbApplication

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.deploy-openejb-internal-application.

docBases

List

-

for TomEE and wars only, which docBase to use for this war.

ejbRemote

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.ejb-remote.

externalRepositories

List

-

for TomEE and wars only, add some external repositories to classloader.

forceReloadable

boolean

-

force webapp to be reloadableDefault value is: false.User property is:
tomee-plugin.force-reloadable.

javaagents

List

-

(no description)

keepServerXmlAsthis

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.keep-server-xml.

lib

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/lib.User property is:
tomee-plugin.lib.

libDir

String

-

relative to tomee.base.Default value is: lib.

libs

List

-

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

mainDir

File

-

(no description)Default value is: $\{project.basedir}/src/main.

password

String

-

(no description)User property is: tomee-plugin.pwd.

quickSession

boolean

-

use a real random instead of secure random. saves few ms at
startup.Default value is: true.User property is:
tomee-plugin.quick-session.

realm

String

-

(no description)User property is: tomee-plugin.realm.

reloadOnUpdate

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.reload-on-update.

removeDefaultWebapps

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-default-webapps.

removeTomeeWebapp

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-tomee-webapps.

simpleLog

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.simple-log.

skipCurrentProject

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.skipCurrentProject.

skipWarResources

boolean

-

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.Default value is: false.User property is:
tomee-plugin.skipWarResources.

synchronization

Synchronization

-

(no description)

synchronizations

List

-

(no description)

systemVariables

Map

-

(no description)

target

File

-

(no description)Default value is: $\{project.build.directory}.

tomeeAjpPort

int

-

(no description)Default value is: 8009.User property is:
tomee-plugin.ajp.

tomeeAlreadyInstalled

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.exiting.

tomeeArtifactId

String

-

(no description)Default value is: apache-tomee.User property is:
tomee-plugin.artifactId.

tomeeClassifier

String

-

(no description)Default value is: webprofile.User property is:
tomee-plugin.classifier.

tomeeGroupId

String

-

(no description)Default value is: org.apache.openejb.User property is:
tomee-plugin.groupId.

tomeeHost

String

-

(no description)Default value is: localhost.User property is:
tomee-plugin.host.

tomeeHttpPort

int

-

(no description)Default value is: 8080.User property is:
tomee-plugin.http.

tomeeHttpsPort

Integer

-

(no description)User property is: tomee-plugin.https.

tomeeShutdownCommand

String

-

(no description)Default value is: SHUTDOWN.User property is:
tomee-plugin.shutdown-command.

tomeeShutdownPort

int

-

(no description)Default value is: 8005.User property is:
tomee-plugin.shutdown.

tomeeVersion

String

-

(no description)Default value is: -1.User property is:
tomee-plugin.version.

useConsole

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.use-console.

useOpenEJB

boolean

-

use openejb-standalone automatically instead of TomEEDefault value is:
false.User property is: tomee-plugin.openejb.

user

String

-

(no description)User property is: tomee-plugin.user.

warFile

File

-

(no description)Default value is:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}.

webappClasses

File

-

(no description)Default value is: $\{project.build.outputDirectory}.User
property is: tomee-plugin.webappClasses.

webappDefaultConfig

boolean

-

forcing nice default for war development (WEB-INF/classes and web
resources)Default value is: false.User property is:
tomee-plugin.webappDefaultConfig.

webappDir

String

-

relative to tomee.base.Default value is: webapps.

webappResources

File

-

(no description)Default value is:
$\{project.basedir}/src/main/webapp.User property is:
tomee-plugin.webappResources.

webapps

List

-

(no description)

Parameter Details

apacheRepos:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.apache-repos

Default: snapshots

appDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: apps

apps:

(no description)

Type: java.util.List

Required: No

args:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.args

bin:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.bin

Default: $\{project.basedir}/src/main/tomee/bin

catalinaBase:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.catalina-base

Default: $\{project.build.directory}/apache-tomee

checkStarted:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.check-started

Default: false

classpaths:

(no description)

Type: java.util.List

Required: No

config:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.conf

Default: $\{project.basedir}/src/main/tomee/conf

context:

rename the current artifact

Type: java.lang.String

Required: No

debug:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.debug

Default: false

debugPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.debugPort

Default: 5005

deployOpenEjbApplication:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.deploy-openejb-internal-application

Default: false

docBases:

for TomEE and wars only, which docBase to use for this war.

Type: java.util.List

Required: No

ejbRemote:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.ejb-remote

Default: true

externalRepositories:

for TomEE and wars only, add some external repositories to classloader.

Type: java.util.List

Required: No

forceReloadable:

force webapp to be reloadable

Type: boolean

Required: No

User Property: tomee-plugin.force-reloadable

Default: false

javaagents:

(no description)

Type: java.util.List

Required: No

keepServerXmlAsthis:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.keep-server-xml

Default: false

lib:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.lib

Default: $\{project.basedir}/src/main/tomee/lib

libDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: lib

libs:

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

Type: java.util.List

Required: No

mainDir:

(no description)

Type: java.io.File

Required: No

Default: $\{project.basedir}/src/main

password:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.pwd

quickSession:

use a real random instead of secure random. saves few ms at startup.

Type: boolean

Required: No

User Property: tomee-plugin.quick-session

Default: true

realm:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.realm

reloadOnUpdate:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.reload-on-update

Default: false

removeDefaultWebapps:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-default-webapps

Default: true

removeTomeeWebapp:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-tomee-webapps

Default: true

simpleLog:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.simple-log

Default: false

skipCurrentProject:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.skipCurrentProject

Default: false

skipWarResources:

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.

Type: boolean

Required: No

User Property: tomee-plugin.skipWarResources

Default: false

synchronization:

(no description)

Type: org.apache.openejb.maven.plugin.Synchronization

Required: No

synchronizations:

(no description)

Type: java.util.List

Required: No

systemVariables:

(no description)

Type: java.util.Map

Required: No

target:

(no description)

Type: java.io.File

Required: No

Default: $\{project.build.directory}

tomeeAjpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.ajp

Default: 8009

tomeeAlreadyInstalled:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.exiting

Default: false

tomeeArtifactId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.artifactId

Default: apache-tomee

tomeeClassifier:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.classifier

Default: webprofile

tomeeGroupId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.groupId

Default: org.apache.openejb

tomeeHost:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.host

Default: localhost

tomeeHttpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.http

Default: 8080

tomeeHttpsPort:

(no description)

Type: java.lang.Integer

Required: No

User Property: tomee-plugin.https

tomeeShutdownCommand:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.shutdown-command

Default: SHUTDOWN

tomeeShutdownPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.shutdown

Default: 8005

tomeeVersion:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.version

Default: -1

useConsole:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.use-console

Default: true

useOpenEJB:

use openejb-standalone automatically instead of TomEE

Type: boolean

Required: No

User Property: tomee-plugin.openejb

Default: false

user:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.user

warFile:

(no description)

Type: java.io.File

Required: No

Default:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}

webappClasses:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappClasses

Default: $\{project.build.outputDirectory}

webappDefaultConfig:

forcing nice default for war development (WEB-INF/classes and web
resources)

Type: boolean

Required: No

User Property: tomee-plugin.webappDefaultConfig

Default: false

webappDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: webapps

webappResources:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappResources

Default: $\{project.basedir}/src/main/webapp

webapps:

(no description)

Type: java.util.List

Required: No

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven-debug-mojo--)

---

<a id="tomee-apache-org-tomee-10-1-docs-maven-deploy-mojo"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# null

tomee:deploy

Full name:

org.apache.openejb.maven:tomee-maven-plugin[:Current Version]:deploy

Description:

Simply deploy an application in a running TomEE

Attributes:

Requires a Maven project to be executed.

Requires dependency resolution of artifacts in scope: runtime.

Requires dependency collection of artifacts in scope: runtime.

Required Parameters

Name

Type

Since

Description

path

String

-

(no description)User property is: tomee-plugin.archive.

Optional Parameters

Name

Type

Since

Description

password

String

-

(no description)User property is: tomee-plugin.pwd.

realm

String

-

(no description)User property is: tomee-plugin.realm.

systemVariables

Map

-

(no description)

tomeeHost

String

-

(no description)Default value is: localhost.User property is:
tomee-plugin.host.

tomeeHttpPort

int

-

(no description)Default value is: 8080.User property is:
tomee-plugin.http.

useBinaries

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.binary.

user

String

-

(no description)User property is: tomee-plugin.user.

Parameter Details

password:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.pwd

path:

(no description)

Type: java.lang.String

Required: Yes

User Property: tomee-plugin.archive

realm:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.realm

systemVariables:

(no description)

Type: java.util.Map

Required: No

tomeeHost:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.host

Default: localhost

tomeeHttpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.http

Default: 8080

useBinaries:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.binary

Default: false

user:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.user

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven-deploy-mojo--)

---

<a id="tomee-apache-org-tomee-10-1-docs-maven-exec-mojo"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# null

tomee:exec

Full name:

org.apache.openejb.maven:tomee-maven-plugin[:Current Version]:exec

Description:

(no description)

Attributes:

Requires a Maven project to be executed.

Requires dependency resolution of artifacts in scope: runtime+system.

Requires dependency collection of artifacts in scope: runtime.

Optional Parameters

Name

Type

Since

Description

apacheRepos

String

-

(no description)Default value is: snapshots.User property is:
tomee-plugin.apache-repos.

appDir

String

-

relative to tomee.base.Default value is: apps.

apps

List

-

(no description)

args

String

-

(no description)User property is: tomee-plugin.args.

attach

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.attach.

bin

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/bin.User property is:
tomee-plugin.bin.

catalinaBase

File

-

(no description)Default value is:
$\{project.build.directory}/apache-tomee.User property is:
tomee-plugin.catalina-base.

checkStarted

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.check-started.

classifier

String

-

(no description)User property is: tomee-plugin.classifier.

classpaths

List

-

(no description)

config

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/conf.User property is:
tomee-plugin.conf.

context

String

-

rename the current artifact

debug

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.debug.

debugPort

int

-

(no description)Default value is: 5005.User property is:
tomee-plugin.debugPort.

deployOpenEjbApplication

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.deploy-openejb-internal-application.

distributionName

String

-

(no description)Default value is: tomee.zip.User property is:
tomee-plugin.distribution-name.

docBases

List

-

for TomEE and wars only, which docBase to use for this war.

ejbRemote

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.ejb-remote.

execFile

File

-

(no description)Default value is:
\({project.build.directory}/\)\{project.build.finalName}-exec.jar.User
property is: tomee-plugin.exec-file.

externalRepositories

List

-

for TomEE and wars only, add some external repositories to classloader.

forceReloadable

boolean

-

force webapp to be reloadableDefault value is: false.User property is:
tomee-plugin.force-reloadable.

javaagents

List

-

(no description)

keepServerXmlAsthis

boolean

-

(Removed since 7.0.0)Default value is: false.User property is:
tomee-plugin.keep-server-xml.

lib

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/lib.User property is:
tomee-plugin.lib.

libDir

String

-

relative to tomee.base.Default value is: lib.

libs

List

-

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

mainDir

File

-

(no description)Default value is: $\{project.basedir}/src/main.

password

String

-

(no description)User property is: tomee-plugin.pwd.

quickSession

boolean

-

use a real random instead of secure random. saves few ms at
startup.Default value is: true.User property is:
tomee-plugin.quick-session.

realm

String

-

(no description)User property is: tomee-plugin.realm.

removeDefaultWebapps

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-default-webapps.

removeTomeeWebapp

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-tomee-webapps.

runnerClass

String

-

(no description)Default value is:
org.apache.openejb.maven.plugin.runner.ExecRunner.User property is:
tomee-plugin.runner-class.

runtimeWorkingDir

String

-

(no description)Default value is: .distribution.User property is:
tomee-plugin.runtime-working-dir.

script

String

-

(no description)Default value is: bin/catalina[.sh|.bat].User property
is: tomee-plugin.script.

simpleLog

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.simple-log.

skipCurrentProject

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.skipCurrentProject.

skipWarResources

boolean

-

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.Default value is: false.User property is:
tomee-plugin.skipWarResources.

systemVariables

Map

-

(no description)

target

File

-

(no description)Default value is: $\{project.build.directory}.

tomeeAjpPort

int

-

(no description)Default value is: 8009.User property is:
tomee-plugin.ajp.

tomeeAlreadyInstalled

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.exiting.

tomeeArtifactId

String

-

(no description)Default value is: apache-tomee.User property is:
tomee-plugin.artifactId.

tomeeClassifier

String

-

(no description)Default value is: webprofile.User property is:
tomee-plugin.classifier.

tomeeGroupId

String

-

(no description)Default value is: org.apache.openejb.User property is:
tomee-plugin.groupId.

tomeeHost

String

-

(no description)Default value is: localhost.User property is:
tomee-plugin.host.

tomeeHttpPort

int

-

(no description)Default value is: 8080.User property is:
tomee-plugin.http.

tomeeHttpsPort

Integer

-

(no description)User property is: tomee-plugin.https.

tomeeShutdownCommand

String

-

(no description)Default value is: SHUTDOWN.User property is:
tomee-plugin.shutdown-command.

tomeeShutdownPort

int

-

(no description)Default value is: 8005.User property is:
tomee-plugin.shutdown.

tomeeVersion

String

-

(no description)Default value is: -1.User property is:
tomee-plugin.version.

useConsole

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.use-console.

useOpenEJB

boolean

-

use openejb-standalone automatically instead of TomEEDefault value is:
false.User property is: tomee-plugin.openejb.

user

String

-

(no description)User property is: tomee-plugin.user.

warFile

File

-

(no description)Default value is:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}.

webappClasses

File

-

(no description)Default value is: $\{project.build.outputDirectory}.User
property is: tomee-plugin.webappClasses.

webappDefaultConfig

boolean

-

forcing nice default for war development (WEB-INF/classes and web
resources)Default value is: false.User property is:
tomee-plugin.webappDefaultConfig.

webappDir

String

-

relative to tomee.base.Default value is: webapps.

webappResources

File

-

(no description)Default value is:
$\{project.basedir}/src/main/webapp.User property is:
tomee-plugin.webappResources.

webapps

List

-

(no description)

zip

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.zip.

zipFile

File

-

(no description)Default value is:
\({project.build.directory}/\)\{project.build.finalName}.zip.User
property is: tomee-plugin.zip-file.

Parameter Details

apacheRepos:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.apache-repos

Default: snapshots

appDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: apps

apps:

(no description)

Type: java.util.List

Required: No

args:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.args

attach:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.attach

Default: true

bin:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.bin

Default: $\{project.basedir}/src/main/tomee/bin

catalinaBase:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.catalina-base

Default: $\{project.build.directory}/apache-tomee

checkStarted:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.check-started

Default: false

classifier:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.classifier

classpaths:

(no description)

Type: java.util.List

Required: No

config:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.conf

Default: $\{project.basedir}/src/main/tomee/conf

context:

rename the current artifact

Type: java.lang.String

Required: No

debug:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.debug

Default: false

debugPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.debugPort

Default: 5005

deployOpenEjbApplication:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.deploy-openejb-internal-application

Default: false

distributionName:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.distribution-name

Default: tomee.zip

docBases:

for TomEE and wars only, which docBase to use for this war.

Type: java.util.List

Required: No

ejbRemote:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.ejb-remote

Default: true

execFile:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.exec-file

Default:
\({project.build.directory}/\)\{project.build.finalName}-exec.jar

externalRepositories:

for TomEE and wars only, add some external repositories to classloader.

Type: java.util.List

Required: No

forceReloadable:

force webapp to be reloadable

Type: boolean

Required: No

User Property: tomee-plugin.force-reloadable

Default: false

javaagents:

(no description)

Type: java.util.List

Required: No

keepServerXmlAsthis:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.keep-server-xml

Default: false

lib:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.lib

Default: $\{project.basedir}/src/main/tomee/lib

libDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: lib

libs:

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

Type: java.util.List

Required: No

mainDir:

(no description)

Type: java.io.File

Required: No

Default: $\{project.basedir}/src/main

password:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.pwd

quickSession:

use a real random instead of secure random. saves few ms at startup.

Type: boolean

Required: No

User Property: tomee-plugin.quick-session

Default: true

realm:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.realm

removeDefaultWebapps:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-default-webapps

Default: true

removeTomeeWebapp:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-tomee-webapps

Default: true

runnerClass:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.runner-class

Default: org.apache.openejb.maven.plugin.runner.ExecRunner

runtimeWorkingDir:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.runtime-working-dir

Default: .distribution

script:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.script

Default: bin/catalina[.sh|.bat]

simpleLog:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.simple-log

Default: false

skipCurrentProject:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.skipCurrentProject

Default: false

skipWarResources:

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.

Type: boolean

Required: No

User Property: tomee-plugin.skipWarResources

Default: false

systemVariables:

(no description)

Type: java.util.Map

Required: No

target:

(no description)

Type: java.io.File

Required: No

Default: $\{project.build.directory}

tomeeAjpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.ajp

Default: 8009

tomeeAlreadyInstalled:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.exiting

Default: false

tomeeArtifactId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.artifactId

Default: apache-tomee

tomeeClassifier:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.classifier

Default: webprofile

tomeeGroupId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.groupId

Default: org.apache.openejb

tomeeHost:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.host

Default: localhost

tomeeHttpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.http

Default: 8080

tomeeHttpsPort:

(no description)

Type: java.lang.Integer

Required: No

User Property: tomee-plugin.https

tomeeShutdownCommand:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.shutdown-command

Default: SHUTDOWN

tomeeShutdownPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.shutdown

Default: 8005

tomeeVersion:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.version

Default: -1

useConsole:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.use-console

Default: true

useOpenEJB:

use openejb-standalone automatically instead of TomEE

Type: boolean

Required: No

User Property: tomee-plugin.openejb

Default: false

user:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.user

warFile:

(no description)

Type: java.io.File

Required: No

Default:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}

webappClasses:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappClasses

Default: $\{project.build.outputDirectory}

webappDefaultConfig:

forcing nice default for war development (WEB-INF/classes and web
resources)

Type: boolean

Required: No

User Property: tomee-plugin.webappDefaultConfig

Default: false

webappDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: webapps

webappResources:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappResources

Default: $\{project.basedir}/src/main/webapp

webapps:

(no description)

Type: java.util.List

Required: No

zip:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.zip

Default: true

zipFile:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.zip-file

Default:
\({project.build.directory}/\)\{project.build.finalName}.zip

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven-exec-mojo--)

---

<a id="tomee-apache-org-tomee-10-1-docs-maven-help-mojo"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# null

tomee:help

Full name:

org.apache.openejb.maven:tomee-maven-plugin[:Current Version]:help

Description:

Display help information on tomee-maven-plugin. Call mvn tomee:help
-Ddetail=true -Dgoal=<goal-name> to display parameter details.

Attributes:

The goal is thread-safe and supports parallel builds.

Optional Parameters

Name

Type

Since

Description

detail

boolean

-

If true, display all settable properties for each goal.Default value is:
false.User property is: detail.

goal

String

-

The name of the goal for which to show help. If unspecified, all goals
will be displayed.User property is: goal.

indentSize

int

-

The number of spaces per indentation level, should be positive.Default
value is: 2.User property is: indentSize.

lineLength

int

-

The maximum length of a display line, should be positive.Default value
is: 80.User property is: lineLength.

Parameter Details

detail:

If true, display all settable properties for each goal.

Type: boolean

Required: No

User Property: detail

Default: false

goal:

The name of the goal for which to show help. If unspecified, all goals
will be displayed.

Type: java.lang.String

Required: No

User Property: goal

indentSize:

The number of spaces per indentation level, should be positive.

Type: int

Required: No

User Property: indentSize

Default: 2

lineLength:

The maximum length of a display line, should be positive.

Type: int

Required: No

User Property: lineLength

Default: 80

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven-help-mojo--)

---

<a id="tomee-apache-org-tomee-10-1-docs-maven-list-mojo"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# null

tomee:list

Full name:

org.apache.openejb.maven:tomee-maven-plugin[:Current Version]:list

Description:

Highly inspired from openejb command helper but with some different
data. List deployed EJB in a running TomEE.

Attributes:

Requires a Maven project to be executed.

Requires dependency resolution of artifacts in scope: runtime.

Requires dependency collection of artifacts in scope: runtime.

Optional Parameters

Name

Type

Since

Description

password

String

-

(no description)User property is: tomee-plugin.pwd.

realm

String

-

(no description)User property is: tomee-plugin.realm.

tomeeHost

String

-

(no description)Default value is: localhost.User property is:
tomee-plugin.host.

tomeeHttpPort

int

-

(no description)Default value is: 8080.User property is:
tomee-plugin.http.

user

String

-

(no description)User property is: tomee-plugin.user.

Parameter Details

password:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.pwd

realm:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.realm

tomeeHost:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.host

Default: localhost

tomeeHttpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.http

Default: 8080

user:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.user

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven-list-mojo--)

---

<a id="tomee-apache-org-tomee-10-1-docs-maven-run-mojo"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# null

tomee:run

Full name:

org.apache.openejb.maven:tomee-maven-plugin[:Current Version]:run

Description:

Start and wait for TomEE.

Attributes:

Requires a Maven project to be executed.

Requires dependency resolution of artifacts in scope: runtime+system.

Requires dependency collection of artifacts in scope: runtime.

Optional Parameters

Name

Type

Since

Description

apacheRepos

String

-

(no description)Default value is: snapshots.User property is:
tomee-plugin.apache-repos.

appDir

String

-

relative to tomee.base.Default value is: apps.

apps

List

-

(no description)

args

String

-

(no description)User property is: tomee-plugin.args.

bin

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/bin.User property is:
tomee-plugin.bin.

catalinaBase

File

-

(no description)Default value is:
$\{project.build.directory}/apache-tomee.User property is:
tomee-plugin.catalina-base.

checkStarted

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.check-started.

classpaths

List

-

(no description)

config

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/conf.User property is:
tomee-plugin.conf.

context

String

-

rename the current artifact

debug

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.debug.

debugPort

int

-

(no description)Default value is: 5005.User property is:
tomee-plugin.debugPort.

deployOpenEjbApplication

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.deploy-openejb-internal-application.

docBases

List

-

for TomEE and wars only, which docBase to use for this war.

ejbRemote

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.ejb-remote.

externalRepositories

List

-

for TomEE and wars only, add some external repositories to classloader.

forceReloadable

boolean

-

force webapp to be reloadableDefault value is: false.User property is:
tomee-plugin.force-reloadable.

javaagents

List

-

(no description)

keepServerXmlAsthis

boolean

-

(Removed since 7.0.0)Default value is: false.User property is:
tomee-plugin.keep-server-xml.

lib

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/lib.User property is:
tomee-plugin.lib.

libDir

String

-

relative to tomee.base.Default value is: lib.

libs

List

-

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

mainDir

File

-

(no description)Default value is: $\{project.basedir}/src/main.

password

String

-

(no description)User property is: tomee-plugin.pwd.

quickSession

boolean

-

use a real random instead of secure random. saves few ms at
startup.Default value is: true.User property is:
tomee-plugin.quick-session.

realm

String

-

(no description)User property is: tomee-plugin.realm.

reloadOnUpdate

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.reload-on-update.

removeDefaultWebapps

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-default-webapps.

removeTomeeWebapp

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-tomee-webapps.

simpleLog

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.simple-log.

skipCurrentProject

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.skipCurrentProject.

skipWarResources

boolean

-

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.Default value is: false.User property is:
tomee-plugin.skipWarResources.

synchronization

Synchronization

-

(no description)

synchronizations

List

-

(no description)

systemVariables

Map

-

(no description)

target

File

-

(no description)Default value is: $\{project.build.directory}.

tomeeAjpPort

int

-

(no description)Default value is: 8009.User property is:
tomee-plugin.ajp.

tomeeAlreadyInstalled

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.exiting.

tomeeArtifactId

String

-

(no description)Default value is: apache-tomee.User property is:
tomee-plugin.artifactId.

tomeeClassifier

String

-

(no description)Default value is: webprofile.User property is:
tomee-plugin.classifier.

tomeeGroupId

String

-

(no description)Default value is: org.apache.openejb.User property is:
tomee-plugin.groupId.

tomeeHost

String

-

(no description)Default value is: localhost.User property is:
tomee-plugin.host.

tomeeHttpPort

int

-

(no description)Default value is: 8080.User property is:
tomee-plugin.http.

tomeeHttpsPort

Integer

-

(no description)User property is: tomee-plugin.https.

tomeeShutdownCommand

String

-

(no description)Default value is: SHUTDOWN.User property is:
tomee-plugin.shutdown-command.

tomeeShutdownPort

int

-

(no description)Default value is: 8005.User property is:
tomee-plugin.shutdown.

tomeeVersion

String

-

(no description)Default value is: -1.User property is:
tomee-plugin.version.

useConsole

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.use-console.

useOpenEJB

boolean

-

use openejb-standalone automatically instead of TomEEDefault value is:
false.User property is: tomee-plugin.openejb.

user

String

-

(no description)User property is: tomee-plugin.user.

warFile

File

-

(no description)Default value is:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}.

webappClasses

File

-

(no description)Default value is: $\{project.build.outputDirectory}.User
property is: tomee-plugin.webappClasses.

webappDefaultConfig

boolean

-

forcing nice default for war development (WEB-INF/classes and web
resources)Default value is: false.User property is:
tomee-plugin.webappDefaultConfig.

webappDir

String

-

relative to tomee.base.Default value is: webapps.

webappResources

File

-

(no description)Default value is:
$\{project.basedir}/src/main/webapp.User property is:
tomee-plugin.webappResources.

webapps

List

-

(no description)

Parameter Details

apacheRepos:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.apache-repos

Default: snapshots

appDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: apps

apps:

(no description)

Type: java.util.List

Required: No

args:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.args

bin:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.bin

Default: $\{project.basedir}/src/main/tomee/bin

catalinaBase:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.catalina-base

Default: $\{project.build.directory}/apache-tomee

checkStarted:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.check-started

Default: false

classpaths:

(no description)

Type: java.util.List

Required: No

config:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.conf

Default: $\{project.basedir}/src/main/tomee/conf

context:

rename the current artifact

Type: java.lang.String

Required: No

debug:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.debug

Default: false

debugPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.debugPort

Default: 5005

deployOpenEjbApplication:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.deploy-openejb-internal-application

Default: false

docBases:

for TomEE and wars only, which docBase to use for this war.

Type: java.util.List

Required: No

ejbRemote:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.ejb-remote

Default: true

externalRepositories:

for TomEE and wars only, add some external repositories to classloader.

Type: java.util.List

Required: No

forceReloadable:

force webapp to be reloadable

Type: boolean

Required: No

User Property: tomee-plugin.force-reloadable

Default: false

javaagents:

(no description)

Type: java.util.List

Required: No

keepServerXmlAsthis:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.keep-server-xml

Default: false

lib:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.lib

Default: $\{project.basedir}/src/main/tomee/lib

libDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: lib

libs:

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

Type: java.util.List

Required: No

mainDir:

(no description)

Type: java.io.File

Required: No

Default: $\{project.basedir}/src/main

password:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.pwd

quickSession:

use a real random instead of secure random. saves few ms at startup.

Type: boolean

Required: No

User Property: tomee-plugin.quick-session

Default: true

realm:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.realm

reloadOnUpdate:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.reload-on-update

Default: false

removeDefaultWebapps:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-default-webapps

Default: true

removeTomeeWebapp:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-tomee-webapps

Default: true

simpleLog:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.simple-log

Default: false

skipCurrentProject:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.skipCurrentProject

Default: false

skipWarResources:

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.

Type: boolean

Required: No

User Property: tomee-plugin.skipWarResources

Default: false

synchronization:

(no description)

Type: org.apache.openejb.maven.plugin.Synchronization

Required: No

synchronizations:

(no description)

Type: java.util.List

Required: No

systemVariables:

(no description)

Type: java.util.Map

Required: No

target:

(no description)

Type: java.io.File

Required: No

Default: $\{project.build.directory}

tomeeAjpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.ajp

Default: 8009

tomeeAlreadyInstalled:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.exiting

Default: false

tomeeArtifactId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.artifactId

Default: apache-tomee

tomeeClassifier:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.classifier

Default: webprofile

tomeeGroupId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.groupId

Default: org.apache.openejb

tomeeHost:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.host

Default: localhost

tomeeHttpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.http

Default: 8080

tomeeHttpsPort:

(no description)

Type: java.lang.Integer

Required: No

User Property: tomee-plugin.https

tomeeShutdownCommand:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.shutdown-command

Default: SHUTDOWN

tomeeShutdownPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.shutdown

Default: 8005

tomeeVersion:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.version

Default: -1

useConsole:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.use-console

Default: true

useOpenEJB:

use openejb-standalone automatically instead of TomEE

Type: boolean

Required: No

User Property: tomee-plugin.openejb

Default: false

user:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.user

warFile:

(no description)

Type: java.io.File

Required: No

Default:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}

webappClasses:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappClasses

Default: $\{project.build.outputDirectory}

webappDefaultConfig:

forcing nice default for war development (WEB-INF/classes and web
resources)

Type: boolean

Required: No

User Property: tomee-plugin.webappDefaultConfig

Default: false

webappDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: webapps

webappResources:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappResources

Default: $\{project.basedir}/src/main/webapp

webapps:

(no description)

Type: java.util.List

Required: No

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven-run-mojo--)

---

<a id="tomee-apache-org-tomee-10-1-docs-maven-start-mojo"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# null

tomee:start

Full name:

org.apache.openejb.maven:tomee-maven-plugin[:Current Version]:start

Description:

Start and forget TomEE.

Attributes:

Requires a Maven project to be executed.

Requires dependency resolution of artifacts in scope: runtime+system.

Requires dependency collection of artifacts in scope: runtime.

Optional Parameters

Name

Type

Since

Description

apacheRepos

String

-

(no description)Default value is: snapshots.User property is:
tomee-plugin.apache-repos.

appDir

String

-

relative to tomee.base.Default value is: apps.

apps

List

-

(no description)

args

String

-

(no description)User property is: tomee-plugin.args.

bin

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/bin.User property is:
tomee-plugin.bin.

catalinaBase

File

-

(no description)Default value is:
$\{project.build.directory}/apache-tomee.User property is:
tomee-plugin.catalina-base.

checkStarted

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.check-started.

classpaths

List

-

(no description)

config

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/conf.User property is:
tomee-plugin.conf.

context

String

-

rename the current artifact

debug

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.debug.

debugPort

int

-

(no description)Default value is: 5005.User property is:
tomee-plugin.debugPort.

deployOpenEjbApplication

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.deploy-openejb-internal-application.

docBases

List

-

for TomEE and wars only, which docBase to use for this war.

ejbRemote

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.ejb-remote.

externalRepositories

List

-

for TomEE and wars only, add some external repositories to classloader.

forceReloadable

boolean

-

force webapp to be reloadableDefault value is: false.User property is:
tomee-plugin.force-reloadable.

javaagents

List

-

(no description)

keepServerXmlAsthis

boolean

-

(Removed since 7.0.0)Default value is: false.User property is:
tomee-plugin.keep-server-xml.

lib

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/lib.User property is:
tomee-plugin.lib.

libDir

String

-

relative to tomee.base.Default value is: lib.

libs

List

-

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

mainDir

File

-

(no description)Default value is: $\{project.basedir}/src/main.

password

String

-

(no description)User property is: tomee-plugin.pwd.

quickSession

boolean

-

use a real random instead of secure random. saves few ms at
startup.Default value is: true.User property is:
tomee-plugin.quick-session.

realm

String

-

(no description)User property is: tomee-plugin.realm.

reloadOnUpdate

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.reload-on-update.

removeDefaultWebapps

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-default-webapps.

removeTomeeWebapp

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-tomee-webapps.

simpleLog

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.simple-log.

skipCurrentProject

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.skipCurrentProject.

skipWarResources

boolean

-

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.Default value is: false.User property is:
tomee-plugin.skipWarResources.

synchronization

Synchronization

-

(no description)

synchronizations

List

-

(no description)

systemVariables

Map

-

(no description)

target

File

-

(no description)Default value is: $\{project.build.directory}.

tomeeAjpPort

int

-

(no description)Default value is: 8009.User property is:
tomee-plugin.ajp.

tomeeAlreadyInstalled

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.exiting.

tomeeArtifactId

String

-

(no description)Default value is: apache-tomee.User property is:
tomee-plugin.artifactId.

tomeeClassifier

String

-

(no description)Default value is: webprofile.User property is:
tomee-plugin.classifier.

tomeeGroupId

String

-

(no description)Default value is: org.apache.openejb.User property is:
tomee-plugin.groupId.

tomeeHost

String

-

(no description)Default value is: localhost.User property is:
tomee-plugin.host.

tomeeHttpPort

int

-

(no description)Default value is: 8080.User property is:
tomee-plugin.http.

tomeeHttpsPort

Integer

-

(no description)User property is: tomee-plugin.https.

tomeeShutdownCommand

String

-

(no description)Default value is: SHUTDOWN.User property is:
tomee-plugin.shutdown-command.

tomeeShutdownPort

int

-

(no description)Default value is: 8005.User property is:
tomee-plugin.shutdown.

tomeeVersion

String

-

(no description)Default value is: -1.User property is:
tomee-plugin.version.

useConsole

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.use-console.

useOpenEJB

boolean

-

use openejb-standalone automatically instead of TomEEDefault value is:
false.User property is: tomee-plugin.openejb.

user

String

-

(no description)User property is: tomee-plugin.user.

warFile

File

-

(no description)Default value is:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}.

webappClasses

File

-

(no description)Default value is: $\{project.build.outputDirectory}.User
property is: tomee-plugin.webappClasses.

webappDefaultConfig

boolean

-

forcing nice default for war development (WEB-INF/classes and web
resources)Default value is: false.User property is:
tomee-plugin.webappDefaultConfig.

webappDir

String

-

relative to tomee.base.Default value is: webapps.

webappResources

File

-

(no description)Default value is:
$\{project.basedir}/src/main/webapp.User property is:
tomee-plugin.webappResources.

webapps

List

-

(no description)

Parameter Details

apacheRepos:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.apache-repos

Default: snapshots

appDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: apps

apps:

(no description)

Type: java.util.List

Required: No

args:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.args

bin:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.bin

Default: $\{project.basedir}/src/main/tomee/bin

catalinaBase:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.catalina-base

Default: $\{project.build.directory}/apache-tomee

checkStarted:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.check-started

Default: false

classpaths:

(no description)

Type: java.util.List

Required: No

config:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.conf

Default: $\{project.basedir}/src/main/tomee/conf

context:

rename the current artifact

Type: java.lang.String

Required: No

debug:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.debug

Default: false

debugPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.debugPort

Default: 5005

deployOpenEjbApplication:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.deploy-openejb-internal-application

Default: false

docBases:

for TomEE and wars only, which docBase to use for this war.

Type: java.util.List

Required: No

ejbRemote:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.ejb-remote

Default: true

externalRepositories:

for TomEE and wars only, add some external repositories to classloader.

Type: java.util.List

Required: No

forceReloadable:

force webapp to be reloadable

Type: boolean

Required: No

User Property: tomee-plugin.force-reloadable

Default: false

javaagents:

(no description)

Type: java.util.List

Required: No

keepServerXmlAsthis:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.keep-server-xml

Default: false

lib:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.lib

Default: $\{project.basedir}/src/main/tomee/lib

libDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: lib

libs:

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

Type: java.util.List

Required: No

mainDir:

(no description)

Type: java.io.File

Required: No

Default: $\{project.basedir}/src/main

password:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.pwd

quickSession:

use a real random instead of secure random. saves few ms at startup.

Type: boolean

Required: No

User Property: tomee-plugin.quick-session

Default: true

realm:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.realm

reloadOnUpdate:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.reload-on-update

Default: false

removeDefaultWebapps:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-default-webapps

Default: true

removeTomeeWebapp:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-tomee-webapps

Default: true

simpleLog:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.simple-log

Default: false

skipCurrentProject:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.skipCurrentProject

Default: false

skipWarResources:

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.

Type: boolean

Required: No

User Property: tomee-plugin.skipWarResources

Default: false

synchronization:

(no description)

Type: org.apache.openejb.maven.plugin.Synchronization

Required: No

synchronizations:

(no description)

Type: java.util.List

Required: No

systemVariables:

(no description)

Type: java.util.Map

Required: No

target:

(no description)

Type: java.io.File

Required: No

Default: $\{project.build.directory}

tomeeAjpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.ajp

Default: 8009

tomeeAlreadyInstalled:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.exiting

Default: false

tomeeArtifactId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.artifactId

Default: apache-tomee

tomeeClassifier:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.classifier

Default: webprofile

tomeeGroupId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.groupId

Default: org.apache.openejb

tomeeHost:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.host

Default: localhost

tomeeHttpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.http

Default: 8080

tomeeHttpsPort:

(no description)

Type: java.lang.Integer

Required: No

User Property: tomee-plugin.https

tomeeShutdownCommand:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.shutdown-command

Default: SHUTDOWN

tomeeShutdownPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.shutdown

Default: 8005

tomeeVersion:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.version

Default: -1

useConsole:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.use-console

Default: true

useOpenEJB:

use openejb-standalone automatically instead of TomEE

Type: boolean

Required: No

User Property: tomee-plugin.openejb

Default: false

user:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.user

warFile:

(no description)

Type: java.io.File

Required: No

Default:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}

webappClasses:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappClasses

Default: $\{project.build.outputDirectory}

webappDefaultConfig:

forcing nice default for war development (WEB-INF/classes and web
resources)

Type: boolean

Required: No

User Property: tomee-plugin.webappDefaultConfig

Default: false

webappDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: webapps

webappResources:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappResources

Default: $\{project.basedir}/src/main/webapp

webapps:

(no description)

Type: java.util.List

Required: No

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven-start-mojo--)

---

<a id="tomee-apache-org-tomee-10-1-docs-maven-stop-mojo"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# null

tomee:stop

Full name:

org.apache.openejb.maven:tomee-maven-plugin[:Current Version]:stop

Description:

Stop a TomEE started with start command.

Attributes:

Requires a Maven project to be executed.

Requires dependency resolution of artifacts in scope: runtime+system.

Requires dependency collection of artifacts in scope: runtime.

Optional Parameters

Name

Type

Since

Description

apacheRepos

String

-

(no description)Default value is: snapshots.User property is:
tomee-plugin.apache-repos.

appDir

String

-

relative to tomee.base.Default value is: apps.

apps

List

-

(no description)

args

String

-

(no description)User property is: tomee-plugin.args.

bin

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/bin.User property is:
tomee-plugin.bin.

catalinaBase

File

-

(no description)Default value is:
$\{project.build.directory}/apache-tomee.User property is:
tomee-plugin.catalina-base.

checkStarted

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.check-started.

classpaths

List

-

(no description)

config

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/conf.User property is:
tomee-plugin.conf.

context

String

-

rename the current artifact

debug

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.debug.

debugPort

int

-

(no description)Default value is: 5005.User property is:
tomee-plugin.debugPort.

deployOpenEjbApplication

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.deploy-openejb-internal-application.

docBases

List

-

for TomEE and wars only, which docBase to use for this war.

ejbRemote

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.ejb-remote.

externalRepositories

List

-

for TomEE and wars only, add some external repositories to classloader.

forceReloadable

boolean

-

force webapp to be reloadableDefault value is: false.User property is:
tomee-plugin.force-reloadable.

javaagents

List

-

(no description)

keepServerXmlAsthis

boolean

-

(Removed since 7.0.0)Default value is: false.User property is:
tomee-plugin.keep-server-xml.

lib

File

-

(no description)Default value is:
$\{project.basedir}/src/main/tomee/lib.User property is:
tomee-plugin.lib.

libDir

String

-

relative to tomee.base.Default value is: lib.

libs

List

-

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

mainDir

File

-

(no description)Default value is: $\{project.basedir}/src/main.

password

String

-

(no description)User property is: tomee-plugin.pwd.

quickSession

boolean

-

use a real random instead of secure random. saves few ms at
startup.Default value is: true.User property is:
tomee-plugin.quick-session.

realm

String

-

(no description)User property is: tomee-plugin.realm.

removeDefaultWebapps

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-default-webapps.

removeTomeeWebapp

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.remove-tomee-webapps.

simpleLog

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.simple-log.

skipCurrentProject

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.skipCurrentProject.

skipWarResources

boolean

-

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.Default value is: false.User property is:
tomee-plugin.skipWarResources.

systemVariables

Map

-

(no description)

target

File

-

(no description)Default value is: $\{project.build.directory}.

tomeeAjpPort

int

-

(no description)Default value is: 8009.User property is:
tomee-plugin.ajp.

tomeeAlreadyInstalled

boolean

-

(no description)Default value is: false.User property is:
tomee-plugin.exiting.

tomeeArtifactId

String

-

(no description)Default value is: apache-tomee.User property is:
tomee-plugin.artifactId.

tomeeClassifier

String

-

(no description)Default value is: webprofile.User property is:
tomee-plugin.classifier.

tomeeGroupId

String

-

(no description)Default value is: org.apache.openejb.User property is:
tomee-plugin.groupId.

tomeeHost

String

-

(no description)Default value is: localhost.User property is:
tomee-plugin.host.

tomeeHttpPort

int

-

(no description)Default value is: 8080.User property is:
tomee-plugin.http.

tomeeHttpsPort

Integer

-

(no description)User property is: tomee-plugin.https.

tomeeShutdownCommand

String

-

(no description)Default value is: SHUTDOWN.User property is:
tomee-plugin.shutdown-command.

tomeeShutdownPort

int

-

(no description)Default value is: 8005.User property is:
tomee-plugin.shutdown.

tomeeVersion

String

-

(no description)Default value is: -1.User property is:
tomee-plugin.version.

useConsole

boolean

-

(no description)Default value is: true.User property is:
tomee-plugin.use-console.

useOpenEJB

boolean

-

use openejb-standalone automatically instead of TomEEDefault value is:
false.User property is: tomee-plugin.openejb.

user

String

-

(no description)User property is: tomee-plugin.user.

warFile

File

-

(no description)Default value is:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}.

webappClasses

File

-

(no description)Default value is: $\{project.build.outputDirectory}.User
property is: tomee-plugin.webappClasses.

webappDefaultConfig

boolean

-

forcing nice default for war development (WEB-INF/classes and web
resources)Default value is: false.User property is:
tomee-plugin.webappDefaultConfig.

webappDir

String

-

relative to tomee.base.Default value is: webapps.

webappResources

File

-

(no description)Default value is:
$\{project.basedir}/src/main/webapp.User property is:
tomee-plugin.webappResources.

webapps

List

-

(no description)

Parameter Details

apacheRepos:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.apache-repos

Default: snapshots

appDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: apps

apps:

(no description)

Type: java.util.List

Required: No

args:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.args

bin:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.bin

Default: $\{project.basedir}/src/main/tomee/bin

catalinaBase:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.catalina-base

Default: $\{project.build.directory}/apache-tomee

checkStarted:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.check-started

Default: false

classpaths:

(no description)

Type: java.util.List

Required: No

config:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.conf

Default: $\{project.basedir}/src/main/tomee/conf

context:

rename the current artifact

Type: java.lang.String

Required: No

debug:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.debug

Default: false

debugPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.debugPort

Default: 5005

deployOpenEjbApplication:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.deploy-openejb-internal-application

Default: false

docBases:

for TomEE and wars only, which docBase to use for this war.

Type: java.util.List

Required: No

ejbRemote:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.ejb-remote

Default: true

externalRepositories:

for TomEE and wars only, add some external repositories to classloader.

Type: java.util.List

Required: No

forceReloadable:

force webapp to be reloadable

Type: boolean

Required: No

User Property: tomee-plugin.force-reloadable

Default: false

javaagents:

(no description)

Type: java.util.List

Required: No

keepServerXmlAsthis:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.keep-server-xml

Default: false

lib:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.lib

Default: $\{project.basedir}/src/main/tomee/lib

libDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: lib

libs:

supported formats: -→ groupId:artifactId:version…​ -→
unzip:groupId:artifactId:version…​ -→ remove:prefix (often prefix =
artifactId)

Type: java.util.List

Required: No

mainDir:

(no description)

Type: java.io.File

Required: No

Default: $\{project.basedir}/src/main

password:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.pwd

quickSession:

use a real random instead of secure random. saves few ms at startup.

Type: boolean

Required: No

User Property: tomee-plugin.quick-session

Default: true

realm:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.realm

removeDefaultWebapps:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-default-webapps

Default: true

removeTomeeWebapp:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.remove-tomee-webapps

Default: true

simpleLog:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.simple-log

Default: false

skipCurrentProject:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.skipCurrentProject

Default: false

skipWarResources:

when you set docBases to src/main/webapp setting it to true will allow
hot refresh.

Type: boolean

Required: No

User Property: tomee-plugin.skipWarResources

Default: false

systemVariables:

(no description)

Type: java.util.Map

Required: No

target:

(no description)

Type: java.io.File

Required: No

Default: $\{project.build.directory}

tomeeAjpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.ajp

Default: 8009

tomeeAlreadyInstalled:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.exiting

Default: false

tomeeArtifactId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.artifactId

Default: apache-tomee

tomeeClassifier:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.classifier

Default: webprofile

tomeeGroupId:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.groupId

Default: org.apache.openejb

tomeeHost:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.host

Default: localhost

tomeeHttpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.http

Default: 8080

tomeeHttpsPort:

(no description)

Type: java.lang.Integer

Required: No

User Property: tomee-plugin.https

tomeeShutdownCommand:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.shutdown-command

Default: SHUTDOWN

tomeeShutdownPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.shutdown

Default: 8005

tomeeVersion:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.version

Default: -1

useConsole:

(no description)

Type: boolean

Required: No

User Property: tomee-plugin.use-console

Default: true

useOpenEJB:

use openejb-standalone automatically instead of TomEE

Type: boolean

Required: No

User Property: tomee-plugin.openejb

Default: false

user:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.user

warFile:

(no description)

Type: java.io.File

Required: No

Default:
\({project.build.directory}/\)\{project.build.finalName}.$\{project.packaging}

webappClasses:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappClasses

Default: $\{project.build.outputDirectory}

webappDefaultConfig:

forcing nice default for war development (WEB-INF/classes and web
resources)

Type: boolean

Required: No

User Property: tomee-plugin.webappDefaultConfig

Default: false

webappDir:

relative to tomee.base.

Type: java.lang.String

Required: No

Default: webapps

webappResources:

(no description)

Type: java.io.File

Required: No

User Property: tomee-plugin.webappResources

Default: $\{project.basedir}/src/main/webapp

webapps:

(no description)

Type: java.util.List

Required: No

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven-stop-mojo--)

---

<a id="tomee-apache-org-tomee-10-1-docs-maven-undeploy-mojo"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# null

tomee:undeploy

Full name:

org.apache.openejb.maven:tomee-maven-plugin[:Current Version]:undeploy

Description:

Simply undeploy an application in a running TomEE

Attributes:

Requires a Maven project to be executed.

Requires dependency resolution of artifacts in scope: runtime.

Requires dependency collection of artifacts in scope: runtime.

Required Parameters

Name

Type

Since

Description

path

String

-

(no description)User property is: tomee-plugin.archive.

Optional Parameters

Name

Type

Since

Description

password

String

-

(no description)User property is: tomee-plugin.pwd.

realm

String

-

(no description)User property is: tomee-plugin.realm.

tomeeHost

String

-

(no description)Default value is: localhost.User property is:
tomee-plugin.host.

tomeeHttpPort

int

-

(no description)Default value is: 8080.User property is:
tomee-plugin.http.

user

String

-

(no description)User property is: tomee-plugin.user.

Parameter Details

password:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.pwd

path:

(no description)

Type: java.lang.String

Required: Yes

User Property: tomee-plugin.archive

realm:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.realm

tomeeHost:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.host

Default: localhost

tomeeHttpPort:

(no description)

Type: int

Required: No

User Property: tomee-plugin.http

Default: 8080

user:

(no description)

Type: java.lang.String

Required: No

User Property: tomee-plugin.user

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-maven-undeploy-mojo--)

---

<a id="tomee-apache-org-tomee-10-1-docs-messagedrivencontainer-config"></a>

# Apache TomEE

```xml
<Container id="myMessageDrivenContainer" type="MESSAGE">
    activationSpecClass = org.apache.activemq.ra.ActiveMQActivationSpec
    instanceLimit = 10
    messageListenerInterface = jakarta.jms.MessageListener
    resourceAdapter = Default JMS Resource Adapter
</Container>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-microprofile-jwt"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

# TomEE MicroProfile JWT

Apache TomEE supports [MicroProfile JWT 2.0](https://download.eclipse.org/microprofile/microprofile-jwt-auth-2.0/microprofile-jwt-auth-spec-2.0.html), which allows applications to be secured using JWTs.
JWTs may be either:

- Signed ([JWS](https://www.rfc-editor.org/rfc/rfc7515)) tokens, verified via a public key you configure
- Encrypted ([JWE](https://www.rfc-editor.org/rfc/rfc7516)) tokens, decrypted via a private key you configure

Key types supported:

- RSA
- Elliptic curve (EC)

Key formats supported:

- PEM PKCS1 and PKCS8
- JWK and JWKS
- OpenSSH Public Key (`.pub` and `authorized_keys`)
- SSH 2 Public Key

Signature algorithms supported:

- RS256
- RS384
- RS512
- ES256
- ES384
- ES512

Decryption algorithms supported:

- RSA-OAEP
- RSA-OAEP-256
- ECDH-ES
- ECDH-ES+A128KW
- ECDH-ES+A192KW
- ECDH-ES+A256KW

## MicroProfile JWT Configuration Properties

Specifying the keys for verifying or decrypting JWTs is done via a `META-INF/microprofile-config.properties` and the following MP JWT 2.0 Configuration Properties.

| Property | Type | Description |
| --- | --- | --- |
| mp.jwt.verify.publickey | String | The contents of any valid public key file.  Allows the public key to be inlined into themicroprofile-config.propertieswithout the need for separate files. |
| mp.jwt.verify.publickey.location | String | The location of any valid public key file.  Can be specified as a relative path on disk, relative path on the classpath, or valid URL such afile:,http:, orhttps:.   Custom URLs are supported as long as there is a correspondingjava.net.URLStreamHandlerinstalled in the JVM. |
| mp.jwt.decrypt.key.location | String | The location of any valid private key file.  Can be specified as a relative path on disk, relative path on the classpath, or valid URL such afile:,http:, orhttps:.   Custom URLs are supported as long as there is a correspondingjava.net.URLStreamHandlerinstalled in the JVM. |
| mp.jwt.token.header | String | The name of the HTTP Request header where clients will JWTs.  The default value isAuthorization, but may be any header name including a customer header.  Specifying a value ofCookiewill enable JWTs to be passed to the server as HTTP Cookies. |
| mp.jwt.token.cookie | String | Whenmp.jwt.token.header=Cookiethe value ofmp.jwt.token.cookiespecifies the exact cookie name holding the JWT.  The default isBearer. |
| mp.jwt.verify.audiences | String | A comma-delimited list of allowable values for the JWTaudclaim.  When specified, a JWT with anaudthat does not appear in the allowed list will result in an HTTP401. When not specified, allaudvalues are accepted including noaudat all. |
| mp.jwt.verify.issuer | String | The expected value of theissJWT claim. When specified, a JWT with anissthat does not match the configured value will result in an HTTP401. When not specified, anyissvalues are accepted including noissat all. |
| mp.jwt.verify.publickey.algorithm | String | The expected value of thealgJWT header. When specified, a JWT with analgthat does not match the configured value will result in an HTTP401and no signature check will occur.  When not specified, any of the supported signature algorithms are considered applicable. |

## TomEE JWT Configuration Properties

In addition to the standard MicroProfile JWT configuration properties above, the `META-INF/microprofile-config.properties` may contain any of the following TomEE-specific configuration properties.

| Property | Type | Description |
| --- | --- | --- |
| mp.jwt.tomee.allow.no-expis deprecated please usetomee.mp.jwt.allow.no-expproperty instead | tomee.mp.jwt.allow.no-exp | Boolean |
| Disables enforcing theexptime of the JWT.  Useful if JWTs are also verified by an API Gateway or proxy before reaching the server.  The default value isfalse | tomee.jwt.verify.publickey.cache | Boolean |
| Enables public keys to be supplied after deployment has occurred or refreshed periodically at runtime.  Useful for when keys are supplied via anhttporhttpsURL.  Settingtomee.jwt.verify.publickey.cache=trueis required for any of the subsequenttomee.jwt.verify.publickey.cache.*properties to take effect.  Default value istrueorhttporhttpsURLs andfalsefor all other key locations. | tomee.jwt.verify.publickey.cache.initialRetryDelay | Duration |
| Should the first attempt to load keys fail, this setting specifies how long we should wait before trying again.  An exponential backoff will occur and the delay will double on each subsequent retry.  This allows retrying to be very aggressive in the event of a temporary issue, but prevents overloading the server supplying the keys.  The default value is2 seconds | tomee.jwt.verify.publickey.cache.maxRetryDelay | Duration |
| Allows the retry attempts to eventually reach a fixed rate after a certain maximum delay is reached.  This property disables the exponential backoff once the specified maximum delay is reached.  All subsequent retries will happen at the interval specified.  To disable exponential backoff entirely, setinitialRetryDelayandmaxRetryDelayto the same value.   The default value is1 hour | tomee.jwt.verify.publickey.cache.accessTimeout | Duration |
| Specifies the maximum time incoming HTTP Requests with JWTs will block and wait for keys when no keys are available.  If specified time is reached, callers will receive an HTTP401.  The default value is30 seconds | tomee.jwt.verify.publickey.cache.refreshInterval | Duration |
| Specifies how frequently TomEE should check the configured location for new keys.  Should any refresh fail or result in no valid keys, the keys currently in use are not replaced and no subsequent attempts are made until the next refresh interval.  The default value is1 day | tomee.jwt.decrypt.key.cache | Boolean |
| Enables private keys to be supplied after deployment has occurred or refreshed periodically at runtime.  Useful for when keys are supplied via anhttporhttpsURL.  Settingtomee.jwt.decrypt.key.cache=trueis required for any of the subsequenttomee.jwt.decrypt.key.cache.*properties to take effect.  Default value istrueorhttporhttpsURLs andfalsefor all other key locations. | tomee.jwt.decrypt.key.cache.initialRetryDelay | Duration |
| Should the first attempt to load keys fail, this setting specifies how long we should wait before trying again.  An exponential backoff will occur and the delay will double on each subsequent retry.  This allows retrying to be very aggressive in the event of a temporary issue, but prevents overloading the server supplying the keys.  The default value is2 seconds | tomee.jwt.decrypt.key.cache.maxRetryDelay | Duration |
| Allows the retry attempts to eventually reach a fixed rate after a certain maximum delay is reached.  This property disables the exponential backoff once the specified maximum delay is reached.  All subsequent retries will happen at the interval specified. To disable exponential backoff entirely, setinitialRetryDelayandmaxRetryDelayto the same value.   The default value is1 hour | tomee.jwt.decrypt.key.cache.accessTimeout | Duration |
| Specifies the maximum time incoming HTTP Requests with JWTs will block and wait for keys when no keys are available.  If specified time is reached, callers will receive an HTTP401.  The default value is30 seconds | tomee.jwt.decrypt.key.cache.refreshInterval | Duration |

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../../privacy-policy.html)

##### [Documentation](../../../latest/docs/)

- [How to configure](../../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../../latest/docs/admin/file-layout.html)
- [Testing](../../../latest/docs/developer/testing/index.html)
- [Clustering](../../../latest/docs/admin/cluster/index.html)

##### [Examples](../../../latest/examples/)

- [CDI Interceptor](../../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../../latest/examples/rest-cdi.html)
- [EJB](../../../latest/examples/ejb-examples.html)
- [JSF](../../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../../community/index.html)

- [Contributors](../../../community/contributors.html)
- [Social](../../../community/social.html)
- [Sources](../../../community/sources.html)

##### [Security](../../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../../docs.html)
- [Community](../../../community/index.html)
- [Security](../../../security/security.html)
- [Downloads](../../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-microprofile-jwt--)

---

<a id="tomee-apache-org-tomee-10-1-docs-multicast-discovery"></a>

# Apache TomEE

```properties
server      = org.apache.openejb.server.discovery.MulticastDiscoveryAgent
bind        = 239.255.2.3
port        = 6142
disabled    = true
group       = default
```

---

<a id="tomee-apache-org-tomee-10-1-docs-multiple-business-interface-hazzards"></a>

# Apache TomEE

```java
import junit.framework.TestCase;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * @version $Rev$ $Date$
 */
public class ExceptionTest extends TestCase {

    public void test() throws Exception {
    ClassLoader classLoader = this.getClass().getClassLoader();
        Class[]
```

---

<a id="tomee-apache-org-tomee-10-1-docs-multipoint-considerations"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Multipoint Considerations

## Network size

The general disadvantage of this topology is the number of connections
required. The number of connections for the network of servers is equal
to `(n * n - n) / 2`, where n is the number of servers. For example,
with 5 servers you need 10 connections, with 10 servers you need 45
connections, and with 50 servers you need 1225 connections. This is of
course the number of connections across the entire network, each
individual server only needs `n - 1` connections.

The handling of these sockets is all asynchronous Java NIO code which
allows the server to handle many connections (all of them) with one
thread. From a pure threading perspective, the option is extremely
efficient with just one thread to listen and broadcast to many peers.

## Double connect

It is possible in this process that two servers learn of each other at
the same time and each attempts to connect to the other simultaneously,
resulting in two connections between the same two servers. When this
happens both servers will detect the extra connection and one of the
connections will be dropped and one will be kept. In practice this race
condition rarely happens and can be avoided almost entirely by fanning
out server startup by as little as 100 milliseconds.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-multipoint-considerations--)

---

<a id="tomee-apache-org-tomee-10-1-docs-multipoint-discovery"></a>

# Apache TomEE

```properties
server      = org.apache.openejb.server.discovery.MultipointDiscoveryAgent
bind        = 127.0.0.1
port        = 4212
disabled    = false
initialServers = 192.168.1.20:4212, 192.168.1.30:4212, 192.168.1.40:4212
```

---

<a id="tomee-apache-org-tomee-10-1-docs-multipoint-recommendations"></a>

# Apache TomEE

```java
!/bin/bash

OPENEJB_HOME=/opt/openejb-3.1.3
INITIAL_LIST=$(cat /some/shared/directory/our_initial_servers.txt)

$OPENEJB_HOME/bin/openejb start -Dmultipoint.initialServers=$INITIAL_LIST
```

---

<a id="tomee-apache-org-tomee-10-1-docs-multipulse-discovery"></a>

# Apache TomEE

```properties
server      = org.apache.openejb.server.discovery.MulticastPulseAgent
bind        = 239.255.2.3
port        = 6142
disabled    = true
group       = default
```

---

<a id="tomee-apache-org-tomee-10-1-docs-new-in-openejb-3-0"></a>

# Apache TomEE

```java
import java.net.URI;
import java.io.File;
import java.util.Date;

@Stateful
public class MyBean {
    @Resource URI blog;
    @Resource Date birthday;
    @Resource File homeDirectory;
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-openejb-3"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# OpenEJB 3

# Past, Present, and Future

The goal of OpenEJB 3 is to merge our past, present, and future into one
codebase. OpenEJB 3 will take the excellent features in OpenEJB 1.0
(tomcat integration, testability, embeddability, ease of use, etc.), move
towards an IoC architecture based on Gbean.org and Spring, bring in the
OpenEJB 2 code, and implement the EJB 3.0 specification.

# The Plan

We will start on OpenEJB 3 by taking the 1.0 code (pretty much the same
as 0.9.2), merging in the 2.0 code, and ensuring that the entire time
the code we write is code you can use! We will never drop a feature,
even temporarily. We will start from code that users are now using and
always keep, maintain, and improve those features as we add new
features. Releasing early and often.

## Past

OpenEJB 1.0 (from 0.9.2 lineage) has some great features and many people
that depend on them. Tomcat integration, Collapsed EARs, Container
Driven Testing, easy embedding, and other features make OpenEJB a unique
EJB implementation. We’re going to take this code, kill all the static
old-school techniques, modernize it with and IoC architecture based on
the gbean.org kernel. The gbean kernel is an IoC kernel compatible with
both Spring and Geronimo.

## Present

OpenEJB 2.0 is an awesome fast implementation of EJB 2.1 that runs in
Apache Geronimo. As the gbean.org kernel is both Spring and Geronimo
compatible, it provides a great way for us to take the
Geronimo-compatible EJB containers and deployers in OpenEJB 2 and start
hammering them out and releasing them to long-time OpenEJB users. It
will also allow people using OpenEJB to start experimenting with
Spring’s sophisticated IoC features.

## Future

EJB 3.0 is a new direction for EJB and we’re going to do it with style.
A focus on simplicity is where OpenEJB shines. Combining the EJB 3.0
Simplified specification with our existing lightweight features, like
Container Driven Testing, is just the beginning. We plan to go way
beyond the planned additions and into areas the J2EE spec groups won’t
go such as deployment descriptors with attributes, simpler packaging,
more flexible classloader setup, more powerful IoC support, simpler web
services support and more.

# Release on Day One

Keep it working, keep it progressing, keep releasing. The 3.0 version
number won’t be the finishing line, but the starting line. Our work will
start out as 3.0 on day one and keep incrementing the version number as
we get further along our feature list. The EJB 3.0 spec is not completed
and the OpenEJB 3.0 code line will be equally dynamic and best suited
for adventurous developers who enjoy reading release notes and
participating on user lists. There will be an incredible focus on
keeping things stable enough to use the entire time as we work towards
feature completion.

The effect of all this is that you get a fixed-up, far more extensible,
version of the code you are already using delivered to you right away.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-openejb-3--)

---

<a id="tomee-apache-org-tomee-10-1-docs-openejb-binaries"></a>

# Apache TomEE

```xml
<settings>
  <servers>
    <server>
      <id>apache.snapshots.https</id>
      <username>yourapacheid</username>
      <password>yourapachepass</password>
    </server>
    <server>
      <id>apache.releases.https</id>
      <username>yourapacheid</username>
      <password>yourapachepass</password>
    </server>
  </servers>
</settings>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-openejb-eclipse-plugin"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# OpenEJB Eclipse Plugin

# OpenEJB Eclipse Plugin

## Overview

The OpenEJB plugin for Eclipse provides the ability to run an OpenEJB
standalone server and deploy projects directly from within the IDE,
using functionality provided by the Eclipse Web Tools Project (WTP).
Additionally, the plugin also provides the capability to read an
`ejb-jar.xml` (and optionally an `openejb-jar.xml`) file and automatically
add the corresponding EJB 3 annotations to your code automatically.

[Installation](#tomee-apache-org-tomee-10-1-docs-installation)
[Building from source](building-from-source.html)
[Generating EJB 3 annotations](#tomee-apache-org-tomee-10-1-docs-generating-ejb-3-annotations)
[Running a standalone
OpenEJB server](#tomee-apache-org-tomee-10-1-docs-running-a-standalone-openejb-server)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-openejb-eclipse-plugin--)

---

<a id="tomee-apache-org-tomee-10-1-docs-openejb-jsr-107-integration"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# OpenEJB JSR-107 Integration

# OpenEJB JSR-107 (JCACHE) Integration

This page is for the collaboration for those involved with the
integration of JSR-107 into OpenEJB.

## Overview

The idea here is to add a caching layer to OpenEJB. The overall
objective is to improve performance in OpenEJB where applicable through
caching EJBs.

## Status

Dain and myself (Jeremy) have deciphered the JSR-107 spec and how I am
working on the first crude integration of JCACHE into OpenEJB. Anyone
interested in helping or providing any feedback/suggestions, please
contact me via the developer mailing list.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-openejb-jsr-107-integration--)

---

<a id="tomee-apache-org-tomee-10-1-docs-openejb-xml"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# openejb.xml

## Overview

The openejb.xml is the main configuration file for the container system
and its services such as transaction, security, and data sources.

The format is a mix of xml and properties inspired by the format of the
httpd configuration file. Basically:

```xml
<tag id="">
  ...properties...
</tag>
```

Such as:

```xml
<Resource id="MyDataSource" type="DataSource">
  username foo
  password bar
</Resource>
```

*Note the space*. White space is a valid name/value pair separator in
any java properties file (along with semicolon). So the above is
equivalent to:

```xml
<Resource id="MyDataSource" type="DataSource">
  username = foo
  password = bar
</Resource>
```

You are free to use white space, ":", or "=" for your name/value pair
separator with no effect on OpenEJB.

## Property Defaults and Overriding

The openejb.xml file itself functions as an override, default values are
specified via other means (service-jar.xml files in the classpath),
therefore you only need to specify property values here for 2 reasons:
1. you wish to for documentation purposes 2. you need to change the
default value

The default openejb.xml file has most of the useful properties for each
component explicitly listed with default values for documentation
purposes. It is safe to delete them and be assured that no behavior will
change if a smaller config file is desired.

Overriding can also be done via the command line or plain Java system
properties. See [System Properties](#tomee-apache-org-tomee-10-1-docs-system-properties) for
details.

## What properties are available?

To know what properties can be overridden the './bin/openejb properties'
command is very useful: see [Properties Tool](#tomee-apache-org-tomee-10-1-docs-properties-tool)

Its function is to connect to a running server and print a canonical
list of all properties OpenEJB can see via the various means of
configuration. When sending requests for help to the users list or jira,
it is highly encouraged to send the output of this tool with your
message.

## Not configurable via openejb.xml

The only thing not yet configurable via this file are ServerServices due
to OpenEJB’s embeddable nature and resulting long-standing tradition of
keeping the container system separate from the server layer. This may
change someday, but until then ServerServices are configurable via
conf/.properties files such as conf/ejbd.properties to configure the
main protocol that services EJB client requests.

The format of those properties files is greatly adapted from the xinet.d
style of configuration and even shares similar functionality and
properties such as host-based authorization (HBA) via the 'only\_from'
property.

## Restoring openejb.xml to the defaults

To restore this file to its original default state, you can simply
delete it or rename it and OpenEJB will see it’s missing and unpack
another openejb.xml into the conf/ directory when it starts.

This is not only handy for recovering from a non-functional config, but
also for upgrading as OpenEJB will not overwrite your existing
configuration file should you choose to unpack a new distro over the
top of an old one — this style of upgrade is safe provided you move
your old lib/ directory first.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-openejb-xml--)

---

<a id="tomee-apache-org-tomee-10-1-docs-openjpa"></a>

# Apache TomEE

```xml
<persistence xmlns="http://java.sun.com/xml/ns/persistence" version="1.0">

  <persistence-unit name="movie-unit">
    <jta-data-source>movieDatabase</jta-data-source>
    <non-jta-data-source>movieDatabaseUnmanaged</non-jta-data-source>
    <class>org.superbiz.injection.jpa.Movie</class>

    <properties>
      <property name="openjpa.jdbc.SynchronizeMappings" value="buildSchema(ForeignKeys=true)"/>
    </properties>
  </persistence-unit>
</persistence>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-persistence-context"></a>

# Apache TomEE

```java
package org.superbiz;

import jakarta.persistence.PersistenceContext;
import jakarta.persistence.EntityManager;
import jakarta.ejb.Stateless;
import javax.naming.InitialContext;

@Stateless
@PersistenceContext(name = "myFooEntityManager", unitName = "foo-unit")
public class MyBean implements MyInterface {

    @PersistenceContext(unitName = "bar-unit")
    private EntityManager myBarEntityManager;

    public void someBusinessMethod() throws Exception {
        if (myBarEntityManager == null) throw new NullPointerException("myBarEntityManager not injected");

        // Both can be looked up from JNDI as well
        InitialContext context = new InitialContext();
        EntityManager fooEntityManager = (EntityManager) context.lookup("java:comp/env/myFooEntityManager");
        EntityManager barEntityManager = (EntityManager) context.lookup("java:comp/env/org.superbiz.MyBean/myBarEntityManager");
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-persistence-unit-ref"></a>

# Apache TomEE

```java
package org.superbiz;

import jakarta.persistence.PersistenceUnit;
import jakarta.persistence.EntityManagerFactory;
import jakarta.ejb.Stateless;
import javax.naming.InitialUnit;

@Stateless
public class MyBean implements MyInterface {

    @PersistenceUnit(unitName = "bar-unit")
    private EntityManagerFactory myBarEntityManagerFactory;

    public void someBusinessMethod() throws Exception {
        if (myBarEntityManagerFactory == null) throw new NullPointerException("myBarEntityManagerFactory not injected");

        // Both can be looked up from JNDI as well
        InitialContext unit = new InitialContext();
        EntityManagerFactory barEntityManagerFactory = (EntityManagerFactory) context.lookup("java:comp/env/org.superbiz.MyBean/myBarEntityManagerFactory");
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-properties-listing"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# System Properties Listing

## OpenEJB system properties

Name

Value

Description

openejb.embedded.remotable

bool

activate or not the remote services when available

bind, <service prefix>.port, <service prefix>.disabled, <service

prefix>.threads

host or IP, port, bool

override the host. Available for ejbd and httpejbd services (used by
jaxws and jaxrs), number of thread to manage requests

openejb.embedded.initialcontext.close

LOGOUT or DESTROY

configure the hook called when closing the initial context. Useful when
starting OpenEJB from a new InitialContext([properties]) instantiation.
By default, it simply logs out the logged user if it exists. DESTROY
means clean the container.

jakarta.persistence.provider

string

override the JPA provider value

jakarta.persistence.transactionType

string

override the transaction type for persistence contexts

jakarta.persistence.jtaDataSource

string

override the JTA datasource value for persistence contexts

jakarta.persistence.nonJtaDataSource

string

override the non JTA datasource value for persistence contexts

openejb.descriptors.output

bool

dump memory deployment descriptors. Can be used to set complete metadata
to true and avoid scanning when starting the container or to check the
used configuration.

openejb.deployments.classpath.require.descriptor

CLIENT or EJB

can allow to filter what you want to scan (client modules or ejb
modules)

openejb.descriptors.output.folder

path

where to dump deployement descriptors if activated.

openejb.strict.interface.declaration

bool

add some validations on session beans (spec validations in particular).
false by default.

openejb.conf.file or openejb.configuration

string

OpenEJB configuration file path

openejb.debuggable-vm-hackery

bool

remove JMS information from deployment

openejb.validation.skip

bool

skip the validations done when OpenEJB deploys beans

openejb.deployments.classpath.ear

bool

deploy the classpath as an ear

openejb.webservices.enabled

bool

activate or not webservices

openejb.validation.output.level

TERSE or MEDIUM or VERBOSE

level of the logs used to report validation errors

openejb.user.mbeans.list

- or a list of classes separated by ,

  +
  list of mbeans to deploy automatically

  +
  openejb.deploymentId.format

  +
  composition (+string) of {ejbName} {ejbType} {ejbClass} and
  \{ejbClass.simpleName}

  +
  default {ejbName}. The format to use to deploy ejbs.

  +
  openejb.deployments.classpath

  +
  bool

  +
  whether or not deploy from classpath

  +
  openejb.deployments.classpath.include and
  openejb.deployments.classpath.exclude

  +
  regex

  +
  regex to filter the scanned classpath (when you are in this case)

  +
  openejb.deployments.package.include and
  openejb.deployments.package.exclude

  +
  regex

  +
  regex to filter scanned packages

  +
  openejb.autocreate.jta-datasource-from-non-jta-one

  +
  bool

  +
  whether or not auto create the jta datasource if it doesn’t exist but a
  non jta datasource exists. Useful when using hibernate to be able to get
  a real non jta datasource.

  +
  openejb.altdd.prefix

  +
  string

  +
  prefix use for altDD (example test to use a test.ejb-jar.xml).

  +
  org.apache.openejb.default.system.interceptors

  +
  list of interceptor (qualified names) separated by a comma or a space

  +
  add these interceptor on all beans

  +
  openejb.jndiname.strategy.class

  +
  class name

  +
  an implementation of
  org.apache.openejb.assembler.classic.JndiBuilder.JndiNameStrategy

  +
  openejb.jndiname.failoncollision

  +
  bool

  +
  if a NameAlreadyBoundException is thrown or not when 2 EJBs have the
  same name

  +
  openejb.jndiname.format

  +
  composition (+string) of these properties: ejbType, ejbClass,
  ejbClass.simpleName, ejbClass.packageName, ejbName, deploymentId,
  interfaceType, interfaceType.annotationName,
  interfaceType.annotationNameLC, interfaceType.xmlName,
  interfaceType.xmlNameCc, interfaceType.openejbLegacyName,
  interfaceClass, interfaceClass.simpleName, interfaceClass.packageName

  +
  default {deploymentId}\{interfaceType.annotationName}. Change the name
  used for the ejb.

  +
  openejb.org.quartz.threadPool.class

  +
  class qualified name which implements org.quartz.spi.ThreadPool

  +
  the thread pool used by quartz (used to manage ejb timers)

  +
  openejb.localcopy

  +
  bool

  +
  default true. whether or not copy EJB arguments[/method/interface] for
  remote invocations.

  +
  openejb.cxf.jax-rs.providers

  +
  the list of the qualified name of the JAX-RS providers separated by
  comma or space. Note: to specify a provider for a specific service
  suffix its class qualified name by ".providers", the value follow the
  same rules. Note 2: default is a shortcut for jaxb and json providers.

  +
  openejb.wsAddress.format

  +
  composition (+string) of {ejbJarId}, ejbDeploymentId, ejbType,
  ejbClass, ejbClass.simpleName, ejbName, portComponentName, wsdlPort,
  wsdlService

  +
  default /{ejbDeploymentId}. The WS name format.

  +
  org.apache.openejb.server.webservices.saaj.provider

  +
  axis2, sun or null

  +
  specified the saaj configuration

  +
  [<uppercase service name>.]<service id>.<name> or [<uppercase service
  name>.]<service id>

  +
  whatever is supported (generally string, int …​)

  +
  set this value to the corresponding service. example:
  [EnterpriseBean.]<ejb-name>.activation.<property>,
  [PERSISTENCEUNIT.]<persistence unit name>.<property>, [RESOURCE.]<name>

  +
  log4j.category.OpenEJB.options

  +
  DEBUG, INFO, …​

  +
  active one OpenEJB log level. need log4j in the classpath

  +
  openejb.jmx.active

  +
  bool

  +
  activate (by default) or not the OpenEJB JMX MBeans

  +
  openejb.nobanner

  +
  bool

  +
  activate or not the OpenEJB banner (activated by default)

  +
  openejb.check.classloader

  +
  bool

  +
  if true print some information about duplicated classes

  +
  openejb.check.classloader.verbose

  +
  bool

  +
  if true print classes intersections

  +
  openejb.additional.exclude

  +
  string separated by comma

  +
  list of prefixes you want to exclude and are not in the default list of
  exclusion

  +
  openejb.additional.include

  +
  string separated by comma

  +
  list of prefixes you want to remove from the default list of exclusion

  +
  openejb.offline

  +
  bool

  +
  if true can create datasources and containers automatically

  +
  openejb.exclude-include.order

  +
  include-exclude or exclude-include

  +
  if the inclusion/exclusion should win on conflicts (intersection)

  +
  openejb.log.color

  +
  bool

  +
  activate or not the color in the console in embedded mode

  +
  openejb.log.color.<level in lowercase>

  +
  color in uppercase

  +
  set a color for a particular level. Color are BLACK, RED, GREEN, YELLOW,
  BLUE, MAGENTA, CYAN, WHITE, DEFAULT.

  +
  tomee.serialization.class.blacklist

  +
  string

  +
  default list of packages/classnames excluded for EJBd deserialization
  (needs to be set on server and client sides). Please see the description
  of Ejbd Transport for details.

  +
  tomee.serialization.class.whitelist

  +
  string

  +
  default list of packages/classnames allowed for EJBd deserialization
  (blacklist wins over whitelist, needs to be set on server and client
  sides). Please see the description of Ejbd Transport for details.

  +
  tomee.remote.support

  +
  boolean

  +
  if true /tomee webapp is auto-deployed and EJBd is active (true by
  default for 1.x, false for 7.x excepted for tomee maven plugin and
  arquillian)

Note: all resources can be configured by properties, see
[http://tomee.apache.org/embedded-configuration.html](http://tomee.apache.org/embedded-configuration.html) and
[http://tomee.apache.org/properties-tool.html](http://tomee.apache.org/properties-tool.html)

## OpenEJB client

Name

Value

Description

openejb.client.identityResolver

implementation of org.apache.openejb.client.IdentityResolver

default org.apache.openejb.client.JaasIdentityResolver. The class to get
the client identity.

openejb.client.connection.pool.timeout or
openejb.client.connectionpool.timeout

int (ms)

the timeout of the client

openejb.client.connection.pool.size or
openejb.client.connectionpool.size

int

size of the socket pool

openejb.client.keepalive

int (ms)

the keepalive duration

openejb.client.protocol.version

string

Optional legacy server protocol compatibility level. Allows 4.6.x
clients to potentially communicate with older servers. OpenEJB 4.5.2 and
older use version "3.1", and 4.6.x currently uses version "4.6"
(Default). This does not allow old clients to communicate with new
servers prior to 4.6.0

## TomEE specific system properties

Name

Value

Description

openejb.crosscontext

bool

set the cross context property on tomcat context (can be done in the
traditional way if the deployment is done through the webapp discovery
and not the OpenEJB Deployer EJB)

openejb.jsessionid-support

bool

remove URL from session tracking modes for this context (see
jakarta.servlet.SessionTrackingMode)

openejb.myfaces.disable-default-values

bool

by default TomEE will initialize myfaces with some its default values to
avoid useless logging

openejb.web.xml.major

int

major version of web.xml. Can be useful to force tomcat to scan servlet
3 annotation when deploying with a servlet 2.x web.xml

tomee.jaxws.subcontext

string

sub context used to bind jaxws web services, default is webservices

openejb.servicemanager.enabled

bool

run all services detected or only known available services (WS and RS

tomee.jaxws.oldsubcontext

bool

whether or not activate old way to bind jaxws webservices directly on
root context

openejb.modulename.useHash

bool

add a hash after the module name of the webmodule if it is generated
from the webmodule location, it avoids conflicts between multiple
deployment (through ear) of the same webapp. Note: it disactivated by
default since names are less nice this way.

openejb.session.manager

qualified name (string)

configure a session manager to use for all contexts

## TomEE Arquillian adaptor

Name

Value

Description

tomee.ejbcontainer.http.port

int

tomee port, -1 means random. When using a random port you can retrieve
it getting this property too.

tomee.arquillian.http

int

http port used by the embedded arquillian adaptor

tomee.arquillian.stop

int

shutdown port used by the embedded arquillian adaptor

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-properties-listing--)

---

<a id="tomee-apache-org-tomee-10-1-docs-properties-tool"></a>

# Apache TomEE

```properties
# Container(id=Default CMP Container)
# className: org.apache.openejb.core.cmp.CmpContainer
#
Default\ CMP\ Container.CmpEngineFactory=org.apache.openejb.core.cmp.jpa.JpaCmpEngineFactory
Default\ CMP\ Container.Engine=instantdb
Default\ CMP\ Container.ConnectorName=Default JDBC Database

# Container(id=Default BMP Container)
# className: org.apache.openejb.core.entity.EntityContainer
#
Default\ BMP\ Container.PoolSize=10

# Container(id=Default Stateful Container)
# className: org.apache.openejb.core.stateful.StatefulContainer
#
Default\ Stateful\ Container.BulkPassivate=50
Default\ Stateful\ Container.Passivator=org.apache.openejb.core.stateful.SimplePassivater
Default\ Stateful\ Container.TimeOut=20
Default\ Stateful\ Container.PoolSize=500

# Container(id=Default Stateless Container)
# className: org.apache.openejb.core.stateless.StatelessContainer
#
Default\ Stateless\ Container.PoolSize=10
Default\ Stateless\ Container.StrictPooling=true
Default\ Stateless\ Container.TimeOut=0

# Container(id=Default MDB Container)
# className: org.apache.openejb.core.mdb.MdbContainer
#
Default\ MDB\ Container.ResourceAdapter=Default JMS Resource Adapter
Default\ MDB\ Container.InstanceLimit=10
Default\ MDB\ Container.MessageListenerInterface=jakarta.jms.MessageListener
Default\ MDB\ Container.ActivationSpecClass=org.apache.activemq.ra.ActiveMQActivationSpec

# ConnectionManager(id=Default Local TX ConnectionManager)
# className: org.apache.openejb.resource.SharedLocalConnectionManager
#

# Resource(id=Default JMS Resource Adapter)
# className: org.apache.activemq.ra.ActiveMQResourceAdapter
#
Default\ JMS\ Resource\ Adapter.ServerUrl=vm\://localhost?async\=true
Default\ JMS\ Resource\ Adapter.BrokerXmlConfig=broker\:(tcp\://localhost\:61616)
Default\ JMS\ Resource\ Adapter.ThreadPoolSize=30

# Resource(id=Default JDBC Database)
# className: org.apache.openejb.resource.jdbc.BasicManagedDataSource
#
Default\ JDBC\ Database.MinIdle=0
Default\ JDBC\ Database.Password=xxxx
Default\ JDBC\ Database.JdbcUrl=jdbc\:hsqldb\:file\:hsqldb
Default\ JDBC\ Database.MaxIdle=20
Default\ JDBC\ Database.ConnectionProperties=
Default\ JDBC\ Database.MaxWait=-1
Default\ JDBC\ Database.TimeBetweenEvictionRunsMillis=-1
Default\ JDBC\ Database.MaxActive=20
Default\ JDBC\ Database.DefaultAutoCommit=true
Default\ JDBC\ Database.AccessToUnderlyingConnectionAllowed=false
Default\ JDBC\ Database.JdbcDriver=org.hsqldb.jdbcDriver
Default\ JDBC\ Database.TestWhileIdle=false
Default\ JDBC\ Database.UserName=sa
Default\ JDBC\ Database.MaxOpenPreparedStatements=0
Default\ JDBC\ Database.TestOnBorrow=true
Default\ JDBC\ Database.PoolPreparedStatements=false
Default\ JDBC\ Database.ConnectionInterface=javax.sql.DataSource
Default\ JDBC\ Database.TestOnReturn=false
Default\ JDBC\ Database.MinEvictableIdleTimeMillis=1800000
Default\ JDBC\ Database.NumTestsPerEvictionRun=3
Default\ JDBC\ Database.InitialSize=0

# Resource(id=Default Unmanaged JDBC Database)
# className: org.apache.openejb.resource.jdbc.BasicDataSource
#
Default\ Unmanaged\ JDBC\ Database.MaxWait=-1
Default\ Unmanaged\ JDBC\ Database.InitialSize=0
Default\ Unmanaged\ JDBC\ Database.DefaultAutoCommit=true
Default\ Unmanaged\ JDBC\ Database.ConnectionProperties=
Default\ Unmanaged\ JDBC\ Database.MaxActive=10
Default\ Unmanaged\ JDBC\ Database.TestOnBorrow=true
Default\ Unmanaged\ JDBC\ Database.JdbcUrl=jdbc\:hsqldb\:file\:hsqldb
Default\ Unmanaged\ JDBC\ Database.TestOnReturn=false
Default\ Unmanaged\ JDBC\ Database.AccessToUnderlyingConnectionAllowed=false
Default\ Unmanaged\ JDBC\ Database.Password=xxxx
Default\ Unmanaged\ JDBC\ Database.MinEvictableIdleTimeMillis=1800000
Default\ Unmanaged\ JDBC\ Database.PoolPreparedStatements=false
Default\ Unmanaged\ JDBC\ Database.MaxOpenPreparedStatements=0
Default\ Unmanaged\ JDBC\ Database.ConnectionInterface=javax.sql.DataSource
Default\ Unmanaged\ JDBC\ Database.MinIdle=0
Default\ Unmanaged\ JDBC\ Database.NumTestsPerEvictionRun=3
Default\ Unmanaged\ JDBC\ Database.TimeBetweenEvictionRunsMillis=-1
Default\ Unmanaged\ JDBC\ Database.JdbcDriver=org.hsqldb.jdbcDriver
Default\ Unmanaged\ JDBC\ Database.UserName=sa
Default\ Unmanaged\ JDBC\ Database.MaxIdle=10
Default\ Unmanaged\ JDBC\ Database.TestWhileIdle=false

# Resource(id=Default JMS Connection Factory)
# className: org.apache.activemq.ra.ActiveMQManagedConnectionFactory
#
Default\ JMS\ Connection\ Factory.ConnectionInterface=jakarta.jms.ConnectionFactory, \
jakarta.jms.QueueConnectionFactory, jakarta.jms.TopicConnectionFactory
Default\ JMS\ Connection\ Factory.ResourceAdapter=Default JMS Resource Adapter

# SecurityService(id=Default Security Service)
# className: org.apache.openejb.core.security.SecurityServiceImpl
#

# TransactionManager(id=Default Transaction Manager)
# className: org.apache.geronimo.transaction.manager.GeronimoTransactionManager
#

# ServerService(id=httpejbd)
# className: org.apache.openejb.server.httpd.HttpEjbServer
#
httpejbd.port=4204
httpejbd.name=httpejbd
httpejbd.disabled=false
httpejbd.server=org.apache.openejb.server.httpd.HttpEjbServer
httpejbd.threads=200
httpejbd.bind=127.0.0.1

# ServerService(id=telnet)
# className: org.apache.openejb.server.telnet.TelnetServer
#
telnet.port=4202
telnet.name=telnet
telnet.disabled=false
telnet.bind=127.0.0.1
telnet.threads=5
telnet.server=org.apache.openejb.server.telnet.TelnetServer

# ServerService(id=ejbd)
# className: org.apache.openejb.server.ejbd.EjbServer
#
ejbd.disabled=false
ejbd.bind=127.0.0.1
ejbd.server=org.apache.openejb.server.ejbd.EjbServer
ejbd.port=4201
ejbd.name=ejbd
ejbd.threads=200

# ServerService(id=hsql)
# className: org.apache.openejb.server.hsql.HsqlService
#
hsql.port=9001
hsql.name=hsql
hsql.disabled=false
hsql.server=org.apache.openejb.server.hsql.HsqlService
hsql.bind=127.0.0.1

# ServerService(id=admin)
# className: org.apache.openejb.server.admin.AdminDaemon
#
admin.disabled=false
admin.bind=127.0.0.1
admin.only_from=localhost
admin.port=4200
admin.threads=1
admin.name=admin
admin.server=org.apache.openejb.server.admin.AdminDaemon
```

---

<a id="tomee-apache-org-tomee-10-1-docs-property-overriding"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Property Overriding

OpenEJB consists of several components (containers,
resource adapters, security services, etc.) all of which are pluggable
and have their own unique set of configurable properties. These
components are required to specify their default property values in
their service-jar.xml file. This means that at a minimum you as a user
only need to specify what you’d like to be different. You specify the
property name and the new value and the default value will be
overwritten before the component is created.

We call this overriding and there are several ways to do it.

# The openejb.xml

The default openejb.xml file has most of the useful properties for each
component explicitly listed with default values for documentation
purposes. It is safe to delete them and be assured that no behavior will
change if a smaller config file is desired.

Overriding can also be done via the command line or plain Java system
properties. See [System Properties](#tomee-apache-org-tomee-10-1-docs-system-properties) for
details.

## What properties are available?

To know what properties can be overridden the './bin/openejb properties'
command is very useful: see [Properties Tool](#tomee-apache-org-tomee-10-1-docs-properties-tool)

Its function is to connect to a running server and print a canonical
list of all properties OpenEJB can see via the various means of
configuration. When sending requests for help to the users list or jira,
it is highly encouraged to send the output of this tool with your
message.

## Not configurable via openejb.xml

The only thing not yet configurable via this file are ServerServices due
to OpenEJB’s embeddable nature and resulting long-standing tradition of
keeping the container system separate from the server layer. This may
change someday, but until then ServerServices are configurable via
conf/.properties files such as conf/ejbd.properties to configure the
main protocol that services EJB client requests.

The format those properties files is greatly adapted from the xinet.d
style of configuration and even shares similar functionality and
properties such as host-based authorization (HBA) via the 'only\_from'
property.

## Restoring openejb.xml to the defaults

To restore this file to its original default state, you can simply
delete it or rename it and OpenEJB will see it’s missing and unpack
another openejb.xml into the conf/ directory when it starts.

This is not only handy for recovering from a non-functional config, but
also for upgrading as OpenEJB will not overwrite your existing
configuration file should you choose to unpack a new distro over the
top of an old one — this style of upgrade is safe provided you move
your old lib/ directory first.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-property-overriding--)

---

<a id="tomee-apache-org-tomee-10-1-docs-provisioning"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# TomEE/OpenEJB provisioning

## Summary

Provisioning is about the way to get binaries or information. It is the
answer to how do I get my application, my webapp, my configuration.

TomEE and OpenEJB brings some help about it allowing you to point out
some resources instead of providing it directly.

This indirection is clearly very useful to industrialize your software
or simply to cloudify it.

## A word about this page

This page will not explain to you how to deploy an application or how to
enhance your container. It will simply explain to you how which kind of
urls are supported for such features.

These features are explained in other places.

## Supported provisioning

### file

This is the default and well know provisioning. Simply give a file path
the container is able to access through its filesystem.

Example:

```java
/MIDDLE/foo/bar/my-local-file.jar
```

### Http/https

Here you give a url to access the desired file. Proxies used are the
JVM ones.

Example:

```properties
http://atos.net/foo/bar/my-http-file.jar
```

### Maven

#### Usage

Probably the most fun but very useful for cloud deployments: maven. Use
maven information to deploy your application.

The location should follow:

```properties
mvn:groupId/artifactId[/[version]/[type]]
```

or

```properties
mvn:groupId:artifactId[:[version]:[type]]
```

Note: classifier are supported (through version field)

For instance, you can use:

```properties
mvn:net.atos.xa/my-application/1.0.0/war
```

#### Installation

The maven url parsing is not included by default in OpenEJB/TomEE
bundle. It needs to be installed.

If you are using an embedded application and maven simply add
org.apache.openejb:openejb-provisionning:VERSION dependency.

If you are using TomEE you have to extract the
org.apache.openejb:openejb-provisionning zip in the same classloader
as tomee (webapps/tomee/lib for instance, for other places please have
a look to other tip pages).

Another way to install it with tomee is to edit or create the file
/conf/provisioning.properties and add the line:

```properties
zip=http://repo1.maven.org/maven2/org/apache/openejb/openejb-provisionning/<version>/openejb-provisionning-<version>.zip
```

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-provisioning--)

---

<a id="tomee-apache-org-tomee-10-1-docs-proxyfactory-config"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# ProxyFactory Configuration

A ProxyFactory can be declared via xml in the
`<tomee-home>/conf/tomee.xml` file or in a `WEB-INF/resources.xml` file
using a declaration like the following. All properties in the element
body are optional.

```xml
<ProxyFactory id="myProxyFactory" type="ProxyFactory">
</ProxyFactory>
```

Alternatively, a ProxyFactory can be declared via properties in the
`<tomee-home>/conf/system.properties` file or via Java VirtualMachine
`-D` properties. The properties can also be used when embedding TomEE
via the `jakarta.ejb.embeddable.EJBContainer` API or `InitialContext`

```properties
myProxyFactory = new://ProxyFactory?type=ProxyFactory
```

Properties and xml can be mixed. Properties will override the xml
allowing for easy configuration change without the need for $\{} style
variable substitution. Properties are not case sensitive. If a property
is specified that is not supported by the declared ProxyFactory a
warning will be logged. If a ProxyFactory is needed by the application
and one is not declared, TomEE will create one dynamically using default
settings. Multiple ProxyFactory declarations are allowed. # Supported
Properties

Property

Type

Default

Description

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-proxyfactory-config--)

---

<a id="tomee-apache-org-tomee-10-1-docs-queue-config"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Queue Configuration

A Queue can be declared via xml in the `<tomee-home>/conf/tomee.xml`
file or in a `WEB-INF/resources.xml` file using a declaration like the
following. All properties in the element body are optional.

```xml
<Resource id="myQueue" type="jakarta.jms.Queue">
    destination =
</Resource>
```

Alternatively, a Queue can be declared via properties in the
`<tomee-home>/conf/system.properties` file or via Java VirtualMachine
`-D` properties. The properties can also be used when embedding TomEE
via the `jakarta.ejb.embeddable.EJBContainer` API or `InitialContext`

```properties
myQueue = new://Resource?type=jakarta.jms.Queue
myQueue.destination =
```

Properties and xml can be mixed. Properties will override the xml
allowing for easy configuration change without the need for $\{} style
variable substitution. Properties are not case-sensitive. If a property
is specified that is not supported by the declared Queue a warning will
be logged. If a Queue is needed by the application and one is not
declared, TomEE will create one dynamically using default settings.
Multiple Queue declarations are allowed. # Supported Properties

Property

Type

Default

Description

destination

String

Specifies the name of the queue

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-queue-config--)

---

<a id="tomee-apache-org-tomee-10-1-docs-quickstart"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Quickstart

# Installation

To install OpenEJB, simply [download the latest
binary](downloads.html) and unpack your zip or tar.gz into the directory where you want
OpenEJB to live.

Windows users can download the zip and unpack it with the WinZip
program.

Linux users can download the tar.gz and unpack it with the following
command:

*tar xzvf openejb-3.0.tar.gz*

Congratulations, you’ve installed OpenEJB.

If you’ve unpacked OpenEJB into the directory C:-3.0, for example, then
this directory is your OPENEJB\_HOME directory. The OPENEJB\_HOME
directory is referred to in various parts of the documentation, so it’s
good to remember where it is.

# Using OpenEJB

Now all you need to do is move to the bin directory in OPENEJB\_HOME, the
directory where OpenEJB was unpacked, and type:

*openejb*

For Windows users, that looks like this:

\*C:-3.0> bin

For UNIX/Linux/Mac OS X users, that looks like this:

`[user@host openejb-3.0]([user@host-openejb-3.0.html](mailto:user@host-openejb-3.0.html)) # ./bin/openejb`

You really only need to know two commands to use OpenEJB,
openejbx30:deploy-tool.html[deploy] and [start|OPENEJBx30:Startup] .
Both are completely documented and have examples.

For help information and command options, try this:

> openejb deploy --help openejb start --help

For examples on using the start command and options, try this:

> openejb start --examples

That’s it!

If you don’t have any EJBs or clients to run, try the ubiquitous
openejbx30:hello-world.html[Hello World] example.

# Join the mailing list

The OpenEJB User list is where the general OpenEJB community goes to ask
questions, make suggestions, chat with other users, and keep a finger on
the pulse of the project. More information about the user list and dev
list can be found [here](mailing-lists.html)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-quickstart--)

---

<a id="tomee-apache-org-tomee-10-1-docs-remote-server"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Remote Server

!http://www.openejb.org/images/diagram-remote-server.gif|valign=top,
align=right, hspace=15! # Accessing EJBs Remotely

When using OpenEJB as a stand-alone server you can connect across a
network and access EJBs from a remote client. The client code for
accessing an EJB’s Remote Interface is the same, however to actually
connect across a network to the server, you need to specify different
JNDI parameters.

# Short version

Using OpenEJB’s default remote server implementation is pretty straight
forward. You simply need to:

1. Deploy your bean.
2. Start the server on the IP and Port you want, 25.14.3.92 and 4201 for
   example.
3. Use that information in your client to create an initial context
4. Add the right jars to your client’s classpath

So, here it is in short.

Deploy your bean with the Deploy Tool:

```java
c:\openejb> openejb.bat deploy beans\myBean.jar
```

See the openejbx30:deploy-tool.html[OPENEJBx30:Deploy Tool]
documentation for more details on deploying beans.

Start the server:

```java
c:\openejb> openejb.bat start -h 25.14.3.92 -p 4201
```

See the Remote Server command-line guide for more details on starting
the Remote Server.

Create an initial context in your client as such:

```java
Properties p = new Properties();
p.put("java.naming.factory.initial", "org.apache.openejb.client.RemoteInitialContextFactory");
p.put("java.naming.provider.url", "ejbd://25.14.3.92:4201");
p.put("java.naming.security.principal", "myuser");
p.put("java.naming.security.credentials", "mypass");

InitialContext ctx = new InitialContext(p);
```

If you don’t have any EJBs or clients to run, try the ubiquitous
openejbx30:hello-world.html[Hello World] example. Add the following
library to your clients classpath:

- openejb-client-x.x.x.jar
- jakartaee-api-x.x.jar

Both can be found in the lib directory where you installed OpenEJB or in
Maven repositories.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-remote-server--)

---

<a id="tomee-apache-org-tomee-10-1-docs-resource-injection"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# @Resource

This example demonstrates the use of the injection of environment
entries using **`@Resource`** annotation.

The EJB 3.0 specification (*EJB Core Contracts and Requirements*)
section 16.2.2 reads:

*A field or method of a bean class may be annotated to request that an
entry from the bean’s environment be injected. Any of the types of
resources or other environment entries described in this chapter may be
injected. Injection may also be requested using entries in the
deployment descriptor corresponding to each of these resource types.*

*Environment entries may also be injected into the bean through bean
methods that follow the naming conventions for JavaBeans properties. The
annotation is applied to the set method for the property, which is the
method that is called to inject the environment entry. The JavaBeans
property name (not the method name) is used as the default JNDI name.*

The *PurchaseOrderBean* class shows use of field-level **`@Resource`**
annotation.

The *InvoiceBean* class shows the use of method-level **`@Resource`**
annotation.

The source for this example can be checked out from svn:

> $ svn co
> [http://svn.apache.org/repos/asf/tomee/tomee/trunk/examples/injection-of-env-entry](http://svn.apache.org/repos/asf/tomee/tomee/trunk/examples/injection-of-env-entry)

To run it change your working directory to the directory
*injection-of-env-entry* and run the following maven2 commands:

> $ cd injection-of-env-entry

> $ mvn clean install

# The Code

## Injection through field (field-level injection)

The *maxLineItem* field in *PurchaseOrderBean* class is annotated with
**@Resource** annotation to inform the EJB container the location where in
the code the injection of a simple environment entry should take place.
The default value of 10 is assigned. You can modify the value of the
environment entries at deployment time using deployment descriptor
(`ejb-jar.xml`).

### @Resource annotation of a field

```java
@Resource
int maxLineItems = 10;
```

## Injection through a setter method (method-level injection)

The *setMaxLineItem* method in *InvoiceBean* class is annotated with
*@Resource* annotation to inject the simple environment entry. Only
setters can be used as a way to inject environment entry values.

You could look up the env-entry using JNDI lookup() method and the
following name:

```properties
java:comp/env/org.apache.openejb.examples.resource.InvoiceBean/maxLineItems
```

The pattern is to combine the fully-qualified class name and the name of
an instance field (or a name of the setter method without *set* prefix
and the first letter lowercased).

### @Resource annotation of a setter method

```java
@Resource
public void setMaxLineItems(int maxLineItems) {
    this.maxLineItems = maxLineItems;
}
```

### Using env-entry in ejb-jar.xml

```xml
<env-entry>
    <description>The maximum number of line items per invoice.</description>
    <env-entry-name>org.apache.openejb.examples.injection.InvoiceBean/maxLineItems</env-entry-name>
    <env-entry-type>java.lang.Integer</env-entry-type>
    <env-entry-value>15</env-entry-value>
</env-entry>
```

### Using @Resource annotated env-entry

```java
public void addLineItem(LineItem item) throws TooManyItemsException {
   if (item == null) {
      throw new IllegalArgumentException("Line item must not be null");
   }

   if (itemCount <= maxLineItems) {
      items.add(item);
      itemCount++;
   } else {
      throw new TooManyItemsException("Number of items exceeded the maximum limit");
   }
}
```

# JUnit Test

Writing an JUnit test for this example is quite simple. We need just to
write a setup method to create and initialize the InitialContext, and
then write our test methods.

## Test fixture

```java
protected void setUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.client.LocalInitialContextFactory");
    properties.setProperty("openejb.deployments.classpath.include", ".*resource-injection.*");
    initialContext = new InitialContext(properties);
}
```

## Test methods

```java
public void testAddLineItem() throws Exception {
    Invoice order = (Invoice)initialContext.lookup("InvoiceBeanBusinessRemote");
    assertNotNull(order);
    LineItem item = new LineItem("ABC-1", "Test Item");

    try {
    order.addLineItem(item);
    } catch (TooManyItemsException tmie) {
    fail("Test failed due to: " + tmie.getMessage());
    }
}
```

# Running

Running the example is fairly simple. Just execute the following
commands:

> $ cd injection-of-env-entry
>
> $ mvn clean test

```java
-------------------------------------------------------
 T E S T S
-------------------------------------------------------
Running org.superbiz.injection.PurchaseOrderBeanTest
Apache OpenEJB 3.0.0-SNAPSHOT    build: 20071218-01:41
http://tomee.apache.org/
INFO - openejb.home = c:\oss\openejb3\examples\injection-of-env-entry
INFO - openejb.base = c:\oss\openejb3\examples\injection-of-env-entry
WARN - Cannot find the configuration file [conf/openejb.xml].  Will attempt to create one for the beans deployed.
INFO - Configuring Service(id=Default Security Service,type=SecurityService, provider-id=Default Security Service)
INFO - Configuring Service(id=Default Transaction Manager, type=TransactionManager, provider-id=Default Transaction Manager)
INFO - Configuring Service(id=Default JDK 1.3 ProxyFactory, type=ProxyFactory, provider-id=Default JDK 1.3 ProxyFactory)
INFO - Found EjbModule in classpath: c:\oss\openejb3\examples\injection-of-env-entry\target\classes
INFO - Configuring app: c:\oss\openejb3\examples\injection-of-env-entry\target\classes
INFO - Configuring Service(id=Default Stateful Container, type=Container, provider-id=Default Stateful Container)
INFO - Auto-creating a container for bean InvoiceBean: Container(type=STATEFUL, id=Default Stateful Container)
INFO - Loaded Module: c:\oss\openejb3\examples\injection-of-env-entry\target\classes
INFO - Assembling app: c:\oss\openejb3\examples\injection-of-env-entry\target\classes
INFO - Jndi(name=InvoiceBeanRemote) --> Ejb(deployment-id=InvoiceBean)
INFO - Jndi(name=PurchaseOrderBeanRemote) --> Ejb(deployment-id=PurchaseOrderBean)
INFO - Created Ejb(deployment-id=InvoiceBean, ejb-name=InvoiceBean, container=Default Stateful Container)
INFO - Created Ejb(deployment-id=PurchaseOrderBean, ejb-name=PurchaseOrderBean, container=Default Stateful Container)
INFO - Deployed Application(path=c:\oss\openejb3\examples\injection-of-env-entry\target\classes)
INFO - OpenEJB ready.
OpenEJB ready.
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 2.859 sec
Running org.superbiz.injection.InvoiceBeanTest
Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 0.031 sec

Results :

Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
```

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-resource-injection--)

---

<a id="tomee-apache-org-tomee-10-1-docs-resource-ref-for-datasource"></a>

# Apache TomEE

```java
package org.superbiz.refs;

import jakarta.annotation.Resource;
import jakarta.ejb.Stateless;
import javax.naming.InitialContext;
import javax.sql.DataSource;

@Stateless
@Resource(name = "myFooDataSource", type = DataSource.class)
public class MyDataSourceRefBean implements MyBeanInterface {

    @Resource
    private DataSource myBarDataSource;

    public void someBusinessMethod() throws Exception {
        if (myBarDataSource == null) throw new NullPointerException("myBarDataSource not injected");

        // Both can be looked up from JNDI as well
        InitialContext context = new InitialContext();
        DataSource fooDataSource = (DataSource) context.lookup("java:comp/env/myFooDataSource");
        DataSource barDataSource = (DataSource) context.lookup("java:comp/env/org.superbiz.refs.MyDataSourceRefBean/myBarDataSource");
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-running-a-standalone-openejb-server"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Running a standalone OpenEJB server

# Configuring the OpenEJB Runtime

The OpenEJB Eclipse plugin provides support for running OpenEJB as a standalone server in Eclipse using WTP.

To set up a server, first of all, you will need to have a copy of OpenEJB
extracted on your machine. Once you have that, the next step is to set
up a runtime.

To set up a new runtime, click on Window, Preferences, and select
Installed Runtimes under the Server category. Click the Add button.

![http://people.apache.org/</sub>jgallimore/images/server_step_4.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_4.jpg)

Select OpenEJB 3.0.0 from the Apache category, and click next. If you
choose to 'also create a new server' on this panel, you can add a server
straight after configuring the runtime.

![http://people.apache.org/</sub>jgallimore/images/server_step_5.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_5.jpg)

Browse to, or enter the path to your copy of OpenEJB. Click on Finish.

# Configuring the OpenEJB Server Open the Servers view (if it isn’t

already), and right click and select New→Server.

![http://people.apache.org/</sub>jgallimore/images/server_step_8.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_8.jpg)

Select OpenEJB 3.0.0 from the Apache category, ensure you have the
OpenEJB runtime selected, and click Next.

![http://people.apache.org/</sub>jgallimore/images/server_step_9.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_9.jpg)

Select the EJB port for the server, and select Finish.

![http://people.apache.org/</sub>jgallimore/images/server_step_10.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_10.jpg)

# Deploying a project In order to deploy your project to an OpenEJB

server in Eclipse, your project must be a Java EE project, with the EJB
facet enabled. If your project doesn’t have the Faceted nature, you can
use the OpenEJB plugin to add it. Simply select OpenEJB→Add Faceted
Nature from the menu bar.

![http://people.apache.org/</sub>jgallimore/images/server_step_1.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_1.jpg)

To add the EJB facet, right-click on the project in the navigator, and
select Properties. Select Project Facets on the left hand side. Click on
the Modify Project button.

![http://people.apache.org/</sub>jgallimore/images/server_step_2.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_2.jpg)

Select the EJB Module facet, and the Java Facet. Remember to select your
OpenEJB runtime too. Click Next.

![http://people.apache.org/</sub>jgallimore/images/server_step_6.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_6.jpg)

Enter the source folder for the EJBs in your project and click Finish.

![http://people.apache.org/</sub>jgallimore/images/server_step_7.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_6.jpg)

Now right-click on your OpenEJB server in the servers view, and select
Add and Remove Projects.

![http://people.apache.org/</sub>jgallimore/images/server_step_11.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_11.jpg)

Add your project to the server, and click Finish.

![http://people.apache.org/</sub>jgallimore/images/server_step_12.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_12.jpg)

To start the server, right-click on your OpenEJB server, and select
Start.

![http://people.apache.org/</sub>jgallimore/images/server_step_13.jpg](http://people.apache.org/<sub>jgallimore/images/server_step_13.jpg)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-running-a-standalone-openejb-server--)

---

<a id="tomee-apache-org-tomee-10-1-docs-securing-a-web-service"></a>

# Apache TomEE

```xml
<properties>
  wss4j.in.action = UsernameToken
  wss4j.in.passwordType = PasswordDigest
  wss4j.in.passwordCallbackClass=org.superbiz.calculator.CustomPasswordHandler
</properties>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-security-annotations"></a>

# Apache TomEE

```java
@Stateless
public class OpenSourceProjectBean implements Project {

    public String svnCheckout(String s) {
    return s;
    }
}

@Stateless
@PermitAll
public class OpenSourceProjectBean implements Project {

    public String svnCheckout(String s) {
    return s;
    }
}

@Stateless
public class OpenSourceProjectBean implements Project {

    @PermitAll
    public String svnCheckout(String s) {
    return s;
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-security"></a>

# Apache TomEE

```java
Properties props = new Properties();
props.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.client.RemoteInitialContextFactory");
props.setProperty(Context.PROVIDER_URL, "ejbd://localhost:4201");
props.setProperty(Context.SECURITY_PRINCIPAL, "someuser");
props.setProperty(Context.SECURITY_CREDENTIALS, "thepass");
props.setProperty("openejb.authentication.realmName", "PropertiesLogin");
// optional
InitialContext ctx = new InitialContext(props);
ctx.lookup(...);
```

---

<a id="tomee-apache-org-tomee-10-1-docs-securityservice-config"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# SecurityService Configuration

A SecurityService can be declared via xml in the
`<tomee-home>/conf/tomee.xml` file or in a `WEB-INF/resources.xml` file
using a declaration like the following. All properties in the element
body are optional.

```xml
<SecurityService id="mySecurityService" type="SecurityService">
    defaultUser = guest
</SecurityService>
```

Alternatively, a SecurityService can be declared via properties in the
`<tomee-home>/conf/system.properties` file or via Java VirtualMachine
`-D` properties. The properties can also be used when embedding TomEE
via the `jakarta.ejb.embeddable.EJBContainer` API or `InitialContext`

```properties
mySecurityService = new://SecurityService?type=SecurityService
mySecurityService.defaultUser = guest
```

Properties and xml can be mixed. Properties will override the xml
allowing for easy configuration change without the need for $\{} style
variable substitution. Properties are not case-sensitive. If a property
is specified that is not supported by the declared SecurityService a
warning will be logged. If a SecurityService is needed by the
application and one is not declared, TomEE will create one dynamically
using default settings. Multiple SecurityService declarations are
allowed. # Supported Properties

Property

Type

Default

Description

defaultUser

String

guest

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-securityservice-config--)

---

<a id="tomee-apache-org-tomee-10-1-docs-service-locator"></a>

# Apache TomEE

```java
public class MyLocator {
    private final Context context;

    public MyLocator() throws NamingException {
        this(null);
    }

    public MyLocator(String commonPrefix) throws NamingException {
        Properties properties = new Properties();
        properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.client.RemoteInitialContextFactory");
        properties.put(Context.PROVIDER_URL, "ejbd://localhost:4201/");
        this.context = new InitialContext(properties);
    }

    public Object lookup(String name) {
        try {
            if (commonPrefix != null) name = commonPrefix + "/" +name;
            return context.lookup(name);
        } catch (NamingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-services"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# ServicePool and Services

OpenEJB and TomEE services using ServicePool as wrapper - for instance
ejbd service or all services not implementing SelfManaging interface -
support some additional configuration due to the pooling. Here is the
list of the additional properties (either configure them in the service
configuration file in conf/conf.d/${service}.properties or in
conf/system.properties prefixing them by “{service}.”).

Basically using ServicePool the service is associated to a
ThreadPoolExecutor and this one is configured with these properties (see
ThreadPoolExecutor constructor for the detail):

- threadsCore (default 10)
- threads (default 150)
- queue (default threadCore-1)
- block (default true)
- keepAliveTime (default 60000)

Additionally, you can force the socket to be closed after each request
(this is an advanced setting, use it with caution):

- forceSocketClose (default true)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-services--)

---

<a id="tomee-apache-org-tomee-10-1-docs-singleton-beans"></a>

# Apache TomEE

```java
public interface ReadWriteLock {
   /**
    * Returns the lock used for reading.
    *
    * @return the lock used for reading.
    */
   Lock readLock();

   /**
    * Returns the lock used for writing.
    *
    * @return the lock used for writing.
    */
   Lock writeLock();
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-singleton-ejb"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Singleton EJB

\{include:OPENEJBx30:Singleton Beans}

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-singleton-ejb--)

---

<a id="tomee-apache-org-tomee-10-1-docs-singletoncontainer-config"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# SingletonContainer Configuration

A SingletonContainer can be declared via xml in the
`<tomee-home>/conf/tomee.xml` file or in a `WEB-INF/resources.xml` file
using a declaration like the following. All properties in the element
body are optional.

```xml
<Container id="mySingletonContainer" type="SINGLETON">
    accessTimeout = 30 seconds
</Container>
```

Alternatively, a SingletonContainer can be declared via properties in
the `<tomee-home>/conf/system.properties` file or via Java
VirtualMachine `-D` properties. The properties can also be used when
embedding TomEE via the `jakarta.ejb.embeddable.EJBContainer` API or
`InitialContext`

```properties
mySingletonContainer = new://Container?type=SINGLETON
mySingletonContainer.accessTimeout = 30 seconds
```

Properties and xml can be mixed. Properties will override the xml
allowing for easy configuration change without the need for $\{} style
variable substitution. Properties are not case-sensitive. If a property
is specified that is not supported by the declared SingletonContainer a
warning will be logged. If a SingletonContainer is needed by the
application and one is not declared, TomEE will create one dynamically
using default settings. Multiple SingletonContainer declarations are
allowed. # Supported Properties

Property

Type

Default

Description

accessTimeout

time

30 seconds

Specifies the maximum time an invocation could wait for the `@Singleton`
bean instance to become available before giving up.

## accessTimeout

Specifies the maximum time an invocation could wait for the `@Singleton`
bean instance to become available before giving up.

After the timeout is reached a
`jakarta.ejb.ConcurrentAccessTimeoutException` will be thrown.

Usable time units: nanoseconds, microseconds, milliseconds, seconds,
minutes, hours, days. Or any combination such as
`1 hour and 27 minutes and 10 seconds`

Any usage of the `jakarta.ejb.AccessTimeout` annotation will override this
setting for the bean or method where the annotation is used.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-singletoncontainer-config--)

---

<a id="tomee-apache-org-tomee-10-1-docs-spring-and-openejb-3-0"></a>

# Apache TomEE

```java
public class OpenEjbFactoryBean implements org.springframework.beans.factory.FactoryBean {

    private Properties properties = new Properties();

    public OpenEjbFactoryBean() {
        properties.put(Context.INITIAL_CONTEXT_FACTORY, "org.apache.openejb.client.LocalInitialContextFactory");
    }

    public Properties getJndiEnvironment() {
        return properties;
    }

    public void setJndiEnvironment(Properties properties) {
        this.properties.putAll(properties);
    }

    public Object getObject() {
        try {
            return new InitialContext(properties);
        } catch (NamingException e) {
            throw new RuntimeException(e);
        }
    }

    public Class getObjectType(){
        return Context.class;
    }

    boolean isSingleton() {
        return true;
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-spring-ejb-and-jpa"></a>

# Apache TomEE

```java
-------------------------------------------------------
 T E S T S
-------------------------------------------------------
Running org.superbiz.spring.MoviesTest
log4j:WARN No appenders could be found for logger
```

---

<a id="tomee-apache-org-tomee-10-1-docs-spring"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Spring

{note} This document and the related feature is considered a prototype
and will change based on user feedback. All comments suggestions
welcome. {note}

# Introduction

The OpenEJB Spring integration makes all Spring defined beans injectable
to Java EE components, and all Java EE components can be injected to
Spring beans. The injection system supports arbitrarily complex nesting
(e.g., Spring bean injected into a Java EE component, which is then
injected into another Spring bean), including:

- @Resource injection of any Spring bean into EJB
- Injection of any Java EE resource into a Spring bean, including: **EJB 3.0 beans**  EJB 3.1 Singleton Bean  **JDBC Connector**  JMS
  Connector  **JMS Queue and Topic**  Generic Java EE Connector (JCA)

In addition, the OpenEJB Spring integration add support for discovery
and deployment of standard Java EE packages within a Spring context,
including:

- EAR
- EJB Jar
- Persistence Unit
- RAR

*Requirements:* \* OpenEJB 3.1+ \* Spring X.X \* Java 1.5 or 1.6

# Spring Beans

The following beans are usable in any spring xml file.

Class

Description

org.apache.openejb.spring.ClassPathApplication

Scrapes the classpath for all EJB, RAR, and Persistence applications,
deploys them, and imports them into the current ApplicationContext. All
applications found are treated as one big EAR unless the
*classpathAsEar* property is set to *false*

org.apache.openejb.spring.Application

Scrapes an individual jar file for EJB, RAR, and Persistence
applications, deploys them, and imports them into the current
ApplicationContext. The 'jarFile' property is required. The application
is treated as its own self-contained EAR, separate from other uses of
'Application'

org.apache.openejb.spring.Resource

Allows an OpenEJB to be declared in the Spring ApplicationContext

org.apache.openejb.spring.OpenEJBResource

A FactoryBean that imports a Resource from OpenEJB into the Spring
ApplicationContext. Has the following properties: *type* such as
javax.sql.DataSource, and *resourceId*. In the future this bean will not
be required and all OpenEJB Resources will automatically be imported
into the Spring ApplicationContext

org.apache.openejb.spring.BmpContainer

Allows an OpenEJB BMP to be declared in the Spring ApplicationContext.
Has the following properties: *poolSize*

org.apache.openejb.spring.CmpContainer

Allows an OpenEJB CMP to be declared in the Spring ApplicationContext.

org.apache.openejb.spring.SingletonContainer

Allows an OpenEJB Singleton to be declared in the Spring
ApplicationContext. Has the following properties: *accessTimeout*

org.apache.openejb.spring.StatefulContainer

Allows an OpenEJB Stateful to be declared in the Spring
ApplicationContext. Has the following properties: *timeOut*

org.apache.openejb.spring.StatelessContainer

Allows an OpenEJB Stateful to be declared in the Spring
ApplicationContext. Has the following properties: *timeOut*, *poolSize*,
and *strictPooling*

org.apache.openejb.spring.MdbContainer

Allows an OpenEJB Message-Driven to be declared in the Spring
ApplicationContext. Has the following properties: *resourceAdapter*,
*messageListenerInterface*, *activationSpecClass*, and *instanceLimit*

org.apache.openejb.spring.EJB

A FactoryBean that imports an EJB from OpenEJB into the Spring
ApplicationContext. One of these is automatically created for each
interface of each EJB, but explicit use can be nice if you desire to
import an EJB with a specific name. Has the following properties:
*deploymentId*, *interface*

# Examples

See the [Spring EJB and JPA](#tomee-apache-org-tomee-10-1-docs-spring-ejb-and-jpa) page for
example code and a working Spring xml file.

# \{anchor:problems} Problems?

If you are having problems with the installation, please send a message
to the OpenEJB users [mailing list](mailing-lists.html) containing
any error message(s) and the following information:

- OpenEJB Version
- Spring Version
- Java Version (execute java -version)
- Operating System Type and Version

# Limitations

*JavaAgent* - OpenEJB uses OpenJPA to provide JPA and CMP persistence,
and OpenJPA currently requires a JavaAgent to function properly in a
Java 1.5 environment. OpenJPA does not require a JavaAgent in Java 1.6.
Use Hibernate as the provider in your `persistence.xml` files if you
wish to avoid this requirement.

*EntityManager* - Having an OpenEJB created EntityManager or
EntityManagerFactory injected into Spring beans is currently not
supported. This will be added to the next release. A small workaround
for this is to use an EJB as a factory by adding a 'getEntityManager'
method using it as a
[Spring
instance factory method](http://static.springframework.org/spring/docs/2.5.x/reference/beans.html#beans-factory-class-instance-factory-method) .

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-spring--)

---

<a id="tomee-apache-org-tomee-10-1-docs-ssh"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# SSH

## Connecting To OpenEJB or TomEE Through SSH

### Description

It can be very useful to connect to the server to get some information.

### Solution

For such a case OpenEJB/TomEE proposes to start with the Java EE server
an SSH server. Currently, the security is based on JAAS (see how to
configure JAAS for TomEE for more information about it).

### Installation

Simply extract the openejb-ssh jar in the lib of tomee
(webapps/tomee/lib) or openejb libs (lib folder). Then simply connect
using your JAAS credential.

Note: you can use the provisioning features of openejb to do this job!

Then simply activate the service manage: it is done setting the system
property openejb.servicemanager.enabled to true.

Note: it can be done through the conf/system.properties file. Note2:
please take care to not add space after true (not 'true ' for instance).

### OpenEJB SSH Shell

Once you are connected you get some commands:

- deploy : deploy an application
- undeploy : undeploy an application
- list: list deployed EJBs
- classloader : print the classloader tree of the app specified by the
  id
- jmx : interact with JMX  **jmx list: list mbeans**  jmx get \*\* jmx set

  - jmx invoke ([, …​)
- properties: print server configuration as properties
- script

  +
  : execute the following script code using the following language with
  the JSR 223
- script file

  +
  : execute the following script using the language (from the extension of
  the file) with the JSR 223
- ls []: list the file in path is specified or in the base of the server
  if not
- cat : print a file
- part - : print the part of a file

Note1: JSR 223 can need to add some jar to openejb/tomee lib folder
(groovy-all for instance to use groovy) Note2: ls, part, cat commands
have to use $home and $base properties to specify the path

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-ssh--)

---

<a id="tomee-apache-org-tomee-10-1-docs-standalone-server"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# null

|  | Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at .http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. |
| --- | --- |

# Links to guide you through OpenEJB-Standalone-Server

- [Startup](#tomee-apache-org-tomee-10-1-docs-startup)
- [Deploy Tool](#tomee-apache-org-tomee-10-1-docs-deploy-tool)
- [Properties Tool](#tomee-apache-org-tomee-10-1-docs-properties-tool)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-standalone-server--)

---

<a id="tomee-apache-org-tomee-10-1-docs-startup"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Startup

# NAME

openejb start - OpenEJB Remote Server

# SYNOPSIS

openejb start [#options](#tomee-apache-org-tomee-10-1-docs-startup--options.html)

# NOTE

The OpenEJB Remote Server can be started by running the openejb.bat
script for windows and the openejb script for Linux and other Unix based
OSes. Before running these scripts you need to set the environment
variable *OPENEJB\_HOME* to the path of the directory where you unpacked
the OpenEJB installation.

From now on we will refer to this directory as and assume that you
unpacked OpenEJB into the directory *C:-3.0* The startup scripts are
present in the /bin directory. You can set this directory in the system
*PATH* for starting openejb from the command shell.

In Windows, the remote server can be executed as follows:

*C:-3.0> binstart*

In UNIX, Linux, or Mac OS X, the deploy tool can be executed as follows:

`\[user@host openejb-3.0]([user@host-openejb-3.0.html](mailto:user@host-openejb-3.0.html)) # ./bin/openejb start`

Depending on your OpenEJB version, you may need to change execution bits
to make the scripts executable. You can do this with the following
command.

`\[user@host openejb-3.0]([user@host-openejb-3.0.html](mailto:user@host-openejb-3.0.html)) # chmod 755 bin/openejb`

From here on out, it will be assumed that you know how to execute the
right openejb script for your operating system and commands will appear
in shorthand as show below.

*openejb start -help*

# DESCRIPTION

Starts OpenEJB as an EJB Server that can be accessed by remote clients
via the OpenEJB Remote Server.

ALWAYS check your openejb.log file for warnings immediately after
starting the Remote Server.

OpenEJB issues warnings when it works around a potential problem,
encounters something it didn’t expect, or when OpenEJB wants to let you
know something may not work as you expected it.

OpenEJB itself is configured with the OpenEJB configuration file, which
is extremely simple and self-documenting. This file is located at
c:-3.0.xml.

# OPTIONS

| *-D=* | Specifies a system property passed into OpenEJB at startup. |
| *--admin-bind \_ | Sets the host to which the admin service should be
bound.| | \_—​admin-port \_ | Sets the port to which the admin service
should be bound.| | \_—​conf \_ | Sets the OpenEJB configuration to the
specified file. | | \_—​ejbd-bind \_ | Sets the host to which the ejbd
service should be bound. | | \_—​ejbd-port \_ | Sets the port to which the
ejbd service should be bound. |  
| \_—​examples* | Show examples of how to use the options. | | -h,
--*help* | Print this help message. | | *--hsql-bind \_ | Sets the host
to which the hsql service should be bound.| | \_—​hsql-port \_ | Sets the
port to which the hsql service should be bound.| | \_—​httpejbd-bind \_ |
Sets the host to which the httpejbd service should be bound.| |
\_—​httpejbd-port \_ | Sets the port to which the httpejbd service should
be bound.| | \_—​local-copy \_ | Instructs the container system to marshal
(ie, copy) all calls between beans. | | \_—​telnet-bind \_ | Sets the host
to which the telnet service should be bound.| | \_—​telnet-port \_ | Sets
the port to which the telnet service should be bound.| | -v, --\_version*
| Print the version. |

# EXAMPLES

## Example: Simplest scenario

*openejb start*

That’s it. The ejbd will start up and bind to IP 127.0.0.1 and port
4201.

The following properties would then be used to get an InitialContext
from the Remote Server.

```properties
java.naming.factory.initial  =
```

org.apache.openejb.client.RemoteInitialContextFactory
java.naming.provider.url = ejbd://127.0.0.1:4201
java.naming.security.principal = myuser java.naming.security.credentials
= mypass

## Example: --conf=file

*openejb start --conf=C:-3.0.conf*

Sets the openejb.configuration system variable to the file *C:.conf*.
When the server starts up and initializes OpenEJB, this configuration
will be used to assemble the container system and load beans.

## Example: --local-copy

The local-copy option controls whether Remote interface arguments and
results are always copied.

*openejb start --local-copy=true* (default)

Remote interface business method arguments and results are always copied
(via serialization), which is compliant with the EJB standard.

*openejb start --local-copy=false*

Remote interface business method arguments and results are copied only
when the client is in a different JVM. Otherwise, they are passed by
reference - as if it were a Local interface. This is faster, of course,
but non-compliant with the EJB standard.

Local interfaces are not affected; their arguments and results are
passed by reference and never copied.

## CONFIG OVERRIDE EXAMPLES

## Example: -D.bind=

*openejb start -Dejbd.bind=10.45.67.8*

This is the most common way to use the EJBd Server Service. The service
will start up and bind to IP 10.45.67.8 and port 4201. The following
properties would then be used to get an InitialContext from the EJBd
Server Service.

```properties
   java.naming.factory.initial      =
```

org.apache.openejb.client.RemoteInitialContextFactory
java.naming.provider.url = ejbd://10.45.67.8:4201
java.naming.security.principal = myuser java.naming.security.credentials
= mypass

DNS names can also be used.

*openejb start -Dejbd.bind=myhost.foo.com*

The following properties would then be used to get an InitialContext
from the Remote Server.

```properties
   java.naming.factory.initial      =
```

org.apache.openejb.client.RemoteInitialContextFactory
java.naming.provider.url = ejbd://myhost.foo.com:4201
java.naming.security.principal = myuser java.naming.security.credentials
= mypass

*openejb start -Dtelnet.bind=myhost.foo.com*

The following properties would then be used to log into the server via a
telnet client as such:

*telnet myhost.foo.com 4202*

## Example: -D.port=

*openejb start -Dejbd.port=8765*

The server will start up and bind to IP 127.0.0.1 and port 8765.

The following properties would then be used to get an InitialContext
from the Remote Server.

```properties
   java.naming.factory.initial      =
```

org.apache.openejb.client.RemoteInitialContextFactory
java.naming.provider.url = ejbd://127.0.0.1:8765
java.naming.security.principal = myuser java.naming.security.credentials
= mypass

*openejb start -Dhttpejbd.port=8888*

The server will start up and the EJB over HTTP service will bind to IP
127.0.0.1 and port 8888.

The following properties would then be used to get an InitialContext
from the HTTP/Remote Server.

```properties
   java.naming.factory.initial      =
```

org.apache.openejb.client.RemoteInitialContextFactory
java.naming.provider.url = [http://127.0.0.1:8888/openejb](http://127.0.0.1:8888/openejb)
java.naming.security.principal = myuser java.naming.security.credentials
= mypass

## Example: -D.only\_from=

*openejb start -Dadmin.only\_from=192.168.1.12*

Adds 192.168.1.12 to the list of IP addresses that are authorized to
shut down the server or access the server via a telnet client. The host
that this server was started on is always allowed to administer the
server.

Multiple hosts can be given administrative access to this server by
listing all the host names separated by commas as such:

*openejb start -Dadmin.only\_from=192.168.1.12,joe.foo.com,robert*

The first host in the string names the host explicitly using an IP
address (192.168.1.12).

The second host uses a DNS name (joe.foo.com) to refer to the hosts IP
address. The DNS name will be resolved and the IP will be added to the
admin list.

The third address refers to the host by a name (robert)that the
operating system is able to resolve into a valid IP address. This is
usually done via a hosts file, internal DNS server, or Windows Domain
Server.

## Example: -D.threads=

*openejb start -Dejbd.threads=200*

Sets the max number of concurrent threads that can enter the EJBd Server
Service to 200.

## Example: -D.disabled=

*openejb start -Dtelnet.disabled=true*

Prevents the Telnet Server Service from starting when the OpenEJB Server
starts.

# CONSOLE OUTPUT

Once you start OpenEJB using the *openejb start* command the following
output will be seen on the console

```properties
Apache OpenEJB 3.0    build: 20070825-01:10
http://tomee.apache.org/
OpenEJB ready.
[OPENEJB:init]
```

OpenEJB Remote Server  **Starting Services**  NAME IP PORT httpejbd
0.0.0.0 4204 telnet 0.0.0.0 4202 ejbd 0.0.0.0 4201 hsql 0.0.0.0 9001
admin thread 0.0.0.0 4200 ------- Ready!

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-startup--)

---

<a id="tomee-apache-org-tomee-10-1-docs-statefulcontainer-config"></a>

# Apache TomEE

```xml
<Container id="myStatefulContainer" type="STATEFUL">
    accessTimeout = 30 seconds
    bulkPassivate = 100
    cache = org.apache.openejb.core.stateful.SimpleCache
    capacity = 1000
    frequency = 60
    passivator = org.apache.openejb.core.stateful.SimplePassivater
    timeOut = 20
</Container>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-statelesscontainer-config"></a>

# Apache TomEE

```xml
<Container id="myStatelessContainer" type="STATELESS">
    accessTimeout = 30 seconds
    callbackThreads = 5
    closeTimeout = 5 minutes
    garbageCollection = false
    idleTimeout = 0 minutes
    maxAge = 0 hours
    maxAgeOffset = -1
    maxSize = 10
    minSize = 0
    replaceAged = true
    replaceFlushed = false
    strictPooling = true
    sweepInterval = 5 minutes
</Container>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-system-properties-files"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# System Properties Files

## OpenEJB System Properties File

OpenEJB and TomEE are really configurable in particular through system
properties.

What is not so known is these system properties can be read from several
places. The order is important, it means if the second place provides
the same property as the first one, the first one will be omitted.

Here how it works:

- JVM system properties: -Dxxx=yyy
- user system.properties: the file
  $\{user.home}/.openejb/system.properties
- instance system.properties: conf/system.properties

Note: generally you place in the user system properties file the constant
configuration (check my openejb version for instance).

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-system-properties-files--)

---

<a id="tomee-apache-org-tomee-10-1-docs-system-properties"></a>

# Apache TomEE

```xml
<Connector id="mysql">
    JdbcDriver com.mysql.jdbc.Driver
    JdbcUrl jdbc:mysql://localhost/test
    UserName test
</Connector>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-telnet-console"></a>

# Apache TomEE

```properties
OPENEJB_HOME=/Users/dblevins/Desktop/openejb-1.0
OpenEJB 1.0    build: 20060226-1701
http://www.openejb.org
resources 1
OpenEJB ready.
[init]
```

---

<a id="tomee-apache-org-tomee-10-1-docs-tip-concurrency"></a>

# Apache TomEE

```xml
<?xml version="1.0"?>
<ejb-jar
    xmlns="http://java.sun.com/xml/ns/javaee"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/ejb-jar_3_1.xsd"
    version="3.1">

    <enterprise-beans>
        <session>
            <ejb-name>*</ejb-name>
            <concurrency-management-type>Bean</concurrency-management-type>
        </session>
    <enterprise-beans>
</ejb-jar>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-tip-jersey-client"></a>

# Apache TomEE

```properties
Caused by: java.lang.ClassNotFoundException: com.sun.jersey.core.util.FeaturesAndProperties
at org.apache.catalina.loader.WebappClassLoader.loadClass(WebappClassLoader.java)
at org.apache.catalina.loader.WebappClassLoader.loadClass(WebappClassLoader.java)
at org.apache.tomee.catalina.LazyStopWebappClassLoader.loadClass(LazyStopWebappClassLoader.java)
... 34 more
```

---

<a id="tomee-apache-org-tomee-10-1-docs-tip-weblogic"></a>

# Apache TomEE

```java
Hashtable<String, String> props = new Hashtable<String, String>();
props.put(javax.naming.Context.INITIAL_CONTEXT_FACTORY, "weblogic.jndi.WLInitialContextFactory");
props.put(javax.naming.Context.URL_PKG_PREFIXES, "weblogic.jndi.factories");
props.put("java.naming.provider.url", "t3://your.host.name:7023");
Context ctx = new InitialContext(props);

IService s = (IService) ctx.lookup("java:global.com.test.ServiceImpl!com.test.IService");
```

---

<a id="tomee-apache-org-tomee-10-1-docs-tomcat-object-factory"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Tomcat Object Factory

*The TomcatEjbFactory as discussed in the
[OnJava
article "OpenEJB: EJB for Tomcat"](http://www.onjava.com/pub/a/onjava/2003/02/12/ejb_tomcat.html) is no longer required.*

As of OpenEJB 3.0 references from Servlets to EJBs happen automatically
with usage of the [`@EJB`
annotation](openejbx30:injection-of-other-ejbs-example.html) in the
Servlet, Filter or Listener declared in the `web.xml`.

See the openejbx30:tomcat.html[Tomcat Integration] page for the most
up-to-date details on using OpenEJB inside Tomcat.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-tomcat-object-factory--)

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-and-eclipse"></a>

# Apache TomEE

```xml
<servlet>
    <servlet-name>jsp</servlet-name>
    <servlet-class>org.apache.jasper.servlet.JspServlet</servlet-class>
    ....
    <init-param>
        <param-name>development</param-name>
        <param-value>true</param-value>
    </init-param>
    ....
</servlet>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-and-hibernate"></a>

# Apache TomEE

```xml
<persistence version="1.0"
       xmlns="http://java.sun.com/xml/ns/persistence"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="http://java.sun.com/xml/ns/persistence
       http://java.sun.com/xml/ns/persistence/persistence_1_0.xsd">

  <persistence-unit name="movie-unit">
    <provider>org.hibernate.ejb.HibernatePersistence</provider>
    <jta-data-source>movieDatabase</jta-data-source>
    <non-jta-data-source>movieDatabaseUnmanaged</non-jta-data-source>

    <properties>
      <property name="hibernate.hbm2ddl.auto" value="create-drop"/>
    </properties>
  </persistence-unit>
</persistence>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-and-intellij"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# TomEE and Intellij

|  | Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at .http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. |
| --- | --- |

Intellij is the preferred editor of most of the developers on Apache
TomEE. It’s fast and light and goes out of its way to guess what you’re
thinking and act accordingly in efforts to save you time and increase
your enjoyment. In this regard TomEE and Intellij have a lot in common.

While TomEE works with most IDEs via the Tomcat adapter and this covers
WAR files, JetBrains has stepped up to the plate with a TomEE specific
adapter to allow deployment of the full range of archives that TomEE
supports. The evolving TomEE/Intellij integration sets the pace for all
other IDE integrations.
[Feature requests very
welcome!](http://youtrack.jetbrains.com/issues/IDEA) TomEE is to Intellij what GlassFish is to NetBeans and your
feedback is a critical part of that.

## Getting Started

We will use one of the existing
[examples](https://svn.apache.org/repos/asf/tomee/tomee/trunk/examples/)
for this demo. Let’s import it.
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_01.png)
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_02.png)
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_03.png)

Give a minute while Intellij imports the dependencies.
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_04.png)

It’s time to run the application. Open "Edit Configurations".
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_05.png)

Click the "+" icon and select "TomEE Server" and "Local".
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_06.png)

If your server is still not configured, click the "Configure" button and
point it to your local TomEE installation.
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_07.png)

If you see a warning message like "No artifacts marked for deployment",
click the "Fix" button and select one of the options.
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_08.png)

You can change the "Application Context".
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_09.png)

Now you can run it. Click the "play" button.
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_10.png)
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_11.png)

Your application is up and running.
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_12.png)
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/intellij_integration/windows8_13.png)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-tomee-and-intellij--)

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-and-netbeans"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# TomEE and NetBeans

|  | Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at .http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. |
| --- | --- |

There is some great information over at
[Geertjan’s
Blog](https://blogs.oracle.com/geertjan/entry/tomee_apache_cxf_and_maven) on how to hit the ground running with Netbeans, CXF and Maven.
Geertjan is a Netbeans evangelist and has an incredible insight into
everything Netbeans.

**WORKAROUND**: There is a known issue with Netbeans 8 and TomEE detection
that currently requires the following workaround:

Netbeans 8 has a bug in which it fails to find the
**tomee-common-[version].jar** in the **[TomEE]/lib** directory. The
solution is to simply rename the jar file to an older version.

For example, you have **[TomEE]/lib/tomee-common-1.6.0.2.jar** or
**[TomEE]/lib/tomee-common-1.7.1.jar**. Rename these files to
**[TomEE]/lib/tomee-common-1.6.0.jar**

This should resolve the detection issue and will not break your
installation - Be sure to document the change for yourself as a
reminder.

## Quickstart Check out this video on

[How to Consume REST in a
Java Client](https://www.youtube.com/watch?v=HISV7eagogI)

You can download Netbeans 8 here:
[https://netbeans.org/community/releases/80/](https://netbeans.org/community/releases/80/)

Here is a quick run through on how to set up TomEE. We will use one of
the existing examples for this demo. Let’s import it.

![Subversion Checkout](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_01.png)
![Subversion URL](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_02.png)
![Local Project](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_03.png)
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_04.png)

Click 'Open Project'.

![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_05.png)
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_06.png)

It’s time to add our local TomEE server. Click 'Tools' and then
'Servers'.

![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_07.png)

Select 'Apache Tomcat'.

![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_08.png)

Select your local TomEE directory.

![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_09.png)
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_10.png)
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_11.png)

It’s time to run it. Click the play button.

![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_12.png)

Select 'Apache Tomcat'.

![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_13.png)

Give it some time. It’s building your application.

![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_14.png)

Done. Your server is up and running.

![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_15.png)
![alt text](http://people.apache.org/~tveronezi/tomee/tomee_site/netbeans_integration/windows8_16.png)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-tomee-and-netbeans--)

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-and-security"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Apache TomEE and security

|  | Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at .http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. |
| --- | --- |

Rather than providing its own security implementation, TomEE makes full
use of the security features that are part of Tomcat. Any Catalina realm
is supported, or you can provide your own security module using the
login.config file.

For example, to add some simple security to the
[moviefun
application](http://tomee.apache.org/examples-trunk/webapps/moviefun/README.html) , all we would need to do is:

1. Add some users to the tomcat-users.xml file
2. Add the necessary `@DefineRoles` and `@RolesAllowed` annotations on
   MoviesImpl
3. Add some security config to do HTTP Basic authentication to `web.xml`
   Webservice security is also looked after – username/password based
   security (HTTP basic, or WS-Security) uses the same Tomcat security.
   Certificate based security is also available.

To put it short,

- TomEE uses Tomcat’s Security Realm
- …​.
  Extra TomEE layer adds support for JAAS JACC WS Security

```text
* ....
Supports any org.apache.catalina.Realm implementation
```

- …​.
  E.g. add users to $CATALINA\_BASE/conf/tomcat-users.xml

```text
* ....
Alternatively use login.config to provide your own security module
```

## See Also: [TomEE-and-JAAS](#tomee-apache-org-tomee-10-1-docs-tomee-jaas)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-tomee-and-security--)

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-and-webspheremq"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# TomEE and WebSphere MQ

|  | Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at .http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. |
| --- | --- |

**Steps to integrate TomEE with Websphere MQ**

1. Unzip rar file place jars under tomee/lib
2. Added the below to conf/tomee.xml

**Code:**

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-tomee-and-webspheremq--)

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-directory-structure"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# TomEE Directory Structure

|  | Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at .http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. |
| --- | --- |

## TomEE Directory Layout:

TomEE directory layout is the same as that of Tomcat, with a few changes
as described below.

Considering this root to be the `$tomee-install-dir>`

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-tomee-directory-structure--)

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# TomEE Embedded Maven Plugin

[TomEE Maven Plugin](#tomee-apache-org-tomee-10-1-docs-tomee-maven-plugin) provides a nice way to
run "as in production" a server fully configured keeping the
configuration in the project (easiness of sharing between team members).
However, for modern web development the fact to run the "exploded war"
prevents to develop web resources in place. TomEE embedded maven plugin
solves it directly allowing to directly deploy the war project in place
using "classpath as war" option.

It also allows to use a flat classpath deployment which is often use
with microservices.

## tomee-embedded:run

Full name:

- `org.apache.tomee.maven:tomee-embedded-maven-plugin:8.0.0-M3:run`

Description:

- Run an Embedded TomEE.

Attributes:

- Requires a Maven project to be executed.
- Requires dependency resolution of artifacts in scope: runtime+system.
- Requires dependency collection of artifacts in scope: runtime.

## Optional Parameters

| Name | Type | Since | Description |
| --- | --- | --- | --- |
| ajpPort | int | - | (no description)Default value is:8009.User property is:tomee-embedded-plugin.ajp. |
| applicationCopyFolder | File | - | (no description)Default value is:${project.build.directory}/tomee-embedded/applications.User property is:tomee-plugin.application-copy. |
| applicationScopes | List | - | (no description) |
| applications | List | - | (no description) |
| classpathAsWar | boolean | - | (no description)Default value is:false.User property is:tomee-embedded-plugin.classpathAsWar. |
| clientAuth | String | - | (no description)User property is:tomee-embedded-plugin.clientAuth. |
| containerProperties | Map | - | (no description) |
| context | String | - | (no description)User property is:tomee-embedded-plugin.context. |
| dir | String | - | (no description)Default value is:${project.build.directory}/apache-tomee-embedded.User property is:tomee-embedded-plugin.lib. |
| docBase | File | - | (no description)Default value is:${project.basedir}/src/main/webapp.User property is:tomee-embedded-plugin.docBase. |
| forceJspDevelopment | boolean | - | force webapp to be reloadable.Default value is:true.User property is:tomee-plugin.jsp-development. |
| host | String | - | (no description)Default value is:localhost.User property is:tomee-embedded-plugin.host. |
| httpPort | int | - | (no description)Default value is:8080.User property is:tomee-embedded-plugin.http. |
| httpsPort | int | - | (no description)Default value is:8443.User property is:tomee-embedded-plugin.httpsPort. |
| inlinedServerXml | PlexusConfiguration | - | (no description) |
| inlinedTomEEXml | PlexusConfiguration | - | (no description) |
| keepServerXmlAsThis | boolean | - | (no description)Default value is:false.User property is:tomee-embedded-plugin.keepServerXmlAsThis. |
| keyAlias | String | - | (no description)User property is:tomee-embedded-plugin.keyAlias. |
| keystoreFile | String | - | (no description)User property is:tomee-embedded-plugin.keystoreFile. |
| keystorePass | String | - | (no description)User property is:tomee-embedded-plugin.keystorePass. |
| keystoreType | String | - | (no description)Default value is:JKS.User property is:tomee-embedded-plugin.keystoreType. |
| mavenLog | boolean | - | (no description)Default value is:true.User property is:tomee-embedded-plugin.mavenLog. |
| modules | List | - | (no description)Default value is:${project.build.outputDirectory}.User property is:tomee-embedded-plugin.modules. |
| packaging | String | - | (no description)Default value is:${project.packaging}. |
| quickSession | boolean | - | (no description)Default value is:true.User property is:tomee-embedded-plugin.quickSession. |
| roles | Map | - | (no description) |
| serverXml | File | - | (no description) |
| skipCurrentProject | boolean | - | (no description)Default value is:false.User property is:tomee-plugin.skip-current-project. |
| skipHttp | boolean | - | (no description)Default value is:false.User property is:tomee-embedded-plugin.skipHttp. |
| ssl | boolean | - | (no description)Default value is:false.User property is:tomee-embedded-plugin.ssl. |
| sslProtocol | String | - | (no description)User property is:tomee-embedded-plugin.sslProtocol. |
| stopPort | int | - | (no description)Default value is:8005.User property is:tomee-embedded-plugin.stop. |
| useProjectClasspath | boolean | - | (no description)Default value is:true.User property is:tomee-embedded-plugin.useProjectClasspath. |
| users | Map | - | (no description) |
| warFile | File | - | (no description)Default value is:${project.build.directory}/{project.build.finalName}. |
| webResourceCached | boolean | - | (no description)Default value is:true.User property is:tomee-embedded-plugin.webResourceCached. |
| withEjbRemote | boolean | - | (no description)Default value is:false.User property is:tomee-embedded-plugin.withEjbRemote. |
| workDir | File | - | (no description)Default value is:${project.build.directory}/tomee-embedded-work.User property is:tomee-plugin.work. |

## Parameter Details

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--ajpPort"></a>**ajpPort:**  
(no description)

- Type: int
- Required: No
- User Property: `tomee-embedded-plugin.ajp`
- Default: `8009`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--applicationCopyFolder"></a>**applicationCopyFolder:**  
(no description)

- Type: java.io.File
- Required: No
- User Property: `tomee-plugin.application-copy`
- Default: `${project.build.directory}/tomee-embedded/applications`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--applicationScopes"></a>**applicationScopes:**  
(no description)

- Type: java.util.List
- Required: No

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--applications"></a>**applications:**  
(no description)

- Type: java.util.List
- Required: No

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--classpathAsWar"></a>**classpathAsWar:**  
(no description)

- Type: boolean
- Required: No
- User Property: `tomee-embedded-plugin.classpathAsWar`
- Default: `false`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--clientAuth"></a>**clientAuth:**  
(no description)

- Type: java.lang.String
- Required: No
- User Property: `tomee-embedded-plugin.clientAuth`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--containerProperties"></a>**containerProperties:**  
(no description)

- Type: java.util.Map
- Required: No

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--context"></a>**context:**  
(no description)

- Type: java.lang.String
- Required: No
- User Property: `tomee-embedded-plugin.context`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--dir"></a>**dir:**  
(no description)

- Type: java.lang.String
- Required: No
- User Property: `tomee-embedded-plugin.lib`
- Default: `${project.build.directory}/apache-tomee-embedded`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--docBase"></a>**docBase:**  
(no description)

- Type: java.io.File
- Required: No
- User Property: `tomee-embedded-plugin.docBase`
- Default: `${project.basedir}/src/main/webapp`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--forceJspDevelopment"></a>**forceJspDevelopment:**  
force webapp to be reloadable

- Type: boolean
- Required: No
- User Property: `tomee-plugin.jsp-development`
- Default: `true`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--host"></a>**host:**  
(no description)

- Type: java.lang.String
- Required: No
- User Property: `tomee-embedded-plugin.host`
- Default: `localhost`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--httpPort"></a>**httpPort:**  
(no description)

- Type: int
- Required: No
- User Property: `tomee-embedded-plugin.http`
- Default: `8080`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--httpsPort"></a>**httpsPort:**  
(no description)

- Type: int
- Required: No
- User Property: `tomee-embedded-plugin.httpsPort`
- Default: `8443`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--inlinedServerXml"></a>**inlinedServerXml:**  
(no description)

- Type: org.codehaus.plexus.configuration.PlexusConfiguration
- Required: No

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--inlinedTomEEXml"></a>**inlinedTomEEXml:**  
(no description)

- Type: org.codehaus.plexus.configuration.PlexusConfiguration
- Required: No

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--keepServerXmlAsThis"></a>**keepServerXmlAsThis:**  
(no description)

- Type: boolean
- Required: No
- User Property: `tomee-embedded-plugin.keepServerXmlAsThis`
- Default: `false`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--keyAlias"></a>**keyAlias:**  
(no description)

- Type: java.lang.String
- Required: No
- User Property: `tomee-embedded-plugin.keyAlias`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--keystoreFile"></a>**keystoreFile:**  
(no description)

- Type: java.lang.String
- Required: No
- User Property: `tomee-embedded-plugin.keystoreFile`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--keystorePass"></a>**keystorePass:**  
(no description)

- Type: java.lang.String
- Required: No
- User Property: `tomee-embedded-plugin.keystorePass`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--keystoreType"></a>**keystoreType:**  
(no description)

- Type: java.lang.String
- Required: No
- User Property: `tomee-embedded-plugin.keystoreType`
- Default: `JKS`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--mavenLog"></a>**mavenLog:**  
(no description)

- Type: boolean
- Required: No
- User Property: `tomee-embedded-plugin.mavenLog`
- Default: `true`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--modules"></a>**modules:**  
(no description)

- Type: java.util.List
- Required: No
- User Property: `tomee-embedded-plugin.modules`
- Default: `${project.build.outputDirectory}`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--packaging"></a>**packaging:**  
(no description)

- Type: java.lang.String
- Required: No
- Default: `${project.packaging}`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--quickSession"></a>**quickSession:**  
(no description)

- Type: boolean
- Required: No
- User Property: `tomee-embedded-plugin.quickSession`
- Default: `true`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--roles"></a>**roles:**  
(no description)

- Type: java.util.Map
- Required: No

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--serverXml"></a>**serverXml:**  
(no description)

- Type: java.io.File
- Required: No

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--skipCurrentProject"></a>**skipCurrentProject:**  
(no description)

- Type: boolean
- Required: No
- User Property: `tomee-plugin.skip-current-project`
- Default: `false`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--skipHttp"></a>**skipHttp:**  
(no description)

- Type: boolean
  -Required: No
- User Property: `tomee-embedded-plugin.skipHttp`
- Default: `false`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--ssl"></a>**ssl:**  
(no description)

- Type: boolean
- Required: No
- User Property: `tomee-embedded-plugin.ssl`
- Default: `false`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--sslProtocol"></a>**sslProtocol:**  
(no description)

- Type: java.lang.String
- Required: No
- User Property: `tomee-embedded-plugin.sslProtocol`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--stopPort"></a>**stopPort:**  
(no description)

- Type: int
- Required: No
- User Property: `tomee-embedded-plugin.stop`
- Default: `8005`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--useProjectClasspath"></a>**useProjectClasspath:**  
(no description)

- Type: boolean
- Required: No
- User Property: `tomee-embedded-plugin.useProjectClasspath`
- Default: `true`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--users"></a>**users:**  
(no description)

- Type: java.util.Map
- Required: No

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--warFile"></a>**warFile:**  
(no description)

- Type: java.io.File
- Required: No
- Default: `${project.build.directory}/{project.build.finalName}`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--webResourceCached"></a>**webResourceCached:**  
(no description)

- Type: boolean
- Required: No
- User Property: `tomee-embedded-plugin.webResourceCached`
- Default: `true`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--withEjbRemote"></a>**withEjbRemote:**  
(no description)

- Type: boolean
- Required: No
- User Property: `tomee-embedded-plugin.withEjbRemote`
- Default: `false`

<a id="tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--workDir"></a>**workDir:**  
(no description)

- Type: java.io.File
- Required: No
- User Property: `tomee-plugin.work`
- Default: `${project.build.directory}/tomee-embedded-work`

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-tomee-embedded-maven-plugin--)

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-jaas"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# JAAS and TomEE

## Purpose

You want to use JAAS in TomEE with custom (or OpenEJB) LoginModules.

## Solution

TomEE tries to keep as possible as it is Tomcat so simply configure your
JAAS LoginModule as in Tomcat.

Note: only the first one will be used.

## Configuration

Add to your `CATALINA_OPTS` the `java.security.auth.login.config` system
property:

```properties
-Djava.security.auth.login.config=$CATALINA_BASE/conf/login.config
```

Configure your realm in server.xml file

```xml
<?xml version='1.0' encoding='utf-8'?>
<Server port="8005" shutdown="SHUTDOWN">
  <Listener className="org.apache.tomee.loader.OpenEJBListener" />
  <Listener className="org.apache.catalina.security.SecurityListener" />

  <Service name="Catalina">
    <Connector port="8080" protocol="HTTP/1.1"
               connectionTimeout="20000"
               redirectPort="8443" />
    <Connector port="8009" protocol="AJP/1.3" redirectPort="8443" />
    <Engine name="Catalina" defaultHost="localhost">
      <!-- here is the magic -->
      <Realm className="org.apache.catalina.realm.JAASRealm" appName="PropertiesLogin"
             userClassNames="org.apache.openejb.core.security.jaas.UserPrincipal"
             roleClassNames="org.apache.openejb.core.security.jaas.GroupPrincipal">
      </Realm>

      <Host name="localhost"  appBase="webapps"
            unpackWARs="true" autoDeploy="true" />
    </Engine>
  </Service>
</Server>
```

Configure your `login.config` file

```java
PropertiesLogin {
    org.apache.openejb.core.security.jaas.PropertiesLoginModule required
    Debug=false
    UsersFile="users.properties"
    GroupsFile="groups.properties";
};
```

Configure your login module specifically (`users.properties` for
snippets of this page for instance).

Place `users.properties` and `groups.properties` files in
`$CATALINA_BASE/conf/` folder. `users.properties` file contains
username and associated password entries, ex.:

```properties
me=password
tomee=tomee
```

`groups.properties` file specifies groups and their users, ex.:

```properties
my-role=me
manager-gui=tomee,me
tomee-admin=tomee
```

**NOTE**: `users.properties` and `groups.properties` file names and file
location are fixed. If other names are used, the files must be placed in
`%CATALINA_BASE/lib/` folder instead.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-tomee-jaas--)

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-logging-in-eclipse"></a>

# Apache TomEE

```properties
-Djava.util.logging.config.file="<tomee>/conf/logging.properties"
-Djava.util.logging.manager=org.apache.juli.ClassLoaderLogManager
```

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-logging"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# null

|  | Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at .http://www.apache.org/licenses/LICENSE-2.0. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License. |
| --- | --- |

Logging in TomEE is configured using the conf/logging.properties file.
Here are some of the benefits:

You do not have to author a logging.properties from scratch.

You get one with sensible defaults.

If you did modify the default file, and you wanted to revert back to the
default file generated by TomEE, all you have to do is

Delete or rename the file e.g. rename it to logging.properties.BAK .

Restart the server

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-tomee-logging--)

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-maven-plugin"></a>

# Apache TomEE

```xml
<plugins>
    <plugin>
      <groupId>org.apache.tomee.maven</groupId>
      <artifactId>tomee-maven-plugin</artifactId>
      <version>${TOMEE_VERSION}</version>
      <configuration>
        <tomeeVersion>${TOMEE_VERSION}</tomeeVersion>
        <tomeeClassifier>plus</tomeeClassifier>
      </configuration>
    </plugin>
</plugins>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-mp-getting-started"></a>

# Apache TomEE

```xml
<dependency>
  <groupId>org.apache.tomee</groupId>
  <artifactId>jakartaee-api</artifactId>
  <version>9.xxx</version>
  <scope>provided</scope>
</dependency>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-version-policies"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# null

1. # TomEE
   versioning policies

TomEE version policy is made of three dot-separated numbers: x.y.z

- x is the major version. It corresponds to the maximum Java EE
  specification supported by TomEE and a codebase generation, starting
  with value 1 for Java EE 6 support. Next major will be 7 targeting
  JavaEE 7 etc…​
- y is the minor version. It corresponds to a features level, starting
  at 0. Initial TomEE release was 1.0.0, followed by 1.5.0 because of the
  accumulation of features changes delivered since 1.0.0. Note that
  features changes must comply with the Java EE specification level given
  by TomEE’s major version.
- z is the fix level. It corresponds to bug fixes changes without new
  features, starting at 0. Upgrades of embedded components (Tomcat,
  OpenEJB, MyFaces, etc.) are normally part of bug fixes releases.

These policies allow a Java EE application to be certified with a given
TomEEE version x.y.z and to be compatible with TomEE versions x.y1.\*
with y1>y and compatible with TomEE version x.y.z1 with z1>z.

Important: being JavaEE certified means the server passed the Test
Compatibility Kit (TCK). To simplify/summarize it is a big test suite to
validate you are compliant with a JavaEE version. TomEE 1.x passed the
JavaEE 6 TCK which was donated to Apache Software Foundation by Oracle.
This donation hasn’t been done to Apache for JavaEE 7 so TomEE is not
able for now to be validated against it and therefore TomEE versions
targeting JavaEE 7 are not yet certified.

To make it more explicit here is a small table showing the targeted
JavaEE version and the related certification state by version:

TomEE Version

JavaEE Version

Certified

1.x

6

Yes

7.x

7

No

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-tomee-version-policies--)

---

<a id="tomee-apache-org-tomee-10-1-docs-tomee-webapp"></a>

# Apache TomEE

```xml
<servlet>
  <servlet-name>ServerServlet</servlet-name>
  <servlet-class>org.apache.openejb.server.httpd.ServerServlet</servlet-class>
</servlet>

<servlet-mapping>
  <servlet-name>ServerServlet</servlet-name>
  <url-pattern>/myejbs/*</url-pattern>
</servlet-mapping>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-topic-config"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Topic Configuration

A Topic can be declared via xml in the `<tomee-home>/conf/tomee.xml`
file or in a `WEB-INF/resources.xml` file using a declaration like the
following. All properties in the element body are optional.

```xml
<Resource id="myTopic" type="jakarta.jms.Topic">
    destination =
</Resource>
```

Alternatively, a Topic can be declared via properties in the
`<tomee-home>/conf/system.properties` file or via Java VirtualMachine
`-D` properties. The properties can also be used when embedding TomEE
via the `jakarta.ejb.embeddable.EJBContainer` API or `InitialContext`

```properties
myTopic = new://Resource?type=jakarta.jms.Topic
myTopic.destination =
```

Properties and xml can be mixed. Properties will override the xml
allowing for easy configuration change without the need for $\{} style
variable substitution. Properties are not case-sensitive. If a property
is specified that is not supported by the declared Topic a warning will
be logged. If a Topic is needed by the application and one is not
declared, TomEE will create one dynamically using default settings.
Multiple Topic declarations are allowed. # Supported Properties

Property

Type

Default

Description

destination

String

Specifies the name of the topic

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-topic-config--)

---

<a id="tomee-apache-org-tomee-10-1-docs-transaction-annotations"></a>

# Apache TomEE

```java
@Stateless
public static class MyBean implements MyBusinessInterface {

    @TransactionAttribute(TransactionAttributeType.MANDATORY)
    public String codeRed(String s) {
    return s;
    }

    public String codeBlue(String s) {
    return s;
    }
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-transactionmanager-config"></a>

# Apache TomEE

```xml
<TransactionManager id="myTransactionManager" type="TransactionManager">
    adler32Checksum = true
    bufferSizeKb = 32
    checksumEnabled = true
    defaultTransactionTimeout = 10 minutes
    flushSleepTime = 50 Milliseconds
    logFileDir = txlog
    logFileExt = log
    logFileName = howl
    maxBlocksPerFile = -1
    maxBuffers = 0
    maxLogFiles = 2
    minBuffers = 4
    threadsWaitingForceThreshold = -1
    txRecovery = false
</TransactionManager>
```

---

<a id="tomee-apache-org-tomee-10-1-docs-understanding-callbacks"></a>

# Apache TomEE

```java
public class Plant {
    @AroundInvoke
    public Object a(InvocationContext ctx) throws Exception {
        return ctx.proceed();
    }
}

public class Fruit extends Plant {
    @AroundInvoke
    public Object b(InvocationContext ctx) throws Exception {
        return ctx.proceed();
    }
}

@Stateless
public class Apple extends Fruit implements AppleLocal {
    @AroundInvoke
    public Object c(InvocationContext ctx) throws Exception {
        return ctx.proceed();
    }

    public String grow(){
        return "ready to pick";
    }
}

public interface AppleLocal {
    public String grow();
}
```

---

<a id="tomee-apache-org-tomee-10-1-docs-understanding-the-directory-layout"></a>

# Apache TomEE

```java
apache-openejb-[version]\apps
apache-openejb-[version]\bin
apache-openejb-[version]\conf
apache-openejb-[version]\data
apache-openejb-[version]\lib
apache-openejb-[version]\logs
apache-openejb-[version]\LICENSE
apache-openejb-[version]\NOTICE
apache-openejb-[version]\README.txt
```

---

<a id="tomee-apache-org-tomee-10-1-docs-unix-daemon"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Unix Daemon

Apache TomEE can be run as a daemon using the
[jsvc](http://commons.apache.org/daemon/jsvc.html) tool from the
[Apache Commons Daemon](http://commons.apache.org/daemon) project.

Source tarballs for `jsvc` are included with Tomcat and therefore can be
found in TomEE as well. These need to be compiled before jsvc can be
used.

## Building jsvc

First, we’ll need to locate and unpack the
`commons-daemon-native.tar.gz`

```bash
cd $TOMEE_HOME/bin
tar xzvf commons-daemon-native.tar.gz
cd commons-daemon-1.0.7-native-src/unix/
```

Note that the `commons-daemon-1.0.7-native-src` directory may have a
slightly different version number.

Second, we’ll need to build the `jsvc` binary. Under a UNIX operating
system you will need:

- An ANSI-C compliant compiler (GCC is good)
- GNU Make
- A Java Platform 2 compliant SDK

You have to specify the `JAVA_HOME` of the SDK either with the
`--with-java=<dir>` parameter or set the `JAVA_HOME` environment to
point to your SDK installation. For example:

```properties
./configure --with-java=/usr/java
```

or

```properties
export JAVA_HOME
./configure
```

If your operating system is supported, configure will go through
cleanly, otherwise it will report an error (please send us the details
of your OS/JDK, or a patch against the sources). To build the binaries
and libraries simply do:

```java
make
```

This will generate the executable file `jsvc`.

Finally, we’ll want to set the execution bits and move the `jsvc` binary

```properties
chmod 755 jsvc
mv jsvc $TOMEE_HOME/bin
```

Done!

As one script, the above might look like:

```bash
cd $TOMEE_HOME/bin
tar xzvf commons-daemon-native.tar.gz
cd commons-daemon-1.0.7-native-src/unix/
./configure
make
chmod 755 jsvc
mv jsvc ../..
```

## Starting (unix)

```bash
sudo "$TOMEE_HOME/bin/jsvc" -cp "$TOMEE_HOME/bin/bootstrap.jar:$TOMEE_HOME/bin/tomcat-juli.jar" \
    "-javaagent:$TOMEE_HOME/lib/openejb-javaagent.jar" -outfile "$TOMEE_HOME/logs/catalina.out" \
    -errfile "$TOMEE_HOME/logs/catalina.err" org.apache.catalina.startup.Bootstrap
```

## Starting (osx)

For a 64-bit JVM such as OSX Lion

```bash
sudo arch -arch x86_64 "$TOMEE_HOME/bin/jsvc" -jvm server -cp "$TOMEE_HOME/bin/bootstrap.jar:$TOMEE_HOME/bin/tomcat-juli.jar" \
    "-javaagent:$TOMEE_HOME/lib/openejb-javaagent.jar" -outfile "$TOMEE_HOME/logs/catalina.out" \
    -errfile "$TOMEE_HOME/logs/catalina.err" org.apache.catalina.startup.Bootstrap
```

For a 32-bit JVM

```bash
sudo arch -arch i386 "$TOMEE_HOME/bin/jsvc" -jvm server -cp "$TOMEE_HOME/bin/bootstrap.jar:$TOMEE_HOME/bin/tomcat-juli.jar" \
    "-javaagent:$TOMEE_HOME/lib/openejb-javaagent.jar" -outfile "$TOMEE_HOME/logs/catalina.out" \
    -errfile "$TOMEE_HOME/logs/catalina.err" org.apache.catalina.startup.Bootstrap
```

### Note on formatting

Note that `\` at the end of each line is unix syntax to keep everything
effectively as one line and one command. The command is simply too long
to show as one line on a fixed width html page. The `\` can be removed
as long as the resulting command is one long line.

## Common Issues

Ensure your `$TOME_HOME` and `$JAVA_HOME` variables are set correctly.
You should see similar output with the following two commands

```bash
mingus:~ 01:51:37
$ ls $TOMEE_HOME
LICENSE     RELEASE-NOTES   bin     endorsed    logs        webapps
NOTICE      RUNNING.txt conf        lib     temp        work

mingus:~ 01:51:46
$ ls $JAVA_HOME
bin bundle  lib man
```

The `jsvc -debug` option can also show useful information for
troubleshooting:

```java
$TOMEE_HOME/bin/jsvc -debug
```

Note on OSX, `$JAVA_HOME` should be set to
`/System/Library/Frameworks/JavaVM.framework/Home`

## Further documentation

See also the full Apache Commons Daemon documentation for jsvc.

- [http://commons.apache.org/daemon/jsvc.html](http://commons.apache.org/daemon/jsvc.html)

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-unix-daemon--)

---

<a id="tomee-apache-org-tomee-10-1-docs-validation-tool"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Validation Tool

# NAME

openejb validate - OpenEJB Validation Tool

# SYNOPSIS

openejb validate [options](options.html) jarfiles

# NOTE

The OpenEJB Validation tool must be executed from the OPENEJB\_HOME
directory. This is the directory where OpenEJB was installed or
unpacked. For the remainder of this document, we will assume you
unpacked OpenEJB into the directory C:.

In Windows, the validation tool can be executed as follows:

*C:> openejb validate -help*

{warning} There is a bug in the openejb.bat script of OpenEJB 0.9.0
that doesn’t allow you to execute the validate command as above. For
that release, Windows users can execute the following command: *C:>
bin.bat -help*

{warning}

In UNIX, Linux, or Mac OS X, the deploy tool can be executed as follows:

`[user@host openejb]([user@host-openejb.html](mailto:user@host-openejb.html)) # ./openejb.sh validate -help`

Depending on your OpenEJB version, you may need to change execution bits
to make the scripts executable. You can do this with the following
command.

`[user@host openejb]([user@host-openejb.html](mailto:user@host-openejb.html)) # chmod 755 openejb.sh bin/*.sh`

From here on out, it will be assumed that you know how to execute the
right openejb script for your operating system and commands will appear
in shorthand as show below.

*openejb validate -help*

# DESCRIPTION

The validation tool currently checks for the following things: \* Does
the Jar have all the ejb-class class files \* Does the Jar have all the
home class files \* Does the Jar have all the remote class files \* Is the
ejb-class a sub-interface of SessionBean or EntityBean \* Is the home a
sub-interface of EJBHome \* Is the remote a sub-interface of EJBObject \*
Are all the methods in the remote interface implemented in the ejb-class
\* Are there create methods in the home interface \* Are the create
methods in the home interface implemented in the ejb-class \* Are the
required post create methods in the ejb-class of EntityBeans \* Are there
any create methods in the ejb-class, but not in the home interface

More checks will be added in the future.

# OPTIONS

-v

Sets the output level to 1. This will output just the minimum details on
each failure.

-vv

Default. Sets the output level to 2. Outputs one line summaries of each
failure. This is the default output level.

-vvv

Sets the output level to 3. Outputs verbose details on each failure,
usually with details on how to correct the failures.

-xml

Outputs information in well-formed XML.

-nowarn

Suppresses warnings.

-version

Print the version.

-help

Print this help message.

-examples

Show examples of how to use the options.

# COMMON ISSUES

## Mislocated class or NoClassDefFoundError

The short explanation is that the parent doesn’t have all the classes it
needs as some of them are only in the child classloader, where the
parent can’t see them.

This would occur, for example, if a class was loaded by the parent
classloader, but that class' superclass wasn’t visible to the parent
classloader, perhaps because it is only in the child classloader.

Here is a more concrete example:

```java
public interface Person extends EJBObject {
}

public interface Employee extends Person {
}
```

Ok, so when we build our ejb jar, we put both the Person and Employee
interfaces in the jar, so everything should be good (so we think). But
now let’s say that for some reason the Employee interface is also in
another jar and that jar was loaded into the system classpath.

When a new classloader is created for my ejb-jar at runtime and the
system attempts to load the Employee interface, the call goes right
through that classloader and down to the system classloader. The
Employee interface is found, because it was accidentally added to that
extra jar in the system classpath. So now the system classloader goes
looking for Employee’s superinterface, Person, where it immediately blows
up and throws a NoClassDefFoundError: Person.

Most people will look at their ejb-jar and think, "But all my classes
are there!?", which is true. It really doesn’t matter though, because
one of those classes is also in the parent classloader. The first call
to load that class will bypass your classloader completely and go to the
parent. Once there, it is the parent’s job to find *all* the dependent
classes. If it can’t …​ NoClassDefFoundError.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-validation-tool--)

---

<a id="tomee-apache-org-tomee-10-1-docs-version-checker"></a>

# Apache TomEE

![Preloader image](tomee.apache.org/img/loader.gif)

Toggle navigation

[

![](tomee.apache.org/img/apache_tomee-logo.svg)
](/ "Apache TomEE")

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

# Checking Your OpenEJB Version

## Update checker

To check your OpenEJB version each time OpenEJB/TomEE starts simple add
the system property openejb.version.check=true.

Note: it can be done through system.properties files.

### Be simple. Be certified. Be Tomcat.

##### "A good application in a good server"

- [](https://www.facebook.com/ApacheTomEE/)
- [](https://twitter.com/apachetomee)

##### [Privacy Policy](../../privacy-policy.html)

##### [Documentation](../../latest/docs/)

- [How to configure](../../latest/docs/admin/configuration/index.html)
- [Dir. Structure](../../latest/docs/admin/file-layout.html)
- [Testing](../../latest/docs/developer/testing/index.html)
- [Clustering](../../latest/docs/admin/cluster/index.html)

##### [Examples](../../latest/examples/)

- [CDI Interceptor](../../latest/examples/simple-cdi-interceptor.html)
- [REST with CDI](../../latest/examples/rest-cdi.html)
- [EJB](../../latest/examples/ejb-examples.html)
- [JSF](../../latest/examples/jsf-managedBean-and-ejb.html)

##### [Community](../../community/index.html)

- [Contributors](../../community/contributors.html)
- [Social](../../community/social.html)
- [Sources](../../community/sources.html)

##### [Security](../../security/index.html)

- [Apache Security](https://apache.org/security)
- [Security Projects](https://apache.org/security/projects.html)
- [CVE](https://cve.mitre.org)

Copyright © 1999-2026 The Apache Software Foundation, Licensed under the Apache License, Version 2.0. Apache TomEE, TomEE, Apache, the Apache logo, and the Apache TomEE project logo are trademarks of The Apache Software Foundation. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

- [Documentation](../../docs.html)
- [Community](../../community/index.html)
- [Security](../../security/security.html)
- [Downloads](../../download.html)

[](#tomee-apache-org-tomee-10-1-docs-version-checker--)