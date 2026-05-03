<a id="synapse-apache-org-userguide-installation"></a>

# Apache Synapse – Apache Synapse - Installation Guide

## <a id="synapse-apache-org-userguide-installation--Apache_Synapse_Installation_Guide"></a>Apache Synapse Installation Guide

Welcome to Apache Synapse Installation Guide. This guide provides information on,

- [Prerequisites for Installing Apache Synapse](#synapse-apache-org-userguide-installation--Prerequisites)
- [Distribution Packages](#synapse-apache-org-userguide-installation--Distribution)
- [Installing Synapse](#synapse-apache-org-userguide-installation--Installing)
  - [Installing on \*nix (Linux/macOS/Solaris)](#synapse-apache-org-userguide-installation--InstallingLinux)
  - [Installing on MS Windows](#synapse-apache-org-userguide-installation--InstallingWin)
- [Building Synapse Using the Source Distribution](#synapse-apache-org-userguide-installation--Building)

<a id="synapse-apache-org-userguide-installation--Prerequisites"></a>

## <a id="synapse-apache-org-userguide-installation--Prerequisites_for_Installing_Apache_Synapse"></a>Prerequisites for Installing Apache Synapse

You should have following pre-requisites installed on your system to run Apache
Synapse.

| Java SE                                 Development Kit | 1.8.0_141 or higher (For instructions on setting up the JDK on different                             operating systems, visitJava homepage.) |
| --- | --- |
| Apache Ant- To run Synapse samples | To compile and run the sample clients, an Ant installation is                                 required.                                 Ant 1.7.0 version or higher is recommended. |
| Apache Maven- To                             build Synapse from the source | To build Apache Synapse from its source distribution, you will need                             Maven 3.2.x or later. |
| Memory | No minimum requirement - A heap size of 1GB is generally                             sufficient to process typical SOAP messages. Requirements may vary                             with larger message size and on the number of messages processed                             concurrently. |
| Disk | No minimum requirement. The installation will require ~75 MB                             excluding space allocated for log files and databases. |
| Operating System | Linux, Solaris, macOS, MS Windows - XP/2003/2008 (Not fully tested on Windows 			    Vista/7/8/10). Since Apache Synapse is a Java application, it will                             generally be possible to run it on other operating systems with a                             JDK 1.6.x or higher runtime. Linux is recommended for production                             deployments. |

<a id="synapse-apache-org-userguide-installation--Distribution"></a>

## <a id="synapse-apache-org-userguide-installation--Distribution_Packages"></a>Distribution Packages

The following distribution packages are available for [download](http://synapse.apache.org/download.html).

1. Binary Distribution: Includes binary files for Linux, macOS and
   MS Windows operating systems, compressed into a single zip file. Recommended
   for normal users.
2. Source Distribution: Includes the source code for Linux, macOS and MS Windows
   operating systems, compressed into a single zip file which can be used to build
   the binaries. Recommended for advanced users.

<a id="synapse-apache-org-userguide-installation--Installing"></a>

## <a id="synapse-apache-org-userguide-installation--Installing_Synapse"></a>Installing Synapse

The following guide will take you through the binary distribution installation
on different platforms.

<a id="synapse-apache-org-userguide-installation--InstallingLinux"></a>

### <a id="synapse-apache-org-userguide-installation--Installing_on_nix_LinuxmacOSSolaris"></a>Installing on \*nix (Linux/macOS/Solaris)

1. [Download](http://synapse.apache.org/download.html) Apache
   Synapse binary distribution.
2. Extract the downloaded zip archive to where you want Synapse installed
   (e.g. into /opt).
3. Set the JAVA\_HOME environment variable to your Java home using the export
   command or by editing /etc/profile, and add the JAVA\_HOME/bin
   directory to your PATH.
4. Execute the Synapse start script or the daemon script from the bin
   directory of your Synapse installation.
     
   i.e., ./synapse.sh OR ./synapse-daemon.sh start
5. Synapse is now ready to accept messages for mediation.

<a id="synapse-apache-org-userguide-installation--InstallingWin"></a>

### <a id="synapse-apache-org-userguide-installation--Installing_on_MS_Windows"></a>Installing on MS Windows

1. [Download](http://synapse.apache.org/download.html) Apache
   Synapse binary distribution.
2. Extract the downloaded zip archive to where you want Synapse installed
   (e.g. into C:\Synapse).
3. Set the JAVA\_HOME environment variable to your Java home using the set
   command or Windows System Properties dialog, and add the JAVA\_HOME\bin
   directory to your PATH.
4. Execute the Synapse start script or the service installation script from
   the bin directory of your Synapse installation.
     
   i.e., synapse.bat OR install-synapse-service.bat
5. Synapse is now ready to accept messages for mediation.

<a id="synapse-apache-org-userguide-installation--Building"></a>

## <a id="synapse-apache-org-userguide-installation--Building_Synapse_Using_the_Source_Distribution"></a>Building Synapse Using the Source Distribution

Apache Synapse build is based on [ Apache
Maven 3](http://maven.apache.org/). Hence, it is a prerequisite to have Maven (version 3.2.0 or later)
installed in order to build Synapse from the source distribution. Instructions on
installing Maven 3 are available on the [ Maven
website](http://maven.apache.org/). Follow these steps to build Synapse after setting up Maven 3.

1. [Download](http://synapse.apache.org/download.html)
   the source
   distribution, which is available as a zip archive. All the necessary
   build scripts are included with this distribution.
   Alternatively Synapse can be cloned from the Github [
   repository](https://github.com/apache/synapse).
2. Extract the source archive to a directory of your choice. If cloned from Github repository,
   change directories into downloaded project root.
3. Run **mvn clean install** command inside that directory to build
   Synapse. Note that you will require a connection to the Internet for the Maven
   build to download dependencies required for the build.

This will create the complete set of release artifacts including the binary
distribution in the modules/distribution/target/ directory which can be installed
using the above instructions.

---

<a id="synapse-apache-org-userguide-config"></a>

# Apache Synapse – Apache Synapse - Configuration Language Guide

<a id="synapse-apache-org-userguide-config--Intro"></a>

## <a id="synapse-apache-org-userguide-config--Introduction"></a>Introduction

Apache Synapse loads its configuration from a set of XML files. This enables the
user to easily hand edit the configuration, maintain backups and even include the
entire configuration in a version control system for easier management and control.
For an example one may check-in all Synapse configuration files into a version
control system such as Subversion and easily move the configuration files from
development, through QA, staging and into production.

All the configuration files related to Synapse are housed in the repository/conf/synapse-config
directory of the Synapse installation. Synapse is also capable of loading certain
configuration elements (eg: sequences, endpoints) from an external SOA registry.
When using a registry to store fragments of the configuration, some configuration
elements such as endpoints can be updated dynamically while Synapse is executing.

This article describes the hierarchy of XML files from which Synapse reads its
configuration. It describes the high level structure of the file set and the XML
syntax used to configure various elements in Synapse.

## <a id="synapse-apache-org-userguide-config--Contents"></a>Contents

- [Synapse Configuration](#synapse-apache-org-userguide-config--SynapseConfig)
  - [Service Mediation (Proxy Services)](#synapse-apache-org-userguide-config--ServiceMediation)
  - [Message Mediation](#synapse-apache-org-userguide-config--MessageMediation)
  - [Task Scheduling](#synapse-apache-org-userguide-config--TaskScheduling)
  - [Eventing](#synapse-apache-org-userguide-config--Eventing)
- [Functional Components Overview](#synapse-apache-org-userguide-config--Overview)
  - [Mediators and Sequences](#synapse-apache-org-userguide-config--MediatorsAndSequences)
  - [Endpoints](#synapse-apache-org-userguide-config--Endpoints)
  - [Proxy Services](#synapse-apache-org-userguide-config--ProxyServices)
  - [Scheduled Tasks](#synapse-apache-org-userguide-config--ScheduledTasks)
  - [Templates](#synapse-apache-org-userguide-config--Templates)
  - [Remote Registry and Local Registry](#synapse-apache-org-userguide-config--Registry)
  - [APIs](#synapse-apache-org-userguide-config--API)
  - [Priority Executors](#synapse-apache-org-userguide-config--PriorityExecutors)
  - [Message Stores and Processors](#synapse-apache-org-userguide-config--Stores)
- [Synapse Configuration Files](#synapse-apache-org-userguide-config--ConfigFiles)
- [Configuration Syntax](#synapse-apache-org-userguide-config--Syntax)
- [Registry Configuration](#synapse-apache-org-userguide-config--RegistryConfig)
- [Local Entry (Local Registry) Configuration](#synapse-apache-org-userguide-config--LocalEntryConfig)
- [Sequence Configuration](#synapse-apache-org-userguide-config--SequenceConfig)
- [Endpoint Configuration](#synapse-apache-org-userguide-config--EndpointConfig)
  - [Address Endpoint](#synapse-apache-org-userguide-config--AddressEndpointConfig)
  - [Default Endpoint](#synapse-apache-org-userguide-config--DefaultEndpointConfig)
  - [WSDL Endpoint](#synapse-apache-org-userguide-config--WSDLEndpointConfig)
  - [Load Balance Endpoint](#synapse-apache-org-userguide-config--LBEndpointConfig)
  - [Dynamic Load Balance Endpoint](#synapse-apache-org-userguide-config--DLBEndpointConfig)
  - [Fail-Over Endpoint](#synapse-apache-org-userguide-config--FOEndpointConfig)
  - [Recipient List Endpoint"](#synapse-apache-org-userguide-config--RecipientListEndpointConfig)
- [Proxy Service Configuration](#synapse-apache-org-userguide-config--ProxyServiceConfig)
- [Scheduled Task Configuration](#synapse-apache-org-userguide-config--TaskConfig)
- [Template Configuration](#synapse-apache-org-userguide-config--TemplateConfig)
- [Event Source Configuration](#synapse-apache-org-userguide-config--EventSourceConfig)
- [API Configuration](#synapse-apache-org-userguide-config--APIConfig)
- [Priority Executor Configuration](#synapse-apache-org-userguide-config--ExecutorConfig)
- [Message Stores and Processors Configuration](#synapse-apache-org-userguide-config--StoresConfig)

<a id="synapse-apache-org-userguide-config--SynapseConfig"></a>

## <a id="synapse-apache-org-userguide-config--The_Synapse_Configuration"></a>The Synapse Configuration

A typical Synapse configuration is comprised of sequences, endpoints, proxy services
and local entries. In certain advanced scenarios, Synapse configuration may also
contain scheduled tasks, event sources, messages stores and priority executors.
Synapse configuration may also include a registry adapter through which the mediation
engine can import various resources to the mediation engine at runtime. Following
diagram illustrates different functional components of Synapse and how they interact
with each other.

![](synapse.apache.org/images/synapse-flow.png)

All the functional components of the Synapse configuration are configured through
XML files. The Synapse configuration language governs the XML syntax used to define
and configure different types of components. This configuration language is now
available as a [XML schema](http://synapse.apache.org/ns/2010/04/configuration/synapse_config.xsd).

Typically the Synapse ESB is used to mediate the message flow between a client
and a back-end service implementation. Therefore Synapse can accept a message on
behalf of the actual service and perform a variety of mediation tasks on it such
as authentication, validation, transformation, logging and routing. Synapse can also
detect timeouts and other communication errors when connecting to back-end services.
In addition to that users can configure Synapse to perform load balancing, access
throttling and response caching. In case of a fault scenario, such as an authentication
failure or a schema validation failure, the Synapse ESB can be configured to return
a custom message or a SOAP fault to the requesting client without forwarding the
message to the back-end service. All these scenarios and use cases can be put into
action by selecting the right set of functional components of Synapse and combining
them appropriately through the Synapse configuration.

Depending on how functional components are used in the Synapse configuration, Synapse
can execute in one or more of the following operational modes.

<a id="synapse-apache-org-userguide-config--ServiceMediation"></a>

### <a id="synapse-apache-org-userguide-config--Service_Mediation_Proxy_Services"></a>Service Mediation (Proxy Services)

In service mediation, the Synapse ESB exposes a service endpoint on the ESB, which
accepts messages from clients. Typically these services acts as proxies for existing
(external) services, and the role of Synapse would be to 'mediate' these messages
before they are delivered to the actual service. In this mode, Synapse could expose
a service already available in one transport, over a different transport; or expose
a service that uses one schema or WSDL as a service that uses a different schema or
WSDL. A Proxy service could define the transports over which the service is exposed,
and point to the mediation sequences that should be used to process request and
response messages. A proxy service maybe a SOAP or a REST/POX service over HTTP/S or
SOAP, POX, plain text or binary/legacy service for other transports such as JMS
and VFS file systems.

<a id="synapse-apache-org-userguide-config--MessageMediation"></a>

### <a id="synapse-apache-org-userguide-config--Message_Mediation"></a>Message Mediation

In message mediation, Synapse acts as a transparent proxy for clients. This way,
Synapse could be configured to filter all the messages on a network for logging,
access control etc, and could 'mediate' messages without the explicit knowledge
of the original client. If Synapse receives a message that is not accepted by any
proxy service, that message is handled through message mediation. Message mediation
always processes messages according to the mediation sequence defined with
the name 'main'.

<a id="synapse-apache-org-userguide-config--TaskScheduling"></a>

### <a id="synapse-apache-org-userguide-config--Task_Scheduling"></a>Task Scheduling

In task scheduling, Synapse can execute a predefined task (job) based on a user
specified schedule. This way a task can be configured to run exactly once or
multiple times with fixed intervals. The schedule can be defined by specifying
the number of times the task should be executed and the interval between
executions. Alternatively one may use the Unix Cron syntax to define task
schedules. This mode of operation can be used to periodically invoke a given
service, poll databases and execute other periodic maintenance activities.

<a id="synapse-apache-org-userguide-config--Eventing"></a>

### <a id="synapse-apache-org-userguide-config--Eventing"></a>Eventing

In eventing mode, Synapse can be used as an event source and users or systems can
subscribe to receive events from Synapse. Synapse can also act as an event broker
which receives events from other systems and delivers them to the appropriate
subscribers with or without mediation. The set of subscribers will be selected
by applying a predefined filter criteria. This mode enables Synapse to integrate
applications and systems based on the Event Driven Architecture (EDA).

<a id="synapse-apache-org-userguide-config--Overview"></a>

## <a id="synapse-apache-org-userguide-config--Functional_Components_Overview"></a>Functional Components Overview

As described in the previous section, Synapse engine is comprised of a range of
functional components. Synapse configuration language is used to define, configure
and combine these components so various messaging scenarios and integration
patterns can be realized. Before diving into the specifics of the configuration
language, it is useful to have a thorough understanding of all the functional
components available, their capabilities and features. A good knowledge on Synapse
functional components will help you determine which components should be used to
implement any given scenario or use case. In turns it will allow you to develop
powerful and efficient Synapse configurations thus putting the ESB to maximum use.

As of now Synapse mediation engine consists of following functional elements:

- Mediators and sequences
- Endpoints
- Proxy services
- Scheduled tasks
- Event sources
- Sequence templates
- Endpoint templates
- Registry adapter
- APIs
- Priority executors
- Message stores and processors

<a id="synapse-apache-org-userguide-config--MediatorsAndSequences"></a>

### <a id="synapse-apache-org-userguide-config--Mediators_and_Sequences"></a>Mediators and Sequences

The Synapse ESB defines a 'mediator' as a component which performs a predefined
action on a message during a message flow. It is the most fundamental message
processing unit in Synapse. A mediator can be thought of as a filter that resides
in a message flow, which processes all the messages passing through it.

A mediator gets full access to the messages at the point where it is defined.
Thus they can inspect, validate and modify messages. Further, mediators can take
external action such as looking up a database or invoking a remote service,
depending on some attributes or values in the current message. Synapse ships
with a variety of built-in mediators which are capable of handling an array of
heterogeneous tasks. There are built-in mediators that can log the requests,
perform content transformations, filter out traffic and a plethora of other
messaging and integration activities.

Synapse also provides an API using which custom mediators can be implemented
easily in Java. The 'Class' and 'POJO (command)' mediators allow one to plugin a
Java class into Synapse with minimal effort. In addition, the 'Script' mediator
allows one to provide an Apache BSF script (eg: JavaScript, Ruby, Groovy etc)
for mediation.

A mediation sequence, commonly called a 'sequence' is a list of mediators. A
sequence may be named for re-use, or defined in-line or anonymously within a
configuration. Sequences may be defined within the Synapse configuration or in
the Registry. From an ESB point of view, a sequence equates to a message flow.
It can be thought of as a pipe consisting of many filters, where individual
mediators play the role of the filters.

A Synapse configuration contains two special sequences named 'main' and 'fault'.
These too may be defined in the Synapse configuration, or externally in the
Registry. If either is not found, a suitable default configuration is generated at
runtime by the ESB. The default 'main' sequence will simply send a message without
any mediation, while the default 'fault' sequence would log the message and error
details and stop further processing. The 'fault' sequence executes whenever Synapse
itself encounters an error while processing a message, or when a fault handler has
not been defined to handle exceptions. A sequence can assign another named sequence
as its 'fault' handler sequence, and handover control to the fault handler if an
error is encountered during the execution of the initial sequence.

<a id="synapse-apache-org-userguide-config--Endpoints"></a>

### <a id="synapse-apache-org-userguide-config--Endpoints"></a>Endpoints

An Endpoint definition within Synapse defines an external service endpoint and
any attributes or semantics that should be followed when communicating with that
endpoint. An endpoint definition can be named for re-use, or defined in-line or
anonymously within a configuration. Typically an endpoint would be based on a
service address or a WSDL. Additionally the Synapse ESB supports Failover and
Load-balance endpoints - which are defined over a group of endpoints. Endpoints
may be defined within the local Synapse configuration or within the Registry.

From a more practical stand point, an endpoint can be used to represent any
entity to which Synapse can make a connection. An endpoint may represent a
URL, a mail box, a JMS queue or a TCP socket. The 'send' mediator of Synapse
which is used to forward messages can take an endpoint as an argument. In that
case the 'send' mediator would forward the message to the specified endpoint.

<a id="synapse-apache-org-userguide-config--ProxyServices"></a>

### <a id="synapse-apache-org-userguide-config--Proxy_Services"></a>Proxy Services

A proxy service is a virtual service exposed on Synapse. For the external
clients, a proxy service looks like a full fledged web service which has a
set of endpoint references (EPRs), a WSDL and a set of developer specified
policies. But in reality, a proxy service sits in front of a real web service
implementation, acting as a proxy, mediating messages back and forth. The
actual business logic of the service resides in the real back-end web service.
Proxy service simply hides the real service from the consumer and provides
an interface through which the actual service can be reached but with some
added mediation/routing logic.

Proxy services have many use cases. A proxy can be used to expose an existing
service over a different protocol or a schema. The mediation logic in the proxy
can take care of performing the necessary content transformations and protocol
switching. A proxy service can act as a load balancer or a lightweight process
manager thereby hiding multiple back-end services from the client. Proxy services
also provide a convenient way of extending existing web services without changing
the back-end service implementations. For an example a proxy service can add logging
and validation capabilities to an existing service without the developer having
to implement such functionality at service level. Another very common usage of
proxy services is to secure an existing service or a legacy system.

A proxy service is a composite functional component. It is made of several
sequences and endpoints. Typically a proxy service consists of an 'in sequence',
an 'out sequence' and an endpoint. The 'in sequence' handles all the incoming
requests sent by the client. Mediated messages are then forwarded to the target
endpoint which generally points to the real back-end service. Responses coming
back from the back-end service are processed by the 'out sequence'. In addition
to these a 'fault sequence' can also be associated with a proxy service which
is invoked in case of an error.

In addition to the above basic configuration elements, a proxy service can
also define a WSDL file to be published, a set of policies and various other
parameters.

<a id="synapse-apache-org-userguide-config--ScheduledTasks"></a>

### <a id="synapse-apache-org-userguide-config--Scheduled_Tasks"></a>Scheduled Tasks

A scheduled task is a job deployed in the Synapse runtime for periodic execution.
Users can program the jobs using the task API (Java) provided by Synapse. Once
deployed, tasks can be configured to run periodically. The execution schedule
can be configured by specifying the delay between successive executions or using
the Unix Cron syntax.

<a id="synapse-apache-org-userguide-config--Templates"></a>

### <a id="synapse-apache-org-userguide-config--Templates"></a>Templates

A Template is an abstract concept in synapse. One way to view a template, is as
a prototype or a function. Templates try to minimize redundancy in synapse
artifacts (ie sequences and endpoints) by creating prototypes that users can
re-use and utilize as and when needed. This is very much analogous to classes
and instances of classes whereas, a template is a class that can be used to
wield instance objects such as sequences and endpoints.

Templates is an ideal way to improve re-usability and readability of
ESB configurations (XML). Addition to that users can utilize predefined templates
that reflect commonly used EIP patterns for rapid development of ESB
message/mediation flows.There are two flavours of templates which are Endpoint
and Sequence Templates.

An endpoint template is an abstract definition of a synapse endpoint. Users have
to invoke this kind of a template using a special template endpoint. Endpoint
templates can specify various commons parameters of an endpoint that can be reused
across many endpoint definitions (eg: address uri, timeouts, error codes etc).

A sequence template defines a functional form of an ESB sequence. Sequence
templates have the ability to parametrize a sequence flow. Generally
parametrization is in the form of static values as well as xpath expressions.
Users can invoke a template of this kind with a mediator named 'call-template'
by passing in the required parameter values.

<a id="synapse-apache-org-userguide-config--Registry"></a>

### <a id="synapse-apache-org-userguide-config--Remote_Registry_and_Local_Registry_Local_Entries"></a>Remote Registry and Local Registry (Local Entries)

Synapse configuration can refer to an external registry/repository for resources
such as WSDLs, schemas, scripts, XSLT and XQuery transformations etc. One or
more remote registries may be hidden or merged behind a local registry interface
defined in the Synapse configuration. Resources from an external registry are
looked up using 'keys' - which are known to the external registry. The Synapse
ESB ships with a simple URL based registry implementation that uses the file system
for storage of resources, and URLs or fragments as 'keys'.

A registry may define a duration for which a resource served may be cached by the
Synapse runtime. If such a duration is specified, the Synapse ESB is capable of
refreshing the resource after cache expiry to support dynamic re-loading of resources
at runtime. Optionally, a configuration could define certain 'keys' to map to locally
defined entities. These entities may refer to a source URL or file, or may be defined
as in-line XML or text within the configuration itself. If a registry contains a
resource whose 'key' matches the key of a locally defined entry, the local entry
shadows the resource available in the registry. Thus it is possible to override
registry resources locally from within the configuration. To integrate Synapse with
a custom/new registry, one needs to implement the org.apache.synapse.registry.Registry
interface to suit the actual registry being used.

<a id="synapse-apache-org-userguide-config--API"></a>

### <a id="synapse-apache-org-userguide-config--APIs"></a>APIs

An API is similar to a web application deployed in Synapse. It provides a
convenient approach for filtering and processing HTTP traffic (specially RESTful
invocations) through the service bus. Each API is anchored at a user defined
URL context (eg: /ws) and can handle all the HTTP requests that
fall within that context. Each API is also comprised of one or more resources.
Resources contain the mediation logic for processing requests and responses.
Resources can also be associated with a set of HTTP methods and header values.
For an example one may define an API with two resources, where one resources is
used to handle GET requests and the other is used to handle POST requests.
Similarly an API can be defined with separate resources for handling XML and JSON
content (by looking at the Content-type HTTP header).

Resources bare a strong resemblance to proxy services. Similar to proxy services,
a resource can also define an 'in sequence', an 'out sequence' and a 'fault
sequence'. Just like in the case of proxy services, the 'in sequence' is used
to process incoming requests and the 'out sequence' is used to mediate responses.

APIs provide a powerful framework using which comprehensive REST APIs can be
constructed on existing systems. For an example a set of SOAP services can be
hidden behind an API defined in Synapse. Clients can access the API in Synapse
by making pure RESTful invocations over HTTP. Synapse takes care of transforming
the requests and routing them to appropriate back-end services which may or may
not be based on REST.

<a id="synapse-apache-org-userguide-config--PriorityExecutors"></a>

### <a id="synapse-apache-org-userguide-config--Priority_Executors"></a>Priority Executors

Priority executors can be used to execute sequences with a given priority.
Priority executors are used in high load scenarios where user wants to execute
different sequences at different priority levels. This allows user to control
the resources allocated to executing sequences and prevent high priority messages
from getting delayed and dropped. A priority has a specific meaning compared to
other priorities specified. For example if we have two priorities with value 10
and 1, messages with priority 10 will get 10 times more resources than messages
with priority 1.

<a id="synapse-apache-org-userguide-config--Stores"></a>

### <a id="synapse-apache-org-userguide-config--Message_Stores_and_Processors"></a>Message Stores and Processors

Message store acts as a unit of storage for messages/data exchanged during synapse
runtime. By default synapse ships with a in-memory message store and the storage
can be plugged in depending on the requirement. There is a specific mediator called
store mediator which is able to direct message traffic to a particular message
store at runtime.

On the other hand a Message processor has the ability to connect to a message
store and perform message mediation or required data manipulations. Essentially
a particular message processor will be coupled with a message store and as a
result respective message processor will be inherited with the traits of that
particular message storage.

For example in the eye of a message processor, data/messages coming from in-memory
message store will be seen as more volatile compared to a persistent message store.
Nevertheless it will find it can perform operations much faster on the former.
This is in fact a very powerful concept and hence depending on the processor and
store combination users can define limitless number of EI patterns in synapse
that could meet different runtime requirements and SLA's. Synapse by default
support two processors which are scheduled message processor and sampling
processor.

<a id="synapse-apache-org-userguide-config--ConfigFiles"></a>

## <a id="synapse-apache-org-userguide-config--Synapse_Configuration_Files"></a>Synapse Configuration Files

All the XML files pertaining to the Synapse configuration are available in the
repository/conf/synapse-config directory of the Synapse installation. This file
hierarchy consists of two files named synapse.xml and registry.xml. In addition to
that, following sub-directories can be found in the synapse-config directory.

- api
- endpoints
- events
- local-entries
- message-processors
- message-stores
- priority-executors
- proxy-services
- sequences
- tasks
- templates

Each of these sub-directories can contain zero or more configuration items. For
an example the 'endpoints' directory may contain zero or more endpoint definitions
and the 'sequences' directory may contain zero or more sequence definitions. The
registry adapter is defined in the top level registry.xml file. The synapse.xml file
is there mainly for backward compatibility reasons. It can be used to define any
type of configuration items. One may define few endpoints in the 'endpoints' directory
and a few endpoints in the synapse.xml file. However it is recommended to stick to
a single, consistent way of defining configuration elements. So you should either
define everything in synapse.xml file, or not use it at all.

The following tree diagram shows the high-level view of the resulting file
hierarchy.

synapse-config
|-- api
|-- endpoints
| `-- foo.xml
|-- events
| `-- event1.xml
|-- local-entries
| `-- bar.xml
|-- message-processors
|-- message-stores
|-- priority-executors
|-- proxy-services
| |-- proxy1.xml
| |-- proxy2.xml
| `-- proxy3.xml
|-- registry.xml
|-- sequences
| |-- custom-logger.xml
| |-- fault.xml
| `-- main.xml
|-- synapse.xml
|-- tasks
| `-- task1.xml
`-- templates

<a id="synapse-apache-org-userguide-config--Syntax"></a>

## <a id="synapse-apache-org-userguide-config--Configuration_Syntax"></a>Configuration Syntax

Synapse ESB is configured using an XML based configuration language. This is a
Domain Specific Language (DSL) created and maintained by the Synapse community.
The language is designed to be simple, intuitive and easy to learn. All XML
elements (tags) in this language must be namespace qualified with the namespace
URL **http://ws.apache.org/ns/synapse**.

As stated earlier, the synapse.xml file can be used to define all kinds of artifacts.
All these different configuration items should be wrapped in a top level 'definitions'
element. A configuration defined in the synapse.xml file looks like this at the
high level.

<definitions>
<[registry](#synapse-apache-org-userguide-config--RegistryConfig) provider="string">...</registry>?
<[localEntry](#synapse-apache-org-userguide-config--LocalEntryConfig) key="string">...</localEntry>?
<[sequence](#synapse-apache-org-userguide-config--SequenceConfig) name="string">...</sequence>?
<[endpoint](#synapse-apache-org-userguide-config--EndpointConfig) name="string">...</endpoint>?
<[proxy](#synapse-apache-org-userguide-config--ProxyServiceConfig) name="string" ...>...</proxy>?
<[task](#synapse-apache-org-userguide-config--TaskConfig) name="string" ...>...</task>?
<[eventSource](#synapse-apache-org-userguide-config--EventSourceConfig) name="string" ...>...</eventSource>?
<[executor](#synapse-apache-org-userguide-config--ExecutorConfig) name="string" ...>...</executor>?
<[api](#synapse-apache-org-userguide-config--APIConfig) name="string" ...>...</api>?
<[template](#synapse-apache-org-userguide-config--TemplateConfig) name="string" ...>...</template>?
<[messageStore](#synapse-apache-org-userguide-config--StoresConfig) name="string" ...>...</messageStore>?
</definitions>

The registry adapter definition is defined under the <registry> element. Similarly
<endpoint>, <sequence>, <proxy>, <localEntry>, <eventSource
and <executor> elements are used to define other functional components.

As pointed out earlier, the synapse.xml file is there in the synapse-config directory
for backwards compatibility reasons. Any artifact defined in this file can be
defined separately in its own XML file. The registry can be defined in the registry.xml
and other artifacts can be defined in the corresponding sub-directories of the synapse-config
directory. However the XML syntax used to configure these artifacts are always the same.
Next few sections of this document explains the XML syntax for defining various
types of components in the Synapse configuration.

<a id="synapse-apache-org-userguide-config--RegistryConfig"></a>

## <a id="synapse-apache-org-userguide-config--Registry_Configuration"></a>Registry Configuration

The <registry> element is used to define the registry adapter used by the
Synapse runtime. The registry provider specifies an implementation class for the
registry being used, and optionally a number of configuration parameters as may be
required by the particular registry implementation. An outline configuration is given
below.

<registry provider="string"/>
<parameter name="string">text | xml</parameter>\*
</registry>

Registry entries loaded from a remote registry may be cached as governed by the
registry, and reloaded after the cache periods expires if a newer version is found.
Hence it is possible to define configuration elements such as (dynamic) sequences and
endpoints, as well as resources such as XSLT's, scripts or XSDs in the registry, and
update the configuration as these are allowed to dynamically change over time.

Synapse ships with a built-in URL based registry implementation called the
'SimpleURLRegistry' and this can be configured as follows:

<registry provider="org.apache.synapse.registry.url.SimpleURLRegistry">
<parameter name="root">file:./repository/conf/sample/resources/</parameter>
<parameter name="cachableDuration">15000</parameter>
</registry>

The 'root' parameter specifies the root URL of the registry for loaded resources. The
SimpleURLRegistry keys are path fragments, that when combined with the root prefix
would form the full URL for the referenced resource. The 'cachableDuration' parameter
specifies the number of milliseconds for which resources loaded from the registry
should be cached. More advanced registry implementations allows different cachable
durations to be specified for different resources, or mark some resources as never
expires. (e.g. Check the WSO2 ESB implementation based on Apache Synapse)

<a id="synapse-apache-org-userguide-config--LocalEntryConfig"></a>

## <a id="synapse-apache-org-userguide-config--Local_Entry_Local_Registry_Configuration"></a>Local Entry (Local Registry) Configuration

Local entries provide a convenient way to import various external configuration
artifacts into the Synapse runtime. This includes WSDLs, policies, XSLT files,
and scripts. Local entry definitions are parsed at server startup and the referenced
configurations are loaded to the memory where they will remain until the server is
shut down. Other functional components such as sequences, endpoints and proxy services
can refer these locally defined in-memory configuration elements by using the local
entry keys.

The <localEntry> element is used to declare registry entries that are local
to the Synapse instance. Following syntax is used to define a local entry in the
Synapse configuration.

<localEntry key="string" [src="url"]>text | xml</localEntry>

A local entry may contain static text or static XML specified as inline content.
Following examples show how such static content can be included in local entry
definitions.

<localEntry key="version">0.1</localEntry>
<localEntry key="validate\_schema">
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
...
</xs:schema>
</localEntry>

Note the validate\_schema local entry which wraps some static XML schema content. A
mediator such as the validate mediator can refer this local entry to load its XML
schema definition.

A local entry may also point to a remote URL (specified using the 'src'
attribute) from which the contents can be loaded. This way the user does not have
to specify all external configurations in the Synapse configuration itself. The
required artifacts can be kept on the file system or hosted on a web server from where
Synapse can fetch them using a local entry definition. Following example shows
how a local entry is configured to load an XSLT file from the local file system.

<localEntry key="xslt-key-req" src="file:repository/conf/sample/resources/transform/transform.xslt"/>

It is important to note that Synapse loads the local entry contents only during
server start up (even when they are defined with a remote URL). Therefore any
changes done on the remote artifacts will not reflect on Synapse until the server
is restarted. This is in contrast to the behavior with remote registry where
Synapse reloads configuration artifacts as soon as the cache period expires.

<a id="synapse-apache-org-userguide-config--SequenceConfig"></a>

## <a id="synapse-apache-org-userguide-config--Sequence_Configuration"></a>Sequence Configuration

As explained earlier a sequence resembles a message flow in Synapse and consists
of an array of mediators. The <sequence> element is used to define a sequence
in the Synapse configuration. Sequences can be defined with names so they can be
reused across the Synapse configuration. The sequences named 'main' and 'fault' have
special significance in a Synapse configuration. The 'main' sequence handles any message
that is accepted for '[Message Mediation](#synapse-apache-org-userguide-config--MessageMediation)'. The
'fault' sequence is invoked if Synapse encounters a fault, and a custom fault handler
is not specified for the sequence via its 'onError' attribute. If the 'main' or
'fault' sequences are not defined locally or not found in the Registry, Synapse
auto generates suitable defaults at initialization.

A Dynamic Sequence may be defined by specifying a key reference to a registry entry.
As the remote registry entry changes, the sequence will dynamically be updated
according to the specified cache duration and expiration. If tracing is enabled on a
sequence, all messages being processed through the sequence would write tracing
information through each mediation step to the 'trace.log' file configured via the
log4j.properties configuration.

The syntax outline of a sequence definition is given below.

<sequence name="string" [onError="string"] [key="string"] [trace="enable"] [statistics="enable"]>
mediator\*
</sequence>

The 'onError' attribute can be used to define a custom error handler sequence.
Statistics collection can be activated by setting the 'statistics' attribute to
'enable' on the sequence. In this mode the sequence will keep track of the number
of messages processed and their processing times. This statistical information can
then be retrieved through the Synapse statistics API.

All the immediate child elements of the sequence element must be valid mediators.
Following example shows a sequence configuration which consists of three child
mediators.

<sequence name="main" onError="errorHandler">
<log/>
<property name="test" value="test value"/>
<send/>
</sequence>

Sequences can also hand over messages to other sequences. In this sense a sequence
is analogous to a procedure in a larger program. In many programming languages
procedures can invoke other procedures. See the following example sequence
configuration.

<sequence name="foo">
<log/>
<property name="test" value="test value"/>
<sequence key="other\_sequence"/>
<send/>
</sequence>

Note how the message is handed to a sequence named 'other\_sequence' using the
'sequence' element. The 'key' attribute could point to another named sequence, a
local entry or a remote registry entry.

<a id="synapse-apache-org-userguide-config--EndpointConfig"></a>

## <a id="synapse-apache-org-userguide-config--Endpoint_Configuration"></a>Endpoint Configuration

An <endpoint> element defines a destination for an outgoing message. There
are several types of endpoints that can be defined in a Synapse configuration.

- Address endpoint
- WSDL endpoint
- Load balance endpoint
- Fail-over endpoint
- Default endpoint
- Recipient list endpoint

Configuration syntax and runtime semantics of these endpoint types differ from
each other. However the high level configuration syntax of an endpoint definition
takes the following form.

<endpoint [name="string"] [key="string"]>
[address-endpoint](#synapse-apache-org-userguide-config--AddressEndpointConfig) | [default-endpoint](#synapse-apache-org-userguide-config--DefaultEndpointConfig) | [wsdl-endpoint](#synapse-apache-org-userguide-config--WSDLEndpointConfig) |
[load-balanced-endpoint](#synapse-apache-org-userguide-config--LBEndpointConfig) | [fail-over-endpoint](#synapse-apache-org-userguide-config--FOEndpointConfig) | [recipient-list-endpoint](#synapse-apache-org-userguide-config--RecipientListEndpointConfig)
</endpoint>

Note how the endpoint definitions always start with an 'endpoint' element. The
immediate child element of this top level 'endpoint' element determines the type of
the endpoint. All above endpoint types can have a 'name' attribute, and such named
endpoints can be referred by other endpoints, through the key attribute. For example
if there is an endpoint named 'foo', the following endpoint can be used in any place,
where 'foo' has to be used.

<endpoint key="foo"/>

This provides a simple mechanism for reusing endpoint definitions within a Synapse
configuration.

The 'trace' attribute turns on detailed trace information for messages being sent
to the endpoint. These will be available in the 'trace.log' file configured via the
log4j.properties file.

<a id="synapse-apache-org-userguide-config--AddressEndpointConfig"></a>

### <a id="synapse-apache-org-userguide-config--Address_Endpoint"></a>Address Endpoint

<address uri="*endpoint address*" [format="soap11|soap12|pox|get"] [optimize="mtom|swa"]
[encoding="*charset encoding*"]
[statistics="enable|disable"] [trace="enable|disable"]>
<enableSec [policy="*key*"]/>?
<enableAddressing [version="final|submission"] [separateListener="true|false"]/>?
<timeout>
<duration>*timeout duration in milliseconds*</duration>
<responseAction>discard|fault</responseAction>
</timeout>?
<markForSuspension>
[<errorCodes>xxx,yyy</errorCodes>]
<retriesBeforeSuspension>m</retriesBeforeSuspension>
<retryDelay>d</retryDelay>
</markForSuspension>
<suspendOnFailure>
[<errorCodes>xxx,yyy</errorCodes>]
<initialDuration>n</initialDuration>
<progressionFactor>r</progressionFactor>
<maximumDuration>l</maximumDuration>
</suspendOnFailure>
</address>

Address endpoint is an endpoint defined by specifying the EPR and other
attributes of the endpoint directly in the configuration. The 'uri' attribute
of the address element contains the EPR of the target endpoint. Message format
for the endpoint and the method to optimize attachments can be specified in the
'format' and 'optimize' attributes respectively. Security
policies for the endpoint can be specified in the policy attribute of the
'enableSec' element. WS-Addressing can be engaged
for the messages sent to the endpoint by using the 'enableAddressing' element.

The 'timeout' element of the endpoint configuration is used to set a specific
socket timeout for the endpoint. By default this is set to 1 minute (60 seconds).
When integrating with back-end services which take longer to respond the timeout
duration should be increased accordingly. The 'responseAction' element states
the action that should be taken in case a response is received after the timeout
period has elapsed. Synapse can either 'discard' the delayed response or inject it
into a 'fault' handler.

A Synapse endpoint is a state machine. At any given point in time it could be
in one of four states - Active, Timeout, Suspended and Switched Off. How and
when an endpoint changes its state is configurable through the Synapse configuration.
An endpoint in suspended or switched off states cannot be used to send messages.
Such an attempt would generate a runtime error.

By default an endpoint is in the 'Active' state. The endpoint will continue to
forward requests as long as its in this state. If an active endpoint encounters
an error while trying to send a message out (eg: a connection failure), the
endpoint may get pushed into the 'Timeout' state or the 'Suspended' state.
Generally most errors will put the endpoint straight into the 'Suspended' state.
Connection timeouts (error code 101504) and connection closed errors (101505)
are the only errors that will not directly suspend an endpoint. Using the
'errorCodes' element in the 'suspendOnFailure' configuration one can explicitly
define the errors for which the endpoint should be suspended. Similarly the
'errorCodes' element in the 'markForSuspension' configuration can be used to
define the errors for which the endpoint should be pushed into the 'Timeout'
state.

Endpoints in 'Timeout' state can be used to send messages. But any consecutive
errors while in this state can push the endpoint into the 'Suspended' state.
The number of consecutive errors that can suspend the endpoint can be configured
using the 'retriesBeforeSuspension' element in the 'markForSuspension' configuration.
The 'retryDelay' is used to specify a duration for which an endpoint will not be
available for immediate use after moving it to the 'Timeout' state. This duration
should be specified in milliseconds.

An endpoint in 'Suspended' state cannot be used to send messages. However the
suspension is only temporary. The suspend duration can be configured using the
'initialDuration' element. When this time period expires a suspended endpoint
becomes available for use again. However any recurring errors can put the
endpoint back in the 'Suspended' state. Such consecutive suspensions can also
progressively increase the suspend duration of the endpoint as configured by the
'progressionFactor' element. But the suspend duration will never exceed the
period configured in the 'maximumDuration' element. Note that both 'initialDuration'
and 'maximumDuration' should be specified in milliseconds.

Some example address endpoint configurations are given below. Note how the
communication protocol is used as a suffix to indicate the outgoing transport.

| Transport | Sample address |
| --- | --- |
| HTTP | http://localhost:9000/services/SimpleStockQuoteService |
| JMS | jms:/SimpleStockQuoteService?transport.jms.ConnectionFactoryJNDIName=QueueConnectionFactory&java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory&java.naming.provider.url=tcp://localhost:61616&transport.jms.DestinationType=topic |
| Mail | mailto:guest@host |
| VFS | vfs:file:///home/user/directory |
|  | vfs:file:///home/user/file |
|  | vfs:ftp://guest:guest@localhost/directory?vfs.passive=true |

<a id="synapse-apache-org-userguide-config--DefaultEndpointConfig"></a>

### <a id="synapse-apache-org-userguide-config--Default_Endpoint"></a>Default Endpoint

Default endpoint is an endpoint defined for adding QoS and other configurations
to the endpoint which is resolved from the 'To' address of the message context.
All the configurations such as message format for the endpoint, the method to
optimize attachments, and security policies for the endpoint
can be specified as in the case of Address Endpoint. This endpoint differs from
the address endpoint only in the 'uri' attribute which will not be present in
this endpoint. Following section describes the configuration of a default
endpoint.

<default [format="soap11|soap12|pox|get"] [optimize="mtom|swa"]
[encoding="*charset encoding*"]
[statistics="enable|disable"] [trace="enable|disable"]>
<enableSec [policy="*key*"]/>?
<enableAddressing [version="final|submission"] [separateListener="true|false"]/>?
<timeout>
<duration>*timeout duration in milliseconds*</duration>
<responseAction>discard|fault</responseAction>
</timeout>?
<markForSuspension>
[<errorCodes>xxx,yyy</errorCodes>]
<retriesBeforeSuspension>m</retriesBeforeSuspension>
<retryDelay>d</retryDelay>
</markForSuspension>
<suspendOnFailure>
[<errorCodes>xxx,yyy</errorCodes>]
<initialDuration>n</initialDuration>
<progressionFactor>r</progressionFactor>
<maximumDuration>l</maximumDuration>
</suspendOnFailure>
</default>

<a id="synapse-apache-org-userguide-config--WSDLEndpointConfig"></a>

### <a id="synapse-apache-org-userguide-config--WSDL_Endpoint"></a>WSDL Endpoint

WSDL endpoint is an endpoint definition based on a specified WSDL document. The
WSDL document can be specified either as a URI or as an inline definition within
the configuration. The service and port name containing the target EPR has to be
specified with the 'service' and 'port' (or 'endpoint') attributes respectively.
Elements like 'enableSec', 'enableAddressing', 'suspendOnFailure' and
'timeout' are same as for an Address endpoint.

<wsdl [uri="wsdl-uri"] service="qname" port/endpoint="qname">
<wsdl:definition>...</wsdl:definition>?
<wsdl20:description>...</wsdl20:description>?
<enableSec [policy="key"]/>?
<enableAddressing/>?
<timeout>
<duration>timeout duration in milliseconds</duration>
<responseAction>discard|fault</responseAction>
</timeout>?
<markForSuspension>
[<errorCodes>xxx,yyy</errorCodes>]
<retriesBeforeSuspension>m</retriesBeforeSuspension>
<retryDelay>d</retryDelay>
</markForSuspension>
<suspendOnFailure>
[<errorCodes>xxx,yyy</errorCodes>]
<initialDuration>n</initialDuration>
<progressionFactor>r</progressionFactor>
<maximumDuration>l</maximumDuration>
</suspendOnFailure>
</wsdl>

<a id="synapse-apache-org-userguide-config--LBEndpointConfig"></a>

### <a id="synapse-apache-org-userguide-config--Load_Balance_Endpoint"></a>Load Balance Endpoint

A Load balanced endpoint distributes the messages (load) among a set of listed
endpoints or static members by evaluating the load balancing policy and any other
relevant parameters. The policy attribute of the load balance element specifies the
load balance policy (algorithm) to be used for selecting the target endpoint or
static member. Currently only the roundRobin policy is supported. The 'failover'
attribute determines if the next endpoint or static member should be selected once
the currently selected endpoint or static member has failed, and defaults to true.
The set of endpoints or static members amongst which the load has to be distributed
can be listed under the 'loadBalance' element. These endpoints can belong to any
endpoint type mentioned in this document. For example, failover endpoints can be
listed inside the load balance endpoint to load balance between failover groups etc.
The 'loadbalance' element cannot have both 'endpoint' and 'member' child elements in
the same configuration. In the case of the 'member' child element, the 'hostName',
'httpPort' and/or 'httpsPort' attributes should be specified.

The optional 'session' element makes the endpoint a session affinity based load
balancing endpoint. If it is specified, sessions are bound to endpoints in the
first message and all successive messages for those sessions are directed to their
associated endpoints. Currently there are two types of sessions supported in session
aware load balancing. Namely HTTP transport based session which identifies the
sessions based on http cookies and the client session which identifies the session
by looking at a SOAP header sent by the client with the QName
'{http://ws.apache.org/ns/synapse}ClientID'. The 'failover' attribute mentioned
above is not applicable for session affinity based endpoints and it is always
considered as set to false. If it is required to have failover behavior in session
affinity based load balance endpoints, failover endpoints should be listed as the
target endpoints.

<loadBalance [policy="roundRobin"] [algorithm="impl of org.apache.synapse.endpoints.algorithms.LoadbalanceAlgorithm"]
[failover="true|false"]>
<endpoint .../>+
<member hostName="host" [httpPort="port"] [httpsPort="port2"]>+
</loadBalance>
<session type="http|simpleClientSession"/>?

<a id="synapse-apache-org-userguide-config--DLBEndpointConfig"></a>

### <a id="synapse-apache-org-userguide-config--Dynamic_Load_Balance_Endpoint"></a>Dynamic Load Balance Endpoint

This is a special variation of the load balance endpoint where instead of
having to specify the child endpoints explicitly, the endpoint automatically
discovers the child endpoints available for load balancing. These child
endpoints will be discovered using the 'membershipHandler' class. Generally, this
class will use a group communication mechanism to discover the application members.
The 'class' attribute of the 'membershipHandler' element should be an
implementation of org.apache.synapse.core.LoadBalanceMembershipHandler.
The 'membershipHandler' specific properties can be specified using the 'property'
elements. The 'policy' attribute of the 'dynamicLoadbalance' element specifies
the load balance policy (algorithm) to be used for selecting the next member to
which the message has to be forwarded. Currently only the 'roundRobin' policy is
supported. 'The failover' attribute determines if the next member should be
selected once the currently selected member has failed, and defaults to true.

<dynamicLoadBalance [policy="roundRobin"] [failover="true|false"]>
<membershipHandler class="impl of org.apache.synapse.core.LoadBalanceMembershipHandler">
<property name="name" value="value"/>+
</membershipHandler>
</dynamicLoadBalance>

Currently Synapse ships with one implementation of the LoadBalanceMembershipHandler
interface. This class is named 'Axis2LoadBalanceMembershipHandler' and its
usage is demonstrated in sample 57.

<a id="synapse-apache-org-userguide-config--FOEndpointConfig"></a>

### <a id="synapse-apache-org-userguide-config--Fail-Over_Endpoint"></a>Fail-Over Endpoint

Failover endpoints send messages to the listed endpoints with the following
failover behavior. At the start, the first listed endpoint is selected as the
primary and all other endpoints are treated as backups. Incoming messages are
always sent only to the primary endpoint. If the primary endpoint fails, next
active endpoint is selected as the primary and failed endpoint is marked as
inactive. Thus it sends messages successfully as long as there is at least one
active endpoint among the listed endpoints.

When a previously failed endpoint becomes available again, it will assume
its position as the primary endpoint and the traffic will be routed to that endpoint.
It is possible to disable this behavior by setting the 'dynamic' attribute to false.

<failover [dynamic="true|false"]>
<endpoint .../>+
</failover>

<a id="synapse-apache-org-userguide-config--RecipientListEndpointConfig"></a>

### <a id="synapse-apache-org-userguide-config--Recipient_List_Endpoint"></a>Recipient List Endpoint

A recipient list endpoint can be used to send a single message to a list of
recipients (child endpoints). This is used to implement the well-known
integration pattern named 'recipient list'. The same functionality can
be achieved using the 'clone' mediator, but the recipient list provides
a more natural and intuitive way of implementing such a scenario. Configuration
of the recipient list endpoint takes the following general form.

<recipientList name="string">
<endpoint>+
<member hostName="host" [httpPort="port"] [httpsPort="port2"]>+
</recipientList>

A recipient list can be named by setting the 'name' attribute on the 'recipientList'
element. Similar to a load balance endpoint, the recipient list endpoint also
wraps a set of endpoint definitions or a set of member definitions. At runtime
messages will be sent to all the child endpoints or members.

<a id="synapse-apache-org-userguide-config--ProxyServiceConfig"></a>

## <a id="synapse-apache-org-userguide-config--Proxy_Service_Configuration"></a>Proxy Service Configuration

A <proxy> element is used to define a Synapse Proxy service.

<proxy name="string" [transports="(http |https |jms |.. )+|all"] [pinnedServers="(serverName )+"] [serviceGroup="string"]>
<description>...</description>?
<target [inSequence="name"] [outSequence="name"] [faultSequence="name"] [endpoint="name"]>
<inSequence>...</inSequence>?
<outSequence>...</outSequence>?
<faultSequence>...</faultSequence>?
<endpoint>...</endpoint>?
</target>?
<publishWSDL key="string" uri="string">
( <wsdl:definition>...</wsdl:definition> | <wsdl20:description>...</wsdl20:description> )?
<resource location="..." key="..."/>\*
</publishWSDL>?
<enableAddressing/>?
<enableSec/>?
<policy key="string" [type="(in | out)"]/>? // optional service or message level policies such as (e.g. WS-Security and/or WS-RM policies)
<parameter name="string"> // optional service parameters such as (e.g. transport.jms.ConnectionFactory)
string | xml
</parameter>
</proxy>

A proxy service is created and exposed on the specified transports through the
underlying Axis2 engine, exposing service EPRs as per the standard Axis2
conventions (ie based on the service name). Note that currently Axis2 does not allow
custom URI's to be set for services on some transports such as http/s. A proxy
service could be exposed over all enabled Axis2 transports such as http, https,
JMS, Mail and File etc. or on a subset of these as specified by the optional
'transports' attribute. By default, if this attribute is not specified, Synapse
will attempt to expose the proxy service on all enabled transports.

In a clustered setup it might be required to deploy a particular proxy service
on a subset of the available nodes. This can be achieved using the 'pinnedServers'
attribute. This attribute takes a list of server names. At server startup Synapse
will check whether the name of the current host matches any of the names given
in this attribute and only deploy the proxy service if a match is found. The
server host name is picked from the system property 'SynapseServerName', failing
which the hostname of the machine would be used or default to 'localhost'. User can
specify a more meaningful name to a Synapse server instance by starting the server
using the following command.

./synapse.sh -serverName=<ServerName>

If Synapse is started as a daemon or a service, the above setting should be specified
in the wrapper.conf file.

By default when a proxy service is created it is added to an Axis service group
which has the same name as the proxy service. With the 'serviceGroup' attribute
this behavior can be further configured. A custom Axis service group can be specified
for a proxy service using the 'serviceGroup' attribute. This way multiple proxy
services can be grouped together at Axis2 level thus greatly simplifying service
management tasks.

Each service could define the target for received messages as a named sequence or a
direct endpoint. Either target inSequence or endpoint is required for the proxy
configuration, and a target outSequence defines how responses should be handled. Any
WS-Policies provided would apply as service level policies, and any service parameters
could be passed into the proxy service's AxisService instance using the 'parameter'
elements (e.g. the JMS destination etc). If the proxy service should enable
WS-Reliable Messaging or Security, the appropriate modules should be engaged, and
specified service level policies will apply. To engage the required modules, one may
use the 'enableSec', and 'enableAddressing' elements.

A dynamic proxy may be defined by specifying the properties of the proxy as dynamic
entries by referring them with the key. For example one could specify the
inSequence or endpoint with a remote key, without defining it in the local
configuration. As the remote registry entry changes, the properties of the proxy
will dynamically be updated accordingly. (Note: proxy service definition itself
cannot be specified to be dynamic; i.e <proxy key="string"/> is wrong)

A WSDL for the proxy service can be published using the 'publishWSDL' element.
The WSDL document can be loaded from the registry by specifying the 'key' attribute
or from any other location by specifying the 'uri' attribute. Alternatively the WSDL
can be provided inline as a child element of the 'publishWSDL' element. Artifacts
(schemas or other WSDL documents) imported by the WSDL can be resolved from the
registry by specifying appropriate 'resource' elements.

<publishWSDL key="my.wsdl">
<resource location="http://www.standards.org/standard.wsdl" key="standard.wsdl"/>
</publishWSDL>

In this example the WSDL is retrieved from the registry using the key 'my.wsdl'. It
imports another WSDL from location 'http://www.standards.org/standard.wsdl'. Instead
of loading it from this location, Synapse will retrieve the imported WSDL from the
registry entry 'standard.wsdl'.

Some well-known parameters that are useful when writing complex proxy services
are listed below. These can be included in a proxy configuration using 'parameter'
tags.

| Parameter | Value | Default | Description |
| --- | --- | --- | --- |
| useOriginalwsdl | true\|false | false | Use the given WSDL instead of generating the WSDL. |
| modifyUserWSDLPortAddress | true\|false | true | (Effective only with useOriginalwsdl=true) If true (default) modify                             the port addresses to current host. |
| showAbsoluteSchemaURL | true\|false | false | Show the absolute path of the referred schemas of the WSDL without                             showing the relative paths. |

Following table lists some transport specific parameters that can be passed into
proxy service configurations.

| Transport | Require | Parameter | Description |
| --- | --- | --- | --- |
| JMS | Optional | transport.jms.ConnectionFactory | The JMS connection factory definition (from axis2.xml) to be used to                             listen for messages for this service |
|  | Optional | transport.jms.Destination | The JMS destination name (Defaults to a Queue with the service name) |
|  | Optional | transport.jms.DestinationType | The JMS destination type. Accept values 'queue' or 'topic' (default:                             queue) |
|  | Optional | transport.jms.ReplyDestination | The destination where a reply will be posted |
|  | Optional | transport.jms.Wrapper | The wrapper element for the JMS message |

<a id="synapse-apache-org-userguide-config--TaskConfig"></a>

## <a id="synapse-apache-org-userguide-config--Scheduled_Task_Configuration"></a>Scheduled Task Configuration

A <task> element is used to define a Synapse task (aka startup).

<task class="mypackage.MyTask" name="string" [pinnedServers="(serverName)+"]>
<property name="stringProp" value="String"/>
<property name="xmlProp">
<somexml>config</somexml>
</property>
<trigger ([[count="10"]? interval="1000"] | [cron="0 \* 1 \* \* ?"] | [once=(true | false)])/>
</task>

A task is created and scheduled to run at specified time intervals or as specified
by the cron expression. The 'class' attribute specifies the actual task
implementation class (which must implement org.apache.synapse.task.Task interface)
to be executed at the specified interval/s, and name specifies an identifier for
the scheduled task.

Fields in the task class can be set using properties provided as string literals or
XML fragments. For example, if the task implementation class has a field named
'version' with a corresponding setter method, the configuration value which will be
assigned to this field before running the task can be specified using a property with
the name 'version'.

There are three different trigger mechanisms to schedule tasks. A simple trigger is
specified with a 'count' and an 'interval', implying that the task will run a
'count' number of times at specified intervals. A trigger may also be specified as
a cron trigger using a cron expression. A one-time trigger is specified using the
'once' attribute in the definition and could be specified as true in which case the
task will be executed only once just after the initialization of Synapse.

In clustered deployments sometimes it would be necessary to deploy a particular task
in a selected set of nodes. This can be achieved using the optional 'pinnedServers'
attribute. A list of server names or host names can be specified in this attribute.
At server startup, Synapse will match the current server name or the host name with
the values specified in this attribute to see whether the task should be initialized
or not.

<a id="synapse-apache-org-userguide-config--TemplateConfig"></a>

## <a id="synapse-apache-org-userguide-config--Template_Configuration"></a>Template Configuration

As explained earlier templates in synapse are defined in two flavors; sequence and
endpoint templates. The configuration, syntax forms and semantics of these are explained
in the following section.

A sequence template consist of two parts. As in any kind of a function, it has a
parameter set definition (argument list) and a function body definition. An important
difference is sequence template parameters are not typed (typically these are
string parameters, but can be of any type which is determined at runtime). Also
function body is a typical esb flow or a sequence.

The syntax outline of a sequence template definition is given below.

<template name="string">
<!-- parameters this sequence template will be supporting -->
(
<parameter name="string"/>
) \*
<!--this is the in-line sequence of the template -->
<sequence>
mediator+
</sequence>
</template>

A sequence template is a top level element defined with the 'name' attribute in Synapse
configuration. Both endpoint and sequence templates start with a 'template' element.
Parameters (defined by <parameter> elements) are the inputs supported by a
sequence template. These sequence template parameters can be referred by any xpath
expression defined within the in-lined sequence. For example parameter named 'foo' can
be referred by a property mediator (defined inside the in-line sequence of the template)
in following ways.

<property name="PropertyValue" expression="$func:foo"/>
<property name="PropertyValue" expression="get-property('foo', 'func')"/>

Note the scope variable used in the XPath expression. We use 'function' scope or '$func'
to refer to template parameters.

Invoking a sequence template can be done with a mediator named 'call-template' by
passing parameter values. The syntax outline of a call template mediator definition
is given below.

<call-template target="string">
<!-- parameter values will be passed on to a sequence template -->
(
<!--passing plain static values -->
<with-param name="string" value="string" /> |
<!--passing xpath expressions -->
<with-param name="string" value="{string}" /> |
<!--passing dynamic xpath expressions where values will be compiled dynamically-->
<with-param name="string" value="{{string}}" /> |
) \*
</call-template>

The 'call-template' mediator should define a target template it should be
invoking, with 'target' attribute.

The 'with-param' element is used to parse parameter values to a target
sequence template. Note that parameter names has to be exact matches to the names
specified in the target template. Parameter elements can contain three types of
parameterized values. xpath values are passed in within curly braces ({}) for value
attribute.

Endpoint templates are similar to the sequence templates in definition. Unlike
sequence templates, endpoint templates are always parameterized using '$' prefixed
values (NOT xpath expressions). Users can parameterize endpoint configuration elements
with these '$' prefixed values. An example is shown below.

<template name="ep\_template">
<parameter name="codes"/>
<parameter name="factor"/>
<parameter name="retries"/>
<endpoint name="$name">
<default>
<suspendOnFailure>
<errorCodes>$codes</errorCodes>
<progressionFactor>$factor</progressionFactor>
</suspendOnFailure>
<markForSuspension>
<retriesBeforeSuspension>$retries</retriesBeforeSuspension>
<retryDelay>0</retryDelay>
</markForSuspension>
</default>
</endpoint>
</template>

The syntax outline of a endpoint template definition is given below.

<template name="string">
<!-- parameters this endpoint template will be supporting -->
(
<parameter name="string"/>
) \*
<!--this is the in-line endpoint of the template -->
<endpoint [name="string"] >
address-endpoint | default-endpoint | wsdl-endpoint |
load-balanced-endpoint | fail-over-endpoint | recipient-list-endpoint
</endpoint>
</template>

As described earlier template endpoint is the artifact that makes a template of an
endpoint type into a concrete endpoint. In other words an endpoint template would
be useless without a template endpoint referring to it. This is semantically similar
to the relationship between a sequence template and a 'call-template' mediator.

The syntax outline of a template endpoint definition is as following..

<endpoint [name="string"] [key="string"] template="string">
<!-- parameter values will be passed on to a endpoint template -->
(
<parameter name="string" value="string" />
) \*
</endpoint>

Template endpoint defines parameter values that can parameterize an endpoint.
The 'template' attribute points to a target endpoint template.

As in the case of sequence template, note that parameter names has to be exact match
to the names specified in target endpoint template.

<a id="synapse-apache-org-userguide-config--EventSourceConfig"></a>

## <a id="synapse-apache-org-userguide-config--Event_Source_Configuration"></a>Event Source Configuration

Event sources enable the user to run Synapse in the eventing mode of operation.
Synapse can act as an event source as well as an event broker. An event source
is defined using the <eventSource> configuration element.

<eventSource name="string">
<subscriptionManager class="mypackage.MyClass">
<parameter name="string"/>
</subscriptionManager>
</eventSource>

Once an event source is deployed in Synapse, it will provide a service URL (EPR) to
which clients can send WS-Eventing subscription requests. Clients can subscribe,
unsubscribe and renew subscriptions by sending messages to this EPR. The subscription
manager configured inside the event source will be responsible for storing
and managing the subscriptions. The 'class' attribute of the 'subscriptionManager'
element should point to the Java class which provides this subscription management
functionality. Synapse ships with an in-memory subscription manager which
keeps and manages all subscriptions in memory.

Any additional parameters required to configure the subscription manager implementation
can be specified using the 'parameter' elements.

<a id="synapse-apache-org-userguide-config--APIConfig"></a>

## <a id="synapse-apache-org-userguide-config--API_Configuration"></a>API Configuration

APIs provide a flexible and powerful approach for defining full fledged REST APIs
in Synapse. An API definition starts with the <api> element.

<api name="string" context="string" [transport="http|https"][hostname="string"][port="int"]>
<resource [methods="http-method-list"][inSequence="string"][outSequence="string"]
[faultSequence="string"][url-mapping="string"][uri-template="string"]
[content-type="string"][user-agent="str"]>
<inSequence>...</inSequence>?
<outSequence>...</outSequence>?
<faultSequence>...</faultSequence>?
</resource>+
<handlers>
<handler class="name"/>+
</handlers>?
</api>

Each API definition must be uniquely named using the 'name' attribute. The 'context'
attribute is used to define the URL context at which the REST API will be anchored
(eg: /ws, /foo/bar, /soap). The API will only receive requests that fall in the
specified URL context. In addition to that an API could be bound to a particular
host and a port using the 'hostname' and 'port' attributes. The 'transport' attribute
can be used to restrict the API to process either HTTP messages or HTTPS messages only.

An API must also contain one or more resources. Resources define how messages
are processed and mediated by the API. A resource can be associated with a set of HTTP
methods using the 'methods' attribute. This attribute can support a single method name
(eg: GET) or a space separated list of methods (eg: GET PUT DELETE). The 'url-mapping'
and 'uri-template' attributes can be used to specify the type of URL requests that should
be handled by any particular resource. The 'url-mapping' attribute accepts any Java
servlet style URL mapping (eg: /test/\*, \*.jsp). The 'uri-template' attribute accepts
valid RFC6570 style expressions (eg: /orders/{orderId}). A resource can also refer
other sequences using the 'inSequence', 'outSequence' and 'faultSequence' attributes.
Alternatively these mediation sequences can be defined inline with the resource using
'inSequence', 'outSequence' and 'faultSequence' tags.

An API can also optionally define a set of handlers. These handlers are invoked
for each incoming API request, before they are dispatched to the appropriate
resources. The 'class' attribute on the 'handler' elements should contain the
ful qualified names of the handler implementation classes.

<a id="synapse-apache-org-userguide-config--ExecutorConfig"></a>

## <a id="synapse-apache-org-userguide-config--Priority_Executor_Configuration"></a>Priority Executor Configuration

The priority executor configuration syntax takes the following general form.

<priority-executor name="string">
<queues isFixed="true|false" nextQueue="class implementing NextQueueAlgorithm">
<queue [size="size of the queue"] priority="priority of the messages put in to this queue"/>\*
</queues>
<threads core="core number of threads" max="max number of threads' keep-alive="keep alive time"/>
</priority-executor>

A priority executor consists of a thread pool and a set of queues for different
priority levels. Queues can be either bounded on unbounded in terms of capacity.
Each executor must define at least two queues. By default queues are unbounded.
By specifying the attribute 'size' they can be configured to have a limited capacity.
The 'priority' attribute specifies the priority level associated with a particular
queue. As explained earlier, higher the level, higher the priority the messages will
get.

The next queue algorithm is used to determine the next message processed. By default
Synapse uses a built-in priority queueing algorithm for this purpose. If required a
custom algorithm can be used by specifying the 'nextQueue' algorithm on the 'queues'
element.

The 'threads' element is used to configure the underlying thread pool. The 'core'
and 'max' attributes are used to specify the initial size and the maximum size of the
thread pool. A keep-alive time duration can be specified for idling threads using
the 'keep-alive' attribute where the duration is configured in seconds. If not
specified a default keep-alive duration of 5 seconds will be used.

In order to process messages through a priority executor one must use the 'enqueue'
mediator. This mediator can be used in a sequence or a proxy service to get all
requests processed through a pre-configured priority executor.

<enqueue priority="10" executor="MyExecutor"/>

For best results it's recommended to dispatch messages into priority executors
straight from the transport level. This can be achieved by adding an additional
parameter to the NHTTP transport configuration in the axis2.xml file of Synapse.

<parameter name="priorityConfigFile">file path</parameter>

The parameter should point to a separate XML configuration which defines the
priority configuration.

<Priority-Configuration>
<priority-executor name="priority-executor">
<queues isFixed="true|false">
<queue [size=""] priority=""/>\*
</queues>
<threads core="core number of threads" max="max number of threads' keep-alive="keep alive time"/>
</priority-executor>
<!-- conditions for calculating the priorities based on HTTP message -->
<conditions defaultPriority="default priority as an integer">
<condition priority="priority value as an integer">
one evaluator, this evaluator can contain other evaluators
</condition>
</conditions>
</Priority-Configuration>

<a id="synapse-apache-org-userguide-config--StoresConfig"></a>

## <a id="synapse-apache-org-userguide-config--Message_Stores_and_Processors_Configuration"></a>Message Stores and Processors Configuration

Both Message Stores and processors are top level configuration of synapse. Following
section tries to describe some of the syntax of the message store/processors
configurations.

The syntax outline of a message store definition is given below.

<messageStore name="string" class="classname" >
<parameter name="string" > "string" </parameter>\*
</messageStore>

The 'class' attribute value is the fully qualified class name of the underlying message
store implementation. There can be many message store implementations.Users can write
their own message store implementation and use it. Parameters section is used to
configure the parameters that is needed by underlying message store implementation.

The syntax outline of a message processor definition is given below.

<messageProcessor name="string" class="class name" messageStore="classname" >
<parameter name="string" > "string" </parameter>\*
</messageProcessor>

The 'class' attribute value is the fully qualified class name of the underlying message
processor implementation. There are two message processor implementations shipped by
default. There can be many message processor implementations.Users can write their
own message processor implementation and use it. Similar to message stores ,parameters
section here as well is used to configure the parameters that is needed by underlying
message processor implementation

Message Forwarding Processor : org.apache.synapse.message.processors.forward.ScheduledMessageForwardingProcessor

Sampling Processor : org.apache.synapse.message.processors.sampler.SamplingProcessor

As mentioned earlier, there are several message store/processor implementations
shipped by default. However if users wants to extend these following interfaces are
available.

Interface: org.apache.synapse.message.store.MessageStore
Abstract Class: org.apache.synapse.message.store.AbstractMessageStore
Interface: org.apache.synapse.message.processors.MessageProcessor
Abstract Class: org.apache.synapse.message.processors.AbstractMessageProcessor

---

<a id="synapse-apache-org-userguide-deployment"></a>

# Apache Synapse – Apache Synapse - Deployment Guide

## <a id="synapse-apache-org-userguide-deployment--Deployment_Guide"></a>Deployment Guide

This article explains the various approaches that can be taken to deploy an
Apache Synapse server instance. It provides information on each deployment
option along with their software requirements and steps that need to be carried
out.

## <a id="synapse-apache-org-userguide-deployment--Contents"></a>Contents

- [Platform requirements](#synapse-apache-org-userguide-deployment--Platform_requirements)
- [Overview of available deployment options](#synapse-apache-org-userguide-deployment--Overview_of_available_deployment_options)
- [Stand-alone deployment](#synapse-apache-org-userguide-deployment--Stand-alone_deployment)
  - [Using the standard binary distribution](#synapse-apache-org-userguide-deployment--Using_the_standard_binary_distribution)
  - [Using Maven to build a custom distribution](#synapse-apache-org-userguide-deployment--Using_Maven_to_build_a_custom_distribution)
- [WAR deployment](#synapse-apache-org-userguide-deployment--WAR_deployment)

<a id="synapse-apache-org-userguide-deployment--Platform_requirements"></a>

## <a id="synapse-apache-org-userguide-deployment--Platform_requirements"></a>Platform requirements

Synapse requires Java 1.6 or higher and has been tested on Java runtime environments
from Sun, IBM and Apple.Synapse is used on various operation systems,
including Linux, Mac OS X, Solaris, Windows and AIX,
as well as mainframe environments. The recommended operation system for production use
is Linux since it offers a wider range of options to tune the TCP/IP stack. This is
important to optimize the performance of the NIO HTTP transport.

When selecting the environment for deployment, the following known issues should be taken into account:

- The synapse.bat and synapse.sh scripts included in the binary
  distribution use the -server option which is not supported by IBM's JRE.
  This problem can be easily solved by manually editing these scripts to
  remove the unsupported -server option. See
  [SYNAPSE-454](https://issues.apache.org/jira/browse/SYNAPSE-454)
  .
- In the past several issues related to subtle concurrency problems have been reported
  with the non-blocking HTTP transport (which is the recommended HTTP implementation
  for Synapse) when used on more "exotic" platforms. While this has been
  improved it is recommended to thoroughly test the HTTP transport before deploying
  Synapse in a production environment based on these platforms. Please don't hesitate
  to report any issues using JIRA or by posting a message on the mailing list.

<a id="synapse-apache-org-userguide-deployment--Overview_of_available_deployment_options"></a>

## <a id="synapse-apache-org-userguide-deployment--Overview_of_available_deployment_options"></a>Overview of available deployment options

Synapse can be deployed in two different ways:

- Stand-alone, i.e. as an independently managed Java process.
- As a J2EE application (WAR) deployed into a simple servlet container (e.g. Tomcat)
  or a full-featured J2EE application server.

Since Synapse doesn't rely on any container API, the features offered are the same in
both deployment scenarios, with very few exceptions:

- There is a minor issue that prevents classpath resources from being used in a
  WAR deployment. See [SYNAPSE-207](https://issues.apache.org/jira/browse/SYNAPSE-207)
  .
- When deployed as a WAR file, Synapse can be configured with the standard Axis2
  servlet based HTTP transport: while the recommended HTTP implementation for Synapse
  is the NIO HTTP transport, there might be situations where it is preferable or
  mandatory to use the HTTP protocol implementation of the application server.

In some scenarios Synapse is used to proxy services that are deployed themselves on
an application server. In these cases it would be interesting to deploy Synapse on
the same application server and use an in-VM transport instead of HTTP to communicate
with these services. Note that for the moment no production-grade implementation of
this type of transport exists yet for Axis2, but this might change in the future.

Since the features offered are almost the same, the differences between the two
deployment options are mainly related to packaging and operational considerations:

- Many IT departments prefer deploying J2EE applications than managing stand-alone
  Java processes, because this allows them to leverage the management and monitoring
  facilities offered by the application server.
- If the use case relies on JNDI resources such as JMS connection factories,
  JDBC data source and transactions it might be easier to set up and configure these
  resources when Synapse is deployed directly on the application
  server that hosts these resources.

<a id="synapse-apache-org-userguide-deployment--Stand-alone_deployment"></a>

## <a id="synapse-apache-org-userguide-deployment--Stand-alone_deployment"></a>Stand-alone deployment

<a id="synapse-apache-org-userguide-deployment--Using_the_standard_binary_distribution"></a>

### <a id="synapse-apache-org-userguide-deployment--Using_the_standard_binary_distribution"></a>Using the standard binary distribution

The easiest way to get started with a stand-alone deployment is using the standard
binary distribution ZIP or tarball (see [download.html](../download.html)).
It already contains everything that is needed to run Synapse stand-alone and you
only need to customize it according to your requirements:

- Place your mediation configuration in repository/conf/synapse-config
  directory.
- Place any additional files such as WSDL files, endpoint definitions, etc.
  referenced by your configuration in the repository directory.
- Customize repository/conf/axis2.xml
  to enable and disable transports according to your needs.
- Add any additional libraries required by your mediation to the
  libdirectory. Alternatively modify repository/conf/wrapper.conf
  to add directories and JAR files to the classpath.
- Add any required modules to repository/modules.
- If necessary, modify lib/log4j.properties to configure logging.

Since the standard binary distribution also contains samples and documentation,
you might want to remove the following folders:

- docs
- repository/conf/sample
- samples

The bin directory contains Unix and Windows scripts to run Synapse:

- synapse.sh and synapse.bat allow to run Synapse in non
  daemon mode.
- synapse-daemon.sh is a Sys V init script that can be used on Unix
  systems to start and stop Synapse in daemon mode.
- install-synapse-service.bat and uninstall-synapse-service.bat
  can be used on Windows to install Synapse as an NT service.

<a id="synapse-apache-org-userguide-deployment--Using_Maven_to_build_a_custom_distribution"></a>

### <a id="synapse-apache-org-userguide-deployment--Using_Maven_to_build_a_custom_distribution"></a>Using Maven to build a custom distribution

Building a custom Synapse package based on the standard binary distribution is a
manual process and this has some drawbacks:

- The JAR files required to run Synapse must be selected manually and it is not easy to identify unused JARs
  that could be safely removed.
- The process is not suitable if there is a requirement for strict configuration management. In particular:
  - Because of the large number of JAR files, managing the artifacts using
    a source control repository is not practical.
  - The process is not repeatable and there is no way to go back to a
    previous version of the artifacts.
- When upgrading to a newer version of Synapse (or when working with snapshot
  versions), it is necessary either to manually replace the JARs in the current
  package or to start again from a new version of the standard binary
  distribution.
- If Synapse needs to be deployed with slightly different configurations in
  multiple environments (e.g. test and production), the corresponding packages
  need to be prepared manually.

Note that these problems not only arise in the development and maintenance phases
of a project, but also when doing proof of concepts that you want to keep in a safe
place for later reuse. One approach to overcome these difficulties is to use Maven
to assemble a custom package. When used correctly, this approach solves all of the
issues identified above. In particular Maven's dependency management together with
the excellent [assembly plugin](http://maven.apache.org/plugins/maven-assembly-plugin/)
can be used to automatically select the relevant JARs to include and pull them
from Maven repositories. The remaining artifacts required to assemble the package
can then be easily stored in a source control repository.

Synapse provides a Maven archetype that allows to set up this kind of project in
only a few simple steps. To begin with, change to the directory where you want to
create the project and issue the following command:

mvn archetype:generate -DarchetypeCatalog=http://synapse.apache.org

In case of problems, you can try to use the latest version of the archetype catalog:

mvn archetype:generate -DarchetypeCatalog=http://svn.apache.org/repos/asf/synapse/trunk/java/src/site/resources

Finally, if you have build Synapse from sources, you don't need to specify a
catalog at all: the archetype is added automatically to the local catalog during
the build.

In any case, when prompted by Maven, select synapse-package-archetype
for the Synapse version you want to use. In the next step enter the values for
groupId, artifactId and version for your project. You
will also be prompted for a package name. Since the archetype doesn't contain any source
code, this value is irrelevant and you can continue with the default value.

At this stage a Maven project has been created in a sub-directory with the same
name as the artifactId specified previously. You should now customize this
projects according to your needs:

- Add your mediation configuration to repository/conf/synapse-config
  directory.
- Customize the dependencies in pom.xml. In particular if additional
  transports such as JMS are needed, add the required dependencies here. Additional
  Axis2 modules should also be added here.
- Enable and configure additional transports in repository/conf/axis2.xml.
- Place any other files referenced by mediation configuration into the
  repository directory.

The project is built as usual with the following command:

mvn package

This will create a ZIP file (in the target directory) containing
everything that is needed to run your custom Synapse configuration. You only
need to extract it and use the appropriate script in the bin
directory to start Synapse.

<a id="synapse-apache-org-userguide-deployment--WAR_deployment"></a>

## <a id="synapse-apache-org-userguide-deployment--WAR_deployment"></a>WAR deployment

Synapse provides a standard WAR file that can be used to deploy mediation on a servlet
container or on a J2EE application server. Note that this WAR file is not part of the
downloadable distributions. It can be retrieved from the following location:

- [http://repo1.maven.org/maven2/org/apache/synapse/synapse-war/](http://repo1.maven.org/maven2/org/apache/synapse/synapse-war/)
  for released versions.
- [https://builds.apache.org/view/All/job/Synapse%20-%20Trunk/lastBuild/org.apache.synapse$synapse-war/
  ](https://builds.apache.org/view/All/job/Synapse%20-%20Trunk/lastBuild/org.apache.synapse$synapse-war/)
  for snapshot versions.

Customization of the Web application is similar to the stand-alone option, but the
default directory structure is different:

- synapse.xml and axis2.xml are placed into the WEB-INF/conf
  directory. All other files referenced by your mediation should go to the
  WEB-INF/repository
  directory.
- Additional libraries must be placed into the standard WEB-INF/lib
  directory.
- Axis2 modules are located in repository/modules.
- log4j.properties is located in WEB-INF/classes.

---

<a id="synapse-apache-org-userguide-extending"></a>

# Apache Synapse – Apache Synapse - Extending Synapse

## <a id="synapse-apache-org-userguide-extending--Apache_Synapse_ESB_-_Extending_Synapse"></a>Apache Synapse ESB - Extending Synapse

Apache Synapse provides a number of extension points so that
users can plug-in custom developed code to extend the
functionality of the ESB. While the built-in mediators are sufficient to implement
most integration scenarios, sometimes it is very helpful to be able to deploy some custom code into the
service bus and make the solution simpler. Most Synapse APIs are in Java and
therefore the users looking to extend Synapse are expected to have a
decent knowledge and experience in Java programming.

## <a id="synapse-apache-org-userguide-extending--Writing_custom_Mediator_implementations"></a>Writing custom Mediator implementations

The primary interface of the Synapse API is the MessageContext
interface defined below. This essentially defines the per-message
context passed through the chain of mediators, for each and every
message received and processed by Synapse. Each message instance is
wrapped within a MessageContext instance, and the message context
is set with the references to the SynapseConfiguration and
SynapseEnvironment objects. The
[SynapseConfiguration](../apidocs/org/apache/synapse/config/SynapseConfiguration.html)
object holds the global configuration model that defines
mediation rules, local registry entries and other and configuration, while
the
[SynapseEnvironment](../apidocs/org/apache/synapse/core/SynapseEnvironment.html)
object gives access to the underlying SOAP implementation used -
Axis2. A typical mediator would need to manipulate the
MessageContext by referring to the SynapseConfiguration. However, it
is strongly recommended that the SynapseConfiguration is not
updated by mediator instances as it is shared by all messages, and
may be updated by Synapse administration or configuration modules.
Mediator instances may store local message properties into the
MessageContext for later retrieval by successive mediators.

#### <a id="synapse-apache-org-userguide-extending--MessageContextInterface"></a> [MessageContext Interface ](http://svn.apache.org/viewvc/synapse/trunk/java/modules/core/src/main/java/org/apache/synapse/MessageContext.java?view=markup)

package org.apache.synapse;
import ...
public interface MessageContext {
/\*\*
\* Get a reference to the current SynapseConfiguration
\*
\* @return the current synapse configuration
\*/
public SynapseConfiguration getConfiguration();
/\*\*
\* Set or replace the Synapse Configuration instance to be used. May be used to
\* programmatically change the configuration at runtime etc.
\*
\* @param cfg The new synapse configuration instance
\*/
public void setConfiguration(SynapseConfiguration cfg);
/\*\*
\* Returns a reference to the host Synapse Environment
\* @return the Synapse Environment
\*/
public SynapseEnvironment getEnvironment();
/\*\*
\* Sets the SynapseEnvironment reference to this context
\* @param se the reference to the Synapse Environment
\*/
public void setEnvironment(SynapseEnvironment se);
/\*\*
\* Get the value of a custom (local) property set on the message instance
\* @param key key to look up property
\* @return value for the given key
\*/
public Object getProperty(String key);
/\*\*
\* Set a custom (local) property with the given name on the message instance
\* @param key key to be used
\* @param value value to be saved
\*/
public void setProperty(String key, Object value);
/\*\*
\* Returns the Set of keys over the properties on this message context
\* @return a Set of keys over message properties
\*/
public Set getPropertyKeySet();
/\*\*
\* Get the SOAP envelope of this message
\* @return the SOAP envelope of the message
\*/
public SOAPEnvelope getEnvelope();
/\*\*
\* Sets the given envelope as the current SOAPEnvelope for this message
\* @param envelope the envelope to be set
\* @throws org.apache.axis2.AxisFault on exception
\*/
public void setEnvelope(SOAPEnvelope envelope) throws AxisFault;
/\*\*
\* SOAP message related getters and setters
\*/
public ....get/set()...
}

The MessageContext interface is based on the Axis2
MessageContext interface, and uses the Axis2 EndpointReference and
SOAPEnvelope classes/interfaces. The purpose of this interface is
to capture a message as it flows through the system. As you will
see the message payload is represented using the SOAP infoset.
Binary messages can be embedded in the Envelope using MTOM or SwA
attachments using the AXIOM object model.

#### <a id="synapse-apache-org-userguide-extending--Mediatorinterface"></a> [Mediator interface ](http://svn.apache.org/viewvc/synapse/trunk/java/modules/core/src/main/java/org/apache/synapse/Mediator.java?view=markup)

The second key interface for mediator writers is the Mediator
interface:

package org.apache.synapse;
import org.apache.synapse.MessageContext;
/\*\*
\* All Synapse mediators must implement this Mediator interface. As a message passes
\* through the synapse system, each mediator's mediate() method is invoked in the
\* sequence/order defined in the SynapseConfiguration.
\*/
public interface Mediator {
/\*\*
\* Invokes the mediator passing the current message for mediation. Each
\* mediator performs its mediation action, and returns true if mediation
\* should continue, or false if further mediation should be aborted.
\*
\* @param synCtx the current message for mediation
\* @return true if further mediation should continue
\*/
public boolean mediate(MessageContext synCtx);
/\*\*
\* This is used for debugging purposes and exposes the type of the current
\* mediator for logging and debugging purposes
\* @return a String representation of the mediator type
\*/
public String getType();
}

A mediator can read and/or modify the message encapsulated in
the MessageContext in any suitable manner - adjusting the routing
headers or changing the message body. If the mediate() method
returns false, it signals to the Synapse processing model to stop
further processing of the message. For example, if the mediator is
a security agent it may decide that this message is dangerous and
should not be processed further. This is generally the exception as
mediators are usually designed to co-operate to rocess the message
onwards.

### <a id="synapse-apache-org-userguide-extending--Leaf_and_Node_Mediators_List_mediators_and_Filter_mediators"></a> Leaf and Node Mediators, List mediators and Filter mediators

Mediators may be Node mediators (i.e. these that can contain
child mediators) or Leaf mediators (mediators that does not hold
any other child mediators). A Node mediator must implement the
org.apache.synapse.mediators.ListMediator interface listed below,
or extend from the
org.apache.synapse.mediators.AbstractListMediator.

#### <a id="synapse-apache-org-userguide-extending--TheListMediator_interface"></a> [The ListMediator interface ](http://svn.apache.org/viewvc/synapse/trunk/java/modules/core/src/main/java/org/apache/synapse/mediators/ListMediator.java?view=markup)

package org.apache.synapse.mediators;
import java.util.List;
/\*\*
\* The List mediator executes a given sequence/list of child mediators
\*/
public interface ListMediator extends Mediator {
/\*\*
\* Appends the specified mediator to the end of this mediator's (children) list
\* @param m the mediator to be added
\* @return true (as per the general contract of the Collection.add method)
\*/
public boolean addChild(Mediator m);
/\*\*
\* Appends all of the mediators in the specified collection to the end of this mediator's (children)
\* list, in the order that they are returned by the specified collection's iterator
\* @param c the list of mediators to be added
\* @return true if this list changed as a result of the call
\*/
public boolean addAll(List c);
/\*\*
\* Returns the mediator at the specified position
\* @param pos index of mediator to return
\* @return the mediator at the specified position in this list
\*/
public Mediator getChild(int pos);
/\*\*
\* Removes the first occurrence in this list of the specified mediator
\* @param m mediator to be removed from this list, if present
\* @return true if this list contained the specified mediator
\*/
public boolean removeChild(Mediator m);
/\*\*
\* Removes the mediator at the specified position in this list
\* @param pos the index of the mediator to remove
\* @return the mediator previously at the specified position
\*/
public Mediator removeChild(int pos);
/\*\*
\* Return the list of mediators of this List mediator instance
\* @return the child/sub mediator list
\*/
public List getList();
}

A ListMediator implementation should call super.mediate(synCtx)
to process its sub mediator sequence. A FilterMediator is a
ListMediator which executes its sequence of sub mediators on
successful outcome of a test condition. The Mediator instance which
performs filtering should implement the FilterMediator interface.

#### <a id="synapse-apache-org-userguide-extending--FilterMediatorinterface"></a> [FilterMediator interface ](http://svn.apache.org/viewvc/synapse/trunk/java/modules/core/src/main/java/org/apache/synapse/mediators/FilterMediator.java?view=markup)

package org.apache.synapse.mediators;
import org.apache.synapse.MessageContext;
/\*\*
\* The filter mediator is a list mediator, which executes the given (sub) list of mediators
\* if the specified condition is satisfied
\*
\* @see FilterMediator#test(org.apache.synapse.MessageContext)
\*/
public interface FilterMediator extends ListMediator {
/\*\*
\* Should return true if the sub/child mediators should execute. i.e. if the filter
\* condition is satisfied
\* @param synCtx
\* @return true if the configured filter condition evaluates to true
\*/
public boolean test(MessageContext synCtx);
}

## <a id="synapse-apache-org-userguide-extending--Writing_custom_Configuration_implementations_for_mediators"></a>Writing custom Configuration implementations for mediators

You may write your own custom configurator for the Mediator
implementation you write without relying on the Class mediator or
Spring extension for its initialization. You could thus write a
MediatorFactory implementation which defines how to digest a custom
XML configuration element to be used to create and configure the
custom mediator instance. A MediatorSerializer implementation
defines how a configuration should be serialized back into
an XML configuration. The custom MediatorFactory &
MediatorSerializer implementations and the mediator class/es must be bundled in a JAR
file conforming to the J2SE Service Provider model (See the
description for Extensions below for more details and examples) and
placed into the SYNAPSE\_HOME/lib folder, so that the Synapse
runtime could find and load the definition. Essentially this means
that a custom JAR file must bundle your class implementing the
Mediator interface, and the MediatorFactory implementation class and
contain two text files named
"org.apache.synapse.config.xml.MediatorFactory" and
"org.apache.synapse.config.xml.MediatorSerializer" which
will contain the fully qualified name(s) of your MediatorFactory
and MediatorSerializer implementation classes. You should also
place any dependency JARs into the same lib folder so that the
correct classpath references could be made.
The MediatorFactory interface listing is given below, which you
should implement, and its getTagQName() method must define the fully qualified
element of interest for custom configuration. The Synapse
initialization will call back to this MediatorFactory instance through the
createMediator(OMElement elem) method passing in this XML element,
so that an instance of the mediator could be created utilizing the
custom XML specification and returned. See the ValidateMediator and
the ValidateMediatorFactory classes under modules/extensions in the
Synapse source distribution for examples.

#### <a id="synapse-apache-org-userguide-extending--TheMediatorFactory_interface"></a> [The MediatorFactory interface ](http://svn.apache.org/viewvc/synapse/trunk/java/modules/core/src/main/java/org/apache/synapse/config/xml/MediatorFactory.java?view=markup)

package org.apache.synapse.config.xml;
import ...
/\*\*
\* A mediator factory capable of creating an instance of a mediator through a given
\* XML should implement this interface
\*/
public interface MediatorFactory {
/\*\*
\* Creates an instance of the mediator using the OMElement
\* @param elem
\* @return the created mediator
\*/
public Mediator createMediator(OMElement elem);
/\*\*
\* The QName of this mediator element in the XML config
\* @return QName of the mediator element
\*/
public QName getTagQName();
}

#### <a id="synapse-apache-org-userguide-extending--TheMediatorSerializer_interface"></a> [The MediatorSerializer interface ](http://svn.apache.org/viewvc/synapse/trunk/java/modules/core/src/main/java/org/apache/synapse/config/xml/MediatorSerializer.java?view=markup)

package org.apache.synapse.config.xml;
import ...
/\*\*
\* Interface which should be implemented by mediator serializers. Does the
\* reverse of the MediatorFactory
\*/
public interface MediatorSerializer {
/\*\*
\* Return the XML representation of this mediator
\* @param m mediator to be serialized
\* @param parent the OMElement to which the serialization should be attached
\* @return the serialized mediator XML
\*/
public OMElement serializeMediator(OMElement parent, Mediator m);
/\*\*
\* Return the class name of the mediator which can be serialized
\* @return the class name
\*/
public String getMediatorClassName();
}

## <a id="synapse-apache-org-userguide-extending--Configuring_mediators"></a>Configuring mediators

Mediators could access the Synapse registry to load resources
and configure the local behaviour. Refer to the Spring mediator and
Script mediator implementations for examples on how this could be
achieved.

#### <a id="synapse-apache-org-userguide-extending--Loading_of_Extensions_by_the_Synapse_runtime"></a> Loading of Extensions by the Synapse runtime

Synapse loads available extensions from the runtime classpath
using the
[J2SE
Service Provider model
](http://java.sun.com/j2se/1.3/docs/guide/jar/jar.html#Service%20Provider)
. This essentially iterates over the available JAR files, for a META-INF/services directory within each file,
and looks for a text file with the name org.apache.synapse.config.xml.MediatorFactory
which contains a list of fully qualified classname that implement
the above interface, listing each class in a separate line. e.g. The
built-in synapse-extensions.jar contains the following structure

synapse-extensions.jar
/META-INF/services
org.apache.synapse.config.xml.MediatorFactory
org.apache.synapse.config.xml.MediatorSerializer
/... the implementation classes as usual...

## <a id="synapse-apache-org-userguide-extending--Writing_Synapse_Observers"></a>Writing Synapse Observers

A Synapse observer is developed by either implementing the
org.apache.synapse.config.SynapseObserver interface or by
extending the org.apache.synapse.config.AbstractSynapseObserver
class. A Synapse observer is notified by the Synapse configuration
when new elements are added to the configuration and
when existing elements are removed from the configuration. The
following event handlers are available to the Synapse observer implementations.

public void sequenceAdded(Mediator sequence);
public void sequenceRemoved(Mediator sequence);
public void entryAdded(Entry entry);
public void entryRemoved(Entry entry);
public void endpointAdded(Endpoint endpoint);
public void endpointRemoved(Endpoint endpoint);
public void proxyServiceAdded(ProxyService proxy);
public void proxyServiceRemoved(ProxyService proxy);
public void startupAdded(Startup startup);
public void startupRemoved(Startup startup);
public void eventSourceAdded(SynapseEventSource eventSource);
public void eventSourceRemoved(SynapseEventSource eventSource);

The AbstractSynapseObserver provides default implementations to
all these event handlers. It simply logs any received events.

In situations where the custom code has access to the
SynapseConfiguration class observers can be directly registered
with the SynapseConfiguration by using
the registerObserver(SynapseObserver o) method. Otherwise
SynapseObserver implementations
can be defined in the synapse.properties file which resides in the
SYNAPSE\_HOME/lib directory. The following example shows how two observers are
registered with the Synapse configuration using the
synapse.properties file.

synapse.observers=test.LoggingObserverImpl, test.SimpleObserverImpl

## <a id="synapse-apache-org-userguide-extending--Scheduled_Tasks"></a>Scheduled Tasks

A scheduled task is a custom developed piece of Java code that
is scheduled in the ESB to execute periodically. A scheduled task
must implement the org.apache.synapse.task.Task
interface. This interface has a single 'execute' method. Once scheduled the
execute method is called by Synapse periodically.

Synapse also comes with a built-in task implementation known as
the MessageInjector. This task can be used to inject messages into
the service bus periodically. Refer sample 300 to see how to use the
MessageInjector task.

---

<a id="synapse-apache-org-userguide-faq"></a>

# Apache Synapse – FAQ

## <a id="synapse-apache-org-userguide-faq--Apache_Synapse_FAQs"></a>Apache Synapse FAQs

Welcome to Apache Synapse FAQs.

## <a id="synapse-apache-org-userguide-faq--GeneralGeneralApache_Synapse_questions_-_Non_technical"></a>General(GeneralApache Synapse questions - Non technical)

1. What is Apache Synapse?
   - Apache Synapse is a lightweight and high-performance Enterprise Service
     Bus (ESB).
2. What makes Apache Synapse unique?
   - Apache Synapse is fast and able to handle thousands of concurrent
     connections
     with constant memory usage. It comes with a rich set of mediators to
     support almost any integration scenario out of the box. It is also
     easily
     extensible and highly customizable.
3. What is the license?
   - Apache Synapse comes with Apache 2.0 licence.

## <a id="synapse-apache-org-userguide-faq--MediationQuestions_related_to_sequences_endpoints_proxies_etc"></a>Mediation(Questions related to sequences, endpoints, proxies etc)

1. What is a proxy service?
   - A proxy service is a virtual service hosted on the ESB. It can accept
     requests from service clients, just like a real Web Service. A proxy
     service can process requests and forward them to an actual Web Service
     (back end service) to be further processed. The responses coming back
     from
     the back end service can be routed back to the original client. Proxy
     services are mostly used to expose an existing service over a different
     transport, format or QoS configuration.
2. What is a mediator?
   - A mediator is the basic message processing unit in the ESB. A mediator
     can take a message, carry out some predefined actions on it and output
     the modified message. Apache Synapse ships with a range of mediators capable
     of carrying out various tasks on input messages.
3. What is a sequence?
   - A sequence is an ordered list of mediators (a mediator chain). When a
     sequence is given a message, it will go through all the mediators in the
     sequence. A sequence can also handover messages to other sequences.
4. What is an Endpoint?
   - A logical representation of an actual endpoint or a group of endpoints
     (i.e. Load Balancing and Fail Over).
5. What are Local Entries?
   - Local entries can be used to hold various configuration elements
     required by sequences and proxy services. Usually they are used to hold
     WSDLs, XSDs, XSLT files etc. A local entry can contain XML content as
     well
     as plain text content. A local entry can be configured to load content
     from a remote file too.
6. What is a Message Mediation?
   - Managing and transforming the messages flowing between the client and a
     service in an enterprise.
7. What is Message Mediation?
   - Mediating messages coming into a specific service by specifying the
     target URI as a Synapse mediation service.
8. What is Service Mediation?
   - Mediating messages coming into a specific service by specifying the
     target URI as a Synapse mediation service.
9. What is a Message Store?
   - Message Store is the storage for ESB messages. It can be an in-memory
     store
     or can be JMS store with an external Message Broker. You can always plug
     your
     own message store implementations as well.
10. What is a Message Processor?
    - Message processor can be used to implement different messaging and
      integration patters along with Message stores. Message processors will
      consume
      the messages in message stores and do the processing of them.
11. What is a Template?
    - ESB Templates try to minimize this redundancy by creating prototypes
      that
      users can re-use and utilize as and when needed. This is very much
      analogous
      to classes and instances of classes where-as, a template is a class that
      can be used to wield instance objects such as templates and endpoints.
12. What is the REST Api?
    - REST Api can be used to mediate HTTP POST, GET, PUT and DELETE request
      through Synapse and to integrate various RESTful services.
13. Can Endpoint perform error handling?
    - Yes. Endpoints can do error handling. User can configure the behavior
      of an endpoints when it faced to a erroneous situation.

## <a id="synapse-apache-org-userguide-faq--TransportsTransport_related_questions"></a>Transports(Transport related questions)

1. What are the transports supported by the Apache Synapse?
   - HTTP, HTTPS, VFS based file transport, FIX, Hessian, HL7,UDP, JMS, Mail,
     TCP, XMPP
2. Do I need an external JMS broker for the JMS transport?
   - Yes, Apache Synapse requires an external JMS broker like Apache ActiveMQ
3. Does Apache Synapse support two way JMS scenario (request/response) ?
   - Yes, you can refer sample 264 which demonstrates exactly the JMS
     request/response scenario.
4. What is the Passthrough transport?
   - This is the default HTTP transport used by Apache Synapse. HTTP PassThrough Transport
     is a non-blocking HTTP transport implementation based on HTTP Core NIO and specially
     designed for streaming messages. It is similar to the old message relay transport,
     but it does not care about the content type and simply streams all received messages
     through. It also has a simpler and cleaner model for forwarding messages back and forth.
     It can be used as an alternative to the NHTTP transport.
5. What is the NHTTP transport?
   - NHTTP stands for non-blocking HTTP. NHTTP transport uses the Java Non-blocking I/O API.
     This allows the NHTTP transport to scale into handling hundreds of connections
     without blocking the threads. The server worker threads used by the NHTTP
     transport do not get blocked on I/O until the Synapse receives responses
     for the already forwarded requests. Therefore Apache Synapse can accept
     more concurrent connections and requests than most HTTP server products.
6. What is the underlying HTTP library used by the NHTTP/Passthrough transport?
   - NHTTP transport uses the Apache Http Core NIO library underneath. This
     library provides low level I/O handling and HTTP level detail handling.

---

<a id="synapse-apache-org-userguide-mediators"></a>

# Apache Synapse – Apache Synapse - Mediators Catalog

## <a id="synapse-apache-org-userguide-mediators--Mediators_Catalog"></a>Mediators Catalog

This document lists all the built-in mediators of Synapse and describes their
usage, functionality and configuration syntax.

## <a id="synapse-apache-org-userguide-mediators--Contents"></a>Contents

- [Introduction](#synapse-apache-org-userguide-mediators--Intro)
- [Mediator Categories](#synapse-apache-org-userguide-mediators--Categories)
- [Core Mediators](#synapse-apache-org-userguide-mediators--CoreMediators)
  - [Drop Mediator](#synapse-apache-org-userguide-mediators--Drop)
  - [Log Mediator](#synapse-apache-org-userguide-mediators--Log)
  - [Property Mediator](#synapse-apache-org-userguide-mediators--Property)
  - [Send Mediator](#synapse-apache-org-userguide-mediators--Send)
  - [Respond Mediator](#synapse-apache-org-userguide-mediators--Respond)
  - [Loopback Mediator](#synapse-apache-org-userguide-mediators--Loopback)
- [Filter Mediators](#synapse-apache-org-userguide-mediators--FilterMediators)
  - [Filter Mediator](#synapse-apache-org-userguide-mediators--Filter)
  - [In/Out Mediator](#synapse-apache-org-userguide-mediators--InOut)
  - [Switch Mediator](#synapse-apache-org-userguide-mediators--Switch)
  - [Validate Mediator](#synapse-apache-org-userguide-mediators--Validate)
- [Transformation Mediators](#synapse-apache-org-userguide-mediators--TransformationMediators)
  - [Header Mediator](#synapse-apache-org-userguide-mediators--Header)
  - [MakeFault Mediator](#synapse-apache-org-userguide-mediators--MakeFault)
  - [Payload Factory Mediator](#synapse-apache-org-userguide-mediators--PayloadFactory)
  - [URL Rewrite Mediator](#synapse-apache-org-userguide-mediators--URLRewrite)
  - [XSLT Mediator](#synapse-apache-org-userguide-mediators--XSLT)
  - [XQuery Mediator](#synapse-apache-org-userguide-mediators--XQuery)
- [Extension Mediators](#synapse-apache-org-userguide-mediators--ExtensionMediators)
  - [Class Mediator](#synapse-apache-org-userguide-mediators--Clazz)
  - [POJO Command Mediator](#synapse-apache-org-userguide-mediators--POJOCommand)
  - [Script Mediator](#synapse-apache-org-userguide-mediators--Script)
  - [Spring Mediator](#synapse-apache-org-userguide-mediators--Spring)
- [Advanced Mediators](#synapse-apache-org-userguide-mediators--AdvancedMediators)
  - [Aggregate Mediator](#synapse-apache-org-userguide-mediators--Aggregate)
  - [Cache Mediator](#synapse-apache-org-userguide-mediators--Cache)
  - [Callout Mediator](#synapse-apache-org-userguide-mediators--Callout)
  - [Clone Mediator](#synapse-apache-org-userguide-mediators--Clone)
  - [DBLookup Mediator](#synapse-apache-org-userguide-mediators--DBLookup)
  - [DBReport Mediator](#synapse-apache-org-userguide-mediators--DBReport)
  - [Iterate Mediator](#synapse-apache-org-userguide-mediators--Iterate)
  - [RMSequence Mediator](#synapse-apache-org-userguide-mediators--RMSequence)
  - [Store Mediator](#synapse-apache-org-userguide-mediators--Store)
  - [Throttle Mediator](#synapse-apache-org-userguide-mediators--Throttle)
  - [Transaction Mediator](#synapse-apache-org-userguide-mediators--Transaction)

<a id="synapse-apache-org-userguide-mediators--Intro"></a>

## <a id="synapse-apache-org-userguide-mediators--Introduction"></a>Introduction

Mediator is the basic message processing unit in Synapse. A mediator takes an
input message, carries out some processing on it, and provides an output message.
Mediators can be linked up and arranged into chains to implement complex message
flows (sequences). Mediators can manipulate message content (payload), properties,
headers and if needed can also execute additional tasks such as database lookup,
service invocation and script execution.

Apache Synapse ships with an array of useful mediators that can be used out of the
box to implement message flows, services and integration patterns. Rest of this
article describes these mediators in detail, along with their use cases and
configuration syntax.

<a id="synapse-apache-org-userguide-mediators--Categories"></a>

## <a id="synapse-apache-org-userguide-mediators--Mediator_Categories"></a>Mediator Categories

Built-in mediators of Synapse can be classified into several groups depending
on the nature of their functionality and use cases.

- Core mediators - Utility mediators that are useful in a variety of scenarios
- Filter mediators - Mediators used to filter out messages
- Transform mediators - Mediators used to transform message content, headers and
  attributes
- Extension mediators - Mediators used to extend the Synapse mediation engine by
  plugging in custom developed code
- Advanced mediators - Mediators used to implement advanced integration scenarios
  and patterns

Rest of this article is structured according to the above classification. Mediators
in each section are arranged in the alphabetical order.

<a id="synapse-apache-org-userguide-mediators--CoreMediators"></a>

## <a id="synapse-apache-org-userguide-mediators--Core_Mediators"></a>Core Mediators

<a id="synapse-apache-org-userguide-mediators--Drop"></a>

### <a id="synapse-apache-org-userguide-mediators--Drop_Mediator"></a>Drop Mediator

Drop mediator can be used to drop the current message being processed and
terminate a message flow. This mediator is configured as follows and it
does not take any additional parameters or arguments.

<drop/>

<a id="synapse-apache-org-userguide-mediators--Log"></a>

### <a id="synapse-apache-org-userguide-mediators--Log_Mediator"></a>Log Mediator

Log mediator can be used in any sequence or proxy service to log the messages
being mediated. Log entries generated by the log mediator will go into the
standard Synapse log files. This can be further configured using the
log4j.properties file.

By default, the log mediator only logs a minimalistic set of details to avoid
the message content being parsed. But if needed it can be configured to log the
full message payload, headers and even custom user defined properties. The log
mediator configuration takes the following general form.

<log [level="simple|full|headers|custom"] [separator="string"]
[category="INFO|DEBUG|WARN|ERROR|TRACE|FATAL"]>
<property name="string" (value="literal" | expression="xpath")/>\*
</log>

The 'level' attribute is used to specify how much information should be logged
by the log mediator. This attribute can take one of following four values.

- simple - Logs a set of standard headers (To, From, WSAction, SOAPAction,
  ReplyTo and MessageID). If no log level is specified, this level will be
  used by default.
- full - Logs all standard headers logged in the log level 'simple' and also
  the full payload of the message. This log level causes the message content
  to be parsed and hence incurs a performance overhead.
- headers - Logs all SOAP header blocks
- custom - Only logs the user defined properties (see the next section)

Users can define custom attributes and properties to be logged by the log mediator
by specifying some 'property' elements. Each property must be named, and can
have a constant value or an XPath expression. If a constant value is specified,
that value will be logged with each and every entry logged by the mediator. If
an XPath is specified instead, that XPath will be evaluated on the message being
mediated and the outcome will be included in the generated log entry.

By default, all properties and attributes logged by the log mediator are separated
by commas (,). This can be configured using the 'separator' attribute. Further
all logs generated by the mediator are logged at log4j log level 'INFO' by default.
This behavior can also be configured using the 'category' attribute.
In addition to this behaviour, when 'category' is set to debug, logs can be printed
at log4j log level 'INFO' by starting the server with the following flag. This is
especially helpful during the development time to quickly debug the mediation flow.

Linux / Unix: ./synapse.sh -synapseDebug  
Windows: synapse.bat -synapseDebug

<a id="synapse-apache-org-userguide-mediators--Property"></a>

### <a id="synapse-apache-org-userguide-mediators--Property_Mediator"></a>Property Mediator

Every message mediated through Synapse can have a set of associated properties.
Synapse engine and the underlying transports set a number of properties on
each message processed which can be manipulated by the user to modify the
runtime behavior of the message flows. In addition, user can set his/her own
properties on the message which is very helpful when it comes to managing
message flow state and storing scenario specific variables. For an example in
some situations a user might want to access a particular value in the request
payload while processing a response. This can be easily achieved by setting the
required value to a property in the request (in) sequence and then later accessing
that property in the response (out) sequence.

Property mediator is used to manipulate the properties of a message. This
mediator can be used to set and remove property values. When it comes to setting
property values, the input could be a constant or a variable value generated
by an XPath expression. The syntax for configuring the property mediator is as
follows.

<property name="string" [action=set|remove] [type="string"] (value="literal" | expression="xpath") [scope=default|transport|axis2|axis2-client] [pattern="regex" [group="integer"]]>
<xml-element/>?
</property>

The 'name' attribute specifies the name of the property which needs to be either
set or removed while the 'action' attribute specifies the exact action that needs
to be carried out by the mediator. If not specified action will default to 'set'.

When setting a property value, either the 'value' or the 'expression' attribute
must be specified. The 'value' attribute can be used to set a constant as
the property value whereas the 'expression' attribute can be used to specify an
XPath expression. If an XPath expression is specified, Synapse will evaluate that
on the message to determine the value that needs to be assigned to the property.

Synapse properties are scoped. Therefore, when using this mediator the user should
specify the scope at which the property will be set or removed from. If not
specified, property mediator will work at the 'default' scope. Properties set in
this scope last as long as the transaction (request-response) exists. Properties
set on scope 'axis2' has a shorter life span and it's mainly used for passing
parameters to the underlying Axis2 engine. Properties set in the 'transport'
scope will be treated as transport headers. For an example if it is required to
send an HTTP header named 'CustomHeader' with an outgoing request, one may use
the property mediator configuration.

<property name="CustomHeader" value="some value" scope="transport" type="type name"/>

This will force Synapse to send a transport header named 'CustomHeader' along
with the outgoing message. Property mediator also supports a scope named
'axis2-client'. Properties set in this scope will be treated as Axis2 client
options.

When using properties to store user or scenario specific information it is
recommended to always use the 'default' scope. Other scopes should not be used
for custom development or mediation work since they have the potential to
alter the behavior of the underlying Axis2 engine and transports framework.

By default, property mediator sets all property values as strings. It is possible
to set properties in other types by specifying the 'type' attribute. This attribute
can accept one of following values.

- STRING
- BOOLEAN
- DOUBLE
- FLOAT
- INTEGER
- LONG
- SHORT
- OM

The type names are case sensitive. Type 'OM' can be used to set XML property
values on the message context. This becomes useful when the expression associated
with the property mediator evaluates to an XML node during mediation. With the
type attribute set to 'OM' the resulting XML will be converted to an AXIOM
OMElement before assigning it to a property.

It is also possible to use the property mediator to set some static XML content
as a property value. To do this specify the static XML content as a child node
of the 'property' element instead of using the 'value' attribute.

<a id="synapse-apache-org-userguide-mediators--Send"></a>

### <a id="synapse-apache-org-userguide-mediators--Send_Mediator"></a>Send Mediator

Send mediator is used to send requests to endpoints. The same can be used
to send response messages back to clients. The send mediator is configured using
the following XML syntax.

<send [receive="string"]>
(endpointref | endpoint)?
</send>

Messages are sent to the endpoint specified as the child of the
'send' element. An optional receiving sequence can be configured using the
'receive' attribute. When specified, response messages from the endpoint will
be dispatched to the referred sequence. This makes it easier to implement
complex service chaining scenarios, where the response from one service needs
to be processed and directed to another service.

The send mediator can be configured without any child endpoints. For an example
following is a perfectly valid send mediator configuration.

<send/>

In this case the messages will be sent to an implicit endpoint. If the message
is a request from a client, Synapse will lookup the 'To' header of the request and
simply forward it to the service addressed by that header. If it is a response
from a back-end service, Synapse will simply send it back to the original
client who initiated the original message flow.

The service invocations done by the send mediator may or may not be
synchronous based on the underlying transport used. If the default non-blocking
HTTP transport is used, the send mediator will make an asynchronous invocation
and release the calling thread as soon as possible. Synapse will asynchronously
handle the response from the endpoint while the giving the illusion that Synapse
is making blocking service calls.

<a id="synapse-apache-org-userguide-mediators--Respond"></a>

### <a id="synapse-apache-org-userguide-mediators--Respond_Mediator"></a>Respond Mediator

The Respond Mediator stops the processing on the current message flow and sends
the message back to the client as a response.

<respond/>

<a id="synapse-apache-org-userguide-mediators--Loopback"></a>

### <a id="synapse-apache-org-userguide-mediators--Loopback_Mediator"></a>Loopback Mediator

The Loopback Mediator moves the message from the In flow to the Out flow.
All the configuration in the In flow that appears after the Loopback mediator is skipped.

<loopback/>

<a id="synapse-apache-org-userguide-mediators--FilterMediators"></a>

## <a id="synapse-apache-org-userguide-mediators--Filter_Mediators"></a>Filter Mediators

<a id="synapse-apache-org-userguide-mediators--Filter"></a>

### <a id="synapse-apache-org-userguide-mediators--Filter_Mediator"></a>Filter Mediator

Filter mediator adds 'if-else' like semantics to the Synapse configuration language.
It can be used to evaluate a condition on a message and take some action
based on the outcome. The configuration of the filter mediator takes the
following form.

<filter (source="xpath" regex="string") | xpath="xpath">
mediator+
</filter>

The filter mediator either tests the given XPath expression as a boolean
expression, or matches the result of the source XPath expression as a string
against the given regular expression. If the condition evaluates to true, the
filter mediator will execute the enclosed child mediators.

Alternatively, one can use the following syntax to configure the filter mediator.

<filter (source="xpath" regex="string") | xpath="xpath">
<then [sequence="string"]>
mediator+
</then>
<else [sequence="string"]>
mediator+
</else>
</filter>

In this case also the filter condition is evaluated in the same manner as
described above. Messages for which the condition evaluates to true will be
mediated through the mediators enclosed by the 'then' element. Failed messages
will be mediated through the mediators enclosed by the 'else' element.

<a id="synapse-apache-org-userguide-mediators--InOut"></a>

### <a id="synapse-apache-org-userguide-mediators--InOut_Mediators"></a>In/Out Mediators

In mediator and Out mediator are used to filter out traffic based on the
direction of the messages. As their names imply, In mediator processes only
the requests (in messages) while ignoring the responses (out messages). The
out mediator does the exact opposite by processing only the responses while
ignoring the requests. In many occasions these two mediators are deployed
together to create separate flows for requests and responses. The syntax
outline for the two mediators is given below.

<in>
mediator+
</in>
<out>
mediator+
</out>

In mediator will process requests through the child mediators and the Out
mediator will process responses through the child mediators.

<a id="synapse-apache-org-userguide-mediators--Switch"></a>

### <a id="synapse-apache-org-userguide-mediators--Switch_Mediator"></a>Switch Mediator

Switch mediator provides switch-case semantics in the Synapse configuration
language.

<switch source="xpath">
<case regex="string">
mediator+
</case>+
<default>
mediator+
</default>?
</switch>

The source XPath is executed on the messages. The resulting value is then
tested against the regular expressions defined in each 'case' element. When
a matching case is found, the message will be mediated through its child
mediators. If none of the cases match, the message will be handed to the 'default'
case (if available).

<a id="synapse-apache-org-userguide-mediators--Validate"></a>

### <a id="synapse-apache-org-userguide-mediators--Validate_Mediator"></a>Validate Mediator

The validate mediator validates the XML node selected by
the source xpath expression, against the specified XML schema. If the source
attribute is not specified, the validation is performed against the first
child of the SOAP body of the current message. If the validation fails,
the on-fail sequence of mediators is executed. Feature elements could be used to
turn on/off some of the underlying features of the schema validator (See [http://xerces.apache.org/xerces2-j/features.html](http://xerces.apache.org/xerces2-j/features.html)).
The schema can be specified as a static or dynamic key. When
needed, imports can be specified using additional resources.

<validate [source="xpath"]>
<schema key="string" />+
<resource location="<external-schema>" key="string">\*
<feature name="<validation-feature-name>" value="true|false"/>\*
<on-fail>
mediator+
</on-fail>
</validate>

<a id="synapse-apache-org-userguide-mediators--TransformationMediators"></a>

## <a id="synapse-apache-org-userguide-mediators--Transformation_Mediators"></a>Transformation Mediators

<a id="synapse-apache-org-userguide-mediators--Header"></a>

### <a id="synapse-apache-org-userguide-mediators--Header_Mediator"></a>Header Mediator

Header mediator sets or removes a specified header from the message.
The optional 'scope' attribute specifies the scope of the header.
Scope can be either 'soap' or 'transport'.
If the scope is set to 'soap', header is treated as a soap header and if it is 'transport',
header is treated as a transport header.
If the scope is omitted, header is treated as a soap header or one of the below mentioned known headers.
The optional 'action' attribute specifies whether the mediator should
set or remove the header. If omitted, it defaults to 'set' action.

<header [name="qname"] (value="literal" | expression="xpath") [action="set"] [scope="soap | transport"]>
[<embeddedxml/>]
</header>

<header name="qname" action="remove" [scope="soap | transport"]/>

The value of the 'name' attribute must be one of the following aliases or
a valid QName with a namespace prefix. In the latter case the namespace prefix
must be mapped to a valid namespace URI using the standard 'xmlns' attribute.
When setting an embedded xml element as a soap header, 'name' attribute is not required.

- To
- From
- Action
- FaultTo
- ReplyTo
- RelatesTo

<a id="synapse-apache-org-userguide-mediators--MakeFault"></a>

### <a id="synapse-apache-org-userguide-mediators--MakeFault_Mediator"></a>MakeFault Mediator

MakeFault mediator transforms the current message into a fault message.
It should be noted that makeFault mediator does NOT send the message after
transforming it. A send mediator needs to be invoked separately to send
a fault message created by this mediator.

<makefault [version="soap11|soap12|pox"] [response="true|false"]>
<code (value="literal" | expression="xpath")/>
<reason (value="literal" | expression="xpath")/>
<node>...</node>?
<role>...</role>?
(<detail expression="xpath"/> | <detail>...</detail>)?
</makefault>

The To header of the fault message is set to the 'Fault-To' of the original message
if such a header exists on the original message. Depending on the 'version'
attribute, the fault message is created as a SOAP 1.1, SOAP 1.2
or POX fault. If the optional response attribute value is set as 'true',
makefault mediator marks the message as a response. Optional 'node',
'role' and 'detail' sub-elements in the mediator configuration can
be used to set the corresponding elements in the resulting SOAP fault.

<a id="synapse-apache-org-userguide-mediators--PayloadFactory"></a>

### <a id="synapse-apache-org-userguide-mediators--Payload_Factory_Mediator"></a>Payload Factory Mediator

Payload-factory mediator creates a new SOAP payload for the message, replacing
the existing one. printf() style formatting is used to configure the
transformation performed by this mediator.

<payloadFactory>
<format>"xmlstring"</format>
<args>
<arg (value="literal" | expression="xpath")/>\*
</args>
</payloadFactory>

'format' sub-element of the mediator configuration specifies the format of the
new payload. All $n occurrences in the format will be replaced by the value of
the n th argument at runtime. Each argument in the mediator configuration could
be a static value or an XPath expression. When an expression is used, value is
fetched at runtime by evaluating the provided XPath expression against the
existing SOAP message/message context.

<a id="synapse-apache-org-userguide-mediators--URLRewrite"></a>

### <a id="synapse-apache-org-userguide-mediators--URL_Rewrite_Mediator"></a>URL Rewrite Mediator

URL Rewrite mediator can be used to modify and transform the URL values
available in the message. By default, this mediator takes the 'To' header of the
message and apples the provided rewrite rules on it. Alternatively, one can
specify a property name in the 'inProperty' attribute, in which case the
mediator takes the value of the specified property as the input URL.

Similarly, the mediator by default sets the transformed URL as the 'To' header of
the message and alternatively you can use the 'outProperty' attribute to
instruct the mediator to set the resulting URL as a property.

<rewrite [inProperty="string"] [outProperty="string"]>
<rewriterule>
<condition>
...
</condition>?
<action [type="append|prepend|replace|remove|set"] [value="string"]
[xpath="xpath"] [fragment="protocol|host|port|path|query|ref|user|full"] [regex="regex"]>+
</rewriterule>+
</rewrite>

The mediator applies URL transformations by evaluating a set of rules on
the message. Rules are specified using the 'rewriterule' element. Rules are
evaluated in the order in which they are specified. A rule can consist of an
optional condition and one or more rewrite actions. If the condition is provided,
it is evaluated first and specified rewrite actions are executed only if the
condition evaluates to true. If no condition is specified, the provided rewrite
actions will be always executed. The condition should be wrapped in a 'condition'
element within the 'rewriterule' element. Rewrite actions are specified using
'action' elements.

<a id="synapse-apache-org-userguide-mediators--XQuery"></a>

### <a id="synapse-apache-org-userguide-mediators--XQuery_Mediator"></a>XQuery Mediator

The XQuery mediator can be used to perform an XQuery transformation. 'key'
attribute specifies the XQuery transformation, and the optional 'target'
attribute specifies the node of the message that should be transformed.
This defaults to the first child of the SOAP body of the payload. 'variable'
element defines a variable that could be bound to the dynamic context of the
XQuery engine in order to access those variables through the XQuery script.

<xquery key="string" [target="xpath"]>
<variable name="string" type="string" [key="string"] [expression="xpath"] [value="string"]/>?
</xquery>

It is possible to specify just a literal 'value', or an XPath expression
over the payload, or even specify a registry key or a registry key
combined with an XPath expression that selects the variable. The name of
the variable corresponds to the name of variable declaration in the XQuery
script. The 'type' of the variable must be a valid type defined by the
JSR-000225 (XQJ API).

The supported types are:

- XQItemType.XQBASETYPE\_INT -> INT
- XQItemType.XQBASETYPE\_INTEGER -> INTEGER
- XQItemType.XQBASETYPE\_BOOLEAN -> BOOLEAN
- XQItemType.XQBASETYPE\_BYTE - > BYTE
- XQItemType.XQBASETYPE\_DOUBLE -> DOUBLE
- XQItemType.XQBASETYPE\_SHORT -> SHORT
- XQItemType.XQBASETYPE\_LONG -> LONG
- XQItemType.XQBASETYPE\_FLOAT -> FLOAT
- XQItemType.XQBASETYPE\_STRING -> STRING
- XQItemType.XQITEMKIND\_DOCUMENT -> DOCUMENT
- XQItemType.XQITEMKIND\_DOCUMENT\_ELEMENT -> DOCUMENT\_ELEMENT
- XQItemType.XQITEMKIND\_ELEMENT -> ELEMENT

<a id="synapse-apache-org-userguide-mediators--XSLT"></a>

### <a id="synapse-apache-org-userguide-mediators--XSLT_Mediator"></a>XSLT Mediator

XSLT mediator applies the specified XSLT transformation to the selected
element of the current message payload. 'source' attribute selects the source
element to apply the transformation on. Where not specified, it defaults to the
first child of the SOAP body. Output of the transformation replaces the source
element when 'target' attribute is not specified. Otherwise, the output is
stored in the property specified by the 'target' attribute.

<xslt key="string" [source="xpath"] [target="string"]>
<property name="string" (value="literal" | expression="xpath")/>\*
<feature name="string" value="true | false" />\*
<attribute name="string" value="string" />\*
<resource location="..." key="..."/>\*
</xslt>

If the output method specified by the stylesheet is text (i.e. the stylesheet
has the <xsl:output method="text"/> directive),
then the output of the transformation is wrapped in an element with name
{http://ws.apache.org/commons/ns/payload}text. Note that when an
element with this name is present as the first child of the SOAP body of an
outgoing message, JMS and VFS transports automatically unwrap the
content and send it out as plain text. XSLT mediator can therefore be used for
integration with systems relying on plain text messages.

Usage of sub-elements of XSLT mediator configuration is as follows:

- property - Stylesheet parameters can be passed into the transformations
  using 'property' elements.
- feature - Defines any features which should be explicitly set to the
  TransformerFactory. For example,
  'http://ws.apache.org/ns/synapse/transform/feature/dom' feature
  enables DOM based transformations instead of serializing elements into byte
  streams and/or temporary files. Although enabling this feature could improve
  performance of the transformation, it might not work for all transformations.
- attribute - Defines attributes which should be explicitly set on the
  TransformerFactory.
- resource - Can be used to resolve XSLT imports and includes from the
  repository. It works in exactly the same way as the corresponding element in
  a <proxy> definition.

<a id="synapse-apache-org-userguide-mediators--ExtensionMediators"></a>

## <a id="synapse-apache-org-userguide-mediators--Extension_Mediators"></a>Extension Mediators

<a id="synapse-apache-org-userguide-mediators--Clazz"></a>

### <a id="synapse-apache-org-userguide-mediators--Class_Mediator"></a>Class Mediator

The class mediator makes it possible to use a custom class as a mediator. The
class must implement the org.apache.synapse.api.Mediator interface. If any properties are
specified, the corresponding setter methods are invoked on the class,
once, during initialization.

<class name="class-name">
<property name="string" value="literal">
(either literal or XML child)
</property>
</class>

This mediator creates an instance of a specified class and sets it as a
mediator. If any properties are specified, the corresponding setter methods are
invoked on the class with the given values, once, during initialization.

<a id="synapse-apache-org-userguide-mediators--POJOCommand"></a>

### <a id="synapse-apache-org-userguide-mediators--POJO_Command_Mediator"></a>POJO Command Mediator

POJO Command mediator implements the popular Command design pattern and can be
used to invoke an object which encapsulates a method call.

<pojoCommand name="class-name">
(
<property name="string" value="string"/> |
<property name="string" context-name="literal" [action=(ReadContext | UpdateContext | ReadAndUpdateContext)]>
(either literal or XML child)
</property> |
<property name="string" expression="xpath" [action=(ReadMessage | UpdateMessage | ReadAndUpdateMessage)]/>
)\*
</pojoCommand>

POJO Command mediator creates an instance of the specified command class,
which may implement the org.apache.synapse.Command interface or should have a
method with "public void execute()" signature. If any properties are specified,
the corresponding setter methods are invoked on the class before each message is
executed. It should be noted that a new instance of the POJO Command class is
created to process each message processed. After execution of the POJO Command
mediator, depending on the 'action' attribute of the property, the new value
returned by a call to the corresponding getter method is stored back to the
message or to the context. The 'action' attribute may specify whether this
behaviour is expected or not via the Read, Update and ReadAndUpdate values.

<a id="synapse-apache-org-userguide-mediators--Script"></a>

### <a id="synapse-apache-org-userguide-mediators--Script_Mediator"></a>Script Mediator

Synapse supports mediators implemented in a variety of scripting languages such
as JavaScript, Python and Ruby. There are two ways of defining a script mediator,
either with the script program statements stored in a separate file which is
referenced via the local or remote registry entry, or with the script program
statements embedded in-line within the Synapse configuration. A script mediator
using a script off the registry (local or remote) is defined as follows:

<script key="string" language="string" [function="script-function-name"]/>

The property key is the registry key to load the script. The language
attribute specifies the scripting language of the script code (e.g. "js"
for Javascript, "rb" for ruby, "groovy" for Groovy, "py" for Python..).
The function is an optional attribute defining the name of the script
function to invoke, if not specified it defaults to a function named
'mediate'. The function is passed a single parameter - which is the
Synapse MessageContext. The function may return a boolean, if it does not,
then true is assumed, and the script mediator returns this value. An
inline script mediator has the script source embedded in the configuration
as follows:

<script language="string">...script source code...<script/>

If the specified script calls a function defined in another script, then the
latter script should also be included in the script mediator configuration.
It's done using the 'include' sub-element of the mediator configuration. The key
attribute of the 'include' element should point to the script which has to be
included. The included script could be stored as a local entry or in the remote
registry. Script includes are defined as follows:

<script key="string" language="string" [function="script-function-name"]>
<include key="string"/>
</script>

The execution context environment of the script has access to the Synapse
MessageContext predefined in a script variable named 'mc'. An example of
an inline mediator using JavaScript/E4X which returns false if the SOAP
message body contains an element named 'symbol' which has a value of 'IBM'
would be:

<script language="js">mc.getPayloadXML()..symbol != "IBM";<script/>

Synapse uses the Apache
[Bean Scripting Framework](http://jakarta.apache.org/bsf/)
for the scripting language support, any script language supported by BSF may be
used to implement a Synapse mediator.

Implementing a mediator with a script language can have advantages over
using the built in Synapse mediator types or implementing a custom Java
class mediator. Script mediators have all the flexibility of a class
mediator with access to the Synapse MessageContext and SynapseEnvironment
APIs, and the ease of use and dynamic nature of scripting languages allows
rapid development and prototyping of custom mediators. An additional
benefit of some scripting languages is that they have very simple and
elegant XML manipulation capabilities, for example JavaScript E4X or Ruby
REXML, so this makes them well suited for use in the Synapse mediation
environment. For both types of script mediator definition, the
MessageContext passed into the script has additional methods over the
standard Synapse MessageContext to enable working with the XML in a way
natural to the scripting language. For example when using JavaScript
getPayloadXML and setPayloadXML, E4X XML objects, and when using Ruby,
REXML documents.

The complete list of available methods can be found in the
[
ScriptMessageContext Javadoc](../apidocs/org/apache/synapse/mediators/bsf/ScriptMessageContext.html).

<a id="synapse-apache-org-userguide-mediators--Spring"></a>

### <a id="synapse-apache-org-userguide-mediators--Spring_Mediator"></a>Spring Mediator

The Spring mediator exposes a spring bean as a mediator. In other terms, it
creates an instance of a mediator, which is managed by Spring. This Spring bean
must implement org.apache.synapse.api.Mediator interface.

<spring:spring bean="string" key="string" xmlns:spring="http://ws.apache.org/ns/synapse/spring"/>

'key' attribute refers to the Spring ApplicationContext/Configuration
(i.e. spring configuration XML) used for the bean. This key can be a registry
key or local entry key. The bean attribute is used for looking up a Spring bean
from the spring Application Context. Therefore, a bean with same name must be in
the given spring configuration. In addition to that, that bean must implement
the Mediator interface.

<a id="synapse-apache-org-userguide-mediators--AdvancedMediators"></a>

## <a id="synapse-apache-org-userguide-mediators--Advanced_Mediators"></a>Advanced Mediators

<a id="synapse-apache-org-userguide-mediators--Aggregate"></a>

### <a id="synapse-apache-org-userguide-mediators--Aggregate_Mediator"></a>Aggregate Mediator

Aggregate mediator implements the Message Aggregator EIP by aggregating the
messages or responses for split messages generated using either the clone or
iterate mediator.

<aggregate [id="string"]>
<correlateOn expression="xpath"/>?
<completeCondition [timeout="time-in-seconds"]>
<messageCount min="int-min" max="int-max"/>?
</completeCondition>?
<onComplete expression="xpath" [sequence="sequence-ref"]>
(mediator +)?
</onComplete>
</aggregate>

This mediator can also aggregate messages on the presence of matching elements
specified by the correlateOn XPath expression. Aggregate will collect the
messages coming into it until the messages collected on the aggregation
satisfies the complete condition. The completion condition can specify a minimum
or maximum number of messages to be collected, or a timeout value in seconds,
after which the aggregation terminates. On completion of the aggregation it will
merge all of the collected messages and invoke the onComplete sequence on it.
The merged message would be created using the XPath expression specified by the
attribute 'expression' on the 'onComplete' element.

<a id="synapse-apache-org-userguide-mediators--Cache"></a>

### <a id="synapse-apache-org-userguide-mediators--Cache_Mediator"></a>Cache Mediator

Cache mediator is used for simple response message caching in Synapse. When a
message reaches the cache mediator, it checks weather an equivalent message is
already cached using a hash value.

When the cache mediator detects that the message is a cached message, it fetches
the cached response and prepares Synapse for sending the response. If a sequence
is specified for a cache hit, user can send back the response message within
this sequence using a send mediator. If a sequence is not specified, then cached
response is sent back to the client.

<cache [id="string"] [hashGenerator="class"] [timeout="seconds"] [scope=(per-host | per-mediator)]
collector=(true | false) [maxMessageSize="in-bytes"]>
<onCacheHit [sequence="key"]>
(mediator)+
</onCacheHit>?
<implementation type=(memory | disk) maxSize="int"/>
</cache>

This mediator will evaluate the hash value of an incoming message as described
in the optional hash generator implementation (which should be a class
implementing the org.wso2.caching.digest.DigestGenerator interface). The default
hash generator is 'org.wso2.caching.digest.DOMHashGenerator'. If the generated
hash value has been found in the cache then the cache mediator will execute the
onCacheHit sequence which can be specified inline or referenced. The cache
mediator must be specified with an 'id' and two instances with this same 'id'
that correlates the response message into the cache for the request message
hash. The optional 'timeout' specifies the valid duration for cached elements,
and the scope defines if mediator instances share a common cache per every host
instance, or per every cache mediator pair (i.e. 'id') instance. 'collector'
attribute value 'true' specifies that the mediator instance is a response
collection instance, and 'false' specifies that its a cache serving instance.
The maximum size of a message to be cached could be specified with the optional
'maxMessageSize' attributes in bytes and defaults to unlimited. Finally,
'implementation' element may define if the cache is disk or memory based, and
'maxSize' attribute defines the maximum number of elements to be cached.

<a id="synapse-apache-org-userguide-mediators--Callout"></a>

### <a id="synapse-apache-org-userguide-mediators--Callout_Mediator"></a>Callout Mediator

Callout mediator performs a blocking external service invocation during mediation.
The target external service can be configured either using a child endpoint element
or using the 'serviceURL' attribute. When serviceURL is specified, it is used as
the EPR of the external service. We can specify the endpoint element if we want to
leverage endpoint functionality like format conversions, security, etc.
The target endpoint can be defined inline or we can refer to an existing named
endpoint in the configuration.
Only Leaf endpoint types (Address/WSDL/Default) are supported.
When both serviceURL and endpoint is not present, 'To' header on the request is
used as the target endpoint.

<callout [serviceURL="string"] [action="string"][passHeaders="true|false"] [initAxis2ClientOptions="true|false"] >
<configuration [axis2xml="string"] [repository="string"]/>?
<endpoint/>?
<source xpath="expression" | key="string">?
<target xpath="expression" | key="string"/>?
<enableSec policy="string" | outboundPolicy="String" | inboundPolicy="String" />?
</callout>

'action' attribute can be used to specify the SOAP Action of the external service
call. When 'initAxis2ClientOptions' is set to false, axis2 client options available
in the incoming message context is reused for the service invocation.
When 'passHeaders' is set to true, SOAP Headers of the received message is parsed
to the external service.

The source element specifies the payload for the request message using an XPath
expression or a registry key.
When source element is not defined, entire SOAP Envelope arrived at the Callout
mediator is treated as the source.
The target element specifies a node, at which the response payload will be
attached into the current message, or the name of a key/property using which the
response would be attached to the current message context as a property.
When target element is not specified, entire SOAP Envelope arrived to the
Callout mediator is replaced from the response received from the external service
invocation.

Since the callout mediator performs a blocking call, it cannot use the default
non-blocking http/s transports based on Java NIO, and thus defaults to
using the repository/conf/axis2\_blocking\_client.xml as the
Axis2 configuration, and repository/ as the client repository
unless these are specified inside the 'configuration' sub-element.

To invoke secured services, Callout mediator can be configured to enable WS-Security
using the 'enableSec' element. Security policy should be specified using the 'policy'
attribute which may point to a registry key or a local entry. You can also specify
two different policies for inbound and outbound messages (flows). This is done by
using the 'inboundPolicy' and 'outboundPolicy' attributes. These security
configurations will not get activated if we configure the external service using
the endpoint element. When endpoint is defined, security settings can be
configured at the endpoint.

<a id="synapse-apache-org-userguide-mediators--Clone"></a>

### <a id="synapse-apache-org-userguide-mediators--Clone_Mediator"></a>Clone Mediator

Clone mediator can be used to create several clones or copies of a message. This
mediator implements the Message Splitter EIP by splitting the message into
number of identical messages which will be processed in parallel. They can also
be set to process sequentially by setting the value of the optional 'sequential'
attribute to 'true'.

<clone [id="string"] [sequential=(true | false)] [continueParent=(true | false)]>
<target [to="uri"] [soapAction="qname"] [sequence="sequence\_ref"] [endpoint="endpoint\_ref"]>
<sequence>
(mediator)+
</sequence>?
<endpoint>
endpoint
</endpoint>?
</target>+
</clone>

The original message can be continued or dropped depending on the boolean value
of the optional 'continueParent' attribute. Optionally a custom 'To' address
and/or a 'Action' may be specified for cloned messages. The optional 'id'
attribute can be used to identify the clone mediator which created a particular
split message when nested clone mediators are used. This is particularly useful
when aggregating responses of messages that were created using nested clone
mediators.

<a id="synapse-apache-org-userguide-mediators--DBLookup"></a>

### <a id="synapse-apache-org-userguide-mediators--DBLookup"></a>DBLookup

DB Lookup mediator is capable of executing an arbitrary SQL SELECT statement,
and then set some resulting values as local message properties on the message
context. The DB connection used maybe looked up from an external DataSource or
specified in-line, in which case an Apache DBCP connection pool is established
and used.

<dblookup>
<connection>
<pool>
(
<driver/>
<url/>
<user/>
<password/>
<property name="name" value="value"/>\*
|
<dsName/>
<inClass/>
<url/>
<user/>
<password/>
)
</pool>
</connection>
<statement>
<sql>SELECT something FROM table WHERE something\_else = ?</sql>
<parameter [value="" | expression=""] type="CHAR|VARCHAR|LONGVARCHAR|NUMERIC|DECIMAL|BIT|TINYINT|SMALLINT|INTEGER|BIGINT|REAL|FLOAT|DOUBLE|DATE|TIME|TIMESTAMP"/>\*
<result name="string" column="int|string"/>\*
</statement>+
</dblookup>

For in-lined data sources the following parameters have to be specified.

- driver: Fully qualified class name of the database driver.
- url: Database URL.
- user: Username for database access.
- password: Password for database access.

This new data source is based on Apache DBCP connection pools. This connection
pool support the following configuration properties:

- autocommit = true | false
- isolation = Connection.TRANSACTION\_NONE | Connection.TRANSACTION\_READ\_COMMITTED | Connection.TRANSACTION\_READ\_UNCOMMITTED |
  Connection.TRANSACTION\_REPEATABLE\_READ | Connection.TRANSACTION\_SERIALIZABLE
- initialsize = int
- maxactive = int
- maxidle = int
- maxopenstatements = int
- maxwait = long
- minidle = int
- poolstatements = true | false
- testonborrow = true | false
- testonreturn = true | false
- testwhileidle = true | false
- validationquery = String

When an external data source is used the following parameters have to be
specified.

- dsName: The name of the data source to be looked up.
- icClass: Initial context factory class. The corresponding Java environment property is java.naming.factory.initial
- url: The naming service provider URL. The corresponding Java environment property is java.naming.provider.url
- user: Username corresponding to the Java environment property java.naming.security.principal
- password: Password corresponding to the Java environment property java.naming.security.credentials

More than one statement can be included in the mediator configuration. SQL
statement may specify parameters which could be specified as values or XPath
expressions. The type of a parameter could be any valid SQL type. 'result'
sub-element contains 'name' and 'column' attributes which define the name
under which the result is stored in the Synapse message context, and a column
number or name respectively.

<a id="synapse-apache-org-userguide-mediators--DBReport"></a>

### <a id="synapse-apache-org-userguide-mediators--DBReport"></a>DBReport

DB Report mediator is quite similar to the
[DB Lookup](#synapse-apache-org-userguide-mediators--DBReport)
mediator, but writes data into a database instead of reading data from a
database.

<dbreport useTransaction=(true|false)>
<connection>
<pool>
(
<driver/>
<url/>
<user/>
<password/>
<property name="name" value="value"/>\*
|
<dsName/>
<icClass/>
<url/>
<user/>
<password/>
)
</pool>
</connection>
<statement>
<sql>INSERT INTO table VALUES (?, ?, ?, ?)</sql>
<parameter [value="" | expression=""] type="CHAR|VARCHAR|LONGVARCHAR|NUMERIC|DECIMAL|BIT|TINYINT|SMALLINT|INTEGER|BIGINT|REAL|FLOAT|DOUBLE|DATE|TIME|TIMESTAMP"/>\*
</statement>+
</dblreport>

This mediator executes the specified SQL INSERT on the database specified
in-line or as an external data source. For information on configuring database
related mediators, refer[DB Lookup mediator guide](#synapse-apache-org-userguide-mediators--DBReport).

<a id="synapse-apache-org-userguide-mediators--Iterate"></a>

### <a id="synapse-apache-org-userguide-mediators--Iterate_Mediator"></a>Iterate Mediator

Iterate mediator splits the message into number of different messages
derived from the parent message by finding matching elements for the XPath
expression specified. New messages will be created for each matching element and
processed in parallel (default behavior) using either the specified sequence or
endpoint.

<iterate [id="string"] [continueParent=(true | false)] [preservePayload=(true | false)] [sequential=(true | false)]
(attachPath="xpath")? expression="xpath">
<target [to="uri"] [soapAction="qname"] [sequence="sequence\_ref"] [endpoint="endpoint\_ref"]>
<sequence>
(mediator)+
</sequence>?
<endpoint>
endpoint
</endpoint>?
</target>+
</iterate>

Created messages can also be set to process sequentially by setting the optional
'sequential' attribute to 'true'. Parent message can be continued or dropped in
the same way as in the clone mediator. The 'preservePayload' attribute specifies
if the original message should be used as a template when creating the split
messages, and defaults to 'false', in which case the split messages would
contain the split elements as the SOAP body. The optional 'id' attribute can be
used to identify the iterator which created a particular split message when
nested iterate mediators are used. This is particularly useful when aggregating
responses of messages that are created using nested iterate mediators.

<a id="synapse-apache-org-userguide-mediators--RMSequence"></a>

### <a id="synapse-apache-org-userguide-mediators--RMSequence"></a>RMSequence

RM Sequence mediator can be used to create a sequence of messages to communicate
via WS-Reliable Messaging with a WS-RM enabled endpoint.

<RMSequence (correlation="xpath" [last-message="xpath"]) | single="true" [version="1.0|1.1"]/>

The simplest use case of this mediator sets 'single' attribute to "true",
which means that only one message is involved in the same sequence. However, if
multiple messages should be sent in the same sequence, 'correlation' attribute
should be used with an XPath expression that selects a unique element value from
the incoming message. With the result of the XPath expression, Synapse can group
messages together that belong to the same sequence. To close the sequence
neatly, an XPath expression should be specified for the last message of the
sequence as well. The optional 'version' attribute, which specifies the WS-RM
specification version as 1.0 or 1.1, defaults to 1.0.

<a id="synapse-apache-org-userguide-mediators--Store"></a>

### <a id="synapse-apache-org-userguide-mediators--Store"></a>Store

Store mediator can be used to store the current message in a specific message
store.

<store messageStore="string" [sequence="sequence-ref"]>

In the mediator configuration 'messageStore' attribute is used to specify the
message store to store the message in. The optional 'sequence' attribute
specifies a sequence through which the message is sent before storing it.

<a id="synapse-apache-org-userguide-mediators--Throttle"></a>

### <a id="synapse-apache-org-userguide-mediators--Throttle_Mediator"></a>Throttle Mediator

Throttle mediator can be used for rate limiting as well as concurrency based
limiting. A WS-Policy dictates the throttling configuration and can be
specified inline or loaded from the registry. Please refer to the samples
document for sample throttling policies.

<throttle [onReject="string"] [onAccept="string"] id="string">
(<policy key="string"/> | <policy>..</policy>)
<onReject>..</onReject>?
<onAccept>..</onAccept>?
</throttle>

The throttle mediator could be used in the request path for rate limiting and
concurrent access limiting. When it's used for concurrent access limitation,
the same throttle mediator 'id' must be triggered on the response flow so that
completed responses are deducted from the available limit. (i.e. two instances
of the throttle mediator with the same 'id' attribute in the request and
response flows). 'onReject' and 'onAccept' sequence references or inline
sequences define how accepted and rejected messages are handled.

<a id="synapse-apache-org-userguide-mediators--Transaction"></a>

### <a id="synapse-apache-org-userguide-mediators--Transaction_Mediator"></a>Transaction Mediator

Transaction mediator can provide transaction facility for a set of mediators
defined as its child mediators. A transaction mediator with the action "new"
indicates the entry point for the transaction. A transaction is marked completed
by a transaction mediator with the action "commit". The suspend and resume
actions are used to pause a transaction at some point and start it again later.
Additionally, the transaction mediator supports three other actions, i.e.
use-existing-or-new, fault-if-no-tx, rollback.

<transaction action="new|use-existing-or-new|fault-if-no-tx|commit|rollback|suspend|resume"/>

- new: Initiate a new transaction.
- use-existing-or-new: If a transaction already exists
  continue it, otherwise create a new transaction.
- fault-if-no-tx: Go to the error handler if no transaction exists.
- commit: End the transaction.
- rollback: Rollback a transaction.
- suspend: Pause a transaction.
- resume: Resume a paused transaction.

---

<a id="synapse-apache-org-userguide-properties"></a>

# Apache Synapse – Apache Synapse - Properties Catalog

<a id="synapse-apache-org-userguide-properties--Introduction"></a>

## <a id="synapse-apache-org-userguide-properties--Properties_Catalog"></a>Properties Catalog

Properties provide the means of accessing various types of information
regarding a message that passes through the ESB. Furthermore, it is also
possible to use properties to control the behavior of the ESB on a given message flow.

<a id="synapse-apache-org-userguide-properties--Content"></a>

## <a id="synapse-apache-org-userguide-properties--Content"></a>Content

- [Introduction](#synapse-apache-org-userguide-properties--Introduction)
- [Contents](#synapse-apache-org-userguide-properties--Contents)
- [Generic Properties](#synapse-apache-org-userguide-properties--Generic_Properties)
  - [PRESERVE\_WS\_ADDRESSING](#synapse-apache-org-userguide-properties--PRESERVE_WS_ADDRESSING)
  - [RESPONSE](#synapse-apache-org-userguide-properties--RESPONSE)
  - [OUT\_ONLY](#synapse-apache-org-userguide-properties--OUT_ONLY)
  - [ERROR\_CODE](#synapse-apache-org-userguide-properties--ERROR_CODE)
  - [ERROR\_MESSAGE](#synapse-apache-org-userguide-properties--ERROR_MESSAGE)
  - [ERROR\_DETAIL](#synapse-apache-org-userguide-properties--ERROR_DETAIL)
  - [ERROR\_EXCEPTION](#synapse-apache-org-userguide-properties--ERROR_EXCEPTION)
  - [TRANSPORT\_HEADERS](#synapse-apache-org-userguide-properties--TRANSPORT_HEADERS)
  - [messageType](#synapse-apache-org-userguide-properties--messageType)
  - [ContentType](#synapse-apache-org-userguide-properties--ContentType)
  - [preserveProcessedHeaders](#synapse-apache-org-userguide-properties--preserveProcessedHeaders)
  - [SERVER\_IP](#synapse-apache-org-userguide-properties--SERVER_IP)
- [HTTP Transport Properties](#synapse-apache-org-userguide-properties--HTTP_Transport_Properties)
  - [POST\_TO\_URI](#synapse-apache-org-userguide-properties--POST_TO_URI)
  - [FORCE\_SC\_ACCEPTED](#synapse-apache-org-userguide-properties--FORCE_SC_ACCEPTED)
  - [DISABLE\_CHUNKING](#synapse-apache-org-userguide-properties--DISABLE_CHUNKING)
  - [NO\_ENTITY\_BODY](#synapse-apache-org-userguide-properties--NO_ENTITY_BODY)
  - [FORCE\_HTTP\_1.0](#synapse-apache-org-userguide-properties--FORCE_HTTP_1.0)
  - [HTTP\_SC](#synapse-apache-org-userguide-properties--HTTP_SC.0)
  - [FAULTS\_AS\_HTTP\_200](#synapse-apache-org-userguide-properties--FAULTS_AS_HTTP_200)
  - [NO\_KEEPALIVE](#synapse-apache-org-userguide-properties--NO_KEEPALIVE)
  - [REST\_URL\_POSTFIX](#synapse-apache-org-userguide-properties--REST_URL_POSTFIX)
  - [REQUEST\_HOST\_HEADER](#synapse-apache-org-userguide-properties--REQUEST_HOST_HEADER)
  - [FORCE\_POST\_PUT\_NOBODY](#synapse-apache-org-userguide-properties--FORCE_POST_PUT_NOBODY)
  - [FORCE\_HTTP\_CONTENT\_LENGTH](#synapse-apache-org-userguide-properties--FORCE_HTTP_CONTENT_LENGTH)
  - [COPY\_CONTENT\_LENGTH\_FROM\_INCOMING](#synapse-apache-org-userguide-properties--COPY_CONTENT_LENGTH_FROM_INCOMING)
  - [COPY\_CONTENT\_LENGTH\_FROM\_INCOMING](#synapse-apache-org-userguide-properties--COPY_CONTENT_LENGTH_FROM_INCOMING)
- [Synapse Message Context Properties](#synapse-apache-org-userguide-properties--Synapse_Message_Context_Properties)
  - [SYSTEM\_DATE](#synapse-apache-org-userguide-properties--SYSTEM_DATE)
  - [SYSTEM\_TIME](#synapse-apache-org-userguide-properties--SYSTEM_TIME)
  - [MESSAGE\_FORMAT](#synapse-apache-org-userguide-properties--MESSAGE_FORMAT)
  - [OperationName](#synapse-apache-org-userguide-properties--OperationName)

<a id="synapse-apache-org-userguide-properties--Generic_Properties"></a>

## <a id="synapse-apache-org-userguide-properties--Generic_Properties"></a>Generic Properties

Generic properties allow to configure or change the behavior of the message flow as they are processed by the ESB.

<a id="synapse-apache-org-userguide-properties--PRESERVE_WS_ADDRESSING"></a>

### <a id="synapse-apache-org-userguide-properties--PRESERVE_WS_ADDRESSING"></a>PRESERVE\_WS\_ADDRESSING

By default, the ESB adds a new set of WS-Addressing headers to the messages
forwarded from the ESB. If this property is set to "true" on a message,
the ESB will forward it without altering its existing WS-Addressing headers.

- Possible Values
  "true", "false"
- Default Behavior
  none
- Scope
  synapse
- Example

  <property name="PRESERVE\_WS\_ADDRESSING" value="true"/>

<a id="synapse-apache-org-userguide-properties--RESPONSE"></a>

### <a id="synapse-apache-org-userguide-properties--RESPONSE"></a>RESPONSE

Once this property is set to 'true' on a message, the ESB will
start treating it as a response message. It is generally used to
route a request message back to its source as the response.
However, currently respond mediator perform the same functionality.

- Possible Values
  "true", "false"
- Default Behavior
  none
- Scope
  synapse
- Example

  <property name="RESPONSE" value="true"/>

<a id="synapse-apache-org-userguide-properties--OUT_ONLY"></a>

### <a id="synapse-apache-org-userguide-properties--OUT_ONLY"></a>OUT\_ONLY

Set this property to "true" on a message to indicate that no response
message is expected for it once it is forwarded from the ESB. In other
words, the ESB will do an out-only invocation with such messages.
It is very important to set this property on messages that are involved
in out-only invocations to prevent the ESB from registering unnecessary
callbacks for response handling and eventually running out of memory.

- Possible Values
  "true", "false"
- Default Behavior
  none
- Scope
  synapse
- Example

  <property name="OUT\_ONLY" value="true"/>

<a id="synapse-apache-org-userguide-properties--ERROR_CODE"></a>

### <a id="synapse-apache-org-userguide-properties--ERROR_CODE"></a>ERROR\_CODE

Use this property to set a custom error code on a message which can be later
processed by a Synapse fault handler. If the Synapse encounters an error during
mediation or routing, this property will be automatically populated.

- Possible Values
  String
- Default Behavior
  none
- Scope
  synapse
- Example

  <property name="error-code" expression="get-property('ERROR\_CODE')"/>

<a id="synapse-apache-org-userguide-properties--ERROR_MESSAGE"></a>

### <a id="synapse-apache-org-userguide-properties--ERROR_MESSAGE"></a>ERROR\_MESSAGE

Use this property to set a custom error message on a message which can be
later processed by a Synapse fault handler. If the Synapse encounters an error
during mediation or routing, this property will be automatically populated.

- Possible Values
  String
- Default Behavior
  none
- Scope
  synapse
- Example

  <property name="Cause" expression="get-property('ERROR\_MESSAGE')"/>

<a id="synapse-apache-org-userguide-properties--ERROR_DETAIL"></a>

### <a id="synapse-apache-org-userguide-properties--ERROR_DETAIL"></a>ERROR\_DETAIL

Use this property to set the exception stacktrace in case of an error.
If the ESB encounters an error during mediation or routing, this property
will be automatically populated.

- Possible Values
  String
- Default Behavior
  none
- Scope
  synapse
- Example

  <property name="Trace" expression="get-property('ERROR\_DETAIL')"/>

<a id="synapse-apache-org-userguide-properties--ERROR_EXCEPTION"></a>

### <a id="synapse-apache-org-userguide-properties--ERROR_EXCEPTION"></a>ERROR\_EXCEPTION

Contains the actual exception thrown in case of a runtime error.

- Possible Values
  String
- Default Behavior
  none
- Scope
  synapse
- Example

  <property name="error-exception" expression="get-property('ERROR\_EXCEPTION')"/>

<a id="synapse-apache-org-userguide-properties--TRANSPORT_HEADERS"></a>

### <a id="synapse-apache-org-userguide-properties--TRANSPORT_HEADERS"></a>TRANSPORT\_HEADERS

Contains the map of transport headers. Automatically populated.
Individual values of this map can be accessed using the property
mediator in the transport scope.

- Possible Values
  java.util.Map
- Default Behavior
  Populated with the transport headers of the incoming request.
- Scope
  axis2
- Example

  <property name="TRANSPORT\_HEADERS" action="remove" scope="axis2"/>

<a id="synapse-apache-org-userguide-properties--messageType"></a>

### <a id="synapse-apache-org-userguide-properties--messageType"></a>messageType

Message formatter is selected based on this property.
This property should have the content type, such as text/xml,
application/xml, or application/json.

- Possible Values
  string
- Default Behavior
  Content type of incoming request.
- Scope
  axis2
- Example

  <property name="messageType" value="text/xml" scope="axis2"/>

<a id="synapse-apache-org-userguide-properties--ContentType"></a>

### <a id="synapse-apache-org-userguide-properties--ContentType"></a>ContentType

This property will be in effect only if the messageType property is set.
If the messageType is set, the value of Content-Type HTTP header of the
outgoing request will be chosen based on this property. Note that this property
is required to be set only if the message formatter seeks it in the
message formatter implementation.

- Possible Values
  string
- Default Behavior
  Value of the Content-type header of the incoming request.
- Scope
  axis2
- Example

  <property name="ContentType" value="text/xml" scope="axis2"/>

<a id="synapse-apache-org-userguide-properties--preserveProcessedHeaders"></a>

### <a id="synapse-apache-org-userguide-properties--preserveProcessedHeaders"></a>preserveProcessedHeaders

By default, Synapse removes the SOAP headers of incoming requests that have been processed.
If we set this property to 'true', Synapse preserves the SOAP headers.

- Possible Values
  "true", "false"
- Default Behavior
  Preserving SOAP headers
- Scope
  synapse
- Example

  <property name="preserveProcessedHeaders" value="true" scope="default"/>

<a id="synapse-apache-org-userguide-properties--SERVER_IP"></a>

### <a id="synapse-apache-org-userguide-properties--SERVER_IP"></a>SERVER\_IP

Server IP/Host name of hosted server

- Possible Values
  string
- Default Behavior
  Set automatically by the mediation engine upon startup with IP address or
  hostname of the ESB host
- Scope
  synapse
- Example

  <property name="StringServerIp" expression="get-property('SERVER\_IP')" scope="default" type="STRING"/>

<a id="synapse-apache-org-userguide-properties--HTTP_Transport_Properties"></a>

## <a id="synapse-apache-org-userguide-properties--HTTP_Transport_Properties"></a>HTTP Transport Properties

HTTP transport properties allow to control and configure how the HTTP transport processes the ongoing messages.

<a id="synapse-apache-org-userguide-properties--POST_TO_URI"></a>

### <a id="synapse-apache-org-userguide-properties--POST_TO_URI"></a>POST\_TO\_URI

This property makes the outgoing URL of the ESB a complete URL.
This is important when we talk through a Proxy Server.

- Possible Values
  "true", "false"
- Default Behavior
  false
- Scope
  axis2
- Example

  <property name="POST\_TO\_URI" scope="axis2" value="true"/>

<a id="synapse-apache-org-userguide-properties--FORCE_SC_ACCEPTED"></a>

### <a id="synapse-apache-org-userguide-properties--FORCE_SC_ACCEPTED"></a>FORCE\_SC\_ACCEPTED

When set to true, this property forces a 202 HTTP response to the client so that it stops waiting
for a response.

- Possible Values
  "true", "false"
- Default Behavior
  false
- Scope
  axis2
- Example

  <property name="FORCE\_SC\_ACCEPTED" scope="axis2" value="true"/>

<a id="synapse-apache-org-userguide-properties--DISABLE_CHUNKING"></a>

### <a id="synapse-apache-org-userguide-properties--DISABLE_CHUNKING"></a>DISABLE\_CHUNKING

Disables the HTTP chunking for outgoing messaging.

- Possible Values
  "true", "false"
- Default Behavior
  false
- Scope
  axis2
- Example

  <property name="DISABLE\_CHUNKING" scope="axis2" value="true"/>

<a id="synapse-apache-org-userguide-properties--NO_ENTITY_BODY"></a>

### <a id="synapse-apache-org-userguide-properties--NO_ENTITY_BODY"></a>NO\_ENTITY\_BODY

This property should be removed if a user want to generate a
response from the ESB to a request without an entity body, for example, GET request.

- Possible Values
  "true", "false"
- Default Behavior
  false
- Scope
  axis2
- Example

  <property name="NO\_ENTITY\_BODY" scope="axis2" value="true"/>

<a id="synapse-apache-org-userguide-properties--FORCE_HTTP_1.0"></a>

### <a id="synapse-apache-org-userguide-properties--FORCE_HTTP_1.0"></a>FORCE\_HTTP\_1.0

Force HTTP 1.0 for outgoing HTTP messages.

- Possible Values
  "true", "false"
- Default Behavior
  false
- Scope
  axis2
- Example

  <property name="FORCE\_HTTP\_1.0" scope="axis2" value="true"/>

<a id="synapse-apache-org-userguide-properties--HTTP_SC"></a>

### <a id="synapse-apache-org-userguide-properties--HTTP_SC"></a>HTTP\_SC

Set the HTTP status code.

- Possible Values
  HTTP status code number
- Default Behavior
  none
- Scope
  axis2
- Example

  <property name="HTTP\_SC" value="500" scope="axis2"/>

<a id="synapse-apache-org-userguide-properties--FAULTS_AS_HTTP_200"></a>

### <a id="synapse-apache-org-userguide-properties--FAULTS_AS_HTTP_200"></a>FAULTS\_AS\_HTTP\_200

Set the HTTP status code.

- Possible Values
  "true", "false"
- Default Behavior
  false
- Scope
  axis2
- Example

  <property name="FAULTS\_AS\_HTTP\_200" value="true" scope="axis2"/>

<a id="synapse-apache-org-userguide-properties--NO_KEEPALIVE"></a>

### <a id="synapse-apache-org-userguide-properties--NO_KEEPALIVE"></a>NO\_KEEPALIVE

Disables HTTP keep alive for corresponded connection flow. This Can be use in both inflow and outflow.

- Possible Values
  "true", "false"
- Default Behavior
  false
- Scope
  axis2
- Example

  <property name="NO\_KEEPALIVE" value="true" scope="axis2"/>

<a id="synapse-apache-org-userguide-properties--REST_URL_POSTFIX"></a>

### <a id="synapse-apache-org-userguide-properties--REST_URL_POSTFIX"></a>REST\_URL\_POSTFIX

The value of this property will be appended to the target URL when sending messages
out in a RESTful manner through an address endpoint. This is useful when you need to
append a context to the target URL in case of RESTful invocations. If you are using an
HTTP endpoint instead of an address endpoint, specify variables in the format of "uri.var.\*"
instead of using this property.

- Possible Values
  A URL fragment starting with "/"
- Default Behavior
  In the case of GET requests through an address endpoint, this contains the query string.
- Scope
  axis2
- Example

  <property name="REST\_URL\_POSTFIX" value="/context" scope="axis2"/>

<a id="synapse-apache-org-userguide-properties--REQUEST_HOST_HEADER"></a>

### <a id="synapse-apache-org-userguide-properties--REQUEST_HOST_HEADER"></a>REQUEST\_HOST\_HEADER

The value of this property will be set as the HTTP host header of outgoing request.

- Possible Values
  string
- Default Behavior
  ESB will set hostname of target endpoint and port as the HTTP host header
- Scope
  axis2
- Example

  <property name="REQUEST\_HOST\_HEADER" value="www.wso2.org" scope="axis2"/>

<a id="synapse-apache-org-userguide-properties--FORCE_HTTP_CONTENT_LENGTH"></a>

### <a id="synapse-apache-org-userguide-properties--FORCE_HTTP_CONTENT_LENGTH"></a>FORCE\_HTTP\_CONTENT\_LENGTH

This property allows the content length to be sent when the ESB sends a
request to a back end server. When HTTP 1.1 is used, this property disables
chunking and sends the content length. When HTTP 1.0 is used, the property
only sends the content length. This property should be set in scenarios where
the backend server is not able to accept chunked content.

- Possible Values
  "true", "false"
- Default Behavior
  false
- Scope
  axis2
- Example

  <property name="FORCE\_HTTP\_CONTENT\_LENGTH" scope="axis2" value="true"/>

<a id="synapse-apache-org-userguide-properties--COPY_CONTENT_LENGTH_FROM_INCOMING"></a>

### <a id="synapse-apache-org-userguide-properties--COPY_CONTENT_LENGTH_FROM_INCOMING"></a>COPY\_CONTENT\_LENGTH\_FROM\_INCOMING

This property allows the HTTP content length to be copied from an incoming message.
It is only valid when the FORCE\_HTTP\_CONTENT\_LENGTH property is used.
The COPY\_CONTENT\_LENGTH\_FROM\_INCOMING avoids buffering the message in memory for calculating
the content length, thus reducing the risk of performance degradation.

- Possible Values
  "true", "false"
- Default Behavior
  false
- Scope
  axis2
- Example

  <property name="COPY\_CONTENT\_LENGTH\_FROM\_INCOMING" scope="axis2" value="true"/>

<a id="synapse-apache-org-userguide-properties--Synapse_Message_Context_Properties"></a>

## <a id="synapse-apache-org-userguide-properties--Synapse_Message_Context_Properties"></a>Synapse Message Context Properties

Synapse Message Context Properties allow to retrieve the data related to synapse
mediation engine information for current message flow.

<a id="synapse-apache-org-userguide-properties--SYSTEM_DATE"></a>

### <a id="synapse-apache-org-userguide-properties--SYSTEM_DATE"></a>SYSTEM\_DATE

Returns the current date as a String. Optionally, a date format as per the standard
date format may be supplied. e.g. synapse:get-property("SYSTEM\_DATE", "yyyy.MM.dd G 'at' HH:mm:ss
z")
or get-property('SYSTEM\_DATE').

- Possible Values
  string
- Default Behavior
  none
- Scope
  synapse
- Example

  <property name="StringDateVal" expression="get-property('SYSTEM\_DATE')" scope="default" type="STRING"/>

<a id="synapse-apache-org-userguide-properties--SYSTEM_TIME"></a>

### <a id="synapse-apache-org-userguide-properties--SYSTEM_TIME"></a>SYSTEM\_TIME

Returns the current time in milliseconds.

- Possible Values
  string
- Default Behavior
  none
- Scope
  synapse
- Example

  <property name="StringTimeVal" expression="get-property('SYSTEM\_TIME')" scope="default" type="STRING"/>

<a id="synapse-apache-org-userguide-properties--MESSAGE_FORMAT"></a>

### <a id="synapse-apache-org-userguide-properties--MESSAGE_FORMAT"></a>MESSAGE\_FORMAT

Returns pox, soap11, soap12 depending on the message.
If a message type is unknown this returns soap12.

- Possible Values
  string
- Default Behavior
  none
- Scope
  synapse
- Example

  <property name="StringMessageFormat" expression="get-property('MESSAGE\_FORMAT')" scope="default" type="STRING"/>

<a id="synapse-apache-org-userguide-properties--OperationName"></a>

### <a id="synapse-apache-org-userguide-properties--OperationName"></a>OperationName

Returns the operation name corresponding to the message.

- Possible Values
  string
- Default Behavior
  none
- Scope
  synapse
- Example

  <property name="StringOperationName" expression="get-property('OperationName')" scope="default" type="STRING"/>

---

<a id="synapse-apache-org-userguide-quick_start"></a>

# Apache Synapse – Apache Synapse - Quick Start Guide

## <a id="synapse-apache-org-userguide-quick_start--Quick_Start_Guide"></a>Quick Start Guide

Welcome to Apache Synapse quick start guide. This tutorial demonstrates two
sample applications covering the fundamental usage scenarios of Synapse, namely
message mediation and service mediation. It starts from the absolute beginning and
walks you through a series of steps while giving a firm grasp on the Synapse
messaging model.

## <a id="synapse-apache-org-userguide-quick_start--Pre-requisites"></a>Pre-requisites

You should have following pre-requisites installed on your system to
follow this tutorial.

- A Java runtime - JDK or JRE of version 1.6.0\_23 or higher
- Apache Ant [http://ant.apache.org](http://ant.apache.org)

## <a id="synapse-apache-org-userguide-quick_start--Installing_Synapse"></a>Installing Synapse

Let's start by downloading Apache Synapse. Launch a web browser and navigate to
the [Synapse Downloads](../download.html) page. Download the binary distribution
of the latest release. Binary distributions are available in standard zip
format and Unix tar ball format.

Once downloaded you can install Synapse by simply extracting the archive to
a suitable location on your local disk. When extracted, a directory named
synapse with the corresponding version number will be created. This directory
houses all the libraries, configuration files, scripts and other artifacts
used by the Synapse runtime. From now on we will refer to this directory as
{SYNAPSE\_HOME}. So for an example {SYNAPSE\_HOME}/bin refers to the subdirectory
named 'bin' which is generally available in the Synapse installation.

## <a id="synapse-apache-org-userguide-quick_start--Running_the_Axis2_Server"></a>Running the Axis2 Server

Samples described in this tutorial involve routing messages to a Web Service
through the Synapse ESB. In real world applications, these Web Services could be
hosted in a web server in your organization, or practically anywhere in the
Internet. In this tutorial we will be using a sample Web Service that ships with
Synapse and we will deploy it in the sample Axis2 server that comes bundled with
Synapse.

To deploy the sample service in the Axis2 server, go to
{SYNAPSE\_HOME}/samples/axis2Server/src/SimpleStockQuoteService directory and run
'ant'. You will see an output similar to the following as the service is built
and deployed to the sample Axis2 server.

user@domain:/opt/synapse-3.0.1/samples/axis2Server/src/SimpleStockQuoteService$ ant
Buildfile: build.xml
clean:
init:
[mkdir] Created dir: /opt/synapse-3.0.1/samples/axis2Server/src/SimpleStockQuoteService/temp
[mkdir] Created dir: /opt/synapse-3.0.1/samples/axis2Server/src/SimpleStockQuoteService/temp/classes
[mkdir] Created dir: /opt/synapse-3.0.1/samples/axis2Server/repository/services
compile-all:
[javac] Compiling 9 source files to /opt/synapse-3.0.1/samples/axis2Server/src/SimpleStockQuoteService/temp/classes
build-service:
[mkdir] Created dir: /opt/synapse-3.0.1/samples/axis2Server/src/SimpleStockQuoteService/temp/SimpleStockQuote
[mkdir] Created dir: /opt/synapse-3.0.1/samples/axis2Server/src/SimpleStockQuoteService/temp/SimpleStockQuote/META-INF
[copy] Copying 1 file to /opt/synapse-3.0.1/samples/axis2Server/src/SimpleStockQuoteService/temp/SimpleStockQuote/META-INF
[copy] Copying 9 files to /opt/synapse-3.0.1/samples/axis2Server/src/SimpleStockQuoteService/temp/SimpleStockQuote
[jar] Building jar: /opt/synapse-3.0.1/samples/axis2Server/repository/services/SimpleStockQuoteService.aar
BUILD SUCCESSFUL
Total time: 1 second

Now go to {SYNAPSE\_HOME}/samples/axis2Server directory and start the sample server
by executing the following command.

Linux / Unix: . axis2server.sh  
Windows: axis2server.bat

This will start the Axis2 server on HTTP port 9000. You can see the WSDL of the
sample service by launching your web browser and navigating to the URL
http://localhost:9000/services/SimpleStockQuoteService?wsdl.

## <a id="synapse-apache-org-userguide-quick_start--Message_Mediation"></a>Message Mediation

Now we are all set to try our first scenario with Synapse. We will be starting
Synapse using the sample configuration found in synapse\_sample\_0.xml file which
resides in {SYNAPSE\_HOME}/repository/conf/sample directory. This configuration
enables Synapse to log all the messages passing through the service bus:

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<log level="full"/>
<send/>
</sequence>
</definitions>

To start the ESB with the above configuration go the {SYNAPSE\_HOME}/bin directory
and execute the following command.

Linux / Unix: . synapse.sh -sample 0  
Windows: synapse.bat -sample 0

Following messages will be displayed on the console as Synapse boots up with the
above configuration.

Starting Synapse/Java ...
Using SYNAPSE\_HOME: /opt/synapse-3.0.1
Using JAVA\_HOME: /opt/jdk1.7.0\_79
Using SYNAPSE\_XML: /opt/synapse-3.0.1/repository/conf/sample/synapse\_sample\_0.xml
2016-12-28 10:38:00,456 [-] [main] INFO SynapseServer Starting Apache Synapse...
2016-12-28 10:38:00,476 [-] [main] INFO SynapseControllerFactory Using Synapse home : /opt/synapse-3.0.1
2016-12-28 10:38:00,476 [-] [main] INFO SynapseControllerFactory Using Axis2 repository : /opt/synapse-3.0.1/repository
2016-12-28 10:38:00,476 [-] [main] INFO SynapseControllerFactory Using axis2.xml location : /opt/synapse-3.0.1/repository/conf/axis2.xml
2016-12-28 10:38:00,476 [-] [main] INFO SynapseControllerFactory Using synapse.xml location : /opt/synapse-3.0.1/repository/conf/sample/synapse\_sample\_0.xml
2016-12-28 10:38:00,476 [-] [main] INFO SynapseControllerFactory Using server name : localhost
2016-12-28 10:38:00,493 [-] [main] INFO SynapseControllerFactory The timeout handler will run every : 15s
2016-12-28 10:38:00,566 [-] [main] INFO Axis2SynapseController Initializing Synapse at : Wed Dec 28 10:38:00 IST 2016
2016-12-28 10:38:01,140 [-] [main] INFO PassThroughHttpSSLSender Loading Identity Keystore from : lib/identity.jks
2016-12-28 10:38:01,174 [-] [main] INFO PassThroughHttpSSLSender Loading Trust Keystore from : lib/trust.jks
2016-12-28 10:38:01,242 [-] [main] INFO PassThroughHttpSSLSender Pass-through HTTPS sender started...
2016-12-28 10:38:01,243 [-] [main] INFO PassThroughHttpSender Pass-through HTTP sender started...
2016-12-28 10:38:01,249 [-] [main] INFO JMSSender JMS Sender started
2016-12-28 10:38:01,250 [-] [main] INFO JMSSender JMS Transport Sender initialized...
2016-12-28 10:38:01,251 [-] [main] INFO VFSTransportSender VFS Sender started
2016-12-28 10:38:01,428 [-] [main] INFO PassThroughHttpSSLListener Loading Identity Keystore from : lib/identity.jks
2016-12-28 10:38:01,429 [-] [main] INFO PassThroughHttpSSLListener Loading Trust Keystore from : lib/trust.jks
2016-12-28 10:38:01,443 [-] [main] INFO Axis2SynapseController Loading mediator extensions...
2016-12-28 10:38:01,451 [-] [main] INFO XMLConfigurationBuilder Generating the Synapse configuration model by parsing the XML configuration
2016-12-28 10:38:01,506 [-] [main] INFO SynapseConfigurationBuilder Loaded Synapse configuration from : /opt/synapse-3.0.1/repository/conf/sample/synapse\_sample\_0.xml
2016-12-28 10:38:01,542 [-] [main] INFO Axis2SynapseController Deploying the Synapse service...
2016-12-28 10:38:01,563 [-] [main] INFO Axis2SynapseController Deploying Proxy services...
2016-12-28 10:38:01,563 [-] [main] INFO Axis2SynapseController Deploying EventSources...
2016-12-28 10:38:01,584 [-] [main] INFO PassThroughHttpSSLListener Starting pass-through HTTPS listener...
2016-12-28 10:38:01,601 [-] [main] INFO PassThroughHttpSSLListener Pass-through HTTPS listener started on port: 8243
2016-12-28 10:38:01,601 [-] [main] INFO PassThroughHttpListener Starting pass-through HTTP listener...
2016-12-28 10:38:01,603 [-] [main] INFO PassThroughHttpListener Pass-through HTTP listener started on port: 8280
2016-12-28 10:38:01,603 [-] [main] INFO Axis2SynapseController Management using JMX available via: service:jmx:rmi:///jndi/rmi://localhost:1099/synapse
2016-12-28 10:38:01,606 [-] [main] INFO TimeoutHandler This engine will expire all callbacks after : 180 seconds, irrespective of the timeout action, after the specified or optional timeout
2016-12-28 10:38:01,607 [-] [main] INFO ServerManager Server ready for processing...
2016-12-28 10:38:01,608 [-] [main] INFO SynapseServer Apache Synapse started successfully

Note that by default Synapse listens for HTTP requests on port 8280.

### <a id="synapse-apache-org-userguide-quick_start--Executing_the_Sample_Client"></a>Executing the Sample Client

Now we have a Web Service hosted in Axis2 and a Synapse ESB instance which
is configured to log and route messages. All that is left is to send some requests
to Synapse and see the magic happen. Synapse comes bundled with a sample
Web Service client that can be used to send different kinds of requests. Go to
{SYNAPSE\_HOME}/samples/axis2Client directory and execute the following command
to send a request to Synapse.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280 -Dmode=quote -Dsymbol=IBM

You should get the following output on the conosle.

Buildfile: build.xml
init:
[mkdir] Created dir: /opt/synapse-3.0.1/samples/axis2Client/target/classes
compile:
[javac] Compiling 22 source files to /opt/synapse-3.0.1/samples/axis2Client/target/classes
[javac] Note: /opt/synapse-3.0.1/samples/axis2Client/src/samples/userguide/PWCallback.java uses or overrides a deprecated API.
[javac] Note: Recompile with -Xlint:deprecation for details.
[javac] Note: /opt/synapse-3.0.1/samples/axis2Client/src/samples/userguide/LoadbalanceFailoverClient.java uses unchecked or unsafe operations.
[javac] Note: Recompile with -Xlint:unchecked for details.
stockquote:
[java] 2010-11-26 01:35:16,485 [-] [main] INFO MailTransportSender MAILTO Sender started
[java] 2010-11-26 01:35:16,496 [-] [main] INFO JMSSender JMS Sender started
[java] 2010-11-26 01:35:16,497 [-] [main] INFO JMSSender JMS Transport Sender initialized...
[java] Standard :: Stock price = $99.14593325984416
BUILD SUCCESSFUL
Total time: 5 seconds

This sends a stock quote request for the symbol 'IBM' with the transport URL set
to http://localhost:8280 (Synapse) and the WS-Addressing EPR set to
http://localhost:9000/services/SimpleStockQuoteService (Axis2). Synapse first
logs the message and then forwards it to the URL given in the WS-Addressing
headers. The actual message sent by the client is as follows.

POST / HTTP/1.1
Content-Type: text/xml; charset=UTF-8
SOAPAction: "urn:getQuote"
User-Agent: Axis2
Host: 127.0.0.1
Transfer-Encoding: chunked
218
<?xml version='1.0' encoding='UTF-8'?>
<soapenv:Envelope xmlns:wsa="http://www.w3.org/2005/08/addressing" xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Header>
<wsa:To>http://localhost:9000/services/SimpleStockQuoteService</wsa:To>
<wsa:MessageID>urn:uuid:D538B21E30B32BB8291177589283717</wsa:MessageID>
<wsa:Action>urn:getQuote</wsa:Action>
</soapenv:Header>
<soapenv:Body>
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>IBM</m0:symbol>
</m0:request>
</m0:getQuote>
</soapenv:Body>
</soapenv:Envelope>0

Now take a look at the console running Synapse. You will see that all the
details of the mediation are logged along with all the SOAP messages
passed through Synapse. If you execute Synapse in debug mode by editing
the lib/log4j.properties file and setting "log4j.category.org.apache.synapse"
as "DEBUG" instead of INFO, you will see even more information as follows after
a restart and on replay of the above scenario.

2012-09-18 09:46:57,909 [-] [HttpServerWorker-2] DEBUG SynapseMessageReceiver Synapse received a new message for message mediation...
2012-09-18 09:46:57,909 [-] [HttpServerWorker-2] DEBUG SynapseMessageReceiver Received To: http://localhost:9000/services/SimpleStockQuoteService
2012-09-18 09:46:57,909 [-] [HttpServerWorker-2] DEBUG SynapseMessageReceiver SOAPAction: urn:getQuote
2012-09-18 09:46:57,909 [-] [HttpServerWorker-2] DEBUG SynapseMessageReceiver WSA-Action: urn:getQuote
2012-09-18 09:46:57,909 [-] [HttpServerWorker-2] DEBUG Axis2SynapseEnvironment Injecting MessageContext
2012-09-18 09:46:57,909 [-] [HttpServerWorker-2] DEBUG Axis2SynapseEnvironment Using Main Sequence for injected message
2012-09-18 09:46:57,909 [-] [HttpServerWorker-2] DEBUG SequenceMediator Start : Sequence <main>
2012-09-18 09:46:57,909 [-] [HttpServerWorker-2] DEBUG SequenceMediator Sequence <SequenceMediator> :: mediate()
2012-09-18 09:46:57,909 [-] [HttpServerWorker-2] DEBUG LogMediator Start : Log mediator
2012-09-18 09:46:57,910 [-] [HttpServerWorker-2] INFO LogMediator To: http://localhost:9000/services/SimpleStockQuoteService, WSAction: urn:getQuote, SOAPAction: urn:getQuote, ReplyTo: http://www.w3.org/2005/08/addressing/anonymous, MessageID: urn:uuid:754cc296-ff58-4875-a999-3a33ec94c8a1, Direction: request, Envelope: <?xml version='1.0' encoding='utf-8'?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Header xmlns:wsa="http://www.w3.org/2005/08/addressing"><wsa:To>http://localhost:9000/services/SimpleStockQuoteService</wsa:To><wsa:MessageID>urn:uuid:754cc296-ff58-4875-a999-3a33ec94c8a1</wsa:MessageID><wsa:Action>urn:getQuote</wsa:Action></soapenv:Header><soapenv:Body><m0:getQuote xmlns:m0="http://services.samples"><m0:request><m0:symbol>IBM</m0:symbol></m0:request></m0:getQuote></soapenv:Body></soapenv:Envelope>
2012-09-18 09:46:57,910 [-] [HttpServerWorker-2] DEBUG LogMediator End : Log mediator
2012-09-18 09:46:57,910 [-] [HttpServerWorker-2] DEBUG SendMediator Start : Send mediator
2012-09-18 09:46:57,910 [-] [HttpServerWorker-2] DEBUG SendMediator Sending request message using implicit message properties..
Sending To: http://localhost:9000/services/SimpleStockQuoteService
SOAPAction: urn:getQuote
2012-09-18 09:46:57,910 [-] [HttpServerWorker-2] DEBUG Axis2FlexibleMEPClient Sending [add = false] [sec = false] [rm = false] [to=Address: http://localhost:9000/services/SimpleStockQuoteService]
2012-09-18 09:46:57,910 [-] [HttpServerWorker-2] DEBUG Axis2FlexibleMEPClient Message [Original Request Message ID : urn:uuid:754cc296-ff58-4875-a999-3a33ec94c8a1] [New Cloned Request Message ID : urn:uuid:835c68a7-0645-496d-9acc-1d84a03ccb09]
2012-09-18 09:46:57,911 [-] [HttpServerWorker-2] DEBUG SynapseCallbackReceiver Callback added. Total callbacks waiting for : 1
2012-09-18 09:46:57,912 [-] [HttpServerWorker-2] DEBUG SendMediator End : Send mediator
2012-09-18 09:46:57,912 [-] [HttpServerWorker-2] DEBUG SequenceMediator End : Sequence <main>
2012-09-18 09:46:58,035 [-] [HttpClientWorker-2] DEBUG SynapseCallbackReceiver Callback removed for request message id : urn:uuid:835c68a7-0645-496d-9acc-1d84a03ccb09. Pending callbacks count : 0
2012-09-18 09:46:58,035 [-] [HttpClientWorker-2] DEBUG SynapseCallbackReceiver Synapse received an asynchronous response message
2012-09-18 09:46:58,035 [-] [HttpClientWorker-2] DEBUG SynapseCallbackReceiver Received To: null
2012-09-18 09:46:58,035 [-] [HttpClientWorker-2] DEBUG SynapseCallbackReceiver SOAPAction:
2012-09-18 09:46:58,035 [-] [HttpClientWorker-2] DEBUG SynapseCallbackReceiver WSA-Action:
2012-09-18 09:46:58,036 [-] [HttpClientWorker-2] DEBUG SynapseCallbackReceiver Body :
<?xml version='1.0' encoding='utf-8'?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><ns:getQuoteResponse xmlns:ns="http://services.samples"><ns:return xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ns:GetQuoteResponse"><ns:change>4.158253518011668</ns:change><ns:earnings>13.000214652478554</ns:earnings><ns:high>176.07121446241788</ns:high><ns:last>171.44223855674258</ns:last><ns:lastTradeTimestamp>Tue Sep 18 09:46:57 CEST 2012</ns:lastTradeTimestamp><ns:low>-169.3791832231285</ns:low><ns:marketCap>3.844340450887613E7</ns:marketCap><ns:name>IBM Company</ns:name><ns:open>-167.9098655007073</ns:open><ns:peRatio>-17.815829214870217</ns:peRatio><ns:percentageChange>-2.4400099237243</ns:percentageChange><ns:prevClose>-170.41953303471544</ns:prevClose><ns:symbol>IBM</ns:symbol><ns:volume>16090</ns:volume></ns:return></ns:getQuoteResponse></soapenv:Body></soapenv:Envelope>
2012-09-18 09:46:58,036 [-] [HttpClientWorker-2] DEBUG Axis2SynapseEnvironment Injecting MessageContext
2012-09-18 09:46:58,036 [-] [HttpClientWorker-2] DEBUG Axis2SynapseEnvironment Using Main Sequence for injected message
2012-09-18 09:46:58,036 [-] [HttpClientWorker-2] DEBUG SequenceMediator Start : Sequence <main>
2012-09-18 09:46:58,036 [-] [HttpClientWorker-2] DEBUG SequenceMediator Sequence <SequenceMediator> :: mediate()
2012-09-18 09:46:58,036 [-] [HttpClientWorker-2] DEBUG LogMediator Start : Log mediator
2012-09-18 09:46:58,037 [-] [HttpClientWorker-2] INFO LogMediator To: http://www.w3.org/2005/08/addressing/anonymous, WSAction: , SOAPAction: , ReplyTo: http://www.w3.org/2005/08/addressing/anonymous, MessageID: urn:uuid:835c68a7-0645-496d-9acc-1d84a03ccb09, Direction: response, Envelope: <?xml version='1.0' encoding='utf-8'?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><ns:getQuoteResponse xmlns:ns="http://services.samples"><ns:return xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:type="ns:GetQuoteResponse"><ns:change>4.158253518011668</ns:change><ns:earnings>13.000214652478554</ns:earnings><ns:high>176.07121446241788</ns:high><ns:last>171.44223855674258</ns:last><ns:lastTradeTimestamp>Tue Sep 18 09:46:57 CEST 2012</ns:lastTradeTimestamp><ns:low>-169.3791832231285</ns:low><ns:marketCap>3.844340450887613E7</ns:marketCap><ns:name>IBM Company</ns:name><ns:open>-167.9098655007073</ns:open><ns:peRatio>-17.815829214870217</ns:peRatio><ns:percentageChange>-2.4400099237243</ns:percentageChange><ns:prevClose>-170.41953303471544</ns:prevClose><ns:symbol>IBM</ns:symbol><ns:volume>16090</ns:volume></ns:return></ns:getQuoteResponse></soapenv:Body></soapenv:Envelope>
2012-09-18 09:46:58,037 [-] [HttpClientWorker-2] DEBUG LogMediator End : Log mediator
2012-09-18 09:46:58,037 [-] [HttpClientWorker-2] DEBUG SendMediator Start : Send mediator
2012-09-18 09:46:58,037 [-] [HttpClientWorker-2] DEBUG SendMediator Sending response message using implicit message properties..
Sending To: http://www.w3.org/2005/08/addressing/anonymous
SOAPAction:
2012-09-18 09:46:58,038 [-] [HttpClientWorker-2] DEBUG SendMediator End : Send mediator
2012-09-18 09:46:58,038 [-] [HttpClientWorker-2] DEBUG SequenceMediator End : Sequence <main>

And with that you have successfully completed the first part of this guide. Now let's
look at the next scenario, service mediation with proxy services.

## <a id="synapse-apache-org-userguide-quick_start--Service_Mediation_Proxy_Services"></a>Service Mediation (Proxy Services)

As the name implies, a proxy service acts as an intermediary service hosted in
Synapse, and typically fronts an existing service endpoint. A proxy service can be
created and exposed on a different transport, schema, WSDL, or QoS setup (such
as WS-Security, WS-Reliable Messaging) than the real service. Proxy services
are capable of mediating requests before they are delivered to the actual
endpoint. Similarly responses from the actual service can be mediated before
they are sent back to the client.

Clients can send proxy service requests directly to Synapse. From the client's
perspective, proxy services are simply Web Services hosted on Synapse. They can
append the '?wsdl' suffix to the proxy service endpoints to get the WSDLs of these
virtual services. But in the Synapse configuration, service requests can be handled
in anyway you like. Most obvious thing would be to do some processing on the
message and send it to the actual service, which could be running on a different host.
But it is not necessary to always send the messages to an actual service. You may
list any combination of tasks to be performed on the messages received by
the proxy service and terminate the flow or send some response back to the
client even without sending it to any service.

Let's explore a simple proxy services scenario step-by-step to get a better feeling.
As you have downloaded and installed Synapse in the previous section, now you
just run the scenario straightaway. This scenario also requires the same stock
quote service we used in the previous example. So have it deployed in Axis2 and make
sure Axis2 server is up and running.

We are going to start Synapse with a configuration which contains a proxy service.
The configuration in synapse\_sample\_150.xml file in repository/conf/sample directory
matches well with the scope of this tutorial.

<definitions xmlns="http://synapse.apache.org/ns/2010/04/configuraiton">
<proxy name="StockQuoteProxy">
<target>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

The above configuration exposes a proxy service named StockQuoteProxy
and specifies an endpoint
(http://localhost:9000/services/SimpleStockQuoteService) as the target for the
proxy service. Therefore, messages coming to the proxy service will be
directed to the address http://localhost:9000/services/SimpleStockQuoteService
specified in the endpoint. There is also an out sequence for the proxy
service, which will be executed for response messages. In the out sequence,
we just send the messages back to the client. The publishWSDL tag
specifies an WSDL to be published for this proxy service. Let's start
Synapse with this sample configuration by running the below command from
the {SYNAPSE\_HOME}/bin directory.

Linux / Unix: . synapse.sh -sample 150  
Windows: synapse.bat -sample 150

Synapse will display a set of messages as it boots up just like in the previous
section describing the start-up procedure. Before running the client, it
is time to observe another feature of proxy services. That is displaying
the published WSDL. Just open a web browser and point it to the URL
http://localhost:8280/services/StockQuoteProxy?wsdl. You will see the
sample\_proxy\_1.wsdl specified in the configuration but containing the
correct EPRs for the service over HTTP/S.

### <a id="synapse-apache-org-userguide-quick_start--Executing_the_Sample_Client"></a>Executing the Sample Client

Now we can invoke the proxy service by sending a request from our sample Axis2
client. Go to the {SYNAPSE\_HOME}/samples/axis2Client directory and run the
following command.

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuoteProxy -Dmode=quote -Dsymbol=IBM

The above command sends a stock quote request directly to the provided
transport endpoint at http://localhost:8280/services/StockQuoteProxy. The
proxy service will forward the message to the Axis2 server and route the
response from Axis2 back to the client. You will see the response from the
server displayed on the console as follows:

Standard :: Stock price = $165.32687331383468

### <a id="synapse-apache-org-userguide-quick_start--More_on_Proxy_Services"></a>More on Proxy Services

Proxy services are among the most powerful functional components of Apache
Synapse. They can be used to perform transport switching, message format
switching and lot more. This quick start tutorial only covers the simple
usecases of proxy services. Please refer samples #150 and above in the
Synapse samples catalog, for in depth coverage on more advanced use cases.

## <a id="synapse-apache-org-userguide-quick_start--Conclusion"></a>Conclusion

This brings the Synapse quick start guide to an end. Now it is time to go
deeper and discover the advanced features of Synapse. You can browse through
the array of samples for your interested areas. If you have any issue regarding
Synapse as a user, feel free write to the Synapse user mailing list
([http://synapse.apache.org/mail-lists.html](http://synapse.apache.org/mail-lists.html)).

---

<a id="synapse-apache-org-userguide-samples"></a>

# Apache Synapse – Apache Synapse - Samples Catalog

## <a id="synapse-apache-org-userguide-samples--Apache_Synapse_Samples_Catalog"></a>Apache Synapse Samples Catalog

Apache Synapse comes preloaded with a horde of sample configurations that
demonstrate various features of the service bus. This catalog lists out all
these sample configurations and provides detailed information on how to run
them. These samples require an Apache ANT installation for you to be able to
try them out. If you are new to Synapse and have no experience running Synapse,
the Quick Start Guide may be a better starting point. If you are comfortable
with running Synapse samples, please go ahead and pick the samples you are
interested in.

### <a id="synapse-apache-org-userguide-samples--Message_Mediation"></a>Message Mediation

- [Sample 0: Introduction to Synapse](#synapse-apache-org-userguide-samples-sample0)
- [Sample 1: Simple content based routing (CBR) of messages](#synapse-apache-org-userguide-samples-sample1)
- [Sample 2: CBR with the Switch-case mediator, using message properties](#synapse-apache-org-userguide-samples-sample2)
- [Sample 3: Local Registry entry definitions, reusable endpoints and sequences](#synapse-apache-org-userguide-samples-sample3)
- [Sample 4: Introduction to error handling](#synapse-apache-org-userguide-samples-sample4)
- [Sample 5: Creating SOAP fault messages and changing the direction of a message](#synapse-apache-org-userguide-samples-sample5)
- [Sample 6: Manipulating SOAP headers, and filtering incoming and outgoing messages](#synapse-apache-org-userguide-samples-sample6)
- [Sample 7: Introduction to local registry entries and using schema validation](#synapse-apache-org-userguide-samples-sample7)
- [Sample 8: Introduction to static and dynamic registry resources, and using XSLT transformations](#synapse-apache-org-userguide-samples-sample8)
- [Sample 9: Introduction to dynamic sequences with registry](#synapse-apache-org-userguide-samples-sample9)
- [Sample 10: Introduction to dynamic endpoints with registry](#synapse-apache-org-userguide-samples-sample10)
- [Sample 11: A full registry based configuration, and sharing a configuration between multiple instances](#synapse-apache-org-userguide-samples-sample11)
- [Sample 12: One-way messaging / fire-and-forget through Synapse](#synapse-apache-org-userguide-samples-sample12)
- [Sample 14: Sequences and Endpoints as local registry items](#synapse-apache-org-userguide-samples-sample14)
- [Sample 15: Message Copying and Content Enriching with Enrich Mediator](#synapse-apache-org-userguide-samples-sample15)
- [Sample 16: Introduction to dynamic and static keys](#synapse-apache-org-userguide-samples-sample16)
- [Sample 17: Introduction to the payloadFactory mediator](#synapse-apache-org-userguide-samples-sample17)

### <a id="synapse-apache-org-userguide-samples--Endpoints"></a>Endpoints

- [Sample 50: POX to SOAP conversion](#synapse-apache-org-userguide-samples-sample50)
- [Sample 51: MTOM and SwA optimizations and request/response correlation](#synapse-apache-org-userguide-samples-sample51)
- [Sample 52: Session less load balancing between 3 endpoints](#synapse-apache-org-userguide-samples-sample52)
- [Sample 53: Fail-over routing among 3 endpoints](#synapse-apache-org-userguide-samples-sample53)
- [Sample 54: Session affinity load balancing between 3 endpoints](#synapse-apache-org-userguide-samples-sample54)
- [Sample 55: Session affinity load balancing between fail-over endpoints](#synapse-apache-org-userguide-samples-sample55)
- [Sample 56: WSDL endpoint](#synapse-apache-org-userguide-samples-sample56)
- [Sample 57: Dynamic load balancing between 3 nodes](#synapse-apache-org-userguide-samples-sample57)
- [Sample 58: Static load balancing between 3 nodes](#synapse-apache-org-userguide-samples-sample58)
- [Sample 59: Weighted Round-Robin loadbalancing between 3 endpoints](#synapse-apache-org-userguide-samples-sample59)
- [Sample 61: Routing message to 3 static recipients](#synapse-apache-org-userguide-samples-sample61)
- [Sample 62: Routing message to dynamic recipients](#synapse-apache-org-userguide-samples-sample62)

### <a id="synapse-apache-org-userguide-samples--QoS_AdditionRemoval_with_Message_Mediation"></a>QoS Addition/Removal with Message Mediation

- [Sample 100: Using WS-Security for outgoing messages](#synapse-apache-org-userguide-samples-sample100)

### <a id="synapse-apache-org-userguide-samples--Proxy_Services"></a>Proxy Services

- [Sample 150: Introduction to proxy services](#synapse-apache-org-userguide-samples-sample150)
- [Sample 151: Custom sequences and endpoints with proxy services](#synapse-apache-org-userguide-samples-sample151)
- [Sample 152: Switching transports and message format from SOAP to REST/POX](#synapse-apache-org-userguide-samples-sample152)
- [Sample 153: Routing the messages without processing the security headers](#synapse-apache-org-userguide-samples-sample153)
- [Sample 154: Load Balancing with proxy services](#synapse-apache-org-userguide-samples-sample154)
- [Sample 155: Dual channel invocation on client side and server side](#synapse-apache-org-userguide-samples-sample155)
- [Sample 156: Service integration with specifying the receiving sequence](#synapse-apache-org-userguide-samples-sample156)
- [Sample 157: Conditional router mediator for implementing complex routing scenarios](#synapse-apache-org-userguide-samples-sample157)
- [Sample 158: Exposing a SOAP service over JSON](#synapse-apache-org-userguide-samples-sample158)

### <a id="synapse-apache-org-userguide-samples--QoS_AdditionRemoval_with_Proxy_Services"></a>QoS Addition/Removal with Proxy Services

- [Sample 200: Engaging WS-Security on proxy services](#synapse-apache-org-userguide-samples-sample200)

### <a id="synapse-apache-org-userguide-samples--Transports"></a>Transports

- [Sample 250: Introduction to transport switching - JMS to HTTP/S](#synapse-apache-org-userguide-samples-sample250)
- [Sample 251: Switching from http/s to JMS](#synapse-apache-org-userguide-samples-sample251)
- [Sample 252: Pure text, binary and POX message support with JMS](#synapse-apache-org-userguide-samples-sample252)
- [Sample 253: One way bridging from JMS to http and replying with a 202 Accepted response](#synapse-apache-org-userguide-samples-sample253)
- [Sample 254: Using file system as the transport medium (reading/writing files)](#synapse-apache-org-userguide-samples-sample254)
- [Sample 255: Switching from file transport (ftp) to the mail transport](#synapse-apache-org-userguide-samples-sample255)
- [Sample 256: Proxy services with the mail transport](#synapse-apache-org-userguide-samples-sample256)
- [Sample 257: Proxy services with the FIX transport](#synapse-apache-org-userguide-samples-sample257)
- [Sample 258: Switching from HTTP to FIX ](#synapse-apache-org-userguide-samples-sample258)
- [Sample 259: Switching from FIX to HTTP ](#synapse-apache-org-userguide-samples-sample259)
- [Sample 260: Switching from FIX to AMQP ](#synapse-apache-org-userguide-samples-sample260)
- [Sample 261: Switch between different FIX versions ](#synapse-apache-org-userguide-samples-sample261)
- [Sample 262: Content Based Routing of FIX messages ](#synapse-apache-org-userguide-samples-sample262)
- [Sample 263: Transport switching - JMS to http/s using JBoss Messaging (JBM)](#synapse-apache-org-userguide-samples-sample263)
- [Sample 264: Request-response invocations with the JMS transport](#synapse-apache-org-userguide-samples-sample264)
- [Sample 265: Switching from TCP to HTTP/S](#synapse-apache-org-userguide-samples-sample265)
- [Sample 266: Switching from UDP to HTTP/S](#synapse-apache-org-userguide-samples-sample266)
- [Sample 269: AMQP transport-consumer proxy](#synapse-apache-org-userguide-samples-sample269)

### <a id="synapse-apache-org-userguide-samples--Scheduled_Tasks"></a>Scheduled Tasks

- [Sample 300: Introduction to tasks with simple trigger](#synapse-apache-org-userguide-samples-sample300)
- [Sample 301: Message Injector Task to invoke a named sequence](#synapse-apache-org-userguide-samples-sample301)
- [Sample 302: Message Injector Task to invoke a Proxy service](#synapse-apache-org-userguide-samples-sample302)

### <a id="synapse-apache-org-userguide-samples--Advanced_Mediators"></a>Advanced Mediators

#### <a id="synapse-apache-org-userguide-samples--Script_Mediator_Writing_Mediation_Logic_in_Scripting_Languages"></a>Script Mediator (Writing Mediation Logic in Scripting Languages)

- [Sample 350: Introduction to the script mediator using js scripts](#synapse-apache-org-userguide-samples-sample350)
- [Sample 351: Inline scripts with the script mediator](#synapse-apache-org-userguide-samples-sample351)
- [Sample 352: Accessing Synapse MessageContext API through scripts](#synapse-apache-org-userguide-samples-sample352)
- [Sample 353: Using Ruby scripts for mediation](#synapse-apache-org-userguide-samples-sample353)
- [Sample 354: Using In-lined Ruby scripts for mediation](#synapse-apache-org-userguide-samples-sample354)
- [Sample 355: Using Python scripts for mediation](#synapse-apache-org-userguide-samples-sample355)

#### <a id="synapse-apache-org-userguide-samples--Database_Mediators_Interacting_with_Databases"></a>Database Mediators (Interacting with Databases)

- [Sample 360: Introduction to dblookup mediator](#synapse-apache-org-userguide-samples-sample360)
- [Sample 361: Introduction to dbreport mediator](#synapse-apache-org-userguide-samples-sample361)
- [Sample 362: Perform database lookups and updates in the same mediation sequence](#synapse-apache-org-userguide-samples-sample362)
- [Sample 363: Reusable database connection pools](#synapse-apache-org-userguide-samples-sample363)
- [Sample 364: Executing database Stored Procedures](#synapse-apache-org-userguide-samples-sample364)

#### <a id="synapse-apache-org-userguide-samples--Throttle_Mediator"></a>Throttle Mediator

- [Sample 370: Introduction to throttle mediator and concurrency throttling](#synapse-apache-org-userguide-samples-sample370)
- [Sample 371: Restricting requests based on policies](#synapse-apache-org-userguide-samples-sample371)
- [Sample 372: Use of both concurrency throttling and request rate based throttling ](#synapse-apache-org-userguide-samples-sample372)

#### <a id="synapse-apache-org-userguide-samples--Class_Mediator_Writing_Mediation_Logic_in_Java"></a>Class Mediator (Writing Mediation Logic in Java)

- [Sample 380: Writing custom mediation logic in Java](#synapse-apache-org-userguide-samples-sample380)
- [Sample 381: Class mediator for CBR of binary messages](#synapse-apache-org-userguide-samples-sample381)

#### <a id="synapse-apache-org-userguide-samples--XQuery_Mediator"></a>XQuery Mediator

- [Sample 390: Introduction to the XQuery mediator](#synapse-apache-org-userguide-samples-sample390)
- [Sample 391: Using external XML documents in the XQuery mediator](#synapse-apache-org-userguide-samples-sample391)

#### <a id="synapse-apache-org-userguide-samples--Iterate_Mediator_and_Aggregate_Mediator"></a>Iterate Mediator and Aggregate Mediator

- [Sample 400: Message splitting and aggregation](#synapse-apache-org-userguide-samples-sample400)

#### <a id="synapse-apache-org-userguide-samples--Transaction_Mediator"></a>Transaction Mediator

- [Sample 410: Distributed transactions management with the transaction mediator](#synapse-apache-org-userguide-samples-sample410)

#### <a id="synapse-apache-org-userguide-samples--Cache_Mediator"></a>Cache Mediator

- [Sample 420: Simple response caching scenario](#synapse-apache-org-userguide-samples-sample420)

#### <a id="synapse-apache-org-userguide-samples--Callout_Mediator"></a>Callout Mediator

- [Sample 430: Callout mediator for synchronous web service invocations](#synapse-apache-org-userguide-samples-sample430)
- [Sample 431: Callout Mediator with WS-Security for Outgoing Messages](#synapse-apache-org-userguide-samples-sample431)
- [Sample 432: Callout Mediator - Invoke a secured service which has different policies for inbound and outbound flows](#synapse-apache-org-userguide-samples-sample432)
- [Sample 433: Callout Mediator - Invoke a service using a defined Endpoint](#synapse-apache-org-userguide-samples-sample433)
- [Sample 434: Callout Mediator - Invoke a service using an inline Endpoint](#synapse-apache-org-userguide-samples-sample434)

#### <a id="synapse-apache-org-userguide-samples--Respond_Mediator"></a>Respond Mediator

- [Sample 440: Respond Mediator - Echo Service with a Proxy service](#synapse-apache-org-userguide-samples-sample440)
- [Sample 441: Respond Mediator - Mock Service with a Proxy service](#synapse-apache-org-userguide-samples-sample441)

#### <a id="synapse-apache-org-userguide-samples--URL_Rewrite_Mediator"></a>URL Rewrite Mediator

- [Sample 450: Introduction to the URL Rewrite mediator](#synapse-apache-org-userguide-samples-sample450)
- [Sample 451: Conditional URL rewriting](#synapse-apache-org-userguide-samples-sample451)
- [Sample 452: Conditional URL rewriting with multiple rules](#synapse-apache-org-userguide-samples-sample452)

#### <a id="synapse-apache-org-userguide-samples--Spring_Mediator"></a>Spring Mediator

- [Sample 460: Introduction to the Spring mediator](#synapse-apache-org-userguide-samples-sample460)

#### <a id="synapse-apache-org-userguide-samples--EJB_Mediator"></a>EJB Mediator

- [Sample 470: Introduction to the EJB mediator I: Invoking Stateless Session Beans](#synapse-apache-org-userguide-samples-sample470)
- [Sample 471: Introduction to the EJB mediator II: Invoking Stateful Session Beans](#synapse-apache-org-userguide-samples-sample471)

### <a id="synapse-apache-org-userguide-samples--Eventing"></a>Eventing

- [Sample 500: Introduction to Eventing](#synapse-apache-org-userguide-samples-sample500)
- [Sample 501: Event source with static subscriptions](#synapse-apache-org-userguide-samples-sample501)
- [Sample 502: Transforming events before publish](#synapse-apache-org-userguide-samples-sample502)

### <a id="synapse-apache-org-userguide-samples--Synapse_Configuration_Model"></a>Synapse Configuration Model

- [Sample 600: File hierarchy based configuration builder](#synapse-apache-org-userguide-samples-sample600)
- [Sample 601: Using Synapse Observers](#synapse-apache-org-userguide-samples-sample601)

### <a id="synapse-apache-org-userguide-samples--Priority_Based_Mediation"></a>Priority Based Mediation

- [Sample 650: Introduction to priority based mediation](#synapse-apache-org-userguide-samples-sample650)
- [Sample 651: Priority based dispatching at transport level](#synapse-apache-org-userguide-samples-sample651)

### <a id="synapse-apache-org-userguide-samples--Message_Stores_and_Message_Processors"></a>Message Stores and Message Processors

- [Sample 700: Introduction to Synapse Message Stores](#synapse-apache-org-userguide-samples-sample700)
- [Sample 701: Introduction to Message Sampling Processor](#synapse-apache-org-userguide-samples-sample701)
- [Sample 702: Introduction to Message Forwarding Processor](#synapse-apache-org-userguide-samples-sample702)
- [Sample 703: Introduction to Message Resequencing Processor](#synapse-apache-org-userguide-samples-sample703)
- [Sample 704: Invoke Secured Services with Scheduled Message Forwarding Processor](#synapse-apache-org-userguide-samples-sample704)
- [Sample 705: Introduction to Message Forwarding Processor With Advance Parameters](#synapse-apache-org-userguide-samples-sample705)

### <a id="synapse-apache-org-userguide-samples--Templates"></a>Templates

- [Sample 750: Introduction to Synapse Templates](#synapse-apache-org-userguide-samples-sample750)

### <a id="synapse-apache-org-userguide-samples--REST_API"></a>REST API

- [Sample 800: Introduction to REST APIs](#synapse-apache-org-userguide-samples-sample800)

### <a id="synapse-apache-org-userguide-samples--Synapse_EIP_Library"></a>Synapse EIP Library

- [Sample 850: Introduction to Synapse Callout Block function template](#synapse-apache-org-userguide-samples-sample850)
- [Sample 851: Introduction to Synapse Splitter and Aggregator eip function templates](#synapse-apache-org-userguide-samples-sample851)
- [Sample 852: Introduction to Synapse Splitter-Agrregator combined function template](#synapse-apache-org-userguide-samples-sample852)
- [Sample 853: Introduction to Synapse Scatter-Gather eip function template](#synapse-apache-org-userguide-samples-sample853)
- [Sample 854: Introduction to Synapse Wire Tap eip function template](#synapse-apache-org-userguide-samples-sample854)
- [Sample 855: Introduction to Synapse Content Based Router eip function template](#synapse-apache-org-userguide-samples-sample855)
- [Sample 856: Introduction to Synapse Dynamic Router eip function template](#synapse-apache-org-userguide-samples-sample856)
- [Sample 857: Introduction to Synapse Recipient List eip function template](#synapse-apache-org-userguide-samples-sample857)

---

<a id="synapse-apache-org-userguide-samples-setup-index"></a>

# Apache Synapse – Apache Synapse - Samples Setup Guide

## <a id="synapse-apache-org-userguide-samples-setup-index--Introduction"></a>Introduction

Apache Synapse comes with a collection of working examples that demonstrates the
basic features of the Synapse ESB. In addition to the sample configurations, a set
of sample client applications and services are provided which can be used to try out
each of the examples. Most examples are self contained and can be run without any third
party applications or libraries. A set of Ant build files and scripts are provided
to make setting up the examples easier. A few examples however require deploying
certain external libraries and using third party client applications.

The main objectives of this article are:

- Introduce the concept of Synapse samples
- Describe how to setup the environment for running samples
- Describe how to run the sample client applications and services
- Describe how to deploy third party libraries when required

## <a id="synapse-apache-org-userguide-samples-setup-index--Prerequisites"></a>Prerequisites

Following applications are required to run any sample that comes with Synapse.
Please make sure you have them properly installed and configured in your system.

- A Java runtime - JDK or JRE of version 1.6.0\_23 or higher
- [Apache Ant](http://ant.apache.org) version 1.6.5 or higher
- A command line interface such as 'Command Prompt' on Windows and the Bash shell
  on Unix/Linux systems

When installing Java, make sure you setup the 'JAVA\_HOME' environment variable
properly. Also adding the JAVA\_HOME/bin directory to the system path will make
running the samples much easier.

In addition to the applications listed above, some samples require setting up few
other external resources such as JMS brokers and database engines. You can find the
relevant documentation under the '[Setting Up Additional Features](#synapse-apache-org-userguide-samples-setup-index--Setting_Up_Additional_Resources)'
section.

It is also advisable to run Synapse in the debug mode when trying out the example
configurations. This will give you important runtime status information that can be
used to better understand the functionality of Synapse. To enable the debug mode,
open up the lib/log4j.properties file and specify 'DEBUG' logging mode for the
'org.apache.synapse' package.

log4j.category.org.apache.synapse=DEBUG

## <a id="synapse-apache-org-userguide-samples-setup-index--Understanding_the_Samples"></a>Understanding the Samples

A Synapse sample scenario is generally comprised of three elements.

- Sample Synapse configuration (an XML configuration file given as the input
  of Synapse)
- Sample service (an Axis2 based Web Service to which Synapse will send messages)
- Sample client (an Axis2 based service client which is used to send requests to
  Synapse)

### <a id="synapse-apache-org-userguide-samples-setup-index--Sample_Synapse_Configurations"></a>Sample Synapse Configurations

All the sample Synapse configurations are housed under the repository/conf/sample
directory. These configuration files are named in the following format.

synapse\_sample\_n.xml

Here 'n' is a number which uniquely identifies the sample. This number can be passed
as an argument to the Synapse startup script in order to start Synapse with a particular
sample configuration. For an example to start Synapse with the configuration numbered
100 (ie synapse\_sample\_100.xml) run one of the following commands in the command line
interface.

Unix/Linux: sh synapse.sh -sample 100  
Windows: synapse.bat -sample 100

### <a id="synapse-apache-org-userguide-samples-setup-index--Sample_Services"></a>Sample Services

All the source of example services can be found in the samples/axis2Server/src directory.
You will find the source code for following services in this directory.

| Service | Description |
| --- | --- |
| SimpleStockQuoteService | This service has four operations; getQuote (in-out), getFullQuote(in-out),                             getMarketActivity(in-out) and placeOrder (in-only). The getQuote operation                             will generate a sample stock quote for a given symbol. The getFullQuote                             operation will generate a history of stock quotes for the symbol for a                             number of days, and the getMarketActivity operation returns stock quotes                             for a list of given symbols. The placeOrder operation will accept a one                             way message for an order. |
| SecureStockQuoteService | This service is a clone of the SimpleStockQuoteService, but has                             WS-Security enabled and an attached security policy for signing and                             encryption of messages. |
| ReliableStockQuoteService | This service is a clone of the SimpleStockQuoteService, but has                             WS-ReliableMessaging enabled. |
| MTOMSwASampleService | This service has three operations uploadFileUsingMTOM(in-out),                             uploadFileUsingSwA(in-out) and oneWayUploadUsingMTOM(in-only) and                             demonstrates the use of MTOM and SwA. The uploadFileUsingMTOM and                             uploadFileUsingSwA operations accept a binary image from the SOAP request                             as MTOM and SwA, and returns this image back again as the response, while                             the oneWayUploadUsingMTOM saves the request message to disk. |
| LoadbalanceFailoverService | A simple web service that can be used to test state less as well as                             session aware load balancing scenarios. |

You can compile and deploy any of these services into the provided sample Axis2
server by switching to the corresponding directory and invoking 'ant'. For an
example to setup the SimpleStockQuoteService, switch to the
samples/axis2Server/src/SimpleStockQuoteService directory and run the 'ant'
command. You will get an output similar to the following.

user@host:/tmp/synapse-1.1/samples/axis2Server/src/SimpleStockQuoteService$ ant
Buildfile: build.xml
...
build-service:
....
[jar] Building jar: /tmp/synapse-1.1/samples/axis2Server/repository/services/SimpleStockQuoteService.aar
BUILD SUCCESSFUL
Total time: 3 seconds

To start the Axis2 server, go to the samples/axis2Server directory and execute
the axis2server.sh or axis2server.bat script. This starts the Axis2 server with
the HTTP transport listener on port 9000 and HTTPS on port 9002 respectively.
For some samples it is required to enable additional transport listeners for the
sample Axis2 server. The resources listed under '[Setting Up Additional Features'](#synapse-apache-org-userguide-samples-setup-index--Setting_Up_Additional_Resources)
section provides more information on this.

### <a id="synapse-apache-org-userguide-samples-setup-index--Sample_Client_Applications"></a>Sample Client Applications

The client applications that come with Synapse are able to send SOAP, REST or
POX messages over transports like HTTP/S and JMS. They also support WS-Addressing,
WS-Security and WS-ReliableMessaging. Some sample clients can be used to send
pure binary or plain text messages. They are also capable of sending optimized
binary content using MTOM or SwA. Most sample scenarios involve invoking one
of these clients to send messages to Synapse. Synapse will then mediate those
requests and forward them to the sample services deployed on Axis2.

The sample clients can be executed from the samples/axis2Client directory
using the provided ant script. Simply executing 'ant' displays the available
clients and some of the options used to configure them. The sample clients
available are further described in the next section.

## <a id="synapse-apache-org-userguide-samples-setup-index--Sample_Axis2_Clients"></a>Sample Axis2 Clients

### <a id="synapse-apache-org-userguide-samples-setup-index--Stock_Quote_Client"></a>Stock Quote Client

This is a simple SOAP client that can send stock quote requests, receive
generated quotes and display the last sale price for a stock symbol.

ant stockquote [-Dsymbol=IBM|MSFT|SUN|..]
[-Dmode=quote | customquote | fullquote | placeorder | marketactivity]
[-Dsoapver=soap11 | soap12]
[-Daddurl=http://localhost:9000/services/SimpleStockQuoteService]
[-Dtrpurl=http://localhost:8280] [-Dprxurl=http://localhost:8280]
[-Dpolicy=../../repository/conf/sample/resources/policy/policy\_1.xml]

The client is able to operate in the following modes, and send the payloads
listed below as SOAP messages.

| Mode | Payload | Description |
| --- | --- | --- |
| quote | <m:getQuote xmlns:m="http://services.samples">   <m:request>     <m:symbol>IBM</m:symbol>   </m:request> </m:getQuote> | Sends a quote request for a single stock symbol. The response                             contains the last sales price for the stock which will be displayed on                             console. |
| customquote | <m0:checkPriceRequest xmlns:m0="http://www.apache-synapse.org/test">   <m0:Code>symbol</m0:Code> </m0:checkPriceRequest> | Sends a quote request in a custom format. Synapse will transform this                             custom request to the standard stock quote request format and send it to                             the Axis2 service. Upon receipt of the response, it will be transformed                             again to a custom response format and returned to the client, which will                             then display the last sales price. |
| fullquote | <m:getFullQuote xmlns:m="http://services.samples">   <m:request>     <m:symbol>IBM</m:symbol>   </m:request> </m:getFullQuote> | Gets quote reports for a stock symbol over a number of days (i.e. last 100                             days of the year). |
| placeorder | <m:placeOrder xmlns:m="http://services.samples">   <m:order>     <m:price>3.141593E0</m:price>     <m:quantity>4</m:quantity>     <m:symbol>IBM</m:symbol>   </m:order> </m:placeOrder> | Places an order for stocks using a one way request. |
| marketactivity | <m:getMarketActivity xmlns:m="http://services.samples">   <m:request>     <m:symbol>IBM</m:symbol>     ...     <m:symbol>MSFT</m:symbol>   </m:request> </m:getMarketActivity> | Gets a market activity report for the day (i.e. quotes for multiple                             symbols) |

To run the stock quote client in a particular mode, pass the name of the mode
as a system property as follows.

ant stockquote -Dmode=placeorder

Behavior of the sample Axis2 client can be further customized by using the 'addurl',
'trpurl' and 'prxurl' parameters. These parameters enable the following modes of
operation.

##### <a id="synapse-apache-org-userguide-samples-setup-index--Smart_Client_Mode"></a>Smart Client Mode

The 'addurl' property sets the WS-Addressing EPR, and the 'trpurl' sets a
transport URL for a message. Thus by specifying both of these properties,
the client can operate in the 'smart client' mode, where the addressing EPR can
specify the ultimate receiver, while the transport URL set to Synapse will ensure
that any necessary mediation takes place before the message is delivered to the
ultimate receiver.

ant stockquote -Daddurl=<addressingEPR> -Dtrpurl=<synapse>

##### <a id="synapse-apache-org-userguide-samples-setup-index--GatewayDumb_Client_Mode"></a>Gateway/Dumb Client Mode

By specifying only a transport URL, the client operates in the 'dumb client'
mode, where it sends the message to Synapse and depends on the rules configured
in Synapse for proper mediation and routing of the message to the ultimate
destination.

ant stockquote -Dtrpurl=<synapse>

##### <a id="synapse-apache-org-userguide-samples-setup-index--Proxy_Client_Mode"></a>Proxy Client Mode

In this mode, the client uses the 'prxurl' as a HTTP proxy to send the request.
Thus by setting the 'prxurl' to Synapse, the client can ensure that the message
will reach Synapse for mediation. The client can optionally set a WS-Addressing
EPR if required.

ant stockquote -Dprxurl=<synapse> [-Daddurl=<addressingEPR>]

### <a id="synapse-apache-org-userguide-samples-setup-index--Generic_JMS_Client"></a>Generic JMS Client

The JMS client is able to send plain text, plain binary content or POX content
by directly publishing a JMS message to the specified destination. The JMS
destination name should be specified with the 'jms\_dest' property. The 'jms\_type'
property can specify 'text', 'binary' or 'pox' to specify the type of message
payload.

The plain text payload for a 'text' message can be specified through the 'payload'
property. For binary messages, the 'payload' property will contain the path to
the binary file. For POX messages, the 'payload' property will hold a stock
symbol name to be used within the POX request for stock order placement requests.

ant jmsclient -Djms\_type=text -Djms\_dest=dynamicQueues/JMSTextProxy -Djms\_payload="24.34 100 IBM"
ant jmsclient -Djms\_type=pox -Djms\_dest=dynamicQueues/JMSPoxProxy -Djms\_payload=MSFT
ant jmsclient -Djms\_type=binary -Djms\_dest=dynamicQueues/JMSFileUploadProxy
-Djms\_payload=./../../repository/conf/sample/resources/mtom/asf-logo.gif

The JMS client assumes the existence of a default ActiveMQ (v4.1.0 or above)
installation on the local machine. Refer JMS setup guide for more details.

### <a id="synapse-apache-org-userguide-samples-setup-index--MTOMSwA_Client"></a>MTOM/SwA Client

The MTOM / SwA client is able to send a binary image file as a MTOM or SwA
optimized message, and receive the same file again through the response and save
it as a temporary file. The 'opt\_mode' can specify 'mtom' or 'swa' respectively
for the above mentioned optimizations. Optionally the path to a custom file can
be specified through the 'opt\_file' property, and the destination address can be
changed through the 'opt\_url' property if required.

ant optimizeclient -Dopt\_mode=[mtom | swa]

## <a id="synapse-apache-org-userguide-samples-setup-index--Setting_Up_Additional_Features"></a>Setting Up Additional Features

- [JMS Setup Guide](#synapse-apache-org-userguide-samples-setup-jms)
- [FIX Setup Guide](#synapse-apache-org-userguide-samples-setup-fix)
- [TCP/UDP Setup Guide](#synapse-apache-org-userguide-samples-setup-tcp_udp)
- [Database Setup Guide](#synapse-apache-org-userguide-samples-setup-db)
- [Script Setup Guide](#synapse-apache-org-userguide-samples-setup-script)
- [JSON Setup Guide](#synapse-apache-org-userguide-samples-setup-script--json-syn3)
- [E-Mail Setup Guide](#synapse-apache-org-userguide-samples-setup-mail)

---

<a id="synapse-apache-org-userguide-template_library"></a>

# Apache Synapse – Apache Synapse - Synapse Template Libraries

## <a id="synapse-apache-org-userguide-template_library--Synapse_Template_Libraries"></a>Synapse Template Libraries

Synapse template libraries are a mechanism to group synapse templates and automatically
expose it as a self contained set of function modules. It can be considered as a container
consisting a set of templates grouped in a particular order. Currently a synapse template
library is shipped as a ".zip" file and should be deployed inside
{$SYNAPSE\_HOME}/repository/conf/synapse-libs .
If a given template container is successfully deployed, all
templates within the library will be accessible to any Synapse user.

Following is a sample skeleton structure of a template library

|-- artifacts.xml
|-- com
| `-- synapse
| `-- sample
| `-- SynapseLibTestMediator.class
|-- lib
| `-- test-mediator-1.0.0.jar
|-- template\_dir-1
| |-- artifact.xml
| |-- templ1\_ns1.xml
| `-- templ2\_ns1.xml
`-- template\_dir-2
|-- artifact.xml
|-- templ1\_ns2.xml
`-- templ2\_ns2.xml

Following are these components at an overview.

- artifacts.xml
    
  This contains synapse library name , package name information and template groupings.

  <artifacts>
  <artifact name="synapse.lib.name" package="synapse.lib.package.name">
  <dependency artifact="template.group.name"/>\*
  <description>sample synapse library</description>?
  </artifact>
  </artifacts>
- artifact.xml
    
  This contains information about each individual template group.
    
  ie: - names of the templates in the group , corresponding configuration file ,etc.

  <artifact name="template.group.name" type="synapse/template">
  <subArtifacts>
  <artifact name="template.name">
  <file>template\_file.xml</file>
  <description>a sample synapse library function</description>?
  </artifact>\*
  </subArtifacts>
  </artifact>
- template\_file.xml
    
  This contains the implementation of each individual template configuration
- ./lib
    
  This is the directory to add any classes for class loading or can contain the .class files from
  the root
  level.

Also utilizing a synapse library is a three step process.

- Create and deploy the library
- Importing the library into synapse
  Users should deploy a import\_lib\_name.xml into
  {$SYNAPSE\_HOME}/repository/conf/synapse-config/imports

  <import xmlns="http://ws.apache.org/ns/synapse" name="SampleLibrary" package="synapse.lib"/>
- Execute functions of library using a template invoker. Target template is a
  combination of the package of synapse library and the target template name
  target == {synapse\_lib\_package}.{template name}

  <call-template target="synapse.lib.pkg.name.template\_name">
  <with-param name="..." value="..."/>\*
  </call-template>

## <a id="synapse-apache-org-userguide-template_library--Synapse_Enterprise_Integration_Patterns_library"></a>Synapse Enterprise Integration Patterns library

Synapse Enterprise Integration Patterns library is a container consisting a set of
templates grouped in a particular order by implementing commonly used
[Enterprise
Integration Patterns
](http://www.eaipatterns.com)
from the set of patterns introduced by Gregor Hohpe and Bobby Woolf.
Users can design their solutions using these well-known patterns and then simply configure
and use these same patterns in Apache Synapse by calling up the function as required.
So this will greatly reduce the effort required when building integrations.

Following lists all the built-in patterns of Synapse EIP library currently supports and
describes their usage, functionality and configuration syntax.
This excludes all generic EIP patterns supported by synapse out of the box.

- [Callout Block](#synapse-apache-org-userguide-template_library--CalloutBlock)
- [Splitter](#synapse-apache-org-userguide-template_library--Splitter)
- [Aggregator](#synapse-apache-org-userguide-template_library--Aggregator)
- [Splitter-Aggregator](#synapse-apache-org-userguide-template_library--SplitterAggregator)
- [Scatter-Gather](#synapse-apache-org-userguide-template_library--ScatterGather)
- [Wire Tap](#synapse-apache-org-userguide-template_library--WireTap)
- [Content-Based Router](#synapse-apache-org-userguide-template_library--ContentBasedRouter)
- [Dynamic Router](#synapse-apache-org-userguide-template_library--DynamicRouter)
- [Recipient List](#synapse-apache-org-userguide-template_library--RecipientList)

<a id="synapse-apache-org-userguide-template_library--CalloutBlock"></a>

### <a id="synapse-apache-org-userguide-template_library--Callout_Block"></a>Callout Block

This pattern is pretty much identical to the
[Routing slip pattern](http://www.eaipatterns.com/RoutingTable.html)
and this blocks
external service invocation during mediation. And useful in scenarios such as service chaining.
As default values are assigned to source and target xpaths,
one can simply utilize this pattern by just defining serviceURL.
  
  
Call template target -
**synapse.lang.eip.callout\_block**
  
Parameters
  
**service\_URL**
- URL of the service
  
**[action]**
- SOAP action(Optional)
  
**[source\_xpath | source\_key]**
- Payload of the message( either Xpath or key and Optional)
  
**[target\_xpath | target\_key]**
- A node to attach the response element ( either Xpath or key and optional)
  
  
Related Sample - [Sample 850](#synapse-apache-org-userguide-samples-sample850)

<a id="synapse-apache-org-userguide-template_library--Splitter"></a>

### <a id="synapse-apache-org-userguide-template_library--Splitter"></a>Splitter

The
[Splitter pattern](http://www.eaipatterns.com/Sequencer.html)
breaks out the composite message into a series of individual messages by
finding matching elements for the XPath expression specified and then redirected to the given
endpoint.
  
  
Call template target -
**synapse.lang.eip.splitter**
  
Parameters
  
**iterate\_exp**
- Xpath expression from which element you want to split the message.
  
**[attach\_path]**
- Xpath expression to specify which elements needs to be attached to form new messages.( Optional)
  
**endpoint\_url**
- Endpoint which newly created messages are redirected to.
  
**[attach\_path\_enabled]**
- Boolean value to enable attach path. Optional and by default this is false.
  
  
Related Sample - [Sample 851](#synapse-apache-org-userguide-samples-sample851)

<a id="synapse-apache-org-userguide-template_library--Aggregator"></a>

### <a id="synapse-apache-org-userguide-template_library--Aggregator"></a>Aggregator

The
[Aggregator pattern](http://www.eaipatterns.com/Aggregator.html)
builds a single message distilled from the individual messages.
And messages will be merged by using the XPath expression specified for aggregator\_exp.
  
  
Call template target -
**synapse.lang.eip.aggregator**
  
Parameters
  
**aggregator\_exp**
- An XPath expression specifying based on which elements to aggregate.
  
**[sequence\_ref]**
- target sequence which message should be mediated after aggregation. (Optional and if this is not
specified aggregator will send the aggregated message to the client).
  
**[oncomplete\_seq\_enabled]**
- Boolean value to enable target sequence. (Optional and by default this is false)
  
  
Related Sample - [Sample 851](#synapse-apache-org-userguide-samples-sample851)

<a id="synapse-apache-org-userguide-template_library--SplitterAggregator"></a>

### <a id="synapse-apache-org-userguide-template_library--Splitter-Aggregator"></a>Splitter-Aggregator

This pattern provides the combined functionality of Splitter and Aggregator patterns.
Which is when you specified following parameters this pattern will split the message and
does a synchronized call for the given endpoint and aggregates replies then send back to
client or mediates to the defined target sequence.
  
  
Call template target -
**synapse.lang.eip.splitter\_aggregator**
  
Parameters
  
**iterate\_exp**
- An Xpath expression from which element you want to split the message.
  
**[attach\_path]**
- An Xpath expression to specify which elements needs to be attached to form new messages.(
Optional)
  
**endpoint\_url**
- Endpoint which newly created messages are redirected to.
  
**[attach\_path\_enabled]**
- Boolean value to enable attach path. Optional and by default this is false.
  
**aggregator\_exp**
- An XPath expression specifying based on which elements to aggregate.
  
**[sequence\_ref]**
- Target sequence which message should be mediated after aggregation. (Optional and if this is not
specified aggregator will send the aggregated message to the client).
  
**[oncomplete\_seq\_enabled]**
- Boolean value to enable target sequence. (Optional and by default this is false)
  
  
Related Sample - [Sample 852](#synapse-apache-org-userguide-samples-sample852)

<a id="synapse-apache-org-userguide-template_library--ScatterGather"></a>

### <a id="synapse-apache-org-userguide-template_library--Scatter-Gather"></a>Scatter-Gather

The
[Scatter-Gather pattern](http://www.eaipatterns.com/BroadcastAggregate.html)
broadcasts a message to multiple recipients and re-aggregates the
responses back into a single message and send back to client or mediates to the defined
target sequence.
  
  
Call template target -
**synapse.lang.eip.scatter\_gather**
  
Parameters
  
**aggregator\_exp**
- An XPath expression specifying based on which elements to aggregate.
  
**[sequence\_ref]**
- Target sequence which message should be mediated after aggregation. (Optional and if this is not
specified aggregator will send the aggregated message to the client).
  
**[oncomplete\_seq\_enabled]**
- Boolean value to enable target sequence. (Optional and by default this is false)
  
**recipient\_list**
- Set of recipient endpoints , which should be specified as comma separated values
  
  
Related Sample - [Sample 853](#synapse-apache-org-userguide-samples-sample853)

<a id="synapse-apache-org-userguide-template_library--WireTap"></a>

### <a id="synapse-apache-org-userguide-template_library--Wire_Tap"></a>Wire Tap

[Wire Tap pattern](http://www.eaipatterns.com/WireTap.html)
enables route messages to a secondary channel while they are being forwarded to the main channel.
  
  
Call template target -
**synapse.lang.eip.wire\_tap**
  
Parameters
  
**destination\_uri**
- Endpoint of main channel
  
**wiretap\_uri**
- Endpoint of secondary channel
  
  
Related Sample - [Sample 854](#synapse-apache-org-userguide-samples-sample854)

<a id="synapse-apache-org-userguide-template_library--ContentBasedRouter"></a>

### <a id="synapse-apache-org-userguide-template_library--Content-Based_Router"></a>Content-Based Router

The
[Content Based Router pattern](http://www.eaipatterns.com/ContentBasedRouter.html)
route messages to the appropriate sequence, according to the message contents.
Routing decision is taken by matching given Xpath expression and Regular Expression.
User can define multiple matching elements as regular expressions and a target sequence where
if any of matching element evaluates to true then it mediates using the target sequence.
If none of the case statements are matching and default case is specified, it will be executed.
  
  
Call template target -
**synapse.lang.eip.content\_base\_router**
  
Parameters
  
**routing\_exp**
- Here you can specify the source xpath to be evaluated
  
**match\_content**
- This is a String which contains the matching conditions. Following is the syntax of it
**"IBM:cnd1\_seq,MSFT:cnd2\_seq;cnd3\_seq"**
  
User can define multiple matching conditions using "," splitter. regular expressions and
target sequence should be separated by inserting":". And finally default sequence needs to be
defined
after inserting ";". If there is no any target sequence
defined for a particular regular expression , default it will be mediated to the main sequence.
  
  
Related Sample - [Sample 855](#synapse-apache-org-userguide-samples-sample855)

<a id="synapse-apache-org-userguide-template_library--DynamicRouter"></a>

### <a id="synapse-apache-org-userguide-template_library--Dynamic_Router"></a>Dynamic Router

The
[Dynamic Router pattern](http://www.eaipatterns.com/DynamicRouter.html)
route a message consecutively through a series of condition steps,
which is parsed by the â€œconditionsâ€ parameter. The list of sequences through which the message
should pass is decided dynamically at run time.
It checks whether the route condition evaluates to true and mediates using the given sequence and
user can define
routing decision based on the message contents such as HTTP url,HTTP headers or combination of both.
  
  
Call template target -
**synapse.lang.eip.dynamic\_router**
  
Parameters
  
**conditions**
- This is a String which contains the routing rules. Following is the syntax of it.
  
**"header=foo:bar.\*{AND}url=/services/;seq=seq1,header=header1:bar.\*{OR}header=header1:foo.\*;seq=seq2,header=header2:foo.\*;seq=seq3"**
  
User can define multiple routing rules by using "," splitter. Routing rule contains following
format,
  
To match HTTP headers , use
**header=regEx:seqRef**
  
header source and regular expression should be separated by inserting ":"
  
To match HTTP url, use
**url=/url**
  
Then target sequence needs to be defined after inserting ";"
  
Also you can use**"{AND}"**as to specify logical AND ,
**"{OR}"**
as to specify logical OR to match
multiple headers and url in your expression.
  
  
Related Sample - [Sample 856](#synapse-apache-org-userguide-samples-sample856)

<a id="synapse-apache-org-userguide-template_library--RecipientList"></a>

### <a id="synapse-apache-org-userguide-template_library--Recipient_List"></a>Recipient List

The
[Recipient List pattern](http://www.eaipatterns.com/RecipientList.html)
forward the message to all channels associated with the defined set of recipients.
  
  
Call template target -
**synapse.lang.eip.recipient\_list**
  
Parameters
  
**recipient\_list**
- set of recipient endpoints , which should be specified as comma separated values
  
  
Related Sample - [Sample 857](#synapse-apache-org-userguide-samples-sample857)

---

<a id="synapse-apache-org-userguide-transports"></a>

# Apache Synapse – Apache Synapse - Transports Catalog

<a id="synapse-apache-org-userguide-transports--Introduction"></a>

## <a id="synapse-apache-org-userguide-transports--Transports_Catalog"></a>Transports Catalog

The Synapse project has developed a set of transport implementations that provide
protocol support and/or features that go beyond what is provided out of the box by
Axis2:

- A non-blocking HTTP transport that gives better performance in a highly
  asynchronous environment like Synapse.
- A VFS transport that can read messages from files and write outgoing messages to
  a file system. The file system can be local or remote, and several remote
  protocols are supported, such as FTP, SSH, WebDAV, etc.
- A transport supporting the
  [Financial Information eXchange](http://www.fixprotocol.org)
  protocol. FIX is a public-domain messaging standard developed specifically for
  the real-time electronic exchange of securities transactions. It has a large user
  base and is developed by the collaborative effort of banks, broker-dealers,
  exchanges, industry utilities and associations, institutional investors, and IT
  providers around the world.
- A AMQP transport which support the AMQP protocol.
  [AMQP](http://www.amqp.org/) is a messaging
  protocol.

Note that while these transports are developed as part of the Synapse project,
they can be used with any Axis2 based application.

The Synapse distribution also comes bundled with the following transports from
the [Axis2 Transport](http://axis.apache.org/axis2/java/transports/index.html)
project:

- A
  [JMS transport](http://axis.apache.org/axis2/java/transports/jms.html)
  supporting any JMS 1.0 or 1.1 provider.
- A
  [Mail transport](http://axis.apache.org/axis2/java/transports/mail.html)
  able to send messages using SMTP and poll messages from a POP3 or IMAP account.

  Apache Synapse is also compatible with the following transport implementations
  from the Apache Axis2 Transports project:
- [TCP transport](http://axis.apache.org/axis2/java/transports/tcp-transport.html)
- [SMS transport](http://axis.apache.org/axis2/java/transports/sms.html)
- UDP transport
- XMPP transport

  These transports are not shipped with Apache Synapse by default and hence they
  should be
  [downloaded separately](http://axis.apache.org/axis2/java/transports/download.cgi)
  from the Axis2 transport website and installed in the Synapse runtime.

<a id="synapse-apache-org-userguide-transports--Content"></a>

## <a id="synapse-apache-org-userguide-transports--Content"></a>Content

- [Introduction](#synapse-apache-org-userguide-transports--Introduction)
- [Contents](#synapse-apache-org-userguide-transports--Contents)
- [Passthrough HTTP transport](#synapse-apache-org-userguide-transports--Passthrough_HTTP_transport)
  - [Example configurations](#synapse-apache-org-userguide-transports--Example_configurations)
  - [Transport listener parameters](#synapse-apache-org-userguide-transports--Transport_listener_parameters)
  - [Transport sender parameters](#synapse-apache-org-userguide-transports--Transport_sender_parameters)
- [Non-blocking HTTP(NHTTP) transport](#synapse-apache-org-userguide-transports--Non-blocking_HTTPNHTTP_transport)
  - [Example configurations](#synapse-apache-org-userguide-transports--Example_configurations)
  - [Transport listener parameters](#synapse-apache-org-userguide-transports--Transport_listener_parameters)
  - [Transport sender parameters](#synapse-apache-org-userguide-transports--Transport_sender_parameters)
- [VFS transport](#synapse-apache-org-userguide-transports--VFS_transport)
  - [Transport listener](#synapse-apache-org-userguide-transports--Transport_listener)
  - [Transport sender](#synapse-apache-org-userguide-transports--Transport_sender)
  - [Using SFTP](#synapse-apache-org-userguide-transports--Using_SFTP)
  - [Known issues](#synapse-apache-org-userguide-transports--Known_issues)
- [FIX transport](#synapse-apache-org-userguide-transports--FIX_transport)
  - [Setting up the FIX Transport](#synapse-apache-org-userguide-transports--Setting_up_the_FIX_Transport)
  - [FIX Transport Parameters](#synapse-apache-org-userguide-transports--FIX_Transport_Parameters)
- [AMQP transport](#synapse-apache-org-userguide-transports--amqp_transport)
  - [Setting up the AMQP Transport](#synapse-apache-org-userguide-transports--setting_up_the_amqp_transport)
  - [AMQP Transport Parameters](#synapse-apache-org-userguide-transports--amqp_transport_parameters)
  - [Sample Configurations](#synapse-apache-org-userguide-transports--amqp_transport_ex)

<a id="synapse-apache-org-userguide-transports--Passthrough_HTTP_transport"></a>

## <a id="synapse-apache-org-userguide-transports--Passthrough_HTTP_transport"></a>Passthrough HTTP transport

<a id="synapse-apache-org-userguide-transports--Example_configurations"></a>

### <a id="synapse-apache-org-userguide-transports--Example_configuration"></a>Example configuration

<transportReceiver name="http" class="org.apache.synapse.transport.passthru.PassThroughHttpListener">
<parameter name="port">8280</parameter>
<parameter name="httpGetProcessor"locked="org.apache.synapse.transport.passthru.api.PassThroughNHttpGetProcessor">true</parameter>
</transportReceiver>
<transportReceiver name="https" class="org.apache.synapse.transport.passthru.PassThroughHttpSSLListener">
<parameter name="port" locked="false">8243</parameter>
<parameter name="httpGetProcessor"locked="org.apache.synapse.transport.passthru.api.PassThroughNHttpGetProcessor">true</parameter>
<parameter name="keystore" locked="false">
<KeyStore>
<Location>lib/identity.jks</Location>
<Type>JKS</Type>
<Password>password</Password>
<KeyPassword>password</KeyPassword>
</KeyStore>
</parameter>
<parameter name="truststore" locked="false">
<TrustStore>
<Location>lib/trust.jks</Location>
<Type>JKS</Type>
<Password>password</Password>
</TrustStore>
</parameter>
</transportReceiver>
<transportSender name="http" class="org.apache.synapse.transport.passthru.PassThroughHttpSender">
</transportSender>
<transportSender name="https" class="org.apache.synapse.transport.passthru.PassThroughHttpSSLSender">
<parameter name="keystore" locked="false">
<KeyStore>
<Location>lib/identity.jks</Location>
<Type>JKS</Type>
<Password>password</Password>
<KeyPassword>password</KeyPassword>
</KeyStore>
</parameter>
<parameter name="truststore" locked="false">
<TrustStore>
<Location>lib/trust.jks</Location>
<Type>JKS</Type>
<Password>password</Password>
</TrustStore>
</parameter>
<parameter name="CertificateRevocationVerifier">
<CacheSize>50</CacheSize>
<CacheDurationMins>5</CacheDurationMins>
</parameter>
</transportSender>

<a id="synapse-apache-org-userguide-transports--Transport_listener_parameters"></a>

### <a id="synapse-apache-org-userguide-transports--Transport_listener_parameters"></a>Transport listener parameters

The following parameters are supported by both the HTTP and the HTTPS listener:

- port
  The TCP port to bind the listener to.
- bind-address
  The IP address to bind the listener to. This can be used on hosts that have
  more than one network interface or IP address to run multiple Synapse instances
  listening to the same port. If this parameter is not specified, the
  listener will accept connections on any IP address.
- hostname
  The host name to use when computing endpoint references in generated WSDL files.
  The default value is the host name as provided by the operation system or
  localhost if the host name can't be determined. The value of this
  parameter is ignored if WSDLEPRPrefix is specified.
- WSDLEPRPrefix
  The URL prefix to use when computing endpoint references in generated WSDL files.
  The value must be a valid URL with at least a protocol and host. If this value
  is unspecified, endpoint references will be computed based on the listener type
  (HTTP or HTTPS) and hostname and port parameters.

  This parameter should be used if clients connect to Synapse through a frontend
  server, e.g. a (load balancing) Apache, and these clients rely on the address
  information in the WSDL documents exposed through ...?wsdl URLs.

The following parameters are specific to the HTTPS listener:

- keystore
  The keystore configuration. The value of this parameter must be a
  <KeyStore>
  element as shown in the example configurations above.
- truststore
  The truststore configuration. The value of this parameter must be a
  <TrustStore>
  element as shown in the example configurations above.
- SSLVerifyClient
  This parameter has the same meaning as the corresponding
  [mod\_ssl directive](http://httpd.apache.org/docs/2.2/mod/mod_ssl.html#sslverifyclient)
  and sets the desired certificate verification level for client authentication:
  - none (default): no client certificate is required at all
  - optional: the client may present a valid certificate, but is
    not required to do so
  - require: the client has to present a valid certificate,
    otherwise the connection request will be terminated during SSL handshake

<a id="synapse-apache-org-userguide-transports--Transport_sender_parameters"></a>

### <a id="synapse-apache-org-userguide-transports--Transport_sender_parameters"></a>Transport sender parameters

The following property can be used to control based on content-types whether the
HTTP/HTTPS sender shall output a warning for responses with HTTP status code 500.

- warnOnHTTP500
  A list of content-types for which Synapse shall output a warning when receiving
  an HTTP 500 response (each value each separated by a |). By default, Synapse
  outputs a warning for any HTTP 500 response, irrespective of the content-type.
  Consequently, also for each SOAP fault a warning will be logged. If only for
  specific content-types a warning shall be logged, please provide a |-separated
  list. To output the warning for messages which do not have a content-type set,
  please use the value 'none'.

  Example value: x-application/hessian|none

The following properties can be used to configure the HTTP sender to use a proxy.
They can be specified either as transport parameters in declared in
<transportSender> or as system properties.

- http.proxyHost
  The host name or address of the proxy server.
- http.proxyPort
  The TCP port of the proxy server.
- http.nonProxyHosts
  The hosts to which the HTTP sender should connect directly and not through
  the proxy server. The value can be a list of hosts, each separated by a |, and
  in addition a wildcard character (\*) can be used for matching.

  Example value:
  \*.foo.com|localhost

Note that the HTTPS sender has no proxy support yet.

The following parameters are specific to the HTTPS sender:

- keystore
  The keystore configuration. The value of this parameter must be a
  <KeyStore> element as shown in the example configurations
  above.
- truststore
  The truststore configuration. The value of this parameter must be a
  <TrustStore>
  element as shown in the example configurations above.
- novalidatecert
  When set to true, this parameter disables server certificate
  validation (trust). The default value is false. This parameter will
  be ignored if truststore is set.

  Setting his parameter to true
  is useful in development and test environments, but should not be used in
  production environments. If validation is disabled, a warning message will
  be logged at startup.
- HostnameVerifier
  This optional parameter specifies the policy to apply when checking that the
  hostname of the server matches the names stored inside the X.509 certificate
  presented by the server. Possible values are Strict, AllowAll
  and DefaultAndLocalhost. See the
  [HostnameVerifier Javadoc](../apidocs/org/apache/synapse/transport/nhttp/HostnameVerifier.html)
  for more details.
- CertificateRevocationVerifier
  This is an optional parameter to validate the revocation status of the host
  certificates using [OCSP](http://www.ietf.org/rfc/rfc2560.txt) and
  [CRL](http://www.ietf.org/rfc/rfc5280.txt) when making HTTPS connections.
  Simply uncomment this parameter in the axis2.xml file to enable the feature.
  Two LRU caches are used to cache CRLs and OCSP responses until they are expired. "CacheSize"
  property defines the maximum size of a cache. When this limit is reached, the
  old values will be automatically removed and updated with new values. "CacheDurationMins"
  is used to configure the time duration (in minutes) between two consecutive
  runs of the CacheManager task which periodically performs housekeeping work
  in each cache. Refer the example configuration above to see how to configure
  these properties. The scheduled CacheManager tasks for OCSP and CRL caches can
  be manually controlled using the JMX MBeans registered under the "CacheController"
  category.

<a id="synapse-apache-org-userguide-transports--Non-blocking_HTTPNHTTP_transport"></a>

## <a id="synapse-apache-org-userguide-transports--Non-blocking_HTTP_NHTTP_transport"></a>Non-blocking HTTP (NHTTP) transport

<a id="synapse-apache-org-userguide-transports--Example_configurations"></a>

### <a id="synapse-apache-org-userguide-transports--Example_configuration"></a>Example configuration

<transportReceiver name="http" class="org.apache.synapse.transport.nhttp.HttpCoreNIOListener">
<parameter name="port">8280</parameter>
<parameter name="non-blocking">true</parameter>
</transportReceiver>
<transportReceiver name="https" class="org.apache.synapse.transport.nhttp.HttpCoreNIOSSLListener">
<parameter name="port" locked="false">8243</parameter>
<parameter name="non-blocking" locked="false">true</parameter>
<parameter name="keystore" locked="false">
<KeyStore>
<Location>lib/identity.jks</Location>
<Type>JKS</Type>
<Password>password</Password>
<KeyPassword>password</KeyPassword>
</KeyStore>
</parameter>
<parameter name="truststore" locked="false">
<TrustStore>
<Location>lib/trust.jks</Location>
<Type>JKS</Type>
<Password>password</Password>
</TrustStore>
</parameter>
</transportReceiver>
<transportSender name="http" class="org.apache.synapse.transport.nhttp.HttpCoreNIOSender">
<parameter name="non-blocking" locked="false">true</parameter>
<parameter name="warnOnHTTP500" locked="false">\*</parameter>
</transportSender>
<transportSender name="https" class="org.apache.synapse.transport.nhttp.HttpCoreNIOSSLSender">
<parameter name="non-blocking" locked="false">true</parameter>
<parameter name="warnOnHTTP500" locked="false">\*</parameter>
<parameter name="keystore" locked="false">
<KeyStore>
<Location>lib/identity.jks</Location>
<Type>JKS</Type>
<Password>password</Password>
<KeyPassword>password</KeyPassword>
</KeyStore>
</parameter>
<parameter name="truststore" locked="false">
<TrustStore>
<Location>lib/trust.jks</Location>
<Type>JKS</Type>
<Password>password</Password>
</TrustStore>
</parameter>
<parameter name="CertificateRevocationVerifier">
<CacheSize>50</CacheSize>
<CacheDurationMins>5</CacheDurationMins>
</parameter>
</transportSender>

<a id="synapse-apache-org-userguide-transports--Transport_listener_parameters"></a>

### <a id="synapse-apache-org-userguide-transports--Transport_listener_parameters"></a>Transport listener parameters

The following parameters are supported by both the HTTP and the HTTPS listener:

- port
  The TCP port to bind the listener to.
- bind-address
  The IP address to bind the listener to. This can be used on hosts that have
  more than one network interface or IP address to run multiple Synapse instances
  listening to the same port. If this parameter is not specified, the
  listener will accept connections on any IP address.
- hostname
  The host name to use when computing endpoint references in generated WSDL files.
  The default value is the host name as provided by the operation system or
  localhost if the host name can't be determined. The value of this
  parameter is ignored if WSDLEPRPrefix is specified.
- WSDLEPRPrefix
  The URL prefix to use when computing endpoint references in generated WSDL files.
  The value must be a valid URL with at least a protocol and host. If this value
  is unspecified, endpoint references will be computed based on the listener type
  (HTTP or HTTPS) and hostname and port parameters.

  This parameter should be used if clients connect to Synapse through a frontend
  server, e.g. a (load balancing) Apache, and these clients rely on the address
  information in the WSDL documents exposed through ...?wsdl URLs.

The following parameters are specific to the HTTPS listener:

- keystore
  The keystore configuration. The value of this parameter must be a
  <KeyStore>
  element as shown in the example configurations above.
- truststore
  The truststore configuration. The value of this parameter must be a
  <TrustStore>
  element as shown in the example configurations above.
- SSLVerifyClient
  This parameter has the same meaning as the corresponding
  [mod\_ssl directive](http://httpd.apache.org/docs/2.2/mod/mod_ssl.html#sslverifyclient)
  and sets the desired certificate verification level for client authentication:
  - none (default): no client certificate is required at all
  - optional: the client may present a valid certificate, but is
    not required to do so
  - require: the client has to present a valid certificate,
    otherwise the connection request will be terminated during SSL handshake

<a id="synapse-apache-org-userguide-transports--Transport_sender_parameters"></a>

### <a id="synapse-apache-org-userguide-transports--Transport_sender_parameters"></a>Transport sender parameters

The following property can be used to control based on content-types whether the
HTTP/HTTPS sender shall output a warning for responses with HTTP status code 500.

- warnOnHTTP500
  A list of content-types for which Synapse shall output a warning when receiving
  an HTTP 500 response (each value each separated by a |). By default, Synapse
  outputs a warning for any HTTP 500 response, irrespective of the content-type.
  Consequently, also for each SOAP fault a warning will be logged. If only for
  specific content-types a warning shall be logged, please provide a |-separated
  list. To output the warning for messages which do not have a content-type set,
  please use the value 'none'.

  Example value: x-application/hessian|none

The following properties can be used to configure the HTTP sender to use a proxy.
They can be specified either as transport parameters in declared in
<transportSender> or as system properties.

- http.proxyHost
  The host name or address of the proxy server.
- http.proxyPort
  The TCP port of the proxy server.
- http.nonProxyHosts
  The hosts to which the HTTP sender should connect directly and not through
  the proxy server. The value can be a list of hosts, each separated by a |, and
  in addition a wildcard character (\*) can be used for matching.

  Example value:
  \*.foo.com|localhost

Note that the HTTPS sender has no proxy support yet.

The following parameters are specific to the HTTPS sender:

- keystore
  The keystore configuration. The value of this parameter must be a
  <KeyStore> element as shown in the example configurations
  above.
- truststore
  The truststore configuration. The value of this parameter must be a
  <TrustStore>
  element as shown in the example configurations above.
- novalidatecert
  When set to true, this parameter disables server certificate
  validation (trust). The default value is false. This parameter will
  be ignored if truststore is set.

  Setting his parameter to true
  is useful in development and test environments, but should not be used in
  production environments. If validation is disabled, a warning message will
  be logged at startup.
- HostnameVerifier
  This optional parameter specifies the policy to apply when checking that the
  hostname of the server matches the names stored inside the X.509 certificate
  presented by the server. Possible values are Strict, AllowAll
  and DefaultAndLocalhost. See the
  [HostnameVerifier Javadoc](../apidocs/org/apache/synapse/transport/nhttp/HostnameVerifier.html)
  for more details.
- CertificateRevocationVerifier
  This is an optional parameter to validate the revocation status of the host
  certificates using [OCSP](http://www.ietf.org/rfc/rfc2560.txt) and
  [CRL](http://www.ietf.org/rfc/rfc5280.txt) when making HTTPS connections.
  Simply uncomment this parameter in the axis2.xml file to enable the feature.
  Two LRU caches are used to cache CRLs and OCSP responses until they are expired. "CacheSize"
  property defines the maximum size of a cache. When this limit is reached, the
  old values will be automatically removed and updated with new values. "CacheDurationMins"
  is used to configure the time duration (in minutes) between two consecutive
  runs of the CacheManager task which periodically performs housekeeping work
  in each cache. Refer the example configuration above to see how to configure
  these properties. The scheduled CacheManager tasks for OCSP and CRL caches can
  be manually controlled using the JMX MBeans registered under the "CacheController"
  category.

<a id="synapse-apache-org-userguide-transports--VFS_transport"></a>

## <a id="synapse-apache-org-userguide-transports--VFS_transport"></a>VFS transport

<a id="synapse-apache-org-userguide-transports--Transport_listener"></a>

### <a id="synapse-apache-org-userguide-transports--Transport_listener"></a>Transport listener

The VFS transport listener receives messages dropped in a given local or remote file
system location. The location is specified by a URL that either identifies a single
file or a directory. The transport listener will periodically poll the specified
location and process any file(s) found. After a file has been processed it will be
deleted or moved to another location. Note that this is absolutely mandatory to
prevent the listener from processing files multiple times. Therefore, the VFS transport
listener can only be used in situations where it has write access to the file system
location and where deleting or moving the dropped files is acceptable.

The transport is based on
[Apache Commons VFS](http://commons.apache.org/vfs/)
and supports any protocol for which a VFS provider is available. The transport is
pre-configured with providers for local files (
file:
scheme), HTTP, HTTPS, FTP and SFTP (i.e. file transfer over SSH).

There is a fundamental difference between the VFS transport and transports such as
HTTP and it is important to understand this difference to be able to use the VFS
transport correctly. The HTTP transport binds to a single protocol endpoint, i.e.
a TCP port on which it accepts incoming HTTP requests. These requests are then
dispatched to the right service based on the request URI. On the other hand, the
VFS transport only receives the payload of a message, but no additional information
that could be used to dispatch the message to a service. This means that file system
locations must be explicitly mapped to services. This is done using a set of service
parameters.

For Synapse this means that the VFS transport listener can only be used in
conjunction with proxy services. The relevant service parameters are then specified
as follows:

<proxy name="MyVFSService" transports="vfs">
<parameter name="transport.vfs.FileURI">file:///var/spool/synapse/in</parameter>
<parameter name="transport.vfs.ContentType">application/xml</parameter>
...
<target>
...
</target>
</proxy>

In this example the file system location
file:///var/spool/synapse/in
is explicitly bound to
MyVFSService
, i.e. any message dropped in that location will be predispatched to that service, bypassing any other configured
dispatch mechanisms that would apply to messages received through HTTP.

The VFS transport recognizes the following service parameters:

- transport.vfs.FileURI(Required)
  The primary File (or Directory) URI in the vfs\* transport format, for this
  service
- transport.vfs.ContentType (Required)
  The expected content type for files retrieved for this service. The VFS
  transport uses this information to select the appropriate message builder.

  Examples:

  - text/xml for plain XML or SOAP
  - text/plain; charset=ISO-8859-1 for text files
  - application/octet-stream for binary data
- transport.vfs.FileNamePattern
  (Optional)
  A file name regex pattern to match files against a directory specified by
  the FileURI
- transport.PollInterval (Optional)
  The poll interval (in seconds)
- transport.vfs.ActionAfterProcess (Optional)
  DELETE or MOVE
- transport.vfs.MoveAfterProcess (Optional)
  The directory to move files after processing (i.e. all files process
  successfully)
- transport.vfs.ActionAfterErrors (Optional)
  DELETE or MOVE
- transport.vfs.MoveAfterErrors (Optional)
  The directory to move files after errors (i.e. some of the files succeed
  but some fail)
- transport.vfs.ActionAfterFailure (Optional)
  DELETE or MOVE
- transport.vfs.MoveAfterFailure (Optional)
  The directory to move after failure (i.e. all files fail)
- transport.vfs.ReplyFileURI (Optional)
  Reply file URI
- transport.vfs.ReplyFileName (Optional)
  Reply file name (defaults to response.xml)
- transport.vfs.MoveTimestampFormat (Optional)
  Timestamp prefix format for processed file name. java.text.SimpleDateFormat
  compatible string. e.g. yyMMddHHmmss'-'
- transport.vfs.Locking (Optional)
  By-default file locking is turned on in the VFS transport, and this parameter
  lets you configure the locking behaviour on a per service basis. Possible values are
  enable or disable, and both these values are important because
  the locking can be disabled at the global level by specifying that at the
  receiver level and selectively enable locking only for a set of services.
- transport.vfs.Streaming (Optional)
  If this parameter is set to true, the transport will attempt to use a
  javax.activation.DataSource (instead of a java.io.InputStream
  ) object to pass the content of the file to the message builder. Note that this
  is only supported by some message builders, e.g. for plain text and binary.

  This allows processing of the message without storing the entire content
  in memory. It also has two other side effects:

  - The incoming file (or connection in case of a remote file) will only
    be opened on demand.
  - Since the data is not cached, the file might be read several times.

  This option can be used to achieve streaming of large payloads. Note that
  this feature is still somewhat experimental and might be superseded by a
  more flexible mechanism in a future release.

Note that since the VFS endpoints are configured at the level of the service, the
only parameter that is available at the listener is the file locking configuration
parameter which is optional and the transport listener is enabled in
axis2.xml
simply as follows:

<transportReceiver name="vfs" class="org.apache.synapse.transport.vfs.VFSTransportListener">
<parameter name="transport.vfs.Locking">enable | disable</parameter> ?
</transportReceiver>

<a id="synapse-apache-org-userguide-transports--Transport_sender"></a>

### <a id="synapse-apache-org-userguide-transports--Transport_sender"></a>Transport sender

The VFS transport sender allows to write outgoing messages to local or remote files.
As with the listener, the transport sender supports any protocol for which there
is a VFS provider.

The sender is enabled be the following directive in
axis2.xml file locking which is by-default enabled can be configured
using the transport.vfs.Locking parameter:

<transportSender name="vfs" class="org.apache.synapse.transport.vfs.VFSTransportSender">
<parameter name="transport.vfs.Locking">enable | disable</parameter> ?
</transportSender>

To send a message using the VFS transport, the destination URI must start with
vfs: followed by a valid VFS URL. For example, in a Synapse mediation,
one would use:

<endpoint>
<address uri="vfs:file:///var/spool/synapse/out"/>
</endpoint>

Other examples of valid VFS URLs are (see
[http://commons.apache.org/vfs/filesystems.html](http://commons.apache.org/vfs/filesystems.html)
for more samples):

- file:///directory/filename.ext
- file:////somehost/someshare/afile.txt
- jar:../lib/classes.jar!/META-INF/manifest.mf
- jar:zip:outer.zip!/nested.jar!/somedir
- ftp://myusername:mypassword@somehost/pub/downloads/somefile.tgz[?vfs.passive=true]

The global configuration of the file locking can be overriden by providing the
transport.vfs.Locking as a URL parameter with the appropriate value (
enable, or disable) on a given endpoint.

It should be noted that by its nature, the VFS transport sender doesn't support
synchronous responses and should only be invoked using the out-only message
exchange pattern. In a Synapse mediation, this can be forced using the
following mediator:

<property action="set" name="OUT\_ONLY" value="true"/>

<a id="synapse-apache-org-userguide-transports--Using_SFTP"></a>

### <a id="synapse-apache-org-userguide-transports--Using_SFTP"></a>Using SFTP

To avoid man-in-the-middle attacks, SSH clients will only connect to hosts with
a known host key. When connecting for the first time to an SSH server, a typical
command line SSH client would request confirmation from the user to add the
server and its fingerprint to the list of known hosts.

The VFS transports supports SFTP through the
[JSch](http://www.jcraft.com/jsch/)
library and this library also requires a list of known hosts. Since Synapse is
not an interactive process, it can't request confirmation from the user and is
therefore unable to automatically add a host to the list. This implies that the
list of known hosts must be set up manually before the transport can connect.

Jsch loads the list of known hosts from a file called known\_hosts in
the .ssh sub-directory of the user's home directory, i.e. $HOME/.ssh
in Unix and %HOMEPATH%\.ssh in Windows. The location and format of this
file are compatible with the [OpenSSH](http://www.openssh.com/)
client.

Since the file not only contains a list of host names but also the fingerprints
of their host keys, the easiest way to add a new host to that file is to simply
use the OpenSSH client to open an SSH session on the target host. The client will
then ask to add the credentials to the known\_hosts file. Note that if
the SSH server is configured to only allow SFTP sessions, but no interactive
sessions, the connection will actually fail. Since this doesn't rollback the
change to the known\_hosts file, this error can be ignored.

<a id="synapse-apache-org-userguide-transports--Known_issues"></a>

### <a id="synapse-apache-org-userguide-transports--Known_issues"></a>Known issues

The VFS listener will start reading a file as soon as it appears in the configured
location. To avoid processing half written files, the creation of these files should
be made atomic. On most platforms, this can be achieved by writing the data to a
temporary file and then moving the file to the target location. Note however that
a move operation is only atomic if the source and destination are on the same
physical file system. The location for the temporary file should be chosen with
that constraint in mind.

It should also be noted that the VFS transport sender doesn't create files atomically.

<a id="synapse-apache-org-userguide-transports--FIX_transport"></a>

## <a id="synapse-apache-org-userguide-transports--FIX_transport"></a>FIX transport

A general overview about the FIX transport can be found in the following articles:

- [Apache Synapse FIX'ed](http://wso2.org/library/3449)
- [Using the WSO2 ESB and FIX](http://wso2.org/library/3837)
  (also applies to Synapse)

<a id="synapse-apache-org-userguide-transports--Setting_up_the_FIX_Transport"></a>

### <a id="synapse-apache-org-userguide-transports--Setting_up_the_FIX_Transport"></a>Setting up the FIX Transport

To use the FIX transport, you need a local
[Quickfix/J](http://www.quickfixj.org)
installation. Download Quickfix/J from
[http://www.quickfixj.org/downloads](http://www.quickfixj.org/downloads)
.

To enable the FIX transport, you need to uncomment the FIX transport sender and
FIX transport receiver configurations in the SYNAPSE\_HOME/repository/conf/axis2.xml.
Simply locate and uncomment the FIXTransportSender and FIXTransportListener sample
configurations. Also, add the following jars to the Synapse class path
(SYNAPSE\_HOME/lib directory).

- quickfixj-core.jar
- quickfixj-msg-fix40.jar
- quickfixj-msg-fix41.jar
- quickfixj-msg-fix42.jar
- quickfixj-msg-fix43.jar
- quickfixj-msg-fix44.jar
- mina-core.jar
- slf4j-api.jar
- slf4j-jdk14.jar

All these jars are shipped with the Quickfix/J binary distribution.

<a id="synapse-apache-org-userguide-transports--FIX_Transport_Parameters"></a>

### <a id="synapse-apache-org-userguide-transports--FIX_Transport_Parameters"></a>FIX Transport Parameters

This is the list of all parameters accepted by the FIX transport. Refer the
sample 257 and 258 to see how some of them are used in practice.

- transport.fix.AcceptorConfigURL
  If a service needs to listen to incoming FIX messages from a remote initiator,
  then Synapse needs to create an acceptor. This parameter should contain the
  URL of the file which contains the FIX configuration for the acceptor.
  (See sample 257)
- transport.fix.InitiatorConfigURL
  If a service needs to send FIX messages to a remote acceptor Synapse should
  create an initiator. This parameter should contain the URL of the file which
  contains the FIX configuration for the initiator. (See sample 257)
- transport.fix.AcceptorMessageStore
  The type of message store to be used with the acceptor. Allowed values for
  this parameter are 'file', 'jdbc', 'memory' and 'sleepycat'. If not specified
  memory based message store will be used by default. Additional parameters
  required to configure each of the message stores should be specified in the
  acceptor configuration file.
- transport.fix.InitiatorMessageStore
  Same as the above but applies only for the initiators. Additional parameters
  required to configure each of the message stores should be specified in the
  initiator configuration file.
- transport.fix.AcceptorLogFactory
  Specifies the transport level log factory to be used to log messages going
  through the acceptor. FIX messages are logged without putting them in SOAP
  envelopes at this level. Accepted values are 'console', 'file' and 'jdbc'.
  If not specified no logging will be done at the transport level. Additional
  parameters required to configure each of the lof factories should be specified
  in the acceptor configuration file.
- transport.fix.InitiatorLogFactory
  Specifies the transport level log factory to be used to log messages going
  through the initiator. Functionality is similar to the above. Additional
  parameters required to configure each of the lof factories should be specified
  in the initiator configuration file.
- transport.fix.ResponseDeliverToCompID
  If a response FIX message sent from Synapse to a remote FIX engine should be
  forwarded from the remote engine to another party, this parameter can be used
  to set the DeliverToCompID field of the messages at Synapse.
- transport.fix.ResponseDeliverToSubID
  If a response FIX message sent from Synapse to a remote FIX engine should be
  forwarded from the remote engine to another party, this parameter can be used
  to set the DeliverToSubID field of the messages at Synapse.
- transport.fix.ResponseDeliverToLocationID
  If a response FIX message sent from Synapse to a remote FIX engine should be
  forwarded from the remote engine to another party, this parameter can be used
  to set the DeliverToLocationID field of the messages at Synapse.
- transport.fix.ServiceName
  Used when messages coming over a different protocol has to be forwarded over
  FIX. The value must be equal to the name of the service and the scope must be
  'axis2-client' (See sample 258)
- transport.fix.SendAllToInSequence
  When there are multiple responses to a FIX request and when we need only one
  of them to be sent to the original requester this parameter has to be set to
  'false'. This mostly comes handy when the original requester is communicating
  over a different protocol (like HTTP). If this parameter is not set to 'false'
  at such scenarios messages might get into a loop. (See sample 258)
- transport.fix.BeginStringValidation
  When the FIX messages sent to Synapse should not be forwarded to a FIX session
  with a different BeginString value this parameter can be set to 'true'. Setting
  this parameter to 'true' will enforce this restriction.

<a id="synapse-apache-org-userguide-transports--amqp_transport"></a>

## <a id="synapse-apache-org-userguide-transports--AMQP_transport"></a>AMQP transport

<a id="synapse-apache-org-userguide-transports--setting_up_the_amqp_transport"></a>

### <a id="synapse-apache-org-userguide-transports--Setting_Up_the_Transport"></a>Setting Up the Transport

AMQP transport is based on the widely used
[Java AMQP client](http://www.rabbitmq.com/java-client.html) library from
[RabbitMQ](http://www.rabbitmq.com/).
The client library is not distributed with Apache Synapse, and hence in order to use
the AMQP transport, download the RabbitMQ Java client and copy the client library
(rabbitmq-client.jar) into Synapse classpath (SYNAPSE\_HOME/lib directory).

To enable the AMQP transport, uncomment the AMQP transport sender and AMQP transport
receiver configurations in the SYNAPSE\_HOME/repository/conf/axis2.xml.

<a id="synapse-apache-org-userguide-transports--amqp_transport_parameters"></a>

### <a id="synapse-apache-org-userguide-transports--AMQP_Transport_Parameters"></a>AMQP Transport Parameters

Following parameters can be configured as part of AMQP transport receiver, sender,
a proxy service or an AMQP endpoint declaration.

- transport.amqp.Uri
  The connection URL for the broker of the form
  amqp://userName:password@hostName:portNumber/virtualHost
- transport.amqp.BrokerList
  The list of broker of the form, host1:port1,host2:port2... which will be used
  as the address array in AMQP connection to the broker
- transport.amqp.ExchangeName
  The name of the exchange to connect
- transport.amqp.IsExchangeDurable
  Should the exchange be declared as durable?
- transport.amqp.IsExchangeAutoDelete
  Should the exchange be auto delete? Possible values are true or false
- transport.amqp.ChannelPreFetchSize
  The channel pre fetch size for fair dispatch
- transport.amqp.ChannelPreFetchCountSize
  The channel prefetch count for fair dispatch
- transport.amqp.ExchangeType
  Type of the exchange to use. Possible values are, fanout, direct, header or topic
- transport.amqp.ExchangeInternal
  Should the exchange be declared as internal? Possible values are true or false
- transport.amqp.BindExchange
  The name of the exchange that the publisher/consumer should publish/consume message to.
- transport.amqp.BindingKeys
  The comma separated binding keys this queue should be bound into exchange
- transport.amqp.RoutingKey
  The routing key to be used by the publisher.
- transport.amqp.ConsumerTx
  Use transactions at consumer side if set to true. By default, this will be
  considered false and explicit acknowledgement will be done
- transport.amqp.QueueName
  The name of the queue
- transport.amqp.IsQueueDurable
  Should the queue declare as durable? Possible values are true or false.
- transport.amqp.IsQueueRestricted
  Should the queue declare as restricted? Possible values are true or false.
- transport.amqp.IsQueueAutoDelete
  Should the queue declare as auto delete when it's no longer in use?.
  Possible values are true or false.
- transport.amqp.OperateOnBlockingMode
  True if the polling task should wait until it process the accepted
  messages. This can be used in conjunction with a single thread polling
  task (in the whole transport, i.e. only a single AMQP proxy per flow)
  to achieve in order delivery
- transport.amqp.ConnectionFactoryName
  The connection factory to be used either with consumer or producer.
- transport.amqp.ResponseConnectionFactoryName
  In a two-way scenario, which connection factory of the senders' should be used
  to send the response
- transport.amqp.ScheduledTaskInitialDelay
  The initial delay (in milliseconds) that the polling task should delay before initial attempt
- transport.amqp.ScheduledTaskDelay
  The delay (in milliseconds) that the polling task should delay before next attempt.
- transport.amqp.ScheduledTaskTimeUnit
  The time unit which should use to calculate,
  transport.amqp.ScheduledTaskInitialDelay and transport.amqp.ScheduledTaskDelay.
- transport.amqp.NoOfConcurrentConsumers
  Number of concurrent consumers per polling task.
- transport.amqp.NoOfDispatchingTask
  Number of dispatching task to use any request messages to actual processing task.
- transport.amqp.ContentType
  Configure the content type as a service parameter.
- AMQP\_CONTENT\_TYPE
  Message context property to set the AMQP message content type.
- AMQP\_CONTENT\_ENCODING
  Message context property to set the AMQP message encoding
- AMQP\_HEADER\_\*
  Specify any AMQP headers as message context properties using AMQP\_HEADER\_\*
- AMQP\_DELIVERY\_MODE
  Message context property to set the AMQP message delivery mode
- AMQP\_PRIORITY
  Message context property to set the AMQP message priority.
- AMQP\_CORRELATION\_ID
  Message context property to set the AMQP message correlation id
- AMQP\_REPLY\_TO
  Message context property to set the AMQP message reply to header.
- AMQP\_EXPIRATION
  Message context property to set the AMQP expiration.
- AMQP\_MESSAGE\_ID
  Message context property to set the message id of the AMQP message
- AMQP\_TIME\_STAMP
  Message context property to set the timestamp of the AMQP message.
- AMQP\_TYPE
  Message context property to set the type of the AMQP message
- AMQP\_PRODUCER\_TX
  Use transactions at producer side. Possible values are tx(for blocking transactions),
  lwpc(for light weight producer connections).
- connection-factory-pool-size
  A system property to set the worker pool size of the connection factory executor service.
- worker-pool-size
  A system property to set the worker pool size used by deployed services for polling broker.
- semaphore-time-out
  A system property to set the timeout (in seconds) of semaphore which waits for
  a response.
- initial-reconnect-duration
  If a polling task encounter an exception due to some reason(most probably
  due to broker outage) it will be suspended until a successful re-connect.
  This system property defines the initial duration that the re-connection
  check task should be suspended (1000 ms by default) before next re-try.
- reconnection-progression-factor
  A system property to define the factor (2.0 by default) to multiply the initial
  suspended duration to calculate the next suspending duration for the
  re-connection check task.
- maximum-reconnection-duration
  The maximum duration that re-connection check task should be suspended(
  10 minutes by default). After this time is reached, the suspended duration
  will be fall back to its original initial configured duration.

<a id="synapse-apache-org-userguide-transports--amqp_transport_ex"></a>

### <a id="synapse-apache-org-userguide-transports--Sample_Configurations"></a>Sample Configurations

Producer example

<proxy name="ProducerProxy" transports="http">
<target>
<inSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
<property name="PRESERVE\_WS\_ADDRESSING" value="true"/>"
<log level="custom">
<property name="status" value="At ProducerProxy"/>
</log>
</inSequence>
<endpoint>
<!--use the defined connection factory in AMQP transport sender-->
<address
uri="amqp://SimpleStockQuoteService?transport.amqp.ConnectionFactoryName=producer&transport.amqp.QueueName=ProducerProxy"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
</proxy>

Consumer example

<proxy name="ConsumerProxy" transports="amqp">
<target>
<inSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
<log level="custom">
<property name="status" value="At ConsumerProxy"/>
</log>
</inSequence>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
<parameter name="transport.amqp.ConnectionFactoryName">consumer</parameter>
<parameter name="transport.amqp.QueueName">ProducerProxy</parameter>
</proxy>

Routing example

<proxy name="DirectPublisherProxy" transports="http">
<target>
<inSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
<log level="custom">
<property name="status" value="At DirectPublisherProxy"/>
</log>
</inSequence>
<endpoint>
<!--use the defined connection factory in AMQP transport sender, note how we don't provide
any queue name because this is bind to the exchange-->
<address
uri="amqp://?transport.amqp.ConnectionFactoryName=publisher&transport.amqp.ExchangeName=direct\_logs&transport.amqp.ExchangeType=direct&transport.amqp.RoutingKey=error"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
</proxy>
<proxy name="DirectSubscriberProxy1" transports="amqp">
<target>
<inSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
<log level="custom">
<property name="status" value="At DirectSubscriberProxy 1"/>
</log>
</inSequence>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
<parameter name="transport.amqp.ConnectionFactoryName">subscriber</parameter>
<parameter name="transport.amqp.ExchangeName">direct\_logs</parameter>
<parameter name="transport.amqp.ExchangeType">direct</parameter>
<parameter name="transport.amqp.BindingKeys">warning,error</parameter>
</proxy>
<proxy name="DirectSubscriberProxy2" transports="amqp">
<target>
<inSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
<log level="custom">
<property name="status" value="At DirectSubscriberProxy 2"/>
</log>
</inSequence>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
<parameter name="transport.amqp.ConnectionFactoryName">subscriber</parameter>
<parameter name="transport.amqp.ExchangeName">direct\_logs</parameter>
<parameter name="transport.amqp.ExchangeType">direct</parameter>
<parameter name="transport.amqp.BindingKeys">error</parameter>
</proxy>

Producer transactions

<proxy name="ProducerTxProxy1" transports="http">
<target>
<inSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
<property action="set" name="AMQP\_PRODUCER\_TX" scope="axis2" value="lwpc"/>
<log level="custom">
<property name="status" value="At ProducerTxProxy1, use light weight producer confirm..."/>
</log>
</inSequence>
<endpoint>
<!--use the defined connection factory in AMQP transport sender-->
<address
uri="amqp://SimpleStockQuoteService?transport.amqp.ConnectionFactoryName=producer&transport.amqp.QueueName=ProducerProxy"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
</proxy>

---

<a id="synapse-apache-org-userguide-upgrading"></a>

# Apache Synapse – Apache Synapse - Upgrading to the Latest Version

## <a id="synapse-apache-org-userguide-upgrading--Upgrading_to_the_Latest_Version"></a>Upgrading to the Latest Version

If you are using an older version of Synapse (1.x/2.x) and now looking to migrate to
the latest versions (3.x) this document would be a good starting point. This
article provides information on steps that need to be carried out to smoothly
migrate from an older release of Synapse to the latest version. If you are migrating from 1.x version
to 3.x we recommend you follow the 1.x -> 2.x migration followed by the 2.x -> 3.x.

## <a id="synapse-apache-org-userguide-upgrading--Contents"></a>Contents

- [General comments](#synapse-apache-org-userguide-upgrading--General_comments)
- [Upgrading from 2.1 to 3.0](#synapse-apache-org-userguide-upgrading--Upgrading_from_2.1_to_3.0)
  - [Custom Extensions](#synapse-apache-org-userguide-upgrading--Custom_Extensions_3.0)
  - [Respond and Loopback Mediators](#synapse-apache-org-userguide-upgrading--New_mediators_3.0)
- [Upgrading from 1.2 to 2.1](#synapse-apache-org-userguide-upgrading--Upgrading_from_1.2_to_2.1)
  - [Configuration file vs multi XML configuration](#synapse-apache-org-userguide-upgrading--Configuration_file_vs_multi_XML_configuration)
  - [Endpoint URLs for proxy services](#synapse-apache-org-userguide-upgrading--Endpoint_URLs_for_proxy_services)
  - [Mediator Deployer](#synapse-apache-org-userguide-upgrading--Mediator_Deployer)
  - [Main Sequence](#synapse-apache-org-userguide-upgrading--Main_Sequence)
  - [Filter Mediator](#synapse-apache-org-userguide-upgrading--Filter_Mediator)
  - [Migration Tool](#synapse-apache-org-userguide-upgrading--Migration_Tool)
  - [Custom Extensions and API changes](#synapse-apache-org-userguide-upgrading--Custom_Extensions_and_API_changes)

<a id="synapse-apache-org-userguide-upgrading--General_comments"></a>

## <a id="synapse-apache-org-userguide-upgrading--General_comments"></a>General comments

If you are using custom extensions (mediators, startups, etc.) implemented in Java and
depending on Synapse APIs, you should go through the following process before upgrading
to a new release:

1. Compile the extension with the libraries from the Synapse release you are
   currently using and check for any deprecation warnings. You should change your
   code to eliminate all those warnings. In general the Javadoc of the
   deprecated class or method gives you a hint on how to change your code. Test all
   your changes with your current Synapse release.
2. Recompile and test the extension with the libraries from the new Synapse release.
   We try to avoid introducing incompatible changes to Synapse's core APIs between
   releases (except if the related classes or methods were deprecated in the previous
   release). However, it is not always possible to maintain compatibility. In addition
   your code might depend on features that are not part of the core API. Therefore,
   even if you don't use deprecated methods and classes, there is no guarantee that
   your code will not break when upgrading to a new release and you always need to
   recompile and test them before deploying to the new release.
3. Upgrade your Synapse installation and deploy the new version of your extensions.

If you are skipping releases when upgrading your installation, you might nevertheless
want to go through the first step for all the intermediate releases. This will make
the migration easier.

<a id="synapse-apache-org-userguide-upgrading--Upgrading_from_2.1_to_3.0"></a>

## <a id="synapse-apache-org-userguide-upgrading--Upgrading_from_2.1_to_3.0"></a>Upgrading from 2.1 to 3.0

<a id="synapse-apache-org-userguide-upgrading--Custom_Extensions_3.0"></a>

### <a id="synapse-apache-org-userguide-upgrading--Custom_Extensions"></a>Custom Extensions

Synapse 3.0 introduces the PassThrough HTTP Transport which improves HTTP transport performance by removing
intermediary buffer use when there is no requirement to read an HTTP payload (i.e. message is merely passed
through). If you have custom extensions that depend on accessing or reading buffers directly, you
may require to change your code to trigger the PassThrough transport to build the payload. This can
be done using the [RelayUtils#buildMessage()](../apidocs/org/apache/synapse/transport/passthru/util/RelayUtils.html)
static methods.

<a id="synapse-apache-org-userguide-upgrading--New_mediators_3.0"></a>

### <a id="synapse-apache-org-userguide-upgrading--Respond_and_Loopback_Mediators"></a>Respond and Loopback Mediators

[Respond](#synapse-apache-org-userguide-mediators--Respond) and [Loopback](#synapse-apache-org-userguide-mediators--Loopback)
are new mediators introduced in Apache Synapse 3.0. [Respond](#synapse-apache-org-userguide-mediators--Respond) provides
a mechanism to send a message back to the client as a response. In older versions of Apache Synapse
we would use the [Send](#synapse-apache-org-userguide-mediators--Send) mediator to respond back to a client either
after a message is received from a backend in an out sequence or force a response back to the client
using a series of mediators as follows:

<header name="To" action="remove"/>
<property name="RESPONSE" value="true" scope="default" type="STRING"/>
<property name="NO\_ENTITY\_BODY" scope="axis2" action="remove"/>
<send/>

We can replace above methods with the [Respond](#synapse-apache-org-userguide-mediators--Respond) mediator anywhere
in the mediation flow to respond back to the client with the current message. Any mediators residing
after the [Respond](#synapse-apache-org-userguide-mediators--Respond) mediator will be ignored.

Similarly the [Loopback](#synapse-apache-org-userguide-mediators--Loopback) mediator is used to jump the flow from
an in sequence to the out sequence ignoring any mediators residing after the
[Loopback](#synapse-apache-org-userguide-mediators--Loopback) mediator. [Loopback](#synapse-apache-org-userguide-mediators--Loopback)
mediator has no effect from inside an out sequence.

<a id="synapse-apache-org-userguide-upgrading--Upgrading_from_1.2_to_2.1"></a>

## <a id="synapse-apache-org-userguide-upgrading--Upgrading_from_1.2_to_2.1"></a>Upgrading from 1.2 to 2.1

<a id="synapse-apache-org-userguide-upgrading--Configuration_file_vs_multi_XML_configuration"></a>

### <a id="synapse-apache-org-userguide-upgrading--Configuration_file_vs_multi_XML_configuration"></a>Configuration file vs multi XML configuration

In 1.2 you have been using a single synapse.xml file which resides on the
repository/conf directory of the distribution, where as on 2.1 we have structured
this into a configuration repository with multiple directories to have different
artifact types and each and every artifact configuration to reside on a different
files inside the desired repository directory. This repository directory on the 2.1
release resides in the repository/conf directory too, and named as synapse-config.
The repository directory structure inside the synapse-config directory
looks like follows;

synapse-config
/api
/endpoints
/event-sources
/local-entries
/priority-executors
/proxy-services
/sequences
main.xml
fault.xml
/tasks
/templates
registry.xml
synapse.xml

As you can see in the above sketch of the repository though it is a repository based
configuration, it also supports the old style single flat synapse.xml file in which
case it has to reside inside the root of the repository.

So the easiest way to migrate the configuration is to move your already existing
synapse.xml file in repository/conf directory in 1.2 version into the
repository/conf/synapse-config directory of the 2.x version. *Note: When doing this
migration you should also delete the main.xml and fault.xml files which are there on the
sequences directory of the repository, otherwise there will be 2 main and fault
sequences one coming from the sequences directory and the other coming from your just
copied synapse.xml file.*

<a id="synapse-apache-org-userguide-upgrading--Endpoint_URLs_for_proxy_services"></a>

### <a id="synapse-apache-org-userguide-upgrading--Endpoint_URLs_for_proxy_services"></a>Endpoint URLs for proxy services

In release 2.1 the endpoint URLs for proxy services have changed from
/soap to /services. E.g. http://localhost:8280/services/StockQuote
should be used instead of http://localhost:8280/soap/StockQuote.

<a id="synapse-apache-org-userguide-upgrading--Mediator_Deployer"></a>

### <a id="synapse-apache-org-userguide-upgrading--MediatorDeployer"></a>MediatorDeployer

Release 1.3 has enhanced capabilities for extension deployment. While in 1.2 extension
deployment was limited to mediators bundled as simple JAR files, 1.3 extended this
support to tasks and defined a new archive format (XAR) that allows to bundle
these extensions together with their dependency JARs (see
[SYNAPSE-377](http://issues.apache.org/jira/browse/SYNAPSE-377)
for more details). Enabling these features requires changes to the axis2.xml
configuration file. In 1.2 the deployer was configured as follows:

<deployer extension="jar" directory="mediators"
class="org.apache.synapse.core.axis2.MediatorDeployer"/>

In 2.1 the suggested configuration is:

<deployer extension="xar" directory="extensions"
class="org.apache.synapse.deployers.ExtensionDeployer"/>

It is possible to have multiple configuration entries for the extension deployer
with different settings. For example, if you used the deployer in 1.2 you might
want to have the following configuration:

<deployer extension="jar" directory="mediators"
class="org.apache.synapse.deployers.ExtensionDeployer"/>
<deployer extension="xar" directory="extensions"
class="org.apache.synapse.deployers.ExtensionDeployer"/>

<a id="synapse-apache-org-userguide-upgrading--JMS_transport"></a>

### <a id="synapse-apache-org-userguide-upgrading--JMS_transport"></a>JMS transport

The way the JMS transport determines the content type of incoming messages has
slightly changed between Synapse 1.2 and 2.x. The mechanism is also more flexible.
See SYNAPSE-304 and SYNAPSE-424 for the reasons of this change and refer to the
[WS-Commons Transport project](http://ws.apache.org/commons/transport/)
for documentation.

<a id="synapse-apache-org-userguide-upgrading--Main_Sequence"></a>

### <a id="synapse-apache-org-userguide-upgrading--Main_Sequence"></a>Main Sequence

On Synapse 1.2 you could have mediator configuration on the top level definitions
tag and they were treated as the **main** sequence if there is no
main sequence defined in the configuration. How ever removing the conflict of having
top level mediators and a main sequence leading the synapse to fail to start on
2.x Synapse configuration builder simply ignores the top level mediators. So you
need to wrap the top level mediators, if there are any, with the sequence named
**main** on the new 2.1 version.

To further explain this lets have a look at the following valid configuration bit
(this is the sample 0 configuration) on the 1.2;

<definitions xmlns="http://ws.apache.org/ns/synapse">
<!-- log all attributes of messages passing through -->
<log level="full"/>
<!-- Send the message to implicit destination -->
<send/>
</definitions>

which needs to be changed to the following configuration on 2.1

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<!-- log all attributes of messages passing through -->
<log level="full"/>
<!-- Send the message to implicit destination -->
<send/>
<sequence/>
</definitions>

<a id="synapse-apache-org-userguide-upgrading--Filter_Mediator"></a>

### <a id="synapse-apache-org-userguide-upgrading--Filter_Mediator"></a>Filter Mediator

From 2.1 onwards Synapse filter mediator supports the else close as well, and hence
the filter matching set of mediators has to be enclosed within a <then> element.

If we consider the following sample from the 1.2 version of synapse;

<filter source="get-property('To')" regex=".\*/StockQuote.\*">
<send>
<endpoint>
<address uri="http://localhost:9000/soap/SimpleStockQuoteService"/>
</endpoint>
</send>
<drop/>
</filter>

the equivalent configuration for the 2.1 release is going to be;

<filter source="get-property('To')" regex=".\*/StockQuote.\*">
<then>
<send>
<endpoint>
<address uri="http://localhost:9000/soap/SimpleStockQuoteService"/>
</endpoint>
</send>
<drop/>
<then/>
</filter>

You could also add an else close to this conditional statement as follows on 2.x
which is not possible on 1.2

<filter source="get-property('To')" regex=".\*/StockQuote.\*">
<then>
<send>
<endpoint>
<address uri="http://localhost:9000/soap/SimpleStockQuoteService"/>
</endpoint>
</send>
<drop/>
<then/>
<else/>
<log/>
<else/>
</filter>

<a id="synapse-apache-org-userguide-upgrading--Migration_Tool"></a>

### <a id="synapse-apache-org-userguide-upgrading--Migration_Tool"></a>Migration Tool

In general it is recommended to run the configuration through the migration tool
provided with the Synapse 2.x release, on your synapse 1.2 configuration before
using it with the 2.1.

To run the migration tool execute the synapse-config-migrator.sh by passing the
synapse.xml file location of the
1.2 configuration. Which will create the 2.1
compatible configuration with the .new suffix. For example;

```text
sh bin/synapse-config-migrator.sh synapse-i1.2/repository/conf/synapse.xml
```

<a id="synapse-apache-org-userguide-upgrading--Custom_Extensions_and_API_changes"></a>

### <a id="synapse-apache-org-userguide-upgrading--Custom_Extensions_and_API_changes"></a>Custom Extensions and API changes

Even though there is a migration tool it just takes care of your configuration and not
custom extensions that you have done for example like CustomMediators or Tasks
and so forth. There are some API changes that affect your custom extensions
unfortunately. This section tries to list all the public API changes which affects
the backward compatibility of the custom extensions that you have been running
with the 1.2 version of Synapse.

| Class | Method | Change Description |
| --- | --- | --- |
| AbstractMediatorFactory | createMediator(OMElement) | This was the method that you have been overwriting on the 1.2 version to 							implement a new custom mediator factory to build the mediator by looking at 							the XML configuration. On the 2.1 version you should be extending thecreateSpecificMediator(OMElement, Properties). Note that in the process of changing the method to be extended, the methodcreateMediatormethod has been changed to be final. From a users point of view of this                             interface he/she should be using the createMediator method which is what                             Synapse does. |
| AbstractMediatorSerializer | serializeMediator(Mediator) | This was the method that you have been overwriting on the 1.2 version to 							implement a new custom mediator serializer to serialize to the XML                             Configuration by walking through the mediator properties. On the 2.1                             version you should be extending theserializeSpecificMediator(Mediator). Note that in the process of changing the method to be extended, the methodserializeMediatormethod has been changed to be final. From a users point of view of this                             interface he/she should be using the serializeMediator method which is                             what Synapse does. |

Further to that if you have been using
[ServerManager](../apidocs/org/apache/synapse/ServerManager.html)
class you may have noticed that the class is no more a singleton and doesn't have
the static getInstance method. Also note that the common utilities like data
sources JMX and RMI registration stuff have been moved to a new module with
org.apache.synapse.commons package name.

On the configuration building front all entities are given a properties map to
construct its instance and that has been used to pass in any additional information
required like RESOLVE\_ROOT, or SYNAPSE\_HOME startup parameters. For example if you
look at the
[MediatorFactoryFinder](../apidocs/org/apache/synapse/config/xml/MediatorFactoryFinder.html)
class the
[getMediator](../apidocs/org/apache/synapse/config/xml/MediatorFactoryFinder.html#getMediatororg.apache.axiom.om.OMElement20java.util.Properties)
method is expecting a properties map apart from the OMElement argument.
It is safe to pass in a empty properties map if you are using these methods for
any testing purposes or even in cases where you do not resolve dependencies.

---

<a id="synapse-apache-org-userguide-xpath"></a>

# Apache Synapse – Apache Synapse - XPath Functions and Variables

<a id="synapse-apache-org-userguide-xpath--Intro"></a>

## <a id="synapse-apache-org-userguide-xpath--Introduction"></a>Introduction

Apache Synapse supports standard XPath functions and variables through its
underlying XPath
engine. Apart from standard XPath functions, there are several custom XPath
functions
and variables defined by Synapse to retrieve various message context properties.

## <a id="synapse-apache-org-userguide-xpath--Contents"></a>Contents

- [Functions](#synapse-apache-org-userguide-xpath--functions)
  - [get-property](#synapse-apache-org-userguide-xpath--get_prop)
  - [base64Encode](#synapse-apache-org-userguide-xpath--base64_encode)
  - [base64Decode](#synapse-apache-org-userguide-xpath--base64_decode)
  - [url-encode](#synapse-apache-org-userguide-xpath--url_encode)
- [Variables](#synapse-apache-org-userguide-xpath--var)
  - [axis2](#synapse-apache-org-userguide-xpath--axis2)
  - [trp](#synapse-apache-org-userguide-xpath--trp)
  - [ctx](#synapse-apache-org-userguide-xpath--ctx)
  - [url](#synapse-apache-org-userguide-xpath--url)
  - [body](#synapse-apache-org-userguide-xpath--body)
  - [header](#synapse-apache-org-userguide-xpath--header)

<a id="synapse-apache-org-userguide-xpath--functions"></a>

## <a id="synapse-apache-org-userguide-xpath--Custom_Functions"></a>Custom Functions

<a id="synapse-apache-org-userguide-xpath--get_prop"></a>

### <a id="synapse-apache-org-userguide-xpath--get-property_function"></a>get-property function

Get property function retrieves a property from the message context at the given
scope.
If the scope is not specified, property is retrieved from the default synapse
scope.

Syntax:

get-property(String scope, String propertyName)
get-property(String propertyName)

#### <a id="synapse-apache-org-userguide-xpath--Supported_Scopes"></a> Supported Scopes

- default
- axis2
- transport
- registry
- system

#### Default scope

Message context properties residing in Synapse scope can be retrieved from the
default scope. These are the properties directly set on the Synapse MessageContext
instance.
Apart from user defined properties, following special properties can also be
retrieved from the default scope.

| Name | Return Value |
| --- | --- |
| To | Incoming URL as a String or empty string if a To address is                                 not defined. |
| From | From address as a String or empty string if a From address                                 is not defined |
| Action | SOAP Action header value as a String or empty string                                 if a Action is not defined |
| FaultTo | SOAP FautTo header value as a String or empty string if a                                 FaultTo address is not defined |
| ReplyTo | ReplyTo header value as a String or empty string if a                                 ReplyTo address is not defined |
| MessageID | A unique identifier (UUID) for the message as a String .                                 This id is guaranteed to be unique. |
| FAULT | TRUE if the message has a fault or empty string if message doesn't                                 have a                                 fault |
| MESSAGE_FORMAT | Returns pox, get, soap11, soap12 depending on the message. If a                                 message type                                 is unknown this returns soap12 |
| OperationName | Operation name corresponding to the message. |

#### Axis2 scope

Message context properties residing in axis2 scope can be retrieved from the axis2
scope. These are the properties set on the underlying Axis2 MessageContext object.

#### Transport scope

Message context properties residing in transport scope can be retrieved from the
transport scope. These are the transport headers set on the MessageContext.

#### Registry scope

Properties residing in registry can be retrieved from the registry scope.

#### System scope

Java System properties can be retrieved from the system scope.

<a id="synapse-apache-org-userguide-xpath--base64_encode"></a>

### <a id="synapse-apache-org-userguide-xpath--base64Encode_function"></a>base64Encode function

Returns the base64 encoded value of the argument.

Syntax:

base64Encode(String value)

<a id="synapse-apache-org-userguide-xpath--base64_decode"></a>

### <a id="synapse-apache-org-userguide-xpath--base64Decode_function"></a>base64Decode function

Returns the base64 decoded value of the argument.

Syntax:

base64Decode(String value)

<a id="synapse-apache-org-userguide-xpath--url_encode"></a>

### <a id="synapse-apache-org-userguide-xpath--url-encode_function"></a>url-encode function

Returns the URL encoded value of the argument.

Syntax:

url-encode(String value)

<a id="synapse-apache-org-userguide-xpath--var"></a>

## <a id="synapse-apache-org-userguide-xpath--Variables"></a>Variables

There are several XPath variables supported by Synapse. These are used for
accessing
various
properties from the message context

- $axis2
- $trp
- $ctx
- $url
- $body
- $header

These XPath variables get the properties at various scopes.

#### $ctx

Variable prefix for accessing the MessageContext properties in default scope.

i.e To get the property named 'foo' at the default scope use the following XPath
expression

$ctx:foo

#### $axis2

Variable prefix for accessing the axis2 MessageContext properties

i.e. To get the property named 'messageType' use the following XPath expression

$axis2:messageType

#### $trp

Variable prefix for accessing transport headers of the message

i.e. To get the transport header named Content-Type use the following XPath
expression

$trp:Content-Type

#### $url

Variable prefix for accessing URL parameters of the message

i.e. To get the URL parameter named 'bar' use the following XPth expression

$url:bar

#### $body

Get the message body

#### $header

Get the soap header

---

<a id="synapse-apache-org-userguide-samples-sample0"></a>

# Apache Synapse – Apache Synapse - Sample 0

## <a id="synapse-apache-org-userguide-samples-sample0--Sample_0:_Introduction_to_Synapse"></a>Sample 0: Introduction to Synapse

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<!-- log all attributes of messages passing through -->
<log level="full"/>
<!-- Send the message to implicit destination -->
<send/>
<sequence/>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample0--Objective"></a>Objective

Introduction to Synapse - Shows how Synape can be configured to log and pass
messages through.

### <a id="synapse-apache-org-userguide-samples-sample0--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 0 (repository/conf/sample/synapse\_sample\_0.xml)

  Unix/Linux: sh synapse.sh -sample 0  
  Windows: synapse.bat -sample 0

### <a id="synapse-apache-org-userguide-samples-sample0--Executing_the_Client"></a>Executing the Client

#### <a id="synapse-apache-org-userguide-samples-sample0--Smart_Client_Mode"></a>Smart Client Mode

Execute the client in the smart client mode using the following command.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

By tracing the execution of Synapse with the log output level set to DEBUG,
you will see that the client request is arriving at Synapse with a WS-Addressing 'To'
header set to EPR http://localhost:9000/services/SimpleStockQuoteService. The
Synapse engine logs the message at the 'full' log level (i.e. all the
message headers and the body) and then sends it to its explicit 'To'
address which is http://localhost:9000/services/SimpleStockQuoteService.
You will see a log entry in the Axis2 server console confirming that the message
got routed to the sample server and the service hosted at the server generated
a stock quote for the requested symbol.

Sat Nov 18 21:01:23 IST 2006 SimpleStockQuoteService :: Generating quote for : IBM

The response message generated by the service is received by Synapse,
and flows through the same mediation rules, which log the response and
send back to the client. On the client console you should see an output
similar to the following based on the message received by the client.

Standard :: Stock price = $95.26454380258552

#### <a id="synapse-apache-org-userguide-samples-sample0--Proxy_Client_Mode"></a>Proxy Client Mode

Execute the sample Axis2 client as follows to run it in the proxy mode.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dprxurl=http://localhost:8280/

You will see the exact same behaviour as per the previous example when you run
this scenario. However this time the client sends the message to the Axis2 server
using Synapse as a HTTP proxy.

The Axis2 client supports another mode of operation known as the 'dumb client
mode'. This will be addressed in [sample 1](#synapse-apache-org-userguide-samples-sample1).

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample1"></a>

# Apache Synapse – Apache Synapse - Sample 1

## <a id="synapse-apache-org-userguide-samples-sample1--Sample_1:_Simple_Content_Based_Routing_CBR_of_Messages"></a>Sample 1: Simple Content Based Routing (CBR) of Messages

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<!-- filtering of messages with XPath and regex matches -->
<filter source="get-property('To')" regex=".\*/StockQuote.\*">
<then>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
<drop/>
</then>
</filter>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample1--Objective"></a>Objective

Introduction to simple content based routing - Shows how a message could be
made to pass through Synapse using the dumb client mode, where Synapse acts as
a gateway to accept all messages and then perform mediation and routing based
on message properties or content.

### <a id="synapse-apache-org-userguide-samples-sample1--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 1 (repository/conf/sample/synapse\_sample\_1.xml)

  Unix/Linux: sh synapse.sh -sample 1  
  Windows: synapse.bat -sample 1

### <a id="synapse-apache-org-userguide-samples-sample1--Executing_the_Client"></a>Executing the Client

Execute the sample client in the dumb client mode using the following command.

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuote

This time you will see Synapse receiving a message for which Synapse was set
as the ultimate receiver. The filter mediator in the main sequence performs
a regular expression match on the 'To' header (http://localhost:8280/services/StockQuote)
to check whether it matches the expression ".\*/StockQuote.\*". Since the 'To' header
matches this expression the child mediators of the filter mediator get executed.
As a result, the message is sent to the Axis2 server. The drop mediator after
the send mediator terminates the flow of the sequence. Axis2 server will print
the following log when it receives the stock quote request from Synapse.

Sat Nov 18 21:01:23 IST 2006 SimpleStockQuoteService :: Generating quote for : IBM

During response processing, the filter condition fails, and thus the child mediators
of the filter are skipped. The 'send' mediator at the end of the sequence
forwards the response back to the client using the implicit 'To' address.
The client will print a message similar to the following when it receives the
response.

Standard :: Stock price = $95.26454380258552

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample10"></a>

# Apache Synapse – Apache Synapse - Sample 10

## <a id="synapse-apache-org-userguide-samples-sample10--Sample_10:_Introduction_to_Dynamic_Endpoints_with_Registry"></a>Sample 10: Introduction to Dynamic Endpoints with Registry

<definitions xmlns="http://ws.apache.org/ns/synapse">
<registry provider="org.apache.synapse.registry.url.SimpleURLRegistry">
<parameter name="root">file:repository/conf/sample/resources/</parameter>
<parameter name="cachableDuration">15000</parameter>
</registry>
<sequence name="main">
<in>
<send>
<endpoint key="endpoint/dynamic\_endpt\_1.xml"/>
</send>
</in>
<out>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample10--Objective"></a>Objective

Demonstrating the ability to load endpoint definitions dynamically from the
remote registry.

### <a id="synapse-apache-org-userguide-samples-sample10--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start another Axis2 server instance on different ports (use the following command)

  Unix/Linux: sh axis2server.sh -http 9001 -https 9003  
  Windows: axis2server.bat -http 9001 -https 9003
- Start Synapse using the configuration numbered 10 (repository/conf/sample/synapse\_sample\_10.xml)

  Unix/Linux: sh synapse.sh -sample 10  
  Windows: synapse.bat -sample 10

### <a id="synapse-apache-org-userguide-samples-sample10--Executing_the_Client"></a>Executing the Client

This example introduces dynamic endpoints, where the definition of the endpoint
is stored in the registry. To follow this example execute the stock quote client
as 'ant stockquote..' and see that the message is routed to the SimpleStockQuoteService
on the default Axis2 instance on HTTP port 9000.

ant stockquote -Dtrpurl=http://localhost:8280/

Repeat the above command immediately again, and notice that the endpoint is
cached and reused by Synapse - similarly to [sample 8](#synapse-apache-org-userguide-samples-sample8).

Now edit the repository/conf/sample/resources/endpoint/dynamic\_endpt\_1.xml file
and update the address to 'http://localhost:9001/services/SimpleStockQuoteService'.
After the cached value expires, the Registry loads the new definition of the
endpoint, and then the messages are routed to the second sample Axis2 server on
HTTP port 9001.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample100"></a>

# Apache Synapse – Apache Synapse - Sample 100

## <a id="synapse-apache-org-userguide-samples-sample100--Sample_100:_Using_WS-Security_for_Outgoing_Messages"></a>Sample 100: Using WS-Security for Outgoing Messages

<definitions xmlns="http://ws.apache.org/ns/synapse">
<localEntry key="sec\_policy" src="file:repository/conf/sample/resources/policy/policy\_3.xml"/>
<sequence name="main">
<in>
<send>
<endpoint name="secure">
<address uri="http://localhost:9000/services/SecureStockQuoteService">
<enableSec policy="sec\_policy"/>
</address>
</endpoint>
</send>
</in>
<out>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample100--Objective"></a>Objective

Showcase the ability of Synapse to connect to secured endpoints using WS-Security
standards

### <a id="synapse-apache-org-userguide-samples-sample100--Pre-requisites"></a>Pre-requisites

- Download and install the Java Cryptography Extension (JCE) unlimited
  strength policy files for your JDK
- Deploy the SecureStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 100 (repository/conf/sample/synapse\_sample\_100.xml)

  Unix/Linux: sh synapse.sh -sample 100  
  Windows: synapse.bat -sample 100

### <a id="synapse-apache-org-userguide-samples-sample100--Executing_the_Client"></a>Executing the Client

Use the stock quote client to send a request without WS-Security. Synapse is
configured to enable WS-Security as per the policy specified by 'policy\_3.xml'
for the outgoing messages to the SecureStockQuoteService endpoint.
The debug log messages on Synapse shows the encrypted message flowing to the
service and the encrypted response being received by Synapse. The wsse:Security
header is then removed from the decrypted message and the response is delivered
back to the client, as expected. You may execute the client as follows:

ant stockquote -Dtrpurl=http://localhost:8280/

The message sent by Synapse to the secure service can be seen as follows, when
TCPMon is used.

POST http://localhost:9001/services/SecureStockQuoteService HTTP/1.1
Host: 127.0.0.1
SOAPAction: urn:getQuote
Content-Type: text/xml; charset=UTF-8
Transfer-Encoding: chunked
Connection: Keep-Alive
User-Agent: Synapse-HttpComponents-NIO
800
<?xml version='1.0' encoding='UTF-8'?>
<soapenv:Envelope xmlns:xenc="http://www.w3.org/2001/04/xmlenc#" xmlns:wsa="http://www.w3.org/2005/08/addressing" ..>
<soapenv:Header>
<wsse:Security ..>
<wsu:Timestamp ..>
...
</wsu:Timestamp>
<xenc:EncryptedKey..>
...
</xenc:EncryptedKey>
<wsse:BinarySecurityToken ...>
<ds:SignedInfo>
...
</ds:SignedInfo>
<ds:SignatureValue>
...
</ds:SignatureValue>
<ds:KeyInfo Id="KeyId-29551621">
...
</ds:KeyInfo>
</ds:Signature>
</wsse:Security>
<wsa:To>http://localhost:9001/services/SecureStockQuoteService</wsa:To>
<wsa:MessageID>urn:uuid:1C4CE88B8A1A9C09D91177500753443</wsa:MessageID>
<wsa:Action>urn:getQuote</wsa:Action>
</soapenv:Header>
<soapenv:Body xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" wsu:Id="Id-3789605">
<xenc:EncryptedData Id="EncDataId-3789605" Type="http://www.w3.org/2001/04/xmlenc#Content">
<xenc:EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#aes256-cbc" />
<xenc:CipherData>
<xenc:CipherValue>Layg0xQcnH....6UKm5nKU6Qqr</xenc:CipherValue>
</xenc:CipherData>
</xenc:EncryptedData>
</soapenv:Body>
</soapenv:Envelope>0

Note the WS-Security headers and the encrypted payload added by Synapse.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample11"></a>

# Apache Synapse – Apache Synapse - Sample 11

## <a id="synapse-apache-org-userguide-samples-sample11--Sample_11:_A_Full_Registry_based_Configuration_and_Sharing_Configuration_Between_Multiple_Instances"></a>Sample 11: A Full Registry based Configuration, and Sharing Configuration Between Multiple Instances

<definitions xmlns="http://ws.apache.org/ns/synapse">
<registry provider="org.apache.synapse.registry.url.SimpleURLRegistry">
<parameter name="root">file:./repository/conf/sample/resources/</parameter>
<parameter name="cachableDuration">15000</parameter>
</registry>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample11--Objective"></a>Objective

Demonstrating the ability of Synapse to load the entire configuration from
a remote registry. This approach can also be used to share a single configuration
between multiple Synapse instances by pointing all the Synapse instances to the
same URL registry.

### <a id="synapse-apache-org-userguide-samples-sample11--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 11 (repository/conf/sample/synapse\_sample\_11.xml)

  Unix/Linux: sh synapse.sh -sample 11  
  Windows: synapse.bat -sample 11

### <a id="synapse-apache-org-userguide-samples-sample11--Executing_the_Client"></a>Executing the Client

This example shows a full registry based Synapse configuration. This makes it
possible to easily start multiple instances of Synapse sharing a single configuration
in a clustered environment. The Synapse configuration of a given node hosting
Synapse simply points to the registry and looks up the actual configuration by
requesting the key 'synapse.xml'.

(Note: Full registry based configuration is not dynamic atleast for the moment -
i.e. it is not reloading itself)

Invoke the client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

Synapse will generate the following log output.

[HttpServerWorker-1] INFO LogMediator - message = This is a dynamic Synapse configuration

The actual synapse.xml loaded from the registry is:

<!-- a registry based Synapse configuration -->
<definitions xmlns="http://synapse.apache.org/ns/2010/04/configuraiton">
<log level="custom">
<property name="message" value="This is a dynamic Synapse configuration $$$"/>
</log>
<send/>
</definitions>

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample12"></a>

# Apache Synapse – Apache Synapse - Sample 12

## <a id="synapse-apache-org-userguide-samples-sample12--Sample_12:_One-way_Messaging__Fire-and-Forget_Through_Synapse"></a>Sample 12: One-way Messaging / Fire-and-Forget Through Synapse

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<!-- filtering of messages with XPath and regex matches -->
<filter source="get-property('To')" regex=".\*/StockQuote.\*">
<then>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
<drop/>
</then>
</filter>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample12--Objective"></a>Objective

Demonstrate the ability to perform one-way invocations (out-only) through
Synapse.

### <a id="synapse-apache-org-userguide-samples-sample12--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- This sample makes use of the configuration used in [sample 1](#synapse-apache-org-userguide-samples-sample1).
  So start Synapse using the configuration numbered 1 (repository/conf/sample/synapse\_sample\_1.xml)

  Unix/Linux: sh synapse.sh -sample 1  
  Windows: synapse.bat -sample 1

### <a id="synapse-apache-org-userguide-samples-sample12--Executing_the_Client"></a>Executing the Client

In this example we will invoke the one-way 'placeOrder' operation on the
SimpleStockQuoteService using the sample client which uses the Axis2
ServiceClient.fireAndForget() API. To send a placeOrder request from the sample
client, execute the following command.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=placeorder

Going through the Axis2 server logs, you will notice the following entry, which
confirms that Axis2 accepted the in-only request.

SimpleStockQuoteService :: Accepted order for : 7482 stocks of IBM at $ 169.27205579038733

If you send your client request through TCPmon, you will notice that the
SimpleStockQuoteService replies to Synapse with a HTTP 202 reply, and that Synapse
in turns replies to the client with a HTTP 202 acknowledgment.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample14"></a>

# Apache Synapse – Apache Synapse - Sample 14

## <a id="synapse-apache-org-userguide-samples-sample14--Sample_14:_Sequences_and_Endpoints_as_local_registry_items"></a>Sample 14: Sequences and Endpoints as local registry items

<definitions xmlns="http://ws.apache.org/ns/synapse"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:schemaLocation="http://ws.apache.org/ns/synapse http://synapse.apache.org/ns/2010/04/configuration/synapse\_config.xsd">
<localEntry key="local-enrty-ep-key"
src="file:repository/conf/sample/resources/endpoint/dynamic\_endpt\_1.xml"/>
<localEntry key="local-enrty-sequence-key">
<sequence name="dynamic\_sequence">
<log level="custom">
<property name="message" value="\*\*\* Test Message 1 \*\*\*"/>
</log>
</sequence>
</localEntry>
<sequence name="main">
<in>
<sequence key="local-enrty-sequence-key"/>
<send>
<endpoint key="local-enrty-ep-key"/>
</send>
</in>
<out>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample14--Objective"></a>Objective

Objective: Sequence and Endpoints as local registry entries

### <a id="synapse-apache-org-userguide-samples-sample14--Pre-requisites"></a>Pre-requisites

- Start the Synapse configuration numbered 14: i.e. synapse
  -sample 14
- Start the Axis2 server and deploy the SimpleStockQuoteService if
  not already done

### <a id="synapse-apache-org-userguide-samples-sample14--Executing_the_Client"></a>Executing the Client

Execute the client as follows.

ant stockquote -Dtrpurl=http://localhost:8280/

This example shows sequences and endpoints fetched from local
registry. Thus it is possible to have endpoints sequences as
local registry entries including file entries.Execute the
following command to see the sample working, where you will be
able to see the log statement from the fetched sequence from the
local entry and the endpoint will be fetched from the specified
file at runtime and be cached in the system

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample15"></a>

# Apache Synapse – Apache Synapse - Sample 15

## <a id="synapse-apache-org-userguide-samples-sample15--Sample_15:_Message_Copying_and_Content_Enriching_with_Enrich_Mediator"></a>Sample 15: Message Copying and Content Enriching with Enrich Mediator

<definitions xmlns="http://synapse.apache.org/ns/2010/04/configuration">
<sequence name="main">
<in>
<enrich>
<source type="custom"
xpath="//m0:getQuote/m0:request/m0:symbol/text()"
xmlns:m0="http://services.samples"/>
<target type="property" property="ORIGINAL\_REQ"/>
</enrich>
<enrich>
<source type="body"/>
<target type="property" property="REQUEST\_PAYLOAD"/>
</enrich>
<enrich>
<source type="inline" key="init\_req"/>
<target xmlns:m0="http://services.samples"
xpath="//m0:getQuote/m0:request/m0:symbol/text()"/>
</enrich>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
<drop/>
</in>
<out>
<header xmlns:urn="http://synapse.apache.org" name="urn:lastTradeTimestamp" value="foo"/>
<enrich>
<source type="custom"
xpath="//ns:getQuoteResponse/ns:return/ax21:lastTradeTimestamp"
xmlns:ns="http://services.samples"
xmlns:ax21="http://services.samples/xsd"/>
<target xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:urn="http://synapse.apache.org"
xpath="/soapenv:Envelope/soapenv:Header/urn:lastTradeTimestamp"/>
</enrich>
<log level="full"/>
<log>
<property name="Original Request Symbol" expression="get-property('ORIGINAL\_REQ')"/>
<property name="Request Payload" expression="get-property('REQUEST\_PAYLOAD')"/>
</log>
<send/>
</out>
</sequence>
<localEntry key="init\_req">MSFT</localEntry>
<localEntry key="price\_req">
<m0:symbol xmlns:m0="http://services.samples">MSFT</m0:symbol>
</localEntry>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample15--Objective"></a>Objective

Objective: Introduction to Message Copying and Content Enriching with Enrich Mediator

### <a id="synapse-apache-org-userguide-samples-sample15--Pre-requisites"></a>Pre-requisites

- Start the Synapse configuration numbered 15: i.e. synapse -sample 15
- Start the Axis2 server and deploy the SimpleStockQuoteService if
  not already done

### <a id="synapse-apache-org-userguide-samples-sample15--Executing_the_Client"></a>Executing the Client

Execute the client as follows.

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuote

This sample demonstrate the various capabilities of Enrich Mediator. Inside the in-sequence we store/copy different
parts of the message to properties and just before sending the message to the StockQuoteService, we modify the
request value based on the local entry value-init\_req. Then in the out-sequence, the enrich mediator is used
to enrich a soap header based on the 'lastTradeTimestamp' value of the response.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample150"></a>

# Apache Synapse – Apache Synapse - Sample 150

## <a id="synapse-apache-org-userguide-samples-sample150--Sample_150:_Introduction_to_Proxy_Services"></a>Sample 150: Introduction to Proxy Services

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy">
<target>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample150--Objective"></a>Objective

Introduce the concept of proxy services in Synapse

### <a id="synapse-apache-org-userguide-samples-sample150--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 150 (repository/conf/sample/synapse\_sample\_150.xml)

  Unix/Linux: sh synapse.sh -sample 150  
  Windows: synapse.bat -sample 150

### <a id="synapse-apache-org-userguide-samples-sample150--Executing_the_Client"></a>Executing the Client

Execute the stock quote client as follows and invoke the proxy service:

ant stockquote -Daddurl=http://localhost:8280/services/StockQuoteProxy

The 'inSequence' or 'endpoint' or both of these would decide how the message
would be handled after the proxy service receives the message. In the above
example, the request received is forwarded to the sample service hosted on Axis2.
The 'outSequence' defines how the response is handled before it is sent back to
the client. By default, a proxy service is exposed over all transportss
configured for Synapse, unless these are specifically mentioned through the
'transports' attribute.

You can also view the WSDL of the proxy service by launching a web browser and
navigating to the following URL:

http://localhost:8280/services/StockQuoteProxy?wsdl

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample151"></a>

# Apache Synapse – Apache Synapse - Sample 151

## <a id="synapse-apache-org-userguide-samples-sample151--Sample_151:_Custom_Sequences_and_Endpoints_with_Proxy_Services"></a>Sample 151: Custom Sequences and Endpoints with Proxy Services

<definitions xmlns="http://ws.apache.org/ns/synapse">
<localEntry key="proxy\_wsdl"
src="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
<endpoint name="proxy\_2\_endpoint">
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<sequence name="proxy\_1">
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</sequence>
<sequence name="out">
<send/>
</sequence>
<proxy name="StockQuoteProxy1">
<target inSequence="proxy\_1" outSequence="out"/>
<publishWSDL key="proxy\_wsdl"/>
</proxy>
<proxy name="StockQuoteProxy2">
<target endpoint="proxy\_2\_endpoint" outSequence="out"/>
<publishWSDL key="proxy\_wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample151--Objective"></a>Objective

Demonstrate how to use predefined endpoints and sequences in a proxy service

### <a id="synapse-apache-org-userguide-samples-sample151--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 151 (repository/conf/sample/synapse\_sample\_151.xml)

  Unix/Linux: sh synapse.sh -sample 151  
  Windows: synapse.bat -sample 151

### <a id="synapse-apache-org-userguide-samples-sample151--Executing_the_Client"></a>Executing the Client

This configuration creates two proxy services. The first proxy service
'StockQuoteProxy1' uses the sequence named 'proxy\_1' to process incoming
messages and the sequence named 'out' to process outgoing responses. The second
proxy service, 'StockQuoteProxy2' is set to directly forward messages to the
endpoint named 'proxy\_2\_endpoint' without any mediation.

You can send a stock quote request to each of these proxy services and receive
the reply generated by the actual service hosted on the Axis2 server instance.

ant stockquote -Daddurl=http://localhost:8280/services/StockQuoteProxy1
  
ant stockquote -Daddurl=http://localhost:8280/services/StockQuoteProxy2

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample152"></a>

# Apache Synapse – Apache Synapse - Sample 152

## <a id="synapse-apache-org-userguide-samples-sample152--Sample_152:_Switching_Transports_and_Message_Format_from_SOAP_to_RESTPOX"></a>Sample 152: Switching Transports and Message Format from SOAP to REST/POX

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy" transports="https">
<target>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService" format="pox"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample152--Objective"></a>Objective

Demonstrate implementing simple transport switching and message format switching
scenarios using proxy services

### <a id="synapse-apache-org-userguide-samples-sample152--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 152 (repository/conf/sample/synapse\_sample\_152.xml)

  Unix/Linux: sh synapse.sh -sample 152  
  Windows: synapse.bat -sample 152

### <a id="synapse-apache-org-userguide-samples-sample152--Executing_the_Client"></a>Executing the Client

This configuration demonstrates how a proxy service can be exposed on a subset
of available transports, and how it could switch from one transport to another.
This example exposes the created proxy service only on HTTPS, and thus if the
user tries to access it over HTTP, it would result in a fault.

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuoteProxy
...
[java] org.apache.axis2.AxisFault: The service cannot be found for the endpoint reference (EPR) /soap/StockQuoteProxy

Accessing this over HTTPS causes the proxy service to access the SimpleStockQuoteService
on the sample Axis2 server using REST/POX.

ant stockquote -Dtrpurl=https://localhost:8243/services/StockQuoteProxy

TCPMon can be used to trace the actual REST/POX messages exchanged between Synapse
and the sample Axis2 server. Synapse converts the POX response back to SOAP before
sending it back to the client.

POST /services/SimpleStockQuoteService HTTP/1.1
Host: 127.0.0.1
SOAPAction: urn:getQuote
Content-Type: application/xml; charset=UTF-8;action="urn:getQuote";
Transfer-Encoding: chunked
Connection: Keep-Alive
User-Agent: Synapse-HttpComponents-NIO
75
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>IBM</m0:symbol>
</m0:request>
</m0:getQuote>

HTTP/1.1 200 OK
Content-Type: application/xml; charset=UTF-8;action="http://services.samples/SimpleStockQuoteServicePortType/getQuoteResponse";
Date: Tue, 24 Apr 2007 14:42:11 GMT
Server: Synapse-HttpComponents-NIO
Transfer-Encoding: chunked
Connection: Keep-Alive
2b3
<ns:getQuoteResponse xmlns:ns="http://services.samples">
<ns:return>
<ns:change>3.7730036841862384</ns:change>
<ns:earnings>-9.950236235550818</ns:earnings>
<ns:high>-80.23868444613285</ns:high>
<ns:last>80.50750970812187</ns:last>
<ns:lastTradeTimestamp>Tue Apr 24 20:42:11 LKT 2007</ns:lastTradeTimestamp>
<ns:low>-79.67368355714606</ns:low>
<ns:marketCap>4.502043663670823E7</ns:marketCap>
<ns:name>IBM Company</ns:name>
<ns:open>-80.02229531286982</ns:open>
<ns:peRatio>25.089295161182022</ns:peRatio>
<ns:percentageChange>4.28842665653824</ns:percentageChange>
<ns:prevClose>87.98107059692451</ns:prevClose>
<ns:symbol>IBM</ns:symbol>
<ns:volume>19941</ns:volume>
</ns:return>
</ns:getQuoteResponse>

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample153"></a>

# Apache Synapse – Apache Synapse - Sample 153

## <a id="synapse-apache-org-userguide-samples-sample153--Sample_153:_Routing_the_Messages_without_Processing_the_Security_Headers"></a>Sample 153: Routing the Messages without Processing the Security Headers

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy">
<target>
<inSequence>
<property name="preserveProcessedHeaders" value="true"/>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SecureStockQuoteService"/>
</endpoint>
</send>
</inSequence>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample153--Objective"></a>Objective

Demonstrate the ability of Synapse to pass SOAP messages through without removing
already processed headers

### <a id="synapse-apache-org-userguide-samples-sample153--Pre-requisites"></a>Pre-requisites

- Download and install the Java Cryptography Extension (JCE) unlimited
  strength policy files for your JDK
- Deploy the SecureStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 153 (repository/conf/sample/synapse\_sample\_153.xml)

  Unix/Linux: sh synapse.sh -sample 153  
  Windows: synapse.bat -sample 153

### <a id="synapse-apache-org-userguide-samples-sample153--Executing_the_Client"></a>Executing the Client

In this sample the proxy service will receive secured messages with security
headers which are flagged 'MustUnderstand'. But since the element 'enableSec'
is not present in the proxy configuration, Synapse will not engage Apache Rampart
on this proxy service. It is expected that a MustUnderstand failure exception
on the AxisEngine would occur before the message arrives at mediation engine.
But Synapse handles this message and gets it through by setting all the
MustUnderstand headers which are not processed as processed. This will enable
Synapse to route the messages without reading the Security headers (just routing
the messages from client to service, both of which are secured). To execute the
client, send a stock quote request to the proxy service, and sign and encrypt
the request by specifying the client side security policy as follows:

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuoteProxy -Dpolicy=./../../repository/conf/sample/resources/policy/client\_policy\_3.xml

By following through the debug logs or TCPMon output, you can see that the
request received by the proxy service was signed and encrypted. Also, looking
up the WSDL of the proxy service by requesting the URL http://localhost:8280/services/StockQuoteProxy?wsdl
reveals that the security policy attachments are not there and security is not engaged.
When sending the message to the backend service, you can verify that the security
headers were there as in the original message to Synapse from client, and that
the response received does use WS-Security, and forwarded back to the client
without any modification. You should note that this won't be a security hole
because the message inside Synapse is signed and encrypted and can only be
forwarded to a secure service.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample154"></a>

# Apache Synapse – Apache Synapse - Sample 154

## <a id="synapse-apache-org-userguide-samples-sample154--Sample_154:_Load_Balancing_with_Proxy_Services"></a>Sample 154: Load Balancing with Proxy Services

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="LBProxy" transports="http" startOnLoad="true">
<target faultSequence="errorHandler">
<inSequence>
<send>
<endpoint>
<session type="simpleClientSession"/>
<loadbalance algorithm="org.apache.synapse.endpoints.algorithms.RoundRobin">
<endpoint>
<address uri="http://localhost:9001/services/LBService1">
<enableAddressing/>
<suspendDurationOnFailure>20</suspendDurationOnFailure>
</address>
</endpoint>
<endpoint>
<address uri="http://localhost:9002/services/LBService1">
<enableAddressing/>
<suspendDurationOnFailure>20</suspendDurationOnFailure>
</address>
</endpoint>
<endpoint>
<address uri="http://localhost:9003/services/LBService1">
<enableAddressing/>
<suspendDurationOnFailure>20</suspendDurationOnFailure>
</address>
</endpoint>
</loadbalance>
</endpoint>
</send>
<drop/>
</inSequence>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_2.wsdl"/>
</proxy>
<sequence name="errorHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason value="COULDN'T SEND THE MESSAGE TO THE SERVER."/>
</makefault>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample154--Objective"></a>Objective

Demonstrate how to use a proxy service as a load balancer

### <a id="synapse-apache-org-userguide-samples-sample154--Pre-requisites"></a>Pre-requisites

- Deploy the LoadbalanceFailoverService in the sample Axis2 server (go to
  samples/axis2Server/src/LoadbalanceFailoverService and run 'ant')
- Start 3 instances of the Axis2 server on different ports as follows

  ./axis2server.sh -http 9001 -https 9005 -name MyServer1  
  ./axis2server.sh -http 9002 -https 9006 -name MyServer2  
  ./axis2server.sh -http 9003 -https 9007 -name MyServer3
- Start Synapse using the configuration numbered 154 (repository/conf/sample/synapse\_sample\_154.xml)

  Unix/Linux: sh synapse.sh -sample 154  
  Windows: synapse.bat -sample 154

### <a id="synapse-apache-org-userguide-samples-sample154--Executing_the_Client"></a>Executing the Client

This sample is similar to [sample 54](#synapse-apache-org-userguide-samples-sample54). The only
notable difference is the use of a proxy service.

Execute the client as follows.

ant loadbalancefailover -Dmode=session -Dtrpurl=http://localhost:8280/services/LBProxy

You will get an output similar to the following.

[java] Request: 1 Session number: 1 Response from server: MyServer3
[java] Request: 2 Session number: 2 Response from server: MyServer2
[java] Request: 3 Session number: 0 Response from server: MyServer1
[java] Request: 4 Session number: 2 Response from server: MyServer2
[java] Request: 5 Session number: 1 Response from server: MyServer3
[java] Request: 6 Session number: 2 Response from server: MyServer2
[java] Request: 7 Session number: 2 Response from server: MyServer2
[java] Request: 8 Session number: 1 Response from server: MyServer3
[java] Request: 9 Session number: 0 Response from server: MyServer1
[java] Request: 10 Session number: 0 Response from server: MyServer1
...

You can see that session ID 0 is always directed to the server named MyServer1.
That means session ID 0 is bound to MyServer1. Similarly session 1 and 2 are bound
to MyServer3 and MyServer2 respectively.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample155"></a>

# Apache Synapse – Apache Synapse - Sample 155

## <a id="synapse-apache-org-userguide-samples-sample155--Sample_155:_Dual_Channel_Invocation_on_Client_Side_and_Server_Side"></a>Sample 155: Dual Channel Invocation on Client Side and Server Side

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy">
<target>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService">
<enableAddressing separateListener="true"/>
</address>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample155--Objective"></a>Objective

[Sample 13](#synapse-apache-org-userguide-samples-sample13) show cased how to perform dual channel
invocations on the client side. This sample demonstrates how to perform dual
channel invocations on both client side and server side, using proxy services.

### <a id="synapse-apache-org-userguide-samples-sample155--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 155 (repository/conf/sample/synapse\_sample\_155.xml)

  Unix/Linux: sh synapse.sh -sample 155  
  Windows: synapse.bat -sample 155

### <a id="synapse-apache-org-userguide-samples-sample155--Executing_the_Client"></a>Executing the Client

This sample will show the action of the dual channel invocation between client
and Synapse as well as between Synapse and the Axis2 server. Note that if you
want to enable dual channel invocation you need to set the separateListener
attribute to true on the enableAddressing element of the endpoint.

Execute the stock quote client in the dual channel mode as follows:

ant stockquote -Daddurl=http://localhost:8280/services/StockQuoteProxy -Dmode=dualquote

In the above example, the request received is forwarded to the sample service
hosted on Axis2 and the endpoint specifies to enable addressing and do the
invocation in dual channel mode. If you observe the message flow using TCPmon,
you will see that on the channel you send the request to Synapse, the response has
been written as HTTP 202 Accepted, where as the real response from Synapse
comes over a different channel which cannot be obsesrved unless you use tcpdump
to dump all the TCP level messages.

At the same time you can observe the behaviour of the invocation between Synapse
and the actual Axis2 service, where you can see a 202 Accepted message being
delivered to Synapse as the response to the request. The actual response will be
delivered to Synapse over a different channel.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample156"></a>

# Apache Synapse – Apache Synapse - Sample 156

## <a id="synapse-apache-org-userguide-samples-sample156--Sample_156:_Service_Integration_with_Specifying_the_Receiving_Sequence"></a>Sample 156: Service Integration with Specifying the Receiving Sequence

<definitions xmlns="http://ws.apache.org/ns/synapse">
<localEntry key="sec\_policy" src="file:repository/conf/sample/resources/policy/policy\_3.xml"/>
<proxy name="StockQuoteProxy">
<target>
<inSequence>
<enrich>
<source type="body"/>
<target type="property" property="REQUEST"/>
</enrich>
<send receive="SimpleServiceSeq">
<endpoint name="secure">
<address uri="http://localhost:9000/services/SecureStockQuoteService">
<enableSec policy="sec\_policy"/>
</address>
</endpoint>
</send>
</inSequence>
<outSequence>
<drop/>
</outSequence>
</target>
</proxy>
<sequence name="SimpleServiceSeq">
<property name="SECURE\_SER\_AMT" expression="//ns:getQuoteResponse/ns:return/ns:last"
xmlns:ns="http://services.samples"/>
<log level="custom">
<property name="SecureStockQuoteService-Amount" expression="get-property('SECURE\_SER\_AMT')"/>
</log>
<enrich>
<source type="body"/>
<target type="property" property="SecureService\_Res"/>
</enrich>
<enrich>
<source type="property" property="REQUEST"/>
<target type="body"/>
</enrich>
<send receive="ClientOutSeq">
<endpoint name="SimpleStockQuoteService">
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</sequence>
<sequence name="ClientOutSeq">
<property name="SIMPLE\_SER\_AMT" expression="//ns:getQuoteResponse/ns:return/ns:last"
xmlns:ns="http://services.samples"/>
<log level="custom">
<property name="SimpleStockQuoteService-Amount" expression="get-property('SIMPLE\_SER\_AMT')"/>
</log>
<enrich>
<source type="body"/>
<target type="property" property="SimpleService\_Res"/>
</enrich>
<filter xpath="fn:number(get-property('SIMPLE\_SER\_AMT')) > fn:number(get-property('SECURE\_SER\_AMT'))">
<then>
<log>
<property name="StockQuote" value="SecureStockQuoteService"/>
</log>
<enrich>
<source type="property" property="SecureService\_Res"/>
<target type="body"/>
</enrich>
</then>
<else>
<log>
<property name="StockQuote" value="SimpleStockQuoteService"/>
</log>
</else>
</filter>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample156--Objective"></a>Objective

Synapse is capable of mediating requests among multiple services and managing
complex message flows thereby acting as a lightweight orchestration engine. This
sample demonstrates how to easily integrate multiple services with Synapse using
the 'receiving sequence' feature of Synapse.

### <a id="synapse-apache-org-userguide-samples-sample156--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server
- Deploy the SecureStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 156 (repository/conf/sample/synapse\_sample\_156.xml)

  Unix/Linux: sh synapse.sh -sample 156  
  Windows: synapse.bat -sample 156

### <a id="synapse-apache-org-userguide-samples-sample156--Executing_the_Client"></a>Executing the Client

This sample includes a proxy service which first forwards the client request to
the SecureStockQuoteService. Once a response has been received from this service,
Synapse will turn around and invoke the SimpleStockQuoteService. To do this proxy
service must hold on to the original request in memory. This is done using an
enrich mediator. Once Synapse has received a response from the SimpleStockQuoteService
it will compare the two responses received from the two services and select the
one with the lower stock quote value. This response will be then sent to the
client.

The important feature to note here is the 'receive' attribute set on the 'send'
mediators. This tells Synapse that responses of those send operations should be
directed to the sequences referred by the 'receive' attribute. Therefore the
response from the SecureStockQuoteService is directed to the sequence named
'SimpleServiceSeq'. Similarly the response from the SimpleStockQuoteService will
be handled by the sequence named 'ClientOutSeq'.

To try this out, execute the stock quote client as follows:

ant stockquote -Daddurl=http://localhost:8280/services/StockQuoteProxy

You can confirm that Synapse invokes both services by going through the console
output of the Axis2 server. However Axis2 client will receive only one response
back. As far as the client is concerned, only one HTTP transaction is taking place.
But Synapse does multiple service invocations with the back-end to make the whole
integration scenario tick.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample157"></a>

# Apache Synapse – Apache Synapse - Sample 157

## <a id="synapse-apache-org-userguide-samples-sample157--Sample_157:_Conditional_Router_Mediator_for_Implementing_Complex_Routing_Scenarios"></a>Sample 157: Conditional Router Mediator for Implementing Complex Routing Scenarios

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy" transports="https http" startOnLoad="true" trace="disable">
<target>
<inSequence>
<conditionalRouter continueAfter="false">
<conditionalRoute breakRoute="false">
<condition>
<match xmlns="" type="header" source="foo" regex="bar.\*"/>
</condition>
<target sequence="cnd1\_seq"/>
</conditionalRoute>
<conditionalRoute breakRoute="false">
<condition>
<and xmlns="">
<match type="header" source="my\_custom\_header1" regex="foo.\*"/>
<match type="url" regex="/services/StockQuoteProxy.\*"/>
</and>
</condition>
<target sequence="cnd2\_seq"/>
</conditionalRoute>
<conditionalRoute breakRoute="false">
<condition>
<and xmlns="">
<match type="header" source="my\_custom\_header2" regex="bar.\*"/>
<equal type="param" source="qparam1" value="qpv\_foo"/>
<or>
<match type="url" regex="/services/StockQuoteProxy.\*"/>
<match type="header" source="my\_custom\_header3" regex="foo.\*"/>
</or>
<not>
<equal type="param" source="qparam2" value="qpv\_bar"/>
</not>
</and>
</condition>
<target sequence="cnd3\_seq"/>
</conditionalRoute>
</conditionalRouter>
</inSequence>
<outSequence>
<send/>
</outSequence>
</target>
</proxy>
<sequence name="cnd1\_seq">
<log level="custom">
<property name="MSG\_FLOW" value="Condition (I) Satisfied"/>
</log>
<sequence key="send\_seq"/>
</sequence>
<sequence name="cnd2\_seq">
<log level="custom">
<property name="MSG\_FLOW" value="Condition (II) Satisfied"/>
</log>
<sequence key="send\_seq"/>
</sequence>
<sequence name="cnd3\_seq">
<log level="custom">
<property name="MSG\_FLOW" value="Condition (III) Satisfied"/>
</log>
<sequence key="send\_seq"/>
</sequence>
<sequence name="send\_seq">
<log level="custom">
<property name="DEBUG" value="Condition Satisfied"/>
</log>
<send>
<endpoint name="simple">
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample157--Objective"></a>Objective

Conditional router mediator can be used to implement complex routing rules in
Synapse. It can route messages to various endpoints based on URLs, query parameters
and transport headers. This sample demonstrates how to use the conditional router
mediator within a proxy service to build a smart routing proxy.

### <a id="synapse-apache-org-userguide-samples-sample157--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server a d start
  Axis2 server.
- Start Synapse using the configuration numbered 157 (repository/conf/sample/synapse\_sample\_157.xml)

  Unix/Linux: sh synapse.sh -sample 157  
  Windows: synapse.bat -sample 157

### <a id="synapse-apache-org-userguide-samples-sample157--Executing_the_Client"></a>Executing the Client

We will be using 'curl' as the client in this scenario. [Curl](http://curl.haxx.se/)
is a neat little command line tool that can be used to generate various types
of HTTP requests (among other things).

First create a sample input file named stockQuoteReq.xml with the following
content.

<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope" xmlns:ser="http://services.samples">
<soap:Header/>
<soap:Body>
<ser:getQuote>
<ser:request>
<ser:symbol>IBM</ser:symbol>
</ser:request>
</ser:getQuote>
</soap:Body>
</soap:Envelope>

Invoke curl as follows to see header based routing feature in action.

curl -d @stockQuoteReq.xml -H "Content-Type: application/soap+xml;charset=UTF-8" -H "foo:bar" "http://localhost:8280/services/StockQuoteProxy"

This sends a HTTP request with a custom header named 'foo'. Proxy service will
detect this header and print a custom log message confirming the receipt of the
request.

Now invoke curl as follows to test a combination header and URL based routing.

curl -d @stockQuoteReq.xml -H "Content-Type: application/soap+xml;charset=UTF-8" -H "my\_custom\_header1:foo1" "http://localhost:8280/services/StockQuoteProxy"

Finally invoke curl as follows to test routing based on complex conditions.

curl -d @stockQuoteReq.xml -H "Content-Type: application/soap+xml;charset=UTF-8" -H "my\_custom\_header2:bar" -H "my\_custom\_header3:foo" "http://localhost:8280/services/StockQuoteProxy?qparam1=qpv\_foo&qparam2=qpv\_foo2"

In each case Synapse will log a different log entry because the conditional router
mediator uses different sequences to process the three messages.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample158"></a>

# Apache Synapse – Apache Synapse - Sample 158

## <a id="synapse-apache-org-userguide-samples-sample158--Sample_158:_Exposing_a_SOAP_service_over_JSON"></a>Sample 158: Exposing a SOAP service over JSON

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="JSONProxy" transports="http https">
<target>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService" format="soap11"/>
</endpoint>
<inSequence>
<log level="full"/>
<xslt key="in\_transform"/>
</inSequence>
<outSequence>
<log level="full"/>
<xslt key="out\_transform"/>
<send/>
</outSequence>
</target>
</proxy>
<localEntry key="in\_transform">
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
xmlns:fn="http://www.w3.org/2005/02/xpath-functions"
xmlns:m0="http://services.samples" version="2.0" exclude-result-prefixes="m0 fn">
<xsl:output method="xml" omit-xml-declaration="yes" indent="yes"/>
<xsl:template match="\*">
<xsl:element name="{local-name()}" namespace="http://services.samples">
<xsl:copy-of select="attribute::\*"/>
<xsl:apply-templates/>
</xsl:element>
</xsl:template>
</xsl:stylesheet>
</localEntry>
<localEntry key="out\_transform">
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">
<xsl:output method="xml" version="1.0" encoding="UTF-8"/>
<xsl:template match="\*">
<xsl:element name="{local-name()}">
<xsl:apply-templates/>
</xsl:element>
</xsl:template>
</xsl:stylesheet>
</localEntry>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample158--Objective"></a>Objective

Demonstrate the ability to switch between JSON and XML/SOAP content interchange formats

### <a id="synapse-apache-org-userguide-samples-sample158--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 158 (repository/conf/sample/synapse\_sample\_158.xml)

  Unix/Linux: sh synapse.sh -sample 158  
  Windows: synapse.bat -sample 158

### <a id="synapse-apache-org-userguide-samples-sample158--Executing_the_Client"></a>Executing the Client

ant jsonclient -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/services/JSONProxy

JSON client will send a stockquote request to Synapse using the
JSON content interchange format. Synapse will transform it into a SOAP
request and forward to the Axis2 server. The SOAP response from the
Axis2 server will be converted into a JSON message and sent back to the
JSON client.

You may use a tool like TCPMon to monitor the JSON requests sent
over the wire. A sample JSON request and response is shown below:

{"getQuote":{"request":{"symbol":"IBM"}}}

{"getQuoteResponse":{"return":{"change":3.853593376681722,"earnings":12.802850763714854,"high":67.92488310190126,"last":66.14619264746406,"lastTradeTimestamp":"Mon Aug 23 16:48:40 IST 2010","low":-66.04000424423522,"marketCap":-9334516.42324327,"name":"IBM Company","open":-64.61950137150009,"peRatio":-19.78600441437058,"percentageChange":5.411779328273005,"prevClose":71.2075112994578,"symbol":"IBM","volume":16842}}}

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample16"></a>

# Apache Synapse – Apache Synapse - Sample 16

## <a id="synapse-apache-org-userguide-samples-sample16--Sample_16:_Introduction_to_dynamic_and_static_registry_keys"></a>Sample 16: Introduction to dynamic and static registry keys

<definitions xmlns="http://ws.apache.org/ns/synapse">
<!-- the SimpleURLRegistry allows access to a URL based registry (e.g. file:/// or http://) -->
<registry provider="org.wso2.carbon.mediation.registry.ESBRegistry">
<!-- the root property of the simple URL registry helps resolve a resource URL as root + key -->
<parameter name="root">file:repository/samples/resources/</parameter>
<!-- all resources loaded from the URL registry would be cached for this number of milli seconds -->
<parameter name="cachableDuration">15000</parameter>
</registry>
<sequence name="main">
<in>
<!-- define the request processing XSLT resource as a property value -->
<property name="symbol" value="transform/transform.xslt"/>
<!-- {} denotes that this key is a dynamic one and it is not a static key -->
<!-- use Xpath expression "get-property()" to evaluate real key from property -->
<xslt key="{get-property('symbol')}"/>
</in>
<out>
<!-- transform the standard response back into the custom format the client expects -->
<!-- the key is looked up in the remote registry using a static key -->
<xslt key="transform/transform\_back.xslt"/>
</out>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample16--Objective"></a>Objective

Objective: Introduction to dynamic and static keys

### <a id="synapse-apache-org-userguide-samples-sample16--Pre-requisites"></a>Pre-requisites

- Start the Synapse configuration numbered 16: i.e. synapse -sample 16
- Start the Axis2 server and deploy the SimpleStockQuoteService if
  not already done

### <a id="synapse-apache-org-userguide-samples-sample16--Executing_the_Client"></a>Executing the Client

Execute the client as follows.

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuote

This Sample demonstrates the use of dynamic keys with mediators. XSLT mediator is used as an
example for that and deference between static and dynamic usage of keys are shown with that.

The first registry resource "transform/transform.xslt" is set as a property value. Inside the
XSLT mediator the local property value is lookup using the Xpath expression "get-property()".
Likewise any Xpath expression can be enclosed inside "{ }" to denote that it is a dynamic key.
Then the mediator will evaluate the real value for that expression.

The second XSLT resource "transform/transform\_back.xslt" is used simply as a static key as
usual. It is not included inside "{ }" and because of the mediator directly use the static
value as the key.

Execute the custom quote client as 'ant stockquote -Dmode=customquote' and analys the output
which is similar to the Sample 8.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample17"></a>

# Apache Synapse – Apache Synapse - Sample 17

## <a id="synapse-apache-org-userguide-samples-sample17--Sample_17:_Introduction_to_the_payloadFactory_mediator"></a>Sample 17: Introduction to the payloadFactory mediator

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<!-- using payloadFactory mediator to transform the request message -->
<payloadFactory media-type="xml">
<format>
<m:getQuote xmlns:m="http://services.samples">
<m:request>
<m:symbol>$1</m:symbol>
</m:request>
</m:getQuote>
</format>
<args>
<arg xmlns:m0="http://services.samples" expression="//m0:Code"/></args>
</payloadFactory>
</in>
<out>
<!-- using payloadFactory mediator to transform the response message -->
<payloadFactory media-type="xml">
<format>
<m:CheckPriceResponse xmlns:m="http://services.samples/xsd">
<m:Code>$1</m:Code>
<m:Price>$2</m:Price>
</m:CheckPriceResponse>
</format>
<args>
<arg xmlns:m0="http://services.samples/xsd" expression="//m0:symbol"/>
<arg xmlns:m0="http://services.samples/xsd" expression="//m0:last"/>
</args>
</payloadFactory>
</out>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample17--Objective"></a>Objective

Objective: Introduction to the payloadFactory mediator

### <a id="synapse-apache-org-userguide-samples-sample17--Pre-requisites"></a>Pre-requisites

- Start the Synapse configuration numbered 17: i.e. synapse -sample 17
- Start the Axis2 server and deploy the SimpleStockQuoteService if
  not already done

### <a id="synapse-apache-org-userguide-samples-sample17--Executing_the_Client"></a>Executing the Client

Execute the client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=customquote

This Sample demonstrates how the PayloadFactory Mediator can be used to perform transformations
as an alternative to the XSLT mediator, which is demonstrated in Sample 8: Introduction to
Static and Dynamic Registry Resources and Using XSLT Transformations

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample2"></a>

# Apache Synapse – Apache Synapse - Sample 2

## <a id="synapse-apache-org-userguide-samples-sample2--Sample_2:_CBR_with_Switch_Case_Mediator"></a>Sample 2: CBR with Switch Case Mediator

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<switch source="//m0:getQuote/m0:request/m0:symbol" xmlns:m0="http://services.samples">
<case regex="IBM">
<!-- the property mediator sets a local property on the \*current\* message -->
<property name="symbol" value="Great stock - IBM"/>
</case>
<case regex="MSFT">
<property name="symbol" value="Are you sure? - MSFT"/>
</case>
<default>
<!-- it is possible to assign the result of an XPath expression as well -->
<property name="symbol" expression="fn:concat('Normal Stock - ', //m0:getQuote/m0:request/m0:symbol)"/>
</default>
</switch>
<log level="custom">
<!-- the get-property() XPath extension function allows the lookup of local message properties
as well as properties from the Axis2 or Transport contexts (i.e. transport headers) -->
<property name="symbol" expression="get-property('symbol')"/>
<!-- the get-property() function supports the implicit message headers To/From/Action/FaultTo/ReplyTo -->
<property name="epr" expression="get-property('To')"/>
</log>
<!-- Send the messages where they are destined to (i.e. the 'To' EPR of the message) -->
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample2--Objective"></a>Objective

Introduction to the switch-case mediator and manipulating properties set on the
messages.

### <a id="synapse-apache-org-userguide-samples-sample2--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 2 (repository/conf/sample/synapse\_sample\_2.xml)

  Unix/Linux: sh synapse.sh -sample 2  
  Windows: synapse.bat -sample 2

### <a id="synapse-apache-org-userguide-samples-sample2--Executing_the_Client"></a>Executing the Client

Execute the sample Axis2 client in the smart client using different symbols
such as IBM, MSFT and SUN.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=IBM

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=MSFT

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=SUN

When the symbol IBM is requested, viewing the mediation logs you will see that
the switch mediator's first case for 'IBM' is executed and a local property named
'symbol' is set to 'Great stock - IBM'. Subsequently this local property value
is looked up by the log mediator and logged using the 'get-property()' XPath
extension function.

INFO LogMediator - symbol = Great stock - IBM, epr = http://localhost:9000/axis2/services/SimpleStockQuoteService

Similarly for the symbol 'MSFT' the second case statement in the switch mediator
will be executed which will result in the following log.

INFO LogMediator - symbol = Are you sure? - MSFT, epr = http://localhost:9000/axis2/services/SimpleStockQuoteService

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample200"></a>

# Apache Synapse – Apache Synapse - Sample 200

## <a id="synapse-apache-org-userguide-samples-sample200--Sample_200:_Engaging_WS-Security_on_Proxy_Services"></a>Sample 200: Engaging WS-Security on Proxy Services

<definitions xmlns="http://ws.apache.org/ns/synapse">
<localEntry key="sec\_policy" src="file:repository/conf/sample/resources/policy/policy\_3.xml"/>
<proxy name="StockQuoteProxy">
<target>
<inSequence>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</inSequence>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
<enableSec/>
<policy key="sec\_policy"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample200--Objective"></a>Objective

Demonstrates how to secure a proxy service using WS-Security and WS-Policy
standards

### <a id="synapse-apache-org-userguide-samples-sample200--Pre-requisites"></a>Pre-requisites

- Download and install the Java Cryptography Extension (JCE) unlimited
  strength policy files for your JDK
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 200 (repository/conf/sample/synapse\_sample\_200.xml)

  Unix/Linux: sh synapse.sh -sample 200  
  Windows: synapse.bat -sample 200

### <a id="synapse-apache-org-userguide-samples-sample200--Executing_the_Client"></a>Executing the Client

The proxy service expects to receive a signed and encrypted message as specified
by the security policy. Please see Apache Rampart and Axis2 documentation on the
format of the policy file. The element 'enableSec' specifies that Apache Rampart
should be engaged on this proxy service. Hence if Rampart rejects any request
messages that does not conform to the specified policy, those messages will
never reach the 'inSequence' to be processed. To execute the client, send a stock
quote request to the proxy service, and sign and encrypt the request by specifying
the client side security policy as follows:

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuoteProxy -Dpolicy=./../../repository/conf/sample/resources/policy/client\_policy\_3.xml

By following through the debug logs or TCPMon output, you can see that the
request received by the proxy service is signed and encrypted. Also, looking up
the WSDL of the proxy service by requesting the URL http://localhost:8280/services/StockQuoteProxy?wsdl
reveals that the security policy is attached to the provided base WSDL. When
sending the message to the backend service, you can verify that the security
headers are removed. The response received from Axis2 does not use WS-Security,
but the response forwarded back to the client is signed and encrypted as
expected by the client.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample250"></a>

# Apache Synapse – Apache Synapse - Sample 250

## <a id="synapse-apache-org-userguide-samples-sample250--Sample_250:Introduction_to_Transport_Switching_-_JMS_to_HTTPS"></a>Sample 250:Introduction to Transport Switching - JMS to HTTP/S

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy" transports="jms">
<target>
<inSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
</inSequence>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
<parameter name="transport.jms.ContentType">
<rules>
<jmsProperty>contentType</jmsProperty>
<default>application/xml</default>
</rules>
</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample250--Objective"></a>Objective

Demonstrate the ability of Synapse to perform transport switching (i.e. receiving
messages over one transport and forwarding them over a different transport)

### <a id="synapse-apache-org-userguide-samples-sample250--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Setup and start a JMS broker (Apache ActiveMQ can be used as the
  JMS broker for this scenario. Refer [JMS setup guide](#synapse-apache-org-userguide-samples-setup-jms--pre)
  for information on how to run ActiveMQ.)
- Enable the JMS transport receiver of Synapse (Refer
  [JMS setup guide](#synapse-apache-org-userguide-samples-setup-jms--listener) for more details)
- Start Synapse using the configuration numbered 250 (repository/conf/sample/synapse\_sample\_250.xml)

  Unix/Linux: sh synapse.sh -sample 250  
  Windows: synapse.bat -sample 250

### <a id="synapse-apache-org-userguide-samples-sample250--Executing_the_Client"></a>Executing the Client

In this sample we are using a proxy service exposed over JMS (note the transports=jms
attribute). If you check the WSDL of the proxy service using a web browser you
will notice that it only has JMS endpoints.

Run the sample JMS client by switching to the samples/axis2Client directory and
executing the following command.

ant jmsclient -Djms\_type=pox -Djms\_dest=dynamicQueues/StockQuoteProxy -Djms\_payload=MSFT

This will send a plain XML formatted place order request to a JMS queue named
'StockQuoteProxy'. Synapse will be polling on this queue for any incoming messages
so it will pick up the request. If you run Synapse in the DEBUG mode, following
entry will be printed on the console.

[JMSWorker-1] DEBUG ProxyServiceMessageReceiver -Proxy Service StockQuoteProxy received a new message...

Then Synapse will mediate the request through the service bus and forward it to
the sample Axis2 server over HTTP. Axis2 server will print the following entry
on the console when it receives the request.

Accepted order for : 16517 stocks of MSFT at $ 169.14622538721846

Note that the operation is out-only and no response is sent back to the client.
The transport.jms.ContentType property is necessary to allow the JMS transport
to determine the content type of incoming messages. With the given configuration
it will first try to read the content type from the 'contentType' message property
and fall back to 'application/xml' (i.e. POX) if this property is not set. Note
that the JMS client used in this example doesn't send any content type
information.

It is also important to note that the name of the source JMS queue is same as the
name of the proxy service (StockQuoteProxy). This is the default behavior of
Synapse. Each proxy service by default listens on a JMS queue which has the same
name as the service. It is possible to instruct a JMS proxy service to listen to
an already existing destination without creating a new one. To do this, use the
parameter elements on the proxy service definition to specify the destination
and connection factory information. An example is given below.

<parameter name="transport.jms.Destination">dynamicTopics/something.TestTopic</parameter>

With the above parameter in the proxy configuration, proxy service will listen
on a JMS topic named 'something.TestTopic' for incoming requests.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample251"></a>

# Apache Synapse – Apache Synapse - Sample 251

## <a id="synapse-apache-org-userguide-samples-sample251--Sample_251:_Switching_from_HTTPS_to_JMS"></a>Sample 251: Switching from HTTP/S to JMS

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy" transports="http">
<target>
<endpoint>
<address
uri="jms:/SimpleStockQuoteService?transport.jms.ConnectionFactoryJNDIName=QueueConnectionFactory&java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory&java.naming.provider.url=tcp://localhost:61616&transport.jms.DestinationType=queue"/>
</endpoint>
<inSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
</inSequence>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample251--Objective"></a>Objective

This sample demonstrates receiving messages over HTTP/S and forwarding them to
a JMS queue

### <a id="synapse-apache-org-userguide-samples-sample251--Pre-requisites"></a>Pre-requisites

- Setup and start a JMS broker (Apache ActiveMQ can be used as the
  JMS broker for this scenario. Refer [JMS setup guide](#synapse-apache-org-userguide-samples-setup-jms--pre)
  for information on how to run ActiveMQ.)
- Enable the JMS transport receiver of the sample Axis2 server (Refer
  [JMS setup guide](#synapse-apache-org-userguide-samples-setup-jms--server) for details)
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2 (Since
  the JMS receiver is enabled, Axis2 will start polling on a JMS queue)
- Start Synapse using the configuration numbered 251 (repository/conf/sample/synapse\_sample\_251.xml)

  Unix/Linux: sh synapse.sh -sample 251  
  Windows: synapse.bat -sample 251

### <a id="synapse-apache-org-userguide-samples-sample251--Executing_the_Client"></a>Executing the Client

This Synapse configuration creates a proxy service over HTTP and forwards
received messages to a JMS queue. To test this functionality, send a place order
request to Synapse over HTTP as follows.

ant stockquote -Daddurl=http://localhost:8280/services/StockQuoteProxy -Dmode=placeorder -Dsymbol=MSFT

Note that the target endpoint of the proxy service points to a JMS queue in the
ActiveMQ broker.

jms:/SimpleStockQuoteService?transport.jms.ConnectionFactoryJNDIName=
QueueConnectionFactory&java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory&
java.naming.provider.url=tcp://localhost:61616

The sample Axis2 server will pick up the message from the JMS queue and print the
following log entry.

Accepted order for : 18406 stocks of MSFT at $ 83.58806051152119

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample252"></a>

# Apache Synapse – Apache Synapse - Sample 252

## <a id="synapse-apache-org-userguide-samples-sample252--Sample_252:_Pure_Text_Binary_and_POX_Message_Support_with_JMS"></a>Sample 252: Pure Text, Binary and POX Message Support with JMS

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="text\_proxy">
<log level="full"/>
<header name="Action" value="urn:placeOrder"/>
<script language="js">
var args = mc.getPayloadXML().toString().split(" ");
mc.setPayloadXML(
&lt;placeOrder xmlns="http://services.samples"&gt;
&lt;order&gt;
&lt;price&gt;{args[0]}&lt;/price&gt;
&lt;quantity&gt;{args[1]}&lt;/quantity&gt;
&lt;symbol&gt;{args[2]}&lt;/symbol&gt;
&lt;/order&gt;
&lt;/placeOrder&gt;);
</script>
<property action="set" name="OUT\_ONLY" value="true"/>
<log level="full"/>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</sequence>
<sequence name="mtom\_proxy">
<log level="full"/>
<property action="set" name="OUT\_ONLY" value="true"/>
<header name="Action" value="urn:oneWayUploadUsingMTOM"/>
<send>
<endpoint>
<address uri="http://localhost:9000/services/MTOMSwASampleService" optimize="mtom"/>
</endpoint>
</send>
</sequence>
<sequence name="pox\_proxy">
<property action="set" name="OUT\_ONLY" value="true"/>
<header name="Action" value="urn:placeOrder"/>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"
format="soap11"/>
</endpoint>
</send>
</sequence>
<sequence name="out">
<send/>
</sequence>
<proxy name="JMSFileUploadProxy" transports="jms">
<target inSequence="mtom\_proxy" outSequence="out"/>
<parameter name="transport.jms.ContentType">
<rules>
<bytesMessage>application/octet-stream</bytesMessage>
</rules>
</parameter>
<parameter name="Wrapper">{http://synapse.apache.org/userguide/samples/}element</parameter>
</proxy>
<proxy name="JMSTextProxy" transports="jms">
<target inSequence="text\_proxy" outSequence="out"/>
<parameter name="transport.jms.ContentType">
<rules>
<textMessage>text/plain</textMessage>
</rules>
</parameter>
<parameter name="Wrapper">{http://synapse.apache.org/userguide/samples/}text</parameter>
</proxy>
<proxy name="JMSPoxProxy" transports="jms">
<target inSequence="pox\_proxy" outSequence="out"/>
<parameter name="transport.jms.ContentType">application/xml</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample252--Objective"></a>Objective

Demonstrate the ability of Synapse to receive and mediate plain text, binary and
POX (Plain Old XML) messages over JMS.

### <a id="synapse-apache-org-userguide-samples-sample252--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Setup and start a JMS broker (Apache ActiveMQ can be used as the
  JMS broker for this scenario. Refer [JMS setup guide](#synapse-apache-org-userguide-samples-setup-jms--pre)
  for information on how to run ActiveMQ.)
- Enable the JMS transport receiver of Synapse (Refer
  [JMS setup guide](#synapse-apache-org-userguide-samples-setup-jms--listener) for more details)
- Start Synapse using the configuration numbered 252 (repository/conf/sample/synapse\_sample\_252.xml)

  Unix/Linux: sh synapse.sh -sample 252  
  Windows: synapse.bat -sample 252

### <a id="synapse-apache-org-userguide-samples-sample252--Executing_the_Client"></a>Executing the Client

This configuration creates 3 JMS proxy services named JMSFileUploadProxy,
JMSTextProxy and JMSPoxProxy exposed over JMS queues with the same names as the
services. The first part of this example demonstrates the pure text message
support with JMS, where a user sends a space separated text message over JMS of
the form '<price> <qty> <symbol>'. Synapse converts this message
into a SOAP message and sends this to the placeOrder operation of the SimpleStockQuoteService.
Synapse uses the script mediator to transform the text message into a XML payload
using the JavaScript support available to tokenize the string. The proxy service
property named 'Wrapper' defines a custom wrapper element QName, to be used when
wrapping text/binary content into a SOAP envelope.

Execute JMS client as follows. This will post a pure text JMS message with the
content defined (e.g. '12.33 1000 ACP') to the specified JMS destination -
dynamicQueues/JMSTextProxy.

ant jmsclient -Djms\_type=text -Djms\_payload="12.33 1000 ACP" -Djms\_dest=dynamicQueues/JMSTextProxy

Following the logs, you will notice that Synapse received the JMS text message
and transformed it into a SOAP payload as follows. Notice that the wrapper element
'{http://synapse.apache.org/userguide/samples/}text' has been used to wrap the text message
content.

[jms-Worker-1] INFO LogMediator To: , WSAction: urn:mediate, SOAPAction: urn:mediate, MessageID: ID:orcus.veithen.net-50631-1225235276233-1:0:1:1:1, Direction: request,
Envelope:
<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Body>
<axis2ns1:text xmlns:axis2ns1="http://synapse.apache.org/userguide/samples/">12.33 1000 ACP</axis2ns1:text>
</soapenv:Body>
</soapenv:Envelope>

Then you can see how the script mediator creates a stock quote request by tokenizing
the text as follows.

[jms-Worker-1] INFO LogMediator To: , WSAction: urn:placeOrder, SOAPAction: urn:placeOrder, MessageID: ID:orcus.veithen.net-50631-1225235276233-1:0:1:1:1, Direction: request,
Envelope:
<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Body>
<placeOrder xmlns="http://services.samples">
<order>
<price>12.33</price>
<quantity>1000</quantity>
<symbol>ACP</symbol>
</order>
</placeOrder>
</soapenv:Body>
</soapenv:Envelope>

This SOAP message is then sent to the SimpleStockQuoteService on the sample
Axis2 server. The sample Axis2 server will accept the one-way message and print
the following log:

samples.services.SimpleStockQuoteService :: Accepted order for : 1000 stocks of ACP at $ 12.33

The next section of this example demonstrates how a pure binary JMS message can
be received and processed through Synapse. The configuration creates a proxy
service named 'JMSFileUploadProxy' that accepts binary messages and wraps them
into a custom element '{http://synapse.apache.org/userguide/samples/}element'. The received
message is then forwarded to the MTOMSwASampleService using the SOAP action
'urn:oneWayUploadUsingMTOM' while optimizing binary content using MTOM. To execute
this sample, use the JMS client to publish a pure binary JMS message containing
the file './../../repository/conf/sample/resources/mtom/asf-logo.gif' to the JMS
destination 'dynamicQueues/JMSFileUploadProxy' as follows:

ant jmsclient -Djms\_type=binary -Djms\_dest=dynamicQueues/JMSFileUploadProxy \
-Djms\_payload=./../../repository/conf/sample/resources/mtom/asf-logo.gif

Examining the Synapse logs will reveal that the binary content was received
over JMS and wrapped with the specified element into a SOAP infoset as follows:

[jms-Worker-1] INFO LogMediator To: , WSAction: urn:mediate, SOAPAction: urn:mediate, MessageID: ID:orcus.veithen.net-50702-1225236039556-1:0:1:1:1, Direction: request,
Envelope:
<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Body>
<axis2ns1:element xmlns:axis2ns1="http://synapse.apache.org/userguide/samples/">R0lGODlhgw...AAOw==</axis2ns1:element>
</soapenv:Body>
</soapenv:Envelope>

Thereafter the message is sent as a MTOM optimized message as specified by the
'format=mtom' attribute of the endpoint, to the MTOMSwASampleService using the
SOAP action 'urn:oneWayUploadUsingMTOM'. Once received by the sample service,
it is saved into a temporary file and could be verified for correctness.

Wrote to file : ./../../work/temp/sampleServer/mtom-4417.gif

The final section of this example shows how a POX JMS message is received by Synapse
and sent to the SimpleStockQuoteService as a SOAP message. Use the JMS client as
follows to create a POX (Plain Old XML) message with a stock quote request payload
(without a SOAP envelope), and send it to the JMS destination 'dynamicQueues/JMSPoxProxy'
as follows:

ant jmsclient -Djms\_type=pox -Djms\_dest=dynamicQueues/JMSPoxProxy -Djms\_payload=MSFT

Synapse converts the POX message into a SOAP payload and sends to the
SimpleStockQuoteService after setting the SOAP action as 'urn:placeOrder'.
The sample Axis2 server displays a successful message on the receipt of the
message as:

samples.services.SimpleStockQuoteService :: Accepted order for : 19211 stocks of MSFT at $ 172.39703010684752

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample253"></a>

# Apache Synapse – Apache Synapse - Sample 253

## <a id="synapse-apache-org-userguide-samples-sample253--Sample_253:_One-way_Bridging_from_JMS_to_HTTP_and_Replying_with_a_202_Accepted_Response"></a>Sample 253: One-way Bridging from JMS to HTTP and Replying with a 202 Accepted Response

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="JMStoHTTPStockQuoteProxy" transports="jms">
<target>
<inSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
</inSequence>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
<proxy name="OneWayProxy" transports="http">
<target>
<inSequence>
<log level="full"/>
</inSequence>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample253--Objective"></a>Objective

This sample demonstrates the ability of Synapse to perform transport switching
between JMS and HTTP. It also shows how to configure a one-way HTTP proxy in
Synapse.

### <a id="synapse-apache-org-userguide-samples-sample253--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Setup and start a JMS broker (Apache ActiveMQ can be used as the
  JMS broker for this scenario. Refer [JMS setup guide](#synapse-apache-org-userguide-samples-setup-jms--pre)
  for information on how to run ActiveMQ.)
- Enable the JMS transport receiver of Synapse (Refer
  [JMS setup guide](#synapse-apache-org-userguide-samples-setup-jms--listener) for more details)
- Start Synapse using the configuration numbered 253 (repository/conf/sample/synapse\_sample\_253.xml)

  Unix/Linux: sh synapse.sh -sample 253  
  Windows: synapse.bat -sample 253

### <a id="synapse-apache-org-userguide-samples-sample253--Executing_the_Client"></a>Executing the Client

This example invokes the one-way 'placeOrder' operation on the SimpleStockQuoteService
using the Axis2 ServiceClient.fireAndForget() API at the client. To test this,
run the sample client as follows and you will notice the one-way JMS message
flowing through Synapse into the sample Axis2 server instance over HTTP, and Axis2
acknowledging it with a HTTP 202 Accepted response.

ant stockquote -Dmode=placeorder -Dtrpurl="jms:/JMStoHTTPStockQuoteProxy?\
transport.jms.ConnectionFactoryJNDIName=QueueConnectionFactory\
&java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory\
&java.naming.provider.url=tcp://localhost:61616\
&transport.jms.ContentTypeProperty=Content-Type&transport.jms.DestinationType=queue"

The second example shows how Synapse could be made to respond with a HTTP 202
Accepted response to a request received. The proxy service simply logs the message
received and acknowledges it. To try this out, run the sample client as follows.

ant stockquote -Dmode=placeorder -Dtrpurl=http://localhost:8280/services/OneWayProxy

On the Synapse console you could see the logged message, and if TCPMon was used
at the client, you would see the 202 Accepted response sent back to the client
from Synapse.

HTTP/1.1 202 Accepted
Content-Type: text/xml; charset=UTF-8
Host: 127.0.0.1
SOAPAction: "urn:placeOrder"
Date: Sun, 06 May 2007 17:20:19 GMT
Server: Synapse-HttpComponents-NIO
Transfer-Encoding: chunked
0

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample254"></a>

# Apache Synapse – Apache Synapse - Sample 254

## <a id="synapse-apache-org-userguide-samples-sample254--Sample_254:_Using_File_System_as_the_Transport_Medium_ReadingWriting_Files"></a>Sample 254: Using File System as the Transport Medium (Reading/Writing Files)

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy" transports="vfs">
<parameter name="transport.vfs.FileURI">file:///home/user/test/in</parameter> <!--CHANGE-->
<parameter name="transport.vfs.ContentType">text/xml</parameter>
<parameter name="transport.vfs.FileNamePattern">.\*\.xml</parameter>
<parameter name="transport.PollInterval">15</parameter>
<parameter name="transport.vfs.MoveAfterProcess">file:///home/user/test/original</parameter> <!--CHANGE-->
<parameter name="transport.vfs.MoveAfterFailure">file:///home/user/test/original</parameter> <!--CHANGE-->
<parameter name="transport.vfs.ActionAfterProcess">MOVE</parameter>
<parameter name="transport.vfs.ActionAfterFailure">MOVE</parameter>
<target>
<endpoint>
<address format="soap12"
uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<outSequence>
<property name="transport.vfs.ReplyFileName"
expression="fn:concat(fn:substring-after(get-property('MessageID'), 'urn:uuid:'), '.xml')"
scope="transport"/>
<property action="set" name="OUT\_ONLY" value="true"/>
<send>
<endpoint>
<address uri="vfs:file:///home/user/test/out"/> <!--CHANGE-->
</endpoint>
</send>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample254--Objective"></a>Objective

Synapse can access the local file system using its VFS (Virtual File System)
transport receiver and sender. This way Synapse can read files in the local file
system as well as write to the local file system. This sample show cases the
Synapse VFS transport in action.

### <a id="synapse-apache-org-userguide-samples-sample254--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Create 3 new directories (folders) named 'in', 'out' and 'original' in a
  suitable location in the local file system (eg: /home/user/test).
- Open the repository/conf/sample/synapse\_sample\_254.xml file in a text
  editor and. Then change the transport.vfs.FileURI, transport.vfs.MoveAfterProcess,
  transport.vfs.MoveAfterFailure parameter values to the above in, original
  and original directories respectively. Note that both 2nd and 3rd parameters
  are pointed to the 'original' directory.
- Change the endpoint in the out-sequence to point to the 'out' directory.
  The prefix 'vfs' in the endpoint URL must not be removed or changed.
- Enable the VFS transport receiver and sender for Synapse (refer VFS
  setup guide for more information)
- Start Synapse using the configuration numbered 254 (repository/conf/sample/synapse\_sample\_254.xml)

  Unix/Linux: sh synapse.sh -sample 254  
  Windows: synapse.bat -sample 254

### <a id="synapse-apache-org-userguide-samples-sample254--Executing_the_Client"></a>Executing the Client

Copy the test.xml file in the repository/conf/sample/resources/vfs directory to
the directory given in transport.vfs.FileURI above (i.e the 'in' directory). This
file contains a simple stock quote request in XML/SOAP format.

<?xml version='1.0' encoding='UTF-8'?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wsa="http://www.w3.org/2005/08/addressing">
<soapenv:Body>
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>IBM</m0:symbol>
</m0:request>
</m0:getQuote>
</soapenv:Body>
</soapenv:Envelope>

VFS transport listener will pick the file from 'in' directory and send it to the
Axis2 service over HTTP. The request XML file will be backed up in the 'original'
directory. The response from the Axis2 server will be saved to the 'out' directory.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample255"></a>

# Apache Synapse – Apache Synapse - Sample 255

## <a id="synapse-apache-org-userguide-samples-sample255--Sample_255:_Switching_from_File_Transport_FTP_to_the_Mail_Transport"></a>Sample 255: Switching from File Transport (FTP) to the Mail Transport

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy" transports="vfs">
<target>
<inSequence>
<header name="Action" value="urn:getQuote"/>
</inSequence>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<outSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
<send>
<endpoint>
<address uri="mailto:user@host"/> <!--CHANGE-->
</endpoint>
</send>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
<parameter name="transport.vfs.FileURI">vfs:ftp://guest:guest@localhost/test?vfs.passive=true</parameter> <!--CHANGE-->
<parameter name="transport.vfs.ContentType">text/xml</parameter>
<parameter name="transport.vfs.FileNamePattern">.\*\.xml</parameter>
<parameter name="transport.PollInterval">15</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample255--Objective"></a>Objective

In [sample 254](#synapse-apache-org-userguide-samples-sample254) we looked at how the VFS transport
can be used to read files from the local file system. VFS transport can also be
used to read files from FTP, SFTP and CIFS sites. This sample illustrates how to
read from a remote FTP site and send the content to a remote client as an e-mail.

### <a id="synapse-apache-org-userguide-samples-sample255--Pre-requisites"></a>Pre-requisites

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

Sat Nov 18 21:01:23 IST 2006 SimpleStockQuoteService :: Generating quote for : IBM

Standard :: Stock price = $95.26454380258552

- This sample requires access to a FTP site and an e-mail account.
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Enable the VFS transport listener for Synapse (refer VFS setup guide for
  more details).
- Enable the mail transport sender for Synapse
  (refer [ Mail transport setup](#synapse-apache-org-userguide-samples-setup-mail--mailTransportSender)
  guide for more details).
- Create a new test directory in the FTP site.
- Open the repository/conf/sample/synapse\_sample\_255.xml and edit the
  transport.vfs.FileURI parameter to point to the test directory in the FTP
  server. Also change the endpoint in the out sequence to point to your
  e-mail account.
- Start Synapse using the configuration numbered 255 (repository/conf/sample/synapse\_sample\_255.xml)

  Unix/Linux: sh synapse.sh -sample 255  
  Windows: synapse.bat -sample 255

### <a id="synapse-apache-org-userguide-samples-sample255--Executing_the_Client"></a>Executing the Client

Copy the test.xml file in the repository/conf/sample/resources/vfs directory to
the directory given in transport.vfs.FileURI above (i.e the test directory in
FTP server). This file contains a simple stock quote request in XML/SOAP format.

<?xml version='1.0' encoding='UTF-8'?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:wsa="http://www.w3.org/2005/08/addressing">
<soapenv:Body>
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>IBM</m0:symbol>
</m0:request>
</m0:getQuote>
</soapenv:Body>
</soapenv:Envelope>

VFS transport will pick up the file from the FTP site and send the content to
the stock quote service in Axis2 over HTTP. Response from Axis2 will be sent to
the mail endpoint as an e-mail. It should show up in the configured e-mail account
after a few seconds.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample256"></a>

# Apache Synapse – Apache Synapse - Sample 256

## <a id="synapse-apache-org-userguide-samples-sample256--Sample_256:_Proxy_Services_with_the_Mail_Transport"></a>Sample 256: Proxy Services with the Mail Transport

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy" transports="mailto">
<target>
<inSequence>
<property name="senderAddress" expression="get-property('transport', 'From')"/>
<log level="full">
<property name="Sender Address" expression="get-property('senderAddress')"/>
</log>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</inSequence>
<outSequence>
<property name="Subject" value="Custom Subject for Response" scope="transport"/>
<header name="To" expression="fn:concat('mailto:', get-property('senderAddress'))"/>
<log level="full">
<property name="message" value="Response message"/>
<property name="Sender Address" expression="get-property('senderAddress')"/>
</log>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
<parameter name="transport.mail.Address">synapse.demo.mail1@gmail.com</parameter>
<parameter name="transport.mail.Protocol">pop3</parameter>
<parameter name="transport.PollInterval">5</parameter>
<parameter name="mail.pop3.host">pop.gmail.com</parameter>
<parameter name="mail.pop3.port">995</parameter>
<parameter name="mail.pop3.user">synapse.demo.mail1</parameter>
<parameter name="mail.pop3.password">mailpassword</parameter>
<parameter name="mail.pop3.socketFactory.class">javax.net.ssl.SSLSocketFactory</parameter>
<parameter name="mail.pop3.socketFactory.fallback">false</parameter>
<parameter name="mail.pop3.socketFactory.port">995</parameter>
<parameter name="transport.mail.ContentType">application/xml</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample256--Objective"></a>Objective

This sample show cases the mail transport of Synapse. The mail transport allows
Synapse to receive and send e-mails using common protocols like POP, IMAP and
SMTP.

### <a id="synapse-apache-org-userguide-samples-sample256--Pre-requisites"></a>Pre-requisites

- You need access to an e-mail account to try out this sample
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Enable the mail transport listener and mail transport sender for Synapse
  (refer [Mail transport setup](#synapse-apache-org-userguide-samples-setup-mail--mailTransportSender)
  guide for more details)
- Start Synapse using the configuration numbered 256 (repository/conf/sample/synapse\_sample\_256.xml)

  Unix/Linux: sh synapse.sh -sample 256  
  Windows: synapse.bat -sample 256

### <a id="synapse-apache-org-userguide-samples-sample256--Executing_the_Client"></a>Executing the Client

Send an e-mail to [synapse.demo.mail1@gmail.com](mailto:synapse.demo.mail1@gmail.com)
with the following payload.

<getQuote xmlns="http://services.samples">
<request>
<symbol>IBM</symbol>
</request>
</getQuote>

Synapse will be polling on the above e-mail account for any incoming requests. When
your mail arrives at this account, Synapse will pick it up and send the payload to
Axis2 over HTTP. The response will be mailed back to your e-mail account. Synapse
retrieves the sender information from the original request to determine the recipient
of the response mail.

Note that in this sample we used the transport.mail.ContentType property to make
sure that the transport parses the request message as POX. If you remove this
property, you may still be able to send requests using a standard mail client if
instead of writing the XML in the body of the message, you add it as an attachment.
In that case, you should use .xml as a suffix for the attachment and format the
request as a SOAP 1.1 message. Indeed, for a file with suffix .xml the mail client
will most likely use text/xml as the content type, exactly as required for SOAP 1.1.
Sending a POX message using this approach will be a lot trickier, because most
standard mail clients don't allow the user to explicitly set the content type.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample257"></a>

# Apache Synapse – Apache Synapse - Sample 257

## <a id="synapse-apache-org-userguide-samples-sample257--Sample_257:_Proxy_Services_with_the_FIX_Transport"></a>Sample 257: Proxy Services with the FIX Transport

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="OrderProcesserProxy40" transports="fix">
<target>
<endpoint>
<address
uri="fix://localhost:19876?BeginString=FIX.4.0&SenderCompID=SYNAPSE&TargetCompID=EXEC"/>
</endpoint>
<inSequence>
<log level="full"/>
</inSequence>
<outSequence>
<log level="full"/>
<send/>
</outSequence>
</target>
<parameter name="transport.fix.AcceptorConfigURL">
file:repository/conf/sample/resources/fix/fix-synapse.cfg
</parameter>
<parameter name="transport.fix.AcceptorMessageStore">file</parameter>
<parameter name="transport.fix.InitiatorConfigURL">
file:repository/conf/sample/resources/fix/synapse-sender.cfg
</parameter>
<parameter name="transport.fix.InitiatorMessageStore">file</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample257--Objective"></a>Objective

This sample demonstrates the FIX (Financial Information eXchange) transport of
Synapse. FIX transport allows Synapse to connect to remote FIX acceptors and
initiators and exchange finance data.

### <a id="synapse-apache-org-userguide-samples-sample257--Pre-requisites"></a>Pre-requisites

- You need a [Quickfix/J](http://www.quickfixj.org) installation
  to try out FIX samples. Please download and extract a Quickfix/J distribution
  into your local machine.
- Configure the Executor sample FIX application (shipped with Quickfix/J)
  to receive messages from Synapse and start it (refer
  [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--exec) for more details).
- Enable the FIX transport listener and sender for Synapse (refer
  [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--synapse) for details).
- Start Synapse using the configuration numbered 257 (repository/conf/sample/synapse\_sample\_257.xml)

  Unix/Linux: sh synapse.sh -sample 257  
  Windows: synapse.bat -sample 257

  If Executor is properly configured, Synapse should establish a FIX session
  with the Executor upon startup. You should see some log entries confirming
  the session logon event on Synapse console as well as Executor console.
- Configure Banzai sample FIX application (shipped with Quickfix/J) to send
  messages to Synapse and start it (refer [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--banzai)
  for more details). If Banzai was properly configured, it should establish
  a FIX session with Synapse upon startup. You should see some session
  logon messages on Synapse console and Banzai console.

### <a id="synapse-apache-org-userguide-samples-sample257--Executing_the_Client"></a>Executing the Client

Send some FIX messages from Banzai to Synapse. Synapse will forward all requests
to Executor and get them processes. Responses from Executor will be routed back
to Banzai.

Synapse converts all received FIX messages into SOAP format. You can view these
SOAP messages from the Synapse log. When SOAP messages are sent to FIX endpoints,
Synapse converts them back into valid FIX messages. While FIX messages are flowing
through the service bus, you can perform various transformations and content based
routing on the FIX messages using the existing mediators like XSLT, XQuery, Filter
and Switch.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample258"></a>

# Apache Synapse – Apache Synapse - Sample 258

## <a id="synapse-apache-org-userguide-samples-sample258--Sample_258:_Switching_from_HTTP_to_FIX"></a>Sample 258: Switching from HTTP to FIX

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="FIXProxy" transports="http">
<target>
<endpoint>
<address
uri="fix://localhost:19876?BeginString=FIX.4.0&SenderCompID=SYNAPSE&TargetCompID=EXEC"/>
</endpoint>
<inSequence>
<property name="transport.fix.ServiceName" value="FIXProxy" scope="axis2-client"/>
<log level="full"/>
</inSequence>
<outSequence>
<log level="full"/>
<send/>
</outSequence>
</target>
<parameter name="transport.fix.InitiatorConfigURL">
file:repository/conf/sample/resources/fix/synapse-sender.cfg
</parameter>
<parameter name="transport.fix.InitiatorMessageStore">file</parameter>
<parameter name="transport.fix.SendAllToInSequence">false</parameter>
<parameter name="transport.fix.DropExtraResponses">true</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample258--Objective"></a>Objective

Demonstrates how to use the FIX transport in a transport switching scenario with
HTTP.

### <a id="synapse-apache-org-userguide-samples-sample258--Pre-requisites"></a>Pre-requisites

- You need a [Quickfix/J](http://www.quickfixj.org) installation
  to try out FIX samples. Please download and extract a Quickfix/J distribution
  into your local machine.
- Configure the Executor sample FIX application (shipped with Quickfix/J)
  to receive messages from Synapse and start it (refer
  [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--exec) for more details).
- Enable the FIX transport sender for Synapse (refer
  [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--synapse) for details).
- Start Synapse using the configuration numbered 258 (repository/conf/sample/synapse\_sample\_258.xml)

  Unix/Linux: sh synapse.sh -sample 258  
  Windows: synapse.bat -sample 258

### <a id="synapse-apache-org-userguide-samples-sample258--Executing_the_Client"></a>Executing the Client

Go to the samples/axis2Client directory and invoke the sample FIX/HTTP client as
follows.

ant fixclient -Dsymbol=IBM -Dqty=5 -Dmode=buy -Daddurl=http://localhost:8280/services/FIXProxy

This command sends a HTTP request to the FIXProxy on Synapse. The message is
converted into a FIX message and sent to the Executor sample application. Executor
will send two responses for this request (receive ack and the execution report) and
Synapse will send the first response back to the HTTP client. (Synapse can't send
both responses back, since HTTP does not allow sending two responses to the same
request)

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample259"></a>

# Apache Synapse – Apache Synapse - Sample 259

## <a id="synapse-apache-org-userguide-samples-sample259--Sample_259:_Switch_from_FIX_to_HTTP"></a>Sample 259: Switch from FIX to HTTP

<definitions xmlns="http://ws.apache.org/ns/synapse">
<localEntry key="xslt-key-req"
src="file:repository/conf/sample/resources/transform/transform\_fix\_to\_http.xslt"/>
<proxy name="FIXProxy" transports="fix">
<target>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<inSequence>
<log level="full"/>
<xslt key="xslt-key-req"/>
<log level="full"/>
</inSequence>
<outSequence>
<log level="full"/>
</outSequence>
</target>
<parameter name="transport.fix.AcceptorConfigURL">
file:repository/conf/sample/resources/fix/fix-synapse.cfg
</parameter>
<parameter name="transport.fix.AcceptorMessageStore">file</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample259--Objective"></a>Objective

In [sample 258](#synapse-apache-org-userguide-samples-sample258) we looked at how to forward HTTP
requests over a FIX session. This sample demonstrates how to send a FIX message
to an HTTP endpoint.

### <a id="synapse-apache-org-userguide-samples-sample259--Pre-requisites"></a>Pre-requisites

- You need a [Quickfix/J](http://www.quickfixj.org) installation
  to try out FIX samples. Please download and extract a Quickfix/J distribution
  into your local machine.
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Enable the FIX transport receiver for Synapse (refer
  [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--synapse) for details)
- Start Synapse using the configuration numbered 259 (repository/conf/sample/synapse\_sample\_259.xml)

  Unix/Linux: sh synapse.sh -sample 259  
  Windows: synapse.bat -sample 259
- Configure Banzai sample FIX application (shipped with Quickfix/J) to send
  messages to Synapse and start it (refer [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--banzai)
  for more details). If Banzai was properly configured, it should establish
  a FIX session with Synapse upon startup. You should see some session logon
  messages on Synapse console and Banzai console.

### <a id="synapse-apache-org-userguide-samples-sample259--Executing_the_Client"></a>Executing the Client

This sample expects a Limit order from Banzai to be received by Synapse and
transformed into a place order request in SOAP format. To try it out send a
'Limit' order request from Banzai to Synapse. Synapse will convert it into a
place order request and send to the Axis2 server over HTTP. The stock quote
service in Axis2 will print the following log when it receives the in-only
place order request.

Accepted order for : 18406 stocks of MSFT at $ 83.58806051152119

Note that the request sent from Banzai must be of type 'Limit'. Otherwise the
XSLT transformation will not work as expected and Axis2 will receive an invalid
request. Also since the place order requests are one-way messages, Banzai is not
supposed to receive any response in this case.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample260"></a>

# Apache Synapse – Apache Synapse - Sample 260

## <a id="synapse-apache-org-userguide-samples-sample260--Sample_260:Switch_from_FIX_to_AMQP"></a>Sample 260:Switch from FIX to AMQP

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="FIXProxy" transports="fix">
<target>
<endpoint>
<address
uri="jms:/QpidStockQuoteService?transport.jms.ConnectionFactoryJNDIName=qpidConnectionfactory&java.naming.factory.initial=org.apache.qpid.jndi.PropertiesFileInitialContextFactory&java.naming.provider.url=repository/conf/sample/resources/fix/conn.properties&transport.jms.ReplyDestination=replyQueue"/>
</endpoint>
<inSequence>
<log level="full"/>
</inSequence>
<outSequence>
<property name="transport.fix.ServiceName" value="FIXProxy" scope="axis2-client"/>
<log level="full"/>
<send/>
</outSequence>
</target>
<parameter name="transport.fix.AcceptorConfigURL">
file:repository/conf/sample/resources/fix/fix-synapse.cfg
</parameter>
<parameter name="transport.fix.AcceptorMessageStore">
file
</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample260--Objective"></a>Objective

Demonstrate the capability of switching between FIX and AMQP protocols

### <a id="synapse-apache-org-userguide-samples-sample260--Pre-requisites"></a>Pre-requisites

- You will need the sample FIX blotter that comes with Quickfix/J (Banzai). Configure the blotter to establish sessions with Synapse. (refer
  [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--synapse) for details)
- Configure the AMQP transport for Synapse. See Configure Synapse for [ AMQP Transport setup](#synapse-apache-org-userguide-samples-setup-jms--amqp) for detail
- To get an idea about the various transport parameters being used in this sample see FIX Transport Parameters .

### <a id="synapse-apache-org-userguide-samples-sample260--Executing_the_Client"></a>Executing the Client

Start the AMQP consumer, by switching to samples/axis2Client directory and running the consumer using the following command. Consumer will listen to the queue 'QpidStockQuoteService', accept the orders and reply to the queue 'replyQueue'.

ant amqpconsumer -Dpropfile=$SYNAPSE\_HOME/repository/conf/sample/resources/fix/direct.properties

Open up the SYNAPSE\_HOME/repository/conf/sample/synapse\_sample\_260.xml file
and make sure that the transport.fix.AcceptorConfigURL property points
to the fix-synapse.cfg file you created. Once done you can start the
Synapse configuration numbered 260: i.e. synapse -sample 260. Note that
Synapse creates a new FIX session with Banzai at this point.

Send an order request from Banzai to Synapse. e.g. Buy DELL 1000 @ MKT.

Synapse will forward the order request by binding it to a JMS message payload and sending it to the AMQP consumer. AMQP consumer will send a execution back to Banzai.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample261"></a>

# Apache Synapse – Apache Synapse - Sample 261

## <a id="synapse-apache-org-userguide-samples-sample261--Sample_261:_Switch_Between_Different_FIX_Versions"></a>Sample 261: Switch Between Different FIX Versions

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="OrderProcesserProxy41" transports="fix">
<target>
<endpoint>
<address
uri="fix://localhost:19877?BeginString=FIX.4.1&SenderCompID=SYNAPSE&TargetCompID=EXEC"/>
</endpoint>
<inSequence>
<log level="full"/>
</inSequence>
<outSequence>
<log level="full"/>
<send/>
</outSequence>
</target>
<parameter name="transport.fix.AcceptorConfigURL">
file:repository/conf/sample/resources/fix/fix-synapse-m40.cfg
</parameter>
<parameter name="transport.fix.AcceptorMessageStore">file</parameter>
<parameter name="transport.fix.InitiatorConfigURL">
file:repository/conf/sample/resources/fix/synapse-sender-m.cfg
</parameter>
<parameter name="transport.fix.InitiatorMessageStore">file</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample261--Objective"></a>Objective

Demonstrate the ability of Synapse to switch between FIX versions (eg: FIX 4.0
to FIX 4.1)

### <a id="synapse-apache-org-userguide-samples-sample261--Pre-requisites"></a>Pre-requisites

- You need a [Quickfix/J](http://www.quickfixj.org) installation
  to try out FIX samples. Please download and extract a Quickfix/J distribution
  into your local machine.
- Enable the FIX transport listener and sender for Synapse (refer
  [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--synapse) for details).
- Configure the Executor sample FIX application (shipped with Quickfix/J)
  to receive messages from Synapse and start it (refer [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--exec)
  for more details). In previous samples we only had a FIX 4.0 session configured
  for Executor. For this sample we should configure a FIX 4.1 session for
  Executor. Therefore before starting it add the following entries to the
  configuration file of Executor (executor.cfg).

  [session]
  BeginString=FIX.4.1
  SocketAcceptPort=19877
- Start Synapse using the configuration numbered 261 (repository/conf/sample/synapse\_sample\_261.xml)

  Unix/Linux: sh synapse.sh -sample 261  
  Windows: synapse.bat -sample 261

  If Executor is properly configured, Synapse should establish a FIX 4.1 session
  with the Executor upon startup. You should see some log entries confirming
  the session logon event on Synapse console as well as Executor console.
- Configure Banzai sample FIX application (shipped with Quickfix/J) to send
  messages to Synapse and start it (refer [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--banzai)
  for more details). You should also add the following entry to the Banzai
  configuration file before starting it (replace $SYNAPSE\_HOME with the
  actual path to Synapse home).

  DataDictionary=$SYNAPSE\_HOME/repository/conf/sample/resources/fix/FIX40-synapse.xml

  If Banzai was properly configured, it should establish a FIX 4.0 session with
  Synapse upon startup. You should see some session logon messages on Synapse
  console and Banzai console.

### <a id="synapse-apache-org-userguide-samples-sample261--Executing_the_Client"></a>Executing the Client

Send some FIX messages from Banzai to Synapse. Synapse will forward all requests
to Executor and get them processes. Responses from Executor will be routed back
to Banzai.

Note that the session between Banzai and Synapse is a FIX 4.0 session whereas
the session between Synapse and Execurot is a FIX 4.1 session. Synapse receives
FIX 4.0 messages and simply forwards them to the FIX 4.1 endpoint.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample262"></a>

# Apache Synapse – Apache Synapse - Sample 262

## <a id="synapse-apache-org-userguide-samples-sample262--Sample_262:_Content_Based_Routing_of_FIX_Messages"></a>Sample 262: Content Based Routing of FIX Messages

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="CBR\_SEQ">
<in>
<switch source="//message/body/field[@id='55']">
<case regex="GOOG">
<send>
<endpoint>
<address
uri="fix://localhost:19876?BeginString=FIX.4.0&SenderCompID=SYNAPSE&TargetCompID=EXEC"/>
</endpoint>
</send>
</case>
<case regex="MSFT">
<send>
<endpoint>
<address
uri="fix://localhost:19877?BeginString=FIX.4.1&SenderCompID=SYNAPSE&TargetCompID=EXEC"/>
</endpoint>
</send>
</case>
<default/>
</switch>
</in>
<out>
<send/>
</out>
</sequence>
<proxy name="FIXProxy" transports="fix">
<target inSequence="CBR\_SEQ"/>
<parameter name="transport.fix.AcceptorConfigURL">
file:repository/conf/sample/resources/fix/fix-synapse.cfg
</parameter>
<parameter name="transport.fix.AcceptorMessageStore">
file
</parameter>
<parameter name="transport.fix.InitiatorConfigURL">
file:repository/conf/sample/resources/fix/synapse-sender.cfg
</parameter>
<parameter name="transport.fix.InitiatorMessageStore">
file
</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample262--Objective"></a>Objective

Show case the ability of Synapse to route FIX messages based on the content they
carry.

### <a id="synapse-apache-org-userguide-samples-sample262--Pre-requisites"></a>Pre-requisites

- You need a [Quickfix/J](http://www.quickfixj.org) installation
  to try out FIX samples. Please download and extract a Quickfix/J distribution
  into your local machine.
- Configure the Executor sample FIX application (shipped with Quickfix/J)
  to receive messages from Synapse and start it (refer [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--exec)
  for more details). In previous samples we only had a FIX 4.0 session configured
  for Executor. For this sample we should also configure a FIX 4.1 session for
  Executor. Therefore before starting it add the following entries to the
  configuration file of Executor. (Do not remove or change the FIX 4.0
  configuration already available in this file)

  [session]
  BeginString=FIX.4.1
  SocketAcceptPort=19877
- Enable the FIX transport listener and sender for Synapse (refer
  [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--synapse) for details).
- Start Synapse using the configuration numbered 262 (repository/conf/sample/synapse\_sample\_262.xml)

  Unix/Linux: sh synapse.sh -sample 262  
  Windows: synapse.bat -sample 262

  If the Executor was configured properly Synapse should establish 2 sessions
  with Executor upon startup (FIX 4.0 session and FIX 4.1 session)
- Configure Banzai sample FIX application (shipped with Quickfix/J) to send
  messages to Synapse and start it (refer [FIX setup guide](#synapse-apache-org-userguide-samples-setup-fix--banzai)
  for more details). You should also add the following entry to the Banzai
  configuration file before starting it (replace $SYNAPSE\_HOME with the actual
  path to Synapse home).

  DataDictionary=$SYNAPSE\_HOME/repository/conf/sample/resources/fix/FIX40-synapse.xml

  If Banzai was properly configured, it should establish a FIX 4.0 session with
  Synapse upon startup. You should see some session logon messages on Synapse
  console and Banzai console.

### <a id="synapse-apache-org-userguide-samples-sample262--Executing_the_Client"></a>Executing the Client

Send some order requests from Banzai to Synapse containing the synbols 'GOOG' and
'MSFT'. Synapse will forward the messages with the symbol 'GOOG' to the FIX 4.0
session. Messages containing the symbol 'MSFT' will be sent to the FIX 4.1 session.
Any other messages will be simply dropped by the service bus since the default case
of the switch mediator has been kept empty.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample263"></a>

# Apache Synapse – Apache Synapse - Sample 262

## <a id="synapse-apache-org-userguide-samples-sample263--Sample_263:_Transport_switching_-_JMS_to_https_using_JBoss_MessagingJBM"></a>Sample 263: Transport switching - JMS to http/s using JBoss Messaging(JBM)

<definitions xmlns="http://ws.apache.org/ns/synapse"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xsi:schemaLocation="http://ws.apache.org/ns/synapse http://synapse.apache.org/ns/2010/04/configuration/synapse\_config.xsd">
<proxy name="StockQuoteProxy" transports="jms">
<target>
<inSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
</inSequence>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
<parameter name="transport.jms.ContentType">
<rules>
<jmsProperty>contentType</jmsProperty>
<default>application/xml</default>
</rules>
</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample263--Objective"></a>Objective

Objective: Introduction to switching transports with proxy
services.
The JMS provider will be
[JBoss Messaging(JBM).](http://jboss.org/jbossmessaging/)

### <a id="synapse-apache-org-userguide-samples-sample263--Pre-requisites"></a>Pre-requisites

- Start the Axis2 server and deploy the SimpleStockQuoteService
  (Refer
  steps above)
- [Download](http://jboss.org/jbossmessaging/)
  , install and start JBM server, and configure Synapse to listen
  on JBM (refer notes below)
- Start the Synapse configuration numbered 263

  Unix/Linux: sh synapse.sh -sample 263
    
  Windows: synapse.bat -sample 23
- We need to configure the required queues in JBM. Add the
  following entry to JBM jms configuration inside
  file-config/stand-alone/non-clustered/jbm-jms.xml.
  Once JBM is
  installed and started you should get a message as
  follows:

  <queue name="StockQuoteProxy">
  <entry name="StockQuoteProxy"/>
  </queue>
- Once you started the JBM server with the above changes you'll be
  able to see the following on STDOUT

  10:18:02,673 INFO [org.jboss.messaging.core.server.impl.MessagingServerImpl] JBoss Messaging Server version 2.0.0.BETA3 (maggot, 104) started
- You will now need to configure the Axis2 instance used by
  Synapse
  (not the sample Axis2 server) to enable JMS support using
  the
  above provider. Refer Axis2 documentation on setting up JMS
  in
  detail (http://ws.apache.org/axis2/1\_1/jms-transport.html).
  You
  will also need to copy the jbm-core-client.jar,
  jbm-jms-client.jar, jnp-client.jar(these jars are inside client
  folder ) and jbm-transports.jar, netty.jar(these jars are from
  lib folder) jars from JBM into the lib directory to allow
  Synapse
  to connect to the JBM JMS provider. This was tested with
  JBM
  2.0.0.BETA3
- You need to add the following configuration for Axis2 JMS transport listener in axis2.xml found at repository/conf/axis2.xml.

  <transportReceiver name="jms" class="org.apache.axis2.transport.jms.JMSListener">
  <parameter name="java.naming.factory.initial">org.jnp.interfaces.NamingContextFactory</parameter>
  <parameter name="java.naming.provider.url">jnp://localhost:1099</parameter>
  <parameter name="java.naming.factory.url.pkgs">org.jboss.naming:org.jnp.interfaces</parameter>
  <parameter name="transport.jms.ConnectionFactoryJNDIName">ConnectionFactory</parameter>
- On the Synapse debug log you will notice that the JMS listener
  received the request message as:

  [JMSWorker-1] DEBUG ProxyServiceMessageReceiver -Proxy Service StockQuoteProxy received a new message...
- In this sample, the client sends the request message to the
  proxy service exposed over JMS in Synsape. Synapse forwards this
  message to the HTTP EPR of the simple stock quote service hosted
  on the sample Axis2 server. Note that the operation is out-only
  and no response is sent back to the client. The
  transport.jms.ContentType property is necessary to allow the JMS
  transport to determine the content type of incoming messages.
  With the given configuration it will first try to read the
  content type from the 'contentType' message property and fall
  back to 'application/xml' (i.e. POX) if this property is not
  set.
  Note that the JMS client used in this example doesn't send any
  content type information.

### <a id="synapse-apache-org-userguide-samples-sample263--Executing_the_Client"></a>Executing the Client

Once you start the Synapse configuration 250 and request for the WSDL of the
proxy service (http://localhost:8280/services/StockQuoteProxy?wsdl) you will
notice that its exposed only on the JMS transport. This is because the configuration specified this
requirement in the proxy service definition.

ant jmsclient -Djms\_type=pox -Djms\_dest=StockQuoteProxy -Djms\_payload=MSFT -Djava.naming.provider.url=jnp://localhost:1099 -Djava.naming.factory.initial=org.jnp.interfaces.NamingContextFactory -D=java.naming.factory.url.pkgs=org.jboss.naming:org.jnp.interfaces

Now if you examine the console running the sample Axis2 server,
you will see a message indicating that the server has accepted an
order as follows:

Accepted order for : 16517 stocks of MSFT at $169.14622538721846

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample264"></a>

# Apache Synapse – Apache Synapse - Sample 264

## <a id="synapse-apache-org-userguide-samples-sample264--Sample_264:_Request-Response_Invocations_with_the_JMS_Transport"></a>Sample 264: Request-Response Invocations with the JMS Transport

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy" transports="http">
<target>
<endpoint>
<address
uri="jms:/SimpleStockQuoteService?transport.jms.ConnectionFactoryJNDIName=QueueConnectionFactory&java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory&java.naming.provider.url=tcp://localhost:61616&transport.jms.DestinationType=queue"/>
</endpoint>
<inSequence>
<property action="set" name="transport.jms.ContentTypeProperty" value="Content-Type"
scope="axis2"/>
</inSequence>
<outSequence>
<property action="remove" name="TRANSPORT\_HEADERS" scope="axis2"/>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample264--Objective"></a>Objective

In [sample 251](#synapse-apache-org-userguide-samples-sample251) we saw how to perform transport switching
between HTTP and JMS using a one-way invocation. Here we will do HTTP to JMS switching
with a two-way, request-response invocation.

### <a id="synapse-apache-org-userguide-samples-sample264--Pre-requisites"></a>Pre-requisites

- Setup and start a JMS broker (Apache ActiveMQ can be used as the
  JMS broker for this scenario. Refer [JMS setup guide](#synapse-apache-org-userguide-samples-setup-jms--pre)
  for information on how to run ActiveMQ.)
- Enable the JMS transport receiver of the sample Axis2 server (Refer
  [JMS setup guide](#synapse-apache-org-userguide-samples-setup-jms--server) for details)
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2 (Since
  the JMS receiver is enabled, Axis2 will start polling on a JMS queue)
- Start Synapse using the configuration numbered 264 (repository/conf/sample/synapse\_sample\_264.xml)

  Unix/Linux: sh synapse.sh -sample 264  
  Windows: synapse.bat -sample 264

### <a id="synapse-apache-org-userguide-samples-sample264--Executing_the_Client"></a>Executing the Client

Send a stock quote request to Synapse over HTTP using the following command.

ant stockquote -Daddurl=http://localhost:8280/services/StockQuoteProxy -Dsymbol=MSFT

The proxy service will send the message to the JMS queue named SimpleStockQuoteService
and wait for a response to arrive. In fact the JMS sender in Synapse will create a temporary
queue to start polling on that queue for the response. The address of this queue will
be sent on the request as a JMS header. Axis2 server will consumer the request from the
queue and place a response on the temporary queue created by Synapse. At this point
Synapse will pick up the response and forward it back to the Axis2 client over HTTP.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample265"></a>

# Apache Synapse – Apache Synapse - Sample 265

## <a id="synapse-apache-org-userguide-samples-sample265--Sample_265:_Switching_from_TCP_to_HTTPS"></a>Sample 265: Switching from TCP to HTTP/S

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy" transports="tcp">
<target>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<inSequence>
<log level="full"/>
<property name="OUT\_ONLY" value="true"/>
</inSequence>
</target>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample265--Objective"></a>Objective

Demonstrate the ability of Synapse to receive raw TCP messages and send them to
HTTP endpoints

### <a id="synapse-apache-org-userguide-samples-sample265--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Enable the TCP transport receiver for Synapse (refer
  [TCP transport setup guide](#synapse-apache-org-userguide-samples-setup-tcp_udp--tcp)).
- Start Synapse using the configuration numbered 265 (repository/conf/sample/synapse\_sample\_265.xml)

  Unix/Linux: sh synapse.sh -sample 265  
  Windows: synapse.bat -sample 265
- Enable the TCP transport sender for the sample Axis2 client (refer
  [TCP transport setup guide](#synapse-apache-org-userguide-samples-setup-tcp_udp--tcp) for
  details).

### <a id="synapse-apache-org-userguide-samples-sample265--Executing_the_Client"></a>Executing the Client

This sample is similar to [Sample 250](#synapse-apache-org-userguide-samples-sample250). Only difference
is instead of the JMS transport we will be using the TCP transport to receive
messages. TCP is not an application layer protocol. Hence there are no application
level headers available in the requests. Synapse has to simply read the XML content
coming through the socket and dispatch it to the right proxy service based on the
information available in the message payload itself. The TCP transport is capable
of dispatching requests based on addressing headers or the first element in the
SOAP body. In this sample, we will get the sample client to send WS-Addressing
headers in the request. Therefore the dispatching will take place based on the
addressing header values.

Invoke the stockquote client using the following command. Note the TCP URL in the
command.

ant stockquote -Daddurl=tcp://localhost:6060/services/StockQuoteProxy -Dmode=placeorder

The TCP transport will receive the message and hand it over to the mediation engine.
Synapse will dispatch the request to the StockQuoteProxy service based on the
addressing header values.

When the proxy service forwards the message to the sample Axis2 server over HTTP,
sample server will print the following entry to confirm that the request has
been received.

Thu May 20 12:25:01 IST 2010 samples.services.SimpleStockQuoteService :: Accepted order #1 for : 17621 stocks of IBM at $ 73.48068475255796

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample266"></a>

# Apache Synapse – Apache Synapse - Sample 266

## <a id="synapse-apache-org-userguide-samples-sample266--Sample_266:_Switching_from_UDP_to_HTTPS"></a>Sample 266: Switching from UDP to HTTP/S

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy" transports="udp">
<target>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<inSequence>
<log level="full"/>
<property name="OUT\_ONLY" value="true"/>
</inSequence>
</target>
<parameter name="transport.udp.port">9999</parameter>
<parameter name="transport.udp.contentType">text/xml</parameter>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample266--Objective"></a>Objective

Showcase the ability of Synapse to receive and process raw UDP messages

### <a id="synapse-apache-org-userguide-samples-sample266--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Enable the UDP transport receiver for Synapse (refer
  [UDP transport setup guide](#synapse-apache-org-userguide-samples-setup-tcp_udp--udp)).
- Start Synapse using the configuration numbered 266 (repository/conf/sample/synapse\_sample\_266.xml)

  Unix/Linux: sh synapse.sh -sample 266  
  Windows: synapse.bat -sample 266
- Enable the UDP transport sender for the sample Axis2 client (refer
  [UDP transport setup guide](#synapse-apache-org-userguide-samples-setup-tcp_udp--udp) for
  details).

### <a id="synapse-apache-org-userguide-samples-sample266--Executing_the_Client"></a>Executing the Client

This sample is similar to [Sample 265](#synapse-apache-org-userguide-samples-sample265). Only difference
is instead of the TCP transport we will be using the UDP transport to receive
messages.

Invoke the stockquote client using the following command. Note the UDP URL in the
command.

ant stockquote -Daddurl=udp://localhost:9999?contentType=text/xml -Dmode=placeorder

Since we have configured the content type as text/xml for the proxy service,
incoming messages will be processed as SOAP 1.1 messages.

When the proxy service forwards the message to the sample Axis2 server over HTTP,
sample server will print the following entry to confirm that the request has
been received.

Thu May 20 12:25:01 IST 2010 samples.services.SimpleStockQuoteService :: Accepted order #1 for : 17621 stocks of IBM at $ 73.48068475255796

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample269"></a>

# Apache Synapse – Apache Synapse - Sample 269

## <a id="synapse-apache-org-userguide-samples-sample269--Sample_269:Introduction_to_AMQP_Transport"></a>Sample 269:Introduction to AMQP Transport

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="ConsumerProxy" transports="amqp">
<target>
<inSequence>
<property action="set" name="OUT\_ONLY" value="true"/>
<log level="custom">
<property name="status" value="At ConsumerProxy"/>
</log>
<log level="full"/>
<drop/>
</inSequence>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
<parameter name="transport.amqp.ConnectionFactoryName">consumer</parameter>
<parameter name="transport.amqp.QueueName">ConsumerProxy</parameter>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample269--Objective"></a>Objective

Demonstrate the AMQP transport of Synapse.

### <a id="synapse-apache-org-userguide-samples-sample269--Pre-requisites"></a>Pre-requisites

- [Download](http://www.rabbitmq.com/java-client.html) the RabbitMQ
  Java client library and copy it into Synapse class path (SYNAPSE\_HOME/lib).
- [Download](http://www.rabbitmq.com/) and install the RabbitMQ AMQP broker.
  Then start the broker on its default port(5672).
- Uncomment the AMQP transport listener section in axis2.xml(repository/conf/axis2.xml).
  If you are running the AMQP broker on a port other than the default port,
  configure the connection factory definitions in AMQP transport listener appropriately.
- Start Synapse using the configuration numbered 269 (repository/conf/sample/synapse\_sample\_269.xml)

  Unix/Linux: sh synapse.sh -sample 269  
  Windows: synapse.bat -sample 269

### <a id="synapse-apache-org-userguide-samples-sample269--Executing_the_Client"></a>Executing the Client

In this sample we are using a proxy service exposed over AMQP (note the transports=amqp
attribute). If you check the WSDL of the proxy service using a web browser, you
will notice that it only has AMQP endpoints.

Run the sample RabbitMQ AMQP client by switching to the samples/axis2Client directory and
executing the following command. Other options that can be passed into the RabbitMQ
client can be found by just executing 'ant'.

ant rabbitmqclient -Damqpmode=producer -DqueueName=ConsumerProxy -DpayLoad=IBM

This will send a plain XML formatted place order request to a queue in the RabbitMQ
broker. The queue is named 'ConsumerProxy'. Synapse will be polling on this queue for
any incoming messages so it will pick up the request. A message similar to following
will be logged on the console indicating that the message has been received at the
proxy service.

2013-07-30 17:00:56,687 [-] [pool-11-thread-5] INFO LogMediator status = At ConsumerProxy

22013-07-30 17:00:56,688 [-] [pool-11-thread-5] INFO LogMediator To: null, Direction: request, Envelope: <?xml version='1.0' encoding='utf-8'?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><m:placeOrder xmlns:m="http://services.samples">
<m:order>
<m:price>163.00923364424872</m:price>
<m:quantity>6620</m:quantity>
<m:symbol>IBM</m:symbol>
</m:order>
</m:placeOrder></soapenv:Body></soapenv:Envelope>

Note that the operation is out-only and no response is sent back to the client.
The content type of the message can be configured using the parameter
transport.amqp.ContentType and by default this is assumed to be application/xml.

<parameter name="transport.amqp.ConnectionFactoryName">consumer</parameter>

Above parameter defines the name of the connection factory that should be used.
If a specific connection factory is not given the default connection factory will be used.

<parameter name="transport.amqp.QueueName">ConsumerProxy</parameter>

Above parameter defines the queue to which the proxy service will connect and start to listen.
The other configuration parameters and more examples of AMQP transport can be
found in the AMQP transport documentation.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample3"></a>

# Apache Synapse – Apache Synapse - Sample 3

## <a id="synapse-apache-org-userguide-samples-sample3--Sample_3:_Local_Registry_Entries_Reusable_Endpoints_and_Sequences"></a>Sample 3: Local Registry Entries, Reusable Endpoints and Sequences

<definitions xmlns="http://ws.apache.org/ns/synapse">
<!-- define a string resource entry to the local registry -->
<localEntry key="version">0.1</localEntry>
<!-- define a reuseable endpoint definition -->
<endpoint name="simple">
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<!-- define a reusable sequence -->
<sequence name="stockquote">
<!-- log the message using the custom log level. illustrates custom properties for log -->
<log level="custom">
<property name="Text" value="Sending quote request"/>
<property name="version" expression="get-property('version')"/>
<property name="direction" expression="get-property('direction')"/>
</log>
<!-- send message to real endpoint referenced by key "simple" endpoint definition -->
<send>
<endpoint key="simple"/>
</send>
</sequence>
<sequence name="main">
<in>
<property name="direction" value="incoming"/>
<sequence key="stockquote"/>
</in>
<out>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample3--Objective"></a>Objective

Demonstrates how to define local registry entries, sequences and endpoints in a
reusable manner so that they can be used for mediation by referencing them by
names.

### <a id="synapse-apache-org-userguide-samples-sample3--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 3 (repository/conf/sample/synapse\_sample\_3.xml)

  Unix/Linux: sh synapse.sh -sample 3  
  Windows: synapse.bat -sample 3

### <a id="synapse-apache-org-userguide-samples-sample3--Executing_the_Client"></a>Executing the Client

Execute the sample client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

This example uses a sequence named 'main' that specifies the main mediation
rules to be executed. Following through the mediation logs you will notice
that the sequence named 'main' is executed on receiving the requests (The main
sequence acts as the default entry point for messages received by Synapse). Then
for the incoming message flow the 'in' mediator executes, and it calls the sequence
named 'stockquote'.

DEBUG SequenceMediator - Sequence mediator <main> :: mediate()
DEBUG InMediator - In mediator mediate()
DEBUG SequenceMediator - Sequence mediator <stockquote> :: mediate()

As the 'stockquote' sequence executes, the log mediator dumps a simple text/string
property, result of an XPath evaluation, that picks up the key named 'version',
and a second result of an XPath evaluation that picks up a local message property
set previously by the property mediator. The get-property() XPath extension
function is able to read message properties local to the current message, local
or remote registry entries, Axis2 message context properties as well as transport
headers. The local entry definition for 'version' defines a simple text/string
registry entry which is visible to all messages that pass through Synapse.

[HttpServerWorker-1] INFO LogMediator - Text = Sending quote request, version = 0.1, direction = incoming
[HttpServerWorker-1] DEBUG SendMediator - Send mediator :: mediate()
[HttpServerWorker-1] DEBUG AddressEndpoint - Sending To: http://localhost:9000/services/SimpleStockQuoteService

Responses from the Axis2 server will also get dispatched to the main sequence.
But because they are responses the in mediator will not be executed on them.
Only the out mediator will execute on these messages which simply sends them
back to the client using a send mediator.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample300"></a>

# Apache Synapse – Apache Synapse - Sample 300

## <a id="synapse-apache-org-userguide-samples-sample300--Sample_300:_Introduction_to_Synapse_Tasks"></a>Sample 300: Introduction to Synapse Tasks

<definitions xmlns="http://ws.apache.org/ns/synapse">
<task class="org.apache.synapse.startup.tasks.MessageInjector" name="CheckPrice">
<property name="to" value="http://localhost:9000/services/SimpleStockQuoteService"/>
<property name="soapAction" value="urn:getQuote"/>
<property name="format" value="soap11"/>
<property name="message">
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>IBM</m0:symbol>
</m0:request>
</m0:getQuote>
</property>
<trigger interval="5"/>
</task>
<sequence name="main">
<in>
<send/>
</in>
<out>
<log level="custom">
<property xmlns:ns="http://services.samples" name="Stock\_Quote\_on"
expression="//ns:return/ns:lastTradeTimestamp/child::text()"/>
<property xmlns:ns="http://services.samples" name="For\_the\_organization"
expression="//ns:return/ns:name/child::text()"/>
<property xmlns:ns="http://services.samples" name="Last\_Value"
expression="//ns:return/ns:last/child::text()"/>
</log>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample300--Objective"></a>Objective

Demonstrate how to schedule tasks in the Synapse runtime for periodic execution

### <a id="synapse-apache-org-userguide-samples-sample300--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 300 (repository/conf/sample/synapse\_sample\_300.xml)

  Unix/Linux: sh synapse.sh -sample 300  
  Windows: synapse.bat -sample 300

### <a id="synapse-apache-org-userguide-samples-sample300--Executing_the_Client"></a>Executing the Client

The above configuration adds a scheduled task to the Synapse runtime. The task
is configured to run every 5 seconds (note the 'interval' attribute on the 'trigger'
element).

One can write his/her own tasks implementing the org.apache.synapse.task.Task
interface and programming the 'execute' method to run the necessary logic. For
this particular sample we have used the MessageInjector class which just injects
a message into Synapse environment. In the configuration we have set the message
payload to be the stock quote request payload.

In this sample, injected messages will be sent to the sample Axis2 server which
will send back a response to Synapse. So every 5 seconds you will notice that Axis2
is generating a quote and Synapse is receiving the stock quote response.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample301"></a>

# Apache Synapse – Apache Synapse - Sample 301

## <a id="synapse-apache-org-userguide-samples-sample301--Sample_301:_Message_Injector_Task_to_invoke_a_named_sequence"></a>Sample 301: Message Injector Task to invoke a named sequence

<definitions xmlns="http://ws.apache.org/ns/synapse">
<task class="org.apache.synapse.startup.tasks.MessageInjector" name="InjectToSequenceTask">
<property name="soapAction" value="urn:getQuote"/>
<property name="format" value="soap11"/>
<property name="injectTo" value="sequence"/>
<property name="sequenceName" value="SampleSequence"/>
<property name="message">
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>IBM</m0:symbol>
</m0:request>
</m0:getQuote>
</property>
<trigger interval="5"/>
</task>
<sequence name="SampleSequence">
<log level="custom">
<property name="MSG" value="SampleSequence invoked"/>
</log>
<send receive="receivingSequence">
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</sequence>
<sequence name="receivingSequence">
<log level="custom">
<property xmlns:ns="http://services.samples" name="Stock\_Quote\_on"
expression="//ns:return/ns:lastTradeTimestamp/child::text()"/>
<property xmlns:ns="http://services.samples" name="For\_the\_organization"
expression="//ns:return/ns:name/child::text()"/>
<property xmlns:ns="http://services.samples" name="Last\_Value"
expression="//ns:return/ns:last/child::text()"/>
</log>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample301--Objective"></a>Objective

Demonstrate how to schedule tasks to invoke a named sequence periodically using
the MessageInjector task implementation

### <a id="synapse-apache-org-userguide-samples-sample301--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 301 (repository/conf/sample/synapse\_sample\_301.xml)

  Unix/Linux: sh synapse.sh -sample 301  
  Windows: synapse.bat -sample 301

### <a id="synapse-apache-org-userguide-samples-sample301--Executing_the_Client"></a>Executing the Client

The above configuration adds a scheduled task and sequences to the Synapse runtime.
The task is configured to run every 5 seconds (note the 'interval' attribute on
the 'trigger' element).

In this sample, the sequence "SampleSequence" will be invoked by the task and
then from the sequence, the injected messages will be sent to the sample Axis2
server, which will send back a response to Synapse. So every 5 seconds you will
notice that Axis2 is generating a quote and Synapse is receiving the stock quote
response. You will also see "SampleSequence invoked" message getting logged on
the console.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample302"></a>

# Apache Synapse – Apache Synapse - Sample 302

## <a id="synapse-apache-org-userguide-samples-sample302--Sample_302:_Message_Injector_Task_to_invoke_a_Proxy_service"></a>Sample 302: Message Injector Task to invoke a Proxy service

<definitions xmlns="http://ws.apache.org/ns/synapse">
<task class="org.apache.synapse.startup.tasks.MessageInjector" name="InjectToProxyTask">
<property name="soapAction" value="urn:getQuote"/>
<property name="format" value="soap11"/>
<property name="injectTo" value="proxy"/>
<property name="proxyName" value="SampleProxy"/>
<property name="message">
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>IBM</m0:symbol>
</m0:request>
</m0:getQuote>
</property>
<trigger interval="5"/>
</task>
<proxy name="SampleProxy" transports="http">
<target>
<inSequence>
<log level="custom">
<property name="MSG" value="SampleProxy invoked"/>
</log>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</inSequence>
<outSequence>
<log level="custom">
<property xmlns:ns="http://services.samples" name="Stock\_Quote\_on"
expression="//ns:return/ns:lastTradeTimestamp/child::text()"/>
<property xmlns:ns="http://services.samples" name="For\_the\_organization"
expression="//ns:return/ns:name/child::text()"/>
<property xmlns:ns="http://services.samples" name="Last\_Value"
expression="//ns:return/ns:last/child::text()"/>
</log>
<drop/>
</outSequence>
</target>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample302--Objective"></a>Objective

Demonstrate how to schedule tasks to invoke a Proxy service periodically using
the MessageInjector task implementation

### <a id="synapse-apache-org-userguide-samples-sample302--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 302 (repository/conf/sample/synapse\_sample\_302.xml)

  Unix/Linux: sh synapse.sh -sample 302  
  Windows: synapse.bat -sample 302

### <a id="synapse-apache-org-userguide-samples-sample302--Executing_the_Client"></a>Executing the Client

The above configuration adds a scheduled task, and a proxy service to the Synapse
runtime. The task is configured to run every 5 seconds (note the 'interval'
attribute on the 'trigger' element).

In this sample, the proxy service "SampleProxy" will be invoked by the task and
then from the proxy service, the injected messages will be sent to the sample
Axis2 server, which will send back a response to Synapse. So every 5 seconds you
will notice that Axis2 is generating a quote and Synapse is receiving the stock
quote response. You will also see the "SampleProxy invoked" message getting
logged on the console.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample350"></a>

# Apache Synapse – Apache Synapse - Sample 350

## <a id="synapse-apache-org-userguide-samples-sample350--Sample_350:_Introduction_to_the_Script_Mediator_using_JavaScript"></a>Sample 350: Introduction to the Script Mediator using JavaScript

<definitions xmlns="http://ws.apache.org/ns/synapse">
<registry provider="org.apache.synapse.registry.url.SimpleURLRegistry">
<!-- the root property of the simple URL registry helps resolve a resource URL as root + key -->
<parameter name="root">file:repository/conf/sample/resources/</parameter>
<!-- all resources loaded from the URL registry would be cached for this number of milli seconds -->
<parameter name="cachableDuration">15000</parameter>
</registry>
<localEntry key="stockquoteScript"
src="file:repository/conf/sample/resources/script/stockquoteTransformRequest.js"/>
<sequence name="main">
<in>
<!-- transform the custom quote request into a standard quote request expected by the service -->
<script language="js" key="stockquoteScript" function="transformRequest"/>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</in>
<out>
<!-- transform the standard response back into the custom format the client expects -->
<script language="js" key="script/stockquoteTransformResponse.js"
function="transformResponse"/>
<send/>
</out>
</sequence>
</definitions>

The JavaScript resource file referenced by the configuration looks like this.

<x><![CDATA[
function transformRequest(mc) {
var symbol = mc.getPayloadXML()..\*::Code.toString();
mc.setPayloadXML(
<m:getQuote xmlns:m="http://services.samples">
<m:request>
<m:symbol>{symbol}</m:symbol>
</m:request>
</m:getQuote>);
}
function transformResponse(mc) {
var symbol = mc.getPayloadXML()..\*::symbol.toString();
var price = mc.getPayloadXML()..\*::last.toString();
mc.setPayloadXML(
<m:CheckPriceResponse xmlns:m="http://www.apache-synapse.org/test">
<m:Code>{symbol}</m:Code>
<m:Price>{price}</m:Price>
</m:CheckPriceResponse>);
}
]]></x>

### <a id="synapse-apache-org-userguide-samples-sample350--Objective"></a>Objective

Showcase the ability to configure the Synapse runtime using common scripting
languages such as JavaScript

### <a id="synapse-apache-org-userguide-samples-sample350--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 350 (repository/conf/sample/synapse\_sample\_350.xml)

  Unix/Linux: sh synapse.sh -sample 350  
  Windows: synapse.bat -sample 350

### <a id="synapse-apache-org-userguide-samples-sample350--Executing_the_Client"></a>Executing the Client

This sample is similar to [sample 8](#synapse-apache-org-userguide-samples-sample8) but instead of using
XSLT, the transformation is done using JavaScript and E4X. Note that the script
source is loaded from a resource in the file system which must be wrapped in
CDATA tags within an XML element. The script used in this example has two functions,
'transformRequest' and 'transformResponse'. The Synapse configuration uses the
'function' attribute to specify which function should be invoked. Use the stock
quote client to send a custom quote request as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=customquote

Synapse uses the script mediator and the specified JavaScript function to convert
the custom request to a standard quote request. Subsequently the response received
is transformed and sent back to the client.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample351"></a>

# Apache Synapse – Apache Synapse - Sample 351

## <a id="synapse-apache-org-userguide-samples-sample351--Sample_351:_Inline_Scripts_with_the_Script_Mediator"></a>Sample 351: Inline Scripts with the Script Mediator

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<!-- transform the custom quote request into a standard quote requst expected by the service -->
<script language="js">
var symbol = mc.getPayloadXML()..\*::Code.toString();
mc.setPayloadXML(
<m:getQuote xmlns:m="http://services.samples">
<m:request>
<m:symbol>{symbol}</m:symbol>
</m:request>
</m:getQuote>);
</script>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</in>
<out>
<!-- transform the standard response back into the custom format the client expects -->
<script language="js">
var symbol = mc.getPayloadXML()..\*::symbol.toString();
var price = mc.getPayloadXML()..\*::last.toString();
mc.setPayloadXML(
<m:CheckPriceResponse xmlns:m="http://services.samples/xsd">
<m:Code>{symbol}</m:Code>
<m:Price>{price}</m:Price>
</m:CheckPriceResponse>);
</script>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample351--Objective"></a>Objective

[Sample 350](#synapse-apache-org-userguide-samples-sample350) shows how to use scripts stored as
external resources for mediation. This sample demonstrates how small scriplets
can be specified inline with the Synapse configuration thus avoiding the requirement
to have an external registry.

### <a id="synapse-apache-org-userguide-samples-sample351--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 351 (repository/conf/sample/synapse\_sample\_351.xml)

  Unix/Linux: sh synapse.sh -sample 351  
  Windows: synapse.bat -sample 351

### <a id="synapse-apache-org-userguide-samples-sample351--Executing_the_Client"></a>Executing the Client

The functionality and the behavior of this sample is identical to
[sample 350](#synapse-apache-org-userguide-samples-sample350). Only difference is that, the 2 JS functions
are embedded in the Synapse configuration. To try this out run the following
command on the sample client.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=customquote

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample352"></a>

# Apache Synapse – Apache Synapse - Sample 352

## <a id="synapse-apache-org-userguide-samples-sample352--Sample_352:_Accessing_the_Synapse_MessageContext_API_Through_Scripts"></a>Sample 352: Accessing the Synapse MessageContext API Through Scripts

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<!-- change the MessageContext into a response and set a response payload -->
<script language="js">
mc.setTo(mc.getReplyTo());
mc.setProperty("RESPONSE", "true");
mc.setPayloadXML(
&lt;ns:getQuoteResponse xmlns:ns="http://services.samples"&gt;
&lt;ns:return&gt;
&lt;ns:last&gt;99.9&lt;/ns:last&gt;
&lt;/ns:return&gt;
&lt;/ns:getQuoteResponse&gt;);
</script>
</in>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample352--Objective"></a>Objective

Demonstrate how to access various methods on the Synapse MessageContext API
using the script mediator

### <a id="synapse-apache-org-userguide-samples-sample352--Pre-requisites"></a>Pre-requisites

- Start Synapse using the configuration numbered 352 (repository/conf/sample/synapse\_sample\_352.xml)

  Unix/Linux: sh synapse.sh -sample 352  
  Windows: synapse.bat -sample 352

### <a id="synapse-apache-org-userguide-samples-sample352--Executing_the_Client"></a>Executing the Client

This example shows how an inline JavaScript can access the Synapse message context
API to set its 'To' EPR and to set a custom property to mark it as a response. Execute
the stock quote client, and you will receive the response '99.9' as the last sale
price as per the above script.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

Note that the symbol 'mc' is bound to the Synapse MessageContext object by the
script mediator. Then the user can invoke various methods on the message context
using common JavaScript syntax such as mc.getProperty('name') and mc.setTo('epr').

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample353"></a>

# Apache Synapse – Apache Synapse - Sample 353

## <a id="synapse-apache-org-userguide-samples-sample353--Sample_353:_Using_Ruby_Scripts_for_Mediation"></a>Sample 353: Using Ruby Scripts for Mediation

<definitions xmlns="http://ws.apache.org/ns/synapse">
<localEntry key="stockquoteScript"
src="file:repository/conf/sample/resources/script/stockquoteTransform.rb"/>
<sequence name="main">
<in>
<!-- transform the custom quote request into a standard quote request expected by the service -->
<script language="rb" key="stockquoteScript" function="transformRequest"/>
<!-- send message to real endpoint referenced by name "stockquote" and stop -->
<send>
<endpoint name="stockquote">
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</in>
<out>
<!-- transform the standard response back into the custom format the client expects -->
<script language="rb" key="stockquoteScript" function="transformResponse"/>
<send/>
</out>
</sequence>
</definitions>

The external script referenced by the configuration contains the following Ruby
scriplet.

<x><![CDATA[
require 'rexml/document'
include REXML
def transformRequest(mc)
newRequest= Document.new '<m:getQuote xmlns:m="http://services.samples">'<<
'<m:request><m:symbol></m:symbol></m:request></m:getQuote>'
newRequest.root.elements[1].elements[1].text = mc.getPayloadXML().root.elements[1].get\_text
mc.setPayloadXML(newRequest)
end
def transformResponse(mc)
newResponse = Document.new '<m:CheckPriceResponse xmlns:m="http://www.apache-synapse.org/test"><m:Code>' <<
'</m:Code><m:Price></m:Price></m:CheckPriceResponse>'
newResponse.root.elements[1].text = mc.getPayloadXML().root.elements[1].elements[1].get\_text
newResponse.root.elements[2].text = mc.getPayloadXML().root.elements[1].elements[2].get\_text
mc.setPayloadXML(newResponse)
end
]]></x>

### <a id="synapse-apache-org-userguide-samples-sample353--Objective"></a>Objective

The script mediator of Synapse can be programmed using any BSF compatible
programming language. [Sample 250](#synapse-apache-org-userguide-samples-sample250) shows how to
configure it using JavaScript. This sample shows how to configure the script
mediator with Ruby.

### <a id="synapse-apache-org-userguide-samples-sample353--Pre-requisites"></a>Pre-requisites

- This sample uses Ruby so first setup support for this in Synapse as described at
  [Configuring JRuby](#synapse-apache-org-userguide-samples-setup-script--ruby)
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Synapse does not ship with a Ruby engine by default. Therefore you should
  download the Ruby engine from JRuby site and copy the downloaded jar file
  to the 'lib' directory of Synapse.
- Start Synapse using the configuration numbered 353 (repository/conf/sample/synapse\_sample\_353.xml)

  Unix/Linux: sh synapse.sh -sample 353  
  Windows: synapse.bat -sample 353

### <a id="synapse-apache-org-userguide-samples-sample353--Executing_the_Client"></a>Executing the Client

This sample is identical to [sample 350](#synapse-apache-org-userguide-samples-sample350) with the
only difference being the use of Ruby instead of JavaScript. Use the stock
quote client to send a custom quote request as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=customquote

The Ruby scriplets will transform the requests and responses as they flow through
the service bus.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample354"></a>

# Apache Synapse – Apache Synapse - Sample 354

## <a id="synapse-apache-org-userguide-samples-sample354--Sample_354:_Using_Inline_Ruby_Scripts_for_Mediation"></a>Sample 354: Using Inline Ruby Scripts for Mediation

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<script language="rb">
require 'rexml/document'
include REXML
newRequest= Document.new '<m:getQuote xmlns:m="http://services.samples"><m:request><m:symbol>...test...</m:symbol></m:request></m:getQuote>'
newRequest.root.elements[1].elements[1].text =
$mc.getPayloadXML().root.elements[1].get\_text
$mc.setPayloadXML(newRequest)
</script>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</in>
<out>
<script language="rb">
require 'rexml/document'
include REXML
newResponse = Document.new '<m:CheckPriceResponse
xmlns:m="http://services.samples/xsd"><m:Code></m:Code><m:Price></m:Price></m:CheckPriceResponse>'
newResponse.root.elements[1].text =
$mc.getPayloadXML().root.elements[1].elements[1].get\_text
newResponse.root.elements[2].text =
$mc.getPayloadXML().root.elements[1].elements[2].get\_text
$mc.setPayloadXML(newResponse)
</script>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample354--Objective"></a>Objective

Shows how to embed Ruby scripts in the Synapse configuration itself.

### <a id="synapse-apache-org-userguide-samples-sample354--Pre-requisites"></a>Pre-requisites

- This sample uses Ruby, so first setup support for this in Synapse as described at
  [Configuring JRuby](#synapse-apache-org-userguide-samples-setup-script--ruby)
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Synapse does not ship with a Ruby engine by default. Therefore you should
  download the Ruby engine from JRuby site and copy the downloaded jar file
  to the 'lib' directory of Synapse.
- Start Synapse using the configuration numbered 354 (repository/conf/sample/synapse\_sample\_354.xml)

  Unix/Linux: sh synapse.sh -sample 354  
  Windows: synapse.bat -sample 354

### <a id="synapse-apache-org-userguide-samples-sample354--Executing_the_Client"></a>Executing the Client

Run the sample client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=customquote

The inline Ruby scripts will transform the requests and responses.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample355"></a>

# Apache Synapse – Apache Synapse - Sample 355

## <a id="synapse-apache-org-userguide-samples-sample355--Sample_355:_Using_Python_Scripts_for_Mediation"></a>Sample 355: Using Python Scripts for Mediation

<definitions xmlns="http://ws.apache.org/ns/synapse">
<registry provider="org.apache.synapse.registry.url.SimpleURLRegistry">
<!-- the root property of the simple URL registry helps resolve a resource URL as root + key -->
<parameter name="root">file:repository/conf/sample/resources/</parameter>
<!-- all resources loaded from the URL registry would be cached for this number of milli seconds -->
<parameter name="cachableDuration">15000</parameter>
</registry>
<localEntry key="stockquoteScript"
src="file:repository/conf/sample/resources/script/stockquoteTransformRequest.py"/>
<sequence name="main">
<in>
<!-- transform the custom quote request into a standard quote request expected by the service -->
<script language="py" key="stockquoteScript" function="transformRequest"/>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</in>
<out>
<!-- transform the standard response back into the custom format the client expects -->
<script language="py" key="script/stockquoteTransformResponse.py"
function="transformResponse"/>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample355--Objective"></a>Objective

Shows how to embed Python scripts in the Synapse configuration itself.

### <a id="synapse-apache-org-userguide-samples-sample355--Pre-requisites"></a>Pre-requisites

- This sample uses Jython, so first setup support for this in Synapse as described at
  [Configuring Jython](#synapse-apache-org-userguide-samples-setup-script--python)
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Synapse does not ship with a Jython engine by default. Therefore you should
  download the Jython engine from Jython site and copy the downloaded jar file
  to the 'lib' directory of Synapse.
- Start Synapse using the configuration numbered 355 (repository/conf/sample/synapse\_sample\_355.xml)

  Unix/Linux: sh synapse.sh -sample 355  
  Windows: synapse.bat -sample 355

### <a id="synapse-apache-org-userguide-samples-sample355--Executing_the_Client"></a>Executing the Client

Run the sample client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=customquote

The Python scripts will transform the requests and responses.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample360"></a>

# Apache Synapse – Apache Synapse - Sample 360

## <a id="synapse-apache-org-userguide-samples-sample360--Sample_360:_Introduction_to_DBLookup_Mediator"></a>Sample 360: Introduction to DBLookup Mediator

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="myFaultHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason expression="get-property('ERROR\_MESSAGE')"/>
</makefault>
<send/>
<drop/>
</sequence>
<sequence name="main" onError="myFaultHandler">
<in>
<log level="custom">
<property name="text" value="\*\* Looking up from the Database \*\*"/>
</log>
<dblookup>
<connection>
<pool>
<driver>org.apache.derby.jdbc.ClientDriver</driver>
<url>jdbc:derby://localhost:1527/synapsedb;create=false</url>
<user>synapse</user>
<password>synapse</password>
</pool>
</connection>
<statement>
<sql>select \* from company where name =?</sql>
<parameter xmlns:m0="http://services.samples"
expression="//m0:getQuote/m0:request/m0:symbol" type="VARCHAR"/>
<result name="company\_id" column="id"/>
</statement>
</dblookup>
<switch source="get-property('company\_id')">
<case regex="c1">
<log level="custom">
<property name="text"
expression="fn:concat('Company ID - ',get-property('company\_id'))"/>
</log>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</case>
<case regex="c2">
<log level="custom">
<property name="text"
expression="fn:concat('Company ID - ',get-property('company\_id'))"/>
</log>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</case>
<case regex="c3">
<log level="custom">
<property name="text"
expression="fn:concat('Company ID - ',get-property('company\_id'))"/>
</log>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</case>
<default>
<log level="custom">
<property name="text" value="\*\* Unrecognized Company ID \*\*"/>
</log>
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope"
value="tns:Receiver"/>
<reason value="\*\* Unrecognized Company ID \*\*"/>
</makefault>
<send/>
<drop/>
</default>
</switch>
<drop/>
</in>
<out>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample360--Objective"></a>Objective

Demonstrating how to perform database lookups during mediation using the dblookup
mediator

### <a id="synapse-apache-org-userguide-samples-sample360--Pre-requisites"></a>Pre-requisites

- Setup a Derby database as described in the [database setup guide](#synapse-apache-org-userguide-samples-setup-db)
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 360 (repository/conf/sample/synapse\_sample\_360.xml)

  Unix/Linux: sh synapse.sh -sample 360  
  Windows: synapse.bat -sample 360

### <a id="synapse-apache-org-userguide-samples-sample360--Executing_the_Client"></a>Executing the Client

This sample demonstrates simple database read operations through Synapse. When a
message arrives at dblookup mediator, it opens a connection to the database and
executes the given SQL query. The SQL query uses '?' character for attributes that
will be filled at runtime. The parameters define how to calculate the value of
those attributes at runtime. In this sample a dblookup mediator has been used to
extract 'id' of the company from the company database using the symbol which is
extracted from the SOAP envelope by evaluating an XPath. Then 'id' bases switching
will be done by a switch mediator.

To try this out, first request a stock quote for the symbol 'IBM' as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=IBM

Synapse console will display the following message.

INFO LogMediator text = \*\* Looking up from the Database \*\*
INFO LogMediator text = Company ID – c1

Now request a quote for the symbol 'SUN'.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=SUN

Synapse will display the following output.

INFO LogMediator text = \*\* Looking up from the Database \*\*
INFO LogMediator text = Company ID – c2

Finally send a stock quote request for the symbol 'MSFT'.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=MSFT

In this case Synapse will display the following output.

INFO LogMediator text = \*\* Looking up from the Database \*\*
INFO LogMediator text = Company ID – c2

If you send any requests with different symbols, dblookup mediator will return
an empty result set, since those symbols are not stored in the Derby database.
So as a result Synapse will not be able to determine the company ID, which will
result in the following log entry (from the default case in the switch mediator).

INFO LogMediator text = \*\* Unrecognized Company ID \*\*

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample361"></a>

# Apache Synapse – Apache Synapse - Sample 361

## <a id="synapse-apache-org-userguide-samples-sample361--Sample_361:_Introduction_to_DBReport_Mediator"></a>Sample 361: Introduction to DBReport Mediator

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</in>
<out>
<log level="custom">
<property name="text" value="\*\* Reporting to the Database \*\*"/>
</log>
<dbreport>
<connection>
<pool>
<driver>org.apache.derby.jdbc.ClientDriver</driver>
<url>jdbc:derby://localhost:1527/synapsedb;create=false</url>
<user>synapse</user>
<password>synapse</password>
</pool>
</connection>
<statement>
<sql>update company set price=? where name =?</sql>
<parameter xmlns:m0="http://services.samples"
expression="//m0:return/m0:last/child::text()" type="DOUBLE"/>
<parameter xmlns:m0="http://services.samples"
expression="//m0:return/m0:symbol/child::text()" type="VARCHAR"/>
</statement>
</dbreport>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample361--Objective"></a>Objective

[Sample 360](#synapse-apache-org-userguide-samples-sample360) shows how to perform database lookups
in Synapse. This sample illustrates how to write to a given database from
Synapse using the dbreport mediator.

### <a id="synapse-apache-org-userguide-samples-sample361--Pre-requisites"></a>Pre-requisites

- Setup a Derby database as described in the [database setup guide](#synapse-apache-org-userguide-samples-setup-db)
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 361 (repository/conf/sample/synapse\_sample\_361.xml)

  Unix/Linux: sh synapse.sh -sample 361  
  Windows: synapse.bat -sample 361

### <a id="synapse-apache-org-userguide-samples-sample361--Executing_the_Client"></a>Executing the Client

This sample demonstrates how to perform simple database write operations in
Synapse. The dbreport mediator writes (i.e. inserts one row) to a table using the
details available in messages. It works the same way as the dblookup mediator.
In this sample, dbreport mediator is used for updating the stock price of the
company using the last quote value which is calculated by evaluating an XPath
against the response message. After running this sample, user can check the
company table using the Derby client tool. It will show the value inserted by the
dbreport mediator.

To try this out run the sample client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=IBM

Now execute the following SQL query against the Derby database using the Derby
client tool.

select price from company where name='IBM';

This operation will return the stock quote value returned earlier by Axis2. You
can compare the output of the sample Axis2 client with the output of the Derby
client tool for confirmation.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample362"></a>

# Apache Synapse – Apache Synapse - Sample 362

## <a id="synapse-apache-org-userguide-samples-sample362--Sample_362:_Perform_Database_Lookups_and_Updates_in_the_Same_Mediation_Sequence"></a>Sample 362: Perform Database Lookups and Updates in the Same Mediation Sequence

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</in>
<out>
<log level="custom">
<property name="text" value="\*\* Reporting to the Database \*\*"/>
</log>
<dbreport>
<connection>
<pool>
<driver>org.apache.derby.jdbc.ClientDriver</driver>
<url>jdbc:derby://localhost:1527/synapsedb;create=false</url>
<user>synapse</user>
<password>synapse</password>
</pool>
</connection>
<statement>
<sql>update company set price=? where name =?</sql>
<parameter xmlns:m0="http://services.samples"
expression="//m0:return/m0:last/child::text()" type="DOUBLE"/>
<parameter xmlns:m0="http://services.samples"
expression="//m0:return/m0:symbol/child::text()" type="VARCHAR"/>
</statement>
</dbreport>
<log level="custom">
<property name="text" value="\*\* Looking up from the Database \*\*"/>
</log>
<dblookup>
<connection>
<pool>
<driver>org.apache.derby.jdbc.ClientDriver</driver>
<url>jdbc:derby://localhost:1527/synapsedb;create=false</url>
<user>synapse</user>
<password>synapse</password>
</pool>
</connection>
<statement>
<sql>select \* from company where name =?</sql>
<parameter xmlns:m0="http://services.samples"
expression="//m0:return/m0:symbol/child::text()" type="VARCHAR"/>
<result name="stock\_price" column="price"/>
</statement>
</dblookup>
<log level="custom">
<property name="text"
expression="fn:concat('Stock price - ',get-property('stock\_price'))"/>
</log>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample362--Objective"></a>Objective

[Sample 360](#synapse-apache-org-userguide-samples-sample360) and [sample 361](#synapse-apache-org-userguide-samples-sample361)
shows how to use the dblookup mediator and dbreport mediator separately. This sample
combines them in a single mediation sequence to perform both database lookup and
update operations.

### <a id="synapse-apache-org-userguide-samples-sample362--Pre-requisites"></a>Pre-requisites

- Setup a Derby database as described in the [database setup guide](#synapse-apache-org-userguide-samples-setup-db)
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 362 (repository/conf/sample/synapse\_sample\_362.xml)

  Unix/Linux: sh synapse.sh -sample 362  
  Windows: synapse.bat -sample 362

### <a id="synapse-apache-org-userguide-samples-sample362--Executing_the_Client"></a>Executing the Client

In this sample, the dbreport mediator works the same way as in
[sample 361](#synapse-apache-org-userguide-samples-sample361). It updates the price for the given company
using the response messages content. Then the dblookup mediator reads the last
updated value from the company database and logs it to the console.

Run the sample client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=IBM

Synapse will update the database using the stock quote value available in the
response sent by Axis2. Then the same value will be retrieved from the database
and logged as follows.

INFO LogMediator text = \*\* Reporting to the Database \*\*
...
INFO LogMediator text = \*\* Looking up from the Database \*\*
...
INFO LogMediator text = Stock price - 153.47886496064808

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample363"></a>

# Apache Synapse – Apache Synapse - Sample 363

## <a id="synapse-apache-org-userguide-samples-sample363--Sample_363:_Reusable_Database_Connection_Pools"></a>Sample 363: Reusable Database Connection Pools

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="myFaultHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason expression="get-property('ERROR\_MESSAGE')"/>
</makefault>
<send/>
<drop/>
</sequence>
<sequence name="main" onError="myFaultHandler">
<in>
<log level="custom">
<property name="text" value="\*\* Looking up from the Database \*\*"/>
</log>
<dblookup>
<connection>
<pool>
<dsName>lookupdb</dsName>
</pool>
</connection>
<statement>
<sql>select \* from company where name =?</sql>
<parameter xmlns:m0="http://services.samples"
expression="//m0:getQuote/m0:request/m0:symbol" type="VARCHAR"/>
<result name="company\_id" column="id"/>
</statement>
</dblookup>
<switch source="get-property('company\_id')">
<case regex="c1">
<log level="custom">
<property name="text"
expression="fn:concat('Company ID - ',get-property('company\_id'))"/>
</log>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</case>
<case regex="c2">
<log level="custom">
<property name="text"
expression="fn:concat('Company ID - ',get-property('company\_id'))"/>
</log>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</case>
<case regex="c3">
<log level="custom">
<property name="text"
expression="fn:concat('Company ID - ',get-property('company\_id'))"/>
</log>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</case>
<default>
<log level="custom">
<property name="text" value="\*\* Unrecognized Company ID \*\*"/>
</log>
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope"
value="tns:Receiver"/>
<reason value="\*\* Unrecognized Company ID \*\*"/>
</makefault>
<send/>
<drop/>
</default>
</switch>
<drop/>
</in>
<out>
<log level="custom">
<property name="text" value="\*\* Reporting to the Database \*\*"/>
</log>
<dbreport>
<connection>
<pool>
<dsName>reportdb</dsName>
</pool>
</connection>
<statement>
<sql>update company set price=? where name =?</sql>
<parameter xmlns:m0="http://services.samples"
expression="//m0:return/m0:last/child::text()" type="DOUBLE"/>
<parameter xmlns:m0="http://services.samples"
expression="//m0:return/m0:symbol/child::text()" type="VARCHAR"/>
</statement>
</dbreport>
<log level="custom">
<property name="text" value="\*\* Looking up from the Database \*\*"/>
</log>
<dblookup>
<connection>
<pool>
<dsName>reportdb</dsName>
</pool>
</connection>
<statement>
<sql>select \* from company where name =?</sql>
<parameter xmlns:m0="http://services.samples"
expression="//m0:return/m0:symbol/child::text()" type="VARCHAR"/>
<result name="stock\_price" column="price"/>
</statement>
</dblookup>
<log level="custom">
<property name="text"
expression="fn:concat('Stock price - ',get-property('stock\_price'))"/>
</log>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample363--Objective"></a>Objective

Demonstrate how to setup reusable connection pools for the dblookup and dbreport
mediators

### <a id="synapse-apache-org-userguide-samples-sample363--Pre-requisites"></a>Pre-requisites

- Setup a Derby database and the Synapse data sources as described in the
  [database setup guide](#synapse-apache-org-userguide-samples-setup-db)
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 363 (repository/conf/sample/synapse\_sample\_363.xml)

  Unix/Linux: sh synapse.sh -sample 363  
  Windows: synapse.bat -sample 363

### <a id="synapse-apache-org-userguide-samples-sample363--Executing_the_Client"></a>Executing the Client

This sample employs two instances of the dblookup mediator and a single instance
of the dbreport mediator. The two dblookup mediators are sharing the same database
connection pool named 'lookupdb'. The dbreport mediator makes use of a different
connection pool named 'dbreport'. Synapse uses Apache DBCP to create and manage
the corresponding data sources and connection pools.

Run this sample by invoking the client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=IBM

Synapse will log the following output as it reads from and writes to the database.

INFO LogMediator text = \*\* Looking up from the Database \*\* ...
INFO LogMediator text = Company ID - c1 ...
INFO LogMediator text = \*\* Reporting to the Database \*\* ...
INFO LogMediator text = \*\* Looking up from the Database \*\* ...
INFO LogMediator text = Stock price - 183.3635460215262

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample364"></a>

# Apache Synapse – Apache Synapse - Sample 364

## <a id="synapse-apache-org-userguide-samples-sample364--Sample_364:_Executing_Database_Stored_Procedures"></a>Sample 364: Executing Database Stored Procedures

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</in>
<out>
<log level="custom">
<property name="text" value="\*\* Reporting to the Database \*\*"/>
</log>
<dbreport>
<connection>
<pool>
<driver>com.mysql.jdbc.Driver</driver>
<url>jdbc:mysql://localhost:3306/synapsedb</url>
<user>user</user>
<password>password</password>
</pool>
</connection>
<statement>
<sql>call updateCompany(?,?)</sql>
<parameter xmlns:m0="http://services.samples"
expression="//m0:return/m0:last/child::text()" type="DOUBLE"/>
<parameter xmlns:m0="http://services.samples"
expression="//m0:return/m0:symbol/child::text()" type="VARCHAR"/>
</statement>
</dbreport>
<log level="custom">
<property name="text" value="\*\* Looking up from the Database \*\*"/>
</log>
<dblookup>
<connection>
<pool>
<driver>com.mysql.jdbc.Driver</driver>
<url>jdbc:mysql://localhost:3306/synapsedb</url>
<user>user</user>
<password>password</password>
</pool>
</connection>
<statement>
<sql>call getCompany(?)</sql>
<parameter xmlns:m0="http://services.samples"
expression="//m0:return/m0:symbol/child::text()" type="VARCHAR"/>
<result name="stock\_prize" column="price"/>
</statement>
</dblookup>
<log level="custom">
<property name="text"
expression="fn:concat('Stock Prize - ',get-property('stock\_prize'))"/>
</log>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample364--Objective"></a>Objective

Demonstrate how to invoke a database stored procedure from Synapse

### <a id="synapse-apache-org-userguide-samples-sample364--Pre-requisites"></a>Pre-requisites

- Setup a MySQL database as described in the [database setup guide](#synapse-apache-org-userguide-samples-setup-db--mysql)
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Open the repository/conf/sample/synapse\_sample\_364.xml file and change the
  database username, password credentials accordingly
- Start Synapse using the configuration numbered 364 (repository/conf/sample/synapse\_sample\_364.xml)

  Unix/Linux: sh synapse.sh -sample 364  
  Windows: synapse.bat -sample 364

### <a id="synapse-apache-org-userguide-samples-sample364--Executing_the_Client"></a>Executing the Client

This scenario is very similar to [sample 363](#synapse-apache-org-userguide-samples-sample363), but makes
use of stored procedures to lookup and update the database instead of simple
SQL queries. Note that we are still using the dblookup and dbreport mediators
to access the database but the statements are simply calling a stored procedure in
MySQL (the syntax to call a stored procedue is database engine specific).

To try this sample out, invoke the sample client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=IBM

Synapse will invoke the two stored procedures as the response is mediated back
to the client. You will see the following output on the Synapse console.

INFO LogMediator text = \*\* Looking up from the Database \*\* ...
INFO LogMediator text = Company ID - c1 ...
INFO LogMediator text = Stock price - 183.3635460215262

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample370"></a>

# Apache Synapse – Apache Synapse - Sample 370

## <a id="synapse-apache-org-userguide-samples-sample370--Sample_370:_Introduction_to_Throttle_Mediator_and_Concurrency_Throttling"></a>Sample 370: Introduction to Throttle Mediator and Concurrency Throttling

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<throttle id="A">
<policy>
<!-- define throttle policy -->
<wsp:Policy xmlns:wsp="http://schemas.xmlsoap.org/ws/2004/09/policy"
xmlns:throttle="http://www.wso2.org/products/wso2commons/throttle">
<throttle:ThrottleAssertion>
<throttle:MaximumConcurrentAccess>10</throttle:MaximumConcurrentAccess>
</throttle:ThrottleAssertion>
</wsp:Policy>
</policy>
<onAccept>
<log level="custom">
<property name="text" value="\*\*Access Accept\*\*"/>
</log>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</onAccept>
<onReject>
<log level="custom">
<property name="text" value="\*\*Access Denied\*\*"/>
</log>
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope"
value="tns:Receiver"/>
<reason value="\*\*Access Denied\*\*"/>
</makefault>
<send/>
<drop/>
</onReject>
</throttle>
</in>
<out>
<throttle id="A"/>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample370--Objective"></a>Objective

Showcase the ability of Synapse to throttle incoming requests based on the
concurrency level

### <a id="synapse-apache-org-userguide-samples-sample370--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 370 (repository/conf/sample/synapse\_sample\_370.xml)

  Unix/Linux: sh synapse.sh -sample 370  
  Windows: synapse.bat -sample 370

### <a id="synapse-apache-org-userguide-samples-sample370--Executing_the_Client"></a>Executing the Client

Above configuration specifies a throttle mediator inside the in mediator.
Therefore, all request messages directed to the main sequence will be subjected
to throttling. Throttle mediator has 'policy', 'onAccept' and 'onReject' tags at
top level. The 'policy' tag specifies the throttling policy for throttling messages.
This sample policy only contains a component called 'MaximumConcurrentAccess'.
This indicates the maximum number of concurrent requests that can pass through
Synapse on a single unit of time. To test concurrency throttling, it is required
to send concurrent requests to Synapse. With this configuration if Synapse receives
20 requests concurrently from clients, then approximately half of those will succeed
while the others being throttled. The client command to try this is as follows.

ant stockquote -Dsymbol=IBM -Dmode=quote -Daddurl=http://localhost:8280/

It's not that easy to try this sample out using the sample Axis2 client. For
better results, consider using a load testing tool like Apache Bench or Java Bench.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample371"></a>

# Apache Synapse – Apache Synapse - Sample 371

## <a id="synapse-apache-org-userguide-samples-sample371--Sample_371:_Restricting_Requests_Based_on_Policies"></a>Sample 371: Restricting Requests Based on Policies

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<throttle id="A">
<policy>
<!-- define throttle policy -->
<wsp:Policy xmlns:wsp="http://schemas.xmlsoap.org/ws/2004/09/policy"
xmlns:throttle="http://www.wso2.org/products/wso2commons/throttle">
<throttle:ThrottleAssertion>
<wsp:All>
<throttle:ID throttle:type="IP">other</throttle:ID>
<wsp:ExactlyOne>
<wsp:All>
<throttle:MaximumCount>4</throttle:MaximumCount>
<throttle:UnitTime>800000</throttle:UnitTime>
<throttle:ProhibitTimePeriod wsp:Optional="true">10000
</throttle:ProhibitTimePeriod>
</wsp:All>
<throttle:IsAllow>true</throttle:IsAllow>
</wsp:ExactlyOne>
</wsp:All>
<wsp:All>
<throttle:ID throttle:type="IP">192.168.8.200-192.168.8.222
</throttle:ID>
<wsp:ExactlyOne>
<wsp:All>
<throttle:MaximumCount>8</throttle:MaximumCount>
<throttle:UnitTime>800000</throttle:UnitTime>
<throttle:ProhibitTimePeriod wsp:Optional="true">10
</throttle:ProhibitTimePeriod>
</wsp:All>
<throttle:IsAllow>true</throttle:IsAllow>
</wsp:ExactlyOne>
</wsp:All>
<wsp:All>
<throttle:ID throttle:type="IP">192.168.8.201</throttle:ID>
<wsp:ExactlyOne>
<wsp:All>
<throttle:MaximumCount>200</throttle:MaximumCount>
<throttle:UnitTime>600000</throttle:UnitTime>
<throttle:ProhibitTimePeriod wsp:Optional="true"/>
</wsp:All>
<throttle:IsAllow>true</throttle:IsAllow>
</wsp:ExactlyOne>
</wsp:All>
<wsp:All>
<throttle:ID throttle:type="IP">192.168.8.198</throttle:ID>
<wsp:ExactlyOne>
<wsp:All>
<throttle:MaximumCount>50</throttle:MaximumCount>
<throttle:UnitTime>500000</throttle:UnitTime>
<throttle:ProhibitTimePeriod wsp:Optional="true"/>
</wsp:All>
<throttle:IsAllow>true</throttle:IsAllow>
</wsp:ExactlyOne>
</wsp:All>
</throttle:ThrottleAssertion>
</wsp:Policy>
</policy>
<onAccept>
<log level="custom">
<property name="text" value="\*\*Access Accept\*\*"/>
</log>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</onAccept>
<onReject>
<log level="custom">
<property name="text" value="\*\*Access Denied\*\*"/>
</log>
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope"
value="tns:Receiver"/>
<reason value="\*\*Access Denied\*\*"/>
</makefault>
<send/>
<drop/>
</onReject>
</throttle>
</in>
<out>
<throttle id="A"/>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample371--Objective"></a>Objective

Demonstrate how to throttle incoming requests based on complex policies

### <a id="synapse-apache-org-userguide-samples-sample371--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 371 (repository/conf/sample/synapse\_sample\_371.xml)

  Unix/Linux: sh synapse.sh -sample 371  
  Windows: synapse.bat -sample 371

### <a id="synapse-apache-org-userguide-samples-sample371--Executing_the_Client"></a>Executing the Client

Above configuration specifies a throttle mediator inside the in mediator.
Therefore, all request messages directed to the main sequence will be subjected
to throttling. Throttle mediator has policy, onAccept and onReject tags at the
top level. Policy tag specifies the throttling policy against which all messages
will be evaluated. It contains some IP address ranges and the maximum number of
messages to be allowed for those ranges within a time period given in 'UnitTime'
tag. 'ProhibitTimePeriod' tag specifies the time period to prohibit further
requests after the received request count exceeds the specified time. Now run the
client 5 times repetitively using the following command to see how throttling works.

ant stockquote -Dsymbol=IBM -Dmode=quote -Daddurl=http://localhost:8280/

For the first four requests you will get the quote prices for IBM as follows.

[java] Standard :: Stock price = $177.20143371883802

Fifth request will not be sent to the Axis2 server and the client will receive
the following fault.

[java] org.apache.axis2.AxisFault: \*\*Access Denied\*\*

Maximum number of requests within 800000 milliseconds is specified as 4 for any
server (including localhost) other than the explicitly specified ones. Therefore,
our fifth request is denied by the throttle mediator. You can verify this by looking
at the Synapse console.

[HttpServerWorker-1] INFO LogMediator - text = \*\*Access Accept\*\*
[HttpServerWorker-2] INFO LogMediator - text = \*\*Access Accept\*\*
[HttpServerWorker-3] INFO LogMediator - text = \*\*Access Accept\*\*
[HttpServerWorker-4] INFO LogMediator - text = \*\*Access Accept\*\*
[HttpServerWorker-5] INFO LogMediator - text = \*\*Access Denied\*\*

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample372"></a>

# Apache Synapse – Apache Synapse - Sample 372

## <a id="synapse-apache-org-userguide-samples-sample372--Sample_372:_Use_of_Concurrency_Throttling_and_Request_Rate_Based_Throttling"></a>Sample 372: Use of Concurrency Throttling and Request Rate Based Throttling

<definitions xmlns="http://ws.apache.org/ns/synapse">
<registry provider="org.apache.synapse.registry.url.SimpleURLRegistry">
<!-- the root property of the simple URL registry helps resolve a resource URL as root + key -->
<parameter name="root">file:repository/</parameter>
<!-- all resources loaded from the URL registry would be cached for this number of milli seconds -->
<parameter name="cachableDuration">150000</parameter>
</registry>
<sequence name="onAcceptSequence">
<log level="custom">
<property name="text" value="\*\*Access Accept\*\*"/>
</log>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</sequence>
<sequence name="onRejectSequence" trace="enable">
<log level="custom">
<property name="text" value="\*\*Access Denied\*\*"/>
</log>
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason value="\*\*Access Denied\*\*"/>
</makefault>
<send/>
<drop/>
</sequence>
<proxy name="StockQuoteProxy">
<target>
<inSequence>
<throttle onReject="onRejectSequence" onAccept="onAcceptSequence" id="A">
<policy key="conf/sample/resources/policy/throttle\_policy.xml"/>
</throttle>
</inSequence>
<outSequence>
<throttle id="A"/>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample372--Objective"></a>Objective

Showcase how to use the concurrency throttling in conjunction with request rate
throttling

### <a id="synapse-apache-org-userguide-samples-sample372--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 372 (repository/conf/sample/synapse\_sample\_372.xml)

  Unix/Linux: sh synapse.sh -sample 372  
  Windows: synapse.bat -sample 372

### <a id="synapse-apache-org-userguide-samples-sample372--Executing_the_Client"></a>Executing the Client

This is a combination of [sample 370](#synapse-apache-org-userguide-samples-sample370) and
[sample 371](#synapse-apache-org-userguide-samples-sample371). In this case the throttle policy is loaded
from the 'throttle\_policy.xml' file which is fetched from the registry. To verify
the functionality, it requires running a load test. The all enabled request from
the concurrency throttling will be controlled by the access rate base throttling
according to the policy.

Run the client as follows.

ant stockquote -Daddurl=http://localhost:8280/services/StockQuoteProxy

You will get same results as in [sample 371](#synapse-apache-org-userguide-samples-sample371). If you
run the load test, results will be different due to the effect of concurrency
throttling.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample380"></a>

# Apache Synapse – Apache Synapse - Sample 380

## <a id="synapse-apache-org-userguide-samples-sample380--Sample_380:_Writing_Custom_Mediation_Logic_in_Java"></a>Sample 380: Writing Custom Mediation Logic in Java

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="fault">
<makefault>
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason value="Mediation failed."/>
</makefault>
<send/>
</sequence>
<sequence name="main" onError="fault">
<in>
<send>
<endpoint name="stockquote">
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</in>
<out>
<class name="samples.mediators.DiscountQuoteMediator">
<property name="discountFactor" value="10"/>
<property name="bonusFor" value="5"/>
</class>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample380--Objective"></a>Objective

Demonstrate the use of class mediator to extend the mediation functionality of
Synapse

### <a id="synapse-apache-org-userguide-samples-sample380--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 380 (repository/conf/sample/synapse\_sample\_380.xml)

  Unix/Linux: sh synapse.sh -sample 380  
  Windows: synapse.bat -sample 380

### <a id="synapse-apache-org-userguide-samples-sample380--Executing_the_Client"></a>Executing the Client

In this configuration, Synapse hands over the request message to the specified
endpoint, which sends it to the Axis2 server running on port 9000. But the response
message is passed through the class mediator before sending it back to the client.
Class mediator in turns hands over the message to the specified Java class for
further processing. In that regard, the class mediator acts as a delegating
mediator which delegates the message to a custom Java class for processing.
Two parameters named 'discountFactor' and 'bonusFor' are passed to the mediator
implementation class (i.e. samples.mediators.DiscountQuoteMediator) before each
invocation. Source of the mediator implementation class is shown below.

package samples.mediators;
import org.apache.synapse.MessageContext;
import org.apache.synapse.Mediator;
import org.apache.axiom.om.OMElement;
import org.apache.axiom.om.OMAbstractFactory;
import org.apache.axiom.om.OMFactory;
import org.apache.axiom.soap.SOAPFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import javax.xml.namespace.QName;
public class DiscountQuoteMediator implements Mediator {
private static final Log log = LogFactory.getLog(DiscountQuoteMediator.class);
private String discountFactor="10";
private String bonusFor="10";
private int bonusCount=0;
public DiscountQuoteMediator(){}
public boolean mediate(MessageContext mc) {
String price= mc.getEnvelope().getBody().getFirstElement().getFirstElement().
getFirstChildWithName(new QName("http://services.samples","last")).getText();
//converting String properties into integers
int discount=Integer.parseInt(discountFactor);
int bonusNo=Integer.parseInt(bonusFor);
double currentPrice=Double.parseDouble(price);
//discounting factor is deducted from current price form every response
Double lastPrice = new Double(currentPrice - currentPrice \* discount / 100);
//Special discount of 5% offers for the first responses as set in the bonusFor property
if (bonusCount <= bonusNo) {
lastPrice = new Double(lastPrice.doubleValue() - lastPrice.doubleValue() \* 0.05);
bonusCount++;
}
String discountedPrice = lastPrice.toString();
mc.getEnvelope().getBody().getFirstElement().getFirstElement().getFirstChildWithName
(new QName("http://services.samples","last")).setText(discountedPrice);
System.out.println("Quote value discounted.");
System.out.println("Original price: " + price);
System.out.println("Discounted price: " + discountedPrice);
return true;
}
public String getType() {
return null;
}
public void setTraceState(int traceState) {
traceState = 0;
}
public int getTraceState() {
return 0;
}
public void setDiscountFactor(String discount) {
discountFactor=discount;
}
public String getDiscountFactor() {
return discountFactor;
}
public void setBonusFor(String bonus){
bonusFor=bonus;
}
public String getBonusFor(){
return bonusFor;
}
}

All classes developed for the class mediator should implement the 'Mediator'
interface, which contains the mediate(...) method. The mediate(...) method of the
above class is invoked for each response message mediated through the main
sequence, with the message context of the current message as the parameter. All
details of the message including the SOAP headers, SOAP body and properties of
the context hierarchy can be accessed from the message context. In this sample,
the body of the message is retrieved and the discount percentage is subtracted
from the quote price. If the quote request number is less than the number specified
in the 'bonusFor' property in the configuration, a special discount is given.

To test the custom code and the class mediator, invoke the test client as
follows.

ant stockquote -Dsymbol=IBM -Dmode=quote -Daddurl=http://localhost:8280

You will see the below output in the client console with the discounted quote value.

Standard :: Stock price = $95.26454380258552

If you check the Synapse console, you will notice the messages printed by the
custom mediator during mediation.

Quote value discounted.
Original price: 162.30945327447262
Discounted price: 138.77458254967408

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample381"></a>

# Apache Synapse – Apache Synapse - Sample 381

## <a id="synapse-apache-org-userguide-samples-sample381--Sample_381:_Class_Mediator_for_CBR_of_Binary_Messages"></a>Sample 381: Class Mediator for CBR of Binary Messages

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="JMSBinaryProxy" transports="jms">
<target inSequence="BINARY\_CBR\_SEQ"/>
</proxy>
<sequence name="BINARY\_CBR\_SEQ">
<in>
<log level="full"/>
<property action="set" name="OUT\_ONLY" value="true"/>
<class name="samples.mediators.BinaryExtractMediator">
<property name="offset" value="11"/>
<property name="length" value="4"/>
<property name="variableName" value="symbol"/>
<property name="binaryEncoding" value="utf-8"/>
</class>
<log level="custom">
<property name="symbol" expression="get-property('symbol')"/>
</log>
<switch source="get-property('symbol')">
<case regex="GOOG">
<send>
<endpoint>
<address
uri="jms:/dynamicTopics/mdd.GOOG?transport.jms.ConnectionFactoryJNDIName=TopicConnectionFactory&java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory&java.naming.provider.url=tcp://localhost:61616&transport.jms.DestinationType=topic"/>
</endpoint>
</send>
</case>
<case regex="MSFT">
<send>
<endpoint>
<address
uri="jms:/dynamicTopics/mdd.MSFT?transport.jms.ConnectionFactoryJNDIName=TopicConnectionFactory&java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory&java.naming.provider.url=tcp://localhost:61616&transport.jms.DestinationType=topic"/>
</endpoint>
</send>
</case>
<default/>
</switch>
</in>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample381--Objective"></a>Objective

Demonstrate an advanced content based routing (CBR) scenario using a custom mediator

### <a id="synapse-apache-org-userguide-samples-sample381--Pre-requisites"></a>Pre-requisites

- Setup and start a JMS broker (Apache ActiveMQ can be used as the
  JMS broker for this scenario.Refer [JMS setup guide](#synapse-apache-org-userguide-samples-setup-jms--intro) for information on
  how to run ActiveMQ.)
- Enable the JMS transport receiver and sender of Synapse (Refer JMS setup
  guide for more details)
- Start Synapse using the configuration numbered 381 (repository/conf/sample/synapse\_sample\_381.xml)

  Unix/Linux: sh synapse.sh -sample 381  
  Windows: synapse.bat -sample 381

### <a id="synapse-apache-org-userguide-samples-sample381--Executing_the_Client"></a>Executing the Client

In this configuration, a proxy service has been defined to accept incoming JMS
messages. JMS messages contain binary payloads. User configures the offset, length,
and binary encoding of the text literal that Synapse should use for CBR. Configuration
simply routes the messages based on this text to different endpoints.

A JMS producer and two instances of a consumer used to demonstrate the CBR functionality.

Now run the first consumer using the following command.

ant mddconsumer -Djms\_topic=mdd.MSFT

Now run the second consumer using the following command.

ant mddconsumer -Djms\_topic=mdd.GOOG

Now run the market data producer to genenrate market data for symbol 'MSFT' using
the following command.

ant mddproducer -Dsymbol=MSFT

Now run the market data producer to genenrate market data for symbol 'GOOG' using
the following command.

ant mddproducer -Dsymbol=GOOG

You will see the below output in the client console(s) based on the symbol.

mddconsumer:
[java] Market data recived for symbol : topic://mdd.MSFT
[java] Market data recived for symbol : topic://mdd.MSFT

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample390"></a>

# Apache Synapse – Apache Synapse - Sample 390

## <a id="synapse-apache-org-userguide-samples-sample390--Sample_390:_Introduction_to_the_XQuery_Mediator"></a>Sample 390: Introduction to the XQuery Mediator

<definitions xmlns="http://ws.apache.org/ns/synapse">
<!-- the SimpleURLRegistry allows access to a URL based registry (e.g. file:/// or http://) -->
<registry provider="org.apache.synapse.registry.url.SimpleURLRegistry">
<!-- the root property of the simple URL registry helps resolve a resource URL as root + key -->
<parameter name="root">file:repository/conf/sample/resources/</parameter>
<!-- all resources loaded from the URL registry would be cached for this number of milli seconds -->
<parameter name="cachableDuration">15000</parameter>
</registry>
<localEntry key="xquery-key-req"
src="file:repository/conf/sample/resources/xquery/xquery\_req.xq"/>
<proxy name="StockQuoteProxy">
<target>
<inSequence>
<property name="body" expression="$body/child::\*[position()=1]"/>
<xquery key="xquery-key-req">
<variable name="payload" type="ELEMENT"/>
</xquery>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</inSequence>
<outSequence>
<out>
<xquery key="xquery/xquery\_res.xq">
<variable name="payload" type="ELEMENT"/>
<variable xmlns:m0="http://services.samples"
name="code" type="STRING"
expression="self::node()//m0:return/m0:symbol/child::text()"/>
<variable xmlns:m0="http://services.samples"
name="price" type="DOUBLE"
expression="self::node()//m0:return/m0:last/child::text()"/>
</xquery>
<send/>
</out>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample390--Objective"></a>Objective

Demonstrate how to use the XQuery mediator for message content transformations

### <a id="synapse-apache-org-userguide-samples-sample390--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 390 (repository/conf/sample/synapse\_sample\_390.xml)

  Unix/Linux: sh synapse.sh -sample 390  
  Windows: synapse.bat -sample 390

### <a id="synapse-apache-org-userguide-samples-sample390--Executing_the_Client"></a>Executing the Client

This example uses the XQuery mediator to perform transformations. This sample
behaves the same as [sample 8](#synapse-apache-org-userguide-samples-sample8) and the only difference
is that this sample uses XQuery instead of XSLT for transformation.

Send a custom quote request to Synapse as follows.

ant stockquote -Daddurl=http://localhost:8280/services/StockQuoteProxy -Dmode=customquote

Request is transformed into a standard stock quote request by the XPery mediator.
The XQuery definition is loaded through a local entry. The response from Axis2
is transformed back to a custom quote response. In this case the XQuery definition
is loaded from the registry.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample391"></a>

# Apache Synapse – Apache Synapse - Sample 391

## <a id="synapse-apache-org-userguide-samples-sample391--Sample_391:_Using_External_XML_Documents_in_the_XQuery_Mediator"></a>Sample 391: Using External XML Documents in the XQuery Mediator

<definitions xmlns="http://ws.apache.org/ns/synapse">
<!-- the SimpleURLRegistry allows access to a URL based registry (e.g. file:/// or http://) -->
<registry provider="org.apache.synapse.registry.url.SimpleURLRegistry">
<!-- the root property of the simple URL registry helps resolve a resource URL as root + key -->
<parameter name="root">file:repository/conf/sample/resources/</parameter>
<!-- all resources loaded from the URL registry would be cached for this number of milli seconds -->
<parameter name="cachableDuration">15000</parameter>
</registry>
<proxy name="StockQuoteProxy">
<target>
<inSequence>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</inSequence>
<outSequence>
<out>
<xquery key="xquery/xquery\_commisson.xq">
<variable name="payload" type="ELEMENT"/>
<variable name="commission" type="ELEMENT" key="misc/commission.xml"/>
</xquery>
<send/>
</out>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample391--Objective"></a>Objective

Demonstrate how to import external XML documents into the XQuery engine using
the XQuery mediator

### <a id="synapse-apache-org-userguide-samples-sample391--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 391 (repository/conf/sample/synapse\_sample\_391.xml)

  Unix/Linux: sh synapse.sh -sample 391  
  Windows: synapse.bat -sample 391

### <a id="synapse-apache-org-userguide-samples-sample391--Executing_the_Client"></a>Executing the Client

In this sample, data from commission.xml file is used inside XQuery. The stock
quote price from the response and commission from the commission.xml document
will be added and given as a new price value.

Try out this sample by invoking the proxy service as follows.

ant stockquote -Daddurl=http://localhost:8280/services/StockQuoteProxy

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample4"></a>

# Apache Synapse – Apache Synapse - Sample 4

## <a id="synapse-apache-org-userguide-samples-sample4--Sample_4:_Introduction_to_Error_Handling"></a>Sample 4: Introduction to Error Handling

<definitions xmlns="http://ws.apache.org/ns/synapse">
<!-- the default fault handling sequence used by Synapse - named 'fault' -->
<sequence name="fault">
<log level="custom">
<property name="text" value="An unexpected error occured"/>
<property name="message" expression="get-property('ERROR\_MESSAGE')"/>
</log>
<drop/>
</sequence>
<sequence name="sunErrorHandler">
<log level="custom">
<property name="text" value="An unexpected error occured for stock SUN"/>
<property name="message" expression="get-property('ERROR\_MESSAGE')"/>
<!--<property name="detail" expression="get-property('ERROR\_DETAIL')"/>-->
</log>
<drop/>
</sequence>
<sequence name="main">
<in>
<switch xmlns:m0="http://services.samples" source="//m0:getQuote/m0:request/m0:symbol">
<case regex="IBM">
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</case>
<case regex="MSFT">
<send>
<endpoint key="bogus"/>
</send>
</case>
<case regex="SUN">
<sequence key="sunSequence"/>
</case>
</switch>
<drop/>
</in>
<out>
<send/>
</out>
</sequence>
<sequence name="sunSequence" onError="sunErrorHandler">
<send>
<endpoint key="sunPort"/>
</send>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample4--Objective"></a>Objective

Introduction to error handling with the 'fault' sequence

### <a id="synapse-apache-org-userguide-samples-sample4--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 4 (repository/conf/sample/synapse\_sample\_4.xml)

  Unix/Linux: sh synapse.sh -sample 4  
  Windows: synapse.bat -sample 4

### <a id="synapse-apache-org-userguide-samples-sample4--Executing_the_Client"></a>Executing the Client

First send a stock quote request from the sample client for the symbol 'IBM' as
follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=IBM

The request will be routed to the Axis2 server and client will receive a response
as expected.

Standard :: Stock price = $95.26454380258552

Now send another stock quote request for the symbol 'MSFT' as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=MSFT

For MSFT requests Synapse is instructed to route the messages to an endpoint named
'bogus', which does not exist. Synapse executes the specified error handler
sequence closest to the point where the error was encountered. In this case, the
currently executing sequence is 'main' and it does not specify an 'onError'
attribute. Whenever Synapse cannot find an error handler, it looks for a sequence
named 'fault'. Thus the 'fault' sequence can be seen executing, and writing the
generic error message to the logs.

[HttpServerWorker-1] DEBUG SendMediator - Send mediator :: mediate()
[HttpServerWorker-1] ERROR IndirectEndpoint - Reference to non-existent endpoint for key : bogus
[HttpServerWorker-1] DEBUG MediatorFaultHandler - MediatorFaultHandler :: handleFault
[HttpServerWorker-1] DEBUG SequenceMediator - Sequence mediator <fault> :: mediate()
[HttpServerWorker-1] DEBUG LogMediator - Log mediator :: mediate()
[HttpServerWorker-1] INFO LogMediator text = An unexpected error occured, message = Couldn't find the endpoint with the key : bogus

Now send another stock quote request for the symbol 'SUN'.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=SUN

When the 'SUN' quote is requested, a custom sequence 'sunSequence' is invoked,
and it specifies 'sunErrorHandler' as its error handler. Hence when the send
fails, you could see the proper error handler invocation and the custom error
message printed as follows.

[HttpServerWorker-1] DEBUG SequenceMediator - Sequence mediator <sunSequence> :: mediate()
[HttpServerWorker-1] DEBUG SequenceMediator - Setting the onError handler for the sequence
[HttpServerWorker-1] DEBUG AbstractListMediator - Implicit Sequence <SequenceMediator> :: mediate()
[HttpServerWorker-1] DEBUG SendMediator - Send mediator :: mediate()
[HttpServerWorker-1] ERROR IndirectEndpoint - Reference to non-existent endpoint for key : sunPort
[HttpServerWorker-1] DEBUG MediatorFaultHandler - MediatorFaultHandler :: handleFault
[HttpServerWorker-1] DEBUG SequenceMediator - Sequence mediator <sunErrorHandler> :: mediate()
[HttpServerWorker-1] DEBUG AbstractListMediator - Implicit Sequence <SequenceMediator> :: mediate()
[HttpServerWorker-1] DEBUG LogMediator - Log mediator :: mediate()
[HttpServerWorker-1] INFO LogMediator text = An unexpected error occured for stock SUN, message = Couldn't find the endpoint with the key : sunPort

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample400"></a>

# Apache Synapse – Apache Synapse - Sample 400

## <a id="synapse-apache-org-userguide-samples-sample400--Sample_400:_Message_Splitting_and_Aggregation"></a>Sample 400: Message Splitting and Aggregation

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="SplitAggregateProxy">
<target>
<inSequence>
<iterate xmlns:m0="http://services.samples" expression="//m0:getQuote/m0:request"
preservePayload="true" attachPath="//m0:getQuote">
<target>
<sequence>
<send>
<endpoint>
<address
uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</sequence>
</target>
</iterate>
</inSequence>
<outSequence>
<aggregate>
<onComplete xmlns:m0="http://services.samples"
expression="//m0:getQuoteResponse">
<send/>
</onComplete>
</aggregate>
</outSequence>
</target>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample400--Objective"></a>Objective

Showcase how Synapse can be used to split a message into multiple fragments
using the iterate mediator, and process them separately. The sample also shows
how to use the aggregate mediator to combine multiple messages into one.

### <a id="synapse-apache-org-userguide-samples-sample400--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 400 (repository/conf/sample/synapse\_sample\_400.xml)

  Unix/Linux: sh synapse.sh -sample 400  
  Windows: synapse.bat -sample 400

### <a id="synapse-apache-org-userguide-samples-sample400--Executing_the_Client"></a>Executing the Client

In this sample, the message sent to Synapse is comprised of a number of elements
of the same type. When Synapse receives this message it will iterate through those
elements and then will send each of them to the specified endpoint as separate
messages. When all the responses are received by Synapse, those messages will be
aggregated to form the resultant response and will send back to the client.

To try this out invoke the sample client as follows.

ant stockquote -Daddurl=http://localhost:8280/services/SplitAggregateProxy -Ditr=4

The above command will send a request containing four fragments in it. The
iterate mediator therefore will break up the message into four. You will notice
that Axis2 server is receiving 4 requests from Synapse. Four responses from Axis2
will be combined into one by the aggregate mediator and sent back to the sample
Axis2 client.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample410"></a>

# Apache Synapse – Apache Synapse - Sample 410

## <a id="synapse-apache-org-userguide-samples-sample410--Sample_410:_Distributed_Transactions_Management_with_the_Transaction_Mediator"></a>Sample 410: Distributed Transactions Management with the Transaction Mediator

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="myFaultHandler">
<log level="custom">
<property name="text" value="\*\* Rollback Transaction\*\*"/>
</log>
<transaction action="rollback"/>
<send/>
</sequence>
<sequence name="main" onError="myFaultHandler">
<in>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</in>
<out>
<transaction action="new"/>
<log level="custom">
<property name="text" value="\*\* Reporting to the Database esbdb\*\*"/>
</log>
<dbreport useTransaction="true" xmlns="http://ws.apache.org/ns/synapse">
<connection>
<pool>
<dsName>java:jdbc/XADerbyDS</dsName>
<icClass>org.jnp.interfaces.NamingContextFactory</icClass>
<url>localhost:1099</url>
<user>synapse</user>
<password>synapse</password>
</pool>
</connection>
<statement>
<sql>delete from company where name =?</sql>
<parameter expression="//m0:return/m0:symbol/child::text()"
xmlns:m0="http://services.samples"
type="VARCHAR"/>
</statement>
</dbreport>
<log level="custom">
<property name="text" value="\*\* Reporting to the Database esbdb1\*\*"/>
</log>
<dbreport useTransaction="true" xmlns="http://ws.apache.org/ns/synapse">
<connection>
<pool>
<dsName>java:jdbc/XADerbyDS1</dsName>
<icClass>org.jnp.interfaces.NamingContextFactory</icClass>
<url>localhost:1099</url>
<user>synapse</user>
<password>synapse</password>
</pool>
</connection>
<statement>
<sql> INSERT into company values ('IBM','c4',12.0)</sql>
</statement>
</dbreport>
<transaction action="commit"/>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample410--Objective"></a>Objective

Demonstrate how to manage complex distributed transactions using the transaction
mediator

### <a id="synapse-apache-org-userguide-samples-sample410--Pre-requisites"></a>Pre-requisites

- To run this sample it is required to deploy Synpase on JBoss application
  server(This is only tested with JBoss application sever). You can use the
  Synapse war distribution to deploy Synapse on JBoss. Use the synpase\_sample\_410.xml
  as the synapse confiuration file and start JBoss. Also you need to define
  two XA datasources for above the two datasources defined in Synapse. You'll
  need to refer JBoss documentation to see how to do this.
- Setup two Derby database instances as described in the database setup guide.
  These databases will be used by the XA datasources in JBoss.
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2

### <a id="synapse-apache-org-userguide-samples-sample410--Executing_the_Client"></a>Executing the Client

In this sample a record is deleted from one database and it is added to the
second database. If either of the operations(deleting from the 1st database and
adding to the second database) fails the entire operation will be roll backed.
The records will be left intact.

Invoke the client as follows to try this out.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=SUN

You can force an error by shutting down one of the two database instances.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample420"></a>

# Apache Synapse – Apache Synapse - Sample 420

## <a id="synapse-apache-org-userguide-samples-sample420--Sample_420:_Simple_Response_Caching_Scenario"></a>Sample 420: Simple Response Caching Scenario

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<cache timeout="20" scope="per-host" collector="false"
hashGenerator="org.wso2.caching.digest.DOMHASHGenerator">
<implementation type="memory" maxSize="100"/>
</cache>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</in>
<out>
<cache collector="true"/>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample420--Objective"></a>Objective

Showcase the caching capabilities of Synapse by implementing a simple response
cache in Synapse for an actual service deployed on Axis2

### <a id="synapse-apache-org-userguide-samples-sample420--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 420 (repository/conf/sample/synapse\_sample\_420.xml)

  Unix/Linux: sh synapse.sh -sample 420  
  Windows: synapse.bat -sample 420

### <a id="synapse-apache-org-userguide-samples-sample420--Executing_the_Client"></a>Executing the Client

In this sample, the message sent to Synapse is checked for an existing cached
response by calculating the hash value of the request. If there is a cache hit
in Synapse, then this request will not be forwarded to the actual service. Rather,
Synapse responds to the client with the cached response. In case of a cache miss
that particular message will be forwarded to the actual service and caches that
response in the out path for the use of consecutive requests of the same type.

To try out this scenario, send a request from the sample client as follows.

ant stockquote -Dtrpurl=http://localhost:8280/

You will notice that if you send more than one requests within 20 seconds, only
the first request is forwarded to the actual service, and the rest of the requests
will be served by the cache inside Synapse. You could observe this by looking at
the logs printed by the Axis2 server, as well as by observing a constant quote value in
the response to the client instead of the random rate, which changes by each and
every 20 seconds.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample430"></a>

# Apache Synapse – Apache Synapse - Sample 430

## <a id="synapse-apache-org-userguide-samples-sample430--Sample_430:_Callout_Mediator_for_Synchronous_Web_Service_Invocations"></a>Sample 430: Callout Mediator for Synchronous Web Service Invocations

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<callout serviceURL="http://localhost:9000/services/SimpleStockQuoteService"
action="urn:getQuote">
<source xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
xpath="s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]"/>
<target xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
xpath="s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]"/>
</callout>
<respond/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample430--Objective"></a>Objective

Demonstrate the usage of the callout mediator for making synchronous (blocking)
Web service calls during mediation

### <a id="synapse-apache-org-userguide-samples-sample430--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 430 (repository/conf/sample/synapse\_sample\_430.xml)

  Unix/Linux: sh synapse.sh -sample 430  
  Windows: synapse.bat -sample 430

### <a id="synapse-apache-org-userguide-samples-sample430--Executing_the_Client"></a>Executing the Client

In this sample, the callout mediator does the direct service invocation to the
StockQuoteService using the client request, gets the response and sets it as the
first child of the SOAP message body. Then using the send mediator, the message
is sent back to the client. As a result there is no need to define any endpoints
in this configuration.

Invoke the client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample431"></a>

# Apache Synapse – Apache Synapse - Sample 431

## <a id="synapse-apache-org-userguide-samples-sample431--Sample_431:_Callout_Mediator_with_WS-Security_for_Outgoing_Messages"></a>Sample 431: Callout Mediator with WS-Security for Outgoing Messages

<definitions xmlns="http://ws.apache.org/ns/synapse">
<localEntry key="sec\_policy" src="file:repository/conf/sample/resources/policy/policy\_3.xml"/>
<sequence name="main">
<callout serviceURL="http://localhost:9000/services/SecureStockQuoteService"
action="urn:getQuote">
<source xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
xpath="s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]"/>
<target xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
xpath="s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]"/>
<enableSec policy="sec\_policy"/>
</callout>
<respond/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample431--Objective"></a>Objective

Demonstrate the usage of the Callout mediator for making synchronous (blocking)
Web service calls to invoke secured services during mediation.

### <a id="synapse-apache-org-userguide-samples-sample431--Pre-requisites"></a>Pre-requisites

- Download and install the Java Cryptography Extension (JCE) unlimited
  strength policy files for your JDK
- Deploy the SecureStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 431 (repository/conf/sample/synapse\_sample\_431.xml)

  Unix/Linux: sh synapse.sh -sample 431  
  Windows: synapse.bat -sample 431

### <a id="synapse-apache-org-userguide-samples-sample431--Executing_the_Client"></a>Executing the Client

In this sample, the Callout mediator does the service invocation to the Secured service
SecureStockQuoteService by encrypting the client request according to the defined policy.
And then received encrypted message is decrypted and after removing the wsse:Security header,
the message is sent back to the client using the send mediator.
Callout Mediator is configured to enable WS-Security as per the policy specified by 'policy\_3.xml'.

Invoke the client as follows.

ant stockquote -Dtrpurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample432"></a>

# Apache Synapse – Apache Synapse - Sample 432

## <a id="synapse-apache-org-userguide-samples-sample432--Sample_432:_Callout_Mediator_-_Invoke_a_secured_service_which_has_different_policies_for_inbound_and_outbound_flows"></a>Sample 432: Callout Mediator - Invoke a secured service which has different policies for inbound and outbound flows

<definitions xmlns="http://ws.apache.org/ns/synapse">
<localEntry key="sec\_policy\_inbound" src="file:repository/conf/sample/resources/policy/policy\_3.xml"/>
<localEntry key="sec\_policy\_outbound" src="file:repository/conf/sample/resources/policy/policy\_3.xml"/>
<sequence name="main">
<callout serviceURL="http://localhost:9000/services/SecureStockQuoteService"
action="urn:getQuote">
<source xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
xpath="s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]"/>
<target xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
xpath="s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]"/>
<enableSec outboundPolicy="sec\_policy\_outbound" inboundPolicy="sec\_policy\_inbound"/>
</callout>
<respond/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample432--Objective"></a>Objective

Demonstrate the usage of the Callout mediator for making synchronous (blocking)
Web service calls to invoke secured services, which has different security policies
for inbound and outbound flows.

### <a id="synapse-apache-org-userguide-samples-sample432--Pre-requisites"></a>Pre-requisites

- Download and install the Java Cryptography Extension (JCE) unlimited
  strength policy files for your JDK
- Deploy the SecureStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 432 (repository/conf/sample/synapse\_sample\_432.xml)

  Unix/Linux: sh synapse.sh -sample 432  
  Windows: synapse.bat -sample 432

### <a id="synapse-apache-org-userguide-samples-sample432--Executing_the_Client"></a>Executing the Client

In this sample, the Callout mediator is configured with different security policies
for inbound and outbound message flows. Messages sent out from synapse is encrypted
using the outboundPolicy and response received from the secured service is decrypted
using the inboundPolicy.

Invoke the client as follows.

ant stockquote -Dtrpurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample433"></a>

# Apache Synapse – Apache Synapse - Sample 433

## <a id="synapse-apache-org-userguide-samples-sample433--Sample_433:_Callout_Mediator_-_Invoke_a_service_using_a_defined_Endpoint"></a>Sample 433: Callout Mediator - Invoke a service using a defined Endpoint

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<callout>
<endpoint key="StockQuoteServiceEndpoint"/>
<source xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
xpath="s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]"/>
<target xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
xpath="s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]"/>
</callout>
<respond/>
</sequence>
<endpoint name="StockQuoteServiceEndpoint">
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample433--Objective"></a>Objective

Demonstrate how to invoke a service from Callout Mediator using a defined endpoint.

### <a id="synapse-apache-org-userguide-samples-sample433--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 433 (repository/conf/sample/synapse\_sample\_433.xml)

  Unix/Linux: sh synapse.sh -sample 433  
  Windows: synapse.bat -sample 433

### <a id="synapse-apache-org-userguide-samples-sample433--Executing_the_Client"></a>Executing the Client

In this sample, the Callout mediator does the direct service invocation to the
StockQuoteService using the client request, gets the response and sets it as the
first child of the SOAP message body. Callout Mediator uses the defined endpoint
named 'StockQuoteServiceEndpoint' to send the message to the StockQuoteService.

Invoke the client as follows.

ant stockquote -Daddurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample434"></a>

# Apache Synapse – Apache Synapse - Sample 434

## <a id="synapse-apache-org-userguide-samples-sample434--Sample_434:_Callout_Mediator_-_Invoke_a_service_using_an_inline_Endpoint"></a>Sample 434: Callout Mediator - Invoke a service using an inline Endpoint

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<callout>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<source xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
xpath="s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]"/>
<target xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/"
xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
xpath="s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]"/>
</callout>
<respond/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample434--Objective"></a>Objective

Demonstrate how to invoke a service from Callout mediator using an inline endpoint.

### <a id="synapse-apache-org-userguide-samples-sample434--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 434 (repository/conf/sample/synapse\_sample\_434.xml)

  Unix/Linux: sh synapse.sh -sample 434  
  Windows: synapse.bat -sample 434

### <a id="synapse-apache-org-userguide-samples-sample434--Executing_the_Client"></a>Executing the Client

In this sample, the Callout mediator does the direct service invocation to the
StockQuoteService using the client request, gets the response, and sets it as the
first child of the SOAP message body. Callout Mediator uses the inline endpoint
to send the message to the StockQuoteService.

Invoke the client as follows.

ant stockquote -Daddurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample440"></a>

# Apache Synapse – Apache Synapse - Sample 440

## <a id="synapse-apache-org-userguide-samples-sample440--Sample_440:_Respond_Mediator_-_Echo_Service_with_a_Proxy_Service"></a>Sample 440: Respond Mediator - Echo Service with a Proxy Service

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="EchoService">
<target>
<inSequence>
<respond/>
</inSequence>
</target>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample440--Objective"></a>Objective

Demonstrate how to use respond mediator to create a simple echo service

### <a id="synapse-apache-org-userguide-samples-sample440--Pre-requisites"></a>Pre-requisites

- Start Synapse using the configuration numbered 440 (repository/conf/sample/synapse\_sample\_440.xml)

  Unix/Linux: sh synapse.sh -sample 440  
  Windows: synapse.bat -sample 440

### <a id="synapse-apache-org-userguide-samples-sample440--Executing_the_Client"></a>Executing the Client

Invoke the EchoService proxy service with a payload.
Following is how we can use curl as the client.

curl -v -X POST -H "Content-type: application/xml" -d '<test>foo</test>' 'http://localhost:8280/services/EchoService'

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample441"></a>

# Apache Synapse – Apache Synapse - Sample 441

## <a id="synapse-apache-org-userguide-samples-sample441--Sample_441:_Respond_Mediator_-_Mock_a_service_with_a_proxy_service"></a>Sample 441: Respond Mediator - Mock a service with a proxy service

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="MockService">
<target>
<inSequence>
<log level="full"/>
<payloadFactory>
<format>
<m:Sample xmlns:m="http://services.samples">
<m:Response>
<m:value>foo</m:value>
</m:Response>
</m:Sample>
</format>
<args/>
</payloadFactory>
<respond/>
</inSequence>
</target>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample441--Objective"></a>Objective

Demonstrate how to create a simple mock service with a proxy service using the respond mediator.

### <a id="synapse-apache-org-userguide-samples-sample441--Pre-requisites"></a>Pre-requisites

- Start Synapse using the configuration number 441
  (repository/conf/sample/synapse\_sample\_441.ml)

  Unix/Linux: sh synapse.sh -sample 441
    
  Windows: synapse.bat -sample 441

### <a id="synapse-apache-org-userguide-samples-sample441--Executing_the_Client"></a>Executing the Client

Invoke the MockService proxy service with a payload.
Following is how we can use curl as the client

curl -v -X POST -H "Content-type: application/xml" -d '<request>foo</request>'
'http://localhost:8280/services/MockService'

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample450"></a>

# Apache Synapse – Apache Synapse - Sample 450

## <a id="synapse-apache-org-userguide-samples-sample450--Sample_450:_Introduction_to_the_URL_Rewrite_Mediator"></a>Sample 450: Introduction to the URL Rewrite Mediator

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<rewrite>
<rule>
<action type="replace" regex="soap" value="services" fragment="path"/>
</rule>
</rewrite>
<send/>
</in>
<out>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample450--Objective"></a>Objective

Demonstrate the basic functions of the URL rewrite mediator

### <a id="synapse-apache-org-userguide-samples-sample450--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 450 (repository/conf/sample/synapse\_sample\_450.xml)

  Unix/Linux: sh synapse.sh -sample 450  
  Windows: synapse.bat -sample 450

### <a id="synapse-apache-org-userguide-samples-sample450--Executing_the_Client"></a>Executing the Client

URL rewrite mediator can be used to modify the 'To' header of a request based on
one or more user defined URL rewrite rules. A rewrite rule could be a
plain rewrite instruction or a conditional instruction. In this sample we use a
plain, unconidtional rewrite rule which simply replaces the string 'soap' with
'services' in the 'To' header.

Invoke the sample client as follows to try this out.

ant stockquote -Dtrpurl=http://localhost:8280 -Daddurl=http://localhost:9000/soap/SimpleStockQuoteService

Note that the address URL of the client request contains the context 'soap'. But
in the Axis2 server all the services are deployed under a context named 'services'
by default. Synapse will rewrite the To header of the request by replacing the
'soap' context with 'services. Hence the request will be delivered to the Axis2
server and the Axis2 client will receive a valid response.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample451"></a>

# Apache Synapse – Apache Synapse - Sample 451

## <a id="synapse-apache-org-userguide-samples-sample451--Sample_451:_Conditional_URL_Rewriting"></a>Sample 451: Conditional URL Rewriting

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<rewrite>
<rule>
<condition>
<and>
<equal type="url" source="host" value="localhost"/>
<not>
<equal type="url" source="protocol" value="https"/>
</not>
</and>
</condition>
<action fragment="protocol" value="https"/>
<action fragment="port" value="9002"/>
</rule>
</rewrite>
<send/>
</in>
<out>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample451--Objective"></a>Objective

Demonstrate the ability of the URL rewrite mediator to evaluate conditions on
messages and perform rewrites based on the results

### <a id="synapse-apache-org-userguide-samples-sample451--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 451 (repository/conf/sample/synapse\_sample\_451.xml)

  Unix/Linux: sh synapse.sh -sample 451  
  Windows: synapse.bat -sample 451

### <a id="synapse-apache-org-userguide-samples-sample451--Executing_the_Client"></a>Executing the Client

Invoke the Axis2 client and send some requests to Synapse with different address
URL values. If the address URL value contains 'localhost' as the hostname and 'https'
as the protocol prefix, Synapse will route the message as it is. But if the
hostname is 'localhost' and the protocol is not https, Synapse will rewrite the
URL by setting 'https' as the protocol. The port number will also be set to the
HTTPS port of the Axis2 server.

If you invoke the client as follows, Synapse will rewrite the 'To' header and
forward the message to Axis2 over HTTPS.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

The condition evaluation feature is provided by the Synapse evaluator framework.
Currently one can evaluate expressions on URL values, query parameters, transport
headers, properties and SOAP envelope content using this framework. Hence URL
rewriting can be done based on any of these aspects.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample452"></a>

# Apache Synapse – Apache Synapse - Sample 452

## <a id="synapse-apache-org-userguide-samples-sample452--Sample_452:_Conditional_URL_Rewriting_with_Multiple_Rules"></a>Sample 452: Conditional URL Rewriting with Multiple Rules

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<property name="http.port" value="9000"/>
<property name="https.port" value="9002"/>
<rewrite>
<rule>
<action fragment="host" value="localhost"/>
<action fragment="path" type="prepend" value="/services"/>
</rule>
<rule>
<condition>
<equal type="url" source="protocol" value="http"/>
</condition>
<action fragment="port" xpath="get-property('http.port')"/>
</rule>
<rule>
<condition>
<equal type="url" source="protocol" value="https"/>
</condition>
<action fragment="port" xpath="get-property('https.port')"/>
</rule>
</rewrite>
<log level="full"/>
<send/>
</in>
<out>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample452--Objective"></a>Objective

Demonstrate the ability of the URL rewrite mediator to perform rewrites based
on multiple rules

### <a id="synapse-apache-org-userguide-samples-sample452--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 452 (repository/conf/sample/synapse\_sample\_452.xml)

  Unix/Linux: sh synapse.sh -sample 452  
  Windows: synapse.bat -sample 452

### <a id="synapse-apache-org-userguide-samples-sample452--Executing_the_Client"></a>Executing the Client

One may specify multiple rewrite rules for a URL rewrite mediator instance. In
that case Synapse will execute all the rules on each message, in the order they
appear. This particular sample lists 3 rewrite rules. To try it out, invoke the
client as follows.

ant stockquote -Dtrpurl=http://localhost:8280 -Daddurl=http://test.com/SimpleStockQuoteService

The provided address URL does not contain a port number and the context. The URL
rewrite mediator will replace the hostname (test.com) with 'localhost' and add the
context '/services' to the path. Then it will add the appropriate port number to
the URL by looking at the protocol prefix. Ultimately the service request will be
routed the sample Axis2 server and the client will receive a valid response.

Another important aspect shown by this sample is the ability of the URL rewirte
mediator to obtain the necessary values by executing XPath expressions. The port
numbers are calculated by executing an XPath on the messages.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample460"></a>

# Apache Synapse – Apache Synapse - Sample 460

## <a id="synapse-apache-org-userguide-samples-sample460--Sample_460:_Introduction_to_the_Spring_Mediator"></a>Sample 460: Introduction to the Spring Mediator

<definitions xmlns="http://ws.apache.org/ns/synapse">
<registry provider="org.apache.synapse.registry.url.SimpleURLRegistry">
<parameter name="root">file:repository/conf/sample/resources/</parameter>
<parameter name="cachableDuration">15000</parameter>
</registry>
<sequence name="main">
<!--Setting the Spring Mediator and its Spring Beans xml file location -->
<!--Note that springtest is the bean id used in springCustomLogger.xml -->
<spring bean="springtest" key="spring/springCustomLogger.xml"/>
<send/>
</sequence>
</definitions>

This sample configuration loads an external SpringBean from a file named
springCustomLogger.xml. Contents of this file are as follows.

<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE beans PUBLIC "-//SPRING//DTD BEAN//EN"
"http://www.springframework.org/dtd/spring-beans.dtd">
<beans>
<bean id="springtest" class="samples.mediators.extentions.SpringCustomLogger" singleton="false">
<property name="userName"><value>"Synapse User"</value></property>
<property name="email"><value>"usr@synapse.org"</value></property>
</bean>
</beans>

### <a id="synapse-apache-org-userguide-samples-sample460--Objective"></a>Objective

Demonstrate how to initialize and use a SpringBean as a mediator

### <a id="synapse-apache-org-userguide-samples-sample460--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 460 (repository/conf/sample/synapse\_sample\_460.xml)

  Unix/Linux: sh synapse.sh -sample 460  
  Windows: synapse.bat -sample 460

### <a id="synapse-apache-org-userguide-samples-sample460--Executing_the_Client"></a>Executing the Client

In this sample, the Spring Bean named 'SpringCustomLogger' gets loaded from the
springCustomLogger.xml file and then it is used to log the message ID of each
message being mediated. To see it in action, invoke the sample client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

If you have enabled logging for the samples.mediators package in the log4j.properties
file, you will see an output similar to the following, on the console.

2010-09-26 20:46:57,946 [-] [HttpServerWorker-1] INFO SpringCustomLogger Starting Spring Meditor
2010-09-26 20:46:57,946 [-] [HttpServerWorker-1] INFO SpringCustomLogger Bean in Initialized with User:["Synapse User"]
2010-09-26 20:46:57,946 [-] [HttpServerWorker-1] INFO SpringCustomLogger E-MAIL:["usr@synapse.org"]
2010-09-26 20:46:57,946 [-] [HttpServerWorker-1] INFO SpringCustomLogger Massage Id: urn:uuid:383FA8B27D7CC549D91285514217720
2010-09-26 20:46:57,946 [-] [HttpServerWorker-1] INFO SpringCustomLogger Logged....

Similarly you can import any SpringBean into the Synapse runtime using the
spring mediator, and use Spring to execute mediation rules.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample470"></a>

# Apache Synapse – Apache Synapse - Sample 470

## <a id="synapse-apache-org-userguide-samples-sample470--Sample_470:_Introduction_to_the_EJB_Mediator_I_-_Invoking_Stateless_Session_Beans"></a>Sample 470: Introduction to the EJB Mediator I - Invoking Stateless Session Beans

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StoreLocatorProxy" transports="https http" startOnLoad="true" trace="disable">
<target>
<!-- First call StoreLocator#getClosestStore(), then call StoreRegistry#getStoreById() with the result. -->
<inSequence>
<bean action="CREATE" class="samples.bean.Location" var="loc"/>
<bean action="SET\_PROPERTY" var="loc" property="latitude" value="{//m:latitude}" xmlns:m="http://services.samples"/>
<bean action="SET\_PROPERTY" var="loc" property="longitude" value="{//m:longitude}" xmlns:m="http://services.samples"/>
<ejb class="samples.ejb.StoreLocator" beanstalk="demo" method="getClosestStore" target="store\_id" jndiName="StoreLocatorBean/remote">
<args>
<arg value="{get-property('loc')}"/>
</args>
</ejb>
<ejb class="samples.ejb.StoreRegistry" beanstalk="demo" method="getStoreById" target="store" jndiName="StoreRegistryBean/remote">
<args>
<arg value="{get-property('store\_id')}"/>
</args>
</ejb>
<!-- Prepare the response. -->
<enrich>
<source type="inline" clone="true">
<getClosestStoreResponse xmlns="">
<store>
<name>?</name>
<address>?</address>
<phone>?</phone>
</store>
</getClosestStoreResponse>
</source>
<target type="body"/>
</enrich>
<bean action="GET\_PROPERTY" var="store" property="name" target="{//store/name/text()}"/>
<bean action="GET\_PROPERTY" var="store" property="address" target="{//store/address/text()}"/>
<bean action="GET\_PROPERTY" var="store" property="phoneNo" target="{//store/phone/text()}"/>
<!-- Send the response back to the client of the ESB. -->
<respond/>
</inSequence>
</target>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample470--Objective"></a>Objective

Demonstrate the usage of the EJB mediator for invoking EJB Stateless
Session Beans hosted on a remote EJB Container.

### <a id="synapse-apache-org-userguide-samples-sample470--Pre-requisites"></a>Pre-requisites

- Build the backend EJB jar to be hosted on the EJB Container by changing
  the directory to SYNAPSE\_HOME/samples/axis2Server/src/EJBSampleBeans
  and invoking:

  mvn clean install
- Deploy the built EJB jar
  (SYNAPSE\_HOME/samples/axis2Server/src/EJBSampleBeans/target/synapse-samples-ejb-1.0.0.jar)
  in an EJB Container such as JBoss or GlassFish.
- Add minimal client JARs of your EJB Container to SYNAPSE\_HOME/lib.
  E.g. If you are using JBoss 7, it is sufficient to add the
  *jboss-client.jar* file.
- Add the *synapse-samples-ejb-1.0.0.jar* to SYNAPSE\_HOME/lib. (Note: adding
  only the remote interfaces of the EJBs will suffice. Here we are using
  the complete jar file for simplicity.)
- Configure a beanstalk named *demo* in
  SYNAPSE\_HOME/repository/conf/synapse.properties. You will need to
  specify the JNDI properties of your EJB Container in this configuration.
  Some example configurations are shown below.   
  For JBoss 7:

  synapse.beanstalks=demo,foo
  # JNDI properties
  synapse.beanstalks.demo.java.naming.factory.url.pkgs=org.jboss.ejb.client.naming
  # Cache settings
  synapse.beanstalks.demo.cache.warn.limit.stateless=256
  synapse.beanstalks.demo.cache.warn.limit.stateful=256
  synapse.beanstalks.demo.cache.timeout.stateless=30
  synapse.beanstalks.demo.cache.timeout.stateful=30

    
  For JBoss 6:

  synapse.beanstalks=demo,foo
  # JNDI properties
  synapse.beanstalks.demo.java.naming.factory.initial=org.jnp.interfaces.NamingContextFactory
  synapse.beanstalks.demo.java.naming.factory.url.pkgs=org.jboss.naming:org.jnp.interfaces
  synapse.beanstalks.demo.java.naming.provider.url=localhost:1099
  # Cache settings
  synapse.beanstalks.demo.cache.warn.limit.stateless=256
  synapse.beanstalks.demo.cache.warn.limit.stateful=256
  synapse.beanstalks.demo.cache.timeout.stateless=30
  synapse.beanstalks.demo.cache.timeout.stateful=30
- If the JNDI names assigned to the EJBs by your EJB Container differ from
  the JNDI names specified in the sample 470 configuration file
  (repository/conf/sample/synapse\_sample\_470.xml), edit the *jndiName*
  attribute of all < ejb /> mediator invocations in the
  *synapse\_sample\_470.xml* accordingly.
- Start Synapse using the configuration numbered 470
  (repository/conf/sample/synapse\_sample\_470.xml):

  Unix/Linux: sh synapse.sh -sample 470  
  Windows: synapse.bat -sample 470

### <a id="synapse-apache-org-userguide-samples-sample470--Executing_the_Client"></a>Executing the Client

Send the following request to http://localhost:8280/services/StoreLocatorProxy
using a tool such at [TCPMon](http://ws.apache.org/tcpmon/) or curl.

<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Body>
<getClosestStore xmlns="http://services.samples">
<latitude>78</latitude>
<longitude>8</longitude>
</getClosestStore>
</soapenv:Body>
</soapenv:Envelope>

When the *StoreLocatorProxy* receives the request, it first creates an instance of
*samples.bean.Location* using the Bean mediator.
Then, it sets the properties of the newly created bean with the values
extracted from the incoming SOAP message, again using the Bean mediator.

The subsequent EJB mediator invokes the *getClosestStore()* method on the remote stateless
session bean, *StoreLocator*, with the previously populated
Location object as an argument and stores the result in the *store\_id* message context
property. Another EJB mediator that follows calls the *getStoreById()* method
on a second stateless session bean,*StoreRegistry*, to obtain store details encapsulated in
a JavaBean (an instance of samples.bean.Store) and stores this resulting bean in a message context
property named *store*. The *demo* beanstalk provides all necessary configurations
needed for the two remote EJB invocations.

Finally, Enrich and Bean mediators are used to build the response message extracting properties from
the JavaBean stored in the *store* message context property.

A sample response is shown below.

<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Body>
<getClosestStoreResponse>
<store>
<name>Kadawatha</name>
<address>253, Kandy Road, Kadawatha</address>
<phone>0112990789</phone>
</store>
</getClosestStoreResponse>
</soapenv:Body>
</soapenv:Envelope>

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample471"></a>

# Apache Synapse – Apache Synapse - Sample 471

## <a id="synapse-apache-org-userguide-samples-sample471--Sample_471:_Introduction_to_the_EJB_Mediator_II_-_Invoking_Stateful_Session_Beans"></a>Sample 471: Introduction to the EJB Mediator II - Invoking Stateful Session Beans

<definitions>
<proxy name="BuyAllProxy" transports="https http" startOnLoad="true" trace="disable">
<target>
<!-- Iterate over all items in the request and call addItem() on the ShoppingCart EJB for each item. -->
<inSequence>
<property name="SESSION\_ID" expression="get-property('MessageID')"/>
<iterate xmlns:m0="http://services.samples" preservePayload="false"
expression="//m0:buyItems/m0:items/m0:item">
<target>
<sequence>
<ejb class="samples.ejb.ShoppingCart" beanstalk="demo" method="addItem" sessionId="{get-property('SESSION\_ID')}" jndiName="ShoppingCartBean/remote">
<args>
<arg value="{//m:item//m:id}" xmlns:m="http://services.samples"/>
<arg value="{//m:item//m:quantity}" xmlns:m="http://services.samples"/>
</args>
</ejb>
<sequence key="collector"/>
</sequence>
</target>
</iterate>
</inSequence>
</target>
</proxy>
<!-- Prepare the response once all addItem() calls are finished. -->
<sequence name="collector">
<aggregate>
<onComplete>
<ejb class="samples.ejb.ShoppingCart" beanstalk="demo" method="getItemCount" sessionId="{get-property('SESSION\_ID')}" target="ITEM\_COUNT"/>
<ejb class="samples.ejb.ShoppingCart" beanstalk="demo" method="getTotal" sessionId="{get-property('SESSION\_ID')}" target="TOTAL" remove="true"/>
<payloadFactory>
<format>
<buyAllResponse xmlns="">
<itemCount>$1</itemCount>
<total>$2</total>
</buyAllResponse>
</format>
<args>
<arg expression="get-property('ITEM\_COUNT')"/>
<arg expression="get-property('TOTAL')"/>
</args>
</payloadFactory>
<respond/>
</onComplete>
</aggregate>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample471--Objective"></a>Objective

Demonstrate the usage of the EJB mediator for invoking EJB Stateful
Session Beans hosted on a remote EJB Container.

### <a id="synapse-apache-org-userguide-samples-sample471--Pre-requisites"></a>Pre-requisites

- Follow steps 1 to 5 in [Sample 470](#synapse-apache-org-userguide-samples-sample470) to host the EJBs
  in an EJB Container of your choice and to configure the *demo* beanstalk.
- If the JNDI names assigned to the EJBs by your EJB Container differ from
  the JNDI names specified in the sample 471 configuration file
  (repository/conf/sample/synapse\_sample\_471.xml), edit the *jndiName*
  attribute of all < ejb /> mediator invocations in the
  *synapse\_sample\_471.xml* accordingly.
- Start Synapse using the configuration numbered 471
  (repository/conf/sample/synapse\_sample\_471.xml):

  Unix/Linux: sh synapse.sh -sample 471  
  Windows: synapse.bat -sample 471

### <a id="synapse-apache-org-userguide-samples-sample471--Executing_the_Client"></a>Executing the Client

Send the following request to http://localhost:8280/services/BuyAllProxy
using a tool such at [TCPMon](http://ws.apache.org/tcpmon/) or curl.

<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Body>
<buyItems xmlns="http://services.samples">
<items>
<item>
<id>2150</id>
<quantity>1</quantity>
</item>
<item>
<id>1189</id>
<quantity>2</quantity>
</item>
<item>
<id>890</id>
<quantity>4</quantity>
</item>
</items>
</buyItems>
</soapenv:Body>
</soapenv:Envelope>

Each instance of the *ShoppingCart* stateful session bean hosted on the remote EJB container
maintains a state which keeps track of the number and the total price of the items
added via its *addItem(String itemId, int count)* method. The *float getTotal()* and
*int getItemCount()* methods are used to retrieve this state.

When the *BuyAllProxy* receives the above request, it iterates over all <item> elements
in the request and calls the *addItem()* method on the *ShoppingCart* bean,
once per each <item>, using the EJB mediator. Since the *sessionId* used for these
invocations is actually the message ID, each request works on a new *ShoppingCart* instance
which is created at the first EJB mediator invocation in that request's flow.

When all *addItem()* method calls are finished, *getItemCount()* and *getTotal()* methods
are invoked on the same *ShoppingCart* instance within the *collector* sequence and
results are stored in two message context properties named *ITEM\_COUNT* and *TOTAL*.
The last EJB call on the shopping cart sets the EJB mediator attribute *remove = "true"*
instructing the current stateful bean stub to be removed from the beanstalk. This is because the
same stateful bean instance will not be used again (each request uses a new bean instance). If the
user does not explicitly remove the stub using this attribute, it will be removed automatically
upon timeout as specified by the beanstalk configuration.

Finally, the PayloadFactory mediator is used to build the response message which is sent back to the
client.

A sample response is shown below.

<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Body>
<buyAllResponse>
<itemCount>7</itemCount>
<total>807.0</total>
</buyAllResponse>
</soapenv:Body>
</soapenv:Envelope>

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample5"></a>

# Apache Synapse – Apache Synapse - Sample 5

## <a id="synapse-apache-org-userguide-samples-sample5--Sample_5:_Creating_SOAP_Faults_and_Changing_the_Direction_of_Messages"></a>Sample 5: Creating SOAP Faults and Changing the Direction of Messages

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="myFaultHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason expression="get-property('ERROR\_MESSAGE')"/>
</makefault>
<send/>
</sequence>
<sequence name="main" onError="myFaultHandler">
<in>
<switch xmlns:m0="http://services.samples" source="//m0:getQuote/m0:request/m0:symbol">
<case regex="MSFT">
<send>
<endpoint>
<address uri="http://bogus:9000/services/NonExistentStockQuoteService"/>
</endpoint>
</send>
</case>
<case regex="SUN">
<send>
<endpoint>
<address uri="http://localhost:9009/services/NonExistentStockQuoteService"/>
</endpoint>
</send>
</case>
</switch>
<drop/>
</in>
<out>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample5--Objective"></a>Objective

Demonstrating how makefault mediator can be used to construct custom SOAP
faults and change the direction (in/out) of messages.

### <a id="synapse-apache-org-userguide-samples-sample5--Pre-requisites"></a>Pre-requisites

- Start Synapse using the configuration numbered 5 (repository/conf/sample/synapse\_sample\_5.xml)

  Unix/Linux: sh synapse.sh -sample 5  
  Windows: synapse.bat -sample 5

### <a id="synapse-apache-org-userguide-samples-sample5--Executing_the_Client"></a>Executing the Client

When the MSFT stock quote is requested, an unknown host exception would be
generated. A connection refused exception would be generated for the SUN stock
request. These errors are captured and returned to the original client as a SOAP
fault in this example.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=MSFT

returns,

<soapenv:Fault xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<faultcode>soapenv:Client</faultcode>
<faultstring>java.net.UnknownHostException: bogus</faultstring>
<detail />
</soapenv:Fault>

And

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dsymbol=SUN

returns,

<soapenv:Fault xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<faultcode>soapenv:Client</faultcode>
<faultstring>java.net.ConnectException: Connection refused</faultstring>
<detail />
</soapenv:Fault>

Note that the response attribute is set to 'true' on the makefault mediator.
This instructs the mediator to change the direction of messages to 'response'
as messages are transformed into SOAP faults.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample50"></a>

# Apache Synapse – Apache Synapse - Sample 50

## <a id="synapse-apache-org-userguide-samples-sample50--Sample_50:_POX_to_SOAP_Conversion"></a>Sample 50: POX to SOAP Conversion

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<!-- filtering of messages with XPath and regex matches -->
<header name="Action" value="urn:getQuote"/>
<filter source="get-property('To')" regex=".\*/StockQuote.\*">
<then>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService" format="soap11"/>
</endpoint>
</send>
<drop/>
</then>
</filter>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample50--Objective"></a>Objective

Demonstrating how to convert a POX (Plain Old XML) message to a standard SOAP 1.1
request.

### <a id="synapse-apache-org-userguide-samples-sample50--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 50 (repository/conf/sample/synapse\_sample\_50.xml)

  Unix/Linux: sh synapse.sh -sample 50  
  Windows: synapse.bat -sample 50

### <a id="synapse-apache-org-userguide-samples-sample50--Executing_the_Client"></a>Executing the Client

Execute the sample client as follows and send a plain XML getQuote request to
Synapse.

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuote -Drest=true

The request sent by the client will look something like this.

POST /services/StockQuote HTTP/1.1
Content-Type: application/xml; charset=UTF-8;action="urn:getQuote";
SOAPAction: urn:getQuote
User-Agent: Axis2
Host: 127.0.0.1
Transfer-Encoding: chunked
75
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>IBM</m0:symbol>
</m0:request>
</m0:getQuote>0

Note that this is simply an XML payload going over the HTTP connection. It is
not a SOAP envelope and hence the content type is set to application/xml. Synapse
will convert the above message to a valid SOAP 1.1 request and send to the sample
Axis2 server. The response from Axis2 will also be a SOAP message, which will
be converted back to POX format before sending back to the client.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample500"></a>

# Apache Synapse – Apache Synapse - Sample 500

## <a id="synapse-apache-org-userguide-samples-sample500--Sample_500:_Introduction_to_Eventing"></a>Sample 500: Introduction to Eventing

<definitions xmlns="http://ws.apache.org/ns/synapse">
<eventSource name="SampleEventSource">
<subscriptionManager
class="org.apache.synapse.eventing.managers.DefaultInMemorySubscriptionManager">
<!--property name="registryURL" value="http://localhost:8180/wso2registry"/>
<property name="username" value="admin"/>
<property name="password" value="admin"/-->
<property name="topicHeaderName" value="Topic"/>
<property name="topicHeaderNS" value="http://apache.org/aip"/>
</subscriptionManager>
</eventSource>
<sequence name="PublicEventSource">
<log level="full"/>
<eventPublisher eventSourceName="SampleEventSource"/>
</sequence>
<proxy name="EventingProxy">
<target inSequence="PublicEventSource"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample500--Objective"></a>Objective

Demonstrate the use of the Eventing functionality built into Synapse

### <a id="synapse-apache-org-userguide-samples-sample500--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 500 (repository/conf/sample/synapse\_sample\_500.xml)

  Unix/Linux: sh synapse.sh -sample 500  
  Windows: synapse.bat -sample 500

### <a id="synapse-apache-org-userguide-samples-sample500--Executing_the_Client"></a>Executing the Client

In this sample an event source is creted based on the provided configuration. Event
subscriber subscribes for the events, Event sender publishes events and the
SimpleStockQuoteService acts as the Event Sink.

First, invoke the sample client (Subscriber) as follows.

ant eventsubscriber

This will create a new subscription with the SimpleStockQuoteService deployed on
the sample Axis2 server acting as the sink. Whenever a new event is published,
SimpleStockQuoteService will receive a message with that event. You should see a
message like this on the client console confirming the subscription.

[java] Subscription identifier: urn:uuid:6989F66706E73C69F5259116575749162017010321

You will need this identifier to modify the subscription in the next steps. Now,
invoke the client (Sender) as follows.

ant eventsender

This will send a placeOrder request to the EventingProxy. You should see a
message in the Synapse logs. Note the presence of the following SOAP header
in the request.

<aip:Topic xmlns:aip="http://apache.org/aip">synapse/event/test</aip:Topic>

Since there is a single subscription with SimpleStockQuoteService as the sink,
Synapse will send the message to the sample Axis2 server and you should see the
following message in its logs:

Accepted order for : 1000 stocks of GOOG at $ 10.1

You can also send various other WS-Eventing messages from the sample client to
modify the subscription details. To get the current status of the subscription,
invoke the client as follows.

ant eventsubscriber -Dmode=getstatus -Didentifier=*<identifier>*

To renew the subscription, invoke the client as follows.

ant eventsubscriber -Dmode=renew -Didentifier=*<identifier>* -Dexpires=2012-12-31T21:07:00.000-08:00

Finally, in order to unsubscribe, use the following command.

ant eventsubscriber -Dmode=unsubscribe -Didentifier=*<identifier>*

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample501"></a>

# Apache Synapse – Apache Synapse - Sample 501

## <a id="synapse-apache-org-userguide-samples-sample501--Sample_501:_Event_Source_with_Static_Subscriptions"></a>Sample 501: Event Source with Static Subscriptions

<definitions xmlns="http://ws.apache.org/ns/synapse">
<eventSource name="SampleEventSource">
<subscriptionManager
class="org.apache.synapse.eventing.managers.DefaultInMemorySubscriptionManager">
<!--property name="registryURL" value="http://localhost:8180/wso2registry"/>
<property name="username" value="admin"/>
<property name="password" value="admin"/-->
<property name="topicHeaderName" value="Topic"/>
<property name="topicHeaderNS" value="http://apache.org/aip"/>
</subscriptionManager>
<subscription id="mysub1">
<filter source="synapse/event/test"
dialect="http://synapse.apache.org/eventing/dialect/topicFilter"/>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</subscription>
<subscription id="mysub2">
<filter source="synapse/event/test"
dialect="http://synapse.apache.org/eventing/dialect/topicFilter"/>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
<expires>2020-06-27T21:07:00.000-08:00</expires>
</subscription>
</eventSource>
<sequence name="PublicEventSource">
<log level="full"/>
<eventPublisher eventSourceName="SampleEventSource"/>
</sequence>
<proxy name="EventingProxy">
<target inSequence="PublicEventSource"/>
</proxy>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample501--Objective"></a>Objective

Showcase how to use a predefined set of static subscriptions with Synapse

### <a id="synapse-apache-org-userguide-samples-sample501--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 501 (repository/conf/sample/synapse\_sample\_501.xml)

  Unix/Linux: sh synapse.sh -sample 501  
  Windows: synapse.bat -sample 501

### <a id="synapse-apache-org-userguide-samples-sample501--Executing_the_Client"></a>Executing the Client

In this sample configuration, two static subscriptions are created by providing the
SimpleStockQuoteService as the event sink. To try this out, invoke the sample
client as follows.

ant eventsender

Events will be mediated and sent to the sample Axis2 server as operated by the
two static subscriptions.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample502"></a>

# Apache Synapse – Apache Synapse - Sample 502

## <a id="synapse-apache-org-userguide-samples-sample502--Sample_502:_Transforming_Events_Before_Publish"></a>Sample 502: Transforming Events Before Publish

<definitions xmlns="http://ws.apache.org/ns/synapse">
<eventSource name="SampleEventSource">
<subscriptionManager
class="org.apache.synapse.eventing.managers.DefaultInMemorySubscriptionManager">
<!--property name="registryURL" value="http://localhost:8180/wso2registry"/>
<property name="username" value="admin"/>
<property name="password" value="admin"/-->
<property name="topicHeaderName" value="Topic"/>
<property name="topicHeaderNS" value="http://apache.org/aip"/>
</subscriptionManager>
<subscription id="mysub1">
<filter source="synapse/event/test"
dialect="http://synapse.apache.org/eventing/dialect/topicFilter"/>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</subscription>
</eventSource>
<sequence name="PublicEventSource">
<log level="full"/>
<xslt key="xslt-key-req"/>
<log level="full"/>
<eventPublisher eventSourceName="SampleEventSource"/>
</sequence>
<proxy name="EventingProxy">
<target inSequence="PublicEventSource"/>
</proxy>
<localEntry key="xslt-key-req"
src="file:repository/conf/sample/resources/transform/transform\_eventing.xslt"/>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample502--Objective"></a>Objective

Demonstrate how to mediate and transform events before they are sent to the
target event sinks

### <a id="synapse-apache-org-userguide-samples-sample502--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 502 (repository/conf/sample/synapse\_sample\_502.xml)

  Unix/Linux: sh synapse.sh -sample 502  
  Windows: synapse.bat -sample 502

### <a id="synapse-apache-org-userguide-samples-sample502--Executing_the_Client"></a>Executing the Client

In this sample, the event (order request) is transformed into a new order with
a different namesapce using the XSLT mediator. Invoke the client as follows.

ant eventsender

Event will be mediated through the 'PublicEventSource' sequence and get transformed
before it gets published to the event sink (Axis2 server).

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample51"></a>

# Apache Synapse – Apache Synapse - Sample 51

## <a id="synapse-apache-org-userguide-samples-sample51--Sample_51:_MTOM_and_SwA_Optimizations_and_RequestResponse_Correlation"></a>Sample 51: MTOM and SwA Optimizations and Request/Response Correlation

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<filter source="get-property('Action')" regex="urn:uploadFileUsingMTOM">
<then>
<property name="example" value="mtom"/>
<send>
<endpoint>
<address uri="http://localhost:9000/services/MTOMSwASampleService"
optimize="mtom"/>
</endpoint>
</send>
</then>
</filter>
<filter source="get-property('Action')" regex="urn:uploadFileUsingSwA">
<then>
<property name="example" value="swa"/>
<send>
<endpoint>
<address uri="http://localhost:9000/services/MTOMSwASampleService"
optimize="swa"/>
</endpoint>
</send>
</then>
</filter>
</in>
<out>
<filter source="get-property('example')" regex="mtom">
<then>
<property name="enableMTOM" value="true" scope="axis2"/>
</then>
</filter>
<filter source="get-property('example')" regex="swa">
<then>
<property name="enableSwA" value="true" scope="axis2"/>
</then>
</filter>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample51--Objective"></a>Objective

Demonstrate the use of content optimization mechanisms like MTOM and SwA with
Synapse.

### <a id="synapse-apache-org-userguide-samples-sample51--Pre-requisites"></a>Pre-requisites

- Deploy the MTOMSwASampleService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 51 (repository/conf/sample/synapse\_sample\_51.xml)

  Unix/Linux: sh synapse.sh -sample 51  
  Windows: synapse.bat -sample 51

### <a id="synapse-apache-org-userguide-samples-sample51--Executing_the_Client"></a>Executing the Client

Execute the client as follows to send a MTOM optimized request to Synapse.

ant optimizeclient -Dopt\_mode=mtom

Synapse sets a local message context property, and forwards the message to
'http://localhost:9000/services/MTOMSwASampleService', while optimizing binary
content as MTOM. By sending this message through TCPMon you will be able to see
the actual message sent by Synapse if required.

POST /services/MTOMSwASampleService HTTP/1.1
Host: 127.0.0.1
SOAPAction: urn:uploadFileUsingMTOM
Content-Type: multipart/related; boundary=MIMEBoundaryurn\_uuid\_B94996494E1DD5F9B51177413845353; type="application/xop+xml";
start="<0.urn:uuid:B94996494E1DD5F9B51177413845354@apache.org>"; start-info="text/xml"; charset=UTF-8
Transfer-Encoding: chunked
Connection: Keep-Alive
User-Agent: Synapse-HttpComponents-NIO
--MIMEBoundaryurn\_uuid\_B94996494E1DD5F9B51177413845353241
Content-Type: application/xop+xml; charset=UTF-8; type="text/xml"
Content-Transfer-Encoding: binary
Content-ID:
<0.urn:uuid:B94996494E1DD5F9B51177413845354@apache.org>221b1
<?xml version='1.0' encoding='UTF-8'?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Body>
<m0:uploadFileUsingMTOM xmlns:m0="http://www.apache-synapse.org/test">
<m0:request>
<m0:image>
<xop:Include href="cid:1.urn:uuid:78F94BC50B68D76FB41177413845003@apache.org" xmlns:xop="http://www.w3.org/2004/08/xop/include" />
</m0:image>
</m0:request>
</m0:uploadFileUsingMTOM>
</soapenv:Body>
</soapenv:Envelope>
--MIMEBoundaryurn\_uuid\_B94996494E1DD5F9B51177413845353217
Content-Type: image/gif
Content-Transfer-Encoding: binary
Content-ID:
<1.urn:uuid:78F94BC50B68D76FB41177413845003@apache.org>22800GIF89a... << binary content >>

During response processing, by checking the local message property Synapse can
discover past information about the current message context, and use this
knowledge to send the response back to the client in the same format as the
original request.

When the client executes successfully, it will upload a file containing the ASF
logo and receive a response which is savesd a temporary file.

[java] Sending file : ./../../repository/conf/sample/resources/mtom/asf-logo.gif as MTOM
[java] Saved response to file : ./../../work/temp/sampleClient/mtom-4417.gif

Now invoke the client as follows to send a SwA optimized request.

ant optimizeclient -Dopt\_mode=swa

This is identical to the previous invocation with only difference being the use
of SwA (SOAP with Attachement) instead of MTOM for content optimization. You
will get an output similar to following.

[java] Sending file : ./../../repository/conf/sample/resources/mtom/asf-logo.gif as SwA
[java] Saved response to file : ./../../work/temp/sampleClient/swa-30391.gif

By using TCPMon and sending the message through it, one can see that the
requests and responses sent are indeed sent as HTTP attachments as follows.

POST /services/MTOMSwASampleService HTTP/1.1
Host: 127.0.0.1
SOAPAction: urn:uploadFileUsingSwA
Content-Type: multipart/related; boundary=MIMEBoundaryurn\_uuid\_B94996494E1DD5F9B51177414170491; type="text/xml";
start="<0.urn:uuid:B94996494E1DD5F9B51177414170492@apache.org>"; charset=UTF-8
Transfer-Encoding: chunked
Connection: Keep-Alive
User-Agent: Synapse-HttpComponents-NIO
--MIMEBoundaryurn\_uuid\_B94996494E1DD5F9B51177414170491225
Content-Type: text/xml; charset=UTF-8
Content-Transfer-Encoding: 8bit
Content-ID:
<0.urn:uuid:B94996494E1DD5F9B51177414170492@apache.org>22159
<?xml version='1.0' encoding='UTF-8'?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Body>
<m0:uploadFileUsingSwA xmlns:m0="http://www.apache-synapse.org/test">
<m0:request>
<m0:imageId>urn:uuid:15FD2DA2584A32BF7C1177414169826</m0:imageId>
</m0:request>
</m0:uploadFileUsingSwA>
</soapenv:Body>
</soapenv:Envelope>22--34MIMEBoundaryurn\_uuid\_B94996494E1DD5F9B511774141704912
17
Content-Type: image/gif
Content-Transfer-Encoding: binary
Content-ID:
<urn:uuid:15FD2DA2584A32BF7C1177414169826>22800GIF89a... << binary content >>

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample52"></a>

# Apache Synapse – Apache Synapse - Sample 52

## <a id="synapse-apache-org-userguide-samples-sample52--Sample_52:_Session-less_Load_Balancing_Between_3_Endpoints"></a>Sample 52: Session-less Load Balancing Between 3 Endpoints

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main" onError="errorHandler">
<in>
<send>
<endpoint>
<loadbalance>
<endpoint>
<address uri="http://localhost:9001/services/LBService1">
<enableAddressing/>
<suspendDurationOnFailure>60</suspendDurationOnFailure>
</address>
</endpoint>
<endpoint>
<address uri="http://localhost:9002/services/LBService1">
<enableAddressing/>
<suspendDurationOnFailure>60</suspendDurationOnFailure>
</address>
</endpoint>
<endpoint>
<address uri="http://localhost:9003/services/LBService1">
<enableAddressing/>
<suspendDurationOnFailure>60</suspendDurationOnFailure>
</address>
</endpoint>
</loadbalance>
</endpoint>
</send>
<drop/>
</in>
<out>
<!-- Send the messages where they have been sent (i.e. implicit To EPR) -->
<send/>
</out>
</sequence>
<sequence name="errorHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason value="COULDN'T SEND THE MESSAGE TO THE SERVER."/>
</makefault>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample52--Objective"></a>Objective

Demonstrate the ability of Synapse to act as a load balancer for a set of
servers hosting stateless services

### <a id="synapse-apache-org-userguide-samples-sample52--Pre-requisites"></a>Pre-requisites

- Deploy the LoadbalanceFailoverService in the sample Axis2 server (go to
  samples/axis2Server/src/LoadbalanceFailoverService and run 'ant')
- Start 3 instances of the Axis2 server on different ports as follows

  ./axis2server.sh -http 9001 -https 9005 -name MyServer1  
  ./axis2server.sh -http 9002 -https 9006 -name MyServer2  
  ./axis2server.sh -http 9003 -https 9007 -name MyServer3
- Start Synapse using the configuration numbered 52 (repository/conf/sample/synapse\_sample\_52.xml)

  Unix/Linux: sh synapse.sh -sample 52  
  Windows: synapse.bat -sample 52

### <a id="synapse-apache-org-userguide-samples-sample52--Executing_the_Client"></a>Executing the Client

Invoke the sample client as follows

ant loadbalancefailover -Di=100

This will send 100 requests to the LoadbalanceFailoverService through Synapse.
Synapse will distribute the load among the three endpoints mentioned in the
configuration in round-robin manner. LoadbalanceFailoverService appends the name
of the server to the response, so that client can determine which server has
processed the message. If you examine the console output of the client, you can
see that requests are processed by three servers as follows:

[java] Request: 1 ==> Response from server: MyServer1
[java] Request: 2 ==> Response from server: MyServer2
[java] Request: 3 ==> Response from server: MyServer3
[java] Request: 4 ==> Response from server: MyServer1
[java] Request: 5 ==> Response from server: MyServer2
[java] Request: 6 ==> Response from server: MyServer3
[java] Request: 7 ==> Response from server: MyServer1
...

Now run the client without the -Di=100 parameter to send requests indefinitely.
While running the client shutdown the server named MyServer1. Then you can observe
that requests are only distributed among MyServer2 and MyServer3. Console output
before and after shutting down MyServer1 is listed below (MyServer1 was shutdown
after request 63):

...
[java] Request: 61 ==> Response from server: MyServer1
[java] Request: 62 ==> Response from server: MyServer2
[java] Request: 63 ==> Response from server: MyServer3
[java] Request: 64 ==> Response from server: MyServer2
[java] Request: 65 ==> Response from server: MyServer3
[java] Request: 66 ==> Response from server: MyServer2
[java] Request: 67 ==> Response from server: MyServer3
...

Now restart MyServer1. You can observe that requests will be again sent to all
three servers within 60 seconds. This is because we have specified
<suspendDurationOnFailure> as 60 seconds in the configuration. Therefore,
load balance endpoint will suspend any failed child endpoint only for 60 seconds
after detecting the failure.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample53"></a>

# Apache Synapse – Apache Synapse - Sample 53

## <a id="synapse-apache-org-userguide-samples-sample53--Sample_53:_Fail-over_Routing_Among_3_Endpoints"></a>Sample 53: Fail-over Routing Among 3 Endpoints

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main" onError="errorHandler">
<in>
<send>
<endpoint>
<failover>
<endpoint>
<address uri="http://localhost:9001/services/LBService1">
<enableAddressing/>
<suspendDurationOnFailure>60</suspendDurationOnFailure>
</address>
</endpoint>
<endpoint>
<address uri="http://localhost:9002/services/LBService1">
<enableAddressing/>
<suspendDurationOnFailure>60</suspendDurationOnFailure>
</address>
</endpoint>
<endpoint>
<address uri="http://localhost:9003/services/LBService1">
<enableAddressing/>
<suspendDurationOnFailure>60</suspendDurationOnFailure>
</address>
</endpoint>
</failover>
</endpoint>
</send>
<drop/>
</in>
<out>
<!-- Send the messages where they have been sent (i.e. implicit To EPR) -->
<send/>
</out>
</sequence>
<sequence name="errorHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason value="COULDN'T SEND THE MESSAGE TO THE SERVER."/>
</makefault>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample53--Objective"></a>Objective

Demonstrate the fail-over routing capabilities of Synapse. In fail-over routing
messages are sent to a designated primary endpoint. When the primary endpoint
fails, Synapse fails over to the one of the backup endpoints.

### <a id="synapse-apache-org-userguide-samples-sample53--Pre-requisites"></a>Pre-requisites

- Deploy the LoadbalanceFailoverService in the sample Axis2 server (go to
  samples/axis2Server/src/LoadbalanceFailoverService and run 'ant')
- Start 3 instances of the Axis2 server on different ports as follows

  ./axis2server.sh -http 9001 -https 9005 -name MyServer1  
  ./axis2server.sh -http 9002 -https 9006 -name MyServer2  
  ./axis2server.sh -http 9003 -https 9007 -name MyServer3
- Start Synapse using the configuration numbered 53 (repository/conf/sample/synapse\_sample\_53.xml)

  Unix/Linux: sh synapse.sh -sample 53  
  Windows: synapse.bat -sample 53

### <a id="synapse-apache-org-userguide-samples-sample53--Executing_the_Client"></a>Executing the Client

Above configuration sends messages with the fail-over behavior. Initially the
server at port 9001 is treated as primary and other two are treated as backups.
Messages are always directed only to the primary server. If the primary server
fails, next listed server is selected as the primary. Thus, messages are sent
successfully as long as there is at least one active server. To test this, run
the loadbalancefailover client to send infinite requests as follows:

ant loadbalancefailover

You can see that all requests are processed by MyServer1. Now shutdown MyServer1
and inspect the console output of the client. You will observe that all subsequent
requests are processed by MyServer2. (MyServer 1 was shutdown after request 127)

...
[java] Request: 125 ==> Response from server: MyServer1
[java] Request: 126 ==> Response from server: MyServer1
[java] Request: 127 ==> Response from server: MyServer1
[java] Request: 128 ==> Response from server: MyServer2
[java] Request: 129 ==> Response from server: MyServer2
[java] Request: 130 ==> Response from server: MyServer2
...

You can keep on shutting servers down like this. Client will get a response until
you shutdown all listed servers. Once all servers are shutdown, the error sequence
is triggered and a fault message is sent to the client as follows.

[java] COULDN'T SEND THE MESSAGE TO THE SERVER.

Once a server is detected as failed, it will be added to the active servers
list again after 60 seconds (specified in <suspendDurationOnFailure> in
the configuration). Therefore, if you have restarted any of the stopped servers,
messages will be directed to the newly started server within 60 seconds.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample54"></a>

# Apache Synapse – Apache Synapse - Sample 54

## <a id="synapse-apache-org-userguide-samples-sample54--Sample_54:_Session_Affinity_Load_Balancing_Between_3_Endpoints"></a>Sample 54: Session Affinity Load Balancing Between 3 Endpoints

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main" onError="errorHandler">
<in>
<send>
<endpoint>
<!-- specify the session as the simple client session provided by Synapse for
testing purpose -->
<session type="simpleClientSession"/>
<loadbalance>
<endpoint>
<address uri="http://localhost:9001/services/LBService1">
<enableAddressing/>
</address>
</endpoint>
<endpoint>
<address uri="http://localhost:9002/services/LBService1">
<enableAddressing/>
</address>
</endpoint>
<endpoint>
<address uri="http://localhost:9003/services/LBService1">
<enableAddressing/>
</address>
</endpoint>
</loadbalance>
</endpoint>
</send>
<drop/>
</in>
<out>
<!-- Send the messages where they have been sent (i.e. implicit To EPR) -->
<send/>
</out>
</sequence>
<sequence name="errorHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason value="COULDN'T SEND THE MESSAGE TO THE SERVER."/>
</makefault>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample54--Objective"></a>Objective

Showcase the ability of Synapse to act as a session aware load balancer with
simple client sessions

### <a id="synapse-apache-org-userguide-samples-sample54--Pre-requisites"></a>Pre-requisites

- Deploy the LoadbalanceFailoverService in the sample Axis2 server (go to
  samples/axis2Server/src/LoadbalanceFailoverService and run 'ant')
- Start 3 instances of the Axis2 server on different ports as follows

  ./axis2server.sh -http 9001 -https 9005 -name MyServer1  
  ./axis2server.sh -http 9002 -https 9006 -name MyServer2  
  ./axis2server.sh -http 9003 -https 9007 -name MyServer3
- Start Synapse using the configuration numbered 54 (repository/conf/sample/synapse\_sample\_54.xml)

  Unix/Linux: sh synapse.sh -sample 54  
  Windows: synapse.bat -sample 54

### <a id="synapse-apache-org-userguide-samples-sample54--Executing_the_Client"></a>Executing the Client

Above configuration is same as the load balancing configuration in
[sample 52](#synapse-apache-org-userguide-samples-sample52), except that the session type is specified
as 'simpleClientSession'. This is a client initiated session, which means that
the client generates the session identifier and sends it with each request. In
this sample, client adds a SOAP header named ClientID containing the identifier
of the client. Synapse binds this ID with a server on the first request and sends
all successive requests containing that ID to the same server. Now switch to
samples/axis2Client directory and run the client using the following command to
check this in action.

ant loadbalancefailover -Dmode=session

In the session mode, client continuously sends requests with three different
client (session) IDs. One ID is selected among these three IDs for each request
randomly. Then client prints the session ID with the responded server for each
request. Client output for the first 10 requests are shown below.

[java] Request: 1 Session number: 1 Response from server: MyServer3
[java] Request: 2 Session number: 2 Response from server: MyServer2
[java] Request: 3 Session number: 0 Response from server: MyServer1
[java] Request: 4 Session number: 2 Response from server: MyServer2
[java] Request: 5 Session number: 1 Response from server: MyServer3
[java] Request: 6 Session number: 2 Response from server: MyServer2
[java] Request: 7 Session number: 2 Response from server: MyServer2
[java] Request: 8 Session number: 1 Response from server: MyServer3
[java] Request: 9 Session number: 0 Response from server: MyServer1
[java] Request: 10 Session number: 0 Response from server: MyServer1
...

You can see that session ID 0 is always directed to the server named MyServer1.
That means session ID 0 is bound to MyServer1. Similarly session 1 and 2 are bound
to MyServer3 and MyServer2 respectively.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample55"></a>

# Apache Synapse – Apache Synapse - Sample 55

## <a id="synapse-apache-org-userguide-samples-sample55--Sample_55:_Session_Affinity_Load_Balancing_Between_Fail-over_Endpoints"></a>Sample 55: Session Affinity Load Balancing Between Fail-over Endpoints

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main" onError="errorHandler">
<in>
<send>
<endpoint>
<!-- specify the session as the simple client session provided by Synapse for
testing purpose -->
<session type="simpleClientSession"/>
<loadbalance>
<endpoint>
<failover>
<endpoint>
<address uri="http://localhost:9001/services/LBService1">
<enableAddressing/>
</address>
</endpoint>
<endpoint>
<address uri="http://localhost:9002/services/LBService1">
<enableAddressing/>
</address>
</endpoint>
</failover>
</endpoint>
<endpoint>
<failover>
<endpoint>
<address uri="http://localhost:9003/services/LBService1">
<enableAddressing/>
</address>
</endpoint>
<endpoint>
<address uri="http://localhost:9004/services/LBService1">
<enableAddressing/>
</address>
</endpoint>
</failover>
</endpoint>
</loadbalance>
</endpoint>
</send>
<drop/>
</in>
<out>
<!-- Send the messages where they have been sent (i.e. implicit To EPR) -->
<send/>
</out>
</sequence>
<sequence name="errorHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason value="COULDN'T SEND THE MESSAGE TO THE SERVER."/>
</makefault>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample55--Objective"></a>Objective

Demonstrate session aware load balancing in conjunction with fail-over
routing.

### <a id="synapse-apache-org-userguide-samples-sample55--Pre-requisites"></a>Pre-requisites

- Deploy the LoadbalanceFailoverService in the sample Axis2 server (go to
  samples/axis2Server/src/LoadbalanceFailoverService and run 'ant')
- Start 4 instances of the Axis2 server on different ports as follows

  ./axis2server.sh -http 9001 -https 9005 -name MyServer1  
  ./axis2server.sh -http 9002 -https 9006 -name MyServer2  
  ./axis2server.sh -http 9003 -https 9007 -name MyServer3  
  ./axis2server.sh -http 9004 -https 9008 -name MyServer4
- Start Synapse using the configuration numbered 55 (repository/conf/sample/synapse\_sample\_55.xml)

  Unix/Linux: sh synapse.sh -sample 55  
  Windows: synapse.bat -sample 55

### <a id="synapse-apache-org-userguide-samples-sample55--Executing_the_Client"></a>Executing the Client

This configuration also uses 'simpleClientSession' to bind session ID values to
servers as in [sample 54](#synapse-apache-org-userguide-samples-sample54). But fail-over endpoints are
specified as the child endpoints of the load balance endpoint. Therefore sessions
are bound to the fail-over endpoints. Session information has to be replicated
among the servers listed under each failover endpoint using some clustering
mechanism. Therefore, if one endpoint bound to a session failed, successive requets
for that session will be directed to the next endpoint in that failover group.
Run the client using the following command to observe this behaviour.

ant loadbalancefailover -Dmode=session

You can see a client output as shown below.

...
[java] Request: 222 Session number: 0 Response from server: MyServer1
[java] Request: 223 Session number: 0 Response from server: MyServer1
[java] Request: 224 Session number: 1 Response from server: MyServer1
[java] Request: 225 Session number: 2 Response from server: MyServer3
[java] Request: 226 Session number: 0 Response from server: MyServer1
[java] Request: 227 Session number: 1 Response from server: MyServer1
[java] Request: 228 Session number: 2 Response from server: MyServer3
[java] Request: 229 Session number: 1 Response from server: MyServer1
[java] Request: 230 Session number: 1 Response from server: MyServer1
[java] Request: 231 Session number: 2 Response from server: MyServer3
...

Note that session 0 is always directed to MyServer1 and session 2 is directed to
MyServer3. No requests are directed to MyServer2 and MyServer4 as they are kept
as backups by fail-over endpoints. Now shutdown the server named MyServer1 while
running the sample. You will observe that all successive requests for session 0
is now directed to MyServer2, which is the backup server for MyServer1's group.
This is shown below, where MyServer1 was shutdown after the request 534.

...
[java] Request: 529 Session number: 2 Response from server: MyServer3
[java] Request: 530 Session number: 1 Response from server: MyServer1
[java] Request: 531 Session number: 0 Response from server: MyServer1
[java] Request: 532 Session number: 1 Response from server: MyServer1
[java] Request: 533 Session number: 1 Response from server: MyServer1
[java] Request: 534 Session number: 1 Response from server: MyServer1
[java] Request: 535 Session number: 0 Response from server: MyServer2
[java] Request: 536 Session number: 0 Response from server: MyServer2
[java] Request: 537 Session number: 0 Response from server: MyServer2
[java] Request: 538 Session number: 2 Response from server: MyServer3
[java] Request: 539 Session number: 0 Response from server: MyServer2
...

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample56"></a>

# Apache Synapse – Apache Synapse - Sample 56

## <a id="synapse-apache-org-userguide-samples-sample56--Sample_56:_WSDL_Endpoint"></a>Sample 56: WSDL Endpoint

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<send>
<!-- get epr from the given wsdl -->
<endpoint>
<wsdl uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"
service="SimpleStockQuoteService"
port="SimpleStockQuoteServiceHttpSoap11Endpoint"/>
</endpoint>
</send>
</in>
<out>
<send/>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample56--Objective"></a>Objective

Showcase the ability of Synapse to use a WSDL as the target endpoint

### <a id="synapse-apache-org-userguide-samples-sample56--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 56 (repository/conf/sample/synapse\_sample\_56.xml)

  Unix/Linux: sh synapse.sh -sample 56  
  Windows: synapse.bat -sample 56

### <a id="synapse-apache-org-userguide-samples-sample56--Executing_the_Client"></a>Executing the Client

This sample uses a WSDL endpoint inside the send mediator. WSDL endpoints can
extract endpoint's address from the given WSDL. As WSDL documents can have many
services and many ports inside each service, the service and port of the
required endpoint has to be specified. As with address endpoints, QoS parameters
for the endpoint can be specified in-line in the configuration. An excerpt taken
from the sample\_proxy\_1.wsdl containing the specified service and port is
listed below.

<wsdl:service name="SimpleStockQuoteService">
<wsdl:port name="SimpleStockQuoteServiceHttpSoap11Endpoint" binding="ns:SimpleStockQuoteServiceSoap11Binding">
<soap:address location="http://localhost:9000/services/SimpleStockQuoteService.SimpleStockQuoteServiceHttpSoap11Endpoint"/>
</wsdl:port>
<wsdl:port name="SimpleStockQuoteServiceHttpSoap12Endpoint" binding="ns:SimpleStockQuoteServiceSoap12Binding">
<soap12:address location="http://localhost:9000/services/SimpleStockQuoteService.SimpleStockQuoteServiceHttpSoap12Endpoint"/>
</wsdl:port>
</wsdl:service>

Specified service and port refers to the endpoint address 'http://localhost:9000/services/SimpleStockQuoteService.SimpleStockQuoteServiceHttpSoap11Endpoint'
according to the above WSDL. Now run the client using the following command.

ant stockquote -Daddurl=http://localhost:8280/

Client will print the quote price for IBM received from the server running on
port 9000.

Standard :: Stock price = $95.26454380258552

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample57"></a>

# Apache Synapse – Apache Synapse - Sample 57

## <a id="synapse-apache-org-userguide-samples-sample57--Sample_57:_Dynamic_Load_Balancing_Between_3_Nodes"></a>Sample 57: Dynamic Load Balancing Between 3 Nodes

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main" onError="errorHandler">
<in>
<send>
<endpoint name="dynamicLB">
<dynamicLoadbalance failover="true"
algorithm="org.apache.synapse.endpoints.algorithms.RoundRobin">
<membershipHandler
class="org.apache.synapse.core.axis2.Axis2LoadBalanceMembershipHandler">
<property name="applicationDomain" value="apache.axis2.app.domain"/>
</membershipHandler>
</dynamicLoadbalance>
</endpoint>
</send>
<drop/>
</in>
<out>
<!-- Send the messages where they have been sent (i.e. implicit To EPR) -->
<send/>
</out>
</sequence>
<sequence name="errorHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason value="COULDN'T SEND THE MESSAGE TO THE SERVER."/>
</makefault>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample57--Objective"></a>Objective

Demonstrate the ability of Synapse to perform dynamic load balancing. In
dynamic load balancing, nodes can be added and removed from the pool dynamically.

### <a id="synapse-apache-org-userguide-samples-sample57--Pre-requisites"></a>Pre-requisites

- Deploy the LoadbalanceFailoverService by switching to samples/axis2Server/src/LoadbalanceFailoverService
  directory and running 'ant'.
- Enable clustering for the sample Axis2 server. Edit the axis2.xml file at
  samples/axis2Server/repository/conf directory and set the 'enable' attribute
  on the 'clustering' element to 'true'. Specify the IP address of the
  machine as the values of 'mcastBindAddress' and 'localMemberHost'
  parameters.
- Enable clustering for Synapse by editing repository/conf/axis2.xml file.
  This should be done by setting the 'enable' attribute of the 'clustering'
  element to 'true'. Also provide the IP address of your machine as the
  values of the 'mcastBindAddress' and 'localMemberHost' parameters. In
  addition also set the 'enable' attribute on 'groupManagement' element
  to 'true'.
- Start Synapse using the configuration numbered 57 (repository/conf/sample/synapse\_sample\_57.xml)

  Unix/Linux: sh synapse.sh -sample 57  
  Windows: synapse.bat -sample 57
- Start 3 instances of the sample Axis2 server as follows.

  ./axis2server.sh -http 9001 -https 9005 -name MyServer1  
  ./axis2server.sh -http 9002 -https 9006 -name MyServer2  
  ./axis2server.sh -http 9003 -https 9007 -name MyServer3

### <a id="synapse-apache-org-userguide-samples-sample57--Executing_the_Client"></a>Executing the Client

Note that the Synapse configuration does not define any concrete addresses or
URLs as targets. They are discovered dynamically by the dynamic load balance
endpoint. To test this feature start the load balance and failover client using
the following command:

ant loadbalancefailover -Di=100

This client sends 100 requests to the LoadbalanceFailoverService through Synapse.
Synapse will distribute the load among the three nodes we have started
in round-robin manner. LoadbalanceFailoverService appends the name of the server
to the response, so that client can determine which server has processed the message.
If you examine the console output of the client, you can see that requests are
processed by three servers as follows:

[java] Request: 1 ==> Response from server: MyServer1
[java] Request: 2 ==> Response from server: MyServer2
[java] Request: 3 ==> Response from server: MyServer3
[java] Request: 4 ==> Response from server: MyServer1
[java] Request: 5 ==> Response from server: MyServer2
[java] Request: 6 ==> Response from server: MyServer3
[java] Request: 7 ==> Response from server: MyServer1
...

Now run the client without the -Di=100 parameter, to send infinite requests. While
running the client shutdown the server named MyServer1. You can observe that
requests are only distributed among MyServer2 and MyServer3 after shutting down
MyServer1. Console output before and after shutting down MyServer1 is listed below
(MyServer1 was shutdown after request 63):

...
[java] Request: 61 ==> Response from server: MyServer1
[java] Request: 62 ==> Response from server: MyServer2
[java] Request: 63 ==> Response from server: MyServer3
[java] Request: 64 ==> Response from server: MyServer2
[java] Request: 65 ==> Response from server: MyServer3
[java] Request: 66 ==> Response from server: MyServer2
[java] Request: 67 ==> Response from server: MyServer3
...

Now restart MyServer1. You can observe that requests will be again sent to all
three servers. If you start a new Axis2 instance (say MyServer4) that will also
be added to the load balance pool dynamically.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample58"></a>

# Apache Synapse – Apache Synapse - Sample 58

## <a id="synapse-apache-org-userguide-samples-sample58--Sample_58:_Static_Load_Balancing_Between_3_Nodes"></a>Sample 58: Static Load Balancing Between 3 Nodes

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main" onError="errorHandler">
<in>
<send>
<endpoint>
<loadbalance failover="true">
<member hostName="127.0.0.1" httpPort="9001" httpsPort="9005"/>
<member hostName="127.0.0.1" httpPort="9002" httpsPort="9006"/>
<member hostName="127.0.0.1" httpPort="9003" httpsPort="9007"/>
</loadbalance>
</endpoint>
</send>
<drop/>
</in>
<out>
<!-- Send the messages where they have been sent (i.e. implicit To EPR) -->
<send/>
</out>
</sequence>
<sequence name="errorHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason value="COULDN'T SEND THE MESSAGE TO THE SERVER."/>
</makefault>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample58--Objective"></a>Objective

Demonstrate the ability of Synapse to act as a load balancer for a set of
servers hosting stateless services. This sample is very similar to
[sample 52](#synapse-apache-org-userguide-samples-sample52) but uses a different syntax style to
configure the load balance endpoint.

### <a id="synapse-apache-org-userguide-samples-sample58--Pre-requisites"></a>Pre-requisites

- Deploy the LoadbalanceFailoverService in the sample Axis2 server (go to
  samples/axis2Server/src/LoadbalanceFailoverService and run 'ant')
- Start 3 instances of the Axis2 server on different ports as follows

  ./axis2server.sh -http 9001 -https 9005 -name MyServer1  
  ./axis2server.sh -http 9002 -https 9006 -name MyServer2  
  ./axis2server.sh -http 9003 -https 9007 -name MyServer3
- Start Synapse using the configuration numbered 52 (repository/conf/sample/synapse\_sample\_52.xml)

  Unix/Linux: sh synapse.sh -sample 52  
  Windows: synapse.bat -sample 52

### <a id="synapse-apache-org-userguide-samples-sample58--Executing_the_Client"></a>Executing the Client

Invoke the sample client as follows

ant loadbalancefailover -Di=100

This will send 100 requests to the LoadbalanceFailoverService through Synapse.
Synapse will distribute the load among the three endpoints mentioned in the
configuration in round-robin manner. LoadbalanceFailoverService appends the name
of the server to the response, so that client can determine which server has
processed the message. If you examine the console output of the client, you can
see that requests are processed by three servers as follows:

[java] Request: 1 ==> Response from server: MyServer1
[java] Request: 2 ==> Response from server: MyServer2
[java] Request: 3 ==> Response from server: MyServer3
[java] Request: 4 ==> Response from server: MyServer1
[java] Request: 5 ==> Response from server: MyServer2
[java] Request: 6 ==> Response from server: MyServer3
[java] Request: 7 ==> Response from server: MyServer1
...

Now run the client without the -Di=100 parameter to send requests indefinitely.
While running the client shutdown the server named MyServer1. Then you can observe
that requests are only distributed among MyServer2 and MyServer3. Console output
before and after shutting down MyServer1 is listed below (MyServer1 was shutdown
after request 63):

...
[java] Request: 61 ==> Response from server: MyServer1
[java] Request: 62 ==> Response from server: MyServer2
[java] Request: 63 ==> Response from server: MyServer3
[java] Request: 64 ==> Response from server: MyServer2
[java] Request: 65 ==> Response from server: MyServer3
[java] Request: 66 ==> Response from server: MyServer2
[java] Request: 67 ==> Response from server: MyServer3
...

Now restart MyServer1. You can observe that requests will be again sent to all
three servers.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample59"></a>

# Apache Synapse – Apache Synapse - Sample 59

## <a id="synapse-apache-org-userguide-samples-sample59--Sample_59:_Weighted_Round-Robin_loadbalancing_between_3_endpoints"></a>Sample 59: Weighted Round-Robin loadbalancing between 3 endpoints

<?xml version="1.0" encoding="UTF-8"?>
<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main" onError="errorHandler">
<in>
<send>
<endpoint>
<loadbalance
algorithm="org.apache.synapse.endpoints.algorithms.WeightedRoundRobin">
<endpoint>
<address uri="http://localhost:9001/services/LBService1">
<enableAddressing/>
<suspendOnFailure>
<initialDuration>20000</initialDuration>
<progressionFactor>1.0</progressionFactor>
</suspendOnFailure>
</address>
<property name="loadbalance.weight" value="1"/>
</endpoint>
<endpoint>
<address uri="http://localhost:9002/services/LBService1">
<enableAddressing/>
<suspendOnFailure>
<initialDuration>20000</initialDuration>
<progressionFactor>1.0</progressionFactor>
</suspendOnFailure>
</address>
<property name="loadbalance.weight" value="2"/>
</endpoint>
<endpoint>
<address uri="http://localhost:9003/services/LBService1">
<enableAddressing/>
<suspendOnFailure>
<initialDuration>20000</initialDuration>
<progressionFactor>1.0</progressionFactor>
</suspendOnFailure>
</address>
<property name="loadbalance.weight" value="3"/>
</endpoint>
</loadbalance>
</endpoint>
</send>
<drop/>
</in>
<out>
<send/>
</out>
</sequence>
<sequence name="errorHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason value="COULDN'T SEND THE MESSAGE TO THE SERVER."/>
</makefault>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample59--Objective"></a>Objective

Objective: Demonstrate the weighted load balancing among a set of
endpoints

### <a id="synapse-apache-org-userguide-samples-sample59--Pre-requisites"></a>Pre-requisites

- Start ESB with sample configuration 59. (i.e. wso2esb-samples -sn 59)
- Deploy the LoadbalanceFailoverService and start three instances of sample Axis2 server as mentioned in sample 52.
- Above configuration sends messages with the weighted loadbalance behaviour. Weight of each leaf
  address endpoint is defined by integer value of "loadbalance.weight" property associated with each endpoint.
  If weight of a endpoint is x, x number of requests will send to that endpoint before switch to next active endpoint.
    
  To test this, run the loadbalancefailover client to send 100 requests as follows:

### <a id="synapse-apache-org-userguide-samples-sample59--Executing_the_Client"></a>Executing the Client

Invoke the sample client as follows

ant loadbalancefailover -Di=100

This client sends 100 requests to the LoadbalanceFailoverService through
ESB. ESB will distribute the load among the three endpoints mentioned in the
configuration in weighted round-robin manner. LoadbalanceFailoverService appends the
name of the server to the response, so that client can determine which server
has processed the message. If you examine the console output of the client,
you can see that requests are processed by three servers as follows:

[java] Request: 1 ==> Response from server: MyServer1
[java] Request: 2 ==> Response from server: MyServer2
[java] Request: 3 ==> Response from server: MyServer2
[java] Request: 4 ==> Response from server: MyServer3
[java] Request: 5 ==> Response from server: MyServer3
[java] Request: 6 ==> Response from server: MyServer3
[java] Request: 7 ==> Response from server: MyServer1
[java] Request: 8 ==> Response from server: MyServer2
[java] Request: 9 ==> Response from server: MyServer2
[java] Request: 10 ==> Response from server: MyServer3
[java] Request: 11 ==> Response from server: MyServer3
[java] Request: 12 ==> Response from server: MyServer3
...

As logs, endpoint with weight 1 received a 1 request and endpoint with weight 2 received 2
requests and etc... in a cycle

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample6"></a>

# Apache Synapse – Apache Synapse - Sample 6

## <a id="synapse-apache-org-userguide-samples-sample6--Sample_6:_Manipulating_SOAP_Headers_and_Filtering_IncomingOutgoing_Messages"></a>Sample 6: Manipulating SOAP Headers and Filtering Incoming/Outgoing Messages

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<header name="To" value="http://localhost:9000/services/SimpleStockQuoteService"/>
</in>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample6--Objective"></a>Objective

Introduction to the header mediator for handling SOAP headers and the in/out
mediators for filtering requests and responses.

### <a id="synapse-apache-org-userguide-samples-sample6--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 6 (repository/conf/sample/synapse\_sample\_6.xml)

  Unix/Linux: sh synapse.sh -sample 6  
  Windows: synapse.bat -sample 6

### <a id="synapse-apache-org-userguide-samples-sample6--Executing_the_Client"></a>Executing the Client

In this sample we wil run the client in dumb client mode (ie without addressing
information) as follows.

ant stockquote -Dtrpurl=http://localhost:8280/

The request is captured by the main sequence of Synapse and handed to the
in mediator. The header mediator sets the WS-Addressing 'To' header on the
request. Finally the send mediator sends the message to the EPR specified in
the addressing 'To' header.

Response coming back from Axis2 will also get dispatched to the main sequence.
But for the response the in mediator will not get executed. Only the send
mediator will operate on responses which will simply send them back to the
client.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample600"></a>

# Apache Synapse – Apache Synapse - Sample 600

## <a id="synapse-apache-org-userguide-samples-sample600--Sample_600:_File_Hierarchy_Based_Configuration_Builder"></a>Sample 600: File Hierarchy Based Configuration Builder

In this sample we will be looking at how Synapse configuration files can be
organized into a single rooted file hierarchy. We will be using the following set
of files and directories.

synapse\_sample\_600.xml
|-- endpoints
| `-- foo.xml
|-- events
| `-- event1.xml
|-- local-entries
| `-- bar.xml
|-- proxy-services
| |-- proxy1.xml
| |-- proxy2.xml
| `-- proxy3.xml
|-- registry.xml
|-- sequences
| |-- custom-logger.xml
| |-- fault.xml
| `-- main.xml
|-- synapse.xml
`-- tasks
`-- task1.xml

### <a id="synapse-apache-org-userguide-samples-sample600--Objective"></a>Objective

Demonstrate the ability to load the Synapse configuration from a file hierarchy

### <a id="synapse-apache-org-userguide-samples-sample600--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 600 (this is available
  in the directory at repository/conf/sample/synapse\_sample\_600.xml)

  Unix/Linux: sh synapse.sh -sample 600  
  Windows: synapse.bat -sample 600

### <a id="synapse-apache-org-userguide-samples-sample600--Description"></a>Description

Go to the SYNAPSE\_HOME/repository/conf/sample directory and locate the subdirectory
named synapse\_sample\_600.xml within it. When Synapse is started with the sample
configuration 600, Synapse will load the configuration from this directory. You
will find a number of subdirectories and a set of XML files in each of those
directories. Synapse will parse all the XML files in this file hierarchy and
construct the full Synapse configuration at startup. As a result when this sample
is executed Synapse will start with four proxy services, several sequences, a task,
an event source and some endpoint and local entry definitions.

The names of the subdirectories (eg: proxy-services, sequences, endpoints) are
fixed and hence cannot be changed. Also the registry definition should go into a
file named registry.xml which resides at the top level of the file hierarchy. It
can also be specified in the synapse.xml file at top level. This synapse.xml file
can include any item that can be normally defined in a synapse.xml file. The files
which define proxy services, sequences, endpoints etc can have any name. These
configuration files must have the .xml extension at the end of the name. Synapse
will ignore any files which do not have the .xml extension.

None of the directories and files in the sample file hierachy are mandatory. You
can leave entire directories out if you do not need them. For example if your
configuration does not contain any proxy services you can leave the
subdirectory named proxy-services out.

To use this feature you should simply pass a path to an existing directory when
starting the Synapse server. The SynapseServer class which is responsible for
starting the server accepts a file path as an argument from where to
load the configuration. Generally we pass the path to the synapse.xml file as the
value of this argument. If you pass a directory path instead, Synapse configuration
will be loaded from the specified directory. Note the following line on the console
when Synapse is loading the configuration from a file hierarchy.

2009-08-04 14:14:42,489 [-] [main] INFO SynapseConfigurationBuilder Loaded Synapse configuration from the directory hierarchy at : /home/synapse/repository/conf/sample/synapse\_sample\_600.xml

This feature comes in handy when managing large Synapse configurations. It is
easier to maintain a well structured file hierarchy than managing one large, flat
XML file.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample601"></a>

# Apache Synapse – Apache Synapse - Sample 601

## <a id="synapse-apache-org-userguide-samples-sample601--Sample_601:_Using_Synapse_Observers"></a>Sample 601: Using Synapse Observers

### <a id="synapse-apache-org-userguide-samples-sample601--Objective"></a>Objective

Demonstrate the ability to monitor the Synapse configuration at runtime using the
SynapseObserver interface

### <a id="synapse-apache-org-userguide-samples-sample601--Running_the_Sample"></a>Running the Sample

Open the synapse.properties file in the SYNAPSE\_HOME/repository/conf directory
using a text editor and uncomment the line which defines the simple logging
Synapse observer.

synapse.observers=samples.userguide.SimpleLoggingObserver

Open the log4j.properties file in the SYNAPSE\_HOME/lib directory and
uncomment the line which sets the INFO log level to the samples.userguide
package.

log4j.category.samples.userguide=INFO

Start Synapse using any of the sample configurations. The SimpleLoggingObserver
will capture events that occur while constructing the Synapse configuration
and log them on the console as follows.

2009-08-06 14:30:24,578 [-] [main] INFO SimpleLoggingObserver Simple logging observer initialized...Capturing Synapse events...
2009-08-06 14:30:24,604 [-] [main] INFO SimpleLoggingObserver Endpoint : a3 was added to the Synapse configuration successfully
2009-08-06 14:30:24,605 [-] [main] INFO SimpleLoggingObserver Endpoint : a2 was added to the Synapse configuration successfully
2009-08-06 14:30:24,606 [-] [main] INFO SimpleLoggingObserver Endpoint : null was added to the Synapse configuration successfully
2009-08-06 14:30:24,611 [-] [main] INFO SimpleLoggingObserver Local entry : a1 was added to the Synapse configuration successfully
2009-08-06 14:30:24,649 [-] [main] INFO SimpleLoggingObserver Proxy service : StockQuoteProxy2 was added to the Synapse configuration successfully
2009-08-06 14:30:24,661 [-] [main] INFO SimpleLoggingObserver Proxy service : StockQuoteProxy1 was added to the Synapse configuration successfully
2009-08-06 14:30:24,664 [-] [main] INFO SimpleLoggingObserver Sequence : main was added to the Synapse configuration successfully
2009-08-06 14:30:24,701 [-] [main] INFO SimpleLoggingObserver Sequence : fault was added to the Synapse configuration successfully

The SimpleLoggingObserver is implemented as follows. It does not override any of the event handler implementations
in the AbstractSynapseObserver class. The AbstractSynapseObserver logs all the received events by default.

package samples.userguide;
import org.apache.synapse.config.AbstractSynapseObserver;
public class SimpleLoggingObserver extends AbstractSynapseObserver {
public SimpleLoggingObserver() {
super();
log.info("Simple logging observer initialized...Capturing Synapse events...");
}
}

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample61"></a>

# Apache Synapse – Apache Synapse - Sample 61

## <a id="synapse-apache-org-userguide-samples-sample61--Sample_61:_Routing_message_to_3_static_recipients"></a>Sample 61: Routing message to 3 static recipients

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="errorHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver" />
<reason value="COULDN'T SEND THE MESSAGE TO THE SERVER." />
</makefault>
<send />
</sequence>
<sequence name="fault">
<log level="full">
<property name="MESSAGE" value="Executing default &quot;fault&quot; sequence" />
<property name="ERROR\_CODE" expression="get-property('ERROR\_CODE')" />
<property name="ERROR\_MESSAGE" expression="get-property('ERROR\_MESSAGE')" />
</log>
<drop />
</sequence>
<sequence name="main" onError="errorHandler">
<in>
<property name="EP\_LIST" value="http://localhost:9001/services/SimpleStockQuoteService,http://localhost:9002/services/SimpleStockQuoteService,http://localhost:9003/services/SimpleStockQuoteService"/>
<property name="OUT\_ONLY" value="true" />
<property name="FORCE\_SC\_ACCEPTED" value="true" scope="axis2" />
<send>
<endpoint>
<recipientlist>
<endpoints value="{get-property('EP\_LIST')}" max-cache="20" />
</recipientlist>
</endpoint>
</send>
<drop/>
</in>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample61--Objective"></a>Objective

Objective: Routing message to 3 static recipients

### <a id="synapse-apache-org-userguide-samples-sample61--Pre-requisites"></a>Pre-requisites

- Start ESB with sample configuration 61. (i.e. wso2esb-samples -sn 61)
- Start three instances of the sample Axis2 server on HTTP ports 9001, 9002 and 9003 and give unique names to each server. For instructions on starting the Axis2 server, see Starting the Axis2 server

### <a id="synapse-apache-org-userguide-samples-sample61--Executing_the_Client"></a>Executing the Client

Invoke the sample client as follows

ant stockquote -Dmode=placeorder -Dtrpurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample62"></a>

# Apache Synapse – Apache Synapse - Sample 62

## <a id="synapse-apache-org-userguide-samples-sample62--Sample_62:_Routing_message_to_dynamic_recipients"></a>Sample 62: Routing message to dynamic recipients

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="errorHandler">
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver" />
<reason value="COULDN'T SEND THE MESSAGE TO THE SERVER." />
</makefault>
<send />
</sequence>
<sequence name="fault">
<log level="full">
<property name="MESSAGE" value="Executing default &quot;fault&quot; sequence" />
<property name="ERROR\_CODE" expression="get-property('ERROR\_CODE')" />
<property name="ERROR\_MESSAGE" expression="get-property('ERROR\_MESSAGE')" />
</log>
<drop />
</sequence>
<sequence name="main" onError="errorHandler">
<in>
<property name="EP\_LIST" value="http://localhost:9001/services/SimpleStockQuoteService,http://localhost:9002/services/SimpleStockQuoteService,http://localhost:9003/services/SimpleStockQuoteService"/>
<send>
<endpoint>
<recipientlist>
<endpoints value="{get-property('EP\_LIST')}" max-cache="20" />
</recipientlist>
</endpoint>
</send>
<drop/>
</in>
<out>
<!--Aggregate responses-->
<aggregate>
<onComplete xmlns:m0="http://services.samples"
expression="//m0:getQuoteResponse">
<log level="full"/>
<send/>
</onComplete>
</aggregate>
</out>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample62--Objective"></a>Objective

Objective: Routing message to dynamic recipients

### <a id="synapse-apache-org-userguide-samples-sample62--Pre-requisites"></a>Pre-requisites

- Start ESB with sample configuration 62. (i.e. wso2esb-samples -sn 62)
- Start three instances of the sample Axis2 server on HTTP ports 9001, 9002 and 9003 and give unique names to each server. For instructions on starting the Axis2 server, see Starting the Axis2 server

### <a id="synapse-apache-org-userguide-samples-sample62--Executing_the_Client"></a>Executing the Client

Invoke the sample client as follows

ant stockquote -Dtrpurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample650"></a>

# Apache Synapse – Apache Synapse - Sample 650

## <a id="synapse-apache-org-userguide-samples-sample650--Sample_650:_Introduction_to_Priority_Based_Mediation"></a>Sample 650: Introduction to Priority Based Mediation

<definitions xmlns="http://ws.apache.org/ns/synapse">
<priorityExecutor name="exec">
<queues>
<queue size="100" priority="1"/>
<queue size="100" priority="10"/>
</queues>
</priorityExecutor>
<proxy name="StockQuoteProxy">
<target>
<inSequence>
<filter source="$trp:priority" regex="1">
<then>
<enqueue priority="1" sequence="priority\_sequence" executor="exec"/>
</then>
<else>
<enqueue priority="10" sequence="priority\_sequence" executor="exec"/>
</else>
</filter>
</inSequence>
<outSequence>
<send/>
</outSequence>
</target>
<publishWSDL uri="file:repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl"/>
</proxy>
<sequence name="priority\_sequence">
<log level="full"/>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample650--Objective"></a>Objective

Demonstrate the usage of priority executors in Synapse to assign priority levels
to requests and mediate them based on the assigned priority

### <a id="synapse-apache-org-userguide-samples-sample650--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 650 (repository/conf/sample/synapse\_sample\_650.xml)

  Unix/Linux: sh synapse.sh -sample 650  
  Windows: synapse.bat -sample 650

### <a id="synapse-apache-org-userguide-samples-sample650--Executing_the_Client"></a>Executing the Client

Priority is applied only when synapse is loaded with enough messages to consume
all of its core worker threads. So to observe the priority based mediation, it is
required to use a load testing tool like JMeter, SOAP UI or Apache bench.

In this sample, client should send a HTTP header that specifies the priority of
the message.This header name is 'priority'. This header is retrieved in the synapse
configuration using the $trp:priority XPath expression. Then it is matched against
the value 1. If it has the value 1, message is executed with priority 1. Otherwise
the message is executed with priority 10.

Messages with different priorities are put into different priority queues. Then they
are mediated in a manner so that high priority messages are always processed first.

Here are two sample SOAP requests that can be used to invoke the service using a
tool like JMeter, or Apache Bench. For SOAP UI, user can use the WSDL
repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl to create the request.
The only difference between the two requests shown here is the symbol. One
has the symbol as IBM and other has MSFT. For one type of requests set the priority
header to 1 and for the next set the priority header to 10. Then load Synapse with
a large volume of traffic consisting of both types of requests using the load testing tool.
Back end Axis2 server prints the symbol of the incoming requests. User should be
able to see more of the high priority symbol.

<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Header xmlns:wsa="http://www.w3.org/2005/08/addressing">
<wsa:To>http://localhost:8281/services/SimpleStockQuoteService</wsa:To>
<wsa:MessageID>urn:uuid:1B57D0B0BF770678DE1261165228620</wsa:MessageID>
<wsa:Action>urn:getQuote</wsa:Action>
</soapenv:Header>
<soapenv:Body>
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>IBM</m0:symbol>
</m0:request>
</m0:getQuote>
</soapenv:Body>
</soapenv:Envelope>

<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Header xmlns:wsa="http://www.w3.org/2005/08/addressing">
<wsa:To>http://localhost:8281/services/SimpleStockQuoteService</wsa:To>
<wsa:MessageID>urn:uuid:1B57D0B0BF770678DE1261165228620</wsa:MessageID>
<wsa:Action>urn:getQuote</wsa:Action>
</soapenv:Header>
<soapenv:Body>
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>MSFT</m0:symbol>
</m0:request>
</m0:getQuote>
</soapenv:Body>
</soapenv:Envelope>

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample651"></a>

# Apache Synapse – Apache Synapse - Sample 651

## <a id="synapse-apache-org-userguide-samples-sample651--Sample_651:_Priority_Based_Dispatching_at_Transport_Level"></a>Sample 651: Priority Based Dispatching at Transport Level

For this sample we will be using the same Synapse configuration used in
[sample 150](#synapse-apache-org-userguide-samples-sample150). In addition we will be using the following
priority configuration for the Synapse NHTTP transport.

<priorityConfiguration>
<priorityExecutor>
<!-- two priorities specified with priority 10 and 1. Both priority messages has a queue depth of 100 -->
<queues isFixedCapacity="true" nextQueue="org.apache.synapse.commons.executors.PRRNextQueueAlgorithm">
<queue size="100" priority="10"/>
<queue size="100" priority="1"/>
</queues>
<!-- these are the default values, values are put here to show their availability -->
<threads core="20" max="100" keep-alive="5"/>
</priorityExecutor>
<!-- if a message comes that we cannot determine priority, we set a default priority of 1 -->
<conditions defaultPriority="1">
<condition priority="10">
<!-- check for the header named priority -->
<equal type="header" source="priority" value="5"/>
</condition>
<condition priority="1">
<equal type="header" source="priority" value="1"/>
</condition>
</conditions>
</priorityConfiguration>

### <a id="synapse-apache-org-userguide-samples-sample651--Objective"></a>Objective

Demonstrate priority based dispatching capabilities of the Synapse NHTTP
transport

### <a id="synapse-apache-org-userguide-samples-sample651--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Open axis2.xml file in repository/conf directory and uncomment the
  following parameter in the NHTTP transport receiver configuration.
    
  **priorityConfigFile**
    
  Set the value to repository/conf/sample/resources/priority/priority-configuration.xml
- Start Synapse using the configuration numbered 150 (repository/conf/sample/synapse\_sample\_150.xml)

  Unix/Linux: sh synapse.sh -sample 150  
  Windows: synapse.bat -sample 150

### <a id="synapse-apache-org-userguide-samples-sample651--Executing_the_Client"></a>Executing the Client

Priority is applied only when synapse is loaded with enough messages to consume
all of its core worker threads. So to observe the priority based mediation, it is
required to use a load testing tool like JMeter, SOAP UI or Apache bench.

In this sample, client should send a HTTP header that specifies the priority of
the message.This header name is 'priority'. This header is retrieved in the synapse
configuration using the $trp:priority XPath expression. Then it is matched against
the value 1. If it has the value 1, message is executed with priority 1. Otherwise
the message is executed with priority 10.

Messages with different priorities are put into different priority queues. Then they
are mediated in a manner so that high priority messages are always processed first.

Here are two sample SOAP requests that can be used to invoke the service using a
tool like JMeter, or Apache Bench. For SOAP UI, user can use the WSDL
repository/conf/sample/resources/proxy/sample\_proxy\_1.wsdl to create the request.
The only difference between the two requests shown here is the symbol. One
has the symbol as IBM and other has MSFT. For one type of requests set the priority
header to 1 and for the next set the priority header to 10. Then load Synapse with
a large volume of traffic consisting of both types of requests using the load testing tool.
Back end Axis2 server prints the symbol of the incoming requests. User should be
able to see more of the high priority symbol.

<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Header xmlns:wsa="http://www.w3.org/2005/08/addressing">
<wsa:To>http://localhost:8281/services/SimpleStockQuoteService</wsa:To>
<wsa:MessageID>urn:uuid:1B57D0B0BF770678DE1261165228620</wsa:MessageID>
<wsa:Action>urn:getQuote</wsa:Action>
</soapenv:Header>
<soapenv:Body>
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>IBM</m0:symbol>
</m0:request>
</m0:getQuote>
</soapenv:Body>
</soapenv:Envelope>

<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
<soapenv:Header xmlns:wsa="http://www.w3.org/2005/08/addressing">
<wsa:To>http://localhost:8281/services/SimpleStockQuoteService</wsa:To>
<wsa:MessageID>urn:uuid:1B57D0B0BF770678DE1261165228620</wsa:MessageID>
<wsa:Action>urn:getQuote</wsa:Action>
</soapenv:Header>
<soapenv:Body>
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>MSFT</m0:symbol>
</m0:request>
</m0:getQuote>
</soapenv:Body>
</soapenv:Envelope>

In this sample, priority based mediation takes place at the transport level
itself (before the message is even received by the mediation engine). High
priority messages will reach the service bus first.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample7"></a>

# Apache Synapse – Apache Synapse - Sample 7

## <a id="synapse-apache-org-userguide-samples-sample7--Sample_7:_Introduction_to_Local_Registry_Entries_and_Using_Schema_Validation"></a>Sample 7: Introduction to Local Registry Entries and Using Schema Validation

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<in>
<validate>
<schema key="validate\_schema"/>
<on-fail>
<!-- if the request does not validate againt schema throw a fault -->
<makefault response="true">
<code xmlns:tns="http://www.w3.org/2003/05/soap-envelope" value="tns:Receiver"/>
<reason value="Invalid custom quote request"/>
</makefault>
</on-fail>
</validate>
</in>
<send/>
</sequence>
<localEntry key="validate\_schema">
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema"
xmlns="http://services.samples" elementFormDefault="qualified"
attributeFormDefault="unqualified" targetNamespace="http://services.samples">
<xs:element name="getQuote">
<xs:complexType>
<xs:sequence>
<xs:element name="request">
<xs:complexType>
<xs:sequence>
<xs:element name="stocksymbol" type="xs:string"/>
</xs:sequence>
</xs:complexType>
</xs:element>
</xs:sequence>
</xs:complexType>
</xs:element>
</xs:schema>
</localEntry>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample7--Objective"></a>Objective

Demonstrating the usage of the validate mediator for XML schema validation
and using local registry (local entries) for storing configuration metadata.

### <a id="synapse-apache-org-userguide-samples-sample7--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 7 (repository/conf/sample/synapse\_sample\_7.xml)

  Unix/Linux: sh synapse.sh -sample 7  
  Windows: synapse.bat -sample 7

### <a id="synapse-apache-org-userguide-samples-sample7--Executing_the_Client"></a>Executing the Client

This example shows how a static XML fragment could be stored in the the
Synapse local registry. Resources defined in the local registry are static
(i.e. never changes over the lifetime of the configuration) and may be
specified as a source URL, in-line text or in-line xml. In this example the
schema is made available under the key 'validate\_schema'.

The validate mediator by default operates on the first child element of the
SOAP body. You may specify an XPath expression using the 'source' attribute
to override this behaviour. The validate mediator in this sample uses the 'validate\_schema'
resource to validate the incoming message, and if the message validation fails
it invokes the 'on-fail' sequence of mediators.

If you send a stockquote request using the 'ant stockquote ...' command as follows
you will get a fault back with the message 'Invalid custom quote request' as
the schema validation fails. This is because the schema used in the example
expects a slightly different message than what is created by the stock quote
client. (i.e. expects a 'stocksymbol' element instead of 'symbol' to specify
the stock symbol)

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample700"></a>

# Apache Synapse – Apache Synapse - Sample 700

## <a id="synapse-apache-org-userguide-samples-sample700--Sample_700:_Introduction_to_Synapse_Message_Stores"></a>Sample 700: Introduction to Synapse Message Stores

<!-- Introduction to the Message Store -->
<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="fault">
<log level="full">
<property name="MESSAGE" value="Executing default 'fault' sequence"/>
<property name="ERROR\_CODE" expression="get-property('ERROR\_CODE')"/>
<property name="ERROR\_MESSAGE"
expression="get-property('ERROR\_MESSAGE')"/>
</log>
<drop/>
</sequence>
<sequence name="onStoreSequence">
<log>
<property name="On-Store" value="Storing message"/>
</log>
</sequence>
<sequence name="main">
<in>
<log level="full"/>
<property name="FORCE\_SC\_ACCEPTED" value="true" scope="axis2"/>
<store messageStore="MyStore" sequence="onStoreSequence"/>
</in>
<description>The main sequence for the message mediation</description>
</sequence>
<messageStore name="MyStore"/>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample700--Objective"></a>Objective

Introduction to Message Stores

### <a id="synapse-apache-org-userguide-samples-sample700--Pre-requisites"></a>Pre-requisites

- Start Synapse using the configuration numbered 700 (repository/conf/sample/synapse\_sample\_700.xml)

  Unix/Linux: sh synapse.sh -sample 700  
  Windows: synapse.bat -sample 700

### <a id="synapse-apache-org-userguide-samples-sample700--Executing_the_Client"></a>Executing the Client

First execute the sample client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=placeorder

When you execute the client the message will be dispatched to the main sequence.
In the Main sequence store mediator will store the placeOrder request message in
the 'MyStore' Message Store.

Now you can use the JMX view of the Synapse message store to see the messages
stored in the dead letter channel and manually perform retries on them.

Before storing the message, store mediator will invoke the sequence named
'onStoreSequence'. You should see something similar to the following in the
log.

INFO - LogMediator To: http://localhost:9000/services/SimpleStockQuoteService,
WSAction: urn:placeOrder, SOAPAction: urn:placeOrder, ReplyTo:
http://www.w3.org/2005/08/addressing/none, MessageID:
urn:uuid:54f0e7c6-7b43-437c-837e-a825d819688c, Direction: request, On-Store =
Storing message

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample701"></a>

# Apache Synapse – Apache Synapse - Sample 701

## <a id="synapse-apache-org-userguide-samples-sample701--Sample_701:_Introduction_to_Message_Sampling_Processor"></a>Sample 701: Introduction to Message Sampling Processor

<!-- Introduction to Message Sampling Processor -->
<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="send\_seq">
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService">
<suspendOnFailure>
<errorCodes>-1</errorCodes>
<progressionFactor>1.0</progressionFactor>
</suspendOnFailure>
</address>
</endpoint>
</send>
</sequence>
<sequence name="main">
<in>
<log level="full"/>
<property name="FORCE\_SC\_ACCEPTED" value="true" scope="axis2"/>
<property name="OUT\_ONLY" value="true"/>
<store messageStore="MyStore"/>
</in>
<description>The main sequence for the message mediation</description>
</sequence>
<messageStore name="MyStore"/>
<messageProcessor
class="org.apache.synapse.message.processors.sampler.SamplingProcessor"
name="SamplingProcessor" messageStore="MyStore">
<parameter name="interval">20000</parameter>
<parameter name="sequence">send\_seq</parameter>
</messageProcessor>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample701--Objective"></a>Objective

Introduction to Message Sampling Processor

### <a id="synapse-apache-org-userguide-samples-sample701--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 701 (repository/conf/sample/synapse\_sample\_701.xml)

  Unix/Linux: sh synapse.sh -sample 701  
  Windows: synapse.bat -sample 701

### <a id="synapse-apache-org-userguide-samples-sample701--Executing_the_Client"></a>Executing the Client

Execute the Client few times with command :

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=placeorder

When you execute the client the message will be dispatched to the main sequence.
In the Main sequence store mediator will store the placeOrder request message in
the 'MyStore' message store.

Message Processor will consume the messages and forward to the 'send\_seq' sequence
in the configured rate.

You will observe that service invocation rate is not changing when increasing the
rate at which we execute the client.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample702"></a>

# Apache Synapse – Apache Synapse - Sample 702

## <a id="synapse-apache-org-userguide-samples-sample702--Sample_702:_Introduction_to_Message_Forwarding_Processor"></a>Sample 702: Introduction to Message Forwarding Processor

<!-- Introduction to Scheduled Message Forwarding Processor -->
<definitions xmlns="http://ws.apache.org/ns/synapse">
<endpoint name="StockQuoteServiceEp">
<address uri="http://localhost:9000/services/SimpleStockQuoteService">
<suspendOnFailure>
<errorCodes>-1</errorCodes>
<progressionFactor>1.0</progressionFactor>
</suspendOnFailure>
</address>
</endpoint>
<sequence name="fault">
<log level="full">
<property name="MESSAGE" value="Executing default 'fault' sequence"/>
<property name="ERROR\_CODE" expression="get-property('ERROR\_CODE')"/>
<property name="ERROR\_MESSAGE" expression="get-property('ERROR\_MESSAGE')"/>
</log>
<drop/>
</sequence>
<sequence name="main">
<in>
<log level="full"/>
<property name="FORCE\_SC\_ACCEPTED" value="true" scope="axis2"/>
<property name="OUT\_ONLY" value="true"/>
<property name="target.endpoint" value="StockQuoteServiceEp"/>
<store messageStore="MyStore"/>
</in>
<description>The main sequence for the message mediation</description>
</sequence>
<messageStore name="MyStore"/>
<messageProcessor
class="org.apache.synapse.message.processors.forward.ScheduledMessageForwardingProcessor"
name="ScheduledProcessor" messageStore="MyStore">
<parameter name="interval">10000</parameter>
</messageProcessor>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample702--Objective"></a>Objective

Introduction to Message Forwarding Processor

### <a id="synapse-apache-org-userguide-samples-sample702--Pre-requisites"></a>Pre-requisites

- Start Synapse using the configuration numbered 702 (repository/conf/sample/synapse\_sample\_702.xml)

  Unix/Linux: sh synapse.sh -sample 702  
  Windows: synapse.bat -sample 702

### <a id="synapse-apache-org-userguide-samples-sample702--Executing_the_Client"></a>Executing the Client

Execute the sample client a few times with the following command. Note that
we still haven't started the sample Axis2 server.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=placeorder

Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2.

When you start the service you will see messages getting delivered to the service,
even though the service was actually down when we invoked the sample client.

Here in the 'main' sequence store mediator will store the placeOrder request
message in the 'MyStore' message store. Message processor will send the message
to the endpoint which is configured as a message context property. Message
processor will remove the message from the store only if the message is delivered
successfully.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample703"></a>

# Apache Synapse – Apache Synapse - Sample 703

## <a id="synapse-apache-org-userguide-samples-sample703--Sample_703:_Introduction_to_Message_Resequencing_Processor"></a>Sample 703: Introduction to Message Resequencing Processor

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="next\_seq">
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService">
<suspendOnFailure>
<errorCodes>-1</errorCodes>
<progressionFactor>1.0</progressionFactor>
</suspendOnFailure>
</address>
</endpoint>
</send>
</sequence>
<sequence name="main">
<in>
<log level="full"/>
<property name="FORCE\_SC\_ACCEPTED" value="true" scope="axis2"/>
<property name="OUT\_ONLY" value="true"/>
<store messageStore="MyStore"/>
</in>
<out>
<send />
</out>
<description>The main sequence for the message mediation</description>
</sequence>
<messageStore name="MyStore"/>
<messageProcessor
class="org.apache.synapse.message.processors.resequence.ResequencingProcessor"
name="ResequencingProcessor" messageStore="MyStore">
<parameter name="interval">10000</parameter>
<parameter name="seqNumXpath" xmlns:m0="http://services.samples" expression="substring-after(//m0:placeOrder/m0:order/m0:symbol,'-')"/>
<parameter name="nextEsbSequence">next\_seq</parameter>
</messageProcessor>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample703--Objective"></a>Objective

Introduction to Message Resequencing Processor

### <a id="synapse-apache-org-userguide-samples-sample703--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 703 (repository/conf/sample/synapse\_sample\_703.xml)

  Unix/Linux: sh synapse.sh -sample 703  
  Windows: synapse.bat -sample 703

### <a id="synapse-apache-org-userguide-samples-sample703--Executing_the_Client"></a>Executing the Client

Execute the Client with commands :

ant stockquote -Dtrpurl=http://localhost:8280/ -Dmode=placeorder -Dsymbol=WSO2-2  
ant stockquote -Dtrpurl=http://localhost:8280/ -Dmode=placeorder -Dsymbol=WSO2-3  
ant stockquote -Dtrpurl=http://localhost:8280/ -Dmode=placeorder -Dsymbol=WSO2-1

You have to use different sequence numbers for WSO2-#. According to configuration
sequence number should be seperated with a hyphen mark.

When you execute the client the message will be dispatched to the main sequence.
In the Main sequence store mediator will store the placeOrder request message in
the 'MyStore' message store.

Message Processor will consume the messages and forward to the 'next\_seq' sequence
according to sequence number order.

You will see that the Axis2 server has recieved the messages in sequence number order

Following logic is used to decide the initial sequence number.

1. When starting Synapse Resequencing Processor checks the attached message store for any messages.
   If any messages found, select the minimum sequence number as the initial sequence number. Else continue without selecting initial sequence number.
2. If initial sequence number is selected at the start up, continue sending messages to the given sequence.
   Otherwise waits for required number of messages to come with in a certain timeout.
3. If the required number of messages are received, select initial sequence number from those and do further resequencing.
   If required number of messages are not received with in timeout, select the initial sequence number from available messages in the store.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample704"></a>

# Apache Synapse – Apache Synapse - Sample 704

## <a id="synapse-apache-org-userguide-samples-sample704--Sample_704:_Invoke_Secured_Services_with_Scheduled_Message_Forwarding_Processor"></a>Sample 704: Invoke Secured Services with Scheduled Message Forwarding Processor

<!-- Invoke Secured Services with Scheduled Message Forwarding Processor -->
<definitions xmlns="http://ws.apache.org/ns/synapse">
<localEntry key="sec\_policy" src="file:repository/conf/sample/resources/policy/policy\_3.xml"/>
<endpoint name="SecuredStockQuoteServiceEp">
<address uri="http://localhost:9000/services/SecureStockQuoteService">
<suspendOnFailure>
<errorCodes>-1</errorCodes>
<progressionFactor>1.0</progressionFactor>
</suspendOnFailure>
<enableSec policy="sec\_policy"/>
</address>
</endpoint>
<sequence name="fault">
<log level="full">
<property name="MESSAGE" value="Executing default 'fault' sequence"/>
<property name="ERROR\_CODE" expression="get-property('ERROR\_CODE')"/>
<property name="ERROR\_MESSAGE" expression="get-property('ERROR\_MESSAGE')"/>
</log>
<drop/>
</sequence>
<sequence name="main">
<in>
<log level="full"/>
<property name="FORCE\_SC\_ACCEPTED" value="true" scope="axis2"/>
<property name="OUT\_ONLY" value="true"/>
<property name="target.endpoint" value="SecuredStockQuoteServiceEp"/>
<store messageStore="MyStore"/>
</in>
<description>The main sequence for the message mediation</description>
</sequence>
<messageStore name="MyStore"/>
<messageProcessor class="org.apache.synapse.message.processors.forward.ScheduledMessageForwardingProcessor" name="ScheduledProcessor" messageStore="MyStore">
<parameter name="interval">10000</parameter>
</messageProcessor>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample704--Objective"></a>Objective

Invoke Secured Services with Scheduled Message Forwarding Processor

### <a id="synapse-apache-org-userguide-samples-sample704--Pre-requisites"></a>Pre-requisites

- Download and install the Java Cryptography Extension (JCE) unlimited
  strength policy files for your JDK
- Start Synapse using the configuration numbered 704 (repository/conf/sample/synapse\_sample\_704.xml)

  Unix/Linux: sh synapse.sh -sample 704  
  Windows: synapse.bat -sample 704

### <a id="synapse-apache-org-userguide-samples-sample704--Executing_the_Client"></a>Executing the Client

Execute the sample client a few times with the following command. Note that
we still haven't started the sample Axis2 server.

ant stockquote -Daddurl=http://localhost:8280/ -Dmode=placeorder

Deploy the SecureStockQuoteService in the sample Axis2 server and start Axis2.

When you start the service you will see messages getting delivered to the service,
even though the service was actually down when we invoked the sample client.

Here in the 'main' sequence, store mediator will store the placeOrder request
message in the 'MyStore' message store.
Message processor will send the message to the secured backend service using the defined endpoint.
Endpoint is configured to use WS-Security.
Message processor will remove the message from the store only if the message is delivered
successfully.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample705"></a>

# Apache Synapse – Apache Synapse - Sample 705

## <a id="synapse-apache-org-userguide-samples-sample705--Sample_705:_Introduction_to_Message_Forwarding_Processor_With_Advance_Parameters"></a>Sample 705: Introduction to Message Forwarding Processor With Advance Parameters

<!-- Introduction to Message Forwarding Processor With max deliver attempt and drop
<definitions xmlns="http://ws.apache.org/ns/synapse">
<endpoint name="StockQuoteServiceEp">
<address uri="http://localhost:9000/services/SimpleStockQuoteService">
<suspendOnFailure>
<errorCodes>-1</errorCodes>
<progressionFactor>1.0</progressionFactor>
</suspendOnFailure>
</address>
</endpoint>
<sequence name="fault">
<log level="full">
<property name="MESSAGE" value="Executing default 'fault' sequence" />
<property name="ERROR\_CODE" expression="get-property('ERROR\_CODE')" />
<property name="ERROR\_MESSAGE" expression="get-property('ERROR\_MESSAGE')" />
</log>
<drop />
</sequence>
<sequence name="main">
<in>
<log level="full" />
<property name="FORCE\_SC\_ACCEPTED" value="true" scope="axis2" />
<property name="OUT\_ONLY" value="true" />
<property name="target.endpoint" value="StockQuoteServiceEp" />
<store messageStore="MyStore" />
</in>
<description>The main sequence for the message mediation</description>
</sequence>
<messageStore name="MyStore" />
<messageProcessor class="org.apache.synapse.message.processors.forward.ScheduledMessageForwardingProcessor" name="ScheduledProcessor" messageStore="MyStore">
<parameter name="interval">10000</parameter>
<parameter name="max.deliver.attempts">3</parameter>
<parameter name="max.deliver.drop">true</parameter>
<parameter name="retry.http.status.codes">500, 504</parameter>
<parameter name="retry.interval">1000</parameter>
<parameter name="consume.all">false</parameter>
</messageProcessor>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample705--Objective"></a>Objective

Introduction to Synapse Scheduled Message Forwarding Processor with following advance parameters

- max.deliver.attempts
- max.deliver.drop
- retry.http.status.codes
- retry.interval

### <a id="synapse-apache-org-userguide-samples-sample705--Pre-requisites"></a>Pre-requisites

- Start Synapse using the configuration numbered 705 (repository/conf/sample/synapse\_sample\_705.xml)

  Unix/Linux: sh synapse.sh -sample 705  
  Windows: synapse.bat -sample 705

### <a id="synapse-apache-org-userguide-samples-sample705--Executing_the_Client"></a>Executing the Client

Execute the sample client a few times with the following command.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=placeorder

When you start to send request to synapse from client, you will see message forwarding processor without
getting deactivate it keep on processing. This is due to the message will be dropped from the message store after
the maximum number of delivery attempts are made, and the message processor will remain activated.
"max.deliver.drop" parameter would have no effect when no value is specified for the Maximum Delivery Attempts parameter.
If this parameter is disabled, the undeliverable message will not be dropped and the message processor will be deactivated.

Message Forwarding Processor by default do not retry for application level failures. It only retries by default when
there is a network level failure. But if the user wants retry based on the application level failures, user can use
"retry.http.status.codes" configuration to do so. Please note that in this context application level failures refers to
HTTP error responses. For instance, in the above example Message Forwarding Processor retries not only for transport
level failures but also for application level failures such as Internal Server Error (500) and Gateway timeout (504).

Message Forwarding Processor sends messages to the back-end with the interval configured using "interval" parameter.
However, when there is a failure, Message Forwarding Processor goes to retry mode. When retrying,
Message Forwarding Processor sends the message to the back-end with the interval configured using "retry.interval"
parameter. Similar to "interval" "retry.interval" value must be set in milli seconds. In the case of setting
a non-integer value, makes Message Forwarding Processor set the default value to "retry.interval", which is 1000ms.

Every time the Message Forwarding Processor is triggered it consumes all the messages in the Message Store at once. For instance,
suppose, the Message Forwarding Processor is configured to run every ten seconds and the Message Store is filled
with five messages within the ten second gap. In such a situation, Message Forwarding Processor consumes all five messages
and try to send it to back-end as fast as possible. However, there could be situations where you need to send messages
to the back-end in a controlled rate. This can be achieved by setting the "consume.all" property value to false. When set to false,
the Message Forwarding Processor will only consume one message at each trigger.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample750"></a>

# Apache Synapse – Apache Synapse - Sample 750

## <a id="synapse-apache-org-userguide-samples-sample750--Sample_750:_Stereotyping_XSLT_Transformations_with_Templates"></a>Sample 750: Stereotyping XSLT Transformations with Templates

<definitions xmlns="http://ws.apache.org/ns/synapse">
<proxy name="StockQuoteProxy">
<target>
<inSequence>
<!--use sequence template to trasnform incoming request-->
<call-template target="xslt\_func">
<with-param name="xslt\_key" value="xslt-key-req"/>
</call-template>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</inSequence>
<outSequence>
<!--use sequence template to trasnform incoming response-->
<call-template target="xslt\_func">
<with-param name="xslt\_key" value="xslt-key-back"/>
</call-template>
<send/>
</outSequence>
</target>
</proxy>
<!--this sequence template will trasnform requests with the given xslt local entry key And will log
the message before and after. Takes Iterate local entry key as an argument-->
<template xmlns="http://ws.apache.org/ns/synapse" name="xslt\_func">
<parameter name="xslt\_key"/>
<sequence>
<log level="full">
<property name="BEFORE\_TRANSFORM" value="true" />
</log>
<xslt key="{$func:xslt\_key}"/>
<log level="full">
<property name="AFTER\_TRANSFORM" value="true" />
</log>
</sequence>
</template>
<localEntry key="xslt-key-req" src="file:repository/samples/resources/transform/transform.xslt"/>
<localEntry key="xslt-key-back" src="file:repository/samples/resources/transform/transform\_back.xslt"/>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample750--Objective"></a>Objective

Introduction to Apache Synapse Sequence Templates

### <a id="synapse-apache-org-userguide-samples-sample750--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 750 (repository/conf/sample/synapse\_sample\_750.xml)

  Unix/Linux: sh synapse.sh -sample 750  
  Windows: synapse.bat -sample 750

### <a id="synapse-apache-org-userguide-samples-sample750--Executing_the_Client"></a>Executing the Client

First execute the sample client as follows.

ant stockquote -Daddurl=http://localhost:8280/services/StockQuoteProxy -Dmode=customquote

Sequence Template can act a reusable function. Here the proxy service reuses
template xslt\_func which will transform requests with the given xslt local entry
key And will log the message before and after. It takes xslt transformation corresponding
to local entry key as an argument (for insequence this key is xslt-key-req and out sequence it is xslt-key-back).
We use call-template mediator for passing the xslt key parameter to a sequence template.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample8"></a>

# Apache Synapse – Apache Synapse - Sample 8

## <a id="synapse-apache-org-userguide-samples-sample8--Sample_8:_Introduction_to_Static_and_Dynamic_Registry_Resources_and_Using_XSLT_Transformations"></a>Sample 8: Introduction to Static and Dynamic Registry Resources, and Using XSLT Transformations

<definitions xmlns="http://ws.apache.org/ns/synapse">
<!-- the SimpleURLRegistry allows access to a URL based registry (e.g. file:/// or http://) -->
<registry provider="org.apache.synapse.registry.url.SimpleURLRegistry">
<!-- the root property of the simple URL registry helps resolve a resource URL as root + key -->
<parameter name="root">file:repository/conf/sample/resources/</parameter>
<!-- all resources loaded from the URL registry would be cached for this number of milli seconds -->
<parameter name="cachableDuration">15000</parameter>
</registry>
<!-- define the request processing XSLT resource as a static URL source -->
<localEntry key="xslt-key-req" src="file:repository/conf/sample/resources/transform/transform.xslt"/>
<sequence name="main">
<in>
<!-- transform the custom quote request into a standard quote requst expected by the service -->
<xslt key="xslt-key-req"/>
</in>
<out>
<!-- transform the standard response back into the custom format the client expects -->
<!-- the key is looked up in the remote registry and loaded as a 'dynamic' registry resource -->
<xslt key="transform/transform\_back.xslt"/>
</out>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample8--Objective"></a>Objective

Demonstrating the usage of the XSLT mediator for transforming message content
and using local registry and remote registry for storing configuration
metadata.

### <a id="synapse-apache-org-userguide-samples-sample8--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 8 (repository/conf/sample/synapse\_sample\_8.xml)

  Unix/Linux: sh synapse.sh -sample 8  
  Windows: synapse.bat -sample 8

### <a id="synapse-apache-org-userguide-samples-sample8--Executing_the_Client"></a>Executing the Client

This example uses the XSLT mediator to perform transformations, and the xslt
transformations are specified as registry resources. The first resource
'xslt-key-req' is specified as a 'local' registry entry. Local entries do not
place the resource on the registry, but simply make it available to the local
configuration. If a local entry is defined with a key that already exists in
the remote registry, the local entry will get higher precedence over the remote
resource.

In this example you will notice the new 'registry' definition. Synapse comes
with a simple URL based registry implementation (SimpleURLRegistry). During
initialization of the registry, the SimpleURLRegistry expects to find a property
named 'root', which specifies a prefix for the registry keys used later.
When the SimpleURLRegistry is used, this root is prefixed to the entry keys to
form the complete URL of the resource being looked up. The registry caches a
resource once requested, and stores it internally for a specified duration.
Once this period expires, it will reload the meta information about the resource
and reloads its cached copy if necessary, the next time the resource is requested.

Hence the second XSLT resource key 'transform/transform\_back.xslt' concatenated
with the 'root' of the SimpleURLRegistry 'file:repository/conf/sample/resources/'
forms the complete URL of the resource as
'file:repository/conf/sample/resources/transform/transform\_back.xslt' and caches
its value for a period of 15000 ms.

Execute the custom quote client as follows and analyze the the Synapse debug
log output.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/ -Dmode=customquote

The incoming message is transformed into a standard stock quote request by the
XSLT mediator. The XSLT mediator uses Xalan-J to perform the transformations.
It is possible to configure the underlying transformation engine using properties
when necessary. The response from the SimpleStockQuoteService is converted back
into the custom format as expected by the client during the out message processing.

During the response processing you could see the SimpleURLRegistry fetching the
resource as shown by the log message below.

[HttpClientWorker-1] DEBUG SimpleURLRegistry ==> Repository fetch of resource with key : transform/transform\_back.xslt

If you run the client again immediately (i.e within 15 seconds of the first
request) you will not see the resource being reloaded by the registry as the
cached value would be still valid.

However if you leave the system idle for 15 seconds or more and then retry the
same request, you will now notice that the registry notices the cached resource
has expired and will reload the meta information about the resource to check if
the resource has changed and will require a fresh fetch from the source URL.
If the meta data / version number indicates that a reload of the cached resource
is not necessary (i.e. unless the resource itself actually changed) the updated
meta information is used and the cache lease extended as appropriate.

[HttpClientWorker-1] DEBUG AbstractRegistry - Cached object has expired for key : transform/transform\_back.xslt
[HttpClientWorker-1] DEBUG SimpleURLRegistry - Perform RegistryEntry lookup for key : transform/transform\_back.xslt
[HttpClientWorker-1] DEBUG AbstractRegistry - Expired version number is same as current version in registry
[HttpClientWorker-1] DEBUG AbstractRegistry - Renew cache lease for another 15s

Thus the SimpleURLRegistry allows resource to be cached, and updates are detected
so that the configuration changes could be reloaded without restarting the
Synapse instance.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample800"></a>

# Apache Synapse – Apache Synapse - Sample 800

## <a id="synapse-apache-org-userguide-samples-sample800--Sample_800:_Introduction_to_REST_APIs"></a>Sample 800: Introduction to REST APIs

<definitions xmlns="http://ws.apache.org/ns/synapse">
<api name="StockQuoteAPI" context="/stockquote">
<resource uri-template="/view/{symbol}" methods="GET">
<inSequence>
<payloadFactory>
<format>
<m0:getQuote xmlns:m0="http://services.samples">
<m0:request>
<m0:symbol>$1</m0:symbol>
</m0:request>
</m0:getQuote>
</format>
<args>
<arg expression="get-property('uri.var.symbol')"/>
</args>
</payloadFactory>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService" format="soap11"/>
</endpoint>
</send>
</inSequence>
<outSequence>
<send/>
</outSequence>
</resource>
<resource url-pattern="/order/\*" methods="POST">
<inSequence>
<property name="FORCE\_SC\_ACCEPTED" value="true" scope="axis2"/>
<property name="OUT\_ONLY" value="true"/>
<send>
<endpoint>
<address uri="http://localhost:9000/services/SimpleStockQuoteService" format="soap11"/>
</endpoint>
</send>
</inSequence>
</resource>
</api>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample800--Objective"></a>Objective

APIs in Synapse provide a convenient approach for receiving and processing
REST traffic through the service bus. APIs can be used to receive specific
types of RESTful invocations and then process them through a set of user
defined resources. This sample is aimed at introducing the basic capabilities
of APIs and how they are configured to front existing services.

### <a id="synapse-apache-org-userguide-samples-sample800--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2 Server
- Start Synapse using the configuration numbered 800 (repository/conf/sample/synapse\_sample\_800.xml)

  Unix/Linux: sh synapse.sh -sample 800  
  Windows: synapse.bat -sample 800

### <a id="synapse-apache-org-userguide-samples-sample800--Executing_the_REST_Client"></a>Executing the REST Client

You might need a REST client like curl to test this

curl -v http://127.0.0.1:8280/stockquote/view/IBM

curl -v http://127.0.0.1:8280/stockquote/view/MSFT

The above GET calls will be handled by the first resource in the StockQuoteAPI.
These REST calls will get converted into SOAP calls and will be sent to the Axis2
server. Response will be sent to the client in POX format.

The following command POSTs a simple XML to the ESB. Save following sample place
order request as "placeorder.xml" file in your local file system and execute the
command. That is used to invoke a SOAP service. ESB returns the 202 response back to the client.

curl -v -d @placeorder.xml -H "Content-type: application/xml" http://127.0.0.1:8280/stockquote/order/

<placeOrder xmlns="http://services.samples">
<order>
<price>50</price>
<quantity>10</quantity>
<symbol>IBM</symbol>
</order>
</placeOrder>

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample850"></a>

# Apache Synapse – Apache Synapse - Sample 850

## <a id="synapse-apache-org-userguide-samples-sample850--Sample_850:_Introduction_to_Synapse_Callout_Block_function_template"></a>Sample 850: Introduction to Synapse Callout Block function template

<!-- Introduction to Synapse Callout Block function template -->
<definitions xmlns="http://ws.apache.org/ns/synapse">
<import xmlns="http://ws.apache.org/ns/synapse" name="EipLibrary" package="synapse.lang.eip" />
<sequence name="main">
<call-template target="synapse.lang.eip.callout\_block">
<with-param name="action" value="urn:getQuote"/>
<with-param name="service\_URL" value="http://localhost:9000/services/SimpleStockQuoteService"/>
<with-param xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/" xmlns:s12="http://www.w3.org/2003/05/soap-envelope" name="source\_xpath" value="{{s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]}}"/>
<with-param xmlns:s11="http://schemas.xmlsoap.org/soap/envelope/" xmlns:s12="http://www.w3.org/2003/05/soap-envelope" name="target\_xpath" value="{{s11:Body/child::\*[fn:position()=1] | s12:Body/child::\*[fn:position()=1]}}"/>
</call-template>
<respond/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample850--Objective"></a>Objective

This pattern is pretty much identical to the Routing slip pattern and this blocks external service invocation during mediation. And useful in scenarios such as service chaining. As default values are assigned to source and target xpaths, one can simply utilize this pattern by just defining serviceURL. This sample is an introduction to Synapse Callout Block function template.

### <a id="synapse-apache-org-userguide-samples-sample850--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 850 (repository/conf/sample/synapse\_sample\_850.xml)

  Unix/Linux: sh synapse.sh -sample 850  
  Windows: synapse.bat -sample 850

### <a id="synapse-apache-org-userguide-samples-sample850--Executing_the_Client"></a>Executing the Client

In this sample, the callout block pattern does a synchronized service invocation to the
StockQuoteService using the client request, gets the response and then using the send mediator, the message
is sent back to the client.

Invoke the client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample851"></a>

# Apache Synapse – Apache Synapse - Sample 851

## <a id="synapse-apache-org-userguide-samples-sample851--Sample_851:_Introduction_to_Synapse_Splitter_and_Aggregator_eip_function_templates"></a>Sample 851: Introduction to Synapse Splitter and Aggregator eip function templates

<definitions xmlns="http://ws.apache.org/ns/synapse">
<import xmlns="http://ws.apache.org/ns/synapse" name="EipLibrary" package="synapse.lang.eip" />
<proxy name="StockQuoteProxy" transports="https http" startOnLoad="true" trace="disable">
<target>
<inSequence>
<log level="custom">
<property name="text" value="splitterAggrigator"/>
</log>
<call-template target="synapse.lang.eip.splitter">
<with-param xmlns:m0="http://services.samples" name="iterate\_exp" value="{{//m0:getQuote/m0:request}}"/>
<with-param xmlns:m0="http://services.samples" name="attach\_path" value="{{//m0:getQuote}}"/>
<with-param name="attach\_path\_enabled" value="true"/>
<with-param name="endpoint\_uri" value="http://localhost:9000/services/SimpleStockQuoteService"/>
</call-template>
</inSequence>
<outSequence>
<call-template target="synapse.lang.eip.aggregator">
<with-param name="sequence\_ref" value="enr"/>
<with-param xmlns:m0="http://services.samples" name="aggregator\_exp" value="{{//m0:return}}"/>
<with-param name="oncomplete\_seq\_enabled" value="true"/>
</call-template>
</outSequence>
</target>
</proxy>
<sequence xmlns="http://ws.apache.org/ns/synapse" name="enr">
<log level="custom">
<property name="text" value="seqhit"/>
</log>
<enrich>
<source xmlns:m0="http://services.samples" clone="true"
xpath="//m0:return[not(preceding-sibling::m0:return/m0:last &lt;= m0:last) and not(following-sibling::m0:return/m0:last &lt; m0:last)]"/>
<target type="body"/>
</enrich>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample851--Objective"></a>Objective

This sample is an introduction to Synapse Splitter and Aggregator eip function templates. This showcase the combined functionality of Splitter and Aggregator patterns.

### <a id="synapse-apache-org-userguide-samples-sample851--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 851 (repository/conf/sample/synapse\_sample\_851.xml)

  Unix/Linux: sh synapse.sh -sample 851  
  Windows: synapse.bat -sample 851

### <a id="synapse-apache-org-userguide-samples-sample851--Executing_the_Client"></a>Executing the Client

In this sample, the message sent to Synapse will be splitted according to the given Xpath expression and does a synchronized call for the given endpoint and aggregates replies. Then mediates to the defined target sequence which filter the response which contains the best quote and send back to the client. Here it uses Splitter and Aggregator function templates.

Invoke the client as follows.

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuoteProxy -Ditr=4

The above command will send a request containing four fragments in it.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample852"></a>

# Apache Synapse – Apache Synapse - Sample 852

## <a id="synapse-apache-org-userguide-samples-sample852--Sample_852:_Introduction_to_Synapse_Splitter-Agrregator_eip_function_template"></a>Sample 852: Introduction to Synapse Splitter-Agrregator eip function template

<definitions xmlns="http://ws.apache.org/ns/synapse">
<import xmlns="http://ws.apache.org/ns/synapse" name="EipLibrary" package="synapse.lang.eip" />
<sequence name="main">
<call-template target="synapse.lang.eip.splitter\_aggregator">
<with-param name="attach\_path\_enabled" value="true"/>
<with-param name="endpoint\_uri" value="http://localhost:9000/services/SimpleStockQuoteService"/>
<with-param xmlns:m0="http://services.samples" name="iterate\_exp" value="{{//m0:getQuote/m0:request}}"/>
<with-param xmlns:m0="http://services.samples" name="attach\_path" value="{{//m0:getQuote}}"/>
<with-param name="sequence\_ref" value="enr"/>
<with-param xmlns:m0="http://services.samples" name="aggregator\_exp" value="{{//m0:return}}"/>
<with-param name="oncomplete\_seq\_enabled" value="true"/>
</call-template>
</sequence>
<sequence xmlns="http://ws.apache.org/ns/synapse" name="enr">
<log level="custom">
<property name="text" value="seqhit"/>
</log>
<enrich>
<source xmlns:m0="http://services.samples" clone="true"
xpath="//m0:return[not(preceding-sibling::m0:return/m0:last &lt;= m0:last) and not(following-sibling::m0:return/m0:last &lt; m0:last)]"/>
<target type="body"/>
</enrich>
<send/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample852--Objective"></a>Objective

This sample is an introduction to Synapse Splitter-Aggregator combined function template.

### <a id="synapse-apache-org-userguide-samples-sample852--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 852 (repository/conf/sample/synapse\_sample\_852.xml)

  Unix/Linux: sh synapse.sh -sample 852  
  Windows: synapse.bat -sample 852

### <a id="synapse-apache-org-userguide-samples-sample852--Executing_the_Client"></a>Executing the Client

In this sample, the message sent to Synapse will be splitted according to the given Xpath expression and does a synchronized call for the given endpoint and aggregates replies. Then mediates to the defined target sequence which filter the response which contains the best quote and send back to the client. Here it only uses Splitter-Aggregator template.

Invoke the client as follows.

ant stockquote -Dtrpurl=http://localhost:8280/ -Ditr=4

The above command will send a request containing four fragments in it.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample853"></a>

# Apache Synapse – Apache Synapse - Sample 853

## <a id="synapse-apache-org-userguide-samples-sample853--Sample_853:_Introduction_to_Synapse_Scatter-Gather_eip_function_template"></a>Sample 853: Introduction to Synapse Scatter-Gather eip function template

<definitions xmlns="http://ws.apache.org/ns/synapse">
<import xmlns="http://ws.apache.org/ns/synapse" name="EipLibrary" package="synapse.lang.eip" />
<sequence name="enr">
<log level="custom">
<property name="text" value="seqhit"/>
</log>
<enrich>
<source xmlns:m0="http://services.samples" clone="true" xpath="//m0:return[not(preceding-sibling::m0:return/m0:last &lt;= m0:last) and not(following-sibling::m0:return/m0:last &lt; m0:last)]"/>
<target type="body"/>
</enrich>
<send/>
</sequence>
<sequence name="main">
<call-template target="synapse.lang.eip.scatter\_gather">
<with-param name="sequence\_ref" value="enr"/>
<with-param xmlns:m0="http://services.samples" name="aggregator\_exp" value="{{//m0:return}}"/>
<with-param name="oncomplete\_seq\_enabled" value="true"/>
<with-param name="recipient\_list" value="http://localhost:9001/services/SimpleStockQuoteService,http://localhost:9002/services/SimpleStockQuoteService,http://localhost:9003/services/SimpleStockQuoteService"/>
</call-template>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample853--Objective"></a>Objective

This sample is an introduction to Synapse Scatter-Gather eip function template. Scatter-Gather pattern broadcasts a message to multiple recipients and re-aggregates the responses back into a single message and send back to client or mediates to the defined target sequence.

### <a id="synapse-apache-org-userguide-samples-sample853--Pre-requisites"></a>Pre-requisites

- Start three instances of sample Axis2 server on HTTP ports 9001,9002,9003. And deploy the SimpleStockQuoteService in all of them.
- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 853 (repository/conf/sample/synapse\_sample\_853.xml)

  Unix/Linux: sh synapse.sh -sample 853  
  Windows: synapse.bat -sample 853

### <a id="synapse-apache-org-userguide-samples-sample853--Executing_the_Client"></a>Executing the Client

In this sample, the message sent to Synapse will be broadcast to the specified recipients. Then aggregates replies and mediates to the defined target sequence which filter the response which contains the best quote and send back to the client.

Invoke the client as follows.

ant stockquote -Dtrpurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample854"></a>

# Apache Synapse – Apache Synapse - Sample 854

## <a id="synapse-apache-org-userguide-samples-sample854--Sample_854:_Introduction_to_Synapse_Wire_Tap_eip_function_template"></a>Sample 854: Introduction to Synapse Wire Tap eip function template

<!-- Introduction to Synapse Wire Tap eip function template -->
<definitions xmlns="http://ws.apache.org/ns/synapse">
<import xmlns="http://ws.apache.org/ns/synapse" name="EipLibrary" package="synapse.lang.eip" />
<sequence name="main">
<property name="OUT\_ONLY" value="true"/>
<property name="FORCE\_SC\_ACCEPTED" value="true" scope="axis2"/>
<call-template target="synapse.lang.eip.wire\_tap">
<with-param name="wiretap\_uri" value="http://localhost:9000/services/SimpleStockQuoteService"/>
<with-param name="destination\_uri" value="http://localhost:9001/services/SimpleStockQuoteService"/>
</call-template>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample854--Objective"></a>Objective

This sample is an introduction to Synapse Wire Tap eip function template.

### <a id="synapse-apache-org-userguide-samples-sample854--Pre-requisites"></a>Pre-requisites

- Start two instances of sample Axis2 server on HTTP ports 9000,9001. And deploy the SimpleStockQuoteService in all of them.
- Start Synapse using the configuration numbered 854 (repository/conf/sample/synapse\_sample\_854.xml)

  Unix/Linux: sh synapse.sh -sample 854  
  Windows: synapse.bat -sample 854

### <a id="synapse-apache-org-userguide-samples-sample854--Executing_the_Client"></a>Executing the Client

In this sample, the messages sent to Synapse will be route to a secondary channel(wiretap\_uri) while they are being forwarded to the main channel(destination\_uri).

Invoke the client as follows.

ant stockquote -Dtrpurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample855"></a>

# Apache Synapse – Apache Synapse - Sample 855

## <a id="synapse-apache-org-userguide-samples-sample855--Sample_855:_Introduction_to_Synapse_Content_Based_Router_eip_function_template"></a>Sample 855: Introduction to Synapse Content Based Router eip function template

<!-- Introduction to Synapse Content Based Router eip function template -->
<definitions xmlns="http://ws.apache.org/ns/synapse">
<import xmlns="http://ws.apache.org/ns/synapse" name="EipLibrary" package="synapse.lang.eip" />
<proxy name="StockQuoteProxy" transports="https http" startOnLoad="true" trace="disable">
<target>
<inSequence>
<call-template target="synapse.lang.eip.content\_based\_router">
<with-param name="routing\_exp" value="{{//m0:getQuote/m0:request/m0:symbol}}" xmlns:m0="http://services.samples"/>
<with-param name="match\_content" value="IBM:cnd1\_seq,MSFT:cnd2\_seq;cnd3\_seq"/>
</call-template>
</inSequence>
<outSequence>
<send/>
</outSequence>
</target>
</proxy>
<sequence name="send\_seq">
<log level="custom">
<property name="DEBUG" value="Condition Satisfied"/>
</log>
<send>
<endpoint name="simple">
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</sequence>
<sequence name="cnd1\_seq">
<log level="custom">
<property name="MSG\_FLOW" value="Condition (I) Satisfied"/>
</log>
<sequence key="send\_seq"/>
</sequence>
<sequence name="cnd2\_seq">
<log level="custom">
<property name="MSG\_FLOW" value="Condition (II) Satisfied"/>
</log>
<sequence key="send\_seq"/>
</sequence>
<sequence name="cnd3\_seq">
<log level="custom">
<property name="MSG\_FLOW" value="Condition (III) Satisfied"/>
</log>
<sequence key="send\_seq"/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample855--Objective"></a>Objective

This sample is an introduction Synapse Content Based Router eip function template.

### <a id="synapse-apache-org-userguide-samples-sample855--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 855 (repository/conf/sample/synapse\_sample\_855.xml)

  Unix/Linux: sh synapse.sh -sample 855  
  Windows: synapse.bat -sample 855

### <a id="synapse-apache-org-userguide-samples-sample855--Executing_the_Client"></a>Executing the Client

In this sample, it routes the message by matching the specified Xpath to the regular expression. Execute the StockQuote client in the dumb client mode, specifying 'IBM', 'MSFT' and 'DELL' as the stock symbols.

When the symbol IBM is requested, you will see cnd1\_seq sequence is getting executed.

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuoteProxy -Dsymbol=IBM

When the symbol MSFT is requested, you will see cnd2\_seq sequence is getting executed.

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuoteProxy -Dsymbol=MSFT

When the symbol DELL is requested, you will see cnd3\_seq sequence is getting executed , which is the default sequence.

ant stockquote -Dtrpurl=http://localhost:8280/services/StockQuoteProxy -Dsymbol=DELL

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample856"></a>

# Apache Synapse – Apache Synapse - Sample 856

## <a id="synapse-apache-org-userguide-samples-sample856--Sample_856:_Introduction_to_Synapse_Dynamic_Router_eip_function_template"></a>Sample 856: Introduction to Synapse Dynamic Router eip function template

<!-- Introduction to Synapse Dynamic Router eip function template -->
<definitions xmlns="http://ws.apache.org/ns/synapse">
<import xmlns="http://ws.apache.org/ns/synapse" name="EipLibrary" package="synapse.lang.eip" />
<proxy name="StockQuoteProxy" transports="https http" startOnLoad="true" trace="disable">
<target>
<inSequence>
<call-template target="synapse.lang.eip.dynamic\_router">
<with-param name="conditions" value="header=foo:bar.\*{AND}url=/services/StockQuoteProxy.\*;seq=cnd1\_seq,header=custom\_header1:bar.\*{OR}header=custom\_header1:foo.\*;seq=cnd2\_seq,header=custom\_header2:foo.\*;seq=cnd3\_seq"/>
</call-template>
</inSequence>
<outSequence>
<send/>
</outSequence>
</target>
</proxy>
<sequence name="send\_seq">
<log level="custom">
<property name="DEBUG" value="Condition Satisfied"/>
</log>
<send>
<endpoint name="simple">
<address uri="http://localhost:9000/services/SimpleStockQuoteService"/>
</endpoint>
</send>
</sequence>
<sequence name="cnd1\_seq">
<log level="custom">
<property name="MSG\_FLOW" value="Condition (I) Satisfied"/>
</log>
<sequence key="send\_seq"/>
</sequence>
<sequence name="cnd2\_seq">
<log level="custom">
<property name="MSG\_FLOW" value="Condition (II) Satisfied"/>
</log>
<sequence key="send\_seq"/>
</sequence>
<sequence name="cnd3\_seq">
<log level="custom">
<property name="MSG\_FLOW" value="Condition (III) Satisfied"/>
</log>
<sequence key="send\_seq"/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample856--Objective"></a>Objective

This sample is an introduction to Synapse Dynamic Router eip function template.

### <a id="synapse-apache-org-userguide-samples-sample856--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 856 (repository/conf/sample/synapse\_sample\_856.xml)

  Unix/Linux: sh synapse.sh -sample 856  
  Windows: synapse.bat -sample 856

### <a id="synapse-apache-org-userguide-samples-sample856--Executing_the_Client"></a>Executing the Client

In this sample, it checks whether the route condition based on HTTP url,HTTP headers evaluates to true and mediates using the given sequence. We will be using 'curl' as the client in this scenario.

Invoke curl commands as follows to see dynamic routing in action.

curl -d @stockQuoteReq.xml -H "Content-Type: application/soap+xml;charset=UTF-8" -H "foo:bar" "http://localhost:8280/services/StockQuoteProxy

You will see logs according to cnd1\_seq in console.

curl -d @stockQuoteReq.xml -H "Content-Type: application/soap+xml;charset=UTF-8" -H "custom\_header1:foo" "http://localhost:8280/services/StockQuoteProxy"

or

curl -d @stockQuoteReq.xml -H "Content-Type: application/soap+xml;charset=UTF-8" -H "custom\_header1:bar" "http://localhost:8280/services/StockQuoteProxy"

You will see logs according to cnd2\_seq in console.

curl -d @stockQuoteReq.xml -H "Content-Type: application/soap+xml;charset=UTF-8" -H "custom\_header2:foo" "http://localhost:8280/services/StockQuoteProxy"

You will see logs according to cnd3\_seq in console.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample857"></a>

# Apache Synapse – Apache Synapse - Sample 857

## <a id="synapse-apache-org-userguide-samples-sample857--Sample_857:_Introduction_to_Synapse_Recipient_List_eip_function_template"></a>Sample 857: Introduction to Synapse Recipient List eip function template

<!-- Introduction to Synapse Recipient List eip function template -->
<definitions xmlns="http://ws.apache.org/ns/synapse">
<import xmlns="http://ws.apache.org/ns/synapse" name="EipLibrary" package="synapse.lang.eip" />
<sequence name="main">
<property name="OUT\_ONLY" value="true"/>
<property name="FORCE\_SC\_ACCEPTED" value="true" scope="axis2"/>
<call-template target="synapse.lang.eip.recipient\_list">
<with-param name="recipient\_list" value="http://localhost:9000/services/SimpleStockQuoteService,http://localhost:9001/services/SimpleStockQuoteService"/>
</call-template>
<drop/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample857--Objective"></a>Objective

This sample is an introduction to Synapse Recipient List eip function template.

### <a id="synapse-apache-org-userguide-samples-sample857--Pre-requisites"></a>Pre-requisites

- Start two instances of sample Axis2 server on HTTP ports 9000,9001. And deploy the SimpleStockQuoteService in all of them.
- Start Synapse using the configuration numbered 857 (repository/conf/sample/synapse\_sample\_857.xml)

  Unix/Linux: sh synapse.sh -sample 857  
  Windows: synapse.bat -sample 857

### <a id="synapse-apache-org-userguide-samples-sample857--Executing_the_Client"></a>Executing the Client

In this sample, the messages sent to Synapse will be route to the endpoints defined under recipient\_list parameter.

Invoke the client as follows.

ant stockquote -Dtrpurl=http://localhost:8280/

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-sample9"></a>

# Apache Synapse – Apache Synapse - Sample 9

## <a id="synapse-apache-org-userguide-samples-sample9--Sample_9:_Introduction_to_Dynamic_Sequences_with_Registry"></a>Sample 9: Introduction to Dynamic Sequences with Registry

<definitions xmlns="http://ws.apache.org/ns/synapse">
<registry provider="org.apache.synapse.registry.url.SimpleURLRegistry">
<parameter name="root">file:./repository/conf/sample/resources/</parameter>
<parameter name="cachableDuration">15000</parameter>
</registry>
<sequence name="main">
<sequence key="sequence/dynamic\_seq\_1.xml"/>
</sequence>
</definitions>

### <a id="synapse-apache-org-userguide-samples-sample9--Objective"></a>Objective

Demonstrating the ability to load sequence definitions dynamically from the
remote registry.

### <a id="synapse-apache-org-userguide-samples-sample9--Pre-requisites"></a>Pre-requisites

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- Start Synapse using the configuration numbered 9 (repository/conf/sample/synapse\_sample\_9.xml)

  Unix/Linux: sh synapse.sh -sample 9  
  Windows: synapse.bat -sample 9

### <a id="synapse-apache-org-userguide-samples-sample9--Executing_the_Client"></a>Executing the Client

This example demonstrates the dynamic behaviour of Synapse through the use of a
registry. Synapse supports dynamic definitions for sequences and endpoints, and
as seen before, for configuration resources (eg: schema files, XSLT files etc).
In this example we define a Synapse configuration which references a sequence
definition specified as a registry key. The registry key resolves to the actual
content of the sequence which would be loaded dynamically by Synapse at runtime,
and cached appropriately as per its definition in the registry. Once the cache
expires, Synapse would re-check the meta information for the definition and
re-load the sequence definition if necessary and re-cache it again.

Execute the client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

Go through the mediation debug logs to see how Synapse has dynamically loaded
the sequence configurations from the registry.

[HttpServerWorker-1] DEBUG SimpleURLRegistry - ==> Repository fetch of resource with key : sequence/dynamic\_seq\_1.xml
...
[HttpServerWorker-1] DEBUG SequenceMediator - Sequence mediator <dynamic\_sequence> :: mediate()
...
[HttpServerWorker-1] INFO LogMediator - message = \*\*\* Test Message 1 \*\*\*

Now if you execute the client immediately (i.e. within 15 seconds of the last
execution) you will notice that the sequence is not reloaded. If you edit the
sequence definition in repository/conf/sample/resources/sequence/dynamic\_seq\_1.xml
(i.e. edit the log message to read as '\*\*\* Test Message 2 \*\*\*') and execute the
client again, you will notice that the new message is not yet visible (i.e. if
you execute this within 15 seconds of loading the resource for the first time).
However, after 15 seconds elapsed since the original caching of the sequence,
you will notice that the new sequence is loaded and executed by Synapse from the
following log messages.

[HttpServerWorker-1] DEBUG SimpleURLRegistry - ==> Repository fetch of resource with key : sequence/dynamic\_seq\_1.xml
...
[HttpServerWorker-1] DEBUG SequenceMediator - Sequence mediator <dynamic\_sequence> :: mediate()
...
[HttpServerWorker-1] INFO LogMediator - message = \*\*\* Test Message 2 \*\*\*

The cache timeout could be tuned appropriately by configuring the URL registry
to suit the environment and the needs.

[Back to Catalog](#synapse-apache-org-userguide-samples)

---

<a id="synapse-apache-org-userguide-samples-setup-db"></a>

# Apache Synapse – Apache Synapse - Database Setup Guide

## <a id="synapse-apache-org-userguide-samples-setup-db--Database_Setup_Guide"></a>Database Setup Guide

This document explains how to setup a database and some data sources as required
by the samples. Most samples require an Apache Derby installation whereas a few
would require a MySQL setup.

## <a id="synapse-apache-org-userguide-samples-setup-db--Contents"></a>Contents

- [Introduction](#synapse-apache-org-userguide-samples-setup-db--intro)
- [Setting Up Apache Derby](#synapse-apache-org-userguide-samples-setup-db--derby)
- [Setting Up MySQL](#synapse-apache-org-userguide-samples-setup-db--mysql)
- [Using Other Database Engines](#synapse-apache-org-userguide-samples-setup-db--other)
- [Setting Up Data Sources](#synapse-apache-org-userguide-samples-setup-db--ds)

<a id="synapse-apache-org-userguide-samples-setup-db--intro"></a>

## <a id="synapse-apache-org-userguide-samples-setup-db--Introduction"></a>Introduction

Apache Synapse has the ability to lookup and update relational databases through
JDBC. Any database engine that provides JDBC drivers can be integrated with
Synapse using the dblookup and dbreport mediators. Synapse ships with a collection
of samples which demonstrates various aspects of dblookup, dbreport mediators and
database integration. This article describes how to setup the databases, sample
tables and reusable data sources required to try these examples out.

Most samples assume Apache Derby is used as the database engine. And therefore this
article also focuses mainly on setting up Apache Derby. Byt in reality
any database engine can be used to run these samples. The database schema and SQL
queries described here will work with any database engine. However in such cases
some minor changes should be made to the Synapse configuration files.

Some samples involve invoking database stored procedures from Synapse. For these
samples MySQL database engine is assumed. Therefore this article provides some
basic information on setting up MySQL for the example scenarios.

<a id="synapse-apache-org-userguide-samples-setup-db--derby"></a>

## <a id="synapse-apache-org-userguide-samples-setup-db--Setting_Up_Apache_Derby"></a>Setting Up Apache Derby

To start with, download the latest binary distribution of [Apache Derby](http://db.apache.org/derby/).
Extract the downloaded archive to a suitable location in the local disk and switch
to the 'bin' directory of the installation. Start the Derby network server by
executing the 'startNetworkServer' startup script. An output similar to the following
will be displayed as the database engine starts up.

Sun Jan 02 10:53:28 IST 2011 : Security manager installed using the Basic server security policy.
Sun Jan 02 10:53:30 IST 2011 : Apache Derby Network Server - 10.7.1.1 - (1040133) started and ready to accept connections on port 1527

Now launch the Derby client tool by executing the 'ij' script. This will give a command
prompt where you can execute various command and SQL queries. Execute the following
connect statement to create a fresh database named 'synapsedb' and obtain a connection
to it.

CONNECT 'jdbc:derby://localhost:1527/synapsedb;user=synapse;password=synapse;create=true';

Now execute the following SQL query to create a new table.

CREATE table company(name varchar(10), id varchar(10), price double);

Insert some sample data to the table by executing following statements.

INSERT into company values ('IBM','c1',0.0);
INSERT into company values ('SUN','c2',0.0);
INSERT into company values ('MSFT','c3',0.0);

Now we have finished setting up the Derby server for the samples. As the final step
we should copy the Derby JDBC drivers to Synapse 'lib' directory. Locate the following
jar files in Derby installation and copy them into Synapse.

- derby.jar
- derbyclient.jar
- derbynet.jar

<a id="synapse-apache-org-userguide-samples-setup-db--mysql"></a>

## <a id="synapse-apache-org-userguide-samples-setup-db--Setting_Up_MySQL"></a>Setting Up MySQL

This section assumes that you already have a MySQL server instance up and running.
For details on installing MySQL, please refer the relevant
[MySQL documentation](http://dev.mysql.com/doc/refman/5.1/en/installing.html).

Create a new database named 'synapsedb' in MySQL. Then execute the SQL queries
given in the previous section to create a table named 'company' and insert some
sample data into it. Then execute the following two commands in MySQL client to
create two stored procedures.

CREATE PROCEDURE getCompany(compName VARCHAR(10)) SELECT name, id, price FROM company WHERE name = compName;
CREATE PROCEDURE updateCompany(compPrice DOUBLE,compName VARCHAR(10)) UPDATE company SET price = compPrice WHERE name = compName;

Then you should download the [MySQL JDBC driver](http://www.mysql.com/products/connector/)
and deploy it into the 'lib' directory of Synapse.

<a id="synapse-apache-org-userguide-samples-setup-db--other"></a>

## <a id="synapse-apache-org-userguide-samples-setup-db--Using_Other_Database_Engines"></a>Using Other Database Engines

You can run the given samples using any RDBMS engine you prefer. In that case please
make sure you do the following.

- Deploy the JDBC drivers for your database engine into Synapse
- Update the sample configuration files and change the driver class name and JDBC
  connection string correctly

## <a id="synapse-apache-org-userguide-samples-setup-db--Setting_Up_Data_Sources"></a>Setting Up Data Sources

Synapse is capable of connecting to databases through predefined data sources.
This enables database connection pooling and connection reuse. Different instances
of the database mediators (dblookup/dbreport) can either use different data sources
or share the same data source.

Data sources are configured in the synapse.properties file which can be found in the
'lib' directory of the Synapse installation. Currently Synapse supports following
types of data sources.

- BasicDataSource
- PerUserPoolDataSource

Both these types of data sources are based on [Apache DBCP](http://commons.apache.org/dbcp).

Following section describes how to setup two data sources as required by some of the
database integration samples of Synapse. First, it is required to setup two
Derby databases. So launch 'ij' client tool for Derby and create two databases
named 'lookupdb' and 'reportdb'. Specify the username and password to be 'synapse'
for both databases. Create the 'company' table in each database and add some sample
data as described under [Setting Up Apache Derby](#synapse-apache-org-userguide-samples-setup-db--derby) section.

Now you can define two data sources for these databases by adding the following
entries to the synapse.properties file.

synapse.datasources=lookupds,reportds
synapse.datasources.icFactory=com.sun.jndi.rmi.registry.RegistryContextFactory
synapse.datasources.providerUrl=rmi://localhost:2199
synapse.datasources.providerPort=2199
synapse.datasources.lookupds.type=BasicDataSource
synapse.datasources.lookupds.driverClassName=org.apache.derby.jdbc.ClientDriver
synapse.datasources.lookupds.url=jdbc:derby://localhost:1527/lookupdb;create=false
synapse.datasources.lookupds.username=synapse
synapse.datasources.lookupds.password=synapse
synapse.datasources.lookupds.dsName=lookupdb
synapse.datasources.lookupds.maxActive=100
synapse.datasources.lookupds.maxIdle=20
synapse.datasources.lookupds.maxWait=10000
synapse.datasources.reportds.type=PerUserPoolDataSource
synapse.datasources.reportds.cpdsadapter.factory=org.apache.commons.dbcp.cpdsadapter.DriverAdapterCPDS
synapse.datasources.reportds.cpdsadapter.className=org.apache.commons.dbcp.cpdsadapter.DriverAdapterCPDS
synapse.datasources.reportds.cpdsadapter.name=cpds
synapse.datasources.reportds.dsName=reportdb
synapse.datasources.reportds.driverClassName=org.apache.derby.jdbc.ClientDriver
synapse.datasources.reportds.url=jdbc:derby://localhost:1527/reportdb;create=false
synapse.datasources.reportds.username=synapse
synapse.datasources.reportds.password=synapse
synapse.datasources.reportds.maxActive=100
synapse.datasources.reportds.maxIdle=20
synapse.datasources.reportds.maxWait=10000

Here we are defining two data sources named 'lookupds' and 'reportds'. The first
data source is defined as a BasicDataSource and the other one is defined as a
PerUserPoolDataSource. Note the various parameters we have specified for each
data source thereby further customizing the behavior of each data source.

---

<a id="synapse-apache-org-userguide-samples-setup-fix"></a>

# Apache Synapse – Apache Synapse - FIX Setup Guide

## <a id="synapse-apache-org-userguide-samples-setup-fix--FIX_Setup_Guide"></a>FIX Setup Guide

This document explains how to setup the FIX transport sender and listener
in Synapse as required by the samples. Further it describes how to setup the
sample FIX applications (Executor and Banzai) which are essential for trying
out the FIX samples.

## <a id="synapse-apache-org-userguide-samples-setup-fix--Contents"></a>Contents

- [Introduction](#synapse-apache-org-userguide-samples-setup-fix--intro)
- [Prerequisites](#synapse-apache-org-userguide-samples-setup-fix--pre)
- [Enabling FIX Transport in Synapse](#synapse-apache-org-userguide-samples-setup-fix--synapse)
- [Configuring Services for FIX Transport](#synapse-apache-org-userguide-samples-setup-fix--services)
- [Setting Up the Sample FIX Applications](#synapse-apache-org-userguide-samples-setup-fix--samples)
  - [Configuring the Executor](#synapse-apache-org-userguide-samples-setup-fix--exec)
  - [Configuring Banzai](#synapse-apache-org-userguide-samples-setup-fix--banzai)

<a id="synapse-apache-org-userguide-samples-setup-fix--intro"></a>

## <a id="synapse-apache-org-userguide-samples-setup-fix--Introduction"></a>Introduction

[FIX (Financial Information eXchange)](http://www.fixprotocol.org)
is a domain specific communication protocol widely used in the finance sector for
securities transactions. The protocol specification spans across the application layer
and the session layer of the OSI reference model of networking. Apache Synapse comes
with a FIX transport adapter which enables the Synapse ESB to communicate with FIX
acceptors and initiators. This allows users to seamlessly integrate FIX applications
together and even link FIX applications with other systems that use different protocols.

This article describes how to enable and configure the FIX transport listener and
sender for Apache Synapse. It also describes how to setup various sample FIX applications
required to try out the FIX protocol related examples.

<a id="synapse-apache-org-userguide-samples-setup-fix--pre"></a>

## <a id="synapse-apache-org-userguide-samples-setup-fix--Prerequisites"></a>Prerequisites

The FIX transport adapter of Synapse is built on top of the [Quickfix/J](http://www.quickfixj.org)
open source FIX engine. Therefore the users must deploy the Quickfix/J libraries
into Synapse before using the FIX transport. Also in order to try out the FIX
samples described in this documentation, it is required to have the 2 sample FIX
applications (Banzai and Executor) that come bundled with Quickfix/J. Therefore as
the first step [download](http://www.quickfixj.org/downloads/) the latest
binary distribution of Quickfix/J and extract the downloaded archive to a suitable
location on the local disk (let's refer to this location as QFJ\_HOME).

All the necessary Quickfix/J libraries are available in the Quickfix/J binary
distribution. You have to copy the following jar files from Quickfix/J installation
to the 'lib' directory of Synapse.

- quickfixj-core.jar
- quickfixj-msg-fix40.jar
- quickfixj-msg-fix41.jar
- quickfixj-msg-fix42.jar
- quickfixj-msg-fix43.jar
- quickfixj-msg-fix44.jar
- mina-core.jar
- slf4j-api.jar

The last 2 jar files can be found in the QFJ\_HOME/bin directory and all other
files should be available in the QFJ\_HOME itself.

<a id="synapse-apache-org-userguide-samples-setup-fix--synapse"></a>

## <a id="synapse-apache-org-userguide-samples-setup-fix--Enabling_FIX_Transport_in_Synapse"></a>Enabling FIX Transport in Synapse

FIX transport listener and the FIX transport sender of Synapse can be enabled by
uncommenting the following sections in the repository/conf/axis2.xml file.

<transportReceiver name="fix" class="org.apache.synapse.transport.fix.FIXTransportListener"/>

<transportSender name="fix" class="org.apache.synapse.transport.fix.FIXTransportSender"/>

This will initialize the FIX transport adapter and have it up and running to be
used by the proxy services. However some additional setting should be applied at
the service level before a service can make use of the FIX transport.

<a id="synapse-apache-org-userguide-samples-setup-fix--services"></a>

## <a id="synapse-apache-org-userguide-samples-setup-fix--Configuring_Services_for_FIX_Transport"></a>Configuring Services for FIX Transport

When a service needs to be exposed over the FIX transport, we should add the
following parameter to the service configuration.

<parameter name="transport.fix.AcceptorConfigURL">url</parameter>

The value of this parameter must be a valid URL which points to a Quickfix/J session
configuration file. All the FIX sample configurations are already equipped with this
parameter and they are pointing to the sample Quickfix/J configuration files that
comes with Synapse. These files can be found in the repository/sample/resources/fix
directory. One such configuration file (fix-synapse.cfg) is shown below.

[default]
FileStorePath=repository/fix/store/acceptor
ConnectionType=acceptor
StartTime=00:00:00
EndTime=00:00:00
HeartBtInt=30
ValidOrderTypes=1,2,F
SenderCompID=EXEC
TargetCompID=SYNAPSE
UseDataDictionary=Y
DefaultMarketPrice=12.30
[session]
BeginString=FIX.4.0
SocketAcceptPort=9876

One of the most important parameters in this configuration is the SocketAcceptPort
setting. This defines the port used by the Synapse proxy service to receive
incoming FIX messages.

As far as the FIX samples are considered you don't have to make any changes to
these Quickfix/J configuration files or the Synapse sample configurations. Default
settings should work out of the box without any issues. However some samples may
require you to make minor changes to these files.

<a id="synapse-apache-org-userguide-samples-setup-fix--samples"></a>

## <a id="synapse-apache-org-userguide-samples-setup-fix--Setting_Up_the_Sample_FIX_Applications"></a>Setting Up the Sample FIX Applications

Two sample FIX applications are available in the Quickfix/J binray distribution
which can be used to send and receive FIX messages. By default these applications
are configured to directly communicate with each other. So we should make a few
modifications to the configuration to get them to communicate with Synapse. The
binaries of these sample programs are available in the quickfixj-examples.jar file
in QFJ\_HOME. Startup scripts needed to run them can be found in the QFJ\_HOME/bin
directory.

<a id="synapse-apache-org-userguide-samples-setup-fix--exec"></a>

### <a id="synapse-apache-org-userguide-samples-setup-fix--Configuring_the_Executor"></a>Configuring the Executor

Executor is the sample acceptor program. To configure this application to
receive messages from Synapse, put the following entries to a file named
'executor.cfg'.

[default]
FileStorePath=examples/target/data/executor
ConnectionType=acceptor
StartTime=00:00:00
EndTime=00:00:00
HeartBtInt=30
ValidOrderTypes=1,2,F
SenderCompID=EXEC
TargetCompID=SYNAPSE
UseDataDictionary=Y
DefaultMarketPrice=12.30
[session]
BeginString=FIX.4.0
SocketAcceptPort=19876

Note that TargetCompID parameter has been set to 'SYNAPSE' and the port number
has been set to 9876. You can launch the Executor using the above configuration
as follows.

Unix/Linux: sh executor.sh <path to executor.cfg>  
Windows: executor.bat <path to executor.cfg>

For some samples you will have to make some minor modifications to this
configuration file.

<a id="synapse-apache-org-userguide-samples-setup-fix--banzai"></a>

### <a id="synapse-apache-org-userguide-samples-setup-fix--Configuring_Banzai"></a>Configuring Banzai

Banzai is a sample FIX initiator that comes with Quickfix/J. This can be
used to send FIX messages to a defined FIX acceptor. In case of samples,
Synapse will act as the acceptor. In order to send messages to Synapse, we
should start Banzai using the following configuration.

[default]
FileStorePath=examples/target/data/banzai
ConnectionType=initiator
SenderCompID=BANZAI
TargetCompID=SYNAPSE
SocketConnectHost=localhost
StartTime=00:00:00
EndTime=00:00:00
HeartBtInt=30
ReconnectInterval=5
[session]
BeginString=FIX.4.0
SocketConnectPort=9876

Note that TargetCompID has been set to 'SYNAPSE' and the socket connect port
is specified to be 9876, which is the port used by Synapse. To start Banzai
with this configuration, save the above in a file named 'banzai.cfg' and
launch the sample application as follows.

Unix/Linux: sh banzai.sh <path to banzai.cfg>  
Windows: banzai.bat <path to banzai.cfg>

---

<a id="synapse-apache-org-userguide-samples-setup-jms"></a>

# Apache Synapse – Apache Synapse - JMS Setup Guide

## <a id="synapse-apache-org-userguide-samples-setup-jms--JMS_Setup_Guide"></a>JMS Setup Guide

This document explains how to setup the JMS transport sender and listener
as required by the samples. An Apache ActiveMQ instance is used as the JMS
provider for the samples.

## <a id="synapse-apache-org-userguide-samples-setup-jms--Contents"></a>Contents

- [Introduction](#synapse-apache-org-userguide-samples-setup-jms--intro)
- [Prerequisites](#synapse-apache-org-userguide-samples-setup-jms--pre)
- [Enabling JMS Support in Synapse](#synapse-apache-org-userguide-samples-setup-jms--synapse)
  - [Enabling the JMS Listener](#synapse-apache-org-userguide-samples-setup-jms--listener)
  - [Enabling the JMS Sender](#synapse-apache-org-userguide-samples-setup-jms--sender)
- [Enabling JMS Support in Axis2 Server](#synapse-apache-org-userguide-samples-setup-jms--server)
- [Enabling JMS Support in Axis2 Client](#synapse-apache-org-userguide-samples-setup-jms--client)
- [Configure Synapse for AMQP Transport](#synapse-apache-org-userguide-samples-setup-jms--amqp)

<a id="synapse-apache-org-userguide-samples-setup-jms--intro"></a>

## <a id="synapse-apache-org-userguide-samples-setup-jms--Introduction"></a>Introduction

Apache Synapse has exceptional support for JMS (Java Message Service). It uses JNDI
to connect to JMS brokers, and therefore works with any JMS provider that supports
JNDI. Synapse has been successfully tested with the following well-known JMS
providers.

- Apache ActiveMQ
- Apache Qpid (AMQP)
- IBM WebsphereMQ
- SwiftMQ
- WebLogic

All the JMS related samples that come with Synapse assumes [ActiveMQ](http://activemq.apache.org)
to be the JMS broker. But they can be executed with any other JMS provider by making
a few simple changes to the JMS transport configuration in Synapse.

This article explains how to enable and setup the JMS transport for Synapse, the sample
Axis2 server and the sample client programs. Since the samples are mainly focusing
on ActiveMQ, much of this discussion will also be biased towards Apache ActiveMQ.

<a id="synapse-apache-org-userguide-samples-setup-jms--pre"></a>

## <a id="synapse-apache-org-userguide-samples-setup-jms--Prerequisites"></a>Prerequisites

First we need to install and start a JMS broker. The actual installation procedure
of the JMS broker may vary depending on the broker application. If ActiveMQ is used
as the JMS broker, you can install and run the broker by following 3 simple steps
given below.

1. [Download](http://activemq.apache.org/download.html) the latest Apache ActiveMQ binary distribution
2. Extract the downloaded archive to a suitable location on the local disk
3. Switch to the 'bin' directory of the installation and execute the startup script

Next we need to deploy the JMS client libraries into Synapse. Client libraries are
also specific to the JMS broker being used. These jar files are usually available in
the binary distribution of the JMS broker. Third party libraries such as JMS client
libraries are deployed into Synapse by simply copying them into the 'lib' directory
of Synapse. Therefore if we are to use ActiveMQ as the JMS broker, the following jar
files which can be found in the 'lib' directory of ActiveMQ installation, should be
copied into the 'lib' directory of Synapse.

ActiveMQ 5.8.0 and above

- activemq-broker-x.x.x.jar
- activemq-client-x.x.x.jar
- activemq-kahadb-store-x.x.x.jar
- geronimo-jms\_1.1\_spec-1.1.1.jar
- geronimo-j2ee-management\_1.1\_spec-1.0.1.jar
- geronimo-jta\_1.0.1B\_spec-1.0.1.jar
- hawtbuf-1.9.jar
- Slf4j-api-1.6.6.jar
- activeio-core-x.x.x.jar (available in AMQ\_HOME/lib/optional folder)

Earlier version of ActiveMQ

- activemq-core-x.x.x.jar
- geronimo-j2ee-management\_1.0\_spec-1.0.jar
- geronimo-jms\_1.1\_spec-1.1.1.jar

Now we are all set to enable the JMS transport receiver and sender for Synapse and
other sample applications.

<a id="synapse-apache-org-userguide-samples-setup-jms--synapse"></a>

## <a id="synapse-apache-org-userguide-samples-setup-jms--Enabling_JMS_Support_in_Synapse"></a>Enabling JMS Support in Synapse

The JMS transport of Synapse consists of two main components.

- JMS transport receiver (JMS listener)
- JMS transport sender (JMS sender)

<a id="synapse-apache-org-userguide-samples-setup-jms--listener"></a>

### <a id="synapse-apache-org-userguide-samples-setup-jms--Enabling_the_JMS_Listener"></a>Enabling the JMS Listener

If we want Synapse to receive messages from a JMS destination, then we should enable
the JMS transport receiver of Synapse. This can be done by editing the axis2.xml file
in the repository/conf directory and uncommenting the following XML fragment which
defines the JMS transport receiver configuration.

<!--Uncomment this and configure as appropriate for JMS transport support, after setting up your JMS environment (e.g. ActiveMQ)-->
<transportReceiver name="jms" class="org.apache.synapse.transport.jms.JMSListener">
<parameter name="myTopicConnectionFactory" locked="false">
<parameter name="java.naming.factory.initial" locked="false">org.apache.activemq.jndi.ActiveMQInitialContextFactory</parameter>
<parameter name="java.naming.provider.url" locked="false">tcp://localhost:61616</parameter>
<parameter name="transport.jms.ConnectionFactoryJNDIName" locked="false">TopicConnectionFactory</parameter>
</parameter>
<parameter name="myQueueConnectionFactory" locked="false">
<parameter name="java.naming.factory.initial" locked="false">org.apache.activemq.jndi.ActiveMQInitialContextFactory</parameter>
<parameter name="java.naming.provider.url" locked="false">tcp://localhost:61616</parameter>
<parameter name="transport.jms.ConnectionFactoryJNDIName" locked="false">QueueConnectionFactory</parameter>
</parameter>
<parameter name="default" locked="false">
<parameter name="java.naming.factory.initial" locked="false">org.apache.activemq.jndi.ActiveMQInitialContextFactory</parameter>
<parameter name="java.naming.provider.url" locked="false">tcp://localhost:61616</parameter>
<parameter name="transport.jms.ConnectionFactoryJNDIName" locked="false">QueueConnectionFactory</parameter>
</parameter>
</transportReceiver>

Please note that above configuration is for the AcitveMQ broker. If you are using some
other JMS provider, then the values of the parameters should be changed accordingly.

<a id="synapse-apache-org-userguide-samples-setup-jms--sender"></a>

### <a id="synapse-apache-org-userguide-samples-setup-jms--Enabling_the_JMS_Sender"></a>Enabling the JMS Sender

If you want to configure Synapse to send out JMS messages, then the JMS transport
sender must be enabled. This is done by uncommenting the following section in the
repository/conf/axis2.xml file.

<transportSender name="jms" class="org.apache.axis2.transport.jms.JMSSender">

Generally JMS transport sender is enabled by default in Synapse.

Synapse also comes with a simple Ant script that can be used to easily setup
and enable the JMS transport in Synapse. To try this out go to the samples/util
directory and execute the following command.

ant setupActiveMQ -Dactivemq.home=*<ActiveMQ home directory>*

This will copy the necessary dependencies into Synapse and update the axis2.xml
file accordingly. Instead of providing the ActiveMQ installation path as a system
property, you can opt to set the ACTIVEMQ\_HOME environment variable too.

<a id="synapse-apache-org-userguide-samples-setup-jms--server"></a>

## <a id="synapse-apache-org-userguide-samples-setup-jms--Enabling_JMS_Support_in_Axis2_Server"></a>Enabling JMS Support in Axis2 Server

Some of the Synapse samples involve Synapse sending messages to the Axis2 server
over JMS. For that we should enable the JMS transport listener and the sender for
the Axis2 server (sender is used to send back responses). Provided that all the
prerequisites are met, this can be done by uncommenting the JMS transport receiver
and sender configurations in the samples/axis2Server/repository/conf/axis2.xml file.
You will find that JMS sender is enabled for the Axis2 server by default.

You can also execute the following command from the samples/util directory to
enable the JMS transport for Axis2 in an automated fashion.

ant setupActiveMQ -Daxis2.xml=../axis2Server/repository/conf/axis2.xml

As in the case of Synapse, the default JMS listener configurations given in the
above axis2.xml file are for ActiveMQ. For other brokers, configuration should be
updated accordingly.

<a id="synapse-apache-org-userguide-samples-setup-jms--client"></a>

## <a id="synapse-apache-org-userguide-samples-setup-jms--Enabling_JMS_Support_in_Axis2_Client"></a>Enabling JMS Support in Axis2 Client

In some sample scenarios we have to send JMS requests using the Axis2 client.
In such cases we should enable the JMS transport sender for the sample client. This
can be done by uncommenting the JMS sender configuration in the
samples/axis2Client/client\_repo/conf/axis2.xml file. Generally this is enabled by
default and so you only need to meet the prerequisites described above.

---

<a id="synapse-apache-org-userguide-samples-setup-mail"></a>

# Apache Synapse – Apache Synapse - E-Mail Setup Guide

## <a id="synapse-apache-org-userguide-samples-setup-mail--E-Mail_Setup_Guide"></a>E-Mail Setup Guide

This document explains how to setup the mail transport sender and listener
as required by the samples.

## <a id="synapse-apache-org-userguide-samples-setup-mail--Contents"></a>Contents

- [Setting up Mail Transport Sender](#synapse-apache-org-userguide-samples-setup-mail--mailTransportSender)
- [Setting up Mail Transport Receiver](#synapse-apache-org-userguide-samples-setup-mail--mailTransportReceiver)

<a id="synapse-apache-org-userguide-samples-setup-mail--mailTransportSender"></a>

## <a id="synapse-apache-org-userguide-samples-setup-mail--Setting_up_Mail_Transport_Sender"></a>Setting up Mail Transport Sender

To enable the mail transport sender for samples, you need to uncomment
the mail transport sender configuration in the
repository/conf/axis2.xml. Uncomment the mail transport sender sample
configuration and make sure it points to a valid SMTP configuration for
any actual scenarios.

<transportSender name="mailto" class="org.apache.synapse.transport.mail.MailTransportSender">
<parameter name="mail.smtp.host">smtp.gmail.com</parameter>
<parameter name="mail.smtp.port">587</parameter>
<parameter name="mail.smtp.starttls.enable">true</parameter>
<parameter name="mail.smtp.auth">true</parameter>
<parameter name="mail.smtp.user">synapse.demo.mail1</parameter>
<parameter name="mail.smtp.password">mailpassword</parameter>
<parameter name="mail.smtp.from">synapse.demo.mail1@gmail.com</parameter>
</transportSender>

<a id="synapse-apache-org-userguide-samples-setup-mail--mailTransportReceiver"></a>

## <a id="synapse-apache-org-userguide-samples-setup-mail--Setting_up_Mail_Transport_Receiver"></a>Setting up Mail Transport Receiver

To enable the mail transport receiver for samples, you need to
uncomment the mail transport receiver configuration in the
configuration. Note: you need to provide correct parameters for a valid
mail account at service level.

<transportReceiver name="mailto" class="org.apache.axis2.transport.mail.MailTransportListener"></transportReceiver>

---

<a id="synapse-apache-org-userguide-samples-setup-script"></a>

# Apache Synapse – Apache Synapse - Script Setup Guide

## <a id="synapse-apache-org-userguide-samples-setup-script--Script_Setup_Guide"></a>Script Setup Guide

Apache Synapse ships with a set of scripting samples. This document explains
how to setup the necessary script engines for these samples. In addition this
guide describes how to setup the JSON message builder and formatter for JSON
mediation samples.

## <a id="synapse-apache-org-userguide-samples-setup-script--Contents"></a>Contents

- [Introduction](#synapse-apache-org-userguide-samples-setup-script--intro)
- [JavaScripts Support](#synapse-apache-org-userguide-samples-setup-script--javaScript)
- [Ruby Support](#synapse-apache-org-userguide-samples-setup-script--ruby)
- [Python Support](#synapse-apache-org-userguide-samples-setup-script--python)
- [JSON Support on Synapse 3.0.0](#synapse-apache-org-userguide-samples-setup-script--json-syn3)
- [JSON Support Prior to Synapse 3.0.0](#synapse-apache-org-userguide-samples-setup-script--json)

<a id="synapse-apache-org-userguide-samples-setup-script--intro"></a>

## <a id="synapse-apache-org-userguide-samples-setup-script--Configuring_Synapse_for_Script_Mediator_Support"></a>Configuring Synapse for Script Mediator Support

The Synapse Script Mediator is a Synapse extension, and thus all
prerequisites are not bundled by default with the Synapse
distribution.Before you use some script mediators you may need to
manually add the required jar files to the Synapse lib directory, and
optionally perform other installation tasks as may be required by the
individual scripting language. This is explained in the following
sections.

<a id="synapse-apache-org-userguide-samples-setup-script--javaScript"></a>

### <a id="synapse-apache-org-userguide-samples-setup-script--JavaScript_Support"></a>JavaScript Support

The JavaScript/E4X support is enabled by default and comes
ready-to-use with the Synapse distribution.

<a id="synapse-apache-org-userguide-samples-setup-script--ruby"></a>

### <a id="synapse-apache-org-userguide-samples-setup-script--Ruby_Support"></a>Ruby Support

For Ruby support you need to download the 'jruby-complete.jar'
from the Maven repository for JRuby, and copy it into the 'lib'
folder of Synapse . The JRuby JAR can be downloaded from
[
here
](http://repo2.maven.org/maven2/org/jruby/jruby-complete/1.3.0/jruby-complete-1.3.0.jar)

<a id="synapse-apache-org-userguide-samples-setup-script--python"></a>

### <a id="synapse-apache-org-userguide-samples-setup-script--Python_Support"></a>Python Support

For Python support you need to download the 'jython.jar'
from the Maven repository for Jython, and copy it into the 'lib'
folder of Synapse . The Jython JAR can be downloaded from
[
here
](http://central.maven.org/maven2/org/python/jython/2.2.1/jython-2.2.1.jar)

<a id="synapse-apache-org-userguide-samples-setup-script--json-syn3"></a>

### <a id="synapse-apache-org-userguide-samples-setup-script--JSON_Support_on_Synapse_3.0.0"></a>JSON Support on Synapse 3.0.0

[JSON](http://json.org)
is a lightweight data-interchange format.
It can be used as an alternative to XML or SOAP. From Synapse 3.0.0 onward, there are no additional
steps required to enable JSON.

<a id="synapse-apache-org-userguide-samples-setup-script--json"></a>

### <a id="synapse-apache-org-userguide-samples-setup-script--JSON_Support_Prior_to_Synapse_3.0.0"></a>JSON Support Prior to Synapse 3.0.0

To enable JSON
support on Synpase versions prior to 3.0.0, the following two jar files should be deployed into the 'lib'
directory of Synapse.

- [axis2-json.jar](http://repo1.maven.org/maven2/org/apache/axis2/axis2-json)
- [jettison.jar](http://central.maven.org/maven2/org/codehaus/jettison/jettison/)

Jettison 1.1 is recommended.

Having deployed the necessary libraries you should now register the JSON message
builder and formatter with Synapse. Open up 'repository/conf/axis2.xml' file
of Synapse and add the following two entries under the 'messageBuilders' and
'messageFormatters' sections respectively.

<messageBuilder contentType="application/json"
class="org.apache.axis2.json.JSONOMBuilder"/>
<messageFormatter contentType="application/json"
class="org.apache.axis2.json.JSONMessageFormatter"/>

If you are planning to run [sample 158](#synapse-apache-org-userguide-samples-sample158), you should also add the above two entries
to the 'samples/axis2Client/client\_repo/conf/axis2.xml' file.

---

<a id="synapse-apache-org-userguide-samples-setup-tcp_udp"></a>

# Apache Synapse – Apache Synapse - TCP/UDP Setup Guide

## <a id="synapse-apache-org-userguide-samples-setup-tcp_udp--TCPUDP_Setup_Guide"></a>TCP/UDP Setup Guide

This document explains how to setup the transport sender and listener
for TCP/UDP transports required by the samples.

## <a id="synapse-apache-org-userguide-samples-setup-tcp_udp--Contents"></a>Contents

- [Introduction](#synapse-apache-org-userguide-samples-setup-tcp_udp--intro)
- [Setting Up the TCP Transport](#synapse-apache-org-userguide-samples-setup-tcp_udp--tcp)
- [Setting Up the UDP Transport](#synapse-apache-org-userguide-samples-setup-tcp_udp--udp)

<a id="synapse-apache-org-userguide-samples-setup-tcp_udp--intro"></a>

## <a id="synapse-apache-org-userguide-samples-setup-tcp_udp--Introduction"></a>Introduction

Apache Synapse is capable of sending and receiving messages over raw TCP and UDP.
Any messages received over these transports can be mediated using the usual set of
mediators and can be forwarded over different protocols such as HTTP ans JMS. The
transport adapters for TCP and UDP are not available in the Synapse binary
distribution by default. This guide will help you to download and setup them in
Synapse ESB.

<a id="synapse-apache-org-userguide-samples-setup-tcp_udp--tcp"></a>

## <a id="synapse-apache-org-userguide-samples-setup-tcp_udp--Setting_Up_the_TCP_Transport"></a>Setting Up the TCP Transport

To enable the TCP transport for Synapse, first you need to download the Axis2 TCP
transport jar, and copy it to the 'lib' directory of Synapse. This library can be
downloaded from the [WS-Commons Transports](http://ws.apache.org/commons/transport)
website. Then open up the axis2.xml file and uncomment the TCP transport receiver
and sender configurations.

<transportReceiver name="tcp" class="org.apache.axis2.transport.tcp.TCPServer">
<parameter name="port">6060</parameter>
</transportReceiver>

<transportSender name="tcp" class="org.apache.axis2.transport.tcp.TCPTransportSender"/>

The above configuration enables Synapse to receive raw TCP messages on port 6060.
Since no application level headers are available on such requests, Synapse will be
solely depending on the addressing headers or the XML payload of the messages to
find the target service for TCP requests.

In some of the samples you will have to send raw TCP messages using the sample
Axis2 client. In that case you should enable the TCP transport sender for the
sample client. This can be done by uncommenting the following entry in the
samples/axis2Client/client\_repo/conf/axis2.xml file.

<transportSender name="tcp" class="org.apache.axis2.transport.tcp.TCPTransportSender"/>

<a id="synapse-apache-org-userguide-samples-setup-tcp_udp--udp"></a>

## <a id="synapse-apache-org-userguide-samples-setup-tcp_udp--Setting_Up_the_UDP_Transport"></a>Setting Up the UDP Transport

Enabling the UDP transport for Synapse is similar to enabling the TCP transport. You
should [download](http://ws.apache.org/commons/transport) the Axis2 UDP
transport jar and copy it into 'lib' directory of Synapse. Then uncomment the
following entries in the repository/conf/axis2.xml file to enable the UDP listener
and sender.

<transportReceiver name="udp" class="org.apache.axis2.transport.udp.UDPListener"/>

<transportSender name="udp" class="org.apache.axis2.transport.udp.UDPSender"/>

To send UDP messages from the sample client, enable the UDP transport sender for the
client in samples/axis2Client/client\_repo/conf/axis2.xml file.

---

<a id="synapse-apache-org-userguide-samples-sample13"></a>

# Apache Synapse - Sample 13

## Sample 13: Dual Channel Invocations Through Synapse<a id="synapse-apache-org-userguide-samples-sample13--Sample_13:_Dual_Channel_Invocations_Through_Synapse"></a>

<definitions xmlns="http://ws.apache.org/ns/synapse">
<sequence name="main">
<!-- log all attributes of messages passing through -->
<log level="full"/>
<!-- Send the message to implicit destination -->
<send/>
<sequence/>
</definitions>

### Objective<a id="synapse-apache-org-userguide-samples-sample13--Objective"></a>

Demonstrate the ability to perform dual channel invocations through
Synapse (asynchronous notification).

### Pre-requisites<a id="synapse-apache-org-userguide-samples-sample13--Pre-requisites"></a>

- Deploy the SimpleStockQuoteService in the sample Axis2 server and start Axis2
- This sample makes use of the configuration used in [sample 0](#synapse-apache-org-userguide-samples-sample0).
  So start Synapse using the configuration numbered 0 (repository/conf/sample/synapse\_sample\_0.xml)

  Unix/Linux: sh synapse.sh -sample 0  
  Windows: synapse.bat -sample 0

### Executing the Client<a id="synapse-apache-org-userguide-samples-sample13--Executing_the_Client"></a>

Execute the client as follows.

ant stockquote -Daddurl=http://localhost:9000/services/SimpleStockQuoteService -Dtrpurl=http://localhost:8280/

This example invokes the same 'getQuote' operation on the SimpleStockQuoteService
using the custom client which uses the Axis2 ServiceClient API with useSeparateListener
set to 'true', so that the response is coming through a different channel than
the one which is used to send the request. Note the following log thrown out
by the sample client.

Response received to the callback
Standard dual channel :: Stock price = $57.16686934968289

If you send your client request through TCPmon, you will notice that Synapse replies
to the client with a HTTP 202 acknowledgment when you send the request and the
communication between Synapse and the server happens on a single channel and then
you get the response back from Synapse to the client callback in a different channel
(which cannot be observed through TCPmon).

Also you could see the wsa:Reply-To header being something like
http://localhost:8200/axis2/services/anonService2 which implies that the reply
is gpoing on a different channel listening on the port 8200. Please note that it
is required to engage addressing when using the dual channel invocation because
it requires the wsa:Reply-To header.

[Back to Catalog](#synapse-apache-org-userguide-samples)