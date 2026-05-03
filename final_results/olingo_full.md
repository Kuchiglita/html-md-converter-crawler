<a id="olingo-apache-org-doc-odata4-overview"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-overview--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-overview--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-overview--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# Java Library for OData Version 4

- [Overview](#olingo-apache-org-doc-odata4-overview--overview "Overview")
- [Entity Data Model (Metadata)](#olingo-apache-org-doc-odata4-overview--entity-data-model-metadata "Entity Data Model (Metadata)")
- [Run-Time Processing of Requests](#olingo-apache-org-doc-odata4-overview--run-time-processing-of-requests "Run-Time Processing of Requests")
  - [General Setup](#olingo-apache-org-doc-odata4-overview--general-setup "General Setup")
    - [Design Considerations](#olingo-apache-org-doc-odata4-overview--design-considerations "Design Considerations")
    - [Overall Handling](#olingo-apache-org-doc-odata4-overview--overall-handling "Overall Handling")
    - [Processor Interfaces and Processing Methods](#olingo-apache-org-doc-odata4-overview--processor-interfaces-and-processing-methods "Processor Interfaces and Processing Methods")
  - [Content Negotiation](#olingo-apache-org-doc-odata4-overview--content-negotiation "Content Negotiation")
    - [Request Content Type](#olingo-apache-org-doc-odata4-overview--request-content-type "Request Content Type")
    - [Response Content Type](#olingo-apache-org-doc-odata4-overview--response-content-type "Response Content Type")
  - [Exceptions](#olingo-apache-org-doc-odata4-overview--exceptions "Exceptions")
  - [Response Status Code](#olingo-apache-org-doc-odata4-overview--response-status-code "Response Status Code")
  - [Helper Methods](#olingo-apache-org-doc-odata4-overview--helper-methods "Helper Methods")
    - [Serialization](#olingo-apache-org-doc-odata4-overview--serialization "Serialization")
      - [Serializers Dependent on Content Negotiation](#olingo-apache-org-doc-odata4-overview--serializers-dependent-on-content-negotiation "Serializers Dependent on Content Negotiation")
      - [Fixed-Format Serializers](#olingo-apache-org-doc-odata4-overview--fixed-format-serializers "Fixed-Format Serializers")
    - [Deserialization](#olingo-apache-org-doc-odata4-overview--deserialization "Deserialization")
    - [URI-related Tasks](#olingo-apache-org-doc-odata4-overview--uri-related-tasks "URI-related Tasks")
      - [Context URL](#olingo-apache-org-doc-odata4-overview--context-url "Context URL")
      - [Canonical URL](#olingo-apache-org-doc-odata4-overview--canonical-url "Canonical URL")
      - [URI parser](#olingo-apache-org-doc-odata4-overview--uri-parser "URI parser")
    - [Helpers for Concurrency Control](#olingo-apache-org-doc-odata4-overview--helpers-for-concurrency-control "Helpers for Concurrency Control")
    - [Helpers for Preference Handling](#olingo-apache-org-doc-odata4-overview--helpers-for-preference-handling "Helpers for Preference Handling")
    - [Debug Output](#olingo-apache-org-doc-odata4-overview--debug-output "Debug Output")
- [Definition of the Web Infrastructure](#olingo-apache-org-doc-odata4-overview--definition-of-the-web-infrastructure "Definition of the Web Infrastructure")

## Overview

The Open Data Protocol (OData) is a web-based protocol for querying and
updating data. It has been defined initially by Microsoft but is now an
[OASIS standard](https://www.oasis-open.org/committees/tc_home.php?wg_abbrev=odata).
This allows anyone to freely interoperate with OData implementations. Data
exposed via the OData standard can be consumed in any environment offering
HTTP-based connectivity. In addition, there are client SDKs available for
various platforms such as .Net, Java, PHP, JavaScript, etc.

For the Java platform, the [Apache Olingo project](/)
offers a library useful for implementing an OData service. It provides
services such as URL parsing, input validation, (de-)serialization of
content, request dispatching, etc., according to the OData specification.

The main parts of an OData service implementation are the metadata
definition, the run-time processing of requests, and the definition of the
web infrastructure. These parts will now be described in more detail.

## Entity Data Model (Metadata)

The Entity Data Model (EDM) is the underlying metadata model of the OData
protocol. Within the EDM the following (main) elements are described:

- Schemas
- Entity Types
- Complex Types
- Type Definitions
- Enum Types
- Properties
- Navigation Properties
- Actions
- Functions
- Entity Container
  - Entity Sets
  - Singletons
  - Action Imports
  - Function Imports

A proper OData Service requires a valid and consistent EDM. In order to speed
up performance, the OData Library does no validation of the EDM.

The standard way of defining the metadata is to write code in so-called
EDM provider classes. It would be possible to add other sources for
metadata, e.g., a predefined metadata document.

EDM provider classes separate the run-time EDM object instances from code
that defines OData EDM objects. All provider classes simply define string
values for the different EDM elements. At run-time, objects are created only
as far as necessary. Since the typical request (apart from the request for
the metadata document, of course) can be processed with only the directly
involved EDM objects, this can be a significant performance improvement.
Furthermore, this solves the problem that many EDM objects depend on each
other, making it no easy task to find the right order of object
instantiation.

Service implementations can derive from `CsdlAbstractEdmProvider` and
override only those methods that should provide EDM objects. Almost all
provider methods have a parameter of type `FullQualifiedName` that specifies
for which (namespace-qualified) name a provider-object instance is to be
returned. In addition, there are some methods with the purpose of retrieving
a list of all names of a given object type; these are used only for the
output of the metadata document.

## Run-Time Processing of Requests

### General Setup

#### Design Considerations

The OData standard describes many different types of requests that have to be
answered by an OData service implementation. It would not be useful to have a
single processing interface or even a single method that a service
implementation has to implement. Different designs are possible how the
multitude of requests can be split into more manageable parts.

This library has been designed to handle a given OData request in a single
call to a processing method, including the complete query-options part. Which
method the request dispatcher calls is decided according to the HTTP method
and the representation type of the expected response. The HTTP method
describes the fundamental type of operation: `GET` for read requests, `PUT`
or `PATCH` for update requests, `POST` for create requests, and `DELETE` for
deletion requests. The representation type describes which information can be
retrieved from the OData service: an Entity representation, or only a link
URL, or even just a simple number as a count of entities.

The fact that a single method call occurs for a given request, as complicated
as it may be, leads immediately to the consequence that almost all processing
methods must be prepared to handle things like navigation and system query
options. Navigation in turn means that even if the response in the end may be
a simple count, the implementation may have to read entity collections, their
relations, and even function imports or bound functions that may occur before
the final `$count` in the request URI. Services therefore have to be
implemented carefully in order to respond to requests they are not prepared
to handle with a `Not Implemented` response and its corresponding HTTP status
code of 501.

A major advantage of the one-step approach is that the processing interfaces
don't have to make any assumptions how application data is to be transported.
The serialization and deserialization helper methods of the library of course
use library-defined data objects but service implementations are free to use
their own code for that tasks and still can use the core run-time library
functionality.

Many implementations will re-use large parts of code for essentially the same
requests that differ only in the returned representation. But there might be
optimizations: An SQL request could be much more efficient for count
determination if not all entities are retrieved first. Since this library does
not favor a specific implementation, the interface is designed to allow the
necessary flexibility.

#### Overall Handling

Processor classes have to be registered in order to be called from the
library core at run-time. The core run-time registers the `DefaultProcessor`
API class with default implementations of `MetadataProcessor`,
`ServiceDocumentProcessor`, and `ErrorProcessor`, but this can be overridden
simply by registering an own implementation.

Each class can implement one or more of the processor interfaces; all
processing methods have unique names across all processor interfaces.

Since the general design of the library is to have as little constraints as
possible, as little as possible happens automatically. The service developer
is responsible for the correct response to a given request. There are several
helper methods to ease this task considerably. But if you are not satisfied
with them, it is always possible to use your own implementation, even if that
does not conform to the OData standard.

#### Processor Interfaces and Processing Methods

All processor interfaces derive from the `Processor` interface which defines
a single `init()` method allowing to get a run-time instance of the `OData`
object and the `ServiceMetadata`. The `OData` object is the root object for
serving factory tasks and the single instance connecting the API interfaces
with their implementations; each thread (processing of one request) should
keep its own instance. The `ServiceMetadata` instance contains the Entity
Data Model and some other objects related to metadata.

All processing methods have at least `ODataRequest` and `ODataResponse`
objects as parameters. They are supposed to find all necessary information
about the request body and its headers in the request object and set the
corresponding information in the response object.

Processing methods that are not called with a static URI like `$batch`
additionally have a `UriInfo` parameter describing the request URI.

Processing methods that have to read request-body content additionally have a
`ContentType` parameter describing the request-body format in order to select
the correct deserializer.

Processing methods that have to deliver content in the response body
additionally have a `ContentType` parameter describing the requested format
in order to select the correct serializer.

An example with all parameters mentioned above is the method `createEntity()`
in the interface `EntityProcessor`:

```java
void createEntity(ODataRequest request, ODataResponse response,
    UriInfo uriInfo, ContentType requestFormat, ContentType responseFormat)
    throws ODataApplicationException, ODataLibraryException;
```

### Content Negotiation

#### Request Content Type

Requests that send data must provide a `Content-Type` HTTP header.
This is checked in the core run-time code. For each representation type
there is a list of content types that are supported by the implementation.

The method `modifySupportedContentTypes()` of the interface
`CustomContentTypeSupport` can be implemented to change that; it is even
possible to remove library-supported content types. Any implementation of
this interface has to be registered in the same way as processor classes in
order to have any effect.

Request content types that do not match the supported content types are
rejected with error messages and the HTTP status code 406 (Not Acceptable).

#### Response Content Type

A request for a response with a content type not matching the supported
content types is rejected with an error message and the HTTP status code 415
(Unsupported Media Type).

The list of supported content types can be modified as described for the
request content type. It is not possible to have different support for
responses than for requests.

### Exceptions

All processing methods can and should throw exceptions if something went
wrong.

Almost all library utility methods declare exceptions that derive from
`ODataLibraryException`. Those exceptions are handled by the core run-time
code which takes care of setting the correct response status code and error
text.

If a service implementation wants to signal an error itself, it can throw an
`ODataApplicationException`. The constructor of this exception allows to
define the response status code and the error text. Please note that the
OData standard mandates specific status codes in many places; this is not
enforced by the library.

If a `RuntimeException` occurs, the core run-time code sets the response
status code to 500 (Internal Server Error) and delivers a corresponding error
text.

### Response Status Code

Service implementations have to set the response status code explicitly.
Failing to do so results in a status code of 500 (Internal Server Error).
Please note that the OData standard mandates specific status codes in many
places; this is not enforced by the library.

### Helper Methods

#### Serialization

##### Serializers Dependent on Content Negotiation

For some representation types the OData standard defines different
representations, e.g., JSON and XML. For the JSON format there are different
sub-formats differing in the amount of metadata that is part of the content.

In order to have a uniform interface and to relieve the service
implementations from many switch statements, it is possible to get the
correct serializer from the `OData` object's `createSerializer()` method that
has a content type as parameter. The requested response content type is
passed as parameter to all processing methods that are supposed to create
response content.

The `ODataSerializer` interface has methods to serialize the following types
of content:

- service document
- metadata document
- error document
- entity
- entity collection
- primitive value
- collection of primitive values
- complex value
- collection of complex values
- reference to an entity
- collection of references to entities

Every data serializer gets its run-time data as an instance of a type defined
in the `commons.api.data` package. It has also an additional parameter to
pass options like the context URL or expand settings in a single object.
Please note that according to the OData standard the context URL is mandatory
for service responses except for responses with no metadata at all.

##### Fixed-Format Serializers

Fixed-format serializers can be used for formats defined in the OData
standard that are not subject to content negotiation. The
`FixedFormatSerializer` instance to be got from the `OData` object's
`createFixedFormatSerializer()` method has methods for serializing a binary,
a count, a primitive raw value, a batch, and an asynchronous response.

#### Deserialization

Deserialization works along the same principles as serialization, with
minor differences, however.

There is no deserializer for the representation types service document,
metadata document, and error document. There is an additional representation
type that can be deserialized: action parameters.

All content-type-dependent methods don't return data objects directly but
instances of `DeserializerResult` instead. This makes it possible to return
additional information like the expand information.

#### URI-related Tasks

##### Context URL

The context URL is a mandatory part of all data-related responses of an OData
service except for responses with no metadata at all. In some cases it cannot
be determined from the data alone that is passed to the serialization
methods. Therefore, the serialization methods expect the correct context URL
as parameter.

The `ContextURL` object has its own builder that allows to build the context
URL from its parts. For the difficult parts the `UriHelper` instance to be
got from the `OData` object's `createUriHelper()` method has helper methods
that assist with building select lists and key predicates.

##### Canonical URL

The `UriHelper` instance to be got from the `OData` object's
`createUriHelper()` method has a helper method to construct the canonical URL
of an entity. This URL can be used as `Location` HTTP header, for example.

##### URI parser

The `UriHelper` instance to be got from the `OData` object's
`createUriHelper()` method has a helper method to parse the entity-ID URI of
an entity. This method can be used in the handling of reference-changing
requests.

#### Helpers for Concurrency Control

The OData standard defines optimistic concurrency control, a mechanism to
ensure that a modification request accesses the current version of the data
to be modified. Furthermore, this mechanism can be used to enable client-side
caching, using the information that the requested data are still current.

To achieve this, OData uses weak entity tags via the `ETag` HTTP response
header and its associated `If-Match` and `If-None-Match` HTTP request
headers.

To enable this functionality for run-time data, service implementations can
register a class that implements the `CustomETagSupport` interface with its
two methods for entities and media-entity data. These methods should return
for a given entity-set or singleton name whether the service supports entity
tags for entities out of this entity set or singleton. No finer granularity
is possible currently. Processing methods still have to check the ETag(s)
themselves; the `ETagHelper` instance to be got from the `OData` object's
`createETagHelper()` method has two ready-made methods for read and change
requests, respectively.

To enable this functionality for metadata, i.e., for the service document and
the metadata document, service implementations can pass an instance of an
implementation of the `ServiceMetadataETagSupport` interface to the
`ServiceMetadata` creation described above. The default processors for
service- and metadata document requests already take this ETag support into
account.

#### Helpers for Preference Handling

Some aspects of OData request processing can be influenced from a client by
setting preferences in the HTTP `Prefer` header. The service implementation
is not obliged to honor these preferences. If it does, it should respond with
an appropriate `Preference-Applied` HTTP header.

The `Preferences` instance to be got from the `OData` object's
`createPreferences()` method with the HTTP `Prefer` headers as parameter
has named access methods for the preferences defined in the OData standard
plus generic access to other preferences.

The `PreferencesApplied` API class has a builder that helps building a
correct `Preference-Applied` HTTP response header.

#### Debug Output

For support purposes there is a possibility to enrich the service response
with additional data helpful for finding bugs. Please note that this
information could also help attackers.

To enable this functionality, service implementations can register a class
that implements the `DebugSupport` interface with its two methods for user
authorization and creation of output.

A ready-made `DefaultDebugSupport` class is already provided where all users
are always authorized and the additional data consists of information about
the request, the response, the parsed request URI, the server environment,
library timings, and the stacktrace in case an error occurred, in a
self-contained HTML document ready for browser usage or in a JSON document.
The `OData` object's `createDebugResponseHelper()` method returns a
`DebugResponseHelper` instance which is used in this default implementation.

To request the debug output for a request to the OData service the query parameter
`odata-debug=html` must be appended to the original request URL
(e.g. `http://localhost:8080/odata-server-tecsvc/odata.svc/?odata-debug=html` for a local published test service).

## Definition of the Web Infrastructure

To enable an OData service on a web server, the service is wrapped by a web
application.

The web application is defined in a `web.xml` file where a servlet is
registered. The servlet is a standard `HttpServlet` configured to dispatch
all requests to URLs below the service's root URL to the OData handler class.
This is done by overriding the servlet's `service()` method. The library
provides an `ODataHttpHandler` object, creatable from the `OData` API class
with the EDM definition as parameter, that can be used inside the `service()`
method.

It receives the request and delegates it to the processor implementation of
the OData service if the URL conforms to the OData specification. This means
that all processor implementations of the OData service have to be registered
with the `register()` method. The responses of the registered handlers are
given back to the servlet infrastructure and will result in corresponding
HTTP responses.

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-download"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-download--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-download--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-download--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# Download OData 4.0 Java Library

Apache Olingo OData4 is a collection of Java libraries for
implementing [OData V4](https://odata.org) protocol clients or servers.

### Release 5.0.0 (2023-12-18)

[Full download page](https://downloads.apache.org/olingo/odata4/5.0.0/) and [release notes](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12314520&version=12353663).

The Apache *Olingo OData 4 5.0.0* release is the latest stable release for the *OData V4* specifications.

### Commodity Packages

| Package | zip | Description |
| --- | --- | --- |
| Olingo OData Sources | Download(sha512,pgp) | Complete source code. |
| Olingo OData Docs | Download(sha512,pgp) | Documentation and JavaDoc. |
| Olingo OData Server for Java | Download(sha512,pgp) | All you need to implement an OData V4 Java server. |
| Olingo OData Server Extension for Java | Download(sha512,pgp) | Convenience API to implement an OData V4 Java server. |
| Olingo OData Client for Java | Download(sha512,pgp) | All you need to implement an OData V4 Java client. |
| Olingo OData Client for Android | Download(sha512,pgp) | All you need to implement an OData V4 Android client. |

### Maven

The Apache Olingo 5.0.0 only artifacts are also available at [Maven Central](https://search.maven.org/#search%7Cga%7C1%7Cg:%22org.apache.olingo%22%20AND%20v:%225.0.0%22).
All Apache Olingo OData 4 artifacts are available at [Maven Central](https://search.maven.org/#search%7Cga%7C1%7Corg.apache.olingo).
For POM dependencies see [here](#olingo-apache-org-doc-odata4-maven).

### Older Releases

For older releases please refer to [Archives](https://archive.apache.org/dist/olingo/)
or you can get them [using Maven](#olingo-apache-org-doc-odata4-maven).

### Verify Authenticity of Downloads package

While downloading the packages, make yourself familiar
on how to verify their integrity, authenticity and provenience
according to the Apache Software Foundation best practices.
Please make sure you check the following resources:

- [Artifact verification](/verification.html) details
- Developers and release managers PGP keys are publicly available here: [KEYS](https://downloads.apache.org/olingo/KEYS).

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-index"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-index--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-index--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-index--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# Documentation OData 4.0 Java Library

## General Documentation

- [Documentation](#olingo-apache-org-doc-odata4-overview)

## How to Start

- [Maven Dependencies](/doc/odata4/dependencies.html)
- [QuickStart with an existing sample service](#olingo-apache-org-doc-odata4-tutorials-od4_quick_start_sample)

## Tutorials (OData 4.0)

### Olingo for Client usage

- [Client sample tutorial](#olingo-apache-org-doc-odata4-tutorials-od4_basic_client_read)
- [Batch Client API](#olingo-apache-org-doc-odata4-tutorials-od4_basic_batch_client)

### Olingo for Server usage

- Basic Tutorial: Create an OData V4 Service with Olingo
  - [Prerequisites for all tutorial parts](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
  - [Tutorial Part 1: Create a read service (with Eclipse)](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
    - [Tutorial Appendix: Use Maven for running the created read service](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read_mvn)
  - [Tutorial Part 2: Extend the read service (with Eclipse)](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
  - [Tutorial Part 3: Add write support to the service (with Eclipse)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
  - [Tutorial Part 4: Add navigation support to the service (with Eclipse)](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
  - Tutorial Part 5: Add system query options support to the service (with Eclipse)
    - [Tutorial Part 5.1: Add system query options support (`$top`, `$count` and `$skip`) to the service (with Eclipse)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
    - [Tutorial Part 5.2: Add system query options support (`$select` and `$expand`) to the service (with Eclipse)](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
    - [Tutorial Part 5.3: Add system query options support (`$orderby`) to the service (with Eclipse)](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
    - [Tutorial Part 5.4: Add system query options support (`$filter`) to the service (with Eclipse)](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
  - [Tutorial Part 6: Add Action Imports and Function Imports to the service (with Eclipse)](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
  - [Tutorial Part 7: Add Media entities to the service (with Eclipse)](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
  - [Tutorial Part 8: Add Batch request support to the service](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
  - [Tutorial Part 9: Add "deep insert" handling to the service](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)
  - [Tutorial Part 10: Add "Bound Actions" to the service](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action)
- Extended Tutorials
  - [Enable *Entity Collection* streaming support](#olingo-apache-org-doc-odata4-tutorials-streaming-tutorial_streaming)

## Javadoc

- [Javadoc for OData 4.0 Library](/javadoc/odata4/index.html)

## Olingo Project Setup (Contributors)

- [Git and Maven Support](#olingo-apache-org-doc-odata4-maven)
- [Eclipse IDE Support](#olingo-apache-org-doc-odata4-eclipse)
- [Release Documentation](#olingo-apache-org-doc-odata4-release)

## External Articles, Blog-Entries and Tutorials

- [JPA Extension library for Olingo V4](https://github.com/SAP/olingo-jpa-processor-v4)
- Series of blog articles which show a tutorial on how to use Olingo as client library
  - [Connection and Data-Exchange Java with Microsoft Dynamics with Olingo V4.5.0](https://egmont-petersen.nl/mesmerize/java-html/connection-and-data-exchange-java-with-microsoft-dynamics/)
  - [Accessing data of OData v4 services with Olingo](https://templth.wordpress.com/2014/12/03/accessing-odata-v4-service-with-olingo/)
  - [Manipulating data of OData v4 services with Olingo](https://templth.wordpress.com/2014/12/05/manipulating-data-of-odata-v4-services-with-olingo/)
  - [Updating data links of OData v4 services with Olingo](https://templth.wordpress.com/2014/12/08/updating-data-links-of-odata-v4-services-with-olingo/)
- Blog articles which show different topics on how to use Olingo as server library
  - [Implementing an OData service with Olingo](https://templth.wordpress.com/2015/04/27/implementing-an-odata-service-with-olingo/)
  - [Handling OData queries with ElasticSearch](https://templth.wordpress.com/2015/04/03/handling-odata-queries-with-elasticsearch/)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-eclipse"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-eclipse--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-eclipse--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-eclipse--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# Eclipse IDE Support

---

[Eclipse](https://eclipse.org) (in version `Juno` and `Kepler`) is supported by the project.

With the Maven project files can be generated with:

`mvn eclipse:clean eclipse:eclipse`

As result each Maven module will get a consistent `.project`, `.classpath` and `.settings` file with which each module can be imported as existing project to Eclipse.

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-maven"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-maven--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-maven--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-maven--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# Building the Olingo Project

---

The project uses Git for source code management and version control and uses Maven for build.

### Clone the project

To get the code for just clone the project from:

##### OData 4.0 repository

```text
git clone https://gitbox.apache.org/repos/asf/olingo-odata4
```

### Building the Project

The project builds with Maven. Simply call

```bash
mvn clean install
```

to run the build, execute JUnit tests and create all artifacts.

Further information to Git and Maven is available here:

- [https://git-scm.com/](https://git-scm.com/)
- [https://maven.apache.org](https://maven.apache.org)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-release"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-release--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-release--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-release--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# Apache Olingo Release Documentation

---

### Introduction

This document describes the release guidelines for Apache Olingo. It heavily refers
to [standard Apache procedures to release](https://maven.apache.org/developers/release/apache-release.html)
Maven based projects at Apache.

### Build Environments

Apache Olingo is built and released with [Maven 3](https://maven.apache.org) and uses
the [Apache POM version 16](https://svn.apache.org/repos/asf/maven/pom/tags/apache-16/pom.xml).

### Release Artifacts

An Apache Olingo release consists of:

- The main POMs/JARs/WARs built as part of the Maven build process.
  For an overview on the released modules see artifacts with *groupId*: `org.apache.olingo` in the [Apache Maven Repository](https://repository.apache.org/index.html#nexus-search;gav%7Eorg.apache.olingo%7E%7E4.0.0%7E%7E).
  In detail the following artifacts are produced for each release module:
  - **Main artifact**: `<artifactId>-<version>.<ext>`
  - **Source artifact**: `<artifactId>-<version>-sources.<ext>`
  - **Javadoc artifact**: `<artifactId>-<version>-javadoc.<ext>`
  - **POM**: `<artifactId>-<version>.pom`

Also the following additional *distribution commodity packages* are
provided as part of the release:

- `Olingo-OData-${version}-source-release.${ext}`   
   A source-release
  bundle containing all files the sources necessary to build all other artifacts.   
   **Package formats**: zip.
- `Olingo-OData-JavaDoc-${version}-javadoc.${ext}`   
   A bundle
  containing JavaDoc of the OData4 library API and annotations, the
  JPA processor API as well as additional documentation and reference scenario
  examples.   
   **Package formats**: zip.
- `Olingo-OData-Server-for-Java-${version}-lib.${ext}`   
   A bundle containing the OData4 core
  library and dependencies required to implement an OData V4 Java server/processor.   
   **Package formats**: zip.
- `Olingo-OData-Client-for-Java-${version}-jpa.${ext}`   
   A bundle containing the OData4 client library
  and dependencies required to implement an OData V4 Java client.   
   **Package formats**: zip.
- `Olingo-OData-Client-for-Android-${version}-lib.${ext}`   
   A bundle containing the OData4 client library
  and dependencies required to implement an OData V4 client for Android devices.   
   **Package formats**: zip.

### Documentation and JavaDoc

The documentation that will be part of the release must match the code.
All examples in the documentation must work. The Java package documentation must be
up-to-date. Release independend documentation is maintained on the [Apache Olingo Documentation](/documentation.html) page.

### Preparation

##### Release Manager

A release manager must be appointed for a release. He or she is in charge of the release process,
following the guidelines and eventually generating the release artifacts.
The release manager might tailor the process for a specific release.

##### Version

The Olingo community decides if the release will be a major or a minor release and
agrees on a version number.

```bash
mvn versions:set -DnewVersion=4.0.0-RC01
find . -name '*.versionsBackup' -type f -delete
git add .
git commit -am '[OLINGO-772] make release - set version 4.0.0-RC01'
git tag -f 4.0.0-RC01

mvn versions:set -DnewVersion=4.1.0-SNAPSHOT
find . -name '*.versionsBackup' -type f -delete
git add .
git commit -am '[OLINGO-772] make release - set version 4.1.0-SNAPSHOT'
git push
git push --tags
```

##### Open Issues

There must not be any open JIRA issues for this release. There might be open issues for
future releases. Check with: [fix for version view](https://issues.apache.org/jira/browse/OLINGO/fixforversion/12324804)

##### Unit Tests and Integration Tests

All unit tests and integration tests must succeed on a
clean machine (starting with an empty local Maven repository). The following Maven
execution will run all unit and integration tests:

```bash
mvn clean install
```

##### Apache License and Code Style

Each source code file must have a current ASF license header. The source
code should follow the Apache Olingo code style. For verification run following
Maven execution

```bash
mvn clean install -Pbuild.quality
```

##### Packaging

NOTICE, LICENSE and DISCLAIMER must be present in all bundles and must be up-to-date.

Remote resources are provided by the ASF and the Maven `remote-resources-plugin` is
configured in the parent pom of the project.

```xml
<resourceBundle>org.apache:apache-jar-resource-bundle:1.4</resourceBundle>
<resourceBundle>org.apache:apache-disclaimer-resource-bundle:1.1</resourceBundle>
```

The Maven module `odata-dist` (in project sub-folder `dist`) is responsible to package convenience distribution zip files
using the assembly plugin. The distributions are created with a release build `mvn clean install -Papache-release -Dgpg.passphrase="yourPassphraseHere"`

##### SHA for distribution packages

SHA files are created manually for distribution packages:

```text
gpg --print-md SHA512 ${filename}.zip > ${filename}.zip.sha512
```

DO NOT create md5 files

##### Release Tag

A tag has to be created for every release candidate. The naming rule
for the tags is `${version}-RCxx`. This is created as
part of the Maven release process. The tag will be renamed to the
final version number upon vote approval.

##### Release Branch

A branch has to be created for every release. The naming rule for this
branch is `${version}`. This has to be created manually upon release approval.

### Release Candidate

Once all preparations are done, a release candidate will be built.

All release candidates must be cryptographically signed. The string
"`-RCxx`" will be attached to the version number of the release candidate
artifacts, where is the number of the release candidate starting with 01.
If more than one release candidate is required a new tag has to be created
and release candidate number will be increased by one.

The release candidate artifacts:

- Maven artifacts will be staged on repository.apache.org. A new staging repo
  is created per RC and will be communicated upon release.
- Distribution commodity packages are staged at
  [https://people.apache.org/~[username]/olingo2/[version]](https://people.apache.org/%7E%5Busername%5D/olingo2/%5Bversion%5D) (e.g. [https://people.apache.org/~mibo/olingo4/4.0.0-RC01](https://people.apache.org/%7Emibo/olingo4/4.0.0-RC01))

Once candidate artifacts are available, release manager kicks off the [VOTE process][3].

If the vote fails, the raised issues will be fixed, a new release candidate will be
built and the VOTE process will be restarted.

If the release candidate gets approved, we can proceed to release publishing.

#### How to verify a Release Candidate

This checklist helps verifying if a release candidate is valid:

- Are all files on "[https://people.apache.org/~[username]/olingo4/[version]](https://people.apache.org/%7E%5Busername%5D/olingo4/%5Bversion%5D)"?
- Check if md5, sha512 and asc files are filled correctly?
- Can the zip files be unpacked without issues?
- Execute a "mvn clean install -Pbuild.quality" on parent distribution. It should work without issues.
- Does the JavaDoc only contain API documentation?
- Is there a Disclaimer, Notice, License and Dependencies File in every folder?
- Do all License files contain the right amount of licenses?
- Do Notice files mention 3rd party libraries if they are contained in the distribution?

After all questions of this checklist can be answered with yes it is OK to give a +1 on the mailing list.
Of course the Release Manager can also use this checklist to make sure all artifacts are correct before publishing the results on the mailing list.

### Publishing the Release

If the release candidate gets approved, we can proceed to release publishing:

- Release candidate maven artifacts are promoted in the Apache Maven Repository and
  made available [here](https://repository.apache.org/index.html#nexus-search;gav%7Eorg.apache.olingo%7E%7E%7E%7E).
- Publish final release Version to [Apache Repository](https://repository.apache.org/)
  - First publish via `mvn deploy -Papache-release` into the *Staging Area*
  - From *Staging Area* close and release the staged Artifacts to finish publishing
  - Afterwards the Maven artifacts are automatically synced to [Maven Central](https://search.maven.org/#search%7Cga%7C1%7Corg.apache.olingo).
- Release candidate commodity packages are synced (together with their checksum and
  signatures) to [Apache Distributions](https://www.apache.org/dist/olingo/).
- Release tag is renamed to final version.
- Release branch is created.
- Release is closed in Jira.
- Release is announced to [dev@olingo.apache.org](mailto:dev@olingo.apache.org), [announce@apache.org](mailto:announce@apache.org).

### Maintain Release Distributions

To maintain the released Distributions for the download pages following steps are required (for more information see [Apache Distribution Documentation](https://www.apache.org/dev/release-publishing.html#distribution_dist)) to upload the new distribution via SVN (`svn co https://dist.apache.org/repos/dist/release/olingo`):

- Check out latest SVN revision via `svn co https://dist.apache.org/repos/dist/release/olingo`
- Change into the directory according to the released Olingo artifact (e.g. `odata2`, `odata4`, ...)
- Create new directory according to release version (e.g `rel-x.x.x`) and copy all distribution artifacts (includes *.zip*, *.asc*, *.md5*, *.sha512*).
- Add new directory (e.g. `svn add rel-x.x.x`) and do the commit (e.g `svn ci -m "Added Olingo x.x.x release"`)
- Afterwards do a cleanup for the old releases according to the Apache Release Guidelines [When](https://www.apache.org/dev/release.html#when-to-archive) and [How](https://www.apache.org/dev/release.html#how-to-archive) to archive.

### Maintain Version Section in DOAP File

[https://olingo.apache.org/doap\_Olingo.rdf](/doap_Olingo.rdf)

Results are shown here:

[link text](https://projects.apache.org/indexes/alpha.html#O)

### Additional Apache Release Information

- [Releases Policy](https://www.apache.org/dev/release.html)
- [Publishing Releases](https://www.apache.org/dev/release-publishing.html)
- [Publishing Maven Artifacts](https://www.apache.org/dev/publishing-maven-artifacts.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-action-tutorial_action"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Part 6: Action Imports and Function Imports

**Table of Contents**

- [Introduction](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--introduction "Introduction")
- [Preparation](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--preparation "Preparation")
- [Implementation](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--implementation "Implementation")
  - [Extend the Metadata model](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--extend-the-metadata-model "Extend the Metadata model")
  - [Extend the data store](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--extend-the-data-store "Extend the data store")
  - [Extend the entity collection and the entity processor to handle function imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--extend-the-entity-collection-and-the-entity-processor-to-handle-function-imports "Extend the entity collection and the entity processor to handle function imports")
  - [Implement an action processor](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--implement-an-action-processor "Implement an action processor")
- [Run the implemented service](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--run-the-implemented-service "Run the implemented service")
- [Links](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--links "Links")
  - [Tutorials](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--tutorials "Tutorials")
  - [Code and Repository](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--code-and-repository "Code and Repository")
  - [Further reading](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action--further-reading "Further reading")

## Introduction

In the present tutorial, we’ll implement a function import and an action import as well.

**Note:**
The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).  
This tutorial can be found in subdirectory `/samples/tutorials/p9_action`

The [OData V4 specification](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398201) gives us a definition what *Functions*, *Actions* are:

> Operations allow the execution of custom logic on parts of a data
> model. Functions are operations that do not have side effects and may
> support further composition, for example, with additional filter
> operations, functions or an action. Actions are operations that allow
> side effects, such as data modification, and cannot be further
> composed in order to avoid non-deterministic behavior. Actions and
> functions are either bound to a type, enabling them to be called as
> members of an instance of that type, or unbound, in which case they
> are called as static operations. Action imports and function imports
> enable unbound actions and functions to be called from the service
> root.

In this short definition are several terms which are to be explained first. As you might expect operation is the superordinate of functions and actions. The result of operations can be:

- An *entity* or a *collection of entities*
- A *primitive property* or a *collection of primitive properties*
- A *complex property* or a *collection of complex properties*

In addition an *Action* can return void that means there is no return value. A *Function* must return a value.

The definition gives us some parts where function and actions can be used. First an *Operation* can be bound or unbound. In this tutorial we will focus on unbound operations. Unbound operations are something like static methods in Java, so if one of these operations have parameters we have to pass all of them explicit to the operation.

**Example**

For example there could be a function that calculates the VAT. The result depends on the one hand from the net price and on the other hand from the country in which the customer lives.

Such a function can be expressed in the metadata document as follows

```xml
    <Function Name="CalculateVAT">
         <Parameter Name="NetPrice" Type="Edm.Decimal" Nullable="false"/>
         <Parameter Name="Country" Type="Edm.String" Nullable="false"/>

        <ReturnType Type="Edm.Decimal"/>
    </Function>
```

To make this function statically callable we have to define a Function Import.

```xml
    <EntityContainer Name="Container">
         <FunctionImport Name="StaticCalculateVAT"
                         Function="CalculateVAT"
                         IncludeInServiceDocument="true"/>
    </EntityContainer>
```

To call such a Function Import the client issues a GET requests to a URL identifying the function import. The parameters are passed by using the so called inline parameter syntax. In this simple case such a call could look like this:

```text
http://host/myService/StaticCalculateVAT(NetPrice=123.00,Country=’US’)
```

The definition talks about composing of functions. By default every function is Composable=false, that means there must be no further resource parts after the function call and there must also be no system query options. If a function is composable you are be able to use system query options. Which options are allowed in particular is based on the return type of the function. Further you can navigate to properties and use Navigation Properties to navigate to related entities as well.

The definition of Actions / Action Imports in metadata document is similar to functions.

```xml
    <Action Name="Reset">
        <Parameter Name="Amount" Type="Edm.Int32"/>
    </Action>

    <EntityContainer Name="Container">
        <ActionImport Name="StaticReset" Action="Reset"/>
    </EntityContainer>
```

As you can see, this function does not return any value and takes one parameter “*Amount*” To call this action import, you have to issue an POST request to

```text
http://host/myService/StaticReset
```

The parameters are passed within the body of the request. In this case such a body could look like the following (JSON - Syntax):

```json
    {
      "Amount": 2
    }
```

As you read in the definition, actions can have side effects (modifying the data) but cannot be composed like functions.

## Preparation

You should read the previous tutorials first to have an idea how to read entities and entity collections. In addition the following code is based on the write tutorial merged with the navigation tutorial.

As a shortcut you should checkout the prepared tutorial project in the [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4) in folder /samples/tutorials/p9\_action\_preparation.

Afterwards do a Deploy and run: it should be working. At this state you can perform CRUD operations and do navigations between products and categories.

## Implementation

We use the given data model you are familiar with. To keep things simple we implement one function import and one action import.

**Function Import: CountCategories**  
This function takes a mandatory parameter “*Amount*”. The function returns a collection of categories with the very same number of related products.

After finishing the implementation the definition of the function should look like this:

```xml
    <Function Name="CountCategories">
          <Parameter Name="Amount" Type="Edm.Int32" Nullable="false"/>
          <ReturnType Type="Collection(OData.Demo.Category)"/>
    </Function>
```

As described in the previous tutorials, the type of the response determines which processor interface will be called by the Olingo library. It is **important to know, that functions are dispatched to the traditional processor interfaces**.
That means there are no special "FunctionProcessors". In our case, the function returns a collection of Categories. So we have to extend the `DemoEntityCollectionProcessor`. As you will see it is possible to address a single entity by its key. So we have to extend the `DemoEntityProcessor` as well to handle requests which responds a single entity.

**Action Import: Reset**  
This action takes an optional parameter “*Amount*”. The actions resets the whole data of the service and creates *# of Amount* products with the related categories.

After finishing the implementation the definition of the action should be like this:

```xml
    <Action Name="Reset" IsBound="false">
      <Parameter Name="Amount" Type="Edm.Int32"/>
    </Action>
```

While actions are called by using HTTP Method POST is nessesary to introduce new processor interfaces for actions. So there exists a bunch of interfaces, for each return type strictly one.

**Steps**

- Extend the Metadata model
- Extend the data store
- Extend the entity collection and the entity processor to handle function imports
- Implement an action processor

### Extend the Metadata model

Create the following constants in the DemoEdmProvider. These constants are used to address the operations.

```java
    // Action
    public static final String ACTION_RESET = "Reset";
    public static final FullQualifiedName ACTION_RESET_FQN = new FullQualifiedName(NAMESPACE, ACTION_RESET);

    // Function
    public static final String FUNCTION_COUNT_CATEGORIES = "CountCategories";
    public static final FullQualifiedName FUNCTION_COUNT_CATEGORIES_FQN = new FullQualifiedName(NAMESPACE, FUNCTION_COUNT_CATEGORIES);

    // Function/Action Parameters
    public static final String PARAMETER_AMOUNT = "Amount";
```

The way to announce the operations is very similar to announcing EntityTypes. We have to override some methods. Those methods provide the definition of the Edm elements. We need methods for:

- Actions
- Functions
- Action Imports
- Function Imports

The code is simple and straight forward. First, we check which function we have to return. Then, a list of parameters and the return type are created. At the end all parts are fit together and get returned as new `CsdlFunction` Object.

```java
    @Override
    public List<CsdlFunction> getFunctions(final FullQualifiedName functionName) {
      if (functionName.equals(FUNCTION_COUNT_CATEGORIES_FQN)) {
        // It is allowed to overload functions, so we have to provide a list of functions for each function name
        final List<CsdlFunction> functions = new ArrayList<CsdlFunction>();

        // Create the parameter for the function
        final CsdlParameter parameterAmount = new CsdlParameter();
        parameterAmount.setName(PARAMETER_AMOUNT);
        parameterAmount.setNullable(false);
        parameterAmount.setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName());

        // Create the return type of the function
        final CsdlReturnType returnType = new CsdlReturnType();
        returnType.setCollection(true);
        returnType.setType(ET_CATEGORY_FQN);

        // Create the function
        final CsdlFunction function = new CsdlFunction();
        function.setName(FUNCTION_COUNT_CATEGORIES_FQN.getName())
            .setParameters(Arrays.asList(parameterAmount))
            .setReturnType(returnType);
        functions.add(function);

        return functions;
      }

      return null;
    }
```

We have created the function itself. To express that function can be called statically we have to override the method `getFunctionImport()`.

```java
    @Override
    public CsdlFunctionImport getFunctionImport(FullQualifiedName entityContainer, String functionImportName) {
      if(entityContainer.equals(CONTAINER)) {
        if(functionImportName.equals(FUNCTION_COUNT_CATEGORIES_FQN.getName())) {
          return new CsdlFunctionImport()
                    .setName(functionImportName)
                    .setFunction(FUNCTION_COUNT_CATEGORIES_FQN)
                    .setEntitySet(ES_CATEGORIES_NAME)
                    .setIncludeInServiceDocument(true);
        }
      }

      return null;
    }
```

To define the actions and the action imports the `getActions()` and `getActionImport()` methods have to be overriden and the necessary code is quite similar to the functions sample above:

```java
    @Override
    public List<CsdlAction> getActions(final FullQualifiedName actionName) {
      if(actionName.equals(ACTION_RESET_FQN)) {
        // It is allowed to overload actions, so we have to provide a list of Actions for each action name
        final List<CsdlAction> actions = new ArrayList<CsdlAction>();

        // Create parameters
        final List<CsdlParameter> parameters = new ArrayList<CsdlParameter>();
        final CsdlParameter parameter = new CsdlParameter();
        parameter.setName(PARAMETER_AMOUNT);
        parameter.setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName());
        parameters.add(parameter);

        // Create the Csdl Action
        final CsdlAction action = new CsdlAction();
        action.setName(ACTION_RESET_FQN.getName());
        action.setParameters(parameters);
        actions.add(action);

        return actions;
      }

      return null;
    }

    @Override
    public CsdlActionImport getActionImport(final FullQualifiedName entityContainer, final String actionImportName) {
      if(entityContainer.equals(CONTAINER)) {
        if(actionImportName.equals(ACTION_RESET_FQN.getName())) {
          return new CsdlActionImport()
                  .setName(actionImportName)
                  .setAction(ACTION_RESET_FQN);
        }
      }

      return null;
    }
```

Finally we have to announce these operations to the schema and the entity container.
Add the following lines to the method `getSchemas()`:

```java
    // add actions
    List<CsdlAction> actions = new ArrayList<CsdlAction>();
    actions.addAll(getActions(ACTION_RESET_FQN));
    schema.setActions(actions);

    // add functions
    List<CsdlFunction> functions = new ArrayList<CsdlFunction>();
    functions.addAll(getFunctions(FUNCTION_COUNT_CATEGORIES_FQN));
    schema.setFunctions(functions);
```

Also add the following lines to the method `getEntityContainer()`

```java
    // Create function imports
    List<CsdlFunctionImport> functionImports = new ArrayList<CsdlFunctionImport>();
    functionImports.add(getFunctionImport(CONTAINER, FUNCTION_COUNT_CATEGORIES));

    // Create action imports
    List<CsdlActionImport> actionImports = new ArrayList<CsdlActionImport>();
    actionImports.add(getActionImport(CONTAINER, ACTION_RESET));

    entityContainer.setFunctionImports(functionImports);
    entityContainer.setActionImports(actionImports);
```

### Extend the data store

We need two methods in the data store to read the function import `CountCategories`.

The first method returns a collection of entites and the second returns a single entity of this collection.

```java
    public EntityCollection readFunctionImportCollection(final UriResourceFunction uriResourceFunction, final ServiceMetadata serviceMetadata) throws ODataApplicationException {

      if(DemoEdmProvider.FUNCTION_COUNT_CATEGORIES.equals(uriResourceFunction.getFunctionImport().getName())) {
        // Get the parameter of the function
        final UriParameter parameterAmount = uriResourceFunction.getParameters().get(0);

        // Try to convert the parameter to an Integer.
        // We have to take care, that the type of parameter fits to its EDM declaration
        int amount;
        try {
          amount = Integer.parseInt(parameterAmount.getText());
        } catch(NumberFormatException e) {
          throw new ODataApplicationException("Type of parameter Amount must be Edm.Int32",
            HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
        }

        final EdmEntityType productEntityType = serviceMetadata.getEdm().getEntityType(DemoEdmProvider.ET_PRODUCT_FQN);
        final List<Entity> resultEntityList = new ArrayList<Entity>();

        // Loop over all categories and check how many products are linked
        for(final Entity category : categoryList) {
          final EntityCollection products = getRelatedEntityCollection(category, productEntityType);
          if(products.getEntities().size() == amount) {
            resultEntityList.add(category);
          }
        }

        final EntityCollection resultCollection = new EntityCollection();
        resultCollection.getEntities().addAll(resultEntityList);
        return resultCollection;
      } else {
          throw new ODataApplicationException("Function not implemented", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
        Locale.ROOT);
      }
    }

    public Entity readFunctionImportEntity(final UriResourceFunction uriResourceFunction,
      final ServiceMetadata serviceMetadata) throws ODataApplicationException {

      final EntityCollection entityCollection = readFunctionImportCollection(uriResourceFunction, serviceMetadata);
      final EdmEntityType edmEntityType = (EdmEntityType) uriResourceFunction.getFunction().getReturnType().getType();

      return Util.findEntity(edmEntityType, entityCollection, uriResourceFunction.getKeyPredicates());
    }
```

We also create two methods to reset the data of our service.

```java
    public void resetDataSet(final int amount) {
      // Replace the old lists with empty ones
      productList = new ArrayList<Entity>();
      categoryList = new ArrayList<Entity>();

      // Create new sample data
      initProductSampleData();
      initCategorySampleData();

      // Truncate the lists
      if(amount < productList.size()) {
        productList = productList.subList(0, amount);
        // Products 0, 1 are linked to category 0
        // Products 2, 3 are linked to category 1
        // Products 4, 5 are linked to category 2
        categoryList = categoryList.subList(0, (amount / 2) + 1);
      }
    }

    public void resetDataSet() {
      resetDataSet(Integer.MAX_VALUE);
    }
```

### Extend the entity collection and the entity processor to handle function imports

We start with the entity collection processor `DemoEntityCollectionProcessor`.
To keep things simple, the first steps is to distinguish between entity collections and function imports.
A cleverer implementation can handle both cases in one method to avoid duplicated code.

The recent implementation of the `readEntityCollection()` has been moved to `readEntityCollectionInternal()`

```java
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat) throws ODataApplicationException, SerializerException {

      final UriResource firstResourceSegment = uriInfo.getUriResourceParts().get(0);

      if(firstResourceSegment instanceof UriResourceEntitySet) {
        readEntityCollectionInternal(request, response, uriInfo, responseFormat);
      } else if(firstResourceSegment instanceof UriResourceFunction) {
        readFunctionImportCollection(request, response, uriInfo, responseFormat);
      } else {
        throw new ODataApplicationException("Not implemented",
          HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
        Locale.ENGLISH);
      }
    }
```

Like by reading *entity collections*, the first step is to analyze the URI and then fetch the data (of the function import).

```java
    private void readFunctionImportCollection(final ODataRequest request, final ODataResponse response,
      final UriInfo uriInfo, final ContentType responseFormat) throws ODataApplicationException, SerializerException {

      // 1st step: Analyze the URI and fetch the entity collection returned by the function import
      // Function Imports are always the first segment of the resource path
      final UriResource firstSegment = uriInfo.getUriResourceParts().get(0);

      if(!(firstSegment instanceof UriResourceFunction)) {
        throw new ODataApplicationException("Not implemented",
          HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ENGLISH);
      }

      final UriResourceFunction uriResourceFunction = (UriResourceFunction) firstSegment;
      final EntityCollection entityCol = storage.readFunctionImportCollection(uriResourceFunction, serviceMetadata);
```

Then the result has to be serialized. The only difference to entity sets is the way how the `EdmEntityType` is determined.

```java
      // 2nd step: Serialize the response entity
      final EdmEntityType edmEntityType = (EdmEntityType) uriResourceFunction.getFunction().getReturnType().getType();
      final ContextURL contextURL = ContextURL.with().asCollection().type(edmEntityType).build();
      EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().contextURL(contextURL).build();
      final ODataSerializer serializer = odata.createSerializer(responseFormat);
      final SerializerResult serializerResult = serializer.entityCollection(serviceMetadata, edmEntityType, entityCol, opts);

      // 3rd configure the response object
      response.setContent(serializerResult.getContent());
      response.setStatusCode(HttpStatusCode.OK.getStatusCode());
      response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

Next we will implement the processor to read a *single entity*. The implementation is quite similar to the implementation of the collection processor.

```java
    public void readEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
      throws ODataApplicationException, SerializerException {

      // The sample service supports only functions imports and entity sets.
      // We do not care about bound functions and composable functions.

      UriResource uriResource = uriInfo.getUriResourceParts().get(0);

      if(uriResource instanceof UriResourceEntitySet) {
        readEntityInternal(request, response, uriInfo, responseFormat);
      } else if(uriResource instanceof UriResourceFunction) {
        readFunctionImportInternal(request, response, uriInfo, responseFormat);
      } else {
        throw new ODataApplicationException("Only EntitySet is supported",
          HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ENGLISH);
      }
    }

    private void readFunctionImportInternal(final ODataRequest request, final ODataResponse response,
      final UriInfo uriInfo, final ContentType responseFormat) throws ODataApplicationException, SerializerException {

      // 1st step: Analyze the URI and fetch the entity returned by the function import
      // Function Imports are always the first segment of the resource path
      final UriResource firstSegment = uriInfo.getUriResourceParts().get(0);

      if(!(firstSegment instanceof UriResourceFunction)) {
        throw new ODataApplicationException("Not implemented",
          HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ENGLISH);
      }

      final UriResourceFunction uriResourceFunction = (UriResourceFunction) firstSegment;
      final Entity entity = storage.readFunctionImportEntity(uriResourceFunction, serviceMetadata);

      if(entity == null) {
        throw new ODataApplicationException("Nothing found.",
          HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ROOT);
      }

      // 2nd step: Serialize the response entity
      final EdmEntityType edmEntityType = (EdmEntityType) uriResourceFunction.getFunction().getReturnType().getType();
      final ContextURL contextURL = ContextURL.with().type(edmEntityType).build();
      final EntitySerializerOptions opts = EntitySerializerOptions.with().contextURL(contextURL).build();
      final ODataSerializer serializer = odata.createSerializer(responseFormat);
      final SerializerResult serializerResult = serializer.entity(serviceMetadata, edmEntityType, entity, opts);

      // 3rd configure the response object
      response.setContent(serializerResult.getContent());
      response.setStatusCode(HttpStatusCode.OK.getStatusCode());
      response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

### Implement an action processor

Create a new class `DemoActionProcessor` make them implement the interface `ActionVoidProcessor`.

```java
    public class DemoActionProcessor implements ActionVoidProcessor {

      private OData odata;
      private Storage storage;

      public DemoActionProcessor(final Storage storage) {
        this.storage = storage;
      }

      @Override
      public void init(final OData odata, final ServiceMetadata serviceMetadata)   {
        this.odata = odata;
      }
```

First analyze the uri.

```java
    public void processActionVoid(ODataRequest request, ODataResponse response, UriInfo uriInfo,
        ContentType requestFormat) throws ODataApplicationException, ODataLibraryException {

      // 1st Get the action from the resource path
      final EdmAction edmAction = ((UriResourceAction) uriInfo.asUriInfoResource().getUriResourceParts()
                                            .get(0)).getAction();
```

Then deserialize the *action parameters*.

```java
      // 2nd Deserialize the parameter
      // In our case there is only one action. So we can be sure that parameter "Amount" has been provided by the client
      if (requestFormat == null) {
        throw new ODataApplicationException("The content type has not been set in the request.",
          HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
      }

      final ODataDeserializer deserializer = odata.createDeserializer(requestFormat);
      final Map<String, Parameter> actionParameter = deserializer.actionParameters(request.getBody(), edmAction)
                                     .getActionParameters();
      final Parameter parameterAmount = actionParameter.get(DemoEdmProvider.PARAMETER_AMOUNT);
```

Execute the action and set the response code.

```java
      // The parameter amount is nullable
      if(parameterAmount.isNull()) {
        storage.resetDataSet();
      } else {
        final Integer amount = (Integer) parameterAmount.asPrimitive();
        storage.resetDataSet(amount);
      }

      response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
    }
```

## Run the implemented service

After building and deploying your service to your server, you can try the following requests:

**Functions (Called via GET)**

- [http://localhost:8080/DemoService-Action/DemoService.svc/CountCategories(Amount=2)](http://localhost:8080/DemoService-Action/DemoService.svc/CountCategories(Amount=2))
- [http://localhost:8080/DemoService-Action/DemoService.svc/CountCategories(Amount=2)(0)](http://localhost:8080/DemoService-Action/DemoService.svc/CountCategories(Amount=2)(0))

**Actions (Called via POST)**  
*Note:* Set the Content-Type header to: `Content-Type: application/json`

- [http://localhost:8080/DemoService-Action/DemoService.svc/Reset](http://localhost:8080/DemoService-Action/DemoService.svc/Reset)

  Content:

  { }
- [http://localhost:8080/DemoService-Action/DemoService.svc/Reset](http://localhost:8080/DemoService-Action/DemoService.svc/Reset)

  Content:

  { "Amount": 1 }

To verify that the service has been reseted, you can request the collection of products

- [http://localhost:8080/DemoService-Action/DemoService.svc/Products](http://localhost:8080/DemoService-Action/DemoService.svc/Products)

# Links

### Tutorials

Further topics to be covered by follow-up tutorials:

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial ODATA V4 service, part 6: Action and Function Imports
- Tutorial ODATA V4 service, part 7: [Media Entities](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.0.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Part 10: Bound Actions and Functions

**Table of Contents**

- [Introduction](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--introduction "Introduction")
- [Preparation](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--preparation "Preparation")
- [Implementation](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--implementation "Implementation")
  - [Extend the Metadata model](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--extend-the-metadata-model "Extend the Metadata model")
  - [Extend the data store](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--extend-the-data-store "Extend the data store")
  - [Extend the entity collection and the entity processor to handle functions](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--extend-the-entity-collection-and-the-entity-processor-to-handle-functions "Extend the entity collection and the entity processor to handle functions")
  - [Implement an action processor](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--implement-an-action-processor "Implement an action processor")
- [Run the implemented service](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--run-the-implemented-service "Run the implemented service")
- [Links](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--links "Links")
  - [Tutorials](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--tutorials "Tutorials")
  - [Code and Repository](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--code-and-repository "Code and Repository")
  - [Further reading](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_bound_action--further-reading "Further reading")

## Introduction

In the present tutorial, we’ll implement a bound action and function.

**Note:**
The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).  
This tutorial can be found in subdirectory `/samples/tutorials/p9_action`

The [OData V4 specification](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398201) gives us a definition what *Functions*, *Actions* are:

> Operations allow the execution of custom logic on parts of a data
> model. Functions are operations that do not have side effects and may
> support further composition, for example, with additional filter
> operations, functions or an action. Actions are operations that allow
> side effects, such as data modification, and cannot be further
> composed in order to avoid non-deterministic behavior. Actions and
> functions are either bound to a type, enabling them to be called as
> members of an instance of that type, or unbound, in which case they
> are called as static operations. Action imports and function imports
> enable unbound actions and functions to be called from the service
> root.

In this short definition are several terms which are to be explained first. As you might expect operation is the superordinate of functions and actions. The result of operations can be:

- An *entity* or a *collection of entities*
- A *primitive property* or a *collection of primitive properties*
- A *complex property* or a *collection of complex properties*

In addition an *Action* can return void that means there is no return value. A *Function* must return a value.

First an *Operation* can be bound or unbound.In this tutorial we will focus on bound operation.

Bound actions and functions support overloading (multiple actions having the same name within the same namespace) by binding parameter type. The combination of action name and the binding parameter type MUST be unique within a namespace.

An action or a function element MAY specify a Boolean value for the IsBound attribute.
Actions/Functions whose IsBound attribute is false or not specified are considered unbound. Unbound actions/functions are invoked through an action import/function import.
Actions/Functions whose IsBound attribute is true are considered bound. Bound actions/functions are invoked by appending a segment containing the qualified action name to a segment of the appropriate binding parameter type within the resource path. Bound actions/functions MUST contain at least one edm:Parameter element, and the first parameter is the binding parameter. The binding parameter can be of any type, and it MAY be nullable.

Bound actions/functions that return an entity or a collection of entities MAY specify a value for the EntitySetPath attribute if determination of the entity set for the return type is contingent on the binding parameter. The value for the EntitySetPath attribute consists of a series of segments joined together with forward slashes. The first segment of the entity set path MUST be the name of the binding parameter. The remaining segments of the entity set path MUST represent navigation segments or type casts.

A navigation segment names the SimpleIdentifier of the navigation property to be traversed. A type cast segment names the QualifiedName of the entity type that should be returned from the type cast.

**Example**

For example there can be a bound action createOrders which is bound to the Customer entity having 2 parameters

Such an action can be expressed in the metadata document as follows

```xml
    <Action Name="CreateOrder" isBound=”true”>
     <Parameter Name="Customers" Type="SampleEntities.Customer" Nullable="false"/>
     <Parameter Name="quantity" Type="Edm.Int16" Nullable="false"/>
     <Parameter Name="discountCode" Type="Edm.String" Nullable="false"/>
     <ReturnType Type="Collection(SampleEntities.Orders)"/>
    </Action>
```

To call such a bound action the client issues a POST request to a URL identifying the action. In this simple case such a call could look like this:

```text
  POST http://host/service/Customers('ALFKI')/SampleEntities.CreateOrder
  {
    "quantity": 2,
    "discountCode": "BLACKFRIDAY"
  }
```

Similarly there can be a bound function GetOrders which is bound to the Customer entity having 1 parameter

Such a function can be expressed in the metadata document as follows

```xml
    <Function Name="GetOrders" isBound=”true”>
     <Parameter Name="Customers" Type="SampleEntities.Customer" Nullable="false"/>
     <Parameter Name="discountCode" Type="Edm.String" Nullable="false"/>
     <ReturnType Type="Collection(SampleEntities.Orders)"/>
    </Function>
```

To call such a bound function the client issues a GET request to a URL identifying the function. In this simple case such a call could look like this:

```text
  GET http://host/service/Customers('ALFKI')/SampleEntities.GetOrders(discountCode='BLACKFRIDAY')
```

## Preparation

You should read the previous tutorials first to have an idea how to read entities and entity collections.

As a shortcut you should checkout the prepared tutorial project in the [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4) in folder /samples/tutorials/p9\_action\_preparation.

Afterwards do a Deploy and run: it should be working. At this state you can perform CRUD operations and do navigations between products and categories.

## Implementation

We use the given data model you are familiar with. To keep things simple we implement one bound action and one bound function.

**Bound Action that returns a collection of entities: DiscountProducts**  
This action takes bound parameter “*ParamCategory*” and an additional parameter “*Amount*”. The action updates the price of all products related to categories by applying the discount amount.

After finishing the implementation the definition of the action should be like this:

```xml
    <Action Name="DiscountProducts" IsBound="true">
      <Parameter Name="ParamCategory" Type="Collection(OData.Demo.Category)"/>
      <Parameter Name="Amount" Type="Edm.Int32"/>
      <ReturnType Type="Collection(OData.Demo.Product)"/>
    </Action>
```

**Bound Action that returns an entity: DiscountProduct**

This action takes bound parameter “*ParamCategory*” and an additional parameter “*Amount*”. The actions updates the price of a specific products related to a category by applying the discount amount.
After finishing the implementation the definition of the action should be like this:

```xml
    <Action Name="DiscountProduct" IsBound="true">
       <Parameter Name="ParamCategory" Type=”OData.Demo.Category"/>
       <Parameter Name="Amount" Type="Edm.Int32"/>
       <ReturnType Type=" OData.Demo.Product"/>
    </Action>
```

While actions are called by using HTTP Method POST is nessesary to introduce new processor interfaces for actions. So there exists a bunch of interfaces, for each return type strictly one.

**Bound Function that returns a collection of entities: GetDiscountedProducts**  
This function takes bound parameter “*ParamCategory*” and an additional parameter “*Amount*”. The function lists all the products related to categories which are eligible for the discount amount.

After finishing the implementation the definition of the action should be like this:

```xml
    <Function Name="GetDiscountedProducts" IsBound="true">
      <Parameter Name="ParamCategory" Type="Collection(OData.Demo.Category)"/>
      <Parameter Name="Amount" Type="Edm.Int32"/>
      <ReturnType Type="Collection(OData.Demo.Product)"/>
    </Function>
```

**Bound Function that returns an entity: GetDiscountedProduct**

This function takes bound parameter “*ParamCategory*” and an additional parameter “*Amount*”. The function lists one specific product related to a category which is eligible for the discount amount.
After finishing the implementation the definition of the action should be like this:

```xml
    <Function Name="GetDiscountedProduct" IsBound="true">
       <Parameter Name="ParamCategory" Type=”OData.Demo.Category"/>
       <Parameter Name="Amount" Type="Edm.Int32"/>
       <ReturnType Type=" OData.Demo.Product"/>
    </Function>
```

**Steps**

- Extend the Metadata model
- Extend the data store
- Implement an action processor

### Extend the Metadata model

Create the following constants in the DemoEdmProvider. These constants are used to address the actions.

```java
    //Bound Action
    public static final String ACTION_PROVIDE_DISCOUNT = "DiscountProducts";
    public static final FullQualifiedName ACTION_PROVIDE_DISCOUNT_FQN = new FullQualifiedName(NAMESPACE, ACTION_PROVIDE_DISCOUNT);

    public static final String ACTION_PROVIDE_DISCOUNT_FOR_PRODUCT = "DiscountProduct";
    public static final FullQualifiedName ACTION_PROVIDE_DISCOUNT_FOR_PRODUCT_FQN = new FullQualifiedName(NAMESPACE, ACTION_PROVIDE_DISCOUNT_FOR_PRODUCT);
    
    //Bound Function
    public static final String FUNCTION_PROVIDE_DISCOUNT = "GetDiscountedProducts";
    public static final FullQualifiedName FUNCTION_PROVIDE_DISCOUNT_FQN = new FullQualifiedName(NAMESPACE, FUNCTION_PROVIDE_DISCOUNT);
      
    public static final String FUNCTION_PROVIDE_DISCOUNT_FOR_PRODUCT = "GetDiscountedProduct";
    public static final FullQualifiedName FUNCTION_PROVIDE_DISCOUNT_FOR_PRODUCT_FQN = new FullQualifiedName(NAMESPACE, FUNCTION_PROVIDE_DISCOUNT_FOR_PRODUCT);
    
    //Parameters
    public static final String PARAMETER_AMOUNT = "Amount";
    
    //Binding Parameter
    public static final String PARAMETER_CATEGORY = "ParamCategory";
```

The way to announce the operations is very similar to announcing EntityTypes. We have to override some methods. Those methods provide the definition of the Edm elements. We need methods for:

- Actions
- Functions

The code is simple and straight forward. We need to create a list of parameters of which the first parameter should be the binding parameter, then create a return type. At the end all parts are fit together and get returned as new CsdlAction Object.

```java
        @Override
        public List<CsdlAction> getActions(final FullQualifiedName actionName) {
       // It is allowed to overload actions, so we have to provide a list of Actions for each action name
        final List<CsdlAction> actions = new ArrayList<CsdlAction>();
        
        if (actionName.equals(ACTION_PROVIDE_DISCOUNT_FQN)) {
          // Create parameters
          final List<CsdlParameter> parameters = new ArrayList<CsdlParameter>();
          CsdlParameter parameter = new CsdlParameter();
          parameter.setName(PARAMETER_CATEGORY);
          parameter.setType(ET_CATEGORY_FQN);
          parameter.setCollection(true);
          parameters.add(parameter);
          parameter = new CsdlParameter();
          parameter.setName(PARAMETER_AMOUNT);
          parameter.setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName());
          parameters.add(parameter);
          
          // Create the Csdl Action
          final CsdlAction action = new CsdlAction();
          action.setName(ACTION_PROVIDE_DISCOUNT_FQN.getName());
          action.setBound(true);
          action.setParameters(parameters);
          action.setReturnType(new CsdlReturnType().setType(ET_PRODUCT_FQN).setCollection(true));
          actions.add(action);
          
          return actions;
        } else if (actionName.equals(ACTION_PROVIDE_DISCOUNT_FOR_PRODUCT_FQN)) {
          // Create parameters
          final List<CsdlParameter> parameters = new ArrayList<CsdlParameter>();
          CsdlParameter parameter = new CsdlParameter();
          parameter.setName(PARAMETER_CATEGORY);
          parameter.setType(ET_CATEGORY_FQN);
          parameter.setCollection(false);
          parameters.add(parameter);
          parameter = new CsdlParameter();
          parameter.setName(PARAMETER_AMOUNT);
          parameter.setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName());
          parameters.add(parameter);
          
          // Create the Csdl Action
          final CsdlAction action = new CsdlAction();
          action.setName(ACTION_PROVIDE_DISCOUNT_FOR_PRODUCT_FQN.getName());
          action.setBound(true);
          action.setParameters(parameters);
          action.setReturnType(new CsdlReturnType().setType(ET_PRODUCT_FQN).setCollection(false));
          actions.add(action);
          
          return actions;
        }
        
        return null;
      }
```

Similarly, for functions we need to create a list of parameters of which the first parameter should be the binding parameter, then create a return type. At the end all parts are fit together and get returned as new CsdlFunction Object.

```java
        @Override
        public List<CsdlFunction> getFunctions(final FullQualifiedName functionName) {
       // It is allowed to overload functions, so we have to provide a list of Functions for each function name
        final List<CsdlFunction> functions = new ArrayList<CsdlFunction>();
        
        if (functionName.equals(FUNCTION_PROVIDE_DISCOUNT_FQN)) {
          // Create parameters
          final List<CsdlParameter> parameters = new ArrayList<CsdlParameter>();
          CsdlParameter parameter = new CsdlParameter();
          parameter.setName(PARAMETER_CATEGORY);
          parameter.setType(ET_CATEGORY_FQN);
          parameter.setCollection(true);
          parameters.add(parameter);
          parameter = new CsdlParameter();
          parameter.setName(PARAMETER_AMOUNT);
          parameter.setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName());
          parameters.add(parameter);
          
          // Create the Csdl Function
          final CsdlFunction function = new CsdlFunction();
          function.setName(FUNCTION_PROVIDE_DISCOUNT_FQN.getName());
          function.setBound(true);
          function.setParameters(parameters);
          function.setReturnType(new CsdlReturnType().setType(ET_PRODUCT_FQN).setCollection(true));
          functions.add(function);
          
          return functions;
        } else if (functionName.equals(FUNCTION_PROVIDE_DISCOUNT_FOR_PRODUCT_FQN)) {
          // Create parameters
          final List<CsdlParameter> parameters = new ArrayList<CsdlParameter>();
          CsdlParameter parameter = new CsdlParameter();
          parameter.setName(PARAMETER_CATEGORY);
          parameter.setType(ET_CATEGORY_FQN);
          parameter.setCollection(false);
          parameters.add(parameter);
          parameter = new CsdlParameter();
          parameter.setName(PARAMETER_AMOUNT);
          parameter.setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName());
          parameters.add(parameter);
          
          // Create the Csdl Function
          final CsdlFunction function= new CsdlFunction();
          function.setName(ACTION_PROVIDE_DISCOUNT_FOR_PRODUCT_FQN.getName());
          function.setBound(true);
          function.setParameters(parameters);
          function.setReturnType(new CsdlReturnType().setType(ET_PRODUCT_FQN).setCollection(false));
          functions.add(function);
          
          return functions;
        }
        
        return null;
      }
```

Finally we have to announce these operations to the schema. Add the following lines to the method getSchemas():

```java
    // add actions
    List<CsdlAction> actions = new ArrayList<CsdlAction>();
    actions.addAll(getActions(ACTION_PROVIDE_DISCOUNT_FQN));
    actions.addAll(getActions(ACTION_PROVIDE_DISCOUNT_FOR_PRODUCT_FQN));
    schema.setActions(actions);

    // add functions
    List<CsdlFunction> functions = new ArrayList<CsdlFunction>();
    functions.addAll(getFunctions(FUNCTION_PROVIDE_DISCOUNT_FQN));
    functions.addAll(getFunctions(FUNCTION_PROVIDE_DISCOUNT_FOR_PRODUCT_FQN));
    schema.setFunctions(functions);
```

### Extend the data store

We need two methods in the data store to read the action DiscountProducts and DiscountProduct. The first method returns a collection of entites and second method returns a single entity.

```java
        public EntityCollection processBoundActionEntityCollection(EdmAction action, Map<String, Parameter> parameters) {
        EntityCollection collection = new EntityCollection();
        if ("DiscountProducts".equals(action.getName())) {
          for (Entity entity : categoryList) {
            Entity en = getRelatedEntity(entity, (EdmEntityType) action.getReturnType().getType());
            Integer currentValue = (Integer)en.getProperty("Price").asPrimitive();
            Integer newValue = currentValue - (Integer)parameters.get("Amount").asPrimitive();
            en.getProperty("Price").setValue(ValueType.PRIMITIVE, newValue);
            collection.getEntities().add(en);
          }
        }
        return collection;
      }
    
      public DemoEntityActionResult processBoundActionEntity(EdmAction action, Map<String, Parameter> parameters,
          List<UriParameter> keyParams) throws ODataApplicationException {
        DemoEntityActionResult result = new DemoEntityActionResult();
        if ("DiscountProduct".equals(action.getName())) {
          for (Entity entity : categoryList) {
            Entity en = getRelatedEntity(entity, (EdmEntityType) action.getReturnType().getType(), keyParams);
            Integer currentValue = (Integer)en.getProperty("Price").asPrimitive();
            Integer newValue = currentValue - (Integer)parameters.get("Amount").asPrimitive();
            en.getProperty("Price").setValue(ValueType.PRIMITIVE, newValue);
            result.setEntity(en);
            result.setCreated(true);
            return result;
          }
        }
        return null;
      }
```

In the second method, we are returning a custom object DemoEntityActionResult. This holds the entity and the status as to whether the entity is created or just returned. This information is used to set the response status.

```java
      public class DemoEntityActionResult {
      private Entity entity;
      private boolean created = false;
    
      public Entity getEntity() {
        return entity;
      }
    
      public DemoEntityActionResult setEntity(final Entity entity) {
        this.entity = entity;
        return this;
      }
    
      public boolean isCreated() {
        return created;
      }
    
      public DemoEntityActionResult setCreated(final boolean created) {
        this.created = created;
        return this;
      }
    }
```

We also create methods for GetDiscountedProducts and GetDiscountedProduct functions.

```java
        public EntityCollection getBoundFunctionEntityCollection(EdmFunction function, Integer amount) {
        EntityCollection collection = new EntityCollection();
        if ("GetDiscountedProducts".equals(function.getName())) {
          for (Entity entity : categoryList) {
            if(amount >= entity.getProperty("amount")){
              Entity en = getRelatedEntity(entity, (EdmEntityType) function.getReturnType().getType());
              collection.getEntities().add(en);
            }
          }
        }
        return collection;
      }
    
      public Entity getBoundFunctionEntity(EdmAction function, Integer amount) throws ODataApplicationException {
        if ("GetDiscountedProduct".equals(function.getName())) {
          for (Entity entity : categoryList) {
            if(amount== entity.getProperty("amount")){
              return getRelatedEntity(entity, (EdmEntityType) function.getReturnType().getType(), keyParams);
            }
          }
        }
        return null;
      }
```

### Extend the entity collection and the entity processor to handle functions

We start with the entity collection processor DemoEntityCollectionProcessor. To keep things simple, the first steps is to distinguish between entity collections and function imports. A cleverer implementation can handle both cases in one method to avoid duplicated code.

The recent implementation of the readEntityCollection() has been moved to readEntityCollectionInternal()

```java
public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat) throws ODataApplicationException, SerializerException {

  final UriResource firstResourceSegment = uriInfo.getUriResourceParts().get(0);

  if(firstResourceSegment instanceof UriResourceEntitySet) {
    readEntityCollectionInternal(request, response, uriInfo, responseFormat);
  } else if(firstResourceSegment instanceof UriResourceFunction) {
    readFunctionImportCollection(request, response, uriInfo, responseFormat);
  } else {
    throw new ODataApplicationException("Not implemented",
      HttpStatusCode.NOT&#95;IMPLEMENTED.getStatusCode(),
    Locale.ENGLISH);
  }
}
```

Like by reading entity collections, the first step is to analyze the URI and then fetch the data (of the function).

```java
     private void readEntityCollectionInternal(final ODataRequest request, final ODataResponse response,
     final UriInfo uriInfo, final ContentType responseFormat) throws ODataApplicationException, SerializerException {

     EdmEntitySet responseEdmEntitySet = null; // we'll need this to build the ContextURL
     EntityCollection responseEntityCollection = null; // we'll need this to set the response body

     // 1st retrieve the requested EntitySet from the uriInfo (representation of the parsed URI)
     List<UriResource> resourceParts = uriInfo.getUriResourceParts();
     int segmentCount = resourceParts.size();

     UriResource uriResource = resourceParts.get(0); // in our example, the first segment is the EntitySet
     if (!(uriResource instanceof UriResourceEntitySet)) {
       throw new ODataApplicationException("Only EntitySet is supported",
           HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
     }

     UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) uriResource;
     EdmEntitySet startEdmEntitySet = uriResourceEntitySet.getEntitySet();

      if (segmentCount == 1) { 
      // This is a normal query fetch the entity from backend and return entityset
      }

      else if (segmentCount == 2) { // in case of function or navigation

        UriResource lastSegment = resourceParts.get(1); // in our example we don't support more complex URIs
        if (lastSegment instanceof UriResourceFunction) {// For bound function
        UriResourceFunction uriResourceFunction = (UriResourceFunction) lastSegment;
        // 2nd: fetch the data from backend
        // first fetch the target entity type 
        String targetEntityType = uriResourceFunction.getFunction().getReturnType().getType().getName();
        // contextURL displays the last segment
        for(EdmEntitySet entitySet : serviceMetadata.getEdm().getEntityContainer().getEntitySets()){
          if(targetEntityType.equals(entitySet.getEntityType().getName())){
            responseEdmEntitySet = entitySet;
            break;
          }
        }
        
        // error handling for null entities
        if (targetEntityType == null || responseEdmEntitySet == null) {
          throw new ODataApplicationException("Entity not found.",
              HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ROOT);
        }
        Integer amount = Integer.parseInt(uriResourceFunction.getParameters().get(0).getText())
        // then fetch the entity collection for the target type
        responseEntityCollection = storage.getBoundFunctionEntityCollection(function, amount);
      }
    }
      // 3rd: create and configure a serializer
     ContextURL contextUrl = ContextURL.with().entitySet(responseEdmEntitySet).build();
     final String id = request.getRawBaseUri() + "/" + responseEdmEntitySet.getName();
     EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with()
        .contextURL(contextUrl).id(id).build();
     EdmEntityType edmEntityType = responseEdmEntitySet.getEntityType();

     ODataSerializer serializer = odata.createSerializer(responseFormat);
     SerializerResult serializerResult = serializer.entityCollection(serviceMetadata, edmEntityType,
        responseEntityCollection, opts);

     // 4th: configure the response object: set the body, headers and status code
     response.setContent(serializerResult.getContent());
     response.setStatusCode(HttpStatusCode.OK.getStatusCode());
     response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

Next we will implement the processor to read a single entity. The implementation is quite similar to the implementation of the collection processor.

```java
    public void readEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
      throws ODataApplicationException, SerializerException {

      UriResource uriResource = uriInfo.getUriResourceParts().get(0);

      if(uriResource instanceof UriResourceEntitySet) {
        readEntityInternal(request, response, uriInfo, responseFormat);
      } else if(uriResource instanceof UriResourceFunction) {
        readFunctionImportInternal(request, response, uriInfo, responseFormat);
      } else {
        throw new ODataApplicationException("Only EntitySet is supported",
          HttpStatusCode.NOT&#95;IMPLEMENTED.getStatusCode(), Locale.ENGLISH);
      }
    }

     private void readEntityInternal(final ODataRequest request, final ODataResponse response,
      final UriInfo uriInfo, final ContentType responseFormat) throws ODataApplicationException, SerializerException {

       EdmEntityType responseEdmEntityType = null; // we'll need this to build the ContextURL
       Entity responseEntity = null; // required for serialization of the response body
       EdmEntitySet responseEdmEntitySet = null; // we need this for building the contextUrl

       // 1st step: retrieve the requested Entity: can be "normal" read operation, or navigation (to-one)
       List<UriResource> resourceParts = uriInfo.getUriResourceParts();
       int segmentCount = resourceParts.size();
    
       UriResource uriResource = resourceParts.get(0); // in our example, the first segment is the EntitySet
       UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) uriResource;
       EdmEntitySet startEdmEntitySet = uriResourceEntitySet.getEntitySet();

      if (segmentCount == 1) { 
      // This is a normal read call fetch the entity from backend and return entity
      } else if (segmentCount == 2) { // Bound Function or navigation
        UriResource segment = resourceParts.get(1);
        if (segment instanceof UriResourceFunction) {
          UriResourceFunction uriResourceFunction = (UriResourceFunction) segment;

          // 2nd: fetch the data from backend.
          // first fetch the target entity type 
          String targetEntityType = uriResourceFunction.getFunction().getReturnType().getType().getName();
       
          // contextURL displays the last segment
          for(EdmEntitySet entitySet : serviceMetadata.getEdm().getEntityContainer().getEntitySets()){
            if(targetEntityType.equals(entitySet.getEntityType().getName())){
              responseEdmEntityType = entitySet.getEntityType();
              responseEdmEntitySet = entitySet;
              break;
            }
          }
        
          // error handling for null entities
          if (targetEntityType == null || responseEdmEntitySet == null) {
            throw new ODataApplicationException("Entity not found.",
                HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ROOT);
          }

        Integer amount = Integer.parseInt(uriResourceFunction.getParameters().get(0).getText())
        // then fetch the entity collection for the target type
        responseEntity = storage.getBoundFunctionEntity(function, amount);
      }
    }

    if (responseEntity == null) {
      // this is the case for e.g. DemoService.svc/Categories(4) or DemoService.svc/Categories(3)/Products(999)
      throw new ODataApplicationException("Nothing found.", HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ROOT);
    }

    // 3. serialize
    ContextURL contextUrl = ContextURL.with().entitySet(responseEdmEntitySet).suffix(Suffix.ENTITY).build();
    EntitySerializerOptions opts = EntitySerializerOptions.with().contextURL(contextUrl).build();

    ODataSerializer serializer = odata.createSerializer(responseFormat);
    SerializerResult serializerResult = serializer.entity(serviceMetadata,
        responseEdmEntityType, responseEntity, opts);

    // 4. configure the response object
    response.setContent(serializerResult.getContent());
    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

### Implement an action processor

Create a new class `DemoActionProcessor` make them implement the interface interface 'ActionEntityCollectionProcessor' and 'ActionEntityProcessor'.

```java
      public class DemoActionProcessor implements ActionEntityCollectionProcessor, ActionEntityProcessor {
    
      private OData odata;
      private Storage storage;
      private ServiceMetadata serviceMetadata;
    
      public DemoActionProcessor(final Storage storage) {
        this.storage = storage;
      }
    
      @Override
      public void init(final OData odata, final ServiceMetadata serviceMetadata)   {
        this.odata = odata;
        this.serviceMetadata = serviceMetadata;
      }
```

The first overriden method returns a collection of entities.

First analyze the uri. Bound Actions will have the first segment in the resource path to be an entity set. It can then be followed by a navigation segment or a type cast. The last segment will be the fully qualified action name.

Then deserialize the action parameters.

Execute the action and set the response code.

```java
        @Override
      public void processActionEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo,
          ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
    
        Map<String, Parameter> parameters = new HashMap<String, Parameter>();
        EdmAction action = null;
        EntityCollection collection = null;
        
        if (requestFormat == null) {
          throw new ODataApplicationException("The content type has not been set in the request.",
              HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
        }
        
        List<UriResource> resourcePaths = uriInfo.asUriInfoResource().getUriResourceParts();
        final ODataDeserializer deserializer = odata.createDeserializer(requestFormat);
        UriResourceEntitySet boundEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
        if (resourcePaths.size() > 1) {
    	// Check if there is a navigation segment added after the bound parameter
          if (resourcePaths.get(1) instanceof UriResourceAction) {
           action = ((UriResourceAction) resourcePaths.get(2))
                .getAction();
            throw new ODataApplicationException("Action " + action.getName() + " is not yet implemented.",
            HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ENGLISH);      } else {
            action = ((UriResourceAction) resourcePaths.get(1))
                .getAction();
            parameters = deserializer.actionParameters(request.getBody(), action)
                .getActionParameters();
            collection =
                storage.processBoundActionEntityCollection(action, parameters);
          }
        }
        // Collections must never be null.
        // Not nullable return types must not contain a null value.
        if (collection == null
            || collection.getEntities().contains(null) && !action.getReturnType().isNullable()) {
          throw new ODataApplicationException("The action could not be executed.",
              HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.ROOT);
        }
    
        final Return returnPreference = odata.createPreferences(request.getHeaders(HttpHeader.PREFER)).getReturn();
        if (returnPreference == null || returnPreference == Return.REPRESENTATION) {
          final EdmEntitySet edmEntitySet = boundEntitySet.getEntitySet();
          final EdmEntityType type = (EdmEntityType) action.getReturnType().getType();
          final EntityCollectionSerializerOptions options = EntityCollectionSerializerOptions.with()
              .contextURL(isODataMetadataNone(responseFormat) ? null : getContextUrl(action.getReturnedEntitySet(edmEntitySet), type, false))
              .build();
          response.setContent(odata.createSerializer(responseFormat)
              .entityCollection(serviceMetadata, type, collection, options).getContent());
          response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
          response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        } else {
          response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
        }
        if (returnPreference != null) {
          response.setHeader(HttpHeader.PREFERENCE_APPLIED,
              PreferencesApplied.with().returnRepresentation(returnPreference).build().toValueString());
        }
      }
      
    //This method fetches the context URL
      private ContextURL getContextUrl(final EdmEntitySet entitySet, final EdmEntityType entityType,
          final boolean isSingleEntity) throws ODataLibraryException {
        Builder builder = ContextURL.with();
        builder = entitySet == null ?
            isSingleEntity ? builder.type(entityType) : builder.asCollection().type(entityType) :
            builder.entitySet(entitySet);
        builder = builder.suffix(isSingleEntity && entitySet != null ? Suffix.ENTITY : null);
        return builder.build();
      }
      
      protected boolean isODataMetadataNone(final ContentType contentType) {
        return contentType.isCompatible(ContentType.APPLICATION_JSON)
            && ContentType.VALUE_ODATA_METADATA_NONE.equalsIgnoreCase(
                contentType.getParameter(ContentType.PARAMETER_ODATA_METADATA));
      }
```

The second method to be overriden returns a single entity.

Again first analyze the uri. Bound Actions will have the first segment in the resource path to be an entity set with a key predicate. It can then be followed by a navigation segment or a type cast. The last segment will be the fully qualified action name.

Then deserialize the action parameters.

Execute the action and set the response code.

```java
        @Override
      public void processActionEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
          ContentType requestFormat, ContentType responseFormat) throws ODataApplicationException, ODataLibraryException {
    
        EdmAction action = null;
        Map<String, Parameter> parameters = new HashMap<String, Parameter>(); 
       // DemoEntityActionResult is a custom object that holds the entity and the status as to whether the entity is created or just returned. This information is used to set the response status
        DemoEntityActionResult entityResult = null;
        if (requestFormat == null) {
          throw new ODataApplicationException("The content type has not been set in the request.",
              HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
        }
        
        final ODataDeserializer deserializer = odata.createDeserializer(requestFormat);
        final List<UriResource> resourcePaths = uriInfo.asUriInfoResource().getUriResourceParts();
        UriResourceEntitySet boundEntity = (UriResourceEntitySet) resourcePaths.get(0);
        if (resourcePaths.size() > 1) {
    	// Checks if there is a navigation segment added after the binding parameter
          if (resourcePaths.get(1) instanceof UriResourceAction) {
            action = ((UriResourceAction) resourcePaths.get(1))
                .getAction();
            throw new ODataApplicationException("Action " + action.getName() + " is not yet implemented.",
            HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ENGLISH);
          } else if (resourcePaths.get(0) instanceof UriResourceEntitySet) {
            action = ((UriResourceAction) resourcePaths.get(1))
                .getAction();
            parameters = deserializer.actionParameters(request.getBody(), action)
                .getActionParameters();
            entityResult =
                storage.processBoundActionEntity(action, parameters, boundEntity.getKeyPredicates());
          }
        }
        final EdmEntitySet edmEntitySet = boundEntity.getEntitySet();
        final EdmEntityType type = (EdmEntityType) action.getReturnType().getType();
    
        if (entityResult == null || entityResult.getEntity() == null) {
          if (action.getReturnType().isNullable()) {
            response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
          } else {
            // Not nullable return type so we have to give back a 500
            throw new ODataApplicationException("The action could not be executed.",
                HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.ROOT);
          }
        } else {
          final Return returnPreference = odata.createPreferences(request.getHeaders(HttpHeader.PREFER)).getReturn();
          if (returnPreference == null || returnPreference == Return.REPRESENTATION) {
            response.setContent(odata.createSerializer(responseFormat).entity(
                serviceMetadata,
                type,
                entityResult.getEntity(),
                EntitySerializerOptions.with()
                    .contextURL(isODataMetadataNone(responseFormat) ? null : getContextUrl(action.getReturnedEntitySet(edmEntitySet), type, true))
                    .build())
                .getContent());
            response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
            response.setStatusCode((entityResult.isCreated() ? HttpStatusCode.CREATED : HttpStatusCode.OK)
                .getStatusCode());
          } else {
            response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
          }
          if (returnPreference != null) {
            response.setHeader(HttpHeader.PREFERENCE_APPLIED,
                PreferencesApplied.with().returnRepresentation(returnPreference).build().toValueString());
          }
          if (entityResult.isCreated()) {
            final String location = request.getRawBaseUri() + '/'
                + odata.createUriHelper().buildCanonicalURL(edmEntitySet, entityResult.getEntity());
            response.setHeader(HttpHeader.LOCATION, location);
            if (returnPreference == Return.MINIMAL) {
              response.setHeader(HttpHeader.ODATA_ENTITY_ID, location);
            }
          }
          if (entityResult.getEntity().getETag() != null) {
            response.setHeader(HttpHeader.ETAG, entityResult.getEntity().getETag());
          }
        }    
      }
```

## Run the implemented service

After building and deploying your service to your server, you can try the following requests:

**Functions (Called via GET)**

- [http://localhost:8080/DemoService-Action/DemoService.svc/Categories/OData.Demo.GetDiscountedProducts(Amount=50)](http://localhost:8080/DemoService-Action/DemoService.svc/Categories/OData.Demo.GetDiscountedProducts(Amount=50))
- [http://localhost:8080/DemoService-Action/DemoService.svc/Categories(0)/OData.Demo.GetDiscountedProduct(Amount=50)](http://localhost:8080/DemoService-Action/DemoService.svc/Categories(0)/OData.Demo.GetDiscountedProduct(Amount=50))

**Actions (Called via POST)**  
*Note:* Set the Content-Type header to: `Content-Type: application/json`

- [http://localhost:8080/DemoService-Action/DemoService.svc/Categories/OData.Demo.DiscountProducts](http://localhost:8080/DemoService-Action/DemoService.svc/Categories/OData.Demo.DiscountProducts)

  Content:

  {"Amount":50 }
- [http://localhost:8080/DemoService-Action/DemoService.svc/Categories(0)/OData.Demo.DiscountProduct](http://localhost:8080/DemoService-Action/DemoService.svc/Categories(0)/OData.Demo.DiscountProduct)

  Content:

  { "Amount": 50 }

# Links

### Tutorials

Further topics to be covered by follow-up tutorials:

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial ODATA V4 service, part 6: Action and Function Imports
- Tutorial ODATA V4 service, part 7: [Media Entities](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.0.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Part 8: Batch Request Support

## Introduction

In the present tutorial, we’ll implement batch requests.

**Note:**
The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).  
This tutorial can be found in subdirectory /samples/tutorials/p11\_batch

**Table of Contents**

1. Introduction
2. Preparation
3. Implementation
4. Run the implemented service
5. Links

# 1. Introduction

Batch requests [(OData Version 4.0 Part 1: Protocol Plus Errata 02)](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398359) allow grouping multiple operations into a single HTTP request payload. A batch request is represented as a Multipart MIME v1.0 message [(RFC2046)](https://www.ietf.org/rfc/rfc2046.txt).
Each part of a Multipart MINE message can have a different content type. For example you can mix OData requests with Content-Type `application/json` and Media Ressource Requests with Content Type `image/png`.

The content of batch requests can consist of a series of individual requests and Change Sets, each represented as a distinct MIME part. In difference to OData V2 an individual request can be a Data Request, Data Modification Request, Action invocation request or a Function invocation request. So all kinds of OData requests are allowed at top level. The order of individual requests and Change Set sets in significant. Within a Change Set you can use Data Modification requests and Action invocation requests. Due to the fact that all requests within a Change Set are unordered, GET requests must not be used within a Change Set. All operations in a change set represent a single change unit so a service must successfully process and apply all the requests in the change set or else apply none of them.

**Example**

The request below consists of an individual request (upper red box), actually a GET request(the upper blue box) to the Entity Set `Products` and a Change Set (lower red box). The Change Set contains a single POST request (lower blue box) to create a new `Product`. Please note that the *boundary delimiter* `abc123` is used in the request below. The whole content must be wrapped in a single POST request issued against the resource `/$batch`. The Content-Type of the POST request is consequently `Content-Type: multipart/mixed;boundary=abc123`

![Test](olingo.apache.org/doc/odata4/tutorials/batch/request.png)

# 2. Preparation

You should read the previous tutorials first to have an idea how to read and write entities. In addition the following code is based on the write tutorial.

As a shortcut you should checkout the prepared tutorial project in the git repository in folder /samples/tutorials/p3\_write.

The main idea of the following implementation is to reuse the existing processors.
To do so, we will implement a new processor, which takes a batch request and dispatches the single requests to the responsible processors.

The following steps have to be performed:

- Modify the data store
- Implement the interface `BatchProcessor`

## Add transactional behavior to the data store

Before we start with the actual processor implementation the data store has to be modified to provide transactional behavior. In real world service the underlying database may supports transactional handling. This tutorial is not based on a database so we have implement a simple transaction handling by ourselves.

Add the following methods to the class `myservice.mynamespace.data.Storage`. When a new transaction has been begun the data of the service is copied and stored in an instance variable. If `rollbackTransaction` has been called the current data is replaced with the previous copied one.

```java
    private List<Entity> productListBeforeTransaction;
    
    public void beginTransaction() {
        if(productListBeforeTransaction == null) {
            productListBeforeTransaction = cloneEntityCollection(productList);
        } 
    }

    public void commitTransaction() {
        if(productListBeforeTransaction != null) {
            productListBeforeTransaction = null;
        }
    }

    public void rollbackTranscation() {
        if(productListBeforeTransaction != null) {
            productList = productListBeforeTransaction;
            productListBeforeTransaction = null;
        }
    }

    private List<Entity> cloneEntityCollection(final List<Entity> entities) {
        final List<Entity> clonedEntities = new ArrayList<Entity>();

        for(final Entity entity : entities) {
            final Entity clonedEntity = new Entity();

            clonedEntity.setId(entity.getId());
            for(final Property property : entity.getProperties()) {
                clonedEntity.addProperty(new Property(property.getType(),
                                                      property.getName(),
                                                      property.getValueType(),
                                                      property.getValue()));
            }

            clonedEntities.add(clonedEntity);
        }

        return clonedEntities;
    }
```

## Implement the interface `BatchProcessor`

Create a new class `myservice.mynamespace.service.DemoBatchProcessor`. The class should implement the interface `org.apache.olingo.server.api.processor.BatchProcessor`.

Create a constructor and pass the data store to the processor.

```java
    public class DemoBatchProcessor implements BatchProcessor {

        private OData odata;
        private Storage storage;

        public DemoBatchProcessor(final Storage storage) {
            this.storage = storage;
        }

        @Override
            public void init(final OData odata, final ServiceMetadata serviceMetadata) {
            this.odata = odata;
        }
        ...
```

### Implement batch handling

Batch requests will be dispatched to the method `processBatch`. First, the boundary have to be extract from the Content-Type of the POST request.

```java
        @Override
        public void processBatch(final BatchFacade facade, final ODataRequest request, final ODataResponse response)
            throws ODataApplicationException, ODataLibraryException {

            // 1. Extract the boundary
            final String boundary = facade.extractBoundaryFromContentType(request.getHeader(HttpHeader.CONTENT_TYPE));
```

After that we are able to parse the multipart mixed body. The parser needs to know what the base URI and the service resolution paths are. The result of the parser is a list of BatchRequestParts. Each of these parts represents either a single request or a Change Set (Collection of one or more requests).

```java
        // 2. Prepare the batch options
        final BatchOptions options = BatchOptions.with().rawBaseUri(request.getRawBaseUri())
                                                        .rawServiceResolutionUri(request.getRawServiceResolutionUri())
                                                        .build();

        // 3. Deserialize the batch request
        final List<BatchRequestPart> requestParts = odata.createFixedFormatDeserializer()
                                                         .parseBatchRequest(request.getBody(), boundary, options);
```

Now the requests have to be executed by our service. If you like, you can do it by our own or simply call the method `handleBatchRequest`. This method dispatches individual requests directly to the responsible processor. If a Change Set is passed to `handleBatchRequest` the implementation dispatches the request to the method `processChangeSet` of our `DemoBatchProcessor`.

```java
        // 4. Execute the batch request parts
        final List<ODataResponsePart> responseParts = new ArrayList<ODataResponsePart>();
        for (final BatchRequestPart part : requestParts) {
            responseParts.add(facade.handleBatchRequest(part));
        }
```

The last steps are to serialize the responses and setup the response of the batch request.

```java
        // 5. Create a new boundary for the response
        final String responseBoundary = "batch_" + UUID.randomUUID().toString();

        // 6. Serialize the response content
        final InputStream responseContent = odata.createFixedFormatSerializer().batchResponse(responseParts, responseBoundary);

        // 7. Setup response
        response.setHeader(HttpHeader.CONTENT_TYPE, ContentType.MULTIPART_MIXED + ";boundary=" + responseBoundary);
        response.setContent(responseContent);
        response.setStatusCode(HttpStatusCode.ACCEPTED.getStatusCode());
    }
```

### Implement Change Set handling

As mentioned above Change Sets are dispatched to the method `processChangeSet`.
In this tutorial the implementation is quite simple. First we begin a new transaction. After that we try to execute all requests of the Change Set. If one of the requests fail all changes have to be rolled back.
The comments in the source code give a detailed explanation about the steps done in this method.

```java
    @Override
    public ODataResponsePart processChangeSet(final BatchFacade facade, final List<ODataRequest> requests)
        throws ODataApplicationException, ODataLibraryException {
        /* 
        * OData Version 4.0 Part 1: Protocol Plus Errata 02
        *      11.7.4 Responding to a Batch Request
        * 
        *      All operations in a change set represent a single change unit so a service MUST successfully process and 
        *      apply all the requests in the change set or else apply none of them. It is up to the service implementation 
        *      to define rollback semantics to undo any requests within a change set that may have been applied before 
        *      another request in that same change set failed and thereby apply this all-or-nothing requirement. 
        *      The service MAY execute the requests within a change set in any order and MAY return the responses to the 
        *       individual requests in any order. The service MUST include the Content-ID header in each response with the 
        *      same value that the client specified in the corresponding request, so clients can correlate requests 
        *      and responses.
        * 
        * To keep things simple, we dispatch the requests within the Change Set to the other processor interfaces.
        */
        final List<ODataResponse> responses = new ArrayList<ODataResponse>();

        try {
            storage.beginTransaction();

            for(final ODataRequest request : requests) {
                // Actual request dispatching to the other processor interfaces.
                final ODataResponse response = facade.handleODataRequest(request);

                // Determine if an error occurred while executing the request.
                // Exceptions thrown by the processors get caught and result in a proper OData response.
                final int statusCode = response.getStatusCode();
                if(statusCode < 400) {
                    // The request has been executed successfully. Return the response as a part of the change set
                    responses.add(response);
                } else {
                    // Something went wrong. Undo all previous requests in this Change Set
                    storage.rollbackTranscation();

                   /*
                    * In addition the response must be provided as follows:
                    * 
                    * OData Version 4.0 Part 1: Protocol Plus Errata 02
                    *     11.7.4 Responding to a Batch Request
                    *
                    *     When a request within a change set fails, the change set response is not represented using
                    *     the multipart/mixed media type. Instead, a single response, using the application/http media type
                    *     and a Content-Transfer-Encoding header with a value of binary, is returned that
                    *     applies to all requests in the change set and MUST be formatted according to the Error Handling
                    *     defined for the particular response format.
                    *     
                    * This can be simply done by passing the response of the failed ODataRequest to a new instance of 
                    * ODataResponsePart and setting the second parameter "isChangeSet" to false.
                    */
                    return new ODataResponsePart(response, false);
                }
            }

            // Everything went well, so commit the changes.
            storage.commitTransaction();
            return new ODataResponsePart(responses, true);

        } catch(ODataApplicationException e) {
            // See below
            storage.rollbackTranscation();
            throw e;
        } catch(ODataLibraryException e) {
            // The batch request is malformed or the processor implementation is not correct.
            // Throwing an exception will stop the whole batch request not only the Change Set!
            storage.rollbackTranscation();
            throw e;
        }
    }
```

# 4. Run the implemented service

After building and deploying your service to your server, you can try the following requests:

All requests are issued againest [http://localhost:8080/DemoService-Action/DemoService.svc/$batch](http://localhost:8080/DemoService-Action/DemoService.svc/$batch)
Set the Content-Type header to `Content-Type: multipart/mixed;boundary=abc123`

**Example 1**  
Please note that the second request in the Change Set references the first request of the Change Set.
This is done by prefixing the Content-Id of the referenced request with a $. e.g. `$abc`

```text
--abc123
Content-Type: application/http
Content-Transfer-Encoding: binary

GET Products HTTP/1.1
Content-Type: application/json

--abc123
Content-Type: multipart/mixed;boundary=changeset_abc

--changeset_abc
Content-Type: application/http 
Content-Transfer-Encoding:binary
Content-Id: 1

POST Products HTTP/1.1
Content-Type: application/json

{"Name": "Test Product", "Description": "This is a test product"}

--changeset_abc
Content-Type: application/http 
Content-Transfer-Encoding:binary
Content-ID: 2

PATCH $1 HTTP/1.1
Content-Type: application/json
Accept: application/json

{"Description": "With a changed Description"}

--changeset_abc--

--abc123
Content-Type: application/http
Content-Transfer-Encoding: binary

GET Products HTTP/1.1
Content-Type: application/json

--abc123--
```

Now have a look at the response. As you can see a new product has been created and the description has been updated to 'With a changed Description'

```json
{
    "@odata.context": "$metadata#Products",
    "value": [
        ...
        {
            "ID": 4,
            "Name": "Test Product",
            "Description": "With a changed Description"
        }
    ]
}
```

**Example 2**  
Let us try what is happing if we send a invalid request within an Change Set. Use the same URI and Content-Type as before and use the following body:

```text
--abc123
Content-Type: multipart/mixed;boundary=changeset_abc

--changeset_abc
Content-Type: application/http 
Content-Transfer-Encoding:binary
Content-Id: 1

POST Products HTTP/1.1
Content-Type: application/json

{"Name": "Test Product2", "Description": "This is a test product"}

--changeset_abc
Content-Type: application/http 
Content-Transfer-Encoding:binary
Content-ID: 2

PATCH $1 HTTP/1.1
Content-Type: application/json
Accept: application/json

{"Description": "Invalid....

--changeset_abc--
--abc123--
```

As you can see the response contains a single request response instead of a Change Set response. The error message in stored in the response body.

# 5. Links

### Tutorials

Further topics to be covered by follow-up tutorials:

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial OData V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial OData V4 service, part 7: [Add Media entities to the service](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: Batch request support
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.0.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Part 9: Handling "Deep Insert" requests

## Introduction

In the present tutorial, we will implement the handling of deep insert requests.

**Note:**
The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).  
This tutorial can be found in subdirectory /samples/tutorials/p12\_deep\_insert

**Table of Contents**

1. Introduction
2. Preparation
3. Implementation
4. Run the implemented service
5. Links

# 1. Introduction

In this tutorial shows how to handle "deep insert" requests. OData gives us the possibility to create related entities, and bind existing entities to a new created entity in a single request. (More detailed information: [OData Version 4.0 Part 1: Protocol](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398326), [OData JSON Format Version 4.0](http://docs.oasis-open.org/odata/odata-json-format/v4.0/errata02/os/odata-json-format-v4.0-errata02-os-complete.html#_Toc403940637))

OData uses to create a related entity the same syntax as for an expanded navigation property, as descripted in [OData JSON Format](http://docs.oasis-open.org/odata/odata-json-format/v4.0/errata02/os/odata-json-format-v4.0-errata02-os-complete.html#_Toc403940637). To bind an existing entity, OData uses the `odata.bind` property annotation. The value of the annotation is either an entity-Id or a collection of entity-Ids. An [entity-Id](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398204) is a durable, opaque, globally unique [IRI](https://www.ietf.org/rfc/rfc3987.txt). The specification recommends to use the canonical URL of the entity. In this tutorial the relative canonical URL of an entity is used.

**Example**

For example you may want to create a new category and also create new products, which are related to the new created category. In addition you would like to bind an existing product to the new created category.
Such a request is issued againest the URL of the entity set.

In this example, a new Category "Food" and two products ("Bread", "Milk") are created. In addition the Product with the key 5 is bind to the just created entity.

```text
POST /Categories HTTP/1.1
Content-Type: application/json

{
    "Name": "Food",
    "Products@odata.bind": [
        "Products(5)"
    ],
    "Products": [
        {
            "Name": "Bread",
            "Description": "Whole grain bread"
        },
        {
            "Name": "Milk",
            "Description": "Low fat milk"
        }
    ]
}
```

# 2. Preparation

You should read the previous tutorials first to have an idea how to read and write entities. In addition the following code is based on the write tutorial merged with the navigation tutorial. **It is strongly recommended to have a look at the prepared code.** There are some changes how related entites are linked together. More details are descripted in the implementation chapter.

As a shortcut you should checkout the prepared tutorial project in the [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4) in folder /samples/tutorials/p12\_deep\_insert\_preparation.

Afterwards do a Deploy and run: it should be working. At this state you can perform CRUD operations and do navigations between products and categories.

# 3. Implementation

Before we start with the implementation, please have a look at the class `myservice.mynamespace.data.Storage`. In difference to the [navigation tutorial](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation) the relations between two entities can not be hard coded because we would like to create and change relations between entities dynamically. In the constructor of the data storage the creation of the sample data is called. After that the method `linkProductsAndCategories`is called. This methods sets a few links between the just created entities. **The linked entites are stored as navigation links**

To express the relation between two entities, Olingo uses the class [Link](/javadoc/odata4/org/apache/olingo/commons/api/data/Link.html). This class is used for related entites (directly connected via Java references) and bindings (which are actually strings) to other entities. To get the related entites for a particual navigation property, you can ask an entity with the method [`getNavigationLink(String name)`](/javadoc/odata4/org/apache/olingo/commons/api/data/Linked.html#getNavigationLink(java.lang.String)) for an navigation property link. The link will contain either an entity or a collection of entities dependenting on the type of the navigation property. To get the actual entities use the methods [`getInlineEntity()`](/javadoc/odata4/org/apache/olingo/commons/api/data/Link.html#getInlineEntity()) or [`getInlineEntitySet()`](http://javadoc/odata4/org/apache/olingo/commons/api/data/Link.html#getInlineEntitySet())
The same can be done for bindings via the method [`getNavigationBinding(String name)`](/javadoc/odata4/org/apache/olingo/commons/api/data/Linked.html#getNavigationBinding(java.lang.String)). The values of the Binding can be gotten by the methods [`getBindingLink()`](/javadoc/odata4/org/apache/olingo/commons/api/data/Link.html#getBindingLink()) and [`getBindingLinks()`](/javadoc/odata4/org/apache/olingo/commons/api/data/Link.html#getBindingLinks()).

The point is that the Entity deserializer uses the same concept to represent the payload as Java objects.
Please have a look at the figure below. The deserializer returns an entity with two aggregated Link Objects which belons to the same navigation property "Product". The upper Link object stores the related entites, which have to be created. The lower Link contains the entity-ids to the already existing entities.
![Deserializer Result](olingo.apache.org/doc/odata4/tutorials/deep_insert/before.png)

When our implementation has processed the whole request, all entites are created and linked as **navigationLinks**.

![After Deep insert](olingo.apache.org/doc/odata4/tutorials/deep_insert/after.png)

If one of the requests fail, or one of the binding links is invalid, none of the entities must be created.
The prepared implementation provides the methods `beginTransaction`, `rollbackTransaction` and `commitTransaction` in the data store to simulate a transactional behavior.
Those methods are called in the `DemoEntityProcessor` implementation. So if you throw an exception in the createEntity Method the transaction will automatically rolled back.

So let us begin with the implementation. In the previous tutorials the entity object returned by the deserializer is passed to the data store. Please open the class `myservice.mynamespace.data.Storage` and jump the method `createEntity`.

The implementation should look like the following:

```java
    private Entity createEntity(EdmEntitySet edmEntitySet, EdmEntityType edmEntityType, Entity entity, 
        List<Entity> entityList, final String rawServiceUri) throws ODataApplicationException {

        // 1.) Create the entity
        final Entity newEntity = new Entity();
        newEntity.setType(entity.getType());

        // Create the new key for the entity
        int newId = 1;
        while (entityIdExists(newId, entityList)) {
            newId++;
        }

        // Add all provided properties
        newEntity.getProperties().addAll(entity.getProperties());

        // Add the key property
        newEntity.getProperties().add(new Property(null, "ID", ValueType.PRIMITIVE, newId));
        newEntity.setId(createId(newEntity, "ID"));

	    // --> Implement Deep Insert handling here <--

        entityList.add(newEntity);
        return newEntity;
    }
```

The implementation is split in two steps:

- Handle entity bindings
- Handle related entities

## Handle entity bindings

To handle entity bindings we need two helper methods.

The first method takes an entity-Id and returns the addressed entity. The OData objects provides a helper object to parse entity-ids. (See [UriHelper](/javadoc/odata4/org/apache/olingo/server/api/uri/UriHelper.html)). After parsing the id we have to check if the addressed entity set fits to the entity set the navigation property points to. After that the data is read by calling `readEntityData`.

```java
    private Entity readEntityByBindingLink(final String entityId, final EdmEntitySet edmEntitySet, 
        final String rawServiceUri) throws ODataApplicationException {

        UriResourceEntitySet entitySetResource = null;
        try {
            entitySetResource = odata.createUriHelper().parseEntityId(edm, entityId, rawServiceUri);

            if(!entitySetResource.getEntitySet().getName().equals(edmEntitySet.getName())) {
                throw new ODataApplicationException("Execpted an entity-id for entity set " + edmEntitySet.getName() +
                    " but found id for entity set " + entitySetResource.getEntitySet().getName(), 
                    HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
            }
        } catch (DeserializerException e) {
            throw new ODataApplicationException(entityId + " is not a valid entity-Id", 
                HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
        }

        return readEntityData(entitySetResource.getEntitySet(), entitySetResource.getKeyPredicates());
    }
```

The second method helps us to link entities together. If the navigation property has partner navigation property the link is set in both directions.

```java
    private void createLink(final EdmNavigationProperty navigationProperty, final Entity srcEntity,
        final Entity destEntity) {

        setLink(navigationProperty, srcEntity, destEntity);

        final EdmNavigationProperty partnerNavigationProperty = navigationProperty.getPartner();
        if (partnerNavigationProperty != null) {
            setLink(partnerNavigationProperty, destEntity, srcEntity);
        }
    }
```

If the client has used the `odata.bind` property annotation, we can get the bindings by calling `getNavigationBindings()`. The implementation loops over all bindings and links the addressed entity to the new created one.

```java
    // 2.1.) Apply binding links
    for(final Link link : entity.getNavigationBindings()) {
        final EdmNavigationProperty edmNavigationProperty = edmEntityType.getNavigationProperty(link.getTitle());
        final EdmEntitySet targetEntitySet = (EdmEntitySet) edmEntitySet.getRelatedBindingTarget(link.getTitle());

        if(edmNavigationProperty.isCollection() && link.getBindingLinks() != null) {
            for(final String bindingLink : link.getBindingLinks()) {
                final Entity relatedEntity = readEntityByBindingLink(bindingLink, targetEntitySet, rawServiceUri);
                createLink(edmNavigationProperty, newEntity, relatedEntity);
            }
        } else if(!edmNavigationProperty.isCollection() && link.getBindingLink() != null) {
            final Entity relatedEntity = readEntityByBindingLink(link.getBindingLink(), targetEntitySet, rawServiceUri);
            createLink(edmNavigationProperty, newEntity, relatedEntity);
        }
    }
```

## Handle related entities

The creation of related entities is similar. First the implementation loops over all navigation properties with related entites in the payload. The simplest way to create releated entities is to call the method `createEntityData`. So the the implementation is called recursively and can handle deep inserts with arbitrary depth.

```java
    // 2.2.) Create nested entities
    for(final Link link : entity.getNavigationLinks()) {
        final EdmNavigationProperty edmNavigationProperty = edmEntityType.getNavigationProperty(link.getTitle());
        final EdmEntitySet targetEntitySet = (EdmEntitySet) edmEntitySet.getRelatedBindingTarget(link.getTitle());

        if(edmNavigationProperty.isCollection() && link.getInlineEntitySet() != null) {
            for(final Entity nestedEntity : link.getInlineEntitySet().getEntities()) {
                final Entity newNestedEntity = createEntityData(targetEntitySet, nestedEntity, rawServiceUri);
                createLink(edmNavigationProperty, newEntity, newNestedEntity);
		    }
        } else if(!edmNavigationProperty.isCollection() && link.getInlineEntity() != null){
            final Entity newNestedEntity = createEntityData(targetEntitySet, link.getInlineEntity(), rawServiceUri);
            createLink(edmNavigationProperty, newEntity, newNestedEntity);
        }
	}
```

# 4. Run the implemented service

After building and deploying your service to your server, you can try the following requests:

URI: [`http://localhost:8080/DemoService-DeepInsert/DemoService.svc/Categories`](http://localhost:8080/DemoService-DeepInsert/DemoService.svc/Categories)  
HTTP-Verb: POST  
Content-Type: application/json  
Payload:

```json
{
    "name": "Food",
    "Products@odata.bind": [
        "Products(5)"
    ],
    "Products": [
        {
            "Name": "Bread",
            "Description": "Whole grain bread"
        },
        {
            "Name": "Milk",
            "Description": "Low fat milk"
        }
    ]
}
```

After sending this requests, let us see if the enties have been created and linked togetger.
Send an GET request to `http://localhost:8080/DemoService-DeepInsert/DemoService.svc/Categories`.
If it is the first category you created, the new entity should have the key `3`

![Entity Set Categories](olingo.apache.org/doc/odata4/tutorials/deep_insert/categories_entity_set.png)

So send a request to fetch all related products for the "Food" category.
[`http://localhost:8080/DemoService-DeepInsert/DemoService.svc/Categories(3)/Products`](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

As you can see the two products are created and linked to the category.

![Related products for category "Food"](olingo.apache.org/doc/odata4/tutorials/deep_insert/related_products.png)

# 5. Links

### Tutorials

Further topics to be covered by follow-up tutorials:

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial OData V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial OData V4 service, part 7: [Add Media entities to the service](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: Handling "Deep Insert" requests

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.0.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-media-tutorial_media"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Part 7: Media Entities

## Introduction

In the present tutorial, we will implement a media entity set.

**Note:**
The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).
This tutorial can be found in subdirectory /samples/tutorials/p10\_media

**Table of Contents**

- [Preparation](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--preparation "Preparation")
- [Implementation](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--implementation "Implementation")
  - [Extend the metadata document](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--extend-the-metadata-document "Extend the metadata document")
  - [Enable the data store to handle media entities](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--enable-the-data-store-to-handle-media-entities "Enable the data store to handle media entities")
  - [Implement the interface MediaEntityProcessor](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--implement-the-interface-mediaentityprocessor "Implement the interface MediaEntityProcessor")
- [Run the implemented service](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--run-the-implemented-service "Run the implemented service")
- [Links](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--links "Links")
  - [Tutorials](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--tutorials "Tutorials")
  - [Code and Repository](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--code-and-repository "Code and Repository")
  - [Further reading](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media--further-reading "Further reading")

## Preparation

You should read the previous tutorials first to have an idea how to read entities and entity collections. In addition the following code is based on the write tutorial merged with the navigation tutorial.

As a shortcut you should checkout the prepared tutorial project in the git repository in folder /samples/tutorials/p9\_action\_preparation.

Afterwards do a Deploy and run: it should be working. At this state you can perform CRUD operations and do navigations between products and categories.

## Implementation

In this tutorial we will implement a media entity set. The idea is to store advertisements and a related image. The metadata document have to be extended by the following elements:

```xml
    <EntityType Name="Advertisement" HasStream="true">
        <Key>
            <PropertyRef Name="ID"/>
        </Key>
        <Property Name="ID" Type="Edm.Guid"/>
        <Property Name="Name" Type="Edm.String"/>
        <Property Name="AirDate" Type="Edm.DateTimeOffset"/>
    </EntityType>

    <EntityContainer Name="Container">
        <EntitySet Name="Advertisements" EntityType="OData.Demo.Advertisement"/>
    </EntityContainer>
```

As you can see, the XML tag `EntityType` has a property `HasStream`, which tells us that this entity has an associated media stream. Such an Entity consists of common properties like *ID* and *Name* and the media stream.

**Tasks**

- Extend the metadata document
- Enable the data store to handle media entities
- Implement the interface `MediaEntityProcessor`

### Extend the metadata document

If you have read the previous tutorials, you should be familiar with the definition entity types. The only difference to regular (Non media enties) is, that they have a `hasStream` property. If this property is not provided it defaults to false. So add the following code to class `DemoEdmProvider`:

We start with method `DemoEdmProvider.getEntityType`

```java
    public CsdlEntityType getEntityType(FullQualifiedName entityTypeName) {
        CsdlEntityType entityType = null;

        if (entityTypeName.equals(ET_PRODUCT_FQN)) {
            // Definition of entity type Product
            // ...
        } else if (entityTypeName.equals(ET_CATEGORY_FQN)) {
            // Definition of entity type Category
            // ...
        } else if(entityTypeName.equals(ET_ADVERTISEMENT_FQN)) {
            CsdlProperty id = new CsdlProperty().setName("ID")
                                    .setType(EdmPrimitiveTypeKind.Guid.getFullQualifiedName());
            CsdlProperty name = new CsdlProperty().setName("Name")
                                    .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
            CsdlProperty airDate = new CsdlProperty().setName("AirDate")
                                    .setType(EdmPrimitiveTypeKind.DateTimeOffset.getFullQualifiedName());

            CsdlPropertyRef propertyRef = new CsdlPropertyRef();
            propertyRef.setName("ID");

            entityType = new CsdlEntityType();
            entityType.setName(ET_ADVERTISEMENT_NAME);
            entityType.setProperties(Arrays.asList(id, name, airDate));
            entityType.setKey(Collections.singletonList(propertyRef));
            entityType.setHasStream(true);    // <- Enable the media entity stream
        }

        return entityType;
    }
```

Further we have to create a new entity set. Add the following snipped to `DemoEdmProvider.getEntitySet`

```java
    @Override
    public CsdlEntitySet getEntitySet(FullQualifiedName entityContainer, String entitySetName) {
        CsdlEntitySet entitySet = null;

        if (entityContainer.equals(CONTAINER)) {
            if (entitySetName.equals(ES_PRODUCTS_NAME)) {
                // Definition of entity set Products
            } else if (entitySetName.equals(ES_CATEGORIES_NAME)) {
                // Definition if entity set Categories
            } else if (entitySetName.equals(ES_ADVERTISEMENTS_NAME)) {
                entitySet = new CsdlEntitySet();
                entitySet.setName(ES_ADVERTISEMENTS_NAME);
                entitySet.setType(ET_ADVERTISEMENT_FQN);
            }
        }

        return entitySet;
    }
```

And finally announce the entity type and entity set:

```java
    @Override
    public List<CsdlSchema> getSchemas() {
        // ...
        entityTypes.add(getEntityType(ET_ADVERTISEMENT_FQN));
        // ...

        return schemas;
    }

    public CsdlEntityContainer getEntityContainer() {
        // ...
        entitySets.add(getEntitySet(CONTAINER, ES_ADVERTISEMENTS_NAME));
    }
```

### Enable the data store to handle media entities

In this tutorial, we will keep things simple. To store the value of media entities, we create a special property *$value*. Note this is not a valid OData Identifier.
All methods have to be implemented in class `myservice.mynamespace.data.Storage`

To read the content to a media entity, we simple return the value of the property *$value*.

```java
    private static final String MEDIA_PROPERTY_NAME = "$value";
    private List<Entity> advertisements;

    public byte[] readMedia(final Entity entity) {
        return (byte[]) entity.getProperty(MEDIA_PROPERTY_NAME).asPrimitive();
    }
```

If we update the content of a media entity, we must also set the the Content Type of the content.

```java
    public void updateMedia(final Entity entity, final String mediaContentType, final byte[] data) {
        entity.getProperties().remove(entity.getProperty(MEDIA_PROPERTY_NAME));
        entity.addProperty(new Property(null, MEDIA_PROPERTY_NAME, ValueType.PRIMITIVE, data));
        entity.setMediaContentType(mediaContentType);
    }
```

If a client creates a new media entity, the body of the requet contains the content of the media entity instead the regular properties! So the other regular properties defaults to `null`. The Content Type of the media content must also be set.

```java
    public Entity createMediaEntity(final EdmEntityType edmEntityType, final String mediaContentType, final byte[] data) {
        Entity entity = null;

        if(edmEntityType.getName().equals(DemoEdmProvider.ET_ADVERTISEMENT_NAME)) {
            entity = new Entity();
            entity.addProperty(new Property(null, "ID", ValueType.PRIMITIVE, UUID.randomUUID()));
            entity.addProperty(new Property(null, "Name", ValueType.PRIMITIVE, null));
            entity.addProperty(new Property(null, "AirDate", ValueType.PRIMITIVE, null));

            entity.setMediaContentType(mediaContentType);
            entity.addProperty(new Property(null, MEDIA_PROPERTY_NAME, ValueType.PRIMITIVE, data));

            advertisements.add(entity);
        }

        return entity;
    }
```

Add an initial set of data to our data store:

```java
    private void initAdvertisementSampleData() {
        Entity entity = new Entity();
        entity.addProperty(new Property(null, "ID", ValueType.PRIMITIVE, UUID.fromString("f89dee73-af9f-4cd4-b330-db93c25ff3c7")));
        entity.addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "Old School Lemonade Store, Retro Style"));
        entity.addProperty(new Property(null, "AirDate", ValueType.PRIMITIVE, Timestamp.valueOf("2012-11-07 00:00:00")));
        entity.addProperty(new Property(null, MEDIA_PROPERTY_NAME, ValueType.PRIMITIVE, "Super content".getBytes()));
        entity.setMediaContentType(ContentType.parse("text/plain").toContentTypeString());
        advertisements.add(entity);

        entity = new Entity();
        entity.addProperty(new Property(null, "ID", ValueType.PRIMITIVE,
        UUID.fromString("db2d2186-1c29-4d1e-88ef-a127f521b9c67")));
        entity.addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "Early morning start, need coffee"));
        entity.addProperty(new Property(null, "AirDate", ValueType.PRIMITIVE, Timestamp.valueOf("2000-02-29 00:00:00")));
        entity.addProperty(new Property(null, MEDIA_PROPERTY_NAME, ValueType.PRIMITIVE, "Super content2".getBytes()));
        entity.setMediaContentType(ContentType.parse("text/plain").toContentTypeString());
        advertisements.add(entity);
    }
```

Call `initAdvertisementSampleData()` in the constructor.

```java
    public Storage() {
        // ...
        advertisements = new ArrayList<Entity>();
        // ...
        initAdvertisementSampleData();
    }
```

Enable the regular entity set for CRUD opertations:

```java
    public EntityCollection readEntitySetData(EdmEntitySet edmEntitySet) throws ODataApplicationException {

        if (edmEntitySet.getName().equals(DemoEdmProvider.ES_PRODUCTS_NAME)) {
            // ...
        } else if(edmEntitySet.getName().equals(DemoEdmProvider.ES_ADVERTISEMENTS_NAME)) {
            return getEntityCollection(advertisements);
        }

        return null;
    }

    public Entity readEntityData(EdmEntitySet edmEntitySet, List<UriParameter> keyParams)
            throws ODataApplicationException {

        EdmEntityType edmEntityType = edmEntitySet.getEntityType();
        if (edmEntitySet.getName().equals(DemoEdmProvider.ES_PRODUCTS_NAME)) {
            // ...
        } else if(edmEntitySet.getName().equals(DemoEdmProvider.ES_ADVERTISEMENTS_NAME)) {
            return getEntity(edmEntityType, keyParams, advertisements);
        }

        return null;
    }

    public Entity createEntityData(EdmEntitySet edmEntitySet, Entity entityToCreate) {

        EdmEntityType edmEntityType = edmEntitySet.getEntityType();
        if (edmEntitySet.getName().equals(DemoEdmProvider.ES_PRODUCTS_NAME)) {
            // ....
        } else if(edmEntitySet.getName().equals(DemoEdmProvider.ES_CATEGORIES_NAME)) {
            return createEntity(edmEntityType, entityToCreate, categoryList);
        }

        return null;
    }

    public void updateEntityData(EdmEntitySet edmEntitySet, List<UriParameter> keyParams,
            Entity updateEntity, HttpMethod httpMethod) throws ODataApplicationException {

        EdmEntityType edmEntityType = edmEntitySet.getEntityType();
        if (edmEntitySet.getName().equals(DemoEdmProvider.ES_PRODUCTS_NAME)) {
            // ...
        } else if(edmEntitySet.getName().equals(DemoEdmProvider.ES_ADVERTISEMENTS_NAME)) {
            updateEntity(edmEntityType, keyParams, updateEntity, httpMethod, advertisements);
        }
    }

    public void deleteEntityData(EdmEntitySet edmEntitySet, List<UriParameter> keyParams)
            throws ODataApplicationException {

        EdmEntityType edmEntityType = edmEntitySet.getEntityType();
        if (edmEntitySet.getName().equals(DemoEdmProvider.ES_PRODUCTS_NAME)) {
            // ...
        } else if(edmEntitySet.getName().equals(DemoEdmProvider.ES_ADVERTISEMENTS_NAME)) {
            deleteEntity(edmEntityType, keyParams, advertisements);
        }
    }
```

### Implement the interface `MediaEntityProcessor`

As you can see the [`MediaEntityProcessor`(Javadoc)](/javadoc/odata4/org/apache/olingo/server/api/processor/MediaEntityProcessor.html) extends [`EntityProcessor`](/javadoc/odata4/org/apache/olingo/server/api/processor/EntityProcessor.html), therefore we will implement `MediaEntityProcessor` in class `DemoEntityProcessor`.

The easiest part is to delete an media entity. The method `deleteMediaEntity` is delegated to the method `deleteEntity(...)`.

```java
    @Override
    public void deleteMediaEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo)
            throws ODataApplicationException, ODataLibraryException {

        /*
         * In this tutorial, the content of the media entity is stored in a special property.
         * So no additional steps to delete the content of the media entity are necessary.
         *
         * A real service may store the content on the file system. So we have to take care to
         * delete external files too.
         *
         * DELETE request to /Advertisements(ID) will be dispatched to the deleteEntity(...) method
         * DELETE request to /Advertisements(ID)/$value will be dispatched to the deleteMediaEntity(...) method
         *
         * So it is a good idea handle deletes in a central place.
         */

        deleteEntity(request, response, uriInfo);
    }
```

Next the creation of media entites is implemented. First we fetch the addressed entity set and convert the body of the request to a byte array. Remember the whole body of the request contains the content of the media entity.

```java
    @Override
    public void createMediaEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType requestFormat, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {

        final EdmEntitySet edmEntitySet = Util.getEdmEntitySet(uriInfo);
        final byte[] mediaContent = odata.createFixedFormatDeserializer().binary(request.getBody());
        ///...
```

After that we call the data store to create the new media entity. The [OData Specification](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398337) tells us, that we have to set the location header to the edit URL of the entity. Since we do not support Prefer Headers we have to return the entity itself.

```java
        final Entity entity = storage.createMediaEntity(edmEntitySet.getEntityType(),
        requestFormat.toContentTypeString(),mediaContent);

        final ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).suffix(Suffix.ENTITY).build();
        final EntitySerializerOptions opts = EntitySerializerOptions.with().contextURL(contextUrl).build();
        final SerializerResult serializerResult = odata.createSerializer(responseFormat)
                            .entity(serviceMetadata, edmEntitySet.getEntityType(), entity, opts);

        final String location = request.getRawBaseUri() + '/'
                + odata.createUriHelper().buildCanonicalURL(edmEntitySet, entity);

        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.CREATED.getStatusCode());
        response.setHeader(HttpHeader.LOCATION, location);
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

To keep things simple, our scenario do not support navigation to media entities. Because of this, the implementation to read a media entity is quite simple. First analayse the URI and fetch the entity. Than take the content of our specical property, serialize them and return the serialized content. The serializer converts the byte array to an `InputStream`.

```java
    @Override
    public void readMediaEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {

        // Since our scenario do not contain navigations from media entities. We can keep things simple and
        // check only the first resource path of the URI.
        final UriResource firstResoucePart = uriInfo.getUriResourceParts().get(0);
        if(firstResoucePart instanceof UriResourceEntitySet) {
            final EdmEntitySet edmEntitySet = Util.getEdmEntitySet(uriInfo);
            final UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) firstResoucePart;

            final Entity entity = storage.readEntityData(edmEntitySet, uriResourceEntitySet.getKeyPredicates());
            if(entity == null) {
                throw new ODataApplicationException("Entity not found",
                    HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
            }

            final byte[] mediaContent = storage.readMedia(entity);
            final InputStream responseContent = odata.createFixedFormatSerializer().binary(mediaContent);

            response.setStatusCode(HttpStatusCode.OK.getStatusCode());
            response.setContent(responseContent);
            response.setHeader(HttpHeader.CONTENT_TYPE, entity.getMediaContentType());
        } else {
            throw new ODataApplicationException("Not implemented",
                HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
        }
    }
```

Updating a media entity in our scenario is quite similar to read an entity. The first step is to analyse the URI, than fetch the entity from data store. Afer that we call the `updateMediaEntity` method. In our case we do not return any content. If we would return content, we must return the recently uploaded content of the media entity ([OData Version 4.0 Part 1: Protocol](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398338)).

```java
    @Override
    public void updateMediaEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
            ContentType requestFormat, ContentType responseFormat)
            throws ODataApplicationException, ODataLibraryException {

        final UriResource firstResoucePart = uriInfo.getUriResourceParts().get(0);
        if (firstResoucePart instanceof UriResourceEntitySet) {
            final EdmEntitySet edmEntitySet = Util.getEdmEntitySet(uriInfo);
            final UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) firstResoucePart;

            final Entity entity = storage.readEntityData(edmEntitySet, uriResourceEntitySet.getKeyPredicates());
            if (entity == null) {
                throw new ODataApplicationException("Entity not found",
                    HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
            }

            final byte[] mediaContent = odata.createFixedFormatDeserializer().binary(request.getBody());
            storage.updateMedia(entity, requestFormat.toContentTypeString(), mediaContent);

            response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
        } else {
            throw new ODataApplicationException("Not implemented",
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ENGLISH);
        }
    }
```

## Run the implemented service

After building and deploying the project, we can invoke our OData service.

- Read media entity set
  **GET** [http://localhost:8080/DemoService-Media/DemoService.svc/Advertisements](http://localhost:8080/DemoService-Media/DemoService.svc/Advertisments)
- Read media entity
  **GET** [http://localhost:8080/DemoService-Media/DemoService.svc/Advertisements(f89dee73-af9f-4cd4-b330-db93c25ff3c7)](http://localhost:8080/DemoService-Media/DemoService.svc/Advertisments(f89dee73-af9f-4cd4-b330-db93c25ff3c7))
- Read media entity content
  **GET** [http://localhost:8080/DemoService-Media/DemoService.svc/Advertisements(f89dee73-af9f-4cd4-b330-db93c25ff3c7)/$value]([http://localhost:8080/DemoService-Media/DemoService.svc/Advertisments(f89dee73-af9f-](http://localhost:8080/DemoService-Media/DemoService.svc/Advertisments(f89dee73-af9f-) 4cd4-b330-db93c25ff3c7)/$value)
- Create a new Media Entity
  **POST** [http://localhost:8080/DemoService-Media/DemoService.svc/Advertisements

Content-Type: image/svg+xml

```xml
    <?xml version="1.0" encoding="UTF-8"?>
        <svg xmlns="http://www.w3.org/2000/svg" version="1.1" viewBox="0 0 100 100">
            <g stroke="darkmagenta" stroke-width="16" fill="crimson">
                <circle cx="50" cy="50" r="42"/>
            </g>
    	</svg>
```

- Update the content of a media entity
  **PUT** [http://localhost:8080/DemoService-Media/DemoService.svc/Advertisements(f89dee73-af9f-4cd4-b330-db93c25ff3c7)/$value](http://localhost:8080/DemoService-Media/DemoService.svc/Advertisments(f89dee73-af9f-4cd4-b330-db93c25ff3c7)/$value)

Content-Type: text/plain

```text
Super super nice content
```

- Update the properties of a media entity
  **PUT** [http://localhost:8080/DemoService-Media/DemoService.svc/Advertisements(f89dee73-af9f-4cd4-b330-db93c25ff3c7)](http://localhost:8080/DemoService-Media/DemoService.svc/Advertisments(f89dee73-af9f-4cd4-b330-db93c25ff3c7))

Content-Type: application/json

```json
    {
        "Name": "New Name",
        "AirDate": "2020-06-05T23:00"
    }
```

- Delete a media entity
  **DELETE** [http://localhost:8080/DemoService-Media/DemoService.svc/Advertisements(f89dee73-af9f-4cd4-b330-db93c25ff3c7)](http://localhost:8080/DemoService-Media/DemoService.svc/Advertisments(f89dee73-af9f-4cd4-b330-db93c25ff3c7))
- Delete a media entity
  **DELETE** [http://localhost:8080/DemoService-Media/DemoService.svc/Advertisements(db2d2186-1c29-4d1e-88ef-127f521b9c67)/$value](http://localhost:8080/DemoService-Media/DemoService.svc/Advertisments(db2d2186-1c29-4d1e-88ef-127f521b9c67)/$value)

# Links

### Tutorials

Further topics to be covered by follow-up tutorials:

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial OData V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial OData V4 service, part 7: Media Entities
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.0.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Part 4: Navigation

## Introduction

In the present tutorial, we will learn how to implement navigation between 2 Entity Types in an OData V4 service.

**Note**

The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).  
This tutorial can be found in subdirectory *\samples\tutorials\p4\_navigation*

**Disclaimer**

Again, in the present tutorial, we will focus only on the relevant implementation, in order to keep the code small and simple.
The sample code shouldn’t be reused for advanced scenarios.

**Background**

Say, we have an electronics shop and we have a lot of products which we’re selling and these products can be notebooks or monitors or organizers, which are the categories.
We would have 3 requirements:

1. We want to show a list of all our categories, then select one and display a list of all products that belong to this category, e.g. all monitors
   In terms of OData, this is called navigation
2. From the list of our products, we want to choose one and display its category
3. We want to navigate from a category to its products and perform a READ operation on one of them.

**Example for navigating in a service**

We open the Categories collection: [http://localhost:8080/DemoService/DemoService.svc/Categories](http://localhost:8080/DemoService/DemoService.svc/Categories)

![CategoryCollection](olingo.apache.org/doc/odata4/tutorials/navigation/browser_categories.JPG "The Category collection")

We open the details of the first Category, the “Notebooks”-category:
[http://localhost:8080/DemoService/DemoService.svc/Categories(1)](http://localhost:8080/DemoService/DemoService.svc/Categories(1))

![CategoryEntity](olingo.apache.org/doc/odata4/tutorials/navigation/browser_categories1.jpg "Read single Category entity")

In order to display all products that are notebooks, we can navigate from the selected category to its products:
[http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products](http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products)

![ProductsOfCategory](olingo.apache.org/doc/odata4/tutorials/navigation/browser_categories1_products.jpg "After navigating from a  Category to the related Products")

In the above example we’ve executed a one-to-many navigation.

As mentioned in the Background section, it is also required to navigate from a selected product to its category, which is a to-one relation, like:
[http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category](http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category)

And finally, it is possible to navigate to a list of products and directly access one of them, e.g.
[http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products(1)](http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products(1))

All three cases are covered by the present tutorial.

**Table of Contents**

1. Prerequisites
2. Preparation
3. Implementating the navigation
   1. Declare the Metadata
   2. Implement the to-many navigation
   3. Implement the to-one navigation
   4. Implement the to-many navigation with key access
4. Run the implemented service
5. Summary
6. Links
7. Appendix: code snippets

---

# 1. Prerequisites

Same prerequisites as in [Tutorial Part 1: Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read) and [Tutorial Part 2: Read Entity](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep) as well as basic knowledge about the concepts presented in both tutorials.

---

# 2. Preparation

Follow [Tutorial Part 1: Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read) and [Tutorial Part 2: Read Entity](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep) or as shortcut import *Part 2: Read Entity, Read Property* into your Eclipse workspace.

Afterwards do a Deploy and run: it should be working.

---

# 3. Implementing the navigation

In our sample scenario, we want to navigate from a product to its category and from a category to a list of products.
In order to achieve this, we need to create a second Entity Type, "Category", and we need to specify *Navigation Properties* in both Entity Types.
Our model looks as follows:

![ODataModelNavigation](olingo.apache.org/doc/odata4/tutorials/navigation/model.JPG "Our OData Model with link between 2 Entity Types")

**Note**
When designing the OData model, we could think of specifying a property “ProductCategory” in the entity type “Product”.
E.g. a product with name “Brilliant flat and wide” would have the category “Monitors”.
But this is not necessary, because that information can be obtained by navigating to the respective “Category”-entity using the navigation property.
That way, we can keep the entity types lightweight, which is one of the intentions of OData.

## 3.1. Declare the metadata

In order to declare the metadata of our OData service, we open the class myservice.mynamespace.service.DemoEdmProvider\_

### 3.1.1. Extend the Entity Type “Product”

In the previous tutorial we’ve already created the metadata for the “Product” entity type:

```xml
    <EntityType Name="Product">
        <Key>
          <PropertyRef Name="ID" />
        </Key>
        <Property Name="ID" Type="Edm.Int32" Nullable="false" />
        <Property Name="Name" Type="Edm.String" Nullable="false" />
        <Property Name="Description" Type="Edm.String" Nullable="false" />
    </EntityType>
```

Now we have to add a navigation property.
That navigation property element has the following attributes:

**Name**
The name of the navigation property is used as segment in the URI
e.g. for the following URL: [http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category](http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category)
The segment “Category” is the name of the navigation property

**Type**
Here we specify the Entity Type to which we’re navigating.
e.g. for the following URL: [http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category](http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category)
we’re navigating to an entity which has the entity type “OData.Demo.Category”
(we still have to create this entity type in this tutorial)
Note that the fully qualified name has to be specified.
Note that here we don’t specify a collection, so we have a to-one relationship.

**Nullable**
Specifies if the navigation target is required.
If we don’t specify it, then the default is assumed to be “true”.
In our example we want to declare that every product must have a category, so we have to set it to “false”

**Partner**
An optional attribute, used to define a bi-directional relationship.
Specifies a path from the entity type (specified here) to the navigation property (defined there).
In our example, we can navigate from product to category and from category to product

In our example, the metadata of our “Product” entity type looks as follows:

```xml
     <EntityType Name="Product">
       <Key>
         <PropertyRef Name="ID"/>
       </Key>
       <Property Name="ID" Type="Edm.Int32"/>
       <Property Name="Name" Type="Edm.String"/>
       <Property Name="Description" Type="Edm.String"/>
       <NavigationProperty Name="Category" Type="OData.Demo.Category" Nullable="false" Partner="Products"/>
     </EntityType>
```

Implementation-wise we have to create and configure an object of type `CsdlNavigationProperty`:

```java
    CsdlNavigationProperty navProp = new CsdlNavigationProperty()
                                        .setName("Category")
                                        .setType(ET_CATEGORY_FQN)
                                        .setNullable(false)
                                        .setPartner("Products");
```

Since an entity type can have multiple navigation properties, we have to put it into a list:

```java
    List<CsdlNavigationProperty> navPropList = new ArrayList<CsdlNavigationProperty>();
    navPropList.add(navProp);
```

That list becomes relevant for the entity type that has been created earlier:

```java
    entityType.setNavigationProperties(navPropList);
```

There’s one more step to consider with respect to the navigation: the entity set.
At runtime, we need to know how to implement the navigation, when an entity set is invoked.
For this purpose, the OData specifies the `NavigationPropertyBinding` element, which is a child element of the entity set and should be defined for each navigation property.
That `NavigationPropertyBinding` has the following attributes:

**Path**
Here we specify the name of the corresponding navigation property.
In our example, the navigation property that we’ve defined above is named “Category”

**Target**
Here we specify the entity set where we’re navigating to.
In our example it is the entity set “Categories” (which we will create below)

In our example, the definition of our “Products” entity set looks as follows:

```xml
    <EntitySet Name="Products" EntityType="OData.Demo.Product">
      <NavigationPropertyBinding Path="Category" Target="Categories"/>
    </EntitySet>
```

Code-wise, the getEntitySet method is extended as follows:

```java
    CsdlNavigationPropertyBinding navPropBinding = new CsdlNavigationPropertyBinding();
    navPropBinding.setPath("Category"); // the path from entity type to navigation property
    navPropBinding.setTarget("Categories"); //target entitySet, where the nav prop points to
    List<CsdlNavigationPropertyBinding> navPropBindingList = new ArrayList<CsdlNavigationPropertyBinding>();
    navPropBindingList.add(navPropBinding);
    entitySet.setNavigationPropertyBindings(navPropBindingList);
```

### 3.1.2. Create the Entity Type “Category”

Now we have to create the second entity type, the “Category”.
In order to keep our sample as simple as possible, we define only 2 properties, the “ID” and a “Name”.
Since we want to be able to navigate from one given category (e.g. “Monitors”) to a list of products (e.g. all products that are monitors), we have to specify a navigation property in this entity type as well.
Here, the navigation property has the following attributes:

**Name**
In our example, we specify “Products”, in plural because we want to get multiple entities.

**Type**
The “Type” attribute can be either an “entity type” or a “collection of entity types”
In our example this time, we specify a “collection” of “OData.Demo.Product”

**Nullable**
According to the OData specification (see odata.org), this attribute is not allowed for a collection
A collection can be empty, but never null.

**Partner**
In our example, we’re defining a bi-directional navigation, so here we specify “Category”, the name of the navigation property defined above.

In our example, the metadata of our “Category” entity type looks as follows:

```xml
    <EntityType Name="Category">
      <Key>
        <PropertyRef Name="ID"/>
      </Key>
      <Property Name="ID" Type="Edm.Int32"/>
      <Property Name="Name" Type="Edm.String"/>
      <NavigationProperty Name="Products" Type="Collection(OData.Demo.Product)" Partner="Category"/>
    </EntityType>
```

The code for the “Category” entity type:

```java
    if (entityTypeName.equals(ET_CATEGORY_FQN)){
	    //create EntityType properties
	    CsdlProperty id = new CsdlProperty()
                                .setName("ID")
                                .setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName());
	    CsdlProperty name = new CsdlProperty()
                                .setName("Name")
                                .setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

	    // create PropertyRef for Key element
	    CsdlPropertyRef propertyRef = new CsdlPropertyRef();
	    propertyRef.setName("ID");

	    // navigation property: one-to-many
	    CsdlNavigationProperty navProp = new CsdlNavigationProperty()
                                .setName("Products")
                                .setType(ET_PRODUCT_FQN)
                                .setCollection(true)
                                .setPartner("Category");
	    List<CsdlNavigationProperty> navPropList = new ArrayList<CsdlNavigationProperty>();
	    navPropList.add(navProp);

    	// configure EntityType
    	entityType = new CsdlEntityType();
    	entityType.setName(ET_CATEGORY_NAME);
    	entityType.setProperties(Arrays.asList(id, name));
    	entityType.setKey(Arrays.asList(propertyRef));
    	entityType.setNavigationProperties(navPropList);
    }
```

The `NavigationPropertyBinding` element and its attributes for the entity set “Categories”:

**Path**
In our example, the navigation property that we’ve defined above is named “Products”

**Target**
In our example it is the entity set “Products”

In our example, the definition of our “Categories” entity set looks as follows:

```xml
    <EntitySet Name="Categories" EntityType="OData.Demo.Category">
      <NavigationPropertyBinding Path="Products" Target="Products"/>
    </EntitySet>
```

And the implementation in the `getEntitySet` method:

```java
    CsdlNavigationPropertyBinding navPropBinding = new CsdlNavigationPropertyBinding();
    navPropBinding.setTarget("Products");//target entitySet, where the nav prop points to
    navPropBinding.setPath("Products"); // the path from entity type to navigation property
    List<CsdlNavigationPropertyBinding> navPropBindingList = new ArrayList<CsdlNavigationPropertyBinding>();
    navPropBindingList.add(navPropBinding);
    entitySet.setNavigationPropertyBindings(navPropBindingList);
```

---

## 3.2. Implement the to-many navigation

Let’s again have a look at our example, as described in the introduction section above.
The user of our service invokes the “Categories” collection and chooses one “Category”.
This is done with e.g. the following URL: [http://localhost:8080/DemoService/DemoService.svc/Categories(1)](http://localhost:8080/DemoService/DemoService.svc/Categories(1))

The returned response payload doesn’t contain any information about possible navigation.
So the user has to check the metadata document, where he can see that the entity type “Category” defines one navigation property:

![CategoryMetadata](olingo.apache.org/doc/odata4/tutorials/navigation/browser_metadataCategory.JPG "The definition of a Navigation Property in the $metadata document")

This means, that he can append the navigation property name to his URL, which takes him to the set of “Products” that belong to the chosen “Category”: [http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products](http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products)

From the metadata we can see that the “Type” attribute defines a collection.
This means that the implementation has to be done in the `EntityCollectionProcessor`, since we have to provide a collection of entities.

Open the class `myservice.mynamespace.service.DemoEntityCollectionProcessor.java`

There, the implementation for a “normal” read operation is already in place and we have to add the case when an entity collection is expected after navigation.
Note that we want to keep our tutorial and our code simple, so we decide that only one step navigation is to be supported by our service.
This means that we can navigate only once from one entity to another one.
For example:
Categories(1)/Products
We don’t support navigation from one entity to an entity and then to another entity and so on
For example:
Categories(1)/Products(1)/Category

Based on this assumption, in our `EntityCollectionProcessor`, we can rely on the fact that the URI can have either one or two segments.
This means: we can be called for the following kind of URLs:

Example URL for one sement: [http://localhost:8080/DemoService/DemoService.svc/Categories](http://localhost:8080/DemoService/DemoService.svc/Categories)

Example URL for two segments: [http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products](http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products)

As such, in our code we distinguish these 2 cases:

```java
    if(segmentCount == 1){
        // here the “normal” entity set is requested
    }else if (segmentCount == 2){
        // this is reached in case of navigation: DemoService.svc/Categories(1)/Products
    }else{
        // in our example, we don’t support URIs like Products(1)/Category/Products
        throw new ODataApplicationException("Not supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),Locale.ENGLISH);
    }
```

The segments of the URI are retrieved from the uriInfo parameter:

```java
    List<UriResource> resourceParts = uriInfo.getUriResourceParts();
    int segmentCount = resourceParts.size();
```

In both cases, we have to retrieve the list of entities to be returned.
For the first case, we have only one entitySet, so the implementation is straight forward:

```java
    EdmEntitySet startEdmEntitySet = uriResourceEntitySet.getEntitySet();
    if(segmentCount == 1) {
        // 2nd: fetch the data from backend for this requested EntitySetName
        responseEntityCollection = storage.readEntitySetData(responseEdmEntitySet);
        responseEdmEntitySet = startEdmEntitySet; //there’s only one entity set
    }
```

Now let’s focus on the second case, the navigation.

Our tasks are:

1. depending on the chosen key of the first segment, we have to compute which and how many entities exactly have to be returned. With other words, find the right data in the backend
   e.g. for the category “monitors”, we have to find the right products that are monitors
2. find out, which entity set has to be returned (can be products, categories, etc)
   This `EdmEntitySet` is required in order to properly build the context URL

The following sections explain how to do that.

### 3.2.1. Get the data for the response

Getting the data for the response is reylized in 2 steps:

**A)** get the data for the first URI segment
in our example, we have to perform a read operation for retrieving the Category with ID 3, which is "Monitors"

**B)** get the data for the navigation
in our example, we have to find the products that are monitors.

With respect to data, remember that we're using sample data that we create in our `Storage` class which represents our kind of database-mock.  
On startup of our service, we initialize some sample products and categories.
During initialization, there’s no assignment of products to its categories.
In our sample code, we’re doing this when requested in a hard-coded method in our `Storage` class.

**A) get the data for the first URI segment**

In our example, the URL would be: [http://localhost:8080/DemoService/DemoService.svc/Categories(3)/Products](http://localhost:8080/DemoService/DemoService.svc/Categories(3)/Products)

For this example, we would have to retrieve the Category with *ID=3*.  
The code looks like a normal `READ` operation:

```java
    List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
    Entity sourceEntity = storage.readEntityData(startEdmEntitySet, keyPredicates);
```

In our example, the result is an entity that represents the “Monitors” – category.

**B) get the data for the navigation**

Now we have to follow the navigation, based on the retrieved entity.  
In our example, we have to retrieve all products that are monitors.

This is backend logic, so we can directly call a helper method in our Storage class:

```java
    responseEntityCollection = storage.getRelatedEntityCollection(sourceEntity, targetEntityType);
```

This helper method requires the source entity and returns the target collection.
Additionally, the method needs the `EdmEntityType` that corresponds to the requested target.
In our example, we pass the “Category” (i.e. "Monitors") as source entity and the navigation target entity type, which is “Product”.
As a result, we get the desired “Products” collection, all products that are monitors.

After this step, we’re almost done, because we have the entity collection that our OData service returns in the response body.
We only need to do some more hand work: the response entity collection has to be serialized and the serializer which is in charge of doing that has to be configured properly.
For that we need the `EdmEntitySet` that corresponds to the response.
Since it is different in case of navigation and non-navigation, we still need to retrieve it for the case of navigation.

### 3.2.2. Retrieve the EdmEntitySet for the response

First, we have to analyze the URI, and find out if the URI segment is used for navigation.
As mentioned, in our simple example we assume that the second segment is used for navigation (in advanced services, a segment could as well be an action or function import, etc).
The navigation URI segment can then be asked for the corresponding `EdmNavigationProperty`

```java
    UriResource lastSegment = resourceParts.get(1);
    if(lastSegment instanceof UriResourceNavigation){
	    UriResourceNavigation uriResourceNavigation = (UriResourceNavigation)lastSegment;
	    EdmNavigationProperty edmNavigationProperty = uriResourceNavigation.getProperty();
```

The bad news is that the `EdmNavigationProperty` doesn’t know about the target `EdmEntitySet`.
This is as per design, just check the metadata:

```xml
    <EntityType Name="Category">
	    ...
	    <NavigationProperty Name="Products" Type="Collection(OData.Demo.Product)" Partner="Category"/>
    </EntityType>
```

The navigation property is defined on entity-type-level and as such, it does know the target entity type.
The target entity set is defined in the navigation property binding element on entity-set-level:

```xml
    <EntitySet Name="Categories" EntityType="OData.Demo.Category">
	    <NavigationPropertyBinding Path="Products" Target="Products"/>
    </EntitySet>
```

This is where we get the information that we need.

For our implementation, this means:

1. we need the `EdmEntitySet` that corresponds to the first segment of the URI
   in our example: Categories
2. we need the navigation property that corresponds to the second segment of the URI
   in our example: Products

As shown below, from the source `EdmEntitySet` we get the binding target, based on the navigation property.

```java
    EdmBindingTarget edmBindingTarget = startEdmEntitySet.getRelatedBindingTarget(navPropName);
    if(edmBindingTarget instanceof EdmEntitySet){
	    navigationTargetEntitySet = (EdmEntitySet)edmBindingTarget;
```

This target is the entity set that we need.

We move the code into the utility method `Util.getNavigationTargetEntitySet(startEdmEntitySet, edmNavigationProperty)`
Reason is that we'll need it again, later in this tutorial.

### 3.2.3 Remaining tasks

In the previous tutorials we’ve already learned what else has to be done: transform the retrieve data into an `InputStream` i.e. serialize the content.
Furthermore, configure the response object, i.e. set the response body, the content type and the header.

The following snippet shows the implementation of the `readEntityCollection(…)` method.

```java
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
                                    throws ODataApplicationException, SerializerException {

	EdmEntitySet responseEdmEntitySet = null; // for building ContextURL
	EntityCollection responseEntityCollection = null; // for the response body

	// 1st retrieve the requested EntitySet from the uriInfo
	List<UriResource> resourceParts = uriInfo.getUriResourceParts();
	int segmentCount = resourceParts.size();

	UriResource uriResource = resourceParts.get(0); // the first segment is the EntitySet
	if (! (uriResource instanceof UriResourceEntitySet)) {
		throw new ODataApplicationException("Only EntitySet is supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),Locale.ROOT);
	}

	UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) uriResource;
	EdmEntitySet startEdmEntitySet = uriResourceEntitySet.getEntitySet();

	if(segmentCount == 1){ // this is the case for: DemoService/DemoService.svc/Categories
		responseEdmEntitySet = startEdmEntitySet; // first (and only) entitySet

		// 2nd: fetch the data from backend for this requested EntitySetName
		responseEntityCollection = storage.readEntitySetData(startEdmEntitySet);
	}else if (segmentCount == 2){ //navigation: e.g. DemoService.svc/Categories(3)/Products
		UriResource lastSegment = resourceParts.get(1); // don't support more complex URIs
		if(lastSegment instanceof UriResourceNavigation){
			UriResourceNavigation uriResourceNavigation = (UriResourceNavigation)lastSegment;
			EdmNavigationProperty edmNavigationProperty = uriResourceNavigation.getProperty();
			EdmEntityType targetEntityType = edmNavigationProperty.getType();
			responseEdmEntitySet = Util.getNavigationTargetEntitySet(startEdmEntitySet, edmNavigationProperty);

			// 2nd: fetch the data from backend
			// first fetch the entity where the first segment of the URI points to
            // e.g. Categories(3)/Products first find the single entity: Category(3)
			List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
			Entity sourceEntity = storage.readEntityData(startEdmEntitySet, keyPredicates);
			// error handling for e.g.  DemoService.svc/Categories(99)/Products
			if(sourceEntity == null) {
                throw new ODataApplicationException("Entity not found.", HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ROOT);
			}
			// then fetch the entity collection where the entity navigates to
			responseEntityCollection = storage.getRelatedEntityCollection(sourceEntity, targetEntityType);
		}
	}else{ // this would be the case for e.g. Products(1)/Category/Products
		throw new ODataApplicationException("Not supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),Locale.ROOT);
	}
        // 3rd: create and configure a serializer
        ContextURL contextUrl = ContextURL.with().entitySet(responseEdmEntitySet).build();
        final String id = request.getRawBaseUri() + "/" + responseEdmEntitySet.getName();
        EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().contextURL(contextUrl).id(id).build();
        EdmEntityType edmEntityType = responseEdmEntitySet.getEntityType();

        ODataSerializer serializer = odata.createSerializer(responseFormat);
        SerializerResult serializerResult = serializer.entityCollection(this.srvMetadata, edmEntityType, responseEntityCollection, opts);

        // 4th: configure the response object: set the body, headers and status code
        response.setContent(serializerResult.getContent());
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());   
    }
```

## 3.3. Implement the to-one navigation

As for the to-one navigation, it is the case if the navigation target is a single entity, not a collection.
In our example, the following URL represents a to-one navigation: [http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category](http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category)

The user of our service has chosen a product and wants to know to which category it belongs. He can find it out by following the navigation property.
As per design, a product can only belong to **one** category (obviously, a product can only be a Notebook or a Monitor, not both). Therefore in our service, we’ve defined a navigation property that is not of type collection:

```xml
    <NavigationProperty
        Name="Category"
        Type="OData.Demo.Category"
        Nullable="false"
        Partner="Products"/>
```

So when the user follows the navigation property in order to display the product category, he expects a response that contains only one entry.
This means that we have to do the implementation in the `EntityProcessor`.

Open the class `myservice.mynamespace.service.DemoEntityProcessor.java`

As usual, we first have to analyze the URI.
Just like we did in the `EntityCollectionProcessor`, we have to distinguish between navigation and “normal” read of an entity:

```java
    if(segmentCount == 1){
        // in case of directly adressing of an entity
    }else if (segmentCount == 2){
        // this is reached in case of navigation
    }
```

In the following section, we will focus on the navigation case only.
In our example, our task is to find the category of a chosen product.
Again, we have to first fetch the chosen product (first URI segment) from our database-mock and in a second step, we have to ask our database-mock for the corresponding category.
This final entity is then serialized and set as response body for the `readEntity()` method, which we’re implementing.

**A) get the data for the first URI segment**

In our example, we have to perform a read operation for retrieving the product with ID 1: [http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category](http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category)

The code is the same like in the previous chapter (to-many navigation):

```java
    List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
    Entity sourceEntity = storage.readEntityData(startEdmEntitySet, keyPredicates);
```

**B) get the data for the navigation**

Now we have to follow the navigation, based on the retrieved entity.
In our example, we have to find the category corresponding to the chosen product.
Therefore, we invoke our helper method and pass the source Entity (Product) and the required target entity type (Category). The method will find the category which is related to the chosen product.

```java
    responseEntity = storage.getRelatedEntity(sourceEntity, responseEdmEntityType);
```

Before we can serialize the `responseEntity`, we have to retrieve the `EdmEntitySet` that corresponds to the response entity, because we need it for building the ContextURL.

The procedure is the same like in the chapter above, where we treated the to-many navigation:

```java
    EdmNavigationProperty edmNavigationProperty = uriResourceNavigation.getProperty();
    responseEdmEntityType = edmNavigationProperty.getType();
    responseEdmEntitySet = Util.getNavigationTargetEntitySet(startEdmEntitySet, edmNavigationProperty);
```

In our example, the value of the variable `responseEdmEntitySet` will be “Categories” and it will be used for building the contextURL, which will look as follows:

```xml
    "$metadata#Categories/$entity"
```

## 3.4. Implement the to-many navigation with key access

"Navigation with key access" means that we have a to-many navigation, like navigating from a chosen category to the list of corresponding products: [http://localhost:8080/DemoService/DemoService.svc/Categories(3)/Products](http://localhost:8080/DemoService/DemoService.svc/Categories(3)/Products)

but in addition, we want to read only one of the collected products, which is directly addressed by its key: [http://localhost:8080/DemoService/DemoService.svc/Categories(3)/Products(5)](http://localhost:8080/DemoService/DemoService.svc/Categories(3)/Products(5))

From this URL, we can assume that the `EntityProcessor` is the relevant place to handle this request in our code.

The steps to find the requested entity are:

1. Do a read operation for the first segment (same as in the previous chapter)
   In our example, this would be read entity for: */Categories(3)*
2. Follow the navigation to get the collection of the second segment
   In our example, this would be get the entity collection for: */Categories(3)/Products*
3. Pick the requested entity from the collection
   In our example, retrieve the product with ID=5, which is contained in the collection */Categories(3)/Products(5)*

We can assume, that our database-mock is able to perform step 2 and 3 together.

In our class `myservice.mynamespace.service.DemoEntityProcessor.java`, we’ve already added the navigation capability for to-one navigation.
How can we find out that we aren’t called for a to-one navigation, but instead, we’re responding to a to-many navigation with key access?
The difference is the “key predicate”.
The necessary info about it can be obtained from the URI segment.
In the first chapter, we’ve already learned that there’s a special interface responsible for navigation segments, the `org.apache.olingo.server.api.uri.UriResourceNavigation`
It also provides a method `getKeyPredicates()`
We can make use of it in order to distinguish between “to-one navigation” and “navigation with key access”.
If the call to

```java
    List<UriParmeter> navKeyPredicates = uriResourceNavigation.getKeyPredicates();
```

returns an empty list, then we can assume that our OData service has been called for a “to-one navigation”.
This to-one navigation has been explained in the chapter 3.3. above.
If the service request is like
/Categories(3)/Products(5)
then the method `getKeyPredicates()` will return a list of with one element that contains ID=5

In our implementation of the `EntityProcessor`, we add the following code:

```java
    List<UriParameter> navKeyPredicates = uriResourceNavigation.getKeyPredicates();
    if(navKeyPredicates.isEmpty()){
	    responseEntity = storage.getRelatedEntity(sourceEntity, responseEdmEntityType);
    }else{
	    responseEntity = storage.getRelatedEntity(sourceEntity, responseEdmEntityType, navKeyPredicates);
    }
```

We get the key predicates for the navigation segment.
Then we check if returned list is empty.
If yes, we use the line that we implemented in chapter 3.3.
If not, we have to create a new helper method that uses the key predicates for retrieving the desired entity.
The new helper method

```java
    responseEntity = storage.getRelatedEntity(sourceEntity, responseEdmEntityType, navKeyPredicates);
```

will take care of getting the collection of products (`responseEntityType`) that are in scope of the chosen category (`sourceEntity`) and will then pick the requested product, based on the given key (`navKeyPredicates`).

One last thing to consider:  
As we mentioned above, the user of our service is expected to specify a key of a product that is contained in the collection of products (e.g. */Categories(3)/Products*) that is addressed by e.g. */Categories(3)/Products(5)*.

But he might specify a product ID that is existing, but not valid for the addressed navigation e.g. */Categories(3)/Products(1)*.

With other words: it is not valid to navigate from category "Monitors" to a product like "Notebook Basic 15"

If this is the case, we have to throw an appropriate exception.
However, in our simple example we’re satisfied with simply checking if an entity was found at all:

```java
    if(responseEntity == null) {
	    throw new ODataApplicationException("Nothing found.", HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ROOT);
    }
```

**Note**
When implementing this navigation for the first time, our first intention might have been:
Let’s just ignore the first segment and simply do a read operation for /Products(5)
Which would mean, from the list of all products, pick the one with ID=5
Why not?
The answer is that we cannot assume that the requested Product is automatically belonging to the specified Category.
E.g. in our example, the following URI should throw an error:
Categories(3)/Products(1)
As we know, Categories(3) is “Monitors” and Product(1) is a “Notebook”

That’s it.
We don’t need to do an additional effort to retrieve the `EdmEntitySet` for the ContextURL, because this has already been implemented in our `DemoEntityProcessor` in the context of the previous chapter 3.3.

So now we can finally have a look at the full implementation of the `readEntity()` method, the covers both the cases of chapter 3.3. and 3.4.

```java
    public void readEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
				throws ODataApplicationException, SerializerException {

    	EdmEntityType responseEdmEntityType = null; // we'll need this to build the ContextURL
    	Entity responseEntity = null; // required for serialization of the response body
    	EdmEntitySet responseEdmEntitySet = null; // we need this for building the contextUrl

    	// 1st step: retrieve the requested Entity:
        // can be "normal" read operation, or navigation (to-one)
    	List<UriResource> resourceParts = uriInfo.getUriResourceParts();
    	int segmentCount = resourceParts.size();

    	UriResource uriResource = resourceParts.get(0);
    	if (! (uriResource instanceof UriResourceEntitySet)) {
    		throw new ODataApplicationException("Only EntitySet is supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
    		                                        Locale.ROOT);
    	}

    	UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) uriResource;
    	EdmEntitySet startEdmEntitySet = uriResourceEntitySet.getEntitySet();

    	// Analyze the URI segments
    	if(segmentCount == 1){  // no navigation
    		responseEdmEntityType = startEdmEntitySet.getEntityType();
    		responseEdmEntitySet = startEdmEntitySet; // since we have only one segment

    		// 2. step: retrieve the data from backend
    		List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
    		responseEntity = storage.readEntityData(startEdmEntitySet, keyPredicates);
    	} else if (segmentCount == 2){ //navigation
    		UriResource navSegment = resourceParts.get(1);
    		if(navSegment instanceof UriResourceNavigation){
    			UriResourceNavigation uriResourceNavigation = (UriResourceNavigation) navSegment;
    			EdmNavigationProperty edmNavigationProperty = uriResourceNavigation.getProperty();
    			responseEdmEntityType = edmNavigationProperty.getType();
    			responseEdmEntitySet = Util.getNavigationTargetEntitySet(startEdmEntitySet, edmNavigationProperty);

    			// 2nd: fetch the data from backend.
                // for:  Products(1)/Category  we have to find the correct Category entity
    			List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
                // e.g. for Products(1)/Category we have to find first the Products(1)
    			Entity sourceEntity = storage.readEntityData(startEdmEntitySet, keyPredicates);

    			// now we have to check if the navigation is
    			// a) to-one: e.g. Products(1)/Category
    			// b) to-many with key: e.g. Categories(3)/Products(5)
    			List<UriParameter> navKeyPredicates = uriResourceNavigation.getKeyPredicates();

    			if(navKeyPredicates.isEmpty()){
                    // e.g. DemoService.svc/Products(1)/Category
    				responseEntity = storage.getRelatedEntity(sourceEntity, responseEdmEntityType);
    			}else{ // e.g. DemoService.svc/Categories(3)/Products(5)
    				responseEntity = storage.getRelatedEntity(sourceEntity, responseEdmEntityType, navKeyPredicates);
    			}
    		}
    	}else{
    		// this would be the case for e.g. Products(1)/Category/Products(1)/Category
    		throw new ODataApplicationException("Not supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    	}

    	if(responseEntity == null) {
    		// this is the case for e.g. DemoService.svc/Categories(4) or
            // DemoService.svc/Categories(3)/Products(999)
    		throw new ODataApplicationException("Nothing found.", HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ROOT);
    	}

    	// 3. serialize
    	ContextURL contextUrl = ContextURL.with().entitySet(responseEdmEntitySet).suffix(Suffix.ENTITY).build();
    	EntitySerializerOptions opts = EntitySerializerOptions.with().contextURL(contextUrl).build();

    	ODataSerializer serializer = this.odata.createSerializer(responseFormat);
    	SerializerResult serializerResult = serializer.entity(this.srvMetadata, responseEdmEntityType, responseEntity, opts);

    	//4. configure the response object
    	response.setContent(serializerResult.getContent());
    	response.setStatusCode(HttpStatusCode.OK.getStatusCode());
    	response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

---

# 4. Run the implemented service

After building and deploying your service to your server, you can try the following URLs:

- Metadata and Service documents
  - [http://localhost:8080/DemoService/DemoService.svc/$metadata](http://localhost:8080/DemoService/DemoService.svc/$metadata)
  - [http://localhost:8080/DemoService/DemoService.svc](http://localhost:8080/DemoService/DemoService.svc)
- “Normal” query of both entity sets
  - [http://localhost:8080/DemoService/DemoService.svc/Products](http://localhost:8080/DemoService/DemoService.svc/Products)
  - [http://localhost:8080/DemoService/DemoService.svc/Categories](http://localhost:8080/DemoService/DemoService.svc/Categories)
- “Normal” read of both entity types
  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)](http://localhost:8080/DemoService/DemoService.svc/Products(1))
  - [http://localhost:8080/DemoService/DemoService.svc/Categories(3)](http://localhost:8080/DemoService/DemoService.svc/Categories(3))
- “to-many” navigation
  - [http://localhost:8080/DemoService/DemoService.svc/Categories(2)/Products](http://localhost:8080/DemoService/DemoService.svc/Categories(2)/Products)
- “to-one” navigation
  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category](http://localhost:8080/DemoService/DemoService.svc/Products(1)/Category)
- “to-many” navigation with key access
  - [http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products(2)](http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products(2))
- “to-many” navigation with key access of invalid key, throwing an error
  - [http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products(3)](http://localhost:8080/DemoService/DemoService.svc/Categories(1)/Products(3))

---

# 5. Summary

In this tutorial we have learned how to add navigation capabilities to an OData service.
We’ve implemented the to-many and to-one relationship and also the READ access to one entity after a to-many navigation.
We’ve restricted the navigation to two segments, no more than navigating from one entity to another one.
The modification of relations has not been covered by this tutorial.
Check the *Links* section for more OData V4 tutorials.

---

# 6. Links

### Tutorials

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: Navigation
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial ODATA V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial ODATA V4 service, part 7: [Media Entities](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.0.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

---

# 7. Appendix: code snippets

When reaching the point where your OData service has to become production ready and support complex scenarions, you’ll find the following code snippets useful.

## 7.1. Find the EdmEntitySet for the navigation target

```java
    public static EdmEntitySet getNavigationTargetEntitySet(final UriInfoResource uriInfo) throws ODataApplicationException {

    	EdmEntitySet entitySet;
    	final List<UriResource> resourcePaths = uriInfo.getUriResourceParts();

    	// First must be entity set (hence function imports are not supported here).
    	if (resourcePaths.get(0) instanceof UriResourceEntitySet) {
    		entitySet = ((UriResourceEntitySet) resourcePaths.get(0)).getEntitySet();
    	} else {
    		throw new ODataApplicationException("Invalid resource type.",
    				HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ROOT);
    	}

    	int navigationCount = 0;
    	while (entitySet != null
    		&& ++navigationCount < resourcePaths.size()
    		&& resourcePaths.get(navigationCount) instanceof UriResourceNavigation) {
    		final UriResourceNavigation uriResourceNavigation = (UriResourceNavigation) resourcePaths.get(navigationCount);
    		final EdmBindingTarget target = entitySet.getRelatedBindingTarget(uriResourceNavigation.getProperty().getName());
    		if (target instanceof EdmEntitySet) {
    			entitySet = (EdmEntitySet) target;
    		} else {
    			throw new ODataApplicationException("Singletons not supported", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),
    			                                     Locale.ROOT);
    		}
    	}

    	return entitySet;
    }
```

## 7.2. Find the last navigation segment

```java
    public static UriResourceNavigation getLastNavigation(final UriInfoResource uriInfo) {

    	final List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
    	int navigationCount = 1;
    	while (navigationCount < resourcePaths.size()
    		&& resourcePaths.get(navigationCount) instanceof UriResourceNavigation) {
    		navigationCount++;
    	}

    	return (UriResourceNavigation) resourcePaths.get(--navigationCount);
    }
```

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-od4_basic_batch_client"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-od4_basic_batch_client--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-od4_basic_batch_client--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-od4_basic_batch_client--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to use the Batch Client API in OData V4

### Construction of OData Client

```text
ODataClient odata = ODataClientFactory.getClient();
odata.getConfiguration().setDefaultPubFormat(ContentType.APPLICATION_JSON);
```

### Construction of a client entity and create request

```text
ClientObjectFactory factory = getClient().getObjectFactory();
final ClientEntity entity = factory.newEntity("OData.Demo.Manufacturer");
entity.getProperties().add(factory.newPrimitiveProperty("Name", factory.newPrimitiveValueBuilder().buildString("MyCarManufacturer")));
     
final URI targetURI = getClient().newURIBuilder(serviceUrl).appendEntitySetSegment("Manufacturers").build();
final ODataEntityCreateRequest<ClientEntity> createRequest = getClient().getCUDRequestFactory().getEntityCreateRequest(targetURI, entity);
```

### Add a create request to a changeset

```text
BatchManager payloadManager = getClient().getBatchRequestFactory().getBatchRequest(serviceUrl).payloadManager();
final ODataChangeset changeset = payloadManager.addChangeset();
 
changeset.addRequest(createRequest);
```

### Construction of a query request

```text
final URI targetURI = getClient().newURIBuilder(serviceUrl).appendEntitySetSegment("Manufacturers").appendKeySegment(1).build();
final URI uri = isRelative ? URI.create(<ServiceUri>).relativize(targetURI) : targetURI;
 
ODataEntityRequest<ClientEntity> queryReq = getClient().getRetrieveRequestFactory().getEntityRequest(uri);
queryReq.setAccept(ContentType.APPLICATION_JSON);
```

### Add query request to payloadManager

```text
payload.addRequest(queryReq);
```

### Fetch the batch response

```text
final ODataBatchResponse response = payload.getResponse();
 
final Iterator<ODataBatchResponseItem> responseBodyIter = response.getBody();
final ODataBatchResponseItem changeSetResponse = responseBodyIter.next();
```

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-od4_basic_client_read"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-od4_basic_client_read--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-od4_basic_client_read--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-od4_basic_client_read--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# Client Scenario

---

## How To Guide for building a Sample OData Client with the OData 4.0 Library (for Java)

This Tutorial shows how to use the Apache Olingo Library for CRUD operations on an existing OData Service.
Therefore it contains the Explaining the Client section which explains how to implement the CRUD operations based on sample code.
For creating a simple odata service refer the section Basic Tutorial: Create an OData V4 Service with Olingo in Olingo V4 tutorial

### Client Quickstart Guide

With this Quickstart guide the runnable sample client and an sample service is created within a few minutes. Therefore it just requires an installed Java 6 Runtime, Maven 3 and an internet connection.
It also requires an odata service. This sample uses the Cars service which can be found under samples/server under olingo [git repository](https://gitbox.apache.org/repos/asf?p=olingo-odata4.git). Build the project and deploy the war on a server. Follow the [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites) to import the sample server project.

1. Create a sample maven project and name it OlingoSampleApp.
2. Create a new package org.apache.olingo.samples.client under this
   project.
3. Create a class OlingoSampleApp.java under package
   org.apache.olingo.samples.client.
4. Copy the entire [sample client code](#olingo-apache-org-doc-odata4-tutorials-od4_basic_client_read--sample) into this class.
5. Copy [this sample pom into](#olingo-apache-org-doc-odata4-tutorials-od4_basic_client_read--pom) into this projects pom.xml file.
6. Run OlingoSampleApp against sample Service
7. In order to fetch all the dependencies in the pom.xml file run
   eclipse:eclipse command on the sample client project

### Explaining the Client

### Create OData Client

```java
public static final ODataClient client = ODataClientFactory.getClient();
odataClient.getConfiguration().setDefaultPubFormat(ContentType.APPLICATION_JSON);
```

### Read EDM

For an OData Service the Entity Data Model (EDM) defines all metadata information about the provided data of the service. This includes all entities with their type, properties and relations, which entities are provided as entity sets and additional functions and operations provided by the OData Service. The EDM also have to be provided by the OData Service via a unique URI (e.g. [http://localhost:8080/cars.svc/$metadata](http://localhost:8080/cars.svc/$metadata)) in the EDMX format.
This fact is important because the Apache Olingo library requires the metadata for serialization and de-serialization of the data of an entity (e.g. the validation of the data is done against the EDM provided metadata). Hence the first step in this sample is to read the whole EDM of an OData Service.

### Code sample: Read EDM ($metadata)

```text
final Edm edm = getClient().getRetrieveRequestFactory().getMetadataRequest(serviceUrl).execute().getBody();
return edm;
```

If annotations defined in external vocabulary file has to be loaded then the below code has to be used

```java
List<InputStream> streams = new ArrayList<InputStream>();
//If file is locally available    
streams.add(getClass().getResourceAsStream("annotations.xml"));
XMLMetadata xmlMetadata = getClient().getRetrieveRequestFactory().getXMLMetadataRequest(serviceUrl).execute().getBody();
//If the reference uri's have to be loaded 
String vocabUrl = metadata.getReferences().get(0).getUri().toString();
URI uri = new URI(vocabUrl);
ODataRawRequest request = getClient().getRetrieveRequestFactory().getRawRequest(uri);
ODataRawResponse response = request.execute();
streams.add(response.getRawResponse());
final Edm edm = getClient().getReader().readMetadata(xmlMetadata, streams);
return edm;
```

Here the serviceUrl is the root Url of the odata service.
For read and de-serialize of the EDM this is all what have to be done and the resulting EDM instance than can be used for necessary serialization and de-serialization in combination of CRUD operations supported by Apache Olingo library.

### Read Entity

For reading entities this sample provides two methods. First is read of a complete collection / Entity Set and second is read of a single Entity. In general, for both first create the request and execute the request.

### Read entityCollection

```text
ODataEntitySetRequest<ClientEntitySet> request = getClient().getRetrieveRequestFactory()
        .getEntitySetRequest(getClient().newURIBuilder(serviceUrl)
        .appendEntitySetSegment(“Manufacturers”).build());
final ODataRetrieveResponse<ClientEntitySet> response = request.execute();
final ClientEntitySet entitySet = response.getBody();
```

For read of a complete collection the request URI is a EntitySet. Via the execute method the request is done against the created uri and the responding content is returned as HttpResponse. This HttpResponse then will be de-serialized by the library into an ClientEntitySet object which contains all entities, each entities navigation links provided by the OData Service.

### Read Entity

```text
ODataEntityRequest<ClientEntity> request = getClient().getRetrieveRequestFactory()
        .getEntityRequest(getClient().newURIBuilder(serviceUrl)
        .appendEntitySetSegment(“Manufacturers”).appendKeySegment(1).build());
final ODataRetrieveResponse<ClientEntity> response = request.execute();
final ClientEntity entity = response.getBody();
```

For read of a single ODataEntry the request URI is an Entity for which a key value is required for creation of the uri. Via the execute method the request is done against the created uri and the responding content is returned as HttpResponse. This HttpResponse then will be de-serialized by the library into an ClientEntity object which contains all properties of an entity, along with navigation links provided by the OData Service.

### Read Entity Property

```text
ODataPropertyRequest<ClientProperty> request = getClient().getRetrieveRequestFactory()
        .getPropertyRequest(odataClient.newURIBuilder(serviceUrl)
        .appendEntitySetSegment(“Manufacturers”).appendKeySegment(1)
        .appendPropertySegment(“Name”).build());
final ODataRetrieveResponse<ClientProperty> response = request.execute();
final ClientProperty property = response.getBody();
//If property is a primitive type and if value has to be fetched
final ClientProperty property = property.get("Name");
final ClientPrimitiveValue clientValue = property.getPrimitiveValue();
//If the property is a Complex Type and if value has to be fetched
// Here Address is a complex property
final ClientComplexValue complexValue = prop.getComplexValue();
final ClientValue propertyComp = complexValue.get("Street").getValue();
```

### Create Entity

To create an entity a HTTP POST on the corresponding entity set URI with the whole entity data as POST Body in a supported format (e.g. atom-xml, json) has to be done. With Apache Olingo the required POST Body can be created (serialized) with the methods available on ClientObjectFactory. This method creates a ClientEntity which contains the content (i.e. the required POST Body) which then can be send to the server. If the entry was created successfully an HTTP Status: 201 created will be returned as well as the complete entry.
For simplicity in the code sample below the prepare and execute of the POST and the read of the response is separated (see Part 1: Post and Part 2: Read).

### Code sample: Create single Entry

### Part 1: POST entry

```text
ClientEntity newEntity = getClient().getObjectFactory().newEntity(“Manufacturers”);
newEntity.getProperties().add(getClient().getObjectFactory().newPrimitiveProperty(“Name”,
        getFactory().newPrimitiveValueBuilder().buildString(“MyCarManufacturer”)));
newEntity.addLink(getClient().getObjectFactory().newEntityNavigationLink(“Cars”,
        client.newURIBuilder(serviceUrl)
            .appendEntitySetSegment(“Cars”)
            .appendKeySegment(1)
            .build()));
```

With the ODataClientFactory it is possible to create a new entity along with its properties, its values and also links.

### Part 2: Read response

```text
final ODataEntityCreateRequest<ClientEntity> createRequest = getClient().getCUDRequestFactory().getEntityCreateRequest(
        getClient().newURIBuilder(serviceUrl).appendEntitySetSegment(“Manufacturers”).build(),
        newEntity);
final ODataEntityCreateResponse<ClientEntity> createResponse = createRequest.execute();
final ClientEntity createdEntity = createResponse.getBody();
```

This executes the create request and the response will return the ClientEntity that was created.

### PUT entry

```java
final URI uri = odataClient.newURIBuilder(serviceUrl)
        .appendEntitySetSegment(“Manufacturers”).appendKeySegment(1).build();

final ClientEntity entity = getClient().getObjectFactory().newEntity(new FullQualifiedName(“OData.Demo.Manufacturer”));
entity.getProperties().add(getClient().getObjectFactory().newPrimitiveProperty(“Name”,
        getClient().getObjectFactory().newPrimitiveValueBuilder().buildString(“MyCarManufacturer”)));

final ODataEntityUpdateRequest<ClientEntity> requestUpdate = odataClient.getCUDRequestFactory().getEntityUpdateRequest(uri,  UpdateType.PATCH, entity);    
final ODataEntityUpdateResponse<ClientEntity> responseUpdate = requestUpdate.execute();
```

With the ODataClientFactory create the entity that has to be updated along with its properties and its values. Then execute the update request and the response will be the ClientEntity with no content.
If the entry was updated successfully an HTTP Status: 204 No content will be returned.

### Code sample: Delete single Entry

### Delete

For deletion of an entry just a DELETE request is necessary on the URI of the entity. Hence the Apache Olingo is not necessary to serialize or de-serialize anything for this use case.

```sql
final URI uri = getClient().newURIBuilder(serviceUrl).appendEntitySetSegment(“Manufacturers”).appendKeySegment(1).build();
final ODataDeleteRequest request = getClient().getCUDRequestFactory().getDeleteRequest(uri);
final ODataDeleteResponse response = request.execute();
```

So the code for delete of an entry the DELETE request URI is an Entity for which a key value is required for creation of the absolut uri. Via execute() method the request is done against the uri and the responding http status code is returned, which is, if the entry was deleted successfully, an HTTP Status: 204 No content.

<a id="olingo-apache-org-doc-odata4-tutorials-od4_basic_client_read--pom"></a>

### Sample pom.xml file

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">

  <modelVersion>4.0.0</modelVersion>

  <artifactId>odata-client-sample</artifactId>
  <packaging>jar</packaging>
  <name>${project.artifactId}</name>

  <parent>
    <groupId>org.apache.olingo</groupId>
    <artifactId>odata-samples</artifactId>
    <version>4.5.0-sap-05-SNAPSHOT</version>
    <relativePath>..</relativePath>
  </parent>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-deploy-plugin</artifactId>
        <configuration>
          <skip>true</skip>
        </configuration>
      </plugin>

      <!-- Disable checkstyle for sample -->
      <plugin>
        <artifactId>maven-checkstyle-plugin</artifactId>
        <configuration>
          <skip>true</skip>
        </configuration>
      </plugin>
    </plugins>
  </build>

  <dependencies>
    <dependency>
      <groupId>org.apache.olingo</groupId>
      <artifactId>odata-client-core</artifactId>
      <version>${project.version}</version>
    </dependency>
    <dependency>
      <groupId>commons-logging</groupId>
      <artifactId>commons-logging</artifactId>
      <version>${commons.logging.version}</version>
      <scope>runtime</scope>
    </dependency>
    <dependency>
      <groupId>org.slf4j</groupId>
      <artifactId>slf4j-simple</artifactId>
      <version>${sl4j.version}</version>
      <scope>runtime</scope>
    </dependency>
  </dependencies>
  <profiles>
    <profile>
      <id>client</id>
      <build>
        <defaultGoal>test</defaultGoal>
        <plugins>
          <plugin>
            <groupId>org.codehaus.mojo</groupId>
            <artifactId>exec-maven-plugin</artifactId>
            <executions>
              <execution>
                <phase>test</phase>
                <goals>
                  <goal>java</goal>
                </goals>
                <configuration>
                  <mainClass>org.apache.olingo.samples.client.OlingoSampleApp</mainClass>
                </configuration>
              </execution>
            </executions>
          </plugin>
        </plugins>
      </build>
    </profile>
  </profiles>
</project>
```

<a id="olingo-apache-org-doc-odata4-tutorials-od4_basic_client_read--sample"></a>

### OlingoSampleApp.java

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 ******************************************************************************/
package org.apache.olingo.samples.client;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.olingo.client.api.ODataClient;
import org.apache.olingo.client.api.communication.request.cud.ODataDeleteRequest;
import org.apache.olingo.client.api.communication.request.cud.ODataEntityCreateRequest;
import org.apache.olingo.client.api.communication.request.cud.ODataEntityUpdateRequest;
import org.apache.olingo.client.api.communication.request.cud.UpdateType;
import org.apache.olingo.client.api.communication.request.retrieve.EdmMetadataRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntityRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetIteratorRequest;
import org.apache.olingo.client.api.communication.response.ODataDeleteResponse;
import org.apache.olingo.client.api.communication.response.ODataEntityCreateResponse;
import org.apache.olingo.client.api.communication.response.ODataEntityUpdateResponse;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientCollectionValue;
import org.apache.olingo.client.api.domain.ClientComplexValue;
import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.domain.ClientEntitySetIterator;
import org.apache.olingo.client.api.domain.ClientEnumValue;
import org.apache.olingo.client.api.domain.ClientProperty;
import org.apache.olingo.client.api.domain.ClientValue;
import org.apache.olingo.client.api.serialization.ODataDeserializerException;
import org.apache.olingo.client.core.ODataClientFactory;
import org.apache.olingo.commons.api.edm.Edm;
import org.apache.olingo.commons.api.edm.EdmComplexType;
import org.apache.olingo.commons.api.edm.EdmEntityType;
import org.apache.olingo.commons.api.edm.EdmProperty;
import org.apache.olingo.commons.api.edm.EdmSchema;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.olingo.commons.api.format.ContentType;

/**
 *
 */
public class OlingoSampleApp {
  private ODataClient client;
  
  public OlingoSampleApp() {
    client = ODataClientFactory.getClient();
  }

  public static void main(String[] params) throws Exception {
    OlingoSampleApp app = new OlingoSampleApp();
    app.perform("http://localhost:8080/cars.svc");
  }

  void perform(String serviceUrl) throws Exception {

    print("\n----- Read Edm ------------------------------");
    Edm edm = readEdm(serviceUrl);
    List<FullQualifiedName> ctFqns = new ArrayList<FullQualifiedName>();
    List<FullQualifiedName> etFqns = new ArrayList<FullQualifiedName>();
    for (EdmSchema schema : edm.getSchemas()) {
      for (EdmComplexType complexType : schema.getComplexTypes()) {
        ctFqns.add(complexType.getFullQualifiedName());
      }
      for (EdmEntityType entityType : schema.getEntityTypes()) {
        etFqns.add(entityType.getFullQualifiedName());
      }
    }
    print("Found ComplexTypes", ctFqns);
    print("Found EntityTypes", etFqns);

    print("\n----- Inspect each property and its type of the first entity: " + etFqns.get(0) + "----");
    EdmEntityType etype = edm.getEntityType(etFqns.get(0));
    for (String propertyName : etype.getPropertyNames()) {
      EdmProperty property = etype.getStructuralProperty(propertyName);
      FullQualifiedName typeName = property.getType().getFullQualifiedName();
      print("property '" + propertyName + "' " + typeName);
    }
    
    print("\n----- Read Entities ------------------------------");
    ClientEntitySetIterator<ClientEntitySet, ClientEntity> iterator = 
      readEntities(edm, serviceUrl, "Manufacturers");

    while (iterator.hasNext()) {
      ClientEntity ce = iterator.next();
      print("Entry:\n" + prettyPrint(ce.getProperties(), 0));
    }

    print("\n----- Read Entry ------------------------------");
    ClientEntity entry = readEntityWithKey(edm, serviceUrl, "Manufacturers", 1);
    print("Single Entry:\n" + prettyPrint(entry.getProperties(), 0));

    //
    print("\n----- Read Entity with $expand  ------------------------------");
    entry = readEntityWithKeyExpand(edm, serviceUrl, "Manufacturers", 1, "Cars");
    print("Single Entry with expanded Cars relation:\n" + prettyPrint(entry.getProperties(), 0));

    //
    print("\n----- Read Entities with $filter  ------------------------------");
    iterator = readEntitiesWithFilter(edm, serviceUrl, "Manufacturers", "Name eq 'Horse Powered Racing'");
    while (iterator.hasNext()) {
      ClientEntity ce = iterator.next();
      print("Entry:\n" + prettyPrint(ce.getProperties(), 0));
    }

    // skip everything as odata4 sample/server only supporting retrieval
    print("\n----- Create Entry ------------------------------");
    ClientEntity ce = loadEntity("/mymanufacturer.json");
    entry = createEntity(edm, serviceUrl, "Manufacturers", ce);

    print("\n----- Update Entry ------------------------------");
    ce = loadEntity("/mymanufacturer2.json");
    int sc = updateEntity(edm, serviceUrl, "Manufacturers", 123, ce);
    print("Updated successfully: " + sc);
    entry = readEntityWithKey(edm, serviceUrl, "Manufacturers", 123);
    print("Updated Entry successfully: " + prettyPrint(entry.getProperties(), 0));

    print("\n----- Delete Entry ------------------------------");
    sc = deleteEntity(serviceUrl, "Manufacturers", 123);
    print("Deletion of Entry was successfully: " + sc);

    try {
      print("\n----- Verify Delete Entry ------------------------------");
      readEntityWithKey(edm, serviceUrl, "Manufacturers", 123);
    } catch(Exception e) {
      print(e.getMessage());
    }
  }

  private static void print(String content) {
    System.out.println(content);
  }

  private static void print(String content, List<?> list) {
    System.out.println(content);
    for (Object o : list) {
        System.out.println("    " + o);
    }
      System.out.println();
  }

  private static String prettyPrint(Map<String, Object> properties, int level) {
    StringBuilder b = new StringBuilder();
    Set<Entry<String, Object>> entries = properties.entrySet();

    for (Entry<String, Object> entry : entries) {
      intend(b, level);
      b.append(entry.getKey()).append(": ");
      Object value = entry.getValue();
      if(value instanceof Map) {
        value = prettyPrint((Map<String, Object>) value, level+1);
      } else if(value instanceof Calendar) {
        Calendar cal = (Calendar) value;
        value = SimpleDateFormat.getInstance().format(cal.getTime());
      }
      b.append(value).append("\n");
    }
    // remove last line break
    b.deleteCharAt(b.length()-1);
    return b.toString();
  }
  
  private static String prettyPrint(Collection<ClientProperty> properties, int level) {
    StringBuilder b = new StringBuilder();

    for (ClientProperty entry : properties) {
      intend(b, level);
      ClientValue value = entry.getValue();
      if (value.isCollection()) {
        ClientCollectionValue cclvalue = value.asCollection();
        b.append(prettyPrint(cclvalue.asJavaCollection(), level + 1));
      } else if (value.isComplex()) {
        ClientComplexValue cpxvalue = value.asComplex();
        b.append(prettyPrint(cpxvalue.asJavaMap(), level + 1));
      } else if (value.isEnum()) {
        ClientEnumValue cnmvalue = value.asEnum();
        b.append(entry.getName()).append(": ");
        b.append(cnmvalue.getValue()).append("\n");
      } else if (value.isPrimitive()) {
        b.append(entry.getName()).append(": ");
        b.append(entry.getValue()).append("\n");
      }
    }
    return b.toString();
  }

  private static void intend(StringBuilder builder, int intendLevel) {
    for (int i = 0; i < intendLevel; i++) {
      builder.append("  ");
    }
  }

  public Edm readEdm(String serviceUrl) throws IOException {
    EdmMetadataRequest request = client.getRetrieveRequestFactory().getMetadataRequest(serviceUrl);
    ODataRetrieveResponse<Edm> response = request.execute();
    return response.getBody();
  }

  public ClientEntitySetIterator<ClientEntitySet, ClientEntity> readEntities(Edm edm, String serviceUri,
    String entitySetName) {
    URI absoluteUri = client.newURIBuilder(serviceUri).appendEntitySetSegment(entitySetName).build();
    return readEntities(edm, absoluteUri);
  }

  public ClientEntitySetIterator<ClientEntitySet, ClientEntity> readEntitiesWithFilter(Edm edm, String serviceUri,
    String entitySetName, String filterName) {
    URI absoluteUri = client.newURIBuilder(serviceUri).appendEntitySetSegment(entitySetName).filter(filterName).build();
    return readEntities(edm, absoluteUri);
  }

  private ClientEntitySetIterator<ClientEntitySet, ClientEntity> readEntities(Edm edm, URI absoluteUri) {
    System.out.println("URI = " + absoluteUri);
    ODataEntitySetIteratorRequest<ClientEntitySet, ClientEntity> request = 
      client.getRetrieveRequestFactory().getEntitySetIteratorRequest(absoluteUri);
    request.setAccept("application/json");
    ODataRetrieveResponse<ClientEntitySetIterator<ClientEntitySet, ClientEntity>> response = request.execute(); 
      
    return response.getBody();
  }

  public ClientEntity readEntityWithKey(Edm edm, String serviceUri, String entitySetName, Object keyValue) {
    URI absoluteUri = client.newURIBuilder(serviceUri).appendEntitySetSegment(entitySetName)
      .appendKeySegment(keyValue).build();
    return readEntity(edm, absoluteUri);
  }

  public ClientEntity readEntityWithKeyExpand(Edm edm, String serviceUri, String entitySetName, Object keyValue,
    String expandRelationName) {
    URI absoluteUri = client.newURIBuilder(serviceUri).appendEntitySetSegment(entitySetName).appendKeySegment(keyValue)
      .expand(expandRelationName).build();
    return readEntity(edm, absoluteUri);
  }

  private ClientEntity readEntity(Edm edm, URI absoluteUri) {
    ODataEntityRequest<ClientEntity> request = client.getRetrieveRequestFactory().getEntityRequest(absoluteUri);
    request.setAccept("application/json;odata.metadata=full");
    ODataRetrieveResponse<ClientEntity> response = request.execute(); 
      
    return response.getBody();
  }
  
  private ClientEntity loadEntity(String path) throws ODataDeserializerException {
    InputStream input = getClass().getResourceAsStream(path);
    return client.getBinder().getODataEntity(client.getDeserializer(ContentType.APPLICATION_JSON).toEntity(input));
  }

  public ClientEntity createEntity(Edm edm, String serviceUri, String entitySetName, ClientEntity ce) {
    URI absoluteUri = client.newURIBuilder(serviceUri).appendEntitySetSegment(entitySetName).build();
    return createEntity(edm, absoluteUri, ce);
  }

  private ClientEntity createEntity(Edm edm, URI absoluteUri, ClientEntity ce) {
    ODataEntityCreateRequest<ClientEntity> request = client.getCUDRequestFactory()
      .getEntityCreateRequest(absoluteUri, ce);
    request.setAccept("application/json");
    ODataEntityCreateResponse<ClientEntity> response = request.execute(); 
      
    return response.getBody();
  }

  public int updateEntity(Edm edm, String serviceUri, String entityName, Object keyValue, ClientEntity ce) {
    URI absoluteUri = client.newURIBuilder(serviceUri).appendEntitySetSegment(entityName)
      .appendKeySegment(keyValue).build();
    ODataEntityUpdateRequest<ClientEntity> request = 
      client.getCUDRequestFactory().getEntityUpdateRequest(absoluteUri, UpdateType.PATCH, ce);
    request.setAccept("application/json;odata.metadata=minimal");
    ODataEntityUpdateResponse<ClientEntity> response = request.execute();
    return response.getStatusCode();
  }

  public int deleteEntity(String serviceUri, String entityName, Object keyValue) throws IOException {
    URI absoluteUri = client.newURIBuilder(serviceUri).appendEntitySetSegment(entityName)
      .appendKeySegment(keyValue).build();
    ODataDeleteRequest request = client.getCUDRequestFactory().getDeleteRequest(absoluteUri);
    request.setAccept("application/json;odata.metadata=minimal");
    ODataDeleteResponse response = request.execute();
    return response.getStatusCode();
  }
}
```

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-od4_quick_start_sample"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-od4_quick_start_sample--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-od4_quick_start_sample--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-od4_quick_start_sample--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# A QuickStart Guide with our OData 4.0 Sample service

---

### Overview

Olingo has prepared a very simple sample car service that can work as a starting point for implementing a custom OData service. This service consists of a very simple EDM with two entity sets that are cars and manufactures and a memory based data provider that is a simple hash map. Therefore the project implements a very basic single OData processor supporting a minimal readonly scenario.

This sample can be deployed to any JEE compliant web application server (e.g. [Tomcat](http://tomcat.apache.org)) and be used to get an overview on how to provide an OData V4 compliant webserver.

### Prerequisites

To run this sample you will need to perfom the following steps:

- Get any JEE compliant web application server (e.g. [Tomcat](http://tomcat.apache.org))
- Get the Sample war file (See the next chapter of this tutorial)
- Deploy the war file to your server

You can now explore the service.

### Get the war file via the maven repository

To get the war file via the maven repository you can simple look [here](https://repository.apache.org/index.html#nexus-search;gav%7Eorg.apache.olingo%7Eodata-server-sample%7E%7E%7E) and download the war file.

### Build the war file on your own

You can also build the war file yourself using maven. To do this please set up your project by cloning our git repository. You can find a how to [here](#olingo-apache-org-doc-odata4-maven)

After you have executed the maven goals "clean install" you can find the war file in the target folder under "olingo-odata4/samples/server/target".

If you would like to debug the service you can do so by integrating the whole project into eclipse by following this [tutorial](#olingo-apache-org-doc-odata4-eclipse).

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# Prerequisites for Apache Olingo Tutorials

## Build Environment

We assume you are familiar with [Maven](https://maven.apache.org/) and [Git](https://git-scm.com/)
and have installed at least the following versions:

- **Maven** Version 3.2.0
- **Git** Version 3.5.1
- **JDK** Version 1.6

The tutorials are tested and work with this versions.

## Tutorial sources

To get the source code of the tutorials, please perform the follwing steps:

- Clone the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4)

  *> git clone [https://gitbox.apache.org/repos/asf/olingo-odata4](https://gitbox.apache.org/repos/asf/olingo-odata4)*
- Checkout the tag 4.6.0

  *> git checkout tags/4.6.0*
- Navigate to the tutorial project  
  You can find each tutorial in a separate subdirectory. e.g. [p1\_read](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)

  *> cd samples/tutorials*
- Build the Project and Eclipse Project
  **Note:** The tutorials projects won`t be created if you build the Olingo library project. To create them, you have to navigate to the sub directory mentioned above.

  *> mvn clean install*

## Eclipse support (optional)

- Create Eclipse projects

```bash
*\> mvn eclipse:clean eclipse:eclipse*
```

- Import projects in Eclipse  
  Open Eclipse and navigate to File -> Import

![ImportEclipse1](olingo.apache.org/doc/odata4/tutorials/prerequisites/eclipseImport1.png "Navigate to File -> Import")

Select General -> Existing Projects into Workspace  
![ImportEclipse2](olingo.apache.org/doc/odata4/tutorials/prerequisites/eclipseImport2.png "Select General -> Existing Projects into Workspace ")

Select the directory of the tutorials and click Finish  
![ImportEclipse3](olingo.apache.org/doc/odata4/tutorials/prerequisites/eclipseImport3.png "Select the directory of the tutorial project and click Finish")

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-read-tutorial_read"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

## Part 1: Read scenario

This tutorial guides you through the steps required to write an OData Service based on the Olingo OData 4.0 Library for Java (based on current olingo version which can be got via the [Download-Page](#olingo-apache-org-doc-odata4-download)).

The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).
This tutorial can be found in subdirectory *\samples\tutorials\p1\_read*

We will create a Web Application and deploy it on a local Tomcat server.
Afterwards, the OData service can be invoked from a browser and it will provide the data according to the OData V4 specification.
This tutorial is kept as simple as possible, in order to fully concentrate on the implementation of the service.
For example, only READ scenario is covered in this tutorial, whereas creation, modification and deletion will be covered in the subsequent tutorial.

**Scenario**

The OData service that we are create will implement the following model:

![datamodel](olingo.apache.org/doc/odata4/tutorials/read/model1.png "The OData model")

The service will display a list of products and a few properties that describe each product.
This data model will be enhanced in the subsequent tutorials in order to display categories and to support navigation from a product to its category.

**Goal**

We will be dealing with 3 java classes and the web.xml descriptor file.
Furthermore, for building with Maven, we will edit the `pom.xml` file.

This is how our working directory in Eclipse will look:

![projectLayout](olingo.apache.org/doc/odata4/tutorials/read/EclipseProjectTree.png "The project layout")

At the end of this tutorial, you will have written an OData service and you will be able to invoke the following URL in a browser:

```text
http://localhost:8080/DemoService/DemoService.svc/Products
```

And the browser will display the following collection of data:

```json
    {
      "@odata.context": "$metadata#Products",
      "value": [
        {
          "ID": 1,
          "Name": "Notebook Basic 15",
          "Description": "Notebook Basic, 1.7GHz - 15 XGA - 1024MB DDR2 SDRAM - 40GB"
        },
        {
          "ID": 2,
          "Name": "1UMTS PDA",
          "Description": "Ultrafast 3G UMTS/HSDPA Pocket PC, supports GSM network"
        },
        {
          "ID": 3,
          "Name": "Ergo Screen",
          "Description": "17 Optimum Resolution 1024 x 768 @ 85Hz, resolution 1280 x 960"
        }
      ]
    }
```

**Table of Contents**

1. Prerequisites
2. Preparation
3. Create Project
   - Create Project
   - Edit `pom.xml` file
   - Check build path
   - Build the project
4. Implementation - Read scenario to request the EntitySet “Products”
   1. Declare the metadata
   2. Provide the data
   3. Web application implementation
5. Run the service
   - Run with Eclipse
   - The Service URLs
6. Summary

---

# 1. Prerequisites

In order to follow this tutorial, you should have

- Basic knowledge about OData and OData V4
- Knowledge of the Java programming language
- Optional: knowledge about developing web applications
- Optional: knowledge about building with Maven

---

# 2. Preparation

Before starting off with the creation of our OData service, we need to prepare the following:

1. Installed JDK 1.6 (or higher version)
2. An IDE for writing the Java code
3. A builder to build the .war file, which will be deployed on server
4. A web server to run our web application / OData service

I recommend using Eclipse for all 3 needs, as it is the easiest approach.
This means, you should install the pre-packaged Eclipse distribution called “Eclipse IDE for Java EE developers” which can be found here: [http://www.eclipse.org/downloads/](http://www.eclipse.org/downloads/)

![eclipseDownload](olingo.apache.org/doc/odata4/tutorials/read/eclipseDownload.png "The Eclipse EE download")

This Eclipse package contains an embedded server and also an integrated Maven builder.

---

# 3. Create Project

The recommended procedure to create a project is to use Maven, because it offers an archetype for generating the project skeleton.
Furthermore, using Maven is convenient for managing the build dependencies.
The description within this section is based on an Eclipse installation that contains the Maven integration.

**Create Project using the maven archetype “webapp”**

Within Eclipse, open the Maven Project wizard via
*File -> New -> Other -> Maven -> Maven Project*

On the second wizard page, choose the archetype: maven-archetype-webapp

![mavenArchetype](olingo.apache.org/doc/odata4/tutorials/read/mavenArchetype.png "The Maven Archetype")

On the next page, enter the following information:

- Groupd Id: *my.group.id*
- Artifact Id: *DemoService*
- Version: *4.6.0*
- Package: *myservice.mynamespace.service*

> Note:
> If you’re using this wizard for the first time, it might take some time, as maven needs to download the archetype itself to your local maven-repo.

After finishing the wizard, the next step is to edit the *pom.xml* file.

**Edit pom file**

In our project, we will be using several libraries, e.g. the Olingo libraries.
In the pom.xml file, we specify the dependencies and Maven will download them to our local maven repository.
Furthermore, the *pom.xml* file tells Maven which output we want to have as result of our build. In our case, this is a war file.

In our example, the pom.xml file looks as follows:

```xml
    <project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/maven-v4_0_0.xsd">
      <modelVersion>4.6.0</modelVersion>
      <groupId>my.group.id</groupId>
      <artifactId>DemoService</artifactId>
      <packaging>war</packaging>
      <version>4.6.0</version>

      <name>DemoService Maven Webapp</name>

      <properties>
        <javax.version>2.5</javax.version>
        <odata.version>4.6.0</odata.version>
        <slf4j.version>1.7.7</slf4j.version>
      </properties>

      <dependencies>
        <dependency>
          <groupId>javax.servlet</groupId>
          <artifactId>servlet-api</artifactId>
          <version>${javax.version}</version>
          <scope>provided</scope>
        </dependency>

        <dependency>
          <groupId>org.apache.olingo</groupId>
          <artifactId>odata-server-api</artifactId>
          <version>${odata.version}</version>
        </dependency>
        <dependency>
          <groupId>org.apache.olingo</groupId>
          <artifactId>odata-server-core</artifactId>
          <version>${odata.version}</version>
          <scope>runtime</scope>
        </dependency>

        <dependency>
          <groupId>org.apache.olingo</groupId>
          <artifactId>odata-commons-api</artifactId>
          <version>${odata.version}</version>
        </dependency>
        <dependency>
          <groupId>org.apache.olingo</groupId>
          <artifactId>odata-commons-core</artifactId>
          <version>${odata.version}</version>
        </dependency>

        <dependency>
          <groupId>org.slf4j</groupId>
          <artifactId>slf4j-simple</artifactId>
          <version>${slf4j.version}</version>
          <scope>runtime</scope>
        </dependency>
        <dependency>
	  <groupId>org.slf4j</groupId>
	  <artifactId>slf4j-api</artifactId>
	  <version>1.7.11</version>
	  <scope>compile</scope>
	</dependency>
      </dependencies>
    </project>
```

**Check Java build path**

In order to check the Build path settings, open the context menu on the project and choose
*Build Path -> Configure Build Path…*

![ConfigureBuildPathAction](olingo.apache.org/doc/odata4/tutorials/read/ConfigureBuildPathAction.png "Open the Configure BuildPath action")

Select the *Source* tab.
You might see that the source folder *src/main/java* is configured, but displays an error marker.

![ConfigureBuildPathErr](olingo.apache.org/doc/odata4/tutorials/read/ConfigureBuildPathErr.png "Configure BuildPath has errors")

The reason is that it is missing on file system.
So the solution is to create the required folder in Eclipse.

![createFolder](olingo.apache.org/doc/odata4/tutorials/read/createFolder.png "Create new java folder")

Afterwards, open the Build Path dialog again.
The second error might be about missing test source folder.
Since we don’t need it for our tutorial, we remove it from the build path.

![ConfigureBuildPathOK](olingo.apache.org/doc/odata4/tutorials/read/ConfigureBuildPathOK.png "Configure Build Path is OK now")

**Build the project**

Although the project doesn’t contain any source files yet, let’s perform our first Maven build, in order to check for any problems.

From the context menu on the project node, chose *Run As -> maven build*
If you have never executed the build before, Maven asks you to specify at least one goal.
Enter the usual goals “clean install” and press “Run”

![mavenBuild](olingo.apache.org/doc/odata4/tutorials/read/mavenBuild.png "The Maven build dialog")

The log output is provided in the Eclipse Console view.
You should check it for the output “Build Success”

> Note:
> If maven provides an error marker right from the beginning,it would help to update your Project:
> From context menu on project node, choose Maven -> update Project ->

---

# 4. Implementation

The implementation of an OData service based on Olingo server library can be grouped in the following steps:

- Declaring the metadata of the service
- Handle service requests

Since our example service has to run on a web server, we have to create some code which calls our service in a web application:

- Web application implementation

The following section will guide you through every step in detail.

## 4.1. Declare the metadata

### 4.1.1. Background

According to the OData specification, an OData service has to declare its structure in the so-called *Metadata Document*.
This document defines the contract, such that the user of the service knows which requests can be executed, the structure of the result and how the service can be navigated.

The Metadata Document can be invoked via the following URI:

```text
:::html
<serviceroot>/$metadata
```

Furthermore, OData specifies the usage of the so-called Service Document
Here, the user can see which Entity Collections are offered by an OData service.

The service document can be invoked via the following URI:

```text
<serviceroot>/
```

The information that is given by these 2 URIs, has to be implemented in the service code.
Olingo provides an API for it and we will use it in the implementation of our *CsdlEdmProvider*.

### 4.1.2. Create class

Create package *myservice.mynamespace.service*
Create class *DemoEdmProvider* and specify the superclass *org.apache.olingo.commons.api.edm.provider.CsdlAbstractEdmProvider*

Note: **edm** is the abbreviation for **Entity Data Model**.
Accordingly, we understand that the *CsdlEdmProvider* is supposed to provide static descriptive information.

The Entity Model of the service can be defined in the EDM Provider. The EDM model basically defines the available EntityTypes and the relation between the entities. An EntityType consists of primitive, complex or navigation properties. The model can be invoked with the Metadata Document request.

As we can see, the Olingo server API provides one package that contains interfaces for the description of the metadata:

![edmPackage](olingo.apache.org/doc/odata4/tutorials/read/edmPackage.png "The edm package")

Some of these interfaces are going to be used in the following sections.
**Note:** Take a look into the [Javadoc](/javadoc/odata4/index.html)

### 4.1.3. Implement the required methods

The base class *CsdlAbstractEdmProvider* provides methods for declaring the metadata of all OData elements.

For example:

- The entries that are displayed in the Service Document are provided by the method
  *getEntityContainerInfo()*
- The structure of EntityTypes is declared in the method *getEntityType()*

In our simple example, we implement the minimum amount of methods, required to run a meaningful OData service.
These are:

- ***getEntityType()***
  Here we declare the EntityType “Product” and a few of its properties
- ***getEntitySet()***
  Here we state that the list of products can be called via the EntitySet “Products”
- ***getEntityContainer()***
  Here we provide a Container element that is necessary to host the EntitySet.
- ***getSchemas()***
  The Schema is the root element to carry the elements.
- ***getEntityContainerInfo()***
  Information about the EntityContainer to be displayed in the Service Document

In Eclipse, in order to select the methods to override, right click into the Java editor and from the context menu choose *Source -> Override/Implement Methods…*
Select the mentioned methods and press OK.

![overrideMethods](olingo.apache.org/doc/odata4/tutorials/read/overrideMethods.png "The dialog for overriding superclass methods in Eclipse")

Let’s have a closer look at our methods in detail.

First, we need to declare some constants, to be used in the code below:

```java
    // Service Namespace
    public static final String NAMESPACE = "OData.Demo";

    // EDM Container
    public static final String CONTAINER_NAME = "Container";
    public static final FullQualifiedName CONTAINER = new FullQualifiedName(NAMESPACE, CONTAINER_NAME);

    // Entity Types Names
    public static final String ET_PRODUCT_NAME = "Product";
    public static final FullQualifiedName ET_PRODUCT_FQN = new FullQualifiedName(NAMESPACE, ET_PRODUCT_NAME);

    // Entity Set Names
    public static final String ES_PRODUCTS_NAME = "Products";
```

***getEntityType()***

In our example service, we want to provide a list of products to users who call the OData service.
The user of our service, for example an app-developer, may ask: What does such a "product" entry look like? How is it structured? Which information about a product is provided? For example, the name of it and which data types can be expected from these properties?
Such information is provided by a `CsdlEdmProvider` (and for convenience we extend the `CsdlAbstractEdmProvider`).

In our example service, for modelling the `CsdlEntityType`, we have to provide the following metadata:

The name of the EntityType: “Product”
The properties: name and type and additional info, e.g. “ID” of type “edm.int32”
Which of the properties is the “key” property: a reference to the “ID” property.

```java
    public CsdlEntityType getEntityType(FullQualifiedName entityTypeName) {

      // this method is called for one of the EntityTypes that are configured in the Schema
      if(entityTypeName.equals(ET_PRODUCT_FQN)){

        //create EntityType properties
        CsdlProperty id = new CsdlProperty().setName("ID").setType(EdmPrimitiveTypeKind.Int32.getFullQualifiedName());
        CsdlProperty name = new CsdlProperty().setName("Name").setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());
        CsdlProperty  description = new CsdlProperty().setName("Description").setType(EdmPrimitiveTypeKind.String.getFullQualifiedName());

        // create CsdlPropertyRef for Key element
        CsdlPropertyRef propertyRef = new CsdlPropertyRef();
        propertyRef.setName("ID");

        // configure EntityType
        CsdlEntityType entityType = new CsdlEntityType();
        entityType.setName(ET_PRODUCT_NAME);
        entityType.setProperties(Arrays.asList(id, name , description));
        entityType.setKey(Collections.singletonList(propertyRef));

        return entityType;
      }

      return null;
    }
```

***getEntitySet()***

The procedure for declaring the *Entity Sets* is similar.
An *EntitySet* is a crucial resource, when an OData service is used to request data.
In our example, we will invoke the following URL, which we expect to provide us a list of products:

```text
http://localhost:8080/DemoService/DemoServlet.svc/Products
```

When declaring an `EntitySet`, we need to define the type of entries which are contained in the list, such as an `CsdlEntityType`.
In our example, we set our previously created `CsdlEntityType`, which is referred by a *FullQualifiedName*.

```java
    public CsdlEntitySet getEntitySet(FullQualifiedName entityContainer, String entitySetName) {

      if(entityContainer.equals(CONTAINER)){
        if(entitySetName.equals(ES_PRODUCTS_NAME)){
          CsdlEntitySet entitySet = new CsdlEntitySet();
          entitySet.setName(ES_PRODUCTS_NAME);
          entitySet.setType(ET_PRODUCT_FQN);

          return entitySet;
        }
      }

      return null;
    }
```

***getEntityContainer()***

In order to provide data, our OData service needs an *EntityContainer* that carries the *EntitySets*.
In our example, we have only one *EntitySet*, so we create one *EntityContainer* and set our *EntitySet*.

```java
    public CsdlEntityContainer getEntityContainer() {

      // create EntitySets
      List<CsdlEntitySet> entitySets = new ArrayList<CsdlEntitySet>();
      entitySets.add(getEntitySet(CONTAINER, ES_PRODUCTS_NAME));

      // create EntityContainer
      CsdlEntityContainer entityContainer = new CsdlEntityContainer();
      entityContainer.setName(CONTAINER_NAME);
      entityContainer.setEntitySets(entitySets);

      return entityContainer;
    }
```

***getSchemas()***

Up to this point, we have declared the type of our data (`CsdlEntityType`) and our list (`CsdlEntitySet`), and we have put it into a container (`CsdlEntityContainer`).
Now we are required to put all these elements into a `CsdlSchema`.
While the model of an OData service can have several schemas, in most cases there will probably be only one schema.
So, in our example, we create a list of schemas, where we add one new `CsdlSchema` object.
The schema is configured with a *Namespace*, which serves to uniquely identify all elements.
Then our elements are added to the Schema.

```java
    public List<CsdlSchema> getSchemas() {

      // create Schema
      CsdlSchema schema = new CsdlSchema();
      schema.setNamespace(NAMESPACE);

      // add EntityTypes
      List<CsdlEntityType> entityTypes = new ArrayList<CsdlEntityType>();
      entityTypes.add(getEntityType(ET_PRODUCT_FQN));
      schema.setEntityTypes(entityTypes);

      // add EntityContainer
      schema.setEntityContainer(getEntityContainer());

      // finally
      List<CsdlSchema> schemas = new ArrayList<CsdlSchema>();
      schemas.add(schema);

      return schemas;
    }
```

***getEntityContainerInfo()***

```java
    public CsdlEntityContainerInfo getEntityContainerInfo(FullQualifiedName entityContainerName) {

        // This method is invoked when displaying the Service Document at e.g. http://localhost:8080/DemoService/DemoService.svc
        if (entityContainerName == null || entityContainerName.equals(CONTAINER)) {
            CsdlEntityContainerInfo entityContainerInfo = new CsdlEntityContainerInfo();
            entityContainerInfo.setContainerName(CONTAINER);
            return entityContainerInfo;
        }

        return null;
    }
```

**Summary:**
We have created a class that declares the metadata of our OData service.
We have declared the main elements of an OData service: *EntityType*, *EntitySet*, *EntityContainer* and *Schema* (with the corresponding Olingo classes `CsdlEntityType`, `CsdlEntitySet`, `CsdlEntityContainer` and `CsdlSchema`).

At runtime of an OData service, such metadata can be viewed by invoking the Metadata Document.

In our example invokation of the URL: [http://localhost:8080/DemoService/DemoService.svc/$metadata](http://localhost:8080/DemoService/DemoService.svc/$metadata)

Give us the result below:

```xml
    <?xml version='1.0' encoding='UTF-8'?>
    <edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
      <edmx:DataServices>
        <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="OData.Demo">
          <EntityType Name="Product">
            <Key>
              <PropertyRef Name="ID"/>
            </Key>
            <Property Name="ID" Type="Edm.Int32"/>
            <Property Name="Name" Type="Edm.String"/>
            <Property Name="Description" Type="Edm.String"/>
          </EntityType>
          <EntityContainer Name="Container">
            <EntitySet Name="Products" EntityType="OData.Demo.Product"/>
          </EntityContainer>
        </Schema>
      </edmx:DataServices>
    </edmx:Edmx>
```

The Service Document can be invoked to view the Entity Sets, like in our example at the URL: [http://localhost:8080/DemoService/DemoService.svc/](http://localhost:8080/DemoService/DemoService.svc/)

Which give us the Service Document as result:

```json
    {
      "@odata.context" : "$metadata",
      "value" : [
      {
        "name" : "Products",
        "url" : "Products"
      } ]
    }
```

> Note:
> After implementing the *EdmProvider*, we can, as an intermediate step, build/deploy the service and invoke the 2 static pages: Service Document and Metadata Document.
> If desired, you can proceed with implementing the required steps for web application as described in
> 4.3. and run the application as described in chapter 5.

## 4.2. Provide the data

After implementing the *EdmProvider*, the next step is the main task of an OData service: provide data.
In our example, we imagine that our OData service is invoked by a user who wants to see which products are offered by a web shop.
He invokes a URL and gets a list of products.
Providing the list of products is the task that we are going to implement in this chapter.

The work that we have to do in this chapter can be divided into 4 tasks:

1. Check the URI
   We need to identify the requested resource and have to consider Query Options (if available)
2. Provide the data
   Based on the URI info, we have to obtain the data from our data store (can be e.g. a database)
3. Serialize the data
   The data has to be transformed into the required format
4. Configure the response
   Since we are implementing a “processor”, the last step is to provide the response object

These 4 steps will be considered in the implementation of the `readEntityCollection()` method.

### 4.2.1. Background

In terms of *Olingo*, while processing a service request, a Processor instance is invoked that is supposed to understand the (user HTTP-) request and deliver the desired data.
*Olingo* provides API for processing different kind of service requests:
Such a service request can ask for a list of entities, or for one entity, or one property.

Example:
In our example, we have stated in our Metadata Document that we will provide a list of “products” whenever the *EntitySet* with name “Products” is invoked.
This means that the user of our OData service will append the *EntitySet* name to the root URL of the service and then invoke the full URL.
This is [http://localhost:8080/DemoService/DemoServlet1.svc/Products](http://localhost:8080/DemoService/DemoServlet1.svc/Products)
So, whenever this URL is fired, Olingo will invoke the `EntityCollectionProcessor` implementation of our OData service.
Then our `EntityCollectionProcessor` implementation is expected to provide a list of products.

As we have already mentioned, the Metadata Document is the contract for providing data.
This means that when it comes to provide the actual data, we have to do it according to the specified metadata.
For example, the property names have to match, also the types of the properties, and, if specified, the length of the strings, etc

### 4.2.2. Create class

Within our package `myservice.mynamespace.service`, we create a Java class `DemoEntityCollectionProcessor` that implements the interface `org.apache.olingo.server.api.processor.EntityCollectionProcessor`.

![createJavaClass](olingo.apache.org/doc/odata4/tutorials/read/createJavaClass.png "The Eclipse dialog for creating a Java class")

### 4.2.3. Implement the required methods

After creation of the Java class, we can see that there are 2 methods to be implemented:

- *init()*
  This method is invoked by the *Olingo* library, allowing us to store the context object
- *readEntityCollection()*
  Here we have to fetch the required data and pass it back to the *Olingo* library

Let’s have a closer look

***init()***

This method is common to all processor interfaces.
The *Olingo* framework initializes the processor with an instance of the *OData* object.
According to the Javadoc, this object is the “Root object for serving factory tasks…”
We will need it later, so we store it as member variable.

```java
    public void init(OData odata, ServiceMetadata serviceMetadata) {
      this.odata = odata;
      this.serviceMetadata = serviceMetadata;
    }
```

Don’t forget to declare the member variables

```java
    private OData odata;
    private ServiceMetadata serviceMetadata;
```

***readEntityCollection()***

The `EntityCollectionProcessor` exposes only one method: `readEntityCollection(...)`

Here we have to understand that this `readEntityCollection(...)`-method is invoked, when the OData service is called with an HTTP GET operation for an entity collection.

The `readEntityCollection(...)` method is used to “read” the data in the backend (this can be e.g. a database) and to deliver it to the user who calls the OData service.

The method signature:

The “request” parameter contains raw HTTP information. It is typically used for creation scenario, where a request body is sent along with the request.

With the second parameter, the “response” object is passed to our method in order to carry the response data. So here we have to set the response body, along with status code and content-type header.

The third parameter, the “uriInfo”, contains information about the relevant part of the URL. This means, the segments starting after the service name.

**Example:**
If the user calls the following URL:
`http://localhost:8080/DemoService/DemoService.svc/Products`
The `readEntityCollection(...)` method is invoked and the *uriInfo* object contains one segment: “Products”

If the user calls the following URL:
`http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID eq 1`
Then the `readEntityCollection(...)` method is invoked and the *uriInfo* contains the information about the entity set and furthermore the system query option $filter and its value.

The last parameter, the “responseFormat”, contains information about the content type that is requested by the user.
This means that the user has the choice to receive the data either in XML or in JSON.

**Example:**
If the user calls the following URL:
`http://localhost:8080/DemoService/DemoService.svc/Products?$format=application/json;odata.metadata=minimal`

then the content type is:
`application/json;odata.metadata=minimal`
which means that the payload is formatted in JSON (like it is shown in the introduction section of this tutorial)

**Note:**
The content type can as well be specified via the following request header
Accept: application/json;odata.metadata=minimal
In this case as well, our readEntityCollection() method will be called with the parameter responseFormat containing the content type > information.

**Note:**
If the user doesn’t specify any content type, then the default is JSON.

Why is this parameter needed?
Because the `readEntityCollection(...)` method is supposed to deliver the data in the format that is requested by the user. We will use this parameter when creating a serializer based on it.

The steps for implementating the method `readEntityCollection(...)` are:

1. Which data is requested?
   Usually, an OData service provides different *EntitySets*, so first it is required to identify which *EntitySet* has been requested. This information can be retrieved from the *uriInfo* object.
2. Fetch the data
   As a developer of the OData service, you have to know how and where the data is stored. In many cases, this would be a database. At this point, you would connect to your database and fetch the requested data with an appropriate SQL statement. The data that is fetched from the data storage has to be put into an *EntityCollection* object.
   The package `org.apache.olingo.commons.api.data` provides interfaces that describe the actual data, not the metadata.

   ![datapackage](olingo.apache.org/doc/odata4/tutorials/read/datapackage.png "The package containing the interfaces for handling runtime data")
3. Transform the data
   *Olingo* expects us to provide the data as low-level *InputStream* object. However, *Olingo* supports us in doing so, by providing us with a proper "serializer".
   So what we have to do is create the serializer based on the requested content type, configure it and call it.
4. Configure the response
   The response object has been passed to us in the method signature. We use it to set the serialized data (the `InputStream` object).
   Furthermore, we have to set the HTTP status code, which means that we have the opportunity to do proper error handling.
   And finally we have to set the content type.

**Sample:**

```java
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
        throws ODataApplicationException, SerializerException {

      // 1st we have retrieve the requested EntitySet from the uriInfo object (representation of the parsed service URI)
      List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
      UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0); // in our example, the first segment is the EntitySet
      EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

      // 2nd: fetch the data from backend for this requested EntitySetName
      // it has to be delivered as EntitySet object
      EntityCollection entitySet = getData(edmEntitySet);

      // 3rd: create a serializer based on the requested format (json)
      ODataSerializer serializer = odata.createSerializer(responseFormat);

      // 4th: Now serialize the content: transform from the EntitySet object to InputStream
      EdmEntityType edmEntityType = edmEntitySet.getEntityType();
      ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).build();

      final String id = request.getRawBaseUri() + "/" + edmEntitySet.getName();
      EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().id(id).contextURL(contextUrl).build();
      SerializerResult serializerResult = serializer.entityCollection(serviceMetadata, edmEntityType, entitySet, opts);
      InputStream serializedContent = serializerResult.getContent();

      // Finally: configure the response object: set the body, headers and status code
      response.setContent(serializedContent);
      response.setStatusCode(HttpStatusCode.OK.getStatusCode());
      response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

***getData()***

We have not elaborated on fetching the actual data.
In our tutorial, to keep the code as simple as possible, we use a little helper method that delivers some hardcoded entries.
Since we are supposed to deliver the data inside an `EntityCollection` instance, we create the instance, ask it for the (initially empty) list of entities and add some new entities to it.
We create the entities and their properties according to what we declared in our `DemoEdmProvider` class. So we have to take care to provide the correct names to the new property objects.
If a client requests the response in [ATOM format](http://docs.oasis-open.org/odata/odata-atom-format/v4.0/odata-atom-format-v4.0.html), each entity have to provide it`s own entity id.
The method *createId* allows us to create an id in a convenient way.

```java
    private EntityCollection getData(EdmEntitySet edmEntitySet){

       EntityCollection productsCollection = new EntityCollection();
       // check for which EdmEntitySet the data is requested
       if(DemoEdmProvider.ES_PRODUCTS_NAME.equals(edmEntitySet.getName())) {
           List<Entity> productList = productsCollection.getEntities();

           // add some sample product entities
           final Entity e1 = new Entity()
              .addProperty(new Property(null, "ID", ValueType.PRIMITIVE, 1))
              .addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "Notebook Basic 15"))
              .addProperty(new Property(null, "Description", ValueType.PRIMITIVE,
                  "Notebook Basic, 1.7GHz - 15 XGA - 1024MB DDR2 SDRAM - 40GB"));
          e1.setId(createId("Products", 1));
          productList.add(e1);

          final Entity e2 = new Entity()
              .addProperty(new Property(null, "ID", ValueType.PRIMITIVE, 2))
              .addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "1UMTS PDA"))
              .addProperty(new Property(null, "Description", ValueType.PRIMITIVE,
                  "Ultrafast 3G UMTS/HSDPA Pocket PC, supports GSM network"));
          e2.setId(createId("Products", 1));
          productList.add(e2);

          final Entity e3 = new Entity()
              .addProperty(new Property(null, "ID", ValueType.PRIMITIVE, 3))
              .addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "Ergo Screen"))
              .addProperty(new Property(null, "Description", ValueType.PRIMITIVE,
                  "19 Optimum Resolution 1024 x 768 @ 85Hz, resolution 1280 x 960"));
          e3.setId(createId("Products", 1));
          productList.add(e3);
       }

       return productsCollection;
    }
```

***createId()***

```java
    private URI createId(String entitySetName, Object id) {
        try {
            return new URI(entitySetName + "(" + String.valueOf(id) + ")");
        } catch (URISyntaxException e) {
            throw new ODataRuntimeException("Unable to create id for entity: " + entitySetName, e);
        }
    }
```

## 4.3. Web Application

After declaring the metadata and providing the data, our OData service implementation is done.
The last step is to enable our OData service to be called on a web server.
Therefore, we are wrapping our service by a web application.

The web application is defined in the web.xml file, where a servlet is registered.
The servlet is a standard *HttpServlet* which dispatches the user requests to the *Olingo* framework.

Let’s quickly do the remaining steps:

### 4.3.1. Create and implement the Servlet

Create a new package *myservice.mynamespace.web*.
Create Java class with name `DemoServlet` that inherits from `HttpServlet`.

![createJavaServletClass](olingo.apache.org/doc/odata4/tutorials/read/createJavaServletClass.png "Creating the servlet class")

Override the `service()` method.
Basically, what we are doing here is to create an `ODataHttpHandler`, which is a class that is provided by *Olingo*.
It receives the user request and if the URL conforms to the OData specification, the request is delegated to the processor implementation of the OData service.
This means that the handler has to be configured with all processor implementations that have been created along with the OData service (in our example, only one processor).
Furthermore, the `ODataHttpHandler` needs to carry the knowledge about the `CsdlEdmProvider`.

This is where our two implemented classes come together, the metadata declaration and the data provisioning.

```java
    public class DemoServlet extends HttpServlet {

      private static final long serialVersionUID = 1L;
      private static final Logger LOG = LoggerFactory.getLogger(DemoServlet.class);

      protected void service(final HttpServletRequest req, final HttpServletResponse resp) throws ServletException, IOException {
        try {
          // create odata handler and configure it with CsdlEdmProvider and Processor
          OData odata = OData.newInstance();
          ServiceMetadata edm = odata.createServiceMetadata(new DemoEdmProvider(), new ArrayList<EdmxReference>());
          ODataHttpHandler handler = odata.createHandler(edm);
          handler.register(new DemoEntityCollectionProcessor());

          // let the handler do the work
          handler.process(req, resp);
        } catch (RuntimeException e) {
          LOG.error("Server Error occurred in ExampleServlet", e);
          throw new ServletException(e);
        }
      }
    }
```

### 4.3.2. Edit the web.xml

The very last step of our tutorial is to register the Servlet in the *web.xml* file.
Furthermore, we need to specify the *url-pattern* for the servlet, such that our OData service can be invoked.

Open the *src/main/webapp/WEB-INF/web.xml* file and paste the following content into it:

```xml
    <web-app xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns="http://java.sun.com/xml/ns/javaee"
        xmlns:web="http://java.sun.com/xml/ns/javaee/web-app_2_5.xsd"
        xsi:schemaLocation="http://java.sun.com/xml/ns/javaee http://java.sun.com/xml/ns/javaee/web-app_2_5.xsd"
        id="WebApp_ID" version="2.5">

    <servlet>
      <servlet-name>DemoServlet</servlet-name>
      <servlet-class> myservice.mynamespace.web.DemoServlet</servlet-class>
      <load-on-startup>1</load-on-startup>
    </servlet>

    <servlet-mapping>
      <servlet-name>DemoServlet</servlet-name>
      <url-pattern>/DemoService.svc/*</url-pattern>
    </servlet-mapping>
    </web-app>
```

That’s it. Now we can build and run the web application.

---

# 5. Run the service

Running the service means build the war file and deploy it on a server.
In our tutorial, we are using the Eclipse web integration tools, which make life easier.

### Run with Eclipse

Select your project and from the context menu choose *Run As -> Run on Server*
If you don’t have any server configured in Eclipse, you have to “manually define a new server” in the subsequent dialog.
If you have installed a Tomcat server on your local file system, you can use it here.
If not, you can use the *Basic -> J2EE Preview* option, which is should be enough for our tutorial.

![runOnServer](olingo.apache.org/doc/odata4/tutorials/read/runOnServer.png "The Eclipse dialog for deploying web apps to a server")

> Note:
> You might have to first execute maven build and also press F5 to refresh the content of the Eclipse project

After pressing "run", Eclipse starts the internal server and deploys the web application on it.
Then the Eclipse internal Browser View is opened and the index.jsp file that has been generated into our Example project is opened.
We ignore it. Instead, we open our OData service in our favorite browser.

> Note:
> If you face problems related to the server, it helps to restart your Eclipse IDE.

### The Service URLs

Try the following URLs:

**Service Document**

```text
http://localhost:8080/DemoService/DemoService.svc/
```

The expected result is the Service Document which displays our *EntityContainerInfo*:

```json
    {
      "@odata.context" : "$metadata",
      "value" : [
      {
        "name" : "Products",
        "url" : "Products"
      } ]
    }
```

**Metadata Document**

```text
http://localhost:8080/DemoService/DemoService.svc/$metadata
```

The expected result is the Metadata Document that displays our *Schema*, *EntityType*, *EntityContainer* and *EntitySet*.

```xml
    <?xml version='1.0' encoding='UTF-8'?>
    <edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
      <edmx:DataServices>
        <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="OData.Demo">
          <EntityType Name="Product">
            <Key>
              <PropertyRef Name="ID"/>
            </Key>
            <Property Name="ID" Type="Edm.Int32"/>
            <Property Name="Name" Type="Edm.String"/>
            <Property Name="Description" Type="Edm.String"/>
          </EntityType>
          <EntityContainer Name="Container">
            <EntitySet Name="Products" EntityType="OData.Demo.Product"/>
          </EntityContainer>
        </Schema>
      </edmx:DataServices>
    </edmx:Edmx>
```

**Query / EntitySet**

```text
http://localhost:8080/DemoService/DemoService.svc/Products
```

The expected result is the hardcoded list of product entries, which we have coded in our processor implementation:

```json
    {
      "@odata.context":"$metadata#Products","
      value":[
        {
          "ID":1,
          "Name":"Notebook Basic 15",
          "Description":"Notebook Basic, 1.7GHz - 15 XGA - 1024MB DDR2 SDRAM - 40GB"
        },
        {
          "ID":2,
          "Name":"1UMTS PDA",
          "Description":"Ultrafast 3G UMTS/HSDPA Pocket PC, supports GSM network"
        },
        {
          "ID":3,
          "Name":"Ergo Screen",
          "Description":"17 Optimum Resolution 1024 x 768 @ 85Hz, resolution 1280 x 960"
      }]
    }
```

---

# 6. Summary

Finally, we have created our first OData service based on the V4 version of the OData specification and using the V4 server library provided by *Olingo*.
Our first OData service is very simple; it only allows invoking one entity collection, apart from the Service Document and the Metadata Document.

The final source code result can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).

# Links

### Tutorials

Further topics to be covered by follow-up tutorials:

- Tutorial OData V4 service part 1: Read Entity Collection
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial ODATA V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial ODATA V4 service, part 7: [Media Entities](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.6.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-read-tutorial_read_mvn"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read_mvn--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read_mvn--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read_mvn--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# Appendix for Maven users

Prerequisites for this Appendix is an installed Maven (Version 3.x) and access to the [Maven Central Repository](www.maven.org)

### Initial project setup

To create a Maven project without Eclipse, execute the following command on command line:

```bash
mvn archetype:generate \
  -DgroupId=org.apache.olingo \
  -DartifactId=DemoService \
  -DarchetypeArtifactId=maven-archetype-webapp \
  -DinteractiveMode=false
```

This creates a project skeleton.
Afterwards, use the goal `eclipse:eclipse` in order to make the project Eclipse-like.
This allows to import the project into Eclipse using the default Eclipse-importer:
`File->Import->General->Existing projects into Workspace`

### Implementation of the Demo Service

To implement the Demo Service see section [4. Implementation](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read--4-implementation) in the tutorial.

### Append Jetty to pom

To add support for run the Demo Service via Maven and the Jetty plugin add following part into the `pom.xml`:

```xml
<build>
  <finalName>${project.artifactId}</finalName>
  <defaultGoal>package jetty:run</defaultGoal>
  <resources>
    <resource>
      <directory>src/main/version</directory>
      <filtering>true</filtering>
      <targetPath>../${project.build.finalName}/gen</targetPath>
    </resource>
    <resource>
      <directory>src/main/resources</directory>
      <filtering>true</filtering>
    </resource>
    <resource>
      <directory>target/maven-shared-archive-resources</directory>
    </resource>
  </resources>

  <plugins>
    <plugin>
      <groupId>org.mortbay.jetty</groupId>
      <artifactId>jetty-maven-plugin</artifactId>
      <version>8.1.14.v20131031</version>
    </plugin>
  </plugins>
</build>
```

Afterwards it is possible to call `mvn` (or `mvn jetty:run`) on the console to build the project and start the Jetty server on [http://localhost:8080](http://localhost:8080)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

## Part 2: Read scenario continued

### Introduction

In the first tutorial, we’ve learned how to build a very simple OData service.
That service exposes its metadata and allows reading a list of products.
It doesn’t support accessing the URL for a single product.
And it also doesn’t support reading a single property of a product.
These 2 topics will be covered in the present tutorial.

Note that this tutorial doesn’t cover modifying operations like create, update and delete.
Such operations will be the focus of the Olingo V4 tutorial no. 3

**Note**  
The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).  
This tutorial can be found in subdirectory *\samples\tutorials\p2\_readep*

**Disclaimer**  
Again, in the present tutorial, We will focus only on the relevant implementation, in order to keep the code small and simple.
The sample code shouldn't be reused for advanced scenarios.

**Table of Contents**

1. Prerequisites
2. Preparation
   1. Create data class
   2. Adapt the servlet class
   3. Modify the DemoEntityCollectionProcessor
   4. Create utility class
3. Implementation of Read Single Entity
   1. Implement the EntityProcessor interface
   2. Adapt the DemoServlet class
   3. Run the service
4. Implementation of Read Single Property
   1. Implement the PrimitiveProcessor interface
   2. Adapt the DemoServlet class
   3. Run the service
5. Summary
6. Links
7. Appendix: PrimitiveValueProcessor

# 1. Prerequisites

Follow [Tutorial Part 1: Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read), which also covers setting up the environment and running the service.

# 2. Preparation

Follow the [Tutorial Part 1: Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read) or import the the tutorial form subdirectory [*\samples\tutorials\p1\_read*](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites) into your Eclipse workspace.  
Afterwards do a *Deploy and run*: it should be working.  
Before we start with the actual tutorial, we need to adapt the existing code to meet the requirements of the present tutorial.

## 2.1. Create a new data-class

In the first tutorial, we had created a single method called `getData()` to provide sample data for the list of products.
Handling the sample data will get a bit more advanced from now on, so we move the code into a separate class.

Create a new package *myservice.mynamespace.data*  
Within this package, create a new class *Storage.java* to simulate the data layer (in a real scenario, this would be e.g. a database or any other data storage)

Here’s the full implementation of this class:

```java
    package myservice.mynamespace.data;

    import java.util.ArrayList;
    import java.util.List;
    import java.util.Locale;

    import myservice.mynamespace.service.DemoEdmProvider;
    import myservice.mynamespace.util.Util;

    import org.apache.olingo.commons.api.data.Entity;
    import org.apache.olingo.commons.api.data.EntityCollection;
    import org.apache.olingo.commons.api.data.Property;
    import org.apache.olingo.commons.api.data.ValueType;
    import org.apache.olingo.commons.api.edm.EdmEntitySet;
    import org.apache.olingo.commons.api.edm.EdmEntityType;
    import org.apache.olingo.commons.api.http.HttpStatusCode;
    import org.apache.olingo.server.api.ODataApplicationException;
    import org.apache.olingo.server.api.uri.UriParameter;

    public class Storage {

        private List<Entity> productList;

        public Storage() {
            productList = new ArrayList<Entity>();
            initSampleData();
        }

        /* PUBLIC FACADE */

        public EntityCollection readEntitySetData(EdmEntitySet edmEntitySet)throws ODataApplicationException{

            // actually, this is only required if we have more than one Entity Sets
            if(edmEntitySet.getName().equals(DemoEdmProvider.ES_PRODUCTS_NAME)){
                return getProducts();
            }

            return null;
        }

        public Entity readEntityData(EdmEntitySet edmEntitySet, List<UriParameter> keyParams) throws ODataApplicationException{

            EdmEntityType edmEntityType = edmEntitySet.getEntityType();

            // actually, this is only required if we have more than one Entity Type
            if(edmEntityType.getName().equals(DemoEdmProvider.ET_PRODUCT_NAME)){
                return getProduct(edmEntityType, keyParams);
            }

            return null;
        }

        /*  INTERNAL */

        private EntityCollection getProducts(){
            EntityCollection retEntitySet = new EntityCollection();

            for(Entity productEntity : this.productList){
                retEntitySet.getEntities().add(productEntity);
            }

            return retEntitySet;
        }

        private Entity getProduct(EdmEntityType edmEntityType, List<UriParameter> keyParams) throws ODataApplicationException{

            // the list of entities at runtime
            EntityCollection entitySet = getProducts();

            /*  generic approach  to find the requested entity */
            Entity requestedEntity = Util.findEntity(edmEntityType, entitySet, keyParams);

            if(requestedEntity == null){
                // this variable is null if our data doesn't contain an entity for the requested key
                // Throw suitable exception
                throw new ODataApplicationException("Entity for requested key doesn't exist",
                                           HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
            }

            return requestedEntity;
         }

         /* HELPER */
         private void initSampleData(){

             // add some sample product entities
             final Entity e1 = new Entity()
                                   .addProperty(new Property(null, "ID", ValueType.PRIMITIVE, 1))
                                   .addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "Notebook Basic 15"))
                                   .addProperty(new Property(null, "Description", ValueType.PRIMITIVE,
                                                "Notebook Basic, 1.7GHz - 15 XGA - 1024MB DDR2 SDRAM - 40GB"));
            e1.setId(createId("Products", 1));
            productList.add(e1);

            final Entity e2 = new Entity()
                                  .addProperty(new Property(null, "ID", ValueType.PRIMITIVE, 2))
                                  .addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "1UMTS PDA"))
                                  .addProperty(new Property(null, "Description", ValueType.PRIMITIVE,
                                               "Ultrafast 3G UMTS/HSDPA Pocket PC, supports GSM network"));
            e2.setId(createId("Products", 1));
            productList.add(e2);

            final Entity e3 = new Entity()
                                  .addProperty(new Property(null, "ID", ValueType.PRIMITIVE, 3))
                                  .addProperty(new Property(null, "Name", ValueType.PRIMITIVE, "Ergo Screen"))
                                  .addProperty(new Property(null, "Description", ValueType.PRIMITIVE,
						"19 Optimum Resolution 1024 x 768 @ 85Hz, resolution 1280 x 960"));
            e3.setId(createId("Products", 1));
            productList.add(e3);
        }

        private URI createId(String entitySetName, Object id) {
            try {
                return new URI(entitySetName + "(" + String.valueOf(id) + ")");
            } catch (URISyntaxException e) {
                throw new ODataRuntimeException("Unable to create id for entity: " + entitySetName, e);
            }
        }
    }
```

The *Public Façade* contains the methods that are called from outside.  
They are data-layer-agnostic; their parameters are objects from the OData world.  
The implementation of these methods simply delegates the logic to the internal methods.

The *internal* methods do know about the names of tables or columns and these methods know how to e.g. find a single product.

## 2.2. Adapt the servlet class

The `Data-Storage` class will be instantiated in the `DemoServlet` class and attached to the HTTP-session.  
This has the advantage that our final OData service can be tested and the sample data can be changed and the changes will remain active until the session is closed.

Open the class `myservice.mynamespace.web.DemoServlet`

Change the code such that it looks as follows:

```java
    protected void service(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    	try {
    		HttpSession session = req.getSession(true);
    		Storage storage = (Storage) session.getAttribute(Storage.class.getName());
    		if (storage == null) {
    		   storage = new Storage();
    		   session.setAttribute(Storage.class.getName(), storage);
    		}

    		// create odata handler and configure it with EdmProvider and Processor
    		OData odata = OData.newInstance();
    		ServiceMetadata edm = odata.createServiceMetadata(new DemoEdmProvider(), new ArrayList<EdmxReference>());
    		ODataHttpHandler handler = odata.createHandler(edm);
    		handler.register(new DemoEntityCollectionProcessor(storage));
      } /* more code */
    }
```

Note that we pass the instance of the `Storage` object to the constructor of our existing `DemoEntityCollectionProcessor`.

So the next step is to adapt the `DemoEntityCollectionProcessor` class.

## 2.3. Modify the DemoEntityCollectionProcessor

In the `DemoEntityCollectionProcessor` class that we created in the first tutorial, we have to make 3 changes:

### 2.3.1 Create Constructor

We have to create a Constructor that takes the `Storage` instance and stores it as a member variable:

```java
    public class DemoEntityCollectionProcessor implements EntityCollectionProcessor {

    	private OData odata;
        private ServiceMetadata serviceMetadata;
    	private Storage storage;

    	public DemoEntityCollectionProcessor(Storage storage) {
    		this.storage = storage;
    	}
    }
```

### 2.3.2. Delete the getData() method

The code that we had written in this method has been moved to the `init()` method of the Storage class.
So we can delete this `getData()` method.

### 2.3.3. Adapt the usage of the getData() method

After deleting the `getData()` method, we get a compile error in the line where this method is used.
We replace this method invocation with a call to our `Storage` object:

```java
    EntitySet entitySet = storage.readEntitySetData(edmEntitySet);
```

The new code looks as follows:

```java
    public void readEntityCollection(ODataRequest request, ODataResponse response,
                                    UriInfo uriInfo, ContentType responseFormat)
                                    throws ODataApplicationException, SerializerException {

    	// 1st retrieve the requested EntitySet from the uriInfo (representation of the parsed URI)
    	List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
        // in our example, the first segment is the EntitySet
    	UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
    	EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

    	// 2nd: fetch the data from backend for this requested EntitySetName and deliver as EntitySet
    	EntityCollection entityCollection = storage.readEntitySetData(edmEntitySet);
    }
```

## 2.4. Create utility class

Furthermore, we create one more class, to host a few helper methods.  
Create package `myservice.mynamespace.util`
Within this package, create a class `Util.java`

Copy the following code into this class:

```java
    package myservice.mynamespace.util;

    import java.util.List;
    import java.util.Locale;

    import org.apache.olingo.commons.api.data.Entity;
    import org.apache.olingo.commons.api.data.EntityCollection;
    import org.apache.olingo.commons.api.edm.EdmEntityType;
    import org.apache.olingo.commons.api.edm.EdmPrimitiveType;
    import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeException;
    import org.apache.olingo.commons.api.edm.EdmProperty;
    import org.apache.olingo.commons.api.edm.EdmType;
    import org.apache.olingo.commons.api.http.HttpStatusCode;
    import org.apache.olingo.server.api.ODataApplicationException;
    import org.apache.olingo.server.api.uri.UriParameter;

    public class Util {

    	public static EdmEntitySet getEdmEntitySet(UriInfoResource uriInfo) throws ODataApplicationException {

    		List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
    		 // To get the entity set we have to interpret all URI segments
    		if (!(resourcePaths.get(0) instanceof UriResourceEntitySet)) {
    			throw new ODataApplicationException("Invalid resource type for first segment.",
    			                        HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(),Locale.ENGLISH);
    		}

    		UriResourceEntitySet uriResource = (UriResourceEntitySet) resourcePaths.get(0);

    		return uriResource.getEntitySet();
    	}

    	public static Entity findEntity(EdmEntityType edmEntityType,
    	                                EntityCollection rt_entitySet, List<UriParameter> keyParams)
    	                                throws ODataApplicationException {

    		List<Entity> entityList = rt_entitySet.getEntities();

    		// loop over all entities in order to find that one that matches all keys in request
    		// an example could be e.g. contacts(ContactID=1, CompanyID=1)
    		for(Entity rt_entity : entityList){
    			boolean foundEntity = entityMatchesAllKeys(edmEntityType, rt_entity, keyParams);
    			if(foundEntity){
    				return rt_entity;
    			}
    		}

    		return null;
    	}

    	public static boolean entityMatchesAllKeys(EdmEntityType edmEntityType, Entity rt_entity,  List<UriParameter> keyParams)
    	                                            throws ODataApplicationException {

            // loop over all keys
            for (final UriParameter key : keyParams) {
            	// key
            	String keyName = key.getName();
            	String keyText = key.getText();

            	// Edm: we need this info for the comparison below
            	EdmProperty edmKeyProperty = (EdmProperty )edmEntityType.getProperty(keyName);
    			Boolean isNullable = edmKeyProperty.isNullable();
    			Integer maxLength = edmKeyProperty.getMaxLength();
    			Integer precision = edmKeyProperty.getPrecision();
    			Boolean isUnicode = edmKeyProperty.isUnicode();
    			Integer scale = edmKeyProperty.getScale();
    			// get the EdmType in order to compare
    			EdmType edmType = edmKeyProperty.getType();
    			// Key properties must be instance of primitive type
    			EdmPrimitiveType edmPrimitiveType = (EdmPrimitiveType)edmType;

    			// Runtime data: the value of the current entity
    			Object valueObject = rt_entity.getProperty(keyName).getValue(); // null-check is done in FWK

    			// now need to compare the valueObject with the keyText String
    			// this is done using the type.valueToString //
    			String valueAsString = null;
    			try {
    				valueAsString = edmPrimitiveType.valueToString(valueObject, isNullable, maxLength,
    				                                                precision, scale, isUnicode);
    			} catch (EdmPrimitiveTypeException e) {
    				throw new ODataApplicationException("Failed to retrieve String value",
    				                             HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(),Locale.ENGLISH, e);
    			}

    			if (valueAsString == null){
    				return false;
    			}

    			boolean matches = valueAsString.equals(keyText);
    			if(!matches){
    				// if any of the key properties is not found in the entity, we don't need to search further
    				return false;
    			}
    		}

    		return true;
    	}
    }
```

These helper methods are going to be used within the implementation of the Processor implementations.

---

# 3. Implementation of Read Single Entity

The user of our sample OData service is enabled to invoke the list of products via the following URL:

```text
http://localhost:8080/DemoService/DemoService.svc/Products
```

In a next step, he wants to read the details of a single product entity.
This is done via the following URL:

```text
http://localhost:8080/DemoService/DemoService.svc/Products(3)
```

From the above URL we can see that the user requests the product which has an ID with value 3  
Whenever such a URL is requested, the Olingo library will delegate the request to an implementation of the Interface `EntityProcessor`.

This means, we have to create a new Java class that implements the mentioned interface and we have to register it in the `DemoServlet` class (remember that all Processor-implementations have to be registered there).

## 3.1. Implement the EntityProcessor interface

The interface `org.apache.olingo.server.api.processor.EntityProcessor` has 5 methods to implement:

- *init(...)*  
  Here we are initialized by the Framework to pass the context objects to us
- *readEntity(…)*  
  This method is relevant for reading a single entity
- *createEntity(…)*  
  We will ignore this method in the present tutorial
- *updateEntity(…)*  
  We will ignore this method in the present tutorial
- *deleteEntity(…)*  
  We will ignore this method in the present tutorial

Let’s have a look at the implementation.

Create the class `DemoEntityProcessor` in package `myservice.mynamespace.service` which implements the interface `EntityProcessor`.  
First we need to implement the `init()` method, in order to store the `OData` object.
Second, as described in the preparation-section, we need to create a constructor that takes and stores our `Storage` instance

```java
    public class DemoEntityProcessor implements EntityProcessor {

    	private OData odata;
    	private ServiceMetadata serviceMetadata;
    	private Storage storage;

    	public DemoEntityProcessor(Storage storage) {
    		this.storage = storage;
    	}

    	public void init(OData odata, ServiceMetadata serviceMetadata) {
    		this.odata = odata;
    		this.serviceMetadata = serviceMetadata;
    	}
    }
```

**readEntity(...)**

When going through the implementation, let’s keep in mind that the user invokes e.g. the following URL

```text
http://localhost:8080/DemoService/DemoService.svc/Products(3)
```

and he receives the following response in the browser:

```json
    {
      @odata.context: "$metadata#Products",
      ID: 3,
      Name: "Ergo Screen",
      Description: "19 Optimum Resolution 1024 x 768 @ 85Hz, resolution 1280 x 960"
    }
```

**Steps**  
The steps to be followed in the implementation of the `readEntity(...)` method are the same that we followed in the previous tutorial, when we implemented the `readEntityCollection(...)` method:

1. **Which data is requested?**  
   Check the UriInfo instance for information about which *EntityCollection* has been requested.  
   Note that in the code, we directly access the first segment of the URI and cast it to `UriResourceEntitySet`

   ```java
      UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
   ```

   This is only possible, because in our current sample scenario we only support simple URIs. In a real production environment OData service, which supports navigation and other OData V4 features, the code would be more complex.
2. **Fetch the data from backend**  
   In the backend, which in our sample is represented by the `Storage` class, we have a list with products.
   From this list we have to pick that one that is requested by the user.
   The information, which one is requested, is contained in the so-called *KeyPredicates*.
   In our OData model, we have only one property that is marked as “key”, it is the property with name *ID*
   In other models, the key could also be composed by multiple properties. In such a case, all key-properties are mentioned in the URI.
   That’s why the *KeyPredicate* information is provided as a list:

   ```java
      List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
   ```

   Now our task is to loop over all product entities that we have in our backend and to find that one that matches all keys. Which means that we have to loop over all key params.
   In our sample code, we have moved this logic to the *Util* class that has a *findEntity()* method, which loops over all existing product entities, and a *entityMatchesAllKeys()* method that checks if the given entity is the right one.
3. **Transform the data**  
   After fetching the Entity object from the backend, we have to convert it to an `InputStream`, using the proper `ODataSerializer` method:

   ```java
      ODataSerializer serializer = odata.createSerializer(responseFormat);
      SerializerResult serializerResult = serializer.entity(serviceMetadata, entityType, entity, options);
      InputStream entityStream = serializerResult.getContent();
   ```

   **Note:**
   The `ODataSerializer` object has to be configured with a `ContextURL` (in case that it is requested) and with `EntitySerializerOptions`. In our sample we keep the code simple, since we know that we don’t support advanced operations.
4. **Configure the response**  
   As usual, we have to set the body, the content type and the HTTP status code, as required by the specification.

The following snippet shows the implementation of the `readEntity(...)` method.

```java
    public void readEntity(ODataRequest request, ODataResponse response,
                            UriInfo uriInfo, ContentType responseFormat)
    		                throws ODataApplicationException, SerializerException {

    	// 1. retrieve the Entity Type
    	List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
    	// Note: only in our example we can assume that the first segment is the EntitySet
    	UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
    	EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

    	// 2. retrieve the data from backend
    	List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
    	Entity entity = storage.readEntityData(edmEntitySet, keyPredicates);

    	// 3. serialize
    	EdmEntityType entityType = edmEntitySet.getEntityType();

    	ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).build();
        // expand and select currently not supported
    	EntitySerializerOptions options = EntitySerializerOptions.with().contextURL(contextUrl).build();

    	ODataSerializer serializer = odata.createSerializer(responseFormat);
    	SerializerResult serializerResult = serializer.entity(serviceMetadata, entityType, entity, options);
    	InputStream entityStream = serializerResult.getContent();

    	//4. configure the response object
    	response.setContent(entityStream);
    	response.setStatusCode(HttpStatusCode.OK.getStatusCode());
    	response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

## 3.2. Adapt the DemoServlet class

As we’ve learned in our first tutorial, the Processor implementations have to be registered on the `ODataHttpHandler` instance in the servlet class.
Open the `DemoServlet` class and add the line that registers the `DemoEntityProcessor` instance:

```java
    // create odata handler and configure it with EdmProvider and Processor
    OData odata = OData.newInstance();
    ServiceMetadata edm = odata.createServiceMetadata(new DemoEdmProvider(),
    new ArrayList<EdmxReference>());
    ODataHttpHandler handler = odata.createHandler(edm);
    handler.register(new DemoEntityCollectionProcessor(storage));
    handler.register(new DemoEntityProcessor(storage));
```

## 3.3. Run the service

We have provided the implementation for the `readEntity(...)`, we have registered the processor and in the preparation section, we’ve created the `Storage` class and the `Util` class.
After building and deploying the project, we can invoke e.g. the following URL:

```text
http://localhost:8080/DemoService/DemoService.svc/Products(3)
```

and get the expected result:

```json
    {
      @odata.context: "$metadata#Products",
      ID: 3,
      Name: "Ergo Screen",
      Description: "19 Optimum Resolution 1024 x 768 @ 85Hz, resolution 1280 x 960"
    }
```

---

# 4. Implementation of Read Single Property

In the following section, We will add the capabilities to our service that allows the user to invoke e.g. the following URL:

```text
http://localhost:8080/DemoService/DemoService.svc/Products(1)/Description
```

Remember:
As described in our first tutorial, “Description” is the name of a property in our OData model.

![datamodel](olingo.apache.org/doc/odata4/tutorials/readep/model1.png "The OData model")

When a user invokes this URL, he doesn’t want to receive the full payload of the entity (since usually there are more properties than in our example), but instead, only the value of the property he is interested in.

Example result:

```json
    {
       @odata.context: "$metadata#Products/Description",
       value: "Notebook Basic, 1.7GHz - 15 XGA - 1024MB DDR2 SDRAM - 40GB"
    }
```

**Advantages:**  
Allows more performant implementation in the backend, (e.g. SQL statement).  
Sends less data through the network to the client (e.g. mobile phone).

**Note:**  
Don’t mix the above mentioned call with this one:

```text
http://localhost:8080/DemoService/DemoService.svc/Products(1)/Description/$value
```

Here, the response body contains only the pure value of the property, in plain text.
This can be realized by implementing the interface `PropertyValueProcessor` (see Appendix)

## 4.1. Implement the PrimitiveProcessor interface

The interface `org.apache.olingo.server.api.processor.PrimitiveProcessor` has 4 methods to implement:

- `init()`  
  Here we are initialized by the Framework to pass the context objects to us
- `readPrimitive`  
  This one is relevant for reading a single property of a single entity
- `updatePrimitive`  
  We will ignore this method in the present tutorial
- `deletePrimitive`  
  We will ignore this method in the present tutorial

Create the class `DemoPrimitiveProcessor` in package `myservice.mynamespace.service` which implements the interface `PrimitiveProcessor`

We have to create a Constructor that takes the `Storage` instance and stores it as a member variable:

```java
    public class DemoPrimitiveProcessor implements PrimitiveProcessor {

        private OData odata;
        private Storage storage;
        private ServiceMetadata serviceMetadata;

        public DemoPrimitiveProcessor(Storage storage) {
            this.storage = storage;
        }

        public void init(OData odata, ServiceMetadata serviceMetadata) {
            this.odata = odata;
            this.serviceMetadata = serviceMetadata;
        }
```

**readPrimitive**

Again, we have the following 4 steps to follow:

1. Which data is requested?  
   From the `UriInfo` object, we not only have to retrieve the information about the `EntitySet` that is requested, but as well the desired property.
2. Fetch the data from backend  
   Based on this information, we can retrieve the backend-data for the entity, just like we did in the `readEntity()` method, described above.
   The property value can then be extracted from it.
3. Transform the data  
   The third step is to serialize the backend data into an `InputStream` object.  
   For the current use case, the `ODataSerializer` instance offers a method called `primitive(...)`
4. Configure the response  
   When reading a property, we have to consider that the value of the property can be empty.  
   If this is the case, when configuring the response object, we don’t provide response body and header.

   ```java
      public void readPrimitive(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
                                throws ODataApplicationException, SerializerException {

               // 1. Retrieve info from URI
               // 1.1. retrieve the info about the requested entity set
               List<UriResource> resourceParts = uriInfo.getUriResourceParts();
               // Note: only in our example we can rely that the first segment is the EntitySet
               UriResourceEntitySet uriEntityset = (UriResourceEntitySet) resourceParts.get(0);
               EdmEntitySet edmEntitySet = uriEntityset.getEntitySet();
               // the key for the entity
               List<UriParameter> keyPredicates = uriEntityset.getKeyPredicates();

               // 1.2. retrieve the requested (Edm) property
               // the last segment is the Property
               UriResourceProperty uriProperty = (UriResourceProperty) resourceParts.get(resourceParts.size() -1);
               EdmProperty edmProperty = uriProperty.getProperty();
               String edmPropertyName = edmProperty.getName();
               // in our example, we know we have only primitive types in our model
               EdmPrimitiveType edmPropertyType = (EdmPrimitiveType) edmProperty.getType();

               // 2. retrieve data from backend
               // 2.1. retrieve the entity data, for which the property has to be read
               Entity entity = storage.readEntityData(edmEntitySet, keyPredicates);
               if (entity == null) { // Bad request
                   throw new ODataApplicationException("Entity not found",
                               HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
               }

               // 2.2. retrieve the property data from the entity
               Property property = entity.getProperty(edmPropertyName);
               if (property == null) {
                    throw new ODataApplicationException("Property not found",
                               HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
               }

               // 3. serialize
               Object value = property.getValue();
               if (value != null) {
                    // 3.1. configure the serializer
                    ODataSerializer serializer = odata.createSerializer(responseFormat);

                    ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).navOrPropertyPath(edmPropertyName).build();
                    PrimitiveSerializerOptions options = PrimitiveSerializerOptions.with().contextURL(contextUrl).build();
                    // 3.2. serialize
                    SerializerResult serializerResult = serializer.primitive(serviceMetadata, edmPropertyType, property, options);
                    InputStream propertyStream = serializerResult.getContent();

                    //4. configure the response object
                    response.setContent(propertyStream);
                    response.setStatusCode(HttpStatusCode.OK.getStatusCode());
                    response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
                 }else{
                     // in case there's no value for the property, we can skip the serialization
                     response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
                 }
    }
   ```

## 4.2. Adapt the DemoServlet class

The DemoServlet has to register a third processor:

```java
    // create odata handler and configure it with EdmProvider and Processor
    OData odata = OData.newInstance();
    ServiceMetadata edm = odata.createServiceMetadata(new DemoEdmProvider(),
    new ArrayList<EdmxReference>());
    ODataHttpHandler handler = odata.createHandler(edm);
    handler.register(new DemoEntityCollectionProcessor(storage));
    handler.register(new DemoEntityProcessor(storage));
    handler.register(new DemoPrimitiveProcessor(storage));
```

## 4.3. Run the service

We have provided the implementation for the `readPrimitive`, we have registered the processor and in the preparation section, we’ve created the `Storage` class and the `Util` class.
After building and deploying the project, we can invoke e.g. the following URL

```text
http://localhost:8080/DemoService/DemoService.svc/Products(ID=3)/Description
```

and get the expected result:

```json
    {
      @odata.context: "$metadata#Products/Description",
      value: "19 Optimum Resolution 1024 x 768 @ 85Hz, resolution 1280 x 960"
    }
```

Of course, all other properties can be accessed in the same way:

```text
http://localhost:8080/DemoService/DemoService.svc/Products(ID=3)/Name
http://localhost:8080/DemoService/DemoService.svc/Products(ID=3)/ID
```

---

# 5. Summary

In this tutorial we have learned how to implement the read operation for single entity and single property.
It has been based on a simple OData model, focusing on simple sample code and sample data.

In the next tutorial ([Part 3: Write](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)) we will learn how to implement write operations, i.e. create, update and delete of an entity.

---

# 6. Links

### Tutorials

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: Read Entity, Read Property
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial ODATA V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial ODATA V4 service, part 7: [Media Entities](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.0.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Part 5.2: System Query Options: `$select`, `$expand`

## Introduction

In the present tutorial, we will continue implementing **OData system query options**.
After we have learned the rather simple system query options `$top`, `$skip` and `$count` in the previous tutorial, we’re going to deal with `$select` and `$expand` in the present tutorial.

**Note:**  
The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).  
This tutorial can be found in subdirectory *\samples\tutorials\p6\_queryoptions-es*

**Disclaimer:**  
Again, in the present tutorial, we’ll focus only on the relevant implementation, in order to keep the code small and simple. The sample code as it is, shouldn’t be reused for advanced scenarios.

**Table of Contents**

1. Prerequisites
2. Preparation
3. Implementating system query options
   1. Implement `$select`
   2. Implement `$expand`
4. Run the implemented service
5. Summary
6. Links

---

# 1. Prerequisites

Same prerequisites as in [Tutorial Part 4: Navigation](doc/odata4/tutorials/navigation/tutorial_navigation.html) as well as basic knowledge about the concepts presented there.

Furthermore, basic knowledge about *System Query Options* (see [Tutorial Part 5.1](doc/odata4/tutorials/sqo_tcs/tutorial_sqo_tcs.html)) is helpful.

---

# 2. Preparation

Follow [Tutorial Part 4: Navigation](doc/odata4/tutorials/navigation/tutorial_navigation.html) or as shortcut import *Part 4: Navigation* into your Eclipse workspace.

Afterwards do a *Deploy and run*: it should be working.

---

# 3. Implementation

Open the class `myservice.mynamespace.service.DemoEntityProcessor`  
The method `readEntity()` contains the code for navigation which was treated in Tutorial Part 4. In the current tutorial we want to focus on query options and to keep the code as simple as possible, therefore we delete the code of the `readEntity()` method and start from scratch.

## 3.1. Implement `$select`

**Background**  
When requesting an entity collection from the backend, the OData service returns a list of entities and each entity contains a list of properties.  
In some cases, the user might not actually need all the properties. As such, he wants to tell the server to return only those properties that he is interested in.  
OData supports this requirement with the system query option `$select`.  
This parameter can be specified in the following ways:

- Specify one property name only: `$select=Name`
- Specify a comma-separated list of properties: `$select=Name,Description`
- Specify a star to include all properties: `$select=*`

**Example**  
First, just to remember how the full payload looks like, the “normal” query of the product without query options:  
[http://localhost:8080/DemoService/DemoService.svc/Products](http://localhost:8080/DemoService/DemoService.svc/Products)

![AllProductsNoQueryOption](olingo.apache.org/doc/odata4/tutorials/sqo_es/responseProducts_Full.jpg "The full list of Products")

The following request provides only one property for each entry in the collection:  
[http://localhost:8080/DemoService/DemoService.svc/Products?$select=Name](http://localhost:8080/DemoService/DemoService.svc/Products?$select=Name)

![AllProductsSelectName](olingo.apache.org/doc/odata4/tutorials/sqo_es/responseProducts_SelectName.jpg "The full list of Products, but displayling only the Name property")

**Implementation**  
The following section describes how to enable the `EntityProcessor` class and the `readEntity()` method for `$select`.

Because we start from scratch with the empty `readEntity()` method, we have to write a bit preparation code before we start with the implementation of the `$select`.  
The following lines are a copy of the Tutorial Part 2 and are necessary to fetch the data for a single entity.

```java
    // 1. retrieve the Entity Type
    List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
    UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
    EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();
    // 2. retrieve the data from backend
    List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
    Entity entity = storage.readEntityData(edmEntitySet, keyPredicates);
```

The Olingo library parses the `$select` option from the request and provides support in the serializer to serialize only the selected properties.  
Based on that the simplest implementation for `$select` is to get the `SelectOption` from the request (via `UriInfo` object) and pass this together with the enties to the serializer.  
The drawback of this implementation is, that the full payload is fetched from the backend and afterwards the unnecessary properties are removed. From performance point of view, this is not optimal. It would be better to fetch only the requested properties from the backend. Which of course depends on the backend.

In this tutorial we use the simple implementation to show the concept for `$select` so that there are only a few steps that have to be done by us.

- We have to get the SelectOption from the UriInfo:

  ```java
      // 3rd: apply system query options
      SelectOption selectOption = uriInfo.getSelectOption();
  ```
- We have to take care about the context URL, which is different in case that `$select` is used.
  Again, the Olingo library provides some support, which we use to build the select list that has to be passed to the ContextURL builder:

  ```java
      // we need the property names of the $select, in order to build the context URL
      String selectList = odata.createUriHelper().buildContextURLSelectList(edmEntityType,
                                                                            null, selectOption);
      ContextURL contextUrl = ContextURL.with()
                                        .entitySet(edmEntitySet)
                                        .selectList(selectList)
                                        .build();
  ```
- Furthermore, the serializer has to know about the usage of `$select`.
  Therefore, the serializer options instance is initialized with the selectOption object that we’ve obtained above. If this object is not null, then the serializer will take care to consider the `$select` statement

  ```java
      EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with()
                                                                                .contextURL(contextUrl)
                                                                                .select(selectOption)
                                                                                .build();
  ```

**The full implementation of the `readEntityCollection()` method:**

```java
    public void readEntityCollection(ODataRequest request, ODataResponse response,
                        UriInfo uriInfo, ContentType responseFormat)
                          throws ODataApplicationException, SerializerException {

      // 1st retrieve the requested EdmEntitySet from the uriInfo
      List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
      UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
      EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

      // 2nd: fetch the data from backend for this requested EntitySetName  
      EntityCollection entityCollection = storage.readEntitySetData(edmEntitySet);

      // 3rd: apply system query options
      // Note: $select is handled by the lib, we only configure ContextURL + SerializerOptions
      // for performance reasons, it might be necessary to implement the $select manually
      SelectOption selectOption = uriInfo.getSelectOption();

      // 4th: create a serializer based on the requested format (json)
      ODataSerializer serializer = odata.createSerializer(responseFormat);

      // and serialize the content: transform from the EntitySet object to InputStream
      EdmEntityType edmEntityType = edmEntitySet.getEntityType();
      // we need the property names of the $select, in order to build the context URL
      String selectList = odata.createUriHelper().buildContextURLSelectList(edmEntityType,
                                                                            null, selectOption);
      ContextURL contextUrl = ContextURL.with()
                                        .entitySet(edmEntitySet)
                                        .selectList(selectList)
                                        .build();

      // adding the selectOption to the serializerOpts will tell the lib to do the job
      final String id = request.getRawBaseUri() + "/" + edmEntitySet.getName();
      EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with()
                                                                                .contextURL(contextUrl)
                                                                                .select(selectOption)
                                                                                .id(id)
                                                                                .build();

      SerializerResult serializerResult = serializer.entityCollection(srvMetadata, edmEntityType,
                                                                          entityCollection, opts);

      // 5th: configure the response object: set the body, headers and status code
      response.setContent(serializerResult.getContent());
      response.setStatusCode(HttpStatusCode.OK.getStatusCode());
      response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

## 3.2. Implement `$expand`

**Background**

In order to understand the `$expand` system query option, let’s first quickly recap what we’ve learned in the navigation-tutorial:

1. In order to be able to navigate from one entity to another entity, we need at least 2 EntityTypes and at least one NavigationProperty  
   ![Metadata](olingo.apache.org/doc/odata4/tutorials/sqo_es/metadataNav.jpg "Declaring the navigation in the metadata")
2. We can invoke one single entity, e.g. display one product:  
   ![Products(1)SingleRead](olingo.apache.org/doc/odata4/tutorials/sqo_es/responseProducts_1.jpg "The result of a single read")
3. And we can follow the navigation to the second entity, by appending the navigation property name, e.g. invoke the category of that product.
   As we’ve seen in the metadata above, the name of the navigation property is *Category*  
   ![Products(1)NavToCat](olingo.apache.org/doc/odata4/tutorials/sqo_es/responseProducts_1_navCat.jpg "Navigating from a Product to its Category")

We have executed two requests to our OData service, in order to obtain the data for the product and for its related category.  
Now, since the category info is so tightly bound to the selected product, we’d like to get the same info by executing only **one** request.

This can be achieved with the `$expand`

The URL is built as follows:

- specify the request URI for the single read operation: [http://localhost:8080/DemoService/DemoService.svc/Products(1)](http://localhost:8080/DemoService/DemoService.svc/Products(1))
- append the `?` to indicate that system query options will follow
- append the `$expand`
- specify the name of the desired navigation property: [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category)

As a result, the data of both entities is provided within one payload.  
The data of the target entity is presented *inline*, which means as a child element of the source entity.

![Products(1)Expanded](olingo.apache.org/doc/odata4/tutorials/sqo_es/responseProducts_1_expandCat.jpg "The Product with its expanded Category")

One more advantage is that the system query option `$expand` can also be applied to an entity collection:

![AllProductsExpanded](olingo.apache.org/doc/odata4/tutorials/sqo_es/responseProducts_expandCat.jpg "All Products with expanded Category")

More details can be found in the [OData specification - Protocol](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398298) and [OData specification - Url Conventions](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part2-url-conventions/odata-v4.0-errata02-os-part2-url-conventions-complete.html#_Toc406398162)

**Implementation**

In the following section, we’ll focus on the implementation of the `$expand` for a single entity request e.g. [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category)

**Note:**  
The implementation for the entity collection is the same, just that we have to loop over all entities and apply the below code to each of them.

In brief, what we have to do is: fetch the data for both entities and merge into one entity

We can distinguish the following steps:

1. Retrieve the ExpandOption from Uri.  
   In our example: `$expand=Category`
2. Retrieve the (target) EdmEntityType which corresponds to the expand.  
   In our example: *Category*
3. Build the response data for the entity, enriched with the data of the expand entity.  
   In our example: *product1* merged with *category1*

Let's have a detailed look.

##### Step 1: Retrieve the ExpandOption from Uri

We need the retrieve the ExpandOption from the UriInfo:

```java
    ExpandOption expandOption = uriInfo.getExpandOption();
```

As usual, if this object is `null`, then the user hasn’t used the `$expand` in his request, and we don’t need to do anything.

##### Step 2: Retrieve the EdmEntityType corresponding to the expand

In brief: `ExpandOption` -> `NavigationProperty` -> `EdmEntityType`

From the `ExpandOption`, we can get the ExpandItems.  
An `ExpandItem` corresponds to the name of the navigation property.  
So for the URL [http://localhost:8080/DemoService/DemoService.svc/Products?$expand=Category](http://localhost:8080/DemoService/DemoService.svc/Products?$expand=Category) we get one `ExpandItem`, which corresponds to the navigation property *Category*.

In the present tutorial, we’re keeping the implementation as simple as possible.
So we’re relying on the fact that our example service only contains one navigation property per entity type.
Therefore, we can directly access the first `ExpandItem`.

```java
    ExpandItem expandItem = expandOption.getExpandItems().get(0);
```

**Note:**  
Most OData services will have more entity types and more navigation possibilities. In such services, it might be desired to invoke `$expand` with more than one navigation property, like for example: [http://localhost:8080/DemoService/DemoService.svc/Products?$expand=Category,Supplier,Sales](http://localhost:8080/DemoService/DemoService.svc/Products?$expand=Category,Supplier,Sales)  
Such `$expand` expression is not considered in our example.

Now that we have the `ExpandItem`, the next step is to extract the navigation property (`EdmNavicationProperty`) from it.

For the case of a request with `$expand=*` (to expand all navigation items which is checked via `expandItem.isStar()`), all known `EdmNavigationPropertyBinding`s from the expanded `EdmEntityType` have to be checked.
For our (reduced) sample service we know that only one navigation exists, hence the implementation is:

```java
    if(expandItem.isStar()) {
      List<EdmNavigationPropertyBinding> bindings = edmEntitySet.getNavigationPropertyBindings();
      // we know that there are navigation bindings
      // however normally in this case a check if navigation bindings exists is done
      if(!bindings.isEmpty()) {
        // can in our case only be 'Category' or 'Products', so we can take the first
        EdmNavigationPropertyBinding binding = bindings.get(0);
        EdmElement property = edmEntitySet.getEntityType().getProperty(binding.getPath());
        // we don't need to handle error cases, as it is done in the Olingo library
        if(property instanceof EdmNavigationProperty) {
          edmNavigationProperty = (EdmNavigationProperty) property;
        }
      }
    } else {
    ...
```

For the case of a request with defined name of navigation property to expand (e.g. `$expand=Category`), we have to ask the ExpandItem for its list if resource segments.  
The reason why an ExpandItem can be formed by multiple segments is that a navigation property can be in a `ComplexType`, such that it would be required to address it by a path.

In our simple example, we don’t need to specify a path, therefore we can safely write

```java
    ...
    } else {
      // can be 'Category' or 'Products', no path supported
      UriResource uriResource = expandItem.getResourcePath().getUriResourceParts().get(0);
      // we don't need to handle error cases, as it is done in the Olingo library
      if(uriResource instanceof UriResourceNavigation) {
        edmNavigationProperty = ((UriResourceNavigation) uriResource).getProperty();
      }
    }
```

This `uriResource` corresponds to the navigation property that we want to extract.  
We expect that the `uriResource` is of type `UriResourceNavigation`, such that we can cast.  
The `UriResourceNavigation` can then be asked for the `NavigationProperty` which in turn delivers the corresponding `EdmEntityType`, which we’re interested in.

Finally after one of above cases we have the necessary `EdmNavigationProperty` from which we need the `EdmEntityType` and the `name` of the navigation property to build the resopnse data.

```java
    if(edmNavigationProperty != null) {
      EdmEntityType expandEdmEntityType = edmNavigationProperty.getType();
      String navPropName = edmNavigationProperty.getName();
      ...
```

##### Step 3: Build the response data

Lets follow our example.
As we have said, we have to merge the data of two entities.  
The first one, the product, is already fetched, as we’ve done that earlier in the code:

```java
    Entity entity = storage.readEntityData(edmEntitySet, keyPredicates);
```

This entity corresponds to the product.

Now we’re ready to fetch the category, as we’ve already retrieved the `EdmEntityType` from the expand expression.  
We can invoke a helper method that we created in Tutorial Part 4 (Navigation), a helper method that is located in our database-mock and that returns the category entity corresponding to a given product entity:

```java
    Entity expandEntity = storage.getRelatedEntity(entity, expandEdmEntityType);
```

In our example, this `expandEntity` contains the data of the related category.

Now we have to merge both entities.  
This is done via a `Link` object that contains the inline entity object.  
And the link is added to the source entity.  
The relevant code is:

```java
    Link link = new Link();
    link.setTitle(navPropName);
    link.setInlineEntity(expandEntity);
    entity.getNavigationLinks().add(link);
```

**Note:**  
It is important to set the correct navigation property name, otherwise the linking doesn’t work and the inline data cannot be displayed.

##### Final step

Now that the response data has been built, we need to tell the serializer to consider the expand, otherwise the data will not be displayed.  
Also, the `expandOption` has to be considered while building the `ContextUrl`.

```java
    String selectList = odata.createUriHelper().buildContextURLSelectList(
                                                edmEntityType, expandOption, selectOption);
    ContextURL contextUrl = ContextURL.with()
                                      .entitySet(edmEntitySet)
                                      .selectList(selectList)
                                      .suffix(Suffix.ENTITY).build();

    // make sure that `$expand` and $select are considered by the serializer
    // adding the selectOption to the serializerOpts will actually tell the lib to do the job
    final String id = request.getRawBaseUri() + "/" + edmEntitySet.getName();
    EntitySerializerOptions opts = EntitySerializerOptions.with()
                                                          .contextURL(contextUrl)
                                                          .select(selectOption)
                                                          .expand(expandOption)
                                                          .id(id)
                                                          .build();
```

**Note:**  
The complete `readEntity(...)` method can be found in the *Appendix* at the end of the site or together with the `readEntityCollection(...)` method in the [sample project zip](http://www.apache.org/dyn/closer.cgi/olingo/odata4/Tutorials/DemoService_Tutorial_sqo_es.zip) ([md5](https://dist.apache.org/repos/dist/release/olingo/odata4/Tutorials/DemoService_Tutorial_sqo_es.zip.md5), [sha512](https://dist.apache.org/repos/dist/release/olingo/odata4/Tutorials/DemoService_Tutorial_sqo_es.zip.sha512), [pgp](https://dist.apache.org/repos/dist/release/olingo/odata4/Tutorials/DemoService_Tutorial_sqo_es.zip.asc)).

## 3.3. Implement `$expand` with options

**Background**

As of OData v4 spec, the expand can also be further refined with system query options

Some samples:

- Expand an navigation with only the first entity:

  - [http://localhost:8080/DemoService/DemoService.svc/Categories?$expand=Products($top=1)](http://localhost:8080/DemoService/DemoService.svc/Categories?$expand=Products($top=1))
- A common use case would be the following request, where all products are displayed along with their corresponding category, but only the interesting properties:

  - [http://localhost:8080/DemoService/DemoService.svc/Products?$select=Name,Description&$expand=Category($select=Name)](http://localhost:8080/DemoService/DemoService.svc/Products?$select=Name,Description&$expand=Category($select=Name))
- With respect to the system query options that are applied to the expand, multiple options are allowed, which are separated by semicolon, e.g.

  - [http://localhost:8080/DemoService/DemoService.svc/Categories(1)?$expand=Products($top=1;$select=Name)](http://localhost:8080/DemoService/DemoService.svc/Categories(1)?$expand=Products($top=1;$select=Name))
- The $select option can itself define a list of comma-separated properties:

  - [http://localhost:8080/DemoService/DemoService.svc/Categories(1)?$expand=NavToProducts($top=1;$select=Name,Description)](http://localhost:8080/DemoService/DemoService.svc/Categories(1)?$expand=NavToProducts($top=1;$select=Name,Description))

**Implementation**

The code for applying system query options to the `$expand` is similar to what is described in the Tutorial Part 5.1.
We only need to know from where to get the information about the query options used in the request URL: It is located in the `ExpandItem` instance.

The procedure is:

- Get the data for the navigation property (the normal expand)
- Refine the result by applying the system query options

**Support for `$select` with `$expand`**  
Like described in section 3.1, the Olingo library provides support and convenience methods for `$select` implementation. So the necessary creation and pass of the `selectList` to the creation of `ContextURL` and pass of the `selectOptions` to the `EntitySerializerOptions` is already done (see also code sample in the final step in section 3.2).

---

# 4. Run the implemented service

After building and deploying your service to your server, you can try the following URLs:

- The “normal” payload without query option

  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)](http://localhost:8080/DemoService/DemoService.svc/Products(1))
- Using `$select`

  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=Name](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=Name)
  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=Description](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=Description)
  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=Name,Description](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=Name,Description)
  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=\*](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=*)
  - [http://localhost:8080/DemoService/DemoService.svc/Categories(1)?$select=Name](http://localhost:8080/DemoService/DemoService.svc/Categories(1)?$select=Name)
- Using `$expand`

  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category)
  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category)
  - [http://localhost:8080/DemoService/DemoService.svc/Categories(1)?$expand=Products](http://localhost:8080/DemoService/DemoService.svc/Categories(1)?$expand=Products)
  - [http://localhost:8080/DemoService/DemoService.svc/Categories(1)?$expand=\*](http://localhost:8080/DemoService/DemoService.svc/Categories(1)?$expand=*)
- Using `$select` and `$expand`

  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=Name&$expand=Category](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=Name&$expand=Category)
- Using `$expand` with nested `$select`  
  We’re interested in *Product* and the name of its *Category*

  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category($select=Name)](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category($select=Name))
  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category($select=Name,ID)](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category($select=Name,ID))
- Using `$select` and `$expand` with nested $select  
  We’re interested in *Product* and its *Category*, but only the *name* of both

  - [http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=Name&$expand=Category($select=Name)](http://localhost:8080/DemoService/DemoService.svc/Products(1)?$select=Name&$expand=Category($select=Name))

**Note:**
The same system query option expressions can be applied to entity collections

---

# 5. Summary

In this tutorial we have learned the basics of the `$select` and `$expand` *system query options* within the OData context as well as how to implement those features with the *Apache Olingo library* by using the provided convenience and support methods.

---

# 6. Links

### Tutorials

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: System Query Options $select, $expand (this page)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial ODATA V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial ODATA V4 service, part 7: [Media Entities](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.6.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

# 7. Appendix

### Sample code snippets

**readEntity(...)**

```java
    public void readEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
            throws ODataApplicationException, SerializerException {

      // 1. retrieve the Entity Type
      List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
      // Note: only in our example we can assume that the first segment is the EntitySet
      UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
      EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

      // 2. retrieve the data from backend
      List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
      Entity entity = storage.readEntityData(edmEntitySet, keyPredicates);

      // 3. apply system query options

      // handle $select
      SelectOption selectOption = uriInfo.getSelectOption();
      // in our example, we don't have performance issues, so we can rely upon the handling in the Olingo lib
      // nothing else to be done

      // handle $expand
      ExpandOption expandOption = uriInfo.getExpandOption();
      // in our example: http://localhost:8080/DemoService/DemoService.svc/Categories(1)/$expand=Products
      // or http://localhost:8080/DemoService/DemoService.svc/Products(1)?$expand=Category
      if(expandOption != null) {
        // retrieve the EdmNavigationProperty from the expand expression
        // Note: in our example, we have only one NavigationProperty, so we can directly access it
        EdmNavigationProperty edmNavigationProperty = null;
        ExpandItem expandItem = expandOption.getExpandItems().get(0);
        if(expandItem.isStar()) {
          List<EdmNavigationPropertyBinding> bindings = edmEntitySet.getNavigationPropertyBindings();
          // we know that there are navigation bindings
          // however normally in this case a check if navigation bindings exists is done
          if(!bindings.isEmpty()) {
            // can in our case only be 'Category' or 'Products', so we can take the first
            EdmNavigationPropertyBinding binding = bindings.get(0);
            EdmElement property = edmEntitySet.getEntityType().getProperty(binding.getPath());
            // we don't need to handle error cases, as it is done in the Olingo library
            if(property instanceof EdmNavigationProperty) {
              edmNavigationProperty = (EdmNavigationProperty) property;
            }
          }
        } else {
          // can be 'Category' or 'Products', no path supported
          UriResource uriResource = expandItem.getResourcePath().getUriResourceParts().get(0);
          // we don't need to handle error cases, as it is done in the Olingo library
          if(uriResource instanceof UriResourceNavigation) {
            edmNavigationProperty = ((UriResourceNavigation) uriResource).getProperty();
          }
        }

        // can be 'Category' or 'Products', no path supported
        // we don't need to handle error cases, as it is done in the Olingo library
        if(edmNavigationProperty != null) {
          EdmEntityType expandEdmEntityType = edmNavigationProperty.getType();
          String navPropName = edmNavigationProperty.getName();

          // build the inline data
          Link link = new Link();
          link.setTitle(navPropName);
          link.setType(Constants.ENTITY_NAVIGATION_LINK_TYPE);
          link.setRel(Constants.NS_ASSOCIATION_LINK_REL + navPropName);

          if(edmNavigationProperty.isCollection()){ // in case of Categories(1)/$expand=Products
            // fetch the data for the $expand (to-many navigation) from backend
            // here we get the data for the expand
            EntityCollection expandEntityCollection = storage.getRelatedEntityCollection(entity, expandEdmEntityType);
            link.setInlineEntitySet(expandEntityCollection);
            link.setHref(expandEntityCollection.getId().toASCIIString());
          } else {  // in case of Products(1)?$expand=Category
            // fetch the data for the $expand (to-one navigation) from backend
            // here we get the data for the expand
            Entity expandEntity = storage.getRelatedEntity(entity, expandEdmEntityType);
            link.setInlineEntity(expandEntity);
            link.setHref(expandEntity.getId().toASCIIString());
          }

          // set the link - containing the expanded data - to the current entity
          entity.getNavigationLinks().add(link);
        }
      }

      // 4. serialize
      EdmEntityType edmEntityType = edmEntitySet.getEntityType();
      // we need the property names of the $select, in order to build the context URL
      String selectList = odata.createUriHelper().buildContextURLSelectList(edmEntityType, expandOption, selectOption);
      ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet)
                                                .selectList(selectList)
                                                .suffix(Suffix.ENTITY)
                                                .build();

      // make sure that $expand and $select are considered by the serializer
      // adding the selectOption to the serializerOpts will actually tell the lib to do the job
      EntitySerializerOptions opts = EntitySerializerOptions.with()
                                                            .contextURL(contextUrl)
                                                            .select(selectOption)
                                                            .expand(expandOption)
                                                            .build();

      ODataSerializer serializer = this.odata.createSerializer(responseFormat);
      SerializerResult serializerResult = serializer.entity(srvMetadata, edmEntityType, entity, opts);

      // 5. configure the response object
      response.setContent(serializerResult.getContent());
      response.setStatusCode(HttpStatusCode.OK.getStatusCode());
      response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Part 5.4: System Query Options: `$filter`

## Introduction

In the present tutorial, we’ll continue implementing OData system query options, this time focusing on `$filter`

**Note:**
The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).  
This tutorial can be found in subdirectory *\samples\tutorials\p8\_queryoptions-f*

**Table of Contents**

1. Preparation
2. Implementation
   1. Implement `$filter`
3. Run the implemented service
4. Summary
5. Links

# 1. Preparation

Follow [Tutorial Part 1: Read Entity Collection](doc/odata4/tutorials/read/tutorial_read.html) and [Tutorial Part 2: Read Entity](doc/odata4/tutorials/readep/tutorial_readep.html) or as shortcut import *Part 2: Read Entity, Read Property* into your Eclipse workspace.

Afterwards do a *Deploy and run*: it should be working.

# 2. Implementation

The system query options we’re focusing on are applied to the entity collection only, therefore our
implementation for the `$filter` query options is done in the class
`myservice.mynamespace.service.DemoEntityCollectionProcessor`

The general sequence is again:

1. Analyze the URI
2. Fetch data from backend
3. Apply the system query option
4. Serialize
5. Configure the response

### 2.1 Implement `$filter`

#### Background

When requesting a list of entities from a service, the default behaviour is to return all entities on the list. The consumer of an OData service might want to be able to receive a subset by specifying certain criteria which each of the returned entities have to fulfill.  
For example, a common use case would be to request all products with a specified minimum and maximum price.
OData supports this requirement with the system query option `$filter`

It is specified as follows:

```text
$filter=<BooleanExpression>
```

See here for more details:  
[OData Version 4.0 Part 1: Protocol Plus Errata 02](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398301)

[OData Version 4.0 Part 2: URL Conventions Plus Errata 02](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part2-url-conventions/odata-v4.0-errata02-os-part2-url-conventions-complete.html#_Toc406398094)

The expression given by the `$filter` query option has to return a Boolean value when applied to a certain entity on the entity list. If the value returned for a given entity is *“true”*, the service has to return the entity. Otherwise the service has to discard the entity.

**Example**

First, just to remember how the full payload looks like, the “normal” query of the product:

[http://localhost:8080/DemoService/DemoService.svc/Products](http://localhost:8080/DemoService/DemoService.svc/Products)

![AllProductsWithoutFilter](olingo.apache.org/doc/odata4/tutorials/sqo_f/no_filter.png "All products without filter")

Now have a look to the following Uri:  
[http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID eq 1 or contains(Description,'1280')]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID eq 1 or contains(Description,'1280'))

The `$filter` system query option has been applied to the Products Entity Collection. The client requests all products which fulfills the following condition: ID equals to one or the Description should contain the string ‘1280’

![ProductsWithFilter](olingo.apache.org/doc/odata4/tutorials/sqo_f/filter_applied.png "Products with applied $filter")

**Visitor pattern**

First things first, the Uri parser creates an *abstract syntax tree* (AST). An abstract syntax tree describes the expression in a hierarchical way. (see figure 1) For example to calculate the root node, all nodes below have to be calculated first. The idea is to traverse the tree in pre order (depth-first).

Consider the following Uri

```text
“/Products?$format=(Price lt 2000) and contains(Description,’Notebook’)”.
```

As you can see, the intention is to request all Products, which costs less than 2000 monetary units and contains the word ‘Notebook’ in their description. The expression is split up in two parts by the binary operator “and”. To calculate the result of the “and” node, the left and also the right child have to be calculated first. The left child itself is another binary operation. So to calculate “less than” the type and value of the property “Price” has to be determined. And so on...

![AbstractSyntaxTree](olingo.apache.org/doc/odata4/tutorials/sqo_f/ast.png "Abstract syntax tree of the filter expression “Price lt 2000 and contains(Description,’Notebook’)")

Key  
![AbstractSyntaxTree](olingo.apache.org/doc/odata4/tutorials/sqo_f/keyAST.png "Key abstract syntax tree")

So the following actions have to be done (The values of the properties are fictitious):

| Action | Result - Type | Result | Method |
| --- | --- | --- | --- |
| 1. Get the value of the property “Price” | Edm.Double | 500.00 | visitMember |
| 2. Determine the Type and value of the literal 2000.00 | Edm.Double | 2000.00 | visitLiteral |
| 3. Calculate – 500.00lt2000 | Edm.Boolean | true | visitBinaryOperator |
| 4. Get the value of the Property “Description” | Edm.String | "Notebook basic..." | visitMember |
| 5. Determine the type and value of the literal ‘Notebook’ | Edm.String | “Notebook” | visitLiteral |
| 6. Calculate –contains(“Notebook Basic…”, “Notebook”) | Edm.Boolean | true | visitMethodCall |
| 7. Calculate – trueandtrue | Edm.Boolean | true | visitBinaryOperator |

Olingo uses the vistor pattern to traverse the AST. Each of these actions is mapped to one method of the ExpressionVistor interface. You can see the name of the methods in last column of table 1. As service developers we have to implement this methods but we do not have to take care about calling them. The libaray will call the proper method and we have only to calculate the result.

#### Implementation

First we will create the *Filter Expression Visitor* and after that, we will integrate the just created Visitor in `EntityCollectionProcessor`.

**1.1 Create our FilterExpressionVisitor**

Create a new class `FilterExpressionVisitor` in package `myservice.mynamespace.service` and  
implement the Interface `org.apache.olingo.server.api.uri.queryoption.expression.ExpressionVisitor`.

As you mentioned the interface needs a generic parameter.
This generic type is used as (return) parameter for the visitXXX methods (e.g. `visitLiteral` ). It is up to your implementation to
choose a proper type for your use case. The main task is to keep track of the type and also the return value of a node in the abstract syntax tree.
In real world scenarios it is common to build a statement to query a database or backend instead modifying the preloaded data.

In this tutorial we will use just `Object` and pass the native Java values around.

```java
public class FilterExpressionVisitor implements ExpressionVisitor<Object> {
```

Please create also a constructor to pass an entity to our visitor implementation.

```java
private Entity currentEntity;

public FilterExpressionVisitor(Entity currentEntity) {
    this.currentEntity = currentEntity;
}
```

**1.2 Implement the interface**

In this basic tutorial we will implement only a subset of the Expression Visitor.
The following methods will **not** be implemented. Add an `ODataApplicationException` to their bodies:

- `public Object visitTypeLiteral(EdmType type)`
- `public Object visitAlias(String aliasName)`
- `public Object visitEnum(EdmEnumType type, List<String> enumValues)`
- `public Object visitLambdaExpression(String lambdaFunction, String lambdaVariable, Expression expression)`
- `public Object visitLambdaReference(String variableName)`

**Example**

```java
@Override
public Object visitTypeLiteral(EdmType type) throws ExpressionVisitException, ODataApplicationException {
    throw new ODataApplicationException("Type literals are not implemented",
        			HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ENGLISH);
}
```

**Implement method visitMember**  
This method is been called if the current node in the AST is a property. So all we have to do is to take the current entity and return the value of the addressed property.

```java
public Object visitMember(UriInfoResource member) throws ExpressionVisitException, ODataApplicationException {
    // To keeps things simple, this tutorial allows only primitive properties.
    // We have faith that the java type of Edm.Int32 is Integer
    final List<UriResource> uriResourceParts = member.getUriResourceParts();

    // Make sure that the resource path of the property contains only a single segment and a
    // primitive property has been addressed. We can be sure, that the property exists because  
    // the UriParser checks if the property has been defined in service metadata document.

    if(uriResourceParts.size() == 1 && uriResourceParts.get(0) instanceof UriResourcePrimitiveProperty) {
      UriResourcePrimitiveProperty uriResourceProperty = (UriResourcePrimitiveProperty) uriResourceParts.get(0);
      return currentEntity.getProperty(uriResourceProperty.getProperty().getName()).getValue();
    } else {
      // The OData specification allows in addition complex properties and navigation    
      // properties with a target cardinality 0..1 or 1.
      // This means any combination can occur e.g. Supplier/Address/City
      //  -> Navigation properties  Supplier
      //  -> Complex Property       Address
      //  -> Primitive Property     City
      // For such cases the resource path returns a list of UriResourceParts
      throw new ODataApplicationException("Only primitive properties are implemented in filter
          expressions", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ENGLISH);
    }
}
```

**Implement method visitLiteral**

The next method takes a String and has to return the type and also the value of literal.

**Example**

- "`‘1’`" is a string with the value "`1`"
- "`1`" could be an Edm.Byte, Edm.SByte, Edm.Int16, Edm.Int32, Edm.Int64, Edm.Single, Edm.Double, Edm.Decimal with value 1

As you can see in this little example, it can be difficult to guess the right type. In this tutorial we will focus on Edm.Int32.

In real world scenarios, there is something called “numeric promotion”, which converts numbers to the next higher type. [OData Version 4.0 Part 2: URL Conventions Plus Errata 02](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part2-url-conventions/odata-v4.0-errata02-os-part2-url-conventions-complete.html#_Toc406398161)

```java
@Override
public Object visitLiteral(Literal literal) throws ExpressionVisitException, ODataApplicationException {
    // To keep this tutorial simple, our filter expression visitor supports only Edm.Int32 and Edm.String
    // In real world scenarios it can be difficult to guess the type of an literal.
    // We can be sure, that the literal is a valid OData literal because the URI Parser checks
    // the lexicographical structure
     // String literals start and end with an single quotation mark
    String literalAsString = literal.getText();
    if(literal.getType() instanceof EdmString) {
        String stringLiteral = "";
        if(literal.getText().length() > 2) {
            stringLiteral = literalAsString.substring(1, literalAsString.length() - 1);
        }

        return stringLiteral;
    } else {
        // Try to convert the literal into an Java Integer
        try {
            return Integer.parseInt(literalAsString);
        } catch(NumberFormatException e) {
            throw new ODataApplicationException("Only Edm.Int32 and Edm.String literals are implemented",
                HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ENGLISH);
        }
    }
}
```

**Implement the operators**

The first two implemented methods dealt on the leaves of the AST. Now we will implement the operations, which can be performed on these values.

The idea behind the implementation is always the same.

1. Check if the types fit together
2. If true => Calculate and return the result
3. Otherwise => Throw an `ODataApplicationException` with StatusCode 400 Bad Request

OData supports two different unary operators. First there is the binary negation (*not*) and second the arithmetic minus (*-*).

```java
public Object visitUnaryOperator(UnaryOperatorKind operator, Object operand)
      throws ExpressionVisitException, ODataApplicationException {
    // OData allows two different unary operators. We have to take care, that the type of the
    // operand fits to the operand

    if(operator == UnaryOperatorKind.NOT && operand instanceof Boolean) {
      // 1.) boolean negation
      return !(Boolean) operand;
    } else if(operator == UnaryOperatorKind.MINUS && operand instanceof Integer){
      // 2.) arithmetic minus
      return -(Integer) operand;
    }

    // Operation not processed, throw an exception
    throw new ODataApplicationException("Invalid type for unary operator",
        HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
}
```

Next are the binary operations. Have a look at the source code comments for a detailed explanation.´

```java
@Override
public Object visitBinaryOperator(BinaryOperatorKind operator, Object left, Object right)     
            throws ExpressionVisitException, ODataApplicationException {

    // Binary Operators are split up in three different kinds. Up to the kind of the
    // operator it can be applied to different types
    //   - Arithmetic operations like add, minus, modulo, etc. are allowed on numeric
    //     types like Edm.Int32
    //   - Logical operations are allowed on numeric types and also Edm.String
    //   - Boolean operations like and, or are allowed on Edm.Boolean
    // A detailed explanation can be found in OData Version 4.0 Part 2: URL Conventions

    if (operator == BinaryOperatorKind.ADD
        || operator == BinaryOperatorKind.MOD
        || operator == BinaryOperatorKind.MUL
        || operator == BinaryOperatorKind.DIV
        || operator == BinaryOperatorKind.SUB) {
      return evaluateArithmeticOperation(operator, left, right);
    } else if (operator == BinaryOperatorKind.EQ
        || operator == BinaryOperatorKind.NE
        || operator == BinaryOperatorKind.GE
        || operator == BinaryOperatorKind.GT
        || operator == BinaryOperatorKind.LE
        || operator == BinaryOperatorKind.LT) {
      return evaluateComparisonOperation(operator, left, right);
    } else if (operator == BinaryOperatorKind.AND
        || operator == BinaryOperatorKind.OR) {
      return evaluateBooleanOperation(operator, left, right);
    } else {
      throw new ODataApplicationException("Binary operation " + operator.name() + " is not
           implemented", HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ENGLISH);
    }
}

private Object evaluateBooleanOperation(BinaryOperatorKind operator, Object left, Object right)
 	    throws ODataApplicationException {

    // First check that both operands are of type Boolean
    if(left instanceof Boolean && right instanceof Boolean) {
       Boolean valueLeft = (Boolean) left;
       Boolean valueRight = (Boolean) right;

       // Than calculate the result value
       if(operator == BinaryOperatorKind.AND) {
           return valueLeft && valueRight;
       } else {
           // OR
           return valueLeft || valueRight;
       }
   } else {
       throw new ODataApplicationException("Boolean operations needs two numeric operands",
                 HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
   }
}

private Object evaluateComparisonOperation(BinaryOperatorKind operator, Object left, Object right) throws ODataApplicationException {

    // All types in our tutorial supports all logical operations, but we have to make sure that   
    // the types are equal
    if(left.getClass().equals(right.getClass())) {
      // Luckily all used types String, Boolean and also Integer support the interface
      // Comparable
      int result;
      if(left instanceof Integer) {
        result = ((Comparable<Integer>) (Integer) left).compareTo((Integer) right);
      } else if(left instanceof String) {
        result = ((Comparable<String>) (String) left).compareTo((String) right);
      } else if(left instanceof Boolean) {
        result = ((Comparable<Boolean>) (Boolean) left).compareTo((Boolean) right);
      } else {
        throw new ODataApplicationException("Class " + left.getClass().getCanonicalName() + " not expected",
            HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.ENGLISH);
      }

      if (operator == BinaryOperatorKind.EQ) {
        return result == 0;
      } else if (operator == BinaryOperatorKind.NE) {
        return result != 0;
      } else if (operator == BinaryOperatorKind.GE) {
        return result >= 0;
      } else if (operator == BinaryOperatorKind.GT) {
        return result > 0;
      } else if (operator == BinaryOperatorKind.LE) {
        return result <= 0;
      } else {
        // BinaryOperatorKind.LT
        return result < 0;
      }

    } else {
      throw new ODataApplicationException("Comparison needs two equal types",
          HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
    }
}

private Object evaluateArithmeticOperation(BinaryOperatorKind operator, Object left,
      	Object right) throws ODataApplicationException {

    // First check if the type of both operands is numerical
    if(left instanceof Integer && right instanceof Integer) {
        Integer valueLeft = (Integer) left;
        Integer valueRight = (Integer) right;

        // Than calculate the result value
        if(operator == BinaryOperatorKind.ADD) {
          return valueLeft + valueRight;
        } else if(operator == BinaryOperatorKind.SUB) {
          return valueLeft - valueRight;
        } else if(operator == BinaryOperatorKind.MUL) {
          return valueLeft * valueRight;
        } else if(operator == BinaryOperatorKind.DIV) {
          return valueLeft / valueRight;
        } else {
          // BinaryOperatorKind,MOD
          return valueLeft % valueRight;
        }
    } else {
        throw new ODataApplicationException("Arithmetic operations needs two numeric
     		operands", HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
    }
}
```

The last method we have to implement is `visitMethodCall`. The principle is always the same, check the types and calculate the return value. As a developer you can be sure, that the number of parameters fits to the MethodKind but the types have to be checked by yourself. E.g. *contains* takes two Strings and return *Edm.Boolean* but

```text
   $filter=contains(123,123)
```

would not lead to an error. It is up to you to throw an exception.

```java
@Override
public Object visitMethodCall(MethodKind methodCall, List<Object> parameters)
	    throws ExpressionVisitException, ODataApplicationException {

    // To keep this tutorial small and simple, we implement only one method call
    // contains(String, String) -> Boolean
    if(methodCall == MethodKind.CONTAINS) {
      if(parameters.get(0) instanceof String && parameters.get(1) instanceof String) {
        String valueParam1 = (String) parameters.get(0);
        String valueParam2 = (String) parameters.get(1);

        return valueParam1.contains(valueParam2);
      } else {
        throw new ODataApplicationException("Contains needs two parametes of type Edm.String",
            HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
      }
    } else {
      throw new ODataApplicationException("Method call " + methodCall + " not implemented",
          HttpStatusCode.NOT_IMPLEMENTED.getStatusCode(), Locale.ENGLISH);
    }
}
```

**2. EntityCollectionProcessor changes**

The following section describes the simple approach to enable the EntityCollectionProcessor class and the readEntityCollection() method for `$filter`.

Just like in the previous tutorials, the data is first fetched from the backend, then the system query option is applied.

```java
EntityCollection entityCollection = storage.readEntitySetData(edmEntitySet);
List<Entity> entityList = entityCollection.getEntities();
```

We will proceed according to these 4 steps:

1. Get the query option from the UriInfo. If null is returned then nothing has to be done.
2. Get the expression from the query option
3. Instantiate our Expression Visitor and evaluate the result for each entity in the collection
4. Modify the EntityCollection based on the result of the expression

**2.1 Get the FilterOption from the uriInfo**

```text
FilterOption filterOption = uriInfo.getFilterOption();
if(filterOption != null) {
```

**2.2 Get the expression from the query option**

```text
Expression filterExpression = filterOption.getExpression();
```

**2.3 Loop over all entities in the collection and calculate the result of the expression for a given entity**

```java
    try {
      List<Entity> entityList = entityCollection.getEntities();
      Iterator<Entity> entityIterator = entityList.iterator();

      // Evaluate the expression for each entity
      // If the expression is evaluated to "true", keep the entity otherwise remove it from
      // the entityList
      while (entityIterator.hasNext()) {
        // To evaluate the the expression, create an instance of the Filter Expression
        // Visitor and pass the current entity to the constructor
        Entity currentEntity = entityIterator.next();
        FilterExpressionVisitor expressionVisitor = new FilterExpressionVisitor(currentEntity);

        // Evaluating the expression
        Object visitorResult = filterExpression.accept(expressionVisitor);
        …
```

**2.4 Modify the collection**

```java
         // The result of the filter expression must be of type Edm.Boolean
         if(visitorResult instanceof Boolean) {
            if(!Boolean.TRUE.equals(visitorResult)) {
              // The expression evaluated to false (or null), so we have to remove the
              // currentEntity from entityList
    	      entityIterator.remove();
            }
         } else {
             throw new ODataApplicationException("A filter expression must evaulate to type Edm.Boolean", HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ENGLISH);
         }
      } // End while
    } catch (ExpressionVisitException e) {
       throw new ODataApplicationException("Exception in filter evaluation",
                     HttpStatusCode.INTERNAL_SERVER_ERROR.getStatusCode(), Locale.ENGLISH);
    }
```

### 3. Run the implemented service

After building and deploying your service to your server, you can try the following URLs:

**Comparison operators**

- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID eq 1]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID) eq 1)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID ne 1]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID) ne 1)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID gt 2]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID) gt 2)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID ge 2]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID) ge 2)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID le 2]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID) le 2)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID lt 2]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID) lt 2)

**Unary operators**

- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=-ID eq -1]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=-ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=-ID) eq -1)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=not(ID eq 1)]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=not(ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=not(ID) eq 1))

**Method calls and strong binding unary not**

- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=contains(Name,'Ergo')](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=contains(Name,'Ergo'))
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=not contains(Name,'Ergo')]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=not](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=not) contains(Name,'Ergo'))

**Arithmetic operators**

- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID add 1 eq 2]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID) add 1 eq 2)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID sub 1 eq 1]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID) sub 1 eq 1)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID div 2 eq 1]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID) div 2 eq 1)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID mul 2 eq 6]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID) mul 2 eq 6)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID mod 2 eq 1]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=ID) mod 2 eq 1)

**String literal**

- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=Name eq '1UMTS PDA']([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=Name](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=Name) eq '1UMTS PDA')

**Boolean operators**

- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=contains(Name,'Ergo') or ID eq 1]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=contains(Name,'Ergo')](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=contains(Name,'Ergo')) or ID eq 1)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=contains(Name,'Ergo') and ID eq 1]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=contains(Name,'Ergo')](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=contains(Name,'Ergo')) and ID eq 1)
- [http://localhost:8080/DemoService/DemoService.svc/Products?$filter=contains(Name,'Ergo') and ID eq 3]([http://localhost:8080/DemoService/DemoService.svc/Products?$filter=contains(Name,'Ergo')](http://localhost:8080/DemoService/DemoService.svc/Products?$filter=contains(Name,'Ergo')) and ID eq 3)

## Summary

In this tutorial we have learned how to implement a simple service with `$filter` system query option. The very same Expression Visitor can be used to support advanced $orderby query options. The main difference is that, the Expression Visitor used by $orderby returns a (may be calculated) value of a primitive property instead a Boolean value.

## Links

### Tutorials

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: System Query Options $filter (this page)
- Tutorial ODATA V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial ODATA V4 service, part 7: [Media Entities](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.0.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Part 5.3: System Query Options: `$orderby`

## Introduction

In the present tutorial, we will continue implementing OData system query options, this time focusing on `$orderby`

**Note:**
The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).  
This tutorial can be found in subdirectory *\samples\tutorials\p7\_queryoptions-o*

**Disclaimer:**
Again, in the present tutorial, we will focus only on the relevant implementation, in order to keep the code small and simple. The sample code as it is, shouldn’t be reused for advanced scenarios.

**Table of Contents**

1. Prerequisites
2. Preparation
3. Implementation
   1. Implement `$orderby`
4. Run the implemented service
5. Summary
6. Links

\_\_

# 1. Prerequisites

Same prerequisites as in [Tutorial Part 1: Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
and [Tutorial Part 2: Read Entity](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep) as well as basic knowledge about the concepts presented in both tutorials.

Furthermore, Tutorial Part 5.1 should have been read.

# 2. Preparation

Follow *Tutorial Part 1: Read Entity Collection* and *Tutorial Part 2: Read Entity* or as shortcut import *Part 2: Read Entity, Read Property* into your Eclipse workspace.

Afterwards do a *Deploy and run*: it should be working.

# Implementation

The system query options we’re focusing on are applied to the entity collection only, therefore our
implementation for all query options is done in the class
*myservice.mynamespace.service.DemoEntityCollectionProcessor*

The general sequence is again:

1. Analyze the URI
2. Fetch data from backend
3. Apply the system query option
4. Serialize
5. Configure the response

## 3.1. Implement `$orderby`

**Background**

When requesting a list of entities from a service, it is up to the service implementation to decide in which order they are presented. This can depend on the backend data source, anyways, it is undefined.
But the consumer of an OData service might want to be able to specify the order, according to his needs.

For example, a usual case would be that the list of entities is sorted as per default by its ID number, but for a user, the ID is not relevant and he would prefer a sorting e.g. by the name
OData supports this requirement with the system query option `$orderby`
It is specified as follows:

```text
$orderby=<propertyName>
```

The order can be ascending or descending:

```text
$orderby=<propertyName> asc
$orderby=<propertyName> desc
```

If not specified, the default is ascending.

See here for more details:
[OData Version 4.0 Part 1: Protocol Plus Errata 02](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398305)

**Note:**
As of the OData specification, the `$orderby` system query option can be applied to multiple properties.
In that case, the value is specified as comma-separated list.

**Example:**

```xml
<http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Name asc, Description desc>
```

In this example, all the products are sorted by their name. Moreover, all products with the same name are sorted by their description in descending order.
Another example could be that I want to display all my customers, they should be sorted by their country. Additionally, within each country, they should be sorted by their name

In order to support such sorting, the OData service implementation has to make use of the *ExpressionVisitor* concept.
We haven’t used it in the present tutorial, because the ExpressionVisitor will be explained in the $filter section

**Example**
First, just to remember how the full payload looks like, the “normal” query of the product:
[http://localhost:8080/DemoService/DemoService.svc/Products](http://localhost:8080/DemoService/DemoService.svc/Products)

![AllProductsNotSorted](olingo.apache.org/doc/odata4/tutorials/sqo_o/products_unsorted.png "All products not sorted")

The following request specifies the sorting by the name.
The order is ascending, if not specified elsewise.

[http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Name](http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Name)

![ProductsOrderedByNameAsc](olingo.apache.org/doc/odata4/tutorials/sqo_o/products_bynameasc.png "All products sorted by name ascending")

**Implementation**

The following section describes the simple approach to enable the *EntityCollectionProcessor* class and the *readEntityCollection()* method for *$orderby*.

Just like in the previous tutorials, the data is first fetched from the backend, then the system query option is applied.

```java
    EntityCollection entityCollection = storage.readEntitySetData(edmEntitySet);
    List<Entity> entityList = entityCollection.getEntities();
```

We will proceed according to these 4 steps:

1. Get the query option from the *UriInfo*. If null is returned then nothing has to be done.
2. Get the value from the query option
3. Analyze the value
4. Modify the *EntityCollection*

**1. Get the OrderByOption from the UriInfo:**

```java
    OrderByOption orderByOption = uriInfo.getOrderByOption();
    if (orderByOption != null) {
```

**2. Get the value of the OrderByOption:**

```java
    List<OrderByItem> orderItemList = orderByOption.getOrders();
    final OrderByItem orderByItem = orderItemList.get(0);
```

The instance of an OrderByOption can be asked for the list of its *OrderByItems*.
Why a list?
Because the `$orderby` expression can be composed with multiple properties
For example, for the following URL, we get 2 OrderByItems:

```text
http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Name asc, Description desc
```

In our example, we support only one property, therefore we directly access the first OrderByItem in the list.

**3. Analyze the value**

What do we want to do?
From the backend we got a list of entities that are products. We want to apply a sorter to that list and we want to sort by the property name that is given in the URI.
In our example, the property name that is provided in the URI can be “Name”, “Description” or “ID”
So we have to retrieve the property name from the URI.

```java
    Expression expression = orderByItem.getExpression();
    if(expression instanceof Member){
    	UriInfoResource resourcePath = ((Member)expression).getResourcePath();
    	UriResource uriResource = resourcePath.getUriResourceParts().get(0);
    	if (uriResource instanceof UriResourcePrimitiveProperty) {
           EdmProperty edmProperty = ((UriResourcePrimitiveProperty)uriResource).getProperty();
    	   final String sortPropertyName = edmProperty.getName();
```

**4. Modify the EntityCollection**

The remaining work is to do the sorting.
We have a list of entities that has to be sorted, therefore we create a java.util.Comparator for Entity:

```java
    Collections.sort(entityList, new Comparator<Entity>() {
```

In the compare method, we extract the required property from the entity.
The required property is the one that we retrieved from the URI.
In our sample, the properties can be of type String or Integer, therefore we have to distinguish these 2 cases.
The actual work of comparing can then be delegated to the String and Integer classes.

```java
    if(sortPropertyName.equals("ID")){
    	Integer integer1 = (Integer) entity1.getProperty(sortPropertyName).getValue();
    	Integer integer2 = (Integer) entity2.getProperty(sortPropertyName).getValue();
    	compareResult = integer1.compareTo(integer2);
    }else{
    	String propertyValue1 = (String) entity1.getProperty(sortPropertyName).getValue();
    	String propertyValue2 = (String) entity2.getProperty(sortPropertyName).getValue();
    	compareResult = propertyValue1.compareTo(propertyValue2);
    }
```

After the sorting is done, we still have to consider, if the required order is ascending or descending.
So we have to retrieve that information from the OrderByItem and then we can simply reverse the current order accordingly:

```java
    if(orderByItem.isDescending()){
    	return - compareResult; // just convert the result to negative value to change the order
    }
```

The full implementation of the readEntityCollection() method:

```java
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
         throws ODataApplicationException, SerializerException {

    	// 1st retrieve the requested EntitySet from the uriInfo
    	List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
    	UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
    	EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

    	// 2nd: fetch the data from backend
    	EntityCollection entityCollection = storage.readEntitySetData(edmEntitySet);
    	List<Entity> entityList = entityCollection.getEntities();

    	// 3rd apply $orderby
    	OrderByOption orderByOption = uriInfo.getOrderByOption();
    	if (orderByOption != null) {
    		List<OrderByItem> orderItemList = orderByOption.getOrders();
    		final OrderByItem orderByItem = orderItemList.get(0); // we support only one
    		Expression expression = orderByItem.getExpression();
    		if(expression instanceof Member){
    			UriInfoResource resourcePath = ((Member)expression).getResourcePath();
    			UriResource uriResource = resourcePath.getUriResourceParts().get(0);
    			if (uriResource instanceof UriResourcePrimitiveProperty) {
    				EdmProperty edmProperty = ((UriResourcePrimitiveProperty)uriResource).getProperty();
    				final String sortPropertyName = edmProperty.getName();

    				// do the sorting for the list of entities  
    				Collections.sort(entityList, new Comparator<Entity>() {

    					// delegate the sorting to native sorter of Integer and String
    					public int compare(Entity entity1, Entity entity2) {
    						int compareResult = 0;

    						if(sortPropertyName.equals("ID")){
    							Integer integer1 = (Integer) entity1.getProperty(sortPropertyName).getValue();
    							Integer integer2 = (Integer) entity2.getProperty(sortPropertyName).getValue();

    							compareResult = integer1.compareTo(integer2);
    						}else{
    							String propertyValue1 = (String) entity1.getProperty(sortPropertyName).getValue();
    							String propertyValue2 = (String) entity2.getProperty(sortPropertyName).getValue();

    							compareResult = propertyValue1.compareTo(propertyValue2);
    						}

    						// if 'desc' is specified in the URI, change the order
    						if(orderByItem.isDescending()){
    							return - compareResult; // just reverse order
    						}

    						return compareResult;
    					}
    				});
    			}
    		}
    	}

    	// 4th: create a serializer based on the requested format (json)
    	ODataSerializer serializer = odata.createSerializer(responseFormat);

    	// and serialize the content: transform from the EntitySet object to InputStream
    	EdmEntityType edmEntityType = edmEntitySet.getEntityType();
    	ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).build();

        final String id = request.getRawBaseUri() + "/" + edmEntitySet.getName();
    	EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().contextURL(contextUrl).id(id).build();
    	SerializerResult serializerResult = serializer.entityCollection(serviceMetadata, edmEntityType, entityCollection, opts);
    	InputStream serializedContent = serializerResult.getContent();

    	// 5th: configure the response object: set the body, headers and status code
    	response.setContent(serializedContent);
    	response.setStatusCode(HttpStatusCode.OK.getStatusCode());
    	response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

**4. Run the implemented service**

After building and deploying your service to your server, you can try the following URLs:

- The “normal” payload without query option [http://localhost:8080/DemoService/DemoService.svc/Products](http://localhost:8080/DemoService/DemoService.svc/Products)
- Sort by Name ascending [http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Name](http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Name)
- Sort by Name descending [http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Name desc]([http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Name](http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Name) desc)
- Sort by Description ascending [http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Description](http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Description)
- Sort by Description descending [http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Description desc]([http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Description](http://localhost:8080/DemoService/DemoService.svc/Products?$orderby=Description) desc)

# 5. Summary

In this tutorial we have learned how to implement a simple `$orderby`.
We have decided to not go for the advanced way of implementing `$orderby`, which would have been using an ExpressionVisitor, because that is treated in the `$filter` implementation.

# 6. Links

### Tutorials

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: System Query Options $orderby (this page)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial ODATA V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial ODATA V4 service, part 7: [Media Entities](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.0.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Part 5.1: System Query Options `$top`, `$skip`, `$count`

## Introduction

In the present tutorial, we’ll learn how to implement **system query options**.
Query options are used to refine the result of a query.
The [OData V4 specification document](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part2-url-conventions/odata-v4.0-errata02-os-part2-url-conventions-complete.html#_Toc406398093) gives the following definition:

> “System query options are query string parameters that control the amount and order of the data returned for the resource identified by the URL. The names of all system query options are prefixed with a dollar ($) character.”

Query options are not part of the resource path, they’re appended to the URL after the `?`.
As an example the URL [http://localhost:8080/my/page?example=true](http://localhost:8080/my/page?example=true) has *example* as *query option* with the value *true*.

As an example for a system query option in Odata:
When querying the list of products, the order of the returned entries is defaulted by the OData service.
However, the user can change the order of the list by specifying the query option `$orderby`.

Examples for system query options that are commonly used:

- `$top`
- `$skip`
- `$count`
- `$select`
- `$orderby`
- `$filter`
- `$expand`

The present tutorial focuses on the first three query options: `$top`, `$skip` and `$count`

**Examples**

The following example calls are based on our sample service and illustrate the usage of these 3 query options.

First, just to remember, the “normal” query of the product without query options:
[http://localhost:8080/DemoService/DemoService.svc/Products](http://localhost:8080/DemoService/DemoService.svc/Products)

![AllProductsNoQueryOption](olingo.apache.org/doc/odata4/tutorials/sqo_tcs/responseFull.jpg "The full list of Products")

The following URL provides only the first 2 entries and ignores all the rest:
[http://localhost:8080/DemoService/DemoService.svc/Products?$top=2](http://localhost:8080/DemoService/DemoService.svc/Products?$top=2)

![ProductsWith$top](olingo.apache.org/doc/odata4/tutorials/sqo_tcs/responseTop2.jpg "The first 2 entries of the list of Products")

The following request returns the products starting with the 3rd and ignores the first 2 entries:
[http://localhost:8080/DemoService/DemoService.svc/Products?$skip=2](http://localhost:8080/DemoService/DemoService.svc/Products?$skip=2)

![ProductsWith$skip](olingo.apache.org/doc/odata4/tutorials/sqo_tcs/responseSkip2.jpg "Skipping the first 2 entries of the list of Products")

The following request returns the total number of products and includes it in the payload:
[http://localhost:8080/DemoService/DemoService.svc/Products?$count=true](http://localhost:8080/DemoService/DemoService.svc/Products?$count=true)

![ProductsWith$count](olingo.apache.org/doc/odata4/tutorials/sqo_tcs/responseCount.jpg "The full list of Products with the count added in the payload")

**Note:**
TThe final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).
This tutorial can be found in subdirectory *\samples\tutorials\p5\_queryoptions-tcs*

**Disclaimer:**
Again, in the present tutorial, we’ll focus only on the relevant implementation, in order to keep the code small and simple. The sample code shouldn’t be reused for advanced scenarios.

**Table of Contents**

1. Prerequisites
2. Preparation
3. Implementating system query options
   1. Implement `$count`
   2. Implement `$skip`
   3. Implement `$top`
4. Run the implemented service
5. Summary
6. Links

---

# 1. Prerequisites

Same prerequisites as in [Tutorial Part 1: Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
and [Tutorial Part 2: Read Entity](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep) as well as basic knowledge about the concepts presented in both tutorials.

---

# 2. Preparation

Follow [Tutorial Part 1: Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
and [Tutorial Part 2: Read Entity](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep) or as shortcut import the project attached to Tutorial Part 2 into your Eclipse workspace.

Afterwards do a *Deploy and run*: it should be working.

---

# 3. Implementing system query options

The system query options we’re focusing on are applied to the entity collection only (for example, it doesn’t make sense to apply a `$top` to a READ request of a single entity)

Therefore our implementation for all three query options is done in the class
`myservice.mynamespace.service.DemoEntityCollectionProcessor`

The general sequence of the implementation remains unchanged:

1. Analyze the URI
2. Fetch data from backend
3. Serialize
4. Configure the response

The only difference is that we apply the query options after getting the data from the backend (our database-mock).
So the procedure will be:

1. Analyze the URI
2. Fetch data from backend
3. Apply all system query options
4. Serialize
5. Configure the response

The following sections describe how such system query options are implemented.
The procedure will be similar in all 3 cases:

1. Get the query option from the UriInfo. If null is returned then nothing has to be done.
2. Get the value from the query option
3. Analyze the value
4. Modify the EntityCollection

## 3.1. Implement `$count`

**Background**
The `$count` allows users to request a count of the matching resources.
The number will be included with the resources in the response (see screenshot above).

The user specifies the `$count` as follows:
`$count=true`
`$count=false`

If the value of `$count` is set to *false*, then no number is returned, the same like if `$count` is not specified at all. However, this case has to be considered in our code as well.

Note:
For those who are used to OData V2:
In V2, the query option `$inlinecount` has now been replaced in V4 by `$count=true`.
In V2, the `/$count` that was part of the resource path, has now been removed in V4.

There’s one more important detail that we have to consider before writing the code:
`$count` always returns the original number of entities, without considering *$top* and `$skip`.
This is specified by the [OData V4 specification](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398308):

> “The `$count` system query option ignores any $top, `$skip`, or `$expand` query options, and returns the total count of results across all pages including only those results matching any specified `$filter` and $search.”

Therefore, in our sample code, the `$count` will be the first to be implemented, to make sure that the data provided by the backend is not modified at the moment when we "count" it.

**Implementation**
As in the previous tutorials, the data is fetched from the backend.
It is provided as `EntityCollection` which we can ask for the list of contained `Entity` instances.
The size of this genuine list is the relevant information for our `$count`.
Furthermore, we create a new instance of an `EntityCollection` object, which will carry the modified list of entities after applying all the query options.

```java
    EntityCollection entityCollection = storage.readEntitySetData(edmEntitySet);
    List<Entity> entityList = entityCollection.getEntities();
    EntityCollection returnEntityCollection = new EntityCollection();
```

Then we proceed with the 4 steps as described above:

1. Get the query option from the `UriInfo`. If null is returned then nothing has to be done.
2. Get the value from the query option
3. Analyze the value
4. Modify the `EntityCollection`

And this is the sample code:

```java
    CountOption countOption = uriInfo.getCountOption();
    if (countOption != null) {
        boolean isCount = countOption.getValue();
        if(isCount){
            returnEntityCollection.setCount(entityList.size());
        }
    }
```

**Note:**
We don’t need to check if the value of the `$count` is incorrect (e.g. `$count=xxx`), as this is handled by the *Olingo OData V4* library.

One additional step has to be considered:
As we know, if `$count=true` is specified, the structure of the response payload is different.
So we have to inform the serializer.that `$count` has to be considered.
So we have to modify the line of code, where the `EntityCollectionSerializerOptions` is created:

```java
    EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with()
                                             .contextURL(contextUrl)
                                             .id(id)
                                             .count(countOption)
                                             .build();
```

Furthermore, we have to change the following line, because the `EntityCollection` to be returned is now different;

```java
    SerializerResult serializerResult = serializer.entityCollection(serviceMetadata,
                                        edmEntityType,
                                        returnEntityCollection,
                                        opts);
```

## 3.2. Implement `$skip`

**Background**
With the query option `$skip`, the user of an OData service can specify the number of entries that should be ignored at the beginning of a collection.
So if a user specifies `$skip=n` then our OData service has to return the list of entries starting at position n+1

One important rule that we have to consider is described by the [OData V4 specification](http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398306):

> “Where `$top` and `$skip` are used together, `$skip` MUST be applied before $top, regardless of the order in which they appear in the request.”

This means for us that we add the code for `$skip` before the code for `$top`.

**Implementation**

Again we follow the 4 mentioned steps.
We get the `SkipOption` object from the `UriInfo`.
If the `SkipOption` is null, then it hasn’t been specified by the user.
Since it is not mandatory to specify any query option, we can ignore the case of `SkipOption` being null.
We ask the `SkipOption` object for the value that has been specified by the user.
Since the user might give invalid numbers, we have to check that and throw an exception with HTTP status as “Bad Request”.
Then we can do the actual job, which is adapting the backend-data according to the specified `$skip`.

```java
    SkipOption skipOption = uriInfo.getSkipOption();
    if (skipOption != null) {
        int skipNumber = skipOption.getValue();
        if (skipNumber >= 0) {
            if(skipNumber <= entityList.size()) {
                entityList = entityList.subList(skipNumber, entityList.size());
            } else {
                // The client skipped all entities
                entityList.clear();
            }
        } else {
            throw new ODataApplicationException("Invalid value for $skip", HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
        }
    }
```

After applying the query option, we have the desired set of entities in the variable `entityList`.
Now we have to populate the `EntityCollection` instance, that we created in the section above, with these entities, before we can pass it to the serializer:

```java
    for(Entity entity : entityList){
        returnEntityCollection.getEntities().add(entity);
    }
```

## 3.3. Implement $top

**Background**
With the query option `$top`, the user of an OData service can specify the maximum number of entries that should be returned, starting from the beginning.

**Implementation**

Again we follow the 4 mentioned steps, the code is very similar, only the logic for reducing the entityList is different:

```java
    TopOption topOption = uriInfo.getTopOption();
    if (topOption != null) {
        int topNumber = topOption.getValue();
        if (topNumber >= 0) {
            if(topNumber <= entityList.size()) {
                entityList = entityList.subList(0, topNumber);
            }  // else the client has requested more entities than available => return what we have
        } else {
            throw new ODataApplicationException("Invalid value for $top", HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
        }
    }
```

So now we can finally have a look at the full implementation of the `readEntityCollection()` method, containing all the three query options:

```java
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat)
                                    throws ODataApplicationException, SerializerException {

        // 1st retrieve the requested EntitySet from the uriInfo
        List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
        UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
        EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

        // 2nd: fetch the data from backend for this requested EntitySetName
        EntityCollection entityCollection = storage.readEntitySetData(edmEntitySet);

        // 3rd: apply System Query Options
        // modify the result set according to the query options, specified by the end user
        List<Entity> entityList = entityCollection.getEntities();
        EntityCollection returnEntityCollection = new EntityCollection();

        // handle $count: return the original number of entities, ignore $top and $skip
        CountOption countOption = uriInfo.getCountOption();
        if (countOption != null) {
            boolean isCount = countOption.getValue();
            if(isCount){
                returnEntityCollection.setCount(entityList.size());
            }
        }

        // handle $skip
        SkipOption skipOption = uriInfo.getSkipOption();
        if (skipOption != null) {
            int skipNumber = skipOption.getValue();
            if (skipNumber >= 0) {
                if(skipNumber <= entityList.size()) {
                    entityList = entityList.subList(skipNumber, entityList.size());
                } else {
                    // The client skipped all entities
                    entityList.clear();
                }
            } else {
                throw new ODataApplicationException("Invalid value for $skip", HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
            }
        }

        // handle $top
        TopOption topOption = uriInfo.getTopOption();
        if (topOption != null) {
            int topNumber = topOption.getValue();
            if (topNumber >= 0) {
                if(topNumber <= entityList.size()) {
                    entityList = entityList.subList(0, topNumber);
                }  // else the client has requested more entities than available => return what we have
            } else {
                throw new ODataApplicationException("Invalid value for $top", HttpStatusCode.BAD_REQUEST.getStatusCode(), Locale.ROOT);
            }
        }

        // after applying the query options, create EntityCollection based on the reduced list
        for(Entity entity : entityList){
            returnEntityCollection.getEntities().add(entity);
        }

        // 4th: create a serializer based on the requested format (json)
        ODataSerializer serializer = odata.createSerializer(responseFormat);

        // and serialize the content: transform from the EntitySet object to InputStream
        EdmEntityType edmEntityType = edmEntitySet.getEntityType();
        ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).build();

        final String id = request.getRawBaseUri() + "/" + edmEntitySet.getName();
        EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with()
                                                                .contextURL(contextUrl)
                                                                .id(id)
                                                                .count(countOption)
                                                                .build();
        SerializerResult serializerResult = serializer.entityCollection(serviceMetadata, edmEntityType,
                                                                        returnEntityCollection, opts);

        // 5th: configure the response object: set the body, headers and status code
        response.setContent(serializedContent);
        response.setStatusCode(HttpStatusCode.OK.getStatusCode());
        response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

---

# 4. Run the implemented service

After building and deploying your service to your server, you can try the following URLs:

- The full collection, no query option
  [http://localhost:8080/DemoService/DemoService.svc/Products](http://localhost:8080/DemoService/DemoService.svc/Products)
- First 2 products only
  [http://localhost:8080/DemoService/DemoService.svc/Products?$top=2](http://localhost:8080/DemoService/DemoService.svc/Products?$top=2)
- Exclude the first 2 products
  [http://localhost:8080/DemoService/DemoService.svc/Products?$skip=2](http://localhost:8080/DemoService/DemoService.svc/Products?$skip=2)
- Add the full number of all products to the response payload
  [http://localhost:8080/DemoService/DemoService.svc/Products?$count=true](http://localhost:8080/DemoService/DemoService.svc/Products?$count=true)
- Combine `$top` and `$skip`
  [http://localhost:8080/DemoService/DemoService.svc/Products?$skip=1&$top=1](http://localhost:8080/DemoService/DemoService.svc/Products?$skip=1&$top=1)
  [http://localhost:8080/DemoService/DemoService.svc/Products?$top=1&$skip=1](http://localhost:8080/DemoService/DemoService.svc/Products?$top=1&$skip=1)
  Regardless of the order, the result should be the same
- Combine all 3 query options
  [http://localhost:8080/DemoService/DemoService.svc/Products?$skip=1&$top=1&$count=true](http://localhost:8080/DemoService/DemoService.svc/Products?$skip=1&$top=1&$count=true)

---

# 5. Summary

In this tutorial we have learned how enhance our OData service to support system query options.
In a first step, we’ve covered `$top`, `$skip` and `$count`.
More system query options will be treated in the subsequent tutorials.

---

# 6. Links

### Tutorials

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: System Query Options $top, $skip, $count (this page)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial ODATA V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial ODATA V4 service, part 7: [Media Entities](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.0.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-streaming-tutorial_streaming"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-streaming-tutorial_streaming--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-streaming-tutorial_streaming--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-streaming-tutorial_streaming--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Add Streaming Support (for Entity Collections)

Available with *Apache Olingo 4.2.0 (and newer).*

## Preface

In the present tutorial we will add streaming support for Entity Collections on a per Entity granularity.

**Note:**
The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).  
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).  
This tutorial can be found in the `DemoService-Streaming` module in the projects subdirectory `/samples/tutorials/pe_streaming`

**Table of Contents**

1. Introduction
2. Preparation
3. Implementation
4. Run the implemented service
5. Links

# Introduction

The actual *streaming support* in the Olingo library enables a way to provide an Entity Collection on a single Entity granularity. This enables support for e.g. *chunked HTTP responses* without the need to have the whole Entity Collection pre-loaded (and probably in memory). Therefore the `EntityIterator` interface is used to check for additional entities and to provide the next available entity. The how a single Entity is provided is than completely based on the decision of the service developer.

A possible implementation then could e.g. pre-load ten entities and serve them as *chunked HTTP responses* and first with the next requested chunkes the next (ten) entities would be loaded from the database. With such an implementation the runtime memory consumption could be reduced (with the counterpart of more database round trips) and the client has the possibility to visualise the already delivered entities (if the client support this).

# Preparation

You should read the previous tutorials first to have an idea how to read entity collections. In addition the following code is based on the [read collection tutorial (Part 2)](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep).

As a shortcut for the upcoming modification steps you should checkout the mentioned tutorial project. It is available in the git repository in folder `/samples/tutorials/p2_readep` (for more information about checkout see in the [read collection tutorial (Part 2)](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)).

The main idea of the following implementation is to enable a basic streaming support in the sample *data provider* (the `Storage` class) and use this in the already existing processors.

Therefore following steps have to be performed:

- Modify the *data provider* (the `Storage` class)
  - Includes a basic implementation of the `EntityIterator` interface
- Use the `EntityIterator` in the `readEntityCollection(..)` method
- Optional: Add exception/error handling (with `ODataContentWriteErrorCallback`)

# Implementation

To enable the *streaming support* in a service there are following steps which need to be done:

1. An `EntityIterator` implementation has to be used to provide the entity collection data (`Entity` objects)
2. This `EntityIterator` has to be passed to the `entityCollectionStreamed(...)` method of the used `ODataSerializer`
3. The `ODataSerializer` than returns a `SerializerStreamResult` which contains the *stream enabled* result within a `ODataContent` object.
4. The `ODataContent` is then set at the `ODataResponse` via the `setODataContent(...)` method

Basically it is the same as in the *none streaming* with the difference that some other objects and classes has to be used.

For demonstration of above steps the existing [read collection tutorial](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep) will be now enabled for *streaming of entity collections*.

## Simplest approach

The *simplest approach* is to wrap the already existing `EntityCollection` into an `EntityIterator` and pass this to the according `entityCollectionStreamed(...)` method.
With this the service would not change how the data is accessed but would (easily) enable the possibility for a (streamed) *chunked HTTP response* (if this is supported by the environment e.g. JEE application server).

In the existing [read collection tutorial](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep) following new method is necessary to create an `EntityIterator` to wrap an `EntityCollection`:

```java
    private EntityIterator wrapAsIterator(final EntityCollection collection) {
      final Iterator<Entity> it = collection.iterator();
      return new EntityIterator() {
        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public Entity next() {
          return it.next();
        }
      };
    }
```

The (as anonymous inner class) created `EntityIterator` only iterates over the already loaded entities (of the `EntityCollection`).

To use this `EntityIterator` in the `readEntityCollection(..)` method the `EntityIterator` must be passed to the `ODataSerializer` via the `entityCollectionStreamed(...)` method and the `ODataContent` object of the resulting `SerializerStreamResult` must be set at the `ODataResponse` via the `setODataContent(...)` method.
What sound like a lot to do is just the below code snippet:

```java
      ...
      EntityIterator iterator = wrapAsIterator(entityCollection);
      SerializerStreamResult serializerResult = serializer.entityCollectionStreamed(serviceMetadata,
          edmEntityType, iterator, opts);

      // 4th: configure the response object: set the body, headers and status code
      response.setODataContent(serializerResult.getODataContent());
      ...
    }
```

Which replaces following original code snippet:

```java
      ...
      SerializerResult serializerResult = serializer.entityCollection(serviceMetadata,
          edmEntityType, entityCollection, opts);

      // 4th: configure the response object: set the body, headers and status code
      response.setContent(serializedContent);
      ...
    }
```

## DataProvider based approach

The *realistic approach* is that the data provider (e.g. a database) creates an `EntityIterator` which is used to provide the entity collection data (`Entity` objects) to the `EntityProcessor` and `ODataSerializer`.

With this approach not only the option for a (streamed) chunked HTTP response (if this is supported by the environment e.g. JEE application server) is enabled. Furthermore the data provider is in charge at which time how many entities are loaded (and hold) in memory.
This means as example, that a data provider can implement a concept of lazy loading of the entity collection in which e.g. a database connection is established but only the first ten entities are loaded in memory and passed for response serialization. First when the serializer need the eleventh (and/or more) entity those are loaded from the database (and the first ten can be removed from memory).
Practically such an approach requires more database roundtrips but also a smaller memory footprint and less eager loading at the begin of the request/response cycle.

In the existing [read collection tutorial](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep) the `Storage` class is used a data provider (acting like a database).
For enablement of the *streaming support* following new method is introduced which create an `EntityIterator` to allow the iterable passed access to the stored entities:

```java
    public EntityIterator readEntitySetDataStreamed(EdmEntitySet edmEntitySet)throws ODataApplicationException {
      // actually, this is only required if we have more than one Entity Sets
      if(edmEntitySet.getName().equals(DemoEdmProvider.ES_PRODUCTS_NAME)){
        final Iterator<Entity> it = productList.iterator();
        return new EntityIterator() {
          @Override
          public boolean hasNext() {
            return it.hasNext();
          }

          @Override
          public Entity next() {
            return it.next();
          }
        };
      }

      return null;
    }
```

As described above in the existing implementation the use of the `EntityCollection` has to be replaced with the `EntityIterator`, which means that this line:
`EntityCollection entityCollection = storage.readEntitySetData(edmEntitySet);`
has to be replaced by that line:
`EntityIterator iterator = storage.readEntitySetDataStreamed(edmEntitySet);`

And the

```java
    SerializerResult serializerResult = serializer.entityCollection(
      serviceMetadata, edmEntityType, entityCollection, opts);
```

has to be replaced by

```java
    SerializerStreamResult serializerResult = serializer.entityCollectionStreamed(
      serviceMetadata, edmEntityType, iterator, opts);
```

And at the `ODataResponse` now instead of:
`response.setContent(serializerResult.getContent());`
the result is set as `ODataContent`:
`response.setODataContent(serializerResult.getODataContent());`

As result the whole `readEntityCollection(...)` method now look like following:

```java
    public void readEntityCollection(ODataRequest request, ODataResponse response, UriInfo uriInfo, ContentType responseFormat) throws ODataApplicationException, SerializerException {

      // 1st retrieve the requested EntitySet from the uriInfo (representation of the parsed URI)
      List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
      UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0); // in our example, the first segment is the EntitySet
      EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

      // 2nd: fetch the data from backend for this requested EntitySetName and deliver as EntitySet
      EntityIterator iterator = storage.readEntitySetDataStreamed(edmEntitySet);

      // 3rd: create a serializer based on the requested format (json)
      ODataSerializer serializer = odata.createSerializer(responseFormat);

      // and serialize the content: transform from the EntitySet object to InputStream
      EdmEntityType edmEntityType = edmEntitySet.getEntityType();
      ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).build();

      final String id = request.getRawBaseUri() + "/" + edmEntitySet.getName();
      EntityCollectionSerializerOptions opts = EntityCollectionSerializerOptions.with().id(id)
              .contextURL(contextUrl).build();

      SerializerStreamResult serializerResult = serializer.entityCollectionStreamed(serviceMetadata,
          edmEntityType, iterator, opts);

      // 4th: configure the response object: set the body, headers and status code
      response.setODataContent(serializerResult.getODataContent());
      response.setStatusCode(HttpStatusCode.OK.getStatusCode());
      response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
    }
```

After this changes the data access (encapsulated in the `EntityIterator`) and serialization is now done directly when the data is processed by the web framework layer (e.g. JEE servlet layer) and not within the call hierarchy of the `readEntityCollection(...)` method.

The *counterpart* of this is that when an error/exception occurs during the serialization of the data the `readEntityCollection(...)` method already returned and hence there is no possibility (at this point) to catch the exception and do an error handling.
Furthermore because of the *streaming* the *HTTP Header* is already sent to the client (with e.g. a `HTTP Status-Code: 200 OK`).
Based on this the *OData-v4.0 Part1 Protocol* describes in chapter *9.4 In-Stream Errors* how to handle this:

> In the case that the service encounters an error after sending a success status to the client, the service MUST generate an error within the payload, which may leave the response malformed. Clients MUST treat the entire response as being in error. This specification does not prescribe a particular format for generating errors within a payload.

And for Olingo exists the `ODataContentWriteErrorCallback` which is described in the chapter *Exception/Error Handling*.

### More realistic data provider

Because the simplistic data provider in the tutorial the `EntityIterator` is also very simplistic.
However it is also realistic to have an `EntityIterator` which e.g. access a database result set which is `next():Entity` call (see below code snippet to get the idea).

```java
    public class MyEntityIterator extends EntityIterator {
      ResultSet set; //...

      public MyEntityIterator(ResultSet set) {
        this.set = set;
      }

      @Override
      public boolean hasNext() {
        return set.next();
      }

      @Override
      public Entity next() {
        return readNextEntityFromResultSet();
      }

      private Entity readNextEntityFromResultSet() {
        // read data from result set and return as entity object
      }
    }
```

## Exception/Error Handling

The *counterpart* of the *streaming support* is that when an error/exception occurs during the serialization of the data the service implementation is not in charge anymore to catch the exception and do an error handling.

Furthermore because of the *streaming* the *HTTP Header* is already sent to the client (with e.g. a `HTTP Status-Code: 200 OK`).
Based on this the *OData-v4.0 Part1 Protocol* describes in chapter *9.4 In-Stream Errors* how to handle this:

> In the case that the service encounters an error after sending a success status to the client, the service MUST generate an error within the payload, which may leave the response malformed. Clients MUST treat the entire response as being in error. This specification does not prescribe a particular format for generating errors within a payload.

For *exception/error handling* in Olingo exists the `ODataContentWriteErrorCallback` interface which must be implemented and then can be set as an option at the `EntityCollectionSerializerOptions` with the `writeContentErrorCallback(...)` method.

If during processing (*write*) of the `ODataContent` object (normally serialization into an according `OutputStream`, like the `javax.servlet.ServletOutputStream` in a JEE servlet environment) an exception occurs the `ODataContentWriteErrorCallback` `handleError` method is called.
This method get as parameter the `ODataContentWriteErrorContext` which contains at least the thrown and to be handled `Exception` and the `WritableByteChannel` in which the payload of the response was written before the error occurred.
Based on the requirements of the OData specification that *the service MUST generate an error within the payload, which may leave the response malformed* the `WritableByteChannel` is still open and the service developer can write additional data to ensure that the response payload is malformed.

A basic `ODataContentWriteErrorCallback` implementation could look like this code snippet:

```java
    private ODataContentWriteErrorCallback errorCallback = new ODataContentWriteErrorCallback() {
      public void handleError(ODataContentWriteErrorContext context, WritableByteChannel channel) {
        String message = "An error occurred with message: ";
        if(context.getException() != null) {
          message += context.getException().getMessage();
        }
        try {
          channel.write(ByteBuffer.wrap(message.getBytes()));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
```

And could be set in the [read collection tutorial](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep) at the `EntityCollectionSerializerOptions` via the `.writeContentErrorCallback(errorCallback)` method.

```java
    EntityCollectionSerializerOptions opts =
        EntityCollectionSerializerOptions.with().id(id)
            .writeContentErrorCallback(errorCallback)
            .contextURL(contextUrl).build();
```

# Run sample service

After building and deploying your service to your server, you can try a requests to the entity set via: [http://localhost:8080/DemoService/DemoService.svc/Products?$format=json](http://localhost:8080/DemoService/DemoService.svc/Products?$format=json)

The response is exactly the same response as in the none streaming request. So unfortunaly here is no difference beside of the technical fact that the response is serialized at the very end of the request chain and directly written into the response output stream (`javax.servlet.ServletOutputStream`)

```json
    {
      "@odata.context": "$metadata#Products",
      "value": [
        {
          "ID": 1,
          "Name": "Notebook Basic 15",
          "Description": "Notebook Basic, 1.7GHz - 15 XGA - 1024MB DDR2 SDRAM - 40GB"
        },
        {
          "ID": 2,
          "Name": "1UMTS PDA",
          "Description": "Ultrafast 3G UMTS/HSDPA Pocket PC, supports GSM network"
        },
        {
          "ID": 3,
          "Name": "Ergo Screen",
          "Description": "19 Optimum Resolution 1024 x 768 @ 85Hz, resolution 1280 x 960"
        }
      ]
    }
```

# Links

### Tutorials

Further topics to be covered by follow-up tutorials:

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: [Write (Create, Update, Delete Entity)](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write)
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial OData V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial OData V4 service, part 7: [Add Media entities to the service](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.0.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)

---

<a id="olingo-apache-org-doc-odata4-tutorials-write-tutorial_write"></a>

# Apache Olingo Library

Toggle navigation

![](olingo.apache.org/img/OlingoOrangeTM.png)
[Apache Olingoâ„¢](/)

- [ASF ](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write--)
  - [ASF Home](https://www.apache.org/foundation/)
  - [Projects](https://projects.apache.org/)
  - [People](https://people.apache.org/)
  - [Get Involved](https://www.apache.org/foundation/getinvolved.html)
  - [Download](https://www.apache.org/dyn/closer.cgi)
  - [Security](https://www.apache.org/security/)
  - [Support Apache](https://www.apache.org/foundation/sponsorship.html)
- [License](https://www.apache.org/licenses/)
- [Download ](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write--)
  - [Download OData 2.0 Java](/doc/odata2/download.html)
  - [Download OData 4.0 Java](#olingo-apache-org-doc-odata4-download)
  - [Download OData 4.0 JavaScript](/doc/javascript/download.html)
- [Documentation ](#olingo-apache-org-doc-odata4-tutorials-write-tutorial_write--)
  - [Documentation OData 2.0 Java](/doc/odata2/index.html)
  - [Documentation OData 4.0 Java](#olingo-apache-org-doc-odata4-index)
  - [Documentation OData 4.0 JavaScript](/doc/javascript/index.html)
- [Support](/support.html)
- [Contribute](/contribute.html)

[
![Apache Software Foundation](olingo.apache.org/img/asf_logo_url.svg)
](https://www.apache.org/foundation/)

# How to build an OData Service with Olingo V4

# Part 3: Write operations

## Introduction

This tutorial guides you through the steps required to write an OData Service based on the Olingo OData 4.0 Library for Java (based on current release which can be got via the [Download-Page](#olingo-apache-org-doc-odata4-download)).

In the first two tutorials ([Read Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read) and [Read Entity](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)), we’ve learned how to build a simple OData service that supports read operations for collection, single entity and property.

In the present tutorial, will cover the write operations, which means creating an entity, modifying an existing entity and deleting an existing entity.

**Note**
The final source code can be found in the project [git repository](https://gitbox.apache.org/repos/asf/olingo-odata4).
A detailed description how to checkout the tutorials can be found [here](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites).
This tutorial can be found in subdirectory *\samples\tutorials\p3\_write*

**Disclaimer**
Again, in the present tutorial, will focus only on the relevant implementation, in order to keep the code small and simple.
The sample code shouldn't be reused for advanced scenarios.

**Table of Contents**

1. Prerequisites
2. Preparation
3. Implementation of Read Single Entity
   1. Implement the `createEntity(...)` method
   2. Implement the `updateEntity(...)` method
   3. Implement the `deleteEntity(...)` method
4. Run the implemented service
   1. Example for **CREATE**
   2. Example for **UPDATE (PUT)**
   3. Example for **UPDATE (PATCH)**
   4. Example for **DELETE**
5. Summary
6. Links

---

# 1. Prerequisites

Same prerequisites as in [Tutorial Part 1: Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read) and [Tutorial Part 2: Read Entity](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep) as well as basic knowledge about the concepts presented in both tutorials.

---

# 2. Preparation

Follow [Tutorial Part 1: Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read) and [Tutorial Part 2: Read Entity](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep) or as shortcut import the project attached to *Tutorial Part 2* into your Eclipse workspace.

Afterwards do a *Deploy and run*: it should be working.

---

# 3. Implementation

In our sample scenario, we want to create a product, to be added to the list of available products that we maintain in our database-mock.
This product that we want to create will have a name and a description that the user of our service will specify in his HTTP request.
The Olingo library takes this user request, serializes the request body and invokes the corresponding method of our processor class.

In the previous tutorial 2, we’ve already implemented the `EntityProcessor` interface and registered our class in the servlet, but we have not written the implementation for the callback methods that are responsible for the write operations.
This is what we are going to do in the below sections.

## 3.1. Implement the createEntity(...) method

Open the class `myservice.mynamespace.service.DemoEntityProcessor`
Go to the method `createEntity(...)`
The method body should be empty, otherwise delete any content.

**Now, how to implement the method?**
Basically, we have to do the same that we did in the `readEntity(...)` method, but the other way ‘round.
In the `createEntity(...)` method, we have to retrieve the payload from the request and then write it to our mock-database.
Furthermore, we have to return the created entity in the response payload.

Again, we can divide our work into 4 steps:

1. Analyze the URI
2. Handle data in backend
3. Serialize
4. Configure the response

**In detail**

We have to keep in mind that -for creation - the URL that is executed in our example is the following:

```text
http://localhost:8080/DemoService/DemoService.svc/Products
```

It is executed as POST request and contains a request body which looks as follows:

```json
    {
      "ID":4,
      "Name":"Gamer Mouse",
      "Description":"optical mouse - gamer edition"
    }
```

**Steps**

1. In the implementation, we have to first retrieve the `EntityCollection` and `EntityType` metadata from the `UriInfo` object.
2. The next step is to create the data in our backend.
   For this purpose, we have to retrieve the data from the HTTP request payload.
   We get the payload from the `ODataRequest` instance as `InputStream`, which can then be deserialized.
   Our `Storage` class is responsible for creating the new product in the backend.
   And for returning the newly created instance.
   The reason is that our OData service has to return the newly created entity in the response body.
3. From now on the procedure is the same like in the `readEntity(...)` method
4. The only difference is the status code, that has to be set to **201 - created** in case of success

Please find below the sample code for the *createEntity()* method

```java
public void createEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
     ContentType requestFormat, ContentType responseFormat)
    throws ODataApplicationException, DeserializerException, SerializerException {

  // 1. Retrieve the entity type from the URI
  EdmEntitySet edmEntitySet = Util.getEdmEntitySet(uriInfo);
  EdmEntityType edmEntityType = edmEntitySet.getEntityType();

  // 2. create the data in backend
  // 2.1. retrieve the payload from the POST request for the entity to create and deserialize it
  InputStream requestInputStream = request.getBody();
  ODataDeserializer deserializer = this.odata.createDeserializer(requestFormat);
  DeserializerResult result = deserializer.entity(requestInputStream, edmEntityType);
  Entity requestEntity = result.getEntity();
  // 2.2 do the creation in backend, which returns the newly created entity
  Entity createdEntity = storage.createEntityData(edmEntitySet, requestEntity);

  // 3. serialize the response (we have to return the created entity)
  ContextURL contextUrl = ContextURL.with().entitySet(edmEntitySet).build();
  // expand and select currently not supported
  EntitySerializerOptions options = EntitySerializerOptions.with().contextURL(contextUrl).build();

  ODataSerializer serializer = this.odata.createSerializer(responseFormat);
  SerializerResult serializedResponse = serializer.entity(serviceMetadata, edmEntityType, createdEntity, options);

  //4. configure the response object
  response.setContent(serializedResponse.getContent());
  response.setStatusCode(HttpStatusCode.CREATED.getStatusCode());
  response.setHeader(HttpHeader.CONTENT_TYPE, responseFormat.toContentTypeString());
}
```

## 3.2. Implement the updateEntity(...) method

Example URL

```text
http://localhost:8080/DemoService/DemoService.svc/Products(3)
```

Example request body:

```json
    {
      "ID":3,
      "Name":"Ergo Screen updated Name",
      "Description":"updated description"
    }
```

The `updateEntity(...)` method is similar.
Again, we have to retrieve the payload from the HTTP request and use it for modifying the data in backend.
The difference is that case of update operation, the OData service is not expected to return any response payload. So we can skip the serialize-step and simply set the HTTP status code to **204 – no content**

```java
public void updateEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo,
  ContentType requestFormat, ContentType responseFormat)
    throws ODataApplicationException, DeserializerException, SerializerException {

  // 1. Retrieve the entity set which belongs to the requested entity
  List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
  // Note: only in our example we can assume that the first segment is the EntitySet
  UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
  EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();
  EdmEntityType edmEntityType = edmEntitySet.getEntityType();

  // 2. update the data in backend
  // 2.1. retrieve the payload from the PUT request for the entity to be updated
  InputStream requestInputStream = request.getBody();
  ODataDeserializer deserializer = this.odata.createDeserializer(requestFormat);
  DeserializerResult result = deserializer.entity(requestInputStream, edmEntityType);
  Entity requestEntity = result.getEntity();
  // 2.2 do the modification in backend
  List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
  // Note that this updateEntity()-method is invoked for both PUT or PATCH operations
  HttpMethod httpMethod = request.getMethod();
  storage.updateEntityData(edmEntitySet, keyPredicates, requestEntity, httpMethod);

  //3. configure the response object
  response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
}
```

In case of update, we have to consider the following:
The update of an entity can be realized in 2 ways: either a **PATCH** or a **PUT** request.
(See the online specification in section [11.4.3 Update an Entity](http://docs.oasis-open.org/odata/odata/v4.0/odata-v4.0-part1-protocol.html) for more details.
For both HTTP methods, our `updateEntity(...)` will be invoked.
But we have to treat the data-modification differently.
Therefore, we have to first retrieve the used HTTP method and in the backend-logic, we have to distinguish between **PATCH** and **PUT**.
The difference becomes relevant only in case if the user doesn’t send all the properties in the request body.

Example: if we modify the above example request body to look as follows:

```json
    {
      "Description":"updated description"
    }
```

Note that in this case, only one of three properties is sent in the request body.

- If the HTTP method is **PATCH**:
  The value of the *Description* property is updated in the backend.
  The values of the other properties remain untouched.
- If the HTTP method is **PUT**:
  The value of the *Description* property is updated in the backend.
  The value of the other properties is set to null (exception: key properties can never be null).

So let’s have a look at our sample implementation in the `Storage` class (see below for full sample code and also see the attached zip file containing the whole sample project)

```java
private void updateProduct(EdmEntityType edmEntityType, List<UriParameter> keyParams, Entity entity, HttpMethod httpMethod)
                            throws ODataApplicationException{

  Entity productEntity = getProduct(edmEntityType, keyParams);
  if(productEntity == null){
    throw new ODataApplicationException("Entity not found",
                        HttpStatusCode.NOT_FOUND.getStatusCode(), Locale.ENGLISH);
  }

  // loop over all properties and replace the values with the values of the given payload
  // Note: ignoring ComplexType, as we don't have it in our odata model
  List<Property> existingProperties = productEntity.getProperties();
  for(Property existingProp : existingProperties){
    String propName = existingProp.getName();

    // ignore the key properties, they aren't updateable
    if(isKey(edmEntityType, propName)){
      continue;
    }

    Property updateProperty = entity.getProperty(propName);
    // the request payload might not consider ALL properties, so it can be null
    if(updateProperty == null){
      // if a property has NOT been added to the request payload
      // depending on the HttpMethod, our behavior is different
      if(httpMethod.equals(HttpMethod.PATCH)){
        // in case of PATCH, the existing property is not touched
        continue; // do nothing
      }else if(httpMethod.equals(HttpMethod.PUT)){
        // in case of PUT, the existing property is set to null
        existingProp.setValue(existingProp.getValueType(), null);
        continue;
      }
    }

    // change the value of the properties
    existingProp.setValue(existingProp.getValueType(), updateProperty.getValue());
  }
}
```

## 3.3. Implement the deleteEntity(...) method

In case of **DELETE** operation, the URL is the same like for the **GET** operation, but the request body is empty.

Example URL:

```text
http://localhost:8080/DemoService/DemoService.svc/Products(3)
```

The implementation is rather simple:

- As usual, determine the entity set.
- Delete the data in backend.
- Configure the response object with the proper status code **204 – no content**.

  ```java
  public void deleteEntity(ODataRequest request, ODataResponse response, UriInfo uriInfo)
                          throws ODataApplicationException {

    // 1. Retrieve the entity set which belongs to the requested entity
    List<UriResource> resourcePaths = uriInfo.getUriResourceParts();
    // Note: only in our example we can assume that the first segment is the EntitySet
    UriResourceEntitySet uriResourceEntitySet = (UriResourceEntitySet) resourcePaths.get(0);
    EdmEntitySet edmEntitySet = uriResourceEntitySet.getEntitySet();

    // 2. delete the data in backend
    List<UriParameter> keyPredicates = uriResourceEntitySet.getKeyPredicates();
    storage.deleteEntityData(edmEntitySet, keyPredicates);

    //3. configure the response object
    response.setStatusCode(HttpStatusCode.NO_CONTENT.getStatusCode());
  }
  ```

# 4. Run the service

After building and deploying the project, we can invoke our OData service.

In order to test the write operations of our OData service, we need a tool that is able to execute the following required HTTP requests:

- **POST**
- **PUT**
- **PATCH**
- **DELETE**

This is usually done with any REST client tool that can be installed into the browser of your choice.

Some *REST* clients which are available as browser extension for:

- Firefox: “RESTClient, a debugger for RESTful web services”
- Chrome: “Advanced REST client”

The following sections provide examples for executing the requests:

### 4.1. Example for **CREATE**:

- URL: [http://localhost:8080/DemoService/DemoService.svc/Products](http://localhost:8080/DemoService/DemoService.svc/Products)
- HTTP verb: **POST**
- Header: `Content-Type: application/json; odata.metadata=minimal`
- Request body:

  ```json
      {
        "ID":6,
        "Name":"Gamer Mouse",
        "Description":"optical mouse - gamer edition"
      }
  ```

**Note:** The value for the ID property is arbitrary, as it will be generated by our OData service implementation

### 4.2. Example for UPDATE (PUT):

- URL: [http://localhost:8080/DemoService/DemoService.svc/Products(3)](http://localhost:8080/DemoService/DemoService.svc/Products(3))
- HTTP verb: **PUT**
- Header: `Content-Type: application/json; odata.metadata=minimal`
- Request body:

  ```json
      {
        "ID":3,
        "Name":"Ergo Screen updated Name",
        "Description":"updated description"
      }
  ```

### 4.3. Example for UPDATE (PATCH):

- URL: [http://localhost:8080/DemoService/DemoService.svc/Products(3)](http://localhost:8080/DemoService/DemoService.svc/Products(3))
- HTTP verb: **PATCH**
- Header: `Content-Type: application/json; odata.metadata=minimal`
- Request body:

  ```json
      {
        "Description": "patched description"
      }
  ```

### 4.4. Example for DELETE:

- URL: [http://localhost:8080/DemoService/DemoService.svc/Products(3)](http://localhost:8080/DemoService/DemoService.svc/Products(3))
- HTTP verb: **DELETE**
- Header: Content-Type: application/json; odata.metadata=minimal
- Request body: `<empty>`

---

# 5. Summary

In this tutorial we have learned how to implement the creation, update and deletion of an entity.
It has been based on a simple OData model, focusing on simple sample code and sample data.

In the next tutorial (Part 4: Navigation) we will learn how to implement navigation, i.e. the linking of resources.

---

# 6. Links

### Tutorials

- Tutorial OData V4 service part 1: [Read Entity Collection](#olingo-apache-org-doc-odata4-tutorials-read-tutorial_read)
- Tutorial OData V4 service part 2: [Read Entity, Read Property](#olingo-apache-org-doc-odata4-tutorials-readep-tutorial_readep)
- Tutorial OData V4 service part 3: Write (Create, Update, Delete Entity
- Tutorial OData V4 service, part 4: [Navigation](#olingo-apache-org-doc-odata4-tutorials-navigation-tutorial_navigation)
- Tutorial OData V4 service, part 5.1: [System Query Options $top, $skip, $count (this page)](#olingo-apache-org-doc-odata4-tutorials-sqo_tcs-tutorial_sqo_tcs)
- Tutorial OData V4 service, part 5.2: [System Query Options $select, $expand](#olingo-apache-org-doc-odata4-tutorials-sqo_es-tutorial_sqo_es)
- Tutorial OData V4 service, part 5.3: [System Query Options $orderby](#olingo-apache-org-doc-odata4-tutorials-sqo_o-tutorial_sqo_o)
- Tutorial OData V4 service, part 5.4: [System Query Options $filter](#olingo-apache-org-doc-odata4-tutorials-sqo_f-tutorial_sqo_f)
- Tutorial ODATA V4 service, part 6: [Action and Function Imports](#olingo-apache-org-doc-odata4-tutorials-action-tutorial_action)
- Tutorial ODATA V4 service, part 7: [Media Entities](#olingo-apache-org-doc-odata4-tutorials-media-tutorial_media)
- Tutorial OData V4 service, part 8: [Batch Request support](#olingo-apache-org-doc-odata4-tutorials-batch-tutorial_batch)
- Tutorial OData V4 service, part 9: [Handling "Deep Insert" requests](#olingo-apache-org-doc-odata4-tutorials-deep_insert-tutorial_deep_insert)

### Code and Repository

- [Git Repository](https://gitbox.apache.org/repos/asf/olingo-odata4)
- [Guide - To fetch the tutorial sources](#olingo-apache-org-doc-odata4-tutorials-prerequisites-prerequisites)
- [Demo Service source code as zip file (contains all tutorials)](http://www.apache.org/dyn/closer.lua/olingo/odata4/4.6.0/DemoService_Tutorial.zip)

### Further reading

- [Official OData Homepage](http://odata.org/)
- [OData documentation](http://www.odata.org/documentation/)
- [Olingo Javadoc](/javadoc/odata4/index.html)

Copyright Â© 2013-2025, The Apache Software Foundation  
Apache Olingo, Olingo, Apache, the Apache feather, and
the Apache Olingo project logo are trademarks of the Apache Software
Foundation.

[Privacy](/doc/odata2/privacy.html)