<a id="openwebbeans-apache-org-documentation"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-documentation--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

<a id="openwebbeans-apache-org-documentation--module-overview"></a>

# Getting Started[¶](#openwebbeans-apache-org-documentation--getting-started "Permalink")

If you are completely new to CDI then you might want to take a look at the following introductory articles

- [How CDI works](#openwebbeans-apache-org-cdi_explained)
- [Adding OpenWebBeans to your JavaSE project](#openwebbeans-apache-org-owbsetup_se)
- [Adding OpenWebBeans to Apache Tomcat](#openwebbeans-apache-org-owbsetup_tomcat)
- [Adding OpenWebBeans to your Servlet Container project](#openwebbeans-apache-org-owbsetup_ee)
- [OpenWebBeans as part of JavaEE Containers](#openwebbeans-apache-org-owb-eecontainers)
- [OpenWebBeans configuration](#openwebbeans-apache-org-owbconfig)
- [FAQ](#openwebbeans-apache-org-faq)

There are several Application Containers which come with Apache OpenWebBeans as their core CDI container

- [Apache Meecrowave](#openwebbeans-apache-org-meecrowave-index)
- [Apache TomEE](https://tomee.apache.org)

# OpenWebBeans Plugin Structure[¶](#openwebbeans-apache-org-documentation--openwebbeans-plugin-structure "Permalink")

OpenWebBeans consists of a core system which heavily uses SPIs (Service Provider Interfaces)
to extend it's functionality. The core system itself is purely JavaSE based
and does not need any further dependency. All special JavaEE features get added via
separate plugins.

### System Core[¶](#openwebbeans-apache-org-documentation--system-core "Permalink")

- [OpenWebBeans Core](#openwebbeans-apache-org-openwebbeans-impl)
- [SPI definition](#openwebbeans-apache-org-openwebbeans-spi)

### Commonly used Plugins[¶](#openwebbeans-apache-org-documentation--commonly-used-plugins "Permalink")

- [Web plugin](#openwebbeans-apache-org-openwebbeans-web)
- [EL plugins 1.0 & 2.2](#openwebbeans-apache-org-openwebbeans-el)
- [JSF plugins 1.2 & 2.x](#openwebbeans-apache-org-openwebbeans-jsf)
- [Apache Tomcat plugins](#openwebbeans-apache-org-openwebbeans-tomcat)

### Technical Integration Plugins[¶](#openwebbeans-apache-org-documentation--technical-integration-plugins "Permalink")

- [EE Common plugin](#openwebbeans-apache-org-openwebbeans-ee-common)
- [Java EE plugin](#openwebbeans-apache-org-openwebbeans-ee)
- [EJB plugin](#openwebbeans-apache-org-openwebbeans-ejb)
- [EE Resource plugin](#openwebbeans-apache-org-openwebbeans-resource)
- [JMS plugin](#openwebbeans-apache-org-openwebbeans-jms)
- [OSGi plugin](#openwebbeans-apache-org-openwebbeans-osgi)

# Testing Strategies for CDI Projects[¶](#openwebbeans-apache-org-documentation--testing-strategies-for-cdi-projects "Permalink")

- [JUnit5 integration](#openwebbeans-apache-org-openwebbeans-junit5)
- [General guidelines for testing](#openwebbeans-apache-org-testing_general)
- [Deltaspike Test-Control](#openwebbeans-apache-org-testing_test-control)
- [Apache DeltaSpike CdiCtrl](#openwebbeans-apache-org-testing_cdictrl)
- [JBoss Arquillian](#openwebbeans-apache-org-testing_arquillian)
- [Writing unit tests for OWB itself](#openwebbeans-apache-org-owbinternalunittests)

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-cdi_explained"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-cdi_explained--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# What can OpenWebBeans as CDI container do for you?[¶](#openwebbeans-apache-org-cdi_explained--what-can-openwebbeans-as-cdi-container-do-for-you "Permalink")

## An introduction to CDI[¶](#openwebbeans-apache-org-cdi_explained--an-introduction-to-cdi "Permalink")

Contexts and Dependency Injection for Java a.k.a. CDI is a JavaEE specification with the number
JSR-299 (CDI-1.0), JSR-346 (CDI-1.1/1.2) and JSR-365 (CDI-2.0).
Apache OpenWebBeans implements these standards. This page will give you an introduction to features of
CDI in general. We will add a special hint whenever a feature is ambiguous in the specification
and OpenWebBeans implements it in a certain way which might be different on other CDI containers.

## What is CDI at all?[¶](#openwebbeans-apache-org-cdi_explained--what-is-cdi-at-all "Permalink")

Originally developed under the name ‘Web Beans’, the CDI specification was created
to fill the gaps which could not be filled by Enterprise Java Beans (EJB) on the back end,
and JavaServer Faces (JSF) in the view layer. The first draft targeted only
Java Enterprise Edition (Java EE), but during the creation of the specification
it became clear that most features were very useful for any Java environment,
including Java SE.

Even if the full name of the specification is

> Contexts and Dependency Injection for the Java Enterprise platform

this does not mean that CDI applications can only run in JavaEE those days.
At least OpenWebBeans provides CDI functionality which also runs in pure Java SE apps like Swing, JavaFX
and even Eclipse RCP apps as well.

### Relationship to JSR-330[¶](#openwebbeans-apache-org-cdi_explained--relationship-to-jsr-330 "Permalink")

Half a year before the CDI specification became final, other communities
(Spring and guice) had begun an effort to specify the basics of injection as

> “JSR-330: Dependency Injection for Java” (nicknamed “AtInject”).

Considering that it did not make sense to provide a new dependency injection container
without collaborating on the actual injection API, the AtInject and CDI expert groups
worked closely together to ensure a common solution across dependency injection frameworks.

As a result, CDI uses the annotations from the AtInject specification,
meaning that every CDI implementation fully implements the AtInject specification,
just like Guice and Spring.

CDI and AtInject are both included in Java Enterprise Edition 6 (JSR-316)
and thus nowadays an integral part of almost every Java Enterprise Edition server.

## CDI features[¶](#openwebbeans-apache-org-cdi_explained--cdi-features "Permalink")

Before we go on to dive into some code, let’s take a quick look at some key CDI features:

- **Type Safety**: Instead of injecting objects by a (string) name,
  CDI uses the Java type to resolve injections. When the type is not sufficient,
  a Qualifier annotation can be used. This allows the compiler to easily detect errors,
  and provides easy refactoring.
- **POJOs**: Almost every Java object can be injected by CDI!
  This includes EJBs, JNDI resources, Persistence Units and Persistence Contexts,
  as well as any object which previously would have been created by a factory method.
- **Extensibility**: Every CDI container can enhance its functionality
  by using “Portable Extensions”. The attribute “portable” means that
  those CDI Extensions can run on every CDI container and Java EE 6 server,
  no matter which vendor. This is accomplished by a well-specified
  SPI (Service Provider Interface) which is part of the JSR-299 specification.
- **Interceptors**: It has never been easier to write your own Interceptors.
  Because of the portable behaviour of CDI, they now also run on every
  EE 6 or later certified server and on all standalone CDI containers.
- **Decorators**: These allow to dynamically extend existing interface
  implementations with business aspects.
- **Events**: CDI specifies a type-safe mechanism to send
  and receive events with loose coupling.
- **Unified EL integration**: EL-2.2 opens a new horizon
  in regard of flexibility and functionality.
  CDI provides out-of-the-box support for it!

## A small CDI Example[¶](#openwebbeans-apache-org-cdi_explained--a-small-cdi-example "Permalink")

The following small JSF and CDI sample application allows
you to send eMails via a web form.
We will only show code fragments, please checkout our samples form our
[Source Code](#openwebbeans-apache-org-source) for more information.

#### The 'Backend'[¶](#openwebbeans-apache-org-cdi_explained--the-backend "Permalink")

For our mail application, we need an “application-scoped” MailService.
Application-scoped objects are essentially singletons –
the container will ensure you always get the same single instance whenever
you inject it into your application.

The `replyTo` address is taken from the ConfigurationService
which is also application-scoped. Here we see our first injection.
The “configuration” field is not set by the application’s code,
but is injected by CDI.
The `@Inject` annotation tells CDI to perform the injection.

```java
@ApplicationScoped
public class MyMailService implements MailService {
    private @Inject ConfigurationService configuration;

    public send(String from, String to, String body) {
        String replyTo = configuration.getReplyToAddress();
        ... // send the email
    }
}
```

#### The User handling[¶](#openwebbeans-apache-org-cdi_explained--the-user-handling "Permalink")

Our application also knows the currently loggedin user which we like to
store in the Servlet Session (the login itself is not part of this sample,
just consider this is done via your container or a
login page + Servlet Filter upfront).

CDI provides the session scope, which ensures that you will get
the same instance of an object per HTTP Session (in a web application).

```java
@SessionScoped
@Named
public class User {
    public String getName() {..}
    public String getEmail() {..}
    ..
}
```

By default, CDI beans are not available for use in JSF via the
Unified Expression Language. In order to expose it for use by JSF and EL,
we simply added the `@Named` annotation.

#### The Backing Bean[¶](#openwebbeans-apache-org-cdi_explained--the-backing-bean "Permalink")

Our web page is implemented with JSF-2. Thus we need a backing bean.
We use a `@RequestScoped` backing bean which means that every request
will get a new instance of it. Otoh, during the request you will always get
the same instance.

```java
@RequestScoped
@Named
public class MailForm {
    private @Inject MailService mailService;
    private @Inject User user;

    private String text; // + getter and setter
    private String recipient; // + getter and setter 

    public String sendMail() {
        mailService.send(user.getName(), recipient, text);
        return "messageSent"; // forward to 'message sent' JSF2 page
    }

}
```

#### The JSF-2 page[¶](#openwebbeans-apache-org-cdi_explained--the-jsf-2-page "Permalink")

We now only miss the JSF page itself to make our example work.
The page is implemented using *facelets*.

```text
...
<h:form>
    <h:outputLabel value="Username" for="username"/>
    <h:outputText id="username" value="#{user.name}"/><br/>
    <h:outputLabel value="Recipient" for="recipient"/>
    <h:inputText id="recipient" value="#{mailForm.recipient}"/><br/>
    <h:outputLabel value="Body" for="body"/>
    <h:inputText id="body" value="#{mailForm.body}"/><br/>
    
    <h:commandButton value="Send" action="#{mailForm.send}"/>
</h:form>
...
```

This page uses the backing bean from above via it's EL name *mailForm*.
In the same way it uses the `@SessionScoped` *user*.

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-community"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-community--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# Users[¶](#openwebbeans-apache-org-community--users "Permalink")

If you are a new user and you would like to start using Apache OpenWebBeans,
you can have a look at the [Documentation](#openwebbeans-apache-org-documentation) and
[subscribe](mailto:user-subscribe@openwebbeans.apache.org)
our [mailing list for user](mailto:user@openwebbeans.apache.org).

Furthermore, you can check our [mail-archives](#openwebbeans-apache-org-community--mailing-lists).

Before you file a ticket in our [Issue Tracker](https://issues.apache.org/jira/browse/OWB), please ask on the mailing list
if it's a known issue in case of a bug or if there is an ongoing discussion in case of a feature.

You are very welcome to follow our twitter account [@Apache OpenWebBeansTeam](https://twitter.com/OwbTeam)
and spread the word of Apache OpenWebBeans with Tweets, Blog-Entries,...

# CDI-4.0 (JakartaEE 10)[¶](#openwebbeans-apache-org-community--cdi-40-jakartaee-10 "Permalink")

The work on a implementing the [Jakarta CDI-4.0](https://projects.eclipse.org/projects/ee4j.cdi/releases/4.0)
specification is finished.
We are now doing minor enhancements and bug fixing in our main branch.

# Getting Involved[¶](#openwebbeans-apache-org-community--getting-involved "Permalink")

Everybody is welcome to get involved with our community. You can find general information at
[https://apache.org/foundation/getinvolved.html](https://apache.org/foundation/getinvolved.html)
and [http://apache.org/foundation/how-it-works.html](http://apache.org/foundation/how-it-works.html).
The following sections provides some details about the different levels of getting involved.

## Contributors[¶](#openwebbeans-apache-org-community--contributors "Permalink")

Before you get a committer you have to contribute to our effort. E.g. you can help users, participate in
discussions on the dev list, submit patches,... . Therefore, it's essential to file a/n
[(I)CLA](https://www.apache.org/licenses/icla.txt) or [CLA](https://www.apache.org/licenses/cla-corporate.txt)
and send it to secretary at apache dot org (or fax it) as early as possible.

If you would like to submit a patch through Jira, just attach your patch to a created JIRA issue.

## Committers[¶](#openwebbeans-apache-org-community--committers "Permalink")

Before you read this section, please ensure that you have read the contributor section.
All of you are welcome to join our development effort. [Subscribe](mailto:dev-subscribe@openwebbeans.apache.org)
our [mailing list for developers](mailto:dev@openwebbeans.apache.org) and start contributing and help users.

Optionally [subscribe](mailto:commits-subscribe@openwebbeans.apache.org) our [mailing list for commits](mailto:commits@openwebbeans.apache.org).

Furthermore, you can check our [mail-archives](#openwebbeans-apache-org-community--mailing-lists).

Further details are available at [https://www.apache.org/dev/](https://www.apache.org/dev/).

If you are running an Apache OpenWebBeans release then please make sure to read our [Release Checklist](#openwebbeans-apache-org-release-checklist)

# Mailing lists[¶](#openwebbeans-apache-org-community--mailing-lists "Permalink")

| List (Address) | Subscribe | Unsubscribe | Archive | Mirrors |  |
| --- | --- | --- | --- | --- | --- |
| User List | Subscribe | Unsubscribe | Archive | MarkMail |  |
| Developer List | Subscribe | Unsubscribe | Archive | MarkMail |  |
| Committer List | Subscribe | Unsubscribe | Archive | MarkMail |  |

# Issue Tracking[¶](#openwebbeans-apache-org-community--issue-tracking "Permalink")

Bug reports and feature requests are handled via JIRA at

[OpenWebBeans JIRA](https://issues.apache.org/jira/browse/OWB).

# Spread the word[¶](#openwebbeans-apache-org-community--spread-the-word "Permalink")

You are very welcome e.g. to write blog entries, tweet (#OwbTeam) about the project
or just follow our twitter account ([@OwbTeam](https://twitter.com/OwbTeam)), ...

```text
//with the irssi command-line client:
$ irssi

> /connect irc.freenode.net
> /join #openwebbeans
```

# Original Project Proposal[¶](#openwebbeans-apache-org-community--original-project-proposal "Permalink")

This is the original project proposal for the incubation of the Apache OpenWebBeans Project sent to the Apache Incubator by Gurkan Erdogdu. Although old (September 2009), it is still worth reading as the original vision of the project. It's a piece of history.

[OpenWebBeans Original Project Proposal](https://wiki.apache.org/incubator/OpenWebBeansProposal).

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-download"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-download--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# Apache OpenWebBeans Releases[¶](#openwebbeans-apache-org-download--apache-openwebbeans-releases "Permalink")

This page contains download links to the latest Apache OpenWebBeans releases.

All maven artifacts are available in the Maven.Central repository with the groupId `org.apache.openwebbeans`. The dependencies you can use are listed at the bottom of this page: [Maven Dependencies](#openwebbeans-apache-org-download--maven-dep).

## KEYS for verifying Apache releases[¶](#openwebbeans-apache-org-download--keys-for-verifying-apache-releases "Permalink")

The GPG keys in the [OpenWebBeans KEYS file](https://www.apache.org/dist/openwebbeans/KEYS) to validate our releases.
Read more about [How to verify downloaded files](https://www.apache.org/info/verification.html)

---

## OWB-4.0.x[¶](#openwebbeans-apache-org-download--owb-40x "Permalink")

OWB-4.0.x implements the CDI-4.0 (Jakarta CDI) specification.
It requires Java11 or higher.

#### Source[¶](#openwebbeans-apache-org-download--source "Permalink")

The source distribution contains all OpenWebBeans source code.
Binaries are available via the Apache Maven Central repository.

- [openwebbeans-4.0.3-source-release.zip](https://www.apache.org/dyn/closer.lua/openwebbeans/4.0.3/openwebbeans-4.0.3-source-release.zip)
- [openwebbeans-4.0.3-source-release.zip.sha512](https://www.apache.org/dist/openwebbeans/4.0.3/openwebbeans-4.0.3-source-release.zip.sha512)
- [openwebbeans-4.0.3-source-release.zip.asc](https://www.apache.org/dist/openwebbeans/4.0.3/openwebbeans-4.0.3-source-release.zip.asc)

---

## OWB-2.0.x[¶](#openwebbeans-apache-org-download--owb-20x "Permalink")

OWB-2.0.x implements the CDI-2.0 (JSR-365) specification.
It uses a shaded version of ASM-8 (Java11 support) for building our proxies and requires JavaSE 8 as minimum version.

#### Source[¶](#openwebbeans-apache-org-download--source_1 "Permalink")

The source distribution contains all OpenWebBeans source code.
Binaries are available via the Apache Maven Central repository.

- [openwebbeans-2.0.27-source-release.zip](https://www.apache.org/dyn/closer.lua/openwebbeans/2.0.27/openwebbeans-2.0.27-source-release.zip)
- [openwebbeans-2.0.27-source-release.zip.sha512](https://www.apache.org/dist/openwebbeans/2.0.27/openwebbeans-2.0.27-source-release.zip.sha512)
- [openwebbeans-2.0.27-source-release.zip.asc](https://www.apache.org/dist/openwebbeans/2.0.27/openwebbeans-2.0.27-source-release.zip.asc)

---

## OWB-1.7.x[¶](#openwebbeans-apache-org-download--owb-17x "Permalink")

OWB-1.7.x implements the full CDI-1.2 specification.
It uses a shaded version of ASM-5 for building our proxies and needs JavaSE 7 as minimum version.

#### Source[¶](#openwebbeans-apache-org-download--source_2 "Permalink")

Should you want to build any of the above binaries, this source bundle is the right one and covers them all.

- [openwebbeans-1.7.6-source-release.zip](https://www.apache.org/dyn/closer.lua/openwebbeans/1.7.6/openwebbeans-1.7.6-source-release.zip)
- [openwebbeans-1.7.6-source-release.zip.sha1](https://www.apache.org/dist/openwebbeans/1.7.6/openwebbeans-1.7.6-source-release.zip.sha1)
- [openwebbeans-1.7.6-source-release.zip.asc](https://www.apache.org/dist/openwebbeans/1.7.6/openwebbeans-1.7.6-source-release.zip.asc)

---

## OWB-1.2.x[¶](#openwebbeans-apache-org-download--owb-12x "Permalink")

OWB-1.2.x implements the CDI-1.0 specification and internally already CDI-1.1.
It uses a shaded version of ASM-5 for building our proxies and needs JavaSE 5 as minimum version.

#### Binaries[¶](#openwebbeans-apache-org-download--binaries "Permalink")

The binary distribution contains all OpenWebBeans modules.

- [openwebbeans-distribution-1.2.8-binary.zip](https://www.apache.org/dyn/closer.lua/openwebbeans/1.2.8/openwebbeans-distribution-1.2.8-binary.zip)
- [openwebbeans-distribution-1.2.8-binary.zip.sha1](https://www.apache.org/dist/openwebbeans/1.2.8/openwebbeans-distribution-1.2.8-binary.zip.sha1)
- [openwebbeans-distribution-1.2.8-binary.zip.asc](https://www.apache.org/dist/openwebbeans/1.2.8/openwebbeans-distribution-1.2.8-binary.zip.asc)
- [openwebbeans-distribution-1.2.8-binary.tar.gz](https://www.apache.org/dyn/closer.lua/openwebbeans/1.2.8/openwebbeans-distribution-1.2.8-binary.tar.gz)
- [openwebbeans-distribution-1.2.8-binary.tar.gz.sha1](https://www.apache.org/dist/openwebbeans/1.2.8/openwebbeans-distribution-1.2.8-binary.tar.gz.sha1)
- [openwebbeans-distribution-1.2.8-binary.tar.gz.asc](https://www.apache.org/dist/openwebbeans/1.2.8/openwebbeans-distribution-1.2.8-binary.tar.gz.asc)

**Hint:** OpenWeBeans has dependencies to several other jars and just adding our jars manually would lead to ClassNotFoundException if you choose not to use maven.
The jars you need depends on what modules you include.
They are all contained in the binary distribution.
We will try to add complete lists of this in the future, meanwhile please ask on the list or maybe look at the pom.xml for the modules you want to use.

#### Source[¶](#openwebbeans-apache-org-download--source_3 "Permalink")

Should you want to build any of the above binaries, this source bundle is the right one covers them all.

- [openwebbeans-1.2.8-source-release.zip](https://www.apache.org/dyn/closer.lua/openwebbeans/1.2.8/openwebbeans-1.2.8-source-release.zip)
- [openwebbeans-1.2.8-source-release.zip.sha1](https://www.apache.org/dist/openwebbeans/1.2.8/openwebbeans-1.2.8-source-release.zip.sha1)
- [openwebbeans-1.2.8-source-release.zip.asc](https://www.apache.org/dist/openwebbeans/1.2.8/openwebbeans-1.2.8-source-release.zip.asc)

---

## OpenWebBeans Archives[¶](#openwebbeans-apache-org-download--openwebbeans-archives "Permalink")

Older versions of Apache OpenWebBeans can be found in our [archives](https://archive.apache.org/dist/openwebbeans/)

---

### Maven Dependencies[¶](#openwebbeans-apache-org-download--maven-dep "Permalink")

#### APIs for OWB-2.0.x[¶](#openwebbeans-apache-org-download--apis-version "Permalink")

```xml
<dependency>
    <groupId>org.apache.geronimo.specs</groupId>
    <artifactId>geronimo-atinject_1.0_spec</artifactId>
    <version>1.2</version>
</dependency>

<dependency>
    <groupId>org.apache.geronimo.specs</groupId>
    <artifactId>geronimo-jcdi_2.0_spec</artifactId>
    <version>1.2</version>
</dependency>

<dependency>
    <groupId>org.apache.geronimo.specs</groupId>
    <artifactId>geronimo-interceptor_1.2_spec</artifactId>
    <version>1.2</version>
</dependency>

<dependency>
    <groupId>org.apache.geronimo.specs</groupId>
    <artifactId>geronimo-annotation_1.3_spec</artifactId>
    <version>1.3</version>
</dependency>
```

Note that you should set the seope of those dependencies to either `provided` or `compile` depending on whether your environment already provide them or not.

#### APIs for OWB-1.7.x[¶](#openwebbeans-apache-org-download--apis-version12 "Permalink")

```xml
<dependency>
    <groupId>org.apache.geronimo.specs</groupId>
    <artifactId>geronimo-atinject_1.0_spec</artifactId>
    <version>1.0</version>
</dependency>

<dependency>
    <groupId>org.apache.geronimo.specs</groupId>
    <artifactId>geronimo-jcdi_1.1_spec</artifactId>
    <version>1.0</version>
</dependency>

<dependency>
    <groupId>org.apache.geronimo.specs</groupId>
    <artifactId>geronimo-interceptor_1.2_spec</artifactId>
    <version>1.0</version>
</dependency>

<dependency>
    <groupId>org.apache.geronimo.specs</groupId>
    <artifactId>geronimo-annotation_1.2_spec</artifactId>
    <version>1.0</version>
</dependency>
```

#### Required[¶](#openwebbeans-apache-org-download--required-version "Permalink")

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-spi</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>

<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-impl</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

#### Plugins[¶](#openwebbeans-apache-org-download--plugins-version "Permalink")

**Web Module** (Required for web-apps)

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-web</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

**JSF 2.X Module**

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-jsf</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

**EL 2.2 Module**

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-el22</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

**Tomcat 7 Module**

(also works for Tomcat-8, Tomcat-8.5 and Tomcat-9)

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-tomcat7</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

**JMS Module**

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-jms</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

**Arquillian Module**

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-arquillian</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-faq"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-faq--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans FAQ[¶](#openwebbeans-apache-org-faq--openwebbeans-faq "Permalink")

- [Does OWB differ from the CDI-2.0 specification](#openwebbeans-apache-org-faq--does-owb-differ-from-the-cdi-20-specification)
- what is the default EL version used in OWB
- how can I provide own plugins if there is a new spec part (e.g EL-3.0) I like to support
- How can I contribute to OpenWebBeans
- I found what could be a bug, how do I proceed

### Does OWB differ from the CDI-2.0 specification[¶](#openwebbeans-apache-org-faq--does-owb-differ-from-the-cdi-20-specification "Permalink")

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-index"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-index--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

![owb_logo](openwebbeans.apache.org/resources/images/logo.png)

Apache OpenWebBeans delivers an implementation of the
[Contexts and Dependency injection for Jakarta EE](https://projects.eclipse.org/projects/ee4j.cdi/releases/4.0) (CDI) 4.0 Specification.  
OpenWebBeans is TCK compliant and the latest version works on Java SE 11 or later.

Apache OpenWebBeans is

- Fast - we aggressively use caches internally and deliver great performance
- Modular - OpenWebBeans Core is purely JavaSE, additional EE functionality gets added via 'Modules'
- Industry Proven - Many projects use OpenWebBeans in production.
- Community Oriented - Please visit our mailing list and we will help you moving your project forward.

## Getting Started with CDI

OpenWebBeans is packaged as modules which get activated by simply dropping them into the classpath.
The below link will take you to a step-by-step guide and get you started in no time!

[View details »](#openwebbeans-apache-org-documentation--module-overview)

## Meecrowave Server

Apache Meecrowave is a Microprofile Server based on Apache OpenWebBeans, Tomcat, CXF and Johnzon
In other words it contains all you need to run a JavaEE based Microservice from the command line - and all that in only 9 MB!

[View details »](#openwebbeans-apache-org-meecrowave-index)

---

## Latest News

#### [Apache OpenWebBeans-4.0.3 has been released](#openwebbeans-apache-org-news)

The Apache OpenWebBeans Team is proud to announce the release of Apache OpenWebBeans-4.0.3.

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-meecrowave-index"></a>

# Meecrowave :: the customizable server

# [ Meecrowave ](#openwebbeans-apache-org-meecrowave-index)

A light JAX-RS+CDI+JSON server!

## Cause power doesn't mean complicated!

Welcome to Meecrowave website, if you are fed up to need to adapt to a server instead of letting the server adapting to you you are on the right place!

### Quick Start

Deploy a JSON webservice in 5 mn!

[](#openwebbeans-apache-org-meecrowave-start)

### Components

Meecrowave extensions making development and production smoother.

[](#openwebbeans-apache-org-meecrowave-components)

### Downloads and License

Ready to get started? Grab meecrowave and run your services!

[](#openwebbeans-apache-org-meecrowave-download)

### Community

OpenWebBeans community is proud to host Meecrowave project.

[](#openwebbeans-apache-org-meecrowave-community)

Copyright © 2016-2020
[The Apache Software Foundation](http://www.apache.org/). All rights reserved.

Designed with  by [Xiaoying Riley](http://themes.3rdwavemedia.com/) for developers

---

<a id="openwebbeans-apache-org-misc-contact"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](index.html)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-misc-contact--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

## Product Support[¶](#openwebbeans-apache-org-misc-contact--product-support "Permalink")

If you have questions or comments about the software or documentation on this site, please subscribe to the appropriate [mailing list](#openwebbeans-apache-org-community).

## Security Issues[¶](#openwebbeans-apache-org-misc-contact--security-issues "Permalink")

If you would like to report a security issues with Apache OpenWebBeans, please contact [security.AT.apache.DOT.org](mailto:security.AT.apache.DOT.org).
Only security issues should be sent to this address.

## General Apache Issues[¶](#openwebbeans-apache-org-misc-contact--general-apache-issues "Permalink")

The OpenWebBeans Project is an effort of the [Apache Software Foundation](https://www.apache.org).
The address for general ASF correspondence and licensing questions is:

[apache.AT.apache.DOT.org](mailto:apache.AT.apache.DOT.org)

You can find more contact information for the Apache Software Foundation on [the contact page of the main Apache site](https://www.apache.org/foundation/contact.html).

You may also use [Apache Site Search](https://search.apache.org/) to scan all the Apache sites at once.

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-misc-legal"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](index.html)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-misc-legal--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

## Legal Stuff They Make Us Say[¶](#openwebbeans-apache-org-misc-legal--legal-stuff-they-make-us-say "Permalink")

All material on this website is Copyright © 2016, The Apache Software Foundation

Sun, Sun Microsystems, Solaris, Java and JavaServer Pages are trademarks or registered trademarks of Oracle Corporation.
UNIX is a registered trademark in the United States and other countries, exclusively licensed through 'The Open Group'.
Microsoft, Windows, WindowsNT, and Win32 are registered trademarks of Microsoft Corporation.
Linux is a registered trademark of Linus Torvalds.
All other product names mentioned herein and throughout the entire web site are trademarks of their respective owners.

## The Apache License[¶](#openwebbeans-apache-org-misc-legal--the-apache-license "Permalink")

All software produced by The Apache Software Foundation or any of its projects or subjects is licensed according to the terms of
[Apache License, Version 2.0 (current)](https://www.apache.org/licenses/LICENSE-2.0).

## Trademarks[¶](#openwebbeans-apache-org-misc-legal--trademarks "Permalink")

"Apache OpenWebBeans" and "OpenWebBeans" are trademarks of the Apache Software Foundation.
Use of these trademarks is subject to the terms of section 6 of
[Apache License, Version 2.0 (current)](https://www.apache.org/licenses/LICENSE-2.0).

If you create a product that uses Apache OpenWebBeans software or provides additional functionality to that software then:

- When referring to Apache OpenWebBeans software, please use the full name of "Apache OpenWebBeans" for at least the first reference on any web page, help file or similar.
  Subsequent references may refer to "OpenWebBeans".
- You may not use the OpenWebBeans logo without the permission of the Apache OpenWebBeans PMC.
- If you use the words "OpenWebBeans" or "Apache" in your product name then you must call your product "... for Apache OpenWebBeans".
  No other form of product name that includes "OpenWebBeans" or "Apache" is permitted.

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-ee-common"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-ee-common--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans EE common SPI[¶](#openwebbeans-apache-org-openwebbeans-ee-common--openwebbeans-ee-common-spi "Permalink")

Coming soon...

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-ee"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-ee--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans SPI[¶](#openwebbeans-apache-org-openwebbeans-ee--openwebbeans-spi "Permalink")

Coming soon...

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-ejb"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-ejb--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans SPI[¶](#openwebbeans-apache-org-openwebbeans-ejb--openwebbeans-spi "Permalink")

Coming soon...

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-el"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-el--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# Unified Expression Language[¶](#openwebbeans-apache-org-openwebbeans-el--unified-expression-language "Permalink")

Described in context here: [Getting started](#openwebbeans-apache-org-owbsetup_ee)

### EL 2.2 Module[¶](#openwebbeans-apache-org-openwebbeans-el--el-22-module "Permalink")

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-el22</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-impl"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-impl--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans Core[¶](#openwebbeans-apache-org-openwebbeans-impl--openwebbeans-core "Permalink")

**Hint:** The actual jar is called openwebbeans-impl and is the implementation of OpenWebBeans core.

## What does 'core' mean?[¶](#openwebbeans-apache-org-openwebbeans-impl--what-does-core-mean "Permalink")

OpenWebBeans follows the design principle loose coupling and high cohesion.
Everything needed to actually be a working CDI-container makes up
OpenWebBeans core.

All the logical parts in core have high cohesion with each other
and will age in approximately the same rate.
To conclude impl is the CDI-container as such and nothing more nothing less.
It does not contain any other JavaEE specific dependencies beside the bare minimum:

- atinject-api.jar
- cdi-api.jar
- interceptors-api jar
- openwebbeans-spi.jar, our SPI for pluggable extending the core.
- a bytecode engineering library (javassist for 1.0.x and 1.1.x, xbean-asm for 1.2.x and above)
- xbean-finder for classpath scanning

openwebbeans-core does **not** contain any JavaEE dependencies beyond that.
All additional features may portably be added via our SPI (Service Provider Interface).

This way core is completely unaffected by the release cycles of other
frameworks and Java specifications and the coupling is not only low,
it's virtually nonexistent.

## Why not a monolithic approach?[¶](#openwebbeans-apache-org-openwebbeans-impl--why-not-a-monolithic-approach "Permalink")

Imagine if the power outlets in your house was tightly coupled to your various devices.
The newest and coolest smartphone or what have you would probably not be as tempting
if it required you hiring an electrician to rewire your entire house.
To make it more ridiculous the new wiring would be non compatible with your TV
because it's two years old.

So if OpenWebBeans would have been built in a monolithic way with tight coupling and low cohesion,
the newest version would have to drop support for everything but the newest frameworks
or be a hot mess with version checks and endless if - else cases between all the framework
combinations. Naturally it would only get worse and worse over time.

Glad we avoided all that and have the exact opposite result, the latest
OpenWebBeans still support JSF 2.0, EL-1.0, JSP, Tomcat 7 and so on.
And you can just plug support for your own framework.

## Introduction to our plugin system[¶](#openwebbeans-apache-org-openwebbeans-impl--introduction-to-our-plugin-system "Permalink")

The end users has to combine the core with their own mix of plugins and can include
exactly what they want (including custom plugins) yet nothing they don't need.
The committers behind OpenWebBeans also have a much easier maintenance process
and can focus on features, speed and robustness of the dependency injection core
rather then the compatibility matrix.

This approach also eases the integration into various JavaEE Containers.

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-jms"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-jms--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans SPI[¶](#openwebbeans-apache-org-openwebbeans-jms--openwebbeans-spi "Permalink")

Coming soon...

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-jsf"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-jsf--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans JSF 2.x and JSF 1.2[¶](#openwebbeans-apache-org-openwebbeans-jsf--openwebbeans-jsf-2x-and-jsf-12 "Permalink")

Described in context here: [Getting started](#openwebbeans-apache-org-owbsetup_ee)

### JSF 2.X Module[¶](#openwebbeans-apache-org-openwebbeans-jsf--jsf-2x-module "Permalink")

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-jsf</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

### JSF 1.2 Module[¶](#openwebbeans-apache-org-openwebbeans-jsf--jsf-12-module "Permalink")

```text
Note: this module got dropped with OWB-1.7.x.
With OWB-2.x and 1.7.x we now only support EE6 upwards and dropped any technology prior to 2009.

<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-jsf12</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-junit5"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-junit5--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans JUnit 5[¶](#openwebbeans-apache-org-openwebbeans-junit5--openwebbeans-junit-5 "Permalink")

OpenWebBeans provides a JUnit 5 integration. It brings an extension which will be backed by CDI Standalone Edition API (`SeContainer`).

The entry point is `@Cdi` API which maps the main methods of `SeContainerInitializer`.
It typically enables you to define which classes you want to deploy for the test or if you want to use classpath scanning.

Here is how a test can look like:

```java
@Cdi(disableDiscovery = true, classes = MyService.class)
class CdiTest {
    @Inject
    private MyService service;

    @Test
    void test() {
        assertEquals("ok", service.ok());
    }
}
```

As you may notice, this test disable the default classpath scanning and only activate `MyService` bean.

Once a test is marked `@Cdi` you get injections of the underlying container available in the test class. This is how previous snippet can inject `MyService` into the test class without having anything else to do.

## Dependency[¶](#openwebbeans-apache-org-openwebbeans-junit5--dependency "Permalink")

```xml
 <dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-junit5</artifactId>
    <version>${owb.version}</version>
  </dependency>
```

IMPORTANT: this feature comes with OpenWebBeans >= 2.0.11.

## Reusable mode[¶](#openwebbeans-apache-org-openwebbeans-junit5--reusable-mode "Permalink")

`@Cdi` also adds a specific API called `reusable`. When this is set to `true`, the container is started and then reused by next test.
This enables to speed up the test suite a lot when reusing the same JVM to run all tests.

For example if you have these two tests:

```java
@Cdi(reusable = true)
class MyFirstTest {
    @Inject
    private MyService service;

    @Test
    void test() {
        assertEquals("ok", service.ok());
    }
}
```

and

```java
@Cdi(reusable = true)
class MySecondTest {
    @Inject
    private MyOtherService service;

    @Test
    void test() {
        assertEquals("ok2", service.ok());
    }
}
```

Then CDI container will be started only once.

However if the first test is:

```text
@Cdi(reusable = true, disableDiscovery = true, classes = MyService.class)
class MyFirstTest {
    // .. as before
}
```

Then the second test will not have its `MyOtherService` bean since previous test will only deploy `MyService`.

To avoid that it is recommended to follow these best practises:

- Never mix reusable and not reusable tests in the same suite (define multiple surefire executions for example),
- For reusable tests define a meta annotation which shares the configuration for all tests, this means you must have a single `reusable = true` in your whole code (per execution).

To define a reusable setup you just define a new annotation decorated with `@Cdi`:

```text
@Target(TYPE)
@Retention(RUNTIME)
@Cdi(reusable = true, disableDiscovery = true, packages = MyService.class)
public @interface TestConfig {
}
```

Then your test class can look like:

```java
@TestConfig
class MySecondTest {
    @Inject
    private MyService service;

    @Test
    void test() {
        assertEquals("ok", service.ok());
    }
}
```

### Surefire[¶](#openwebbeans-apache-org-openwebbeans-junit5--surefire "Permalink")

For reusable mode to be efficient, it is recommended to use this surefire configuration:

```xml
<plugin>
  <groupId>org.apache.maven.plugins</groupId>
  <artifactId>maven-surefire-plugin</artifactId>
  <version>${surefire.version}</version>
  <configuration>
    <forkCount>1</forkCount>
  </configuration>
</plugin>
```

And here is how to define executions to mix reusable and not reusable tests - which can have a different deployment setup:

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <version>${surefire.version}</version>
    <executions>
        <execution> <!-- skip default setup since we redefine it -->
            <id>default-test</id>
            <configuration>
                <skip>true</skip>
            </configuration>
        </execution>
        <execution>
            <id>not-reusable</id>
            <phase>test</phase>
            <goals>
                <goal>test</goal>
            </goals>
            <configuration>
                <includes>**/perclass/*</includes>
            </configuration>
        </execution>
        <execution>
            <id>reusable</id>
            <phase>test</phase>
            <goals>
                <goal>test</goal>
            </goals>
            <configuration>
                <includes>**/reusable/*</includes>
            </configuration>
        </execution>
    </executions>
    <configuration>
      <forkCount>1</forkCount>
    </configuration>
</plugin>
```

This setup assumes not reusable tests are in a `perclass` package and reusable ones are in a `reusable` package.
Alternatively you can use categories (just define markers in your test code) but this is generally less obvious to manage on the long run.

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-osgi"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-osgi--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans SPI[¶](#openwebbeans-apache-org-openwebbeans-osgi--openwebbeans-spi "Permalink")

Coming soon...

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-resource"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-resource--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans SPI[¶](#openwebbeans-apache-org-openwebbeans-resource--openwebbeans-spi "Permalink")

Coming soon...

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-spi"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-spi--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans SPI[¶](#openwebbeans-apache-org-openwebbeans-spi--openwebbeans-spi "Permalink")

## What is an SPI?[¶](#openwebbeans-apache-org-openwebbeans-spi--what-is-an-spi "Permalink")

> Service Provider Interface (SPI) is an API intended to be implemented
> or extended by a third party. It can be used to enable framework
> extension and replaceable components. - [wikipedia](https://en.wikipedia.org/wiki/Service_provider_interface)

## Why using a SPI in OpenWebBeans?[¶](#openwebbeans-apache-org-openwebbeans-spi--why-using-a-spi-in-openwebbeans "Permalink")

First off, reading about [OpenWebBeans Core](#openwebbeans-apache-org-openwebbeans-impl) will give you
the overall idea about the usage of SPIs in our plugin system.

Now as mentioned in that description the SPI is simply used to integrate other
frameworks with OpenWebBeans. The point of gravity for Java EE is definitely going
towards CDI today and the SPI pattern ensures that OpenWebBeans can manage this handily.

From a more technical standpoint it's nothing more then a bunch of interfaces.
For example a part of the SPI is the following interface:

```java
package org.apache.webbeans.spi;

/**
* Conversation related SPI.
* @version $Rev$ $Date$
*/
public interface ConversationService
{
    /**
    * Gets the current conversation id or null
    * if there is no conversation.
    * @return the current conversation id
    */
    public String getConversationId();
   
    /**
    * Gets the session id of the current session.
    * @return the session id of the current user session
    */
    public String getConversationSessionId();

}
```

After seeing this interface one can easily conclude that frameworks that want to utilize the Conversation Id functionality must implement this interface.
Now since this is part of the specification for JSF 2.x the JSF plugin of course implements it and actually the JSF 1.2 plugin as well. Supporting the Conversation Id in another plugin should be rather intuitive and this is true for the SPI in general.

### How does OWB know which implementation it should pick?[¶](#openwebbeans-apache-org-openwebbeans-spi--how-does-owb-know-which-implementation-it-should-pick "Permalink")

Each OpenWebBeans JAR has a property file to configure it's internal features
and also the SPI implementation which should be used:

```text
META-INF/openwebbeans/openwebbeans.properties
```

All those files contain at single property `configuration.ordinal` which defines their
'importance'. Any setting from a property file with a higher configuration.ordinal will
overwrite settings from one with a lower configuration.ordinal. The internally used
configuration.ordinal values range from 1 to 100.

For example: if you use a different UI technology than JSF like Vaadin, you could still provide
CDI Conversations by writing an own implementation of the respective SPI and tweak
some configuration settings:

```properties
configuration.ordinal=120 

# enable CDI conversation support at all
org.apache.webbeans.application.supportsConversation=true

# define your own implementation of our ConversationService SPI
org.apache.webbeans.spi.ConversationService=some.mycomp.MyVaadinConversationService
```

All those tricks allow us to remain extensible in the future and to support whatever scenario
we will face.

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-tomcat"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-tomcat--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans Tomcat 9 and 10[¶](#openwebbeans-apache-org-openwebbeans-tomcat--openwebbeans-tomcat-9-and-10 "Permalink")

Note: You also need other modules to run OpenWebBeans on Tomcat.
These Tomcat plugins only adds extra features like injection into Servlets and are not even mandatory for running on Tomcat.
For more information read [Getting started](#openwebbeans-apache-org-owbsetup_ee).

### Tomcat 7 and above[¶](#openwebbeans-apache-org-openwebbeans-tomcat--tomcat-7-and-above "Permalink")

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-tomcat</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-openwebbeans-web"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-openwebbeans-web--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans Web[¶](#openwebbeans-apache-org-openwebbeans-web--openwebbeans-web "Permalink")

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-web</artifactId>
    <version>${owb.version}</version>
    <scope>compile</scope>
</dependency>
```

Required for web-apps / Servlet-container projects that deploy to for example Tomcat.

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-owb-eecontainers"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-owb-eecontainers--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans in various JavaEE Containers[¶](#openwebbeans-apache-org-owb-eecontainers--openwebbeans-in-various-javaee-containers "Permalink")

Apache OpenWebBeans is used in a few JavaEE containers:

- Apache TomEE
- Apache Geronimo 3.x
- IBM WebSphere Application Server 8.0 and 8.5
- IBM WebSphere Application Server Liberty Profile 8.5
- SiwPas
- As part of various other Vendor offerings like SAP, etc

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-owbconfig"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-owbconfig--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans Configuration[¶](#openwebbeans-apache-org-owbconfig--openwebbeans-configuration "Permalink")

Internal configuration of OpenWebBeans can be done via: src/main/resources/META-INF/openwebbeans/openwebbeans.properties

All those files contain at single property `configuration.ordinal` which defines their
'importance'. Any setting from a property file with a higher configuration.ordinal will
overwrite settings from one with a lower configuration.ordinal. The internally used
configuration.ordinal values range from 1 to 100. The default value is 100.

This trick allows us to split OpenWebBeans in different modules which automatically change the container
configuration if you put that jar on the classpath.

For overriding the default configuration with own settings simply put a `META-INF/openwebbeans/openwebbeans.properties`
file into your projects classpath (e.g. into a jar) and either use a `configuration.ordinal` higher than 100 or leave
it empty to get the default value of 100.

If you use OpenWebBeans as part of another project then you can assume that most of the values got tweaked
by the integration regarding to the specific needs.

## Configure SPI implementations[¶](#openwebbeans-apache-org-owbconfig--configure-spi-implementations "Permalink")

OpenWebBeans provide a set of Service Provider Interfaces and multiple different implementations a user can choose from.

You can choose the implementations you like to use for your situation by configuring them in
`META-INF/openwebbeans/openwebbeans.properties`.

Read more about our SPIs in [OpenWebBeans SPI](#openwebbeans-apache-org-openwebbeans-spi)

## Other Configurable Values[¶](#openwebbeans-apache-org-owbconfig--other-configurable-values "Permalink")

The following configuration values can get tweaked to get tailor OWB to your specific needs.

Boolean values can either be `true`, or `TRUE`, or `false`, or `FALSE`.

- `org.apache.webbeans.forceNoCheckedExceptions`

  Lifycycle methods like `javax.annotation.PostConstruct` and
  `javax.annotation.PreDestroy` must not define a checked Exception
  regarding to the spec. But this is often unnecessary restrictive so we
  allow to disable this check application wide.

  Defaults to `true`.
- `org.apache.webbeans.spi.deployer.useEjbMetaDataDiscoveryService`

  Whether to perform EJB discovery or not.

  Defaults to `false`. In TomEE this gets automatically enabled.
- `org.apache.webbeans.application.jsp`

  If enabled, we automatically try to register the ELResolver in the JSP engine.
  Enable this setting if you like to access `@Named` CDI beans in JSP Expression Language.

  Default is `false`
- `org.apache.webbeans.application.supportsConversation`

  Enable support for the CDI `@ConversationScoped`.

  Disabled by default in JavaSE, but enabled by default when adding the webbeans-web module (Servlets)
- `org.apache.webbeans.application.supportsProducerInterception`

  Define if a CDI interceptor can be used on a producer method or field.
  In OpenWebBeans you can use a `@StereoType` with an Interceptor to enable
  an Interceptor on the instance returned from a Producer Method or Producer Field.

  Enabled by default.
- `org.apache.webbeans.scanExclusionPaths`

  A list of known JARs/paths which should not be scanned for beans.
  This is only used by the default ScannerService implementation.

  Please refer to the openwebbeans.properties file in ``webbeans-impl.jar``
- `org.apache.webbeans.scanBeansXmlOnly`

  Flag which indicates that only jars with an explicit META-INF/beans.xml marker file shall get parsed.
  This basically switches OWB back to the CDI-1.0 scanning behaviour and might speed up the boot process.

  Default is false
- `org.apache.webbeans.ignoredDecoratorInterfaces`

  A comma-separated list of fully qualified class names that should be ignored
  when determining if a decorator matches its delegate.

  Default is an empty list
- `org.apache.webbeans.web.eagerSessionInitialisation`

  By default we do \_not\_ force session creation in our WebBeansConfigurationListener. We only create the
  Session if we really need the SessionContext. E.g. when we create a Contextual Instance in it.
  Sometimes this creates a problem as the HttpSession can only be created BEFORE anything got written back
  to the client.
  With this configuration you can choose between 3 settings

  - "true" the Session will always eagerly be created at the begin of a request
  - "false" the Session will never eagerly be created but only lazily when the first @SessionScoped bean gets used
  - any other value will be interpreted as Java regular expression for request URIs which need eager Session initialization

  Defaults to false. A session will only get created if a `@SessionScoped` bean gets accessed for the first time.
- `org.apache.webbeans.generator.javaVersion`

  The Java Version to use for the generated proxy classes.
  If `auto` then we will pick the version of the current JVM.
  *Attention:* If you like to use Java8 Lambdas in CDI bean method signatures then you need to
  switch to either `auto` or `1.8`!

  The default is set to `1.6` as some tools in jetty/tomcat/etc still
  cannot properly handle Java8 (mostly due to older Eclipse JDT versions).
- `javax.enterprise.inject.allowProxying.classes`

  Environment property which comma separated list of classes which
  should NOT fail with an UnproxyableResolutionException.
  You only have to configure additional classes in your openwebbeans.properties file.
  All the configured values get added together into a big List.

  By default we allow the following classes: `java.util.HashMap` and `java.util.Calendar`

## Proxy Mapping[¶](#openwebbeans-apache-org-owbconfig--proxy-mapping "Permalink")

OpenWebBeans enables the user to define the NormalScope handlers for specific scopes.

NormalScope handlers are used by OpenWebBeans' proxies to resolve the 'Contextual Instance'.
E.g. for a `@SessionScoped User` injected into some other class, this is exactly the piece of code
which goes into the current Http Session and gets the User instance from there.
This class must extend `org.apache.webbeans.intercept.NormalScopedBeanInterceptorHandler` and overwrite the
`Object getContextualInstance()` method.

This allows for more aggressive caching than with the generic `NormalScopedBeanInterceptorHandler` which is the default.
The default NormalScope handler will look up the Contextual Instance in the respective Context for each and every
method invocation on the proxy.

But sometimes we can much more aggressively cache the instances.

E.g. for `@ApplicationScoped` beans we can keep the contextual instance inside the proxy,
making it as fast as a pure Java instance - but still gaining all the benefits of CDI!

For `@RequestScoped` and `@SessionScoped` we can use a NormalScope handler which caches the Contextual Instance in a ThreadLocal.

By default the following NormalScope handlers get used:

```properties
org.apache.webbeans.proxy.mapping.javax.enterprise.context.ApplicationScoped
    =org.apache.webbeans.intercept.ApplicationScopedBeanInterceptorHandler
org.apache.webbeans.proxy.mapping.javax.enterprise.context.RequestScoped
    =org.apache.webbeans.intercept.RequestScopedBeanInterceptorHandler
org.apache.webbeans.proxy.mapping.javax.enterprise.context.SessionScoped
    =org.apache.webbeans.intercept.SessionScopedBeanInterceptorHandler
```

As you can see we use a prefix `org.apache.webbeans.proxy.mapping.` followed by the fully qualified scope name as key.
The value represents the fully qualified name of the handler class.

If you have a custom scope which spans a Request or longer then you can simply reuse the `RequestScopedBeanInterceptorHandler` as shown in the following example:

```properties
org.apache.webbeans.proxy.mapping.org.apache.deltaspike.core.api.scope.ViewAccessScoped
    =org.apache.webbeans.intercept.RequestScopedBeanInterceptorHandler
```

## Enable FailOver / Session Replication support[¶](#openwebbeans-apache-org-owbconfig--enable-failover-session-replication-support "Permalink")

#### Since OpenWebBeans-1.5.0[¶](#openwebbeans-apache-org-owbconfig--since-openwebbeans-150 "Permalink")

OWB-1.5.x and later does *not* need any special module or filter to enable clustering.
All that works out of the box as we now directly utilize the Servlet Session.

#### OpenWebBeans-1.2.x[¶](#openwebbeans-apache-org-owbconfig--openwebbeans-12x "Permalink")

Add the clustering module to your project:

```xml
<dependency>
    <groupId>org.apache.openwebbeans</groupId>
    <artifactId>openwebbeans-clustering</artifactId>
</dependency>
```

Register the FailOverFilter in your web.xml:

```xml
<filter>
    <filter-name>OWB FailOverFilter</filter-name>
    <filter-class>org.apache.webbeans.web.failover.FailOverFilter</filter-class>
</filter>
<filter-mapping>
    <filter-name>OWB FailOverFilter</filter-name>
    <servlet-name>Faces Servlet</servlet-name>
</filter-mapping>
```

#### OpenWebBeans-1.0.x and 1.1.x[¶](#openwebbeans-apache-org-owbconfig--openwebbeans-10x-and-11x "Permalink")

Add the following properties in your openwebbeans.properties:

```properties
configuration.ordinal=100 
org.apache.webbeans.web.failover.issupportfailover=true
org.apache.webbeans.web.failover.issupportpassivation=true
```

Register the FailOverFilter in your web.xml:

```xml
<filter>
    <filter-name>OWB FailOverFilter</filter-name>
    <filter-class>org.apache.webbeans.web.failover.FailOverFilter</filter-class>
</filter>
<filter-mapping>
    <filter-name>OWB FailOverFilter</filter-name>
    <servlet-name>Faces Servlet</servlet-name>
</filter-mapping>
```

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-owbinternalunittests"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-owbinternalunittests--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# Testing strategies for unit tests inside OpenWebBeans[¶](#openwebbeans-apache-org-owbinternalunittests--testing-strategies-for-unit-tests-inside-openwebbeans "Permalink")

One could argue that unit tests are harder to write when your instances are managed by a container.
However this is only true if the booting of said container is uncharted territory.
But lets assume the container control is taken cared of handily. Well then it can be quite the breath of fresh air to test code
that leverage dependency injection.

Here comes the good news. Testing OpenWebBeans and CDI in general (and actually many other Java EE frameworks)
is in a good state as of today and testing frameworks are reaching a pretty decent level of maturity. Stay with us and
we will compare the pros and cons with
the different strategies.

### Good Practice[¶](#openwebbeans-apache-org-owbinternalunittests--good-practice "Permalink")

We won't go in to detail on how to properly use unit tests or integration tests in your project. However so called "Whitebox Testing" is
discouraged. Using whitebox testing is often critizied regardless but with frameworks that leverage
proxies it's simply not something you should attempt. Any reflection trick would likely miss the mark and modify the proxy.
Instead focus on functional tests and structure your code with high cohesion so that testing the public methods get's the job done.
Remember to add beans.xml and other resources to your to your test path.

### Start small with plain tests[¶](#openwebbeans-apache-org-owbinternalunittests--start-small-with-plain-tests "Permalink")

Testing code that leverage CDI does not differ much from using CDI in your project. You should start with just a plain pojo
in your project and likewise a plain unit test. Only when you need context should you upgrade the pojo to a CDI managed instance.
Still this does not mean you automatically need something more then plain unit test. But when you need the container to act on your
instances (for example to trigger `@PostConstruct`) then go ahead and upgrade the test to be CDI aware.

### CDI aware tests[¶](#openwebbeans-apache-org-owbinternalunittests--cdi-aware-tests "Permalink")

In OpenWebBeans we use JUnit as testing framework. For not having to deal with the container details each time you can
simply write a JUnit test which extends the `org.apache.webbeans.test.AbstractUnitTest` base class.

Lets look at how to write a unit test which e.g. tests a method invocation on a specific CDI bean.

The first thing we obviously need is the CDI bean which should get tested:

```java
@RequestScoped
public class BeanUnderTest
{
    public int meaningOfLife()
    {
        return 42;
    }
}
```

And now let's write the unit test which calls this method:

```java
public class MySimpleTest extends AbstractUnitTest
{
    @Test
    public void testMeaningOfLife()
    {
        startContainer(BeanUnderTest.class);
        BeanUnderTest instance = getInstance(BeanUnderTest.class);
        Assert.assertnotNull(instance);
        Assert.assertEquals(42, instance.meaningOfLife());
    }
}
```

That's all! You don't need even need to manually shut down the container after the method.
If you have multiple beans to test then pass all of them as argument to `startContainer(Class...);`.
There are also startContainer variants which take a beans.xml. Also look at the other methods of `AbstractUnitTest` for more useful features.

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-owbsetup_ee"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-owbsetup_ee--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# Adding OpenWebBeans to your Servlet Container project[¶](#openwebbeans-apache-org-owbsetup_ee--adding-openwebbeans-to-your-servlet-container-project "Permalink")

OpenWebBeans in a great match for web-apps and should work well for your favorite Servlet Containers such as Jetty or Tomcat.
To add OpenWebBeans to your Servlet Container project you need to take the following steps.

1. Add [dependencies](#openwebbeans-apache-org-owbsetup_ee--required-parts) accordingly to instructions below.
2. In some cases add [org.apache.webbeans.servlet.WebBeansConfigurationListener](#openwebbeans-apache-org-owbsetup_ee--configuring-owb) to web.xml as a listener
3. Done! Congratulations.

### Adding required jars and plugins to your project[¶](#openwebbeans-apache-org-owbsetup_ee--adding-required-jars-and-plugins-to-your-project "Permalink")

You can add OpenWebBeans to your project manually by adding jars or with Apache Maven. How to download is explained here: [download page](#openwebbeans-apache-org-download--apis-version).
The binary distributions include all the jars you need and the download page lists all the [maven dependencies](#openwebbeans-apache-org-download--maven-dep).
But since OpenWebBeans is modular you should read below so you know what to add.

### API jars[¶](#openwebbeans-apache-org-owbsetup_ee--api-jars "Permalink")

Several API bundles exists for java EE and they are mostly compatible with OpenWebBeans. If you already include one of these you might not need any api jars. CDI and thus OpenWebBeans depends on the following four apis:

- **CDI: [geronimo-jcdi\_2.0\_spec.jar](#openwebbeans-apache-org-download--apis-version)**
- **AtInject: [geronimo-atinject\_1.0\_spec.jar](#openwebbeans-apache-org-download--apis-version)**
- **Interceptor: [geronimo-interceptor\_1.2\_spec.jar](#openwebbeans-apache-org-download--apis-version)**
- **Common Annotations: [geronimo-annotation\_1.3\_spec.jar](#openwebbeans-apache-org-download--apis-version)**

You will only reference these API:s in your own project. This way your project will stay CDI vendor neutral. A typical use case would be to add all four of the above to your parent-pom or your core module.

### Required[¶](#openwebbeans-apache-org-owbsetup_ee--required-parts "Permalink")

For Servlet Container projects such as Tomcat and Jetty you always start with:

- **[openwebbeans-spi](#openwebbeans-apache-org-download--required-version)**
- **[openwebbeans-impl](#openwebbeans-apache-org-download--required-version)**
- **[openwebbeans-web](#openwebbeans-apache-org-download--plugins-version)**

### Plugins[¶](#openwebbeans-apache-org-owbsetup_ee--plugins "Permalink")

The following plugins are very useful if you need JSF, expression language (el) etc. Add accordingly to your needs.

- **[openwebbeans-el22](#openwebbeans-apache-org-download--plugins-version)**
- **[openwebbeans-tomcat7](#openwebbeans-apache-org-download--plugins-version)**
- **[openwebbeans-jsf](#openwebbeans-apache-org-download--plugins-version)**

### When to use respective plugin[¶](#openwebbeans-apache-org-owbsetup_ee--when-to-use-respective-plugin "Permalink")

If the project uses Expression Language add EL plugin accordingly to your version. This is required for using EL.

- Expression Language 2.2 / 3.0 - **[openwebbeans-el22](#openwebbeans-apache-org-download--plugins-version)**

For JSF support add JSF plugin accordingly to your version of JSF. This plugin is required for JSF support and do not forget the include the EL-plugin explained above.

- Java Server Faces 2.0 or later - **[openwebbeans-jsf](#openwebbeans-apache-org-download--plugins-version)**

If the project uses Tomcat 7 or above you can add the respective plugin.
This is not required but enables injection in Servlets and Filters.
Note that the tomcat7 plugin works perfectly fine with Tomcat 8, 8.5 and even Tomcat 9.

- Tomcat 7 / Tomcat 8 / Tomcat 9 - **[openwebbeans-tomcat7](#openwebbeans-apache-org-download--plugins-version)**

### Bootstrapping OpenWebBeans[¶](#openwebbeans-apache-org-owbsetup_ee--configuring-owb "Permalink")

Simply put the following listener in web.xml:

```xml
    <listener>
        <listener-class>org.apache.webbeans.servlet.WebBeansConfigurationListener</listener-class>
    </listener>
```

This is not required if you use Tomcat and added the corresponding Tomcat plugin because in that case it's managed by the plugin.

From here you might want to look at our samples selection: [samples](#openwebbeans-apache-org-samples).

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-owbsetup_se"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-owbsetup_se--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans and JavaSE[¶](#openwebbeans-apache-org-owbsetup_se--openwebbeans-and-javase "Permalink")

To add OpenWebBeans to your javaSE project you need to take the following steps:

1. Add required jars to your project
2. Bootstrap OpenWebBeans
3. Done! Congratulations.

### Adding required jars to your project[¶](#openwebbeans-apache-org-owbsetup_se--adding-required-jars-to-your-project "Permalink")

You can add OpenWebBeans to your project manually by adding jars or with Apache Maven.
How to download is explained here: [download page](#openwebbeans-apache-org-download). This is especially useful if you are not a maven user since the below links goes directly to the maven coordinates.

For JavaSE you need:

- **[openwebbeans-spi.jar](#openwebbeans-apache-org-download--required-version)**
- **[openwebbeans-impl.jar](#openwebbeans-apache-org-download--required-version)**

Those two parts of OpenWebBeans are what you could call "system core".
These are the only OWB artifacts you need for JavaSE capabilities and
for the time being the existing plugins basically just adds JavaEE capabilities.

You also need to add some spec API jars for the CDI, atinject and interceptors
specifications.

- **[geronimo-jcdi\_2.0\_spec.jar](#openwebbeans-apache-org-download--apis-version)**
- **[geronimo-atinject\_1.0\_spec.jar](#openwebbeans-apache-org-download--apis-version)**
- **[geronimo-interceptor\_1.2\_spec.jar](#openwebbeans-apache-org-download--apis-version)**
- **[geronimo-annotation\_1.3\_spec.jar](#openwebbeans-apache-org-download--apis-version)**

After you have added the jars described above to your project accordingly
to the download page and added them to your projects classpath.

### Bootstrapping OpenWebBeans[¶](#openwebbeans-apache-org-owbsetup_se--bootstrapping-openwebbeans "Permalink")

For now we recommend two ways for booting up the OpenWebBeans container:
[**Deltaspike CdiCtrl**](https://deltaspike.apache.org/documentation.html#with-java-se) or booting it yourself in i.e. a standard main method.

#### Option number one - Apache DeltaSpike CdiCtrl[¶](#openwebbeans-apache-org-owbsetup_se--option-number-one-apache-deltaspike-cdictrl "Permalink")

Apache DeltaSpike is a set of portable CDI Extensions. It contains a module which allows
to control various CDI-Containers without having to change your own code. It contains an API
and multiple implementations for a few CDI Containers.

For most projects [**Deltaspike CdiCtrl**](https://deltaspike.apache.org/documentation.html#with-java-se) will be the smoother choice to boot your project
in JavaSE .

#### Option number two - booting yourself\*\*[¶](#openwebbeans-apache-org-owbsetup_se--option-number-two-booting-yourself "Permalink")

Going native and booting Apache OpenWebBeans yourself could however be useful if you need full control
to do advanced things.

```java
import org.apache.deltaspike.cdise.api.CdiContainer;
import org.apache.deltaspike.cdise.api.CdiContainerLoader;
import org.apache.deltaspike.cdise.api.ContextControl;
import javax.enterprise.context.ApplicationScoped;

public class MainApp  {
    private static ContainerLifecycle lifecycle = null;
    public static void main(String[] args)  {
        lifecycle = WebBeansContext.currentInstance().getService(ContainerLifecycle.class);
        lifecycle.startApplication(null);
    }

    public static void shutdown()  {
        lifecycle.stopApplication(null);
    }
}
```

From here you might want to look at our samples selection: [samples](#openwebbeans-apache-org-samples).

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-owbsetup_tomcat"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-owbsetup_tomcat--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# Adding OpenWebBeans to an Apache Tomcat installation[¶](#openwebbeans-apache-org-owbsetup_tomcat--adding-openwebbeans-to-an-apache-tomcat-installation "Permalink")

By integrating OpenWebBeans with an existing tomcat installation you don't need to add any CDI container jars to your WAR files.

Instead, OpenWebBeans will get copied to the tomcat lib directory and activated for all webapps.

## Downloading the latest OpenWebBeans distribution[¶](#openwebbeans-apache-org-owbsetup_tomcat--downloading-the-latest-openwebbeans-distribution "Permalink")

Download the latest [OWB distribution bundle](#openwebbeans-apache-org-download) and unzip it.

## Integrating Apache OpenWebBeans with Apache Tomcat[¶](#openwebbeans-apache-org-owbsetup_tomcat--integrating-apache-openwebbeans-with-apache-tomcat "Permalink")

Change to the unzipped OWB distribution directory and run the following script:

```text
./install_owb_tomcat7.sh /path/to/your/tomcat/installation
```

This will copy all necessary Apache OpenWebBeans jars over to your tomcat installation directory.
It will also patch the tomcat context.xml and add org.apache.webbeans.web.tomcat7.ContextLifecycleListener.

## Integrating OWB and Apache MyFaces with tomcat[¶](#openwebbeans-apache-org-owbsetup_tomcat--integrating-owb-and-apache-myfaces-with-tomcat "Permalink")

We also provide another script which can be used to install OpenWebBeans and [Apache MyFaces JSF Container](http://myfaces.apache.org) into tomcat.

```text
./install_owb_tomcat7_myfaces.sh somelocation/myfaces-core-assembly-2.2.8-bin.zip /path/to/your/tomcat/installation
```

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-source"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-source--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# Getting Involved[¶](#openwebbeans-apache-org-source--getting-involved "Permalink")

We are always looking for new contributors to the project.

Pleaes see our [Community Section](#openwebbeans-apache-org-community) for more information.

# Cannonical Source Repository[¶](#openwebbeans-apache-org-source--cannonical-source-repository "Permalink")

The sources of Apache OpenWebBeans are maintained in the Apache Software Foundation gitbox repository.
This is the repository where all committers work on.
It gets mirrored to GitHub in both directions.
That means you can either push to Apache GitBox or GitHub.

The master branch currently contains our implementation of the CDI-2.0 specification and is considered production ready.

The sources can be checked out read only with the following command:

```text
$> git clone https://github.com/apache/openwebbeans
```

or

```text
$> git clone https://gitbox.apache.org/repos/asf/openwebbeans.git
```

## Maintenance releases targetting older CDI specifications[¶](#openwebbeans-apache-org-source--maintenance-releases-targetting-older-cdi-specifications "Permalink")

### CDI-1.2 - OpenWebBeans-2.0.x[¶](#openwebbeans-apache-org-source--cdi-12-openwebbeans-20x "Permalink")

For checking out sources of the stable CDI-2.0 version of OpenWebBeans, please use the owb\_2.0.x branch from here:

```text
$> git clone https://github.com/apache/openwebbeans
$> git checkout owb_2.0.x
```

### CDI-1.2 - OpenWebBeans-1.7.x[¶](#openwebbeans-apache-org-source--cdi-12-openwebbeans-17x "Permalink")

For checking out sources of the stable CDI-1.2 version of OpenWebBeans, please use the owb\_1.7.x branch from here:

```text
$> git clone https://github.com/apache/openwebbeans
$> git checkout owb_1.7.x
```

### CDI-1.0 - OpenWebBeans-1.2.x[¶](#openwebbeans-apache-org-source--cdi-10-openwebbeans-12x "Permalink")

For checking out sources of the stable CDI-1.0 version of OpenWebBeans, please use the owb\_1.2.x branch from here:

```text
$> git clone https://github.com/apache/openwebbeans
$> git checkout owb_1.2.x
```

# Building OpenWebBeans[¶](#openwebbeans-apache-org-source--building-openwebbeans "Permalink")

Apache OpenWebBeans can be built by using Apache Maven. Just go into the source directory and execute

```bash
mvn clean install
```

The following maven profiles exist in our build to trigger additional build steps and configuration:

- tck - for executing the CDI (JSR-299, JSR-346 resp JSR-365) standalone TCK
- jsr330-tck - for executing the JSR-330 'atinject' TCK

In master they are all activated by default and run every time you build OpenWebBeans.

For older OpenWebBeans versions you might enable them manually.

```bash
mvn clean install -Ptck -Pjsr330-tck -Pdoc
```

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-testing_arquillian"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-testing_arquillian--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# Testing your application with JBoss Arquillian[¶](#openwebbeans-apache-org-testing_arquillian--testing-your-application-with-jboss-arquillian "Permalink")

## About Arquillian[¶](#openwebbeans-apache-org-testing_arquillian--about-arquillian "Permalink")

[JBoss Arquillian](http://arquillian.org/) is a very popular testing framework for complex use cases. You write your test with
JUnit or TestNG but with the extended possibility to have fine grained control over the managed environment.
Basically Arquillian allows you to create artificial war / jar files with the exact content you need for your test.
Many powerful features are available in extensions. One such extension is `Arquillian Drone`
that allows you to test with a web-based user interface and `Arquillian Performance` is another one
that offers rich functionality aimed at performance testing.

One of the main principles of `Arquillian` is to be portable and thus using it to test applications that
leverage OpenWebBeans is fully supported.

To author a new `Arquillian` test you follow this rough flow:

- Create a new JUnit / TestNG test class
- Annotate the class with `@RunWith(Arquillian.class)`
- Create a new deployment and include exactly what you need for that test.
  It's important that you add beans.xml to your test path and include it in the deployment.
- Use @Inject (from the usual package) to obtain instances that you included in the deployment
- Perform assertions on injected instances "normally".

## Testing our Java SE / Servlet container project[¶](#openwebbeans-apache-org-testing_arquillian--testing-our-java-se-servlet-container-project "Permalink")

The best way to get started with `Arquillian` is to follow the official [Getting Started](http://arquillian.org/guides/getting_started/) guide. However for testing
OpenWebBeans standalone you will need to use the following adapter:

```xml
<dependency>
   <groupId>org.apache.openwebbeans.arquillian</groupId>
   <artifactId>owb-arquillian-parent</artifactId>
   <version>${owb.version}</version>
</dependency>
```

## Testing your Java EE project[¶](#openwebbeans-apache-org-testing_arquillian--testing-your-java-ee-project "Permalink")

In addition to the adapter described above TomEE offers serveral adapters for working with TomEE. For more information visit [TomEE: Available Arquillian Adapters](https://tomee.apache.org/arquillian-available-adapters.html).

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-testing_cdictrl"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-testing_cdictrl--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# Testing your application with Apache DeltaSpike CdiCtrl[¶](#openwebbeans-apache-org-testing_cdictrl--testing-your-application-with-apache-deltaspike-cdictrl "Permalink")

## About CdiCtrl[¶](#openwebbeans-apache-org-testing_cdictrl--about-cdictrl "Permalink")

`CdiCtrl` is *not* part of Apache OpenWebBeans but a module of
[Apache DeltaSpike](https://deltaspike.apache.org).

The `CdiCtrl` interface abstracts away all the logic to boot a CDI Container
and controls the lifecycle of it's Contexts (Request Context, Session Context, etc).

The actual CDI Container is determined by using the `java.util.ServiceLoader`.
There are a few different implementations available. Besides Apache OpenWebBeans
there are also plugins for JBoss Weld and [Apache TomEE](https://tomee.apache.org).

## Adding OpenWebBeans CdiCtrl to your project[¶](#openwebbeans-apache-org-testing_cdictrl--adding-openwebbeans-cdictrl-to-your-project "Permalink")

The following are the dependencies you need in your Apache Maven pom.xml file in addition to
OWB itself:

```xml
<dependency>
    <groupId>org.apache.deltaspike.cdictrl</groupId>
    <artifactId>deltaspike-cdictrl-api</artifactId>
    <version>$  {deltaspike.version}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.apache.deltaspike.cdictrl</groupId>
    <artifactId>deltaspike-cdictrl-owb</artifactId>
    <version>$  {deltaspike.version}</version>
    <scope>test</scope>
</dependency>
```

## Why use CdiCtrl for your unit tests?[¶](#openwebbeans-apache-org-testing_cdictrl--why-use-cdictrl-for-your-unit-tests "Permalink")

Whenever you need to write unit tests for a full application, then you will need to
have a CDI container scann all your classes, create `Bean<T>` from it and provide
them for injection. All this can be done by either using JUnits `@RunWith` or
by simply creating a common base class for your unit tests which boots up the
container on your test classpath.

There is no need to restart the container for each and every of your unit tests
as this would cause a big performance loss. Instead it is usually sufficient to
use the CdiCtrls `ContextControl` mechanism to just stop and restart the
respective CDI Contexts.

Such a base class could look roughly like the following:

```java
import org.apache.deltaspike.cdise.api.CdiContainer;
import org.apache.deltaspike.cdise.api.CdiContainerLoader;
import org.apache.deltaspike.core.api.projectstage.ProjectStage;
import org.apache.deltaspike.core.util.ProjectStageProducer;
import org.apache.deltaspike.core.api.provider.BeanProvider;

public abstract class ContainerTest  {

    protected static volatile CdiContainer cdiContainer;
    // nice to know, since testng executes tests in parallel.
    protected static int containerRefCount = 0;

    protected ProjectStage runInProjectStage()  {
        return ProjectStage.UnitTest;
    }
    
    /**
     * Starts container
     * @throws Exception in case of severe problem
     */
    @BeforeMethod
    public final void beforeMethod() throws Exception  {
        containerRefCount++;

        if (cdiContainer == null)  {
            // setting up the Apache DeltaSpike ProjectStage
            ProjectStage projectStage = runInProjectStage();
            ProjectStageProducer.setProjectStage(projectStage);

            cdiContainer = CdiContainerLoader.getCdiContainer();

            cdiContainer.boot();
            cdiContainer.getContextControl().startContexts();
        }
        else  {
            cleanInstances();
        }
    }

    public static CdiContainer getCdiContainer()  {
        return cdiContainer;
    }

    /**
     * This will fill all the InjectionPoints of the current test class for you
     */
    @BeforeClass
    public final void beforeClass() throws Exception  {
        beforeMethod();

        // perform injection into the very own test class
        BeanManager beanManager = cdiContainer.getBeanManager();

        CreationalContext creationalContext = beanManager.createCreationalContext(null);

        AnnotatedType annotatedType = beanManager.createAnnotatedType(this.getClass());
        InjectionTarget injectionTarget = beanManager.createInjectionTarget(annotatedType);
        injectionTarget.inject(this, creationalContext);

        // this is a trick we use to have proper DB transactions when using the entitymanager-per-request pattern
        cleanInstances();
        cleanUpDb();
        cleanInstances();
    }

    /**
     * Shuts down container.
     * @throws Exception in case of severe problem
     */
    @AfterMethod
    public final void afterMethod() throws Exception  {
        if (cdiContainer != null)  {
            cleanInstances();
            containerRefCount--;
        }
    }

    /**
     * clean the NormalScoped contextual instances by stopping and restarting
     * some contexts. You could also restart the ApplicationScoped context
     * if you have some caches in your classes. 
     */
    public final void cleanInstances() throws Exception  {
        cdiContainer.getContextControl().stopContext(RequestScoped.class);
        cdiContainer.getContextControl().startContext(RequestScoped.class);
        cdiContainer.getContextControl().stopContext(SessionScoped.class);
        cdiContainer.getContextControl().startContext(SessionScoped.class);
    }

    @AfterSuite
    public synchronized void shutdownContainer() throws Exception  {
        if (cdiContainer != null)  {
            cdiContainer.shutdown();
            cdiContainer = null;
        }
    }

    public void finalize() throws Throwable  {
        shutdownContainer();
        super.finalize();
    }

    /**
     * Override this method for database clean up.
     *
     * @throws Exception in case of severe problem
     */
    protected void cleanUpDb() throws Exception  {
        //Override in subclasses when needed
    }

    protected <T> T getInstance(Class<T> type, Qualifier... qualifiers)  {
        return BeanProvider.getContextualReference(type, qualifiers);
    }

}
```

## Testing JavaEE applications[¶](#openwebbeans-apache-org-testing_cdictrl--testing-javaee-applications "Permalink")

You can also plug in a cdictrl backend for [Apache TomEE](https://tomee.apache.org) whenever you need to not only test CDI applications
but a full JavaEE application which has EJBs, managed DataSources, JTA, etc
The only thing you need to do is to replace your `deltaspike-cdictrl-owb` dependency in your pom with
`deltaspike-cdictrl-openejb`. Since Apache TomEE and Apache OpenEJB both contain OpenWebBeans as CDI container
you will get all the OWB functionality plus other JavaEE functionality.

You can pass DataSource configuration by simply providing a `Properties` instance to
`CdiContainer.boot(dbConfiguration)` in the beforeMethod method of the test class above:

```java
public final void beforeMethod() throws Exception  {
    containerRefCount++;

    if (cdiContainer == null)  {
        // setting up the Apache DeltaSpike ProjectStage
        ProjectStage projectStage = runInProjectStage();
        ProjectStageProducer.setProjectStage(projectStage);
        cdiContainer = CdiContainerLoader.getCdiContainer();

        Properties dbProperties = new Properties();
        String dbvendor = ConfigResolver.getPropertyValue("dbvendor", "h2");
        URL dbPropertiesUrl =  getClass().getResource("/db/db-" + dbvendor + ".properties");
        if (dbPropertiesUrl != null)  {
            InputStream is = dbPropertiesUrl.openStream();
            try  {
                dbProperties.load(is);
            }
            finally  {
                is.close();
            }
        }

        cdiContainer.boot(dbProperties);
    }
    else  {
        cleanInstances();
    }
}
```

The `db/db-mysql.properties` file for Apache OpenEJB (the former name of TomEE) would look like:

```properties
MYDS = new://Resource?type=DataSource
MYDS.JdbcDriver = org.h2.Driver
MYDS.JdbcUrl = jdbc:h2:file:/tmp/h2/myappdb
MYDS.JtaManaged = true
MYDS.UserName = sa
MYDS.Password =
```

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-testing_general"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-testing_general--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# Testing strategies for projects that leverage CDI[¶](#openwebbeans-apache-org-testing_general--testing-strategies-for-projects-that-leverage-cdi "Permalink")

One could argue that unit tests are harder to write when your instances are managed by a container.
However this is only true if the booting of said container is uncharted territory.
But lets assume the container control is taken cared of handily. Well then it can be quite the breath of fresh air to test code
that leverage dependency injection.

Here comes the good news. Testing OpenWebBeans and CDI in general (and actually many other Java EE frameworks)
is in a good state as of today and testing frameworks are reaching a pretty decent level of maturity. Stay with us and
we will compare the pros and cons with
the different strategies.

### Good Practice[¶](#openwebbeans-apache-org-testing_general--good-practice "Permalink")

We won't go in to detail on how to properly use unit tests or integration tests in your project. However so called "Whitebox Testing" is
discouraged. Using whitebox testing is often critizied regardless but with frameworks that leverage
proxies it's simply not something you should attempt. Any reflection trick will would likely miss the mark and modify the proxy.
Instead focus on functional tests and structure your code with high cohesion so that testing the public methods get's the job done.
Remember to add beans.xml and other resources to your to your test path.

### Start small with plain tests[¶](#openwebbeans-apache-org-testing_general--start-small-with-plain-tests "Permalink")

Testing code that leverage CDI does not differ much from using CDI in your project. You should start with just a plain pojo
in your project and likewise a plain unit test. Only when you need context should you upgrade the pojo to a CDI managed instance.
Still this does not mean you automatically need something more then plain unit test. But when you need the container to act on your
instances (for example to trigger `@PostConstruct`) then go ahead and upgrade the test to CDI aware. To get started with plain unit testing please
refer to [JUnit](https://junit.org/) or [TestNG](https://testng.org/doc/index.html) as a starting point.

### CDI aware tests[¶](#openwebbeans-apache-org-testing_general--cdi-aware-tests "Permalink")

These frameworks for testing all rely on either JUnit or TestNG and simply adds CDI container control capabilities. None of these frameworks are exclusive
and `CdiCtrl` can support any CDI environment including tests. Apache Deltaspike Core may also prove useful (perhaps especially the BeanProvider). [Deltaspike Core](https://deltaspike.apache.org/core.html).

**[Apache Deltaspike Test-Control](#openwebbeans-apache-org-testing_test-control)**  
The by far easiest and most straight forward way to write your first test is definitely with [Apache Deltaspike Test-Control](https://deltaspike.apache.org/test-control.html).
This way of testing boots the CDI Container with minimal code and there's not much to learn besides CDI and unit testing with JUnit / TestNG.
Since Test-Control is a thin layer on top of `CdiCtrl` it's advised to continue reading below for more information.

**[Apache Deltaspike CdiCtrl](#openwebbeans-apache-org-testing_test-control)**  
Since `Test-Control` is very new it currently lacks support for TestNG and only offers one integration point (MyFaces-Test). If you experience Test-Control
as insufficient the Apache Deltaspike team might propose that you use `Apache Deltaspike CdiCtrl` directly but communicating with them might be a
good idea, please refer to [Deltaspike Community](https://deltaspike.apache.org/community.html) for contact details. `CdiCtrl`
is a powerful framework that Test-Control is built on top off and using it directly is another solution to consider.
This is still common and well documented since Test-Control is a new module. `CdiCtrl` is commonly referred to as the lightweight champion of CDI testing.
In it's nature it's simple and to the point (`Test-Control` even more so).

**[JBoss Arquillian](#openwebbeans-apache-org-testing_arquillian)**  
Crowned the heavyweight champion of CDI testing certainly not all for naught. `Arquillian` offers fine grained control over the deployment and has
many popular extensions for testing. For example testing with a web based user interface is possible. If you want to test very complex combinations in your deployments
or you need to isolate to the bare minimum, then Arquillian really shines. It also offers several xml configuration points and similar that might be needed for large
complicated projects.

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-testing_test-control"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-testing_test-control--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# Testing your application with Apache DeltaSpike Test-Control[¶](#openwebbeans-apache-org-testing_test-control--testing-your-application-with-apache-deltaspike-test-control "Permalink")

## About Test-Control[¶](#openwebbeans-apache-org-testing_test-control--about-test-control "Permalink")

`Test-Control` is *not* part of Apache OpenWebBeans but a module of
[Apache DeltaSpike](https://deltaspike.apache.org). It is based on another Deltaspike module called `CdiCtrl`.
`Test-Control` is available in Deltaspike 0.6 and onwards.

`CdiCtrl` abstracts away all the logic to boot a CDI Container
and controls the life cycle of it's Contexts (Request Context, Session Context, etc).
This module can be extremely powerful for CDI projects both for tests and during runtime.
It's a long time recommendation to use
`CdiCtrl` to write tests with low - medium complexity and we explain how here:
[Testing with cdiCtrl](#openwebbeans-apache-org-testing_cdictrl).

With `Test-Control` the few steps you had to do on your own are taken cared of for you. In a way `Test-Control`
However `Test-Control` though it's fairly new and has no support for TestNG at the moment. It may also be insufficient if you need your own custom
extension points or have other complex demands.

## Adding Deltaspike Test-Control to your project[¶](#openwebbeans-apache-org-testing_test-control--adding-deltaspike-test-control-to-your-project "Permalink")

The following are the dependencies you need in your Apache Maven pom.xml file in addition to
OWB itself:

```xml
<dependency>
    <groupId>org.apache.deltaspike.cdictrl</groupId>
    <artifactId>deltaspike-cdictrl-api</artifactId>
    <version>${deltaspike.version}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.apache.deltaspike.cdictrl</groupId>
    <artifactId>deltaspike-cdictrl-owb</artifactId>
    <version>${deltaspike.version}</version>
    <scope>test</scope>
</dependency>

<dependency>
    <groupId>org.apache.deltaspike.cdictrl</groupId>
    <artifactId>deltaspike-test-control-module-api</artifactId>
    <version>${deltaspike.version}</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.apache.deltaspike.cdictrl</groupId>
    <artifactId>deltaspike-test-control-module-impl</artifactId>
    <version>${deltaspike.version}</version>
    <scope>test</scope>
</dependency>
```

Note that deltaspike-cdictrl-openejb can substitute the deltaspike-cdictrl-owb dependency if you are using TomEE.

## Why use Test-Control for your unit tests?[¶](#openwebbeans-apache-org-testing_test-control--why-use-test-control-for-your-unit-tests "Permalink")

Test-Control offers the strong testing capabilities of `CdiCtrl`
but let's you focus on writing the actual tests alone. `Test-Control` is the new lightweight champion for testing CDI and the required setup is very minimal.
The testing flow is intuitive and follows the principles of other test runners.

## Examples and Getting started[¶](#openwebbeans-apache-org-testing_test-control--examples-and-getting-started "Permalink")

This information can be found here [Deltaspike documentation for Test-Control](https://deltaspike.apache.org/test-control.html)

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-meecrowave-community"></a>

# Meecrowave :: the customizable server

# [ Meecrowave ](#openwebbeans-apache-org-meecrowave-index)

# Community

[ Download as PDF](/meecrowave/community.pdf)

For now please use OpenWebBeans mailing-lists, JIRA and IRC channel.

See [OpenWebBeans Community](#openwebbeans-apache-org-community) page for more details.

## Source code

Source code can be found at [https://gitbox.apache.org/repos/asf/openwebbeans-meecrowave.git](https://gitbox.apache.org/repos/asf/openwebbeans-meecrowave.git)

```none
$> git clone https://gitbox.apache.org/repos/asf/openwebbeans-meecrowave.git
```

A mirror is available on github at [https://github.com/apache/openwebbeans-meecrowave](https://github.com/apache/openwebbeans-meecrowave).

The github mirror is linked in both directions.
That means it doesn’t matter whether you push to GitHub or Apache GitBox.

### Build it

To build the project just run maven:

```none
mvn clean install
```

## Example Source Code

The Apache Meecrowave community also hosts some examples which show how Meecrowave is to be used.
Those examples are hosted in a separate repository and can be found at [https://github.com/apache/openwebbeans-meecrowave-examples](https://github.com/apache/openwebbeans-meecrowave-examples)

The github repo works fine for pull requests.
Please use our official [Meecrowave Bug Tracker](https://issues.apache.org/jira/projects/MEECROWAVE) for reporting bugs or enhancement requests.

The cannonical repository hosted directly at the Apache Software Foundation intended to be used by committers is

```none
git clone https://gitbox.apache.org/repos/asf/openwebbeans-meecrowave-examples.git
```

- [Home](#openwebbeans-apache-org-meecrowave-index)
- [Quick Start](#openwebbeans-apache-org-meecrowave-start)
- [Components](#openwebbeans-apache-org-meecrowave-components)
- [Download](#openwebbeans-apache-org-meecrowave-download)
- [Community](#openwebbeans-apache-org-meecrowave-community)

Copyright © 2016-2020
[The Apache Software Foundation](http://www.apache.org/). All rights reserved.

Designed with  by [Xiaoying Riley](http://themes.3rdwavemedia.com/) for developers

---

<a id="openwebbeans-apache-org-meecrowave-components"></a>

# Meecrowave :: the customizable server

# [ Meecrowave ](#openwebbeans-apache-org-meecrowave-index)

# Components

[ Download as PDF](/meecrowave/components.pdf)

## Meecrowave Core

Core component is the backbone of Meecrowave. It is based on Tomcat embedded for
Servlet container, CXF for JAX-RS, OpenWebBeans for CDI and Log4j2 for the logging.

[Read about Meecrowave configuration](#openwebbeans-apache-org-meecrowave-meecrowave-core-configuration)

[Read about Meecrowave command line](#openwebbeans-apache-org-meecrowave-meecrowave-core-cli)

[Read about Meecrowave and webapp/wars](#openwebbeans-apache-org-meecrowave-meecrowave-core-deploy-webapp)

## Meecrowave JPA

Meecrowave JPA provides a thin layer on top of JPA to make it easier to use JPA
without requiring to use a full container like JavaEE or Spring. It is just a
CDI extension.

[Read More](#openwebbeans-apache-org-meecrowave-meecrowave-jpa-index)

## Meecrowave Maven

Meecrowave provides a Maven plugin to run meecrowave with your preferred build tool.

[Read More](#openwebbeans-apache-org-meecrowave-meecrowave-maven-index)

## Meecrowave Gradle

Meecrowave provides a Gradle plugin to run meecrowave with your preferred build tool.

[Read More](#openwebbeans-apache-org-meecrowave-meecrowave-gradle-index)

## Meecrowave and the Testing

Meecrowave provides two main testing integration: a JUnit one and an Arquillian Container.

[Read More](#openwebbeans-apache-org-meecrowave-testing-index)

## Meecrowave and Monitoring

For monitoring, [Microprofile](https://microprofile.io/) can be a neat companion of Apache Meecrowave.
You can have a look to [Geronimo](http://geronimo.apache.org/microprofile/) implementation.

## Meecrowave and JTA

This is an experimental integration of geronimo-transaction and meecrowave.

[JTA module](#openwebbeans-apache-org-meecrowave-meecrowave-jta-index)

## Meecrowave and OAuth2

This is an experimental module integrating CXF OAuth2 server in Meecrowave
through an embeddable dependency or a directly executable jar.

[OAuth2 module](#openwebbeans-apache-org-meecrowave-meecrowave-oauth2-index)

## Meecrowave Let’s Encrypt

This is an experimental module integrating with Let’s Encrypt to provide you
free and easy SSL support on your HTTPS connectors.

[Let’s Encrypt module](#openwebbeans-apache-org-meecrowave-meecrowave-letsencrypt-index)

## Meecrowave Websocket

This is an experimental module wrapping `tomcat-websocket` to make it CDI friendly for server endpoints.

[Websocket module](#openwebbeans-apache-org-meecrowave-meecrowave-websocket-index)

## Going further

Meecrowave scope is not the full scope of microservices (whatever it means) or at least enterprise needs
cause several Apache projects cover part of them in a very good way.

See [Companion Projects](#openwebbeans-apache-org-meecrowave-companion-projects) for more information.

- [Home](#openwebbeans-apache-org-meecrowave-index)
- [Quick Start](#openwebbeans-apache-org-meecrowave-start)
- [Components](#openwebbeans-apache-org-meecrowave-components)
- [Download](#openwebbeans-apache-org-meecrowave-download)
- [Community](#openwebbeans-apache-org-meecrowave-community)

Copyright © 2016-2020
[The Apache Software Foundation](http://www.apache.org/). All rights reserved.

Designed with  by [Xiaoying Riley](http://themes.3rdwavemedia.com/) for developers

---

<a id="openwebbeans-apache-org-meecrowave-download"></a>

# Meecrowave :: the customizable server

# [ Meecrowave ](#openwebbeans-apache-org-meecrowave-index)

# Downloads

[ Download as PDF](/meecrowave/download.pdf)

License under Apache License v2 (ALv2).

| Name | Version | Date | Size | Type | Links |
| --- | --- | --- | --- | --- | --- |
| Meecrowave Source Release | 1.2.15 | 2022-12-27 22:42:51 | 1 MB 567 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.15 | 2022-12-27 22:44:07 | 10 MB 449 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.15 | 2022-12-27 22:44:04 | 217 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.14 | 2022-04-30 13:03:09 | 1 MB 567 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.14 | 2022-04-30 13:04:19 | 10 MB 358 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.14 | 2022-04-30 13:04:18 | 218 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.13 | 2021-12-14 22:10:42 | 1 MB 567 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.13 | 2021-12-14 22:11:54 | 10 MB 318 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.13 | 2021-12-14 22:11:52 | 218 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.12 | 2021-08-02 07:50:15 | 1 MB 567 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.12 | 2021-08-02 07:51:48 | 10 MB 212 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.12 | 2021-08-02 07:51:38 | 218 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.11 | 2021-04-26 07:52:20 | 1 MB 572 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.11 | 2021-04-26 07:53:14 | 10 MB 222 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.11 | 2021-04-26 07:53:13 | 222 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.10 | 2020-11-12 10:57:09 | 1 MB 559 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.10 | 2020-11-12 10:58:01 | 10 MB 162 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.10 | 2020-11-12 10:58:00 | 220 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.9 | 2019-09-30 08:19:59 | 1 MB 552 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.9 | 2019-09-30 08:21:17 | 9 MB 972 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.9 | 2019-09-30 08:21:05 | 220 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.8 | 2019-05-26 08:49:48 | 1 MB 512 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.8 | 2019-05-26 08:51:12 | 9 MB 796 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.8 | 2019-05-26 08:51:00 | 214 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.7 | 2019-02-25 08:33:35 | 1 MB 507 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.7 | 2019-02-25 08:35:06 | 9 MB 701 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.7 | 2019-02-25 08:34:49 | 218 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.6 | 2019-01-30 08:52:01 | 1 MB 505 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.6 | 2019-01-30 08:53:21 | 9 MB 672 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.6 | 2019-01-30 08:53:07 | 217 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.5 | 2019-01-09 11:17:10 | 1 MB 493 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.5 | 2019-01-09 11:18:33 | 9 MB 621 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.5 | 2019-01-09 11:18:18 | 216 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.4 | 2018-09-21 09:14:38 | 1 MB 466 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.4 | 2018-09-21 09:16:03 | 9 MB 534 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.4 | 2018-09-21 09:15:51 | 202 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.3 | 2018-07-19 09:53:16 | 1 MB 448 kB | zip | zipsha512asc |
| Meecrowave core runner | 1.2.3 | 2018-07-19 09:54:34 | 10 MB 159 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.3 | 2018-07-19 09:54:20 | 199 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.2 | 2018-07-14 07:14:12 | 1 MB 448 kB | zip | zipsha1asc |
| Meecrowave core runner | 1.2.2 | 2018-07-14 07:15:41 | 10 MB 177 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.2 | 2018-07-14 07:15:27 | 199 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.1 | 2018-02-26 21:02:45 | 1 MB 425 kB | zip | zipsha1asc |
| Meecrowave core runner | 1.2.1 | 2018-02-26 21:03:50 | 9 MB 883 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.1 | 2018-02-26 21:03:37 | 192 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.2.0 | 2017-12-20 16:37:49 | 2 MB 767 kB | zip | zipsha1asc |
| Meecrowave core runner | 1.2.0 | 2017-12-20 16:39:33 | 9 MB 839 kB | jar | jarsha1asc |
| Meecrowave core | 1.2.0 | 2017-12-20 16:39:19 | 186 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.1.0 | 2017-09-01 21:09:23 | 1 MB 369 kB | zip | zipsha1asc |
| Meecrowave core runner | 1.1.0 | 2017-09-01 21:10:27 | 9 MB 569 kB | jar | jarsha1asc |
| Meecrowave core | 1.1.0 | 2017-09-01 21:10:15 | 184 kB | jar | jarsha1asc |
| Meecrowave Source Release | 1.0.0 | 2017-07-07 22:27:32 | 1 MB 357 kB | zip | zipsha1asc |
| Meecrowave core runner | 1.0.0 | 2017-07-07 22:28:34 | 9 MB 286 kB | jar | jarsha1asc |
| Meecrowave core | 1.0.0 | 2017-07-07 22:28:22 | 174 kB | jar | jarsha1asc |
| Meecrowave Source Release | 0.3.1 | 2017-04-28 15:34:47 | 1 MB 331 kB | zip | zipsha1asc |
| Meecrowave core runner | 0.3.1 | 2017-04-28 15:35:22 | 9 MB 224 kB | jar | jarsha1asc |
| Meecrowave core | 0.3.1 | 2017-04-28 15:35:13 | 158 kB | jar | jarsha1asc |
| Meecrowave Source Release | 0.3.0 | 2017-02-19 15:56:04 | 1 MB 304 kB | zip | zipsha1asc |
| Meecrowave core runner | 0.3.0 | 2017-02-19 15:56:35 | 9 MB 123 kB | jar | jarsha1asc |
| Meecrowave core | 0.3.0 | 2017-02-19 15:56:26 | 156 kB | jar | jarsha1asc |
| Meecrowave Source Release | 0.2.0 | 2017-01-02 15:11:14 | 1 MB 229 kB | zip | zipsha1asc |
| Meecrowave core runner | 0.2.0 | 2017-01-02 15:12:12 | 9 MB 9 kB | jar | jarsha1asc |
| Meecrowave core | 0.2.0 | 2017-01-02 15:12:02 | 145 kB | jar | jarsha1asc |

- [Home](#openwebbeans-apache-org-meecrowave-index)
- [Quick Start](#openwebbeans-apache-org-meecrowave-start)
- [Components](#openwebbeans-apache-org-meecrowave-components)
- [Download](#openwebbeans-apache-org-meecrowave-download)
- [Community](#openwebbeans-apache-org-meecrowave-community)

Copyright © 2016-2020
[The Apache Software Foundation](http://www.apache.org/). All rights reserved.

Designed with  by [Xiaoying Riley](http://themes.3rdwavemedia.com/) for developers

---

<a id="openwebbeans-apache-org-meecrowave-index"></a>

# Meecrowave :: the customizable server

# [ Meecrowave ](#openwebbeans-apache-org-meecrowave-index)

A light JAX-RS+CDI+JSON server!

## Cause power doesn't mean complicated!

Welcome to Meecrowave website, if you are fed up to need to adapt to a server instead of letting the server adapting to you you are on the right place!

### Quick Start

Deploy a JSON webservice in 5 mn!

[](#openwebbeans-apache-org-meecrowave-start)

### Components

Meecrowave extensions making development and production smoother.

[](#openwebbeans-apache-org-meecrowave-components)

### Downloads and License

Ready to get started? Grab meecrowave and run your services!

[](#openwebbeans-apache-org-meecrowave-download)

### Community

OpenWebBeans community is proud to host Meecrowave project.

[](#openwebbeans-apache-org-meecrowave-community)

Copyright © 2016-2020
[The Apache Software Foundation](http://www.apache.org/). All rights reserved.

Designed with  by [Xiaoying Riley](http://themes.3rdwavemedia.com/) for developers

---

<a id="openwebbeans-apache-org-meecrowave-start"></a>

# Meecrowave :: the customizable server

```xml
<dependency>
  <groupId>org.apache.meecrowave</groupId>
  <artifactId>meecrowave-core</artifactId>
  <version>${meecrowave.version}</version>
</dependency>
```

---

<a id="openwebbeans-apache-org-news"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-news--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# News[¶](#openwebbeans-apache-org-news--news "Permalink")

- 2024-12-11 OpenWebBeans-4.0.3 has been released - this is a Jakarta CDI-4.0 bugfix release
- 2020-01-17 OpenWebBeans-2.0.14 has been released - this is a CDI-2.0 bugfix release
- 2018-11-12 OpenWebBeans-2.0.8 has been released - this is a CDI-2.0 bugfix release
- 2017-09-03 OpenWebBeans-2.0.1 has been released - this is a CDI-2.0 bugfix release
- 2017-07-16 OpenWebBeans-2.0.0 has been released - this is our first CDI-2.0 release!
- 2017-07-11 Meecrowave-1.0.0 has been released
- 2017-07-11 OpenWebBeans-1.7.4 has been released - this is a CDI-1.2 bugfix release
- 2017-04-19 OpenWebBeans-1.7.3 has been released - this is a CDI-1.2 bugfix release
- 2017-02-19 OpenWebBeans-1.7.2 has been released - this is a CDI-1.2 bugfix release
- 2012-12-12 OpenWebBeans-1.1.7 has been released - this is a CDI-1.0 bugfix release
- 2012-12-03 We switched CDI-1.0 development to the owb\_1.1.x branch and started with implementing CDI-1.1 in trunk
- 2012-10-01 OpenWebBeans 1.1.6 has been released - this is a CDI-1.0 bugfix release
- 2011-09-30 - New Committer: Jean-Louis Monteiro
- 2011-09-15 - New Committer: Thomas Andraschko
- 2012-08-16 OpenWebBeans 1.1.5 has been released - this is a CDI-1.0 bugfix release
- 2012-04-11 OpenWebBeans 1.1.4 has been released - this is a CDI-1.0 bugfix release
- 2012-03-12 - New Committer: Romain Manni-Bucau
- 2012-01-08 - New Committer: Martin Koci
- 2011-12-10 OpenWebBeans 1.1.3 has been released - this is a CDI-1.0 bugfix release
- 2011-10-17 OpenWebBeans 1.1.2 has been released - this is a CDI-1.0 bugfix release
- 2011-10-04 Apache TomEE release includes OWB as CDI container
- 2011-09-29 - Trademark Rules & Logo download page added
- 2011-08-05 - New Committer: Arne Limburg
- 2011-08-04 - Added backward compat support for Java5 for running OWB with older EE containers
- 2001-05-19 - We got a new Logo, designed by Adonis Raduca and sponsored by Irian.at
- 2011-04-09 - New PMC members: Gerhard Petracek, David Jencks
- 2011-03-30 - 1.1.0 released
- 2010-11-07 - New Committer: David Jencks
- 2010-10-08 - New Committers: Paul J Reder, Rohit Kelapure
- 2010-09-03 - alpha-2 released
- 2010-08-03 - New committer: Gerhard Petracek
- 2010-07-10 - alpha-1 released
- 2010-03-05 - M4 released
- 2010-03-05 - New committer: Ying Wang
- 2009-11-17 - New committer: Eric Covener
- 2009-11-10 - New committer: Joe Bergmark
- 2009-09-30 - M3 is released
- 2009-09-14 - New committer: David Blevins
- 2009-07-07 - New committer: James Carman
- 2009-06-09 - M2 is released
- 2009-02-16 - M1 is released
- 2009-01-18 - New committer: Mark Struberg
- 2008-12-24 - New committer: Mohammad Nour El-Din
- 2008-11-23 - Source code is available in project SVN.

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-release-checklist"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-release-checklist--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans Release Checklist[¶](#openwebbeans-apache-org-release-checklist--openwebbeans-release-checklist "Permalink")

Before performing the release you need to configure your environment if you haven't done it before.

1. Publishing Maven Artifacts ([https://www.apache.org/dev/publishing-maven-artifacts.html](https://www.apache.org/dev/publishing-maven-artifacts.html))
2. Go to the section SETUP YOUR DEVELOPMENT ENVIRONMENT and generate the pgp key signature. Don't forget to distribute the public key step.  
   Generate PGP signature: [https://blog.sonatype.com/2010/01/how-to-generate-pgp-signatures-with-maven/#.Vm9Km8q22-q](https://blog.sonatype.com/2010/01/how-to-generate-pgp-signatures-with-maven/#.Vm9Km8q22-q)

## Prepare[¶](#openwebbeans-apache-org-release-checklist--prepare "Permalink")

1. JIRA Release Management

   - Make sure that all tickets for the release are properly marked as 'released' in our issue tracker.
   - Create ${version.next} (if not already done)
   - Move unresolved issues from ${version} (e.g. 2.0.8) to ${version.next} (e.g. 2.0.9)
2. Update README

   - Export changelog from JIRA
   - add it to readme/README.txt
   - also update the version numbers in the preface
3. Release via MVN

   - mvn clean install -Papache-release
   - mvn release:prepare -DdryRun=true
   - mvn release:prepare -Dresume=false
   - mvn release:perform

   if the release:perform fails on the last step (commit the new version in trunk) but the tag was successfully created,
   you can deploy the tag manually to nexus:

   - commit the version change in trunk
   - checkout the tag and execute: mvn clean install deploy -Papache-release
4. Provide the staging repository

   - login to nexus: [https://repository.apache.org/#stagingRepositories](https://repository.apache.org/#stagingRepositories)
   - select the staging repository and "close" it
   - the final url should be similar to [https://repository.apache.org/content/repositories/orgapacheopenwebbeans-1047](https://repository.apache.org/content/repositories/orgapacheopenwebbeans-1047)
5. Provide assembly

   - SVN [https://dist.apache.org/repos/dist/dev/openwebbeans/](https://dist.apache.org/repos/dist/dev/openwebbeans/)
   - Commit following files inside the ${version} (2.0.8) directory:
     - openwebbeans-2.0.8-source-release.zip
     - openwebbeans-2.0.8-source-release.zip.asc
     - openwebbeans-2.0.8-source-release.zip.sha512
     - openwebbeans-distribution-2.0.8-binary.tar.gz
     - openwebbeans-distribution-2.0.8-binary.tar.gz.asc
     - openwebbeans-distribution-2.0.8-binary.tar.gz.sha512 (must be generated manually)
     - openwebbeans-distribution-2.0.8-binary.zip
     - openwebbeans-distribution-2.0.8-binary.zip.asc
     - openwebbeans-distribution-2.0.8-binary.zip.sha512 (must be generated manually)

   OR

   - announce the VOTE with links to the staging repo
     In this case you MUST provide at least the sha1 of the source-release.zip!
6. Send the VOTE mail

## After successful vote[¶](#openwebbeans-apache-org-release-checklist--after-successful-vote "Permalink")

1. Release from the staging repository

   - login to nexus: [https://repository.apache.org/#stagingRepositories](https://repository.apache.org/#stagingRepositories)
   - select the staging repository and "release" it
   - the artifacts will be synced with the maven repos now
2. JIRA Release Management

   - Select the version and release it
   - bulk-transition all resolved tickets of ${version} to 'closed'
3. Upload assembly

   - SVN [https://dist.apache.org/repos/dist/release/openwebbeans/](https://dist.apache.org/repos/dist/release/openwebbeans/)
   - commit same files as in 5) under the directory for the ${version} (2.0.8)
   - delete ${version.old} (2.0.7) directory
4. Apache Reporter Service

   - wait for the mail
   - login to [https://reporter.apache.org/addrelease.html?openwebbeans](https://reporter.apache.org/addrelease.html?openwebbeans)
   - add the ${version} and release date

## After the artifacts has been synced to central maven repo[¶](#openwebbeans-apache-org-release-checklist--after-the-artifacts-has-been-synced-to-central-maven-repo "Permalink")

1. Create blog

   - Login to [https://blogs.apache.org/owb/](https://blogs.apache.org/owb/)
   - Add a post for the new release, this will automatically picked up by the site deployment
2. Update site

   - Browse [https://github.com/apache/openwebbeans-site/tree/main/content](https://github.com/apache/openwebbeans-site/tree/main/content)
   - Edit news.md
   - Edit download.md
   - Committing will cause the site to be published
3. Send release mail
4. Twitter

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-samples"></a>

# Apache OpenWebBeans

[![owb_logo_small](openwebbeans.apache.org/resources/images/logo_dbg-small.png)](#openwebbeans-apache-org-index)

- [Home](#openwebbeans-apache-org-index)
- [Documentation](#openwebbeans-apache-org-documentation)
- [Source](#openwebbeans-apache-org-source)
- [Download](#openwebbeans-apache-org-download)
- [Community](#openwebbeans-apache-org-community)
- [Misc](#openwebbeans-apache-org-samples--)
  - [Apache Home](https://www.apache.org)
  - [Contact](#openwebbeans-apache-org-misc-contact)
  - [Legal](#openwebbeans-apache-org-misc-legal)
  - [Sponsorship](https://www.apache.org/foundation/sponsorship.html)
  - [Thanks](https://www.apache.org/foundation/thanks.html)

# OpenWebBeans Samples[¶](#openwebbeans-apache-org-samples--openwebbeans-samples "Permalink")

Coming soon...

---

[![asf_feather](https://www.apache.org/images/asf_logo_wide.png)](https://www.apache.org)

Copyright © 2008-2025 The Apache Software Foundation, Licensed under the Apache License, Version 2.0.

OpenWebBeans, Apache, and the Apache feather logo are trademarks of The Apache Software Foundation.

---

<a id="openwebbeans-apache-org-meecrowave-companion-projects"></a>

# Meecrowave :: the customizable server

# [ Meecrowave ](#openwebbeans-apache-org-meecrowave-index)

# Companion projects

[ Download as PDF](/meecrowave/companion-projects.pdf)

## Apache DeltaSpike

Apache DeltaSpike will cover several useful areas:

- Configuration
- Exception handling
- Advanced CDI utilities like partial bean binding (you define a bean behavior from a CDI handler implementation)
- Quartz integration
- Data/JPA integration (it is different than the Meecrowave one but depending your need can be useful)
- And much more

See [http://deltaspike.apache.org/](http://deltaspike.apache.org/) for more information.

## Apache Sirona

Sirona aims to provide some monitoring capabilities (metrics on the runtime).
By default its servlet integration makes it smooth to integrate with Meecrowave.

See [http://sirona.apache.org/](http://sirona.apache.org/) for more information.

- [Home](#openwebbeans-apache-org-meecrowave-index)
- [Quick Start](#openwebbeans-apache-org-meecrowave-start)
- [Components](#openwebbeans-apache-org-meecrowave-components)
- [Download](#openwebbeans-apache-org-meecrowave-download)
- [Community](#openwebbeans-apache-org-meecrowave-community)

Copyright © 2016-2020
[The Apache Software Foundation](http://www.apache.org/). All rights reserved.

Designed with  by [Xiaoying Riley](http://themes.3rdwavemedia.com/) for developers

---

<a id="openwebbeans-apache-org-meecrowave-howto"></a>

# Meecrowave :: the customizable server

```xml
<dependency>
    <groupId>org.apache.meecrowave</groupId>
    <artifactId>meecrowave-specs-api</artifactId>
    <version>${meecrowave.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.meecrowave</groupId>
    <artifactId>meecrowave-core</artifactId>
    <version>${meecrowave.version}</version>
</dependency>

<!-- if you intend to have unit tests (you really should) -->
<dependency>
    <groupId>org.apache.meecrowave</groupId>
    <artifactId>meecrowave-junit</artifactId>
    <version>${meecrowave.version}</version>
    <scope>test</scope>
</dependency>
```

---

<a id="openwebbeans-apache-org-meecrowave-meecrowave-core-cli"></a>

# Meecrowave :: the customizable server

# [ Meecrowave ](#openwebbeans-apache-org-meecrowave-index)

# Meecrowave Command Line Interface

[ Download as PDF](/meecrowave/meecrowave-core/cli.pdf)

Meecrowave provides a CLI (Command Line Interface) called `org.apache.meecrowave.runner.Cli`.

It can be used to deploy the java classpath or a war. Here are the main options:

| Name | Description |
| --- | --- |
| --tomcat-antiresourcelocking | Should Tomcat anti resource locking feature be activated on StandardContext. |
| --cdi-conversation | Should CDI conversation be activated |
| --client-auth | HTTPS keystore client authentication |
| --conf | Conf folder to synchronize |
| --connector | Custom connectors |
| --tomcat-context-configurer | Configurers for all webapps. The Consumer<Context> instances will be applied to all deployments. |
| --cxf-servlet-params | Init parameters passed to CXF servlet |
| --default-ssl-hostconfig-name | The name of the default SSLHostConfig that will be used for secure https connections. |
| --delete-on-startup | Should the directory be cleaned on startup if existing |
| --dir | Root folder if provided otherwise a fake one is created in tmp-dir |
| --host | Default host |
| --http2 | Activate HTTP 2 |
| --http | HTTP port |
| --https | HTTPS port |
| --cxf-initialize-client-bus | Should the client bus be set. If false the server one will likely be reused. |
| --servlet-container-initializer | ServletContainerInitializer instances. |
| --servlet-container-initializer-injection | Should ServletContainerInitialize support injections. |
| --jaxrs-beanvalidation | Should bean validation be activated on JAX-RS endpoint if present in the classpath. |
| --jaxrs-default-providers | If jaxrsProviderSetup is true the list of default providers to load (or defaulting to johnson jsonb and jsonp ones) |
| --jaxrs-log-provider | Should JAX-RS providers be logged |
| --jaxrs-mapping | Default jaxrs mapping |
| --jaxrs-provider-setup | Should default JAX-RS provider be configured |
| --jaxws-support-if-present | Should @WebService CDI beans be deployed if cxf-rt-frontend-jaxws is in the classpath. |
| --jsonb-binary-strategy | Should JSON-B provider prettify the output |
| --jsonb-encoding | Which encoding provider JSON-B should use |
| --jsonb-ijson | Should JSON-B provider comply to I-JSON |
| --jsonb-naming-strategy | Should JSON-B provider prettify the output |
| --jsonb-nulls | Should JSON-B provider serialize nulls |
| --jsonb-order-strategy | Should JSON-B provider prettify the output |
| --jsonb-prettify | Should JSON-B provider prettify the output |
| --jsonp-buffer-strategy | JSON-P JAX-RS provider buffer strategy (see johnzon) |
| --jsonp-read-buffer-length | JSON-P JAX-RS provider read buffer limit size (see johnzon) |
| --jsonp-max-string-length | JSON-P JAX-RS provider max string limit size (see johnzon) |
| --jsonp-write-buffer-length | JSON-P JAX-RS provider write buffer limit size (see johnzon) |
| --jsonp-supports-comment | Should JSON-P JAX-RS provider prettify the outputs (see johnzon) |
| --jsonp-supports-comment | Should JSON-P JAX-RS provider support comments (see johnzon) |
| --keep-server-xml-as-this | Don’t replace ports in server.xml |
| --keystore-alias | HTTPS keystore alias |
| --keystore-file | HTTPS keystore location |
| --keystore-password | HTTPS keystore password |
| --keystore-type | HTTPS keystore type |
| --logging-global-setup | Should logging be configured to use log4j2 (it is global) |
| --login-config | web.xml login config |
| --meecrowave-properties | Loads a meecrowave properties, defaults to meecrowave.properties. |
| --pid-file | A file path to write the process id if the server starts |
| --properties | Passthrough properties |
| --quick-session | Should an unsecured but fast session id generator be used |
| --realm | realm |
| --roles | In memory roles |
| --scanning-exclude | A forced exclude list of jar names (comma separated values) |
| --scanning-include | A forced include list of jar names (comma separated values) |
| --scanning-package-exclude | A forced exclude list of packages names (comma separated values) |
| --scanning-package-include | A forced include list of packages names (comma separated values) |
| --security-constraint | web.xml security constraint |
| --server-xml | Provided server.xml |
| --shared-libraries | A folder containing shared libraries. |
| --skip-http | Skip HTTP connector |
| --ssl | Use HTTPS |
| --ssl-protocol | HTTPS protocol |
| --stop | Shutdown port if used or -1 |
| --tmp-dir | Temporary directory |
| --tomcat-access-log-pattern | Activates and configure the access log valve. Value example: '%h %l %u %t "%r" %s %b "%{Referer}i" "%{User-Agent}i"' |
| --tomcat-default-setup | Add default servlet |
| --tomcat-filter | A Tomcat JarScanFilter |
| --tomcat-default-setup-jsp-development | Should JSP support if available be set in development mode |
| --tomcat-skip-jmx | (Experimental) Should Tomcat MBeans be skipped. |
| --tomcat-scanning | Should Tomcat scanning be used (@HandleTypes, @WebXXX) |
| --tomcat-wrap-loader | (Experimental) When deploying a classpath (current classloader), should meecrowave wrap the loader to define another loader identity but still use the same classes and resources. |
| --log4j2-jul-bridge | Should JUL logs be redirected to Log4j2 - only works before JUL usage. |
| --use-shutdown-hook | Use shutdown hook to automatically stop the container on Ctrl+C |
| --tomcat-default | Should Tomcat default be set (session timeout, mime mapping etc…​) |
| --users | In memory users |
| --watcher-bouncing | Activate redeployment on directories update using this bouncing. |
| --web-resource-cached | Cache web resources |
| --web-session-cookie-config | Force the cookie-config, it uses a properties syntax with the keys being the web.xml tag names. |
| --web-session-timeout | Force the session timeout for webapps |
| --web-xml | Global web.xml |
| --help | Show the CLI help/usage |
| --context | The context to use to deploy the webapp |
| --webapp | Location of the webapp, if not set the classpath will be deployed |
| --docbase | Location of the docbase for a classpath deployment |

Note that `help` command is supported as well.

## Extending the CLI

You can add your own CLI options implementing `org.apache.meecrowave.runner.Cli$Options`
(just a marker interface, no logic to code) and use `@CliOption` to define fields
as coming from the CLI arguments. To register your option bean just add it in `META-INF/services/org.apache.meecrowave.runner.Cli$Options`.

|  | Meecrowave.Builderprovides agetExtension(type)which can be used to get this kind of extension. This is common and works in all modes (arquillian, JUnit, embedded etc…​) replacing the arguments by properties onMeecrowave.Builderinstance. |
| --- | --- |

- [Home](#openwebbeans-apache-org-meecrowave-index)
- [Quick Start](#openwebbeans-apache-org-meecrowave-start)
- [Components](#openwebbeans-apache-org-meecrowave-components)
- [Download](#openwebbeans-apache-org-meecrowave-download)
- [Community](#openwebbeans-apache-org-meecrowave-community)

Copyright © 2016-2020
[The Apache Software Foundation](http://www.apache.org/). All rights reserved.

Designed with  by [Xiaoying Riley](http://themes.3rdwavemedia.com/) for developers

---

<a id="openwebbeans-apache-org-meecrowave-meecrowave-core-configuration"></a>

# Meecrowave :: the customizable server

```java
new Meecrowave(new Builder() {{
        randomHttpPort();
        setTomcatScanning(false);
        setTomcatAutoSetup(false);
        setRealm(new JAASRealm());
        user("admin", "secret");
     }})
    .bake()
    .await();
```

---

<a id="openwebbeans-apache-org-meecrowave-meecrowave-core-deploy-webapp"></a>

# Meecrowave :: the customizable server

# [ Meecrowave ](#openwebbeans-apache-org-meecrowave-index)

# Meecrowave and webapps

[ Download as PDF](/meecrowave/meecrowave-core/deploy-webapp.pdf)

Meecrowave is a development enabler and simplifier thanks to its classpath deployment. However it is
still a plain Apache Tomcat and you can deploy existing webapp you developed with no particular constraint.

From now on, we will assume you have a Servlet or Spring webapp `myapp.war`.

## Deployment with a Meecrowave bundle

This part assumed you built a bundle with [Meecrowave Maven Plugin](#openwebbeans-apache-org-meecrowave-meecrowave-maven-index). It gives
you a zip which has a tomcat layout once exploded:

```none
.
|- bin
|- conf
|- logs
`- lib
```

If you didn’t package a webapp at bundle time you can create a `webapps` folder in this layout and add your war inside.
Then to launch this war you can either use a `server.xml` in `conf` and add as in any Tomcat your `<Context />` in it
or you can launch it directly using:

```bash
./bin/meecrowave.sh run --webapp=webapps/myapp.war
```

## Deployment with the runner

If you prefer to use the runner you can deploy a war with the following command:

```bash
java -jar meecrowave-core-runner.jar --webapp=webapps/myapp.war
```

## Going further

You can find more information about deployment checking out the [CLI](#openwebbeans-apache-org-meecrowave-meecrowave-core-cli) documentation
which presents all options of the several ways to launch Meecrowave.

- [Home](#openwebbeans-apache-org-meecrowave-index)
- [Quick Start](#openwebbeans-apache-org-meecrowave-start)
- [Components](#openwebbeans-apache-org-meecrowave-components)
- [Download](#openwebbeans-apache-org-meecrowave-download)
- [Community](#openwebbeans-apache-org-meecrowave-community)

Copyright © 2016-2020
[The Apache Software Foundation](http://www.apache.org/). All rights reserved.

Designed with  by [Xiaoying Riley](http://themes.3rdwavemedia.com/) for developers

---

<a id="openwebbeans-apache-org-meecrowave-meecrowave-gradle-index"></a>

# Meecrowave :: the customizable server

```groovy
buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        classpath "org.apache.meecrowave:meecrowave-gradle-plugin:${meecrowave.version}"
    }
}

group 'com.app'
version '1.0-SNAPSHOT'

apply plugin: 'java'
apply plugin: 'org.apache.meecrowave'

meecrowave {
    httpPort = 9090
    // most of the meecrowave core configuration
}
```

---

<a id="openwebbeans-apache-org-meecrowave-meecrowave-jpa-index"></a>

# Meecrowave :: the customizable server

```java
@ApplicationScoped
public class JpaConfig {
    @Produces
    public PersistenceUnitInfoBuilder unit(final DataSource ds) {
        return new PersistenceUnitInfoBuilder()
                .setUnitName("test")
                .setDataSource(ds)
                .setExcludeUnlistedClasses(true)
                .addManagedClazz(User.class)
                .addProperty("openjpa.RuntimeUnenhancedClasses", "supported")
                .addProperty("openjpa.jdbc.SynchronizeMappings", "buildSchema");
    }
}
```

---

<a id="openwebbeans-apache-org-meecrowave-meecrowave-jta-index"></a>

# Meecrowave :: the customizable server

```xml
<dependency>
  <groupId>org.apache.meecrowave</groupId>
  <artifactId>meecrowave-jta</artifactId>
  <version>${meecrowave.version}</version>
</dependency>
```

---

<a id="openwebbeans-apache-org-meecrowave-meecrowave-letsencrypt-index"></a>

# Meecrowave :: the customizable server

# [ Meecrowave ](#openwebbeans-apache-org-meecrowave-index)

# Meecrowave Let’s Encrypt Extension

[ Download as PDF](/meecrowave/meecrowave-letsencrypt/index.pdf)

Meecrowave provides a Let’s Encrypt integration which will grab the default tomcat connector
and reload regularly its certificate based on Let’s Encrypt protocol.

You must configure the domain(s) to include during Let’s Encrypt discussion to activate that feature.

| Name | Description |
| --- | --- |
| --letsencrypt-certificate-domain-location | Where the domain certificate must be stored |
| --letsencrypt-chain-domain-location | Where the domain chain must be stored |
| --letsencrypt-key-domain-location | Where the domain key must be stored |
| --letsencrypt-key-domain-size | Domain key size |
| --letsencrypt-domains | Comma separated list of domains to manage |
| --letsencrypt-endpoint | Endpoint to use to get the certificates |
| --letsencrypt-refresh-interval | Number of second between let’sencrypt refreshes |
| --letsencrypt-retry-count | How many retries to do |
| --letsencrypt-retry-timeout-ms | How long to wait before retrying to get the certificate, default is 3s |
| --letsencrypt-endpoint-staging | Ignore if endpoint is set, otherwise it set the endpoint accordingly |
| --letsencrypt-key-user-location | Where the user key must be stored |
| --letsencrypt-key-user-size | User key size |

- [Home](#openwebbeans-apache-org-meecrowave-index)
- [Quick Start](#openwebbeans-apache-org-meecrowave-start)
- [Components](#openwebbeans-apache-org-meecrowave-components)
- [Download](#openwebbeans-apache-org-meecrowave-download)
- [Community](#openwebbeans-apache-org-meecrowave-community)

Copyright © 2016-2020
[The Apache Software Foundation](http://www.apache.org/). All rights reserved.

Designed with  by [Xiaoying Riley](http://themes.3rdwavemedia.com/) for developers

---

<a id="openwebbeans-apache-org-meecrowave-meecrowave-maven-index"></a>

# Meecrowave :: the customizable server

```xml
<plugin>
  <groupId>org.apache.meecrowave</groupId>
  <artifactId>meecrowave-maven-plugin</artifactId>
  <version>${meecrowave.version}</version>
</plugin>
```

---

<a id="openwebbeans-apache-org-meecrowave-meecrowave-oauth2-index"></a>

# Meecrowave :: the customizable server

```xml
<dependency>
  <groupId>org.apache.meecrowave</groupId>
  <artifactId>meecrowave-oauth2</artifactId>
  <version>${meecrowave.version}</version>
</dependency>
```

---

<a id="openwebbeans-apache-org-meecrowave-meecrowave-websocket-index"></a>

# Meecrowave :: the customizable server

```xml
<dependency>
  <groupId>org.apache.meecrowave</groupId>
  <artifactId>meecrowave-websocket</artifactId>
  <version>${meecrowave.version}</version>
</dependency>
```

---

<a id="openwebbeans-apache-org-meecrowave-testing-index"></a>

# Meecrowave :: the customizable server

```xml
<dependency>
  <groupId>org.apache.meecrowave</groupId>
  <artifactId>meecrowave-junit</artifactId>
  <version>${meecrowave.version}</version>
  <scope>test</scope>
</dependency>
```