<a id="mina-apache-org-mina-project-documentation"></a>

# Documentation — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-documentation--Navigation-Upcoming"></a>Upcoming

- No event

# Documentation

The MINA 2.X User Guide can be found here : [User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

- [Java requirement](#mina-apache-org-mina-project-documentation--java-requirement)
- [Presentation Materials](#mina-apache-org-mina-project-documentation--presentation-materials)
- [Versions & References](#mina-apache-org-mina-project-documentation--versions--references)
- [Tutorials](#mina-apache-org-mina-project-documentation--tutorials)
  - [For Developers](#mina-apache-org-mina-project-documentation--for-developers)
- [Examples](#mina-apache-org-mina-project-documentation--examples)
- [Older Presentation Materials](#mina-apache-org-mina-project-documentation--older-presentation-materials)

## Java requirement

**MINA 2.X** branches can all be used with **Java version 8**.

In order to be able to build **MINA**, you must use **Java version 11** at least

## Presentation Materials

These presentation materials will help you understand the overall architecture and core constructs of MINA

- [MINA in real life (ApacheCon EU 2009)](resources/Mina_in_real_life_ASEU-2009.pdf) by Emmanuel Lécharny
- [Rapid Network Application Development with Apache MINA (JavaOne 2008)](resources/JavaOne2008.pdf) by Trustin Lee
- [Apache MINA - The High Performance Protocol Construction Toolkit (ApacheCon US 2007)](resources/ACUS2007.pdf) by Peter Royal
- [Introduction to MINA (ApacheCon Asia 2006)](resources/ACAsia2006.pdf) by Trustin Lee

## Versions & References

There are currently four branches in MINA:

| JavaDoc | Source Code | Description |
| --- | --- | --- |
| 2.0.X | main,test | The 2.0 recommended production-ready branch |
| 2.1.X | main,test | The 2.1 recommended production-ready branch |
| [2.2.X]http://mina.apache.org/mina-project/gen-docs/latest-2.2/apidocs/index.html) | main,test | The new 2.2 recommended production-ready branch |
| 3.0 | trunk | A defunct branch that we worked on years ago as a attempt of a complete rewrite |

You might also want to read the [frequently asked questions](faq.html] and learn how to [contact us](../contact.html) before getting started.

## Tutorials

- [MINA v2.0 Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide) - Create your first MINA based program using MINA version 2.0
- [Logging Configuration](#mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter) - Configuring your MINA-based application for logging
- Transport-specific Configuration
  - [Serial Tutorial](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-2-serial-transport) - Serial communications with MINA trunk
  - [UDP Tutorial](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports-index) - Writing a User Datagram Protocol (UDP) client and server using MINA
  - [APR Transport](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-1-apr-transport) - Describes use of APR Transport with MINA
- [Integrating with Spring](#mina-apache-org-mina-project-userguide-ch17-spring-integration-ch17-spring-integration) - Demonstrates how to integrate MINA application with Spring
- [Codec Repository](#mina-apache-org-mina-project-codec-repo) - Links to available codec implementations for MINA
- Advanced Topic
  - [Writing IoFilter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters) - Writing your own *IoFilter* implementation to deal with cross-cutting concerns
  - [Writing Protocol Codec for MINA 2.x](#mina-apache-org-mina-project-userguide-ch9-codec-filter-ch9-codec-filter) - Implementing a protocol codec for separation of concern
  - [Using an Executor Filter](#mina-apache-org-mina-project-userguide-ch10-executor-filter-ch10-executor-filter) - Controlling the size of thread pool and choosing the right thread model
  - [JMX Integration](#mina-apache-org-mina-project-userguide-ch16-jmx-support-ch16-jmx-support) - Making your network application manageable
  - [Introduction to mina-statemachine](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine) - Implementing state machine based MINA applications using Java5 annotations
- [User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc) - The new **MINA 2.0** User Guide.

### For Developers

- [Developer Guide](#mina-apache-org-mina-project-developer-guide) - Building & deploying MINA, Coding Standard, and more

## Examples

You can browse all examples [here](http://mina.apache.org/mina-project/gen-docs/latest-2.0/xref/org/apache/mina/example/).

| Name | Feature it demonstrates | Side |
| --- | --- | --- |
| Reverser | Text protocol based on a protocol codec | Server |
| SumUp server | Complex binary protocol based on a protocol codec | Both |
| Echo server | Low-level I/O and SSL | Server |
| NetCat | Client programming | Client |
| HTTP server | Stream-based synchronous I/O | Server |
| Tennis | In-VM pipe communication | Both |
| Chat server | Spring integration | Both |
| Proxy | Resending received bytes on another session. | Both |

## Older Presentation Materials

- [Building TCP/IP Servers with Apache MINA (ApacheCon EU 2007)](resources/ACEU2007.pdf) by Peter Royal
- [Building TCP/IP Servers with Apache MINA (ApacheCon EU 2006)](resources/ACEU2006.pdf) by Peter Royal
- [Introduction to MINA (ApacheCon US 2005)](resources/ACUS2005.pdf) by Trustin Lee ([Demo movie](resources/ACUS2005.swf))

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-2-1-vs-2-0"></a>

# MINA 2.1.x vs MINA 2.0.x — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-2-1-vs-2-0--Navigation-Upcoming"></a>Upcoming

- No event

# 2.1.x vs 2.0.x differences

The way an application is informed that a session has been secured (ie, the SSL/TLS handshake has been successfully completed) was to use a notification system. A specific message was pushed for that purpose, up to the client application to check that message. It forced the application implementer to inject a special session attribute (*SslFilter.USE\_NOTIFICATION*), which is a bit heavy.

It was decided to change that and make it easier for the application to get this information. The idea was to add a new event being propagated to the *IoHandler* interface, up to the application to react to such an event or not.

## Secure session detection in Apache MINA 2.0.x

The following application code is to be used if the application needs to know if the session has been secured or not:

```java
connector.setHandler(new IoHandlerAdapter() {
    @Override
    public void sessionCreated(IoSession session) throws Exception {
        // Add the SSL notification in the session's attribute liste
        session.setAttribute(SslFilter.USE_NOTIFICATION, Boolean.TRUE);
    }
    ...
    @Override
    public void messageReceived(IoSession session, Object message) throws Exception {
        // Check if the 'fake' session secured message notification has been received
        if (message == SslFilter.SESSION_SECURED) {
            counter.countDown();
        }
    }
} 
```

As we can see in this piece of code, this is a two step process:

- first we have to inform our session that we want to be informed about the secured status
- second we have to check for every received message if the session has been secured

This is a bit clumsy, as the check has to be done in the *messageReceived* event, which is clearly mixing concepts (the remote peer never sends such a message)

## Secure session detection in Apache MINA 2.1.x

The idea was to extend the *IoHandler* interface with a generic *event* method that can be used to be informed about whatever type of event the session is subject to, beside the already processed events (session created, closed, etc).

The *SslHandler* code has been modified to fire this event when the session has been secured, using a dedicated event, *SslEvent.SECURED*.
We also have modified the *SslFilter* code to generate a event when the session is not anymore secured, sending the *SslEvent.UNSECURED* event.

The following code show the difference with the previous code :

```java
connector.setHandler(new IoHandlerAdapter() {
    @Override
    public void event(IoSession session, FilterEvent event) throws Exception {
        if (event == SslEvent.SECURED ) {
            // DO whatever the application needs to do when the session is secured
        }
    }
}
```

As we can see, we don’t need to initialize the session telling it to inform the application through a notification: this will be done no matter what.

## Why is it API incompatible ?

The *event* addition in the *IoHandler* interface does not break your code: we always have an abstract implementation that will handle the events if your application does not.

The real issue is that if your application was using the Notification mechanism for that purpose, the new version will not send you this specific message(*SslFilter.SESSION\_SECURED*).

## Migration

This is pretty straightforward :

- get rid of the notification declaration by removing the *session.setAttribute(SslFilter.USE\_NOTIFICATION, Boolean.TRUE)* code.
- implement the *event(IoSession session, FilterEvent event)* method in your application handler, checking for the *SslEvent.SECURED*/*SslEvent.UNSECURED* specific events.

and that’s it !

## Future evolution

The added event mechanism could be used to other purposes, and that includes user specific needs. It’s possible to write a specific filter that will send a dedicated *FilterEvent* instance, for the application to process it.

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-2-2-vs-2-1"></a>

# MINA 2.2.x vs MINA 2.1.x — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-2-2-vs-2-1--Navigation-Upcoming"></a>Upcoming

- No event

# 2.2.x vs 2.1.x differences

The **SSL/TLS** handling has been totally rewritten in **MINA 2.2**. This has an impact in many areas.

## Removal of the SslFilter.DISABLE\_ENCRYPTION\_ONCE attribute

This attribute was used in previous **MINA** versions to insure that we can send a clear text message to the remote peer while establishing the TLS connection when using the **startTLS** command.

The idea is that the **startTLS** command is sent by an application (an **LDAP** client, for instance), which tells the server it should establish the **SSL/TLS** layer. The problem is that the server should be able to inform the client that the **SSL/TLS** layer is up and running, in clear text, which is not possible as the **SSL/TLS** layer is already fonctionning…

This kind of chicken and egg problem was solved by giving the opportunity to the **SSL/TLS** layer to send back the **startTLS** response to the client in clear text, assuming it’s the server’s first message. A bit of a hack.

In **MINA 2.2**, this attribute has been removed and replaced by either a filter to be added, or by encapsulating the message that should not be encrypted into an instance that implements the **DisableEncryptWriteRequest** interface.

Typically, in **Apache Directory**, we use this filter:

```java
public class StartTlsFilter extends IoFilterAdapter 
{
    /**
     * {@inheritDoc}
     */
    @Override
    public void filterWrite( NextFilter nextFilter, IoSession session, WriteRequest writeRequest ) throws Exception 
    {
        if ( writeRequest.getOriginalMessage() instanceof StartTlsResponse )
        {
            // We need to bypass the SslFilter
            IoFilterChain chain = session.getFilterChain();
            
            for ( IoFilterChain.Entry entry : chain.getAll() )
            {
                IoFilter filter = entry.getFilter();
                
                if ( filter instanceof SslFilter )
                {
                    entry.getNextFilter().filterWrite( session, writeRequest );
                }
            }
        }
        else
        {
            nextFilter.filterWrite( session, writeRequest );
        }
    }
}
```

As you can see in the code above, we check if the message is a **startTLS** response, and if so, we bypass the **SSLFilter**, which leads to the message to be sent in clear text.

## Addition of the IoSession.isServer() method

This method tells if the underlaying service is an *IoAcceptor* or not. It’s useful to quickly find out if we have to set the **Tls** flag to client or server when initializing the **SslEngine** instance, we also use it for the **SslFilter** logs.

## Removal of the SslFilter.getSslSession() method

This method is not used. Would you like to get the **SSLSession** instance, it’s a matter of calling the *IoSession.getAttribute()* method with **SslFilter.SSL\_SECURED** as a parameter:

```java
...
            SSLSession sslSession = SSLSession.class.cast(getAttribute(SslFilter.SSL_SECURED));
...
```

## Why is it API incompatible ?

The removal of the **SslFilter.DISABLE\_ENCRYPTION\_ONCE** attribute makes it impossible for application that leverage the **startTLS** command to work, without some code change.

## Migration

This is pretty straightforward :

- Create a filter that bypasses the message that should not be encrypted, or encapsulate it into an instance that implements the **DisableEncryptWriteRequest** interface.

and that’s it !

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-codec-repo"></a>

# MINA Codec Repository — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-codec-repo--Navigation-Upcoming"></a>Upcoming

- No event

# Overview

This page captures known MINA Codecs available. These codecs may not be part of Apache MINA project. The Codecs are for reference purpose only.

The Codecs listed here may not be part of Apache MINA project. The information is for MINA Users for reference implementation available over the web

## Protocol Codecs

The table below summarizes some of the known codecs

| Protocol | Project | Description |
| --- | --- | --- |
| Prefixed String | Apache MINA | Encodes/Decodes a a String with fixed length prefix |
| Object Serializer | Apache MINA | Serializes and deserializes Java objects |
| Text Line | Apache MINA | Encoding/Decoding between a text line data and a Java string object |
| Ftp | Apache FtpServer | FTP codecs |
| LDAP | Apache Directory | LDAP protocol Codecs |
| DNS | Apache Directory | DNS protocol Codecs |
| Kerberos | Apache Directory | Kerberos protocol Codecs |
| NTP | Apache Directory | NTP protocol Codecs |
| DHCP | Apache Directory | DHCP protocol Codecs |
| MRTMP | Red5 | Codecs for Multiplexing RTMP |
| RTMP | Red5 | Codecs for RTMP |
| RTSP | Red5 | Codecs for RTSP |
| SMTP | MailsterSMTP | SMTP Codecs |
| AMQP | Apache Qpid | AMQP Codecs |
| XMPP | Jive Software Openfire | XMPP Codecs |
| XMPP | Vysper | XMPP/XML Codecs. Subproject of MINA. |
| Google Protocol Buffers | Apache MINA | Codecs are still insandbox |

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-conferences"></a>

# MINA Presentation Materials — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-conferences--Navigation-Upcoming"></a>Upcoming

- No event

# Presentation Materials

These presentation materials will help you understand the overall architecture and core constructs of MINA.

- [MINA in real life](/assets/pdfs/Mina_in_real_life_ASEU-2009.pdf) (ApacheCon EU 2009) by Emmanuel Lécharny
- [Rapid Network Application Development with Apache MINA](/assets/pdfs/JavaOne2008.pdf) (JavaOne 2008) by Trustin Lee
- [Apache MINA - The High Performance Protocol Construction Toolkit](/assets/pdfs/ACUS2007.pdf) (ApacheCon US 2007) by Peter Royal
- [Introduction to MINA (ApacheCon Asia 2006)](/assets/pdfs/ACAsia2006.pdf) by Trustin Lee

# Other Presentation Materials

- [Building TCP/IP Servers with Apache MINA](/assets/pdfs/ACEU2007.pdf) (ApacheCon EU 2007) by Peter Royal
- [Building TCP/IP Servers with Apache MINA](/assets/pdfs/ACEU2006.pdf) (ApacheCon EU 2006) by Peter Royal
- [Introduction to MINA](/assets/pdfs/ACUS2005.pdf) (ApacheCon US 2005) by Trustin Lee [Demo movie](/assets/pdfs/ACUS2005.swf)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-developer-guide"></a>

# Developer Guide — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-developer-guide--Navigation-Upcoming"></a>Upcoming

- No event

# Building MINA

Please read [the Developer Infrastructure Information](https://www.apache.org/dev/) if you haven't yet before you proceed.

- [Preparing the release for the vote](#mina-apache-org-mina-project-developer-guide--preparing-the-release-for-the-vote)
  - [Step 0: Building MINA](#mina-apache-org-mina-project-developer-guide--step-0-building-mina)
  - [Step 1: Tagging and Deploying](#mina-apache-org-mina-project-developer-guide--step-1-tagging-and-deploying)
  - [step 2 : Processing with a dry run](#mina-apache-org-mina-project-developer-guide--step-2--processing-with-a-dry-run)
  - [Step 3 : Processing with the real release](#mina-apache-org-mina-project-developer-guide--step-3--processing-with-the-real-release)
  - [Step 4 : perform the release](#mina-apache-org-mina-project-developer-guide--step-4--perform-the-release)
  - [Step 5 : closing the staging release on nexus](#mina-apache-org-mina-project-developer-guide--step-5--closing-the-staging-release-on-nexus)
  - [Step 6 : Build the Site](#mina-apache-org-mina-project-developer-guide--step-6--build-the-site)
  - [Step 7 : Sign the packages](#mina-apache-org-mina-project-developer-guide--step-7--sign-the-packages)
  - [Step 8 : Publish Source and Binary Distribution Packages](#mina-apache-org-mina-project-developer-guide--step-8--publish-source-and-binary-distribution-packages)
  - [Step 9 : Test the New Version with FtpServer, Sshd and Vysper](#mina-apache-org-mina-project-developer-guide--step-9--test-the-new-version-with-ftpserver-sshd-and-vysper)
- [Step 10 : Voting a release](#mina-apache-org-mina-project-developer-guide--step-10--voting-a-release)
  - [Step 11 : Close the vote](#mina-apache-org-mina-project-developer-guide--step-11--close-the-vote)
  - [Step 12: Deploy Web Reports (JavaDoc and JXR)](#mina-apache-org-mina-project-developer-guide--step-12-deploy-web-reports-javadoc-and-jxr)
  - [Step 13: Update the Links in Web Site](#mina-apache-org-mina-project-developer-guide--step-13-update-the-links-in-web-site)
  - [Step 14: Wait an hour](#mina-apache-org-mina-project-developer-guide--step-14-wait-an-hour)
  - [Step 15: Announce the New Release](#mina-apache-org-mina-project-developer-guide--step-15-announce-the-new-release)

# Checking out the code

The current *MINA* code requires to be built with Java 8 for *MINA* 2.0.X and *MINA* 2.1.X, and with Java 17 or higher for *MINA* 2.2.X branch. See the table below.

You need **Git** to check out the source code from our source code repository, and [Maven](https://maven.apache.org/) 3.8 (pick the latest Maven version) to build the source code (Building with Maven 3.0 will also work).

Here are the Java version required for each branch. The Maven *pom.xml* has been configured to enforce those versions:

| Banche | Build Java required version |
| --- | --- |
| [2.0.X] | Java 1.8 |
| [2.1.X] | Java 1.8 |
| [2.2.X] | Java 17 or higher |

The following example shows how to build the current stable branch (2.2.X).

```bash
$ git clone -b 2.2.X https://gitbox.apache.org/repos/asf/mina.git mina
$ cd mina
$ mvn -Pserial clean install             # Build packages (JARs) for the core API and other 
                                         # extensions and install them to the local Maven repository.
$ mvn -Pserial site                      # Generate reports (JavaDoc and JXR)
$ mvn -Pserial package assembly:assembly # Generate a tarball (package goal needed to fix an assembly plugin bug)
```

Eclipse users:
Don’t forget to declare a classpath variable named **M2\_REPO**, pointing to `~/.m2/repository`, otherwise many links to existing jars will be broken.
You can declare new variables in Eclipse in *Windows -> Preferences…* and selecting *Java -> Build Path -> Classpath Variables*.

There are also other branches that might interest you:

- 2.1.X: For MINA 2.1 version
- 2.0.X: For MINA 2.1 version

If you want to check out the source code of previous releases, you have to select the branch you want to work on :

```bash
$ git clone https://gitbox.apache.org/repos/asf/mina.git mina
$ cd mina
$ git checkout <tag>
```

For instance, to work on the 2.0.X version trunk, just do :

```bash
$ git clone https://gitbox.apache.org/repos/asf/mina.git mina
$ cd mina
$ git checkout 2.0.X
```

or in two lines only:

```bash
$ git clone -b 2.0.X https://gitbox.apache.org/repos/asf/mina.git mina
$ cd mina
```

# Coding Convention

We follow [Sun’s standard Java coding convention](https://www.oracle.com/technetwork/java/codeconventions-150003.pdf) except that we always use spaces instead of tabs. Please download [the Eclipse Java formatter settings file](ImprovedJavaConventions.xml) before you make any changes to the code.

This file is also available in the `/resources` directory.

# Class header

As class header we use :

```java
/** 
 * Class desciption here.
 *
 * @author <a href="https://mina.apache.org">Apache MINA Project</a>
 */
```

The headers revisions tags are removed.

# Deploying Snapshots (Committers Only)

Before running Maven to deploy artifacts, *please make sure if your umask is configured correctly*. Unless configured properly, other committers will experience annoying ‘permission denied’ errors. If your default shell is `bash`, please update your umask setting in the `~/.bashrc` file (create one if it doesn’t exist.) by adding the following line:

```bash
umask 002
```

Please note that you have to edit the correct `shrc` file. If you use `csh`, then you will have to edit `~/.cshrc` file.

Now you are ready to deploy the artifacts if you configured your umask correctly.

```bash
$ git clone https://gitbox.apache.org/repos/asf/mina.git mina
$ cd mina
$ mvn -Pserial clean deploy site site:deploy    # Make sure to run 'clean' goal first to prevent side effects from your IDE.
```

Please double-check the mode (i.e. `0664` or `-rw-rw-r--`, a.k.a permission code) of the deployed artifacts, otherwise you can waste other people’s time significantly.

# Releasing a Point Release (Committers Only)

## Preparing the release for the vote

Before starting be sure to have the java and mvn command in your PATH.
On linux you can check with the following commands (change the Maven version accordingly):

```bash
$ type mvn
mvn is hashed (/opt/maven-3.8.5/bin/mvn)
$ type java
java is hashed (/usr/bin/java)
```

### Step 0: Building MINA

As weird as it sounds, for some unknown reason (most certainly a misconfiguration in the Maven poms), we can’t just run the release without having previously build all the projects. This is done with the following command :

```bash
$ mvn clean install -Pserial
```

### Step 1: Tagging and Deploying

First you need to configure maven for using the good username for scp and operation.

In the `~/.m2/settings.xml` you need the following lines :

```xml
<settings xmlns="http://maven.apache.org/POM/4.0.0"
  xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
  xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
                      http://maven.apache.org/xsd/settings-1.0.0.xsd">

  <!-- SERVER SETTINGS -->
  <servers>
    <!-- To publish a snapshot of some part of Maven -->
    <server>
      <id>apache.snapshots.https</id>
      <username>elecharny</username>
      <password>-----Your password here-----</password>
    </server>
    <!-- To publish a website of some part of Maven -->
    <server>
      <id>apache.websites</id>
      <username>elecharny</username>
      <filePermissions>664</filePermissions>
      <directoryPermissions>775</directoryPermissions>
    </server>
    <!-- To stage a release of some part of Maven -->
    <server>
      <id>apache.releases.https</id>
      <username>elecharny</username>
      <password>-----Your password here-----</password>
    </server>
    <!-- To stage a website of some part of Maven -->
    <server>
      <id>stagingSite</id> <!-- must match hard-coded repository identifier in site:stage-deploy -->
      <username>elecharny</username>
      <filePermissions>664</filePermissions>
      <directoryPermissions>775</directoryPermissions>
    </server>
  </servers>

  <!-- PROFILE SETTINGS -->
  <profiles>
    <profile>
      <id>apache-release</id>
      <properties>
        <!-- Configuration for artifacts signature -->
        <gpg.passphrase>-----Your passphrase here-----</gpg.passphrase>
      </properties>
    </profile>
  </profiles>

</settings>
```

### step 2 : Processing with a dry run

After having checked out the branch you want to release (2.2.X, 2.1.X or 2.0.X), and built it (see step 0), here the 2.2.X branch:

```bash
$ git clone -b 2.2.X https://gitbox.apache.org/repos/asf/mina.git mina
$ cd mina
$ mvn clean install -Pserial
```

run the following commands :

```bash
$ mvn -Pserial,apache-release -DdryRun=true release:prepare    # Dry-run first.
```

Answer to maven questions :

```text
"What is the release version for "Apache MINA"? (org.apache.mina:mina-parent) <version>: :" 
<either use the default version as suggested, or type in the version you@qot;d like to be used>
[..]
```

Then some other questions will be asked, about the next version to use. The default values should be fine.

**Be Careful**  

```text
Make sure the change made by the release plugin is correct! (pom.xml, tags created)
```

**In case of a problem...**  

```bash
It's frequent that the dry-run fails, typically when you have some Javadoc issues (starting with Java 8, the compiler is really picky about wrong HTML tags or missing parameters).

You can rollback the release with the command:

$ mvn -Pserial,apache-release -DdryRun=true release:rollback

You should be back on your feet. 
```

### Step 3 : Processing with the real release

When the dry run is successful, then you can do in real with the following commands:

```bash
$ mvn -Pserial,apache-release release:clean      # Clean up the temporary files created by the dry-run.
$ mvn -Pserial,apache-release release:prepare    # Copy to tags directory.
```

The first step will clean up the local sources, the second step will release for real. The same questions will be asked as those we had during the dry run step.

At some point, it will ask for your passphrase (the one you used when you created your PGP key). Type it in.

Three mails will be generated, and sent to [commits@mina.apache.org](mailto:commits@mina.apache.org) :

```text
git commit: [maven-release-plugin] prepare release 2.0.9
Git Push Summary
git commit: [maven-release-plugin] prepare for next development iteration
```

The first mail tells you that the SNAPSHOT has been moved to the release version in trunk, the second mails tells you that this version has been tagged, and the last mail tells you that trunk has moved to the next version.

### Step 4 : perform the release

The last step before launching a vote is to push the potential release to Nexus so that every user can test the created packages. Perform the following actions (note that we have to run a build first for the javadoc to be correctly generated…) :

```bash
$ mvn clean install -Pserial -DskipTests
...
$ mvn -Pserial,apache-release release:perform
...
[INFO] [INFO] ------------------------------------------------------------------------
[INFO] [INFO] Reactor Summary:
[INFO] [INFO] ------------------------------------------------------------------------
[INFO] [INFO] Apache MINA ........................................... SUCCESS [1:05.896s]
[INFO] [INFO] Apache MINA Legal ..................................... SUCCESS [30.708s]
[INFO] [INFO] Apache MINA Core ...................................... SUCCESS [4:44.973s]
[INFO] [INFO] Apache MINA APR Transport ............................. SUCCESS [46.082s]
[INFO] [INFO] Apache MINA Compression Filter ........................ SUCCESS [40.230s]
[INFO] [INFO] Apache MINA State Machine ............................. SUCCESS [52.718s]
[INFO] [INFO] Apache MINA JavaBeans Integration ..................... SUCCESS [46.358s]
[INFO] [INFO] Apache MINA XBean Integration ......................... SUCCESS [1:21.054s]
[INFO] [INFO] Apache MINA OGNL Integration .......................... SUCCESS [40.740s]
[INFO] [INFO] Apache MINA JMX Integration ........................... SUCCESS [40.482s]
[INFO] [INFO] Apache MINA Examples .................................. SUCCESS [1:13.837s]
[INFO] [INFO] Apache MINA Serial Communication support .............. SUCCESS [41.684s]
[INFO] [INFO] Apache MINA Distribution .............................. SUCCESS [12:39.542s]
[INFO] [INFO] ------------------------------------------------------------------------
[INFO] [INFO] ------------------------------------------------------------------------
[INFO] [INFO] BUILD SUCCESSFUL
[INFO] [INFO] ------------------------------------------------------------------------
[INFO] [INFO] Total time: 26 minutes 46 seconds
[INFO] [INFO] Finished at: Mon Sep 13 16:45:14 CEST 2010
[INFO] [INFO] Final Memory: 98M/299M
[INFO] [INFO] ------------------------------------------------------------------------
[INFO] Cleaning up after release...
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESSFUL
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 27 minutes 5 seconds
[INFO] Finished at: Mon Sep 13 16:45:18 CEST 2010
[INFO] Final Memory: 28M/81M
[INFO] ------------------------------------------------------------------------
```

Done !

### Step 5 : closing the staging release on nexus

Now, you have to close the staged project on nexus. In order to do that you **must** have exported your PGP key to a PGP public server [see](https://www.apache.org/dev/openpgp.html)

Connect to the [Nexus server](https://repository.apache.org), login, and select the MINA staging repository you just created, then click on the ‘close’ button. You are home…

### Step 6 : Build the Site

Just run this command:

```bash
$ cd target/checkout
$ mvn -Pserial site
```

This creates the site in target/checkout/target

### Step 7 : Sign the packages

Now, you have to sign the binary packages which are in target/checkout/distribution/target.

You should have all the build packages here.

First, remove the already signed files:

```bash
rm *.asc
```

Then start with the signing part.

Use your PGP key ID (the pub key, 4096R/[XXXXXXX] where [XXXXXXX] is the key ID)

You can get the keys by typing :

```bash
gpg --list-keys
```

You’ll get something like :

```text
localhost:target elecharny$ gpg --list-keys
/Users/elecharny/.gnupg/pubring.gpg
-----------------------------------
pub   dsa2048 2009-12-03 [SCA]
  C62BFD988278310B5B7A43D16FC4BEA60A8A0BBA
uid           [ultimate] Emmanuel Lecharny <elecharny@nextury.com>
sub   elg2048 2009-12-03 [E]

pub   rsa4096 2010-09-13 [SC]
  4D2DB2916149BAA9D0C92F3731474E5E7C6B7034
uid           [ultimate] Emmanuel Lecharny (CODE SIGNING KEY) <elecharny@apache.org>
sub   rsa4096 2010-09-13 [E]

...
```

Take the long hexadecimal part following the ‘pub’ part (ie “4D2DB2916149BAA9D0C92F3731474E5E7C6B7034” for the 4096 bits key)

Use a shell script to sign the packages which are stored in target/checkout/distribution/target. You will first have to delete the created .asc files :

```text
localhost:target elecharny$ rm *.asc
localhost:target elecharny$ ~/sign.sh 
PGP Key ID: 
<your PGP key>
PGP Key Password: 
<Your PGP passphrase>

-n Signing: ./apache-mina-2.0.9-bin.tar.bz2 ... 
  - Generated './apache-mina-2.0.9-bin.tar.bz2.sha512'
  - Generated './apache-mina-2.0.9-bin.tar.bz2.asc'
-n Signing: ./apache-mina-2.0.9-bin.tar.gz ... 
  - Generated './apache-mina-2.0.9-bin.tar.gz.sha512'
  - Generated './apache-mina-2.0.9-bin.tar.gz.asc'
...
```

Here is the `sign.sh` script you can use :

```bash
#!/bin/sh

echo "PGP Key ID: "
read DEFAULT_KEY

echo "PGP Key Password: "
stty -echo
read PASSWORD
stty echo
echo ""

for FILE in $(find . -maxdepth 1 -not '(' -name "sign.sh" -or -name ".*" -or -name "*.sha256" -or -name "*.sha512" -or -name "*.asc" ')' -and -type f) ; do
  if [ -f "$FILE.asc" ]; then
      echo "Skipping: $FILE"
      continue
  fi

  echo -n "Signing: $FILE ... "

  # SHA-512
  if [ ! -f "$FILE.sha512" ];
  then
      gpg -v --default-key "$DEFAULT_KEY" --print-md SHA512 "$FILE" > "$FILE".sha512
      echo "  - Generated '$FILE.sha512'"
  else
      echo "  - Skipped '$FILE.sha512' (file already existing)"
  fi

  # ASC
  if [ ! -f "$FILE.asc" ];
  then
      echo "$PASSWORD" | gpg --default-key "$DEFAULT_KEY" --detach-sign --armor --no-tty --yes --passphrase-fd 0 "$FILE"
      echo "  - Generated '$FILE.asc'"
  else
      echo "  - Skipped '$FILE.asc' (file already existing)"
  fi
done
```

Once done, you can remove the signed pom files, they are useless:

```text
rm *.pom.*
```

### Step 8 : Publish Source and Binary Distribution Packages

The sources, binaries and their signatures, have to be pushed in a place where they can be downloaded by the other committers, in order to be checked while validating the release. As the ~/people.apache.org server is not anymore available for that purpose, we use the distribution space for that purpose.

If you haven’t checked out this space, do it now :

```bash
$ mkdir -p ~/mina/dist/dev/mina
$ svn co https://dist.apache.org/repos/dist/dev/mina ~/mina/dist/dev/mina
```

That will checkout the full project distributions.

You may want to checkout only the part that you are going to generate, to avoid getting Gb of data :

```bash
$ mkdir -p ~/mina/dist/dev/mina/mina
$ svn co https://dist.apache.org/repos/dist/dev/mina/mina ~/mina/dist/dev/mina/mina
```

Now, create a sub-directory for the version you have generated (here, for version 2.0.14) :

```bash
$ mkdir ~/mina/dist/dev/mina/mina/2.0.14
```

Then copy the packages :

```bash
$ cd target/checkout/distributions/target
$ cp apache-mina-2.0.14-* ~/mina/dist/dev/mina/mina/2.0.14/
```

Last, not least, commit your changes

```bash
$ svn add ~/mina/dist/dev/mina/mina/2.0.14
$ svn ci ~/mina/dist/dev/mina/mina/2.0.14 -m "Apache MINA 2.0.14 packages"
```

### Step 9 : Test the New Version with FtpServer, Sshd and Vysper

In *FtpServer/pom.xml* change the <org.apache.directory.shared.version> property, build FtpServer. It should build with no error. Do the same thing with Sshd and Vysper.

It’s time to launch a vote !

## Step 10 : Voting a release

Once the tarballs have been created, and the binaries available in Nexus, a vote can be launched. Simply send a mail on the [dev@mina.apache.org](mailto:dev@mina.apache.org) mailing list describing the new release.

Here is how you send a [VOTE] mail on the dev mailing list :

```text
Hi,

<blah blah blah>

Here is the list of fixed issues :
 

   * [DIRMINA-803 <https://issues.apache.org/jira/browse/DIRMINA-803>]
     - ProtocolCodecFilter.filterWrite() is no longer thread-safe
   * ...

Here's the Jira link for this version if you'd like to review issues in more details:

https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=10670&styleName=Html&version=12313702

A temporary tag has been created (it can be removed if the vote is not approved)

The newly approved Nexus has been used for the preparation of this release and all final artifacts are stored 
in a staging repository:
https://repository.apache.org/content/repositories/orgapachemina-002/

The distributions are available for download on :
https://repository.apache.org/content/repositories/orgapachemina-004/org/apache/mina/mina-parent/2.0.1/

Let us vote :
[ ] +1 | Release MINA 2.0.1
[ ] +/- | Abstain
[ ] -1 | Do *NOT*  release MINA 2.0.1

Thanks !
```

The vote will be open for 72 hours. Once the delay is over, collect the votes, and count the binding +1/-1. If the vote is positive, then we can release.

### Step 11 : Close the vote

You can officially close the vote now. There are some more steps to fulfill :

- Release the project on the [Nexus server](https://repository.apache.org)
- Copy the tarballs and heir signature in [https://dist.apache.org/repos/dist/release/mina/mina](https://dist.apache.org/repos/dist/release/mina/mina)

The sources, binaries and their signatures, have to be pushed in a place where they can be downloaded by users. We use the [distribution](https://dist.apache.org/repos/dist/release/mina/mina) space for that purpose.

Move the distribution packages (sources and binaries) to the dist SVN repository: `https://dist.apache.org/repos/dist/release/mina/mina/$(version)`

If you haven’t checked out this space, do it now :

```bash
$ mkdir -p ~/mina/dist/release/mina
$ svn co https://dist.apache.org/repos/dist/release/mina/mina ~/mina/dist/release/mina
```

That will checkout the full project distributions.

Then move the packages from ‘dev’ to ‘release’ :

```bash
$ cd ~/mina/dist/release/mina
$ cp ~/mina/dist/dev/mina/mina/<version> .
$ svn add <version>
$ svn ci <version>
...
$ exit
```

The packages should now be available on [https://dist.apache.org/repos/dist/release/mina/mina/](https://dist.apache.org/repos/dist/release/mina/mina/)

### Step 12: Deploy Web Reports (JavaDoc and JXR)

The javadoc and xref files have been generated in step 6, it’s now time to push them into the production site. They are generated in the following directory :

```text
target/checkout/target/site
```

We will copy four directories :

```text
apidocs
testapidocs
xref
xref-test
```

They are uploaded to [https://nightlies.apache.org/](https://nightlies.apache.org/) via WebDAV protocol.

First create the folders for the version (change the <version> part):

```bash
$ curl -u <your asf id> -X MKCOL 'https://nightlies.apache.org/mina/mina/<version>/'
$ curl -u <your asf id> -X MKCOL 'https://nightlies.apache.org/mina/mina/<version>/apidocs'
$ curl -u <your asf id> -X MKCOL 'https://nightlies.apache.org/mina/mina/<version>/testapidocs'
$ curl -u <your asf id> -X MKCOL 'https://nightlies.apache.org/mina/mina/<version>/xref'
$ curl -u <your asf id> -X MKCOL 'https://nightlies.apache.org/mina/mina/<version>/xref-test'
```

Each of those commands will ask for your ASF password.

I used **rclone** to copy folders via WebDAV.

After intallation run rclone config and configure the nightlies connection:

```bash
$ rclone config
name: nightlies
type: webdav
url: https://nightlies.apache.org
vendor: other
user: <your asf id>
pass: <your asf password> (will be stored encrypted)
```

Then copy the directories (change the <version> part):

```bash
cd target/checkout/target/site
rclone copy --progress apidocs nightlies:/mina/mina/<version>/apidocs
rclone copy --progress testapidocs nightlies:/mina/mina/<version>/testapidocs
rclone copy --progress xref nightlies:/mina/mina/<version>/xref
rclone copy --progress xref-test nightlies:/mina/mina/<version>/xref-test
```

Finally update the links in the static/mina-project/gen-docs/.htaccess of the mina-site repo (change the <version> part):

```text
RewriteRule ^latest-2.1$ https://nightlies.apache.org/mina/mina/<version>/ [QSA,L]
RewriteRule ^latest-2.1/(.*)$ https://nightlies.apache.org/mina/mina/<version>/$1 [QSA,L]
```

Save and commit the file, the web site should be automatically generated and published.

### Step 13: Update the Links in Web Site

Some pages have to be updated. Assuming the MINA site has been checked out in ~/mina/site (this can be done with the command *$ svn co [https://svn.apache.org/viewvc/mina/site/trunk](https://svn.apache.org/viewvc/mina/site/trunk) ~/mina/site*), here are the pages that need to be changed :

- /config.toml: update the `version_mina_XYZ` variable with the new version.
- /source/mina-project/news.md: add the news on top of this page
- /source/mina-project/downloads-2\_0.md or /source/mina-project/downloads-2\_1.md or /source/mina-project/downloads-2\_2.md: change the version all over the page
- /source/downloads-mina\_2\_0.md or /source/downloads-mina2\_1.md or /source/downloads-mina2\_2.md: change the version all over the page
- /source/mina-project/downloads-old.md: Add a line for the latest version which has been replaced by the released one

Commit the changes, and publish the web site, you are done !

### Step 14: Wait an hour

We need to wait until any changes made in the web site and metadata file(s) go live.

### Step 15: Announce the New Release

An announcement message can be sent to [mailto:announce@apache.org], [mailto:announce@apachenews.org], [mailto:users@mina.apache.org] and [mailto:dev@mina.apache.org]. Please note that announcement messages are rejected unless your from-address ends with `@apache.org`. Plus, you shouldn’t forget to post a news to the MINA site main page.

Enjoy !

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-downloads_2_0"></a>

# MINA 2.0.x Downloads — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-downloads_2_0--Navigation-Upcoming"></a>Upcoming

- No event

# Latest MINA Releases

## Apache MINA 2.0.28 stable (Java 8+)

### Binaries

- .tar.gz archive [mina-2.0.28](https://dlcdn.apache.org/mina/mina/2.0.28/apache-mina-2.0.28-bin.tar.gz) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-bin.tar.gz.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-bin.tar.gz.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-bin.tar.gz.asc))
- .tar.bz2 archive [mina-2.0.28](https://dlcdn.apache.org/mina/mina/2.0.28/apache-mina-2.0.28-bin.tar.bz2) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-bin.tar.bz2.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-bin.tar.bz2.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-bin.tar.bz2.asc))
- .zip archive [mina-2.0.28](https://dlcdn.apache.org/mina/mina/2.0.28/apache-mina-2.0.28-bin.zip) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-bin.zip.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-bin.zip.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-bin.zip.asc))

### Sources

- .src.tar.gz archive [mina-2.0.28](https://dlcdn.apache.org/mina/mina/2.0.28/apache-mina-2.0.28-src.tar.gz) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-src.tar.gz.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-src.tar.gz.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-src.tar.gz.asc))
- .src.tar.bz2 archive [mina-2.0.28](https://dlcdn.apache.org/mina/mina/2.0.28/apache-mina-2.0.28-src.tar.bz2) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-src.tar.bz2.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-src.tar.bz2.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-src.tar.bz2.asc))
- .src.zip archive [mina-2.0.28](https://dlcdn.apache.org/mina/mina/2.0.28/apache-mina-2.0.28-src.zip) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-src.zip.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-src.zip.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.0.28/apache-mina-2.0.28-src.zip.asc))

For people wanting to use the **serial** package, we don't include the **rxtx.jar** library in the releases, as it's under a LGPL license. Please download it from [http://rxtx.qbang.org/wiki/index.php/Download](http://rxtx.qbang.org/wiki/index.php/Download) or add the associated dependency in your maven pom.xml :

```xml
<dependency>
    <groupId>org.rxtx</groupId>
    <artifactId>rxtx</artifactId>
    <version>2.1.7</version>
    <scope>provided<scope>
</dependency>
```

# Verify the integrity of the files

The PGP signatures can be verified using PGP or GPG. First download the [KEYS](https://downloads.apache.org/mina/KEYS) as well as the asc signature file for the relevant distribution. Then verify the signatures using:

```bash
$ pgpk -a KEYS
$ pgpv mina-2.0.28.tar.gz.asc
```

or

```bash
$ pgp -ka KEYS
$ pgp mina-2.0.28.tar.gz.asc
```

or

```bash
$ gpg --import KEYS
$ gpg --verify mina-2.0.28.tar.gz.asc
```

Alternatively, you can verify the checksums of the files (see the [How to verify downloaded files page](https://www.apache.org/info/verification.html)).

# Previous Releases

The previous releases can be found [here](https://archive.apache.org/dist/mina/) and [here](https://archive.apache.org/dist/mina/mina/). Please note that the following releases contains a LGPL licensed file, rxtx-2.1.7.jar: 2.0.0-M4, 2.0.0-M5, 2.0.0-M6, 2.0.0-RC1.

# Version Numbering Scheme

The version number of MINA has the following form:

<major>.<minor>.<micro> \[-M<milestone number> or -RC<release candidate number>]

This scheme has three number components:

- The **major** number increases when there are incompatible changes in the API.
- The **minor** number increases when a new feature is introduced.
- The **micro** number increases when a bug or a trivial change is made.

and an optional label that indicates the maturity of a release:

- **M** (Milestone) means the feature set can change at any time in the next milestone releases. The last milestone release becomes the first release candidate after a vote.
- **RC** (Release Candidate) means the feature set is frozen and the next RC releases will focus on fixing problems unless there is a serious flaw in design. The last release candidate becomes the first GA release after a vote.
- No label implies **GA** (General Availability), which means the release is stable enough and therefore ready for production environment.

MINA is not a stand-alone software, so ‘the feature set’ here also includes the API of the newly introduced features and the overall architecture of the software,

Here’s an example that illustrates how MINA version number increases:

2.0.0-M1 -> 2.0.0-M3 -> 2.0.0-M3 -> 2.0.0-M4 -> 2.0.0-RC1 -> 2.0.0-RC2 -> 2.0.0-RC3 -> **2.0.0** -> 2.0.1 -> 2.0.2 -> 2.1.0-M1 ...

Please note that we always specify the micro number, even if it’s zero.

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-downloads_2_1"></a>

# MINA 2.1.x Downloads — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-downloads_2_1--Navigation-Upcoming"></a>Upcoming

- No event

# Latest MINA Releases

## Apache MINA 2.1.12 stable (Java 8+)

### Binaries

- .tar.gz archive [mina-2.1.12](https://dlcdn.apache.org/mina/mina/2.1.12/apache-mina-2.1.12-bin.tar.gz) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-bin.tar.gz.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-bin.tar.gz.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-bin.tar.gz.asc))
- .tar.bz2 archive [mina-2.1.12](https://dlcdn.apache.org/mina/mina/2.1.12/apache-mina-2.1.12-bin.tar.bz2) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-bin.tar.bz2.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-bin.tar.bz2.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-bin.tar.bz2.asc))
- .zip archive [mina-2.1.12](https://dlcdn.apache.org/mina/mina/2.1.12/apache-mina-2.1.12-bin.zip) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-bin.zip.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-bin.zip.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-bin.zip.asc))

### Sources

- .src.tar.gz archive [mina-2.1.12](https://dlcdn.apache.org/mina/mina/2.1.12/apache-mina-2.1.12-src.tar.gz) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-src.tar.gz.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-src.tar.gz.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-src.tar.gz.asc))
- .src.tar.bz2 archive [mina-2.1.12](https://dlcdn.apache.org/mina/mina/2.1.12/apache-mina-2.1.12-src.tar.bz2) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-src.tar.bz2.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-src.tar.bz2.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-src.tar.bz2.asc))
- .src.zip archive [mina-2.1.12](https://dlcdn.apache.org/mina/mina/2.1.12/apache-mina-2.1.12-src.zip) (signatures : [SHA256](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-src.zip.sha256) [SHA512](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-src.zip.sha512) [ASC](https://www.apache.org/dist/mina/mina/2.1.12/apache-mina-2.1.12-src.zip.asc))

For people wanting to use the **serial** package, we don't include the **rxtx.jar** library in the releases, as it's under a LGPL license. Please download it from [http://rxtx.qbang.org/wiki/index.php/Download](http://rxtx.qbang.org/wiki/index.php/Download) or add the associated dependency in your maven pom.xml :

```xml
<dependency>
    <groupId>org.rxtx</groupId>
    <artifactId>rxtx</artifactId>
    <version>2.1.7</version>
    <scope>provided<scope>
</dependency>
```

# Verify the integrity of the files

The PGP signatures can be verified using PGP or GPG. First download the [KEYS](https://downloads.apache.org/mina/KEYS) as well as the asc signature file for the relevant distribution. Then verify the signatures using:

```bash
$ pgpk -a KEYS
$ pgpv mina-2.1.12.tar.gz.asc
```

or

```bash
$ pgp -ka KEYS
$ pgp mina-2.1.12.tar.gz.asc
```

or

```bash
$ gpg --import KEYS
$ gpg --verify mina-2.1.12.tar.gz.asc
```

Alternatively, you can verify the checksums of the files (see the [How to verify downloaded files page](https://www.apache.org/info/verification.html)).

# Previous Releases

The previous releases can be found [here](https://archive.apache.org/dist/mina/) and [here](https://archive.apache.org/dist/mina/mina/). Please note that the following releases contains a LGPL licensed file, rxtx-2.1.8.jar: 2.0.0-M4, 2.0.0-M5, 2.0.0-M6, 2.0.0-RC1.

# Version Numbering Scheme

The version number of MINA has the following form:

<major>.<minor>.<micro> \[-M<milestone number> or -RC<release candidate number>]

This scheme has three number components:

- The **major** number increases when there are incompatible changes in the API.
- The **minor** number increases when a new feature is introduced.
- The **micro** number increases when a bug or a trivial change is made.

and an optional label that indicates the maturity of a release:

- **M** (Milestone) means the feature set can change at any time in the next milestone releases. The last milestone release becomes the first release candidate after a vote.
- **RC** (Release Candidate) means the feature set is frozen and the next RC releases will focus on fixing problems unless there is a serious flaw in design. The last release candidate becomes the first GA release after a vote.
- No label implies **GA** (General Availability), which means the release is stable enough and therefore ready for production environment.

MINA is not a stand-alone software, so ‘the feature set’ here also includes the API of the newly introduced features and the overall architecture of the software,

Here’s an example that illustrates how MINA version number increases:

2.0.0-M1 -> 2.0.0-M3 -> 2.0.0-M3 -> 2.0.0-M4 -> 2.0.0-RC1 -> 2.0.0-RC2 -> 2.0.0-RC3 -> **2.0.0** -> 2.0.1 -> 2.0.2 -> 2.1.8-M1 ...

Please note that we always specify the micro number, even if it’s zero.

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-downloads_2_2"></a>

# MINA 2.2.x Downloads — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-downloads_2_2--Navigation-Upcoming"></a>Upcoming

- No event

# Latest MINA Releases

## Apache MINA 2.2.7 stable (Java 8+)

### Binaries

- .tar.gz archive [mina-2.2.7](https://dlcdn.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.tar.gz) (signatures : [SHA256](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.tar.gz.sha256) [SHA512](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.tar.gz.sha512) [ASC](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.tar.gz.asc))
- .tar.bz2 archive [mina-2.2.7](https://dlcdn.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.tar.bz2) (signatures : [SHA256](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.tar.bz2.sha256) [SHA512](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.tar.bz2.sha512) [ASC](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.tar.bz2.asc))
- .zip archive [mina-2.2.7](https://dlcdn.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.zip) (signatures : [SHA256](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.zip.sha256) [SHA512](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.zip.sha512) [ASC](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-bin.zip.asc))

### Sources

- .src.tar.gz archive [mina-2.2.7](https://dlcdn.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.tar.gz) (signatures : [SHA256](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.tar.gz.sha256) [SHA512](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.tar.gz.sha512) [ASC](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.tar.gz.asc))
- .src.tar.bz2 archive [mina-2.2.7](https://dlcdn.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.tar.bz2) (signatures : [SHA256](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.tar.bz2.sha256) [SHA512](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.tar.bz2.sha512) [ASC](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.tar.bz2.asc))
- .src.zip archive [mina-2.2.7](https://dlcdn.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.zip) (signatures : [SHA256](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.zip.sha256) [SHA512](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.zip.sha512) [ASC](https://downloads.apache.org/mina/mina/2.2.7/apache-mina-2.2.7-src.zip.asc))

For people wanting to use the **serial** package, we don't include the **rxtx.jar** library in the releases, as it's under a LGPL license. Please download it from [http://rxtx.qbang.org/wiki/index.php/Download](http://rxtx.qbang.org/wiki/index.php/Download) or add the associated dependency in your maven pom.xml :

```xml
<dependency>
    <groupId>org.rxtx</groupId>
    <artifactId>rxtx</artifactId>
    <version>2.1.7</version>
    <scope>provided<scope>
</dependency>
```

# Verify the integrity of the files

The PGP signatures can be verified using PGP or GPG. First download the [KEYS](https://downloads.apache.org/mina/KEYS) as well as the asc signature file for the relevant distribution. Then verify the signatures using:

```bash
$ pgpk -a KEYS
$ pgpv mina-2.2.7.tar.gz.asc
```

or

```bash
$ pgp -ka KEYS
$ pgp mina-2.2.7.tar.gz.asc
```

or

```bash
$ gpg --import KEYS
$ gpg --verify mina-2.2.7.tar.gz.asc
```

Alternatively, you can verify the checksums of the files (see the [How to verify downloaded files page](https://www.apache.org/info/verification.html)).

# Previous Releases

The previous releases can be found [here](https://archive.apache.org/dist/mina/) and [here](https://archive.apache.org/dist/mina/mina/). Please note that the following releases contains a LGPL licensed file, rxtx-2.1.7.jar: 2.0.0-M4, 2.0.0-M5, 2.0.0-M6, 2.0.0-RC1.

# Version Numbering Scheme

The version number of MINA has the following form:

<major>.<minor>.<micro> \[-M<milestone number> or -RC<release candidate number>]

This scheme has three number components:

- The **major** number increases when there are incompatible changes in the API.
- The **minor** number increases when a new feature is introduced.
- The **micro** number increases when a bug or a trivial change is made.

and an optional label that indicates the maturity of a release:

- **M** (Milestone) means the feature set can change at any time in the next milestone releases. The last milestone release becomes the first release candidate after a vote.
- **RC** (Release Candidate) means the feature set is frozen and the next RC releases will focus on fixing problems unless there is a serious flaw in design. The last release candidate becomes the first GA release after a vote.
- No label implies **GA** (General Availability), which means the release is stable enough and therefore ready for production environment.

MINA is not a stand-alone software, so ‘the feature set’ here also includes the API of the newly introduced features and the overall architecture of the software,

Here’s an example that illustrates how MINA version number increases:

2.0.0-M1 -> 2.0.0-M3 -> 2.0.0-M3 -> 2.0.0-M4 -> 2.0.0-RC1 -> 2.0.0-RC2 -> 2.0.0-RC3 -> **2.0.0** -> 2.0.1 -> 2.0.2 -> 2.2.7-M1 ...

Please note that we always specify the micro number, even if it’s zero.

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-downloads_old"></a>

# MINA Older Downloads — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-downloads_old--Navigation-Upcoming"></a>Upcoming

- No event

# Older MINA Releases

For people wanting to use the **serial** package, we don't include the **rxtx.jar** library in the releases, as it's under a LGPL license. Please download it from [http://rxtx.qbang.org/wiki/index.php/Download](http://rxtx.qbang.org/wiki/index.php/Download) or add the associated dependency in your maven pom.xml :

```xml
<dependency>
    <groupId>org.rxtx</groupId>
    <artifactId>rxtx</artifactId>
    <version>2.1.7</version>
    <scope>provided<scope>
</dependency>
```

## MINA 2.2.x

| Version | Download Links | Date |
| --- | --- | --- |
| ApacheDS MINA 2.2.6 | Download,Javadoc,Test javadoc,Xref,Xref test | 27/Apr/2026 |
| ApacheDS MINA 2.2.5 | Download,Javadoc,Test javadoc,Xref,Xref test | 28/Nov/2025 |
| ApacheDS MINA 2.2.4 | Download,Javadoc,Test javadoc,Xref,Xref test | 24/Dec/2024 |
| ApacheDS MINA 2.2.3 | Download,Javadoc,Test javadoc,Xref,Xref test | 12/Sep/2023 |
| ApacheDS MINA 2.2.2 | Download,Javadoc,Test javadoc,Xref,Xref test | 5/Jun/2023 |
| ApacheDS MINA 2.2.1 | Download,Javadoc,Test javadoc,Xref,Xref test | 24/Jul/2022 |
| ApacheDS MINA 2.2.0 | Download,Javadoc,Test javadoc,Xref,Xref test | 19/Jul/2022 |

## MINA 2.1.x

| Version | Download Links | Date |
| --- | --- | --- |
| ApacheDS MINA 2.1.11 | Download,Javadoc,Test javadoc,Xref,Xref test | 27/Apr/2026 |
| ApacheDS MINA 2.1.10 | Download,Javadoc,Test javadoc,Xref,Xref test | 24/Dec/2024 |
| ApacheDS MINA 2.1.9 | Download,Javadoc,Test javadoc,Xref,Xref test | 15/Oct/2023 |
| ApacheDS MINA 2.1.8 | Download,Javadoc,Test javadoc,Xref,Xref test | 12/Sep/2023 |
| ApacheDS MINA 2.1.7 | Download,Javadoc,Test javadoc,Xref,Xref test | 5/Jun/2023 |
| ApacheDS MINA 2.1.6 | Download,Javadoc,Test javadoc,Xref,Xref test | 18/Feb/2022 |
| ApacheDS MINA 2.1.5 | Download,Javadoc,Test javadoc,Xref,Xref test | 29/Oct/2021 |
| ApacheDS MINA 2.1.4 | Download,Javadoc,Test javadoc,Xref,Xref test | 24/Aug/2020 |
| ApacheDS MINA 2.1.3 | Download,Javadoc,Test javadoc,Xref,Xref test | 02/Jun/2019 |
| ApacheDS MINA 2.1.2 | Download,Javadoc,Test javadoc,Xref,Xref test | 20/Apr/2019 |
| ApacheDS MINA 2.1.1 | Download,Javadoc,Test javadoc,Xref,Xref test | 14/Apr/2019 |
| ApacheDS MINA 2.1.0 | Download,Javadoc,Test javadoc,Xref,Xref test | 14/Mar/2019 |

## MINA 2.0.x

| Version | Download Links | Date |
| --- | --- | --- |
| ApacheDS MINA 2.0.27 | Download,Javadoc,Test javadoc,Xref,Xref test | 24/Dec/2024 |
| ApacheDS MINA 2.0.26 | Download,Javadoc,Test javadoc,Xref,Xref test | 15/Oct/2023 |
| ApacheDS MINA 2.0.25 | Download,Javadoc,Test javadoc,Xref,Xref test | 12/Sep/2023 |
| ApacheDS MINA 2.0.24 | Download,Javadoc,Test javadoc,Xref,Xref test | 5/Jun/2023 |
| ApacheDS MINA 2.0.23 | Download,Javadoc,Test javadoc,Xref,Xref test | 18/Feb/2022 |
| ApacheDS MINA 2.0.22 | Download,Javadoc,Test javadoc,Xref,Xref test | 29/Oct/2021 |
| ApacheDS MINA 2.0.21 | Download,Javadoc,Test javadoc,Xref,Xref test | 14/Apr/2019 |
| ApacheDS MINA 2.0.20 | Download,Javadoc,Test javadoc,Xref,Xref test | 24/Feb/2019 |
| ApacheDS MINA 2.0.19 | Download,Javadoc,Test javadoc,Xref,Xref test | 11/Jun/2018 |
| ApacheDS MINA 2.0.18 | Download,Javadoc,Test javadoc,Xref,Xref test | 01/Jun/2018 |
| ApacheDS MINA 2.0.17 | Download,Javadoc,Test javadoc,Xref,Xref test | 15/Mar/2018 |
| ApacheDS MINA 2.0.16 | Download,Javadoc,Test javadoc,Xref,Xref test | 31/Oct/2016 |
| ApacheDS MINA 2.0.15 | Download,Javadoc,Test javadoc,Xref,Xref test | 27/Sep/2016 |
| ApacheDS MINA 2.0.14 | Download,Javadoc,Test javadoc,Xref,Xref test | 30/AUG/2016 |
| ApacheDS MINA 2.0.13 | Download,Javadoc,Test javadoc,Xref,Xref test | 15/Feb/2016 |
| ApacheDS MINA 2.0.12 | Download,Javadoc,Test javadoc,Xref,Xref test | 07/Feb/2016 |
| ApacheDS MINA 2.0.11 | Download,Javadoc,Test javadoc,Xref | 26/Jan/2016 |
| ApacheDS MINA 2.0.10 | Download,Javadoc, Test javadoc (N/A),Xref,Xref test | 16/Dec/2015 |
| ApacheDS MINA 2.0.9 | Download,Javadoc, Test javadoc (N/A),Xref, Xref test (N/A) | 25/Oct/2014 |
| ApacheDS MINA 2.0.8 | Download,Javadoc,Xref,Xref test | 20/Sep/2014 |
| ApacheDS MINA 2.0.7 | Download,Javadoc,Xref,Xref test | 17/Nov/2012 |
| ApacheDS MINA 2.0.6 | Download,Javadoc,Xref,Xref test | 05/Oct/2012 |
| ApacheDS MINA 2.0.5 | Download,Javadoc,Xref,Xref test | 25/Aug/2012 |
| ApacheDS MINA 2.0.4 | Download,Javadoc,Xref,Xref test | 16/Jun/2011 |
| ApacheDS MINA 2.0.3 | Download,Javadoc,Xref,Xref test | 15/Apr/2011 |
| ApacheDS MINA 2.0.2 | Download,Javadoc,Xref,Xref test | 17/Dec/2010 |
| ApacheDS MINA 2.0.1 | Download,Javadoc,Xref,Xref test | 28/Oct/2010 |
| ApacheDS MINA 2.0.0 | Download,Javadoc,Xref,Xref test | 27/Sep/2010 |
| ApacheDS MINA 2.0.0-RC1 | Download,Javadoc,Xref,Xref test | 20/Oct/2009 |
| ApacheDS MINA 2.0.0-M6 | Download,Javadoc,Xref,Xref test | 03/Jun/2009 |
| ApacheDS MINA 2.0.0-M5 | Download,Javadoc,Xref,Xref test | 19/Apr/2009 |
| ApacheDS MINA 2.0.0-M4 | Download,Javadoc,Xref,Xref test | 12/Dec/2008 |
| ApacheDS MINA 2.0.0-M3 | Download,Javadoc,Xref,Xref test | 12/Aug/2008 |
| ApacheDS MINA 2.0.0-M2 | Download,Javadoc,Xref,Xref test | 09/Jul/2008 |
| ApacheDS MINA 2.0.0-M1 | Download,Javadoc,Xref,Xref test | 19/Feb/2008 |

## MINA 1.1.x

Note: those versions are not maintained, those links are just provided for those interested in archeology…

| Version | Download Links | Date |
| --- | --- | --- |
| ApacheDS MINA 1.1.7 | Download,Javadoc | 23/Apr/2008 |
| ApacheDS MINA 1.1.6 | Download,Javadoc | 09/Feb/2008 |
| ApacheDS MINA 1.1.5 | Download,Javadoc | 26/Nov/2007 |
| ApacheDS MINA 1.1.4 | Download,Javadoc | 29/Oct/2007 |
| ApacheDS MINA 1.1.3 | Download,Javadoc | 17/Oct/2007 |
| ApacheDS MINA 1.1.2 | Download,Javadoc | 14/Aug/2007 |
| ApacheDS MINA 1.1.1 | Download,Javadoc | 19/Jul/2007 |
| ApacheDS MINA 1.1.0 | Download,Javadoc | 16/Apr/2007 |

## MINA 1.0.x

Note: those versions are not maintained, those links are just provided for those interested in archeology…

| Version | Download Links | Date |
| --- | --- | --- |
| ApacheDS MINA 1.0.10 | Download,Javadoc | 15/Apr/2008 |
| ApacheDS MINA 1.0.9 | Download,Javadoc | 09/Feb/2008 |
| ApacheDS MINA 1.0.8 | Download,Javadoc | 19/Nov/2007 |
| ApacheDS MINA 1.0.7 | Download,Javadoc | 29/Oct/2007 |
| ApacheDS MINA 1.0.6 | Download,Javadoc | 17/Oct/2007 |
| ApacheDS MINA 1.0.5 | Download,Javadoc | 14/Aug/2007 |
| ApacheDS MINA 1.0.4 | Download,Javadoc | 19/Jul/2007 |
| ApacheDS MINA 1.0.3 | Download,Javadoc | 16/Apr/2007 |
| ApacheDS MINA 1.0.2 | Download,Javadoc | 20/Feb/2007 |
| ApacheDS MINA 1.0.1 | Download, javadoc unavailable | 06/Dec/2006 |
| ApacheDS MINA 1.0.0 | Download,Javadoc | 02/Oct/2006 |

## MINA 0.9.x

Note: those versions are not maintained, those links are just provided for those interested in archeology…

| Version | Download Links | Date |
| --- | --- | --- |
| ApacheDS MINA 0.9.5 | Download,Javadoc | 05/Sep/2006 |
| ApacheDS MINA 0.9.4 | Download,Javadoc | 01/May/2006 |
| ApacheDS MINA 0.9.3 | Download,Javadoc | 04/Apr/2006 |
| ApacheDS MINA 0.9.2 | Download,Javadoc | 25/Feb/2006 |
| ApacheDS MINA 0.9.1 | Download,Javadoc | 02/Feb/2006 |
| ApacheDS MINA 0.9.0 | Download,Javadoc,Examples Javadoc,xref,Tests xref,Examples xref | 08/Dec/2005 |

## MINA 0.8.x

Note: those versions are not maintained, those links are just provided for those interested in archeology…

| Version | Download Links | Date |
| --- | --- | --- |
| ApacheDS MINA 0.8.4 | Download,Javadoc | 19/Nov/2006 |
| ApacheDS MINA 0.8.3 | Download,Javadoc | 02/Oct/2006 |
| ApacheDS MINA 0.8.2 | Download,Javadoc,Examples Javadoc,xref,Tests xref,Examples xref | 23/Dec/2005 |
| ApacheDS MINA 0.8.1 | Download,Javadoc,Examples Javadoc,xref,Tests xref,Examples xref | 11/Nov/2005 |
| ApacheDS MINA 0.8.0 | Download,Javadoc,Examples Javadoc,xref,Tests xref,Examples xref | 22/Oct/2005 |

## MINA 0.7.x

Note: those versions are not maintained, those links are just provided for those interested in archeology…

| Version | Download Links | Date |
| --- | --- | --- |
| ApacheDS MINA 0.7.4 | Download,Javadoc,Examples Javadoc,xref,Tests xref,Examples xref | 03/Sep/2005 |
| ApacheDS MINA 0.7.3 | Download,Javadoc,Examples Javadoc,xref,Tests xref,Examples xref | 11/Jul/2005 |
| ApacheDS MINA 0.7.2 | Download,Javadoc,Examples Javadoc,xref,Tests xref,Examples xref | 08/Jun/2005 |
| ApacheDS MINA 0.7.1 | Download,Javadoc,Examples Javadoc,xref,Tests xref,Examples xref | 23/May/2005 |

# Verify the integrity of the files

The PGP signatures can be verified using PGP or GPG. First download the [KEYS](https://downloads.apache.org/mina/KEYS) as well as the asc signature file for the relevant distribution. Then verify the signatures using:

```bash
$ pgpk -a KEYS
$ pgpv mina-2.0.20.tar.gz.asc
```

or

```bash
$ pgp -ka KEYS
$ pgp mina-2.0.20.tar.gz.asc
```

or

```bash
$ gpg --import KEYS
$ gpg --verify mina-2.0.20.tar.gz.asc
```

Alternatively, you can verify the checksums of the files (see the [How to verify downloaded files page](https://www.apache.org/info/verification.html)).

# Previous Releases

The previous releases can be found [here](https://archive.apache.org/dist/mina/) and [here](https://archive.apache.org/dist/mina/mina/). Please note that the following releases contains a LGPL licensed file, rxtx-2.1.7.jar: 2.0.0-M4, 2.0.0-M5, 2.0.0-M6, 2.0.0-RC1.

# Version Numbering Scheme

The version number of MINA has the following form:

<major>.<minor>.<micro> \[-M<milestone number> or -RC<release candidate number>]

This scheme has three number components:

- The **major** number increases when there are incompatible changes in the API.
- The **minor** number increases when a new feature is introduced.
- The **micro** number increases when a bug or a trivial change is made.

and an optional label that indicates the maturity of a release:

- **M** (Milestone) means the feature set can change at any time in the next milestone releases. The last milestone release becomes the first release candidate after a vote.
- **RC** (Release Candidate) means the feature set is frozen and the next RC releases will focus on fixing problems unless there is a serious flaw in design. The last release candidate becomes the first GA release after a vote.
- No label implies **GA** (General Availability), which means the release is stable enough and therefore ready for production environment.

MINA is not a stand-alone software, so ‘the feature set’ here also includes the API of the newly introduced features and the overall architecture of the software,

Here’s an example that illustrates how MINA version number increases:

2.0.0-M1 -> 2.0.0-M3 -> 2.0.0-M3 -> 2.0.0-M4 -> 2.0.0-RC1 -> 2.0.0-RC2 -> 2.0.0-RC3 -> **2.0.0** -> 2.0.1 -> 2.0.2 -> 2.1.0-M1 ...

Please note that we always specify the micro number, even if it’s zero.

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-faq"></a>

# FAQ — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-faq--Navigation-Upcoming"></a>Upcoming

- No event

# MINA FAQ

- [General](#mina-apache-org-mina-project-faq--general)
- [What does MINA mean?](#mina-apache-org-mina-project-faq--what-does-mina-mean)
  - [What transport does MINA support?](#mina-apache-org-mina-project-faq--what-transport-does-mina-support)
  - [How does MINA perform?](#mina-apache-org-mina-project-faq--how-does-mina-perform)
  - [Which version of MINA should I use?](#mina-apache-org-mina-project-faq--which-version-of-mina-should-i-use)
  - [What is required to build/run MINA?](#mina-apache-org-mina-project-faq--what-is-required-to-buildrun-mina)
  - [How can I get help?](#mina-apache-org-mina-project-faq--how-can-i-get-help)
  - [How / What can I contribute?](#mina-apache-org-mina-project-faq--how--what-can-i-contribute)
- [Can MINA…?](#mina-apache-org-mina-project-faq--can-mina)
  - [Can I use MINA to create client (or server) applications?](#mina-apache-org-mina-project-faq--can-i-use-mina-to-create-client-or-server-applications)
  - [Can MINA handle text protocols such as HTTP?](#mina-apache-org-mina-project-faq--can-mina-handle-text-protocols-such-as-http)
  - [Can MINA handle complex binary protocols such as LDAP?](#mina-apache-org-mina-project-faq--can-mina-handle-complex-binary-protocols-such-as-ldap)
  - [Can I implement protocols that keeps connection alive with MINA?](#mina-apache-org-mina-project-faq--can-i-implement-protocols-that-keeps-connection-alive-with-mina)
  - [Does MINA support SSL/TLS and SASL out-of-the-box?](#mina-apache-org-mina-project-faq--does-mina-support-ssltls-and-sasl-out-of-the-box)
  - [Do I need to make my IoHandler thread-safe?](#mina-apache-org-mina-project-faq--do-i-need-to-make-my-iohandler-thread-safe)
  - [What transport types can MINA support except TCP/IP and UDP/IP?](#mina-apache-org-mina-project-faq--what-transport-types-can-mina-support-except-tcpip-and-udpip)
  - [Does MINA support multicast?](#mina-apache-org-mina-project-faq--does-mina-support-multicast)
- [How do I…?](#mina-apache-org-mina-project-faq--how-do-i)
  - [How can I store session-specific information?](#mina-apache-org-mina-project-faq--how-can-i-store-session-specific-information)
  - [How can I separate an event handler into multiple handlers when I implement complex business logic?](#mina-apache-org-mina-project-faq--how-can-i-separate-an-event-handler-into-multiple-handlers-when-i-implement-complex-business-logic)
  - [How do I close my sessions and dispose my Connector?](#mina-apache-org-mina-project-faq--how-do-i-close-my-sessions-and-dispose-my-connector)
  - [How can I reconnect to server after my client session is closed?](#mina-apache-org-mina-project-faq--how-can-i-reconnect-to-server-after-my-client-session-is-closed)
  - [When should I implement my protocol handler using filters?](#mina-apache-org-mina-project-faq--when-should-i-implement-my-protocol-handler-using-filters)
  - [How can I detect when the remote peer doesn’t send a response message for my request message?](#mina-apache-org-mina-project-faq--how-can-i-detect-when-the-remote-peer-doesnt-send-a-response-message-for-my-request-message)
  - [How can I let MINA log messages using my favorite logging framework (i.e. Log4J)?](#mina-apache-org-mina-project-faq--how-can-i-let-mina-log-messages-using-my-favorite-logging-framework-ie-log4j)
- [Troubleshooting](#mina-apache-org-mina-project-faq--troubleshooting)
  - [I get OutOfMemoryError or response timeout and connection reset under heavy load.](#mina-apache-org-mina-project-faq--i-get-ttoutofmemoryerrortt-or-response-timeout-and-connection-reset-under-heavy-load)
  - [No data is writtin out to the session even if the buffer is not empty.](#mina-apache-org-mina-project-faq--no-data-is-writtin-out-to-the-session-even-if-the-buffer-is-not-empty)
  - [I created an SSL client with MINA, but it doesn’t initiate any handshake after the session is open.](#mina-apache-org-mina-project-faq--i-created-an-ssl-client-with-mina-but-it-doesnt-initiate-any-handshake-after-the-session-is-open)
  - [Why does SocketConnector send several messages as one message?](#mina-apache-org-mina-project-faq--why-does-socketconnector-send-several-messages-as-one-message)
  - [I get InvalidClassChangeError.](#mina-apache-org-mina-project-faq--i-get-ttinvalidclasschangeerrortt)
  - [My server fails with java.net.SocketException: Too many files open<](#mina-apache-org-mina-project-faq--my-server-fails-with-javanetsocketexception-too-many-files-open)

## General

## What does MINA mean?

MINA is:

- An acronym for ‘Multipurpose Infrastructure for Network Applications’;
- A girl’s name;
- ‘South’ in Japanese;
- ‘Mine’ (as in mineshaft) in Spanish and Portuguese;
- Look at [Wikipedia](http://en.wikipedia.org/wiki/Mina) for more meanings.

### What transport does MINA support?

MINA currently supports TCP and UDP based on Java NIO API, provides support for serial port communication, and transports based on [Apache Portable Runtime](http://apr.apache.org/).

### How does MINA perform?

It is known to perform as good as C/C++ servers. Please refer to the [Performance Test Reports](#mina-apache-org-mina-project-performances "Performance Test Reports") or the [Testimonials](#mina-apache-org-mina-project-testimonials "Testimonials").

### Which version of MINA should I use?

Use the latest point-release of 2.0 (for Java 8 or above). 1.0 and 1.1 aren’t maintained anymore.

### What is required to build/run MINA?

JDK 7 or above is required to build MINA.

MINA core module depends on two libraries, SLF4J and backport-util-concurrent (for 1.0):

[SLF4J (Simple Logging Facade for Java)](http://www.slf4j.org/), a logging framework from the author of [Log4J](http://logging.apache.org/log4j/1.2/index.html). SLF4J is very similar to [Commons-Logging](http://jakarta.apache.org/commons/logging/), but it doesn’t cause any class loader issues at all. SLF4J provides bindings for Log4J, JDK 1.4 logging API, and NLog4J. Please put an appropriate SLF4J JAR file which corresponds to your favorite logging framework to the classpath as SLF4J documentation explains.

[Spring framework](http://www.springframework.org/) and [JZlib](http://www.jcraft.com/jzlib/) are also required to build integration-spring and filter-compression module.

### How can I get help?

The primary source to get help is the [User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc). You can also contact us via [various channels](../contact.html) to ask questions on MINA or to contribute to it.

### How / What can I contribute?

You can contribute anything related with MINA; examples, useful codecs for existing protocols, tutorials, feature improvements, bug fixes, benchmarks, and whatever. Please [contact us](../contact.html) without hesitation.

## Can MINA…?

### Can I use MINA to create client (or server) applications?

Yes. You can create both client and server applications with MINA. Please take a look at IoConnector and IoAcceptor.

### Can MINA handle text protocols such as HTTP?

Yes. Please take a look at [the Reversed and HTTP server examples](#mina-apache-org-mina-project-documentation). [AsyncWeb](/asyncweb-project/index.html) is a HTTP server implementation based on MINA.

### Can MINA handle complex binary protocols such as LDAP?

Yes. Please take a look at [the SumUp example](#mina-apache-org-mina-project-documentation). There is no full ASN.1 support yet, but we will implement it someday and you can contribute to make it available sooner.

### Can I implement protocols that keeps connection alive with MINA?

Yes. MINA doesn’t close any connections unless you called `IoSession.close()` or connection is closed by the remote peer.

### Does MINA support SSL/TLS and SASL out-of-the-box?

We support SSL/TLS out-of-the-box. Please refer to `SSLFilter`. It also provides a way to implement StartTLS. JDK 7 provides complete SASL support which works well with MINA.

### Do I need to make my IoHandler thread-safe?

It depends on your implementation. If you access the resource which is shared across multiple sessions, you have to make it thread-safe. If the resource is not shared at all and accessed by only one session (e.g. storing context information as a session attribute), then you don’t need to make it thread-safe. It is because all events generated by MINA are transmitted to your handler in order (when using the Executor Filter), and the newer event is not processed if the event handler method for the older event for the same session hasn’t returned yet.

### What transport types can MINA support except TCP/IP and UDP/IP?

Virtually all kind of transport types. MINA API is designed to be transport-independent. You can implement any transport type support only if you can conform to MINA API. Support for Pre-1.4 I/O (aka BIO), reliable multicast, Java Communications API, and file I/O are planned.

### Does MINA support multicast?

Not yet. Java NIO doesn’t support multicast yet. Multicast for NIO will be available in Java SE 7, Dolphin. We are seriously considering to implement multicasts using pre-1.4 Java API.

## How do I…?

### How can I store session-specific information?

Sessions are capable of custom attributes that you can add or remove at any time. These custom attributes are not shared between sessions; it is designed to store session specific information.

### How can I separate an event handler into multiple handlers when I implement complex business logic?

Please refer to DemuxingIoHandler.

### How do I close my sessions and dispose my Connector?

You have to do it in two steps : first close your sessions, then dispose the connector. Of course, if you dispose your Connector first, then all the sessions will be closed. Here is the code :

```java
ConnectFuture cf = connector.connect(new InetSocketAddress("localhost", 8080));

// Get the close future for this session
CloseFuture closeFuture = cf.getSession().getCloseFuture();

// Adding a listener to this close event
closeFuture.addListener((IoFutureListener<?>) new IoFutureListener<IoFuture>() {
        @Override
        public void operationComplete(IoFuture future) {
            System.out.println("The session is now closed");
        }
});

// Do the close requesting that the pending messages are sent before
// the session is closed
closeFuture.getSession().close(false);

// Now wait for the close to be completed
closeFuture.awaitUninterruptibly();

// We can now dispose the connector
connector.dispose();
```

### How can I reconnect to server after my client session is closed?

Here is an example code:

```java
public void sessionClosed( IoSession session ) throws Exception {
    // Wait for five seconds before reconnecting.
    Thread.sleep( 5000 );

    // Reconnect.
    connector.connect( session.getRemoteAddress(), this );
}
```

Possibly it would be better to extract this code to a method like `reconnect()` so that it can reusable in more than one place.

### When should I implement my protocol handler using filters?

`IoFilter` is usually considered reusable just like we think about Servlet filters. Please implement commonly used business logic such as authorization and logging as a filter. In case you implement just complex multi-layer protocols like Kerberos, you could consider using `org.apache.mina.handler.chain`> package.

### How can I detect when the remote peer doesn’t send a response message for my request message?

You can’t use `sessionIdle` event simply here. You’ll have to use `java.util.concurrent.ScheduledExecutor` (or [OpenSymphony Quartz](http://www.opensymphony.com/quartz/) as an alternative). Schedule a timeout task to be executed on timeout situation for each request message, and cancel it when you receive the corresponding response message.

### How can I let MINA log messages using my favorite logging framework (i.e. Log4J)?

Please refer to ‘Swapping implementations at deployment time’ section in [the SLF4J Manual](http://www.slf4j.org/manual.html).

## Troubleshooting

### I get OutOfMemoryError or response timeout and connection reset under heavy load.

We recommend to switch the default buffer type to ‘heap’ by inserting the following code before you start a server:

```java
ByteBuffer.setUseDirectBuffers(false);
ByteBuffer.setAllocator(new SimpleByteBufferAllocator());
```

If you prefer direct buffers to heap buffers, JVM might have ran out of direct memory. Please try increasing maximum direct memory size using **-XX:MaxDirectMemorySize** option (e.g. **-XX:MaxDirectMemorySize=128M**)

### No data is writtin out to the session even if the buffer is not empty.

Please make sure if you called `ByteBuffer.flip()` to flip the buffer before writing the buffer out. It is a common mistake NIO beginners make.

### I created an SSL client with MINA, but it doesn’t initiate any handshake after the session is open.

Please make sure you called `SSLFilter.setUseClientMode(true)` before you initiate a connection. Server developers will also have to disconnect users who doesn’t initiate SSL handshake by setting `IoSession.readerIdleTime` and closing the session in `IoHandler.sessionIdle()`.

### Why does SocketConnector send several messages as one message?

*For example, I tried using SocketConnector to send “abc” and “def”, but it sent “abcdef”. Is it a MINA bug?*

No, this is due to your OS trying to send packets more efficiently (see [Nagle algorithm](http://en.wikipedia.org/wiki/Nagle_algorithm)). You can enable/disable Nagle’s algorithm by a call to SocketSessionConfig.setTcpNoDelay(), e.g.:

```java
((SocketSessionConfig) connector.getSessionConfig()).setTcpNoDelay(false)
```

However, even if you do this you cannot expect one session.write(bytes) in MINA to correspond to one TCP packet on your network. You should probably implement your own MINA ProtocolDecoder to handle the assembly of incoming bytes into message objects. The TextLineCodec is a good start if the protocol you’re implementing is based on text lines. For a more advanced example have a look at the SumUp example in the MINA distribution.

### I get InvalidClassChangeError.

Please make sure if you are using the appropriate SLF4J version. You will get `InvalidClassChangeError` if you are using outdated SLF4J release.

### My server fails with java.net.SocketException: Too many files open<

Network sockets are treated like files and your operating system has a limit to the number of file handles it can manage. Running out of file handles is usually due to a large number of clients connecting and disconnecting frequently. As specified by TCP, after being closed sockets remain in the TIME\_WAIT state for some additional time. The reason is to ensure that delayed packets arrive on the correct socket. In Windows, the default TIME\_WAIT timeout is 4 minutes, in Linux it is 60 seconds.

#### Change the timeout in Windows

1. Run regedit to start the Registry Editor
2. Locate the following key: HKEY\_LOCAL\_MACHINE\System\CurrentControlSet\Services\tcpip\Parameters
3. Add a new value named TcpTimedWaitDelay asa decimal and set the desired timeout in seconds (30-300)
4. Reboot

#### Change the timeout in Linux

1. Update the configuration value by running (30 seconds used in the example)

   ```bash
    echo 30 > /proc/sys/net/ipv4/tcp_fin_timeout
   ```
2. Restart the networking component, for example by running

   ```text
    /etc/init.d/networking restart
   ```

   or

   ```text
    service network restart
   ```

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-features"></a>

# Features — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-features--Navigation-Upcoming"></a>Upcoming

- No event

# Features

MINA is a simple yet full-featured network application framework which provides:

- Unified API for various transport types:
  - TCP/IP & UDP/IP via Java NIO
  - Serial communication (RS232) via RXTX
  - In-VM pipe communication
  - You can implement your own!
- Filter interface as an extension point; similar to Servlet filters
- Low-level and high-level API:
  - Low-level: uses ByteBuffers
  - High-level: uses user-defined message objects and codecs
- Highly customizable thread model:
  - Single thread
  - One thread pool
  - More than one thread pools (i.e. [SEDA](https://web.archive.org/web/20061208181754/http://www.eecs.harvard.edu/~mdw/papers/mdw-phdthesis.pdf))
- Out-of-the-box SSL · TLS · StartTLS support using Java 5 `SSLEngine`
- Overload shielding & traffic throttling
- Unit testability using mock objects
- JMX managability
- Stream-based I/O support via `StreamIoHandler`
- Integration with well known containers such as PicoContainer and Spring
- Smooth migration from Netty, an ancestor of Apache MINA.

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-index"></a>

# MINA Home — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-index--Navigation-Upcoming"></a>Upcoming

- No event

# Welcome to Apache MINA

## Overview

Apache MINA is a network application framework which helps users develop high performance and high scalability network applications easily. It provides an abstract · event-driven · asynchronous API over various transports such as TCP/IP and UDP/IP via Java NIO.

Apache MINA is often called:

- NIO framework · library,
- client · server framework · library, or
- a networking · socket library.

However, it’s much more than that. Please take a look around the list of the *[features](#mina-apache-org-mina-project-features)* that enable rapid network application development, and *[what people says about MINA](#mina-apache-org-mina-project-testimonials)*.

Please grab yourself a *[2.0.x download](#mina-apache-org-mina-project-downloads_2_0)* or a *[2.1.x download](#mina-apache-org-mina-project-downloads_2_1)*, try our *[Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)*, surf our *[FAQ](#mina-apache-org-mina-project-faq)* or start join us on *[our community](../contact.html)*

# News

## MINA 2.2.7, 2.1.12 released *posted on April, 30 2026*

The MINA project is pleased to announce the MINA 2.2.7 and 2.1.12 release.

This issue fixes two critical security issues, which were expected to have been fixed by the previous release. Sadly the code change that was supposed to be applied to the three versions was only applied to the 2.0.X branch, leaving 2.2.6 and 2.1.11 in the same state than before. These new releases correct this mistake.

### [CVE-2026-42778](https://www.cve.org/CVERecord?id=CVE-2026-42778)

Note: this is the exact same CVE than *CVE-2026-41409*

**MINA** applications using unbounded deserialization may allow **RCE**.

Affected versions:

- Apache MINA 2.1 through 2.1.11
- Apache MINA 2.2 through 2.2.6

Description:

The *ObjectSerializationDecoder* in Apache **MINA** uses **Java** native deserialization protocol to process
incoming serialized data but lacks the necessary security checks and defenses. This vulnerability allows
attackers to exploit the deserialization process by sending specially crafted malicious serialized data,
potentially leading to remote code execution (**RCE**) attacks.

A security release has been issued in Decmber 2024, but was incomplete. An allow-list of classes was added to tell MINA which classes can be used by the deserialization of messages through the *AbstractIoBuffer.getObject()* method, but it was applied too late for classes that have a static initializer which get executed even for not allowed classes.

### [CVE-2026-42779](https://www.cve.org/CVERecord?id=CVE-2026-42779)

Note: this is the exact same CVE than *CVE-2026-41635*

**MINA** applications using unbounded deserialization may allow **RCE**.

Affected versions:

- Apache MINA 2.1 through 2.1.11
- Apache MINA 2.2 through 2.2.6

Description:

The *ObjectSerializationDecoder* in Apache **MINA** uses **Java** native deserialization protocol to process incoming serialized data but lacks the necessary security checks and defenses. This vulnerability allows attackers to exploit the deserialization process by sending specially crafted malicious serialized data,
potentially leading to remote code execution (**RCE**) attacks.

A security release has been issued in Decmber 2024, but was incomplete. An allow-list of classes was added to tell MINA which classes can be used by the deserialization of messages through the *AbstractIoBuffer.getObject()* method, but static classes or primitives types are bypassing this check.

## Versions affected

These issues affects **MINA** core versions 2.1.X and 2.2.X, and is fixed by the releases 2.1.12 and 2.2.7.

## Mitigation

It’s also important to note that an application using **MINA** core library will only be affected if the *IoBuffer#getObject()* method is called, and this specific method is potentially called when adding a *ProtocolCodecFilter* instance using the *ObjectSerializationCodecFactory* class in the filter chain. If your application is specifically using those classes, you have to upgrade to the latest version of **MINA** core library.

**Upgrading will not be enough: you also need to explicitly allow the classes the decoder will accept in the *ObjectSerializationDecoder* instance, using one of the three new methods:**

```java
    /**
     * Accept class names where the supplied ClassNameMatcher matches for
     * deserialization, unless they are otherwise rejected.
     *
     * @param classNameMatcher the matcher to use
     */
    public void accept(ClassNameMatcher classNameMatcher)

    /**
     * Accept class names that match the supplied pattern for
     * deserialization, unless they are otherwise rejected.
     *
     * @param pattern standard Java regexp
     */
    public void accept(Pattern pattern) 

    /**
     * Accept the wildcard specified classes for deserialization,
     * unless they are otherwise rejected.
     *
     * @param patterns Wildcard file name patterns as defined by
     *                  org.apache.commons.io.FilenameUtils#wildcardMatch(String, String)
     */
    public void accept(String... patterns)
```

By default, the decoder will reject *all* classes that will be present in the incoming data.

Note: The **FtpServer**, **SSHd** and **Vysper** sub-project are not affected by this issue.

## MINA 2.2.6, 2.1.11, 2.0.28 released *posted on April, 27 2026*

The MINA project is pleased to announce the MINA 2.2.6, 2.1.11 and 2.0.28 release.

This issue fixes two critical security issues:

### [CVE-2026-41409](https://www.cve.org/CVERecord?id=CVE-2026-41409)

**MINA** applications using unbounded deserialization may allow **RCE**.

Affected versions:

- Apache MINA 2.0 through 2.0.27
- Apache MINA 2.1 through 2.1.10
- Apache MINA 2.2 through 2.2.5

Description:

The *ObjectSerializationDecoder* in Apache **MINA** uses **Java** native deserialization protocol to process
incoming serialized data but lacks the necessary security checks and defenses. This vulnerability allows
attackers to exploit the deserialization process by sending specially crafted malicious serialized data,
potentially leading to remote code execution (**RCE**) attacks.

A security release has been issued in Decmber 2024, but was incomplete. An allow-list of classes was added to tell MINA which classes can be used by the deserialization of messages through the *AbstractIoBuffer.getObject()* method, but it was applied too late for classes that have a static initializer which get executed even for not allowed classes.

### [CVE-2026-41635](https://www.cve.org/CVERecord?id=CVE-2026-41635)

**MINA** applications using unbounded deserialization may allow **RCE**.

Affected versions:

- Apache MINA 2.0 through 2.0.27
- Apache MINA 2.1 through 2.1.10
- Apache MINA 2.2 through 2.2.5

Description:

The *ObjectSerializationDecoder* in Apache **MINA** uses **Java** native deserialization protocol to process incoming serialized data but lacks the necessary security checks and defenses. This vulnerability allows attackers to exploit the deserialization process by sending specially crafted malicious serialized data,
potentially leading to remote code execution (**RCE**) attacks.

A security release has been issued in Decmber 2024, but was incomplete. An allow-list of classes was added to tell MINA which classes can be used by the deserialization of messages through the *AbstractIoBuffer.getObject()* method, but static classes or primitives types are bypassing this check.

## Versions affected

These issues affects **MINA** core versions 2.0.X, 2.1.X and 2.2.X, and is fixed by the releases 2.0.28, 2.1.11 and 2.2.6.

## Mitigation

It’s also important to note that an application using **MINA** core library will only be affected if the *IoBuffer#getObject()* method is called, and this specific method is potentially called when adding a *ProtocolCodecFilter* instance using the *ObjectSerializationCodecFactory* class in the filter chain. If your application is specifically using those classes, you have to upgrade to the latest version of **MINA** core library.

**Upgrading will not be enough: you also need to explicitly allow the classes the decoder will accept in the *ObjectSerializationDecoder* instance, using one of the three new methods:**

```java
    /**
     * Accept class names where the supplied ClassNameMatcher matches for
     * deserialization, unless they are otherwise rejected.
     *
     * @param classNameMatcher the matcher to use
     */
    public void accept(ClassNameMatcher classNameMatcher)

    /**
     * Accept class names that match the supplied pattern for
     * deserialization, unless they are otherwise rejected.
     *
     * @param pattern standard Java regexp
     */
    public void accept(Pattern pattern) 

    /**
     * Accept the wildcard specified classes for deserialization,
     * unless they are otherwise rejected.
     *
     * @param patterns Wildcard file name patterns as defined by
     *                  org.apache.commons.io.FilenameUtils#wildcardMatch(String, String)
     */
    public void accept(String... patterns)
```

By default, the decoder will reject *all* classes that will be present in the incoming data.

Note: The **FtpServer**, **SSHd** and **Vysper** sub-project are not affected by this issue.

## MINA 2.2.5 released *posted on November, 28, 2025*

The MINA project is pleased to announce the MINA 2.2.5 release.

This is a bug fix release, which fixes the following issues:

- A potential issue with the selector not being protected against concurrent access, found by Adam Herring
- A (temporary) fix for DIRMINA-423, brought by Jan Zelmer, forbiding asynchronous tasks to be executed.

## MINA 2.2.4, 2.1.10, 2.0.27 released *posted on December, 24, 2024*

The MINA project is pleased to announce the MINA 2.2.4, 2.1.10 and 2.0.27 release.

### [CVE-2024-52046](https://www.cve.org/CVERecord?id=CVE-2024-52046)

**MINA** applications using unbounded deserialization may allow **RCE**.

Affected versions:

- Apache MINA 2.0 through 2.0.26
- Apache MINA 2.1 through 2.1.9
- Apache MINA 2.2 through 2.2.3

Description:

The *ObjectSerializationDecoder* in Apache **MINA** uses **Java** native deserialization protocol to process
incoming serialized data but lacks the necessary security checks and defenses. This vulnerability allows
attackers to exploit the deserialization process by sending specially crafted malicious serialized data,
potentially leading to remote code execution (**RCE**) attacks.

This issue affects **MINA** core versions 2.0.X, 2.1.X and 2.2.X, and is fixed by the releases 2.0.27, 2.1.10 and 2.2.4.

It’s also important to note that an application using **MINA** core library will only be affected if the *IoBuffer#getObject()* method is called, and this specific method is potentially called when adding a *ProtocolCodecFilter* instance using the *ObjectSerializationCodecFactory* class in the filter chain. If your application is specifically using those classes, you have to upgrade to the latest version of **MINA** core library.

**Upgrading will not be enough: you also need to explicitly allow the classes the decoder will accept in the *ObjectSerializationDecoder* instance, using one of the three new methods:**

```java
    /**
     * Accept class names where the supplied ClassNameMatcher matches for
     * deserialization, unless they are otherwise rejected.
     *
     * @param classNameMatcher the matcher to use
     */
    public void accept(ClassNameMatcher classNameMatcher)

    /**
     * Accept class names that match the supplied pattern for
     * deserialization, unless they are otherwise rejected.
     *
     * @param pattern standard Java regexp
     */
    public void accept(Pattern pattern) 

    /**
     * Accept the wildcard specified classes for deserialization,
     * unless they are otherwise rejected.
     *
     * @param patterns Wildcard file name patterns as defined by
     *                  org.apache.commons.io.FilenameUtils#wildcardMatch(String, String)
     */
    public void accept(String... patterns)
```

By default, the decoder will reject *all* classes that will be present in the incoming data.

Note: The **FtpServer**, **SSHd** and **Vysper** sub-project are not affected by this issue.

## MINA 2.1.9, 2.0.26 released *posted on October, 15, 2024*

The MINA project is pleased to announce the MINA 2.1.9 and 2.0.26 release.

### Changes

Those versions are a maintenance release, fixing a bug in the way we
treat Strings when reading a IoBuffer:

- [DIRMINA-1181](https://issues.apache.org/jira/browse/DIRMINA-1181): Exception thrown when attempting to decode certain UTF-16 chars

## MINA 2.2.3, 2.1.8, 2.0.25 released *posted on September, 12, 2023*

The MINA project is pleased to announce the MINA 2.2.3, 2.1.8 and 2.0.25 release.

### Changes

Those versions are fixing some Datagram session issue:

- [DIRMINA-996](https://issues.apache.org/jira/browse/DIRMINA-996) IoSessionRecycler RemoteAddress Collision
- [DIRMINA-1172](https://issues.apache.org/jira/browse/DIRMINA-1172) Multiple DatagramAcceptors and the creation of a session object

## MINA 2.2.2, 2.1.7, 2.0.24 released *posted on June, 05, 2023*

The MINA project is pleased to announce the MINA 2.2.2, 2.1.7 and 2.0.24 release.

### Changes

Those versions are fixing some SSL/TLS issues and bring some added features:

- [DIRMINA-1122](https://issues.apache.org/jira/browse/DIRMINA-1122) support for endpoint identification algorithm (thanks to Marcin L)
- [DIRMINA-1157](https://issues.apache.org/jira/browse/DIRMINA-1157) A fix for a sporadic SSL/TLS connection establishement for version 2.0.X and 2.1.X (thanks to Steffen Liersch)
- [DIRMINA-1169](https://issues.apache.org/jira/browse/DIRMINA-1169) A fix in the Acceptor for Java 11 and upper (thanks to Thomas Wolf)

## MINA 2.2.1 released *posted on july, 24, 2022*

The MINA project is pleased to announce the MINA 2.2.1 release.

### Changes

This new version is just a fix in some **OSGi** export declaration that was done wrong in the previous release.

## MINA 2.2.0 released *posted on july, 19, 2022*

The MINA project is pleased to announce the MINA 2.2.0 release.

### Changes

This new version comes with complete rewrite of the **SSL/TLS** layer. The previous implementation had some flaws that were difficult to fix or workaround, and with the arrival of **TLS-1.3**, it was the opportunity to review and recode this part, which is the main change.

For any information about the API modifications and the impact on existing application, please read the [2.2 vs 2.1 page](#mina-apache-org-mina-project-2-2-vs-2-1).

## MINA 2.1.6 & MINA 2.0.23 released *posted on Februray, 18, 2022*

The MINA project is pleased to announce two new releases, MINA 2.1.6 and MINA 2.0.23.

### MINA 2.1.6 fixes

Here is the list of fixed issues :

- [DIRMINA-1152](https://issues.apache.org/jira/browse/DIRMINA-1152) IoServiceStatistics introduces huge latencies
- [DIRMINA-1156](https://issues.apache.org/jira/browse/DIRMINA-1156) Inconsistent worker / idleWorker in OrderedThreadPoolExecutor

It also contain some minor fixes (ignored tests being fixed, a minor
infinite loop fixed in the Buffer toString() method if used in some
corner case, etc)

For any information about the API modifications and the impact on existing application, please read the [2.1 vs 2.0 page](#mina-apache-org-mina-project-2-1-vs-2-0).

### MINA 2.0.23

This is a maintenance release for MINA 2.0.

It contains many backported issues from the 2.1 and 2.2 branches.

## MINA 2.1.5 & MINA 2.0.22 released *posted on October, 29, 2021*

The MINA project is pleased to announce two new releases, MINA 2.1.5 and MINA 2.0.22.

**These are fixing a critical issue, CVE-2021-41973**

CVE-2021-41973: ‘Apache MINA HTTP listener DoS’

**We urge anyone using any previous MINA version to migrate to one of those two new versions**

## MINA 2.1.4 released *posted on August, 24, 2020*

The MINA project is pleased to announce a new release, MINA 2.1.4. This is a bug fix release. Here are the fixed issues :

Bugs

- [DIRMINA-966](https://issues.apache.org/jira/browse/DIRMINA-966) NIO Datagram messages can get duplicated when unable to be sent by the underlying DatagramChannel
- [DIRMINA-1014](https://issues.apache.org/jira/browse/DIRMINA-1014) SocketAcceptor doesn’t unbind correctly
- [DIRMINA-1115](https://issues.apache.org/jira/browse/DIRMINA-1115) Filter ProfilerTimerFilter ArithmeticException
- [DIRMINA-1123](https://issues.apache.org/jira/browse/DIRMINA-1123) Receive buffer size is never set for NIO acceptor
- [DIRMINA-1126](https://issues.apache.org/jira/browse/DIRMINA-1126) filterWrite in ProtocolCodecFilter can send corrupted writeRequest message to the next filter
- [DIRMINA-1064](https://issues.apache.org/jira/browse/DIRMINA-1064) Implement cipher suites preference flag introduced in JDK 8
- [DIRMINA-1105](https://issues.apache.org/jira/browse/DIRMINA-1105) SSLHandler buffer handling

For any information about the API modifications and the impact on existing application, please read the [2.1 vs 2.0 page](#mina-apache-org-mina-project-2-1-vs-2-0).

## MINA 2.1.3 released *posted on June, 2, 2019*

The MINA project is pleased to announce a new release, MINA 2.1.3. This is a bug fix release: it fixes a 100% CPU usage in some corner case. Here are the fixed issues :

- [DIRMINA-1095](https://issues.apache.org/jira/browse/DIRMINA-1095) Seems like the management f UDP sessions is really unneficient
- [DIRMINA-1107](https://issues.apache.org/jira/browse/DIRMINA-1107) SslHandler flushScheduledEvents race condition, redux
- [DIRMINA-1111](https://issues.apache.org/jira/browse/DIRMINA-1111) 100% CPU (epoll bug) on 2.1.x, Linux only
- [DIRMINA-1104](https://issues.apache.org/jira/browse/DIRMINA-1104) IoBufferHexDumper.getHexdump(IoBuffer in, int lengthLimit) does not truncate the output

For any information about the API modifications and the impact on existing application, please read the [2.1 vs 2.0 page](#mina-apache-org-mina-project-2-1-vs-2-0).

## MINA 2.1.2 released *posted on April, 20, 2019*

The MINA project is pleased to announce a new release, MINA 2.1.2. This is a bug fix release: it fixes an issue for applications using *SSL/TLS*, which will stall waiting on a *WriteFuture* because it does not get signaled when the message has been fully sent.

For any information about the API modifications and the impact on existing application, please read the [2.1 vs 2.0 page](#mina-apache-org-mina-project-2-1-vs-2-0).

## MINA 2.1.1 & MINA 2.0.21 released *posted on April, 14, 2019*

The MINA project is pleased to announce two new releases, MINA 2.1.1 and MINA 2.0.21.

**These are fixing a critical issue, CVE-2019-0231**

CVE-2019-0231: ‘Handling of the close\_notify SSL/TLS message does not lead to a connection closure, leading the server to retain the socket opened and to have the client potentially receive clear-text messages which were supposed to be encrypted.’

MINA 2.1.1 also fixes the *CompressionFilter* usage, by simplifying the way we proceed with writes. A side effect is that it should be slightly faster to write data from an application. (This fix is not included in 2.0.21)

**We urge anyone using any previous MINA version to migrate to one of those two new versions**

## MINA 2.1.0 released *posted on March, 14, 2019*

The MINA project is pleased to announce a new release, MINA 2.1.0. This is a evolution over the
2.0.x branch, with some API modifications that makes it incompatible.

That means some effort will be required from applications to be able to use Apache MINA
2.1.0 as a replacement for Apache MINA 2.0.20.

Otherwise, every fix applied in Apache MINA 2.0.20 has been ported to this version, so one can still keep going with Apache MINA 2.0.20 which will be maintained for the coming months.

For any information about the API modifications and the impact on existing application, please read the [2.1 vs 2.0 page](#mina-apache-org-mina-project-2-1-vs-2-0).

## MINA 2.0.20 released *posted on February, 24, 2019*

The MINA project is pleased to announce a new release, MINA 2.0.20, fixing some API issues:

- [DIRMINA-1092](https://issues.apache.org/jira/browse/DIRMINA-1092) Removed a spurious printstacktrace
- [DIRMINA-1098](https://issues.apache.org/jira/browse/DIRMINA-1098) handshakeStatus variable has been wrongly made global
- [DIRMINA-1088](https://issues.apache.org/jira/browse/DIRMINA-1088) the OrderedThreadPool implementation has been made Java 10 compatible

We urge you to switch to this version if you were using MINA 2.0.19 or any older version.

## MINA 2.0.19 released *posted on June, 11, 2018*

The MINA project is pleased to announce a new release, MINA 2.0.19, fixing some API regression:

- the ‘event’ message has been removed from the IoHandler interface
- the SESSION\_SECURED/SESSION\_UNSECURED message are back

Those changes have been introduced in MINA 2.0.18 by mistake, and break applications that were
working with MINA 2.0.17.

They will be reintroduced in MINA 2.1.0

We urge you to switch to this version if you were using MINA 2.0.17 or any older version.

## MINA 2.0.18 released *posted on June, 1, 2018*

The MINA project is pleased to announce a new bug fix release, MINA 2.0.18.

There is some important addition in this version, the IoHandler interface now exposes a
new method :

```text
void event(IoSession session, FilterEvent event) throws Exception;
```

This can be used by any filter to generate a specific event (which will
be handled on demand by the application).

Currently, the only added event is defined in SslEvent, and it tells if
the session has been secured (ie the Handshake has completed) or isn’t
anymore.

It changes one thing in your application: if you were implementing
IoHandler, you have to add this method. You may also extends
IoHandlerAdapter which has a void implementation of this event() method.

The few fixes bugs/added features are:

- Added a flag to tell the Handshake to start immediately or not
- The IoBufferHexDumper implementation now does not modify the IoBuffer
  position
- Some missing synchronization have been added in teh SslFilter
- The suspendRead call is handled for Datagrams, instead of throwing an
  exception

We urge you to switch to this version if you were using MINA 2.0.17 or any older version.

## MINA 2.0.17 released *posted on March, 15, 2018*

The MINA project is pleased to announce a new bug fix release, MINA 2.0.17. It fixes many issues, and adds the missing Javadoc.

Here are the fixed issues :

## Bugs :

- [DIRMINA-844](https://issues.apache.org/jira/browse/DIRMINA-844) - Http Proxy Authentication failed to complete (see description for exact point of failure)
- [DIRMINA-1002](https://issues.apache.org/jira/browse/DIRMINA-1002) - Mina IoHandlerEvents missing inputClosed enum item.
- [DIRMINA-1051](https://issues.apache.org/jira/browse/DIRMINA-1051) - The MD5withRSA cipher is not anymore supported by Java 8, and our tests certificates have been generated with it.
- [DIRMINA-1052](https://issues.apache.org/jira/browse/DIRMINA-1052) - Fix the mvn-site command
- [DIRMINA-1056](https://issues.apache.org/jira/browse/DIRMINA-1056) - IllegalArgumentException when setting max and minReadBufferSize > 65536 (default)
- [DIRMINA-1057](https://issues.apache.org/jira/browse/DIRMINA-1057) - AbstractIoSession getScheduledWriteMessages always -negative?
- [DIRMINA-1059](https://issues.apache.org/jira/browse/DIRMINA-1059) - NioProcessor’s selector is synchronized but accessed outside
- [DIRMINA-1060](https://issues.apache.org/jira/browse/DIRMINA-1060) - Handle the spinning selectors in Socket/Datagram Acceptor and Connector
- [DIRMINA-1072](https://issues.apache.org/jira/browse/DIRMINA-1072) - SslFilter does not account for SSLEngine runtime exceptions
- [DIRMINA-1073](https://issues.apache.org/jira/browse/DIRMINA-1073) - NioSocketSession#isSecured does not comply with interface contract
- [DIRMINA-1076](https://issues.apache.org/jira/browse/DIRMINA-1076) - Leaking NioProcessors/NioSocketConnectors hanging in call to dispose
- [DIRMINA-1077](https://issues.apache.org/jira/browse/DIRMINA-1077) - Threads hanging in dispose() on SSLHandshakeException

## Improvement :

- [DIRMINA-1061](https://issues.apache.org/jira/browse/DIRMINA-1061) - When AbstractPollingIoProcessor read nothing, free the temporary buffer should be better

## Task :

- [DIRMINA-1058](https://issues.apache.org/jira/browse/DIRMINA-1058) - Add the missing Javadoc

We urge you to switch to this version if you were using MINA 2.0.16 or any older version.

## MINA 2.0.16 released *posted on October, 31, 2016*

The MINA project is pleased to announce a new bug fix release, MINA 2.0.16. It fixes a critical SSL issue, and a regression introduced in 2.0.14.

Here are the fixed issues :

## Bugs :

- [DIRMINA-1043](https://issues.apache.org/jira/browse/DIRMINA-1043) NullPointerException after upgrade to mina 2.0.14
- [DIRMINA-1044](https://issues.apache.org/jira/browse/DIRMINA-1044) Non-Secure (no TLS/SSL) based client could successfully send message to secure Mina endpoint after second attempt

We urge you to switch to this version if you were using MINA 2.0.15 or any older version.

## MINA 2.0.15 released *posted on September, 27, 2016*

The MINA project is pleased to announce a new bug fix release, MINA 2.0.15. It fixes a hang, a NPE and a few other minor issues.

Here are the fixed issues :

## Bugs :

- [DIRMINA-1041](https://issues.apache.org/jira/browse/DIRMINA-1041) WriteFuture.await() hangs when the connection is closed remotely before await is invoked
- [DIRMINA-1047](https://issues.apache.org/jira/browse/DIRMINA-1047) NullPointerException in AbstractIoSession.destroy()
- [DIRMINA-1049](https://issues.apache.org/jira/browse/DIRMINA-1049) Error in mina-statemachine manifest prevents using it in Apache Karaf

We urge you to switch to this version if you were using MINA 2.0.14 or any older version.

## MINA 2.0.14 released *posted on August, 30, 2016*

The MINA project is pleased to announce a new bug fix release, MINA 2.0.14. It fixes many issues, some of them being a real burden for SSHD DIRMINA-1021). Some patches were also applied (thanks to Maria Petridan).

Here are the fixed issues :

## Bugs :

- [DIRMINA-760](https://issues.apache.org/jira/browse/DIRMINA-760) Client fails to detect disconnection
- [DIRMINA-976](https://issues.apache.org/jira/browse/DIRMINA-976) ScheduledWriteBytes Increases after Exception on Writing
- [DIRMINA-1021](https://issues.apache.org/jira/browse/DIRMINA-1021) MINA-CORE does not remove sessions if exceptions occur while closing
- [DIRMINA-1025](https://issues.apache.org/jira/browse/DIRMINA-1025) A call to session.closed(true) may still flush messages.
- [DIRMINA-1028](https://issues.apache.org/jira/browse/DIRMINA-1028) The supported ciphers configuration might not be used
- [DIRMINA-1029](https://issues.apache.org/jira/browse/DIRMINA-1029) The sent buffer is reset to its original position when using the SSL Filter after a session.write()
- [DIRMINA-1037](https://issues.apache.org/jira/browse/DIRMINA-1037) Throw exception in NioProcessor.write if the session is closing
- [DIRMINA-1039](https://issues.apache.org/jira/browse/DIRMINA-1039) Response messages queue up on the server side waiting to be written to socket, while the server continues to read more request messages, causing out of heap memory
- [DIRMINA-1042](https://issues.apache.org/jira/browse/DIRMINA-1042) Epoll spinning causes memory leak

## Task :

- [DIRMINA-986](https://issues.apache.org/jira/browse/DIRMINA-986) Update the web site to reflect the switch to git for the release process

1027. SSLHandler writes corrupt messages under heavy load

A security issue has also been fixed in this version.

We urge you to switch to this version if you were using MINA 2.0.13.

## MINA 2.0.13 released *posted on February, 16, 2016*

Another release to fix a critical SSL bug ( a race condition which could lead to a deadlock in some corner cases).

We urge you to switch to this version if you were using MINA 2.0.12.

## Bugs :

- [DIRMINA-1019](https://issues.apache.org/jira/browse/DIRMINA-1019) SslHandler flushScheduledEvents race condition
- [DIRMINA-1027](https://issues.apache.org/jira/browse/DIRMINA-1027) SSLHandler writes corrupt messages under heavy load

## MINA 2.0.12 released *posted on February, 07, 2016*

This new release of MINA is a bug fix release. There are a few new bugs that were wound in the way we handle closure, leading to some infinite loop consuming 100% CPU, and a bad counter update forbidding the main loop to be exited.

We urge you to switch to this version if you were using MINA 2.0.11.

## Bugs :

- [DIRMINA-1001](https://issues.apache.org/jira/browse/DIRMINA-1001) mina2.0.9 session.close cpu100%
- [DIRMINA-1006](https://issues.apache.org/jira/browse/DIRMINA-1006) mina2.0.9 NioProcessor thread make cpu 100%
- [DIRMINA-1024](https://issues.apache.org/jira/browse/DIRMINA-1024) There is no way to start a SslHandshake when the autoStart flag is set to false
- [DIRMINA-1026](https://issues.apache.org/jira/browse/DIRMINA-1026) Session may be removed twice from the removedSession queue

## MINA 2.0.11 released *posted on January, 26, 2016*

This new release of MINA is a bug fix release. We have found a critical bug in the SSL Handler, that may cause a loop when dealing with big messages being transmitted over an SSL connection.

Otherwise, thee Javadoc has been cleaned.

We urge you to switch to this version if you were using MINA 2.0.10.

## Bugs :

- [DIRMINA-1023](https://issues.apache.org/jira/browse/DIRMINA-1023) - Infinite loop in SslHandler when the AppBuffer is too small
- [DIRMINA-1022](https://issues.apache.org/jira/browse/DIRMINA-0122) - The IoBuffer.fill(byte, int) method does not work when byte > 0x7F

## Improvements :

- [DIRMINA-985](https://issues.apache.org/jira/browse/DIRMINA-985) - Fix the various Javadoc issues

## MINA 2.0.10 released *posted on December, 16, 2015*

This new release of MINA is a bug fix release. Among important fixes, we have removed a bottleneck in the way we were using Codecs, removed a deadlock in SSL when using the proxy, a race condition, and a few other things :

## Bugs :

- [DIRMINA-992](https://issues.apache.org/jira/browse/DIRMINA-992) - NioSocketConnector.newHandle throws the wrong exception
- [DIRMINA-994](https://issues.apache.org/jira/browse/DIRMINA-994) - The ConnectionRequest.cancel() method is inconsistent wrt concurrent access
- [DIRMINA-995](https://issues.apache.org/jira/browse/DIRMINA-995) - Deadlock when using SSL and proxy
- [DIRMINA-1013](https://issues.apache.org/jira/browse/DIRMINA-1013) - Threading model is suppressed by ProtocolCodecFilter
- [DIRMINA-1016](https://issues.apache.org/jira/browse/DIRMINA-1016) - Regression with 2.0.9: Missing javax.net.ssl import in manifest
- [DIRMINA-1017](https://issues.apache.org/jira/browse/DIRMINA-1017) - SSLEngine BUFFER\_OVERFLOW (unwrap)
- [DIRMINA-1019](https://issues.apache.org/jira/browse/DIRMINA-1019) - SslHandler flushScheduledEvents race condition

## Improvements :

- [DIRMINA-1018](https://issues.apache.org/jira/browse/DIRMINA-1018) - fetchAppBuffer shrink
- [DIRMINA-1020](https://issues.apache.org/jira/browse/DIRMINA-1020) - Change minimum version for slf4j in MANIFEST

## MINA 2.0.9 released *posted on October, 25, 2014*

This new release of MINA is just a bug fix release. A few issues have been fixed, one critical, inducing a 100% CPU, and one was annoying as it was generating stack traces for nothing.

You can check the list of fixes for this version there :

[Release note](https://issues.apache.org/jira/issues/?jql=project%20%3D%20DIRMINA%20AND%20fixVersion%20%3D%202.0.9%20AND%20status%20%3D%20Resolved%20ORDER%20BY%20priority%20DESC)

- [DIRMINA-921](https://issues.apache.org/jira/browse/DIRMINA-921) - Maven build fails if test phase is given
- [DIRMINA-988](https://issues.apache.org/jira/browse/DIRMINA-988) - 100% CPU when using IoBuffer.shrink() method in some cases
- [DIRMINA-989](https://issues.apache.org/jira/browse/DIRMINA-989) - Frequent CancelledKeyException
- [DIRMINA-990](https://issues.apache.org/jira/browse/DIRMINA-990) - Control flow over exceptional path in AbstractIoBuffer
- [DIRMINA-991](https://issues.apache.org/jira/browse/DIRMINA-991) - Possible faster deserialization in AbstractIoBuffer object deserialization.

## MINA 2.0.8 released *posted on September, 22, 2014*

It’s 2 years we haven’t had a release of MINA 2.0, it’s about time.

We have tried to fix as much issues as we could in the last 3 weeks. As a result, we have closed around 90 JIRAs (fixed, postponed or simply discarded).

There is one change that might break the build for those switching from MINA 2.0.7 to MINA 2.0.8 : the *IoHandler* interface now has a method called *inputClosed()*, so either you have to implement this method if you are directly implementing the *IoHandler* interface, or better, you can extends *IoHandlerAdapter*, which implements a placeholder for this method.

You can check the list of fixes for this version there :

[Release note](https://issues.apache.org/jira/issues/?jql=project%20%3D%20DIRMINA%20AND%20fixVersion%20%3D%202.0.8%20AND%20status%20%3D%20Resolved%20ORDER%20BY%20priority%20DESC)

### Bug

- [DIRMINA-539](https://issues.apache.org/jira/browse/DIRMINA-539) - NioDatagramConnector doesn’t takes the TrafficClass value set to his DatagramSessionConfig
- [DIRMINA-574](https://issues.apache.org/jira/browse/DIRMINA-574) - ClassCastException when a message is written on a closed session.
- [DIRMINA-604](https://issues.apache.org/jira/browse/DIRMINA-604) - Deadlock occurs when implementing two mina StateMachine
- [DIRMINA-639](https://issues.apache.org/jira/browse/DIRMINA-639) - WriteFuture are updated long after a session.write() is done
- [DIRMINA-738](https://issues.apache.org/jira/browse/DIRMINA-738) - Using IoEventQueueThrottler with a WriteRequestFilter can lead to hangs
- [DIRMINA-760](https://issues.apache.org/jira/browse/DIRMINA-760) - Client fails to detect disconnection
- [DIRMINA-764](https://issues.apache.org/jira/browse/DIRMINA-764) - DDOS possible in only a few seconds…
- [DIRMINA-777](https://issues.apache.org/jira/browse/DIRMINA-777) - IoSessionConfig.setUseReadOperation(true) doesn’t seem to work
- [DIRMINA-779](https://issues.apache.org/jira/browse/DIRMINA-779) - SSLHandler can re-order data that it reads
- [DIRMINA-782](https://issues.apache.org/jira/browse/DIRMINA-782) - Combination of SslFilter & FileRegionWriteFilter causes messageSent events to be lost
- [DIRMINA-785](https://issues.apache.org/jira/browse/DIRMINA-785) - Half-duplex close of TCP channel
- [DIRMINA-789](https://issues.apache.org/jira/browse/DIRMINA-789) - Possible Deadlock/Out of memory when sending large amounts of data using Nio
- [DIRMINA-792](https://issues.apache.org/jira/browse/DIRMINA-792) - await() forever
- [DIRMINA-804](https://issues.apache.org/jira/browse/DIRMINA-804) - NioDatagramAcceptor.unbind does not unbind cleanly
- [DIRMINA-805](https://issues.apache.org/jira/browse/DIRMINA-805) - No cipher suites and protocols in SslFilter
- [DIRMINA-813](https://issues.apache.org/jira/browse/DIRMINA-813) - Starvation occurs sometimes in SerialSession#close()
- [DIRMINA-818](https://issues.apache.org/jira/browse/DIRMINA-818) - Loosing connects on NioSocketConnector
- [DIRMINA-833](https://issues.apache.org/jira/browse/DIRMINA-833) - LoggingFilter does not log SENT bytes when used with a ProtocolCodecFilter
- [DIRMINA-843](https://issues.apache.org/jira/browse/DIRMINA-843) - NioSocketAcceptor does not provide an interface to input connectiontimeout parameter.
- [DIRMINA-844](https://issues.apache.org/jira/browse/DIRMINA-844) - Http Proxy Authentication failed to complete (see description for exact point of failure)
- [DIRMINA-845](https://issues.apache.org/jira/browse/DIRMINA-845) - ProtocolEncoderOutputImpl isn’t thread-safe
- [DIRMINA-891](https://issues.apache.org/jira/browse/DIRMINA-891) - SSLHandler throws SSLException during handshake that sequence number triggers
- [DIRMINA-899](https://issues.apache.org/jira/browse/DIRMINA-899) - IoSession.getAttribute() doesn’t store default value
- [DIRMINA-902](https://issues.apache.org/jira/browse/DIRMINA-902) - Buffer read incorrectly when reading after a NEED\_DATA trigger.
- [DIRMINA-905](https://issues.apache.org/jira/browse/DIRMINA-905) - mina serial close
- [DIRMINA-911](https://issues.apache.org/jira/browse/DIRMINA-911) - Surprising behaviour with ConnectFuture
- [DIRMINA-912](https://issues.apache.org/jira/browse/DIRMINA-912) - Different instances of OrderedThreadPoolExecutor may use same task queue
- [DIRMINA-920](https://issues.apache.org/jira/browse/DIRMINA-920) - HTTP server decoding is broken
- [DIRMINA-926](https://issues.apache.org/jira/browse/DIRMINA-926) - IoSession IP Error when Socket Server Communicate With Microcomputer In LAN and Internet.
- [DIRMINA-928](https://issues.apache.org/jira/browse/DIRMINA-928) - when client want to connect to server by binding wrong ip address,there is a bug.
- [DIRMINA-931](https://issues.apache.org/jira/browse/DIRMINA-931) - HTTP header decoding is broken
- [DIRMINA-932](https://issues.apache.org/jira/browse/DIRMINA-932) - HTTP Request decoding is broken if request headers are received in several messages
- [DIRMINA-933](https://issues.apache.org/jira/browse/DIRMINA-933) - subtle HttpServerDecoder problems
- [DIRMINA-937](https://issues.apache.org/jira/browse/DIRMINA-937) - sslfilter hangs with openjdk works with oracle?
- [DIRMINA-940](https://issues.apache.org/jira/browse/DIRMINA-940) - HTTP Client decoder does not support responses without Content-Length header
- [DIRMINA-942](https://issues.apache.org/jira/browse/DIRMINA-942) - Infinite loop flushing to broken pipe
- [DIRMINA-948](https://issues.apache.org/jira/browse/DIRMINA-948) - Performance recession when invoke session.write concurrent
- [DIRMINA-956](https://issues.apache.org/jira/browse/DIRMINA-956) - Status code match bug in AbstractHttpLogicHandler
- [DIRMINA-957](https://issues.apache.org/jira/browse/DIRMINA-957) - MINA build in BlacklistFilter does not support IPV6 address
- [DIRMINA-962](https://issues.apache.org/jira/browse/DIRMINA-962) - Immediate session close with a SSL filter
- [DIRMINA-963](https://issues.apache.org/jira/browse/DIRMINA-963) - Socks5 and ProxyConnector don’t work with InetSocketAddress.createUnresolved
- [DIRMINA-965](https://issues.apache.org/jira/browse/DIRMINA-965) - HttpServerDecoder is broken in certain condition
- [DIRMINA-966](https://issues.apache.org/jira/browse/DIRMINA-966) - NIO Datagram messages can get duplicated when unable to be sent by the underlying DatagramChannel
- [DIRMINA-967](https://issues.apache.org/jira/browse/DIRMINA-967) - IoSession updateThroughput not automatically called
- [DIRMINA-968](https://issues.apache.org/jira/browse/DIRMINA-968) - Memory leak in SSL Handshake errors
- [DIRMINA-970](https://issues.apache.org/jira/browse/DIRMINA-970) - ProtocolEncoderOutputImpl.flush() occur a IllegalArgumentException
- [DIRMINA-972](https://issues.apache.org/jira/browse/DIRMINA-972) - NPE during handshake on Android using SSLFilter
- [DIRMINA-973](https://issues.apache.org/jira/browse/DIRMINA-973) - IllegalArgumentException thrown on ProtocolCodecFilter.flush
- [DIRMINA-976](https://issues.apache.org/jira/browse/DIRMINA-976) - ScheduledWriteBytes Increases after Exception on Writing
- [DIRMINA-977](https://issues.apache.org/jira/browse/DIRMINA-977) - DefaultIoFilterChain.replace does not call register/deregister
- [DIRMINA-978](https://issues.apache.org/jira/browse/DIRMINA-978) - ClosedSelectorException handling in AbstractPollingIoProcessor
- [DIRMINA-980](https://issues.apache.org/jira/browse/DIRMINA-980) - Missing implementation of write() method in SerialSessionImpl.SerialIoProcessor
- [DIRMINA-981](https://issues.apache.org/jira/browse/DIRMINA-981) - IoBuffer GetSlice throw an IllegalArgumentException
- [DIRMINA-982](https://issues.apache.org/jira/browse/DIRMINA-982) - ProtocolEncoderOutputImpl.flush() throws an IllegalArgumentException if buffers queue is empty
- [DIRMINA-983](https://issues.apache.org/jira/browse/DIRMINA-983) - Problems with TextLineDecoder and special characters

### Improvement

- [DIRMINA-210](https://issues.apache.org/jira/browse/DIRMINA-210) - Investigate removal of static methods in ByteBuffer
- [DIRMINA-237](https://issues.apache.org/jira/browse/DIRMINA-237) - Improve Spring integration
- [DIRMINA-572](https://issues.apache.org/jira/browse/DIRMINA-572) - Add Spring support for Mina statemachine
- [DIRMINA-586](https://issues.apache.org/jira/browse/DIRMINA-586) - Dynamic delimiter support for TextLineCodecFactory
- [DIRMINA-593](https://issues.apache.org/jira/browse/DIRMINA-593) - Javadoc & documentation for org/apache/mina/filter/reqres
- [DIRMINA-629](https://issues.apache.org/jira/browse/DIRMINA-629) - The IoServiceStatistics methods are called for every new session creation
- [DIRMINA-631](https://issues.apache.org/jira/browse/DIRMINA-631) - AbstractIoFilter: increment written- and receivedMessages statistics on application end of filter chain
- [DIRMINA-668](https://issues.apache.org/jira/browse/DIRMINA-668) - Modify the way we use IoProcessors
- [DIRMINA-682](https://issues.apache.org/jira/browse/DIRMINA-682) - We need a better documentation for the ExecutorFilter [was :Writing more than one message will block until the MessageReceived as been fully proceced]
- [DIRMINA-723](https://issues.apache.org/jira/browse/DIRMINA-723) - OrderedThreadPoolExecutor behavior: configurable queue size, corePoolSize, maximumPoolSize
- [DIRMINA-752](https://issues.apache.org/jira/browse/DIRMINA-752) - maybe move SerialAddressEditor.class to the mina beans project
- [DIRMINA-761](https://issues.apache.org/jira/browse/DIRMINA-761) - how to shutdown a mina application
- [DIRMINA-766](https://issues.apache.org/jira/browse/DIRMINA-766) - Read does not exploit buffer optimally
- [DIRMINA-767](https://issues.apache.org/jira/browse/DIRMINA-767) - Move encoder/decoder out of the session Attributes
- [DIRMINA-773](https://issues.apache.org/jira/browse/DIRMINA-773) - org.apache.mina.filter.firewall.Subnet should consider 0.0.0.0/0 as a subnet that contains ‘all the ipv4 addresses’
- [DIRMINA-780](https://issues.apache.org/jira/browse/DIRMINA-780) - Writing null objects to the Session should raise an Exception
- [DIRMINA-825](https://issues.apache.org/jira/browse/DIRMINA-825) - Add host and port info to BindException thrown by NioSocketAcceptor#open
- [DIRMINA-838](https://issues.apache.org/jira/browse/DIRMINA-838) - Redundant AttributeKey allocation resulting in high garbage collector activity
- [DIRMINA-913](https://issues.apache.org/jira/browse/DIRMINA-913) - Add a method IoSession.isSecured() to tell the user if the SSL filter has been started or not
- [DIRMINA-921](https://issues.apache.org/jira/browse/DIRMINA-921) - Maven build fails if test phase is given
- [DIRMINA-929](https://issues.apache.org/jira/browse/DIRMINA-929) - AbstractPollingIoProcessor patch to mark buffer as free
- [DIRMINA-934](https://issues.apache.org/jira/browse/DIRMINA-934) - Replace synchronized with a Semaphore for better performance
- [DIRMINA-941](https://issues.apache.org/jira/browse/DIRMINA-941) - DefaultIoFilterChain (or any other class) should not catch Throwable without re-throwing
- [DIRMINA-945](https://issues.apache.org/jira/browse/DIRMINA-945) - DefaultVmPipeSessionConfig is empty

### New Feature

- [DIRMINA-23](https://issues.apache.org/jira/browse/DIRMINA-23) - New transport type: non-NIO sockets
- [DIRMINA-68](https://issues.apache.org/jira/browse/DIRMINA-68) - Automatic reconnect configuration for client channels.
- [DIRMINA-389](https://issues.apache.org/jira/browse/DIRMINA-389) - Create a Connection Throttle Filter
- [DIRMINA-453](https://issues.apache.org/jira/browse/DIRMINA-453) - Multiple IoServices for one java.nio.Selector
- [DIRMINA-485](https://issues.apache.org/jira/browse/DIRMINA-485) - SCTP Transport based on APR (Apache Portable Runtime)
- [DIRMINA-489](https://issues.apache.org/jira/browse/DIRMINA-489) - Composite IoBuffer
- [DIRMINA-507](https://issues.apache.org/jira/browse/DIRMINA-507) - IoBuffer: Support prepending data
- [DIRMINA-554](https://issues.apache.org/jira/browse/DIRMINA-554) - A hook between bind() and accept()
- [DIRMINA-655](https://issues.apache.org/jira/browse/DIRMINA-655) - Add a more general purpose text based decoder
- [DIRMINA-816](https://issues.apache.org/jira/browse/DIRMINA-816) - NioSocketConnector missing defaultLocalAddress
- [DIRMINA-964](https://issues.apache.org/jira/browse/DIRMINA-964) - Custom NIO SelectorProvider for NioSocketAcceptor

### Task

- [DIRMINA-56](https://issues.apache.org/jira/browse/DIRMINA-56) - Create a Benchmark Suite That Generates HTML Reports.
- [DIRMINA-188](https://issues.apache.org/jira/browse/DIRMINA-188) - All-in-one JAR
- [DIRMINA-477](https://issues.apache.org/jira/browse/DIRMINA-477) - Update page about differences between 1.x and 2.x
- [DIRMINA-721](https://issues.apache.org/jira/browse/DIRMINA-721) - Get rid of multiton iohandler and netty2 codec as proposed on ML

### Test

- [DIRMINA-922](https://issues.apache.org/jira/browse/DIRMINA-922) - Add a benchmark project to compare with other IO frameworks

### Wish

- [DIRMINA-250](https://issues.apache.org/jira/browse/DIRMINA-250) - Provide a test suite for a transport implementor.
- [DIRMINA-916](https://issues.apache.org/jira/browse/DIRMINA-916) - Adding Http Status code 101 “101 Switching Protocols” in org.apache.mina.http.api.HttpStatus

## MINA 2.0.7 released *posted on October, 12, 2012*

The Apache MINA project is pleased to announce MINA 2.0.7 ! This version is a bug fix release.

It fixes a regression introduced in MINA 2.0.5, and some performance improvements for the UDP server.

We recommend all users to upgrade to this release. We consider this a stable and production ready release.

[Release note1](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=10670&version=12323341)
[Release note2](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=10670&version=12316652)

## MINA 2.0.5 released *posted on August, 26, 2012*

The Apache MINA project is pleased to announce MINA 2.0.5 ! This version is a bug fix release.

We recommend all users to upgrade to this release. We consider this a stable and production ready release.

[Release note](http://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=10670&version=12316474)

## MINA 2.0.4 released *posted on August, 26, 2012*

The Apache MINA project is pleased to announce MINA 2.0.4 ! This version is a bug fix release.

We recommend all users to upgrade to this release. We consider this a stable and production ready release.

[Release note](https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=10670&version=12316009)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-issue-tracking"></a>

# Issue Tracking — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-issue-tracking--Navigation-Upcoming"></a>Upcoming

- No event

Our project uses [JIRA](http://www.atlassian.com/software/jira), a Java EE based issue tracking and project management application.

# General guidance

First, this is the best place to submit bugs (or what you think is a bug). The mailing list is a short term memory place, don’t expect your problem to be answered if it’s not in the next couple of days you posted on it.

What are the important information you need to put when filing a JIRA ?

- The version you are using. This is mandatory. We are dealing with many versions, and we don’t have time to check what version you are using by looking to your code.
- The trace you got (attach them to the issue you have opened). Thread dumps, logs, whatever you can provide is just better than nothing
- Some code that expose the problem, if possible. Not thousands of lines, just the bare minimum.
- Some clear explanation on what’s going on and what is expected instead, in English of course. It does not have to be perfect english - most of the committers aren’t English native’s speaker - but at least something we can understand.
- A short description in the title.
- Last, not least, if you are kind enough to propose a patch, attach it and **do not forget to check the box which grant The ASF the right to apply it in the code base.**

Remember that this issue tracking system is public. Would you provide some confidential information, it will be visible to the world. Keep that in mind before posting !!!

# Filling an issue

Issues, bugs, and feature requests should be submitted to the following issue tracking system :

|  |  |  |
| --- | --- | --- |
| Project | JIRA key | Corresponding link to issue tracking system |
| MINA | DIRMINA | http://issues.apache.org/jira/browse/DIRMINA |

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-mailing-lists"></a>

# Apache MINA Mailing Lists — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-mailing-lists--Navigation-Upcoming"></a>Upcoming

- No event

The Apache MINA team interacts with MINA developers and users via [mailing lists](http://en.wikipedia.org/wiki/Mailing_list). If you have any questions or something to say to us, please subscribe to our user mailing list and post a message. If you would like to contribute to the development of MINA, please subscribe to the developer mailing list.

**Do NOT cross post (ie mail to users and dev). Pick the right list**

Take a few minutes to read this page :[Asking Smart Questions](http://www.catb.org/~esr/faqs/smart-questions.html)

More specifically, follow those simple rules :

- Do not cross post (ie mail to users and dev). Pick the right list.
- No need to send a mail every hour if you don’t get an answer. We don’t have an answer to every question …
- Use meaningful subject headers. HELP PLEASE HELP ME is just plain useless…
- Be precise about your problem.
- Always give some information about the version you are using, the OS, the Java version.
- You may have find a bug in MINA, but it’s way more likely that your code is buggy.
- Don’t be afraid to post, even if you think your english suck. Most of us are not english native speakers, anyways …

Thanks for listening !

# Users mailing list

This list is for any questions related to MINA. It’s where you need to post for requesting support, or asking question about API usage.

## Subscribing

Send a message to [users-subscribe@mina.apache.org](mailto:users-subscribe@mina.apache.org). After sending your initial email, you will be sent a confirmation email. Simply reply to the confirmation email and you will be subscribed.

## Posting a message

You can ask any questions or provide feedback by sending an email to [users@mina.apache.org](mailto:users@mina.apache.org) after subscribing to the mailing list. Your message can be sent even if you didn’t subscribe to the mailing list, but it will take some time for your message to be in our mail box because of the moderation process.

## Unsubscribing

Oh, did you lose your interest in MINA? Please let us know what made so if MINA couldn’t solve your problem and give it a chance!

Send a message to [users-unsubscribe@mina.apache.org](mailto:users-unsubscribe@mina.apache.org), you’ll receive a confirmation and you will just have to reply to this confirmation.

# Developers mailing list

This mailing list is used for discussions about the actual development of MINA and sub-projects (FtpServer, AsyncWeb).

## Subscribing

Send a message to [dev-subscribe@mina.apache.org](mailto:dev-subscribe@mina.apache.org). After sending your initial email, you will be sent a confirmation email. Simply reply to the confirmation email and you will be subscribed.

## Posting a message

You can ask any questions or feedback to [dev@mina.apache.org](mailto:dev@mina.apache.org) after subscribing to the mailing list. Your message can be sent even if you didn’t subscribe to the mailing list, but it will take for some time for your message to be in our mail box because of moderation process.

Alternatively, you can use the web forum interface.

## Unsubscribing from the mailing list

Send a message to [dev-unsubscribe@mina.apache.org](mailto:dev-unsubscribe@mina.apache.org), you’ll receive a confirmation and you will just have to reply to this confirmation.

# Commits mailing list

This mailing list is tracking all the code modifications realised in MINA subversion repository. It’s an useful tool for knowing what is going on MINA development and giving feedback on last modifications using the development mailing list

## Subscribing

Send a message to [commits-subscribe@mina.apache.org](mailto:commits-subscribe@mina.apache.org). After sending your initial email, you will be sent a confirmation email. Simply reply to the confirmation email and you will be subscribed.

## Unsubscribing from the mailing list

Send a message to [commits-unsubscribe@mina.apache.org](mailto:commits-unsubscribe@mina.apache.org), you’ll receive a confirmation and you will just have to reply to this confirmation.

# Archive

All previous messages are archived in the following site:

- [MarkMail.org](http://mina.markmail.org/)
- Mail-Archive.com - [dev](http://www.mail-archive.com/dev@mina.apache.org/), [users](http://www.mail-archive.com/users@mina.apache.org/), [commits](http://www.mail-archive.com/commits@mina.apache.org/)
- Apache.org - [dev](http://mail-archives.apache.org/mod_mbox/mina-dev/), [users](http://mail-archives.apache.org/mod_mbox/mina-users/), [commits](http://mail-archives.apache.org/mod_mbox/mina-commits/)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-performances"></a>

# Performance Test Reports — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-performances--Navigation-Upcoming"></a>Upcoming

- No event

# Before You Read the Performance Test Reports…

This page exhibits the performance test results under various conditions (e.g. various protocols and system environments). Please [contact us|Mailing Lists] if you have any specific performance test results to publish for your MINA-based application.

The following performance test results may have critical flaws in test design or contain wrong values. Please regard these reports as just a hint for understanding general performance characteristics of Apache MINA. Additionally, these reports are not meant to claim that Apache MINA outperforms a certain product purposely.

Also note that this benchmark has been created 14 years ago. This page remains present solely for historical reasons…

## Apache MINA 2.0.0-M1-SNAPSHOT + AsyncWeb 0.9.0-SNAPSHOT

[Trustin Lee](https://web.archive.org/web/20121221090054/http://gleamynode.net/) ran a HTTP performance test with the latest snapshot of Apache MINA and [AsyncWeb](https://svn.apache.org/repos/asf/mina/asyncweb/trunk/) combo, using [the AsyncWeb lightweight HTTP server example](https://svn.apache.org/repos/asf/mina/asyncweb/trunk/examples/src/main/java/org/apache/asyncweb/examples/lightweight/).

- Protocol
  - HTTP
  - Tested keep-alive mode using [ApacheBench](http://en.wikipedia.org/wiki/ApacheBench).
  - Content length: 128 (excluding the header)
- Client
  - Pentium 4 3GHz
  - Ubuntu Linux 6.10
- Server
  - 2 dual-core Opterons (4 cores, 270 Italy)
  - Gentoo Linux 2.6.18-r6 x86\_64
- Network
  - 100Mbit Ethernet (direct link)
- JVM
  - Sun Java HotSpot(TM) 64-Bit Server VM (build 1.6.0-b105, mixed mode)
  - {{-server -Xms512m -Xmx512m -Xss128k -XX:+AggressiveOpts -XX:+UseParallelGC -XX:+UseBiasedLocking -XX:NewSize=64m}}

To show the performance characteristics of Apache MINA doesn’t differ with the production-ready Web servers, the same test has been run on [the Apache HTTPD 2.0.58|http://httpd.apache.org/]. Because I don’t know how to write an Apache HTTPD module, I simply used a dummy static file. Because the amount of the response header two HTTP servers generate is different, I changed the AsyncWeb to generate more traffic in the content. The size of one response was about 405 bytes.

![Asyncweb performances](mina.apache.org/assets/img/AsyncWeb-0.9.0-SNAPSHOT.png)

The client machine in my company doesn’t have 1Gbps Ethernet adapter nor a gigabit-capable CPU, I was not able to increase the content size. I made sure the network didn’t saturate while the test at least.

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-quick-start-guide"></a>

# Quick Start Guide — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-quick-start-guide--Navigation-Upcoming"></a>Upcoming

- No event

# Quick Start Guide

This tutorial will walk you through the process of building a MINA based program. This tutorial will walk through building a time server. The following prerequisites are required for this tutorial:

- MINA 2.0.7 Core
- JDK 1.5 or greater
- [SLF4J|http://www.slf4j.org/] 1.3.0 or greater
  - **Log4J 1.2** users: *slf4j-api.jar*, *slf4j-log4j12.jar*, and [Log4J](http://logging.apache.org/log4j/1.2/) 1.2.x
  - **Log4J 1.3** users: *slf4j-api.jar*, *slf4j-log4j13.jar*, and [Log4J](http://logging.apache.org/log4j/1.2/) 1.3.x
  - **java.util.logging** users: *slf4j-api.jar* and *slf4j-jdk14.jar*
  - **IMPORTANT** : Please make sure you are using the right *slf4j-\*.jar* that matches to your logging framework.
    For instance, *slf4j-log4j12.jar* and *log4j-1.3.x.jar* can not be used together, and will malfunction.

I have tested this program on both WindowsÂ© 2000 professional and linux. If you have any problems getting this program to work, please do not hesitate to [contact us|Contact] in order to talk to the MINA developers. Also, this tutorial has tried to remain independent of development environments (IDE, editors..etc). This tutorial will work with any environment that you are comfortable with. Compilation commands and steps to execute the program have been removed for brevity. If you need help learning how to either compile of execute java programs, please consult the [Java tutorial](http://java.sun.com/docs/books/tutorial/).

## Writing the MINA time server

We will begin by creating a file called MinaTimeServer.java. The initial code can be found below:

```java
public class MinaTimeServer {

    public static void main(String[] args) {
        // code will go here next
    }
}
```

This code should be straightforward to all. We are simply defining a main method that will be used to kick off the program. At this point, we will begin to add the code that will make up our server. First off, we need an object that will be used to listen for incoming connections. Since this program will be TCP/IP based, we will add a *SocketAcceptor* to our program.

```java
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class MinaTimeServer
{
    public static void main( String[] args )
    {
        IoAcceptor acceptor = new NioSocketAcceptor();
    }
}
```

With the *NioSocketAcceptor* class in place, we can go ahead and define the handler class and bind the NioSocketAcceptor to a port.

Next we add a filter to the configuration. This filter will log all information such as newly created sessions, messages received, messages sent, session closed. The next filter is a *ProtocolCodecFilter*. This filter will translate binary or protocol specific data into message object and vice versa. We use an existing TextLine factory because it will handle text base message for you (you don’t have to write the codec part)

```java
import java.nio.charset.Charset;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class MinaTimeServer
{
    public static void main( String[] args )
    {
        IoAcceptor acceptor = new NioSocketAcceptor();

        acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new TextLineCodecFactory( Charset.forName( "UTF-8" ))));
    }
}
```

At this point, we will define the handler that will be used to service client connections and the requests for the current time. The handler class is a class that must implement the interface IoHandler. For almost all programs that use MINA, this becomes the workhorse of the program, as it services all incoming requests from the clients. For this tutorial, we will extend the class IoHandlerAdapter. This is a class that follows the adapter design pattern which simplifies the amount of code that needs to be written in order to satisfy the requirement of passing in a class that implements the IoHandler interface.

```java
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class MinaTimeServer
{
    public static void main( String[] args ) throws IOException
    {
        IoAcceptor acceptor = new NioSocketAcceptor();

        acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new TextLineCodecFactory( Charset.forName( "UTF-8" ))));

        acceptor.setHandler(  new TimeServerHandler() );
    }
}
```

We will now add in the NioSocketAcceptor configuration. This will allow us to make socket-specific settings for the socket that will be used to accept connections from clients.

```java
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class MinaTimeServer
{
    public static void main( String[] args ) throws IOException
    {
        IoAcceptor acceptor = new NioSocketAcceptor();

        acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new TextLineCodecFactory( Charset.forName( "UTF-8" ))));

        acceptor.setHandler(  new TimeServerHandler() );

        acceptor.getSessionConfig().setReadBufferSize( 2048 );
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );
    }
}
```

There are 2 new lines in the MinaTimeServer class. These methods set the set the IoHandler, input buffer size and the idle property for the sessions. The buffer size will be specified in order to tell the underlying operating system how much room to allocate for incoming data. The second line will specify when to check for idle sessions. In the call to setIdleTime, the first parameter defines what actions to check for when determining if a session is idle, the second parameter defines the length of time in seconds that must occur before a session is deemed to be idle.

The code for the handler is shown below:

```java
import java.util.Date;

import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;

public class TimeServerHandler extends IoHandlerAdapter
{
    @Override
    public void exceptionCaught( IoSession session, Throwable cause ) throws Exception
    {
        cause.printStackTrace();
    }

    @Override
    public void messageReceived( IoSession session, Object message ) throws Exception
    {
        String str = message.toString();
        if( str.trim().equalsIgnoreCase("quit") ) {
            session.close();
            return;
        }

        Date date = new Date();
        session.write( date.toString() );
        System.out.println("Message written...");
    }

    @Override
    public void sessionIdle( IoSession session, IdleStatus status ) throws Exception
    {
        System.out.println( "IDLE " + session.getIdleCount( status ));
    }
}
```

The methods used in this class are *exceptionCaught*, *messageReceived* and *sessionIdle*. *exceptionCaught* should always be defined in a handler to process and exceptions that are raised in the normal course of handling remote connections. If this method is not defined, exceptions may not get properly reported.

The *exceptionCaught* method will simply print the stack trace of the error and close the session. For most programs, this will be standard practice unless the handler can recover from the exception condition.

The *messageReceived* method will receive the data from the client and write back to the client the current time. If the message received from the client is the word “quit”, then the session will be closed. This method will also print out the current time to the client. Depending on the protocol codec that you use, the object (second parameter) that gets passed in to this method will be different, as well as the object that you pass in to the session.write(Object) method. If you do not specify a protocol codec, you will most likely receive a IoBuffer object, and be required to write out a IoBuffer object.

The *sessionIdle* method will be called once a session has remained idle for the amount of time specified in the call *acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH\_IDLE, 10 );*.

All that is left to do is define the socket address that the server will listen on, and actually make the call that will start the server. That code is shown below:

```java
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class MinaTimeServer
{
    private static final int PORT = 9123;

    public static void main( String[] args ) throws IOException
    {
        IoAcceptor acceptor = new NioSocketAcceptor();

        acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new TextLineCodecFactory( Charset.forName( "UTF-8" ))));

        acceptor.setHandler( new TimeServerHandler() );
    acceptor.getSessionConfig().setReadBufferSize( 2048 );
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );
        acceptor.bind( new InetSocketAddress(PORT) );
    }
}
```

## Try out the Time server

At this point, we can go ahead and compile the program. Once you have compiled the program you can run the program in order to test out what happens. The easiest way to test the program is to start the program, and then telnet in to the program:

| Client Output | Server Output |
| --- | --- |
| user@myhost:~> telnet 127.0.0.1 9123Trying 127.0.0.1…Connected to 127.0.0.1.Escape character is ‘^]'.helloMon Apr 09 23:42:55 EDT 2007quitConnection closed by foreign host.user@myhost:~> | MINA Time server started.Session created…Message written… |

## What’s Next?

Please visit our [User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc) page to find out more resources.

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-related-projects"></a>

# Related Projects — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-related-projects--Navigation-Upcoming"></a>Upcoming

- No event

# Related Projects

This page lists the projects which use Apache MINA as its networking layer. Please contact us if you are using MINA and you want to add a link to this page. You could also read the users’ testimonials. To compare MINA to other network application frameworks, please refer to the ‘Other network application frameworks’ section below.

- [Other network application frameworks](#mina-apache-org-mina-project-related-projects--other-network-application-frameworks)
  - [Grizzly](#mina-apache-org-mina-project-related-projects--grizzlyhttpsgrizzlyjavanet)
  - [Netty 3](#mina-apache-org-mina-project-related-projects--netty-3httpwwwjbossorgnetty)
  - [NIO Framework](#mina-apache-org-mina-project-related-projects--nio-frameworkhttpnioframeworksourceforgenet)
  - [QuickServer](#mina-apache-org-mina-project-related-projects--quickserverhttpwwwquickserverorg)
  - [xSocket](#mina-apache-org-mina-project-related-projects--xsockethttpxsocketsourceforgenet)
- [Messaging](#mina-apache-org-mina-project-related-projects--messaging)
  - [Apache Camel](#mina-apache-org-mina-project-related-projects--apache-camelhttpactivemqapacheorgcamel)
  - [Apache Qpid (incubating)](#mina-apache-org-mina-project-related-projects--apache-qpid-incubatinghttpcwikiapacheorgqpid)
  - [Avis](#mina-apache-org-mina-project-related-projects--avishttpavissourceforgenet)
  - [MailsterSMTP](#mina-apache-org-mina-project-related-projects--mailstersmtphttptedorgfreefrenprojectsphpsectionsmtp)
- [Instant Messaging](#mina-apache-org-mina-project-related-projects--instant-messaging)
  - [Jive Software Openfire](#mina-apache-org-mina-project-related-projects--jive-software-openfirehttpwwwjivesoftwarecomproductsopenfire)
- [Media Storage & Streaming](#mina-apache-org-mina-project-related-projects--media-storage--streaming)
  - [OpenLSD](#mina-apache-org-mina-project-related-projects--openlsdhttpopenlsdfreefrenopenlsdhtml)
  - [Red5](#mina-apache-org-mina-project-related-projects--red5httpwwwosflashorgred5)
- [Miscellaneous](#mina-apache-org-mina-project-related-projects--miscellaneous)
  - [Apache Directory Project](#mina-apache-org-mina-project-related-projects--apache-directory-projecthttpdirectoryapacheorg)
  - [Beep4j](#mina-apache-org-mina-project-related-projects--beep4jhttpbeep4jsourceforgenet)
  - [VFS FTPServer Bridge](#mina-apache-org-mina-project-related-projects--vfs-ftpserver-bridgehttpvfs-utilssourceforgenetftpserverindexhtml)
  - [HDFS over FTP](#mina-apache-org-mina-project-related-projects--a-namehdfsoverftp-hdfs-over-ftphttpssitesgooglecomaiponwebnethadoophomehdfs-over-ftp)

## Other network application frameworks

The projects referred in this section 'Other network application frameworks' are **not** based on MINA but are their own network application frameworks. These links are provided for users who want to compare MINA to other network application frameworks.

### [Grizzly](https://grizzly.java.net/)

Grizzly framework has been designed to help developers to take advantage of the Javaâ„¢ NIO API. Originally developed under the GlassFish umbrella, the framework is now available as a standalone project. Grizzly goals is to help developers to build scalable and robust servers using NIO.

### [Netty 3](http://www.jboss.org/netty/)

The new version of Netty Trustin Lee is working on now. The Netty project is an effort to provide an asynchronous event-driven network application framework and tools for rapid development of maintainable high performance and high scalability protocol servers and clients.

### [NIO Framework](http://nioframework.sourceforge.net/)

The NIO Framework is a library on top of NIO that hides most of the complexity of plain NIO. With the NIO Framework you can implement high-performance Java network applications without having to deal with all the nasty details of NIO. The issues above are resolved while the performance is preserved.

### [QuickServer](http://www.quickserver.org/)

QuickServer is an open source Java library/framework for quick creation of robust multi-client TCP server applications. QuickServer provides an abstraction over the ServerSocket, Socket and other network and input output classes and it eases the creation of powerful network servers.

### [xSocket](http://xsocket.sourceforge.net/)

xSocket is a easy to use NIO-based library to build high performance, highly scalable network applications. It supports writing client-side applications as well as server-side applications in an intuitive way. Issues like low level NIO selector programming, connection pool management, connection timeout detection or fragmented buffer reads are encapsulated by xSocket.

## Messaging

### [Apache Camel](http://activemq.apache.org/camel/)

Apache Camel is a POJO routing and mediation library for working with files, FTP, HTTP, MINA, JMS, JBI and web services.

### [Apache Qpid (incubating)](http://cwiki.apache.org/qpid/)

The Apache Qpid Project implemented [AMQP (Advanced Message Queuing Protocol)](http://www.amqp.org/) using Apache MINA.

### [Avis](http://avis.sourceforge.net/)

Avis is an event router service compatible with the commercial Elvin implementation developed by Mantara Software. Avis provides a fast, general-purpose publish/subscribe message bus.

### [MailsterSMTP](http://tedorg.free.fr/en/projects.php?section=smtp)

MailsterSMTP is designed to be a easy to understand Java library which provides a receptive SMTP server component. Using this library, you can easily receive mails using a simple Java interface, extend the set of implemented commands or control how mails are delivered by plugging your custom implementations.

## Instant Messaging

### [Jive Software Openfire](http://www.jivesoftware.com/products/openfire/)

Jive Software Openfire implemented [XMPP (Extensible Messaging and Presence Protocol)](http://www.xmpp.org/) server on top of Apache MINA. After switching to Apache MINA, [they gained 11 times scalability boost](http://community.igniterealtime.org/blogs/ignite/2006/12/19/scalability-turn-it-to-eleven/).

## Media Storage & Streaming

### [OpenLSD](http://openlsd.free.fr/en/OpenLSD.html)

OpenLSD is an open source framework for massive document archiving. The web site also contains an interesting performance test report.

### [Red5](http://www.osflash.org/red5)

OSFlash.org team implemented an open-source flash media streaming ([RTMP, Real Time Messaging Protocol](http://en.wikipedia.org/wiki/Real_Time_Messaging_Protocol)) server with Apache MINA.

## Miscellaneous

### [Apache Directory Project](http://directory.apache.org/)

The Apache Directory Project implemented LDAP v3, Kerberos, DNS, DHCP, NTP, and ChangePW using Apache MINA.

### [Beep4j](http://beep4j.sourceforge.net/)

Beep4j is an open-source implementation of the [BEEP](http://www.beepcore.org/) specification (RFC3080 and RFC3081).

### [VFS FTPServer Bridge](http://vfs-utils.sourceforge.net/ftpserver/index.html)

This project provides an Apache Commons VFS implementation for the Apache FTPServer project. Instead of working only on local files, with this VFS bridge you can connect to any VFS provider. You can still use a local file system, but you can also use a ZIP file, loop through to another FTP server, or use any other available VFS implementation such as DctmVFS.

### <a id="mina-apache-org-mina-project-related-projects--HDFSoverFTP"></a>[HDFS over FTP](https://sites.google.com/a/iponweb.net/hadoop/Home/hdfs-over-ftp)

FTP server which works on a top of HDFS. It aAllows to connect to HDFS using any FTP client. FTP server is configurable by hdfs-over-ftp.conf and users.conf. Also it allows to use secure connection over SSL and supports all HDFS permissions

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-road-map"></a>

# Road map — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-road-map--Navigation-Upcoming"></a>Upcoming

- No event

Please click the following links to find out what issues have been resolved and what issues will be resolved.

- [Road map for the future releases](http://issues.apache.org/jira/browse/DIRMINA?report=com.atlassian.jira.plugin.system.project:roadmap-panel&subset=-1)
- [Change log for the past releases](http://issues.apache.org/jira/browse/DIRMINA?report=com.atlassian.jira.plugin.system.project:changelog-panel&subset=-1)

## Genesis of MINA

by Trustin Lee

In June 2004, I released a network application framework, ‘Netty2’. It was the first network application framework that provides event-based architecture in Java community. It attracted network application programmers because of its simplicity and ease of use. As the Netty2 community matured, its problems also arose. Netty2 didn’t work fine with text protocols and had a critical architectural flaw that prevents users from using it for applications with many concurrent clients.

Quite a large amount of information was collected about what users like about Netty2 and what improvements they want from it for 6 months. It was clear they like its ease of use and unit-testability. They wanted support for UDP/IP and text protocols. I had to invent a cleaner, more flexible, and more extensible API so that it is easy to learn yet full-featured.

Meanwhile around 2003 at Apache Directory, Alex Karasulu was wrestling with a network application framework he developed based on the [Matt Welsh’s SEDA (Staged Event Driven Architecture)](http://www.sosp.org/2001/papers/welsh.pdf). After several iterations Alex realized it was very difficult to manage, and started to research other network application frameworks looking for a replacement. He wanted something for Java that would scale like SEDA yet was simple to use like [ACE](http://www.cs.wustl.edu/~schmidt/ACE.html). Alex encountered Netty2 at [gleamynode.net](https://web.archive.org/web/20130502105932/http://gleamynode.net/) and contacted me asking if I wanted to work with him on a new network application framework.

In September 2004, I formally joined the Apache Directory team. Alex and I decided to mix concepts between the two architectures to create a new network application framework. We exchanged various ideas to extract the strengths of both legacy frameworks to ultimately come up with what is today’s ‘MINA’.

Since then MINA became the primary network application framework used by the Apache Directory project for the various implemented by Apache Directory Server (ApacheDS). Several complex protocols in ApacheDS are implemented with MINA: LDAP, Kerberos, DNS and NTP.

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-sources"></a>

# Sources — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-sources--Navigation-Upcoming"></a>Upcoming

- No event

## Overview

Sources for the Apache MINA projects are currently managed through GIT. Instructions on GIT use can be found at [http://git-scm.com/book/](http://git-scm.com/book/).

For each project you can find a detailed description how to checkout and build the source on the project documentation. This page is just a short overview.

# Normal Git Access

Anyone can check code out of Git. You only need to specify a username and password in order to update the Git repository, and only MINA committers have the permissions to do that. We run Git over standard HTTPS, so hopefully you won’t have problems with intervening firewalls.

## Web Access

The following is a link to the [online source repository](https://gitbox.apache.org/repos/asf?p=mina.git;a=summary).

# Cloning from the Git repo

Again, anyone can do this. Use a command like to checkout the current development version (the trunk):

### For MINA

read only access :

```bash
$ git clone http://gitbox.apache.org/repos/asf/mina.git mina
```

write access :

```bash
$ git clone https://gitbox.apache.org/repos/asf/mina.git mina
```

Note that you will get the full repository, and you may probably want to work on a specific branch. We currently have 3 active branches :

- Mina 2.0.X
- Mina 2.1.X
- Mina 2.2.X

Cloning the MINA repository will get you to the trunk, ie the MINA 3.0 branch. If you want to work on the MINA 2.0.X branch, you ought to checkout the latest 2.0.X tag, after having cloned the repository :

```bash
$ git checkout -b 2.0.X 2.0.X
```

You can also clone the branch you want to work on directly:

```bash
$ git clone -b 2.2.X https://gitbox.apache.org/repos/asf/mina.git mina-2.2.X
```

Will checkout the 2.2.X branche immediately, in a directory named mina-2.2.X.

### 

# Building MINA

Instructions on how to build MINA can be found [here](#mina-apache-org-mina-project-developer-guide)

# Released version

The following table displays the URL of each project, and the URL where you can find information about how to build each project.

| Subproject/Documentation | Git URL |
| --- | --- |
| MINA | https://gitbox.apache.org/repos/asf/mina.git |
| FtpServer | https://gitbox.apache.org/repos/asf/mina-ftpserver.git |
| SSHD | https://gitbox.apache.org/repos/asf/mina-sshd.git |
| Vysper | https://gitbox.apache.org/repos/asf/mina-vysper.git |
| AsyncWeb | https://gitbox.apache.org/repos/asf/mina-asyncweb.git |

# Commit Changes to Git

In order to be able to push some modification, you have to be a committer.

# Documentation

The Website documentation is published via Apache SVN pubsub. The website source resides at

[https://svn.apache.org/repos/asf/mina/site/trunk](https://svn.apache.org/repos/asf/mina/site/trunk)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-testimonials"></a>

# Testimonials — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-testimonials--Navigation-Upcoming"></a>Upcoming

- No event

[**Marko Asplund**](http://practicingtechie.wordpress.com/2012/08/06/asynchronous-event-driven-servers-with-apache-mina/) says:

> I found that Apache MINA really did fulfill its promise and implementing a high-performance, scalable and extensible network server was easy using it. MINA also helps very cleanly separate network communication and application level message processing logic. Supporting multiple different protocols in in the same server is well supported in MINA. As a downside the documentation for v2.0 is a bit lacking, but fortunately there are quite a few code samples that you can check out.

**Maarten Bosteels** says:

> EURid used MINA during the landrush for .eu domain names on the 7th of april 2006. More than 700.000 domain names were registered during the first 4 hours. After one hour MINA had handled more than 0.5 million SSL connections.

> We found the speed and stability of MINA to be excellent. And although we are still using MINA 0.8.1, we found the API very elegant and easy.

**Frédéric Brégier** says:

> MINA helped us to get the network layout of OpenLSD done in about 2 months, saving us about 9 months to 1 year of development and fine-grained testing, so we were able to focus on our problem; Open Legacy Storage Document, a framework for document archiving in a huge storage. OpenLSD brings security, network layout, JDBC, and good performance, and allows at least 2 petabytes of documents (2000 terabytes, the limit is virtually 2^192 bytes).

> Our benchmark test went well with massive import capacity (through network with multiple processes) of 1400 documents per second, network (web) retrieves of 1000 documents per second with small latency. Network (MINA) was not the bottleneck so we were able to focus mainly on database optimization. For the web interface, our Tomcat web application connected to OpenLSD Server using pool of MINA connections almost like a JDBC pool.

> Any MINA problems were resolved very quickly either by a quick fix in MINA source code (a few) or by code fix with the support of the MINA mailing list. This was one reason of this success. Therefore we will continue to use MINA on other related project (email archiving and mimic of a professional file transfer monitor). Again, MINA is helping us to
> focus on what the applications need to do and not too much on network layout.

**Alex Burmester** says:

> We are using MINA at a telco to route low level protocol packets to a third party. We already had a SOAP and also a CORBA interface but for speed purposes we are trying out a lower level protocol and we needed a gateway of sorts to route messages between our cluster of servers and the third party’s servers.

> I had been planning on using NIO and some aspects of SEDA but finding MINA was a real treat as it saved a lot of time, is well written and gets more testing than our in house QA would be able to cover. The speed and stability of our app on top of MINA has been excellent.

**Nicholas Clare** says:

> We use MINA as a networking library to handle concurrent connections to our text based communication server. MINA has worked like a charm. It makes writing server applications simple and is much easier to use than Java’s NIO libraries. Because of MINA’s stability and ease of use, we plan on using MINA more in our future projects.

**Jean-François Daune** says:

> We use MINA to communicate with [Banksys](http://www.banksys.com/) ‘point of sale’ terminals (Visa, Mastercard…) for technical management operations. (software upgrade, remote monitoring, log transfer…)

> So far, MINA has worked really well for us. We used Netty2, and clearly saw the improvements in MINA. I like the MINA API more. MINA really makes it easier to write applications using NIO.

**Luke Hubbard** says:

> We are using it for the network layer of [Red5](http://www.osflash.org/red5), an open source flash server. At the moment we have RTMP and AMF working and hope to add more protocols in the future. MINA’s design and ease of use has helped us get a prototype up and running quickly.

**Thomas Muller** says:

> What a fantastic API! Definitely the best I’ve seen since [Doug Lea’s Concurrency API](http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html).

**Paolo Perrucci** says:

> We are using MINA to build the network layer of our multiplayer game server at [Leonardo.it](http://ludonet.leonardo.it/). Using MINA, we implemented different protocols in a few days; Game and HTTP tunneling. In the past, we used NIO, and the advantage of using MINA is evident; the MINA API is elegant and very simple to use. Last, but not least, MINA have a really responsive support.

**Frédéric Soulier** says:

> In 3 days, starting from scratch (knowing nothing about MINA) and with help from this list, I’ve re-implemented something that took us 2+ months to develop! I’ve thrown 4000 concurrent connections at it without a problem. The only problem I faced was to increase the limit for open files on my linux box (default was 1024).

**Niklas Therning** says:

> [SpamDrain](http://www.spamdrain.net/), our online anti-spam service, has been using MINA since late 2005. So far we have developed custom proxies for the POP3, IMAP and SMTP protocols on top of the MINA API. Before MINA we used our own Java NIO abstraction layer which had some serious stability problems. With MINA we haven’t experienced any stability issues and the MINA API has really helped us write cleaner code. I just love the way MINA helps you separate the decoding and encoding of protocol messages from the implementation of the protocol’s state machine. Other features, like SSLFilter which gives you SSL support virtually without any effort, are also very much appreciated.

**Julien Vermillard** says:

> I’m using MINA for supervisory control and data acquisition (SCADA) embedded application. It’s used for several tasks; connecting supervision clients to the server, interaction of the server with different hardware (other SCADA systems, media stream matrix, programmable automaton, remote data acquisition systems), custom replication protocols for fail-over service. I found MINA when I started implementation using NIO and it was a great time saver. You can switch from RS232 to TCP/IP and add SSL connectivity easily. The stability and the support is really great. The code and the design are simple and efficient, so you can easily implement high quality protocol logic without bothering with all the NIO quirks. I didn’t really tested the maximum performance you can get out of MINA, but all I can say is that MINA is running 24/7 with an amazing stability and I’m not afraid of using it in harsh environment.

[**Ashish Paliwal**](http://www.ashishpaliwal.com/blog) says:

> Used Apache MINA for Building a Trap Receiver

> see [White Paper](http://www.hsc.com/HSFiles/Wpos/WhitePaper_Trap_Receiver_using_Apache_MINA.pdf)

**Emmanuel Lécharny** says:

> MINA handles the following protocols in ADS :

> - LDAP
> - DNS
> - NTP
> - DHCP
> - Kerberos
>   We also use it to manage replication (using a specific protocol to communicate between two LDAP server). A LDAP client is being drafted atm, using MINA 2.0 too.
>   We are using 1.1.7 currently, but a migration to 2.0 is ready (and we will switch as soon as 2.0.0-M4 will be released)

**Dan Creswell** says:

> I’ve used it to build:

> 1. A framework for Paxos consensus
> 2. A remote transport for a JavaSpace
> 3. A transport for various gossip-based protocols

**Matthew Estes** says:

> I’m using it for an asynchronous messaging framework (think RPC/RMI), which
> is mostly done, but needs documenting and clean up. I also plan to use it
> to for the HTTP part of a web framework/container. Most of my work is/will
> be Apache 2.0 licensed open source

**Kevin Williams** says:

> We used Mina to build an internal distributed coherent cache system

**Matthew Phillips** says:

> The Avis event notification router and client library uses MINA.

> [Avis](http://avis.sourceforge.net/)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch10-executor-filter-ch10-executor-filter"></a>

# Chapter 10 - Executor Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch10-executor-filter-ch10-executor-filter--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 9 - Codec Filter](#mina-apache-org-mina-project-userguide-ch9-codec-filter-ch9-codec-filter)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 11 - SSL Filter](#mina-apache-org-mina-project-userguide-ch11-ssl-filter-ch11-ssl-filter)

# Chapter 10 - Executor Filter

MINA 1.X version let the user define the Thread Model at the Acceptor level. It was part of the Acceptor configuration. This led to complexity, and the MINA team decided to remove this option, replacing it with a much more versatile system, based on a filter : the **ExecutorFilter**.

## The ExecutorFilter class

This class is implementing the IoFilter interface, and basically, it contains an Executor to spread the incoming events to a pool of threads. This will allow an application to use more efficiently the processors, if some tasks are CPU intensive.

This Filter can be used just before the handlers, assuming that most of the processing will be done in your application, or somewhere before some CPU intensive filter (for instance, a CodecFilter).

It uses an *Executor* instance to process the tasks, and can limit the number of events that can be sent to this executor. By default, the following events can be passed to the executor:

- close
- event
- exceptionCaught
- inputClosed
- messageReceived
- messageSent
- sessionCreated
- sessionClosed
- sessionIdle
- sessionOpened
- write

[Chapter 9 - Codec Filter](#mina-apache-org-mina-project-userguide-ch9-codec-filter-ch9-codec-filter)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 11 - SSL Filter](#mina-apache-org-mina-project-userguide-ch11-ssl-filter-ch11-ssl-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter"></a>

# Chapter 12 - Logging Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 11 - SSL Filter](#mina-apache-org-mina-project-userguide-ch11-ssl-filter-ch11-ssl-filter)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 13 - Debugging](#mina-apache-org-mina-project-userguide-ch13-debugging-ch13-debugging)

# Chapter 12 - Logging Filter

- [SLF4J](#mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter--slf4j)
  - [Choosing the Right JARs](#mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter--choosing-the-right-jars)
  - [Overriding Jakarta Commons Logging](#mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter--overriding-jakarta-commons-logging)
- [log4j example](#mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter--log4j-example)

# Background

The Apache MINA uses a system that allows for the developer of the MINA-base application to use their own logging system.

## SLF4J

MINA employs the Simple Logging Facade for Java (SLF4J). You can find information on SLF4J here. This logging utility allows for the implementation of any number of logging systems. You may use log4j, java.util.logging or other logging systems. The nice part about this is that if you want to change from java.util.logging to log4j later on in the development process, you do not need to change your source code at all.

### Choosing the Right JARs

SLF4J uses a static binding. This means there is one JAR file for each supported logging framework. You can use your favorite logging framework by choosing the JAR file that calls the logging framework you chose statically. The following is the table of required JAR files to use a certain logging framework.

| Logging framework | Required JARs |
| --- | --- |
| Log4J 1.2.x | slf4j-api.jar**, **slf4j-log4j12.jar** |
| Log4J 1.3.x | slf4j-api.jar,slf4j-log4j13.jar |
| java.util.logging | slf4j-api.jar**, **slf4j-jdk14.jar** |
| Commons Logging | slf4j-api.jar,slf4j-jcl.jar |

There are a few things to keep in mind:

- slf4j-api.jar is used commonly across any implementation JARs.
- **IMPORTANT** You should not put more than one implementation JAR files in the class path (e.g. slf4j-log4j12.jar and slf4j-jdk14.jar); it might lead your application to a unexpected behavior.
- The version of slf4j-api.jar and slf4j-.jar should be identical.

Once configured properly, you can continue to configure the actual logging framework you chose (e.g. modifying log4j.properties).

### Overriding Jakarta Commons Logging

SLF4J also provides a way to convert the existing applications that use Jakarta Commons Logging to use SLF4J without changing the application code. Just remove commons-logging JAR file from the class path, and add jcl104-over-slf4j.jar to the class path.

## log4j example

For this example we will use the log4j logging system. We set up a project and place the following snippet into a file called log4j.properties:

```text
# Set root logger level to DEBUG and its only appender to A1.
log4j.rootLogger=DEBUG, A1

# A1 is set to be a ConsoleAppender.
log4j.appender.A1=org.apache.log4j.ConsoleAppender

# A1 uses PatternLayout.
log4j.appender.A1.layout=org.apache.log4j.PatternLayout
log4j.appender.A1.layout.ConversionPattern=%-4r [%t] %-5p %c{1} %x - %m%n
```

This file will be placed in the src directory of our project. If you are using an IDE, you essentially want the configuration file to be in the classpath for the JVM when you are testing your code.

Although this shows you how to set up an IoAcceptor to use logging, understand that the SLF4J API may be used anywhere in your program in order to generate proper logging information suitable to your needs.

Next we will set up a simple example server in order to generate some logs. Here we have taken the EchoServer example project and added logging to the class:

```java
public static void main(String[] args) throws Exception {
    IoAcceptor acceptor = new SocketAcceptor();
    DefaultIoFilterChainBuilder chain = acceptor.getFilterChain();

    LoggingFilter loggingFilter = new LoggingFilter();
    chain.addLast("logging", loggingFilter);                  

    acceptor.setLocalAddress(new InetSocketAddress(PORT));
    acceptor.setHandler(new EchoProtocolHandler());
    acceptor.bind();

    System.out.println("Listening on port " + PORT);
}
```

As you can see we removed the addLogger method and added in the 2 lines added to the example EchoServer. With a reference to the LoggingFilter, you can set the logging level per event type in your handler that is associated with the IoAcceptor here. In order to specify the IoHandler events that trigger logging and to what levels the logging is performed, there is a method in the LoggingFilter called setLogLevel(IoEventType, LogLevel). Below are the options for this method:

| IoEventType | Description |
| --- | --- |
| SESSION_CREATED | Called when a new session has been created |
| SESSION_OPENED | Called when a new session has been opened |
| SESSION_CLOSED | Called when a session has been closed |
| MESSAGE_RECEIVED | Called when data has been received |
| MESSAGE_SENT | Called when a message has been sent |
| SESSION_IDLE | Called when a session idle time has been reached |
| EXCEPTION_CAUGHT | Called when an exception has been thrown |

Here are the descriptions of the LogLevels:

| LogLevel | Description |
| --- | --- |
| NONE | This will result in no log event being created regardless of the configuration |
| TRACE | Creates a TRACE event in the logging system |
| DEBUG | Generates debug messages in the logging system |
| INFO | Generates informational messages in the logging system |
| WARN | Generates warning messages in the logging system |
| ERROR | Generates error messages in the logging system |

With this information, you should be able to get a basic system up and running and be able to expand upon this simple example in order to be generating log information for your system.

[Chapter 11 - SSL Filter](#mina-apache-org-mina-project-userguide-ch11-ssl-filter-ch11-ssl-filter)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 13 - Debugging](#mina-apache-org-mina-project-userguide-ch13-debugging-ch13-debugging)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine"></a>

# Chapter 14 - State Machine — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 13 - Debugging](#mina-apache-org-mina-project-userguide-ch13-debugging-ch13-debugging)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 15 - Proxy](#mina-apache-org-mina-project-userguide-ch15-proxy-ch15-proxy)

# Chapter 14 - State Machine

If you are using MINA to develop an application with complex network interactions you may at some point find yourself reaching for the good old [State pattern](http://home.earthlink.net/~huston2/dp/state.html) to try to sort out some of that complexity. However, before you do that you might want to checkout mina-statemachine which tries to address some of the shortcomings of the State pattern.

- [A simple example](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine--a-simple-example)

- [Lookup a StateContext object](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine--lookup-a-statecontext-object)
- [Convert the method invocation into an Event object](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine--convert-the-method-invocation-into-an-event-object)
- [Invoke the StateMachine](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine--invoke-the-statemachine)
- [Execute the Transition](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine--execute-the-transition)

- [State inheritance](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine--state-inheritance)
- [Error handling using state inheritance](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine--error-handling-using-state-inheritance)
- [mina-statemachine with IoHandler](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine--mina-statemachine-with-iohandler)

- [Changing state programmatically](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine--changing-state-programmatically)
- [Calling the state machine recursively](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine--calling-the-state-machine-recursively)

## A simple example

Let’s demonstrate how mina-statemachine works with a simple example. The picture below shows a state machine for a typical tape deck. The ellipsis are the states while the arrows are the transitions. Each transition is labeled with an event name which triggers that transition.

![](mina.apache.org/assets/img/mina/state-diagram.png)

Initially, the tape deck is in the **Empty** state. When a tape is inserted the **load** event is fired and the tape deck moves to the **Loaded** state. In **Loaded** the **eject** event will trigger a move back to **Empty** while the **play** event will trigger a move to the **Playing** state. And so on… I think you can work out the rest on your own.

Now let’s write some code. The outside world (the code interfacing with the tape deck) will only see the TapeDeck interface:

```java
public interface TapeDeck {
    void load(String nameOfTape);
    void eject();
    void start();
    void pause();
    void stop();
}
```

Next we will write the class which contains the actual code executed when a transition occurs in the state machine. First we will define the states. The states are all defined as constant String objects and are annotated using the @State annotation:

```java
public class TapeDeckHandler {
    @State public static final String EMPTY   = "Empty";
    @State public static final String LOADED  = "Loaded";
    @State public static final String PLAYING = "Playing";
    @State public static final String PAUSED  = "Paused";
}
```

Now when we have the states defined we can set up the code corresponding to each transition. Each transition will correspond to a method in TapeDeckHandler. Each transition method is annotated using the @Transition annotation which defines the event id which triggers the transition (on), the start state of the transition (in) and the end state of the transition (next):

```java
public class TapeDeckHandler {
    @State public static final String EMPTY = "Empty";
    @State public static final String LOADED = "Loaded";
    @State public static final String PLAYING = "Playing";
    @State public static final String PAUSED = "Paused";

    @Transition(on = "load", in = EMPTY, next = LOADED)
    public void loadTape(String nameOfTape) {
        System.out.println("Tape '" + nameOfTape + "' loaded");
    }

    @Transitions({
        @Transition(on = "play", in = LOADED, next = PLAYING),
        @Transition(on = "play", in = PAUSED, next = PLAYING)
    })
    public void playTape() {
        System.out.println("Playing tape");
    }

    @Transition(on = "pause", in = PLAYING, next = PAUSED)
    public void pauseTape() {
        System.out.println("Tape paused");
    }

    @Transition(on = "stop", in = PLAYING, next = LOADED)
    public void stopTape() {
        System.out.println("Tape stopped");
    }

    @Transition(on = "eject", in = LOADED, next = EMPTY)
    public void ejectTape() {
        System.out.println("Tape ejected");
    }
}
```

Please note that the TapeDeckHandler class does not implement the TapeDeck interface. That’s intentional.

Now, let’s have a closer look at some of this code. The @Transition annotation on the loadTape method

```java
@Transition(on = "load", in = EMPTY, next = LOADED)
public void loadTape(String nameOfTape) {
```

specifies that when the tape deck is in the EMPTY state and the load event occurs the loadTape method will be invoked and then the tape deck will move on to the LOADED state. The @Transition annotations on the pauseTape, stopTape and ejectTape methods should not require any further explanation. The annotation on the playTape method looks slightly different though. As can be seen in the diagram above, when the tape deck is in either the LOADED or in the PAUSED state the play event will play the tape. To have the same method called for multiple transitions the @Transitions annotation has to be used:

```java
@Transitions({
    @Transition(on = "play", in = LOADED, next = PLAYING),
       @Transition(on = "play", in = PAUSED, next = PLAYING)
})
public void playTape() {
```

The @Transitions annotation simply lists multiple transitions for which the annotated method will be called.

**More about the @Transition parameters**  

- If you omit the on parameter it will default to "\*" which will match any event.
- If you omit the next parameter it will default to "\_*self*\_" which is an alias for the current state. To create a loop transition in your state machine all you have to do is to omit the next parameter.
- The weight parameter can be used to define in what order transitions will be searched. Transitions for a particular state will be ordered in ascending order according to their weight value. weight is 0 by default.

Now the final step is to create a StateMachine object from the annotated class and use it to create a proxy object which implements TapeDeck:

```java
public static void main(String[] args) {
    TapeDeckHandler handler = new TapeDeckHandler();
    StateMachine sm = StateMachineFactory.getInstance(Transition.class).create(TapeDeckHandler.EMPTY, handler);
    TapeDeck deck = new StateMachineProxyBuilder().create(TapeDeck.class, sm);

    deck.load("The Knife - Silent Shout");
    deck.play();
    deck.pause();
    deck.play();
    deck.stop();
    deck.eject();
}
```

The lines

```java
TapeDeckHandler handler = new TapeDeckHandler();
StateMachine sm = StateMachineFactory.getInstance(Transition.class).create(TapeDeckHandler.EMPTY, handler);
```

creates the StateMachine instance from an instance of TapeDeckHandler. The Transition.class in the call to StateMachineFactory.getInstance(…) tells the factory that we’ve used the @Transition annotation to build the state machine. We specify EMPTY as the start state. A StateMachine is basically a directed graph. State objects correspond to nodes in the graph while Transition objects correspond to edges. Each @Transition annotation we used in the TapeDeckHandler will correspond to a Transition instance.

**Uhhm, what's the difference between @Transition and Transition?**  
@Transition is the annotation you use to mark a method which should be used when a transition between states occur. Behind the scenes mina-statemachine will create instances of the MethodTransition class for each @Transition annotated method. MethodTransition implements the Transition interface. As a mina-statemachine user you will never use the Transition or MethodTransition types directly.

The TapeDeck instance is created by calling StateMachineProxyBuilder:

```java
TapeDeck deck = new StateMachineProxyBuilder().create(TapeDeck.class, sm);
```

The StateMachineProxyBuilder.create() method takes the interfaces the returned proxy object should implement and the StateMachine instance which will receive the events generated by the method calls on the proxy.

When the code is executed the output should be:

```text
Tape 'The Knife - Silent Shout' loaded
Playing tape
Tape paused
Playing tape
Tape stopped
Tape ejected
```

**What does all this have to do with MINA?**  
As you might have noticed there's nothing MINA specific about this example. But don't be alarmed. Later on we will see how to create state machines for MINA's IoHandler interface.

# How does it work?

Let’s walk through what happens when a method is called on the proxy.

## Lookup a StateContext object

The StateContext object is important because it holds the current State. When a method is called on the proxy it will ask a StateContextLookup instance to get the StateContext from the method’s arguments. Normally, the StateContextLookup implementation will loop through the method arguments and look for a particular type of object and use it to retrieve a StateContext object. If no StateContext has been assigned yet the StateContextLookup will create one and store it in the object.

When proxying MINA’s IoHandler we will use a IoSessionStateContextLookup instance which looks for an IoSession in the method arguments. It will use the IoSession’s attributes to store a separate instance of StateContext for each MINA session. That way the same state machine can be used for all MINA sessions without them interfering with each other.

In the example above we never specified what StateContextLookup implementation to use when we created the proxy using StateMachineProxyBuilder. If not specified a SingletonStateContextLookup will be used. SingletonStateContextLookup totally disregards the method arguments passed to it – it'll always return the same StateContext object. Obviously this won't be very useful when the same state machine is used concurrently by many clients as will be the case when we proxy IoHandler later on.

## Convert the method invocation into an Event object

All method invocations on the proxy object will be translated into Event objects by the proxy. An Event has an id and zero or more arguments. The id corresponds to the name of the method and the event arguments correspond to the method arguments. The method call deck.load(“The Knife - Silent Shout”) corresponds to the event {id = “load”, arguments = [“The Knife - Silent Shout”]}. The Event object also contains a reference to the StateContext object looked up previously.

## Invoke the StateMachine

Once the Event object has been created the proxy will call StateMachine.handle(Event). StateMachine.handle(Event) loops through the Transition objects of the current State in search for a Transition instance which accepts the current Event. This process will stop after a Transition has been found. The Transition objects will be searched in order of weight (typically specified by the @Transition annotation).

## Execute the Transition

The final step is to call Transition.execute(Event) on the Transition which matched the Event. After the Transition has been executed the StateMachine will update the current State with the end state defined by the Transition.

Transition is an interface. Every time you use the @Transition annotation a MethodTransition object will be created.

# MethodTransition

MethodTransition is very important and requires some further explanation. MethodTransition matches an Event if the event’s id matches the on parameter of the @Transition annotation and the annotated method’s arguments are assignment compatible with a subset of the event’s arguments.

So, if the Event looks like {id = “foo”, arguments = [a, b, c]} the method

```java
@Transition(on = "foo")
public void someMethod(One one, Two two, Three three) { ... }
```

matches if and only if ((a instanceof One && b instanceof Two && c instanceof Three) == true). On match the method will be called with the matching event arguments bound to the method’s arguments:

```java
someMethod(a, b, c);
```

Integer, Double, Float, etc also match their primitive counterparts int, double, float, etc.

As stated above also a subset would match:

```java
@Transition(on = "foo")
public void someMethod(Two two) { ... }
```

matches if ((a instanceof Two || b instanceof Two || c instanceof Two) == true). In this case the first matching event argument will be bound to the method argument named two when someMethod is called.

A method which takes no arguments always matches if the event id matches:

```java
@Transition(on = "foo")
public void someMethod() { ... }
```

To make things even more complicated the first two method arguments also matches against the Event class and the StateContext interface. This means that

```java
@Transition(on = "foo")
public void someMethod(Event event, StateContext context, One one, Two two, Three three) { ... }
@Transition(on = "foo")
public void someMethod(Event event, One one, Two two, Three three) { ... }
@Transition(on = "foo")
public void someMethod(StateContext context, One one, Two two, Three three) { ... }
```

also matches the Event {id = “foo”, arguments = [a, b, c]} if ((a instanceof One && b instanceof Two && c instanceof Three) == true). The current Event object will be bound to the event method argument and the current StateContext will be bound to context when someMethod is invoked.

As before a subset of the event arguments can be used. Also, a specific StateContext implementation may be specified instead of using the generic interface:

```java
@Transition(on = "foo")
public void someMethod(MyStateContext context, Two two) { ... }
```

The order of the method arguments is important. If the method needs access to the current Event it must be specified as the first method argument. StateContext has to be the either the second arguments if the first is Event or the first argument. The event arguments also have to match in the correct order. MethodTransition will not try to reorder the event's arguments in search for a match.

If you’ve made it this far, congratulations! I realize that the section above might be a little hard to digest. Hopefully some examples could make things clearer:

Consider the Event {id = “messageReceived”, arguments = [ArrayList a = […], Integer b = 1024]}. The following methods match this Event:

```java
// All method arguments matches all event arguments directly
@Transition(on = "messageReceived")
public void messageReceived(ArrayList l, Integer i) { ... }

// Matches since ((a instanceof List && b instanceof Number) == true)
@Transition(on = "messageReceived")
public void messageReceived(List l, Number n) { ... }

// Matches since ((b instanceof Number) == true)
@Transition(on = "messageReceived")
public void messageReceived(Number n) { ... }

// Methods with no arguments always matches
@Transition(on = "messageReceived")
public void messageReceived() { ... }

// Methods only interested in the current Event or StateContext always matches
@Transition(on = "messageReceived")
public void messageReceived(StateContext context) { ... }

// Matches since ((a instanceof Collection) == true)
@Transition(on = "messageReceived")
public void messageReceived(Event event, Collection c) { ... }
```

The following would not match:

```java
// Incorrect ordering
@Transition(on = "messageReceived")
public void messageReceived(Integer i, List l) { ... }

// ((a instanceof LinkedList) == false)
@Transition(on = "messageReceived")
public void messageReceived(LinkedList l, Number n) { ... }

// Event must be first argument
@Transition(on = "messageReceived")
public void messageReceived(ArrayList l, Event event) { ... }

// StateContext must be second argument if Event is used
@Transition(on = "messageReceived")
public void messageReceived(Event event, ArrayList l, StateContext context) { ... }

// Event must come before StateContext
@Transition(on = "messageReceived")
public void messageReceived(StateContext context, Event event) { ... }
```

## State inheritance

State instances may have a parent State. If StateMachine.handle(Event) cannot find a Transition matching the current Event in the current State it will search the parent State. If no match is found there either the parent’s parent will be searched and so on.

This feature is useful when you want to add some generic code to all states without having to specify @Transition annotations for each state. Here’s how you create a hierarchy of states using the @State annotation:

```java
@State    public static final String A = "A";
@State(A) public static final String B = "A->B";
@State(A) public static final String C = "A->C";
@State(B) public static final String D = "A->B->D";
@State(C) public static final String E = "A->C->E";
```

## Error handling using state inheritance

Let’s go back to the TapeDeck example. What happens if you call deck.play() when there’s no tape in the deck? Let’s try:

```java
public static void main(String[] args) {
    ...
    deck.load("The Knife - Silent Shout");
    deck.play();
    deck.pause();
    deck.play();
    deck.stop();
    deck.eject();
    deck.play();
}

...
Tape stopped
Tape ejected
Exception in thread "main" o.a.m.sm.event.UnhandledEventException: 
Unhandled event: org.apache.mina.statemachine.event.Event@15eb0a9[id=play,...]
    at org.apache.mina.statemachine.StateMachine.handle(StateMachine.java:285)
    at org.apache.mina.statemachine.StateMachine.processEvents(StateMachine.java:142)
    ...
```

Oops! We get an UnhandledEventException because when we’re in the Empty state there’s no transition which handles the play event. We could add a special transition to all states which handles unmatched Event objects:

```java
@Transitions({
    @Transition(on = "*", in = EMPTY, weight = 100),
    @Transition(on = "*", in = LOADED, weight = 100),
    @Transition(on = "*", in = PLAYING, weight = 100),
    @Transition(on = "*", in = PAUSED, weight = 100)
})
public void error(Event event) {
    System.out.println("Cannot '" + event.getId() + "' at this time");
}
```

Now when you run the main() method above you won’t get an exception. The output should be:

```text
    ...
    Tape stopped
    Tape ejected
    Cannot 'play' at this time.
```

Now this seems to work very well, right? But what if we had 30 states instead of only 4? Then we would need 30 @Transition annotations on the error() method. Not good. Let’s use state inheritance instead:

```java
public static class TapeDeckHandler {
    @State public static final String ROOT = "Root";
    @State(ROOT) public static final String EMPTY = "Empty";
    @State(ROOT) public static final String LOADED = "Loaded";
    @State(ROOT) public static final String PLAYING = "Playing";
    @State(ROOT) public static final String PAUSED = "Paused";
    
    ...
    
    @Transition(on = "*", in = ROOT)
    public void error(Event event) {
        System.out.println("Cannot '" + event.getId() + "' at this time");
    }
}
```

The result will be the same but things will be much easier to maintain with this last approach.

## mina-statemachine with IoHandler

Now we’re going to convert our tape deck into a TCP server and extend it with some more functionality. The server will receive commands like load , play, stop, etc. The responses will either be positive +  or negative - . The protocol is text based, all commands and responses are lines of UTF-8 text terminated by CRLF (i.e. \r\n in Java). Here’s an example session:

```text
telnet localhost 12345
S: + Greetings from your tape deck!
C: list
S: + (1: "The Knife - Silent Shout", 2: "Kings of convenience - Riot on an empty street")
C: load 1
S: + "The Knife - Silent Shout" loaded
C: play
S: + Playing "The Knife - Silent Shout"
C: pause
S: + "The Knife - Silent Shout" paused
C: play
S: + Playing "The Knife - Silent Shout"
C: info
S: + Tape deck is playing. Current tape: "The Knife - Silent Shout"
C: eject
S: - Cannot eject while playing
C: stop
S: + "The Knife - Silent Shout" stopped
C: eject
S: + "The Knife - Silent Shout" ejected
C: quit
S: + Bye! Please come back!
```

The complete code for the TapeDeckServer described in this section is available in the org.apache.mina.example.tapedeck package in the mina-example module in the Subversion repository. The code uses a MINA ProtocolCodecFilter to convert bytes from/to Command objects. There is one Command implementation for each type of request the server recognizes. We will not describe the codec implementation here in any detail.

Now, let’s have a look at how this server works. The important class which implements the state machine is the TapeDeckServer class. The first thing we do is to define the states:

```java
@State public static final String ROOT = "Root";
@State(ROOT) public static final String EMPTY = "Empty";
@State(ROOT) public static final String LOADED = "Loaded";
@State(ROOT) public static final String PLAYING = "Playing";
@State(ROOT) public static final String PAUSED = "Paused";
```

Nothing new there. However, the methods which handle the events now look different. Let’s look at the playTape method:

```java
@IoHandlerTransitions({
    @IoHandlerTransition(on = MESSAGE_RECEIVED, in = LOADED, next = PLAYING),
    @IoHandlerTransition(on = MESSAGE_RECEIVED, in = PAUSED, next = PLAYING)
})
public void playTape(TapeDeckContext context, IoSession session, PlayCommand cmd) {
    session.write("+ Playing \"" + context.tapeName + "\"");
}
```

This code doesn’t use the general @Transition and @Transitions annotations used previously but rather the MINA specific @IoHandlerTransition and @IoHandlerTransitions annotations. This are preferred when creating state machines for MINA’s IoHandler interface as they let you use a Java enum for the event ids instead of strings as we used before. There are also corresponding annotations for MINA’s IoFilter interface.

We’re now using MESSAGE\_RECEIVED instead of “play” for the event name (the on attribute in @IoHandlerTransition). This constant is defined in org.apache.mina.statemachine.event.IoHandlerEvents and has the value “messageReceived” which of course corresponds to the messageReceived() method in MINA’s IoHandler interface. Thanks to Java5’s static imports we don’t have to write out the name of the class holding the constant. We just need to put the

```java
import static org.apache.mina.statemachine.event.IoHandlerEvents.*;
```

statement in the imports section.

Another thing that has changed is that we’re using a custom StateContext implementation, TapeDeckContext. This class is used to keep track of the name of the current tape:

```java
static class TapeDeckContext extends AbstractStateContext {
    public String tapeName;
}
```

**Why not store tape name in IoSession?**  
We could have stored the name of the tape as an attribute in the IoSession but using a custom StateContext is recommended since it provides type safety.

The last thing to note about the playTape() method is that it takes a PlayCommand as its last argument. The last argument corresponds to the message argument of IoHandler’s messageReceived(IoSession session, Object message) method. This means that playTape() method will only be called if the bytes sent by the client can be decoded as a PlayCommand.

Before the tape deck can play anything a tape has to be loaded. When a LoadCommand is received from the client the supplied tape number will be used to get the name of the tape to load from the tapes array of available tapes:

```java
@IoHandlerTransition(on = MESSAGE_RECEIVED, in = EMPTY, next = LOADED)
public void loadTape(TapeDeckContext context, IoSession session, LoadCommand cmd) {
    if (cmd.getTapeNumber() < 1 || cmd.getTapeNumber() > tapes.length) {
        session.write("- Unknown tape number: " + cmd.getTapeNumber());
        StateControl.breakAndGotoNext(EMPTY);
    } else {
        context.tapeName = tapes[cmd.getTapeNumber() - 1];
        session.write("+ \"" + context.tapeName + "\" loaded");
    }
}
```

This code uses the StateControl class to override the next state. If the user specify an unknown tape number we shouldn’t move to the LOADED state but instead remain in EMPTY which is what the

```java
StateControl.breakAndGotoNext(EMPTY);
```

line does. The StateControl class is described more in a later section.

The connect() method will always be called at the start of a session when MINA calls sessionOpened() on the IoHandler:

```java
@IoHandlerTransition(on = SESSION_OPENED, in = EMPTY)
public void connect(IoSession session) {
    session.write("+ Greetings from your tape deck!");
}
```

All it does is to write the greeting to the client. The state machine will remain in the EMPTY state.

The pauseTape(), stopTape() and ejectTape() methods are very similar to playTape() and won’t be described in any detail. The listTapes(), info() and quit() methods should be simple enough to understand by now, too. Please note how these last three methods are used for the ROOT state. This means that the list, info and quit commands can be issued in any state.

Now let’s have a look at error handling. The error() method will be called when the client sends a Command which isn’t legal in the current state:

```java
@IoHandlerTransition(on = MESSAGE_RECEIVED, in = ROOT, weight = 10)
public void error(Event event, StateContext context, IoSession session, Command cmd) {
    session.write("- Cannot " + cmd.getName() + " while " 
           + context.getCurrentState().getId().toLowerCase());
}
```

error() has been given a higher weight than listTapes(), info() and quit() to prevent it to be called for any of those commands. Notice how error() uses the StateContext object to get hold of the id of the current state. The values of the String constants which are annotated with the @State annotation (Empty, Loaded etc) will be used by mina-statemachine as state id.

The commandSyntaxError() method will be called when a CommandSyntaxException has been thrown by our ProtocolDecoder. It simply prints out that the line sent by the client couldn’t be converted into a Command.

The exceptionCaught() will be called for any thrown exception except CommandSyntaxException (it has a higher weight than the commandSyntaxError() method). It closes the session immediately.

The last @IoHandlerTransition method is unhandledEvent() which will be called if none of the other @IoHandlerTransition methods match the Event. We need this since we don’t have @IoHandlerTransition annotations for all possible types of events in all states (e.g., we never handle messageSent events). Without this mina-statemachine throws an exception if an Event is handled by the state machine.

The last piece of code we’re going to have a look at is the code which creates the IoHandler proxy and the main() method:

```java
private static IoHandler createIoHandler() {
    StateMachine sm = StateMachineFactory.getInstance(IoHandlerTransition.class).create(EMPTY, new TapeDeckServer());
        
    return new StateMachineProxyBuilder().setStateContextLookup(
            new IoSessionStateContextLookup(new StateContextFactory() {
                public StateContext create() {
                    return new TapeDeckContext();
                }
            })).create(IoHandler.class, sm);
}

// This code will work with MINA 1.0/1.1:
public static void main(String[] args) throws Exception {
    SocketAcceptor acceptor = new SocketAcceptor();
    SocketAcceptorConfig config = new SocketAcceptorConfig();
    config.setReuseAddress(true);
    ProtocolCodecFilter pcf = new ProtocolCodecFilter(
            new TextLineEncoder(), new CommandDecoder());
    config.getFilterChain().addLast("codec", pcf);
    acceptor.bind(new InetSocketAddress(12345), createIoHandler(), config);
}

// This code will work with MINA trunk:
public static void main(String[] args) throws Exception {
    SocketAcceptor acceptor = new NioSocketAcceptor();
    acceptor.setReuseAddress(true);
    ProtocolCodecFilter pcf = new ProtocolCodecFilter(
            new TextLineEncoder(), new CommandDecoder());
    acceptor.getFilterChain().addLast("codec", pcf);
    acceptor.setHandler(createIoHandler());
    acceptor.setLocalAddress(new InetSocketAddress(PORT));
    acceptor.bind();
}
```

createIoHandler() creates a StateMachine just like we did before except that we specify IoHandlerTransition.class instead of Transition.class in the call to StateMachineFactory.getInstance(…). This is necessary since we’re now using the @IoHandlerTransition annotation. Also, this time we use IoSessionStateContextLookup and a custom StateContextFactory when we create the IoHandler proxy. If we didn’t use IoSessionStateContextLookup all clients would share the same state machine which isn’t desirable.

The main() method creates the SocketAcceptor and attaches a ProtocolCodecFilter which decodes/encodes Command objects to its filter chain. Finally, it binds to port 12345 using an IoHandler instance created by the createIoHandler() method.

# Advanced topics

## Changing state programmatically

To be written…

## Calling the state machine recursively

To be written…

[Chapter 13 - Debugging](#mina-apache-org-mina-project-userguide-ch13-debugging-ch13-debugging)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 15 - Proxy](#mina-apache-org-mina-project-userguide-ch15-proxy-ch15-proxy)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch16-jmx-support-ch16-jmx-support"></a>

# Chapter 16 - JMX Support — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch16-jmx-support-ch16-jmx-support--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 15 - Proxy](#mina-apache-org-mina-project-userguide-ch15-proxy-ch15-proxy)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 17 - Spring Integration](#mina-apache-org-mina-project-userguide-ch17-spring-integration-ch17-spring-integration)

# Chapter 16 - JMX Support

Java Management Extensions (JMX) is used for managing and monitoring java applications. This tutorial will provide you with an example as to how you can JMX-enable your MINA based application.

This tutorial is designed to help you get the JMX technology integrated in to your MINA-based application. In this tutorial, we will integrate the MINA-JMX classes into the imagine server example program.

# Adding JMX Support

To JMX enable MINA application we have to perform following

- Create/Get MBean server
- Instantiate desired MBeans (IoAcceptor, IoFilter)
- Register MBeans with MBean server

We shall follow \src\main\java\org\apache\mina\example\imagine\step3\server\ImageServer.java, for the rest of our discussion

## Create/Get MBean server

```java
// create a JMX MBean Server server instance
MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
```

This lines get the MBean Server instance.

## Instantiate MBean(s)

We create an MBean for IoService

```java
// create a JMX-aware bean that wraps a MINA IoService object.  In this
// case, a NioSocketAcceptor. 
IoServiceMBean acceptorMBean = new IoServiceMBean( acceptor );
```

This creates an IoService MBean. It accepts instance of an acceptor that it exposed via JMX.

Similarly, you can add IoFilterMBean and other custom MBeans as well

## Registering MBeans with MBean Server

```java
// create a JMX ObjectName.  This has to be in a specific format.  
ObjectName acceptorName = new ObjectName( acceptor.getClass().getPackage().getName() +
        ":type=acceptor,name=" + acceptor.getClass().getSimpleName());
    
// register the bean on the MBeanServer.  Without this line, no JMX will happen for
// this acceptor.
mBeanServer.registerMBean( acceptorMBean, acceptorName );
```

We create an ObjectName that need to be used as logical name for accessing the MBean and register the MBean to the MBean Server. Our application in now JMX enabled. Lets see it in action.

## Start the Imagine Server

If you are using Java 5 or earlier:

```bash
java -Dcom.sun.management.jmxremote -classpath <CLASSPATH> org.apache.mina.example.imagine.step3.server.ImageServer
```

If you are using Java 6:

```bash
java  -classpath <CLASSPATH> }}{{{}org.apache.mina.example.imagine.step3.server.ImageServer
```

## Start JConsole

Start JConsole using the following command:

```bash
<JDK_HOME>/bin/jconsole
```

We can see the different attributes and operations that are exposed by the MBeans

[Chapter 15 - Proxy](#mina-apache-org-mina-project-userguide-ch15-proxy-ch15-proxy)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 17 - Spring Integration](#mina-apache-org-mina-project-userguide-ch17-spring-integration-ch17-spring-integration)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch17-spring-integration-ch17-spring-integration"></a>

# Chapter 17 - Spring Integration — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch17-spring-integration-ch17-spring-integration--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 16 - Adding JMX Support](#mina-apache-org-mina-project-userguide-ch16-jmx-support-ch16-jmx-support)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

# Chapter 17 - Spring Integration

This article demonstrates integrating MINA application with Spring. I wrote this article on my blog, and though to put it here, where this information actually belongs to. Can find the original copy at [Integrating Apache MINA with Spring](http://www.ashishpaliwal.com/blog/2008/11/integrating-apache-mina-with-spring/).

## Application Structure

We shall take a standard MINA application which has following construct

- One Handler
- Two Filter - Logging Filter and a ProtocolCodec Filter
- NioDatagram Socket

### Initialization Code

Lets see the code first. For simplicity we have omitted the glue code.

```java
public void initialize() throws IOException {

    // Create an Acceptor
    NioDatagramAcceptor acceptor = new NioDatagramAcceptor();

    // Add Handler
    acceptor.setHandler(new ServerHandler());

    acceptor.getFilterChain().addLast("logging",
                new LoggingFilter());
    acceptor.getFilterChain().addLast("codec",
                new ProtocolCodecFilter(new SNMPCodecFactory()));

    // Create Session Configuration
    DatagramSessionConfig dcfg = acceptor.getSessionConfig();
        dcfg.setReuseAddress(true);
        logger.debug("Starting Server......");
        // Bind and be ready to listen
        acceptor.bind(new InetSocketAddress(DEFAULT_PORT));
        logger.debug("Server listening on "+DEFAULT_PORT);
}
```

## Integration Process

To integrate with Spring, we need to do following:

- Set the IO handler
- Create the Filters and add to the chain
- Create the Socket and set Socket Parameters

NOTE: The latest MINA releases doesn’t have the package specific to Spring, like its earlier versions. The package is now named Integration Beans, to make the implementation work for all DI frameworks.

Lets see the Spring xml file. Please see that I have removed generic part from xml and have put only the specific things needed to pull up the implementation.
This example has been derived from [Chat example](http://svn.apache.org/viewvc/mina/mina/branches/2.0/mina-example/src/main/java/org/apache/mina/example/chat/) shipped with MINA release. Please refer the xml shipped with chat example.

Now lets pull things together

Lets set the IO Handler in the spring context file

```xml
    <!-- The IoHandler implementation -->
    <bean id="trapHandler" class="com.ashishpaliwal.udp.mina.server.ServerHandler">
```

Lets create the Filter chain

```xml
    <bean id="snmpCodecFilter" class="org.apache.mina.filter.codec.ProtocolCodecFilter">
      <constructor-arg>
        <bean class="com.ashishpaliwal.udp.mina.snmp.SNMPCodecFactory" />
      </constructor-arg>
    </bean>
    
    <bean id="loggingFilter" class="org.apache.mina.filter.logging.LoggingFilter" />
    
    <!-- The filter chain. -->
    <bean id="filterChainBuilder" class="org.apache.mina.core.filterchain.DefaultIoFilterChainBuilder">
      <property name="filters">
        <map>
          <entry key="loggingFilter" value-ref="loggingFilter"/>
          <entry key="codecFilter" value-ref="snmpCodecFilter"/>
        </map>
      </property>
    </bean>
```

Here, we create instance of our IoFilter. See that for the ProtocolCodec factory, we have used Constructor injection. Logging Filter creation is straight forward. Once we have defined the beans for the filters to be used, we now create the Filter Chain to be used for the implementation. We define a bean with id “FilterChainBuidler” and add the defined filters to it. We are almost ready, and we just need to create the Socket and call bind

Lets complete the last part of creating the Socket and completing the chain

```xml
    <bean class="org.springframework.beans.factory.config.CustomEditorConfigurer">
        <property name="customEditors">
          <map>
            <entry key="java.net.SocketAddress">
              <bean class="org.apache.mina.integration.beans.InetSocketAddressEditor" />
            </entry>
          </map>
        </property>
    </bean>
          
    <!-- The IoAcceptor which binds to port 161 -->
    <bean id="ioAcceptor" class="org.apache.mina.transport.socket.nio.NioDatagramAcceptor" init-method="bind" destroy-method="unbind">
      <property name="defaultLocalAddress" value=":161" />
      <property name="handler" ref="trapHandler" />
      <property name="filterChainBuilder" ref="filterChainBuilder" />
    </bean>    
```

Now we create our ioAcceptor, set IO handler and Filter Chain. Now we have to write a function to read this file using Spring and start our application. Here’s the code

```java
    public void initializeViaSpring() throws Exception {
        new ClassPathXmlApplicationContext("trapReceiverContext.xml");
    }
```

[Chapter 16 - Adding JMX Support](#mina-apache-org-mina-project-userguide-ch16-jmx-support-ch16-jmx-support)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters"></a>

# Chapter 5 - Filters — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

# Chapter 5 - Filters

- [5.1 - Blacklist Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-1-blacklist-filter)
- [5.2 - Buffered Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-2-buffered-write-filter)
- [5.3 - Compression Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-3-compression-filter)
- [5.4 - Connection Throttle Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-4-connection-throttle-filter)
- [5.5 - Error Generating Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-5-error-generating-filter)
- [5.6 - Executor Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-6-executor-filter)
- [5.7 - FileRegion Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-7-file-region-write-filter)
- [5.8 - KeepAlive Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-8-keep-alive-filter)
- [5.9 - Logging Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-9-logging-filter)
- [5.10 - MDC Injection Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-10-mdc-injection-filter)
- [5.11 - NOOP Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-11-noop-filter)
- [5.12 - Profiler Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-12-profiler-filter)
- [5.13 - Protocol Codec Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-13-protocol-codec-filter)
- [5.14 - Proxy Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-14-proxy-filter)
- [5.15 - Reference Counting Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-15-reference-counting-filter)
- [5.16 - Request/Response Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-16-request-response-filter)
- [5.17 - Session Attribute Initializing Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-17-session-attribute-initializing-filter)
- [5.18 - Stream Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-18-stream-write-filter)
- [5.19 - SSL/TLS Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-19-ssl-filter)
- [5.20 - Write Request Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-20-write-request-filter)

## Introduction

IoFilter is one of the MINA core constructs that serves a very important role. It filters all I/O events and requests between IoService and IoHandler. If you have an experience with web application programming, you can safely think that it’s a cousin of Servlet filter. Many out-of-the-box filters are provided to accelerate network application development pace by simplifying typical cross-cutting concerns using the out-of-the-box filters such as:

- LoggingFilter logs all events and requests.
- ProtocolCodecFilter converts an incoming ByteBuffer into message POJO and vice versa.
- CompressionFilter compresses all data.
- SSLFilter adds SSL - TLS - StartTLS support.
- and many more!

In this tutorial, we will walk through how to implement an IoFilter for a real world use case. It’s easy to implement an IoFilter in general, but you might also need to know specifics of MINA internals. Any related internal properties will be explained here.

- [Introduction](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters--introduction)
- [Filters already present](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters--filters-already-present)
- [Overriding Events Selectively](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters--overriding-events-selectively)
- [Transforming a Write Request](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters--transforming-a-write-request)
- [Be Careful When Filtering sessionCreated Event](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters--be-careful-when-filtering-sessioncreated-event)
- [Watch out the Empty Buffers!](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters--watch-out-the-empty-buffers)

## Filters already present

We have many filters already written. The following table list all the existing filters, with a short description of their usage.

| Filter | class | Description |
| --- | --- | --- |
| Blacklist | BlacklistFilter | Blocks connections from blacklisted remote addresses |
| Buffered Write | BufferedWriteFilter | Buffers outgoing requests like the BufferedOutputStream does |
| Compression | CompressionFilter |  |
| ConnectionThrottle | ConnectionThrottleFilter |  |
| ErrorGenerating | ErrorGeneratingFilter |  |
| Executor | ExecutorFilter |  |
| FileRegionWrite | FileRegionWriteFilter |  |
| KeepAlive | KeepAliveFilter |  |
| Logging | LoggingFilter | Logs event messages, like MessageReceived, MessageSent, SessionOpened, … |
| MDC Injection | MdcInjectionFilter | Inject key IoSession properties into the MDC |
| Noop | NoopFilter | A filter that does nothing. Useful for tests. |
| Profiler | ProfilerTimerFilter | Profile event messages, like MessageReceived, MessageSent, SessionOpened, … |
| ProtocolCodec | ProtocolCodecFilter | A filter in charge of encoding and decoding messages |
| Proxy | ProxyFilter |  |
| Reference counting | ReferenceCountingFilter | Keeps track of the number of usages of this filter |
| SessionAttributeInitializing | SessionAttributeInitializingFilter |  |
| StreamWrite | StreamWriteFilter |  |
| SslFilter | SslFilter |  |
| WriteRequest | WriteRequestFilter |  |

## Overriding Events Selectively

You can extend IoAdapter instead of implementing IoFilter directly. Unless overridden, any received events will be forward to the next filter immediately:

```java
public class MyFilter extends IoFilterAdapter {
    @Override
    public void sessionOpened(NextFilter nextFilter, IoSession session) throws Exception {
        // Some logic here...
        nextFilter.sessionOpened(session);
        // Some other logic here...
    }
}
```

## Transforming a Write Request

If you are going to transform an incoming write request via IoSession.write(), things can get pretty tricky. For example, let’s assume your filter transforms HighLevelMessage to LowLevelMessage when IoSession.write() is invoked with a HighLevelMessage object. You could insert appropriate transformation code to your filter’s filterWrite() method and think that’s all. However, you have to note that you also need to take care of messageSent event because an IoHandler or any filters next to yours will expect messageSent() method is called with HighLevelMessage as a parameter, because it’s irrational for the caller to get notified that LowLevelMessage is sent when the caller actually wrote HighLevelMessage. Consequently, you have to implement both filterWrite() and messageSent() if your filter performs transformation.

Please also note that you still need to implement similar mechanism even if the types of the input object and the output object are identical (e.g. CompressionFilter) because the caller of IoSession.write() will expect exactly what he wrote in his or her messageSent() handler method.

Let’s assume that you are implementing a filter that transforms a String into a char[]. Your filter’s filterWrite() will look like the following:

```java
public void filterWrite(NextFilter nextFilter, IoSession session, WriteRequest request) {
    nextFilter.filterWrite(
        session, new DefaultWriteRequest(
                ((String) request.getMessage()).toCharArray(), request.getFuture(), request.getDestination()));
}
```

Now, we need to do the reverse in messageSent():

```java
public void messageSent(NextFilter nextFilter, IoSession session, Object message) {
    nextFilter.messageSent(session, new String((char[]) message));
}
```

What about String-to-ByteBuffer transformation? We can be a little bit more efficient because we don’t need to reconstruct the original message (String). However, it’s somewhat more complex than the previous example:

```java
public void filterWrite(NextFilter nextFilter, IoSession session, WriteRequest request) {
    String m = (String) request.getMessage();
    ByteBuffer newBuffer = new MyByteBuffer(m, ByteBuffer.wrap(m.getBytes());
    
    nextFilter.filterWrite(
            session, new WriteRequest(newBuffer, request.getFuture(), request.getDestination()));
}
        
public void messageSent(NextFilter nextFilter, IoSession session, Object message) {
    if (message instanceof MyByteBuffer) {
        nextFilter.messageSent(session, ((MyByteBuffer) message).originalValue);
    } else {
        nextFilter.messageSent(session, message);
    }
}

private static class MyByteBuffer extends ByteBufferProxy {
    private final Object originalValue;
    private MyByteBuffer(Object originalValue, ByteBuffer encodedValue) {
        super(encodedValue);
        this.originalValue = originalValue;
    }
}
```

If you are using MINA 2.0, it will be somewhat different from 1.0 and 1.1. Please refer to [CompressionFilter](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/filter/compression/CompressionFilter.html) meanwhile.

## Be Careful When Filtering sessionCreated Event

sessionCreated is a special event that must be executed in the I/O processor thread (see Configuring Thread Model). Never forward sessionCreated event to the other thread.

```java
public void sessionCreated(NextFilter nextFilter, IoSession session) throws Exception {
    // ...
    nextFilter.sessionCreated(session);
}

// DON'T DO THIS!
public void sessionCreated(final NextFilter nextFilter, final IoSession session) throws Exception {
    Executor executor = ...;
    executor.execute(new Runnable() {
        nextFilter.sessionCreated(session);
        });
    }
```

## Watch out the Empty Buffers!

MINA uses an empty buffer as an internal signal at a couple of cases. Empty buffers sometimes become a problem because it’s a cause of various exceptions such as IndexOutOfBoundsException. This section explains how to avoid such a unexpected situation.

ProtocolCodecFilter uses an empty buffer (i.e. buf.hasRemaining() = 0) to mark the end of the message. If your filter is placed before the ProtocolCodecFilter, please make sure your filter forward the empty buffer to the next filter if your filter implementation can throw a unexpected exception if the buffer is empty:

```java
public void messageSent(NextFilter nextFilter, IoSession session, Object message) {
    if (message instanceof ByteBuffer && !((ByteBuffer) message).hasRemaining()) {
        nextFilter.messageSent(nextFilter, session, message);
        return;
    }
    ...
}

public void filterWrite(NextFilter nextFilter, IoSession session, WriteRequest request) {
    Object message = request.getMessage();
    if (message instanceof ByteBuffer && !((ByteBuffer) message).hasRemaining()) {
        nextFilter.filterWrite(nextFilter, session, request);
        return;
    }
    ...
}
```

Do we always have to insert the if block for every filters? Fortunately, you don’t have to. Here’s the golden rule of handling empty buffers:

- If your filter works without any problem even if the buffer is empty, you don’t need to add the if blocks at all.
- If your filter is placed after ProtocolCodecFilter, you don’t need to add the if blocks at all.
- Otherwise, you need the if blocks.

If you need the if blocks, please remember you don’t always need to follow the example above. You can check if the buffer is empty wherever you want as long as your filter doesn’t throw a unexpected exception.

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports-index"></a>

# Chapter 6 - Transports — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports-index--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 7 - Handler](#mina-apache-org-mina-project-userguide-ch7-handler-ch7-handler)

# Chapter 6 - Transports

- [6.1 - APR Transport](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-1-apr-transport)
- [6.2 - Serial Transport](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-2-serial-transport)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 7 - Handler](#mina-apache-org-mina-project-userguide-ch7-handler-ch7-handler)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch6-transports-ch6-1-apr-transport"></a>

# 6.1 - APR Transport — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch6-transports-ch6-1-apr-transport--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

[6.2 - Serial Transport](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-2-serial-transport)

# 6.1 - APR Transport

## Introduction

[APR (Apache Portable Runtime)](https://apr.apache.org/) provide superior scalability, performance, and better integration with native server technologies. APR transport is supported by MINA. In this section, we shall touch base upon how to use APR transport with MINA. We shall the Time Server example for this.

## Pre-requisite

APR transport depends following components  
APR library - Download/install appropriate library for the platform from [https://www.apache.org/dist/tomcat/tomcat-connectors/native/](https://www.apache.org/dist/tomcat/tomcat-connectors/native/)  
JNI wrapper (tomcat-apr-5.5.23.jar) The jar is shipped with release

Put the native library in PATH

## Using APR Transport

Refer [Time Server]https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/example/gettingstarted/timeserver/) example for complete source

Lets see how NIO based Time server implementation looks like

```java
IoAcceptor acceptor = new NioSocketAcceptor();

acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new TextLineCodecFactory( Charset.forName( "UTF-8" ))));

acceptor.setHandler(  new TimeServerHandler() );

acceptor.getSessionConfig().setReadBufferSize( 2048 );
acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );

acceptor.bind( new InetSocketAddress(PORT) );
```

Lets see how to use APR Transport

```java
IoAcceptor acceptor = new AprSocketAcceptor();

acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new TextLineCodecFactory( Charset.forName( "UTF-8" ))));

acceptor.setHandler(  new TimeServerHandler() );

acceptor.getSessionConfig().setReadBufferSize( 2048 );
acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );

acceptor.bind( new InetSocketAddress(PORT) );
```

We just change the NioSocketAcceptor to AprSocketAcceptor. That’s it, now our Time Server shall use APR transport.

Rest complete process remains same.

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

[6.2 - Serial Transport](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-2-serial-transport)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch6-transports-ch6-2-serial-transport"></a>

# 6.2 - Serial Transport — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch6-transports-ch6-2-serial-transport--Navigation-Upcoming"></a>Upcoming

- No event

[6.1 - APR Transport](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-1-apr-transport)

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

[Chapter 7 - Handler](#mina-apache-org-mina-project-userguide-ch7-handler-ch7-handler)

# 6.2 - Serial Transport

With the MINA 2.0 you are able to connect to serial port like you use to connect to a TCP/IP port with MINA.

## Getting MINA 2.0

You you can download the latest built version (2.0.2).

If you prefer to build the code from the trunk, and need assistance to do so, please consult the Developer Guide.

## Prerequisite

**Useful Information**  
Before accessing serial port from a Java program you need a native library (.DLL or .so depending of your OS). MINA use the one from RXTX.org : [ftp://ftp.qbang.org/pub/rxtx/rxtx-2.1-7-bins-r2.zip](ftp://ftp.qbang.org/pub/rxtx/rxtx-2.1-7-bins-r2.zip).   
Just put the good .dll or .so in the jre/lib/i386/ path of your JDK/JRE or use the -Djava.library.path= argument for specify where you placed the native libraries

**Useful Information**  
The **mina-transport-serial** jar is not included in the full distribution. You can download it from [here](https://repo1.maven.org/maven2/org/apache/mina/mina-transport-serial/2.0.2/)

## Connecting to a serial port

Serial communication for MINA provide only an IoConnector, due to the point-to-point nature of the communication media.

At this point you are supposed to have already read the MINA tutorial.

Now for connecting to a serial port you need a SerialConnector :

```java
// create your connector
IoConnector connector = new SerialConnector()
connector.setHandler( ... here your buisness logic IoHandler ... );
```

Nothing very different of a SocketConnector.

Let’s create an address for connecting to our serial port.

```java
SerialAddress portAddress=new SerialAddress( "/dev/ttyS0", 38400, 8, StopBits.BITS_1, Parity.NONE, FlowControl.NONE );
```

The first parameter is your port identifier. For Windows computer, the serial ports are called “COM1”, “COM2”, etc… For Linux and some other Unix : “/dev/ttyS0”, “/dev/ttyS1”, “/dev/ttyUSB0”.

The remaining parameters are depending of the device you are driving and the supposed communications characteristics.

- the baud rate
- the data bits
- the parity
- the flow control mechanism

Once it’s done, connect the connector to the address :

```java
ConnectFuture future = connector.connect( portAddress );
future.await();
IoSession sessin = future.getSession();
```

And voila ! Everything else is as usual, you can plug your filters and codecs.
for learn more about RS232 : [http://en.wikipedia.org/wiki/RS232](http://en.wikipedia.org/wiki/RS232)

[6.1 - APR Transport](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-1-apr-transport)

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

[Chapter 7 - Handler](#mina-apache-org-mina-project-userguide-ch7-handler-ch7-handler)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch9-codec-filter-ch9-codec-filter"></a>

# Chapter 9 - Codec Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch9-codec-filter-ch9-codec-filter--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 8 - IoBuffer](#mina-apache-org-mina-project-userguide-ch8-iobuffer-ch8-iobuffer)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 10 - Executor Filter](#mina-apache-org-mina-project-userguide-ch10-executor-filter-ch10-executor-filter)

# Chapter 9 - Codec Filter

This tutorial tries to explain why and how to use a ProtocolCodecFilter.

## Why use a ProtocolCodecFilter?

- TCP guarantees delivery of all packets in the correct order.
  But there is no guarantee that one write operation on the sender-side will result in one read event on the receiving side.
  see [http://en.wikipedia.org/wiki/IPv4#Fragmentation\_and\_reassembly](http://en.wikipedia.org/wiki/IPv4#Fragmentation_and_reassembly) and [http://en.wikipedia.org/wiki/Nagle%27s\_algorithm](http://en.wikipedia.org/wiki/Nagle%27s_algorithm)
  In MINA terminology: without a ProtocolCodecFilter one call of IoSession.write(Object message) by the sender can result in multiple messageReceived(IoSession session, Object message) events on the receiver; and multiple calls of IoSession.write(Object message) can lead to a single messageReceived event. You might not encounter this behavior when client and server are running on the same host (or an a local network) but your applications should be able to cope with this.
- Most network applications need a way to find out where the current message ends and where the next message starts.
- You could implement all this logic in your IoHandler, but adding a ProtocolCodecFilter will make your code much cleaner and easier to maintain.
- It allows you to separate your protocol logic from your business logic (IoHandler).

## How ?

Your application is basically just receiving a bunch of bytes and you need to convert these bytes into messages (higher level objects).

There are three common techniques for splitting the stream of bytes into messages:

- use fixed length messages
- use a fixed length header that indicates the length of the body
- using a delimiter; for example many text-based protocols append a newline (or CR LF pair) after every message ([http://www.faqs.org/rfcs/rfc977.html](http://www.faqs.org/rfcs/rfc977.html))

In this tutorial we will use the first and second method since they are definitely easier to implement. Afterwards we will look at using a delimiter.

## Example

We will develop a (pretty useless) graphical chargen server to illustrate how to implement your own protocol codec (ProtocolEncoder, ProtocolDecoder, and ProtocolCodecFactory).
The protocol is really simple. This is the layout of a request message:

| 4 bytes | 4 bytes | 4 bytes |
| --- | --- | --- |
| width | height | numchars |

- width: the width of the requested image (an integer in network byte-order)
- height: the height of the requested image (an integer in network byte-order)
- numchars: the number of chars to generate (an integer in network byte-order)

The server responds with two images of the requested dimensions, with the requested number of characters painted on it.
This is the layout of a response message:

| 4 bytes | variable length body | 4 bytes | variable length body |
| --- | --- | --- | --- |
| length1 | image1 | length2 | image2 |

Overview of the classes we need for encoding and decoding requests and responses:

- **ImageRequest**: a simple POJO representing a request to our ImageServer.
- **ImageRequestEncoder**: encodes ImageRequest objects into protocol-specific data (used by the client)
- **ImageRequestDecoder**: decodes protocol-specific data into ImageRequest objects (used by the server)
- **ImageResponse**: a simple POJO representing a response from our ImageServer.
- **ImageResponseEncoder**: used by the server for encoding ImageResponse objects
- **ImageResponseDecoder**: used by the client for decoding ImageResponse objects
- **ImageCodecFactory**: this class creates the necessary encoders and decoders

Here is the ImageRequest class :

```java
public class ImageRequest {
    
    private int width;
    private int height;
    private int numberOfCharacters;
    
    public ImageRequest(int width, int height, int numberOfCharacters) {
        this.width = width;
        this.height = height;
        this.numberOfCharacters = numberOfCharacters;
    }
    
    public int getWidth() {
        return width;
    }
    
    public int getHeight() {
        return height;
    }
    
    public int getNumberOfCharacters() {
        return numberOfCharacters;
    }
}
```

Encoding is usually simpler than decoding, so let’s start with the ImageRequestEncoder:

```java
public class ImageRequestEncoder implements ProtocolEncoder {
    
    public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {
        ImageRequest request = (ImageRequest) message;
        IoBuffer buffer = IoBuffer.allocate(12, false);
        buffer.putInt(request.getWidth());
        buffer.putInt(request.getHeight());
        buffer.putInt(request.getNumberOfCharacters());
        buffer.flip();
        out.write(buffer);
    }
    
    public void dispose(IoSession session) throws Exception {
        // nothing to dispose
    }
}
```

Remarks:

- MINA will call the encode function for all messages in the IoSession’s write queue. Since our client will only write ImageRequest objects, we can safely cast message to ImageRequest.
- We allocate a new IoBuffer from the heap. It’s best to avoid using direct buffers, since generally heap buffers perform better.
  see [http://issues.apache.org/jira/browse/DIRMINA-289](http://issues.apache.org/jira/browse/DIRMINA-289))
- You do not have to release the buffer, MINA will do it for you, see [https://nightlies.apache.org/mina/mina/2.0.22/apidocs/org/apache/mina/core/buffer/IoBuffer.html](https://nightlies.apache.org/mina/mina/2.0.22/apidocs/org/apache/mina/core/buffer/IoBuffer.html)
- In the dispose() method you should release all resources acquired during encoding for the specified session. If there is nothing to dispose you could let your encoder inherit from ProtocolEncoderAdapter.

Now let’s have a look at the decoder. The CumulativeProtocolDecoder is a great help for writing your own decoder: it will buffer all incoming data until your decoder decides it can do something with it.
In this case the message has a fixed size, so it’s easiest to wait until all data is available:

```java
public class ImageRequestDecoder extends CumulativeProtocolDecoder {
    
    protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
        if (in.remaining() >= 12) {
            int width = in.getInt();
            int height = in.getInt();
            int numberOfCharachters = in.getInt();
            ImageRequest request = new ImageRequest(width, height, numberOfCharachters);
            out.write(request);
            return true;
        } else {
            return false;
        }
    }
}
```

Remarks:

- every time a complete message is decoded, you should write it to the ProtocolDecoderOutput; these messages will travel along the filter-chain and eventually arrive in your IoHandler.messageReceived method
- you are not responsible for releasing the IoBuffer
- when there is not enough data available to decode a message, just return false

The response is also a very simple POJO:

```java
public class ImageResponse {
    
    private BufferedImage image1;
    
    private BufferedImage image2;
    
    public ImageResponse(BufferedImage image1, BufferedImage image2) {
        this.image1 = image1;
        this.image2 = image2;
    }
    
    public BufferedImage getImage1() {
        return image1;
    }
    
    public BufferedImage getImage2() {
        return image2;
    }
}
```

Encoding the response is also trivial:

```java
public class ImageResponseEncoder extends ProtocolEncoderAdapter {
    
    public void encode(IoSession session, Object message, ProtocolEncoderOutput out) throws Exception {
        ImageResponse imageResponse = (ImageResponse) message;
        byte[] bytes1 = getBytes(imageResponse.getImage1());
        byte[] bytes2 = getBytes(imageResponse.getImage2());
        int capacity = bytes1.length + bytes2.length + 8;
        IoBuffer buffer = IoBuffer.allocate(capacity, false);
        buffer.setAutoExpand(true);
        buffer.putInt(bytes1.length);
        buffer.put(bytes1);
        buffer.putInt(bytes2.length);
        buffer.put(bytes2);
        buffer.flip();
        out.write(buffer);
    }
    
    private byte[] getBytes(BufferedImage image) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(image, "PNG", baos);
        return baos.toByteArray();
    }
}
```

Remarks:

- when it is impossible to calculate the length of the IoBuffer beforehand, you can use an auto-expanding buffer by calling buffer.setAutoExpand(true);

Now let’s have a look at decoding the response:

```java
public class ImageResponseDecoder extends CumulativeProtocolDecoder {
    
    private static final String DECODER_STATE_KEY = ImageResponseDecoder.class.getName() + ".STATE";
    
    public static final int MAX_IMAGE_SIZE = 5 * 1024 * 1024;
    
    private static class DecoderState {
        BufferedImage image1;
    }
    
    protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
        DecoderState decoderState = (DecoderState) session.getAttribute(DECODER_STATE_KEY);
        if (decoderState == null) {
            decoderState = new DecoderState();
            session.setAttribute(DECODER_STATE_KEY, decoderState);
        }
        if (decoderState.image1 == null) {
            // try to read first image
            if (in.prefixedDataAvailable(4, MAX_IMAGE_SIZE)) {
                decoderState.image1 = readImage(in);
            } else {
                // not enough data available to read first image
                return false;
            }
        }
        if (decoderState.image1 != null) {
            // try to read second image
            if (in.prefixedDataAvailable(4, MAX_IMAGE_SIZE)) {
                BufferedImage image2 = readImage(in);
                ImageResponse imageResponse = new ImageResponse(decoderState.image1, image2);
                out.write(imageResponse);
                decoderState.image1 = null;
                return true;
            } else {
                // not enough data available to read second image
                return false;
            }
        }
        return false;
    }
    
    private BufferedImage readImage(IoBuffer in) throws IOException {
        int length = in.getInt();
        byte[] bytes = new byte[length];
        in.get(bytes);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        return ImageIO.read(bais);
    }
}
```

Remarks:

- We store the state of the decoding process in a session attribute. It would also be possible to store this state in the Decoder object itself but this has several disadvantages:
  - every IoSession would need its own Decoder instance
  - MINA ensures that there will never be more than one thread simultaneously executing the decode() function for the same IoSession, but it does not guarantee that it will always be the same thread. Suppose the first piece of data is handled by thread-1 who decides it cannot yet decode, when the next piece of data arrives, it could be handled by another thread. To avoid visibility problems, you must properly synchronize access to this decoder state (IoSession attributes are stored in a ConcurrentHashMap, so they are automatically visible to other threads).
  - a discussion on the mailing list has lead to this conclusion: choosing between storing state in the IoSession or in the Decoder instance itself is more a matter of taste. To ensure that no two threads will run the decode method for the same IoSession, MINA needs to do some form of synchronization => this synchronization will also ensure you can’t have the visibility problem described above.
    (Thanks to Adam Fisk for pointing this out)
    see [https://www.mail-archive.com/dev@mina.apache.org/msg03038.html](https://www.mail-archive.com/dev@mina.apache.org/msg03038.html)
- IoBuffer.prefixedDataAvailable() is very convenient when your protocol uses a length-prefix; it supports a prefix of 1, 2 or 4 bytes.
- don’t forget to reset the decoder state when you’ve decoded a response (removing the session attribute is another way to do it)

If the response would consist of a single image, we would not need to store decoder state:

```java
protected boolean doDecode(IoSession session, IoBuffer in, ProtocolDecoderOutput out) throws Exception {
    if (in.prefixedDataAvailable(4)) {
        int length = in.getInt();
        byte[] bytes = new byte[length];
        in.get(bytes);
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        BufferedImage image = ImageIO.read(bais);
        out.write(image);
        return true;
    } else {
        return false;
    }
}
```

Now let’s glue it all together:

```java
public class ImageCodecFactory implements ProtocolCodecFactory {
    private ProtocolEncoder encoder;
    private ProtocolDecoder decoder;
    
    public ImageCodecFactory(boolean client) {
        if (client) {
            encoder = new ImageRequestEncoder();
            decoder = new ImageResponseDecoder();
        } else {
            encoder = new ImageResponseEncoder();
            decoder = new ImageRequestDecoder();
        }
    }
    
    public ProtocolEncoder getEncoder(IoSession ioSession) throws Exception {
        return encoder;
    }
    
    public ProtocolDecoder getDecoder(IoSession ioSession) throws Exception {
        return decoder;
    }
}
```

Remarks:

- for every new session, MINA will ask the ImageCodecFactory for an encoder and a decoder.
- since our encoders and decoders store no conversational state, it is safe to let all sessions share a single instance.

This is how the server would use the ProtocolCodecFactory:

```java
public class ImageServer {
    public static final int PORT = 33789;
    
    public static void main(String[] args) throws IOException {
        ImageServerIoHandler handler = new ImageServerIoHandler();
        NioSocketAcceptor acceptor = new NioSocketAcceptor();
        acceptor.getFilterChain().addLast("protocol", new ProtocolCodecFilter(new ImageCodecFactory(false)));
        acceptor.setLocalAddress(new InetSocketAddress(PORT));
        acceptor.setHandler(handler);
        acceptor.bind();
        System.out.println("server is listenig at port " + PORT);
    }
}
```

Usage by the client is identical:

```java
public class ImageClient extends IoHandlerAdapter {
    public static final int CONNECT_TIMEOUT = 3000;
    
    private String host;
    private int port;
    private SocketConnector connector;
    private IoSession session;
    private ImageListener imageListener;
    
    public ImageClient(String host, int port, ImageListener imageListener) {
        this.host = host;
        this.port = port;
        this.imageListener = imageListener;
        connector = new NioSocketConnector();
        connector.getFilterChain().addLast("codec", new ProtocolCodecFilter(new ImageCodecFactory(true)));
        connector.setHandler(this);
    }
    
    public void messageReceived(IoSession session, Object message) throws Exception {
        ImageResponse response = (ImageResponse) message;
        imageListener.onImages(response.getImage1(), response.getImage2());
    }
    ...
```

For completeness, I will add the code for the server-side IoHandler:

```java
public class ImageServerIoHandler extends IoHandlerAdapter {
    
    private final static String characters = "mina rocks abcdefghijklmnopqrstuvwxyz0123456789";
    
    public static final String INDEX_KEY = ImageServerIoHandler.class.getName() + ".INDEX";
    
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    
    public void sessionOpened(IoSession session) throws Exception {
        session.setAttribute(INDEX_KEY, 0);
    }
    
    public void exceptionCaught(IoSession session, Throwable cause) throws Exception {
        IoSessionLogger sessionLogger = IoSessionLogger.getLogger(session, logger);
        sessionLogger.warn(cause.getMessage(), cause);
    }
    
    public void messageReceived(IoSession session, Object message) throws Exception {
        ImageRequest request = (ImageRequest) message;
        String text1 = generateString(session, request.getNumberOfCharacters());
        String text2 = generateString(session, request.getNumberOfCharacters());
        BufferedImage image1 = createImage(request, text1);
        BufferedImage image2 = createImage(request, text2);
        ImageResponse response = new ImageResponse(image1, image2);
        session.write(response);
    }
    
    private BufferedImage createImage(ImageRequest request, String text) {
        BufferedImage image = new BufferedImage(request.getWidth(), request.getHeight(), BufferedImage.TYPE_BYTE_INDEXED);
        Graphics graphics = image.createGraphics();
        graphics.setColor(Color.YELLOW);
        graphics.fillRect(0, 0, image.getWidth(), image.getHeight());
        Font serif = new Font("serif", Font.PLAIN, 30);
        graphics.setFont(serif);
        graphics.setColor(Color.BLUE);
        graphics.drawString(text, 10, 50);
        return image;
    }
    
    private String generateString(IoSession session, int length) {
        Integer index = (Integer) session.getAttribute(INDEX_KEY);
        StringBuffer buffer = new StringBuffer(length);
    
        while (buffer.length() < length) {
            buffer.append(characters.charAt(index));
            index++;
            if (index >= characters.length()) {
                index = 0;
            }
        }
        session.setAttribute(INDEX_KEY, index);
        return buffer.toString();
    }
}
```

![](mina.apache.org/assets/img/mina/codec-filter.jpeg)

## Conclusion

There is a lot more to tell about encoding and decoding. But I hope this tutorial already gets you started.
I will try to add something about the DemuxingProtocolCodecFactory in the near future.
And then we will also have a look at how to use a delimiter instead of a length prefix.

[Chapter 8 - IoBuffer](#mina-apache-org-mina-project-userguide-ch8-iobuffer-ch8-iobuffer)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 10 - Executor Filter](#mina-apache-org-mina-project-userguide-ch10-executor-filter-ch10-executor-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-user-guide-toc"></a>

# User Guide — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-user-guide-toc--Navigation-Upcoming"></a>Upcoming

- No event

## MINA 2.0 User Guide

Part I - Basics

- [Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)
  - [1.1 - NIO Overview](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-1-nio-overview)
  - [1.2 - Why MINA ?](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-2-why-mina)
  - [1.3 - Features](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-3-features)
  - [1.4 - First Steps](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-4-first-steps)
  - [1.5 - Summary](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-5-summary)
- [Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)
  - [2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)
    - [2.1.1 - Server Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-1-server-architecture)
    - [2.1.2 - Client Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-2-client-architecture)
  - [2.2 - Sample TCP Server](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-2-sample-tcp-server)
  - [2.3 - Sample TCP Client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-3-sample-tcp-client)
  - [2.4 - Sample UDP Server](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-4-sample-udp-server)
  - [2.5 - Sample UDP Client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-5-sample-udp-client)
  - [2.6 - Summary](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-6-summary)
- [Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)
  - [3.1 - IoService Introduction](#mina-apache-org-mina-project-userguide-ch3-service-ch3-1-io-service)
  - [3.2 - IoService Details](#mina-apache-org-mina-project-userguide-ch3-service-ch3-2-io-service-details)
  - [3.3 - IoAcceptor](#mina-apache-org-mina-project-userguide-ch3-service-ch3-3-acceptor)
  - [3.4 - IoConnector](#mina-apache-org-mina-project-userguide-ch3-service-ch3-4-connector)
- [Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)
  - [4.1 - Session Configuration](#mina-apache-org-mina-project-userguide-ch4-session-ch4-1-session-configuration)
  - [4.2 - Session Statistics](#mina-apache-org-mina-project-userguide-ch4-session-ch4-2-session-statistics)
- [Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)
  - [5.1 - Blacklist Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-1-blacklist-filter)
  - [5.2 - Buffered Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-2-buffered-write-filter)
  - [5.3 - Compression Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-3-compression-filter)
  - [5.4 - Connection Throttle Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-4-connection-throttle-filter)
  - [5.5 - Error Generating Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-5-error-generating-filter)
  - [5.6 - Executor Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-6-executor-filter)
  - [5.7 - FileRegion Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-7-file-region-write-filter)
  - [5.8 - KeepAlive Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-8-keep-alive-filter)
  - [5.9 - Logging Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-9-logging-filter)
  - [5.10 - MDC Injection Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-10-mdc-injection-filter)
  - [5.11 - NOOP Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-11-noop-filter)
  - [5.12 - Profiler Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-12-profiler-filter)
  - [5.13 - Protocol Codec Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-13-protocol-codec-filter)
  - [5.14 - Proxy Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-14-proxy-filter)
  - [5.15 - Reference Counting Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-15-reference-counting-filter)
  - [5.16 - Request/Response Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-16-request-response-filter)
  - [5.17 - Session Attribute Initializing Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-17-session-attribute-initializing-filter)
  - [5.18 - Stream Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-18-stream-write-filter)
  - [5.19 - SSL/TLS Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-19-ssl-filter)
  - [5.20 - Write Request Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-20-write-request-filter)
- [Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)
  - [6.1 - APR Transport](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-1-apr-transport)
  - [6.2 - Serial Transport](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-2-serial-transport)
- [Chapter 7 - Handler](#mina-apache-org-mina-project-userguide-ch7-handler-ch7-handler)

Part II - MINA Core

- [Chapter 8 - IoBuffer](#mina-apache-org-mina-project-userguide-ch8-iobuffer-ch8-iobuffer)
- [Chapter 9 - Codec Filter](#mina-apache-org-mina-project-userguide-ch9-codec-filter-ch9-codec-filter)
- [Chapter 10 - Executor Filter](#mina-apache-org-mina-project-userguide-ch10-executor-filter-ch10-executor-filter)
- [Chapter 11 - SSL Filter](#mina-apache-org-mina-project-userguide-ch11-ssl-filter-ch11-ssl-filter)
- [Chapter 12 - Logging Filter](#mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter)

Part III - MINA Advanced

- [Chapter 13 - Debugging](#mina-apache-org-mina-project-userguide-ch13-debugging-ch13-debugging)
- [Chapter 14 - State Machine](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine)
- [Chapter 15 - Proxy](#mina-apache-org-mina-project-userguide-ch15-proxy-ch15-proxy)
- [Chapter 16 - JMX Integration](#mina-apache-org-mina-project-userguide-ch16-jmx-support-ch16-jmx-support)
- [Chapter 17 - Spring Integration](#mina-apache-org-mina-project-userguide-ch17-spring-integration-ch17-spring-integration)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started"></a>

# Chapter 1 - Getting Started — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started--Navigation-Upcoming"></a>Upcoming

- No event

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

# Chapter 1 - Getting Started

In this chapter, we will give you first sense of what is **MINA**, what is **NIO**, why we developed a framework on top of **NIO** and what you will find inside.
We will also show you how to run a very simple example of a server run with **MINA**

- [1.1 - NIO Overview](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-1-nio-overview)
- [1.2 - Why MINA ?](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-2-why-mina)
- [1.3 - Features](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-3-features)
- [1.4 - First Steps](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-4-first-steps)
- [1.5 - Summary](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-5-summary)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-1-nio-overview"></a>

# 1.1 - NIO Overview — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-1-nio-overview--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[1.2 - Why MINA ?](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-2-why-mina)

# NIO Overview

The **NIO** API was introduced in **Java 1.4** and had since been used for wide number of applications. The **NIO** API covers **IO** non-blocking operations.

First of all, it's good to know that **MINA** is written on top of **NIO 1**. A new version has been designed in **Java 7**, **NIO-2**, we don't yet benefit from the added features this version is carrying.

It's also important to know that the **N** in **NIO** means **New**, but we will use the **Non-Blocking** term in many places. **NIO-2** should be seen as a **New New I/O**...

The `java.nio.*` package contains following key constructs

- *Buffers* - Data Containers
- *Chartsets* - Containers translators for bytes and Unicode
- *Channels* - represents connections to entities capable of I/O operations
- *Selectors* - provide selectable, multiplexed non-blocking IO
- *Regexps* - provide provide some tools to manipulate regular expressions

We are mostly interested in the *Channels*, *Selectors* and *Buffers* parts in the **MINA** framework, except that we want to hide those elements to the user.

This user guide will thus focus on everything built on top of those internal components.

# NIO vs BIO

It’s important to understand the difference between those two APIs. **BIO**, or **Blocking** **IO**, relies on plain sockets used in a blocking mode: when you read, write or do whatever operation on a socket, the called operation will block the caller until the operation is completed.

In some cases, it’s critical to be able to call the operation, and to expect the called operation to inform the caller when the operation is done: the caller can then do something else in the mean time.

This is also where **NIO** offers a better way to handle **IO** when you have numerous connected sockets: you don’t have to create a specific thread for each connection, you can just use a few threads to do the same job.

If you want to get more information about what covers **NIO**, there is a lot of good articles around the web, and a few books covering this matter.

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[1.2 - Why MINA ?](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-2-why-mina)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-2-why-mina"></a>

# 1.2 - Why MINA — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-2-why-mina--Navigation-Upcoming"></a>Upcoming

- No event

[1.1 - NIO Overview](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-1-nio-overview)

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[1.3 - Features](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-3-features)

# Why MINA?

Writing network applications are generally seen as a burden and perceived as low level development. It is an area which is not frequently studied or known by developers, either because it has been studied in school a long time ago and everything has been forgotten, or because the complexity of the network layer is frequently hidden by higher level layers, so you never get deep into it.

Added to that (when it comes to asynchronous IO) an extra layer of complexity comes into play: time.

The big difference between **BIO** (Blocking IO) and **NIO** (Non-Blocking IO) is that in **BIO**, you send a request, and you wait until you get the response. On the server side, it means one thread will be associated with any incoming connection, so you won’t have to deal with the complexity of multiplexing the connections. In **NIO**, on the other hand, you have to deal with the synchronous nature of a non-blocking system, which means that your application will be invoked when some events occur. In **NIO**, you don’t call and wait for a result, you send a command and you are informed when the result is ready.

## The need of a framework

Considering those differences, and the fact that most of the applications are usually expecting a blocking mode when invoking the network layer, the best solution is to hide this aspect by writing a framework that mimics a blocking mode. This is what **MINA** does!

But **MINA** does more. It provides a common IO vision to an application that needs to communicate over **TCP**, **UDP** or whatever mechanism. If we consider only **TCP** and **UDP**, one is a connected protocol (**TCP**) where the other is connectionless (**UDP**). **MINA** masks this difference, and makes you focus on the two parts that are important for your application: the application code and the application protocol encoding/decoding.

\***MINA** does not only handle **TCP** and **UDP**, it also offers a layer on top of serial communication (**RSC232**), over **VmpPipe** or **APR**.

Last but not least, **MINA** is a network framework that has been specifically designed to work on both the client side and server side. Writing a server makes it critical to have a scalable system, which is tunable to fit the server needs, in terms of performance and memory usage. This is what **MINA** is good for, making it easy to develop you server.

## When to use MINA?

This is an interesting question! **MINA** does not expect to be the best possible choice in all cases. There are a few elements to take into account when considering using **MINA**. Let’s list them:

- Ease of use
  When you have no special performance requirements, **MINA** is probably a good choice as it allows you to develop a server or a client easily, without having to deal with the various parameters and use cases to handle when writing the same application on top of **BIO** or **NIO**. You could probably write your server with only a few lines of code, and there are less pitfalls in which you are likely to fall.
- A high number of connected users
  **BIO** is definitively faster that **NIO**. The difference is something like 30% in favor of **BIO**. This is true for up to a few thousands of connected users, but up to a point, the **BIO** approach just stops scaling; you won’t be able to handle millions of connected users using one thread per user! **NIO** can. Now, one other aspect is that the time spent in the **MINA** part of your code is probably non significant, compared to whatever your application will consume. At some point, it’s probably not worthwhile to spend the energy trying to writing a faster network layer on your own for a gain which will be barely noticeable.
- A proven system
  **MINA** is used by many applications all over the world. There are some *Apache* projects based on **MINA**, and they are working pretty well. This gives you the ease of mind that you won’t have to spend hours on some cryptic errors in your own implementation of the network layer.
- Existing supported protocols
  **MINA** ships with various implemented protocols: HTTP, XML, TCP, LDAP, DHCP, NTP, DNS, XMPP, SSH, FTP… At some point, **MINA** can be seen not only as a **NIO** framework, but as a network layer with some protocol implementation. In the near future **MINA** may offer a more extensive collection of protocols for you to use.

[1.1 - NIO Overview](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-1-nio-overview)

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[1.3 - Features](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-3-features)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-3-features"></a>

# 1.3 Features — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-3-features--Navigation-Upcoming"></a>Upcoming

- No event

[1.2 - Why MINA ?](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-2-why-mina)

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[1.4 - First steps](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-4-first-steps)

# Features

MINA is a simple yet full-featured network application framework which provides:

- Unified API for various transport types:
  - TCP/IP & UDP/IP via Java NIO
  - Serial communication (RS232) via RXTX
  - In-VM pipe communication
  - You can implement your own!
- Filter interface as an extension point; similar to Servlet filters
- Low-level and high-level API:
  - Low-level: uses ByteBuffers
  - High-level: uses user-defined message objects and codecs
- Highly customizable thread model:
  - Single thread
  - One thread pool
  - More than one thread pools (i.e. SEDA)
- Out-of-the-box SSL · TLS · StartTLS support using Java 5 SSLEngine
- Overload shielding & traffic throttling
- Unit testability using mock objects
- JMX manageability
- Stream-based I/O support via StreamIoHandler
- Integration with well known containers such as PicoContainer and Spring
- Smooth migration from Netty, an ancestor of Apache MINA.

[1.2 - Why MINA ?](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-2-why-mina)

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[1.4 - First steps](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-4-first-steps)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-4-first-steps"></a>

# 1.4 - First Steps — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-4-first-steps--Navigation-Upcoming"></a>Upcoming

- No event

[1.3 - Features](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-3-features)

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[1.5 - Summary](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-5-summary)

# First Steps

We will show you how easy it is to use MINA, running a very simple example provided with the **MINA** package.

The first thing you have to do is to setup your environment when you want to use **MINA** in your application. We will describe what you need to install and how to run a **MINA** program. Nothing fancy, just a first taste of **MINA**…

## Download

First, you have to download the latest **MINA** release from [MINA 2.0 Downloads Section](#mina-apache-org-mina-project-downloads_2_0) or [MINA 2.1 Downloads Section](#mina-apache-org-mina-project-downloads_2_1). Just take the latest version, unless you have very good reasons not to do so…

Generally speaking, if you are going to use **Maven** to build your project, you won’t even have to download anything, as soon as you will depend on a repository which already contains the **MINA** libraries: you just tell your **Maven** poms that you want to use the **MINA** jars you need.

## What’s inside

After the download is complete, extract the content of *tar.gz* or *zip* file to local hard drive. The downloaded compressed file has following contents

On UNIX system, type:

```bash
$ tar xzpf apache-mina-2.0.7-tar.gz
```

In the *apache-mina-2.0.7* directory, you will get:

```text
 |
 +- dist
 +- docs
 +- lib
 +- src
 +- LICENSE.txt
 +- LICENSE.jzlib.txt
 +- LICENSE.ognl.txt
 +- LICENSE.slf4j.txt
 +- LICENSE.springframework.txt
 +- NOTICE.txt
```

## Content Details

- *dist* - Contains jars for the **MINA** library code
- *docs* - Contains API docs and Code xrefs
- *lib* - Contains all needed jars for all the libraries needed for using **MINA**

Additional to these, the base directory has couple of license and notice files

# Running your first MINA program

Well, we have downloaded the release, let’s run our first **MINA** example, shipped with the release.

Put the following jars in the class path

- mina-core-2.0.7.jar
- mina-example-2.0.7.jar
- slf4j-api-1.6.6.jar
- slf4j-log4j12-1.6.6.jar
- log4j-1.2.17.jar

**Logging Tip

- Log4J 1.2 users: slf4j-api.jar, slf4j-log4j12.jar, and Log4J 1.2.x
- Log4J 1.3 users: slf4j-api.jar, slf4j-log4j13.jar, and Log4J 1.3.x
- java.util.logging users: slf4j-api.jar and slf4j-jdk14.jar

**IMPORTANT: **Please make sure you are using the right *slf4j-\*.jar* that matches to your logging framework.
For instance, *slf4j-log4j12.jar* and *log4j-1.3.x.jar* can not be used together, and will malfunction.
If you don’t need a logging framework you can use *slf4j-nop.jar* for no logging or *slf4j-simple.jar* for
very basic logging.******

******On the command prompt, issue the following command:

```bash
$ java org.apache.mina.example.gettingstarted.timeserver.MinaTimeServer
```

This shall start the server. Now telnet and see the program in action

Issue following command to telnet

```text
telnet 127.0.0.1 9123
```

Well, we have run our first **MINA** program. Please try other sample programs shipped with **MINA** as examples.

[1.3 - Features](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-3-features)

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[1.5 - Summary](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-5-summary)******

****© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.****

---

<a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-5-summary"></a>

# 1.5 - Summary — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-5-summary--Navigation-Upcoming"></a>Upcoming

- No event

[1.4 - First Steps](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-4-first-steps)

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

# Summary

In this chapter, we looked at MINA based Application Architecture, for Client as well as Server. We also touched upon the implementation of Sample TCP Server/Client, and UDP Server and Client.

In the chapters to come we shall discuss about MINA Core constructs and advanced topics

[1.4 - First Steps](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-4-first-steps)

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch11-ssl-filter-ch11-ssl-filter"></a>

# Chapter 11 - SSL Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch11-ssl-filter-ch11-ssl-filter--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 10 - Executor Filter](#mina-apache-org-mina-project-userguide-ch10-executor-filter-ch10-executor-filter)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 12 - Logging Filter](#mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter)

# Chapter 11 - SSL Filter

The **SslFilter** is the filter in charge of managing the encryption and decryption of data sent through a secured connection. Whenever you need to establish a secured connection, or to transform an existing connection to make it secure, you have to add the **SslFilter** in your filter chain.

As any session can modify it’s message filter chain at will, it allows for protocols like **startTLS** to be used on an opened connection.

Please note that although the name include **SSL**, **SslFilter** supports **TLS**.

Actually, **TLS** is supposed to have replaced **SSL**, but for historical reason, **SSL** remains widely used.

## Basic usage

If you want your application to support **SSL/TLS**, just add the **SslFilter** in your chain :

```java
...
DefaultIoFilterChainBuilder chain = acceptor.getFilterChain();
SslFilter sslFilter = new SslFilter(sslContext);
chain.addFirst("sslFilter", sslFilter);
...
```

You obviously need a **SslContext** instance too :

```text
SSLContext sslContext;

    try
    {
        // Initialize the SSLContext to work with our key managers.
        sslContext = SSLContext.getInstance( "TLS" );
        sslContext.init( ... ); // Provide the needed KeyManager[], TrustManager[] and SecureRandom instances
    }
    catch ( Exception e )
    {
        // Handle your exception
    }
```

This is up to you to provide the **KeyManager**, **TrustManager** and **SecureRandom** instances.

Be sure to inject the **SslFilter** on the first position in your chain !

We will see later a detailed example on how to create a **SSLContext**.

## A bit of theory

If you want to get a deeper understanding on how it all works, please read the following paragraphs…

### SSL Basics

We are not going to explain how **SSL** works, there are very [good books](http://www.amazon.com/SSL-TLS-Designing-Building-Systems/dp/0201615983) out there. We will just give a quick introduction on how it works and how it is implemented in MINA.

First of all, you have to understand that **SSL/TLS** is a protocol defined in **RFCs** : [TLS 1.0](https://www.rfc-editor.org/rfc/rfc2246.txt), [TLS 1.1](https://www.rfc-editor.org/rfc/rfc4346.txt), [TLS 1.2](https://www.rfc-editor.org/rfc/rfc5246.txt) and [RLS 1.3](https://tools.ietf.org/html/rfc8446).

It was initially developed by **Netscape**, and named **SSL** (from 1.0 to 3.0), before becoming **TLS**. Nowadays, \***SSL 2.0** and **SSL 3.0** have been deprecated and should not be used.

### The SSL/TLS protocol

As it’s a protocol, it requires some dialog between a client and a server. This is all what **SSL/TLS** is about : describing this dialog.

It’s enough to know that any secured exchange is precluded by a negotiation phase, called the **Handshake**, which role is to come to an agreement between the client and the server on what will be the encryption method to use. A basic **SSL/TLS** session will be something that looks like :

![TLS Protocol](mina.apache.org/mina-project/userguide/images/TLS-protocol.png)

As you can see in this picture, it’s a 2 phases protocol : first the handshake, then when completed the client and the server will be able to exchange data that will be encrypted.

There are also other phases, like the **SSL/TLS** closure, or a renegotiation phase.

#### The Handshake

Basically, it’s all about negotiating many elements that are to be used to encrypt/decrypt the data. The details are not so interesting in the context of this document, enough said that many messages are going to be exchanged between the client and the server, and no data can be sent during this phase.

Actually, there are two conditions for the handshake to start :

- The server must be waiting for some handshake message to arrive
- The client must send a **ClientHello** message

We do use the Java **SSLEngine** class to manage the whole **SSL/TLS** protocol. What **MINA** should take care of is the current status of the session is such that it will be able to get and deal with the client **HelloClient** message. When you inject the **SslFilter** in your filter chain, a few things happen :

- A **SslHandler** instance is created (we create one instance per session). This **SslHandler** instance is in charge of the whole processing (handshake and encryption/decryption of forthcoming messages)
- This **SslHandler** creates a **SSLEngine** using the **SslContext** instance that has been attached to the **SslFilter**
- The **SslEngine** instance is configured and initialized
- The **SslHandler** instance is stored into the session
- Unless required specifically, we initiate the Handshake (which has different meanings on client side and on server side : the client will then send the **ClientHello** message, while the server switch to a mode where it waits for some data to be unwrapped). Note that the handshake initialization can be done later on, if needed

We are all set. The next few steps are pure **SSL/TLS** protocol exchange. If the *session.write()* method is called, the message will be enqueued waiting for the handshake to be completed. Any pending message at the time the **SslFilter** is added into the chain will cause the **SSL/TLS** handshake to fail, so be sure that you have a clean place when you want to inject it. We also won’t receive any message that is not a **SSL/TLS** protocol message.

This last point is important if you are to implement **StartTLS** : as it allows your application to switch from a plain text exchange to an encrypted exchange at any time, you have to be sure that there are not pending messages on both side. Obviously, on the client side - the side that initiates **StartTLS** - every pending messages will have been sent before the **StartTLS** message can be sent, but it has to block any other message that are not part of the following handshake, until the handshake is completed. On the server side, once the **StartTLS** message has been received, no message should be written to the remote peer.

As a matter of fact, injecting the **SslFilter** in the chain should block any exchange that are not part of the handshake protocol until the handshake is completed. If you submit a message to be sent and encrypted before the handshake has been completed, the message will not be rejected but queued and will be processed when the handshake has been completed.

Afterward, every message sent will go through the **SslHandler** instance to be encrypted, and every message received will have to be fully decrypted by the **SslHandler** before being available to the next filters.

#### Sending data

Ok, the Handshake has been completed. Your **SslFilter** is ready to process incoming and outgoing messages. Let’s focus on those your session are going to write.

One important thing is that you may write more than one message on the same session (if you have an **Executor** in your chain). The problem is that the **SSLEngine** is not capable of dealing with more than one message at a time. We need to serialize the messages being written out. It’s even worse : you can’t process an incoming message **and** and outgoing message at the same time.

All in all, the **SSL/TLS** processing is like a black box that accept only one input and can’t process anything until it has completed its task. the following schema represent the way it works for outgoing messages.

![Outgoing messages](mina.apache.org/mina-project/userguide/images/TLS-outMessage.png)

It’s not that different for incoming messages, except that we won’t have an **Executor** between the **IoProcessor** and the **SslFilter**. That makes things simpler, except that one critical thing happens : when we process an incoming message, we can’t anymore process outgoing messages. Note that it also works on the other way around : when an outgoing message is being processed, we can’t process an incoming message :

![Incoming message](mina.apache.org/mina-project/userguide/images/TLS-inMessage.png)

What is important here is that the **SslHander** can't process more than one message at a time.

## SSL/TLS in MINA 2

Now, we will dive a bit deeper into **MINA** code. We will cover all the filter operations:

- Management
  - init()
  - destroy()
  - onPreAdd(IoFilterChain, String, NextFilter)
  - onPostAdd(IoFilterChain, String, NextFilter)
  - onPreRemove(IoFilterChain, String, NextFilter)
  - onPostRemove(IoFilterChain, String, NextFilter)
- Session events
  - sessionCreated(NextFilter, IoSession)
  - sessionOpened(NextFilter, IoSession)
  - sessionClosed(NextFilter, IoSession)
  - sessionIdle(NextFilter, IoSession, IdleStatus)
  - exceptionCaught(NextFilter, IoSession, Throwable)
  - filterClose(NextFilter, IoSession)
  - inputClosed(NextFilter, IoSession)
- Messages events
  - messageReceived(NextFilter, IoSession, Object)
  - filterWrite(NextFilter, IoSession, WriteRequest)
  - messageSent(NextFilter, IoSession, WriteRequest)

### Management

Here are the Filter’s management methods :

#### onPreAdd

This is where we create the **SslHandler** instance, and initialize it. We also define the supported ciphers.

The **SslHandler** instance will itself create an instance of **SSLEngine**, and configure it with all the parameters set in the **SslFilter**:

- If this is client or a server side
- When it’s server side, the flag that says we want or require the client authentication
- The list of enabled ciphers
- The list of enabled protocols

When it’s done, the reference to this instance is stored into the Session’s attributes.

#### onPostAdd

This is where we start the handshake if it’s not explicitly postponed. This is all what this method does. All the logic is implemented by the **SslHandler**

#### onPreRemove

Here, we stop the SSL session and we cleanup the session (removing the filter from the session’s chain and the **SslHandler** instance from the session’s attributes). The **Sslhandler** instance si also destroyed after having flushed any event that is not yet processed.

### Session events

Here are the events that are propagated across the filter’s chain and processed by the **SslFilter** :

#### sessionClosed

We just destroy the **SslHandler** instance.

#### exceptionCaught

We have one special task to proceed when the exception is due to a closed session : we have to gather all the messages that were pending to add them to the exception that will be propagated.

#### filterClose

Here, if there is a SSL session started, we need to close it. In any case, we propagate the event into the chain to the next filter.

### Messages events

Last, not least, the three events relative to messages :

#### messageReceived event

This event is received when we read some data from the socket. We have to take care of a few corner cases :

- The handshake has been completed
- The handshake has been started but is not completed
- No handshake has started, and the **SslHandler** is not yet initialized

Those three use cases are listed by order of frequency. Let’s see what is going to happen for each of those use cases.

##### The handshake has been completed

Good ! That means every incoming message is encapsulated in a **SSL/TLS** envelop, and should be decrypted. Now, we are talking about messages, but we actually receive bytes, that may need to be aggregated to form a full message (at least in **TCP**). If a message is fragmented, we will receive many buffers, and we will be able to decrypt it fully when we will receive the last piece. Remember that we are blocked during all the process, which can block the **SslHandler** instance for this session for quite some time…

In any case, every block of data is processed by the **SslHandler**, which delegates to the **SslEngine** the decryption of the bytes it received.

Here is the basic algorithm we have implemented in *messageReceived()* :

```text
get the session's sslHandler

syncrhonized on sshHandler {
    if handshake completed
        then
            get the sslHandler decrypting the data
            if the application buffer is completed, push it into the message to forward to the IoHandler
        else
            enqueue the incoming data
}

flush the messages if any
```

The important part here is that the **SslHandler** will cumulate the data until it has a complete message to push into the chain. This may take a while, and many socket reads. The reason is that the **SSLEngine** cannot process a message unless it has all the bytes needed to decode the message fully.

**Tip** : increase the transmission buffer size to limit the number of round trips necessary to send a big message.

##### The handshake has not been completed

The means the received message is part of the Handshake protocol. Nothing will be propagated to the **IoHandler**, the message will be consumed by the **SslHandler**.

Until the full handshake is completed, every incoming data will be considered as a Handshake protocol message.

At the same time, messages that the **IoHandler** will be enqueued, waiting for the Handshake to be completed.

Here is a schema representing the full process when the data are received in two round-trips :

![Unwrapping message](mina.apache.org/mina-project/userguide/images/TLS-unwrap.png)

#### filterwWrite event

This event is processed when the **IoSession.write()** method is called.

If the SSL session is not started, we simply accumulate the message to write. It will be send later.

There is one tricky parameter that comes into play here, for some very specific need. Typically, when implementing the **startTLS** protocol, where the server is switching from a non secured connection to a secured connection by the mean of an application message (and potentially a response), we need the response to be send back to the client **before** the **SslFilter** is installed (otherwise, the response will be blocked, and the etablishement of a secured connection will simply fail). This is the **DISABLE\_ENCRYPTION\_ONCE** Attribute. It does not matter what it contains (it can be just a boolean), it's enough for this parameter to be present in the session for the first message to bypasse the **SslFilter**.

We control the presence of the **DISABLE\_ENCRYPTION\_ONCE** flag in the session’s attributes, and if present, we remove it from the session, and push the message uncrypted into the messages queue to be send.

Otherwise, if the handshake is not yet completed, we keep the message in a queue, and if it’s completed, we encrypt it and schedule it to be written.

If some message has been scheduled for write, we flush them all.

#### messageSent event

Here, it’s just a matter of getting back the unencrypted message to propagate it to the **IoHandler**

## SSLContext initialisation

We saw that in order to establish a **SSL** session, we need to create a **SSLContext**. Here is the code :

```text
SSLContext sslContext;

try
{
    // Initialize the SSLContext to work with our key managers.
    sslContext = SSLContext.getInstance( "TLS" );
    sslContext.init( ... ); // Provide the needed KeyManager[], TrustManager[] and SecureRandom instances
}
catch ( Exception e )
{
    // Handle your exception
}
```

What we have not exposed here is the constructor and the **init()** method.

The **SSLContext** can either be created explicitly - through its constructor -, or we ask the static factory to return an instance (this is what we have done in the previous code. teh second method is quite straightforward, and would fit most of the time. It’s enough to pass it the name of the protocol to use, which is one of :

- **SSL**
- **SSLv2**
- **SSLv3**
- **TLS**
- **TLSv1**
- **TLSv1.1**
- **TLSv1.2** (not supported in Java 6)

It’s strongly suggested to pick the higher algorithm (ie **TLSv1.2**) if your client supports it.

The **init()** method takes 3 arguments :

- a **KeyManager** (can be null)
- a **TrustManager** (can be null)
- a random generator (can be null)

If the parameters are set to null, the installed security provider will pick the highest priority implementation.

[Chapter 10 - Executor Filter](#mina-apache-org-mina-project-userguide-ch10-executor-filter-ch10-executor-filter)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 12 - Logging Filter](#mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch13-debugging-ch13-debugging"></a>

# Chapter 13 - Debugging — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch13-debugging-ch13-debugging--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 12 - Logging Filter](#mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 14 - State Machine](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine)

# Chapter 13 - Debugging

To be completed…

[Chapter 12 - Logging Filter](#mina-apache-org-mina-project-userguide-ch12-logging-filter-ch12-logging-filter)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 14 - State Machine](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch15-proxy-ch15-proxy"></a>

# Chapter 15 - Proxy — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch15-proxy-ch15-proxy--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 14 - State Machine](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 16 - JMX support](#mina-apache-org-mina-project-userguide-ch16-jmx-support-ch16-jmx-support)

# Chapter 15 - Proxy

To be completed…

[Chapter 14 - State Machine](#mina-apache-org-mina-project-userguide-ch14-state-machine-ch14-state-machine)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 16 - JMX support](#mina-apache-org-mina-project-userguide-ch16-jmx-support-ch16-jmx-support)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics"></a>

# Chapter 2 - Basics — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

# Chapter 2 - Basics

In Chapter 1, we had a brief glimpse of Apache MINA. In this chapter we shall have a look at Client/Server Architecture and details on working out a MINA based Server and Client.

We will also expose some very simple Servers and Clients, based on TCP and UDP.

- [2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)
  - [2.1.1 - Server Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-1-server-architecture)
  - [2.1.2 - Client Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-2-client-architecture)
- [2.3 - Sample TCP Client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-3-sample-tcp-client)
- [2.4 - Sample UDP Server](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-4-sample-udp-server)
- [2.5 - Sample UDP Client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-5-sample-udp-client)
- [2.6 - Summary](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-6-summary)

[Chapter 1 - Getting Started](#mina-apache-org-mina-project-userguide-ch1-getting-started-ch1-getting-started)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture"></a>

# 2.1 - Application Architecture — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[2.2 - Sample TCP Server](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-2-sample-tcp-server)

- [2.1.1 - Server Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-1-server-architecture)
- [2.1.2 - Client Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-2-client-architecture)

# 2.1 - MINA based Application Architecture

It’s the question most asked: ‘How does a **MINA** based application look like’? In this article lets see what’s the architecture of MINA based application. Have tried to gather the information from presentations based on **MINA**.

A Bird’s Eye View:

![](mina.apache.org/assets/img/mina/apparch_small.png)

Here, we can see that **MINA** is the glue between your application (be it a client or a server) and the underlying network layer, which can be based on TCP, UDP, in-VM communication or even a RS-232C serial protocol for a client.

You just have to design your application on top of MINA without having to handle all the complexity of the network layer.

Lets take a deeper dive into the details now. The following image shows a bit more the internal of **MINA**, and what are each of the **MINA** components doing:

![](mina.apache.org/assets/img/mina/mina_app_arch.png)

(The image is from Emmanuel Lécharny presentation [MINA in real life (ApacheCon EU 2009)](/assets/pdfs/Mina_in_real_life_ASEU-2009.pdf))

Broadly, MINA based applications are divided into 3 layers

- I/O Service - Performs actual I/O
- I/O Filter Chain - Filters/Transforms bytes into desired Data Structures and vice-versa
- I/O Handler - Here resides the actual business logic

So, in order to create a MINA based Application, you have to:

1. Create an I/O service - Choose from already available Services (*Acceptor*) or create your own
2. Create a Filter Chain - Choose from already existing Filters or create a custom Filter for transforming request/response
3. Create an I/O Handler - Write business logic, on handling different messages

This is pretty much it.

You can get a bit deeper by reading those two pages:

- [2.1.1 - Server Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-1-server-architecture)
- [2.1.2 - Client Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-2-client-architecture)

Of course, **MINA** offers more than just that, and you will probably have to take care of many other aspects, like the messages encoding/decoding, the network configuration how to scale up, etc… We will have a further look at those aspects in the next chapters.

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[2.2 - Sample TCP Server](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-2-sample-tcp-server)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-1-server-architecture"></a>

# 2.1.1 - Server Architecture — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-1-server-architecture--Navigation-Upcoming"></a>Upcoming

- No event

[2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)

[2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)

[2.1.2 - Client Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-2-client-architecture)

# 2.1.1 - Server Architecture

We have exposed the **MINA** Application Architecture in the previous section. Let’s now focus on the Server Architecture. Basically, a Server listens on a port for incoming requests, process them and send replies. It also creates and handles a session for each client (whenever we have a TCP or UDP based protocol), this will be explain more extensively in the [chapter 4](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session).

![](mina.apache.org/assets/img/mina/Server_arch.png)

- IOAcceptor listens on the network for incoming connections/packets
- For a new connection, a new session is created and all subsequent request from IP Address/Port combination are handled in that Session
- All packets received for a Session, traverses the Filter Chain as specified in the diagram. Filters can be used to modify the content of packets (like converting to Objects, adding/removing information etc). For converting to/from raw bytes to High Level Objects, PacketEncoder/Decoder are particularly useful
- Finally the packet or converted object lands in `IOHandler`. `IOHandler`s can be used to fulfill business needs.

## Session creation

Whenever a client connects on a MINA server, we will create a new session to store persistent data into it. Even if the protocol is not connected, this session will be created. The following schema shows how **MINA** handles incoming connections:

![Incoming connections handling](mina.apache.org/assets/img/mina/incoming-connections.png)

## Incoming messages processing

We will now explain how **MINA** processes incoming messages.

Assuming that a session has been created, any new incoming message will result in a selector being waken up

[2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)

[2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)

[2.1.2 - Client Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-2-client-architecture)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-2-client-architecture"></a>

# 2.1.2 - Client Architecture — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-2-client-architecture--Navigation-Upcoming"></a>Upcoming

- No event

[2.1.1 - Server Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-1-server-architecture)

[2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)

[2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)

# 2.1.2 - Client Architecture

We had a brief look at MINA based Server Architecture, lets see how Client looks like. Clients need to connect to a Server, send message and process the responses.

![diagram](mina.apache.org/assets/img/mina/clientdiagram.png)

- Client first creates an IOConnector (MINA Construct for connecting to Socket), initiates a bind with Server
- Upon Connection creation, a Session is created and is associated with Connection
- Application/Client writes to the Session, resulting in data being sent to Server, after traversing the Filter Chain
- All the responses/messages received from Server are traverses the Filter Chain and lands at IOHandler, for processing

[2.1.1 - Server Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-1-server-architecture)

[2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)

[2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-2-sample-tcp-server"></a>

# 2.2 - Sample TCP Server — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-2-sample-tcp-server--Navigation-Upcoming"></a>Upcoming

- No event

[2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[2.3 - Sample TCP Client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-3-sample-tcp-client)

# 2.2 - Sample TCP Server

This tutorial will walk you through the process of building a MINA based program. This tutorial will walk through building a time server. The following prerequisites are required for this tutorial:

- MINA 2.x Core
- JDK 1.5 or greater
- SLF4J 1.3.0 or greater
  - **Log4J 1.2** users: slf4j-api.jar, slf4j-log4j12.jar, and [Log4J](http://logging.apache.org/log4j/1.2/) 1.2.x
  - **Log4J 1.3** users: slf4j-api.jar, slf4j-log4j13.jar, and [Log4J](http://logging.apache.org/log4j/1.2/) 1.3.x
  - **java.util.logging** users: slf4j-api.jar and slf4j-jdk14.jar
  - **IMPORTANT**: Please make sure you are using the right slf4j-\*.jar that matches to your logging framework.

For instance, slf4j-log4j12.jar and log4j-1.3.x.jar can not be used together, and will malfunction.

We have tested this program on both Windows© 2000 professional and linux. If you have any problems getting this program to work, please do not hesitate to [contact us](../../../contact.html) in order to talk to the MINA developers. Also, this tutorial has tried to remain independent of development environments (IDE, editors..etc). This tutorial will work with any environment that you are comfortable with. Compilation commands and steps to execute the program have been removed for brevity. If you need help learning how to either compile or execute java programs, please consult the [Java tutorial](http://java.sun.com/docs/books/tutorial/).

## Writing the MINA time server

We will begin by creating a file called MinaTimeServer.java. The initial code can be found below:

```java
public class MinaTimeServer {
    public static void main(String[] args) {
        // code will go here next
    }
}
```

This code should be straightforward to all. We are simply defining a main method that will be used to kick off the program. At this point, we will begin to add the code that will make up our server. First off, we need an object that will be used to listen for incoming connections. Since this program will be TCP/IP based, we will add a SocketAcceptor to our program.

```java
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class MinaTimeServer
{
    public static void main( String[] args )
    {
        IoAcceptor acceptor = new NioSocketAcceptor();
    }
}
```

With the NioSocketAcceptor class in place, we can go ahead and define the handler class and bind the NioSocketAcceptor to a port:

```java
import java.net.InetSocketAddress;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class MinaTimeServer
{
    private static final int PORT = 9123;
    public static void main( String[] args ) throws IOException
    {
        IoAcceptor acceptor = new NioSocketAcceptor();
        acceptor.bind( new InetSocketAddress(PORT) );
    }
}
```

As you see, there is a call to acceptor.setLocalAddress( new InetSocketAddress(PORT) );. This method defines what host and port this server will listen on. The final method is a call to IoAcceptor.bind(). This method will bind to the specified port and start processing of remote clients.

Next we add a filter to the configuration. This filter will log all information such as newly created sessions, messages received, messages sent, session closed. The next filter is a ProtocolCodecFilter. This filter will translate binary or protocol specific data into message object and vice versa. We use an existing TextLine factory because it will handle text base message for you (you don’t have to write the codec part)

```java
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class MinaTimeServer
{
    public static void main( String[] args )
    {
        IoAcceptor acceptor = new NioSocketAcceptor();
        acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new TextLineCodecFactory( Charset.forName( "UTF-8" ))));
        acceptor.bind( new InetSocketAddress(PORT) );
    }
}
```

At this point, we will define the handler that will be used to service client connections and the requests for the current time. The handler class is a class that must implement the interface IoHandler. For almost all programs that use MINA, this becomes the workhorse of the program, as it services all incoming requests from the clients. For this tutorial, we will extend the class IoHandlerAdapter. This is a class that follows the [adapter design pattern](http://en.wikipedia.org/wiki/Adapter_pattern) which simplifies the amount of code that needs to be written in order to satisfy the requirement of passing in a class that implements the IoHandler interface.

```java
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class MinaTimeServer
{
    public static void main( String[] args ) throws IOException
    {
        IoAcceptor acceptor = new NioSocketAcceptor();
        acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new TextLineCodecFactory( Charset.forName( "UTF-8" ))));
        acceptor.setHandler(  new TimeServerHandler() );
        acceptor.bind( new InetSocketAddress(PORT) );
    }
}
```

We will now add in the NioSocketAcceptor configuration. This will allow us to make socket-specific settings for the socket that will be used to accept connections from clients.

```java
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class MinaTimeServer
{
    public static void main( String[] args ) throws IOException
    {
        IoAcceptor acceptor = new NioSocketAcceptor();
        acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new TextLineCodecFactory( Charset.forName( "UTF-8" ))));
        acceptor.setHandler(  new TimeServerHandler() );
        acceptor.getSessionConfig().setReadBufferSize( 2048 );
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );
        acceptor.bind( new InetSocketAddress(PORT) );
    }
}
```

There are 2 new lines in the MinaTimeServer class. These methods set the set the IoHandler, input buffer size and the idle property for the sessions. The buffer size will be specified in order to tell the underlying operating system how much room to allocate for incoming data. The second line will specify when to check for idle sessions. In the call to setIdleTime, the first parameter defines what actions to check for when determining if a session is idle, the second parameter defines the length of time in seconds that must occur before a session is deemed to be idle.

The code for the handler is shown below:

```java
import java.util.Date;

import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;

public class TimeServerHandler extends IoHandlerAdapter
{
    @Override
    public void exceptionCaught( IoSession session, Throwable cause ) throws Exception
    {
        cause.printStackTrace();
    }
    @Override
    public void messageReceived( IoSession session, Object message ) throws Exception
    {
        String str = message.toString();
        if( str.trim().equalsIgnoreCase("quit") ) {
            session.close();
            return;
        }
        Date date = new Date();
        session.write( date.toString() );
        System.out.println("Message written...");
    }
    @Override
    public void sessionIdle( IoSession session, IdleStatus status ) throws Exception
    {
        System.out.println( "IDLE " + session.getIdleCount( status ));
    }
}
```

The methods used in this class are exceptionCaught, messageReceived and sessionIdle. exceptionCaught should always be defined in a handler to process and exceptions that are raised in the normal course of handling remote connections. If this method is not defined, exceptions may not get properly reported.

The exceptionCaught method will simply print the stack trace of the error and close the session. For most programs, this will be standard practice unless the handler can recover from the exception condition.

The messageReceived method will receive the data from the client and write back to the client the current time. If the message received from the client is the word “quit”, then the session will be closed. This method will also print out the current time to the client. Depending on the protocol codec that you use, the object (second parameter) that gets passed in to this method will be different, as well as the object that you pass in to the session.write(Object) method. If you do not specify a protocol codec, you will most likely receive a IoBuffer object, and be required to write out a IoBuffer object.

The sessionIdle method will be called once a session has remained idle for the amount of time specified in the call acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH\_IDLE, 10 );.

All that is left to do is define the socket address that the server will listen on, and actually make the call that will start the server. That code is shown below:

```java
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;

import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.filter.logging.LoggingFilter;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;

public class MinaTimeServer
{
    private static final int PORT = 9123;
    public static void main( String[] args ) throws IOException
    {
        IoAcceptor acceptor = new NioSocketAcceptor();
        acceptor.getFilterChain().addLast( "logger", new LoggingFilter() );
        acceptor.getFilterChain().addLast( "codec", new ProtocolCodecFilter( new TextLineCodecFactory( Charset.forName( "UTF-8" ))));
        acceptor.setHandler( new TimeServerHandler() );
        acceptor.getSessionConfig().setReadBufferSize( 2048 );
        acceptor.getSessionConfig().setIdleTime( IdleStatus.BOTH_IDLE, 10 );
        acceptor.bind( new InetSocketAddress(PORT) );
    }
}
```

## Try out the Time server

At this point, we can go ahead and compile the program. Once you have compiled the program you can run the program in order to test out what happens. The easiest way to test the program is to start the program, and then telnet in to the program:

| Client Output | Server Output |
| --- | --- |
| user@myhost:~> telnet 127.0.0.1 9123Trying 127.0.0.1…Connected to 127.0.0.1.Escape character is ‘^]'.helloWed Oct 17 23:23:36 EDT 2007quitConnection closed by foreign host.user@myhost:~> | MINA Time server started.Message written… |

## What’s Next?

Please visit our Documentation page to find out more resources. You can also keep reading other tutorials.

[2.1 - Application Architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[2.3 - Sample TCP Client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-3-sample-tcp-client)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-3-sample-tcp-client"></a>

# 2.3 - Sample TCP Client — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-3-sample-tcp-client--Navigation-Upcoming"></a>Upcoming

- No event

[2.2 - Sample TCP Server](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-2-sample-tcp-server)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[2.4 - Sample UDP Server](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-4-sample-udp-server)

# 2.3 - Sample TCP Client

We have seen the Client Architecture. Lets explore a sample Client implementation.

We shall use [Sumup Client](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/example/sumup/Client.html) as a reference implementation.

We will remove boiler plate code and concentrate on the important constructs. Below the code for the Client :

```java
public static void main(String[] args) throws Throwable {
    NioSocketConnector connector = new NioSocketConnector();
    connector.setConnectTimeoutMillis(CONNECT_TIMEOUT);

    if (USE_CUSTOM_CODEC) {
       connector.getFilterChain().addLast("codec",
        new ProtocolCodecFilter(new SumUpProtocolCodecFactory(false)));
    } else {
        connector.getFilterChain().addLast("codec",
            new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
    }
    
    connector.getFilterChain().addLast("logger", new LoggingFilter());
    connector.setHandler(new ClientSessionHandler(values));
    IoSession session;
    
    for (;;) {
        try {
            ConnectFuture future = connector.connect(new InetSocketAddress(HOSTNAME, PORT));
            future.awaitUninterruptibly();
            session = future.getSession();
            break;
        } catch (RuntimeIoException e) {
            System.err.println("Failed to connect.");
            e.printStackTrace();
            Thread.sleep(5000);
        }
    }
        
    // wait until the summation is done
    session.getCloseFuture().awaitUninterruptibly();
    connector.dispose();
}
```

To construct a Client, we need to do following

- Create a Connector
- Create a Filter Chain
- Create a IOHandler and add to Connector
- Bind to Server

Lets examine each one in detail

## Create a Connector

```java
NioSocketConnector connector = new NioSocketConnector();
```

Here we have created a NIO Socket connector

## Create a Filter Chain

```java
if (USE_CUSTOM_CODEC) {
    connector.getFilterChain().addLast("codec",
        new ProtocolCodecFilter(new SumUpProtocolCodecFactory(false)));
} else {
    connector.getFilterChain().addLast("codec",
        new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
}
```

We add Filters to the Filter Chain for the Connector. Here we have added a ProtocolCodec, to the filter Chain.

## Create IOHandler

```java
connector.setHandler(new ClientSessionHandler(values));
```

Here we create an instance of [ClientSessionHandler](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/example/sumup/ClientSessionHandler.html) and set it as a handler for the Connector.

## Bind to Server

```java
IoSession session;

for (;;) {
    try {
        ConnectFuture future = connector.connect(new InetSocketAddress(HOSTNAME, PORT));
        future.awaitUninterruptibly();
        session = future.getSession();
        break;
    } catch (RuntimeIoException e) {
        System.err.println("Failed to connect.");
        e.printStackTrace();
        Thread.sleep(5000);
    }
}
```

Here is the most important stuff. We connect to remote Server. Since, connect is an async task, we use the [ConnectFuture](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/core/future/ConnectFuture.html) class to know the when the connection is complete.
Once the connection is complete, we get the associated [IoSession](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/core/session/IoSession.html). To send any message to the Server, we shall have to write to the session. All responses/messages from server shall traverse the Filter chain and finally be handled in IoHandler.

[2.2 - Sample TCP Server](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-2-sample-tcp-server)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[2.4 - Sample UDP Server](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-4-sample-udp-server)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-4-sample-udp-server"></a>

# 2.4 - Sample UDP Server — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-4-sample-udp-server--Navigation-Upcoming"></a>Upcoming

- No event

[2.3 - Sample TCP-Client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-3-sample-tcp-client)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[2.5 - Sample UDP Client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-5-sample-udp-client)

# 2.4 - Sample UDP Server

We will begin by looking at the code found in the [org.apache.mina.example.udp](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/example/udp/package-summary.html) package. To keep life simple, we shall concentrate on MINA related constructs only.

To construct the server, we shall have to do the following:

1. Create a Datagram Socket to listen for incoming Client requests (See [MemoryMonitor.java](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/example/udp/MemoryMonitor.html))
2. Create an IoHandler to handle the MINA framework generated events (See [MemoryMonitorHandler.java](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/example/udp/MemoryMonitorHandler.html))

Here is the first snippet that addresses Point# 1:

```java
NioDatagramAcceptor acceptor = new NioDatagramAcceptor();
acceptor.setHandler(new MemoryMonitorHandler(this));
```

Here, we create a NioDatagramAcceptor to listen for incoming Client requests, and set the IoHandler.The variable ‘PORT’ is just an int. The next step is to add a logging filter to the filter chain that this DatagramAcceptor will use. LoggingFilter is a very nice way to see MINA in Action. It generate log statements at various stages, providing an insight into how MINA works.

```java
DefaultIoFilterChainBuilder chain = acceptor.getFilterChain();
chain.addLast("logger", new LoggingFilter());
```

Next we get into some more specific code for the UDP traffic. We will set the acceptor to reuse the address

```java
DatagramSessionConfig dcfg = acceptor.getSessionConfig();
dcfg.setReuseAddress(true);acceptor.bind(new InetSocketAddress(PORT));
```

Of course the last thing that is required here is to call bind().

## IoHandler implementation

There are three major events of interest for our Server Implementation

- Session Created
- Message Received
- Session Closed

Lets look at each of them in detail

### Session Created Event

```java
@Override
public void sessionCreated(IoSession session) throws Exception {
    SocketAddress remoteAddress = session.getRemoteAddress();
    server.addClient(remoteAddress);
} 
```

In the session creation event, we just call addClient() function, which internally adds a Tab to the UI

### Message Received Event

```java
@Override
public void messageReceived(IoSession session, Object message) throws Exception {
    if (message instanceof IoBuffer) {
        IoBuffer buffer = (IoBuffer) message;
        SocketAddress remoteAddress = session.getRemoteAddress();
        server.recvUpdate(remoteAddress, buffer.getLong());
    }
}
```

In the message received event, we just dump the data received in the message. Applications that need to send responses, can process message and write the responses onto session in this function.

### Session Closed Event

```java
@Override
public void sessionClosed(IoSession session) throws Exception {
    System.out.println("Session closed...");
    SocketAddress remoteAddress = session.getRemoteAddress();
    server.removeClient(remoteAddress);
}
```

In the Session Closed, event we just remove the Client tab from the UI

[2.3 - Sample TCP-Client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-3-sample-tcp-client)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[2.5 - Sample UDP Client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-5-sample-udp-client)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-5-sample-udp-client"></a>

# 2.5 - Sample UDP Client — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-5-sample-udp-client--Navigation-Upcoming"></a>Upcoming

- No event

[2.4 - Sample UDP Server](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-4-sample-udp-server)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[2.6 - Summary](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-6-summary)

# 2.5 -Sample UDP Client

Lets look at the client code for the UDP Server from previous section.

To implement the Client we need to do following:

- Create Socket and Connect to Server
- Set the IoHandler
- Collect free memory
- Send the Data to the Server

We will begin by looking at the file [MemMonClient.java](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/example/udp/client/MemMonClient.html), found in the org.apache.mina.example.udp.client java package. The first few lines of the code are simple and straightforward.

```java
connector = new NioDatagramConnector();
connector.setHandler( this );
ConnectFuture connFuture = connector.connect( new InetSocketAddress("localhost", MemoryMonitor.PORT ));
```

Here we create a NioDatagramConnector, set the handler and connect to the server. One gotcha I ran into was that you must set the host in the InetSocketAddress object or else nothing seems to work. This example was mostly written and tested on a Windows XP machine, so things may be different elsewhere. Next we will wait for acknowledgment that the client has connected to the server. Once we know we are connected, we can start writing data to the server. Here is that code:

```java
connFuture.addListener( new IoFutureListener(){
            public void operationComplete(IoFuture future) {
                ConnectFuture connFuture = (ConnectFuture)future;
                if( connFuture.isConnected() ){
                    session = future.getSession();
                    try {
                        sendData();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    log.error("Not connected...exiting");
                }
            }
        });
```

Here we add a listener to the ConnectFuture object and when we receive a callback that the client has connected, we will start to write data. The writing of data to the server will be handled by a method called sendData. This method is shown below:

```java
private void sendData() throws InterruptedException {
    for (int i = 0; i < 30; i++) {
        long free = Runtime.getRuntime().freeMemory();
        IoBuffer buffer = IoBuffer.allocate(8);
        buffer.putLong(free);
        buffer.flip();
        session.write(buffer);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw new InterruptedException(e.getMessage());
        }
    }
}
```

This method will write the amount of free memory to the server once a second for 30 seconds. Here you can see that we allocate a IoBuffer large enough to hold a long variable and then place the amount of free memory in the buffer. This buffer is then flipped and written to the server.

Our UDP Client implementation is complete.

[2.4 - Sample UDP Server](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-4-sample-udp-server)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[2.6 - Summary](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-6-summary)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-6-summary"></a>

# 2.6 - Summary — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch2-basics-ch2-6-summary--Navigation-Upcoming"></a>Upcoming

- No event

[2.5 - Sample UDP client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-5-sample-udp-client)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

# 2.6 - Summary

In this chapter, we looked at MINA based Application Architecture, for Client as well as Server. We also touched upon the implementation of Sample TCP Server/Client, and UDP Server and Client.

In the chapters to come we shall discuss about MINA Core constructs and advanced topics

[2.5 - Sample UDP client](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-5-sample-udp-client)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch3-service-ch3-service"></a>

# Chapter 3 - Service — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch3-service-ch3-service--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

# Chapter 3 - IoService

A **MINA** *IoService* - as seen in the [application architecture](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-1-application-architecture) chapter, is the base class supporting all the **IO** services, either from the server side or the client side.

It will handle all the interaction with your application, and with the remote peer, send and receive messages, manage sessions, connections, etc.

It’s an interface, which is implemented as an *IoAcceptor* for the server side, and *IoConnector* for the client side.

We will expose the interface in those chapters :

- [3.1 - IoService Introduction](#mina-apache-org-mina-project-userguide-ch3-service-ch3-1-io-service)
- [3.2 - IoService Details](#mina-apache-org-mina-project-userguide-ch3-service-ch3-2-io-service-details)
- [3.3 - IoAcceptor](#mina-apache-org-mina-project-userguide-ch3-service-ch3-3-acceptor)
- [3.4 - IoConnector](#mina-apache-org-mina-project-userguide-ch3-service-ch3-4-connector)

[Chapter 2 - Basics](#mina-apache-org-mina-project-userguide-ch2-basics-ch2-basics)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch3-service-ch3-1-io-service"></a>

# 3.1 - IO Service Introduction — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch3-service-ch3-1-io-service--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[3.2 - IoService Details](#mina-apache-org-mina-project-userguide-ch3-service-ch3-2-io-service-details)

# 3.1 - IoService Introduction

[IoService](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/core/service/IoService.html) provides basic **I/O** Service and manages **I/O** Sessions within **MINA**. Its one of the most crucial part of **MINA** Architecture. The implementing classes of *IoService* and child interface, are where most of the low level **I/O** operations are handled.

# IoService Mind Map

Let’s try to see what are the responsibilities of the *IoService* and it implementing class [AbstractIoService](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/core/service/AbstractIoService.html). Let’s take a slightly different approach of first using a [Mind Map](http://en.wikipedia.org/wiki/Mind_map) and then jump into the inner working. The Mind Map was created using [XMind](http://www.xmind.net/).

![](mina.apache.org/assets/img/mina/IoService_mindmap.png)

## Responsibilities

As seen in the previous graphic, The *IoService* has many responsibilities :

- sessions management : Creates and deletes sessions, detect idleness.
- filter chain management : Handles the filter chain, allowing the user to change the chain on the fly
- handler invocation : Calls the handler when some new message is received, etc
- statistics management : Updates the number of messages sent, bytes sent, and many others
- listeners management : Manages the Listeners a suer can set up
- communication management : Handles the transmission of data, in both side

All those aspects will be described in the following chapters.

## Interface Details

*IoService* is the base interface for all the *IoConnector*‘s and *IoAcceptor*‘s that provides **I/O** services and manages **I/O** sessions. The interface has all the functions need to perform **I/O** related operations.

Lets take a deep dive into the various methods in the interface

- getTransportMetadata()
- addListener()
- removeListener()
- isDisposing()
- isDisposed()
- dispose()
- getHandler()
- setHandler()
- getManagedSessions()
- getManagedSessionCount()
- getSessionConfig()
- getFilterChainBuilder()
- setFilterChainBuilder()
- getFilterChain()
- isActive()
- getActivationTime()
- broadcast()
- setSessionDataStructureFactory()
- getScheduledWriteBytes()
- getScheduledWriteMessages()
- getStatistics()

### getTransportMetadata()

This method returns the Transport meta-data the *IoAcceptor* or *IoConnector* is running. The typical details include provider name (nio, apr, rxtx), connection type (connectionless/connection oriented) etc.

### addListener

Allows to add a *IoServiceListener* to listen to specific events related to *IoService*.

### removeListener

Removes specified *IoServiceListener* attached to this *IoService*.

### isDisposing

This method tells if the service is currently being disposed. As it can take a while, it’s useful to know the current status of the service.

### isDisposed

This method tells if the service has been disposed. A service will be considered as disposed only when all the resources it has allocated have been released.

### dispose

This method releases all the resources the service has allocated. As it may take a while, the user should check the service status using the *isDisposing()* and *isDisposed()* to know if the service is now disposed completely.

Always call *dispose()* when you shutdown a service !

### getHandler

Returns the *IoHandler* associated with the service.

### setHandler

Sets the *IoHandler* that will be responsible for handling all the events for the service. The handler contains your application logic !

### getManagedSessions

Returns the map of all sessions which are currently managed by this service. A managed session is a session which is added to the service listener. It will be used to process the idle sessions, and other session aspects, depending on the kind of listeners a user adds to a service.

### getManagedSessionCount

Returns the number of all sessions which are currently managed by this service.

### getSessionConfig

Returns the session configuration.

### getFilterChainBuilder

Returns the *Filter* chain builder. This is useful if one wants to add some new filter that will be injected when the sessions will be created.

### setFilterChainBuilder

Defines the *Filter* chain builder to use with the service.

### getFilterChain

Returns the current default *Filter* chain for the service.

### isActive

Tells if the service is active or not.

### getActivationTime

Returns the time when this service was activated. It returns the last time when this service was activated if the service is not anymore active.

### broadcast

Writes the given message to all the managed sessions.

### setSessionDataStructureFactory

Sets the *IoSessionDataStructureFactory* that provides related data structures for a new session created by this service.

### getScheduledWriteBytes

Returns the number of bytes scheduled to be written (ie, the bytes stored in memory waiting for the socket to be ready for write).

### getScheduledWriteMessages

Returns the number of messages scheduled to be written (ie, the messages stored in memory waiting for the socket to be ready for write).

### getStatistics

Returns the *IoServiceStatistics* object for this service.

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[3.2 - IoService Details](#mina-apache-org-mina-project-userguide-ch3-service-ch3-2-io-service-details)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch3-service-ch3-2-io-service-details"></a>

# 3.2 - IoService Details — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch3-service-ch3-2-io-service-details--Navigation-Upcoming"></a>Upcoming

- No event

[3.1 - IoService Introduction](#mina-apache-org-mina-project-userguide-ch3-service-ch3-1-io-service)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[3.3 - Acceptor](#mina-apache-org-mina-project-userguide-ch3-service-ch3-3-acceptor)

# 3.2 - IoService Details

*IoService* is an interface that is implemented by the two most important classes in **MINA** :

- IoAcceptor
- IoConnector

In order to build a server, you need to select an implementation of the *IoAcceptor* interface. For client applications, you need to implement an implementation of the *IoConnector* interface.

## IoAcceptor

Basically, this interface is named because of the *accept()* method, responsible for the creation of new connections between a client and the server. The server accepts incoming connection requests.

At some point, we could have named this interface ‘Server’ (and this is the new name in the coming **MINA 3.0**).

As we may deal with more than one kind of transport (TCP/UDP/…), we have more than one implementation for this interface. It would be very unlikely that you need to implement a new one.

We have many of those implementing classes

- **NioSocketAcceptor** : the non-blocking Socket transport *IoAcceptor*
- **NioDatagramAcceptor** : the non-blocking UDP transport *IoAcceptor*
- **AprSocketAcceptor** : the blocking Socket transport *IoAcceptor*, based on APR
- **VmPipeSocketAcceptor** : the in-VM *IoAcceptor*

Just pick the one that fit your need.

Here is the class diagram for the *IoAcceptor* interfaces and classes :

![](mina.apache.org/assets/img/mina/IoServiceAcceptor.png)

## IoConnector

As we have to use an *IoAcceptor* for servers, you have to implement the *IoConnector* for clients. Again, we have many implementation classes :

- **NioSocketConnector** : the non-blocking Socket transport *IoConnector*
- **NioDatagramConnector** : the non-blocking UDP transport *IoConnector*
- **AprSocketConnector** : the blocking Socket transport *IoConnector*, based on APR
- **ProxyConnector** : a *IoConnector* providing proxy support
- **SerialConnector** : a *IoConnector* for a serial transport
- **VmPipeConnector** : the in-VM *IoConnector*

Just pick the one that fit your need.

Here is the class diagram for the *IoConnector* interfaces and classes :

![](mina.apache.org/assets/img/mina/IoServiceConnector.png)

[3.1 - IoService Introduction](#mina-apache-org-mina-project-userguide-ch3-service-ch3-1-io-service)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[3.3 - Acceptor](#mina-apache-org-mina-project-userguide-ch3-service-ch3-3-acceptor)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch3-service-ch3-3-acceptor"></a>

# 3.3 - Acceptor — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch3-service-ch3-3-acceptor--Navigation-Upcoming"></a>Upcoming

- No event

[3.2 - IoService Details](#mina-apache-org-mina-project-userguide-ch3-service-ch3-2-io-service-details)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[3.4 - Connector](#mina-apache-org-mina-project-userguide-ch3-service-ch3-4-connector)

# 3.3 - Acceptor

In order to build a server, you need to select an implementation of the *IoAcceptor* interface.

## IoAcceptor

Basically, this interface is named because of the *accept()* method, responsible for the creation of new connections between a client and the server. The server accepts incoming connections request.

At some point, we could have named this interface ‘Server’.

As we may deal with more than one kind of transport (TCP/UDP/…), we have more than one implementation for this interface. It would be very unlikely that you need to implement a new one.

We have many of those implementing classes

- **NioSocketAcceptor**: the non-blocking Socket transport *IoAcceptor*
- **NioDatagramAcceptor**: the non-blocking UDP transport *IoAcceptor*
- **AprSocketAcceptor**: the blocking Socket transport *IoAcceptor*, based on APR
- **VmPipeSocketAcceptor**: the in-VM *IoAcceptor*

Just pick the one that fit your need.

Here is the class diagram for the *IoAcceptor* interfaces and classes:

![](mina.apache.org/assets/img/mina/IoServiceAcceptor.png)

## Creation

You first have to select the type of *IoAcceptor* you want to instantiate. This is a choice you will made early in the process, as it all boils down to which network protocol you will use. Let’s see with an example how it works:

```java
public TcpServer() throws IOException {
    // Create a TCP acceptor
    IoAcceptor acceptor = new NioSocketAcceptor();

    // Associate the acceptor to an IoHandler instance (your application)
    acceptor.setHandler(this);

    // Bind : this will start the server...
    acceptor.bind(new InetSocketAddress(PORT));

    System.out.println("Server started...");
}
```

That’s it ! You have created a TCP server. If you want to start an UDP server, simply replace the first line of code:

```java
...
// Create an UDP acceptor
IoAcceptor acceptor = new NioDatagramAcceptor();
...
```

## Disposal

The service can be stopped by calling the *dispose()* method. The service will be stopped only when all the pending sessions have been processed :

```java
// Stop the service, waiting for the pending sessions to be inactive
acceptor.dispose();
```

You can also wait for every thread being executed to be properly completed by passing a boolean parameter to this method:

```java
// Stop the service, waiting for the processing session to be properly completed
acceptor.dispose( true );
```

## Status

You can get the *IoService* status by calling one of the following methods:

- *isActive()*: true if the service can accept incoming requests
- *isDisposing()*: true if the *dispose()* method has been called. It does not tell if the service is actually stopped (some sessions might be processed)
- *isDisposed()*: true if the *dispose(boolean)* method has been called, and the executing threads have been completed.

## Managing the IoHandler

You can add or get the associated *IoHandler* when the service has been instantiated. You just have to call the *setHandler(IoHandler)* or *getHandler()* methods.

## Managing the Filters chain

if you want to manage the filters chain, you will have to call the *getFilterChain()* method. Here is an example:

```java
// Add a logger filter
DefaultIoFilterChainBuilder chain = acceptor.getFilterChain();
chain.addLast("logger", new LoggingFilter());
```

You can also create the chain before and set it into the service:

```java
// Add a logger filter
DefaultIoFilterChainBuilder chain = new DefaultIoFilterChainBuilder();
chain.addLast("logger", new LoggingFilter());

// And inject the created chain builder in the service
acceptor.setFilterChainBuilder(chain);
```

[3.2 - IoService Details](#mina-apache-org-mina-project-userguide-ch3-service-ch3-2-io-service-details)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[3.4 - Connector](#mina-apache-org-mina-project-userguide-ch3-service-ch3-4-connector)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch3-service-ch3-4-connector"></a>

# 3.4 - Connector — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch3-service-ch3-4-connector--Navigation-Upcoming"></a>Upcoming

- No event

[3.3 - Acceptor](#mina-apache-org-mina-project-userguide-ch3-service-ch3-3-acceptor)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

# 3.4 - Connector

For client applications, you need to implement an implementation of the IoConnector interface.

## IoConnector

As we have to use an IoAcceptor for servers, you have to implement the IoConnector. Again, we have many implementation classes :

- **NioSocketConnector** : the non-blocking Socket transport Connector
- **NioDatagramConnector** : the non-blocking UDP transport \* Connector\*
- **AprSocketConnector** : the blocking Socket transport \* Connector\*, based on APR
- **ProxyConnector** : a Connector providing proxy support
- **SerialConnector** : a Connector for a serial transport
- **VmPipeConnector** : the in-VM \* Connector\*

Just pick the one that fit your need.

Here is the class diagram for the IoConnector interfaces and classes :

![](mina.apache.org/assets/img/mina/IoServiceConnector.png)

[3.3 - Acceptor](#mina-apache-org-mina-project-userguide-ch3-service-ch3-3-acceptor)

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch4-session-ch4-session"></a>

# Chapter 4 - Session — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch4-session-ch4-session--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

# Chapter 4 - Session

- [4.1 - Session Configuration](#mina-apache-org-mina-project-userguide-ch4-session-ch4-1-session-configuration)
- [4.2 - Session Statistics](#mina-apache-org-mina-project-userguide-ch4-session-ch4-2-session-statistics)

## Introduction

The Session is at the heart of MINA : every time a client connects to the server, a new session is created on the server, and will be kept in memory until the client is disconnected. If you are using MINA on the client side, every time you connect to a server, a session will be created on the client too.

A session is used to store persistent information about the connection, plus any kind of information the client or the server might need to use during the request processing, and eventually during the whole session life.

This is also your access point for any operation you need to do on a session : sending messages, closing the session, etc…

It is critical to understand that due to the asynchrnous very nature of NIO, reading from a session does not make a lot of sense. Actually, your application get signalled whe some incoming message has arrived, and this is the IoHandler which is responsible for handling such event.

In other words, don’t call session.read(). Never.

## Session state

A session has a state, which will evolve during time.

- *Created* : the session has just been created
- *Connected* : the session has been created and is available
- \_Idle \_: the session hasn’t processed any request for at least a period of time (this period is configurable)
  - Idle for read\_ : no read has actually been made for a period of time
  - Idle for write\_ : no write has actually been made for a period of time
  - Idle for both\_ : no read nor write for a period of time
- *Secured* : the TLS layer has been initialised
- *Unsecured* : The TLS layer has been shut down
- *Closing* : the session is being closed (the remaining messages are being flushed, cleaning up is not terminated)
- *Input closed* : the input part of the socket has been closed
- *Closed* : The session is now closed, nothing else can be done to revive it. This is actually not a real state : when teh session is closed, it’s removed.

The following state diagram exposes all the possible states and transitions :

![](mina.apache.org/assets/img/mina/session-state.png)

We have a set of methods to get some information about the session status.

Session status :

- *isActive()* : tells if the session is valid (it might mean different things depending on the implementation)
- *isBothIdle()* : tells if the session is idling on reads and writes
- *isClosing()* : tells if the session is already being closed
- *isConnected()* : tells if the session is active (ie, not in the closing mode)
- *isIdle( idling status )* : tells if the session is idling on a specific state (read or write)
- *isReadIdle()* : tells if the session is idling on reads
- *isReadSuspended()* : tells if the session is not allowed to read messsages
- *isScheduledForFlush()* : tells if the session has pending messages that are to be written
- *isSecured()* : tells if teh TLS layer is active and initialized
- *isServer()* : tells if the session is on the server side
- *isWriteIdle()* : tells if the session is idling on writes
- *isWriteSuspended()* : tells if the session is not allowed to write messsages

## Opening a session

Actually, there is nothing you have to do : it’s automatic ! Every time a remote peer connect to a server, the server will create a new connection. On the client side, every time you connect to a server, a session will be created.

This session is passed as an argument to your handler, so that you can do something with it in your application. On the client side, when you do connect to a server, you can get back the created session this way :

```java
...
ConnectFuture connectionFuture = connector.connect(address);
connectionFuture.awaitUninterruptibly();

if (!connectionFuture.isConnected()) {
    return false;
}

session = connectionFuture.getSession();
...
```

You can also do it in a shortest way :

```java
...
session = connector.connect(address).getSession();
...
```

## Initialization

When a new session is created, it has to be initialized. This is done using the default *IoService* configuration, but you can update this configuration later on. Actually, when the session is created, we internally create a copy of the default *IoService* configuration that is stored within the session, and this is this configuration instance that will be used (and that can be modified).

This initialization will also starts the statistics counters, create the *Attributes* container, associate a write queue to to the session (this is where the messages will be enqueued until they have been sent), and ultimately, would you have provided a specific task to do during this phase, it will call it.

## Closing a session

A session can be closed in 4 ways, two of which are explicit :

- calling the *closeNow()* method (explicit)
- calling the *closeOnFlush()* method (explicit)
- when the remote peer has nicely closed the connection
- if an exception occurs

(note there are two deprecated methods that should not anymore be used : *close(boolean)* and *close()*)

### Explicit closure

The first two methods can be called anywhere in your application, the big difference is one (*closeNow()*) will simply close the session, discarding any message waiting to be transmitted to the peer, while the *closeOnFlush()* will wait until any pending message has been transmitted to the peer.

Be aware that if the remote peer is not anymore connected, the session that you are closing using a \_closeOnFlush()\_ call will never be destroyed, unless you also handle its idleness, or before the system TCP timeout has closed the socket - which might take hours -. Always manage idleness in your applications.

### Remote peer closing

When the remote peer closes the session properly, the session will be closed, and all the pending messages will be discarded. This is usually the way it works.

However, sometime, the remote peer does not properly close the connection (this could happen when the cable is brutally unplugged). In this case, the session never get informed about the disconnection. The only way to know about it is to regularly check for the session state : if it’s idle for more than a specific amount of time - it has to be configured -, then the application can decide to close the session. Otherwise, the session will be closed eventually when the TCP timeout will be reached (it can take hours…).

### Exception

In some case, an exception will occur that will cause the session to be closed. Typically, when a session is being created, we may face an issue, and the session will be immediately closed. One other possibility is that we can’t write some message, for instance because the channel has been closed : we then close the session.

All in all, every time we met an exception while processing a session, this session will be closed.

Of course, your application will be informed through the *ExceptionCaught* event.

## Configuration

Many different parameters can be set for a specific session :

- receive buffer size
- sending buffer size
- Idle time
- Write timeOut
- …

plus other configuration, depending on the transport type used (see Chapter 6 - Transports).

All those configuration parameters are stored into the *IoSessionConfig* object, which can be get from the session using the *session.getConfig()* method.

For further information about the session configuration, see [Chapter 4.1 - Session Configuration](#mina-apache-org-mina-project-userguide-ch4-session-ch4-1-session-configuration)

## Managing user-defined attributes

It might be necessary to store some data which may be used later. This is done using the dedicated data structure associated which each session. This is a key-value association, which can store any type of data the developer might want to keep permanent along the session’s life.

For instance, if you want to track the number of request a user has sent since the session has been created, it’s easy to store it into this map: just create a key that will be associated with this value.

```java
...
int counterValue = session.getAttribute( "counter" );
session.setAttribute( "counter", counterValue + 1 );
...
```

We have a way to handle stored Attributes into the session : an Attribute is a key/value pair, which can be added, removed and read from the session’s container.

This container is created automatically when the session is created, and will be disposed when the session is terminated.

### The session container

As we said, this container is a key/value container, which default to a Map, but it’s also possible to define another data structure if one want to handle long lived data, or to avoid storing all those data in memory if they are large : we can implement an interface and a factory that will be used to create this container when the session is created.

This snippet of code shows how the container is created during the session initialization :

```java
  protected final void initSession(IoSession session,
          IoFuture future, IoSessionInitializer sessionInitializer) {
      ...
      try {
          ((AbstractIoSession) session).setAttributeMap(session.getService()
                  .getSessionDataStructureFactory().getAttributeMap(session));
      } catch (IoSessionInitializationException e) {
          throw e;
      } catch (Exception e) {
          throw new IoSessionInitializationException(
                  "Failed to initialize an attributeMap.", e);
      }
      ...
```

and here is the factory interface we can implement if we want to define another kind of container :

```java
public interface IoSessionDataStructureFactory {
    /**
    * Returns an {@link IoSessionAttributeMap} which is going to be associated
    * with the specified <tt>session</tt>.  Please note that the returned
    * implementation must be thread-safe.
    */
    IoSessionAttributeMap getAttributeMap(IoSession session) throws Exception;
}
```

### The session attributes access

There are many methods available to manipulate the session’s attributes :

- *boolean containsAttribute(Object key)* : tells if a given attribute is present
- *Object getAttribute(Object key)* : gets the value for a given attribute
- *Object getAttribute(Object key, Object defaultValue)* : gets the value for a given attribute, or a default value if absent
- *Set<Object> getAttributeKeys()* : gets the set of all the stored attributes
- *Object removeAttribute(Object key)* : remove a given attribute
- *boolean removeAttribute(Object key, Object value)* : remove a given attribute/value pair
- *boolean replaceAttribute(Object key, Object oldValue, Object newValue)* : replace a give attribute/value pair
- *Object setAttribute(Object key)* : adds a new attribute with no value
- *Object setAttribute(Object key, Object value)* : adds a new attribute/value pair
- *Object setAttributeIfAbsent(Object key)* : adds a new attribute with no value, if it does not already exist
- *Object setAttributeIfAbsent(Object key, Object value)* : adds a new attribute/value pair, if it does not already exist

All those methods allows your application to store, remove, get or update the attributes stored into your session. Also note that some attributes are used internally by MINA : don’t lightly modify those you didn’t create !

## Filter chain

Each session is associated with a chain of filters, which will be processed when an incoming request or an outgoing message is received or emitted. Those filters are specific for each session individually, even if most of the cases, we will use the very same chain of filters for all the existing sessions.

However, it’s possible to dynamically modify the chain for a single session, for instance by adding a Logger Filter in the chain for a specific session.

## Statistics

Each session also keep a track of records about what has been done for the session :

- number of bytes received/sent
- number of messages received/sent
- Idle status
- throughput

and many other useful information.

For further information about the session statistics, see [Chapter 4.2 - Session Statistics](#mina-apache-org-mina-project-userguide-ch4-session-ch4-2-session-statistics)

## Handler

Last, not least, a session is attached to a Handler, in charge of dispatching the messages to your application. This handler will also send back response by using the session, simply by calling the write() method :

```java
...
session.write( <your message> );
...
```

[Chapter 3 - Service](#mina-apache-org-mina-project-userguide-ch3-service-ch3-service)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch4-session-ch4-1-session-configuration"></a>

# 4.1 - Session Configuration — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch4-session-ch4-1-session-configuration--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

[4.2 - Session Statistics](#mina-apache-org-mina-project-userguide-ch4-session-ch4-2-session-statistics)

# 4.1 - Session Configuration

Depending on the Session’s type, we can configure various elements. Some of those elements are shared across all the session’s type, some other are specific.

We currently support 4 session flavors :

- Socket : support for the TCP transport
- Datagram : support for the UDP transport
- Serial : support for the RS232 transport
- VmPipe : support for the IPC transport

## General parameters

Here is the list of all the global parameters (they can be set fo any of the Session flavors) :

| Parameter | type | Description | Default value |
| --- | --- | --- | --- |
| idleTimeForBoth | int | The number of seconds to wait before notify a session that is idle on reads and writes | Infinite |
| idleTimeForRead | int | The number of seconds to wait before notify a session that is idle on reads | Infinite |
| idleTimeForWrite | int | The number of seconds to wait before notify a session that is idle on writes | Infinite |
| maxReadBufferSize | int | The maximum size of the buffer used to read incoming data | 65536 bytes |
| minReadBufferSize | int | The minimal size of the buffer used to read incoming data | 64 bytes |
| readBufferSize | int | The default size of the buffer used to read incoming data | 2048 bytes |
| throughputCalculationInterval | int | The interval (seconds) between each throughput calculation. | 3s |
| useReadOperation | boolean | A flag set to TRUE when we allow an application to do a _session.read() | FALSE |
| writeTimeout | int | Delay to wait for completion before bailing out a write operation | 60s |

All those parameters can be accessed through the use of getters and setters (the *useReadOperation* parameter getter is using the *isUseReadOperation()* method).

## Socket specific parameters

| Parameter | type | Description | Default value |
| --- | --- | --- | --- |
| defaultReuseAddress | boolean | The value for the SO_REUSEADDR flag | true |
| keepAlive | boolean | The value for the SO_KEEPALIVE flag | false |
| oobInline | boolean | The value for the SO_OOBINLINE flag | false |
| receiveBufferSize | int | The value for the SO_RCVBUF parameter | -1 |
| reuseAddress | boolean | The value for the SO_REUSEADDR flag | false |
| sendBufferSize | int | The value for the SO_SNDBUF parameter | -1 |
| soLinger | int | The value for the SO_LINGER parameter | -1 |
| tcpNoDelay | boolean | The value for the TCP_NODELAY flag | false |
| trafficClass | int | The value for the IP_TOS parameter. One of IPTOS_LOWCOST(0x02), IPTOS_RELIABILITY(0x04), IPTOS_THROUGHPUT (0x08) or IPTOS_LOWDELAY (0x10) | 0 |

## Datagram specific parameters

| Parameter | type | Description | Default value |
| --- | --- | --- | --- |
| broadcast | boolean | The value for the SO_BROADCAST flag | false |
| closeOnPortUnreachable | boolean | Tells if we should close the session if the port is unreachable | true |
| receiveBufferSize | int | The value for the SO_RCVBUF parameter | -1 |
| reuseAddress | boolean | The value for the SO_REUSEADDR flag | false |
| sendBufferSize | int | The value for the SO_SNDBUF parameter | -1 |
| trafficClass | int | The value for the IP_TOS parameter. One of IPTOS_LOWCOST(0x02), IPTOS_RELIABILITY(0x04), IPTOS_THROUGHPUT (0x08) or IPTOS_LOWDELAY (0x10) | 0 |

## Serial specific parameters

| Parameter | type | Description | Default value |
| --- | --- | --- | --- |
| inputBufferSize | int | The input buffer size to use | 8 |
| lowLatency | boolean | Set the Low Latency mode | false |
| outputBufferSize | int | The output buffer size to use | 8 |
| receiveThreshold | int | Set the receive threshold in byte (set it to -1 for disable) | -1 |

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

[4.2 - Session Statistics](#mina-apache-org-mina-project-userguide-ch4-session-ch4-2-session-statistics)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch4-session-ch4-2-session-statistics"></a>

# 4.2 - Session Statistics — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch4-session-ch4-2-session-statistics--Navigation-Upcoming"></a>Upcoming

- No event

[4.1 - Session Configuration](#mina-apache-org-mina-project-userguide-ch4-session-ch4-1-session-configuration)

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

# 4.2 - Session Statistics

We keep some statistics in each sessions about what’s going on. Not all those statistics are computed fr every message though : some of them are computed on demand.

| Parameter | type | Description | automatic |
| --- | --- | --- | --- |
| readBytes | long | The total number of bytes read since the session was created | yes |
| readBytesThroughput | double | The number of bytes read per second in the last interval | no |
| readMessages | long | The total number of messages read since the session was created | yes |
| readMessagesThroughput | double | The number of messages read per second in the last interval | no |
| scheduledWriteBytes | AtomicInteger | The number of bytes waiting to be written | yes |
| scheduledWriteMessages | AtomicInteger | The number of messages waiting to be written | yes |
| writtenBytes | long | The total number of bytes written since the session was created | yes |
| writtenBytesThroughput | double | The number of bytes written per second in the last interval | no |
| writtenMessages | long | The total number of messages written since the session was created | yes |
| writtenMessagesThroughput | double | The number of messages written per second in the last interval | no |

All those parameters can be read using getters.

[4.1 - Session Configuration](#mina-apache-org-mina-project-userguide-ch4-session-ch4-1-session-configuration)

[Chapter 4 - Session](#mina-apache-org-mina-project-userguide-ch4-session-ch4-session)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-1-blacklist-filter"></a>

# 5.1 - Blacklist Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-1-blacklist-filter--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.2 - Buffered Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-2-buffered-write-filter)

# 5.1 - Blacklist Filter

This filter blocks connections from blacklisted remote addresses. One can block *Addresses* or *Subnets*. In any case, when an event happens on a blocked session, the session will simply be closed. Here are the events this filter handles :

- *event* (MINA 2.1)
- *messageReceived*
- *messageSent*
- *sessionCreated*
- *sessionIdle*
- *sessionOpened*

There is no need to handle any other event.

## Blocking an address

Any address or subnet can be blocked live, ie it does not matter if a session is already active or not, this dynamically activated.
It’s enough to add the filter in the chain, and to set (or unset) the addresses to block:

```java
...
BlacklistFilter blackList = new BlacklistFilter();
blackList.block(InetAddress.getByName("1.2.3.4"));
acceptor.getFilterChain().addLast("blacklist", new BlacklistFilter());
...
```

Here, the “1.2.3.4” address will be blocked.

## Unblocking an address

It’s possible to unblock an address, it’s just a matter of fetching the filter and remove a previously blocked address:

```java
...
BlacklistFilter blackList = (BlacklistFilter)session.getFilterChain().get(BlacklistFilter.class);
blackList.unblock(InetAddress.getByName("1.2.3.4"));
...
```

Here, the “1.2.3.4” address will be unblocked.

## Performances

Currently, the implementation is not really optimal… We use a *List* to store the blocked addresses/subnet, so the more of them you have in the list the longer it will take to process any event.

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.2 - Buffered Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-2-buffered-write-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-10-mdc-injection-filter"></a>

# 5.10 - MDC Injection Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-10-mdc-injection-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.9 - Logging Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-9-logging-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.11 - NOOP Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-11-noop-filter)

# 5.10 - MDC Injection Filter

TBD…

[5.9 - Logging Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-9-logging-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.11 - NOOP Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-11-noop-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-11-noop-filter"></a>

# 5.11 - NOOP Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-11-noop-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.10 - MDC Injection Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-10-mdc-injection-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.12 - Profiler Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-12-profiler-filter)

# 5.11 - NOOP Filter

TBD…

[5.10 - MDC Injection Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-10-mdc-injection-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.12 - Profiler Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-12-profiler-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-12-profiler-filter"></a>

# 5.12 - Profiler Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-12-profiler-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.11 - NOOP Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-11-noop-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.13 - Protocol Codec Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-13-protocol-codec-filter)

# 5.12 - Profiler Filter

TBD…

[5.11 - NOOP Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-11-noop-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.13 - Protocol Codec Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-13-protocol-codec-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-13-protocol-codec-filter"></a>

# 5.13 - Protocol Codec Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-13-protocol-codec-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.12 - Profiler Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-12-profiler-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.14 - Proxy Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-14-proxy-filter)

# 5.13 - Protocol Codec Filter

TBD…

[5.12 - Profiler Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-12-profiler-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.14 - Proxy Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-14-proxy-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-14-proxy-filter"></a>

# 5.14 - Proxy Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-14-proxy-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.13 - Protocol Codec Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-13-protocol-codec-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.15 - Reference Counting Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-15-reference-counting-filter)

# 5.14 - Proxy Filter

TBD…

[5.13 - Protocol Codec Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-13-protocol-codec-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.15 - Reference Counting Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-15-reference-counting-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-15-reference-counting-filter"></a>

# 5.15 - Reference Counting Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-15-reference-counting-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.14 - Proxy Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-14-proxy-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.16 - Request/Response Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-16-request-response-filter)

# 5.15 - Reference Counting Filter

TBD…

[5.14 - Proxy Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-14-proxy-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.16 - Request/Response Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-16-request-response-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-16-request-response-filter"></a>

# 5.16 - Request/Response Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-16-request-response-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.15 - Reference Counting Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-15-reference-counting-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.17 - Session Attribute Initializing Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-17-session-attribute-initializing-filter)

# 5.16 - Request/Response Filter

TBD…

[5.15 - Reference Counting Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-15-reference-counting-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.17 - Session Attribute Initializing Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-17-session-attribute-initializing-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-17-session-attribute-initializing-filter"></a>

# 5.17 - Session Attribute Initializing Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-17-session-attribute-initializing-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.16 - Request/Response Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-16-request-response-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.18 - Stream Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-18-stream-write-filter)

# 5.17 - Session Attribute Initializing Filter

TBD…

[5.16 - Request/Response Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-16-request-response-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.18 - Stream Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-18-stream-write-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-18-stream-write-filter"></a>

# 5.18 - Stream Write Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-18-stream-write-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.17 - Session Attribute Initializing Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-17-session-attribute-initializing-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.19 - SSL/TLS Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-19-ssl-filter)

# 5.18 - Stream Write Filter

TBD…

[5.17 - Session Attribute Initializing Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-17-session-attribute-initializing-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.19 - SSL/TLS Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-19-ssl-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-19-ssl-filter"></a>

# 5.19 - SSL/TLS Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-19-ssl-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.18 - Stream Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-18-stream-write-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.20 - Write Request Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-20-write-request-filter)

# 5.19 - SSL/TLS Filter

This is a critical part of any serious network application: securing the communication between the client and the server.

In **MINA**, we do that with the **SslFilter**. The idea is to add this *Filter* in the chain and it will deal with the *Handshake* negociation, and the following encrypting and uncrypting of the data.

## How it works

The thing to remember is that the **TLS** protocol works in two steps:

- First there is a negociation part (the *Handshake*)
- Then once the *Handshake* is completed, all the data will be encrypted before being sent, and decryptd when received.

We just have to add the filter in the chain like this:

```java
   ...
   SslFilter sslFilter = new SslFilter(sslContext);
   DefaultIoFilterChainBuilder chain = acceptor.getFilterChain();
   chain.addFirst("sslFilter", sslFilter);
   ...
```

The *chain* is the instance of **IoFilterChainBuilder** class that is associated with either the *Connector* or the *Acceptor* instances (wether we are using a client or a server). It will be used when a connection is set to define the filter chain this connected session will use.

Usually the \*\*SSLFilter\*\* is always set at the first position in the chain. The rationnal is that it will receive and send encrypted data, so there is little use of having a functionnal filter between the head of the chain and the \*\*SslFilter\*\*. However, if such a filter is to be used, make sure it does not interfere with the data being exchanged with the remote peer.

The missing part here is the *sslContext* which has to be defined. It’s an instance of the **SSLContext** class, where you define a **TrustManager**, a **KeyManager** and a **Securerandom**.

This part is strandard Java security. You'll find plenty of pages on internet explaining you to set up a proper \_SSLContext\_ instance.

The filter can be added either *before* the connection has been established, or *after* the connection has been established.
In the first case, it’s easy, the filter initialisation will be done when the connection is created. The second is a bit more complex, as we will have to deal with ongoing messages.

### Initialization

When a connection is created, the filter chain is created, and the **SslFilter** is added to this chain, which intializes it. The filter’s *onPreAdd* method is called, which will only check if the filter has already been added (we can’t have the **SslFilter** twice in the chain).

The filter’s *onPostAdd* method is then called, and if the *autostart* flag is set to *true* and the session is connected (which should be the case…), then the *onConnected* method is called.

The \_autostart\_ flag can be set when creating the \*\*SslFilter\*\*. It indicates that the filter will be initialized immediatly on a new connection. Otherwise the filter will be initialized later on when the session will have been opened.

The **SslHandler** will then be created, which will contain a newly created **SslEngine**. This is the place where the **TLS** parameters will be set:

- The authentication **WANT** or **NEED** flags
- The cipher suite
- The enabled protocols
- The endpoint identification algorithms
- and finally the flag indicating if it’s for a server or a client

All those flags are optionnal but the last one.

The newly created **SslHandler** instance will be added into the session’s attributes, under the *org.apache.mina.filter.ssl.SslHandler.handler@* key (The **ID** part is unique).

Last, not least, the **SslHandler** instance is opened. If we are on the client side, the **TLS** *Handshake* will be started, otherwise a flag will be set to indicate we are expecting a *Handshake* on the server side.

We are all set and ready for the *Handshake* to be processed!

### Handshake

The *Handshake* must be done as soon as the **TLS** layer is activated. It’s initiated by the client, which must send a *CLIENT\_HELLO* message (see the [Client Hello](https://tls13.xargs.org/#client-hello) description), but the server must be ready to accept this message.

We will consider two use cases: the **Client** side and the **Server** side (as **Mina** can handle both sides, but could work with a peer written using a different implementation)

#### Initial Handshake message

The first *Handshake* message to be processed is the **CLIENT HELLO** **TLS** message.

##### Server side

The *Handshake* is initiated by a **CLIENT\_HELLO** **TLS** request which is sent by the client, the server is waiting for it. There starts a serie of **TLS** messages exchanged between the cleint and the server, at the end of which the session is secured, and data can be exchanged encrypted.

What is important to note here is that during the *Handshake* exchanges, no application data can be sent, and the **SslFilter** is handling the whole process, before returning the hand to the application. That means messages emitted by the application will be stacked until the session is secured.

##### Client side

The client is the initiator of the *Handshake* exchange. The difference with the server is that the client **SSLhandler** will generate the inital *Handshake* message (**CLIENT HELLO**) and send it to the server

#### Follow up Handshake messages

Once the *Handshake* has been initiated, the dance keeps going until the *Handshake* is either done or fails:

- The client sends *Handshake* requests
- The server sends *Handshake* responses

This dance can use many steps.

It ends with both side send a *finished* message, and has verified that it’s valid.

### Session secured

The application is informed that the session is secured:

- In **MINA** 2.0.X, the **SslFilterMessage** message is sent and can be processed in the **IoHandler.messageReived** handler.
- In **MINA** 2.1.X and 2.2.X, the **IoHandler** interface exposes an *event* handler that is used by the **SslFilter** to inform the application that the session is secured, by emmiting a **SslEvent.SECURED** event.

It’s clear that the way **MINA** 2.1.X and 2.2.X signal that the session is secured is way simpler.

### StartTLS use case

Some protocols, like **LDAP**, **FTP**, **XMPP**, **NNTP**, **SMTP**, can be used without encryption, but allow some encryption to be set by using a specific command. When this command is issued, the **TLS** layer is added to an already connected session, without reestablishing a new session.

That is what the **startTLS** command is used for that purpose. Here are a list of existing protocols that use this mechanism:

- [**IMAP, POP3 and ACAP**](https://datatracker.ietf.org/doc/html/rfc2595)
- [**SMTP**](https://datatracker.ietf.org/doc/html/rfc3207)
- [**XMPP**](https://datatracker.ietf.org/doc/html/rfc6120)
- [**NNTP**](https://datatracker.ietf.org/doc/html/rfc4642)
- [**LDAP**](https://datatracker.ietf.org/doc/html/rfc4513)

The way it works is that at some point of a clear text established connection, the client sends a **STARTLS** command, which forces the **TLS** *Handshake* to start to establish a secured connection. When the secured connection is established, the client and the server exchanges will be encrypted.

Now, that is a bit tricky to implement, because both the client *and* the server must block the exchange of messages during the establishement of the **TLS** session. As the client is the initiator of the command, it’s easy, but on the server side, we must pass a response to the original command **in clear text** informing the client that the command has been received and processed.

```text
(C) The client sends a **StartTLS** command
(S) The server receives the command, initialize the **TLS** layer, and send back a response to the client, bypassing the **TLS** layer.
(C) The client received the server response to the command, and setup the **TLS** layer, and initiates the **TLS** _Handshake_
(S/C) The _Handshake_ is processed
(S/C) The session is secured on both side
(S/C) at this point, every message exchanged are encrypted.
```

[5.18 - Stream Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-18-stream-write-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.20 - Write Request Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-20-write-request-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-2-buffered-write-filter"></a>

# 5.2 - Buffered Write Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-2-buffered-write-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.1 - Blacklist Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-1-blacklist-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.3 - Compression Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-3-compression-filter)

# 5.2 - Buffered Write Filter

TBD…

[5.1 - Blacklist Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-1-blacklist-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.3 - Compression Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-3-compression-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-20-write-request-filter"></a>

# 5.20 - Write Request Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-20-write-request-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.19 - SSL/TLS Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-19-ssl-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

# 5.20 - Write Request Filter

TBD…

[5.19 - SSL/TLS Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-19-ssl-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-3-compression-filter"></a>

# 5.3 - Compression Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-3-compression-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.2 - Buffered Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-2-buffered-write-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.4 - Connection Throttle Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-4-connection-throttle-filter)

# 5.3 - Compression Filter

The *CompressionFilter* class is used to compress data before it gets sent and decompress it when it’s received. It uses the [JZLIB library](http://www.jcraft.com/jzlib/).

It only handles two events :

- *messageReceived*: The received message will be decompressed, if it’s contained in a *IoBuffer*.
- *filterWrite*: The message to write will be compressed, assuming it’s stored in a *IoBuffer* (otherwise an exception will be generated).

## Configuration

It’s possible to configure the *CompressionFilter*, with the listed parameters :

- *compressInbound*: if set to *true*, the incoming data will be decompressed, otherwise they will be left intact.
- *compressOutbound*: if set to *true*, the outgoing data will be compressed, otherwise they will be ignored.
- *compressionLevel*: one of *COMPRESSION\_DEFAULT* (-1), *COMPRESSION\_MAX* (9), *COMPRESSION\_MIN* (1), *COMPRESSION\_NONE* (0). Sets the level, of desired compression.

There is one more parameter that can be used, injected into the session’s attributes, the *DISABLE\_COMPRESSION\_ONCE* flag. If present in the session’s attributes, the first compression will be ignored (this is probably useful for application that needs to send a message that the flowing messages are going to be compressed).

## Initialization

When the filter is added to the chain, instances of the deflater and inflater are created and injected into the session’s attributes. This could change, as each session has its own instance of the filter in its chain, so there is no reason not to store those instances in the filter itself.

## Side notes

The way this filter works does not make it very efficient. It requires that the received data to be decompressed are fully read - and there are no control that it’s the case of not -, and it also requires that the data to be compressed be fully compressed in one pass, which is quite inefficient from the memory perspective, as we may compress data by blocks, and send them immediately, waiting for the block to be really sent to compress the next one.

One more thing: there is no way this filter can be used to compress something like a file or anything that is not contained in a *IoFilter*. This is a pretty strong limitation.

All in all, it’s a brittle filter, you’d better not use it as it is!

## Future improvements

So we need a compression filter that does not eat all the memory, possibly a stateless one - why should we have to create as many instances as we have sessions ? -, that is fast, AL 2.0 compatible, and that can flush data pieces by pieces.

### Stateless compression/decompression

The idea is to have a function that can be called statically, with a context. It’s not necessarily complex we just need to keep track of a context.

It should be possible top compress a block of data, send it, and process the next block when the first block has been processed. Same thing with received data: it should be possible to process a buffer, decompress it and wait for the remaining bytes until a block of data has been fully decompressed.

[5.2 - Buffered Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-2-buffered-write-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.4 - Connection Throttle Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-4-connection-throttle-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-4-connection-throttle-filter"></a>

# 5.4 - Connection Throttle Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-4-connection-throttle-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.3 - Compression Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-3-compression-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.5 - Error Generating Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-5-error-generating-filter)

# 5.4 - Connection Throttle Filter

TBD…

[5.3 - Compression Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-3-compression-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.5 - Error Generating Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-5-error-generating-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-5-error-generating-filter"></a>

# 5.5 - Error Generating Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-5-error-generating-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.4 - Connection Throttle Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-4-connection-throttle-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.6 - Executor Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-6-executor-filter)

# 5.5 - Error Generating Filter

TBD…

[5.4 - Connection Throttle Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-4-connection-throttle-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.6 - Executor Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-6-executor-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-6-executor-filter"></a>

# 5.6 - Executor Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-6-executor-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.5 - Error Generating Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-5-error-generating-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.7 - FileRegion Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-7-file-region-write-filter)

# 5.6 - Executor Filter

TBD…

[5.5 - Error Generating Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-5-error-generating-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.7 - FileRegion Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-7-file-region-write-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-7-file-region-write-filter"></a>

# 5.7 - FileRegion Write Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-7-file-region-write-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.6 - Executor Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-6-executor-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.8 - KeepAlive Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-8-keep-alive-filter)

# 5.7 - FileRegion Write Filter

TBD…

[5.6 - Executor Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-6-executor-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.8 - KeepAlive Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-8-keep-alive-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-8-keep-alive-filter"></a>

# 5.8 - KeepAlive Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-8-keep-alive-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.7 - FileRegion Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-7-file-region-write-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.9 - Logging Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-9-logging-filter)

# 5.8 - KeepAlive Filter

TBD…

[5.7 - FileRegion Write Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-7-file-region-write-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.9 - Logging Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-9-logging-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-9-logging-filter"></a>

# 5.9 - Logging Filter — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch5-filters-ch5-9-logging-filter--Navigation-Upcoming"></a>Upcoming

- No event

[5.8 - KeepAlive Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-8-keep-alive-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.10 - MDC Injection Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-10-mdc-injection-filter)

# 5.9 - Logging Filter

The *Logging* filter allows an application to logs **MINA** protocol events while they are transiting on the filters chain. It can be added dynamically (ie, a session may add a filter whenever it wants).

The tracked events are :

- *exceptionCaught*
- *messageReceived*
- *messageSent*
- *sessionClosed*
- *sessionCreated*
- *sessionIdle*
- *sessionOpened*

The *event*, *filterClose*, *filterWrite* and *inputClosed* events are not tracked.

## Adding the filter

This can be done once and for each session, while creating the *IoFilterChainBuilder* instance :

```java
...
NioSocketAcceptor acceptor = new NioSocketAcceptor();
DefaultIoFilterChainBuilder builderChain = acceptor.getFilterChain();
builderChain.addLast("logger", new LoggingFilter());
...
```

or it can be added dynamically, in a given session:

```java
...
session.getFilterChain().addLast("logger", new LoggingFilter());
...
```

(here, the filter is added at the end of the filter’s chain, but it can added at the beginning, using *addFirst*, or before or after a given filter, with *addBefore* or *addAfter*)

## Configuring the filter

Each event can be configured individually. For instance, to log the *messageReceived* event in *DEBUG* mode, simply add this code in your *IoHandler* implementation:

```java
...
LoggingFilter loggingFilter = (LoggingFilter)session.getFilterChain().get( LoggingFilter.class );

if (logginFilter != null) {
    loggingFilter.setMessageReceivedLogLevel( LogLevel.DEBUG);
}
...
```

This is per session, and it’s dynamic.

## Removing the filter

It’s always possible to remove the filter for a given session :

```java
...
session.getFilterChain().remove("logger");
...
```

[5.8 - KeepAlive Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-8-keep-alive-filter)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[5.10 - MDC Injection Filter](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-10-mdc-injection-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports"></a>

# Chapter 6 - Transports — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 7 - Handler](#mina-apache-org-mina-project-userguide-ch7-handler-ch7-handler)

# Chapter 6 - Transports

- [6.1 - APR Transport](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-1-apr-transport)
- [6.2 - Serial Transport](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-2-serial-transport)

[Chapter 5 - Filters](#mina-apache-org-mina-project-userguide-ch5-filters-ch5-filters)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 7 - Handler](#mina-apache-org-mina-project-userguide-ch7-handler-ch7-handler)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch7-handler-ch7-handler"></a>

# Chapter 7 - Handler — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch7-handler-ch7-handler--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 8 - IoBuffer](#mina-apache-org-mina-project-userguide-ch8-iobuffer-ch8-iobuffer)

# Chapter 7 - Handler

Handles all I/O events fired by MINA. The interface is hub of all activities done at the end of the Filter Chain.

IoHandler has following functions

- sessionCreated
- sessionOpened
- sessionClosed
- sessionIdle
- exceptionCaught
- messageReceived
- messageSent

## sessionCreated Event

Session Created event is fired when a new connection is created. For TCP its the result of connection accept, and for UDP this is generated when a UDP packet is received. This function can be used to initialize session attributes, and perform one time activities for a particular connection.

This function is invoked from the I/O processor thread context, hence should be implemented in a way that it consumes minimal amount of time, as the same thread handles multiple sessions.

## sessionOpened Event

Session opened event is invoked when a connection is opened. Its is always called after sessionCreated event. If a thread model is configured, this function is called in a thread other than the I/O processor thread.

## sessionClosed Event

Session Closed event is closed, when a session is closed. Session cleaning activities like cash cleanup can be performed here.

## sessionIdle Event

Session Idle event is fired when a session becomes idle. This function is not invoked for UDP transport.

## exceptionCaught Event

This functions is called, when an Exception is thrown by user code or by MINA. The connection is closed, if its an IOException.

## messageReceived Event

Message Received event is fired whenever a message is received. This is where the most of the processing of an application happens. You need to take care of all the message type you expect here.

## messageSent Event

Message Sent event is fired, whenever a message aka response has been sent(calling IoSession.write()).

[Chapter 6 - Transports](#mina-apache-org-mina-project-userguide-ch6-transports-ch6-transports)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 8 - IoBuffer](#mina-apache-org-mina-project-userguide-ch8-iobuffer-ch8-iobuffer)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.

---

<a id="mina-apache-org-mina-project-userguide-ch8-iobuffer-ch8-iobuffer"></a>

# Chapter 8 - IoBuffer — Apache MINA

[
Apache MINA Project
](/)
 | 
[
**MINA**
](#mina-apache-org-mina-project-index)
 | 
[
AsyncWeb
](/asyncweb-project/)
 | 
[
FtpServer
](/ftpserver-project/)
 | 
[
SSHD
](/sshd-project/)
 | 
[
Vysper
](/vysper-project/)

[![Apache Events Calendar](https://www.apachecon.com/event-images/current-event-wide-light.png "Apache Events Calendar")](https://events.apache.org/)

##### Social Networks

- [Apache MINA Mastodon](https://fosstodon.org/@apachemina)

##### Latest Downloads

- [Mina 2.0.28](#mina-apache-org-mina-project-downloads_2_0)
- [Mina 2.1.11](#mina-apache-org-mina-project-downloads_2_1)
- [Mina 2.2.6](#mina-apache-org-mina-project-downloads_2_2)
- [Mina old versions](#mina-apache-org-mina-project-downloads_old)

##### Documentation

- [Base documentation](#mina-apache-org-mina-project-documentation)
- [User guide](#mina-apache-org-mina-project-userguide-user-guide-toc)
- [2.2 vs 2.1](#mina-apache-org-mina-project-2-2-vs-2-1)
- [2.1 vs 2.0](#mina-apache-org-mina-project-2-1-vs-2-0)
- [Features](#mina-apache-org-mina-project-features)
- [Road Map](#mina-apache-org-mina-project-road-map)
- [Quick Start Guide](#mina-apache-org-mina-project-quick-start-guide)
- [FAQ](#mina-apache-org-mina-project-faq)

##### Resources

- [Mailing lists & IRC](#mina-apache-org-mina-project-mailing-lists)
- [Issue tracking](#mina-apache-org-mina-project-issue-tracking)
- [Sources](#mina-apache-org-mina-project-sources)
- [API Javadoc 2.0.28](/mina-project/gen-docs/latest-2.0/apidocs/index.html)
- [API Javadoc 2.1.11](/mina-project/gen-docs/latest-2.1/apidocs/index.html)
- [API Javadoc 2.2.6](/mina-project/gen-docs/latest-2.2/apidocs/index.html)
- [API xref 2.0.28](/mina-project/gen-docs/latest-2.0/xref/index.html)
- [API xref 2.1.11](/mina-project/gen-docs/latest-2.1/xref/index.html)
- [API xref 2.2.6](/mina-project/gen-docs/latest-2.2/xref/index.html)
- [Performances](#mina-apache-org-mina-project-performances)
- [Testimonials](#mina-apache-org-mina-project-testimonials)
- [Conferences](#mina-apache-org-mina-project-conferences)
- [Developers Guide](#mina-apache-org-mina-project-developer-guide)
- [Related Projects](#mina-apache-org-mina-project-related-projects)
- [Statistics](https://people.apache.org/~vgritsenko/stats/projects/mina.html)

##### Community

- [Contributing](https://www.apache.org/foundation/contributing.html)
- [Team](/contributors.html)
- [Special Thanks](/special-thanks.html)
- [Security](https://www.apache.org/security/)

##### About Apache

- [Apache main site](https://www.apache.org)
- [License](https://www.apache.org/licenses/)
- [Sponsorship program](https://www.apache.org/foundation/sponsorship.html "The ASF sponsorship program")
- [Thanks](https://www.apache.org/foundation/thanks.html)

### <a id="mina-apache-org-mina-project-userguide-ch8-iobuffer-ch8-iobuffer--Navigation-Upcoming"></a>Upcoming

- No event

[Chapter 7 - IoHandler](#mina-apache-org-mina-project-userguide-ch7-handler-ch7-handler)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 9 - Codec Filter](#mina-apache-org-mina-project-userguide-ch9-codec-filter-ch9-codec-filter)

# Chapter 8 - IoBuffer

A byte buffer used by MINA applications.

This is a replacement for [ByteBuffer](http://java.sun.com/j2se/1.5.0/docs/api/java/nio/ByteBuffer.html). MINA does not use NIO ByteBuffer directly for two reasons:

- It doesn’t provide useful getters and putters such as fill, get/putString, and get/putAsciiInt() .
- It is difficult to write variable-length data due to its fixed capacity

This will change in MINA 3. The main reason why MINA has its own wrapper on top of nio ByteBuffer is to have extensible buffers. This was a very bad decision. Buffers are just buffers : a temporary place to store temporary data, before it is used. Many other solutions exist, like defining a wrapper which relies on a list of NIO ByteBuffers, instead of copying the existing buffer to a bigger one just because we want to extend the buffer capacity.

It might also be more comfortable to use an InputStream instead of a byte buffer all along the filters, as it does not imply anything about the nature of the stored data : it can be a byte array, strings, messages...

Last, not least, the current implementation defeat one of the target : zero-copy strategy (ie, once we have read the data from the socket, we want to avoid a copy being done later). As we use extensible byte buffers, we will most certainly copy those data if we have to manage big messages. Assuming that the MINA ByteBuffer is just a wrapper on top of NIO ByteBuffer, this can be a real problem when using direct buffers.

## IoBuffer Operations

### Allocating a new Buffer

IoBuffer is an abstract class, hence can’t be instantiated directly. To allocate IoBuffer, we need to use one of the two allocate() methods.

```java
// Allocates a new buffer with a specific size, defining its type (direct or heap)
public static IoBuffer allocate(int capacity, boolean direct)

// Allocates a new buffer with a specific size
public static IoBuffer allocate(int capacity)
```

The allocate() method takes one or two arguments. The first form takes two arguments :

- **capacity** - the capacity of the buffer
- **direct** - type of buffer. true to get direct buffer, false to get heap buffer

The default buffer allocation is handled by [SimpleBufferAllocator](https://nightlies.apache.org/mina/mina/2.0.22/xref/org/apache/mina/core/buffer/SimpleBufferAllocator.html)

Alternatively, following form can also be used

```java
// Allocates heap buffer by default.
IoBuffer.setUseDirectBuffer(false);

// A new heap buffer is returned.
IoBuffer buf = IoBuffer.allocate(1024);
```

When using the second form, don’t forget to set the default buffer type before, otherwise you will get Heap buffers by default.

## Creating Auto Expanding Buffer

Creating auto expanding buffer is not very easy with java NIO API’s, because of the fixed size of the buffers. Having a buffer, that can auto expand on needs is a big plus for networking applications. To address this, IoBuffer has introduced the autoExpand property. It automatically expands its capacity and limit value.

Lets see how to create an auto expanding buffer :

```java
IoBuffer buffer = IoBuffer.allocate(8);
buffer.setAutoExpand(true);
buffer.putString("12345678", encoder);

// Add more to this buffer
buffer.put((byte)10);
```

The underlying ByteBuffer is reallocated by IoBuffer behind the scene if the encoded data is larger than 8 bytes in the example above. Its capacity will double, and its limit will increase to the last position the string is written. This behavior is very similar to the way StringBuffer class works.

This mechanism is very likely to be removed from MINA 3.0, as it's not really the best way to handle increased buffer size. It should be replaced by something like a InputStream hiding a list or an array of fixed sized ByteBuffers.

## Creating Auto Shrinking Buffer

There are situations which calls for releasing additionally allocated bytes from the buffer, to preserve memory. IoBuffer provides autoShrink property to address the need. If autoShrink is turned on, IoBuffer halves the capacity of the buffer when compact() is invoked and only 1/4 or less of the current capacity is being used. To manually shrink the buffer, use shrink() method.

Lets see this in action :

```java
IoBuffer buffer = IoBuffer.allocate(16);
buffer.setAutoShrink(true);
buffer.put((byte)1);
System.out.println("Initial Buffer capacity = "+buffer.capacity());

buffer.shrink();
System.out.println("Initial Buffer capacity after shrink = "+buffer.capacity());

buffer.capacity(32);
System.out.println("Buffer capacity after incrementing capacity to 32 = "+buffer.capacity());

buffer.shrink();
System.out.println("Buffer capacity after shrink= "+buffer.capacity());
```

We have initially allocated a capacity as 16, and set the autoShrink property as true.

Lets see the output of this :

```java
Initial Buffer capacity = 16
Initial Buffer capacity after shrink = 16
Buffer capacity after incrementing capacity to 32 = 32
Buffer capacity after shrink= 16
```

Lets take a break and analyze the output

- Initial buffer capacity is 16, as we created the buffer with this capacity. Internally this becomes the minimum capacity of the buffer
- After calling shrink(), the capacity remains 16, as capacity shall never be less than minimum capacity
- After incrementing capacity to 32, the capacity becomes 32
- Call to shrink(), reduces the capacity to 16, thereby eliminating extra storage

Again, this mechanism should be a default one, without needing to explicitely tells the buffer that it can shrink.

## Buffer Allocation

IoBufferAllocator is responsible for allocating and managing buffers. To have precise control on the buffer allocation policy, implement the IoBufferAllocator interface.

MINA ships with following implementations of IoBufferAllocator

- **SimpleBufferAllocator (default)** - Create a new buffer every time
- **CachedBufferAllocator** - caches the buffer which are likely to be reused during expansion

With the new available JVM, using cached IoBuffer is very unlikely to improve performances.

You can implement you own implementation of IoBufferAllocator and call setAllocator() on IoBuffer to use the same.

[Chapter 7 - IoHandler](#mina-apache-org-mina-project-userguide-ch7-handler-ch7-handler)

[User Guide](#mina-apache-org-mina-project-userguide-user-guide-toc)

[Chapter 9 - Codec Filter](#mina-apache-org-mina-project-userguide-ch9-codec-filter-ch9-codec-filter)

© 2003-2026, [The Apache Software Foundation](https://www.apache.org) - [Privacy Policy](https://privacy.apache.org/policies/privacy-policy-public.html)  
Apache MINA, MINA, Apache Vysper, Vysper, Apache SSHd, SSHd, Apache FtpServer, FtpServer, Apache AsyncWeb, AsyncWeb,
Apache, the Apache feather logo, and the Apache Mina project logos are trademarks of The Apache Software Foundation.