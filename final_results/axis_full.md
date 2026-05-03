<a id="axis-apache-org-axis2-java-sandesha-userguide"></a>

# Sandesha2 - 
  
  Sandesha2 User Guide

# Apache Sandesha2 User's Guide

This document introduces you to Apache Sandesha2. This will first take you
through a step by step process of developing a sample application using
Sandesha2. In the latter sections you will be introduced to some advance
features making you more familiar with the application.

## Contents<a id="axis-apache-org-axis2-java-sandesha-userguide--Contents"></a>

- [Introduction](#axis-apache-org-axis2-java-sandesha-userguide--introduction)
- [Installing the Sandesa2 module](#axis-apache-org-axis2-java-sandesha-userguide--install)
- [Creating and deploying a RM enabled Web
  service - SimpleService](#axis-apache-org-axis2-java-sandesha-userguide--your_first_service)
- [Writing clients for the SimpleService](#axis-apache-org-axis2-java-sandesha-userguide--writing_clients)
  - [Configuring the client repository](#axis-apache-org-axis2-java-sandesha-userguide--client_repo)
  - [Doing One-Way invocation](#axis-apache-org-axis2-java-sandesha-userguide--one_way)
  - [Doing a Request-Reply invocation](#axis-apache-org-axis2-java-sandesha-userguide--request_reply)
- [Sandesha2 Client API](#axis-apache-org-axis2-java-sandesha-userguide--client_api)
  - [Selecting your RM version](#axis-apache-org-axis2-java-sandesha-userguide--version)
  - [Getting Acknowledgements and Faults to a given
    Endpoint](#axis-apache-org-axis2-java-sandesha-userguide--acks)
  - [Managing Sequences](#axis-apache-org-axis2-java-sandesha-userguide--managing)
  - [Offering a Sequence ID for the Response
    Sequence](#axis-apache-org-axis2-java-sandesha-userguide--offering)
  - [Creating a Sequence Without Sending any
    Messages](#axis-apache-org-axis2-java-sandesha-userguide--creating)
  - [Sending Acknowledgement Requests from the
    Client Code](#axis-apache-org-axis2-java-sandesha-userguide--ack_requests)
  - [Terminating a Sequence from the Client
    Code](#axis-apache-org-axis2-java-sandesha-userguide--terminating)
  - [Closing a Sequence from the Client Code](#axis-apache-org-axis2-java-sandesha-userguide--closing)
  - [Blocking the Client Code until a Sequence is
    complete](#axis-apache-org-axis2-java-sandesha-userguide--blocking)
  - [Working with Sandesha Reports](#axis-apache-org-axis2-java-sandesha-userguide--reports)
    - [SandeshaReports](#axis-apache-org-axis2-java-sandesha-userguide--sandesha_reports)
    - [SequenceReports](#axis-apache-org-axis2-java-sandesha-userguide--sequence_reports)
  - [Sandesha Listener Feature](#axis-apache-org-axis2-java-sandesha-userguide--listners)
    - [onError](#axis-apache-org-axis2-java-sandesha-userguide--on_error)
    - [onTimeOut](#axis-apache-org-axis2-java-sandesha-userguide--on_timeout)
- [More about sequences](#axis-apache-org-axis2-java-sandesha-userguide--sequences)
  - [Creation of sequences](#axis-apache-org-axis2-java-sandesha-userguide--creation)
  - [Termination of sequences](#axis-apache-org-axis2-java-sandesha-userguide--termination)
  - [Closing of sequences](#axis-apache-org-axis2-java-sandesha-userguide--closing1)
  - [Timing out of sequences](#axis-apache-org-axis2-java-sandesha-userguide--timing_out)
- [Delivery Assurances of Sandesha2](#axis-apache-org-axis2-java-sandesha-userguide--delivary_assurances)
- [Configuring Sandesha2](#axis-apache-org-axis2-java-sandesha-userguide--configuring)
  - [AcknowledgementInterval](#axis-apache-org-axis2-java-sandesha-userguide--acknowledgementinterval)
  - [RetransmissionInterval](#axis-apache-org-axis2-java-sandesha-userguide--retransmissioninterval)
  - [ExponentialBackoff](#axis-apache-org-axis2-java-sandesha-userguide--exponentialbackoff)
  - [MaximumRetransmissionCount](#axis-apache-org-axis2-java-sandesha-userguide--maximumretransmissioncount)
  - [InactivityTimeout](#axis-apache-org-axis2-java-sandesha-userguide--inactivitytimeout)
  - [InactivityTimeoutMeasure](#axis-apache-org-axis2-java-sandesha-userguide--inactivitytimeoutmeasure)
  - [InvokeInOrder](#axis-apache-org-axis2-java-sandesha-userguide--invokeinorder)
  - [StorageManagers](#axis-apache-org-axis2-java-sandesha-userguide--storagemanagers)
  - [MessageTypesToDrop](#axis-apache-org-axis2-java-sandesha-userguide--messagetypestodrop)
  - [SecurityManager](#axis-apache-org-axis2-java-sandesha-userguide--securitymanager)

<a id="axis-apache-org-axis2-java-sandesha-userguide--introduction"></a>

## Introduction<a id="axis-apache-org-axis2-java-sandesha-userguide--Introduction"></a>

Sandesha2 is a Web Service-ReliableMessaging (WS-RM) implementation for
Apache Axis2. With Sandesha2 you can make your Web services reliable, or you
can invoke already hosted reliable Web services.

If you want to learn more about Apache Axis2, refer to [Apache Axis2 User
Guide](http://axis.apache.org/axis2/java/core/docs/userguide.html) and [Apache
Axis2 Architecture Guide](http://axis.apache.org/axis2/java/core/docs/Axis2ArchitectureGuide.html).

Architecure guide for Sandesha2 is accessible at [Sandesha2 Architecture Guide](#axis-apache-org-axis2-java-sandesha-architectureguide).

Sandesha2 supports the WS-ReliableMessaging specification. It fully
supports the WS-ReliableMessaging 1.0 specification (February 2005) ([
http://specs.xmlsoap.org/ws/2005/02/rm/ws-reliablemessaging.pdf ](http://specs.xmlsoap.org/ws/2005/02/rm/ws-reliablemessaging.pdf)).

This specification has been submitted to OASIS and currently being
standardized under the OASIS WS-RX Technical Committee as WSRM 1.1 (see [
http://www.oasis-open.org/committees/tc\_home.php?wg\_abbrev=ws-rx ](http://www.oasis-open.org/committees/tc_home.php?wg_abbrev=ws-rx) ).
Sandesha2 currently supports Committee Draft 4 of the specification being
developed under this technical committee ( [
http://docs.oasis-open.org/ws-rx/wsrm/200602/wsrm-1.1-spec-cd-04.pdf
](http://docs.oasis-open.org/ws-rx/wsrm/200602/wsrm-1.1-spec-cd-04.pdf)).

<a id="axis-apache-org-axis2-java-sandesha-userguide--install"></a>

## Installing the Sandesa2 module<a id="axis-apache-org-axis2-java-sandesha-userguide--Installing_the_Sandesa2_module"></a>

In this section you will be tought how to install the Sandesha2 module in
a Axis2 environment. Simply follow the given instructions step by step.

1. Download a compatible Axis2 binary distribution.
2. Extract the distribution file to a temporary folder (from now on
   referred as AXIS2\_HOME)
3. Add a user phase named 'RMPhase' to all four flows of the axis2.xml
   file which is in the AXIS2\_HOME/conf directory. Note the positioning of
   'RMPhase' within different phaseOrders.

   ```xml
                   
                   
                   <axisconfig name="AxisJava2.0">
                   
                           <!-- REST OF THE CONFIGURATION-->
                   
                           <phaseOrder type="InFlow">
                                   <phase name="Transport"/>
                                       <phase name="Security"/>
                                       <phase name="PreDispatch"/>
                                       <phase name="Dispatch" />
                                       <phase name="OperationInPhase"/>
                                       <phase name="soapmonitorPhase"/>
                                       <phase name="RMPhase"/>
                               </phaseOrder>
                               
                               <phaseOrder type="OutFlow">
                                       <phase name="RMPhase"/>
                                       <phase name="soapmonitorPhase"/>
                                       <phase name="OperationOutPhase"/>
                                       <phase name="PolicyDetermination"/>
                                       <phase name="MessageOut"/>
                                       <phase name="Security"/>
                               </phaseOrder>
                                       
                               <phaseOrder type="InFaultFlow">
                                       <phase name="PreDispatch"/>
                                       <phase name="Dispatch" />
                                       <phase name="OperationInFaultPhase"/>
                                       <phase name="soapmonitorPhase"/>
                                       <phase name="RMPhase"/>
                               </phaseOrder>
                               
                               <phaseOrder type="OutFaultFlow">
                                       <phase name="RMPhase"/>
                                       <phase name="soapmonitorPhase"/>
                                       <phase name="OperationOutFaultPhase"/>
                                       <phase name="PolicyDetermination"/>
                                       <phase name="MessageOut"/>
                               </phaseOrder>
                       
                       </axisconfig>
                   
                   
   ```
4. Download the Sandesha2 binary distribution and extract it to a
   temporary folder (from now on referred as SANDESHA2\_HOME).
5. Get the Sandesha2 module file (sandesha2-<VERSION>.mar) from
   SANDESHA2\_HOME directory and put it to the AXIS2\_HOME/repository/modules
   directory.
6. Get the Sandesha2-policy-<VERSION>.jar file from the
   SANDESHA2\_HOME directory and put it to the AXIS2\_HOME/lib directory.
7. Build the Axis2 web application by moving to the AXIS2\_HOME/webapp
   directory and running the ant target 'create.war'. Deploy the generated
   axis2.war.

<a id="axis-apache-org-axis2-java-sandesha-userguide--your_first_service"></a>

## Creating and deploying a RM enabled Web service - SimpleService<a id="axis-apache-org-axis2-java-sandesha-userguide--Creating_and_deploying_a_RM_enabled_Web_service_-_SimpleService"></a>

This section will give you step by step guidelines on creating a Web
service with Reliable Messaging support and making it available within your
Axis2 server. This simple service will have a single one-way operation (ping)
and a request-response operation (echoString). We assume that you have
followed the previous steps to install Sandesha2 and you have installed and
configured JDK 1.4 or later in your system. We also assume you have the
ability to compile & run a simple java class.

1. Add all the files that come under the AXIS2\_HOME/lib to your
   CLASSPATH.
2. Add the Sandesha2 jar file (sandesha2-<VERSION>.jar) from your
   SANDESHA2\_HOME to your CLASSPATH.
3. Create the service implementation java file as given below and
   compile it.

   ```java
               
   package sandesha2.samples.userguide;

   import java.util.HashMap;
   import java.util.Map;
   import javax.xml.namespace.QName;
   import org.apache.axiom.om.OMAbstractFactory;
   import org.apache.axiom.om.OMElement;
   import org.apache.axiom.om.OMFactory;
   import org.apache.axiom.om.OMNamespace;

   public class RMSampleService {

           private static Map sequenceStrings = new HashMap();
           private final String applicationNamespaceName = "http://tempuri.org/"; 
           private final String Text = "Text";
           private final String Sequence = "Sequence";
           private final String echoStringResponse = "echoStringResponse";
           private final String EchoStringReturn = "EchoStringReturn";
           
           public OMElement echoString(OMElement in) throws Exception {
                   
                   OMElement textElem = in.getFirstChildWithName(new QName (applicationNamespaceName,Text));
                   OMElement sequenceElem = in.getFirstChildWithName(new QName (applicationNamespaceName,Sequence));
                   
                   if (textElem==null)
                           throw new Exception ("'Text' element is not present as a child of the 'echoString' element");
                   if (sequenceElem==null)
                           throw new Exception ("'Sequence' element is not present as a child of the 'echoString' element");
                   
                   String textStr = textElem.getText();
                   String sequenceStr = sequenceElem.getText();
                   
                   System.out.println("'EchoString' service got text '" + textStr + "' for the sequence '" + sequenceStr + "'");
                   
                   String previousText = (String) sequenceStrings.get(sequenceStr);
                   String resultText = (previousText==null)?textStr:previousText+textStr;
                   sequenceStrings.put(sequenceStr,resultText);
                   
                   OMFactory fac = OMAbstractFactory.getOMFactory();
                   OMNamespace applicationNamespace = fac.createOMNamespace(applicationNamespaceName,"ns1");
                   OMElement echoStringResponseElem = fac.createOMElement(echoStringResponse, applicationNamespace);
                   OMElement echoStringReturnElem = fac.createOMElement(EchoStringReturn, applicationNamespace);
                   
                   echoStringReturnElem.setText(resultText);
                   echoStringResponseElem.addChild(echoStringReturnElem);
                   
                   return echoStringResponseElem;
           }
     
           public void ping(OMElement in) throws Exception  {
                   OMElement textElem = in.getFirstChildWithName(new QName (applicationNamespaceName,Text));
                   if (textElem==null)
                           throw new Exception ("'Text' element is not present as a child of the 'Ping' element");
                   
                   String textValue = textElem.getText();
                   
                   System.out.println("ping service got text:" + textValue);
           }
           
   }
               
   ```
4. Create your services.xml file as following.

   ```xml
           
   <service name="RMSampleService">

      <parameter name="ServiceClass" locked="xsd:false">sandesha2.samples.userguide.RMSampleService</parameter>
      
       <description>
           The userguide Sample service.
       </description>
       
       <module ref="sandesha2" />
       <module ref="addressing" />
           
       <operation name="ping" mep="http://www.w3.org/2004/08/wsdl/in-only">  
           <messageReceiver class="org.apache.axis2.receivers.RawXMLINOnlyMessageReceiver" />
       </operation>
       <operation name="echoString">
           <messageReceiver class="org.apache.axis2.receivers.RawXMLINOutMessageReceiver" />
       </operation>
       
   </service>        
           
           
   ```
5. Set RMSampleService.class and the services.xml files obtained from
   the previous steps in the folder structure below. Create a
   RMSampleService.aar file by compressing it using the jar or a zip
   tool.

   ```text
                 RMSampleService.aar
                      |
                      |--META-INF
                      |    |--services.xml
                      |-sandesha2
                           |--samples
                                |--userguide
                                    |--RMSampleService.class
       
   ```
6. To deploy the web service simply drop it to the
   WEB-INF/repository/services direcory of your Axis2 web application.
   Please read the Axis2 userguide for more information on deploying Web
   service archieves.
7. List the Axis2 services to see whether the SimpleService has been
   correctly deployed.
8. Congradulations. You just deployed your first Web Service with
   Reliable Messaging support from Sandesha2.

<a id="axis-apache-org-axis2-java-sandesha-userguide--writing_clients"></a>

## Writing Clients for Reliable Services<a id="axis-apache-org-axis2-java-sandesha-userguide--Writing_Clients_for_Reliable_Services"></a>

<a id="axis-apache-org-axis2-java-sandesha-userguide--client_repo"></a>

### Configuring the client repository<a id="axis-apache-org-axis2-java-sandesha-userguide--Configuring_the_client_repository"></a>

1. Create a repository directory in your system to be used as the Axis2
   client repository (from now on referred as CLIENT\_REPO).
2. Put the configured the axis2.xml file from the previous section to the
   CLIENT\_REPO directory & rename it as client\_axis2.xml.
3. Create a directory named 'modules' under the CLIENT\_REPO.
4. Put the Sandesha mar (sandesha2-<VERSION>.mar) file from the
   Sandesha2 binary distribution and the addressing mar file from the Axis2
   binary distribution (addressing-<VERSION>.mar) to the
   CLIENT\_REPO/modules directory.
5. Now you should have the following folder structure in the CLIENT\_REPO.

   ```text
          Client_Repo  
               |-- client_axis2.xml
               |--modules
                     |--sandesha2.mar
                     |--addressing.mar
           
   ```

<a id="axis-apache-org-axis2-java-sandesha-userguide--one_way"></a>

### Doing One-Way invocation<a id="axis-apache-org-axis2-java-sandesha-userguide--Doing_One-Way_invocation"></a>

1. Add all the jar files fromAXIS2\_HOME/lib directory to your
   CLASSPATH.
2. Put the Sandesha2 jar file (sandesha2-<VERSION>.jar) to your
   CLASSPATH.
3. Create a 'UserguidePingClient.java' file with following content.

   ```java
   package sandesha2.samples.userguide;

   import java.io.File;
   import org.apache.axiom.om.OMAbstractFactory;
   import org.apache.axiom.om.OMElement;
   import org.apache.axiom.om.OMFactory;
   import org.apache.axiom.om.OMNamespace;
   import org.apache.axis2.AxisFault;
   import org.apache.axis2.addressing.EndpointReference;
   import org.apache.axis2.client.Options;
   import org.apache.axis2.client.ServiceClient;
   import org.apache.axis2.context.ConfigurationContext;
   import org.apache.axis2.context.ConfigurationContextFactory;
   import org.apache.sandesha2.client.SandeshaClientConstants;
   import javax.xml.namespace.QName;

   public class UserguidePingClient {

           private static final String applicationNamespaceName = "http://tempuri.org/"; 
           private static final String ping = "ping";
           private static final String Text = "Text";
           private static String toEPR = "http://127.0.0.1:8070/axis2/services/RMSampleService";

           private static String CLIENT_REPO_PATH = "CLIENT_REPO";
           
           public static void main(String[] args) throws AxisFault {
                   
                   String axis2_xml = CLIENT_REPO_PATH + File.separator + "client_axis2.xml";
                   ConfigurationContext configContext = ConfigurationContextFactory.createConfigurationContextFromFileSystem(CLIENT_REPO_PATH,axis2_xml);
                   
                   Options clientOptions = new Options ();
                   clientOptions.setTo(new EndpointReference (toEPR));

                   ServiceClient serviceClient = new ServiceClient (configContext,null);
                   clientOptions.setAction("urn:wsrm:Ping");
                   serviceClient.setOptions(clientOptions);
                   
                   serviceClient.engageModule(new QName ("sandesha2"));
                   serviceClient.engageModule(new QName ("addressing"));
                   
                   serviceClient.fireAndForget(getPingOMBlock("ping1"));
                   serviceClient.fireAndForget(getPingOMBlock("ping2"));
                   
                   clientOptions.setProperty(SandeshaClientConstants.LAST_MESSAGE, "true");
                   serviceClient.fireAndForget(getPingOMBlock("ping3"));
                   
                   serviceClient.cleanup();
           }
           
           private static OMElement getPingOMBlock(String text) {
                   OMFactory fac = OMAbstractFactory.getOMFactory();
                   OMNamespace namespace = fac.createOMNamespace(applicationNamespaceName,"ns1");
                   OMElement pingElem = fac.createOMElement(ping, namespace);
                   OMElement textElem = fac.createOMElement(Text, namespace);
                   
                   textElem.setText(text);
                   pingElem.addChild(textElem);

                   return pingElem;
           }
   }
       
   ```
4. In the above file replace the value of the CLIENT\_REPO\_PATH variable
   with the full path to the CLIENT\_REPO folder. This should be in a JAVA
   compatible manner. e.g.: c:\\sandesha2\\repository
5. Compile the class
6. Run it.
7. Observe the following differences between the RM client code and a
   normal Axis2 one-way client invocation.

   - Engaging Sandesha2 module.

     serviceClient.engageModule(new QName ("sandesha2"));
   - Engaging Addressing module

     serviceClient.engageModule(new QName ("addressing"));
   - Setting the LAST\_MESSAGE property before doing the last invocation.

     clientOptions.setProperty(SandeshaClientConstants.LAST\_MESSAGE,
     "true");

<a id="axis-apache-org-axis2-java-sandesha-userguide--request_reply"></a>

### Doing Request-Reply invocation<a id="axis-apache-org-axis2-java-sandesha-userguide--Doing_Request-Reply_invocation"></a>

1. Add all the jar files from the AXIS2\_HOME/lib directory to your
   CLASSPATH.
2. Put the Sandesha2 jar file (sandesha2-<VERSION>.jar) to your
   CLASSPATH.
3. Create a 'UserguideEchoClient.java' file with following content.

   ```java
   package sandesha2.samples.userguide;

   import java.io.File;
   import javax.xml.namespace.QName;
   import org.apache.axiom.om.OMAbstractFactory;
   import org.apache.axiom.om.OMElement;
   import org.apache.axiom.om.OMFactory;
   import org.apache.axiom.om.OMNamespace;
   import org.apache.axiom.soap.SOAPBody;
   import org.apache.axis2.addressing.EndpointReference;
   import org.apache.axis2.client.Options;
   import org.apache.axis2.client.ServiceClient;
   import org.apache.axis2.client.async.AsyncResult;
   import org.apache.axis2.client.async.Callback;
   import org.apache.axis2.context.ConfigurationContext;
   import org.apache.axis2.context.ConfigurationContextFactory;
   import org.apache.sandesha2.client.SandeshaClientConstants;

   public class UserguideEchoClient {
           
           private final static String applicationNamespaceName = "http://tempuri.org/"; 
           private final static String echoString = "echoString";
           private final static String Text = "Text";
           private final static String Sequence = "Sequence";
           private final static String echoStringResponse = "echoStringResponse";
           private final static String EchoStringReturn = "EchoStringReturn";
           private static String toEPR = "http://127.0.0.1:8070/axis2/services/RMSampleService";

           private static String CLIENT_REPO_PATH = "Full path to the Client Repo folder";
           
           public static void main(String[] args) throws Exception {
                   
                   String axis2_xml = CLIENT_REPO_PATH + File.separator +"client_axis2.xml";
           ConfigurationContext configContext = ConfigurationContextFactory.createConfigurationContextFromFileSystem(CLIENT_REPO_PATH,axis2_xml);
                   ServiceClient serviceClient = new ServiceClient (configContext,null);        
                   
                   Options clientOptions = new Options ();
                   clientOptions.setTo(new EndpointReference (toEPR));
                   clientOptions.setUseSeparateListener(true);
                   serviceClient.setOptions(clientOptions);

                   serviceClient.engageModule(new QName ("sandesha2"));
                   serviceClient.engageModule(new QName ("addressing"));

                   Callback callback1 = new TestCallback ("Callback 1");
                   serviceClient.sendReceiveNonBlocking (getEchoOMBlock("echo1","sequence1"),callback1);
                   Callback callback2 = new TestCallback ("Callback 2");
                   serviceClient.sendReceiveNonBlocking(getEchoOMBlock("echo2","sequence1"),callback2);

                   clientOptions.setProperty(SandeshaClientConstants.LAST_MESSAGE, "true");
                   Callback callback3 = new TestCallback ("Callback 3");
                   serviceClient.sendReceiveNonBlocking(getEchoOMBlock("echo3","sequence1"),callback3);
                   
           while (!callback3.isComplete()) {
               Thread.sleep(1000);
           }
           
           Thread.sleep(4000); 
           }

           private static OMElement getEchoOMBlock(String text, String sequenceKey) {
                   OMFactory fac = OMAbstractFactory.getOMFactory();
                   OMNamespace applicationNamespace = fac.createOMNamespace(applicationNamespaceName,"ns1");
                   OMElement echoStringElement = fac.createOMElement(echoString, applicationNamespace);
                   OMElement textElem = fac.createOMElement(Text,applicationNamespace);
                   OMElement sequenceElem = fac.createOMElement(Sequence,applicationNamespace);
                   
                   textElem.setText(text);
                   sequenceElem.setText(sequenceKey);
                   echoStringElement.addChild(textElem);
                   echoStringElement.addChild(sequenceElem);
                   
                   return echoStringElement;
           }

           static class TestCallback extends Callback {

                   String name = null;
                   public TestCallback (String name) {
                           this.name = name;
                   }
                   
                   public void onComplete(AsyncResult result) {
                           SOAPBody body = result.getResponseEnvelope().getBody();
                           
                           OMElement echoStringResponseElem = body.getFirstChildWithName(new QName (applicationNamespaceName,echoStringResponse));                        
                           OMElement echoStringReturnElem = echoStringResponseElem.getFirstChildWithName(new QName (applicationNamespaceName,EchoStringReturn));
                           
                           String resultStr = echoStringReturnElem.getText();
                           System.out.println("Callback '" + name +  "' got result:" + resultStr);
                   }

                   public void onError (Exception e) {
                           System.out.println("Error reported for test call back");
                           e.printStackTrace();
                   }
           }
   }
       
   ```
4. In the above file replace the value of the CLIENT\_REPO\_PATH variable
   with the full path to the CLIENT\_REPO folder. This should be in a JAVA
   compatible manner. e.g.: c:\\sandesha2\\repository
5. Compile the above class.
6. Run the above class.
7. Observer the following differences between the RM client code and a
   normal Axis2 one-way client invocation.
   - Engaging Sandesha2 module.
   - Engaging Addressing module
   - Setting the LAST\_MESSAGE property before doing the last
     invocation.

<a id="axis-apache-org-axis2-java-sandesha-userguide--client_api"></a>

## Sandesha2 Client API<a id="axis-apache-org-axis2-java-sandesha-userguide--Sandesha2_Client_API"></a>

This section will introduce you to some client API features which you may
not use for general cases. These features will be useful if you have some
knowledge in WSRM (Web service Reliable Messaging) and if you want to
customize the default behavior of Sandesha2 to make it work according to your
requirements. Some of these have to be done by simply setting a property in
the 'Options' object which you set to your ServiceClient. For these you have
to add the sandesha2-client-<VERSION>.jar to your classpath. For
others, you have to use a special class called SandeshaClient, which is
available in the Sandesha-<VERSION>.jar file. Both these comes with
Sandesha2 distributions.

<a id="axis-apache-org-axis2-java-sandesha-userguide--version"></a>

### Selecting your RM version<a id="axis-apache-org-axis2-java-sandesha-userguide--Selecting_your_RM_version"></a>

As it was explained earlier Sandesha2 supports two WSRM specifications.
The default is the submitted WSRM specification. But if you want to change
this and work in the new OASIS WSRM specification, set the following property
in the Options object.

```text
clientOptions.setProperty(SandeshaClientConstants.RM_SPEC_VERSION,Sandesha2Constants.SPEC_VERSIONS.v1_1);
```

To go back to the WSRM submitted specification set the property as
follows.

```text
clientOptions.setProperty(SandeshaClientConstants.RM_SPEC_VERSION,Sandesha2Constants.SPEC_VERSIONS.v1_0);
```

<a id="axis-apache-org-axis2-java-sandesha-userguide--acks"></a>

### **Getting Acknowledgements and Faults to a Given Endpoint**<a id="axis-apache-org-axis2-java-sandesha-userguide--Getting_Acknowledgements_and_Faults_to_a_Given_Endpoint"></a>

In the default configuration, response path for acknowledgements and
faults related to a sequence is the anonymous endpoint. For example, HTTP
transport will send acknowledgements and faults in the HTTP response of
request messages. If you want to avoid this and if you want to get
acknowledgements and faults to a different endpoint, add following part to
the client code before doing any invocation. Note that this does not effect
the path of your application level faults. Only RM faults which occur within
the Sandesha2 will be sent to this endpoint.

```text
clientOptions.setTransportInProtocol(org.apache.axis2.Constants.TRANSPORT_HTTP);
clientOptions.setProperty(SandeshaClientConstants.AcksTo,<endpoint>); //example endpoint - http://tempuri.org/acks.
```

<a id="axis-apache-org-axis2-java-sandesha-userguide--managing"></a>

### **Managing Sequences**<a id="axis-apache-org-axis2-java-sandesha-userguide--Managing_Sequences"></a>

In the default behaviour Sandesha2 assumes that messages going to the same
endpoint should go in the same RM sequence. Messages will be sent in
different RM sequences only if their WS-Addressing To address is different.
But if required you can instruct Sandesha2 to send messages that have the
same WS-Addressing To address in two or more sequences. To do this you have
to set a property called Sequence Key.

```text
clientOptions.setProperty(SandeshaClientConstants.SEQUENCE_KEY,<a string to identify the sequence>);
```

If the sequence key is different, Sandesha2 will send messages in two
sequences even if they are sent to the same endpoint.

<a id="axis-apache-org-axis2-java-sandesha-userguide--offering"></a>

### Offering a Sequence ID for the Response Sequence<a id="axis-apache-org-axis2-java-sandesha-userguide--Offering_a_Sequence_ID_for_the_Response_Sequence"></a>

This is a concept of reliable messaging which may not be very useful to
you as a end user. Here what you do is offering a sequence ID for the
sequence to be created in the response side within the Create Sequence
Request message of the request path. If you provide this and if the Sandesha2
server accepts the offered sequence ID it can refrain from doing the Create
Sequence message exchange in the response path. To do this, add the following
to the client code.

```text
clientOptions.setProperty(SandeshaClientConstants.OFFERED_SEQUENCE_ID,<new uuid>);
```

<a id="axis-apache-org-axis2-java-sandesha-userguide--creating"></a>

### Creating a Sequence Without Sending any Messages<a id="axis-apache-org-axis2-java-sandesha-userguide--Creating_a_Sequence_Without_Sending_any_Messages"></a>

Sometimes you may need Sandesha2 client to start a sequence with a server
without sending any application messages. When you ask for this, Sandesha2
will do a Create Sequence message exchange and obtain a new sequence ID from
the server. The sequenceKey value of the newly created sequence will be
returned from this method which could be used do message invocations with it.
This method also has a boolean parameter which tells whether to offer a
sequence for the response side. (read the part on [offering
sequence IDs](#axis-apache-org-axis2-java-sandesha-userguide--oas) to learn more about offering sequences) . The line you have
to add to your client code for creating a sequence is as follows.

```text
String sequenceKey = SandeshaClient.createSequence (ServiceClient serviceClient, booleanoffer);
```

There is an overloaded method of this which takes the sequenceKey from the
user.

```text
SandeshaClient.createSequnce (ServiceClient serviceClient, boolean offer,String sequenceKey);
```

<a id="axis-apache-org-axis2-java-sandesha-userguide--ack_requests"></a>

### Sending Acknowledgement Requests from the Client Code<a id="axis-apache-org-axis2-java-sandesha-userguide--Sending_Acknowledgement_Requests_from_the_Client_Code"></a>

You can ask Sandesha2 to get an acknowledgement from a server with which
it is maintaining a sequence. This may be useful in a case where your [SequenceReports](#axis-apache-org-axis2-java-sandesha-userguide--sequencereport) indicate that some of the messages
you sent have not been acknowledged and when you want to verify that. You can
do this by adding following line to the client code.

```text
SandeshaClient.sendAckRequest (ServiceClient serviceClient);
```

You can use following method to send an acknowledgement request to a
specific sequence identified by the Sequence Key.

```text
SandeshaClient.sendAckRequest (ServiceClient serviceClient, String sequenceKey);
```

<a id="axis-apache-org-axis2-java-sandesha-userguide--terminating"></a>

### Terminating a Sequence from the Client Code<a id="axis-apache-org-axis2-java-sandesha-userguide--Terminating_a_Sequence_from_the_Client_Code"></a>

You can terminate an on going sequence at any time by adding the line
given in this section to your client code. Remember that if you terminate a
sequence some of your messages may not get delivered to the service.

```text
SandeshaClient.terminateSequence (ServiceClient serviceClient);
```

To terminate a specific sequence identified by a sequenceKey use
following.

```text
SandeshaClient.terminateSequence (ServiceClient serviceClient, String sequenceKey);
```

<a id="axis-apache-org-axis2-java-sandesha-userguide--closing"></a>

### Closing a Sequence from the Client Code<a id="axis-apache-org-axis2-java-sandesha-userguide--Closing_a_Sequence_from_the_Client_Code"></a>

You can close an ongoing sequence at any time by adding the line given in
this section to your client code. Sequence close feature is only available
for new WSRM specification being developed under OASIS. Remember that if you
do not close elegantly, some of your messages may not get delivered to the
service. Again, see the section on [More about
sequences](#axis-apache-org-axis2-java-sandesha-userguide--sequences) for more details. You can issue following command from your
client code to close a sequence.

```text
SandeshaClient.closeSequence (ServiceClient serviceClient);
```

To close a specific sequence identified by a sequence key use the
following

```text
SandeshaClient.closeSequence (ServiceClient serviceClient, String sequenceKey);
```

<a id="axis-apache-org-axis2-java-sandesha-userguide--blocking"></a>

### Blocking the Client Code until a Sequence is Complete<a id="axis-apache-org-axis2-java-sandesha-userguide--Blocking_the_Client_Code_until_a_Sequence_is_Complete"></a>

After your client code delivered some messages to the RM layer, you may
have to wait for some time until the RM layer does its work. The time you
have to block depends on your system performance and network latencies. It
may be easier to ask the RM layer to block until its work is done by issuing
one of the following commands in your client code.

```text
SandeshaClient.waitUntilSequenceCompleted (ServiceClient serviceClient);

SandeshaClient.waitUntilSequenceCompleted (ServiceClient serviceClient, String sequenceKey);
```

You can also give the maximum number of seconds the RM Layer should block.
The blocking will stop at the end of this maximum time even if the sequence
is not terminated or not timed out. But note that internally RM is still
working. So even though the blocking stops, RM layer will continue its work
until you exit the program.

```text
SandeshaClient.waitUntilSequenceCompleted (ServiceClient serviceClient, long maxWaitingTime);

SandeshaClient.waitUntilSequenceCompleted (ServiceClient serviceClient, long maxWaitingTime, String sequenceKey);
```

<a id="axis-apache-org-axis2-java-sandesha-userguide--reports"></a>

### Working with Sandesha Reports<a id="axis-apache-org-axis2-java-sandesha-userguide--Working_with_Sandesha_Reports"></a>

Sandesha introduces a feature called Sandesha Reports with which you can
get status information about the sequences managed by Sandesha2. There are
basically two kinds of reports, each explained in following subtopics.

<a id="axis-apache-org-axis2-java-sandesha-userguide--sandesha_reports"></a>

#### SandeshaReport<a id="axis-apache-org-axis2-java-sandesha-userguide--SandeshaReport"></a>

This gives information on all the incoming and outgoing sequences
Sandesha2 system is managing. When we consider a particular endpoint, an
incoming sequence is a sequence to which that endpoint is working as a
RM-Destination (RMD). An outgoing sequence is a sequence to which this
endpoint works as a RM-Source (RMS).

A SandeshaReport include following information:

- Sequence IDs of all the outgoing sequences.
- Number of completed messages of each outgoing sequences.
- Sequence IDs of all the incoming sequences.
- No of completed messages of each incoming sequence.

To get a SandeshaReport at any time, invoke following method from your
client code.

```text
SandeshaClient.getSandeshaReport (ConfigurationContext c);
```

<a id="axis-apache-org-axis2-java-sandesha-userguide--sequence_reports"></a>

#### SequenceReport<a id="axis-apache-org-axis2-java-sandesha-userguide--SequenceReport"></a>

A SequenceReport gives information on a specific sequences that a Sandesha
system is working on. This can be an incoming sequence or an outgoing
sequence.

A SequenceReport will give following information:

1. Status of the sequence which can be one of the following.
   - INITIAL - The sequence has not been established yet.
   - ESTABLISHED - Create Sequence / Create Sequence Response message
     exchange has been done.
   - TERMINATED - The sequence has been terminated.
   - TIMEDOUT - The sequence has timed out.
   - UNKNOWN - The status cannot be determined.
2. Sequence Direction
   - OUT - Outgoing sequence
   - IN - Incoming sequence
3. Sequence ID of the sequence
4. Internal sequence ID of the sequence.
5. Number of completed messages of the sequence.

A messages is considered as **completed** when a RMS has
successfully sent the message to the RMD and received an acknowledgement.

To get an incoming sequence report, you have to issue following command
from your client code.

```text
SandeshaClient.getIncomingSequenceReports (ConfigurationContext configCtx);
```

To get an outgoing Sequence Report you can invoke any of the following
functions.

```text
SandeshaClient.getOutgoingSequenceReport (ServiceClient serviceClient);
SandeshaClient.getOutgoingSequenceReport (String to,String sequenceKey,ConfigurationContext configurationContext);
SandeshaClient.getOutgoingSequenceReport (String internalSequenceID,ConfigurationContext configurationContext);
```

<a id="axis-apache-org-axis2-java-sandesha-userguide--listners"></a>

### Sandesha Listener Feature<a id="axis-apache-org-axis2-java-sandesha-userguide--Sandesha_Listener_Feature"></a>

You can use this new feature to register a listener class in Sandesha2 and
get notified when specific event happens in the system. The basic interface
is given below.

```java
public interface SandeshaListener {
    public void onError(AxisFault fault);
    public void onTimeOut(SequenceReport report);
}
```

You can implement this class and set an object of that type as a property
in the Options object in your client code. An example is given below.

```java
options.setProperty (SandeshaClientConstants.SANDESHA_LISTENER, new SandeshaListnerImpl ());
```

Currently SandeshaListener defines the following two methods- onError
& onTimedOut.

<a id="axis-apache-org-axis2-java-sandesha-userguide--on_error"></a>

### onError<a id="axis-apache-org-axis2-java-sandesha-userguide--onError"></a>

This will be invoked if Sandesha2 receives a fault SOAP message. The parameter will be an AxisFault
representing that fault message. this will be specially useful for capturing the faults that get returned
due to RM protocol messages, since they do not get returned to the client code.

<a id="axis-apache-org-axis2-java-sandesha-userguide--on_timeout"></a>

### onTimeOut<a id="axis-apache-org-axis2-java-sandesha-userguide--onTimeOut"></a>

As mentioned in the earlier section [Sequence Management of
Sanesha2](#axis-apache-org-axis2-java-sandesha-userguide--smo), there is a possibility of an inactive sequence timing out. When
a specific sequence times out, this method of the SandeshaListener will be
invoked giving a report of that sequence as a parameter.

<a id="axis-apache-org-axis2-java-sandesha-userguide--sequences"></a>

## More about sequences<a id="axis-apache-org-axis2-java-sandesha-userguide--More_about_sequences"></a>

This section will explain you about the sequence management method of
Sandesha2. This is basically about four things, each explained in following
sub topics.

<a id="axis-apache-org-axis2-java-sandesha-userguide--creation"></a>

### Creation of sequences<a id="axis-apache-org-axis2-java-sandesha-userguide--Creation_of_sequences"></a>

Sandesha client uses two properties given by the client to decide the
sequence in which it should send a particular application message. First one
is the address of the WS-Addressing To endpoint address. The second is a
special constant given by the client called Sequence Key which is set as a
property in the Options object as it was explained before. Sandesha2 client
generates a value called Internal Sequence ID by combining these two values.
All messages having the same Internal Sequence ID will be sent in a single
sequence, until that particular sequence is terminated.

Sequences that carry messages from the client to a server are called
request sequences and ones that carry messages from the server to the client
are called response sequences. Sandesha2 always keep a single response
sequence corresponding to a particular request sequence.

<a id="axis-apache-org-axis2-java-sandesha-userguide--termination"></a>

### Termination of sequences<a id="axis-apache-org-axis2-java-sandesha-userguide--Termination_of_sequences"></a>

There are currently two methods to terminate a particular sequence from
the Client API. The first method is to [set the Last
Message property](#axis-apache-org-axis2-java-sandesha-userguide--lastmessage) as it was explained earlier. After all the messages up
to the last message get delivered reliably Sandesha2 will terminate that
sequence. Remember that if you are working on the Submitted WSRM
specification (the default), this is the only method you can use.

If you are working on the new WSRM specification (see previous section on
[Selecting the Specification Version](#axis-apache-org-axis2-java-sandesha-userguide--sts) if you want to know
how to set this), these is an alternate method you can use to terminate a
sequence. You can keep invoking the ServiceClient to send messages, without
setting a Last Message property. After you finish your work call following
function to terminate the sequence.

```text
SandeshaClient.terminateSequence (ServiceClient);
```

You can use the function below to terminate a sequence identified by a
particular Sequence Key.

```text
SandeshaClient.terminateSequence (ServiceClient, SequenceKey);
```

When a request sequence is terminated, Sandesha2 will wait till all the
response messages are reliably delivered to the client and after which will
terminate the response sequence as well.

<a id="axis-apache-org-axis2-java-sandesha-userguide--closing1"></a>

### Closing of sequences<a id="axis-apache-org-axis2-java-sandesha-userguide--Closing_of_sequences"></a>

New WSRM specification being developed under OASIS introduces a new
feature called closing a sequence. When a sequence is closed the server will
not except new application messages, but will accept RM control messages like
acknowledgement requests. If you are writing your code for this RM version
you can use following functions to close the current sequence.

```text
SandeshaClient.closeSequence (ServiceClient);
```

You can use the function below to close a sequence identified by a
particular Sequence Key.

```text
SandeshaClient.terminateSequence (ServiceClient,, SequenceKey);
```

<a id="axis-apache-org-axis2-java-sandesha-userguide--timing_out"></a>

### Timing Out a Sequence<a id="axis-apache-org-axis2-java-sandesha-userguide--Timing_Out_a_Sequence"></a>

Depending on its policy configurations Sandesha2 may time out certain
sequences. After a sequence get timed out, it is considered finalized and
cannot be used any more. There are basically two ways a sequence can time
out, and both can be configured using policies. See '[InactivityTimeout](#axis-apache-org-axis2-java-sandesha-userguide--inactivitytimeout)' and '[MaximumRetransmissionCount](#axis-apache-org-axis2-java-sandesha-userguide--maximumretransmissioncount)' parts of
the '[Configuring Sandesha2](#axis-apache-org-axis2-java-sandesha-userguide--cs)' sub topic for more details.

<a id="axis-apache-org-axis2-java-sandesha-userguide--wws"></a> <a id="axis-apache-org-axis2-java-sandesha-userguide--delivary_assurances"></a>

## Delivery Assurances of Sandesha2<a id="axis-apache-org-axis2-java-sandesha-userguide--Delivery_Assurances_of_Sandesha2"></a>

As it was mentioned in the [Architecture
Guide](#axis-apache-org-axis2-java-sandesha-architectureguide), Sandesha2 provide an in-order exactly-once delivery assurance.
**In-order** means that Sandesha2 will guarantee delivering of
the messages to the Web service in the order of their message numbers. If you
use a Sandesha2 client this will be the order you called the invocation
methods of your service client. **Exactly-once** delivery
assurance means that Sandesha2 will make sure that the service will be
invoked only once for each message. As it was mentioned earlier Sandesha2
retransmits messages to obtain reliability. Due to the exactly-once delivery
assurance you can be sure that your service gets invoked only once.

If you require the performance to be maximized and if you do not want
ordering, you can configure Sandesha2 to invoke messages in the order they
arrive. Read 'Configuring Sandesha2' section below to learn how to do
this.

<a id="axis-apache-org-axis2-java-sandesha-userguide--configuring"></a>

## Configuring Sandesha2<a id="axis-apache-org-axis2-java-sandesha-userguide--Configuring_Sandesha2"></a>

Sandesha2 provides a set of configurations which you can use to customize
its execution behavior. All these configurations are available in a WS-Policy
format. These policies can be in the module.xml file of the Sandesha module
or in the services.xml file of a service on which Sandesha2 module has been
engaged. Most of the policies in the module.xml can be overridden by setting
different values in a services.xml. But some policies cannot be overridden
and must be set correctly in the module.xml file.

You will find each Sandesha2 policy and the way an alteration of it can
effect Sandesha2. Make sure that you set these values carefully. Setting
incompatible types or values may cause Sandesha system to malfunction.
Normally if Sandesha2 can detect that the value you have set is incompatible,
it will set a default value which is mentioned in the SandeshaConstants
class.

<a id="axis-apache-org-axis2-java-sandesha-userguide--acknowledgementinterval"></a>

### AcknowledgementInterval<a id="axis-apache-org-axis2-java-sandesha-userguide--AcknowledgementInterval"></a>

When a RMD receives an application message and when it has to send
acknowledgements to an endpoint (other than the anonymous URL), it will not
send this message immediately but will wait for some time to see whether
there are any other messages (for example application response messages)
going towards the destination of the acknowledgement message. If it finds
any, the acknowledgement message is piggybacked in this second message and
both are sent together. If the RMD does not find any messages that go towards
the destination of the acknowledgement within a specific time interval, the
acknowledgement is sent as a stand alone message. This time interval is
called the **acknowledgement interval** and can be configured in
Sandesha2 policies. The measurement unit is in milliseconds.

<a id="axis-apache-org-axis2-java-sandesha-userguide--retransmissioninterval"></a>

### RetransmissionInterval<a id="axis-apache-org-axis2-java-sandesha-userguide--RetransmissionInterval"></a>

As it was mentioned earlier some messages in RM should be retransmitted
until a proper response or acknowledgement is returned. After sending a
message once, the RMS will wait for some time before sending it for the
second time. This waiting time between the first and second retransmission
attempts is given by this policy. If the policy given later called the
ExponentialBackoff is set to false the time gap between all the
retransmissions attempts will have the same value, which is the
RetransmissionInterval. Measurement unit is in milliseconds.

<a id="axis-apache-org-axis2-java-sandesha-userguide--exponentialbackoff"></a>

### ExponentialBackoff<a id="axis-apache-org-axis2-java-sandesha-userguide--ExponentialBackoff"></a>

Value of this can either be 'true' or 'false'. This measure is used to
adjust the retransmission attempts so that an RMD does not get flooded with a
large number of retransmitted messages. If this is 'true', a time gap between
two retransmissions will be twice as the time gap between previous two
retransmissions. For example, if the time gap between the fourth and fifth
retransmission attempts is twenty seconds the time gap between the fifth and
sixth attempts will be forty seconds. If this property is set to 'false', all
retransmissions will have the same value, which is given by the
'RetransmissionInterval' property.

<a id="axis-apache-org-axis2-java-sandesha-userguide--maximumretransmissioncount"></a>

### MaximumRetransmissionCount<a id="axis-apache-org-axis2-java-sandesha-userguide--MaximumRetransmissionCount"></a>

This gives the maximum number of times a message has to be retransmitted.
When a specific message gets retransmitted a maximum number of times, and is
still not sent correctly to the RMD, it will not be sent again and the
request will be marked as Timed Out. When a sequence is timed out, it cannot
be used any more. If the value of this property is '-1' there is no limit in
the number of retransmission attempts.

<a id="axis-apache-org-axis2-java-sandesha-userguide--inactivitytimeout"></a>

### InactivityTimeout<a id="axis-apache-org-axis2-java-sandesha-userguide--InactivityTimeout"></a>

A Sandesha2 RMS always keeps track of the last time a particular RMD
responded to a request by it. If the RMD does not response within the time
limit given by the time interval given by this measure, the RMS will give up
attempting and will mark the sequence as Timed Out. After timing out the
particular sequence, it cannot be used any more. If the value of this is -1,
there is not inactivity timeout limit The measure of this is given by the
property 'InactivityTimeoutMeasure'.

<a id="axis-apache-org-axis2-java-sandesha-userguide--inactivitytimeoutmeasure"></a>

### InactivityTimeoutMeasure<a id="axis-apache-org-axis2-java-sandesha-userguide--InactivityTimeoutMeasure"></a>

This gives the measure of the property 'InactivityTimeout'. The value of
this can be seconds, minutes, hours or days. If you give a value that cannot
be interpreted the default will be used.

<a id="axis-apache-org-axis2-java-sandesha-userguide--invokeinorder"></a>

### InvokeInOrder<a id="axis-apache-org-axis2-java-sandesha-userguide--InvokeInOrder"></a>

As it was mentioned earlier, Sandesha2 implement the in-order invoking
delivery assurance. This property can be used to turn this on or off. The
value of this has to be 'true' if in-order invoking has to be enabled. It has
to be false if in-order invoking has to be disabled. Please remember that
this is a non-overridable property. I.e. value you set in the module.xml is
the one that is used for all the services and will not be overridden for a
particular service by setting a different value there.

<a id="axis-apache-org-axis2-java-sandesha-userguide--storagemanagers"></a>

### StorageManagers<a id="axis-apache-org-axis2-java-sandesha-userguide--StorageManagers"></a>

This gives the storage manager implementation classes used by Sandesha2.
You have to mention the full qualified class name here.

You can basically define two StorageManagers

InMemoryStorageManager - Expected to store data in memory. Supposed to be
faster.

PermanentStorageManager - Will be storing data in a database. So will be
little slower but real business applications will most probably prefer this
could provide additional features like recovery from failures.

Only one of these StorageManagers will be active in a given Sandesha2
instance. By default it will be the InMemorystoragemanager. If you want to
make the PermanentStoragemanager active add the following property to the
axis2.xml.

<parameter name="Sandesha2StorageManager"
locked="false">persistent</parameter>

StorageManager property is not overridable, i.e., value you set in the
module.xml is the one that is used for all the services and will not be
overridden for a particular service by setting a different value there.

You can easily create your own Storage Manager. Please read the Sandesha2 [Architecture Guide](#axis-apache-org-axis2-java-sandesha-architectureguide) for more details on
this. <a id="axis-apache-org-axis2-java-sandesha-userguide--messagetypestodrop"></a>

### MessageTypesToDrop<a id="axis-apache-org-axis2-java-sandesha-userguide--MessageTypesToDrop"></a>

This is a property that may not be very useful to an end user, but may be
useful for some debug purposes. As it was mentioned earlier Sandesha2 gives a
Message Type to each message it sends. For example, Create Sequence messages
will have the type 1 and Acknowledgement messages will have the type 4. You
can add a comma separated list of integers in the property telling Sandesha2
not to send messages of those types.

<a id="axis-apache-org-axis2-java-sandesha-userguide--securitymanager"></a>

### SecurityManager<a id="axis-apache-org-axis2-java-sandesha-userguide--SecurityManager"></a>

The security manager allows you to plug a certain Secure Conversation
implemenation into axis2. The default is a DummySecurityManager which does
not do any real work. We have also implemented a RamaprtBasedSecurityManager
which allows you to use Rampart as your secure conversation
implementation.

To use the rampart follow the below steps.

1. Add the wss4j jar file (wss4j-VERSION.jar) to your to your
   CLASSPATH. (If your are in the server side add this to the WEB-INF/lib
   folder of the Axis2 webapp).
2. Add the Rampart and Rahas modules to the Axis2 modules directory.
3. Change the SecurityManager policy of the Sandesha2 module.xml to the
   following

   org.apache.sandesha2.security.rampart.RampartBasedSecurityManager

---

Copyright © 2005-2013
[The Apache Software Foundation](http://www.apache.org/).
All Rights Reserved.

---

---

<a id="axis-apache-org-axis2-java-sandesha-architectureguide"></a>

# Sandesha2 - 

Sandesha2 Architecture guide

# Apache Sandesha2 Architecture Guide

## Content<a id="axis-apache-org-axis2-java-sandesha-architectureguide--Content"></a>

- [Introduction](#axis-apache-org-axis2-java-sandesha-architectureguide--intro)
- [Architecture](#axis-apache-org-axis2-java-sandesha-architectureguide--architecture)
  - [Handlers](#axis-apache-org-axis2-java-sandesha-architectureguide--hnd)
    - [SandeshaGlobalInHandler](#axis-apache-org-axis2-java-sandesha-architectureguide--globalin)
    - [SandeshaInHandler](#axis-apache-org-axis2-java-sandesha-architectureguide--in)
    - [SandeshaOutHandler](#axis-apache-org-axis2-java-sandesha-architectureguide--out)
  - [RMMessageReceiver](#axis-apache-org-axis2-java-sandesha-architectureguide--rmm)
  - [Sender](#axis-apache-org-axis2-java-sandesha-architectureguide--sender)
  - [Inorder Invoker](#axis-apache-org-axis2-java-sandesha-architectureguide--ioi)
  - Polling Manager
  - [Storage Framework](#axis-apache-org-axis2-java-sandesha-architectureguide--sf)
- [Delivery Assurances](#axis-apache-org-axis2-java-sandesha-architectureguide--da)
- [Configuring Sandesha](#axis-apache-org-axis2-java-sandesha-architectureguide--config)
- [Example Scenarios](#axis-apache-org-axis2-java-sandesha-architectureguide--es)
  - [Client Side](#axis-apache-org-axis2-java-sandesha-architectureguide--cs)
  - [Server Side](#axis-apache-org-axis2-java-sandesha-architectureguide--ss)

<a id="axis-apache-org-axis2-java-sandesha-architectureguide--intro"></a>

## Introduction<a id="axis-apache-org-axis2-java-sandesha-architectureguide--Introduction"></a>

Sandesha2 gives reliable messaging capabilities to
Axis2. From the point of view of the Axis2 engine, Sandesha2 is a module. When
this module is engaged to a service, clients have the option of invoking it in a
reliable manner. In the client side Sandesha2 module can be used to interact
with existing reliable Web services.

According to the Web service-ReliableMessaging (WS-RM)
specification which is implemented by Sandesha2, reliable communication happens
between two endpoints. These endpoints are called the RM Source (RMS) and the RM
Destination (RMD). Before communication, RMS and RMD perform a message exchange
to create a relationship called a Sequence between them. A Sequence is always
identified by a unique Sequence Identifier.

Each message of a sequence is numbered, starting from one. In
Sandesha2 the maximum number of messages a sequence can support is 2 64
(size of *long*
data type). Of course practically this may be limited by the memory available
for your system . The message number is used by the destination to support
additional delivery assurances. This will be explained later in this tutorial.

The reliability is obtained basically using
acknowledgements. RMS is required to send each message one or more times to the
RMD. RMD sends back acknowledgements to notify the successful reception of
messages. After receiving an acknowledgement for a certain message RMS can stop
the retransmission of that message.

When all messages of a certain sequence have been
successfully transmitted to RMD, RMS sends a TerminateSequence message. If RMD
receives this message it can free any resources allocated for this sequence.
Otherwise resource de-allocation will happen based on a timeout.

**Following diagram explains operation of the RMS
and the RMD**.

![WS-RM Model](axis.apache.org/axis2/java/sandesha/images/RMModel.jpg)
Sandesha2 supports two reliable messaging specifications. It
fully supports the WS-ReliableMessaging February 2005 specification and
February 2007 specification which was created by collaborative efforts
of several companies.

<a id="axis-apache-org-axis2-java-sandesha-architectureguide--architecture"></a>

## Architecture<a id="axis-apache-org-axis2-java-sandesha-architectureguide--Architecture"></a>

![Architecture](axis.apache.org/axis2/java/sandesha/images/architecture.jpg)

Sandesha2 components are used in a completely symmetric
manner, in the server side and client as shown in the diagram above. Lets just
consider a single side for this discussion.
<a id="axis-apache-org-axis2-java-sandesha-architectureguide--hnd"></a>

### Handlers<a id="axis-apache-org-axis2-java-sandesha-architectureguide--Handlers"></a>

Sandesha2 adds three handlers to the execution chain of
Axis2. Two of these handlers are added to a special user phase called 'RMPhase'
of in and out flows. The other handler is added to the predispatch phase of the
inFlow. These handlers and their functions are given below.

![Storage](axis.apache.org/axis2/java/sandesha/images/handlers.jpg)
<a id="axis-apache-org-axis2-java-sandesha-architectureguide--globalin"></a>

#### SandeshaGlobalInHandler<a id="axis-apache-org-axis2-java-sandesha-architectureguide--SandeshaGlobalInHandler"></a>

This handler is added to the predispatch phase of the
inFlow. Since this is a global phase, this handler will be called for each and
every message that comes to the Axis2 system. To maximize performance, the very
first function of this handler is to identify whether the current message can be
processed by it. It checks whether the message is intended for a RM enabled
service, and if so, check the message type to further verify whether it should
be processed globally. This handler was placed to perform functions that should
be done before the instance dispatching level of Axis2.

**Some of these functions are given below:**

- Detecting duplicate messages.
- Detecting faults that occur due to RM control messages and
  reporting them.

<a id="axis-apache-org-axis2-java-sandesha-architectureguide--in"></a>

#### SandeshaInHandler<a id="axis-apache-org-axis2-java-sandesha-architectureguide--SandeshaInHandler"></a>

This is added to the RMPhase of the inFlow. Since
RMPhase is a user phase, this handler will only be invoked for messages that are
aimed at RM enabled service. This handler processes the SOAP header of the
message. Acknowledgement headers, Acknowledgement requests and sequence
processing headers are processed by this handler. Sandesha2 has a special set of
classes called message processors which are capable of processing each type of
message. Depending on the type, the message is send through the
'processInMessage' method of the message processor which will do the further
processing of it.

<a id="axis-apache-org-axis2-java-sandesha-architectureguide--out"></a>

#### SandeshaOutHandler<a id="axis-apache-org-axis2-java-sandesha-architectureguide--SandeshaOutHandler"></a>

This handler is responsible for doing the basic outFlow
processing. This will first generate an ID called the Internal Sequence ID which
is used to identify the sequence this message should belongs to. All the
messages having the same Internal Sequence ID will be sent within a single
sequence. An Internal Sequence ID will have a corresponding Sequence ID which
would be obtained after the Create Sequence message exchange. In the client side
the Internal Sequence ID is the combination of the wsa:To address and a special
value given by the client called Sequence Key. In the server side the Internal
Sequence ID is a derivation of the Sequence ID value of the messages of the
incoming sequence.

Before sending the message through other handlers the
SandeshaOutHandler will send it through the 'processOutMessage' method of the
respective message processor.

<a id="axis-apache-org-axis2-java-sandesha-architectureguide--rmm"></a>

#### RMMessageReceiver<a id="axis-apache-org-axis2-java-sandesha-architectureguide--RMMessageReceiver"></a>

All the Reliable messaging operations
(CreateSequence/CloseSequence etc) have the RMMessageReceiver as the ultimate
receiver for the message. The RMMessageReceiver will identify the type of RM
control message. Sandesha2 has a special set of classes called message
processors which are capable of processing each type of message. Depending on
the type, the message is send through the 'processInMessage' method of the
message processor which will do the further processing of it.

<a id="axis-apache-org-axis2-java-sandesha-architectureguide--sender"></a>

### Sender<a id="axis-apache-org-axis2-java-sandesha-architectureguide--Sender"></a>

Sender is responsible for transmission and retransmission of
messages. The Sender is a separate thread that keeps running all the
time. At each iteration Sender checks whether there is any messages to
be sent. If there is any, it is sent to the destination. Sender also
identifies messages that has to be retransmitted and keep re-sending
them until a maximum limit decided by [Sandesha2 policies](#axis-apache-org-axis2-java-sandesha-userguide--cs) is exceeded.

<a id="axis-apache-org-axis2-java-sandesha-architectureguide--ioi"></a>

### In Order Invoker<a id="axis-apache-org-axis2-java-sandesha-architectureguide--In_Order_Invoker"></a>

InOrderInvoker is another separate thread that is
started by the Sandesha2 system. This is started only if Sadesha2 has been
configured to support in-order delivery assurance. InOrderInvoker makes sure
that it invokes messages of a sequence only in the order of message numbers.
<a id="axis-apache-org-axis2-java-sandesha-architectureguide--sf"></a>

### Storage Framework<a id="axis-apache-org-axis2-java-sandesha-architectureguide--Storage_Framework"></a>

Sandesha2 storage framework is one of the most important
parts of the Sandesha2 system. This was designed to support the RM message
exchange while being independent of the storage implementation used. The storage
framework defines a set of interfaces and abstract classes that can be
implemented by a particular storage implementation. Sandesha2 system comes with
an in-memory storage implementation. There can be other implementations based on
different databases and persistence mechanisms.

**Following diagram gives a brief view of the
Sandesha2 storage framework.**

![Storage](axis.apache.org/axis2/java/sandesha/images/storage.jpg)
<a id="axis-apache-org-axis2-java-sandesha-architectureguide--RMbeans"></a>

Storage framework defines several beans that extend the
RMBean abstract class. They are given below:

1. RMSBean (fields - internalSequenceID, createSeqMsgID,
   sequenceID, createSequenceMsgStoreKey, referenceMessageStoreKey,
   securityTokenData, clientCompletedMessages, toEPR, soapVersion, replyToEPR,
   rMVersion, acksToEPR, terminated, serviceName, pollingMode)
2. SenderBean (fields - messageContextRefKey,
   internalSequenceID, messageNumber, messageID, messageType, send, resend,
   sentCount,timeToSend)
3. RMDBean (fields - sequenceID, nextMsgToProcess,
   pollingMode, referenceMessageKey, toEPR, replyToEPR, rMVersion, acksToEPR,
   terminated, serviceName, pollingMode)
4. InvokerBean (fields - invoked,messageContextRefKey,
   sequenceID, msgNo)

There are four bean manager interfaces corresponding to
each of above beans.They are as follows:

1. RMSBeanMgr
2. InvokerBeanMgr
3. RMDBeanMgr
4. SenderBeanMgr

Sandesha2 also defines a StorageManager interface that
defines methods to create each of these bean managers and to create a
Transaction object which should implement the Transaction interface. Transaction
interface defines commit and rollback methods. The StorageManager interface is
also responsible for storing, updating, retrieving and deleting of
MessageContext instances for a sequence.

Collectively each Sandesha2 storage implementation
should have following classes:

1. An implementation of the StorageManager interface.
2. Implementations of the four Bean Manager interfaces.
3. An implementation of the Transaction interface.

These classes can be packed as a jar archive and added
to the classpath. The name of the StorageManager implementation class must be
mentioned in Sandesha2 policy configurations. This will be picked up after a
restart of the the Axis2 engine.  
  
**InMemory
Implementation**  
As discussed, Sandesha ships with an
InMemory implementation of the storage manager. Perhaps the most significant
point of interest in this implementation is the transaction model. Transactions
are scoped by thread: a transaction can only be associated with one thread ever
and a thread can only have one transaction active at any single point in time.
Any storage manager beans touched by the transaction will be enlisted into the
transaction in such a way that any other transactions that attempt to touch the
beans will block until the enlisting transaction completes (either commits or
rollsback).

<a id="axis-apache-org-axis2-java-sandesha-architectureguide--da"></a>

## Delivery Assurances<a id="axis-apache-org-axis2-java-sandesha-architectureguide--Delivery_Assurances"></a>

Sandesha2 can provide an in-order exactly-once delivery
assurance. The ordering (in-order) is optional. You can disable it using
Sandesha2 policy configurations. The ordering is done using the [InOrderInvoker thread](#axis-apache-org-axis2-java-sandesha-architectureguide--ioi) that was
introduced earlier.

**If ordering (in-order) is enabled**, SandeshaInHandler pauses the execution of an incoming
application message. As a result of this, the message will not go through rest
of the handler chain in the first invocation. Note that it also starts the
InOrderInvoker thread if it is stopped. This thread goes through the paused
messages and resume each of them in the order of message numbers.

**If in-order invocation is not enabled** the SandeshaInHandler will not pause the messages and they
will go in their full execution path in one go.

The delivery assurance to be used depends on your
requirements. If you want the invocation to be as fast as possible, and you do
not care about ordering, disable in order invocation. But if you want message to
be invoked in the order they were sent by the client, you have to enable it.
There could be a considerable performance improvements if this feature is
disabled. Specially if majority of the messages come out of order. In the
current implementation, each message (identified by sequenceID and message
number) will be invoked only once. So exactly once delivery assurance is
guaranteed. You cannot ask Sandesha2 to invoke the same message more than
once.  
  
**Configuring
Sandesha**  
  
<a id="axis-apache-org-axis2-java-sandesha-architectureguide--config"></a>Sandesha is configured using various means, and
this configuration is made accesible in the SandeshaPolicyBean object, which is
stored in the AxisDescription as a property at module init time. The
configuration data can be loaded by examing the policies in the sandesha
module.xml, from default values (if there is nothing in the module.xml) or from
property files if explicitly driven by client code.  
Some of the possible
options to configure are: **AcknowledgementInterval:** time between
sending acknowledgements  
**RetransmissionInterval:** time
between retransmitting messages  
**MaximumRetransmissionCount:**
max count to retry sending unacknowledged
messages  
**ExponentialBackoff:** if true the time between
message retransmission attempts will grow
exponentially.  
**InactivityTimeout:** time that the sequence is
allowed to remain inactive before it is cleaned
up.  
**SequenceRemovalTimeout:** time to wait after a sequence is
terminated before removing the sequence state from the
store.  
**InvokeInOrder:** if true messages will only be
delivered to the webservice endpoint in the exact order they were sent by the
RMS.  
**MessageTypesToDrop:** the set of message types (stored by
number, see Sandesha2Constants.MessageTypes) that are elligible to drop by the
RMS.  
**StorageManager:InMemoryStorageManager:** the classname to
use for the volatile
storagemanager  
**StorageManager:PermanentStorageManager:** the
classname to use for the non-volatile
storagemanager  
**SecurityManager:** the class to use in order to
process any WS-Security tokens associated with a sequence. A NO-OP
implementation is shipped with Sandesha.  
**ContextManager:** the
class to use to ensure the inOrderInvoker thread uses a specific context. A
NO-OP implementation is shipped with Sandesha.  
**EPRDecorator:**
the class to use in order to augment any endpoint references with any extra
information required. A NO-OP implementation is shipped with
Sandesha.  
**MakeConnection:Enabled:** if true, makeConnection
messages will be used when sandesha is performing synchronous
messaging.  
**MakeConnection:UseRMAnonURI:** if true,
makeConnection messages used for synchronous messaging will use the RM anonymous
URI.  
**MakeConnection:UseMessageSerialization:** if true
messages are serialized into binary when sbeing stored in the
storageManager.  
**EnforceRM:** if true any non-RM messages
recieved by the RMD will cause an exception to be shown.

<a id="axis-apache-org-axis2-java-sandesha-architectureguide--es"></a>

## Example Scenario<a id="axis-apache-org-axis2-java-sandesha-architectureguide--Example_Scenario"></a>

This part explains how Sandesha2 framework works internally for
the most common RM scenario, which is the sending of a couple of Ping
messages from a client to the server. We will mainly look at how
Sandesha2 uses its storage to do the RM message exchange correctly.
While going through the following, keep the [RM
Beans and their fields](#axis-apache-org-axis2-java-sandesha-architectureguide--RMbeans) which were mentioned
earlier, in mind.

<a id="axis-apache-org-axis2-java-sandesha-architectureguide--cs"></a>

### Client Side<a id="axis-apache-org-axis2-java-sandesha-architectureguide--Client_Side"></a>

- Client does the first fireAndForget method invocation
  of a serviceClient after setting necessary properties.
- Message reaches the SandeshaOutHandler which detects
  it as an application message. The processing is delegated to the
  processOutMessage method of the Application Message Processor.
- Application Message Processor generates the Internal
  Sequence ID as explained earlier. It understands that this is a new sequence
  and generates a Create Sequence Message for it. The Application Message gets
  paused.
- Application Message Processor adds an entry to the
  RMS bean manager representing the newly created Create Sequence message. This
  entry has three properties. The sequenceID property is initially null. The
  createSeqMsgID is the message ID of the created CreateSequence message. The
  internalSequenceID property gets the generated Internal Sequence ID value.
- Application Message Processor adds two entries to the
  SenderBeanManager. One which has the send property to 'false' represents the
  application message, other which has the send property to 'true' represents
  the CreateSequence message. The Sender thread sends (and retransmits) only the
  CreateSequence message.
- Application Message Processor stores three
  MessageContext instances inside the StoreManager. The first is the
  CreateSequence message, the second a "reference message", which is a copy of
  the CreateSequence message. The third is the application message.
- After some time the client side would receive a
  Create Sequence Response message from the server. The SandeshaInHandler
  delegates the processing to the CreateSequenceResponse message processor. It
  finds the correct CreateSequence manager entry using the
  createSequenceMessageID property (which is in the relatesTo entry of the
  response message).
- Client updates the sequenceID property of the RMS
  bean manager entry. Also the send value of the application message entries are
  set to 'true'. The sender starts transmitting and retransmitting application
  messages.
- When the client receives acknowledgements for the
  messages it send, they are delivered to the Acknowledgement Processor which
  removes the corresponding application message entries from the Sender bean
  manager.
- If an acknowledgement says that all the sent messages (up to
  last message) was successfully received, the Acknowledgement Processor
  creates a Terminate Sequence message and adds a corresponding entry to
  the Sender bean manager.

<a id="axis-apache-org-axis2-java-sandesha-architectureguide--ss"></a>

### Server Side<a id="axis-apache-org-axis2-java-sandesha-architectureguide--Server_Side"></a>

- Server receives a CreateSequence message. It
  generates a new sequence ID and creates a new Create Sequence Response message
  containing this ID.
- CreateSequence message processor processInMessage
  creates an RMD bean representing the server side of the sequence. The sequence
  identifier for this sequence is stored in the RMD bean and the bean is added
  to the RMD bean manager. The initial value for nextMsgNoToProcess property is
  1.
- The CreateSequence message processor starts the
  "worker" threads for the sequence. This includes the Sender thread for
  response messages and the Invoker thread if the StorageManager returns one.
- The CreateSequenceResponse message is created and
  sent immediately by the CreateSequence message processor.
- After some time the server receives an application
  message. The SandeshaGlobalInHandler retrieves the RMD bean which matches the
  inbound sequence. A check is made to ensure this is not a duplicate message
  before processing is allowed to continue.
- The server side SandeshaInHandler delegates this to the
  RMMessageReceiver which creates an acknowledgement message and sends
  it. If in-order invocation is enabled, an entry is added to the
  InvokerBeanManager representing this new application message.

  *Lets assume that the message number of this message is
  2.*
- The InOrderInvoker which keeps looking at the
  InvokerBeanManager entries sees that there are entries to be invoked.
- The InOrderInvoker checks the entry of the RMDBean
  manager of the relevant sequence and sees that it is 1. But since only message
  number 2 is present in the invokerBeanManager entries, the invocation is not
  done.
- After some time, application message 1 also comes.
  Now the Invoker sees this entry and invokes the message. It also updates the
  nextMsgNoToProcess property of RMD Bean to 2. The Invoker again checks whether
  the new entry for the nextMsgNoToProcess (2) is present in the
  InvokerBeanManager entries. Since this is present it is also invoked. The
  value is again updated (to 3) but no invocation is done since an entry is not
  found.
- Some time later the server may receive a TerminateSequence
  message. It can partly remove the resources allocated for the sequence.
  The other part of resources (which is required by the InOrderInvoker)
  is removed after the invocation of the last message.

---

Copyright © 2005-2013
[The Apache Software Foundation](http://www.apache.org/).
All Rights Reserved.

---

---

<a id="axis-apache-org-axis2-java-sandesha-download-cgi-index"></a>

# Sandesha2 - 
        Sandesha2 - Releases

Please select the Sandesha2 version you want to download. Latest release is 1.6.3-SNAPSHOT. You can download both
the binary distribution and the source distribution.

| Name | Type | Distribution | Date | Description | Compatible Axis2 version |
| --- | --- | --- | --- | --- | --- |
| 1.6.2 | Release | Source Distributionzip\|MD5\|PGPBinary Distributionzip\|MD5\|PGP | 30 - Apr - 2012 | 1.6.2 Release (Mirrored) | 1.6.2 |
| 1.6.1 | Release | Source Distributionzip\|MD5\|PGPBinary Distributionzip\|MD5\|PGP | 10 - Dec - 2011 | 1.6.1 Release (Archived) | 1.6.1 |
| 1.6.0 | Release | Source Distributionzip\|MD5\|PGPBinary Distributionzip\|MD5\|PGP | 02 - Jun - 2011 | 1.6.0 Release (Archived) | 1.6.0 |
| 1.4 | Release | Source Distributionzip\|MD5\|PGPBinary Distributionzip\|MD5\|PGP | 19 - Dec - 2010 | 1.4 Release (Mirrored) | 1.5.4 |
| 1.3 | Release | Source Distributionzip\|MD5\|PGPBinary Distributionzip\|MD5\|PGP | 08 - Oct - 2007 | 1.3 Release (Archived) | 1.3 |
| 1.2 | Release | Source Distributionzip\|MD5\|PGPBinary Distributionzip\|MD5\|PGP | 10 - Jun - 2007 | 1.2 Release (Archived) | 1.2 |
| 1.1 | Release | Source Distributionzip\|MD5\|PGPBinary Distributionzip\|MD5\|PGP | 10 - Dec - 2006 | 1.1 Release (Archived) | 1.1 |
| 1.0 | Release | Source Distributionzip\|MD5\|PGPBinary Distributionzip\|MD5\|PGP | 08 - May - 2006 | 1.0 Release (Archived) | 1.0 |
| 0.9 | Release | Source Distributionzip\|MD5\|PGPBinary Distributionzip\|MD5\|PGP | 05 - Dec - 2005 | 0.9 Release (Archived) | 0.93 |

The currently selected mirror is **https://dlcdn.apache.org/**. If
you encounter a problem with this mirror, please select another mirror. If
all mirrors are failing, there are *backup* mirrors (at the end of the
mirrors list) that should be available.

Other mirrors:
https://dlcdn.apache.org/
https://dlcdn.apache.org/ (backup)

You may also consult the [complete
list of mirrors](http://www.apache.org/mirrors/).

**Note:** when downloading from a mirror please check the [md5sum](http://www.apache.org/dev/release-signing#md5) and verify
the [OpenPGP](http://www.apache.org/dev/release-signing#openpgp)
compatible signature from the main Apache site. These can be downloaded by
following the links above. This [KEYS](http://www.apache.org/dist/axis/axis2/java/sandesha/KEYS) file contains the
public keys used for signing release. It is recommended that (when possible)
a [web of
trust](http://www.apache.org/dev/release-signing#web-of-trust) is used to confirm the identity of these keys.

---

<a id="axis-apache-org-axis2-java-sandesha-index"></a>

# Sandesha2 - 
        Welcome to Sandesha2 home page

# Welcome to Apache Sandesha2

Apache Sandesha2™ is an Axis2 module that implements the [WS-ReliableMessaging
specification](http://www.ibm.com/developerworks/webservices/library/specification/ws-rm/) published by IBM, Microsoft, BEA and TIBCO.
By using Sandesha2 you can add reliable
messaging capability to the web services hosted using Axis2. Sandesha2 can
also be used with Axis2 client to interact with already hosted web services
in a reliable manner. Please see sandesha2 user guide for more information on
using Sandesha2. Read Sandesha2 Architecture guide to see how Sandesha2 work
internally.

Apache Sandesha2, Sandesha2, Apache, the Apache feather logo, and the Apache Sandesha2 project logo are trademarks of The Apache Software Foundation.

---

<a id="axis-apache-org-axis2-java-sandesha-issue-tracking"></a>

# Sandesha2 - Issue Tracking

<a id="axis-apache-org-axis2-java-sandesha-issue-tracking--Overview"></a>

## Overview<a id="axis-apache-org-axis2-java-sandesha-issue-tracking--Overview"></a>

This project uses [JIRA](http://www.atlassian.com/software/jira) a J2EE-based, issue tracking and project management application.

<a id="axis-apache-org-axis2-java-sandesha-issue-tracking--Issue_Tracking"></a>

## Issue Tracking<a id="axis-apache-org-axis2-java-sandesha-issue-tracking--Issue_Tracking"></a>

Issues, bugs, and feature requests should be submitted to the following issue tracking system for this project.

```text
https://issues.apache.org/jira/browse/SANDESHA2
```

---

<a id="axis-apache-org-axis2-java-sandesha-mail-lists"></a>

# Sandesha2 - Project Mailing Lists

<a id="axis-apache-org-axis2-java-sandesha-mail-lists--Project_Mailing_Lists"></a>

## Project Mailing Lists<a id="axis-apache-org-axis2-java-sandesha-mail-lists--Project_Mailing_Lists"></a>

These are the mailing lists that have been established for this project. For each list, there is a subscribe, unsubscribe, and an archive link.

| Name | Subscribe | Unsubscribe | Post | Archive | Other Archives |
| --- | --- | --- | --- | --- | --- |
| Axis2 Developer List | Subscribe | Unsubscribe | Post | mail-archives.apache.org | markmail.org |
| Axis2 User List | Subscribe | Unsubscribe | Post | mail-archives.apache.org | markmail.org |

---

<a id="axis-apache-org-axis2-java-sandesha-source-repository"></a>

# Sandesha2 - Source Repository

<a id="axis-apache-org-axis2-java-sandesha-source-repository--Overview"></a>

## Overview<a id="axis-apache-org-axis2-java-sandesha-source-repository--Overview"></a>

This project uses [Subversion](http://subversion.tigris.org/) to manage its source code. Instructions on Subversion use can be found at [http://svnbook.red-bean.com/](http://svnbook.red-bean.com/).

<a id="axis-apache-org-axis2-java-sandesha-source-repository--Web_Access"></a>

## Web Access<a id="axis-apache-org-axis2-java-sandesha-source-repository--Web_Access"></a>

The following is a link to the online source repository.

```text
http://svn.apache.org/viewvc/axis/axis2/java/sandesha/branches/1_6
```

<a id="axis-apache-org-axis2-java-sandesha-source-repository--Anonymous_access"></a>

## Anonymous access<a id="axis-apache-org-axis2-java-sandesha-source-repository--Anonymous_access"></a>

The source can be checked out anonymously from SVN with this command:

```bash
$ svn checkout http://svn.apache.org/repos/asf/axis/axis2/java/sandesha/branches/1_6 sandesha2-parent
```

<a id="axis-apache-org-axis2-java-sandesha-source-repository--Developer_access"></a>

## Developer access<a id="axis-apache-org-axis2-java-sandesha-source-repository--Developer_access"></a>

Everyone can access the Subversion repository via HTTP, but Committers must checkout the Subversion repository via HTTPS.

```bash
$ svn checkout https://svn.apache.org/repos/asf/axis/axis2/java/sandesha/branches/1_6 sandesha2-parent
```

To commit changes to the repository, execute the following command to commit your changes (svn will prompt you for your password)

```bash
$ svn commit --username your-username -m "A message"
```

<a id="axis-apache-org-axis2-java-sandesha-source-repository--Access_from_behind_a_firewall"></a>

## Access from behind a firewall<a id="axis-apache-org-axis2-java-sandesha-source-repository--Access_from_behind_a_firewall"></a>

For those users who are stuck behind a corporate firewall which is blocking HTTP access to the Subversion repository, you can try to access it via the developer connection:

```bash
$ svn checkout https://svn.apache.org/repos/asf/axis/axis2/java/sandesha/branches/1_6 sandesha2-parent
```

<a id="axis-apache-org-axis2-java-sandesha-source-repository--Access_through_a_proxy"></a>

## Access through a proxy<a id="axis-apache-org-axis2-java-sandesha-source-repository--Access_through_a_proxy"></a>

The Subversion client can go through a proxy, if you configure it to do so. First, edit your "servers" configuration file to indicate which proxy to use. The file's location depends on your operating system. On Linux or Unix it is located in the directory "~/.subversion". On Windows it is in "%APPDATA%\Subversion". (Try "echo %APPDATA%", note this is a hidden directory.)

There are comments in the file explaining what to do. If you don't have that file, get the latest Subversion client and run any command; this will cause the configuration directory and template files to be created.

Example: Edit the 'servers' file and add something like:

```text
[global]
http-proxy-host = your.proxy.name
http-proxy-port = 3128
```