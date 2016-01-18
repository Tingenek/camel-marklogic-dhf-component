Camel MarkLogic Component
==========================
A component wrapping a small part of the MarkLogic 3.0 Java API
Definitely a work in progress!

2016 MLawson (@tingenek).

### Build Instructions:

 >mvn clean install

This puts the code in your local Maven repository. Then you can use 
```
<dependency>
	<groupId>me.tingenek.camel</groupId>
	<artifactId>camel.ml.component</artifactId>
	<version>1.0-SNAPSHOT</version>
</dependency>
```

### Usage:
This version can only act as a producer, i.e. only as a "to" component. 

```
ml:host[:port][/database][?user=...&password=..]
```
For example, to read files from inbox and send them to the server into database Documents using the original filename. Files will be added to the collections 'import' and 'camel' :
```
<route>
     <from uri="file:inbox"/>
     <setHeader headerName="ml_docId">
        <simple>${header.CamelFileNameOnly}</simple>
    </setHeader>
    <setHeader headerName="ml_docCollection">
        <simple>import,camel</simple>
    </setHeader>
    <to uri="ml:localhost:8000/Documents" />
</route>
```    

### Notes:
* You must supply a host, defaults for other params are: admin/admin, 8000 and Documents.
* The Producer endpoint saves whatever data is in the message body to ML. It can be anything (JSON/XML,binary etc) that the GenericDocumentManager understands. 
* Data without a ml_docId header will get named after the Camel Message Id eg ID-MacPro-2708-58220-1452764057613-0-1
* You can use header ml_docCollection to set one or more collections.
* Errors are marked in the Exchange so any problems get reported to Camel. This means for instance that files will be re-tried. This is the most flexible approach as the route can see the problem. See RoutePolicy.
