Camel MarkLogic Component
==========================
A component wrapping a small part of the MarkLogic DHF 5.x Java API
Definitely a work in progress!

2020 MLawson (@tingenek).

### Build Instructions:

 >mvn clean install

This puts the code in your local Maven repository. Then you can use 
```
<dependency>
	<groupId>me.tingenek.camel</groupId>
	<artifactId>camel.dhf.component</artifactId>
	<version>1.0-SNAPSHOT</version>
</dependency>
```

### Usage:
This version can only act as a producer, i.e. only as a "to" component. 

```
ml:host/runflow[?user=...&password=..]
```
For example, to read files from inbox and send them to the server into database Documents using the original filename. Files will be added to the collections 'import' and 'camel' :
```
<route>
     <from uri="file:inbox"/>
     <setHeader headerName="ml_flowname">
        <simple>my-flow-name</simple>
    </setHeader>
    <to uri="ml:localhost:runflow" />
</route>
```    

### Usage:
Depends on how you have configured your flows. All this does is call one.

### Notes:
* You must supply a host, and a header with the flow name; defaults for authentication are: admin/admin,.
* The Producer endpoint calls the named flow with it's options set to all the current camel message headers. 
* The message body is set to the JSON return from the flow.
* Errors are marked in the Exchange so any problems get reported to Camel. This means for instance that files will be re-tried. This is the most flexible approach as the route can see the problem. See RoutePolicy.





