Camel MarkLogic Data Hub Framework Component
==========================
A component wrapping the MarkLogic DHF 5.2 standalone client jar. Allows you to call a Flow and it'sStepsfrom Camel 
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
For example, to read files from inbox and then run the flow "my-flow-name", but only steps 1 and 3:
```
<route>
     <from uri="file:inbox"/>
     <setHeader headerName="dhf_flowname">
        <simple>my-flow-name</simple>
    </setHeader>
    <setHeader headerName="dhf_steps">
        <simple>1,3</simple>
    </setHeader>
    <to uri="dhf:localhost:runflow" />
</route>
```    

### Usage:
Depends on how you have configured your flows. All this does is call one.

### Notes:
* You must supply a host, and a dhf_header with the flow name; defaults for authentication are: admin/admin,.
* You can set a dhf_steps header with a comma separated list of steps to run. Default is all steps.
* The Producer endpoint calls the named flow with it's options set to all the current camel message headers. 
* The message body is set to the JSON return from the flow.
* Errors are marked in the Exchange so any problems get reported to Camel. This means for instance that files will be re-tried. This is the most flexible approach as the route can see the problem. See RoutePolicy.





