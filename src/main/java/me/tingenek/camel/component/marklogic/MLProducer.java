package me.tingenek.camel.component.marklogic;

import com.marklogic.hub.flow.FlowInputs;
import com.marklogic.hub.flow.FlowRunner;
import com.marklogic.hub.flow.RunFlowResponse;
import com.marklogic.hub.flow.impl.FlowRunnerImpl;

import java.io.InputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultProducer;
import org.apache.camel.util.ExchangeHelper;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.CamelExchangeException;
import org.apache.camel.util.MessageHelper;
import org.apache.camel.component.file.GenericFile;

import org.apache.log4j.Logger;
import java.util.Arrays;


/**
 * ML Producer provides a channel on which clients can create and invoke message exchanges
 * on an {@link Endpoint}
 *  
 * Uses MarkLogic API to send generic body to server 
 * 
 * @author Mark Lawson (tingenek) For llamas everywhere.
 *         
 */
public class MLProducer extends DefaultProducer {
	private static final transient Logger LOG = Logger.getLogger(MLProducer.class);
    private MLEndpoint endpoint;
	
	public MLProducer(MLEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}
	
	@Override
    public void process(Exchange exchange) throws Exception {
	    String user = endpoint.getUser();
	    String password = endpoint.getPassword();	    
	    String host = endpoint.getHost();		    
	    
		Message message = exchange.getIn();	
		String flowName = message.getHeader("dhf_flowname", String.class);		
		String steps =  message.getHeader("dhf_steps", String.class);
								
	      try { 
        	FlowRunner flowRunner = new FlowRunnerImpl(host, user, password);
            FlowInputs inputs = new FlowInputs(flowName);
            //Set Options to current headers 
	        inputs.setOptions(message.getHeaders());
	        //If we defined steps then set them too
			if (steps != null) inputs.setSteps(Arrays.asList(steps.split(","))); 
        	//Run the flow and wait        
	        RunFlowResponse response = flowRunner.runFlow(inputs);
	        flowRunner.awaitCompletion();
	        //Set the message body to the JSON return
	        exchange.getIn().setBody(response);


		} catch (Exception e) {
			LOG.error("Error writing message " + flowName +" :" + e.getMessage());		
			exchange.setException(e);
			//You can't throw here. You could kill the whole context but it's bad practice. 
			//exchange.getContext().stop();
			//Better to use a RoutePolicy :-)
		} 
		
	}
	
	
}
