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
	    LOG.info("user " + user);       
	    LOG.info("password " + password);    
		Message message = exchange.getIn();	
		String flowName = message.getHeader("ml_flowname", String.class);
		// If collection(s) add 
		//if (docCollection != null) metadata.getCollections().addAll(docCollection.split(","));
		
        try { 
        	
        	//no docId - so set to random uuid           
    	    FlowRunner flowRunner = new FlowRunnerImpl(host, user, password);
            FlowInputs inputs = new FlowInputs(flowName);
	        // This is needed so that an absolute file path is used
	        inputs.setOptions(message.getHeaders());
	        RunFlowResponse response = flowRunner.runFlow(inputs);
	        flowRunner.awaitCompletion();
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
