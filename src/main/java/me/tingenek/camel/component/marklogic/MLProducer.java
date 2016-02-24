package me.tingenek.camel.component.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.document.GenericDocumentManager;
import com.marklogic.client.document.DocumentManager;
import com.marklogic.client.eval.ServerEvaluationCall;
import com.marklogic.client.eval.EvalResultIterator;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.document.DocumentWriteSet;
import com.marklogic.client.io.DOMHandle;
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
		Integer batchSize = exchange.getProperty(Exchange.AGGREGATED_SIZE, Integer.class);
	    String cmdMode = endpoint.getMode();
		if (batchSize != null && batchSize > 0) {
			LOG.info("Sending Batch of " + batchSize);
			batchProcess(exchange);
		} else if (batchSize == null) {
			if (cmdMode.equals("run") ) {
				 LOG.info("Executing message body");
				execProcess(exchange);
			} else {
				singleProcess(exchange);
			}  
		}
	}	
	
	//Process a batch Body
	private void batchProcess(Exchange exchange) throws Exception {
		Message message = exchange.getIn();
		DocumentMetadataHandle metadata = new DocumentMetadataHandle();

		LOG.debug("Processing " + message.getBody(String.class));
		List<Message> list =  message.getBody(List.class);		
			
		String docId = null;
		String docCollection = null;
       	GenericDocumentManager docMgr = endpoint.getClient().newDocumentManager();
       	DocumentWriteSet batch = docMgr.newWriteSet();
		batch.addDefault(metadata);
		
		for (Message msg : list) {
			docId = msg.getHeader("ml_docId", String.class);
			if (docId == null) docId = UUID.randomUUID().toString();

			LOG.debug("Adding " + docId + " as " + msg.getBody().getClass());
			batch.add(docId, new StringHandle(msg.getBody(String.class)));
		}
       //We don't know if we've got a good connection with ML until we try and use it!
        try {
 			docMgr.write(batch);
		} catch (Exception e) {
			LOG.error("Error writing message " + docId +" to MarkLogic." + e.getMessage());		
			exchange.setException(e);
			//You can't throw here. You could kill the whole context but it's bad practice. 
			//exchange.getContext().stop();
			//Better to use a RoutePolicy :-)
		} 
		
	}
	
	//Process a single Body
	private void singleProcess(Exchange exchange) throws Exception {
		Message message = exchange.getIn();	
		DocumentMetadataHandle metadata = new DocumentMetadataHandle();
		String docId = message.getHeader("ml_docId", String.class);
		String docCollection = message.getHeader("ml_docCollection", String.class);
		String fileName =  message.getHeader("CamelFileNameOnly", String.class);
	    InputStreamHandle handle = new InputStreamHandle();
		DatabaseClient client = endpoint.getClient();
       	GenericDocumentManager docMgr = client.newDocumentManager();

       	// If collection(s) add 
		if (docCollection != null) metadata.getCollections().addAll(docCollection.split(","));
		
       	//Check the body isn't a stream, else assume convertable to byte[]
		if (message.getBody() instanceof GenericFile) {
			handle.set(message.getBody(InputStream.class));
			// If we've a filename, we need that for ML to infer type
			if (docId == null) docId = fileName;		
		} else {
			//Try to get body as byte array
			handle.fromBuffer(message.getBody(byte[].class));
		}	
	       //We don't know if we've got a good connection with ML until we try and use it!
        try { 
        	//no docId - so set to random uuid
        	if (docId == null) docId = UUID.randomUUID().toString();
        	docMgr.write(docId,metadata,handle);
		} catch (Exception e) {
			LOG.error("Error writing message " + docId +" :" + e.getMessage());		
			exchange.setException(e);
			//You can't throw here. You could kill the whole context but it's bad practice. 
			//exchange.getContext().stop();
			//Better to use a RoutePolicy :-)
		} 
		
	}
	
	/* Exec the body if it's JS or XQuery */
	private void execProcess(Exchange exchange) throws Exception {
		Message message = exchange.getIn();	
		//Treat message as a string
		String body = message.getBody(String.class);
		DatabaseClient client = endpoint.getClient();
		ServerEvaluationCall call = client.newServerEval();
		//String response = "";
		List response = new ArrayList();
		EvalResultIterator result = null;
		
		try {
			call.xquery(body);
			result = call.eval();
			//response = call.evalAs(String.class);
			 //message.setBody(response);
			while (result.hasNext()){
			    response.add(result.next().getString());
			}
			
			message.setBody(response);
		} catch (Exception e) {
			LOG.error("Error running script: " + e.getMessage());		
			exchange.setException(e);
		} finally { 
			result.close(); 
		}
	}
		
	
	/* Get Doc metadata as XML */
	private String getMetaData(String docId, GenericDocumentManager docMgr) throws Exception {        
		DOMHandle readMetadataHandle = new DOMHandle();
		try {
			docMgr.readMetadata(docId, readMetadataHandle);				
		} catch (Exception e) {
			LOG.error("Error reading metadata for  " + docId);		
		}
		return readMetadataHandle.toString();	
	}
		
}
