package me.tingenek.camel.component.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.document.DocumentDescriptor;
import com.marklogic.client.io.InputStreamHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.document.GenericDocumentManager;
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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.CamelExchangeException;

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
	private static final transient Log LOG = LogFactory.getLog(MLProducer.class);
    private MLEndpoint endpoint;
	
	public MLProducer(MLEndpoint endpoint) {
		super(endpoint);
		this.endpoint = endpoint;
	}
	
	@Override
    public void process(Exchange exchange) throws Exception {
		if (exchange.getProperty(Exchange.AGGREGATED_SIZE, Integer.class) != null) {
			LOG.info("This exchange is an Aggregation");
			batchProcess(exchange);
		} else	{
		    LOG.info("This exchange is a single");
		    singleProcess(exchange);
		}      
    }
	
	//Process a batch Body
	private void batchProcess(Exchange exchange) throws Exception {
		Message message = exchange.getIn();
		DocumentMetadataHandle metadata = new DocumentMetadataHandle();

		//List list = message.getBody(List.class);
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
		
		// If null use message Id
		if (docId == null) docId = message.getMessageId();
		// If collection(s) add 
		if (docCollection != null) metadata.getCollections().addAll(docCollection.split(","));

        //We don't know if we've got a good connection with ML until we try and use it!
        try {
        	GenericDocumentManager docMgr = endpoint.getClient().newDocumentManager();
 			InputStreamHandle handle = new InputStreamHandle(message.getBody(InputStream.class));
			docMgr.write(docId,metadata,handle);
		} catch (Exception e) {
			LOG.error("Error writing message " + docId +" to MarkLogic.");		
			exchange.setException(e);
			//You can't throw here. You could kill the whole context but it's bad practice. 
			//exchange.getContext().stop();
			//Better to use a RoutePolicy :-)
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
