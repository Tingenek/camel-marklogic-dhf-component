package me.tingenek.camel.component.marklogic;

import org.apache.camel.Processor;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.impl.DefaultConsumer;
import org.apache.camel.impl.ScheduledPollConsumer;

import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.StringQueryDefinition;
import com.marklogic.client.io.SearchHandle;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.MatchDocumentSummary;

import org.apache.log4j.Logger;

import java.util.ArrayList;

/**
 * The Example consumer.
 */
public class MLConsumer extends ScheduledPollConsumer {
    public static final long DEFAULT_CONSUMER_DELAY = 5 * 60 * 1000L;
	private static final transient Logger LOG = Logger.getLogger(MLConsumer.class);
    private QueryManager queryMgr;
    

    public MLConsumer(MLEndpoint endpoint, Processor processor) {
        super(endpoint, processor); 
        queryMgr = endpoint.getClient().newQueryManager();
    }
    
    @Override
    public MLEndpoint getEndpoint() {
        return (MLEndpoint) super.getEndpoint();
    }
    
    @Override
    protected int poll() throws Exception {  	
    	StringQueryDefinition query = queryMgr.newStringDefinition();
    	query.setCriteria(getEndpoint().getQuery());
 //   	StringHandle resultsHandle = new StringHandle();
 //   	queryMgr.search(query, resultsHandle);
		SearchHandle resultsHandle = new SearchHandle();
		queryMgr.search(query, resultsHandle);
		LOG.info("Matched "+resultsHandle.getTotalResults()+ " documents with '"+query.getCriteria());	
		ArrayList al = new ArrayList();
		MatchDocumentSummary[] docSummaries = resultsHandle.getMatchResults();
		for (MatchDocumentSummary docSummary: docSummaries) {
			al.add(docSummary.getUri());
		}
    	Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setBody(al);
        exchange.getIn().setHeader("ml_results",resultsHandle.getTotalResults());
 
        try {
            // send message to next processor in the route
            getProcessor().process(exchange);
            return 1; // number of messages polled
        } finally {
            // log exception if an exception occurred and was not handled
            if (exchange.getException() != null) {
                getExceptionHandler().handleException("Error processing exchange", exchange, exchange.getException());
            }
            return 1;
        }   	
    }

}

