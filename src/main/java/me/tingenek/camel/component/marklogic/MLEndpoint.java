package me.tingenek.camel.component.marklogic;

import org.apache.camel.impl.DefaultEndpoint;

import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;

import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;

import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * ML Endpoint that serves as Factory for {@link Producer} 
 * and {@link Exchange}.
 * 
 * 
 * @author Mark Lawson (tingenek) For llamas everywhere.
 *         
 */
@UriEndpoint(scheme = "dhf", syntax = "dhf:host/runflow?user?password", title = "dhf flowrunner endpoint")
public class MLEndpoint extends DefaultEndpoint {
	private static final transient Logger LOG = Logger.getLogger(MLEndpoint.class);

	private static final String URI_ERROR = "Invalid URI. Format must be of the form ml:host/runflow?user?password...";

    
	/* Params */
	@UriPath(label="host") @Metadata(required = "true")
	private String host;	
	@UriParam(label = "user", defaultValue = "admin")
	private String user ="admin";
	@UriParam(label = "password", defaultValue = "admin")
	private String password ="admin";
		

	public MLEndpoint(String endpointUri, MLComponent component) throws URISyntaxException  {
		super(endpointUri, component);
	    URI uri = new URI(endpointUri);

	    /* Check we're got path vars - they don't get assigned automatically (params do) */
	    host = uri.getHost();
	    if (host == null) {
	    	throw new IllegalArgumentException(URI_ERROR);
	    }
	    
	    //LOG.info("Connecting to ML using params:" + host + ":" + port + ":" + database);
	}

	public MLEndpoint(String endpointUri) {
		super(endpointUri);
	}
	
	
	@Override
    public Producer createProducer() throws Exception {
       return new MLProducer(this);
    }


	@Override
	 public Consumer createConsumer(final Processor processor) throws Exception {
         throw new UnsupportedOperationException();
     }
	
	
	public boolean isSingleton() {
		return true;
	}
			

	//Host
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host= host;
	}


	//User
	public String getUser() {
		return this.user;
	}

	public void setUser(String user) {
		this.user= user;
	}

	//Password
	public String getPassword() {
		return this.password;
	}

	public void setPassword(String password) {
		this.password= password;
	}
	
}
