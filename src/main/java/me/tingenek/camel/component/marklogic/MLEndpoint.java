package me.tingenek.camel.component.marklogic;;

import org.apache.camel.impl.DefaultEndpoint;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
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
@UriEndpoint(scheme = "ml", syntax = "ml:host:port/[database]?user?password")
public class MLEndpoint extends DefaultEndpoint {
	private static final transient Logger LOG = Logger.getLogger(MLEndpoint.class);

	private static final String DEFAULT_DATABASE = "Documents";
    private static final int DEFAULT_PORT = 8000;
	private static final String URI_ERROR = "Invalid URI. Format must be of the form ml:host[:port]/[database]?user?password...";

    
	/* Params */
	@UriPath(label="host") @Metadata(required = "true")
	private String host;
	@UriPath(label="port", defaultValue = "" + DEFAULT_PORT) 
	private int port;	
	@UriPath(label="database", defaultValue = DEFAULT_DATABASE)

	private String database;
	@UriParam(label = "user", defaultValue = "admin")
	private String user ="admin";
	@UriParam(label = "password", defaultValue = "admin")
	private String password ="admin";
	@UriParam(label = "query")
	private String query;
	
	/*Database handle */
	private DatabaseClient client;
	
	public MLEndpoint(String endpointUri, MLComponent component) throws URISyntaxException  {
		super(endpointUri, component);
	    URI uri = new URI(endpointUri);

	    /* Check we're got path vars - they don't get assigned automatically (params do) */
	    host = uri.getHost();
	    if (host == null) {
	    	throw new IllegalArgumentException(URI_ERROR);
	    }

	    port = uri.getPort() == -1 ? DEFAULT_PORT : uri.getPort();

	    database = uri.getPath() == null || uri.getPath().trim().length() == 0 ? DEFAULT_DATABASE : uri.getPath().substring(1);
	    
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
    public Consumer createConsumer(Processor processor) throws Exception {
    	MLConsumer consumer = new MLConsumer(this,processor);
        // ScheduledPollConsumer default delay is 500 millis and that is too often for polling a feed, so we override
        // with a new default value. End user can override this value by providing a consumer.delay parameter
    	consumer.setDelay(MLConsumer.DEFAULT_CONSUMER_DELAY);
    	return consumer;
    }
	

	public boolean isSingleton() {
		return true;
	}
	
	  @Override
	public void doStart() throws Exception {
	        super.doStart();
	    	client = DatabaseClientFactory.newClient(host, port,user,password,DatabaseClientFactory.Authentication.DIGEST);
	  }
		
	@Override
    protected void doStop() throws Exception {
		if (client != null) {
			client.release();
		}
        super.doStop();
    }


	//Host
	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host= host;
	}

	//Port
	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port= port;
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
	//Database
	public String getDatabase() {
		return this.database;
	}

	public void setDatabase(String database) {
		this.database= database;
	}

	//Query
		public String getQuery() {
			return this.query;
		}

		public void setQuery(String query) {
			this.query= query;
		}
	
	
	// Connection
	public DatabaseClient getClient() {
		return this.client;
	}
	
}
