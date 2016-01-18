package me.tingenek.camel.component.marklogic;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;

import java.util.Map;

/**
 * MarkLogic component class that serves as a Factory of Endpoints.
 * 
 * @author Mark Lawson (tingenek) For llamas everywhere.      
 *         
 */
public class MLComponent extends DefaultComponent {

	public MLComponent() {
		super();
	}

	public MLComponent(CamelContext context) {
		super(context);
	}

	@Override
	protected Endpoint createEndpoint(String uri, String remaining, Map parameters) throws Exception {
		
		MLEndpoint myEndpoint = new MLEndpoint(uri, this);
		setProperties(myEndpoint, parameters);
		return myEndpoint;
	}

}
