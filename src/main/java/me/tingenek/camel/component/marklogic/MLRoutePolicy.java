package me.tingenek.camel.component.marklogic;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;

import org.apache.camel.Route;
import org.apache.camel.support.RoutePolicySupport;
import org.apache.camel.util.ObjectHelper;

import org.apache.log4j.Logger;

import java.lang.Exception;

/**
 * Example Route Policy - shuts the route if there's an error in the Exchange.
 * 
 * @author Mark Lawson (tingenek) For llamas everywhere.
 *        
 *         
 */


public class MLRoutePolicy extends RoutePolicySupport {
private static final transient Logger LOG = Logger.getLogger(MLRoutePolicy.class);

  @Override
  public void onExchangeDone(Route route, Exchange exchange) {
    Exception e = exchange.getException();
    if (e != null) {
    	LOG.error(e.getMessage());
    	shutDownRoute(route,exchange);
    }  	  
  }
  
  private void shutDownRoute(Route route, Exchange exchange) {
	  CamelContext context = exchange.getContext();
	  String routeId = route.getId();
	  LOG.info("Attempting to shutdown route: " + routeId);
	  try {
		  // force stopping this route while we are routing an Exchange
		  // requires two steps:
		  // 1) unregister from the inflight registry
		  // 2) stop the route
		  context.getInflightRepository().remove(exchange);
		  context.stopRoute(routeId);	
	  LOG.info("Route: " + routeId + " stopped.");
	  } catch (Exception e) {
		  getExceptionHandler().handleException(e);
	  }
	  
  }

}