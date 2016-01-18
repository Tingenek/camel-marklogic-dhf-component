package me.tingenek.camel.component.marklogic;

import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultConsumer;

/**
 * The Example consumer.
 */
public class MLConsumer extends DefaultConsumer  {

    public MLConsumer(MLEndpoint endpoint, Processor processor) {
        super(endpoint, processor);       
    }

}
