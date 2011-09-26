#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.transport;

import java.net.InetSocketAddress;

import ${package}.service.SimpleOrderService;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ${package}.service.OrderProcessingService;

/**
 * <code>SimpleOrderProcessingServer</code> provides a very basic example Netty endpoint for the
 * {@link SimpleOrderService} implementation
 */
public class SimpleOrderServiceEndpoint {

	private static final Logger log = LoggerFactory.getLogger(SimpleOrderServiceEndpoint.class);

	private InetSocketAddress endpointAddress;
	
	private Server service;
	
	public SimpleOrderServiceEndpoint(InetSocketAddress endpointAddress) {
		this.endpointAddress = endpointAddress;
	}
	
	public synchronized void start() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Starting Simple Ordering Netty Server on '{}'", endpointAddress);
		}
		
		SpecificResponder responder = new SpecificResponder(OrderProcessingService.class, new SimpleOrderService());
		service = new NettyServer(responder, endpointAddress);
		service.start();
	}

	public synchronized void stop() throws Exception {
		if (log.isInfoEnabled()) {
			log.info("Stopping Simple Ordering Server on '{}'", endpointAddress);
		}
		service.start();
	}
}
