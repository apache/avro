#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.transport;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ${package}.service.Confirmation;
import ${package}.service.Order;
import ${package}.service.OrderFailure;
import ${package}.service.OrderProcessingService;

/**
 * <code>SimpleOrderServiceClient</code> is a basic client for the Netty backed {@link OrderProcessingService}
 * implementation.
 */
public class SimpleOrderServiceClient implements OrderProcessingService {

	private static final Logger log = LoggerFactory.getLogger(SimpleOrderServiceEndpoint.class);

	private InetSocketAddress endpointAddress;

	private Transceiver transceiver;

	private OrderProcessingService service;

	public SimpleOrderServiceClient(InetSocketAddress endpointAddress) {
		this.endpointAddress = endpointAddress;
	}

	public synchronized void start() throws IOException {
		if (log.isInfoEnabled()) {
			log.info("Starting Simple Ordering Netty client on '{}'", endpointAddress);
		}
		transceiver = new NettyTransceiver(endpointAddress);
		service = SpecificRequestor.getClient(OrderProcessingService.class, transceiver);
	}

	public void stop() throws IOException {
		if (log.isInfoEnabled()) {
			log.info("Stopping Simple Ordering Netty client on '{}'", endpointAddress);
		}
		if (transceiver != null && transceiver.isConnected()) {
			transceiver.close();
		}
	}

	@Override
	public Confirmation submitOrder(Order order) throws AvroRemoteException, OrderFailure {
		return service.submitOrder(order);
	}

}
