package mitonize.datastore;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketOption;
import java.net.SocketOptions;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketManager {
	Logger logger = LoggerFactory.getLogger(SocketManager.class);
	/**
	 * 接続先のTCPエンドポイントを保持
	 */
	class Endpoint {
		InetAddress address;
		int port;
	}
	Endpoint[] endpoints;
	int currentEndpointIndex = 0;

	protected ArrayBlockingQueue<Socket> queue;
	protected AtomicInteger activeSocket;

	public SocketManager(String[] masternodes, int maxPoolSize) throws UnknownHostException {
		queue = new ArrayBlockingQueue<Socket>(maxPoolSize);
		activeSocket = new AtomicInteger(0);
		endpoints = new Endpoint[masternodes.length];
		for (int i = 0; i < masternodes.length; ++i) {
			endpoints[i] = new Endpoint();
			endpoints[i].address = InetAddress.getByName(masternodes[i].split(":")[0]);
			endpoints[i].port = Short.parseShort(masternodes[i].split(":")[1]);
		}
	}

	public Socket aquire() throws IOException {
		Socket socket = queue.poll();
        return socket == null || !isAvailable(socket) ? openSocket() : socket;
	}

	public void recycle(Socket socket) {
		if (socket != null && !queue.offer(socket)) {
			closeSocket(socket);
		}
	}

	Socket openSocket() throws IOException {
		if (endpoints.length == 0) {
			throw new IllegalStateException("No connection endpoint setting specified.");
		}
		if (currentEndpointIndex >= endpoints.length)
			currentEndpointIndex = 0;

		Endpoint endpoint = endpoints[currentEndpointIndex];
		if (endpoint == null)
			throw new IllegalStateException("No connection endpoint setting specified.");

		try {
			InetSocketAddress address = new InetSocketAddress (endpoint.address, endpoint.port);
			Socket socket = new Socket();
			socket.setSoTimeout(2000);
			socket.connect(address, 2000);
			int c = activeSocket.incrementAndGet();
			if (logger.isInfoEnabled()) {
				logger.info("Socket opened -  count:" + c);
			}
			return socket;
		} catch (UnresolvedAddressException e) {
			return null;
		}
	}
	
	void closeSocket(Socket socket) {
		try {
			socket.close();
			int c = activeSocket.decrementAndGet();
			if (logger.isInfoEnabled()) {
				logger.info("Socket closed - count:" + c);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private boolean isAvailable(Socket socket) {
		if (socket.isConnected())
			return true;
		return false;
	}

}
