package mitonize.datastore;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.concurrent.ArrayBlockingQueue;

public class ChannelManager {
	/**
	 * 接続先のTCPエンドポイントを保持
	 */
	class Endpoint {
		InetAddress address;
		int port;
	}
	Endpoint[] endpoints;
	int currentEndpointIndex = 0;

	protected ArrayBlockingQueue<SocketChannel> queue;

	public ChannelManager(String[] masternodes, int maxPoolSize) throws UnknownHostException {
		queue = new ArrayBlockingQueue<SocketChannel>(maxPoolSize);
		endpoints = new Endpoint[masternodes.length];
		for (int i = 0; i < masternodes.length; ++i) {
			endpoints[i] = new Endpoint();
			endpoints[i].address = InetAddress.getByName(masternodes[i].split(":")[0]);
			endpoints[i].port = Short.parseShort(masternodes[i].split(":")[1]);
		}
	}

	public SocketChannel aquire() throws IOException {
		SocketChannel channel = queue.poll();
        return channel == null || !isAvailable(channel) ? openSocket() : channel;
	}

	public void recycle(SocketChannel channel) {
		if (channel != null && !queue.offer(channel)) {
			closeSocket(channel);
		}
	}

	SocketChannel openSocket() throws IOException {
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
			SocketChannel channel = SocketChannel.open(address);
			return channel;
		} catch (UnresolvedAddressException e) {
			return null;
		}
	}
	
	void closeSocket(SocketChannel socketChannel) {
		try {
			socketChannel.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private boolean isAvailable(SocketChannel channel) {
		if (channel.isConnected())
			return true;
		return false;
	}

}
