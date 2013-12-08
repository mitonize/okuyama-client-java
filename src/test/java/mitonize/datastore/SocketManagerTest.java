package mitonize.datastore;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import mitonize.datastore.SocketManager.Endpoint;

import org.junit.Before;
import org.junit.Test;

public class SocketManagerTest {
	ServerSocket[] serverSockets;
	String[] endpoints;
	final int ENDPOINT_COUNT = 3;

	/**
	 * サーバソケットを作成してオープンしたソケットのエンドポイント情報を設定する。
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
		serverSockets = new ServerSocket[ENDPOINT_COUNT];
		for (int i=0; i < ENDPOINT_COUNT; ++i) {
			serverSockets[i] = new ServerSocket(0);
		}
		endpoints = new String[ENDPOINT_COUNT];
		for (int i=0; i < ENDPOINT_COUNT; ++i) {
			endpoints[i] = "localhost:" + serverSockets[i].getLocalPort();
		}
	}
	String str(Endpoint endpoint) {
		return endpoint.toString();
//		return endpoint.address.getHostName() + ":" + endpoint.port;
	}

	String str(SocketStreams streams) {
		Socket s = streams.getSocket();
		return s.getInetAddress().getHostName() + ":" + s.getPort();
	}

	@Test
	public void testNextEndpoint() throws UnknownHostException {
		SocketManager manager = new SocketManager(endpoints, 3);
		Endpoint endpoint = manager.nextEndpoint();
		assertEquals(endpoints[1], str(endpoint));
		endpoint = manager.nextEndpoint();
		assertEquals(endpoints[2], str(endpoint));
		endpoint = manager.nextEndpoint();
		assertEquals(endpoints[0], str(endpoint));
		endpoint = manager.nextEndpoint();
		assertEquals(endpoints[1], str(endpoint));
		endpoint = manager.nextEndpoint();
		assertEquals(endpoints[2], str(endpoint));
	}

	@Test
	public void testOpenSocket() throws UnknownHostException, InterruptedException {
		SocketManager manager = new SocketManager(endpoints, 3);
		try {
			SocketAddress backup = serverSockets[1].getLocalSocketAddress();
			serverSockets[1].close();
			SocketStreams streams;
			streams = manager.openSocket();
			assertEquals(endpoints[2], str(streams));
			streams = manager.openSocket();
			assertEquals(endpoints[0], str(streams));
			streams = manager.openSocket();
			assertEquals(endpoints[2], str(streams));

			serverSockets[1] = new ServerSocket();
			serverSockets[1].bind(backup);
			streams = manager.openSocket();
			assertEquals(endpoints[2], str(streams));
			Thread.sleep(10000);
			streams = manager.openSocket();
			assertEquals(endpoints[0], str(streams));
			streams = manager.openSocket();
			assertEquals(endpoints[1], str(streams));
		} catch (IOException e) {
			System.err.println(e);
		}
	}

}
