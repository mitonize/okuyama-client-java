package mitonize.datastore;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Stack;

import mitonize.datastore.SocketManager.Endpoint;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SocketManagerTest {
	ServerSocket[] serverSockets;
	String[] endpoints;
	final int ENDPOINT_COUNT = 3;

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

	@Test
	public void testAquire() {
		fail("Not yet implemented");
	}

	@Test
	public void testRecycle() {
		fail("Not yet implemented");
	}

	@Test
	public void testNextEndpoint() throws UnknownHostException {
		SocketManager manager = new SocketManager(endpoints, 3);
		Endpoint endpoint = manager.nextEndpoint();
		System.out.printf("%s:%s\n", endpoint.address.getHostName(), endpoint.port);
		endpoint = manager.nextEndpoint();
		System.out.printf("%s:%s\n", endpoint.address.getHostName(), endpoint.port);
		endpoint = manager.nextEndpoint();
		System.out.printf("%s:%s\n", endpoint.address.getHostName(), endpoint.port);
		endpoint = manager.nextEndpoint();
		System.out.printf("%s:%s\n", endpoint.address.getHostName(), endpoint.port);
		endpoint = manager.nextEndpoint();
		System.out.printf("%s:%s\n", endpoint.address.getHostName(), endpoint.port);
	}

	@Test
	public void testOpenSocket() throws UnknownHostException, InterruptedException {
		SocketManager manager = new SocketManager(endpoints, 3);
		try {
			SocketAddress backup = serverSockets[1].getLocalSocketAddress();
			serverSockets[1].close();
			SocketStreams streams;
			streams = manager.openSocket();
			System.out.println(streams.socket);
			streams = manager.openSocket();
			System.out.println(streams.socket);
			streams = manager.openSocket();
			System.out.println(streams.socket);

			serverSockets[1] = new ServerSocket();
			serverSockets[1].bind(backup);
//			manager.getEndpointAt(1).markEndpointOffline(false);
			streams = manager.openSocket();
			System.out.println(streams.socket);
			Thread.sleep(1500);
			streams = manager.openSocket();
			System.out.println(streams.socket);
			streams = manager.openSocket();
			System.out.println(streams.socket);
		} catch (IOException e) {
			System.err.println(e);
		}
	}

	@Test
	public void testCloseSocket() {
		fail("Not yet implemented");
	}

	@Test
	public void testDestroy() {
		fail("Not yet implemented");
	}

}
