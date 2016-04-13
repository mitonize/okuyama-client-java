package mitonize.datastore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import junit.framework.Assert;
import mitonize.datastore.SocketManager.Endpoint;

import org.junit.Before;
import org.junit.Test;

public class SocketManagerTest {
	ServerSocket[] serverSockets;
	String[] endpoints;
	final int ENDPOINT_COUNT = 3;

	/**
	 * サーバソケットを作成してオープンしたソケットのエンドポイント情報を設定する。
	 *
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
		serverSockets = new ServerSocket[ENDPOINT_COUNT];
		for (int i = 0; i < ENDPOINT_COUNT; ++i) {
			serverSockets[i] = new ServerSocket(0);
		}
		endpoints = new String[ENDPOINT_COUNT];
		for (int i = 0; i < ENDPOINT_COUNT; ++i) {
			endpoints[i] = "localhost:" + serverSockets[i].getLocalPort();
		}
	}

	String str(Endpoint endpoint) {
		return endpoint.toString();
		// return endpoint.address.getHostName() + ":" + endpoint.port;
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
	public void testOpenSocket() throws UnknownHostException,
	InterruptedException {
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

	/**
	 * ソケットが時間の経過で破棄されるかテスト
	 */
	@Test
	public void testSocketInvalidate() throws Exception {

		SocketManager manager = new SocketManager(endpoints, 5);
		manager.setSocketTimeToLiveInMilli(500);

		// 50msecおきに5つソケットを作成する
		SocketStreams[] pool = new SocketStreams[10];
		for (int i = 0; i < 5; i++) {
			pool[i] = manager.aquire();
			Thread.sleep(50);
		}
		// 作ったソケットをリサイクル
		for (int i = 0; i < 5; i++) {
			manager.recycle(pool[i]);
		}

		// 310msec後、最初のソケットが作られてから560msec立つのでソケット[0][1]は破棄されており、取得できない
		Thread.sleep(310);
		for (int i = 2; i < 5; i++) {
			assertEquals(String.valueOf(i), pool[i], manager.aquire());
		}
		for (int i = 0; i < 2; i++) {
			assertTrue(String.valueOf(i), pool[i].isExpired());
		}

		// 作成しているすべてのソケットを取得したので、新しいソケットができる
		pool[5] = manager.aquire();
		// 新しく追加されたものか確認
		for (int i = 0; i < 5; i++) {
			assertTrue(String.valueOf(i), !pool[i].equals(pool[5]));
		}

		// さらに50msec後、リサイクルを行う
		Thread.sleep(50);
		for (int i = 2; i < 6; i++) {
			manager.recycle(pool[i]);
		}

		// ソケット[2]は破棄されており、、取得できない
		for (int i = 3; i < 6; i++) {
			Assert.assertEquals(String.valueOf(i), pool[i], manager.aquire());
		}
		for (int i = 0; i < 3; i++) {
			assertTrue(String.valueOf(i), pool[i].isExpired());
		}

		// 作成しているすべてのソケットを取得したので、、新しいソケットができる
		pool[6] = manager.aquire();
		// 新しく追加されたものか確認
		for (int i = 0; i < 6; i++) {
			assertTrue(String.valueOf(i), !pool[i].equals(pool[6]));
		}
	}
}
