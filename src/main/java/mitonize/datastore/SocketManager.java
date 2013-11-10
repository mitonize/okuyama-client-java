package mitonize.datastore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.UnresolvedAddressException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
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
		boolean online = true;
	}
	Endpoint[] endpoints;
	int currentEndpointIndex = 0;

	protected ArrayBlockingQueue<SocketStreams> queue;
	protected AtomicInteger activeSocketCount;
	private boolean dumpStream = false;

	public SocketManager(String[] masternodes, int maxPoolSize) throws UnknownHostException {
		queue = new ArrayBlockingQueue<SocketStreams>(maxPoolSize);
		activeSocketCount = new AtomicInteger(0);
		endpoints = new Endpoint[masternodes.length];
		for (int i = 0; i < masternodes.length; ++i) {
			endpoints[i] = new Endpoint();
			endpoints[i].address = InetAddress.getByName(masternodes[i].split(":")[0]);
			endpoints[i].port = Short.parseShort(masternodes[i].split(":")[1]);
		}
	}
	
	public void setDumpStream(boolean b) {
		dumpStream = b;
	}

	public boolean isDumpStream() {
		return dumpStream;
	}

	public SocketStreams aquire() throws IOException {
		SocketStreams socket = queue.poll();
		return (socket == null || !isAvailable(socket)) ? openSocket() : socket;
	}

	public void recycle(SocketStreams socket) {
		if (socket == null)
			return;
		if (!isAvailable(socket) || !queue.offer(socket)) {
			closeSocket(socket);
		}
	}

	@SuppressWarnings("resource")
	SocketStreams openSocket() throws IOException {
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
			OutputStream os = new BufferedOutputStream(socket.getOutputStream());
			InputStream is = new BufferedInputStream(socket.getInputStream());
			Charset cs = Charset.forName("UTF-8");
			if (dumpStream) {
				os = new DumpFilterOutputStream(os, cs);
				is = new DumpFilterInputStream(is, cs);
			}
			int c = activeSocketCount.incrementAndGet();
			if (logger.isInfoEnabled()) {
				logger.info("Socket opened - count:" + c + " queue:" + queue.size());
			}
			return new SocketStreams(socket, os, is);
		} catch (UnresolvedAddressException e) {
			logger.error("Hostname cannot be resolved. {}", e.getMessage());
			return null;
		}
	}
	
	void closeSocket(SocketStreams socket) {
		try {
			socket.getOutputStream().close();
			socket.getInputStream().close();
			socket.getSocket().close();
			int c = activeSocketCount.decrementAndGet();
			if (logger.isInfoEnabled()) {
				logger.info("Socket closed - count:" + c + " queue:" + queue.size());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private boolean isAvailable(SocketStreams socket) {
		Socket s = socket.getSocket();
		if (s.isInputShutdown() || s.isOutputShutdown() || s.isClosed()) {
			return false;
		}
		return true;
	}

	public void destroy(SocketStreams socket) {
		if (socket == null)
			return;
		try {
			socket.getOutputStream().close();
			socket.getInputStream().close();
		} catch (IOException e) {
		}
	}

}

class DumpFilterOutputStream extends FilterOutputStream {
	Charset cs;
	CharsetDecoder decoder;
	CharBuffer buffer = CharBuffer.allocate(256);
	private int charsInLine ;

	protected DumpFilterOutputStream(OutputStream os, Charset cs) {
		super(os);
		this.cs = cs;
		decoder = cs.newDecoder();
	}

	@Override
	public void write(int b) throws IOException {
		super.write(b);
		++charsInLine;
		if (charsInLine < 256) {
			decoder.decode(ByteBuffer.wrap(new byte[]{(byte) b}), buffer, false);
		}
		boolean newline = false;
		if (b == '\n') {
			charsInLine = 0;
			newline = true;
			System.out.print("OUTPUT: ");
		}
		if (newline || !buffer.hasRemaining()) {
			buffer.flip();
			System.out.print(buffer.toString());
			buffer.clear();
		}
	}
}

class DumpFilterInputStream extends FilterInputStream {
	Charset cs;
	CharsetDecoder decoder;
	CharBuffer buffer = CharBuffer.allocate(256);
	private int charsInLine;

	protected DumpFilterInputStream(InputStream is, Charset cs) {
		super(is);
		this.cs = cs;
		decoder = cs.newDecoder();
	}

	@Override
	public int read() throws IOException {
		int b = super.read();
		++charsInLine;
		if (charsInLine < 256) {
			decoder.decode(ByteBuffer.wrap(new byte[]{(byte) b}), buffer, false);
		}
		boolean newline = false;
		if (b == '\n') {
			charsInLine = 0;
			newline = true;
			System.out.print(" INTPUT: ");
		}
		if (newline || !buffer.hasRemaining()) {
			buffer.flip();
			System.out.print(buffer.toString());
			buffer.clear();
		}
		return b;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int read = super.read(b, off, len);

		for (int i=off; i < off + read; ++i) {
			int c = b[i];
			++charsInLine;
			if (charsInLine < 256) {
				decoder.decode(ByteBuffer.wrap(new byte[]{(byte) c}), buffer, false);
			}
			boolean newline = false;
			if (c == '\n') {
				charsInLine = 0;
				newline = true;
				System.out.print(" INPUT: ");
			}
			if (newline || !buffer.hasRemaining()) {
				buffer.flip();
				System.out.print(buffer.toString());
				buffer.clear();
			}
		}
		return read;
	}

}
