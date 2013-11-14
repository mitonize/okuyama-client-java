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
import java.util.concurrent.PriorityBlockingQueue;
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
		boolean offline = false;
	}
	Endpoint[] endpoints;
	int currentEndpointIndex = 0;

	protected PriorityBlockingQueue<SocketStreams> queue;
	protected AtomicInteger activeSocketCount;
	private boolean dumpStream = false;
	private int maxPoolSize = 0;

	public SocketManager(String[] masternodes, int maxPoolSize) throws UnknownHostException {
		if (masternodes.length == 0) {
			throw new IllegalStateException("No connection endpoint setting specified.");
		}
		this.queue = new PriorityBlockingQueue<SocketStreams>(maxPoolSize);
		this.activeSocketCount = new AtomicInteger(0);
		this.maxPoolSize = maxPoolSize;
		this.endpoints = new Endpoint[masternodes.length];
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

	/**
	 * 次のエンドポイント情報を取得する。
	 * オフラインマークが付いているものはスキップする。登録されているすべてのエンドポイントにオフラインマークが付いていた場合はnullを返す。
	 * @return オフラインでないエンドポイント情報。すべてオフラインの場合はnull
	 */
	Endpoint nextEndpoint() {
		int count = endpoints.length;
		int index = currentEndpointIndex;
		while (count-- > 0) {
			if (index >= endpoints.length)
				index = 0;

			Endpoint endpoint = endpoints[index];
			if (endpoint == null)
				throw new IllegalStateException("No connection endpoint setting specified.");
			/** オフラインのマークが付いていたら次のものを検査 */
			if (endpoint.offline) {
				continue;
			}
			return endpoint;
		}
		return null;
	}

	@SuppressWarnings("resource")
	SocketStreams openSocket() throws IOException {
		while (true) {
			Endpoint endpoint = nextEndpoint();
			if (endpoint == null) {
				logger.error("No available endpoint to connect");
				throw new IOException("No available endpoint to connect");
			}
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
					logger.info("Socket opened - {}:{} count:{}", endpoint.address.getHostName(), endpoint.port, c);
				}
				return new SocketStreams(socket, os, is);
			} catch (UnresolvedAddressException e) {
				logger.error("Hostname cannot be resolved. {}", e.getMessage());
				endpoint.offline = true;
			} catch (IOException e) {
				endpoint.offline = true;
			}
		}
	}
	
	void closeSocket(SocketStreams socket) {
		try {
			socket.getOutputStream().close();
			socket.getInputStream().close();
			socket.getSocket().close();
			int c = activeSocketCount.decrementAndGet();
			if (logger.isInfoEnabled()) {
				Socket s = socket.getSocket();
				logger.info("Socket closed - {}:{} count:{}", s.getInetAddress().getHostName(), s.getPort(), c);
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
			for (Endpoint e: endpoints) {
				if (e.address.equals(s.getInetAddress()) && e.port == s.getPort()) {
					logger.info("Mark offline - {}:{}", s.getInetAddress().getHostName(), s.getPort());
					e.offline = true;
				}
			}
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

	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
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
