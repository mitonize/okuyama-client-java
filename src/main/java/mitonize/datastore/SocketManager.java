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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SocketManager {
	Logger logger = LoggerFactory.getLogger(SocketManager.class);
	/**
	 * 接続先のTCPエンドポイントを保持
	 */
	class Endpoint {
		public Endpoint(InetAddress address, int port) {
			this.address = address;
			this.port = port;
		}
		final InetAddress address;
		final int port;
		private boolean offline = false;
		/** for canceling on being online */
		ScheduledFuture<Object> offlineManagementFuture;
		
		/**
		 * 指定したエンドポイントのオフライン状態を設定する。
		 * @param endpoint エンドポイント
		 * @param offline オフラインかどうか (true:オフライン、false:オンライン)
		 */
		@SuppressWarnings("unchecked")
		synchronized void markEndpointOffline(boolean offline) {
			if (offline) {
				if (!this.offline) {
					OfflineManagementTask task = new OfflineManagementTask(this);
					this.offlineManagementFuture = 
							(ScheduledFuture<Object>) offlineManagementService.scheduleWithFixedDelay(task, 0, 5, TimeUnit.SECONDS);
					this.offline = true;
					logger.warn("Mark offline - {}:{}", this.address.getHostName(), this.port);
				}
			} else {
				ScheduledFuture<Object> future = this.offlineManagementFuture;
				if (future != null) {
					future.cancel(true);
					this.offlineManagementFuture = null;
				}
				this.offline = false;
				logger.warn("Mark online  - {}:{}", this.address.getHostName(), this.port);
			}
		}
	}
	Endpoint[] endpoints;
	long timestampLatelyPooled = 0;

	final ScheduledExecutorService offlineManagementService;

	final protected ArrayBlockingQueue<SocketStreams> queue;
	final protected AtomicInteger activeSocketCount;
	final protected AtomicInteger currentEndpointIndex;
	private boolean dumpStream = false;
	private int maxPoolSize = 0;

	public SocketManager(String[] masternodes, int maxPoolSize) throws UnknownHostException {
		if (masternodes.length == 0) {
			throw new IllegalStateException("No connection endpoint setting specified.");
		}
		this.offlineManagementService = Executors.newSingleThreadScheduledExecutor();
		this.queue = new ArrayBlockingQueue<SocketStreams>(maxPoolSize);
		this.activeSocketCount = new AtomicInteger(0);
		this.currentEndpointIndex = new AtomicInteger(0);
		this.maxPoolSize = maxPoolSize;
		this.endpoints = new Endpoint[masternodes.length];
		for (int i = 0; i < masternodes.length; ++i) {
			String hostname = masternodes[i].split(":")[0];
			int port = Integer.parseInt(masternodes[i].split(":")[1]); // May throws NumberFormatException
			
			if (!hostname.matches("[\\d\\w.]+")) {
				throw new IllegalArgumentException("hostname contains illegal character. " + hostname);
			}
			if (port < 0 || port > 65535) {
				throw new IllegalArgumentException("port number must in range 0 to 65535. " + port);
			}			
			endpoints[i] = new Endpoint(InetAddress.getByName(hostname), port);
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
		if (socket.timestamp > timestampLatelyPooled
				|| !isAvailable(socket)
				|| !queue.offer(socket)) {
			closeSocket(socket);
		}
	}
	
	/**
	 * オフラインとマークされたエンドポイントを定期的にチェックして復帰していればオフラインとしてマークする。
	 */
	class OfflineManagementTask implements Runnable {
		private Endpoint endpoint;
		public OfflineManagementTask(Endpoint endpoint) {
			this.endpoint = endpoint;
		}
		@Override
		public void run() {
			InetSocketAddress address = new InetSocketAddress(endpoint.address, endpoint.port);
			Socket socket = new Socket();
			try {
				// 接続に成功したらオフラインマークをクリアする。
				socket.connect(address, 1000);
				endpoint.markEndpointOffline(false);
			} catch (IOException e) {
			} finally {
				try {
					socket.close();
				} catch (IOException e) {
				}
			}
		}
	}

	/**
	 * 指定したインデックスのエンドポイント情報を取得する。
	 * @return エンドポイント情報。
	 */
	Endpoint getEndpointAt(int i) {
		return endpoints[i];
	}

	/**
	 * 次のエンドポイント情報を取得する。
	 * オフラインマークが付いているものはスキップする。登録されているすべてのエンドポイントにオフラインマークが付いていた場合は1番目の要素を返す。
	 * @return オフラインでないエンドポイント情報。すべてオフラインの場合は1番目の要素を返す。
	 */
	Endpoint nextEndpoint() {
		int count = endpoints.length;
		int index = currentEndpointIndex.getAndSet(currentEndpointIndex.incrementAndGet() % count);
		while (count-- > 0) {
			if (index >= endpoints.length)
				index = 0;

			Endpoint endpoint = endpoints[index++];
			if (endpoint == null)
				throw new IllegalStateException("No connection endpoint setting specified.");
			/** オフラインのマークが付いていないならこれを返す */
			if (!endpoint.offline) {
				return endpoint;
			}
		}
		return endpoints[0];
	}
	
	/**
	 * 指定したソケットに関連づいているエンドポイントのオフライン状態を設定する。
	 * @param socket ソケットオブジェクト
	 * @param offline オフラインかどうか (true:オフライン、false:オンライン)
	 */
	void markEndpointOffline(Socket socket, boolean offline) {
		for (Endpoint e: endpoints) {
			if (e.address.equals(socket.getInetAddress()) && e.port == socket.getPort()) {
				e.markEndpointOffline(offline);
			}
		}
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
				SocketStreams s = new SocketStreams(socket, os, is);
				int c = activeSocketCount.incrementAndGet();
				// プールする上限数に収まっていれば最近のタイムスタンプを保持しておく。
				// プール上限数を超えた場合、このタイムスタンプより新しいソケットはrecycleでプールに戻されない。
				if (c <= maxPoolSize) {
					timestampLatelyPooled = s.timestamp;
				}
				if (logger.isInfoEnabled()) {
					logger.info("Socket opened - {}:{} count:{}", endpoint.address.getHostName(), endpoint.port, c);
				}
				return s;
			} catch (UnresolvedAddressException e) {
				endpoint.markEndpointOffline(true);
				logger.error("Hostname cannot be resolved. {}:{} {}", endpoint.address.getHostName(), endpoint.port, e.getMessage());
			} catch (IOException e) {
				endpoint.markEndpointOffline(true);
				logger.error("IOException is thrown. {}:{} {}", endpoint.address.getHostName(), endpoint.port, e.getMessage());
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

	private boolean isAvailable(SocketStreams streams) {
		Socket s = streams.getSocket();
		if (s.isInputShutdown() || s.isOutputShutdown() || s.isClosed()) {
			try {
				s.close();
			} catch (IOException e) {
			} finally {
				markEndpointOffline(s, true);				
			}
			return false;
		}
		return true;
	}

	public void destroy(SocketStreams streams) {
		if (streams == null)
			return;
		Socket s = streams.getSocket();
		logger.info("Destroy connection - {}:{}", s.getInetAddress().getHostName(), s.getPort());
		try {
			streams.getOutputStream().close();
			streams.getInputStream().close();
			streams.getSocket().close();
		} catch (IOException e) {
		}
	}

	public int getMaxPoolSize() {
		return maxPoolSize;
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
