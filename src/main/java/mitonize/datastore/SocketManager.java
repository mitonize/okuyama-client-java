package mitonize.datastore;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.channels.UnresolvedAddressException;
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

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append(address.getHostName());
			builder.append(":");
			builder.append(port);
			if (offline) {
				builder.append(" (offline)");
			}
			return builder.toString();
		}
	}

	/**
	 * 接続先のエンドポイント(ホスト名とポートの組で"hostname:port"の形式)の配列。
	 * ラウンドロビンで選択されるが、接続がオフラインであることを検知するとラウンドロビン対象から外される。
	 * ただし、すべての接続先がオフラインの時は1番目(添字0)の要素が返却される。
	 */
	Endpoint[] endpoints;

	/**
	 * 直近で新規に開いたソケットをプールに格納した時刻。初期値は0。
	 */
	long timestampLatelyPooled = 0;

	/**
	 * 同時に開いているソケットの最大値。
	 */
	int maxCountOfCoucurrentSockets = 0;

	/**
	 * オフライン状態になったエンドポイントがオンラインになったかをバックグラウンドで検査してオンライン状態にするExecutor。
	 */
	final ScheduledExecutorService offlineManagementService;

	/**
	 * オフライン状態からTCP接続が確立してからオンラインにするまでに待つ時間(ミリ秒)。
	 * TCPポートが開いてから実際に待ち受け可能になるまで時間がかかるサーバの場合に指定する。
	 * 不要な場合は0でよい。
	 */
	private int delayToMarkOnlineInMillis = 3000;

	/**
	 * TCP接続が確立するかどうか確認するときのコネクションタイムアウト時間(ミリ秒)。
	 */
	private int timeoutToConnectInMillis = 1000;

	/**
	 * ソケットの読み取りタイムアウト時間(ミリ秒)。
	 */
	private int timeoutToReadInMillis = 1000;

	/**
	 * オープンしているソケット、入出力ストリームをプールするためのキュー。
	 * 要求された際にキューから取り出し、使用後に返却された時にキューに戻す。
	 */
	final protected ArrayBlockingQueue<SocketStreams> queue;
	final protected AtomicInteger activeSocketCount;
	final protected AtomicInteger currentEndpointIndex;

	private int maxPoolSize = 0;

	private DumpFilterStreamFactory dumpFilterStreamFactory;

	/**
	 * ソケットの生存期間[ミリ秒]（デフォルトは5分）
	 */
	private long socketTimeToLiveInMilli = 5 * 60 * 1000;

	public SocketManager(String[] masternodes, int maxPoolSize) throws UnknownHostException {
		if (masternodes.length == 0) {
			throw new IllegalStateException("No connection endpoint setting specified.");
		}
		this.offlineManagementService = Executors.newSingleThreadScheduledExecutor();
		this.queue = new ArrayBlockingQueue<SocketStreams>(maxPoolSize);
		this.activeSocketCount = new AtomicInteger(0);
		this.currentEndpointIndex = new AtomicInteger(0);
		this.maxPoolSize = maxPoolSize;
		setEndpoints(masternodes);
	}

	private void setEndpoints(String[] nodes) throws UnknownHostException {
		this.endpoints = new Endpoint[nodes.length];
		for (int i = 0; i < nodes.length; ++i) {
			String hostname = nodes[i].split(":")[0];
			int port = Integer.parseInt(nodes[i].split(":")[1]); // May throws NumberFormatException

			if (!hostname.matches("[0-9a-zA-Z.-]+")) {
				throw new IllegalArgumentException("hostname contains illegal character. " + hostname);
			}
			if (port < 0 || port > 65535) {
				throw new IllegalArgumentException("port number must in range 0 to 65535. " + port);
			}
			endpoints[i] = new Endpoint(InetAddress.getByName(hostname), port);
		}
	}

	@Deprecated
	public void setDumpStream(boolean b) {
		dumpFilterStreamFactory = new TextDumpFilterStreamFactory();
	}

	public boolean isDumpStream() {
		return dumpFilterStreamFactory != null;
	}

	/**
	 * プールしているソケットを取り出して返却する。
	 * 取り出した時にそのソケットが有効かをチェックして、無効なら新規にソケットを開く。
	 * 新規のソケットは登録されているエンドポイントのうちラウンドロビンで選択され、接続に成功したものとなる。
	 *
	 * @return 有効な接続先のソケット。
	 * @throws IOException 有効な接続先が1つもない場合。
	 */
	public SocketStreams aquire() throws IOException {
		SocketStreams socket = queue.poll();

		if(socket == null){
			return openSocket();
		}

		// ソケットに問題がある場合、そのソケットは利用せず、次のソケットを利用する
		if(!isAvailable(socket)){
			// closeSocket(socket);
			return aquire();
		}

		return socket;
	}

	/**
	 * プールから取り出されたソケットを返却する。
	 * 新規にオープンしたソケットのタイムスタンプを管理して、プールの上限数に達した最後のタイムスタンプよりも
	 * 新しいソケットはキューに戻されない。タイムスタンプよりも古いソケットが無効だった場合はプール数が減るが、
	 * 同時接続数が有効なプール数よりも増えた段階で新規に開かれ、タイムスタンプも更新される。
	 *
	 * @param socket プールから取り出したソケット
	 */
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
			int timeout = getTimeoutToConnectInMillis();
			int delay = getDelayToMarkOnlineInMillis();
			InetSocketAddress address = new InetSocketAddress(endpoint.address, endpoint.port);
			Socket socket = new Socket();
			try {
				// 接続に成功したらオフラインマークをクリアする。
				socket.connect(address, timeout);
				Thread.sleep(delay);
				endpoint.markEndpointOffline(false);
			} catch (IOException e) {
			} catch (InterruptedException e) {
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
	 * 新しくソケットを開く。登録されているエンドポイントをラウンドロビンで順次選択し、接続が確立するまで繰り返す。
	 * 最後に失敗した接続試行と今回取得したエンドポイントが同じであればそれ以上の試行は行わず、例外を投げる。
	 * @return 開かれたソケット
	 * @throws IOException 有効な接続先が1つもないとき
	 */
	@SuppressWarnings("resource")
	SocketStreams openSocket() throws IOException {
		int timeoutToConnect = timeoutToConnectInMillis;
		int timeoutToRead = timeoutToReadInMillis;

		Endpoint lastAttempt = null;
		for (int i=endpoints.length; i >= 0; --i) {
			Endpoint endpoint = nextEndpoint(); // nextEndpoint() never returns null.
			if (endpoint.equals(lastAttempt)) {
				// 一連の接続試行の最後に試したエンドポイントと今回取得したエンドポイントが同じなら失敗させる。
				break;
			}
			try {
				InetSocketAddress address = new InetSocketAddress (endpoint.address, endpoint.port);
				Socket socket = new Socket();
				socket.setSoTimeout(timeoutToRead);
				socket.connect(address, timeoutToConnect);
				OutputStream os = new BufferedOutputStream(socket.getOutputStream());
				InputStream is = new BufferedInputStream(socket.getInputStream());
				if (dumpFilterStreamFactory != null) {
					is = dumpFilterStreamFactory.wrapInputStream(is);
					os = dumpFilterStreamFactory.wrapOutputStream(os);
				}
				SocketStreams s = new SocketStreams(socket, os, is, socketTimeToLiveInMilli);
				int c = activeSocketCount.incrementAndGet();
				// プールする上限数に収まっていれば最近のタイムスタンプを保持しておく。
				// プール上限数を超えた場合、このタイムスタンプより新しいソケットはrecycleでプールに戻されない。
				if (c <= maxPoolSize) {
					timestampLatelyPooled = s.timestamp;
				}
				if (c > maxCountOfCoucurrentSockets) {
					maxCountOfCoucurrentSockets = c;
				}
				if (logger.isInfoEnabled()) {
					logger.info("Socket opened - {}:{} count:{}", endpoint.address.getHostName(), endpoint.port, c);
				}
				if (endpoint.offline) {
					endpoint.markEndpointOffline(false);
				}
				return s;
			} catch (UnresolvedAddressException e) {
				logger.error("Hostname cannot be resolved. {}:{} {}", endpoint.address.getHostName(), endpoint.port, e.getMessage());
				endpoint.markEndpointOffline(true);
			} catch (IOException e) {
				logger.error("Failed to open socket. {}:{} {}", endpoint.address.getHostName(), endpoint.port, e.getMessage());
				endpoint.markEndpointOffline(true);
			}
			lastAttempt = endpoint;
		}
		logger.error("No available endpoint to connect");
		throw new IOException("No available endpoint to connect");
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
		if(streams.isExpired()){
			closeSocket(streams);
			return false;
		}

		Socket s = streams.getSocket();
		if (s.isInputShutdown() || s.isOutputShutdown() || s.isClosed()) {
			try {
				s.close();
			} catch (IOException e) {
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

	public int getMaxCoucurrentSockets() {
		return maxCountOfCoucurrentSockets;
	}

	public void setDumpFilterStreamFactory(
			DumpFilterStreamFactory dumpFilterStreamFactory) {
		this.dumpFilterStreamFactory = dumpFilterStreamFactory;
	}

	/**
	 * ソケットの読み取りタイムアウト時間(ミリ秒)を取得する。
	 * @return ソケットの読み取りタイムアウト時間(ミリ秒)
	 */
	public int getTimeoutToReadInMillis() {
		return timeoutToReadInMillis;
	}

	/**
	 * ソケットの読み取りタイムアウト時間(ミリ秒)を設定する(デフォルト:1000ミリ秒)
	 * @param timeoutToReadInMillis ソケットの読み取りタイムアウト時間(ミリ秒)
	 */
	public void setTimeoutToReadInMillis(int timeoutToReadInMillis) {
		this.timeoutToReadInMillis = timeoutToReadInMillis;
	}

	/**
	 * TCP接続が確立するかどうか確認するときのコネクションタイムアウト時間(ミリ秒)を取得する。
	 * @return TCP接続が確立するかどうか確認するときのコネクションタイムアウト時間(ミリ秒)
	 */
	public int getTimeoutToConnectInMillis() {
		return timeoutToConnectInMillis;
	}

	/**
	 * TCP接続が確立するかどうか確認するときのコネクションタイムアウト時間(ミリ秒)を設定する(デフォルト:1000ミリ秒)。
	 * @param timeoutToConnectInMillis TCP接続が確立するかどうか確認するときのコネクションタイムアウト時間(ミリ秒)
	 */
	public void setTimeoutToConnectInMillis(int timeoutToConnectInMillis) {
		this.timeoutToConnectInMillis = timeoutToConnectInMillis;
	}

	/**
	 * オフライン状態からTCP接続が確立後にオンラインにするまでに待つ時間(ミリ秒)を取得
	 * @return オフライン状態からTCP接続が確立後にオンラインにするまでに待つ時間(ミリ秒)
	 */
	public int getDelayToMarkOnlineInMillis() {
		return delayToMarkOnlineInMillis;
	}

	/**
	 * オフライン状態からTCP接続が確立後にオンラインにするまでに待つ時間(ミリ秒)を設定する(デフォルト:3000ミリ秒)。
	 * TCPポートが開いてから実際に待ち受け可能になるまで時間がかかるサーバの場合に指定する。
	 * 不要な場合は0でよい。
	 * @param delayToMarkOnlineInMillis オフライン状態からTCP接続が確立後にオンラインにするまでに待つ時間(ミリ秒)
	 */
	public void setDelayToMarkOnlineInMillis(int delayToMarkOnlineInMillis) {
		this.delayToMarkOnlineInMillis = delayToMarkOnlineInMillis;
	}

	/**
	 * 新規に作成されてから設定値の期間経過したソケットは、そのソケットがプールから選択されるタイミングで削除され、利用されなくなります。
	 *
	 * @param socketTimeToLiveInMilli ソケットの生存期間[ミリ秒]（デフォルトは5分）
	 */
	public void setSocketTimeToLiveInMilli(long socketTimeToLiveInMilli) {
		this.socketTimeToLiveInMilli = socketTimeToLiveInMilli;
	}

	public void shutdown() {
		this.offlineManagementService.shutdown();
	}
}
