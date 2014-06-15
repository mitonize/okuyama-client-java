package mitonize.datastore.okuyama;

import java.net.UnknownHostException;

import mitonize.datastore.CompressionStrategy;
import mitonize.datastore.DefaultCompressionStrategy;
import mitonize.datastore.SocketManager;
import mitonize.datastore.TextDumpFilterStreamFactory;

public class OkuyamaClientFactoryImpl implements OkuyamaClientFactory {
	private static CompressionStrategy DEFAULT_COMPRESSION_STRATEGY = new DefaultCompressionStrategy();

	private SocketManager socketManager;

	boolean compatibilityMode = true;
	private CompressionStrategy compressionStrategy = null;
	private boolean base64key = true;
	private boolean serializeString = false;

	@Override
	public OkuyamaClient createClient() {
		OkuyamaClientImpl2 okuyamaClient;
		okuyamaClient = new OkuyamaClientImpl2(socketManager, base64key, serializeString || compatibilityMode, compressionStrategy);
		return okuyamaClient;
	}

	/**
	 * マスターノード、最小のソケットプールサイズを指定してファクトリクラスを生成する。
	 * マスターノードの指定は、[ホスト名]:[ポート番号] の形式の文字列の配列である。
	 * オリジナルのOkuyamaClientから読み出せる形式で書き出すようにする（互換モード）。
	 * 
	 * <p>互換モードを設定すると下のような状態で稼働する。</p>
	 * <ul>
	 * <li>キーを必ずbase64エンコードする</li>
	 * <li>文字列を格納する場合にもJavaのシリアライズを行ってからBase64エンコードする</li>
	 * <li>圧縮しない（圧縮設定すると例外を発生させる）</li>
	 * </ul>
	 * 
	 * @param masternodes マスターノード接続先の配列
	 * @param minPoolSize 最小のソケットプールサイズ
	 * @throws UnknownHostException 指定したホスト名のIPアドレスが取得できない場合
	 * @throws IllegalArgumentException 指定したホストの形式が不正の場合
	 */
	public OkuyamaClientFactoryImpl(String[] masternodes, int minPoolSize) throws UnknownHostException {
		this(masternodes, minPoolSize, true, false, null);
	}

	/**
	 * マスターノード、最小のソケットプールサイズ、互換モードを指定してファクトリクラスを生成する。
	 * マスターノードの指定は、[ホスト名]:[ポート番号] の形式の文字列の配列である。
	 * オリジナルのOkuyamaClientからでも読み出し可能な形式で格納するかを指定する（互換モード）。
	 * 互換モードを設定すると下のような状態で稼働する。
	 * <ul>
	 * <li>キーを必ずbase64エンコードする</li>
	 * <li>文字列を格納する場合にもJavaのシリアライズを行ってからBase64エンコードする</li>
	 * <li>圧縮しない（圧縮設定すると例外を発生させる）</li>
	 * </ul>
	 * 
	 * @param masternodes マスターノード接続先の配列
	 * @param minPoolSize 最小のソケットプールサイズ
	 * @param compatibilityMode オリジナルOkuyamaClient互換モード
	 * @throws UnknownHostException 指定したホスト名のIPアドレスが取得できない場合
	 * @throws IllegalArgumentException 指定したホストの形式が不正の場合
	 */
	public OkuyamaClientFactoryImpl(String[] masternodes, int minPoolSize, boolean compatibilityMode) throws UnknownHostException {
		this(masternodes, minPoolSize, compatibilityMode, false, null);
	}

	/**
	 * マスターノード、最小のソケットプールサイズ、互換モード、ストリームのダンプモードを指定してファクトリクラスを生成する。
	 * マスターノードの指定は、[ホスト名]:[ポート番号] の形式の文字列の配列である。
	 * オリジナルのOkuyamaClientからでも読み出し可能な形式で格納するかを指定する（互換モード）。
	 * 互換モードを設定すると下のような状態で稼働する。
	 * <ul>
	 * <li>キーを必ずbase64エンコードする</li>
	 * <li>文字列を格納する場合にもJavaのシリアライズを行ってからBase64エンコードする</li>
	 * <li>圧縮しない（圧縮設定すると例外を発生させる）</li>
	 * </ul>
	 * 
	 * @param masternodes マスターノード接続先の配列
	 * @param minPoolSize 最小のソケットプールサイズ
	 * @param compatibilityMode オリジナルOkuyamaClient互換モード
	 * @param dumpStream 入出力のストリームをダンプする(主にデバッグ用)
	 * @throws UnknownHostException 指定したホスト名のIPアドレスが取得できない場合
	 * @throws IllegalArgumentException 指定したホストの形式が不正の場合
	 */
	public OkuyamaClientFactoryImpl(String[] masternodes, int minPoolSize, boolean compatibilityMode, boolean dumpStream) throws UnknownHostException {
		this(masternodes, minPoolSize, compatibilityMode, dumpStream, null);
	}
	
	/**
	 * マスターノード、最小のソケットプールサイズ、互換モード、ストリームのダンプモードを指定してファクトリクラスを生成する。
	 * マスターノードの指定は、[ホスト名]:[ポート番号] の形式の文字列の配列である。
	 * ストリームのダンプを指定すると標準出力にOkuyamaマスターノードへの入出力データをダンプする。
	 * 圧縮戦略を指定すると、キーのパターンや値のサイズに応じで圧縮の有無、圧縮アルゴリズムの選択ができる。(参照 {@link CompressionStrategy})
	 * 
	 * <p>
	 * 互換モードを指定すると、オリジナルのOkuyamaClientからでも読み出し可能な形式で格納するように下の設定でクライアントを生成する。
	 * </p>
	 * <ul>
	 * <li>キーを必ずbase64エンコードする</li>
	 * <li>文字列を格納する場合にもJavaのシリアライズを行ってからBase64エンコードする</li>
	 * <li>圧縮しない（圧縮戦略を設定すると例外を発生させる）</li>
	 * </ul>
	 * 
	 * @param masternodes マスターノード接続先の配列
	 * @param minPoolSize 最小のソケットプールサイズ
	 * @param compatibilityMode オリジナルOkuyamaClient互換モード
	 * @param dumpStream 入出力のストリームをダンプする(主にデバッグ用)
	 * @param compressionStrategy 圧縮戦略
	 * @throws UnknownHostException 指定したホスト名のIPアドレスが取得できない場合
	 * @throws IllegalArgumentException 指定したホストの形式が不正の場合、互換モードを指定しているにもかかわらず圧縮戦略を指定した場合
	 */
	public OkuyamaClientFactoryImpl(String[] masternodes, int minPoolSize, boolean compatibilityMode, boolean dumpStream, CompressionStrategy compressionStrategy) throws UnknownHostException {
		socketManager = new SocketManager(masternodes, minPoolSize);
		if (dumpStream) {
			TextDumpFilterStreamFactory dumpFilterStreamFactory = new TextDumpFilterStreamFactory();
			socketManager.setDumpFilterStreamFactory(dumpFilterStreamFactory);
		}

		setCompatibilityMode(compatibilityMode);
		setCompressionStrategy(compressionStrategy);
	}

	/**
	 * 互換モードが設定されているかを確認する。
	 * @return 互換モードに設定されているなら true
	 */
	public boolean isCompatibilityMode() {
		return compatibilityMode;
	}

	/**
	 * 互換モードを設定する。trueが設定された場合は圧縮は無効化される。
	 * 
	 * @param compatibilityMode 互換モードに設定するなら true
	 */
	public void setCompatibilityMode(boolean compatibilityMode) {
		if (compatibilityMode) {
			setCompressionMode(false);
		}
		this.compatibilityMode = compatibilityMode;
	}

	/**
	 * 圧縮戦略を取得する。
	 * @return 設定されている圧縮戦略。未設定ならnull
	 */
	public CompressionStrategy getCompressionStrategy() {
		return compressionStrategy;
	}

	/**
	 * 圧縮戦略を設定する。簡易的なものは{@link DefaultCompressionStrategy}で提供される。設定を解除する場合はnullを設定する。
	 * @param compressionStrategy 圧縮戦略。解除する場合はnull
	 */
	public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
		if (compatibilityMode && compressionStrategy != null) {
			throw new IllegalArgumentException("Compression is not supported in compatible mode");
		}
		this.compressionStrategy = compressionStrategy;
	}

	/**
	 * 圧縮モードを取得する。
	 * @return 圧縮する場合はtrue
	 */
	public boolean isCompressionMode() {
		return compressionStrategy != null;
	}

	/**
	 * 圧縮モードを設定する。圧縮する場合はデフォルトの圧縮戦略{@link DefaultCompressionStrategy}が利用される。
	 * @param doCompress 圧縮する場合はtrue
	 */
	public void setCompressionMode(boolean doCompress) {
		if (doCompress) {
			this.compressionStrategy = DEFAULT_COMPRESSION_STRATEGY;
		} else {
			this.compressionStrategy = null;
		}
	}

	/**
	 * キーをBase64でエンコードするかを返す。
	 * @return キーをBase64でエンコードするならtrue
	 */
	public boolean isBase64key() {
		return base64key;
	}

	/**
	 * キーをBase64でエンコードする設定をする。(デフォルト:true)
	 * @param キーをBase64でエンコードするならtrue
	 */
	public void setBase64key(boolean base64key) {
		this.base64key = base64key;
	}

	/**
	 * 文字列を格納する際にJavaのシリアライズをするかを返す。
	 * @return 文字列を格納する際にJavaのシリアライズをするならtrue
	 */
	public boolean isSerializeString() {
		return serializeString;
	}

	/**
	 * 文字列を格納する際にJavaのシリアライズをするかを設定する。(デフォルト:false)
	 * 互換モードが設定されている場合は強制的にシリアライズされる。
	 * 
	 * @return 文字列を格納する際にJavaのシリアライズをするならtrue
	 */
	public void setSerializeString(boolean serializeString) {
		this.serializeString = serializeString;
	}

	/* DELEGATED METHODS */
	/**
	 * 保持するソケットの最大数を取得する。
	 * 一時的に同時利用ソケット数がこの上限値を超える場合があるが、同時使用数が落ち着くと、
	 * この上限数分のソケットは切断されずに保持される。
	 */
	public int getMaxPoolSize() {
		return socketManager.getMaxPoolSize();
	}

	/**
	 * 同時に開いたソケットの最大値を取得する。
	 * @return 同時に開いたソケットの最大値
	 */
	public int getMaxCoucurrentSockets() {
		return socketManager.getMaxCoucurrentSockets();
	}

	/**
	 * ソケットの読み取りタイムアウト時間(ミリ秒)を取得する。
	 * @return ソケットの読み取りタイムアウト時間(ミリ秒)
	 */
	public int getTimeoutToReadInMillis() {
		return socketManager.getTimeoutToReadInMillis();
	}

	/**
	 * ソケットの読み取りタイムアウト時間(ミリ秒)を設定する(デフォルト:1000ミリ秒)
	 * @param timeoutToReadInMillis ソケットの読み取りタイムアウト時間(ミリ秒)
	 */
	public void setTimeoutToReadInMillis(int timeoutToReadInMillis) {
		socketManager.setTimeoutToReadInMillis(timeoutToReadInMillis);
	}

	/**
	 * TCP接続が確立するかどうか確認するときのコネクションタイムアウト時間(ミリ秒)を取得する。
	 * @return TCP接続が確立するかどうか確認するときのコネクションタイムアウト時間(ミリ秒)
	 */
	public int getTimeoutToConnectInMillis() {
		return socketManager.getTimeoutToConnectInMillis();
	}

	/**
	 * TCP接続が確立するかどうか確認するときのコネクションタイムアウト時間(ミリ秒)を設定する(デフォルト:1000ミリ秒)。
	 * @param timeoutToConnectInMillis TCP接続が確立するかどうか確認するときのコネクションタイムアウト時間(ミリ秒)
	 */
	public void setTimeoutToConnectInMillis(int timeoutToConnectInMillis) {
		socketManager.setTimeoutToConnectInMillis(timeoutToConnectInMillis);
	}

	/**
	 * オフライン状態からTCP接続が確立後にオンラインにするまでに待つ時間(ミリ秒)を取得
	 * @return オフライン状態からTCP接続が確立後にオンラインにするまでに待つ時間(ミリ秒)
	 */
	public int getDelayToMarkOnlineInMillis() {
		return socketManager.getDelayToMarkOnlineInMillis();
	}

	/**
	 * オフライン状態からTCP接続が確立後にオンラインにするまでに待つ時間(ミリ秒)を設定する(デフォルト:3000ミリ秒)。
	 * TCPポートが開いてから実際に待ち受け可能になるまで時間がかかるサーバの場合に指定する。
	 * 不要な場合は0でよい。
	 * @param delayToMarkOnlineInMillis オフライン状態からTCP接続が確立後にオンラインにするまでに待つ時間(ミリ秒)
	 */
	public void setDelayToMarkOnlineInMillis(int delayToMarkOnlineInMillis) {
		socketManager.setDelayToMarkOnlineInMillis(delayToMarkOnlineInMillis);
	}
}
