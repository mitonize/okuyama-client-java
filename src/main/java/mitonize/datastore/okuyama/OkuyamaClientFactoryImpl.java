package mitonize.datastore.okuyama;

import java.net.UnknownHostException;

import mitonize.datastore.CompressionStrategy;
import mitonize.datastore.DefaultCompressionStrategy;
import mitonize.datastore.SocketManager;
import mitonize.datastore.TextDumpFilterStreamFactory;

public class OkuyamaClientFactoryImpl extends OkuyamaClientFactory {

	private static CompressionStrategy DEFAULT_COMPRESSION_STRATEGY = new DefaultCompressionStrategy();

	SocketManager socketManager;
	boolean compatibilityMode = true;
	private CompressionStrategy compressionStrategy;
	private boolean base64key = true;
	private boolean serializeString = true;

	@Override
	public OkuyamaClient createClient() {
		OkuyamaClientImpl2 okuyamaClient;
		okuyamaClient = new OkuyamaClientImpl2(socketManager, base64key, serializeString, compressionStrategy);
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
	 * @throws IllegalArgumentException 指定したホストの形式が不正の場合
	 */
	public OkuyamaClientFactoryImpl(String[] masternodes, int minPoolSize, boolean compatibilityMode, boolean dumpStream, CompressionStrategy compressionStrategy) throws UnknownHostException {
		super.setMasterNodes(masternodes);
		socketManager = new SocketManager(masternodes, minPoolSize);
		if (dumpStream) {
			TextDumpFilterStreamFactory dumpFilterStreamFactory = new TextDumpFilterStreamFactory();
			socketManager.setDumpFilterStreamFactory(dumpFilterStreamFactory);
		}

		setCompressionStrategy(compressionStrategy);
		setCompatibilityMode(compatibilityMode);
	}

	public boolean isCompatibilityMode() {
		return compatibilityMode;
	}

	public void setCompatibilityMode(boolean compatibilityMode) {
		this.compatibilityMode = compatibilityMode;
	}

	public boolean isCompressionMode() {
		return compressionStrategy != null;
	}

	public void setCompressionMode(boolean doCompress) {
		if (doCompress) {
			this.compressionStrategy = DEFAULT_COMPRESSION_STRATEGY;
		} else {
			this.compressionStrategy = null;
		}
	}

	public CompressionStrategy getCompressionStrategy() {
		return compressionStrategy;
	}

	public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
		this.compressionStrategy = compressionStrategy;
	}

	public boolean isBase64key() {
		return base64key;
	}

	public void setBase64key(boolean base64key) {
		this.base64key = base64key;
	}

	public boolean isSerializeString() {
		return serializeString;
	}

	public void setSerializeString(boolean serializeString) {
		this.serializeString = serializeString;
	}

}
