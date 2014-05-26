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

	@Override
	public OkuyamaClient createClient() {
		OkuyamaClientImpl2 okuyamaClient;
		okuyamaClient = new OkuyamaClientImpl2(socketManager, true, compatibilityMode, compressionStrategy);
		return okuyamaClient;
	}

	public OkuyamaClientFactoryImpl(String[] masternodes, int minPoolSize) throws UnknownHostException {
		this(masternodes, minPoolSize, true, false, null);
	}

	public OkuyamaClientFactoryImpl(String[] masternodes, int minPoolSize, boolean compatibilityMode) throws UnknownHostException {
		this(masternodes, minPoolSize, compatibilityMode, false, null);
	}

	public OkuyamaClientFactoryImpl(String[] masternodes, int minPoolSize, boolean compatibilityMode, boolean dumpStream) throws UnknownHostException {
		this(masternodes, minPoolSize, compatibilityMode, dumpStream, null);
	}
	
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

}
