package mitonize.datastore.okuyama;

import java.net.UnknownHostException;

import mitonize.datastore.SocketManager;


public class OkuyamaClientFactoryImpl extends OkuyamaClientFactory {
	SocketManager socketManager;
	boolean compatibilityMode = true;
	boolean compressionMode = false;

	@Override
	public OkuyamaClient createClient() {
		OkuyamaClient okuyamaClient;
		okuyamaClient = new OkuyamaClientImpl2(socketManager, true, compatibilityMode, compressionMode);
		return okuyamaClient;
	}

	public OkuyamaClientFactoryImpl(String[] masternodes, int minPoolSize) throws UnknownHostException {
		this(masternodes, minPoolSize, true, false);
	}

	public OkuyamaClientFactoryImpl(String[] masternodes, int minPoolSize, boolean compatibilityMode) throws UnknownHostException {
		this(masternodes, minPoolSize, compatibilityMode, false);
	}
	
	public OkuyamaClientFactoryImpl(String[] masternodes, int minPoolSize, boolean compatibilityMode, boolean dumpStream) throws UnknownHostException {
		super.setMasterNodes(masternodes);
		socketManager = new SocketManager(masternodes, minPoolSize);
		socketManager.setDumpStream(dumpStream);
		setCompatibilityMode(compatibilityMode);
	}

	public boolean isCompatibilityMode() {
		return compatibilityMode;
	}

	public void setCompatibilityMode(boolean compatibilityMode) {
		this.compatibilityMode = compatibilityMode;
	}

	public boolean isCompressionMode() {
		return compressionMode;
	}

	public void setComressionMode(boolean doCompress) {
		this.compressionMode = doCompress;		
	}

}
