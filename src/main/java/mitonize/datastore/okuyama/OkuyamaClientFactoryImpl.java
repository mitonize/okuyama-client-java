package mitonize.datastore.okuyama;

import java.net.UnknownHostException;

import mitonize.datastore.SocketManager;


public class OkuyamaClientFactoryImpl extends OkuyamaClientFactory {
	SocketManager socketManager;

	@Override
	public OkuyamaClient createClient() {
		OkuyamaClient okuyamaClient;
		okuyamaClient = new OkuyamaClientImpl2(socketManager);
		return okuyamaClient;
	}
	
	public OkuyamaClientFactoryImpl(String[] masternodes) throws UnknownHostException {
		super.setMasterNodes(masternodes);
		socketManager = new SocketManager(masternodes, 5);
	}

}
