package mitonize.datastore.okuyama;

import java.net.UnknownHostException;

import mitonize.datastore.ChannelManager;
import mitonize.datastore.SocketManager;


public class OkuyamaClientFactoryImpl extends OkuyamaClientFactory {
	SocketManager socketManager;
	ChannelManager channelManager;
	boolean useSocketChannel = true;

	@Override
	public OkuyamaClient createClient() {
		OkuyamaClient okuyamaClient;
		if (useSocketChannel) {
			okuyamaClient = new OkuyamaClientImpl(channelManager);
		} else {
			okuyamaClient = new OkuyamaClientImpl2(socketManager);
		}
		return okuyamaClient;
	}
	
	public OkuyamaClientFactoryImpl(String[] masternodes, boolean useSocketChannel) throws UnknownHostException {
		super.setMasterNodes(masternodes);
		channelManager = new ChannelManager(masternodes, 5);
		socketManager = new SocketManager(masternodes, 5);
		this.useSocketChannel = useSocketChannel;
	}

}
