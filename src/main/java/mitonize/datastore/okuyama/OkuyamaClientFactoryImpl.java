package mitonize.datastore.okuyama;

import java.net.UnknownHostException;

import mitonize.datastore.ChannelManager;


public class OkuyamaClientFactoryImpl extends OkuyamaClientFactory {
	ChannelManager channelManager;

	@Override
	public OkuyamaClient createClient() {
		return new OkuyamaClientImpl(channelManager);
	}
	
	public OkuyamaClientFactoryImpl(String[] masternodes) throws UnknownHostException {
		super.setMasterNodes(masternodes);
		channelManager = new ChannelManager(masternodes, 10);
	}

}
