package mitonize.datastore.okuyama;

import java.io.IOException;

/**
 * OkuyamaClinet を生成するファクトリインタフェース
 */
public abstract class OkuyamaClientFactory {
	private String[] masternodes;

	/**
	 * {@link OkuyamaClient}を生成する。接続先や各種パラメータは {@code OkuyamaClientFactory}の実装クラスで設定されたものを用いる。
	 * @return OkuyamaClientインスタンス(使用後はdestroyClientに渡す)
	 * @throws IOException 
	 */
	public abstract OkuyamaClient createClient();
	
	public void setMasterNodes(String[] masternodes) {
		this.masternodes = masternodes;
	}

	public String[] getMasterNodes() {
		return this.masternodes;
	}
}
