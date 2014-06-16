package mitonize.datastore.okuyama;

import java.io.IOException;

/**
 * OkuyamaClinet を生成するファクトリインタフェース
 */
public interface OkuyamaClientFactory {

	/**
	 * {@link OkuyamaClient}を生成する。接続先や各種パラメータは {@code OkuyamaClientFactory}の実装クラスで設定されたものを用いる。
	 * @return OkuyamaClientインスタンス(使用後はdestroyClientに渡す)
	 * @throws IOException 
	 */
	public OkuyamaClient createClient();
}
