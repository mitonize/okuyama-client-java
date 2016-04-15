package mitonize.datastore.okuyama;

/**
 * OkuyamaClinet を生成するファクトリインタフェース
 */
public interface OkuyamaClientFactory {

	/**
	 * {@link OkuyamaClient}を生成する。接続先や各種パラメータは {@code OkuyamaClientFactory}の実装クラスで設定されたものを用いる。
	 * @return OkuyamaClientインスタンス(使用後はdestroyClientに渡す)
	 */
	public OkuyamaClient createClient();

	/**
	 * 確保しているリソースを解放してファクトリクラスを破棄する。
	 */
	public void destroy();
}
