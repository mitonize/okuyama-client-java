package mitonize.datastore;


/**
 * 保管するキーや値のサイズによって適切な圧縮戦略を利用側で適用するためのインタフェース。
 * Factoryクラスごとに指定する。
 */
public interface CompressionStrategy {
	/**
	 * 保管するキーや値のサイズによって適切なCompressorを返す。値が保管されるときにコールバックされる。
	 * 
	 * @param key 保管しようとしているキー
	 * @param valueLength 保管しようとしている値の長さ
	 * @return 圧縮に使用させたいCompressorオブジェクト。圧縮させない場合はnullを返す。
	 */
	Compressor getSuitableCompressor(String key, int valueLength);
}
