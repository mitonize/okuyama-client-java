package mitonize.datastore;

/**
 * デフォルトのCompressionStrategyを提供するクラス。
 */
public class DefaultCompressionStrategy implements CompressionStrategy {
	/** 圧縮を適用する際の最小の値の長さ。この長さより小さい値場合は圧縮しない。 */
	private static final int MINIMUM_LENGTH_TO_COMPRESS = 32;

	/** デフォルトのコンプレッサーID */
	private static final int DEFAULT_COMPRESSOR_ID = JdkDeflaterCompressor.COMPRESSOR_ID;

	private Compressor compressor;
	public DefaultCompressionStrategy() {
		compressor = Compressor.getCompressor(DEFAULT_COMPRESSOR_ID);
	}
	@Override
	public Compressor getSuitableCompressor(String key, int valueLength) {
		if (valueLength > MINIMUM_LENGTH_TO_COMPRESS) {
			return compressor;
		}
		return null;
	}
}