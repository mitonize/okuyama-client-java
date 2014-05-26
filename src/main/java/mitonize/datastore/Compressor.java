package mitonize.datastore;

import java.nio.ByteBuffer;

/**
 * Okuyamaに保管する値を圧縮するためのクラスのための抽象クラス。圧縮アルゴリズムは値の容量や
 * アクセスの特性によって選択することができる。
 * <p>Okuyamaの値として保管する際は、マジックコードとして1バイト目、2バイト目として 0xAC 0xEE を
 * 設定し、更に3バイト目に圧縮に使用したCompressor具象クラスのIDを格納する。これにより、値を参照した
 * 際に圧縮の有無を判定し、展開に必要なCompressorを選択して透過的に値の参照ができるようになっている。</p>
 * <p>ただし、この格納形式はOkuyamaサーバに同梱のOkuyamaClientの圧縮方式とは互換性がない。オリジナルのOkuyamaClientは
 * 値が圧縮されているかどうかを判定できないため、取得側で全てを圧縮するか、圧縮しないかを決める必要がある。</p>
 */
public abstract class Compressor {
	static final int MAX_COMPRESSORS = 4;
	static private Compressor[] list = new Compressor[MAX_COMPRESSORS];
	static {
		/* register built-in compressor */
		Compressor.registerCompressor(new JdkDeflaterCompressor());
		Compressor.registerCompressor(new LZFCompressor());
	}

	/**
	 * <ul>
	 * <li>独自仕様：圧縮されたバイト列はマジックコード 0xac 0xee で始めることとする。</li>
	 * <li>独自仕様：3バイト目には格納時に使用されたCompressorの識別子を設定する。</li>
	 * </ul>
	 * @param data
	 * @param offset
	 * @param length
	 * @return 与えられたバイト列がCompressorによって圧縮されている場合は、展開に必要なCompressorを返す。
	 * @throws IllegalStateException
	 */
	public static Compressor getAppliedCompressor(byte[] data, int offset, int length) throws IllegalStateException {
		if (length >= 2 && data[offset] == (byte) 0xac && data[offset+1] == (byte) 0xee) {
			int compressorId = data[offset + 2];
			if (compressorId < MAX_COMPRESSORS) {
				return list[compressorId];
			}
			throw new IllegalStateException("Unknown compressorId");
		}
		return null;
	}

	public static Compressor getCompressor(int compressorId) {
		if (compressorId < MAX_COMPRESSORS) {
			return list[compressorId];
		}
		return null;
	}

	public static void registerCompressor(Compressor compressor) {
		int id = compressor.getCompressorId();
		if (id < MAX_COMPRESSORS) {
			list[id] = compressor;
		}
	}

	protected void writeMagicBytes(byte[] buf) {
		buf[0] = (byte) 0xac;
		buf[1] = (byte) 0xee;
		buf[2] = (byte) getCompressorId();
	}
	
	abstract public int getCompressorId();
	abstract public ByteBuffer compress(byte[] serialized);
	abstract public ByteBuffer compress(byte[] serialized, int offset, int length);
	abstract public ByteBuffer decompress(byte[] b, int offset, int length) throws OperationFailedException;
}
