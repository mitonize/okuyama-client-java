package mitonize.datastore.okuyama;

import java.nio.ByteBuffer;

import mitonize.datastore.OperationFailedException;

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

	public static boolean isCompressedPayload(byte[] data, int offset, int length) {
		if (length >= 2 && data[offset] == (byte) 0xac && data[offset+1] == (byte) 0xee) {
			return true;
		}
		return false;
	}

	public static Compressor getCompressor(int compressorId) {
		if (compressorId < MAX_COMPRESSORS) {
			return list[compressorId];
		}
		return null;
	}
	
	protected void registerCompressor() {
		int id = getCompressorId();
		if (id < MAX_COMPRESSORS) {
			list[id] = this;
		}
	}

	protected void writeMagicBytes(byte[] buf) {
		buf[0] = (byte) 0xac;
		buf[1] = (byte) 0xee;
		buf[2] = (byte) getCompressorId();
	}
	
	abstract int getCompressorId();
	abstract ByteBuffer compress(byte[] serialized);
	abstract ByteBuffer compress(byte[] serialized, int offset, int length);
	abstract ByteBuffer decompress(byte[] b, int offset, int length) throws OperationFailedException;
}
