package mitonize.datastore.okuyama;

import java.nio.ByteBuffer;

import mitonize.datastore.OperationFailedException;

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
