package mitonize.datastore.okuyama;

import java.nio.ByteBuffer;

import mitonize.datastore.OperationFailedException;

public interface Compressor {
	ByteBuffer compress(byte[] serialized);
	ByteBuffer decompress(byte[] b, int offset, int length) throws OperationFailedException;
}
