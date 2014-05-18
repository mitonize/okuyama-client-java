package mitonize.datastore.okuyama;

import java.nio.ByteBuffer;

import mitonize.datastore.OperationFailedException;

import com.ning.compress.CompressionFormatException;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

public class LZFCompressor implements Compressor {
	public ByteBuffer compress(byte[] serialized) {
		return ByteBuffer.wrap(LZFEncoder.encode(serialized));
	}

	public ByteBuffer decompress(byte[] b, int offset, int length) throws OperationFailedException {
		try {
			return ByteBuffer.wrap(LZFDecoder.decode(b, offset, length));
		} catch (CompressionFormatException e) {
			throw new OperationFailedException("Unexpected compression format");
		}
	}

}
