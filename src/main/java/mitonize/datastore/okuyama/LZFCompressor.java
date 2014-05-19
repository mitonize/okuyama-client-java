package mitonize.datastore.okuyama;

import java.nio.ByteBuffer;

import mitonize.datastore.OperationFailedException;

import com.ning.compress.CompressionFormatException;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

public class LZFCompressor extends Compressor {
	byte[] HEADER;
	
	public LZFCompressor() {
		HEADER = new byte[3];
		writeMagicBytes(HEADER);
		registerCompressor();
	}
	
	@Override
	int getCompressorId() {
		return 1;
	}

	public ByteBuffer compress(byte[] serialized) {
		return compress(serialized, 0, serialized.length);
	}
	
	public ByteBuffer compress(byte[] serialized, int offset, int length) {
		byte[] compressed = LZFEncoder.encode(serialized, offset, length);
		ByteBuffer result = ByteBuffer.allocate(compressed.length + 3);
		result.put(HEADER).put(compressed).flip();
		return result;
	}

	public ByteBuffer decompress(byte[] b, int offset, int length) throws OperationFailedException {
		try {
			return ByteBuffer.wrap(LZFDecoder.decode(b, offset+3, length-3));
		} catch (CompressionFormatException e) {
			throw new OperationFailedException("Unexpected compression format");
		}
	}

}
