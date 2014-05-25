package mitonize.datastore;

import java.nio.ByteBuffer;

import com.ning.compress.CompressionFormatException;
import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

/**
 * Compress payload with LZF algorithm. 
 * LZF compression algorithm, written in Java, is much faster 
 * than {@code}{@link Deflater}{@code} and {@code}{@link Inflater}{@code},
 *  but compressed size is larger about 40-50% than that by them.
 *  
 *  @see https://github.com/ning/compress
 *  @since 1.0.7
 */
public class LZFCompressor extends Compressor {
	public static final int COMPRESSOR_ID = 1;
	
	byte[] HEADER;
	
	public LZFCompressor() {
		HEADER = new byte[3];
		writeMagicBytes(HEADER);
		registerCompressor();
	}
	
	@Override
	public int getCompressorId() {
		return COMPRESSOR_ID;
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
