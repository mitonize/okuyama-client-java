package mitonize.datastore.okuyama;

import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class JdkDeflaterCompressor implements Compressor {
	private static final int BLOCK_SIZE_DECOMPRESS = 8192;
	private static final int BLOCK_SIZE_COMPRESS   = 4096;
	int averageRatio = 4;
	Deflater deflater;
	Inflater inflater;
	byte[] allocatedBytesCompress = new byte[BLOCK_SIZE_COMPRESS];
	byte[] allocatedBytesDecompress = new byte[BLOCK_SIZE_DECOMPRESS];

	public JdkDeflaterCompressor() {
		deflater = new Deflater(Deflater.BEST_SPEED);
		inflater = new Inflater();
	}
	
	public ByteBuffer compress(byte[] serialized) {
		try {
			Deflater deflater = this.deflater;
			deflater.reset();
			deflater.setInput(serialized);
			deflater.finish();

			int blocks = serialized.length / averageRatio / BLOCK_SIZE_COMPRESS;
			blocks = blocks < 1 ? 1: blocks;
			byte[] z;
			if (blocks > 1) {
				z = new byte[BLOCK_SIZE_COMPRESS * blocks];
			} else {
				z = allocatedBytesCompress;
			}
			int compressed = 0;
			while (!deflater.finished()) {
				int remain = z.length - compressed;
				int size = deflater.deflate(z, compressed, remain);
				remain -= size;
				compressed += size;
				if (remain < 100) {
					byte[] z0 = new byte[z.length + BLOCK_SIZE_COMPRESS];
					System.arraycopy(z, 0, z0, 0, z.length);
					z = z0;
				}
			}
			return ByteBuffer.wrap(z, 0, compressed);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	public ByteBuffer decompress(byte[] b, int offset, int length) {
		Inflater inflater = this.inflater;
		inflater.reset();
		inflater.setInput(b, offset, length);

		int blocks = (length * averageRatio) / BLOCK_SIZE_DECOMPRESS;
		blocks = blocks < 1 ? 1: blocks;
		byte[] z;
		if (blocks > 1) {
			z = new byte[BLOCK_SIZE_DECOMPRESS * blocks];
		} else {
			z = allocatedBytesDecompress;
		}
		int extracted = 0;
		try {
			while (!inflater.finished()) {
				int remain = z.length - extracted;
				int size = inflater.inflate(z, extracted, remain);
				remain -= size;
				extracted += size;
				if (size == 0) {
					break;
				}
				if (remain < 100) {
					byte[] z0 = new byte[z.length + BLOCK_SIZE_DECOMPRESS];
					System.arraycopy(z, 0, z0, 0, z.length);
					z = z0;
				}
			}
		} catch (DataFormatException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ByteBuffer.wrap(z, 0, extracted);
	}

}
