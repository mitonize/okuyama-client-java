package mitonize.datastore.okuyama;

import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class Compressor {
	private static final int BLOCK_SIZE = 8192;

	void compress(byte[] serialized) {
		try {
				Deflater deflater = new Deflater();
				deflater.setInput(serialized);
				deflater.finish();
	
				byte[] z = new byte[BLOCK_SIZE];
				int compressed = 0;
				while (!deflater.finished()) {
					int remain = z.length - compressed;
					int size = deflater.deflate(z, compressed, remain, Deflater.SYNC_FLUSH);
					remain -= size;
					compressed += size;
//					System.err.println("b: compressing " + compressed);
				}
//				System.err.println("b: compressed " + compressed);
				b = ByteBuffer.wrap(z, 0, compressed);
			} catch (Exception e) {
				e.printStackTrace();
				throw e;
			}
	}
	void uncompress(byte[] b, int offset, int length) {
			Inflater inflater = new Inflater();
			inflater.setInput(b, offset, length);

			byte[] z = new byte[BLOCK_SIZE];
			int extracted = 0;
			try {
				while (!inflater.finished()) {
					int remain = z.length - extracted;
					int size = inflater.inflate(z, extracted, remain);
					if (size == 0) {
						break;
					} else if (inflater.needsInput()) {
						System.err.println("needsInput");
					} else if (inflater.needsDictionary()) {
						System.err.println("needsDictionary");
					}
					remain -= size;
					extracted += size;
					System.err.println("z: read " + size);
					if (remain < 1000) {
						byte[] z0 = new byte[z.length + BLOCK_SIZE];
						System.arraycopy(z, 0, z0, 0, z.length);
						z = z0;
					}
				}
			} catch (DataFormatException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
			inflater.end();
			b = z;
			offset = 0;
			length = extracted;
		}

	}

}
