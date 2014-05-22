package mitonize.datastore.okuyama;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class JdkDeflaterCompressor extends Compressor {
	private static final int BLOCK_SIZE_DECOMPRESS = 8192;
	private static final int BLOCK_SIZE_COMPRESS   = 4096;
	private static final int POOL_SIZE = 10;
	int averageRatio = 4;
	
	ArrayBlockingQueue<Deflater> deflaters;
	ArrayBlockingQueue<Inflater> inflaters;
	
	byte[] allocatedBytesCompress = new byte[BLOCK_SIZE_COMPRESS];
	byte[] allocatedBytesDecompress = new byte[BLOCK_SIZE_DECOMPRESS];

	public JdkDeflaterCompressor() {
		deflaters = new ArrayBlockingQueue<>(POOL_SIZE);
		inflaters = new ArrayBlockingQueue<>(POOL_SIZE);
		registerCompressor();
	}
	
	private Deflater getDeflater() {
		Deflater deflater = deflaters.poll();
		if (deflater != null) {
			return deflater;
		}
		return new Deflater(Deflater.BEST_SPEED);
	}
	
	private void recycleDeflater(Deflater deflater) {
		deflater.reset();
		boolean	pooled = deflaters.offer(deflater);
		if (!pooled) {
			// 返却できなかったら捨てられる。
			deflater.end();
		}
	}

	private Inflater getInflater() {
		Inflater inflater = inflaters.poll();
		if (inflater != null) {
			return inflater;
		}
		return new Inflater();
	}
	
	private void recycleInflater(Inflater inflater) {
		inflater.reset();
		boolean	pooled = inflaters.offer(inflater);
		if (!pooled) {
			// 返却できなかったら捨てられる。
			inflater.end();
		}
	}
	
	
	@Override
	int getCompressorId() {
		return 0;
	}
	
	public ByteBuffer compress(byte[] serialized) {
		return compress(serialized, 0, serialized.length);
	}
	
	public ByteBuffer compress(byte[] serialized, int offset, int length) {
		Deflater deflater = null;
		try {
			deflater = getDeflater();
			deflater.setInput(serialized, offset, length);
			deflater.finish();

			int blocks = serialized.length / averageRatio / BLOCK_SIZE_COMPRESS;
			blocks = blocks < 1 ? 1: blocks;
			byte[] z;
			if (blocks > 1) {
				z = new byte[BLOCK_SIZE_COMPRESS * blocks];
			} else {
				z = allocatedBytesCompress;
			}

			writeMagicBytes(z);
			int compressed = 3;

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
		} finally {
			if (deflater != null) {
				recycleDeflater(deflater);
			}
		}
	}

	public ByteBuffer decompress(byte[] b, int offset, int length) {
		int blocks = (length * averageRatio) / BLOCK_SIZE_DECOMPRESS;
		blocks = blocks < 1 ? 1: blocks;
		byte[] z;
		if (blocks > 1) {
			z = new byte[BLOCK_SIZE_DECOMPRESS * blocks];
		} else {
			z = allocatedBytesDecompress;
		}
		int extracted = 0;
		Inflater inflater = null;		
		try {
			inflater = getInflater();
			inflater.reset();
			inflater.setInput(b, offset+3, length-3);

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
		} finally {
			if (inflater != null) {
				recycleInflater(inflater);
			}
		}
		return ByteBuffer.wrap(z, 0, extracted);
	}

}
