package mitonize.datastore;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class JdkDeflaterCompressor extends Compressor {
	public static final int COMPRESSOR_ID = 0;

	private static final int MAGIC_BYTES_LENGTH = 3;
	private static final int BLOCK_SIZE_COMPRESS = 4096;
	private static final int POOL_SIZE = 10;
	
	ArrayBlockingQueue<Deflater> deflaters;
	ArrayBlockingQueue<Inflater> inflaters;

	public JdkDeflaterCompressor() {
		deflaters = new ArrayBlockingQueue<Deflater>(POOL_SIZE);
		inflaters = new ArrayBlockingQueue<Inflater>(POOL_SIZE);
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
	public int getCompressorId() {
		return COMPRESSOR_ID;
	}

	public ByteBuffer compress(byte[] serialized) {
		return compress(serialized, 0, serialized.length);
	}
	
	public ByteBuffer compress(byte[] serialized, int offset, int length) {
		byte[] z = new byte[length];
		writeMagicBytes(z);
		int compressed = MAGIC_BYTES_LENGTH;
		// 圧縮前のサイズを最初の4バイトに格納する。
		ByteBuffer.wrap(z).putInt(compressed, length);
		compressed += Integer.SIZE / 8;

		Deflater deflater = null;
		try {
			deflater = getDeflater();
			deflater.setInput(serialized, offset, length);
			deflater.finish();

			while (!deflater.finished()) {
				int remain = z.length - compressed;
				if (remain == 0) {
					z = Arrays.copyOf(z, z.length + BLOCK_SIZE_COMPRESS);
					remain = z.length - compressed;
				}
				int size = deflater.deflate(z, compressed, remain);
				compressed += size;
			}
			return ByteBuffer.wrap(z, 0, compressed);
		} finally {
			if (deflater != null) {
				recycleDeflater(deflater);
			}
		}
	}

	public ByteBuffer decompress(byte[] b, int offset, int length) {
		ByteBuffer buffer = ByteBuffer.wrap(b, offset, length);
		int extractedSize = buffer.getInt(MAGIC_BYTES_LENGTH);

		byte[] z = new byte[extractedSize];
		int leadingBytes = MAGIC_BYTES_LENGTH + Integer.SIZE / 8;

		int extracted = 0;
		Inflater inflater = null;		
		try {
			inflater = getInflater();
			inflater.setInput(b, offset+leadingBytes, length-leadingBytes);

			while (!inflater.finished()) {
				int remain = z.length - extracted;
				if (remain == 0) {
					z = Arrays.copyOf(z, z.length + BLOCK_SIZE_COMPRESS);
					System.err.println("reallocated:" + z.length);
					remain = z.length - extracted;
				}
				int size = inflater.inflate(z, extracted, remain);
				extracted += size;
			}
			return ByteBuffer.wrap(z, 0, extracted);
		} catch (DataFormatException e) {
			throw new RuntimeException(e);
		} finally {
			if (inflater != null) {
				recycleInflater(inflater);
			}
		}
	}

}
