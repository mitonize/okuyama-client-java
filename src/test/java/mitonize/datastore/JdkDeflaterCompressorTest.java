package mitonize.datastore;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

public class JdkDeflaterCompressorTest {
	Compressor compressor;

	@Before
	public void setUp() throws Exception {
		compressor = new JdkDeflaterCompressor();
	}

	@Test
	public void testShortString() throws UnsupportedEncodingException {
		byte[] serialized = "文字列".getBytes("UTF-8");
		try {
			testEquality(compressor, serialized);
		} catch (OperationFailedException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testSerizlisedObjectMedium() throws IOException {
		HashMap<String, Object> map = new HashMap<>();
		for (int i = 0; i < 256; ++i) {
			map.put("key" + i, "value" + i);
		}

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(outputStream);
		os.writeObject(map);
		byte[] serialized = outputStream.toByteArray();
		try {
			testEquality(compressor, serialized);
		} catch (OperationFailedException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testSerizlisedObjectLarge() throws IOException {
		HashMap<String, Object> map = new HashMap<>();
		for (int i = 0; i < 1024 * 2; ++i) {
			map.put("key" + i, "value" + i);
		}

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(outputStream);
		os.writeObject(map);
		byte[] serialized = outputStream.toByteArray();
		try {
			testEquality(compressor, serialized);
		} catch (OperationFailedException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testPerformanceShortString() throws UnsupportedEncodingException {
		byte[] serialized = "文字列".getBytes("UTF-8");
		try {
			testPerformance(compressor, serialized, 10000);
		} catch (OperationFailedException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testPerformanceSerizlisedObjectMedium() throws IOException {
		HashMap<String, Object> map = new HashMap<>();
		for (int i = 0; i < 512; ++i) {
			map.put("key" + i, "value" + i);
		}

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(outputStream);
		os.writeObject(map);
		byte[] serialized = outputStream.toByteArray();
		try {
			testPerformance(compressor, serialized, 10000);
		} catch (OperationFailedException e) {
			fail(e.getMessage());
		}
	}

	@Test
	public void testPerformanceSerizlisedObjectLarge() throws IOException {
		HashMap<String, Object> map = new HashMap<>();
		for (int i = 0; i < 1024 * 2; ++i) {
			map.put("key" + i, "value" + i);
		}

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		ObjectOutputStream os = new ObjectOutputStream(outputStream);
		os.writeObject(map);
		byte[] serialized = outputStream.toByteArray();
		try {
			testPerformance(compressor, serialized, 10000);
		} catch (OperationFailedException e) {
			fail(e.getMessage());
		}
	}

	void testEquality(Compressor compressor, byte[] serialized) throws OperationFailedException, UnsupportedEncodingException {
		ByteBuffer compressed = compressor.compress(serialized);
		System.out.println("compressed:" + compressed.limit());
		ByteBuffer extracted = compressor.decompress(compressed.array(), 0,
				compressed.limit());
		System.out.println("extracted: " + extracted.limit());

		byte[] result = new byte[extracted.limit()];
		extracted.get(result);
		assertArrayEquals(serialized, result);
	}

	void testPerformance(Compressor compressor, byte[] serialized, int times) throws OperationFailedException, UnsupportedEncodingException {
		long startNanoTime = System.nanoTime();
		for (int i = 0; i < times; ++i) {
			ByteBuffer compressed = compressor.compress(serialized);
			compressor.decompress(compressed.array(), 0, compressed.limit());
		}
		long ellapse = System.nanoTime() - startNanoTime;
		System.out.println("Ellapse:" + ellapse / 1000000L);
	}

}
