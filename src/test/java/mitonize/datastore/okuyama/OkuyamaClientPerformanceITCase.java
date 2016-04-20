package mitonize.datastore.okuyama;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mitonize.datastore.ElapseWatch;
import mitonize.datastore.OperationFailedException;

public class OkuyamaClientPerformanceITCase {
	private static final String OKUYAMA_ENDPOINTS = "OKUYAMA_ENDPOINTS";
	static Logger logger = LoggerFactory.getLogger(OkuyamaClientPerformanceITCase.class);
	static OkuyamaClientFactoryImpl factory;
	static boolean compatibility = false;
	static boolean verbose = false;//logger.isTraceEnabled();

	@BeforeClass
	public static void setup() throws UnknownHostException {
		String[] endpoints = System.getProperty(OKUYAMA_ENDPOINTS, "127.0.0.1:8888").split(",");
		factory = new OkuyamaClientFactoryImpl(endpoints, 6, compatibility, verbose);
		factory.setCompressionMode(true);
//		factory.setCompressionStrategy(new CompressionStrategy() {
//			@Override
//			public Compressor getSuitableCompressor(String key, int valueLength) {
//				return Compressor.getCompressor(LZFCompressor.COMPRESSOR_ID);
//			}
//		});
	}

	void log(String method, Object ... msg) {
		StringBuilder sb = new StringBuilder();
		for (Object m: msg) {
			if (m == null) m = "(null)";
			sb.append(m.toString()).append(' ');
		}
		logger.debug("{}: {}", method, sb.toString());
	}

	class Load implements Runnable {
		static final int MAX_PAYLOAD_SIZE = 100 * 1024;

		private String id;
		private int loopCount;
		private int payloadSize;

		public Load(String id, int loopCount, int payloadSize) {
			this.id = id;
			this.loopCount = loopCount;
			this.payloadSize = payloadSize;
		}

		public byte[] makePayload(int size) {
			if (size > MAX_PAYLOAD_SIZE) {
				throw new IllegalArgumentException("Too large payload");
			}
			Random random = new Random();
			byte[] payload = new byte[size];
			random.nextBytes(payload);
			Arrays.fill(payload, 0, payload.length / 2, (byte)1);
			return payload;
		}

		@Override
		public void run() {
			byte[] payload = makePayload(payloadSize);
//			final String METHOD_NAME = "test1_7_multithread";
//			log(METHOD_NAME, "payload =", payload.length, "start:" + Thread.currentThread().getName());
			OkuyamaClient client = factory.createClient();
			for (int i = 0; i < loopCount; ++i) {
				String key = String.format("%s_%08d", id, i);
				try {
					ElapseWatch watch = i == (loopCount / 2) ? new ElapseWatch(): null ;
					double lapSet = 0, lapGet = 0;

					if (watch != null)
						watch.start();

					client.setObjectValue(key, payload, null, 30);
					if (watch != null) {
						lapSet = watch.lap();
					}
					byte[] ret = (byte[]) client.getObjectValue(key);
					if (watch != null) {
						lapGet = watch.lap();
						System.out.printf("%6.2f\t%6.2f%n", lapSet, lapGet);
					}
					if (ret == null || !Arrays.equals(payload, ret)) {
						fail("value is not match " + key);
					}
					Thread.sleep(1);
				} catch (IOException e) {
					System.err.println(key + " ERROR IOException:" + e.getMessage());
				} catch (OperationFailedException e) {
					System.err.println(key + " ERROR OperationFaildException:" + e.getMessage());
				} catch (Exception e) {
					System.err.println(key + " ERROR DataFormatException:" + e.getMessage());
					e.printStackTrace();
				}
			}
//			log(METHOD_NAME, "done:" + Thread.currentThread().getName());
		}
	}

	@Test
	//@Ignore
	public void test1_7_multithread() throws IOException, OperationFailedException, InterruptedException {
		int threadCount = 6;
		int payloadSize = 5000;
		ArrayList<Thread> threads = new ArrayList<Thread>();
		System.out.printf("%6s\t%6s %dbytes%n","set", "get", payloadSize);

		Random random = new Random();
		for (int i = 0; i < threadCount; ++i) {
			Thread thread = new Thread(new Load("key" + i + "_" + random.nextInt(), 100, payloadSize));
			threads.add(thread);
		}

		for (int i = 0; i < threads.size(); ++i) {
			threads.get(i).start();
		}
		for (int i = 0; i < threads.size(); ++i) {
			threads.get(i).join();
		}
	}

}