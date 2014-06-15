package mitonize.datastore.okuyama;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;

import mitonize.datastore.OperationFailedException;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OkuyamaClientPerformanceITCase {
	static Logger logger = LoggerFactory.getLogger(OkuyamaClientPerformanceITCase.class);
	static OkuyamaClientFactoryImpl factory;
	static boolean compatibility = true;
	static boolean verbose = false;//logger.isTraceEnabled();
	
	@BeforeClass
	public static void setup() throws UnknownHostException {
		factory = new OkuyamaClientFactoryImpl(new String[]{"127.0.0.1:8888"/*, "127.0.0.1:8889"*/}, 6, compatibility, verbose);
		factory.setCompressionMode(true);
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
		static final int LOOP_COUNT = 10000;
		private String id;
		public Load(String id) {
			this.id = id;
		}

		@Override
		public void run() {
			final String METHOD_NAME = "test1_7_multithread";
			log(METHOD_NAME, "start:" + Thread.currentThread().getName());
			OkuyamaClient client = factory.createClient();
			for (int i = 0; i < LOOP_COUNT; ++i) {
				String key = String.format("%s_%08d", id, i);
				try {
					String val = Integer.toString(i);
					client.setObjectValue(key, val, null, 2);
					String str = (String) client.getObjectValue(key);
					if (str == null || !str.equals(val)) {
						fail("value is not match");
						break;
					}
					Thread.sleep(1);
				} catch (IOException e) {
					System.err.println("ERROR IOException:" + e.getMessage());
				} catch (OperationFailedException e) {
					System.err.println("ERROR OperationFaildException:" + e.getMessage());
				} catch (InterruptedException e) {
					System.err.println("ERROR InterruptedException:" + e.getMessage());
				}
			}
			log(METHOD_NAME, "done:" + Thread.currentThread().getName());
		}
	}

	@Test
	//@Ignore
	public void test1_7_multithread() throws IOException, OperationFailedException, InterruptedException {
		factory.setCompressionMode(false);
		ArrayList<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < 6; ++i) {
			Thread thread = new Thread(new Load("key" + i));
			threads.add(thread);
			// 起動時にウエイトを入れないとOkuyamaから例外が発生する。
			// NG:MasterNode - setKeyValue - Exception - okuyama.base.lang.BatchException: Key Node IO Error: detail info for log file
			Thread.sleep(900);
			thread.start();
		}
		for (int i = 0; i < threads.size(); ++i) {
			threads.get(i).join();
		}
	}

}