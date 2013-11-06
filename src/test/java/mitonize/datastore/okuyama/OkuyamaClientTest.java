package mitonize.datastore.okuyama;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import mitonize.datastore.OperationFailedException;
import mitonize.datastore.Pair;
import mitonize.datastore.VersionedValue;
import mitonize.datastore.okuyama.OkuyamaClient;
import mitonize.datastore.okuyama.OkuyamaClientFactory;
import mitonize.datastore.okuyama.OkuyamaClientFactoryImpl;

import org.apache.commons.codec.binary.Base64;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class OkuyamaClientTest {
	static OkuyamaClientFactory factory;
	
	@BeforeClass
	public static void setup() throws UnknownHostException {
		factory = new OkuyamaClientFactoryImpl(new String[]{"127.0.0.1:8888"}, true);
	}

	@Test
	public void test0() throws IOException {
		OkuyamaClient client = factory.createClient();

		long maxStorable = client.initClient();
		System.out.println(maxStorable);
	}

	@Test
	public void test1_0() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		client.setObjectValue("hoge0", new Date(), null, 0);
	}

	@Test
	public void test1_1() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		client.setObjectValue("hoge0", "タグ1", new String[]{"tag1","tag2"}, 0);
		client.setObjectValue("hoge1", new Date(), new String[]{"tag1"}, 0);
	}

	@Test
	public void test1_2() throws IOException, ClassNotFoundException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		client.setObjectValue("hoge2", "日本語", null, 0);
		String str = (String) client.getObjectValue("hoge2");
		System.out.println(str);
	}

	@Test
	public void test1_3() throws IOException {
		OkuyamaClient client = factory.createClient();

		char[] verylongkey = new char[1024];
		Arrays.fill(verylongkey, 'a');
		try {
			client.setObjectValue(new String(verylongkey), new Date(), new String[]{"tag1"}, 0);
			fail("must throw an exception of 'Key Length Error'");
		} catch (OperationFailedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void test1_4() throws IOException {
		OkuyamaClient client = factory.createClient();

		long maxlength = client.initClient();
		int size = (int) maxlength + 55005;
		// 55005 OK                    2170528
		// 55006 NG:Max Data Size Over 2170532
		// 55021 Value Length Error    2170556
		byte[] verylargevalue = new byte[size];
		Arrays.fill(verylargevalue, (byte)'a');
		try {
			client.setObjectValue("hoge2", verylargevalue, null, 0);
			fail("must throw an exception");
		} catch (OperationFailedException e) {
			System.out.println(e.getMessage());
		}
	}

	@Test
	public void test1_5_expire() throws IOException, ClassNotFoundException, OperationFailedException, InterruptedException {
		OkuyamaClient client = factory.createClient();

		client.setObjectValue("hoge3", "タイムアウト", null, 1);
		Thread.sleep(2000);
		String str = (String) client.getObjectValue("hoge3");
		System.out.println(str);
	}

	@Test
	public void test1_6_set_null() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		client.setObjectValue("hoge6", "", new String[]{"tag1","tag2"}, 0);
		client.setObjectValue("hoge6", null, new String[]{"tag1","tag2"}, 0);
	}

	class Load implements Runnable {
		static final int LOOP_COUNT = 10000;
		private String id;
		private Object notifier;
		public Load(String id, Object notifier) {
			this.id = id;
			this.notifier = notifier;
		}

		@Override
		public void run() {
			OkuyamaClient client = factory.createClient();
//			try {
//				notifier.wait();
//			} catch (InterruptedException e1) {
//				e1.printStackTrace();
//			}
			for (int i = 0; i < LOOP_COUNT; ++i) {
				String key = String.format("%s_%08d", id, i);
				try {
					client.setObjectValue(key, Integer.toString(i), null, 2);
					client.getObjectValue(key);
				} catch (IOException e) {
					e.printStackTrace();
				} catch (OperationFailedException e) {
					e.printStackTrace();
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				}
			}
		}
	}
	@Test
	public void test1_7_multithread() throws IOException, OperationFailedException, InterruptedException {
		Object notifier = new Object();
		ArrayList<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 10; ++i) {
			Thread thread = new Thread(new Load("key" + i, notifier));
			threads.add(thread);
			thread.start();
		}
		for (int i = 0; i < threads.size(); ++i) {
			threads.get(i).join();
		}
		notifier.notifyAll();
	}


	@Test
	public void test2_0() throws IOException, ClassNotFoundException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		Object obj = client.getObjectValue("hoge0");
		System.out.println(obj.getClass().getName());
	}

	@Test
	public void test2_1() throws IOException, ClassNotFoundException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		Object obj = client.getObjectValue("notexistkey");
		if (obj == null)
			System.out.println("(null)");
		else
			System.out.println(obj.getClass().getName());
	}

	@Test
	public void test2_2() throws IOException, ClassNotFoundException {
		OkuyamaClient client = factory.createClient();

		char[] verylongkey = new char[1024];
		Arrays.fill(verylongkey, 'a');
		try {
			Object obj = client.getObjectValue(new String(verylongkey));
			fail("must throw an exception of 'Key Length Error'");
		} catch (OperationFailedException e) {
			// 
		}
	}


	@Test
	public void test4_0() throws IOException, ClassNotFoundException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		String[] strs = client.getTagKeys("tag1", false);
		System.out.println(Arrays.toString(strs));
	}

	@Test
	public void test15_0() throws IOException, ClassNotFoundException, OperationFailedException {
		OkuyamaClient client = factory.createClient();
		
		client.setObjectValue("hoge0", new Date(), null, 0);
		VersionedValue v = client.getObjectValueVersionCheck("hoge0");
		client.setObjectValueVersionCheck("hoge0", new Date(), v.getVersion(), null, 0);
		v = client.getObjectValueVersionCheck("hoge0");
		System.out.println(v.getVersion());
	}

	@Test
	public void test15_1() throws ClassNotFoundException, OperationFailedException, IOException {
		OkuyamaClient client = factory.createClient();
		
//		client.setObjectValue("hoge0", new Date(), null, 0);
//		VersionedValue v = client.getObjectValueVersionCheck("hoge0");
		client.setObjectValueVersionCheck("hoge0", new Date(), "0", null, 0);
		VersionedValue v = client.getObjectValueVersionCheck("hoge0");
		System.out.println(v.getVersion());
	}

	@Test
	public void test23_0() throws IOException, ClassNotFoundException {
		OkuyamaClient client = factory.createClient();

		Pair[] pairs = client.getPairsByTag("tag1");
		System.out.println(Arrays.toString(pairs));
	}

}
