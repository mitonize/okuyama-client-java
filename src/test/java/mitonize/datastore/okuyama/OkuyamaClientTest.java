package mitonize.datastore.okuyama;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import mitonize.datastore.KeyValueConsistencyException;
import mitonize.datastore.OperationFailedException;
import mitonize.datastore.Pair;
import mitonize.datastore.VersionedValue;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class OkuyamaClientTest {
	static OkuyamaClientFactory factory;
	
	@BeforeClass
	public static void setup() throws UnknownHostException {
		factory = new OkuyamaClientFactoryImpl(new String[]{"127.0.0.1:8888"}, 5);
	}

	@Test
	public void test0() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		long maxStorable = client.initClient();
		System.out.println(maxStorable);
	}

	@Test
	public void test1_0() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		client.setObjectValue("HOGE00", new Date(), null, 0);
	}

	@Test
	public void test1_1() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		boolean success;
		success = client.setObjectValue("HOGE00", "タグ1", new String[]{"tag1","tag2"}, 0);
		assertTrue(success);
		success = client.setObjectValue("HOGE1", new Date(), new String[]{"tag1"}, 0);
		assertTrue(success);
	}

	@Test
	public void test1_2() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		boolean success;
		success = client.setObjectValue("HOGE2", "日本語", null, 0);
		assertTrue(success);
		String str = (String) client.getObjectValue("HOGE2");
		assertEquals("日本語", str);
	}

	@Test
	public void test1_3() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		char[] verylongkey = new char[1024];
		Arrays.fill(verylongkey, 'a');
		boolean ret = client.setObjectValue(new String(verylongkey), new Date(), new String[]{"tag1"}, 0);
		assertFalse(ret);
	}

	@Test
	public void test1_4() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		long maxlength = client.initClient();
		int size = (int) maxlength + 55005;
		// 55005 OK                    2170528
		// 55006 NG:Max Data Size Over 2170532
		// 55021 Value Length Error    2170556
		byte[] verylargevalue = new byte[size];
		Arrays.fill(verylargevalue, (byte)'a');
		boolean ret = client.setObjectValue("HOGE2", verylargevalue, null, 0);
		assertFalse(ret);
	}

	@Test
	public void test1_5_expire() throws IOException, OperationFailedException, InterruptedException {
		OkuyamaClient client = factory.createClient();

		client.setObjectValue("HOGE3", "タイムアウト", null, 1);
		Thread.sleep(2000);
		String str = (String) client.getObjectValue("HOGE3");
		assertNull(str);
	}

	@Test
	public void test1_6_set_emptystring() throws IOException, OperationFailedException, ClassNotFoundException {
		OkuyamaClient client = factory.createClient();

		boolean success;
		success = client.setObjectValue("HOGE6", "", new String[]{"tag1","tag2"}, 0);
		assertTrue(success);
		Object obj = client.getObjectValue("HOGE6");
		assertEquals("", obj);
	}

	@Test
	public void test1_7_set_null() throws IOException, OperationFailedException, ClassNotFoundException {
		OkuyamaClient client = factory.createClient();

		try {
			client.setObjectValue("HOGE6", null, new String[]{"tag1","tag2"}, 0);
			fail("Okuyama does not permit storing null value");
		} catch (IllegalArgumentException e) {			
		}
	}

	class Load implements Runnable {
		static final int LOOP_COUNT = 10000;
		private String id;
		public Load(String id) {
			this.id = id;
		}

		@Override
		public void run() {
			System.out.println("start:" + Thread.currentThread().getName());
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
					System.out.println(e.getMessage());
				} catch (OperationFailedException e) {
					System.out.println(e.getMessage());
				} catch (InterruptedException e) {
					System.out.println(e.getMessage());
				}
			}
			System.out.println("done:" + Thread.currentThread().getName());
		}
	}

	@Test
	@Ignore
	public void test1_7_multithread() throws IOException, OperationFailedException, InterruptedException {
		ArrayList<Thread> threads = new ArrayList<>();
		for (int i = 0; i < 6; ++i) {
			Thread thread = new Thread(new Load("key" + i));
			threads.add(thread);
			thread.start();
		}
		for (int i = 0; i < threads.size(); ++i) {
			threads.get(i).join();
		}
	}


	@Test
	public void test2_0() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		Object obj = client.getObjectValue("HOGE00");
		System.out.println(obj.getClass().getName());
	}

	@Test
	public void test1_10() throws IOException, OperationFailedException {
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
			client.getObjectValue(new String(verylongkey));
			fail("must throw an exception of 'Key Length Error'");
		} catch (OperationFailedException e) {
			// 
		}
	}


	@Test
	public void test4_0() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		String[] strs = client.getTagKeys("tag1", false);
		System.out.println(Arrays.toString(strs));
	}

	@Test
	public void test5_0() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		Object obj = client.removeObjectValue("HOGE1");
		System.out.println(obj);
	}

	@Test
	@Ignore
	public void test6_0() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();
	
		SecureRandom rand = new SecureRandom(); 
		String key = "HOGE_" + Long.toString(rand.nextLong());
	
		boolean success;
		success = client.addObjectValue(key, "タグ1", null, 60);
		assertTrue(success);
		success = client.addObjectValue(key, "タグ1", new String[]{"tag1","tag2"}, 60);
		assertFalse(success);

		client.removeObjectValue(key);
	}

	@Test
	public void test15_0() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();
		
//		client.setObjectValue("HOGE00", new Date(), null, 0);
		VersionedValue v = client.getObjectValueVersionCheck("HOGE00");
		client.setObjectValueVersionCheck("HOGE00", new Date(), v.getVersion(), null, 0);
		v = client.getObjectValueVersionCheck("HOGE00");
		System.out.println(v.getVersion());
	}

	@Test
	public void test15_1() throws OperationFailedException, IOException {
		OkuyamaClient client = factory.createClient();

		try {
			client.setObjectValueVersionCheck("HOGE00", new Date(), "0", null, 0);
		} catch (KeyValueConsistencyException e) {
			// pass
			return;
		}
		fail("KeyValueConsistencyException must be thrown");
	}

	@Test
	public void test22_0() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		Object[] objects = client.getMultiObjectValues("HOGE00", "HOGE01", "HOGE6", "HOGE1");
		System.out.println(Arrays.toString(objects));
	}

	@Test
	public void test23_0() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		Pair[] pairs = client.getPairsByTag("tag1");
		System.out.println(Arrays.toString(pairs));
	}

	@Test
	public void test23_unresolved_host() throws IOException, OperationFailedException {
		try {
			OkuyamaClientFactory f = new OkuyamaClientFactoryImpl(new String[]{"unresolved.local:8888"}, 5);
			fail("Expects UnknownHostException");
			f.createClient();
		} catch(UnknownHostException e) {
		}
	}
	
	@Test
	public void test23_multiple_hosts() throws IOException, OperationFailedException {
		String[] masternodes = new String[]{"localhost:8888", "127.0.0.1:8889"};
		
		OkuyamaClientFactory f = new OkuyamaClientFactoryImpl(masternodes, 5);
		OkuyamaClient client = f.createClient();

		Pair[] pairs = client.getPairsByTag("tag1");
		System.out.println(Arrays.toString(pairs));
	}
}