package mitonize.datastore.okuyama;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Date;

import mitonize.datastore.KeyValueConsistencyException;
import mitonize.datastore.OperationFailedException;
import mitonize.datastore.Pair;
import mitonize.datastore.VersionedValue;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OkuyamaClientITCase {
	static Logger logger = LoggerFactory.getLogger(OkuyamaClientITCase.class);
	static OkuyamaClientFactoryImpl factory;
	static boolean compatibility = false;
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

	@Test
	@Ignore
	public void test0() throws IOException, OperationFailedException {
		final String METHOD_NAME = "test0";
		OkuyamaClient client = factory.createClient();

		long maxStorable = client.initClient();
		log(METHOD_NAME, maxStorable);
	}

	@Test
	public void test1_0() throws IOException, OperationFailedException {
		final String METHOD_NAME = "test1_0";
		OkuyamaClient client = factory.createClient();

		client.setObjectValue("HOGE00", new Date(), null, 0);
		Object obj = client.getObjectValue("HOGE00");
		log(METHOD_NAME, obj);
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
	public void test1_3_longkey() throws IOException, OperationFailedException {
		OkuyamaClient client = factory.createClient();

		char[] verylongkey = new char[1024];
		Arrays.fill(verylongkey, 'a');
		try {
			client.setObjectValue(new String(verylongkey), new Date(), new String[]{"tag1"}, 0);
			fail("Exception must be thrown with 'Key Length Error'");
		} catch (OperationFailedException e) {
		}
	}

	@Test
	@Ignore
	public void test1_4_largedata_over() throws IOException, OperationFailedException {
		boolean doCompress = factory.isCompressionMode();		
		factory.setCompressionMode(false);
		try {
			OkuyamaClient client = factory.createClient();
	
			long maxlength = client.initClient();
			int size = (int) maxlength + 55006;

			// 55005 OK                    2170528
			// 55006 NG:Max Data Size Over 2170532
			// 55021 Value Length Error    2170556
			byte[] verylargevalue = new byte[size];
			Arrays.fill(verylargevalue, (byte)'a');
			client.setObjectValue("HOGE2", verylargevalue, null, 0);
			fail("Exception must be thrown");
		} catch (OperationFailedException e) {
		} finally {
			factory.setCompressionMode(doCompress);
		}
	}

	@Test
//	@Ignore
	public void test1_4_largedata_muchover() throws IOException, OperationFailedException {
		boolean doCompress = factory.isCompressionMode();		
		factory.setCompressionMode(false);
		try {
			OkuyamaClient client = factory.createClient();
	
			long maxlength = client.initClient();
			int size = (int) maxlength + 55021;

			// 55005 OK                    2170528
			// 55006 NG:Max Data Size Over 2170532
			// 55021 Value Length Error    2170556
			byte[] verylargevalue = new byte[size];
			Arrays.fill(verylargevalue, (byte)'a');
			try {
				client.setObjectValue("HOGE2", verylargevalue, null, 0);
				fail("Exception must be thrown.");
			} catch (OperationFailedException e) {
			}
		} finally {
			factory.setCompressionMode(doCompress);
		}
	}

	@Test
//	@Ignore
	public void test1_4_largedata_just() throws IOException, OperationFailedException {
		boolean doCompress = factory.isCompressionMode();		
		factory.setCompressionMode(false);
		try {
			OkuyamaClient client = factory.createClient();
	
			long maxlength = client.initClient();
			int size = (int) maxlength;

			// 55005 OK                    2170528
			// 55006 NG:Max Data Size Over 2170532
			// 55021 Value Length Error    2170556
			byte[] verylargevalue = new byte[size];
			Arrays.fill(verylargevalue, (byte)'a');
			boolean ret = client.setObjectValue("HOGE2", verylargevalue, null, 0);
			assertTrue(ret);
		} finally {
			factory.setCompressionMode(doCompress);
		}
	}

	@Test
	public void test1_4_largedata64k() throws IOException, OperationFailedException {
		boolean doCompress = factory.isCompressionMode();		
		factory.setCompressionMode(false);
		try {
			OkuyamaClient client = factory.createClient();
	
			int size = (int) 65536;
			byte[] verylargevalue = new byte[size];
			Arrays.fill(verylargevalue, (byte)'a');
			boolean ret = client.setObjectValue("HOGE2_0", verylargevalue, null, 0);
			assertTrue(ret);
		} finally {
			factory.setCompressionMode(doCompress);
		}
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

	@Test
	public void test2_0() throws IOException, OperationFailedException {
		final String METHOD_NAME = "test2_0";
		OkuyamaClient client = factory.createClient();

		Object obj = client.getObjectValue("HOGE00");
		log(METHOD_NAME, obj.getClass().getName());
	}

	@Test
	public void test1_10() throws IOException, OperationFailedException {
		final String METHOD_NAME = "test1_10";
		OkuyamaClient client = factory.createClient();

		Object obj = client.getObjectValue("notexistkey");
		if (obj == null)
			log(METHOD_NAME, "(null)");
		else
			log(METHOD_NAME, obj.getClass().getName());
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
		final String METHOD_NAME = "test4_0";
		OkuyamaClient client = factory.createClient();

		String[] strs = client.getTagKeys("tag1", false);
		log(METHOD_NAME, Arrays.toString(strs));
	}

	@Test
	public void test5_0() throws IOException, OperationFailedException {
		final String METHOD_NAME = "test5_0";
		OkuyamaClient client = factory.createClient();

		Object obj = client.removeObjectValue("HOGE1");
		log(METHOD_NAME, obj);
	}

	@Test
//	@Ignore
	public void test6_0() throws IOException, OperationFailedException, InterruptedException {
		OkuyamaClient client = factory.createClient();
	
		SecureRandom rand = new SecureRandom(); 
		String key = "HOGE_" + Long.toString(rand.nextLong());
	
		boolean success;
		success = client.addObjectValue(key, "タグ1", null, 1);
		assertTrue(success);
		success = client.addObjectValue(key, "タグ1", new String[]{"tag1","tag2"}, 1);
		assertFalse(success);

		client.removeObjectValue(key);
	}

	@Test
//	@Ignore
	public void test6_1() throws IOException, OperationFailedException, InterruptedException {
		OkuyamaClient client = factory.createClient();
	
		SecureRandom rand = new SecureRandom(); 
		String key = "HOGE_" + Long.toString(rand.nextLong());
	
		boolean success;
		success = client.addObjectValue(key, "タグ1", null, 1);
		assertTrue(success);
		Thread.sleep(1100);
		success = client.addObjectValue(key, "タグ1", new String[]{"tag1","tag2"}, 1);
		assertTrue(success);

		client.removeObjectValue(key);
	}

	@Test
	public void test15_0() throws IOException, OperationFailedException {
		final String METHOD_NAME = "test15_0";
		OkuyamaClient client = factory.createClient();
		
//		client.setObjectValue("HOGE00", new Date(), null, 0);
		VersionedValue v = client.getObjectValueVersionCheck("HOGE00");
		client.setObjectValueVersionCheck("HOGE00", new Date(), v.getVersion(), null, 0);
		v = client.getObjectValueVersionCheck("HOGE00");
		log(METHOD_NAME, v.getVersion());
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
		final String METHOD_NAME = "test22_0";
		OkuyamaClient client = factory.createClient();

		Object[] objects = client.getMultiObjectValues("HOGE00", "HOGE01", "HOGE6", "HOGE1");
		log(METHOD_NAME, Arrays.toString(objects));
	}

	@Test
	public void test23_0() throws IOException, OperationFailedException {
		final String METHOD_NAME = "test23_0";
		OkuyamaClient client = factory.createClient();

		Pair[] pairs = client.getPairsByTag("tag1");
		log(METHOD_NAME, Arrays.toString(pairs));
	}

	@Test
	public void test23_unresolved_host() throws IOException, OperationFailedException {
		try {
			OkuyamaClientFactory f = new OkuyamaClientFactoryImpl(new String[]{"unresolved.localdomain:8888"}, 5);
			fail("Expects UnknownHostException");
			f.createClient();
		} catch(UnknownHostException e) {
		}
	}
	
	@Test
	@Ignore
	public void test23_multiple_hosts() throws IOException, OperationFailedException {
		final String METHOD_NAME = "test23_multiple_hosts";
		String[] masternodes = new String[]{"localhost:8888", "127.0.0.1:8889"};
		
		OkuyamaClientFactory f = new OkuyamaClientFactoryImpl(masternodes, 5);
		OkuyamaClient client = f.createClient();

		Pair[] pairs = client.getPairsByTag("tag1");
		log(METHOD_NAME, Arrays.toString(pairs));
	}
	

	@Test
	public void test999_version() throws IOException, OperationFailedException {
		final String METHOD_NAME = "test999_version";
		try {
			OkuyamaClient client = factory.createClient();
			String str = client.getMasterNodeVersion();
			
			log(METHOD_NAME, "Vervion: " + str);
		} catch(UnknownHostException e) {
		}
	}

}