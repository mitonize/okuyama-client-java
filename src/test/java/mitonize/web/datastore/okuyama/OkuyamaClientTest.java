package mitonize.web.datastore.okuyama;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;

import mitonize.web.datastore.OperationFailedException;
import mitonize.web.datastore.Pair;
import mitonize.web.datastore.VersionedValue;

import org.apache.commons.codec.binary.Base64;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class OkuyamaClientTest {
	static OkuyamaClientFactory factory;
	
	@BeforeClass
	public static void setup() throws UnknownHostException {
		factory = new OkuyamaClientFactoryImpl(new String[]{"127.0.0.1:8888"});
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

		char[] verylargevalue = new char[1572865];
		Arrays.fill(verylargevalue, 'a');
		try {
			// TODO:内部バッファの容量よりも大きいサイズの値を登録しようとすると BufferOverflowExceptionが発生
			client.setObjectValue("hoge2", verylargevalue, null, 0);
			fail("must throw an exception");
		} catch (OperationFailedException e) {
			System.out.println(e.getMessage());
		}
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
