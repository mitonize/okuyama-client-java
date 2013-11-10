package mitonize.datastore.okuyama;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;

import mitonize.datastore.OperationFailedException;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.BeforeClass;
import org.junit.Test;

public class CompatibilityTest {
	static OkuyamaClientFactory factory;
	
	@BeforeClass
	public static void setup() throws UnknownHostException {
		factory = new OkuyamaClientFactoryImpl(new String[]{"127.0.0.1:8888"}, 5);
	}

	private okuyama.imdst.client.OkuyamaClient createClient() throws OkuyamaClientException {
		String[] masternodes = new String[]{"localhost:8888"};
		okuyama.imdst.client.OkuyamaClient origClient = new okuyama.imdst.client.OkuyamaClient();
		origClient.setConnectionInfos(masternodes);
		origClient.autoConnect();
		return origClient;
	}

	@Test
	public void compatibility_0() throws IOException, OperationFailedException, OkuyamaClientException {
		OkuyamaClient client = factory.createClient();
		okuyama.imdst.client.OkuyamaClient origClient = createClient();
		

		System.err.println("/** compatibility_0:オリジナルset → 独自get */");
		origClient.setObjectValue("COMPAT_01", "STRING");
		Object[] obj10 = origClient.getObjectValue("COMPAT_01");
		System.err.println(obj10[1]);

		Object obj11 = client.getObjectValue("COMPAT_01");
		System.err.println(obj11);

		System.err.println("/** compatibility_0:独自set → オリジナルget */");
		client.setObjectValue("COMPAT_00", "STRING", null, 0);

		Object obj = client.getObjectValue("COMPAT_00");
		System.err.println(obj);

		Object[] obj1 = origClient.getObjectValue("COMPAT_00");
		System.err.println(obj1[1]);

	}

	@Test
	public void compatibility_1() throws IOException, OperationFailedException, OkuyamaClientException {
//		((OkuyamaClientFactoryImpl) factory).setCompatibilityMode(false);
		OkuyamaClient client = factory.createClient();
		okuyama.imdst.client.OkuyamaClient origClient = createClient();
		
		System.err.println("/** compatibility_1:オリジナルset → 独自get */");
		origClient.setObjectValue("COMPAT_10", "");
		Object[] obj00 = origClient.getObjectValue("COMPAT_10");
		System.err.println(obj00[1]);

		Object obj01 = client.getObjectValue("COMPAT_10");
		System.err.println(obj01);
		assertEquals(obj00[1], obj01);

		System.err.println("/** compatibility_1:独自set → オリジナルget */");
		client.setObjectValue("COMPAT_11", "", null, 0);

		Object obj10 = client.getObjectValue("COMPAT_11");
		System.err.println(obj10);

		Object[] obj11 = origClient.getObjectValue("COMPAT_11");
		System.err.println(obj11[1]);
		assertEquals(obj10, obj11[1]);
	}

	@Test
	public void compatibility_2() throws IOException, OperationFailedException, OkuyamaClientException {
//		((OkuyamaClientFactoryImpl) factory).setCompatibilityMode(false);
		OkuyamaClient client = factory.createClient();
		okuyama.imdst.client.OkuyamaClient origClient = createClient();
		
		System.err.println("/** compatibility_2:オリジナルset → 独自get */");
		try {
			origClient.setObjectValue("COMPAT_20", null);
			fail("Okuyama does not allow to store null value.");
		} catch (OkuyamaClientException e) {
		}
		Object[] obj00 = origClient.getObjectValue("COMPAT_20");
		System.err.println(obj00[1]);

		Object obj01 = client.getObjectValue("COMPAT_20");
		System.err.println(obj01);
		assertEquals(obj00[1], obj01);

		System.err.println("/** compatibility_2:独自set → オリジナルget */");
		try {
			client.setObjectValue("COMPAT_21", null, null, 0);
			fail("Okuyama does not allow to store null value.");
		} catch (IllegalArgumentException e) {
		}

		Object obj10 = client.getObjectValue("COMPAT_21");
		System.err.println(obj10);

		Object[] obj11 = origClient.getObjectValue("COMPAT_21");
		System.err.println(obj11[1]);
		assertEquals(obj10, obj11[1]);
	}

	@Test
	public void compatibility_3() throws IOException, OperationFailedException, OkuyamaClientException {
//		((OkuyamaClientFactoryImpl) factory).setCompatibilityMode(false);
		OkuyamaClient client = factory.createClient();
		okuyama.imdst.client.OkuyamaClient origClient = createClient();
		
		System.err.println("/** compatibility_3:オリジナルset → 独自get */");
		origClient.setObjectValue("COMPAT_30", "日本語表示");
		Object[] obj00 = origClient.getObjectValue("COMPAT_30");
		System.err.println(obj00[1]);

		Object obj01 = client.getObjectValue("COMPAT_30");
		System.err.println(obj01);
		assertEquals(obj00[1], obj01);

		System.err.println("/** compatibility_3:独自set → オリジナルget */");
		client.setObjectValue("COMPAT_31", "日本語表示", null, 0);
		Object obj10 = client.getObjectValue("COMPAT_31");
		System.err.println(obj10);

		Object[] obj11 = origClient.getObjectValue("COMPAT_31");
		System.err.println(obj11[1]);
		assertEquals(obj10, obj11[1]);
	}

	@Test
	public void compatibility_4() throws IOException, OperationFailedException, OkuyamaClientException {
//		((OkuyamaClientFactoryImpl) factory).setCompatibilityMode(false);
		OkuyamaClient client = factory.createClient();
		okuyama.imdst.client.OkuyamaClient origClient = createClient();
		
		System.err.println("/** compatibility_4:オリジナルset → 独自get */");
		origClient.setValue("COMPAT_40", "日本語表示");
		Object[] obj00 = origClient.getValue("COMPAT_40");
		System.err.println(obj00[1]);

		/** コンパチビリティモードによらず、シリアライズされているかを検知して不要ならデシリアライズしない。 */
		Object obj01 = client.getObjectValue("COMPAT_40");
		System.err.println(obj01);
		assertEquals(obj00[1], obj01);

		System.err.println("/** compatibility_4:独自set → オリジナルget */");
		/** コンパチビリティモードをOffにすると文字列はシリアライズせずに保管するため setValueと同等となる */
		((OkuyamaClientFactoryImpl) factory).setCompatibilityMode(false);
		client = factory.createClient();
		
		client.setObjectValue("COMPAT_41", "日本語表示", null, 0);
		Object obj10 = client.getObjectValue("COMPAT_41");
		System.err.println(obj10);

		Object[] obj11 = origClient.getValue("COMPAT_41");
		System.err.println(obj11[1]);
		
		assertEquals(obj10, obj11[1]);
	}

	@Test
	public void compatibility_5() throws IOException, OperationFailedException, OkuyamaClientException {
//		((OkuyamaClientFactoryImpl) factory).setCompatibilityMode(false);
		OkuyamaClient client = factory.createClient();
		okuyama.imdst.client.OkuyamaClient origClient = createClient();
		
		System.err.println("/** compatibility_4:オリジナルset → 独自get */");
		origClient.setObjectValue("COMPAT_400", new String[]{"タグ1"}, "STRING");
		origClient.setObjectValue("COMPAT_401", new String[]{"タグ1", "tag2"}, "日本語表示");
		Object[] obj00 = origClient.getTagKeys("タグ1");
		System.err.println(Arrays.toString((String[]) obj00[1]));

		/** コンパチビリティモードによらず、シリアライズされているかを検知して不要ならデシリアライズしない。 */
		Object[] obj01 = client.getTagKeys("タグ1", false);
		System.err.println(Arrays.toString(obj01));
//		assertEquals(obj00[1], obj01);

		System.err.println("/** compatibility_4:独自set → オリジナルget */");
		/** コンパチビリティモードをOffにすると文字列はシリアライズせずに保管するため setValueと同等となる */
		((OkuyamaClientFactoryImpl) factory).setCompatibilityMode(false);
		client = factory.createClient();
		
		client.setObjectValue("COMPAT_41", "日本語表示", null, 0);
		Object obj10 = client.getObjectValue("COMPAT_41");
		System.err.println(obj10);

		Object[] obj11 = origClient.getValue("COMPAT_41");
		System.err.println(obj11[1]);
		
		assertEquals(obj10, obj11[1]);
	}
}
