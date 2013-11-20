package mitonize.datastore.okuyama;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import mitonize.datastore.Base64;

import org.junit.Assert;
import org.junit.Test;

public class OkuyamaClientImplTest {

	@Test
	public void test() {
		Charset cs = Charset.forName("UTF-8");
		String str = "abcd";
		ByteBuffer enc = Base64.encodeBuffer(cs.encode(str));
		System.out.println(cs.decode(enc).toString());
		enc.flip();

		ByteBuffer res = Base64.decodeBuffer(enc);
		Assert.assertEquals(6, res.array().length);

		Assert.assertEquals(4, OkuyamaClientImpl2.createFlipedBytes(res).length);
	}
}
