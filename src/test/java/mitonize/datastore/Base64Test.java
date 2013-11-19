package mitonize.datastore;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.junit.Test;


public class Base64Test extends Base64 {

    @Test
    public void testEncodeBuffer() {
    }

    @Test
    public void testDecodeBuffer0() {
        Charset cs = Charset.forName("UTF-8");
        String str = "";
        ByteBuffer enc = Base64.encodeBuffer(cs.encode(str));
        System.out.println(cs.decode(enc).toString());
        enc.flip();

        ByteBuffer res = Base64.decodeBuffer(enc);        
        System.out.println(cs.decode(res).toString());
    }

    @Test
    public void testDecodeBuffer1() {
        Charset cs = Charset.forName("UTF-8");
        String str = "a";
        ByteBuffer enc = Base64.encodeBuffer(cs.encode(str));
        System.out.println(cs.decode(enc).toString());
        enc.flip();

        ByteBuffer res = Base64.decodeBuffer(enc);
        System.out.println(cs.decode(res).toString());
    }

    @Test
    public void testDecodeBuffer2() {
        Charset cs = Charset.forName("UTF-8");
        String str = "ab";
        ByteBuffer enc = Base64.encodeBuffer(cs.encode(str));
        System.out.println(cs.decode(enc).toString());
        enc.flip();

        ByteBuffer res = Base64.decodeBuffer(enc);
        System.out.println(cs.decode(res).toString());
    }

    @Test
    public void testDecodeBuffer3() {
        Charset cs = Charset.forName("UTF-8");
        String str = "abc";
        ByteBuffer enc = Base64.encodeBuffer(cs.encode(str));
        System.out.println(cs.decode(enc).toString());
        enc.flip();

        ByteBuffer res = Base64.decodeBuffer(enc);
        System.out.println(cs.decode(res).toString());
    }

    @Test
    public void testDecodeBuffer4() {
        Charset cs = Charset.forName("UTF-8");
        String str = "abcd";
        ByteBuffer enc = Base64.encodeBuffer(cs.encode(str));
        System.out.println(cs.decode(enc).toString());
        enc.flip();

        ByteBuffer res = Base64.decodeBuffer(enc);
        System.out.println(cs.decode(res).toString());
    }

    @Test
    public void testDecodeBuffer5() {
        Charset cs = Charset.forName("UTF-8");
        String str = "abcde";
        ByteBuffer enc = Base64.encodeBuffer(cs.encode(str));
        System.out.println(cs.decode(enc).toString());
        enc.flip();

        ByteBuffer res = Base64.decodeBuffer(enc);
        System.out.println(cs.decode(res).toString());
    }

    @Test
    public void testDecodeBuffer6() {
        Charset cs = Charset.forName("UTF-8");
        String str = "abcde";
        ByteBuffer enc = Base64.encodeBuffer(cs.encode(str));
        System.out.println(cs.decode(enc).toString());
        enc.flip();

        ByteBuffer res = Base64.decodeBuffer(enc);
        System.out.println(cs.decode(res).toString());
    }

    @Test
    public void testDecodeBuffer10() {
        Charset cs = Charset.forName("UTF-8");
        String str = " ";
        ByteBuffer enc = Base64.encodeBuffer(cs.encode(str));
        System.out.println(cs.decode(enc).toString());
        enc.flip();

        ByteBuffer res = Base64.decodeBuffer(enc);
        System.out.println(cs.decode(res).toString());
    }
}
