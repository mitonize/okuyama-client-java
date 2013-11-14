package mitonize.datastore;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Base64 {
	private static byte[] map_encode;
	private static byte[] map_decode;

	static {
		// エンコーディング用変換表の生成
		map_encode = new byte[64];
		for (int i=0; i < 26; ++i) {
			map_encode[i] = (byte) ('A' + i);
		}
		for (int i=26; i < 52; ++i) {
			map_encode[i] = (byte) ('a' + i - 26);
		}
		for (int i=52; i < 62; ++i) {
			map_encode[i] = (byte) ('0' + i - 52);
		}
		map_encode[62] = '+';
		map_encode[63] = '/';

		// デコーディング用変換表の生成
		map_decode = new byte[256];
		Arrays.fill(map_decode, (byte) 0);
		for (int i='A'; i <= 'Z'; ++i) {
			map_decode[i] = (byte) (i - 'A');
		}
		for (int i='a'; i <= 'z'; ++i) {
			map_decode[i] = (byte) (i - 'a' + 26);
		}
		for (int i='0'; i <= '9'; ++i) {
			map_decode[i] = (byte) (i - '0' + 52);
		}
		map_decode['+'] = 62;
		map_decode['/'] = 63;
	}

	public static ByteBuffer encodeBuffer(ByteBuffer buffer) {
		buffer.mark();
		int limit = buffer.remaining();
		ByteBuffer b = ByteBuffer.allocate((limit / 3 + (limit % 3 == 0 ? 0: 1)) * 4);		
		byte[] bits = new byte[3];

		while (buffer.hasRemaining()) {
			bits[0] = bits[1] = bits[2] = 0;
			int bc = 0;
			try {
				// 3バイト読み込んで4バイトにエンコード
				bits[bc] = buffer.get();
				++bc;
				bits[bc] = buffer.get();
				++bc;
				bits[bc] = buffer.get();
				++bc;
			} catch (BufferUnderflowException e) {
			}

			// Base64エンコーディング処理を行う
			// 変換前： aaaaaaaa bbbbbbbb cccccccc
			// 変換後： 00aaaaaa 00aabbbb 00bbbbcc 00cccccc
			int i0 = (bits[0] >>> 2) & 0x3f;
			int i1 = (bits[0] & 0x03) << 4 | (bits[1] >>> 4) & 0x0f;
			int i2 = (bits[1] & 0x0f) << 2 | (bits[2] >>> 6) & 0x03;
			int i3 = (bits[2]) & 0x3f;
//			System.err.printf("F:%8s %8s %8s\n",Integer.toBinaryString(bits[0]), Integer.toBinaryString(bits[1]), Integer.toBinaryString(bits[2]));
//			System.err.printf("B:%8s %8s %8s %8s\n", Integer.toBinaryString(i0), Integer.toBinaryString(i1), Integer.toBinaryString(i2), Integer.toBinaryString(i3));

			byte b0 = map_encode[i0];
			byte b1 = bc >= 1 ? map_encode[i1]: (byte)'=';
			byte b2 = bc >= 2 ? map_encode[i2]: (byte)'=';
			byte b3 = bc >= 3 ? map_encode[i3]: (byte)'=';
			b.put(b0).put(b1).put(b2).put(b3);
		}
		buffer.reset();
		b.flip();
		return b;
	}

	public static ByteBuffer decodeBuffer(ByteBuffer buffer) {
		buffer.mark();
		int limit = buffer.remaining();
		int blocks = limit / 4 + (limit % 4 == 0 ? 0: 1);
		ByteBuffer b = ByteBuffer.allocate(blocks * 3);
		byte[] bits = new byte[4];

		/* 
		 * バッファの末尾に余分に確保されたバイトをlimitを調節して詰める。つまりallocateした領域自体は詰めない。
		 * したがって、受け取り側で array()をしたときは注意のこと。
		 */
        for (int i=0; i < blocks; ++i) {
			bits[0] = bits[1] = bits[2] = bits[3] = 0;
			int bc = 0;
			// 4バイト読み込んで3バイトにデコード
			for (bc = 0; bc < 4; ++bc) {
				try {
					byte c = buffer.get();
					if (c == '=') {
						// '=' はパディングなので読み込み文字として扱わない
						break;
					}
					bits[bc] = map_decode[c];
				} catch (BufferUnderflowException e) {
					// 足りない部分は0でよいので例外処理は不要
					break;
				}
			}

			// Base64デコーディング処理を行う
			// 変換前： 00aaaaaa 00aabbbb 00bbbbcc 00cccccc
			// 変換後： aaaaaaaa bbbbbbbb cccccccc
			if (bc >= 1) {
				byte b0 = (byte) ((bits[0] << 2) | (bits[1] >>> 4) & 0x03);
				b.put(b0);
			}
			if (bc >= 2) {
				byte b1 = (byte) ((bits[1] << 4) | (bits[2] >>> 2) & 0x0f);
				b.put(b1);
			}
			if (bc == 3) {
				byte b2 = (byte) (bits[2] << 6);
				if (b2 != 0) {
				    b.put(b2);
				}
			} else if (bc == 4) {
                byte b2 = (byte) ((bits[2] << 6) | bits[3]);
                b.put(b2);
            }
		}
		buffer.reset();
		b.flip();
		return b;
	}
}
