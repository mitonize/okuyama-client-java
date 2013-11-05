package mitonize.datastore.okuyama;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;

import mitonize.datastore.ChannelManager;
import mitonize.datastore.OperationFailedException;
import mitonize.datastore.Pair;
import mitonize.datastore.VersionedValue;

import org.apache.commons.codec.binary.Base64;

public class OkuyamaClientImpl implements OkuyamaClient {
	ChannelManager channelManager;
	Charset cs;
	boolean base64Key = true;

	public OkuyamaClientImpl(ChannelManager channelManager) {
		this.channelManager = channelManager;
		cs = Charset.forName("UTF-8");
	}

	
	/**
	 * 指定されたプロトコル番号を設定したリクエスト用のバッファを生成する。
	 * @param protocolNo プロトコル番号
	 * @return ByteBufferインスタンス
	 */
	ByteBuffer createBuffer(int protocolNo) {
		ByteBuffer buffer = ByteBuffer.allocate(8129);
		buffer.put(Integer.toString(protocolNo).getBytes());
		return buffer;
	}

	/**
	 * プロトコル書式に合わせて数値フィールドをバッファに追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @param num 数値
	 */
	void appendNumber(ByteBuffer buffer, long num) {
		buffer.put((byte)',').put(Long.toString(num).getBytes());
	}

	/**
	 * プロトコル書式に合わせて文字列フィールドをバッファに追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @param str 文字列
	 * @param base64 Base64エンコード有無
	 */
	void appendString(ByteBuffer buffer, String str, boolean base64) {
		ByteBuffer b = cs.encode(CharBuffer.wrap(str));
		if (base64) {
			b = ByteBuffer.wrap(Base64.encodeBase64(b.array()));
		}
		buffer.put((byte)',').put(b);
	}

	/**
	 * プロトコル書式に合わせてオブジェクトをバッファに追加する。
	 * シリアライズ後のバイト列をBase64エンコードした文字列として追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @param obj オブジェクト
	 * @throws IOException シリアライズできない場合
	 */
	void appendSerializedObjectBase64(ByteBuffer buffer, Object obj) throws IOException {
		ByteArrayOutputStream b = new ByteArrayOutputStream();
		ObjectOutputStream stream = new ObjectOutputStream(b);
		stream.writeObject(obj);
		byte[] r = b.toByteArray();
		r = Base64.encodeBase64(r);
		buffer.put((byte)',').put(r);
	}

	/**
	 * プロトコル書式に合わせて文字列リストをバッファに追加する。主にタグを指定する場合に用いる。
	 * リストが空の場合は "(B)" を追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @param strs 文字列リスト
	 * @param base64 Base64エンコード有無
	 */
	void appendStringList(ByteBuffer buffer, String[] strs, boolean base64) {
		if (strs == null) {
			buffer.put((byte)',').put("(B)".getBytes());			
		} else {
			buffer.put((byte)',');
			for (int i=0; i < strs.length; ++i) {
				if (i != 0) {
					buffer.put((byte)':');
				}				
				byte[] b = strs[i].getBytes();
				if (base64) {
					b = Base64.encodeBase64(b);
				}
				buffer.put(b);
			}
		}
	}

	/**
	 * 作成したリクエストを Channel に送信する。
	 * @param channel ソケットチャネル
	 * @param request リクエストの ByteBufferオブジェクト
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 */
	void sendRequest(SocketChannel channel, ByteBuffer request) throws IOException {
		request.put((byte)'\n');
		request.flip();
		while (request.hasRemaining()) {
			channel.write(request);
		}
		System.err.print("REQUEST:" + cs.decode((ByteBuffer) request.rewind()).toString());
	}

	/**
	 * サーバからのレスポンスをバッファに読み込む。
	 * @param channel ソケットチャネル
	 * @return レスポンスを読み込んだバッファ
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 */
	ByteBuffer readResponse(SocketChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(8192);
		channel.read(buffer);
		buffer.flip();
		return buffer;
	}


	/**
	 * レスポンスのバッファから数値を読み取る。バッファが足りなくなったら指定したチャネルから追加読み取りする。
	 * @param buffer ByteBufferオブジェクト
	 * @param channel ソケットチャネル
	 * @return 読み取った数値
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 * @throws {@link IllegalStateException} 期待したフォーマットでない場合
	 */
	long nextNumber(ByteBuffer buffer, SocketChannel channel) throws IOException {
		long code = 0;
		while (true) {
			while (buffer.hasRemaining()) {
				byte ch = buffer.get();
				if ('0' <= ch && ch <= '9') {
					code = code * 10 + (ch - '0');
				} else if (ch == ',' || ch == '\n') {
					return (int) code;
				} else {
					throw new IllegalStateException(String.format("Format error on expecting digit: %c", ch));
				}
			}
			buffer.flip();
			channel.read(buffer);
			buffer.flip();
		}
	}

	/**
	 * レスポンスのバッファから文字列を読み取る。バッファが足りなくなったら指定したチャネルから追加読み取りする。
	 * @param buffer ByteBufferオブジェクト
	 * @param channel ソケットチャネル
	 * @param base64Key 読み取る文字列がBase64エンコードされている前提でデコードする。
	 * @return 読み取った文字列
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 * @throws {@link IllegalStateException} 期待したフォーマットでない場合
	 */
	String nextString(ByteBuffer buffer, SocketChannel channel, boolean base64Key) throws IOException {
		ByteBuffer strBuffer = ByteBuffer.allocate(8192);
		while (true) {
			while (buffer.hasRemaining()) {
				byte ch = buffer.get();
				if (ch == ',' || ch == '\n') {
					strBuffer.flip();
					if (base64Key) {
						return cs.decode(ByteBuffer.wrap(Base64.decodeBase64(strBuffer.array()))).toString();
					} else {
						return cs.decode(strBuffer).toString();
					}
				} else {
					strBuffer.put(ch);
				}
			}
			buffer.flip();
			channel.read(buffer);
			buffer.flip();
		}
	}
	
	/**
	 * レスポンスのバッファから文字列リストを読み取る。バッファが足りなくなったら指定したチャネルから追加読み取りする。
	 * @param buffer ByteBufferオブジェクト
	 * @param channel ソケットチャネル
	 * @param base64Key 読み取る文字列がBase64エンコードされている前提でデコードする。
	 * @return 読み取った文字列の配列
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 * @throws {@link IllegalStateException} 期待したフォーマットでない場合
	 */
	String[] nextStringList(ByteBuffer buffer, SocketChannel channel, boolean base64Key) throws IOException {
		ByteBuffer strBuffer = ByteBuffer.allocate(8192);
		ArrayList<String> list = new ArrayList<String>();
		while (true) {
			while (buffer.hasRemaining()) {
				byte ch = buffer.get();
				if (ch == ':') {
					if (base64Key) {
						list.add(cs.decode(ByteBuffer.wrap(Base64.decodeBase64(strBuffer.array()))).toString());
					} else {
						list.add(cs.decode(strBuffer).toString());
					}			
				}
				if (ch == ',' || ch == '\n') {
					strBuffer.flip();
					if (base64Key) {
						list.add(cs.decode(ByteBuffer.wrap(Base64.decodeBase64(strBuffer.array()))).toString());
					} else {
						list.add(cs.decode(strBuffer).toString());
					}
					return list.toArray(new String[list.size()]);
				} else {
					strBuffer.put(ch);
				}
			}
			buffer.flip();
			channel.read(buffer);
			buffer.flip();
		}
	}

	/**
	 * レスポンスのバッファからJavaオブジェクトを読み取る。Base64デコードしたバイト列がシリアライズされた列であれば
	 * デシリアライズする。シリアライズされたバイト列でなければ文字列オブジェクトとして返す。
	 * @param buffer ByteBufferオブジェクト
	 * @param channel ソケットチャネル
	 * @return 読み取ったバイト列
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 * @throws ClassNotFoundException  
	 * @throws {@link IllegalStateException} 期待したフォーマットでない場合
	 */
	Object nextObject(ByteBuffer buffer, SocketChannel channel) throws IOException, ClassNotFoundException  {
		ByteBuffer bytes = ByteBuffer.allocate(8192);
		while (true) {
			while (buffer.hasRemaining()) {
				byte ch = buffer.get();
				if (ch == ',' || ch == '\n') {
					byte[] b = Base64.decodeBase64(bytes.array());
					return decodeObject(b);
				} else {
					bytes.put(ch);
				}
			}
			buffer.flip();
			channel.read(buffer);
			buffer.flip();
		}
	}
	
	/**
	 * バイト列からJavaオブジェクトを復元する。バイト列がシリアライズされた列であれば
	 * デシリアライズする。シリアライズされたバイト列でなければ文字列オブジェクトとして返す。
	 * @param b バイト配列
	 * @return デコードしたオブジェクト
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 * @throws ClassNotFoundException  
	 */
	Object decodeObject(byte[] b) throws IOException, ClassNotFoundException {
		if (b.length == 0) {
			return null;
		}
		/**
		 * シリアル化されたバイト列はマジックコード 0xac 0xed で始まるという
		 * JavaSEの仕様(Object Serialization Stream Protocol)である。
		 * @see http://docs.oracle.com/javase/6/docs/platform/serialization/spec/protocol.html
		 */
		if (b.length > 2 && b[0] == (byte) 0xac && b[1] == (byte) 0xed) { // Magic code of Object Serialization Stream Protocol
			ObjectInputStream os = new ObjectInputStream(new ByteArrayInputStream(b));
			Object obj = os.readObject();
			return obj;
		} else {
			/** シリアル化されて以内バイト列は文字列として復元 **/
			return cs.decode(ByteBuffer.wrap(b)).toString();
		}	
	}
	
	@Override
	public long initClient() throws IOException {
		SocketChannel channel = channelManager.aquire();
		try	{
			ByteBuffer request = createBuffer(0);
			sendRequest(channel, request);
	
			ByteBuffer buffer = readResponse(channel);
			long code = nextNumber(buffer, channel);
			if (code != 0) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, channel, false);
			if (str.equals("true")) {
				return nextNumber(buffer, channel);
			} else {
				throw new IllegalStateException();
			}
		} finally {
			channelManager.recycle(channel);
		}
	}


	@Override
	public boolean setObjectValue(String key, Object value, String[] tags, long age) throws IOException, OperationFailedException {
		SocketChannel channel = channelManager.aquire();
		try	{
			ByteBuffer request = createBuffer(1);
			appendString(request, key, base64Key);
			appendStringList(request, tags, true);
			appendNumber(request, 0);
			if (value instanceof String) {
				appendString(request, (String) value, true);				
			} else {
				appendSerializedObjectBase64(request, value);
			}
			appendNumber(request, 0);
			sendRequest(channel, request);
	
			ByteBuffer buffer = readResponse(channel);
			long code = nextNumber(buffer, channel);
			if (code != 1) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, channel, false);
			if (str.equals("true")) {
				return true;
			} else if (str.equals("false")){
				String msg = nextString(buffer, channel, false);				
				throw new OperationFailedException(msg);
			} else {
				throw new OperationFailedException();				
			}
		} finally {
			channelManager.recycle(channel);
		}		
	}

	@Override
	public Object getObjectValue(String key) throws IOException, ClassNotFoundException, OperationFailedException {
		SocketChannel channel = channelManager.aquire();
		try	{
			ByteBuffer request = createBuffer(2);
			appendString(request, key, base64Key);
			sendRequest(channel, request);
	
			ByteBuffer buffer = readResponse(channel);
			long code = nextNumber(buffer, channel);
			if (code != 2) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, channel, false);
			if (str.equals("true")) {
				Object obj = nextObject(buffer, channel);
				return obj;
			} else if (str.equals("false")) {
				/** falseの場合は第三列が文字列を返すときはエラーメッセージを例外としてスローし、空の時は値無しとしてnullを返す。 */
				String msg = nextString(buffer, channel, false);
				if (msg != null && !msg.isEmpty()) {
					throw new OperationFailedException(msg);
				}
				return null;
			} else {
				throw new OperationFailedException();
			}
		} finally {
			channelManager.recycle(channel);
		}
	}

	@Override
	public String[] getTagKeys(String tag, boolean withDeletedKeys) throws IOException, OperationFailedException {
		SocketChannel channel = channelManager.aquire();
		try	{
			ByteBuffer request = createBuffer(4);
			appendString(request, tag, base64Key);
			appendString(request, withDeletedKeys ? "true": "false", false);
			sendRequest(channel, request);

			ByteBuffer buffer = readResponse(channel);
			long code = nextNumber(buffer, channel);
			if (code != 4) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, channel, false);
			if (str.equals("true")) {
				String[] strs = nextStringList(buffer, channel, base64Key);
				return strs;
			} else {
				throw new OperationFailedException();
			}
		} finally {
			channelManager.recycle(channel);
		}
	}

	@Override
	public VersionedValue getObjectValueVersionCheck(String key) throws IOException, ClassNotFoundException, OperationFailedException {
		SocketChannel channel = channelManager.aquire();
		try	{
			ByteBuffer request = createBuffer(15);
			appendString(request, key, base64Key);
			sendRequest(channel, request);
	
			ByteBuffer buffer = readResponse(channel);
			long code = nextNumber(buffer, channel);
			if (code != 15) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, channel, false);
			if (str.equals("true")) {
				Object obj = nextObject(buffer, channel);
				String version = nextString(buffer, channel, false);
				return new VersionedValue(obj, version);
			} else if (str.equals("false")) {
				/** falseの場合は第三列が文字列を返すときはエラーメッセージを例外としてスローし、空の時は値無しとしてnullを返す。 */
				String msg = nextString(buffer, channel, false);
				if (msg != null && !msg.isEmpty()) {
					throw new IllegalStateException(msg);
				}
				return null;
			} else {
				throw new IllegalStateException();
			}
		} finally {
			channelManager.recycle(channel);
		}
	}

	@Override
	public boolean setObjectValueVersionCheck(String key, Object value, String version, String[] tags, long age) throws IOException, OperationFailedException {
		SocketChannel channel = channelManager.aquire();
		try	{
			ByteBuffer request = createBuffer(16);
			appendString(request, key, base64Key);
			appendStringList(request, tags, true);
			appendNumber(request, 0);
			if (value instanceof String) {
				appendString(request, (String) value, true);				
			} else {
				appendSerializedObjectBase64(request, value);
			}
//			appendNumber(request, 0);
			appendString(request, version, false);
			sendRequest(channel, request);
	
			ByteBuffer buffer = readResponse(channel);
			long code = nextNumber(buffer, channel);
			if (code != 16) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, channel, false);
			if (str.equals("true")) {
				return true;
			} else if (str.equals("false")){
				String msg = nextString(buffer, channel, false);				
				throw new OperationFailedException(msg);
			} else {
				throw new OperationFailedException();				
			}
		} finally {
			channelManager.recycle(channel);
		}		
	}

	@Override
	public Pair[] getPairsByTag(String tag) throws IOException, ClassNotFoundException {
		SocketChannel channel = channelManager.aquire();
		try	{
			ByteBuffer request = createBuffer(23);
			appendString(request, tag, true);
			sendRequest(channel, request);

			ByteBuffer buffer = readResponse(channel);
			ArrayList<Pair> list = new ArrayList<Pair>(); 
			while (true) {
				String str = nextString(buffer, channel, false);
				if (str.equals("END")) {
					return list.toArray(new Pair[list.size()]);
				}
				if (str.equals("23")) {
					str = nextString(buffer, channel, false);
					if (str.equals("true")) {
						String key = nextString(buffer, channel, base64Key);
						Object obj = nextObject(buffer, channel);
						list.add(new Pair(key, obj));
					} else {
						throw new IllegalStateException();
					}
				} else {
					throw new IllegalStateException();
				}				
			}
		} finally {
			channelManager.recycle(channel);
		}
	}
}