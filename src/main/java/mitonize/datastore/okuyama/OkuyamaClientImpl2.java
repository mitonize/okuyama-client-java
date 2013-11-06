package mitonize.datastore.okuyama;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;

import mitonize.datastore.ChannelManager;
import mitonize.datastore.OperationFailedException;
import mitonize.datastore.Pair;
import mitonize.datastore.SocketManager;
import mitonize.datastore.VersionedValue;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.xml.internal.ws.api.streaming.XMLStreamReaderFactory.Default;

public class OkuyamaClientImpl2 implements OkuyamaClient {
	Logger logger = LoggerFactory.getLogger(OkuyamaClientImpl2.class);

	SocketManager socketManager;
	Charset cs;
	boolean base64Key = true;
	ByteBuffer buffer = ByteBuffer.allocate(8129);
	private boolean doCompress = true;

	public OkuyamaClientImpl2(SocketManager socketManager) {
		this.socketManager = socketManager;
		cs = Charset.forName("UTF-8");
	}

	
	/**
	 * 指定されたプロトコル番号を設定したリクエスト用のバッファを生成する。
	 * @param protocolNo プロトコル番号
	 * @return ByteBufferインスタンス
	 */
	void createBuffer(int protocolNo) {
		buffer.clear();
		buffer.put(Integer.toString(protocolNo).getBytes());
	}

	/**
	 * プロトコル書式に合わせてセパレータをバッファに追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @throws IOException 
	 */
	void appendSeparator(WritableByteChannel channel) throws IOException {
		buffer.put((byte)',');
	}
	/**
	 * プロトコル書式に合わせて数値フィールドをバッファに追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @param num 数値
	 * @throws IOException 
	 */
	void appendNumber(WritableByteChannel channel, long num) throws IOException {
		byte[] b = Long.toString(num).getBytes();
		if (buffer.remaining() < b.length + 1) {
			buffer.flip();
			channel.write(buffer);
			buffer.flip();
		}
		buffer.put((byte)',').put(b);
	}

	/**
	 * プロトコル書式に合わせて文字列フィールドをバッファに追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @param str 文字列
	 * @param base64 Base64エンコード有無
	 * @throws IOException 
	 */
	void appendString(WritableByteChannel channel, String str, boolean base64) throws IOException {
		if (str == null || str.length() == 0) {
			str = "(B)";
			base64 = false;
		}
		ByteBuffer b = cs.encode(CharBuffer.wrap(str));
		if (base64) {
			b = ByteBuffer.wrap(Base64.encodeBase64(b.array()));
		}
		if (buffer.remaining() < b.position() + 1) {
			buffer.flip();
			channel.write(buffer);
			buffer.flip();
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
	void appendSerializedObjectBase64(WritableByteChannel channel, Object obj) throws IOException {
		if (obj == null) {
			appendString(channel, "(B)", false);
			return;
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream stream = new ObjectOutputStream(baos);
		stream.writeObject(obj);
		stream.close();

		byte[] b = baos.toByteArray();	
		b = Base64.encodeBase64(b);
		buffer.put((byte)',');
		System.err.println("size:" + b.length);
		if (buffer.remaining() < b.length + 1) {
			buffer.flip();
			channel.write(buffer);
			channel.write(ByteBuffer.wrap(b));
			buffer.flip();
			buffer.clear();
		} else {
			buffer.put(b);
		}
	}

	/**
	 * プロトコル書式に合わせて文字列リストをバッファに追加する。主にタグを指定する場合に用いる。
	 * リストが空の場合は "(B)" を追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @param strs 文字列リスト
	 * @param base64 Base64エンコード有無
	 */
	void appendStringList(WritableByteChannel channel, String[] strs, boolean base64) {
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
	void sendRequest(WritableByteChannel channel) throws IOException {
		buffer.put((byte)'\n');
		buffer.flip();
		while (buffer.hasRemaining()) {
			channel.write(buffer);
		}
		if (logger.isDebugEnabled()) {
			logger.debug(cs.decode((ByteBuffer) buffer.rewind()).toString());
		}
	}

	/**
	 * サーバからのレスポンスをバッファに読み込む。
	 * @param channel ソケットチャネル
	 * @return レスポンスを読み込んだバッファ
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 */
	ByteBuffer readResponse(ReadableByteChannel channel) throws IOException {
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
	long nextNumber(ByteBuffer buffer, ReadableByteChannel channel) throws IOException {
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
	String nextString(ByteBuffer buffer, ReadableByteChannel channel, boolean base64Key) throws IOException {
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
	String[] nextStringList(ByteBuffer buffer, ReadableByteChannel channel, boolean base64Key) throws IOException {
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
	Object nextObject(ByteBuffer buffer, ReadableByteChannel channel) throws IOException, ClassNotFoundException  {
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
		Socket socket = socketManager.aquire();
		try	{
			WritableByteChannel wchannel = Channels.newChannel(socket.getOutputStream());
			ReadableByteChannel rchannel = Channels.newChannel(socket.getInputStream());
			createBuffer(0);
			sendRequest(wchannel);
	
			ByteBuffer buffer = readResponse(rchannel);
			long code = nextNumber(buffer, rchannel);
			if (code != 0) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, rchannel, false);
			if (str.equals("true")) {
				return nextNumber(buffer, rchannel);
			} else {
				throw new IllegalStateException();
			}
		} finally {
			socketManager.recycle(socket);
		}
	}


	@Override
	public boolean setObjectValue(String key, Object value, String[] tags, long age) throws IOException, OperationFailedException {
		Socket socket = socketManager.aquire();
		try	{
			WritableByteChannel wchannel = Channels.newChannel(socket.getOutputStream());
			ReadableByteChannel rchannel = Channels.newChannel(socket.getInputStream());
			createBuffer(1);
			appendString(wchannel, key, base64Key);
			appendStringList(wchannel, tags, true);
			appendNumber(wchannel, 0);
			if (value instanceof String) {
				appendString(wchannel, (String) value, true);				
			} else {
				appendSerializedObjectBase64(wchannel, value);
			}
			appendNumber(wchannel, age);
			appendSeparator(wchannel);
			sendRequest(wchannel);
	
			ByteBuffer buffer = readResponse(rchannel);
			long code = nextNumber(buffer, rchannel);
			if (code != 1) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, rchannel, false);
			if (str.equals("true")) {
				return true;
			} else if (str.equals("false")){
				String msg = nextString(buffer, rchannel, false);				
				throw new OperationFailedException(msg);
			} else {
				String msg = nextString(buffer, rchannel, false);
				throw new OperationFailedException(msg);
			}
		} finally {
			socketManager.recycle(socket);
		}		
	}

	@Override
	public Object getObjectValue(String key) throws IOException, ClassNotFoundException, OperationFailedException {
		Socket socket = socketManager.aquire();
		try	{
			WritableByteChannel wchannel = Channels.newChannel(socket.getOutputStream());
			ReadableByteChannel rchannel = Channels.newChannel(socket.getInputStream());
			createBuffer(2);
			appendString(wchannel, key, base64Key);
			sendRequest(wchannel);
	
			ByteBuffer buffer = readResponse(rchannel);
			long code = nextNumber(buffer, rchannel);
			if (code != 2) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, rchannel, false);
			if (str.equals("true")) {
				Object obj = nextObject(buffer, rchannel);
				return obj;
			} else if (str.equals("false")) {
				/** falseの場合は第三列が文字列を返すときはエラーメッセージを例外としてスローし、空の時は値無しとしてnullを返す。 */
				String msg = nextString(buffer, rchannel, false);
				if (msg != null && !msg.isEmpty()) {
					throw new OperationFailedException(msg);
				}
				return null;
			} else {
				throw new OperationFailedException();
			}
		} finally {
			socketManager.recycle(socket);
		}
	}

	@Override
	public String[] getTagKeys(String tag, boolean withDeletedKeys) throws IOException, OperationFailedException {
		Socket socket = socketManager.aquire();
		try	{
			WritableByteChannel wchannel = Channels.newChannel(socket.getOutputStream());
			ReadableByteChannel rchannel = Channels.newChannel(socket.getInputStream());
			createBuffer(4);
			appendString(wchannel, tag, base64Key);
			appendString(wchannel, withDeletedKeys ? "true": "false", false);
			sendRequest(wchannel);

			ByteBuffer buffer = readResponse(rchannel);
			long code = nextNumber(buffer, rchannel);
			if (code != 4) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, rchannel, false);
			if (str.equals("true")) {
				String[] strs = nextStringList(buffer, rchannel, base64Key);
				return strs;
			} else {
				throw new OperationFailedException();
			}
		} finally {
			socketManager.recycle(socket);
		}
	}

	@Override
	public VersionedValue getObjectValueVersionCheck(String key) throws IOException, ClassNotFoundException, OperationFailedException {
		Socket socket = socketManager.aquire();
		try	{
			WritableByteChannel wchannel = Channels.newChannel(socket.getOutputStream());
			ReadableByteChannel rchannel = Channels.newChannel(socket.getInputStream());
			createBuffer(15);
			appendString(wchannel, key, base64Key);
			sendRequest(wchannel);
	
			ByteBuffer buffer = readResponse(rchannel);
			long code = nextNumber(buffer, rchannel);
			if (code != 15) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, rchannel, false);
			if (str.equals("true")) {
				Object obj = nextObject(buffer, rchannel);
				String version = nextString(buffer, rchannel, false);
				return new VersionedValue(obj, version);
			} else if (str.equals("false")) {
				/** falseの場合は第三列が文字列を返すときはエラーメッセージを例外としてスローし、空の時は値無しとしてnullを返す。 */
				String msg = nextString(buffer, rchannel, false);
				if (msg != null && !msg.isEmpty()) {
					throw new IllegalStateException(msg);
				}
				return null;
			} else {
				throw new IllegalStateException();
			}
		} finally {
			socketManager.recycle(socket);
		}
	}

	@Override
	public boolean setObjectValueVersionCheck(String key, Object value, String version, String[] tags, long age) throws IOException, OperationFailedException {
		Socket socket = socketManager.aquire();
		try	{
			WritableByteChannel wchannel = Channels.newChannel(socket.getOutputStream());
			ReadableByteChannel rchannel = Channels.newChannel(socket.getInputStream());
			createBuffer(16);
			appendString(wchannel, key, base64Key);
			appendStringList(wchannel, tags, true);
			appendNumber(wchannel, 0);
			if (value instanceof String) {
				appendString(wchannel, (String) value, true);				
			} else {
				appendSerializedObjectBase64(wchannel, value);
			}
//			appendNumber(channel, 0);
			appendString(wchannel, version, false);
			sendRequest(wchannel);
	
			ByteBuffer buffer = readResponse(rchannel);
			long code = nextNumber(buffer, rchannel);
			if (code != 16) {
				throw new IllegalStateException();
			}
			String str = nextString(buffer, rchannel, false);
			if (str.equals("true")) {
				return true;
			} else if (str.equals("false")){
				String msg = nextString(buffer, rchannel, false);				
				throw new OperationFailedException(msg);
			} else {
				throw new OperationFailedException();				
			}
		} finally {
			socketManager.recycle(socket);
		}		
	}

	@Override
	public Pair[] getPairsByTag(String tag) throws IOException, ClassNotFoundException {
		Socket socket = socketManager.aquire();
		try	{
			WritableByteChannel wchannel = Channels.newChannel(socket.getOutputStream());
			ReadableByteChannel rchannel = Channels.newChannel(socket.getInputStream());
			createBuffer(23);
			appendString(wchannel, tag, true);
			sendRequest(wchannel);

			ByteBuffer buffer = readResponse(rchannel);
			ArrayList<Pair> list = new ArrayList<Pair>(); 
			while (true) {
				String str = nextString(buffer, rchannel, false);
				if (str.equals("END")) {
					return list.toArray(new Pair[list.size()]);
				}
				if (str.equals("23")) {
					str = nextString(buffer, rchannel, false);
					if (str.equals("true")) {
						String key = nextString(buffer, rchannel, base64Key);
						Object obj = nextObject(buffer, rchannel);
						list.add(new Pair(key, obj));
					} else {
						throw new IllegalStateException();
					}
				} else {
					throw new IllegalStateException();
				}				
			}
		} finally {
			socketManager.recycle(socket);
		}
	}
}