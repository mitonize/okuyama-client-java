package mitonize.datastore.okuyama;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mitonize.datastore.Base64;
import mitonize.datastore.CompressionStrategy;
import mitonize.datastore.Compressor;
import mitonize.datastore.KeyValueConsistencyException;
import mitonize.datastore.OperationFailedException;
import mitonize.datastore.Pair;
import mitonize.datastore.SocketManager;
import mitonize.datastore.SocketStreams;
import mitonize.datastore.VersionedValue;

public class OkuyamaClientImpl2 implements OkuyamaClient {
	private Logger logger = LoggerFactory.getLogger(OkuyamaClientImpl2.class);

	private static final char VALUE_SEPARATOR = ',';
	private static final int BLOCK_SIZE = 8192;

	SocketManager socketManager;
	Charset cs;
	boolean base64Key = true;
	boolean serializeString = false;
	ByteBuffer buffer;

	private CompressionStrategy compressionStrategy;

	/**
	 * OkuyamaClient インスタンスを生成する。
	 *
	 * @param socketManager ソケットマネージャ
	 * @param base64Key キーをBase64エンコードする。
	 * @param serializeString 値に文字列を保管するときにシリアライズするなら true。 falseなら文字列をUTF-8でBase64エンコードする。
	 * @param compressionStrategy 圧縮戦略
	 */
	protected OkuyamaClientImpl2(SocketManager socketManager, boolean base64Key, boolean serializeString, CompressionStrategy compressionStrategy) {
		this.cs = Charset.forName("UTF-8");
		this.buffer = ByteBuffer.allocate(BLOCK_SIZE);
		this.socketManager = socketManager;
		this.base64Key = base64Key;
		this.serializeString = serializeString;
		setCompressionStrategy(compressionStrategy);
	}

	/**
	 * キー文字列に不正な文字が含まれないかをチェックする
	 * @param key キー文字列
	 */
	void validateKey(String key) {
		for (int i=key.length() - 1; i >= 0; --i) {
			if (Character.isISOControl(key.codePointAt(i))) {
				throw new IllegalArgumentException();
			};
		}
	}

	/**
	 * Nullを表すバイト列かどうかを確認する
	 * @param b バイト列
	 * @return Nullを表すバイト列ならtrue
	 */
	boolean isNullString(ByteBuffer b) {
		b.mark();
		if (b.remaining() >= 3 && b.get() == '(' && b.get() == 'B' && b.get() == ')') {
			b.reset();
			return true;
		}
		b.reset();
		return false;
	}

	/**
	 * 指定されたプロトコル番号を設定したリクエスト用のバッファを生成する。
	 * @param protocolNo プロトコル番号
	 * @return ByteBufferインスタンス
	 * @throws IOException
	 */
	void createBuffer(OutputStream os, int protocolNo) throws IOException {
		os.write(Integer.toString(protocolNo).getBytes());
	}

	/**
	 * プロトコル書式に合わせてセパレータをバッファに追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @throws IOException 通信エラーが発生した場合
	 */
	void appendSeparator(OutputStream os) throws IOException {
		os.write(VALUE_SEPARATOR);
	}
	/**
	 * プロトコル書式に合わせて数値フィールドをバッファに追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @param num 数値
	 * @throws IOException 通信エラーが発生した場合
	 */
	void appendNumber(OutputStream os, long num) throws IOException {
		byte[] b = Long.toString(num).getBytes();
		os.write(VALUE_SEPARATOR);
		os.write(b);
	}

	/**
	 * プロトコル書式に合わせて文字列フィールドをバッファに追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @param str 文字列
	 * @param base64 Base64エンコード有無
	 * @throws IOException 通信エラーが発生した場合
	 */
	void appendString(OutputStream os, String str, boolean base64) throws IOException {
		if (str == null) {
			str = "(B)";
			base64 = false;
		}
		ByteBuffer b = cs.encode(str);
		if (base64) {
			b = Base64.encodeBuffer(b);
		}
		os.write(VALUE_SEPARATOR);
		os.write(b.array(), 0, b.limit());
	}

	/**
	 * プロトコル書式に合わせてオブジェクトをバッファに追加する。
	 * シリアライズ後のバイト列をBase64エンコードした文字列として追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @param obj オブジェクト
	 * @throws IOException シリアライズできない場合
	 */
	void appendSerializedObjectBase64(OutputStream os, Object obj, String key) throws IOException {
		if (obj == null) {
			appendString(os, "(B)", false);
			return;
		}
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream stream = new ObjectOutputStream(baos);
		stream.writeObject(obj);
		stream.close();

		byte[] serialized = baos.toByteArray();
		ByteBuffer b;
		Compressor compressor = null;
		if (compressionStrategy != null) {
			compressor = compressionStrategy.getSuitableCompressor(key, serialized.length);
		}

		if (compressor != null) {
			b = compressor.compress(serialized);
		} else {
			b = ByteBuffer.wrap(serialized);
		}

		ByteBuffer buf = Base64.encodeBuffer(b);
		os.write(VALUE_SEPARATOR);
		os.write(buf.array(), buf.position(), buf.limit() - buf.position());
	}

	/**
	 * プロトコル書式に合わせて文字列リストをバッファに追加する。主にタグを指定する場合に用いる。
	 * リストが空の場合は "(B)" を追加する。
	 * @param buffer ByteBufferオブジェクト
	 * @param strs 文字列リスト
	 * @param base64 Base64エンコード有無
	 * @throws IOException 通信エラーが発生した場合
	 */
	void appendStringList(OutputStream os, String[] strs, boolean base64) throws IOException {
		if (strs == null) {
			os.write(VALUE_SEPARATOR);
			os.write("(B)".getBytes());
		} else {
			os.write(VALUE_SEPARATOR);
			for (int i=0; i < strs.length; ++i) {
				if (i != 0) {
					os.write(':');
				}
				byte[] b = strs[i].getBytes();
				if (base64) {
					ByteBuffer buf = Base64.encodeBuffer(ByteBuffer.wrap(b));
					os.write(buf.array(), 0, buf.limit());
				} else {
					os.write(b);
				}
			}
		}
	}

	/**
     * 終端が必要なプロトコル番号の場合に末尾にセパレータを付加する。
     * @param buffer ByteBufferオブジェクト
	 * @throws IOException 通信エラーが発生した場合
     */
    void terminate(OutputStream os) throws IOException {
        os.write(VALUE_SEPARATOR);
    }

	/**
	 * 作成したリクエストを Channel に送信する。
	 * @param channel ソケットチャネル
	 * @param request リクエストの ByteBufferオブジェクト
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 */
	void sendRequest(OutputStream os) throws IOException {
		os.write('\n');
		os.flush();
	}

	/**
	 * サーバからのレスポンスをバッファに読み込む。
	 * @param channel ソケットチャネル
	 * @return レスポンスを読み込んだバッファ
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 */
	void readResponse(InputStream is) throws IOException {
		buffer.clear();
		int read = is.read(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
		if (read < 0) {
			throw new IOException("No more data on stream");
		}
		buffer.position(buffer.position() + read);
		buffer.flip();
	}


	/**
	 * レスポンスのバッファから数値を読み取る。バッファが足りなくなったら指定したチャネルから追加読み取りする。
	 * @param buffer ByteBufferオブジェクト
	 * @param channel ソケットチャネル
	 * @return 読み取った数値
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 * @throws OperationFailedException
	 * @throws {@link OperationFailedException} 期待したフォーマットでない場合
	 */
	long nextNumber(InputStream is) throws IOException, OperationFailedException {
		long code = 0;
		while (true) {
			while (buffer.hasRemaining()) {
				byte ch = buffer.get();
				if ('0' <= ch && ch <= '9') {
					code = code * 10 + (ch - '0');
				} else if (ch == VALUE_SEPARATOR || ch == '\n') {
					return (int) code;
				} else {
					throw new OperationFailedException(String.format("Format error on expecting digit: %c", ch));
				}
			}
			buffer.clear();
			int read = is.read(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
			buffer.position(buffer.position() + read);
			if (read == 0) {
				throw new OperationFailedException("buffer underflow");
			}
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
	 * @throws {@link OperationFailedException} 期待したフォーマットでない場合
	 */
	String nextString(InputStream is, boolean base64Key) throws IOException, OperationFailedException {
		ByteBuffer strBuffer = ByteBuffer.allocate(BLOCK_SIZE);
		while (true) {
			try {
				while (buffer.hasRemaining()) {
					byte ch = buffer.get();
					if (ch == VALUE_SEPARATOR || ch == '\n') {
						strBuffer.flip();
						if (strBuffer.limit() == 0) {
							return "";
						}
						if (isNullString(strBuffer)) {
							return null;
						}
						if (base64Key) {
							ByteBuffer b = Base64.decodeBuffer(strBuffer);
							return cs.decode(b).toString();
						} else {
							return cs.decode(strBuffer).toString();
						}
					} else {
						strBuffer.put(ch);
					}
				}
			} catch (BufferOverflowException e) {
				// 足りなくなれば追加
				ByteBuffer newBytes = ByteBuffer.allocate(strBuffer.capacity() + BLOCK_SIZE);
				newBytes.put(strBuffer);
				strBuffer = newBytes;
			}
			buffer.clear();
			int read = is.read(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
			buffer.position(buffer.position() + read);
			if (read == 0) {
//				throw new OperationFailedException("buffer underflow");
			}
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
	 * @throws OperationFailedException
	 * @throws {@link OperationFailedException} 期待したフォーマットでない場合
	 */
	String[] nextStringList(InputStream is, boolean base64Key) throws IOException, OperationFailedException {
		ByteBuffer strBuffer = ByteBuffer.allocate(BLOCK_SIZE);
		ArrayList<String> list = new ArrayList<String>();
		while (true) {
			try {
				while (buffer.hasRemaining()) {
					byte ch = buffer.get();
					if (ch == ':') {
						strBuffer.flip();
						if (base64Key) {
							list.add(cs.decode(Base64.decodeBuffer(strBuffer)).toString());
						} else {
							list.add(cs.decode(strBuffer).toString());
						}
						strBuffer.clear();
					} else if (ch == VALUE_SEPARATOR || ch == '\n') {
						strBuffer.flip();
						if (base64Key) {
							list.add(cs.decode(Base64.decodeBuffer(strBuffer)).toString());
						} else {
							list.add(cs.decode(strBuffer).toString());
						}
						strBuffer.clear();
						return list.toArray(new String[list.size()]);
					} else {
						strBuffer.put(ch);
					}
				}
			} catch (BufferOverflowException e) {
				// 足りなくなれば追加
				ByteBuffer newBytes = ByteBuffer.allocate(strBuffer.capacity() + BLOCK_SIZE);
				newBytes.put(strBuffer);
				strBuffer = newBytes;
			}
			buffer.clear();
			int read = is.read(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
			buffer.position(buffer.position() + read);
			if (read == 0) {
				throw new OperationFailedException("buffer underflow");
			}
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
	 * @throws OperationFailedException
	 * @throws {@link OperationFailedException} 期待したフォーマットでない場合
	 */
	Object nextObject(InputStream is) throws IOException, ClassNotFoundException, OperationFailedException  {
		ByteBuffer bytes = ByteBuffer.allocate(BLOCK_SIZE);
		byte ch = 0;
		while (true) {
			try {
				while (buffer.hasRemaining()) {
					ch = buffer.get();
					if (ch == VALUE_SEPARATOR || ch == '\n') {
						bytes.flip();
						if (bytes.limit() == 0) {
							return "";
						}
						if (isNullString(bytes)) {
							return null;
						}
						ByteBuffer raw = Base64.decodeBuffer(bytes);
						return decodeObject(raw.array(), raw.position(), raw.limit() - raw.position());
					} else {
						bytes.put(ch);
					}
				}
				buffer.clear();
				int read = is.read(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
				buffer.position(buffer.position() + read);
				if (read == 0) {
					throw new OperationFailedException("buffer underflow");
				}
				buffer.flip();
			} catch (BufferOverflowException e) {
				// 足りなくなれば追加
				ByteBuffer newBytes = ByteBuffer.allocate(bytes.capacity() + BLOCK_SIZE);
				bytes.flip();
				newBytes.put(bytes);
				bytes = newBytes;
				bytes.put(ch);
			}
		}
	}

	/**
	 * バイト列からJavaオブジェクトを復元する。バイト列がシリアライズされた列であれば
	 * デシリアライズする。シリアライズされたバイト列でなければ文字列オブジェクトとして返す。
	 * @param b バイト配列
	 * @param length
	 * @param offset
	 * @return デコードしたオブジェクト
	 * @throws IOException 通信に何らかのエラーが発生した場合
	 * @throws ClassNotFoundException
	 * @throws OperationFailedException
	 */
	Object decodeObject(byte[] b, int offset, int length) throws IOException, ClassNotFoundException, OperationFailedException {
		if (b.length == 0 || length == 0 || b.length < offset + length) {
			return null;
		}

		/* 適用されているCompressorがあれば返す。圧縮されているが未知のCompressorであれば IllegalStateException を返す。 */
		try {
			Compressor compressor = Compressor.getAppliedCompressor(b, offset, length);
			if (compressor != null) {
				ByteBuffer decompressed = compressor.decompress(b, offset, length);
				b = decompressed.array();
				offset = decompressed.position();
				length = decompressed.limit() - decompressed.position();
			}
		} catch (IllegalStateException e) {
			throw new OperationFailedException("Unexpected compression state");
		}

		/*
		 * シリアル化されたバイト列はマジックコード 0xac 0xed で始まるという
		 * JavaSEの仕様(Object Serialization Stream Protocol)である。
		 * @see http://docs.oracle.com/javase/6/docs/platform/serialization/spec/protocol.html
		 */
		if (length >= 2 && b[offset] == (byte) 0xac && b[offset+1] == (byte) 0xed) { // Magic code of Object Serialization Stream Protocol
			ObjectInputStream os = new ObjectInputStream(new ByteArrayInputStream(b, offset, length));
			Object obj = os.readObject();
			return obj;
		} else {
			/** シリアル化されて以内バイト列は文字列として復元 **/
			return cs.decode(ByteBuffer.wrap(b, offset, length)).toString();
		}
	}




	@Override
	public String getMasterNodeVersion() throws IOException, OperationFailedException {
		try {
			return _getMasterNodeVersion();
		} catch (IOException e) {
			// 既に接続が切れていた、あるいは途中で接続が切れた場合は1回だけリトライする。
			// 相手方あるいは途中のネットワーク機器で切断された可能性もあるため。
			getLogger().debug("retry once cause:{}", e.getMessage());
			return _getMasterNodeVersion();
		}
	}

	String _getMasterNodeVersion() throws IOException,
			OperationFailedException {
		SocketStreams socket = null;
		boolean failed = true;
		try	{
			socket = socketManager.aquire();

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();
			createBuffer(os, 999);
			sendRequest(os);

			readResponse(is);
			long code = nextNumber(is);
			if (code != 999) {
				throw new OperationFailedException("Unexprected code:" + code);
			}
			String str = nextString(is, false);
			if (str.startsWith("VERSION ")) {
				failed = false;
				str = str.substring("VERSION okuyama-".length());
				return str;
			} else {
				throw new OperationFailedException();
			}
		} finally {
			if (failed) {
				socketManager.destroy(socket);
			}
			socketManager.recycle(socket);
		}
	}

	@Override
	public long initClient() throws IOException, OperationFailedException {
		try {
			return _initClient();
		} catch (IOException e) {
			// 既に接続が切れていた、あるいは途中で接続が切れた場合は1回だけリトライする。
			// 相手方あるいは途中のネットワーク機器で切断された可能性もあるため。
			getLogger().debug("retry once cause:{}", e.getMessage());
			return _initClient();
		}
	}

	long _initClient() throws IOException, OperationFailedException {
		SocketStreams socket = null;
		boolean failed = true;
		try	{
			socket = socketManager.aquire();

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();
			createBuffer(os, 0);
			sendRequest(os);

			readResponse(is);
			long code = nextNumber(is);
			if (code != 0) {
				throw new OperationFailedException("Unexprected code:" + code);
			}
			String str = nextString(is, false);
			if (str.equals("true")) {
				failed = false;
				return nextNumber(is);
			} else {
				throw new OperationFailedException();
			}
		} finally {
			if (failed) {
				socketManager.destroy(socket);
			}
			socketManager.recycle(socket);
		}
	}

	@Override
	public boolean setObjectValue(String key, Object value, String[] tags, long age) throws IOException, OperationFailedException {
//		long st = System.currentTimeMillis();
		try {
			return _setObjectValue(key, value, tags, age);
		} catch (IOException e) {
			// 既に接続が切れていた、あるいは途中で接続が切れた場合は1回だけリトライする。
			// 相手方あるいは途中のネットワーク機器で切断された可能性もあるため。
			getLogger().debug("retry once cause:{}", e.getMessage());
			return _setObjectValue(key, value, tags, age);
		} finally {
//			logger.debug("set:{}ms", System.currentTimeMillis() - st);
		}
	}

	boolean _setObjectValue(String key, Object value, String[] tags, long age) throws IOException, OperationFailedException {
		if (value == null) {
			throw new IllegalArgumentException("Okuyama does not allow to store null value.");
		}
		validateKey(key);
		SocketStreams socket = null;
		boolean failed = true;
		try	{
			socket = socketManager.aquire();

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();

			createBuffer(os, 1);
			appendString(os, key, base64Key);
			appendStringList(os, tags, true);
			appendNumber(os, 0);
			if (!serializeString && (value instanceof String)) {
				appendString(os, (String) value, true);
			} else {
				appendSerializedObjectBase64(os, value, key);
			}
			appendNumber(os, age);
			appendSeparator(os);
			sendRequest(os);

			readResponse(is);
			long code = nextNumber(is);
			if (code != 1) {
				throw new OperationFailedException("Unexprected code:" + code);
			}
			String str = nextString(is, false);
			if (str.equals("true")) {
				failed = false;
				return true;
			} else if (str.equals("false")){
				String msg = nextString(is, false);
				throw new OperationFailedException(msg);
			} else {
				String msg = nextString(is, false);
				throw new OperationFailedException(msg);
			}
		} finally {
			if (failed) {
				socketManager.destroy(socket);
			}
			socketManager.recycle(socket);
		}
	}

	@Override
	public Object getObjectValue(String key) throws IOException, OperationFailedException {
//		long st = System.currentTimeMillis();
		try {
			return _getObjectValue(key);
		} catch (IOException e) {
			// 既に接続が切れていた、あるいは途中で接続が切れた場合は1回だけリトライする。
			// 相手方あるいは途中のネットワーク機器で切断された可能性もあるため。
			getLogger().debug("retry once cause:{}", e.getMessage());
			return _getObjectValue(key);
		} finally {
//			logger.debug("get:{}ms", System.currentTimeMillis() - st);
		}
	}

	Object _getObjectValue(String key) throws IOException, OperationFailedException {
		validateKey(key);
		SocketStreams socket = null;
		boolean failed = true;
		try	{
			socket = socketManager.aquire();

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();

			createBuffer(os, 2);
			appendString(os, key, base64Key);
			sendRequest(os);

			readResponse(is);
			long code = nextNumber(is);
			if (code != 2) {
				throw new OperationFailedException("Unexprected code:" + code);
			}
			String str = nextString(is, false);
			if (str.equals("true")) {
				Object obj;
				try {
					obj = nextObject(is);
				} catch (ClassNotFoundException e) {
					// オブジェクトがデシリアライズできなかった場合は値として ClassNotFoundException インスタンスを設定
					obj = e;
				}
				failed = false;
				return obj;
			} else if (str.equals("false")) {
				/** falseの場合は第三列が文字列を返すときはエラーメッセージを例外としてスローし、空の時は値無しとしてnullを返す。 */
				String msg = nextString(is, false);
				if (msg != null && !msg.isEmpty()) {
					failed = false;
					throw new OperationFailedException(msg);
				}
				failed = false;
				return null;
			} else {
				String msg = nextString(is, false);
				throw new OperationFailedException(msg);
			}
		} finally {
			if (failed) {
				socketManager.destroy(socket);
			}
			socketManager.recycle(socket);
		}
	}

	@Override
	public Object removeObjectValue(String key) throws IOException, OperationFailedException {
		try {
			return _removeObjectValue(key);
		} catch (IOException e) {
			// 既に接続が切れていた、あるいは途中で接続が切れた場合は1回だけリトライする。
			// 相手方あるいは途中のネットワーク機器で切断された可能性もあるため。
			getLogger().debug("retry once cause:{}", e.getMessage());
			return _removeObjectValue(key);
		}
	}

	Object _removeObjectValue(String key) throws IOException, OperationFailedException {
		validateKey(key);
		SocketStreams socket = null;
		boolean failed = true;
		try	{
			socket = socketManager.aquire();

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();

			createBuffer(os, 5);
			appendString(os, key, base64Key);
			appendNumber(os, 0);
			sendRequest(os);

			readResponse(is);
			long code = nextNumber(is);
			if (code != 5) {
				throw new OperationFailedException("Unexprected code:" + code);
			}
			String str = nextString(is, false);
			if (str.equals("true")) {
				Object obj;
				try {
					obj = nextObject(is);
				} catch (ClassNotFoundException e) {
					// オブジェクトがデシリアライズできなかった場合は値として ClassNotFoundException インスタンスを設定
					obj = e;
				}
				failed = false;
				return obj;
			} else if (str.equals("false")) {
				/** falseの場合は第三列が文字列を返すときはエラーメッセージを例外としてスローし、空の時は値無しとしてnullを返す。 */
				String msg = nextString(is, false);
				if (msg != null && !msg.isEmpty()) {
					failed = true;
					throw new OperationFailedException(msg);
				}
				failed = false;
				return null;
			} else {
				String msg = nextString(is, false);
				throw new OperationFailedException(msg);
			}
		} finally {
			if (failed) {
				socketManager.destroy(socket);
			}
			socketManager.recycle(socket);
		}
	}

	@Override
	public boolean addObjectValue(String key, Object value, String[] tags,
			long age) throws IOException, OperationFailedException {
		try {
			return _addObjectValue(key, value, tags, age);
		} catch (IOException e) {
			// 既に接続が切れていた、あるいは途中で接続が切れた場合は1回だけリトライする。
			// 相手方あるいは途中のネットワーク機器で切断された可能性もあるため。
			getLogger().debug("retry once cause:{}", e.getMessage());
			return _addObjectValue(key, value, tags, age);
		}
	}

	boolean _addObjectValue(String key, Object value, String[] tags,
			long age) throws IOException, OperationFailedException {
		if (value == null) {
			throw new IllegalArgumentException("Okuyama does not allow to store null value.");
		}
		validateKey(key);
		SocketStreams socket = null;
		boolean failed = true;
		try	{
			socket = socketManager.aquire();

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();

			createBuffer(os, 6);
			appendString(os, key, base64Key);
			appendStringList(os, tags, true);
			appendNumber(os, 0);
			if (!serializeString && (value instanceof String)) {
				appendString(os, (String) value, true);
			} else {
				appendSerializedObjectBase64(os, value, key);
			}
			appendNumber(os, age);
			appendSeparator(os);
			sendRequest(os);

			readResponse(is);
			long code = nextNumber(is);
			if (code != 6) {
				throw new OperationFailedException("Unexprected code:" + code);
			}
			String str = nextString(is, false);
			if (str.equals("true")) {
				failed = false;
				return true;
			} else if (str.equals("false")){
				String msg = nextString(is, false);
				if (msg.startsWith("NG:Data has already")) {
					failed = false;
					return false;
				} else {
					getLogger().debug("addObjectValue failed. {}", msg);
					throw new OperationFailedException(msg);
				}
			} else {
				String msg = nextString(is, false);
				throw new OperationFailedException(msg);
			}
		} finally {
			if (failed) {
				socketManager.destroy(socket);
			}
			socketManager.recycle(socket);
		}
	}

	@Override
	public Object[] getMultiObjectValues(String... keys) throws IOException,
			OperationFailedException {
		try {
			return _getMultiObjectValues(keys);
		} catch (IOException e) {
			// 既に接続が切れていた、あるいは途中で接続が切れた場合は1回だけリトライする。
			// 相手方あるいは途中のネットワーク機器で切断された可能性もあるため。
			getLogger().debug("retry once cause:{}", e.getMessage());
			return _getMultiObjectValues(keys);
		}
	}

	Object[] _getMultiObjectValues(String... keys) throws IOException,
			OperationFailedException {
		for (String key: keys) {
			validateKey(key);
		}
		SocketStreams socket = null;
		boolean failed = true;
		try	{
			socket = socketManager.aquire();

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();

			createBuffer(os, 22);
			for (String key: keys) {
				appendString(os, key, base64Key);
			}
			sendRequest(os);

			readResponse(is);
			ArrayList<Object> list = new ArrayList<Object>();
			while (true) {
				String str = nextString(is, false);
				if (str.equals("END")) {
					failed = false;
					return list.toArray();
				}
				if (str.equals("22")) {
					str = nextString(is, false);
					if (str.equals("true")) {
						Object obj;
						try {
							obj = nextObject(is);
						} catch (ClassNotFoundException e) {
							// オブジェクトがデシリアライズできなかった場合は値として ClassNotFoundException インスタンスを設定
							obj = e;
						}
						list.add(obj);
					} else if (str.equals("false")){
						// 存在しないオブジェクトは読みとばす
						nextString(is, false);
					} else {
						String msg = nextString(is, false);
						throw new OperationFailedException(msg);
					}
				} else {
					String msg = nextString(is, false);
					throw new OperationFailedException(msg);
				}
			}
		} finally {
			if (failed) {
				socketManager.destroy(socket);
			}
			socketManager.recycle(socket);
		}
	}

	@Override
	public String[] getTagKeys(String tag, boolean withDeletedKeys) throws IOException, OperationFailedException {
		try {
			return _getTagKeys(tag, withDeletedKeys);
		} catch (IOException e) {
			// 既に接続が切れていた、あるいは途中で接続が切れた場合は1回だけリトライする。
			// 相手方あるいは途中のネットワーク機器で切断された可能性もあるため。
			getLogger().debug("retry once cause:{}", e.getMessage());
			return _getTagKeys(tag, withDeletedKeys);
		}
	}

	String[] _getTagKeys(String tag, boolean withDeletedKeys) throws IOException, OperationFailedException {
		SocketStreams socket = null;
		boolean failed = true;
		try	{
			socket = socketManager.aquire();

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();

			createBuffer(os, 3);
			appendString(os, tag, true);
			appendString(os, withDeletedKeys ? "true": "false", false);
			sendRequest(os);

			readResponse(is);
			long code = nextNumber(is);
			if (code != 4) {
				throw new OperationFailedException("Unexprected code:" + code);
			}
			String str = nextString(is, false);
			if (str.equals("true")) {
				String[] strs = nextStringList(is, base64Key);
				failed = false;
				return strs;
			} else if (str.equals("false")){
				String msg = nextString(is, false);
				failed = false;
				throw new OperationFailedException(msg);
			} else {
				String msg = nextString(is, false);
				throw new OperationFailedException(msg);
			}
		} finally {
			if (failed) {
				socketManager.destroy(socket);
			}
			socketManager.recycle(socket);
		}
	}

	@Override
	public VersionedValue getObjectValueVersionCheck(String key) throws IOException, OperationFailedException {
		try {
			return _getObjectValueVersionCheck(key);
		} catch (IOException e) {
			// 既に接続が切れていた、あるいは途中で接続が切れた場合は1回だけリトライする。
			// 相手方あるいは途中のネットワーク機器で切断された可能性もあるため。
			getLogger().debug("retry once cause:{}", e.getMessage());
			return _getObjectValueVersionCheck(key);
		}
	}

	VersionedValue _getObjectValueVersionCheck(String key) throws IOException, OperationFailedException {
		validateKey(key);
		SocketStreams socket = null;
		boolean failed = true;
		try	{
			socket = socketManager.aquire();

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();

			createBuffer(os, 15);
			appendString(os, key, base64Key);
			sendRequest(os);

			readResponse(is);
			long code = nextNumber(is);
			if (code != 15) {
				throw new OperationFailedException("Unexprected code:" + code);
			}
			String str = nextString(is, false);
			if (str.equals("true")) {
				Object obj;
				try {
					obj = nextObject(is);
				} catch (ClassNotFoundException e) {
					// オブジェクトがデシリアライズできなかった場合は値として ClassNotFoundException インスタンスを設定
					obj = e;
				}
				String version = nextString(is, false);
				failed = false;
				return new VersionedValue(obj, version);
			} else if (str.equals("false")) {
				/** falseの場合は第三列が文字列を返すときはエラーメッセージを例外としてスローし、空の時は値無しとしてnullを返す。 */
				String msg = nextString(is, false);
				if (msg != null && !msg.isEmpty()) {
					failed = false;
					throw new OperationFailedException(msg);
				}
				failed = false;
				return null;
			} else {
				String msg = nextString(is, false);
				throw new OperationFailedException(msg);
			}
		} finally {
			if (failed) {
				socketManager.destroy(socket);
			}
			socketManager.recycle(socket);
		}
	}

	@Override
	public boolean setObjectValueVersionCheck(String key, Object value, String version, String[] tags, long age) throws IOException, OperationFailedException {
		try {
			return _setObjectValueVersionCheck(key, value, version, tags, age);
		} catch (IOException e) {
			// 既に接続が切れていた、あるいは途中で接続が切れた場合は1回だけリトライする。
			// 相手方あるいは途中のネットワーク機器で切断された可能性もあるため。
			getLogger().debug("retry once cause:{}", e.getMessage());
			return _setObjectValueVersionCheck(key, value, version, tags, age);
		}
	}

	boolean _setObjectValueVersionCheck(String key, Object value, String version, String[] tags, long age) throws IOException, OperationFailedException {
		if (value == null) {
			throw new IllegalArgumentException("Okuyama does not allow to store null value.");
		}
		validateKey(key);
		SocketStreams socket = null;
		boolean failed = true;
		try	{
			socket = socketManager.aquire();

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();

			createBuffer(os, 16);
			appendString(os, key, base64Key);
			appendStringList(os, tags, true);
			appendNumber(os, 0);
			if (!serializeString && (value instanceof String)) {
				appendString(os, (String) value, true);
			} else {
				appendSerializedObjectBase64(os, value, key);
			}
//			appendNumber(channel, 0);
			appendString(os, version, false);
            appendNumber(os, age);
            terminate(os);
			sendRequest(os);

			readResponse(is);
			long code = nextNumber(is);
			if (code != 16) {
				throw new OperationFailedException("Unexprected code:" + code);
			}
			String str = nextString(is, false);
			if (str.equals("true")) {
				failed = false;
				return true;
			} else if (str.equals("false")){
				String msg = nextString(is, false);
				if (msg.equals("NG:Data has already been updated")) {
					failed = false;
					throw new KeyValueConsistencyException(msg);
				}
				failed = false;
				throw new OperationFailedException(msg);
			} else {
				String msg = nextString(is, false);
				throw new OperationFailedException(msg);
			}
		} finally {
			if (failed) {
				socketManager.destroy(socket);
			}
			socketManager.recycle(socket);
		}
	}

	@Override
	public Pair[] getPairsByTag(String tag) throws IOException, OperationFailedException {
		try {
			return _getPairsByTag(tag);
		} catch (IOException e) {
			// 既に接続が切れていた、あるいは途中で接続が切れた場合は1回だけリトライする。
			// 相手方あるいは途中のネットワーク機器で切断された可能性もあるため。
			getLogger().debug("retry once cause:{}", e.getMessage());
			return _getPairsByTag(tag);
		}
	}

	Pair[] _getPairsByTag(String tag) throws IOException, OperationFailedException {
		SocketStreams socket = null;
		boolean failed = true;
		try	{
			socket = socketManager.aquire();

			OutputStream os = socket.getOutputStream();
			InputStream is = socket.getInputStream();

			createBuffer(os, 23);
			appendString(os, tag, true);
			sendRequest(os);

			readResponse(is);
			ArrayList<Pair> list = new ArrayList<Pair>();
			while (true) {
				String str = nextString(is, false);
				if (str.equals("END")) {
					failed = false;
					return list.toArray(new Pair[list.size()]);
				}
				if (str.equals("23")) {
					str = nextString(is, false);
					if (str.equals("true")) {
						String key = nextString(is, base64Key);
						Object obj;
						try {
							obj = nextObject(is);
						} catch (ClassNotFoundException e) {
							// オブジェクトがデシリアライズできなかった場合は値として ClassNotFoundException インスタンスを設定
							obj = e;
						}
						list.add(new Pair(key, obj));
					} else if (str.equals("false")){
						// 存在しないオブジェクトは読みとばす
						nextString(is, false);
					} else {
						String msg = nextString(is, false);
						throw new OperationFailedException(msg);
					}
				} else {
					String msg = nextString(is, false);
					throw new OperationFailedException(msg);
				}
			}
		} finally {
			if (failed) {
				socketManager.destroy(socket);
			}
			socketManager.recycle(socket);
		}
	}

	public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
		this.compressionStrategy = compressionStrategy;
	}

	protected Logger getLogger() {
		return logger;
	}

	protected void setLogger(Logger logger) {
		this.logger = logger;
	}
}