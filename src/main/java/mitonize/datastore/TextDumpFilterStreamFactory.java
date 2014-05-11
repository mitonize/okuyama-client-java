package mitonize.datastore;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;

public class TextDumpFilterStreamFactory implements DumpFilterStreamFactory {
	Charset cs;
	
	public TextDumpFilterStreamFactory() {
		this(Charset.forName("UTF-8"));
	}

	public TextDumpFilterStreamFactory(Charset cs) {
		this.cs = cs;
	}

	@Override
	public FilterInputStream wrapInputStream(InputStream is) {
		return new TextDumpFilterInputStream(is, cs);
	}

	@Override
	public FilterOutputStream wrapOutputStream(OutputStream os) {
		return new TextDumpFilterOutputStream(os, cs);
	}

}

class TextDumpFilterInputStream extends FilterInputStream {
	Charset cs;
	CharsetDecoder decoder;
	CharBuffer buffer = CharBuffer.allocate(256);
	private int charsInLine;

	protected TextDumpFilterInputStream(InputStream is, Charset cs) {
		super(is);
		this.cs = cs;
		decoder = cs.newDecoder();
	}

	@Override
	public int read() throws IOException {
		int b = super.read();
		++charsInLine;
		if (charsInLine < 256) {
			decoder.decode(ByteBuffer.wrap(new byte[]{(byte) b}), buffer, false);
		}
		boolean newline = false;
		if (b == '\n') {
			charsInLine = 0;
			newline = true;
			System.out.print(" INTPUT: ");
		}
		if (newline || !buffer.hasRemaining()) {
			buffer.flip();
			System.out.print(buffer.toString());
			buffer.clear();
		}
		return b;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int read = super.read(b, off, len);

		for (int i=off; i < off + read; ++i) {
			int c = b[i];
			++charsInLine;
			if (charsInLine < 256) {
				decoder.decode(ByteBuffer.wrap(new byte[]{(byte) c}), buffer, false);
			}
			boolean newline = false;
			if (c == '\n') {
				charsInLine = 0;
				newline = true;
				System.out.print(" INPUT: ");
			}
			if (newline || !buffer.hasRemaining()) {
				buffer.flip();
				System.out.print(buffer.toString());
				buffer.clear();
			}
		}
		return read;
	}

}

class TextDumpFilterOutputStream extends FilterOutputStream {
	Charset cs;
	CharsetDecoder decoder;
	CharBuffer buffer = CharBuffer.allocate(256);
	private int charsInLine ;

	protected TextDumpFilterOutputStream(OutputStream os, Charset cs) {
		super(os);
		this.cs = cs;
		decoder = cs.newDecoder();
	}

	@Override
	public void write(int b) throws IOException {
		super.write(b);
		++charsInLine;
		if (charsInLine < 256) {
			decoder.decode(ByteBuffer.wrap(new byte[]{(byte) b}), buffer, false);
		}
		boolean newline = false;
		if (b == '\n') {
			charsInLine = 0;
			newline = true;
			System.out.print("OUTPUT: ");
		}
		if (newline || !buffer.hasRemaining()) {
			buffer.flip();
			System.out.print(buffer.toString());
			buffer.clear();
		}
	}
}
