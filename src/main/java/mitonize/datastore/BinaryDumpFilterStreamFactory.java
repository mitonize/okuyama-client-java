package mitonize.datastore;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BinaryDumpFilterStreamFactory implements DumpFilterStreamFactory {

	public BinaryDumpFilterStreamFactory() {
	}

	@Override
	public FilterInputStream wrapInputStream(InputStream is) {
		return new DumpFilterInputStream(is);
	}

	@Override
	public FilterOutputStream wrapOutputStream(OutputStream os) {
		return new DumpFilterOutputStream(os);
	}

}

class DumpFilterInputStream extends FilterInputStream {
	private ByteBuffer buffer;

	protected DumpFilterInputStream(InputStream is) {
		super(is);
		buffer = ByteBuffer.allocate(32);
	}

	void dumpLine() {
		StringBuilder hexBuf = new StringBuilder();
		StringBuilder strBuf = new StringBuilder();

		buffer.flip();
		while (buffer.hasRemaining()) {
			byte c = buffer.get();
			hexBuf.append(String.format("%02x ", c));
			if (Character.isISOControl(c) || c < 0) {
				strBuf.append('.');
			} else {
				strBuf.append(new String(new byte[] {c}));
			}
		}
		buffer.clear();
		System.err.print("IN:  ");
		System.err.print(hexBuf.toString());
		System.err.print(" ");
		System.err.println(strBuf.toString());		
	}
	
	@Override
	public int read() throws IOException {
		int b = super.read();
		buffer.put((byte)b);

		if (buffer.position() == 16) {
			dumpLine();
		}
		return b;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int read = super.read(b, off, len);

		for (int i=off; i < off + read; ++i) {
			buffer.put((byte)b[i]);

			if (buffer.position() == 16) {
				dumpLine();
			}
		}
		return read;
	}

}

class DumpFilterOutputStream extends FilterOutputStream {
	ByteBuffer buffer;

	protected DumpFilterOutputStream(OutputStream os) {
		super(os);
		buffer = ByteBuffer.allocate(32);
	}
	
	void dumpLine() {
		StringBuilder hexBuf = new StringBuilder();
		StringBuilder strBuf = new StringBuilder();

		buffer.flip();
		while (buffer.hasRemaining()) {
			byte c = buffer.get();
			hexBuf.append(String.format("%02x ", c));
			if (Character.isISOControl(c) || c < 0) {
				strBuf.append('.');
			} else {
				strBuf.append(new String(new byte[] {c}));
			}
		}
		buffer.clear();
		System.err.print("OUT: ");
		System.err.print(hexBuf.toString());
		System.err.print(" ");
		System.err.println(strBuf.toString());		
	}

	@Override
	public void write(int b) throws IOException {
		super.write(b);
		buffer.put((byte)b);

		if (buffer.position() == 16) {
			dumpLine();
		}
	}
}
