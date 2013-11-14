package mitonize.datastore;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class SocketStreams implements Comparable<SocketStreams>{
	OutputStream os;
	InputStream is;
	Socket socket;
	long timestamp;
	
	public SocketStreams(Socket socket, OutputStream os, InputStream is) {
		this.socket = socket;
		this.os = os;
		this.is = is;
		this.timestamp = System.currentTimeMillis();
	}

	public OutputStream getOutputStream() {
		return os;
	}
	
	public InputStream getInputStream() {
		return is;
	}

	public Socket getSocket() {
		return socket;
	}

	@Override
	public int compareTo(SocketStreams other) {
		return (int) (timestamp - other.timestamp);
	}
}
