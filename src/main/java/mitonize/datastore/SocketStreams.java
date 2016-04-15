package mitonize.datastore;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class SocketStreams implements Comparable<SocketStreams> {
	OutputStream os;
	InputStream is;
	Socket socket;
	long timestamp;
	long expiryTime;

	public SocketStreams(Socket socket, OutputStream os, InputStream is,
		long socketTimeToLiveInMilli) {
		this.socket = socket;
		this.os = os;
		this.is = is;
		this.timestamp = System.currentTimeMillis();
		this.expiryTime = this.timestamp + socketTimeToLiveInMilli;
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

	public long getExpiryTime() {
		return expiryTime;
	}

	public void setExpiryTime(long expiryTime) {
		this.expiryTime = expiryTime;
	}

	public boolean isExpired() {
		return System.currentTimeMillis() > expiryTime;
	}

	@Override
	public int compareTo(SocketStreams other) {
		return (int) (timestamp - other.timestamp);
	}
}
