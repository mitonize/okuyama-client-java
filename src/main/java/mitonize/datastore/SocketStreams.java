package mitonize.datastore;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class SocketStreams {
	OutputStream os;
	InputStream is;
	Socket socket;
	
	public SocketStreams(Socket socket, OutputStream os, InputStream is) {
		this.socket = socket;
		this.os = os;
		this.is = is;
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
}
