package mitonize.datastore;

public class OperationFailedException extends Exception {
	private static final long serialVersionUID = 1L;

	public OperationFailedException() {
		super();
	}

	public OperationFailedException(String msg) {
		super(msg);
	}

	public OperationFailedException(Throwable cause) {
		super(cause);
	}

	public OperationFailedException(String msg, Throwable e) {
		super(msg, e);
	}
}
