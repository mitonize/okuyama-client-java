package mitonize.datastore;

/**
 * キー・バリューの更新の際に、既に期待した状態になっていなかった場合に発生する例外。
 * 例えば、MemcachedのCAS命令や INCR命令が失敗した場合。
 */
public class KeyValueConsistencyException extends OperationFailedException {
	private static final long serialVersionUID = 1L;

	public KeyValueConsistencyException(String msg) {
		super(msg);
	}

}
