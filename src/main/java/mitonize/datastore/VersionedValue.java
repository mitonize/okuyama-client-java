package mitonize.datastore;

public class VersionedValue {
	private Object value;
	private String version;

	public VersionedValue(Object value, String version) {
		this.value = value;
		this.version = version;
	}

	public Object getValue() {
		return value;
	}

	public String getVersion() {
		return version;
	}
	
}
