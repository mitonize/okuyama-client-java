package mitonize.datastore;

import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public interface DumpFilterStreamFactory {
	FilterInputStream wrapInputStream(InputStream is);
	FilterOutputStream wrapOutputStream(OutputStream os);
}
