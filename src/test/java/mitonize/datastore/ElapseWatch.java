package mitonize.datastore;

public class ElapseWatch {
	long startNano;
	
	public void start() {
		startNano = System.nanoTime();
	}

	public double lap() {
		double lap = (System.nanoTime() - startNano) / 1000000.0;
		startNano = System.nanoTime();
		return lap;
	}
	
}
