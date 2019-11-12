package kawkab.fs.testclient;

public class Printer {
	private final int cid;

	public Printer(int cid) {
		this.cid = cid;
	}

	public synchronized void print(String msg) {
		System.out.println("[C"+cid+"] " + msg);
	}
}
