package kawkab.fs.testclient;

public class Printer {
	public synchronized void print(String msg) {
		System.out.println(msg);
	}
}
