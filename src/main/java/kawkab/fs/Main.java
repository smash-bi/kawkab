package kawkab.fs;

import java.io.IOException;

import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;

public class Main {
	public static void main(String[] args) throws KawkabException, IOException, InterruptedException {
		Filesystem fs = Filesystem.instance();
		fs.bootstrap();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					fs.shutdown();
				} catch (KawkabException | InterruptedException | IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread.currentThread().setName("Main thread");
		System.out.println("Running...");
		fs.waitUntilShutdown();
	}
}
