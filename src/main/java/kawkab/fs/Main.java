package kawkab.fs;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.AlreadyConfiguredException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.utils.GCMonitor;

import java.io.IOException;
import java.util.Properties;

public class Main {
	public static void main(String[] args) throws KawkabException, IOException, InterruptedException, AlreadyConfiguredException {
		initialize();
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					Filesystem.instance().shutdown();
				} catch (KawkabException | InterruptedException | IOException e) {
					e.printStackTrace();
				}
			}
		});

		Thread.currentThread().setName("Main thread");
		System.out.println("Running...");
		Filesystem.instance().waitUntilShutdown();
	}

	private static void initialize() throws IOException, InterruptedException, KawkabException {
		System.out.println("-------------------------------");
		System.out.println("- Initializing the filesystem -");
		System.out.println("-------------------------------");

		int nodeID = Configuration.getNodeID();
		String propsFile = System.getProperty("conf", Configuration.propsFileCluster);

		System.out.println("Node ID = " + nodeID);
		System.out.println("Loading properties from: " + propsFile);

		Properties props = Configuration.getProperties(propsFile);
		Filesystem.bootstrap(nodeID, props);
	}
}
