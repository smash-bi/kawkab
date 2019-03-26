package kawkab.fs;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.AlreadyConfiguredException;
import kawkab.fs.core.exceptions.KawkabException;

public class Main {
	public static void main(String[] args) throws KawkabException, IOException, InterruptedException, AlreadyConfiguredException {
		int nodeID = getNodeID();
		Properties props = getProperties();
		
		Filesystem fs = Filesystem.bootstrap(nodeID, props);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					fs.shutdown();
				} catch (KawkabException | InterruptedException e) {
					e.printStackTrace();
				}
			}
		});

		Thread.currentThread().setName("Main thread");
		System.out.println("Running...");
		fs.waitUntilShutdown();
	}
	
	private static Properties getProperties() throws IOException {
		String propsFile = "/config.properties";
		
		try (InputStream in = Thread.class.getResourceAsStream(propsFile)) {
			Properties props = new Properties();
			props.load(in);
			return props;
		}
	}
	
	private static int getNodeID() throws KawkabException {
		String nodeIDProp = "nodeID";
		
		if (System.getProperty(nodeIDProp) == null) {
			throw new KawkabException("System property nodeID is not defined.");
		}
		
		return Integer.parseInt(System.getProperty(nodeIDProp));
	}
}
