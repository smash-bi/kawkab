package kawkab.fs.core.services.grpc;

import java.io.IOException;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import kawkab.fs.commons.Configuration;

public final class PrimaryNodeServiceServer {
	private final Server server;
	private boolean started = false;
	
	public PrimaryNodeServiceServer() throws IOException {
		server = ServerBuilder
				.forPort(Configuration.instance().primaryNodeServicePort)
				.addService(new PrimaryNodeService())
				.build();
		
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
		      @Override
		      public void run() {
		        // Use stderr here since the logger may has been reset by its JVM shutdown hook.
		        try {
					PrimaryNodeServiceServer.this.stopServer();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		      }
		});
	}
	
	public synchronized void startServer() throws IOException {
		if (started)
			return;
		
		server.start();
		started = true;
	}
	
	
	public synchronized void stopServer() throws InterruptedException {
		if (!started)
			return;
		
		if (server != null) {
			server.shutdown();
		}
		
		server.awaitTermination();
		
		started = false;
	}
}
