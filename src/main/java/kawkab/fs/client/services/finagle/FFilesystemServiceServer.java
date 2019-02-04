package kawkab.fs.client.services.finagle;

import com.twitter.finagle.ListeningServer;
import com.twitter.finagle.Thrift;
import com.twitter.finagle.thrift.Protocols.TFinagleBinaryProtocol;
import com.twitter.util.Await;
import com.twitter.util.TimeoutException;

import kawkab.fs.client.services.FilesystemService;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.Filesystem;


public class FFilesystemServiceServer {
	private ListeningServer server;
	private Thrift.Server tserver;
	private FilesystemService.Service service;
	
	public FFilesystemServiceServer(Filesystem fs) {
		int maxConcurrentRequests = 1000; //Maximum number of concurrent requests the server can handle
		int maxWaiters = 0; //Number of connections in the wait queue
		
		service = new FilesystemService.Service(new FFilesystemServiceImpl(fs));
		
		tserver = Thrift.server()
				.withProtocolFactory(new TFinagleBinaryProtocol.Factory());
				//.withAdmissionControl().concurrencyLimit(maxConcurrentRequests,maxWaiters);
	}
	
	public void startServer() {
		server = tserver.serve(":"+Constants.FS_SERVER_LISTEN_PORT, service);
	}
	
	public void stopServer() {
		try {
			Await.ready(server.close());
		} catch (TimeoutException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}
