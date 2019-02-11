package kawkab.fs.client.services.thrift;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import kawkab.fs.client.services.FilesystemService;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Filesystem;

public class TFilesystemServiceServer {
	private TServer server;
	public TFilesystemServiceServer(Filesystem fs) {
		int minThreads = 500;
		int maxThreads = 2000;
		
		
		try {
			TServerTransport transport = new TServerSocket(Configuration.instance().fsServerListenPort);

			TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
			args.transportFactory(new TTransportFactory());
			args.protocolFactory(new TCompactProtocol.Factory());
	
			args.processor(new FilesystemService.Processor(new TFilesystemServiceImpl(fs)));
			args.executorService(new ThreadPoolExecutor(minThreads, maxThreads, 600,
					TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>()));
			
			server = new TThreadPoolServer(args);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void startServer() {
		server.serve();
	}
	
	public void stopServer() {
		server.stop();
	}
}
