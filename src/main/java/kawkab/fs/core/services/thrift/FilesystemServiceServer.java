package kawkab.fs.core.services.thrift;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.services.thrift.FilesystemService.Processor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.util.concurrent.*;

public class FilesystemServiceServer {
	private TServer server;
	private boolean started = false;
	private ExecutorService executor;

	public FilesystemServiceServer(Filesystem fs) {
		int minThreads = 2;
		int maxThreads = 4;


		try {
			TServerTransport transport = new TServerSocket(Configuration.instance().fsServerListenPort);

			TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
			args.transportFactory(new TTransportFactory());
			args.protocolFactory(new TCompactProtocol.Factory());

			args.processor(new Processor(new FilesystemServiceImpl(fs)));
			args.executorService(new ThreadPoolExecutor(minThreads, maxThreads, 600,
					TimeUnit.SECONDS, new LinkedBlockingQueue<>()));

			server = new TThreadPoolServer(args);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	public synchronized void startServer() {
		if (started)
			return;
		started = true;

		System.out.println("[PNS] Staring PrimaryNodeService server");

		executor = Executors.newSingleThreadExecutor();
		executor.execute(() -> server.serve());
	}

	public synchronized void stopServer() {
		if (!started)
			return;

		if (server != null) {
			System.out.println("[PNS] Stopping PrimaryNodeService server");
			server.stop();
		}

		executor.shutdown();
		started = false;
	}
}
