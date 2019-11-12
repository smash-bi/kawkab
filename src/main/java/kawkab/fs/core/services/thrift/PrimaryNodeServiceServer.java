package kawkab.fs.core.services.thrift;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.services.thrift.PrimaryNodeService.Processor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PrimaryNodeServiceServer {
	private TServer server;
	private boolean started = false;
	private ExecutorService executor;

	public PrimaryNodeServiceServer() {
		System.out.println("[PNS] Creating Primary Node Service");
		int minThreads = 4;
		int maxThreads = 8;

		try {
			TNonblockingServerTransport transport = new TNonblockingServerSocket(Configuration.instance().primaryNodeServicePort);
			//TServerTransport transport = new TServerSocket(Configuration.instance().primaryNodeServicePort);

			server = new THsHaServer(new THsHaServer.Args(transport)
					.transportFactory(new TFramedTransport.Factory())
					.protocolFactory(new TBinaryProtocol.Factory())
					.processor(new Processor<>(new PrimaryNodeServiceImpl()))
					.minWorkerThreads(minThreads)
					.maxWorkerThreads(maxThreads));

			/*TThreadPoolServer.Args args = new TThreadPoolServer.Args(transport);
			args.transportFactory(new TTransportFactory());
			args.protocolFactory(new TBinaryProtocol.Factory());

			args.processor(new Processor<PrimaryNodeService.Iface>(new PrimaryNodeServiceImpl()));
			args.executorService(new ThreadPoolExecutor(minThreads, maxThreads, 600,
					TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>()));*/

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
