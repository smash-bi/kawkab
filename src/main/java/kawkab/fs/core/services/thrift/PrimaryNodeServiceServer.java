package kawkab.fs.core.services.thrift;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.thrift.PrimaryNodeService.Processor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PrimaryNodeServiceServer {
	private TServer server;
	private boolean started = false;
	private ExecutorService executor;
	private PrimaryNodeServiceImpl handler;

	public PrimaryNodeServiceServer() throws KawkabException {
		System.out.println("[PNS] Creating Primary Node Service");

		int workerThreads = 8;
		//int maxThreads = 1000;
		int ioThreads = 8;

		handler = new PrimaryNodeServiceImpl(workerThreads);

		//server = hsHaServer(handler, workerThreads, maxThreads);
		server = threadedSelectorServer(handler, workerThreads, ioThreads);
		//server = threadPoolServer(handler, workerThreads, maxThreads);
	}

	private TServer threadPoolServer(PrimaryNodeService.Iface handler, int minThreads, int maxThreads) throws KawkabException {
		System.out.printf("[PSS] TThreadPoolServer: minThreads=%d, maxThreads=%d\n", minThreads, maxThreads);
		Configuration conf = Configuration.instance();
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(conf.primaryNodeServicePort);

			// Uses Java's ThreadPool to create concurrent worker threads
			return new THsHaServer(new THsHaServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory(conf.maxBufferLen))
					.protocolFactory(new TBinaryProtocol.Factory())
					.processor(new Processor<>(handler))
					.minWorkerThreads(minThreads)
					.maxWorkerThreads(maxThreads));
		} catch(Exception e) {
			e.printStackTrace();
			throw new KawkabException(e);
		}
	}

	private TServer threadedSelectorServer(PrimaryNodeService.Iface handler, int workerThreads, int ioThreads) throws KawkabException {
		System.out.printf("[PSS] ThreadedSelectorServer: workerThreads=%d, ioThreads-%d\n", workerThreads, ioThreads);

		Configuration conf = Configuration.instance();
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(conf.primaryNodeServicePort);

			// Uses Java's ThreadPool to create concurrent worker threads
			return new TThreadedSelectorServer(new TThreadedSelectorServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory(conf.maxBufferLen))
					.protocolFactory(new TBinaryProtocol.Factory())
					.processor(new Processor<>(handler))

					.selectorThreads(ioThreads)
					.workerThreads(workerThreads));
		} catch(Exception e) {
			e.printStackTrace();
			throw new KawkabException(e);
		}
	}

	private TServer hsHaServer(PrimaryNodeService.Iface handler, int minThreads, int maxThreads) throws KawkabException {
		System.out.printf("[PSS] HsHaServer: minThreads=%d, maxThreads=%d\n", minThreads, maxThreads);

		Configuration conf = Configuration.instance();
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(conf.primaryNodeServicePort);

			// Uses Java's ThreadPool to create concurrent worker threads
			return new THsHaServer(new THsHaServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory(conf.maxBufferLen))
					.protocolFactory(new TBinaryProtocol.Factory())
					.processor(new Processor<>(handler))
					.minWorkerThreads(minThreads)
					.maxWorkerThreads(maxThreads));
		} catch(Exception e) {
			e.printStackTrace();
			throw new KawkabException(e);
		}
	}

	public synchronized void startServer() {
		if (started)
			return;
		started = true;

		System.out.println("[PSS] Staring PrimaryNodeService server");

		executor = Executors.newSingleThreadExecutor();
		executor.execute(() -> server.serve());
	}

	public synchronized void stopServer() {
		if (!started)
			return;

		if (server != null) {
			System.out.println("[PSS] Stopping PrimaryNodeService server");
			server.stop();
		}

		executor.shutdown();
		started = false;
	}

	public void printStats() {
		handler.printStats();
	}

	public void resetStats() {
		handler.resetStats();
	}
}
