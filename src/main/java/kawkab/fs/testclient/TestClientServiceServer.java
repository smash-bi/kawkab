package kawkab.fs.testclient;

import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.testclient.thrift.TestClientService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestClientServiceServer {
	private TServer server;
	private boolean started = false;
	private ExecutorService executor;
	private final int maxBufferLen = 10 * 1024 * 1024;

	public TestClientServiceServer(int numClients, int svrPort, int masterID) throws KawkabException {
		int numWorkers = numClients;
		int ioThreads = 10;

		TestClientService.Iface handler = new TestClientServiceImpl(numClients, masterID);

		//server = hsHaServer(svrPort, handler, numWorkers, numWorkers*2);
		//server = threadedSelectorServer(svrPort, handler, numWorkers, ioThreads);
		server = threadPoolServer(svrPort, handler, numWorkers, (int)(numWorkers*1.2));
	}

	private TServer threadedSelectorServer(int port, TestClientService.Iface handler, int workerThreads, int ioThreads) throws KawkabException {
		System.out.printf("[TCSS] ThreadedSelectorServer: workerThreads=%d, ioThreads-%d\n", workerThreads, ioThreads);
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(port);

			// Uses Java's ThreadPool to create concurrent worker threads
			return new TThreadedSelectorServer(new TThreadedSelectorServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory(maxBufferLen, maxBufferLen))
					.protocolFactory(new TBinaryProtocol.Factory())
					.processor(new TestClientService.Processor<>(handler))

					.selectorThreads(ioThreads)
					.workerThreads(workerThreads));
		} catch(Exception e) {
			e.printStackTrace();
			throw new KawkabException(e);
		}
	}

	private TServer hsHaServer(int port, TestClientService.Iface handler, int minThreads, int maxThreads) throws KawkabException {
		System.out.printf("[TCSS] HsHaServer: minThreads=%d, maxThreads=%d\n", minThreads, maxThreads);
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(port);

			// Uses Java's ThreadPool to create concurrent worker threads
			return new THsHaServer(new THsHaServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory(maxBufferLen, maxBufferLen))
					.protocolFactory(new TBinaryProtocol.Factory())
					.processor(new TestClientService.Processor<>(handler))
					.minWorkerThreads(minThreads)
					.maxWorkerThreads(maxThreads));
		} catch(Exception e) {
			e.printStackTrace();
			throw new KawkabException(e);
		}
	}

	private TServer threadPoolServer(int port, TestClientService.Iface handler, int minThreads, int maxThreads) throws KawkabException {
		System.out.printf("[TCSS] TThreadPoolServer: minThreads=%d, maxThreads=%d\n", minThreads, maxThreads);
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(port);

			// Uses Java's ThreadPool to create concurrent worker threads
			return new THsHaServer(new THsHaServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory(maxBufferLen, maxBufferLen))
					.protocolFactory(new TBinaryProtocol.Factory())
					.processor(new TestClientService.Processor<>(handler))
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

		System.out.println("[TCSS] Staring TestClientServiceServer");

		executor = Executors.newSingleThreadExecutor();
		executor.execute(() -> server.serve());
	}

	public synchronized void stopServer() {
		if (!started)
			return;

		if (server != null) {
			System.out.println("[TCSS] Stopping TestClientServiceServer");
			server.stop();
		}

		executor.shutdown();
		started = false;
	}
}
