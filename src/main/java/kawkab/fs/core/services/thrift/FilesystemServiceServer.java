package kawkab.fs.core.services.thrift;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.Filesystem;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.core.services.thrift.FilesystemService.Processor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * See the following for the brief explanation of different Thrift servers:
 * https://livebook.manning.com/book/programmers-guide-to-apache-thrift/chapter-10/
 */
public class FilesystemServiceServer {
	private TServer server;
	private boolean started = false;
	private ExecutorService executor;

	public FilesystemServiceServer(Filesystem fs) throws KawkabException {
		int minThreads = 8;
		int maxThreads = 1000;
		int ioThreads = 8;

		FilesystemService.Iface handler = new FilesystemServiceImpl(fs);

		//server = hsHaServer(handler, minThreads, maxThreads);
		server = threadedSelectorServer(handler, minThreads, ioThreads);
		//server = threadPoolServer(handler, minThreads, maxThreads);
	}

	private TServer threadPoolServer(FilesystemService.Iface handler, int minThreads, int maxThreads) throws KawkabException {
		System.out.printf("[FSI] TThreadPoolServer: minThreads=%d, maxThreads=%d\n", minThreads, maxThreads);

		Configuration conf = Configuration.instance();
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(conf.fsServerListenPort);

			// Uses Java's ThreadPool to create concurrent worker threads
			return new THsHaServer(new THsHaServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory())
					.protocolFactory(new TBinaryProtocol.Factory())
					.processor(new Processor<>(handler))
					.minWorkerThreads(minThreads)
					.maxWorkerThreads(maxThreads));
		} catch(Exception e) {
			e.printStackTrace();
			throw new KawkabException(e);
		}
	}

	private TServer threadedSelectorServer(FilesystemService.Iface handler, int workerThreads, int ioThreads) throws KawkabException {
		System.out.printf("[FSI] ThreadedSelectorServer: workerThreads=%d, ioThreads-%d\n", workerThreads, ioThreads);
		Configuration conf = Configuration.instance();
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(conf.fsServerListenPort);

			// Uses Java's ThreadPool to create concurrent worker threads
			return new TThreadedSelectorServer(new TThreadedSelectorServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory())
					.protocolFactory(new TBinaryProtocol.Factory())
					.processor(new Processor<>(handler))

					.selectorThreads(ioThreads)
					.workerThreads(workerThreads));
		} catch(Exception e) {
			e.printStackTrace();
			throw new KawkabException(e);
		}
	}

	private TServer hsHaServer(FilesystemService.Iface handler, int minThreads, int maxThreads) throws KawkabException {
		System.out.printf("[FSI] HsHaServer: minThreads=%d, maxThreads=%d\n", minThreads, maxThreads);
		Configuration conf = Configuration.instance();
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(conf.fsServerListenPort);

			// Uses Java's ThreadPool to create concurrent worker threads
			return new THsHaServer(new THsHaServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory())
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

		System.out.println("[FSS] Staring FilesystemServiceServer");

		executor = Executors.newSingleThreadExecutor();
		executor.execute(() -> server.serve());
	}

	public synchronized void stopServer() {
		if (!started)
			return;

		if (server != null) {
			System.out.println("[FSS] Stopping FilesystemServiceServer");
			server.stop();
		}

		executor.shutdown();
		started = false;
	}
}
