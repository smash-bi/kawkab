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

	public FilesystemServiceServer(Filesystem fs, int port, int numIOThreads, int numWorkers) throws KawkabException {
		//int maxThreads = 1000;

		FilesystemService.Iface handler = new FilesystemServiceImpl(fs);

		//server = hsHaServer(handler, minThreads, maxThreads, port);
		server = threadedSelectorServer(handler, numWorkers, numIOThreads, port);
		//server = threadPoolServer(handler, minThreads, maxThreads, port);
	}

	private TServer threadPoolServer(FilesystemService.Iface handler, int minThreads, int maxThreads, int port) throws KawkabException {
		System.out.printf("[FSI] TThreadPoolServer: minThreads=%d, maxThreads=%d\n", minThreads, maxThreads);

		Configuration conf = Configuration.instance();
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(port);

			int bufLen = conf.maxBufferLen;

			// Uses Java's ThreadPool to create concurrent worker threads
			return new THsHaServer(new THsHaServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory(bufLen, bufLen))
					.protocolFactory(new TBinaryProtocol.Factory())
					.processor(new Processor<>(handler))
					.minWorkerThreads(minThreads)
					.maxWorkerThreads(maxThreads));
		} catch(Exception e) {
			e.printStackTrace();
			throw new KawkabException(e);
		}
	}

	private TServer threadedSelectorServer(FilesystemService.Iface handler, int workerThreads, int ioThreads, int port) throws KawkabException {
		System.out.printf("[FSI] ThreadedSelectorServer: workerThreads=%d, ioThreads-%d\n", workerThreads, ioThreads);
		Configuration conf = Configuration.instance();
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(port);

			// Uses Java's ThreadPool to create concurrent worker threads
			return new TThreadedSelectorServer(new TThreadedSelectorServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory(conf.maxBufferLen, conf.maxBufferLen))
					.protocolFactory(new TBinaryProtocol.Factory())
					.processor(new Processor<>(handler))
					.selectorThreads(ioThreads)
					.workerThreads(workerThreads));
		} catch(Exception e) {
			e.printStackTrace();
			throw new KawkabException(e);
		}
	}

	private TServer hsHaServer(FilesystemService.Iface handler, int minThreads, int maxThreads, int port) throws KawkabException {
		System.out.printf("[FSI] HsHaServer: minThreads=%d, maxThreads=%d\n", minThreads, maxThreads);
		Configuration conf = Configuration.instance();
		try {
			// For transmitting data to wire
			TNonblockingServerTransport transport = new TNonblockingServerSocket(port);

			// Uses Java's ThreadPool to create concurrent worker threads
			return new THsHaServer(new THsHaServer.Args(transport)
					.transportFactory(new TFastFramedTransport.Factory(conf.maxBufferLen, conf.maxBufferLen))
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
