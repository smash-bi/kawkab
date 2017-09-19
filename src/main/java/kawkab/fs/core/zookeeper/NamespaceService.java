package kawkab.fs.core.zookeeper;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException.NoNodeException;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileAlreadyExistsException;
import kawkab.fs.core.exceptions.FileNotExistException;

public class NamespaceService {
	private static NamespaceService instance; //There must be only one instance of 
	private CuratorFramework client;
	private final static String namePrefix = "KawkabFiles";
	
	private NamespaceService() throws IOException {
		String zkservers = Constants.zkServers; //CSV list of ip:port pairs of the servers
		
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(Constants.connectRetrySleepMs, Constants.connectMaxRetries);
		client = CuratorFrameworkFactory.builder()  //There should be only one instance of the framework for each cluster
				.namespace(namePrefix)
				.retryPolicy(retryPolicy)
				.connectString(zkservers)
				.build();
		client.start(); //Connect to ZooKeeper
		//createBaseDir();
	}
	
	public static NamespaceService instance() throws IOException {
		if (instance == null) {
			instance = new NamespaceService();
		}
		
		return instance;
	}
	
	public void addFile(String path, long inumber) throws FileAlreadyExistsException {
		path = fixPath(path);
		
		try {
			client.create().forPath(path, Commons.longToBytes(inumber));
		} catch(Exception e) {
			e.printStackTrace();
			throw new FileAlreadyExistsException(e.getMessage()); //TODO: Check for the type of exception and then handle it appropriately
		}
	}
	
	public long getInumber(String path) throws FileNotExistException, IOException {
		path = fixPath(path);
		
		long inumber = -1;
		try {
			byte[] res = client.getData().forPath(path);
			inumber = Commons.bytesToLong(res);
		} catch (NoNodeException e) {
			throw new FileNotExistException(e.getMessage()); //TODO: Check for the type of exception and then handle it appropriately
		} catch(Exception e) {
			throw new IOException(e); //TODO: Check for the type of exception and then handle it appropriately
		}
		
		return inumber;
	}
	
	private String fixPath(String path) {
		return "/"+path.replace('/', '~');
	}
}
