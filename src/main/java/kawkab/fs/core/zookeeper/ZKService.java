package kawkab.fs.core.zookeeper;

import java.util.HashMap;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.KawkabException;

public final class ZKService {
	private static final Object initLock = new Object();
	
	private Map<Integer, CuratorFramework> clients;
	private static ZKService instance;
	
	private ZKService() {
		clients = new HashMap<Integer, CuratorFramework>();
	}
	
	public static ZKService instance() {
		if (instance == null) {
			synchronized(initLock) {
				if (instance == null)
					instance = new ZKService();
			}
		}
		
		return instance;
	}
	
	public synchronized void initService(ZKClusterConfig cluster) {
		System.out.println("Initializing ZooKeeper client: " + cluster.id());
		
		if (clients.containsKey(cluster.id()))
			return;
		
		CuratorFramework client = null;
		
		ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(cluster.connectRetryIntervalMs(), cluster.connectMaxRetries());
		
		try {
			client = CuratorFrameworkFactory.builder()  //There should be only one instance of the framework for each cluster
					.retryPolicy(retryPolicy)
					.connectString(cluster.servers())
					.build();
			client.start(); //Connect to ZooKeeper
			client.blockUntilConnected();
			clients.put(cluster.id(), client);
		} catch(Exception e) {
			e.printStackTrace();
			if (client != null)
				client.close();
		}
	}
	
	public void addNode(int zkClusterID, String path, byte[] data) throws KeeperException, KawkabException { //TODO: Change KawkabException to proper type
		//System.out.println("[ZKService] Adding node " + path);
		
		CuratorFramework client = clients.get(zkClusterID);
		if (client == null)
			throw new KawkabException("ZK cluster "+zkClusterID+" is not initialized.");
		
		try {
			client.create().creatingParentsIfNeeded().forPath(path, data);
		} catch (KeeperException e) {
			throw e;
		} catch(Exception e) {
			throw new KawkabException(e); //TODO: Check for the type of exception and then handle it appropriately
		}
	}
	
	public byte[] getData(int zkClusterID, String path) throws KeeperException, KawkabException { //TODO: Change KawkabException to proper type
		//System.out.println("[ZKS] Get node " + path);
		
		CuratorFramework client = clients.get(zkClusterID);
		if (client == null)
			throw new KawkabException("ZK cluster "+zkClusterID+" is not initialized.");
		
		byte[] res = null;
		try {
			res = client.getData().forPath(path);
		} catch (KeeperException e) {
			throw e;
		} catch(Exception e) {
			throw new KawkabException(e); //TODO: Check for the type of exception and then handle it appropriately
		}
		
		return res;
	}
	
	public void shutdown() {
		System.out.println("Closed ZooKeeper clients");
		for (CuratorFramework client : clients.values()) {
			client.close();
		}
		
		System.out.println("Closed ZooKeeper clients");
	}
}
