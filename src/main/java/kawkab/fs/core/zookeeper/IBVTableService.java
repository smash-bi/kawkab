package kawkab.fs.core.zookeeper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.KawkabException;

public class IBVTableService {
	private static final Object initLock = new Object();
	
	private Map<Long, Long> versions; //Map of <inodesBlock ID, version number> pairs 
	private static IBVTableService instance;
	private final static String pathPrefix = "/KawkabInodesBlocksVersions/";
	private ZKService zkclient;
	private ZKClusterConfig zkcluster;
	
	private IBVTableService() throws KawkabException {
		versions = new ConcurrentHashMap<Long, Long>();
		zkclient = ZKService.instance();
		zkcluster = Configuration.instance().zkMainCluster;
		zkclient.initService(zkcluster);
		createRootNode();
	}
	
	public static IBVTableService instance() throws KawkabException {
		if (instance == null)
			synchronized(initLock) {
				if (instance == null)
					instance = new IBVTableService();
			}
		return instance;
	}
	
	/**
	 * @param blockID InodesBlock number
	 * @return Current version of the given inodesBlock. The value can be staled.
	 */
	public long version(long blockNum) {
		Long ver = versions.get(blockNum);
		if (ver == null) //If the block does not exist in the map, it means the block has not been updated to another
			             // version. So the block should have default version of 1. This can be a staled value.
			return 1;
		
		return ver;
	}
	
	/*
	 * Updates the version of the given inodesBlock number...
	 */
	public void updateVersion(long blockID, long version) {
		assert blockID > 0;
		assert version > 1;
		
		//TODO: Verify that the new version is greater than the previous version.
		//FIXME: Should the version be constrainted to one larger than th
		versions.put(blockID, version);
	}
	
	/**
	 * Creates the root node under which all the files are created.
	 * @throws KawkabException
	 */
	private void createRootNode() throws KawkabException {
		String path = pathPrefix.substring(0, pathPrefix.length()-1);
		try {
			zkclient.addNode(zkcluster.id(), path, new byte[]{0});
		} catch (KeeperException e) {
			if (e.code() != Code.NODEEXISTS)
				throw new KawkabException(e);
		}
	}
}
