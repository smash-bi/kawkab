package kawkab.fs.core.zookeeper;

import java.util.Base64;
import java.util.Base64.Encoder;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Constants;
import kawkab.fs.core.exceptions.FileAlreadyExistsException;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public class NamespaceService {
	private static NamespaceService instance; //There must be only one instance of 
	private final static String pathPrefix = "/KawkabFiles/";
	private ZKService zkClient;
	private ZKClusterConfig zkcluster;
	private Encoder encoder;
	
	private NamespaceService() throws KawkabException {
		zkClient = ZKService.instance();
		zkcluster = Constants.zkMainCluster;
		zkClient.initService(zkcluster);
		encoder = Base64.getUrlEncoder();
		createRootNode();
	}
	
	public static NamespaceService instance() throws KawkabException {
		if (instance == null)
			instance = new NamespaceService();
		return instance;
	}
	
	/**
	 * 
	 * 
	 * @param filename Name of the file
	 * @param inumber Inumber of the file
	 * @throws FileAlreadyExistsException if the file already exists in the namespace
	 * @throws KawkabException Any other exception
	 */
	public void addFile(String filename, long inumber) throws FileAlreadyExistsException, KawkabException {
		String path = fixPath(filename);
		
		try {
			zkClient.addNode(zkcluster.id(), path, Commons.longToBytes(inumber));
		} catch (KeeperException e) {
			if (e.code() == Code.NODEEXISTS) {
				throw new FileAlreadyExistsException(e.getMessage());
			}
		}
	}
	
	/**
	 * @param filename Name of the file
	 * @return The inumber associated with the given file
	 * @throws FileNotExistException If the file does not exist in the namespace
	 * @throws KawkabException Any other exception
	 */
	public long getInumber(String filename) throws FileNotExistException, KawkabException {
		String path = fixPath(filename);
		
		long inumber = -1;
		try {
			byte[] res = zkClient.getData(zkcluster.id(), path);
			inumber = Commons.bytesToLong(res);
		} catch (KeeperException e) {
			if (e.code() == Code.NONODE)
				throw new FileNotExistException(e.getMessage());
		}
		
		return inumber;
	}
	
	/**
	 * Converts the given path in a Base64 encoded string and prepends the pathPrefix
	 * @param path
	 * @return Absolute ZK path
	 */
	private String fixPath(String path) {
		return pathPrefix + encoder.encodeToString(path.getBytes());
	}
	
	/**
	 * Creates the root node under which all the files are created.
	 * @throws KawkabException
	 */
	private void createRootNode() throws KawkabException {
		String path = pathPrefix.substring(0, pathPrefix.length()-1);
		try {
			zkClient.addNode(zkcluster.id(), path, new byte[]{0});
		} catch (KeeperException e) {
			if (e.code() != Code.NODEEXISTS)
				throw new KawkabException(e.getMessage());
		}
	}
}
