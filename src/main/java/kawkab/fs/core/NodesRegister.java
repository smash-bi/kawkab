package kawkab.fs.core;

import java.util.HashMap;
import java.util.Map;

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.KawkabException;

public class NodesRegister {
	private static final Object initLock = new Object();
	
	private Map<Integer, NodeInfo> nodesMap;
	private static NodesRegister instance;
	
	private NodesRegister() {
		nodesMap = new HashMap<Integer, NodeInfo>();
		load();
	}
	
	public static NodesRegister instance() {
		if (instance == null) {
			synchronized(initLock) {
				if (instance == null)
					instance = new NodesRegister();
			}
		}
		
		return instance;
	}
	
	/**
	 * @param nodeID
	 * @return IP address of the node identified by the nodeID
	 * @throws KawkabException if the given nodeID does not exist.
	 */
	public String getIP(int nodeID) {
		NodeInfo info = nodesMap.get(nodeID);
		
		assert info != null : "info should not be null for node ID " + nodeID;
		
		return info.ip;
			
	}
	
	//Populates the map with the given 
	private void load() {
		//FIXME: Load from a configuration file.
		nodesMap.putAll(Configuration.instance().nodesMap);
	}
}
