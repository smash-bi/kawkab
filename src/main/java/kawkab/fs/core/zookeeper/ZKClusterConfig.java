package kawkab.fs.core.zookeeper;

public class ZKClusterConfig {
	private int id;
	private String servers;
	
	/**
	 * @param id Uniue ID of the cluster. Each cluster should have only one client.
	 * @param servers CSV list of ip:port pairs of the servers in the ZK ensemble
	 */
	
	public ZKClusterConfig(int id, String servers) {
		this.id = id;
		this.servers = servers;
	}
	
	public int id() {
		return id;
	}
	
	public String servers() {
		return servers;
	}
}
