package kawkab.fs.core.zookeeper;

public class ZKClusterConfig {
	private int id;
	private String servers;
	private int connectRetrySleepMs = 1000;
	private int connectMaxRetries = 5;
	
	/**
	 * @param id Uniue ID of the cluster. Each cluster should have only one client.
	 * @param servers CSV list of ip:port pairs of the servers in the ZK ensemble
	 */
	
	public ZKClusterConfig(int id, String servers, int connectRetrySleepMs, int connectMaxRetries) {
		this.id = id;
		this.servers = servers;
		this.connectRetrySleepMs = connectRetrySleepMs;
		this.connectMaxRetries = connectMaxRetries;
	}
	
	public int id() {
		return id;
	}
	
	public String servers() {
		return servers;
	}
	
	public int connectRetryIntervalMs() {
		return connectRetrySleepMs;
	}
	
	public int connectMaxRetries() {
		return connectMaxRetries;
	}
}
