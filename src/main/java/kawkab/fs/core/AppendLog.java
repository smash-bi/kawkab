package kawkab.fs.core;

import kawkab.fs.commons.Configuration;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;

public class AppendLog {
	private final String basePath = Configuration.instance().basePath + System.lineSeparator()+"chronicleQueues";
	private ChronicleQueue queue;
	
	public AppendLog() {
		queue = SingleChronicleQueueBuilder.single(basePath).build();
	}
}
