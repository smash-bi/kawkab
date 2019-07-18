package kawkab.fs.core;

import kawkab.fs.core.timerqueue.DeferredWorkReceiver;
import kawkab.fs.core.timerqueue.TimerQueue;
import kawkab.fs.core.timerqueue.TimerQueueItem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;


/**
 * This class track the opened files in the underlying storage. The purpose of this class is (1) to avoid opening a luarge
 * number of files at the same time, and (2) to avoid frequint opening and closing of files and file channels for the
 * same blocks.
 *
 * The first objective is achieved by opening the file once for the all the blocks that reside in the same file.
 * However, each block gets its own FileChannel. In this way, for N blocks in the same file, we open the file once, and
 * create N FileChannels.
 *
 * In addition to preventing redundantly opening the same file multiple times, the opened files are closed after a
 * predefined timeout. In this way, we keep closing the opened files, even though the blocks can still be in the cache.
 * This is further explained below.
 *
 * The second objective is achieved by adding a small delay in closing the file. The usage of this class requires that
 * the user open a the required FileChannel for the block using the openFileChannel function and then immediately
 * close the FileChannel by calling the closeFileChannel function after reading/writing from/to the FileChannel.
 * When the user returns the FileChannel, this class doesn't close the channel immediately. Instead, it adds the
 * channel in a TimerQueue to be closed after a predefined timeout. If the user opens the same FileChannel before
 * the timeout, this channel is removed from the TimerQueue and the same chanel is returned to the user. This prevents
 * frequently opening and closing the same FileChannel, and subsequently, the same file in the undelrying filesystem.
 * After the timeout for a FileChannel, the channel is closed, and if associated file has no opened channels,
 * the file is closed as well5
 *
 * Note 1: This class is supposed to be used by either a single thread or the threads use unique BlockIDs for the channels.
 * The reason is that the openFileChannel function looks into a map where the key is blockID. So if two different
 * threads open the FileChannel for the same block, this class gives the same channel to both threads. However, the
 * releaseChannel will erroneously release the channel for both of the threads.
 *
 * Note 2: The caller should not close the FileChannel by itself. Instead, the caller should return the FileChannel
 * using the returnFileChannel() function. This class will close the FileChannel after a timeout if the caller doesn't
 * borrow the same channel before the timeout.
 *
 * Note 3: The caller must handle the exceptions properly and must call returnFileChannel even if there is an exception.
 *
 * Note 4: The caller must not save the FileChannel object. So the best would be to use the FileChannel object
 * as a local variable.
 * */

public class FileChannels implements DeferredWorkReceiver<FileChannels.FileChannelWrap> {
	private final Map<String, TimerQueueItem<FileChannelWrap>> timerItemsMap;
	
	private static final int timeoutMillis = 5000; //5 seconds selected arbitrarily
	private final TimerQueue tq;
	private final Clock clock = Clock.instance();
	private long hitCount;
	private long missCount;
	private final String name; //For debugging and tracing
	
	public FileChannels(String name) {
		this.name = name;
		tq = new TimerQueue(name+"-TimerQ");
		timerItemsMap = new HashMap<>();
	}
	
	/**
	 * Opens a FileChannel to read/write from the file in the underlying filesystem.
	 * @param blockID
	 * @return
	 * @throws FileNotFoundException
	 */
	public synchronized FileChannel acquireChannel(BlockID blockID) throws IOException {
		String filePath = blockID.localPath();
		TimerQueueItem<FileChannelWrap> itemWrap = timerItemsMap.get(filePath);
		
		// If the channel is already opened and not in the process of closing, return the existing channel
		if (itemWrap != null && tq.tryDisable(itemWrap)) {
			hitCount++;
			return itemWrap.getItem().channel();
		}
		
		// At this point, either the channel does not exist, or the channel is in the process of closing. It is not
		// safe to return the existing channel. We have to create a new wrapper object and put/replace that in the
		// map.
		
		
		FileChannelWrap channelWrap = new FileChannelWrap(filePath);
		itemWrap = new TimerQueueItem<>(channelWrap, this);
		timerItemsMap.put(filePath, itemWrap);
		
		missCount++;
		
		return channelWrap.channel();
	}
	
	@Override
	public synchronized void deferredWork(FileChannelWrap wrap) {
		try {
			String filePath = wrap.filePath();
			TimerQueueItem<FileChannelWrap> item = timerItemsMap.get(filePath);

			// Here we should remove the saved wrapper from the timerItemsMap. However, we have to check if the
			// mapped FileChannelWrap is the same as the one that is passed in this function. If they are not the same,
			// it means that another thread has created a new wrapper after the wrap was expired. We must not remove
			// the new item.
			
			if (item.getItem() == wrap) { // If the saved wrapper and the warp are the same, it is safe to remove the wrap because the channel cannot be reused
				timerItemsMap.remove(filePath);
			}
			
			// Close the file channel and release the channel object
			item.getItem().closeChannel();
		} catch (IOException e) { // We cannot return the exception because it will just reach to the TimerQueue thread, which cannot help anyway
			e.printStackTrace();
		}
	}
	
	public synchronized void releaseFileChannel(BlockID id) {
		TimerQueueItem<FileChannelWrap> item = timerItemsMap.get(id.localPath());
		
		assert item != null : id + " is null";
		
		tq.enableAndAdd(item, timeoutMillis+clock.currentTime());
	}
	
	public void shutdown() {
		System.out.printf("["+name+"]File Channel: Hits: %d, Miss: %d, Total: %d, Hit ratio: %.2f\n",
				hitCount, missCount, hitCount+missCount, (hitCount*1.0/(hitCount+missCount)));
		
		synchronized (this) {
			if (timerItemsMap.size() > 0) {
				for (String key : timerItemsMap.keySet()) {
					System.out.println("\t\t\t[" + name + "] >> File not closed yet: " + key);
				}
			}
		}
		
		tq.waitUntilEmpty();
		tq.shutdown();
		
		assert timerItemsMap.size() == 0 : "TimeItemsMap size is not zero";
	}
	
	@Override
	public String toString() {
		return name;
	}
	
	public class FileChannelWrap {
		private FileChannel channel;
		private String filePath;
		
		private FileChannelWrap (String filePath) throws IOException {
			this.filePath = filePath;
			this.channel = FileChannel.open(new File(filePath).toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ);
		}
		
		private String filePath() { return filePath; }
		private FileChannel channel() { return channel; }
		private void closeChannel() throws IOException {
			channel.close();
			channel = null;
		}
		
		@Override
		public  String toString() {
			return filePath;
		}
	}
}
