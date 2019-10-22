package kawkab.fs.core.index.poh;

import com.google.protobuf.ByteString;
import kawkab.fs.commons.Commons;
import kawkab.fs.core.Block;
import kawkab.fs.core.IndexNodeID;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.IndexBlockFullException;
import kawkab.fs.core.exceptions.KawkabException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The class is not thread safe for concurrent appends. However, the search and append can be concurrent.
 */
public class POHNode extends Block {
	// Persistent variables
	private int nodeNumber; //node number
	private int height;
	private POHEntry[] entries; // Number of index entries
	private POHChild[] children;

	private int entryIdx; //Number of entries added in the node.
	private int pointerIdx;

	private final IndexNodeID id;


	private long entryMinTS = Long.MAX_VALUE;
	private long nodeMaxTS;
	private long nodeMinTS = Long.MAX_VALUE;

	private ByteBuffer storeBuffer; // For writing to FileChannel in storeTo(FileChannel) function
	private AtomicInteger lastWriteIdx; // Next byte write position in the storeBuffer

	// The offset where the dirty bytes start, the bytes that are not persisted yet.
	// Initialized to -1 to indicate that the node header is not persisted yet.
	// dirtyOffset doesn't need to be thread-safe because only the localStore thread reads and updates its value.
	// The value is initially set when the block is loaded, at which point the block cannot be accessed by the localStore.
	private int dirtyIdx = -1;


	private int nodeSizeBytes; // nodeNumber, height, number of pointers, number of entries

	/**
	 * @param id Node ID
	 */
	public POHNode(IndexNodeID id) {
		super(id);
		this.id = id;
		this.nodeNumber = id.nodeNumber();
	}

	/**
	 * @param nodeNum The node number in the k-ary post-order heap
	 * @param entriesCount Number of index entries in the node, must be greater than zero so that we have at least one index entry
	 * @param childrenCount Number of pointers or children of each node, must be greater than one so that we have at least a binary tree
	 */
	void init(final int nodeNum, final int height, final int entriesCount, final int childrenCount, final int nodeSizeBytes) {
		assert entriesCount > 0;
		assert childrenCount > 1;
		assert nodeNumber == nodeNum;

		this.height = height;

		entries = new POHEntry[entriesCount];
		children = new POHChild[childrenCount];

		lastWriteIdx = new AtomicInteger(-1);
		storeBuffer = ByteBuffer.allocate(nodeSizeBytes); // Integer for numEntries

		this.nodeSizeBytes = nodeSizeBytes;
	}

	static int headerSizeBytes() {
		return Integer.BYTES + Integer.BYTES; //NodeNumber and height
	}

	public static int entrySizeBytes() {
		return POHEntry.sizeBytes();
	}

	static int childSizeBytes() {
		return POHChild.sizeBytes();
	}

	public IndexNodeID id() {
		return this.id;
	}

	/**
	 * Appends an entry in the node
	 *
	 * minTS is required to be greater or equal to the last maxTS
	 *
	 * segmentInFile must be greater than the last segmentInFile
	 *
	 * minTS must be smaller or equal to maxTS
	 *
	 * The caller must ensure that there are no concurrent appendEntry calls to the same POHNode
	 *
	 * @param minTS timestamp of the first record in the segment
	 * @param segmentInFile Byte offset in file
	 * @throws IllegalArgumentException if the preconditions are not met
	 * @throws IndexBlockFullException if the block is already full with the index entries
	 */
	void appendEntryMinTS(final long minTS, final long segmentInFile)
			throws IllegalArgumentException, IndexBlockFullException {

		assert pointerIdx == 0 || pointerIdx == children.length :
				"Either the pointers should not exist or they all be appended before the entries; pointerIdx="
						+pointerIdx+", limit="+ children.length;

		if (entryIdx == entries.length) {
			throw new IndexBlockFullException("No space left in the POHNode to add more index entries");
		}

		// Check the arguments
		if (entryIdx > 0) {
			POHEntry last = entries[entryIdx-1];

			if (minTS < last.maxTS())
				throw new IllegalArgumentException("Current minTS must be greater or equal to last entry's maxTS; minTS="
						+ minTS + ", last maxTS=" + last.maxTS());

			if (segmentInFile <= last.segmentInFile()) {
				throw new IllegalArgumentException("Current segmentInFile must be greater than the last segmentInFile; segmentInFile="
						+ segmentInFile + ", last segmentInFile=" + last.segmentInFile());
			}

			assert last.isMaxSet();
		}

		// Append the entry
		entries[entryIdx] = new POHEntry(minTS, segmentInFile);

		// Update the minTS and maxTS for this node. minTS will not change if this node is an internal node.
		if (entryIdx == 0) { // This will change only for leaf nodes
			if (height == 0) {
				nodeMinTS = minTS;
			}

			entryMinTS = minTS;
		}

		assert nodeMaxTS <= minTS;
		nodeMaxTS = minTS; // Using minTS as the max value until appendEntryMaxTS is called

		lastWriteIdx.incrementAndGet();

		entryIdx++;
		markLocalDirty();
	}

	/**
	 * This function must be called after the corresponding appendEntryMinTS() call.
	 *
	 * @param maxTS timestamp of the last record in the segment
	 * @param segmentInFile This must be equal to the segmentInFile that was provided in the last appendEntryMinTS().
	 */
	void appendEntryMaxTS(final long maxTS, final long segmentInFile) {
		assert entryIdx > 0;
		assert entryIdx <= entries.length;

		POHEntry entry = entries[entryIdx-1];

		assert entry.segmentInFile() == segmentInFile;

		entry.setMaxTS(maxTS);

		nodeMaxTS = maxTS; // We add the entries in sequence. Therefore, each entries maxTS is greater or equal than the last etnry's maxTS

		markLocalDirty();
	}

	void appendEntry(final long minTS, final long maxTS, final long segmentInFile) throws IndexBlockFullException {
		assert pointerIdx == 0 || pointerIdx == children.length :
				"Either the pointers should not exist or they all be appended before the entries; pointerIdx="
						+pointerIdx+", limit="+ children.length;

		assert minTS <= maxTS;

		if (entryIdx == entries.length) {
			throw new IndexBlockFullException("No space left in the POHNode to add more index entries");
		}

		// Check the arguments
		if (entryIdx > 0) {
			POHEntry last = entries[entryIdx-1];

			if (minTS < last.maxTS())
				throw new IllegalArgumentException("Current minTS must be greater or equal to last entry's maxTS; minTS="
						+ minTS + ", last maxTS=" + last.maxTS());

			if (segmentInFile <= last.segmentInFile()) {
				throw new IllegalArgumentException("Current segmentInFile must be greater than the last segmentInFile; segmentInFile="
						+ segmentInFile + ", last segmentInFile=" + last.segmentInFile());
			}
		}

		// Append the entry
		entries[entryIdx] = new POHEntry(minTS, maxTS, segmentInFile);

		// Update the minTS and maxTS for this node. minTS will not change if this node is an internal node.
		if (this.nodeMinTS > minTS) // This will change only for leaf nodes
			this.nodeMinTS = minTS;

		if (entryMinTS > minTS)
			entryMinTS = minTS;

		if (this.nodeMaxTS < maxTS) // Using minTS as the max value until appendEntryMaxTS is called
			this.nodeMaxTS = maxTS; // This will be updated when appendEntryMaxTS is called

		if (entryIdx == 0) { // if it is the first index entry
			if (height == 0) {
				nodeMinTS = minTS;
			}

			entryMinTS = minTS;
		}

		nodeMaxTS = minTS; // We add the entries in sequence. Therefore, each entries maxTS is greater or equal than the last etnry's maxTS

		lastWriteIdx.incrementAndGet();

		entryIdx++;
		markLocalDirty();
	}

	/**
	 * nodeNum must be smaller than this node's number
	 *
	 * minTS must be smaller or equal to maxTS
	 *
	 * maxTS of the last pointer must be equal or smaller than minTS
	 *
	 * @param child Child node
	 * @throws IndexBlockFullException if the node is already full with the pointers
	 */
	void appendChild(final POHNode child) throws IndexBlockFullException {
		assert entryIdx == 0; //The pointers must be added before the entries to make this node append-only
		assert height > 0;

		if (pointerIdx == children.length)
			throw new IndexBlockFullException();

		POHChild last = children[pointerIdx];

		if (child.nodeNumber() >= nodeNumber)
			throw new IllegalArgumentException("nodeNum of the child must be smaller than the current node, child nodeNum="
					+child.nodeNumber()+", current nodeNum="+ nodeNumber);

		if (last != null) {
			if (last.nodeNumber() >= child.nodeNumber())
				throw new IllegalArgumentException("nodeNum of the child must be larger than the last child, child nodeNum="
						+child.nodeNumber()+", last nodeNum="+ last.nodeNumber());

			if (child.minTS() >= last.maxTS())
				throw new IllegalArgumentException("Current minTS must be greater or equal to the last pointer's maxTS; minTS="
						+ child.nodeMinTS + ", last maxTS=" + last.maxTS());
		}

		children[pointerIdx++] = new POHChild(child.nodeNumber(), child.minTS(), child.maxTS());

		// As this function is called, this is an internal node. So we should update the minimum timestamp of this node from the children.
		if (pointerIdx == 1) //If we are adding the first child, its min value must be the min of this node
			this.nodeMinTS = child.nodeMinTS;

		// The children are append in the time order. Therefore, the maxTS of the current child must be greater than the last child
		this.nodeMaxTS = child.maxTS(); // This will be updated when the first index entry is added

		markLocalDirty();
	}

	long getSegmentInFile(final int entryIndex) {
		assert entryIndex < entryIdx;

		return entries[entryIndex].segmentInFile();
	}

	/**
	 * Returns the node number of the left most child node that contain the given timestamp
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the node number of the left most child that has the timestamp
	 */
	int findFirstChild(final long ts) {
		int idx = TimeRangeSearch.find(children, pointerIdx, true, ts);
		if (idx < 0)
			return -1;

		return children[idx].nodeNumber();
	}

	/**
	 * Returns the node number of the right most child that contain the given timestamp
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the node number of the right most child that has the timestamp
	 */
	int findLastChild(final long ts) {
		int idx = TimeRangeSearch.find(children, pointerIdx, false, ts);
		if (idx < 0)
			return -1;

		return children[idx].nodeNumber();
	}

	/**
	 * Returns the segments number of all the index entries between minTS and maxTS inclusively
	 *
	 * minTS must be less than or equal to maxTS
	 *
	 * @param minTS
	 * @param maxTS
	 * @return null if no entries are found, otherwise returns the list in the descending order
	 */
	long[] findAllEntriesMinBased(final long minTS, final long maxTS) {
		assert minTS <= maxTS;

		if (entries.length == 0)
			return null;

		if (entries[0] == null)
			return null;

		if (maxTS < entries[0].minTS()) // if the first entry is larger than the given range
			return null;

		int length = entryIdx;

		// Find the highest range that is equal or smaller than maxTS
		int rightIdx = length - 1; // Assume that the last entry is smaller or equal than maxTS
		if (maxTS < entries[rightIdx].minTS())
			rightIdx = TimeRangeSearch.findLastFloor(entries, length, maxTS);

		// Find the highest range that is smaller than minTS or the lowest range that equals ts
		int leftIdx = 0; // Assume that the first entry is larger or equal to minTS
		if (entries[0].minTS() < minTS) { // if the first entry is smaller than minTS
			leftIdx = TimeRangeSearch.findFirstFloor(entries, rightIdx-leftIdx+1, minTS);
		}

		if (leftIdx == -1 && rightIdx == -1 || leftIdx > rightIdx) // no entry found
			return null;

		if (leftIdx == -1)
			//leftIdx = 0;
			assert false;
		else if (rightIdx == -1)
			//rightIdx = length - 1;
			assert false;

		long[] segs = new long[rightIdx - leftIdx + 1];
		for (int i=0; i<segs.length; i++) {
			// segs[i] = entries[i+leftIdx].offsetInFile(); //Ascending order
			segs[i] = entries[rightIdx-i].segmentInFile(); //Descending order
		}

		return segs;
	}

	/**
	 * Returns the entries that have minTS larger or equal to the given minTS or maxTS less than or equal to the given maxTS
	 * @param minTS
	 * @param maxTS
	 * @return
	 */
	long[] findAllEntries(final long minTS, final long maxTS) {

		//verify this funtion

		assert minTS <= maxTS;

		if (entries.length == 0)
			return null;

		if (entries[0] == null)
			return null;

		if (maxTS < entries[0].minTS()) // if the first entry is larger than the given range
			return null;

		int length = entryIdx;

		// Find the highest range that is equal or smaller than maxTS
		int rightIdx = length - 1; // Assume that the last entry is smaller or equal than maxTS
		if (maxTS < entries[rightIdx].minTS())
			rightIdx = TimeRangeSearch.findLastFloor(entries, length, maxTS);

		// Find the highest range that is smaller than minTS or the lowest range that equals ts
		int leftIdx = 0; // Assume that the first entry is larger or equal to minTS
		if (entries[0].minTS() < minTS) { // if the first entry is smaller than minTS
			leftIdx = TimeRangeSearch.findCeil(entries, rightIdx-leftIdx+1, minTS);
		}

		if (leftIdx == -1 || leftIdx > rightIdx) // no entry found
			return null;

		long[] segs = new long[rightIdx - leftIdx + 1];
		for (int i=0; i<segs.length; i++) {
			// segs[i] = entries[i+leftIdx].offsetInFile(); //Ascending order
			segs[i] = entries[rightIdx-i].segmentInFile(); //Descending order
		}

		return segs;
	}

	/**
	 * Returns the highest index entry that has the range smaller than ts or the smallest index entry that
	 * covers ts
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the first occurrence of the segment that has the timestamp
	 */
	long findFirstEntry(final long ts) {
		if (entries.length == 0)
			return -1;

		int idx = TimeRangeSearch.findFirstFloor(entries, entryIdx, ts);
		if (idx < 0)
			return -1;

		return entries[idx].segmentInFile();
	}

	/**
	 * Returns the highest index entry that has the range smaller than ts or the highest index entry that
	 * covers ts
	 *
	 * @param ts ts to search
	 * @return -1 if no entry found, otherwise returns the last occurrence of the segment that has the timestamp
	 */
	long findLastEntry(final long ts) {
		if (entries.length == 0)
			return -1;

		int idx = TimeRangeSearch.findLastFloor(entries, entryIdx, ts);
		if (idx < 0)
			return -1;

		return entries[idx].segmentInFile();
	}

	int nodeNumber () {
		return nodeNumber;
	}

	boolean isFull () {
		return entryIdx == entries.length;
	}

	long entryMinTS() {
		return entryMinTS;
	}

	int height () {
		return height;
	}

	public long minTS() {
		return nodeMinTS;
	}

	public long maxTS() {
		return nodeMaxTS;
	}

	@Override
	protected boolean shouldStoreGlobally() {
		return false;
	}

	@Override
	public int loadFromFile() throws IOException {
		try (
				RandomAccessFile file = new RandomAccessFile(id.localPath(), "r");
				SeekableByteChannel channel = file.getChannel()
		) {
			channel.position(id.numNodeInIndexBlock() * nodeSizeBytes);
			//System.out.printf("[POHN] Current channel position: %d, reading node %d, node in block %d\n", channel.position(), nodeNumber, id.numNodeInIndexBlock());
			int count = loadFrom(channel);
			//System.out.printf("[POHN] Current channel position after loading data: %d\n", channel.position());
			return count;
		}
	}

	@Override
	public int loadFrom(ReadableByteChannel channel) throws IOException {
		synchronized (storeBuffer) {
			storeBuffer.clear();
			int bytesRead = Commons.readFrom(channel, storeBuffer); //If the node is not the last node, the storeBuffer is always read fully
			assert bytesRead > 0;

			//System.out.printf("[POHN] Bytes read from channel: %d\n", bytesRead);

			storeBuffer.flip();
			return loadFrom(storeBuffer);
		}
	}

	/**
	 * This funciton is not thread-safe due to storeBuffer. However, this funciton is not concurrent
	 * @param srcBuffer
	 * @return
	 * @throws IOException
	 */
	@Override
	public int loadFrom(ByteBuffer srcBuffer) throws IOException {
		// System.out.println("\t[POHN] Bytes to load from the buffer = " + srcBuffer.remaining() + ", storeBuffer pos = " + srcBuffer.position());

		int initPos = srcBuffer.position();

		synchronized (srcBuffer) {
			// Assuming that the loadFrom is called only when the node is read into-memory. Moreover, the whole node
			// is read when the file is opened. Therefore, we do not worry about loading the node partially from the file
			// in order to support appends without loading data from the file.

			//WARNING: This function will fail if a node is loaded from the middle instead of the start byte

			int padding = nodeSizeBytes - entries.length*POHEntry.sizeBytes() - children.length*POHChild.sizeBytes() - headerSizeBytes();
			srcBuffer.position(srcBuffer.position() + padding);

			//System.out.printf("\t     >> [POHN] pos=%d, rem=%d, padding=%d\n", storeBuffer.position(), storeBuffer.remaining(), padding);

			int nodeNum = srcBuffer.getInt();
			int ht = srcBuffer.getInt();

			assert nodeNum == nodeNumber : String.format("Node number mismatch: loaded %d, should be %d",nodeNum, nodeNumber);
			assert ht == height : String.format("Node height mismatch: loaded %d, should be %d",ht, height);

			if (height > 0) { //This is not a leaf node. Therefore, this node must have min/max value of the children
				for (int i=0; i<children.length; i++) {
					nodeNum = srcBuffer.getInt();
					long minTS = srcBuffer.getLong();
					long maxTS = srcBuffer.getLong();
					children[i] = new POHChild(nodeNum, minTS, maxTS);
				}
			} else {
				srcBuffer.position(srcBuffer.position()+POHChild.sizeBytes()*children.length); // Skip loading the children
				/*for (int i=0; i<children.length; i++) {
					srcBuffer.getInt();
					srcBuffer.getLong();
					srcBuffer.getLong();
				}*/
			}

			pointerIdx = children.length;

			int entriesCount = (int)Math.ceil(srcBuffer.remaining()*1.0/POHEntry.sizeBytes());

			//System.out.printf("\t  [POHN] pos=%d, rem=%d, entriesCount=%d\n", srcBuffer.position(), srcBuffer.remaining(), entriesCount);

			dirtyIdx = 0;
			for (int i=0; i<entriesCount; i++) {
				POHEntry entry = new POHEntry();
				entries[entryIdx++] = entry;
				lastWriteIdx.incrementAndGet();

				boolean isMaxSet = entry.loadFrom(srcBuffer);

				//System.out.printf("\t  [POHN] pos=%d, rem=%d\n", srcBuffer.position(), srcBuffer.remaining());

				nodeMaxTS = entry.maxTS();

				if (!isMaxSet) { // If the max value is not loaded, this must be the last index entry that is partially stored
					System.out.println("\t>>> [POHN] Max is not set in the last entry when loaded");
					break;
				}

				dirtyIdx++;
			}
		}

		entryMinTS = entries[0].minTS();
		nodeMinTS = entryMinTS;

		if (height > 0) {
			nodeMinTS = children[0].minTS();
		}

		int bytesRead = srcBuffer.position() - initPos;

		//System.out.printf("\t[POHN] node=%d, entryIdx=%d, dirtyIdx=%d, lastWriteIdx=%d, entryMinTS=%d, nodeMinTS=%d, nodeMaxTS=%d\n",
		//		nodeNumber, entryIdx, dirtyIdx, lastWriteIdx.get(), entryMinTS, nodeMinTS, nodeMaxTS);

		//System.out.println("\t[POHN] Bytes read from the buffer: " + bytesRead);

		return  bytesRead;
	}

	@Override
	protected int storeTo(FileChannel channel) throws IOException {
		//System.out.printf("  >> [POHN] Storing node %d at channel pos %d, channel size %d\n", nodeNumber, channel.position(), channel.size());

		synchronized (storeBuffer) {
			storeBuffer.clear();
			channel.position(channel.size()); //Adjust the file channel position to the end for appending data

			// When a node is created, it is populated with the child pointers and the first node entry. Therefore,
			// the first writeToFile of a node includes the node header, the child pointers (for non-leaf nodes) and
			// at least a partial first index entry. Therefore, the dirtyIdx will be initially -1 and will be at least
			// zero after the first writeToChannel.

			if (dirtyIdx == -1) { // If we have not stored the node yet
				if (nodeNumber == 1) {
					channel.position(nodeSizeBytes); // Because we don't have node number zero. If we skip the first node in the file, all math goes wrong.
				}

				int padding = nodeSizeBytes - entries.length*POHEntry.sizeBytes() - children.length*POHChild.sizeBytes() - headerSizeBytes();
				storeBuffer.position(storeBuffer.position() + padding);

				//System.out.printf("     >> [POHN] pos=%d, rem=%d, padding=%d\n", storeBuffer.position(), storeBuffer.remaining(), padding);

				storeBuffer.putInt(nodeNumber);
				storeBuffer.putInt(height);

				//System.out.printf("     >> [POHN] pos=%d, rem=%d\n", storeBuffer.position(), storeBuffer.remaining());

				if (height > 0) { // The leaf nodes have no children. Therefore, we don't need to save the min/max timestamps
					for (int i = 0; i < children.length; i++) {
						POHChild child = children[i];
						storeBuffer.putInt(child.nodeNumber());
						storeBuffer.putLong(child.minTS());
						storeBuffer.putLong(child.maxTS());
						//System.out.printf("      [POHN] pos=%d, rem=%d\n", storeBuffer.position(), storeBuffer.remaining());
					}
				} else {
					storeBuffer.position(storeBuffer.position()+POHChild.sizeBytes()*children.length); // Fill the children bytes with zeros
					/*for (int i = 0; i < children.length; i++) {
						storeBuffer.putInt(-1);
						storeBuffer.putLong(-1);
						storeBuffer.putLong(-1);
						//System.out.printf("      [POHN] pos=%d, rem=%d\n", storeBuffer.position(), storeBuffer.remaining());
					}*/
				}

				dirtyIdx = 0; //Indicate that the current index entry is zero that is dirty
			}

			int lastIdx = lastWriteIdx.get();
			int numEntries = (lastIdx - dirtyIdx)+1; //Take the floor value because the last entry may not be fully written yet

			//System.out.printf("\t  [POHN] pos=%d, rem=%d, entriesCount=%d, lastIdx=%d, dirtyIdx=%d\n", storeBuffer.position(), storeBuffer.remaining(), numEntries, lastIdx, dirtyIdx);

			for (int i = 0; i<numEntries; i++) {
				if (entries[dirtyIdx].storeTo(storeBuffer)) { //If the max value is not set, we do not update the dirty index
					dirtyIdx++;
				} else {
					//System.out.println("  >>> [POHN] Max is not set in the last entry");
					break; // This must be the last index entry as the max value is not set
				}
			}

			//System.out.printf("\t  [POHN] node=%d, entryIdx=%d, dirtyIdx=%d, lastWriteIdx=%d, entryMinTS=%d, nodeMinTS=%d, nodeMaxTS=%d\n",
			//		nodeNumber, entryIdx, dirtyIdx, lastWriteIdx.get(), entryMinTS, nodeMinTS, nodeMaxTS);

			storeBuffer.flip(); //Flip the buffer; not rewinding because we have not set the limit manually.

			int count = Commons.writeTo(channel, storeBuffer);
			//System.out.printf("\t  [POHN] Stored %d bytes in the channel, current pos %d\n",count, channel.position());
			return count;
		}
	}

	@Override
	protected int storeToFile() throws IOException {
		FileChannel channel = FileChannel.open(new File(id().localPath()).toPath(), StandardOpenOption.APPEND);
		//channel.position(id.numNodeInIndexBlock() * nodeSizeBytes);

		int count = storeTo(channel);

		//System.out.printf("[POHN] Stored %d bytes in the file\n", count);

		channel.force(true);
		channel.close();

		return count;
	}

	@Override
	public ByteString byteString() {
		int limit = lastWriteIdx.get();

		ByteString bytes = null;
		/*synchronized (storeBuffer) {
			storeBuffer.clear();
			storeBuffer.limit(limit);
			storeBuffer.rewind();

			bytes = ByteString.copyFrom(storeBuffer, limit);
		}*/

		return bytes;
	}

	@Override
	public int sizeWhenSerialized() {
		return nodeSizeBytes;
	}

	@Override
	public boolean evictLocallyOnMemoryEviction() {
		return false;
	}

	@Override
	protected void loadBlockOnNonPrimary() throws FileNotExistException, KawkabException, IOException {

	}

	@Override
	protected void loadBlockFromPrimary() throws FileNotExistException, KawkabException, IOException {

	}

	@Override
	protected void onMemoryEviction() {

	}

}
