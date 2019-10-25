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

/**
 * The class is not thread safe for concurrent appends. However, the search and append can be concurrent.
 */
public class POHNode extends Block {
	// Persistent variables
	private int nodeNumber; //node number
	private int height;
	private POHEntry[] entries; // Number of index entries
	private POHChild[] children;

	private int entryIdx; //Index of the next entry that will be added. In other words, the number of valid entries.
	private int pointerIdx;

	private final IndexNodeID id;


	private long entryMinTS = Long.MAX_VALUE;
	private long nodeMaxTS;
	private long nodeMinTS = Long.MAX_VALUE;

	private ByteBuffer storeBuffer; // For writing to FileChannel in storeTo(FileChannel) function
	//private AtomicInteger lastUpdatedEntryIdx; // Next byte write position in the storeBuffer

	// The offset where the dirty bytes start, the bytes that are not persisted yet.
	private int dirtyOffsetStart = 0;
	private int tsCount = 0; //Number of dirty timestamps

	private int nodeSizeBytes; // The size includes nodeNumber, height, children pointer entries, and index entries
	private boolean inited;

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
	synchronized void init(final int nodeNum, final int height, final int entriesCount, final int childrenCount, final int nodeSizeBytes) {
		if (inited)
			return;
		inited = true;

		assert entriesCount > 0;
		assert childrenCount > 1;
		assert nodeNumber == nodeNum;

		this.height = height;

		entries = new POHEntry[entriesCount];
		children = new POHChild[childrenCount];
		this.nodeSizeBytes = nodeSizeBytes;

		storeBuffer = ByteBuffer.allocate(nodeSizeBytes); // Integer for numEntries
	}

	void initForAppend() {
		setIsLoaded();
	}

	/**
	 * Size of the header in the node without padding.
	 * @return
	 */
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

		entryIdx++;
		tsCount++;
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

		assert entry.segmentInFile() == segmentInFile : String.format("SegInFile should match: expected %d, have %d", entry.segmentInFile(), segmentInFile);

		entry.setMaxTS(maxTS);

		nodeMaxTS = maxTS; // We add the entries in sequence. Therefore, each entries maxTS is greater or equal than the last etnry's maxTS

		tsCount++;
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

		entryIdx++;
		tsCount += 2;
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
		assert entryIndex < entryIdx : String.format("given idx should be less than the node's entryIdx. Given %d, expected < %d", entryIndex, entryIdx);

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
			//System.out.printf("[PN] Current channel position: %d, reading node %d, node in block %d\n", channel.position(), nodeNumber, id.numNodeInIndexBlock());
			int count = loadFrom(channel);
			//System.out.printf("[PN] Current channel position after loading data: %d\n", channel.position());
			return count;
		}
	}

	/**
	 * This funciton is not thread-safe due to storeBuffer. However, this funciton is not concurrent
	 * @return
	 * @throws IOException
	 */
	@Override
	public int loadFrom(ReadableByteChannel channel) throws IOException {
		System.out.printf("[PN] Loading %s from channel\n",id);

		assert isOnPrimary;
		// System.out.println("\t[PN] Bytes to load from the buffer = " + srcBuffer.remaining() + ", storeBuffer pos = " + srcBuffer.position());

		int count = 0;
		synchronized (storeBuffer) {
			storeBuffer.clear();
			int bytesRead = Commons.readFrom(channel, storeBuffer); //If the node is not the last node, the storeBuffer is always read fully
			assert bytesRead > 0;

			//System.out.printf("[PN] Bytes read from channel: %d\n", bytesRead);

			storeBuffer.flip();

			// Assuming that the loadFrom is called only when the node is read into-memory. Moreover, the whole node
			// is read when the file is opened. Therefore, we do not worry about loading the node partially from the file
			// in order to support appends without loading data from the file.

			//WARNING: This function will fail if a node is loaded from the middle instead of the start byte

			count += loadHeaderFrom(storeBuffer, nodeSizeBytes, entries.length, children.length);

			count += loadChildrenFrom(storeBuffer, true);

			pointerIdx = children.length;

			int pos = storeBuffer.position();
			int numTSLoaded = loadEntriesFrom(storeBuffer, 0);
			count += storeBuffer.position() - pos;

			entryIdx = (numTSLoaded+1)/2;
			dirtyOffsetStart = numTSLoaded;
			tsCount = dirtyOffsetStart;

			//if (loadedLastMax) { // Last entry's maxTS is not loaded
				// System.out.printf("[PN] maxTS is not set in %s when loaded, bytesLoaded=%d, shouldBe=%d, entries=%d\n",
				// 		id, bytesLoaded, numEntries*POHEntry.sizeBytes(), numEntries);
				//dirtyOffsetStart--;
			//}

			entryMinTS = entries[0].minTS();
			nodeMaxTS = entries[entryIdx-1].maxTS();
			nodeMinTS = height == 0 ? entryMinTS : children[0].minTS();
		}

		// System.out.printf("\t[PN] node=%d, entryIdx=%d, dirtyIdx=%d, lastWriteIdx=%d, entryMinTS=%d, nodeMinTS=%d, nodeMaxTS=%d\n",
		//		nodeNumber, entryIdx, dirtyIdx, lastWriteIdx.get(), entryMinTS, nodeMinTS, nodeMaxTS);

		//System.out.println("\t[PN] Bytes read from the buffer: " + bytesRead);

		return  count;
	}

	private int loadHeaderFrom(ByteBuffer buffer, int nodeSizeBytes, int numEntries, int numChildren) {
		int padding = nodeSizeBytes -numEntries*POHEntry.sizeBytes() - numChildren*POHChild.sizeBytes() - headerSizeBytes();
		buffer.position(buffer.position() + padding);

		//System.out.printf("\t     >> [PN] pos=%d, rem=%d, padding=%d\n", storeBuffer.position(), storeBuffer.remaining(), padding);

		int nodeNum = buffer.getInt();
		int ht = buffer.getInt();

		assert nodeNum == nodeNumber : String.format("Node number mismatch: loaded %d, should be %d",nodeNum, nodeNumber);
		assert ht == height : String.format("Node height mismatch: loaded %d, should be %d",ht, height);

		System.out.printf(" Loaded header: nodenum=%d, height=%d, padding=%d\n", nodeNum, ht, padding);

		return padding + Integer.BYTES*2;
	}

	private int loadChildrenFrom(ByteBuffer buffer, boolean withPadding) {
		System.out.printf("Before load children: rem=%d\n", buffer.remaining());

		int count = 0;
		if (height > 0) { //This is not a leaf node. Therefore, this node must have min/max value of the children
			for (int i=0; i<children.length; i++) {
				POHChild child = new POHChild();
				count += child.loadFrom(buffer);
				children[i] = child;
			}
		} else if (withPadding) {
			int bytesToSkip = POHChild.sizeBytes()*children.length;
			buffer.position(buffer.position()+bytesToSkip); // Skip loading the children
			count += bytesToSkip;
		}

		System.out.printf("After load children: rem=%d\n", buffer.remaining());

		return count;
	}

	/**
	 * The first and the last entry can be partial. The first entry may not have the minTS, and the last entry may not have maxTS.
	 *
	 * @param buffer
	 * @param atTSOffset TSCount offset.
	 * @return
	 */
	private int loadEntriesFrom(ByteBuffer buffer, int atTSOffset) {


		boolean loadedLastMax = false; //The last entry in the buffer has the max value or not

		int idxOffset = atTSOffset/2;
		boolean hasFirstMin = atTSOffset % 2 == 0;
		int count = hasFirstMin ? 0 : -1;

		System.out.printf(" Load entries from: rem=%d, fromIdx=%d, atTSOffset=%d\n", buffer.remaining(), idxOffset, atTSOffset);

		while (buffer.hasRemaining()) {
			POHEntry entry = new POHEntry();
			entries[idxOffset++] = entry;

			loadedLastMax = entry.loadFrom(buffer, hasFirstMin);
			// System.out.printf("\t  [PN] pos=%d, rem=%d\n", buffer.position(), buffer.remaining());

			count += 2;

			if (!loadedLastMax) {
				count--;
				break;
			}
		}

		return count;
	}

	private int storeHeaderTo(ByteBuffer buffer, int nodeSizeBytes, int numEntries, int numChildren) {
		int padding = nodeSizeBytes - numEntries*POHEntry.sizeBytes() - numChildren*POHChild.sizeBytes() - headerSizeBytes();
		buffer.position(buffer.position() + padding);

		//System.out.printf("     >> [PN] pos=%d, rem=%d, padding=%d\n", storeBuffer.position(), storeBuffer.remaining(), padding);

		buffer.putInt(nodeNumber);
		buffer.putInt(height);

		return padding + Integer.BYTES*2;
	}

	/**
	 *
	 * @param buffer
	 * @param withPadding If the height of the node is zero, the buffer is padded if the withPadding is true, otherwise
	 *                    the buffer is not modified
	 * @return
	 */
	private int storeChildrenTo(ByteBuffer buffer, boolean withPadding) {
		int count = 0;
		if (height > 0) { // The leaf nodes have no children. Therefore, we don't need to save the min/max timestamps
			for (int i = 0; i < children.length; i++) {
				POHChild child = children[i];
				count += child.storeTo(buffer);
				// System.out.printf("      [PN] pos=%d, rem=%d\n", buffer.position(), buffer.remaining());
			}
		} else if (withPadding){
			int bytesToSkip = POHChild.sizeBytes()*children.length;
			buffer.position(buffer.position()+bytesToSkip); // Fill the children bytes with zeros
			count = bytesToSkip;
		}

		return count;
	}

	/**
	 *
	 * @param buffer
	 * @return Number of timestamps stored in the buffer.
	 */
	private int storeEntriesTo(ByteBuffer buffer, int tsIdxOffset) {
		//System.out.printf("\t  [PN] pos=%d, rem=%d, entriesCount=%d, lastIdx=%d, dirtyIdx=%d\n", storeBuffer.position(), storeBuffer.remaining(), numEntries, lastIdx, dirtyIdx);

		int numTS = tsCount;
		int entryIdx = (numTS+1)/2;
		int idxOffset = tsIdxOffset/2;
		boolean withFirstMin = tsIdxOffset % 2 == 0;

		for (;idxOffset < entryIdx; idxOffset++) {
			POHEntry entry = entries[idxOffset];
			entry.storeTo(buffer, withFirstMin);
			withFirstMin = true;
		}

		return numTS - tsIdxOffset;
	}

	@Override
	protected int storeTo(FileChannel channel) throws IOException {
		System.out.printf("[PN] Storing %s in channel\n",id);

		assert isOnPrimary;
		//System.out.printf("  >> [PN] Storing node %d at channel pos %d, channel size %d\n", nodeNumber, channel.position(), channel.size());

		synchronized (storeBuffer) {
			storeBuffer.clear();
			channel.position(channel.size()); //Adjust the file channel position to the end for appending data

			// When a node is created, it is populated with the child pointers and the first node entry. Therefore,
			// the first writeToFile of a node includes the node header, the child pointers (for non-leaf nodes) and
			// at least a partial first index entry. Therefore, the dirtyIdx will be initially -1 and will be at least
			// zero after the first writeToChannel.

			if (dirtyOffsetStart == 0) { // If we have not stored the node yet
				if (nodeNumber == 1) {
					channel.position(nodeSizeBytes); // Because we don't have node number zero. If we skip the first node in the file, all math goes wrong.
				}

				storeHeaderTo(storeBuffer, nodeSizeBytes, entries.length, children.length);

				//System.out.printf("     >> [PN] pos=%d, rem=%d\n", storeBuffer.position(), storeBuffer.remaining());

				storeChildrenTo(storeBuffer, true);
			}

			int numDirtyTS = tsCount - dirtyOffsetStart; //Take the floor value because the last entry may not be fully written yet

			int numTSStored = storeEntriesTo(storeBuffer, dirtyOffsetStart);

			assert numTSStored == numDirtyTS;

			dirtyOffsetStart += numTSStored;


			//if (!hasLastMax) { //Last entry has not maxTS set
				//dirtyOffsetStart--;
				// System.out.printf("  >>> [PN] Max is not set in the last entry when storing, id=%s, numEntries=%d, bytesStored=%d, shouldBe=%d, entriesStored=%d\n",
				//		id, numEntries, bytesStored, numEntries*POHEntry.sizeBytes(), (int)Math.ceil(1.0*bytesStored/POHEntry.sizeBytes()));
			//}

			// System.out.printf("\t  [PN] node=%d, entryIdx=%d, dirtyIdx=%d, lastWriteIdx=%d, entryMinTS=%d, nodeMinTS=%d, nodeMaxTS=%d\n",
			//		nodeNumber, entryIdx, dirtyIdx, lastWriteIdx.get(), entryMinTS, nodeMinTS, nodeMaxTS);

			storeBuffer.flip(); //Flip the buffer; not rewinding because we have not set the limit manually.

			//System.out.printf("\t  [PN] Stored %d bytes in the channel, current pos %d\n",count, channel.position());

			return Commons.writeTo(channel, storeBuffer);
		}
	}

	@Override
	protected int storeToFile() throws IOException {
		FileChannel channel = FileChannel.open(new File(id().localPath()).toPath(), StandardOpenOption.APPEND);
		//channel.position(id.numNodeInIndexBlock() * nodeSizeBytes);

		int count = storeTo(channel);

		//System.out.printf("[PN] Stored %d bytes in the file\n", count);

		channel.force(true);
		channel.close();

		return count;
	}

	@Override
	public ByteString byteString() {
		assert false;

		return null;
	}

	public ByteString byteString(int fromTSIdx) throws IOException {
		if (fromTSIdx == tsCount) { //If no new entry is present
			return ByteString.EMPTY;
		}

		synchronized (storeBuffer) {
			try {
				storeBuffer.clear();
				storeTo(storeBuffer, fromTSIdx);
				storeBuffer.flip();
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}

			return ByteString.copyFrom(storeBuffer);
		}
	}

	/**
	 *
	 * @param buffer
	 * @param fromTSIdx
	 * @return Number of TS stored in the buffer
	 * @throws IOException
	 */
	protected int storeTo(ByteBuffer buffer, int fromTSIdx) throws IOException {
		System.out.printf("[PN] Storing %s in buffer\n",id);

		boolean withHeader = fromTSIdx % 2 == 0;
		if (withHeader) {
			storeHeaderTo(buffer, nodeSizeBytes, entries.length, children.length);
			storeChildrenTo(buffer, false);
		}

		return storeEntriesTo(buffer, fromTSIdx);
	}

	@Override
	public int loadFrom(ByteBuffer buffer) {
		assert false;

		return 0;
	}

	public int loadFrom(ByteBuffer buffer, int atTSOffset) throws IOException {
		System.out.printf("[PN] Loading %s from buffer\n",id);

		assert !isOnPrimary;

		if (atTSOffset % 2 == 0) {
			loadHeaderFrom(buffer, nodeSizeBytes, entries.length, children.length);
			loadChildrenFrom(buffer, false);
		}

		System.out.println(" Now loading children");
		return loadEntriesFrom(buffer, atTSOffset);
	}

	private void loadBlockFromPrimary() throws FileNotExistException, KawkabException, IOException {
		boolean withHeader = dirtyOffsetStart == 0;
		boolean withFirstMin = dirtyOffsetStart % 2 == 0;

		ByteBuffer buffer = primaryNodeService.getIndexNode(id, dirtyOffsetStart);

		if (buffer.remaining() == 0) {
			System.out.println("No new entries retrieved.");
			return;
		}

		System.out.printf("Before loading the node: pos=%d, rem=%d, dirtyOffset=%d, entryIdx=%d, txCount=%d\n",
				buffer.position(), buffer.remaining(), dirtyOffsetStart, entryIdx, tsCount);

		int numTSLoaded = loadFrom(buffer, dirtyOffsetStart);

		if (dirtyOffsetStart == 0)
			pointerIdx = children.length;

		assert dirtyOffsetStart+numTSLoaded >= entryIdx;

		dirtyOffsetStart += numTSLoaded;
		tsCount = dirtyOffsetStart;
		entryIdx = (tsCount+1)/2;

		System.out.printf("After loading the node: pos=%d, rem=%d, dirtyOffset=%d, entryIdx=%d, txCount=%d\n",
				buffer.position(), buffer.remaining(), dirtyOffsetStart, entryIdx, tsCount);


		//if (loadedLastMax) { // Last entry's maxTS is not loaded
		// System.out.printf("[PN] maxTS is not set in %s when loaded, bytesLoaded=%d, shouldBe=%d, entries=%d\n",
		// 		id, bytesLoaded, numEntries*POHEntry.sizeBytes(), numEntries);
		//dirtyOffsetStart--;
		//}

		entryMinTS = entries[0].minTS();
		nodeMaxTS = entries[entryIdx-1].maxTS();
		nodeMinTS = height == 0 ? entryMinTS : children[0].minTS();
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
	protected synchronized void loadBlockOnNonPrimary() throws FileNotExistException, KawkabException, IOException {
		// If the node is full, try fetching from the global store. If fails, load from the primary node.
		// Otherwise, the node is most likely not in the global store. Therefore, fetch from the primary node.

		if (entryIdx != entries.length) {
			loadBlockFromPrimary();
			return;
		}

		try {
			loadFromGlobal();
		} catch (FileNotExistException e) {
			System.out.println("[PN] Node not found in the global: " + id());
			loadBlockFromPrimary();
		}
	}

	@Override
	protected void onMemoryEviction() {
		// Do nothing
	}
}
