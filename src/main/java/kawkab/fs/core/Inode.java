package kawkab.fs.core;

import kawkab.fs.api.Record;
import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.commons.FixedLenRecordUtils;
import kawkab.fs.core.exceptions.*;
import kawkab.fs.core.index.poh.PostOrderHeapIndex;
import kawkab.fs.core.timerqueue.DeferredWorkReceiver;
import kawkab.fs.core.timerqueue.TimerQueueIface;
import kawkab.fs.core.timerqueue.TimerQueueItem;
import kawkab.fs.utils.LatHistogram;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class Inode implements DeferredWorkReceiver<DataSegment> {
	//FIXME: fileSize can be internally staled. This is because append() is a two step process.
	// The FileHandle first calls append and then calls updateSize to update the fileSize. Ideally,
	// We should update the fileSize immediately after adding a new block. The fileSize is not being
	// updated immediately to allow concurrent reading and writing to the same dataBlock.

	private long inumber;
	private AtomicLong fileSize = new AtomicLong(0);
	private int recordSize; //Temporarily set to 1 until we implement reading/writing records
	private PostOrderHeapIndex index;

	private volatile TimerQueueItem<DataSegment> acquiredSeg;

	private static final Cache cache = Cache.instance();
	private static final ApproximateClock clock = ApproximateClock.instance();
	private static final LocalStoreManager localStore = LocalStoreManager.instance();
	//private static final GlobalStoreManager globalStore = GlobalStoreManager.instance();
	private static final Configuration conf = Configuration.instance();
	private TimerQueueIface timerQ;

	private boolean isInited;
	private int recsPerSeg;

	private static final int bufferTimeOffsetMs = 1;

	public static final long MAXFILESIZE = conf.maxFileSizeBytes;

	private AtomicInteger initCount;

	private static ThreadLocal<ByteBuffer> thrLocalBuf = ThreadLocal.withInitial(() -> ByteBuffer.allocate(conf.maxBufferLen));

	protected Inode(long inumber, int recordSize) {
		this.inumber = inumber;
		this.recordSize = recordSize;
	}

	private LatHistogram idxLog;
	//private Accumulator loadLog;
	//private long fileOpenTime;
	//private LatHistogram tl;
	private LatHistogram blkCrLog;

	/**
	 * Prepare after opening the file.
	 * This function should be called after the inode has been loaded from the file or the remote node or the global store.
	 */
	synchronized void prepare(TimerQueueIface indexQ, TimerQueueIface segsQ) {
		thrLocalBuf.get(); //FIXME: To warmup the thread for the readers. This is not a good approach.
		if (isInited) {
			initCount.incrementAndGet();
			System.out.println("Inode already inited.");
			assert index != null;
			return;
		}
		isInited = true;
		initCount = new AtomicInteger(1);

		//System.out.printf("[I] Initializing index for inode: %d, recSize=%d\n", inumber, recordSize);

		timerQ = segsQ;

		assert recordSize >= 1;
		recsPerSeg = conf.segmentSizeBytes / recordSize;
		if (recordSize > 1)
			index = new PostOrderHeapIndex(inumber, conf.indexNodeSizeBytes, conf.nodesPerBlockPOH, conf.percentIndexEntriesPerNode, cache, indexQ);

		idxLog = new LatHistogram(TimeUnit.MICROSECONDS, "Index search", 10, 5000);
		blkCrLog = new LatHistogram(TimeUnit.MICROSECONDS, "Chunk create", 100, 50000);
		//loadLog = new Accumulator(500);
		//fileOpenTime = System.currentTimeMillis();
		//tl = new LatHistogram(TimeUnit.MILLISECONDS, "segLoadLog", 100, 1000);

		/*try {
			index.loadAndInit(indexLength(fileSize.get()));
		} catch (IOException | KawkabException e) {
			e.printStackTrace();
		}*/
	}

	synchronized void loadLastBlock() throws KawkabException, IOException {
		// Pre-fetching the last block to warm up for writes
		long fs = fileSize.get();

		// System.out.printf("[I] Should load the last block? %b, fs=%d, recsPerSeg*recSize=%d\n",
		//		fs%(recsPerSeg*recordSize)!=0, fs, recsPerSeg*recordSize);

		if (fs % (recsPerSeg*recordSize) != 0) {
			BlockID curSegId = getByFileOffset(fs);

			System.out.printf("[I] Loading the last seg %s on file open\n", curSegId );

			cache.acquireBlock(curSegId).loadBlock(false);
			cache.releaseBlock(curSegId);
		}
	}

	/**
	 * Returns the ID of the segment that contains the given offset in file
	 * @throws IOException
	 */
	private BlockID getByFileOffset(long offsetInFile) {
		long segmentInFile = FixedLenRecordUtils.segmentInFile(offsetInFile, recordSize);
		int segmentInBlock = FixedLenRecordUtils.segmentInBlock(segmentInFile);
		long blockInFile = FixedLenRecordUtils.blockInFile(segmentInFile);

		return new DataSegmentID(inumber, blockInFile, segmentInBlock, recordSize);
	}

	private BlockID idBySegInFile(long segInFile) {
		int segmentInBlock = FixedLenRecordUtils.segmentInBlock(segInFile);
		long blockInFile = FixedLenRecordUtils.blockInFile(segInFile);

		return new DataSegmentID(inumber, blockInFile, segmentInBlock, recordSize);
	}

	/**
	 * Performs the read operation. Single writer and multiple readers are allowed to write and read concurrently.
	 *
	 * @return
	 * @throws IllegalArgumentException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 * @throws KawkabException
	 */
	public boolean readAt(final ByteBuffer dstBuf, final long timestamp, final int recSize, boolean loadFromPrimary) throws IOException, KawkabException {
		if (timestamp < 0) {
			throw new KawkabException(String.format("Invalid timestamp %d", timestamp));
		}

		if (recSize != recordSize) {
			throw new KawkabException(String.format("Record sizes do not match. Given %d, expected %d", recSize, recordSize));
		}

		if (dstBuf.remaining() < recordSize) {
			throw new KawkabException(String.format("Not enough space in the dstBuf, available %d, required %d\n", dstBuf.remaining(), recordSize));
		}

		idxLog.start();
		long segInFile = index.findHighest(timestamp, indexLength(fileSize.get()), loadFromPrimary);
		idxLog.end(1);

		if (segInFile == -1) {
			System.out.printf("[I] Record with timestamp %d not found.\n", timestamp);
			return false;
		}

		//System.out.println("  Read at offset: " + offsetInFile);
		BlockID curSegId = idBySegInFile(segInFile);

		//System.out.println("Reading block at offset " + offsetInFile + ": " + curBlkUuid.key);

		DataSegment curSegment = null;
		try {
			curSegment = (DataSegment)cache.acquireBlock(curSegId);
			//segLoadLog.start();
			curSegment.loadBlock(loadFromPrimary); //The segment data might not be loaded when we get from the cache
			//segLoadLog.end();
			boolean recFound = curSegment.readRecord(dstBuf, timestamp);
			dstBuf.flip();

			if (!recFound)
				return false;
		} finally {
			if (curSegment != null) {
				cache.releaseBlock(curSegment.id());
			}
		}

		return true;
	}

	/**
	 * Reads the record at index recNum.
	 *
	 * @return
	 * @throws IllegalArgumentException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 * @throws KawkabException
	 */
	public boolean readRecordN(final ByteBuffer dstBuf, final long recNum, final int recSize, boolean loadFromPrimary) throws IOException, KawkabException {
		if (recNum <= 0) {
			throw new KawkabException(String.format("Invalid record number %d", recNum));
		}

		if (recSize != recordSize) {
			throw new KawkabException(String.format("Record sizes do not match. Given %d, expected %d", recSize, recordSize));
		}

		if (dstBuf.remaining() < recordSize) {
			throw new KawkabException(String.format("Not enough space in the dstBuf, available %d, required %d\n", dstBuf.remaining(), recordSize));
		}

		long offsetInFile = (recNum-1) * recordSize;
		long fileSize = this.fileSize.get();

		assert offsetInFile >= 0 : String.format("Invalid offsetInFile %d, recNum %d", offsetInFile, recNum);

		if (offsetInFile+recordSize > fileSize)
			throw new KawkabException(String.format("Record number %d is out of range of the file. OffsetInFile=%d, recordSize=%d, recsInFile=%d",
					recNum, offsetInFile, recordSize, fileSize/recordSize));

		System.out.println("  Read at offset: " + offsetInFile);
		BlockID curSegId = getByFileOffset(offsetInFile);

		//System.out.println("Reading block at offset " + offsetInFile + ": " + curBlkUuid.key);

		DataSegment curSegment = null;
		try {
			curSegment = (DataSegment)cache.acquireBlock(curSegId);

			//segLoadLog.start();
			curSegment.loadBlock(loadFromPrimary); //The segment data might not be loaded when we get from the cache
			//segLoadLog.end();

			int bytesRead = curSegment.read(dstBuf, offsetInFile);
			dstBuf.flip();

			assert bytesRead == recordSize;
		} finally {
			if (curSegment != null) {
				cache.releaseBlock(curSegment.id());
			}
		}

		return true;
	}

	public List<ByteBuffer> readRecords(final long minTS, final long maxTS, final int recSize, boolean loadFromPrimary) throws KawkabException, IOException {
		if (minTS < 0 || maxTS < 0) {
			throw new KawkabException(String.format("Invalid minTS (%d) or maxTS (%d) is given", minTS, maxTS));
		}

		if (recSize != recordSize) {
			throw new KawkabException(String.format("Record sizes do not match. Given %d, expected %d", recSize, recordSize));
		}

		idxLog.start();
		List<long[]> offsets = index.findAll(minTS, maxTS, indexLength(fileSize.get()), loadFromPrimary); //Get the offsets
		idxLog.end(1);

		if (offsets == null) {
			System.out.printf("[I] F%d: No recs found between %d and %d. Recs in file = %d\n", inumber, minTS, maxTS, numRecords());
			return null;
		}

		//isContiguous(offsets);

		if (Commons.onPrimaryNode(inumber) || loadFromPrimary) {
			return readRecordsPrimary(offsets, minTS, maxTS, loadFromPrimary);
		}

		return readRecordsNonPrimary(offsets, minTS, maxTS);
	}

	private List<ByteBuffer> readRecordsPrimary(final List<long[]> offsets, final long minTS, final long maxTS, boolean loadFromPrimary) throws KawkabException, IOException {
		ByteBuffer dstBuf = thrLocalBuf.get();
		dstBuf.clear();

		int length = offsets.size();
		List<ByteBuffer> results = new ArrayList<>(); //The lists contains offsets to unique segments.

		for (int i=0; i<length; i++) {
			long[] segNums = offsets.get(i);
			for (int j=0; j<segNums.length; j++) {
				long segInFile = segNums[j];
				BlockID curSegId = idBySegInFile(segInFile);

				//System.out.printf("[I] Searching recs in: %s\n", curSegId);

				DataSegment curSegment = null;
				try {
					curSegment = (DataSegment)cache.acquireBlock(curSegId);

					//segLoadLog.start();
					curSegment.loadBlock(loadFromPrimary); //The segment data might not be loaded when we get from the cache
					//segLoadLog.end();

					//System.out.println("Buffer: " + dstBuf.remaining());

					//ByteBuffer dstBuf = ByteBuffer.allocate(conf.segmentSizeBytes);
					int cnt = curSegment.readRecords(minTS, maxTS, dstBuf);
					//dstBuf.flip();

					//if (cnt > 0)
					//	results.add(dstBuf);
					//System.out.printf("  seg=%d, cnt=%d\n", segInFile, cnt);
				} finally {
					if (curSegment != null) {
						cache.releaseBlock(curSegment.id()); //We can potentially improve the performance here
					}
				}
			}
		}

		if (dstBuf.position() > 0) {
			results.add(dstBuf);
			dstBuf.flip();
		}

		if (results.size() == 0)
			return null;

		return results;

	}

	private List<ByteBuffer> readRecordsNonPrimary(List<long[]> offsets, final long minTS, final long maxTS) throws KawkabException, IOException {
		LinkedList<DataSegment> segments = acquireSegments(offsets);
		try {
			loadBlocks(segments);
		} catch (IOException | FileNotExistException e) {
			for (DataSegment seg : segments) {
				cache.releaseBlock(seg.id());
			}

			throw e;
		}

		ByteBuffer dstBuf = thrLocalBuf.get();
		dstBuf.clear();

		int count = 0;
		List<ByteBuffer> results = new ArrayList<>(); //The lists contains offsets to unique segments.
		//ByteBuffer dstBuf = ByteBuffer.allocate(conf.maxBufferLen);
		for (DataSegment seg : segments) {
			/*if (dstBuf.remaining() < conf.segmentSizeBytes) {
				dstBuf.flip();
				results.add(dstBuf);
				dstBuf = ByteBuffer.allocate(conf.maxBufferLen);
			}*/

			count += seg.readRecords(minTS, maxTS, dstBuf);
			cache.releaseBlock(seg.id());
		}

		//System.out.printf("[I]\t F%d: Read %d between %d and %d from %d segments, range %d, fs=%d, \n",
		//		inumber, count, minTS, maxTS, segments.size(), maxTS-minTS, numRecords());

		if (dstBuf.position() > 0) {
			results.add(dstBuf);
			dstBuf.flip();
		}

		if (results.size() == 0) {
			assert count == 0 : String.format("[I] F%d: %d records not added in the result",inumber, count);
			return null;
		}

		assert dstBuf.remaining() > 0 :
				String.format("[I] F%d: %d records added but buf remaining is zero. Buf pos=%d, lim=%d",
						inumber, count, dstBuf.position(), dstBuf.limit());

		return results;
	}

	public List<Record> readAll(long minTS, long maxTS, final Record recFactory, boolean loadFromPrimary)
			throws IOException, KawkabException {
		if (recFactory.size() != recordSize) {
			throw new KawkabException(String.format("Record sizes do not match. Given %d, expected %d", recFactory.size(), recordSize));
		}

		idxLog.start();
		List<long[]> offsets = index.findAll(minTS, maxTS, indexLength(fileSize.get()), loadFromPrimary); //Get the offsets
		idxLog.end(1);

		if (offsets == null)
			return null;

		//System.out.println(offsets.size());

		if (loadFromPrimary || Commons.onPrimaryNode(inumber))
			return readAllRecords(offsets, minTS, maxTS, recFactory, loadFromPrimary);

		return readAllOnNonPrimary(offsets, minTS, maxTS, recFactory);

	}

	private List<Record> readAllRecords(List<long[]> offsets, long minTS, long maxTS, Record recFactory, boolean loadFromPrimary)
			throws FileNotExistException, IOException, OutOfMemoryException {
		int length = offsets.size();
		List<Record> results = new ArrayList<>(); //The lists contains offsets to unique segments.
		for (int i=0; i<length; i++) {
			long[] segNums = offsets.get(i);
			for (int j=0; j<segNums.length; j++) {
				long segInFile = segNums[j];
				BlockID curSegId = idBySegInFile(segInFile);

				//System.out.printf("[I] Searching recs in: %s\n", curSegId);

				DataSegment curSegment = null;
				try {
					curSegment = (DataSegment)cache.acquireBlock(curSegId);
					curSegment.loadBlock(loadFromPrimary); //The segment data might not be loaded when we get from the cache
					int cnt = curSegment.readAll(minTS, maxTS, recFactory, results);
					//System.out.printf("  seg=%d, cnt=%d\n", segInFile, cnt);
				} finally {
					if (curSegment != null) {
						cache.releaseBlock(curSegment.id()); //We can potentially improve the performance here
					}
				}
			}
		}

		if (results.size() == 0)
			return null;

		return results;
	}

	private List<Record> readAllOnNonPrimary(List<long[]> offsets, long minTS, long maxTS, Record recFactory)
			throws IOException, OutOfMemoryException, FileNotExistException {
		LinkedList<DataSegment> segments = acquireSegments(offsets);

		try {
			loadBlocks(segments);
		} catch (IOException | FileNotExistException e) {
			for (DataSegment seg : segments) {
				cache.releaseBlock(seg.id());
			}

			throw e;
		}

		List<Record> results = new ArrayList<>(segments.size()); //The lists contains offsets to unique segments.

		for (DataSegment seg : segments) {
			seg.readAll(minTS, maxTS, recFactory, results);
			cache.releaseBlock(seg.id());
		}

		if (results.size() == 0)
			return null;

		return results;
	}

	private LinkedList<DataSegment> acquireSegments(List<long[]> offsets) throws IOException, OutOfMemoryException {
		LinkedList<DataSegment> segs = new LinkedList<>();
		int length = offsets.size();
		for (int i=0; i<length; i++) {
			long[] segNums = offsets.get(i);
			for (int j = 0; j < segNums.length; j++) {
				long segInFile = segNums[j];

				BlockID curSegId = idBySegInFile(segInFile);
				try {
					segs.add((DataSegment)cache.acquireBlock(curSegId));
				} catch(IOException | OutOfMemoryException e) {
					for(DataSegment seg : segs) {
						cache.releaseBlock(seg.id());
					}

					throw e;
				}
			}
		}

		return segs;
	}

	private synchronized void loadBlocks(LinkedList<DataSegment> dss) throws FileNotExistException, IOException {
		assert dss.size() > 0;

		//DataSegmentID f = (DataSegmentID)(dss.get(0).id());
		//DataSegmentID l = (DataSegmentID)(dss.get(dss.size()-1).id());
		//System.out.printf("[I] F=%d: Loading blocks from %d:%d to %d:%d\n",
		//		inumber, f.blockInFile(),f.segmentInBlock(),l.blockInFile(),l.segmentInBlock());

		long curBlock = -1;
		int lastSIB = -1;
		BlockLoader bl = null;

		int skipped = 0;
		Iterator<DataSegment> itr = dss.iterator();
		while(itr.hasNext()) {
			if (!(itr.next().isFull()))break;
			skipped++;
		}

		//if (skipped > 0) System.out.printf("[I] F=%d: Total segments skipped %d out of %d\n", inumber, skipped, dss.size());

		itr = dss.listIterator(skipped);
		int cnt = 0;
		while(itr.hasNext()) {
			DataSegment ds = itr.next();
		//for (DataSegment ds : dss) {
			DataSegmentID dsid = (DataSegmentID) ds.id;
			long blockInFile = dsid.blockInFile();
			int sib = dsid.segmentInBlock();

			if (blockInFile != curBlock || lastSIB != sib+1) {
				if (curBlock != -1) { //If not the first iteration
					//bl.printSegsInLoader();
					bl.load();
				}

				curBlock = blockInFile;
				bl = new BlockLoader(inumber, blockInFile);
			}

			cnt++;
			bl.add(ds);
			lastSIB = sib;
		}

		//bl.printSegsInLoader();

		if (bl != null) {
			bl.load();
		}

		if (cnt != dss.size()-skipped)
			System.out.printf("[I] F=%d: Total segments loaded %d, expected %d\n", inumber, cnt, dss.size());

		//loadLog.put((int)(clock.currentTime()-fileOpenTime)/1000, cnt);
	}

	private boolean isContiguous(List<long[]> offsets) {
		//printOffsets(offsets);
		int length = offsets.size();
		long last = -1;
		for (int i=0; i<length; i++) {
			long[] segNums = offsets.get(i);
			for (int j = 0; j < segNums.length; j++) {
				long segInFile = segNums[j];
				if (last == -1) {
					last = segInFile;
					continue;
				}

				if (last != segInFile + 1) {
					System.out.printf("[I] Error: Index search results not contiguous. Last=%d, current=%d\n", last, segInFile);
					return false;
				}

				last = segInFile;
			}
		}

		return true;
	}

	private void printOffsets(List<long[]> offsets) {
		System.out.print("Offsets: ");
		int length = offsets.size();
		for (int i=0; i<length; i++) {
			long[] segNums = offsets.get(i);
			System.out.print(Arrays.toString(segNums) + ", ");
		}
		System.out.println();
	}

	/**
	 * Performs the read operation. Single writer and multiple readers are allowed to write and read concurrently.
	 * @param buffer
	 * @param length
	 * @param offsetInFile
	 * @return
	 * @throws IllegalArgumentException
	 * @throws InvalidFileOffsetException
	 * @throws IOException
	 * @throws KawkabException
	 */
	public int read(final byte[] buffer, final int length, final long offsetInFile, boolean loadFromPrimary) throws
			IllegalArgumentException, InvalidFileOffsetException, IOException, KawkabException {
		if (length <= 0)
			throw new IllegalArgumentException("Given length is 0.");

		if (offsetInFile + length > fileSize.get())
			throw new IllegalArgumentException(String.format("File offset + read length is greater "
					+ "than file size: %d + %d > %d", offsetInFile,length,fileSize));

		int bufferOffset = 0;
		int remaining = length;
		long curOffsetInFile = offsetInFile;

		while(remaining > 0 && curOffsetInFile < fileSize.get() && bufferOffset<buffer.length) {
			//System.out.println("  Read at offset: " + offsetInFile);
			BlockID curSegId = getByFileOffset(curOffsetInFile);

			//System.out.println("Reading block at offset " + offsetInFile + ": " + curBlkUuid.key);

			long segNumber = FixedLenRecordUtils.segmentInFile(curOffsetInFile, recordSize);
			long nextSegStart = (segNumber + 1) * conf.segmentSizeBytes;
			int toRead = (int)(curOffsetInFile+remaining <= nextSegStart ? remaining : nextSegStart - curOffsetInFile);
			int bytesRead = 0;

			// System.out.println(String.format("Seg=%s, bufLen=%d, bufOffset=%d, toRead=%d, offInFile=%d, remInBuf=%d,dataRem=%d",
			//     curSegId.toString(), buffer.length, bufferOffset, toRead, curOffsetInFile, buffer.length-bufferOffset,remaining));

			assert bufferOffset+toRead <= buffer.length;

			DataSegment curSegment = null;
			try {
				curSegment = (DataSegment)cache.acquireBlock(curSegId);
				curSegment.loadBlock(loadFromPrimary); //The segment data might not be loaded when we get from the cache
				bytesRead = curSegment.read(buffer, bufferOffset, toRead, curOffsetInFile);
			} finally {
				if (curSegment != null) {
					cache.releaseBlock(curSegment.id());
				}
			}

			bufferOffset += bytesRead;
			remaining -= bytesRead;
			curOffsetInFile += bytesRead;
		}

		return bufferOffset;
	}

	int appendRecords(final ByteBuffer srcBuf, int recSize) throws OutOfMemoryException, IOException, InterruptedException, KawkabException {
		if (recSize != recordSize)
			throw new KawkabException(String.format("The given record size (%d bytes) does not match with the file's record size (%d bytes)", recSize, recordSize));

		verifyTimestampNotNull(srcBuf);

		long fileSizeBuffered = this.fileSize.get(); // Current file size

		int appended = 0;
		int pos = srcBuf.position();
		int initLimit = srcBuf.limit();

		if (fileSizeBuffered + (initLimit-pos)*recSize > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}

		while(pos < initLimit) {
			long recInFile = fileSizeBuffered / recSize;
			int recInSeg = (int) (recInFile % recsPerSeg);
			int toAppend = (recsPerSeg - recInSeg) * recSize;
			int limit = Math.min(pos + toAppend, initLimit);
			srcBuf.limit(limit);

			int wBytes = appendBuffered(srcBuf, recSize);

			pos += wBytes;
			appended += wBytes;
			fileSizeBuffered += wBytes;
		}

		return appended;
	}

	private int appendBuffered(final ByteBuffer srcBuf, int recSize) throws OutOfMemoryException, IOException, InterruptedException, KawkabException {
		if (recSize != recordSize)
			throw new KawkabException(String.format("The given record size (%d bytes) does not match with the file's record size (%d bytes)", recSize, recordSize));

		long fileSizeBuffered = this.fileSize.get(); // Current file size

		/*if (fileSizeBuffered + recSize > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}*/

		boolean haveNewBlock = false;
		boolean acquiredFromCache = false;
		if (acquiredSeg == null || !timerQ.tryDisable(acquiredSeg)) {
			acquiredFromCache = true;
			haveNewBlock = (FixedLenRecordUtils.offsetInBlock(fileSizeBuffered, recordSize) % conf.dataBlockSizeBytes) == 0L;
			acquiredSeg = acquireSegment(fileSizeBuffered, haveNewBlock);
			//acquiredSeg.getItem().setIsLoaded();
			acquiredSeg.getItem().prepareForAppend(fileSizeBuffered);
		}

		long timestamp = srcBuf.getLong(srcBuf.position());
		DataSegment ds = acquiredSeg.getItem();

		int appended = 0;
		try {
			appended = ds.append(srcBuf, fileSizeBuffered);
		} catch (Exception e) {
			if (acquiredFromCache) {
				deferredWork(acquiredSeg.getItem());
				acquiredSeg = null;
			}
			if (haveNewBlock) {
				localStore.evictFromLocal(ds.id());
			}

			System.out.print("I1 ");
			throw e;
		}

		long recInFile = fileSizeBuffered/recordSize;
		boolean isFirstRec = recInFile % recsPerSeg == 0;
		long segInFile = recInFile/recsPerSeg;

		if (isFirstRec) { // If the current record is the first record in the data segment
			try {
				index.appendMinTS(timestamp, segInFile, indexLength(fileSizeBuffered));
			} catch(Exception e) {
				ds.rollback(appended);

				if (acquiredFromCache) {
					deferredWork(acquiredSeg.getItem());
					acquiredSeg = null;
				}
				if (haveNewBlock) {
					localStore.evictFromLocal(ds.id());

				}

				//e.printStackTrace();
				System.out.print("I2 ");
				throw e;
			}
		}

		//fileSizeBuffered += appended;

		TimerQueueItem<DataSegment> item = acquiredSeg;

		//tlog1.start();
		if (ds.isFull()) {	// If the current segment is full, we don't need to keep the segment as the segment
			acquiredSeg = null;		// is now immutable. The segment will be eventually returned to the cache.

			//long segmentInFile = FixedLenRecordUtils.segmentInFile(fileSizeBuffered-recordSize, recordSize);

			//System.out.printf("[I] DS is full: segInFile=%d, indexLen=%d\n", segInFile, indexLength(fileSizeBuffered-recordSize));

			long lastTS = srcBuf.getLong(srcBuf.position()-recSize);

			try {
				index.appendMaxTS(lastTS, segInFile, indexLength(fileSizeBuffered  + appended- recSize));
			} catch(Exception e) {
				ds.rollback(appended);

				if (acquiredFromCache) {
					deferredWork(ds);
				}
				if (haveNewBlock) {
					localStore.evictFromLocal(ds.id());
				}

				throw e;
			}
		}

		timerQ.enableAndAdd(item,clock.currentTime()+ bufferTimeOffsetMs);

		fileSizeBuffered += appended;

		fileSize.set(fileSizeBuffered);

		localStore.store(ds);

		return appended;
	}

	private void verifyTimestampNotNull(ByteBuffer buf) {
		int pos = buf.position();
		int limit = buf.limit();
		while (pos < limit) {
			assert buf.getLong(pos) > 0;
			pos += recordSize;
		}
	}

	//public static LatHistogram tlog1 = new LatHistogram(LatHistogram.TimeLogUnit.NANOS, "ab all");
	public synchronized int appendBuffered(final byte[] data, int offset, final int length) //Syncrhonized with close() due to acquiredSeg
			throws OutOfMemoryException, MaxFileSizeExceededException, IOException, InterruptedException, KawkabException {

		assert recordSize == 1;

		int remaining = length; //Reaming size of data to append
		long fileSizeBuffered = this.fileSize.get(); // Current file size

		if (fileSizeBuffered + length > MAXFILESIZE) {
			throw new MaxFileSizeExceededException();
		}


		while (remaining > 0) {
			//If null, no need to synchronize because the TimerQueue thread will never synchronize with this timer
			if (acquiredSeg == null || (!timerQ.tryDisable(acquiredSeg))) {
				boolean createNew = (fileSizeBuffered % conf.dataBlockSizeBytes) == 0L;
				acquiredSeg = acquireSegment(fileSizeBuffered, createNew);
				//acquiredSeg.getItem().setIsLoaded();
				acquiredSeg.getItem().prepareForAppend(fileSizeBuffered);
			}

			DataSegment ds = acquiredSeg.getItem();

			try {
				int bytes = ds.append(data, offset, remaining, fileSizeBuffered);

				remaining -= bytes;
				offset += bytes;
				fileSizeBuffered += bytes;
			} catch (IOException e) {
				throw new KawkabException(e);
			}

			timerQ.enableAndAdd(acquiredSeg, clock.currentTime()+ bufferTimeOffsetMs);

			//tlog1.start();
			if (ds.isFull()) {	// If the current segment is full, we don't need to keep the segment as the segment
				acquiredSeg = null;		// is now immutable. The segment will be eventually returned to the cache.

			}
		}

		fileSize.set(fileSizeBuffered);

		return length;
	}

	/**
	 * A helper function to acquire segment from the cache, and create a new data block if needed.
	 * @param fileSize Current file size
	 * @param createBlock Whether to create a new file block
	 * @return An wrapper object to release block to the cache through the TimerQueue
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private TimerQueueItem<DataSegment> acquireSegment(long fileSize, boolean createBlock) throws IOException, InterruptedException, KawkabException {
		DataSegmentID segId = getSegmentID(fileSize);
		DataSegment ds = (DataSegment) cache.acquireBlock(segId);
		assert ds.id() != null;

		if (createBlock) { // if the last block is full
			try {
				createNewBlock(segId);
			} catch(OutOfDiskSpaceException e) {
				cache.releaseBlock(segId);
				throw e;
			}
		}

		//ds.initForAppend(fileSize, recordSize);
		return new TimerQueueItem<>(ds, this);
	}

	private DataSegmentID getSegmentID(long offsetInFile) {
		long segmentInFile = FixedLenRecordUtils.segmentInFile(offsetInFile, recordSize);
		long blockInFile = FixedLenRecordUtils.blockInFile(segmentInFile);
		int segmentInBlock = FixedLenRecordUtils.segmentInBlock(segmentInFile);

		return new DataSegmentID(inumber, blockInFile, segmentInBlock, recordSize);
	}

	/**
	 * Creates a new block in the virtual file.
	 * @return Returns the ID of the first segment in the block.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void createNewBlock(final DataSegmentID dsid) throws IOException, OutOfDiskSpaceException {
		blkCrLog.start();
		localStore.createBlock(dsid);
		int t = blkCrLog.end(1);
		if (t > 1000000) {
			System.out.printf("\t\t[I] Block creation took %d microseconds\n", t);
		}
	}

	public long fileSize(){
		return fileSize.get();
	}

	/**
	 * Loads data in the inode variables from the buffer
	 * @param buffer
	 * @return Number of bytes read from the buffer
	 * @throws IOException
	 */
	int loadFrom(final ByteBuffer buffer) {
		long inum = buffer.getLong();
		long fs = buffer.getLong();
		int recSize = buffer.getInt();

		assert fileSize.get() <= fs : String.format("Received file size (%d) is smaller than existing (%d)",fs, fileSize.get());

		fileSize.set(fs);
		if (!isInited) {
			inumber = inum;
			recordSize = recSize;
		}

		//System.out.printf("[I] Loaded inode %d from buffer: fs=%d, recSize=%d, recordSize=%d\n", inum, fs, recSize, recordSize);

		return Long.BYTES*2 + Integer.BYTES;
	}

	/**
	 * Loads inode variables from the channel
	 * @param channel
	 * @return Number of bytes read from the channel
	 * @throws IOException
	 */
	int loadFrom(final ReadableByteChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(conf.inodeSizeBytes);
		int bytesRead = Commons.readFrom(channel, buffer);

		if (bytesRead < conf.inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Full block is not loaded. Loaded "
					+ "%d bytes out of %d.", bytesRead, conf.inodeSizeBytes));
		}

		buffer.rewind();
		return loadFrom(buffer);
	}

	/*
	 * Serializes the inode in the channel
	 * @return Number of bytes written in the channel
	 */
	int storeTo(final WritableByteChannel channel) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(conf.inodeSizeBytes);
		buffer.clear();
		storeTo(buffer);
		buffer.rewind();

		int bytesWritten = Commons.writeTo(channel, buffer);

		if (bytesWritten < conf.inodeSizeBytes) {
			throw new InsufficientResourcesException(String.format("Full inode is not stored. Stored "
					+ "%d bytes out of %d.", bytesWritten, conf.inodeSizeBytes));
		}
		return bytesWritten;

	}

	int storeTo(ByteBuffer buffer) {
		//System.out.printf("[I] Storing inode %d to buffer: fs=%d, recSize=%d, bufSize=%d, pos=%d, rem=%d\n",
		//		inumber, fileSize.get(), recordSize, buffer.capacity(), buffer.position(), buffer.remaining());

		buffer.putLong(inumber);
		buffer.putLong(fileSize.get());
		buffer.putInt(recordSize);

		return Long.BYTES*2 + Integer.BYTES;
	}

	/**
	 * This function must not be concurrent to the append function. So this should be called from the same thread as
	 * the append function. Otherwise, we should synchronize the caller and the appender in append and close functions.
	 */
	synchronized void cleanup() throws KawkabException { //Synchronized with appendBuffered() due to acquiredSeg
		//tlog.printStats("DS.append,ls.store");
		int n = initCount.decrementAndGet();
		if (n > 0)
			return;

		releaseAcquiredSeg();

		if (index != null)
			index.shutdown();

		index = null;
		isInited = false;

		idxLog.printStats();
		blkCrLog.printStats();
		//System.out.printf("Segments loading counts timeline: "); loadLog.printPairs();
	}

	synchronized void flush() throws KawkabException {
		releaseAcquiredSeg();
		if (index != null)
			index.flush();
	}

	private void releaseAcquiredSeg() {
		if (acquiredSeg != null && timerQ.tryDisable(acquiredSeg)) {
			deferredWork(acquiredSeg.getItem());
			acquiredSeg = null;
		}
	}

	public void printStats() {
		idxLog.printStats();
		blkCrLog.printStats();
		//loadLog.printPairs();

		if (index != null)
			index.printStats();
		//segLoadLog.printStats();
		long fs = fileSize();
		System.out.printf("\tFile=%d, FileSize=%d, recsInFile=%d, segsInFile=%d, recsPerSeg=%d, indexLen=%d, numIdxNodes=%d\n",inumber, fs, recordsInFile(),
				FixedLenRecordUtils.segmentInFile(fs, recordSize), recsPerSeg, indexLength(fs), index!=null?index.size(indexLength(fs)):0);
	}

	public void resetStats() {
		idxLog.reset();
		//segLoadLog.reset();
		if (index != null)
			index.resetStats();
	}

	@Override
	public void deferredWork(DataSegment ds) {
		cache.releaseBlock(ds.id());
	}

	public long numRecords() {
		return fileSize.get() / recordSize;
	}

	private long indexLength(final long filesize) {
		// ceil(recsInFile / recsPerSeg)
		// 0 1 2 | 3 4 5 | 6 7
		// (fs / recordSize) / (segSize / recordSize)
		long recsInFile = filesize / recordSize;
		//long segsInFile = (long) Math.ceil(recsInFile / (double)recsPerSeg); // Taking the ceil value
		long segsInFile = (recsInFile+recsPerSeg-1)/recsPerSeg; //FIXME: The long value may overflow
		return segsInFile * 2 - ((recsInFile % recsPerSeg) == 0 ? 0 : 1);
	}

	public int recordSize() {
		return recordSize;
	}

	public long recordsInFile() {
		return fileSize.get()/recordSize;
	}
}
