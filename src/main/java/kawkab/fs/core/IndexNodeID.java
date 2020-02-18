package kawkab.fs.core;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.index.poh.POHNode;

import java.io.File;
import java.util.Objects;

public class IndexNodeID extends BlockID {
	private final long inumber; // inumber of the file with which this index is associated
	private final int nodeNumInIndex; // Node number in the index. This is equivalent to segmentInFile
	private final int blockInIndex; // Block number in the index. Each block have many index nodes.
	private String localPath;
	private int hash;
	private int hash2;

	private static final Configuration conf = Configuration.instance();
	private static final int indexBlocksPerDirectory = conf.indexBlocksPerDirectory;
	private static final String indexBlocksPath = conf.indexBlocksPath + File.separator;
	private static final int nodesPerBlock = conf.nodesPerBlockPOH;

	public IndexNodeID(final long inumber, final int nodeNumInIndex) {
		super(BlockID.BlockType.INDEX_BLOCK);

		this.inumber = inumber;
		this.nodeNumInIndex = nodeNumInIndex ;
		this.blockInIndex = nodeNumInIndex / nodesPerBlock;
	}

	public long inumber() {
		return inumber;
	}

	public int nodeNumber() {
		return nodeNumInIndex;
	}

	public int numNodeInIndexBlock() {
		return nodeNumInIndex % nodesPerBlock;
	}

	@Override
	public Block newBlock() {
		return new POHNode(this);
	}

	@Override
	public int primaryNodeID() {
		return Commons.primaryWriterID(inumber);
	}

	@Override
	public String localPath() {
		if (localPath == null) //TODO: Use StringBuilder
			localPath = indexBlocksPath + (inumber/indexBlocksPerDirectory) + File.separator + inumber + "-" + blockInIndex;

		return localPath;
	}

	@Override
	public int perBlockTypeKey() {
		if (hash2 == 0)
			Objects.hash(inumber, blockInIndex);
		return hash2;
	}

	@Override
	public String fileID() {
		return "N"+inumber+ blockInIndex;
	}

	@Override
	public int hashCode() {
		if (hash == 0)
			hash = Objects.hash(inumber, nodeNumInIndex);
		return hash;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		IndexNodeID that = (IndexNodeID) o;
		return inumber == that.inumber &&
				nodeNumInIndex == that.nodeNumInIndex;
	}

	@Override
	public String toString() {
		return String.format("N-%d-%d-%d",inumber, blockInIndex, nodeNumInIndex);
	}
}
