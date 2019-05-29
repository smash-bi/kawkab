package kawkab.fs.core;

import kawkab.fs.commons.Commons;
import kawkab.fs.commons.Configuration;

import java.io.File;
import java.util.Objects;

public class IndexBlockID extends BlockID {
	private final long inumber;
	private final int blockIndex;
	private String localPath;
	private int hash;
	
	private static final int indexBlocksPerDirectory = Configuration.instance().indexBlocksPerDirectory;
	private static final String indexBlocksPath = Configuration.instance().indexBlocksPath + File.separator;
	
	public IndexBlockID(long inumber, int blockIndex) {
		super(BlockType.INDEX_BLOCK);
		this.inumber = inumber;
		this.blockIndex = blockIndex;
	}
	
	@Override
	public Block newBlock() {
		return new IndexBlock(this);
	}
	
	@Override
	public int primaryNodeID() {
		return Commons.primaryWriterID(inumber);
	}
	
	@Override
	public String localPath() {
		if (localPath == null) //TODO: Use StringBuilder
			localPath = indexBlocksPath + (inumber/indexBlocksPerDirectory) + File.separator + inumber + "-" + blockIndex;
		
		return localPath;
	}
	
	@Override
	public int perBlockKey() {
		return (int)inumber;
	}
	
	@Override
	public int hashCode() {
		if (hash == 0)
			hash = Objects.hash(inumber, blockIndex);
		return hash;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		IndexBlockID that = (IndexBlockID) o;
		return inumber == that.inumber &&
				blockIndex == that.blockIndex;
	}
}
