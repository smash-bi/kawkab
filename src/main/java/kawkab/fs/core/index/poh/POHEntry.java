package kawkab.fs.core.index.poh;

class POHEntry extends TimeRange{
	private final long segInFile;

	POHEntry(final long minTS, final long maxTS, final long segInFile) {
		super(minTS, maxTS);
		this.segInFile = segInFile;
	}

	long segInFile() {
		return segInFile;
	}

	@Override
	public String toString() {
		return String.format("minTS=%d, maxTS=%d, segInFile=%d", minTS, maxTS, segInFile);
	}
}
