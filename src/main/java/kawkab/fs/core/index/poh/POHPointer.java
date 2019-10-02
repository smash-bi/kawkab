package kawkab.fs.core.index.poh;

class POHPointer extends TimeRange {
	private final int number; // Node number in the post-order heap

	POHPointer(int number, long minTS, long maxTS) {
		super(minTS, maxTS);
		this.number = number;
	}

	int nodeNum () {
		return number;
	}
}
