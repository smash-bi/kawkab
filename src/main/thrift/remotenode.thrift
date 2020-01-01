namespace java kawkab.fs.core.services.thrift

exception TFileNotExistException {
	1: string message
}

exception TRequestFailedException {
	1: string message
}

service PrimaryNodeService {
	binary getSegment (1: i64 inumber, 2: i64 blockInFile, 3: i32 segmentInBlock, 4: i32 recordSize, 5: i32 offset) throws (1: TFileNotExistException fne);
	binary getInodesBlock (1: i32 blockIndex) throws (1: TFileNotExistException fne);
	binary getIndexNode (1: i64 inumber, 2: i32 nodeNumInIndex, 3: i32 fromTsIndex) throws (1: TFileNotExistException fne);
}
