namespace java kawkab.fs.core.services.thrift

enum TFileMode {
	FM_READ,
	FM_APPEND
}

enum TReturnCode {
    RC_UNKNOWN,
    RC_SUCCESS,
    RC_FAILED,
    RC_FILE_NOT_EXIST,
    RC_FILE_ALREADY_EXIST
}

exception TFileNotExistException {
	1: string message
}

exception TFileAlreadyOpenedException {
	1: string message
}

exception TRequestFailedException {
	1: string message
}

exception TInvalidArgumentException {
	1: string message
}

exception TInvalidSessionException {
	1: string message
}

exception TOutOfMemoryException {
	1: string message
}

struct TOpenRequest {
	1: required string filename,
	2: required TFileMode fileMode
}

struct TOpenResponse {
	1: required TReturnCode rc,
	2: required i64 sessionID
}

struct TReadRequest {
	1: required i64 sessionID,
	2: required i64 offset,
	3: required i32 length
}

struct TReadResponse {
	1: required TReturnCode rc,
	2: required binary data,
	3: required i64 latency
}

struct TAppendRequest {
	1: required i64 sessionID,
	2: required binary data
}

struct TAppendResponse {
	1: required TReturnCode rc,
	2: required i64 latency
}

struct TFileOpenRequest {
    1: required string filename;
    2: required TFileMode fileMode;
    3: required i32 recordSize;
}

service FilesystemService {
	i32 open (1: string filename, 2: TFileMode fileMode, 3: i32 recordSize) throws (1: TRequestFailedException rfe);

	list<i32> bulkOpen (1: list<TFileOpenRequest> fopenReqs) throws (1: TRequestFailedException rfe);
	
	// Returns data read from the given offset in the file
	binary read (1: i32 sessionID, 2: i64 offset, 3: i32 length, 4: bool loadFromPrimary) throws
		(1: TRequestFailedException rfe, 2: TInvalidSessionException ise, 3: TInvalidArgumentException iae, 4: TOutOfMemoryException ome);

    // Read a specific record number
	binary recordNum(1: i32 sessionID, 2: i64 recordNum, 3: i32 recSize, 4: bool loadFromPrimary) throws
	    (1: TRequestFailedException rfe, 2: TInvalidSessionException ise, 3: TOutOfMemoryException ome);

    // Read the record from a given timestamp, matching exactly
	binary recordAt(1: i32 sessionID, 2: i64 timestamp, 3: i32 recSize, 4: bool loadFromPrimary) throws
	    (1: TRequestFailedException rfe, 2: TInvalidSessionException ise, 3: TOutOfMemoryException ome);

    // Read the records within the given time range, which is inclusive
	list<binary> readRecords(1: i32 sessionID, 2: i64 minTS, 3: i64 maxTS, 4: i32 recSize, 5: bool loadFromPrimary) throws
	    (1: TRequestFailedException rfe, 2: TInvalidSessionException ise, 3: TOutOfMemoryException ome);

	i32 appendRecord (1: i32 sessionID, 2: binary data, 3: i32 recSize) throws
	    (1: TRequestFailedException rfe, 2: TInvalidSessionException ise, 3: TOutOfMemoryException ome);

    /*Batching individual ByteBuffers*/
	i32 appendRecordBatched (1: i32 sessionID, 2: list<binary> data, 3: i32 recSize) throws
    	    (1: TRequestFailedException rfe, 2: TInvalidSessionException ise, 3: TOutOfMemoryException ome);

    /*Batching records of a single file in a buffer*/
    i32 appendRecordBuffered (1: i32 sessionID, 2: binary data, 3: i32 recSize) throws
        	    (1: TRequestFailedException rfe, 2: TInvalidSessionException ise, 3: TOutOfMemoryException ome);

    /*Batching records of multiple files in a buffer*/
    i32 appendRecords (1: binary data) throws
            	    (1: TRequestFailedException rfe, 2: TInvalidSessionException ise, 3: TOutOfMemoryException ome);

    //NoOP equivalent of appendRecords
    i32 appendNoops (1: binary data) throws
                	    (1: TRequestFailedException rfe, 2: TInvalidSessionException ise, 3: TOutOfMemoryException ome);

	// Returns ths number of bytes appended
	i32 append (1: i32 sessionID, 2: binary data) throws
		(1: TRequestFailedException rfe, 2: TInvalidSessionException ise, 3: TOutOfMemoryException ome);

	i64 size(1: i32 sessionID) throws (1: TRequestFailedException rfe, 2: TInvalidSessionException ise);

	i32 recordSize(1: i32 sessionID) throws (1: TRequestFailedException rfe, 2: TInvalidSessionException ise);
	
	oneway void close (1: i32 sessionID);

	oneway void bulkClose (1: list<i32> sessionID);

	i32 flush();

	i32 noopWrite (1: i64 none) throws (1: TRequestFailedException rfe, 2: TInvalidSessionException ise);
	binary noopRead (1: i32 recSize) throws (1: TRequestFailedException rfe, 2: TInvalidSessionException ise);
}