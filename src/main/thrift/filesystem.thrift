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

service FilesystemService {
	i64 open (1: string filename, 2: TFileMode fileMode, 3: i32 recordSize) throws
		(1: TRequestFailedException rfe);
	
	// Returns data read from the given offset in the file
	binary read (1: i64 sessionID, 2: i64 offset, 3: i32 length) throws
		(1: TRequestFailedException rfe, 2: TInvalidSessionException ise, 3: TInvalidArgumentException iae);
	
	// Returns ths number of bytes appended
	i32 append (1: i64 sessionID, 2: binary data) throws 
		(1: TRequestFailedException rfe, 2: TInvalidSessionException ise);
	
	oneway void close (1: i64 sessionID);
}