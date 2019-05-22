namespace java kawkab.fs.client.services

enum FileMode {
	FM_READ,
	FM_APPEND
}

enum ReturnCode {
    RC_UNKNOWN,
    RC_SUCCESS,
    RC_FAILED,
    RC_FILE_NOT_EXIST,
    RC_FILE_ALREADY_EXIST
}

exception FileNotExistException {
	1: string message
}

exception FileAlreadyOpenedException {
	1: string message
}

exception RequestFailedException {
	1: string message
}

exception InvalidArgumentException {
	1: string message
}

exception InvalidSessionException {
	1: string message
}

struct OpenRequest {
	1: required string filename,
	2: required FileMode fileMode
}

struct OpenResponse {
	1: required ReturnCode rc,
	2: required i64 sessionID
}

struct ReadRequest {
	1: required i64 sessionID,
	2: required i64 offset,
	3: required i32 length
}

struct ReadResponse {
	1: required ReturnCode rc,
	2: required binary data,
	3: required i64 latency
}

struct AppendRequest {
	1: required i64 sessionID,
	2: required binary data
}

struct AppendResponse {
	1: required ReturnCode rc,
	2: required i64 latency
}

service FilesystemService {
	i64 open (1: string filename, 2: FileMode fileMode) throws 
		(1: RequestFailedException rfe);
	
	// Returns data read from the given offset in the file
	binary read (1: i64 sessionID, 2: i64 offset, 3: i32 length) throws
		(1: RequestFailedException rfe, 2: InvalidSessionException ise, 3: InvalidArgumentException iae);
	
	// Returns ths number of bytes appended
	i32 append (1: i64 sessionID, 2: binary data) throws 
		(1: RequestFailedException rfe, 2: InvalidSessionException ise);
	
	oneway void close (1: i64 sessionID);
}