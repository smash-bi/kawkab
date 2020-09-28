namespace java kawkab.fs.testclient.thrift

struct TResult {
    1: required i64 totalCount;
    2: required i32 minVal;
    3: required i32 maxVal;
    4: required double dataTput;
    5: required double opsTput;
    6: required double recsTput;
    7: required list<i32> tputLogKeys; //Throughput timeline log keys
    8: required list<i32> tputLogValues; //Throughput timeline log values
    9: required list<i32> latHistKeys; //Latency histogram keys
    10: required list<i32> latHistValues; //Latency histogram values
}

struct TSyncResponse {
    1: TResult aggResult;
	2: required bool stopAll;
}

service TestClientService {
	TSyncResponse sync (1: i32 clid, 2: i32 testID, 3: bool stopAll, 4:TResult result);

	void setup(1: i32 testID);

	void barrier(1: i32 clid);
}
