namespace java kawkab.fs.testclient.thrift

struct TResult {
    1: required list<i64> latHistogram; //Latency histogram
    2: required i64 totalCount;
    3: required double minVal;
    4: required double maxVal;
    5: required double dataTput;
    6: required double opsTput;
    7: required list<i64> tputLog; //Throughput timeline log
    8: required i32 recsTput;
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
