namespace java kawkab.fs.testclient.thrift

struct TAccumulator {
    1: required list<i64> histogram;
    2: required i64 totalCount;
    3: required double minVal;
    4: required double maxVal;
}

struct TSyncResponse {
    1: required i64 reqsCount;
    2: required double opsTput;
    3: required double tput;
	4: required double lat50;
	5: required double lat95;
	6: required double lat99;
	7: required double latMin;
	8: required double latMax;
	9: required double latMean;
	10: required bool stopAll;
}

service TestClientService {
	TSyncResponse sync (1: i32 clid, 2: i32 testID, 3: bool stopAll, 4: double tput, 5: double opsTput, 6:TAccumulator accm);

	void setup(1: i32 testID);

	void barrier(1: i32 clid);
}
