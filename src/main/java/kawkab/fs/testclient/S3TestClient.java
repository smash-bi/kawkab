package kawkab.fs.testclient;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkBaseException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.utils.Accumulator;
import kawkab.fs.utils.AccumulatorMap;
import kawkab.fs.utils.LatHistogram;
import org.apache.zookeeper.server.ByteBufferInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class S3TestClient {
	private AmazonS3 client;
	private static final String rootBucket = "kawkab-blocks"; //Cannot contain uppercase letters.
	private static final String contentType = "application/octet-stream";
	private int id;
	private boolean working;

	public S3TestClient(int id, String accessKey, String secretKey, String server) {
		this.id = id;
		client = newS3Client(accessKey, secretKey, server);
		createRootBucket();
		listExistingBuckets();
		working = true;

		//dlRateLog = new AccumulatorMap(1000); //Number of seconds we will run
	}

	public Result uploadTest(int objSizeBytes, int numObjs) {
		ByteBuffer buffer = ByteBuffer.allocate(objSizeBytes);
		AccumulatorMap wLats = new AccumulatorMap(1000);
		LatHistogram wLog = new LatHistogram(TimeUnit.MICROSECONDS, "S3 upload lat", 50, 100000);

		long st = System.currentTimeMillis();
		String path = "test/"+id+"/";
		for (int i=0; i<numObjs; i++) {
			buffer.limit(buffer.capacity());
			buffer.position(0);
			int lat = -1;
			try {
				wLog.start();
				storeToGlobal(path+i, buffer);
				lat = wLog.end(1);
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (lat >= 0) {
				wLats.put(lat, 1);
			}
		}

		int durSec = (int)(System.currentTimeMillis() - st)/1000;

		double tput = 1.0 * objSizeBytes * numObjs / (1024*1024.0) / durSec;

		Result wRes = new Result(numObjs, numObjs/durSec, tput, wLats.min(), wLats.max(), wLats, new Accumulator(1), numObjs/durSec);

		return wRes;
	}

	public Result downloadTest(int objSizeBytes, int numObjs) {
		ByteBuffer buffer = ByteBuffer.allocate(objSizeBytes);
		AccumulatorMap wLats = new AccumulatorMap(1000);
		LatHistogram wLog = new LatHistogram(TimeUnit.MICROSECONDS, "S3 upload lat", 50, 100000);

		long st = System.currentTimeMillis();
		String path = "test/"+id+"/";
		for (int i=0; i<numObjs; i++) {
			buffer.limit(buffer.capacity());
			buffer.position(0);
			int lat = -1;
			try {
				wLog.start();
				storeToGlobal(path+i, buffer);
				lat = wLog.end(1);
			} catch (IOException e) {
				e.printStackTrace();
			}

			if (lat >= 0) {
				wLats.put(lat, 1);
			}
		}

		int durSec = (int)(System.currentTimeMillis() - st)/1000;

		double tput = 1.0 * objSizeBytes * numObjs / (1024*1024.0) / durSec;

		Result wRes = new Result(numObjs, numObjs/durSec, tput, wLats.min(), wLats.max(), wLats, new Accumulator(1), numObjs/durSec);

		return wRes;
	}

	private void download(long rangeStart, long rangeEnd, String path, ByteBuffer dstBuf) throws IOException {
		GetObjectRequest getReq = new GetObjectRequest(rootBucket, path);
		getReq.setRange(rangeStart, rangeEnd);

		int retries = 3;
		Random rand = new Random();

		while(retries-- > 0) {
			try (
					S3Object obj = client.getObject(getReq); // client is an S3 client
					S3ObjectInputStream is = obj.getObjectContent();
					ReadableByteChannel chan = Channels.newChannel(new BufferedInputStream(is));) {

				int loaded = readFromChannel(chan, dstBuf);
				break;
			} catch (SdkBaseException | IOException ae) { // If the block does not exist in S3, it throws NoSucKey error code
				if (ae instanceof AmazonS3Exception) {
					if (((AmazonS3Exception)ae).getErrorCode().equals("NoSuchKey")) {
						throw new IOException("S3 NoSuckKey: " + path);
					}
				}

				if (retries == 1)
					throw new IOException(", failed to complete the request after retires");

				try {
					long sleepMs = (100+(Math.abs(rand.nextLong())%400));
					System.out.println(String.format("[S3] Load from the global store failed for %s, retrying in %d ms...",
							path,sleepMs));
					Thread.sleep(sleepMs);
				} catch (InterruptedException e) {
					//throw new KawkabException(e);
					return;
				}
			}
		}

	}

	public int readFromChannel(ReadableByteChannel channel, ByteBuffer dstBuffer) throws IOException {
		int readNow;
		int totalRead = 0;

		while(dstBuffer.remaining() > 0) {
			readNow = channel.read(dstBuffer);
			if (readNow == -1)
				break;

			totalRead += readNow;
		}

		return totalRead;
	}

	private void storeToGlobal(String path, final ByteBuffer buffer) throws IOException {
		int length = buffer.remaining();

		try (InputStream istream = new ByteBufferInputStream(buffer)) {
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(length);
			metadata.setContentType(contentType);
			try {

				client.putObject(rootBucket, path, istream, metadata);
			} catch (AmazonServiceException ase) {
				System.out.println("[S3] Failed to upload block: " + id);
				throw ase;
			}
		} catch (IOException e) {
			throw e;
		}

		//if (id.type() == BlockID.BlockType.DATA_SEGMENT && ((DataSegmentID)id).inumber() == 0)
		//System.out.println("\t[S3] >>> Finished store to global: " + id + " rec0TS: " + buffer.getLong(0));
	}

	private AmazonS3 newS3Client(String accessKey, String secretKey, String sip) {
		AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
		ClientConfiguration clientConfiguration = new ClientConfiguration();
		clientConfiguration.setSignerOverride("AWSS3V4SignerType"); //API signature S3v4. Depends on the minio API signature

		System.out.printf("[S3] Connecting to %s\n", sip);

		AmazonS3 client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(credentials))
				.withClientConfiguration(clientConfiguration)
				.withPathStyleAccessEnabled(true)
				.withForceGlobalBucketAccessEnabled(true)
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(sip, "ca-central-1"))
				.build();
		return client;
	}

	private void createRootBucket() {
		try {
			client.createBucket(rootBucket);
		} catch(AmazonS3Exception e) {
			if (!e.getErrorCode().equals("BucketAlreadyOwnedByYou")) {
				e.printStackTrace();
			}
		}
	}

	private void listExistingBuckets() {
		List<Bucket> buckets = client.listBuckets();

		System.out.println("Number of existing buckets: " + buckets.size());
		for(Bucket b : buckets) {
			System.out.println(b.getName());
		}
	}

	public void shutdown() {
		if (!working)
			return;

		printStats();
		working = false;
		System.out.println("Closing S3 backend ...");
		client.shutdown();
	}

	public void printStats() {
		//System.out.printf("[S3] Upload rates (bytes per sec): "); ulRateLog.printPairs();
		//System.out.printf("[S3] Download rates (bytes per sec):"); dlRateLog.printPairs();
	}

}
