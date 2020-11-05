package kawkab.fs.testclient;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.zookeeper.server.ByteBufferInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

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

	public double uploadTest(int objSizeBytes, int numObjs) {
		ByteBuffer buffer = ByteBuffer.allocate(objSizeBytes);

		long st = System.currentTimeMillis();
		String path = "test/"+id+"/";
		for (int i=0; i<numObjs; i++) {
			buffer.limit(buffer.capacity());
			buffer.position(0);

			try {
				storeToGlobal(path+i, buffer);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		int durSec = (int)(System.currentTimeMillis() - st)/1000;

		double tput = 1.0 * objSizeBytes * numObjs / (1024*1024.0) / durSec;

		return tput;
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
