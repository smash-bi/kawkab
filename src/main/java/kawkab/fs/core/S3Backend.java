package kawkab.fs.core;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;

import kawkab.fs.commons.Constants;

public class S3Backend implements GlobalProcessor {
	private AmazonS3 client;
	private static S3Backend instance;
	private static final String rootBucket = "kawkab-blocks"; //Cannot contain uppercase letters.
	
	private ExecutorService loadWorkers;
	private ExecutorService storeWorkers;
	
	private S3Backend() {
		client = getClient();
		createRootBucket();
		listExistingBuckets();
		loadWorkers = Executors.newFixedThreadPool(Constants.numGlobalStoreLoadWorkers);
		storeWorkers = Executors.newFixedThreadPool(Constants.numGlobalStoreStoreWorkers);
	}
	
	public static S3Backend instance() {	
		if (instance == null) {
			instance = new S3Backend();
		}
		
		return instance;
	}

	@Override
	public Future<?> load(Block destBlock) throws IOException {
		return loadWorkers.submit(() -> loadFromGlobal(destBlock));
	}
	
	@Override
	public Future<?> store(Block srcBlock) throws IOException {
		System.out.println("[GS] S3 store block: " + srcBlock.name());
		return storeWorkers.submit(() -> storeToGlobal(srcBlock));
	}
	
	private void loadFromGlobal(Block dstBlock) {
		System.out.println("[GS] S3 loading block: " + dstBlock.name());
		
		//TODO: Verify that the block does not exist locally. If it exists, should we overwrite the block?
		
		File file = new File(dstBlock.localPath());
		
		GetObjectRequest getReq = new GetObjectRequest(rootBucket, dstBlock.name());
		client.getObject(getReq, file);
	}
	
	private void storeToGlobal(Block srcBlock) {
		System.out.println("[GS] Storing to global: " + srcBlock.name());
		
		File file = new File(srcBlock.localPath());
		client.putObject(rootBucket, srcBlock.name(), file);
		
		System.out.println("\t[GS] >>> Finished store to global: " + srcBlock.name());
	}
	
	private AmazonS3 getClient() {
		AWSCredentials credentials = new BasicAWSCredentials(Constants.minioAccessKey,
				Constants.minioSecretKey);
		ClientConfiguration clientConfiguration = new ClientConfiguration();
	    clientConfiguration.setSignerOverride("AWSS3V4SignerType"); //API signature S3v4. Depends on the minio API signature
	    
		AmazonS3 client = AmazonS3ClientBuilder.standard()
							.withCredentials(new AWSStaticCredentialsProvider(credentials))
							.withClientConfiguration(clientConfiguration)
							.withPathStyleAccessEnabled(true)
							.withForceGlobalBucketAccessEnabled(true)
							.withEndpointConfiguration(new EndpointConfiguration(Constants.minioServers[0], "ca-central-1"))
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
	
	public void stop() {
		System.out.println("Closing S3 backend ...");
		
		for (ExecutorService workers : new ExecutorService[]{loadWorkers, storeWorkers}) {
			if (workers == null)
				continue;
			
			workers.shutdown();
			
			try {
				while (!workers.awaitTermination(5, TimeUnit.SECONDS)) { //Wait until all workers finish their work.
					System.err.println("S3 backend: Unable to close all worker threads.");
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				workers.shutdownNow();
			}
		}
	}
}
