package kawkab.fs.core;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.concurrent.ExecutionException;
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
import com.amazonaws.services.s3.model.S3Object;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.BlockID.BlockType;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public class S3Backend extends GlobalProcessor {
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
	
	public static synchronized S3Backend instance() {	
		if (instance == null) {
			instance = new S3Backend();
		}
		
		return instance;
	}

	@Override
	public void load(Block destBlock) throws FileNotExistException, KawkabException {
		Future<?> future = loadWorkers.submit(() -> loadFromGlobal(destBlock));
		try {
			if (future.get() != null) { //Returns null on success
				throw new KawkabException("Unbale to load block from the global store. Block: " + destBlock.id().name());
			}
		} catch (ExecutionException e) {
			Throwable w = e.getCause();
			if (w instanceof AmazonS3Exception) {
				AmazonS3Exception ae = (AmazonS3Exception)w;
				if (ae.getErrorCode().equals("NoSuchKey")) {
					throw new FileNotExistException();
				} else {
					throw new KawkabException(ae);
				}
			} else {
				throw new KawkabException(e);
			}
		} catch (InterruptedException e) {
			throw new KawkabException(e);
		}
	}
	
	@Override
	public void store(Block srcBlock) throws KawkabException {
		System.out.println("[GS] S3 store block: " + srcBlock.id().name());
		Future<?> future = storeWorkers.submit(() -> storeToGlobal(srcBlock));
		try {
			if (future.get() != null) { //Returns null on success
				throw new KawkabException("Unbale to store block in the global store. Block: " + srcBlock.id().name());
			}
		} catch (ExecutionException e) {
			throw new KawkabException(e);
		} catch (InterruptedException e) {
			throw new KawkabException(e);
		}
	}
	
	private void loadFromGlobal(Block dstBlock) {
		long rangeStart = 0;
		long rangeEnd = dstBlock.sizeWhenSerialized() - 1; //end range is inclusive
		
		BlockID id = dstBlock.id();
		if (id.type == BlockType.DataBlock) { //If it's dataSegment, it can be any segment in the block. GlobalStore has complete blocks.
			int segmentInBlock = ((DataSegmentID)id).segmentInBlock();
			rangeStart = segmentInBlock * dstBlock.sizeWhenSerialized();
			rangeEnd = rangeStart + dstBlock.sizeWhenSerialized() - 1; //end range is inclusive
		}
		
		System.out.println("[GS] S3 loading block: " + id.localPath() + ": " + rangeStart + " to " + rangeEnd);
		
		String path = id.localPath();
		GetObjectRequest getReq = new GetObjectRequest(rootBucket, path);
		getReq.setRange(rangeStart, rangeEnd);
		
		S3Object obj = client.getObject(getReq);
		ReadableByteChannel chan = Channels.newChannel(new BufferedInputStream(obj.getObjectContent()));
		
		try {
			dstBlock.loadFrom(chan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//TODO: Verify that the block does not exist locally. If it exists, should we overwrite the block?
		//File file = new File(dstBlock.localPath());
		//client.getObject(getReq, file);
		
		System.out.println("[GS] Finished loading from global: " + id.name());
	}
	
	private void storeToGlobal(Block srcBlock) {
		BlockID id = srcBlock.id(); 
		System.out.println("[GS] Storing to global: " + id.localPath());
		
		String path = id.localPath();
		File file = new File(path);
		client.putObject(rootBucket, path, file);
		
		System.out.println("\t[GS] >>> Finished store to global: " + id.localPath());
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
