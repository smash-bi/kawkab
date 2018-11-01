package kawkab.fs.core;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import java.util.Random;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkBaseException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import kawkab.fs.commons.Constants;
import kawkab.fs.core.BlockID.BlockType;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public final class S3Backend implements GlobalBackend{
	private AmazonS3 client;
	private static final String rootBucket = "kawkab-blocks"; //Cannot contain uppercase letters.
	private static final String contentType = "application/octet-stream";
	
	public S3Backend() {
		client = newS3Client();
		createRootBucket();
		listExistingBuckets();
	}
	
	@Override
	public void loadFromGlobal(final Block dstBlock) throws FileNotExistException, KawkabException {
		long rangeStart = 0;
		long rangeEnd = dstBlock.sizeWhenSerialized() - 1; //end range is inclusive
		
		BlockID id = dstBlock.id();
		if (id.type() == BlockType.DATA_SEGMENT) { //If it's dataSegment, it can be any segment in the block. GlobalStore has complete blocks.
			int segmentInBlock = ((DataSegmentID)id).segmentInBlock();
			rangeStart = segmentInBlock * dstBlock.sizeWhenSerialized();
			rangeEnd = rangeStart + dstBlock.sizeWhenSerialized() - 1; //end range is inclusive
		}
		
		//System.out.println("[S3] Loading block: " + id.localPath() + ": " + rangeStart + " to " + rangeEnd);
		
		String path = id.localPath();
		GetObjectRequest getReq = new GetObjectRequest(rootBucket, path);
		getReq.setRange(rangeStart, rangeEnd);
		
		S3Object obj = null;
		int retries = 3;
		Random rand = new Random();
		try {
			while(retries-- > 0) {
				try {
					obj = client.getObject(getReq); // client is an S3 client
					break;
				} catch (SdkBaseException ae) { // If the block does not exist in S3, it throws NoSucKey error code
					if (ae instanceof AmazonS3Exception) {
						
						if (((AmazonS3Exception)ae).getErrorCode().equals("NoSuchKey")) {
							throw new FileNotExistException("S3 NoSuckKey: " + path);
						}
					}
					
					if (retries == 1)
						throw new KawkabException(ae);
					try {
						long sleepMs = (100+(Math.abs(rand.nextLong())%400));
						System.out.println(String.format("[S3] Load from the global store failed for %s, retyring in %d ms...",
								dstBlock.id().toString(),sleepMs));
						Thread.sleep(sleepMs);
					} catch (InterruptedException e) {
						throw new KawkabException(e);
					}
				}
			}
			
			S3ObjectInputStream is = obj.getObjectContent();
			ReadableByteChannel chan = Channels.newChannel(new BufferedInputStream(is));
			
			try {
				dstBlock.loadFrom(chan);
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					chan.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		} finally {
			if (obj != null)
				try {
					obj.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
		
		
		//TODO: Verify that the block does not exist locally. If it exists, should we overwrite the block?
		//File file = new File(dstBlock.localPath());
		//client.getObject(getReq, file);
		
		//System.out.println("[S3] Loading from global: " + id.name());
	}
	
	@Override
	public void storeToGlobal(final Block srcBlock) throws KawkabException {
		BlockID id = srcBlock.id(); 
		//System.out.println("[S3] Storing to global: " + id.localPath());
		
		String path = id.localPath();
		File file = new File(path);
		InputStream inStream = null;
		try {
			inStream = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			throw new KawkabException(e);
		}
		
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(file.length());
		metadata.setContentType(contentType);
		
		try {
			client.putObject(rootBucket, path, inStream, metadata);
		} catch (AmazonServiceException ase) {
			System.out.println("Failed to upload block: " + srcBlock.id());
			throw ase;
		}
		
		//System.out.println("\t[S3] >>> Finished store to global: " + id.localPath());   
	}
	
	private AmazonS3 newS3Client() {
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
	
	@Override
	public void shutdown() {
		System.out.println("Closing S3 backend ...");
		//client.shutdown();
	}
}
