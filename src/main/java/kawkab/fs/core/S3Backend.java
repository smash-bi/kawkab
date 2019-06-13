package kawkab.fs.core;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
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

import kawkab.fs.commons.Configuration;
import kawkab.fs.core.BlockID.BlockType;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;

public final class S3Backend implements GlobalBackend{
	private AmazonS3 client;
	private static final String rootBucket = "kawkab-blocks"; //Cannot contain uppercase letters.
	private byte[] buffer;
	private FileLocks fileLocks;
	private static final String contentType = "application/octet-stream";
	
	public S3Backend() {
		client = newS3Client();
		createRootBucket();
		listExistingBuckets();
		fileLocks = FileLocks.instance();
		
		Configuration conf = Configuration.instance();
		buffer = new byte[(Math.max(conf.dataBlockSizeBytes, conf.inodesBlockSizeBytes))];
	}
	
	@Override
	public void loadFromGlobal(final Block dstBlock) throws FileNotExistException, KawkabException {
		long rangeStart = 0;
		long rangeEnd = dstBlock.sizeWhenSerialized() - 1; //end range is inclusive
		
		BlockID id = dstBlock.id();
		if (id.type() == BlockType.DATA_SEGMENT) { //If it's dataSegment, it can be any segment in the block. GlobalStore has complete blocks.
			int segmentInBlock = ((DataSegmentID)id).segmentInBlock();
			rangeStart = segmentInBlock * rangeEnd;
			rangeEnd = rangeStart + rangeEnd - 1; //end range is inclusive
		}
		
		//System.out.println("[S3] Loading block: " + id.localPath() + ": " + rangeStart + " to " + rangeEnd);
		
		String path = id.localPath();
		GetObjectRequest getReq = new GetObjectRequest(rootBucket, path);
		getReq.setRange(rangeStart, rangeEnd);
		
		int retries = 3;
		Random rand = new Random();
		
		while(retries-- > 0) {
			try (
					S3Object obj = client.getObject(getReq); // client is an S3 client
					S3ObjectInputStream is = obj.getObjectContent();
					ReadableByteChannel chan = Channels.newChannel(new BufferedInputStream(is));
				) {
					dstBlock.loadFrom(chan);
				break;
			} catch (SdkBaseException | IOException ae) { // If the block does not exist in S3, it throws NoSucKey error code
				if (ae instanceof AmazonS3Exception) {
					
					if (((AmazonS3Exception)ae).getErrorCode().equals("NoSuchKey")) {
						throw new FileNotExistException("S3 NoSuckKey: " + path);
					}
				}
				
				if (retries == 1)
					throw new KawkabException(ae);
				
				try {
					long sleepMs = (100+(Math.abs(rand.nextLong())%400));
					// System.out.println(String.format("[S3] Load from the global store failed for %s, retyring in %d ms...",
					//		dstBlock.id().toString(),sleepMs));
					Thread.sleep(sleepMs);
				} catch (InterruptedException e) {
					throw new KawkabException(e);
				}
			}
		}
		
		//System.out.println("[S3] Loading from global: " + id.name());
	}
	
	@Override
	public void storeToGlobal(final Block srcBlock) throws KawkabException {
		//System.out.println("[S3] Storing to global: " + id.localPath());
		
		int length = 0;
		try(
				RandomAccessFile raf = new RandomAccessFile(srcBlock.id().localPath(), "r");
            ) {
			
			length = (int)raf.length(); //Block size in Kawkab is an integer
			
			try {
				fileLocks.lockFile(srcBlock.id());
				raf.readFully(buffer, 0, length);		
			} catch (InterruptedException e) {
				throw new KawkabException(e);
			} finally {
				fileLocks.unlockFile(srcBlock.id());
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new KawkabException(e);
		}
		
		try (InputStream istream = new ByteArrayInputStream(buffer, 0, length)) {
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(length);
			metadata.setContentType(contentType);
			try {
				client.putObject(rootBucket, srcBlock.id().localPath(), istream, metadata);
			} catch (AmazonServiceException ase) {
				System.out.println("Failed to upload block: " + srcBlock.id());
				throw ase;
			}
		} catch (IOException e) {
			throw new KawkabException(e);
		}
		
		
		//System.out.println("\t[S3] >>> Finished store to global: " + id);   
	}
	
	private AmazonS3 newS3Client() {
		Configuration conf = Configuration.instance();
		
		AWSCredentials credentials = new BasicAWSCredentials(conf.minioAccessKey, conf.minioSecretKey);
		ClientConfiguration clientConfiguration = new ClientConfiguration();
	    clientConfiguration.setSignerOverride("AWSS3V4SignerType"); //API signature S3v4. Depends on the minio API signature
	    
		AmazonS3 client = AmazonS3ClientBuilder.standard()
							.withCredentials(new AWSStaticCredentialsProvider(credentials))
							.withClientConfiguration(clientConfiguration)
							.withPathStyleAccessEnabled(true)
							.withForceGlobalBucketAccessEnabled(true)
							.withEndpointConfiguration(new EndpointConfiguration(conf.minioServers[0], "ca-central-1"))
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
		client.shutdown();
	}
}
