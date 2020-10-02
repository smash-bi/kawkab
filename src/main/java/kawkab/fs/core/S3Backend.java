package kawkab.fs.core;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkBaseException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import kawkab.fs.commons.Configuration;
import kawkab.fs.core.exceptions.FileNotExistException;
import kawkab.fs.core.exceptions.KawkabException;
import kawkab.fs.utils.Accumulator;
import kawkab.fs.utils.AccumulatorMap;
import org.apache.zookeeper.server.ByteBufferInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.Lock;

public final class S3Backend implements GlobalBackend{
	private AmazonS3 client;
	private static final String rootBucket = "kawkab-blocks"; //Cannot contain uppercase letters.
	//private ByteBuffer buffer;
	//private ByteBuffer bufferWrap;
	private FileLocks fileLocks;
	private static final String contentType = "application/octet-stream";
	private int id;
	private boolean working;
	private AccumulatorMap dlRateLog;
	private AccumulatorMap ulRateLog;
	private ApproximateClock clock;
	private final long startTime;
	
	public S3Backend(int id) {
		this.id = id;
		client = newS3Client();
		working = true;
		createRootBucket();
		listExistingBuckets();
		fileLocks = FileLocks.instance();
		clock = ApproximateClock.instance();
		startTime = clock.currentTime();

		dlRateLog = new AccumulatorMap(10000); //Number of seconds we will run
		ulRateLog = new AccumulatorMap(10000);// FIXME: Use AccumulatorMap instead of fixed length Accumulator

		//Configuration conf = Configuration.instance();
		//buffer = ByteBuffer.allocateDirect((Math.max(conf.dataBlockSizeBytes, conf.inodesBlockSizeBytes)));
		//bufferWrap = ByteBuffer.wrap(buffer);
	}
	
	@Override
	public void loadFromGlobal(final Block dstBlock, final int offset, final int length) throws FileNotExistException, IOException{
		//TODO: Take a ByteBuffer as an input argument. Load the fetched data in the given ByteBuffer instead of calling block.load().

		long rangeStart = offset;
		long rangeEnd = offset + length - 1; //end range is inclusive
		
		//BlockID id = dstBlock.id();
		/*if (id.type() == BlockType.DATA_SEGMENT) { //If it's dataSegment, it can be any segment in the block. GlobalStore has complete blocks.
			int segmentInBlock = ((DataSegmentID)id).segmentInBlock();
			rangeStart = segmentInBlock * dstBlock.sizeWhenSerialized();
			rangeEnd = rangeStart + dstBlock.sizeWhenSerialized() - 1; //end range is inclusive
		}*/
		
		// System.out.printf("\t\t[S3] Loading from GS %s: stIdx=%d, endIdx=%d, len=%d, path=%s\n", dstBlock.id(), rangeStart, rangeEnd, length, dstBlock.id().localPath());
		
		String path = dstBlock.id().localPath();
		GetObjectRequest getReq = new GetObjectRequest(rootBucket, path);
		getReq.setRange(rangeStart, rangeEnd);
		
		int retries = 3;
		Random rand = new Random();
		
		while(retries-- > 0) {
			try (
				S3Object obj = client.getObject(getReq); // client is an S3 client
				S3ObjectInputStream is = obj.getObjectContent();
				ReadableByteChannel chan = Channels.newChannel(new BufferedInputStream(is));) {

				int loaded = dstBlock.loadFrom(chan);

				int elapsed = (int)(clock.currentTime() - startTime)/1000;
				dlRateLog.put(elapsed, loaded);

				break;
			} catch (SdkBaseException | IOException ae) { // If the block does not exist in S3, it throws NoSucKey error code
				if (ae instanceof AmazonS3Exception) {
					if (((AmazonS3Exception)ae).getErrorCode().equals("NoSuchKey")) {
						throw new FileNotExistException("S3 NoSuckKey: " + path);
					}
				}
				
				if (retries == 1)
					throw new IOException(ae.getMessage()+", failed to complete the request after retires");
				
				try {
					long sleepMs = (100+(Math.abs(rand.nextLong())%400));
					 System.out.println(String.format("[S3] Load from the global store failed for %s, retrying in %d ms...",
							dstBlock.id().toString(),sleepMs));
					Thread.sleep(sleepMs);
				} catch (InterruptedException e) {
					//throw new KawkabException(e);
					return;
				}
			}
		}
		
		//System.out.println("[S3] Loading from global: " + id.name());
	}

	private double rateMBps(int numBytes, int durMillis) {
		double durSec = durMillis / 1000.0;
		return numBytes/1048576.0/durSec;
	}

	@Override
	public void bulkLoadFromGlobal(final BlockLoader bl) throws FileNotExistException, IOException{
		//TODO: Take a ByteBuffer as an input argument. Load the fetched data in the given ByteBuffer instead of calling block.load().

		long rangeStart = bl.offset();
		long rangeEnd = rangeStart + bl.length() - 1; //end range is inclusive

		// System.out.printf("\t\t[S3] Loading from GS %s: stIdx=%d, endIdx=%d, len=%d, path=%s\n", dstBlock.id(), rangeStart, rangeEnd, length, dstBlock.id().localPath());

		String path = bl.blockPath();
		GetObjectRequest getReq = new GetObjectRequest(rootBucket, path);
		getReq.setRange(rangeStart, rangeEnd);

		int retries = 3;
		Random rand = new Random();

		//System.out.printf("[S3 Bulk load: %s. start=%d, end=%d, len=%d\n", bl.blockPath(), rangeStart, rangeEnd, rangeEnd-rangeStart+1);

		while(retries-- > 0) {
			try (
					S3Object obj = client.getObject(getReq); // client is an S3 client
					S3ObjectInputStream is = obj.getObjectContent();
					ReadableByteChannel chan = Channels.newChannel(new BufferedInputStream(is));
			) {
				int read = bl.loadFrom(chan);

				int elapsed = (int)(clock.currentTime() - startTime)/1000;

				dlRateLog.put(elapsed, read);

				assert read == rangeEnd - rangeStart + 1 :
						String.format("[S3] Not all bytes read from channel: read=%d, expected=%d",read, rangeEnd-rangeStart+1);
				break;
			} catch (SdkBaseException | IOException ae) { // If the block does not exist in S3, it throws NoSucKey error code
				if (ae instanceof AmazonS3Exception) {
					if (((AmazonS3Exception)ae).getErrorCode().equals("NoSuchKey")) {
						throw new FileNotExistException("S3 NoSuckKey: " + path);
					}
				}

				if (retries == 1)
					throw new IOException(ae.getMessage()+", failed to complete the request after retires");

				try {
					long sleepMs = (100+(Math.abs(rand.nextLong())%400));
					System.out.println(String.format("[S3] Load from the global store failed for %s, retrying in %d ms...",
							bl.blockPath(), sleepMs));
					Thread.sleep(sleepMs);
				} catch (InterruptedException e) {
					return;
				}
			}
		}

		//System.out.println("[S3] Loading from global: " + id.name());
	}
	
	@Override
	public void storeToGlobal(final BlockID id, final ByteBuffer buffer) throws KawkabException {
		if (id.type == BlockID.BlockType.INDEX_BLOCK)
			System.out.println("[S3] Storing to global: " + id.localPath());

		int length = 0;
		try(
				RandomAccessFile raf = new RandomAccessFile(id.localPath(), "r");
				FileChannel chan = raf.getChannel();
            ) {

			length = (int)raf.length(); //Block size in Kawkab is an integer

			Lock lock = fileLocks.grabLock(id);
			try {
				lock.lock();
				//System.out.printf("\t\t[S3] Reading %d bytes from %s for storing in global: path=%s\n",length, srcBlock.id(), srcBlock.id().localPath());
				//raf.readFully(buffer, 0, length);


				chan.position(0);
				buffer.clear();
				chan.read(buffer);
				buffer.flip();
			} finally {
				lock.unlock();
			}
		} catch (IOException e) {
			System.out.println("[S3] Unable to upload to S3: " + id);
			e.printStackTrace();
			throw new KawkabException(e);
		}

		try (InputStream istream = new ByteBufferInputStream(buffer)) {
			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(length);
			metadata.setContentType(contentType);
			try {

				client.putObject(rootBucket, id.localPath(), istream, metadata);

				int elapsed = (int)(clock.currentTime() - startTime)/1000;
				ulRateLog.put(elapsed, length);

				//System.out.printf("[S3] Upload %d bps at %d\n", ulBps, elapsed);

			} catch (AmazonServiceException ase) {
				System.out.println("Failed to upload block: " + id);
				throw ase;
			}
		} catch (IOException e) {
			throw new KawkabException(e);
		}

		//if (id.type() == BlockID.BlockType.DATA_SEGMENT && ((DataSegmentID)id).inumber() == 0)
			//System.out.println("\t[S3] >>> Finished store to global: " + id + " rec0TS: " + buffer.getLong(0));
	}
	
	private AmazonS3 newS3Client() {
		Configuration conf = Configuration.instance();
		
		AWSCredentials credentials = new BasicAWSCredentials(conf.minioAccessKey, conf.minioSecretKey);
		ClientConfiguration clientConfiguration = new ClientConfiguration();
	    clientConfiguration.setSignerOverride("AWSS3V4SignerType"); //API signature S3v4. Depends on the minio API signature

		System.out.printf("[S3] Connecting to %s\n", conf.minioServers[id]);
	    
		AmazonS3 client = AmazonS3ClientBuilder.standard()
							.withCredentials(new AWSStaticCredentialsProvider(credentials))
							.withClientConfiguration(clientConfiguration)
							.withPathStyleAccessEnabled(true)
							.withForceGlobalBucketAccessEnabled(true)
							.withEndpointConfiguration(new EndpointConfiguration(conf.minioServers[id], "ca-central-1"))
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
		if (!working)
			return;

		printStats();
		working = false;
		System.out.println("Closing S3 backend ...");
		client.shutdown();
	}

	public void printStats() {
		System.out.printf("[S3] S3 backend start time ..., %s\n", new SimpleDateFormat("HH:mm:ss.SSS").format(new Date(startTime)));
		System.out.printf("[S3] Upload rates (bytes per sec): "); ulRateLog.printPairs();
		System.out.printf("[S3] Download rates (bytes per sec):"); dlRateLog.printPairs();
	}

	@Override
	public AccumulatorMap getUploadStats() {
		return ulRateLog;
	}

	@Override
	public AccumulatorMap getDownloadStats() {
		return dlRateLog;
	}
}
