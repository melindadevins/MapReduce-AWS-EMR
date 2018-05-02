package FeatureTransformer.FeatureTransformer;

import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;
import org.xerial.snappy.Snappy;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

//Melinda: test file
import java.io.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
//import com.elliemae.core.model.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
//import org.xerial.snappy.SnappyInputStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.amazonaws.util.IOUtils;
/*
import com.elliemae.crypto.DataKey;
import com.elliemae.crypto.DecryptRequest;
import com.elliemae.crypto.DecryptResult;
*/

//https://github.com/awsdocs/aws-doc-sdk-examples/tree/master/java/example_code/s3/src/main/java/aws/example/s3
//A thread safe singleton class
public class AWSS3Client {

	private static AWSS3Client instance;
	private final static Logger logger = Logger.getLogger(AWSS3Client.class);

	private AWSS3Client() {

	}

	// To avoid cost associated with the synchronized method after the instance
	// is created,
	// use double checked locking principle.
	public static AWSS3Client getInstance() {
		if (instance == null) {
			synchronized (AWSS3Client.class) {
				if (instance == null) {
					instance = new AWSS3Client();
				}
			}
		}
		return instance;
	}
	public String getJsonFromS3Json(String bucket_name, String s3Object_key) throws IOException {
		System.out.println("getJsonFromS3Json");
		final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
		S3Object s3object = s3.getObject(new GetObjectRequest(bucket_name, s3Object_key));

		String contentType = s3object.getObjectMetadata().getContentType();
		long contentLen = s3object.getObjectMetadata().getContentLength();
 
		S3ObjectInputStream s3InputStream = s3object.getObjectContent();
		// Process the objectData stream.
		byte[] read_buf = new byte[(int) contentLen];

		s3InputStream.read(read_buf);

		s3InputStream.close();

		String s3json = new String(read_buf);
		

		return s3json;
	
	}
	public String getJsonFromS3Snappy(String bucket_name, String s3Object_key) throws IOException {
		System.out.println("getJsonFromS3Snappy");

		final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
		S3Object s3object = s3.getObject(new GetObjectRequest(bucket_name, s3Object_key));

		String contentType = s3object.getObjectMetadata().getContentType();
		long contentLen = s3object.getObjectMetadata().getContentLength();
 
		S3ObjectInputStream s3InputStream = s3object.getObjectContent();
		// Process the objectData stream.
		byte[] read_buf = new byte[(int) contentLen];

		s3InputStream.read(read_buf);

		byte[]  uncompressed = Snappy.uncompress(read_buf);
		
		
		String s3json = new String(uncompressed);
		s3InputStream.close();
		
		return s3json;

		
		
		
		// Test snappy lib code
		/*
		System.out.println("getJsonFromS3 test Snappy");
		String input_test = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
				+ "Snappy, a fast compresser/decompresser.";
		byte[] compressed_test = Snappy.compress(input_test.getBytes("UTF-8"));
		byte[] uncompressed_test = Snappy.uncompress(compressed_test);

		String result_test = new String(uncompressed_test, "UTF-8");
		System.out.println(result_test);
*/
		/*
		S3Object s3object = s3.getObject(new GetObjectRequest(bucket_name,s3Object_key));
	    InputStream inContent = s3object.getObjectContent();
	    System.err.println("1");
	    CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(SnappyCodec.class, new Configuration());
	    System.err.println("2");
	    InputStream inStream = codec.createInputStream(new BufferedInputStream(inContent));
	    System.err.println("3");
	    InputStreamReader  inRead = new InputStreamReader(inStream);
	    BufferedReader br = new BufferedReader(inRead);
	    System.err.println("4");
	    String line=null;
	    while ((line = br.readLine()) != null){
	        System.out.println(line);
	    }   
	    return line;
		// end of test snappy lib code
		*/
		
		
	    /*
		if (!s3.doesBucketExist(bucket_name)) {
			System.err.println(String.format("S3 bucket {%s} is not found", bucket_name));
		}
		if (!s3.doesObjectExist(bucket_name, s3Object_key)) {
			System.err.println(String.format("{%s} is not found", s3Object_key));
		}

		S3Object s3object = s3.getObject(new GetObjectRequest(bucket_name, s3Object_key));

		String contentType = s3object.getObjectMetadata().getContentType();
		long contentLen = s3object.getObjectMetadata().getContentLength();
		System.out.printf("contentType=%s, contentLen=%d\n", contentType, contentLen);
		
		S3ObjectInputStream s3InputStream = s3object.getObjectContent();
		// Process the objectData stream.
		byte[] read_buf = new byte[(int) contentLen];

		s3InputStream.read(read_buf);
		s3InputStream.close();
		
		final int uncompressedLength = Snappy.uncompressedLength(read_buf);
		System.out.printf("uncompressedLength=%d\n", uncompressedLength);
		
		//Test decompress
		ByteBuffer input = ByteBuffer.wrap(read_buf);

		ByteBuffer output = ByteBuffer.allocateDirect((int)contentLen * 2 ); //1024 * 32
        output = ByteBuffer.allocateDirect(Math.max(output.capacity() * 2, uncompressedLength));
		output.clear();

	    final int actualUncompressedLength = Snappy.uncompress(input, output);
	    System.out.printf("actual uncompressed length = %d", actualUncompressedLength);
	    assert(uncompressedLength == actualUncompressedLength);

	    byte uncompressed[] = new byte[actualUncompressedLength];
	    output.get(uncompressed);
	    String result = new String(uncompressed, "UTF-8");
	    System.out.println(result);
		return result;
	    
	    //end test decompress
	*/
	    /* real code
		byte[] uncompressed = Snappy.uncompress(read_buf);
		
		Syste.out.printf("actual uncompressed length = %d", )
		
		String result = new String(uncompressed, "UTF-8");
		System.out.println(result);
		return result;
		
		*/
	}
	
	public String[] getBucketAndPrefix(String s3UriPath) {
		System.out.printf("s3UriPath = %s\n", s3UriPath);
		AmazonS3URI s3Uri = new AmazonS3URI(s3UriPath);
		String s3BucketName = s3Uri.getBucket();
		String prefix = s3Uri.getKey();
		System.out.printf("s3BucketName = %s; prefix = %s\n", s3BucketName, prefix);

		return new String[] {s3BucketName, prefix};
	}
	
	public void putS3Object(String s3Uri, String key_name, String content) {
			//throws AmazonServiceException, SdkClientException {
		String bucket_name = getBucketAndPrefix(s3Uri)[0];
		String prefix = getBucketAndPrefix(s3Uri)[1];

	
		final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

		s3.putObject(bucket_name, prefix + "/" + key_name, content);
	}

	
	public List<String> listS3Objects(String s3BucketName, String prefix) {
		final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
		List<String> s3ObjectsKeys = new ArrayList<String>();
	
		ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(s3BucketName).withPrefix(prefix).withDelimiter("/");
		ListObjectsV2Result listing = s3.listObjectsV2(req);
		for (String commonPrefix : listing.getCommonPrefixes()) {
		        System.out.println(commonPrefix);
		}
		for (S3ObjectSummary summary: listing.getObjectSummaries()) {
		    
		    System.out.printf("Object with key '%s'\n", summary.getKey());
			s3ObjectsKeys.add(summary.getKey());
		}
		
		
		/*
		logger.debug(String.format("get all S3 Objects from S3 bucket [%s]", s3BucketName));

		for (S3ObjectSummary summary : S3Objects.inBucket(s3, s3BucketName)) {
			System.out.printf("Object with key '%s'\n", summary.getKey());
			s3ObjectsKeys.add(summary.getKey());
		}
		*/
		
		return s3ObjectsKeys;
	}

	// Write contentList to s3BucketName, s3ObjectName, one line for each string in
	// the contentList string list
	public void putS3ObKeyList(String inputBucketName, String outputPath, String tempOutputKey, List<String> inputS3ObjKeyList) throws IOException {
		String outputBucketName = AWSS3Client.getInstance().getBucketAndPrefix(outputPath)[0];

		File tempFile = File.createTempFile("s3List", ".tmp");
		FileOutputStream fos = new FileOutputStream(tempFile);

		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		for (String content : inputS3ObjKeyList) {
			bw.write(inputBucketName+";");
			bw.write(content+";");
			bw.write(outputPath);
			bw.newLine();
		}

		bw.close();
		final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

		try {
			s3.putObject(outputBucketName, tempOutputKey, tempFile);
		} finally {
			tempFile.delete();
		}
	}

	private ObjectNode addNoArchiveHeader(byte [] uncompressedByte) {
		ObjectNode objectNode =null;
        try {
			ObjectMapper jsonMapper = new ObjectMapper();
			JsonNode node = jsonMapper.readTree(uncompressedByte);
			
            objectNode = (ObjectNode) node;
            //objectNode.put(EventType.JSON_FIELD.toString(), EventType.SKIP_ARCHIVE.toString());
        } catch (Exception e) {
            logger.error("Error occured while adding No Archive Header {}", e);
        }

        return objectNode;
	}
	/*
	 private byte[] byteWiseReadFully(InputStream input)
	            throws IOException
	    {
	        SnappyInputStream sin = new SnappyInputStream(input);
	        ByteArrayOutputStream out = new ByteArrayOutputStream();
	        for (int readData = 0; (readData = sin.read()) != -1; ) {
	            out.write(readData);
	        }
	        out.flush();
	        return out.toByteArray();
	    }
	
	// Get file from S3 bucket
    public String getS3ArchiverData(String bucketName, String s3ObjKey)  {
		final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

    	String loan_data = "";
        long startTime, endTime;
        try {
            startTime = System.currentTimeMillis();
            logger.info("Fetching loan file " + bucketName + "/" + s3ObjKey);
            //byte[] decryptedLoan = SymmSecurityUtil.getEncryptedFromS3(bucketName, loanKey);
            
            byte[] decryptedLoan = getS3Object(s3, bucketName, s3ObjKey);
            InputStream is = new ByteArrayInputStream(decryptedLoan);
            byte[] uncompressBytes = byteWiseReadFully(is);
            ObjectNode objectNode = addNoArchiveHeader(uncompressBytes);
            loan_data = objectNode.toString();
            endTime = System.currentTimeMillis();
            logger.debug("Retrieved loan file in " + (endTime-startTime) + " milliseconds.");
        } catch (Exception e) {
        	 System.out.println(e.getMessage());
            e.printStackTrace();
        }
        return loan_data;
    }
    */
    
/*
    public byte[] getEncByteArrayFromBucket(String bucketName, String key) throws Exception {
    	//AmazonS3 s3client = AmazonS3ClientBuilder.standard().withRegion(region).build();
    	final AmazonS3 s3client = AmazonS3ClientBuilder.defaultClient();
	S3Object object = s3client.getObject(bucketName, key);
	InputStream inStream = object.getObjectContent();
	byte[] output = IOUtils.toByteArray(inStream);
	String encryptedDataKey = object.getObjectMetadata().getUserMetadata().get(DATA_KEY_HEADER);

	byte[] decryptedData = decrypt(output, encryptedDataKey.trim());

	return decryptedData;
    }
	
    
    private byte[] decrypt(byte[] output, String encryptedDataKey) throws Exception {

    	DataKey dataKey = keyMap.get(encryptedDataKey);
    	if (dataKey == null) {
    	    DecryptResult decryptResult = awsKmsClient.decrypt(new DecryptRequest()
    		    .withCiphertextBlob(ByteBuffer.wrap(Base64.getDecoder().decode(encryptedDataKey))));
    	    if (decryptResult == null) {
    		logger.warn("Error getting the key decrypted for value: " + encryptedDataKey);
    		throw new Exception("Error getting the key decrypted for value: " + encryptedDataKey);
    	    }

    	    dataKey = new DataKey(encryptedDataKey, decryptResult.getPlaintext().array());
    	    keyMap.put(encryptedDataKey, dataKey);

    	}

    	dataKey.setLastUsed(System.currentTimeMillis());
    	return dataKey.getCrypto().decrypt(output);

        }
    */
    private byte[] getS3Object(AmazonS3 s3, String bucket_name, String s3Object_key) throws IOException{
    	S3Object s3object = s3.getObject(new GetObjectRequest(bucket_name, s3Object_key));

		String contentType = s3object.getObjectMetadata().getContentType();
		long contentLen = s3object.getObjectMetadata().getContentLength();
 
		S3ObjectInputStream s3InputStream = s3object.getObjectContent();
		// Process the objectData stream.
		byte[] read_buf = new byte[(int) contentLen];

		s3InputStream.read(read_buf);

		s3InputStream.close();
		return read_buf;
    }
	
}