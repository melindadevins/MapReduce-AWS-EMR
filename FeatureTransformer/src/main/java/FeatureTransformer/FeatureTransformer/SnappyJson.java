/*
 * https://github.com/dain/snappy
 */

//Melinda: test file

package FeatureTransformer.FeatureTransformer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.util.GenericOptionsParser;
import org.xerial.snappy.Snappy;

//import org.xerial.snappy.SnappyCodec;
//import org.xerial.snappy.SnappyInputStream;
//import org.xerial.snappy.SnappyOutputStream;

import com.amazonaws.services.s3.AmazonS3;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class SnappyJson {

	public static void main(String[] args) {

		try {
			String input = "Hello snappy-java! Snappy-java is a JNI-based wrapper of "
					+ "Snappy, a fast compresser/decompresser.";
			byte[] compressed = Snappy.compress(input.getBytes("UTF-8"));
			byte[] uncompressed = Snappy.uncompress(compressed);

			String result = new String(uncompressed, "UTF-8");
			System.out.println(result);

			
			/*
			if (args.length != 2) {
				System.err.println("Usage: FeatureReducer<input_file><output_directory");
				System.exit(2);
			}
			*/


			/*
			 * String fileName = "foo.snap";
			 * 
			 * // write a snappy compressed file try (OutputStream os = new
			 * SnappyOutputStream(new FileOutputStream(fileName))) {
			 * os.write("Hello Snappy-World".getBytes(Charset.defaultCharset()))
			 * ; }
			 */

			/*
			 * String fileName = args[0]; // read a snappy compressed file try
			 * (InputStream is = new SnappyInputStream(new
			 * FileInputStream(fileName))) { byte[] bytes = new byte[100];
			 * is.read(bytes); System.out.println(new String(bytes,
			 * Charset.defaultCharset())); }
			 * 
			 * // check if the file is compressed with the snappy algorithm try
			 * (InputStream is = new FileInputStream(fileName)) { SnappyCodec
			 * readHeader = SnappyCodec.readHeader(is); if
			 * (readHeader.isValidMagicHeader()) {
			 * System.out.println("is a Snappy compressed file");
			 * System.out.printf("%s: %d%n%s: %d%n", "compatible version",
			 * readHeader.compatibleVersion, "version", readHeader.version ); }
			 * else { System.out.println("is not a Snappy compressed file"); } }
			 * 
			 */
		} catch (IOException ex) {
			System.exit(1);
		}
	}

	/*
	 * public void decompressFile(String inputFilePath, String outputFilePath)
	 * throws IOException {
	 * 
	 * 
	 * //https://howtodoinjava.com/java-7/nio/3-ways-to-read-files-using-java-
	 * nio/ RandomAccessFile aFile = new RandomAccessFile(inputFilePath, "r");
	 * FileChannel inChannel = aFile.getChannel(); MappedByteBuffer input =
	 * inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
	 * input.load(); for (int i = 0; i < input.limit(); i++) {
	 * System.out.print((char) input.get()); } //input.clear(); // do something
	 * with the data and clear/compact it. inChannel.close(); aFile.close();
	 * 
	 * //https://www.programcreek.com/java-api-examples/?api=org.xerial.snappy.
	 * Snappy
	 * //https://www.programcreek.com/java-api-examples/?code=s-store/sstore-
	 * soft/sstore-soft-master/src/frontend/org/voltdb/utils/CompressionService.
	 * java ByteBuffer output = ByteBuffer.allocateDirect(1024 * 32); final int
	 * uncompressedLength = Snappy.uncompressedLength(input); if
	 * (output.capacity() < uncompressedLength) { output =
	 * ByteBuffer.allocateDirect(Math.max(output.capacity() * 2,
	 * uncompressedLength)); //buffers = new IO Buffers(input, output);
	 * //m_buffers.set(buffers); } output.clear();
	 * 
	 * final int actualUncompressedLength = Snappy.uncompress(input, output);
	 * assert(uncompressedLength == actualUncompressedLength);
	 * 
	 * //byte result[] = new byte[actualUncompressedLength];
	 * //output.get(result);
	 * 
	 * 
	 * //Write decompressed buffer to file //ByteBuffer bbuf =
	 * ByteBuffer.allocate(100); File file = new File(outputFilePath);
	 * 
	 * boolean append = false;
	 * 
	 * FileChannel wChannel = new FileOutputStream(file, append).getChannel();
	 * 
	 * wChannel.write(output);
	 * 
	 * wChannel.close();
	 * 
	 * }
	 */
}
