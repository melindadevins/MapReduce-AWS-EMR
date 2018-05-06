package FeatureTransformer.FeatureTransformer;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.amazonaws.services.s3.AmazonS3URI;

import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class S3MapperReducer {

	public static class S3PathMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object key, Text value, Context context) {
			System.out.printf("Mapper: value= %s\n", value);
			String[] pathes = value.toString().split(";");
			String inputBucketName = pathes[0];
			String inputObjKey = pathes[1];
			String outputPath = pathes[2];

			System.out.println(pathes[0]);
			System.out.println(pathes[1]);
			System.out.println(pathes[2]);

			String loanJson;
			try {
				String extention = "";
				if (inputObjKey.lastIndexOf(".") != -1 && inputObjKey.lastIndexOf(".") != 0) {
					extention = inputObjKey.substring(inputObjKey.lastIndexOf(".") + 1);
				}

				if (extention.toLowerCase().equals("snappy")) {
					System.out.printf("extention = %s, process snappy file: %s\n", extention, inputObjKey);
					//loanJson = AWSS3Client.getInstance().getS3ArchiverData(inputBucketName, inputObjKey);
					loanJson = AWSS3Client.getInstance().getJsonFromS3Snappy(inputBucketName, inputObjKey);
				} else {
					System.out.printf("extention = %s, process json file: %s\n", extention, inputObjKey);
					loanJson = AWSS3Client.getInstance().readS3ObjectUsingByteArray(inputBucketName, inputObjKey);
				}

				String newData = FeatureSelector.getInstance().selectField(loanJson);

				System.out.printf("Input key=%s, output path=%s \n The json size=%d, newData size=%d \n", inputObjKey, outputPath, loanJson.length(), newData.length());
				System.out.println(newData);
				System.out.println("\n");
				AWSS3Client.getInstance().putS3Object(outputPath, inputObjKey, newData);
				
				/*
				 * StringTokenizer st = new StringTokenizer(value.toString());
				 * Text wordOut = new Text(); IntWritable one = new
				 * IntWritable(1); while(st.hasMoreTokens()) {
				 * wordOut.set(st.nextToken()); context.write(wordOut, one); }
				 */
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.err.println("caught IOException");
				System.err.println(e.getMessage());
				e.printStackTrace();

			} catch (Exception ex) {
				System.err.println("caught Exception");
				System.err.println(ex.getMessage());
				ex.printStackTrace();
			}

		}

	}

	public static class S3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text term, Iterable<IntWritable> ones, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			Iterator<IntWritable> iterator = ones.iterator();
			while (iterator.hasNext()) {
				count++;
				iterator.next();
			}
			IntWritable output = new IntWritable(count);
			context.write(term, output);
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// Controller. Interact with Hadoop info, job
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.err.println("Usage: S3MapperReducer<input_s3path> <output_s3path> ");
			System.exit(2);
		}

		String inputPath = otherArgs[0];
		String outputPath = otherArgs[1];

		String[] inputBucketPrefix = AWSS3Client.getInstance().getBucketAndPrefix(inputPath);
		String inputBucketName = inputBucketPrefix[0];
		String inputPrefix = inputBucketPrefix[1];

		String[] outputBucketPrefix = AWSS3Client.getInstance().getBucketAndPrefix(outputPath);
		String outputBucketName = outputBucketPrefix[0];
		String outputPrefix = outputBucketPrefix[1];

		List<String> s3ObjList = AWSS3Client.getInstance().listS3Objects(inputBucketName, inputPrefix);
		System.out.printf("Find %d object in %s, %s\n", s3ObjList.size(), inputBucketName, inputPrefix);
		String tempKey = "temp/s3List.tmp";
		AWSS3Client.getInstance().putS3ObKeyList(inputBucketName, outputPath, tempKey, s3ObjList);

		Job job = Job.getInstance(conf, "S3MapperReducer");
		job.setJarByClass(S3MapperReducer.class);
		job.setMapperClass(S3PathMapper.class);
		job.setReducerClass(S3Reducer.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//The above code generate a temp file in s3 bucket:  outputBucketName/temp/s3List.tmp
		//To test on EMR, run the code as it is.
		//FileInputFormat.addInputPath(job, new Path("s3://" + outputBucketName + "/" + tempKey));  //run on EMR
		//FileOutputFormat.setOutputPath(job, new Path("s3://" + outputBucketName + "/output/"));
		
		// To test on local, download the outputBucketName/temp/s3List.tmp to local project directory:
		//  ../FeatureTransformer/temp/s3List.tmp
		// and run the following code:
		FileInputFormat.addInputPath(job, new Path(tempKey ));		//run on local
		FileOutputFormat.setOutputPath(job, new Path("output"));

		
		
		boolean status = job.waitForCompletion(true);
		if (status) {
			System.exit(0);
		} else {
			System.exit(1);
		}

	}

}
