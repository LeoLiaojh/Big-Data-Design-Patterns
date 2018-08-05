package CrimeTypeCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimeTypeCountDriver {

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		// Counts number of crimes per crime type
		
		Job job = Job.getInstance(new Configuration(), "Crime Type Count");
		job.setJarByClass(CrimeTypeCountDriver.class);
		job.setMapperClass(CrimeTypeCountMapper.class);
		job.setCombinerClass(CrimeTypeCountReducer.class);
		job.setReducerClass(CrimeTypeCountReducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 0:1);
	}

}
