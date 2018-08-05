package NeighborhoodCrimeCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NeighborhoodCrimeCountDriver {

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		// Counts number of crimes per neighborhood
		
		Job job = Job.getInstance(new Configuration(), "Neighborhood Crime Count");
		job.setJarByClass(NeighborhoodCrimeCountDriver.class);
		job.setMapperClass(NeighborhoodCrimeCountMapper.class);
		job.setCombinerClass(NeighborhoodCrimeCountReducer.class);
		job.setReducerClass(NeighborhoodCrimeCountReducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)? 0:1);
	}

}
