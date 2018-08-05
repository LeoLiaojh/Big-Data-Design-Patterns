package NeighborhoodCrimeTypeIndex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NeighborhoodCrimeTypeIndexDriver {

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		// Creates inverted index of neighborhood to crime type.
		// The output value should be comma seperated crime types e.g. West End Other Theft,Break
		
		Job job = Job.getInstance(new Configuration(), "Neighborhood Crime Type Index");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setJarByClass(NeighborhoodCrimeTypeIndexDriver.class);
		job.setMapperClass(NeighborhoodCrimeTypeIndexMapper.class);
		job.setReducerClass(NeighborhoodCrimeTypeIndexReducer.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}

}
