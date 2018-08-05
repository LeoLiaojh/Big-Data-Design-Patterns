package CrimeFirstLastDate;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimeFirstLastDateDriver {

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		// Outputs the first date and last date a crime type has happened
		// For date use the year, month and day values in YYYY-mm-dd (four digit year, two digit month an day) format
		
		Job job = Job.getInstance(new Configuration(), "Crime First/Last Date Count");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(CrimeFirstLastDateTuple.class);
		
		job.setJarByClass(CrimeFirstLastDateDriver.class);
		job.setMapperClass(CrimeFirstLastDateMapper.class);
		job.setReducerClass(CrimeFirstLastDateReducer.class);
		job.setCombinerClass(CrimeFirstLastDateReducer.class);
		
		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

	}

}
