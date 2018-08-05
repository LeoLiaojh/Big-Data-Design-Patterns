package Top5LongestTweets;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Top5LongestTweetsDriver {

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		//output top 5 longest tweets and the user information about those tweets. 
		// The output should be: tweet_id \t tweet \t userId \t #followers.
		
		Job job = Job.getInstance(new Configuration(), "Top 5 Longest Tweets");
		
		job.setJarByClass(Top5LongestTweetsDriver.class);
		job.setMapperClass(Top5LongestTweetsMapper.class);
//		job.setCombinerClass(Top5LongestTweetsReducer.class);
//		job.setReducerClass(Top5LongestTweetsReducer.class);
		
		job.setNumReduceTasks(0);
		
		TextInputFormat.setInputPaths(job, new Path(args[1]));
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// Configure Distributed Cache
//		job.addCacheFile(new Path(args[0]).toUri());
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
		DistributedCache.setLocalFiles(job.getConfiguration(), args[0]);
		
		System.exit(job.waitForCompletion(true)? 0:1);
	}

}
