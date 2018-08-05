package Top5LongestTweets2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Top5LongestTweetsDriver {

	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {
		//output top 5 longest tweets and the user information about those tweets. 
		// The output should be: tweet_id \t tweet \t userId \t #followers.
		Configuration conf = new Configuration();
		conf.set("joinType", args[3]);
		
		Job job = Job.getInstance(conf, "Top 5 Longest Tweets");
				
		job.setJarByClass(Top5LongestTweetsDriver.class);
				
		job.setNumReduceTasks(1);
				
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, UsersMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TweetsMapper.class);
		job.setReducerClass(Top5LongestTweetsReducer.class);
		
		TextOutputFormat.setOutputPath(job, new Path(args[2]));
				
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
				
		System.exit(job.waitForCompletion(true)? 0:1);
	}

}
