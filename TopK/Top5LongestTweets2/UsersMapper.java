package Top5LongestTweets2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UsersMapper 
		extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text outputKey = new Text();
	private Text outputValue = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] users = value.toString().split("\t");
		outputKey.set(users[0]);
		outputValue.set("B" + users[3]);
		context.write(outputKey, outputValue);
	}
}
