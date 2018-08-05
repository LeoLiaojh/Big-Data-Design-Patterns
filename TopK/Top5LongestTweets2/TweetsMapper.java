package Top5LongestTweets2;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetsMapper 
		extends Mapper<LongWritable, Text, Text, Text> {

	private TreeMap<Integer, Text[]> tweetsMap = new TreeMap<Integer, Text[]> ();
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] tweets = value.toString().split("\t");
		
		if (tweets[1] != null && (!tweets[1].equals(""))) {
			Text[] tweetValues = {new Text(tweets[1]), new Text("A" + tweets[0] + "\t" + tweets[4])};
			
			tweetsMap.put(tweets[4].length(), tweetValues);
			if (tweetsMap.size() > 5) {
				tweetsMap.remove(tweetsMap.firstKey());
			}
		}
	}
	
	protected void cleanup(Context context) 
			throws IOException, InterruptedException {
		// output top5 longest tweets in this mapper
		
		for (Text[] tweet : tweetsMap.values()) {
			context.write(tweet[0], tweet[1]);
		}
	}
}
