package Top5LongestTweets;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top5LongestTweetsReducer 
		extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		for (Text val : values) {
			context.write(new Text("success"), val);
		}
	}
	
}
