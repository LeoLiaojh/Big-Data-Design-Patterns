package Top5LongestTweets;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top5LongestTweetsMapper 
		extends Mapper<LongWritable, Text, Text, Text> {

	private ArrayList<String> users = new ArrayList<String>();
	private Text outputVal = new Text();
	private String joinType = "outer";
	
	@Override
	public void setup(Context context) {
		try {
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for (Path p : files) {
				BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(new File(p.toString())))));
				String line;
				while ((line = reader.readLine()) != null) {
//					String[] userArray = line.split("\t");
//					if (userArray[0] != null) users.put(userArray[0], userArray[0] + "\t" + userArray[3]);
					users.add(line);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}	
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
//		String[] tweets = value.toString().split("\t");
//		String userInfo = users.get(tweets[1]);
//		if (userInfo != null) {
//			outputVal.set(userInfo);
//			context.write(value, outputVal);
//		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (String val : users) {
			outputVal.set(val);
			context.write(new Text("check"), outputVal);
		}
	}
}
