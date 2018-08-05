package Top5LongestTweets2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top5LongestTweetsReducer 
		extends Reducer<Text, Text, NullWritable, Text> {

	private TreeMap<Integer, String> outputList = new TreeMap<Integer, String>();
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	private Text outputValue = new Text();
	private Text temp = new Text();
	private String joinType = null;
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		
		// clear list A and B in the beginning
		listA.clear();
		listB.clear();
		// divide values to two lists
		for (Text val : values) {
			if (val.toString().charAt(0) == 'A') {
				temp.set(val.toString().substring(1));
				listA.add(temp);
				
			} else if (val.toString().charAt(0) == 'B') {
				temp.set(val.toString().substring(1));
				listB.add(temp);
			}
		}
		// join and put result into the tree map
		executeJoinLogic(context);
	}
	
	protected void cleanup(Context context) 
			throws IOException, InterruptedException {
		for (String val : outputList.descendingMap().values()) {
			outputValue.set(val);
			context.write(NullWritable.get(), outputValue);
		}
	}
	
	public void executeJoinLogic(Context context) 
			throws IOException, InterruptedException {
		
		joinType = context.getConfiguration().get("joinType");
		
		if (joinType.equalsIgnoreCase("left")) {
			for (Text valA : listA) {
				int textLength = valA.toString().split("\t")[1].length();
				if (!listB.isEmpty()) {
					for (Text valB : listB) {
						outputList.put(textLength, valA.toString() + valB.toString());
					}
				} else {
					outputList.put(textLength, valA.toString());
				}
				
				// Doing top 5 here
				if (outputList.size() > 5) {
					outputList.remove(outputList.firstKey());
				}
			}
		}
	}
}
