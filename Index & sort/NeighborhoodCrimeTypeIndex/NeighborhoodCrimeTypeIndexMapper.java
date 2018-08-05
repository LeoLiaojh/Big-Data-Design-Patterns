package NeighborhoodCrimeTypeIndex;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NeighborhoodCrimeTypeIndexMapper 
		extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text neighbor = new Text();
	private Text crimeType = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] InputArray = value.toString().split(",");
		
		if (InputArray[7].equals("")) return;
		neighbor.set(InputArray[7]);
		crimeType.set(InputArray[0]);
		
		context.write(neighbor, crimeType);
	}
}
