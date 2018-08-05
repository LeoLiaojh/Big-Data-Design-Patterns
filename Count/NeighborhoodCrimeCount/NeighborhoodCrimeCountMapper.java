package NeighborhoodCrimeCount;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NeighborhoodCrimeCountMapper
		extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final IntWritable CountOne = new IntWritable(1);
	private Text neighborhood = new Text();
	
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		List<String> valueList = Arrays.asList(value.toString().split(","));
		neighborhood.set(valueList.get(7));
		context.write(neighborhood, CountOne);
		
	}
}
