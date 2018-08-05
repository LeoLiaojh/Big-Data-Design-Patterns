package CrimeFirstLastDate;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeFirstLastDateMapper 
		extends Mapper<LongWritable, Text, Text, CrimeFirstLastDateTuple> {

	private Text crimeType = new Text();
	private CrimeFirstLastDateTuple outputTuple = new CrimeFirstLastDateTuple();
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		String[] inputArray = value.toString().split(",");
		String strDate = inputArray[1] + "-" + inputArray[2] + "-" + inputArray[3];
		
		if (crimeType == null || strDate == null) return;
		
		try {
			Date crimeDate = frmt.parse(strDate);
			outputTuple.setMin(crimeDate);
			outputTuple.setMax(crimeDate);
			crimeType.set(inputArray[0]);
			
			context.write(crimeType, outputTuple);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			// e.printStackTrace();
		}
		
	}
}
