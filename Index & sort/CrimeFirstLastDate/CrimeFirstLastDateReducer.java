package CrimeFirstLastDate;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CrimeFirstLastDateReducer 
		extends Reducer<Text, CrimeFirstLastDateTuple, Text, CrimeFirstLastDateTuple>{
	
	private CrimeFirstLastDateTuple result = new CrimeFirstLastDateTuple();
	
	@Override
	public void reduce(Text key, Iterable<CrimeFirstLastDateTuple> values, Context context) 
			throws IOException, InterruptedException {
		result.setMin(null);
		result.setMax(null);
		
		for (CrimeFirstLastDateTuple val : values) {
			if (result.getMin() == null || val.getMin().compareTo(result.getMin()) < 0 ) {
				result.setMin(val.getMin());
			} // this for the min date
			
			if (result.getMax() == null || val.getMax().compareTo(result.getMax()) > 0) {
				result.setMax(val.getMax());
			} // this for the max date
		}
		
		// output result
		context.write(key, result);
	}
}
