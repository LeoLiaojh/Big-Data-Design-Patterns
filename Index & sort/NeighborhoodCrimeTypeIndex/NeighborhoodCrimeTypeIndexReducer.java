package NeighborhoodCrimeTypeIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NeighborhoodCrimeTypeIndexReducer
		extends Reducer<Text, Text, Text, Text> {
	
	private Text result = new Text();
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		List<String> checkList = new ArrayList<String>();
		StringBuffer crimeTypeList = new StringBuffer();
		boolean isExist = false;
		
		for (Text val : values) {
			if (checkList.size() == 0) {
				crimeTypeList.append(val.toString() + ", ");
				checkList.add(val.toString());
			} else {
				for (int i=0; i<checkList.size(); i++) {
					if (checkList.get(i).equals(val.toString())) {
						isExist = true;
					}
				}
				
				if (isExist == false) {
					crimeTypeList.append(val.toString() + ", ");
					checkList.add(val.toString());
				}
			}
			isExist = false;
		}
		
		crimeTypeList.delete(crimeTypeList.length()-2, crimeTypeList.length()-1);
		
		result.set(crimeTypeList.toString());
		context.write(key, result);
	}
}
