package CrimeFirstLastDate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Writable;

public class CrimeFirstLastDateTuple implements Writable {
	private Date min = new Date();
	private Date max = new Date();
	private final static SimpleDateFormat frmt = new SimpleDateFormat("yyyy-MM-dd");
	
	public void setMin(Date minVal) {
		this.min = minVal;
	}
	public Date getMin() {
		return this.min;
	}
	public void setMax(Date maxVal) {
		this.max = maxVal;
	}
	public Date getMax() {
		return this.max;
	}
	
	public void readFields(DataInput in) 
			throws IOException {
		min = new Date(in.readLong());
		max = new Date(in.readLong());
	}
	public void write(DataOutput out) 
			throws IOException {
		out.writeLong(min.getTime());
		out.writeLong(max.getTime());
		
	}
	
	@Override
	public String toString() {
		return "Min: " + frmt.format(min) + "\tMax:" + frmt.format(max);
	}

}
