package org.shirdrn.hadoop.sort.secondary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class EvaluatedPeriod implements WritableComparable<EvaluatedPeriod> {

	private LongWritable startTime;
	private LongWritable terminateTime;
	
	public EvaluatedPeriod() {
		super();
		this.startTime = new LongWritable();
		this.terminateTime = new LongWritable();
	}
	
	public EvaluatedPeriod(LongWritable startTime, LongWritable terminateTime) {
		this.startTime = startTime;
		this.terminateTime = terminateTime;
	}

	public LongWritable getStartTime() {
		return startTime;
	}

	public void setStartTime(LongWritable startTime) {
		this.startTime = startTime;
	}

	public LongWritable getTerminateTime() {
		return terminateTime;
	}

	public void setTerminateTime(LongWritable terminateTime) {
		this.terminateTime = terminateTime;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		startTime.readFields(in);
		terminateTime.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		startTime.write(out);
		terminateTime.write(out);
	}

	@Override
	public int compareTo(EvaluatedPeriod other) {
		long cmp = (terminateTime.get() - startTime.get()) 
				- (other.terminateTime.get() - other.startTime.get());
		return (int) cmp;
	}
	
	@Override
	public String toString() {
		return startTime.get() + "\t" + terminateTime.get();
	}
	
}
