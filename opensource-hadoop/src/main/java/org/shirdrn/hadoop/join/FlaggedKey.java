package org.shirdrn.hadoop.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class FlaggedKey implements WritableComparable<FlaggedKey> {

	private IntWritable organizationId;
	private IntWritable joinFlag;
	
	public FlaggedKey() {
		super();
		this.organizationId = new IntWritable();
		this.joinFlag = new IntWritable();
	}
	
	public FlaggedKey(IntWritable organizationId, IntWritable joinFlag) {
		super();
		this.organizationId = organizationId;
		this.joinFlag = new IntWritable(joinFlag.get());
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		organizationId.readFields(in);
		joinFlag.readFields(in);		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		organizationId.write(out);
		joinFlag.write(out);		
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(organizationId.get()).append("\t")
		.append(joinFlag.get());
		// field 'joinFlag' is required to convert String!
		return builder.toString();
	}

	@Override
	public int compareTo(FlaggedKey o) {
		int cmp = this.organizationId.compareTo(o.organizationId);
		if(cmp != 0) {
			return cmp;
		}
		return joinFlag.compareTo(o.joinFlag);
	}

	public IntWritable getOrganizationId() {
		return organizationId;
	}

	public void setOrganizationId(IntWritable organizationId) {
		this.organizationId = organizationId;
	}

	public IntWritable getJoinFlag() {
		return joinFlag;
	}

	public void setJoinFlag(IntWritable joinFlag) {
		this.joinFlag = joinFlag;
	}

	@Override
	public int hashCode() {
		return this.organizationId.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		FlaggedKey other = (FlaggedKey) obj;
		return this.organizationId.equals(other.organizationId) 
				&& this.joinFlag.equals(other.joinFlag);
	}

}
