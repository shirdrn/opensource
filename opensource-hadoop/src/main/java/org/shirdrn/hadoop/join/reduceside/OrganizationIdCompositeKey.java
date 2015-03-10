package org.shirdrn.hadoop.join.reduceside;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class OrganizationIdCompositeKey implements WritableComparable<OrganizationIdCompositeKey> {

	private IntWritable organizationId;
	private IntWritable joinSide;
	
	public OrganizationIdCompositeKey(IntWritable organizationId, JoinSide joinSide) {
		super();
		this.organizationId = organizationId;
		this.joinSide = new IntWritable(joinSide.getCode());
	}

	public OrganizationIdCompositeKey() {
		super();
		this.organizationId = new IntWritable();
		this.joinSide = new IntWritable(JoinSide.LEFT.getCode());
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		organizationId.readFields(in);
		joinSide.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		organizationId.write(out);
		joinSide.write(out);
	}
	
	public IntWritable getOrganizationId() {
		return organizationId;
	}

	public void setOrganizationId(IntWritable organizationId) {
		this.organizationId = organizationId;
	}

	public IntWritable getJoinSide() {
		return joinSide;
	}

	public void setJoinSide(JoinSide joinSide) {
		this.joinSide = new IntWritable(joinSide.getCode());
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(organizationId.get()).append("\t")
		.append(joinSide.get());
		// field 'joinSide' is required to convert String!
		return builder.toString();
	}

	@Override
	public int compareTo(OrganizationIdCompositeKey o) {
		int cmp = this.organizationId.compareTo(o.organizationId);
		if(cmp != 0) {
			return cmp;
		}
		return joinSide.compareTo(o.joinSide);
	}

	@Override
	public int hashCode() {
		return this.organizationId.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		OrganizationIdCompositeKey other = (OrganizationIdCompositeKey) obj;
		return this.organizationId.get() == other.organizationId.get() 
				&& this.joinSide.get() == other.joinSide.get();
	}

}
