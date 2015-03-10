package org.shirdrn.hadoop.sort.secondary;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CodeCostPairKey implements WritableComparable<CodeCostPairKey> {

	protected Text code;
	protected LongWritable cost;
	
	public CodeCostPairKey() {
		this(new Text(), new LongWritable());
	}
	
	public CodeCostPairKey(Text code, LongWritable cost) {
		super();
		this.code = code;
		this.cost = cost;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		code.readFields(in);
		cost.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		code.write(out);
		cost.write(out);
	}

	@Override
	public int compareTo(CodeCostPairKey o) {
		int cmp = code.compareTo(o.code);
		if(cmp != 0) {
			return cmp;
		}
		return -cost.compareTo(o.cost);
	}

	public Text getCode() {
		return code;
	}

	public void setCode(Text code) {
		this.code = code;
	}

	public LongWritable getCost() {
		return cost;
	}

	public void setCost(LongWritable cost) {
		this.cost = cost;
	}

	@Override
	public int hashCode() {
		return code.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		CodeCostPairKey other = (CodeCostPairKey) obj;
		return code.equals(other.code) && cost.equals(other.cost);
	}

	@Override
	public String toString() {
		return code.toString() + "\t" + cost.get();
	}
	
	

}
