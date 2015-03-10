package org.shirdrn.hadoop.sort.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {

	public CompositeKeyComparator() {
		super(CodeCostPairKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CodeCostPairKey pairKey1 = (CodeCostPairKey) a;
		CodeCostPairKey pairKey2 = (CodeCostPairKey) b;
		return pairKey1.compareTo(pairKey2);
	}
	
	

}
