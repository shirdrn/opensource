package org.shirdrn.hadoop.sort.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyGroupComparator extends WritableComparator {

	public KeyGroupComparator() {
		super(CodeCostPairKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		CodeCostPairKey pairKey1 = (CodeCostPairKey) a;
		CodeCostPairKey pairKey2 = (CodeCostPairKey) b;
		// just group by field 'first'
		return pairKey1.getCode().compareTo(pairKey2.getCode());
	}

}
