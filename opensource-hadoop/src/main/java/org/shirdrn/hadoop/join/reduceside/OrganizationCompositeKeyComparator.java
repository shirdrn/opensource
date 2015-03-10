package org.shirdrn.hadoop.join.reduceside;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrganizationCompositeKeyComparator extends WritableComparator {
	public OrganizationCompositeKeyComparator() {
		super(OrganizationIdCompositeKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrganizationIdCompositeKey pairKey1 = (OrganizationIdCompositeKey) a;
		OrganizationIdCompositeKey pairKey2 = (OrganizationIdCompositeKey) b;
		// compare based on fileds 'organizationId' and 'joinSide'
		return pairKey1.compareTo(pairKey2);
	}

}
