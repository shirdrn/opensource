package org.shirdrn.hadoop.join.reduceside;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrganizationKeyGroupComparator extends WritableComparator {

	public OrganizationKeyGroupComparator() {
		super(OrganizationIdCompositeKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrganizationIdCompositeKey k1 = (OrganizationIdCompositeKey) a;
		OrganizationIdCompositeKey k2 = (OrganizationIdCompositeKey) b;
		// just group by field 'organizationId'
		return k1.getOrganizationId().compareTo(k2.getOrganizationId());
	}

}
