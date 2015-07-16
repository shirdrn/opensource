package org.shirdrn.hadoop.join.reduceside;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DomainKeyPartitioner extends
		Partitioner<OrganizationIdCompositeKey, Text> {

	@Override
	public int getPartition(OrganizationIdCompositeKey key, Text value, int numPartitions) {
		int whichPartition = (key.getOrganizationId().hashCode() & Integer.MAX_VALUE) % numPartitions;
		return whichPartition;
	}

}
