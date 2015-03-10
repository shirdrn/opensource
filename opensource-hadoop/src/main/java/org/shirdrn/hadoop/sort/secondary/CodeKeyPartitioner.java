package org.shirdrn.hadoop.sort.secondary;

import org.apache.hadoop.mapreduce.Partitioner;

public class CodeKeyPartitioner extends
		Partitioner<CodeCostPairKey, EvaluatedPeriod> {

	@Override
	public int getPartition(CodeCostPairKey key, EvaluatedPeriod value, int numPartitions) {
		int whichPartition = key.getCode().hashCode() % numPartitions;
		return whichPartition;
	}

}
