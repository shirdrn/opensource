package org.shirdrn.hadoop.join.reduceside;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoinDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "reduce-side join");
		job.setJarByClass(ReduceSideJoinDriver.class);
		
		Path domainIn = new Path(args[0].trim());
		Path organizationIn = new Path(args[1].trim());
		Path output = new Path(args[2].trim());
	    
		MultipleInputs.addInputPath(job, organizationIn, TextInputFormat.class, JoinOrganizationMapper.class);
		MultipleInputs.addInputPath(job, domainIn, TextInputFormat.class, JoinDomainMapper.class);
		FileOutputFormat.setOutputPath(job, output);
		
		job.setPartitionerClass(DomainKeyPartitioner.class);
		job.setSortComparatorClass(OrganizationCompositeKeyComparator.class);
		job.setGroupingComparatorClass(OrganizationKeyGroupComparator.class);
		
		job.setMapOutputKeyClass(OrganizationIdCompositeKey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(OrganizationIdCompositeKey.class);
		job.setOutputValueClass(DomainDetail.class);
	    
		job.setReducerClass(JoinDomainOrganizationReducer.class);
		job.setNumReduceTasks(10);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ReduceSideJoinDriver(), args);
		System.exit(exitCode);
	}

}
