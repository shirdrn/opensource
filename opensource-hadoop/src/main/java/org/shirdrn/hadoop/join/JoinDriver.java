package org.shirdrn.hadoop.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinDriver extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(JoinDriver.class);
	
	public static class JoinMapper extends
		Mapper<LongWritable, Text, FlaggedKey, JoinedRecord> {

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Left : cdbaby.com	cdbaby.com	70.102.112.164	2	3
			// Right: 45050	Zynga Game Network
			String line = value.toString();
			String[] fields = line.split("\t");
			FlaggedKey flaggedKey = null;
			JoinedRecord record = new JoinedRecord();
			if(fields.length == 2) {
				int orgId = Integer.parseInt(fields[0]);
				// set flag to 0
				flaggedKey = new FlaggedKey(new IntWritable(orgId), new IntWritable(0));
				record.setOrganizationId(new IntWritable(orgId));
				record.setOrganization(new Text(fields[1]));
			} else if(fields.length == 5) {
				String domain = fields[0];
				String ip = fields[2];
				int orgId = Integer.parseInt(fields[3]);
				// set flag to 1
				flaggedKey = new FlaggedKey(new IntWritable(orgId), new IntWritable(1));
				record.setDomain(new Text(domain));
				record.setIpAddress(new Text(ip));
				record.setOrganizationId(new IntWritable(orgId));
			} 
			if(fields.length == 5 || fields.length == 2) {
				context.write(flaggedKey, record);
			}
		}
		
	}
	
	public static class JoinReducer extends
		Reducer<FlaggedKey, JoinedRecord, FlaggedKey, JoinedRecord> {

		@Override
		protected void reduce(FlaggedKey key, Iterable<JoinedRecord> values, Context context)
				throws IOException, InterruptedException {
			Iterator<JoinedRecord> iter = values.iterator();
			// MUST copy first element!!!
			// WRONG usage: Text organization = iter.next();
			Text organization = new Text(iter.next().getOrganization());
			LOG.info("ORGANIZATION=" + organization);
			while(iter.hasNext()) {
				JoinedRecord value = iter.next();
				JoinedRecord record = new JoinedRecord(value);
				record.setOrganization(organization);
				context.write(key, record);
			}
		}

	}
	
	public static class KeyComparator extends WritableComparator {
		public KeyComparator() {
			super(FlaggedKey.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			FlaggedKey pairKey1 = (FlaggedKey) a;
			FlaggedKey pairKey2 = (FlaggedKey) b;
			// compare based on fileds 'organizationId' and 'joinFlag'
			return pairKey1.compareTo(pairKey2);
		}

	}
	
	public static class KeyGroupComparator extends WritableComparator {

		public KeyGroupComparator() {
			super(FlaggedKey.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			FlaggedKey k1 = (FlaggedKey) a;
			FlaggedKey k2 = (FlaggedKey) b;
			// just group by field 'organizationId'
			return k1.getOrganizationId().compareTo(k2.getOrganizationId());
		}

	}
	
	public static class KeyPartitioner extends
		Partitioner<FlaggedKey, JoinedRecord> {

		@Override
		public int getPartition(FlaggedKey key, JoinedRecord value, int numPartitions) {
			int whichPartition = (key.getOrganizationId().hashCode() & Integer.MAX_VALUE) % numPartitions;
			return whichPartition;
		}
	
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "map-input-all join");
		
		job.setJarByClass(JoinDriver.class);
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);
		
		job.setMapOutputKeyClass(FlaggedKey.class);
		job.setMapOutputValueClass(JoinedRecord.class);
		job.setOutputKeyClass(FlaggedKey.class);
		job.setOutputValueClass(JoinedRecord.class);
		
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(KeyGroupComparator.class);
		job.setPartitionerClass(KeyPartitioner.class);
	    
		job.setNumReduceTasks(5);
		
		Path input = new Path(args[0].trim());
		Path output = new Path(args[1].trim());
		FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new JoinDriver(), args);
		System.exit(exitCode);
	}

}
