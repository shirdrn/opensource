package org.shirdrn.hadoop.sort.secondary;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.shirdrn.hadoop.common.DefaultReducer;

public class SecondarySorter extends Configured implements Tool {

	public static class CodeRecordMapper extends Mapper<LongWritable, Text, CodeCostPairKey, EvaluatedPeriod> {
		
		private CodeCostPairKey codeCostPair = new CodeCostPairKey();
		private EvaluatedPeriod period = new EvaluatedPeriod();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// a line, such as 'SG 253654006139495 253654006164392 619850464'
			String line = value.toString();
			String[] array = line.split("\\s");
			if (array.length == 4) {
				String countryCode = array[0];
				String strCost = array[3];
				long cost = Long.parseLong(strCost.trim());
				long startTime = Long.parseLong(array[1].trim());
				long terminateTime = Long.parseLong(array[2].trim());
				if (cost != 0) {
					codeCostPair.setCode(new Text(countryCode));
					codeCostPair.setCost(new LongWritable(cost));
					period.setStartTime(new LongWritable(startTime));
					period.setTerminateTime(new LongWritable(terminateTime));
					context.write(codeCostPair, period);
				}
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "secordary sort");
		
		job.setJarByClass(SecondarySorter.class);
		job.setMapperClass(CodeRecordMapper.class);
		job.setReducerClass(DefaultReducer.class);
		
		job.setPartitionerClass(CodeKeyPartitioner.class);
		job.setGroupingComparatorClass(KeyGroupComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		
		job.setOutputKeyClass(CodeCostPairKey.class);
		job.setOutputValueClass(EvaluatedPeriod.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
		job.setNumReduceTasks(10);
		
//	    SequenceFileOutputFormat.setCompressOutput(job, true);
//	    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
//	    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	    
		Path input = new Path(args[0].trim());
		Path output = new Path(args[1].trim());
		FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SecondarySorter(), args);
		System.exit(exitCode);
	}

}
