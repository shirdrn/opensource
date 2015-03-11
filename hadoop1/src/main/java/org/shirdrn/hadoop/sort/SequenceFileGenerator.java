package org.shirdrn.hadoop.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.shirdrn.hadoop.common.DefaultReducer;

public class SequenceFileGenerator extends Configured implements Tool {

	public static class TimeToCodeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		private LongWritable timestamp = new LongWritable();
		private Text code = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// a line, such as 'SG 253654006139495 253654006164392 619850464'
			String line = value.toString();
			String[] array = line.split("\\s");
			if (array.length == 4) {
				String countryCode = array[0];
				String strCost = array[3];
				long cost = Long.parseLong(strCost);
				if (cost != 0) {
					code.set(countryCode);
					timestamp.set(cost);
					context.write(timestamp, code);
				}
			}
		}
	}
	
	private Path output;
	
	public Path getOutputPath() {
		return output;
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Path input = new Path(args[0]);
	    output = new Path(args[1]);
	    
	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "gen sequence file");
	    
	    job.setJarByClass(SequenceFileGenerator.class);
	    job.setMapperClass(SequenceFileGenerator.TimeToCodeMapper.class);
	    job.setReducerClass(DefaultReducer.class);
	    
	    job.setMapOutputKeyClass(LongWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    SequenceFileOutputFormat.setCompressOutput(job, true);
	    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
	    
	    job.setNumReduceTasks(5);
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	
}
