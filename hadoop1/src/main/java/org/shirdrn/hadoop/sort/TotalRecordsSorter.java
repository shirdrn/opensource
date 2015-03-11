package org.shirdrn.hadoop.sort;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalRecordsSorter extends Configured implements Tool {

	private Path input;
	private Path output;
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "gen partition file");
	    
		job.setJarByClass(TotalRecordsSorter.class);
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    SequenceFileOutputFormat.setCompressOutput(job, true);
	    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
	    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	    
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    job.setNumReduceTasks(10);
	    job.setPartitionerClass(TotalOrderPartitioner.class);
	    
	    InputSampler.Sampler<LongWritable, Text> sampler =
	    		new InputSampler.RandomSampler<LongWritable, Text>(0.1, 50000, 10);
	    InputSampler.writePartitionFile(job, sampler);
	    
	    String partitionFile = TotalOrderPartitioner.getPartitionFile(job.getConfiguration());
	    URI partitionUri = new URI(partitionFile + "#" + TotalOrderPartitioner.DEFAULT_PATH);
		DistributedCache.addCacheFile(partitionUri, job.getConfiguration());
		DistributedCache.createSymlink(job.getConfiguration());
	    
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public void setInput(Path input) {
		this.input = input;
	}

	public void setOutput(Path output) {
		this.output = output;
	}

	public static void main(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Usage: \n" +
					" TotalRecordsSorter <in> <output> <sortedOutput>");
			System.exit(1);
		}
		SequenceFileGenerator generator = new SequenceFileGenerator();
		int xCode = ToolRunner.run(generator, args);
		if(xCode == 0) {
			TotalRecordsSorter sorter = new TotalRecordsSorter();
			sorter.setInput(generator.getOutputPath());
			sorter.setOutput(new Path(args[2]));
			ToolRunner.run(sorter, args);
		}
	}

}
