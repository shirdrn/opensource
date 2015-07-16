package org.shirdrn.hadoop.io;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.shirdrn.hadoop.common.DefaultReducer;
import org.shirdrn.hadoop.io.LookupMapFiles.MapFilesGenerator.CountryMapper;

public class LookupMapFiles extends Configured implements Tool {

	public static class MapFilesGenerator {
		
		public static class CountryMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
			private LongWritable costValue = new LongWritable(1);
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
					long cost = 0L;
					try {
						cost = Long.parseLong(strCost);
					} catch (NumberFormatException e) {
						cost = 0L;
					}
					if (cost != 0) {
						code.set(countryCode);
						costValue.set(cost);
						context.write(code, costValue);
					}
				}
			}
		}
	}
	
	private int numReduceTasks = 5;
	
	private int genMapFiles(String in, String out) throws Exception {
		Configuration conf = new Configuration();
	    Job job = new Job(conf, "gen mapfiles");
	    
	    job.setJarByClass(MapFilesGenerator.class);
	    job.setMapperClass(CountryMapper.class);
	    job.setReducerClass(DefaultReducer.class);
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongWritable.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    SequenceFileOutputFormat.setCompressOutput(job, true);
	    SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
	    
	    job.setNumReduceTasks(numReduceTasks);
	    
	    Path input = new Path(in);
	    Path output = new Path(out);
	    FileInputFormat.addInputPath(job, input);
	    FileOutputFormat.setOutputPath(job, output);
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
	private void lookup(String mapfile, String query, int maxCount) throws IOException {
		Path path = new Path(mapfile);
		Text key = new Text(query);
		LongWritable value = new LongWritable();
		Configuration conf = new Configuration();
		FileSystem fs = path.getFileSystem(conf);
		Reader[] readers = MapFileOutputFormat.getReaders(fs, path, conf);
		Partitioner<Text, LongWritable> partitioner = new HashPartitioner<Text, LongWritable>();
		if(maxCount <= 1) {
			Writable entry = MapFileOutputFormat.getEntry(readers, (Partitioner<Text, LongWritable>) partitioner, key, value);
			if(entry != null) {
				System.out.println("code=" + key + ",cost=" + value.get());
			}
		} else {
			int which = partitioner.getPartition(key, value, readers.length);
			Reader reader = readers[which];
			Writable entry = reader.get(key, value);
			if(entry != null) {
				System.out.println("code=" + key + ",cost=" + value.get());
				Text nextKey = new Text();
				while(reader.next(nextKey, value)) {
					if(key.equals(nextKey)) {
						System.out.println("code=" + key + ",cost=" + value.get());
					}
				}
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length == 1) {
			// Convert SequenceFile to MapFile
			String output = args[0];
			for(int i=0; i<numReduceTasks; i++) {
				Path file = new Path(output + "/part-r-" + String.format("%05d", i));
				FileSystem fs = file.getFileSystem(conf);
				MapFile.fix(fs, file, Text.class, LongWritable.class, false, conf);
			}
		} else if(otherArgs.length == 2) {
			// Generate MapFile
			genMapFiles(otherArgs[0], otherArgs[1]);
		} else if(otherArgs.length == 3) {
			// Lookup MapFile
			int maxCount = 1;
			maxCount = Integer.parseInt(otherArgs[3]);
			lookup(otherArgs[0], otherArgs[1], maxCount);
		} else if(otherArgs.length == 4) {
			String seqDir = otherArgs[1];
			String mapUri = otherArgs[3];
			FileSystem fs = FileSystem.get(URI.create(mapUri), conf);
			Path mapPath = new Path(mapUri);
			Path mapData = new Path(mapPath, MapFile.DATA_FILE_NAME);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, mapData, conf);
			reader.close();
			long entries = MapFile.fix(fs, mapPath, Text.class, LongWritable.class, false, conf);
			System.out.printf("Created MapFile %s with %d entries\n", mapPath, entries);
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int xCode = ToolRunner.run(new LookupMapFiles(), args);
		System.exit(xCode);
	}

}
