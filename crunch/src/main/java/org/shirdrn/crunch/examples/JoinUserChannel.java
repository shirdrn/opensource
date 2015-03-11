package org.shirdrn.crunch.examples;

import java.io.Serializable;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Join;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinUserChannel extends Configured implements Tool, Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 3) {
			System.err.println("Usage: hadoop jar crunch-0.0.1-SNAPSHOT" + 
					JoinUserChannel.class.getName() + " <user_input> <channel_input> <output>");
			return 1;
		}

		String userPath = args[0];
		String channelPath = args[1];
		String outputPath = args[2];

		// Create an pipeline & read 2 text files
		Pipeline pipeline = new MRPipeline(JoinUserChannel.class, getConf());
		
		// user data
		PCollection<String> users = pipeline.readTextFile(userPath);
		PTypeFamily uTF = users.getTypeFamily();
		PTable<String, String> left = users.parallelDo(new DoFn<String, Pair<String, String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void process(String input, Emitter<Pair<String, String>> emitter) {
				String[] kv = input.split("\\s+");
				if(kv.length == 2) {
					String userId = kv[0];
					String channelId = kv[1].trim();
					emitter.emit(Pair.of(channelId, userId)); // key=channelId, value=userId
				}
			}
			
		}, uTF.tableOf(uTF.strings(), uTF.strings()));
		
		// channel data
		PCollection<String> channels = pipeline.readTextFile(channelPath);
		PTypeFamily cTF = channels.getTypeFamily();
		PTable<String, String> right = channels.parallelDo(new DoFn<String, Pair<String, String>>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void process(String input, Emitter<Pair<String, String>> emitter) {
				String[] kv = input.split("\\s+");
				if(kv.length == 2) {
					String channelId = kv[0].trim();
					String channelName = kv[1];
					emitter.emit(Pair.of(channelId, channelName)); // key=channelId, value=channelName
				}
			}
			
		}, cTF.tableOf(cTF.strings(), cTF.strings()));
		
		// join 2 tables & write to HDFS
		PTable<String, Pair<String, String>> joinedResult = Join.innerJoin(left, right);
		pipeline.writeTextFile(joinedResult, outputPath);
		
		// Execute the pipeline as a MapReduce.
		PipelineResult result = pipeline.done();

		return result.succeeded() ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new JoinUserChannel(), args);
		
	}

}
