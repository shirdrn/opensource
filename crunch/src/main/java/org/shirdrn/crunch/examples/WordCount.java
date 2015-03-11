package org.shirdrn.crunch.examples;

import static org.apache.crunch.types.writable.Writables.ints;
import static org.apache.crunch.types.writable.Writables.strings;
import static org.apache.crunch.types.writable.Writables.tableOf;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.crunch.CombineFn;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.lib.Sort;
import org.apache.crunch.lib.Sort.ColumnOrder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Strings;

public class WordCount extends Configured implements Tool, Serializable {

	private static final long serialVersionUID = 1L;

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 2) {
			System.err.println("Usage: hadoop jar crunch-0.0.1-SNAPSHOT" + 
					WordCount.class.getName() + " <input> <output>");
			return 1;
		}

		String inputPath = args[0];
		String outputPath = args[1];

		// Create an pipeline & read a text file
		Pipeline pipeline = new MRPipeline(WordCount.class, getConf());
		PCollection<String> lines = pipeline.readTextFile(inputPath);

		// map
		PTable<String, Integer> mappedWords = map(lines);
		
		// group by key
		PGroupedTable<String, Integer> groupedWords = mappedWords.groupByKey();
		
		// reduce
		PTable<String, Integer> reducedWords = reduce(groupedWords);
		
		// sort
		PCollection<Pair<String, Integer>> sortedValues = 
				Sort.sortPairs(reducedWords, ColumnOrder.by(2, Sort.Order.DESCENDING));

		// write the result to a text file
		pipeline.writeTextFile(sortedValues, outputPath);

		// Execute the pipeline as a MapReduce.
		PipelineResult result = pipeline.done();

		return result.succeeded() ? 0 : 1;
	}
	
	private final PTable<String, Integer> map(PCollection<String> lines) {
		PTable<String, Integer> mappedWords = lines.parallelDo(new DoFn<String, Pair<String, Integer>>() {
			private static final long serialVersionUID = 1L;
			private static final String PATTERN = "\\s+";
			@Override
			public void process(String input, Emitter<Pair<String, Integer>> emitter) {
				if(!Strings.isNullOrEmpty(input)) {
					for(String word : input.split(PATTERN)) {
						if(!Strings.isNullOrEmpty(word)) {
							emitter.emit(Pair.of(word, 1));
						}
					}
				}				
			}
		}, tableOf(strings(), ints()));
		return mappedWords;
	}

	private final PTable<String, Integer> reduce(PGroupedTable<String, Integer> groupedWords) {
		PTable<String, Integer> reducedWords = groupedWords.combineValues(new CombineFn<String, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public void process(Pair<String, Iterable<Integer>> values, Emitter<Pair<String, Integer>> emitter) {
				int count = 0;
				Iterator<Integer> iter = values.second().iterator();
				while(iter.hasNext()) {
					count += iter.next();
				}
				emitter.emit(Pair.of(values.first(), count));
			}
		});
		return reducedWords;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new WordCount(), args);
	}

}
