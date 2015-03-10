package org.shirdrn.hadoop.extremum;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExtremumGlobalCostReducer extends
		Reducer<Text, LongWritable, Text, Extremum> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long max = 0L;
		long min = Long.MAX_VALUE;
		Iterator<LongWritable> iter = values.iterator();
		while(iter.hasNext()) {
			LongWritable current = iter.next();
			if(current.get() > max) {
				max = current.get();
			}
			if(current.get() < min) {
				min = current.get();
			}
		}
		Extremum extremum = new Extremum(min, max);
		context.write(key, extremum);
	}

}
