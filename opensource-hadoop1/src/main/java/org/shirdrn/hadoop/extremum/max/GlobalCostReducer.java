package org.shirdrn.hadoop.extremum.max;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GlobalCostReducer extends
		Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
		long max = 0L;
		Iterator<LongWritable> iter = values.iterator();
		while(iter.hasNext()) {
			LongWritable current = iter.next();
			if(current.get() > max) {
				max = current.get();
			}
		}
		context.write(key, new LongWritable(max));
	}

}
