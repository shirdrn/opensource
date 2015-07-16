package org.shirdrn.hadoop.smallfiles;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class IdentityReducer<Text, BytesWritable> extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {

	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values, Context context)
			throws IOException, InterruptedException {
		for(BytesWritable value : values) {
			context.write(key, value);
		}
	}

}
