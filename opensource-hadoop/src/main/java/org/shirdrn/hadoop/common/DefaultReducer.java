package org.shirdrn.hadoop.common;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

public class DefaultReducer<K, V> extends Reducer<K, V, K, V> {

	@Override
	protected void reduce(K key, Iterable<V> values, Context context)
			throws IOException, InterruptedException {
		for(V value : values) {
			context.write(key, value);
		}
	}
	
}
