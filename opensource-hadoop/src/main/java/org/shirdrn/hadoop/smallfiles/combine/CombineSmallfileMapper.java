package org.shirdrn.hadoop.smallfiles.combine;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CombineSmallfileMapper extends
		Mapper<LongWritable, BytesWritable, Text, BytesWritable> {

	private Text file = new Text();
	
	@Override
	protected void map(LongWritable key, BytesWritable value, Context context)
			throws IOException, InterruptedException {
		String fileName = context.getConfiguration().get("map.input.file.name");
		file.set(fileName);
		context.write(file, value);
	}
	
}
