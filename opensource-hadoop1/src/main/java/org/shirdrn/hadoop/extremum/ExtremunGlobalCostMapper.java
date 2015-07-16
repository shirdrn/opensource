package org.shirdrn.hadoop.extremum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ExtremunGlobalCostMapper extends
		Mapper<LongWritable, Text, Text, LongWritable> {

	private LongWritable costValue = new LongWritable(1);
    private Text code = new Text();
    
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// a line, such as 'SG 253654006139495 253654006164392 619850464'
		String line = value.toString();
		String[] array = line.split("\\s");
		if(array.length == 4) {
			String countryCode = array[0];
			String strCost = array[3];
			long cost = 0L;
			try {
				cost = Long.parseLong(strCost);
			} catch (NumberFormatException e) {
				cost = 0L;
			}
			if(cost != 0) {
				code.set(countryCode);
				costValue.set(cost);
				context.write(code, costValue);
			}
		}
	}

}
