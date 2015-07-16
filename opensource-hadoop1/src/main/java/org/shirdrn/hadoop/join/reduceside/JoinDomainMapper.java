package org.shirdrn.hadoop.join.reduceside;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinDomainMapper extends
		Mapper<LongWritable, Text, OrganizationIdCompositeKey, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Right : cdbaby.com	cdbaby.com	70.102.112.164	2	3
		String line = value.toString();
		String[] fields = line.split("\t");
		if(fields.length == 5) {
			int orgId = Integer.parseInt(fields[3]);
			IntWritable id = new IntWritable(orgId);
			OrganizationIdCompositeKey oid = new OrganizationIdCompositeKey(id, JoinSide.RIGHT);
			StringBuilder builder = new StringBuilder();
			String domain = fields[0];
			String ip = fields[2];
			builder.append(domain).append("\t").append(ip);
			Text keep = new Text(builder.toString());
			context.write(oid, keep);
		}
	}

}
