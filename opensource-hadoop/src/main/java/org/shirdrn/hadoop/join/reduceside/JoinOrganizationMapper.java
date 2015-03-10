package org.shirdrn.hadoop.join.reduceside;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinOrganizationMapper extends
		Mapper<LongWritable, Text, OrganizationIdCompositeKey, Text> {

	private Text organization = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// Left: 45050	Zynga Game Network
		String line = value.toString();
		String[] fields = line.split("\t");
		if(fields.length == 2) {
			int orgId = Integer.parseInt(fields[0]);
			IntWritable id = new IntWritable(orgId);
			OrganizationIdCompositeKey oid = new OrganizationIdCompositeKey(id, JoinSide.LEFT);
			String name = fields[1];
			organization.set(name);
			context.write(oid, organization);
		}
	}

}
