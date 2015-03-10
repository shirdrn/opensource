package org.shirdrn.hadoop.join.reduceside;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinDomainOrganizationReducer extends
		Reducer<OrganizationIdCompositeKey, Text, OrganizationIdCompositeKey, DomainDetail> {

	private static final Log LOG = LogFactory.getLog(JoinDomainOrganizationReducer.class);
	
	@Override
	protected void reduce(OrganizationIdCompositeKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iter = values.iterator();
		// MUST copy first element!!!
		// WRONG usage: Text organization = iter.next();
		Text organization = new Text(iter.next());
		LOG.info("Organization=" + organization);
		while(iter.hasNext()) {
			Text value = iter.next();
			String[] fields = value.toString().split("\t");
			if(fields.length == 2) {
				DomainDetail detail = new DomainDetail();
				String domain = fields[0];
				String ip = fields[1];
				detail.setDomain(new Text(domain));
				detail.setIpAddress(new Text(ip));
				detail.setOrganization(organization);
				context.write(key, detail);
			}
		}
	}

}
