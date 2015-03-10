package org.shirdrn.hadoop.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class JoinedRecord implements Writable {

	private Text domain;
	private Text ipAddress;
	private IntWritable organizationId;
	private Text organization;
	
	public JoinedRecord() {
		super();
		this.domain = new Text();
		this.ipAddress = new Text();
		this.organizationId = new IntWritable();
		this.organization = new Text();
	}
	
	public JoinedRecord(JoinedRecord record) {
		this();
		this.domain = new Text(record.getDomain().toString());
		this.ipAddress = new Text(record.getIpAddress().toString());
		this.organizationId = new IntWritable(record.getOrganizationId().get());
		this.organization = new Text(record.getOrganization().toString());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		domain.readFields(in);
		ipAddress.readFields(in);
		organizationId.readFields(in);
		organization.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		domain.write(out);
		ipAddress.write(out);
		organizationId.write(out);
		organization.write(out);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(domain.toString()).append("\t")
		.append(ipAddress.toString()).append("\t")
		.append(organizationId.toString()).append("\t")
		.append(organization.toString());
		return builder.toString();
	}

	public Text getDomain() {
		return domain;
	}

	public void setDomain(Text domain) {
		this.domain = domain;
	}

	public Text getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(Text ipAddress) {
		this.ipAddress = ipAddress;
	}

	public IntWritable getOrganizationId() {
		return organizationId;
	}

	public void setOrganizationId(IntWritable organizationId) {
		this.organizationId = organizationId;
	}

	public Text getOrganization() {
		return organization;
	}

	public void setOrganization(Text organization) {
		this.organization = organization;
	}

}
