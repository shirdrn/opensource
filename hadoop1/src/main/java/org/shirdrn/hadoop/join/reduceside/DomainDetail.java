package org.shirdrn.hadoop.join.reduceside;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DomainDetail implements Writable {

	private Text domain;
	private Text ipAddress;
	private Text organization;
	
	public DomainDetail(Text domain, Text ipAddress, Text organization) {
		super();
		this.domain = domain;
		this.ipAddress = ipAddress;
		this.organization = organization;
	}

	public DomainDetail() {
		super();
		this.domain = new Text();
		this.ipAddress = new Text();
		this.organization = new Text();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		domain.readFields(in);
		ipAddress.readFields(in);
		organization.readFields(in);

	}

	@Override
	public void write(DataOutput out) throws IOException {
		domain.write(out);
		ipAddress.write(out);
		organization.write(out);
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

	public Text getOrganization() {
		return organization;
	}

	public void setOrganization(Text organization) {
		this.organization = organization;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		if(domain != null) {
			builder.append(domain.toString()).append("\t");
		}
		if(ipAddress != null) {
			builder.append(ipAddress.toString()).append("\t");
		}
		if(organization != null) {
			builder.append(organization.toString());
		}
		return builder.toString();
	}

}
