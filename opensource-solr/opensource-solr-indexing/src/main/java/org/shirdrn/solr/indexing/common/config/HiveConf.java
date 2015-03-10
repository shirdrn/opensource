package org.shirdrn.solr.indexing.common.config;

public class HiveConf {
	String url;
	String user;
	String password;
	String table;
	String conditions;
	public HiveConf(String url, String table, String conditions) {
		super();
		this.url = url;
		this.table = table;
		this.conditions = conditions;
	}
	public HiveConf(String url, String user, String password, String table,
			String conditions) {
		super();
		this.url = url;
		this.user = user;
		this.password = password;
		this.table = table;
		this.conditions = conditions;
	}
	public String getUrl() {
		return url;
	}
	public String getTable() {
		return table;
	}
	public String getConditions() {
		return conditions;
	}
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("url=").append(url).append(",")
		.append("user=").append(user).append(",")
		.append("password=").append(password).append(",")
		.append("table=").append(table).append(",")
		.append("table=").append(table);
		return sb.toString();
	}
}
