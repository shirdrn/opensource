package org.shirdrn.platform.dubbo.service.conf;


public class ClientConf {

	ZkConf zkConf;
	int batchCount = 1000;
	String collectionName;
	String schemaMappingFile = "schema-mapping.xml";

	public ClientConf(ZkConf zkConf) {
		this.zkConf = zkConf;
	}

	public ZkConf getZkConf() {
		return zkConf;
	}

	public int getBatchCount() {
		return batchCount;
	}

	public void setBatchCount(int batchCount) {
		this.batchCount = batchCount;
	}

	public String getCollectionName() {
		return collectionName;
	}

	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}

	public String getSchemaMappingFile() {
		return schemaMappingFile;
	}

	public void setSchemaMappingFile(String schemaMappingFile) {
		this.schemaMappingFile = schemaMappingFile;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(zkConf).append(",").append("collection=")
				.append(collectionName).append(",").append("batchCount=")
				.append(batchCount).append(",").append("schemaMappingFile=")
				.append(schemaMappingFile);
		return sb.toString();
	}

}
