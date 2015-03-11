package org.shirdrn.platform.dubbo.service.conf;

public class ZkConf {

	String zkHost;
	int zkClientTimeout = 30000;
	int zkConnectTimeout = 30000;

	public ZkConf() {
	}

	public ZkConf(String zkHost) {
		this.zkHost = zkHost;
	}

	public String getZkHost() {
		return zkHost;
	}

	public void setZkHost(String zkHost) {
		this.zkHost = zkHost;
	}

	public int getZkClientTimeout() {
		return zkClientTimeout;
	}

	public void setZkClientTimeout(int zkClientTimeout) {
		this.zkClientTimeout = zkClientTimeout;
	}

	public int getZkConnectTimeout() {
		return zkConnectTimeout;
	}

	public void setZkConnectTimeout(int zkConnectTimeout) {
		this.zkConnectTimeout = zkConnectTimeout;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("zkHost=").append(zkHost).append(",")
				.append("zkConnectTimeout=").append(zkConnectTimeout)
				.append(",").append("zkClientTimeout=").append(zkClientTimeout);
		return sb.toString();
	}
}
