package org.shirdrn.platform.dubbo.service.rpc.utils;

import java.net.MalformedURLException;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.shirdrn.platform.dubbo.service.conf.ClientConf;

public class QueryUtils {

	public static CloudSolrServer createServer(ClientConf clientConf) {
		CloudSolrServer server = null;
		try {
			server = new CloudSolrServer(clientConf.getZkConf().getZkHost());
			server.setDefaultCollection(clientConf.getCollectionName());
			server.setZkClientTimeout(clientConf.getZkConf().getZkClientTimeout());
			server.setZkConnectTimeout(clientConf.getZkConf().getZkConnectTimeout());
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
		return server;
	}

	public static HttpClient createClient(ClientConf clientConf) {
		HttpClient client = new DefaultHttpClient();
		return client;
	}
	
}
