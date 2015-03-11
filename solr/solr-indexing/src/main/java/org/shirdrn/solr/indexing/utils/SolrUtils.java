package org.shirdrn.solr.indexing.utils;

import java.net.MalformedURLException;

import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.FieldMappingBuilder;

public class SolrUtils {

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
	
	public static FieldMappingBuilder createFieldMappingBuilder(ClientConf clientConf) {
		FieldMappingBuilder builder = new FieldMappingBuilder();
		if(clientConf.getSchemaMappingFile() == null 
				|| clientConf.getSchemaMappingFile().equals("schema-mapping.xml")) {
			builder.build();
		} else {
			builder.build(clientConf.getSchemaMappingFile());
		}
		return builder;
	}
}
