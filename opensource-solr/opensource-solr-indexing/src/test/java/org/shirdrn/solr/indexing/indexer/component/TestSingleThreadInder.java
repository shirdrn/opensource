package org.shirdrn.solr.indexing.indexer.component;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.solr.client.solrj.SolrServerException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.ZkConf;
import org.shirdrn.solr.indexing.indexer.SingleThreadIndexer;
import org.shirdrn.solr.indexing.indexer.component.FilesIndexer;

public class TestSingleThreadInder {
	
	SingleThreadIndexer indexer;
	int totalCount = 10000;
	
	String schemaMappingFile = "schema-mapping.xml";
	String zkHost;
	int connectTimeout;
	int clientTimeout;
	String collection;
	int batchCount;
	String input;
	
	@Before
	public void initialize() throws MalformedURLException {
		zkHost = "slave1:2188";
		connectTimeout = 10000;
		clientTimeout = 30000;
		collection = "tinycollection";
		batchCount = 20;
		String workspace = System.getProperty("user.dir");
		String packageName = this.getClass().getPackage().getName();
		schemaMappingFile = workspace + "/src/test/resources/" + packageName.replaceAll("\\.", "/") + "/" + schemaMappingFile;
		
		ZkConf zkConf = new ZkConf();
		zkConf.setZkHost(zkHost);
		zkConf.setZkConnectTimeout(connectTimeout);
		zkConf.setZkClientTimeout(clientTimeout);
		ClientConf clientConf = new ClientConf(zkConf);
		clientConf.setCollectionName(collection);
		clientConf.setSchemaMappingFile(schemaMappingFile);
		clientConf.setBatchCount(batchCount);
		input = workspace + "/src/test/resources/" + packageName.replaceAll("\\.", "/") + "/data";
		indexer = new FilesIndexer(clientConf, input);
	}
	
	@Test
	public void indexDocs() throws Throwable {
		indexDocs(totalCount);		
	}
	
	private void indexDocs(int totalCount) throws Throwable {
		indexer.indexDocs();
	}

	@After
	public void distory() throws SolrServerException, IOException {
		indexer.close();
	}
}
