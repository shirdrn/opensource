package org.shirdrn.solr.indexing.indexer.component;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.HiveConf;
import org.shirdrn.solr.indexing.common.config.ZkConf;
import org.shirdrn.solr.indexing.indexer.MultiThreadedIndexer;
import org.shirdrn.solr.indexing.indexer.component.MultiThreadedDBIndexer;

public class TestMultiThreadedHiveBasedIndexer {

	private static final Log LOG = LogFactory.getLog(TestMultiThreadedHiveBasedIndexer.class);
	MultiThreadedIndexer indexer;
	String zkHost;
	String collection;
	int batchCount;
	String schemaMappingFile = "i_event-schema-mapping.xml";
	int connectTimeout;
	int clientTimeout;
	int threadCount;
	String url;
	String user;
	String password;
	String table;
	String conditions;
	
	@Before
	public void initialize() {
		String zkHost = "slave1:2188";
		String collection = "i_event";
		batchCount = 1000;
		threadCount = 20;
		url = "jdbc:hive2://slave1:21050/default/;auth=noSasl";
		user = null;
		password = null;
		table = "v_i_event";
		conditions = "limit 100000";
		
		String workspace = System.getProperty("user.dir");
		String packageName = this.getClass().getPackage().getName();
		schemaMappingFile = workspace + "/src/test/resources/" + packageName.replaceAll("\\.", "/") + "/" + schemaMappingFile;
		LOG.info("schemaMappingFile=" + schemaMappingFile);
		
		ZkConf zkConf = new ZkConf();
		zkConf.setZkHost(zkHost);
		ClientConf clientConf = new ClientConf(zkConf);
		clientConf.setCollectionName(collection);
		clientConf.setBatchCount(batchCount);
		clientConf.setSchemaMappingFile(schemaMappingFile);
		LOG.info("Solr client configuration: " + clientConf);
		
		HiveConf hiveConf = new HiveConf(url, user, password, table, conditions);
		LOG.info("Hive configuration: " + hiveConf);
		
		indexer = new MultiThreadedDBIndexer(clientConf, hiveConf, threadCount);
	}
	
	@Test
	public void indexDocs() throws Throwable {
		indexer.indexDocs();
	}
	
	@After
	public void destroy() throws IOException {
		indexer.close();
	}
	
}
