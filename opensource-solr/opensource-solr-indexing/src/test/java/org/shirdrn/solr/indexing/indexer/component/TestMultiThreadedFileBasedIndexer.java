package org.shirdrn.solr.indexing.indexer.component;

import java.io.IOException;
import java.net.MalformedURLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.ZkConf;
import org.shirdrn.solr.indexing.indexer.AbstractIndexer;
import org.shirdrn.solr.indexing.indexer.component.MultiThreadedFilesIndexer;

public class TestMultiThreadedFileBasedIndexer {

	private static final Log LOG = LogFactory.getLog(TestMultiThreadedFileBasedIndexer.class);
	AbstractIndexer indexer;
	String zkHost;
	String collection;
	int batchCount;
	String schemaMappingFile = "schema-mapping.xml";
	int connectTimeout;
	int clientTimeout;
	int threadCount;
	String dir;
	
	@Before
	public void initialize() {
		String zkHost = "slave1:2188";
		String collection = "mycollection";
		batchCount = 100;
		threadCount = 2;
		
		String workspace = System.getProperty("user.dir");
		String packageName = this.getClass().getPackage().getName();
		schemaMappingFile = workspace + "/src/test/resources/" + packageName.replaceAll("\\.", "/") + "/" + schemaMappingFile;
		LOG.info("schemaMappingFile=" + schemaMappingFile);
		
		dir = workspace + "/src/test/resources/" + packageName.replaceAll("\\.", "/") + "/data";
		
		ZkConf zkConf = new ZkConf();
		zkConf.setZkHost(zkHost);
		ClientConf clientConf = new ClientConf(zkConf);
		clientConf.setCollectionName(collection);
		clientConf.setBatchCount(batchCount);
		clientConf.setSchemaMappingFile(schemaMappingFile);
		LOG.info("Solr client configuration: " + clientConf);
		
		try {
			indexer = new MultiThreadedFilesIndexer(clientConf, dir, threadCount);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
