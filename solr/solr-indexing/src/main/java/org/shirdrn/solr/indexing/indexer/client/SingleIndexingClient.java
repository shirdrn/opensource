package org.shirdrn.solr.indexing.indexer.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.shirdrn.solr.indexing.common.DocCreator;
import org.shirdrn.solr.indexing.common.IndexingService;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.FieldMappingBuilder;
import org.shirdrn.solr.indexing.utils.SolrUtils;

public final class SingleIndexingClient implements IndexingService, Closeable {

	private static final Log LOG = LogFactory.getLog(SingleIndexingClient.class);
	private final CloudSolrServer cloudSolrServer;
	private final ClientConf clientConf;
	private final FieldMappingBuilder builder;
	private boolean isClosed = false;
	private volatile int totalCount = 0;
	private volatile int indexedCount = 0;
	private volatile int failureCount = 0;
	private Collection<SolrInputDocument> docs = new ArrayList<>(0);
	
	private SingleIndexingClient(ClientConf clientConf) {
		this.builder = SolrUtils.createFieldMappingBuilder(clientConf);
		this.clientConf = clientConf;
		cloudSolrServer = SolrUtils.createServer(clientConf);
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if(!isClosed && cloudSolrServer != null) {
					shutdownAndLogging();
				}
			}
		});
	}
	
	public static IndexingService newIndexingClient(ClientConf clientConf) {
		return new SingleIndexingClient(clientConf);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> void addDoc(T docValue) throws SolrServerException, IOException {
		// total counter
		incrTotalCount(1);
		DocCreator<T> creator = (DocCreator<T>) builder.getDocCreator();
		SolrInputDocument doc = creator.createDoc(docValue);
		docs.add(doc);
		if(docs.size() >= clientConf.getBatchCount()) {
			cloudSolrServer.add(docs);
			LOG.debug("Added docs: " + docs);
			commit(false, false, true);
			LOG.info("Commit: count=" + docs.size());
			// success counter
			incrIndexedCount(docs.size());
			docs = new ArrayList<>(0);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> void addDocs(Collection<T> docValues) throws SolrServerException, IOException {
		// total counter
		incrTotalCount(docValues.size());
		LOG.debug("docValues=" + docValues);
		Collection<SolrInputDocument> docList = new ArrayList<>(0);
		DocCreator<T> creator = (DocCreator<T>) builder.getDocCreator();
		for(T docValue : docValues) {
			SolrInputDocument doc = creator.createDoc(docValue);
			docList.add(doc);
		}
		try {
			cloudSolrServer.add(docList);
			LOG.debug("Added docs: " + docList);
			LOG.info("Added docs: count=" + docList.size());
			commit(false, false, true);
			LOG.info("Commit.");
			// success counter
			incrIndexedCount(docValues.size());
		} catch (Exception e) {
			e.printStackTrace();
			rollback(docList);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> void addDocAndCommit(T docValue) throws SolrServerException, IOException {
		// total counter
		incrTotalCount(1);
		DocCreator<T> creator = (DocCreator<T>) builder.getDocCreator();
		SolrInputDocument doc = creator.createDoc(docValue);
		try {
			cloudSolrServer.add(doc);
			LOG.info("Added doc: " + doc);
			commit(false, false, true);
			LOG.info("Commit.");
			// success counter
			incrIndexedCount(1);
		} catch (Exception e) {
			e.printStackTrace();
			rollback(doc);
		}
	}

	@Override
	public void finallyCommit() throws SolrServerException, IOException {
		if(!docs.isEmpty()) {
			cloudSolrServer.add(docs);
			LOG.info("Add docs: count=" + docs.size());
			try {
				commit(false, false, true);
				LOG.info("Commit.");
				// success counter
				incrIndexedCount(docs.size());
				docs = new ArrayList<>(0);
			} catch (Exception e) {
				e.printStackTrace();
				rollback(docs);
			}
		}
	}
	
	private void commit(boolean waitFlush, boolean waitSearcher, boolean softCommit) throws SolrServerException, IOException {
		cloudSolrServer.commit(false, false, true);
	}
	
	private <D> void rollback(D doc) throws SolrServerException, IOException {
		LOG.error("Caught exceptions, rollback...");
		LOG.error("Rollbacked documents: " + doc);
		cloudSolrServer.rollback();
	}
	
	public int incrIndexedCount(int count) {
		indexedCount += count;
		return indexedCount;
	}
	
	public int incrTotalCount(int count) {
		totalCount += count;
		return totalCount;
	}
	
	@Override
	public void close() throws IOException {
		try {
			Thread.sleep(5);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		shutdownAndLogging();
	}

	private void shutdownAndLogging() {
		try {
			cloudSolrServer.shutdown();
		} catch (Exception e) {
			LOG.warn("Ignore: " + e);
		}
		isClosed = true;
		LOG.info("Solr cloud server is closed.");
		LOG.info("Indexing statistics: "
				+ "totalCount=" + totalCount + ", "
				+ "indexedCount=" + indexedCount + ", "
				+ "failureCount=" + failureCount);
	}

	public int getIndexedCount() {
		return indexedCount;
	}

	public int getFailuerCount() {
		return totalCount - indexedCount;
	}

	public int getTotalCount() {
		return totalCount;
	}
	
	public FieldMappingBuilder getBuilder() {
		return builder;
	}
}
