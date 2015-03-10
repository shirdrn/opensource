package org.shirdrn.solr.indexing.indexer.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.shirdrn.solr.indexing.common.DocCreator;
import org.shirdrn.solr.indexing.common.IndexingService;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.FieldMappingBuilder;
import org.shirdrn.solr.indexing.indexer.SingleThreadIndexer;

public class SingleThreadClient implements IndexingService {

	private static final Log LOG = LogFactory.getLog(SingleThreadClient.class);
	protected final ClientConf clientConf;
	protected final FieldMappingBuilder builder;
	protected final SingleThreadIndexer indexer;
	private Collection<SolrInputDocument> docs = new ArrayList<>(0);
	
	protected SingleThreadClient(ClientConf clientConf, SingleThreadIndexer indexer) {
		super();
		this.clientConf = clientConf;
		this.builder = indexer.getBuilder();
		this.indexer = indexer;
	}
	
	public static IndexingService newIndexingClient(
			ClientConf clientConf, SingleThreadIndexer indexer) {
		return new SingleThreadClient(clientConf, indexer);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> void addDoc(T docValue) throws SolrServerException, IOException {
		// total counter
		indexer.addAndGetTotalCount(1);
		DocCreator<T> creator = (DocCreator<T>) builder.getDocCreator();
		SolrInputDocument doc = creator.createDoc(docValue);
		docs.add(doc);
		if(docs.size() >= clientConf.getBatchCount()) {
			indexer.getCloudSolrServer().add(docs);
			LOG.debug("Added docs: " + docs);
			commit(false, false, true);
			LOG.info("Commit: count=" + docs.size());
			// success counter
			indexer.addAndGetIndexedCount(docs.size());
			docs = new ArrayList<>(0);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> void addDocs(Collection<T> docValues) throws SolrServerException, IOException {
		// total counter
		indexer.addAndGetTotalCount(docValues.size());
		LOG.debug("docValues=" + docValues);
		Collection<SolrInputDocument> docList = new ArrayList<SolrInputDocument>(0);
		DocCreator<T> creator = (DocCreator<T>) builder.getDocCreator();
		for(T docValue : docValues) {
			SolrInputDocument doc = creator.createDoc(docValue);
			docList.add(doc);
		}
		try {
			indexer.getCloudSolrServer().add(docList);
			LOG.debug("Added docs: " + docList);
			LOG.info("Added docs: count=" + docList.size());
			commit(false, false, true);
			LOG.info("Commit.");
			// success counter
			indexer.addAndGetIndexedCount(docValues.size());
		} catch (Exception e) {
			e.printStackTrace();
			rollback(docList);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> void addDocAndCommit(T docValue) throws SolrServerException, IOException {
		// total counter
		indexer.addAndGetTotalCount(1);
		DocCreator<T> creator = (DocCreator<T>) builder.getDocCreator();
		SolrInputDocument doc = creator.createDoc(docValue);
		try {
			indexer.getCloudSolrServer().add(doc);
			LOG.info("Added doc: " + doc);
			commit(false, false, true);
			LOG.info("Commit.");
			// success counter
			indexer.addAndGetIndexedCount(1);
		} catch (Exception e) {
			e.printStackTrace();
			rollback(doc);
		}
	}

	@Override
	public void finallyCommit() throws SolrServerException, IOException {
		if(!docs.isEmpty()) {
			indexer.getCloudSolrServer().add(docs);
			LOG.info("Add docs: count=" + docs.size());
			try {
				commit(false, false, true);
				LOG.info("Commit.");
				// success counter
				indexer.addAndGetIndexedCount(docs.size());
				docs = new ArrayList<>(0);
			} catch (Exception e) {
				e.printStackTrace();
				rollback(docs);
			}
		}
	}
	
	protected void commit(boolean waitFlush, boolean waitSearcher, boolean softCommit) throws SolrServerException, IOException {
		// waitFlush=false, waitSearcher=false, softCommit=true
		indexer.getCloudSolrServer().commit(waitFlush, waitSearcher, softCommit);
	}
	
	protected <D> void rollback(D doc) throws SolrServerException, IOException {
		LOG.error("Caught exceptions, rollback...");
		LOG.error("Rollbacked documents: " + doc);
		indexer.getCloudSolrServer().rollback();
	}

}
