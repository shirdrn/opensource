package org.shirdrn.solr.indexing.indexer;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.utils.SolrUtils;

public abstract class SingleThreadIndexer extends AbstractIndexer {

	private static final Log LOG = LogFactory.getLog(SingleThreadIndexer.class);
	protected final CloudSolrServer cloudSolrServer;
	
	public SingleThreadIndexer(ClientConf clientConf) {
		super(clientConf);
		LOG.info("Create a connection to cloud solr server...");
		cloudSolrServer = SolrUtils.createServer(clientConf);
		LOG.info("Created.");
	}
	
	@Override
	public void close() throws IOException {
		// close solr cloud server
		cloudSolrServer.shutdown();
		super.close();
	}

	public CloudSolrServer getCloudSolrServer() {
		return cloudSolrServer;
	}

}
