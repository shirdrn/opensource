package org.shirdrn.solr.indexing.common;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.SolrServerException;

public interface DeletionService {

	void deleteById(List<String> ids) throws SolrServerException, IOException;
	void deleteByQuery(String query) throws SolrServerException, IOException;
	
}
