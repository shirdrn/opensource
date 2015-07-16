package org.shirdrn.solr.indexing.common;

import java.io.IOException;
import java.util.Collection;

import org.apache.solr.client.solrj.SolrServerException;

public interface IndexingService {
	<T> void addDocAndCommit(T docValue) throws SolrServerException, IOException;
	<T> void addDoc(T docValue) throws SolrServerException, IOException;
	<T> void addDocs(Collection<T> docValues) throws SolrServerException, IOException;
	void finallyCommit() throws SolrServerException, IOException;
}
