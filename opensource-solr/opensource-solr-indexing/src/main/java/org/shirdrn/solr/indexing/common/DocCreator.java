package org.shirdrn.solr.indexing.common;

import org.apache.solr.common.SolrInputDocument;

public interface DocCreator<T> {
	SolrInputDocument createDoc(T docValue);
}


