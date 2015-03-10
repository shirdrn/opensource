package org.shirdrn.solr.indexing.indexer;

import java.util.Map;

import org.apache.solr.common.SolrInputDocument;
import org.shirdrn.solr.indexing.common.DocCreator;
import org.shirdrn.solr.indexing.common.config.FieldMappingBuilder;

public class MapDocCreator implements DocCreator<Map<String, String>> {
	private FieldMappingBuilder buidler;
	
	public MapDocCreator(FieldMappingBuilder buidler) {
		super();
		this.buidler = buidler;
	}

	@Override
	public SolrInputDocument createDoc(Map<String, String> docValue) {
		SolrInputDocument doc = new SolrInputDocument();
		for(Map.Entry<String, String> entry : docValue.entrySet()) {
			doc.addField(entry.getKey(), 
					buidler.getMappedField(entry.getKey()).valueHandler.handle(entry.getValue()));
		}
		return doc;
	}
}
