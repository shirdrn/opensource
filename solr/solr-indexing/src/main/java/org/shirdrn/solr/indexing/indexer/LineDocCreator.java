package org.shirdrn.solr.indexing.indexer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.common.SolrInputDocument;
import org.shirdrn.solr.indexing.common.DocCreator;
import org.shirdrn.solr.indexing.common.config.FieldMappingBuilder;

public class LineDocCreator implements DocCreator<String> {
	private static final Log LOG = LogFactory.getLog(LineDocCreator.class);
	private final String lineSeparator;
	private FieldMappingBuilder builder;
	
	public LineDocCreator(FieldMappingBuilder builder) {
		this(builder, ",");
	}
	
	public LineDocCreator(FieldMappingBuilder builder, String lineSeparator) {
		this.builder = builder;
		this.lineSeparator = lineSeparator;
	}
	
	@Override
	public SolrInputDocument createDoc(String docValue) {
		LOG.debug("Input line: docValue=" + docValue);
		SolrInputDocument doc = new SolrInputDocument();
		if(docValue != null && !docValue.trim().isEmpty()) {
			String[] values = docValue.split(lineSeparator);
			for (int i = 0; i < values.length; i++) {
				String field = builder.getMappedField(i).getField();
				LOG.debug("\tfield=" + field + ", value=" + values[i]);
				if(values[i] != null && !values[i].trim().isEmpty()) {
					doc.addField(field, 
							builder.getMappedField(field).valueHandler.handle(values[i]));
				}
			}
		}
		return doc;
	}
}