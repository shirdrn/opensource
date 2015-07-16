package org.shirdrn.solr.indexing.indexer;

import org.junit.Test;
import org.shirdrn.solr.indexing.common.config.FieldMappingBuilder;

public class TestFieldMappingBuilder {

	@Test
	public void buildWithoutArgs() {
		FieldMappingBuilder builder = new FieldMappingBuilder();
		builder.build();
		
		System.out.println(builder.getFields());
		System.out.println(builder.getMappedField(0));
		System.out.println(builder.getMappedField("end_time"));
		System.out.println(builder.getValueHandlerByType("int"));
	}
	
	@Test
	public void buildWithArgs() {
		FieldMappingBuilder builder = new FieldMappingBuilder();

		String schemaMappingFile = "my-schema-mapping.xml";
		String workspace = System.getProperty("user.dir");
		String packageName = this.getClass().getPackage().getName();
		schemaMappingFile = workspace + "/src/test/resources/" + packageName.replaceAll("\\.", "/") + "/" + schemaMappingFile;
		
		builder.build(schemaMappingFile);
		
		System.out.println(builder.getFields());
		System.out.println(builder.getMappedField(3));
		System.out.println(builder.getMappedField("log_id"));
		System.out.println(builder.getValueHandlerByType("double"));
	}
}
