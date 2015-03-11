package org.shirdrn.solr.indexing.common;

public interface IndexingManager {

	void parseProperties();
	void readMappings();
	int getInt(String key, int defaultValue);
	String get(String key);
	String get(String key, String defaultValue);
	
	void buildIndexes(String[] args) throws Throwable;
	void dumpStatFor(String collection);
	void dumpStatAll();
}
