package org.shirdrn.solr.indexing.indexer.manager;

import org.junit.Test;
import org.shirdrn.solr.indexing.common.AbstractIndexingManager;
import org.shirdrn.solr.indexing.indexer.manager.RememberMaxTimestampIndexingmanager;

public class TestRememberMaxTimestampIndexingmanager {

	@Test
	public void start() {
		String[] args = new String[] {};
		AbstractIndexingManager.startIndexer(RememberMaxTimestampIndexingmanager.class, args);
	}
}
