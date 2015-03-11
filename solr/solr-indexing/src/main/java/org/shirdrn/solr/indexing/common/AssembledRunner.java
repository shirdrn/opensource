package org.shirdrn.solr.indexing.common;

import java.io.IOException;

public interface AssembledRunner {
	void assembleAndRun(String... params) throws IOException;
}
