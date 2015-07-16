package org.shirdrn.solr.indexing.common;

import org.shirdrn.solr.indexing.indexer.AbstractIndexer.Status;

public interface ArgsAssembler<T> {
	T assemble(String[] args) throws Exception;
	String getUsageArgList();
	int getRequiredArgCount();
	String[] showCLIExamples();
	String getType();
	String getName();
	Status getStatus();
}
