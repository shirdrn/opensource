package org.shirdrn.solr.indexing.common;

public interface ValueHandler<T> {
	T handle(String value);
}
