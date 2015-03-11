package org.shirdrn.platform.dubbo.service.rpc.api;

public interface SolrSearchService {

	String search(String collection, String q, ResponseType type, int start, int rows);
	
	public enum ResponseType {
		JSON,
		XML
	}
	
}
