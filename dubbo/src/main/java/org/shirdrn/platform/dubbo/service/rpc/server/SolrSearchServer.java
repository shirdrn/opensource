package org.shirdrn.platform.dubbo.service.rpc.server;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.platform.dubbo.service.rpc.api.SolrSearchService;
import org.shirdrn.platform.dubbo.service.rpc.utils.QueryPostClient;

public class SolrSearchServer implements SolrSearchService {

	private static final Log LOG = LogFactory.getLog(SolrSearchServer.class);
	private String baseUrl;
	private final QueryPostClient postClient;
	private static final Map<ResponseType, FormatHandler> handlers = new HashMap<ResponseType, FormatHandler>(0);
	static {
		handlers.put(ResponseType.XML, new FormatHandler() {
			public String format() {
				return "&wt=xml";
			}
		});
		handlers.put(ResponseType.JSON, new FormatHandler() {
			public String format() {
				return "&wt=json";
			}
		});
	}
	
	public SolrSearchServer() {
		super();
		postClient = QueryPostClient.newIndexingClient(null);
	}
	
	public void setBaseUrl(String baseUrl) {
		this.baseUrl = baseUrl;
	}

	public String search(String collection, String q, ResponseType type,
			int start, int rows) {
		StringBuffer url = new StringBuffer();
		url.append(baseUrl).append(collection).append("/select?").append(q);
		url.append("&start=").append(start).append("&rows=").append(rows);
		url.append(handlers.get(type).format());
		LOG.info("[REQ] " + url.toString());
		return postClient.request(url.toString());
	}
	
	interface FormatHandler {
		String format();
	}
	
}
