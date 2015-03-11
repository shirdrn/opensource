package org.shirdrn.platform.dubbo.service.rpc.utils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
import org.shirdrn.platform.dubbo.service.conf.ClientConf;

public final class QueryPostClient implements Closeable {

	private static final Log LOG = LogFactory.getLog(QueryPostClient.class);
	private final HttpClient httpClient;
	private static final Charset charset = Charset.forName("UTF-8");
	
	private QueryPostClient(ClientConf clientConf) {
		httpClient = QueryUtils.createClient(clientConf);
	}
	
	public static QueryPostClient newIndexingClient(ClientConf clientConf) {
		return new QueryPostClient(clientConf);
	}
	
	public String request(String url) {
		String result = null;
		HttpGet get = new HttpGet(url);
		try {
			HttpResponse response = httpClient.execute(get);
			int statusCode = response.getStatusLine().getStatusCode();
			if(statusCode == HttpStatus.SC_OK) {
				HttpEntity entity = response.getEntity();
				if(entity != null) {
					result = EntityUtils.toString(entity, charset);
					
				}
			}
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	public void close() throws IOException {
		if(httpClient != null) {
			httpClient.getConnectionManager().shutdown();
		}
	}
	
}
