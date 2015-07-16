package org.shirdrn.dubbo.consumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class HttpConsumer {
	// https://github.com/alibaba/dubbo/issues/54
	public static void main(String[] args) throws Exception, IOException {
		CloseableHttpClient httpclient = HttpClients.createDefault();
		try {
			String reqUrl = "http://192.168.14.1:8080/dubbo-provider-webapp/org.shirdrn.dubbo.api.LiveRoomOnlineUserCountService";
			HttpPost httpPost = new HttpPost(reqUrl);
			
			httpPost.setHeader("Content-Type", "application/x-java-serialized-object");
			
			List <NameValuePair> nvPairs = new ArrayList <NameValuePair>();
			nvPairs.add(new BasicNameValuePair("rooms", "1424815101,1408492761,1430546839,1474178862"));
			httpPost.setEntity(new UrlEncodedFormEntity(nvPairs, "UTF-8"));
			System.out.println("Executing request " + httpPost.getRequestLine());
			
			// Create a custom response handler
			ResponseHandler<String> responseHandler = new ResponseHandler<String>() {

				@Override
				public String handleResponse(final HttpResponse response) throws ClientProtocolException, IOException {
					int status = response.getStatusLine().getStatusCode();
					if (status >= 200 && status < 300) {
						HttpEntity entity = response.getEntity();
						return entity != null ? EntityUtils.toString(entity) : null;
					} else {
						throw new ClientProtocolException("Unexpected response status: " + status);
					}
				}

			};
			String responseBody = httpclient.execute(httpPost, responseHandler);
			System.out.println("----------------------------------------");
			System.out.println(responseBody);
		} finally {
			httpclient.close();
		}
	}

}
