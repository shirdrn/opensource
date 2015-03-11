package org.shirdrn.platform.dubbo.service.rpc.client;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.shirdrn.platform.dubbo.service.rpc.api.SolrSearchService;
import org.shirdrn.platform.dubbo.service.rpc.api.SolrSearchService.ResponseType;
import org.springframework.beans.BeansException;
import org.springframework.context.support.AbstractXmlApplicationContext;

import com.alibaba.dubbo.rpc.RpcContext;

public class SearchConsumer {
	
	private final String collection;
	private AbstractXmlApplicationContext context;
	private SolrSearchService searchService;
	
	public SearchConsumer(String collection, Callable<AbstractXmlApplicationContext> call) {
		super();
		this.collection = collection;
		try {
			context = call.call();
			context.start();
			searchService = (SolrSearchService) context.getBean("searchService");
		} catch (BeansException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public Future<String> asyncCall(final String q, final ResponseType type, final int start, final int rows) {
		Future<String> future = RpcContext.getContext().asyncCall(new Callable<String>() {
			public String call() throws Exception {
				return search(q, type, start, rows);
			}
		});
		return future;
	}
	
	public String syncCall(final String q, final ResponseType type, final int start, final int rows) {
		return search(q, type, start, rows);
	}

	private String search(final String q, final ResponseType type, final int start, final int rows) {
		return searchService.search(collection, q, type, start, rows);
	}
	

}
