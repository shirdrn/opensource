package org.shirdrn.platform.dubbo.service.rpc.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;
import org.shirdrn.platform.dubbo.service.rpc.api.SolrSearchService.ResponseType;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class TestSearchConsumer {

	final String collection = "tinycollection";
	final String beanXML = "search-consumer.xml";
	final String config = SearchConsumer.class.getPackage().getName().replace('.', '/') + "/" + beanXML;
	SearchConsumer consumer;
	
	@Before
	public void initialize() {
		consumer = new SearchConsumer(collection, new Callable<AbstractXmlApplicationContext>() {
			public AbstractXmlApplicationContext call() throws Exception {
				final AbstractXmlApplicationContext context = new ClassPathXmlApplicationContext(config);
				return context;
			}
		});
	}
	
	@Test
	public void asyncCall() throws Exception {
		String q = "q=上海&fl=*&fq=building_type:1";
		int start = 0;
		int rows = 10;
		ResponseType type  = ResponseType.XML;
		
		final BlockingQueue<Future<String>> queue = 
				new LinkedBlockingQueue<Future<String>>();
		final AtomicBoolean completed = new AtomicBoolean(false);
		
		Thread worker = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					while(!completed.get()) {
						while(!queue.isEmpty()) {
							Future<String> f = queue.peek();
							if(f != null) {
								String result = f.get(1, TimeUnit.MILLISECONDS);
								System.out.println("Result: " + result);
								queue.remove(f);
							}
						}
						Thread.sleep(5);
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (TimeoutException e) {
					e.printStackTrace();
				}				
			}
		});
		worker.start();
				
		for (int i = 0; i < 3; i++) {
			start = 1 * 10 * i;
			if(i % 2 == 0) {
				type = ResponseType.XML;
			} else {
				type = ResponseType.JSON;
			}
			Thread.sleep(200);

			Future<String> future = consumer.asyncCall(q, type, start, rows);
			if(future != null) {
				queue.add(future);
			}
		}
		completed.compareAndSet(false, true);
	}
	
	@Test
	public void syncCall() throws Exception {
		String q = "q=上海&fl=*&fq=building_type:1";
		int start = 0;
		int rows = 10;
		ResponseType type  = ResponseType.XML;
		for (int i = 0; i < 3; i++) {
			start = 1 * 10 * i;
			if(i % 2 == 0) {
				type = ResponseType.XML;
			} else {
				type = ResponseType.JSON;
			}
			String result = consumer.syncCall(q, type, start, rows);
			System.out.println("Result: " + result);
		}
	}
}
