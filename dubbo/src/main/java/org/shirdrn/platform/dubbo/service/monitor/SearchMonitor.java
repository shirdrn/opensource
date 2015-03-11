package org.shirdrn.platform.dubbo.service.monitor;

import java.util.concurrent.Callable;

import org.springframework.beans.BeansException;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.alibaba.dubbo.monitor.MonitorService;

public class SearchMonitor {
	
	private AbstractXmlApplicationContext context;
	private MonitorService monitorService;
	
	public SearchMonitor(Callable<AbstractXmlApplicationContext> call) {
		super();
		try {
			context = call.call();
			context.start();
			monitorService = (MonitorService) context.getBean("monitorService");
		} catch (BeansException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		final String beanXML = "search-monitor.xml";
		final String config = SearchMonitor.class.getPackage().getName().replace('.', '/') + "/" + beanXML;
		final SearchMonitor monitor = new SearchMonitor(new Callable<AbstractXmlApplicationContext>() {
			public AbstractXmlApplicationContext call() throws Exception {
				return new ClassPathXmlApplicationContext(config);
			}
		});
		
		synchronized(monitor) {
			monitor.wait();
		}
	}

}
