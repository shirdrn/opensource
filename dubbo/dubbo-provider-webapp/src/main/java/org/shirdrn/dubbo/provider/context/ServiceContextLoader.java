package org.shirdrn.dubbo.provider.context;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.web.context.ContextLoader;

public class ServiceContextLoader extends ContextLoader implements ServletContextListener {

	private ClassPathXmlApplicationContext context;
	
	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		String config = arg0.getServletContext().getInitParameter("contextConfigLocation");
		context = new ClassPathXmlApplicationContext(config);
		context.start();
	}

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		context.stop();
	}

}
