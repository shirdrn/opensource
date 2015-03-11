package org.shirdrn.platform.dubbo.service.rpc.server;

import java.io.IOException;

import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class TestSolrSearchServer {

	@Test
	public void  start() throws IOException {
		String config = SolrSearchServer.class.getPackage().getName().replace('.', '/') + "/search-provider.xml";
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(config);
        context.start();
        System.in.read();
	}
	
}
