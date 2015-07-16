package org.shirdrn.dubbo.provider.common;

import java.io.IOException;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public abstract class DubboServer {

	public static void startServer(String config) {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(config);
		try {
			context.start();
			System.in.read();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			context.close();
		}
	}
}
