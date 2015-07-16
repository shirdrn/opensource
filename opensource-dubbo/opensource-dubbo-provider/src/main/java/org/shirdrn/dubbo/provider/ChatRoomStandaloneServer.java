package org.shirdrn.dubbo.provider;

import org.shirdrn.dubbo.provider.common.DubboServer;

public class ChatRoomStandaloneServer {

	public static void main(String[] args) throws Exception {
		DubboServer.startServer("classpath:provider-standalone.xml");
	}

}
