package org.shirdrn.dubbo.provider;

import org.shirdrn.dubbo.provider.common.DubboServer;

public class ChatRoomClusterServer {

	public static void main(String[] args) throws Exception {
		DubboServer.startServer("classpath:provider-cluster.xml");
	}

}
