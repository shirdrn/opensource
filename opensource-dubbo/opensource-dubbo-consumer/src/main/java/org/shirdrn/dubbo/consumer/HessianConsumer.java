package org.shirdrn.dubbo.consumer;

import org.shirdrn.dubbo.api.ChatRoomOnlineUserCounterService;

import com.caucho.hessian.client.HessianProxyFactory;

public class HessianConsumer {

	public static void main(String[] args) throws Exception {
		String serviceUrl = "http://192.168.14.1:8080/dubbo-provider-webapp/org.shirdrn.dubbo.api.LiveRoomOnlineUserCountService";
		HessianProxyFactory factory = new HessianProxyFactory();

		for(;;) {
			ChatRoomOnlineUserCounterService chatRoomOnlineUserCounterService = (ChatRoomOnlineUserCounterService) factory.create(ChatRoomOnlineUserCounterService.class, serviceUrl);
			String rooms = "1424815101,1408492761,1430546839,1474178862";
			String result = (String) chatRoomOnlineUserCounterService.queryRoomUserCount(rooms);
			System.out.println(result);
			Thread.sleep(3000);
		}
	}
}
