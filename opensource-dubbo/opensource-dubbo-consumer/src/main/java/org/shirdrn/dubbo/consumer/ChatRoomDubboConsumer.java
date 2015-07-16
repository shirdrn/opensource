package org.shirdrn.dubbo.consumer;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dubbo.api.ChatRoomOnlineUserCounterService;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ChatRoomDubboConsumer {

	private static final Log LOG = LogFactory.getLog(ChatRoomDubboConsumer.class);
	
	public static void main(String[] args) throws Exception {
		AbstractXmlApplicationContext context = new ClassPathXmlApplicationContext("classpath:consumer.xml");
		try {
			context.start();
			ChatRoomOnlineUserCounterService chatRoomOnlineUserCounterService = (ChatRoomOnlineUserCounterService) context.getBean("chatRoomOnlineUserCounterService");
			
			getMaxOnlineUserCount(chatRoomOnlineUserCounterService);
			
			getRealtimeOnlineUserCount(chatRoomOnlineUserCounterService);
			
			System.in.read();
		} finally {
			context.close();
		}
		
	}

	private static void getMaxOnlineUserCount(ChatRoomOnlineUserCounterService liveRoomOnlineUserCountService) {
		List<String> maxUserCounts = liveRoomOnlineUserCountService.getMaxOnlineUserCount(
				Arrays.asList(new String[] {"1482178010" , "1408492761", "1430546839", "1412517075", "1435861734"}), "20150327", "yyyyMMdd");
		LOG.info("After getMaxOnlineUserCount invoked: maxUserCounts= " + maxUserCounts);
	}

	private static void getRealtimeOnlineUserCount(ChatRoomOnlineUserCounterService liveRoomOnlineUserCountService)
			throws InterruptedException {
		String rooms = "1482178010,1408492761,1430546839,1412517075,1435861734";
		String onlineUserCounts = liveRoomOnlineUserCountService.queryRoomUserCount(rooms);
		LOG.info("After queryRoomUserCount invoked: onlineUserCounts= " + onlineUserCounts);
	}
}
