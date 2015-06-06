package org.shirdrn.dubbo.api;

import java.util.List;

public interface ChatRoomOnlineUserCounterService {

	String queryRoomUserCount(String rooms);
	
	List<String> getMaxOnlineUserCount(List<String> rooms, String date, String dateFormat);
}
