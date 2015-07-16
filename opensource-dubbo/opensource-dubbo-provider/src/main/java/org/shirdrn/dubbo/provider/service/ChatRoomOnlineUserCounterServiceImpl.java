package org.shirdrn.dubbo.provider.service;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.dubbo.api.ChatRoomOnlineUserCounterService;
import org.shirdrn.dubbo.common.utils.DateTimeUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import com.alibaba.dubbo.common.utils.StringUtils;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

public class ChatRoomOnlineUserCounterServiceImpl implements ChatRoomOnlineUserCounterService {

	private static final Log LOG = LogFactory.getLog(ChatRoomOnlineUserCounterServiceImpl.class);
	private JedisPool jedisPool;
	private static final String KEY_USER_COUNT = "live::room::play::user::cnt";
	private static final String KEY_MAX_USER_COUNT_PREFIX = "live::room::max::user::cnt::";
	private static final String DF_YYYYMMDD = "yyyyMMdd";

	public String queryRoomUserCount(String rooms) {
		LOG.info("Params[Server|Recv|REQ] rooms=" + rooms);
		StringBuffer builder = new StringBuffer();
		if(!Strings.isNullOrEmpty(rooms)) {
			Jedis jedis = null;
			try {
				jedis = jedisPool.getResource();
				String[] fields = rooms.split(",");
				List<String> results = jedis.hmget(KEY_USER_COUNT, fields);
				builder.append(StringUtils.join(results, ","));
			} catch (Exception e) {
				LOG.error("", e);
			} finally {
				if(jedis != null) {
					jedis.close();
				}
			}
		}
		LOG.info("Result[Server|Recv|RES] " + builder.toString());
		return builder.toString();
	}
	
	@Override
	public List<String> getMaxOnlineUserCount(List<String> rooms, String date, String dateFormat) {
		// HGETALL live::room::max::user::cnt::20150326
		LOG.info("Params[Server|Recv|REQ] rooms=" + rooms + ",date=" + date + ",dateFormat=" + dateFormat);
		String whichDate = DateTimeUtils.format(date, dateFormat, DF_YYYYMMDD);
		String key = KEY_MAX_USER_COUNT_PREFIX + whichDate;
		StringBuffer builder = new StringBuffer();
		if(rooms != null && !rooms.isEmpty()) {
			Jedis jedis = null;
			try {
				jedis = jedisPool.getResource();
				return jedis.hmget(key, rooms.toArray(new String[rooms.size()]));
			} catch (Exception e) {
				LOG.error("", e);
			} finally {
				if(jedis != null) {
					jedis.close();
				}
			}
		}
		LOG.info("Result[Server|Recv|RES] " + builder.toString());
		return Lists.newArrayList();
	}
	
	public void setJedisPool(JedisPool jedisPool) {
		this.jedisPool = jedisPool;
	}

}
