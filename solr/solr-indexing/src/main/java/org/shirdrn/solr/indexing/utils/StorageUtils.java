package org.shirdrn.solr.indexing.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class StorageUtils {

	private static final  ConcurrentMap<String, Connection> pooledConnections = new ConcurrentHashMap<>(1);
	private static final Lock lock = new ReentrantLock();
	static {
		Runtime.getRuntime().addShutdownHook(new Thread() {

			@Override
			public void run() {
				lock.lock();
				try {
					Iterator<Entry<String, Connection>> iter = pooledConnections.entrySet().iterator();
					while(iter.hasNext()) {
						try {
							Entry<String, Connection> entry = iter.next();
							if(entry.getValue() != null && !entry.getValue().isClosed()) {
								entry.getValue().close();
							}
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				} finally {
					lock.unlock();
				}
			}
		});
	}
	
	public static void loadDrivers(String driverClass) {
		try {
			Class.forName(driverClass);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public static Connection getConnection(String jdbcUrl) {
		Connection conn = null;
		lock.lock();
		try {
			conn = pooledConnections.get(jdbcUrl);
			if(conn == null || conn.isClosed()) {
				// create a connection
				conn = DriverManager.getConnection(jdbcUrl);
				pooledConnections.put(jdbcUrl, conn);
			}
		} catch (Exception e) {
			lock.unlock();
			throw new RuntimeException(e);
		} finally {
			lock.unlock();
		}
		return conn;
	}
	
	public static void close(Connection conn, ResultSet rs, Statement stmt) {
		try {
			if(rs != null) {
				rs.close();
			}
			if(stmt != null) {
				stmt.close();
			}
			if(conn != null) {
				conn.close();
				lock.lock();
				try {
					Iterator<Entry<String, Connection>> iter = pooledConnections.entrySet().iterator();
					while(iter.hasNext()) {
						Entry<String, Connection> entry = iter.next();
						if(conn.equals(entry.getValue())) {
							iter.remove();
							break;
						}
					}
				} finally {
					lock.unlock();
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void close(ResultSet rs, Statement stmt) {
		close(null, rs, stmt);
	}
	
}
