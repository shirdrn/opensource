package org.shirdrn.solr.indexing.indexer.manager;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import org.shirdrn.solr.indexing.utils.FileUtils;
import org.shirdrn.solr.indexing.utils.StorageUtils;
import org.shirdrn.solr.indexing.utils.TimeUtils;


public class RememberMaxTimestampIndexingmanager extends ContinuousTimeIntervalIndexingManager {

	static {
		StorageUtils.loadDrivers();
	}
	
	public RememberMaxTimestampIndexingmanager() {
		super();
	}
	
	@Override
	protected Worker makeWorker(Entry<String, MappedConf> entry) {
		return new SmallWorker(entry);
	}


	class SmallWorker extends Worker {

		public SmallWorker(Entry<String, MappedConf> entry) {
			super(entry);
		}

		@Override
		protected String checkConditions(String tableName) {
			Date date = TimeUtils.getDateBefore(Calendar.HOUR_OF_DAY, beforeHours);
			String previousTime = TimeUtils.format(date, timestampFormat);
			StringBuffer condition = new StringBuffer();
			if(!thisFile.exists()) {
				try {
					thisFile.createNewFile();
				} catch (IOException e) {
					WOG.error("Error to create file: " + thisFile.getAbsolutePath());
					throw new RuntimeException(e);
				}
				
			} else {
				List<String> lines = FileUtils.populateListWithLines(thisFile, "UTF-8");
				previousTime = lines.get(0);
			}
			previousIndexTime = previousTime;
			lastIndexTime = computeMaxTimestamp(entry.getValue());
			// time interval: [previousIndexTime, lastIndexTime]
			WOG.info("Query time interval: [" + previousTime + ", " + lastIndexTime + "]");
			condition.append(entry.getValue().getTimestampFieldName()).append(">='").append(previousTime).append("'")
			.append(" AND ").append(entry.getValue().getTimestampFieldName()).append("<='").append(lastIndexTime).append("'");
			return condition.toString();
		}

		private String computeMaxTimestamp(MappedConf mappedConf) {
			Connection conn = null;
			Statement stmt = null;
			ResultSet rs = null;
			String aliasMax = null;
			try {
				conn = StorageUtils.getConnection(jdbcUrl);
				try {
					stmt = conn.createStatement();
					String sql = "SELECT MAX(" + mappedConf.getTimestampFieldName() + ") AS alias_max FROM " + mappedConf.getTableName();
					rs = stmt.executeQuery(sql);
					if(rs.next()) {
						aliasMax = rs.getString("alias_max");
					}
				} catch (SQLException e) {
					throw new RuntimeException(e);
				}
			} finally {
				StorageUtils.close(rs, stmt);
			}
			WOG.info("Max timestamp computed: aliasMax=" + aliasMax);
			return aliasMax;
		}
		
		
		
	}
}
