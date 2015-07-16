package org.shirdrn.solr.indexing.indexer.component;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.shirdrn.solr.indexing.common.AbstractArgsAssembler;
import org.shirdrn.solr.indexing.common.IndexingService;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.HiveConf;
import org.shirdrn.solr.indexing.common.config.ZkConf;
import org.shirdrn.solr.indexing.indexer.AbstractIndexer;
import org.shirdrn.solr.indexing.indexer.MultiThreadedIndexer;
import org.shirdrn.solr.indexing.indexer.client.MultiThreadedClient;

public class MultiThreadedDBIndexer extends MultiThreadedIndexer {

	private static final Log LOG = LogFactory.getLog(MultiThreadedDBIndexer.class);
	private static String DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";
	private final HiveConf hiveConf;
	private final Object waitingLock = new Object();
	private volatile int groupCount;
	private final AtomicInteger counter = new AtomicInteger(0);
	private boolean iterateToEnd = false;
	static {
		try {
			Class.forName(DRIVER_CLASS_NAME);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public MultiThreadedDBIndexer(ClientConf clientConf, HiveConf hiveConf, int nThreads) {
		super(clientConf, nThreads);
		this.hiveConf = hiveConf;
	}
	
	@Override
	protected void process() throws Exception {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = DriverManager.getConnection(hiveConf.getUrl());
			stmt = conn.createStatement();
			String sql = "select * from " + hiveConf.getTable();
			if(hiveConf.getConditions() != null) {
				if(hiveConf.getConditions().trim().toLowerCase().startsWith("limit ")) {
					sql += " " + hiveConf.getConditions();
				} else {
					sql += " where " + hiveConf.getConditions();
				}
			}
			LOG.info("Query SQL statement: sql=" + sql);
			rs = stmt.executeQuery(sql);
			List<String> records = new ArrayList<String>(clientConf.getBatchCount());
			
			while(rs.next()) {
				StringBuffer buf = new StringBuffer();
				for(String field : builder.getFields()) {
					LOG.debug("Get field name: " + "name=" + field);
					String value = rs.getString(field);
					LOG.debug("Get field value: " + field + "=" + value);
					if(value != null 
							&& !value.trim().isEmpty() 
							&& !value.trim().toLowerCase().equals("null")) {
						buf.append(value).append(",");
					} else {
						buf.append("").append(",");
					}
				}
				if(buf.length() > 0) {
					buf.deleteCharAt(buf.length()-1);
					records.add(buf.toString());
				} else {
					LOG.warn("Skip bad record: 0 length.");
					break;
				}
				LOG.debug("buf=" + buf.toString());
				if(records.size() >= clientConf.getBatchCount()) {
					submit(records);
			    	records = new ArrayList<String>(clientConf.getBatchCount());
			    	groupCount++;
				}
			}
			if(!records.isEmpty()) {
				submit(records);
				groupCount++;
			}
		} finally {
			iterateToEnd = true;
			try {
				if(rs != null) {
					rs.close();
				}
				if(stmt != null) {
					stmt.close();
				}
				if(conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				throw e;
			}
		}
	    
		if(groupCount > 0) {
			synchronized(waitingLock) {
				try {
					waitingLock.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private void submit(List<String> records) {
		IndexingService client = MultiThreadedClient.newIndexingClient(clientConf, this);
		Worker worker = new Worker(client, records);
		super.execute(worker);
	}
	
	private void finishNotify() {
		if(groupCount >= counter.get() && iterateToEnd) {
			synchronized(waitingLock) {
				waitingLock.notify();
			}
		}
	}
	
	@Override
	public void close() throws IOException {
		try {
			Thread.sleep(3000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		super.close();
	}
	
	class Worker implements Runnable {
		private final IndexingService client;
		private final Collection<String> records;
		
		public Worker(final IndexingService client, Collection<String> records) {
			super();
			this.client = client;
			this.records = records;
		}

		@Override
		public void run() {
			counter.incrementAndGet();
			try {
				client.addDocs(records);
			} catch (SolrServerException | IOException e) {
				e.printStackTrace();
			} finally {
				try {
					Thread.sleep(500);
					client.finallyCommit();
					MultiThreadedClient thisClient = ((MultiThreadedClient)client);
					thisClient.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
				finishNotify();
			}
			
		}

	}

	public static class Assembler extends AbstractArgsAssembler<AbstractIndexer> {
		private static final String TYPE_MULTITHREADED_DB_INDEXER = "3";
		private static final String NAME_MULTITHREADED_DB_INDEXER = "Multithreaded_DB_Indexer";

		public Assembler() {
			super();
			argCount = 10;
			type = TYPE_MULTITHREADED_DB_INDEXER;
			name = NAME_MULTITHREADED_DB_INDEXER;
		}
		
		@Override
		public AbstractIndexer assemble(String[] args) throws Exception {
			super.assemble(args);
			
			String zkHost;
			int connectTimeout = 10000;
			int clientTimeout = 30000;
			String collection;
			int batchCount = 1000;
			String schemaMappingFile;
			int threadCount = 1;
			String url;
			String table;
			String conditions;
			AbstractIndexer indexer = null;
			
			zkHost = args[0];
			try {
				connectTimeout = Integer.parseInt(args[1]);
				clientTimeout = Integer.parseInt(args[2]);
				batchCount = Integer.parseInt(args[3]);
				threadCount = Integer.parseInt(args[4]);
			} catch (NumberFormatException e) { }
			collection = args[5];
			schemaMappingFile = args[6];
			url = args[7];
			table = args[8];
			StringBuffer t = new StringBuffer();
			for (int i = 9; i < args.length; i++) {
				t.append(args[i]).append(" ");			
			}
			conditions = t.toString().trim();
			
			ZkConf zkConf = new ZkConf();
			zkConf.setZkHost(zkHost);
			zkConf.setZkConnectTimeout(connectTimeout);
			zkConf.setZkClientTimeout(clientTimeout);
			ClientConf clientConf = new ClientConf(zkConf);
			clientConf.setCollectionName(collection);
			clientConf.setSchemaMappingFile(schemaMappingFile);
			clientConf.setBatchCount(batchCount);
			
			HiveConf hiveConf = new HiveConf(url, table, conditions);
			
			indexer = new MultiThreadedDBIndexer(clientConf, hiveConf, threadCount);
			return indexer;
		}
		
		@Override
		public String getUsageArgList() {
			StringBuffer usage = new StringBuffer();
			usage.append("<0:zkHost> <1:connectTimeout> <2:clientTimeout> <3:batchCount> <4:nThreads> <5:collection> ")
			.append("<6:schemaMappingFile> <7:jdbcUrl> <8:tableName> <9:queryConditions>");
			return usage.toString();
		}
		
		@Override
		public String[] showCLIExamples() {
			String[] examples = new String[] {
					"zk:2181 10000 30000 1000 15 mycollection schema-mapping.xml jdbc:hive2://slave1:21050/default/;auth=noSasl t_i_event id>1 and id<=1",
					"zk:2181 10000 30000 1000 15 mycollection /home/solr/schemas/schema-mapping.xml jdbc:hive2://slave1:21050/default t_i_event id>1 and id<=1"
			};
			return examples;
		}
	}
	
}
