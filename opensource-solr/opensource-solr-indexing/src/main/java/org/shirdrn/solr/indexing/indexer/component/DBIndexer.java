package org.shirdrn.solr.indexing.indexer.component;

import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.solr.indexing.common.AbstractArgsAssembler;
import org.shirdrn.solr.indexing.common.DocCreator;
import org.shirdrn.solr.indexing.common.IndexingService;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.HiveConf;
import org.shirdrn.solr.indexing.common.config.ZkConf;
import org.shirdrn.solr.indexing.indexer.AbstractIndexer;
import org.shirdrn.solr.indexing.indexer.LineDocCreator;
import org.shirdrn.solr.indexing.indexer.SingleThreadIndexer;
import org.shirdrn.solr.indexing.indexer.client.SingleThreadClient;

public class DBIndexer extends SingleThreadIndexer {

	private static final Log LOG = LogFactory.getLog(DBIndexer.class);
	private HiveConf hiveConf;
	static {
		try {
			Class.forName(DRIVER_CLASS_NAME);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	private final IndexingService client;
	
	public DBIndexer(ClientConf clientConf, HiveConf hiveConf) 
			throws MalformedURLException {
		super(clientConf);
		this.hiveConf = hiveConf;
		DocCreator<String> docCreator = new LineDocCreator(builder);
		builder.setDocCreator(docCreator);
		this.client = SingleThreadClient.newIndexingClient(clientConf, this);
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
				sql += " where " + hiveConf.getConditions();
			}
			LOG.info("Query SQL statement: sql=" + sql);
			rs = stmt.executeQuery(sql);
			
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
					client.addDoc(buf.toString());
				} else {
					LOG.warn("Skip bad record: 0 length.");
					break;
				}
			}
		} finally {
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
	}
	
	public static class Assembler extends AbstractArgsAssembler<AbstractIndexer> {
		
		private static final String TYPE_DB_INDEXER = "2";
		private static final String NAME_DB_INDEXER = "DB_Indexer";

		public Assembler() {
			super();
			argCount = 9;
			type = TYPE_DB_INDEXER;
			name = NAME_DB_INDEXER;
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
			String url;
			String table;
			String conditions;
			AbstractIndexer indexer = null;
			
			zkHost = args[0];
			try {
				connectTimeout = Integer.parseInt(args[1]);
				clientTimeout = Integer.parseInt(args[2]);
				batchCount = Integer.parseInt(args[3]);
			} catch (NumberFormatException e) { }
			collection = args[4];
			schemaMappingFile = args[5];
			url = args[6];
			table = args[7];
			StringBuffer t = new StringBuffer();
			for (int i = 8; i < args.length; i++) {
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
			
			try {
				indexer = new DBIndexer(clientConf, hiveConf);
				setStatus(Status.SUCCESS);
			} catch (Exception e) {
				setStatus(Status.FAILURE);
				throw new RuntimeException(e);
			}
			return indexer;
		}
		
		@Override
		public String getUsageArgList() {
			StringBuffer usage = new StringBuffer();
			usage.append("<0:zkHost> <1:connectTimeout> <2:clientTimeout> <3:batchCount> <4:collection> ")
			.append("<5:schemaMappingFile> <6:jdbcUrl> <7:tableName> <8:queryConditions>");
			return usage.toString();
		}

		@Override
		public String[] showCLIExamples() {
			String[] examples = new String[] {
					"zk:2181 10000 30000 1000 mycollection schema-mapping.xml jdbc:hive2://slave1:21050/default/;auth=noSasl t_i_event id>1 and id<=1",
					"zk:2181 10000 30000 1000 mycollection /home/solr/schemas/schema-mapping.xml jdbc:hive2://slave1:21050/default t_i_event id>1 and id<=1"
			};
			return examples;
		}
	}
	
}
