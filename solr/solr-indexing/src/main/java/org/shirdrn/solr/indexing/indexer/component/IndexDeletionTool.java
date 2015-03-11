package org.shirdrn.solr.indexing.indexer.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.shirdrn.solr.indexing.common.AbstractArgsAssembler;
import org.shirdrn.solr.indexing.common.DeletionService;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.ZkConf;
import org.shirdrn.solr.indexing.indexer.AbstractIndexer;
import org.shirdrn.solr.indexing.indexer.SingleThreadIndexer;
import org.shirdrn.solr.indexing.indexer.client.SingleThreadClient;

public class IndexDeletionTool extends SingleThreadIndexer {

	private static final Log LOG = LogFactory.getLog(IndexDeletionTool.class);
	private final DeletionService deletionService;
	private final String query;
	private final List<String> ids;
	
	public IndexDeletionTool(ClientConf clientConf, String query) {
		this(clientConf, null, query);
	}
	
	public IndexDeletionTool(ClientConf clientConf, List<String> ids) {
		this(clientConf, ids, null);
	}
	
	private IndexDeletionTool(ClientConf clientConf, List<String> ids, String query) {
		super(clientConf);
		deletionService = NativeClient.newIndexDeletionClient(clientConf, this);
		this.query = query;
		this.ids = ids;
	}
	
	@Override
	protected void process() throws Exception {
		if(query != null) {
			deletionService.deleteByQuery(query);
		} else if(ids != null && !ids.isEmpty()) {
			deletionService.deleteById(ids);
		}
	}
	
	static class NativeClient extends SingleThreadClient implements DeletionService {
		
		public NativeClient(ClientConf clientConf, SingleThreadIndexer indexer) {
			super(clientConf, indexer);
		}
		
		public static DeletionService newIndexDeletionClient(
				ClientConf clientConf, SingleThreadIndexer indexer) {
			return new NativeClient(clientConf, indexer);
		}
		
		@Override
		public void deleteById(List<String> ids) throws SolrServerException, IOException {
			indexer.addAndGetTotalCount(ids.size());
			if(!ids.isEmpty()) {
				int logCount = Math.min(10, ids.size());
				StringBuffer docs = new StringBuffer();
				docs.append("[");
				for (int i = 0; i < logCount; i++) {
					docs.append(ids.get(i)).append(",");
				}
				docs.append(" ...]");
				try {
					LOG.info("Delete documents: " + docs.toString());
					indexer.getCloudSolrServer().deleteById(ids);
					commit(false, false, true);
					indexer.addAndGetDeletedCount(ids.size());
					LOG.info("Done!");
				} catch (Exception e) {
					e.printStackTrace();
					rollback(ids);
				}
			}
		}

		@Override
		public void deleteByQuery(String query) throws SolrServerException, IOException {
			LOG.info("Delete by queries: " + query);
			if(query!=null && !query.trim().isEmpty()) {
				try {
					UpdateResponse res = indexer.getCloudSolrServer().deleteByQuery(query);
					LOG.info("ElapsedTime: " + res.getElapsedTime());
					LOG.info("Response: " + res.getResponse());
					commit(false, false, true);
					LOG.info("Done!");
				} catch (Exception e) {
					e.printStackTrace();
					rollback(query);
				}
			}
		}
		
	}
	
	public static class Assembler extends AbstractArgsAssembler<AbstractIndexer> {

		private static final String TYPE_INDEX_DELETION_TOOL = "11";
		private static final String NAME_INDEX_DELETION_TOOL = "Index_Deletion_Tool";
		
		public Assembler() {
			super();
			argCount = 7;
			type = TYPE_INDEX_DELETION_TOOL;
			name = NAME_INDEX_DELETION_TOOL;
		}
		
		@Override
		public AbstractIndexer assemble(String[] args) throws Exception {
			super.assemble(args);
			String zkHost;
			int connectTimeout = 10000;
			int clientTimeout = 30000;
			String collection;
			String schemaMappingFile;
			String deleteBy;
			
			zkHost = args[0];
			try {
				connectTimeout = Integer.parseInt(args[1]);
				clientTimeout = Integer.parseInt(args[2]);
			} catch (NumberFormatException e) { }
			collection = args[3];
			schemaMappingFile = args[4];
			deleteBy = args[5];
			
			ZkConf zkConf = new ZkConf();
			zkConf.setZkHost(zkHost);
			zkConf.setZkConnectTimeout(connectTimeout);
			zkConf.setZkClientTimeout(clientTimeout);
			ClientConf clientConf = new ClientConf(zkConf);
			clientConf.setCollectionName(collection);
			clientConf.setSchemaMappingFile(schemaMappingFile);
			
			if(deleteBy == null 
					|| !(deleteBy.equalsIgnoreCase("query") && deleteBy.equalsIgnoreCase("ids"))) {
				return null;
			}
			
			AbstractIndexer tool = null;
			if(deleteBy.equalsIgnoreCase("query")) {
				StringBuffer query = new StringBuffer();
				for (int i = 6; i < args.length; i++) {
					query.append(args[i]).append(" ");					
				}
				tool = new IndexDeletionTool(clientConf, query.toString().trim());
			} else if(deleteBy.equalsIgnoreCase("ids")) {
				List<String> ids = new ArrayList<String>(0);
				String[] idArray = args[6].split(",");
				for(String id : idArray) {
					ids.add(id.trim());
				}
				tool = new IndexDeletionTool(clientConf, ids);
			}
			return tool;
		}
		
		@Override
		public String getUsageArgList() {
			StringBuffer usage = new StringBuffer();
			usage.append("<0:zkHost> <1:connectTimeout> <2:clientTimeout> <3:collection> ")
			.append("<4:schemaMappingFile> <5:deleteBy(ids|query)> <6:idList|queryValue>");
			return usage.toString();
		}

		@Override
		public String[] showCLIExamples() {
			return new String[] {
					"zk:2181 10000 30000 mycollection schema-mapping.xml ids 1,2,3,4,5,6,7",
					"zk:2181 10000 30000 mycollection schema-mapping.xml query area:北京 AND latitue:[23.8767 TO 46.9980]",
					"zk:2181 10000 30000 mycollection schema-mapping.xml query *:*",
					"zk:2181 10000 30000 mycollection /home/solr/schemas/schema-mapping.xml ids 1,2,3,4,5,6,7",
					"zk:2181 10000 30000 mycollection /home/solr/schemas/schema-mapping.xml query area:北京 AND latitue:[23.8767 TO 46.9980]",
					"zk:2181 10000 30000 mycollection /home/solr/schemas/schema-mapping.xml query *:*"
			};
		}
		
	}

}
