package org.shirdrn.solr.indexing.indexer.manager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.solr.indexing.common.AbstractIndexingManager;
import org.shirdrn.solr.indexing.common.ArgsAssembler;
import org.shirdrn.solr.indexing.indexer.AbstractIndexer;

public class SingleIndexingManager extends AbstractIndexingManager {

	private static final Log LOG = LogFactory.getLog(SingleIndexingManager.class);
	private String timeTypeValue;
	private String startTimeId;
	private String finishTimeId;
	
	public SingleIndexingManager() {
		super();
	}
	
	@Override
	public void buildIndexes(String[] args) throws Throwable {
		// args[]: timeType startTime finishTime table1[ table2]
		if(args != null && args.length >= 4) {
			this.timeTypeValue = args[0];
			this.startTimeId = args[1];
			this.finishTimeId = args[2];
			String[] tables = args[3].split(",");
			for (int i = 0; i < tables.length; i++) {
				MappedConf mappedConf = null;
				StatInfo stat = null;
				try {
					mappedConf = getMappedConfByTable(tables[i]);
					if(mappedConf == null) {
						throw new RuntimeException("Missing configuration for table: " + tables[i]);
					}
					stat = super.getStat(mappedConf.getCollection());
					AbstractIndexer indexer = newIndexer(mappedConf, stat);
					indexer.indexDocs();
					indexer.close();
				} catch (Exception e) {
					stat.getCauses().add(e);
				} finally {
					// dump stat information for a collection
					super.dumpStatFor(mappedConf.getCollection());
				}
			}
		} else {
			throw new Exception("Invalid arguments: " + args.length);
		}
		
	}
	
	protected AbstractIndexer newIndexer(MappedConf mappedConf, StatInfo stat) {
		ArgsAssembler<? extends AbstractIndexer> assembler = null;
		AbstractIndexer indexer = null;
		stat.setMappedConf(mappedConf);
		stat.setPreviousIndexTime(startTimeId);
		stat.setLastIndexTime(finishTimeId);
		try {
			String conditions = genQueryConditions(mappedConf);
			stat.setQueryCondition(conditions);
			assembler = super.createAssembler();
			stat.setAssembler(assembler);
			try {
				indexer = assembler.assemble(new String[] {
						zkHost, String.valueOf(connectTimeout), String.valueOf(clientTimeout), String.valueOf(batchCount), 
						mappedConf.getCollection(), mappedConf.getSchemaMappingFile(),
						jdbcUrl, mappedConf.getTableName(), conditions
				});
			} catch (Exception e) {
				throw e;
			}
		} catch (Throwable t) {
			stat.getCauses().add(t);
			LOG.error(t);
		}
		stat.setIndexer(indexer);
		return indexer;
	}

	private String genQueryConditions(MappedConf mappedConf) {
		StringBuffer condition = new StringBuffer();
		condition
		.append(timeTypeField).append("='").append(timeTypeValue).append("' AND ")
		.append(timeIdField).append(">='").append(startTimeId).append("'").append(" AND ")
		.append(timeIdField).append("<='").append(finishTimeId).append("'");
		LOG.info("Query condition: " + condition.toString());
		return condition.toString();
	}

}
