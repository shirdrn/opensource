package org.shirdrn.solr.indexing.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.solr.indexing.indexer.AbstractIndexer;
import org.shirdrn.solr.indexing.indexer.AbstractIndexer.Status;
import org.shirdrn.solr.indexing.utils.ReflectionUtils;
import org.shirdrn.solr.indexing.utils.TimeUtils;

public abstract class AbstractIndexingManager implements IndexingManager {

	private static final Log LOG = LogFactory.getLog(AbstractIndexingManager.class);
	protected static final String PROP_FILE_NAME = "solr.properties";
	protected static final String MAPPINGS_FILE_NAME = "mappings.conf";
	private final Properties props = new Properties(); 
	protected String zkHost;
	protected int connectTimeout;
	protected int clientTimeout;
	protected String jdbcUrl;
	protected int batchCount;
	protected String timestampFormat;
	protected final int mappedCount;
	protected String timeTypeField;
	protected String timeIdField;
	protected String indexerAssemblerClassname;
	
	// <collection, mappedConf>
	private final LinkedHashMap<String, MappedConf> mappedByCollection = new LinkedHashMap<>(0);
	// <table, mappedConf>
	private final LinkedHashMap<String, MappedConf> mappedByTable = new LinkedHashMap<>(0);
	// <collection, indexerStat>
	private final ConcurrentHashMap<String, StatInfo> statistics = new ConcurrentHashMap<>(0);
	
	public AbstractIndexingManager(){
        loadProperties();
        readMappings();
        mappedCount = mappedByCollection.size();
        LOG.info("Mapped count: mappedCount=" + mappedCount);
	}
	
	public Iterator<Entry<String, MappedConf>>  getIterator() {
		return mappedByCollection.entrySet().iterator();
	}
	
	@Override
	public void readMappings() {
		InputStream in = this.getClass().getClassLoader().getResourceAsStream(MAPPINGS_FILE_NAME);
        BufferedReader reader = null;
        reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        try {
        	// read and parse configuration file 'mappings.conf'
			while((line = reader.readLine()) != null) {
				if(line != null && !line.trim().isEmpty() 
						&& !line.trim().startsWith("#")) {
					LOG.info("Read line: " + line);
					parseLine(line);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	protected void parseLine(String line) {
		String[] a = line.split("\\s*,\\s*");
		if(a.length == 4) {
			MappedConf mo = new MappedConf();
			mo.collection = a[0];
			mo.tableName = a[1];
			mo.schemaMappingFile = a[2];
			mo.timestampFieldName = a[3];
			mappedByCollection.put(mo.collection, mo);
			mappedByTable.put(mo.tableName, mo);
			statistics.put(mo.collection, new StatInfo());
		}
	}

	private void loadProperties() {
		InputStream in = this.getClass().getClassLoader().getResourceAsStream(PROP_FILE_NAME);
        try {
			props.load(in);
			LOG.info("Properties: " + props);
		} catch (Exception e) {
			LOG.info("Unable to load properties file: " + PROP_FILE_NAME);
			throw new RuntimeException(e);
		} finally {
			if(in != null) {
				try {
					in.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
        parseProperties();
	}
	
	@Override
	public void parseProperties() {
		this.zkHost = get("solr.zkHost");
		this.connectTimeout = getInt("solr.connectTimeout", 10000);
		this.clientTimeout = getInt("solr.clientTimeout", 30000);
		this.batchCount = getInt("solr.batchCount", 1000);
		this.jdbcUrl = get("solr.impala.jdbcUrl");
		this.timeTypeField = get("solr.time.type.field", "time_type");
		this.timeIdField = get("solr.time.id.field", "time_id");
		this.indexerAssemblerClassname = get("solr.indexer.assembler.class", 
				"org.shirdrn.solr.cloud.index.standalone.HiveBasedIndexer.Assembler");
	}
	
	@Override
	public int getInt(String key, int defaultValue) {
		String strValue = props.getProperty(key);
		Integer value = null;
		try {
			value = Integer.parseInt(strValue);
		} catch (Exception e) {
			LOG.error(e);
			value = defaultValue;
		}
		return value;
	}
	
	@Override
	public String get(String key) {
		return props.getProperty(key);
	}
	
	@Override
	public String get(String key, String defaultValue) {
		String value = props.getProperty(key);
		if(value == null) {
			value = defaultValue;
		}
		return value;
	}
	
	protected StatInfo getStat(String collection) {
		return statistics.get(collection);
	}
	
	protected MappedConf getMappedConfByCollection(String collection) {
		return mappedByCollection.get(collection);
	}
	
	protected MappedConf getMappedConfByTable(String table) {
		return mappedByTable.get(table);
	}
	
	@SuppressWarnings("unchecked")
	protected ArgsAssembler<? extends AbstractIndexer> createAssembler() {
		ArgsAssembler<? extends AbstractIndexer> assembler = 
				(ArgsAssembler<? extends AbstractIndexer>) ReflectionUtils.getInstance(indexerAssemblerClassname);
		return assembler;
	}

	public static void startIndexer(Class<? extends IndexingManager> managerClass, String[] args) {
		IndexingManager indexingManager = ReflectionUtils.getInstance(managerClass);
		try {
			indexingManager.buildIndexes(args);
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}
	
	public static void startIndexer(String managerClass, String[] args) {
		IndexingManager indexingManager = (IndexingManager) ReflectionUtils.getInstance(managerClass);
		try {
			indexingManager.buildIndexes(args);
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public void dumpStatFor(String collection) {
		StringBuffer allStat = new StringBuffer();
		allStat.append("\n");
		appendHeader(allStat);
		appendDlimiter(allStat);
		StatInfo stat = statistics.get(collection);
		formatStat(allStat, collection, stat);
		appendDlimiter(allStat);
		LOG.info(allStat.toString());
	}
	
	@Override
	public void dumpStatAll() {
		StringBuffer allStat = new StringBuffer();
		allStat.append("\n");
		appendHeader(allStat);
		appendDlimiter(allStat);
		for(Entry<String, StatInfo> entry : statistics.entrySet()) {
			formatStat(allStat, entry.getKey(), entry.getValue());
		}
		appendDlimiter(allStat);
		LOG.info(allStat.toString());
	}
	
	private void appendHeader(StringBuffer buffer) {
		buffer.append("      Indexer Statistics Report\n");
	}
	
	private void appendDlimiter(StringBuffer allStat) {
		allStat.append("==================================================\n");
	}

	private final String dateFormat = "yyyy-MM-dd HH:mm:ss";
	
	private void formatStat(StringBuffer allStat, String collection, StatInfo stat) {
		StringBuffer s = new StringBuffer();
		s.append(">>COLLECTION : ").append(collection).append("\n")
		 .append(">>TABLE      : ").append(stat.mappedConf.tableName).append("\n");
		String status = null;
		String startTime = null;
		String finishTime = null;
		long timeTaken = 0L;
		int totalCount = 0;
		int indexedCount = 0;
		int failureCount = 0;
		String cause = null;
		if(stat.indexer != null && stat.assembler != null) {
			if(stat.indexer.getIndexingStatus() == stat.assembler.getStatus() 
					&& stat.indexer.getIndexingStatus() == Status.SUCCESS) {
				status = Status.SUCCESS.toString();
			} else {
				status = Status.FAILURE.toString();
			}
			startTime = TimeUtils.format(stat.indexer.getStartTime(), dateFormat);
			finishTime = TimeUtils.format(stat.indexer.getFinishTime(), dateFormat);
			timeTaken = stat.indexer.timeTaken();
			totalCount = stat.indexer.getTotalCount();
			indexedCount = stat.indexer.getIndexedCount();
			failureCount = stat.indexer.getFailuerCount();
		} else {
			status = Status.FAILURE.toString();
			if(stat.indexer == null && stat.assembler == null) {
				cause = "indexer==null && assembler==null";
			} else {
				if(stat.indexer != null) {
					if(stat.indexer.getCause() != null) {
						cause = stat.indexer.getCause().getMessage();
					}
				}
				if(cause == null) {
					cause = "indexer==null || assembler==null";
				}
			}
		}
		if(stat.assembler != null) {
			if(!stat.causes.isEmpty()) {
				if(stat.causes.size() == 1) {
					cause = stat.causes.get(0).getMessage();
				} else {
					cause = "";
					for (int i = 0; i < stat.causes.size()-1; i++) {
						cause += stat.causes.get(i).getMessage() + " | "; 								
					}
					cause += stat.causes.get(stat.causes.size()-1);
				}
			}
		}
		s.append("  ").append("IndexingStatus      : ").append(status).append("\n");
		s.append("  ").append("PreviousIndexTime   : ").append(stat.previousIndexTime).append("\n");
		s.append("  ").append("LastIndexTime       : ").append(stat.lastIndexTime).append("\n");
		s.append("  ").append("QueryCondition      : ").append(stat.queryCondition).append("\n");
		s.append("  ").append("StartTime           : ").append(startTime).append("\n");
		s.append("  ").append("FinishTime          : ").append(finishTime).append("\n");
		s.append("  ").append("TimeTaken           : ").append(timeTaken).append(" (ms)\n");
		s.append("  ").append("totalCount          : ").append(totalCount).append("\n");
		s.append("  ").append("indexedCount        : ").append(indexedCount).append("\n");
		s.append("  ").append("failureCount        : ").append(failureCount).append("\n");
		if(!stat.equals("SUCCESS")) {
			s.append("  ").append("Cause               : ").append(cause).append("\n");
		}
		
		allStat.append(s.toString());
	}

	protected final class MappedConf {
		String collection;
		String tableName;
		String schemaMappingFile;
		String timestampFieldName;
		String startTime;
		String finishTime;
		public String getCollection() {
			return collection;
		}
		public String getTableName() {
			return tableName;
		}
		public String getSchemaMappingFile() {
			return schemaMappingFile;
		}
		public String getTimestampFieldName() {
			return timestampFieldName;
		}
		public String getStartTime() {
			return startTime;
		}
		public String getFinishTime() {
			return finishTime;
		}
	}
	
	public final class StatInfo {
		ArgsAssembler<? extends AbstractIndexer> assembler;
		AbstractIndexer indexer;
		String previousIndexTime;
		String lastIndexTime;
		MappedConf mappedConf;
		List<Throwable> causes = new ArrayList<>();
		String queryCondition;
		
		public void clear() {
			assembler = null;
			indexer = null;
			previousIndexTime = "0000-00-00 00:00:00";
			lastIndexTime = "0000-00-00 00:00:00";
			mappedConf = null;
			causes.clear();
			queryCondition = null;
		}
		public ArgsAssembler<? extends AbstractIndexer> getAssembler() {
			return assembler;
		}
		public AbstractIndexer getIndexer() {
			return indexer;
		}
		public String getPreviousIndexTime() {
			return previousIndexTime;
		}
		public String getLastIndexTime() {
			return lastIndexTime;
		}
		public MappedConf getMappedConf() {
			return mappedConf;
		}
		public List<Throwable> getCauses() {
			return causes;
		}
		public void setAssembler(ArgsAssembler<? extends AbstractIndexer> assembler) {
			this.assembler = assembler;
		}
		public void setIndexer(AbstractIndexer indexer) {
			this.indexer = indexer;
		}
		public void setPreviousIndexTime(String previousIndexTime) {
			this.previousIndexTime = previousIndexTime;
		}
		public void setLastIndexTime(String lastIndexTime) {
			this.lastIndexTime = lastIndexTime;
		}
		public void setMappedConf(MappedConf mappedConf) {
			this.mappedConf = mappedConf;
		}
		public String getQueryCondition() {
			return queryCondition;
		}
		public void setQueryCondition(String queryCondition) {
			this.queryCondition = queryCondition;
		}
	}

}
