package org.shirdrn.solr.indexing;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.solr.indexing.common.ArgsAssembler;
import org.shirdrn.solr.indexing.executors.NamedThreadFactory;
import org.shirdrn.solr.indexing.executors.ScheduleAgainPolicy;
import org.shirdrn.solr.indexing.indexer.AbstractIndexer;
import org.shirdrn.solr.indexing.indexer.AbstractIndexer.Status;
import org.shirdrn.solr.indexing.indexer.component.DBIndexer;
import org.shirdrn.solr.indexing.utils.FileUtils;
import org.shirdrn.solr.indexing.utils.ReflectionUtils;
import org.shirdrn.solr.indexing.utils.TimeUtils;

public class MultipleTablesIndexingTool {

	private static final Log LOG = LogFactory.getLog(MultipleTablesIndexingTool.class);
	private static final String PROP_FILE_NAME = "solr.properties";
	private static final String MAPPINGS_FILE_NAME = "mappings.conf";
	private final Properties props = new Properties(); 
	private String zkHost;
	private int connectTimeout;
	private int clientTimeout;
	private String jdbcUrl;
	private int batchCount;
	private int threadPoolSize;
	private File metadataDir;
	private int beforeHours;
	private String timestampFormat;
	private ExecutorService pool;
	private final int totalCount;
	private final AtomicInteger counter = new AtomicInteger(0);
	
	// <collection, mappedObj>
	private final LinkedHashMap<String, MappedObj> mapped = new LinkedHashMap<>(0);
	private final ConcurrentHashMap<String, IndexerStat> indexerStats = new ConcurrentHashMap<>();
	
	public MultipleTablesIndexingTool(){
        readProperties();
        readMappings();
        totalCount = mapped.size();
        LOG.info("Total count: totalCount=" + totalCount);
        if(totalCount != 0) {
        	int nThreads = Math.min(threadPoolSize, totalCount);
        	if(nThreads > 0) {
        		threadPoolSize = nThreads;
        	}
        	LOG.info("Thread pool size: threadPoolSize=" + threadPoolSize);
        	BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(threadPoolSize);
        	pool = new ThreadPoolExecutor(
        			1, threadPoolSize, 120, TimeUnit.SECONDS, workQueue, 
        			new NamedThreadFactory("POOL"), new ScheduleAgainPolicy(threadPoolSize));
        } else {
        	System.exit(0);
        }
	}

	private void readMappings() {
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

	private void parseLine(String line) {
		String[] a = line.split("\\s*,\\s*");
		if(a.length == 4) {
			MappedObj mo = new MappedObj();
			mo.collection = a[0];
			mo.tableName = a[1];
			mo.schemaMappingFile = a[2];
			mo.timestampFieldName = a[3];
			mapped.put(mo.collection, mo);
		}
	}

	private void readProperties() {
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
        this.zkHost = get("solr.zkHost");
        this.connectTimeout = getInt("solr.connectTimeout", 10000);
        this.clientTimeout = getInt("solr.clientTimeout", 30000);
        this.batchCount = getInt("solr.bacchCount", 1000);
        this.jdbcUrl = get("solr.impala.jdbcUrl");
        this.threadPoolSize = getInt("solr.thread.pool.size", 1);
        String md = get("solr.index.metadata.dir", "index.metadata");
        this.metadataDir = new File(md);
        if(!metadataDir.exists()) {
        	metadataDir.mkdirs();
        }
        this.beforeHours = getInt("solr.index.init.hour.before", 1);
        this.timestampFormat = get("solr.index.timestamp.format", "yyyy-MM-dd HH:mm:ss");
	}
	
	private int getInt(String key, int defaultValue) {
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

	private String get(String key) {
		return props.getProperty(key);
	}
	
	private String get(String key, String defaultValue) {
		String value = props.getProperty(key);
		if(value == null) {
			value = defaultValue;
		}
		return value;
	}

	private final Object lock = new Object();
	
	public void indexDocs() {
		Iterator<Entry<String, MappedObj>> iter = mapped.entrySet().iterator();
		while(iter.hasNext()) {
			final Entry<String, MappedObj> entry = iter.next();
			LOG.info("Indexing for: collection=" + entry.getKey() + ",table=" + entry.getValue().tableName);
			Worker worker = new Worker(entry);
			pool.execute(worker);
		}
		
		// wait thread to finish task
		synchronized(lock) {
			try {
				lock.wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		// dump statistics information
		dumpStat();
		// shut down thread pool service
		pool.shutdown();
	}
	
	private static final Log WLOG = LogFactory.getLog(Worker.class);
	
	class Worker implements Runnable {
		private Entry<String, MappedObj> entry;
		private String previousIndexTime;
		private String lastIndexTime;
		private File thisFile;
		
		public Worker(Entry<String, MappedObj> entry) {
			super();
			this.entry = entry;
		}
		
		@Override
		public void run() {
			WLOG.info("Enter worker thread body.");
			counter.incrementAndGet();
			ArgsAssembler<? extends AbstractIndexer> assembler = null;
			AbstractIndexer indexer = null;
			Throwable cause = null;
			try {
				String conditions = checkConditions(entry.getValue().tableName);
				assembler = ReflectionUtils.getInstance(DBIndexer.Assembler.class);
				try {
					indexer = assembler.assemble(new String[] {
							zkHost, String.valueOf(connectTimeout), String.valueOf(clientTimeout), String.valueOf(batchCount), 
							entry.getKey(), entry.getValue().schemaMappingFile,
							jdbcUrl, entry.getValue().tableName, conditions
					});
				} catch (Exception e) {
					throw e;
				}
				// execute to index
				indexer.indexDocs();
				indexer.close();
				// write last index time to file
				FileUtils.writeToFile(thisFile, lastIndexTime);
			} catch (Throwable t) {
				cause = t;
				WLOG.error(t);
			} finally {
				if(counter.get() == totalCount) {
					synchronized(lock) {
						lock.notify();
					}
				}
				
				// collect statistics information
				IndexerStat stat = new IndexerStat();
				stat.previousIndexTime = previousIndexTime;
				stat.lastIndexTime = lastIndexTime;
				indexerStats.putIfAbsent(entry.getKey(), stat);
				stat.indexer = indexer;
				stat.detail = entry.getValue();
				if(cause != null) {
					stat.cause = cause;
				}
				WLOG.info("Leave worker thread body.");
			}
		}

		private String checkConditions(String tableName) {
			thisFile = new File(metadataDir, tableName);
			Date date = TimeUtils.getDateBefore(Calendar.HOUR_OF_DAY, beforeHours);
			String previousTime = TimeUtils.format(date, timestampFormat);
			lastIndexTime = TimeUtils.format(new Date(), timestampFormat);
			StringBuffer condition = new StringBuffer();
			if(!thisFile.exists()) {
				try {
					thisFile.createNewFile();
				} catch (IOException e) {
					WLOG.error("Error to create file: " + thisFile.getAbsolutePath());
					throw new RuntimeException(e);
				}
				
			} else {
				List<String> lines = FileUtils.populateListWithLines(thisFile, "UTF-8");
				previousTime = lines.get(0);
			}
			previousIndexTime = previousTime;
			condition.append(entry.getValue().timestampFieldName).append(">='").append(previousTime).append("'")
			.append(" AND ").append(entry.getValue().timestampFieldName).append("<='").append(lastIndexTime).append("'");
			return condition.toString();
		}
		
	
	}
	
	private void dumpStat() {
		StringBuffer allStat = new StringBuffer();
		allStat.append("\n")
		.append("      Indexer Statistics Report\n")
		.append("====================================\n");
		for(Entry<String, IndexerStat> entry : indexerStats.entrySet()) {
			formatStat(allStat, entry.getKey(), entry.getValue());
		}
		allStat
		.append("====================================\n");
		LOG.info(allStat.toString());
	}

	private final String dateFormat = "yyyy-MM-dd HH:mm:ss";
	
	private void formatStat(StringBuffer allStat, String collection, IndexerStat stat) {
		StringBuffer s = new StringBuffer();
		s.append(">>COLLECTION:").append(collection).append("\n")
		 .append(">>TABLE     :").append(stat.detail.tableName).append("\n");
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
				if(stat.assembler != null) {
					if(stat.cause != null) {
						cause = stat.cause.getMessage();
					}
				}
				if(cause == null) {
					if(stat.cause != null) {
						cause = stat.cause.getMessage();
					} else {
						cause = "indexer==null || assembler==null";
					}
				}
			}
		}
		s.append("  ").append("IndexingStatus      : ").append(status).append("\n");
		s.append("  ").append("PreviousIndexTime   : ").append(stat.previousIndexTime).append("\n");
		s.append("  ").append("LastIndexTime       : ").append(stat.lastIndexTime).append("\n");
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

	final class MappedObj {
		String collection;
		String tableName;
		String schemaMappingFile;
		String timestampFieldName;
	}
	
	final class IndexerStat {
		ArgsAssembler<? extends AbstractIndexer> assembler;
		AbstractIndexer indexer;
		String previousIndexTime;
		String lastIndexTime;
		MappedObj detail;
		Throwable cause;
	}
	
	public static void main(String[] args) {
		MultipleTablesIndexingTool manager = new MultipleTablesIndexingTool();
		manager.indexDocs();
	}

}
