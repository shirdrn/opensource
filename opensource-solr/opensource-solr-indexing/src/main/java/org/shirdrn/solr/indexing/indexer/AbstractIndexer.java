package org.shirdrn.solr.indexing.indexer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.solr.indexing.common.DocCreator;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.FieldMappingBuilder;
import org.shirdrn.solr.indexing.utils.SolrUtils;

public abstract class AbstractIndexer implements Closeable {

	private static final Log LOG = LogFactory.getLog(AbstractIndexer.class);
	protected final FieldMappingBuilder builder;
	protected final ClientConf clientConf;
	private volatile int totalCount = 0;
	private volatile int indexedCount = 0;
	private volatile int deletedCount = 0;
	private volatile int failureCount = 0;
	private Date startTime;
	private Date finishTime;
	private boolean closed = false;
	protected Status indexingStatus = Status.UNKNOWN;
	private Throwable cause;
	
	public AbstractIndexer() {
		clientConf = null;
		builder = null;
	}
	
	public AbstractIndexer(ClientConf clientConf) {
		super();
		this.builder = SolrUtils.createFieldMappingBuilder(clientConf);
		DocCreator<String> docCreator = new LineDocCreator(builder);
		builder.setDocCreator(docCreator);
		this.clientConf = clientConf;
		LOG.info("Client conf: " + clientConf);
		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if(!closed) {
					logging();
				}
			}
		});
	}
	
	public enum Status {
		UNKNOWN,
		SUCCESS,
		FAILURE
	}
	
	public int addAndGetIndexedCount(int count) {
		indexedCount += count;
		return indexedCount;
	}
	
	public int addAndGetDeletedCount(int count) {
		deletedCount += count;
		return deletedCount;
	}
	
	public int addAndGetTotalCount(int count) {
		totalCount += count;
		return totalCount;
	}
	
	public void indexDocs() throws Throwable {
		startTime = new Date();
		try {
			process();
			indexingStatus = Status.SUCCESS;
		} catch (Throwable t) {
			cause = t;
			throw t;
		} finally {
			finishTime = new Date();
		}
	}
	
	protected abstract void process() throws Exception;

	@Override
	public void close() throws IOException {
		closed = true;
		try {
			Thread.sleep(5);
			logging();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void logging() {
//		LOG.info("Indexing statistics: \n"
//				+ "\ttotalCount=" + totalCount + "\n"
//				+ "\tindexedCount=" + indexedCount + "\n"
//				+ "\tdeletedCount=" + deletedCount + "\n"
//				+ "\tfailureCount=" + failureCount + "\n"
//				+ "\ttimeTaken=" + (finishTime.getTime()-startTime.getTime()) + "ms");
	}
	
	public long timeTaken() {
		return (finishTime.getTime()-startTime.getTime());
	}

	public int getIndexedCount() {
		return indexedCount;
	}

	public int getFailuerCount() {
		return totalCount - Math.max(indexedCount, deletedCount);
	}

	public int getTotalCount() {
		return totalCount;
	}
	
	public int getDeletedCount() {
		return deletedCount;
	}

	public ClientConf getClientConf() {
		return clientConf;
	}
	
	public FieldMappingBuilder getBuilder() {
		return builder;
	}

	public Status getIndexingStatus() {
		return indexingStatus;
	}

	public Throwable getCause() {
		return cause;
	}

	public void setCause(Throwable cause) {
		this.cause = cause;
	}

	public Date getStartTime() {
		return startTime;
	}

	public Date getFinishTime() {
		return finishTime;
	}
	
}
