package org.shirdrn.solr.indexing.indexer;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.executors.NamedThreadFactory;
import org.shirdrn.solr.indexing.executors.ScheduleAgainPolicy;

public abstract class MultiThreadedIndexer extends AbstractIndexer {

	private static final Log LOG = LogFactory.getLog(MultiThreadedIndexer.class);
	private final ExecutorService executorService;
	
	public MultiThreadedIndexer(ClientConf clientConf, int nThreads) {
		super(clientConf);
		int workQSize = 2 * nThreads;
		BlockingQueue<Runnable> q = new ArrayBlockingQueue<>(workQSize);
		executorService = new ThreadPoolExecutor(nThreads, nThreads,
				10000L, TimeUnit.MILLISECONDS, q, new NamedThreadFactory(), 
				new ScheduleAgainPolicy(workQSize));
	}
	
	protected void execute(Runnable r) {
		executorService.execute(r);
	}
	
	@Override
	public void close() throws IOException {
		LOG.info("Close thread executor...");
		executorService.shutdown();
		super.close();
	}

}
