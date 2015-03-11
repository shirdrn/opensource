package org.shirdrn.solr.indexing.indexer.component;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServerException;
import org.shirdrn.solr.indexing.common.AbstractArgsAssembler;
import org.shirdrn.solr.indexing.common.DocCreator;
import org.shirdrn.solr.indexing.common.IndexingService;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.ZkConf;
import org.shirdrn.solr.indexing.indexer.AbstractIndexer;
import org.shirdrn.solr.indexing.indexer.LineDocCreator;
import org.shirdrn.solr.indexing.indexer.MultiThreadedIndexer;
import org.shirdrn.solr.indexing.indexer.client.MultiThreadedClient;

public class MultiThreadedFilesIndexer extends MultiThreadedIndexer {

	private static final Log LOG = LogFactory.getLog(MultiThreadedFilesIndexer.class);
	private int threadCount = 1;
	private final Object waitingLock = new Object();
	private int fileCount;
	private AtomicInteger counter = new AtomicInteger(0);
	private File filesDir;
	
	public MultiThreadedFilesIndexer(ClientConf clientConf, String dir, int nThreads) 
			throws MalformedURLException {
		super(clientConf, nThreads);
		this.filesDir = new File(dir);
		LOG.info("Files directory: dir=" + dir);
	}
	
	@Override
	protected void process() throws Exception {
		File[] files = filesDir.listFiles();
		for(File file : files) {
			LOG.info("file=" + file.getAbsolutePath());
		}
		if(files != null && files.length>0) {
			fileCount = filesDir.listFiles().length;
			DocCreator<String> docCreator = new LineDocCreator(builder);
			builder.setDocCreator(docCreator);
			threadCount = Math.min(threadCount, fileCount);
			LOG.info("File count: fileCount=" + fileCount);
			
			for(final File f : files) {
				File absoluteFile = new File(filesDir, f.getName());
				try {
					IndexingService client = MultiThreadedClient.newIndexingClient(clientConf, this);
					Worker worker = new Worker(absoluteFile.getAbsolutePath(), client);
					super.execute(worker);
				} catch (Exception e) {
					throw e;
				}
			}
			synchronized(waitingLock) {
				try {
					waitingLock.wait();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
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

		private String file;
		private IndexingService client;
		
		public Worker(String file, IndexingService client) {
			super();
			this.file = file;
			this.client = client;
		}

		@Override
		public void run() {
			counter.incrementAndGet();
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(file));
				String record = null;
				while((record=reader.readLine()) != null) {
					client.addDoc(record);
				}
			} catch (SolrServerException | IOException e) {
				e.printStackTrace();
			} finally {
				try {
					client.finallyCommit();
					MultiThreadedClient thisClient = ((MultiThreadedClient)client);
					thisClient.close();
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					if(counter.get() == fileCount) {
						synchronized(waitingLock) {
							waitingLock.notify();
						}
					}
				}
			}
			
		}

	}
	
	public static class Assembler extends AbstractArgsAssembler<AbstractIndexer> {
		private static final String TYPE_MULTITHREADED_FILES_INDEXER = "5";
		private static final String NAME_MULTITHREAD_FILES_INDEXER = "Multithread_Files_Indexer";
		

		public Assembler() {
			super();
			argCount = 8;
			type = TYPE_MULTITHREADED_FILES_INDEXER;
			name = NAME_MULTITHREAD_FILES_INDEXER;
		}
		
		@Override
		public AbstractIndexer assemble(String[] args) throws Exception {
			super.assemble(args);
			
			String zkHost;
			int connectTimeout = 10000;
			int clientTimeout = 30000;
			String collection;
			int batchCount = 1000;
			String input;
			String schemaMappingFile;
			int threadCount = 1;
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
			input = args[7];
			
			ZkConf zkConf = new ZkConf();
			zkConf.setZkHost(zkHost);
			zkConf.setZkConnectTimeout(connectTimeout);
			zkConf.setZkClientTimeout(clientTimeout);
			ClientConf clientConf = new ClientConf(zkConf);
			clientConf.setCollectionName(collection);
			clientConf.setSchemaMappingFile(schemaMappingFile);
			clientConf.setBatchCount(batchCount);
			
			try {
				indexer = new MultiThreadedFilesIndexer(clientConf, input, threadCount);
			} catch (MalformedURLException e) {
				throw new RuntimeException(e);
			}
			return indexer;
		}
		
		@Override
		public String getUsageArgList() {
			StringBuffer usage = new StringBuffer();
			usage.append("<0:zkHost> <1:connectTimeout> <2:clientTimeout> <3:batchCount> <4:nThreads> <5:collection> ")
			.append("<6:schemaMappingFile> <7:inputDir>");
			return usage.toString();
		}

		@Override
		public String[] showCLIExamples() {
			String[] examples = new String[] {
					"zk:2181 10000 30000 1000 15 mycollection schema-mapping.xml /home/solr/data",
					"zk:2181 10000 30000 1000 15 mycollection /home/solr/schemas/schema-mapping.xml /home/solr/data"
			};
			return examples;
		}
	}
	

}
