package org.shirdrn.solr.indexing.indexer.mapred;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.solr.client.solrj.SolrServerException;
import org.shirdrn.solr.indexing.common.AbstractArgsAssembler;
import org.shirdrn.solr.indexing.common.AssembledRunner;
import org.shirdrn.solr.indexing.common.DocCreator;
import org.shirdrn.solr.indexing.common.IndexingService;
import org.shirdrn.solr.indexing.common.config.ClientConf;
import org.shirdrn.solr.indexing.common.config.ZkConf;
import org.shirdrn.solr.indexing.indexer.LineDocCreator;
import org.shirdrn.solr.indexing.indexer.client.SingleIndexingClient;
import org.shirdrn.solr.indexing.utils.TimeUtils;

public class SolrCloudIndexer extends AbstractArgsAssembler<SolrCloudIndexer> implements AssembledRunner {
	
	private static final Log LOG = LogFactory.getLog(SolrCloudIndexer.class);
	private static final String TYPE_MAPRED_INDEXER = "1";
	private static final String NAME_MAPRED_INDEXER = "Solr_Mapred_Indexer";
	public SolrCloudIndexer() {
		super();
		argCount = 1;
		type = TYPE_MAPRED_INDEXER;
		name = NAME_MAPRED_INDEXER;
	}
	
	public static class IndexingMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

		private static final Log MAPPER_LOG = LogFactory.getLog(IndexingMapper.class);
		private IndexingService client;
		private SingleIndexingClient thisClient;
		private String heading = null;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			ZkConf zkConf = new ZkConf();
			zkConf.setZkHost(context.getConfiguration().get("solr.cloud.zk.host", "zk:2188"));
			MAPPER_LOG.info("Zk conf: " + zkConf);
			
			ClientConf clientConf = new ClientConf(zkConf);
			clientConf.setCollectionName(context.getConfiguration().get("solr.cloud.collection", "mycollection"));
			clientConf.setBatchCount(context.getConfiguration().getInt("solr.cloud.index.commit.batch.count", 3000));
			MAPPER_LOG.info("Client conf: " + clientConf);
			
			client = SingleIndexingClient.newIndexingClient(clientConf);
			thisClient = (SingleIndexingClient) client;
			DocCreator<String> docCreator = new LineDocCreator(thisClient.getBuilder());
			thisClient.getBuilder().setDocCreator(docCreator);
			
			heading = context.getConfiguration().get("solr.cloud.file.heading");
			if(heading != null) {
				MAPPER_LOG.info("Heading is: " + heading);
			}
		}
		
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String docValue = value.toString();
			
			try {
				if(heading != null && docValue.equals(heading)) {
					return;
				}
				if(!docValue.trim().isEmpty()) {
					client.addDoc(docValue);
				}
			} catch (SolrServerException e) {
				e.printStackTrace();
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			try {
				client.finallyCommit();
			} catch (SolrServerException e) {
				e.printStackTrace();
			} finally {
				// close client and output statistics info
				thisClient.close();
			}
		}

	}
	
	@Override
	public void assembleAndRun(String... params) throws IOException {
		String[] args = params;
		Configuration conf = new Configuration();
		conf.addResource("solr-cloud.xml");
		String[] otherArgs = args;
		LOG.info("otherArgs count: " + otherArgs.length);
		for(int i=0; i<otherArgs.length; i++) {
			LOG.info(i + ". " + otherArgs[i]);
		}
		if (otherArgs.length < 1) {
			System.err.println("Usage: \n"
					+ "  " + SolrCloudIndexer.class.getName() + " <in>");
			System.exit(-1);
		}
		
		LOG.info("Create and configure hadoop job...");
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "indexer");
		job.setJarByClass(SolrCloudIndexer.class);
		job.setMapperClass(IndexingMapper.class);
   
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
   
		job.setNumReduceTasks(0);
   
		boolean exceptionCaught = false;
		Exception ex = null;
		Path out = new Path("temp-" + TimeUtils.format(new Date(), "yyyyMMddHHmmss"));
		FileSystem fs = null;
		try {
			Path in = new Path(args[0]);
			FileInputFormat.addInputPath(job, in);
			// create temporary output path
			fs = in.getFileSystem(conf);
			LOG.info("Using temporary output path: " + out.toString());
			FileOutputFormat.setOutputPath(job, out);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			ex = e;
			LOG.error(ex);
			exceptionCaught = true;
		} finally {
			if(fs != null && fs.exists(out)) {
				// delete temporary output path
				fs.deleteOnExit(out);
				LOG.info("Delete temporary output path: " + out.toString());
			}
			if(!exceptionCaught) {
			 	LOG.info("Job executed successfully!");
			} else {
				LOG.error("Err message: " + ex);
			}
		}
	}

	@Override
	public String getUsageArgList() {
		return "<0:inputDir>";
	}

	@Override
	public String[] showCLIExamples() {
		return new String[] {
				"/user/hadoop/solr/data"
		};
	}

}
