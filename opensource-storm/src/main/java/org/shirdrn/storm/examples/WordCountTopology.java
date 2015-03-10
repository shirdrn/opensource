package org.shirdrn.storm.examples;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.common.collect.Maps;

/**
 * Topology transformation graph:
 * <pre>
 * +------------------+      +----------------+      +---------------+
 * |ProduceRecordSpout| ---> |WordSplitterBolt| ---> |WordCounterBolt|
 * +------------------+      +----------------+      +---------------+
 * </pre>
 *  
 * @author yanjun
 */
@SuppressWarnings("rawtypes")
public class WordCountTopology {
	
	public static class ProduceRecordSpout extends BaseRichSpout {

		private static final long serialVersionUID = 1L;
		private static final Log LOG = LogFactory.getLog(ProduceRecordSpout.class);
		private SpoutOutputCollector collector;
		private Random random;
		private String[] records;
		
		public ProduceRecordSpout(String[] records) {
			this.records = records;
		}
		
		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;	
			random = new Random();
		}


		@Override
		public void nextTuple() {
			Utils.sleep(500);
			String record = records[random.nextInt(records.length)];
			List<Object> values = new Values(record);
			collector.emit(values, values);
			LOG.info("Record emitted: record=" + record);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("record"));		
		}

	}
	
	public static class WordSplitterBolt extends BaseRichBolt {

		private static final long serialVersionUID = 1L;
		private static final Log LOG = LogFactory.getLog(WordSplitterBolt.class);
		private OutputCollector collector;
		
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;			
		}

		@Override
		public void execute(Tuple input) {
			String record = input.getString(0);
			if(record != null && !record.trim().isEmpty()) {
				for(String word : record.split("\\s+")) {
					collector.emit(input, new Values(word, 1));
					LOG.info("Emitted: word=" + word);
				}
			}
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));		
		}
		
	}
	
	public static class WordCounterBolt extends BaseRichBolt {

		private static final long serialVersionUID = 1L;
		private static final Log LOG = LogFactory.getLog(WordCounterBolt.class);
		private OutputCollector collector;
		private final Map<String, AtomicInteger> counterMap = Maps.newHashMap();
		
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;			
		}

		@Override
		public void execute(Tuple input) {
			String word = input.getString(0);
			int count = input.getIntegerByField("count");
			AtomicInteger ai = counterMap.get(word);
			if(ai == null) {
				ai = new AtomicInteger(0);
				counterMap.put(word, ai);
			}
			ai.addAndGet(count);
			LOG.info("DEBUG: word=" + word + ", count=" + ai.get());
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}
		
		@Override
		public void cleanup() {
			// print count results
			LOG.info("Word count results:");
			for(Entry<String, AtomicInteger> entry : counterMap.entrySet()) {
				LOG.info("\tword=" + entry.getKey() + ", count=" + entry.getValue().get());
			}
		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
		// configure & build topology
		TopologyBuilder builder = new TopologyBuilder();
		String[] records = new String[] {
				"A Storm cluster is superficially similar to a Hadoop cluster",
				"All coordination between Nimbus and the Supervisors is done through a Zookeeper cluster",
				"The core abstraction in Storm is the stream"
		};
		builder
			.setSpout("spout-producer", new ProduceRecordSpout(records), 1)
			.setNumTasks(3);
		builder
			.setBolt("bolt-splitter", new WordSplitterBolt(), 2)
			.shuffleGrouping("spout-producer")
			.setNumTasks(2);
		builder.setBolt("bolt-counter", new WordCounterBolt(), 1)
			.fieldsGrouping("bolt-splitter", new Fields("word"))
			.setNumTasks(2);
		
		// submit topology
		Config conf = new Config();
		String name = WordCountTopology.class.getSimpleName();
		if (args != null && args.length > 0) {
			String nimbus = args[0];
			conf.put(Config.NIMBUS_HOST, nimbus);
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			Thread.sleep(60000);
			cluster.shutdown();
		}
	}

}
