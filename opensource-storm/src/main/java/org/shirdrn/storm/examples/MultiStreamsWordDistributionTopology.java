package org.shirdrn.storm.examples;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
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

/**
 * Transformation graph of topology {@link MultiStreamsWordDistributionTopology} is depict as follows:
 * <pre>
 * +-----------------------+                                                                +-----------------+
 * |ProduceRecordSpout(NUM)| -                                                           -> |SaveDataBolt(NUM)|
 * +-----------------------+  -                                                         -   +-----------------+
 *                             -                                                       -
 * +-----------------------+    -> +---------------+       +------------------------+ -     +-----------------+
 * |ProduceRecordSpout(STR)| ----> |SplitRecordBolt| ----> |DistributeWordByTypeBolt| ----> |SaveDataBolt(STR)|
 * +-----------------------+    -> +---------------+       +------------------------+ -     +-----------------+
 *                             -                                                       -
 * +-----------------------+  -                                                         -   +-----------------+
 * |ProduceRecordSpout(SIG)| -                                                           -> |SaveDataBolt(SIG)|
 * +-----------------------+                                                                +-----------------+
 * </pre>
 * Here, 3 spouts are producing records separately, and then they are subscribed by bolt {@link SplitRecordBolt}, 
 * which split the records to a group of words. Bolt {@linkplain DistributeWordByTypeBolt} subscribes the split 
 * bolt, and distributes the words grouped by type to feed the last 3 bolt {@link SaveDataBolt} respectively. 
 *  
 * @author yanjun
 */
@SuppressWarnings("rawtypes")
public class MultiStreamsWordDistributionTopology {
	
	interface Type {
		String NUMBER = "NUMBER";
		String STRING = "STRING";
		String SIGN = "SIGN";
	}
	
	public static class ProduceRecordSpout extends BaseRichSpout {

		private static final long serialVersionUID = 1L;
		private static final Log LOG = LogFactory.getLog(ProduceRecordSpout.class);
		private SpoutOutputCollector collector;
		private Random rand;
		private String[] recordLines;
		private String type;
		
		public ProduceRecordSpout(String type, String[] lines) {
			this.type = type;
			recordLines = lines;
		}
		
		@Override
		public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
			this.collector = collector;	
			rand = new Random();
		}


		@Override
		public void nextTuple() {
			Utils.sleep(500);
			String record = recordLines[rand.nextInt(recordLines.length)];
			List<Object> values = new Values(type, record);
			collector.emit(values, values);
			LOG.info("Record emitted: type=" + type + ", record=" + record);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("type", "record"));		
		}

	}
	
	public static class SplitRecordBolt extends BaseRichBolt {

		private static final long serialVersionUID = 1L;
		private static final Log LOG = LogFactory.getLog(SplitRecordBolt.class);
		private OutputCollector collector;
		
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;	
		}

		@Override
		public void execute(Tuple input) {
			String type = input.getString(0);
			String line = input.getString(1);
			if(line != null && !line.trim().isEmpty()) {
				for(String word  : line.split("\\s+")) {
					collector.emit(input, new Values(type, word));
					LOG.info("Word emitted: type=" + type + ", word=" + word);
				}
			}
			// ack tuple
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("type", "word"));
		}
	}
	
	public static class DistributeWordByTypeBolt extends BaseRichBolt {

		private static final long serialVersionUID = 1L;
		private static final Log LOG = LogFactory.getLog(DistributeWordByTypeBolt.class);
		private OutputCollector collector;
		
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;	
			Map<GlobalStreamId, Grouping> sources = context.getThisSources();
			LOG.info("sources==> " + sources);
		}

		@Override
		public void execute(Tuple input) {
			String type = input.getString(0);
			String word = input.getString(1);
			switch(type) {
				case Type.NUMBER:
					emit("stream-number-saver", type, input, word);
					break;
				case Type.STRING:
					emit("stream-string-saver", type, input, word);
					break;
				case Type.SIGN:
					emit("stream-sign-saver", type, input, word);
					break;
				default:
					// if unknown type, record is discarded. 
					// as needed, you can define a bolt to subscribe the stream 'stream-discarder'.
					emit("stream-discarder", type, input, word);
			}
			// ack tuple
			collector.ack(input);
		}
		
		private void emit(String streamId, String type, Tuple input, String word) {
			collector.emit(streamId, input, new Values(type, word));
			LOG.info("Distribution, typed word emitted: type=" + type + ", word=" + word);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream("stream-number-saver", new Fields("type", "word"));
			declarer.declareStream("stream-string-saver", new Fields("type", "word"));
			declarer.declareStream("stream-sign-saver", new Fields("type", "word"));
			declarer.declareStream("stream-discarder", new Fields("type", "word"));
		}
	}
	
	public static class SaveDataBolt extends BaseRichBolt {

		private static final long serialVersionUID = 1L;
		private static final Log LOG = LogFactory.getLog(SaveDataBolt.class);
		private OutputCollector collector;
		
		private String type;
		
		public SaveDataBolt(String type) {
			this.type = type;
		}
		
		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			this.collector = collector;	
		}

		@Override
		public void execute(Tuple input) {
			LOG.info("[" + type + "] " + 
					"SourceComponent=" + input.getSourceComponent() + 
					", SourceStreamId=" + input.getSourceStreamId() + 
					", type=" + input.getString(0) + 
					", value=" + input.getString(1));
			collector.ack(input);
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// do nothing		
		}
		
	}
	

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {

		// configure & build topology
		TopologyBuilder builder = new TopologyBuilder();
		
		// configure 3 spouts
		builder.setSpout("spout-number", new ProduceRecordSpout(Type.NUMBER, new String[] {"111 222 333", "80966 31"}), 1);
		builder.setSpout("spout-string", new ProduceRecordSpout(Type.STRING, new String[] {"abc ddd fasko", "hello the word"}), 1);
		builder.setSpout("spout-sign", new ProduceRecordSpout(Type.SIGN, new String[] {"++ -*% *** @@", "{+-} ^#######"}), 1);
		
		// configure splitter bolt
		builder.setBolt("bolt-splitter", new SplitRecordBolt(), 2)
			.shuffleGrouping("spout-number")
			.shuffleGrouping("spout-string")
			.shuffleGrouping("spout-sign");
		
		// configure distributor bolt
		builder.setBolt("bolt-distributor", new DistributeWordByTypeBolt(), 6)
			.fieldsGrouping("bolt-splitter", new Fields("type"));
		
		// configure 3 saver bolts
		builder.setBolt("bolt-number-saver", new SaveDataBolt(Type.NUMBER), 3)
			.shuffleGrouping("bolt-distributor", "stream-number-saver");
		builder.setBolt("bolt-string-saver", new SaveDataBolt(Type.STRING), 3)
			.shuffleGrouping("bolt-distributor", "stream-string-saver");
		builder.setBolt("bolt-sign-saver", new SaveDataBolt(Type.SIGN), 3)
			.shuffleGrouping("bolt-distributor", "stream-sign-saver");
		
		// submit topology
		Config conf = new Config();
		String name = MultiStreamsWordDistributionTopology.class.getSimpleName();
		if (args != null && args.length > 0) {
			String nimbus = args[0];
			conf.put(Config.NIMBUS_HOST, nimbus);
			conf.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(name, conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(name, conf, builder.createTopology());
			Thread.sleep(60 * 60 * 1000);
			cluster.shutdown();
		}
	}

}
