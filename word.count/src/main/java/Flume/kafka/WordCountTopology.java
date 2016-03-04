package Flume.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class WordCountTopology {
	private static final String SENTENCE_SPOUT_ID="sentence-spout";
	private static final String SPLIT_BOLT_ID="split-bolt";
	private static final String COUNT_BOLT_ID ="count-bolt";
	private static final String REPORT_BOLT_ID="report-bolt";
	private static final String TOPOLOGY_NAME="word-count-topology";
	public static void main(String[] args) {
		SentenceSpout spout = new SentenceSpout();
		SplitSentenceBolt spliteBolt = new SplitSentenceBolt();
		WordCountBolt countBolt = new WordCountBolt();
		ReportBolt reportBolt = new ReportBolt();
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout(SENTENCE_SPOUT_ID, spout);
		builder.setBolt(SPLIT_BOLT_ID, spliteBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
		builder.setBolt(COUNT_BOLT_ID, countBolt).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
//		builder.setBolt(SPLIT_BOLT_ID, spliteBolt,4).setNumTasks(4).shuffleGrouping(SENTENCE_SPOUT_ID);
//		builder.setBolt(COUNT_BOLT_ID, countBolt,8).fieldsGrouping(SPLIT_BOLT_ID, new Fields("word"));
		builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(COUNT_BOLT_ID);
		
		Config config = new Config();
		config.setNumWorkers(2);
		LocalCluster localCluster = new LocalCluster();
		config.setMaxTaskParallelism(111);
		localCluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
		Utils.sleep(10000);
		localCluster.killTopology(TOPOLOGY_NAME);
	    localCluster.shutdown();			
		
		

	}

}
