package Flume.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class ReportBolt extends BaseRichBolt {
	private HashMap<String, Long> counts = null;
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counts = new HashMap<String, Long>();

	}

	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		Long count = input.getLongByField("count");
		this.counts.put(word, count);

	}
	
	

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}
	
	public void cleanup(){
		System.err.println("----------------------FINAL COUNTS----------------------");
		List<String> keys = new ArrayList<String>();
		keys.addAll(counts.keySet());
		for (String key : keys) {
			System.err.println(key+":"+counts.get(key));
			
		}
		System.err.println("-------------------------------------------------------");
		
	}

}
