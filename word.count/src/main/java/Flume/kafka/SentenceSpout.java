package Flume.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import backtype.storm.util__init;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * 
 * @ClassName: SentenceSpout 
 * @Description:word init
 * @author baifugui
 * @date Mar 2, 2016 8:31:51 PM 
 *
 */
public class SentenceSpout extends BaseRichSpout {
	private HashMap<UUID, Values> pending = null;
	private SpoutOutputCollector collector;
	private String[] sentens = {
			"my dog has fleas",
	        "i like cold beverages",
	        "the dog ate my homework",
	        "don't have a cow man",
	        "i don't think i like fleas"
			
	}; 
	private int index = 0;
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this .collector = collector;
		this.pending = new HashMap<UUID, Values>();

	}

	public void nextTuple() {
		
		Values values = new Values(sentens[index]);
		UUID msgId = UUID.randomUUID();
		pending.put(msgId, values);
		
		this.collector.emit(values);
		index++;
		if (index >= sentens.length) {
			index = 0;
		}
		Utils.sleep(1);
        
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));;

	}
	
	public void ack(Object msgId){
		pending.remove(msgId);
	}
	
	public void fail(Object msgId){
		collector.emit(pending.get(msgId),msgId);
	}

}
