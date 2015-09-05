package storm.starter.bolt;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;


public class HbaseBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    System.out.println("tuple:"+tuple.getString(0));
  }

  @Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("kword"));
	}

}
