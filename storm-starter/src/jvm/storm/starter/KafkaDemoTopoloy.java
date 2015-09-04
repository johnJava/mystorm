package storm.starter;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.collect.ImmutableList;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.starter.spout.KafkaDemoSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class KafkaDemoTopoloy {
	public static class KafkaWordCount extends BaseBasicBolt {
		static Producer<String, String> producer =null;
		private void initParti(){
			if(producer==null) {
				System.out.println("connect producer..." );  
				Properties props = new Properties();
				props.put("zookeeper.connect", "CH5:2181,CH6:2181,CH7:2181");
				props.put("serializer.class", "kafka.serializer.StringEncoder");
				props.put("producer.type", "async");
				props.put("compression.codec", "1");
				props.put("metadata.broker.list", "CH6:9092");
		  		props.put("group.id", "test");  
				ProducerConfig config = new ProducerConfig(props);
				producer = new Producer<String, String>(config);
				System.out.println("connect producer successed" );  
			}
		}
		
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			initParti();
			super.prepare(stormConf, context);
		}

		private static final long serialVersionUID = 1L;
	    @Override
	    public void execute(Tuple tuple, BasicOutputCollector collector) {
	      System.out.println("msg:"+tuple);
	      initParti();
	      producer.send(new KeyedMessage<String, String>("t2","from storm :"+tuple.toString()));
	    }
	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("kword"));
	    }

		@Override
		public void cleanup() {
			producer.close();
			producer=null;
			super.cleanup();
		}
	    
	  }
	public static void main(String[] args) throws Exception {
		System.out.println("begin.....");
		String kafkaZookeeper = "CH5:2181,CH6:2181,CH7:2181";
		BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper);
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "t2", "/order", "id");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.zkServers =  ImmutableList.of("CH6");
        kafkaConfig.zkPort = 2181;
        //kafkaConfig.forceFromStart = true;
	    TopologyBuilder builder = new TopologyBuilder();
	    builder.setSpout("kafkaspout", new KafkaSpout(kafkaConfig), 2);
	    builder.setBolt("kafkacount", new KafkaWordCount(), 1).shuffleGrouping("kafkaspout");
	    Config conf = new Config();
	    conf.setDebug(true);
	    if (args != null && args.length > 0) {
	      conf.setNumWorkers(3);
	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	    }
	    else {
	      conf.setMaxTaskParallelism(3);
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("kafka-count", conf, builder.createTopology());
	      Thread.sleep(10000);
	      cluster.shutdown();
	    }
	  }
}