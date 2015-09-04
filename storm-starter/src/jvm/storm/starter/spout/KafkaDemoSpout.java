package storm.starter.spout;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.commons.collections.CollectionUtils;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class KafkaDemoSpout extends BaseRichSpout{
	SpoutOutputCollector _collector;
	static List<KafkaStream<byte[], byte[]>> partitions =null;
	private void initParti(){
		if(partitions==null) {
			System.out.println("connect zookeeper..." );  
			Properties props = new Properties();
			props.put("zookeeper.connect", "CH5:2181,CH6:2181,CH7:2181");
			props.put("serializer.class", "kafka.serializer.StringEncoder");
			props.put("producer.type", "async");
			props.put("compression.codec", "1");
			props.put("metadata.broker.list", "CH6:9092");
			props.put("auto.commit.enable", "true");  
			props.put("auto.commit.interval.ms", "60000");  
			props.put("group.id", "test");  
			ConsumerConfig consumerConfig = new ConsumerConfig(props);  
			ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);  
			//topic的过滤器  
			Whitelist whitelist = new Whitelist("t2");  
			partitions = javaConsumerConnector.createMessageStreamsByFilter(whitelist);  
			System.out.println("connect zookeeper successed" );  
		}
	}
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		initParti();
	    _collector = collector;
	}

	 @Override
	  public void nextTuple() {
		  initParti();
		  if(partitions.size()>0){
			  for (KafkaStream<byte[], byte[]> partition : partitions) {  
				  
				  ConsumerIterator<byte[], byte[]> iterator = partition.iterator();  
				  while (iterator.hasNext()) {  
					  MessageAndMetadata<byte[], byte[]> next = iterator.next();  
					  String msg;
					  try {
						  msg = new String(next.message(), "utf-8");
						  System.out.println("message:" + msg);  
						  _collector.emit(new Values(msg));
					  } catch (UnsupportedEncodingException e) {
						  msg="error:"+e.getMessage();
						  _collector.emit(new Values(msg));
						  e.printStackTrace();
					  }
				  }  
			  }   
		  }
	  }


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("kword"));
	}

}
