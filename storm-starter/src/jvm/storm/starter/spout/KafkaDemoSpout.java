package storm.starter.spout;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import storm.kafka.SpoutConfig;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class KafkaDemoSpout extends BaseRichSpout{
	/**
	 * 
	 */
	private static final long serialVersionUID = -416071943650664495L;
	SpoutOutputCollector _collector;
	private SpoutConfig kafkaConfig;
	static AtomicInteger count = new AtomicInteger(0);
	static AtomicLong cur_offset = new AtomicLong(0);
	private static final Map<String, String> cs=new ConcurrentHashMap<String, String>();
	public KafkaDemoSpout(SpoutConfig kafkaConfig) {
		this.kafkaConfig=kafkaConfig;
	}
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
			Whitelist whitelist = new Whitelist("t5");  
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
	  public synchronized void nextTuple() {
		  initParti();
		  if(partitions.size()>0){
			  for (KafkaStream<byte[], byte[]> partition : partitions) {  
				  ConsumerIterator<byte[], byte[]> iterator = partition.iterator();  
				  while (iterator.hasNext()) {
					  count.incrementAndGet();
					  System.out.println("kafkademospout[count]:"+count);
					  MessageAndMetadata<byte[], byte[]> next = iterator.next();  
					  String msg;
					try {
						  long offset = next.offset();
						  System.out.println(cur_offset.longValue()+":"+offset);
						  if(cur_offset.longValue()>offset){
							  System.out.println("cur_offset continue");
							  continue;
						  }
						  cur_offset.set(offset);
						  msg = new String(next.message(), "utf-8");
						  String[] values = msg.split("\t");
						  if(values.length!=6){
							  System.out.println("nextTuple values:"+values);
							  continue;
						  }
//						  String cre = values[5];
//						  if(cs.containsKey(cre)){
//							  System.out.println("cs.containsKey("+cre+")");
//							  continue;
//						  }
//						  cs.put(cre, cre);
						  _collector.emit(new Values(values[0],values[1],values[2],values[3],values[4],values[5]));
						 /*System.out.println("message:" + msg); 
						  Iterable<List<Object>> tups = this.kafkaConfig.scheme.deserialize(next.message());
						  for(List<Object> tup:tups){
							  _collector.emit(tup);
						  }*/ 
					  } catch (UnsupportedEncodingException e) {
						  msg="error:"+e.getMessage();
						  _collector.emit(new Values("error","error","error","error","error","error"));
						  e.printStackTrace();
					  }
				  }  
			  }   
		  }
	  }


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//this.kafkaConfig.scheme.getOutputFields();
		declarer.declare(new Fields("id","memberid","totalprice","youhui","sendpay","createdate"));
	}

}
