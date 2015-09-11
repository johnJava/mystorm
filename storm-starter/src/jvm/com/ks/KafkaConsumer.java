package com.ks;
import java.io.UnsupportedEncodingException;
import java.util.List;
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
       
       
      
    public class KafkaConsumer {  
       
      public static void main(String[] args) throws InterruptedException, UnsupportedEncodingException {  
       
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
       
        //topic�Ĺ�����  
        Whitelist whitelist = new Whitelist("t5");  
        List<KafkaStream<byte[], byte[]>> partitions = javaConsumerConnector.createMessageStreamsByFilter(whitelist);  
       
        if (CollectionUtils.isEmpty(partitions)) {  
          System.out.println("empty!");  
          TimeUnit.SECONDS.sleep(1);  
        }  
       
        //������Ϣ  
        for (KafkaStream<byte[], byte[]> partition : partitions) {  
       
          ConsumerIterator<byte[], byte[]> iterator = partition.iterator();  
          while (iterator.hasNext()) {  
            MessageAndMetadata<byte[], byte[]> next = iterator.next();  
            //System.out.println("partiton:" + next.partition());  
            //System.out.println("offset:" + next.offset());  
            System.out.println("message:" + new String(next.message(), "utf-8"));  
          }  
        }   
      }  
    }  