package com.ks;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class SendMessage {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*Properties props = new Properties();
		props.put("zookeeper.connect", "CH5:2181,CH6:2181,CH7:2181");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("producer.type", "async");
		props.put("compression.codec", "1");
		props.put("metadata.broker.list", "CH6:9092");
  		props.put("group.id", "test");  
		
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		*/
		Random r = new Random();
		int end=10000;
		int i=0;
		while(++i<=end){
			int id = r.nextInt(10000000);
			int memberid = r.nextInt(100000);
			int totalprice = r.nextInt(1000)+100;
			int youhui = r.nextInt(100);
			int sendpay = r.nextInt(3);
			StringBuffer data = new StringBuffer();
			data.append(String.valueOf(id))
			.append("\t")
			.append(String.valueOf(memberid))
			.append("\t")
			.append(String.valueOf(totalprice))
			.append("\t")
			.append(String.valueOf(youhui))
			.append("\t")
			.append(String.valueOf(sendpay))
			.append("\t")
			.append(System.currentTimeMillis());
			System.out.println(data.toString());
//			producer.send(new KeyedMessage<String, String>("t2",data.toString()));
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		//producer.close();
		//System.out.println("send over ------------------");
	}

}
