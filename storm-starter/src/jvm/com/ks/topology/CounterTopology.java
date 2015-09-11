package com.ks.topology;

import java.io.File;
import java.io.FileOutputStream;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.google.common.collect.ImmutableList;
import com.ks.bolt.CounterBolt;

public class CounterTopology {
	public static FileOutputStream out=null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try{
			System.out.println("begin.....");
			File file = new File("/opt/counter.log");
			if(!file.exists())file.createNewFile();
			out = new FileOutputStream("/opt/counter.log");
			KafkaSpout.LOG.debug("begin.....");
			String kafkaZookeeper = "CH6:2181";
			BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper);
			SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "t2", "/order", "id");
	        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	        kafkaConfig.zkServers =  ImmutableList.of("CH6");
	        kafkaConfig.zkPort = 2181;
	        //kafkaConfig.forceFromStart = true;
	        TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("spout", new KafkaSpout(kafkaConfig), 2);
	        builder.setBolt("counter", new CounterBolt(),1).shuffleGrouping("spout");
	        Config config = new Config();
	        config.setDebug(true);
	        config.setNumWorkers(2);
			System.out.println("submitTopology begin");
	        KafkaSpout.LOG.debug("submitTopology begin");
	        StormSubmitter.submitTopology("counter", config, builder.createTopology());
	        KafkaSpout.LOG.debug("submitTopology success");
	    	KafkaSpout.LOG.debug("sleep");
	        Thread.sleep(500000);
	        /*if(args!=null && args.length > 0) {
	        } else {        
	            config.setMaxTaskParallelism(3);
	            LocalCluster cluster = new LocalCluster();
	            cluster.submitTopology("special-topology", config, builder.createTopology());
	            cluster.shutdown();
	        }*/
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}
