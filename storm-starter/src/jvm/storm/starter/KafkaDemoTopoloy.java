package storm.starter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
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

import com.google.common.collect.ImmutableList;
public class KafkaDemoTopoloy {
	public static class KafkaWordCount extends BaseBasicBolt {
		
		static Producer<String, String> producer =null;
		static HBaseThread hbt=null;
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
			//initParti();
			try {
				hbt=new HBaseThread("order");
				hbt.initTable();
				//hbt.start();
			} catch (IOException e) {
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}

		private static final long serialVersionUID = 1L;
	    @Override
	    public void execute(Tuple tuple, BasicOutputCollector collector) {
	      System.out.println("msg:"+tuple);
	      String data = tuple.getString(0);
			if(data!=null && data.length()>0){
				String[] values = data.split("\t");
				if(values.length==6){
					try {
						String id = values[0];
						String memberid = values[1];
						String totalprice = values[2];
						String youhui = values[3];
						String sendpay = values[4];
						String createdate = values[5];
						System.out.println("data:"+data);
						long longMax = Long.MAX_VALUE;
						put("order", String.valueOf((longMax-Long.parseLong(createdate))), "info", "id", id);
						put("order", String.valueOf((longMax-Long.parseLong(createdate))), "info", "memberid", memberid);
						put("order", String.valueOf((longMax-Long.parseLong(createdate))), "info", "totalprice", totalprice);
						put("order", String.valueOf((longMax-Long.parseLong(createdate))), "info", "youhui", youhui);
						put("order", String.valueOf((longMax-Long.parseLong(createdate))), "info", "sendpay", sendpay);
						put("order", String.valueOf((longMax-Long.parseLong(createdate))), "info", "createdate", createdate);
						hbt.commit();
						//collector.emit(new Values(id,memberid,totalprice,youhui,sendpay));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}	
			}
	      /*initParti();
	      producer.send(new KeyedMessage<String, String>("t2","from storm :"+tuple.toString()));*/
	    }
	    public Put mkPut(String tablename, String row, String columnFamily,
				String column, String data) throws Exception {
	    	Put p1 = new Put(Bytes.toBytes(row));
			p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
					Bytes.toBytes(data));
			return p1;
	    }
	    public  void put(String tablename, String row, String columnFamily,
				String column, String data) throws Exception {
	    		hbt.addPut(mkPut(tablename,row, columnFamily, column, data)) ;

		}
	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("kword"));
	    }

		@Override
		public void cleanup() {
//			producer.close();
//			producer=null;
			hbt.commit();
			super.cleanup();
		}
	    
	  }
	
	
	public static void main(String[] args) throws Exception {
		System.out.println("begin.....");
		String kafkaZookeeper = "CH5:2181,CH6:2181,CH7:2181";
		BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper,"/brokers");
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "t4", "/order", "id");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.zkServers =  ImmutableList.of("CH6");
        kafkaConfig.zkPort = 2181;
        //kafkaConfig.forceFromStart = true;
	    TopologyBuilder builder = new TopologyBuilder();
	    builder.setSpout("kafkaspout", new KafkaSpout(kafkaConfig), 2);
	    builder.setBolt("kafkacount", new KafkaWordCount(), 20).shuffleGrouping("kafkaspout");
	    //builder.setBolt("hbasebolt", new HbaseBolt(), 2).fieldsGrouping("kafkacount", new Fields("kword"));
	    Config conf = new Config();
	    conf.setDebug(true);
	    if (args != null && args.length > 0) {
	      conf.setNumWorkers(6);
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
class HBaseThread extends Thread{
	static HTable table = null;
	// 声明静态配置 HBaseConfiguration
	private static Configuration cfg = HBaseConfiguration.create();
	private Queue<Put> puts = new LinkedBlockingQueue<Put>();
	static List<Put> ps=new Vector<Put>(11*10000);
	static AtomicInteger count=new AtomicInteger(0);
	static ThreadPoolExecutor pool=null;
	private String tablename;
	HBaseThread(String tablename) throws IOException{
		this.tablename=tablename;
	}
	@SuppressWarnings("deprecation")
	public void initTable() throws IOException{
		if(table==null){
			pool = HTable.getDefaultExecutor(cfg);
			pool.setCorePoolSize(5);
			pool.setMaximumPoolSize(10);
			pool.prestartCoreThread();
//			pool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
//				@Override
//				public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
//					System.out.println("handle rejectedExecution");
//					System.out.println("pool.isTerminated():"+pool.isTerminated());
//					if(pool.isTerminated()){
//						pool = HTable.getDefaultExecutor(cfg);
//						pool.setCorePoolSize(5);
//						pool.setMaximumPoolSize(10);
//						pool.prestartCoreThread();
//						table.close();
//						table=new HTable(cfg, Bytes.toBytes(this.tablename), pool);
//						table.setWriteBufferSize(5*1024*1024);//5MB
//						table.setAutoFlush(false);
//					}
//				}
//			});
			table=new HTable(cfg, Bytes.toBytes(this.tablename), pool);
			table.setWriteBufferSize(5*1024*1024);//5MB
			table.setAutoFlush(false);
		}else if(pool.isTerminated()){
			pool = HTable.getDefaultExecutor(cfg);
			pool.setCorePoolSize(5);
			pool.setMaximumPoolSize(10);
			pool.prestartCoreThread();
			table.close();
			table=new HTable(cfg, Bytes.toBytes(this.tablename), pool);
			table.setWriteBufferSize(5*1024*1024);//5MB
			table.setAutoFlush(false);
		}
	}
	public synchronized  void addPut(Put p){
		count.incrementAndGet();
		System.out.println("put["+count+"]...");
		//puts.add(p);
		ps.add(p);
	}
	@Override
	public void run() {
		System.out.println("running");
		Put p=null;
		while(!puts.isEmpty()){
			p = puts.poll();
			System.out.println("p="+p.toString());
			ps.add(p);
			if(ps.size()>10){
				commit();
			}
		}
	}

	public synchronized void commit(){
		try {
			System.out.println("triger commit["+ps.size()+"]");
			System.out.println("pool.isTerminated():"+pool.isTerminated());
			System.out.println("pool.getActiveCount():"+pool.getActiveCount());
			if(count.get()%(5*10000)==0){
			System.out.println("hbasedata["+ps.size()+"] commit...");
			initTable();
			table.put(ps);
			table.flushCommits();
			ps.clear();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	@Override
	protected void finalize() throws Throwable {
		if(ps.size()>0){
			System.out.println("hbasedata["+ps.size()+"] commit...");
			initTable();
			table.put(ps);
			table.flushCommits();
			ps.clear();
		}
		table.close();
		super.finalize();
	}
	
}