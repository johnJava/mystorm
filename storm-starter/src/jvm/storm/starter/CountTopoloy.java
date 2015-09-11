package storm.starter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.Scheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.ImmutableList;

public class CountTopoloy {
	 
	
	public static class KafkaWordCount extends BaseBasicBolt {
		static DataHandler dh = null;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			try {
				dh = new DataHandler("order");
				dh.initTable();
			} catch (IOException e) {
				e.printStackTrace();
			}
			super.prepare(stormConf, context);
		}
		private static final long serialVersionUID = 1L;
		private static final Map<String, ConcurrentHashMap<String, Integer>> ymap=new ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>();
		@Override
		public synchronized void execute(Tuple tuple, BasicOutputCollector collector) {
			System.out.println("msg:" + tuple);
			List<Object> list = tuple.getValues();
			if(list.size()==6&&!"error".equalsIgnoreCase((String) list.get(0))){
				try {
					//synchronized(ysumset) {
					String id = (String) list.get(0);// values[0];
					String memberid = (String) list.get(1);// values[1];
					String totalprice = (String) list.get(2);// values[2];
					String youhui = (String) list.get(3);// values[3];
					String sendpay = (String) list.get(4);// values[4];
					String createdate = (String) list.get(5);// values[5];
					System.out.println("data:" + list);
					System.out.println("sendpay:" + sendpay);
					long longMax = Long.MAX_VALUE;
					put("order", String.valueOf((longMax - Long.parseLong(createdate))), "info", "id", id);
					put("order", String.valueOf((longMax - Long.parseLong(createdate))), "info", "memberid", memberid);
					put("order", String.valueOf((longMax - Long.parseLong(createdate))), "info", "totalprice", totalprice);
					put("order", String.valueOf((longMax - Long.parseLong(createdate))), "info", "youhui", youhui);
					put("order", String.valueOf((longMax - Long.parseLong(createdate))), "info", "sendpay", sendpay);
					put("order", String.valueOf((longMax - Long.parseLong(createdate))), "info", "createdate", createdate);
					dh.commit();
					ConcurrentHashMap<String, Integer> ym = ymap.get(sendpay);
					if(ym==null){
						ym =new ConcurrentHashMap<String, Integer>();
						ymap.put(sendpay, ym);
					} 
					Integer ysum = ym.get("sum");
					if (ysum == null)
						ysum = 0;
					ysum+=Integer.valueOf(youhui);
					ym.put("sum", ysum);
					Integer ycount = ym.get("count");
					if (ycount == null)
						ycount = 0;
					ycount++;
					ym.put("count", ycount);
					collector.emit(new Values(sendpay,ysum,ycount));
					//}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		public Put mkPut(String tablename, String row, String columnFamily, String column, String data) throws Exception {
			Put p1 = new Put(Bytes.toBytes(row));
			p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
			return p1;
		}

		public void put(String tablename, String row, String columnFamily, String column, String data) throws Exception {
			dh.addPut(mkPut(tablename, row, columnFamily, column, data));

		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("sendpay","sum","count"));
		}

		@Override
		public void cleanup() {
			dh.commit();
			super.cleanup();
		}

	}

	public static class KafkaPrint extends BaseBasicBolt {
	    /**
		 * 
		 */
		private static final long serialVersionUID = 1L;

	    @Override
	    public void execute(Tuple tuple, BasicOutputCollector collector) {
	      System.out.println("KafkaPrint:"+tuple.toString());
	    }

	    @Override
	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("word"));
	    }
	  }
	public static void main(String[] args) throws Exception {
		System.out.println("begin.....");
		String kafkaZookeeper = "CH5:2181,CH6:2181,CH7:2181";
		BrokerHosts brokerHosts = new ZkHosts(kafkaZookeeper);
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, "t5", "/order", "id");
		KafakMsgScheme kmscheme = new KafakMsgScheme("id","memberid","totalprice","youhui","sendpay","createdate");
		kafkaConfig.scheme = new SchemeAsMultiScheme(kmscheme);
		kafkaConfig.zkServers = ImmutableList.of("CH6");
		kafkaConfig.zkPort = 2181;
		kafkaConfig.id="counttopology";
		// kafkaConfig.forceFromStart = true;
		TopologyBuilder builder = new TopologyBuilder();
		//builder.setSpout("kafkaspout", new KafkaDemoSpout(kafkaConfig), 1);
		builder.setSpout("kafkaspout", new KafkaSpout(kafkaConfig),1);
		builder.setBolt("kafkacount", new KafkaWordCount(), 30).fieldsGrouping("kafkaspout", new Fields("sendpay"));//.shuffleGrouping("kafkaspout");//.fieldsGrouping("kafkaspout", new Fields("sendpay"));
		builder.setBolt("print", new KafkaPrint(), 1).globalGrouping("kafkacount");
		Config conf = new Config();
		conf.setDebug(true);
		if (args != null && args.length > 0) {
			conf.setNumWorkers(12);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafka-count", conf, builder.createTopology());
			Thread.sleep(10000);
			cluster.shutdown();
		}
	}
}

class DataHandler extends Thread {
	static HTable table = null;
	// 声明静态配置 HBaseConfiguration
	private static Configuration cfg = HBaseConfiguration.create();
	private Queue<Put> puts = new LinkedBlockingQueue<Put>();
	static List<Put> ps = new Vector<Put>(11 * 10000);
	static AtomicInteger count = new AtomicInteger(0);
	static ThreadPoolExecutor pool = null;
	private String tablename;

	DataHandler(String tablename) throws IOException {
		this.tablename = tablename;
	}

	@SuppressWarnings("deprecation")
	public void initTable() throws IOException {
		if (table == null) {
			pool = HTable.getDefaultExecutor(cfg);
			pool.setCorePoolSize(5);
			pool.setMaximumPoolSize(10);
			pool.prestartCoreThread();
			table = new HTable(cfg, Bytes.toBytes(this.tablename), pool);
			table.setWriteBufferSize(5 * 1024 * 1024);// 5MB
			table.setAutoFlush(false);
		} else if (pool.isTerminated()) {
			pool = HTable.getDefaultExecutor(cfg);
			pool.setCorePoolSize(5);
			pool.setMaximumPoolSize(10);
			pool.prestartCoreThread();
			table.close();
			table = new HTable(cfg, Bytes.toBytes(this.tablename), pool);
			table.setWriteBufferSize(5 * 1024 * 1024);// 5MB
			table.setAutoFlush(false);
		}
	}

	public synchronized void addPut(Put p) {
		count.incrementAndGet();
		System.out.println("put[" + count + "]...");
		ps.add(p);
	}

	@Override
	public void run() {
		System.out.println("running");
		Put p = null;
		while (!puts.isEmpty()) {
			p = puts.poll();
			System.out.println("p=" + p.toString());
			ps.add(p);
			if (ps.size() > 10) {
				commit();
			}
		}
	}

	public synchronized void commit() {
		try {
			System.out.println("triger commit[" + ps.size() + "]");
			System.out.println("pool.isTerminated():" + pool.isTerminated());
			System.out.println("pool.getActiveCount():" + pool.getActiveCount());
			//if (count.get() % (5 * 10000) == 0) {
				System.out.println("hbasedata[" + ps.size() + "] commit...");
				initTable();
				table.put(ps);
				table.flushCommits();
				ps.clear();
			//}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void finalize() throws Throwable {
		if (ps.size() > 0) {
			System.out.println("hbasedata[" + ps.size() + "] commit...");
			initTable();
			table.put(ps);
			table.flushCommits();
			ps.clear();
		}
		table.close();
		super.finalize();
	}

}
class KafakMsgScheme implements Scheme {
	private static final long serialVersionUID = -8067580225874801465L;
	private String[] vs ;
	public KafakMsgScheme(String... vals) {
		vs = vals;
	}
    @Override
    public List<Object> deserialize(byte[] bytes) {
    	String[] values=null;
		try {
			String msg = new String(bytes, "UTF-8");
			values = msg.split("\t");
			System.out.println("deserialize msg:"+msg);
		} catch (UnsupportedEncodingException e1) {
			e1.printStackTrace();
		}
		try {
            return new Values(values[0],values[1],values[2],values[3],values[4],values[5]);
        } catch (ArrayIndexOutOfBoundsException e) {
			System.out.println( "ArrayIndexOutOfBoundsException:values["+values+"]");
			return new Values("error","error","error","error","error","error");
		}
    }
 
    @Override
    public Fields getOutputFields() {
        return new Fields(vs);
    }
 
}