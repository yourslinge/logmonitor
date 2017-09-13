package cn.itcast.shizhan_storm_mvn.kafka_storm_demo.kafka_storm;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年4月4日 下午12:58:19
 */
public class KafkaAndStormTopologyMain {

	public static void main(String[] args) throws Exception {
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		//zookeeper地址，原因：zk上存储有消费的offset
		topologyBuilder.setSpout("kafkaSpout", new KafkaSpout(new SpoutConfig(
				new ZkHosts("192.168.78.90:2181,192.168.78.91:2181,192.168.78.92:2181"), 
				"orderMq", 
				"/myKafka", 
				"kafkaSpout")), 1);
		topologyBuilder.setBolt("myBolt", new ParserOrderMqBolt(), 1).shuffleGrouping("kafkaSpout");
		
		Config config = new Config();
		config.setNumWorkers(1);
		//提交任务的两种模式
		if(args.length>0){
			//集群模式
			StormSubmitter.submitTopology(args[0], config, topologyBuilder.createTopology());
		}else{
			//本地模式
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("storm2kafka", config, topologyBuilder.createTopology());
		}		
	}
}
