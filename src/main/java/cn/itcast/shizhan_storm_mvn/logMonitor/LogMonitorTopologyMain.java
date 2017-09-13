package cn.itcast.shizhan_storm_mvn.logMonitor;

import org.apache.log4j.Logger;

import cn.itcast.shizhan_storm_mvn.logMonitor.bolt.FilterBolt;
import cn.itcast.shizhan_storm_mvn.logMonitor.bolt.PrepareRecordBolt;
import cn.itcast.shizhan_storm_mvn.logMonitor.bolt.SaveMessage2MySQL;
import cn.itcast.shizhan_storm_mvn.logMonitor.spout.RandomSpout;
import cn.itcast.shizhan_storm_mvn.logMonitor.spout.StringScheme;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年4月8日 下午3:39:03
 */
public class LogMonitorTopologyMain {

	private static Logger logger = Logger.getLogger(LogMonitorTopologyMain.class);
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("kafka-spout", new RandomSpout(new StringScheme()), 2);
		builder.setBolt("filter-bolt", new FilterBolt(), 2).shuffleGrouping("kafka-spout");
		builder.setBolt("prepareRecord-bolt", new PrepareRecordBolt(), 2).fieldsGrouping("filter-bolt", new Fields("appId"));
		builder.setBolt("saveMessage-bolt", new SaveMessage2MySQL(), 2).shuffleGrouping("prepareRecord-bolt");
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("topology", conf, builder.createTopology());
		
	}
}
