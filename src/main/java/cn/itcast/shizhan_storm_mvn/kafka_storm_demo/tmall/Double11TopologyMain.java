package cn.itcast.shizhan_storm_mvn.kafka_storm_demo.tmall;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年5月29日 下午1:49:17
 * 程序说明:
 * 根据双十一当天的订单mq，快速计算当天的订单量、销售金额
 * 思路：
 * 1,支付系统发送mq到kafka集群中，编写storm程序消费kafka的数据并计算实时的订单数量、订单数量
 * 2,将计算的实时结果保存在redis中
 * 3,外部程序实时展示结果
 * 程序设计
 * 数据产生：编写kafka数据生产者，模拟订单系统发送mq
 * 数据输入：使用PaymentSpout消费kafka中的数据
 * 数据计算：使用CountBolt对数据进行统计
 * 数据存储：使用Sava2RedisBolt对数据进行存储
 * 数据展示：编写java app客户端，对数据进行展示，展示方式为打印在控制台。
 * Data:     2015/11/3.
 */
public class Double11TopologyMain {

	public static void main(String[] args) throws Exception{
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("readPaymentInfoSpout", new TestPaymentInfoSpout(), 1);
		builder.setBolt("filterBolt", new FilterMessageBolt(), 2).shuffleGrouping("readPaymentInfoSpout");
		builder.setBolt("saveBolt", new Save2RedisBolt(), 1).shuffleGrouping("filterBolt");
		Config conf = new Config();
		if(args.length>0){
			conf.setNumWorkers(2);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}else{
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("double11", conf, builder.createTopology());
		}
	}
}
