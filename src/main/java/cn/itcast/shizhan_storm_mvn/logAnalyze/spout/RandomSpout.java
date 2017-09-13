package cn.itcast.shizhan_storm_mvn.logAnalyze.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.gson.Gson;

import cn.itcast.shizhan_storm_mvn.logAnalyze.domain.LogMessage;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年4月23日 上午10:20:09
 */
public class RandomSpout extends BaseRichSpout{

	private TopologyContext context;
	private SpoutOutputCollector collector;
	private List<LogMessage> list;
	public RandomSpout(){}
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.context = context;
		this.collector = collector;
		list = new ArrayList<LogMessage>();
		list.add(new LogMessage(1,"http://www.itcast.cn/product?id=1002",
                "http://www.itcast.cn/","maoxiangyi"));
        list.add(new LogMessage(1,"http://www.itcast.cn/product?id=1002",
                "http://www.itcast.cn/","maoxiangyi"));
        list.add(new LogMessage(1,"http://www.itcast.cn/product?id=1002",
                "http://www.itcast.cn/","maoxiangyi"));
        list.add(new LogMessage(1,"http://www.itcast.cn/product?id=1002",
                "http://www.itcast.cn/","maoxiangyi"));
		
	}

	@Override
	public void nextTuple() {
		final Random rand = new Random();
		LogMessage msg = list.get(rand.nextInt(4));
		collector.emit(new Values(new Gson().toJson(msg)));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("paymentInfo"));
	}

}
