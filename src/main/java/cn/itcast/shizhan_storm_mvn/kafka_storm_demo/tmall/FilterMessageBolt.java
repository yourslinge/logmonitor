package cn.itcast.shizhan_storm_mvn.kafka_storm_demo.tmall;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import cn.itcast.shizhan_storm_mvn.kafka_storm_demo.tmall.other.PaymentInfo;

import com.google.gson.Gson;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年5月29日 下午8:42:26
 */
public class FilterMessageBolt extends BaseBasicBolt{

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		super.prepare(stormConf, context);
	}
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String paymentInfoStr = input.getStringByField("paymentInfo");
		PaymentInfo paymentInfo = new Gson().fromJson(paymentInfoStr, PaymentInfo.class);
		Date date = paymentInfo.getCreateOrderTime();
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		if(cal.get(Calendar.DAY_OF_MONTH)!=11){
			System.out.println("日期不对！");
			return;
		}
		collector.emit(new Values(paymentInfoStr));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("message"));
	}

}
