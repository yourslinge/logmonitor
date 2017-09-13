package cn.itcast.shizhan_storm_mvn.kafka_storm_demo.tmall;

import java.util.Map;

import cn.itcast.shizhan_storm_mvn.kafka_storm_demo.tmall.other.PaymentInfo;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年5月29日 下午7:28:01
 */
public class TestPaymentInfoSpout extends BaseRichSpout{

	private SpoutOutputCollector collector;
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		collector.emit(new Values(new PaymentInfo().random()));
		Utils.sleep(100);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("paymentInfo"));
	}

}
