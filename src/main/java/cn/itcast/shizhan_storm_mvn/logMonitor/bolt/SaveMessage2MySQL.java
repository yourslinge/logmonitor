package cn.itcast.shizhan_storm_mvn.logMonitor.bolt;

import cn.itcast.shizhan_storm_mvn.logMonitor.domain.Record;
import cn.itcast.shizhan_storm_mvn.logMonitor.utils.MonitorHandler;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年4月8日 下午3:37:23
 */
public class SaveMessage2MySQL extends BaseBasicBolt{

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		Record record = (Record) input.getValueByField("record");
		MonitorHandler.save(record);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
