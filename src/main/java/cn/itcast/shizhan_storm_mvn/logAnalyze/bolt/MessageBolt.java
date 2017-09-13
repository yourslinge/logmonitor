package cn.itcast.shizhan_storm_mvn.logAnalyze.bolt;

import cn.itcast.shizhan_storm_mvn.logAnalyze.domain.LogMessage;
import cn.itcast.shizhan_storm_mvn.logAnalyze.utils.LogAnalyzeHandler;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年4月23日 下午1:48:16
 */
public class MessageBolt extends BaseBasicBolt{

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);
		LogMessage logMessage = LogAnalyzeHandler.parser(line);
		if(logMessage==null||LogAnalyzeHandler.isValidType(logMessage.getType())){
			return;
		}
		collector.emit(new Values(logMessage.getType(),logMessage));
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("type","message"));
	}

}
