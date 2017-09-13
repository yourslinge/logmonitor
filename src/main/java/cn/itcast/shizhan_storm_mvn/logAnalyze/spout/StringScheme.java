package cn.itcast.shizhan_storm_mvn.logAnalyze.spout;

import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年4月23日 上午10:14:42
 */
public class StringScheme implements Scheme{

	@Override
	public List<Object> deserialize(byte[] bytes) {
		return new Values(new String(bytes));
	}

	@Override
	public Fields getOutputFields() {
		
		return new Fields("line");
	}

}
