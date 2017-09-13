package cn.itcast.shizhan_storm_mvn.kafka_operation;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年5月26日 下午6:30:06
 */
public class MyLogPartitioner implements Partitioner{

	public MyLogPartitioner(VerifiableProperties props){
		
	}
	@Override
	public int partition(Object obj, int numPartitions) {
		return Integer.parseInt(obj.toString())%numPartitions;
	}

}
