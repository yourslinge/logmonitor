package cn.itcast.shizhan_storm_mvn.kafka_storm_demo.tmall.other;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年5月29日 下午2:12:43
 */
public class PaymentSender {

	public static void main(String[] args) {
		String topic = "payment";
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.78.90:9092,192.168.78.91:9092,192.168.78.92:9092");
		props.put("request.required.acks", "1");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        for(int i=0;i<1000;i++){
        	producer.send(new KeyedMessage<String, String>(topic, i+"", new PaymentInfo().random()));
        }
		
	}
}
