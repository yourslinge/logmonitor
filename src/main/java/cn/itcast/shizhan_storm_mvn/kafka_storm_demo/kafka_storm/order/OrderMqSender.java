package cn.itcast.shizhan_storm_mvn.kafka_storm_demo.kafka_storm.order;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年4月4日 下午12:10:47
 */
public class OrderMqSender {

	public static void main(String[] args) {
		String topic = "orderMq";
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.78.90:9092,192.168.78.91:9092,192.168.78.92:9092");
		props.put("request.required.acks", "1");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        
        Producer<String, String> producer = new Producer<String, String>(new ProducerConfig(props));
        
        for(int messageNo = 3;messageNo<10000;messageNo++){
        	producer.send(new KeyedMessage<String, String>(topic, messageNo+"", new OrderInfo().random()));
        }
	}
}
