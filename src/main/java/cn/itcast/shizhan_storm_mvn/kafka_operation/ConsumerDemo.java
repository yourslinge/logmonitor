package cn.itcast.shizhan_storm_mvn.kafka_operation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年5月27日 上午9:53:18
 */
public class ConsumerDemo {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zookeeper.connect", "weekend05:2181,weekend06:2181,weekend07:2181");
		props.put("group.id", "1111");
		props.put("auto.offset.reset", "smallest");
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
		Map<String,Integer> topicMap = new HashMap<String, Integer>();
		topicMap.put("order", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get("order");
		System.out.println(streams.size());
		for (final KafkaStream<byte[], byte[]> kafkaStream : streams) {
			new Thread(new Runnable() {
				@Override
				public void run() {
					for (MessageAndMetadata<byte[], byte[]> mm : kafkaStream) {
						String msg = new String(mm.message());
						System.out.println(msg);
					}
				}
			}).start();
		}
	}
}
