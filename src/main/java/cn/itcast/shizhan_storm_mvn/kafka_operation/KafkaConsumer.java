package cn.itcast.shizhan_storm_mvn.kafka_operation;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年5月28日 下午7:30:30
 */
public class KafkaConsumer implements Runnable{
	
	private String title;
	private KafkaStream<byte[], byte[]> stream;
	
	public KafkaConsumer(String title, KafkaStream<byte[], byte[]> kafkaStream) {
		this.title = title;
		this.stream = kafkaStream;
	}

	@Override
	public void run() {
		System.out.println("开始运行"+title);
		ConsumerIterator<byte[],byte[]> it = stream.iterator();
		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> data = it.next();
			String topic = data.topic();
			int partition = data.partition();
			long offset = data.offset();
			String msg = new String(data.message());
			System.out.println(String.format("Consumer: [%s],  Topic: [%s],  PartitionId: [%d], Offset: [%d], msg: [%s]",
									title, topic, partition, offset, msg));
		}
		System.out.println(String.format("Consumer: [%s] exiting ...", title));
	}
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("group.id", "zero");
		props.put("zookeeper.connect", "weekend05:2181,weekend06:2181,weekend07:2181");
		props.put("auto.offset.reset", "smallest");
		ConsumerConfig config = new ConsumerConfig(props);
		String topic = "order";
		//只要ConsumerConnector还在的话，consumer会一直等待新消息，不会自己退出
		ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(config);
		//定义一个map
		Map<String,Integer> topicMap = new HashMap<String, Integer>(); 
		topicMap.put(topic, 1);
		//Map<String, List<KafkaStream<byte[], byte[]>> 中String是topic， List<KafkaStream<byte[], byte[]>是对应的流
		Map<String, List<KafkaStream<byte[], byte[]>>> topicStreamMap = consumerConnector.createMessageStreams(topicMap);
		//取出 topic 对应的 streams
		List<KafkaStream<byte[], byte[]>> topicStream = topicStreamMap.get(topic);
		//创建一个容量为4的线程池
		ExecutorService executor = Executors.newFixedThreadPool(4);
		
		for(int i=0;i<topicStream.size();i++){
			executor.execute(new KafkaConsumer("消费者"+i, topicStream.get(i)));
		}
		
		
		
		
		
		
		
		
		
	}
}
