package cn.itcast.shizhan_storm_mvn.kafka_storm_demo.tmall;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
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
 * Created on 2017年5月29日 下午7:47:42
 * 本类在程序执行的时候会有一些逻辑问题存在！是因为open方法只执行一次，然后在执行其他方法，但是本程序的open方法是只要
 * kafka topic中存在数据，就不会执行完毕，所以程序就会卡在这个方法里，不会继续执行下面的步骤，比如：nextTuple()
 */
public class PaymentInfoSpout extends BaseRichSpout{

	private SpoutOutputCollector collector;
	private static final String topic = "Payment";
	//ArrayBlockingQueue是一个由数组支持的有界阻塞队列。此队列按 FIFO（先进先出）原则对元素进行排序。
    // 队列的头部 是在队列中存在时间最长的元素
	private ArrayBlockingQueue<String> paymentInfoQueue = new ArrayBlockingQueue<String>(100);
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "192.168.78.90:9092,192.168.78.91:9092,192.168.78.92:9092");
		props.put("request.required.acks", "1");
        props.put("partitioner.class", "kafka.producer.DefaultPartitioner");
        ConsumerConfig config = new ConsumerConfig(props);
        ConsumerConnector consumerConn = Consumer.createJavaConsumerConnector(config);
        Map<String,Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> topicStreams = consumerConn.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> topicStream = topicStreams.get(topic);
        KafkaStream<byte[], byte[]> stream = topicStream.get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while(it.hasNext()){
        	try {
				paymentInfoQueue.put(new String(it.next().message()));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        }
	}

	@Override
	public void nextTuple() {
		try {
			collector.emit(new Values(paymentInfoQueue.take()));
			Utils.sleep(100);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("paymentInfo"));
	}

}
