package cn.itcast.shizhan_storm_mvn.kafka_storm_demo.kafka_storm;

import java.util.Map;

import cn.itcast.shizhan_storm_mvn.kafka_storm_demo.kafka_storm.order.OrderInfo;

import com.google.gson.Gson;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年4月4日 下午1:09:10
 */
public class ParserOrderMqBolt extends BaseRichBolt{

	private JedisPool pool;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		//change "maxActive" -> "maxTotal" and "maxWait" -> "maxWaitMillis" in all examples
		JedisPoolConfig config = new JedisPoolConfig();
		//控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
		//控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
		//如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
		config.setMaxIdle(3);
		config.setMaxTotal(1000*100);
		//表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
		config.setMaxWaitMillis(30);
		//在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	    config.setTestOnBorrow(true);
	    config.setTestOnReturn(true);
	    /**
	     *如果你遇到 java.net.SocketTimeoutException: Read timed out exception的异常信息
	     *请尝试在构造JedisPool的时候设置自己的超时值. JedisPool默认的超时时间是2秒(单位毫秒)
	     */
	    pool = new JedisPool(config, "192.168.78.89", 6379);
	}

	@Override
	public void execute(Tuple input) {
		Jedis jedis = pool.getResource();
		//网络传输走的都是序列化，也即是比特流，这边使用byte转型是合理的
		String jsonStr = new String((byte[])input.getValue(0));
		OrderInfo orderInfo = new Gson().fromJson(jsonStr, OrderInfo.class);
		//整个网站，各个业务线，各个品类，各个店铺，各个品牌，每个商品
		//获取整个网站的金额统计指标
		String totalValue = jedis.get("totalValue");
		System.out.println("当前网站总金额："+totalValue);
		jedis.incr("totalValue");
		jedis.close();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}


}
