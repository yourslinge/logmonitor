package cn.itcast.shizhan_storm_mvn.kafka_storm_demo.tmall;

import java.util.HashMap;
import java.util.Map;

import cn.itcast.shizhan_storm_mvn.kafka_storm_demo.tmall.other.PaymentInfo;

import com.google.gson.Gson;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * @author linge E-mail:
 * @version 
 * Created on 2017年5月30日 上午8:56:17
 */
public class Save2RedisBolt extends BaseBasicBolt{
	//private JedisPool pool;
	//storm中共享一个序列化对象
	private Map<String,Integer> counter = new HashMap<String, Integer>();
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		/*
		JedisPoolConfig config = new JedisPoolConfig();
		config.setMaxIdle(5);
		config.setMaxTotal(1000*100);
		config.setMaxWaitMillis(3000);
		config.setTestOnBorrow(true);;
		config.setTestOnReturn(true);
		pool = new JedisPool(config, "192.168.78.89", 6379, 50);
		*/
		super.prepare(stormConf, context);
	}
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		//读取订单数据
		String paymentInfoStr = input.getStringByField("message");
		//将订单数据解析成JavaBean
		PaymentInfo paymentInfo = new Gson().fromJson(paymentInfoStr, PaymentInfo.class);
		//计算业务订单总数
		//Jedis jedis = pool.getResource();
		if(paymentInfo!=null){
			if(counter.containsKey("orderTotalNum")){
				Integer orderTotalNum = counter.get("orderTotalNum");
				counter.put("orderTotalNum", orderTotalNum+1);
			}else{
				counter.put("orderTotalNum", 1);
			}
			
			/*
			//计算订单的总数
			jedis.incrBy("orderTotalNum", 1);
			//计算总的销售额
			jedis.incrBy("orderTotalPrice", paymentInfo.getProductPrice());
			//计算折扣后的销售额
            jedis.incrBy("orderPromotionPrice", paymentInfo.getPromotionPrice());
            //计算实际交易额
            jedis.incrBy("orderTotalRealPay", paymentInfo.getPayPrice());
            jedis.incrBy("userNum", 1);
            */
		}
		System.out.println("订单总数：" + counter.get("orderTotalNum"));
		/*
		System.out.println("订单总数：" + jedis.get("orderTotalNum") +
                "   销售额" + jedis.get("orderTotalPrice") +
                "   交易额" + jedis.get("orderPromotionPrice") +
                "   实际支付：" + jedis.get("orderTotalRealPay") +
                "   下单用户：" + jedis.get("userNum"));
                */
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

}
