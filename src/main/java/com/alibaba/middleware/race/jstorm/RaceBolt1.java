package com.alibaba.middleware.race.jstorm;

import java.util.*;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RaceBolt1 implements IRichBolt{
	
	private static final long serialVersionUID = 2L;

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// 输入数据组
		MessageTuple messageTuple = (MessageTuple) tuple.getValue(0);
		// 输出数据组
		ValidInfoTuple validInfoTuple = new ValidInfoTuple();
		
		for (Message message : messageTuple.getMessages()) {

			String topic = message.getTopic();
			byte[] body = message.getBody();

			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				continue; // 不处理
			}

			if (topic.equals(RaceConfig.MqPayTopic)) {
				validInfoTuple.addValidInfos(sendPay(body));
			} else {
				validInfoTuple.addValidInfos(sendTmallOrTaobao(topic, body));
			}

		}
		
		// 汇集后提交
		if(validInfoTuple.getValidInfos().size()>0)
			collector.emit(new Values(validInfoTuple));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("validInfoTuple"));
	}

	@Override
	public void cleanup() {}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	
	
	class PayInfo{
		long time;
		double money;
		short kind;
		public PayInfo(long time, double money,short kind) {
			super();
			this.time = time;
			this.money = money;
			this.kind = kind;
		}
	}
	
	class OrderInfo{
		short kind;
		double totalPrice;
		public OrderInfo(short kind, double totalPrice) {
			super();
			this.kind = kind;
			this.totalPrice = totalPrice;
		}
	}
	
	/** orderId -> (time,money,kind) */
	private Map<Long,List<PayInfo>> payMap = new HashMap<Long,List<PayInfo>>();
	
	/** orderId -> (kind,totalPrice) */
	private Map<Long,OrderInfo> orderMap = new HashMap<Long, OrderInfo>();

	/** 处理支付订单 */
	private List<ValidInfo> sendPay(byte[] body){
		
		List<ValidInfo> validInfos = new ArrayList<>();
		
		PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);	
		double money = paymentMessage.getPayAmount();
		long time = paymentMessage.getCreateTime();
		short kind = paymentMessage.getPayPlatform();  // 0 支付pC 1  支付无线  2  淘宝  3  天猫 
		Long orderId = paymentMessage.getOrderId();
		
		// 消耗时再提交
		// collector.emit(new Values(money, time, kind));
		
		// 缓存订单的支付时间和金钱
		if(orderMap.containsKey(orderId)){ // 订单已来
			
			OrderInfo orderInfo = orderMap.get(orderId);
			if(orderInfo.totalPrice-money<0){ // 消耗完了
				return validInfos;
			}else{ // 可以消耗
				orderInfo.totalPrice -= money;	
			}
			
			// order
			// collector.emit(new Values(money, time, orderInfo.kind ));
			validInfos.add(new ValidInfo(money, time, orderInfo.kind ));
			
			// pay
			//collector.emit(new Values(money, time, kind)); // 消耗时再提交
			validInfos.add(new ValidInfo(money, time, kind));
			
			// 及时删除
			if(orderInfo.totalPrice <= 0){ 
				orderMap.remove(orderId);
				payMap.remove(orderId);
			}
			
		}else{ // 订单没来,或者是支付重复（少量，缓存）
			
			if (!payMap.containsKey(orderId)) {
				payMap.put(orderId, new ArrayList<PayInfo>());
			}
			payMap.get(orderId).add(new PayInfo(time, money,kind));
			
		}
		
		return validInfos;
		    
	}
		
	/** 淘宝或者天猫 */
	private List<ValidInfo> sendTmallOrTaobao(String topic,byte[] body){
		
		List<ValidInfo> validInfos = new ArrayList<>();

		OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);
		short kind = topic.equals(RaceConfig.MqTaobaoTradeTopic) ? Global.TAOBAO_KIND : Global.TMALL_KIND;
		long orderId = orderMessage.getOrderId();
		double totalPrice = orderMessage.getTotalPrice();
		
		// 根据id防止重复
		if(orderMap.containsKey(orderId)){
			return validInfos;
		}
			
		// 缓存:为了匹配总价，所有订单都缓存
		orderMap.put(orderId, new OrderInfo(kind, totalPrice));
		
		if (payMap.containsKey(orderId)) { // 支付已来
			
			OrderInfo orderInfo = orderMap.get(orderId);
			
			// 按支付的时间分发
			for (PayInfo info : payMap.get(orderId)){
				if(orderInfo.totalPrice-info.money<0){ // 消耗完了
					break;
				}else{ // 可以消耗
					orderInfo.totalPrice -= info.money;
				}
				// order
				// collector.emit(new Values(info.money, info.time, kind));
				validInfos.add(new ValidInfo(info.money, info.time, kind));
				// pay
				// collector.emit(new Values(info.money, info.time, info.kind)); // 消耗时再提交
				validInfos.add(new ValidInfo(info.money, info.time, info.kind));
			}
			
			// 及时删除
			if (orderInfo.totalPrice <= 0) {
				orderMap.remove(orderId);
				payMap.remove(orderId);
			}
			
		} 
		
		return validInfos;
	}
	

}
