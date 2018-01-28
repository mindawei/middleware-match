package com.alibaba.middleware.mindw;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.taobao.tair.impl.DefaultTairManager;


public class DealCenter {
	
	private static Logger LOG = Logger.getLogger(DealCenter.class);
	
	private DefaultTairManager tairManager;
	
	public void run() {
		initConsumer();
		initTair();
	}
	
	class RaceMessageListener implements MessageListenerConcurrently{

		/** 初始时间 */
		private static final long INITIATE_TIME = -1;
		
		// >>> 支付信息
		
		/** 支付信息最新时间段 */
		private long pay_last_minute = INITIATE_TIME;
		/** 0 pc 支付总和 */
		private double pcSum = 0;   
		/** 1  无线  支付总和 */
		private double wireSum = 0; 
		/** double类型输出格式,保留两位小数 */
		private DecimalFormat df = new DecimalFormat("#.00");
		
		/** 是否结束 */
		private boolean pay_end = false;
		
		private void dealPay(byte[] body){
			
			// 结束时把最后一个值输出
			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				if(!pay_end){ // 并不是马上结束，可能还会读到结束的标志 ?
					// 每整分时刻无线和PC端总交易金额比值，官方说肯定有记录
					double ratio = wireSum / pcSum; 
					ratio = Double.parseDouble(df.format(ratio));
					write(RaceConfig.prex_ratio + pay_last_minute, ratio);
					pay_end = true;
				}
				return;
			}

			PaymentMessage paymentMessage = RaceUtils.readKryoObject(PaymentMessage.class, body);

			// 支付费用、支付平台、支付时间
			double payAmount = paymentMessage.getPayAmount();
			short payPlatform = paymentMessage.getPayPlatform();
			long createTime = paymentMessage.getCreateTime();
			
			// 转换为整分
			Long minuteTime = (createTime / 60000) * 60;
			
			// 是否要把之前的输出
			if (minuteTime > pay_last_minute) {
				if (pay_last_minute != INITIATE_TIME) { // 排除第一次接受数据
					// 每整分时刻无线和PC端总交易金额比值，官方说肯定有记录
					double ratio = wireSum / pcSum; 
					ratio = Double.parseDouble(df.format(ratio));
					write(RaceConfig.prex_ratio + pay_last_minute, ratio);
				}
				// 更新 时间
				pay_last_minute = minuteTime;
			}
			
			// 0 pc
			if(payPlatform==0)
				pcSum += payAmount;
			
			// 1 无线 
			if(payPlatform==1)
				wireSum += payAmount;
			
		}
		
		
		// >>> 淘宝订单数据
		
		private long taobao_last_minute = INITIATE_TIME;
		/** 每分钟的支付总额 */
		private double taobao_minute_sum = 0; 
		/** 是否结束 */
		private boolean taobao_end = false;
		
		private void dealTaobao(byte[] body){
			
			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				if(!taobao_end){ 
					write(RaceConfig.prex_taobao + taobao_last_minute,taobao_minute_sum);
					taobao_minute_sum = 0; // 清0
					taobao_end = true;
				}
				return;
			}

			OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);

			// 消息: 订单总价, 订单时间, 淘宝 0 or 天猫 1
			double totalPrice = orderMessage.getTotalPrice();
			long createTime = orderMessage.getCreateTime();

			// 转换为整分
			Long minuteTime = (createTime / 60000) * 60;

			// 是否要把之前的输出
			if (minuteTime > taobao_last_minute) {
				if (taobao_last_minute != INITIATE_TIME) { // 排除第一次接受数据
					write(RaceConfig.prex_taobao + taobao_last_minute,taobao_minute_sum);
					taobao_minute_sum = 0; // 清0
				}
				// 更新 时间
				taobao_last_minute = minuteTime;
			}
			taobao_minute_sum += totalPrice;

		}
		
		
	    // >>> 天猫订单数据
		
		private long tmall_last_minute = INITIATE_TIME;
		/** 每分钟的支付总额 */
		private double tmall_minute_sum = 0; 
		/** 是否结束 */
		private boolean tmall_end = false;
		
		private void dealTmall(byte[] body){
			
			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				if(!tmall_end){ 
					write(RaceConfig.prex_taobao + tmall_last_minute,tmall_minute_sum);
					tmall_minute_sum = 0; // 清0
					tmall_end = true;
				}
				return;
			}

			OrderMessage orderMessage = RaceUtils.readKryoObject(OrderMessage.class, body);

			// 消息: 订单总价, 订单时间, 淘宝 0 or 天猫 1
			double totalPrice = orderMessage.getTotalPrice();
			long createTime = orderMessage.getCreateTime();

			// 转换为整分
			Long minuteTime = (createTime / 60000) * 60;

			// 是否要把之前的输出
			if (minuteTime > tmall_last_minute) {
				if (tmall_last_minute != INITIATE_TIME) { // 排除第一次接受数据
					write(RaceConfig.prex_tmall + tmall_last_minute,tmall_minute_sum);
					tmall_minute_sum = 0; // 清0
				}
				// 更新 时间
				tmall_last_minute = minuteTime;
			}
			tmall_minute_sum += totalPrice;

		}

		@Override
		public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,ConsumeConcurrentlyContext context) {
			
			for (MessageExt msg : msgs) {
				
				byte[] body = msg.getBody();
		
				// 分发消息
				if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {
					dealPay(body);
				} else if (msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)) {
					dealTaobao(body);
				} else if (msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)) {
					dealTmall(body);
				}
			}
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		}
	}
	
	/** initiate Consumer */
	private void initConsumer() {
		try {
			DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);

			// where pull begin
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

			// need to use when debug local
			// String nameSrvAddr = "127.0.0.1:9876";
			// consumer.setNamesrvAddr(Global.nameSrvAddr);

			// topic
			consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqPayTopic, "*");

			// the number of message pull every time
			// int consumeMessageBatchMaxSize = 10;
			// consumer.setConsumeMessageBatchMaxSize(consumeMessageBatchMaxSize);

			consumer.registerMessageListener(new RaceMessageListener());
			consumer.start();
			LOG.info("Consumer in spout Started.");

		} catch (MQClientException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	/** initiate tair */
	private void initTair(){
		
		// 创建config server列表
        List<String> confServers = new ArrayList<String>();
        confServers.add(RaceConfig.TairConfigServer); 
        confServers.add(RaceConfig.TairSalveConfigServer); // 可选

        // 创建客户端实例
        DefaultTairManager tairManager = new DefaultTairManager();
        tairManager.setConfigServerList(confServers);

        // 设置组名
        tairManager.setGroupName(RaceConfig.TairGroup);
        
        // 初始化客户端
        tairManager.init();
	}
	
	/** 将结果写到Tari中 */
	private void write(Serializable key, Serializable value) {
		// 第一个参数是namespace，第二个是key，第三是value，第四个是版本，第五个是有效时间
		tairManager.put(RaceConfig.TairNamespace, key, value);
		LOG.info(key + " " + value);
	}
}
