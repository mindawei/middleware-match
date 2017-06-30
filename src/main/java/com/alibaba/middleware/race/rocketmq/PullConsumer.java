package com.alibaba.middleware.race.rocketmq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.MQPullConsumerScheduleService;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;

/**
 * @Description: 主动去拉数据
 * @Author: 闵大为
 * @Since:2016年6月24日
 * @Version:0.1.0
 */
public class PullConsumer {

	/** 记录对应的队列读到哪个位置了 */
	private static final Map<MessageQueue, Long> offseTable = new HashMap<MessageQueue, Long>();

	/** 保存下一个位置 */
	private static void putMessageQueueOffset(MessageQueue mq, long offset) {
		offseTable.put(mq, offset);
	}

	/** 获得下一个位置 */
	private static long getMessageQueueOffset(MessageQueue mq) {
		Long offset = offseTable.get(mq);
		if (offset != null)
			return offset;
		return 0;
	}

	public static void main(String[] args) throws MQClientException {

		DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("PullConsumerGroup_mindw");
		consumer.setNamesrvAddr("127.0.0.1:9876");
		consumer.start();

		// 拉取订阅主题的队列，默认队列大小是4
		Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(RaceConfig.MqPayTopic);
		
		for (MessageQueue mq : mqs) {
			System.out.println("Consume from the queue: " + mq);
			SINGLE_MQ: while (true) {
				try {
					PullResult pullResult = consumer.pullBlockIfNotFound(mq,
							null, getMessageQueueOffset(mq), 32);
					List<MessageExt> list = pullResult.getMsgFoundList();
					if (list != null) {
						for (MessageExt msg : list) {
							byte[] body = msg.getBody();
							if (body.length == 2 && body[0] == 0
									&& body[1] == 0) {
								// Info: 生产者停止生成数据, 并不意味着马上结束
								System.out.println("Got the end signal");
								continue;
							}
							PaymentMessage paymentMessage = RaceUtils
									.readKryoObject(PaymentMessage.class, body);
							System.out.println(paymentMessage);
						}

					}

					putMessageQueueOffset(mq, pullResult.getNextBeginOffset());
					switch (pullResult.getPullStatus()) {
					case FOUND: // 找到信息
						break;
					case NO_MATCHED_MSG: // 没有匹配的信息
						break;
					case NO_NEW_MSG: // 没有信息
						break SINGLE_MQ;
					case OFFSET_ILLEGAL: // 取的位置不对
						break;
					default:
						break;
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}
		consumer.shutdown();

	}

}