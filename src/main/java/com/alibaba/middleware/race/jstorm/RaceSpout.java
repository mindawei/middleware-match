package com.alibaba.middleware.race.jstorm;

import java.util.*;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.task.TopologyContext;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;


public class RaceSpout implements IRichSpout,MessageListenerConcurrently{
	
	private static final long serialVersionUID = 1L;
	
	private SpoutOutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		initConsumer();
	}
	
	/** initiate Consumer */
	private void initConsumer() {
		try {
			
			DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);

			// where pull begin
			// consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

			// need to use when debug local
			// String nameSrvAddr = "127.0.0.1:9876";
			// consumer.setNamesrvAddr(Global.nameSrvAddr);

			// topic
			consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqPayTopic, "*");

			// the number of message pull every time
			int consumeMessageBatchMaxSize = 32; // default
			consumer.setConsumeMessageBatchMaxSize(consumeMessageBatchMaxSize);

			consumer.registerMessageListener(this);
			consumer.start();
			// LOG.info("Consumer in spout Started.");

		} catch (MQClientException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	

	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,ConsumeConcurrentlyContext context) {
		MessageTuple messageTuple = new MessageTuple();
		for (MessageExt msg : msgs) {
			messageTuple.addMessages(new Message(msg.getTopic(), msg.getBody()));
		}
		collector.emit(new Values(messageTuple));
		return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
	}
	
	
	
	@Override
	public void nextTuple() {}
	
	@Override
	public void fail(Object msgId) {}	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("messageTuple"));
	}
	
	@Override
	public void ack(Object object) {}

	@Override
    public void close() {}

    @Override
    public void activate() {}

    @Override
    public void deactivate() {}

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
