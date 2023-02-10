package cn.jannal.rocketmq.consumer;


import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.LockSupport;


public class ConsumerRetryTest {
    private final static Logger logger = LoggerFactory.getLogger("rocketmq-consumer");


    @Test
    public void testPushConsumer() {
        String namesrvAddr = "rocketmq-nameserver1:9876;rocketmq-nameserver2:9876";
        String consumerGroup = "ConsumerGroupName3";

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        try {

            //从上一次消费位置开始消费
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setInstanceName("Consumber");

            //设置集群模式
            consumer.setMessageModel(MessageModel.CLUSTERING);

            //批量接收20条
            consumer.setConsumeMessageBatchMaxSize(20);
            //默认64
            consumer.setConsumeThreadMax(4);
            //默认20
            consumer.setConsumeThreadMin(1);

            String topic = "testTopic";
            String tag = "TagA";
            consumer.subscribe(topic, tag);

            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                    for (MessageExt messageExt : msgs) {
                        try {

                            String topic = messageExt.getTopic();
                            String msg = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            String tags = messageExt.getTags();
                            logger.info("threadName:{},topic:{},tag:{},msg:{}", Thread.currentThread().getName(), topic, tags, msg);
                            int i = 1 / 0;
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();
            LockSupport.park();
        } catch (MQClientException e) {
            logger.error(e.getMessage(), e);
        } finally {
            consumer.shutdown();
        }

    }

}
