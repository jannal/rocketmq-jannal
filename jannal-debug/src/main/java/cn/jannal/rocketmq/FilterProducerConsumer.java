package cn.jannal.rocketmq;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * @author jannal
 **/
public class FilterProducerConsumer {

    private Logger producerLogger = LoggerFactory.getLogger("rocketmq-producer");
    private Logger consumerLogger = LoggerFactory.getLogger("rocketmq-consumer");


    @Test
    public void testProducerFilter() {
        String namesrvAddr = "rocketmq-nameserver1:9876;rocketmq-nameserver2:9877";
        String producerGroup = "filterProducerGroupName";
        final DefaultMQProducer defaultMQProducer = new DefaultMQProducer(producerGroup);
        try {
            defaultMQProducer.setSendMsgTimeout(20000);
            defaultMQProducer.setVipChannelEnabled(false);
            defaultMQProducer.setNamesrvAddr(namesrvAddr);
            defaultMQProducer.setRetryTimesWhenSendFailed(3);

            defaultMQProducer.start();
            for (int i = 0; i < 100; i++) {
                String msg = "hello world " + i;
                Message message = new Message("filterTopic", //topic
                        "TagFilter", // tag
                        "keys", //key
                        msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
                message.putUserProperty("num", String.valueOf(i));
                SendResult sendResult = defaultMQProducer.send(message);
                producerLogger.info("第{}条消息:{}发送成功", i, sendResult);
            }

        } catch (MQClientException e) {
            producerLogger.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            producerLogger.error(e.getMessage(), e);
        } catch (RemotingException e) {
            producerLogger.error(e.getMessage(), e);
        } catch (MQBrokerException e) {
            producerLogger.error(e.getMessage(), e);
        } catch (UnsupportedEncodingException e) {
            producerLogger.error(e.getMessage(), e);
        } finally {
            defaultMQProducer.shutdown();
        }
    }

    @Test
    public void testConsumerFilter() {
        String namesrvAddr = "rocketmq-nameserver1:9876;rocketmq-nameserver2:9877";
        String consumerGroup = "FilterConsumerGroupName";
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        try {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setInstanceName("FilterConsumber");
            consumer.setMessageModel(MessageModel.CLUSTERING);

            consumer.setConsumeMessageBatchMaxSize(20);
            consumer.setConsumeThreadMax(4);
            consumer.setConsumeThreadMin(1);
            //有属性num且值范围20~50
            //consumer.subscribe("filterTopic", "*");
            consumer.subscribe("filterTopic", MessageSelector.bySql("num >=20 and num <= 50"));
            consumer.registerMessageListener(new MessageListenerConcurrently() {
                @Override
                public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                    for (MessageExt messageExt : msgs) {
                        try {

                            String topic = messageExt.getTopic();
                            String msg = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            String tags = messageExt.getTags();
                            consumerLogger.info("threadName:{},topic:{},tag:{},msg:{}", Thread.currentThread().getName(), topic, tags, msg);
                        } catch (Exception e) {
                            consumerLogger.error(e.getMessage(), e);
                            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                        }
                    }
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
            });

            consumer.start();


            LockSupport.park();
        } catch (MQClientException e) {
            consumerLogger.error(e.getMessage(), e);
        } finally {
            consumer.shutdown();
        }
    }


}
