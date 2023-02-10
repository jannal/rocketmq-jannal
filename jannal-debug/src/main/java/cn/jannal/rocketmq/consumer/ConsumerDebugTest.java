package cn.jannal.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumerScheduleService;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullTaskCallback;
import org.apache.rocketmq.client.consumer.PullTaskContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * @author jannal
 **/
public class ConsumerDebugTest {
    private org.slf4j.Logger logger = LoggerFactory.getLogger("rocketmq-consumer");

    /**
     * push(推模式)
     */
    @Test
    public void testPushConsumer() {
        String namesrvAddr = "rocketmq-nameserver1:9876";
        String consumerGroup = "ConsumerGroupName4";

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


    @Test
    public void testPullConsumer() {
        String namesrvAddr = "rocketmq-nameserver1:9876;rocketmq-nameserver2:9877";
        final MQPullConsumerScheduleService scheduleService = new MQPullConsumerScheduleService("ConsumerGroupName11");
        DefaultMQPullConsumer defaultMQPullConsumer = scheduleService.getDefaultMQPullConsumer();
        try {

            defaultMQPullConsumer.setNamesrvAddr(namesrvAddr);
            scheduleService.setMessageModel(MessageModel.CLUSTERING);
            String topic = "testTopic";

            scheduleService.registerPullTaskCallback(topic, new PullTaskCallback() {
                @Override
                public void doPullTask(MessageQueue mq, PullTaskContext context) {
                    MQPullConsumer consumer = context.getPullConsumer();
                    logger.info("brokerName:{},topic:{},queueId:{}", mq.getBrokerName(), mq.getTopic(), mq.getQueueId());
                    try {
                        //获取消费偏移量,其中fromStore为true表示从存储端（即Broker端）获取消费进度；
                        // 若fromStore为false表示从本地内存获取消费进度
                        long offset = consumer.fetchConsumeOffset(mq, true);
                        if (offset < 0)
                            offset = 0;

                        PullResult pullResult = consumer.pull(mq, "TagA", offset, 32);
                        switch (pullResult.getPullStatus()) {
                            case FOUND:
                                List<MessageExt> list = pullResult.getMsgFoundList();
                                for (MessageExt messageExt : list) {
                                    logger.info("msg:{}", new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET));
                                }
                                break;
                            case NO_MATCHED_MSG:
                                logger.info("没有匹配的消息");
                                break;
                            case NO_NEW_MSG:
                                logger.info("没有新消息");
                                break;
                            case OFFSET_ILLEGAL:
                                logger.info("偏移量非法");
                                break;
                            default:
                                break;
                        }

                        // 存储Offset，客户端每隔5s会定时刷新到Broker
                        consumer.updateConsumeOffset(mq, pullResult.getNextBeginOffset());

                        // 设置再过1000ms后重新拉取
                        context.setPullNextDelayTimeMillis(1000);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            });
            scheduleService.start();
            LockSupport.park();
        } catch (MQClientException e) {
            logger.error(e.getMessage(), e);
        } finally {
            scheduleService.shutdown();
        }

    }

    @Test
    public void testMsgOrder() {
        String namesrvAddr = "rocketmq-nameserver1:9876;rocketmq-nameserver2:9877";
        String consumerGroup = "ConsumerGroupNameOrder";

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);
        try {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setInstanceName("ConsumberOrder");
            consumer.setMessageModel(MessageModel.CLUSTERING);

            consumer.setConsumeMessageBatchMaxSize(20);
            consumer.setConsumeThreadMax(4);
            consumer.setConsumeThreadMin(1);

            consumer.subscribe("testTopic", "TagA");
            consumer.registerMessageListener(new MessageListenerOrderly() {

                @Override
                public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext context) {
                    //设置自动提交,默认true
                    context.setAutoCommit(true);

                    for (MessageExt messageExt : list) {
                        String msgStr = null;
                        try {
                            msgStr = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
                            logger.info("threadName:{},topic:{},tag:{},msg:{}", Thread.currentThread().getName(), messageExt.getTopic(), messageExt.getTags(), msgStr);
                        } catch (Exception e) {
                            logger.error(e.getMessage(), e);
                            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }

                    }

                    return ConsumeOrderlyStatus.SUCCESS;
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




