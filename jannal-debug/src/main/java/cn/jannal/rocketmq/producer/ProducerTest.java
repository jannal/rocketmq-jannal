package cn.jannal.rocketmq.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @author jannal
 **/
public class ProducerTest {
    private Logger logger = LoggerFactory.getLogger("rocketmq-producer");


    @Test
    public void test() throws UnsupportedEncodingException {
        System.out.println("hello world 0".getBytes(RemotingHelper.DEFAULT_CHARSET).length);
    }

    /**
     * 同步发送
     */
    @Test
    public void testSync() {
        String namesrvAddr = "rocketmq-nameserver1:9876;rocketmq-nameserver2:9876";
        String producerGroup = "ProducerGroupName";
        final DefaultMQProducer defaultMQProducer = new DefaultMQProducer(producerGroup);
        try {
            defaultMQProducer.setInstanceName("producer");
            //发送超时时间,也可以在send方法设置不同消息的超时时间
            defaultMQProducer.setSendMsgTimeout(20000);
            defaultMQProducer.setVipChannelEnabled(false);
            defaultMQProducer.setNamesrvAddr(namesrvAddr);
            //发送失败重试3次，默认2次
            defaultMQProducer.setRetryTimesWhenSendFailed(3);
            //Producer对象在使用之前必须要调用start初始化，初始化一次即可
            defaultMQProducer.start();

            String topic = "testTopic";
            String tag = "TagA";
            String keys = "keys";
            for (int i = 0; i < 1; i++) {
                String msg = "hello world 0";
                System.out.println(msg.getBytes(RemotingHelper.DEFAULT_CHARSET).length);
                Message message = new Message(topic, tag, keys, msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = defaultMQProducer.send(message);
                logger.info("第{}条消息:返回状态{}", i, sendResult.getSendStatus());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            defaultMQProducer.shutdown();
        }
    }

    /**
     * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从RocketMQ服务器上注销自己
     * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
     */
                /*Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    public void run() {
                        defaultMQProducer.shutdown();
                    }
                }));
                System.exit(0);*/


    /**
     * 异步发送
     */
    @Test
    public void testAsync() {
        String namesrvAddr = "rocketmq-nameserver1:9876;rocketmq-nameserver2:9877";
        String producerGroup = "ProducerGroupName";
        final DefaultMQProducer defaultMQProducer = new DefaultMQProducer(producerGroup);
        try {
            defaultMQProducer.setSendMsgTimeout(20000);
            defaultMQProducer.setVipChannelEnabled(false);
            defaultMQProducer.setNamesrvAddr(namesrvAddr);
            defaultMQProducer.setRetryTimesWhenSendFailed(3);
            defaultMQProducer.start();

            String topic = "testTopic";
            String tag = "TagA";
            String keys = "keys";
            final CountDownLatch countDownLatch = new CountDownLatch(100);
            for (int i = 100; i < 200; i++) {
                String msg = "hello world " + i;
                Message message = new Message(topic, tag, keys, msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
                final int k = i;
                defaultMQProducer.send(message, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        countDownLatch.countDown();
                        logger.info("第{}条消息:返回状态{}", k, sendResult.getSendStatus());
                    }

                    @Override
                    public void onException(Throwable e) {
                        countDownLatch.countDown();
                        logger.error(e.getMessage(), e);
                    }
                });
            }
            countDownLatch.await();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            defaultMQProducer.shutdown();
        }
    }

    /**
     * 延迟消息
     */
    @Test
    public void testDelayMessage() {
        String namesrvAddr = "rocketmq-nameserver1:9876;rocketmq-nameserver2:9877";
        String producerGroup = "ProducerGroupName";
        final DefaultMQProducer defaultMQProducer = new DefaultMQProducer(producerGroup);
        try {
            defaultMQProducer.setInstanceName("producer");
            //发送超时时间,也可以在send方法设置不同消息的超时时间
            defaultMQProducer.setSendMsgTimeout(20000);
            defaultMQProducer.setVipChannelEnabled(false);
            defaultMQProducer.setNamesrvAddr(namesrvAddr);
            //发送失败重试3次，默认2次
            defaultMQProducer.setRetryTimesWhenSendFailed(3);
            //Producer对象在使用之前必须要调用start初始化，初始化一次即可
            defaultMQProducer.start();

            String topic = "testTopic";
            String tag = "TagA";
            String keys = "keys";
            for (int i = 0; i < 100; i++) {
                String msg = "hello world " + i;
                Message message = new Message(topic, tag, keys, msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
                //messageDelayLevel=1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
                //这里就表示10s
                message.setDelayTimeLevel(3);
                SendResult sendResult = defaultMQProducer.send(message);
                logger.info("第{}条消息:返回状态{}", i, sendResult.getSendStatus());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            defaultMQProducer.shutdown();
        }
    }


    @Test
    public void testSelectorOrder() {
        String namesrvAddr = "rocketmq-nameserver1:9876;rocketmq-nameserver2:9877";
        String producerGroup = "ProducerGroupName";
        final DefaultMQProducer defaultMQProducer = new DefaultMQProducer(producerGroup);
        try {
            defaultMQProducer.setSendMsgTimeout(20000);
            defaultMQProducer.setVipChannelEnabled(false);
            defaultMQProducer.setNamesrvAddr(namesrvAddr);
            defaultMQProducer.setRetryTimesWhenSendFailed(3);
            defaultMQProducer.start();

            String topic = "testTopic";
            String tag = "TagA";
            String keys = "keys";
            for (int i = 200; i < 300; i++) {
                String msg = "hello world " + i;
                Message message = new Message(topic, tag, keys, msg.getBytes(RemotingHelper.DEFAULT_CHARSET));

                SendResult sendResult = defaultMQProducer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        //获取总共的队列数
                        int size = mqs.size();
                        int curIndex = Integer.parseInt(arg.toString());
                        int index = (curIndex / 10) % size;
                        MessageQueue messageQueue = mqs.get(index);
                        //225,index:22,queueId:6,size:32
                        logger.info("{},index:{},queueId:{},size:{}", curIndex, index, messageQueue.getQueueId(), size);
                        return messageQueue;
                    }
                }, i);
                //logger.info("第{}条消息:返回状态{}", i, sendResult.getSendStatus());
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            defaultMQProducer.shutdown();
        }
    }


    /**
     * 同类型消息有序,发送到一个MessageQueue
     */
    @Test
    public void testMessageOrder() {
        String namesrvAddr = "rocketmq-nameserver1:9876;rocketmq-nameserver2:9877";
        String producerGroup = "ProducerGroupName";
        final DefaultMQProducer defaultMQProducer = new DefaultMQProducer(producerGroup);
        try {
            defaultMQProducer.setSendMsgTimeout(20000);
            defaultMQProducer.setVipChannelEnabled(false);
            defaultMQProducer.setNamesrvAddr(namesrvAddr);
            defaultMQProducer.setRetryTimesWhenSendFailed(3);
            defaultMQProducer.start();

            String topic = "testTopic";
            String tag = "TagA";
            String keys = "keys";
            String msgType = null;
            String msg = null;
            for (int i = 200; i < 300; i++) {
                if (i < 250) {
                    msgType = "hello";
                    msg = "hello " + i;
                } else {
                    msgType = "world";
                    msg = "world " + i;
                }
                Message message = new Message(topic, tag, keys, msg.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = defaultMQProducer.send(message, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                        int size = mqs.size();
                        Integer id = arg.hashCode();
                        int index = id % size;
                        return mqs.get(index);
                    }
                }, msgType);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            defaultMQProducer.shutdown();
        }
    }


}
