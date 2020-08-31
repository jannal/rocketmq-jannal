package cn.jannal.rocketmq.transaction;

import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author jannal
 **/
public class TransactionProducerTest {

    private Logger logger = LoggerFactory.getLogger("rocketmq-producer");

    @Test
    public void testTransaction() {
        String namesrvAddr = "rocketmq-nameserver1:9876;rocketmq-nameserver2:9877";
        String producerGroup = "ProducerTransactionGroupName";


        TransactionListener transactionListener = new TransactionListenerImpl();
        TransactionMQProducer producer = new TransactionMQProducer(producerGroup);
        producer.setNamesrvAddr(namesrvAddr);
        try {
            ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName("client-transaction-msg-check-thread");
                    return thread;
                }
            });

            producer.setExecutorService(executorService);
            producer.setTransactionListener(transactionListener);
            producer.start();

            String[] tags = new String[]{"TagA", "TagB", "TagC", "TagD", "TagE"};
            for (int i = 0; i < 10; i++) {
                String msgStr = ("Hello world " + i);
                Message msg =
                        new Message("transactionTopic",
                                tags[i % tags.length],
                                "KEY" + i,
                                msgStr.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                logger.info("{}", sendResult);

            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            producer.shutdown();
        }
    }
}
