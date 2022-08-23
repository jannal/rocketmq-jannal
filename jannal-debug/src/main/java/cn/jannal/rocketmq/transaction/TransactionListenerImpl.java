package cn.jannal.rocketmq.transaction;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jannal
 **/
public class TransactionListenerImpl implements TransactionListener {
    private final static Logger logger = LoggerFactory.getLogger("rocketmq-producer");

    private AtomicInteger DB_ID = new AtomicInteger(0);

    private ConcurrentHashMap<Integer, String> localDB = new ConcurrentHashMap<>();

    /**
     * 当发送事务prepare消息成功时，将调用该方法执行本地事务。
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        String messageStr = JSON.parseObject(msg.getBody(), String.class);
        logger.info("事务ID:{},消息数据:{}", msg.getTransactionId(), msg.getBody());
        try {
            //模拟业务逻辑(向数据库添加一条数据)
            localDB.put(DB_ID.incrementAndGet(), messageStr);
            logger.info("事务ID:{},执行业务逻辑:{}", msg.getTransactionId(), messageStr);
        } catch (Exception e) {
            logger.error("业务处理异常，回滚事务");
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        logger.info("消息回查，事务ID:{},消息数据:{}", msg.getTransactionId(), msg.getBody());
        String messageStr = JSON.parseObject(msg.getBody(), String.class);
        int i = Integer.parseInt(messageStr);
        //模拟回查状态
        if (i >= 0 && i < 3) {
            //继续回查，直到回滚或者提交
            return LocalTransactionState.UNKNOW;
        } else if (i >= 3 && i < 6) {
            //回滚
            return LocalTransactionState.ROLLBACK_MESSAGE;
        } else {
            //提交事务
            return LocalTransactionState.COMMIT_MESSAGE;
        }

    }
}


