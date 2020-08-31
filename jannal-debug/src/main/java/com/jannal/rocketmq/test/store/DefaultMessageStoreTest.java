package com.jannal.rocketmq.test.store;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;
import org.junit.After;
import org.junit.Before;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author jannal
 * @create 2019-05-01
 **/
public class DefaultMessageStoreTest {
    private int QUEUE_TOTAL = 100;
    private AtomicInteger QueueId = new AtomicInteger(0);
    private SocketAddress BornHost;
    private SocketAddress StoreHost;
    private byte[] MessageBody;
    private MessageStore messageStore;

    @Before
    public void init() throws Exception {
        StoreHost = new InetSocketAddress(InetAddress.getLocalHost(), 8123);
        BornHost = new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0);

        messageStore = buildMessageStore();
        boolean load = messageStore.load();
        assertTrue(load);
        messageStore.start();
    }


    @After
    public void destory() {
        messageStore.shutdown();
        messageStore.destroy();

        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        File file = new File(messageStoreConfig.getStorePathRootDir());
        UtilAll.deleteFile(file);
    }

    private MessageStore buildMessageStore() throws Exception {
        MessageStoreConfig messageStoreConfig = new MessageStoreConfig();
        messageStoreConfig.setMapedFileSizeCommitLog(1024 * 1024 * 1);
        messageStoreConfig.setMapedFileSizeConsumeQueue(1024 * 1024 * 1);
        messageStoreConfig.setMaxHashSlotNum(10);
        messageStoreConfig.setMaxIndexNum(100 * 100);
        messageStoreConfig.setFlushDiskType(FlushDiskType.SYNC_FLUSH);
        messageStoreConfig.setFlushIntervalConsumeQueue(1);
        return new DefaultMessageStore(messageStoreConfig, new BrokerStatsManager("simpleTest"), new MessageArrivingListener(){
            @Override
            public void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties) {

            }
        }, new BrokerConfig());
    }




}
