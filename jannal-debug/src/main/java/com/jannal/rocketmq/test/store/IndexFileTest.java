package com.jannal.rocketmq.test.store;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.store.index.IndexFile;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author jannal
 * @create 2019-07-20
 **/
public class IndexFileTest {
    private final int HASH_SLOT_NUM = 100;
    private final int INDEX_NUM = 400;

    @Test
    public void testSelectPhyOffset() throws Exception {
        IndexFile indexFile = new IndexFile("200", HASH_SLOT_NUM, INDEX_NUM, 0, 0);

        indexFile.putKey(Long.toString(0), 0, System.currentTimeMillis());
        indexFile.putKey(Long.toString(0), 1, System.currentTimeMillis());
        indexFile.putKey(Long.toString(1), 2, System.currentTimeMillis());
        indexFile.putKey(Long.toString(0), 3, System.currentTimeMillis());


        final List<Long> phyOffsets = new ArrayList<Long>();
        indexFile.selectPhyOffset(phyOffsets, "0", 10, 0, Long.MAX_VALUE, true);
        indexFile.destroy(0);
        File file = new File("200");
        UtilAll.deleteFile(file);
    }
}
