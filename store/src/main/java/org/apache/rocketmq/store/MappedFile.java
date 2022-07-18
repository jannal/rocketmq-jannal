/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * commitLog consumerQueue、index三类文件磁盘的读写都是通过MappedFile
 */
public class MappedFile extends ReferenceResource {
    //内存页大小，linux下通过getconf PAGE_SIZE获取，一般默认是4k
    public static final int OS_PAGE_SIZE = 1024 * 4;
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //所有MappedFile实例已使用字节总数
    private static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    //MappedFile个数
    private static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);
    //MappedFile 当前文件所映射到的消息写入pagecache的位置
    protected final AtomicInteger wrotePosition = new AtomicInteger(0);
    //ADD BY ChenYang 已经提交(持久化)的位置
    protected final AtomicInteger committedPosition = new AtomicInteger(0);
    //flushedPosition来维持刷盘的最新位置
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    /**
     *映射文件的大小，参照{@link org.apache.rocketmq.store.config.MessageStoreConfig#mapedFileSizeCommitLog}，默认1G
     */
    protected int fileSize;
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 堆外内存ByteBuffer，如果不为空（transientStorePoolEnable=true），数据受限将存储在buffer中，然后提交到FileChannel
     */
    protected ByteBuffer writeBuffer = null;
    //堆外内存池，内存池中的内存会提供内存锁机制
    protected TransientStorePool transientStorePool = null;
    private String fileName;
    //映射的起始偏移量，也是文件名
    private long fileFromOffset;
    //磁盘的物理文件
    private File file;
    private MappedByteBuffer mappedByteBuffer;
    //文件最后一次写入的时间戳
    private volatile long storeTimestamp = 0;
    //是否是MappedFileQueue中的第一个文件
    private boolean firstCreateInQueue = false;

    public MappedFile() {
    }

    /**
     * 如果设置transientStorePoolEnable为false则调用此方法，参见
     * {@link org.apache.rocketmq.store.AllocateMappedFileService#mmapOperation()}
     */
    public MappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }
    /**
     * 如果设置transientStorePoolEnable为true则调用此方法，参见
     * org.apache.rocketmq.store.config.MessageStoreConfig#isTransientStorePoolEnable()
     * org.apache.rocketmq.store.AllocateMappedFileService#mmapOperation()
     */
    public MappedFile(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            File f = new File(dirName);
            if (!f.exists()) {
                boolean result = f.mkdirs();
                log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
            }
        }
    }

    /**
     * 通过反射清理MappedByteBuffer
     * @param buffer
     */
    public static void clean(final ByteBuffer buffer) {
        if (buffer == null || !buffer.isDirect() || buffer.capacity() == 0)
            return;
        /**
         * 嵌套递归获取directByteBuffer的最内部的attachment或者viewedBuffer方法
         * 获取directByteBuffer的Cleaner对象，然后调用cleaner.clean方法，进行释放资源
         *
         */
        invoke(invoke(viewed(buffer), "cleaner"), "clean");
    }

    private static Object invoke(final Object target, final String methodName, final Class<?>... args) {
        return AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                try {
                    Method method = method(target, methodName, args);
                    method.setAccessible(true);
                    return method.invoke(target);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        });
    }

    private static Method method(Object target, String methodName, Class<?>[] args)
        throws NoSuchMethodException {
        try {
            return target.getClass().getMethod(methodName, args);
        } catch (NoSuchMethodException e) {
            return target.getClass().getDeclaredMethod(methodName, args);
        }
    }

    private static ByteBuffer viewed(ByteBuffer buffer) {
        String methodName = "viewedBuffer";

        Method[] methods = buffer.getClass().getMethods();
        for (int i = 0; i < methods.length; i++) {
            if (methods[i].getName().equals("attachment")) {
                methodName = "attachment";
                break;
            }
        }

        ByteBuffer viewedBuffer = (ByteBuffer) invoke(buffer, methodName);
        if (viewedBuffer == null)
            return buffer;
        else
            return viewed(viewedBuffer);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    public void init(final String fileName, final int fileSize,
        final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize);
        //如果transientStorePoolEnable为true，则初始化MappedFile的
        //writeBuffer，该buffer从transientStorePool中获取
        this.writeBuffer = transientStorePool.borrowBuffer();
        this.transientStorePool = transientStorePool;
    }

    private void init(final String fileName, final int fileSize) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.file = new File(fileName);
        //通过文件名获取起始偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;
        //确保父目录存在
        ensureDirOK(this.file.getParent());

        try {
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("create file channel " + this.fileName + " Failed. ", e);
            throw e;
        } catch (IOException e) {
            log.error("map file " + this.fileName + " Failed. ", e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public int getFileSize() {
        return fileSize;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }

    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb) {
        return appendMessagesInner(msg, cb);
    }

    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb) {
        return appendMessagesInner(messageExtBatch, cb);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb) {
        assert messageExt != null;
        assert cb != null;
        //获取当前写位置
        int currentPos = this.wrotePosition.get();

        if (currentPos < this.fileSize) {
            /**
             * RocketMQ提供两种数据落盘的方式:
             * 1. 直接将数据写到mappedByteBuffer, 然后flush;
             * 2. 先写到writeBuffer, 再从writeBuffer提交到fileChannel, 最后flush.
             */
            ByteBuffer byteBuffer = writeBuffer != null ? writeBuffer.slice() : this.mappedByteBuffer.slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = null;
            //执行append消息的过程
            if (messageExt instanceof MessageExtBrokerInner) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBrokerInner) messageExt);
            } else if (messageExt instanceof MessageExtBatch) {
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos, (MessageExtBatch) messageExt);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            this.wrotePosition.addAndGet(result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    public boolean appendMessage(final byte[] data) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + data.length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(data.length);
            return true;
        }

        return false;
    }

    /**
     * Content of data from offset to offset + length will be wrote to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = this.wrotePosition.get();

        if ((currentPos + length) <= this.fileSize) {
            try {
                this.fileChannel.position(currentPos);
                this.fileChannel.write(ByteBuffer.wrap(data, offset, length));
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            this.wrotePosition.addAndGet(length);
            return true;
        }

        return false;
    }

    /**
     * @return The current flushed position
     */
    public int flush(final int flushLeastPages) {
        if (this.isAbleToFlush(flushLeastPages)) {
            if (this.hold()) {
                //有效数据的最大位置
                int value = getReadPosition();

                try {
                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        //false表示只需要将文件内容的更新写入存储;true表示必须写入文件内容和元数据更改
                        this.fileChannel.force(false);
                    } else {
                        this.mappedByteBuffer.force();
                    }
                } catch (Throwable e) {
                    /**
                     * FIXME jannal
                     * fileChannel.force会抛出IOException，可能会丢失一部分数据
                     * 如果抛异常不去设置flushedPosition，等到下次flush，岂不是更好???
                     */
                    log.error("Error occurred when force data to disk.", e);
                }

                this.flushedPosition.set(value);
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + this.flushedPosition.get());
                this.flushedPosition.set(getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     * commitLeastPages 为本次提交最小的页面，默认4页(4*4KB),可参见
     * org.apache.rocketmq.store.CommitLog.CommitRealTimeService#run()
     */
    public int commit(final int commitLeastPages) {
        /**
         * 1.writeBuffer 为空就不提交，而writeBuffer只有开启
         * transientStorePoolEnable为true并且是异步刷盘模式才会不为空
         * 所以commit是针对异步刷盘使用的
         */
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return this.wrotePosition.get();
        }
        if (this.isAbleToCommit(commitLeastPages)) {
            if (this.hold()) {
                commit0(commitLeastPages);
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + this.committedPosition.get());
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == this.committedPosition.get()) {
            //清理工作，归还到堆外内存池中，并且释放当前writeBuffer
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }

        return this.committedPosition.get();
    }

    protected void commit0(final int commitLeastPages) {
        int writePos = this.wrotePosition.get();
        int lastCommittedPosition = this.committedPosition.get();

        if (writePos - this.committedPosition.get() > 0) {
            try {
                //创建writeBuffer的共享缓存区，slice共享内存，其实就是切片
                //但是position、mark、limit单独维护
                //新缓冲区的position=0，其capacity和limit将是缓冲区中剩余的字节数，其mark=undefined
                ByteBuffer byteBuffer = writeBuffer.slice();
                //上一次的提交指针作为position
                byteBuffer.position(lastCommittedPosition);
                //当前最大的写指针作为limit
                byteBuffer.limit(writePos);
                //把commitedPosition到wrotePosition的写入FileChannel中
                this.fileChannel.position(lastCommittedPosition);
                this.fileChannel.write(byteBuffer);
                //更新提交指针
                this.committedPosition.set(writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    /**
     * 是否能够flush
     *  1. 文件已经写满
     *  2. flushLeastPages > 0 && 未flush部分超过flushLeastPages
     *  3. flushLeastPages==0&&有新写入的部分
     * @param flushLeastPages flush最小分页
     *      mmap映射后的内存一般是内存页大小的倍数，而内存页大小一般为4K，所以写入到映射内存的数据大小可以以4K进行分页，
     *      而flushLeastPages这个参数只是指示写了多少页后才可以强制将映射内存区域的数据强行写入到磁盘文件
     * @return
     */
    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = this.flushedPosition.get();
        int write = getReadPosition();
        //写满了
        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            //总共写入的页大小-已经提交的页大小>=最少一次写入的页大小，OS_PAGE_SIZE默认4kb
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int flush = this.committedPosition.get();
        int write = this.wrotePosition.get();

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            //总共写入的页大小-已经提交的页大小>=最少一次写入的页大小，OS_PAGE_SIZE默认4kb
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > flush;
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    public void setFlushedPosition(int pos) {
        this.flushedPosition.set(pos);
    }

    public boolean isFull() {
        return this.fileSize == this.wrotePosition.get();
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                    + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                int size = readPosition - pos;
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    @Override
    public boolean cleanup(final long currentRef) {
        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                + " have cleanup, do not do it again.");
            return true;
        }

        clean(this.mappedByteBuffer);
        //加一个fileSize大小的负数值
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    public boolean destroy(final long intervalForcibly) {
        this.shutdown(intervalForcibly);

        if (this.isCleanupOver()) {
            try {
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                    + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                    + this.getFlushedPosition() + ", "
                    + UtilAll.computeEclipseTimeMilliseconds(beginTime));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public void setWrotePosition(int pos) {
        this.wrotePosition.set(pos);
    }

    /**
     * @return The max position which have valid data
     */
    public int getReadPosition() {
        // 如果writeBuffer不为空，则应为上一次commit的指针，如果不为空，则为写入位置
        return this.writeBuffer == null ? this.wrotePosition.get() : this.committedPosition.get();
    }

    public void setCommittedPosition(int pos) {
        this.committedPosition.set(pos);
    }

    /**
     * 1. 对当前映射文件进行预热
     *   1.1. 先对当前映射文件的每个内存页写入一个字节0.当刷盘策略为同步刷盘时，执行强制刷盘，并且是每修改pages个分页刷一次盘
     *        再将当前MappedFile全部的地址空间锁定，防止被swap
     *   1.2. 然后将当前MappedFile全部的地址空间锁定在物理存储中，防止其被交换到swap空间。再调用madvise，传入 WILL_NEED 策略，将刚刚锁住的内存预热，其实就是告诉内核，
     *        我马上就要用（WILL_NEED）这块内存，先做虚拟内存到物理内存的映射，防止正式使用时产生缺页中断。
     *  2. 只要启用缓存预热，都会通过mappedByteBuffer来写入假值(字节0)，并且都会对mappedByteBuffer执行mlock和madvise。
     * @param type 刷盘策略
     * @param pages 预热时一次刷盘的分页数
     */
    public void warmMappedFile(FlushDiskType type, int pages) {
        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        //上一次刷盘的位置
        int flush = 0;
        long time = System.currentTimeMillis();
        for (int i = 0, j = 0; i < this.fileSize; i += MappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put(i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                /**
                 *  同步刷盘，每修改pages个分页强制刷一次盘，默认16MB
                 * 参见org.apache.rocketmq.store.config.MessageStoreConfig#flushLeastPagesWhenWarmMapedFile
                 */
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    //FIXME 刷入修改的内容，不会有性能问题？？
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            if (j % 1000 == 0) {
                log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
                time = System.currentTimeMillis();
                try {
                    /***
                     * Thread.yield与Thread.sleep(0);相同，jvm底层使用的就是os::yield();
                     * https://www.jianshu.com/p/0964124ae822
                     * openJdk源码thread.c jvm.cpp
                     */
                    Thread.sleep(0);
                } catch (InterruptedException e) {
                    log.error("Interrupted", e);
                }
            }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
            System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    public String getFileName() {
        return fileName;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public ByteBuffer sliceByteBuffer() {
        return this.mappedByteBuffer.slice();
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    /**
     * 在读取CommitLog时，虽然可以通过PageCache提高目标消息直接在物理内存中读取的命中率。但是由于CommitLog存放的是所有Topic的消息，在读取时是随机访问，所以仍会出现缺页中断问题，导致内存被频繁换入换出。为此，RocketMQ使用了mlock系统调用，
     * 将mmap调用后所占用的堆外内存锁定，变为常驻内存，进一步让目标消息更多的在内存中读取。
     *
     *  这个方法是一个Native级别的调用，调用了标准C库的方法
     *  mlock方法在标准C中的实现是将锁住指定的内存区域避免被操作系统调到swap空间中，
     *  而madvise方法则要配合着mmap来说了，一般来说通过mmap建立起的内存文件在刚开始并没有将文件内容映射进来，
     *  而是只建立一个映射关系，而当你读相对应区域的时候，它第一次还是会去读磁盘
     *  读写基本上都只是和Page Cache打交道，那么当读相对应页没有拿到数据的时候，系统将会产生一个缺页异常，
     *  然后去读磁盘中的内容，最后写回Page Cache然后再次读取Page Cache然后返回，
     *  而madvise的作用是一次性先将一段数据读入到映射内存区域，这样就减少了缺页异常的产生，
     *  不过mlock和madvise在windows下的C库可没有
     *
     *
     *  调用mmap()时内核只是建立了逻辑地址到物理地址的映射表，并没有映射任何数据到内存。
     *  在你要访问数据时内核会检查数据所在分页是否在内存，如果不在，则发出一次缺页中断，
     *  linux默认分页为4K，可以想象读一个1G的消息存储文件要发生多少次中断。
     *
     *  解决办法：将madvise()和mmap()搭配起来使用，在使用数据前告诉内核这一段数据需要使用，将其一次读入内存。
     *  madvise()这个函数可以对映射的内存提出使用建议，从而减少在程序运行时的硬盘缺页中断。。
     */
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            // 内存锁定
            // 通过mlock可以将进程使用的部分或者全部的地址空间锁定在物理内存中，防止其被交换到swap空间。
            // 对时间敏感的应用会希望全部使用物理内存，提高数据访问和操作的效率。
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            //文件预读
            //madvise 一次性先将一段数据读入到映射内存区域，这样就减少了缺页异常的产生。
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    //testable
    File getFile() {
        return this.file;
    }

    @Override
    public String toString() {
        return this.fileName;
    }
}
