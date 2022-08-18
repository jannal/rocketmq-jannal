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

import java.util.concurrent.atomic.AtomicLong;

/**
 * ByteBuffer通常使用slice方法，即两个ByteBuffer对象内存地址相同，但是维护不同的指针
 * 这样做的好处：避免内存复制
 * 这样做的坏处：ByteBufer释放变得复杂。需要跟踪ByteBuffer调用slice的次数，如果创建的对象生命周期没有结束，则不能随便释放
 * 否则导致内存访问错误。
 * RocketMQ 通过每次slice后引用计数加一，释放后引用计数减一，只有当前的引用计数为0，才可以真正释放
 *
 */
public abstract class ReferenceResource {
    protected final AtomicLong refCount = new AtomicLong(1);
    //是否可用
    protected volatile boolean available = true;
    //是否清理完毕
    protected volatile boolean cleanupOver = false;
    //第一次执行shutdown的时间
    private volatile long firstShutdownTimestamp = 0;

    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            // 标记不可用
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            // 如果引用大于0，则不不会释放
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {
            //如果引用计数小于或者等于0，则执行清理堆外内存
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
