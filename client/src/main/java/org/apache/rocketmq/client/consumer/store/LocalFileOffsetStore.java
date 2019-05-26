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
package org.apache.rocketmq.client.consumer.store;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local storage implementation
 * 读取本地的偏移量，消费队列的偏移信息 存放在本地的配置文件中
 */
public class LocalFileOffsetStore implements OffsetStore {
    //看能否读取到客户端本地缓存，不存在的话 就会新建一个 user.home/.rocketmq_offsets,是个隐藏的文件
    public final static String LOCAL_OFFSET_STORE_DIR = System.getProperty(
            "rocketmq.client.localOffsetStoreDir",
            System.getProperty("user.home") + File.separator + ".rocketmq_offsets");
    private final static Logger log = ClientLogger.getLog();
    //客户端实例
    private final MQClientInstance mQClientFactory;
    //区分不同的消费者组
    private final String groupName;
    //存储的路径
    private final String storePath;
    /**
     * 消费进度
     */
    private ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<>();

    public LocalFileOffsetStore(MQClientInstance mQClientFactory, String groupName) {
        this.mQClientFactory = mQClientFactory;
        this.groupName = groupName;
        //把本地的消费进度 以group为纬度 进行区分 封装进了offsets.json
        this.storePath = LOCAL_OFFSET_STORE_DIR + File.separator + //
                this.mQClientFactory.getClientId() + File.separator + //
                this.groupName + File.separator + //
                "offsets.json";
    }

    /**
     * 从本地配置的偏移文件中去加载读取进度
     * @throws MQClientException
     */
    @Override
    public void load() throws MQClientException {
        // 从本地硬盘读取消费进度
        OffsetSerializeWrapper offsetSerializeWrapper = this.readLocalOffset();
        if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
            offsetTable.putAll(offsetSerializeWrapper.getOffsetTable());

            // 打印每个消费队列的消费进度
            for (MessageQueue mq : offsetSerializeWrapper.getOffsetTable().keySet()) {
                AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                log.info("load consumer's offset, {} {} {}",
                        this.groupName,
                        mq,
                        offset.get());
            }
        }
    }

    /**
     * 更新一个队列的消费偏移
     * @param mq
     * @param offset
     * @param increaseOnly 是否是递增增加
     */
    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        if (mq != null) {
            AtomicLong offsetOld = this.offsetTable.get(mq);
            //不存在应该是 还没被消费过
            if (null == offsetOld) {
                offsetOld = this.offsetTable.putIfAbsent(mq, new AtomicLong(offset));
            }

            if (null != offsetOld) {
                //如果是仅仅递增的方式，则offset小于当前就不会更新偏移量
                if (increaseOnly) {
                    MixAll.compareAndIncreaseOnly(offsetOld, offset);
                } else {
                    //否则就直接设置即可
                    offsetOld.set(offset);
                }
            }
        }
    }

    @Override
    public long readOffset(final MessageQueue mq, final ReadOffsetType type) {
        if (mq != null) {
            //这个swich做的不错，不存在就返回-1
            switch (type) {
                case MEMORY_FIRST_THEN_STORE:
                case READ_FROM_MEMORY: {
                    AtomicLong offset = this.offsetTable.get(mq);
                    if (offset != null) {
                        return offset.get();
                    } else if (ReadOffsetType.READ_FROM_MEMORY == type) { //这里设计的好
                        return -1;
                    }
                }
                case READ_FROM_STORE: {
                    OffsetSerializeWrapper offsetSerializeWrapper;
                    try {
                        offsetSerializeWrapper = this.readLocalOffset();
                    } catch (MQClientException e) {
                        return -1;
                    }
                    //从本地配置中查找
                    if (offsetSerializeWrapper != null && offsetSerializeWrapper.getOffsetTable() != null) {
                        AtomicLong offset = offsetSerializeWrapper.getOffsetTable().get(mq);
                        if (offset != null) {
                            //在更新到缓存中
                            this.updateOffset(mq, offset.get(), false);
                            return offset.get();
                        }
                    }
                }
                default:
                    break;
            }
        }

        return -1;
    }

    /**
     * 持久化所有的配置文件
     * @param mqs
     */
    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (null == mqs || mqs.isEmpty())
            return;

        OffsetSerializeWrapper offsetSerializeWrapper = new OffsetSerializeWrapper();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            if (mqs.contains(entry.getKey())) {
                AtomicLong offset = entry.getValue();
                offsetSerializeWrapper.getOffsetTable().put(entry.getKey(), offset);
            }
        }
        //把参数中所有的消息队列的偏移量持久化到磁盘中
        String jsonString = offsetSerializeWrapper.toJson(true);
        if (jsonString != null) {
            try {
                MixAll.string2File(jsonString, this.storePath);
            } catch (IOException e) {
                log.error("persistAll consumer offset Exception, " + this.storePath, e);
            }
        }
    }

    @Override
    public void persist(MessageQueue mq) {
    }

    @Override
    public void removeOffset(MessageQueue mq) {

    }

    @Override
    public void updateConsumeOffsetToBroker(final MessageQueue mq, final long offset, final boolean isOneway)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {

    }

    //浅克隆，仅仅克隆引用
    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        Map<MessageQueue, Long> cloneOffsetTable = new HashMap<MessageQueue, Long>();
        for (Map.Entry<MessageQueue, AtomicLong> entry : this.offsetTable.entrySet()) {
            MessageQueue mq = entry.getKey();
            if (!UtilAll.isBlank(topic) && !topic.equals(mq.getTopic())) {
                continue;
            }
            cloneOffsetTable.put(mq, entry.getValue().get());

        }
        return cloneOffsetTable;
    }

    //读取本地配置文件
    private OffsetSerializeWrapper readLocalOffset() throws MQClientException {
        //读取本地文件中的内容
        String content = MixAll.file2String(this.storePath);
        //如果没有读取到对应的配置文件
        if (null == content || content.length() == 0) {
            //尝试读取.bak文件 即备份文件
            return this.readLocalOffsetBak();
        } else {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                        OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception, and try to correct", e);
                return this.readLocalOffsetBak();
            }

            return offsetSerializeWrapper;
        }
    }
    //读取本地配置文件的备份文件
    private OffsetSerializeWrapper readLocalOffsetBak() throws MQClientException {
        String content = MixAll.file2String(this.storePath + ".bak");
        if (content != null && content.length() > 0) {
            OffsetSerializeWrapper offsetSerializeWrapper = null;
            try {
                offsetSerializeWrapper =
                        OffsetSerializeWrapper.fromJson(content, OffsetSerializeWrapper.class);
            } catch (Exception e) {
                log.warn("readLocalOffset Exception", e);
                throw new MQClientException("readLocalOffset Exception, maybe fastjson version too low" //
                        + FAQUrl.suggestTodo(FAQUrl.LOAD_JSON_EXCEPTION), //
                        e);
            }
            return offsetSerializeWrapper;
        }

        return null;
    }
}
