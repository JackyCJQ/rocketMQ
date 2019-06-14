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
package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.util.Set;

/**
 * 消费者内部接口实现
 */
public interface MQConsumerInner {
    /**
     * 获取消费者所在组的名字
     *
     * @return
     */
    String groupName();

    /**
     * 消费类型（包括广播消费和集群消费）
     */
    MessageModel messageModel();

    /**
     * 消费方式（pull和push）
     *
     * @return
     */
    ConsumeType consumeType();

    /**
     * 指定从哪个位置消费
     *
     * @return
     */
    ConsumeFromWhere consumeFromWhere();

    /**
     * 订阅的消息
     */
    Set<SubscriptionData> subscriptions();

    /**
     * 负载均衡
     */
    void doRebalance();

    /**
     * 持久化消费者偏移
     */
    void persistConsumerOffset();

    /**
     * 更新主题订阅消息
     */
    void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

    /**
     * 判断订阅的主题是否更新了
     */
    boolean isSubscribeTopicNeedUpdate(final String topic);

    boolean isUnitMode();

    ConsumerRunningInfo consumerRunningInfo();
}
