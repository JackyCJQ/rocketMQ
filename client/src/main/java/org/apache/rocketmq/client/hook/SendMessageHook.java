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
package org.apache.rocketmq.client.hook;

/**
 * 发送消息勾子
 */
public interface SendMessageHook {
    /**
     * 可以起一个名字
     *
     * @return
     */
    String hookName();

    /**
     * 发送消息之前做一些操作
     *
     * @param context
     */
    void sendMessageBefore(final SendMessageContext context);

    /**
     * 发送消息之后做一些操作
     *
     * @param context
     */
    void sendMessageAfter(final SendMessageContext context);
}
