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

package org.apache.rocketmq.remoting.netty;

public class NettySystemConfig {
    //
    public static final String COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE =
        "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    //发送大小
    public static final String COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE =
        "com.rocketmq.remoting.socket.sndbuf.size";
    //接收大小
    public static final String COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE =
        "com.rocketmq.remoting.socket.rcvbuf.size";
    //客户端异步信号
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE =
        "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    //客户端直接发送信号
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE =
        "com.rocketmq.remoting.clientOnewaySemaphoreValue";
    //默认为false
    public static final boolean NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE =
        Boolean.parseBoolean(System.getProperty(COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE, "false"));
    //客户端异步信号，默认是65535
    public static final int CLIENT_ASYNC_SEMAPHORE_VALUE =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE, "65535"));
    //客户端直接发送信号 默认也是65535
    public static final int CLIENT_ONEWAY_SEMAPHORE_VALUE =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE, "65535"));
    //发送大小 默认65535
    public static int socketSndbufSize =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE, "65535"));
    //接收大小 默认65535
    public static int socketRcvbufSize =
        Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE, "65535"));
}
