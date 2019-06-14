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
package org.apache.rocketmq.namesrv;

import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.namesrv.kvconfig.KVConfigManager;
import org.apache.rocketmq.namesrv.processor.ClusterTestRequestProcessor;
import org.apache.rocketmq.namesrv.processor.DefaultRequestProcessor;
import org.apache.rocketmq.namesrv.routeinfo.BrokerHousekeepingService;
import org.apache.rocketmq.namesrv.routeinfo.RouteInfoManager;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * NamesrvController实例为 NameSerer核心控制器
 */
public class NamesrvController {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    private final NamesrvConfig namesrvConfig;

    private final NettyServerConfig nettyServerConfig;
    //创建了一个线程的线程池 用于定时检查broker是否存活和定期打印配置
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
            "NSScheduledThread"));
    //对于kv配置的管理
    private final KVConfigManager kvConfigManager;
    //对于路由信息的管理
    private final RouteInfoManager routeInfoManager;
    //远程连接管理 核心处理流程
    private RemotingServer remotingServer;
    //
    private BrokerHousekeepingService brokerHousekeepingService;
    //处理远程链接
    private ExecutorService remotingExecutor;
    //配置保存
    private Configuration configuration;

    //从NameserverStartup传递过来的配置
    public NamesrvController(NamesrvConfig namesrvConfig, NettyServerConfig nettyServerConfig) {
        this.namesrvConfig = namesrvConfig;
        this.nettyServerConfig = nettyServerConfig;
        //初始化对于k/v配置的管理
        this.kvConfigManager = new KVConfigManager(this);
        //初始化路由管理
        this.routeInfoManager = new RouteInfoManager();
        //初始化Broker连接管理服务
        this.brokerHousekeepingService = new BrokerHousekeepingService(this);
        //初始化配置管理
        this.configuration = new Configuration(log, this.namesrvConfig, this.nettyServerConfig);
        this.configuration.setStorePathFromConfig(this.namesrvConfig, "configStorePath");
    }

    //开始进行初始化
    public boolean initialize() {
        //加载 KV 配置。根据-c 指定的配置文件的路径，从本地文件中读取配置
        this.kvConfigManager.load();
        //创建 NettyServer 网络处理对象
        this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.brokerHousekeepingService);
        this.remotingExecutor =
                Executors.newFixedThreadPool(nettyServerConfig.getServerWorkerThreads(), new ThreadFactoryImpl("RemotingExecutorThread_"));
        //注册处理器
        this.registerProcessor();
        //NameServer每隔1Os扫描一次Broker,移除处于不激活状态的Broker
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                NamesrvController.this.routeInfoManager.scanNotActiveBroker();
            }
        }, 5, 10, TimeUnit.SECONDS);
        //nameserver每隔10分钟打印一次KV配置。
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                NamesrvController.this.kvConfigManager.printAllPeriodically();
            }
        }, 1, 10, TimeUnit.MINUTES);

        return true;
    }

    /**
     * 注册处理器
     */
    private void registerProcessor() {
        //如果是集群测试
        if (namesrvConfig.isClusterTest()) {

            this.remotingServer.registerDefaultProcessor(new ClusterTestRequestProcessor(this, namesrvConfig.getProductEnvName()),
                    this.remotingExecutor);
        } else {
            //如果是正常,注册处理器和对应处理线程池
            this.remotingServer.registerDefaultProcessor(new DefaultRequestProcessor(this), this.remotingExecutor);
        }
    }

    /**
     * 启动监听 处理来自broker的链接
     *
     * @throws Exception
     */
    public void start() throws Exception {
        //开启netty服务 监听端口 处理broker发送过来的请求
        this.remotingServer.start();
    }

    //结束时终止的顺序
    public void shutdown() {
        //1。关闭netty服务
        this.remotingServer.shutdown();
        //2。关闭
        this.remotingExecutor.shutdown();
        //3。关闭定时检测broker
        this.scheduledExecutorService.shutdown();
    }

    public NamesrvConfig getNamesrvConfig() {
        return namesrvConfig;
    }

    public NettyServerConfig getNettyServerConfig() {
        return nettyServerConfig;
    }

    public KVConfigManager getKvConfigManager() {
        return kvConfigManager;
    }

    public RouteInfoManager getRouteInfoManager() {
        return routeInfoManager;
    }

    public RemotingServer getRemotingServer() {
        return remotingServer;
    }

    public void setRemotingServer(RemotingServer remotingServer) {
        this.remotingServer = remotingServer;
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
