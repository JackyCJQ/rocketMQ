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

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvConfig;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamesrvStartup {
    public static Properties properties = null;
    public static CommandLine commandLine = null;

    public static void main(String[] args) {
        main0(args);
    }

    public static NamesrvController main0(String[] args) {
        //首先设置全局的版本
        //rocketmq.remoting.version 设置全局版本是V4_1_0_SNAPSHOT但是存放的值是对应的枚举类的序列
         System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
           //key-->com.rocketmq.remoting.socket.sndbuf.size
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE)) {
            //修改系统默认大小，修改为4k
            NettySystemConfig.socketSndbufSize = 4096;
        }
          //key-->com.rocketmq.remoting.socket.rcvbuf.size
        if (null == System.getProperty(NettySystemConfig.COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE)) {
            //修改系统默认大小，修改为4k
            NettySystemConfig.socketRcvbufSize = 4096;
        }

        try {
            //添加了两个命令 -h 帮助 -n 命名服务器的地址
            Options options = ServerUtil.buildCommandlineOptions(new Options());
            //解析程序入口输入的program arguments 中输入 -n  -c 四个参数的值
            commandLine =
                ServerUtil.parseCmdLine("mqnamesrv", args, buildCommandlineOptions(options),
                    new PosixParser());
            //如果没有配置-n -c 则不能启动 直接结束 找不到配置文件和命名服务
            if (null == commandLine) {
                System.exit(-1);
                return null;
            }
            //创建两个关键的配置，都是无参配置，设置的参数需要要set和get方法设置
            final NamesrvConfig namesrvConfig = new NamesrvConfig();
            final NettyServerConfig nettyServerConfig = new NettyServerConfig();
            //netty默认监听的端口为9876
            nettyServerConfig.setListenPort(9876);
            // 解析程序入口输入的program arguments 含有-c configFile
            if (commandLine.hasOption('c')) {
                //获取配置的值
                String file = commandLine.getOptionValue('c');
                //将配置文件读取
                if (file != null) {
                    InputStream in = new BufferedInputStream(new FileInputStream(file));
                    properties = new Properties();
                    properties.load(in);
                    //解析配置文件里面的配置，并设置到对应的配置中
                    MixAll.properties2Object(properties, namesrvConfig);
                    MixAll.properties2Object(properties, nettyServerConfig);
                    //设置一下配置文件的路径
                    namesrvConfig.setConfigStorePath(file);
                    System.out.printf("load config properties file OK, " + file + "%n");
                    in.close();
                }
            }

            // 解析程序入口输入的program arguments 含有-p printConfigItem
            if (commandLine.hasOption('p')) {
                MixAll.printObjectProperties(null, namesrvConfig);
                MixAll.printObjectProperties(null, nettyServerConfig);
                System.exit(0);
            }
            //主要是-n 参数 会覆盖掉配置文件里面的
            MixAll.properties2Object(ServerUtil.commandLine2Properties(commandLine), namesrvConfig);
            //RocketmqHome是必须要设置的
            if (null == namesrvConfig.getRocketmqHome()) {
                System.out.printf("Please set the " + MixAll.ROCKETMQ_HOME_ENV
                    + " variable in your environment to match the location of the RocketMQ installation%n");
                System.exit(-2);
            }
            //配置日志。使用logback
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            //设置logback的配置文件的位置
            configurator.doConfigure(namesrvConfig.getRocketmqHome() + "/conf/logback_namesrv.xml");
            //获取一个日志
            final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
            //打印出每一项配置 配置的两个关键的配置
            MixAll.printObjectProperties(log, namesrvConfig);
            MixAll.printObjectProperties(log, nettyServerConfig);
            //当把配置解析完毕之后， 生成一个NamesrvController
            final NamesrvController controller = new NamesrvController(namesrvConfig, nettyServerConfig);

            //配置文件里面的属性
            controller.getConfiguration().registerConfig(properties);
            //初始化这个controller
            boolean initResult = controller.initialize();
            //如果初始化失败，则就退出
            if (!initResult) {
                controller.shutdown();
                System.exit(-3);
            }
            //注册JVM钩子函数并启动服务器，以便监昕 Broker、消息生产者 的网络请求 。
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                private volatile boolean hasShutdown = false;
                private AtomicInteger shutdownTimes = new AtomicInteger(0);
             //注册勾子函数，在jvm结束的时候，会调用以便来结束线程池
                @Override
                public void run() {
                    synchronized (this) {
                        log.info("shutdown hook was invoked, " + this.shutdownTimes.incrementAndGet());
                        if (!this.hasShutdown) {
                            this.hasShutdown = true;
                            long begineTime = System.currentTimeMillis();
                            //优雅的结束
                            controller.shutdown();
                            long consumingTimeTotal = System.currentTimeMillis() - begineTime;
                            log.info("shutdown hook over, consuming time total(ms): " + consumingTimeTotal);
                        }
                    }
                }
            }, "ShutdownHook"));
            //启动命名服务
            controller.start();
             //设置默认的序列化的格式为json
            String tip = "The Name Server boot success. serializeType=" + RemotingCommand.getSerializeTypeConfigInThisServer();
            log.info(tip);
            System.out.printf(tip + "%n");

            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }

    /**
     * 又增加了两个命令 -c -p
     * @param options
     * @return
     */
    public static Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("c", "configFile", true, "Name server config properties file");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "printConfigItem", false, "Print all config item");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }
}
