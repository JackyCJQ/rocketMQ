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
package org.apache.rocketmq.srvutil;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ServerUtil {

    public static Options buildCommandlineOptions(final Options options) {
        //指定这个命令行 短操作为 -h 完整操作为 -help 是否有参数   对该命令的描述
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        //增加一个帮助命令
        options.addOption(opt);
        //构造启动时需要加上 -n 说明命名服务器的地址
        opt =new Option("n", "namesrvAddr", true,
                        "Name server address list, eg: 192.168.0.1:9876;192.168.0.2:9876");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    /**
     * @param appName
     * @param args
     * @param options 包括 -h -n -p -c 四个参数
     * @param parser
     * @return
     */
    public static CommandLine parseCmdLine(final String appName, String[] args, Options options, CommandLineParser parser) {
        HelpFormatter hf = new HelpFormatter();
        //每行显示的字符数。超过了就换行
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            //解析程序入口输入的program arguments 中输入-h -n -p -c 四个参数的值
            commandLine = parser.parse(options, args);
            //如果有-h的输入 则输出：
            /**
             * usage: mqnamesrv [-c <arg>] [-h] [-n <arg>] [-p]
             -c,--configFile <arg>    Name server config properties file
             -h,--help                Print help
             -n,--namesrvAddr <arg>   Name server address list, eg: 192.168.0.1:9876;192.168.0.2:9876
             -p,--printConfigItem     Print all config item
             */
            if (commandLine.hasOption('h')) {
                hf.printHelp(appName, options, true);
                return null;
            }
        } catch (ParseException e) {
            hf.printHelp(appName, options, true);
        }
      //如果没有则直接返回，解析出没有配置的结果
        return commandLine;
    }

    public static void printCommandLineHelp(final String appName, final Options options) {
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(appName, options, true);
    }

    public static Properties commandLine2Properties(final CommandLine commandLine) {
        Properties properties = new Properties();
        Option[] opts = commandLine.getOptions();

        if (opts != null) {
            for (Option opt : opts) {
                String name = opt.getLongOpt();
                String value = commandLine.getOptionValue(name);
                if (value != null) {
                    properties.setProperty(name, value);
                }
            }
        }

        return properties;
    }

}
