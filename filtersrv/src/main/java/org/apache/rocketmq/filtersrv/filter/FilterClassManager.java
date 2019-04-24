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

package org.apache.rocketmq.filtersrv.filter;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.MessageFilter;
import org.apache.rocketmq.filtersrv.FiltersrvController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 过滤类管理
 */
public class FilterClassManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.FILTERSRV_LOGGER_NAME);

    /**
     * 编译Lock
     */
    private final Object compileLock = new Object();
    private final FiltersrvController filtersrvController;
    //创建一个线程池，定时去获取过滤器类
    private final ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("FSGetClassScheduledThread"));

    /**
     * 过滤类信息映射
     */
    private ConcurrentHashMap<String/* topic@consumerGroup */, FilterClassInfo> filterClassTable =
            new ConcurrentHashMap<>(128);
    /**
     * 类获取的方式
     */
    private FilterClassFetchMethod filterClassFetchMethod;

    public FilterClassManager(FiltersrvController filtersrvController) {
        this.filtersrvController = filtersrvController;
        this.filterClassFetchMethod =
                //url来自于配置文件
                new HttpFilterClassFetchMethod(this.filtersrvController.getFiltersrvConfig()
                        .getFilterClassRepertoryUrl());
    }

    //构建key
    private static String buildKey(final String consumerGroup, final String topic) {
        return topic + "@" + consumerGroup;
    }

    public void start() {
        //如果不允许客户端上传过滤器类的话
        if (!this.filtersrvController.getFiltersrvConfig().isClientUploadFilterClassEnable()) {
            //线程池定时去更新过处理器类
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    fetchClassFromRemoteHost();
                }
            }, 1, 1, TimeUnit.MINUTES);
        }
    }

    /**
     * 定时去更新过滤器类
     */
    private void fetchClassFromRemoteHost() {
        Iterator<Entry<String, FilterClassInfo>> it = this.filterClassTable.entrySet().iterator();
        while (it.hasNext()) {
            try {
                Entry<String, FilterClassInfo> next = it.next();
                FilterClassInfo filterClassInfo = next.getValue();
                String[] topicAndGroup = next.getKey().split("@");
                //根据消费者组，topic去获取远程机器上的过滤器类信息
                String responseStr =
                        this.filterClassFetchMethod.fetch(topicAndGroup[0], topicAndGroup[1],
                                filterClassInfo.getClassName());
                //二进制信息
                byte[] filterSourceBinary = responseStr.getBytes("UTF-8");
                //生成一个新的crc校验
                int classCRC = UtilAll.crc32(responseStr.getBytes("UTF-8"));
                //如果crc信息不想等，说明发生了变化
                if (classCRC != filterClassInfo.getClassCRC()) {
                    String javaSource = new String(filterSourceBinary, MixAll.DEFAULT_CHARSET);
                    //重新编译加载
                    Class<?> newClass =
                            DynaCode.compileAndLoadClass(filterClassInfo.getClassName(), javaSource);
                    Object newInstance = newClass.newInstance();
                    filterClassInfo.setMessageFilter((MessageFilter) newInstance);
                    filterClassInfo.setClassCRC(classCRC);

                    log.info("fetch Remote class File OK, {} {}", next.getKey(),
                            filterClassInfo.getClassName());
                }
            } catch (Exception e) {
                log.error("fetchClassFromRemoteHost Exception", e);
            }
        }
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
    }

    /**
     * 注册过滤类
     *
     * @param consumerGroup      消费分组
     * @param topic              Topic
     * @param className          过滤类名
     * @param classCRC           过滤类源码CRC
     * @param filterSourceBinary 过滤类源码
     * @return 是否注册成功
     */
    public boolean registerFilterClass(final String consumerGroup, final String topic,
                                       final String className, final int classCRC, final byte[] filterSourceBinary) {
        //生成一个key
        final String key = buildKey(consumerGroup, topic);
        // 判断是否要注册新的过滤类
        boolean registerNew = false;
        FilterClassInfo filterClassInfoPrev = this.filterClassTable.get(key);
        //如果是第一次注册
        if (null == filterClassInfoPrev) {
            registerNew = true;
        } else {
            //如果是更新 默认是true允许客户端上传过滤器类
            if (this.filtersrvController.getFiltersrvConfig().isClientUploadFilterClassEnable()) {
                //如果类有变化也相当于重新注册
                if (filterClassInfoPrev.getClassCRC() != classCRC && classCRC != 0) { // 类有变化
                    registerNew = true;
                }
            }
        }
        // 注册新的过滤类
        if (registerNew) {
            synchronized (this.compileLock) {
                filterClassInfoPrev = this.filterClassTable.get(key);
                //如果类没有变化则不进行更新
                if (null != filterClassInfoPrev && filterClassInfoPrev.getClassCRC() == classCRC) {
                    return true;
                }
                try {
                    //新建一个过滤器类信息
                    FilterClassInfo filterClassInfoNew = new FilterClassInfo();
                    //只初始化类的名字
                    filterClassInfoNew.setClassName(className);
                    filterClassInfoNew.setClassCRC(0);
                    filterClassInfoNew.setMessageFilter(null);
                    //允许客户端上传过滤器类 默认是开启的
                    if (this.filtersrvController.getFiltersrvConfig().isClientUploadFilterClassEnable()) {
                        //二进制传递过来的java信息 ，默认编码格式为utf-8
                        String javaSource = new String(filterSourceBinary, MixAll.DEFAULT_CHARSET);
                        // 编译新的过滤类 className应该是完整的类型的名字
                        Class<?> newClass = DynaCode.compileAndLoadClass(className, javaSource);
                        // 创建新的过滤类对象
                        Object newInstance = newClass.newInstance();
                        filterClassInfoNew.setMessageFilter((MessageFilter) newInstance);
                        filterClassInfoNew.setClassCRC(classCRC);
                    }
                    //把这个新的过滤器信息更新
                    this.filterClassTable.put(key, filterClassInfoNew);
                } catch (Throwable e) {
                    String info = String.format("FilterServer, registerFilterClass Exception, consumerGroup: %s topic: %s className: %s",
                            consumerGroup, topic, className);
                    log.error(info, e);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 根据消费组和topic获取有哪些过滤器类
     *
     * @param consumerGroup 消费组
     * @param topic         话题
     * @return
     */
    public FilterClassInfo findFilterClass(final String consumerGroup, final String topic) {
        return this.filterClassTable.get(buildKey(consumerGroup, topic));
    }

    /**
     * 一般是通过netty传递过来的把
     *
     * @return
     */
    public FilterClassFetchMethod getFilterClassFetchMethod() {
        return filterClassFetchMethod;
    }

    public void setFilterClassFetchMethod(FilterClassFetchMethod filterClassFetchMethod) {
        this.filterClassFetchMethod = filterClassFetchMethod;
    }
}
