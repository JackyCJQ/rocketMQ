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
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name");

        producer.setNamesrvAddr("127.0.0.1:9876"); //
//        producer.setSendLatencyFaultEnable(true);

        producer.start();

//        Thread.sleep(10000000L);

        String body = "";
        for (int i = 0; i < 10 * 1024; i++) {
            body += "" + i;
        }

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            try {

                /*
                 * Create a message instance, specifying topic, tag and message body.
                 */

                Message msg = new Message("TopicTest_mis" /* Topic */,
                        "TagA" /* Tag */,
                        (body).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );

                /*
                 * Call send message to deliver message to one of brokers.
                 */
                SendResult sendResult = producer.send(msg);
//                producer.send(msg);
//                producer.sendOneway(msg);


                System.out.printf("%s%n", sendResult);
                break;
//                System.out.println(i);
//                if (i % 10000 == 0) {
//                    System.out.println("sendOneway：" + i);
//                }
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
    }
}
