/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.wmq;

//import static com.ibm.mq.constants.CMQC.MQGMO_CONVERT;
//import static com.ibm.mq.constants.CMQC.MQGMO_FAIL_IF_QUIESCING;
//import static com.ibm.mq.constants.CMQC.MQGMO_NO_WAIT;
//import static com.ibm.mq.constants.CMQC.MQGMO_BROWSE_NEXT;
import static com.ibm.mq.constants.CMQC.MQOO_INPUT_AS_Q_DEF;
import static com.ibm.mq.constants.CMQC.MQOO_OUTPUT;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQPutMessageOptions;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.jmqi.ConnectionName;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
        name = "wmq_sink",
        type = IOType.SINK,
        help = "A simple connector to move messages from a IBM MQ queue to a Pulsar topic",
        configClass = WMQConnectorConfig.class)

@Slf4j
public class WMQSink implements Sink<byte[]> {

    private WMQSink() {
    }
    @Getter
    private WMQConnectorConfig config;

    private ConnectionName connection;
    MQQueue queue1;
    MQQueueManager qMgr;
    String messContent;
    byte[] b;
    MQMessage mess = null;
    MQGetMessageOptions gmo = new MQGetMessageOptions();
    //SessionConfig session;
    File file = new File("append.txt");
    //MQConsumer messageConsumer;
    @Override
    public void open(Map<String, Object> map, SinkContext sinkContext) throws Exception {

        if (null != config) {
            throw new Exception("Connector is open");
        }

        config = WMQConnectorConfig.load(map);
        config.validate();

        qMgr = new MQQueueManager(config.getQmanName());
        int openOptions = MQOO_OUTPUT | MQOO_INPUT_AS_Q_DEF;
        queue1 = qMgr.accessQueue(config.getQueueName(), openOptions, null, null, null);
        log.info("### A new connection to {}:{} has been opened successfully. ###",
                config.getQmanName(),
                config.getPort());

    }

    public void write(Record<byte[]> record) throws Exception {

            try {

                MQPutMessageOptions pmo = new MQPutMessageOptions();
                pmo.options = MQConstants.MQPMO_ASYNC_RESPONSE;
                mess = new MQMessage();

                mess.format = MQConstants.MQFMT_STRING;
                mess.write(record.getValue());
                queue1.put(mess, pmo);
                record.ack();
                //queue1.close();

                //tekst = scan.nextLine();
            } catch (com.ibm.mq.MQException mqex) {
                //System.out.println("MQException cc=" + mqex.completionCode + " : rc=" + mqex.reasonCode);
                log.error("MQException cc=" + mqex.completionCode + " : rc=" + mqex.reasonCode);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (Exception e){
                record.fail();
                log.warn("### Failed to publish the message to WebsphereMQ ###", e);
            }
    }

    @Override
    public void close() throws Exception {
        queue1.close();
        qMgr.disconnect();

    }
}
