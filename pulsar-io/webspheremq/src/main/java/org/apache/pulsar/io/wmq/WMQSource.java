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
import static com.ibm.mq.constants.CMQC.MQGMO_CONVERT;
import static com.ibm.mq.constants.CMQC.MQGMO_FAIL_IF_QUIESCING;
import static com.ibm.mq.constants.CMQC.MQGMO_WAIT;
import static com.ibm.mq.constants.CMQC.MQOO_FAIL_IF_QUIESCING;
import static com.ibm.mq.constants.CMQC.MQOO_INPUT_SHARED;
import static com.ibm.mq.constants.CMQC.MQOO_INQUIRE;
import static com.ibm.mq.constants.CMQC.MQWI_UNLIMITED;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.jmqi.ConnectionName;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
        name = "wmq_source",
        type = IOType.SOURCE,
        help = "A simple connector to move messages from a IBM MQ queue to a Pulsar topic",
        configClass = WMQConnectorConfig.class)

@Slf4j
public class WMQSource extends PushSource<byte[]> implements Source<byte[]> {

    private WMQSource() {
    }
    @Getter
    private WMQConnectorConfig config;

    private ConnectionName connection;
    MQQueue queue1;
    MQQueueManager qMgr;
    String messContent;
    byte[] b;
    int depth;
    MQMessage mess = null;
    MQGetMessageOptions gmo = new MQGetMessageOptions();
    //SessionConfig session;
    File file = new File("append.txt");
    //MQConsumer messageConsumer;
    @Override
    public void open(Map<String, Object> map, SourceContext sourceContext) throws Exception {

        if (null != config) {
            throw new Exception("Connector is open");
        }

        config = WMQConnectorConfig.load(map);
        config.validate();

        qMgr = new MQQueueManager(config.getQmanName());
        int openOptions = MQOO_INQUIRE + MQOO_FAIL_IF_QUIESCING + MQOO_INPUT_SHARED;
        queue1 = qMgr.accessQueue(config.getQueueName(), openOptions, null, null, null);
    }

    public Record<byte[]> read() throws Exception {

        depth = queue1.getCurrentDepth();

            FileWriter fr = new FileWriter(file, true);
            fr.write("depth= " + depth + "\n");
            mess = new MQMessage();
            // Get message contentss
            gmo.options = MQGMO_WAIT + MQGMO_FAIL_IF_QUIESCING + MQGMO_CONVERT;
            gmo.waitInterval = MQWI_UNLIMITED;

            queue1.get(mess, gmo);
            b = new byte[mess.getMessageLength()]; //tworzy byte b o wielkosci mess
            mess.readFully(b); //wypelnia b trescia z mess
            messContent = new String(b); //messContent

            fr.write("b= " + b + "\nmessContent= " + messContent + "\nmess= " + mess + "\n");
            fr.close();

            Thread.sleep(100);

            //tekst = scan.nextLine();


            //} catch (com.ibm.mq.MQException mqex) {
            //    System.out.println("MQException cc=" + mqex.completionCode + " : rc=" + mqex.reasonCode);
            //} catch (IOException e) {
            // TODO Auto-generated catch block
            //    e.printStackTrace();
            //}
            //log.warn("No messages found");
            //throw new Exception("No messages found");

            return new Record<byte[]>() {
                @Override
                public byte[] getValue() {
                    return messContent.getBytes();

                }
            };

        }

        @Override
        public void close () throws Exception {
            queue1.close();
            qMgr.disconnect();
    }

}
