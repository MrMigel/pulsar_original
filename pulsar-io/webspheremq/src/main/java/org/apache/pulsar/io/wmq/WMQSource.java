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

import static com.ibm.mq.constants.CMQC.MQGMO_CONVERT;
import static com.ibm.mq.constants.CMQC.MQGMO_FAIL_IF_QUIESCING;
import static com.ibm.mq.constants.CMQC.MQGMO_WAIT;
import static com.ibm.mq.constants.CMQC.MQOO_FAIL_IF_QUIESCING;
import static com.ibm.mq.constants.CMQC.MQOO_INPUT_SHARED;
import static com.ibm.mq.constants.CMQC.MQOO_INQUIRE;
import static com.ibm.mq.constants.CMQC.MQWI_UNLIMITED;
import com.ibm.mq.MQEnvironment;
import com.ibm.mq.MQGetMessageOptions;
import com.ibm.mq.MQMessage;
import com.ibm.mq.MQQueue;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.jmqi.ConnectionName;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
//import java.io.File;
//import java.io.FileWriter;


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
    private MQQueue queue;
    private MQQueueManager qMgr;
    private String messContent;
    private byte[] b;
    private int depth;
    private MQMessage mess = null;
    private MQGetMessageOptions gmo = new MQGetMessageOptions();

    //File file = new File("_debug.txt");

    @Override
    public void open(Map<String, Object> map, SourceContext sourceContext) throws Exception {

        if (null != config) {
            throw new Exception("Connector is open");
        }

        config = WMQConnectorConfig.load(map);
        config.validate();

        MQEnvironment.hostname = config.getHost();
        MQEnvironment.channel = config.getChannelName();
        MQEnvironment.port = Integer.valueOf(config.getPort());
        MQEnvironment.userID = config.getUsername();
        MQEnvironment.password = config.getPassword();

        qMgr = new MQQueueManager(config.getQmanName());
        int openOptions = MQOO_INQUIRE + MQOO_FAIL_IF_QUIESCING + MQOO_INPUT_SHARED;
        queue = qMgr.accessQueue(config.getQueueName(), openOptions, null, null, null);
    }


    public Record<byte[]> read() throws Exception {

        depth = queue.getCurrentDepth();

        mess = new MQMessage();
        // Get message contentss
        gmo.options = MQGMO_WAIT + MQGMO_FAIL_IF_QUIESCING + MQGMO_CONVERT;
        gmo.waitInterval = MQWI_UNLIMITED;

        queue.get(mess, gmo);
        b = new byte[mess.getMessageLength()]; // create byte b mess lenght size
        mess.readFully(b); // fill b by content of mess
        messContent = new String(b); // messContent

        // ### Save variables to file - for debugging ###
        //FileWriter fr = new FileWriter(file, true);
        //fr.write("depth= " + depth + "\n");
        //fr.write("b= " + b + "\nmessContent= " + messContent + "\nmess= " + mess + "\n");
        //fr.write("queue name= " + config.getQueueName() + "\nQmanagerName= " + config.getQmanName());
        //fr.close();

        Thread.sleep(100);

            return new Record<byte[]>() {
                @Override
                public byte[] getValue() {
                    return messContent.getBytes();

                }
            };
        }

        @Override
        public void close () throws Exception {
            queue.close();
            qMgr.disconnect();
    }
}
