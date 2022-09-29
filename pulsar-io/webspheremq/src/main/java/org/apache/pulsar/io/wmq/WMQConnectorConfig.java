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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.msg.client.wmq.common.internal.messages.WMQTextMessage;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import lombok.Data;

@Data
class WMQConnectorConfig implements Serializable{
  private String protocol;
  private String host;
  private String port;

  private String username;
  private String password;
  private String queueName;
  private String channelName;
  private String qmanName;
  private String topicName;

  private String wmqMessageType = WMQTextMessage.class.getSimpleName();

  // Connection and open parameters
  private int mqoo;
  private int mqgmo;
  private int mqpmo;

    // Map Objects from class
  public static WMQConnectorConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), WMQConnectorConfig.class);
    }

    // Validate variable values
    public void validate() {

        Objects.requireNonNull(protocol, "protocol property not set.");
        Objects.requireNonNull(host, "host property not set.");
        Objects.requireNonNull(port, "port property not set.");

        // Check if destination is queue or topic
    String destinationName = null;
    if (StringCheck.isNotEmpty(queueName)) {
       destinationName = queueName;
    } else if (StringCheck.isNotEmpty(topicName)) {
       destinationName = topicName;
    }
    Objects.requireNonNull(destinationName, "queueName and topicName all not set.");
    }

    // Generate broker url
    public String getBrokerUrl() {
        return protocol + "://" + host + ":" + port;
    }

}

