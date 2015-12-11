/**
 * Copyright 2015 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.kafka.impl;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import kafka.utils.TestUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import scala.Option;

import java.io.File;
import java.io.IOException  ;
import java.net.ServerSocket;
import java.util.Properties;

public class TestUtil {

  public static int getFreePort() throws IOException {
    ServerSocket serverSocket = new ServerSocket(0);
    int port = serverSocket.getLocalPort();
    serverSocket.close();
    return port;
  }

  public static KafkaServer createKafkaServer(int port, String zkConnect) {
    final Option<File> noFile = scala.Option.apply(null);
    final Option<SecurityProtocol> noInterBrokerSecurityProtocol = scala.Option.apply(null);
    Properties props = TestUtils.createBrokerConfig(
      0, zkConnect, false, false, port, noInterBrokerSecurityProtocol,
      noFile, true, false, TestUtils.RandomPort(), false, TestUtils.RandomPort(), false,
      TestUtils.RandomPort());
    props.setProperty("auto.create.topics.enable", "true");
    props.setProperty("num.partitions", "1");
    props.setProperty("zookeeper.connect", zkConnect);
    KafkaConfig config = new KafkaConfig(props);
    return TestUtils.createServer(config, SystemTime$.MODULE$);
  }


}
