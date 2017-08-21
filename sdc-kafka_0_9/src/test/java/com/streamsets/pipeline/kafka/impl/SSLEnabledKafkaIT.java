/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.streamsets.testing.NetworkUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.util.Map;
import java.util.Properties;

@Ignore
public class SSLEnabledKafkaIT extends SecureKafkaBase {

  private static int plaintextPort;
  private static int specializedPort;

  @BeforeClass
  public static void beforeClass() throws Exception {
    plaintextPort = NetworkUtils.getRandomPort();
    specializedPort = NetworkUtils.getRandomPort();
    SecureKafkaBase.beforeClass();
  }

  @AfterClass
  public static void afterClass() {
    SecureKafkaBase.afterClass();
  }

  @Override
  protected void addBrokerSecurityConfig(Properties props) {
    TestUtil09.addBrokerSslConfig(props);
    StringBuilder listeners = new StringBuilder();
    listeners
      .append(String.format("PLAINTEXT://localhost:%d", getPlainTextPort()))
      .append(",")
      .append(String.format("SSL://localhost:%d", getSecurePort()));
    // security config
    props.setProperty("listeners", listeners.toString());
  }

  @Override
  protected void addClientSecurityConfig(Map<String, Object> props) {
    TestUtil09.addClientSslConfig(props);
  }

  @Override
  protected int getPlainTextPort() {
    return plaintextPort;
  }

  @Override
  protected int getSecurePort() {
    return specializedPort;
  }

  @Override
  protected String getTopic() {
    return "SSLEnabledKafkaIT";
  }
}
