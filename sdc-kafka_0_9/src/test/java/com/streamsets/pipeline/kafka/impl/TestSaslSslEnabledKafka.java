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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.util.Map;
import java.util.Properties;

@Ignore
public class TestSaslSslEnabledKafka extends TestSaslEnabledKafka {

  @BeforeClass
  public static void beforeClass() throws Exception {
    TestSaslEnabledKafka.beforeClass();
  }

  @AfterClass
  public static void afterClass() {
    TestSaslEnabledKafka.afterClass();
  }

  @Override
  protected void addBrokerSecurityConfig(Properties props) {
    TestUtil09.addBrokerSslConfig(props);
    props.setProperty("security.inter.broker.protocol", "SASL_SSL");
    props.setProperty("sasl.kerberos.service.name", "kafkaBroker");
    StringBuilder listeners = new StringBuilder();
    listeners
      .append(String.format("PLAINTEXT://localhost:%d", getPlainTextPort()))
      .append(",")
      .append(String.format("SASL_SSL://localhost:%d", getSecurePort()));
    props.setProperty("listeners", listeners.toString());
  }

  @Override
  protected void addClientSecurityConfig(Map<String, Object> props) {
    TestUtil09.addClientSslConfig(props);
    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.kerberos.service.name", "kafkaBroker");
  }

  @Override
  protected String getTopic() {
    return "TestSaslSslEnabledKafka";
  }
}
