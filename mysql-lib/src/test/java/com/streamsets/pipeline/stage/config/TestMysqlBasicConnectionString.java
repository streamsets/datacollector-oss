/*
 * Copyright 2019 StreamSets Inc.
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

package com.streamsets.pipeline.stage.config;

import com.streamsets.pipeline.lib.jdbc.BasicConnectionString;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMysqlBasicConnectionString {

  private static BasicConnectionString basicConnectionString;

  @BeforeClass
  public static void setUp(){
    HikariPoolConfigBean hikariPoolConfigBean = new MySQLHikariPoolConfigBean();
    basicConnectionString = new BasicConnectionString(hikariPoolConfigBean.getPatterns(),
        hikariPoolConfigBean.getConnectionStringTemplate()
    );
  }

  @Test
  public void patternVerification_localhost() {
    String url = "jdbc:mysql://localhost:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("localhost", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_defaultPort() {
    String url = "jdbc:mysql://localhost/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("localhost", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_hostWithDomain() {
    String url = "jdbc:mysql://mysql_8.0.cluster:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("mysql_8.0.cluster", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_awsHost() {
    String url = "jdbc:mysql://ec2-54-71-128-112.eu-east-13.compute.amazonaws.com:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("ec2-54-71-128-112.eu-east-13.compute.amazonaws.com", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_IPAddress() {
    String url = "jdbc:mysql://127.0.0.1:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("127.0.0.1", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_IPAddressV6() {
    String url = "jdbc:mysql://2001:0db8:85a3:0000:0000:8a2e:0370:7334:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("2001:0db8:85a3:0000:0000:8a2e:0370:7334", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_IPAddressV6LocalHost() {
    String url = "jdbc:mysql://0:0:0:0:0:0:0:1:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("0:0:0:0:0:0:0:1", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_IPAddressV6LocalHost_noTail() {
    String url = "jdbc:mysql://0:0:0:0:0:0:0:1:3306";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("0:0:0:0:0:0:0:1", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("", info.getTail());
  }

  @Test
  public void patternVerification_IPAddressV6LocalHost_defaultPort() {
    String url = "jdbc:mysql://0:0:0:0:0:0:0:1/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("0:0:0:0:0:0:0:1", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_IPAddressV6Compressed() {
    String url = "jdbc:mysql://2001:db8:85a3::8a2e:370:7334:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_IPAddressWithoutTail() {
    String url = "jdbc:mysql://127.0.0.1:3306";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("127.0.0.1", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("", info.getTail());
  }

  @Test
  public void patternVerification_otherVendor() {
    String url = "jdbc:oracle://localhost:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNull(info);
  }

  @Test
  public void patternVerification_wrongProtocol() {
    String url = "jdc:mysql://localhost:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNull(info);
  }
}
