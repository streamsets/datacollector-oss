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

public class TestOracleBasicConnectionString {

  private static BasicConnectionString basicConnectionString;

  @BeforeClass
  public static void setUp() {
    HikariPoolConfigBean hikariPoolConfigBean = new OracleHikariPoolConfigBean();
    basicConnectionString = new BasicConnectionString(hikariPoolConfigBean.getPatterns(),
        hikariPoolConfigBean.getConnectionStringTemplate()
    );
  }

  @Test
  public void patternVerification_localhost_verifyHostname() {
    String
        url
        = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=localhost) (PORT=2484)) (CONNECT_DATA= " +
        "(SID=SDC)) (SECURITY=(SSL_SERVER_CERT_DN=CN=stf-oracle-11.c9v0wdsbqa7e.us-west-2.rds.amazonaws.com,OU=RDS," +
        "O=Amazon.com,L=Seattle,ST=Washington,C=US)))";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("localhost", info.getHost());
    Assert.assertEquals(2484, info.getPort());
    Assert.assertEquals(
        "(CONNECT_DATA= (SID=SDC)) (SECURITY=(SSL_SERVER_CERT_DN=CN=stf-oracle-11.c9v0wdsbqa7e.us-west-2.rds" +
            ".amazonaws.com,OU=RDS,O=Amazon.com,L=Seattle,ST=Washington,C=US)))",
        info.getTail()
    );
  }

  @Test
  public void patternVerification_localhost() {
    String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=localhost) (PORT=2484)) " +
        "(CONNECT_DATA= (SID=SDC)))";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("localhost", info.getHost());
    Assert.assertEquals(2484, info.getPort());
    Assert.assertEquals("(CONNECT_DATA= (SID=SDC)))", info.getTail());
  }

  @Test
  public void patternVerification_IPv4() {
    String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=127.0.0.1) (PORT=2484)) " +
        "(CONNECT_DATA= (SID=SDC)))";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("127.0.0.1", info.getHost());
    Assert.assertEquals(2484, info.getPort());
    Assert.assertEquals("(CONNECT_DATA= (SID=SDC)))", info.getTail());
  }

  @Test
  public void patternVerification_IPv6() {
    String url =
        "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=2001:0db8:85a3:0000:0000:8a2e:0370:7334) " +
            "(PORT=2484)) (CONNECT_DATA= (SID=SDC)))";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("2001:0db8:85a3:0000:0000:8a2e:0370:7334", info.getHost());
    Assert.assertEquals(2484, info.getPort());
    Assert.assertEquals("(CONNECT_DATA= (SID=SDC)))", info.getTail());
  }

  @Test
  public void patternVerification_IPv6_localhost() {
    String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=0:0:0:0:0:0:0:1) " +
        "(PORT=2484)) (CONNECT_DATA= (SID=SDC)))";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("0:0:0:0:0:0:0:1", info.getHost());
    Assert.assertEquals(2484, info.getPort());
    Assert.assertEquals("(CONNECT_DATA= (SID=SDC)))", info.getTail());
  }

  @Test
  public void patternVerification_IPv6Compressed() {
    String url = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=2001:db8:85a3::8a2e:370:7334) " +
        "(PORT=2484)) (CONNECT_DATA= (SID=SDC)))";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", info.getHost());
    Assert.assertEquals(2484, info.getPort());
    Assert.assertEquals("(CONNECT_DATA= (SID=SDC)))", info.getTail());
  }

  @Test
  public void patternVerification_awsHost() {
    String url =
        "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=ec2-54-71-128-112.eu-east-13.compute.amazonaws" +
            ".com) " +
            "(PORT=2484)) (CONNECT_DATA= (SID=SDC)))";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("ec2-54-71-128-112.eu-east-13.compute.amazonaws.com", info.getHost());
    Assert.assertEquals(2484, info.getPort());
    Assert.assertEquals("(CONNECT_DATA= (SID=SDC)))", info.getTail());
  }

  @Test
  public void patternVerification_otherVendor() {
    String url = "jdbc:mysql://localhost:2484/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNull(info);
  }

  @Test
  public void patternVerification_wrongProtocol() {
    String url = "jdc:oracle://localhost:2484/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNull(info);
  }
}