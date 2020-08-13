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

package com.streamsets.pipeline.lib.jdbc;

import com.streamsets.pipeline.lib.jdbc.connection.JdbcConnection;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBasicConnectionString {


  private static BasicConnectionString basicConnectionString;

  @BeforeClass
  public static void setUp() {
    JdbcHikariPoolConfigBean hikariPoolConfigBean = new JdbcHikariPoolConfigBean();
    hikariPoolConfigBean.connection = new JdbcConnection();
    basicConnectionString = new BasicConnectionString(hikariPoolConfigBean.getPatterns(),
        hikariPoolConfigBean.getConnectionStringTemplate()
    );
  }

  @Test
  public void patternVerification_host() {
    String url = "jdbc:vendor://somehost:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }

  @Test
  public void patternVerification_hostWithDomain() {
    String url = "jdbc:vendor://somehost.domain:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }

  @Test
  public void patternVerification_IPAddressV6() {
    String url = "jdbc:vendor://2001:0db8:85a3:0000:0000:8a2e:0370:7334:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }

  @Test
  public void patternVerification_IPAddressV6Compressed() {
    String url = "jdbc:vendor://2001:db8:85a3::8a2e:370:7334:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }

  @Test
  public void patternVerification_postgreSQL() {
    String url = "jdbc:postgresql://postgresql_8.0.cluster:5432/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }

  @Test
  public void patternVerification_postgreSQL_IPV6() {
    String url = "jdbc:postgresql://2001:0db8:85a3:0000:0000:8a2e:0370:7334:5432/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }

  @Test
  public void patternVerification_sqlServer() {
    String url = "jdbc:sqlserver://sqlserver_8.0.cluster:1433;DatabaseName=default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }

  @Test
  public void patternVerification_sqlServer_IPV6() {
    String url = "jdbc:sqlserver://0:0:0:0:0:0:0:1:1433";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }

  @Test
  public void patternVerification_mysql() {
    String url = "jdbc:mysql://mysql_8.0.cluster:3306";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }

  @Test
  public void patternVerification_mysql_database() {
    String url = "jdbc:mysql://mysql_8.0.cluster:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }

  @Test
  public void patternVerification_oracle() {
    String
        url
        = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=localhost) (PORT=2484)) (CONNECT_DATA= " +
        "(SID=SDC)))";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }

  @Test
  public void patternVerification_oracle_secured() {
    String
        url
        = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=localhost) (PORT=2484)) (CONNECT_DATA=" +
        "(SID=SDC)) (SECURITY=(SSL_SERVER_CERT_DN=CN=stf-oracle-11.c9v0wdsbqa7e.us-west-2.rds.amazonaws.com,OU=RDS," +
        "O=Amazon.com,L=Seattle,ST=Washington,C=US)))";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);
  }
}
