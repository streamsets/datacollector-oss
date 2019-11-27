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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestBasicConnectionString {


  private static BasicConnectionString basicConnectionString;

  @BeforeClass
  public static void setUp(){
    HikariPoolConfigBean hikariPoolConfigBean = new HikariPoolConfigBean();
    basicConnectionString = new BasicConnectionString(hikariPoolConfigBean.getPatterns(),
        hikariPoolConfigBean.getConnectionStringTemplate()
    );
  }

  @Test
  public void patternVerification_host() {
    String url = "jdbc:vendor://somehost:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("somehost", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_hostWithDomain() {
    String url = "jdbc:vendor://somehost.domain:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("somehost.domain", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_IPAddressV6() {
    String url = "jdbc:vendor://2001:0db8:85a3:0000:0000:8a2e:0370:7334:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("2001:0db8:85a3:0000:0000:8a2e:0370:7334", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }

  @Test
  public void patternVerification_IPAddressV6Compressed() {
    String url = "jdbc:vendor://2001:db8:85a3::8a2e:370:7334:3306/default";
    BasicConnectionString.Info info = basicConnectionString.getBasicConnectionInfo(url);

    Assert.assertNotNull(info);

    Assert.assertEquals("2001:db8:85a3::8a2e:370:7334", info.getHost());
    Assert.assertEquals(3306, info.getPort());
    Assert.assertEquals("/default", info.getTail());
  }
}
