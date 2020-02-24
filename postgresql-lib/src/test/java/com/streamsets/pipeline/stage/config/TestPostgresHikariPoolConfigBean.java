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

import com.streamsets.pipeline.api.ConfigIssue;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.jdbc.ConnectionPropertyBean;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class TestPostgresHikariPoolConfigBean {

  private static final String CA_CERT = "-----BEGIN CERTIFICATE-----\n" +
      "MIIEljCCAn4CCQC0lJfzscyaSDANBgkqhkiG9w0BAQsFADANMQswCQYDVQQDDAJD\n" +
      "QTAeFw0xOTA5MTgxODIzNTJaFw0yMjA3MDgxODIzNTJaMA0xCzAJBgNVBAMMAkNB\n" +
      "MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAmO60YtPjPRhVAvx+Nc5X\n" +
      "ib8wVS9c/Feb79b8yaDfWw8VFP8DaJ6gRfMX6EMwuUJQZEBBrc6zz7CLVNGX+a5d\n" +
      "hzU57vpNvZRARQILcqHOl+kIrYONqvQ3O88zW9Hapt8NneA+RagRxBxnqJIXDT3P\n" +
      "YHZiXEJohfmnxVbsHdsu1Kx8TeLGddsP8Lbde/R0h3QIsPqo2Ib9bDVJjuJ0kORy\n" +
      "4p16Apfqrqgb2rBbNKBa3KHgHsxxFgtsMmMqt9vGdDP55O+22UKQrGOIrTGoVelK\n" +
      "LVeRnRTCqkAIDhDLA0f4cE2E6PSsq8tNP5YmGj+ilxfQTACJQgLY2g4pBfpVy6nh\n" +
      "T5mRBWpxHJ8fqqIPcn/SHvB/FBcUQeWvYP+xQSfAfAk8PjH+S9DMeNGQ9RnS07AY\n" +
      "KNXOyzhly7ZETxUhEwwChQywbzqTdlELxQbkw7sryfVkJST5MtWFjEhs1JeCPrLq\n" +
      "1G0V3CReXoW8UGOlKTijTsGT+colwoqFiNPVVzOVy64Oo0oee6fWUSRR+VwouAob\n" +
      "WPZEqtnaiXN4CHv34FzLo4Kt154SuYNExXJ782Szh2OWzfVHburH5K9NQ4kClPrv\n" +
      "2td+LDf5OiW3vdLo0L+lHSVFCRxX+m7ZgAWXLdjYaF3WFbRJgz/TsWHJXBvU0wcP\n" +
      "46BnFpUi95BBUEhrU0xIvBsCAwEAATANBgkqhkiG9w0BAQsFAAOCAgEAifP6yczm\n" +
      "GWOgGbunIrE6Bctv6KBL8JeErs9dQnc2iTPlgxwWtXaVDjVJ7z7reSeSRUNhnnED\n" +
      "da+tk3MduppfREb68P7n+bk7hb2E1VmT8kbJ9YbfBrmil8Sq+q3jtLLX4RhC6ZY8\n" +
      "WTJ4sxNhRSwkQIWphlhKvaYsdp5O3qXDvoLgoJS1uMiTm6k010rVbSmw6KeGLajI\n" +
      "NUkC3i2muZVrM2Hj0FTm6iHQYAmT13/oUnLR2EeKKWbtVDLPK5NEumMZ6kIO8FVq\n" +
      "hyK28H7RRPhoZ1eeNgFlG7rcT6PJIhscyqFYvz8Br1XJP5FF3C8enAQ8CLwHAQ+P\n" +
      "xTWaoe3qR1HDUxGyeiB1Yil9A/THqkyWv+FTCiukTup6zYLqW+IexSQsRv7YQir6\n" +
      "rgJ7fJzeNUQbIoNUXLkOxFSKUBfP/skDjCakXG39r4rXpr4ouBazlYLVcFXK5nEt\n" +
      "v5FkrYe7hQnqGuoYE9b6d5HqsgMLGKAhVLXxk9D+pZUt/uFkImzfgzTZjeS5NNIM\n" +
      "tdF8FAs97gYI0d8vY500sbpP74raU9FqngD4sea2NmQVKLMF69IDX0UobmQnyiRk\n" +
      "ogP5FyDTs8iXDvZ9tSHxWYWM59TyTYL9xeNIbXVfzHBckwTpU0/0PA8sl/dvMSl4\n" +
      "wJxrpkpRys9OWz0PX0F94nB/ZguAKhzgX2Q=\n" +
      "-----END CERTIFICATE-----";

  private static final String SERVER_CERT = "-----BEGIN CERTIFICATE-----\n" +
      "MIIDojCCAYoCCQD8V+nVg7ax2DANBgkqhkiG9w0BAQsFADANMQswCQYDVQQDDAJD\n" +
      "QTAeFw0xOTA5MTcyMTUxMjRaFw0yMjA3MDcyMTUxMjRaMBkxFzAVBgNVBAMMDmRv\n" +
      "bnRrbm93LmxvY2FsMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAs06D\n" +
      "5ZAAiQKKRCe0ZmeszwtQ9CIeHkA5hNQMthkxDcKqUSsyuxCbS5Q/cM70Yk39xP+j\n" +
      "QKvFte7KaHKqwTfPcpRCFna7Ba65bQCg14jB5BLB+8Pv9DhWyiYkhVLEpTSNHSn4\n" +
      "eoUwXseuj5NJeQ53qwAGY/neRnGjqAIDEKYoG30t5KdrmYw4XcXjYfaaKFRPe9Go\n" +
      "iqAUewdsNcWyDMQ/bAxnvoW+oqprj8BHaKMnZaqflSM80XK0V9UH4AXfQ5uoeXxQ\n" +
      "SNrkgMDOhQZYQKanVKdfmysLlSOxCEtep6iKXmI/1fJT7MePmirYY0BB0ukrIW/L\n" +
      "c/qQsM7wu3qAYy2iQQIDAQABMA0GCSqGSIb3DQEBCwUAA4ICAQClthNVQsuc9mWt\n" +
      "XDz19yn9EPSir1Okr/2GNs2DdZtoOWJa2VwKMnWC+wVfTafBGWR4lydDVKqnhgW1\n" +
      "7ezL9hn/8zi0cQX6Jm/bghHcDHP3MOXsKToZmo34A95LPdpX1TL/2ZJ9zU8EAiCG\n" +
      "C1bZUtAnS8jgwPY8kTri2+y3iGjY+XXxMK3O+537Luii4VCHOQxX9JBmRS2hr352\n" +
      "qYAuWgeG8VGm38KUOpTsVYAuTxL2myPyaVY+vtXsZg3nt9Q6n9iraEwGz+8dQqHW\n" +
      "mz5P7mbDZ4s3x7U424ByxJoAvJ3lcMd1f/wWiDzfce6t55F7e43h6F8B7aKQkEVD\n" +
      "r1D4GXsNbij4F4jBUfYxZZvpRNQ5ho5gdbigonHZeSMZnzTrq2urWhQxAxJTXyMw\n" +
      "X+Rjps7XJXbBG9ibvMVs6qNtPPpv/2lDqXnuuA8X4m3K13RzzxBWOd+UP73CYWXK\n" +
      "qYj6sC9mXC7/MYZXCnRzpP/0Rw4dW5axabXByPfDBPra1CiQDRxes6WXVV9ZO2rc\n" +
      "LgzrNKu8IQJvICQJJAa9dh5vk2TxvykEMqM8euh/TAFmeA/hwLJk3kl6zY+pmFV3\n" +
      "Cbc/n3jlOO4z9wbQkYX1SvPYk2rcBh2K9+W2Ux7GjBNx0hny7/9VsfDp2ytHX8gZ\n" +
      "Dsg8jKoiRXDRvw0hox67C509lJ07Qw==\n" +
      "-----END CERTIFICATE-----\n";

  @Test
  public void testPostgresUrl() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    PostgresHikariPoolConfigBean bean = new PostgresHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = PostgresHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.sslMode = PostgresSSLMode.REQUIRED;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
    Assert.assertEquals(1, bean.getEncryptionProperties().size());
    Assert.assertEquals("require", bean.getEncryptionProperties().get("sslmode"));
  }

  @Test
  public void testNonPostgresServerUrl() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    PostgresHikariPoolConfigBean bean = new PostgresHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = "jdbc:mysql://foo";
    bean.sslMode = PostgresSSLMode.REQUIRED;

    Assert.assertEquals(1, bean.validateConfigs(context, issues).size());
  }

  @Test
  public void testDisabled() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Mockito.mock(
        ConfigIssue.class));

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    PostgresHikariPoolConfigBean bean = new PostgresHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = PostgresHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.sslMode = PostgresSSLMode.DISABLED;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());

    Assert.assertEquals(1, bean.getEncryptionProperties().size());
    Assert.assertEquals("disable", bean.getEncryptionProperties().get("sslmode"));
  }

  @Test
  public void testVerifyServerCertificateWithCACertificate() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    PostgresHikariPoolConfigBean bean = new PostgresHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = PostgresHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.sslMode = PostgresSSLMode.VERIFY_CA;
    bean.serverPem = SERVER_CERT;
    bean.caPem = CA_CERT;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());

    Assert.assertEquals(3, bean.getEncryptionProperties().size());
    Assert.assertEquals("verify-ca", bean.getEncryptionProperties().get("sslmode"));
    Assert.assertTrue(bean.getEncryptionProperties().containsKey("sslcert"));
    Assert.assertTrue(new File((String) bean.getEncryptionProperties().get("sslcert")).exists());
    Assert.assertTrue(bean.getEncryptionProperties().containsKey("sslrootcert"));
    Assert.assertTrue(new File((String) bean.getEncryptionProperties().get("sslrootcert")).exists());
  }

  @Test
  public void testVerifyHostnameInUrl() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    PostgresHikariPoolConfigBean bean = new PostgresHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = PostgresHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.sslMode = PostgresSSLMode.VERIFY_FULL;
    bean.serverPem = SERVER_CERT;
    bean.caPem = CA_CERT;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());

    Assert.assertEquals(3, bean.getEncryptionProperties().size());
    Assert.assertEquals("verify-full", bean.getEncryptionProperties().get("sslmode"));
    Assert.assertTrue(bean.getEncryptionProperties().containsKey("sslcert"));
    Assert.assertTrue(new File((String) bean.getEncryptionProperties().get("sslcert")).exists());
    Assert.assertTrue(bean.getEncryptionProperties().containsKey("sslrootcert"));
    Assert.assertTrue(new File((String) bean.getEncryptionProperties().get("sslrootcert")).exists());
  }


  @Test
  public void testBlacklistedProperties() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    PostgresHikariPoolConfigBean bean = new PostgresHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = PostgresHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.sslMode = PostgresSSLMode.REQUIRED;
    ConnectionPropertyBean prop = new ConnectionPropertyBean();
    prop.key = "sslmode";
    prop.value = () -> "require";
    bean.driverProperties.add(prop);

    Assert.assertEquals(1, bean.validateConfigs(context, issues).size());
  }

  @Test
  public void testNoBlacklistedProperties() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    PostgresHikariPoolConfigBean bean = new PostgresHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = PostgresHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.sslMode = PostgresSSLMode.REQUIRED;
    ConnectionPropertyBean prop = new ConnectionPropertyBean();
    prop.key = "anything";
    prop.value = () -> "true";
    bean.driverProperties.add(prop);

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
  }

}
