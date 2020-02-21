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

public class TestSQLServerHikariPoolConfigBean {

  private static final String CERT = "-----BEGIN CERTIFICATE-----\n" +
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
  public void testSqlServerUrl() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
           .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    SQLServerHikariPoolConfigBean bean = new SQLServerHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = SQLServerHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.trustServerCertificate = true;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
    Assert.assertEquals(2, bean.getEncryptionProperties().size());
    Assert.assertEquals("true", bean.getEncryptionProperties().get("encrypt"));
    Assert.assertEquals("true", bean.getEncryptionProperties().get("trustServerCertificate"));
  }

  @Test
  public void testSqlServerUrl_noEncryption() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
           .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    SQLServerHikariPoolConfigBean bean = new SQLServerHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = SQLServerHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.encrypt = false;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
    Assert.assertEquals(0, bean.getEncryptionProperties().size());
  }

  @Test
  public void testNonSqlServerUrl() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    SQLServerHikariPoolConfigBean bean = new SQLServerHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = "jdbc:mysql://foo";
    bean.trustServerCertificate = true;

    Assert.assertEquals(1, bean.validateConfigs(context, issues).size());
  }

  @Test
  public void testNoTrustServerCertificateValidPEM() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    SQLServerHikariPoolConfigBean bean = new SQLServerHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = SQLServerHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.trustServerCertificate = false;
    bean.serverCertificatePem = CERT;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());

    Assert.assertEquals(4, bean.getEncryptionProperties().size());
    Assert.assertEquals("true", bean.getEncryptionProperties().get("encrypt"));
    Assert.assertEquals("false", bean.getEncryptionProperties().get("trustServerCertificate"));
    Assert.assertTrue(bean.getEncryptionProperties().containsKey("trustStore"));
    Assert.assertTrue(new File((String) bean.getEncryptionProperties().get("trustStore")).exists());
    Assert.assertTrue(bean.getEncryptionProperties().containsKey("trustStorePassword"));
  }

  @Test
  public void testNoTrustServerCertificateInvalidPEM() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    SQLServerHikariPoolConfigBean bean = new SQLServerHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = SQLServerHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.trustServerCertificate = false;
    bean.serverCertificatePem = "INVALID";

    Assert.assertEquals(1, bean.validateConfigs(context, issues).size());
  }

  @Test
  public void testVerifyHostnameInUrlFalse() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    SQLServerHikariPoolConfigBean bean = new SQLServerHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = SQLServerHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.trustServerCertificate = true;
    bean.verifyHostnameInUrl = false;
    bean.verifyHostname = "hostname";

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());

    Assert.assertEquals(3, bean.getEncryptionProperties().size());
    Assert.assertEquals("true", bean.getEncryptionProperties().get("encrypt"));
    Assert.assertEquals("true", bean.getEncryptionProperties().get("trustServerCertificate"));
    Assert.assertEquals("hostname", bean.getEncryptionProperties().get("hostNameInCertificate"));
  }

  @Test
  public void testBlacklistedProperties() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    SQLServerHikariPoolConfigBean bean = new SQLServerHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = SQLServerHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.trustServerCertificate = true;
    ConnectionPropertyBean prop = new ConnectionPropertyBean();
    prop.key = "encrypt";
    prop.value = () -> "true";
    bean.driverProperties.add(prop);

    Assert.assertEquals(1, bean.validateConfigs(context, issues).size());
  }

  @Test
  public void testNoBlacklistedProperties() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(ConfigIssue.class));

    List<Stage.ConfigIssue> issues =  new ArrayList<>();

    SQLServerHikariPoolConfigBean bean = new SQLServerHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.connectionString = SQLServerHikariPoolConfigBean.DEFAULT_CONNECTION_STRING_PREFIX;
    bean.trustServerCertificate = true;

    ConnectionPropertyBean prop = new ConnectionPropertyBean();
    prop.key = "anything";
    prop.value = () -> "true";
    bean.driverProperties.add(prop);

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
  }

}
