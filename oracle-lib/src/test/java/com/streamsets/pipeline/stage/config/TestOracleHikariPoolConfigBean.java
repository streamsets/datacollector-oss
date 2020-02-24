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

import java.util.ArrayList;
import java.util.List;

public class TestOracleHikariPoolConfigBean {

  private static final String CERT = "-----BEGIN CERTIFICATE-----\n" +
      "MIID9DCCAtygAwIBAgIBQjANBgkqhkiG9w0BAQUFADCBijELMAkGA1UEBhMCVVMx\n" +
      "EzARBgNVBAgMCldhc2hpbmd0b24xEDAOBgNVBAcMB1NlYXR0bGUxIjAgBgNVBAoM\n" +
      "GUFtYXpvbiBXZWIgU2VydmljZXMsIEluYy4xEzARBgNVBAsMCkFtYXpvbiBSRFMx\n" +
      "GzAZBgNVBAMMEkFtYXpvbiBSRFMgUm9vdCBDQTAeFw0xNTAyMDUwOTExMzFaFw0y\n" +
      "MDAzMDUwOTExMzFaMIGKMQswCQYDVQQGEwJVUzETMBEGA1UECAwKV2FzaGluZ3Rv\n" +
      "bjEQMA4GA1UEBwwHU2VhdHRsZTEiMCAGA1UECgwZQW1hem9uIFdlYiBTZXJ2aWNl\n" +
      "cywgSW5jLjETMBEGA1UECwwKQW1hem9uIFJEUzEbMBkGA1UEAwwSQW1hem9uIFJE\n" +
      "UyBSb290IENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuD8nrZ8V\n" +
      "u+VA8yVlUipCZIKPTDcOILYpUe8Tct0YeQQr0uyl018StdBsa3CjBgvwpDRq1HgF\n" +
      "Ji2N3+39+shCNspQeE6aYU+BHXhKhIIStt3r7gl/4NqYiDDMWKHxHq0nsGDFfArf\n" +
      "AOcjZdJagOMqb3fF46flc8k2E7THTm9Sz4L7RY1WdABMuurpICLFE3oHcGdapOb9\n" +
      "T53pQR+xpHW9atkcf3pf7gbO0rlKVSIoUenBlZipUlp1VZl/OD/E+TtRhDDNdI2J\n" +
      "P/DSMM3aEsq6ZQkfbz/Ilml+Lx3tJYXUDmp+ZjzMPLk/+3beT8EhrwtcG3VPpvwp\n" +
      "BIOqsqVVTvw/CwIDAQABo2MwYTAOBgNVHQ8BAf8EBAMCAQYwDwYDVR0TAQH/BAUw\n" +
      "AwEB/zAdBgNVHQ4EFgQUTgLurD72FchM7Sz1BcGPnIQISYMwHwYDVR0jBBgwFoAU\n" +
      "TgLurD72FchM7Sz1BcGPnIQISYMwDQYJKoZIhvcNAQEFBQADggEBAHZcgIio8pAm\n" +
      "MjHD5cl6wKjXxScXKtXygWH2BoDMYBJF9yfyKO2jEFxYKbHePpnXB1R04zJSWAw5\n" +
      "2EUuDI1pSBh9BA82/5PkuNlNeSTB3dXDD2PEPdzVWbSKvUB8ZdooV+2vngL0Zm4r\n" +
      "47QPyd18yPHrRIbtBtHR/6CwKevLZ394zgExqhnekYKIqqEX41xsUV0Gm6x4vpjf\n" +
      "2u6O/+YE2U+qyyxHE5Wd5oqde0oo9UUpFETJPVb6Q2cEeQib8PBAyi0i6KnF+kIV\n" +
      "A9dY7IHSubtCK/i8wxMVqfd5GtbA8mmpeJFwnDvm9rBEsHybl08qlax9syEwsUYr\n" +
      "/40NawZfTUU=\n" +
      "-----END CERTIFICATE-----";

  @Test
  public void testOracleBasic_doNotVerifyHostName() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Mockito.mock(
        ConfigIssue.class));

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.host = "foo";
    bean.ssid = "SDC";
    bean.serverCertificatePem = CERT;
    bean.verifyHostname = false;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
    Assert.assertEquals(6, bean.getEncryptionProperties().size());
    Assert.assertNotNull(bean.getEncryptionProperties().getProperty("javax.net.ssl.trustStoreType"));
    Assert.assertNotNull(bean.getEncryptionProperties().getProperty("javax.net.ssl.trustStore"));
    Assert.assertNotNull(bean.getEncryptionProperties().getProperty("javax.net.ssl.trustStorePassword"));
    Assert.assertNotNull(bean.getEncryptionProperties().getProperty("oracle.net.ssl_client_authentication"));
    Assert.assertNotNull(bean.getEncryptionProperties().getProperty("oracle.net.ssl_cipher_suites"));
    Assert.assertEquals("false", bean.getEncryptionProperties().getProperty("oracle.net.ssl_server_dn_match"));
  }

  @Test
  public void testOracleBasic_VerifyHostName() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Mockito.mock(
        ConfigIssue.class));

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.host = "foo";
    bean.ssid = "SDC";
    bean.serverCertificatePem = CERT;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
    Assert.assertEquals(6, bean.getEncryptionProperties().size());
    Assert.assertNotNull(bean.getEncryptionProperties().getProperty("javax.net.ssl.trustStoreType"));
    Assert.assertNotNull(bean.getEncryptionProperties().getProperty("javax.net.ssl.trustStore"));
    Assert.assertNotNull(bean.getEncryptionProperties().getProperty("javax.net.ssl.trustStorePassword"));
    Assert.assertNotNull(bean.getEncryptionProperties().getProperty("oracle.net.ssl_client_authentication"));
    Assert.assertNotNull(bean.getEncryptionProperties().getProperty("oracle.net.ssl_cipher_suites"));
    Assert.assertEquals("true", bean.getEncryptionProperties().getProperty("oracle.net.ssl_server_dn_match"));
  }

  @Test
  public void testBlacklistedProperties() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Mockito.mock(
        ConfigIssue.class));

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.host = "foo";
    bean.ssid = "SDC";
    ConnectionPropertyBean prop = new ConnectionPropertyBean();
    prop.key = "oracle.net.ssl_client_authentication";
    prop.value = () -> "true";
    bean.driverProperties.add(prop);
    bean.serverCertificatePem = CERT;

    Assert.assertEquals(1, bean.validateConfigs(context, issues).size());
  }

  @Test
  public void testNoBlacklistedProperties() {
    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Mockito.mock(
        ConfigIssue.class));

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.host = "foo";
    bean.ssid = "SDC";
    ConnectionPropertyBean prop = new ConnectionPropertyBean();
    prop.key = "anything";
    prop.value = () -> "true";
    bean.driverProperties.add(prop);
    bean.serverCertificatePem = CERT;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
  }

  @Test
  public void testConnectionString_basic() {
    String basicConnectionString = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=foo) (PORT=2484)) " +
        "(CONNECT_DATA= (SID=SDC)) (SECURITY=(SSL_SERVER_CERT_DN=)))";

    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Mockito.mock(
        ConfigIssue.class));

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.host = "foo";
    bean.port = 2484;
    bean.ssid = "SDC";
    bean.serverCertificatePem = CERT;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
    Assert.assertEquals(basicConnectionString, bean.getConnectionString());
  }

  @Test
  public void testConnectionString_DN() {
    String basicConnectionString = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=foo) (PORT=2484)) " +
        "(CONNECT_DATA= (SID=SDC)) (SECURITY=(SSL_SERVER_CERT_DN=CN=stf-oracle-11.c9v0wdsbqa7e.us-west-2.rds" +
        ".amazonaws.com,OU=RDS,O=Amazon.com,L=Seattle,ST=Washington,C=US)))";

    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Mockito.mock(
        ConfigIssue.class));

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.host = "foo";
    bean.port = 2484;
    bean.ssid = "SDC";
    bean.serverCertificatePem = CERT;
    bean.distinguishedName = "CN=stf-oracle-11.c9v0wdsbqa7e.us-west-2.rds.amazonaws.com,OU=RDS,O=Amazon.com," +
        "L=Seattle,ST=Washington,C=US";

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
    Assert.assertEquals(basicConnectionString, bean.getConnectionString());
  }

  @Test
  public void testConnectionString_verifyHostName() {
    String connectionString = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=foo) (PORT=2484)) " +
        "(CONNECT_DATA= (SID=SDC)) )";

    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Mockito.mock(
        ConfigIssue.class));

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.host = "foo";
    bean.port = 2484;
    bean.ssid = "SDC";
    bean.serverCertificatePem = CERT;
    bean.verifyHostname = false;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
    Assert.assertEquals(connectionString, bean.getConnectionString());
  }

  @Test
  public void testConnectionString_nonEncrypted() {
    String basicConnectionString = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=foo) (PORT=2484)) " +
        "(CONNECT_DATA= (SID=SDC)) (SECURITY=(SSL_SERVER_CERT_DN=)))";

    Stage.Context context = Mockito.mock(Stage.Context.class);
    Mockito.when(context.createConfigIssue(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(Mockito.mock(
        ConfigIssue.class));

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();
    bean = Mockito.spy(bean);
    Mockito.doReturn(new ArrayList<>()).when(bean).superValidateConfigs(Mockito.eq(context), Mockito.eq(issues));

    bean.host = "foo";
    bean.port = 2484;
    bean.ssid = "SDC";
    ConnectionPropertyBean prop = new ConnectionPropertyBean();
    prop.key = "anything";
    prop.value = () -> "true";
    bean.driverProperties.add(prop);
    bean.serverCertificatePem = CERT;

    Assert.assertEquals(0, bean.validateConfigs(context, issues).size());
    Assert.assertEquals(basicConnectionString, bean.getConnectionString());
  }

  @Test
  public void testGetConnectionStringTemplate() {
    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();

    String
        encryptionHostnameTemplate
        = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=%s) (PORT=%d)) %s";

    String hostnameTemplate = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp) (HOST=%s) (PORT=%d)) %s";

    Assert.assertEquals(encryptionHostnameTemplate, bean.getConnectionStringTemplate());

    bean.verifyHostname = false;
    Assert.assertEquals(encryptionHostnameTemplate, bean.getConnectionStringTemplate());

    bean.isEncryptedConnection = false;
    Assert.assertEquals(hostnameTemplate, bean.getConnectionStringTemplate());
  }

  @Test
  public void testConstructConnectionString_nonSecured_basic() {
    String expectedConnectionString = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp) (HOST=host-oracle.rds" +
        ".amazonaws.com) (PORT=1521)) (CONNECT_DATA= (SID=SDC)) )";

    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();
    bean.host = "host-oracle.rds.amazonaws.com";
    bean.port = 1521;
    bean.ssid = "SDC";
    bean.isEncryptedConnection = false;

    Assert.assertEquals(expectedConnectionString, bean.getConnectionString());
  }

  @Test
  public void testConstructConnectionString_secured_basic() {
    String expectedConnectionString = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=host-oracle.rds" +
        ".amazonaws.com) (PORT=1521)) (CONNECT_DATA= (SID=SDC)) )";

    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();
    bean.host = "host-oracle.rds.amazonaws.com";
    bean.port = 1521;
    bean.ssid = "SDC";
    bean.verifyHostname = false;

    Assert.assertEquals(expectedConnectionString, bean.getConnectionString());
  }

  @Test
  public void testConstructConnectionString_secured_verifyHostname() {
    String expectedConnectionString =
        "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps) (HOST=host-oracle.rds.amazonaws.com) (PORT=1521)) " +
            "(CONNECT_DATA= (SID=SDC)) (SECURITY=(SSL_SERVER_CERT_DN=amazonaws)))";

    OracleHikariPoolConfigBean bean = new OracleHikariPoolConfigBean();
    bean.host = "host-oracle.rds.amazonaws.com";
    bean.port = 1521;
    bean.ssid = "SDC";
    bean.distinguishedName = "amazonaws";

    Assert.assertEquals(expectedConnectionString, bean.getConnectionString());
  }
}