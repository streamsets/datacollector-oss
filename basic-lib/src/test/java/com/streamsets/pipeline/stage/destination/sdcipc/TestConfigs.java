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
package com.streamsets.pipeline.stage.destination.sdcipc;

import com.google.common.io.Files;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.sdk.ContextInfoCreator;
import com.streamsets.pipeline.stage.util.tls.TLSTestUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.security.KeyPair;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class TestConfigs {

  private void injectConfigsHttp(ForTestConfigs config) {
    config.appId = () -> "appId";
    config.connectionTimeOutMs = 100;
    config.readTimeOutMs = 200;
    config.hostPorts = Arrays.asList("localhost:10000");
    config.retriesPerBatch = 2;
    config.tlsConfigBean.tlsEnabled = false;
    config.tlsConfigBean.trustStoreFilePath = "";
    config.tlsConfigBean.trustStorePassword = () -> "";
    config.hostVerification = true;
  }

  @Test
  public void testCreateConnection() throws Exception {

    // HTTP
    HttpURLConnection conn = Mockito.mock(MockHttpURLConnection.class);
    ForTestConfigs config = new ForTestConfigs(conn);
    injectConfigsHttp(config);
    config.createConnection("localhost:10000");
    Mockito.verify(conn).setConnectTimeout(Mockito.eq(config.connectionTimeOutMs));
    Mockito.verify(conn).setReadTimeout(Mockito.eq(config.readTimeOutMs));
    Mockito.verify(conn).setRequestProperty(Mockito.eq(Constants.X_SDC_APPLICATION_ID_HEADER),
                                            Mockito.eq(config.appId.get()));

    // HTTPS
    HttpsURLConnection sconn = Mockito.mock(MockHttpsURLConnection.class);
    config = new ForTestConfigs(sconn);
    injectConfigsHttp(config);
    config.tlsConfigBean.tlsEnabled = true;
    config.createConnection("localhost:10000");
    Mockito.verify(sconn).setConnectTimeout(Mockito.eq(config.connectionTimeOutMs));
    Mockito.verify(sconn).setReadTimeout(Mockito.eq(config.readTimeOutMs));
    Mockito.verify(sconn).setRequestProperty(Mockito.eq(Constants.X_SDC_APPLICATION_ID_HEADER),
                                             Mockito.eq(config.appId.get()));
    Mockito.verify(sconn).setSSLSocketFactory(Mockito.any(SSLSocketFactory.class));
    Mockito.verify(sconn, Mockito.never()).setHostnameVerifier(Mockito.any(HostnameVerifier.class));

    //HTTPS with no host verification
    Mockito.reset(sconn);
    config = new ForTestConfigs(sconn);
    injectConfigsHttp(config);
    config.tlsConfigBean.tlsEnabled = true;
    config.hostVerification = false;
    config.createConnection("localhost:10000");
    Mockito.verify(sconn).setHostnameVerifier(Mockito.eq(ForTestConfigs.ACCEPT_ALL_HOSTNAME_VERIFIER));
  }

  private void injectConfigsHttps(ForTestConfigs config, String trustStoreFile, String trustStorePassword,
      boolean hostnameVerification) {
    config.appId = () -> "appId";
    config.connectionTimeOutMs = 100;
    config.readTimeOutMs = 200;
    config.hostPorts = Arrays.asList("localhost:10000");
    config.retriesPerBatch = 2;
    config.tlsConfigBean.tlsEnabled = true;
    config.tlsConfigBean.trustStoreFilePath = trustStoreFile;
    config.tlsConfigBean.trustStorePassword = () -> trustStorePassword;
    config.hostVerification = hostnameVerification;
  }

  private Stage.Context getContext() {
    return ContextInfoCreator.createTargetContext("i", false, OnRecordError.TO_ERROR);
  }
  @Test
  public void testCreateSSLSocketFactory() throws Exception {
    // create trust store
    File testDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDir.mkdirs());
    KeyPair kp = TLSTestUtils.generateKeyPair();
    Certificate cert1 = TLSTestUtils.generateCertificate("CN=Cert1", kp, 30);
    String truststoreFile = new File(testDir, "truststore.jks").toString();
    TLSTestUtils.createTrustStore(truststoreFile, "password", "cert1", cert1);

    ForTestConfigs target = new ForTestConfigs(null);
    injectConfigsHttps(target, truststoreFile, "password", true);
    List<Stage.ConfigIssue> issues = new LinkedList<>();
    target.tlsConfigBean.init(getContext(), "TLS", "tlsConfigBean.", issues);
    Assert.assertEquals(0, issues.size());
    SSLSocketFactory factory = target.createSSLSocketFactory(getContext());
    Assert.assertNotNull(factory);
  }

  @Test
  public void testValidateHostPorts() throws Exception {
    HttpURLConnection conn = Mockito.mock(MockHttpURLConnection.class);
    ForTestConfigs config = new ForTestConfigs(conn);
    injectConfigsHttp(config);
    config.hostPorts = Collections.emptyList();

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    // no hostports
    config.validateHostPorts(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // invalid hostport
    config.hostPorts = Arrays.asList("localhost");
    config.validateHostPorts(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // null hostport
    List<String> list = new ArrayList<>();
    list.add(null);
    config.hostPorts = list;
    config.validateHostPorts(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // invalid hostport
    config.hostPorts = Arrays.asList("localhost:-1");
    config.validateHostPorts(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // invalid hostport
    config.hostPorts = Arrays.asList("localhost:1000000");
    config.validateHostPorts(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // invalid hostport
    config.hostPorts = Arrays.asList("localhost:x");
    config.validateHostPorts(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // invalid host
    config.hostPorts = Collections.singletonList("WO**#&:10000");
    config.validateHostPorts(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // dup hostport
    config.hostPorts = Arrays.asList("localhost:10000", "localhost:10000");
    config.validateHostPorts(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // invalid ipv6 hostport
    config.hostPorts = Collections.singletonList("2001:0db8:0000:0000:0000:ff00:0042:8329:10000");
    config.validateHostPorts(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // good hostport
    config.hostPorts = Arrays.asList("localhost:10000", "localhost:10001");
    config.validateHostPorts(getContext(), issues);
    Assert.assertEquals(0, issues.size());

    // good ipv6 addresses
    config.hostPorts = Arrays.asList(
        "[2001:0db8:0000:0000:0000:ff00:0042:8329]:10000",
        "[2001:db8:0:0:0:ff00:43:8329]:10001",
        "[2001:db8::ff00:44:8329]:10002",
        "[::1]:10003"
    );
    config.validateHostPorts(getContext(), issues);
    Assert.assertEquals(0, issues.size());
  }

  @Test
  public void testValidateSecurity() throws Exception {
    File testDir = new File("target", UUID.randomUUID().toString());
    Assert.assertTrue(testDir.mkdirs());

    HttpURLConnection conn = Mockito.mock(MockHttpURLConnection.class);
    ForTestConfigs config = new ForTestConfigs(conn);
    injectConfigsHttps(config, "", "", true);

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    // no path is not a file
    config.tlsConfigBean.trustStoreFilePath = testDir.toString();
    config.validateSecurity(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // file does not exist
    config.tlsConfigBean.trustStoreFilePath = UUID.randomUUID().toString();
    config.validateSecurity(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // invalid trust store file
    File invalidStore = new File(testDir, "invalid.jks");
    Files.touch(invalidStore);
    config.tlsConfigBean.trustStoreFilePath = invalidStore.toString();
    config.validateSecurity(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // valid trust store file
    KeyPair kp = TLSTestUtils.generateKeyPair();
    Certificate cert1 = TLSTestUtils.generateCertificate("CN=Cert1", kp, 30);
    String trustStoreLocation = new File(testDir, "truststore.jks").toString();
    TLSTestUtils.createTrustStore(trustStoreLocation, "password", "cert1", cert1);

    config.tlsConfigBean.trustStoreFilePath = trustStoreLocation;
    config.tlsConfigBean.trustStorePassword = () -> "password";
    config.validateSecurity(getContext(), issues);
    Assert.assertEquals(0, issues.size());

    // valid trust store file, invalid password
    config.tlsConfigBean.trustStoreFilePath = trustStoreLocation;
    config.tlsConfigBean.trustStorePassword = () -> "invalid";
    config.validateSecurity(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();
  }

  @Test
  public void testValidateInPreviewMode() throws Exception {
    HttpURLConnection conn = Mockito.mock(MockHttpURLConnection.class);
    ForTestConfigs config = new ForTestConfigs(conn);
    injectConfigsHttp(config);
    // Mock that destination SDC PRC pipeline is not running.
    Mockito.when(conn.getResponseCode()).thenThrow(new ConnectException("Connection Refused"));

    // in Preview mode
    Stage.Context previewMode = ContextInfoCreator.createTargetContext(
        "SdcIpcDTargetInstance",
        true,
        OnRecordError.TO_ERROR
    );
    List<Stage.ConfigIssue> issues0 = config.init(previewMode);
    // should not validate the connectivity, so no issues
    Assert.assertTrue(issues0.isEmpty());

    // Start SDC but still destination SDC PRC pipeline is not running
    Stage.Context context = getContext();
    List<Stage.ConfigIssue> issues1 = config.init(context);
    Assert.assertEquals(1, issues1.size());
    Assert.assertTrue(issues1.get(0).toString().contains(Errors.IPC_DEST_15.name()));

    // destination SDC PRC pipeline is running
    Mockito.reset(conn);
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(conn.getHeaderField(Mockito.eq(Constants.X_SDC_PING_HEADER))).thenReturn(Constants.X_SDC_PING_VALUE);
    List<Stage.ConfigIssue> issues4 = config.init(getContext());
    Assert.assertEquals(0, issues4.size());
    Mockito.verify(conn).setRequestMethod(Mockito.eq("GET"));
    Mockito.verify(conn).setDefaultUseCaches(Mockito.eq(false));
  }

  @Test
  public void testValidateConnectivity() throws Exception {
    HttpURLConnection conn = Mockito.mock(MockHttpURLConnection.class);
    ForTestConfigs config = new ForTestConfigs(conn);
    injectConfigsHttp(config);

    List<Stage.ConfigIssue> issues = new ArrayList<>();

    // test HTTP_ACCEPTED
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    Mockito.when(conn.getHeaderField(Mockito.eq(Constants.X_SDC_PING_HEADER))).thenReturn(Constants.X_SDC_PING_VALUE);
    config.validateConnectivity(getContext(), issues);
    Assert.assertEquals(0, issues.size());
    Mockito.verify(conn).setRequestMethod(Mockito.eq("GET"));
    Mockito.verify(conn).setDefaultUseCaches(Mockito.eq(false));

    // test not HTTP_ACCEPTED
    Mockito.reset(conn);
    Mockito.when(conn.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
    config.validateConnectivity(getContext(), issues);
    Assert.assertEquals(1, issues.size());
    issues.clear();

    // test conn exception
    Mockito.reset(conn);
    Mockito.when(conn.getResponseCode()).thenThrow(new IOException());
    config.validateConnectivity(getContext(), issues);
    Assert.assertEquals(1, issues.size());
  }

}
