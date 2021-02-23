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
package com.streamsets.pipeline.stage.origin.salesforce;

import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.ForceRepeatQuery;
import com.streamsets.pipeline.lib.salesforce.ForceSourceConfigBean;
import com.streamsets.pipeline.lib.salesforce.connection.SalesforceConnection;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.testing.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.sham.salesforce.MockSalesforceApiServer;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSalesforceSource {
  private static final Logger LOG = LoggerFactory.getLogger(TestSalesforceSource.class);
  private static final String username = "test@example.com";
  private static final String password = "p455w0rd";
  private static final String apiVersion = "37.0";
  private static final int maxBatchSize = 1000;
  private static final int maxWaitTime = 1000;
  private static final String initialOffset = "000000000000000";
  private static final String offsetColumn = "Id";
  private static final String query = "SELECT Id, Name FROM Account WHERE Id > '${offset}' ORDER BY Id";
  private static final String SOQL_QUERY_MUST_INCLUDE = "SOQL query must include";
  public static final String ERROR_PARSING_SOQL_QUERY = "Error parsing SOQL query";

  private int port;
  private String authEndpoint;
  private MockSalesforceApiServer mockServer;

  // Set up mock server to respond to connection attempts
  @Before
  public void setUp() throws NoSuchAlgorithmException, KeyManagementException {
    SSLContext sc = SSLContext.getInstance("TLS");
    sc.init(null, new TrustManager[] { new TrustAllX509TrustManager() }, new java.security.SecureRandom());
    HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    HttpsURLConnection.setDefaultHostnameVerifier( new HostnameVerifier(){
      public boolean verify(String string,SSLSession ssls) {
        return true;
      }
    });

    port = NetworkUtils.getRandomPort();
    mockServer = new MockSalesforceApiServer(port);

    authEndpoint = "localhost:"+port;
  }

  // Skip until Mock Server supports metadata
  @After
  public void tearDown() {
    mockServer.stop();
  }

  @Test
  public void testMissingWhereClause() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.soqlQuery = "SELECT Id, Name FROM Account ORDER BY Id";
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(SOQL_QUERY_MUST_INCLUDE));
  }

  @Test
  public void testWrongWhereClause() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.soqlQuery = "SELECT Id, Name FROM Account WHERE Name = 'foo' ORDER BY Id";
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(SOQL_QUERY_MUST_INCLUDE));
  }

  @Test
  public void testMissingOrderByClause() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.soqlQuery = "SELECT Id, Name FROM Account WHERE Id > '${offset}'";
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(SOQL_QUERY_MUST_INCLUDE));
  }

  @Test
  public void testWrongOrderByClause() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.soqlQuery = "SELECT Id, Name FROM Account WHERE Id > '${offset}' ORDER BY Name";
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(SOQL_QUERY_MUST_INCLUDE));
  }

  @Test
  public void testMultipleOrderByFields() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.soqlQuery = "SELECT Id, Name FROM Account WHERE Id > '${offset}' ORDER BY Id, Name";
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(0, issues.size());
  }

  @Test
  public void testMultipleOrderByFieldsWrongOrder() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.soqlQuery = "SELECT Id, Name FROM Account WHERE Id > '${offset}' ORDER BY Name, Id";
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(SOQL_QUERY_MUST_INCLUDE));
  }

  @Test
  public void testMissingWhereAndOrderByClause() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.soqlQuery = "SELECT Id, Name FROM Account";
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(SOQL_QUERY_MUST_INCLUDE));
  }

  @Test
  public void testPKChunkingOrderByClause() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.bulkConfig.usePKChunking = true;
    conf.soqlQuery = "SELECT Id, Name FROM Account ORDER BY Id";
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(Errors.FORCE_31.getMessage()));
  }

  @Test
  public void testPKChunkingWhereClause() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.bulkConfig.usePKChunking = true;
    conf.soqlQuery = "SELECT Id, Name FROM Account WHERE Id > '${offset}'";
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(Errors.FORCE_32.getMessage()));
  }

  @Test
  public void testPKChunkingRepeatIncremental() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.bulkConfig.usePKChunking = true;
    conf.soqlQuery = "SELECT Id, Name FROM Account";
    conf.repeatQuery = ForceRepeatQuery.INCREMENTAL;
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(Errors.FORCE_33.getMessage()));
  }

  @Test
  public void testNeitherQueryNorStreamingEnabled() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.queryExistingData = false;
    conf.subscribeToStreaming = false;
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("A configuration is invalid"));
  }

  @Test
  public void testStreamingNoPushTopic() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.queryExistingData = false;
    conf.subscribeToStreaming = true;
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains("A configuration is invalid"));
  }

  private ForceSourceConfigBean getForceSourceConfig() {
    ForceSourceConfigBean conf = new ForceSourceConfigBean();

    conf.connection = new SalesforceConnection();
    conf.connection.username = () -> username;
    conf.connection.password = () -> password;
    conf.connection.authEndpoint = authEndpoint;
    conf.connection.apiVersion = apiVersion;
    conf.basicConfig.maxBatchSize = maxBatchSize;

    conf.queryExistingData = true;
    conf.soqlQuery = query;
    conf.initialOffset = initialOffset;
    conf.offsetColumn = offsetColumn;
    conf.useBulkAPI = false;
    conf.repeatQuery = ForceRepeatQuery.NO_REPEAT;

    conf.subscribeToStreaming = false;
    conf.pushTopic = null;
    conf.basicConfig.maxWaitTime = maxWaitTime;

    conf.useCompression = false;
    conf.showTrace = true;

    return conf;
  }

  // SDC-12932 - The Salesforce Origin fails and gives cryptic error message
  @Test
  public void testSoqlQueryParseError() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.soqlQuery = "FirstName, LastName, Email, LeadSource FROM Contact";
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
    assertTrue(issues.get(0).toString().contains(ERROR_PARSING_SOQL_QUERY));
  }

  // SDC-12935 - Valid SOQL queries are giving validation errors
  @Test
  public void testSoqlQueryObjectName() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.soqlQuery = "select contact.id, firstname, lastname, email from Contact " +
        "where Contact.Id <> '0036g000002s3cqAAA' and Account.Id = '0016g0000036KRCAA2' order by Contact.Id";
    conf.disableValidation = true;
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());
  }
}
