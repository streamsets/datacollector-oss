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

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.salesforce.Errors;
import com.streamsets.pipeline.lib.salesforce.ForceRepeatQuery;
import com.streamsets.pipeline.lib.salesforce.ForceSourceConfigBean;
import com.streamsets.pipeline.sdk.SourceRunner;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.testing.NetworkUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

  private int port;
  private String authEndpoint;
  private MockSalesforceApiServer mockServer;

  // Skip until Mock Server supports metadata
  @Ignore
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
  @Ignore
  @After
  public void tearDown() {
    mockServer.stop();
  }

  // Skip until Mock Server supports metadata
  @Ignore
  private void testAPI(ForceSource origin, String secondOffset) throws Exception {
    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 2);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(2, parsedRecords.size());
      assertEquals("recordId:001000000000002", output.getNewOffset());

      // Check that the remaining rows in the initial cursor are read.
      output = runner.runProduce(output.getNewOffset(), 100);
      parsedRecords = output.getRecords().get("lane");
      assertEquals(2, parsedRecords.size());
      assertEquals(secondOffset, output.getNewOffset());
    } finally {
      runner.runDestroy();
    }
  }

  // Skip until Mock Server supports metadata
  @Ignore
  private void addFourRowsBulk() {
    LinkedHashMap<String, String> record1 = new LinkedHashMap<>();
    record1.put("Name", "Pat");
    LinkedHashMap<String, String> record2 = new LinkedHashMap<>();
    record2.put("Name", "Arvind");
    LinkedHashMap<String, String> record3 = new LinkedHashMap<>();
    record3.put("Name", "Adam");
    LinkedHashMap<String, String> record4 = new LinkedHashMap<>();
    record4.put("Name", "Natty");

    LinkedHashMap<String,LinkedHashMap<String,String>> initFourRecords = new LinkedHashMap<>();
    initFourRecords.put("001000000000001", record1);
    initFourRecords.put("001000000000002", record2);
    initFourRecords.put("001000000000003", record3);
    initFourRecords.put("001000000000004", record4);

    mockServer.asyncApi().insert(initFourRecords);
  }

  // Skip until Mock Server supports metadata
  @Ignore
  @Test
  public void testBulkAPINoRepeat() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.useBulkAPI = true;
    conf.repeatQuery = ForceRepeatQuery.NO_REPEAT;

    addFourRowsBulk();

    testAPI(new ForceSource(conf), null);
  }

  // Skip until Mock Server supports metadata
  @Ignore
  @Test
  public void testBulkAPIFull() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.useBulkAPI = true;
    conf.repeatQuery = ForceRepeatQuery.FULL;

    addFourRowsBulk();

    testAPI(new ForceSource(conf), "recordId:"+initialOffset);
  }

  // Skip until Mock Server supports metadata
  @Ignore
  @Test
  public void testBulkAPIIncremental() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.useBulkAPI = true;
    conf.repeatQuery = ForceRepeatQuery.INCREMENTAL;

    addFourRowsBulk();

    testAPI(new ForceSource(conf), "recordId:001000000000004");
  }

  // Skip until Mock Server supports metadata
  @Ignore
  private void addFourRowsSoap() {
    mockServer.sforceApi().query().returnResults()
        .withRow().withField("Id", "001000000000001").withField("Name", "Pat")
        .withRow().withField("Id", "001000000000002").withField("Name", "Arvind")
        .withRow().withField("Id", "001000000000003").withField("Name", "Adam")
        .withRow().withField("Id", "001000000000004").withField("Name", "Natty");
  }

  // Skip until Mock Server supports metadata
  @Ignore
  @Test
  public void testSoapAPINoRepeat() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.repeatQuery = ForceRepeatQuery.NO_REPEAT;

    addFourRowsSoap();

    testAPI(new ForceSource(conf), null);
  }

  // Skip until Mock Server supports metadata
  @Ignore
  @Test
  public void testSoapAPIFull() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.repeatQuery = ForceRepeatQuery.FULL;

    addFourRowsSoap();

    testAPI(new ForceSource(conf), "recordId:"+initialOffset);
  }

  // Skip until Mock Server supports metadata
  @Ignore
  @Test
  public void testSoapAPIIncremental() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.repeatQuery = ForceRepeatQuery.INCREMENTAL;

    addFourRowsSoap();

    testAPI(new ForceSource(conf), "recordId:001000000000004");
  }

  // Skip until Mock Server supports metadata
  @Ignore
  @Test
  public void testBadConnectionString() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.authEndpoint = "badhost:"+port;
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(1, issues.size());
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
    conf.usePKChunking = true;
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
    conf.usePKChunking = true;
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
    conf.usePKChunking = true;
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

  // Skip until Mock Server supports metadata
  @Ignore
  @Test
  public void testEmptyResultSetBulk() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.useBulkAPI = true;
    conf.subscribeToStreaming = false;
    ForceSource origin = new ForceSource(conf);

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    runner.runInit();

    try {
      // Check that existing rows are loaded.
      StageRunner.Output output = runner.runProduce(null, 1000);
      Map<String, List<Record>> recordMap = output.getRecords();
      List<Record> parsedRecords = recordMap.get("lane");

      assertEquals(0, parsedRecords.size());
      assertEquals(null, output.getNewOffset());
    } finally {
      runner.runDestroy();
    }
  }

  // Skip until Mock Server supports metadata
  @Ignore
  @Test
  public void testStreaming() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.queryExistingData = false;
    conf.subscribeToStreaming = true;
    conf.pushTopic = "Test";
    ForceSource origin = new ForceSource(conf);

    mockServer.sforceApi().query().returnResults()
        .withRow().withField("Id", "001000000000001").withField("Query", "SELECT Id, Name FROM Account");

    SourceRunner runner = new SourceRunner.Builder(ForceDSource.class, origin)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    assertEquals(0, issues.size());
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

  // Skip until Mock Server supports metadata
  @Ignore
  @Test
  public void testStreamingBadPushTopic() throws Exception {
    ForceSourceConfigBean conf = getForceSourceConfig();
    conf.queryExistingData = false;
    conf.subscribeToStreaming = true;
    conf.pushTopic = "Test";
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

    conf.username = () -> username;
    conf.password = () -> password;
    conf.authEndpoint = authEndpoint;
    conf.apiVersion = apiVersion;
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
}
