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

import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.lib.salesforce.DataType;
import com.streamsets.pipeline.lib.salesforce.ForceLookupConfigBean;
import com.streamsets.pipeline.lib.salesforce.ForceSDCFieldMapping;
import com.streamsets.pipeline.sdk.ProcessorRunner;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.StageRunner;
import com.streamsets.pipeline.stage.processor.lookup.ForceLookupDProcessor;
import com.streamsets.testing.NetworkUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;


// Skip these tests until Mock Server supports metadata
@Ignore
public class TestSalesforceLookupProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TestSalesforceLookupProcessor.class);
  private static final String username = "test@example.com";
  private static final String password = "p455w0rd";
  private static final String apiVersion = "37.0";
  private static final String listQuery = "SELECT Name FROM Account WHERE Id = '${record:value(\"[0]\")}'";
  private static final String mapQuery = "SELECT Name FROM Account WHERE Id = '${record:value(\"/id\")}'";
  private static final String queryReturnsNoRow = "SELECT Name FROM Account WHERE false";

  private int port;
  private String authEndpoint;
  private MockSalesforceApiServer mockServer;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

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


  @After
  public void tearDown() { mockServer.stop(); }


  private ForceLookupConfigBean createConfigBean() {
    ForceLookupConfigBean conf = new ForceLookupConfigBean();

    conf.username = () -> username;
    conf.password = () -> password;
    conf.authEndpoint = authEndpoint;
    conf.apiVersion = apiVersion;

    conf.useCompression = false;
    conf.showTrace = true;

    return conf;
  }

  @Test
  public void testEmptyBatch() throws Exception {
    ForceLookupDProcessor processor = new ForceLookupDProcessor();
    processor.forceConfig = createConfigBean();

    processor.forceConfig.fieldMappings = ImmutableList.of(new ForceSDCFieldMapping("Name", "/name"));
    processor.forceConfig.soqlQuery = mapQuery;

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(ForceLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    List<Record> emptyBatch = ImmutableList.of();
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(emptyBatch);
      Assert.assertEquals(0, output.getRecords().get("lane").size());
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testSingleRecordList() throws Exception {
    mockServer.sforceApi().query().returnResults()
        .withRow().withField("Id", "001000000000001").withField("Name", "Pat");

    ForceLookupDProcessor processor = new ForceLookupDProcessor();
    processor.forceConfig = createConfigBean();

    processor.forceConfig.fieldMappings = ImmutableList.of(new ForceSDCFieldMapping("Name", "[2]"));
    processor.forceConfig.soqlQuery = listQuery;

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(ForceLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("001000000000001"));
    fields.add(Field.create("abcd"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);

      Assert.assertNotEquals(null, record.get("[2]"));
      Assert.assertEquals("Pat", record.get("[2]").getValueAsString());
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testSingleRecordMap() throws Exception {
    mockServer.sforceApi().query().returnResults()
        .withRow().withField("Id", "001000000000001").withField("Name", "Pat");

    ForceLookupDProcessor processor = new ForceLookupDProcessor();
    processor.forceConfig = createConfigBean();

    processor.forceConfig.fieldMappings = ImmutableList.of(new ForceSDCFieldMapping("Name", "/name"));
    processor.forceConfig.soqlQuery = mapQuery;

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(ForceLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    fields.put("id", Field.create("001000000000001"));
    fields.put("blah", Field.create("abcd"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);

      Assert.assertNotEquals(null, record.get("/name"));
      Assert.assertEquals("Pat", record.get("/name").getValueAsString());
    } finally {
      processorRunner.runDestroy();
    }

  }

  @Test
  public void testNullFieldMap() throws Exception {
    mockServer.sforceApi().query().returnResults()
        .withRow().withField("Id", "001000000000001").withField("Name", null);

    ForceLookupDProcessor processor = new ForceLookupDProcessor();
    processor.forceConfig = createConfigBean();

    processor.forceConfig.fieldMappings = ImmutableList.of(new ForceSDCFieldMapping("Name", "/name"));
    processor.forceConfig.soqlQuery = mapQuery;

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(ForceLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    fields.put("id", Field.create("001000000000001"));
    fields.put("blah", Field.create("abcd"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);

      Assert.assertEquals(true, record.has("/name"));
      Assert.assertEquals(null, record.get("/name").getValueAsString());
    } finally {
      processorRunner.runDestroy();
    }

  }

  @Test
  public void testBadConnectionString() throws Exception {
    ForceLookupDProcessor processor = new ForceLookupDProcessor();
    processor.forceConfig = createConfigBean();
    processor.forceConfig.authEndpoint = "badhost:"+port;
    processor.forceConfig.fieldMappings = ImmutableList.of();

    ProcessorRunner runner = new ProcessorRunner.Builder(ForceLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(1, issues.size());
  }

  @Test
  public void testMissingColumnMappingList() throws Exception {
    mockServer.sforceApi().query().returnResults()
        .withRow().withField("Id", "001000000000001").withField("Name", "Pat");

    ForceLookupDProcessor processor = new ForceLookupDProcessor();
    processor.forceConfig = createConfigBean();

    processor.forceConfig.fieldMappings = ImmutableList.of(new ForceSDCFieldMapping("QQQ", "[2]"));
    processor.forceConfig.soqlQuery = listQuery;

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(ForceLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("001000000000001"));
    fields.add(Field.create("abcd"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    List<Record> outputRecords = processorRunner.runProcess(singleRecord).getRecords().get("lane");

    Assert.assertEquals("Pat", outputRecords.get(0).get("[2]").getValueAsMap().get("value").getValueAsString());
  }

  @Test
  public void testMissingColumnMappingMap() throws Exception {
    mockServer.sforceApi().query().returnResults()
        .withRow().withField("Id", "001000000000001").withField("Name", "Pat");

    ForceLookupDProcessor processor = new ForceLookupDProcessor();
    processor.forceConfig = createConfigBean();

    processor.forceConfig.fieldMappings = ImmutableList.of(new ForceSDCFieldMapping("QQQ", "[2]"));
    processor.forceConfig.soqlQuery = mapQuery;

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(ForceLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    fields.put("id", Field.create("001000000000001"));
    fields.put("blah", Field.create("abcd"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    List<Record> outputRecords = processorRunner.runProcess(singleRecord).getRecords().get("lane");

    Assert.assertEquals("Pat", outputRecords.get(0).get("/Name").getValueAsString());
  }

  @Test
  public void testCaseInsensitiveMap() throws Exception {
    mockServer.sforceApi().query().returnResults()
        .withRow().withField("Id", "001000000000001").withField("Name", "Pat");

    ForceLookupDProcessor processor = new ForceLookupDProcessor();
    processor.forceConfig = createConfigBean();

    processor.forceConfig.fieldMappings = ImmutableList.of(new ForceSDCFieldMapping("NAME", "/name"));
    processor.forceConfig.soqlQuery = mapQuery;

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(ForceLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    LinkedHashMap<String, Field> fields = new LinkedHashMap<>();
    fields.put("id", Field.create("001000000000001"));
    fields.put("blah", Field.create("abcd"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);

      Assert.assertNotEquals(null, record.get("/name"));
      Assert.assertEquals("Pat", record.get("/name").getValueAsString());
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testValidationForDefaultValue() throws Exception {
    ForceLookupDProcessor processor = new ForceLookupDProcessor();
    processor.forceConfig = createConfigBean();

    processor.forceConfig.fieldMappings = ImmutableList.of(
        new ForceSDCFieldMapping("Name", "[2]", "Pat", DataType.USE_SALESFORCE_TYPE)
    );
    processor.forceConfig.soqlQuery = listQuery;

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(ForceLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    List<Stage.ConfigIssue> issues = processorRunner.runValidateConfigs();
    for (Stage.ConfigIssue issue : issues) {
      LOG.info(issue.toString());
    }
    assertEquals(1, issues.size());
  }

  @Test
  public void testWrongDataTypeForDefaultValue() throws Exception {
    mockServer.sforceApi().query().returnResults(); // return empty result

    ForceLookupDProcessor processor = new ForceLookupDProcessor();
    processor.forceConfig = createConfigBean();

    processor.forceConfig.fieldMappings = ImmutableList.of(
        new ForceSDCFieldMapping("Name", "[2]", "Bob", DataType.INTEGER)
    );
    processor.forceConfig.soqlQuery = listQuery;

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(ForceLookupDProcessor.class, processor)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("001000000000001"));
    fields.add(Field.create("abcd"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(0, output.getRecords().get("lane").size());

      List<Record> errors = processorRunner.getErrorRecords();
      Assert.assertEquals(1, errors.size());
    } finally {
      processorRunner.runDestroy();
    }
  }

  @Test
  public void testDefaultValue() throws Exception {
    mockServer.sforceApi().query().returnResults(); // return empty result

    ForceLookupDProcessor processor = new ForceLookupDProcessor();
    processor.forceConfig = createConfigBean();

    processor.forceConfig.fieldMappings = ImmutableList.of(
        new ForceSDCFieldMapping("Name", "[2]", "Bob", DataType.STRING)
    );
    processor.forceConfig.soqlQuery = listQuery;

    ProcessorRunner processorRunner = new ProcessorRunner.Builder(ForceLookupDProcessor.class, processor)
        .addOutputLane("lane")
        .build();

    Record record = RecordCreator.create();
    List<Field> fields = new ArrayList<>();
    fields.add(Field.create("001000000000001"));
    fields.add(Field.create("abcd"));
    record.set(Field.create(fields));

    List<Record> singleRecord = ImmutableList.of(record);
    processorRunner.runInit();
    try {
      StageRunner.Output output = processorRunner.runProcess(singleRecord);
      Assert.assertEquals(1, output.getRecords().get("lane").size());

      record = output.getRecords().get("lane").get(0);

      Assert.assertNotEquals(null, record.get("[2]"));
      Assert.assertEquals("Bob", record.get("[2]").getValueAsString());
    } finally {
      processorRunner.runDestroy();
    }
  }
}
