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
package com.streamsets.pipeline.stage.destination.elasticsearch;

import com.google.common.collect.ImmutableMap;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.util.ThreadUtil;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.config.elasticsearch.ElasticsearchTargetConfig;
import com.streamsets.pipeline.stage.config.elasticsearch.Errors;
import com.streamsets.pipeline.stage.connection.elasticsearch.SecurityConfig;
import com.streamsets.pipeline.stage.connection.elasticsearch.SecurityMode;
import com.streamsets.pipeline.stage.elasticsearch.common.ElasticsearchBaseIT;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

public class ElasticSearchTargetIT extends ElasticsearchBaseIT {

  @BeforeClass
  public static void setUp() throws Exception {
    ElasticsearchBaseIT.setUp();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    ElasticsearchBaseIT.cleanUp();
  }

  @Test
  public void testValidations() throws Exception {
    ElasticsearchTargetConfig conf = new ElasticsearchTargetConfig();
    conf.connection.serverUrl = "";
    conf.timeDriver = "${time:now()}";
    conf.timeZoneID = "UTC";
    conf.indexTemplate = "${record:value('/index')x}";
    conf.typeTemplate = "${record:nonExistentFunction()}";
    conf.docIdTemplate = "";
    conf.parentIdTemplate = "";
    conf.routingTemplate = "";
    conf.charset = "UTF-8";
    conf.defaultOperation = ElasticsearchOperationType.INDEX;
    conf.connection.useSecurity= false;
    conf.connection.securityConfig = new SecurityConfig();
    conf.rawAdditionalProperties =  "{\n}";

    Target target = new ElasticsearchTarget(conf);
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(3, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_00.name()));
    Assert.assertTrue(issues.get(1).toString().contains(Errors.ELASTICSEARCH_03.name()));
    Assert.assertTrue(issues.get(2).toString().contains(Errors.ELASTICSEARCH_06.name()));

    conf.connection.serverUrl = "x:";
    conf.timeDriver = "${time:now()}";
    conf.timeZoneID = "UTC";
    conf.indexTemplate = "x";
    conf.typeTemplate = "x";
    conf.docIdTemplate = "";
    conf.parentIdTemplate = "";
    conf.routingTemplate = "";
    conf.charset = "UTF-8";
    conf.defaultOperation = ElasticsearchOperationType.INDEX;
    conf.connection.useSecurity = false;
    conf.connection.securityConfig = new SecurityConfig();
    conf.rawAdditionalProperties =  "{\n}";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_07.name()));

    conf.connection.serverUrl = "localhost";
    conf.timeDriver = "${time:now()}";
    conf.timeZoneID = "UTC";
    conf.indexTemplate = "x";
    conf.typeTemplate = "x";
    conf.docIdTemplate = "";
    conf.parentIdTemplate = "";
    conf.routingTemplate = "";
    conf.charset = "UTF-8";
    conf.defaultOperation = ElasticsearchOperationType.INDEX;
    conf.connection.useSecurity = false;
    conf.connection.securityConfig = new SecurityConfig();
    conf.rawAdditionalProperties =  "{\n}";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_43.name()));

    conf.connection.serverUrl = "localhost";
    conf.connection.port = "9200";
    conf.timeDriver = "${time:now()}";
    conf.timeZoneID = "UTC";
    conf.indexTemplate = "x";
    conf.typeTemplate = "x";
    conf.docIdTemplate = "";
    conf.parentIdTemplate = "${record:value('/parent')x}";
    conf.routingTemplate = "${record:nonExistentFunction()}";
    conf.charset = "UTF-8";
    conf.defaultOperation = ElasticsearchOperationType.INDEX;
    conf.connection.useSecurity = false;
    conf.connection.securityConfig= new SecurityConfig();
    conf.rawAdditionalProperties =  "{\n}";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(2, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_27.name()));
    Assert.assertTrue(issues.get(1).toString().contains(Errors.ELASTICSEARCH_30.name()));

    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;
    conf.timeDriver = "${time:now()}";
    conf.timeZoneID = "UTC";
    conf.indexTemplate = "${record:value('/index')}";
    conf.typeTemplate = "${record:value('/type')}";
    conf.docIdTemplate = "docId";
    conf.parentIdTemplate = "";
    conf.routingTemplate = "";
    conf.charset = "UTF-8";
    conf.defaultOperation = ElasticsearchOperationType.UPDATE;
    conf.connection.useSecurity = false;
    conf.connection.securityConfig = new SecurityConfig();
    conf.rawAdditionalProperties =  "{\n\"_retry_on_conflict\"}";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_34.name()));

    conf.rawAdditionalProperties =  "{\n\"_retry_on_conflict\":3,}";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_34.name()));

    conf.rawAdditionalProperties =  "{${record:value('/text')}}";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_34.name()));
  }

  @Test
  public void testAdditionalPropertiesValidationWithRecordLabel() throws Exception {
    ElasticsearchTargetConfig conf = new ElasticsearchTargetConfig();
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;
    conf.timeDriver = "${time:now()}";
    conf.timeZoneID = "UTC";
    conf.indexTemplate = "${record:value('/index')}";
    conf.typeTemplate = "${record:value('/type')}";
    conf.docIdTemplate = "docId";
    conf.parentIdTemplate = "";
    conf.routingTemplate = "";
    conf.charset = "UTF-8";
    conf.defaultOperation = ElasticsearchOperationType.UPDATE;
    conf.connection.useSecurity = false;
    conf.connection.securityConfig = new SecurityConfig();
    conf.rawAdditionalProperties =  "{}";

    Target target = new ElasticsearchTarget(conf);
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());

    conf.rawAdditionalProperties =  "{\"_index\":${record:value(\'/text\')}}";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());

    conf.rawAdditionalProperties =  "{\"_index\":record:value(\'/text\'),\"_retry_on_conflict\":3}}";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_34.name()));

    conf.rawAdditionalProperties =  "{${record:value(\'/text\')}}";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_34.name()));
  }

  @Test
  public void testAdditionalPropertiesValidationWithoutRecordLabel() throws Exception {
    ElasticsearchTargetConfig conf = new ElasticsearchTargetConfig();
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;
    conf.timeDriver = "${time:now()}";
    conf.timeZoneID = "UTC";
    conf.indexTemplate = "${record:value('/index')}";
    conf.typeTemplate = "${record:value('/type')}";
    conf.docIdTemplate = "docId";
    conf.parentIdTemplate = "";
    conf.routingTemplate = "";
    conf.charset = "UTF-8";
    conf.defaultOperation = ElasticsearchOperationType.UPDATE;
    conf.connection.useSecurity = false;
    conf.connection.securityConfig = new SecurityConfig();
    conf.rawAdditionalProperties =  "{}";

    Target target = new ElasticsearchTarget(conf);
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());

    conf.rawAdditionalProperties =  "{\"_retry_on_conflict\":3}";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(0, issues.size());

    conf.rawAdditionalProperties =  "{\"_index\":record:value(\'/text\'),\"_retry_on_conflict\":3}}";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_34.name()));
  }

  @Test
  public void testCredentialValue() {
    ElasticsearchTargetConfig conf = new ElasticsearchTargetConfig();
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;
    conf.timeDriver = "${time:now()}";
    conf.timeZoneID = "UTC";
    conf.indexTemplate = "${YYYY()}";
    conf.typeTemplate = "${record:value('/type')}";
    conf.docIdTemplate = "";
    conf.charset = "UTF-8";
    conf.defaultOperation = ElasticsearchOperationType.INDEX;
    conf.rawAdditionalProperties =  "{\n}";
    conf.connection.useSecurity = true;
    conf.connection.securityConfig.securityMode = SecurityMode.BASIC;
    // Test for blank credentials using security
    conf.connection.securityConfig.securityUser = () -> "";
    conf.connection.securityConfig.securityPassword = () -> "";

    Target target = new ElasticsearchTarget(conf);
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_20.name()));

    // Test for null user name using security
    conf.connection.securityConfig.securityUser = () -> null;
    conf.connection.securityConfig.securityPassword = () -> "";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_40.name()));

    // Test for null password and invalid user name using security
    conf.connection.securityConfig.securityUser = () -> "";
    conf.connection.securityConfig.securityPassword = () -> null;

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_40.name()));

    // Test for user name without password format and blank password field
    conf.connection.securityConfig.securityUser = () -> "elastic";
    conf.connection.securityConfig.securityPassword = () -> "";

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_39.name()));
  }

  private Target createTarget() {
    return createTarget("", "", "");
  }

  private Target createTarget(String docId, String parent, String routing) {
    return createTarget(
        "${time:now()}",
        "${record:value('/index')}",
        docId,
        ElasticsearchOperationType.INDEX,
        parent,
        routing
    );
  }
  private ElasticsearchTarget createTarget(String timeDriver, String indexEL, String docIdEL, ElasticsearchOperationType op,
                                           String parent, String routing) {
    ElasticsearchTargetConfig conf = new ElasticsearchTargetConfig();
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;
    conf.timeDriver = timeDriver;
    conf.timeZoneID = "UTC";
    conf.indexTemplate = indexEL;
    conf.typeTemplate = "${record:value('/type')}";
    conf.docIdTemplate = docIdEL;
    conf.parentIdTemplate = parent;
    conf.routingTemplate = routing;
    conf.charset = "UTF-8";
    conf.defaultOperation = op;
    conf.connection.useSecurity = false;
    conf.connection.securityConfig = new SecurityConfig();
    conf.rawAdditionalProperties =  "{\n\"_retry_on_conflict\":1\n}";

    return new ElasticsearchTarget(conf);
  }

  @Test
  public void testWriteRecords() throws Exception {
    Target target = createTarget();
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    try {
      runner.runInit();
      List<Record> records = new ArrayList<>();
      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of("a", Field.create("Hello"),
                                               "index", Field.create("i"), "type", Field.create("t"))));
      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("Bye"),
                                               "index", Field.create("i"), "type", Field.create("t"))));
      records.add(record1);
      records.add(record2);
      runner.runWrite(records);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());


      prepareElasticSearchServerForQueries();

      Set<Map> expected = new HashSet<>();
      expected.add(ImmutableMap.of("a", "Hello", "index", "i", "type", "t"));
      expected.add(ImmutableMap.of("a", "Bye", "index", "i", "type", "t"));

      SearchResponse response = esServer.client().prepareSearch("i").setTypes("t")
                                        .setSearchType(SearchType.DEFAULT).execute().actionGet();
      SearchHit[] hits = response.getHits().getHits();
      Assert.assertEquals(2, hits.length);
      Set<Map> got = new HashSet<>();
      got.add(hits[0].getSource());
      got.add(hits[1].getSource());

      Assert.assertEquals(expected, got);

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWriteRecordsOnErrorDiscard() throws Exception {
    Target target = createTarget();
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).setOnRecordError(OnRecordError.DISCARD).build();
    try {
      runner.runInit();
      List<Record> records = new ArrayList<>();
      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of("a", Field.create("Hello"),
                                               "index", Field.create("II"), "type", Field.create("t"))));
      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("Bye"),
                                               "index", Field.create("ii"), "type", Field.create("t"))));
      records.add(record1);
      records.add(record2);
      runner.runWrite(records);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());


      prepareElasticSearchServerForQueries();

      Set<Map> expected = new HashSet<>();
      expected.add(ImmutableMap.of("a", "Bye", "index", "ii", "type", "t"));

      SearchResponse response = esServer.client().prepareSearch("ii").setTypes("t")
                                        .setSearchType(SearchType.DEFAULT).execute().actionGet();
      SearchHit[] hits = response.getHits().getHits();
      Assert.assertEquals(1, hits.length);
      Set<Map> got = new HashSet<>();
      got.add(hits[0].getSource());

      Assert.assertEquals(expected, got);

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWriteRecordsOnErrorToError() throws Exception {
    Target target = createTarget();
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).setOnRecordError(OnRecordError.TO_ERROR).build();
    try {
      runner.runInit();
      List<Record> records = new ArrayList<>();
      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of("a", Field.create("Hello"),
                                               "index", Field.create("III"), "type", Field.create("t"))));
      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("Bye"),
                                               "index", Field.create("iii"), "type", Field.create("t"))));
      records.add(record1);
      records.add(record2);
      runner.runWrite(records);
      Assert.assertEquals(1, runner.getErrorRecords().size());
      Assert.assertEquals("Hello", runner.getErrorRecords().get(0).get("/a").getValueAsString());
      Assert.assertTrue(runner.getErrors().isEmpty());


      prepareElasticSearchServerForQueries();

      Set<Map> expected = new HashSet<>();
      expected.add(ImmutableMap.of("a", "Bye", "index", "iii", "type", "t"));

      SearchResponse response = esServer.client().prepareSearch("iii").setTypes("t")
                                        .setSearchType(SearchType.DEFAULT).execute().actionGet();
      SearchHit[] hits = response.getHits().getHits();
      Assert.assertEquals(1, hits.length);
      Set<Map> got = new HashSet<>();
      got.add(hits[0].getSource());

      Assert.assertEquals(expected, got);

    } finally {
      runner.runDestroy();
    }
  }

  @Test(expected = StageException.class)
  public void testWriteRecordsOnErrorStopPipeline() throws Exception {
    Target target = createTarget();
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).setOnRecordError(OnRecordError.STOP_PIPELINE).build();
    try {
      runner.runInit();
      List<Record> records = new ArrayList<>();
      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of("a", Field.create("Hello"),
                                               "index", Field.create("IIII"), "type", Field.create("t"))));
      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("Bye"),
                                               "index", Field.create("iiii"), "type", Field.create("t"))));
      records.add(record1);
      records.add(record2);
      runner.runWrite(records);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testJsonParseError() throws Exception {
    Target target = createTarget("${record:value('/index')}", "", "");
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();

    try {
      runner.runInit();
      List<Record> records = new ArrayList<>();
      Record record1 = RecordCreator.create();
      // The backslash should cause a JsonParseException which in turn makes Elasticsearch RestClient
      // throw a ResponseException. Since the HTTP request entirely failed, the entire batch should be
      // sent to error.
      record1.set(Field.create(ImmutableMap.of("a", Field.create("Hello"),
          "index", Field.create("\\01234"), "type", Field.create("t"))));
      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("Bye"),
          "index", Field.create("iii"), "type", Field.create("t"))));
      records.add(record1);
      records.add(record2);
      runner.runWrite(records);
      Assert.assertEquals(2, runner.getErrorRecords().size());
      Assert.assertEquals("Hello", runner.getErrorRecords().get(0).get("/a").getValueAsString());
      Assert.assertEquals("Bye", runner.getErrorRecords().get(1).get("/a").getValueAsString());
      Assert.assertTrue(runner.getErrors().isEmpty());
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTimeDriverNow() throws Exception {
    ElasticsearchTargetConfig conf = new ElasticsearchTargetConfig();
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;
    conf.timeDriver = "${time:now()}";
    conf.timeZoneID = "UTC";
    conf.indexTemplate = "${YYYY()}";
    conf.typeTemplate = "${record:value('/type')}";
    conf.docIdTemplate = "";
    conf.charset = "UTF-8";
    conf.defaultOperation = ElasticsearchOperationType.INDEX;
    conf.connection.useSecurity = false;
    conf.connection.securityConfig = new SecurityConfig();
    conf.rawAdditionalProperties =  "{\n}";

    ElasticsearchTarget target = new ElasticsearchTarget(conf);
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      Date timeNow = target.setBatchTime();
      Date timeGot = target.getRecordTime(record);
      Assert.assertEquals(timeNow, timeGot);
      ELVars elVars = runner.getContext().createELVars();
      RecordEL.setRecordInContext(elVars, record);
      String index = target.getRecordIndex(elVars, record);
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.setTime(timeNow);
      Assert.assertEquals(Integer.toString(calendar.get(Calendar.YEAR)), index);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testTimeDriverValue() throws Exception {
    ElasticsearchTarget target = createTarget(
        "${record:value('/')}",
        "${YYYY()}",
        "",
        ElasticsearchOperationType.INDEX,
        "",
        ""
    );
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    runner.runInit();
    try {
      Record record = RecordCreator.create();
      Date timeNow = target.setBatchTime();
      record.set(Field.createDatetime(timeNow));
      ThreadUtil.sleep(10);
      target.setBatchTime();
      Date timeGot = target.getRecordTime(record);
      Assert.assertEquals(timeNow, timeGot);
      ELVars elVars = runner.getContext().createELVars();
      RecordEL.setRecordInContext(elVars, record);
      String index = target.getRecordIndex(elVars, record);
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.setTime(timeNow);
      Assert.assertEquals(Integer.toString(calendar.get(Calendar.YEAR)), index);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testWriteRecordsNow() throws Exception {
    ElasticsearchTarget target = createTarget(
            "${time:now()}",
            "${YYYY()}",
            "",
            ElasticsearchOperationType.INDEX,
            "",
            ""
    );
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    try {
      runner.runInit();
      List<Record> records = new ArrayList<>();
      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of("a", Field.create("Hello"),
          "type", Field.create("t"))));
      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("Bye"),
          "type", Field.create("t"))));
      records.add(record1);
      records.add(record2);
      runner.runWrite(records);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());

      Date batchTime = target.getBatchTime();
      Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      calendar.setTime(batchTime);
      String index = Integer.toString(calendar.get(Calendar.YEAR));

      prepareElasticSearchServerForQueries();

      Set<Map> expected = new HashSet<>();
      expected.add(ImmutableMap.of("a", "Hello", "type", "t"));
      expected.add(ImmutableMap.of("a", "Bye",  "type", "t"));

      SearchResponse response = esServer.client().prepareSearch(index).setTypes("t")
          .setSearchType(SearchType.DEFAULT).execute().actionGet();
      SearchHit[] hits = response.getHits().getHits();
      Assert.assertEquals(2, hits.length);
      Set<Map> got = new HashSet<>();
      got.add(hits[0].getSource());
      got.add(hits[1].getSource());

      Assert.assertEquals(expected, got);

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testInvalidUrisAndSecurityUser() throws Exception {
    ElasticsearchTargetConfig conf = new ElasticsearchTargetConfig();
    conf.timeDriver = "${time:now()}";
    conf.timeZoneID = "UTC";
    conf.indexTemplate = "${YYYY()}";
    conf.typeTemplate = "${record:value('/type')}";
    conf.docIdTemplate = "";
    conf.charset = "UTF-8";
    conf.defaultOperation = ElasticsearchOperationType.INDEX;
    conf.connection.useSecurity = false;
    conf.connection.securityConfig = new SecurityConfig();
    conf.rawAdditionalProperties = "{}";

    // Invalid url
    conf.connection.serverUrl = "127.0.0.1:NOT_A_NUMBER";

    ElasticsearchTarget target = new ElasticsearchTarget(conf);
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_07.name()));

    // Invalid port number
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + Integer.MAX_VALUE;

    target = new ElasticsearchTarget(conf);
    runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_08.name()));

  }

  @Test
  public void testNonIndexOperationWithoutDocId() throws Exception {
    ElasticsearchTargetConfig conf = new ElasticsearchTargetConfig();
    conf.connection.serverUrl = "127.0.0.1";
    conf.connection.port = "" + esHttpPort;
    conf.timeDriver = "${time:now()}";
    conf.timeZoneID = "UTC";
    conf.indexTemplate = "${YYYY()}";
    conf.typeTemplate = "${record:value('/type')}";
    conf.docIdTemplate = ""; // empty document ID expression
    conf.charset = "UTF-8";
    conf.defaultOperation = ElasticsearchOperationType.CREATE;
    conf.connection.useSecurity = false;
    conf.connection.securityConfig = new SecurityConfig();
    conf.rawAdditionalProperties =  "{\n}";

    ElasticsearchTarget target = new ElasticsearchTarget(conf);
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    List<Stage.ConfigIssue> issues = runner.runValidateConfigs();
    Assert.assertEquals(1, issues.size());
    Assert.assertTrue(issues.get(0).toString().contains(Errors.ELASTICSEARCH_19.name()));
  }

  @Test
  public void testUpsertRecords() throws Exception {
    // Use the index field as document ID.
    Target target = createTarget(
        "${time:now()}",
        "${record:value('/index')}",
        "docId",
        ElasticsearchOperationType.INDEX,
        "",
        ""
    );
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    try {
      runner.runInit();
      List<Record> records = new ArrayList<>();
      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of("a", Field.create("Old"),
          "index", Field.create("j"), "type", Field.create("t"))));
      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("New"),
          "index", Field.create("j"), "type", Field.create("t"))));
      records.add(record1);
      records.add(record2);
      runner.runWrite(records);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());

      prepareElasticSearchServerForQueries();

      // First record must be replaced by second record: "Old" => "New".
      Set<Map> expected = new HashSet<>();
      expected.add(ImmutableMap.of("a", "New", "index", "j", "type", "t"));

      SearchResponse response = esServer.client().prepareSearch("j").setTypes("t")
          .setSearchType(SearchType.DEFAULT).execute().actionGet();
      SearchHit[] hits = response.getHits().getHits();
      Assert.assertEquals(1, hits.length);
      Set<Map> got = new HashSet<>();
      got.add(hits[0].getSource());

      Assert.assertEquals(expected, got);

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testParentIDAndRouting() throws Exception {
    Target target = createTarget("", "${record:value('/parent')}", "${record:value('/routing')}");
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();

    // Create mappings for parent on the child type on index
    String mapping = "{ \"mappings\": { \"parent\": {}, \"child\": { \"_parent\": { \"type\": \"parent\" }}}}";
    esServer.client().admin().indices().prepareCreate("i").setSource(mapping).execute().actionGet();

    try {
      runner.runInit();
      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of("index", Field.create("i"),
              "type", Field.create("child"), "parent", Field.create("parent1"), "routing", Field.create("routing1"))));
      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("index", Field.create("i"),
              "type", Field.create("child"), "parent", Field.create("parent2"), "routing", Field.create(""))));
      runner.runWrite(Arrays.asList(record1, record2));
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());

      prepareElasticSearchServerForQueries();

      Set<Map> expectedSource = new HashSet<Map>(Arrays.asList(
              ImmutableMap.of("parent", "parent1", "routing", "routing1", "index", "i", "type", "child"),
              ImmutableMap.of("parent", "parent2", "routing", "", "index", "i", "type", "child")
      ));
      SearchResponse response = esServer.client().prepareSearch("i").setTypes("child")
              .setSearchType(SearchType.DEFAULT).execute().actionGet();
      SearchHit[] hits = response.getHits().getHits();
      Assert.assertEquals(2, hits.length);

      Set<Map> actualSource = new HashSet<Map>(Arrays.asList(hits[0].getSource(), hits[1].getSource()));
      Assert.assertEquals(expectedSource, actualSource);

      Set<Map> actualFields = new HashSet<>();
      for (SearchHit hit : hits) {
        Map<String, String> fieldMap = new HashMap<>();
        for (Map.Entry<String, SearchHitField> entry : hit.getFields().entrySet()) {
          fieldMap.put(entry.getKey().toString(), entry.getValue().getValue().toString());
        }
        actualFields.add(fieldMap);
      }
      Set<Map> expectedFields = new HashSet<Map>(Arrays.asList(
              ImmutableMap.of("_parent", "parent1", "_routing", "routing1"),
              ImmutableMap.of("_parent", "parent2", "_routing", "parent2")
      ));
      Assert.assertEquals(expectedFields, actualFields);
    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testMergeRecords() throws Exception {
    // Use the index field as document ID.
    Target target = createTarget(
            "${time:now()}",
            "${record:value('/index')}",
            "docId",
            ElasticsearchOperationType.MERGE,
            "",
            ""
    );
    TargetRunner runner = new TargetRunner.Builder(ElasticSearchDTarget.class, target).build();
    try {
      runner.runInit();
      List<Record> records = new ArrayList<>();
      Record record1 = RecordCreator.create();
      record1.set(Field.create(ImmutableMap.of("a", Field.create("Old"), // field to be changed
              "nested", Field.create(ImmutableMap.of("one", Field.create("uno"), // nested unchanged
                      "two", Field.create("dos"),      // nested changed
                      "three", Field.create("tres"))), // nested left alone
              "existing", Field.create("not touched"), // left alone
              "index", Field.create("j"), "type", Field.create("t")))); // index and mapping
      Record record2 = RecordCreator.create();
      record2.set(Field.create(ImmutableMap.of("a", Field.create("New"), // field changed
              "nested", Field.create(ImmutableMap.of("one", Field.create("uno"), // nested unchanged
                      "two", Field.create("duo"),          // nested changed
                      "four", Field.create("quattour"))),  // nested new field added
              "new", Field.create("fresh"),                // new field added
              "index", Field.create("j"), "type", Field.create("t")))); // index and mapping
      records.add(record1);
      records.add(record2);
      runner.runWrite(records);
      Assert.assertTrue(runner.getErrorRecords().isEmpty());
      Assert.assertTrue(runner.getErrors().isEmpty());

      prepareElasticSearchServerForQueries();

      // Second record must be merged into first record: "New" replaces "Old", etc.
      Map expected = ImmutableMap.builder().put("a", "New")  // field changed
              .put("existing", "not touched") // untouched fields left alone
              .put("nested", ImmutableMap.of( // can merge nested fields as well
                 "one", "uno",                // nested and unchanged
                 "two", "duo",                // nested and changed
                 "three", "tres",             // nested and left alone
                 "four", "quattour"           // nested new field added
              ))
              .put("new", "fresh")            // new field added
              .put("index", "j")
              .put("type", "t").build();

      SearchResponse response = esServer.client().prepareSearch("j").setTypes("t")
              .setSearchType(SearchType.DEFAULT).execute().actionGet();
      SearchHit[] hits = response.getHits().getHits();
      Assert.assertEquals(1, hits.length);
      Map got = hits[0].getSource();

      Assert.assertEquals(expected, got);

    } finally {
      runner.runDestroy();
    }
  }

  @Test
  public void testAddAdditionalProperties() {
    String additionalProperties = "{\"_retry_on_conflict\":1}";
    ElasticsearchTarget target = createTarget("${time:now()}",
        "${record:value('/index')}",
        "docId",
        ElasticsearchOperationType.UPDATE,
        "",
        ""
    );

    String expected = ",\"_retry_on_conflict\":1";

    Assert.assertEquals(expected, target.addAdditionalProperties(additionalProperties));
  }
}
