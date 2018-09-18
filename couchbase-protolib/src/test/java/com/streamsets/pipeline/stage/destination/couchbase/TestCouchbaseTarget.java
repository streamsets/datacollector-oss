/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.stage.destination.couchbase;

import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.endpoint.kv.KeyValueStatus;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.kv.InsertResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.core.message.kv.RemoveResponse;
import com.couchbase.client.core.message.kv.ReplaceResponse;
import com.couchbase.client.core.message.kv.UpsertResponse;
import com.couchbase.client.core.message.kv.subdoc.BinarySubdocRequest;
import com.couchbase.client.core.message.kv.subdoc.simple.SimpleSubdocResponse;
import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.CouchbaseAsyncBucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.lib.couchbase.AuthenticationType;
import com.streamsets.pipeline.lib.couchbase.CouchbaseConnector;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;
import io.opentracing.Span;
import org.junit.After;
import org.junit.Test;
import rx.subjects.AsyncSubject;
import rx.subjects.Subject;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCouchbaseTarget {

  private static final String BUCKET = "myTestBucket";
  private static final String USERNAME = "myTestUser";
  private static final String PASSWORD = "myTestPassword";

  //private CouchbaseConnector connector;
  private static CouchbaseEnvironment ENV;

  private static CouchbaseTargetConfig getDefaultConfig() {
    CouchbaseTargetConfig config = new CouchbaseTargetConfig();
    config.couchbase.nodes = "localhost";
    config.couchbase.bucket = BUCKET;
    config.couchbase.kvTimeout = 2500;
    config.couchbase.connectTimeout = 5000;
    config.couchbase.disconnectTimeout = 25000;
    config.couchbase.tls.tlsEnabled = false;
    config.credentials.version = AuthenticationType.USER;
    config.credentials.userName = () -> USERNAME;
    config.credentials.userPassword = () -> PASSWORD;
    config.documentKeyEL = "myDocumentKey";
    config.documentTtlEL = "0";
    config.defaultWriteOperation = WriteOperationType.UPSERT;
    config.unsupportedOperation = UnsupportedOperationType.TOERROR;
    config.useCas = true;
    config.allowSubdoc = true;
    config.subdocPathEL = "";
    config.subdocOperationEL = "";
    config.replicateTo = ReplicateTo.NONE;
    config.persistTo = PersistTo.NONE;
    config.dataFormatConfig = new DataGeneratorFormatConfig();
    config.dataFormatConfig.charset = "UTF-8";
    config.dataFormat = DataFormat.JSON;

    return config;
  }

  private static TargetRunner getTargetRunner(CouchbaseTargetConfig config) {
    Target target = new CouchbaseTarget(config);
    return new TargetRunner.Builder(CouchbaseDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
  }

  private static TargetRunner getMockedTargetRunner(CouchbaseTargetConfig config, Class responseClass) {
    try {
      Target target = new CouchbaseTarget(config, getConnector(responseClass));
      return new TargetRunner.Builder(CouchbaseDTarget.class, target)
          .setOnRecordError(OnRecordError.TO_ERROR)
          .build();
    } catch (Exception e) {
      fail(e.getMessage());
      return null;
    }
  }

  private static TargetRunner getMockedTargetRunner(CouchbaseTargetConfig config) {
    return getMockedTargetRunner(config, SimpleSubdocResponse.class);
  }

  private static TargetRunner getMockedTargetRunner(Class responseClass) {
    return getMockedTargetRunner(getDefaultConfig(), responseClass);
  }

  private static List<Record> getTestRecord(String cdcOperation, String casHeader) {
    Record record = RecordCreator.create();
    Map<String, Field> map = new HashMap<>();
    map.put("myKey", Field.create("myValue"));
    record.set(Field.create(map));

    if (cdcOperation != null) {
      record.getHeader().setAttribute("sdc.operation.type", cdcOperation);
    }

    if (casHeader != null) {
      record.getHeader().setAttribute("couchbase.cas", casHeader);
    }

    List<Record> records = new ArrayList<>();
    records.add(record);

    return records;
  }

  private static List<Record> getTestRecord() {
    return getTestRecord(null, null);
  }

  private static List<Record> getTestRecord(String cdcOperation) {
    return getTestRecord(cdcOperation, null);
  }

  @SuppressWarnings("unchecked")
  private static CouchbaseConnector getConnector(Class responseClass) throws Exception {
    ENV = DefaultCouchbaseEnvironment.create();

    CouchbaseCore core = mock(CouchbaseCore.class);
    CouchbaseAsyncBucket asyncBucket = new CouchbaseAsyncBucket(core,
        ENV,
        BUCKET,
        USERNAME,
        PASSWORD,
        Collections.emptyList()
    );

    final CouchbaseRequest requestMock = mock(CouchbaseRequest.class);

    Subject<CouchbaseResponse, CouchbaseResponse> response = AsyncSubject.create();

    if(responseClass == SimpleSubdocResponse.class) {
      final BinarySubdocRequest subdocRequestMock = mock(BinarySubdocRequest.class);
      when(subdocRequestMock.span()).thenReturn(mock(Span.class));

      response.onNext(new SimpleSubdocResponse(ResponseStatus.SUCCESS,
          KeyValueStatus.SUCCESS.code(),
          BUCKET,
          Unpooled.EMPTY_BUFFER,
          subdocRequestMock,
          1234,
          null
      ));

      response.onCompleted();
    } else {

      Constructor con = responseClass.getConstructor(ResponseStatus.class,
          short.class,
          long.class,
          String.class,
          ByteBuf.class,
          MutationToken.class,
          CouchbaseRequest.class
      );

      response.onNext((CouchbaseResponse) con.newInstance(ResponseStatus.SUCCESS,
          KeyValueStatus.SUCCESS.code(),
          1234,
          BUCKET,
          Unpooled.EMPTY_BUFFER,
          null,
          requestMock
      ));
      response.onCompleted();
    }

    when(core.send(any(BinarySubdocRequest.class))).thenReturn(response);
    when(core.send(any())).thenReturn(response);
    when(requestMock.span()).thenReturn(mock(Span.class));

    CouchbaseConnector connector = mock(CouchbaseConnector.class);
    when(connector.getScheduler()).thenReturn(ENV.scheduler());
    when(connector.bucket()).thenReturn(asyncBucket);

    return connector;
  }

  @After
  public void tearDown() {
    if (ENV != null) {
      ENV.shutdown();
    }
  }

  @Test
  public void shouldFailNullDocumentKey() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.documentKeyEL = null;

    TargetRunner runner = getMockedTargetRunner(config);
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    runner.runDestroy();

    assertTrue(issues.stream().anyMatch(configIssue -> configIssue.toString().contains("COUCHBASE_07")));
  }

  @Test
  public void shouldFailNullDocumentFormat() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.dataFormat = null;

    TargetRunner runner = getMockedTargetRunner(config);
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    runner.runDestroy();

    assertTrue(issues.stream().anyMatch(configIssue -> configIssue.toString().contains("COUCHBASE_28")));
  }

  @Test
  public void shouldFailNullNodes() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.couchbase.nodes = null;

    TargetRunner runner = getTargetRunner(config);
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    runner.runDestroy();

    assertTrue(issues.stream().anyMatch(configIssue -> configIssue.toString().contains("COUCHBASE_29")));
  }

  @Test
  public void shouldFailNegativeKVTimeout() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.couchbase.kvTimeout = -1;

    TargetRunner runner = getTargetRunner(config);
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    runner.runDestroy();

    assertTrue(issues.stream().anyMatch(configIssue -> configIssue.toString().contains("COUCHBASE_30")));
  }

  @Test
  public void shouldFailNegativeConnectTimeout() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.couchbase.connectTimeout = -1;

    TargetRunner runner = getTargetRunner(config);
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    runner.runDestroy();

    assertTrue(issues.stream().anyMatch(configIssue -> configIssue.toString().contains("COUCHBASE_31")));
  }

  @Test
  public void shouldFailNegativeDisconnectTimeout() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.couchbase.disconnectTimeout = -1;

    TargetRunner runner = getTargetRunner(config);
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    runner.runDestroy();

    assertTrue(issues.stream().anyMatch(configIssue -> configIssue.toString().contains("COUCHBASE_32")));
  }

  @Test
  public void shouldFailNullAuthenticationMode() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.credentials.version = null;

    TargetRunner runner = getTargetRunner(config);
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    runner.runDestroy();

    assertTrue(issues.stream().anyMatch(configIssue -> configIssue.toString().contains("COUCHBASE_33")));
  }

  @Test
  public void shouldFailNullUserName() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.credentials.version = AuthenticationType.USER;
    config.credentials.userName = null;

    TargetRunner runner = getTargetRunner(config);
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    runner.runDestroy();

    assertTrue(issues.stream().anyMatch(configIssue -> configIssue.toString().contains("COUCHBASE_34")));
  }

  @Test
  public void shouldFailNullPassword() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.credentials.version = AuthenticationType.USER;
    config.credentials.userPassword = null;

    TargetRunner runner = getTargetRunner(config);
    List<Target.ConfigIssue> issues = runner.runValidateConfigs();
    runner.runDestroy();

    assertTrue(issues.stream().anyMatch(configIssue -> configIssue.toString().contains("COUCHBASE_35")));
  }

  @Test
  public void shouldWriteEmptyBatch() throws StageException {
    TargetRunner runner = getMockedTargetRunner(InsertResponse.class);

    runner.runInit();
    runner.runWrite(new ArrayList<>());
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldInsertSingleRecord() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.defaultWriteOperation = WriteOperationType.INSERT;

    TargetRunner runner = getMockedTargetRunner(config, InsertResponse.class);
    if(runner == null) return;

    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldUpsertSingleRecord() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.defaultWriteOperation = WriteOperationType.UPSERT;

    TargetRunner runner = getMockedTargetRunner(config, UpsertResponse.class);
    if(runner == null) return;

    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldUpdateSingleRecord() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.defaultWriteOperation = WriteOperationType.REPLACE;

    TargetRunner runner = getMockedTargetRunner(config, ReplaceResponse.class);
    if(runner == null) return;

    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldDeleteSingleRecord() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.defaultWriteOperation = WriteOperationType.DELETE;

    TargetRunner runner = getMockedTargetRunner(config, RemoveResponse.class);
    if(runner == null) return;

    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldFailUnsupportedSubdocOperation() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.subdocPathEL = "myPath";
    config.subdocOperationEL = "badOperation";
    config.unsupportedOperation = UnsupportedOperationType.TOERROR;

    TargetRunner runner = getMockedTargetRunner(config);

    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("COUCHBASE_14", errors.get(0).getHeader().getErrorCode());
  }

  @Test
  public void shouldFailUnsupportedSubdocCDCOperation() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.subdocPathEL = "myPath";
    config.subdocOperationEL = "badOperation";

    TargetRunner runner = getMockedTargetRunner(config);

    runner.runInit();
    runner.runWrite(getTestRecord("999"));
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("COUCHBASE_14", errors.get(0).getHeader().getErrorCode());
  }

  @Test
  public void shouldInsertSubdocRecord() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.subdocPathEL = "myPath";
    config.subdocOperationEL = "INSERT";

    TargetRunner runner = getMockedTargetRunner(config);

    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldUpsertSubdocRecord() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.defaultWriteOperation = WriteOperationType.DELETE;
    config.subdocPathEL = "myPath";
    config.subdocOperationEL = "UPSERT";

    TargetRunner runner = getMockedTargetRunner(config);

    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldUpdateSubdocRecord() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.subdocPathEL = "myPath";
    config.subdocOperationEL = "REPLACE";

    TargetRunner runner = getMockedTargetRunner(config);

    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldDeleteSubdocRecord() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.subdocPathEL = "myPath";
    config.subdocOperationEL = "DELETE";

    TargetRunner runner = getMockedTargetRunner(config);

    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldCdcInsert() throws StageException {
    TargetRunner runner = getMockedTargetRunner(InsertResponse.class);

    runner.runInit();
    runner.runWrite(getTestRecord(String.valueOf(OperationType.INSERT_CODE)));
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldCdcUpsert() throws StageException {
    TargetRunner runner = getMockedTargetRunner(UpsertResponse.class);

    runner.runInit();
    runner.runWrite(getTestRecord(String.valueOf(OperationType.UPSERT_CODE)));
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldCdcUpdate() throws StageException {
    TargetRunner runner = getMockedTargetRunner(ReplaceResponse.class);

    runner.runInit();
    runner.runWrite(getTestRecord(String.valueOf(OperationType.UPDATE_CODE)));
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldCdcDelete() throws StageException {
    TargetRunner runner = getMockedTargetRunner(RemoveResponse.class);

    runner.runInit();
    runner.runWrite(getTestRecord(String.valueOf(OperationType.DELETE_CODE)));
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldUseDefaultOperationForUnsupportedCdcOperation() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.defaultWriteOperation = WriteOperationType.REPLACE;
    config.unsupportedOperation = UnsupportedOperationType.DEFAULT;

    TargetRunner runner = getMockedTargetRunner(config, ReplaceResponse.class);
    if(runner == null) return;

    runner.runInit();
    runner.runWrite(getTestRecord("999"));
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldFailUnsupportedCdcOperation() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.unsupportedOperation = UnsupportedOperationType.TOERROR;

    TargetRunner runner = getMockedTargetRunner(config);
    runner.runInit();
    runner.runWrite(getTestRecord("999"));
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("COUCHBASE_09", errors.get(0).getHeader().getErrorCode());
  }

  @Test
  public void shouldFailNegativeTTL() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.documentTtlEL = "${-1}";

    TargetRunner runner = getMockedTargetRunner(config);
    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("COUCHBASE_36", errors.get(0).getHeader().getErrorCode());
  }

  @Test
  public void shouldEmptyTTL() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.documentTtlEL = "";

    TargetRunner runner = getMockedTargetRunner(config, UpsertResponse.class);
    if(runner == null) return;

    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldFailUnparsableTTL() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.documentTtlEL = "${<>}";
    config.defaultWriteOperation = WriteOperationType.REPLACE;


    TargetRunner runner = getMockedTargetRunner(config);
    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("COUCHBASE_17", errors.get(0).getHeader().getErrorCode());
  }

  @Test
  public void shouldFailEmptyDocumentKey() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.documentKeyEL = "${NULL}";

    TargetRunner runner = getMockedTargetRunner(config);
    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("COUCHBASE_07", errors.get(0).getHeader().getErrorCode());
  }

  @Test
  public void shouldFailUnparsableDocumentKey() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.documentKeyEL = "${<>}";

    TargetRunner runner = getMockedTargetRunner(config);
    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("COUCHBASE_16", errors.get(0).getHeader().getErrorCode());
  }

  @Test
  public void shouldFailUnparsableCdcOperation() throws StageException {
    TargetRunner runner = getMockedTargetRunner(getDefaultConfig());
    runner.runInit();
    runner.runWrite(getTestRecord("${<>}"));
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("COUCHBASE_08", errors.get(0).getHeader().getErrorCode());
  }

  @Test
  public void shouldFailUnparsableSubdocOperation() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.subdocPathEL = "myPath";
    config.subdocOperationEL = "${<>}";

    TargetRunner runner = getMockedTargetRunner(config);
    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("COUCHBASE_13", errors.get(0).getHeader().getErrorCode());
  }

  @Test
  public void shouldFailUnparsableSubdocPath() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.subdocPathEL = "${<>}";

    TargetRunner runner = getMockedTargetRunner(config);
    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("COUCHBASE_18", errors.get(0).getHeader().getErrorCode());
  }

  @Test
  public void shouldFailUnparsableCASHeader() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.useCas = true;

    TargetRunner runner = getMockedTargetRunner(config);
    runner.runInit();
    runner.runWrite(getTestRecord(null, "${<>}"));
    runner.runDestroy();

    List<Record> errors = runner.getErrorRecords();
    assertEquals(1, errors.size());
    assertEquals("COUCHBASE_15", errors.get(0).getHeader().getErrorCode());
  }

  @Test
  public void shouldSkipCASHeader() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.useCas = false;

    TargetRunner runner = getMockedTargetRunner(config, UpsertResponse.class);
    if(runner == null) return;

    runner.runInit();
    runner.runWrite(getTestRecord(null, "${<>}"));
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }

  @Test
  public void shouldSkipSubdocOperation() throws StageException {
    CouchbaseTargetConfig config = getDefaultConfig();
    config.subdocPathEL = "";
    config.subdocOperationEL = "${<>}";

    TargetRunner runner = getMockedTargetRunner(config, UpsertResponse.class);
    if(runner == null) return;

    runner.runInit();
    runner.runWrite(getTestRecord());
    runner.runDestroy();

    assertTrue(runner.getErrorRecords().isEmpty());
    assertTrue(runner.getErrors().isEmpty());
  }
}