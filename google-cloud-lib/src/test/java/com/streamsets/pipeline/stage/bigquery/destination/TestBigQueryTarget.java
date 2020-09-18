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
package com.streamsets.pipeline.stage.bigquery.destination;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.FileRef;
import com.streamsets.pipeline.api.OnRecordError;
import com.streamsets.pipeline.api.ProtoConfigurableEntity;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.googlecloud.BigQueryCredentialsConfig;
import com.streamsets.pipeline.sdk.RecordCreator;
import com.streamsets.pipeline.sdk.TargetRunner;
import com.streamsets.pipeline.stage.bigquery.lib.BigQueryDelegate;
import com.streamsets.pipeline.stage.bigquery.lib.Errors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@RunWith(PowerMockRunner.class)
@PrepareForTest({
    BigQuery.class,
    BigQueryTarget.class,
    InsertAllResponse.class,
    BigQueryDelegate.class,
    BigQueryError.class,
    Table.class,
    Credentials.class,
    BigQueryCredentialsConfig.class
})
@PowerMockIgnore({
    "jdk.internal.reflect.*"
})
public class TestBigQueryTarget {

  private BigQuery bigQuery = PowerMockito.mock(BigQuery.class);

  private static final SimpleDateFormat DATE_FORMAT = BigQueryTarget.createSimpleDateFormat(BigQueryTarget.YYYY_MM_DD);
  private static final SimpleDateFormat TIME_FORMAT  = BigQueryTarget.createSimpleDateFormat(BigQueryTarget.HH_MM_SS_SSSSSS);
  private static final SimpleDateFormat  DATE_TIME_FORMAT = BigQueryTarget.createSimpleDateFormat(BigQueryTarget.YYYY_MM_DD_T_HH_MM_SS_SSSSSS);

  @Before
  public void setup() {
    PowerMockito.replace(
        MemberMatcher.method(BigQueryCredentialsConfig.class, "getCredentials", Stage.Context.class, List.class)
    ).with((proxy,method,args) -> PowerMockito.mock(Credentials.class));
  }

  private TargetRunner createAndRunner(BigQueryTargetConfig config, List<Record> records) throws Exception {
    Target target = new BigQueryTarget(config);
    TargetRunner runner = new TargetRunner.Builder(BigQueryDTarget.class, target)
        .setOnRecordError(OnRecordError.TO_ERROR)
        .build();
    runner.runInit();
    Whitebox.setInternalState(target, "bigQuery", bigQuery);
    try {
      runner.runWrite(records);
    } finally {
      runner.runDestroy();
    }
    return runner;
  }


  private void mockBigQueryInsertAllRequest(Answer<InsertAllResponse> insertAllResponseAnswer) {
    PowerMockito.doAnswer(insertAllResponseAnswer).when(bigQuery).insertAll(Mockito.any(InsertAllRequest.class));
    PowerMockito.doAnswer((Answer<Table>) invocationOnMock -> {
      TableId tableId = (TableId) invocationOnMock.getArguments()[0];
      return tableId.equals(TableId.of("correctDataset", "correctTable")) ?
          Mockito.mock(Table.class) : null;
    }).when(bigQuery).getTable(Mockito.any(TableId.class));
  }

  @SuppressWarnings("unchecked")
  private Field createField(Object object) {
    Field field;
    if (object instanceof Map) {
      Map<String, Object> mapObject = (Map<String, Object>)object;
      return Field.create(mapObject.entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> createField(e.getValue()))));
    } else if (object instanceof List) {
      List<Object> listObject = (List<Object>)object;
      return Field.create(listObject.stream().map(this::createField).collect(Collectors.toList()));
    } else if (object instanceof Boolean) {
      field = Field.create((Boolean) object);
    } else if (object instanceof Character) {
      field = Field.create((Character) object);
    } else if (object instanceof Byte) {
      field = Field.create((Byte) object);
    } else if (object instanceof Short) {
      field = Field.create((Short) object);
    } else if (object instanceof Integer) {
      field = Field.create((Integer) object);
    } else if (object instanceof Long) {
      field = Field.create((Long) object);
    } else if (object instanceof Float) {
      field = Field.create((Float) object);
    } else if (object instanceof Double) {
      field = Field.create((Double) object);
    } else if (object instanceof Date) {
      field = Field.createDatetime((Date) object);
    } else if (object instanceof BigDecimal) {
      field = Field.create((BigDecimal) object);
    } else if (object instanceof String) {
      field = Field.create((String) object);
    } else if (object instanceof byte[]) {
      field = Field.create((byte[]) object);
    } else if (object instanceof FileRef){
      field = Field.create((FileRef)object);
    } else {
      throw new IllegalArgumentException(Utils.format("Cannot convert object type '{}' to field", object.getClass()));
    }
    return field;
  }

  private Record createRecord(Map<String, Object> values) {
    Record record = RecordCreator.create();
    record.set(
        Field.create(values.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> createField(e.getValue()))))
    );
    return record;
  }


  @Test
  public void testCorrectImplicitMapping() throws Exception {
    List<Record> records = new ArrayList<>();

    records.add(createRecord(ImmutableMap.of("a", "1", "b", 1, "c", 1.0)));
    records.add(createRecord(ImmutableMap.of("a", "2", "b", 2, "c", 2.0)));
    records.add(createRecord(ImmutableMap.of("a", "3", "b", 3, "c", 3.0)));

    mockBigQueryInsertAllRequest(invocationOnMock -> {
      InsertAllRequest insertAllRequest = (InsertAllRequest)invocationOnMock.getArguments()[0];

      List<InsertAllRequest.RowToInsert> rows = insertAllRequest.getRows();

      final AtomicInteger recordIdx = new AtomicInteger(0);
      rows.forEach(row -> {
        int idx = recordIdx.getAndIncrement();
        Record record = records.get(idx);
        Map<String, ?> rowContent = row.getContent();
        record.get().getValueAsMap().forEach((k, v) -> Assert.assertEquals(v.getValue(), rowContent.get(k)));
      });

      InsertAllResponse response = PowerMockito.mock(InsertAllResponse.class);
      Mockito.doReturn(Collections.emptyMap()).when(response).getInsertErrors();
      Mockito.doReturn(false).when(response).hasErrors();
      return response;
    });

    BigQueryTargetConfigBuilder configBuilder = new BigQueryTargetConfigBuilder();

    createAndRunner(configBuilder.build(), records);
  }

  @Test
  public void testDateTimeAndByteFields() throws Exception {
    Record record = RecordCreator.create();
    Map<String, Field> rootField = new LinkedHashMap<>();
    Map<String, Object> expectedContentMap = new LinkedHashMap<>();

    Date currentDate = new Date();

    String sampleBytes = "sample";

    rootField.put("dateField", Field.create(Field.Type.DATE, currentDate));
    expectedContentMap.put("dateField", DATE_FORMAT.format(currentDate));

    rootField.put("timeField", Field.create(Field.Type.TIME, currentDate));
    expectedContentMap.put("timeField", TIME_FORMAT.format(currentDate));

    rootField.put("datetimeField", Field.create(Field.Type.DATETIME, currentDate));
    expectedContentMap.put("datetimeField", DATE_TIME_FORMAT.format(currentDate));

    rootField.put("bytesField", Field.create(Field.Type.BYTE_ARRAY, sampleBytes.getBytes()));
    expectedContentMap.put("bytesField", Base64.getEncoder().encodeToString(sampleBytes.getBytes()));

    record.set(Field.create(rootField));

    mockBigQueryInsertAllRequest(invocationOnMock -> {
      InsertAllRequest request = (InsertAllRequest) invocationOnMock.getArguments()[0];
      InsertAllRequest.RowToInsert rowToInsert = request.getRows().get(0);
      Map<String,Object> actualContentMap =  rowToInsert.getContent();

      Assert.assertEquals(expectedContentMap.keySet(), actualContentMap.keySet());

      expectedContentMap.forEach((ek, ev) -> {
        Object actualContent = actualContentMap.get(ek);
        Assert.assertEquals(ev, actualContent);
      });

      InsertAllResponse response = PowerMockito.mock(InsertAllResponse.class);
      Mockito.doReturn(Collections.emptyMap()).when(response).getInsertErrors();
      Mockito.doReturn(false).when(response).hasErrors();
      return response;
    });

    BigQueryTargetConfigBuilder configBuilder = new BigQueryTargetConfigBuilder();
    configBuilder.ignoreInvalidColumns(true);
    createAndRunner(configBuilder.build(), Collections.singletonList(record));
  }


  @Test
  public void testRowId() throws Exception {
    List<Record> records = new ArrayList<>();

    records.add(
        createRecord(
            ImmutableMap.of("a", 1, "b", 11, "c", 111)
        )
    );
    records.add(
        createRecord(
            ImmutableMap.of("a", 2, "b", 22, "c", 222)
        )
    );
    records.add(
        createRecord(
            ImmutableMap.of("a", 1, "b", 33, "c", 333)
        )
    );


    final Map<String, Map<String, Object>> rowIdToRow = new LinkedHashMap<>();

    mockBigQueryInsertAllRequest(invocationOnMock -> {
      InsertAllResponse response = PowerMockito.mock(InsertAllResponse.class);
      Mockito.doReturn(Collections.emptyMap()).when(response).getInsertErrors();
      Mockito.doReturn(false).when(response).hasErrors();

      InsertAllRequest request = (InsertAllRequest)invocationOnMock.getArguments()[0];

      request.getRows().forEach(row ->
          rowIdToRow.computeIfAbsent(row.getId(), rowId -> new LinkedHashMap<>()).putAll(row.getContent())
      );
      return response;
    });


    BigQueryTargetConfigBuilder configBuilder = new BigQueryTargetConfigBuilder();
    configBuilder.ignoreInvalidColumns(true);
    //Set value of a has row id
    configBuilder.rowIdExpression("${record:value('/a')}");
    createAndRunner(configBuilder.build(), records);

    Assert.assertEquals(2, rowIdToRow.size());

    rowIdToRow.forEach((rowId, row) ->{
      switch (rowId) {
        case "1":
          Assert.assertEquals(1, row.get("a"));
          Assert.assertEquals(33, row.get("b"));
          Assert.assertEquals(333, row.get("c"));
          break;
        case "2":
          Assert.assertEquals(2, row.get("a"));
          Assert.assertEquals(22, row.get("b"));
          Assert.assertEquals(222, row.get("c"));
          break;
        default:
          Assert.fail("Unexpected row id: " + rowId);
          break;
      }
    });
  }

  @SuppressWarnings("unchecked")
  private void checkValues(Object expected, Object actual) {
    if (expected instanceof Map) {
      Map<String, Object> expectedMap = (Map<String, Object>)expected;
      Map<String, Object> actualMap = (Map<String, Object>)actual;
      Assert.assertEquals(expectedMap.keySet(), actualMap.keySet());
      expectedMap.forEach((k,v) -> checkValues(expectedMap.get(k), v));
    } else if (expected  instanceof List) {
      List<Object> expectedList = (List<Object>)expected;
      List<Object> actualList = (List<Object>)actual;
      Assert.assertEquals(expectedList.size(), actualList.size());
      IntStream.range(0, expectedList.size()).forEach(idx -> checkValues(expectedList.get(idx), actualList.get(idx)));
    } else {
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testNestedAndRepeatedRecords() throws Exception {
    Map<String, Object> expectedRowContent = new LinkedHashMap<>();

    //one level map
    Map<String, Object> innerLevelMap = new LinkedHashMap<>();
    innerLevelMap.put("aMap1", 1);
    innerLevelMap.put("aMap2", 2.0);
    innerLevelMap.put("aMap3", true);

    expectedRowContent.put("a", innerLevelMap);

    //single list
    List<String> innerStringList = new ArrayList<>();
    innerStringList.add("bList1");
    innerStringList.add("bList2");
    innerStringList.add("bList3");

    expectedRowContent.put("b", innerStringList);

    //Nested Repeated map
    List<Map<String, Object>> innerLevelMapList = new ArrayList<>();

    Map<String, Object> innerLevelMap1 = new LinkedHashMap<>();
    innerLevelMap1.put("cList1a", "a");
    innerLevelMap1.put("cList1b", 1);
    innerLevelMap1.put("cList1c", 2.0);
    innerLevelMap1.put("cList1d", true);
    innerLevelMap1.put("cList1e", ImmutableMap.of("e", 1));


    Map<String, Object> innerLevelMap2 = new LinkedHashMap<>();
    innerLevelMap2.put("cList2a", "b");
    innerLevelMap2.put("cList2b", 2);
    innerLevelMap2.put("cList2c", 3.0);
    innerLevelMap2.put("cList2d", false);
    innerLevelMap1.put("cList2e", ImmutableMap.of("e", 2));


    Map<String, Object> innerLevelMap3 = new LinkedHashMap<>();
    innerLevelMap3.put("cList3a", "c");
    innerLevelMap3.put("cList3b", 3);
    innerLevelMap3.put("cList3c", 4.0);
    innerLevelMap3.put("cList3d", true);
    innerLevelMap1.put("cList3e", ImmutableMap.of("e", 3));

    innerLevelMapList.add(innerLevelMap1);
    innerLevelMapList.add(innerLevelMap2);
    innerLevelMapList.add(innerLevelMap3);

    expectedRowContent.put("c", innerLevelMapList);


    Record record = RecordCreator.create();
    record.set(createField(expectedRowContent));

    mockBigQueryInsertAllRequest(invocationOnMock -> {
      InsertAllRequest insertAllRequest = (InsertAllRequest)invocationOnMock.getArguments()[0];

      List<InsertAllRequest.RowToInsert> rows = insertAllRequest.getRows();

      Assert.assertEquals(1, rows.size());

      InsertAllRequest.RowToInsert row = rows.get(0);

      row.getContent().forEach((k, v) -> checkValues(expectedRowContent.get(k), v));

      InsertAllResponse response = PowerMockito.mock(InsertAllResponse.class);
      Mockito.doReturn(Collections.emptyMap()).when(response).getInsertErrors();
      Mockito.doReturn(false).when(response).hasErrors();
      return response;
    });

    BigQueryTargetConfigBuilder configBuilder = new BigQueryTargetConfigBuilder();
    createAndRunner(configBuilder.build(), Collections.singletonList(record));
  }

  @Test
  public void testUnsupportedFields() throws Exception {
    List<Record> records = new ArrayList<>();

    records.add(
        createRecord(
            ImmutableMap.of("a",  new BigDecimal(1.0))
        )
    );

    records.add(
        createRecord(
            ImmutableMap.of("a", 2, "b", new FileRef(1000) {
              @Override
              @SuppressWarnings("unchecked")
              public <T extends AutoCloseable> Set<Class<T>> getSupportedStreamClasses() {
                return ImmutableSet.of((Class<T>)InputStream.class);
              }

              @Override
              @SuppressWarnings("unchecked")
              public <T extends AutoCloseable> T createInputStream(ProtoConfigurableEntity.Context context, Class<T> streamClassType) throws IOException {
                return (T)new ByteArrayInputStream("abc".getBytes());
              }
            })
        )
    );

    Map<String, Field> innerLevelMap = new LinkedHashMap<>();
    innerLevelMap.put("aMapDecimal", Field.create(new BigDecimal(1)));
    Map<String, Field> rootMap3 = new HashMap<>();
    rootMap3.put("a", Field.create(innerLevelMap));
    Record record3 = RecordCreator.create();
    record3.set(Field.create(rootMap3));
    records.add(record3);


    List<Field> innerLevelList = new ArrayList<>();
    innerLevelList.add(Field.create(new BigDecimal(1)));
    Map<String, Field> rootMap4 = new HashMap<>();
    rootMap4.put("a", Field.create(innerLevelList));
    Record record4 = RecordCreator.create();
    record4.set(Field.create(rootMap4));
    records.add(record4);

    records.add(createRecord(ImmutableMap.of("c", 'c')));
    records.add(createRecord(ImmutableMap.of("b", "b".getBytes()[0])));


    PowerMockito.doReturn(PowerMockito.mock(Table.class)).when(bigQuery).getTable(Mockito.any(TableId.class));

    BigQueryTargetConfigBuilder configBuilder = new BigQueryTargetConfigBuilder();
    TargetRunner targetRunner = createAndRunner(configBuilder.build(), records);
    Assert.assertEquals(6, targetRunner.getErrorRecords().size());
    for (Record record : targetRunner.getErrorRecords()) {
      String errorCode = record.getHeader().getErrorCode();
      Assert.assertEquals(Errors.BIGQUERY_13.getCode(), errorCode);
      String errorMessage = record.getHeader().getErrorMessage();
      Assert.assertTrue(errorMessage.contains("Unsupported field "));
    }
  }
  @Test
  public void testDataSetOrTableExistence() throws Exception {
    List<Record> records = new ArrayList<>();

    Record record1 = createRecord(ImmutableMap.of("a", 1));
    Record record2 = createRecord(ImmutableMap.of("a",  2));
    Record record3 = createRecord(ImmutableMap.of("a",  3));

    record1.getHeader().setAttribute("dataSet", "correctDataset");
    record1.getHeader().setAttribute("table", "wrongTable");

    record2.getHeader().setAttribute("dataSet", "wrongDataset");
    record2.getHeader().setAttribute("table", "correctTable");

    record3.getHeader().setAttribute("dataSet", "correctDataset");
    record3.getHeader().setAttribute("table", "correctTable");

    records.add(record1);
    records.add(record2);
    records.add(record3);

    mockBigQueryInsertAllRequest(invocationOnMock -> {
      InsertAllResponse response = PowerMockito.mock(InsertAllResponse.class);
      Mockito.doReturn(Collections.emptyMap()).when(response).getInsertErrors();
      Mockito.doReturn(false).when(response).hasErrors();
      return response;
    });

    BigQueryTargetConfigBuilder configBuilder = new BigQueryTargetConfigBuilder();
    configBuilder.datasetEL("${record:attribute('dataSet')}");
    configBuilder.tableNameEL("${record:attribute('table')}");
    TargetRunner targetRunner = createAndRunner(configBuilder.build(), records);

    Assert.assertEquals(2, targetRunner.getErrorRecords().size());
    for (Record errorRecord : targetRunner.getErrorRecords()) {
      String errorCode = errorRecord.getHeader().getErrorCode();
      Assert.assertEquals(Errors.BIGQUERY_17.getCode(), errorCode);
    }
  }

  @Test
  public void testErrorInIngestingMultipleTables() throws Exception {
    List<Record> records = new ArrayList<>();

    Record record1 = createRecord(ImmutableMap.of("a", 1));
    Record record2 = createRecord(ImmutableMap.of("a",  2));
    Record record3 = createRecord(ImmutableMap.of("a",  3));

    record1.getHeader().setAttribute("table", "table1");
    record2.getHeader().setAttribute("table", "table2");
    record3.getHeader().setAttribute("table", "table3");

    records.add(record1);
    records.add(record2);
    records.add(record3);

    Answer<InsertAllResponse> insertAllResponseAnswer = invocationOnMock -> {
      InsertAllRequest request = (InsertAllRequest) (invocationOnMock.getArguments()[0]);
      InsertAllResponse response = PowerMockito.mock(InsertAllResponse.class);
      if (request.getTable().getTable().equals("table2")) {
        BigQueryError bigQueryError = PowerMockito.mock(BigQueryError.class);
        Mockito.doReturn("Error in bigquery").when(bigQueryError).getMessage();
        Mockito.doReturn("Error in bigquery").when(bigQueryError).getReason();
        Mockito.doReturn(
            ImmutableMap.of(0L, Collections.singletonList(bigQueryError))
        ).when(response).getInsertErrors();
        Mockito.doReturn(true).when(response).hasErrors();
      } else {
        Mockito.doReturn(Collections.emptyMap()).when(response).getInsertErrors();
        Mockito.doReturn(false).when(response).hasErrors();
      }
      return response;
    };

    PowerMockito.doAnswer(insertAllResponseAnswer).when(bigQuery).insertAll(Mockito.any(InsertAllRequest.class));
    PowerMockito.doAnswer((Answer<Table>) invocationOnMock -> Mockito.mock(Table.class))
        .when(bigQuery).getTable(Mockito.any(TableId.class));

    BigQueryTargetConfigBuilder configBuilder = new BigQueryTargetConfigBuilder();
    configBuilder.datasetEL("sample");
    configBuilder.tableNameEL("${record:attribute('table')}");
    TargetRunner targetRunner = createAndRunner(configBuilder.build(), records);

    Assert.assertEquals(1, targetRunner.getErrorRecords().size());
    for (Record errorRecord : targetRunner.getErrorRecords()) {
      String errorCode = errorRecord.getHeader().getErrorCode();
      Assert.assertNotNull(errorCode);
      Assert.assertEquals("table2", errorRecord.getHeader().getAttribute("table"));
    }

  }
}
