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
package com.streamsets.pipeline.stage.bigquery.lib;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.Schema;
import com.google.common.collect.ImmutableList;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.StageException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TestBigQueryDelegate {
  private BigQuery mockBigquery;
  private JobId jobId;

  @Parameters
  public static Collection<Object[]> streams() {
    // useLegacySql
    return Arrays.asList(new Object[][]{
        {true}, {false}
    });
  }

  @Parameter
  public boolean useLegacySql;

  @Before
  public void setUp() throws Exception {
    mockBigquery = mock(BigQuery.class);
    jobId = JobId.of("test-project", "datacollector");
  }

  @Test
  public void runQuery() throws Exception {
    QueryRequest queryRequest = QueryRequest.newBuilder("SELECT * FROM [sample:table] LIMIT 1000")
        .setPageSize(1000L)
        .setUseQueryCache(true)
        .setUseLegacySql(useLegacySql)
        .build();

    QueryResponse mockQueryResponse = mock(QueryResponse.class);
    // First pretend we haven't finished running the query, second time around its completed.
    when(mockQueryResponse.jobCompleted()).thenReturn(false).thenReturn(true);
    when(mockQueryResponse.getJobId()).thenReturn(jobId);
    when(mockQueryResponse.hasErrors()).thenReturn(false);

    when(mockBigquery.query(queryRequest)).thenReturn(mockQueryResponse);
    when(mockBigquery.getQueryResults(jobId)).thenReturn(mockQueryResponse);

    QueryResult mockQueryResult = mock(QueryResult.class);
    when(mockQueryResponse.getResult()).thenReturn(mockQueryResult);

    BigQueryDelegate delegate = new BigQueryDelegate(mockBigquery, useLegacySql);
    delegate.runQuery(queryRequest, 1000);
  }

  @Test(expected = IllegalArgumentException.class)
  public void runQueryInvalidTimeout() throws Exception {
    QueryRequest queryRequest = QueryRequest.newBuilder("SELECT * FROM [sample:table] LIMIT 1000")
        .setPageSize(1000L)
        .setUseQueryCache(true)
        .setUseLegacySql(useLegacySql)
        .build();

    BigQueryDelegate delegate = new BigQueryDelegate(mockBigquery, useLegacySql);
    delegate.runQuery(queryRequest, 500);
  }

  @Test(expected = StageException.class)
  public void runQueryTimeout() throws Exception {
    QueryRequest queryRequest = QueryRequest.newBuilder("SELECT * FROM [sample:table] LIMIT 1000")
        .setPageSize(1000L)
        .setUseQueryCache(true)
        .setUseLegacySql(useLegacySql)
        .build();

    QueryResponse mockQueryResponse = mock(QueryResponse.class);
    when(mockQueryResponse.jobCompleted()).thenReturn(false);
    when(mockQueryResponse.getJobId()).thenReturn(jobId);
    when(mockQueryResponse.hasErrors()).thenReturn(false);

    when(mockBigquery.query(queryRequest)).thenReturn(mockQueryResponse);
    when(mockBigquery.cancel(jobId)).thenReturn(true);
    when(mockBigquery.getQueryResults(jobId)).thenReturn(mockQueryResponse);

    BigQueryDelegate delegate = new BigQueryDelegate(
        mockBigquery,
        useLegacySql,
        Clock.offset(Clock.systemDefaultZone(), Duration.ofSeconds(1))
    );

    ErrorCode code = null;
    try {
      delegate.runQuery(queryRequest, 1000);
    } catch (StageException e) {
      code = e.getErrorCode();
      throw e;
    } finally {
      assertEquals(Errors.BIGQUERY_00, code);
    }
  }

  @Test(expected = StageException.class)
  public void runQueryHasErrors() throws Exception {
    QueryRequest queryRequest = QueryRequest.newBuilder("SELECT * FROM [sample:table] LIMIT 1000")
        .setPageSize(1000L)
        .setUseQueryCache(true)
        .setUseLegacySql(useLegacySql)
        .build();

    QueryResponse mockQueryResponse = mock(QueryResponse.class);
    // First pretend we haven't finished running the query, second time around its completed.
    when(mockQueryResponse.jobCompleted()).thenReturn(true);
    when(mockQueryResponse.getJobId()).thenReturn(jobId);
    when(mockQueryResponse.hasErrors()).thenReturn(true);
    when(mockQueryResponse.getExecutionErrors()).thenReturn(ImmutableList.of(new BigQueryError(
        "Some Error",
        "Some Location",
        "Some Error Message"
    )));

    when(mockBigquery.query(queryRequest)).thenReturn(mockQueryResponse);
    when(mockBigquery.getQueryResults(jobId)).thenReturn(mockQueryResponse);

    BigQueryDelegate delegate = new BigQueryDelegate(mockBigquery, useLegacySql);

    ErrorCode code = null;
    try {
      delegate.runQuery(queryRequest, 1000);
    } catch (StageException e) {
      code = e.getErrorCode();
      throw e;
    } finally {
      assertEquals(Errors.BIGQUERY_02, code);
    }
  }

  @Test
  public void fieldsToMap() throws Exception {
    Schema schema = createTestSchema();
    List<FieldValue> fieldValues = createTestValues();

    BigQueryDelegate delegate = new BigQueryDelegate(mockBigquery, useLegacySql);
    LinkedHashMap<String, com.streamsets.pipeline.api.Field> map = delegate.fieldsToMap(schema.getFields(), fieldValues);
    assertTrue(map.containsKey("a"));
    assertEquals("a string", map.get("a").getValueAsString());
    assertArrayEquals("bytes".getBytes(), map.get("b").getValueAsByteArray());
    List<com.streamsets.pipeline.api.Field> c = map.get("c").getValueAsList();
    assertEquals(1L, c.get(0).getValueAsLong());
    assertEquals(2L, c.get(1).getValueAsLong());
    assertEquals(3L, c.get(2).getValueAsLong());
    assertEquals(2.0d, map.get("d").getValueAsDouble(), 1e-15);
    assertEquals(true, map.get("e").getValueAsBoolean());
    assertEquals(new Date(1351700038292L), map.get("f").getValueAsDatetime());
    assertEquals(new Date(1351700038292L), map.get("g").getValueAsDatetime());
    assertEquals(new Date(1351700038292L), map.get("h").getValueAsDatetime());
    assertEquals(new Date(1351700038292L), map.get("i").getValueAsDate());
    Map<String, com.streamsets.pipeline.api.Field> j = map.get("j").getValueAsListMap();
    assertEquals("nested string", j.get("x").getValueAsString());
    Map<String, com.streamsets.pipeline.api.Field> y = j.get("y").getValueAsListMap();
    assertEquals("z", y.get("z").getValueAsString());
  }

  public static Schema createTestSchema() {
    return Schema.newBuilder()
        .addField(Field.of("a", Field.Type.string()))
        .addField(Field.of("b", Field.Type.bytes()))
        .addField(Field.newBuilder("c", Field.Type.integer()).setMode(Field.Mode.REPEATED).build())
        .addField(Field.of("d", Field.Type.floatingPoint()))
        .addField(Field.of("e", Field.Type.bool()))
        .addField(Field.of("f", Field.Type.timestamp()))
        .addField(Field.of("g", Field.Type.time()))
        .addField(Field.of("h", Field.Type.datetime()))
        .addField(Field.of("i", Field.Type.date()))
        .addField(Field.of("j",
            Field.Type.record(
                Field.of("x", Field.Type.string()),
                Field.of("y", Field.Type.record(Field.of("z", Field.Type.string())))
            )
        )).build();
  }

  public static List<FieldValue> createTestValues() {
    return ImmutableList.<FieldValue>builder()
        .add(createFieldValue("a string"))
        .add(createFieldValue("bytes".getBytes()))
        .add(createFieldValue(
            ImmutableList.of(
                createFieldValue(1L),
                createFieldValue(2L),
                createFieldValue(3L)
            ),
            FieldValue.Attribute.REPEATED)
        )
        .add(createFieldValue(2.0d))
        .add(createFieldValue(true))
        .add(createFieldValue(1351700038292387L))
        .add(createFieldValue(1351700038292387L))
        .add(createFieldValue(1351700038292387L))
        .add(createFieldValue(1351700038292387L))
        .add(createFieldValue(
            ImmutableList.of(
                createFieldValue("nested string"),
                createFieldValue(ImmutableList.of(createFieldValue("z")), FieldValue.Attribute.RECORD)
            ),
            FieldValue.Attribute.RECORD
        ))
        .build();
  }

  @SuppressWarnings("unchecked")
  private static FieldValue createFieldValue(Object value, FieldValue.Attribute attribute) {
    FieldValue fieldValue = mock(FieldValue.class);
    when(fieldValue.getAttribute()).thenReturn(attribute);
    when(fieldValue.getValue()).thenReturn(value);

    if (value instanceof Long) {
      when(fieldValue.getTimestampValue()).thenReturn((long) value);
    }

    if (attribute.equals(FieldValue.Attribute.RECORD)) {
      when(fieldValue.getRecordValue()).thenReturn((List<FieldValue>) value);
    }
    if (attribute.equals(FieldValue.Attribute.REPEATED)) {
      when(fieldValue.getRepeatedValue()).thenReturn((List<FieldValue>) value);
    }

    return fieldValue;
  }

  private static FieldValue createFieldValue(Object value) {
    return createFieldValue(value, FieldValue.Attribute.PRIMITIVE);
  }
}
