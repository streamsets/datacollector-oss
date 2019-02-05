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

import com.google.api.services.bigquery.Bigquery.Jobs.GetQueryResults;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.QueryResultsOption;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.impl.Utils;

import com.streamsets.pipeline.lib.util.ThreadUtil;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class BigQueryDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryDelegate.class);
  private static final Set<Field.Type> TEMPORAL_TYPES = ImmutableSet.of(
      Field.Type.DATETIME,
      Field.Type.DATE,
      Field.Type.TIME
  );

  private static final Map<StandardSQLTypeName, Field.Type> STANDARD_SQL_TYPES = ImmutableMap
      .<StandardSQLTypeName, Field.Type>builder()
      .put(StandardSQLTypeName.STRING, Field.Type.STRING)
      .put(StandardSQLTypeName.BYTES, Field.Type.BYTE_ARRAY)
      .put(StandardSQLTypeName.INT64, Field.Type.LONG)
      .put(StandardSQLTypeName.FLOAT64, Field.Type.DOUBLE)
      .put(StandardSQLTypeName.NUMERIC, Field.Type.DECIMAL)
      .put(StandardSQLTypeName.BOOL, Field.Type.BOOLEAN)
      .put(StandardSQLTypeName.TIMESTAMP, Field.Type.DATETIME)
      .put(StandardSQLTypeName.TIME, Field.Type.TIME)
      .put(StandardSQLTypeName.DATETIME, Field.Type.DATETIME)
      .put(StandardSQLTypeName.DATE, Field.Type.DATE)
      .put(StandardSQLTypeName.STRUCT, Field.Type.LIST_MAP)
      .put(StandardSQLTypeName.ARRAY, Field.Type.LIST)
      .build();

  private static final Map<LegacySQLTypeName, Field.Type> LEGACY_SQL_TYPES = ImmutableMap
      .<LegacySQLTypeName, Field.Type>builder()
      .put(LegacySQLTypeName.STRING, Field.Type.STRING)
      .put(LegacySQLTypeName.BYTES, Field.Type.BYTE_ARRAY)
      .put(LegacySQLTypeName.INTEGER, Field.Type.LONG)
      .put(LegacySQLTypeName.FLOAT, Field.Type.DOUBLE)
      .put(LegacySQLTypeName.NUMERIC, Field.Type.DECIMAL)
      .put(LegacySQLTypeName.BOOLEAN, Field.Type.BOOLEAN)
      .put(LegacySQLTypeName.TIMESTAMP, Field.Type.DATETIME)
      .put(LegacySQLTypeName.TIME, Field.Type.TIME)
      .put(LegacySQLTypeName.DATETIME, Field.Type.DATETIME)
      .put(LegacySQLTypeName.DATE, Field.Type.DATE)
      .put(LegacySQLTypeName.RECORD, Field.Type.LIST_MAP)
      .build();

  private final Map<StandardSQLTypeName, SimpleDateFormat> StandardSQLTypeDateFormatter =
          ImmutableMap.of(
                  StandardSQLTypeName.DATE, new SimpleDateFormat("yyy-MM-dd"),
                  StandardSQLTypeName.DATETIME, new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"),
                  StandardSQLTypeName.TIMESTAMP, new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"),
                  StandardSQLTypeName.TIME, new SimpleDateFormat("HH:mm:ss.SSS")
          );

  private final Map<LegacySQLTypeName, SimpleDateFormat> LegacySQLTypeDateFormatter =
          ImmutableMap.of(
                  LegacySQLTypeName.DATE, new SimpleDateFormat("yyy-MM-dd"),
                  LegacySQLTypeName.DATETIME, new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"),
                  LegacySQLTypeName.TIMESTAMP, new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS"),
                  LegacySQLTypeName.TIME, new SimpleDateFormat("HH:mm:ss.SSS")
          );

  private static final Pattern stripMicrosec = Pattern.compile("(\\.\\d{3})(\\d+)");

  private final Map<FieldValue.Attribute, BiFunction<com.google.cloud.bigquery.Field, FieldValue, Field>> transforms =
      ImmutableMap.of(
          FieldValue.Attribute.PRIMITIVE, this::fromPrimitiveField,
          FieldValue.Attribute.RECORD, (s, v) -> Field.create(fieldsToMap(s.getSubFields(), v.getRecordValue())),
          FieldValue.Attribute.REPEATED, (s, v) -> Field.create(fromRepeatedField(s, v.getRepeatedValue()))
      );


  private final BigQuery bigquery;
  private final Clock clock;

  private Function<com.google.cloud.bigquery.Field, Field.Type> asRecordFieldTypeFunction;
  private Function<com.google.cloud.bigquery.Field, SimpleDateFormat> dateTimeFormatter;

  /**
   * Constructs a new {@link BigQueryDelegate} for a specific project and set of credentials.
   * #legacySql and #standardSql annotations in the query are not supported, instead the
   * boolean flag useLegacySql should be set to determine which syntax is to be used.
   *
   * @param bigquery BigQuery instance
   * @param useLegacySql When false (default), uses Standard SQL syntax and data types.
   * @param clock Clock implementation for checking whether query has timed out. Normally only supplied in tests.
   */
  BigQueryDelegate(BigQuery bigquery, boolean useLegacySql, Clock clock) {
    this.bigquery = bigquery;
    this.clock = clock;

    if (useLegacySql) {
      asRecordFieldTypeFunction = field -> LEGACY_SQL_TYPES.get(field.getType());
      dateTimeFormatter = field -> LegacySQLTypeDateFormatter.get(field.getType());
    } else {
      asRecordFieldTypeFunction = field -> STANDARD_SQL_TYPES.get(field.getType().getStandardType());
      dateTimeFormatter = field -> StandardSQLTypeDateFormatter.get(field.getType().getStandardType());
    }
  }

  /**
   * Constructs a new {@link BigQueryDelegate} for a specific project and set of credentials.
   * #legacySql and #standardSql annotations in the query are not supported, instead the
   * boolean flag useLegacySql should be set to determine which syntax is to be used.
   *
   * @param bigquery BigQuery instance
   * @param useLegacySql When false (default), uses Standard SQL syntax and data types.
   */
  public BigQueryDelegate(BigQuery bigquery, boolean useLegacySql) {
    this(bigquery, useLegacySql, Clock.systemDefaultZone());
  }

  /**
   * Executes a query request and returns the results. A timeout is required to avoid
   * waiting for an indeterminate amount of time. If the query fails to complete within the
   * timeout it is aborted.
   *
   * @param queryConfig request describing the query to execute
   * @param timeout maximum amount of time to wait for job completion before attempting to cancel
   * @return result of the query.
   * @throws StageException if the query does not complete within the requested timeout or
   * there is an error executing the query.
   */
  public TableResult runQuery(QueryJobConfiguration queryConfig, long timeout, long pageSize) throws
      StageException {
    checkArgument(timeout >= 1000, "Timeout must be at least one second.");
    Instant maxTime = Instant.now().plusMillis(timeout);

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    JobInfo jobInfo = JobInfo.newBuilder(queryConfig).setJobId(jobId).build();
    Job queryJob = bigquery.create(jobInfo);

    // Check for errors
    if (queryJob == null) {
      LOG.error("Job no longer exists: {}", jobInfo);
      throw new RuntimeException("Job no longer exists: "+jobInfo);
    } else if (queryJob.getStatus().getError() != null) {
      BigQueryError error = queryJob.getStatus().getError();
      LOG.error("Query Job execution error: {}", error);
      throw new StageException(Errors.BIGQUERY_02, error);
    }

    //Should consider using .waitFor(RetryOption.totalTimeout())
    while(!queryJob.isDone()) {
      if (Instant.now(clock).isAfter(maxTime) || !ThreadUtil.sleep(100)) {
        if (bigquery.cancel(queryJob.getJobId())) {
          LOG.info("Job {} cancelled successfully.", queryJob.getJobId());
        } else {
          LOG.warn("Job {} not found", queryJob.getJobId());
        }
        throw new StageException(Errors.BIGQUERY_00);
      }
    }


    if (queryJob.getStatus().getError() != null) {
      String errorMsg = queryJob.getStatus().getError().toString();
      throw new StageException(Errors.BIGQUERY_02, errorMsg);
    }

    // Get the results.
    TableResult result = null;
    try {
      result = queryJob.getQueryResults(QueryResultsOption.pageSize(pageSize));
    } catch (InterruptedException e) {
      String errorMsg = e.getMessage();
      throw new StageException(Errors.BIGQUERY_02, errorMsg);
    }

    return result;
  }

  /**
   * Returns the SDC record {@link Field} type mapped from a
   * BigQuery {@link com.google.cloud.bigquery.Field} type.
   *
   * @param field A BigQuery {@link com.google.cloud.bigquery.Field}
   * @return SDC record {@link Field.Type}
   */
  public Field.Type asRecordFieldType(com.google.cloud.bigquery.Field field) {
    Field.Type type = asRecordFieldTypeFunction.apply(field);
    return checkNotNull(type, Utils.format("Unsupported type '{}'", field.getType()));
  }

  /**
   * Converts a list of BigQuery fields to SDC Record fields.
   * The provided parameters must have matching lengths.
   *
   * If not, an unchecked exception will be thrown. This method is called when the resulting
   * container type should be a {@link Field.Type#LIST_MAP}
   *
   * @param schema List of {@link com.google.cloud.bigquery.Field} representing the schema
   * at the current level of nesting. For example, if processing a
   * {@link LegacySQLTypeName#RECORD} or {@link StandardSQLTypeName#STRUCT}
   * this would only include the fields for that particular data structure and not the entire
   * result set.
   *
   * @param values List of {@link FieldValue} representing the values to set in the generated
   * fields.
   * @return Specifically, a LinkedHashMap as the return value of this method is expected to be
   * used to create a
   * {@link Field.Type#LIST_MAP} field.
   */
  public LinkedHashMap<String, Field> fieldsToMap( // NOSONAR
      List<com.google.cloud.bigquery.Field> schema,
      List<FieldValue> values
  ) {
    checkState(
        schema.size() == values.size(),
        "Schema '{}' and Values '{}' sizes do not match.",
        schema.size(),
        values.size()
    );

    LinkedHashMap<String, Field> root = new LinkedHashMap<>();
    for (int i = 0; i < values.size(); i++) {
      FieldValue value = values.get(i);
      com.google.cloud.bigquery.Field field = schema.get(i);
      if (value.getAttribute().equals(FieldValue.Attribute.PRIMITIVE)) {
        root.put(field.getName(), fromPrimitiveField(field, value));
      } else if (value.getAttribute().equals(FieldValue.Attribute.RECORD)) {
        root.put(
            field.getName(),
            Field.create(fieldsToMap(field.getSubFields(), value.getRecordValue()))
        );
      } else if (value.getAttribute().equals(FieldValue.Attribute.REPEATED)) {
        root.put(field.getName(), Field.create(fromRepeatedField(field, value.getRepeatedValue())));
      }
    }
    return root;
  }

  /**
   * Repeated fields are simply fields that may appear more than once. In SDC we will
   * represent them as a list field. For example a repeated field of type RECORD would be a
   * {@link Field.Type#LIST} of
   * {@link Field.Type#LIST_MAP} and a repeated field of type STRING would be
   * a {@link Field.Type#LIST} of {@link Field.Type#STRING}.
   *
   * @param schema The field metadata for the repeated field
   * @param repeatedValue a list of individual field values that represent the repeated field
   * @return a list of SDC record fields.
   * Intended to be used for creating a {@link Field.Type#LIST} {@link Field}
   */
  public List<Field> fromRepeatedField(com.google.cloud.bigquery.Field schema,
      List<FieldValue> repeatedValue) {

    if (repeatedValue.isEmpty()) {
      return Collections.emptyList();
    }
    FieldValue.Attribute repeatedFieldType = repeatedValue.get(0).getAttribute();

    BiFunction<com.google.cloud.bigquery.Field, FieldValue, Field> transform =
        transforms.get(repeatedFieldType);
    return repeatedValue
        .stream()
        .map( v -> transform.apply(schema, v))
        .collect(Collectors.toList());
  }

  public Field fromPrimitiveField(com.google.cloud.bigquery.Field field, FieldValue value) {
    Field.Type type = asRecordFieldType(field);
    Field f;
    if (value.isNull()) {
      f = Field.create(type, null);
    } else if (TEMPORAL_TYPES.contains(type)) {
      if (field.getType().getStandardType() == StandardSQLTypeName.TIMESTAMP) {
        // in general only TIMESTAMP should be a Unix epoch value.  However, to be certain will test for getTimeStampValue() and process as as string format if that fails
        f = Field.create(type, value.getTimestampValue() / 1000L); // micro to milli
      } else {
        // not a timestamp value.  Assume it's a date string format
        // Other BigQuery temporal types come as string representations.
        try {
          String dval = value.getStringValue();
          if (dval != null) {
            dval = stripMicrosec.matcher(dval).replaceAll("$1"); // strip out microseconds, for milli precision
          }
          f = Field.create(type, dateTimeFormatter.apply(field).parse(dval));
        } catch (ParseException ex) {
          // In case of failed date/time parsing, simply allow the value to proceed as a string
          LOG.error(String.format("Unable to convert BigQuery field type %s to field type %s", field.getType().toString(), type.toString()), ex);
          f = Field.create(Field.Type.STRING, value.getStringValue()); // allow it to proceed with a null value
          f.setAttribute("bq.parseException", String.format("%s using format %s", ex.getMessage(), dateTimeFormatter.apply(field).toPattern()) );
        }
      }
      if (f != null) f.setAttribute("bq.fullValue", value.getStringValue()); // add the parse error as a field header
    } else if (type == Field.Type.BYTE_ARRAY) {
      f = Field.create(type, value.getBytesValue());
    } else {
      f = Field.create(type, value.getValue());
    }
    return f;
  }

  public static BigQuery getBigquery(Credentials credentials, String projectId) {
    BigQueryOptions options = BigQueryOptions.newBuilder()
        .setCredentials(credentials)
        .setProjectId(projectId)
        .build();
    return options.getService();
  }

}
