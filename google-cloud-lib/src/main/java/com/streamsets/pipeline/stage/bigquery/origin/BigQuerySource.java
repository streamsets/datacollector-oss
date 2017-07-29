/**
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
package com.streamsets.pipeline.stage.bigquery.origin;

import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.QueryRequest;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.Schema;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.event.EventCreator;
import com.streamsets.pipeline.stage.bigquery.lib.BigQueryDelegate;
import com.streamsets.pipeline.stage.bigquery.lib.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static com.streamsets.pipeline.stage.bigquery.lib.Errors.BIGQUERY_04;
import static com.streamsets.pipeline.stage.bigquery.lib.Errors.BIGQUERY_05;

public class BigQuerySource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySource.class);

  private static final String QUERY = "query";
  private static final String TIMESTAMP = "timestamp";
  private static final String ROW_COUNT = "rows";
  private static final String SOURCE_OFFSET = "offset";
  private static final EventCreator QUERY_SUCCESS = new EventCreator.Builder("big-query-success", 1)
      .withRequiredField(QUERY)
      .withRequiredField(TIMESTAMP)
      .withRequiredField(ROW_COUNT)
      .withRequiredField(SOURCE_OFFSET)
      .build();

  private final BigQuerySourceConfig conf;

  private BigQueryDelegate delegate;
  private QueryResult result;
  private Schema schema;
  private int totalCount;

  public BigQuerySource(BigQuerySourceConfig conf) {
    this.conf = conf;
  }

  @Override
  public void destroy() {
    result = null;
  }

  @Override
  protected List<ConfigIssue> init() {
    List<ConfigIssue> issues = super.init();

    getCredentials(issues).ifPresent(c -> delegate = new BigQueryDelegate(getBigQuery(c), conf.useLegacySql));

    return issues;
  }

  private Optional<Credentials> getCredentials(List<ConfigIssue> issues) {
    Credentials credentials = null;

    File credentialsFile;
    if (Paths.get(conf.credentials.path).isAbsolute()) {
      credentialsFile = new File(conf.credentials.path);
    } else {
      credentialsFile = new File(getContext().getResourcesDirectory(), conf.credentials.path);
    }

    try (InputStream in = new FileInputStream(credentialsFile)) {
      credentials = ServiceAccountCredentials.fromStream(in);
    } catch (FileNotFoundException e) {
      LOG.error(BIGQUERY_04.getMessage(), credentialsFile.getPath(), e);
      issues.add(getContext().createConfigIssue(
          Groups.CREDENTIALS.name(),
          "conf.credentials.path",
          BIGQUERY_04,
          credentialsFile.getPath()
      ));
    } catch (IOException | IllegalArgumentException e) {
      LOG.error(BIGQUERY_05.getMessage(), e);
      issues.add(getContext().createConfigIssue(
          Groups.CREDENTIALS.name(),
          "conf.credentials.path",
          BIGQUERY_05
      ));
    }

    return Optional.ofNullable(credentials);
  }

  @VisibleForTesting
  BigQuery getBigQuery(Credentials credentials) {
    BigQueryOptions options = BigQueryOptions.newBuilder()
        .setCredentials(credentials)
        .setProjectId(conf.credentials.projectId)
        .build();

    return options.getService();
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String sourceOffset = lastSourceOffset;

    if (result == null) {
      QueryRequest queryRequest = QueryRequest.newBuilder(conf.query)
          .setPageSize((long) Math.min(conf.maxBatchSize, maxBatchSize))
          .setUseQueryCache(conf.useQueryCache)
          .setUseLegacySql(conf.useLegacySql)
          .build();

      result = runQuery(queryRequest);
      schema = result.getSchema();
      totalCount = 0;
      LOG.debug("Will process a total of {} rows.", result.getTotalRows());
    }

    int count = 0;

    // process one page (batch)
    for (List<FieldValue> row : result.getValues()) {
      sourceOffset = Utils.format("projectId:{}::rowNum:{}", conf.credentials.projectId, count);
      Record r = getContext().createRecord(sourceOffset);

      LinkedHashMap<String, Field> root = delegate.fieldsToMap(schema.getFields(), row);
      r.set(Field.createListMap(root));
      batchMaker.addRecord(r);
      ++count;
      ++totalCount;
    }

    result = result.getNextPage();

    if (result == null) {
      // finished because no more pages
      QUERY_SUCCESS.create(getContext())
          .with(QUERY, conf.query)
          .with(TIMESTAMP, System.currentTimeMillis())
          .with(ROW_COUNT, totalCount)
          .with(SOURCE_OFFSET, sourceOffset)
          .createAndSend();
      return null;
    }

    return sourceOffset;
  }

  @VisibleForTesting
  QueryResult runQuery(QueryRequest queryRequest) throws StageException {
    return delegate.runQuery(queryRequest, conf.timeout * 1000);
  }
}
