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
package com.streamsets.pipeline.stage.bigquery.origin;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableResult;
import com.google.common.annotations.VisibleForTesting;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseSource;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.stage.bigquery.lib.BigQueryDelegate;
import com.streamsets.pipeline.stage.bigquery.lib.Groups;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import static com.streamsets.pipeline.stage.bigquery.lib.Errors.BIGQUERY_05;
import static com.streamsets.pipeline.stage.bigquery.lib.Errors.BIGQUERY_19;

public class BigQuerySource extends BaseSource {
  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySource.class);

  private final BigQuerySourceConfig conf;

  private BigQueryDelegate delegate;
  private TableResult result;
  private Schema schema;
  private int totalCount;
  private boolean checkBatchSize = true;

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

    conf.credentials.getCredentialsProvider(getContext(), issues).ifPresent(provider -> {
      if (issues.isEmpty()) {
        try {
          Optional.ofNullable(provider.getCredentials()).ifPresent(c -> delegate = new BigQueryDelegate(getBigQuery(c), conf.useLegacySql));
        } catch (IOException e) {
          LOG.error(BIGQUERY_05.getMessage(), e);
          issues.add(getContext().createConfigIssue(
              Groups.CREDENTIALS.name(),
              "conf.credentials.connection.credentialsProvider",
              BIGQUERY_05
          ));
        }
      }
    });
    return issues;
  }

  @VisibleForTesting
  BigQuery getBigQuery(Credentials credentials) {
    return BigQueryDelegate.getBigquery(credentials, conf.credentials.getProjectId());
  }

  @Override
  public String produce(String lastSourceOffset, int maxBatchSize, BatchMaker batchMaker) throws StageException {
    String sourceOffset = lastSourceOffset;
    long pageSize = (long) Math.min(conf.maxBatchSize, maxBatchSize);
    if (!getContext().isPreview() && checkBatchSize && conf.maxBatchSize > maxBatchSize) {
      getContext().reportError(BIGQUERY_19, maxBatchSize);
      checkBatchSize = false;
    }

    if (result == null) {
      QueryJobConfiguration queryRequest = QueryJobConfiguration.newBuilder(conf.query)
          .setUseQueryCache(conf.useQueryCache)
          .setUseLegacySql(conf.useLegacySql)
          .build();
      result = runQuery(queryRequest, pageSize);
      schema = result.getSchema();
      totalCount = 0;
      LOG.debug("Will process a total of {} rows.", result.getTotalRows());
    }

    int count = 0;

    // process one page (batch)
    for (FieldValueList row : result.getValues()) {
      sourceOffset = Utils.format("projectId:{}::rowNum:{}", conf.credentials.getProjectId(), count);
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
      BigQuerySuccessEvent.EVENT_CREATOR.create(getContext())
          .with(BigQuerySuccessEvent.QUERY, conf.query)
          .with(BigQuerySuccessEvent.TIMESTAMP, System.currentTimeMillis())
          .with(BigQuerySuccessEvent.ROW_COUNT, totalCount)
          .with(BigQuerySuccessEvent.SOURCE_OFFSET, sourceOffset)
          .createAndSend();
      return null;
    }

    return sourceOffset;
  }

  @VisibleForTesting
  TableResult runQuery(QueryJobConfiguration queryRequest, long pageSize) throws StageException {
    return delegate.runQuery(queryRequest, conf.timeout * 1000, pageSize);
  }
}
