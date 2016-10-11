/**
 * Copyright 2016 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.processor.jdbclookup;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnMapping;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;
import com.streamsets.pipeline.stage.processor.kv.CacheConfig;
import com.streamsets.pipeline.stage.processor.kv.EvictionPolicyType;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.streamsets.pipeline.lib.jdbc.JdbcUtil.closeQuietly;

public class JdbcLookupProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcLookupProcessor.class);

  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";
  private final CacheConfig cacheConfig;

  private ELEval queryEval;

  private final String query;
  private final List<JdbcFieldColumnMapping> columnMappings;
  private final int maxClobSize;
  private final int maxBlobSize;
  private final HikariPoolConfigBean hikariConfigBean;

  private ErrorRecordHandler errorRecordHandler;
  private HikariDataSource dataSource = null;
  private Map<String, String> columnsToFields = new HashMap<>();
  private final Properties driverProperties = new Properties();

  private LoadingCache<String, Map<String, Field>> cache;

  public JdbcLookupProcessor(
      String query,
      List<JdbcFieldColumnMapping> columnMappings,
      int maxClobSize,
      int maxBlobSize,
      HikariPoolConfigBean hikariConfigBean,
      CacheConfig cacheConfig
  ) {
    this.query = query;
    this.columnMappings = columnMappings;
    this.maxClobSize = maxClobSize;
    this.maxBlobSize = maxBlobSize;
    this.hikariConfigBean = hikariConfigBean;
    this.cacheConfig = cacheConfig;
    driverProperties.putAll(hikariConfigBean.driverProperties);
  }

  /** {@inheritDoc} */
  @Override
  protected List<ConfigIssue> init() {
    // Validate configuration values and open any required resources.
    List<ConfigIssue> issues = super.init();

    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    Processor.Context context = getContext();

    queryEval = getContext().createELEval("query");

    issues = hikariConfigBean.validateConfigs(context, issues);

    if (issues.isEmpty() && null == dataSource) {
      try {
        dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean, driverProperties);
      } catch (StageException e) {
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      }
    }

    for (JdbcFieldColumnMapping mapping : columnMappings) {
      LOG.debug("Mapping field {} to column {}", mapping.field, mapping.columnName);
      columnsToFields.put(mapping.columnName, mapping.field);
    }

    if (issues.isEmpty()) {
      cache = buildCache();
    }
    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    closeQuietly(dataSource);
    super.destroy();
  }

  /** {@inheritDoc} */
  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    try {
      ELVars elVars = getContext().createELVars();
      RecordEL.setRecordInContext(elVars, record);
      String preparedQuery = queryEval.eval(elVars, query, String.class);
      Map<String, Field> values = cache.get(preparedQuery);
      if (values.isEmpty()) {
        // No results
        LOG.error(JdbcErrors.JDBC_04.getMessage(), preparedQuery);
        errorRecordHandler.onError(new OnRecordErrorException(record, JdbcErrors.JDBC_04, preparedQuery));
      }
      for (Map.Entry<String, Field> entry : values.entrySet()) {
        record.set(entry.getKey(), entry.getValue());
      }
      batchMaker.addRecord(record);
    } catch (ELEvalException e) {
      LOG.error(JdbcErrors.JDBC_01.getMessage(), query, e);
      throw new OnRecordErrorException(record, JdbcErrors.JDBC_01, query);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), StageException.class);
      throw new IllegalStateException(e); // The cache loader shouldn't throw anything that isn't a StageException.
    } catch (OnRecordErrorException error) { // NOSONAR
      errorRecordHandler.onError(new OnRecordErrorException(record, error.getErrorCode(), error.getParams()));
    }
  }

  @SuppressWarnings("unchecked")
  private LoadingCache<String, Map<String, Field>> buildCache() {
    CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
    if (!cacheConfig.enabled) {
      return cacheBuilder.maximumSize(0).build(new JdbcLookupLoader(dataSource,
          columnsToFields,
          maxClobSize,
          maxBlobSize,
          errorRecordHandler
      ));
    }

    if (cacheConfig.maxSize == -1) {
      cacheConfig.maxSize = Long.MAX_VALUE;
    }

    // CacheBuilder doesn't support specifying type thus suffers from erasure, so
    // we build it with this if / else logic.
    if (cacheConfig.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_ACCESS) {
      cacheBuilder.maximumSize(cacheConfig.maxSize).expireAfterAccess(cacheConfig.expirationTime, cacheConfig.timeUnit);
    } else if (cacheConfig.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_WRITE) {
      cacheBuilder.maximumSize(cacheConfig.maxSize).expireAfterWrite(cacheConfig.expirationTime, cacheConfig.timeUnit);
    } else {
      throw new IllegalArgumentException(Utils.format("Unrecognized EvictionPolicyType: '{}'",
          cacheConfig.evictionPolicyType
      ));
    }
    return cacheBuilder.build(new JdbcLookupLoader(dataSource,
        columnsToFields,
        maxClobSize,
        maxBlobSize,
        errorRecordHandler
    ));
  }
}