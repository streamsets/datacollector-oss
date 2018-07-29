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
package com.streamsets.pipeline.stage.processor.jdbclookup;

import com.google.common.base.Throwables;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Processor;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.jdbc.DataType;
import com.streamsets.pipeline.lib.jdbc.HikariPoolConfigBean;
import com.streamsets.pipeline.lib.jdbc.JdbcErrors;
import com.streamsets.pipeline.lib.jdbc.JdbcFieldColumnMapping;
import com.streamsets.pipeline.lib.jdbc.JdbcUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.common.MissingValuesBehavior;
import com.streamsets.pipeline.stage.common.MultipleValuesBehavior;
import com.streamsets.pipeline.stage.destination.jdbc.Groups;
import com.streamsets.pipeline.stage.processor.kv.CacheConfig;
import com.streamsets.pipeline.stage.processor.kv.LookupUtils;
import com.zaxxer.hikari.HikariDataSource;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.streamsets.pipeline.lib.jdbc.JdbcUtil.closeQuietly;

public class JdbcLookupProcessor extends SingleLaneRecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcLookupProcessor.class);

  public static final String DATE_FORMAT = "yyyy/MM/dd";
  public static final String DATETIME_FORMAT = "yyyy/MM/dd HH:mm:ss";
  static final DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern(DATE_FORMAT);
  static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormat.forPattern(DATETIME_FORMAT);

  private static final String HIKARI_CONFIG_PREFIX = "hikariConfigBean.";
  private static final String CONNECTION_STRING = HIKARI_CONFIG_PREFIX + "connectionString";
  private static final String COLUMN_MAPPINGS = "columnMappings";
  private final CacheConfig cacheConfig;

  private ELEval queryEval;

  private final String query;
  private final List<JdbcFieldColumnMapping> columnMappings;
  private final MultipleValuesBehavior multipleValuesBehavior;
  private final int maxClobSize;
  private final int maxBlobSize;
  private final HikariPoolConfigBean hikariConfigBean;

  private ErrorRecordHandler errorRecordHandler;
  private HikariDataSource dataSource = null;
  private Map<String, String> columnsToFields = new HashMap<>();
  private Map<String, String> columnsToDefaults = new HashMap<>();
  private Map<String, DataType> columnsToTypes = new HashMap<>();

  private LoadingCache<String, Optional<List<Map<String, Field>>>> cache;
  private Optional<List<Map<String, Field>>> defaultValue;
  private CacheCleaner cacheCleaner;
  private final MissingValuesBehavior missingValuesBehavior;

  private List<ExecutorService> generationExecutors = new ArrayList<>();
  private int preprocessThreads = 0;

  public JdbcLookupProcessor(
      String query,
      List<JdbcFieldColumnMapping> columnMappings,
      MultipleValuesBehavior multipleValuesBehavior,
      MissingValuesBehavior missingValuesBehavior,
      int maxClobSize,
      int maxBlobSize,
      HikariPoolConfigBean hikariConfigBean,
      CacheConfig cacheConfig
  ) {
    this.query = query;
    this.columnMappings = columnMappings;
    this.multipleValuesBehavior = multipleValuesBehavior;
    this.missingValuesBehavior = missingValuesBehavior;
    this.maxClobSize = maxClobSize;
    this.maxBlobSize = maxBlobSize;
    this.hikariConfigBean = hikariConfigBean;
    this.cacheConfig = cacheConfig;
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
        dataSource = JdbcUtil.createDataSourceForRead(hikariConfigBean);
      } catch (StageException e) {
        issues.add(context.createConfigIssue(Groups.JDBC.name(), CONNECTION_STRING, JdbcErrors.JDBC_00, e.toString()));
      }
    }

    if(issues.isEmpty()) {
      this.defaultValue = calculateDefault(context, issues);
    }

    if (issues.isEmpty()) {
      cache = buildCache();
      cacheCleaner = new CacheCleaner(cache, "JdbcLookupProcessor", 10 * 60 * 1000);
      if (cacheConfig.enabled) {
        preprocessThreads = Math.min(hikariConfigBean.minIdle, Runtime.getRuntime().availableProcessors()-1);
        preprocessThreads = Math.max(preprocessThreads, 1);
      }
    }
    // If issues is not empty, the UI will inform the user of each configuration issue in the list.
    return issues;
  }

  private Optional<List<Map<String, Field>>> calculateDefault(Processor.Context context, List<ConfigIssue> issues) {
    for (JdbcFieldColumnMapping mapping : columnMappings) {
      LOG.debug("Mapping field {} to column {}", mapping.field, mapping.columnName);
      columnsToFields.put(mapping.columnName, mapping.field);
      if (!StringUtils.isEmpty(mapping.defaultValue) && mapping.dataType == DataType.USE_COLUMN_TYPE) {
        issues.add(context.createConfigIssue(Groups.JDBC.name(), COLUMN_MAPPINGS, JdbcErrors.JDBC_53, mapping.field));
      }
      columnsToDefaults.put(mapping.columnName, mapping.defaultValue);
      columnsToTypes.put(mapping.columnName, mapping.dataType);
      if (mapping.dataType == DataType.DATE) {
        try {
          DATE_FORMATTER.parseDateTime(mapping.defaultValue);
        } catch (IllegalArgumentException e) {
          issues.add(context.createConfigIssue(
              Groups.JDBC.name(),
              COLUMN_MAPPINGS,
              JdbcErrors.JDBC_55,
              mapping.field,
              e.toString()
          ));
        }
      } else if (mapping.dataType == DataType.DATETIME) {
        try {
          DATETIME_FORMATTER.parseDateTime(mapping.defaultValue);
        } catch (IllegalArgumentException e) {
          issues.add(context.createConfigIssue(
              Groups.JDBC.name(),
              COLUMN_MAPPINGS,
              JdbcErrors.JDBC_56,
              mapping.field,
              e.toString()
          ));
        }
      }
    }

    if(!issues.isEmpty()) {
      return Optional.empty();
    }

    Map<String, Field> defaultValues = new HashMap<>();

    for (String column : columnsToFields.keySet()) {
      String defaultValue = columnsToDefaults.get(column);
      DataType dataType = columnsToTypes.get(column);
      if (dataType != DataType.USE_COLUMN_TYPE) {
        Field field;
        try {
          if (dataType == DataType.DATE) {
            field = Field.createDate(DATE_FORMATTER.parseDateTime(defaultValue).toDate());
          } else if (dataType == DataType.DATETIME) {
            field = Field.createDatetime(DATETIME_FORMATTER.parseDateTime(defaultValue).toDate());
          } else {
            field = Field.create(Field.Type.valueOf(columnsToTypes.get(column).getLabel()), defaultValue);
          }
          defaultValues.put(column, field);
        } catch (IllegalArgumentException e) {
          issues.add(context.createConfigIssue(
            Groups.JDBC.name(),
            COLUMN_MAPPINGS,
            JdbcErrors.JDBC_03,
            column,
            defaultValue,
            e
          ));
        }
      }
    }

    return defaultValues.isEmpty() ? Optional.empty() : Optional.of(ImmutableList.of(defaultValues));
  }

  /** {@inheritDoc} */
  @Override
  public void destroy() {
    closeQuietly(dataSource);
    for (ExecutorService generationExecutor : generationExecutors) {
      generationExecutor.shutdown();
      try {
        generationExecutor.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException ex) {
        LOG.error("Interrupted while attempting to shutdown Generator thread", ex);
        Thread.currentThread().interrupt();
      }
    }
    super.destroy();
  }

  public void preprocess(Batch batch) throws StageException {
    //Gather all JDBC queries
    Iterator<Record> it = batch.getRecords();
    List<List<String>> preparedQueries = new ArrayList<>();
    for (int i =0; i < preprocessThreads; i++) {
      preparedQueries.add(new ArrayList<String>());
    }
    int recordNum = 0;
    while (it.hasNext()) {
      Record record = it.next();
      recordNum++;
      try {
        ELVars elVars = getContext().createELVars();
        RecordEL.setRecordInContext(elVars, record);
        String preparedQuery = queryEval.eval(elVars, query, String.class);
        preparedQueries.get((recordNum-1) % preprocessThreads).add(preparedQuery);
      } catch (ELEvalException e) {
        LOG.error(JdbcErrors.JDBC_01.getMessage(), query, e);
        throw new OnRecordErrorException(record, JdbcErrors.JDBC_01, query);
      }
    }

    for (int i =0; i < preprocessThreads; i++) {
      generationExecutors.add(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("JDBC Lookup "
          + "Cache Warmer"+i)
          .build()));
      final List<String> preparedQueriesPart = preparedQueries.get(i);
      generationExecutors.get(i).submit(() -> {
        try {
          for ( String query : preparedQueriesPart)
            cache.get(query);
        } catch (Throwable ex) {
          LOG.error("Error while producing records", ex);
        }
      });
    }
  }

  @Override
  public void process(Batch batch, SingleLaneBatchMaker batchMaker) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      cacheCleaner.periodicCleanUp();
    }
    //Cache warming
    if (preprocessThreads > 0) {
      preprocess(batch);
    }
    //Normal processing per record
    super.process(batch, batchMaker);
  }

  /** {@inheritDoc} */
  @Override
  protected void process(Record record, SingleLaneBatchMaker batchMaker) throws StageException {
    try {
      ELVars elVars = getContext().createELVars();
      RecordEL.setRecordInContext(elVars, record);
      String preparedQuery = queryEval.eval(elVars, query, String.class);
      Optional<List<Map<String, Field>>> entry = cache.get(preparedQuery);

      if (!entry.isPresent()) {
        // No results
        switch (missingValuesBehavior) {
          case SEND_TO_ERROR:
            LOG.error(JdbcErrors.JDBC_04.getMessage(), preparedQuery);
            errorRecordHandler.onError(new OnRecordErrorException(record, JdbcErrors.JDBC_04, preparedQuery));
            break;
          case PASS_RECORD_ON:
            batchMaker.addRecord(record);
            break;
          default:
            throw new IllegalStateException("Unknown missing value behavior: " + missingValuesBehavior);
        }
      } else {
        List<Map<String, Field>> values = entry.get();
        switch (multipleValuesBehavior) {
          case FIRST_ONLY:
            setFieldsInRecord(record, values.get(0));
            batchMaker.addRecord(record);
            break;
          case SPLIT_INTO_MULTIPLE_RECORDS:
            for(Map<String, Field> lookupItem : values) {
              Record newRecord = getContext().cloneRecord(record);
              setFieldsInRecord(newRecord, lookupItem);
              batchMaker.addRecord(newRecord);
            }
            break;
          default:
            throw new IllegalStateException("Unknown multiple value behavior: " + multipleValuesBehavior);
        }

      }

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

  private void setFieldsInRecord(Record record, Map<String, Field>fields) {
    for (Map.Entry<String, Field> entry : fields.entrySet()) {
      String columnName = entry.getKey();
      String fieldPath = columnsToFields.get(columnName);
      Field field = entry.getValue();
      if (fieldPath == null) {
        Field root = record.get();
        // No mapping
        switch (root.getType()) {
          case LIST:
            // Add new field to the end of the list
            fieldPath = "[" + root.getValueAsList().size() + "]";
            Map<String, Field> cell = new HashMap<>();
            cell.put("header", Field.create(columnName));
            cell.put("value", field);
            field = Field.create(cell);
            break;
          case LIST_MAP:
          case MAP:
            // Just use the column name
            fieldPath = "/" + columnName;
            break;
          default:
            break;
        }
      }
      record.set(fieldPath, field);
    }
  }

  @SuppressWarnings("unchecked")
  private LoadingCache<String, Optional<List<Map<String, Field>>>> buildCache() {
    JdbcLookupLoader loader = new JdbcLookupLoader(
      getContext(),
      dataSource,
      columnsToTypes,
      maxClobSize,
      maxBlobSize,
      errorRecordHandler
    );
    return LookupUtils.buildCache(loader, cacheConfig, defaultValue);
  }
}
