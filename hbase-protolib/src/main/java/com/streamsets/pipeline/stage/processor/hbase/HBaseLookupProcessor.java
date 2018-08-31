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
package com.streamsets.pipeline.stage.processor.hbase;

import com.google.common.base.CharMatcher;
import com.google.common.base.Optional;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.BatchMaker;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseProcessor;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.impl.Utils;
import com.streamsets.pipeline.lib.cache.CacheCleaner;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.hbase.common.Errors;
import com.streamsets.pipeline.lib.hbase.common.HBaseColumn;
import com.streamsets.pipeline.lib.hbase.common.HBaseConnectionHelper;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.processor.kv.LookupMode;
import com.streamsets.pipeline.stage.processor.kv.LookupUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HBaseLookupProcessor extends BaseProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseLookupProcessor.class);
  private static final String ROW_EXPR = "rowExpr";
  private static final String COLUMN_EXPR = "columnExpr";
  private static final String TIMESTAMP_EXPR = "timestampExpr";
  private HBaseLookupConfig conf;
  private Configuration hbaseConf;
  private ErrorRecordHandler errorRecordHandler;
  private ELEval keyExprEval;
  private ELEval columnExprEval;
  private ELEval timestampExprEval;
  private HBaseStore store;
  private LoadingCache<Pair<String, HBaseColumn>, Optional<String>> cache;
  private CacheCleaner cacheCleaner;
  private HBaseConnectionHelper hbaseConnectionHelper;

  public HBaseLookupProcessor(HBaseLookupConfig conf) {
    if (null != conf.hBaseConnectionConfig.zookeeperQuorum) {
      conf.hBaseConnectionConfig.zookeeperQuorum =
        CharMatcher.WHITESPACE.removeFrom(conf.hBaseConnectionConfig.zookeeperQuorum);
    }
    this.conf = conf;
    this.hbaseConnectionHelper = new HBaseConnectionHelper();
  }

  @SuppressWarnings("unchecked")
  @Override
  protected List<ConfigIssue> init() {
    final List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    keyExprEval = getContext().createELEval(ROW_EXPR);
    columnExprEval = getContext().createELEval(COLUMN_EXPR);
    timestampExprEval = getContext().createELEval(TIMESTAMP_EXPR);

    if(conf.lookups.isEmpty()) {
      issues.add(getContext().createConfigIssue(
          conf.lookups.toString(),
          conf.lookups.toString(),
          Errors.HBASE_36
      ));
    }

    // Validate EL Expression
    for(HBaseLookupParameterConfig lookup : conf.lookups) {
      // Row key is required
      if(lookup.rowExpr.length() < 1) {
        issues.add(getContext().createConfigIssue(
            lookup.rowExpr,
            lookup.rowExpr,
            Errors.HBASE_35
        ));
      } else {
        ELUtils.validateExpression(
            keyExprEval,
            getContext().createELVars(),
            lookup.rowExpr,
            getContext(),
            Groups.HBASE.getLabel(),
            lookup.rowExpr,
            Errors.HBASE_38,
            String.class,
            issues
        );
      }

      // Column and timestamps are optional
      ELUtils.validateExpression(
          columnExprEval,
          getContext().createELVars(),
          lookup.columnExpr,
          getContext(),
          Groups.HBASE.getLabel(),
          lookup.columnExpr,
          Errors.HBASE_38,
          String.class,
          issues
      );

      ELUtils.validateExpression(
          timestampExprEval,
          getContext().createELVars(),
          lookup.timestampExpr,
          getContext(),
          Groups.HBASE.getLabel(),
          lookup.timestampExpr,
          Errors.HBASE_38,
          Date.class,
          issues
      );

      if(lookup.outputFieldPath.length() < 1) {
        issues.add(getContext().createConfigIssue(
            lookup.outputFieldPath,
            lookup.outputFieldPath,
            Errors.HBASE_40
        ));
      }
    }

    if(issues.isEmpty()) {
      hbaseConf = hbaseConnectionHelper.getHBaseConfiguration(issues,
          getContext(),
          Groups.HBASE.getLabel(),
          conf.hBaseConnectionConfig.hbaseConfDir,
          conf.hBaseConnectionConfig.tableName,
          conf.hBaseConnectionConfig.hbaseConfigs
      );

      HBaseConnectionHelper.validateQuorumConfigs(issues,
          getContext(),
          Groups.HBASE.getLabel(),
          conf.hBaseConnectionConfig.zookeeperQuorum,
          conf.hBaseConnectionConfig.zookeeperParentZNode,
          conf.hBaseConnectionConfig.clientPort
      );

      hbaseConnectionHelper.validateSecurityConfigs(issues,
          getContext(),
          Groups.HBASE.getLabel(),
          conf.hBaseConnectionConfig.hbaseUser,
          hbaseConf,
          conf.hBaseConnectionConfig.kerberosAuth
      );
    }

    if(issues.isEmpty()) {
      HBaseConnectionHelper.setIfNotNull(hbaseConf, HConstants.ZOOKEEPER_QUORUM, conf.hBaseConnectionConfig.zookeeperQuorum);
      hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, conf.hBaseConnectionConfig.clientPort);
      HBaseConnectionHelper.setIfNotNull(hbaseConf, HConstants.ZOOKEEPER_ZNODE_PARENT, conf.hBaseConnectionConfig.zookeeperParentZNode);
    }

    if (issues.isEmpty()) {
      try {
        hbaseConnectionHelper.getUGI().doAs((PrivilegedExceptionAction<HTableDescriptor>) () -> HBaseConnectionHelper
            .checkConnectionAndTableExistence(
          issues,
          getContext(),
          hbaseConf,
          Groups.HBASE.getLabel(),
          conf.hBaseConnectionConfig.tableName
        ));
      } catch (Exception e) {
        LOG.warn("Unexpected exception", e.toString());
        throw new RuntimeException(e);
      }
    }

    if(issues.isEmpty()) {
      try {
        hbaseConnectionHelper.getUGI().doAs((PrivilegedExceptionAction<Void>) () -> {
          keyExprEval = getContext().createELEval("rowExpr");
          store = new HBaseStore(conf, hbaseConf);
          return null;
        });
      } catch (Exception e) {
        LOG.error(Errors.HBASE_36.getMessage(), e.toString(), e);
        issues.add(getContext().createConfigIssue(
            conf.hBaseConnectionConfig.tableName,
            conf.hBaseConnectionConfig.tableName,
            Errors.HBASE_36,
            e.toString(),
            e
        ));
      }
      cache = LookupUtils.buildCache(store, conf.cache);

      cacheCleaner = new CacheCleaner(cache, "HBaseLookupProcessor", 10 * 60 * 1000);
    }
    return issues;
  }

  @Override
  public void process(final Batch batch, final BatchMaker batchMaker) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // No records - take the opportunity to clean up the cache so that we don't hold on to memory indefinitely
      cacheCleaner.periodicCleanUp();
      return;
    }

    if(conf.mode == LookupMode.BATCH) {
      doBatchLookup(batch, batchMaker);
    } else if(conf.mode == LookupMode.RECORD) {
      doRecordLookup(batch, batchMaker);
    } else {
      throw new IllegalArgumentException(Utils.format("Unrecognized lookup mode: '{}'", conf.mode));
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    if(store != null) {
      try {
        hbaseConnectionHelper.getUGI().doAs((PrivilegedExceptionAction<Void>) () -> {
          store.close();
          return null;
        });
      } catch (IOException | InterruptedException e) {
        LOG.warn("Unexpected exception", e);
        throw new RuntimeException(e);
      }
    }
  }

  private void doRecordLookup(Batch batch, BatchMaker batchMaker) throws StageException {
    Iterator<Record> records;
    records = batch.getRecords();
    Record record;
    while (records.hasNext()) {
      record = records.next();
      ELVars elVars = getContext().createELVars();
      RecordEL.setRecordInContext(elVars, record);
      try {
        for (HBaseLookupParameterConfig parameter : conf.lookups) {
          final Pair<String, HBaseColumn> key = getKey(record, parameter);

          if (key != null && !key.getKey().trim().isEmpty()) {
            Optional<String> value = hbaseConnectionHelper.getUGI().doAs((PrivilegedExceptionAction<Optional<String>>) () -> cache
                .getUnchecked(key));
            updateRecord(record, parameter, key, value);
          } else {
            handleEmptyKey(record, key);
          }
        }
      } catch (ELEvalException | JSONException e1) {
        LOG.error(Errors.HBASE_38.getMessage(), e1.toString(), e1);
        errorRecordHandler.onError(new OnRecordErrorException(record, Errors.HBASE_38, e1.toString()));
      } catch (IOException | InterruptedException | UncheckedExecutionException e) {
        HBaseConnectionHelper.handleHBaseException(e, ImmutableList.of(record).iterator(), errorRecordHandler);
      }

      batchMaker.addRecord(record);
    }
  }

  private void  doBatchLookup(Batch batch, BatchMaker batchMaker) throws StageException {
    Iterator<Record> records = batch.getRecords();
    final Set<Pair<String, HBaseColumn>> keys = getKeyColumnListMap(batch);

    try {
      Map<Pair<String, HBaseColumn>, Optional<String>> values = hbaseConnectionHelper.getUGI()
        .doAs((PrivilegedExceptionAction<ImmutableMap<Pair<String, HBaseColumn>, Optional<String>>>) () -> cache.getAll(keys));
      Record record;
      while (records.hasNext()) {
        record = records.next();
        for (HBaseLookupParameterConfig parameter : conf.lookups) {
          Pair<String, HBaseColumn> key = getKey(record, parameter);

          if (key != null && !key.getKey().trim().isEmpty()) {
            Optional<String> value = hbaseConnectionHelper.getUGI().doAs((PrivilegedExceptionAction<Optional<String>>) () -> cache
                .getUnchecked(key));
            updateRecord(record, parameter, key, value);
          } else {
            handleEmptyKey(record, key);
          }
        }
        batchMaker.addRecord(record);
      }
    } catch (ELEvalException | JSONException e1) {
      records = batch.getRecords();
      while(records.hasNext()) {
        Record record = records.next();
        LOG.error(Errors.HBASE_38.getMessage(), e1.toString(), e1);
        errorRecordHandler.onError(new OnRecordErrorException(record, Errors.HBASE_38, e1.toString()));
      }
    } catch (IOException | InterruptedException | UndeclaredThrowableException e2) {
      HBaseConnectionHelper.handleHBaseException(e2, records, errorRecordHandler);
    }
  }

  private void handleEmptyKey(Record record, Pair<String, HBaseColumn> key) throws StageException {
    if (conf.ignoreMissingFieldPath) {
      LOG.debug(
          Errors.HBASE_41.getMessage(),
          record,
          key.getKey(),
          Bytes.toString(key.getValue().getCf()) + ":" + Bytes.toString(key.getValue().getQualifier()),
          key.getValue().getTimestamp()
      );
    } else {
      LOG.error(
          Errors.HBASE_41.getMessage(),
          record,
          key.getKey(),
          Bytes.toString(key.getValue().getCf()) + ":" + Bytes.toString(key.getValue().getQualifier()),
          key.getValue().getTimestamp()
      );
      errorRecordHandler.onError(new OnRecordErrorException(record, Errors.HBASE_41, record,
          key.getKey(),
          Bytes.toString(key.getValue().getCf()) + ":" + Bytes.toString(key.getValue().getQualifier()),
          key.getValue().getTimestamp()));
    }
  }

  private Set<Pair<String, HBaseColumn>> getKeyColumnListMap(Batch batch) throws StageException {
    Iterator<Record> records;
    records = batch.getRecords();
    Record record;
    Set<Pair<String, HBaseColumn>> keyList = new HashSet<>();
    while (records.hasNext()) {
      record = records.next();
      for (HBaseLookupParameterConfig parameters : conf.lookups) {
        Pair<String, HBaseColumn> key = getKey(record, parameters);
        if(key != null && !key.getKey().trim().isEmpty()) {
          keyList.add(key);
        } else {
          LOG.debug(
              "No key on Record '{}' with key:'{}', column:'{}', timestamp:'{}'",
              record,
              key.getKey(),
              Bytes.toString(key.getValue().getCf()) + ":" + Bytes.toString(key.getValue().getQualifier()),
              key.getValue().getTimestamp()
          );
        }
      }
    }
    return keyList;
  }

  private Pair<String, HBaseColumn> getKey(Record record, HBaseLookupParameterConfig config) throws ELEvalException {
    if (config.rowExpr.isEmpty()) {
      throw new IllegalArgumentException(Utils.format("Empty lookup Key Expression"));
    }

    ELVars elVars = getContext().createELVars();
    RecordEL.setRecordInContext(elVars, record);

    String rowKey = keyExprEval.eval(elVars, config.rowExpr, String.class);
    String column = columnExprEval.eval(elVars, config.columnExpr, String.class);
    byte[][] parts = KeyValue.parseColumn(Bytes.toBytes(column));
    byte[] cf = null;
    byte[] qualifier = null;
    if (parts.length == 2) {
      cf = parts[0];
      qualifier = parts[1];
    }
    Date timestamp = timestampExprEval.eval(elVars, config.timestampExpr, Date.class);
    HBaseColumn hBaseColumn = new HBaseColumn(cf, qualifier);
    if(timestamp != null) {
      hBaseColumn.setTimestamp(timestamp.getTime());
    }

    return Pair.of(rowKey, hBaseColumn);
  }

  private void updateRecord(Record record, HBaseLookupParameterConfig parameter, Pair<String, HBaseColumn> key, Optional<String> value)
      throws JSONException {
    // If the value does not exists in HBase, no updates on record
    if(value == null || !value.isPresent()) {
      LOG.debug(
          "No value found on Record '{}' with key:'{}', column:'{}', timestamp:'{}'",
          record,
          key.getKey(),
          Bytes.toString(key.getValue().getCf()) + ":" + Bytes.toString(key.getValue().getQualifier()),
          key.getValue().getTimestamp()
      );
      return;
    }

    // if the Column Expression is empty, return the row data ( columnName + value )
    if(parameter.columnExpr.isEmpty()) {
      JSONObject json = new JSONObject(value.get());
      Iterator<String> iter = json.keys();
      Map<String, Field> columnMap = new HashMap<>();
      while(iter.hasNext()) {
        String columnName = iter.next();
        String columnValue = json.get(columnName).toString();
        columnMap.put(columnName, Field.create(columnValue));
      }
      record.set(parameter.outputFieldPath, Field.create(columnMap));
    } else {
      record.set(parameter.outputFieldPath, Field.create(value.get()));
    }
  }
}
