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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
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
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.hbase.common.Errors;
import com.streamsets.pipeline.lib.hbase.common.HBaseColumn;
import com.streamsets.pipeline.lib.hbase.common.HBaseUtil;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.processor.kv.EvictionPolicyType;
import com.streamsets.pipeline.stage.processor.kv.LookupMode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
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
  private HBaseLookupConfig conf;
  private Configuration hbaseConf;
  private ErrorRecordHandler errorRecordHandler;
  private ELEval keyExprEval;
  private HBaseStore store;
  private LoadingCache<Pair<String, HBaseColumn>, Optional<String>> cache;
  private HTableDescriptor hTableDescriptor = null;

  public HBaseLookupProcessor(HBaseLookupConfig conf) {
    if (null != conf.hBaseConnectionConfig.zookeeperQuorum) {
      conf.hBaseConnectionConfig.zookeeperQuorum =
        CharMatcher.WHITESPACE.removeFrom(conf.hBaseConnectionConfig.zookeeperQuorum);
    }
    this.conf = conf;
  }

  @Override
  protected List<ConfigIssue> init() {
    final List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());

    hbaseConf = HBaseUtil.getHBaseConfiguration(
      issues,
      getContext(),
      Groups.HBASE.getLabel(),
      conf.hBaseConnectionConfig.hbaseConfDir,
      conf.hBaseConnectionConfig.zookeeperQuorum,
      conf.hBaseConnectionConfig.zookeeperParentZnode,
      conf.hBaseConnectionConfig.clientPort,
      conf.hBaseConnectionConfig.tableName,
      conf.hBaseConnectionConfig.kerberosAuth,
      conf.hBaseConnectionConfig.hbaseConfigs
    );

    HBaseUtil.validateQuorumConfigs(
      issues,
      getContext(),
      Groups.HBASE.getLabel(),
      conf.hBaseConnectionConfig.zookeeperQuorum,
      conf.hBaseConnectionConfig.zookeeperParentZnode,
      conf.hBaseConnectionConfig.clientPort
    );

    HBaseUtil.validateSecurityConfigs(issues, getContext(), Groups.HBASE.getLabel(), hbaseConf, conf.hBaseConnectionConfig.kerberosAuth);

    if(issues.isEmpty()) {
      HBaseUtil.setIfNotNull(hbaseConf, HConstants.ZOOKEEPER_QUORUM, conf.hBaseConnectionConfig.zookeeperQuorum);
      hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, conf.hBaseConnectionConfig.clientPort);
      HBaseUtil.setIfNotNull(hbaseConf, HConstants.ZOOKEEPER_ZNODE_PARENT, conf.hBaseConnectionConfig.zookeeperParentZnode);
    }

    if (issues.isEmpty()) {
      try {
        hTableDescriptor = HBaseUtil.getUGI(conf.hBaseConnectionConfig.hbaseUser).doAs(new PrivilegedExceptionAction<HTableDescriptor>() {
          @Override
          public HTableDescriptor run() throws Exception {
            return HBaseUtil.checkConnectionAndTableExistence(issues, getContext(), hbaseConf, Groups.HBASE.getLabel(), conf.hBaseConnectionConfig.tableName);
          }
        });
      } catch (Exception e) {
        LOG.warn("Unexpected exception", e);
        throw new RuntimeException(e);
      }
    }

    if(issues.isEmpty()) {
      try {
        HBaseUtil.getUGI(conf.hBaseConnectionConfig.hbaseUser).doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            keyExprEval = getContext().createELEval("rowExpr");
            store = new HBaseStore(conf, hbaseConf);
            return null;
          }
        });
      } catch (Exception e) {
        issues.add(getContext().createConfigIssue(
          conf.hBaseConnectionConfig.tableName,
          conf.hBaseConnectionConfig.tableName,
          Errors.HBASE_36
          )
        );
      }
      cache = buildCache();
    }
    return issues;
  }

  @Override
  public void process(final Batch batch, final BatchMaker batchMaker) throws StageException {
    if (!batch.getRecords().hasNext()) {
      // empty batch
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
        HBaseUtil.getUGI(conf.hBaseConnectionConfig.hbaseUser).doAs(new PrivilegedExceptionAction<Void>() {
          @Override
          public Void run() throws Exception {
            store.close();
            return null;
          }
        });
      } catch (IOException | InterruptedException e) {
        LOG.warn("Unexpected exception", e);
        throw new RuntimeException(e);
      }
    }
  }

  private LoadingCache<Pair<String, HBaseColumn>, Optional<String>> buildCache() {
    CacheBuilder cacheBuilder = CacheBuilder.newBuilder();
    if (!conf.cache.enabled) {
      return cacheBuilder.maximumSize(0)
        .build(store);
    }

    if(conf.cache.maxSize == -1) {
      conf.cache.maxSize = Long.MAX_VALUE;
    }

    // CacheBuilder doesn't support specifying type thus suffers from erasure, so
    // we build it with this if / else logic.
    if (conf.cache.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_ACCESS) {
      cacheBuilder.maximumSize(conf.cache.maxSize)
        .expireAfterAccess(conf.cache.expirationTime, conf.cache.timeUnit);
    } else if (conf.cache.evictionPolicyType == EvictionPolicyType.EXPIRE_AFTER_WRITE) {
      cacheBuilder.maximumSize(conf.cache.maxSize)
        .expireAfterWrite(conf.cache.expirationTime, conf.cache.timeUnit);
    } else {
      throw new IllegalArgumentException(
        Utils.format("Unrecognized EvictionPolicyType: '{}'", conf.cache.evictionPolicyType)
      );
    }
    return cacheBuilder.build(store);
  }

  private void doRecordLookup(Batch batch, BatchMaker batchMaker) throws StageException {
    Iterator<Record> records;
    records = batch.getRecords();
    Record record;
    while (records.hasNext()) {
      record = records.next();
      ELVars elVars = getContext().createELVars();
      RecordEL.setRecordInContext(elVars, record);
      for (HBaseLookupParameterConfig parameter : conf.lookups) {
        final Pair<String, HBaseColumn> key = getKey(record, elVars,parameter.rowExpr, parameter.columnExpr, parameter.timestampExpr);
        try {
          if (key != null) {
            Optional<String> value = HBaseUtil.getUGI(conf.hBaseConnectionConfig.hbaseUser).doAs(new PrivilegedExceptionAction<Optional<String>>() {
              @Override
              public Optional<String> run() throws Exception {
                return cache.getUnchecked(key);
              }
            });

            updateRecord(record, parameter, key, value);
          }
        } catch (IOException | InterruptedException | UncheckedExecutionException e) {
          HBaseUtil.handleNoColumnFamilyException(e, ImmutableList.of(record).iterator(), errorRecordHandler);
        }
      }
      batchMaker.addRecord(record);
    }
  }

  private void  doBatchLookup(Batch batch, BatchMaker batchMaker) throws StageException {
    Iterator<Record> records = batch.getRecords();
    final Set<Pair<String, HBaseColumn>> keys = getKeyColumnListMap(batch);

    try {
      Map<Pair<String, HBaseColumn>, Optional<String>> values = HBaseUtil.getUGI(conf.hBaseConnectionConfig.hbaseUser).doAs(new PrivilegedExceptionAction<Map<Pair<String, HBaseColumn>, Optional<String>>>() {
        @Override
        public Map<Pair<String, HBaseColumn>, Optional<String>> run() throws Exception {
          return cache.getAll(keys);
        }
      });
      Record record;
      while (records.hasNext()) {
        record = records.next();
        ELVars elVars = getContext().createELVars();
        RecordEL.setRecordInContext(elVars, record);

        for (HBaseLookupParameterConfig parameter : conf.lookups) {
          Pair<String, HBaseColumn> key = getKey(record, elVars, parameter.rowExpr, parameter.columnExpr, parameter.timestampExpr);
          Optional<String> value = values.get(key);
          updateRecord(record, parameter, key, value);
        }
        batchMaker.addRecord(record);
      }
    } catch (IOException | InterruptedException | UndeclaredThrowableException e) {
      HBaseUtil.handleNoColumnFamilyException(e, records, errorRecordHandler);
    }
  }

  private Set<Pair<String, HBaseColumn>> getKeyColumnListMap(Batch batch) throws StageException {
    Iterator<Record> records;
    records = batch.getRecords();
    Record record;
    Set<Pair<String, HBaseColumn>> keyList = new HashSet<>();
    while (records.hasNext()) {
      record = records.next();
      ELVars elVars = getContext().createELVars();
      RecordEL.setRecordInContext(elVars, record);

      for (HBaseLookupParameterConfig parameters : conf.lookups) {
        Pair<String, HBaseColumn> key = getKey(record, elVars, parameters.rowExpr, parameters.columnExpr, parameters.timestampExpr);
        if(key != null) {
          keyList.add(key);
        }
      }
    }
    return keyList;
  }

  private Pair<String, HBaseColumn> getKey(Record record, ELVars elVars, String rowExpr, String columnExpr, String timestampExpr) throws StageException {
    if (rowExpr.isEmpty()) {
      throw new IllegalArgumentException(Utils.format("Empty lookup Key Expression"));
    }

    String rowKey = "";
    HBaseColumn hBaseColumn = null;
    try {
      rowKey = keyExprEval.eval(elVars, rowExpr, String.class);
      hBaseColumn = getHBaseColumn(elVars, columnExpr, timestampExpr);
    } catch (ELEvalException e) {
      errorRecordHandler.onError(
        new OnRecordErrorException(
          record,
          Errors.HBASE_38,
          rowExpr,
          columnExpr
        )
      );
    }

    Pair<String, HBaseColumn> key = Pair.of(rowKey, hBaseColumn);
    return key;
  }

  private HBaseColumn getHBaseColumn(ELVars elVars, String columnExpr, String timestampExpr) throws ELEvalException {
    HBaseColumn hBaseColumn;
    // if Column Expression is empty, get the row data
    if (columnExpr.trim().isEmpty()) {
      hBaseColumn = new HBaseColumn();
    } else {
      hBaseColumn = HBaseUtil.getColumn(keyExprEval.eval(elVars, columnExpr, String.class));
    }

    if(!timestampExpr.trim().isEmpty()) {
      final Date timestamp = keyExprEval.eval(elVars, timestampExpr, Date.class);
      hBaseColumn.setTimestamp(timestamp.getTime());
    }
    return hBaseColumn;
  }

  private void updateRecord(Record record, HBaseLookupParameterConfig parameter, Pair<String, HBaseColumn> key, Optional<String> value) throws StageException {
    if(!value.isPresent()) {
      LOG.debug("No value found for key '{}' and/or column '{}'", key.getKey(), key.getValue().getCf() + ":" + key.getValue().getQualifier());
      return;
    }

    // if the Column Expression is empty, return the row data ( columnName + value )
    if(parameter.columnExpr.isEmpty()) {
      try {
        JSONObject json = new JSONObject(value.get());
        Iterator<String> iter = json.keys();
        Map<String, Field> columnMap = new HashMap<>();
        while(iter.hasNext()) {
          String columnName = iter.next();
          String columnValue = json.get(columnName).toString();
          columnMap.put(columnName, Field.create(columnValue));
        }
        record.set(parameter.outputFieldPath, Field.create(columnMap));
      } catch (JSONException ex) {
        errorRecordHandler.onError(
          new OnRecordErrorException(
            record,
            Errors.HBASE_10,
            value.get()
          )
        );
      }
    } else {
      record.set(parameter.outputFieldPath, Field.create(value.get()));
    }
  }
}
