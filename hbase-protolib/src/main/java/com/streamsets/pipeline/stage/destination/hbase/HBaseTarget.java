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
package com.streamsets.pipeline.stage.destination.hbase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.lineage.EndPointType;
import com.streamsets.pipeline.api.lineage.LineageEvent;
import com.streamsets.pipeline.api.lineage.LineageEventType;
import com.streamsets.pipeline.api.lineage.LineageSpecificAttribute;
import com.streamsets.pipeline.hbase.api.HBaseConnectionHelper;
import com.streamsets.pipeline.hbase.api.HBaseProducer;
import com.streamsets.pipeline.hbase.api.HBaseFactory;
import com.streamsets.pipeline.hbase.api.impl.AbstractHBaseConnectionHelper;
import com.streamsets.pipeline.hbase.api.common.producer.ColumnInfo;
import com.streamsets.pipeline.hbase.api.common.Errors;
import com.streamsets.pipeline.hbase.api.common.producer.Groups;
import com.streamsets.pipeline.hbase.api.common.producer.HBaseColumn;
import com.streamsets.pipeline.hbase.api.common.producer.HBaseConnectionConfig;
import com.streamsets.pipeline.hbase.api.common.producer.StorageType;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HBaseTarget extends BaseTarget {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseTarget.class);
  private static final String HBASE_FIELD_COLUMN_MAPPING = "hbaseFieldColumnMapping";

  private final String hbaseRowKey;
  private final List<HBaseFieldMappingConfig> hbaseFieldColumnMapping;
  private final Map<String, ColumnInfo> columnMappings = new HashMap<>();
  private final StorageType rowKeyStorageType;
  private final boolean implicitFieldMapping;
  private final boolean ignoreMissingField;
  private final boolean ignoreInvalidColumn;
  private final String timeDriver;
  private final HBaseConnectionConfig conf;
  private ErrorRecordHandler errorRecordHandler;
  private HBaseConnectionHelper hbaseConnectionHelper;
  private HBaseProducer hbaseProducer;

  public HBaseTarget(
      HBaseConnectionConfig conf,
      String hbaseRowKey,
      StorageType rowKeyStorageType,
      List<HBaseFieldMappingConfig> hbaseFieldColumnMapping,
      boolean implicitFieldMapping,
      boolean ignoreMissingField,
      boolean ignoreInvalidColumn,
      String timeDriver
  ) {
    this.conf = conf;
    // ZooKeeper Quorum may be null for MapRDBTarget
    if (conf.zookeeperQuorum != null) {
      this.conf.zookeeperQuorum = CharMatcher.WHITESPACE.removeFrom(conf.zookeeperQuorum);
    }
    this.hbaseRowKey = hbaseRowKey;
    this.hbaseFieldColumnMapping = hbaseFieldColumnMapping;
    this.rowKeyStorageType = rowKeyStorageType;
    this.implicitFieldMapping = implicitFieldMapping;
    this.ignoreMissingField = ignoreMissingField;
    this.ignoreInvalidColumn = ignoreInvalidColumn;
    this.timeDriver = timeDriver;
  }

  @Override
  protected List<ConfigIssue> init() {
    final List<ConfigIssue> issues = super.init();
    errorRecordHandler = new DefaultErrorRecordHandler(getContext());
    hbaseProducer = HBaseFactory.createProducer(getContext(), conf, errorRecordHandler);
    hbaseConnectionHelper = hbaseProducer.getHBaseConnectionHelper();
    hbaseConnectionHelper.createHBaseConfiguration(issues,
        getContext(),
        Groups.HBASE.name(),
        conf.hbaseConfDir,
        conf.tableName,
        conf.hbaseConfigs
    );

    if (isReallyHBase()){
      hbaseProducer.validateQuorumConfigs(issues);
    }
    hbaseConnectionHelper.validateSecurityConfigs(issues,
        getContext(),
        Groups.HBASE.name(),
        conf.hbaseUser,
        conf.kerberosAuth
    );

    if (issues.isEmpty()) {
      AbstractHBaseConnectionHelper.setIfNotNull(HConstants.ZOOKEEPER_QUORUM, conf.zookeeperQuorum);
      hbaseConnectionHelper.setConfigurationIntProperty(HConstants.ZOOKEEPER_CLIENT_PORT, conf.clientPort);
      AbstractHBaseConnectionHelper.setIfNotNull(HConstants.ZOOKEEPER_ZNODE_PARENT, conf.zookeeperParentZNode);
    }

    HTableDescriptor hTableDescriptor = null;
    if (issues.isEmpty()) {
      try {
        hTableDescriptor = hbaseConnectionHelper.getUGI().doAs((PrivilegedExceptionAction<HTableDescriptor>) () -> {
          if (isReallyHBase()){
            hbaseProducer.checkHBaseAvailable(issues);
          }
          return hbaseConnectionHelper.checkConnectionAndTableExistence(issues,
              getContext(),
              Groups.HBASE.name(),
              conf.tableName
          );
        });
      } catch (InterruptedException | IOException e) {
        LOG.error("Unexpected exception: {}", e.toString(), e);
        throw Throwables.propagate(e);
      }
    }
    if (!issues.isEmpty()) {
      return issues;
    }

    if (!timeDriver.trim().isEmpty()) {
      try {
        hbaseProducer.setBatchTime();
        hbaseProducer.getRecordTime(getContext().createRecord("validateTimeDriver"), timeDriver);
      } catch (OnRecordErrorException ex) { // NOSONAR
        // OREE is just a wrapped ElEvalException, so unwrap this for the error message
        issues.add(getContext().createConfigIssue(Groups.HBASE.name(),
            "timeDriverEval",
            Errors.HBASE_33,
            ex.getCause().toString(),
            ex.getCause()
        ));
      }
    }

    validateStorageTypes(issues);
    if (issues.isEmpty() && hTableDescriptor != null) {
      for (HBaseFieldMappingConfig column : hbaseFieldColumnMapping) {
        HBaseColumn hbaseColumn = hbaseConnectionHelper.getColumn(column.columnName);
        if (hbaseColumn == null) {
          issues.add(getContext().createConfigIssue(Groups.HBASE.name(),
              HBASE_FIELD_COLUMN_MAPPING,
              Errors.HBASE_28,
              column.columnName,
              KeyValue.COLUMN_FAMILY_DELIMITER
          ));
        } else if (hTableDescriptor.getFamily(hbaseColumn.getCf()) == null) {
          issues.add(getContext().createConfigIssue(Groups.HBASE.name(),
              HBASE_FIELD_COLUMN_MAPPING,
              Errors.HBASE_32,
              column.columnName,
              conf.tableName
          ));
        } else {
          columnMappings.put(column.columnValue, new ColumnInfo(hbaseColumn, column.columnStorageType));
        }
      }
    }

    try {
      hbaseProducer.createTable(conf.tableName);
    } catch (InterruptedException | IOException ex) {
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), "tableName", Errors.HBASE_42, conf.tableName));
    }
    if (!issues.isEmpty()) {
      return issues;
    }

    LineageEvent event = getContext().createLineageEvent(LineageEventType.ENTITY_WRITTEN);
    event.setSpecificAttribute(LineageSpecificAttribute.ENTITY_NAME, conf.tableName);
    event.setSpecificAttribute(LineageSpecificAttribute.ENDPOINT_TYPE, EndPointType.HBASE.name());
    List<String> names = new ArrayList<>();
    for (HBaseFieldMappingConfig column : hbaseFieldColumnMapping) {
      names.add(column.columnName);
    }

    if (!names.isEmpty()) {
      event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, StringUtils.join(names, ", "));
    } else {
      if (implicitFieldMapping) {
        event.setSpecificAttribute(LineageSpecificAttribute.DESCRIPTION, "Implicit Field Mapping");
      }
    }
    getContext().publishLineageEvent(event);

    return issues;
  }

  protected boolean isReallyHBase() {
    return true;
  }

  @Override
  public void destroy() {
    super.destroy();
    hbaseProducer.destroyTable();
  }

  private void validateStorageTypes(List<ConfigIssue> issues) {
    switch (this.rowKeyStorageType) {
      case BINARY:
      case TEXT:
        break;
      default:
        issues.add(getContext().createConfigIssue(Groups.HBASE.name(),
            "rowKeyStorageType",
            Errors.HBASE_14,
            rowKeyStorageType
        ));
    }

    if (!hbaseFieldColumnMapping.isEmpty()) {
      for (HBaseFieldMappingConfig hbaseFieldMappingConfig : hbaseFieldColumnMapping) {
        switch (hbaseFieldMappingConfig.columnStorageType) {
          case BINARY:
          case JSON_STRING:
          case TEXT:
            break;
          default:
            issues.add(getContext().createConfigIssue(Groups.HBASE.name(),
                "columnStorageType",
                Errors.HBASE_15,
                hbaseFieldMappingConfig.columnStorageType
            ));
        }
      }
    } else if (!implicitFieldMapping) {
      issues.add(getContext().createConfigIssue(Groups.HBASE.name(), HBASE_FIELD_COLUMN_MAPPING, Errors.HBASE_18));
    }
  }

  @Override
  public void write(final Batch batch) throws StageException {
    hbaseProducer.setBatchTime();
    try {
      hbaseConnectionHelper.getUGI().doAs((PrivilegedExceptionAction<Void>) () -> {
        writeBatch(batch);
        return null;
      });
    } catch (Exception e) {
      throw throwStageException(e);
    }
  }

  private static StageException throwStageException(Exception e) {
    if (e instanceof RuntimeException) {
      Throwable cause = e.getCause();
      if (cause != null) {
        return new StageException(Errors.HBASE_26, cause, cause);
      }
    }
    return new StageException(Errors.HBASE_26, e, e);
  }

  private void writeBatch(Batch batch) throws StageException {
    hbaseProducer.writeRecordsInHBase(batch,
        rowKeyStorageType,
        hbaseRowKey,
        timeDriver,
        columnMappings,
        ignoreMissingField,
        implicitFieldMapping,
        ignoreInvalidColumn
    );
  }

  @VisibleForTesting
  Configuration getHBaseConfiguration() {
    return hbaseConnectionHelper.getHBaseConfiguration();
  }
}
