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
package com.streamsets.pipeline.stage.destination.mapr;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.exceptions.DBException;
import com.mapr.org.apache.hadoop.hbase.util.Bytes;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.config.JsonMode;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.ojai.Document;
import org.ojai.exceptions.DecodingException;
import org.ojai.store.exceptions.DocumentExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

public class MapRJsonTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(MapRJsonTarget.class);

  private Table table;
  private MapRJsonConfigBean mapRJsonConfigBean;
  private ErrorRecordHandler errorRecordHandler;
  private DataGeneratorFactory generatorFactory;

  public MapRJsonTarget(MapRJsonConfigBean mapRJsonConfigBean) {
    this.mapRJsonConfigBean = mapRJsonConfigBean;
  }

  @Override
  protected List<ConfigIssue> init() {

    List<ConfigIssue> issues = super.init();
    errorRecordHandler = new    DefaultErrorRecordHandler(getContext());

    if (StringUtils.isEmpty(mapRJsonConfigBean.tableName)) {
      issues.add(getContext().createConfigIssue(
          Groups.MAPR_JSON.getLabel(),
          "mapRJsonConfigBean.tableName",
          Errors.MAPR_JSON_01
          ));
    }

    if (StringUtils.isEmpty(mapRJsonConfigBean.keyField)) {
      issues.add(getContext().createConfigIssue(
          Groups.MAPR_JSON.getLabel(),
          "mapRJsonConfigBean.keyField",
          Errors.MAPR_JSON_08
      ));
    }

    try {
      table = MapRDB.getTable(mapRJsonConfigBean.tableName);
    } catch (DBException ex) {
      if (mapRJsonConfigBean.createTable) {
        try {
          table = MapRDB.createTable(mapRJsonConfigBean.tableName);
        } catch (DBException ee) {
          issues.add(getContext().createConfigIssue(
              Groups.MAPR_JSON.getLabel(),
              "mapRJsonConfigBean.tableName",
              Errors.MAPR_JSON_03,
              mapRJsonConfigBean.tableName,
              ee
          ));
        }
      } else {
        issues.add(getContext().createConfigIssue(
            Groups.MAPR_JSON.getLabel(),
            "mapRJsonConfigBean.tableName",
            Errors.MAPR_JSON_02,
            mapRJsonConfigBean.tableName,
            ex
        ));
      }
    }

    if (issues.isEmpty()) {
      generatorFactory = new DataGeneratorFactoryBuilder(getContext(), DataGeneratorFormat.JSON)
          .setMode(JsonMode.MULTIPLE_OBJECTS)
          .setMaxDataLen(-1)
          .setCharset(StandardCharsets.UTF_8)
          .build();
    }
    return issues;
  }

  @Override
  public void write(final Batch batch) throws StageException {

    Iterator<Record> iter = batch.getRecords();
    while (iter.hasNext()) {
      Record rec = iter.next();

      try {
        ByteArrayOutputStream os = new ByteArrayOutputStream(1024 * 1024);
        createJson(os, rec);
        Document document = populateDocument(os, rec);
        setId(document, rec);
        doInsert(document, rec);

      } catch(OnRecordErrorException ee) {
        errorRecordHandler.onError(ee);
      }
    }

  }

  private void createJson(OutputStream os, Record rec) throws OnRecordErrorException {

    try (DataGenerator generator = generatorFactory.getGenerator(os)) {
      generator.write(rec);

    } catch (IOException | DataGeneratorException ex) {
      LOG.error(Errors.MAPR_JSON_09.getMessage(), ex.toString(), ex);
      throw new OnRecordErrorException(rec, Errors.MAPR_JSON_09, ex.toString(), ex);
    }

  }

  private void setId(Document document, Record rec) throws OnRecordErrorException {

    // check if the key column exists...
    Field field;
    try {
      field = rec.get(mapRJsonConfigBean.keyField);

    } catch (IllegalArgumentException ex) {
      LOG.info(Errors.MAPR_JSON_11.getMessage(), mapRJsonConfigBean.keyField, ex);
      throw new OnRecordErrorException(rec, Errors.MAPR_JSON_11, mapRJsonConfigBean.keyField, ex);
    }

    if(mapRJsonConfigBean.isBinaryRowKey || field.getType() == Field.Type.BYTE_ARRAY) {
      try {
        byte [] bArr = convertToByteArray(field, rec);
        document.setId(ByteBuffer.wrap(bArr));

      } catch (IllegalArgumentException ex) {
        LOG.error(Errors.MAPR_JSON_12.getMessage(), mapRJsonConfigBean.keyField, field.getType().name(), ex);
        throw new OnRecordErrorException(rec, Errors.MAPR_JSON_12, mapRJsonConfigBean.keyField, field.getType().name(), ex);
      }

    } else {
      try {
        String str = field.getValueAsString();
        if (StringUtils.isEmpty(str)) {
          LOG.error(Errors.MAPR_JSON_11.getMessage(), mapRJsonConfigBean.keyField);
          throw new OnRecordErrorException(rec, Errors.MAPR_JSON_11, mapRJsonConfigBean.keyField);
        }
        document.setId(str);

      } catch (IllegalArgumentException ex) {
        LOG.error(Errors.MAPR_JSON_13.getMessage(), mapRJsonConfigBean.keyField, ex);
        throw new OnRecordErrorException(rec, Errors.MAPR_JSON_13, mapRJsonConfigBean.keyField, ex);
      }
    }

  }

  private byte [] convertToByteArray(Field field, Record rec) throws OnRecordErrorException {

    switch (field.getType()) {
      case INTEGER:
        return Bytes.toBytes(field.getValueAsInteger());

      case SHORT:
        return Bytes.toBytes(field.getValueAsShort());

      case LONG:
      case DATE:
      case TIME:
      case DATETIME:
        return Bytes.toBytes(field.getValueAsLong());

      case STRING:
        return Bytes.toBytes(field.getValueAsString());

      case BYTE_ARRAY:
        return field.getValueAsByteArray();

      case BOOLEAN:
      case MAP:
      case LIST:
      case LIST_MAP:
      case DOUBLE:
      case FLOAT:
      case CHAR:
      case BYTE:
      default:
        throw new OnRecordErrorException(rec, Errors.MAPR_JSON_14, field.getType().name());
    }
  }

  private Document populateDocument(ByteArrayOutputStream os, Record rec) throws OnRecordErrorException {
    Document document;
    try {
      document = MapRDB.newDocument(new String(os.toByteArray(), StandardCharsets.UTF_8));

    } catch (DecodingException ex) {
      LOG.error(Errors.MAPR_JSON_10.getMessage(), ex.toString(), ex);
      throw new OnRecordErrorException(rec, Errors.MAPR_JSON_10, ex.toString(), ex);
    }
    return document;

  }

  private void doInsert(Document document, Record record) throws StageException {

    if (mapRJsonConfigBean.insertOrReplace == InsertOrReplace.REPLACE) {
      try {
        table.insertOrReplace(document);
      } catch (DBException ex) {
        LOG.info(Errors.MAPR_JSON_06.getMessage(), ex.toString(), ex);
        throw new StageException(Errors.MAPR_JSON_06, ex.toString(), ex);
      }

    } else {
      try {
        table.insert(document);

      } catch (DocumentExistsException ex) {
        LOG.error(Errors.MAPR_JSON_07.getMessage(), ex.toString(), ex);
        throw new OnRecordErrorException(record, Errors.MAPR_JSON_07, ex.toString(), ex);

      } catch (DBException ex) {
        LOG.error(Errors.MAPR_JSON_06.getMessage(), ex.toString(), ex);
        throw new StageException(Errors.MAPR_JSON_06, ex.toString(), ex);
      }

    }

    try {
      table.flush();

    } catch (DBException ex) {
      LOG.error(Errors.MAPR_JSON_04.getMessage(), mapRJsonConfigBean.tableName, ex);
      throw new StageException(Errors.MAPR_JSON_04, mapRJsonConfigBean.tableName, ex);
    }

  }

  @Override
  public void destroy() {
    if (table != null) {
      try {
        table.close();
        table = null;

      } catch (DBException ex) {
        LOG.error(Errors.MAPR_JSON_05.getMessage(), mapRJsonConfigBean.tableName, ex);
      }
    }
  }

}
