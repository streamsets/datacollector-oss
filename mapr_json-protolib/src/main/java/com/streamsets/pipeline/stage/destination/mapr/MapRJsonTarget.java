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
package com.streamsets.pipeline.stage.destination.mapr;

import com.google.common.annotations.VisibleForTesting;
import com.mapr.org.apache.hadoop.hbase.util.Bytes;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELEvalException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.api.ext.json.Mode;
import com.streamsets.pipeline.lib.el.ELUtils;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.streamsets.pipeline.lib.generator.DataGeneratorException;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactory;
import com.streamsets.pipeline.lib.generator.DataGeneratorFactoryBuilder;
import com.streamsets.pipeline.lib.generator.DataGeneratorFormat;
import com.streamsets.pipeline.lib.operation.OperationType;
import com.streamsets.pipeline.stage.common.DefaultErrorRecordHandler;
import com.streamsets.pipeline.stage.common.ErrorRecordHandler;
import com.streamsets.pipeline.stage.destination.mapr.loader.MapRJsonDocumentLoader;
import com.streamsets.pipeline.stage.destination.mapr.loader.MapRJsonDocumentLoaderException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.ojai.Document;
import org.ojai.exceptions.DecodingException;
import org.ojai.store.DocumentMutation;
import org.ojai.store.exceptions.DocumentExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.streamsets.pipeline.lib.operation.OperationType.DELETE_CODE;
import static com.streamsets.pipeline.lib.operation.OperationType.INSERT_CODE;
import static com.streamsets.pipeline.lib.operation.OperationType.UPDATE_CODE;

public class MapRJsonTarget extends BaseTarget {

  private static final Logger LOG = LoggerFactory.getLogger(MapRJsonTarget.class);
  private static final String DOLLAR_BRACE = "${";
  private static final String TABLE_NAME = "mapRJsonConfigBean.tableName";

  private MapRJsonConfigBean mapRJsonConfigBean;
  private ErrorRecordHandler errorRecordHandler;
  private DataGeneratorFactory generatorFactory;

  private ELEval tableNameEval;
  private ELVars tableNameVars;
  private String tab;
  private boolean hasEL;

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
          TABLE_NAME,
          Errors.MAPR_JSON_01
      ));

    } else {
      //EL with constants or String functions
      //evaluate expression and validate tableName
      if(mapRJsonConfigBean.tableName.contains(DOLLAR_BRACE)) {
        hasEL = true;

        tableNameEval = getContext().createELEval("tableName");
        tableNameVars = getContext().createELVars();
        ELUtils.validateExpression(
            tableNameEval,
            tableNameVars,
            mapRJsonConfigBean.tableName,
            getContext(),
            Groups.MAPR_JSON.name(),
            mapRJsonConfigBean.tableName,
            Errors.MAPR_JSON_16,
            String.class,
            issues
        );

      } else {
        tab = mapRJsonConfigBean.tableName;

      }
    }

    // check if the key field is empty.
    if (StringUtils.isEmpty(mapRJsonConfigBean.keyField)) {
      issues.add(getContext().createConfigIssue(
          Groups.MAPR_JSON.getLabel(),
          "mapRJsonConfigBean.keyField",
          Errors.MAPR_JSON_08
      ));
    }

    if (issues.isEmpty()) {
      generatorFactory = new DataGeneratorFactoryBuilder(getContext(), DataGeneratorFormat.JSON)
          .setMode(Mode.MULTIPLE_OBJECTS)
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
        resolveTableName(rec);

        int opType = OperationType.INSERT_CODE;
        if(rec.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE) != null) {
          opType = Integer.valueOf(rec.getHeader().getAttribute(OperationType.SDC_OPERATION_TYPE));
        }
        switch (opType) {
          case INSERT_CODE:
            performInsertOperation(rec);
            break;
          case DELETE_CODE:
            performDeleteOperation(rec);
            break;
          case UPDATE_CODE:
            performUpdateOperation(rec);
            break;
          default:
            throw new OnRecordErrorException(
                Errors.MAPR_JSON_17,
                OperationType.getLabelFromIntCode(opType),
                null
            );
        }
      } catch (OnRecordErrorException ee) {
        errorRecordHandler.onError(ee);
      }
    }
  }

  private void performInsertOperation(Record rec) throws StageException {
    ByteArrayOutputStream os = new ByteArrayOutputStream(1024 * 1024);
    createJson(os, rec);
    Document document = populateDocument(os, rec);
    setId(document, rec);
    doInsert(document, rec);
    flush();
  }

  private void performDeleteOperation(Record rec) throws StageException {
    doDelete(rec);
    flush();
  }

  private void performUpdateOperation(Record rec) throws StageException {
    ByteArrayOutputStream os = new ByteArrayOutputStream(1024 * 1024);
    createJson(os, rec);
    DocumentMutation document = populateDocumentMutation(rec);
    doUpdate(document, rec);
    flush();
  }

  private void resolveTableName(Record rec) throws StageException {
    if(hasEL) {
      RecordEL.setRecordInContext(tableNameVars, rec);
      try {
        tab = tableNameEval.eval(tableNameVars, mapRJsonConfigBean.tableName, String.class);
      } catch (ELEvalException ex) {
        LOG.error(Errors.MAPR_JSON_16.getMessage(), mapRJsonConfigBean.tableName, ex);
        throw new OnRecordErrorException(rec, Errors.MAPR_JSON_16, mapRJsonConfigBean.tableName, ex);
      }
    }
    // if tableName contains an EL, "tab" was initialized above,
    // otherwise use the tableName from the UI
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
    if(!rec.has(mapRJsonConfigBean.keyField)) {
      LOG.error(Errors.MAPR_JSON_15.getMessage(), mapRJsonConfigBean.keyField);
      throw new OnRecordErrorException(rec, Errors.MAPR_JSON_15, mapRJsonConfigBean.keyField);
    }

    field = rec.get(mapRJsonConfigBean.keyField);

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

  @VisibleForTesting
  static byte [] convertToByteArray(Field field, Record rec) throws OnRecordErrorException {

    switch (field.getType()) {
      case DOUBLE:
        return Bytes.toBytes(field.getValueAsDouble());

      case FLOAT:
        return Bytes.toBytes(field.getValueAsFloat());

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
      case CHAR:
      case BYTE:
      default:
        throw new OnRecordErrorException(rec, Errors.MAPR_JSON_14, field.getType().name());
    }
  }

  private Document populateDocument(ByteArrayOutputStream os, Record rec) throws OnRecordErrorException {
    Document document;
    try {
      document = MapRJsonDocumentLoader.createDocument(new String(os.toByteArray(), StandardCharsets.UTF_8));
    } catch (DecodingException ex) {
      LOG.error(Errors.MAPR_JSON_10.getMessage(), ex.toString(), ex);
      throw new OnRecordErrorException(rec, Errors.MAPR_JSON_10, ex.toString(), ex);
    }
    return document;
  }

  private DocumentMutation populateDocumentMutation(Record rec) throws OnRecordErrorException {
    DocumentMutation documentMutation = MapRJsonDocumentLoader.createDocumentMutation();
    boolean replace = mapRJsonConfigBean.setOrReplace == SetOrReplace.REPLACE;

    if(rec != null && (rec.get().getType() == Field.Type.LIST_MAP || rec.get().getType() == Field.Type.MAP)) {
      Map<String, Field> fields = rec.get().getValueAsMap();
      for(Map.Entry<String, Field> entry : fields.entrySet()) {
        String path = entry.getKey();
        Field field = entry.getValue();

        //don't add the keyField to the document mutation set. that gets added later
        if(entry.getKey().equals(mapRJsonConfigBean.keyField)) {
          continue;
        }
        switch(field.getType()) {
          case DOUBLE:
            double d = field.getValueAsDouble();
            documentMutation = (replace ? documentMutation.setOrReplace(path, d) : documentMutation.set(path, d));
            break;
          case FLOAT:
            float f = field.getValueAsFloat();
            documentMutation = (replace ? documentMutation.setOrReplace(path, f) : documentMutation.set(path, f));
            break;
          case INTEGER:
            int i = field.getValueAsInteger();
            documentMutation = (replace ? documentMutation.setOrReplace(path, i) : documentMutation.set(path, i));
            break;
          case SHORT:
            short s = field.getValueAsShort();
            documentMutation = (replace ? documentMutation.setOrReplace(path, s) : documentMutation.set(path, s));
            break;
          case LONG:
          case DATE:
          case TIME:
          case DATETIME:
            long l = field.getValueAsLong();
            documentMutation = (replace ? documentMutation.setOrReplace(path, l) : documentMutation.set(path, l));
            break;
          case STRING:
            String st = field.getValueAsString();
            documentMutation = (replace ? documentMutation.setOrReplace(path, st) : documentMutation.set(path, st));
            break;
          case BYTE_ARRAY:
            byte[] ba = field.getValueAsByteArray();
            documentMutation = (replace ? documentMutation.setOrReplace(path, ByteBuffer.wrap(ba)) : documentMutation.set(path, ByteBuffer.wrap(ba)));
            break;
          case BOOLEAN:
          case MAP:
          case LIST:
          case LIST_MAP:
          case CHAR:
          case BYTE:
          default:
            throw new OnRecordErrorException(rec, Errors.MAPR_JSON_14, field.getType().name());
        }
      }
    }

    return documentMutation;
  }

  private void doInsert(Document document, Record record) throws StageException {
    if (mapRJsonConfigBean.insertOrReplace == InsertOrReplace.REPLACE) {
      try {
        MapRJsonDocumentLoader.commitReplace(tab, document, mapRJsonConfigBean.createTable);

      } catch (MapRJsonDocumentLoaderException ex) {
        LOG.error(Errors.MAPR_JSON_06.getMessage(), ex.toString(), ex);
        throw new StageException(Errors.MAPR_JSON_06, ex.toString(), ex);
      }
    } else {
      try {
        MapRJsonDocumentLoader.commit(tab, document, mapRJsonConfigBean.createTable);
      } catch (DocumentExistsException ex) {
        LOG.error(Errors.MAPR_JSON_07.getMessage(), ex.toString(), ex);
        throw new OnRecordErrorException(record, Errors.MAPR_JSON_07, ex.toString(), ex);

      } catch (MapRJsonDocumentLoaderException ex) {
        LOG.error(Errors.MAPR_JSON_06.getMessage(), ex.toString(), ex);
        throw new StageException(Errors.MAPR_JSON_06, ex.toString(), ex);
      }
    }
  }

  private void doDelete(Record rec) throws StageException {
    // check if the key column exists...
    Field field;
    if(!rec.has(mapRJsonConfigBean.keyField)) {
      LOG.error(Errors.MAPR_JSON_15.getMessage(), mapRJsonConfigBean.keyField);
      throw new OnRecordErrorException(rec, Errors.MAPR_JSON_15, mapRJsonConfigBean.keyField);
    }

    field = rec.get(mapRJsonConfigBean.keyField);

    try {
      if (mapRJsonConfigBean.isBinaryRowKey || field.getType() == Field.Type.BYTE_ARRAY) {
        MapRJsonDocumentLoader.deleteRow(tab, new String(convertToByteArray(field, rec), "UTF-8"));
      } else {
        String str = field.getValueAsString();
        if (StringUtils.isEmpty(str)) {
          LOG.error(Errors.MAPR_JSON_11.getMessage(), mapRJsonConfigBean.keyField);
          throw new OnRecordErrorException(rec, Errors.MAPR_JSON_11, mapRJsonConfigBean.keyField);
        }
        MapRJsonDocumentLoader.deleteRow(tab, str);
      }
    } catch (MapRJsonDocumentLoaderException ex) {
      LOG.error(Errors.MAPR_JSON_19.getMessage(), ex.getMessage());
      throw new StageException(Errors.MAPR_JSON_19, ex);
    } catch (UnsupportedEncodingException ex) {
      LOG.error(Errors.MAPR_JSON_12.getMessage(), mapRJsonConfigBean.keyField, field.getType().name());
      throw new OnRecordErrorException(rec,Errors.MAPR_JSON_12, mapRJsonConfigBean.keyField, field.getType().name());
    }
  }

  private void doUpdate(DocumentMutation documentMutation, Record rec) throws StageException {
    // check if the key column exists...
    Field field;
    if(!rec.has(mapRJsonConfigBean.keyField)) {
      LOG.error(Errors.MAPR_JSON_15.getMessage(), mapRJsonConfigBean.keyField);
      throw new OnRecordErrorException(rec, Errors.MAPR_JSON_15, mapRJsonConfigBean.keyField);
    }

    field = rec.get(mapRJsonConfigBean.keyField);

    try {
      if (mapRJsonConfigBean.isBinaryRowKey || field.getType() == Field.Type.BYTE_ARRAY) {
        MapRJsonDocumentLoader.commitMutation(tab, new String(convertToByteArray(field, rec), "UTF-8"), documentMutation);
      } else {
        String str = field.getValueAsString();
        if (StringUtils.isEmpty(str)) {
          LOG.error(Errors.MAPR_JSON_11.getMessage(), mapRJsonConfigBean.keyField);
          throw new OnRecordErrorException(rec, Errors.MAPR_JSON_11, mapRJsonConfigBean.keyField);
        }
        MapRJsonDocumentLoader.commitMutation(tab, str, documentMutation);
      }
    } catch (MapRJsonDocumentLoaderException ex) {
      LOG.error(Errors.MAPR_JSON_18.getMessage(), ex.getMessage());
      throw new StageException(Errors.MAPR_JSON_18, ex);
    } catch (UnsupportedEncodingException ex) {
      LOG.error(Errors.MAPR_JSON_12.getMessage(), mapRJsonConfigBean.keyField, field.getType().name());
      throw new OnRecordErrorException(rec,Errors.MAPR_JSON_12, mapRJsonConfigBean.keyField, field.getType().name());
    }
  }

  private void flush() throws StageException {
    try {
      MapRJsonDocumentLoader.flush(tab);
    } catch (MapRJsonDocumentLoaderException ex) {
      LOG.error(Errors.MAPR_JSON_04.getMessage(), mapRJsonConfigBean.tableName, ex);
      throw new StageException(Errors.MAPR_JSON_04, mapRJsonConfigBean.tableName, ex);
    }
  }

  @Override
  public void destroy() {
    MapRJsonDocumentLoader.close();
  }

}
