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
package com.streamsets.pipeline.stage.destination.mapr.loader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang.StringUtils;
import org.ojai.Document;
import org.ojai.store.DocumentMutation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

public abstract class MapRJsonDocumentLoader {
  /**
   * Instantiated MapRJson Document Loader (it will be different version based on MapR version that is being used).
   */
  private static MapRJsonDocumentLoader delegate;

  /**
   * Since the test delegate lives in different scope (test one), we load it as usual, but thread it differently.
   */
  private static MapRJsonDocumentLoader testDelegate;
  private static final String TEST_DELEGATE_NAME = "com.streamsets.pipeline.stage.destination.mapr.loader.MockMapRJsonDocumentLoader";

  /**
   * The parser to write records into a MapR document
   */
  private RecordToMapRJsonDocumentGenerator generator;

  @VisibleForTesting
  public static boolean isTest = false;

  static {
    int count = 0;

    Set<String> loaderClasses = new HashSet<>();
    for(MapRJsonDocumentLoader loader : ServiceLoader.load(MapRJsonDocumentLoader.class)) {
      String loaderName = loader.getClass().getName();
      loaderClasses.add(loaderName);

      if(TEST_DELEGATE_NAME.equals(loaderName)) {
        testDelegate = loader;
      } else {
        count++;
        delegate = loader;
      }
    }

    Utils.checkState(
        count == 1,
        Utils.format("Unexpected number of loaders, found {} instead of 1: {}",
            count,
            StringUtils.join(loaderClasses, ", ")));
  }

  protected abstract Document createNewEmptyDocument();
  protected abstract Document createDocumentInternal(String jsonString);
  protected abstract Document createDocumentInternal(Record record) throws IOException;
  protected abstract DocumentMutation createDocumentMutationInternal();
  protected abstract void commitInternal(String tableName, Document document, boolean createTable) throws MapRJsonDocumentLoaderException;
  protected abstract void commitReplaceInternal(String tableName, Document document, boolean createTable) throws MapRJsonDocumentLoaderException;
  protected abstract void commitMutationInternal(String tableName, String fieldPath, DocumentMutation documentMutation) throws MapRJsonDocumentLoaderException;
  protected abstract void deleteRowInternal(String tableName, String id) throws MapRJsonDocumentLoaderException;
  protected abstract void flushInternal(String tableName) throws MapRJsonDocumentLoaderException;
  protected abstract void closeInternal();

  protected RecordToMapRJsonDocumentGenerator getGenerator() {
    if (generator == null) {
      generator = new RecordToMapRJsonDocumentGenerator(this);
    }
    return generator;
  }

  public static Document createDocument(String jsonString) {
    if(isTest) {
      Preconditions.checkNotNull(testDelegate);
      return testDelegate.createDocumentInternal(jsonString);
    } else {
      Preconditions.checkNotNull(delegate);
      return delegate.createDocumentInternal(jsonString);
    }
  }

  public static Document createDocument(Record record) throws IOException{
    if(isTest) {
      Preconditions.checkNotNull(testDelegate);
      return testDelegate.createDocumentInternal(record);
    } else {
      Preconditions.checkNotNull(delegate);
      return delegate.createDocumentInternal(record);
    }
  }

  public static DocumentMutation createDocumentMutation() {
    if(isTest) {
      Preconditions.checkNotNull(testDelegate);
      return testDelegate.createDocumentMutationInternal();
    } else {
      Preconditions.checkNotNull(delegate);
      return delegate.createDocumentMutationInternal();
    }
  }

  public static void commit(String tableName, Document document, boolean createTable) throws MapRJsonDocumentLoaderException {
    if(isTest) {
      Preconditions.checkNotNull(testDelegate);
      testDelegate.commitInternal(tableName, document, createTable);
    } else {
      Preconditions.checkNotNull(delegate);
      delegate.commitInternal(tableName, document, createTable);
    }
  }

  public static void commitReplace(String tableName, Document document, boolean createTable) throws MapRJsonDocumentLoaderException {
    if(isTest) {
      Preconditions.checkNotNull(testDelegate);
      testDelegate.commitReplaceInternal(tableName, document, createTable);
    } else {
      Preconditions.checkNotNull(delegate);
      delegate.commitReplaceInternal(tableName, document, createTable);
    }
  }

  public static void commitMutation(String tableName, String fieldPath, DocumentMutation documentMutation) throws MapRJsonDocumentLoaderException {
    if(isTest) {
      Preconditions.checkNotNull(testDelegate);
      testDelegate.commitMutationInternal(tableName, fieldPath, documentMutation);
    } else {
      Preconditions.checkNotNull(delegate);
      delegate.commitMutationInternal(tableName, fieldPath, documentMutation);
    }
  }

  public static void deleteRow(String tableName, String id) throws MapRJsonDocumentLoaderException {
    if(isTest) {
      Preconditions.checkNotNull(testDelegate);
      testDelegate.deleteRowInternal(tableName, id);
    } else {
      Preconditions.checkNotNull(delegate);
      delegate.deleteRowInternal(tableName, id);
    }
  }

  public static void flush(String tableName) throws MapRJsonDocumentLoaderException {
    if(isTest) {
      Preconditions.checkNotNull(testDelegate);
      testDelegate.flushInternal(tableName);
    } else {
      Preconditions.checkNotNull(delegate);
      delegate.flushInternal(tableName);
    }
  }

  public static void close() {
    if(isTest) {
      Preconditions.checkNotNull(testDelegate);
      testDelegate.closeInternal();
    } else {
      Preconditions.checkNotNull(delegate);
      delegate.closeInternal();
    }
  }

  /**
   * <pre>
   * Writes all the field data into the document recursively. It covers all the data types available on SDC records.
   * This method can be called in 3 different modes:
   *  - doc   field   null    null  ->   the root mode.
   *  - doc   field   name    null  ->   the map mode.
   *  - null  field   null    list  ->   the list mode.
   *  The root mode: Called only at the beginning for the Record.get() value. Called from the writeRecordToDocument()
   *  method.
   *  The map mode: When the record contains a map of values.
   *  The list mode: When the record contains a list of values. This mode saves the data on the list instead of the
   *  document so the document param is null. Also the field has no name as it's a member of a list.
   *
   *    doc   - the document where to write field data.
   *    field - the field to be written on the doc.
   *    name  - Only for the map mode. That is the name of the entry that contains the field.
   *    list  - Only for the list mode. This is the list where the values will be stored.
   * </pre>
   */
  public static class RecordToMapRJsonDocumentGenerator {

    /**
     * Pointer to the loader class needed to create new empty Documents during the
     * parse process.
     */
    private MapRJsonDocumentLoader loader;

    public RecordToMapRJsonDocumentGenerator(MapRJsonDocumentLoader loader) {
      this.loader = loader;
    }

    public void writeRecordToDocument(Document doc, Record rec) throws IOException{
      // Call in a root mode.
      writeFieldToDocumentRoot(doc,rec.get());
    }

    /**
     * Map mode
     */
    private void writeFieldToDocumentMap(Document doc, Field field, String name) throws IOException {
      if (field.getValue() == null) {
        // On the Map mode just set the null on the document using the name.
        doc.setNull(name);
      } else {
        switch (field.getType()) {
          case FILE_REF:
            throw new IOException("Cannot serialize FileRef fields.");
          case MAP:
          case LIST_MAP:
            Document newDoc = loader.createNewEmptyDocument();
            Map<String, Field> map = field.getValueAsMap();
            for (Map.Entry<String, Field> fieldEntry : map.entrySet()) {
              String fieldName = fieldEntry.getKey();
              Field newField = fieldEntry.getValue();
              // recursive call in map mode.
              writeFieldToDocumentMap(newDoc, newField, fieldName);
            }
            // Map the new doc
            doc.set(name, newDoc);
            break;
          case LIST:
            List<Field> listOfFields = field.getValueAsList();
            List<Object> objsList = new ArrayList<>();
            for (Field f : listOfFields) {
              // recursive call in a list mode.
              writeFieldToDocumentList(f, objsList);
            }
            doc.setArray(name, objsList.toArray());
            break;
          case BOOLEAN:
            doc.set(name, field.getValueAsBoolean());
            break;
          case CHAR:
            doc.set(name, String.valueOf(field.getValueAsChar()));
            break;
          case BYTE:
            doc.set(name, new byte[]{field.getValueAsByte()});
            break;
          case SHORT:
            doc.set(name, field.getValueAsShort());
            break;
          case INTEGER:
            doc.set(name, field.getValueAsInteger());
            break;
          case LONG:
            doc.set(name, field.getValueAsLong());
            break;
          case FLOAT:
            doc.set(name, field.getValueAsFloat());
            break;
          case DOUBLE:
            doc.set(name, field.getValueAsDouble());
            break;
          case DATE:
            doc.set(name, field.getValueAsDate().getTime());
            break;
          case DATETIME:
            doc.set(name, field.getValueAsDatetime().getTime());
            break;
          case TIME:
            doc.set(name, field.getValueAsTime().getTime());
            break;
          case DECIMAL:
            doc.set(name, field.getValueAsDecimal());
            break;
          case STRING:
          case ZONED_DATETIME:
            doc.set(name, field.getValueAsString());
            break;
          case BYTE_ARRAY:
            doc.set(name, field.getValueAsByteArray());
            break;
          default:
            throw new IllegalStateException(String.format("Unrecognized field type (%s) in field: %s",
                field.getType().name(),
                field.toString()
            ));
        }
      }
    }

    /**
     * List mode
     */
    private void writeFieldToDocumentList(Field field, List list) throws IOException {
      if (field.getValue() == null) {
        list.add(null);
      } else {
        switch (field.getType()) {
          case FILE_REF:
            throw new IOException("Cannot serialize FileRef fields.");
          case MAP:
          case LIST_MAP:
            Document newDoc = loader.createNewEmptyDocument();
            Map<String, Field> map = field.getValueAsMap();
            for (Map.Entry<String, Field> fieldEntry : map.entrySet()) {
              String fieldName = fieldEntry.getKey();
              Field newField = fieldEntry.getValue();
              // recursive call in map mode.
              writeFieldToDocumentMap(newDoc, newField, fieldName);
            }
            // List mode
            list.add(newDoc);
            break;
          case LIST:
            List<Field> listOfFields = field.getValueAsList();
            List<Object> objsList = new ArrayList<>();
            for (Field f : listOfFields) {
              // recursive call in a list mode.
              writeFieldToDocumentList(f, objsList);
            }
            list.add(objsList);
            break;
          case BOOLEAN:
            list.add(field.getValueAsBoolean());
            break;
          case CHAR:
            list.add(String.valueOf(field.getValueAsChar()));
            break;
          case BYTE:
            list.add(new byte[]{field.getValueAsByte()});
            break;
          case SHORT:
            list.add(field.getValueAsShort());
            break;
          case INTEGER:
            list.add(field.getValueAsInteger());
            break;
          case LONG:
            list.add(field.getValueAsLong());
            break;
          case FLOAT:
            list.add(field.getValueAsFloat());
            break;
          case DOUBLE:
            list.add(field.getValueAsDouble());
            break;
          case DATE:
            list.add(field.getValueAsDate().getTime());
            break;
          case DATETIME:
            list.add(field.getValueAsDatetime().getTime());
            break;
          case TIME:
            list.add(field.getValueAsTime().getTime());
            break;
          case DECIMAL:
            list.add(field.getValueAsDecimal());
            break;
          case STRING:
          case ZONED_DATETIME:
            list.add(field.getValueAsString());
            break;
          case BYTE_ARRAY:
            list.add(field.getValueAsByteArray());
            break;
          default:
            throw new IllegalStateException(String.format(
                "Unrecognized field type (%s) in field: %s",
                field.getType().name(),
                field.toString())
            );
        }
      }
    }

    /**
     * Root mode
     */
    private void writeFieldToDocumentRoot(Document doc, Field field) throws IOException {
      if (field.getValue() != null) {
        switch (field.getType()) {
          case FILE_REF:
            throw new IOException("Cannot serialize FileRef fields.");
          case MAP:
          case LIST_MAP:
            Map<String, Field> map = field.getValueAsMap();
            for (Map.Entry<String, Field> fieldEntry : map.entrySet()) {
              String fieldName = fieldEntry.getKey();
              Field newField = fieldEntry.getValue();
              // recursive call in map mode.
              writeFieldToDocumentMap(doc, newField, fieldName);
            }
            break;
          case LIST:
            // Root mode
            throw new IllegalStateException("Wrong record format. The input record must be a MAP or LIST_MAP in order to be inserted in a MAPR JSON table.");
          case BOOLEAN:
          case CHAR:
          case BYTE:
          case SHORT:
          case INTEGER:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case DATE:
          case DATETIME:
          case TIME:
          case DECIMAL:
          case STRING:
          case BYTE_ARRAY:
          case ZONED_DATETIME:
            throw new IllegalStateException("Root record value must be a MAP a LIST_MAP.");
          default:
            throw new IllegalStateException(String.format(
                "Unrecognized field type (%s) in field: %s",
                field.getType().name(),
                field.toString())
            );
        }
      }
    }
  }
}
