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
package com.streamsets.pipeline.stage.destination.mapr.v5_2.loader;

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.db.exceptions.DBException;
import com.streamsets.pipeline.stage.destination.mapr.loader.MapRJsonDocumentLoader;
import com.streamsets.pipeline.stage.destination.mapr.loader.MapRJsonDocumentLoaderException;
import org.ojai.Document;
import org.ojai.store.DocumentMutation;

import java.util.HashMap;
import java.util.Map;

public class MapRJson5_2DocumentLoader extends MapRJsonDocumentLoader {
  private Map<String, Table> theTables = new HashMap<>();

  @Override
  protected Document createDocumentInternal(String jsonString) {
    return MapRDB.newDocument(jsonString);
  }

  @Override
  protected DocumentMutation createDocumentMutationInternal() {
    return MapRDB.newMutation();
  }

  @Override
  protected void commitInternal(String tableName, Document document, boolean createTable) throws MapRJsonDocumentLoaderException {
    initTable(tableName, createTable);
    theTables.get(tableName).insert(document);
  }

  @Override
  protected void commitReplaceInternal(String tableName, Document document, boolean createTable) throws MapRJsonDocumentLoaderException {
    initTable(tableName, createTable);
    theTables.get(tableName).insertOrReplace(document);
  }

  @Override
  protected void commitMutationInternal(String tableName, String fieldPath, DocumentMutation documentMutation) throws MapRJsonDocumentLoaderException {
    initTable(tableName, false);
    theTables.get(tableName).update(fieldPath, documentMutation);
  }

  @Override
  protected void deleteRowInternal(String tableName, String id) throws MapRJsonDocumentLoaderException {
    initTable(tableName, false);
    theTables.get(tableName).delete(id);
  }

  @Override
  protected void flushInternal(String tableName) throws MapRJsonDocumentLoaderException {
    try {
      theTables.get(tableName).flush();
    } catch (DBException ex) {
      throw new MapRJsonDocumentLoaderException(
          "Encountered error flushing table changes",
          ex
      );
    }
  }

  @Override
  protected void closeInternal() {
    for(Map.Entry<String, Table> entry: theTables.entrySet()) {
      entry.getValue().close();
    }

    theTables.clear();
  }

  private void initTable(String tableName, boolean createTable) throws MapRJsonDocumentLoaderException {
    // if we've encountered this table name already, it's open.
    if(theTables.containsKey(tableName)) {
      theTables.get(tableName);
    }

    // open table (optionally create it)
    // and add to the group of open tables.
    try {
      theTables.put(tableName, MapRDB.getTable(tableName));
    } catch (DBException ex) {
      if (createTable) {
        try {
          theTables.put(tableName, MapRDB.createTable(tableName));
        } catch (DBException ex2) {
          throw new MapRJsonDocumentLoaderException(
              "Encountered error creating table " + tableName, ex2);
        }
      } else {
        throw new MapRJsonDocumentLoaderException(
            "MapR DB table " + tableName + "does not exist, and not configured to create", ex);
      }
    }
  }
}
