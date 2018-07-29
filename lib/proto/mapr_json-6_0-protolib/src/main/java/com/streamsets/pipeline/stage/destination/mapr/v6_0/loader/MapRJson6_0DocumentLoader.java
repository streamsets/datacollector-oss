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
package com.streamsets.pipeline.stage.destination.mapr.v6_0.loader;

import com.mapr.db.MapRDB;
import com.mapr.db.exceptions.DBException;
import com.streamsets.pipeline.stage.destination.mapr.loader.MapRJsonDocumentLoader;
import com.streamsets.pipeline.stage.destination.mapr.loader.MapRJsonDocumentLoaderException;
import org.ojai.Document;
import org.ojai.exceptions.OjaiException;
import org.ojai.store.Connection;
import org.ojai.store.DocumentMutation;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.ojai.store.exceptions.StoreException;

import java.util.HashMap;
import java.util.Map;

public class MapRJson6_0DocumentLoader extends MapRJsonDocumentLoader {
  private Map<String, DocumentStore> theStores = new HashMap<>();
  private Connection connection;

  @Override
  protected Document createDocumentInternal(String jsonString) {
    if(connection == null) {
      startConnection();
    }

    return connection.newDocument(jsonString);
  }

  @Override
  protected DocumentMutation createDocumentMutationInternal() {
    if(connection == null) {
      startConnection();
    }

    return connection.newMutation();
  }

  @Override
  protected void commitInternal(String tableName, Document document, boolean createTable) throws MapRJsonDocumentLoaderException {
    initTable(tableName, createTable);
    theStores.get(tableName).insert(document);
  }

  @Override
  protected void commitReplaceInternal(String tableName, Document document, boolean createTable) throws MapRJsonDocumentLoaderException {
    initTable(tableName, createTable);
    theStores.get(tableName).insertOrReplace(document);
  }

  @Override
  protected void commitMutationInternal(String tableName, String fieldPath, DocumentMutation documentMutation) throws MapRJsonDocumentLoaderException {
    initTable(tableName, false);
    theStores.get(tableName).update(fieldPath, documentMutation);
  }

  @Override
  protected void deleteRowInternal(String tableName, String id) throws MapRJsonDocumentLoaderException {
    initTable(tableName, false);
    theStores.get(tableName).delete(id);
  }

  @Override
  protected void flushInternal(String tableName) throws MapRJsonDocumentLoaderException {
    try {
      connection.getStore(tableName).flush();
    } catch (StoreException ex) {
      throw new MapRJsonDocumentLoaderException(
          "Encountered error flushing table changes",
          ex
      );
    }
  }

  @Override
  protected void closeInternal() {
    if(connection != null) {
      for (Map.Entry<String, DocumentStore> entry : theStores.entrySet()) {
        entry.getValue().close();
      }

      theStores.clear();
      connection.close();
      connection = null;
    }
  }

  private void initTable(String tableName, boolean createTable) throws MapRJsonDocumentLoaderException {
    if(connection == null) {
      startConnection();
    }

    // if we've encountered this table name already, it's open.
    if(theStores.containsKey(tableName)) {
      return;
    }

    // open table (optionally create it)
    // and add to the group of open tables.
    try {
      theStores.put(tableName, connection.getStore(tableName));
      // isReadOnly() call triggers TableNotFoundException, since getStore() doesn't even check for table existence
      if(theStores.get(tableName).isReadOnly()) {
        throw new MapRJsonDocumentLoaderException("Cannot edit readonly table " + tableName);
      }
    } catch (OjaiException ex) {
      if(createTable) {
        try {
          theStores.put(tableName, MapRDB.createTable(tableName));
        } catch (DBException ex2) {
          throw new MapRJsonDocumentLoaderException("Encountered error creating table " + tableName, ex2);
        }
      } else {
        throw new MapRJsonDocumentLoaderException(
            "MapR DB table " + tableName + " does not exist, and not configured to create", ex);
      }
    }
  }

  private void startConnection() {
    connection = DriverManager.getConnection("ojai:mapr:");
  }
}
