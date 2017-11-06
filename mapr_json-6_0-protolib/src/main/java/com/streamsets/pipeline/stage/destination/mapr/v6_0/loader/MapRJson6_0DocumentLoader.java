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

import com.streamsets.pipeline.stage.destination.mapr.loader.MapRJsonDocumentLoader;
import com.streamsets.pipeline.stage.destination.mapr.loader.MapRJsonDocumentLoaderException;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  protected void flushInternal(String tableName) {
    connection.getStore(tableName).flush();
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
    if(createTable) {
      throw new MapRJsonDocumentLoaderException("MapR 6.0 Stage Lib does not support auto-creation of tables.");
    }

    if(connection == null) {
      startConnection();
    }

    // if we've encountered this table name already, it's open.
    if(theStores.containsKey(tableName)) {
      return;
    }

    // open table
    // and add to the group of open tables.
    theStores.put(tableName, connection.getStore(tableName));
  }

  private void startConnection() {
    connection = DriverManager.getConnection("ojai:mapr:");
  }
}
