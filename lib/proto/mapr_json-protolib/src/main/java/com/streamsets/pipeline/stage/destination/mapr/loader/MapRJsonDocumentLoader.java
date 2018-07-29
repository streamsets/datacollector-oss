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
import com.streamsets.pipeline.api.impl.Utils;
import org.apache.commons.lang.StringUtils;
import org.ojai.Document;
import org.ojai.store.DocumentMutation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.ServiceLoader;
import java.util.Set;

public abstract class MapRJsonDocumentLoader {
  private static final Logger LOG = LoggerFactory.getLogger(MapRJsonDocumentLoader.class);
  /**
   * Instantiated MapRJson Document Loader (it will be different version based on MapR version that is being used).
   */
  private static MapRJsonDocumentLoader delegate;

  /**
   * Since the test delegate lives in different scope (test one), we load it as usual, but thread it differently.
   */
  private static MapRJsonDocumentLoader testDelegate;
  private static final String TEST_DELEGATE_NAME = "com.streamsets.pipeline.stage.destination.mapr.loader.MockMapRJsonDocumentLoader";

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

  protected abstract Document createDocumentInternal(String jsonString);
  protected abstract DocumentMutation createDocumentMutationInternal();
  protected abstract void commitInternal(String tableName, Document document, boolean createTable) throws MapRJsonDocumentLoaderException;
  protected abstract void commitReplaceInternal(String tableName, Document document, boolean createTable) throws MapRJsonDocumentLoaderException;
  protected abstract void commitMutationInternal(String tableName, String fieldPath, DocumentMutation documentMutation) throws MapRJsonDocumentLoaderException;
  protected abstract void deleteRowInternal(String tableName, String id) throws MapRJsonDocumentLoaderException;
  protected abstract void flushInternal(String tableName) throws MapRJsonDocumentLoaderException;
  protected abstract void closeInternal();

  public static Document createDocument(String jsonString) {
    if(isTest) {
      Preconditions.checkNotNull(testDelegate);
      return testDelegate.createDocumentInternal(jsonString);
    } else {
      Preconditions.checkNotNull(delegate);
      return delegate.createDocumentInternal(jsonString);
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
}
