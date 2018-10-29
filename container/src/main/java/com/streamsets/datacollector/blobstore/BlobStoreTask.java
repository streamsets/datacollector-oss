/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.datacollector.blobstore;

import com.streamsets.datacollector.task.Task;
import com.streamsets.pipeline.api.BlobStore;
import com.streamsets.pipeline.api.StageException;

import java.util.Set;

/**
 * Extend the base BlobStore with our Task interface to plug it into runtime structure of data collector.
 */
public interface BlobStoreTask extends Task, BlobStore {

  /**
   * List all namespaces that exists.
   */
  public Set<String> listNamespaces();

  /**
   * List all objects in given namespace.
   */
  public Set<String> listObjects(String namespace);

  /**
   * Retrieve content file name of the given namespace,  ID, and version.
   */
  public String retrieveContentFileName(String namespace, String id, long version) throws StageException;
}
