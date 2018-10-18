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

import com.streamsets.datacollector.util.LambdaUtil;
import com.streamsets.pipeline.api.BlobStore;
import com.streamsets.pipeline.api.StageException;

import java.util.Set;

/**
 * This is a runtime wrapper for BlobStore that will properly switch class loaders.
 */
public class BlobStoreRuntime implements BlobStore {

  private ClassLoader cl;
  private BlobStore delegate;

  public BlobStoreRuntime(ClassLoader cl, BlobStore delegate) {
    this.cl = cl;
    this.delegate = delegate;
  }

  @Override
  public void store(String namespace, String id, long version, String content) throws StageException {
    LambdaUtil.privilegedWithClassLoader(
      cl,
      StageException.class,
      () -> { delegate.store(namespace, id, version, content); return null; }
    );
  }

  @Override
  public long latestVersion(String namespace, String id) throws StageException {
    return LambdaUtil.privilegedWithClassLoader(
      cl,
      StageException.class,
      () -> delegate.latestVersion(namespace, id)
    );
  }

  @Override
  public boolean exists(String namespace, String id) {
    return LambdaUtil.privilegedWithClassLoader(
      cl,
      () -> delegate.exists(namespace, id)
    );
  }

  @Override
  public boolean exists(String namespace, String id, long version) {
    return LambdaUtil.privilegedWithClassLoader(
      cl,
      () -> delegate.exists(namespace, id, version)
    );
  }

  @Override
  public Set<Long> allVersions(String namespace, String id) {
    return LambdaUtil.privilegedWithClassLoader(
      cl,
      () -> delegate.allVersions(namespace, id)
    );
  }

  @Override
  public String retrieve(String namespace, String id, long version) throws StageException {
    return LambdaUtil.privilegedWithClassLoader(
      cl,
      StageException.class,
      () -> delegate.retrieve(namespace, id, version)
    );
  }

  @Override
  public VersionedContent retrieveLatest(String namespace, String id) throws StageException {
    return LambdaUtil.privilegedWithClassLoader(
      cl,
      StageException.class,
      () -> delegate.retrieveLatest(namespace, id)
    );
  }

  @Override
  public void delete(String namespace, String id, long version) throws StageException {
    LambdaUtil.privilegedWithClassLoader(
      cl,
      StageException.class,
      () -> { delegate.delete(namespace, id, version); return null; }
    );
  }

  @Override
  public void deleteAllVersions(String namespace, String id) throws StageException {
    LambdaUtil.privilegedWithClassLoader(
      cl,
      StageException.class,
      () -> { delegate.deleteAllVersions(namespace, id); return null; }
    );
  }
}
