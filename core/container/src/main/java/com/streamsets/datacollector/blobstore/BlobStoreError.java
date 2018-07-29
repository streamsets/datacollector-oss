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

import com.streamsets.pipeline.api.ErrorCode;

public enum  BlobStoreError implements ErrorCode {
  BLOB_STORE_0001("Can't update metadata on disk store: {}"),
  BLOB_STORE_0002("Can't load metadata from disk: {}"),
  BLOB_STORE_0003("Object already exists: namespace={}, object={}, version={}"),
  BLOB_STORE_0004("Can't write new version of an object: {}"),
  BLOB_STORE_0005("Object doesn't exists: namespace={}"),
  BLOB_STORE_0006("Object doesn't exists: namespace={}, object={}"),
  BLOB_STORE_0007("Object doesn't exists: namespace={}, object={}, version={}"),
  BLOB_STORE_0008("Can't read object: {}"),
  BLOB_STORE_0009("Can't remove object: {}"),
  BLOB_STORE_0010("Temporary file for three-phase commit already exists"),
  BLOB_STORE_0011("Can't delete old metadata file: {}"),
  BLOB_STORE_0012("Can't promote new metadata file: {}"),
  BLOB_STORE_0013("Can't locate metadata files"),
  ;

  private final String message;

  BlobStoreError(String message) {
    this.message = message;
  }

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getMessage() {
    return message;
  }
}
