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
package com.streamsets.pipeline.stage.processor.fieldhasher;

import com.streamsets.pipeline.lib.hashing.HashingUtil;

public enum HashType {
  MD5("MD5", HashingUtil.HashType.MD5),
  SHA1("SHA-1", HashingUtil.HashType.SHA1),
  SHA256("SHA-256", HashingUtil.HashType.SHA256),
  SHA512("SHA-512", HashingUtil.HashType.SHA512),
  MURMUR3_128("murmur3_128", HashingUtil.HashType.MURMUR3_128),;

  private String digest;
  private HashingUtil.HashType hashType;

  private HashType(String digest, HashingUtil.HashType hashType) {
    this.digest = digest;
    this.hashType = hashType;
  }

  public HashingUtil.HashType getHashType() {
    return hashType;
  }

  public String getDigest() {
    return digest;
  }
}
