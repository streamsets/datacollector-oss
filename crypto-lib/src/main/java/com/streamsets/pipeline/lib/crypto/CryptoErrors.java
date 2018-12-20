/*
 * Copyright 2018 StreamSets Inc.
 *
 * Licensed under the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.streamsets.pipeline.lib.crypto;

import com.streamsets.pipeline.api.ErrorCode;

public enum CryptoErrors implements ErrorCode {
  CRYPTO_01("Unable to get credentials provider for AWS: '{}'"),
  CRYPTO_02("Expected decryption input to be a BYTE_ARRAY but found '{}'"),
  CRYPTO_03("Received a '{}' but complex field types are not supported"),
  CRYPTO_04("The value '{}' is not a valid integer"),
  CRYPTO_05("The value '{}' must be in the range {} and {}"),
  CRYPTO_06("Data key caching is not supported without a key derivation function (KDF).\n" +
      "Please choose a compatible cipher or disable data key caching."),
  CRYPTO_07("Error while encrypting/decrypting data: {}"),
  ;

  private final String message;

  CryptoErrors(String message) {
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
