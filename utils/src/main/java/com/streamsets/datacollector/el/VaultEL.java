/**
 * Copyright 2016 StreamSets Inc.
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
package com.streamsets.datacollector.el;

import com.streamsets.datacollector.vault.Vault;
import com.streamsets.pipeline.api.ElFunction;
import com.streamsets.pipeline.api.ElParam;

public class VaultEL {
  private VaultEL() {}

  @ElFunction(
      prefix = "vault",
      name = "read",
      description = "Retrieves the value of the specified path in Vault."
  )
  public static String read(@ElParam("path") String path, @ElParam("key") String key) {
    return read(path, key, 0);
  }

  @ElFunction(
      prefix = "vault",
      name = "readWithDelay",
      description = "Retrieves the value of the specified path in Vault and waits for delay milliseconds." +
          "Primarily for AWS since generated credentials can take 5-10 seconds before they are ready for use."
  )
  public static String read(@ElParam("path") String path, @ElParam("key") String key, @ElParam("delay") long delay) {
    return Vault.read(path, key, delay);
  }

  @ElFunction(
      prefix = "vault",
      name = "token",
      description = "Returns the current Vault auth token."
  )
  public static String token() {
    return Vault.token();
  }
}
