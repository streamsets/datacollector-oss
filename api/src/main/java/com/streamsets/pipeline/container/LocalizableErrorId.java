/**
 * Licensed to the Apache Software Foundation (ASF) under one
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
package com.streamsets.pipeline.container;

import com.streamsets.pipeline.api.ErrorId;

public class LocalizableErrorId extends LocalizableMessage {

  private final String id;

  public LocalizableErrorId(ErrorId errorId, Object... params) {
    this(errorId.getClass().getName(), errorId, params);
  }

  public LocalizableErrorId(String resourceBundleName, ErrorId errorId, Object... params) {
    super(errorId.getClass().getClassLoader(), resourceBundleName, errorId.toString(), errorId.getMessage(),
          params);
    Utils.checkArgument(errorId instanceof Enum, Utils.format("ErrorId class '{}' must be an enum to be Localizable",
                                                              errorId.getClass()));
    id = errorId.toString();
  }

  @Override
  public String getNonLocalized() {
    return Utils.format("[{}] - {}", id, super.getNonLocalized());
  }

  @Override
  public String getLocalized() {
    return Utils.format("[{}] - {}", id, super.getLocalized());
  }

  @Override
  public String toString() {
    return getNonLocalized();
  }

}
