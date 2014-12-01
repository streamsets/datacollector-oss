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

public class NonLocalizableErrorId implements LocalizableString {
  private static final Object[] NULL_ONE_ARG = {null};

  private final ErrorId errorId;
  private final Object[] params;

  public NonLocalizableErrorId(ErrorId errorId, Object... params) {
    this.errorId = errorId;
    this.params = (params != null) ? params : NULL_ONE_ARG;
  }

  @Override
  public String getNonLocalized() {
    return Utils.format(errorId.getMessage(), params);
  }

  @Override
  public String getLocalized() {
    return getNonLocalized();
  }
}
