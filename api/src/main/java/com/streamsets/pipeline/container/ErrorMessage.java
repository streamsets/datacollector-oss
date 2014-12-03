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

public class ErrorMessage implements LocalizableString {
  private static final Object[] NULL_ONE_ARG = {null};

  private final ErrorId errorId;
  private final LocalizableString localizableMessage;
  private final Object[] params;

  public ErrorMessage(ErrorId errorId, Object... params) {
    this(Utils.checkNotNull(errorId, "errorId").getClass().getName(), errorId, params);
  }

  public ErrorMessage(String resourceBundle, ErrorId errorId, Object... params) {
    this.errorId = Utils.checkNotNull(errorId, "errorId");
    params = (params != null) ? params : NULL_ONE_ARG;
    if (errorId instanceof Enum) {
      localizableMessage = new LocalizableMessage(errorId.getClass().getClassLoader(), resourceBundle,
                                                  errorId.toString(),
                                                  "[" + errorId.toString() + "] - " + errorId.getMessage(), params);
      this.params = null;
    } else {
      localizableMessage = null;
      this.params = params;
    }
  }

  public ErrorId getId() {
    return errorId;
  }

  @Override
  public String getNonLocalized() {
    return (localizableMessage != null) ? localizableMessage.getNonLocalized()
                                        : Utils.format(errorId.getMessage(), params);
  }

  @Override
  public String getLocalized() {
    return (localizableMessage != null) ? localizableMessage.getLocalized()
                                        : Utils.format(errorId.getMessage(), params);
  }

}
