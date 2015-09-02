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
package com.streamsets.pipeline.api.impl;

import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.StageException;

public class ErrorMessage implements LocalizableString {
  private static final Object[] NULL_ONE_ARG = {null};

  private final String errorCode;
  private final LocalizableString localizableMessage;
  private final long timestamp;
  private final boolean preppendErrorCode;

  public ErrorMessage(final StageException ex) {
    errorCode = ex.getErrorCode().getCode();
    timestamp = System.currentTimeMillis();
    localizableMessage = new LocalizableString() {
      @Override
      public String getNonLocalized() {
        return ex.getMessage();
      }

      @Override
      public String getLocalized() {
        return ex.getLocalizedMessage();
      }
    };
    preppendErrorCode = false;
  }

  public ErrorMessage(String errorCode, final String nonLocalizedMsg, long timestamp) {
    this.errorCode = errorCode;
    this.localizableMessage = new LocalizableString() {
      @Override
      public String getNonLocalized() {
        return nonLocalizedMsg;
      }

      @Override
      public String getLocalized() {
        return nonLocalizedMsg;
      }
    };
    this.timestamp = timestamp;
    preppendErrorCode = true;
  }

  public ErrorMessage(ErrorCode errorCode, Object... params) {
    this(Utils.checkNotNull(errorCode, "errorCode").getClass().getName() + "-bundle", errorCode, params);
  }

  public ErrorMessage(String resourceBundle, ErrorCode errorCode, Object... params) {
    this.errorCode = Utils.checkNotNull(errorCode, "errorCode").getCode();
    params = (params != null) ? params : NULL_ONE_ARG;
    localizableMessage = new LocalizableMessage(errorCode.getClass().getClassLoader(), resourceBundle,
                                                errorCode.getCode(), errorCode.getMessage(), params);
    timestamp = System.currentTimeMillis();
    preppendErrorCode = true;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String getNonLocalized() {
    return (preppendErrorCode) ? Utils.format("{} - {}", getErrorCode(), localizableMessage.getNonLocalized())
                               : localizableMessage.getNonLocalized();
  }

  @Override
  public String getLocalized() {
    return (preppendErrorCode) ? Utils.format("{} - {}", getErrorCode(), localizableMessage.getLocalized())
                               : localizableMessage.getLocalized();
  }

  @Override
  public String toString() {
    return getNonLocalized();
  }
}
