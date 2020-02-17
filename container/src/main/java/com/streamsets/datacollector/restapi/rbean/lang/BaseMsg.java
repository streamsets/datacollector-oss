/*
 * Copyright 2020 StreamSets Inc.
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
package com.streamsets.datacollector.restapi.rbean.lang;

import com.google.common.base.Preconditions;
import com.streamsets.pipeline.api.ErrorCode;
import com.streamsets.pipeline.api.impl.Utils;

public abstract class BaseMsg <M extends BaseMsg> {
  public enum Type {ERROR, WARNING, NOTIFICATION}

  private Type type;
  private ErrorCode code;
  private Object[] args;
  private String message;
  private boolean isException;

  protected BaseMsg(Type type, ErrorCode code, Object... args) {
    this.type = Preconditions.checkNotNull(type, "type cannot be NULL");
    this.code = Preconditions.checkNotNull(code, "code cannot be NULL");;
    this.args = args;
  }

  public Type getType() {
    return type;
  }

  public String getCode() {
    return code.getCode();
  }

  public synchronized String getMessage() {
    if (message == null) {
      try {
        message = Utils.format(code.getMessage(), args);
      } catch (Exception ex) {
        setException();
        message = getType() + " : " + getCode() + " - [Error while generating the message: " + ex.toString() + "]";
      }
    }
    return message;
  }

  @SuppressWarnings("unchecked")
  M setException() {
    isException = true;
    return (M) this;
  }

  public boolean isException() {
    return isException;
  }

  @Override
  public String toString() {
    return getType() + " : " + getCode() + " - " + getMessage();
  }

}
